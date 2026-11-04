//! `tessel-host serve`: accepts this user's connections on the private socket and answers them
//! with the protocol. Threads, not an async runtime: a few clients, each with its own reader.
use crate::files::Files;
use crate::log::Log;
use crate::outbound::Outbound;
use crate::pty::{Peer, Terminals};
use crate::runtime::{Runtime, bind, is_this_user, peer_uid, random_bytes, runtime, try_lock};
use crate::tree::Trees;
use std::io::{self, Write};
use std::net::Shutdown;
use std::os::unix::net::UnixStream;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;
use tessel_host_protocol::{
    Budgets, Capability, ClientFrame, Connection, Epoch, ErrorCode, HostFrame, HostInfo,
    HostLimits, MAX_FRAME_BYTES, PersistenceMethod, RawMessage, ReplyMessage, Step, StreamId,
    WireError, encode_frame, read_frame, wire_error,
};
use tessel_host_protocol::{file, pty, tree};

const MAX_CLIENTS: usize = 16;
/// A connection that has not said hello by then is closed; after hello there is no limit.
const HELLO_TIMEOUT: Duration = Duration::from_secs(10);
const READY_TIMEOUT_MS: libc::c_int = 10_000;

enum Detached {
    /// The process that was started: exits with this once the daemon is ready (or not).
    Parent(i32),
    /// The daemon, in a new session, told to write one byte on this pipe when ready.
    Child(libc::c_int),
}

fn last_error() -> io::Error {
    io::Error::last_os_error()
}

/// Must run before any thread exists.
fn detach() -> io::Result<Detached> {
    let mut fds = [0 as libc::c_int; 2];
    // SAFETY: fds has room for the two descriptors pipe2 writes.
    if unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_CLOEXEC) } != 0 {
        return Err(last_error());
    }
    // SAFETY: still single-threaded, so the child may go on running Rust code.
    match unsafe { libc::fork() } {
        -1 => Err(last_error()),
        0 => {
            // SAFETY: plain system calls on our own descriptors and a static path.
            unsafe {
                libc::close(fds[0]);
                libc::setsid();
                libc::chdir(c"/".as_ptr());
            }
            Ok(Detached::Child(fds[1]))
        }
        _ => {
            // SAFETY: our own descriptors; poll/read write only into locals.
            let code = unsafe {
                libc::close(fds[1]);
                let mut poll = libc::pollfd {
                    fd: fds[0],
                    events: libc::POLLIN,
                    revents: 0,
                };
                let mut byte = 0u8;
                let ready = libc::poll(&mut poll, 1, READY_TIMEOUT_MS) == 1
                    && libc::read(fds[0], (&mut byte as *mut u8).cast(), 1) == 1
                    && byte == b'k';
                libc::close(fds[0]);
                if ready { 0 } else { 1 }
            };
            Ok(Detached::Parent(code))
        }
    }
}

fn report(ready: &mut Option<libc::c_int>, byte: u8) {
    if let Some(fd) = ready.take() {
        // SAFETY: our own pipe end, written once and closed.
        unsafe {
            libc::write(fd, (&byte as *const u8).cast(), 1);
            libc::close(fd);
        }
    }
}

fn quiet_stdio() {
    // SAFETY: open/dup2/close on descriptors this process owns.
    unsafe {
        let null = libc::open(c"/dev/null".as_ptr(), libc::O_RDWR | libc::O_CLOEXEC);
        if null >= 0 {
            for fd in 0..3 {
                libc::dup2(null, fd);
            }
            if null > 2 {
                libc::close(null);
            }
        }
    }
}

pub fn serve(daemonize: bool, method: PersistenceMethod) -> i32 {
    // SAFETY: process-wide settings, before any thread: files are private, and the end of the
    // SSH session that started us is not a reason to stop.
    unsafe {
        libc::umask(0o077);
        libc::signal(libc::SIGHUP, libc::SIG_IGN);
    }
    let runtime = match runtime() {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("tessel-host: {error}");
            return 1;
        }
    };
    let mut ready = None;
    if daemonize {
        match detach() {
            Ok(Detached::Parent(code)) => return code,
            Ok(Detached::Child(fd)) => ready = Some(fd),
            Err(error) => {
                eprintln!("tessel-host: {error}");
                return 1;
            }
        }
    }
    match run(&runtime, method, &mut ready, daemonize) {
        Ok(()) => 0,
        Err(error) => {
            report(&mut ready, b'e');
            eprintln!("tessel-host: {error}");
            1
        }
    }
}

fn run(
    runtime: &Runtime,
    method: PersistenceMethod,
    ready: &mut Option<libc::c_int>,
    daemonize: bool,
) -> io::Result<()> {
    let Some(lock) = try_lock(runtime)? else {
        // Another daemon of this version serves this user: nothing to do.
        report(ready, b'k');
        return Ok(());
    };
    let listener = bind(runtime, &lock)?;
    let log = Arc::new(Log::new(runtime.log.clone()));
    let epoch = Epoch::from_bytes(random_bytes()?);
    let budgets = Budgets::SLOW_HOST;
    let trees = Trees::new()?;
    let shared = Arc::new(Shared {
        limits: HostLimits {
            capabilities: vec![Capability::Pty, Capability::Tree, Capability::Editor],
            budgets,
            epoch: epoch.clone(),
            info: HostInfo {
                version: crate::VERSION.to_owned(),
                arch: crate::arch().to_owned(),
                pid: std::process::id(),
                persistence: crate::persistence::describe(method),
            },
        },
        terminals: Terminals::new(epoch, budgets.total_ring_bytes as usize),
        files: Files::new(trees.clone(), runtime.dir.join("uploads")),
        trees,
        next_peer: AtomicU64::new(1),
    });
    crate::tree::raise_file_limit();
    handle_termination(
        shared.terminals.clone(),
        runtime.socket.clone(),
        log.clone(),
    );
    log.event("serve", &[("pid", u64::from(std::process::id()))]);
    report(ready, b'k');
    if daemonize {
        quiet_stdio();
    }
    let active = Arc::new(AtomicUsize::new(0));
    for stream in listener.incoming() {
        let Ok(mut stream) = stream else { continue };
        if !peer_uid(&stream).is_ok_and(is_this_user) {
            log.event("peer_refused", &[]);
            continue;
        }
        if active.load(Ordering::SeqCst) >= MAX_CLIENTS {
            if let Ok(bytes) = encode_frame(
                &HostFrame::connection_error(WireError::new(ErrorCode::Busy)),
                MAX_FRAME_BYTES,
            ) {
                let _ = stream.write_all(&bytes);
            }
            continue;
        }
        active.fetch_add(1, Ordering::SeqCst);
        let (shared, log, active) = (shared.clone(), log.clone(), active.clone());
        std::thread::spawn(move || {
            log.event("client_open", &[]);
            client(stream, &shared);
            log.event("client_close", &[]);
            active.fetch_sub(1, Ordering::SeqCst);
        });
    }
    drop(lock);
    Ok(())
}

struct Shared {
    limits: HostLimits,
    terminals: Arc<Terminals>,
    trees: Arc<Trees>,
    files: Arc<Files>,
    next_peer: AtomicU64,
}

/// SIGTERM or SIGINT: hang up every terminal's process group (a kill for those that linger),
/// remove the socket, exit. Blocked in every thread so only this one receives them; programs
/// started on terminals get a clean signal mask from `Command`.
fn handle_termination(terminals: Arc<Terminals>, socket: std::path::PathBuf, log: Arc<Log>) {
    // SAFETY: a local signal set; masking before any other thread exists.
    let set = unsafe {
        let mut set: libc::sigset_t = std::mem::zeroed();
        libc::sigemptyset(&mut set);
        libc::sigaddset(&mut set, libc::SIGTERM);
        libc::sigaddset(&mut set, libc::SIGINT);
        libc::pthread_sigmask(libc::SIG_BLOCK, &set, std::ptr::null_mut());
        set
    };
    std::thread::spawn(move || {
        let mut signal = 0;
        // SAFETY: waits on the set blocked above.
        unsafe { libc::sigwait(&set, &mut signal) };
        log.event("shutdown", &[("signal", signal as u64)]);
        terminals.shutdown();
        let _ = std::fs::remove_file(&socket);
        std::process::exit(0);
    });
}

fn encode(frame: &HostFrame, limit: usize) -> Option<Vec<u8>> {
    encode_frame(frame, limit).ok()
}

/// Tree events hold at least one entry with a long name and its envelope.
const MIN_TREE_FRAME_BYTES: usize = 16 * 1024;

fn tree_frame(peer: &Peer) -> Result<(), WireError> {
    if peer.frame_limit >= MIN_TREE_FRAME_BYTES {
        Ok(())
    } else {
        Err(WireError::detail(
            ErrorCode::BadRequest,
            "the frame budget is too small for file trees",
        ))
    }
}

fn done() -> RawMessage {
    ReplyMessage::Done.into_raw()
}

/// A capability request, answered. The stream of a new attachment comes back too: its events
/// start only once the reply is queued, so the client knows the stream before its first event.
fn handle(
    shared: &Shared,
    peer: &Peer,
    message: &RawMessage,
) -> (Result<RawMessage, WireError>, Option<StreamId>) {
    let terminals = &shared.terminals;
    let reply = match message.kind.as_str() {
        pty::kind::CREATE => message
            .body_as::<pty::Create>()
            .and_then(|request| terminals.create(request, peer))
            .map(|created| RawMessage::of(pty::kind::CREATED, &created)),
        pty::kind::ATTACH => {
            return match message
                .body_as::<pty::Attach>()
                .and_then(|request| terminals.attach(request, peer))
            {
                Ok(attached) => {
                    let stream = attached.stream.clone();
                    (
                        Ok(RawMessage::of(pty::kind::ATTACHED, &attached)),
                        Some(stream),
                    )
                }
                Err(error) => (Err(error), None),
            };
        }
        pty::kind::DETACH => message
            .body_as::<pty::Detach>()
            .and_then(|request| terminals.detach(peer.id, &request))
            .map(|()| done()),
        pty::kind::INPUT => message
            .body_as::<pty::Input>()
            .and_then(|request| terminals.input(request))
            .map(|ok| RawMessage::of(pty::kind::INPUT_OK, &ok)),
        pty::kind::RESIZE => message
            .body_as::<pty::Resize>()
            .and_then(|request| terminals.resize(request))
            .map(|()| done()),
        pty::kind::SIGNAL => message
            .body_as::<pty::SignalRequest>()
            .and_then(|request| terminals.signal(request))
            .map(|()| done()),
        pty::kind::CLOSE => message
            .body_as::<pty::Close>()
            .and_then(|request| terminals.close(request))
            .map(|()| done()),
        pty::kind::LIST if message.body.is_null() => {
            Ok(RawMessage::of(pty::kind::LISTED, &terminals.list()))
        }
        pty::kind::LIST => Err(WireError::detail(
            ErrorCode::BadRequest,
            "this message has no body",
        )),
        tree::kind::ROOT_OPEN => message
            .body_as::<tree::RootOpen>()
            .and_then(|request| shared.trees.open_root(request))
            .map(|opened| RawMessage::of(tree::kind::ROOT_OPENED, &opened)),
        tree::kind::SUBSCRIBE => tree_frame(peer)
            .and_then(|()| message.body_as::<tree::Subscribe>())
            .and_then(|request| shared.trees.subscribe(request, peer))
            .map(|subscribed| RawMessage::of(tree::kind::SUBSCRIBED, &subscribed)),
        tree::kind::UNSUBSCRIBE => message
            .body_as::<tree::Unsubscribe>()
            .and_then(|request| shared.trees.unsubscribe(peer.id, &request))
            .map(|()| done()),
        tree::kind::EXPAND => tree_frame(peer)
            .and_then(|()| message.body_as::<tree::Expand>())
            .and_then(|request| shared.trees.expand(request, peer))
            .map(|listing| RawMessage::of(tree::kind::LISTING, &listing)),
        tree::kind::COLLAPSE => message
            .body_as::<tree::Collapse>()
            .and_then(|request| shared.trees.collapse(request, peer.id))
            .map(|()| done()),
        tree::kind::STATS => message
            .body_as::<tree::Stats>()
            .and_then(|request| shared.trees.stats(request))
            .map(|stats| RawMessage::of(tree::kind::STATS_OK, &stats)),
        file::kind::STAT => message
            .body_as::<file::Stat>()
            .and_then(|r| shared.files.stat(r))
            .map(|info| RawMessage::of(file::kind::INFO, &info)),
        file::kind::READ => message
            .body_as::<file::Read>()
            .and_then(|r| shared.files.read(r))
            .map(|chunk| RawMessage::of(file::kind::CHUNK, &chunk)),
        file::kind::WRITE_BEGIN => message
            .body_as::<file::WriteBegin>()
            .and_then(|r| shared.files.begin(r, peer))
            .map(|upload| RawMessage::of(file::kind::UPLOAD, &upload)),
        file::kind::WRITE_CHUNK => message
            .body_as::<file::WriteChunk>()
            .and_then(|r| shared.files.chunk(r, peer.id))
            .map(|()| done()),
        file::kind::WRITE_COMMIT => message
            .body_as::<file::WriteCommit>()
            .and_then(|r| shared.files.commit(r, peer.id))
            .map(|written| RawMessage::of(file::kind::WRITTEN, &written)),
        file::kind::WRITE_ABORT => message
            .body_as::<file::WriteAbort>()
            .and_then(|r| shared.files.abort(r, peer.id))
            .map(|()| done()),
        file::kind::COMPARE => message
            .body_as::<file::Compare>()
            .and_then(|r| shared.files.compare(r))
            .map(|compared| RawMessage::of(file::kind::COMPARED, &compared)),
        file::kind::RENAME => message
            .body_as::<file::Rename>()
            .and_then(|r| shared.files.rename(r))
            .map(|()| done()),
        file::kind::REMOVE => message
            .body_as::<file::Remove>()
            .and_then(|r| shared.files.remove(r))
            .map(|()| done()),
        file::kind::MKDIR => message
            .body_as::<file::Mkdir>()
            .and_then(|r| shared.files.mkdir(r))
            .map(|()| done()),
        _ => Err(WireError::new(ErrorCode::UnsupportedKind)),
    };
    (reply, None)
}

fn client(stream: UnixStream, shared: &Shared) {
    let Ok(socket) = stream.try_clone() else {
        return;
    };
    let out = Arc::new(Outbound::default());
    let writer = {
        let out = out.clone();
        std::thread::spawn(move || out.drain(socket))
    };
    let mut reader = stream;
    let _ = reader.set_read_timeout(Some(HELLO_TIMEOUT));
    let mut connection = Connection::new(shared.limits.clone());
    let mut limit = MAX_FRAME_BYTES;
    let mut peer: Option<Peer> = None;
    let id = shared.next_peer.fetch_add(1, Ordering::SeqCst);
    loop {
        let frame = match read_frame::<ClientFrame>(&mut reader, limit) {
            Ok(Some(frame)) => frame,
            Ok(None) => break,
            Err(error) => {
                if let Some(refusal) = wire_error(&error)
                    && let Some(bytes) =
                        encode(&HostFrame::connection_error(refusal.clone()), limit)
                {
                    out.control(bytes);
                }
                break;
            }
        };
        match connection.on_frame(frame) {
            Ok(Step::Reply(reply)) => {
                if let Some(bytes) = encode(&reply, limit) {
                    out.control(bytes);
                }
            }
            Ok(Step::Dispatch(dispatch)) => {
                let Some(peer) = &peer else { break };
                let (result, stream) = handle(shared, peer, &dispatch.message);
                if let Some(reply) = connection.finish(dispatch.request_id, result)
                    && let Some(bytes) = encode(&reply, limit)
                {
                    out.control(bytes);
                }
                if let Some(stream) = stream {
                    shared.terminals.start(&stream);
                }
            }
            Ok(Step::Ack(ack)) => {
                let routed = match shared.terminals.ack(id, &ack) {
                    Ok(true) => Ok(true),
                    Ok(false) => shared.trees.ack(id, &ack),
                    Err(refusal) => Err(refusal),
                };
                if let Err(refusal) = routed {
                    if let Some(bytes) = encode(&HostFrame::connection_error(refusal), limit) {
                        out.control(bytes);
                    }
                    break;
                }
            }
            Err(fatal) => {
                if let Some(bytes) = encode(&fatal.frame(), limit) {
                    out.control(bytes);
                }
                break;
            }
        }
        if out.is_closed() {
            break;
        }
        if peer.is_none()
            && let Some(agreed) = connection.negotiated()
        {
            limit = agreed.budgets.frame_bytes as usize;
            let _ = reader.set_read_timeout(None);
            peer = Some(Peer {
                id,
                out: out.clone(),
                frame_limit: limit,
                terminal_ring_bytes: agreed.budgets.terminal_ring_bytes as usize,
                window_bytes: agreed.budgets.stream_window_bytes,
            });
        }
    }
    shared.terminals.disconnect(id);
    shared.trees.disconnect(id);
    shared.files.disconnect(id);
    out.end();
    let _ = writer.join();
    let _ = reader.shutdown(Shutdown::Both);
}
