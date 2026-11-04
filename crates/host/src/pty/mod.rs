//! The daemon's terminals. Each has a pseudo-terminal, a program in its own session, one I/O
//! thread (reads output, writes queued input, never blocks on either), a waiter that records
//! the exit without reaping early, a bounded replay ring of numbered output chunks, and a
//! screen model for snapshots. Terminals belong to the daemon, not to a connection: losing a
//! connection only drops its attachments. One lock guards them all, so replay history is
//! bounded across terminals, not only per terminal.
pub mod spawn;

use crate::outbound::Outbound;
use crate::runtime::random_bytes;
use std::collections::{HashMap, VecDeque};
use std::fs::File;
use std::io::{self, Read, Write};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;
use tessel_host_protocol::pty::{
    self, Attach, AttachMode, Attached, Close, Create, Created, Detach, Exit, Input, InputOk,
    Listed, Output, Resize, Signal, SignalRequest, Size, Snapshot, TerminalInfo, Writer,
};
use tessel_host_protocol::{
    Ack, Chunk, Dedup, DedupCache, Epoch, ErrorCode, HostFrame, LeaseKey, MAX_CHUNK_BYTES,
    RawMessage, StreamId, StreamPos, StreamSender, TerminalId, WireError, encode_frame,
};

const MAX_TERMINALS: usize = 64;
const MAX_PENDING_INPUT: usize = 1024 * 1024;
const CREATE_TTL_MS: u64 = 10 * 60 * 1000;
const CREATE_KEYS: usize = 256;
const SCROLLBACK_ROWS: usize = 0;
/// Room for an event's envelope beside its data, for the acknowledgment window.
const EVENT_OVERHEAD: usize = 256;
const CLOSE_GRACE: Duration = Duration::from_secs(3);
/// A 64 KiB output chunk, in base64, inside one frame: terminals need at least this frame budget.
pub const MIN_FRAME_BYTES: usize = 96 * 1024;

fn frame_budget(peer: &Peer) -> Result<(), WireError> {
    if peer.frame_limit >= MIN_FRAME_BYTES {
        Ok(())
    } else {
        Err(refuse_with(
            ErrorCode::BadRequest,
            "the frame budget is too small for terminals",
        ))
    }
}

fn refuse(code: ErrorCode) -> WireError {
    WireError::new(code)
}

fn refuse_with(code: ErrorCode, detail: &'static str) -> WireError {
    WireError::detail(code, detail)
}

/// Who an attachment sends to.
#[derive(Clone)]
pub struct Peer {
    pub id: u64,
    pub out: Arc<Outbound>,
    pub frame_limit: usize,
    pub terminal_ring_bytes: usize,
    pub window_bytes: u32,
}

struct Attachment {
    stream: StreamId,
    peer: Peer,
    sender: StreamSender,
    /// The next terminal sequence number to send.
    next: u64,
    snapshot: VecDeque<Snapshot>,
    exit_sent: bool,
    started: bool,
}

struct Lease {
    key: LeaseKey,
    last_input: u64,
}

struct Term {
    pid: u32,
    master: Arc<File>,
    wake: Arc<OwnedFd>,
    size: Size,
    seq: u64,
    ring: VecDeque<Vec<u8>>,
    ring_bytes: usize,
    ring_cap: usize,
    /// Sequence number of the oldest chunk in `ring` (seq + 1 when empty).
    first: u64,
    parser: vt100::Parser,
    lease: Option<Lease>,
    pending: VecDeque<u8>,
    eof: bool,
    status: Option<(Option<i32>, Option<i32>)>,
    exit: Option<Exit>,
    closing: bool,
    attachments: Vec<Attachment>,
}

struct State {
    terms: HashMap<TerminalId, Term>,
    total_bytes: usize,
    total_cap: usize,
    creates: DedupCache<Created>,
}

pub struct Terminals {
    state: Mutex<State>,
    epoch: Epoch,
    started: std::time::Instant,
}

fn now_ms(start: std::time::Instant) -> u64 {
    start.elapsed().as_millis() as u64
}

fn event_frame(
    stream: &StreamId,
    seq: u64,
    kind: &str,
    body: &impl serde::Serialize,
    limit: usize,
) -> Option<Vec<u8>> {
    let frame = HostFrame::event(
        StreamPos {
            id: stream.clone(),
            seq,
        },
        RawMessage::of(kind, body),
    );
    encode_frame(&frame, limit).ok()
}

impl Term {
    fn info(&self, terminal: &TerminalId) -> TerminalInfo {
        TerminalInfo {
            terminal: terminal.clone(),
            pid: self.pid,
            size: self.size,
            seq: self.seq,
            attached: self.attachments.len() as u32,
            leased: self.lease.is_some(),
            retained_bytes: self.ring_bytes as u64,
            exited: self.exit,
        }
    }

    fn snapshot(&self, resync: bool) -> VecDeque<Snapshot> {
        snapshot_of(&self.parser, self.size, self.seq, resync)
    }

    fn check_lease(&self, lease: &LeaseKey) -> Result<(), WireError> {
        match &self.lease {
            Some(held) if held.key == *lease => Ok(()),
            _ => Err(refuse_with(
                ErrorCode::PermissionDenied,
                "this client does not hold the terminal's lease",
            )),
        }
    }

    fn alive(&self) -> bool {
        self.status.is_none()
    }

    /// Sends what each started attachment may: pending snapshot parts, then output, then the
    /// exit, as far as its window allows. An attachment whose next chunk was evicted gets a
    /// new snapshot instead (a gap it is told about), never a hole.
    fn pump(&mut self) {
        let (first, seq, exit, size) = (self.first, self.seq, self.exit, self.size);
        let (ring, parser) = (&self.ring, &self.parser);
        for a in self.attachments.iter_mut() {
            if !a.started || a.peer.out.is_closed() {
                continue;
            }
            // Its next output was evicted. Once it has acknowledged everything sent, so the
            // snapshot is of now and not of a moment it will never see, it gets one.
            if a.snapshot.is_empty()
                && a.next < first
                && a.next <= seq
                && a.sender.unacked_bytes() == 0
            {
                a.snapshot = snapshot_of(parser, size, seq, true);
                a.next = seq + 1;
            }
            while let Some(part) = a.snapshot.front() {
                let Ok(stream_seq) = a
                    .sender
                    .reserve(part.data.as_bytes().len() + EVENT_OVERHEAD)
                else {
                    break;
                };
                if let Some(frame) = event_frame(
                    &a.stream,
                    stream_seq,
                    pty::kind::SNAPSHOT,
                    part,
                    a.peer.frame_limit,
                ) {
                    a.peer.out.event(frame);
                }
                a.snapshot.pop_front();
            }
            if !a.snapshot.is_empty() {
                continue;
            }
            while a.next >= first && a.next <= seq {
                let chunk = &ring[(a.next - first) as usize];
                let Ok(stream_seq) = a.sender.reserve(chunk.len() + EVENT_OVERHEAD) else {
                    break;
                };
                let output = Output {
                    seq: a.next,
                    data: Chunk::new(chunk.clone()).unwrap_or_default(),
                };
                if let Some(frame) = event_frame(
                    &a.stream,
                    stream_seq,
                    pty::kind::OUTPUT,
                    &output,
                    a.peer.frame_limit,
                ) {
                    a.peer.out.event(frame);
                }
                a.next += 1;
            }
            if let Some(exit) = &exit
                && !a.exit_sent
                && a.next > seq
                && let Ok(stream_seq) = a.sender.reserve(EVENT_OVERHEAD)
            {
                if let Some(frame) = event_frame(
                    &a.stream,
                    stream_seq,
                    pty::kind::EXIT,
                    exit,
                    a.peer.frame_limit,
                ) {
                    a.peer.out.event(frame);
                }
                a.exit_sent = true;
            }
        }
        self.attachments.retain(|a| !a.peer.out.is_closed());
    }

    fn signal(&self, signal: libc::c_int) {
        // Only a process group this daemon started and has not reaped: its id cannot have been
        // reused (the waiter reaps under the same lock, after recording the exit).
        if self.alive() {
            // SAFETY: a signal to the terminal's own process group (its session leader's pid).
            unsafe { libc::kill(-(self.pid as libc::pid_t), signal) };
        }
    }
}

/// The screen as of output `seq`, in parts of at most 64 KiB. On the alternate screen, the
/// parts start by switching to it, so a reset terminal ends up there too (the main screen
/// behind it is not part of a snapshot: the screen model keeps only what is shown).
fn snapshot_of(parser: &vt100::Parser, size: Size, seq: u64, resync: bool) -> VecDeque<Snapshot> {
    let screen = parser.screen();
    let mut bytes = Vec::new();
    if screen.alternate_screen() {
        bytes.extend_from_slice(b"\x1b[?1049h");
    }
    bytes.extend_from_slice(&screen.state_formatted());
    let parts: Vec<&[u8]> = bytes.chunks(MAX_CHUNK_BYTES).collect();
    let count = parts.len();
    parts
        .into_iter()
        .enumerate()
        .map(|(i, part)| Snapshot {
            at: seq,
            part: i as u32,
            last: i + 1 == count,
            size,
            alternate: screen.alternate_screen(),
            resync,
            data: Chunk::new(part.to_vec()).unwrap_or_default(),
        })
        .collect()
}

fn wake(fd: &OwnedFd) {
    // SAFETY: one byte to our own non-blocking pipe; a full pipe already means "wake up".
    unsafe { libc::write(fd.as_raw_fd(), [1u8].as_ptr().cast(), 1) };
}

fn pipe() -> io::Result<(OwnedFd, OwnedFd)> {
    let mut fds = [0 as libc::c_int; 2];
    // SAFETY: room for two descriptors, wrapped at once.
    if unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_CLOEXEC | libc::O_NONBLOCK) } != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(unsafe { (OwnedFd::from_raw_fd(fds[0]), OwnedFd::from_raw_fd(fds[1])) })
}

fn rss_bytes() -> u64 {
    let pages = std::fs::read_to_string("/proc/self/statm")
        .ok()
        .and_then(|s| {
            s.split_whitespace()
                .nth(1)
                .and_then(|n| n.parse::<u64>().ok())
        })
        .unwrap_or(0);
    // SAFETY: sysconf has no preconditions.
    pages * unsafe { libc::sysconf(libc::_SC_PAGESIZE) }.max(0) as u64
}

impl Terminals {
    pub fn new(epoch: Epoch, total_ring_bytes: usize) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(State {
                terms: HashMap::new(),
                total_bytes: 0,
                total_cap: total_ring_bytes,
                creates: DedupCache::new(CREATE_KEYS, CREATE_TTL_MS),
            }),
            epoch,
            started: std::time::Instant::now(),
        })
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    pub fn create(self: &Arc<Self>, request: Create, peer: &Peer) -> Result<Created, WireError> {
        request.validate()?;
        frame_budget(peer)?;
        let now = now_ms(self.started);
        {
            let mut state = self.lock();
            match state.creates.begin(&request.dedup, now)? {
                Dedup::Finished(created) => return Ok(created),
                Dedup::Running => {
                    return Err(refuse_with(
                        ErrorCode::Busy,
                        "this terminal is still being created",
                    ));
                }
                Dedup::Fresh => {}
            }
            if state.terms.len() >= MAX_TERMINALS {
                state.creates.abandon(&request.dedup);
                return Err(refuse_with(ErrorCode::Busy, "too many terminals"));
            }
        }
        let spawned = spawn::spawn(
            &request.argv,
            request.cwd.as_deref(),
            &request.env,
            request.size,
        );
        let mut state = self.lock();
        let spawned = match spawned {
            Ok(spawned) => spawned,
            Err(_) => {
                state.creates.abandon(&request.dedup);
                return Err(refuse_with(
                    ErrorCode::NotFound,
                    "the program could not be started",
                ));
            }
        };
        let (wake_read, wake_write) = pipe().map_err(|_| refuse(ErrorCode::Internal))?;
        let terminal =
            TerminalId::from_bytes(random_bytes().map_err(|_| refuse(ErrorCode::Internal))?);
        let lease = LeaseKey::from_bytes(random_bytes().map_err(|_| refuse(ErrorCode::Internal))?);
        let master = Arc::new(spawned.master);
        let ring_cap = peer.terminal_ring_bytes.min(state.total_cap).max(1);
        state.terms.insert(
            terminal.clone(),
            Term {
                pid: spawned.pid,
                master: master.clone(),
                wake: Arc::new(wake_write),
                size: request.size,
                seq: 0,
                ring: VecDeque::new(),
                ring_bytes: 0,
                ring_cap,
                first: 1,
                parser: vt100::Parser::new(request.size.rows, request.size.cols, SCROLLBACK_ROWS),
                lease: Some(Lease {
                    key: lease.clone(),
                    last_input: 0,
                }),
                pending: VecDeque::new(),
                eof: false,
                status: None,
                exit: None,
                closing: false,
                attachments: Vec::new(),
            },
        );
        let created = Created {
            terminal: terminal.clone(),
            pid: spawned.pid,
            lease,
        };
        state.creates.finish(&request.dedup, created.clone(), now);
        drop(state);
        let this = self.clone();
        let id = terminal.clone();
        std::thread::spawn(move || this.io_loop(id, master, wake_read));
        let this = self.clone();
        std::thread::spawn(move || this.wait_loop(terminal, spawned.pid));
        Ok(created)
    }

    /// Registers the attachment; its events start with `start`, after the reply was queued.
    pub fn attach(&self, request: Attach, peer: &Peer) -> Result<Attached, WireError> {
        if request.epoch != self.epoch {
            return Err(refuse(ErrorCode::EpochMismatch));
        }
        frame_budget(peer)?;
        let mut state = self.lock();
        let term = state
            .terms
            .get_mut(&request.terminal)
            .ok_or_else(|| refuse(ErrorCode::NotFound))?;
        if request.after > term.seq {
            return Err(refuse_with(
                ErrorCode::BadRequest,
                "after is beyond this terminal's output",
            ));
        }
        let lease = match request.writer {
            Writer::None => None,
            Writer::Take => {
                let key =
                    LeaseKey::from_bytes(random_bytes().map_err(|_| refuse(ErrorCode::Internal))?);
                term.lease = Some(Lease {
                    key: key.clone(),
                    last_input: 0,
                });
                Some(key)
            }
            Writer::Resume(key) => {
                term.check_lease(&key)?;
                Some(key)
            }
        };
        let replay = request.after + 1 >= term.first;
        let (mode, snapshot, next) = if replay {
            (AttachMode::Replay, VecDeque::new(), request.after + 1)
        } else {
            (AttachMode::Snapshot, term.snapshot(false), term.seq + 1)
        };
        let stream = StreamId::from_bytes(random_bytes().map_err(|_| refuse(ErrorCode::Internal))?);
        term.attachments.push(Attachment {
            stream: stream.clone(),
            peer: peer.clone(),
            sender: StreamSender::new(peer.window_bytes),
            next,
            snapshot,
            exit_sent: false,
            started: false,
        });
        Ok(Attached {
            stream,
            pid: term.pid,
            size: term.size,
            mode,
            seq: term.seq,
            lease,
            exited: term.exit,
        })
    }

    pub fn start(&self, stream: &StreamId) {
        let mut state = self.lock();
        for term in state.terms.values_mut() {
            if let Some(a) = term.attachments.iter_mut().find(|a| a.stream == *stream) {
                a.started = true;
                term.pump();
                return;
            }
        }
    }

    /// Ok(false): not a terminal stream.
    pub fn ack(&self, peer: u64, ack: &Ack) -> Result<bool, WireError> {
        let mut state = self.lock();
        for term in state.terms.values_mut() {
            if let Some(a) = term
                .attachments
                .iter_mut()
                .find(|a| a.stream == ack.stream && a.peer.id == peer)
            {
                a.sender.ack(ack.seq).map_err(|_| {
                    refuse_with(ErrorCode::BadFrame, "acknowledgment of an event never sent")
                })?;
                term.pump();
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub fn detach(&self, peer: u64, request: &Detach) -> Result<(), WireError> {
        let mut state = self.lock();
        for term in state.terms.values_mut() {
            let before = term.attachments.len();
            term.attachments
                .retain(|a| !(a.stream == request.stream && a.peer.id == peer));
            if term.attachments.len() != before {
                return Ok(());
            }
        }
        Err(refuse(ErrorCode::NotFound))
    }

    /// The connection is gone: its attachments go; terminals and leases stay.
    pub fn disconnect(&self, peer: u64) {
        let mut state = self.lock();
        for term in state.terms.values_mut() {
            term.attachments.retain(|a| a.peer.id != peer);
        }
    }

    pub fn input(&self, request: Input) -> Result<InputOk, WireError> {
        let mut state = self.lock();
        let term = state
            .terms
            .get_mut(&request.terminal)
            .ok_or_else(|| refuse(ErrorCode::NotFound))?;
        term.check_lease(&request.lease)?;
        let last_input = term.lease.as_ref().map_or(0, |lease| lease.last_input);
        if request.input_id <= last_input {
            return Ok(InputOk { duplicate: true });
        }
        if term.eof || !term.alive() {
            return Err(refuse_with(ErrorCode::BadRequest, "the program has ended"));
        }
        let data = request.data.as_bytes();
        if term.pending.len() + data.len() > MAX_PENDING_INPUT {
            return Err(refuse_with(
                ErrorCode::Busy,
                "the program is not reading its input",
            ));
        }
        if let Some(lease) = term.lease.as_mut() {
            lease.last_input = request.input_id;
        }
        term.pending.extend(data);
        wake(&term.wake);
        Ok(InputOk { duplicate: false })
    }

    pub fn resize(&self, request: Resize) -> Result<(), WireError> {
        request.size.validate()?;
        let mut state = self.lock();
        let term = state
            .terms
            .get_mut(&request.terminal)
            .ok_or_else(|| refuse(ErrorCode::NotFound))?;
        term.check_lease(&request.lease)?;
        // Serialized with output under the lock: a snapshot never mixes two sizes.
        spawn::set_size(&term.master, request.size).map_err(|_| refuse(ErrorCode::Internal))?;
        term.parser.set_size(request.size.rows, request.size.cols);
        term.size = request.size;
        Ok(())
    }

    pub fn signal(&self, request: SignalRequest) -> Result<(), WireError> {
        let state = self.lock();
        let term = state
            .terms
            .get(&request.terminal)
            .ok_or_else(|| refuse(ErrorCode::NotFound))?;
        term.check_lease(&request.lease)?;
        term.signal(match request.signal {
            Signal::Int => libc::SIGINT,
            Signal::Term => libc::SIGTERM,
            Signal::Kill => libc::SIGKILL,
            Signal::Hup => libc::SIGHUP,
            Signal::Quit => libc::SIGQUIT,
            Signal::Tstp => libc::SIGTSTP,
            Signal::Cont => libc::SIGCONT,
        });
        Ok(())
    }

    pub fn close(self: &Arc<Self>, request: Close) -> Result<(), WireError> {
        let mut state = self.lock();
        let term = state
            .terms
            .get_mut(&request.terminal)
            .ok_or_else(|| refuse(ErrorCode::NotFound))?;
        term.check_lease(&request.lease)?;
        term.closing = true;
        if term.exit.is_some() {
            state.terms.remove(&request.terminal);
            return Ok(());
        }
        term.signal(libc::SIGHUP);
        let this = self.clone();
        let id = request.terminal;
        std::thread::spawn(move || {
            std::thread::sleep(CLOSE_GRACE);
            let state = this.lock();
            if let Some(term) = state.terms.get(&id) {
                term.signal(libc::SIGKILL);
            }
        });
        Ok(())
    }

    pub fn list(&self) -> Listed {
        let state = self.lock();
        let mut terminals: Vec<TerminalInfo> =
            state.terms.iter().map(|(id, t)| t.info(id)).collect();
        terminals.sort_by(|a, b| a.terminal.cmp(&b.terminal));
        Listed {
            terminals,
            daemon_rss_bytes: rss_bytes(),
            retained_bytes: state.total_bytes as u64,
        }
    }

    /// Clean shutdown: a hang-up to every live terminal's process group, a kill to the rest.
    pub fn shutdown(&self) {
        {
            let state = self.lock();
            for term in state.terms.values() {
                term.signal(libc::SIGHUP);
            }
        }
        std::thread::sleep(Duration::from_millis(500));
        let state = self.lock();
        for term in state.terms.values() {
            term.signal(libc::SIGKILL);
        }
    }

    fn output(&self, terminal: &TerminalId, bytes: &[u8]) {
        let mut state = self.lock();
        let State {
            terms,
            total_bytes,
            total_cap,
            ..
        } = &mut *state;
        let Some(term) = terms.get_mut(terminal) else {
            return;
        };
        term.seq += 1;
        term.parser.process(bytes);
        term.ring.push_back(bytes.to_vec());
        term.ring_bytes += bytes.len();
        *total_bytes += bytes.len();
        while term.ring_bytes > term.ring_cap && term.ring.len() > 1 {
            let gone = term.ring.pop_front().map(|c| c.len()).unwrap_or(0);
            term.ring_bytes -= gone;
            *total_bytes -= gone;
            term.first += 1;
        }
        // Across terminals: the largest history gives way first.
        while *total_bytes > *total_cap {
            let Some(largest) = terms
                .values_mut()
                .filter(|t| t.ring.len() > 1)
                .max_by_key(|t| t.ring_bytes)
            else {
                break;
            };
            let gone = largest.ring.pop_front().map(|c| c.len()).unwrap_or(0);
            largest.ring_bytes -= gone;
            *total_bytes -= gone;
            largest.first += 1;
        }
        for term in terms.values_mut() {
            term.pump();
        }
    }

    fn finish_if_ended(state: &mut State, terminal: &TerminalId) {
        let Some(term) = state.terms.get_mut(terminal) else {
            return;
        };
        if let (true, Some((code, signal)), None) = (term.eof, term.status, term.exit) {
            term.exit = Some(Exit {
                seq: term.seq,
                code,
                signal,
            });
            term.pump();
            if term.closing {
                state.terms.remove(terminal);
            }
        }
    }

    fn io_loop(self: Arc<Self>, terminal: TerminalId, master: Arc<File>, wake_read: OwnedFd) {
        let fd = master.as_raw_fd();
        // SAFETY: flags on our own descriptor.
        unsafe {
            libc::fcntl(
                fd,
                libc::F_SETFL,
                libc::fcntl(fd, libc::F_GETFL) | libc::O_NONBLOCK,
            )
        };
        let mut buffer = vec![0u8; MAX_CHUNK_BYTES];
        loop {
            let want_write = !self
                .lock()
                .terms
                .get(&terminal)
                .is_none_or(|t| t.pending.is_empty());
            let mut fds = [
                libc::pollfd {
                    fd,
                    events: libc::POLLIN | if want_write { libc::POLLOUT } else { 0 },
                    revents: 0,
                },
                libc::pollfd {
                    fd: wake_read.as_raw_fd(),
                    events: libc::POLLIN,
                    revents: 0,
                },
            ];
            // SAFETY: two valid pollfd entries.
            if unsafe { libc::poll(fds.as_mut_ptr(), 2, -1) } < 0 {
                if io::Error::last_os_error().kind() == io::ErrorKind::Interrupted {
                    continue;
                }
                break;
            }
            if fds[1].revents != 0 {
                let mut sink = [0u8; 64];
                // SAFETY: reading into a local buffer from our own pipe.
                while unsafe {
                    libc::read(wake_read.as_raw_fd(), sink.as_mut_ptr().cast(), sink.len())
                } > 0
                {}
            }
            if fds[0].revents & libc::POLLOUT != 0 {
                let mut state = self.lock();
                if let Some(term) = state.terms.get_mut(&terminal) {
                    let (front, _) = term.pending.as_slices();
                    let chunk = front.len().min(MAX_CHUNK_BYTES);
                    match (&*term.master).write(&front[..chunk]) {
                        Ok(n) => {
                            term.pending.drain(..n);
                        }
                        Err(e)
                            if e.kind() == io::ErrorKind::WouldBlock
                                || e.kind() == io::ErrorKind::Interrupted => {}
                        Err(_) => term.pending.clear(),
                    }
                }
            }
            if fds[0].revents & (libc::POLLIN | libc::POLLHUP | libc::POLLERR) != 0 {
                match (&*master).read(&mut buffer) {
                    Ok(0) => break,
                    Ok(n) => self.output(&terminal, &buffer[..n]),
                    Err(e)
                        if e.kind() == io::ErrorKind::WouldBlock
                            || e.kind() == io::ErrorKind::Interrupted => {}
                    // EIO: every program on the terminal closed it.
                    Err(_) => break,
                }
            }
        }
        let mut state = self.lock();
        if let Some(term) = state.terms.get_mut(&terminal) {
            term.eof = true;
        }
        Self::finish_if_ended(&mut state, &terminal);
    }

    fn wait_loop(self: Arc<Self>, terminal: TerminalId, pid: u32) {
        // Wait without reaping, so the pid stays ours until the exit is recorded under the lock.
        loop {
            // SAFETY: siginfo is plain data written by waitid.
            let mut info: libc::siginfo_t = unsafe { std::mem::zeroed() };
            let result = unsafe {
                libc::waitid(
                    libc::P_PID,
                    pid as libc::id_t,
                    &mut info,
                    libc::WEXITED | libc::WNOWAIT,
                )
            };
            if result == 0 || io::Error::last_os_error().kind() != io::ErrorKind::Interrupted {
                break;
            }
        }
        let mut state = self.lock();
        let mut raw = 0;
        // SAFETY: reaping our own child, which has exited.
        unsafe { libc::waitpid(pid as libc::pid_t, &mut raw, 0) };
        let status = if libc::WIFEXITED(raw) {
            (Some(libc::WEXITSTATUS(raw)), None)
        } else if libc::WIFSIGNALED(raw) {
            (None, Some(libc::WTERMSIG(raw)))
        } else {
            (None, None)
        };
        if let Some(term) = state.terms.get_mut(&terminal) {
            term.status = Some(status);
        }
        Self::finish_if_ended(&mut state, &terminal);
    }
}
