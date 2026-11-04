//! A terminal on the host, seen from the desktop. Events come in order, each handed over once:
//! output and resizes numbered by the terminal, a snapshot (all its parts joined) when history
//! was dropped, the exit once. When the link breaks the events say `Disconnected`; on the same
//! daemon the terminal reattaches after the last event handed over and says `Reattached` before
//! anything the new attachment sends, keeping its lease; on a restarted daemon it is `Lost`.
//! Input is numbered here and retried with the same number through busy answers and
//! reconnections, so it is typed at most once: `Ok` means it was typed; an error after a
//! reconnection that outlasted `request_timeout` means it may or may not have been.
//!
//! One stream at a time hands events over: a reattachment or a takeover starts a new
//! generation, and whatever the replaced stream still holds (even an event waiting for the
//! application to read) is never handed over, so two streams can never interleave.
use crate::link::{Link, StreamItem};
use crate::{Client, ClientError, Resume};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, SyncSender, TrySendError, sync_channel};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tessel_host_protocol::pty::{
    self, Attach, AttachMode, Attached, Close, Create, Created, Detach, Exit, Input, InputOk,
    LeaseLost, Output, Resize, Resized, Signal, SignalRequest, Size, Snapshot, Writer,
};
use tessel_host_protocol::{
    Chunk, DedupKey, Epoch, ErrorCode, LeaseKey, MAX_CHUNK_BYTES, MAX_ID, RawMessage, StreamId,
    TerminalId,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TerminalEvent {
    Output {
        seq: u64,
        data: Vec<u8>,
    },
    /// Redraw from this, on a reset screen (keep your own scrollback); then output after `at`.
    Snapshot {
        at: u64,
        size: Size,
        alternate: bool,
        resync: bool,
        data: Vec<u8>,
    },
    Resized {
        seq: u64,
        size: Size,
    },
    Exit(Exit),
    /// Another client took the lease: this one only watches now.
    LeaseLost,
    /// The link broke; the terminal may still run on the host. Always before `Reattached`.
    Disconnected,
    /// Back on the same daemon: replay or snapshot follows, after this event.
    Reattached {
        mode: AttachMode,
        lease: bool,
    },
    /// The daemon restarted: this terminal is gone.
    Lost,
    /// This terminal's stream sent something this client refuses to apply. It is let go, as
    /// if dropped: nothing more comes, it is never reattached, and it no longer types (its
    /// lease stays valid on the host and can be used by a new, explicit attachment). Other
    /// terminals and the link are not affected. The last event.
    StreamRejected(StreamRejection),
}

/// Why a terminal's stream was refused. Fixed reasons, never the stream's contents.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StreamRejection {
    /// Snapshot parts out of order, disagreeing on the screen they describe, too many of them,
    /// or interrupted by another event.
    InvalidSnapshot,
    /// A snapshot larger than `MAX_SNAPSHOT_BYTES`.
    OversizedSnapshot,
}

/// The largest snapshot this client assembles (all parts together). Parts are refused before
/// they are kept, so no more than this is ever held for one snapshot.
pub const MAX_SNAPSHOT_BYTES: usize = 16 * 1024 * 1024;

/// Most parts of one snapshot: the daemon sends full 64 KiB parts and a last one, so a
/// snapshot within `MAX_SNAPSHOT_BYTES` has at most this many. Bounds a flood of empty parts.
const MAX_SNAPSHOT_PARTS: usize = MAX_SNAPSHOT_BYTES / MAX_CHUNK_BYTES + 1;

/// A snapshot being received, checked part by part before anything is kept.
#[derive(Default)]
struct Parts {
    first: Option<Snapshot>,
    count: usize,
    data: Vec<u8>,
}

impl Parts {
    fn active(&self) -> bool {
        self.first.is_some()
    }

    /// Takes one part; the assembled snapshot once its last part came.
    fn accept(&mut self, part: Snapshot) -> Result<Option<TerminalEvent>, StreamRejection> {
        let consistent = match &self.first {
            None => part.part == 0,
            Some(first) => {
                part.part as usize == self.count
                    && part.at == first.at
                    && part.size == first.size
                    && part.alternate == first.alternate
                    && part.resync == first.resync
            }
        };
        if !consistent || self.count >= MAX_SNAPSHOT_PARTS {
            return Err(StreamRejection::InvalidSnapshot);
        }
        if self.data.len() + part.data.as_bytes().len() > MAX_SNAPSHOT_BYTES {
            return Err(StreamRejection::OversizedSnapshot);
        }
        self.data.extend_from_slice(part.data.as_bytes());
        self.count += 1;
        let last = part.last;
        if self.first.is_none() {
            self.first = Some(Snapshot {
                data: Chunk::default(),
                ..part
            });
        }
        if !last {
            return Ok(None);
        }
        let first = self.first.take().expect("a first part");
        self.count = 0;
        Ok(Some(TerminalEvent::Snapshot {
            at: first.at,
            size: first.size,
            alternate: first.alternate,
            resync: first.resync,
            data: std::mem::take(&mut self.data),
        }))
    }
}

/// What has been handed to the application, and which stream may hand over more.
struct Delivery {
    /// Bumped when a stream is replaced (a reattachment, a takeover): only the follower of the
    /// current generation hands events over.
    generation: u64,
    /// The last numbered event handed over: an output's or resize's `seq`, a snapshot's `at`.
    last: u64,
    /// The last connection event handed over was `Disconnected` (so `Reattached` follows one).
    disconnected: bool,
    exited: bool,
    /// The current generation's stream, detached when a takeover replaces it.
    stream: Option<StreamId>,
    /// The application let go (dropped the `Terminal` or its receiver). Kept here, under the
    /// same lock as the generation, so letting go and starting a generation never interleave:
    /// once set, no generation starts, none is current, nothing is handed over.
    abandoned: bool,
}

impl Delivery {
    fn new(last: u64) -> Self {
        Self {
            generation: 0,
            last,
            disconnected: false,
            exited: false,
            stream: None,
            abandoned: false,
        }
    }

    /// A new generation, unless the application let go.
    fn begin(&mut self) -> Option<u64> {
        if self.abandoned {
            return None;
        }
        self.generation += 1;
        Some(self.generation)
    }

    /// Whether `generation` may hand events over and keep its stream.
    fn admits(&self, generation: u64) -> bool {
        !self.abandoned && self.generation == generation
    }

    /// Lets go: no generation is current any more. The stream to stop, the first time only.
    fn abandon(&mut self) -> Option<Option<StreamId>> {
        if self.abandoned {
            return None;
        }
        self.abandoned = true;
        self.generation += 1;
        Some(self.stream.take())
    }

    /// Lets go, but only for `generation` while it is current: a replaced stream never gives
    /// up the terminal its successor now follows.
    fn abandon_current(&mut self, generation: u64) -> Option<Option<StreamId>> {
        if !self.admits(generation) {
            return None;
        }
        self.abandon()
    }
}

/// What handing an event over changes here.
enum Effect {
    Last(u64),
    Exited,
    Disconnected,
    Reattached,
    LeaseLost,
    Nothing,
}

enum Delivered {
    Sent,
    /// Already handed over (a replayed event, a second exit, a second `Disconnected`).
    Skipped,
    /// A newer generation took over: this follower stops.
    Stale,
    /// The application dropped its receiver.
    Closed,
}

struct Shared {
    client: Client,
    terminal: TerminalId,
    epoch: Mutex<Epoch>,
    lease: Mutex<Option<LeaseKey>>,
    delivery: Mutex<Delivery>,
    next_input: AtomicU64,
    events: SyncSender<TerminalEvent>,
    lost: Mutex<bool>,
}

/// What `Terminal::create_keyed` answers: the terminal, its events, and the lease the create
/// granted, which stays valid on the host whatever the object does afterwards (keep it to
/// resume or end the program; `Terminal::lease()` alone says whether this object types).
pub struct CreatedTerminal {
    pub terminal: Terminal,
    pub events: Receiver<TerminalEvent>,
    pub lease: LeaseKey,
}

/// One terminal. Dropping it (or its event receiver) lets go of it: its attachment is detached
/// on the host and it is never reattached, but the program goes on there, with its lease, so
/// another `Terminal::attach_fresh` can pick it up later. Ending the program is `close`.
pub struct Terminal {
    shared: Arc<Shared>,
    pub pid: u32,
}

impl Drop for Terminal {
    fn drop(&mut self) {
        self.shared.abandon();
    }
}

fn random_key() -> DedupKey {
    use std::collections::hash_map::RandomState;
    use std::hash::{BuildHasher, Hasher};
    let mut bytes = [0u8; 16];
    for chunk in bytes.chunks_mut(8) {
        let mut hasher = RandomState::new().build_hasher();
        hasher.write_u128(Instant::now().elapsed().as_nanos() ^ std::process::id() as u128);
        chunk.copy_from_slice(&hasher.finish().to_le_bytes()[..chunk.len()]);
    }
    DedupKey::from_bytes(bytes)
}

impl Terminal {
    /// Starts a program and attaches to it, holding the lease.
    pub fn create(
        client: &Client,
        argv: Vec<String>,
        cwd: Option<String>,
        env: BTreeMap<String, String>,
        size: Size,
    ) -> Result<(Self, Receiver<TerminalEvent>), ClientError> {
        let epoch = client.epoch().ok_or(ClientError::ConnectionLost)?;
        let created = Self::create_keyed(client, random_key(), &epoch, argv, cwd, env, size)?;
        Ok((created.terminal, created.events))
    }

    /// As `create`, with the deduplication key chosen and kept by the caller (16 random
    /// bytes), on the daemon of `epoch` only.
    ///
    /// The daemon answers the same key with the same terminal and lease instead of starting
    /// another program, but only within its own run and for 10 minutes after it finished the
    /// first request, timed by its own clock. A caller cannot see that moment, only a lower
    /// bound: when it first sent the key. So send the same key again only within the same run
    /// of the desktop, timed with a monotonic clock (`Instant`) from the first send, and well
    /// inside the 10 minutes: at most 10 minutes minus `request_timeout` minus a margin (8
    /// minutes is safe). Never resend a key after the desktop restarted, whatever the wall
    /// clock says (it may have moved back): look at the host's terminals (`terminal.list`)
    /// and attach explicitly instead. A key the daemon forgot starts a second program.
    ///
    /// The key is a capability: whoever sends it within that time gets the terminal and its
    /// lease. Keep it like the lease, never in plain text.
    ///
    /// `epoch` is the daemon the caller means: if the link reaches another one (it restarted:
    /// its keys and terminals are gone), this answers `EpochChanged` without sending anything.
    /// A daemon still creating the terminal for an earlier request answers `busy`; that is
    /// retried briefly.
    ///
    /// The answer carries the lease the create granted (`CreatedTerminal::lease`), apart from
    /// the object: the object may let go before the caller looks at it (a snapshot it refused
    /// right after attaching), and the lease must still be kept and usable to end the program
    /// explicitly. If the create succeeded but the attachment after it failed, this answers
    /// the attachment's error: the same key sent again finds the same terminal and lease.
    pub fn create_keyed(
        client: &Client,
        dedup: DedupKey,
        epoch: &Epoch,
        argv: Vec<String>,
        cwd: Option<String>,
        env: BTreeMap<String, String>,
        size: Size,
    ) -> Result<CreatedTerminal, ClientError> {
        let request = Create {
            dedup,
            argv,
            cwd,
            env,
            size,
        };
        let deadline = Instant::now() + Duration::from_secs(3);
        let mut pause = Duration::from_millis(50);
        let created: Created = loop {
            let attempt = client.retrying(|| {
                if client.epoch().as_ref() != Some(epoch) {
                    return Err(ClientError::EpochChanged);
                }
                client.ask(pty::kind::CREATE, &request)
            });
            match attempt {
                Err(ClientError::Refused(e))
                    if e.code == ErrorCode::Busy && Instant::now() < deadline =>
                {
                    std::thread::sleep(pause);
                    pause = (pause * 2).min(Duration::from_millis(500));
                }
                other => break other?,
            }
        };
        let lease = created.lease.clone();
        let (terminal, events) = Self::attach(
            client,
            created.terminal,
            epoch.clone(),
            0,
            Some(created.lease),
        )?;
        Ok(CreatedTerminal {
            terminal,
            events,
            lease,
        })
    }

    /// Attaches to a terminal this pane still shows (after `after`, the last event it applied),
    /// as the lease holder when `lease` is given and still valid, else as a watcher.
    pub fn attach(
        client: &Client,
        terminal: TerminalId,
        epoch: Epoch,
        after: u64,
        lease: Option<LeaseKey>,
    ) -> Result<(Self, Receiver<TerminalEvent>), ClientError> {
        Self::attach_from(client, terminal, epoch, Some(after), lease)
    }

    /// Attaches to a terminal this pane shows nothing of (the desktop just started): the
    /// events start with a snapshot of the current screen, then go on live. With a lease kept
    /// from before, inputs are numbered above the ones it already used. Needs tessel-host 0.1.1
    /// or later; an older daemon refuses it.
    pub fn attach_fresh(
        client: &Client,
        terminal: TerminalId,
        epoch: Epoch,
        lease: Option<LeaseKey>,
    ) -> Result<(Self, Receiver<TerminalEvent>), ClientError> {
        Self::attach_from(client, terminal, epoch, None, lease)
    }

    fn attach_from(
        client: &Client,
        terminal: TerminalId,
        epoch: Epoch,
        after: Option<u64>,
        lease: Option<LeaseKey>,
    ) -> Result<(Self, Receiver<TerminalEvent>), ClientError> {
        let (events, receiver) = sync_channel(crate::link::STREAM_BUFFER);
        let shared = Arc::new(Shared {
            client: client.clone(),
            terminal,
            epoch: Mutex::new(epoch),
            lease: Mutex::new(lease),
            delivery: Mutex::new(Delivery::new(after.unwrap_or(0))),
            next_input: AtomicU64::new(1),
            events,
            lost: Mutex::new(false),
        });
        let link = client.link()?;
        let had_lease = shared.lease.lock().unwrap().is_some();
        let attached = shared.request_attach(&link, shared.writer(), after)?;
        shared.start(&link, &attached, 0, had_lease, false);
        client.register(Arc::downgrade(&shared) as std::sync::Weak<dyn Resume>);
        Ok((
            Self {
                pid: attached.pid,
                shared,
            },
            receiver,
        ))
    }

    pub fn id(&self) -> &TerminalId {
        &self.shared.terminal
    }

    /// The lease this object types with. None once it was let go (dropped receiver, rejected
    /// stream): such an object never types again, even though the lease stays valid on the
    /// host for a new, explicit attachment.
    pub fn lease(&self) -> Option<LeaseKey> {
        if self.shared.delivery.lock().unwrap().abandoned {
            return None;
        }
        self.shared.lease.lock().unwrap().clone()
    }

    /// The last numbered event handed to the application (an output's or resize's `seq`, a
    /// snapshot's `at`). It says what was delivered, not what the application applied (events
    /// can wait in its receiver), and nothing about a screen being rebuilt. A desktop that
    /// starts again shows nothing: it attaches with `attach_fresh` (a snapshot first), never
    /// with `attach` and a number kept from its previous run.
    pub fn last_seq(&self) -> u64 {
        self.shared.delivery.lock().unwrap().last
    }

    fn lease_or_refuse(&self) -> Result<LeaseKey, ClientError> {
        self.lease()
            .ok_or(ClientError::Refused(tessel_host_protocol::WireError::new(
                ErrorCode::PermissionDenied,
            )))
    }

    /// Types `bytes` (split in 64 KiB inputs). Each input is typed at most once: it is retried
    /// with the same number through busy answers and reconnections until `request_timeout`;
    /// `Ok` means it was typed, an error after a reconnection that outlasted the timeout means
    /// it may or may not have been.
    pub fn input(&self, bytes: &[u8]) -> Result<(), ClientError> {
        for piece in bytes.chunks(MAX_CHUNK_BYTES) {
            let id = self.shared.next_input.fetch_add(1, Ordering::SeqCst);
            if id > MAX_ID {
                return Err(ClientError::Protocol(
                    "this lease has used every input number".into(),
                ));
            }
            let mut pause = Duration::from_millis(20);
            let deadline = Instant::now() + self.shared.client.options().request_timeout;
            loop {
                let lease = self.lease_or_refuse()?;
                let request = Input {
                    terminal: self.shared.terminal.clone(),
                    lease,
                    input_id: id,
                    data: Chunk::new(piece.to_vec()).unwrap_or_default(),
                };
                match self.shared.client.retrying(|| {
                    self.shared
                        .client
                        .ask::<InputOk>(pty::kind::INPUT, &request)
                }) {
                    Ok(_) => break,
                    // The program is not reading: the same input, a little later.
                    Err(ClientError::Refused(e))
                        if e.code == ErrorCode::Busy && Instant::now() < deadline =>
                    {
                        std::thread::sleep(pause);
                        pause = (pause * 2).min(Duration::from_millis(500));
                    }
                    Err(error) => return Err(error),
                }
            }
        }
        Ok(())
    }

    pub fn resize(&self, size: Size) -> Result<(), ClientError> {
        let request = Resize {
            terminal: self.shared.terminal.clone(),
            lease: self.lease_or_refuse()?,
            size,
        };
        self.shared
            .client
            .retrying(|| {
                self.shared
                    .client
                    .ask::<serde_json::Value>(pty::kind::RESIZE, &request)
            })
            .map(|_| ())
    }

    pub fn signal(&self, signal: Signal) -> Result<(), ClientError> {
        let request = SignalRequest {
            terminal: self.shared.terminal.clone(),
            lease: self.lease_or_refuse()?,
            signal,
        };
        self.shared
            .client
            .retrying(|| {
                self.shared
                    .client
                    .ask::<serde_json::Value>(pty::kind::SIGNAL, &request)
            })
            .map(|_| ())
    }

    /// Ends the program (hang-up, then a kill). Refused once this object was let go; a
    /// program whose terminal was let go is ended with `close_terminal`, from an explicit,
    /// confirmed action.
    pub fn close(&self) -> Result<(), ClientError> {
        let request = Close {
            terminal: self.shared.terminal.clone(),
            lease: self.lease_or_refuse()?,
        };
        self.shared
            .client
            .retrying(|| {
                self.shared
                    .client
                    .ask::<serde_json::Value>(pty::kind::CLOSE, &request)
            })
            .map(|_| ())
    }

    /// Takes the lease from whoever holds it, and returns it: keep that one (encrypted) to
    /// resume or end the program later, even if this object lets go right after. `Ok` means
    /// this client holds it now; events go on in order, from a new attachment that replaces
    /// the current one. `lease()` stays the live authority to type.
    pub fn take_lease(&self) -> Result<LeaseKey, ClientError> {
        let shared = &self.shared;
        let link = shared.client.link()?;
        // The current stream stops handing events over now (its own lease_lost included); the
        // new attachment replays everything after the last event handed over.
        let (generation, replaced) = {
            let mut delivery = shared.delivery.lock().unwrap();
            let Some(generation) = delivery.begin() else {
                return Err(ClientError::Protocol(
                    "this terminal was let go: its event receiver was dropped".into(),
                ));
            };
            (generation, delivery.stream.clone())
        };
        let result = shared.request_attach(&link, Writer::Take, shared.after());
        let attached = match result {
            Ok(attached) => {
                *shared.lease.lock().unwrap() = attached.lease.clone();
                shared.start(&link, &attached, generation, false, false);
                attached.lease.clone().ok_or(ClientError::Protocol(
                    "the host granted no lease to a takeover".into(),
                ))
            }
            Err(error) => {
                // Back to following as before, on a new attachment of its own.
                if let Some(generation) = shared.next_generation() {
                    let had_lease = shared.lease.lock().unwrap().is_some();
                    if let Ok(attached) =
                        shared.request_attach(&link, shared.writer(), shared.after())
                    {
                        shared.start(&link, &attached, generation, had_lease, false);
                    }
                }
                Err(error)
            }
        };
        if let Some(stream) = replaced {
            shared.detach(&link, stream);
        }
        attached
    }
}

/// Ends a terminal's program without attaching to it: no stream, no snapshot. For a terminal
/// whose object was let go (a refused snapshot, a closed pane), from an explicit action the
/// person confirmed, with the lease kept for it. Before each attempt the link must reach
/// `epoch` (otherwise `EpochChanged`, nothing sent: a restarted daemon's terminals are gone);
/// the daemon checks the lease (`PermissionDenied` if another client took it meanwhile).
pub fn close_terminal(
    client: &Client,
    epoch: &Epoch,
    terminal: &TerminalId,
    lease: &LeaseKey,
) -> Result<(), ClientError> {
    let request = Close {
        terminal: terminal.clone(),
        lease: lease.clone(),
    };
    client
        .retrying(|| {
            if client.epoch().as_ref() != Some(epoch) {
                return Err(ClientError::EpochChanged);
            }
            client.ask::<serde_json::Value>(pty::kind::CLOSE, &request)
        })
        .map(|_| ())
}

/// What a stream's message means to the application, if anything. Snapshot parts are checked
/// one by one before they are kept (`Parts`); another event in the middle of a snapshot is
/// refused, so a snapshot is never applied half-received.
fn translate(
    message: &RawMessage,
    parts: &mut Parts,
) -> Result<Option<TerminalEvent>, StreamRejection> {
    if parts.active() && message.kind != pty::kind::SNAPSHOT {
        return Err(StreamRejection::InvalidSnapshot);
    }
    Ok(match message.kind.as_str() {
        pty::kind::OUTPUT => message
            .body_as::<Output>()
            .ok()
            .map(|output| TerminalEvent::Output {
                seq: output.seq,
                data: output.data.into_bytes(),
            }),
        pty::kind::RESIZED => {
            message
                .body_as::<Resized>()
                .ok()
                .map(|resized| TerminalEvent::Resized {
                    seq: resized.seq,
                    size: resized.size,
                })
        }
        pty::kind::SNAPSHOT => {
            let part: Snapshot = message
                .body_as()
                .map_err(|_| StreamRejection::InvalidSnapshot)?;
            return parts.accept(part);
        }
        pty::kind::EXIT => message.body_as::<Exit>().ok().map(TerminalEvent::Exit),
        pty::kind::LEASE_LOST => message
            .body_as::<LeaseLost>()
            .ok()
            .map(|_| TerminalEvent::LeaseLost),
        _ => None,
    })
}

impl Shared {
    fn writer(&self) -> Writer {
        match self.lease.lock().unwrap().clone() {
            Some(lease) => Writer::Resume(lease),
            None => Writer::None,
        }
    }

    /// Starts a new generation: from here, only a follower started with the number returned
    /// hands events over.
    fn next_generation(&self) -> Option<u64> {
        self.delivery.lock().unwrap().begin()
    }

    fn current(&self, generation: u64) -> bool {
        self.delivery.lock().unwrap().admits(generation)
    }

    /// Attaches after the last event handed over, or with a snapshot first when this client
    /// shows nothing yet (`after: None`, the first attachment of `attach_fresh` only).
    ///
    /// With a lease, the daemon says how far its inputs went (`last_input`) and the next input
    /// is numbered above: a lease kept across a restart of the desktop must never reuse an
    /// identifier the daemon would take for a repeat (and silently not type). A lease whose
    /// numbering is not given, or exhausted, is refused here rather than guessed.
    fn request_attach(
        &self,
        link: &Arc<Link>,
        writer: Writer,
        after: Option<u64>,
    ) -> Result<Attached, ClientError> {
        let request = Attach {
            terminal: self.terminal.clone(),
            epoch: self.epoch.lock().unwrap().clone(),
            after,
            writer,
        };
        let attached: Attached = link
            .request(
                RawMessage::of(pty::kind::ATTACH, &request),
                self.client.options().request_timeout,
            )?
            .body_as()
            .map_err(ClientError::Refused)?;
        if attached.lease.is_some() {
            match attached.last_input.filter(|used| *used < MAX_ID) {
                Some(used) => {
                    self.next_input.fetch_max(used + 1, Ordering::SeqCst);
                }
                None => {
                    self.detach(link, attached.stream.clone());
                    return Err(ClientError::Protocol(
                        "the host did not say how far this lease's inputs went".into(),
                    ));
                }
            }
        }
        Ok(attached)
    }

    /// After the last event handed over (a reattachment or a takeover).
    fn after(&self) -> Option<u64> {
        Some(self.delivery.lock().unwrap().last)
    }

    /// The application let go of this terminal: its follower stops at once (its stream is
    /// unregistered here, which ends the follower's wait even while the program prints
    /// nothing), it is never reattached, and the daemon is asked to drop the attachment. The
    /// program goes on, with its lease. Idempotent, and never waits for the daemon (the detach
    /// is sent from a thread of its own).
    fn abandon(&self) {
        // Under the delivery lock: a generation starting at the same moment either comes
        // first (and this stops it) or sees `abandoned` and never starts.
        let Some(stream) = self.delivery.lock().unwrap().abandon() else {
            return;
        };
        self.let_go(stream);
    }

    /// Refuses the stream of `generation`: hands `StreamRejected` over and lets go, both in
    /// the same critical section as a generation starting, so the rejection is always the last
    /// event and a replaced stream (an old follower draining what its dead link still held)
    /// can neither tell the application nor give up the terminal its successor follows.
    /// Returns whether it was the current stream. Waits outside the lock while the application
    /// has not read its events.
    fn reject(&self, generation: u64, reason: StreamRejection) -> bool {
        let mut pause = Duration::from_millis(1);
        loop {
            {
                let mut delivery = self.delivery.lock().unwrap();
                if !delivery.admits(generation) {
                    return false;
                }
                match self.events.try_send(TerminalEvent::StreamRejected(reason)) {
                    Ok(()) | Err(TrySendError::Disconnected(_)) => {
                        let stream = delivery.abandon_current(generation);
                        drop(delivery);
                        if let Some(stream) = stream {
                            self.let_go(stream);
                        }
                        return true;
                    }
                    Err(TrySendError::Full(_)) => {}
                }
            }
            std::thread::sleep(pause);
            pause = (pause * 2).min(Duration::from_millis(20));
        }
    }

    /// Stops following `stream` and asks the daemon to drop the attachment, from a thread of
    /// its own (never waits for the daemon).
    fn let_go(&self, stream: Option<StreamId>) {
        let (Some(stream), Ok(link)) = (stream, self.client.link()) else {
            return;
        };
        link.unregister(&stream);
        let timeout = self.client.options().request_timeout;
        std::thread::spawn(move || {
            let _ = link.request(
                RawMessage::of(pty::kind::DETACH, &Detach { stream }),
                timeout,
            );
        });
    }

    /// Stops an attachment nobody follows any more (best effort: the link may be gone).
    fn detach(&self, link: &Arc<Link>, stream: StreamId) {
        link.unregister(&stream);
        let _ = link.request(
            RawMessage::of(pty::kind::DETACH, &Detach { stream }),
            self.client.options().request_timeout,
        );
    }

    /// Hands over what the attachment says (a lease that did not come back, `Reattached`), then
    /// follows its stream as `generation`. Does nothing more if a newer generation took over.
    fn start(
        self: &Arc<Self>,
        link: &Arc<Link>,
        attached: &Attached,
        generation: u64,
        had_lease: bool,
        reattached: bool,
    ) {
        // A newer generation took over, or the application let go: this attachment goes.
        let ended = |delivered: Delivered| match delivered {
            Delivered::Sent | Delivered::Skipped => false,
            Delivered::Stale => true,
            Delivered::Closed => {
                self.abandon();
                true
            }
        };
        if had_lease
            && attached.lease.is_none()
            && ended(self.deliver(generation, TerminalEvent::LeaseLost))
        {
            return self.detach(link, attached.stream.clone());
        }
        if reattached
            && ended(self.deliver(
                generation,
                TerminalEvent::Reattached {
                    mode: attached.mode,
                    lease: attached.lease.is_some(),
                },
            ))
        {
            return self.detach(link, attached.stream.clone());
        }
        // Registered under the delivery lock: letting go after this unregisters the stream (so
        // the follower's wait ends), and letting go before it means it is never registered.
        let receiver = {
            let mut delivery = self.delivery.lock().unwrap();
            if !delivery.admits(generation) {
                drop(delivery);
                return self.detach(link, attached.stream.clone());
            }
            delivery.stream = Some(attached.stream.clone());
            link.register(&attached.stream)
        };
        self.follow(link, attached.stream.clone(), receiver, generation);
    }

    /// Hands `event` to the application, in order and once. Only the current generation hands
    /// events over; an output or resize at or before the last one handed over, a second exit
    /// and a second `Disconnected` are skipped; `last` moves only with what was handed over.
    /// The check and the hand-over happen under one lock without ever blocking in it: when the
    /// application has not read its events, this waits outside the lock and tries again.
    fn deliver(&self, generation: u64, mut event: TerminalEvent) -> Delivered {
        let mut pause = Duration::from_millis(1);
        loop {
            {
                let mut delivery = self.delivery.lock().unwrap();
                if !delivery.admits(generation) {
                    return Delivered::Stale;
                }
                let skip = match &event {
                    TerminalEvent::Output { seq, .. } | TerminalEvent::Resized { seq, .. } => {
                        *seq <= delivery.last
                    }
                    TerminalEvent::Exit(_) => delivery.exited,
                    TerminalEvent::Disconnected => delivery.disconnected,
                    _ => false,
                };
                if skip {
                    return Delivered::Skipped;
                }
                let effect = match &event {
                    TerminalEvent::Output { seq, .. } | TerminalEvent::Resized { seq, .. } => {
                        Effect::Last(*seq)
                    }
                    TerminalEvent::Snapshot { at, .. } => Effect::Last(*at),
                    TerminalEvent::Exit(_) => Effect::Exited,
                    TerminalEvent::Disconnected => Effect::Disconnected,
                    TerminalEvent::Reattached { .. } => Effect::Reattached,
                    TerminalEvent::LeaseLost => Effect::LeaseLost,
                    TerminalEvent::Lost | TerminalEvent::StreamRejected(_) => Effect::Nothing,
                };
                match self.events.try_send(event) {
                    Ok(()) => {
                        match effect {
                            Effect::Last(seq) => delivery.last = seq,
                            Effect::Exited => delivery.exited = true,
                            Effect::Disconnected => delivery.disconnected = true,
                            Effect::Reattached => delivery.disconnected = false,
                            Effect::LeaseLost => *self.lease.lock().unwrap() = None,
                            Effect::Nothing => {}
                        }
                        return Delivered::Sent;
                    }
                    Err(TrySendError::Full(back)) => event = back,
                    Err(TrySendError::Disconnected(_)) => return Delivered::Closed,
                }
            }
            std::thread::sleep(pause);
            pause = (pause * 2).min(Duration::from_millis(20));
        }
    }

    /// Hands a stream's events over in order, once each, acknowledging each once handed over
    /// (so a slow application holds the host back instead of piling events up here).
    fn follow(
        self: &Arc<Self>,
        link: &Arc<Link>,
        stream: StreamId,
        receiver: Receiver<StreamItem>,
        generation: u64,
    ) {
        let me = self.clone();
        let link = link.clone();
        std::thread::spawn(move || {
            let mut parts = Parts::default();
            for StreamItem { seq, message } in receiver {
                let event = match translate(&message, &mut parts) {
                    Ok(event) => event,
                    Err(reason) => {
                        // Refused for this terminal only: told once, then let go. The link and
                        // the other terminals go on; nothing reconnects for it. The parts go
                        // first: up to 16 MiB need not wait for the application to read.
                        drop(std::mem::take(&mut parts));
                        if !me.reject(generation, reason) {
                            // A replaced stream: not this follower's terminal to give up.
                            link.unregister(&stream);
                        }
                        return;
                    }
                };
                if let Some(event) = event {
                    match me.deliver(generation, event) {
                        Delivered::Sent | Delivered::Skipped => {}
                        Delivered::Stale => {
                            link.unregister(&stream);
                            return;
                        }
                        // The application dropped its receiver: let go of the terminal.
                        Delivered::Closed => return me.abandon(),
                    }
                }
                link.ack(&stream, seq);
            }
            if !link.alive() {
                let _ = me.deliver(generation, TerminalEvent::Disconnected);
            }
        });
    }
}

impl Resume for Shared {
    fn resume(self: Arc<Self>, client: &Client, same_epoch: bool) {
        self.resume_arc(client, same_epoch);
    }
}

impl Shared {
    /// Called by the supervisor on each new link. The generation starts here, in the order
    /// of the links; the rest runs on a thread of its own, so an application slow to read
    /// this terminal's events never holds back the other sessions.
    pub(crate) fn resume_arc(self: &Arc<Self>, client: &Client, same_epoch: bool) {
        if *self.lost.lock().unwrap() {
            return;
        }
        // Refused once the application let go, in the same critical section as letting go.
        let Some(generation) = self.next_generation() else {
            return;
        };
        let link = client.link().ok();
        let me = self.clone();
        std::thread::spawn(move || me.reattach(link, generation, same_epoch));
    }

    fn reattach(self: &Arc<Self>, link: Option<Arc<Link>>, generation: u64, same_epoch: bool) {
        // The replaced stream may have stopped before saying it: `Disconnected` always comes
        // before `Reattached` or `Lost`.
        match self.deliver(generation, TerminalEvent::Disconnected) {
            Delivered::Sent | Delivered::Skipped => {}
            Delivered::Stale => return,
            Delivered::Closed => return self.abandon(),
        }
        if !same_epoch {
            *self.lost.lock().unwrap() = true;
            let _ = self.deliver(generation, TerminalEvent::Lost);
            return;
        }
        let Some(link) = link else { return };
        let had_lease = self.lease.lock().unwrap().is_some();
        match self.request_attach(&link, self.writer(), self.after()) {
            Ok(attached) => {
                if self.current(generation) {
                    self.start(&link, &attached, generation, had_lease, true);
                } else {
                    self.detach(&link, attached.stream);
                }
            }
            Err(ClientError::Refused(e))
                if e.code == ErrorCode::NotFound || e.code == ErrorCode::EpochMismatch =>
            {
                *self.lost.lock().unwrap() = true;
                let _ = self.deliver(generation, TerminalEvent::Lost);
            }
            Err(_) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A resumption that saw the terminal still held, then starts its generation after the
    /// application let go, is refused: both happen under the delivery lock, in one order or the
    /// other, never interleaved.
    #[test]
    fn letting_go_and_starting_a_generation_never_interleave() {
        let mut delivery = Delivery::new(0);
        let first = delivery.begin().unwrap();
        assert!(delivery.admits(first));
        assert!(delivery.abandon().is_some());
        assert!(!delivery.admits(first), "the current generation stops");
        assert_eq!(delivery.begin(), None, "no generation starts after");
        assert!(
            !delivery.admits(delivery.generation),
            "and none is current, whatever its number"
        );
        assert!(
            delivery.abandon().is_none(),
            "letting go twice does nothing"
        );
    }

    #[test]
    fn a_replaced_generation_cannot_let_go_of_its_successor() {
        let mut delivery = Delivery::new(0);
        let old = delivery.begin().unwrap();
        let current = delivery.begin().unwrap();
        assert_eq!(
            delivery.abandon_current(old),
            None,
            "the old follower is stale"
        );
        assert!(!delivery.abandoned);
        assert!(delivery.admits(current), "the successor goes on");
        assert!(delivery.abandon_current(current).is_some());
        assert!(delivery.abandoned);
    }

    #[test]
    fn letting_go_hands_back_the_stream_to_stop_once() {
        let mut delivery = Delivery::new(0);
        delivery.begin().unwrap();
        let stream = StreamId::from_bytes([1; 16]);
        delivery.stream = Some(stream.clone());
        assert_eq!(delivery.abandon(), Some(Some(stream)));
        assert_eq!(delivery.stream, None);
        assert_eq!(delivery.abandon(), None);
    }
}
