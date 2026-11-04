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

/// One terminal. Dropping it detaches nothing on the host: the program goes on.
pub struct Terminal {
    shared: Arc<Shared>,
    pub pid: u32,
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
        let request = Create {
            dedup: random_key(),
            argv,
            cwd,
            env,
            size,
        };
        let created: Created = client.retrying(|| client.ask(pty::kind::CREATE, &request))?;
        let epoch = client.epoch().ok_or(ClientError::ConnectionLost)?;
        Self::attach(client, created.terminal, epoch, 0, Some(created.lease))
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
            delivery: Mutex::new(Delivery {
                generation: 0,
                last: after.unwrap_or(0),
                disconnected: false,
                exited: false,
                stream: None,
            }),
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

    pub fn lease(&self) -> Option<LeaseKey> {
        self.shared.lease.lock().unwrap().clone()
    }

    /// The last numbered event handed to the application (an output's or resize's `seq`, a
    /// snapshot's `at`). It is not what the application has applied yet: events can wait in
    /// its receiver. To attach again from another run of the desktop, keep the number of the
    /// last event the UI actually applied, and pass that as `after`.
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

    /// Takes the lease from whoever holds it. `Ok` means this client holds it now; events go
    /// on in order, from a new attachment that replaces the current one.
    pub fn take_lease(&self) -> Result<(), ClientError> {
        let shared = &self.shared;
        let link = shared.client.link()?;
        // The current stream stops handing events over now (its own lease_lost included); the
        // new attachment replays everything after the last event handed over.
        let generation = shared.next_generation();
        let replaced = shared.delivery.lock().unwrap().stream.clone();
        let result = shared.request_attach(&link, Writer::Take, shared.after());
        let attached = match result {
            Ok(attached) => {
                *shared.lease.lock().unwrap() = attached.lease.clone();
                shared.start(&link, &attached, generation, false, false);
                Ok(())
            }
            Err(error) => {
                // Back to following as before, on a new attachment of its own.
                let generation = shared.next_generation();
                let had_lease = shared.lease.lock().unwrap().is_some();
                if let Ok(attached) = shared.request_attach(&link, shared.writer(), shared.after())
                {
                    shared.start(&link, &attached, generation, had_lease, false);
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

/// What a stream's message means to the application, if anything (a snapshot part that is not
/// the last one is kept until the last arrives).
fn translate(message: &RawMessage, parts: &mut Vec<Snapshot>) -> Option<TerminalEvent> {
    match message.kind.as_str() {
        pty::kind::OUTPUT => {
            let output: Output = message.body_as().ok()?;
            Some(TerminalEvent::Output {
                seq: output.seq,
                data: output.data.into_bytes(),
            })
        }
        pty::kind::RESIZED => {
            let resized: Resized = message.body_as().ok()?;
            Some(TerminalEvent::Resized {
                seq: resized.seq,
                size: resized.size,
            })
        }
        pty::kind::SNAPSHOT => {
            let part: Snapshot = message.body_as().ok()?;
            let done = part.last;
            parts.push(part);
            if !done {
                return None;
            }
            let all = std::mem::take(parts);
            let first = &all[0];
            Some(TerminalEvent::Snapshot {
                at: first.at,
                size: first.size,
                alternate: first.alternate,
                resync: first.resync,
                data: all
                    .iter()
                    .flat_map(|p| p.data.as_bytes().to_vec())
                    .collect(),
            })
        }
        pty::kind::EXIT => message.body_as::<Exit>().ok().map(TerminalEvent::Exit),
        pty::kind::LEASE_LOST => {
            let _: LeaseLost = message.body_as().ok()?;
            Some(TerminalEvent::LeaseLost)
        }
        _ => None,
    }
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
    fn next_generation(&self) -> u64 {
        let mut delivery = self.delivery.lock().unwrap();
        delivery.generation += 1;
        delivery.generation
    }

    fn current(&self, generation: u64) -> bool {
        self.delivery.lock().unwrap().generation == generation
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
        let stale = |delivered| matches!(delivered, Delivered::Stale);
        if had_lease
            && attached.lease.is_none()
            && stale(self.deliver(generation, TerminalEvent::LeaseLost))
        {
            return self.detach(link, attached.stream.clone());
        }
        if reattached
            && stale(self.deliver(
                generation,
                TerminalEvent::Reattached {
                    mode: attached.mode,
                    lease: attached.lease.is_some(),
                },
            ))
        {
            return self.detach(link, attached.stream.clone());
        }
        {
            let mut delivery = self.delivery.lock().unwrap();
            if delivery.generation != generation {
                drop(delivery);
                return self.detach(link, attached.stream.clone());
            }
            delivery.stream = Some(attached.stream.clone());
        }
        self.follow(link, attached.stream.clone(), generation);
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
                if delivery.generation != generation {
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
                    TerminalEvent::Lost => Effect::Nothing,
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
    fn follow(self: &Arc<Self>, link: &Arc<Link>, stream: StreamId, generation: u64) {
        let receiver = link.register(&stream);
        let me = self.clone();
        let link = link.clone();
        std::thread::spawn(move || {
            let mut parts: Vec<Snapshot> = Vec::new();
            for StreamItem { seq, message } in receiver {
                if let Some(event) = translate(&message, &mut parts) {
                    match me.deliver(generation, event) {
                        Delivered::Sent | Delivered::Skipped => {}
                        Delivered::Stale | Delivered::Closed => {
                            link.unregister(&stream);
                            return;
                        }
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
        let generation = self.next_generation();
        let link = client.link().ok();
        let me = self.clone();
        std::thread::spawn(move || me.reattach(link, generation, same_epoch));
    }

    fn reattach(self: &Arc<Self>, link: Option<Arc<Link>>, generation: u64, same_epoch: bool) {
        // The replaced stream may have stopped before saying it: `Disconnected` always comes
        // before `Reattached` or `Lost`.
        if matches!(
            self.deliver(generation, TerminalEvent::Disconnected),
            Delivered::Stale | Delivered::Closed
        ) {
            return;
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
