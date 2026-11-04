//! A terminal on the host, seen from the desktop. Events come in order, each applied once:
//! output and resizes numbered by the terminal, a snapshot (all its parts joined) when history
//! was dropped, the exit once. When the link breaks the events say `Disconnected`; on the same
//! daemon the terminal reattaches after the last event it applied (`Reattached`), keeping its
//! lease; on a restarted daemon it is `Lost`. Input is numbered here and retried with the same
//! number through busy answers and reconnections, so it is typed exactly once.
use crate::link::{Link, StreamItem};
use crate::{Client, ClientError, Resume};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, SyncSender, sync_channel};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tessel_host_protocol::pty::{
    self, Attach, AttachMode, Attached, Close, Create, Created, Exit, Input, InputOk, LeaseLost,
    Output, Resize, Resized, Signal, SignalRequest, Size, Snapshot, Writer,
};
use tessel_host_protocol::{
    Chunk, DedupKey, Epoch, ErrorCode, LeaseKey, MAX_CHUNK_BYTES, TerminalId,
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
    /// The link broke; the terminal may still run on the host.
    Disconnected,
    /// Back on the same daemon: replay or snapshot follows.
    Reattached {
        mode: AttachMode,
        lease: bool,
    },
    /// The daemon restarted: this terminal is gone.
    Lost,
}

struct Shared {
    client: Client,
    terminal: TerminalId,
    epoch: Mutex<Epoch>,
    lease: Mutex<Option<LeaseKey>>,
    last: Mutex<u64>,
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

    /// Attaches to a terminal known from before (after `after`, its last applied event), as
    /// the lease holder when `lease` is given and still valid, else as a watcher.
    pub fn attach(
        client: &Client,
        terminal: TerminalId,
        epoch: Epoch,
        after: u64,
        lease: Option<LeaseKey>,
    ) -> Result<(Self, Receiver<TerminalEvent>), ClientError> {
        let (events, receiver) = sync_channel(crate::link::STREAM_BUFFER);
        let shared = Arc::new(Shared {
            client: client.clone(),
            terminal,
            epoch: Mutex::new(epoch),
            lease: Mutex::new(lease),
            last: Mutex::new(after),
            next_input: AtomicU64::new(1),
            events,
            lost: Mutex::new(false),
        });
        let attached = shared.attach(&client.link()?)?;
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

    /// The last event applied (to attach again later, from another run of the desktop).
    pub fn last_seq(&self) -> u64 {
        *self.shared.last.lock().unwrap()
    }

    fn lease_or_refuse(&self) -> Result<LeaseKey, ClientError> {
        self.lease()
            .ok_or(ClientError::Refused(tessel_host_protocol::WireError::new(
                ErrorCode::PermissionDenied,
            )))
    }

    /// Types `bytes` (split in 64 KiB inputs), each exactly once.
    pub fn input(&self, bytes: &[u8]) -> Result<(), ClientError> {
        for piece in bytes.chunks(MAX_CHUNK_BYTES) {
            let id = self.shared.next_input.fetch_add(1, Ordering::SeqCst);
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

    /// Takes the lease from whoever holds it.
    pub fn take_lease(&self) -> Result<(), ClientError> {
        let link = self.shared.client.link()?;
        let mut lease = self.shared.lease.lock().unwrap();
        let attached: Attached = link
            .request(
                tessel_host_protocol::RawMessage::of(
                    pty::kind::ATTACH,
                    &Attach {
                        terminal: self.shared.terminal.clone(),
                        epoch: self.shared.epoch.lock().unwrap().clone(),
                        after: self.last_seq(),
                        writer: Writer::Take,
                    },
                ),
                self.shared.client.options().request_timeout,
            )?
            .body_as()
            .map_err(ClientError::Refused)?;
        *lease = attached.lease.clone();
        drop(lease);
        // A second stream would duplicate events: this attachment replaces the first.
        self.shared.follow(&link, attached.stream);
        Ok(())
    }
}

impl Shared {
    fn attach(self: &Arc<Self>, link: &Arc<Link>) -> Result<Attached, ClientError> {
        let writer = match self.lease.lock().unwrap().clone() {
            Some(lease) => Writer::Resume(lease),
            None => Writer::None,
        };
        let request = Attach {
            terminal: self.terminal.clone(),
            epoch: self.epoch.lock().unwrap().clone(),
            after: *self.last.lock().unwrap(),
            writer,
        };
        let attached: Attached = link
            .request(
                tessel_host_protocol::RawMessage::of(pty::kind::ATTACH, &request),
                self.client.options().request_timeout,
            )?
            .body_as()
            .map_err(ClientError::Refused)?;
        let had = self.lease.lock().unwrap().is_some();
        if had && attached.lease.is_none() {
            *self.lease.lock().unwrap() = None;
            let _ = self.events.send(TerminalEvent::LeaseLost);
        }
        self.follow(link, attached.stream.clone());
        Ok(attached)
    }

    /// Delivers a stream's events in order, once each, acknowledging each once delivered.
    fn follow(self: &Arc<Self>, link: &Arc<Link>, stream: tessel_host_protocol::StreamId) {
        let receiver = link.register(&stream);
        let me = self.clone();
        let link = link.clone();
        std::thread::spawn(move || {
            let mut parts: Vec<Snapshot> = Vec::new();
            for StreamItem { seq, message } in receiver {
                if let Some(event) = me.translate(&message, &mut parts)
                    && me.events.send(event).is_err()
                {
                    break;
                }
                link.ack(&stream, seq);
            }
            if !link.alive() {
                let _ = me.events.send(TerminalEvent::Disconnected);
            }
        });
    }

    fn translate(
        &self,
        message: &tessel_host_protocol::RawMessage,
        parts: &mut Vec<Snapshot>,
    ) -> Option<TerminalEvent> {
        let mut last = self.last.lock().unwrap();
        match message.kind.as_str() {
            pty::kind::OUTPUT => {
                let output: Output = message.body_as().ok()?;
                if output.seq <= *last {
                    return None;
                }
                *last = output.seq;
                Some(TerminalEvent::Output {
                    seq: output.seq,
                    data: output.data.into_bytes(),
                })
            }
            pty::kind::RESIZED => {
                let resized: Resized = message.body_as().ok()?;
                if resized.seq <= *last {
                    return None;
                }
                *last = resized.seq;
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
                *last = first.at;
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
                *self.lease.lock().unwrap() = None;
                Some(TerminalEvent::LeaseLost)
            }
            _ => None,
        }
    }
}

impl Resume for Shared {
    fn resume(self: Arc<Self>, client: &Client, same_epoch: bool) {
        self.resume_arc(client, same_epoch);
    }
}

impl Shared {
    pub(crate) fn resume_arc(self: &Arc<Self>, client: &Client, same_epoch: bool) {
        if *self.lost.lock().unwrap() {
            return;
        }
        if !same_epoch {
            *self.lost.lock().unwrap() = true;
            let _ = self.events.send(TerminalEvent::Lost);
            return;
        }
        let Ok(link) = client.link() else { return };
        match self.attach(&link) {
            Ok(attached) => {
                let _ = self.events.send(TerminalEvent::Reattached {
                    mode: attached.mode,
                    lease: attached.lease.is_some(),
                });
            }
            Err(ClientError::Refused(e))
                if e.code == ErrorCode::NotFound || e.code == ErrorCode::EpochMismatch =>
            {
                *self.lost.lock().unwrap() = true;
                let _ = self.events.send(TerminalEvent::Lost);
            }
            Err(_) => {}
        }
    }
}
