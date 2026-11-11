//! One live connection to a host daemon over any byte stream (an SSH exec channel running
//! `tessel-host connect --stdio`, a local process, a test pipe). It says hello, matches replies
//! to requests, routes stream events to whoever registered the stream, and reports its end.
use crate::ClientError;
use std::collections::{HashMap, HashSet, VecDeque};
use std::io::{self, Read, Write};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender, SyncSender, channel, sync_channel};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tessel_host_protocol::{
    Ack, Budgets, ClientFrame, ClientMessage, Hello, HelloOk, HostFrame, MAX_FRAME_BYTES,
    RawMessage, ReplyMessage, StreamId, VERSION, read_frame, write_frame,
};

/// Events of a stream nobody claimed yet (its reply is still on its way to the requester).
const MAX_ORPHAN_EVENTS: usize = 4096;
const MAX_ORPHAN_STREAMS: usize = 64;
/// Streams let go of that are remembered, so their late events are dropped (the last ones).
const MAX_GONE_STREAMS: usize = 1024;
/// Events delivered to a stream's owner but not consumed yet: beyond, the link waits, the
/// acknowledgments stop, and the host holds the stream back.
pub(crate) const STREAM_BUFFER: usize = 256;

/// Opens a fresh byte stream to `tessel-host connect --stdio`. Called again for each reconnect.
pub trait Connector: Send + Sync {
    fn connect(&self) -> io::Result<Pipe>;
}

/// Both directions of one connection, and whatever must live as long as it (a child process,
/// killed when the pipe is dropped).
pub struct Pipe {
    pub reader: Box<dyn Read + Send>,
    pub writer: Box<dyn Write + Send>,
    pub guard: Option<Box<dyn Send>>,
}

/// One event of a stream, with its stream sequence number (for the acknowledgment).
pub struct StreamItem {
    pub seq: u64,
    pub message: RawMessage,
}

type Pending = HashMap<u64, Sender<Result<RawMessage, ClientError>>>;

/// What the opening thread hands back: the agreement and both directions of the stream.
type Opened = Result<(HelloOk, Box<dyn Read + Send>, Box<dyn Write + Send>), ClientError>;

/// One connection being opened, so another thread can abandon it (`Client::close` during a
/// reconnection): its guard is dropped, which ends a connect, read or write blocked on it.
#[derive(Default)]
pub struct Opening {
    cancelled: AtomicBool,
    guard: Mutex<Option<Box<dyn Send>>>,
}

impl Opening {
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::SeqCst);
        drop(self.guard.lock().unwrap().take());
    }

    pub fn cancelled(&self) -> bool {
        self.cancelled.load(Ordering::SeqCst)
    }
}

/// Says hello and reads the answer, on the opening thread.
fn handshake(
    reader: &mut Box<dyn Read + Send>,
    writer: &mut Box<dyn Write + Send>,
    hello: Hello,
) -> Result<HelloOk, ClientError> {
    write_frame(
        writer,
        &ClientFrame::request(1, ClientMessage::Hello(hello)),
        MAX_FRAME_BYTES,
    )
    .map_err(|e| ClientError::Io(e.to_string()))?;
    writer.flush().map_err(|e| ClientError::Io(e.to_string()))?;
    let reply = read_frame::<HostFrame>(reader, MAX_FRAME_BYTES)
        .map_err(|e| ClientError::Io(e.to_string()))?
        .ok_or(ClientError::ConnectionLost)?;
    if let Some(error) = reply.error {
        return Err(ClientError::Refused(error));
    }
    match reply.ok.as_ref().map(ReplyMessage::from_raw) {
        Some(Ok(Some(ReplyMessage::HelloOk(agreed)))) if reply.reply_to == Some(1) => Ok(agreed),
        _ => Err(ClientError::Protocol("no hello_ok".into())),
    }
}

/// Streams let go of (`unregister`): what the host still sends on them (events already on their
/// way when it was told to stop) is dropped, never kept as orphans, where it would hold memory
/// and orphan slots for the life of the link and could crowd out a new stream's first events.
#[derive(Default)]
struct Gone {
    order: VecDeque<StreamId>,
    set: HashSet<StreamId>,
}

impl Gone {
    fn insert(&mut self, stream: &StreamId) {
        if self.set.insert(stream.clone()) {
            self.order.push_back(stream.clone());
            if self.order.len() > MAX_GONE_STREAMS
                && let Some(oldest) = self.order.pop_front()
            {
                self.set.remove(&oldest);
            }
        }
    }

    fn remove(&mut self, stream: &StreamId) {
        if self.set.remove(stream) {
            self.order.retain(|s| s != stream);
        }
    }
}

pub struct Link {
    writer: Mutex<Box<dyn Write + Send>>,
    pending: Mutex<Pending>,
    routes: Mutex<HashMap<StreamId, SyncSender<StreamItem>>>,
    orphans: Mutex<HashMap<StreamId, VecDeque<StreamItem>>>,
    gone: Mutex<Gone>,
    next_id: AtomicU64,
    alive: AtomicBool,
    pub hello: HelloOk,
    frame_limit: usize,
    closed: Mutex<Vec<Sender<()>>>,
    _guard: Mutex<Option<Box<dyn Send>>>,
}

impl Link {
    /// Connects and says hello within `timeout`. `Err` if the host refused it, the stream failed,
    /// nothing came in time (`Timeout`: an SSH channel can stay up with a daemon that never
    /// answers), or `opening` was cancelled meanwhile (`ConnectionLost`).
    ///
    /// Connecting and the hello run on a thread of their own, so waiting for them is bounded
    /// whatever the connector does. Giving up drops the connection's guard, which must end its
    /// stream (`CommandConnector` kills its process); the thread then ends too.
    pub fn open(
        connector: Arc<dyn Connector>,
        capabilities: &[&str],
        budgets: Budgets,
        timeout: Duration,
        opening: Arc<Opening>,
    ) -> Result<Arc<Self>, ClientError> {
        let hello = Hello {
            versions: vec![VERSION],
            capabilities: capabilities.iter().map(|c| c.to_string()).collect(),
            budgets,
        };
        let (sender, receiver) = channel::<Opened>();
        let worker = opening.clone();
        std::thread::spawn(move || {
            let Pipe {
                mut reader,
                mut writer,
                guard,
            } = match connector.connect() {
                Ok(pipe) => pipe,
                Err(e) => {
                    let _ = sender.send(Err(ClientError::Io(e.to_string())));
                    return;
                }
            };
            {
                let mut slot = worker.guard.lock().unwrap();
                if worker.cancelled() {
                    // Abandoned while connecting: the stream goes with its guard, here.
                    return;
                }
                *slot = guard;
            }
            let result = handshake(&mut reader, &mut writer, hello);
            let _ = sender.send(result.map(|agreed| (agreed, reader, writer)));
        });

        let deadline = Instant::now() + timeout;
        let opened = loop {
            if opening.cancelled() {
                opening.cancel();
                return Err(ClientError::ConnectionLost);
            }
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                opening.cancel();
                return Err(ClientError::Timeout);
            }
            match receiver.recv_timeout(left.min(Duration::from_millis(50))) {
                Ok(opened) => break opened,
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => {
                    opening.cancel();
                    return Err(ClientError::ConnectionLost);
                }
            }
        };
        let (agreed, reader, writer) = match opened {
            Ok(opened) => opened,
            Err(error) => {
                opening.cancel();
                return Err(error);
            }
        };
        let guard = opening.guard.lock().unwrap().take();
        if opening.cancelled() {
            // Cancelled between the hello and here: its guard is already gone.
            drop(guard);
            return Err(ClientError::ConnectionLost);
        }
        let frame_limit = agreed.budgets.frame_bytes as usize;
        let link = Arc::new(Self {
            writer: Mutex::new(writer),
            pending: Mutex::new(HashMap::new()),
            routes: Mutex::new(HashMap::new()),
            orphans: Mutex::new(HashMap::new()),
            gone: Mutex::new(Gone::default()),
            next_id: AtomicU64::new(2),
            alive: AtomicBool::new(true),
            hello: agreed,
            frame_limit,
            closed: Mutex::new(Vec::new()),
            _guard: Mutex::new(guard),
        });
        let reading = link.clone();
        std::thread::spawn(move || reading.read_loop(reader));
        Ok(link)
    }

    /// Ends this connection: whatever it owns (the bridge process) goes, so its reader ends.
    pub fn shutdown(&self) {
        drop(self._guard.lock().unwrap().take());
    }

    pub fn alive(&self) -> bool {
        self.alive.load(Ordering::SeqCst)
    }

    /// A receiver that gets a message (or is dropped) when the link ends.
    pub fn on_close(&self) -> Receiver<()> {
        let (sender, receiver) = channel();
        let mut closed = self.closed.lock().unwrap();
        if self.alive() {
            closed.push(sender);
        }
        receiver
    }

    fn read_loop(self: Arc<Self>, mut reader: Box<dyn Read + Send>) {
        while let Ok(Some(frame)) = read_frame::<HostFrame>(&mut reader, self.frame_limit) {
            if let Some(id) = frame.reply_to {
                let result = match (frame.ok, frame.error) {
                    (Some(ok), _) => Ok(ok),
                    (_, Some(error)) => Err(ClientError::Refused(error)),
                    _ => Err(ClientError::Protocol("empty reply".into())),
                };
                if let Some(waiter) = self.pending.lock().unwrap().remove(&id) {
                    let _ = waiter.send(result);
                }
                continue;
            }
            if let (Some(stream), Some(message)) = (frame.stream, frame.event) {
                self.route(
                    stream.id,
                    StreamItem {
                        seq: stream.seq,
                        message,
                    },
                );
                continue;
            }
            // A connection error: the host closes after it.
            break;
        }
        self.alive.store(false, Ordering::SeqCst);
        for (_, waiter) in self.pending.lock().unwrap().drain() {
            let _ = waiter.send(Err(ClientError::ConnectionLost));
        }
        {
            // Under the orphans lock, as `register` claims: a stream claimed from now on gets a
            // receiver that has already ended, never a route nobody will feed.
            let _orphans = self.orphans.lock().unwrap();
            self.routes.lock().unwrap().clear();
        }
        for closed in self.closed.lock().unwrap().drain(..) {
            let _ = closed.send(());
        }
    }

    fn route(&self, stream: StreamId, item: StreamItem) {
        // Orphans first, then routes, here and in register: a stream claimed meanwhile is seen.
        let mut orphans = self.orphans.lock().unwrap();
        let sender = self.routes.lock().unwrap().get(&stream).cloned();
        match sender {
            Some(sender) => {
                drop(orphans);
                // Blocks while its owner has not consumed: acknowledgments stop, the host waits.
                let _ = sender.send(item);
            }
            None => {
                // Let go of: what is still on its way is dropped.
                if self.gone.lock().unwrap().set.contains(&stream) {
                    return;
                }
                if orphans.len() >= MAX_ORPHAN_STREAMS && !orphans.contains_key(&stream) {
                    return;
                }
                let queue = orphans.entry(stream).or_default();
                if queue.len() < MAX_ORPHAN_EVENTS {
                    queue.push_back(item);
                }
            }
        }
    }

    /// Claims a stream: events that came before the claim are delivered first, in order. On a
    /// link that ended, the receiver ends at once (its follower sees the end, not a silence).
    pub fn register(&self, stream: &StreamId) -> Receiver<StreamItem> {
        let mut orphans = self.orphans.lock().unwrap();
        if !self.alive() {
            let (_, ended) = sync_channel(0);
            return ended;
        }
        // Claimed again (a daemon may answer with an id seen before): live from now on.
        self.gone.lock().unwrap().remove(stream);
        let early = orphans.remove(stream).unwrap_or_default();
        let (sender, receiver) = sync_channel(STREAM_BUFFER + early.len());
        for item in early {
            let _ = sender.try_send(item);
        }
        self.routes.lock().unwrap().insert(stream.clone(), sender);
        drop(orphans);
        receiver
    }

    /// Lets go of a stream: its owner's receiver ends once it took what it holds, and whatever
    /// the host still sends on it is dropped. Under the orphans lock, as in `route`, so an event
    /// arriving meanwhile goes either to the route (ending next) or nowhere, never to orphans.
    pub fn unregister(&self, stream: &StreamId) {
        let mut orphans = self.orphans.lock().unwrap();
        self.routes.lock().unwrap().remove(stream);
        orphans.remove(stream);
        self.gone.lock().unwrap().insert(stream);
    }

    pub fn ack(&self, stream: &StreamId, seq: u64) {
        let frame = ClientFrame::ack(Ack {
            stream: stream.clone(),
            seq,
        });
        let mut writer = self.writer.lock().unwrap();
        let _ = write_frame(&mut *writer, &frame, self.frame_limit).and_then(|()| writer.flush());
    }

    /// One request and its reply.
    pub fn request(
        &self,
        message: RawMessage,
        timeout: Duration,
    ) -> Result<RawMessage, ClientError> {
        self.send(message)?.wait(timeout)
    }

    /// Writes a request now and hands back its reply to wait for later (or never: dropping it
    /// ignores the reply). The host answers a connection's requests in the order written, so a
    /// caller that decides and writes under one lock keeps its decisions in order.
    pub(crate) fn send(&self, message: RawMessage) -> Result<Reply<'_>, ClientError> {
        if !self.alive() {
            return Err(ClientError::ConnectionLost);
        }
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let (sender, receiver) = channel();
        self.pending.lock().unwrap().insert(id, sender);
        let mut writer = self.writer.lock().unwrap();
        let sent = write_frame(
            &mut *writer,
            &ClientFrame::request_raw(id, message),
            self.frame_limit,
        )
        .and_then(|()| writer.flush());
        if sent.is_err() {
            // The other end is gone: no request goes through this link any more, even
            // before its reader sees the end.
            self.alive.store(false, Ordering::SeqCst);
            self.pending.lock().unwrap().remove(&id);
            return Err(ClientError::ConnectionLost);
        }
        Ok(Reply {
            link: self,
            id,
            receiver,
        })
    }
}

/// The reply to a request already written (`Link::send`).
pub(crate) struct Reply<'a> {
    link: &'a Link,
    id: u64,
    receiver: Receiver<Result<RawMessage, ClientError>>,
}

impl Drop for Reply<'_> {
    fn drop(&mut self) {
        self.link.pending.lock().unwrap().remove(&self.id);
    }
}

impl Reply<'_> {
    pub(crate) fn wait(self, timeout: Duration) -> Result<RawMessage, ClientError> {
        match self.receiver.recv_timeout(timeout) {
            Ok(result) => result,
            Err(RecvTimeoutError::Timeout) => {
                self.link.pending.lock().unwrap().remove(&self.id);
                Err(ClientError::Timeout)
            }
            Err(RecvTimeoutError::Disconnected) => Err(ClientError::ConnectionLost),
        }
    }
}
