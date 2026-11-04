//! One live connection to a host daemon over any byte stream (an SSH exec channel running
//! `tessel-host connect --stdio`, a local process, a test pipe). It says hello, matches replies
//! to requests, routes stream events to whoever registered the stream, and reports its end.
use crate::ClientError;
use std::collections::{HashMap, VecDeque};
use std::io::{self, Read, Write};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, Sender, SyncSender, channel, sync_channel};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tessel_host_protocol::{
    Ack, Budgets, ClientFrame, ClientMessage, Hello, HelloOk, HostFrame, MAX_FRAME_BYTES,
    RawMessage, ReplyMessage, StreamId, VERSION, read_frame, write_frame,
};

/// Events of a stream nobody claimed yet (its reply is still on its way to the requester).
const MAX_ORPHAN_EVENTS: usize = 4096;
const MAX_ORPHAN_STREAMS: usize = 64;
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

pub struct Link {
    writer: Mutex<Box<dyn Write + Send>>,
    pending: Mutex<Pending>,
    routes: Mutex<HashMap<StreamId, SyncSender<StreamItem>>>,
    orphans: Mutex<HashMap<StreamId, VecDeque<StreamItem>>>,
    next_id: AtomicU64,
    alive: AtomicBool,
    pub hello: HelloOk,
    frame_limit: usize,
    closed: Mutex<Vec<Sender<()>>>,
    _guard: Mutex<Option<Box<dyn Send>>>,
}

impl Link {
    /// Connects and says hello; `Err` if the host refused it or the stream failed.
    pub fn open(
        connector: &dyn Connector,
        capabilities: &[&str],
        budgets: Budgets,
    ) -> Result<Arc<Self>, ClientError> {
        let Pipe {
            mut reader,
            mut writer,
            guard,
        } = connector
            .connect()
            .map_err(|e| ClientError::Io(e.to_string()))?;
        let hello = Hello {
            versions: vec![VERSION],
            capabilities: capabilities.iter().map(|c| c.to_string()).collect(),
            budgets,
        };
        write_frame(
            &mut writer,
            &ClientFrame::request(1, ClientMessage::Hello(hello)),
            MAX_FRAME_BYTES,
        )
        .map_err(|e| ClientError::Io(e.to_string()))?;
        writer.flush().map_err(|e| ClientError::Io(e.to_string()))?;
        let reply = read_frame::<HostFrame>(&mut reader, MAX_FRAME_BYTES)
            .map_err(|e| ClientError::Io(e.to_string()))?
            .ok_or(ClientError::ConnectionLost)?;
        if let Some(error) = reply.error {
            return Err(ClientError::Refused(error));
        }
        let agreed = match reply.ok.as_ref().map(ReplyMessage::from_raw) {
            Some(Ok(Some(ReplyMessage::HelloOk(agreed)))) if reply.reply_to == Some(1) => agreed,
            _ => return Err(ClientError::Protocol("no hello_ok".into())),
        };
        let frame_limit = agreed.budgets.frame_bytes as usize;
        let link = Arc::new(Self {
            writer: Mutex::new(writer),
            pending: Mutex::new(HashMap::new()),
            routes: Mutex::new(HashMap::new()),
            orphans: Mutex::new(HashMap::new()),
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
        if self.alive() {
            self.closed.lock().unwrap().push(sender);
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
        self.routes.lock().unwrap().clear();
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

    /// Claims a stream: events that came before the claim are delivered first, in order.
    pub fn register(&self, stream: &StreamId) -> Receiver<StreamItem> {
        let mut orphans = self.orphans.lock().unwrap();
        let early = orphans.remove(stream).unwrap_or_default();
        let (sender, receiver) = sync_channel(STREAM_BUFFER + early.len());
        for item in early {
            let _ = sender.try_send(item);
        }
        self.routes.lock().unwrap().insert(stream.clone(), sender);
        drop(orphans);
        receiver
    }

    pub fn unregister(&self, stream: &StreamId) {
        self.routes.lock().unwrap().remove(stream);
        self.orphans.lock().unwrap().remove(stream);
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
        if !self.alive() {
            return Err(ClientError::ConnectionLost);
        }
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let (sender, receiver) = channel();
        self.pending.lock().unwrap().insert(id, sender);
        {
            let mut writer = self.writer.lock().unwrap();
            let sent = write_frame(
                &mut *writer,
                &ClientFrame::request_raw(id, message),
                self.frame_limit,
            )
            .and_then(|()| writer.flush());
            if sent.is_err() {
                self.pending.lock().unwrap().remove(&id);
                return Err(ClientError::ConnectionLost);
            }
        }
        match receiver.recv_timeout(timeout) {
            Ok(result) => result,
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                self.pending.lock().unwrap().remove(&id);
                Err(ClientError::Timeout)
            }
            Err(_) => Err(ClientError::ConnectionLost),
        }
    }
}
