//! Client of the Tessel host daemon, for the desktop (Rust now, the same sequences for Vue
//! later; see GUIDE-CLIENT.md). A [`Client`] keeps one link to the daemon over a byte stream a
//! [`Connector`] opens (typically `ssh host <installed tessel-host> connect --stdio`), and opens
//! another when it breaks. On the same daemon (same epoch) every session resumes where it was:
//! terminals reattach after their last applied event, trees and git subscribe again. On a
//! restarted daemon (another epoch) the client says so, terminals report they are gone, and
//! trees, editors and git open again by path. Every state is explicit ([`ConnectionState`]);
//! nothing is presented as live when it is not.
mod command;
mod delivery;
pub mod editor;
pub mod git;
mod link;
pub mod terminal;
pub mod tree;

pub use command::CommandConnector;
pub use link::{Connector, Link, Opening, Pipe, StreamItem};

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, Weak};
use std::time::{Duration, Instant};
use tessel_host_protocol::{Budgets, Epoch, HelloOk, RawMessage, WireError};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ClientError {
    /// The host answered with this refusal.
    Refused(WireError),
    /// The link ended before the answer; the client is reconnecting.
    ConnectionLost,
    Timeout,
    /// The host restarted: what was known by its old epoch is gone.
    EpochChanged,
    Protocol(String),
    Io(String),
}

impl std::fmt::Display for ClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Refused(error) => write!(f, "refused: {error}"),
            Self::ConnectionLost => f.write_str("connection lost"),
            Self::Timeout => f.write_str("timed out"),
            Self::EpochChanged => f.write_str("the host restarted"),
            Self::Protocol(why) => write!(f, "protocol error: {why}"),
            Self::Io(why) => write!(f, "{why}"),
        }
    }
}

impl std::error::Error for ClientError {}

impl ClientError {
    pub fn code(&self) -> Option<tessel_host_protocol::ErrorCode> {
        match self {
            Self::Refused(error) => Some(error.code),
            _ => None,
        }
    }
}

/// What the desktop shows about the connection.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ConnectionState {
    Connected {
        epoch: Epoch,
    },
    /// The link broke; trying again (attempt number, from 1).
    Reconnecting {
        attempt: u32,
    },
    /// Connected to a daemon that restarted since: terminals of `previous` are gone.
    Restarted {
        previous: Epoch,
        current: Epoch,
    },
    /// Closed by the application.
    Closed,
}

#[derive(Clone, Debug)]
pub struct Options {
    pub capabilities: Vec<&'static str>,
    pub budgets: Budgets,
    /// How long one request may wait for its answer.
    pub request_timeout: Duration,
    /// How long one connection attempt may take, from opening the stream to the daemon's hello
    /// (signing in over SSH and starting the daemon included). Beyond, the attempt is abandoned
    /// and its process stopped: `Client::connect` answers `Timeout`, a reconnection tries again.
    pub connect_timeout: Duration,
    /// Longest pause between two reconnection attempts.
    pub max_backoff: Duration,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            capabilities: vec!["pty", "tree", "editor", "git"],
            budgets: Budgets::SLOW_HOST,
            request_timeout: Duration::from_secs(30),
            connect_timeout: Duration::from_secs(20),
            max_backoff: Duration::from_secs(5),
        }
    }
}

/// A session that knows how to continue on a new link.
pub(crate) trait Resume: Send + Sync {
    /// `same_epoch`: the same daemon (resume); otherwise a restarted one (start over or report).
    fn resume(self: Arc<Self>, client: &Client, same_epoch: bool);
}

pub(crate) struct Inner {
    folders: Mutex<tree::Folders>,
    connector: Arc<dyn Connector>,
    options: Options,
    link: Mutex<Option<Arc<Link>>>,
    /// The reconnection attempt in progress, so `close` can abandon it.
    opening: Mutex<Option<Arc<Opening>>>,
    state: Mutex<ConnectionState>,
    changed: Condvar,
    sessions: Mutex<Vec<Weak<dyn Resume>>>,
    closed: AtomicBool,
}

/// A connection to one host daemon that reconnects by itself. Cheap to clone.
#[derive(Clone)]
pub struct Client {
    pub(crate) inner: Arc<Inner>,
}

impl Client {
    /// Connects once within `options.connect_timeout` (an error here is returned, not retried),
    /// then keeps the link up.
    pub fn connect(connector: Box<dyn Connector>, options: Options) -> Result<Self, ClientError> {
        Self::connect_cancellable(connector, options, Arc::new(Opening::default()))
    }

    /// As `connect`, and `opening.cancel()` from another thread (a "Cancel" button) abandons
    /// the first connection at once: its process is stopped and this answers `ConnectionLost`.
    /// Once connected, cancelling `opening` changes nothing; `close` ends the client.
    pub fn connect_cancellable(
        connector: Box<dyn Connector>,
        options: Options,
        opening: Arc<Opening>,
    ) -> Result<Self, ClientError> {
        let connector: Arc<dyn Connector> = Arc::from(connector);
        let link = Link::open(
            connector.clone(),
            &options.capabilities,
            options.budgets,
            options.connect_timeout,
            opening,
        )?;
        let epoch = link.hello.host_epoch.clone();
        let inner = Arc::new(Inner {
            folders: Mutex::new(tree::Folders::default()),
            connector,
            options,
            link: Mutex::new(Some(link)),
            opening: Mutex::new(None),
            state: Mutex::new(ConnectionState::Connected { epoch }),
            changed: Condvar::new(),
            sessions: Mutex::new(Vec::new()),
            closed: AtomicBool::new(false),
        });
        let client = Self { inner };
        let supervisor = client.clone();
        std::thread::spawn(move || supervisor.supervise());
        Ok(client)
    }

    pub fn state(&self) -> ConnectionState {
        self.inner.state.lock().unwrap().clone()
    }

    /// What the daemon agreed to on the current link.
    pub fn hello(&self) -> Option<HelloOk> {
        self.inner
            .link
            .lock()
            .unwrap()
            .as_ref()
            .map(|l| l.hello.clone())
    }

    pub fn epoch(&self) -> Option<Epoch> {
        self.hello().map(|h| h.host_epoch)
    }

    /// The live link, or `ConnectionLost` while reconnecting.
    pub fn link(&self) -> Result<Arc<Link>, ClientError> {
        match &*self.inner.link.lock().unwrap() {
            Some(link) if link.alive() => Ok(link.clone()),
            _ => Err(ClientError::ConnectionLost),
        }
    }

    pub fn options(&self) -> &Options {
        &self.inner.options
    }

    pub(crate) fn folders(&self) -> std::sync::MutexGuard<'_, tree::Folders> {
        self.inner.folders.lock().unwrap()
    }

    pub fn request(&self, message: RawMessage) -> Result<RawMessage, ClientError> {
        self.link()?
            .request(message, self.inner.options.request_timeout)
    }

    pub fn ask<T: serde::de::DeserializeOwned>(
        &self,
        kind: &str,
        body: &impl serde::Serialize,
    ) -> Result<T, ClientError> {
        let reply = self.request(RawMessage::of(kind, body))?;
        reply.body_as().map_err(ClientError::Refused)
    }

    /// Waits until a live link exists (on whatever epoch), or `timeout`. A link that just broke
    /// counts as broken even before the state says so.
    pub fn wait_connected(&self, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        loop {
            if self.inner.closed.load(Ordering::SeqCst) {
                return false;
            }
            if self.link().is_ok() {
                return true;
            }
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return false;
            }
            let state = self.inner.state.lock().unwrap();
            let _ = self
                .inner
                .changed
                .wait_timeout(state, left.min(Duration::from_millis(50)))
                .unwrap();
        }
    }

    /// Runs `call` until it answers, waiting through reconnections (a call must be safe to
    /// repeat: the host deduplicates those with an effect).
    pub(crate) fn retrying<T>(
        &self,
        mut call: impl FnMut() -> Result<T, ClientError>,
    ) -> Result<T, ClientError> {
        let deadline = Instant::now() + self.inner.options.request_timeout;
        loop {
            match call() {
                Err(ClientError::ConnectionLost) if Instant::now() < deadline => {
                    if !self.wait_connected(deadline.saturating_duration_since(Instant::now())) {
                        return Err(ClientError::ConnectionLost);
                    }
                }
                other => return other,
            }
        }
    }

    pub(crate) fn register(&self, session: Weak<dyn Resume>) {
        let mut sessions = self.inner.sessions.lock().unwrap();
        sessions.retain(|s| s.strong_count() > 0);
        sessions.push(session);
    }

    /// Ends the link for good: the connection's process (if any) is stopped, a reconnection in
    /// progress is abandoned (its process stopped too), and the client does not reconnect.
    /// Sessions on the host go on.
    pub fn close(&self) {
        // Set first: the supervisor checks it under each lock taken below.
        self.inner.closed.store(true, Ordering::SeqCst);
        if let Some(opening) = self.inner.opening.lock().unwrap().take() {
            opening.cancel();
        }
        if let Some(link) = self.inner.link.lock().unwrap().take() {
            link.shutdown();
        }
        *self.inner.state.lock().unwrap() = ConnectionState::Closed;
        self.inner.changed.notify_all();
    }

    fn closed(&self) -> bool {
        self.inner.closed.load(Ordering::SeqCst)
    }

    /// A closed client stays `Closed`.
    fn set_state(&self, state: ConnectionState) {
        let mut current = self.inner.state.lock().unwrap();
        if self.closed() {
            return;
        }
        *current = state;
        drop(current);
        self.inner.changed.notify_all();
    }

    /// One connection attempt that `close` can abandon.
    fn open_link(&self) -> Result<Arc<Link>, ClientError> {
        let opening = Arc::new(Opening::default());
        {
            let mut slot = self.inner.opening.lock().unwrap();
            if self.closed() {
                return Err(ClientError::ConnectionLost);
            }
            *slot = Some(opening.clone());
        }
        let result = Link::open(
            self.inner.connector.clone(),
            &self.inner.options.capabilities,
            self.inner.options.budgets,
            self.inner.options.connect_timeout,
            opening.clone(),
        );
        let mut slot = self.inner.opening.lock().unwrap();
        if slot.as_ref().is_some_and(|o| Arc::ptr_eq(o, &opening)) {
            *slot = None;
        }
        result
    }

    /// Waits `pause`, or less if the client is closed meanwhile.
    fn backoff(&self, pause: Duration) {
        let state = self.inner.state.lock().unwrap();
        let _ = self
            .inner
            .changed
            .wait_timeout_while(state, pause, |_| !self.closed())
            .unwrap();
    }

    fn supervise(self) {
        loop {
            let Some(link) = self.inner.link.lock().unwrap().clone() else {
                return;
            };
            let _ = link.on_close().recv();
            if self.closed() {
                return;
            }
            let previous = link.hello.host_epoch.clone();
            let mut attempt = 0u32;
            let mut pause = Duration::from_millis(100);
            let fresh = loop {
                if self.closed() {
                    return;
                }
                attempt += 1;
                self.set_state(ConnectionState::Reconnecting { attempt });
                match self.open_link() {
                    Ok(fresh) => break fresh,
                    Err(_) => {
                        if self.closed() {
                            return;
                        }
                        self.backoff(pause);
                        pause = (pause * 2).min(self.inner.options.max_backoff);
                    }
                }
            };
            let current = fresh.hello.host_epoch.clone();
            let same = current == previous;
            {
                let mut slot = self.inner.link.lock().unwrap();
                if self.closed() {
                    // Closed while this link opened: it never becomes the client's.
                    drop(slot);
                    fresh.shutdown();
                    return;
                }
                *slot = Some(fresh);
            }
            self.set_state(if same {
                ConnectionState::Connected { epoch: current }
            } else {
                ConnectionState::Restarted { previous, current }
            });
            let sessions: Vec<Arc<dyn Resume>> = self
                .inner
                .sessions
                .lock()
                .unwrap()
                .iter()
                .filter_map(Weak::upgrade)
                .collect();
            for session in sessions {
                session.resume(&self, same);
            }
        }
    }
}
