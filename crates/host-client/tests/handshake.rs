//! Opening a link is bounded, and closing the client abandons an opening in progress. Fake
//! daemons over in-process pipes (no host, no SSH): one that never answers, one that sends half
//! a frame, one that answers and then drops the link. Runs on Linux and on Windows.
use std::io::{self, PipeReader, PipeWriter, Read, Write};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tessel_host_client::{Client, ClientError, ConnectionState, Connector, Opening, Options, Pipe};
use tessel_host_protocol::{
    Budgets, ClientFrame, Epoch, HelloOk, HostFrame, MAX_FRAME_BYTES, ReplyMessage, VERSION,
    read_frame, write_frame,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Daemon {
    /// The stream is up, nothing ever comes.
    Silent,
    /// The length of a frame and a few of its bytes, then nothing.
    Partial,
    /// Answers the hello, then drops the link a moment later.
    AnswersThenDrops,
    /// Answers the hello and stays.
    Answers,
    /// The stream cannot be opened.
    Refused,
}

/// Set when a connection's guard is dropped: its process would be stopped.
struct Stopped(Arc<AtomicBool>);

impl Drop for Stopped {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

struct Guard {
    /// Both ends a silent daemon holds: what it would write, and what the client wrote.
    _daemon_end: Option<(PipeWriter, PipeReader)>,
    _stopped: Stopped,
}

/// Connection `n` behaves as `plan[n]` (the last one repeats).
struct Fake {
    plan: Vec<Daemon>,
    attempts: AtomicUsize,
    stopped: Mutex<Vec<Arc<AtomicBool>>>,
}

impl Fake {
    fn new(plan: &[Daemon]) -> Arc<Self> {
        Arc::new(Self {
            plan: plan.to_vec(),
            attempts: AtomicUsize::new(0),
            stopped: Mutex::new(Vec::new()),
        })
    }

    fn attempts(&self) -> usize {
        self.attempts.load(Ordering::SeqCst)
    }

    /// Whether connection `n`'s guard was dropped.
    fn stopped(&self, n: usize) -> bool {
        self.stopped.lock().unwrap()[n].load(Ordering::SeqCst)
    }
}

struct Shared(Arc<Fake>);

impl Connector for Shared {
    fn connect(&self) -> io::Result<Pipe> {
        let fake = &self.0;
        let n = fake.attempts.fetch_add(1, Ordering::SeqCst);
        let daemon = fake.plan[n.min(fake.plan.len() - 1)];
        let stopped = Arc::new(AtomicBool::new(false));
        fake.stopped.lock().unwrap().push(stopped.clone());
        if daemon == Daemon::Refused {
            return Err(io::Error::other("refused"));
        }
        let (client_reader, mut daemon_writer) = io::pipe()?;
        let (mut daemon_reader, client_writer) = io::pipe()?;
        let daemon_end = match daemon {
            Daemon::Silent => Some((daemon_writer, daemon_reader)),
            Daemon::Partial => {
                daemon_writer.write_all(&100u32.to_be_bytes())?;
                daemon_writer.write_all(br#"{"version":1,"#)?;
                Some((daemon_writer, daemon_reader))
            }
            Daemon::AnswersThenDrops | Daemon::Answers => {
                std::thread::spawn(move || {
                    let Ok(Some(hello)) =
                        read_frame::<ClientFrame>(&mut daemon_reader, MAX_FRAME_BYTES)
                    else {
                        return;
                    };
                    let agreed = HelloOk {
                        version: VERSION,
                        capabilities: Vec::new(),
                        budgets: Budgets::SLOW_HOST,
                        host_epoch: Epoch::from_bytes([7; 16]),
                    };
                    let reply = HostFrame::reply_ok(
                        hello.request_id.unwrap_or(1),
                        ReplyMessage::HelloOk(agreed).into_raw(),
                    );
                    if write_frame(&mut daemon_writer, &reply, MAX_FRAME_BYTES)
                        .and_then(|()| daemon_writer.flush())
                        .is_err()
                    {
                        return;
                    }
                    if daemon == Daemon::AnswersThenDrops {
                        std::thread::sleep(Duration::from_millis(100));
                        drop(daemon_writer);
                    } else {
                        // Stays until the client's end goes.
                        let mut sink = [0u8; 1024];
                        while matches!(daemon_reader.read(&mut sink), Ok(n) if n > 0) {}
                    }
                });
                None
            }
            Daemon::Refused => unreachable!(),
        };
        Ok(Pipe {
            reader: Box::new(client_reader),
            writer: Box::new(client_writer),
            guard: Some(Box::new(Guard {
                _daemon_end: daemon_end,
                _stopped: Stopped(stopped),
            })),
        })
    }
}

fn options(connect_timeout: Duration) -> Options {
    Options {
        connect_timeout,
        max_backoff: Duration::from_millis(400),
        ..Options::default()
    }
}

fn wait_until(what: &str, timeout: Duration, mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + timeout;
    while !condition() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn a_silent_daemon_times_out_and_its_connection_is_stopped() {
    let fake = Fake::new(&[Daemon::Silent]);
    let started = Instant::now();
    let result = Client::connect(
        Box::new(Shared(fake.clone())),
        options(Duration::from_millis(300)),
    );
    assert!(matches!(result, Err(ClientError::Timeout)));
    assert!(started.elapsed() < Duration::from_secs(3));
    assert!(fake.stopped(0));
}

#[test]
fn cancelling_the_first_connection_stops_it_at_once() {
    let fake = Fake::new(&[Daemon::Silent]);
    let opening = Arc::new(Opening::default());
    let cancel = opening.clone();
    let canceller = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(200));
        cancel.cancel();
    });
    let started = Instant::now();
    // Long enough that only the cancel can end it.
    let result = Client::connect_cancellable(
        Box::new(Shared(fake.clone())),
        options(Duration::from_secs(60)),
        opening,
    );
    canceller.join().unwrap();
    assert!(matches!(result, Err(ClientError::ConnectionLost)));
    assert!(started.elapsed() < Duration::from_secs(2));
    assert!(fake.stopped(0));
}

#[test]
fn an_opening_cancelled_before_it_starts_never_connects() {
    let fake = Fake::new(&[Daemon::Silent]);
    let opening = Arc::new(Opening::default());
    // Abandoned before the attempt begins (a close that races the connection's start).
    opening.cancel();
    let result = Client::connect_cancellable(
        Box::new(Shared(fake.clone())),
        options(Duration::from_secs(60)),
        opening,
    );
    assert!(matches!(result, Err(ClientError::ConnectionLost)));
    assert_eq!(fake.attempts(), 0, "a cancelled opening starts no process");
}

#[test]
fn half_a_hello_times_out_too() {
    let fake = Fake::new(&[Daemon::Partial]);
    let started = Instant::now();
    let result = Client::connect(
        Box::new(Shared(fake.clone())),
        options(Duration::from_millis(300)),
    );
    assert!(matches!(result, Err(ClientError::Timeout)));
    assert!(started.elapsed() < Duration::from_secs(3));
    assert!(fake.stopped(0));
}

#[test]
fn a_reconnection_that_times_out_is_stopped_and_tried_again() {
    let fake = Fake::new(&[Daemon::AnswersThenDrops, Daemon::Silent, Daemon::Answers]);
    let client = Client::connect(
        Box::new(Shared(fake.clone())),
        options(Duration::from_millis(300)),
    )
    .unwrap();
    wait_until("the third connection", Duration::from_secs(10), || {
        fake.attempts() >= 3 && client.link().is_ok()
    });
    assert!(fake.stopped(1), "the silent attempt's process is stopped");
    assert!(matches!(client.state(), ConnectionState::Connected { .. }));
    client.close();
}

#[test]
fn closing_during_a_reconnection_abandons_it() {
    let fake = Fake::new(&[Daemon::AnswersThenDrops, Daemon::Silent]);
    // Long enough that only closing can end the silent attempt.
    let client = Client::connect(
        Box::new(Shared(fake.clone())),
        options(Duration::from_secs(60)),
    )
    .unwrap();
    wait_until("the silent attempt", Duration::from_secs(5), || {
        fake.attempts() >= 2 && matches!(client.state(), ConnectionState::Reconnecting { .. })
    });
    let started = Instant::now();
    client.close();
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "close does not wait"
    );
    wait_until(
        "the attempt's process to stop",
        Duration::from_secs(2),
        || fake.stopped(1),
    );
    std::thread::sleep(Duration::from_millis(500));
    assert_eq!(client.state(), ConnectionState::Closed);
    assert_eq!(fake.attempts(), 2, "no attempt after close");
    assert!(matches!(client.link(), Err(ClientError::ConnectionLost)));
}

#[test]
fn closing_during_the_pause_between_attempts_stays_closed() {
    let fake = Fake::new(&[Daemon::AnswersThenDrops, Daemon::Refused]);
    let client = Client::connect(
        Box::new(Shared(fake.clone())),
        options(Duration::from_secs(5)),
    )
    .unwrap();
    wait_until("failed attempts", Duration::from_secs(5), || {
        fake.attempts() >= 3
    });
    client.close();
    let attempts = fake.attempts();
    std::thread::sleep(Duration::from_millis(900));
    assert_eq!(client.state(), ConnectionState::Closed);
    assert_eq!(fake.attempts(), attempts, "no attempt after close");
}
