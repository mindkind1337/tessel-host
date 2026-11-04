//! A home directory of its own, and a protocol client over the real bridge, for the tests.
#![allow(dead_code)]
use serde::Serialize;
use std::collections::VecDeque;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::mpsc::{Receiver, channel};
use std::time::{Duration, Instant};
use tessel_host_protocol::pty::{self, Exit, Output, Snapshot};
use tessel_host_protocol::*;

pub const EXE: &str = env!("CARGO_BIN_EXE_tessel-host");
pub const WAIT: Duration = Duration::from_secs(30);

pub struct Home {
    pub path: PathBuf,
    pub daemons: Vec<u32>,
}

impl Home {
    pub fn new(name: &str) -> Self {
        let path = std::env::temp_dir().join(format!("tessel-pty-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700)).unwrap();
        Self {
            path,
            daemons: Vec::new(),
        }
    }

    pub fn client(&mut self, budgets: Budgets) -> Client {
        let mut client = Client::open(self, budgets);
        let info = client.host_info();
        if !self.daemons.contains(&info.pid) {
            self.daemons.push(info.pid);
        }
        client
    }
}

impl Drop for Home {
    fn drop(&mut self) {
        for pid in &self.daemons {
            // SAFETY: a daemon this test started.
            unsafe { libc::kill(*pid as i32, libc::SIGTERM) };
        }
        // The daemon still writes its log while it hangs up its terminals: let it finish first.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
        // SAFETY: signal 0 only checks that the process exists.
        while self
            .daemons
            .iter()
            .any(|pid| unsafe { libc::kill(*pid as i32, 0) } == 0)
            && std::time::Instant::now() < deadline
        {
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

pub struct Client {
    pub child: Child,
    stdin: ChildStdin,
    frames: Receiver<HostFrame>,
    next: u64,
    pub epoch: Epoch,
    early: VecDeque<HostFrame>,
}

impl Client {
    fn open(home: &Home, budgets: Budgets) -> Self {
        let mut child = Command::new(EXE)
            .args(["connect", "--stdio"])
            .env("HOME", &home.path)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();
        let stdin = child.stdin.take().unwrap();
        let mut stdout = child.stdout.take().unwrap();
        let (sender, frames) = channel();
        std::thread::spawn(move || {
            while let Ok(Some(frame)) = read_frame::<HostFrame>(&mut stdout, MAX_FRAME_BYTES) {
                if sender.send(frame).is_err() {
                    break;
                }
            }
        });
        let mut client = Self {
            child,
            stdin,
            frames,
            next: 0,
            epoch: Epoch::from_bytes([0; 16]),
            early: VecDeque::new(),
        };
        let hello = Hello {
            versions: vec![VERSION],
            capabilities: vec!["pty".into()],
            budgets,
        };
        let ok = client.request("hello", &hello).expect("hello");
        let Some(ReplyMessage::HelloOk(agreed)) = ReplyMessage::from_raw(&ok).unwrap() else {
            panic!("{ok:?}")
        };
        client.epoch = agreed.host_epoch;
        client
    }

    pub fn host_info(&mut self) -> HostInfo {
        let ok = self.request_raw(RawMessage::empty("host.info")).unwrap();
        match ReplyMessage::from_raw(&ok).unwrap() {
            Some(ReplyMessage::HostInfo(info)) => info,
            other => panic!("{other:?}"),
        }
    }

    pub fn send(&mut self, kind: &str, body: &impl Serialize) -> u64 {
        self.next += 1;
        write_frame(
            &mut self.stdin,
            &ClientFrame::request_raw(self.next, RawMessage::of(kind, body)),
            MAX_FRAME_BYTES,
        )
        .unwrap();
        self.next
    }

    pub fn request(&mut self, kind: &str, body: &impl Serialize) -> Result<RawMessage, WireError> {
        let id = self.send(kind, body);
        self.reply(id)
    }

    pub fn request_raw(&mut self, message: RawMessage) -> Result<RawMessage, WireError> {
        self.next += 1;
        write_frame(
            &mut self.stdin,
            &ClientFrame::request_raw(self.next, message),
            MAX_FRAME_BYTES,
        )
        .unwrap();
        self.reply(self.next)
    }

    pub fn ask<T: serde::de::DeserializeOwned>(&mut self, kind: &str, body: &impl Serialize) -> T {
        let ok = self
            .request(kind, body)
            .unwrap_or_else(|e| panic!("{kind}: {e}"));
        ok.body_as().unwrap()
    }

    fn reply(&mut self, id: u64) -> Result<RawMessage, WireError> {
        let deadline = Instant::now() + WAIT;
        loop {
            let frame = self
                .frames
                .recv_timeout(deadline.saturating_duration_since(Instant::now()))
                .expect("a reply");
            if frame.reply_to == Some(id) {
                return match (frame.ok, frame.error) {
                    (Some(ok), _) => Ok(ok),
                    (_, Some(error)) => Err(error),
                    _ => panic!("an empty reply"),
                };
            }
            assert!(frame.event.is_some(), "an unexpected frame: {frame:?}");
            self.early.push_back(frame);
        }
    }

    /// The next event. A reply nobody waits for (a request sent with `send`) is skipped.
    pub fn event(&mut self, wait: Duration) -> Option<HostFrame> {
        if let Some(frame) = self.early.pop_front() {
            return Some(frame);
        }
        let deadline = Instant::now() + wait;
        loop {
            let frame = self
                .frames
                .recv_timeout(deadline.saturating_duration_since(Instant::now()))
                .ok()?;
            if frame.event.is_some() {
                return Some(frame);
            }
        }
    }

    pub fn ack(&mut self, stream: &StreamId, seq: u64) {
        write_frame(
            &mut self.stdin,
            &ClientFrame::ack(Ack {
                stream: stream.clone(),
                seq,
            }),
            MAX_FRAME_BYTES,
        )
        .unwrap();
    }

    pub fn kill(mut self) {
        self.child.kill().unwrap();
        self.child.wait().unwrap();
    }
}

/// What a terminal looks like from the client: each output applied once, in order.
pub struct Viewer {
    pub stream: Option<StreamId>,
    receiver: StreamReceiver,
    pub last: u64,
    pub text: Vec<u8>,
    pub screen: vt100::Parser,
    parts: Vec<Snapshot>,
    pub snapshots: Vec<Snapshot>,
    pub exit: Option<Exit>,
    pub acking: bool,
    pub last_stream_seq: u64,
}

impl Viewer {
    pub fn new() -> Self {
        Self {
            stream: None,
            receiver: StreamReceiver::default(),
            last: 0,
            text: Vec::new(),
            screen: vt100::Parser::new(24, 80, 0),
            parts: Vec::new(),
            snapshots: Vec::new(),
            exit: None,
            acking: true,
            last_stream_seq: 0,
        }
    }

    /// A new attachment: its own stream, numbered from 1.
    pub fn attached(&mut self, stream: StreamId) {
        self.stream = Some(stream);
        self.receiver = StreamReceiver::default();
    }

    pub fn text(&self) -> String {
        String::from_utf8_lossy(&self.text).into_owned()
    }

    fn apply(&mut self, client: &mut Client, frame: HostFrame) {
        let pos = frame.stream.expect("an event");
        assert_eq!(
            Some(&pos.id),
            self.stream.as_ref(),
            "an event of another stream"
        );
        assert_eq!(
            self.receiver.accept(pos.seq),
            Arrival::Next,
            "stream events in order, once"
        );
        self.last_stream_seq = pos.seq;
        let event = frame.event.unwrap();
        match event.kind.as_str() {
            pty::kind::OUTPUT => {
                let output: Output = event.body_as().unwrap();
                assert_eq!(output.seq, self.last + 1, "no output twice, none missing");
                self.last = output.seq;
                self.text.extend_from_slice(output.data.as_bytes());
                self.screen.process(output.data.as_bytes());
            }
            pty::kind::SNAPSHOT => {
                let part: Snapshot = event.body_as().unwrap();
                assert_eq!(part.part as usize, self.parts.len());
                let last = part.last;
                self.parts.push(part);
                if last {
                    let parts = std::mem::take(&mut self.parts);
                    let first = &parts[0];
                    self.screen = vt100::Parser::new(first.size.rows, first.size.cols, 0);
                    for p in &parts {
                        self.screen.process(p.data.as_bytes());
                    }
                    assert!(first.at >= self.last, "a snapshot never goes back");
                    self.last = first.at;
                    self.snapshots.push(first.clone());
                }
            }
            pty::kind::EXIT => {
                let exit: Exit = event.body_as().unwrap();
                assert_eq!(exit.seq, self.last);
                self.exit = Some(exit);
            }
            other => panic!("unexpected event {other}"),
        }
        if self.acking {
            client.ack(&pos.id, pos.seq);
        }
    }

    /// Applies events until `done`, or fails after WAIT.
    pub fn until(&mut self, client: &mut Client, done: impl Fn(&Viewer) -> bool) {
        let deadline = Instant::now() + WAIT;
        while !done(self) {
            let left = deadline.saturating_duration_since(Instant::now());
            assert!(
                !left.is_zero(),
                "timed out; got {} bytes, last {}",
                self.text.len(),
                self.last
            );
            if let Some(frame) = client.event(left) {
                self.apply(client, frame);
            }
        }
    }

    /// Applies what arrives within `wait`.
    pub fn drain(&mut self, client: &mut Client, wait: Duration) {
        while let Some(frame) = client.event(wait) {
            self.apply(client, frame);
        }
    }
}

pub fn alive(pid: u32) -> bool {
    // SAFETY: signal 0 only checks that the process exists.
    unsafe { libc::kill(pid as i32, 0) == 0 }
}
