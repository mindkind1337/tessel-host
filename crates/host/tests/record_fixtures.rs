//! Records the cross-client fixtures in `fixtures/sessions/` from real sessions with the daemon
//! (not run by default: `cargo test -p tessel-host --test record_fixtures -- --ignored`). Every
//! frame both ways, in the order the client saw them, with what changes from run to run made
//! stable: random identifiers and digests numbered, process ids, times and git object ids of
//! commits fixed. `tessel-host-protocol/tests/sessions.rs` checks them on every CI run.
#![cfg(target_os = "linux")]
mod common;

use common::{EXE, Home};
use serde_json::{Value, json};
use std::collections::{BTreeMap, HashMap};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tessel_host_client::editor::{Editor, SaveError};
use tessel_host_client::git::{Git, GitEvent};
use tessel_host_client::terminal::{Terminal, TerminalEvent};
use tessel_host_client::tree::{Tree, TreeEvent};
use tessel_host_client::{Client, CommandConnector, Connector, Options, Pipe};
use tessel_host_protocol::pty::Size;
use tessel_host_protocol::{RawMessage, RelPath};

type Log = Arc<Mutex<Vec<(&'static str, Vec<u8>)>>>;

/// Splits a byte stream into frames and logs each whole frame.
struct Framer {
    from: &'static str,
    buffer: Vec<u8>,
    log: Log,
}

impl Framer {
    fn feed(&mut self, bytes: &[u8]) {
        self.buffer.extend_from_slice(bytes);
        while self.buffer.len() >= 4 {
            let length = u32::from_be_bytes(self.buffer[..4].try_into().unwrap()) as usize;
            if self.buffer.len() < 4 + length {
                break;
            }
            let payload = self.buffer[4..4 + length].to_vec();
            self.buffer.drain(..4 + length);
            self.log.lock().unwrap().push((self.from, payload));
        }
    }
}

struct TeeRead(Box<dyn Read + Send>, Framer);
impl Read for TeeRead {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let n = self.0.read(buf)?;
        self.1.feed(&buf[..n]);
        Ok(n)
    }
}

struct TeeWrite(Box<dyn Write + Send>, Framer);
impl Write for TeeWrite {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let n = self.0.write(buf)?;
        self.1.feed(&buf[..n]);
        Ok(n)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.0.flush()
    }
}

struct Recording {
    inner: CommandConnector,
    log: Log,
}

impl Connector for Recording {
    fn connect(&self) -> io::Result<Pipe> {
        let pipe = self.inner.connect()?;
        Ok(Pipe {
            reader: Box::new(TeeRead(
                pipe.reader,
                Framer {
                    from: "host",
                    buffer: Vec::new(),
                    log: self.log.clone(),
                },
            )),
            writer: Box::new(TeeWrite(
                pipe.writer,
                Framer {
                    from: "client",
                    buffer: Vec::new(),
                    log: self.log.clone(),
                },
            )),
            guard: pipe.guard,
        })
    }
}

/// Makes what changes from run to run stable, keeping every value valid for its type.
struct Stable {
    home: String,
    ids: HashMap<String, String>,
    digests: HashMap<String, String>,
}

impl Stable {
    fn token(&mut self, value: &str) -> Option<String> {
        let hex = |s: &str| s.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'));
        if value.len() == 32 && hex(value) {
            let next = self.ids.len() + 1;
            return Some(
                self.ids
                    .entry(value.to_owned())
                    .or_insert_with(|| format!("{next:032x}"))
                    .clone(),
            );
        }
        if value.len() == 64 && hex(value) {
            let next = self.digests.len() + 1;
            return Some(
                self.digests
                    .entry(value.to_owned())
                    .or_insert_with(|| format!("{:064x}", 0xd1_0000 + next))
                    .clone(),
            );
        }
        None
    }

    fn walk(&mut self, key: &str, value: &mut Value) {
        match value {
            Value::String(text) => {
                if let Some(rest) = text.strip_prefix(self.home.as_str()) {
                    *text = format!("/home/user{rest}");
                } else if let Some(stable) = self.token(text) {
                    *text = stable;
                } else if key == "oid" {
                    *text = "0".repeat(text.len());
                } else if key == "record" {
                    // Object ids inside a porcelain record are content hashes: stable already.
                } else if key == "git_version" {
                    *text = "git version 2.43.0".into();
                }
            }
            Value::Number(_)
                if matches!(
                    key,
                    "pid"
                        | "mtime_ms"
                        | "duration_ms"
                        | "daemon_rss_bytes"
                        | "last_full_ms"
                        | "last_partial_ms"
                ) =>
            {
                *value = json!(if key == "pid" {
                    4242
                } else if key == "mtime_ms" {
                    1_700_000_000_000u64
                } else {
                    0
                });
            }
            Value::Array(items) => items.iter_mut().for_each(|v| self.walk(key, v)),
            Value::Object(map) => {
                for (k, v) in map.iter_mut() {
                    self.walk(k, v);
                }
            }
            _ => {}
        }
    }
}

fn write_fixture(name: &str, log: &Log, home: &Home) {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/sessions");
    std::fs::create_dir_all(&dir).unwrap();
    let mut stable = Stable {
        home: home.path.to_string_lossy().into_owned(),
        ids: HashMap::new(),
        digests: HashMap::new(),
    };
    let mut out = String::new();
    for (from, payload) in log.lock().unwrap().iter() {
        let mut frame: Value = serde_json::from_slice(payload).unwrap();
        stable.walk("", &mut frame);
        out.push_str(&serde_json::to_string(&json!({ "from": from, "frame": frame })).unwrap());
        out.push('\n');
    }
    std::fs::write(dir.join(format!("{name}.jsonl")), out).unwrap();
}

fn client(home: &mut Home) -> (Client, Log) {
    let log: Log = Arc::new(Mutex::new(Vec::new()));
    let inner = CommandConnector::new(EXE, ["connect", "--stdio"]).env("HOME", &home.path);
    let client = Client::connect(
        Box::new(Recording {
            inner,
            log: log.clone(),
        }),
        Options::default(),
    )
    .unwrap();
    let info = client.request(RawMessage::empty("host.info")).unwrap();
    if let Ok(Some(tessel_host_protocol::ReplyMessage::HostInfo(info))) =
        tessel_host_protocol::ReplyMessage::from_raw(&info)
    {
        home.daemons.push(info.pid);
    }
    (client, log)
}

fn settle() {
    std::thread::sleep(Duration::from_millis(400));
}

fn git_env(dir: &Path, home: &Path, script: &str) {
    let ok = std::process::Command::new("bash")
        .args(["-ec", script])
        .current_dir(dir)
        .env("HOME", home)
        .env("GIT_AUTHOR_NAME", "Fixture")
        .env("GIT_AUTHOR_EMAIL", "fixture@example.invalid")
        .env("GIT_COMMITTER_NAME", "Fixture")
        .env("GIT_COMMITTER_EMAIL", "fixture@example.invalid")
        .env("GIT_AUTHOR_DATE", "2026-01-01T00:00:00Z")
        .env("GIT_COMMITTER_DATE", "2026-01-01T00:00:00Z")
        .status()
        .unwrap()
        .success();
    assert!(ok);
}

#[test]
#[ignore]
fn record() {
    // hello, host.info, a terminal: output, input, exit.
    let mut home = Home::new("fixtures-terminal");
    let (c, log) = client(&mut home);
    let (terminal, events) = Terminal::create(
        &c,
        vec![
            "/bin/sh".into(),
            "-c".into(),
            "printf 'hello\\n'; read line; echo \"got $line\"".into(),
        ],
        None,
        BTreeMap::new(),
        Size { rows: 24, cols: 80 },
    )
    .unwrap();
    let mut text = Vec::new();
    while !text.windows(6).any(|w| w == b"hello\n" || w == b"hello\r") {
        if let Ok(TerminalEvent::Output { data, .. }) = events.recv_timeout(Duration::from_secs(10))
        {
            text.extend_from_slice(&data);
        }
    }
    terminal.input(b"yes\n").unwrap();
    while !matches!(
        events.recv_timeout(Duration::from_secs(10)),
        Ok(TerminalEvent::Exit(_))
    ) {}
    settle();
    write_fixture("terminal", &log, &home);
    c.close();

    // A file tree: open, subscribe, expand, a file created.
    let mut home = Home::new("fixtures-tree");
    let project = home.path.join("project");
    std::fs::create_dir_all(project.join("src")).unwrap();
    std::fs::write(project.join("README.md"), b"# fixture\n").unwrap();
    let (c, log) = client(&mut home);
    let (tree, events) = Tree::open(&c, &project.to_string_lossy()).unwrap();
    tree.expand(&RelPath::root()).unwrap();
    std::fs::write(project.join("new.txt"), b"new\n").unwrap();
    while !matches!(
        events.recv_timeout(Duration::from_secs(10)),
        Ok(TreeEvent::Changed(_))
    ) {}
    settle();
    write_fixture("tree", &log, &home);
    c.close();

    // The editor: read, save, then a conflict.
    let mut home = Home::new("fixtures-editor");
    let project = home.path.join("project");
    std::fs::create_dir_all(&project).unwrap();
    std::fs::write(project.join("notes.txt"), b"first\n").unwrap();
    let (c, log) = client(&mut home);
    let editor = Editor::open(&c, &project.to_string_lossy()).unwrap();
    let path = RelPath::from_components(vec![b"notes.txt".to_vec()]).unwrap();
    let copy = editor.read(&path).unwrap();
    editor.save(&path, Some(copy.base()), b"second\n").unwrap();
    std::fs::write(project.join("notes.txt"), b"someone else\n").unwrap();
    assert_eq!(
        editor
            .save(&path, Some(copy.base()), b"third\n")
            .unwrap_err(),
        SaveError::Conflict
    );
    settle();
    write_fixture("editor", &log, &home);
    c.close();

    // Git: open, subscribe (the whole status), an edit (a difference).
    let mut home = Home::new("fixtures-git");
    let project = home.path.join("project");
    std::fs::create_dir_all(&project).unwrap();
    git_env(
        &project,
        &home.path,
        "git init -q -b main; printf 'a\\n' > a.txt; git add -A; git commit -qm one; printf 'u\\n' > untracked.txt",
    );
    let (c, log) = client(&mut home);
    let (_git, events) = Git::open(&c, &project.to_string_lossy(), &RelPath::root()).unwrap();
    while !matches!(events.recv_timeout(Duration::from_secs(10)), Ok(GitEvent::Changed(ref ch)) if ch.reset)
    {
    }
    std::fs::write(project.join("a.txt"), b"a, edited\n").unwrap();
    while !matches!(events.recv_timeout(Duration::from_secs(10)), Ok(GitEvent::Changed(ref ch)) if !ch.reset && !ch.upsert.is_empty())
    {
    }
    settle();
    write_fixture("git", &log, &home);
    c.close();
}
