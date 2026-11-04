//! Gate 7: the client library against the real daemon, through the real bridge as its
//! connector, with the link cut by killing the bridge, and the daemon restarted.
#![cfg(target_os = "linux")]
mod common;

use common::{EXE, Home, WAIT, alive};
use std::collections::BTreeMap;
use std::sync::mpsc::Receiver;
use std::time::{Duration, Instant};
use tessel_host_client::editor::{Editor, SaveError};
use tessel_host_client::git::{Git, GitEvent};
use tessel_host_client::terminal::{Terminal, TerminalEvent};
use tessel_host_client::tree::{Tree, TreeEvent};
use tessel_host_client::{Client, CommandConnector, ConnectionState, Options};
use tessel_host_protocol::pty::{AttachMode, Size};
use tessel_host_protocol::{HostInfo, RawMessage, RelPath, ReplyMessage};

fn connector(home: &Home) -> CommandConnector {
    CommandConnector::new(EXE, ["connect", "--stdio"]).env("HOME", &home.path)
}

fn connect(home: &mut Home) -> (Client, CommandConnector) {
    let connector = connector(home);
    let client = Client::connect(Box::new(connector.clone()), Options::default()).unwrap();
    let info = daemon(&client);
    home.daemons.push(info.pid);
    (client, connector)
}

fn daemon(client: &Client) -> HostInfo {
    let reply = client.request(RawMessage::empty("host.info")).unwrap();
    match ReplyMessage::from_raw(&reply).unwrap() {
        Some(ReplyMessage::HostInfo(info)) => info,
        other => panic!("{other:?}"),
    }
}

/// Cuts the link without warning, as a dropped SSH channel does.
fn cut(connector: &CommandConnector) {
    let pid = connector.current_pid().unwrap();
    // SAFETY: the bridge process this test started.
    unsafe { libc::kill(pid as i32, libc::SIGKILL) };
}

fn wait_for(what: &str, mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + WAIT;
    while !done() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn text_until(
    events: &Receiver<TerminalEvent>,
    text: &mut Vec<u8>,
    seen: &mut Vec<TerminalEvent>,
    until: impl Fn(&[u8], &[TerminalEvent]) -> bool,
) {
    let deadline = Instant::now() + WAIT;
    while !until(text, seen) {
        let left = deadline.saturating_duration_since(Instant::now());
        assert!(
            !left.is_zero(),
            "timed out; got {} bytes, events {:?}",
            text.len(),
            seen.iter()
                .filter(|e| !matches!(e, TerminalEvent::Output { .. }))
                .collect::<Vec<_>>()
        );
        if let Ok(event) = events.recv_timeout(left) {
            if let TerminalEvent::Output { data, .. } = &event {
                text.extend_from_slice(data);
            } else {
                seen.push(event);
            }
        }
    }
}

fn contains(text: &[u8], needle: &str) -> bool {
    text.windows(needle.len()).any(|w| w == needle.as_bytes())
}

#[test]
fn a_cut_link_reconnects_and_the_terminal_continues_exactly() {
    let mut home = Home::new("clientcut");
    let (client, connector) = connect(&mut home);
    let script = "for i in $(seq 1 3000); do echo \"line $i\"; [ $((i % 150)) -eq 0 ] && sleep 0.1; done; cat";
    let (terminal, events) = Terminal::create(
        &client,
        vec!["/bin/sh".into(), "-c".into(), script.into()],
        None,
        BTreeMap::new(),
        Size { rows: 24, cols: 80 },
    )
    .unwrap();
    let epoch = client.epoch().unwrap();
    let (mut text, mut seen) = (Vec::new(), Vec::new());
    text_until(&events, &mut text, &mut seen, |t, _| {
        contains(t, "line 300\r\n")
    });
    cut(&connector);
    text_until(&events, &mut text, &mut seen, |_, s| {
        s.iter()
            .any(|e| matches!(e, TerminalEvent::Reattached { .. }))
    });
    assert!(seen.contains(&TerminalEvent::Disconnected), "{seen:?}");
    assert!(
        seen.contains(&TerminalEvent::Reattached {
            mode: AttachMode::Replay,
            lease: true
        }),
        "{seen:?}"
    );
    assert_eq!(client.state(), ConnectionState::Connected { epoch });
    text_until(&events, &mut text, &mut seen, |t, _| {
        contains(t, "line 3000\r\n")
    });
    let expected: String = (1..=3000).map(|i| format!("line {i}\r\n")).collect();
    assert_eq!(
        String::from_utf8_lossy(&text),
        expected,
        "no line twice, none missing"
    );
    // Still the lease holder after the reconnect: typing works, exactly once.
    terminal.input(b"typed once\n").unwrap();
    text_until(&events, &mut text, &mut seen, |t, _| {
        t.windows(10).filter(|w| *w == b"typed once").count() >= 2
    });
    std::thread::sleep(Duration::from_millis(300));
    while let Ok(TerminalEvent::Output { data, .. }) = events.try_recv() {
        text.extend_from_slice(&data);
    }
    assert_eq!(
        text.windows(10).filter(|w| *w == b"typed once").count(),
        2,
        "the echo and cat's copy"
    );
    client.close();
}

#[test]
fn a_restarted_daemon_is_explicit_and_sessions_start_over() {
    let mut home = Home::new("clientrestart");
    let project = home.path.join("project");
    std::fs::create_dir_all(project.join("src")).unwrap();
    std::fs::write(project.join("a.txt"), b"a").unwrap();
    let git_ok = std::process::Command::new("bash")
        .args(["-ec", "git init -q -b main; git add -A; git -c user.name=T -c user.email=t@example.invalid commit -qm one"])
        .current_dir(&project)
        .env("HOME", &home.path)
        .status()
        .unwrap()
        .success();
    assert!(git_ok);
    let (client, _connector) = connect(&mut home);
    let root = project.to_string_lossy().into_owned();
    let (terminal, events) = Terminal::create(
        &client,
        vec!["cat".into()],
        None,
        BTreeMap::new(),
        Size { rows: 24, cols: 80 },
    )
    .unwrap();
    let (tree, tree_events) = Tree::open(&client, &root).unwrap();
    tree.expand(&RelPath::root()).unwrap();
    let (_git, git_events) = Git::open(&client, &root, &RelPath::root()).unwrap();
    // The first git event carries the whole status.
    let first = git_events.recv_timeout(WAIT).unwrap();
    assert!(
        matches!(first, GitEvent::Changed(ref c) if c.reset),
        "{first:?}"
    );

    let old = daemon(&client);
    let previous = client.epoch().unwrap();
    // SAFETY: the daemon this test started.
    unsafe { libc::kill(old.pid as i32, libc::SIGKILL) };
    wait_for("the restarted state", || {
        matches!(client.state(), ConnectionState::Restarted { .. })
    });
    let ConnectionState::Restarted {
        previous: was,
        current,
    } = client.state()
    else {
        unreachable!()
    };
    assert_eq!(was, previous);
    assert_ne!(current, previous);
    home.daemons.push(daemon(&client).pid);

    let (mut text, mut seen) = (Vec::new(), Vec::new());
    text_until(&events, &mut text, &mut seen, |_, s| {
        s.contains(&TerminalEvent::Lost)
    });
    assert!(alive(daemon(&client).pid));
    let _ = terminal;
    // The tree opened its root again and listed what was expanded.
    let deadline = Instant::now() + WAIT;
    loop {
        match tree_events.recv_timeout(deadline.saturating_duration_since(Instant::now())) {
            Ok(TreeEvent::Relisted(listing)) => {
                assert!(listing.path.is_root());
                assert!(
                    listing
                        .entries
                        .iter()
                        .any(|e| e.name.as_bytes() == b"a.txt")
                );
                break;
            }
            Ok(_) => {}
            Err(_) => panic!("no relisting after the restart"),
        }
    }
    // Git opened the repository again: the next status starts over.
    let deadline = Instant::now() + WAIT;
    loop {
        match git_events.recv_timeout(deadline.saturating_duration_since(Instant::now())) {
            Ok(GitEvent::Changed(change)) if change.reset => break,
            Ok(_) => {}
            Err(_) => panic!("no fresh git status after the restart"),
        }
    }
    client.close();
}

#[test]
fn a_save_cut_by_the_link_is_sent_again_and_a_conflict_is_never_silent() {
    let mut home = Home::new("clientsave");
    let project = home.path.join("project");
    std::fs::create_dir_all(&project).unwrap();
    std::fs::write(project.join("doc.txt"), b"version one\n").unwrap();
    let (client, connector) = connect(&mut home);
    let editor = Editor::open(&client, &project.to_string_lossy()).unwrap();
    let path = RelPath::from_components(vec![b"doc.txt".to_vec()]).unwrap();
    let copy = editor.read(&path).unwrap();
    assert_eq!(copy.bytes, b"version one\n");

    // The link drops before the save: the dirty buffer is sent whole once reconnected.
    cut(&connector);
    let edited = vec![b'x'; 200_000];
    let written = editor.save(&path, Some(copy.base()), &edited).unwrap();
    assert_eq!(std::fs::read(project.join("doc.txt")).unwrap(), edited);
    assert_eq!(written.size, 200_000);

    // Changed on the host after this read: a conflict, the host's file untouched.
    let mine = editor.read(&path).unwrap();
    std::fs::write(project.join("doc.txt"), b"someone else\n").unwrap();
    assert_eq!(
        editor
            .save(&path, Some(mine.base()), b"mine\n")
            .unwrap_err(),
        SaveError::Conflict
    );
    assert_eq!(
        std::fs::read(project.join("doc.txt")).unwrap(),
        b"someone else\n"
    );
    client.close();
}
