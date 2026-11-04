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

/// The application reads nothing for a while: its receiver fills up and the terminal's
/// follower waits with an event in hand, while the link (whose own buffer still has room) sees
/// the cut and reconnects. Then the application reads everything: one `Disconnected`, then
/// `Reattached`, then the rest; every output once, in order, and nothing from the replaced
/// stream after `Reattached`.
#[test]
fn a_cut_while_the_application_is_not_reading_keeps_every_event_in_order() {
    let mut home = Home::new("clientsat");
    let (client, connector) = connect(&mut home);
    // One line every 3 ms: about one output event each, more than the application's receiver
    // holds (256), fewer than that plus the link's buffer (256 more).
    let script =
        "i=1; while [ $i -le 400 ]; do echo \"line $i\"; i=$((i+1)); sleep 0.003; done; cat";
    let (_terminal, events) = Terminal::create(
        &client,
        vec!["/bin/sh".into(), "-c".into(), script.into()],
        None,
        BTreeMap::new(),
        Size { rows: 24, cols: 80 },
    )
    .unwrap();
    let epoch = client.epoch().unwrap();
    // Not reading: the receiver fills while the program prints.
    std::thread::sleep(Duration::from_secs(3));
    cut(&connector);
    wait_for("the reconnection", || {
        client.link().is_ok()
            && client.state()
                == ConnectionState::Connected {
                    epoch: epoch.clone(),
                }
    });
    // Let the replaced follower and the reattachment race for the receiver a moment.
    std::thread::sleep(Duration::from_millis(300));

    let mut all = Vec::new();
    let mut text = Vec::new();
    let deadline = Instant::now() + WAIT;
    while !contains(&text, "line 400\r\n") {
        let left = deadline.saturating_duration_since(Instant::now());
        assert!(!left.is_zero(), "timed out; {} bytes", text.len());
        if let Ok(event) = events.recv_timeout(left) {
            if let TerminalEvent::Output { data, .. } = &event {
                text.extend_from_slice(data);
            }
            all.push(event);
        }
    }
    let got = String::from_utf8_lossy(&text);
    let lines: Vec<&str> = got.split("\r\n").filter(|l| !l.is_empty()).collect();
    let misplaced = lines
        .iter()
        .enumerate()
        .find(|(i, line)| **line != format!("line {}", i + 1));
    assert!(
        misplaced.is_none() && lines.len() == 400,
        "every line once, in order: {} lines, first out of place {misplaced:?}",
        lines.len()
    );

    let mut last = 0;
    for event in &all {
        if let TerminalEvent::Output { seq, .. } | TerminalEvent::Resized { seq, .. } = event {
            assert!(*seq > last, "seq {seq} after {last}");
            last = *seq;
        }
    }
    let connection: Vec<(usize, &TerminalEvent)> = all
        .iter()
        .enumerate()
        .filter(|(_, e)| !matches!(e, TerminalEvent::Output { .. }))
        .collect();
    assert_eq!(
        connection.iter().map(|(_, e)| *e).collect::<Vec<_>>(),
        vec![
            &TerminalEvent::Disconnected,
            &TerminalEvent::Reattached {
                mode: AttachMode::Replay,
                lease: true
            }
        ],
        "one Disconnected, then Reattached"
    );
    assert_eq!(
        connection[1].0,
        connection[0].0 + 1,
        "nothing between Disconnected and Reattached"
    );
    client.close();
}

fn count(text: &[u8], needle: &str) -> usize {
    text.windows(needle.len())
        .filter(|w| *w == needle.as_bytes())
        .count()
}

/// Two clients pass the lease back and forth: each takeover replaces the taker's attachment
/// without a gap or a repeat, the other side is told, and only the holder types.
#[test]
fn taking_the_lease_back_and_forth_keeps_each_side_in_order() {
    let mut home = Home::new("clientlease");
    let (a, _connector_a) = connect(&mut home);
    let (b, _connector_b) = connect(&mut home);
    let (terminal_a, events_a) = Terminal::create(
        &a,
        vec!["/bin/cat".into()],
        None,
        BTreeMap::new(),
        Size { rows: 24, cols: 80 },
    )
    .unwrap();
    let epoch = a.epoch().unwrap();
    let (terminal_b, events_b) =
        Terminal::attach(&b, terminal_a.id().clone(), epoch, 0, None).unwrap();
    assert!(terminal_b.lease().is_none());

    let (mut text_a, mut seen_a) = (Vec::new(), Vec::new());
    let (mut text_b, mut seen_b) = (Vec::new(), Vec::new());

    terminal_b.take_lease().unwrap();
    assert!(terminal_b.lease().is_some());
    text_until(&events_a, &mut text_a, &mut seen_a, |_, s| {
        s.contains(&TerminalEvent::LeaseLost)
    });
    assert!(terminal_a.lease().is_none());
    assert!(
        terminal_a.input(b"refused\n").is_err(),
        "a watcher cannot type"
    );
    terminal_b.input(b"from b\n").unwrap();
    for (events, text, seen) in [
        (&events_a, &mut text_a, &mut seen_a),
        (&events_b, &mut text_b, &mut seen_b),
    ] {
        text_until(events, text, seen, |t, _| count(t, "from b") >= 2);
    }

    terminal_a.take_lease().unwrap();
    text_until(&events_b, &mut text_b, &mut seen_b, |_, s| {
        s.contains(&TerminalEvent::LeaseLost)
    });
    terminal_a.input(b"from a\n").unwrap();
    for (events, text, seen) in [
        (&events_a, &mut text_a, &mut seen_a),
        (&events_b, &mut text_b, &mut seen_b),
    ] {
        text_until(events, text, seen, |t, _| count(t, "from a") >= 2);
    }
    std::thread::sleep(Duration::from_millis(300));
    for (events, text) in [(&events_a, &mut text_a), (&events_b, &mut text_b)] {
        while let Ok(event) = events.try_recv() {
            if let TerminalEvent::Output { data, .. } = event {
                text.extend_from_slice(&data);
            }
        }
        // The echo and cat's copy, once each: no replay repeated by a takeover.
        assert_eq!(count(text, "from b"), 2);
        assert_eq!(count(text, "from a"), 2);
        assert_eq!(count(text, "refused"), 0);
    }
    a.close();
    b.close();
}

/// The desktop quits and starts again with the terminal's id, epoch and lease (kept encrypted
/// by the desktop): it attaches fresh, gets the current screen first, and its inputs are typed,
/// numbered above the ones the lease used before, not taken for repeats.
#[test]
fn a_restarted_desktop_gets_the_screen_and_types_with_its_kept_lease() {
    let mut home = Home::new("clientrestart");
    let (first, _connector_first) = connect(&mut home);
    let (terminal, events) = Terminal::create(
        &first,
        vec!["/bin/cat".into()],
        None,
        BTreeMap::new(),
        Size { rows: 24, cols: 80 },
    )
    .unwrap();
    let (id, epoch, lease) = (
        terminal.id().clone(),
        first.epoch().unwrap(),
        terminal.lease().unwrap(),
    );
    let (mut text, mut seen) = (Vec::new(), Vec::new());
    terminal.input(b"before restart\n").unwrap();
    text_until(&events, &mut text, &mut seen, |t, _| {
        count(t, "before restart") >= 2
    });
    drop(terminal);
    drop(events);
    first.close();

    let (second, _connector_second) = connect(&mut home);
    assert_eq!(second.epoch(), Some(epoch.clone()), "the same daemon");
    let (terminal, events) = Terminal::attach_fresh(&second, id, epoch, Some(lease)).unwrap();
    assert!(terminal.lease().is_some(), "the kept lease still types");
    match events.recv_timeout(WAIT).unwrap() {
        TerminalEvent::Snapshot { size, data, .. } => {
            let mut screen = vt100::Parser::new(size.rows, size.cols, 0);
            screen.process(&data);
            assert!(
                screen.screen().contents().contains("before restart"),
                "{}",
                screen.screen().contents()
            );
        }
        other => panic!("the screen first, got {other:?}"),
    }
    let (mut text, mut seen) = (Vec::new(), Vec::new());
    terminal.input(b"after restart\n").unwrap();
    text_until(&events, &mut text, &mut seen, |t, _| {
        count(t, "after restart") >= 2
    });
    std::thread::sleep(Duration::from_millis(300));
    while let Ok(TerminalEvent::Output { data, .. }) = events.try_recv() {
        text.extend_from_slice(&data);
    }
    assert_eq!(count(&text, "after restart"), 2, "the echo and cat's copy");
    assert_eq!(count(&text, "before restart"), 0, "nothing replayed");
    second.close();
}

/// How many attachments the daemon has for `terminal`, and whether it still runs.
fn attachments(client: &Client, terminal: &tessel_host_protocol::TerminalId) -> Option<u32> {
    let listed: tessel_host_protocol::pty::Listed = client
        .ask(
            tessel_host_protocol::pty::kind::LIST,
            &serde_json::Value::Null,
        )
        .unwrap();
    listed
        .terminals
        .iter()
        .find(|t| t.terminal == *terminal)
        .map(|t| t.attached)
}

/// A pane closed while its program prints nothing: dropping the `Terminal` and its receiver
/// lets go at once (the daemon drops the attachment, no follower waits for an event that may
/// never come), the program keeps running, and a reconnection never reattaches it.
#[test]
fn dropping_a_silent_terminal_lets_go_without_ending_it_or_reattaching() {
    let mut home = Home::new("clientdrop");
    let (client, connector) = connect(&mut home);
    let (terminal, events) = Terminal::create(
        &client,
        vec!["/bin/cat".into()],
        None,
        BTreeMap::new(),
        Size { rows: 24, cols: 80 },
    )
    .unwrap();
    let (id, pid) = (terminal.id().clone(), terminal.pid);
    assert_eq!(attachments(&client, &id), Some(1));
    drop(terminal);
    drop(events);
    wait_for("the attachment to go", || {
        attachments(&client, &id) == Some(0)
    });
    assert!(alive(pid), "the program goes on");

    let epoch = client.epoch().unwrap();
    cut(&connector);
    wait_for("the reconnection", || {
        client.link().is_ok()
            && client.state()
                == ConnectionState::Connected {
                    epoch: epoch.clone(),
                }
    });
    std::thread::sleep(Duration::from_millis(500));
    assert_eq!(attachments(&client, &id), Some(0), "never reattached");
    assert!(alive(pid));
    client.close();
}

/// Dropping the `Terminal` while the application still holds its receiver: nothing more is
/// handed over, not the old stream's `Disconnected` and not a reattachment after a reconnect.
#[test]
fn dropping_the_terminal_but_keeping_its_receiver_hands_nothing_more() {
    let mut home = Home::new("clientkeep");
    let (client, connector) = connect(&mut home);
    let (terminal, events) = Terminal::create(
        &client,
        vec!["/bin/cat".into()],
        None,
        BTreeMap::new(),
        Size { rows: 24, cols: 80 },
    )
    .unwrap();
    let id = terminal.id().clone();
    drop(terminal);
    wait_for("the attachment to go", || {
        attachments(&client, &id) == Some(0)
    });
    let epoch = client.epoch().unwrap();
    cut(&connector);
    wait_for("the reconnection", || {
        client.link().is_ok()
            && client.state()
                == ConnectionState::Connected {
                    epoch: epoch.clone(),
                }
    });
    std::thread::sleep(Duration::from_millis(500));
    assert_eq!(attachments(&client, &id), Some(0), "never reattached");
    let late: Vec<TerminalEvent> = events.try_iter().collect();
    assert!(late.is_empty(), "nothing after letting go: {late:?}");
    client.close();
}

/// Dropping only the receiver lets go too, at the next event: nothing can read the terminal's
/// events any more, so it is detached and never reattached.
#[test]
fn dropping_only_the_receiver_lets_go_at_the_next_event() {
    let mut home = Home::new("clientrecv");
    let (client, connector) = connect(&mut home);
    let (terminal, events) = Terminal::create(
        &client,
        vec!["/bin/cat".into()],
        None,
        BTreeMap::new(),
        Size { rows: 24, cols: 80 },
    )
    .unwrap();
    let (id, lease) = (terminal.id().clone(), terminal.lease().unwrap());
    drop(events);
    terminal.input(b"wake up\n").unwrap();
    wait_for("the attachment to go", || {
        attachments(&client, &id) == Some(0)
    });

    let epoch = client.epoch().unwrap();
    cut(&connector);
    wait_for("the reconnection", || {
        client.link().is_ok()
            && client.state()
                == ConnectionState::Connected {
                    epoch: epoch.clone(),
                }
    });
    std::thread::sleep(Duration::from_millis(500));
    assert_eq!(attachments(&client, &id), Some(0), "never reattached");
    // Let go: this object no longer types, though the lease stays valid on the host (a new,
    // explicit attachment could use it).
    assert!(terminal.lease().is_none());
    assert!(terminal.input(b"blind typing\n").is_err());
    let listed: tessel_host_protocol::pty::Listed = client
        .ask(
            tessel_host_protocol::pty::kind::LIST,
            &serde_json::Value::Null,
        )
        .unwrap();
    let info = listed.terminals.iter().find(|t| t.terminal == id).unwrap();
    assert!(
        info.leased && info.exited.is_none(),
        "still running, still leased"
    );
    // Ending it takes an explicit close with the kept lease, without attaching.
    assert!(terminal.close().is_err());
    tessel_host_client::terminal::close_terminal(&client, &epoch, &id, &lease).unwrap();
    wait_for("the program to end", || {
        let listed: tessel_host_protocol::pty::Listed = client
            .ask(
                tessel_host_protocol::pty::kind::LIST,
                &serde_json::Value::Null,
            )
            .unwrap();
        listed
            .terminals
            .iter()
            .find(|t| t.terminal == id)
            .is_none_or(|t| t.exited.is_some())
    });
    client.close();
}

/// A create whose reply never arrived, sent again with the same key over another connection
/// to the same daemon: the same terminal and lease, one program. A key meant for another epoch
/// sends nothing.
#[test]
fn the_same_key_finds_the_terminal_a_lost_reply_created() {
    let mut home = Home::new("clientkeyed");
    let (first, _connector_first) = connect(&mut home);
    let (second, _connector_second) = connect(&mut home);
    let epoch = first.epoch().unwrap();
    let key = tessel_host_protocol::DedupKey::from_bytes([0x5a; 16]);
    let size = Size { rows: 24, cols: 80 };
    let (original, _events) = Terminal::create_keyed(
        &first,
        key.clone(),
        &epoch,
        vec!["/bin/cat".into()],
        None,
        BTreeMap::new(),
        size,
    )
    .unwrap();
    // The desktop never saw that reply; it sends the same key again.
    let (again, _events_again) = Terminal::create_keyed(
        &second,
        key,
        &epoch,
        vec!["/bin/cat".into()],
        None,
        BTreeMap::new(),
        size,
    )
    .unwrap();
    assert_eq!(again.id(), original.id());
    assert_eq!(again.lease(), original.lease());
    assert_eq!(again.pid, original.pid);
    let listed: tessel_host_protocol::pty::Listed = second
        .ask(
            tessel_host_protocol::pty::kind::LIST,
            &serde_json::Value::Null,
        )
        .unwrap();
    assert_eq!(listed.terminals.len(), 1, "one program");

    let other = tessel_host_protocol::Epoch::from_bytes([0x11; 16]);
    let refused = Terminal::create_keyed(
        &second,
        tessel_host_protocol::DedupKey::from_bytes([0x6b; 16]),
        &other,
        vec!["/bin/cat".into()],
        None,
        BTreeMap::new(),
        size,
    );
    assert!(matches!(
        refused,
        Err(tessel_host_client::ClientError::EpochChanged)
    ));
    let listed: tessel_host_protocol::pty::Listed = second
        .ask(
            tessel_host_protocol::pty::kind::LIST,
            &serde_json::Value::Null,
        )
        .unwrap();
    assert_eq!(listed.terminals.len(), 1, "nothing sent for another epoch");
    first.close();
    second.close();
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
