//! Gate 3: terminals over the real bridge and daemon, each test in a home of its own.
#![cfg(target_os = "linux")]
mod common;

use common::*;
use std::collections::BTreeMap;
use std::time::Duration;
use tessel_host_protocol::pty::*;
use tessel_host_protocol::*;

fn key(n: u8) -> DedupKey {
    DedupKey::from_bytes([n; 16])
}

fn sh(script: &str) -> Vec<String> {
    vec!["/bin/sh".into(), "-c".into(), script.into()]
}

fn create(client: &mut Client, n: u8, argv: Vec<String>, size: Size) -> Created {
    client.ask(
        kind::CREATE,
        &Create {
            dedup: key(n),
            argv,
            cwd: None,
            env: BTreeMap::new(),
            size,
        },
    )
}

fn attach(
    client: &mut Client,
    viewer: &mut Viewer,
    terminal: &TerminalId,
    after: u64,
    writer: Writer,
) -> Attached {
    let attached: Attached = client.ask(
        kind::ATTACH,
        &Attach {
            terminal: terminal.clone(),
            epoch: client.epoch.clone(),
            after,
            writer,
        },
    );
    viewer.attached(attached.stream.clone());
    attached
}

fn input(
    client: &mut Client,
    terminal: &TerminalId,
    lease: &LeaseKey,
    id: u64,
    text: &str,
) -> Result<InputOk, WireError> {
    client
        .request(
            kind::INPUT,
            &Input {
                terminal: terminal.clone(),
                lease: lease.clone(),
                input_id: id,
                data: Chunk::new(text.as_bytes().to_vec()).unwrap(),
            },
        )
        .map(|ok| ok.body_as().unwrap())
}

const SIZE: Size = Size { rows: 24, cols: 80 };

fn count(text: &str, word: &str) -> usize {
    text.matches(word).count()
}

#[test]
fn numbered_output_goes_on_through_a_killed_bridge_without_a_duplicate_or_a_hole() {
    let mut home = Home::new("numbered");
    let mut client = home.client(Budgets::SLOW_HOST);
    let script = "for i in $(seq 1 4000); do echo \"line $i\"; [ $((i % 200)) -eq 0 ] && sleep 0.1; done; sleep 60";
    let created = create(&mut client, 1, sh(script), SIZE);
    let mut viewer = Viewer::new();
    let first = attach(&mut client, &mut viewer, &created.terminal, 0, Writer::None);
    assert_eq!((first.mode, first.pid), (AttachMode::Replay, created.pid));
    viewer.until(&mut client, |v| v.text().contains("line 600\r\n"));

    // The SSH channel dies without warning while the program keeps writing.
    let epoch = client.epoch.clone();
    client.kill();
    std::thread::sleep(Duration::from_millis(700));
    assert!(alive(created.pid));

    let mut client = home.client(Budgets::SLOW_HOST);
    assert_eq!(client.epoch, epoch, "the same daemon");
    let after = viewer.last;
    let again = attach(
        &mut client,
        &mut viewer,
        &created.terminal,
        after,
        Writer::None,
    );
    assert_eq!(
        (again.mode, again.pid),
        (AttachMode::Replay, created.pid),
        "the same program, history kept"
    );
    assert!(
        again.seq > viewer.last,
        "it wrote while nobody was attached"
    );
    viewer.until(&mut client, |v| v.text().contains("line 4000\r\n"));
    let expected: String = (1..=4000).map(|i| format!("line {i}\r\n")).collect();
    assert_eq!(viewer.text(), expected);
}

#[test]
fn a_lost_reply_never_types_twice_and_leases_decide_who_types() {
    let mut home = Home::new("lease");
    let mut client = home.client(Budgets::SLOW_HOST);
    let created = create(&mut client, 2, vec!["cat".into()], SIZE);
    // Creating again with the same key is the same terminal, not a second one.
    let retried = create(&mut client, 2, vec!["cat".into()], SIZE);
    assert_eq!(retried, created);
    let mut viewer = Viewer::new();
    let attached = attach(
        &mut client,
        &mut viewer,
        &created.terminal,
        0,
        Writer::Resume(created.lease.clone()),
    );
    assert_eq!(attached.lease.as_ref(), Some(&created.lease));

    // Typed; the program echoes it; the reply is never read: the connection dies.
    client.send(
        kind::INPUT,
        &Input {
            terminal: created.terminal.clone(),
            lease: created.lease.clone(),
            input_id: 1,
            data: Chunk::new(b"hello\n".to_vec()).unwrap(),
        },
    );
    viewer.until(&mut client, |v| count(&v.text(), "hello") >= 2);
    client.kill();

    let mut client = home.client(Budgets::SLOW_HOST);
    let after = viewer.last;
    attach(
        &mut client,
        &mut viewer,
        &created.terminal,
        after,
        Writer::Resume(created.lease.clone()),
    );
    assert!(
        input(&mut client, &created.terminal, &created.lease, 1, "hello\n")
            .unwrap()
            .duplicate
    );
    assert!(
        !input(&mut client, &created.terminal, &created.lease, 2, "world\n")
            .unwrap()
            .duplicate
    );
    viewer.until(&mut client, |v| count(&v.text(), "world") >= 2);
    assert_eq!(
        count(&viewer.text(), "hello"),
        2,
        "typed once: the echo and cat's copy"
    );

    // Watching gives no right to type; taking the lease ends the old one.
    let mut watcher = Viewer::new();
    let mut other = home.client(Budgets::SLOW_HOST);
    let watching = attach(&mut other, &mut watcher, &created.terminal, 0, Writer::None);
    assert!(watching.lease.is_none());
    let taken = attach(&mut other, &mut watcher, &created.terminal, 0, Writer::Take);
    let new_lease = taken.lease.unwrap();
    let refused = input(&mut client, &created.terminal, &created.lease, 3, "x\n").unwrap_err();
    assert_eq!(refused.code, ErrorCode::PermissionDenied);
    assert!(
        !input(&mut other, &created.terminal, &new_lease, 1, "again\n")
            .unwrap()
            .duplicate
    );

    // Closing hangs up the program and forgets the terminal.
    other.ask::<serde_json::Value>(
        kind::CLOSE,
        &Close {
            terminal: created.terminal.clone(),
            lease: new_lease,
        },
    );
    viewer.until(&mut client, |v| v.exit.is_some());
    let listed: Listed = client.ask(kind::LIST, &serde_json::Value::Null);
    assert!(
        listed
            .terminals
            .iter()
            .all(|t| t.terminal != created.terminal)
    );
}

#[test]
fn a_snapshot_redraws_the_alternate_screen_and_wide_characters() {
    let mut home = Home::new("snapshot");
    let budgets = Budgets {
        terminal_ring_bytes: 1024,
        ..Budgets::SLOW_HOST
    };
    let mut client = home.client(budgets);
    let script = "head -c 5000 /dev/zero | tr '\\0' x; sleep 0.3; printf '\\033[?1049h\\033[2J\\033[H\u{e9}\u{6f22}\u{5b57} alt'; sleep 60";
    let created = create(&mut client, 3, sh(script), SIZE);
    std::thread::sleep(Duration::from_millis(1200));
    let mut viewer = Viewer::new();
    let attached = attach(&mut client, &mut viewer, &created.terminal, 0, Writer::None);
    assert_eq!(
        attached.mode,
        AttachMode::Snapshot,
        "the start was evicted from a 1 KiB history"
    );
    viewer.until(&mut client, |v| !v.snapshots.is_empty());
    let snapshot = &viewer.snapshots[0];
    assert_eq!(
        (snapshot.at, snapshot.alternate, snapshot.resync),
        (attached.seq, true, false)
    );
    assert!(viewer.screen.screen().alternate_screen());
    assert!(
        viewer
            .screen
            .screen()
            .contents()
            .contains("\u{e9}\u{6f22}\u{5b57} alt"),
        "{}",
        viewer.screen.screen().contents()
    );
}

#[test]
fn a_resize_reaches_the_program() {
    let mut home = Home::new("resize");
    let mut client = home.client(Budgets::SLOW_HOST);
    let created = create(&mut client, 4, vec!["/bin/sh".into()], SIZE);
    let mut viewer = Viewer::new();
    attach(
        &mut client,
        &mut viewer,
        &created.terminal,
        0,
        Writer::Resume(created.lease.clone()),
    );
    let size = Size {
        rows: 40,
        cols: 100,
    };
    client.ask::<serde_json::Value>(
        kind::RESIZE,
        &Resize {
            terminal: created.terminal.clone(),
            lease: created.lease.clone(),
            size,
        },
    );
    input(
        &mut client,
        &created.terminal,
        &created.lease,
        1,
        "stty size\n",
    )
    .unwrap();
    viewer.until(&mut client, |v| v.text().contains("40 100"));
    let refused = client
        .request(
            kind::RESIZE,
            &Resize {
                terminal: created.terminal.clone(),
                lease: LeaseKey::from_bytes([9; 16]),
                size,
            },
        )
        .unwrap_err();
    assert_eq!(refused.code, ErrorCode::PermissionDenied);
}

#[test]
fn an_exit_while_nobody_watches_is_kept_for_the_next_attachment() {
    let mut home = Home::new("exit");
    let mut client = home.client(Budgets::SLOW_HOST);
    let created = create(&mut client, 5, sh("echo bye; exit 7"), SIZE);
    std::thread::sleep(Duration::from_millis(800));
    let listed: Listed = client.ask(kind::LIST, &serde_json::Value::Null);
    let info = listed
        .terminals
        .iter()
        .find(|t| t.terminal == created.terminal)
        .unwrap();
    assert_eq!(info.exited.map(|e| e.code), Some(Some(7)));
    assert!(listed.retained_bytes >= 5 && listed.daemon_rss_bytes > 0);
    let mut viewer = Viewer::new();
    let attached = attach(&mut client, &mut viewer, &created.terminal, 0, Writer::None);
    assert_eq!(attached.exited.map(|e| e.code), Some(Some(7)));
    viewer.until(&mut client, |v| v.exit.is_some());
    assert_eq!(viewer.text(), "bye\r\n");
    assert_eq!(viewer.exit.unwrap().code, Some(7));
}

#[test]
fn history_evicted_before_a_slow_client_read_it_comes_back_as_a_resync_snapshot() {
    let mut home = Home::new("resync");
    let budgets = Budgets {
        frame_bytes: 98_304,
        stream_window_bytes: 98_304,
        terminal_ring_bytes: 131_072,
        ..Budgets::SLOW_HOST
    };
    let mut client = home.client(budgets);
    let created = create(
        &mut client,
        6,
        sh("sleep 0.5; head -c 600000 /dev/zero | tr '\\0' y; sleep 60"),
        SIZE,
    );
    let mut viewer = Viewer::new();
    viewer.acking = false;
    attach(&mut client, &mut viewer, &created.terminal, 0, Writer::None);
    // Nothing acknowledged: the host stops at the window, then the program goes on.
    std::thread::sleep(Duration::from_millis(2500));
    viewer.drain(&mut client, Duration::from_millis(300));
    let stalled = viewer.last;
    let listed: Listed = client.ask(kind::LIST, &serde_json::Value::Null);
    let total = listed.terminals[0].seq;
    assert!(
        stalled < total,
        "the window held the stream back ({stalled} of {total})"
    );
    assert!(listed.terminals[0].retained_bytes <= 131_072 + 65_536);
    viewer.acking = true;
    let stream = viewer.stream.clone().unwrap();
    client.ack(&stream, viewer.last_stream_seq);
    viewer.until(&mut client, |v| v.last >= total);
    let resync = viewer
        .snapshots
        .iter()
        .find(|s| s.resync)
        .expect("a resync snapshot");
    assert!(resync.at > stalled);
    assert!(
        viewer.text.len() < 600_000,
        "what was evicted came back as the screen, not as bytes"
    );
}

#[test]
fn after_a_daemon_restart_its_terminals_are_reported_gone() {
    let mut home = Home::new("epoch");
    let mut client = home.client(Budgets::SLOW_HOST);
    let created = create(&mut client, 7, sh("sleep 60"), SIZE);
    let old_epoch = client.epoch.clone();
    let daemon = client.host_info().pid;
    // SAFETY: the daemon this test started.
    unsafe { libc::kill(daemon as i32, libc::SIGKILL) };
    client.kill();
    std::thread::sleep(Duration::from_millis(300));
    let mut client = home.client(Budgets::SLOW_HOST);
    assert_ne!(client.epoch, old_epoch);
    let attach_with = |client: &mut Client, epoch: Epoch| {
        client
            .request(
                kind::ATTACH,
                &Attach {
                    terminal: created.terminal.clone(),
                    epoch,
                    after: 0,
                    writer: Writer::None,
                },
            )
            .unwrap_err()
            .code
    };
    assert_eq!(
        attach_with(&mut client, old_epoch),
        ErrorCode::EpochMismatch
    );
    let epoch = client.epoch.clone();
    assert_eq!(attach_with(&mut client, epoch), ErrorCode::NotFound);
}

#[test]
fn a_small_frame_budget_is_refused_for_terminals() {
    let mut home = Home::new("frame");
    let budgets = Budgets {
        frame_bytes: 4096,
        stream_window_bytes: 4096,
        ..Budgets::SLOW_HOST
    };
    let mut client = home.client(budgets);
    let refused = client
        .request(
            kind::CREATE,
            &Create {
                dedup: key(8),
                argv: vec!["true".into()],
                cwd: None,
                env: BTreeMap::new(),
                size: SIZE,
            },
        )
        .unwrap_err();
    assert_eq!(refused.code, ErrorCode::BadRequest);
}
