//! Snapshots are checked part by part before anything is kept: parts out of order, disagreeing
//! on the screen they describe, interrupted by another event, too many of them or too large in
//! all make this terminal's stream refused (`StreamRejected`), for this terminal only: the link
//! stays up, nothing reconnects for it, the daemon is asked to drop the attachment, and the
//! object no longer types. A fake daemon over in-process pipes answers hello, attach and detach,
//! then plays the stream; runs on Linux and Windows.
use std::io::{self, Write};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tessel_host_client::terminal::{
    MAX_SNAPSHOT_BYTES, StreamRejection, Terminal, TerminalEvent, close_terminal,
};
use tessel_host_client::{Client, ConnectionState, Connector, Options, Pipe};
use tessel_host_protocol::pty::{
    self, Attach, AttachMode, Attached, Close, Output, Size, Snapshot, Writer,
};
use tessel_host_protocol::{
    Budgets, Capability, Chunk, ClientFrame, Epoch, HelloOk, HostFrame, LeaseKey, MAX_CHUNK_BYTES,
    MAX_FRAME_BYTES, RawMessage, ReplyMessage, StreamId, StreamPos, TerminalId, VERSION,
    read_frame, write_frame,
};

const EPOCH: [u8; 16] = [7; 16];
const STREAM: [u8; 16] = [9; 16];
const SIZE: Size = Size { rows: 24, cols: 80 };

/// What the fake daemon sends on the stream after the attach reply.
type Script = Vec<RawMessage>;

#[derive(Default)]
struct Seen {
    detached: usize,
    attaches: usize,
    closed: Vec<LeaseKey>,
}

struct Fake {
    script: Script,
    seen: Arc<Mutex<Seen>>,
}

impl Connector for Fake {
    fn connect(&self) -> io::Result<Pipe> {
        let (client_reader, mut daemon_writer) = io::pipe()?;
        let (mut daemon_reader, client_writer) = io::pipe()?;
        let script = self.script.clone();
        let seen = self.seen.clone();
        std::thread::spawn(move || {
            let mut send = |frame: HostFrame| {
                write_frame(&mut daemon_writer, &frame, MAX_FRAME_BYTES)
                    .and_then(|()| daemon_writer.flush())
            };
            while let Ok(Some(frame)) =
                read_frame::<ClientFrame>(&mut daemon_reader, MAX_FRAME_BYTES)
            {
                let (Some(id), Some(message)) = (frame.request_id, frame.message) else {
                    continue; // an acknowledgment
                };
                let answer = match message.kind.as_str() {
                    "hello" => ReplyMessage::HelloOk(HelloOk {
                        version: VERSION,
                        capabilities: vec![Capability::Pty],
                        budgets: Budgets::SLOW_HOST,
                        host_epoch: Epoch::from_bytes(EPOCH),
                    })
                    .into_raw(),
                    "ping" => ReplyMessage::Pong.into_raw(),
                    pty::kind::ATTACH => {
                        let request: Attach = message.body_as().unwrap();
                        let lease = match request.writer {
                            Writer::Resume(lease) => Some(lease),
                            _ => None,
                        };
                        seen.lock().unwrap().attaches += 1;
                        let reply = RawMessage::of(
                            pty::kind::ATTACHED,
                            &Attached {
                                stream: StreamId::from_bytes(STREAM),
                                pid: 1,
                                size: SIZE,
                                mode: AttachMode::Snapshot,
                                seq: 5,
                                last_input: lease.as_ref().map(|_| 0),
                                lease,
                            },
                        );
                        if send(HostFrame::reply_ok(id, reply)).is_err() {
                            return;
                        }
                        for (n, event) in script.iter().enumerate() {
                            let pos = StreamPos {
                                id: StreamId::from_bytes(STREAM),
                                seq: n as u64 + 1,
                            };
                            if send(HostFrame::event(pos, event.clone())).is_err() {
                                return;
                            }
                        }
                        continue;
                    }
                    pty::kind::DETACH => {
                        seen.lock().unwrap().detached += 1;
                        ReplyMessage::Done.into_raw()
                    }
                    pty::kind::CLOSE => {
                        let close: Close = message.body_as().unwrap();
                        seen.lock().unwrap().closed.push(close.lease);
                        ReplyMessage::Done.into_raw()
                    }
                    other => panic!("unexpected request {other}"),
                };
                if send(HostFrame::reply_ok(id, answer)).is_err() {
                    return;
                }
            }
        });
        Ok(Pipe {
            reader: Box::new(client_reader),
            writer: Box::new(client_writer),
            guard: None,
        })
    }
}

fn part(part: u32, last: bool, size: Size, bytes: Vec<u8>) -> RawMessage {
    RawMessage::of(
        pty::kind::SNAPSHOT,
        &Snapshot {
            at: 5,
            part,
            last,
            size,
            alternate: false,
            resync: false,
            data: Chunk::new(bytes).unwrap(),
        },
    )
}

/// Attaches with a lease to a fake daemon playing `script`; returns what the application got.
fn play(script: Script) -> (Client, Terminal, Vec<TerminalEvent>, Arc<Mutex<Seen>>) {
    let seen = Arc::new(Mutex::new(Seen::default()));
    let options = Options {
        capabilities: vec!["pty"],
        ..Options::default()
    };
    let client = Client::connect(
        Box::new(Fake {
            script,
            seen: seen.clone(),
        }),
        options,
    )
    .unwrap();
    let (terminal, events) = Terminal::attach_fresh(
        &client,
        TerminalId::from_bytes([4; 16]),
        Epoch::from_bytes(EPOCH),
        Some(LeaseKey::from_bytes([3; 16])),
    )
    .unwrap();
    let mut got = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        match events.recv_timeout(Duration::from_millis(200)) {
            Ok(event) => {
                let end = matches!(
                    event,
                    TerminalEvent::StreamRejected(_) | TerminalEvent::Snapshot { .. }
                );
                got.push(event);
                if end {
                    break;
                }
            }
            Err(_) if !got.is_empty() => break,
            Err(_) => {}
        }
    }
    (client, terminal, got, seen)
}

fn assert_rejected(script: Script, reason: StreamRejection) {
    let (client, terminal, got, seen) = play(script);
    assert_eq!(
        got.last(),
        Some(&TerminalEvent::StreamRejected(reason)),
        "{got:?}"
    );
    assert!(
        !got.iter()
            .any(|e| matches!(e, TerminalEvent::Snapshot { .. })),
        "nothing half-received applied"
    );
    // For this terminal only: the link is up and answers.
    assert!(matches!(client.state(), ConnectionState::Connected { .. }));
    assert!(client.request(RawMessage::empty("ping")).is_ok());
    // Let go: detached on the daemon, no longer types.
    let deadline = Instant::now() + Duration::from_secs(5);
    while seen.lock().unwrap().detached == 0 {
        assert!(Instant::now() < deadline, "the attachment is let go");
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(terminal.lease().is_none());
    assert!(terminal.input(b"x").is_err());
    assert!(terminal.close().is_err(), "a let-go object ends nothing");
    assert_eq!(seen.lock().unwrap().attaches, 1, "nothing reattached");
    assert!(
        seen.lock().unwrap().closed.is_empty(),
        "nothing ended by itself"
    );
    client.close();
}

/// A terminal let go after a refused snapshot is ended only by an explicit `close_terminal`
/// with its lease, which needs no attachment; another epoch sends nothing.
#[test]
fn a_let_go_terminal_is_ended_only_by_an_explicit_close() {
    let (client, terminal, got, seen) = play(vec![part(1, true, SIZE, b"x".to_vec())]);
    assert_eq!(
        got.last(),
        Some(&TerminalEvent::StreamRejected(
            StreamRejection::InvalidSnapshot
        ))
    );
    assert!(terminal.close().is_err());
    let kept = LeaseKey::from_bytes([3; 16]);
    let other = Epoch::from_bytes([1; 16]);
    assert!(matches!(
        close_terminal(&client, &other, terminal.id(), &kept),
        Err(tessel_host_client::ClientError::EpochChanged)
    ));
    assert!(
        seen.lock().unwrap().closed.is_empty(),
        "nothing sent for another epoch"
    );
    close_terminal(&client, &Epoch::from_bytes(EPOCH), terminal.id(), &kept).unwrap();
    assert_eq!(seen.lock().unwrap().closed, [kept]);
    assert_eq!(seen.lock().unwrap().attaches, 1, "no attachment needed");
    client.close();
}

#[test]
fn a_well_formed_snapshot_is_handed_over_whole() {
    let (client, _terminal, got, _seen) = play(vec![
        part(0, false, SIZE, b"ab".to_vec()),
        part(1, false, SIZE, b"cd".to_vec()),
        part(2, true, SIZE, b"ef".to_vec()),
    ]);
    match got.last() {
        Some(TerminalEvent::Snapshot { at, size, data, .. }) => {
            assert_eq!((*at, *size, data.as_slice()), (5, SIZE, &b"abcdef"[..]));
        }
        other => panic!("{other:?}"),
    }
    client.close();
}

#[test]
fn parts_out_of_order_are_refused() {
    assert_rejected(
        vec![part(1, true, SIZE, b"x".to_vec())],
        StreamRejection::InvalidSnapshot,
    );
    assert_rejected(
        vec![
            part(0, false, SIZE, b"x".to_vec()),
            part(2, true, SIZE, b"y".to_vec()),
        ],
        StreamRejection::InvalidSnapshot,
    );
}

#[test]
fn parts_describing_another_screen_are_refused() {
    assert_rejected(
        vec![
            part(0, false, SIZE, b"x".to_vec()),
            part(1, true, Size { rows: 30, cols: 80 }, b"y".to_vec()),
        ],
        StreamRejection::InvalidSnapshot,
    );
}

#[test]
fn another_event_in_the_middle_of_a_snapshot_is_refused() {
    let output = RawMessage::of(
        pty::kind::OUTPUT,
        &Output {
            seq: 6,
            data: Chunk::new(b"live".to_vec()).unwrap(),
        },
    );
    assert_rejected(
        vec![part(0, false, SIZE, b"x".to_vec()), output],
        StreamRejection::InvalidSnapshot,
    );
}

#[test]
fn a_flood_of_empty_parts_is_refused() {
    let flood: Script = (0..(MAX_SNAPSHOT_BYTES / MAX_CHUNK_BYTES + 5) as u32)
        .map(|n| part(n, false, SIZE, Vec::new()))
        .collect();
    assert_rejected(flood, StreamRejection::InvalidSnapshot);
}

#[test]
fn a_snapshot_beyond_the_limit_is_refused_before_it_is_kept() {
    let full = vec![b'x'; MAX_CHUNK_BYTES];
    let count = MAX_SNAPSHOT_BYTES / MAX_CHUNK_BYTES + 1;
    let parts: Script = (0..count as u32)
        .map(|n| part(n, n as usize + 1 == count, SIZE, full.clone()))
        .collect();
    assert_rejected(parts, StreamRejection::OversizedSnapshot);
}

/// A daemon where the terminal is taken over on the same link: the first attachment's stream
/// starts a snapshot; the takeover's stream gets a whole snapshot and output; only then does
/// the first stream send a part out of order. That old follower is stale by then: nothing is
/// rejected, and the successor goes on.
struct Takeover {
    seen: Arc<Mutex<Seen>>,
}

impl Connector for Takeover {
    fn connect(&self) -> io::Result<Pipe> {
        let (client_reader, mut daemon_writer) = io::pipe()?;
        let (mut daemon_reader, client_writer) = io::pipe()?;
        let seen = self.seen.clone();
        std::thread::spawn(move || {
            let mut send = |frame: HostFrame| {
                write_frame(&mut daemon_writer, &frame, MAX_FRAME_BYTES)
                    .and_then(|()| daemon_writer.flush())
            };
            let first = StreamId::from_bytes([9; 16]);
            let second = StreamId::from_bytes([10; 16]);
            let event = |id: &StreamId, seq: u64, message: RawMessage| {
                HostFrame::event(
                    StreamPos {
                        id: id.clone(),
                        seq,
                    },
                    message,
                )
            };
            while let Ok(Some(frame)) =
                read_frame::<ClientFrame>(&mut daemon_reader, MAX_FRAME_BYTES)
            {
                let (Some(id), Some(message)) = (frame.request_id, frame.message) else {
                    continue;
                };
                let answer = match message.kind.as_str() {
                    "hello" => ReplyMessage::HelloOk(HelloOk {
                        version: VERSION,
                        capabilities: vec![Capability::Pty],
                        budgets: Budgets::SLOW_HOST,
                        host_epoch: Epoch::from_bytes(EPOCH),
                    })
                    .into_raw(),
                    pty::kind::ATTACH => {
                        let request: Attach = message.body_as().unwrap();
                        let takeover = matches!(request.writer, Writer::Take);
                        seen.lock().unwrap().attaches += 1;
                        let attached = Attached {
                            stream: if takeover {
                                second.clone()
                            } else {
                                first.clone()
                            },
                            pid: 1,
                            size: SIZE,
                            mode: AttachMode::Snapshot,
                            seq: 5,
                            lease: Some(LeaseKey::from_bytes(if takeover {
                                [8; 16]
                            } else {
                                [3; 16]
                            })),
                            last_input: Some(0),
                        };
                        if send(HostFrame::reply_ok(
                            id,
                            RawMessage::of(pty::kind::ATTACHED, &attached),
                        ))
                        .is_err()
                        {
                            return;
                        }
                        let events = if takeover {
                            vec![
                                event(&second, 1, part(0, false, SIZE, b"ne".to_vec())),
                                event(&second, 2, part(1, true, SIZE, b"w".to_vec())),
                                event(
                                    &second,
                                    3,
                                    RawMessage::of(
                                        pty::kind::OUTPUT,
                                        &Output {
                                            seq: 6,
                                            data: Chunk::new(b"live".to_vec()).unwrap(),
                                        },
                                    ),
                                ),
                                // Late, on the replaced stream: a part out of order.
                                event(&first, 2, part(5, true, SIZE, b"bad".to_vec())),
                            ]
                        } else {
                            vec![event(&first, 1, part(0, false, SIZE, b"old".to_vec()))]
                        };
                        for frame in events {
                            if send(frame).is_err() {
                                return;
                            }
                        }
                        continue;
                    }
                    pty::kind::DETACH => {
                        seen.lock().unwrap().detached += 1;
                        ReplyMessage::Done.into_raw()
                    }
                    pty::kind::INPUT => RawMessage::of(
                        pty::kind::INPUT_OK,
                        &tessel_host_protocol::pty::InputOk { duplicate: false },
                    ),
                    other => panic!("unexpected request {other}"),
                };
                if send(HostFrame::reply_ok(id, answer)).is_err() {
                    return;
                }
            }
        });
        Ok(Pipe {
            reader: Box::new(client_reader),
            writer: Box::new(client_writer),
            guard: None,
        })
    }
}

#[test]
fn a_replaced_stream_with_a_bad_snapshot_never_rejects_its_successor() {
    let seen = Arc::new(Mutex::new(Seen::default()));
    let options = Options {
        capabilities: vec!["pty"],
        ..Options::default()
    };
    let client = Client::connect(Box::new(Takeover { seen: seen.clone() }), options).unwrap();
    let (terminal, events) = Terminal::attach_fresh(
        &client,
        TerminalId::from_bytes([4; 16]),
        Epoch::from_bytes(EPOCH),
        Some(LeaseKey::from_bytes([3; 16])),
    )
    .unwrap();
    // The first stream's snapshot is still incomplete when the takeover replaces it.
    terminal.take_lease().unwrap();
    let mut got = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if let Ok(event) = events.recv_timeout(Duration::from_millis(100)) {
            got.push(event);
        } else if got.len() >= 2 {
            break;
        }
    }
    // Give the old follower time to read its late, bad part.
    std::thread::sleep(Duration::from_millis(300));
    got.extend(events.try_iter());
    assert!(
        !got.iter()
            .any(|e| matches!(e, TerminalEvent::StreamRejected(_))),
        "a replaced stream rejects nothing: {got:?}"
    );
    assert_eq!(
        got.iter()
            .filter(|e| !matches!(
                e,
                TerminalEvent::Snapshot { .. } | TerminalEvent::Output { .. }
            ))
            .count(),
        0,
        "{got:?}"
    );
    assert!(matches!(
        got.first(),
        Some(TerminalEvent::Snapshot { data, .. }) if data == b"new"
    ));
    assert!(matches!(
        got.get(1),
        Some(TerminalEvent::Output { seq: 6, data }) if data == b"live"
    ));
    // The successor is still this terminal's, and types.
    assert_eq!(terminal.lease(), Some(LeaseKey::from_bytes([8; 16])));
    terminal.input(b"still mine").unwrap();
    client.close();
}
