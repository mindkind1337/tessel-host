//! A lease kept across a restart of the desktop: the client numbers its inputs above the ones
//! the daemon says the lease already used, and refuses a lease whose numbering it is not told
//! (an older daemon) or that is exhausted, instead of starting again from 1, which the daemon
//! would take for repeats and silently not type. A fake daemon over in-process pipes answers
//! hello, attach, detach and input; runs on Linux and Windows.
use std::io::{self, Write};
use std::sync::{Arc, Mutex};
use tessel_host_client::terminal::Terminal;
use tessel_host_client::{Client, ClientError, Connector, Options, Pipe};
use tessel_host_protocol::pty::{self, Attach, AttachMode, Attached, Input, InputOk, Size, Writer};
use tessel_host_protocol::{
    Budgets, Capability, ClientFrame, Epoch, HelloOk, HostFrame, LeaseKey, MAX_FRAME_BYTES, MAX_ID,
    RawMessage, ReplyMessage, StreamId, TerminalId, VERSION, read_frame, write_frame,
};

const EPOCH: [u8; 16] = [7; 16];

/// What the fake daemon says about a lease's inputs, and what it saw.
struct Script {
    last_input: Option<u64>,
    attaches: Vec<Attach>,
    inputs: Vec<u64>,
    detached: usize,
}

struct Fake(Arc<Mutex<Script>>);

impl Connector for Fake {
    fn connect(&self) -> io::Result<Pipe> {
        let (client_reader, mut daemon_writer) = io::pipe()?;
        let (mut daemon_reader, client_writer) = io::pipe()?;
        let script = self.0.clone();
        std::thread::spawn(move || {
            let mut reply = |frame: HostFrame| {
                write_frame(&mut daemon_writer, &frame, MAX_FRAME_BYTES)
                    .and_then(|()| daemon_writer.flush())
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
                        let lease = match &request.writer {
                            Writer::Resume(lease) => Some(lease.clone()),
                            _ => None,
                        };
                        let mut script = script.lock().unwrap();
                        script.attaches.push(request);
                        RawMessage::of(
                            pty::kind::ATTACHED,
                            &Attached {
                                stream: StreamId::from_bytes([9; 16]),
                                pid: 1,
                                size: Size { rows: 24, cols: 80 },
                                mode: AttachMode::Snapshot,
                                seq: 0,
                                last_input: lease.as_ref().and(script.last_input),
                                lease,
                            },
                        )
                    }
                    pty::kind::DETACH => {
                        script.lock().unwrap().detached += 1;
                        ReplyMessage::Done.into_raw()
                    }
                    pty::kind::INPUT => {
                        let input: Input = message.body_as().unwrap();
                        script.lock().unwrap().inputs.push(input.input_id);
                        RawMessage::of(pty::kind::INPUT_OK, &InputOk { duplicate: false })
                    }
                    other => panic!("unexpected request {other}"),
                };
                if reply(HostFrame::reply_ok(id, answer)).is_err() {
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

fn daemon(last_input: Option<u64>) -> (Client, Arc<Mutex<Script>>) {
    let script = Arc::new(Mutex::new(Script {
        last_input,
        attaches: Vec::new(),
        inputs: Vec::new(),
        detached: 0,
    }));
    let options = Options {
        capabilities: vec!["pty"],
        ..Options::default()
    };
    let client = Client::connect(Box::new(Fake(script.clone())), options).unwrap();
    (client, script)
}

fn kept_lease() -> LeaseKey {
    LeaseKey::from_bytes([3; 16])
}

fn terminal() -> TerminalId {
    TerminalId::from_bytes([4; 16])
}

#[test]
fn inputs_are_numbered_above_what_the_kept_lease_already_used() {
    let (client, script) = daemon(Some(41));
    let (terminal, _events) = Terminal::attach_fresh(
        &client,
        terminal(),
        Epoch::from_bytes(EPOCH),
        Some(kept_lease()),
    )
    .unwrap();
    assert!(terminal.lease().is_some());
    terminal.input(b"one").unwrap();
    terminal.input(b"two").unwrap();
    let script = script.lock().unwrap();
    assert_eq!(script.inputs, [42, 43], "never 1 again");
    assert_eq!(
        script.attaches[0].after, None,
        "a fresh attachment asks for the screen, not a replay"
    );
    client.close();
}

#[test]
fn a_lease_whose_numbering_is_not_given_is_refused_not_restarted_from_one() {
    let (client, script) = daemon(None);
    let refused = Terminal::attach(
        &client,
        terminal(),
        Epoch::from_bytes(EPOCH),
        0,
        Some(kept_lease()),
    );
    assert!(matches!(refused, Err(ClientError::Protocol(_))));
    let script = script.lock().unwrap();
    assert!(script.inputs.is_empty());
    assert_eq!(script.detached, 1, "the attachment it refused is let go");
    client.close();
}

#[test]
fn an_exhausted_numbering_is_refused() {
    let (client, script) = daemon(Some(MAX_ID));
    let refused = Terminal::attach_fresh(
        &client,
        terminal(),
        Epoch::from_bytes(EPOCH),
        Some(kept_lease()),
    );
    assert!(matches!(refused, Err(ClientError::Protocol(_))));
    assert!(script.lock().unwrap().inputs.is_empty());
    client.close();
}

#[test]
fn watching_needs_no_numbering() {
    let (client, _script) = daemon(None);
    let (terminal, _events) =
        Terminal::attach_fresh(&client, terminal(), Epoch::from_bytes(EPOCH), None).unwrap();
    assert!(terminal.lease().is_none());
    assert!(terminal.input(b"x").is_err(), "a watcher cannot type");
    client.close();
}
