//! Terminal messages, capability `pty`. Requests carry a `terminal`; everything that changes a
//! terminal (input, size, signals, closing) carries the writer `lease` as well. Output comes as
//! events of an attachment's stream, numbered by the terminal's own sequence (`seq`), so a
//! client that reattaches gets either the bytes after the last one it applied, or a snapshot of
//! the screen at a sequence barrier followed by everything after it, never a byte twice.
use crate::bytes::Chunk;
use crate::ids::{DedupKey, Epoch, LeaseKey, StreamId, TerminalId};
use crate::wire::{ErrorCode, WireError};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub const MAX_ROWS: u16 = 500;
pub const MAX_COLS: u16 = 1000;
pub const MAX_ARGS: usize = 64;
pub const MAX_ARG_BYTES: usize = 4096;
pub const MAX_ENV: usize = 256;
pub const MAX_ENV_NAME_BYTES: usize = 128;
pub const MAX_ENV_VALUE_BYTES: usize = 32 * 1024;
pub const MAX_CWD_BYTES: usize = 4096;

/// Message kinds of this capability.
pub mod kind {
    pub const CREATE: &str = "terminal.create";
    pub const ATTACH: &str = "terminal.attach";
    pub const DETACH: &str = "terminal.detach";
    pub const INPUT: &str = "terminal.input";
    pub const RESIZE: &str = "terminal.resize";
    pub const SIGNAL: &str = "terminal.signal";
    pub const CLOSE: &str = "terminal.close";
    pub const LIST: &str = "terminal.list";
    // Replies.
    pub const CREATED: &str = "terminal.created";
    pub const ATTACHED: &str = "terminal.attached";
    pub const INPUT_OK: &str = "terminal.input_ok";
    pub const LISTED: &str = "terminal.listed";
    // Events.
    pub const OUTPUT: &str = "terminal.output";
    pub const SNAPSHOT: &str = "terminal.snapshot";
    pub const EXIT: &str = "terminal.exit";
    pub const RESIZED: &str = "terminal.resized";
    pub const LEASE_LOST: &str = "terminal.lease_lost";
}

fn bad(detail: &'static str) -> WireError {
    WireError::detail(ErrorCode::BadRequest, detail)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Size {
    pub rows: u16,
    pub cols: u16,
}

impl Size {
    pub fn validate(self) -> Result<(), WireError> {
        if (1..=MAX_ROWS).contains(&self.rows) && (1..=MAX_COLS).contains(&self.cols) {
            Ok(())
        } else {
            Err(bad("terminal size out of range"))
        }
    }
}

/// Start a program on a new terminal. `argv` empty: the user's login shell. `cwd` absent: the
/// home directory. `env` is added to a small base (HOME, USER, LOGNAME, SHELL, PATH, LANG,
/// LC_*, TZ); `TERM` defaults to `xterm-256color`. Retrying with the same `dedup` returns the
/// first terminal instead of starting another.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Create {
    pub dedup: DedupKey,
    #[serde(default)]
    pub argv: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cwd: Option<String>,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    pub size: Size,
}

impl Create {
    pub fn validate(&self) -> Result<(), WireError> {
        self.size.validate()?;
        let text_ok = |s: &str, max: usize| s.len() <= max && !s.contains('\0');
        if self.argv.len() > MAX_ARGS
            || !self.argv.iter().all(|a| text_ok(a, MAX_ARG_BYTES))
            || self.argv.first().is_some_and(String::is_empty)
        {
            return Err(bad("invalid program arguments"));
        }
        if self
            .cwd
            .as_deref()
            .is_some_and(|c| !c.starts_with('/') || !text_ok(c, MAX_CWD_BYTES))
        {
            return Err(bad("the working directory must be an absolute path"));
        }
        let name_ok = |n: &str| {
            !n.is_empty()
                && n.len() <= MAX_ENV_NAME_BYTES
                && !n.as_bytes()[0].is_ascii_digit()
                && n.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
        };
        if self.env.len() > MAX_ENV
            || !self
                .env
                .iter()
                .all(|(n, v)| name_ok(n) && text_ok(v, MAX_ENV_VALUE_BYTES))
        {
            return Err(bad("invalid environment"));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Created {
    pub terminal: TerminalId,
    pub pid: u32,
    /// The creator holds the writer lease.
    pub lease: LeaseKey,
}

/// What an attachment may do besides watching.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Writer {
    /// Watch only.
    None,
    /// Take the lease (whoever held it loses it).
    Take,
    /// Keep a lease held before a reconnect, with its input numbering.
    Resume(LeaseKey),
}

/// Start receiving a terminal's output. `after`: the last sequence number the client applied
/// (0: none of its events, so everything still kept is replayed). Absent: the client shows
/// nothing of this terminal (a desktop that just started), so the attachment always starts with
/// a snapshot of the current screen, then live events; a daemon from before 0.1.1 refuses that
/// request (`bad_request`, a missing field) rather than reading it as 0. `epoch`: the host epoch
/// the client knew the terminal from; another one is refused with `epoch_mismatch` (the daemon
/// restarted, its terminals are gone).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Attach {
    pub terminal: TerminalId,
    pub epoch: Epoch,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after: Option<u64>,
    pub writer: Writer,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AttachMode {
    /// The bytes after `after` follow, as `terminal.output` events.
    Replay,
    /// Those bytes are gone: `terminal.snapshot` events at `seq` come first, then the rest.
    Snapshot,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Attached {
    pub stream: StreamId,
    pub pid: u32,
    pub size: Size,
    pub mode: AttachMode,
    /// The sequence barrier: the last event when the attachment started.
    pub seq: u64,
    /// The lease this attachment holds: given for `take`, and for `resume` while the lease is
    /// still that one. A lease taken by someone else meanwhile gives none: the attachment only
    /// watches (and says so), it is not refused.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lease: Option<LeaseKey>,
    /// With a lease: the highest input identifier this lease already used (0 for a new lease),
    /// so a client that kept the lease across its own restart numbers its inputs above it
    /// instead of reusing identifiers the daemon would take for repeats. Always present with a
    /// lease from 0.1.1 on; a client must not type with a lease whose numbering it does not
    /// know.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_input: Option<u64>,
}

/// Bytes to type. Each `input_id` is typed at most once per lease: a retry of one already
/// applied (after a lost reply) is not typed again and the reply says so; one refused (`busy`)
/// can be retried. The last 1024 applied identifiers are remembered: an older one is refused
/// rather than guessed.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Input {
    pub terminal: TerminalId,
    pub lease: LeaseKey,
    pub input_id: u64,
    pub data: Chunk,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InputOk {
    pub duplicate: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Resize {
    pub terminal: TerminalId,
    pub lease: LeaseKey,
    pub size: Size,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Signal {
    Int,
    Term,
    Kill,
    Hup,
    Quit,
    Tstp,
    Cont,
}

/// A signal to the terminal's whole process group.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SignalRequest {
    pub terminal: TerminalId,
    pub lease: LeaseKey,
    pub signal: Signal,
}

/// End the terminal: hang-up to its process group, then kill if it lingers, then forget it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Close {
    pub terminal: TerminalId,
    pub lease: LeaseKey,
}

/// Stop the events of one attachment (the terminal goes on).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Detach {
    pub stream: StreamId,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Listed {
    pub terminals: Vec<TerminalInfo>,
    /// The daemon's resident memory, and what all terminals keep for replay, separately.
    pub daemon_rss_bytes: u64,
    pub retained_bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TerminalInfo {
    pub terminal: TerminalId,
    pub pid: u32,
    pub size: Size,
    pub seq: u64,
    pub attached: u32,
    pub leased: bool,
    pub retained_bytes: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exited: Option<Exit>,
}

/// Event: output, numbered by the terminal.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Output {
    pub seq: u64,
    pub data: Chunk,
}

/// Event: one part of the screen as of output `at`, as escape sequences that redraw it (screen
/// contents, colours, cursor, input modes, title) without replaying what produced it: no
/// clipboard, bell or reply to the program is executed again. Parts come in order; apply them
/// to a reset terminal once `last` arrived, then output after `at`. `resync`: sent during an
/// attachment because history it had not received yet was evicted.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Snapshot {
    pub at: u64,
    pub part: u32,
    pub last: bool,
    pub size: Size,
    pub alternate: bool,
    pub resync: bool,
    pub data: Chunk,
}

/// Event (and part of `listed`): the program ended; `seq` is its last event. It comes once per
/// attachment, after every event up to `seq`: show the end when it arrives.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Exit {
    pub seq: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub code: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signal: Option<i32>,
}

/// Event: the terminal was resized, numbered like output so it applies between the right bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Resized {
    pub seq: u64,
    pub size: Size,
}

/// Event: another client took the lease this attachment held; it now only watches.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LeaseLost {
    pub seq: u64,
}
