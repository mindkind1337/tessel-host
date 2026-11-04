//! Git status messages, capability `git`. The host keeps each repository's status (git's
//! porcelain v2) in a cache it keeps current from file system events, running git itself: one
//! git command at a time on the whole host, at low priority, bounded in time, output and
//! entries, and only for repositories someone subscribed to. What changes is sent as revisioned
//! differences, with a state that never presents an incomplete or out-of-date status as clean.
use crate::bytes::RelPath;
use crate::ids::{RepoId, RootId, StreamId};
use serde::{Deserialize, Serialize};

pub mod kind {
    pub const OPEN: &str = "git.open";
    pub const SUBSCRIBE: &str = "git.subscribe";
    pub const UNSUBSCRIBE: &str = "git.unsubscribe";
    pub const REFRESH: &str = "git.refresh";
    pub const UNTRACKED: &str = "git.untracked";
    pub const STATS: &str = "git.stats";
    // Replies.
    pub const OPENED: &str = "git.opened";
    pub const SUBSCRIBED: &str = "git.subscribed";
    pub const UNTRACKED_LIST: &str = "git.untracked_list";
    pub const STATS_OK: &str = "git.stats_ok";
    // Events.
    pub const CHANGED: &str = "git.changed";
}

/// The repository holding `path` below a root (a linked worktree is its own repository).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Open {
    pub root: RootId,
    pub path: RelPath,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Opened {
    pub repo: RepoId,
    /// The worktree's top directory, for people to read.
    pub toplevel: String,
    pub features: Features,
}

/// What this repository's own configuration already uses. The host never turns either on.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Features {
    pub untracked_cache: bool,
    pub fsmonitor: bool,
    pub git_version: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Subscribe {
    pub repo: RepoId,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Subscribed {
    pub stream: StreamId,
    pub revision: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Unsubscribe {
    pub stream: StreamId,
}

/// Read the whole status again (soon, within the same budgets).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Refresh {
    pub repo: RepoId,
}

/// The untracked files inside an untracked directory the status shows as one entry.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Untracked {
    pub repo: RepoId,
    pub path: RelPath,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UntrackedList {
    pub path: RelPath,
    pub entries: Vec<RelPath>,
    pub truncated: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Stats {
    pub repo: RepoId,
}

/// What the repository cost so far: git commands run, bytes they printed, the last full and
/// partial status durations, watches, whether every directory is watched.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GitStats {
    pub commands: u64,
    pub output_bytes: u64,
    pub last_full_ms: u64,
    pub last_partial_ms: u64,
    pub watches: u32,
    pub coverage_complete: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EntryKind {
    /// `1`: changed in the index or the worktree.
    Changed,
    /// `2`: renamed or copied in the index (`orig_path` set).
    Renamed,
    /// `u`: unmerged.
    Unmerged,
    /// `?`: untracked; a path ending in `/` (`untracked_dir`) is a whole untracked directory.
    Untracked,
}

/// One status entry. `record` is git's porcelain v2 line without its path(s), exactly as git
/// printed it (`1 .M N... 100644 100644 100644 <hH> <hI>`, or `?`), so a client can read any
/// field; `xy` and `sub` are copied out of it for convenience.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GitEntry {
    pub path: RelPath,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub orig_path: Option<RelPath>,
    pub kind: EntryKind,
    pub xy: String,
    pub sub: String,
    pub record: String,
    #[serde(default)]
    pub untracked_dir: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Branch {
    pub head: String,
    pub oid: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ahead: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub behind: Option<u64>,
}

/// How far the status can be trusted. `complete`: the last full status finished within its
/// budgets, nothing was cut and every directory is watched. `stale`: something changed that is
/// not read yet, or the last read failed or was cut (the previous good entries are kept).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GitState {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub branch: Option<Branch>,
    pub complete: bool,
    pub stale: bool,
    /// More than 50,000 entries or 8 MiB of output: only the first ones are listed.
    pub truncated: bool,
    /// The last read took longer than the 2-second soft budget.
    pub slow: bool,
    /// Every directory is watched; otherwise changes in some are seen only on a refresh.
    pub coverage_complete: bool,
    pub duration_ms: u64,
}

/// Event: entries added or changed (`upsert`) and paths gone (`removed`), as of `revision`,
/// with the state after it. A large change comes as several events; only the first can say
/// `reset`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Changed {
    pub revision: u64,
    /// Forget every entry before applying this one: the start of a whole status (a new
    /// subscription, or a client that fell behind and lost events).
    pub reset: bool,
    pub upsert: Vec<GitEntry>,
    pub removed: Vec<RelPath>,
    pub state: GitState,
}
