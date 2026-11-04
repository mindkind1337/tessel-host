//! File tree messages, capability `tree`. A client opens a root once, then expands the
//! directories it shows; the host lists each from a cache it keeps current with inotify, and
//! sends what changes in them as revisioned differences. Expanding again reads the cache, not
//! the disk. A directory the host cannot keep current (excluded, over the watch limit, too
//! large) is listed again on each expansion and says so, never shown as empty or complete.
use crate::bytes::{Name, RelPath};
use crate::ids::{RootId, StreamId};
use crate::wire::{ErrorCode, WireError};
use serde::{Deserialize, Serialize};

pub const MAX_ROOT_BYTES: usize = 4096;

pub mod kind {
    pub const ROOT_OPEN: &str = "root.open";
    pub const SUBSCRIBE: &str = "tree.subscribe";
    pub const UNSUBSCRIBE: &str = "tree.unsubscribe";
    pub const EXPAND: &str = "tree.expand";
    pub const COLLAPSE: &str = "tree.collapse";
    pub const STATS: &str = "tree.stats";
    // Replies.
    pub const ROOT_OPENED: &str = "root.opened";
    pub const SUBSCRIBED: &str = "tree.subscribed";
    pub const LISTING: &str = "tree.listing";
    pub const STATS_OK: &str = "tree.stats_ok";
    // Events.
    pub const CHANGED: &str = "tree.changed";
    pub const STALE: &str = "tree.stale";
    pub const RESYNCED: &str = "tree.resynced";
}

/// Open a directory as a root: an absolute path, resolved once. Every later request names
/// paths below it, resolved name by name without following a symbolic link.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RootOpen {
    pub path: String,
}

impl RootOpen {
    pub fn validate(&self) -> Result<(), WireError> {
        if self.path.starts_with('/')
            && self.path.len() <= MAX_ROOT_BYTES
            && !self.path.contains('\0')
        {
            Ok(())
        } else {
            Err(WireError::detail(
                ErrorCode::BadRequest,
                "a root is an absolute path",
            ))
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RootOpened {
    pub root: RootId,
    /// The resolved path, for people to read.
    pub label: String,
    pub limits: TreeLimits,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TreeLimits {
    /// Directories this host keeps current at once (from fs.inotify.max_user_watches and its
    /// own cap). Beyond, directories are listed on demand.
    pub max_watches: u32,
    /// Directory names (or `a/b` path endings) never watched: listed on demand only.
    pub exclusions: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Subscribe {
    pub root: RootId,
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Expand {
    pub root: RootId,
    pub path: RelPath,
}

/// A directory's entries as of `revision`. Apply only `tree.changed` events of this directory
/// with a higher revision (they may arrive before the listing: keep them until it comes).
/// `watched`: kept current by events; otherwise it was read on demand and expanding it again
/// reads it again. `truncated`: more entries than one listing holds; not kept current.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Listing {
    pub path: RelPath,
    pub revision: u64,
    pub entries: Vec<Entry>,
    pub watched: bool,
    pub excluded: bool,
    pub truncated: bool,
}

/// Stop keeping a directory (and those below it) current for this connection.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Collapse {
    pub root: RootId,
    pub path: RelPath,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Stats {
    pub root: RootId,
}

/// What the root costs: directory reads so far, watches, directories cached.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TreeStats {
    pub enumerations: u64,
    pub watches: u32,
    pub cached_dirs: u32,
    pub max_watches: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EntryKind {
    File,
    Dir,
    Symlink,
    Other,
}

/// One entry, never following a symbolic link (a link is a `symlink`, whatever it points to).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Entry {
    pub name: Name,
    pub label: String,
    pub kind: EntryKind,
    pub size: u64,
    pub mtime_ms: u64,
    #[serde(default)]
    pub excluded: bool,
}

/// Event: what changed in one watched directory.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Changed {
    pub revision: u64,
    pub path: RelPath,
    pub added: Vec<Entry>,
    pub updated: Vec<Entry>,
    pub removed: Vec<Name>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StaleReason {
    /// The kernel dropped events and the host could not read this directory again in time.
    Overflow,
    /// This client fell behind: its events were not kept. Expand again (from the cache).
    Lagging,
    /// The directory could not be read or watched any more.
    Error,
}

/// Event: what the client shows of `path` (empty: the whole root) may be out of date; expand
/// it again.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Stale {
    pub revision: u64,
    pub path: RelPath,
    pub reason: StaleReason,
}

/// Event: after the kernel dropped events, every directory still kept was read again and its
/// differences sent; the tree is complete again as of `revision`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Resynced {
    pub revision: u64,
}
