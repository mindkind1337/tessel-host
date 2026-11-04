//! Editor messages, capability `editor` (paths below a root opened with `root.open`). Files
//! are read as their original bytes, a chunk per request, each chunk saying which version of
//! the file it came from. A save is an upload to a temporary file in the same directory, then
//! one atomic rename, allowed only if the file is still the one the client read (same version,
//! or same content): otherwise `conflict`, never a silent overwrite.
use crate::bytes::{Chunk, MAX_CHUNK_BYTES, RelPath};
use crate::ids::{Digest, FileVersion, RootId, UploadId};
use crate::tree::EntryKind;
use crate::wire::{ErrorCode, WireError};
use serde::{Deserialize, Serialize};

/// Largest file this host uploads, or hashes for a save or a comparison.
pub const MAX_FILE_BYTES: u64 = 512 * 1024 * 1024;

pub mod kind {
    pub const STAT: &str = "file.stat";
    pub const READ: &str = "file.read";
    pub const WRITE_BEGIN: &str = "file.write_begin";
    pub const WRITE_CHUNK: &str = "file.write_chunk";
    pub const WRITE_COMMIT: &str = "file.write_commit";
    pub const WRITE_ABORT: &str = "file.write_abort";
    pub const COMPARE: &str = "file.compare";
    pub const RENAME: &str = "file.rename";
    pub const REMOVE: &str = "file.remove";
    pub const MKDIR: &str = "file.mkdir";
    // Replies.
    pub const INFO: &str = "file.info";
    pub const CHUNK: &str = "file.chunk";
    pub const UPLOAD: &str = "file.upload";
    pub const WRITTEN: &str = "file.written";
    pub const COMPARED: &str = "file.compared";
}

fn bad(detail: &'static str) -> WireError {
    WireError::detail(ErrorCode::BadRequest, detail)
}

fn not_root(path: &RelPath) -> Result<(), WireError> {
    if path.is_root() {
        Err(bad("a file needs a name below the root"))
    } else {
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Stat {
    pub root: RootId,
    pub path: RelPath,
}

/// `version` changes whenever the file does (its identity, size or times): equal versions are
/// the same file content.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Info {
    pub kind: EntryKind,
    pub size: u64,
    pub mtime_ms: u64,
    pub version: FileVersion,
}

/// One chunk, at most 64 KiB, from `offset`. Stop reading when you like; a chunk whose
/// `version` differs from the first one means the file changed meanwhile: start again.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Read {
    pub root: RootId,
    pub path: RelPath,
    pub offset: u64,
    pub length: u32,
}

impl Read {
    pub fn validate(&self) -> Result<(), WireError> {
        not_root(&self.path)?;
        if self.length == 0 || self.length as usize > MAX_CHUNK_BYTES {
            return Err(bad("a chunk is 1 byte to 64 KiB"));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadChunk {
    pub offset: u64,
    pub data: Chunk,
    /// The end of the file was reached.
    pub eof: bool,
    pub size: u64,
    pub version: FileVersion,
}

/// What the client's copy came from: the version it read and the SHA-256 of those bytes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Base {
    pub version: FileVersion,
    pub digest: Digest,
}

/// Start a save of `size` bytes. `base` absent: the file must not exist (a new file).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WriteBegin {
    pub root: RootId,
    pub path: RelPath,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base: Option<Base>,
    pub size: u64,
}

impl WriteBegin {
    pub fn validate(&self) -> Result<(), WireError> {
        not_root(&self.path)?;
        if self.size > MAX_FILE_BYTES {
            return Err(WireError::detail(
                ErrorCode::TooLarge,
                "file too large to save here",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Upload {
    pub upload: UploadId,
}

/// The next bytes, in order: `offset` is what was sent so far.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WriteChunk {
    pub upload: UploadId,
    pub offset: u64,
    pub data: Chunk,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WriteCommit {
    pub upload: UploadId,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WriteAbort {
    pub upload: UploadId,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Written {
    pub version: FileVersion,
    pub digest: Digest,
    pub size: u64,
}

/// Whether the file on the host has the content `digest`, judged by the same authority a save
/// is: the host hashes what is there now.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Compare {
    pub root: RootId,
    pub path: RelPath,
    pub digest: Digest,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Compared {
    pub same: bool,
    pub version: FileVersion,
    pub digest: Digest,
    pub size: u64,
}

/// Rename without ever replacing: an existing `to` is a `conflict`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Rename {
    pub root: RootId,
    pub from: RelPath,
    pub to: RelPath,
}

/// Remove a file (only if it is still `version`, when given) or an empty directory.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Remove {
    pub root: RootId,
    pub path: RelPath,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<FileVersion>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Mkdir {
    pub root: RootId,
    pub path: RelPath,
}

pub fn named(path: &RelPath) -> Result<(), WireError> {
    not_root(path)
}
