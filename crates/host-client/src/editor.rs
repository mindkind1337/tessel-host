//! Reading and saving files on the host. `read` returns the original bytes, the version they
//! came from and their SHA-256; `save` sends a buffer against that base and answers either the
//! new version or `Conflict` (the file changed on the host: never overwritten silently). A
//! save cut by a broken link is sent again whole once reconnected (the host dropped the
//! upload), against the same base: the dirty buffer stays the caller's until `save` succeeds.
use crate::{Client, ClientError};
use sha2::{Digest as _, Sha256};
use std::sync::Mutex;
use tessel_host_protocol::file::{
    self, Base, Compare, Compared, Info, Mkdir, Read, ReadChunk, Remove, Rename, Stat, Upload,
    WriteBegin, WriteChunk, WriteCommit, Written,
};
use tessel_host_protocol::tree::{self, RootOpen, RootOpened};
use tessel_host_protocol::{Digest, ErrorCode, FileVersion, MAX_CHUNK_BYTES, RelPath, RootId};

/// What a client holds of a file: its bytes and where they came from.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FileCopy {
    pub bytes: Vec<u8>,
    pub version: FileVersion,
    pub digest: Digest,
}

impl FileCopy {
    pub fn base(&self) -> Base {
        Base {
            version: self.version.clone(),
            digest: self.digest.clone(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SaveError {
    /// Someone changed (or created, or removed) the file since `base`: show both, let the
    /// person choose; nothing was written.
    Conflict,
    /// No space left, or the quota is used up: nothing was written.
    NoSpace,
    Other(ClientError),
}

pub fn digest_of(bytes: &[u8]) -> Digest {
    let mut out = [0u8; 32];
    out.copy_from_slice(&Sha256::digest(bytes));
    Digest::from_bytes(out)
}

pub struct Editor {
    client: Client,
    path: String,
    root: Mutex<(RootId, tessel_host_protocol::Epoch)>,
}

impl Editor {
    /// Opens `path` (absolute, on the host) as the root files are named below.
    pub fn open(client: &Client, path: &str) -> Result<Self, ClientError> {
        let opened: RootOpened = client.retrying(|| {
            client.ask(
                tree::kind::ROOT_OPEN,
                &RootOpen {
                    path: path.to_owned(),
                },
            )
        })?;
        let epoch = client.epoch().ok_or(ClientError::ConnectionLost)?;
        Ok(Self {
            client: client.clone(),
            path: path.to_owned(),
            root: Mutex::new((opened.root, epoch)),
        })
    }

    /// The root's id on the current daemon (opened again after a restart).
    fn root(&self) -> Result<RootId, ClientError> {
        let epoch = self.client.epoch().ok_or(ClientError::ConnectionLost)?;
        let mut root = self.root.lock().unwrap();
        if root.1 != epoch {
            let opened: RootOpened = self.client.ask(
                tree::kind::ROOT_OPEN,
                &RootOpen {
                    path: self.path.clone(),
                },
            )?;
            *root = (opened.root, epoch);
        }
        Ok(root.0.clone())
    }

    fn call<T: serde::de::DeserializeOwned>(
        &self,
        kind: &str,
        body: impl Fn(RootId) -> serde_json::Value,
    ) -> Result<T, ClientError> {
        self.client.retrying(|| {
            let root = self.root()?;
            self.client.ask(kind, &body(root))
        })
    }

    pub fn stat(&self, path: &RelPath) -> Result<Info, ClientError> {
        self.call(file::kind::STAT, |root| {
            serde_json::to_value(Stat {
                root,
                path: path.clone(),
            })
            .unwrap()
        })
    }

    /// The whole file, read again from the start if it changed while being read.
    pub fn read(&self, path: &RelPath) -> Result<FileCopy, ClientError> {
        'again: for _ in 0..5 {
            let mut bytes = Vec::new();
            let mut version: Option<FileVersion> = None;
            loop {
                let offset = bytes.len() as u64;
                let chunk: ReadChunk = self.call(file::kind::READ, |root| {
                    serde_json::to_value(Read {
                        root,
                        path: path.clone(),
                        offset,
                        length: MAX_CHUNK_BYTES as u32,
                    })
                    .unwrap()
                })?;
                if version.get_or_insert(chunk.version.clone()) != &chunk.version {
                    continue 'again;
                }
                bytes.extend_from_slice(chunk.data.as_bytes());
                if chunk.eof {
                    let digest = digest_of(&bytes);
                    return Ok(FileCopy {
                        bytes,
                        version: version.unwrap(),
                        digest,
                    });
                }
            }
        }
        Err(ClientError::Protocol(
            "the file kept changing while it was read".into(),
        ))
    }

    /// Saves `bytes` over the file read as `base` (None: a new file).
    pub fn save(
        &self,
        path: &RelPath,
        base: Option<Base>,
        bytes: &[u8],
    ) -> Result<Written, SaveError> {
        let deadline = std::time::Instant::now() + self.client.options().request_timeout;
        loop {
            match self.save_once(path, base.clone(), bytes) {
                Err(ClientError::ConnectionLost) if std::time::Instant::now() < deadline => {
                    if !self
                        .client
                        .wait_connected(self.client.options().request_timeout)
                    {
                        return Err(SaveError::Other(ClientError::ConnectionLost));
                    }
                }
                Err(ClientError::Refused(e)) if e.code == ErrorCode::Conflict => {
                    return Err(SaveError::Conflict);
                }
                Err(ClientError::Refused(e)) if e.code == ErrorCode::NoSpace => {
                    return Err(SaveError::NoSpace);
                }
                Err(error) => return Err(SaveError::Other(error)),
                Ok(written) => return Ok(written),
            }
        }
    }

    fn save_once(
        &self,
        path: &RelPath,
        base: Option<Base>,
        bytes: &[u8],
    ) -> Result<Written, ClientError> {
        let root = self.root()?;
        // One link for the whole upload: if it breaks, the host drops the upload and it starts over.
        let link = self.client.link()?;
        let timeout = self.client.options().request_timeout;
        let ask = |kind: &str,
                   body: serde_json::Value|
         -> Result<tessel_host_protocol::RawMessage, ClientError> {
            link.request(tessel_host_protocol::RawMessage::new(kind, body), timeout)
        };
        let upload: Upload = ask(
            file::kind::WRITE_BEGIN,
            serde_json::to_value(WriteBegin {
                root,
                path: path.clone(),
                base,
                size: bytes.len() as u64,
            })
            .unwrap(),
        )?
        .body_as()
        .map_err(ClientError::Refused)?;
        let mut offset = 0usize;
        for piece in bytes.chunks(MAX_CHUNK_BYTES) {
            let chunk = WriteChunk {
                upload: upload.upload.clone(),
                offset: offset as u64,
                data: tessel_host_protocol::Chunk::new(piece.to_vec()).unwrap_or_default(),
            };
            ask(
                file::kind::WRITE_CHUNK,
                serde_json::to_value(chunk).unwrap(),
            )?;
            offset += piece.len();
        }
        ask(
            file::kind::WRITE_COMMIT,
            serde_json::to_value(WriteCommit {
                upload: upload.upload,
            })
            .unwrap(),
        )?
        .body_as()
        .map_err(ClientError::Refused)
    }

    /// Whether the file now has `digest` (the host hashes it).
    pub fn compare(&self, path: &RelPath, digest: &Digest) -> Result<Compared, ClientError> {
        self.call(file::kind::COMPARE, |root| {
            serde_json::to_value(Compare {
                root,
                path: path.clone(),
                digest: digest.clone(),
            })
            .unwrap()
        })
    }

    pub fn rename(&self, from: &RelPath, to: &RelPath) -> Result<(), ClientError> {
        self.call::<serde_json::Value>(file::kind::RENAME, |root| {
            serde_json::to_value(Rename {
                root,
                from: from.clone(),
                to: to.clone(),
            })
            .unwrap()
        })
        .map(|_| ())
    }

    pub fn remove(&self, path: &RelPath, version: Option<FileVersion>) -> Result<(), ClientError> {
        self.call::<serde_json::Value>(file::kind::REMOVE, |root| {
            serde_json::to_value(Remove {
                root,
                path: path.clone(),
                version: version.clone(),
            })
            .unwrap()
        })
        .map(|_| ())
    }

    pub fn mkdir(&self, path: &RelPath) -> Result<(), ClientError> {
        self.call::<serde_json::Value>(file::kind::MKDIR, |root| {
            serde_json::to_value(Mkdir {
                root,
                path: path.clone(),
            })
            .unwrap()
        })
        .map(|_| ())
    }
}
