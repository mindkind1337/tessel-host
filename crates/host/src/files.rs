//! The editor's file operations, below the roots of `tree.rs`. Every file is opened from its
//! directory's descriptor with O_NOFOLLOW (and O_NONBLOCK until it is known to be a regular
//! file, so a FIFO never blocks the daemon). A save goes to a temporary file in the same
//! directory, created exclusively, and becomes the file by one rename, after the file there is
//! checked once more against what the client read. A failed write (no space, quota) removes the
//! temporary file: the file itself was never touched, so it is never truncated. Uploads left by
//! a connection that went, or idle too long, are removed; a journal in the runtime directory
//! lets a restarted daemon remove those of a daemon that died.
use crate::pty::Peer;
use crate::runtime::random_bytes;
use crate::tree::Trees;
use sha2::{Digest as _, Sha256};
use std::collections::HashMap;
use std::ffi::CString;
use std::fs::File;
use std::io::{self, Read};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::os::unix::fs::FileExt;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};
use tessel_host_protocol::file::{
    self, Compare, Compared, Info, MAX_FILE_BYTES, Read as ReadRequest, ReadChunk, Upload,
    WriteBegin, WriteChunk, Written,
};
use tessel_host_protocol::tree::EntryKind;
use tessel_host_protocol::{
    Chunk, Digest, ErrorCode, FileVersion, RelPath, RootId, UploadId, WireError,
};

const MAX_UPLOADS: usize = 16;
const MAX_UPLOAD_TOTAL: u64 = 1024 * 1024 * 1024;
const DEFAULT_IDLE: Duration = Duration::from_secs(10 * 60);
const TEMP_PREFIX: &str = ".tessel-upload-";
const TEMP_SUFFIX: &str = ".tmp";
const NEW_FILE_MODE: libc::mode_t = 0o644;
const NEW_DIR_MODE: libc::mode_t = 0o755;
const RENAME_NOREPLACE: libc::c_uint = 1;

fn refuse(code: ErrorCode, detail: &'static str) -> WireError {
    WireError::detail(code, detail)
}

fn from_errno(error: &io::Error) -> WireError {
    match error.raw_os_error() {
        Some(libc::ENOENT) => refuse(ErrorCode::NotFound, "no such file"),
        Some(libc::EEXIST) | Some(libc::ENOTEMPTY) => {
            refuse(ErrorCode::Conflict, "something is already there")
        }
        Some(libc::EACCES) | Some(libc::EPERM) | Some(libc::EROFS) => {
            refuse(ErrorCode::PermissionDenied, "not allowed")
        }
        Some(libc::ENOSPC) | Some(libc::EDQUOT) | Some(libc::EFBIG) => {
            WireError::new(ErrorCode::NoSpace)
        }
        Some(libc::ELOOP) | Some(libc::ENOTDIR) => refuse(
            ErrorCode::BadRequest,
            "a symbolic link or a file is on the way",
        ),
        Some(libc::EISDIR) => refuse(ErrorCode::BadRequest, "a directory, not a file"),
        _ => refuse(ErrorCode::Internal, "the file operation failed"),
    }
}

fn last_error() -> io::Error {
    io::Error::last_os_error()
}

fn cname(bytes: &[u8]) -> Result<CString, WireError> {
    CString::new(bytes).map_err(|_| refuse(ErrorCode::BadRequest, "invalid name"))
}

fn fstat(fd: RawFd) -> io::Result<libc::stat> {
    // SAFETY: stat is plain data written by fstat.
    let mut st: libc::stat = unsafe { std::mem::zeroed() };
    if unsafe { libc::fstat(fd, &mut st) } == 0 {
        Ok(st)
    } else {
        Err(last_error())
    }
}

fn fstatat(dir: RawFd, name: &CString) -> io::Result<libc::stat> {
    // SAFETY: as above, without following a final link.
    let mut st: libc::stat = unsafe { std::mem::zeroed() };
    if unsafe { libc::fstatat(dir, name.as_ptr(), &mut st, libc::AT_SYMLINK_NOFOLLOW) } == 0 {
        Ok(st)
    } else {
        Err(last_error())
    }
}

/// Changes whenever the file does: its identity, size, and both times to the nanosecond.
fn version(st: &libc::stat) -> FileVersion {
    let text = format!(
        "{}:{}:{}:{}.{}:{}.{}",
        st.st_dev,
        st.st_ino,
        st.st_size,
        st.st_mtime,
        st.st_mtime_nsec,
        st.st_ctime,
        st.st_ctime_nsec
    );
    let hash = Sha256::digest(text.as_bytes());
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&hash[..16]);
    FileVersion::from_bytes(bytes)
}

fn kind(st: &libc::stat) -> EntryKind {
    match st.st_mode & libc::S_IFMT {
        libc::S_IFREG => EntryKind::File,
        libc::S_IFDIR => EntryKind::Dir,
        libc::S_IFLNK => EntryKind::Symlink,
        _ => EntryKind::Other,
    }
}

fn digest_of(hasher: Sha256) -> Digest {
    let mut bytes = [0u8; 32];
    bytes.copy_from_slice(&hasher.finalize());
    Digest::from_bytes(bytes)
}

/// A regular file, read-only, never through a link, never blocking on a FIFO.
fn open_regular(dir: RawFd, name: &CString) -> Result<File, WireError> {
    // SAFETY: a NUL-terminated name and our own descriptor; the result is wrapped at once.
    let fd = unsafe {
        libc::openat(
            dir,
            name.as_ptr(),
            libc::O_RDONLY | libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK,
        )
    };
    if fd < 0 {
        return Err(from_errno(&last_error()));
    }
    let file = unsafe { File::from_raw_fd(fd) };
    let st = fstat(file.as_raw_fd()).map_err(|e| from_errno(&e))?;
    if st.st_mode & libc::S_IFMT != libc::S_IFREG {
        return Err(refuse(ErrorCode::BadRequest, "not a regular file"));
    }
    // SAFETY: flags on our own descriptor.
    unsafe {
        libc::fcntl(
            fd,
            libc::F_SETFL,
            libc::fcntl(fd, libc::F_GETFL) & !libc::O_NONBLOCK,
        )
    };
    Ok(file)
}

fn hash_file(file: &mut File, size: i64) -> Result<Digest, WireError> {
    if size as u64 > MAX_FILE_BYTES {
        return Err(refuse(
            ErrorCode::TooLarge,
            "file too large to compare here",
        ));
    }
    let mut hasher = Sha256::new();
    let mut buffer = vec![0u8; 256 * 1024];
    let mut total = 0u64;
    loop {
        let n = file.read(&mut buffer).map_err(|e| from_errno(&e))?;
        if n == 0 {
            break;
        }
        total += n as u64;
        if total > MAX_FILE_BYTES {
            return Err(refuse(
                ErrorCode::TooLarge,
                "file too large to compare here",
            ));
        }
        hasher.update(&buffer[..n]);
    }
    Ok(digest_of(hasher))
}

struct Pending {
    peer: u64,
    dir: OwnedFd,
    temp: CString,
    target: CString,
    file: File,
    size: u64,
    written: u64,
    hasher: Sha256,
    base: Option<file::Base>,
    touched: Instant,
    journal: PathBuf,
}

pub struct Files {
    trees: Arc<Trees>,
    uploads: Mutex<HashMap<UploadId, Pending>>,
    journal: PathBuf,
    idle: Duration,
    /// Test knob only: writes beyond this many bytes fail as a full disk would.
    write_limit: Option<u64>,
}

impl Files {
    /// `journal`: a private directory of this daemon's runtime directory.
    pub fn new(trees: Arc<Trees>, journal: PathBuf) -> Arc<Self> {
        let idle = std::env::var("TESSEL_HOST_UPLOAD_IDLE_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .map(Duration::from_millis)
            .unwrap_or(DEFAULT_IDLE);
        let files = Arc::new(Self {
            trees,
            uploads: Mutex::new(HashMap::new()),
            journal,
            idle,
            write_limit: std::env::var("TESSEL_HOST_TEST_WRITE_LIMIT")
                .ok()
                .and_then(|v| v.parse().ok()),
        });
        files.recover();
        let sweeper = files.clone();
        std::thread::spawn(move || sweeper.sweep_loop());
        files
    }

    fn lock(&self) -> MutexGuard<'_, HashMap<UploadId, Pending>> {
        self.uploads
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// The directory holding `path`'s last name, and that name.
    fn parent(&self, root: &RootId, path: &RelPath) -> Result<(OwnedFd, CString), WireError> {
        file::named(path)?;
        let components = path.components();
        let dir = self
            .trees
            .open_dir(root, &components[..components.len() - 1])?;
        Ok((dir, cname(&components[components.len() - 1])?))
    }

    pub fn stat(&self, request: file::Stat) -> Result<Info, WireError> {
        let (dir, name) = self.parent(&request.root, &request.path)?;
        let st = fstatat(dir.as_raw_fd(), &name).map_err(|e| from_errno(&e))?;
        Ok(Info {
            kind: kind(&st),
            size: st.st_size.max(0) as u64,
            mtime_ms: (st.st_mtime.max(0) as u64) * 1000
                + (st.st_mtime_nsec.max(0) as u64) / 1_000_000,
            version: version(&st),
        })
    }

    pub fn read(&self, request: ReadRequest) -> Result<ReadChunk, WireError> {
        request.validate()?;
        let (dir, name) = self.parent(&request.root, &request.path)?;
        let file = open_regular(dir.as_raw_fd(), &name)?;
        let st = fstat(file.as_raw_fd()).map_err(|e| from_errno(&e))?;
        let size = st.st_size.max(0) as u64;
        let mut data = vec![0u8; request.length as usize];
        let mut got = 0usize;
        while got < data.len() {
            match file.read_at(&mut data[got..], request.offset + got as u64) {
                Ok(0) => break,
                Ok(n) => got += n,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return Err(from_errno(&e)),
            }
        }
        data.truncate(got);
        Ok(ReadChunk {
            offset: request.offset,
            eof: got < request.length as usize || request.offset + got as u64 >= size,
            data: Chunk::new(data).unwrap_or_default(),
            size,
            version: version(&st),
        })
    }

    pub fn compare(&self, request: Compare) -> Result<Compared, WireError> {
        let (dir, name) = self.parent(&request.root, &request.path)?;
        let mut file = open_regular(dir.as_raw_fd(), &name)?;
        let st = fstat(file.as_raw_fd()).map_err(|e| from_errno(&e))?;
        let digest = hash_file(&mut file, st.st_size)?;
        Ok(Compared {
            same: digest == request.digest,
            version: version(&st),
            digest,
            size: st.st_size.max(0) as u64,
        })
    }

    /// The file at `name` is still what `base` says (same version, or the same content);
    /// returns the version it was checked at. None: it must not exist.
    fn check_base(
        dir: RawFd,
        name: &CString,
        base: &Option<file::Base>,
    ) -> Result<Option<FileVersion>, WireError> {
        let Some(base) = base else {
            return match fstatat(dir, name) {
                Err(e) if e.raw_os_error() == Some(libc::ENOENT) => Ok(None),
                Ok(_) => Err(refuse(
                    ErrorCode::Conflict,
                    "a file was created there meanwhile",
                )),
                Err(e) => Err(from_errno(&e)),
            };
        };
        let mut file = match open_regular(dir, name) {
            Err(e) if e.code == ErrorCode::NotFound => {
                return Err(refuse(
                    ErrorCode::Conflict,
                    "the file was removed meanwhile",
                ));
            }
            other => other?,
        };
        let st = fstat(file.as_raw_fd()).map_err(|e| from_errno(&e))?;
        let now = version(&st);
        if now != base.version && hash_file(&mut file, st.st_size)? != base.digest {
            return Err(refuse(
                ErrorCode::Conflict,
                "the file changed since it was read",
            ));
        }
        Ok(Some(now))
    }

    fn journal_record(&self, temp: &[u8], root: &RootId, path: &RelPath) -> PathBuf {
        let record = self
            .journal
            .join(format!("{}", std::process::id()) + &format!("-{}", now_nanos()));
        if let Some(root_path) = self.trees.root_path(root) {
            let mut full = root_path.into_os_string().into_encoded_bytes();
            for component in &path.components()[..path.components().len() - 1] {
                full.push(b'/');
                full.extend_from_slice(component);
            }
            full.push(b'/');
            full.extend_from_slice(temp);
            let _ = std::fs::create_dir_all(&self.journal);
            let _ = std::fs::write(&record, full);
        }
        record
    }

    pub fn begin(&self, request: WriteBegin, peer: &Peer) -> Result<Upload, WireError> {
        request.validate()?;
        let (dir, name) = self.parent(&request.root, &request.path)?;
        Self::check_base(dir.as_raw_fd(), &name, &request.base)?;
        {
            let uploads = self.lock();
            let total: u64 = uploads.values().map(|p| p.size).sum();
            if uploads.len() >= MAX_UPLOADS || total + request.size > MAX_UPLOAD_TOTAL {
                return Err(refuse(ErrorCode::Busy, "too many saves in progress"));
            }
        }
        let suffix: [u8; 8] =
            random_bytes().map_err(|_| refuse(ErrorCode::Internal, "no randomness"))?;
        let temp_name = format!(
            "{TEMP_PREFIX}{}{TEMP_SUFFIX}",
            suffix
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<String>()
        );
        let temp = cname(temp_name.as_bytes())?;
        // SAFETY: a NUL-terminated name and our own descriptor; wrapped at once.
        let fd = unsafe {
            libc::openat(
                dir.as_raw_fd(),
                temp.as_ptr(),
                libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_NOFOLLOW | libc::O_CLOEXEC,
                0o600,
            )
        };
        if fd < 0 {
            return Err(from_errno(&last_error()));
        }
        let file = unsafe { File::from_raw_fd(fd) };
        let journal = self.journal_record(temp_name.as_bytes(), &request.root, &request.path);
        let discard = |dir: &OwnedFd| {
            // SAFETY: removing the temporary file we just created.
            unsafe { libc::unlinkat(dir.as_raw_fd(), temp.as_ptr(), 0) };
            let _ = std::fs::remove_file(&journal);
        };
        // Reserve the space now: a full disk or quota shows before anything is sent.
        if request.size > 0 {
            // SAFETY: our own descriptor.
            let result = unsafe { libc::posix_fallocate(fd, 0, request.size as libc::off_t) };
            if result != 0 && result != libc::EOPNOTSUPP && result != libc::EINVAL {
                discard(&dir);
                return Err(from_errno(&io::Error::from_raw_os_error(result)));
            }
        }
        let upload = UploadId::from_bytes(
            random_bytes().map_err(|_| refuse(ErrorCode::Internal, "no randomness"))?,
        );
        self.lock().insert(
            upload.clone(),
            Pending {
                peer: peer.id,
                dir,
                temp,
                target: name,
                file,
                size: request.size,
                written: 0,
                hasher: Sha256::new(),
                base: request.base,
                touched: Instant::now(),
                journal,
            },
        );
        Ok(Upload { upload })
    }

    fn discard(pending: Pending) {
        // SAFETY: removing our own temporary file.
        unsafe { libc::unlinkat(pending.dir.as_raw_fd(), pending.temp.as_ptr(), 0) };
        let _ = std::fs::remove_file(&pending.journal);
    }

    pub fn chunk(&self, request: WriteChunk, peer: u64) -> Result<(), WireError> {
        let mut uploads = self.lock();
        let pending = uploads
            .get_mut(&request.upload)
            .filter(|p| p.peer == peer)
            .ok_or_else(|| refuse(ErrorCode::NotFound, "unknown upload"))?;
        let data = request.data.as_bytes();
        if request.offset != pending.written || pending.written + data.len() as u64 > pending.size {
            let pending = uploads.remove(&request.upload).unwrap();
            Self::discard(pending);
            return Err(refuse(
                ErrorCode::BadRequest,
                "chunks must come in order and within the size announced",
            ));
        }
        let result = match self.write_limit {
            Some(limit) if pending.written + data.len() as u64 > limit => {
                Err(io::Error::from_raw_os_error(libc::ENOSPC))
            }
            _ => pending.file.write_all_at(data, pending.written),
        };
        if let Err(error) = result {
            // The file itself was never touched: only the temporary one goes.
            let pending = uploads.remove(&request.upload).unwrap();
            Self::discard(pending);
            return Err(from_errno(&error));
        }
        pending.written += data.len() as u64;
        pending.hasher.update(data);
        pending.touched = Instant::now();
        Ok(())
    }

    pub fn commit(&self, request: file::WriteCommit, peer: u64) -> Result<Written, WireError> {
        let pending = {
            let mut uploads = self.lock();
            match uploads.get(&request.upload) {
                Some(p) if p.peer == peer => uploads.remove(&request.upload).unwrap(),
                _ => return Err(refuse(ErrorCode::NotFound, "unknown upload")),
            }
        };
        let outcome = (|| {
            if pending.written != pending.size {
                return Err(refuse(ErrorCode::BadRequest, "fewer bytes than announced"));
            }
            pending.file.sync_all().map_err(|e| from_errno(&e))?;
            let dir = pending.dir.as_raw_fd();
            let checked = Self::check_base(dir, &pending.target, &pending.base)?;
            let mode = match &checked {
                Some(_) => fstatat(dir, &pending.target)
                    .map(|st| st.st_mode & 0o7777)
                    .unwrap_or(NEW_FILE_MODE),
                None => NEW_FILE_MODE,
            };
            // SAFETY: our own descriptor.
            unsafe { libc::fchmod(pending.file.as_raw_fd(), mode) };
            let flags = if checked.is_none() {
                RENAME_NOREPLACE
            } else {
                0
            };
            if let Some(version_checked) = &checked {
                // The last look, right before the rename: anything since is a conflict.
                let st = fstatat(dir, &pending.target).map_err(|e| from_errno(&e))?;
                if version(&st) != *version_checked {
                    return Err(refuse(
                        ErrorCode::Conflict,
                        "the file changed during the save",
                    ));
                }
            }
            // SAFETY: renameat2 with names and descriptors we own.
            let result = unsafe {
                libc::syscall(
                    libc::SYS_renameat2,
                    dir,
                    pending.temp.as_ptr(),
                    dir,
                    pending.target.as_ptr(),
                    flags,
                )
            };
            if result != 0 {
                let error = last_error();
                return Err(match error.raw_os_error() {
                    Some(libc::EEXIST) => {
                        refuse(ErrorCode::Conflict, "a file was created there meanwhile")
                    }
                    _ => from_errno(&error),
                });
            }
            // SAFETY: making the rename durable.
            unsafe { libc::fsync(dir) };
            let st = fstat(pending.file.as_raw_fd()).map_err(|e| from_errno(&e))?;
            Ok(Written {
                version: version(&st),
                digest: digest_of(pending.hasher.clone()),
                size: pending.size,
            })
        })();
        match outcome {
            Ok(written) => {
                let _ = std::fs::remove_file(&pending.journal);
                Ok(written)
            }
            Err(error) => {
                Self::discard(pending);
                Err(error)
            }
        }
    }

    pub fn abort(&self, request: file::WriteAbort, peer: u64) -> Result<(), WireError> {
        let mut uploads = self.lock();
        match uploads.get(&request.upload) {
            Some(p) if p.peer == peer => {
                let pending = uploads.remove(&request.upload).unwrap();
                Self::discard(pending);
                Ok(())
            }
            _ => Err(refuse(ErrorCode::NotFound, "unknown upload")),
        }
    }

    /// The connection is gone: its uploads go (a client resends after reconnecting).
    pub fn disconnect(&self, peer: u64) {
        let mut uploads = self.lock();
        let gone: Vec<UploadId> = uploads
            .iter()
            .filter(|(_, p)| p.peer == peer)
            .map(|(id, _)| id.clone())
            .collect();
        for id in gone {
            if let Some(pending) = uploads.remove(&id) {
                Self::discard(pending);
            }
        }
    }

    pub fn rename(&self, request: file::Rename) -> Result<(), WireError> {
        let (from_dir, from) = self.parent(&request.root, &request.from)?;
        let (to_dir, to) = self.parent(&request.root, &request.to)?;
        // SAFETY: names and descriptors we own.
        let result = unsafe {
            libc::syscall(
                libc::SYS_renameat2,
                from_dir.as_raw_fd(),
                from.as_ptr(),
                to_dir.as_raw_fd(),
                to.as_ptr(),
                RENAME_NOREPLACE,
            )
        };
        if result == 0 {
            Ok(())
        } else {
            Err(from_errno(&last_error()))
        }
    }

    pub fn remove(&self, request: file::Remove) -> Result<(), WireError> {
        let (dir, name) = self.parent(&request.root, &request.path)?;
        let st = fstatat(dir.as_raw_fd(), &name).map_err(|e| from_errno(&e))?;
        let is_dir = st.st_mode & libc::S_IFMT == libc::S_IFDIR;
        if let Some(expected) = &request.version
            && version(&st) != *expected
        {
            return Err(refuse(
                ErrorCode::Conflict,
                "the file changed since it was read",
            ));
        }
        let flags = if is_dir { libc::AT_REMOVEDIR } else { 0 };
        // SAFETY: a name in a directory we opened.
        if unsafe { libc::unlinkat(dir.as_raw_fd(), name.as_ptr(), flags) } == 0 {
            Ok(())
        } else {
            Err(from_errno(&last_error()))
        }
    }

    pub fn mkdir(&self, request: file::Mkdir) -> Result<(), WireError> {
        let (dir, name) = self.parent(&request.root, &request.path)?;
        // SAFETY: a name in a directory we opened.
        if unsafe { libc::mkdirat(dir.as_raw_fd(), name.as_ptr(), NEW_DIR_MODE) } == 0 {
            Ok(())
        } else {
            Err(from_errno(&last_error()))
        }
    }

    fn sweep_loop(self: Arc<Self>) {
        let pause = (self.idle / 2).clamp(Duration::from_millis(50), Duration::from_secs(30));
        loop {
            std::thread::sleep(pause);
            let mut uploads = self.lock();
            let idle: Vec<UploadId> = uploads
                .iter()
                .filter(|(_, p)| p.touched.elapsed() > self.idle)
                .map(|(id, _)| id.clone())
                .collect();
            for id in idle {
                if let Some(pending) = uploads.remove(&id) {
                    Self::discard(pending);
                }
            }
        }
    }

    /// Temporary files of a daemon that died: removed if they are still ours by name and kind.
    fn recover(&self) {
        let Ok(records) = std::fs::read_dir(&self.journal) else {
            return;
        };
        for record in records.flatten() {
            if let Ok(target) = std::fs::read(record.path()) {
                let path = PathBuf::from(
                    <std::ffi::OsString as std::os::unix::ffi::OsStringExt>::from_vec(target),
                );
                let name = path
                    .file_name()
                    .map(|n| n.to_string_lossy().into_owned())
                    .unwrap_or_default();
                let ours = name.starts_with(TEMP_PREFIX) && name.ends_with(TEMP_SUFFIX);
                if ours && std::fs::symlink_metadata(&path).is_ok_and(|m| m.file_type().is_file()) {
                    let _ = std::fs::remove_file(&path);
                }
            }
            let _ = std::fs::remove_file(record.path());
        }
    }
}

fn now_nanos() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0)
}
