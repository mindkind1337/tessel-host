//! The daemon's file trees. A root is opened once (resolved, then held as a directory
//! descriptor); every path below it is opened name by name with O_NOFOLLOW from that
//! descriptor, so a symbolic link swapped in is never followed. A directory a client expands
//! is watched (through /proc/self/fd, the very directory opened) before it is read, and the
//! read happens under the same lock the event reader needs, so no change between the watch and
//! the read is lost: queued events are applied after, idempotently, by looking at each name
//! again. One inotify instance serves the daemon; directories are cached while some connection
//! keeps them expanded; expanding again reads the cache. When the kernel drops events, every
//! cached directory is read again (bounded) and its differences sent, then `resynced`.
//! A root stands for its path: before each use, and every two seconds while folders of it are
//! kept current, its descriptor is compared with what the path names, and a folder removed and
//! made again there is opened again (`stale` sent) instead of being read as empty for good.
use crate::pty::Peer;
use crate::runtime::random_bytes;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::ffi::{CStr, CString};
use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};
use tessel_host_protocol::tree::{
    self, Changed, Entry, EntryKind, Listing, Resynced, RootOpened, Stale, StaleReason, Subscribed,
    TreeLimits, TreeStats,
};
use tessel_host_protocol::{
    Ack, ErrorCode, HostFrame, Name, RawMessage, RelPath, RootId, StreamId, StreamPos,
    StreamSender, WireError, encode_frame,
};

pub const EXCLUSIONS: [&str; 6] = [
    "node_modules",
    ".git/objects",
    ".cache",
    "__pycache__",
    ".venv",
    "target",
];
const OWN_WATCH_CAP: usize = 8192;
const MAX_ENTRIES: usize = 20_000;
const MAX_ROOTS: usize = 64;
const RESYNC_DIRS: usize = 512;
const EVENT_OVERHEAD: usize = 256;
const ROOT_CHECK_MS: i32 = 2000;
const ROOT_CHECK: Duration = Duration::from_millis(ROOT_CHECK_MS as u64);
const WATCH_MASK: u32 = libc::IN_CREATE
    | libc::IN_DELETE
    | libc::IN_MOVED_FROM
    | libc::IN_MOVED_TO
    | libc::IN_MODIFY
    | libc::IN_ATTRIB
    | libc::IN_CLOSE_WRITE
    | libc::IN_DELETE_SELF
    | libc::IN_MOVE_SELF
    | libc::IN_ONLYDIR
    | libc::IN_EXCL_UNLINK;

fn refuse(code: ErrorCode, detail: &'static str) -> WireError {
    WireError::detail(code, detail)
}

fn from_errno(error: &io::Error) -> WireError {
    match error.raw_os_error() {
        Some(libc::ENOENT) => refuse(ErrorCode::NotFound, "no such directory"),
        Some(libc::EACCES) | Some(libc::EPERM) => {
            refuse(ErrorCode::PermissionDenied, "this directory cannot be read")
        }
        Some(libc::ELOOP) | Some(libc::ENOTDIR) => refuse(
            ErrorCode::BadRequest,
            "not a directory (symbolic links are not followed)",
        ),
        _ => refuse(ErrorCode::Internal, "the directory could not be read"),
    }
}

/// Watches this daemon may hold: its own cap, a quarter of the user's inotify budget (other
/// programs watch too), and the descriptors it may open. TESSEL_HOST_MAX_WATCHES lowers it.
pub(crate) fn max_watches() -> usize {
    let kernel = std::fs::read_to_string("/proc/sys/fs/inotify/max_user_watches")
        .ok()
        .and_then(|s| s.trim().parse::<usize>().ok())
        .map(|n| n / 4)
        .unwrap_or(OWN_WATCH_CAP);
    // SAFETY: rlimit is plain data written by getrlimit.
    let mut limit: libc::rlimit = unsafe { std::mem::zeroed() };
    let files = if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) } == 0 {
        (limit.rlim_cur as usize).saturating_sub(256)
    } else {
        768
    };
    let configured = std::env::var("TESSEL_HOST_MAX_WATCHES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(usize::MAX);
    OWN_WATCH_CAP.min(kernel).min(files).min(configured).max(1)
}

/// Raise the soft limit on open files to the hard one: each kept directory holds one.
pub fn raise_file_limit() {
    // SAFETY: rlimit is plain data.
    unsafe {
        let mut limit: libc::rlimit = std::mem::zeroed();
        if libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) == 0 && limit.rlim_cur < limit.rlim_max
        {
            limit.rlim_cur = limit.rlim_max;
            libc::setrlimit(libc::RLIMIT_NOFILE, &limit);
        }
    }
}

pub fn is_excluded(path: &RelPath) -> bool {
    let components = path.components();
    let Some(last) = components.last() else {
        return false;
    };
    EXCLUSIONS.iter().any(|rule| match rule.split_once('/') {
        None => last.as_slice() == rule.as_bytes(),
        Some((parent, name)) => {
            components.len() >= 2
                && last.as_slice() == name.as_bytes()
                && components[components.len() - 2].as_slice() == parent.as_bytes()
        }
    })
}

fn openat_dir(parent: RawFd, name: &[u8]) -> io::Result<OwnedFd> {
    let name = CString::new(name).map_err(|_| io::Error::from_raw_os_error(libc::EINVAL))?;
    // SAFETY: a NUL-terminated name, a descriptor we own; the result is wrapped at once.
    let fd = unsafe {
        libc::openat(
            parent,
            name.as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
        )
    };
    if fd < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(unsafe { OwnedFd::from_raw_fd(fd) })
    }
}

fn dup(fd: RawFd) -> io::Result<OwnedFd> {
    // SAFETY: duplicating a descriptor we own.
    let copy = unsafe { libc::fcntl(fd, libc::F_DUPFD_CLOEXEC, 0) };
    if copy < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(unsafe { OwnedFd::from_raw_fd(copy) })
    }
}

type Identity = (libc::dev_t, libc::ino_t);

/// The directory a path names now (following links, as opening it does).
fn path_identity(path: &std::path::Path) -> io::Result<Identity> {
    let cpath = CString::new(path.as_os_str().as_encoded_bytes())
        .map_err(|_| io::Error::from_raw_os_error(libc::EINVAL))?;
    // SAFETY: stat is plain data written by stat; a NUL-terminated path.
    let mut st: libc::stat = unsafe { std::mem::zeroed() };
    if unsafe { libc::stat(cpath.as_ptr(), &mut st) } != 0 {
        return Err(io::Error::last_os_error());
    }
    if st.st_mode & libc::S_IFMT != libc::S_IFDIR {
        return Err(io::Error::from_raw_os_error(libc::ENOTDIR));
    }
    Ok((st.st_dev, st.st_ino))
}

/// The directory a descriptor holds, and whether it still has a name: a removed directory
/// stays readable through a descriptor, and reads as empty.
fn fd_identity(fd: RawFd) -> io::Result<(Identity, bool)> {
    // SAFETY: stat is plain data written by fstat on a descriptor we own.
    let mut st: libc::stat = unsafe { std::mem::zeroed() };
    if unsafe { libc::fstat(fd, &mut st) } != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(((st.st_dev, st.st_ino), st.st_nlink > 0))
}

fn open_root_dir(path: &std::path::Path) -> Result<OwnedFd, WireError> {
    let cpath = CString::new(path.as_os_str().as_encoded_bytes())
        .map_err(|_| refuse(ErrorCode::BadRequest, "invalid root"))?;
    // SAFETY: a NUL-terminated path; the result is wrapped at once.
    let fd = unsafe {
        libc::open(
            cpath.as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC,
        )
    };
    if fd < 0 {
        return Err(from_errno(&io::Error::last_os_error()));
    }
    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}

fn stat_entry(dir: RawFd, name: &[u8], path: &RelPath) -> Option<Entry> {
    let name = Name::new(name.to_vec())?;
    let cname = CString::new(name.as_bytes()).ok()?;
    // SAFETY: stat is plain data written by fstatat.
    let mut st: libc::stat = unsafe { std::mem::zeroed() };
    if unsafe { libc::fstatat(dir, cname.as_ptr(), &mut st, libc::AT_SYMLINK_NOFOLLOW) } != 0 {
        return None;
    }
    let kind = match st.st_mode & libc::S_IFMT {
        libc::S_IFREG => EntryKind::File,
        libc::S_IFDIR => EntryKind::Dir,
        libc::S_IFLNK => EntryKind::Symlink,
        _ => EntryKind::Other,
    };
    let mtime_ms =
        (st.st_mtime.max(0) as u64) * 1000 + (st.st_mtime_nsec.max(0) as u64) / 1_000_000;
    let excluded = kind == EntryKind::Dir && path.child(&name).is_some_and(|p| is_excluded(&p));
    Some(Entry {
        label: name.label(),
        name,
        kind,
        size: st.st_size.max(0) as u64,
        mtime_ms,
        excluded,
    })
}

/// Reads a directory from its descriptor (from the start), at most MAX_ENTRIES entries.
fn enumerate(dir: RawFd, path: &RelPath) -> io::Result<(BTreeMap<Name, Entry>, bool)> {
    let copy = dup(dir)?;
    // SAFETY: fdopendir takes ownership of `copy`; closedir releases it.
    let stream = unsafe { libc::fdopendir(copy.as_raw_fd()) };
    if stream.is_null() {
        return Err(io::Error::last_os_error());
    }
    std::mem::forget(copy);
    let mut entries = BTreeMap::new();
    let mut truncated = false;
    // SAFETY: a valid DIR stream until closedir; names are NUL-terminated by readdir.
    unsafe {
        libc::rewinddir(stream);
        loop {
            *libc::__errno_location() = 0;
            let item = libc::readdir(stream);
            if item.is_null() {
                let error = io::Error::last_os_error();
                libc::closedir(stream);
                if error.raw_os_error().unwrap_or(0) != 0 {
                    return Err(error);
                }
                break;
            }
            let name = CStr::from_ptr((*item).d_name.as_ptr()).to_bytes();
            if name == b"." || name == b".." {
                continue;
            }
            if entries.len() >= MAX_ENTRIES {
                truncated = true;
                continue;
            }
            if let Some(entry) = stat_entry(dir, name, path) {
                entries.insert(entry.name.clone(), entry);
            }
        }
    }
    Ok((entries, truncated))
}

struct Dir {
    fd: OwnedFd,
    wd: i32,
    entries: BTreeMap<Name, Entry>,
    peers: HashSet<u64>,
}

struct Sub {
    stream: StreamId,
    peer: Peer,
    sender: StreamSender,
    lagging: bool,
}

struct Root {
    fd: OwnedFd,
    path: PathBuf,
    label: String,
    revision: u64,
    dirs: HashMap<RelPath, Dir>,
    subs: Vec<Sub>,
    enumerations: u64,
}

struct State {
    roots: HashMap<RootId, Root>,
    wds: HashMap<i32, (RootId, RelPath)>,
}

pub struct Trees {
    state: Mutex<State>,
    inotify: OwnedFd,
    max_watches: usize,
}

/// Events for one subscription, each within the client's frame: a large change is sent as
/// several `tree.changed` events, each with its own revision.
fn send_event(sub: &mut Sub, kind: &str, body: &impl serde::Serialize) {
    if sub.lagging || sub.peer.out.is_closed() {
        return;
    }
    let message = RawMessage::of(kind, body);
    let estimate = serde_json::to_vec(&message)
        .map(|v| v.len())
        .unwrap_or(usize::MAX);
    let Ok(seq) = sub.sender.reserve(estimate + EVENT_OVERHEAD) else {
        // Not kept: this client is told to expand again once it catches up.
        sub.lagging = true;
        return;
    };
    let frame = HostFrame::event(
        StreamPos {
            id: sub.stream.clone(),
            seq,
        },
        message,
    );
    if let Ok(bytes) = encode_frame(&frame, sub.peer.frame_limit) {
        sub.peer.out.event(bytes);
    }
}

impl Root {
    fn emit_changes(
        &mut self,
        path: &RelPath,
        added: Vec<Entry>,
        updated: Vec<Entry>,
        removed: Vec<Name>,
    ) {
        let budget = self
            .subs
            .iter()
            .map(|s| s.peer.frame_limit)
            .min()
            .unwrap_or(1 << 20)
            / 2;
        let mut parts: Vec<Changed> = Vec::new();
        let mut current = Changed {
            revision: 0,
            path: path.clone(),
            added: Vec::new(),
            updated: Vec::new(),
            removed: Vec::new(),
        };
        let mut size = 0usize;
        let mut flush = |current: &mut Changed, size: &mut usize| {
            if !current.added.is_empty()
                || !current.updated.is_empty()
                || !current.removed.is_empty()
            {
                let next = Changed {
                    revision: 0,
                    path: path.clone(),
                    added: Vec::new(),
                    updated: Vec::new(),
                    removed: Vec::new(),
                };
                parts.push(std::mem::replace(current, next));
            }
            *size = 0;
        };
        enum Item {
            Added(Entry),
            Updated(Entry),
            Removed(Name),
        }
        let items = added
            .into_iter()
            .map(Item::Added)
            .chain(updated.into_iter().map(Item::Updated))
            .chain(removed.into_iter().map(Item::Removed));
        for item in items {
            let cost = match &item {
                Item::Added(e) | Item::Updated(e) => {
                    serde_json::to_vec(e).map(|v| v.len()).unwrap_or(1024)
                }
                Item::Removed(n) => n.as_bytes().len() * 2 + 4,
            } + 8;
            if size + cost > budget {
                flush(&mut current, &mut size);
            }
            size += cost;
            match item {
                Item::Added(e) => current.added.push(e),
                Item::Updated(e) => current.updated.push(e),
                Item::Removed(n) => current.removed.push(n),
            }
        }
        flush(&mut current, &mut size);
        for mut part in parts {
            self.revision += 1;
            part.revision = self.revision;
            for sub in &mut self.subs {
                send_event(sub, tree::kind::CHANGED, &part);
            }
        }
    }

    fn emit_stale(&mut self, path: &RelPath, reason: StaleReason) {
        self.revision += 1;
        let stale = Stale {
            revision: self.revision,
            path: path.clone(),
            reason,
        };
        for sub in &mut self.subs {
            send_event(sub, tree::kind::STALE, &stale);
        }
    }
}

impl State {
    /// Forgets `path` and every cached directory below it, with their watches.
    fn drop_dirs(&mut self, inotify: RawFd, root: &RootId, path: &RelPath) {
        let Some(r) = self.roots.get_mut(root) else {
            return;
        };
        let gone: Vec<RelPath> = r
            .dirs
            .keys()
            .filter(|p| p.starts_with(path))
            .cloned()
            .collect();
        for p in gone {
            if let Some(dir) = r.dirs.remove(&p) {
                self.wds.remove(&dir.wd);
                // SAFETY: removing our own watch; an already removed one only returns an error.
                unsafe { libc::inotify_rm_watch(inotify, dir.wd) };
            }
        }
    }
}

impl Trees {
    pub fn new() -> io::Result<Arc<Self>> {
        // SAFETY: the result is wrapped at once.
        let fd = unsafe { libc::inotify_init1(libc::IN_NONBLOCK | libc::IN_CLOEXEC) };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        let trees = Arc::new(Self {
            state: Mutex::new(State {
                roots: HashMap::new(),
                wds: HashMap::new(),
            }),
            inotify: unsafe { OwnedFd::from_raw_fd(fd) },
            max_watches: max_watches(),
        });
        let reader = trees.clone();
        std::thread::spawn(move || reader.read_loop());
        Ok(trees)
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn limits(&self) -> TreeLimits {
        TreeLimits {
            max_watches: self.max_watches as u32,
            exclusions: EXCLUSIONS.iter().map(|s| s.to_string()).collect(),
        }
    }

    pub fn open_root(&self, request: tree::RootOpen) -> Result<RootOpened, WireError> {
        request.validate()?;
        let path = std::fs::canonicalize(&request.path).map_err(|e| from_errno(&e))?;
        let mut state = self.lock();
        let existing = state
            .roots
            .iter()
            .find(|(_, r)| r.path == path)
            .map(|(id, _)| id.clone());
        if let Some(id) = existing {
            self.ensure_current(&mut state, &id)?;
            return Ok(RootOpened {
                label: state.roots[&id].label.clone(),
                root: id,
                limits: self.limits(),
            });
        }
        if state.roots.len() >= MAX_ROOTS {
            return Err(refuse(ErrorCode::Busy, "too many roots"));
        }
        let fd = open_root_dir(&path)?;
        let id = RootId::from_bytes(
            random_bytes().map_err(|_| refuse(ErrorCode::Internal, "no randomness"))?,
        );
        let label = path.to_string_lossy().into_owned();
        state.roots.insert(
            id.clone(),
            Root {
                fd,
                path,
                label: label.clone(),
                revision: 0,
                dirs: HashMap::new(),
                subs: Vec::new(),
                enumerations: 0,
            },
        );
        Ok(RootOpened {
            root: id,
            label,
            limits: self.limits(),
        })
    }

    /// A root is held by descriptor but stands for its path. When the folder at that path is
    /// removed and made again (a fresh clone, a deployment), the descriptor still holds the
    /// removed directory, which reads as empty for good. So before each use the root is checked
    /// against its path: another directory there is opened instead (the cache of the old one
    /// dropped, its viewers told to expand again); nothing there any more is refused, never
    /// listed as empty.
    fn ensure_current(&self, state: &mut State, id: &RootId) -> Result<(), WireError> {
        let root = state
            .roots
            .get(id)
            .ok_or_else(|| refuse(ErrorCode::NotFound, "unknown root"))?;
        let gone = || {
            refuse(
                ErrorCode::NotFound,
                "the root folder is gone; open it again",
            )
        };
        let named = path_identity(&root.path).map_err(|_| gone())?;
        if let Ok((held, linked)) = fd_identity(root.fd.as_raw_fd())
            && linked
            && held == named
        {
            return Ok(());
        }
        // Opened again only while the path is still its own canonical form: a folder on the
        // way replaced by a link leads elsewhere, and is not followed.
        let path = root.path.clone();
        if std::fs::canonicalize(&path).ok().as_deref() != Some(path.as_path()) {
            return Err(gone());
        }
        let fd = open_root_dir(&path)?;
        match fd_identity(fd.as_raw_fd()) {
            Ok((now, true)) if now == named => {}
            // Changed again meanwhile: the next use looks again.
            _ => return Err(gone()),
        }
        let cached = !root.dirs.is_empty();
        state.drop_dirs(self.inotify.as_raw_fd(), id, &RelPath::root());
        if let Some(root) = state.roots.get_mut(id) {
            root.fd = fd;
            if cached {
                root.emit_stale(&RelPath::root(), StaleReason::Error);
            }
        }
        Ok(())
    }

    pub fn subscribe(
        &self,
        request: tree::Subscribe,
        peer: &Peer,
    ) -> Result<Subscribed, WireError> {
        let mut state = self.lock();
        let root = state
            .roots
            .get_mut(&request.root)
            .ok_or_else(|| refuse(ErrorCode::NotFound, "unknown root"))?;
        let stream = StreamId::from_bytes(
            random_bytes().map_err(|_| refuse(ErrorCode::Internal, "no randomness"))?,
        );
        root.subs.push(Sub {
            stream: stream.clone(),
            peer: peer.clone(),
            sender: StreamSender::new(peer.window_bytes),
            lagging: false,
        });
        Ok(Subscribed {
            stream,
            revision: root.revision,
        })
    }

    pub fn unsubscribe(&self, peer: u64, request: &tree::Unsubscribe) -> Result<(), WireError> {
        let mut state = self.lock();
        for root in state.roots.values_mut() {
            let before = root.subs.len();
            root.subs
                .retain(|s| !(s.stream == request.stream && s.peer.id == peer));
            if root.subs.len() != before {
                return Ok(());
            }
        }
        Err(refuse(ErrorCode::NotFound, "unknown stream"))
    }

    /// Ok(false): not a tree stream.
    pub fn ack(&self, peer: u64, ack: &Ack) -> Result<bool, WireError> {
        let mut state = self.lock();
        for root in state.roots.values_mut() {
            let revision = root.revision;
            if let Some(sub) = root
                .subs
                .iter_mut()
                .find(|s| s.stream == ack.stream && s.peer.id == peer)
            {
                sub.sender.ack(ack.seq).map_err(|_| {
                    refuse(ErrorCode::BadFrame, "acknowledgment of an event never sent")
                })?;
                if sub.lagging && sub.sender.unacked_bytes() == 0 {
                    sub.lagging = false;
                    let stale = Stale {
                        revision,
                        path: RelPath::root(),
                        reason: StaleReason::Lagging,
                    };
                    send_event(sub, tree::kind::STALE, &stale);
                }
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub fn expand(&self, request: tree::Expand, peer: &Peer) -> Result<Listing, WireError> {
        let mut guard = self.lock();
        let state = &mut *guard;
        self.ensure_current(state, &request.root)?;
        let root = state
            .roots
            .get_mut(&request.root)
            .ok_or_else(|| refuse(ErrorCode::NotFound, "unknown root"))?;
        let path = request.path;
        let excluded = is_excluded(&path)
            || path
                .components()
                .iter()
                .any(|c| EXCLUSIONS.contains(&String::from_utf8_lossy(c).as_ref()));
        if let Some(dir) = root.dirs.get_mut(&path) {
            dir.peers.insert(peer.id);
            return Ok(Listing {
                path,
                revision: root.revision,
                entries: dir.entries.values().cloned().collect(),
                watched: true,
                excluded: false,
                truncated: false,
            });
        }
        let mut fd = dup(root.fd.as_raw_fd()).map_err(|e| from_errno(&e))?;
        for name in path.components() {
            fd = openat_dir(fd.as_raw_fd(), name).map_err(|e| from_errno(&e))?;
        }
        let watches = state.wds.len();
        let mut wd = -1;
        if !excluded && watches < self.max_watches {
            let link =
                CString::new(format!("/proc/self/fd/{}", fd.as_raw_fd())).unwrap_or_default();
            // SAFETY: a NUL-terminated path naming the directory just opened.
            wd = unsafe {
                libc::inotify_add_watch(self.inotify.as_raw_fd(), link.as_ptr(), WATCH_MASK)
            };
        }
        root.enumerations += 1;
        let (entries, truncated) = match enumerate(fd.as_raw_fd(), &path) {
            Ok(read) => read,
            Err(error) => {
                if wd >= 0 {
                    // SAFETY: our own watch.
                    unsafe { libc::inotify_rm_watch(self.inotify.as_raw_fd(), wd) };
                }
                return Err(from_errno(&error));
            }
        };
        // A listing too large to send whole is cut; it is then not kept current.
        let mut list: Vec<Entry> = entries.values().cloned().collect();
        let budget = peer.frame_limit / 2;
        let mut cut = truncated;
        while !list.is_empty()
            && serde_json::to_vec(&list)
                .map(|v| v.len())
                .unwrap_or(usize::MAX)
                > budget
        {
            list.truncate(list.len() / 2);
            cut = true;
        }
        let watched = wd >= 0 && !cut;
        if watched {
            // The same directory may already be watched under another path (a bind mount, or a
            // second root): the kernel then returns the same descriptor; one path keeps it.
            if state.wds.contains_key(&wd) {
                return Ok(Listing {
                    path,
                    revision: root.revision,
                    entries: list,
                    watched: false,
                    excluded,
                    truncated: cut,
                });
            }
            state.wds.insert(wd, (request.root.clone(), path.clone()));
            root.dirs.insert(
                path.clone(),
                Dir {
                    fd,
                    wd,
                    entries,
                    peers: HashSet::from([peer.id]),
                },
            );
        } else if wd >= 0 && !state.wds.contains_key(&wd) {
            // SAFETY: our own watch.
            unsafe { libc::inotify_rm_watch(self.inotify.as_raw_fd(), wd) };
        }
        Ok(Listing {
            path,
            revision: root.revision,
            entries: list,
            watched,
            excluded,
            truncated: cut,
        })
    }

    pub fn collapse(&self, request: tree::Collapse, peer: u64) -> Result<(), WireError> {
        let mut state = self.lock();
        let root = state
            .roots
            .get_mut(&request.root)
            .ok_or_else(|| refuse(ErrorCode::NotFound, "unknown root"))?;
        let mut unused = Vec::new();
        for (path, dir) in root.dirs.iter_mut() {
            if path.starts_with(&request.path) {
                dir.peers.remove(&peer);
                if dir.peers.is_empty() {
                    unused.push(path.clone());
                }
            }
        }
        for path in unused {
            state.drop_dirs(self.inotify.as_raw_fd(), &request.root, &path);
        }
        Ok(())
    }

    /// A directory below a root, opened name by name without following links.
    pub fn open_dir(&self, root: &RootId, components: &[Vec<u8>]) -> Result<OwnedFd, WireError> {
        let mut fd = {
            let mut state = self.lock();
            self.ensure_current(&mut state, root)?;
            let root = state
                .roots
                .get(root)
                .ok_or_else(|| refuse(ErrorCode::NotFound, "unknown root"))?;
            dup(root.fd.as_raw_fd()).map_err(|e| from_errno(&e))?
        };
        for name in components {
            fd = openat_dir(fd.as_raw_fd(), name).map_err(|e| from_errno(&e))?;
        }
        Ok(fd)
    }

    pub fn root_path(&self, root: &RootId) -> Option<PathBuf> {
        self.lock().roots.get(root).map(|r| r.path.clone())
    }

    pub fn stats(&self, request: tree::Stats) -> Result<TreeStats, WireError> {
        let state = self.lock();
        let root = state
            .roots
            .get(&request.root)
            .ok_or_else(|| refuse(ErrorCode::NotFound, "unknown root"))?;
        Ok(TreeStats {
            enumerations: root.enumerations,
            watches: state.wds.len() as u32,
            cached_dirs: root.dirs.len() as u32,
            max_watches: self.max_watches as u32,
        })
    }

    /// The connection is gone: its subscriptions go, and the directories only it kept.
    pub fn disconnect(&self, peer: u64) {
        let mut state = self.lock();
        let mut unused = Vec::new();
        for (id, root) in state.roots.iter_mut() {
            root.subs.retain(|s| s.peer.id != peer);
            for (path, dir) in root.dirs.iter_mut() {
                dir.peers.remove(&peer);
                if dir.peers.is_empty() {
                    unused.push((id.clone(), path.clone()));
                }
            }
        }
        for (id, path) in unused {
            state.drop_dirs(self.inotify.as_raw_fd(), &id, &path);
        }
    }

    /// The kernel reports a removed directory to its watch only once its last descriptor is
    /// closed, and the daemon keeps its roots open: a root folder removed (and made again) sends
    /// nothing, and no parent folder is watched to say so. So roots whose folders are kept
    /// current are compared with their paths every ROOT_CHECK (two system calls each).
    fn check_roots(&self) {
        let mut guard = self.lock();
        let state = &mut *guard;
        let kept: Vec<RootId> = state
            .roots
            .iter()
            .filter(|(_, r)| !r.dirs.is_empty())
            .map(|(id, _)| id.clone())
            .collect();
        for id in kept {
            if self.ensure_current(state, &id).is_err() {
                // Nothing at its path any more: what its viewers show is out of date.
                state.drop_dirs(self.inotify.as_raw_fd(), &id, &RelPath::root());
                if let Some(root) = state.roots.get_mut(&id) {
                    root.emit_stale(&RelPath::root(), StaleReason::Error);
                }
            }
        }
    }

    fn read_loop(self: Arc<Self>) {
        let delay = std::env::var("TESSEL_HOST_TEST_INOTIFY_DELAY_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok());
        let fd = self.inotify.as_raw_fd();
        let mut buffer = vec![0u8; 256 * 1024];
        let mut checked = Instant::now();
        loop {
            if checked.elapsed() >= ROOT_CHECK {
                self.check_roots();
                checked = Instant::now();
            }
            let mut poll = libc::pollfd {
                fd,
                events: libc::POLLIN,
                revents: 0,
            };
            // SAFETY: one valid pollfd.
            if unsafe { libc::poll(&mut poll, 1, ROOT_CHECK_MS) } <= 0 {
                continue; // interrupted, or time to look at the roots
            }
            if let Some(ms) = delay {
                // Test knob only: lets the kernel queue fill, to exercise overflow recovery.
                std::thread::sleep(Duration::from_millis(ms));
            }
            let mut events = Vec::new();
            loop {
                // SAFETY: reading into our own buffer.
                let n = unsafe { libc::read(fd, buffer.as_mut_ptr().cast(), buffer.len()) };
                if n <= 0 {
                    break;
                }
                let mut at = 0usize;
                let n = n as usize;
                let header = std::mem::size_of::<libc::inotify_event>();
                while at + header <= n {
                    // SAFETY: the kernel writes whole events; read_unaligned copes with alignment.
                    let event: libc::inotify_event =
                        unsafe { std::ptr::read_unaligned(buffer.as_ptr().add(at).cast()) };
                    let len = event.len as usize;
                    let raw = &buffer[at + header..(at + header + len).min(n)];
                    let name = raw.split(|&b| b == 0).next().unwrap_or(&[]).to_vec();
                    events.push((event.wd, event.mask, name));
                    at += header + len;
                }
            }
            if !events.is_empty() {
                self.apply(events);
            }
        }
    }

    fn apply(&self, events: Vec<(i32, u32, Vec<u8>)>) {
        let mut guard = self.lock();
        let state = &mut *guard;
        let inotify = self.inotify.as_raw_fd();
        if events
            .iter()
            .any(|(_, mask, _)| mask & libc::IN_Q_OVERFLOW != 0)
        {
            self.resync(state);
            return;
        }
        // Names touched per directory, then each looked at once: idempotent, so events queued
        // while a directory was being read change nothing that read already saw.
        let mut touched: BTreeMap<(RootId, RelPath), BTreeSet<Vec<u8>>> = BTreeMap::new();
        let mut gone: Vec<(RootId, RelPath)> = Vec::new();
        for (wd, mask, name) in events {
            let Some((root, path)) = state.wds.get(&wd).cloned() else {
                continue;
            };
            if mask
                & (libc::IN_DELETE_SELF | libc::IN_MOVE_SELF | libc::IN_IGNORED | libc::IN_UNMOUNT)
                != 0
            {
                gone.push((root, path));
            } else if !name.is_empty() {
                touched.entry((root, path)).or_default().insert(name);
            }
        }
        for ((root_id, path), names) in touched {
            let Some(root) = state.roots.get_mut(&root_id) else {
                continue;
            };
            let Some(dir) = root.dirs.get_mut(&path) else {
                continue;
            };
            let (mut added, mut updated, mut removed) = (Vec::new(), Vec::new(), Vec::new());
            let mut lost_dirs = Vec::new();
            for raw in names {
                let Some(name) = Name::new(raw.clone()) else {
                    continue;
                };
                let now = stat_entry(dir.fd.as_raw_fd(), &raw, &path);
                match (dir.entries.get(&name), now) {
                    (None, Some(entry)) => {
                        dir.entries.insert(name, entry.clone());
                        added.push(entry);
                    }
                    (Some(before), Some(entry)) if *before != entry => {
                        if before.kind == EntryKind::Dir && entry.kind != EntryKind::Dir {
                            lost_dirs.push(path.child(&name));
                        }
                        dir.entries.insert(name, entry.clone());
                        updated.push(entry);
                    }
                    (Some(before), None) => {
                        if before.kind == EntryKind::Dir {
                            lost_dirs.push(path.child(&name));
                        }
                        dir.entries.remove(&name);
                        removed.push(name);
                    }
                    _ => {}
                }
            }
            if !added.is_empty() || !updated.is_empty() || !removed.is_empty() {
                root.emit_changes(&path, added, updated, removed);
            }
            for child in lost_dirs.into_iter().flatten() {
                gone.push((root_id.clone(), child));
            }
        }
        for (root, path) in gone {
            // The root folder itself moved away (a removal is only seen by check_roots): no
            // parent folder will say so.
            let root_left = path.is_root()
                && state
                    .roots
                    .get(&root)
                    .is_some_and(|r| r.dirs.contains_key(&path));
            state.drop_dirs(inotify, &root, &path);
            if root_left && let Some(r) = state.roots.get_mut(&root) {
                r.emit_stale(&path, StaleReason::Error);
            }
        }
    }

    /// The kernel dropped events: read every cached directory again (a bounded number), send
    /// what differs, and say the tree is complete again. Directories beyond the bound are
    /// dropped and reported stale.
    fn resync(&self, state: &mut State) {
        let inotify = self.inotify.as_raw_fd();
        let mut budget = RESYNC_DIRS;
        let mut dropped = Vec::new();
        for (root_id, root) in state.roots.iter_mut() {
            let paths: Vec<RelPath> = root.dirs.keys().cloned().collect();
            for path in paths {
                if budget == 0 {
                    dropped.push((root_id.clone(), path));
                    continue;
                }
                budget -= 1;
                let Some(dir) = root.dirs.get_mut(&path) else {
                    continue;
                };
                root.enumerations += 1;
                let Ok((now, truncated)) = enumerate(dir.fd.as_raw_fd(), &path) else {
                    dropped.push((root_id.clone(), path));
                    continue;
                };
                if truncated {
                    dropped.push((root_id.clone(), path));
                    continue;
                }
                let (mut added, mut updated, mut removed) = (Vec::new(), Vec::new(), Vec::new());
                for (name, entry) in &now {
                    match dir.entries.get(name) {
                        None => added.push(entry.clone()),
                        Some(before) if before != entry => updated.push(entry.clone()),
                        _ => {}
                    }
                }
                for name in dir.entries.keys() {
                    if !now.contains_key(name) {
                        removed.push(name.clone());
                    }
                }
                dir.entries = now;
                if !added.is_empty() || !updated.is_empty() || !removed.is_empty() {
                    root.emit_changes(&path, added, updated, removed);
                }
            }
        }
        for (root_id, path) in &dropped {
            state.drop_dirs(inotify, root_id, path);
            if let Some(root) = state.roots.get_mut(root_id) {
                root.emit_stale(path, StaleReason::Overflow);
            }
        }
        for root in state.roots.values_mut() {
            let resynced = Resynced {
                revision: root.revision,
            };
            for sub in &mut root.subs {
                send_event(sub, tree::kind::RESYNCED, &resynced);
            }
        }
    }
}
