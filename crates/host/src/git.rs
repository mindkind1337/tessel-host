//! Git status without blocking. Each subscribed repository's porcelain v2 status is cached and
//! kept current from inotify events on its worktree directories and its git metadata (index,
//! HEAD, refs, packed-refs, info). A changed file costs one `git status` limited to the paths
//! that changed (literal pathspecs); an index, HEAD, refs or ignore change costs one full
//! status. Changes are grouped for 300 ms (2 s at most under continuous activity). One git
//! command runs at a time on the whole host, in its own process group, at nice 10 and idle I/O
//! priority, never taking git's optional locks (it never writes the index), within 2 s (soft:
//! reported as slow) and 5 s (hard: the group is killed), 8 MiB of output and 50,000 entries. A
//! failed or cut read keeps the last good status, says so, and backs off exponentially.
//! Nothing runs for a repository nobody subscribed to.
use crate::pty::Peer;
use crate::runtime::random_bytes;
use crate::tree::Trees;
use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::ffi::{CString, OsStr, OsString};
use std::io::Read;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::{Duration, Instant};
use tessel_host_protocol::git::{
    self, Branch, Changed, EntryKind, Features, GitEntry, GitState, GitStats, Opened, Subscribed,
    UntrackedList,
};
use tessel_host_protocol::{
    Ack, ErrorCode, HostFrame, RawMessage, RelPath, RepoId, StreamId, StreamPos, StreamSender,
    WireError, encode_frame,
};

const SOFT_BUDGET: Duration = Duration::from_secs(2);
const HARD_BUDGET: Duration = Duration::from_secs(5);

/// The hard budget, lowered only by the test knob TESSEL_HOST_TEST_GIT_HARD_MS (to exercise a
/// killed git and the back-off without a huge repository).
fn hard_budget() -> Duration {
    static BUDGET: std::sync::OnceLock<Duration> = std::sync::OnceLock::new();
    *BUDGET.get_or_init(|| {
        std::env::var("TESSEL_HOST_TEST_GIT_HARD_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .map(Duration::from_millis)
            .unwrap_or(HARD_BUDGET)
    })
}
const MAX_OUTPUT: usize = 8 * 1024 * 1024;
const MAX_ENTRIES: usize = 50_000;
const QUIET: Duration = Duration::from_millis(300);
const MAX_WAIT: Duration = Duration::from_secs(2);
const MAX_PATHSPECS: usize = 512;
const FIRST_BACKOFF: Duration = Duration::from_secs(1);
const MAX_BACKOFF: Duration = Duration::from_secs(60);
const MAX_REPOS: usize = 32;
const EVENT_OVERHEAD: usize = 256;
/// Events one subscriber may have waiting for its window; beyond, it is told to start over.
const MAX_QUEUED_BYTES: usize = 32 * 1024 * 1024;
const WORK_MASK: u32 = libc::IN_CREATE
    | libc::IN_DELETE
    | libc::IN_MOVED_FROM
    | libc::IN_MOVED_TO
    | libc::IN_MODIFY
    | libc::IN_ATTRIB
    | libc::IN_CLOSE_WRITE
    | libc::IN_DELETE_SELF
    | libc::IN_MOVE_SELF
    | libc::IN_ONLYDIR
    | libc::IN_DONT_FOLLOW
    | libc::IN_EXCL_UNLINK;

fn refuse(code: ErrorCode, detail: &'static str) -> WireError {
    WireError::detail(code, detail)
}

/// What one git command gave.
struct Run {
    output: Vec<u8>,
    ok: bool,
    timed_out: bool,
    truncated: bool,
    elapsed: Duration,
}

/// Runs git in `dir`, in its own process group, at low CPU and I/O priority, without optional
/// locks, within the hard budget and the output limit.
/// `work`: a status or listing of the repository, under the (possibly lowered) hard budget;
/// otherwise a quick question (version, rev-parse, config) under the normal one.
fn run_git(dir: &Path, args: &[&OsStr], work: bool) -> Run {
    let limit = if work { hard_budget() } else { HARD_BUDGET };
    let started = Instant::now();
    let mut command = Command::new("git");
    command
        .current_dir(dir)
        .arg("--no-optional-locks")
        .args(args)
        .env("GIT_OPTIONAL_LOCKS", "0")
        .env("GIT_TERMINAL_PROMPT", "0")
        .env("LC_ALL", "C")
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .env_remove("GIT_INDEX_FILE")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    // SAFETY: only async-signal-safe calls between fork and exec.
    unsafe {
        command.pre_exec(|| {
            libc::signal(libc::SIGHUP, libc::SIG_DFL);
            libc::setpgid(0, 0);
            libc::setpriority(libc::PRIO_PROCESS, 0, 10);
            // IOPRIO_WHO_PROCESS = 1, IOPRIO_CLASS_IDLE = 3 (in the top bits).
            libc::syscall(libc::SYS_ioprio_set, 1, 0, 3 << 13);
            Ok(())
        });
    }
    let Ok(mut child) = command.spawn() else {
        return Run {
            output: Vec::new(),
            ok: false,
            timed_out: false,
            truncated: false,
            elapsed: started.elapsed(),
        };
    };
    let pid = child.id() as libc::pid_t;
    let mut stdout = child.stdout.take().expect("piped");
    let reader = std::thread::spawn(move || {
        let mut output = Vec::new();
        let mut buffer = vec![0u8; 64 * 1024];
        let mut over = false;
        loop {
            match stdout.read(&mut buffer) {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    if output.len() + n > MAX_OUTPUT {
                        over = true;
                        // SAFETY: the group of the git we started.
                        unsafe { libc::kill(-pid, libc::SIGKILL) };
                        break;
                    }
                    output.extend_from_slice(&buffer[..n]);
                }
            }
        }
        (output, over)
    });
    let mut timed_out = false;
    let status = loop {
        // Over the budget is over the budget, even if git happened to finish since the last look.
        if started.elapsed() >= limit {
            timed_out = true;
            // SAFETY: the group of the git we started (it has not been reaped yet).
            unsafe { libc::kill(-pid, libc::SIGKILL) };
            break child.wait().ok();
        }
        match child.try_wait() {
            Ok(Some(status)) => break Some(status),
            Ok(None) => std::thread::sleep(Duration::from_millis(5)),
            Err(_) => break None,
        }
    };
    let (output, truncated) = reader.join().unwrap_or_default();
    Run {
        ok: !timed_out && !truncated && status.is_some_and(|s| s.success()),
        output,
        timed_out,
        truncated,
        elapsed: started.elapsed(),
    }
}

fn os(text: &str) -> &OsStr {
    OsStr::new(text)
}

fn rel(key: &[u8]) -> Option<RelPath> {
    let trimmed = key.strip_suffix(b"/").unwrap_or(key);
    if trimmed.is_empty() {
        return Some(RelPath::root());
    }
    RelPath::from_components(trimmed.split(|&b| b == b'/').map(|c| c.to_vec()).collect())
}

/// Parses `git status --porcelain=v2 -z`: entries by path (an untracked directory keeps its
/// trailing `/`), the branch headers, and whether there were more than MAX_ENTRIES.
pub fn parse_status(output: &[u8]) -> (BTreeMap<Vec<u8>, GitEntry>, Option<Branch>, bool) {
    let mut entries = BTreeMap::new();
    let mut branch: Option<Branch> = None;
    let mut cut = false;
    let mut records = output.split(|&b| b == 0).filter(|r| !r.is_empty());
    while let Some(record) = records.next() {
        if let Some(header) = record.strip_prefix(b"# ") {
            let text = String::from_utf8_lossy(header);
            let (key, value) = text.split_once(' ').unwrap_or((&text, ""));
            let b = branch.get_or_insert(Branch {
                head: String::new(),
                oid: String::new(),
                upstream: None,
                ahead: None,
                behind: None,
            });
            match key {
                "branch.oid" => b.oid = value.to_owned(),
                "branch.head" => b.head = value.to_owned(),
                "branch.upstream" => b.upstream = Some(value.to_owned()),
                "branch.ab" => {
                    let mut parts = value.split(' ');
                    b.ahead = parts
                        .next()
                        .and_then(|a| a.trim_start_matches('+').parse().ok());
                    b.behind = parts
                        .next()
                        .and_then(|a| a.trim_start_matches('-').parse().ok());
                }
                _ => {}
            }
            continue;
        }
        let (kind, fields) = match record.first() {
            Some(b'1') => (EntryKind::Changed, 8),
            Some(b'2') => (EntryKind::Renamed, 9),
            Some(b'u') => (EntryKind::Unmerged, 10),
            Some(b'?') => (EntryKind::Untracked, 1),
            _ => continue,
        };
        // The path starts after `fields` spaces; it may contain spaces itself.
        let mut at = 0;
        let mut seen = 0;
        while seen < fields && at < record.len() {
            if record[at] == b' ' {
                seen += 1;
            }
            at += 1;
        }
        if seen < fields {
            continue;
        }
        let head = String::from_utf8_lossy(&record[..at - 1]).into_owned();
        let path = record[at..].to_vec();
        let orig = if kind == EntryKind::Renamed {
            records.next().map(|o| o.to_vec())
        } else {
            None
        };
        if entries.len() >= MAX_ENTRIES {
            cut = true;
            continue;
        }
        let words: Vec<&str> = head.split(' ').collect();
        let (xy, sub) = match kind {
            EntryKind::Untracked => ("??".to_owned(), String::new()),
            _ => (
                words.get(1).unwrap_or(&"").to_string(),
                words.get(2).unwrap_or(&"").to_string(),
            ),
        };
        let Some(path_rel) = rel(&path) else { continue };
        let entry = GitEntry {
            untracked_dir: kind == EntryKind::Untracked && path.ends_with(b"/"),
            path: path_rel,
            orig_path: orig.as_deref().and_then(rel),
            kind,
            xy,
            sub,
            record: head,
        };
        entries.insert(path, entry);
    }
    (entries, branch, cut)
}

struct Sub {
    stream: StreamId,
    peer: Peer,
    sender: StreamSender,
    /// Events waiting for room in the window, in order.
    queue: VecDeque<(Changed, usize)>,
    queued_bytes: usize,
    lagging: bool,
}

impl Sub {
    fn new(stream: StreamId, peer: Peer) -> Self {
        Self {
            stream,
            sender: StreamSender::new(peer.window_bytes),
            peer,
            queue: VecDeque::new(),
            queued_bytes: 0,
            lagging: false,
        }
    }

    /// Queues an event; a subscriber too far behind drops its queue and will get the whole
    /// status again once it has caught up.
    fn push(&mut self, change: Changed) {
        if self.lagging || self.peer.out.is_closed() {
            return;
        }
        let size = serde_json::to_vec(&change)
            .map(|v| v.len())
            .unwrap_or(usize::MAX / 2)
            + EVENT_OVERHEAD;
        if self.queued_bytes + size > MAX_QUEUED_BYTES {
            self.queue.clear();
            self.queued_bytes = 0;
            self.lagging = true;
            return;
        }
        self.queued_bytes += size;
        self.queue.push_back((change, size));
        self.drain();
    }

    /// Sends what the window allows.
    fn drain(&mut self) {
        while let Some((change, size)) = self.queue.front() {
            let Ok(seq) = self.sender.reserve(*size) else {
                break;
            };
            let message = RawMessage::of(git::kind::CHANGED, change);
            if let Ok(bytes) = encode_frame(
                &HostFrame::event(
                    StreamPos {
                        id: self.stream.clone(),
                        seq,
                    },
                    message,
                ),
                self.peer.frame_limit,
            ) {
                self.peer.out.event(bytes);
            }
            self.queued_bytes -= size;
            self.queue.pop_front();
        }
    }
}

#[derive(Clone)]
enum Watch {
    Work(Vec<u8>),
    Meta,
}

struct Repo {
    toplevel: PathBuf,
    gitdir: PathBuf,
    commondir: PathBuf,
    features: Features,
    entries: BTreeMap<Vec<u8>, GitEntry>,
    branch: Option<Branch>,
    revision: u64,
    subs: Vec<Sub>,
    watches: Vec<i32>,
    built: bool,
    rebuild: bool,
    coverage_complete: bool,
    submodules: Vec<Vec<u8>>,
    /// Directories known to be untracked: from the last watch build, and created since.
    untracked_dirs: BTreeSet<Vec<u8>>,
    pending_paths: BTreeSet<Vec<u8>>,
    pending_full: bool,
    first: Option<Instant>,
    last: Option<Instant>,
    running: bool,
    has_status: bool,
    stale: bool,
    stale_told: bool,
    truncated: bool,
    slow: bool,
    duration_ms: u64,
    backoff: Duration,
    backoff_until: Option<Instant>,
    commands: u64,
    output_bytes: u64,
    last_full_ms: u64,
    last_partial_ms: u64,
}

struct State {
    repos: HashMap<RepoId, Repo>,
    wds: HashMap<i32, (RepoId, Watch)>,
}

pub struct Git {
    state: Mutex<State>,
    wake: Condvar,
    /// One git command at a time on the whole host.
    job: Mutex<()>,
    inotify: OwnedFd,
    trees: Arc<Trees>,
    max_watches: usize,
    version: String,
}

enum JobKind {
    Full { rebuild: bool },
    Partial(Vec<Vec<u8>>),
}

struct Job {
    repo: RepoId,
    toplevel: PathBuf,
    gitdir: PathBuf,
    commondir: PathBuf,
    kind: JobKind,
}

impl Repo {
    fn state(&self) -> GitState {
        GitState {
            branch: self.branch.clone(),
            complete: self.has_status && !self.truncated && self.coverage_complete && !self.stale,
            stale: self.stale || !self.has_status,
            truncated: self.truncated,
            slow: self.slow,
            coverage_complete: self.coverage_complete,
            duration_ms: self.duration_ms,
        }
    }

    /// Sends a change to every subscriber (or only to `only`), in parts that fit their frames.
    fn emit(
        &mut self,
        upsert: Vec<GitEntry>,
        removed: Vec<RelPath>,
        reset: bool,
        only: Option<&StreamId>,
    ) {
        let budget = self
            .subs
            .iter()
            .map(|s| s.peer.frame_limit)
            .min()
            .unwrap_or(1 << 20)
            / 2;
        let mut parts: Vec<(Vec<GitEntry>, Vec<RelPath>)> = vec![(Vec::new(), Vec::new())];
        let mut size = 0usize;
        for entry in upsert {
            let cost = serde_json::to_vec(&entry).map(|v| v.len()).unwrap_or(1024) + 8;
            if size + cost > budget
                && !(parts.last().unwrap().0.is_empty() && parts.last().unwrap().1.is_empty())
            {
                parts.push((Vec::new(), Vec::new()));
                size = 0;
            }
            size += cost;
            parts.last_mut().unwrap().0.push(entry);
        }
        for path in removed {
            let cost = serde_json::to_vec(&path).map(|v| v.len()).unwrap_or(1024) + 8;
            if size + cost > budget
                && !(parts.last().unwrap().0.is_empty() && parts.last().unwrap().1.is_empty())
            {
                parts.push((Vec::new(), Vec::new()));
                size = 0;
            }
            size += cost;
            parts.last_mut().unwrap().1.push(path);
        }
        let state = self.state();
        for (index, (upsert, removed)) in parts.into_iter().enumerate() {
            self.revision += 1;
            let change = Changed {
                revision: self.revision,
                reset: reset && index == 0,
                upsert,
                removed,
                state: state.clone(),
            };
            for sub in self
                .subs
                .iter_mut()
                .filter(|s| only.is_none_or(|id| *id == s.stream))
            {
                sub.push(change.clone());
            }
        }
    }

    /// The path git should be asked about for a change at `path`: the submodule holding it, or
    /// the untracked directory shown as one entry around it, or the path itself.
    fn spec_for(&self, path: &[u8]) -> Vec<u8> {
        for module in &self.submodules {
            if path == module.as_slice()
                || (path.starts_with(module) && path.get(module.len()) == Some(&b'/'))
            {
                return module.clone();
            }
        }
        // The outermost untracked directory around the path (or the path itself, if it is one),
        // asked about as a directory: git then answers as a whole status would, one entry for
        // a wholly untracked directory, never a file inside it.
        let ends = path
            .iter()
            .enumerate()
            .filter(|(_, b)| **b == b'/')
            .map(|(i, _)| i)
            .chain(std::iter::once(path.len()));
        for end in ends {
            let prefix = &path[..end];
            let mut dir = prefix.to_vec();
            dir.push(b'/');
            if self.untracked_dirs.contains(prefix)
                || self.entries.get(&dir).is_some_and(|e| e.untracked_dir)
            {
                return dir;
            }
        }
        path.to_vec()
    }
}

/// Whether a status entry keyed `key` is about pathspec `spec`.
fn covered(key: &[u8], entry: &GitEntry, spec: &[u8]) -> bool {
    let under = |path: &[u8]| {
        if spec.ends_with(b"/") {
            path.starts_with(spec)
        } else {
            path == spec || (path.starts_with(spec) && path.get(spec.len()) == Some(&b'/'))
        }
    };
    under(key)
        || entry
            .orig_path
            .as_ref()
            .is_some_and(|o| under(&o.to_bytes()))
}

fn add_watch(inotify: i32, path: &Path, mask: u32) -> Option<i32> {
    let cpath = CString::new(path.as_os_str().as_bytes()).ok()?;
    // SAFETY: a NUL-terminated path.
    let wd = unsafe { libc::inotify_add_watch(inotify, cpath.as_ptr(), mask) };
    (wd >= 0).then_some(wd)
}

impl Git {
    pub fn new(trees: Arc<Trees>) -> std::io::Result<Arc<Self>> {
        // SAFETY: the result is wrapped at once.
        let fd = unsafe { libc::inotify_init1(libc::IN_NONBLOCK | libc::IN_CLOEXEC) };
        if fd < 0 {
            return Err(std::io::Error::last_os_error());
        }
        let version =
            String::from_utf8_lossy(&run_git(Path::new("/"), &[os("--version")], false).output)
                .trim()
                .to_owned();
        let git = Arc::new(Self {
            state: Mutex::new(State {
                repos: HashMap::new(),
                wds: HashMap::new(),
            }),
            wake: Condvar::new(),
            job: Mutex::new(()),
            inotify: unsafe { OwnedFd::from_raw_fd(fd) },
            trees,
            max_watches: crate::tree::max_watches(),
            version,
        });
        let reader = git.clone();
        std::thread::spawn(move || reader.read_loop());
        let worker = git.clone();
        std::thread::spawn(move || worker.work_loop());
        Ok(git)
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn one_job(&self) -> MutexGuard<'_, ()> {
        self.job
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    pub fn open(&self, request: git::Open) -> Result<Opened, WireError> {
        // Resolved name by name without following links, like every path below a root.
        let _checked = self
            .trees
            .open_dir(&request.root, request.path.components())?;
        let mut dir = self
            .trees
            .root_path(&request.root)
            .ok_or_else(|| refuse(ErrorCode::NotFound, "unknown root"))?;
        for component in request.path.components() {
            dir.push(OsStr::from_bytes(component));
        }
        let (toplevel, gitdir, commondir, features) = {
            let _job = self.one_job();
            let found = run_git(
                &dir,
                &[
                    os("rev-parse"),
                    os("--path-format=absolute"),
                    os("--show-toplevel"),
                    os("--absolute-git-dir"),
                    os("--git-common-dir"),
                ],
                false,
            );
            let lines: Vec<&[u8]> = found
                .output
                .split(|&b| b == b'\n')
                .filter(|l| !l.is_empty())
                .collect();
            if !found.ok || lines.len() != 3 {
                return Err(refuse(ErrorCode::NotFound, "not inside a git worktree"));
            }
            let path = |line: &[u8]| PathBuf::from(OsString::from_vec(line.to_vec()));
            let toplevel = path(lines[0]);
            let config =
                |key: &str| run_git(&toplevel, &[os("config"), os("--get"), os(key)], false).output;
            let untracked = String::from_utf8_lossy(&config("core.untrackedCache"))
                .trim()
                .to_ascii_lowercase();
            let monitor = String::from_utf8_lossy(&config("core.fsmonitor"))
                .trim()
                .to_ascii_lowercase();
            let features = Features {
                untracked_cache: matches!(untracked.as_str(), "true" | "yes" | "on" | "1"),
                fsmonitor: !monitor.is_empty()
                    && !matches!(monitor.as_str(), "false" | "no" | "off" | "0"),
                git_version: self.version.clone(),
            };
            (toplevel, path(lines[1]), path(lines[2]), features)
        };
        let mut state = self.lock();
        if let Some((id, repo)) = state.repos.iter().find(|(_, r)| r.toplevel == toplevel) {
            return Ok(Opened {
                repo: id.clone(),
                toplevel: repo.toplevel.to_string_lossy().into_owned(),
                features: repo.features.clone(),
            });
        }
        if state.repos.len() >= MAX_REPOS {
            return Err(refuse(ErrorCode::Busy, "too many repositories"));
        }
        let id = RepoId::from_bytes(
            random_bytes().map_err(|_| refuse(ErrorCode::Internal, "no randomness"))?,
        );
        state.repos.insert(
            id.clone(),
            Repo {
                toplevel: toplevel.clone(),
                gitdir,
                commondir,
                features: features.clone(),
                entries: BTreeMap::new(),
                branch: None,
                revision: 0,
                subs: Vec::new(),
                watches: Vec::new(),
                built: false,
                rebuild: true,
                coverage_complete: false,
                submodules: Vec::new(),
                untracked_dirs: BTreeSet::new(),
                pending_paths: BTreeSet::new(),
                pending_full: false,
                first: None,
                last: None,
                running: false,
                has_status: false,
                stale: false,
                stale_told: false,
                truncated: false,
                slow: false,
                duration_ms: 0,
                backoff: FIRST_BACKOFF,
                backoff_until: None,
                commands: 0,
                output_bytes: 0,
                last_full_ms: 0,
                last_partial_ms: 0,
            },
        );
        Ok(Opened {
            repo: id,
            toplevel: toplevel.to_string_lossy().into_owned(),
            features,
        })
    }

    pub fn subscribe(&self, request: git::Subscribe, peer: &Peer) -> Result<Subscribed, WireError> {
        let mut state = self.lock();
        let repo = state
            .repos
            .get_mut(&request.repo)
            .ok_or_else(|| refuse(ErrorCode::NotFound, "unknown repository"))?;
        let stream = StreamId::from_bytes(
            random_bytes().map_err(|_| refuse(ErrorCode::Internal, "no randomness"))?,
        );
        repo.subs.push(Sub::new(stream.clone(), peer.clone()));
        if repo.has_status {
            // What is known already, at once; the next change follows as usual.
            let upsert: Vec<GitEntry> = repo.entries.values().cloned().collect();
            repo.emit(upsert, Vec::new(), true, Some(&stream));
        } else {
            let now = Instant::now();
            repo.pending_full = true;
            repo.first.get_or_insert(now - MAX_WAIT);
            repo.last = Some(now - QUIET);
        }
        let revision = repo.revision;
        drop(state);
        self.wake.notify_all();
        Ok(Subscribed { stream, revision })
    }

    pub fn unsubscribe(&self, peer: u64, request: &git::Unsubscribe) -> Result<(), WireError> {
        let mut state = self.lock();
        let mut found = None;
        for (id, repo) in state.repos.iter_mut() {
            let before = repo.subs.len();
            repo.subs
                .retain(|s| !(s.stream == request.stream && s.peer.id == peer));
            if repo.subs.len() != before {
                found = Some(id.clone());
                break;
            }
        }
        let id = found.ok_or_else(|| refuse(ErrorCode::NotFound, "unknown stream"))?;
        self.stop_if_unwatched(&mut state, &id);
        Ok(())
    }

    /// Nobody subscribed any more: no watch, no pending work, no git command.
    fn stop_if_unwatched(&self, state: &mut State, id: &RepoId) {
        let Some(repo) = state.repos.get_mut(id) else {
            return;
        };
        if !repo.subs.is_empty() {
            return;
        }
        for wd in std::mem::take(&mut repo.watches) {
            // SAFETY: our own watch.
            unsafe { libc::inotify_rm_watch(self.inotify.as_raw_fd(), wd) };
            state.wds.remove(&wd);
        }
        let repo = state.repos.get_mut(id).unwrap();
        repo.built = false;
        repo.rebuild = true;
        repo.has_status = false;
        repo.entries.clear();
        repo.pending_paths.clear();
        repo.pending_full = false;
        repo.first = None;
        repo.last = None;
    }

    pub fn disconnect(&self, peer: u64) {
        let mut state = self.lock();
        let ids: Vec<RepoId> = state.repos.keys().cloned().collect();
        for id in ids {
            if let Some(repo) = state.repos.get_mut(&id) {
                repo.subs.retain(|s| s.peer.id != peer);
            }
            self.stop_if_unwatched(&mut state, &id);
        }
    }

    /// Ok(false): not a git stream.
    pub fn ack(&self, peer: u64, ack: &Ack) -> Result<bool, WireError> {
        let mut state = self.lock();
        for repo in state.repos.values_mut() {
            let Some(index) = repo
                .subs
                .iter()
                .position(|s| s.stream == ack.stream && s.peer.id == peer)
            else {
                continue;
            };
            let sub = &mut repo.subs[index];
            sub.sender.ack(ack.seq).map_err(|_| {
                refuse(ErrorCode::BadFrame, "acknowledgment of an event never sent")
            })?;
            sub.drain();
            if sub.lagging && sub.queue.is_empty() && sub.sender.unacked_bytes() == 0 {
                // It lost events: it gets the whole status again, from the cache.
                sub.lagging = false;
                let stream = sub.stream.clone();
                let upsert: Vec<GitEntry> = repo.entries.values().cloned().collect();
                repo.emit(upsert, Vec::new(), true, Some(&stream));
            }
            return Ok(true);
        }
        Ok(false)
    }

    pub fn refresh(&self, request: git::Refresh) -> Result<(), WireError> {
        let mut state = self.lock();
        let repo = state
            .repos
            .get_mut(&request.repo)
            .ok_or_else(|| refuse(ErrorCode::NotFound, "unknown repository"))?;
        let now = Instant::now();
        repo.pending_full = true;
        repo.rebuild = true;
        repo.first.get_or_insert(now);
        repo.last = Some(now);
        drop(state);
        self.wake.notify_all();
        Ok(())
    }

    pub fn stats(&self, request: git::Stats) -> Result<GitStats, WireError> {
        let state = self.lock();
        let repo = state
            .repos
            .get(&request.repo)
            .ok_or_else(|| refuse(ErrorCode::NotFound, "unknown repository"))?;
        Ok(GitStats {
            commands: repo.commands,
            output_bytes: repo.output_bytes,
            last_full_ms: repo.last_full_ms,
            last_partial_ms: repo.last_partial_ms,
            watches: repo.watches.len() as u32,
            coverage_complete: repo.coverage_complete,
        })
    }

    pub fn untracked(&self, request: git::Untracked) -> Result<UntrackedList, WireError> {
        let toplevel = {
            let state = self.lock();
            state
                .repos
                .get(&request.repo)
                .map(|r| r.toplevel.clone())
                .ok_or_else(|| refuse(ErrorCode::NotFound, "unknown repository"))?
        };
        let mut spec = b":(literal)".to_vec();
        spec.extend_from_slice(&request.path.to_bytes());
        spec.push(b'/');
        let spec = OsString::from_vec(spec);
        let run = {
            let _job = self.one_job();
            run_git(
                &toplevel,
                &[
                    os("status"),
                    os("--porcelain=v2"),
                    os("-z"),
                    os("--untracked-files=all"),
                    os("--"),
                    &spec,
                ],
                true,
            )
        };
        {
            let mut state = self.lock();
            if let Some(repo) = state.repos.get_mut(&request.repo) {
                repo.commands += 1;
                repo.output_bytes += run.output.len() as u64;
            }
        }
        if run.timed_out {
            return Err(WireError::new(ErrorCode::Timeout));
        }
        let (entries, _, cut) = parse_status(&run.output);
        let list = entries
            .values()
            .filter(|e| e.kind == EntryKind::Untracked)
            .map(|e| e.path.clone())
            .collect();
        Ok(UntrackedList {
            path: request.path,
            entries: list,
            truncated: cut || run.truncated,
        })
    }

    fn read_loop(self: Arc<Self>) {
        let fd = self.inotify.as_raw_fd();
        let mut buffer = vec![0u8; 256 * 1024];
        loop {
            let mut poll = libc::pollfd {
                fd,
                events: libc::POLLIN,
                revents: 0,
            };
            // SAFETY: one valid pollfd.
            if unsafe { libc::poll(&mut poll, 1, -1) } < 0 {
                continue;
            }
            let mut events = Vec::new();
            loop {
                // SAFETY: reading into our own buffer.
                let n = unsafe { libc::read(fd, buffer.as_mut_ptr().cast(), buffer.len()) };
                if n <= 0 {
                    break;
                }
                let (mut at, n) = (0usize, n as usize);
                let header = std::mem::size_of::<libc::inotify_event>();
                while at + header <= n {
                    // SAFETY: the kernel writes whole events.
                    let event: libc::inotify_event =
                        unsafe { std::ptr::read_unaligned(buffer.as_ptr().add(at).cast()) };
                    let len = event.len as usize;
                    let raw = &buffer[at + header..(at + header + len).min(n)];
                    events.push((
                        event.wd,
                        event.mask,
                        raw.split(|&b| b == 0).next().unwrap_or(&[]).to_vec(),
                    ));
                    at += header + len;
                }
            }
            if !events.is_empty() {
                self.note(events);
                self.wake.notify_all();
            }
        }
    }

    /// Records what changed; the worker decides when to ask git.
    fn note(&self, events: Vec<(i32, u32, Vec<u8>)>) {
        let mut guard = self.lock();
        let state = &mut *guard;
        let inotify = self.inotify.as_raw_fd();
        let now = Instant::now();
        for (wd, mask, name) in events {
            if mask & libc::IN_Q_OVERFLOW != 0 {
                for repo in state.repos.values_mut().filter(|r| !r.subs.is_empty()) {
                    repo.pending_full = true;
                    repo.rebuild = true;
                    repo.first.get_or_insert(now);
                    repo.last = Some(now);
                }
                continue;
            }
            let Some((id, watch)) = state.wds.get(&wd).cloned() else {
                continue;
            };
            if mask & libc::IN_IGNORED != 0 {
                state.wds.remove(&wd);
                if let Some(repo) = state.repos.get_mut(&id) {
                    repo.watches.retain(|w| *w != wd);
                }
                continue;
            }
            let Some(repo) = state.repos.get_mut(&id) else {
                continue;
            };
            match watch {
                Watch::Meta => {
                    if name.ends_with(b".lock") {
                        continue;
                    }
                    // The index, HEAD or refs moved: read it all, and watch again (directories
                    // may have become tracked).
                    repo.pending_full = true;
                    repo.rebuild = true;
                }
                Watch::Work(dir) => {
                    if mask & (libc::IN_DELETE_SELF | libc::IN_MOVE_SELF) != 0 || name.is_empty() {
                        continue;
                    }
                    let mut path = dir.clone();
                    if !path.is_empty() {
                        path.push(b'/');
                    }
                    path.extend_from_slice(&name);
                    if name == b".gitignore" {
                        repo.pending_full = true;
                        repo.rebuild = true;
                    }
                    // A new directory: watched at once (as untracked: its own entries only).
                    if mask & libc::IN_ISDIR != 0
                        && mask & (libc::IN_CREATE | libc::IN_MOVED_TO) != 0
                        && name != b".git"
                    {
                        if repo.watches.len() < self.max_watches {
                            let full = repo.toplevel.join(OsStr::from_bytes(&path));
                            if let Some(new) = add_watch(inotify, &full, WORK_MASK) {
                                if !repo.watches.contains(&new) {
                                    repo.watches.push(new);
                                }
                                state
                                    .wds
                                    .insert(new, (id.clone(), Watch::Work(path.clone())));
                            }
                        } else {
                            repo.coverage_complete = false;
                        }
                    }
                    let repo = state.repos.get_mut(&id).unwrap();
                    if mask & libc::IN_ISDIR != 0
                        && mask & (libc::IN_CREATE | libc::IN_MOVED_TO) != 0
                    {
                        repo.untracked_dirs.insert(path.clone());
                    }
                    repo.pending_paths.insert(path);
                }
            }
            let repo = state.repos.get_mut(&id).unwrap();
            repo.first.get_or_insert(now);
            repo.last = Some(now);
            if repo.pending_full && !repo.stale_told && repo.has_status {
                // A whole read is needed (index, HEAD, refs, ignore rules): stale until it is done.
                repo.stale = true;
                repo.stale_told = true;
                repo.emit(Vec::new(), Vec::new(), false, None);
            }
        }
    }

    fn take_job(&self, state: &mut State) -> Option<Job> {
        let now = Instant::now();
        for (id, repo) in state.repos.iter_mut() {
            if repo.subs.is_empty()
                || repo.running
                || (!repo.pending_full && repo.pending_paths.is_empty())
            {
                continue;
            }
            if repo.backoff_until.is_some_and(|until| now < until) {
                continue;
            }
            let quiet = repo
                .last
                .is_none_or(|last| now.duration_since(last) >= QUIET);
            let waited = repo
                .first
                .is_some_and(|first| now.duration_since(first) >= MAX_WAIT);
            if !quiet && !waited {
                continue;
            }
            let kind = if repo.pending_full || !repo.has_status {
                repo.pending_paths.clear();
                JobKind::Full {
                    rebuild: repo.rebuild || !repo.built,
                }
            } else {
                let mut specs: BTreeSet<Vec<u8>> = BTreeSet::new();
                for path in std::mem::take(&mut repo.pending_paths) {
                    specs.insert(repo.spec_for(&path));
                }
                if specs.len() > MAX_PATHSPECS {
                    JobKind::Full { rebuild: false }
                } else {
                    JobKind::Partial(specs.into_iter().collect())
                }
            };
            repo.pending_full = false;
            repo.rebuild = false;
            repo.first = None;
            repo.last = None;
            repo.running = true;
            return Some(Job {
                repo: id.clone(),
                toplevel: repo.toplevel.clone(),
                gitdir: repo.gitdir.clone(),
                commondir: repo.commondir.clone(),
                kind,
            });
        }
        None
    }

    fn work_loop(self: Arc<Self>) {
        loop {
            let job = {
                let mut state = self.lock();
                loop {
                    if let Some(job) = self.take_job(&mut state) {
                        break job;
                    }
                    state = self
                        .wake
                        .wait_timeout(state, Duration::from_millis(50))
                        .map(|(s, _)| s)
                        .unwrap_or_else(|p| p.into_inner().0);
                }
            };
            let _one = self.one_job();
            match job.kind {
                JobKind::Full { rebuild } => self.full(&job, rebuild),
                JobKind::Partial(ref specs) => self.partial(&job, specs),
            }
        }
    }

    fn count(&self, id: &RepoId, run: &Run) {
        if let Some(repo) = self.lock().repos.get_mut(id) {
            repo.commands += 1;
            repo.output_bytes += run.output.len() as u64;
        }
    }

    /// Lists, without descending into them, the directories git reports as one entry.
    fn directories(&self, job: &Job, ignored: bool) -> BTreeSet<Vec<u8>> {
        let mut args = vec![
            os("ls-files"),
            os("-z"),
            os("--others"),
            os("--directory"),
            os("--exclude-standard"),
        ];
        if ignored {
            args.push(os("--ignored"));
        }
        let run = run_git(&job.toplevel, &args, true);
        self.count(&job.repo, &run);
        run.output
            .split(|&b| b == 0)
            .filter(|p| p.ends_with(b"/"))
            .map(|p| p[..p.len() - 1].to_vec())
            .collect()
    }

    /// Watches every worktree directory (not inside .git, ignored or untracked directories, which
    /// are watched for their own entries only) and the git metadata, within the watch budget.
    fn build_watches(&self, job: &Job) {
        let untracked = self.directories(job, false);
        let ignored = self.directories(job, true);
        let submodules: Vec<Vec<u8>> = if job.toplevel.join(".gitmodules").exists() {
            let run = run_git(&job.toplevel, &[os("ls-files"), os("-z"), os("-s")], true);
            self.count(&job.repo, &run);
            run.output
                .split(|&b| b == 0)
                .filter(|r| r.starts_with(b"160000 "))
                .filter_map(|r| {
                    r.iter()
                        .position(|&b| b == b'\t')
                        .map(|tab| r[tab + 1..].to_vec())
                })
                .collect()
        } else {
            Vec::new()
        };
        let mut state = self.lock();
        let inotify = self.inotify.as_raw_fd();
        let Some(repo) = state.repos.get_mut(&job.repo) else {
            return;
        };
        for wd in std::mem::take(&mut repo.watches) {
            // SAFETY: our own watch.
            unsafe { libc::inotify_rm_watch(inotify, wd) };
        }
        repo.submodules = submodules;
        repo.untracked_dirs = untracked.clone();
        let budget = self.max_watches;
        let mut added: Vec<(i32, Watch)> = Vec::new();
        let mut complete = true;
        let meta_mask = libc::IN_CREATE
            | libc::IN_DELETE
            | libc::IN_MOVED_TO
            | libc::IN_MOVED_FROM
            | libc::IN_CLOSE_WRITE
            | libc::IN_ONLYDIR;
        let mut meta_dirs = vec![
            job.gitdir.clone(),
            job.commondir.clone(),
            job.commondir.join("info"),
        ];
        let mut refs = vec![job.commondir.join("refs")];
        while let Some(dir) = refs.pop() {
            meta_dirs.push(dir.clone());
            if let Ok(children) = std::fs::read_dir(&dir) {
                refs.extend(
                    children
                        .flatten()
                        .filter(|c| c.file_type().is_ok_and(|t| t.is_dir()))
                        .map(|c| c.path()),
                );
            }
        }
        for dir in meta_dirs {
            if let Some(wd) = add_watch(inotify, &dir, meta_mask) {
                added.push((wd, Watch::Meta));
            }
        }
        let mut queue: Vec<(Vec<u8>, bool)> = vec![(Vec::new(), true)];
        while let Some((dir, descend)) = queue.pop() {
            if added.len() >= budget {
                complete = false;
                break;
            }
            let full = job.toplevel.join(OsStr::from_bytes(&dir));
            let Some(wd) = add_watch(inotify, &full, WORK_MASK) else {
                continue;
            };
            added.push((wd, Watch::Work(dir.clone())));
            if !descend {
                continue;
            }
            let Ok(children) = std::fs::read_dir(&full) else {
                continue;
            };
            for child in children.flatten() {
                if !child.file_type().is_ok_and(|t| t.is_dir()) {
                    continue;
                }
                let name = child.file_name();
                if name.as_bytes() == b".git" {
                    continue;
                }
                let mut path = dir.clone();
                if !path.is_empty() {
                    path.push(b'/');
                }
                path.extend_from_slice(name.as_bytes());
                if ignored.contains(&path) {
                    continue;
                }
                queue.push((path.clone(), !untracked.contains(&path)));
            }
        }
        let repo = state.repos.get_mut(&job.repo).unwrap();
        repo.watches = added.iter().map(|(wd, _)| *wd).collect();
        repo.coverage_complete = complete;
        repo.built = true;
        for (wd, watch) in added {
            state.wds.insert(wd, (job.repo.clone(), watch));
        }
    }

    fn finish(&self, id: &RepoId, run: &Run, partial: bool) -> bool {
        let mut state = self.lock();
        let Some(repo) = state.repos.get_mut(id) else {
            return false;
        };
        repo.running = false;
        repo.commands += 1;
        repo.output_bytes += run.output.len() as u64;
        repo.duration_ms = run.elapsed.as_millis() as u64;
        repo.slow = run.elapsed > SOFT_BUDGET;
        if partial {
            repo.last_partial_ms = repo.duration_ms;
        } else {
            repo.last_full_ms = repo.duration_ms;
        }
        if run.ok {
            repo.backoff = FIRST_BACKOFF;
            repo.backoff_until = None;
            return true;
        }
        // Timed out, cut or failed: the last good status stays, marked stale; this read is
        // tried again later, a little later each time.
        repo.stale = true;
        repo.truncated = run.truncated;
        repo.backoff_until = Some(Instant::now() + repo.backoff);
        repo.backoff = (repo.backoff * 2).min(MAX_BACKOFF);
        repo.pending_full = true;
        repo.first.get_or_insert(Instant::now());
        repo.last = Some(Instant::now());
        repo.emit(Vec::new(), Vec::new(), false, None);
        false
    }

    fn full(&self, job: &Job, rebuild: bool) {
        // Watches first, then the read: nothing that changes in between is missed.
        if rebuild {
            self.build_watches(job);
        }
        let run = run_git(
            &job.toplevel,
            &[
                os("status"),
                os("--porcelain=v2"),
                os("-z"),
                os("--branch"),
                os("--untracked-files=normal"),
            ],
            true,
        );
        if !self.finish(&job.repo, &run, false) {
            return;
        }
        let (entries, branch, cut) = parse_status(&run.output);
        let mut state = self.lock();
        let Some(repo) = state.repos.get_mut(&job.repo) else {
            return;
        };
        let first = !repo.has_status;
        let mut upsert = Vec::new();
        let mut removed = Vec::new();
        for (key, entry) in &entries {
            if repo.entries.get(key) != Some(entry) {
                upsert.push(entry.clone());
            }
        }
        for (key, entry) in &repo.entries {
            if !entries.contains_key(key) {
                removed.push(entry.path.clone());
            }
        }
        repo.entries = entries;
        repo.branch = branch;
        repo.truncated = cut;
        repo.has_status = true;
        // Changes that came during this read keep it stale until they are read too.
        repo.stale = repo.pending_full;
        repo.stale_told = repo.stale;
        if first {
            let all: Vec<GitEntry> = repo.entries.values().cloned().collect();
            repo.emit(all, Vec::new(), true, None);
        } else {
            repo.emit(upsert, removed, false, None);
        }
    }

    fn partial(&self, job: &Job, specs: &[Vec<u8>]) {
        let mut args: Vec<OsString> = [
            "status",
            "--porcelain=v2",
            "-z",
            "--untracked-files=normal",
            "--",
        ]
        .iter()
        .map(OsString::from)
        .collect();
        for spec in specs {
            let mut literal = b":(literal)".to_vec();
            literal.extend_from_slice(spec);
            args.push(OsString::from_vec(literal));
        }
        let refs: Vec<&OsStr> = args.iter().map(|a| a.as_os_str()).collect();
        let run = run_git(&job.toplevel, &refs, true);
        if !self.finish(&job.repo, &run, true) {
            return;
        }
        let (found, _, cut) = parse_status(&run.output);
        let mut state = self.lock();
        let Some(repo) = state.repos.get_mut(&job.repo) else {
            return;
        };
        let mut removed = Vec::new();
        let gone: Vec<Vec<u8>> = repo
            .entries
            .iter()
            .filter(|(k, e)| specs.iter().any(|s| covered(k, e, s)) && !found.contains_key(*k))
            .map(|(k, _)| k.clone())
            .collect();
        for key in gone {
            if let Some(entry) = repo.entries.remove(&key) {
                removed.push(entry.path);
            }
        }
        let mut upsert = Vec::new();
        for (key, entry) in found {
            if repo.entries.get(&key) != Some(&entry) {
                upsert.push(entry.clone());
                repo.entries.insert(key, entry);
            }
        }
        repo.truncated |= cut;
        if !upsert.is_empty() || !removed.is_empty() {
            repo.emit(upsert, removed, false, None);
        }
    }
}
