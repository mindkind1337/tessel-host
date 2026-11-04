//! Gate 4: file trees over the real bridge and daemon, against the real file system.
#![cfg(target_os = "linux")]
mod common;

use common::*;
use std::collections::{BTreeMap, HashMap};
use std::os::unix::fs::{PermissionsExt, symlink};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use tessel_host_protocol::tree::*;
use tessel_host_protocol::*;

/// What the client shows, kept from listings and events.
struct Mirror {
    stream: Option<StreamId>,
    dirs: HashMap<RelPath, (u64, BTreeMap<Vec<u8>, Entry>)>,
    pending: Vec<Changed>,
    stale: Vec<Stale>,
    resynced: Vec<u64>,
    events: usize,
}

impl Mirror {
    fn new() -> Self {
        Self {
            stream: None,
            dirs: HashMap::new(),
            pending: Vec::new(),
            stale: Vec::new(),
            resynced: Vec::new(),
            events: 0,
        }
    }

    fn listing(&mut self, listing: &Listing) {
        let entries = listing
            .entries
            .iter()
            .map(|e| (e.name.as_bytes().to_vec(), e.clone()))
            .collect();
        self.dirs
            .insert(listing.path.clone(), (listing.revision, entries));
        let pending = std::mem::take(&mut self.pending);
        for change in pending {
            self.change(change);
        }
    }

    fn change(&mut self, change: Changed) {
        let Some((revision, entries)) = self.dirs.get_mut(&change.path) else {
            self.pending.push(change);
            return;
        };
        if change.revision <= *revision {
            return;
        }
        *revision = change.revision;
        for entry in change.added.into_iter().chain(change.updated) {
            entries.insert(entry.name.as_bytes().to_vec(), entry);
        }
        for name in change.removed {
            entries.remove(name.as_bytes());
        }
    }

    fn apply(&mut self, client: &mut Client, frame: HostFrame) {
        let pos = frame.stream.expect("an event");
        let event = frame.event.unwrap();
        self.events += 1;
        match event.kind.as_str() {
            kind::CHANGED => self.change(event.body_as().unwrap()),
            kind::STALE => self.stale.push(event.body_as().unwrap()),
            kind::RESYNCED => self
                .resynced
                .push(event.body_as::<Resynced>().unwrap().revision),
            other => panic!("unexpected event {other}"),
        }
        client.ack(&pos.id, pos.seq);
    }

    fn drain(&mut self, client: &mut Client, wait: Duration) {
        while let Some(frame) = client.event(wait) {
            self.apply(client, frame);
        }
    }

    fn names(&self, path: &RelPath) -> BTreeMap<Vec<u8>, EntryKind> {
        self.dirs[path]
            .1
            .iter()
            .map(|(n, e)| (n.clone(), e.kind))
            .collect()
    }
}

fn on_disk(dir: &Path) -> BTreeMap<Vec<u8>, EntryKind> {
    use std::os::unix::ffi::OsStrExt;
    std::fs::read_dir(dir)
        .unwrap()
        .flatten()
        .map(|e| {
            let meta = std::fs::symlink_metadata(e.path()).unwrap();
            let kind = if meta.file_type().is_symlink() {
                EntryKind::Symlink
            } else if meta.is_dir() {
                EntryKind::Dir
            } else if meta.is_file() {
                EntryKind::File
            } else {
                EntryKind::Other
            };
            (e.file_name().as_bytes().to_vec(), kind)
        })
        .collect()
}

fn path(parts: &[&str]) -> RelPath {
    RelPath::from_components(parts.iter().map(|p| p.as_bytes().to_vec()).collect()).unwrap()
}

struct Tree {
    client: Client,
    root: RootId,
    opened: RootOpened,
    mirror: Mirror,
}

impl Tree {
    fn open(home: &mut Home, dir: &Path) -> Self {
        let mut client = home.client(Budgets::SLOW_HOST);
        let opened: RootOpened = client.ask(
            kind::ROOT_OPEN,
            &RootOpen {
                path: dir.to_string_lossy().into_owned(),
            },
        );
        let subscribed: Subscribed = client.ask(
            kind::SUBSCRIBE,
            &Subscribe {
                root: opened.root.clone(),
            },
        );
        let mut mirror = Mirror::new();
        mirror.stream = Some(subscribed.stream);
        Self {
            client,
            root: opened.root.clone(),
            opened,
            mirror,
        }
    }

    fn expand(&mut self, at: &RelPath) -> Result<Listing, WireError> {
        let listing = self
            .client
            .request(
                kind::EXPAND,
                &Expand {
                    root: self.root.clone(),
                    path: at.clone(),
                },
            )
            .map(|ok| ok.body_as::<Listing>().unwrap())?;
        self.mirror.listing(&listing);
        Ok(listing)
    }

    fn stats(&mut self) -> TreeStats {
        self.client.ask(
            kind::STATS,
            &Stats {
                root: self.root.clone(),
            },
        )
    }

    /// Applies events until the mirror of `at` matches the disk, or fails after WAIT.
    fn settle(&mut self, at: &RelPath, dir: &Path) {
        let deadline = Instant::now() + WAIT;
        loop {
            self.mirror
                .drain(&mut self.client, Duration::from_millis(100));
            if self.mirror.names(at) == on_disk(dir) {
                return;
            }
            assert!(Instant::now() < deadline, "the tree never matched the disk");
        }
    }
}

fn root_dir(home: &Home) -> PathBuf {
    let dir = home.path.join("project");
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

#[test]
fn changes_arrive_as_revisioned_differences() {
    let mut home = Home::new("changes");
    let dir = root_dir(&home);
    std::fs::write(dir.join("a.txt"), b"1").unwrap();
    std::fs::create_dir(dir.join("sub")).unwrap();
    let mut tree = Tree::open(&mut home, &dir);
    let listing = tree.expand(&RelPath::root()).unwrap();
    assert!(listing.watched && !listing.truncated);
    assert_eq!(tree.mirror.names(&RelPath::root()), on_disk(&dir));

    std::fs::write(dir.join("new.txt"), b"hello").unwrap();
    std::fs::write(dir.join("a.txt"), b"12345").unwrap();
    tree.settle(&RelPath::root(), &dir);
    assert_eq!(
        tree.mirror.dirs[&RelPath::root()].1[b"a.txt".as_slice()].size,
        5,
        "an update carries the new size"
    );
    std::fs::rename(dir.join("new.txt"), dir.join("renamed.txt")).unwrap();
    std::fs::remove_file(dir.join("a.txt")).unwrap();
    std::fs::create_dir(dir.join("newdir")).unwrap();
    std::fs::write(dir.join("caf\u{e9} \u{1f600}.txt"), b"").unwrap();
    tree.settle(&RelPath::root(), &dir);
    assert!(tree.mirror.events > 0);

    // Inside a subdirectory too, and a removed directory takes its cache and watch with it.
    tree.expand(&path(&["sub"])).unwrap();
    std::fs::write(dir.join("sub/x"), b"x").unwrap();
    tree.settle(&path(&["sub"]), &dir.join("sub"));
    assert_eq!(tree.stats().cached_dirs, 2);
    std::fs::remove_dir_all(dir.join("sub")).unwrap();
    tree.settle(&RelPath::root(), &dir);
    let stats = tree.stats();
    assert_eq!((stats.cached_dirs, stats.watches), (1, 1));
}

#[test]
fn expanding_again_reads_the_cache_not_the_disk() {
    let mut home = Home::new("cache");
    let dir = root_dir(&home);
    std::fs::create_dir_all(dir.join("src/deep")).unwrap();
    let mut tree = Tree::open(&mut home, &dir);
    tree.expand(&RelPath::root()).unwrap();
    tree.expand(&path(&["src"])).unwrap();
    assert_eq!(tree.stats().enumerations, 2);
    for _ in 0..3 {
        tree.expand(&RelPath::root()).unwrap();
        tree.expand(&path(&["src"])).unwrap();
    }
    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(
        tree.stats().enumerations,
        2,
        "idle and refreshes read nothing again"
    );
    // Another connection expanding the same directories shares the cache.
    let mut other = Tree::open(&mut home, &dir);
    assert_eq!(other.root, tree.root, "the same root");
    other.expand(&RelPath::root()).unwrap();
    assert_eq!(other.stats().enumerations, 2);
}

#[test]
fn a_directory_changing_while_it_is_read_misses_nothing() {
    let mut home = Home::new("race");
    let dir = root_dir(&home);
    let churn = dir.join("churn");
    std::fs::create_dir(&churn).unwrap();
    let mut tree = Tree::open(&mut home, &dir);
    let writer = {
        let churn = churn.clone();
        std::thread::spawn(move || {
            for i in 0..3000 {
                std::fs::write(churn.join(format!("f{i}")), b"x").unwrap();
                if i % 3 == 0 && i > 0 {
                    let _ = std::fs::remove_file(churn.join(format!("f{}", i - 1)));
                }
                if i % 7 == 0 {
                    let _ =
                        std::fs::rename(churn.join(format!("f{i}")), churn.join(format!("r{i}")));
                }
            }
        })
    };
    std::thread::sleep(Duration::from_millis(30));
    tree.expand(&path(&["churn"])).unwrap();
    writer.join().unwrap();
    tree.settle(&path(&["churn"]), &churn);
}

#[test]
fn symbolic_links_are_never_followed() {
    let mut home = Home::new("links");
    let dir = root_dir(&home);
    let outside = home.path.join("outside");
    std::fs::create_dir_all(outside.join("secret")).unwrap();
    symlink(&outside, dir.join("link")).unwrap();
    std::fs::create_dir(dir.join("d")).unwrap();
    let mut tree = Tree::open(&mut home, &dir);
    let listing = tree.expand(&RelPath::root()).unwrap();
    let link = listing
        .entries
        .iter()
        .find(|e| e.name.as_bytes() == b"link")
        .unwrap();
    assert_eq!(link.kind, EntryKind::Symlink);
    for through in [path(&["link"]), path(&["link", "secret"])] {
        assert_eq!(
            tree.expand(&through).unwrap_err().code,
            ErrorCode::BadRequest
        );
    }
    // A directory replaced by a link after it was expanded: shown as a link, never entered.
    tree.expand(&path(&["d"])).unwrap();
    std::fs::remove_dir(dir.join("d")).unwrap();
    symlink(&outside, dir.join("d")).unwrap();
    tree.settle(&RelPath::root(), &dir);
    assert_eq!(
        tree.mirror.names(&RelPath::root())[b"d".as_slice()],
        EntryKind::Symlink
    );
    assert_eq!(
        tree.expand(&path(&["d"])).unwrap_err().code,
        ErrorCode::BadRequest
    );
    assert_eq!(tree.stats().cached_dirs, 1);
}

#[test]
fn an_unreadable_directory_says_so() {
    // SAFETY: geteuid has no preconditions.
    if unsafe { libc::geteuid() } == 0 {
        return; // root reads everything
    }
    let mut home = Home::new("perm");
    let dir = root_dir(&home);
    std::fs::create_dir(dir.join("locked")).unwrap();
    std::fs::set_permissions(dir.join("locked"), std::fs::Permissions::from_mode(0o000)).unwrap();
    let mut tree = Tree::open(&mut home, &dir);
    let refused = tree.expand(&path(&["locked"])).unwrap_err();
    std::fs::set_permissions(dir.join("locked"), std::fs::Permissions::from_mode(0o700)).unwrap();
    assert_eq!(refused.code, ErrorCode::PermissionDenied);
}

#[test]
fn exclusions_and_the_watch_limit_are_honest() {
    let mut home = Home::new("limits");
    home.env
        .push(("TESSEL_HOST_MAX_WATCHES".into(), "2".into()));
    let dir = root_dir(&home);
    for d in ["node_modules/pkg", "a", "b"] {
        std::fs::create_dir_all(dir.join(d)).unwrap();
    }
    let mut tree = Tree::open(&mut home, &dir);
    assert_eq!(tree.opened.limits.max_watches, 2);
    assert!(
        tree.opened
            .limits
            .exclusions
            .iter()
            .any(|e| e == "node_modules")
    );
    let top = tree.expand(&RelPath::root()).unwrap();
    assert!(
        top.entries
            .iter()
            .find(|e| e.name.as_bytes() == b"node_modules")
            .unwrap()
            .excluded
    );
    let modules = tree.expand(&path(&["node_modules"])).unwrap();
    assert!(
        modules.excluded && !modules.watched,
        "listed, on demand, never watched"
    );
    assert!(tree.expand(&path(&["a"])).unwrap().watched);
    let b = tree.expand(&path(&["b"])).unwrap();
    assert!(!b.watched, "over the watch limit: listed on demand");
    assert_eq!(tree.stats().watches, 2);
    let before = tree.stats().enumerations;
    tree.expand(&path(&["a"])).unwrap();
    assert_eq!(
        tree.stats().enumerations,
        before,
        "a watched directory comes from the cache"
    );
    tree.expand(&path(&["b"])).unwrap();
    assert_eq!(
        tree.stats().enumerations,
        before + 1,
        "an unwatched one is read again, as it said"
    );
}

#[test]
fn dropped_kernel_events_are_followed_by_a_resync() {
    let mut home = Home::new("overflow");
    home.env
        .push(("TESSEL_HOST_TEST_INOTIFY_DELAY_MS".into(), "1500".into()));
    let queue: usize = std::fs::read_to_string("/proc/sys/fs/inotify/max_queued_events")
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    let dir = root_dir(&home);
    let many = dir.join("many");
    std::fs::create_dir(&many).unwrap();
    let mut tree = Tree::open(&mut home, &dir);
    tree.expand(&RelPath::root()).unwrap();
    tree.expand(&path(&["many"])).unwrap();
    for i in 0..queue + 2000 {
        std::fs::File::create(many.join(format!("{i}"))).unwrap();
    }
    let deadline = Instant::now() + WAIT;
    while tree.mirror.resynced.is_empty() {
        assert!(Instant::now() < deadline, "no resync after an overflow");
        tree.mirror
            .drain(&mut tree.client, Duration::from_millis(200));
    }
    // Lagging (if the client fell behind) means: expand again, from the cache.
    if !tree.mirror.stale.is_empty() {
        tree.expand(&path(&["many"])).unwrap();
    }
    tree.settle(&path(&["many"]), &many);
    assert_eq!(tree.mirror.names(&path(&["many"])).len(), queue + 2000);
}

#[test]
fn unsubscribing_stops_events_and_collapsing_drops_watches() {
    let mut home = Home::new("unsubscribe");
    let dir = root_dir(&home);
    let mut tree = Tree::open(&mut home, &dir);
    tree.expand(&RelPath::root()).unwrap();
    let stream = tree.mirror.stream.clone().unwrap();
    tree.client
        .ask::<serde_json::Value>(kind::UNSUBSCRIBE, &Unsubscribe { stream });
    std::fs::write(dir.join("quiet.txt"), b"").unwrap();
    assert!(
        tree.client.event(Duration::from_millis(500)).is_none(),
        "no event after unsubscribing"
    );
    assert_eq!(tree.stats().watches, 1);
    tree.client.ask::<serde_json::Value>(
        kind::COLLAPSE,
        &Collapse {
            root: tree.root.clone(),
            path: RelPath::root(),
        },
    );
    let stats = tree.stats();
    assert_eq!((stats.watches, stats.cached_dirs), (0, 0));
}
