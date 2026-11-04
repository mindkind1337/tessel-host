//! Gate 6: git status over the real bridge, compared after every step with a complete
//! `git status` of the same repository.
#![cfg(target_os = "linux")]
mod common;

use common::*;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};
use tessel_host_protocol::git::*;
use tessel_host_protocol::tree::{RootOpen, RootOpened};
use tessel_host_protocol::*;

type Status = BTreeMap<Vec<u8>, (String, Option<Vec<u8>>)>;

fn sh(dir: &Path, home: &Path, script: &str) {
    let ok = Command::new("bash")
        .args(["-ec", script])
        .current_dir(dir)
        .env("HOME", home)
        .env("GIT_AUTHOR_NAME", "Test")
        .env("GIT_AUTHOR_EMAIL", "test@example.invalid")
        .env("GIT_COMMITTER_NAME", "Test")
        .env("GIT_COMMITTER_EMAIL", "test@example.invalid")
        .status()
        .unwrap()
        .success();
    assert!(ok, "script failed: {script}");
}

/// What a complete `git status` says now (without touching the index).
fn reference(dir: &Path) -> Status {
    let output = Command::new("git")
        .args([
            "--no-optional-locks",
            "status",
            "--porcelain=v2",
            "-z",
            "--untracked-files=normal",
        ])
        .current_dir(dir)
        .output()
        .unwrap();
    let (entries, _, _) = tessel_host::git::parse_status(&output.stdout);
    entries
        .into_iter()
        .map(|(k, e)| (k, (e.record, e.orig_path.map(|o| o.to_bytes()))))
        .collect()
}

struct Repo {
    client: Client,
    repo: RepoId,
    status: Status,
    state: Option<GitState>,
    resets: usize,
}

impl Repo {
    fn open(home: &mut Home, root: &Path, below: &str) -> Self {
        let mut client = home.client(Budgets::SLOW_HOST);
        let opened: RootOpened = client.ask(
            tessel_host_protocol::tree::kind::ROOT_OPEN,
            &RootOpen {
                path: root.to_string_lossy().into_owned(),
            },
        );
        let path = if below.is_empty() {
            RelPath::root()
        } else {
            RelPath::from_components(below.split('/').map(|c| c.as_bytes().to_vec()).collect())
                .unwrap()
        };
        let repo: Opened = client.ask(
            kind::OPEN,
            &Open {
                root: opened.root,
                path,
            },
        );
        Self {
            client,
            repo: repo.repo,
            status: Status::new(),
            state: None,
            resets: 0,
        }
    }

    fn subscribe(&mut self) -> StreamId {
        let subscribed: Subscribed = self.client.ask(
            kind::SUBSCRIBE,
            &Subscribe {
                repo: self.repo.clone(),
            },
        );
        subscribed.stream
    }

    fn apply(&mut self, frame: HostFrame) {
        let pos = frame.stream.expect("an event");
        let change: Changed = frame.event.unwrap().body_as().unwrap();
        if change.reset {
            self.status.clear();
            self.resets += 1;
        }
        for entry in change.upsert {
            let mut key = entry.path.to_bytes();
            if entry.untracked_dir {
                key.push(b'/');
            }
            self.status
                .insert(key, (entry.record, entry.orig_path.map(|o| o.to_bytes())));
        }
        for path in change.removed {
            let key = path.to_bytes();
            self.status.remove(&key);
            let mut dir = key;
            dir.push(b'/');
            self.status.remove(&dir);
        }
        self.state = Some(change.state);
        self.client.ack(&pos.id, pos.seq);
    }

    /// Applies events until the status equals a complete `git status` and says it is complete.
    fn settle(&mut self, dir: &Path) {
        let deadline = Instant::now() + WAIT;
        loop {
            while let Some(frame) = self.client.event(Duration::from_millis(150)) {
                self.apply(frame);
            }
            let fresh = self.state.as_ref().is_some_and(|s| s.complete && !s.stale);
            if fresh && self.status == reference(dir) {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "never matched: host {:?}\nreference {:?}\nstate {:?}",
                self.status,
                reference(dir),
                self.state
            );
        }
    }

    fn stats(&mut self) -> GitStats {
        self.client.ask(
            kind::STATS,
            &Stats {
                repo: self.repo.clone(),
            },
        )
    }
}

fn repos(home: &Home) -> PathBuf {
    let dir = home.path.join("repos");
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

#[test]
fn edits_staging_renames_deletes_and_ignores_match_a_complete_status() {
    let mut home = Home::new("gitref");
    let root = repos(&home);
    let dir = root.join("app");
    std::fs::create_dir_all(&dir).unwrap();
    sh(
        &dir,
        &home.path,
        "git init -q -b main; printf 'a\\n' > a.txt; printf 'b\\n' > b.txt; mkdir dir; printf 'c\\n' > dir/c.txt; printf '*.log\\nbuild/\\n' > .gitignore; git add -A; git commit -qm one",
    );
    let mut repo = Repo::open(&mut home, &root, "app");
    repo.subscribe();
    repo.settle(&dir);
    assert_eq!(repo.resets, 1, "the whole status first");
    let after_first = repo.stats();

    std::fs::write(dir.join("a.txt"), b"a, edited\n").unwrap();
    repo.settle(&dir);
    let after_edit = repo.stats();
    assert!(
        after_edit.last_partial_ms > 0 || after_edit.commands == after_first.commands + 1,
        "an edit costs one partial status"
    );
    assert_eq!(
        after_edit.commands,
        after_first.commands + 1,
        "one git command for one edit"
    );

    sh(&dir, &home.path, "git add a.txt");
    repo.settle(&dir);
    sh(&dir, &home.path, "git mv b.txt b2.txt");
    repo.settle(&dir);
    assert!(
        repo.status
            .values()
            .any(|(record, orig)| record.starts_with("2 R")
                && orig.as_deref() == Some(b"b.txt".as_slice()))
    );
    std::fs::remove_file(dir.join("dir/c.txt")).unwrap();
    repo.settle(&dir);
    sh(
        &dir,
        &home.path,
        "printf x > x.log; mkdir -p build; printf o > build/out; printf n > new.txt; mkdir newdir; printf 1 > newdir/one",
    );
    repo.settle(&dir);
    assert!(
        repo.status.contains_key(b"newdir/".as_slice()),
        "an untracked directory is one entry"
    );
    assert!(
        !repo
            .status
            .keys()
            .any(|k| k.starts_with(b"build") || k.ends_with(b".log")),
        "ignored files are not shown"
    );
    std::fs::write(dir.join("newdir/two"), b"2").unwrap();
    repo.settle(&dir);
    sh(&dir, &home.path, "printf 'new.txt\\n' >> .gitignore");
    repo.settle(&dir);
    sh(&dir, &home.path, "git add -A; git commit -qm two");
    repo.settle(&dir);
    assert!(
        repo.status.is_empty(),
        "clean after the commit: {:?}",
        repo.status
    );
    let stats = repo.stats();
    assert!(stats.coverage_complete);
    assert!(stats.commands < 40, "{stats:?}");
}

#[test]
fn a_linked_worktree_and_a_submodule_match_a_complete_status() {
    let mut home = Home::new("gitlinks");
    let root = repos(&home);
    let main = root.join("main");
    let lib = root.join("lib");
    std::fs::create_dir_all(&main).unwrap();
    std::fs::create_dir_all(&lib).unwrap();
    sh(
        &lib,
        &home.path,
        "git init -q -b main; printf 'l\\n' > l.txt; git add -A; git commit -qm lib",
    );
    sh(
        &main,
        &home.path,
        "git init -q -b main; printf 'm\\n' > m.txt; git add -A; git commit -qm main; git -c protocol.file.allow=always submodule add -q ../lib lib; git commit -qm sub; git worktree add -q ../wt -b feature",
    );
    let wt = root.join("wt");

    let mut tree = Repo::open(&mut home, &root, "wt");
    tree.subscribe();
    tree.settle(&wt);
    std::fs::write(wt.join("m.txt"), b"in the worktree\n").unwrap();
    tree.settle(&wt);
    std::fs::write(wt.join("wt-only.txt"), b"new\n").unwrap();
    tree.settle(&wt);
    // A commit in the main worktree moves refs in the common directory: still right here.
    sh(
        &main,
        &home.path,
        "printf 'more\\n' >> m.txt; git commit -qam more",
    );
    tree.settle(&wt);

    let mut parent = Repo::open(&mut home, &root, "main");
    parent.subscribe();
    parent.settle(&main);
    // A change inside the submodule shows as the submodule's entry in the parent.
    std::fs::write(main.join("lib/l.txt"), b"changed inside\n").unwrap();
    parent.settle(&main);
    assert!(
        parent
            .status
            .get(b"lib".as_slice())
            .is_some_and(|(record, _)| record.contains(" S")),
        "{:?}",
        parent.status
    );
    std::fs::write(main.join("lib/new-in-sub.txt"), b"x\n").unwrap();
    parent.settle(&main);
}

#[test]
fn an_untracked_directory_is_listed_on_demand() {
    let mut home = Home::new("gituntracked");
    let root = repos(&home);
    let dir = root.join("app");
    std::fs::create_dir_all(&dir).unwrap();
    sh(
        &dir,
        &home.path,
        "git init -q -b main; printf a > a; git add a; git commit -qm a; mkdir -p cache/deep; printf 1 > cache/one; printf 2 > cache/two; printf 3 > cache/deep/three",
    );
    let mut repo = Repo::open(&mut home, &root, "app");
    repo.subscribe();
    repo.settle(&dir);
    assert!(repo.status.contains_key(b"cache/".as_slice()));
    let path = RelPath::from_components(vec![b"cache".to_vec()]).unwrap();
    let list: UntrackedList = repo.client.ask(
        kind::UNTRACKED,
        &Untracked {
            repo: repo.repo.clone(),
            path,
        },
    );
    let mut names: Vec<String> = list
        .entries
        .iter()
        .map(|p| String::from_utf8_lossy(&p.to_bytes()).into_owned())
        .collect();
    names.sort();
    assert_eq!(names, ["cache/deep/three", "cache/one", "cache/two"]);
    assert!(!list.truncated);
}

#[test]
fn nothing_runs_for_a_repository_nobody_subscribed_to() {
    let mut home = Home::new("gitidle");
    let root = repos(&home);
    let dir = root.join("app");
    std::fs::create_dir_all(&dir).unwrap();
    sh(
        &dir,
        &home.path,
        "git init -q -b main; printf a > a; git add a; git commit -qm a",
    );
    let mut repo = Repo::open(&mut home, &root, "app");
    std::fs::write(dir.join("a"), b"changed").unwrap();
    std::thread::sleep(Duration::from_millis(800));
    assert_eq!(repo.stats().commands, 0);
    let stream = repo.subscribe();
    repo.settle(&dir);
    repo.client
        .ask::<serde_json::Value>(kind::UNSUBSCRIBE, &Unsubscribe { stream });
    let before = repo.stats();
    std::fs::write(dir.join("a"), b"changed again").unwrap();
    std::thread::sleep(Duration::from_millis(1000));
    let after = repo.stats();
    assert_eq!(
        after.commands, before.commands,
        "no git once nobody subscribes"
    );
    assert_eq!(after.watches, 0);
}

#[test]
fn more_entries_than_the_budget_are_marked_truncated_never_complete() {
    let mut home = Home::new("gittruncate");
    let root = repos(&home);
    let dir = root.join("app");
    std::fs::create_dir_all(&dir).unwrap();
    sh(
        &dir,
        &home.path,
        "git init -q -b main; printf a > a; git add a; git commit -qm a",
    );
    for i in 0..50_100 {
        std::fs::File::create(dir.join(format!("u{i}"))).unwrap();
    }
    let mut repo = Repo::open(&mut home, &root, "app");
    repo.subscribe();
    let deadline = Instant::now() + WAIT;
    while repo.state.is_none() {
        assert!(Instant::now() < deadline);
        if let Some(frame) = repo.client.event(Duration::from_millis(200)) {
            repo.apply(frame);
        }
    }
    while let Some(frame) = repo.client.event(Duration::from_millis(300)) {
        repo.apply(frame);
    }
    let state = repo.state.clone().unwrap();
    assert!(state.truncated && !state.complete, "{state:?}");
    assert_eq!(repo.status.len(), 50_000);
}

#[test]
fn a_git_over_its_hard_budget_is_killed_and_retried_later_and_later() {
    let mut home = Home::new("gitkill");
    home.env
        .push(("TESSEL_HOST_TEST_GIT_HARD_MS".into(), "1".into()));
    let root = repos(&home);
    let dir = root.join("app");
    std::fs::create_dir_all(&dir).unwrap();
    sh(
        &dir,
        &home.path,
        "git init -q -b main; printf a > a; git add a; git commit -qm a; printf b > a",
    );
    let mut repo = Repo::open(&mut home, &root, "app");
    repo.subscribe();
    // Each failed read is told (one event each). Attempts at about 0 s, 1 s and 3 s, the next at
    // 7 s: three in four seconds, not one every few milliseconds.
    let mut failures = 0;
    let deadline = Instant::now() + Duration::from_secs(4);
    while Instant::now() < deadline {
        if let Some(frame) = repo.client.event(Duration::from_millis(100)) {
            failures += 1;
            repo.apply(frame);
        }
    }
    let state = repo.state.clone().expect("told about the failure");
    assert!(state.stale && !state.complete, "{state:?}");
    assert!(
        (2..=4).contains(&failures),
        "backed off ({failures} failed reads in 4 s)"
    );
    let stats = repo.stats();
    // The first attempt also lists untracked and ignored directories to build the watches.
    assert!(stats.commands <= 6, "{stats:?}");
}
