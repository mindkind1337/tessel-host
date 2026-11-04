//! Gate 6 benchmark (not run by default: `cargo test --release -p tessel-host --test git_bench
//! -- --ignored --nocapture`). A synthetic repository of 20,000 tracked files and 300,000
//! untracked ones (in untracked directories, plus 1,000 in tracked ones); measures, end to end
//! over the real bridge, the first full status, the time from an edit to its status event (p50,
//! p95), git commands and bytes per edit, a full refresh, and a terminal's echo latency while
//! all that runs.
#![cfg(target_os = "linux")]
mod common;

use common::*;
use std::collections::BTreeMap;
use std::path::Path;
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use tessel_host_protocol::git::*;
use tessel_host_protocol::pty::{self, Attach, Created, Writer};
use tessel_host_protocol::tree::{RootOpen, RootOpened};
use tessel_host_protocol::*;

fn percentile(values: &mut [f64], p: f64) -> f64 {
    values.sort_by(|a, b| a.partial_cmp(b).unwrap());
    if values.is_empty() {
        return 0.0;
    }
    values[((values.len() - 1) as f64 * p).round() as usize]
}

fn git(dir: &Path, home: &Path, args: &[&str]) {
    let ok = Command::new("git")
        .args(args)
        .current_dir(dir)
        .env("HOME", home)
        .env("GIT_AUTHOR_NAME", "Bench")
        .env("GIT_AUTHOR_EMAIL", "bench@example.invalid")
        .env("GIT_COMMITTER_NAME", "Bench")
        .env("GIT_COMMITTER_EMAIL", "bench@example.invalid")
        .status()
        .unwrap()
        .success();
    assert!(ok);
}

/// A terminal running `cat`, typed into every 100 ms until `stop`; returns echo latencies (ms).
fn echo_probe(mut client: Client, stop: Arc<AtomicBool>) -> Vec<f64> {
    let created: Created = client.ask(
        pty::kind::CREATE,
        &pty::Create {
            dedup: DedupKey::from_bytes([42; 16]),
            argv: vec!["cat".into()],
            cwd: None,
            env: BTreeMap::new(),
            size: pty::Size { rows: 24, cols: 80 },
        },
    );
    let mut viewer = Viewer::new();
    let attached: pty::Attached = client.ask(
        pty::kind::ATTACH,
        &Attach {
            terminal: created.terminal.clone(),
            epoch: client.epoch.clone(),
            after: 0,
            writer: Writer::Resume(created.lease.clone()),
        },
    );
    viewer.attached(attached.stream);
    let mut latencies = Vec::new();
    let mut id = 0u64;
    while !stop.load(Ordering::SeqCst) {
        id += 1;
        let token = format!("k{id}z");
        let started = Instant::now();
        client.send(
            pty::kind::INPUT,
            &pty::Input {
                terminal: created.terminal.clone(),
                lease: created.lease.clone(),
                input_id: id,
                data: Chunk::new(token.as_bytes().to_vec()).unwrap(),
            },
        );
        viewer.until(&mut client, |v| v.text().contains(&token));
        latencies.push(started.elapsed().as_secs_f64() * 1000.0);
        std::thread::sleep(Duration::from_millis(100));
    }
    latencies
}

#[test]
#[ignore]
fn benchmark_300k_untracked_20k_tracked() {
    let mut home = Home::new("gitbench");
    let dir = home.path.join("big");
    std::fs::create_dir_all(&dir).unwrap();
    let setup = Instant::now();
    for d in 0..200 {
        let sub = dir.join(format!("src/d{d:03}"));
        std::fs::create_dir_all(&sub).unwrap();
        for f in 0..100 {
            std::fs::write(sub.join(format!("f{f:03}.txt")), format!("{d} {f}\n")).unwrap();
        }
    }
    git(&dir, &home.path, &["init", "-q", "-b", "main"]);
    git(&dir, &home.path, &["add", "-A"]);
    git(&dir, &home.path, &["commit", "-qm", "tracked"]);
    for c in 0..30 {
        let sub = dir.join(format!("cache/c{c:02}"));
        std::fs::create_dir_all(&sub).unwrap();
        for f in 0..10_000 {
            std::fs::File::create(sub.join(format!("{f:05}"))).unwrap();
        }
    }
    for i in 0..1000 {
        std::fs::write(
            dir.join(format!("src/d{:03}/untracked{i}.tmp", i % 200)),
            b"u",
        )
        .unwrap();
    }
    eprintln!("setup: {:.1} s", setup.elapsed().as_secs_f64());

    let mut client = home.client(Budgets::SLOW_HOST);
    let opened: RootOpened = client.ask(
        tessel_host_protocol::tree::kind::ROOT_OPEN,
        &RootOpen {
            path: home.path.to_string_lossy().into_owned(),
        },
    );
    let repo: Opened = client.ask(
        kind::OPEN,
        &Open {
            root: opened.root,
            path: RelPath::from_components(vec![b"big".to_vec()]).unwrap(),
        },
    );
    let started = Instant::now();
    client.ask::<Subscribed>(
        kind::SUBSCRIBE,
        &Subscribe {
            repo: repo.repo.clone(),
        },
    );
    let mut entries = 0usize;
    let mut state: Option<GitState> = None;
    while state.as_ref().is_none_or(|s| !s.complete) {
        let frame = client.event(WAIT).expect("the first status");
        let pos = frame.stream.clone().unwrap();
        let change: Changed = frame.event.unwrap().body_as().unwrap();
        entries += change.upsert.len();
        state = Some(change.state);
        client.ack(&pos.id, pos.seq);
    }
    let first_ms = started.elapsed().as_secs_f64() * 1000.0;
    let stats_first: GitStats = client.ask(
        kind::STATS,
        &Stats {
            repo: repo.repo.clone(),
        },
    );

    // A terminal typed into all along, on another connection.
    let stop = Arc::new(AtomicBool::new(false));
    let probe = {
        let terminal_client = home.client(Budgets::SLOW_HOST);
        let stop = stop.clone();
        std::thread::spawn(move || echo_probe(terminal_client, stop))
    };

    let mut latencies = Vec::new();
    for i in 0..40 {
        let name = format!("src/d{:03}/f{:03}.txt", (i * 7) % 200, (i * 13) % 100);
        let edited = Instant::now();
        std::fs::write(dir.join(&name), format!("edited {i}\n")).unwrap();
        let wanted =
            RelPath::from_components(name.split('/').map(|p| p.as_bytes().to_vec()).collect())
                .unwrap();
        loop {
            let frame = client.event(WAIT).expect("a status event for the edit");
            let pos = frame.stream.clone().unwrap();
            let change: Changed = frame.event.unwrap().body_as().unwrap();
            client.ack(&pos.id, pos.seq);
            if change.upsert.iter().any(|e| e.path == wanted) {
                latencies.push(edited.elapsed().as_secs_f64() * 1000.0);
                break;
            }
        }
        std::thread::sleep(Duration::from_millis(150));
    }
    let stats_edits: GitStats = client.ask(
        kind::STATS,
        &Stats {
            repo: repo.repo.clone(),
        },
    );

    // Full refreshes while the terminal is typed into.
    let mut fulls = Vec::new();
    for _ in 0..5 {
        client.ask::<serde_json::Value>(
            kind::REFRESH,
            &Refresh {
                repo: repo.repo.clone(),
            },
        );
        std::thread::sleep(Duration::from_millis(400));
        while let Some(frame) = client.event(Duration::from_millis(300)) {
            let pos = frame.stream.clone().unwrap();
            client.ack(&pos.id, pos.seq);
        }
        let s: GitStats = client.ask(
            kind::STATS,
            &Stats {
                repo: repo.repo.clone(),
            },
        );
        fulls.push(s.last_full_ms as f64);
    }
    let stats_end: GitStats = client.ask(
        kind::STATS,
        &Stats {
            repo: repo.repo.clone(),
        },
    );
    stop.store(true, Ordering::SeqCst);
    let mut echoes = probe.join().unwrap();

    // For comparison: what the status that blocked sessions does on the same repository.
    let all_started = Instant::now();
    let all = Command::new("git")
        .args([
            "--no-optional-locks",
            "status",
            "--porcelain=v2",
            "-z",
            "--untracked-files=all",
        ])
        .current_dir(&dir)
        .output()
        .unwrap();
    let all_ms = all_started.elapsed().as_secs_f64() * 1000.0;
    let all_entries = all
        .stdout
        .split(|&b| b == 0)
        .filter(|r| r.starts_with(b"?"))
        .count();

    let commands = stats_edits.commands - stats_first.commands;
    let bytes = stats_edits.output_bytes - stats_first.output_bytes;
    let report = serde_json::json!({
        "tracked": 20_000, "untracked": 301_000, "status_entries": entries,
        "first_status_ms": first_ms.round(),
        "first_status_commands": stats_first.commands, "first_status_bytes": stats_first.output_bytes,
        "edit_to_event_ms": { "p50": percentile(&mut latencies, 0.5).round(), "p95": percentile(&mut latencies, 0.95).round(), "max": percentile(&mut latencies, 1.0).round() },
        "edits": 40, "commands_for_edits": commands, "bytes_for_edits": bytes,
        "full_refresh_ms": { "p50": percentile(&mut fulls, 0.5), "max": percentile(&mut fulls, 1.0) },
        "echo_ms": { "samples": echoes.len(), "p50": percentile(&mut echoes, 0.5), "p95": percentile(&mut echoes, 0.95), "max": percentile(&mut echoes, 1.0) },
        "watches": stats_end.watches, "coverage_complete": stats_end.coverage_complete,
        "commands_total": stats_end.commands, "bytes_total": stats_end.output_bytes,
        "for_comparison_untracked_files_all": { "ms": all_ms.round(), "entries": all_entries, "bytes": all.stdout.len() },
    });
    println!("{}", serde_json::to_string_pretty(&report).unwrap());
    assert!(
        percentile(&mut echoes, 0.95) < 100.0,
        "the terminal stayed responsive"
    );
    assert!(commands <= 45, "about one git command per edit");
}
