//! Acceptance run of a released tessel-host on a real Linux host, through the SSH client of the
//! machine running it (Linux only: it cuts the link by killing that SSH process).
//!
//! It installs the given binary without root (the commands of `tessel_host::install`, digest
//! checked on the host), connects with `connect --stdio`, then goes through a terminal (input
//! and echo, a link cut and its replay, a fresh attachment's snapshot), a watched directory, the
//! editor's compare-and-swap saves and the git worker, timing each step. Everything it creates
//! is in one new directory under the host's home, removed at the end with the daemon stopped
//! (`--keep` leaves them). Nothing names a host or a user: pass them.
//!
//! ```text
//! cargo run -p tessel-host --example remote_trial -- \
//!     --target USER@HOST --binary tessel-host-x86_64-unknown-linux-musl \
//!     --sha256 <digest from SHA256SUMS> --version 0.1.1 \
//!     [--ssh ssh] [--ssh-option ControlPath=...]... [--git-url URL] [--remove-install] [--keep]
//! ```
//!
//! Each SSH command must authenticate without a prompt (an SSH ControlMaster opened beforehand,
//! or a key): pass `--ssh-option BatchMode=yes` so a lost master fails instead of waiting.
//! Leases and keys are never printed.

#[cfg(not(target_os = "linux"))]
fn main() {
    eprintln!("remote_trial runs on Linux only (it cuts the SSH link with a signal).");
    std::process::exit(2);
}

#[cfg(target_os = "linux")]
fn main() {
    match trial::run() {
        Ok(()) => {}
        Err(error) => {
            eprintln!("FAILED: {error}");
            std::process::exit(1);
        }
    }
}

#[cfg(target_os = "linux")]
mod trial {
    use std::collections::BTreeMap;
    use std::io::{Read, Write};
    use std::process::{Command, Stdio};
    use std::sync::Arc;
    use std::sync::mpsc::Receiver;
    use std::time::{Duration, Instant};
    use tessel_host::install::{self, Arch};
    use tessel_host_client::editor::{Editor, SaveError};
    use tessel_host_client::git::{Git, GitEvent};
    use tessel_host_client::terminal::{Terminal, TerminalEvent};
    use tessel_host_client::tree::{Tree, TreeEvent};
    use tessel_host_client::{Client, CommandConnector, Connector, Options, Pipe};
    use tessel_host_protocol::pty::{AttachMode, Size};
    use tessel_host_protocol::{RawMessage, RelPath, ReplyMessage};

    const WAIT: Duration = Duration::from_secs(30);
    const ECHO_ROUNDS: usize = 20;

    struct Args {
        target: String,
        binary: String,
        sha256: String,
        version: String,
        ssh: String,
        options: Vec<String>,
        git_url: Option<String>,
        remove_install: bool,
        keep: bool,
    }

    fn args() -> Result<Args, String> {
        let mut target = None;
        let mut binary = None;
        let mut sha256 = None;
        let mut version = None;
        let mut ssh = "ssh".to_owned();
        let mut options = Vec::new();
        let mut git_url = None;
        let mut remove_install = false;
        let mut keep = false;
        let mut given = std::env::args().skip(1);
        while let Some(arg) = given.next() {
            let mut value = || given.next().ok_or(format!("{arg} needs a value"));
            match arg.as_str() {
                "--target" => target = Some(value()?),
                "--binary" => binary = Some(value()?),
                "--sha256" => sha256 = Some(value()?.to_ascii_lowercase()),
                "--version" => version = Some(value()?),
                "--ssh" => ssh = value()?,
                "--ssh-option" => options.push(value()?),
                "--git-url" => git_url = Some(value()?),
                "--remove-install" => remove_install = true,
                "--keep" => keep = true,
                other => return Err(format!("unknown argument {other}")),
            }
        }
        let git_url = git_url
            .map(|url| {
                url.bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"-._~:/@".contains(&b))
                    .then_some(url)
                    .ok_or("--git-url has characters this run does not pass to a shell")
            })
            .transpose()?;
        Ok(Args {
            target: target.ok_or("--target is required")?,
            binary: binary.ok_or("--binary is required")?,
            sha256: sha256.ok_or("--sha256 is required (from the release's SHA256SUMS)")?,
            version: version.ok_or("--version is required")?,
            ssh,
            options,
            git_url,
            remove_install,
            keep,
        })
    }

    struct Ssh {
        program: String,
        options: Vec<String>,
        target: String,
    }

    impl Ssh {
        fn args(&self, command: &str) -> Vec<String> {
            let mut args = Vec::new();
            for option in &self.options {
                args.push("-o".to_owned());
                args.push(option.clone());
            }
            args.push(self.target.clone());
            args.push(command.to_owned());
            args
        }

        /// Runs `command` on the host (its shell), with `input` on its standard input.
        fn run(&self, command: &str, input: Option<&[u8]>) -> Result<String, String> {
            let mut child = Command::new(&self.program)
                .args(self.args(command))
                .stdin(if input.is_some() {
                    Stdio::piped()
                } else {
                    Stdio::null()
                })
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .map_err(|e| format!("cannot start {}: {e}", self.program))?;
            if let Some(input) = input {
                let mut stdin = child.stdin.take().expect("piped stdin");
                stdin
                    .write_all(input)
                    .map_err(|e| format!("upload interrupted: {e}"))?;
            }
            let output = child
                .wait_with_output()
                .map_err(|e| format!("ssh failed: {e}"))?;
            if !output.status.success() {
                let mut stderr = String::from_utf8_lossy(&output.stderr).into_owned();
                stderr.truncate(400);
                return Err(format!(
                    "host command failed ({}): {}",
                    output.status,
                    stderr.trim()
                ));
            }
            Ok(String::from_utf8_lossy(&output.stdout).into_owned())
        }
    }

    /// The connector, shared so the run can cut the current link.
    struct Shared(Arc<CommandConnector>);

    impl Connector for Shared {
        fn connect(&self) -> std::io::Result<Pipe> {
            self.0.connect()
        }
    }

    struct Report {
        steps: Vec<(String, Duration)>,
        series: Vec<(String, Vec<Duration>)>,
    }

    impl Report {
        fn step(&mut self, name: &str, took: Duration) {
            println!("  {name}: {} ms", took.as_millis());
            self.steps.push((name.to_owned(), took));
        }

        fn timed<T>(&mut self, name: &str, call: impl FnOnce() -> T) -> T {
            let start = Instant::now();
            let value = call();
            self.step(name, start.elapsed());
            value
        }

        fn print(&mut self) {
            println!("\n== Summary");
            for (name, took) in &self.steps {
                println!("{name:<40} {:>8} ms", took.as_millis());
            }
            for (name, values) in &mut self.series {
                values.sort();
                let at = |q: f64| {
                    values[((values.len() as f64 - 1.0) * q).round() as usize].as_secs_f64()
                        * 1000.0
                };
                println!(
                    "{name:<40} n={} median {:.1} ms, p95 {:.1} ms, max {:.1} ms",
                    values.len(),
                    at(0.5),
                    at(0.95),
                    at(1.0)
                );
            }
        }
    }

    fn nonce() -> Result<String, String> {
        let mut bytes = [0u8; 8];
        std::fs::File::open("/dev/urandom")
            .and_then(|mut f| f.read_exact(&mut bytes))
            .map_err(|e| format!("no randomness: {e}"))?;
        Ok(bytes.iter().map(|b| format!("{b:02x}")).collect())
    }

    fn rel(path: &str) -> RelPath {
        RelPath::from_components(path.split('/').map(|c| c.as_bytes().to_vec()).collect())
            .expect("a valid relative path")
    }

    /// Reads terminal events until `done` says the collected output is enough.
    fn until(
        events: &Receiver<TerminalEvent>,
        text: &mut Vec<u8>,
        what: &str,
        mut done: impl FnMut(&[u8], &TerminalEvent) -> bool,
    ) -> Result<(), String> {
        let deadline = Instant::now() + WAIT;
        loop {
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return Err(format!("timed out waiting for {what}"));
            }
            match events.recv_timeout(left) {
                Ok(event) => {
                    if let TerminalEvent::Output { data, .. } = &event {
                        text.extend_from_slice(data);
                    }
                    if done(text, &event) {
                        return Ok(());
                    }
                }
                Err(_) => return Err(format!("the stream ended waiting for {what}")),
            }
        }
    }

    fn contains(haystack: &[u8], needle: &[u8]) -> bool {
        haystack.windows(needle.len()).any(|w| w == needle)
    }

    /// Types a command whose output (not its echo) carries `mark-<n>`, and times its echo.
    fn round(
        terminal: &Terminal,
        events: &Receiver<TerminalEvent>,
        n: usize,
    ) -> Result<Duration, String> {
        let mut text = Vec::new();
        let start = Instant::now();
        terminal
            .input(format!("printf 'mark-%s\\n' {n}\n").as_bytes())
            .map_err(|e| format!("input: {e}"))?;
        let mark = format!("mark-{n}");
        until(events, &mut text, &mark, |text, _| {
            contains(text, mark.as_bytes())
        })?;
        Ok(start.elapsed())
    }

    pub fn run() -> Result<(), String> {
        let args = args()?;
        let mut report = Report {
            steps: Vec::new(),
            series: Vec::new(),
        };
        let ssh = Ssh {
            program: args.ssh.clone(),
            options: args.options.clone(),
            target: args.target.clone(),
        };
        let bytes =
            std::fs::read(&args.binary).map_err(|e| format!("cannot read the binary: {e}"))?;
        let local = install::sha256_hex(&bytes);
        if local != args.sha256 {
            return Err("the binary's digest is not the one given (--sha256)".into());
        }
        println!("== Install ({} bytes, digest checked)", bytes.len());

        let probe = report.timed("probe (uname, home)", || {
            ssh.run(r#"uname -m && printf '%s\n' "$HOME""#, None)
        })?;
        let mut lines = probe.lines();
        let arch = lines
            .next()
            .and_then(Arch::from_uname)
            .ok_or("unsupported architecture")?;
        let home = lines
            .next()
            .filter(|h| h.starts_with('/') && !h.contains(['"', '\\', '$', '`']))
            .ok_or("the host's home is not a plain absolute path")?
            .to_owned();
        let nonce = nonce()?;
        let plan = install::plan(&args.version, arch, &nonce).ok_or("no install plan")?;
        report.timed("upload", || ssh.run(&plan.upload, Some(&bytes)))?;
        let digest = ssh.run(&plan.digest, None)?;
        if install::parse_digest(&digest).as_deref() != Some(local.as_str()) {
            let _ = ssh.run(&plan.abort, None);
            return Err("the digest on the host differs: upload removed".into());
        }
        println!("  digest on the host: matches");
        let version = report.timed("activate", || ssh.run(&plan.activate, None))?;
        if !install::check_version(&version, &args.version, arch) {
            return Err(format!("unexpected version line: {}", version.trim()));
        }

        println!("== Connect");
        let connector = Arc::new(CommandConnector::new(&args.ssh, ssh.args(&plan.connect)));
        let client = report.timed("connect (ssh + daemon start + hello)", || {
            Client::connect(Box::new(Shared(connector.clone())), Options::default())
        });
        let client = client.map_err(|e| format!("connect: {e}"))?;
        client.epoch().ok_or("no epoch after connect")?;
        let info = client
            .request(RawMessage::empty("host.info"))
            .map_err(|e| format!("host.info: {e}"))?;
        let daemon_pid = match ReplyMessage::from_raw(&info) {
            Ok(Some(ReplyMessage::HostInfo(info))) => {
                println!(
                    "  daemon {} {} pid {}, persistence {:?}",
                    info.version, info.arch, info.pid, info.persistence.method
                );
                info.pid
            }
            _ => return Err("host.info answered something else".into()),
        };

        let test_dir = format!("{home}/tessel-trial-{nonce}");
        let outcome = exercise(&mut report, &client, &connector, &ssh, &args, &test_dir);
        if let Err(error) = &outcome {
            eprintln!("step failed: {error}");
        }

        println!("== Cleanup");
        client.close();
        if args.keep {
            println!("  --keep: daemon (pid {daemon_pid}) and {test_dir} left in place");
        } else {
            let mut stop = format!("kill -TERM {daemon_pid} 2>/dev/null; rm -rf \"{test_dir}\"");
            if args.remove_install {
                stop.push_str("; sleep 1; rm -rf \"$HOME/.tessel-server\"");
            }
            ssh.run(&stop, None)?;
            println!(
                "  daemon stopped, test directory removed{}",
                if args.remove_install {
                    ", installation removed"
                } else {
                    " (installation kept)"
                }
            );
        }
        report.print();
        outcome
    }

    fn exercise(
        report: &mut Report,
        client: &Client,
        connector: &CommandConnector,
        ssh: &Ssh,
        args: &Args,
        test_dir: &str,
    ) -> Result<(), String> {
        ssh.run(&format!("umask 077 && mkdir \"{test_dir}\""), None)?;

        println!("== Terminal");
        let (terminal, events) = report
            .timed("terminal create", || {
                Terminal::create(
                    client,
                    vec!["/bin/sh".into()],
                    Some(test_dir.to_owned()),
                    BTreeMap::new(),
                    Size { rows: 24, cols: 80 },
                )
            })
            .map_err(|e| format!("create: {e}"))?;
        let first = round(&terminal, &events, 0)?;
        report.step("first command echo", first);
        let mut echoes = Vec::new();
        for n in 1..=ECHO_ROUNDS {
            echoes.push(round(&terminal, &events, n)?);
        }
        report.series.push(("input -> output".into(), echoes));

        // A dropped SSH channel: the client reconnects and the terminal replays what it missed.
        let pid = connector.current_pid().ok_or("no SSH process to cut")?;
        let cut = Instant::now();
        // SAFETY: the SSH client process this run started.
        unsafe { libc::kill(pid as i32, libc::SIGKILL) };
        let mut text = Vec::new();
        let mut disconnected = false;
        until(&events, &mut text, "the reattachment", |_, event| {
            disconnected |= matches!(event, TerminalEvent::Disconnected);
            disconnected && matches!(event, TerminalEvent::Reattached { .. })
        })?;
        report.step("link cut -> reattached", cut.elapsed());
        let after = round(&terminal, &events, 100)?;
        report.step("first echo after the cut", after);

        // A fresh attachment (as after a desktop restart) starts with a snapshot of the screen.
        let id = terminal.id().clone();
        let lease = terminal.lease();
        let epoch = client.epoch().ok_or("no epoch")?;
        drop(terminal);
        drop(events);
        let start = Instant::now();
        let (terminal, events) = Terminal::attach_fresh(client, id, epoch, lease)
            .map_err(|e| format!("attach_fresh: {e}"))?;
        let mut screen = Vec::new();
        let deadline = Instant::now() + WAIT;
        loop {
            let left = deadline.saturating_duration_since(Instant::now());
            match events.recv_timeout(left) {
                Ok(TerminalEvent::Snapshot { data, .. }) => {
                    screen.extend_from_slice(&data);
                    break;
                }
                Ok(TerminalEvent::Reattached {
                    mode: AttachMode::Replay,
                    ..
                }) => return Err("a fresh attachment replayed instead of a snapshot".into()),
                Ok(_) => {}
                Err(_) => return Err("no snapshot came".into()),
            }
        }
        report.step("fresh attach -> snapshot", start.elapsed());
        if !contains(&screen, b"mark-100") {
            return Err("the snapshot does not show the last output".into());
        }
        let resumed = round(&terminal, &events, 200)?;
        report.step("first echo after the snapshot", resumed);
        terminal.close().map_err(|e| format!("close: {e}"))?;
        let mut tail = Vec::new();
        until(&events, &mut tail, "the exit", |_, event| {
            matches!(event, TerminalEvent::Exit(_))
        })?;

        println!("== Watched directory and editor");
        let (tree, tree_events) = report
            .timed("tree open", || Tree::open(client, test_dir))
            .map_err(|e| format!("tree: {e}"))?;
        report
            .timed("tree expand", || tree.expand(&RelPath::root()))
            .map_err(|e| format!("expand: {e}"))?;
        let editor = report
            .timed("editor open", || Editor::open(client, test_dir))
            .map_err(|e| format!("editor: {e}"))?;
        let note = rel("note.txt");
        let saved = Instant::now();
        let written = editor
            .save(&note, None, b"one\n")
            .map_err(|e| format!("save: {e:?}"))?;
        report.step("save (new file)", saved.elapsed());
        let deadline = Instant::now() + WAIT;
        loop {
            let left = deadline.saturating_duration_since(Instant::now());
            match tree_events.recv_timeout(left) {
                Ok(TreeEvent::Changed(changed))
                    if changed
                        .added
                        .iter()
                        .chain(&changed.updated)
                        .any(|e| e.name.as_bytes() == b"note.txt") =>
                {
                    break;
                }
                Ok(_) => {}
                Err(_) => return Err("the watched directory never showed the new file".into()),
            }
        }
        report.step("save -> watch event", saved.elapsed());
        let copy = report
            .timed("read", || editor.read(&note))
            .map_err(|e| format!("read: {e}"))?;
        if copy.bytes != b"one\n" || copy.digest != written.digest {
            return Err("read back something else".into());
        }
        report
            .timed("save over its base", || {
                editor.save(&note, Some(copy.base()), b"two\n")
            })
            .map_err(|e| format!("save over base: {e:?}"))?;
        match editor.save(&note, Some(copy.base()), b"stale\n") {
            Err(SaveError::Conflict) => println!("  stale base refused (conflict): ok"),
            other => return Err(format!("a stale base was not refused: {other:?}")),
        }

        println!("== Git");
        let repo = format!("{test_dir}/repo");
        match &args.git_url {
            Some(url) => {
                report.timed("git clone (shallow)", || {
                    ssh.run(
                        &format!("git clone --quiet --depth 1 '{url}' \"{repo}\""),
                        None,
                    )
                })?;
            }
            None => {
                ssh.run(
                    &format!(
                        "git init -q \"{repo}\" && cd \"{repo}\" && printf 'a\\n' > a.txt && git add a.txt && git -c user.name=trial -c user.email=trial@invalid commit -q -m init"
                    ),
                    None,
                )?;
            }
        }
        let opened = Instant::now();
        let (_git, git_events) =
            Git::open(client, test_dir, &rel("repo")).map_err(|e| format!("git open: {e}"))?;
        report.step("git open", opened.elapsed());
        let deadline = Instant::now() + WAIT;
        loop {
            let left = deadline.saturating_duration_since(Instant::now());
            match git_events.recv_timeout(left) {
                Ok(GitEvent::Changed(changed)) if changed.reset => break,
                Ok(_) => {}
                Err(_) => return Err("no first status came".into()),
            }
        }
        report.step("git open -> first status", opened.elapsed());
        let touched = Instant::now();
        editor
            .save(&rel("repo/trial-new.txt"), None, b"new\n")
            .map_err(|e| format!("save in repo: {e:?}"))?;
        let deadline = Instant::now() + WAIT;
        loop {
            let left = deadline.saturating_duration_since(Instant::now());
            match git_events.recv_timeout(left) {
                Ok(GitEvent::Changed(changed))
                    if changed
                        .upsert
                        .iter()
                        .any(|e| e.path.to_bytes() == b"trial-new.txt") =>
                {
                    break;
                }
                Ok(_) => {}
                Err(_) => return Err("the status never showed the new file".into()),
            }
        }
        report.step("file saved -> git status", touched.elapsed());
        Ok(())
    }
}
