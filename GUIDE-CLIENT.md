# Client guide

How the desktop (the Rust version through `tessel-host-client`, the Vue version by the same
sequences) talks to `tessel-host`. The wire contract is
[`crates/host-protocol/PROTOCOL.md`](crates/host-protocol/PROTOCOL.md); recorded sessions to
replay are in [`fixtures/sessions/`](fixtures/sessions) (one JSON object per line: `from` is
`client` or `host`, `frame` is the frame; random identifiers, digests, times and paths are made
stable, everything else is what the daemon sent).

## 1. Where the programs come from

Each release of tessel-host is a tag (`v<version>`) whose CI publishes the static programs for
`x86_64` and `aarch64`, `manifest.json`, `SHA256SUMS` and `LICENSE` as release assets. The
desktop carries them in its own repository (an xtask copies them and checks `SHA256SUMS`); it
never downloads anything at run time.

## 2. Installing on a host, and connecting

`tessel_host::install` (no Linux dependency; it builds on Windows) is the only source of the
commands run on a host and of the program's path. A client passes its strings to the exec
channel as they are: it never builds a path, quotes or expands `$HOME` itself.

```rust
pub fn plan(version: &str, arch: Arch, nonce: &str) -> Option<Plan>;   // nonce: 16 hex digits
pub fn commands(version: &str, arch: Arch) -> Option<(String, String)>; // (version, connect)
pub struct Plan { pub probe, pub upload, pub digest, pub activate, pub abort,
                  pub installed, pub version, pub connect: String }
pub fn Arch::from_uname(output: &str) -> Option<Arch>;
pub fn Manifest::parse(text: &str) -> Option<Manifest>;
pub fn matches(artifact: &Artifact, bytes: &[u8]) -> bool;
pub fn parse_digest(output: &str) -> Option<String>;
pub fn check_version(output: &str, version: &str, arch: Arch) -> bool;
```

1. Already installed? Run `commands(version, arch).0` (the version command) and accept it only if
   `check_version(output, version, arch)`. The architecture comes from step 2 the first time;
   remember it per host.
2. Otherwise run `plan.probe` (`uname -m`) and map it with `Arch::from_uname`. Unknown: not
   supported; keep the SFTP and terminal path for this host.
3. Take the artifact for that architecture from the release's `manifest.json`; check the bytes
   with `matches(artifact, bytes)` before sending.
4. Run `plan.upload` with the program on its standard input, then `plan.digest`, and compare
   `parse_digest(output)` with the manifest. Different: run `plan.abort`, report, stop.
5. Run `plan.activate`; it ends with the installed program's version line: accept it only if
   `check_version(output, version, arch)`.
6. A connection is one SSH exec channel running `plan.connect` (or `commands(..).1`):
   `"$HOME/.tessel-server/host/bin/tessel-host-<version>-<arch>" connect --stdio`, the path in
   double quotes so the host's shell expands `$HOME`. With the system ssh:
   `CommandConnector::new("ssh", ["-T", host, &connect])`, the command as one argument. On
   Windows it starts ssh without a console window, so no window flashes at each connection or
   reconnection.

The connection starts the daemon when there is none. Then:

```rust
let client = Client::connect(Box::new(connector), Options::default())?;
```

`Options` lists the capabilities wanted (`pty`, `tree`, `editor`, `git`) and the budgets.
`connect_timeout` (20 s by default) bounds each connection attempt, from opening the channel to
the daemon's hello: an SSH channel that stays up while the daemon never answers is abandoned and
its process stopped (`Client::connect` answers `Timeout`; a reconnection tries again).
`client.close()` also abandons a reconnection in progress; a closed client stays `Closed`. To let
the person cancel the first connection, use `Client::connect_cancellable(connector, options,
opening)` and call `opening.cancel()` from the UI: the attempt's process is stopped at once.
`client.hello()` says what the host granted; a capability not granted is not offered in the UI.
Ask `host.info` once for `persistence.survives_logout`: show `no` and `unknown` as a warning next
to terminals ("terminals end when you log out of this host" / "may end").

## 3. The connection's state

`client.state()` is one of:

| state | show |
| --- | --- |
| `Connected { epoch }` | normal |
| `Reconnecting { attempt }` | every pane of this host marked "reconnecting", input kept local, nothing presented as live |
| `Restarted { previous, current }` | "the host's daemon restarted": its terminals are gone; trees, editors and git reopened by path |
| `Closed` | closed by the application |

A request made while reconnecting answers `ConnectionLost`; the session types below wait and
retry by themselves where that is safe, and say so otherwise.

## 4. Terminals

```rust
let (terminal, events) = Terminal::create(&client, argv, cwd, env, size)?;
```

- Keep `terminal.id()` and `client.epoch()` with the pane. After a restart of the desktop the
  pane shows nothing, so attach with `Terminal::attach_fresh(&client, id, epoch, lease)`: the
  events start with a snapshot of the current screen, then go on live (or the attach says the
  terminal is gone). Its inputs are numbered above the ones the lease already used (the daemon
  says how far they went); a lease whose numbering the daemon does not give is refused, never
  restarted from 1. Needs tessel-host 0.1.1 or later. `Terminal::attach(.., after, ..)` is for a
  pane that still shows the terminal (after the last event it applied: an `Output`'s or
  `Resized`'s `seq`, a `Snapshot`'s `at`). `terminal.last_seq()` is the last event handed to your
  receiver, not necessarily applied yet. The lease is a capability (whoever has it types into the terminal): keep it
  in memory, or encrypted with the desktop's secrets if it must outlive a restart; never in the
  workspace file, a backup or a log (`LeaseKey`'s `Debug` hides it). Without it, attach as a
  watcher and take the lease (`take_lease`).
- Apply events in order:
  - `Output { data }`: feed the terminal emulator.
  - `Resized { size }`: resize the emulator here, between the bytes before and after.
  - `Snapshot { data, size, alternate, resync }`: move the visible screen into your own
    scrollback, reset the screen to `size`, then feed `data`. It redraws the screen, the main
    screen behind an alternate one, input modes, kitty keyboard flags, focus reporting and the
    cursor style without replaying what produced them. With `resync`, show a discreet "some
    output was skipped" marker.
  - `Exit`: show the end (code or signal). It comes once.
  - `LeaseLost`: another client took the terminal: read-only badge, a "Take over" action
    (`terminal.take_lease()`, whose `Ok` means this client holds it again), keyboard input
    disabled.
  - `Disconnected`: "reconnecting" on the pane; keep what is shown. It always comes before
    `Reattached` or `Lost`, once per break.
  - `Reattached { mode, lease }`: back; `lease: false` means read-only now. Everything the new
    attachment sends (replay or snapshot) comes after it; nothing from the old one does, even
    when your receiver was full at the time of the break.
  - `Lost`: the daemon restarted: the pane's program is gone; offer to start a new one.
- `terminal.input(bytes)` types each input at most once, even across a reconnect or a busy
  program (it retries with the same input number until `request_timeout`): `Ok` means typed; an
  error after a reconnection that outlasted the timeout means it may or may not have been, so
  say so rather than typing it again. Resize, signal and close need the lease.
- Closing a pane: dropping the `Terminal` (or only its receiver) lets go of it at once, even
  while its program prints nothing. The daemon drops the attachment and the client never
  reattaches it, but the program goes on with its lease, so keep the id, epoch and lease to
  pick it up later with `attach_fresh`. To end the program, `terminal.close()` first.

## 5. File trees

```rust
let (tree, events) = Tree::open(&client, "/home/user/project")?;
let listing = tree.expand(&RelPath::root())?;
```

- Names are raw bytes (`entry.name`); show `entry.label`. Symbolic links are `symlink` entries,
  never entered.
- A listing says `watched` or not. Not watched (excluded such as `node_modules`, beyond the
  watch limit, or `truncated`): mark the folder "not kept up to date" and expand it again when
  opened again; never show it as empty or complete.
- `Changed { path, revision, added, updated, removed }`: apply to the listing of `path` only if
  `revision` is above that listing's revision. Keep events of a folder you are still expanding
  until its listing arrives.
- `Stale { path }`: expand `path` again (from the host's cache: cheap). An empty `path` means
  the whole root (the client fell behind).
- `Resynced`: the host read everything again after the kernel dropped events; the tree is
  complete again.
- `Relisted(listing)`: after a reconnect (or a daemon restart), replace what is shown for that
  folder.
- Collapse folders you no longer show (`tree.collapse`): the host stops watching them.

## 6. The editor

```rust
let editor = Editor::open(&client, "/home/user/project")?;
let copy = editor.read(&path)?;          // bytes, version, digest
```

Keep `copy.base()` with the open document: it is what a save is checked against.

Saving:

1. `editor.save(&path, Some(base), &buffer)`; a new file passes `None`.
2. `Ok(written)`: the buffer is clean; the new base is `{ written.version, written.digest }`.
3. `Err(SaveError::Conflict)`: the file changed on the host since `base`. Nothing was written.
   Read it again (`editor.read`) and show both versions; the person chooses. Never save again
   automatically with the new base.
4. `Err(SaveError::NoSpace)`: the host's disk or quota is full; nothing was written, the file is
   intact. Keep the buffer dirty and say so.
5. `Err(SaveError::Other(_))`: keep the buffer dirty; offer to try again.

The dirty buffer and a reconnect:

- While reconnecting, the document stays editable and dirty; the buffer is the desktop's.
- A save in progress when the link breaks is sent again whole once reconnected, against the same
  base (the host dropped the partial upload). The caller sees one `save` call.
- On reconnect, for each open document, `editor.compare(&path, &base.digest)`: `same: true`, the
  host's file is still what the buffer started from (save normally when the person saves);
  `same: false`, the file changed on the host while disconnected: show it before the next save
  (the save would answer `Conflict` anyway).
- After a daemon restart nothing changes for the editor: files are on disk, the root is reopened
  by path.

`rename`, `remove` (with the version read, so a changed file is not removed) and `mkdir` never
replace anything: an existing destination is `Conflict`.

## 7. Git

```rust
let (git, events) = Git::open(&client, "/home/user/project", &RelPath::root())?;
```

- `Changed { reset, upsert, removed, state }`: with `reset`, clear the list first. Entries carry
  `xy`, `sub`, the full porcelain `record`, raw paths, and `orig_path` for renames. An
  `untracked_dir` entry is a whole untracked folder: list its files with `git.untracked(path)`
  when opened.
- Show the state as it is: `complete` (fully read, nothing cut, every folder watched), `stale`
  (a refresh is pending or the last one failed: keep showing the list, marked "updating" or
  "out of date"), `truncated` ("more than 50,000 changes: list cut"), `slow`, and the branch.
- `git.refresh()` asks for a full read (a "Refresh" button); otherwise nothing needs asking.
- After a reconnect or a daemon restart the session subscribes again and the next event resets
  the list.

## 8. Falling back

Keep the SFTP and terminal path for hosts where the program cannot be installed (unknown
architecture, `noexec` home, failed checks), and show which one a host uses. Move existing tmux
sessions only when the person starts new terminals through the daemon; never end them silently.

## 9. Checking a client

Replay `fixtures/sessions/*.jsonl` against the client: the frames it sends should match the
`client` lines (identifiers and digests aside), and it should accept every `host` line.
`crates/host-protocol/tests/sessions.rs` checks the files themselves; `crates/host/tests/
record_fixtures.rs` records them again from a real daemon.
