# Changelog

Each release is a tag `v<version>` whose CI publishes the static programs (x86_64 and aarch64
musl), `manifest.json`, `SHA256SUMS` and `LICENSE`. The protocol version is still 1 in every
release below: new fields are optional, and a client relying on one says which release it needs.

## Unreleased

Client (`tessel-host-client`), no daemon change:

- Dropping a `CommandConnector` ends the command's whole process tree, not the command alone: on
  Unix it runs in its own process group, killed as a group with `SIGKILL`; on Windows it runs in a
  kill-on-close job object. An ssh's helpers (a proxy command, an askpass) go with it instead of
  lingering after the connection is dropped.
- A connection abandoned before it begins (a `close` that races the opening thread's start) never
  runs its connector, so no process is spawned only to be killed at once. The opening answers
  `ConnectionLost`, as cancelling once connecting was under way already did.

## 0.1.2

Daemon:

- A root whose folder is removed and made again at the same path (a fresh clone, a deployment)
  is read again. The daemon held the removed directory's descriptor and listed it as empty, with
  `ok`, for as long as it ran; reconnecting did not help, since `tree.root.open` handed back the
  same root by path. Each use of a root now checks it against its path (device and inode): a
  different folder there is opened instead, under the same root id, its old cache dropped and
  its subscribers told `Stale { path: "", reason: error }`; a path that names nothing any more
  answers `not_found`, never an empty listing. While a root has folders kept current, its
  subscribers get that `Stale` within two seconds of its folder being removed, moved away or made
  again: the daemon compares it with its path every two seconds, since the kernel says nothing
  about a removed folder the daemon still holds open, and no parent folder is watched.

Client (`tessel-host-client`), no daemon change:

- Dropping a `Tree` or a `Git` lets go of it on the host. Before, nothing was sent: the
  subscription and the expanded folders (their watches) stayed until the connection ended, and
  after a reconnect the dropped session subscribed again and listed its folders again. Now its
  follower stops at once, even on a silent stream or while the application's receiver is full
  (it no longer blocks in `send`); `tree.unsubscribe` and a `tree.collapse` of the topmost
  expanded folders (or `git.unsubscribe`) go out from a thread of their own, never waited for;
  it is never subscribed again; nothing more is handed over. Dropping only the receiver lets go
  at the next event, as for terminals. Folder ownership is tracked across trees of the same
  client, so replacing a tree on the same root preserves the replacement's watches.
- Reconnection delivers `Disconnected` before replacement Tree/Git events. Cleanup uses the
  stream's own link; late followers cannot replace a newer root/repository. The supervisor
  keeps reconnecting if a replacement link dies during session recovery, and close notification
  registration is synchronized with link shutdown.
- Late events of a stream let go of (`Link::unregister`) are dropped instead of kept as orphans,
  where they held memory and orphan slots for the life of the link and could crowd out a new
  stream's first events (the last 1,024 such streams are remembered; claiming one again makes it
  live).

- `Terminal::create_keyed` takes the deduplication key from the caller and the epoch it means,
  so a create whose reply was lost can be sent again within the same run of the desktop and
  find the same terminal and lease (the daemon keeps the outcome 10 minutes, per run). Another
  epoch answers `EpochChanged` without sending; a terminal still being created (`busy`) is
  retried briefly.
- Snapshots are checked part by part before anything is kept: parts must come in order and
  agree on the screen, nothing else may come in the middle, and one snapshot holds at most
  `MAX_SNAPSHOT_BYTES` (16 MiB). Otherwise `TerminalEvent::StreamRejected` (`InvalidSnapshot`
  or `OversizedSnapshot`) ends this terminal's stream only: it is let go and never
  reattached, the link and the other terminals go on, nothing reconnects for it.
- A terminal object that was let go (dropped receiver, rejected stream) no longer types or
  closes: `lease()` is `None` and input, resize, signal and close are refused. The lease stays
  valid on the host. `close_terminal(client, epoch, terminal, lease)` ends such a program
  without attaching (no stream, no snapshot), from an explicit action; another epoch answers
  `EpochChanged` without sending.
- A stream rejected by an old follower (one replaced after a reconnection, still reading what its
  dead link held) no longer lets go of the terminal its successor follows: the rejection and the
  letting go happen under the same lock as a new generation, for the current one only.

- `Terminal::create_keyed` answers a `CreatedTerminal { terminal, events, lease }`: the lease
  the create granted comes with the answer, apart from the object, so it can still be kept and
  used to end the program explicitly when the object lets go right away (a snapshot refused
  just after attaching). `Terminal::take_lease` returns the lease it took. `lease()` stays the
  live authority to type.
- `DedupKey` is a secret like `LeaseKey`: its `Debug` shows `DedupKey(<hidden>)`. Sending it again
  within the daemon's time returns the terminal and its lease, so it must not reach a log.
- `RawMessage`'s `Debug` shows the kind only, never the body, which can carry a lease, a
  deduplication key, typed input or a file's contents. Frames that contain messages inherit
  this. Nothing changes on the wire. Hiding a value from `Debug` does not wipe it from
  memory: keep such values encrypted at rest.

Repository:

- `fixtures/install/plan.json`: the install plan's eight commands for x86_64 and aarch64
  (version 0.1.1, a fixed nonce), byte for byte, checked against `install::plan` by a test, so
  another client's port of the install can be compared exactly.
- `cargo run -p tessel-host --example remote_trial`: an acceptance run on a real Linux host
  through the local SSH client (install without root with its digest checked on the host,
  terminal with a link cut and a fresh snapshot, watched directory, editor saves, git), timing
  each step and removing what it made. Host and user are arguments; Linux only.
- Tests: two tests of one process no longer share a home directory when they use the same
  name. They did, and wiped each other's daemon and repository now and then (a flaky CI run).
- GUIDE-CLIENT.md: `connect --stdio` is the exec request's own command, never a line written to
  a shell's standard input. Dash (`/bin/sh` on Debian and Ubuntu) reads past that line and
  swallows the client's hello; the daemon then closes the connection after its hello timeout.

## 0.1.1

Daemon and protocol:

- `terminal.attach` without `after`: the attachment always starts with a snapshot of the current
  screen, then live events. It is for a client that shows nothing of the terminal yet, such as a
  desktop that just started. A 0.1.0 daemon refuses that request (`bad_request`) and never reads
  it as 0.
- `terminal.attached` carries `last_input` whenever the attachment holds the lease: the highest
  input identifier that lease used, or 0 for a new lease. A client that kept a lease across its
  own restart numbers its inputs above it. Before this, it started again from 1, and the daemon
  took those inputs for repeats and did not type them.

Client (`tessel-host-client`):

- `Terminal::attach_fresh` for a desktop that restarts. Inputs are numbered above `last_input`,
  and a lease whose numbering is missing or exhausted is refused instead of restarted from 1.
- One stream at a time hands a terminal's events over, in order and once. `Disconnected` always
  comes before `Reattached`, and nothing from a replaced stream comes after it, even when the
  application's receiver was full at the time of the break. A takeover replaces the attachment
  the same way. `last_seq()` is what was handed over, not what the UI applied.
- Dropping a `Terminal`, or only its receiver, lets go of it: the attachment is detached on the
  host at once (even while the program prints nothing) and never reattached, and the program
  goes on with its lease.
- Opening a link is bounded by `Options::connect_timeout` (20 s by default). `Client::close`
  abandons a reconnection in progress, and `Client::connect_cancellable` lets the UI cancel the
  first connection.
- `CommandConnector`: no console window on Windows. Dropping a connection kills its process at
  once and reaps it on a thread of its own.
- Inputs are documented as typed at most once: `Ok` means typed, and an error after a
  reconnection that outlasted `request_timeout` means it may or may not have been.

## 0.1.0

First release: the daemon (persistent terminals, file trees, editor saves, git status), the
`connect --stdio` bridge, the installer plan, and the client crate.
