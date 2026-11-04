# Changelog

Each release is a tag `v<version>` whose CI publishes the static programs (x86_64 and aarch64
musl), `manifest.json`, `SHA256SUMS` and `LICENSE`. The protocol version is still 1 in every
release below: new fields are optional, and a client relying on one says which release it needs.

## Unreleased

Client (`tessel-host-client`), no daemon change:

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
