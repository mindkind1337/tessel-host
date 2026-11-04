# Changelog

Each release is a tag `v<version>` whose CI publishes the static programs (x86_64 and aarch64
musl), `manifest.json`, `SHA256SUMS` and `LICENSE`. The protocol version is still 1 in every
release below: new fields are optional, and a client relying on one says which release it needs.

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
