# tessel-host

The program Tessel installs on a Linux host, without root, to keep terminals, file trees, git
status and editor saves on the host side of one SSH connection. It is shared by the Rust and Vue
versions of Tessel and has its own releases.

- `crates/host-protocol`: the version 1 wire protocol, transport-independent
  ([PROTOCOL.md](crates/host-protocol/PROTOCOL.md)): bounded frames, negotiation of independent
  capabilities (`pty`, `tree`, `git`, `editor`, `tunnel.reverse`), structured errors,
  cancellation, stream acknowledgments and deduplication. Golden fixtures and a deterministic
  fuzzer check it.
- `crates/host`: the `tessel-host` program. `serve` is the daemon, one per user and version, on a
  private Unix socket in `~/.tessel-server/host/<version>/`; `connect --stdio` is what an SSH
  channel runs: it starts the daemon when needed and relays bytes. Losing the connection never
  stops the daemon. `install` is the client side of putting the program on a host with the exec
  channel it already has (`cat`, `sha256sum`, `chmod`, `mv`: no curl, sudo or package manager).
- `crates/host-client`: the client library for the desktop (builds on Windows): one link over any
  byte stream (`CommandConnector` runs e.g. `ssh host <installed> connect --stdio`), reconnection
  with resumed sessions on the same daemon and an explicit state on a restarted one, and typed
  sessions for terminals, file trees, the editor and git.
- `crates/host-xtask`: builds and checks releases.
- [GUIDE-CLIENT.md](GUIDE-CLIENT.md): the exact sequences a desktop implements, installation,
  degraded states and the editor's dirty buffer across a reconnect included.
- `fixtures/sessions/`: recorded sessions with a real daemon (terminal, tree, editor, git), one
  frame per line, for clients in other languages to replay; checked on every CI run.
- [HOST_PROTOCOL_PLAN.md](HOST_PROTOCOL_PLAN.md): the plan, its acceptance gates and the evidence
  for each.

## Status

Gates 1 (protocol), 2 (daemon, bridge, rootless installer, static builds) , 3 (terminals that
outlive the connection, with replay or a snapshot on reattach), 4 (file trees kept current
with inotify, from a cache), 5 (the editor: chunked reads, saves that never overwrite a
change silently) and 6 (git status kept current from file events, one bounded low-priority git
at a time) are done; for gate 7 the client library, its guide and the cross-client fixtures are
done, and the desktop integration (in the Rust desktop) follows. Nothing here replaces
tmux or the JavaScript helper on a host yet.

## Build and test (Linux)

```sh
cargo test --locked --workspace
cargo clippy --locked --workspace --all-targets -- -D warnings
```

`rust-toolchain.toml` pins the toolchain and installs both musl targets.

## Releases

```sh
cargo run --locked -p tessel-host-xtask -- release --out dist
cargo run --locked -p tessel-host-xtask -- accept --out dist --qemu /usr/bin/qemu-aarch64-static
```

`release` builds `x86_64-unknown-linux-musl` and `aarch64-unknown-linux-musl` (aarch64 linked
with rust-lld), refuses a dependency on a desktop toolkit, web view or JavaScript runtime, checks
that each ELF file is static for its machine, and writes `manifest.json` and `SHA256SUMS`.
`accept` runs each program: version, daemon, bridge, hello, host information, a killed bridge and
the same daemon afterwards; a foreign architecture runs under a user-mode emulator. CI does both
on every push and keeps `dist/` as an artifact.

## Using the protocol from Tessel

Depend on a pinned commit:

```toml
tessel-host-protocol = { git = "https://github.com/mindkind1337/tessel-host", rev = "<commit>" }
```

The repository is private: Cargo then needs `net.git-fetch-with-cli = true` so that git uses
your credentials.

See [CONTRIBUTING.md](CONTRIBUTING.md) for the language rule (Rust, and TypeScript for anything
else) and the checks to run.
