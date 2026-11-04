# Tessel host protocol, version 1

The wire contract between a Tessel client (the Rust desktop, later the Vue app) and
`tessel-host` on a remote machine. `src/` implements it; `tests/fixtures/` holds one golden
frame of each shape; `tests/wire.rs` checks the rules below and `tests/fuzz.rs` fuzzes them.
Terminal, file tree, editor and git messages are below.

## Frames

A frame is a four-byte big-endian length, then that many bytes of UTF-8 JSON.

- At most 1 MiB, or less once negotiated (`frame_bytes`). A larger or zero length is refused
  from the header alone, before any payload is read or allocated.
- At most 16 nested objects and arrays, counted before parsing.
- No unknown field and no duplicate field, at any level of a frame's own structure.
- Numbers are integers within ±(2^53 − 1), bodies included: no fraction, no exponent, so a
  number means the same on both sides, in JavaScript too, and survives being sent again.
- Bytes (file content, terminal output) are canonical padded base64 of the standard alphabet,
  at most 64 KiB once decoded. One spelling per value: unpadded text or stray bits are refused.
- Paths below an opened root are arrays of names, each the base64 of its raw bytes: Linux names
  need not be UTF-8. No name is empty, `.`, `..`, or contains `/` or NUL; at most 255 bytes per
  name, 4096 for the whole path, 1024 names. `[]` is the root. Labels for people are separate.
- Identifiers chosen at random (`host_epoch`, stream ids, deduplication keys) are 32 lowercase
  hexadecimal digits.

A refusal never repeats what was sent: error details are fixed sentences of the host's, so they
can be logged without leaking a command, a file or a secret.

### From the client

A request, which gets exactly one reply:

```json
{"version":1,"request_id":9,"message":{"kind":"cancel","body":{"target":8}}}
```

An acknowledgment, which gets none: every event of that stream up to `seq` was received.

```json
{"version":1,"ack":{"stream":"89abcdef0123456789abcdef01234567","seq":42}}
```

`request_id` starts at 1 and goes up within a connection (gaps allowed), up to 2^53 − 1. A new
connection starts again from 1. `kind` is 1 to 64 characters of `a-z 0-9 . _`, starting with a
letter. `body` is left out when there is none.

### From the host

The reply to a request, either `ok` or `error`:

```json
{"version":1,"reply_to":1,"ok":{"kind":"hello_ok","body":{...}}}
{"version":1,"reply_to":7,"error":{"code":"capability_not_negotiated","retryable":false}}
```

An event of a stream (sequence numbers start at 1 and go up by exactly 1):

```json
{"version":1,"stream":{"id":"89abcdef0123456789abcdef01234567","seq":42},"event":{"kind":"terminal.output","body":{"data":"w6kbWzBtCg=="}}}
```

An error about the whole connection, after which the host closes it:

```json
{"version":1,"error":{"code":"bad_frame","retryable":false,"detail":"request identifier must increase"}}
```

## Negotiation

The first request must be `hello`. Before it, anything else, an acknowledgment included,
closes the connection.

```json
{"version":1,"request_id":1,"message":{"kind":"hello","body":{
  "versions":[1],
  "capabilities":["tree","git","future.optional"],
  "budgets":{"frame_bytes":1048576,"terminal_ring_bytes":2097152,"total_ring_bytes":16777216,
             "stream_window_bytes":2097152,"in_flight":32}}}}
```

- `versions`: those the client speaks. The host answers with the highest it shares, or refuses
  with `unsupported_version` and closes.
- `capabilities`: `pty`, `tree`, `git`, `editor`, `tunnel.reverse`, each negotiated on its own.
  Names the host does not know are ignored, so a newer client can ask for more. The answer lists
  those granted, once each. A client may take tree and git and keep its terminals elsewhere.
- `budgets`: each side's limits; the smaller of each pair applies. `frame_bytes` 1 KiB to 1 MiB;
  `terminal_ring_bytes` ≤ `total_ring_bytes` ≤ 64 MiB; `stream_window_bytes` from `frame_bytes`
  to 16 MiB; `in_flight` 1 to 256. The slow-host default is the one above.

`hello_ok` carries `version`, `capabilities`, `budgets` and `host_epoch`, the identity of this
run of the daemon: another epoch after a reconnect means what the client knew is gone.

## Requests

- Core kinds, always allowed after hello: `hello` (refused: already accepted), `ping` → `pong`,
  `cancel` → `done`, `host.info` → `host_info`: the daemon's `version`, `arch`, `pid` and
  `persistence` (`method`: `user_scope` or `session`; `linger` and `kill_user_processes`: true,
  false or null when unknown; `survives_logout`: `yes`, `no` or `unknown`). Persistence is read
  from the host's policy, never assumed; a client shows `no` and `unknown` as a warning.
- Every other kind belongs to a capability by its first word: `terminal.*` → pty, `tree.*` and
  `root.*` → tree, `file.*` → editor, `git.*` → git, `tunnel.*` → tunnel.reverse. A kind of a
  capability not negotiated is refused with `capability_not_negotiated`; a kind no capability
  knows, or one the host does not implement, with `unsupported_kind`. Neither closes anything.
- At most `in_flight` requests wait for their reply at once; one more is refused with `busy`,
  which is retryable.
- `cancel` names an earlier request still waiting: that request then gets `cancelled` as its one
  reply, and the cancel itself gets `done`. A target already answered or unknown: `not_found`.
- An operation with an effect (creating a terminal) carries a deduplication key chosen by the
  client. Retrying with the same key, even over a new connection, returns the first outcome
  instead of doing it again, for 10 minutes after it finished. While the table of keys is full of
  live entries, a new key is refused with `busy` rather than forgetting one.

## Streams

Each event has a stream id and a sequence number. The client applies an event only if it is the
next one: an older one is a duplicate (a replay overlapping what it saw) and is dropped; a jump
is a gap, never applied, and needs a snapshot or a reset. The host keeps at most
`stream_window_bytes` unacknowledged per stream (one event is always allowed when nothing is in
flight); a slow client only slows its own stream. An acknowledgment for an event never sent
closes the connection; an older one than already seen is ignored.

## Terminals (capability `pty`)

Requests and their replies (`src/pty.rs` has every field):

| request | reply | what it does |
| --- | --- | --- |
| `terminal.create` | `terminal.created` | starts a program on a new terminal; the creator holds the lease |
| `terminal.attach` | `terminal.attached` | starts a stream of the terminal's output for this connection |
| `terminal.detach` | `done` | ends one attachment; the terminal goes on |
| `terminal.input` | `terminal.input_ok` | bytes to type (needs the lease) |
| `terminal.resize` | `done` | rows and columns (needs the lease) |
| `terminal.signal` | `done` | `int`, `term`, `kill`, `hup`, `quit`, `tstp` or `cont` to its process group (needs the lease) |
| `terminal.close` | `done` | hang-up to its process group, a kill after 3 seconds, then forgotten (needs the lease) |
| `terminal.list` | `terminal.listed` | every terminal, the daemon's resident memory and the replay bytes kept, separately |

- A terminal belongs to the daemon, not to a connection: a connection that goes, by a closed
  channel or a killed SSH client, only takes its attachments with it. The program goes on, its
  output is kept, and its exit is kept for the next attachment.
- `create` carries a deduplication key: a retry returns the first terminal. `argv` empty starts
  the user's login shell; `env` is added to a small base (HOME, USER, LOGNAME, SHELL, PATH, LANG,
  LC_*, TZ, and TERM defaulting to `xterm-256color`). At most 64 terminals.
- A terminal's events are numbered (`seq`, from 1): `terminal.output` (a chunk of up to 64 KiB)
  and `terminal.resized` (the new size, between the right bytes, sent to every attachment). An
  attachment names the last `seq` the client applied (`after`). If what follows is still kept,
  the reply says `replay` and those events follow; otherwise it says `snapshot`:
  `terminal.snapshot` events at the barrier `seq` come first, then what follows it. Either way no
  event arrives twice and none is skipped silently. Without `after` (since 0.1.1: a client that
  shows nothing of the terminal yet, such as a desktop that just started), the reply always says
  `snapshot`: the current screen first, then live events. An older daemon refuses that request
  (`bad_request`); it never reads a missing `after` as 0.
- `terminal.exit` comes once per attachment, after every event up to its `seq`: show the end
  when it arrives (the attach reply does not carry it).
- A snapshot redraws the screen without replaying what produced it, so no clipboard write, bell
  or reply to the program happens again. It holds the screen contents, colours, cursor, input
  modes (bracketed paste, mouse, application keys) and title; on the alternate screen, the main
  screen behind it first, then the switch, so leaving the alternate screen shows the right main
  screen; and the modes the screen model does not keep, followed from the output: the kitty
  keyboard flags of each screen (`CSI > u`, so Shift+Enter stays a new line in programs that use
  it), focus reporting and the cursor style. Not in a snapshot: scrollback. A client keeps its own
  scrollback and applies the snapshot to the visible screen.
- Each terminal keeps at most the `terminal_ring_bytes` of the connection that created it; all
  terminals together at most the daemon's `total_ring_bytes`, the largest history giving way
  first. History, never the program, is what is dropped. An attachment whose next event was
  dropped before it read it gets a snapshot with `resync: true` once it has acknowledged what it
  was sent.
- Events of an attachment count against its `stream_window_bytes` until acknowledged, so a slow
  client holds back only its own stream.
- `attach` names the host epoch the client knew; another one is refused with `epoch_mismatch`
  (the daemon restarted, its terminals are gone). An unknown terminal is `not_found`.
- The lease: one holder at a time, needed to type, resize, signal and close. `writer: "take"`
  takes it, and every attachment that held it gets `terminal.lease_lost` and only watches from
  then on. `{"resume": lease}` keeps it across a reconnect; if someone took it meanwhile, the
  attachment is made all the same, watching only, and its reply carries no lease. `"none"` only
  watches.
- Each `input_id` is typed at most once per lease: a retry of one already applied (after a lost
  reply) is answered `duplicate: true` and not typed again; one refused as `busy` (input waiting
  for the program is limited to 1 MiB) is not remembered and can be retried, even after later
  ones went through. The last 1024 applied identifiers are remembered; an older one is refused
  (`bad_request`) rather than guessed. An attachment that holds the lease says how far its
  inputs went (`last_input`, the highest identifier used, 0 for a new lease; since 0.1.1): a
  client that kept the lease across its own restart numbers its inputs above it, and must not
  type with a lease whose `last_input` it was not given.
- `close` hangs up the process group, kills it 3 seconds later if needed, and forgets the
  terminal once every attachment received its last events and its exit (at most 60 seconds after
  the exit, so a stalled watcher cannot keep it). Its history then stops counting against the
  daemon's budget.
- Terminals need a frame budget of at least 96 KiB (a 64 KiB chunk in base64 with its envelope);
  below that, `create` and `attach` are refused with `bad_request`.

## File trees (capability `tree`)

| request | reply | what it does |
| --- | --- | --- |
| `root.open` | `root.opened` | resolves an absolute directory once; the same directory gives the same root |
| `tree.subscribe` | `tree.subscribed` | starts this connection's stream of changes for a root |
| `tree.unsubscribe` | `done` | ends it |
| `tree.expand` | `tree.listing` | lists a directory below the root and keeps it current |
| `tree.collapse` | `done` | stops keeping a directory and those below it current for this connection |
| `tree.stats` | `tree.stats_ok` | directory reads so far, watches, directories cached, the watch limit |

- Paths below a root are opened name by name with `O_NOFOLLOW` from the root's descriptor: a
  symbolic link is listed as `symlink` and never entered, even one swapped in after an
  expansion (`bad_request`). A directory that cannot be read is `permission_denied`, never an
  empty listing.
- An expanded directory is watched before it is read, and the read happens under the lock the
  event reader needs, so nothing that changes in between is lost; queued events are applied
  after it by looking at each name again, which changes nothing the read already saw.
- A listing carries the root's `revision`. `tree.changed` events of that directory with a higher
  revision apply on top (they may arrive before the listing: keep them). A large change comes as
  several events, each with its own revision. Entries carry raw names (base64), a `label` for
  people, `kind`, `size` and `mtime_ms`.
- Expanding a directory again, from any connection, reads the cache, not the disk; `tree.stats`
  counts every directory read so this can be checked.
- Exclusions (`root.opened` lists them: `node_modules`, `.git/objects`, `.cache`,
  `__pycache__`, `.venv`, `target`) are listed with `excluded: true`, and expanding one reads it
  on demand, unwatched. So does a directory beyond the watch limit (a quarter of
  `fs.inotify.max_user_watches`, at most 8192, and the open-file limit) and one too large for a
  listing (`truncated: true`). Such listings say `watched: false`: expanding again reads again.
  Nothing uncovered is ever shown as empty or complete, and nothing polls.
- When the kernel drops events (inotify overflow), every cached directory is read again (at most
  512), its differences are sent, then `tree.resynced`. Directories beyond that are dropped
  with `tree.stale` (`overflow`): expand them again.
- A client whose window is full loses its tree events rather than holding the daemon; once it
  has acknowledged what it was sent it gets `tree.stale` for the whole root (`lagging`) and
  expands again, from the cache.
- Directories stay cached while some connection keeps them expanded; a removed or moved
  directory takes its cache and watch with it.
- File trees need a frame budget of at least 16 KiB (`bad_request` below that).

## Editor (capability `editor`)

Paths are below a root opened with `root.open` (admitted by `tree`, `editor` or `git`).

| request | reply | what it does |
| --- | --- | --- |
| `file.stat` | `file.info` | kind, size, modification time and `version` (never following a link) |
| `file.read` | `file.chunk` | up to 64 KiB from an offset: original bytes, the file's size and version, `eof` |
| `file.write_begin` | `file.upload` | starts a save of `size` bytes against a `base` (version and SHA-256 the client read); no base: a new file |
| `file.write_chunk` | `done` | the next bytes, in order |
| `file.write_commit` | `file.written` | makes the upload the file; its new version and SHA-256 |
| `file.write_abort` | `done` | throws the upload away |
| `file.compare` | `file.compared` | whether the file's content now has a given SHA-256 (the host hashes it) |
| `file.rename` | `done` | renames without replacing: an existing destination is `conflict` |
| `file.remove` | `done` | a file (only if still `version`, when given) or an empty directory (else `conflict`) |
| `file.mkdir` | `done` | a new directory (`conflict` if anything is there) |

- A `version` changes whenever the file does (its identity, size, and both times to the
  nanosecond). Reading is a chunk per request, so a client stops when it likes; every chunk
  carries the version, and a different one means the file changed: read it again.
- Only regular files are read or saved; a symbolic link is never followed and a FIFO is refused
  without waiting (`bad_request`).
- A save is written to a temporary file in the same directory, created exclusively, with the
  space reserved first, so a full disk or a used-up quota answers `no_space` before anything is
  sent where the file system can tell. The file itself is never written in place: a failure at
  any point removes the temporary file and leaves the file as it was, never truncated.
- The base is checked when the save starts and again at the commit: the file must still be the
  version read, or have the same content (a touch is not a change). A last look right before
  the rename catches a change during the commit. Anything else is `conflict`, never a silent
  overwrite. A new file is created with a rename that never replaces (a file created meanwhile
  is `conflict`). The saved file keeps the mode of the one it replaces (new files: 0644); being
  a new inode, it does not keep hard links or extended attributes.
- At most 16 saves at once and 1 GiB reserved in all (`busy`); a file is at most 512 MiB
  (`too_large`). An upload ends with its connection, or after 10 minutes without a chunk; a
  client resends its buffer after reconnecting. Temporary files of a daemon that died are
  removed by the next one, from a journal in its private runtime directory.
- `compare` and a save's content check hash the file on the host, the same authority.

## Git status (capability `git`)

| request | reply | what it does |
| --- | --- | --- |
| `git.open` | `git.opened` | the repository holding a path below a root (a linked worktree is its own); what its own configuration already uses (`core.untrackedCache`, `core.fsmonitor`) and the git version |
| `git.subscribe` | `git.subscribed` | starts this connection's stream of status changes |
| `git.unsubscribe` | `done` | ends it |
| `git.refresh` | `done` | reads the whole status again, soon |
| `git.untracked` | `git.untracked_list` | the files inside an untracked directory shown as one entry |
| `git.stats` | `git.stats_ok` | git commands run, their output bytes, the last full and partial durations, watches |

- The status is git's own (`--porcelain=v2`, `--untracked-files=normal`): each entry carries its
  porcelain record without the path (`record`), with `xy` and `sub` copied out, the path as raw
  names, and for a rename the original path. A wholly untracked directory is one entry
  (`untracked_dir`); its files come from `git.untracked` on demand. Ignored files are not shown.
- The first event of a subscription carries the whole status (`reset: true`); then each
  `git.changed` carries entries added or changed (`upsert`) and paths gone (`removed`), with the
  state after it: the branch, and whether the status is `complete` (a full read finished within
  its budgets, nothing cut, every directory watched) or `stale` (something to read again, or the
  last read failed or was cut). A client that fell behind gets the whole status again (`reset`).
- The cache is kept current from file system events: the worktree's directories (not inside
  `.git`, ignored or untracked directories, which are watched for their own entries only) and
  the git metadata (index, HEAD, refs, packed-refs, info). Changes are grouped for 300 ms (2 s at
  most under continuous activity). A changed file costs one `git status` limited to the paths
  that changed, as literal pathspecs (a path inside a submodule asks about the submodule, a path
  inside an untracked directory about that directory). An index, HEAD, refs or ignore change
  costs one full status; the status is marked stale until it is done.
- One git command at a time on the whole host, in its own process group, at nice 10 and idle I/O
  priority, never taking git's optional locks (it never writes the index). Budgets: 2 s soft
  (reported as `slow`), 5 s hard (the process group is killed), 8 MiB of output, 50,000 entries
  (beyond: `truncated`, never `complete`). A failed, killed or cut read keeps the last good status,
  marks it stale and is retried after 1 s, 2 s, 4 s… up to a minute.
- Nothing runs for a repository nobody subscribed to: no watch, no git command.
- The host never changes the repository's configuration: it uses `core.untrackedCache` and
  `core.fsmonitor` when the repository has them, and turns neither on.

## Errors

| code | meaning | retryable |
| --- | --- | --- |
| `bad_frame` | the frame itself is broken; the connection closes | no |
| `bad_request` | a well-formed frame asking something invalid | no |
| `unsupported_version` | no shared version; the connection closes | no |
| `unsupported_kind` | a kind this host does not handle | no |
| `not_negotiated` | something other than hello came first; the connection closes | no |
| `capability_not_negotiated` | that capability was not granted | no |
| `busy` | too many requests or entries at once | yes |
| `cancelled` | the request was cancelled | no |
| `not_found`, `permission_denied`, `conflict`, `too_large` | as named; `conflict`: someone else changed it first | no |
| `no_space` | no space left on the device, or the quota is used up | no |
| `gap` | the history needed to resume is gone: take a snapshot or start over | no |
| `epoch_mismatch` | the daemon restarted since | no |
| `timeout` | took too long | yes |
| `internal` | the host failed | no |

A client reads a code it does not know as `unknown`.

## Compatibility matrix

| client sends | when | host does |
| --- | --- | --- |
| hello, versions `[1]` | first | `hello_ok`, version 1 |
| hello, versions `[2, 1]` | first | `hello_ok`, version 1 |
| hello, versions `[2]` | first | reply `unsupported_version`, closes |
| a frame with `"version":2` | any time | connection error `unsupported_version`, closes |
| hello with a malformed body | first | reply `bad_request`, closes |
| ping, any request, or an ack | before hello | reply (or connection error) `not_negotiated`, closes |
| `host.info` | after hello | `host_info` |
| hello asking for `future.optional` | first | ignored, not in `hello_ok` |
| hello again | after hello | reply `bad_request` |
| `future.thing` | after hello | reply `unsupported_kind` |
| `git.status` without git granted | after hello | reply `capability_not_negotiated` |
| `tree.subscribe` with tree granted | after hello | handled; one reply later |
| one request more than `in_flight` | after hello | reply `busy` (retryable) |
| cancel of a waiting request | after hello | `done`; the target gets `cancelled` |
| cancel of an answered or unknown request | after hello | reply `not_found` |
| a `request_id` not above the last one | any time | connection error `bad_frame`, closes |
| a fraction or a number beyond ±(2^53 − 1) | any time | connection error `bad_frame`, closes |
| `terminal.create` again with the same key | after hello | the same `terminal.created` |
| `terminal.attach` with another epoch | after hello | reply `epoch_mismatch` |
| `terminal.input` without the lease | after hello | reply `permission_denied` |
| `terminal.input` with an `input_id` already applied | after hello | `duplicate: true`, not typed |
| `terminal.input` retrying one refused as `busy` | after hello | typed (or `busy` again), never `duplicate` |
| `terminal.attach` resuming a lease taken meanwhile | after hello | attached, watching, no lease in the reply |
| `terminal.attach` without `after` | after hello | `snapshot` first, then live (0.1.1; older: `bad_request`) |
| `terminal.attach` holding the lease | after hello | `last_input` in the reply (0.1.1) |
| `terminal.create` or `attach` under a 96 KiB frame budget | after hello | reply `bad_request` |
| `tree.expand` through a symbolic link | after hello | reply `bad_request` |
| `tree.expand` of an unreadable directory | after hello | reply `permission_denied` |
| `tree.expand` of an excluded directory, or beyond the watch limit | after hello | listing with `watched: false` |
| `tree.subscribe` or `expand` under a 16 KiB frame budget | after hello | reply `bad_request` |
| `root.open` with only `editor` or `git` granted | after hello | `root.opened` |
| a file edited in a subscribed repository | after hello | one partial `git status`, then `git.changed` |
| more than 50,000 status entries | after hello | `git.changed` with `truncated: true`, never `complete` |
| `file.write_commit` after the file changed on disk | after hello | reply `conflict`; the file is the other writer's |
| `file.write_begin` or `chunk` on a full disk | after hello | reply `no_space`; no temporary file, the file intact |
