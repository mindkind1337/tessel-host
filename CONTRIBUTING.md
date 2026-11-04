# Contributing

## Languages: Rust, and TypeScript for everything else

- The daemon, the protocol and their release tooling are Rust (`crates/`).
- Anything that is not Rust is TypeScript, strict: tooling scripts, fixture generators, and
  later the protocol client or adapter for the Vue version of Tessel and cross-client fixtures.
- No JavaScript is committed: no `.js`, `.cjs` or `.mjs` file outside `node_modules/`. CI
  refuses one. A script that must run without a build step is a `.mts` file run with
  `node --experimental-strip-types` (or `tsx`).

## Before pushing

```sh
cargo fmt --all --check
cargo clippy --locked --workspace --all-targets -- -D warnings
cargo test --locked --workspace
```

The host crates are Linux programs; run these on Linux (WSL works). Release builds and their
acceptance are described in the README.

## Rules the code keeps

- Nothing a client sends (a command, input, a file, an environment, a credential) is logged or
  echoed in an error: logs hold operation names and numbers, refusals are fixed sentences.
- Every limit is enforced before the work or the allocation it limits.
- Never claim what was not observed: a persistence the host's policy does not grant is reported
  as `no` or `unknown`, a replay that lost history as a gap.
- No test runs against a real user's server; tests use their own home directory.
