# AGENTS.md — coop

`coop` fires jobs at a remote host over a private ssh control channel nothing
else can contend with, and hands back a job id you poll, wait or tail against.
The caller never holds an ssh connection.

Read [`SPEC.md`](SPEC.md) before changing behaviour: it carries the design, the
measurements behind it, and why the alternatives were rejected. The plan lives
in the `coop` mu workstream (`mu state -w coop`).

---

## The commit gate

**Every commit must be clean on all three. No exceptions.**

```sh
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
```

One line if you want it:

```sh
cargo fmt --check && cargo clippy --all-targets -- -D warnings && cargo test
```

Rules:

- Run the gate **before** every `git commit`, not at the end of a branch. A
  commit that fails it is a commit someone else has to bisect through.
- `cargo fmt --check`, not `cargo fmt`, in the gate — formatting your own diff
  is fine, but the gate must *verify* rather than mutate, or it always passes.
- `-D warnings` is not negotiable. A warning you meant to allow gets an
  `#[allow(...)]` with a comment saying why, so the next reader sees a decision
  instead of rot.
- `--all-targets` so clippy sees tests too. Test code is code.
- No `#[ignore]` to get a commit through. A test that cannot pass yet belongs in
  the task that makes it pass, red first (see TDD below).
- Never commit with `--no-verify`.

`cargo test` must pass **with no network and no ssh**. Test layers 1 and 2 (see
below) are the default suite precisely so this holds on a plane.

## Testing layers

Three layers, because every invariant this design rests on was measured against
a real capped host and none is reachable from a plain unit test.

| layer | what | needs |
| --- | --- | --- |
| 1 | pure logic over the `Transport` trait with `Fake` | nothing |
| 2 | the real wrapper against `tmux -L coop-test-<pid> -f /dev/null` | `tmux` |
| 3 | a local non-root `sshd` with `MaxSessions 1` on a loopback port | `sshd` |
| 3b | a real capped host | `$COOP_TEST_HOST` + a live master |

Layers 1–3 are unattended and part of `cargo test`; each skips with a printed
note if its binary is missing. Only 3b needs a human (one hardware-token tap to
open the master), and it holds only what a local sshd cannot show: the real
`Permission denied (keyboard-interactive)` string from 2FA, and real network
latency.

Layer 2 always uses a private tmux socket and `-f /dev/null` — never the
developer's server, never their `~/.tmux.conf`. Clean up: ask tmux for
`#{socket_path}` rather than reconstructing it.

## TDD

Red, green, refactor. The "run it and watch it fail" step is not ceremony: a
test that has never failed has never been shown to test anything. Commit one
task's worth at a time, atomic and revertible.

## Invariants — do not break these

These are the tool. Breaking one silently makes coop worse than plain `ssh`.

1. **coop uses its own `ControlPath`** (`~/.ssh/coop/<host>.sock`). The
   `MaxSessions` cap is per-connection, not per-user, so coop cannot contend
   with `git fetch`, rsync or murmur on the default socket — and they cannot
   starve it.
2. **Jobs run under a private tmux server** (`tmux -L coop`), invisible to the
   user's `tmux ls`.
3. **Nothing long-running ever rides the channel.** Every ssh coop issues is a
   sub-second detached dispatch or an artifact read. This is why `--wait` polls
   instead of staying attached, and why coop's channel is never lent to local
   commands like rsync.

Consequences, each of which has cost someone real debugging time:

- **The lock covers every ssh except `ssh -O check`.** Two concurrent `poll`s
  hit the same cap that motivated the tool. `-O check` is exempt because it was
  measured at 0s and opens no session channel.
- **User commands are base64-encoded**, never interpolated. They cross three
  expansion layers (ssh's shell, tmux's argument, `sh -c`) plus coop's appended
  redirect.
- **Jobs get a non-login, non-interactive shell.** No sourcing remote dotfiles:
  that would make every job depend on the host's config, and the failure mode is
  "works when I ssh in, fails under coop".
- **The remote artifact is the only source of truth.** No local job index. `rc`
  is the completion signal; poll the artifact, not the process.
- **coop never opens the ssh master.** `ssh -MNf` needs a TTY for a hardware
  token and cannot prompt from a background call. Exit 3 and print the command.
- **No daemon, and no process between invocations.** This is what makes coop
  honest rather than an orchestrator.
- **Never pass ssh's stderr through raw.** A refused session channel surfaces as
  `Permission denied (keyboard-interactive)`, which reads as a credentials
  problem and sends you to the wrong place. Classify it.

## Style

Follow `~/hacking/tuicr`: Rust edition 2024, `clap` derive, `anyhow` at the
boundary with `thiserror` for errors callers switch on, `serde` + `toml` for
config.

Comments explain **why**, not what. The what is recoverable from the code; the
why is not. Where a decision was measured, put the number in the comment — that
is what stops someone "simplifying" it back.
