# AGENTS.md — coop

`coop` fires jobs at a remote host over a private ssh control channel nothing
else can contend with, and hands back a job id you poll, wait or tail against.
The caller never holds an ssh connection.

Read [`SPEC.md`](SPEC.md) before changing behaviour: it carries the design,
the measurements behind it, and why the alternatives were rejected.

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

`cargo test` must pass **without a remote host or internet**. Layers 1–3 are
the default suite precisely so this holds on a plane. Layer 3 uses a local
`sshd` on loopback when available and skips otherwise.

## If the master is down, stop and ask

`coop` verbs exit **3** when there is no ssh control master. Opening one can
require a human to touch a hardware key, and `ssh -MNf` cannot prompt without a
terminal — so this is the one failure no program resolves on its own.

Working on this repo, you will hit it while testing against a real host. Ask the
operator to run the command coop prints. Do not retry it, do not call
`ssh -MNf` from a tool call, and do not fall back to `ssh <host> <cmd>` "just to
check something" — that holds the capped channel for the whole command and is
the exact failure this tool exists to remove.

For automated tests, use the local `sshd` recipe instead (test layer 3, below).
It needs no master and no token.

## Testing layers

Four evidence layers, because every invariant this design rests on was measured
against a real capped host and none is reachable from a plain unit test.

| layer | what | needs |
| --- | --- | --- |
| 1 | pure logic over the `Transport` trait with `Fake` | nothing |
| 2 | the real wrapper against `tmux -L coop-test-<pid> -f /dev/null` | `tmux` |
| 3 | a local non-root `sshd` with `MaxSessions 1` on a loopback port | `sshd` |
| 3b | a real capped host | operator-chosen host + a live coop master |

Layer 3 lives in `tests/local_sshd.rs`, on the fixture in `tests/common/sshd.rs`.
It re-runs the measurements the design rests on — channel refusal, isolation in
both directions, five concurrent gated dispatches, detached dispatch latency,
artifact durability across a destroyed tmux server, private-server invisibility,
and exit 3 on every verb — in about 3s, with no token and no remote host.

Anything that builds a remote script belongs here rather than behind `Fake`. A
`Fake` returns canned output regardless of the script it was handed, so it
cannot see a wrong script; every bug of that class so far was found by driving a
real sshd.

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
   with tools on the default socket, and they cannot starve it.
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

  This is enforced by the type, not by discipline: `Transport::run` is a
  provided method that takes the lock, and implementations supply only
  `run_unlocked`. Never call `run_unlocked` outside `src/transport.rs` -- doing
  so opens a session channel the fairness gate cannot see.
- **User commands are base64-encoded**, never interpolated. They cross three
  expansion layers (ssh's shell, tmux's argument, `sh -c`) plus coop's appended
  redirect.
- **Jobs get a non-login, non-interactive shell.** Do not add a flag to source
  login files: that would make a job depend on the host's config, and the
  failure mode is "works when I ssh in, fails under coop". Measured and
  rejected — see SPEC.md § Sourcing login files is out of scope. Note bash does
  source `~/.bashrc` over ssh, so a `PATH` set there already reaches jobs; what
  is missing is only `.bash_profile`, i.e. an environment manager's `activate`,
  and shims cover that.
- **The remote artifact is the only source of truth.** No local job index. `rc`
  is the completion signal; poll the artifact, not the process.
- **Never pipe a job's command into `head` or `tail`.** Observed from a real
  agent: `coop run 'lake build 2>&1 | tail -3 && ./check'`. `rc` becomes the
  pipe's -- measured, `sh -c 'echo x; exit 1' | tail -3` exits 0 -- so a failed
  build reports success, and the `&&` then runs the next stage against a broken
  tree. The instinct is right (logs get big) and coop already serves it better:
  `coop tail <id> -n 3` shapes the output, `max_log_bytes` caps the write, and a
  one-shot `tail` caps the read at 64KB. `set -o pipefail` inside your own
  command is the escape hatch if the remote `sh` supports it; coop does not
  inject it, because the command belongs to the caller.
- **coop never opens the ssh master.** `ssh -MNf` needs a TTY for a hardware
  token and cannot prompt from a background call. Exit 3 and print the command.
- **No daemon, and no process between invocations.** This is what makes coop
  honest rather than an orchestrator.
- **Never pass ssh's stderr through raw.** A refused session channel surfaces as
  `Permission denied (keyboard-interactive)`, which reads as a credentials
  problem and sends you to the wrong place. Classify it.

## Style

Use Rust edition 2024, `clap` derive, `anyhow` at the boundary with
`thiserror` for errors callers switch on, and `serde` with `toml` for config.

Comments explain **why**, not what. The what is recoverable from the code; the
why is not. Where a decision was measured, put the number in the comment — that
is what stops someone "simplifying" it back.
