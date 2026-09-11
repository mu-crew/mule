# Changelog

## 0.1.1 — 2026-09-11

**`ls` reports how long a job RAN, replacing the AGE column.** Age answered
neither question anyone asks: for a finished job it meant "time since it
finished", for a running one "time since dispatch". Runtime comes from the
`cmd` and `rc` artifact mtimes, so nothing new is written — elapsed for a
running job, total for a finished one, `-` for an orphan whose total is
unknowable. `--json` keeps `age_secs` and adds `runtime_secs`.

**`--quiet` no longer hides warnings.** It was documented as suppressing
next-step hints and also silenced the dispatch warnings, so the clean way to
get a bare job id disabled the safety net at the same time. Hints are
convenience; warnings are correctness. They no longer share a switch.

Note `coop run` puts the id alone on stdout, so `id=$(coop --quiet run '<cmd>')`
captures a clean handle while any warning still reaches your terminal. Piping
coop through `tail -1` to get the id captures a hint instead.

## 0.1.0 — 2026-09-10

First release. `coop` fires jobs at a remote host over its own ssh control
channel, hands back a job id, and holds no connection while the job runs.

Published as `coop-cli` on crates.io. The binary is `coop`.

```sh
cargo install coop-cli
coop host list
```

### Commands

`run` (`--wait`, `--no-tail`, `--cwd`, `--max-secs`), `poll`, `wait`
(`--timeout`), `tail` (`-f`, `-n`, `--all`), `ls` (`--all`, `--full`,
`--json`), `kill` (`--rm`), `rm` (`--all`), `host list`, `host info`. Every job
verb takes `--host`; `--quiet` suppresses next-step hints everywhere.

### What it guarantees

- **Its own `ControlPath`** (`~/.ssh/coop/<host>.sock`). `MaxSessions` is
  per-connection, so coop cannot contend with `git fetch` or `rsync` on the
  default socket, and they cannot starve coop. Measured on a `MaxSessions 1`
  host: five concurrent calls, **1 of 5** succeeded ungated, **5 of 5** through
  coop. With a multi-minute job running under coop, a concurrent plain
  `ssh` still succeeded.
- **Jobs run detached** under a private `tmux -L coop` server, invisible to the
  user's `tmux ls`. Dispatch is one sub-second round trip (**~125ms**, against
  ~33ms for a bare ssh over an existing master); nothing long-running rides the
  channel, which is why `--wait` polls instead of staying attached.
- **A fair ticket lock** covers every ssh except `ssh -O check`, which opens no
  session channel and measured at 0s. `Transport::run` takes the lock as a
  provided method, so a caller cannot forget it.
- **The remote artifact is the only source of truth.** No local index, no
  daemon, no process between invocations. `rc` is the completion signal, and
  job state outlives the tmux server, the connection and a reboot.
- **Commands are base64-encoded, never interpolated.** They cross four
  expansion layers -- the local argv join, the remote shell, tmux's argument
  parse, and the final `sh` -- and stay inert until that last decode. Argument
  boundaries are preserved before encoding, so
  `coop run printf '[%s]' 'a b' c` runs as `[a b][c]`.

### Exit codes

| code | meaning |
| --- | --- |
| `0` | the operation or job succeeded |
| `<n>` | the job's own code, from `wait` / `--wait` |
| `3` | no ssh control master — **needs a human**, coop never opens one |
| `4` | your wait timed out; the job is still running |
| `5` | the job is orphaned; no `rc` will ever arrive |
| `6` | the connection dropped while waiting; the job continues |

Exit 3 is a handback, not a transient error: `ssh -MNf` can need a hardware
token and cannot prompt from a background call, so coop prints the command and
stops. 4 and 6 mean wait again; 5 means never.

### Safety rails

- **Dispatch warnings** for two patterns that hide failure, both observed in
  real use: a final `head`/`tail` pipeline (measured:
  `sh -c 'echo x; exit 1' | tail -1` exits **0**, so a failed build reports
  success and any `&&` proceeds) and an unbounded loop with no runtime cap.
  Each warning names the job and how to end it. Suppressed by
  `set -o pipefail` and `--max-secs` respectively — coop never injects either,
  because the command belongs to the caller, and never blocks on a heuristic.
- **`--max-secs` / `max_job_secs`** kill a runaway job remotely and record rc
  **124**, distinct from `kill`'s 137. Built on a tmux watchdog rather than
  `timeout(1)`, which is absent from a stock macOS.
- **`max_log_bytes`** caps a log at 100MB (a verbose build was measured at 35MB
  in 5s) and a truncated log says so. A one-shot `tail` reads the last 64KB.
- **Classified ssh failures.** A refused channel surfaces as
  `Permission denied (keyboard-interactive)`, which reads as a credentials
  problem. coop distinguishes a busy channel from an unreachable ssh agent and
  from an agent with no keys, names the discriminator, and warns that the
  prompt may be waiting where you cannot see it. Reporting ambiguity as
  certainty sent one debugging session an hour in the wrong direction.
- **Validated inputs at the boundary.** Job ids are six lowercase hex digits;
  host names and `tmux_socket` must be filename components, so a quoted TOML
  key cannot place a socket or a lock outside the directory coop owns.

### Tested

132 tests. Three unattended layers run in `cargo test`, each skipping with a
printed reason when its binary is absent: pure logic over a `Transport` trait,
the real wrapper against a private `tmux` server, and a local non-root `sshd`
with `MaxSessions 1` on loopback that re-runs the measurements the design rests
on — channel refusal, isolation in both directions, concurrent gated dispatch,
artifact durability across a destroyed tmux server, and exit 3 on every verb.
A fourth layer, a real capped host, is operator-driven and holds only what a
local sshd cannot show: the genuine 2FA refusal string and real latency.
