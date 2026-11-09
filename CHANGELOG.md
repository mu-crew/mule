# Changelog

## Unreleased

The surface mu's mule plugin needs (mu plugins spec, D15): a status a caller
can trust after its own crash, a caller key with dispatch dedupe, a listing by
key, and a cancel that stays inside the caller's process group.

### Added

- `mule run --key K`: an idempotency key, stored as the job's `key` file
  before the job starts. A dispatch whose key exists starts nothing and prints
  that job's id (stderr says so, even under `--quiet`), whatever its state.
  The id stays generated. The check and the start are one locked round trip,
  so five concurrent dispatches of one key over a `MaxSessions 1` connection
  make one job. A key file that cannot be read refuses the dispatch; two jobs
  under one key (two clients) are an error naming both. With `--lease`, a key
  that names a job is held as `--attach` holds one, in any state, and
  `--job-file` names it; a key taken between the lookup and the dispatch exits
  1 with nothing dispatched. Keys are 1-128 characters of `A-Z a-z 0-9 . _ -`.
- `mule ls --key-prefix P`: only keyed jobs whose key starts with `P`, at any
  age. `ls --json` rows carry `key` for a keyed job; an unkeyed row has no
  `key` field and is byte-identical to before.
- `mule exec --no-detach`: on a signal, one `rm` from exec itself before the
  exit, never a detached child, so a caller that kills exec's process group
  after a grace leaves nothing running outside it. The stop is not retried and
  can outlast the grace.

### Changed

- Release binaries are built with fat LTO, one codegen unit and stripped
  symbols, about a third smaller (macOS arm64: 2.10 -> 1.33 MB). A panic
  backtrace from a release binary no longer names functions.
- **`poll --json` answers five states and exits 0 for all of them**:
  `running`, `done`, `orphan`, `missing` and `unknown` (with `reason`). It was
  exit 1 for a missing job, the same code as a refused session. Exit 3 (no
  master) is unchanged, and so is plain `poll`. `log_size` is null for
  `missing` and `unknown`.
- **The probe proves absence instead of inferring it**, as `rm` does. `[ -d ]`
  is false behind an unsearchable parent and `tmux has-session` exits 1 for a
  protocol mismatch, so a live job could read `missing` or `orphan`. The
  directory is now checked with `ls -d` and the sessions with `list-sessions`
  whenever the answer rests on an absence; `missing` also needs `watch-<id>`
  gone, and a state dir removed under a live session is not `missing`. A check
  that cannot tell is `unknown` for `poll --json` and the probe error it always
  was everywhere else (exit 1, `wait`'s lost contact, a lease that holds on).
- `ls` fails (exit 1) when its jobs root or the tmux session list cannot be
  checked, instead of listing no jobs or calling every running job an orphan.

### Fixed

- **Remote snippets fail closed, by one rule and one prelude** (SPEC § Remote
  snippets fail closed). The probe, `ls`, prune, `rm`'s proof, the key lookup
  and the claim now start with shared shell helpers that check every external
  command's status and output, refuse symlinked keys and `rc`, and answer an
  absence only on proof. `tests/fault_injection.rs` fails every external
  command of every one of those snippets in four modes and holds each verb
  to it. Round 2 of the review, all red in that harness before: a failed
  `test -e key` let prune delete a keyed job (and the key then ran again); a
  symlinked unreadable key was skipped by the lookup and the listing; `ls
  --running --key-prefix` dropped the unreadable-key row before the guard; a
  `cat` that printed `1` then failed was taken as rc 1.
- Review of the plugin surface (each with a regression test in
  `tests/regressions_p7a.rs`):
  - A failing tool is never a proven absence. `poll --json` read a `grep`
    error (exit 2) as "session not listed" and answered `missing` while
    `watch-<id>` lived; `rm` "proved" the same stop; prune could delete a
    running job's state. All now treat it as cannot-tell (`unknown`, exit 1,
    or spare the directory). An `rc` that exists but will not read is
    `unknown`, not an orphan, and every `rc` writer renames into place.
  - `ls --key-prefix` with a `key` file that will not read exits 1 naming the
    job, instead of an empty answer; plain `ls` shows that row as `unknown`,
    and a keyed dispatch refuses. A `find` that fails fails the listing.
  - Keys compare as strings: awk compared `01` and `1` as numbers, so the
    second caller's command never ran.
  - A keyed lease claims its key before writing its job file, so two racing
    leases both hold the one job and name it; the loser used to exit 1 with
    a job file naming an id that never ran. A claim not yet started polls
    `unknown`.
  - Prune never removes a keyed job: a finished keyed job past `keep_days`
    was pruned by the next dispatch, and its key ran the effect again.
- A job whose session was torn down from outside could still write its rc:
  129 over `mule kill`'s 137, 129 in place of an orphan, or an empty `rc`
  that made `kill` fail with "invalid exit code". The hangup does not kill a
  waiting bash or zsh at once, and under vtmux (which signals the pane's group
  itself) the wrapper's `echo $? > rc` usually won the race. The wrapper now
  records `rc` only if none exists and its session still does, through a temp
  file and a rename.
- `mule ls` on a macOS host with two or more live jobs failed with `awk:
  newline in string`: BSD awk refuses a `-v` value holding a newline. The
  session list now reaches awk through `ENVIRON`.

## 0.4.0 — 2026-11-05

### Added

- `mule rm <id> --retry-for SECS [--fence-gen N] [--failed-file F]`: a stop
  that waits for a missing master (a lock-free `-O check` every 5s, never
  opening one) and retries a failed `rm` after 5, 10, 20, then every 30s, until
  a soft monotonic deadline (checked after every sleep: no attempt starts
  past it; one in flight is not cut short). On giving up it writes one atomic 0600 line to `F`
  (`<id> on <host> not stopped: <reason> (gave up after <n> attempts, <s>s; at
  unix <t>)`, at most 512 bytes) and exits 3 (never saw a master) or 1. On
  success it removes `F`. `--fence-gen` makes it stand down (exit 0, nothing
  removed or recorded) once the job's local stop fence has moved, i.e. the job
  was reattached. Plain `rm` and `rm --all` are unchanged.
- `mule run --lease --attach <id> --job-file F [--forward-unix ...]`: a new
  carrier for a job that is still running after its lease's pane died. In the
  2026-11-04 aiv-blog incident six lease panes were hung up, mu reaped their
  agents, and the jobs on the host with a down master ran on with nothing able
  to put a pane back on them; mu's `agent reattach` runs this. It exits 3 with
  no master and 1 if the job is not running or `F` names another job (or is
  empty or unreadable), before holding or changing anything. `F` is written
  only if missing (`link`, never a replacing rename) and kept if it names the
  job. It waits out a stop attempt in flight, bumps the job's stop fence so an
  old carrier's retrying stop stands down, owns the new generation for its own
  signal stop, then holds exactly like a lease. Nothing is dispatched.

### Changed

- **A lease's signal-path stop retries for up to 10 minutes, fenced against
  attach, and records its failure.** In the 2026-11-04 aiv-blog incident a
  tmux restart hung up six lease panes. On the host whose master was down,
  each detached `rm` failed once into `/dev/null`, and the jobs kept running
  with nobody told. The stop child now passes `--retry-for 600 --fence-gen G
  --failed-file <job_file>.<id>.stop-failed`, where `G` is the fence
  generation the lease read before its dispatch (an unreadable fence exits 1
  with nothing launched), never one read when signalled; the path names the job, so an
  old child's late write or delete never touches a newer job's record. `exec`'s
  signal-path stop retries too (no fence, no record: it has no job file).

## 0.3.0 — 2026-10-29

**A mule killed mid-write no longer wedges the host.** The ticket lock wrote its
files by truncate-then-write, so a SIGKILL between the two left an empty
`holder`, `waiter.N`, `next` or `serving`, and every later mule call on that
host failed (`invalid pid ... cannot parse integer from empty string`) until
someone deleted the file. Lock files are now written to a temp file and renamed
inside the counter lock (no fsync: a reboot leaves every pid dead anyway). An
empty or unparsable pid file counts as a dead process, and a torn counter is
rebuilt from the waiter files rather than read as 0, so hosts an older mule
already wedged recover on the next call. A run of abandoned tickets is stepped
over in one pass rather than one per 5ms poll.

**A refused session is waited out, and never run twice.** After a mule process
died mid-call, its ssh could keep the one session after its ticket was freed,
and the next caller was refused: under three parallel layer-3 suites a `mule rm`
right after a TERMed lease exited 1 (3 of 15 runs), so mu kept a job it meant to
stop. Now a call the master refused before its script started -- proven from
ssh's own `-E` log, not the remote's stderr, with nothing on stdout and the
master still answering -- releases its ticket, backs off, and re-queues at the
back, for up to 12s, then reports the busy error as before (exit 1). Later
callers may finish first. Waiting for a ticket stays unbounded; a caller that
needs a wall-clock bound imposes it, as mu does.

Every mule ssh passes `ProxyCommand=/usr/bin/false` instead of `none`, so a
refused or masterless call can never fall back to a direct connection and run
the command outside the master. `ssh -O check` is killed after 2s, so a stopped
master reads as down instead of hanging every verb. `mule exec` now waits for
its in-flight probe on a signal, as the lease does, so its detached `rm` is not
refused.

**`mule run --lease --job-file F [--forward-unix LOCAL:REMOTE ...] -- <cmd>`**
holds a TUI job for a local pane's life, for mu's lease transport. It writes the
job id to `F` (atomic, 0600, one line) *before* dispatching, so the job can
always be stopped with `mule rm <id>`; forwards each socket and re-forwards on
drift (LOCAL removed or replaced, master replaced); exits with the job's code
(1 missing, 5 orphan) after printing its final screen and unforwarding. A lost
master or a dropped probe prints a hand-back line once and keeps holding: exit 3
only at startup. SIGHUP/SIGINT/SIGTERM stop the job through the detached `mule
rm` and exit 129/130/143. The pane shows a status line (job, host, state,
forwards, `murmur pick --all  # select mule-<id>`, the stop command) and mirrors
the remote screen for 30s after the first forward, then on state changes.

`F`'s temp file is created exclusively (`O_EXCL|O_NOFOLLOW`) under a random
name and `fchmod`ed to 0600, so a symlink or a loose-mode file planted beside a
`/tmp` job file can neither be written through nor weaken `F`'s mode. Every
tick makes one locked call, the last included: a state-only probe that sees the
job done leaves the final screen to the next tick's probe, and the code it saw
stands even if that capture fails.

**The probe reads session liveness before `rc`.** Every path that ends a job
writes `rc` before its session goes, so this order cannot see "no session, no
rc" for a job that finished between the two reads. The old order could, and
reported a done job as an orphan: under three parallel layer-3 suites a lease
whose job exited 7 exited 5 instead, 3 runs in 3. `poll`, `wait`, `tail` and
`exec` share the probe and get the fix.

On a signal, a lease starts no further locked call and waits up to 3s for the
one in flight (its dispatch or a probe) before it exits. Killing a local ssh leaves the remote script holding
the one session while the ticket goes with the lease, so the detached stop (or
any next `mule` call) was refused. The signal-path `unforward` bound is 3s, up
from 1s, which a loaded machine overran.

The poll probe gains a screen mode that carries `capture-pane` in the same
locked call, so mirroring costs no extra round trip.

**`mule rm <id>` exit 0 now proves the job is stopped and gone.** It used to
exit 0 even when `kill-session` did not take or `rm -rf` failed. Now it verifies
that neither `mule-<id>` nor `watch-<id>` exists and the state directory is
gone; otherwise it exits 1 naming which and keeps what is left. A missing job
still exits 0. `rm --all` is unchanged.

Exit 0 needs *seen* absence, not a failed probe: a tmux that cannot be queried
(version mismatch, unsafe socket directory) or a state path behind an
unsearchable directory exits 1 with the tmux or `ls` error, and keeps the state.
Only `no server running`, a missing socket, or `No such file or directory`
count as gone.

**`mule exec [--host H] [--cwd D] [--max-secs S] -- <cmd>`** is `ssh host cmd`
over mule's private connection, for a caller that needs a synchronous answer and
must not hold the default master. It runs a detached job, polls at 100ms
settling at 1s, prints the command's stdout to stdout and its stderr to stderr
(its own wrapper mode, unlike `run`'s merged log), exits with the command's
code and removes the job. No id line, no hints, and no mule warnings on stderr:
only the command's stderr and fatal errors. SIGHUP/SIGINT/SIGTERM stop the job
through a detached `mule rm` and exit 129/130/143.

**New exit code 7** (`exec` only): the output is partial -- it hit
`max_log_bytes`, or a process the command left behind (`sleep 30 &`) still holds
its stdout, so the log cannot be known complete. exec no longer returns the
job's code beside a possibly-empty log in that case.

`exec` removal failure is fatal: the collect step now runs `rm <id>`'s proof
(sessions and state directory seen gone) and, if it cannot, exits 1 naming the
leftover instead of returning the command's code. The 5s wait for a finished
job's session to end is wall-clock (probe time counts), not summed pauses.
`mule --help` and `mule exec --help` describe exit 7 as "output incomplete:
truncated at max_log_bytes, or a process still holds the job's stdout".

No locked call in `exec` waits: the wait for the job's session to end moved out
of the collect round trip (which held the ticket ~14s) into the unlocked poll
loop. A stop signal that arrives with a probe in flight now always gives exit
129/130/143, never exit 1 from the probe the signal killed.

The detached cleanup (`cleanup.rs`) runs `current_exe()` with the effective
`--config`, so a private config is never swapped for the default one, and
detaches with `setsid()` through `libc` (macOS has no `setsid` executable). The
signal handler only sets a flag and writes a self-pipe (`signal-hook`).
`main` no longer panics when stderr is a dead tty.

**`mule forward` / `mule unforward`** carry a unix control socket over mule's
master with `--unix LOCAL:REMOTE`. A forward is a channel, not a session, so it
works while the one `MaxSessions` slot is held. `forward` verifies with
`connect(LOCAL)` instead of trusting `ssh -O forward`'s exit code, remembers
each forward so changing one LOCAL's REMOTE cannot wedge the master's forward
table, and exits **3** with `ssh -O exit` and the master command if the table is
wedged anyway. Unix sockets only; TCP stays out.

Changes to one LOCAL are serialised by an exclusive per-(host, LOCAL) file lock,
so two concurrent `forward`s of it to different REMOTEs can no longer wedge the
master. `unforward` exits 1, naming the path and keeping the record, when it
cannot remove LOCAL or the record; a missing socket, record or master is still
success. `unforward LOCAL:R2` while the record names R1 now also cancels R1, so
the forward is not orphaned in the master's table for the next `forward` to
wedge.

`Transport::mux_op` is the new lock-free path (`-O check`, `-O forward`,
`-O cancel` for local unix forwards), as a closed enum so the exemption cannot
widen by accident. `master_alive` is now `mux_op(Check)`.

The printed master command now includes `-o StreamLocalBindUnlink=yes`, so a
master reopened from it rebinds over a stale socket file.

## 0.2.1 — 2026-09-27

First release on npm: `npm i -g @mu-crew/mule` installs a prebuilt binary for
Linux x64, Linux arm64 and macOS arm64. `cargo install mule-cli` still works.

**Jobs over ~12KB of script dispatch again.** The job's script was inlined
into the tmux `new-session` command, which tmux rejects past ~16KB with
"command too long". The job now reads its script from the `cmd` file written
before tmux starts. Measured with a 54KB script.

## 0.2.0 — 2026-09-27

**Breaking:** `coop` is now `mule`. Before upgrading, stop all running jobs,
then move the config with `mv ~/.config/coop ~/.config/mule`. Reopen the SSH
control master on the new `~/.ssh/mule/<host>.sock` path before dispatching new
jobs. Cut over only when no jobs are running because the remote state and
private tmux server also move to the `mule` name.

## 0.1.2 — 2026-09-16

`coop run --tui` keeps the command attached to its private tmux pane PTY while
`pipe-pane` writes a capped durable transcript. A start barrier prevents early
output loss; TUI jobs record their mode and final visible screen, and retain the
existing timeout, kill, runtime, and cleanup lifecycle. Plain `tail` now reads
the live or saved TUI screen; `tail --transcript` explicitly reads raw terminal
bytes, and `tail -f` points at the screen and murmur picker instead of streaming
redraws. TUI dispatch prints pasteable screen, picker, mu-control, and cleanup
commands with shell-quoted target and forwarded workstream values.

Jobs identify as managed agents by default. The final command shell receives
`MU_MANAGED_AGENT=1`, `MU_AGENT_NAME=coop-<job-id>`, and the caller's non-empty
`MU_WORKSTREAM`. Forwarded workstream values are encoded before they enter the
remote wrapper. `coop run --human` omits all three variables.

`coop ls --running` shows only live remote work. Removed jobs now report
`not found` with exit 1 instead of being misclassified as orphans.

TUI jobs enable tmux extended keys with CSI-u before startup, matching pi's
keyboard protocol. Ordinary jobs keep tmux defaults.

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
