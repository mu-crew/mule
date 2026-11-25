# Changelog

## Unreleased

### Fixed

- Pi extension: a first turn that raced workspace startup no longer gets
  "Still starting" baked into its system prompt; `before_agent_start` now
  waits (up to 30s) for the in-flight startup and reports unavailable only on
  a real failure. If startup outlives that wait, the prompt says the
  workspace is still connecting (not unavailable) and remote tools await the
  same in-flight attempt.

## [0.7.2] — 2026-11-25

### Fixed

- `mule` retries a job's `tmux new-session` up to 4 times when it hits a
  `tmux` server exiting with its last session (`server exited unexpectedly`),
  fixing intermittent failures under parallel Pi tool calls.
- Pi extension: enforce bash job deadlines without `timeout(1)` on the host,
  preserve trailing newlines in `find` git-ancestor walks, and mark remote
  commands with `MU_REMOTE_WORKSPACE=1` (mu Spec B D1.2c).
- `mule ls` encodes command listings under a bytewise `awk` locale; `mule run`
  derives swallowed-flag warnings from Clap and no longer warns on a command's
  own `--help`; `probe` rejects malformed flags instead of inferring state.

### Changed

- Updated README and package descriptions to cover `mule serve`, the mu plugin,
  and the Pi extension.
- Internal refactors (`libc::flock`, shared wrapper/CLI/dispatch helpers,
  `serve/script.rs` split; −324 net lines) and event-based test oracles + Pi CI
  release gating.

## [0.7.1] — 2026-11-24

0.7.0 was tagged but never published (its release stopped at the macOS
gate); its notes below ship with 0.7.1.

### Fixed

- `mule serve` no longer needs `timeout(1)` on the host. Stock macOS before
  27 has none, so every op against such a host failed `host_tool_missing`
  (CI's macOS 26 runner: sshd's `PATH` has no `timeout`). The payload read
  and the delivery are bounded by the batch script itself, as before (TERM
  at 5s, KILL 1s later), and a delivery's leftovers die with it.

## [0.7.0] — 2026-11-24

### Added

- `mule serve --stdio`: bounded synchronous file ops (read, stat, write,
  cancel; no arbitrary command) for one caller over JSON lines, one
  ticket-locked round trip per batch, ~0.35s p50 per op against ~1.5s for
  `exec`. The batch runs under `/bin/sh` whatever the login shell (zsh
  aborted it). Every op runs in its own session (a process group without
  `setsid`) under a 1s remote deadline that kills it whole, children of its
  host tools in groups of their own (GNU `timeout`'s) included, and
  whatever an op leaves behind dies when that op ends, not when the batch
  does; payload, delivery and output (per op and per batch, on disk) are bounded on the
  host, with a local hard kill behind them that outlives serve itself. A
  cancel that loses the race to an op's end reports the real result. `write`
  takes an absolute path and replaces exactly that file, keeping its mode
  from the moment the new bytes are visible. A host without
  `timeout`, `base64`, `mktemp`, `sleep`, process groups, or a way to find
  an op's processes (`pkill -s`, or `ps` and `awk`) fails every op closed.
  A child of its caller, not a daemon: it exits when stdin closes, and 3 when the master
  goes. SPEC's invariant 3 gains this as a third class of ssh use; longer
  work stays a detached job. Cargo feature `serve`, on by default.
- `Transport::run_with_stdin`: the ticket-locked, refusal-retrying stdin path
  `serve` rides, so its lock is the transport's like every other verb's.
- **Pi remote workspace.** `@mu-crew/mule` is also a Pi package. With
  `--mule host[:/root]` (or a `"mule"` key in a trusted project's
  `.pi/settings.json`) the extension sends `read`, `write` and `edit` to the
  host over `mule serve --stdio`, and `bash`, `grep`, `find`, `ls` and `!`
  to detached mule jobs (a command's exit 3 is its own; mule's 3 is only a
  hand-back). The remote `AGENTS.md` goes
  into the system prompt and the status line shows `host:root`. `mu`,
  `mu_delegate`, skills and the loop's own files stay local. The extension
  uses the binary from its own package, never one on `PATH`. Off by default.
- **Pi workspace policy.** Esc on a running remote command stops it with
  `mule kill`; a cancelled file op is cancelled alone. Each detached tool job
  is keyed by session and tool call, so a replayed call reattaches instead of
  running twice; `MULE_PI_JOB_PREFIX` prefixes every key so mu can find a
  worker's jobs with `mule ls --key-prefix`. mule's exit 3 stops the turn
  and tells the user once (`HANDBACK`), without retrying or killing jobs
  through the missing master; the next call reconnects. `--mule-role
  read-only` (or `MULE_PI_ROLE`) hides `write` and `edit` and runs only
  known read-only bash (a guard against accidents, not a sandbox). Heavy bash queues on the host's `muzzle` when it
  has one (`MULE_PI_MUZZLE` to pin or turn it off), with the Pi timeout
  counted from admission. Files are capped at 4 MiB; a log cut at `max_log_bytes` or a tail clipped at
  64 KiB is a note after the output; a command's own 3 or 7 is its result.
  Reopening Pi reattaches to the jobs of `bash` calls left without a
  result (found with `mule lookup`, never restarted) and reports their
  outcome to the model; a watch the master drops resumes on the next
  workspace call that succeeds, never on a timer.
- `poll --json` reports `drained` for a `done` job: whether its log
  pipeline has finalized, so the log and its `truncated` marker are final.
  `rc` lands first when a process the command left behind still holds
  stdout. It is the pipeline's own `drained` marker, not the session's
  end, so a `mule kill` that cuts the pipeline short stays `drained: false`.

## [0.6.0] — 2026-11-23

### Added

- **Use mule as a mu link.** `mu-plugin-mule` runs commands through a mule SSH
  master with `exec --no-detach --strict-codes`. The link manages its own slots,
  supports cancellation and does not fetch files. `mule init` seeds a config,
  prints master commands without opening them and registers the companion
  plugin when mu is on PATH. Rerun init after an upgrade to refresh the
  registration if its version or schema changed.
- **Lease jobs through the mule link.** `mu-plugin-mule` supplies a bound
  lease job kind: a file handle, exact keyed recovery, status, proven stop,
  and attach with mu's built-in ctl forwarding. The view and its cleanup use
  the host resolved at handshake.
- **`--strict-codes`** on `exec`, `wait` and `run --wait` reserves exit 3 for a
  hand-back (no master). A command's own 3 becomes 8; a command can also
  return 8 itself, so the remap notice is diagnostic only. Default exit
  codes are unchanged.

### Changed

- Local helpers such as ssh and ps are resolved once per mule process to
  avoid repeated PATH probes. The Rust build requires 1.88, matching the
  language features already in use.
- The job-view hint points to the agents view in tsesh (Ctrl-g).

### Fixed

- npm platform packages and GitHub release archives bundle `mu-plugin-mule`
  beside `mule` so `mule init` can register it. Release builds remap local
  paths and check both binaries for embedded home or build paths. A platform
  package refuses to publish when either binary is missing, empty or not
  executable.
- `exec` rejects a remote script over 112 KiB before opening an SSH session,
  with a size diagnostic instead of a broken pipe. If a bounded result is
  truncated and cleanup also fails, it is still reported as partial.
- Stopping a lease or `exec` on a signal closes the dispatch gate immediately:
  a queued command won't start. An already admitted call gets a bounded
  best-effort wait of up to 3 s.
- SSH refusal logs live in a private per-user directory under the state root,
  not in the shared temp directory. Mule validates the canonical path and
  its ancestors before use, so swapping a symlink alias can't redirect a
  log. Root-owned sticky directories such as `/tmp` are allowed ancestors.
- A failed `run --wait` shows the missing-tool hint on hosts that use dash.

## 0.5.0 — 2026-11-11

For callers that must survive their own crash: a status they can trust, a
caller key with dispatch dedupe, a listing by key, and a cancel that stays
inside the caller's process group. **Breaking for scripts:** `poll --json`
now exits 0 for every state, including a missing job (it was exit 1); read
`state` instead of the exit code.

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

- Review of P7a, round 4:
  - A keyed lease forgets its fresh id once its claim is known to have made
    nothing (the key was held, or the lookup stopped before `mkdir`), so a
    later failure no longer spawns a detached `mule rm` of an id that never
    existed.
  - The claimer's start time is read under `TZ=UTC` as well as `LC_ALL=C`:
    two mule processes in different zones read one live claimer's start as
    two times, which is the proof a claim was abandoned.
  - Test harness: the fault shims stop waiting for their lock once the
    fault dir is gone (bounded either way), every fault run reaps the
    processes it left (a detached `mule rm`), and every fault test fails if
    any survive. Unbounded shim loops left by detached `rm`s were the load
    behind `exec_tmux`'s 1s flake.
- Review of P7a, round 3 (all in the fault-injection harness):
  - `exec --no-detach`'s collect now runs on the prelude. A failed `wc`
    used to read as size 0, so exec exited 0 with the output silently
    dropped and removed the job.
  - A failed dispatch rolls back: a `pending` marker until the session
    starts, `cmd` written atomically, and the dir (key, claim) removed on
    failure. A failed `run --key` no longer leaves its key holding a
    never-started "orphan", and a failed TUI `pipe-pane` or `wait-for` no
    longer leaves a session hung. The key is written inside that guard,
    and the dir goes only once the sessions are proven gone; a session
    that will not die keeps the dir and its `pending`, so the job reads
    `unknown`, not `running`, and its key is never handed to a second job.
  - Prune and the final removal of `pending` are silent on stdout: an `rm`
    that printed before failing broke the dispatch reply's count and
    failed the dispatch of a job that had started.
  - The dispatch's advisory count can no longer print `1?` and fail a
    dispatch whose job already started.
  - `mule_regular` decides from one `ls -ld`, so an `rc` appearing mid-check
    is not "not a regular file".
  - A non-numeric `rc` makes one `ls` row unknown instead of failing the
    listing.
  - A claim records its claimer (host, pid, start time); a keyed lease rolls
    an unstarted claim back and dispatches only on proof the claimer is gone
    on this host and no session exists. It no longer holds every abandoned
    claim as unknown forever.
  - SPEC lists what is not fail closed yet: `tail`, `kill`, `rm --all`, and
    the interactive non-lease TUI helpers.
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
