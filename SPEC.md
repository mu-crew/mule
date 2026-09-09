# coop — fire remote jobs down a channel nothing else can take

Design spec. Written 2026-09-09 over three rounds of brainstorming; every
decision Q1–Q21 is settled. Implementation is in progress against the plan in
the `coop` mu workstream (`mu state -w coop`).

Language: Rust, edition 2024, following `~/hacking/tuicr`.

The *why* is written down throughout, because the *what* is recoverable from the
code later and the why is not. Where a decision was measured, the number is in
the text — that is what stops someone simplifying it back.

## Where this came from

It was not planned. It fell out of a day of orchestrating remote agents on a
devserver whose sshd sets `MaxSessions 1`, during which three separate agents —
and the author — independently rediscovered the same three lessons:

1. **Do not hold the ssh connection.** An interactive attach or a long
   `ssh host run-the-tests` consumes the host's only session channel, so every
   other ssh fails. The error is `Permission denied (keyboard-interactive)`,
   which reads as a credentials problem and sends you to look at the wrong
   thing. Walked into twice by the person who had just written the warning
   against it.
2. **Do not sleep-poll.** With no completion signal to block on, an orchestrator
   wrote `sleep 540` around a status check and blew its tool timeout twice.
3. **Poll the artifact, not the process.** Agent status flickers; a commit sha
   or an exit code does not.

coop is the tool that makes those three unnecessary to know.

## Problem

An orchestrator with work for a remote host has no good way to run it. Every
approach it reaches for is wrong:

1. **`ssh host run-the-tests` holds the connection** for the whole job. On a
   host whose sshd sets `MaxSessions 1`, that starves everything else —
   `git fetch`, `rsync`, a state collector — and the failure surfaces as
   `Permission denied (keyboard-interactive)`, which reads as a credentials
   problem and sends you to look at the wrong thing.
2. **Polling degenerates into `sleep 540`.** With no completion signal to block
   on, callers guess an interval, and a long one is what blows a tool timeout.
3. **Nothing survives a dropped connection.** The job dies with the ssh, or
   worse, survives with no way to learn its exit code.

Measured, not assumed. Five concurrent gated ssh calls to a capped host: **1 of
5 succeeded without a lock, 5 of 5 with one.**

## The evidence

Every claim in this document maps to something that was run against a real
capped host. None of it was reachable from a unit test at the time, which is
why the testing section below matters.

| claim | evidence |
| --- | --- |
| a gate is needed | 5 concurrent ssh: **1 of 5** succeeded ungated, **5 of 5** gated |
| the naive gate is unfair | 4 loopers: worst wait **6× the work**; one case **4.14s** for a 0.25s op |
| a second master is a second connection | two masters, distinct pids, concurrent traffic on both |
| isolation works | `sleep 12` starving the **default** channel; coop's socket answered normally in the same instant |
| isolation is symmetric | coop holding its channel 14s; three default probes all succeeded |
| a private tmux server is invisible | session on `tmux -L coop` absent from the user's `tmux ls` |
| dispatch is instant | detached job dispatch returned in **0s** while a full suite ran remotely |
| the master needs a TTY | `ssh -MNf` failed from a background call: hardware token cannot prompt |
| stale artifacts are real | a leftover `.rc` made a *running* job read as finished with the old exit code |
| duplicate names fail silently | `tmux new-session` twice prints `duplicate session` on stderr while the wrapper reports success |

Since then, most of this has been made reproducible without a devserver — see
*Testing*.

## The shape

Hand coop a command, get an id back, then poll, wait or tail against that id.
The user never sees ssh, never sees tmux, and never holds a connection.

```
$ coop run 'npm run check'
7a3f19

$ coop poll 7a3f19
running

$ coop wait 7a3f19
0

$ coop run --wait 'npm test'     # blocks, tails live, exits with the job's rc

$ coop run --cwd ~/hacking/murmur-w --wait 'npm run check'
```

The id prints before anything can go wrong, so it is never lost to a later
failure.

## Three invariants, all measured

**1. A private channel nothing else can find.** coop uses its own
`ControlPath` (`~/.ssh/coop/<host>.sock`). Everything else on the machine —
murmur's collector, an rsync, a hand-typed `ssh` — uses the default
`~/.ssh/control/%r@%h:%p` and cannot contend with it.

Verified on a real `MaxSessions 1` host: two masters ran concurrently with
different pids; with a `sleep 12` holding the **default** channel, a probe on it
failed with `Permission denied` while coop's socket answered normally in the
same instant. The reverse also held — coop holding its channel for 14s left
three default probes untouched. **The cap is per-connection, not per-user.**

**2. A private tmux server.** Jobs run under `tmux -L coop`, which does not
appear in the user's `tmux ls`. Verified: a session created on `-L coop` was
invisible to the default server.

**3. Nothing long-running ever rides the channel.** Every ssh coop issues is a
sub-second detached dispatch or an artifact read — including under `--wait`,
which polls rather than staying attached. Holding the connection is not a
discipline the user must observe; it is unreachable by construction.

## Who can starve whom

Invariant 1 means coop **cannot** starve the tools it shares the host with, and
they cannot starve it: different `ControlPath`, different connection, and the
cap is per-connection. Measured in both directions.

So the only contention coop can create is **coop against itself** — several
callers, or several `--wait` tails, competing for coop's own single channel.
That is the whole job of the ticket lock, and it is why the lock covers *every*
ssh coop issues and not only dispatch:

| call | takes the lock? |
| --- | --- |
| dispatch (`run`) | yes |
| `poll`, `wait`, `tail`, `ls`, `kill`, `rm` | yes |
| `ssh -O check` (master probe) | **no** — measured at 0s, opens no session channel |

Two concurrent `poll`s hit exactly the cap that motivated the tool, so treating
dispatch as the only privileged call would have left the hole open. The
exemption for `-O check` is what keeps `host list` and every "is the master
up?" path free.

This one decision propagates: it forces `ls` to be sequential, makes the
`--wait` cadence a fairness question rather than a taste question, and makes a
seconds-long lock wait a legitimate state that has to be reported (see
Fairness).

## Job identity: generated, never chosen

`run` mints a short random id (6 hex characters) and prints it. Everything about
a job derives from it — the tmux session, its state directory.

This kills two hazards outright, both reproduced:

- **Name collisions.** `tmux new-session -s job-x` twice prints
  `duplicate session` on stderr while the wrapper still reports success, so a
  caller sees a launch that never happened.
- **Stale artifacts, the dangerous one.** A leftover `.rc` from a previous run
  makes a *currently running* job read as finished, with the old exit code. A
  fresh id cannot collide with a leftover.

Hex avoids `:` and `.`, which tmux's target grammar reserves — the same trap
that once wedged a peer name permanently in murmur.

Cost: the id is the only handle back to the job, so `coop ls` is the recovery
path rather than a convenience, and `run` must record the command alongside the
artifacts or `ls` shows a column of meaningless ids.

## State on the remote host

Per job, under `~/.local/state/coop/<id>/`:

```
cmd     the command as given, for `ls`
log     stdout+stderr, appended live
rc      the exit code, written ONLY when the job finishes
```

Not `/tmp`: it is shared, cleared unpredictably, and is where the stale-`.rc`
hazard was reproduced. A coop-owned directory gives `ls` something to enumerate
and makes cleanup one `rm -rf`.

**`rc` is the completion signal.** It is durable, edge-triggered, survives a
dropped connection, and carries the exit code rather than a proxy for it. This
is the same lesson as polling a worker's commit sha rather than its activity:
poll the artifact, not the process.

The dispatch is one line, constructed by coop and never by the caller:

```sh
tmux -L coop new-session -d -s coop-<id> \
  'cd <cwd> && printf %s <b64> | base64 -d | sh > ~/.local/state/coop/<id>/log 2>&1; \
   echo $? > ~/.local/state/coop/<id>/rc'
```

Coop owning the wrapper is what makes invariant 3 structural. An earlier probe
let the caller pass an arbitrary command and a non-detaching one held the gate
for its full duration.

### The command is base64, not quoted

A command typed by the user crosses **three** expansion layers before it runs:
ssh's shell, tmux's argument parsing, and the `sh -c` tmux hands it — and coop
appends a redirect on top. Naive interpolation breaks on the first realistic
input:

```
coop run 'echo $HOME && grep "a b" file'
```

So the command is base64-encoded locally and decoded inside the wrapper. Layered
shell-quoting is correct in principle and wrong in practice: it is the class of
bug found in production rather than in review, and each new layer reopens it.
Encoding pays a few bytes once and makes quoting *structurally* impossible to
get wrong.

The `cmd` file in the state directory is therefore a **display copy** for `ls`,
not the thing that executes, and coop writes it through the same encoding so a
command containing quotes cannot corrupt its own record.

## Working directory and environment

`tmux new-session` starts in the invoking directory — `$HOME` over ssh — under a
shell that is **neither login nor interactive**. No `~/.profile`, so no `nvm`,
no `cargo`, no user `PATH` additions. Real usage is
`cd ~/hacking/murmur-w && npm test`, so making every caller write the `cd` is
boilerplate with a trap in it.

- `run --cwd <dir>` sets the directory, with a per-host `default_cwd` config key
  behind it. The wrapper `cd`s and **fails the job if the `cd` fails**, rather
  than running the command somewhere unintended.
- coop does **not** run the wrapper under a login shell. That would make every
  job's environment depend on the remote host's dotfiles: irreproducible, and
  the failure mode is the worst kind — "works when I ssh in, fails under coop".

The environment is the non-interactive default, and the caller owns any
sourcing it needs (`--cwd ~/x` plus `source ~/.nvm/nvm.sh && npm test` is
explicit and reproducible). `--help` states this rather than leaving it to be
discovered.

## Config

`~/.config/coop/config.toml`, hand-edited.

```toml
[hosts.dev]
target        = "dev"           # ssh target
socket        = "~/.ssh/coop/dev.sock"
tmux_socket   = "coop"
max_running   = 4               # warn past this; not a queue
default_cwd   = "~/hacking"     # where `run` starts, unless --cwd
keep_days     = 14              # prune `done` jobs older than this

[hosts.bubba]
target = "bubba"
```

A file rather than a state database, and this reverses an earlier instinct to
copy murmur's `peers` table. murmur's peers carry *discovered* state — snapshots,
`fetched_at`, `last_error` — which is why they need a store. coop's hosts are
pure user intent, so a file is editable, diffable and needs no migration story.

Defaults are derived from the host name, so a minimal entry is two lines.

## Fairness: a ticket lock

Coop's own concurrent callers share its one channel, so every ssh is serialised
locally (see *Who can starve whom*). The naive `until acquire; do sleep; done`
is a thundering herd — measured with four callers in a loop, worst wait **6× the
work it was waiting for**, and one case of **4.14s for a 0.25s operation**,
because a loser can lose repeatedly.

A ticket lock fixes it in about ten more lines: claim the next number
atomically, wait until now-serving reaches it, do the sub-second work, advance.
Bounded wait, no starvation, no daemon, and no state outliving the callers. A
holder that dies is handled by writing its pid into the lock and stealing when
that pid is gone — the pattern murmur's `withResetLock` already uses.

**Location:** `~/.local/state/coop/<host>.lock`, one per host. Local, because it
guards a local channel; per host, because two hosts must never serialise against
each other; in the state dir rather than beside the socket, because the socket
directory should hold only sockets.

**A long wait must be visible.** With tails in the queue a dispatch can honestly
wait seconds, and a silent wait is indistinguishable from the hang that
motivated the whole tool. After ~5s coop warns to stderr, naming the ticket
position and the pid of the holder, then keeps waiting. It does **not** abort on
a timeout: that would reintroduce the failure the ticket lock exists to remove.
pid-stealing already covers a dead holder, so this path is only about a live,
honest queue looking dead.

## The ssh master is required, not managed

coop refuses to run without one, exits non-zero, and prints the command:

```
coop: no control master for dev
  run: ssh -MNf -S ~/.ssh/coop/dev.sock -o ControlPersist=8h dev
```

Measured: `ssh -MNf` needs a TTY for a hardware token and cannot prompt from a
background invocation. A tool that tries anyway fails opaquely; `mu-sync-dev`
already takes this posture and exits 3 with the command to run.

This costs one hardware-token tap per `ControlPersist` window, which `--help`
should state rather than surprising someone with.

## Commands

```
coop run [--host H] [--cwd D] [--wait] [--no-tail] <cmd>
                                      dispatch; --wait blocks and tails live
coop poll <id> [--json]               exit code, or "running"
coop wait <id> [--timeout S]          block on rc; exits with the job's code
coop tail <id> [-f]                   output
coop ls [--host H] [--all] [--json]   id, state, rc, age, cmd
coop kill <id>
coop rm <id>                          drop the state directory
coop host list [--json]               configured hosts, and whether each has a master
```

`ls` states must distinguish three cases, because they are genuinely different:

| state | meaning |
| --- | --- |
| `running` | session alive, no `rc` |
| `done` | `rc` present |
| `orphan` | session gone, no `rc` — the host rebooted, or the session was killed by hand |

`orphan` is the row that matters. It is the same distinction as crashed-versus-
finished for a remote agent, and it is answerable here only because `rc` is
durable.

**`kill` writes an `rc` first.** `[ -f rc ] || echo 137 > rc`, then destroy the
session. One line, and it earns `orphan` its meaning: *coop did not do this*.
Without it a killed job and a rebooted host are indistinguishable, which is most
of what the deferred boot-id idea was for. The `||` guard matters because the job
may finish between the decision to kill and the kill.

**`--json` on `ls`, `poll` and `host list`.** coop's primary caller is an
orchestrator or an agent, and `ls` is the recovery path for a generated id — a
human reads it, a script parses it. murmur exposes `peer list --json` for the
same reason. `run` already prints one token and `wait` already communicates
through its exit code, so neither needs a flag.

### One probe primitive

`poll`, `wait` and `tail -f` are all the same **single round trip**, which
returns `rc` (if present), the log size, the new bytes from a byte offset, and
session presence together. A tail therefore costs exactly what a poll costs, and
there is one mechanism to test rather than three.

`--wait` is exactly `run` then `wait`, with the tail attached: the id prints
**first** so it survives any later failure, and the exit code is the job's.

**Cadence: 1s, backing off exponentially to 5s, reset to 1s whenever new output
arrives.** Because tail probes take the lock, cadence is a fairness decision:
four concurrent `--wait` jobs at a fixed 1s would spend most of the channel on
tails and queue dispatch behind them; ten would oversubscribe it and grow the
lock wait without bound. Backoff handles the common case of a job that is
running but quiet, while the output-driven reset keeps a chatty job feeling
live. Start there and measure against a real suite before making it a config
key.

`--wait --no-tail` is the retreat: block on `rc` and dump the whole log once at
completion. If the channel proves tighter than measured, that is a one-flag
change per call rather than a redesign.

## How the user gets the output

The exit code and the output are deliberately different questions, answered by
different verbs. `log` is durable on the host, so **`coop tail <id>` is the
universal answer** — it works during a job, after it, on a job someone else
started, and on an `orphan` (you get everything up to the moment it died).

| situation | how output arrives |
| --- | --- |
| dispatched, come back later | `coop tail <id>` — one probe from offset 0 |
| watching it happen | `coop run --wait` — streams at the 1s→5s cadence |
| watching someone else's job | `coop tail <id> -f` |
| `--wait` and the connection dropped | exit non-zero, and coop prints `coop tail <id>` |
| channel too tight to stream | `--wait --no-tail` — one dump at completion |
| job died or the host rebooted | `coop tail <id>` still works; `rc` is what's missing, not the log |
| after `coop rm <id>` | gone, deliberately |

**`poll` and `wait` print no output at all, and that is the point.** `wait`'s
result *is* its exit code, which is what makes `coop wait x; echo $?`
composable, and `run --wait` already covers "block and show me". A third
spelling would only raise the question of what `--wait --print --no-tail` means.
But `--help` for both verbs must say *prints nothing; use `coop tail`* —
otherwise silence reads as a broken job.

### stdout and stderr are merged, and stay merged

The wrapper's `2>&1` puts both streams in one `log`, unrecoverably. That is a
real limitation and not an oversight: merging preserves **causal interleaving**,
which is what someone debugging a failed remote job actually wants, and it is
why `2>&1` is the reflex in the first place.

Splitting into `log.out`/`log.err` would double the artifacts and make the probe
fetch two offsets — a real cost against invariant 3 — to serve a case the caller
can already handle, because the command is theirs:

```sh
coop run 'build --json > out.json 2>err.log'
```

So `out=$(coop tail <id>)` on a job emitting machine-readable output will mix
warnings into it. Documented, not fixed.

### Log bytes are bytes

A job may emit a tarball, or simply invalid UTF-8. `String::from_utf8_lossy` on
the log path silently substitutes replacement characters, which is corruption of
the one artifact the whole design treats as the truth — worse than refusing.

So the probe reply carries the log as **bytes**, and `tail` writes them to stdout
raw. `Output.stderr` stays a `String`: ssh's own diagnostics are text, and
`errors.rs` pattern-matches them.

(Text-only would have been a defensible scope cut. Silent corruption is not.)

### `tail` is capped by default

A 200MB log from a chatty suite is not a "sub-second artifact read": it pulls
200MB through the channel **while holding the lock**, which is the
`ssh host run-the-tests` failure wearing a different hat.

So `tail` fetches the **last 64KB** by default, with `--all` for the whole log
and `-n <lines>` when that is cheaper. Same reasoning as the probe cadence — the
lock makes payload size a fairness problem, not merely a speed one. `-f` is
unaffected, since it only ever fetches the delta since its last offset.

The 64KB is a guess and should be measured against a real suite's log.

### `max_running`

Counted inside the dispatch round trip, so the warning is retrospective —
`dispatched 7a3f19; 5 now running on dev, cap 4`. A separate pre-flight probe
would double dispatch to two round trips and two lock cycles to improve a
warning that is already advisory, and dispatch being *one* round trip is the
condition the "dispatch is instant" measurement was taken under.

## Behaviour under failure

**Connection drops mid-`--wait`:** exit non-zero, print `coop tail <id>`. The
job genuinely survives — that is the point of detached tmux — so reporting
failure would be a lie, and reconnect logic is where a dispatcher becomes a
supervisor.

**Refused session channel:** probe with `ssh -O check` (0s, takes no channel,
measured) and report that the master is down or the slot is held, rather than
passing ssh's misleading auth error through.

**Host at `max_running`:** warn and proceed, naming the count. Backpressure, not
a scheduler — the caller decides.

**Host no longer in config:** accepted, not solved. A job's state directory and
tmux session outlive the config entry that named them, but coop needs a target
and a socket to speak to a host at all — so a job on an unconfigured host is
unreachable by definition. `ls` iterates configured hosts and says so.

A local `id → host, cmd, time` index would make those jobs listable, and was
declined: it puts a second, staleable copy of job identity on the local machine,
and "the remote artifact is the only source of truth" is precisely what kills
the stale-`rc` class of bug. What `ls` **must** do instead is report a
configured host as *unreachable* rather than silently omitting its rows — a
missing row and a host that is down must never look the same.

## Cleanup

Job directories accumulate forever otherwise. Two mechanisms, both
conservative:

- `coop rm <id>` — manual, explicit, one `rm -rf`.
- `run` opportunistically prunes `done` jobs older than `keep_days`
  (default **14**), on the host it is already talking to, inside the round trip
  it is already making.

Pruning touches **`done` only**. A `running` job is live and an `orphan` is
evidence of something that went wrong — both are the rows a person came to read.
Deleting a `log` someone still wants costs more than the disk it saves, which is
why the horizon is generous and the default errs toward keeping.

## Testing

Every invariant this design rests on was measured against a real capped host.
The original conclusion was that none of it was reachable from an automated
test, which would have left the whole design resting on manual probing.

**That turned out to be wrong, and it is the most useful thing learned since.**
A non-root `sshd` on a loopback port reproduces the contention exactly:

```sh
/usr/sbin/sshd -f <tmp>/sshd_config -E <tmp>/log   # no root, no PAM, no token
# MaxSessions 1, HostKey <tmp>/host_ed25519, AuthorizedKeysFile <tmp>/id.pub,
# UsePAM no, StrictModes no, PasswordAuthentication no,
# KbdInteractiveAuthentication no
```

Measured locally on macOS 26.6.2 / OpenSSH_10.3p1:

- a control master opens on a private `-S` path; `ssh -O check` answers
  `Master running (pid=NNN)`
- with one session channel held by `sleep 6`, a concurrent call on the **same**
  socket is refused with `mux_client_request_session: session request failed:
  Session open refused by peer` — the exact string `mu-sync-dev` matches
- a concurrent call on a **different** connection succeeds while the first
  socket is starved. **This is invariant 1, reproduced without a devserver.**

So the suite is **four layers**, and the first three are unattended:

| layer | what it covers | needs |
| --- | --- | --- |
| 1 | pure logic over the `Transport` trait with `Fake` | nothing |
| 2 | the real wrapper executed against `tmux -L coop-test-<pid> -f /dev/null` | `tmux` |
| 3 | channel isolation, gating, bounded lock wait against a local `sshd` | `sshd` |
| 3b | the real `Permission denied (keyboard-interactive)` from 2FA, and real network latency | `$COOP_TEST_HOST` |

Only 3b needs a human, and only for the one hardware-token tap that opens the
master. Everything else runs on a plane.

The original three-layer split, kept because the reasoning still holds:

**1. Pure logic — no network.** Id generation, config loading and host
defaults, wrapper *construction* (including the base64 encoding and the `cd`),
lock ticket ordering, and the `rc`/session-presence → `running`/`done`/`orphan`
mapping. Behind a `Transport` trait with a fake, so the mapping is tested as a
function rather than through ssh.

**2. Real tmux, no ssh.** The constructed wrapper actually executed against
`tmux -L coop-test-<pid> -f /dev/null`, following
`murmur/test/mux-targets.test.ts`: private socket so nothing touches the
developer's server, `-f /dev/null` so no personal config leaks in (measured
there at 3.5s vs 0.02s server start, and a source of flakiness). This layer is
where the highest-value bugs live — quoting survival, the `rc` write actually
firing, `kill` writing 137, exit codes propagating through
`base64 -d | sh` — and a fake transport cannot see any of them.

**3. A real sshd — local, unattended.** The four measurements the design rests
on: channel isolation in both directions, gated vs ungated concurrency,
dispatch latency, and bounded lock wait under four concurrent callers. Skips
with a printed note if `sshd` is missing or the port is taken.

**3b. A real capped host — gated on `COOP_TEST_HOST`.** Only what a local sshd
cannot show. Locally the fallback-auth failure says
`Permission denied (publickey)` because there is no 2FA; the *class* is
identical, so `errors.rs` must match both strings, but only a real host proves
the 2FA wording. Skipped when the variable is unset; never run in CI.

The layer the original "fake plus integration" split missed is layer 2, and it
is the one that pays for itself.

Traps found while proving layer 3 works, all of which cost time once:

- pick a free ephemeral port rather than hardcoding one
- `chmod 600` the config and key, or sshd refuses to start
- `StrictModes no` is required because `/tmp` is world-writable
- always kill the pid in `PidFile` and remove the temp dir
- **quote ssh option arrays properly.** Word-splitting an `$SSHOPTS` string
  produced `Can't open user config file /dev/null -i ...` and the probe
  *silently passed anyway* — a test that cannot fail is worse than no test.

## Why this is not part of murmur

Asked and declined during the design session.

murmur's `ARCHITECTURE.md:827` states that it *"observes and never places
work"*, which is exactly what lets it skip an op-log, watermarks and conflict
resolution. Firing jobs is placing work. A control plane would need job
identity, exit-code capture, output retrieval, cancellation and a completion
signal — four new writers and a lifecycle, on a store designed as
one-writer-per-fact, current-state-only, no history. The same file names the
endpoint: *"a murmur with `put`/`del` over arbitrary entities would just be
mu."*

A `murmur wait` verb was declined for a sharper reason: the facts murmur holds
that are genuinely edge-triggered (`crashed`, a peer going unreachable) are not
what an orchestrator wants, and the exact completion signal for a remote job is
an exit code or a commit sha — git's to report, not murmur's.

Note that murmur is **not** a client of coop's gate either, despite an early
draft claiming so; see *Running local commands on coop's channel* below.

## Prior art this follows

- **`mu-sync-dev`** (in the author's dotfiles) is the closest posture match: it
  exits 3 with the exact `ssh -MNf` command when the master is missing, rather
  than attempting a connection that cannot succeed. coop copies both the
  posture and the exit code, and its `SLOT_BUSY` regex is the source of coop's
  error classification.
- **murmur's `test/mux-targets.test.ts`** drives a real tmux server on a private
  socket with `-f /dev/null`, rather than asserting on argv. That is test layer
  2. Its own measurement is worth knowing: a server that sourced the author's
  `~/.tmux.conf` took 3.5s to start against 0.02s with an empty config, a 175×
  difference that made tests flaky under parallel load.
- **murmur's `withResetLock`** is the pid-into-the-lock, steal-when-gone pattern
  coop's ticket lock uses for a dead holder.
- **`~/hacking/tuicr`** sets the Rust conventions: edition 2024, `clap` derive,
  `anyhow` at the boundary with `thiserror` for errors callers switch on,
  `serde` + `toml` for config.

## Out of scope, deliberately

**A queue and priorities.** Three reasons, in order of weight. (If host
contention ever turns out to be real, the answers in order are the concurrency
cap already in scope, then a second master on another `ControlPath`.)

*Nothing worth ranking is contended.* Every call through the lock is
sub-second, and the jobs themselves run in parallel tmux sessions entirely
outside it, so a priority on a queue of 250ms operations buys nothing. Q14 does
put tail probes in that queue, which is real contention — but the answer to it is
the backoff cadence and `--no-tail`, both of which reduce the number of probes,
not a scheduler that reorders them.

*A queue needs something to dequeue when a slot frees*, which is a daemon — and
coop having no process between invocations is what makes it honest.

*`mu` already ranks work by ROI*; coop ranking it again would be coop becoming
an orchestrator.

If host contention turns out to be real, the answers in order are the
concurrency cap and backpressure already in scope, then a second master on
another `ControlPath` — channel contention is self-imposed, not a property of
the host.

**Holding a connection to stream.** `--wait` tails by polling with a byte
offset. Real streaming would mean an attached channel, which is the thing coop
exists to avoid.

**A local job index.** See *Host no longer in config*. One source of truth, and
it is remote.

**Running local commands on coop's channel.** rsync, `git fetch` and murmur's
collector contend with each other on the *default* `ControlPath`; coop cannot
help them, and a wrapper like `coop with <host> -- rsync ...` is not a missing
feature but a **violation of invariant 3**. An 18MB transfer holding coop's
channel for 30s is exactly the `ssh host run-the-tests` failure the tool exists
to make unreachable: every `--wait` tail would stall behind it and the 5s lock
warning would fire on all of them. coop's channel is for sub-second calls, and
accepting bulk transfer onto it would trade a solved problem for an open one.

Nor can rsync become a coop *job* — a transfer has a local endpoint, and coop
only dispatches work that runs entirely on the remote host; detached in
`tmux -L coop` it would have nothing to talk to.

The answer for those tools is their own master on their own `ControlPath`,
which the measurements already support (two masters, distinct pids, concurrent
traffic on both) and which costs one `ssh -MNf` and zero coop code. The real
ceiling is not sshd but one hardware-token tap per `ControlPersist` window.

If sharing ever looks worthwhile, the honest shape is `coop ssh-args <host>`
printing `-S <socket> -o BatchMode=yes` for `RSYNC_RSH`/`GIT_SSH_COMMAND` — it
hands over the args and lets the caller own the consequence, and it is a
`println!`. Not in v1.

**Anything site-specific.** The design is general; the capped host is only the
motivating case, and every site-specific value lives in the user's config —
the same boundary the murmur/dotfiles split already draws.

## Key decisions

All three rounds, summarised so the *why* survives without re-reading the whole
document. Every one is written into the sections above.

| # | decision | why |
| --- | --- | --- |
| Q1 | config is a hand-edited TOML file, not a state DB | hosts are pure user intent; editable, diffable, no migration story |
| Q2 | job state on the remote host under `~/.local/state/coop/<id>/` | `/tmp` is shared and cleared unpredictably — where the stale-`rc` hazard was reproduced |
| Q3 | a dropped connection during `--wait` exits non-zero and prints the id | the job keeps running; saying otherwise would be a lie |
| Q4 | ticket lock in v1, not a plain mutex | the naive gate measured 4.14s of wait for a 0.25s op |
| Q5 | coop requires an ssh master and refuses without one, printing the command | `ssh -MNf` needs a TTY for a hardware token |
| Q6 | the tool is general; the capped host is only the motivating case | every site-specific value lives in the user's config |
| Q7 | lock at `~/.local/state/coop/<host>.lock`, one per host | local channel, local file; two hosts must not serialise against each other |
| Q8 | `run --wait` = `run` then `wait`, tail attached, id printed first | no second code path; the id survives any later failure |
| Q9 | probe at 1s → 5s backoff, reset on new output | tail probes take the lock, so cadence is fairness; backoff for quiet jobs, reset for chatty ones |
| Q10 | `ls` sequential | with the lock over every ssh, parallel `ls` cannot exist — it would serialise inside the lock anyway |
| Q11 | manual `rm` + prune `done` past `keep_days` (14) | a wanted log costs more than the disk; never prune `running` or `orphan` |
| Q12 | four test layers: fake, real-tmux, local sshd, real host | layer 2 catches quoting and rc-write bugs a fake cannot see; a local sshd reproduces the channel cap without a token |
| Q13 | unconfigured host accepted; `ls` reports unreachable, never omits | a local index would be a second staleable copy of job identity |
| Q14 | lock **every** ssh except `ssh -O check` | two concurrent `poll`s hit the same cap; `-O check` takes no channel |
| Q15 | base64 the command, decode in the wrapper | three expansion layers; layered quoting fails in production, not review |
| Q16 | `--cwd` + `default_cwd`, non-interactive shell, no login shell | removes boilerplate without making jobs depend on remote dotfiles |
| Q17 | `--json` on `ls`, `poll`, `host list` | the primary caller is an agent; `run`/`wait` already speak in tokens and exit codes |
| Q18 | warn to stderr after ~5s of lock wait, never abort | a silent wait looks like the hang coop exists to remove; a timeout reintroduces it |
| Q19 | `kill` writes `rc` 137 if absent, then kills | earns `orphan` its meaning: *coop did not do this* |
| Q20 | count `max_running` inside the dispatch round trip | the warning is advisory; dispatch stays one round trip |
| Q21 | coop's channel is never lent to local commands (rsync, `git fetch`) | a long transfer on coop's socket *is* the failure coop exists to remove; those tools get their own `ControlPath` |
| Q22 | `poll`/`wait` print no output; `tail` is the output verb | `wait`'s result is its exit code, which is what makes it composable; `--help` must say so or silence reads as a broken job |
| Q23 | stdout and stderr stay merged in one `log` | preserves causal interleaving; splitting doubles artifacts and probe offsets to serve a case the caller can redirect for themselves |
| Q24 | the log payload is bytes, not a lossy `String` | `from_utf8_lossy` corrupts the one artifact the design calls the truth |
| Q25 | `tail` fetches the last 64KB by default, `--all` to override | a 200MB read holding the lock is the `ssh host run-the-tests` failure again |

### Deferred, recorded so they are decisions and not omissions

- **Whether `ls` reaches for `log`.** Showing the last line makes `ls` far more
  useful and costs a read per job. Probably worth it behind a flag.
- **Reboot detection.** Largely obviated by Q19: with `kill` writing an `rc`, a
  remaining `orphan` already means "not coop's doing". A boot id would still
  separate a reboot from a hand-killed session; not worth it until it bites.
- **Cadence as a config key.** Fixed at 1s→5s until a real suite says otherwise.
- **The `tail` cap as a config key.** Same posture: 64KB until measured.
- **Splitting stdout from stderr.** See Q23. Reconsider only if a caller
  genuinely cannot redirect for itself.
- **Crate, binary and repo name.** `coop` throughout, chosen for the private
  enclosure the architecture actually is. Not checked against crates.io.
- **Whether coop should expose the jump-command idea murmur has.** Both tools
  now hold "how to reach this host", in different files. Not a duplication
  worth solving until one of them changes.

## Implementation checklist

Ordered so each step is independently verifiable. The live version of this,
with full per-task detail, is the `coop` mu workstream (`mu state -w coop`);
when reality disagrees with the plan, the task note is what gets updated.

1. ~~**Config + `Transport` trait.**~~ **Done** (`24b51e0`). Load TOML, derive
   host defaults, fake transport for tests. `coop host list [--json]` with
   master detection via `ssh -O check` — the one path that takes no lock.
2. **Ticket lock** at `~/.local/state/coop/<host>.lock`, pid-based stealing,
   stderr warning past ~5s. Verify with four concurrent callers asserting a
   **bounded** wait — the property the naive version measurably lacks.
3. **Wrapper construction**, as a pure function: base64 encode, `cd <cwd> &&`,
   redirect, `rc` write. Unit-tested on the string, then executed for real
   against `tmux -L coop-test-<pid> -f /dev/null` (test layer 2), including a
   command containing quotes, `$VAR` and spaces.
4. **`run`**: id generation, remote state dir, dispatch through the lock, `cmd`
   display copy, `max_running` count folded into the same round trip.
5. **The probe primitive**: one round trip returning `rc`, log size, bytes from
   an offset, and session presence. `poll [--json]` and `wait [--timeout]` on
   top of it; `orphan` detection.
6. **`tail [-f]`**, then `--wait` composed from `run` + `wait` + tail, with the
   1s→5s output-reset cadence, plus `--no-tail` as the dump-at-end retreat.
7. **`ls [--json]`** sequentially across hosts — unreachable hosts reported, not
   omitted — then `kill` (rc 137 first) and `rm`.
8. **Prune** `done` jobs past `keep_days` inside `run`'s round trip.
9. **Failure paths**: no master (exit 3, print the `ssh -MNf` command), refused
   channel classified rather than passed through, dropped connection mid-wait
   (non-zero, print `coop tail <id>`).
10. **Local-sshd suite**, unattended: isolation both directions, gated vs
    ungated concurrency, dispatch latency, bounded lock wait. Plus a thin
    real-host suite gated on `COOP_TEST_HOST` for the 2FA error string and real
    network latency.
