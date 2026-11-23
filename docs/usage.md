# Using mule

## Flags go before the command

mule's own flags go **before** the command, because everything after the first
word belongs to the command: `mule run --wait ls`, not `mule run ls --wait`.
The second form sends `--wait` to `ls`, and mule warns when it spots one of its
flags there. Use `--` when the command takes a flag mule also has:

```sh
mule run -- ls --all
```

## Agent metadata

By default, the job command receives `MU_MANAGED_AGENT=1` and
`MU_AGENT_NAME=mule-<job-id>`. If the caller has a non-empty `MU_WORKSTREAM`,
mule forwards it unchanged, so murmur shows the job as crew. Use
`mule run --human <cmd>` to omit all three variables for an ad-hoc agent.

## TUI jobs

Commands that need a terminal use explicit TUI mode:

```sh
id=$(mule run --tui 'pi --approve')
mule tail "$id"                 # current screen, or final saved screen
mule tail --transcript "$id"    # raw terminal transcript, explicitly
```

TUI dispatch prints pasteable `tsesh pick-and-connect`, `mu agent spawn`, and
`mule kill --rm` commands on stderr. A mu worker uses `run --lease` instead
(below): its pane is the job's life, not a viewer. `tail -f` refuses TUI jobs because a redraw
stream is not a useful log; use the screen or attach through the tsesh agents view instead.

## The job's shell

Jobs use a non-login, non-interactive shell, so login profiles do not run and a
job cannot depend on your dotfiles. Bash is a partial exception: it sources
`~/.bashrc` even for a non-interactive command over ssh, so a `PATH` set there
does reach a job. `.bash_profile` — where an environment manager's `activate`
usually lives — does not run.

In practice that is enough: put your version manager's shims directory on
`PATH` in `.bashrc` and jobs resolve the same tool versions a login shell
would. If you need activation itself, ask for it in the command:
`mule run 'source ~/.zshrc && npm test'`. There is deliberately no flag for it —
a login shell measured 83ms against a 125ms dispatch and changed nothing but
`PATH` length.

## Output

Standard output and standard error are merged into one log, in the order the
job wrote them; redirect inside your command to separate them. One artifact per
job is deliberate: two would mean two probe offsets and a lost interleaving, to
serve a case a redirect already covers.

`poll` and `wait` do not print job output; use `tail`. A one-shot `tail` prints
the last **64KB** by default; use `--all` or `-n LINES` to choose another range.
Logs are capped at 100MB per job (`max_log_bytes`), and a truncated log says so.

### Do not pipe your command into `head` or `tail`

Truncating output inside the job looks thrifty and silently breaks the one
thing mule guarantees:

```sh
mule run 'lake build 2>&1 | tail -3 && ./check'   # WRONG
```

`rc` becomes the pipe's. Measured: `sh -c 'echo x; exit 1' | tail -3` exits
**0**, so a failed build reports success — and because `rc` is 0, the `&&`
proceeds and `./check` runs against a broken tree. `rc` is the artifact the
whole design rests on, so this failure is invisible everywhere a caller looks.

It is also redundant. mule already bounds the log at both ends: `max_log_bytes`
caps the write at 100MB, and a one-shot `tail` caps the read at 64KB. Let the
job be the work and let mule shape the output:

```sh
id=$(mule run 'lake build 2>&1 && ./check')
mule wait "$id"; mule tail "$id" -n 3
```

If you genuinely want a pipeline, `set -o pipefail` inside your command keeps it
honest — verified working in the remote `sh` on both macOS and Linux, but not
portable POSIX, so mule will not inject it. Changing the meaning of every
caller's command to fix some of them is not mule's call to make.

## Timeouts

Jobs are unbounded by default. Set `max_job_secs` on a host, or pass
`mule run --max-secs S`, to cap remote runtime. A timed-out job records rc
**124**, following GNU `timeout`; `poll`, `wait`, and `ls --json` therefore
distinguish it from an ordinary failure. This is separate from
`mule wait --timeout S`, which only stops the local wait and leaves the job
running.

## Listing and cleanup

`mule ls` shows the last 24 hours plus anything running or orphaned; `--all`
reaches further back. Its `RUNTIME` column is elapsed time for running jobs,
total time for finished jobs, and `-` for orphans, whose runtime is unknowable.
The table collapses whitespace and marks commands longer than 80 characters with
`…`, keeping each job on one line. Use `mule ls --full` to read a long command
in the table, and `mule ls --json` as the machine surface; JSON keeps `age_secs`
and adds `runtime_secs`.

`mule rm <id>` ends that job if it is still running, then removes its state.
Exit 0 means it is stopped and gone: neither tmux session exists and the state
directory is removed. A job that is already missing also exits 0, so it is safe
to repeat. If a session survives the kill or the directory cannot be removed it
exits 1, names which, and keeps what is left. It also exits 1, keeping state,
when absence cannot be verified: a tmux that cannot be queried (version mismatch,
unsafe socket directory) or a state path behind an unsearchable directory. Only
`no server running`, a missing socket, or `No such file or directory` count as
gone. So a script can treat exit 0 as proof before deleting its own handle.
`mule rm --all` removes every **finished** job, ignoring `keep_days`, and leaves
running jobs and orphans alone. Prefer `mule kill` when you want the exit code
recorded as **137** without dropping state; use `kill --rm` to end and discard
in one trip.

`mule rm <id> --retry-for SECS` is the signal-path stop a lease or `exec`
spawns. You rarely type it. It waits for a missing master (a `-O check` every
5s; it never opens one), retries a failed `rm` after 5, 10, 20, then every 30s,
and stops at a soft deadline: no attempt starts after it, but one in flight is
not cut short.
`--failed-file F` writes one line there on giving up and removes it on success.
`--fence-gen N` stands down (exit 0, nothing removed) once the job has been
reattached; a lease always passes the generation it owned at dispatch. A lease's stop retries for 10 minutes and records to
`<job_file>.<id>.stop-failed`:

```text
3a043f on box not stopped: no control master (gave up after 0 attempts, 600s; at unix 1762270000)
```

That file means the job may still be running: open the master, then
`mule ls --host box`, then `mule rm --host box 3a043f`.

## Status for a program: `poll --json`

`mule poll <id> --json` prints one object and exits 0 for every state:

| `state` | Meaning |
| --- | --- |
| `running` | the session exists and there is no `rc` |
| `done` | `rc` exists; it is in `rc` |
| `orphan` | the state is there, the session is gone, and no `rc` will come |
| `missing` | no state directory and no session: proven, as `rm` proves a stop |
| `unknown` | mule could not tell; `reason` says why |

```json
{"state":"running","rc":null,"runtime_secs":12,"log_size":0}
{"state":"missing","rc":null,"runtime_secs":null,"log_size":null}
{"state":"unknown","rc":null,"runtime_secs":null,"log_size":null,"reason":"cannot tell the state of job 3a043f: cannot verify state dir ...: Permission denied"}
```

`unknown` covers every probe that did not complete: a state path behind an
unsearchable directory, a tmux that cannot be queried, a tool that failed, an
`rc` that exists but will not read, a key claimed by a lease that has not
started its job, a refused or dropped session. Never read it as "gone". Exit 3 still means no master. Plain `poll`
is unchanged: a missing job is exit 1, `job <id> not found`.

## Idempotent dispatch: `run --key`

```sh
mule run --key mu.3f2c9a1e-7b4d-4e8f-9a0b-1c2d3e4f5a6b -- make test
```

If a job with that key already exists, in any state, mule prints its id,
says `key K already names job <id>; nothing was dispatched` on stderr, and
starts nothing. Otherwise it dispatches as usual and stores the key with the
job. The id is still generated. The key is 1-128 characters of `A-Z a-z 0-9
. _ -`, compared as a string (`01` and `1` are two keys), and stays with the
job until `mule rm`: prune never removes a keyed job, at any age. If the
lookup cannot be completed (a `key` that will not read, say), the dispatch
refuses rather than risk a second run. It works with `--wait`, `--tui` and
`--lease`.

`mule ls --json --key-prefix P` lists the keyed jobs whose key starts with
`P`, at any age (the 24-hour window does not apply), each with its `key`. A
host under `unreachable` was not searched. If any job's `key` will not read,
`--key-prefix` exits 1 naming it instead of answering. In plain `ls --json`, a
keyed row has a `key` field and an unkeyed row has none, and a row whose `rc`
or `key` will not read has state `unknown` and a `reason`.

## Forwarding a unix socket

```sh
mule forward   [--host H] --unix LOCAL:REMOTE ...
mule unforward [--host H] --unix LOCAL:REMOTE ...
```

For control-sized traffic only: a ctl socket, an RPC endpoint. A forward is a
channel on mule's connection, not a session, so it works while a job verb holds
the one `MaxSessions` slot and takes no ticket lock. There is no TCP: a
dev-server or bulk forward would share the connection with every poll.

`forward` checks that LOCAL is listening with `connect()`, not by ssh's exit
code, which proves nothing. REMOTE need not exist yet; it is resolved per
connection. It replaces a stale LOCAL socket, but refuses (exit 1) a LOCAL that
is any other kind of file. Paths are passed to ssh as arguments, never through a
shell, and neither may contain `:`.

Forwarding one LOCAL to a second REMOTE is safe through `mule forward`, which
remembers what it forwarded (`~/.local/state/mule/forwards/`). Doing it by hand
with `ssh -O forward` can wedge the master's forward table, and **exit 3** from
`forward` means exactly that: stop, and ask a human to run the printed
`ssh -O exit` and `ssh -MNf` commands. The master command now includes
`-o StreamLocalBindUnlink=yes`, so a reopened master rebinds over a stale file.

`unforward` cancels, removes LOCAL and forgets the record. It is idempotent and
needs no master, but exits 1 naming the path if it cannot remove LOCAL or the
record (a read-only directory, say), keeping the record so a retry can finish.
If the record names a different REMOTE than the one you gave, `unforward` cancels
that recorded forward too: leaving it in the master's table would let the next
`forward` of the LOCAL put a second REMOTE on it, which wedges the table.
Concurrent `forward`/`unforward` of the same LOCAL take turns on a local lock;
different LOCALs never wait on each other.

## Holding a job for a pane's life: `run --lease`

```sh
mule run --lease [--host H] [--cwd D] [--human] [--max-secs S] \
  --job-file F [--forward-unix LOCAL:REMOTE ...] -- <cmd>
```

For a local pane whose life must be the job's. `mu-plugin-mule` serves this as
mu's bound `lease` job kind (tmux-backed views, file handle, built-in `ctl`
channel; see [As a mu plugin](#as-a-mu-plugin-mu-plugin-mule)), and the verb
also works directly or from a mu lease template profile. It is `run --tui` plus
a foreground holder, and holds no session in between: a probe per tick (1s
settling to 5s) over mule's own connection.

- **The id is in `F` before the job exists** (one line, mode 0600, written
  atomically). If the write fails, nothing is dispatched (exit 1). The stop is
  always `mule rm --host H $(cat F)`, even if the lease died a moment after
  writing it.
- **Forwards** each `--forward-unix` as `mule forward` does, straight away
  (REMOTE need not exist yet), and re-forwards when LOCAL is removed or
  replaced, or the master is replaced.
- **Exits with the job.** The job's own code when it ends; 1 if it is missing;
  5 if orphaned. On the way out it prints the final screen and unforwards.
- **A lost master does not end it.** It prints once `master for H is gone; a
  human must run: ssh -MNf …`, keeps checking every 5s, and re-forwards when the
  master is back. Exit 3 happens only at startup, before anything is dispatched.
- **SIGHUP/SIGINT/SIGTERM stop the job** (exit 129/130/143) through a detached
  `mule rm`, and cancel the forwards. Closing the pane is a SIGHUP. `kill -9`
  cannot be handled: the job keeps running and `F` names it. The detached stop
  retries for up to 10 minutes (a down master included). If it still fails it
  writes `F.<id>.stop-failed`, one line starting with the id, so a job that
  could not be stopped is never silent.
- **The pane shows** a status line (job, host, state, forwards, the
  `tsesh pick-and-connect --mode agents  # (tmux: prefix a) ctrl-g shows crew, select mule-<id>` attach command and the stop command)
  and mirrors the remote screen for 30s after the first forward, then only on
  a change of state and at exit. Keystrokes are not forwarded; attach through
  the tsesh agents view to interact.

```sh
mule run --host box --lease --human --cwd ~/w --job-file /tmp/w1.job \
  --forward-unix /tmp/w1.sock:/home/me/.mu/w1.sock -- env K=v sh -c 'pi'
```

With `--key K`, a lease whose key already names a job holds that job instead
of starting one: `F` names it, and the lease ends with its code (at once, for
a finished one). The key is claimed before `F` is written, so `F` only ever
names the job holding `K`: two leases racing with one key both hold the one
job, and both job files name it.

### Reattaching to a running job

```sh
mule run --lease --attach <id> [--host H] --job-file F [--forward-unix LOCAL:REMOTE ...]
```

Puts a new carrier pane on a job whose lease died while the job ran on (a
`kill -9`, or a hangup whose stop could not reach the host). mu's `agent
reattach` runs it from the host's `attach` template. Nothing is dispatched:
it is a lease minus the start.

- **Refuses before holding or changing anything.** Exit 3 with no master.
  Exit 1 if the job is not running (`mule: job <id> is not running (done 0 |
  orphan | missing)`) or the probe fails.
- **`F` is never replaced.** Missing, it is written (0600, atomic, never over
  a file that appeared meanwhile). Naming `<id>`, it is kept. Naming another
  job, empty, or unreadable: exit 1, `F` untouched.
- **An old stop is fenced off.** If the dead lease left a stop retrying (for
  instance waiting for a master), the attach waits out an attempt in flight,
  then moves the job's stop fence; that old stop then exits without touching
  the job, even once its master returns. The attach's own signal stop still
  works.
- **Then it holds exactly like a lease**: same pane output, exit codes and
  signal stop.

The first forward replaces what the dead carrier left: a stale listener for
the same spec, a LOCAL file that was deleted while the master still holds the
forward, or forwards lost with a replaced master. It cannot repair a wedged
forwarding table (only `ssh -O exit` and a new master do), nor a surviving
forward to a different REMOTE whose record is gone. Then the pane prints the
error and keeps holding, and whatever connects to LOCAL (mu's ctl handshake)
fails; restart the master and attach again.

## Running a command and waiting: `exec`

```sh
mule exec [--host H] [--cwd D] [--max-secs S] -- <cmd>
```

`ssh host cmd` semantics over mule's private connection, for a caller that needs
the answer now and must not hold the default master. The command is a normal
detached job, polled every 100ms (settling at 1s); when it ends, its stdout goes
to your stdout and its stderr to your stderr, and the job is removed. Exit code
is the command's own.

- **Streams are separate**, unlike `run`. Parse stdout; read the reason from
  stderr's first line.
- **No id and no hints.** stderr is the command's plus mule's *fatal* errors:
  the lock-wait notice, the `max_running` warning and the dispatch-pattern
  warnings are all suppressed.
- **Exit 7** means the output is incomplete: truncated at `max_log_bytes`, or
  the command left a process (`sleep 30 &`) holding the job's stdout, so the log
  cannot be known complete (mule waits at most 5s of wall clock to find out).
  Never parse it as complete. exec is for control-sized output: the whole log
  comes back in one locked read, so use `run` and `tail` for anything big.
- **Exit 1** also covers a job mule could not remove (a session or state left
  on the host): the command's own code is never returned beside a leftover job.
- **SIGHUP/SIGINT/SIGTERM stop the job** (exit 129/130/143), through a detached `mule rm`
  that queues like any caller and retries for up to 10 minutes. Give it a moment before you tear the machine
  down. `run --wait` is the opposite: its job continues.
- **Exit 3** is still "no master: stop and ask a human".
- **A command whose dispatch script passes 112 KiB** (the dispatch size cap, set
  below Linux's 128 KiB per-argument limit; roughly 80KB of command)
  exits 1 with `command too large for one dispatch (<n> bytes of remote script,
  limit <m>)`, before any ssh call. Put big payloads in a file and run that.
- **`--no-detach`** stops the job from exec itself on a signal: one `mule rm`
  attempt, then the exit, with every process left in exec's process group.
  For a caller that kills the group after a grace (a plugin host), where a
  detached child would be a leak. The stop is not retried and can outlast
  that grace, so a killed exec means the job may still run.

```sh
mule exec --cwd ~/repo -- git rev-parse HEAD
mule exec -- env K=v sh -c 'echo "$K"'      # several words: quoted for you
```

## Hosts

Every job verb accepts `--host`; with one configured host you can omit it.
`mule host list` reports which configured hosts have a master (`ssh -O check`
only). `mule host info` probes OS, cores, RAM, and GPU and takes the lock.

Every mule ssh goes through mule's master or fails: it never falls back to a
fresh connection (`ProxyCommand=/usr/bin/false`). If the master refuses a
session that something outside mule's queue still holds -- typically the ssh of
a mule process that was killed mid-call -- the verb waits its turn again, at the
back of the queue, for up to 12s, and only then fails with "the master is down
or its one slot is held" (exit 1; `rm` keeps the job). A refusal is retried only
when ssh itself reports it and the command printed nothing, so a command is
never run twice. Waiting for the queue has no limit of its own: if you need a
hard wall-clock bound on a mule verb, impose it yourself (mu runs its stop
command under a 20s timeout). `ssh -O check` is bounded at 2s, so a stopped
master reads as down rather than hanging the verb.

## Exit codes

| Code | Meaning |
| --- | --- |
| 0 | mule operation or job succeeded |
| 3 | No SSH control master: a human must open it (see the README). `run --lease`: only at startup |
| 4 | Timed out waiting (`wait --timeout`); the job keeps running |
| 5 | Orphaned job: no exit code will arrive (`run --lease` too) |
| 6 | Connection dropped while waiting; wait again |
| 7 | `exec` only: output incomplete (truncated at `max_log_bytes`, or a leftover process still holds the job's stdout) |
| 8 | `--strict-codes` only (`exec`, `wait`, `run --wait`): the command's 3 or its own 8 (ambiguous) |
| *n* | `wait`, `run --wait`, `run --lease` and `exec` return the job's own exit code, including 124 (hit `--max-secs`) and 137 (`mule kill`) |
| 129/130/143 | `exec` and `run --lease` after SIGHUP/SIGINT/SIGTERM: the job was stopped |

`mule --help` carries the same table.

### Telling a command's 3 from mule's 3

By default both are 3. Pass `--strict-codes` to `exec`, `wait` or `run --wait`
and a command's own 3 comes out as **8**, with `mule: command exited 3
(remapped to 8)` on stderr; 3 then always means mule's handback in `exec`,
`wait` and `run --wait` (no master: stop and ask a human). All other codes are
unchanged, so a command that itself exits 8 also reads 8: 8 is ambiguous. The
stderr note is diagnostic only, is not proof of the original status, and may
not be the first line (`run --wait` can print dispatch notices before it). Rely
only on '3 = handback'.

## Local helpers (`ssh`, `ps`, `mule`)

mule snapshots `PATH` once on entry and resolves each local helper (`ssh`,
`ssh-add`, `ps`, `mule`) to the first executable regular file, once per
process. This matters on macOS with a long `PATH`: a bare `ssh` under a
70-entry `PATH` made 54 failed spawn attempts per check; resolving once cut
200 checks from 10,800 spawn attempts to 200. Empty and relative `PATH`
entries still resolve (relative to the starting directory), `PATH` changes
after start are not seen, and a missing `ssh` still exits 3 with the same
message from commands that need a live master; `mule host list` instead reports
the host down and succeeds. The bundled `mule` and cleanup executable beside the running binary
come before `PATH`. Remote commands are not resolved this way.

## As a mu plugin: `mu-plugin-mule`

`mu-plugin-mule` is a second binary in this package. It serves
[mu](https://github.com/mu-crew/mu)'s plugin protocol (v1) over stdio, so a mu
host can use mule for its link and its bound lease jobs instead of using
shell templates for them. The plugin looks for `mule` beside itself, then on
`PATH`.

```sh
mule init
mu plugin list
mu host add box --plugin mule --fetch '<your fetch command>'
mu host check box
```

`mule init` writes the commented config template only if missing and prints
the master command for every configured host without opening any masters. It
skips registration if `mu` is not on `PATH`. Rerun it after upgrading mule:
it re-registers when the plugin reports a new version or schema digest and
leaves a matching registration alone. It never creates mu host profiles.

mu requires the executable to be named `mu-plugin-mule`, owned by you and
not writable by others. If the install is root-owned, use a user-owned
install or follow the printed commands to copy both binaries into a private
directory and run `init` there. Refresh that copy after upgrading.

The mu host's name is the mule host's name. `mule init --config PATH` seeds
and uses that config instead of the default and records its absolute path in
the plugin registration. To register manually, use
`mu plugin add mule -- /abs/path/to/mu-plugin-mule [--config PATH]`. `init`
will not replace an existing registration with different arguments.

`init` exits 0 on success (including when mu is absent), 1 on a local setup
error such as invalid config, and 9 when plugin registration fails.

When switching an existing profile, drain, collect and close its jobs before
removing and re-adding it. Live template jobs are not converted.

The **link** seam has `exec` served by
`mule exec --no-detach --strict-codes`. `fetch` is false (mule has no fetch, so
the host keeps its own `--fetch` template), `slots` is `self` (mule's ticket
lock arbitrates; mu holds no channel), `quiescence` is `provable`, and
`remoteOnTimeout` is `unknown` (the stop `exec` runs on a cancel can outlast
mu's grace).

How results map:

| `mule exec` | link result |
| --- | --- |
| 3 (no master) | hand-back: a `human` error carrying the `ssh -MNf` command. The script did not run |
| any other code | the command's result with that code. A command's own 3 reads 8 (see [Telling a command's 3 from mule's 3](#telling-a-commands-3-from-mules-3)); a 7 mule reports as truncation comes with `partial: true`, a command's own 7 does not |
| no answer by `timeoutMs` | `exitCode: null`; the `mule` child is sent SIGTERM, then SIGKILL after 3 s |

`host check`'s reach phase runs `mule host info --json` (one locked session,
no job): a master that is up is a passing `master` step, followed by a `tmux`
step that passes only if `tmux -V` ran on the remote shell; a master that is
down is a `handback` step naming the command to run. The plugin never opens a
master itself.

Limits and fences:

- `partial: true` when mule cut the output (`max_log_bytes`, or a process still
  holding stdout). The plugin learns this from a file mule writes
  (`MULE_PARTIAL_FILE`), not from exit 7, which a command can also return. It
  is also set when the plugin shortens a stream to keep the JSON reply under
  mu's 1 MiB frame (measured after escaping: a NUL is six bytes).
- A request whose `deadline` has passed is refused unstarted, and a live
  deadline shortens `timeoutMs`.
- The handshake `instance` is `mule-<hash>` over the config and resolved host
  targets and sockets. RPCs use that handshake's private copy. A lease view
  and its detached cleanup keep the selected host from the same snapshot,
  even after the plugin exits. To pick up an edit, start a new plugin session;
  mu's instance fence prevents recovery against a retargeted backend.
- `$/cancel` before dispatch means nothing runs; on stdin EOF the plugin stops
  the in-flight RPC children and their `ssh` helpers. The bound lease is
  run by its own view, not by the RPC process.

### Lease jobs

`job.start` returns the interactive launcher, not a job ID. The view runs
`mule run --lease --key`, using mu's command, working directory and remote
job environment. For a pi agent mu uses its built-in ctl channel; the view
forwards the socket pair in the job request. Closing the view starts the usual
lease cleanup. There is no detached view mode.

The carrier writes the ID to mu's local handle file before dispatch. A file
can be read while the SSH master is down. Exact lookup by key also finds a
claim made before the file was published, without starting anything; a miss
alone is not proof that no view could still dispatch. Attach runs the same
lease carrier in attach mode and does not rerun the command.

Status and stop use the handle. Status distinguishes a running job, a job
with an exit code, a proven missing or orphaned job, and a state mule cannot
verify. Stop acknowledges only after `mule rm` proves the job and its sessions
are gone. A missing master is a hand-back, not proof of loss or stop. Open the
master as directed before retrying close.
