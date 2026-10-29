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
id=$(mule run --tui 'pi-meta --pi-meta-no-solo --approve')
mule tail "$id"                 # current screen, or final saved screen
mule tail --transcript "$id"    # raw terminal transcript, explicitly
```

TUI dispatch prints pasteable `murmur pick`, `mu agent spawn`, and
`mule kill --rm` commands on stderr. A mu worker uses `run --lease` instead
(below): its pane is the job's life, not a viewer. `tail -f` refuses TUI jobs because a redraw
stream is not a useful log; use the screen or attach through murmur instead.

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

For a local pane whose life must be the job's: a mu remote worker, run by mu's
lease profile. It is `run --tui` plus a foreground holder, and holds no session
in between: a probe per tick (1s settling to 5s) over mule's own connection.

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
  cannot be handled: the job keeps running and `F` names it.
- **The pane shows** a status line (job, host, state, forwards, the
  `murmur pick --all  # select mule-<id>` attach command and the stop command)
  and mirrors the remote screen for 30s after the first forward, then only on
  a change of state and at exit. Keystrokes are not forwarded; attach through
  murmur to interact.

```sh
mule run --host dev --lease --human --cwd ~/w --job-file /tmp/w1.job \
  --forward-unix /tmp/w1.sock:/home/me/.mu/w1.sock -- env K=v sh -c 'pi'
```

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
  that queues like any caller. Give it a moment before you tear the machine
  down. `run --wait` is the opposite: its job continues.
- **Exit 3** is still "no master: stop and ask a human".

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
| *n* | `wait`, `run --wait`, `run --lease` and `exec` return the job's own exit code, including 124 (hit `--max-secs`) and 137 (`mule kill`) |
| 129/130/143 | `exec` and `run --lease` after SIGHUP/SIGINT/SIGTERM: the job was stopped |

`mule --help` carries the same table.
