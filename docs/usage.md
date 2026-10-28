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
`mule kill --rm` commands on stderr. `tail -f` refuses TUI jobs because a redraw
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

## Hosts

Every job verb accepts `--host`; with one configured host you can omit it.
`mule host list` reports which configured hosts have a master (`ssh -O check`
only). `mule host info` probes OS, cores, RAM, and GPU and takes the lock.

## Exit codes

| Code | Meaning |
| --- | --- |
| 0 | mule operation or job succeeded |
| 3 | No SSH control master: a human must open it (see the README) |
| 4 | Timed out waiting (`wait --timeout`); the job keeps running |
| 5 | Orphaned job: no exit code will arrive |
| 6 | Connection dropped while waiting; wait again |
| *n* | `wait` and `run --wait` return the job's own exit code, including 124 (hit `--max-secs`) and 137 (`mule kill`) |

`mule --help` carries the same table.
