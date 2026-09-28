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
`mule rm --all` removes every **finished** job, ignoring `keep_days`, and leaves
running jobs and orphans alone. Prefer `mule kill` when you want the exit code
recorded as **137** without dropping state; use `kill --rm` to end and discard
in one trip.

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
