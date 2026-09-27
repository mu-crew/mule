# mule

Long remote commands occupy an SSH session; on a `MaxSessions 1` connection, concurrent calls fail with misleading authentication errors. `mule` detaches each job under a private tmux server, returns an ID, and reserves a separate gated SSH connection for short dispatches and artifact reads.

## Install

```sh
npm i -g @mu-crew/mule
```

The npm package provides prebuilt binaries for Linux x64, Linux arm64, and
macOS arm64. Rust users can install from crates.io instead:

```sh
cargo install mule-cli
```

The binary is `mule`, which is what every command below uses.

From source:

```sh
git clone https://github.com/mu-crew/mule && cd mule && cargo install --path .
```

Needs Rust 1.85 or newer, and `tmux` on the remote host. First run writes a
commented config template to `~/.config/mule/config.toml`.

## 30-second demo

```console
$ mule run 'printf "starting\n"; sleep 2; printf "done\n"'
f61454
$ mule poll f61454
running
$ mule wait f61454; echo $?
0
$ mule tail f61454
starting
done
```

`run --wait` combines dispatch, live output, and the job's exit status.

By default, the job command receives `MU_MANAGED_AGENT=1` and
`MU_AGENT_NAME=mule-<job-id>`. If the caller has a non-empty `MU_WORKSTREAM`,
mule forwards it unchanged. Use `mule run --human <cmd>` to omit all three
variables for an ad-hoc agent.

Commands that need a terminal use explicit TUI mode:

```sh
id=$(mule run --tui 'pi-meta --pi-meta-no-solo --approve')
mule tail "$id"                 # current screen, or final saved screen
mule tail --transcript "$id"    # raw terminal transcript, explicitly
```

TUI dispatch prints pasteable `murmur pick`, `mu agent spawn`, and
`mule kill --rm` commands on stderr. `tail -f` refuses TUI jobs because a redraw
stream is not a useful log; use the screen or attach through murmur instead.

## Why a plain SSH command fails

One connection to the host carries one session channel. A long command holds it
for its whole run, and everything else is refused:

```
  your machine                        host (MaxSessions 1)
  +--------------+                    +----------------------+
  | ssh host make|===================>| make        (12 min) |
  | git fetch    |--X refused         |                      |
  | rsync        |--X refused         | one channel, in use  |
  | collector    |--X refused         |                      |
  +--------------+                    +----------------------+

  the refusal reads as: Permission denied (keyboard-interactive)
  which is about sessions, not credentials
```

Measured with `MaxSessions 1`: five concurrent calls produced **1 success out of
5** ungated, **5 out of 5** gated.

mule opens a *second* connection on its own `ControlPath` and uses it only for
sub-second calls -- dispatch the job detached, then read the artifacts it leaves
behind. The channel is free again before the work starts, and other tools are on
other connections, so neither side takes the other's slot.

```
  your machine                        host (MaxSessions 1)
  +--------------+                    +----------------------+
  | mule run make|--- 125ms --------->| tmux -> make (12 min)|
  |              |<-- job id ---------|          |           |
  | git fetch    |===================>|          v           |
  | rsync        |===================>|   log, rc on disk    |
  | collector    |===================>|          |           |
  | mule wait id |--- 125ms --------->|<---------+  reads rc |
  +--------------+                    +----------------------+

  ---> mule's own connection    ===> everyone else's, uncontended
```

## Which jobs belong here

A job runs **on the host**, detached, with no terminal and no route back to you.
That decides the fit, not the program:

```
  mule run 'rsync /data/a/ /data/b/'          OK   both ends on the host
  mule run 'rsync /data/ other-host:/data/'   OK   host to a third machine
  mule run 'rsync /data/ your-laptop:/data/'  NO   needs a route back to you
  rsync host:/data/ ~/local/                  NO   not a job: one end is here
```

The last two are the same mistake. A job cannot reach the machine that
dispatched it, because a laptop behind NAT has no inbound route -- which is why
collection is always orchestrator-pull. Give `rsync`, `scp` and `git push` their
own control master on their own `ControlPath`: two masters were measured
carrying traffic concurrently, so they take nothing from mule and mule takes
nothing from them.

Dispatch costs about **125ms**, against **33ms** for a bare `ssh` over an
existing master.

| Command | Use mule? |
| --- | --- |
| Test suite, build, long-running script | Yes. Minutes of held channel starves every other tool. |
| Anything that must survive a dropped connection | Yes. That is the only way to get an exit code back later. |
| Several long commands at once | Yes. Ungated, 1 of 5 concurrent calls succeeded. |
| A transfer between the host and a *third* machine | Yes. Both endpoints are remote. |
| `git rev-parse`, a status poll, a state collector | Usually direct. Use mule if refusal can look like success: 8 concurrent bare `rev-parse` polls returned 1 sha and 7 empty results; mule dispatched all 8. |
| A transfer to or from *this* machine | No. A job cannot reach its dispatcher. |
| A TUI command that can live in remote tmux | Yes, explicitly with `run --tui`; interact through murmur or mu. |

**Threshold: roughly one second.** The cost of a long call is not paid by you --
it is paid by every other tool that needs the channel while you hold it. So the
question is not "is 125ms of dispatch worth it to me" but "how long am I willing
to break `git fetch` for". One second is already a long outage.

Below that, a direct call is cheaper when failure is loud. Route through mule
when refusal can be mistaken for a valid empty result.

Measured on the capped host: while a multi-minute job ran through mule, a
concurrent plain `ssh dev` still succeeded. That coexistence is the promise:
long work uses mule's private connection while ordinary tools keep theirs.

## Install and configure

Build and install with Cargo:

```sh
cargo install --path .
```

The first run without a config writes a commented template to
`~/.config/mule/config.toml` and tells you to edit it. The minimum is two lines:

```toml
[hosts.build]
target = "build"
```

The section name is the value for `--host`. With one configured host, you can omit `--host`. The default socket is `~/.ssh/mule/build.sock`.

Mule requires an SSH control master and does not open it. Open it before the first job:

```sh
ssh -MNf -S ~/.ssh/mule/build.sock -o ControlPersist=8h build
```

If authentication uses a hardware token, this costs one tap per `ControlPersist` window. A job command that needs the missing master exits **3** and prints the command above.

### Exit 3 needs a human. Stop and ask.

Opening the master can require a physical act — touching a hardware key, typing a one-time code. No program can do that for you, which is why `mule` refuses to try instead of failing in a way that looks like something else.

If you are an automated caller and you get exit 3, **stop and ask the operator to run the printed command.** Do not:

- retry, or sleep and retry. The master does not appear on its own.
- run `ssh -MNf` yourself. It needs a terminal to prompt on and fails silently from a background process.
- fall back to `ssh host command`. That holds a session channel for the whole job, which is the failure this tool exists to remove, and on a capped host it breaks every other tool's connection.

Exit 3 is a distinct code so that a script can recognise this one case and escalate rather than improvise. One tap unblocks every job for the life of the window.

## Operational details

Three details from `mule --help` matter:

- Mule never opens the SSH master because a background call cannot handle a hardware-token prompt.
- Jobs use a non-login, non-interactive shell, so login profiles do not run and
  a job cannot depend on your dotfiles. Bash is a partial exception: it sources
  `~/.bashrc` even for a non-interactive command over ssh, so a `PATH` set
  there does reach a job. `.bash_profile` — where an environment manager's
  `activate` usually lives — does not run.

  In practice that is enough: put your version manager's shims directory on
  `PATH` in `.bashrc` and jobs resolve the same tool versions a login shell
  would. If you need activation itself, ask for it in the command:
  `mule run 'source ~/.zshrc && npm test'`. There is deliberately no flag for
  it — a login shell measured 83ms against a 125ms dispatch and changed nothing
  but `PATH` length.
- Standard output and standard error are merged into one log, in the order the
  job wrote them; redirect inside your command to separate them. One artifact
  per job is deliberate: two would mean two probe offsets and a lost
  interleaving, to serve a case a redirect already covers.

mule's own flags go **before** the command, because everything after the first
word belongs to the command: `mule run --wait ls`, not `mule run ls --wait`.
The second form sends `--wait` to `ls`, and mule warns when it spots one of its
flags there. Use `--` when the command takes a flag mule also has:

```sh
mule run -- ls --all
```

`mule ls` shows the last 24 hours plus anything running or orphaned; `--all` reaches further back. Its `RUNTIME` column is elapsed time for running jobs, total time for finished jobs, and `-` for orphans, whose runtime is unknowable. The table collapses whitespace and marks commands longer than 80 characters with `…`, keeping each job on one line. Use `mule ls --full` to read a long command in the table, and `mule ls --json` as the machine surface; JSON keeps `age_secs` and adds `runtime_secs`. Logs are capped at 100MB per job (`max_log_bytes`), and a truncated log says so.

Jobs are unbounded by default. Set `max_job_secs` on a host, or pass
`mule run --max-secs S`, to cap remote runtime. A timed-out job records rc
**124**, following GNU `timeout`; `poll`, `wait`, and `ls --json` therefore
distinguish it from an ordinary failure. This is separate from
`mule wait --timeout S`, which only stops the local wait and leaves the job
running.

`mule rm <id>` ends that job if it is still running, then removes its state. `mule rm --all` removes every **finished** job, ignoring `keep_days`, and leaves running jobs and orphans alone. Prefer `mule kill` when you want the exit code recorded as **137** without dropping state; use `kill --rm` to end and discard in one trip.

Every job verb accepts `--host`. `mule host list` reports which configured hosts have a master (`ssh -O check` only). `mule host info` probes OS, cores, RAM, and GPU and takes the lock. `poll` and `wait` do not print job output; use `tail`. A one-shot `tail` prints the last **64KB** by default; use `--all` or `-n LINES` to choose another range.

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

If you genuinely want a pipeline, `set -o pipefail` inside your command keeps
it honest — verified working in the remote `sh` on both macOS and Linux, but
not portable POSIX, so mule will not inject it. Changing the meaning of every
caller's command to fix some of them is not mule's call to make.

See [SPEC.md](SPEC.md) for measurements, invariants, failure behavior, and the local `sshd` reproduction.
