# coop

Long remote commands occupy an SSH session; on a `MaxSessions 1` connection, concurrent calls fail with misleading authentication errors. `coop` detaches each job under a private tmux server, returns an ID, and reserves a separate gated SSH connection for short dispatches and artifact reads.

## 30-second demo

```console
$ coop run 'printf "starting\n"; sleep 2; printf "done\n"'
f61454
$ coop poll f61454
running
$ coop wait f61454; echo $?
0
$ coop tail f61454
starting
done
```

`run --wait` combines dispatch, live output, and the job's exit status.

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

coop opens a *second* connection on its own `ControlPath` and uses it only for
sub-second calls -- dispatch the job detached, then read the artifacts it leaves
behind. The channel is free again before the work starts, and other tools are on
other connections, so neither side takes the other's slot.

```
  your machine                        host (MaxSessions 1)
  +--------------+                    +----------------------+
  | coop run make|--- 125ms --------->| tmux -> make (12 min)|
  |              |<-- job id ---------|          |           |
  | git fetch    |===================>|          v           |
  | rsync        |===================>|   log, rc on disk    |
  | collector    |===================>|          |           |
  | coop wait id |--- 125ms --------->|<---------+  reads rc |
  +--------------+                    +----------------------+

  ---> coop's own connection    ===> everyone else's, uncontended
```

## Which jobs belong here

A job runs **on the host**, detached, with no terminal and no route back to you.
That decides the fit, not the program:

```
  coop run 'rsync /data/a/ /data/b/'          OK   both ends on the host
  coop run 'rsync /data/ other-host:/data/'   OK   host to a third machine
  coop run 'rsync /data/ your-laptop:/data/'  NO   needs a route back to you
  rsync host:/data/ ~/local/                  NO   not a job: one end is here
```

The last two are the same mistake. A job cannot reach the machine that
dispatched it, because a laptop behind NAT has no inbound route -- which is why
collection is always orchestrator-pull. Give `rsync`, `scp` and `git push` their
own control master on their own `ControlPath`: two masters were measured
carrying traffic concurrently, so they take nothing from coop and coop takes
nothing from them.

Dispatch costs about **125ms**, against **33ms** for a bare `ssh` over an
existing master.

| Command | Use coop? |
| --- | --- |
| Test suite, build, long-running script | Yes. Minutes of held channel starves every other tool. |
| Anything that must survive a dropped connection | Yes. That is the only way to get an exit code back later. |
| Several long commands at once | Yes. Ungated, 1 of 5 concurrent calls succeeded. |
| A transfer between the host and a *third* machine | Yes. Both endpoints are remote. |
| `git rev-parse`, a status poll, a state collector | No. Already sub-second, so 125ms buys nothing. |
| A transfer to or from *this* machine | No. A job cannot reach its dispatcher. |
| A command needing a live terminal | No. Jobs are detached and read no input. |

**Threshold: roughly one second.** The cost of a long call is not paid by you --
it is paid by every other tool that needs the channel while you hold it. So the
question is not "is 125ms of dispatch worth it to me" but "how long am I willing
to break `git fetch` for". One second is already a long outage.

Below that, a direct call is cheaper, and a refused channel on a short
idempotent command is better retried than routed around.

## Install and configure

Build and install with Cargo:

```sh
cargo install --path .
```

The first run without a config writes a commented template to
`~/.config/coop/config.toml` and tells you to edit it. The minimum is two lines:

```toml
[hosts.build]
target = "build"
```

The section name is the value for `--host`. With one configured host, you can omit `--host`. The default socket is `~/.ssh/coop/build.sock`.

Coop requires an SSH control master and does not open it. Open it before the first job:

```sh
ssh -MNf -S ~/.ssh/coop/build.sock -o ControlPersist=8h build
```

If authentication uses a hardware token, this costs one tap per `ControlPersist` window. A job command that needs the missing master exits **3** and prints the command above.

### Exit 3 needs a human. Stop and ask.

Opening the master can require a physical act — touching a hardware key, typing a one-time code. No program can do that for you, which is why `coop` refuses to try instead of failing in a way that looks like something else.

If you are an automated caller and you get exit 3, **stop and ask the operator to run the printed command.** Do not:

- retry, or sleep and retry. The master does not appear on its own.
- run `ssh -MNf` yourself. It needs a terminal to prompt on and fails silently from a background process.
- fall back to `ssh host command`. That holds a session channel for the whole job, which is the failure this tool exists to remove, and on a capped host it breaks every other tool's connection.

Exit 3 is a distinct code so that a script can recognise this one case and escalate rather than improvise. One tap unblocks every job for the life of the window.

## Operational details

Three details from `coop --help` matter:

- Coop never opens the SSH master because a background call cannot handle a hardware-token prompt.
- Jobs use a non-login, non-interactive shell. Source environment setup in the command when needed.
- Standard output and standard error share one log. Redirect inside the command when you need separate files.

`coop ls` shows the last 24 hours plus anything running or orphaned; `--all` reaches further back. Logs are capped at 100MB per job (`max_log_bytes`), and a truncated log says so.

Every job verb accepts `--host`. `poll` and `wait` do not print job output; use `tail`. A one-shot `tail` prints the last **64KB** by default; use `--all` or `-n LINES` to choose another range.

See [SPEC.md](SPEC.md) for measurements, invariants, failure behavior, and the local `sshd` reproduction.
