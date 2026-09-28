# Why mule exists, and which jobs belong in it

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

Measured on the capped host: while a multi-minute job ran through mule, a
concurrent plain `ssh dev` still succeeded. That coexistence is the promise:
long work uses mule's private connection while ordinary tools keep theirs.

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

Design, invariants and the local `sshd` reproduction: [SPEC.md](../SPEC.md).
