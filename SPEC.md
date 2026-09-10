# coop design

`coop` dispatches detached jobs through a private SSH control connection. A caller gets a six-hex-digit job ID, then polls, waits, or reads the remote artifact without holding an SSH session for the job's lifetime.

This document records the constraints, measurements, and rejected alternatives behind that design.

## Problem

A long `ssh host command` consumes a session channel until the command exits. On a host with `MaxSessions 1`, every concurrent call on that connection can fail with a misleading authentication error such as `Permission denied (keyboard-interactive)`.

Polling without a completion artifact also fails poorly: callers guess sleep intervals, time out, and cannot distinguish a finished process from a lost one. `coop` instead returns immediately after detached dispatch and treats the remote `rc` file as the completion signal.

Five concurrent SSH calls to a capped connection produced the defining result: **1 of 5 succeeded without a gate; 5 of 5 succeeded with one.**

## Measurements

| Claim | Result |
| --- | --- |
| A gate is necessary | Five concurrent calls: **1 of 5** succeeded ungated, **5 of 5** gated. |
| A retrying mutex is unfair | With four looping callers, the worst wait was **6 times the work**. One **0.25s** operation caused a **4.14s** wait. |
| A second master is a second connection | Two masters had distinct PIDs and carried concurrent traffic. |
| Connection isolation works | While `sleep 12` starved the default connection, the private connection answered immediately. |
| Isolation works both ways | While the private connection was held for **14s**, three probes on the default connection succeeded. |
| The tmux server is private | A session on `tmux -L coop` did not appear in `tmux ls`. |
| Dispatch is detached | Dispatch returned in **0s** while a full test suite kept running. |
| The master needs a terminal | A background `ssh -MNf` could not prompt for a hardware token. |
| Old artifacts misreport state | A leftover `rc` made a running job appear finished with the old code. |
| Duplicate tmux names are unsafe | A second `tmux new-session` printed `duplicate session` while its wrapper reported success. |
| Empty tmux config matters in tests | One measured server start took **3.5s** with personal config and **0.02s** with `-f /dev/null`, a **175x** difference. |
| Dispatch is cheap but not free | **125ms** per dispatch against **33ms** for a bare `ssh` over an existing master: ~65ms local startup, ~30ms round trip, ~25ms tmux. |
| An unpinned tmux config is expensive | A cold server sourcing a personal `~/.tmux.conf` took **4518ms** to start against **30ms** with `-f /dev/null`, a **150x** difference. |
| Lock poll interval is paid per handoff | Four callers, five rounds, 50ms of work each. A 20ms poll gave a p50 of ~275ms and a worst case of **443ms**; a 5ms poll gave ~237ms and **261ms**. |

## Three invariants

### 1. Use a private SSH connection

Each host uses `~/.ssh/coop/<host>.sock` by default. `MaxSessions` applies per connection, not per user. Other SSH traffic cannot take coop's session slot, and coop cannot take theirs.

This was measured in both directions. Two masters ran under different PIDs. A `sleep 12` on the default connection did not block coop, and a **14s** hold on coop's connection did not block three default probes.

### 2. Use a private tmux server

Jobs run under `tmux -L coop`. They do not appear in the user's default `tmux ls` output.

### 3. Never carry long work over SSH

Every SSH call is a detached dispatch or an artifact read. `run --wait` polls; it never attaches to the job. The private channel is not available to `rsync`, `git fetch`, or other bulk transfers.

An **18MB** transfer that held this channel for **30s** would recreate the original failure: every poll and tail would queue behind it.

## Contention and fairness

Only coop callers share coop's connection. Every SSH call takes the per-host ticket lock except `ssh -O check`, which measured **0s** and opens no session channel. This includes `run`, `poll`, `wait`, `tail`, `ls`, `kill`, and `rm`; two simultaneous polls can hit the same cap as two dispatches.

The lock lives at `~/.local/state/coop/<host>.lock`. A ticket lock gives bounded, first-come-first-served progress. A retrying mutex did not: four callers produced a worst wait of **4.14s** for **0.25s** of work. The lock records each waiter's PID and the holder PID, then skips either when that process is gone. After about **5s**, a waiter reports its ticket position and the holder PID but keeps waiting.

The lock poll interval is **5ms**. With four callers, five rounds each, and **50ms** of work per round, the workload has **1s** of serialized work and an ideal peak wait near **200ms**. A **20ms** poll interval had a roughly **275ms** median and **443ms** worst wait, and exceeded a **600ms** bound once in 15 runs. A **5ms** interval had a roughly **237ms** median and **261ms** worst wait across six runs.

## Job identity and remote state

`run` generates a six-character lowercase hexadecimal ID. Generated IDs avoid user-chosen tmux name collisions and stale state from reused names. Hex also avoids `:` and `.`, which tmux reserves in target syntax.

Each job lives under `~/.local/state/coop/jobs/<id>/`:

```text
cmd    command as entered, for ls
log    merged stdout and stderr
rc     exit code, written only after completion
```

The remote artifact is the only source of truth. There is no local job index. The `rc` file is durable across dropped connections and distinguishes these states:

| State | Meaning |
| --- | --- |
| `running` | The tmux session exists and `rc` does not. |
| `done` | `rc` exists. |
| `orphan` | The session is gone and `rc` does not exist. |

`kill` writes `137` if `rc` is absent before destroying the session. An orphan therefore means that coop did not stop the job normally. `rm` destroys the session if needed, then removes the state directory.

A job wrapper has this shape:

```sh
tmux -L coop new-session -d -s coop-<id> \
  'cd <cwd> && printf %s <base64> | base64 -d | sh > <state>/log 2>&1; \
   echo $? > <state>/rc'
```

Both the command and a user-supplied working directory are base64-encoded. They cross the local argument parser, the remote shell, tmux argument parsing, and `sh`; layered quoting reopens injection and expansion bugs at each boundary. The `cmd` file is a display copy, not executable input.

## Shell and working directory

Jobs use a non-login, non-interactive shell. Remote profile files do not run, so commands do not inherit tools added to `PATH` by those files. This avoids host-specific behavior where an interactive command works but a dispatched command fails.

`run --cwd <dir>` sets the working directory. Otherwise coop uses the host's `default_cwd`, then the remote home directory. A failed `cd` fails the job instead of running in the wrong directory. Callers that need an environment manager must source it in the command.

## Configuration

The first run without a config file writes a commented template to the default
path and exits non-zero, naming the file and the next step. A bare
`No such file or directory` names a path but not what belongs in it, which
leaves a first-time caller nowhere; a file that already exists is something to
edit.

Every host in the template is commented out, so it configures nothing: coop
cannot know a host name, and inventing one produces confusing failures against a
target that does not exist. An explicit `--config` is never seeded, since a
missing path there is the caller's typo to see.

The default file is `~/.config/coop/config.toml`. Host entries are user intent, so a text file is easier to edit and diff than a state database.

```toml
[hosts.build]
target = "build"
socket = "~/.ssh/coop/build.sock"
tmux_socket = "coop"
max_running = 4
default_cwd = "~/work/project"
keep_days = 14
```

Only the section and target are required. The socket defaults to `~/.ssh/coop/<name>.sock`, the tmux socket to `coop`, `max_running` to **4**, and `keep_days` to **14**.

Every job verb accepts `--host`. The flag is optional when exactly one host is configured.

## SSH master

Coop requires an existing control master and never opens one:

```text
coop: no control master for build
  run: ssh -MNf -S ~/.ssh/coop/build.sock -o ControlPersist=8h build
```

Opening a master can require a terminal for a hardware token. The explicit command costs one token tap per `ControlPersist` window; a background process cannot perform that prompt. Missing masters exit with status **3**.

### Exit 3 is a request for a human, not a transient error

The distinction matters for automated callers, which are the primary users. Every other failure is something a program can reason about; this one requires a physical act that no amount of retrying produces.

So exit 3 has its own code, and the contract for a caller receiving it is to **stop and escalate to an operator**. Three specific responses are wrong:

| Response | Why it fails |
| --- | --- |
| Retry, or sleep and retry | A master does not appear without the human act. The wait is unbounded. |
| Run `ssh -MNf` from the agent | Measured: it cannot prompt for a token without a terminal, and fails opaquely from a background call. |
| Fall back to `ssh host command` | Holds a session channel for the job's lifetime — the exact failure this design removes — and starves every other tool on a capped connection. |

One tap unblocks every job for the life of the `ControlPersist` window, so the escalation is cheap and rare. An agent that improvises instead converts a ten-second interruption into a broken host.

## Commands and output

```text
coop run [--host H] [--cwd D] [--wait] [--no-tail] <cmd>
coop poll <id> [--host H] [--json]
coop wait <id> [--host H] [--timeout S]
coop tail <id> [--host H] [-f] [--all | -n LINES]
coop ls [--host H] [--all] [--json]
coop kill <id> [--host H]
coop rm <id> [--host H]
coop host list [--json]
```

`poll` prints `running`, `orphan`, or the exit code. `wait` prints no job output and exits with the job's code. `run --wait` prints the ID first, follows the log, and exits with the job's code. Printing the ID first preserves the recovery handle if a later read fails.

`tail` writes raw bytes because lossy UTF-8 conversion would corrupt the artifact. Standard output and standard error stay merged to preserve their order. A caller that needs separate streams can redirect them inside the submitted command.

A one-shot `tail` reads only the last **64KB** by default. `--all` reads the full log, and `-n` reads the requested number of lines. A **200MB** read would hold the only channel slot and defeat the design. Follow mode reads only bytes added since its previous offset.

`poll`, `wait`, and follow mode use one probe shape that returns `rc`, session presence, log size, and requested bytes. Follow starts at a **1s** interval, doubles to at most **5s** while quiet, and resets to **1s** when output arrives. `--wait --no-tail` polls state, then reads the full log once.

The stable coop-specific exit codes are:

| Code | Meaning |
| --- | --- |
| 3 | No SSH control master. |
| 4 | Wait timed out. |
| 5 | Job is orphaned. |
| 6 | Connection dropped while waiting. |

A refused session channel is classified instead of exposing the misleading authentication message. A dropped connection during a wait reports that the detached job continues and prints `coop tail <id>` as the recovery command.

## Listing, load, and cleanup

`ls` checks configured hosts sequentially because every host call takes its own lock. It reports unreachable hosts instead of silently omitting them. Finished jobs appear only with `--all`.

`max_running` is advisory. Dispatch counts sessions in the same round trip and warns after starting a job when the count exceeds the configured cap. A preflight count would double the round trips for a warning that does not block work.

`run` removes only completed jobs older than `keep_days`, which defaults to **14**. It keeps running jobs and orphans because their logs can still be needed.

## Failure behavior

- A missing master exits **3** and prints the command that opens it.
- A timeout exits **4** while the remote job continues.
- An orphan exits **5** because no `rc` can arrive.
- A dropped connection during `wait` exits **6** while the remote job continues.
- A refused channel reports a busy or down master rather than raw SSH authentication text.
- An unreachable configured host remains visible in `ls` diagnostics.

## Reproduce the channel cap locally

A non-root `sshd` on loopback reproduces `MaxSessions 1` without PAM or a hardware token. The following recipe was exercised on macOS **26.6.2** with **OpenSSH_10.3p1**. It also works with paths adjusted for another OpenSSH installation.

```sh
tmp=$(mktemp -d)
port=$(python3 - <<'PY'
import socket
s = socket.socket()
s.bind(("127.0.0.1", 0))
print(s.getsockname()[1])
s.close()
PY
)
ssh-keygen -q -t ed25519 -N '' -f "$tmp/host_ed25519"
ssh-keygen -q -t ed25519 -N '' -f "$tmp/id"
cp "$tmp/id.pub" "$tmp/authorized_keys"
chmod 600 "$tmp"/host_ed25519 "$tmp"/id "$tmp"/authorized_keys
cat > "$tmp/sshd_config" <<EOF
Port $port
ListenAddress 127.0.0.1
HostKey $tmp/host_ed25519
PidFile $tmp/sshd.pid
AuthorizedKeysFile $tmp/authorized_keys
MaxSessions 1
UsePAM no
StrictModes no
PasswordAuthentication no
KbdInteractiveAuthentication no
PubkeyAuthentication yes
EOF
chmod 600 "$tmp/sshd_config"
/usr/sbin/sshd -f "$tmp/sshd_config" -E "$tmp/log"

opts=(-F /dev/null -o StrictHostKeyChecking=no \
  -o UserKnownHostsFile=/dev/null -o IdentitiesOnly=yes \
  -i "$tmp/id" -p "$port" "$(id -un)@127.0.0.1")
ssh "${opts[@]}" -MNf -S "$tmp/master.sock"
ssh "${opts[@]}" -S "$tmp/master.sock" -O check
ssh "${opts[@]}" -S "$tmp/master.sock" 'sleep 6' & held=$!
sleep 1
ssh "${opts[@]}" -S "$tmp/master.sock" true
ssh "${opts[@]}" -S none true
wait "$held"
kill "$(cat "$tmp/sshd.pid")"
rm -rf "$tmp"
```

The check reports `Master running (pid=NNN)`. While `sleep 6` occupies its only session, the same socket reports `Session open refused by peer`; SSH may then open a fallback connection. The explicitly separate connection succeeds. Coop treats the refusal text as a busy channel even if SSH's fallback succeeds.

The important setup details are a free ephemeral port, mode `600` on keys and config, `StrictModes no` for a temporary directory, quoted option arrays, and cleanup through `PidFile`. A previously unquoted option string made SSH parse `-i` as part of the config filename, and the probe passed without testing the intended connection.

## Test layers

The design separates four kinds of evidence:

| Layer | Coverage | Requirement |
| --- | --- | --- |
| 1 | Logic through a fake `Transport`: config, IDs, scripts, state mapping, and lock ordering | None |
| 2 | The generated wrapper against `tmux -L coop-test-<pid> -f /dev/null` | `tmux` |
| 3 | Channel isolation, gated concurrency, dispatch latency, and bounded wait | Local `sshd` recipe above |
| 3b | The 2FA-specific error text and network latency | A capped host and a live master |

Layer 2 uses a private tmux socket and no personal config. It catches quoting, `rc` writes, kill status **137**, and shell exit-code propagation that a fake transport cannot test. Layer 3 reproduces the channel cap without privileged setup. Layer 3b is the only layer that needs a token tap.

## What belongs in a job

Dispatch costs ~125ms against ~33ms for a bare `ssh` over an existing master, so the tool earns its overhead on **duration**, not frequency:

- **Worth it:** anything holding the channel for a noticeable time — a test suite, a build, a large transfer — anything that must survive a dropped connection, and any group of long commands that would otherwise contend.
- **Not worth it:** sub-second commands such as a `rev-parse`, a status poll, or a state collector. There is no long hold to remove, so the 125ms is pure cost. A refused channel on a cheap idempotent command is better retried than routed around.
- **Impossible:** anything needing a live terminal, and anything with an endpoint on the calling machine.

The endpoint rule is about topology, not about which program runs. A transfer whose endpoints are both remote — one host directory to another, or the host to a third machine — is an ordinary job. The same command aimed back at the dispatcher is not, because a job cannot reach the machine that dispatched it: a laptop behind NAT has no inbound route, which is also why collection is always orchestrator-pull. So `rsync host:/data ~/local` is not a job at all, and `coop run 'rsync /data host2:/data'` is a perfectly good one.

**Threshold: roughly one second**, and the reasoning matters more than the number. Holding a capped channel is an externality: the cost falls on `git fetch`, a collector, a transfer — never on the caller doing the holding. Judging by whether 125ms of overhead feels worth it is therefore the wrong test and yields thresholds far too generous; an earlier draft of this section said ten seconds, which is ten times longer than anything else on the host should be made to wait. The right question is how long the rest of the host may be broken.

Below a second, a direct call is cheaper and a refused channel is better retried than routed around.

## Rejected alternatives

### Hold one SSH session for the job

This consumes the only channel on a capped connection. Polling keeps each channel use short and lets the job survive a dropped client.

### Attach for live streaming

An attachment is another long-lived SSH session. Follow mode polls by byte offset instead.

### Use a retrying mutex

Repeated acquisition was unfair: one **0.25s** operation caused a **4.14s** wait. Ticket order bounds the wait and handles dead holders and waiters by PID.

A dead *waiter* matters as much as a dead holder. A caller that claims a ticket and exits before its turn writes no holder file, so nothing identifies the gap; the queue then stops at that number permanently. Each waiter therefore records its PID at claim time, and a successor steps over a ticket whose waiter is gone.

### Accept a job ID from the caller unchecked

A job ID reaches a remote path, a tmux target, and a shell script. An unvalidated ID is therefore remote command execution: `poll 'x$(...)y'` substitutes on the host. IDs parse at the boundary as exactly six lowercase hex digits, which also excludes the `:` and `.` that tmux target syntax reserves.

The same applies to `tmux_socket`, which comes from configuration and is interpolated unquoted. It is restricted to a filename component.

### Queue jobs or assign priorities

The lock protects sub-second SSH operations, while jobs run concurrently outside it. Priorities add little to a queue of short probes. A persistent queue also needs a daemon to notice free slots; coop has no process between invocations. If host load becomes a problem, `max_running` supplies backpressure, and another private control connection supplies another channel.

### Manage the SSH master

A hardware-token prompt needs a terminal. Coop cannot open the master reliably from a background call, so it prints the command instead.

### Use a login shell

Remote dotfiles would make job behavior depend on interactive host configuration. Commands must source any required environment explicitly.

### Quote commands through every shell layer

Each parser adds another opportunity for expansion or injection. Base64 makes the command and working directory inert until the final wrapper decodes them.

### Let users choose job names

Duplicate tmux session names can report success from an outer wrapper even when no new job starts. Reused names can also inherit stale `rc` files. Generated IDs avoid both failures.

### Store a local job index

A local index would duplicate remote identity and become stale after disconnects or use from another client. The remote directory remains the sole record; removing a host from config makes its jobs unreachable until the host is configured again.

### Store jobs in `/tmp`

`/tmp` is shared and cleared unpredictably. It is also where the stale-`rc` failure was reproduced. A user-owned state directory gives cleanup and listing one stable root.

### Split stdout and stderr

Two logs require two offsets and extra reads while losing causal interleaving. The submitted command can redirect either stream when separation matters.

### Read every complete log by default

A **200MB** transfer would monopolize the channel. The **64KB** default bounds that cost; `--all` remains explicit.

### Lend coop's connection to local transfer tools

A bulk transfer violates the rule that every use of the private channel is short. Other tools can open their own master on another `ControlPath`; two masters were measured carrying traffic concurrently.

### Add a local scheduler or supervisor

Detached tmux and durable artifacts provide survival and completion without a daemon. Reconnect logic, retries, and placement policy would turn a dispatcher into an orchestrator.

## Deferred choices

- Add a last-log-line option to `ls` only if its extra read proves useful.
- Add boot IDs only if distinguishing a reboot from a manually killed session becomes necessary.
- Make the **1s to 5s** cadence configurable only after measurements justify it.
- Make the **64KB** tail cap configurable only after measurements justify it.
- Reconsider split streams only for a caller that cannot redirect its own command.
