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

`ssh host command` holds a session channel for the command's lifetime. With `MaxSessions 1`, five concurrent calls produced **1 success out of 5** without a gate and **5 out of 5** with a gate. A refused channel can appear as `Permission denied (keyboard-interactive)`, even though the credentials are valid.

Coop uses its own `ControlPath`, serializes every channel-opening call, and detaches the job before returning. Other tools use other connections, so neither side takes the other's session slot.

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

Every job verb accepts `--host`. `poll` and `wait` do not print job output; use `tail`. A one-shot `tail` prints the last **64KB** by default; use `--all` or `-n LINES` to choose another range.

See [SPEC.md](SPEC.md) for measurements, invariants, failure behavior, and the local `sshd` reproduction.
