# mule

On a host that allows one SSH session per connection (`MaxSessions 1`), a long
remote command holds that session, and every other tool's ssh fails with an
error that looks like bad credentials. `mule` dispatches each job detached under
a private tmux server on the host, returns an ID, and uses its own connection
only for sub-second dispatches and reads. The build runs; `git fetch` keeps
working.

Measured on a capped host: 5 concurrent plain ssh calls, 1 succeeded; through
mule, 5 of 5. [Why, with diagrams, and which jobs belong here](docs/why.md).

## Install

```sh
npm i -g @mu-crew/mule
mule init
```

Prebuilt for Linux x64, Linux arm64 and macOS arm64. Or `cargo install mule-cli`
(Rust 1.88+), or `cargo install --path .` from a clone. The host needs `tmux`.

Run `mule init` after installing or upgrading. It registers the bundled
plugin if `mu` is on `PATH`, but does not create any mu host profiles. mu
requires a plugin you own; use a user-owned install or follow the copy
commands printed by `mule init`.

## Set up a host

`mule init` writes a commented template to `~/.config/mule/config.toml` if it
is missing. The minimum is two lines:

```toml
[hosts.build]
target = "build"
```

The section name is what `--host` takes; with one host you can omit it. Rerun
`mule init` after editing to print the command for each host.

mule needs an SSH control master on its own socket and never opens it. Open
it yourself (this can require a hardware-key tap):

```sh
ssh -MNf -S ~/.ssh/mule/build.sock -o ControlPersist=8h build
```

Without it, a job command exits 3 and prints that line.

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

`mule run --wait` combines dispatch, live output and the job's exit code. mule's
flags go before the command. `--tui` runs a full-screen program, and murmur
shows jobs as crew.

## Exit 3 needs a human. Stop and ask.

Exit 3 means the control master is missing, and opening one can need a physical
act: touching a hardware key, typing a one-time code. If you are an agent or a
script and get exit 3, **stop and ask the operator to run the printed command.**
Do not retry, do not run `ssh -MNf` yourself (it fails silently without a
terminal), and do not fall back to `ssh host command`, which holds the session
for the whole job: the failure mule exists to remove.

## More

| Doc | For |
| --- | --- |
| [docs/why.md](docs/why.md) | The session cap, measurements, which jobs to route through mule |
| [docs/usage.md](docs/usage.md) | TUI jobs, the job's shell, output, timeouts, cleanup, exit codes |
| [SPEC.md](SPEC.md) | Design, invariants, failure behaviour, reproducing the cap locally |
| [RELEASING.md](RELEASING.md) | Cutting a release |

---

Part of [mu-crew](https://github.com/mu-crew). Written mostly by AI coding agents, with a human reviewing what ships, and built for running them.
