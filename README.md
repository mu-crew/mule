# mule

On a host that allows one SSH session per connection (`MaxSessions 1`), a long
remote command holds that session, and every other tool's ssh fails with an
error that looks like bad credentials. `mule` keeps one private connection per
host and never holds it: each job runs detached under a private tmux server on
the host, and file reads and writes go over short `mule serve` batches. The
build runs; `git fetch` keeps working.

Three ways in: the `mule` CLI for jobs; [mu](https://github.com/mu-crew/mu)'s
plugin for remote workers; and a [pi](https://github.com/earendil-works/pi)
extension that keeps your agent local while its tools work on the remote tree.

Measured on a capped host: 5 concurrent plain ssh calls, 1 succeeded; through
mule, 5 of 5. [Why, with diagrams, and which jobs belong here](docs/why.md).

## Install

```sh
npm i -g @mu-crew/mule
mule init
```

Prebuilt for Linux x64, Linux arm64 and macOS arm64. Or `cargo install mule-cli`
(Rust 1.88+), or `cargo install --path .` from a clone. The host needs `tmux`.

Run `mule init` after installing or upgrading. It seeds the config if missing,
prints the master command for each host (it opens none), and registers the
bundled `mu-plugin-mule` with [mu](https://github.com/mu-crew/mu) if `mu` is
on `PATH`. It creates no mu host profiles. mu requires a plugin you own; use a
user-owned install or follow the copy commands `mule init` prints.

## Set up a host

`mule init` writes a commented template to `~/.config/mule/config.toml` if it
is missing. Add a host section such as `[hosts.build]`; the section name is
both the default SSH target and what `--host` takes.

Rerun `mule init` and run the SSH command it prints for your host. mule never
opens the control master itself: it can require a hardware-key tap. Without
it, a job command exits 3 and prints the command again.

## Use it from mu

`mu-plugin-mule` is mule's plugin for mu: mu runs `exec` and persistent `lease`
workers (bound tmux views with a file handle) through mule's private connection
instead of holding a session itself. After `mule init`:

```sh
mu host add box --plugin mule --fetch '<your fetch command>'
mu host check box
```

Details (including setup, `lease` capabilities, and exit codes) in
[docs/usage.md](docs/usage.md#as-a-mu-plugin-mu-plugin-mule).

## Use it from pi

`@mu-crew/mule` is also a pi package. With `pi install npm:@mu-crew/mule` and
`--mule build:/path/to/worktree`, pi's read, write, edit, bash, grep, find and
ls run on the host while the agent loop, its session and its credentials stay
on your machine. mu starts this for you with `mu agent spawn --mule`.
Details in [pi/README.md](pi/README.md).

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
Callers that need to tell mule's 3 from a command's own 3 pass
`--strict-codes`: the command's 3 then reads 8, so 3 always means handback (8
is ambiguous, it may be the command's own). Do not retry, do not run `ssh -MNf` yourself (it fails silently without a
terminal), and do not fall back to `ssh host command`, which holds the session
for the whole job: the failure mule exists to remove.

## More

| Doc | For |
| --- | --- |
| [docs/why.md](docs/why.md) | The session cap, measurements, which jobs to route through mule |
| [docs/usage.md](docs/usage.md) | TUI jobs, the job's shell, output, timeouts, cleanup, exit codes, `--strict-codes`, the mu plugin |
| [pi/README.md](pi/README.md) | The pi extension: remote workspace tools, policy, setup |
| [SPEC.md](SPEC.md) | Design, invariants, failure behaviour, reproducing the cap locally |
| [RELEASING.md](RELEASING.md) | Cutting a release |

---

Part of [mu-crew](https://github.com/mu-crew). Written mostly by AI coding agents, with a human reviewing what ships, and built for running them.
