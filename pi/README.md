# Pi remote workspace

The npm package `@mu-crew/mule` is also a Pi package: `pi install
npm:@mu-crew/mule` loads this extension, which uses the mule binary from
the same install. From a checkout, build mule and load the file:

```sh
cargo build
pi -e /path/to/mule/pi/mule.ts --mule build:/path/to/worktree
```

`build` is a host alias in your mule config. With `--mule build` the
workspace is the host's configured default directory (or its home). A
`"mule": "build:/path/to/worktree"` key in a trusted project's
`.pi/settings.json` does the same; the flag wins. Without either, the
extension changes no tools or prompt. The binary is the package's
platform build (`@mu-crew/mule-<os>-<cpu>`), or in a checkout its
`target/debug/mule`; set `MULE_PI_BIN` to an absolute path to use another.
Nothing looks up `mule` on `PATH`.

The footer shows `mule host:root`. The system prompt uses the remote cwd
and the remote root's `AGENTS.md` (less than 64 KiB), not instructions from
the local workspace checkout. Open the mule master yourself when asked:
the extension does not open it or run a workspace tool locally when remote
startup fails. The next workspace call retries startup, without restarting
Pi. Concurrent calls share one attempt, with a 250 ms to 2 s backoff between
failed attempts. This does not replay a failed tool operation.

| Input | Placement |
| --- | --- |
| `read`, `write`, `edit` on workspace paths | Remote file ops over `mule serve --stdio` |
| `grep`, `find`, `ls` on workspace paths | Detached jobs running remote `rg`, `fd`, `ls` respectively; path globs such as `src/**/*.ts` are relative to the find search directory |
| `bash`, `!` and `!!` | Detached remote bash, never in a file-op batch |
| Reads and searches in loop resource dirs | Local Pi tools |
| Writes and edits in loop resource dirs | Refused; manage loop configuration locally |
| `mu` | `loop_mu` accepts an argv array and invokes local `mu` without a shell |
| `mu_delegate`, skills, credentials, Pi packages/extensions | Stay with the local loop |
| Pi's PowerShell tool | Hidden in remote mode |

Relative file paths name the remote workspace, except loop resources in
`.pi/` and `.agents/`. Absolute paths under the local cwd map into the
remote workspace; native remote paths and other absolute paths stay remote.
The loop's `~/.pi`, `~/.agents`, `~/.ssh`, Pi agent directory (including its
installed packages), this extension's directory, and discovered skill
directories stay local. For a remote home path, supply its absolute path.
Direct `mu` commands in `bash` or `!` are refused before dispatch; use
`loop_mu` instead. This is placement, not a shell sandbox: the extension
does not parse shell programs or extract and run parts of them locally.
A session marked `MU_ORCHESTRATOR` cannot use remote workspace mode.

The `WorkspaceOps` seam (bounded read, stat and write; detached exec and
poll) lets a different link implement it without changing tool placement.
The serve backend keeps one `mule serve --stdio` child per session and
sends each file op as it comes; serve batches whatever arrives while a
round trip is in flight, so a burst of parallel reads costs about one
round trip. A cancelled op is cancelled alone. A dropped master ends the
child (exit 3, a handback); the next workspace call starts a new one.
serve does a read up to 1 MB within 1 s and a write by rename into an
existing directory. The rest goes through the exec backend: a larger or
slower read (paged, up to 4 MiB), a write to a symlink (written through)
or under a missing directory (created). serve keeps an existing file's
mode itself, set before the rename, so a 0600 file is never published
wider. A serve write replaces the inode, so a
hard link to the old file keeps the old content.

The exec backend groups bounded file reads/stats within a 5 ms window. Members run concurrently; one bounded batch result
is collected and checked before decoding results. A transport failure or
incomplete frame fails affected calls without replay. Cancelling a queued
read removes it; cancelling an in-flight read discards its reply without
stopping peers. Only read-only operations can be in such a batch. Writes
bypass the read batch and stage large payloads in dispatch-sized chunks
before copying to the destination. Files are capped at 4 MiB; writes that
lose the master during staging may leave a `.mule-write.*` in the destination
directory. The final copy preserves the existing file's mode but is not an
atomic replace.

Detached commands use keyed dispatch and `poll --json`. Repeating the same
session/tool-call ID reattaches; a new call (including a new `!` invocation)
gets a new key. Completed commands return the bounded tail, not live output.
Keyed artifacts are retained by mule until explicitly removed.

## Policy

- **Cancellation.** Esc (a tool abort) on `bash`, `grep`, `find`, `ls` or
  `!` waits for the dispatch answer, then stops exactly that job with
  `mule kill`. An abort before dispatch starts nothing. A cancelled file op
  is cancelled alone in serve (queued: dropped; in flight: killed), and the
  serve child keeps serving. Under muzzle, a light command with a timeout
  runs `timeout(1)` in a process group of its own, which `mule kill`'s
  hangup does not reach; the job's shell takes the hangup and kills that
  group, so nothing of an aborted command runs on.
- **Reattach.** A job's key is a hash of the Pi session id and the tool
  call id, so a replayed call gets the job already running, or its result,
  instead of running it again. Pi itself does not replay an interrupted
  call on reopen (its next prompt gets a synthetic "No result provided"),
  so at startup the extension looks for `bash` calls on the session's
  branch that have no result, finds each one's job with `mule lookup`
  (never `run --key`, which would start one), and watches it: a message
  says the job is still running and must not be run again, and a second
  one carries its exit code and output when it ends. A call whose job
  never started is left alone, and a reported result is not reported
  again on the next reopen. If the master drops while a job is watched,
  the user is told and the job runs on; the watch resumes on the next
  workspace call that succeeds (never on a timer), and the result is still
  reported once. With
  `MULE_PI_JOB_PREFIX` set, it is prepended verbatim to every detached job
  key, so mu can find and stop a worker's jobs with `mule ls --key-prefix`
  across Pi sessions. A prefix that would make an invalid key (more than
  128 characters, or outside `A-Z a-z 0-9 . _ -`) fails the call before
  anything runs.
- **Handback (mule's exit 3).** Only mule's own 3 is a handback: file ops
  use `exec --strict-codes` (a script's 3 comes back as 8), and a detached
  job's code is the `rc` in `poll --json`. On a handback the extension
  stops the turn (`ctx.abort()`), notifies the user once with the master
  command, shows `SSH master down` in the footer, and returns a `HANDBACK:`
  error telling the model not to retry. Jobs the stopped turn was waiting
  on are left running on the host (a kill could not reach them) and keep
  their keys. Nothing retries in a loop; the first tool call after the
  user's next message reconnects.
- **Read-only role.** mu records `--role read-only` but does not pass it to
  the pane, so the pane's command carries it: `--mule-role read-only`, else
  `MULE_PI_ROLE=read-only`, else a `"muleRole"` setting. An unknown value
  runs read-only. `write` and `edit` are hidden and refuse if called; the
  agent's `bash` runs only commands whose every simple command is a known
  reader (`cat`, `rg`, `git log`/`diff`/`show`/`status`, `find` without
  `-exec`/`-delete`, ...) with no output redirection
  other than to `/dev/null` and no command substitution. `sed` and `awk`
  are languages that can write files themselves, so only a subset passes:
  sed with print, delete, `s`, `y`, blocks and branches (no `w`, `W`, `e`,
  `r`, `-i` or `-f`), awk with `-F`/`-v` only and no `print > file`, pipe,
  `system` or `getline` (lines joined first as awk joins them, so a
  `print` continued onto the next line is still caught). This guards
  against accidents; like mu's role it is not a security boundary. `!` is
  the user's own command and is not filtered.
- **muzzle.** The root probe also looks for `muzzle` on the jobs' `PATH`
  (one round trip, at startup). When found, each `bash` job asks the
  host's `muzzle wrap` whether the command is heavy and, if so, runs it
  under `muzzle run --rule R --slots N --timeout 900`, with Pi's timeout as
  `--run-timeout` (counted from admission) and mule's deadline widened to
  timeout + 930 s as a backstop; a light command keeps Pi's timeout via
  `timeout(1)`. A muzzle that is gone or answers nothing usable runs the
  command unwrapped. `MULE_PI_MUZZLE=off` skips it; an absolute path pins it.
- **Output caps.** A `bash` result is the last 64 KiB of the job's log.
  When the job outran `max_log_bytes` (`mule tail` says so on stderr), or
  the log is longer than that tail, a `[mule: ...]` note follows the
  output, where Pi's own tail truncation keeps it. `rc` lands before the
  log drains when a process the command started still holds its output,
  so the result waits for `poll --json`'s `drained` (up to 5 s, as `exec`
  does) before reading the log and the cap marker; a log that never
  drains, or that a kill cut short, gets a note saying it may be incomplete. A command's own exit 7
  (or 3) is its result. For file ops, exec's 7 means mule cut the output;
  a file-op script's own 7 is reported as 1, so the two cannot be confused.

`npm pack ./npm/mule` copies this directory's runtime files (not tests)
into the package first (`scripts/npm-pi.js`, the package's `prepack`).

Tests (Node 22.18+):

- `node --test pi/routing.test.ts` exercises the op seam and binary
  resolution without Pi or SSH.
- `node --test pi/tools.test.ts` loads the extension with real Pi tool
  definitions, a fake ExtensionAPI, and a disposable mule CLI double that
  runs the generated scripts with real `bash`, `rg`, `fd` and `ls`. No SSH,
  model credentials or real mu state is needed. If `pi` on `PATH` is a site
  wrapper, set `MULE_TEST_PI` to a standalone Pi executable.
- `cargo test --test local_sshd pi_ops_over_a_capped_master` drives the real
  exec and serve backends against a private Linux loopback sshd with
  `MaxSessions 1`, and, given a standalone Pi (`MULE_TEST_PI`), a burst of
  parallel Pi tool calls through the extension.
