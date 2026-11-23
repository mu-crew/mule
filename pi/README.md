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
Role/admission policy is separate work.

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
