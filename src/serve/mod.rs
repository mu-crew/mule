//! `mule serve --stdio`: bounded synchronous ops for one caller.
//!
//! The third class of ssh use (SPEC § Bounded synchronous ops). `exec` costs
//! about four round trips per command (dispatch, probe, collect, removal
//! proof: 1.3-1.7s measured on two real hosts); serve answers a read, stat
//! or write in one, at 0.32-0.38s p50 on the same hosts. Those three only:
//! no arbitrary command runs here (human decision, 2026-11-20); a command
//! is a detached job (`run`, `exec`).
//!
//! - **A child of its caller, not a daemon.** JSON lines on stdin, JSON lines
//!   on stdout. stdin EOF, or a stdout the caller closed, means the caller is
//!   gone: the in-flight ops are cancelled, the queue is dropped, and the
//!   process exits. Nothing is shared between callers and nothing outlives
//!   one, so SPEC's "no process between invocations" holds per caller.
//! - **One ticket-locked round trip per batch**, through
//!   [`Transport::run_with_stdin`]: the lock is the transport's, as for every
//!   other verb. No session is held between batches.
//! - **Bounded by the host, not by this process.** The remote script bounds
//!   every phase -- payload read [`PAYLOAD_SECS`] and delivery
//!   [`DELIVER_SECS`] under a timer of the script's own ([`BOUND_FN`]), each op [`MAX_OP_SECS`] in its own
//!   session (or process group), KILLed whole 1s later -- and caps every op's output, and
//!   the batch's, on disk and on the wire. So a batch frees the session even
//!   if this process is SIGKILLed or the link wedges. Work longer than
//!   [`MAX_OP_SECS`] comes back rc 124 and belongs in a detached job. A host
//!   without `base64`, `mktemp`, `sleep`, a way to make process
//!   groups or a way to find an op's processes fails every op closed.
//! - **Payloads on stdin**, never argv: a 200KB write is one round trip.
//! - **Coalescing.** Requests queued while a batch is in flight go together
//!   in the next one and run in parallel on the host.
//! - **Cancel without a second session.** After the payload, the session's
//!   stdin is a control stream: `<k>\n` kills op `k`, EOF
//!   kills them all; an op the host saw complete is not killed, and reports
//!   its real result. A second ssh could not: on a `MaxSessions 1` host it
//!   would be refused until the op it means to stop had ended.

use std::collections::{HashMap, HashSet, VecDeque};
use std::io::{BufRead, Write};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use anyhow::Result;
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as B64;
use serde::{Deserialize, Serialize};

use crate::config::Host;
use crate::transport::{Control, Feed, Transport};

/// Hard per-op limit on the host. 1s: rung 2 wants a single op at <=0.5s, so
/// a bounded op needing twice that is a job (human decision, 2026-11-20).
pub const MAX_OP_SECS: u32 = 1;
/// Bound on reading the batch's payloads off stdin on the host. A 200KB write
/// took 0.54-1.05s whole on two real hosts; 5s leaves room for 8MB on a slow
/// link.
pub const PAYLOAD_SECS: u32 = 5;
/// Bound on writing the frames back. The output budget is
/// [`MAX_BATCH_OUTPUT`], so this is a slow-link allowance, not a size limit.
pub const DELIVER_SECS: u32 = 5;
/// Local hard kill, from the spawn. Above every remote bound together
/// (payload + op + kill grace + delivery = 12s) so it fires only when the host
/// or the link is wedged. Killing the ssh does not free the session (SPEC §
/// Detached cleanup); the remote bounds do, and they run without us.
pub const LOCAL_KILL: Duration = Duration::from_secs(15);
/// Per-op stdout cap for `read`, enforced on the host: control-sized, like
/// `tail`'s one-shot read is bounded.
pub const MAX_OUT: u64 = 1 << 20;
/// Both caps of `stat` and `write`, and `read`'s stderr: a mode line, a path
/// or an error message. 8KB holds a 4096-byte `PATH_MAX` path twice.
pub const MAX_SMALL: u64 = 8 << 10;
/// All ops' output in one batch, on the host's disk and on the wire. Each op
/// reserves its caps out of it when it joins a batch, so the spool can never
/// pass it however the ops behave: 7 reads a batch, or dozens of
/// stats and writes. A shared cap the ops drew on as they wrote would let
/// a batch hold more, but `sh` has no atomic counter, and the per-op caps
/// alone let 64 ops spool 68MB for an 8MB reply (review, 2026-11-20).
pub const MAX_BATCH_OUTPUT: u64 = 8 << 20;
/// Payload cap per batch (and so per op), so a batch is a bounded transfer.
pub const MAX_PAYLOAD: usize = 8 << 20;
/// Script cap per batch, before [`batch_script`] base64s it for `/bin/sh`:
/// with that 4/3 and the fixed part (~5KB) it stays under the 112 KiB
/// dispatch cap (SPEC § The dispatch size cap). Payloads do not count: they
/// ride stdin.
pub const MAX_SCRIPT: usize = 64 << 10;
/// Ops per batch.
pub const MAX_BATCH: usize = 64;
/// How long the worker lets a burst gather before sending. Windows of 1-20ms
/// were invisible next to the round trip (codemode spike).
pub const WINDOW: Duration = Duration::from_millis(3);
/// After the caller goes, how long serve waits for the batch in flight to end
/// before exiting anyway. Exiting early is safe -- the remote bounds still
/// free the session -- so this only gives a clean end a chance.
const EXIT_GRACE: Duration = Duration::from_secs(2);

/// The host lacks one of the tools the batch script needs.
pub const RC_MISSING_TOOL: i32 = 97;
/// Grace between an op's TERM and the KILL of its whole process group.
const KILL_GRACE_SECS: u32 = 1;

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "op", rename_all = "lowercase", deny_unknown_fields)]
pub enum Request {
    Read {
        id: u64,
        path: String,
    },
    Stat {
        id: u64,
        path: String,
    },
    Write {
        id: u64,
        path: String,
        #[serde(default)]
        data_b64: String,
    },
    Cancel {
        id: u64,
    },
}

/// One bounded op, decoded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Op {
    Read(String),
    Stat(String),
    Write(String, Vec<u8>),
}

impl Op {
    fn payload(&self) -> &[u8] {
        match self {
            Op::Write(_, data) => data,
            _ => &[],
        }
    }

    fn path(&self) -> &str {
        match self {
            Op::Read(p) | Op::Stat(p) | Op::Write(p, _) => p,
        }
    }

    /// `(stdout, stderr)` caps on the host, reserved from the batch's
    /// [`MAX_BATCH_OUTPUT`].
    pub fn caps(&self) -> (u64, u64) {
        match self {
            Op::Read(_) => (MAX_OUT, MAX_SMALL),
            Op::Stat(_) | Op::Write(..) => (MAX_SMALL, MAX_SMALL),
        }
    }

    /// What this op can put on the host's disk: each cap plus the one byte
    /// past it the runner keeps to see a cut.
    fn spool(&self) -> u64 {
        let (o, e) = self.caps();
        o + e + 2
    }

    /// The shell body run in the op's own session, with the op's path as
    /// `$1`. The path is never part of any shell code: it travels base64'd
    /// and is decoded into a variable ([`batch_body`]), so it is data
    /// byte-for-byte, trailing newlines included. [`accept`] refuses a NUL,
    /// which no shell string can hold.
    ///
    /// A path `-` becomes `./-`, the same file: `--` ends option parsing but
    /// leaves `-` meaning stdin to GNU `head` and `stat`, and a read of the
    /// file named `-` replied empty, a stat size 0, both rc 0 (review,
    /// 2026-11-22, Linux host).
    fn body(&self) -> String {
        const DASH: &str = "case $1 in -) set -- ./-;; esac; ";
        match self {
            // One byte past the cap, so the runner sees and flags a cut.
            Op::Read(_) => format!("{DASH}exec head -c {} -- \"$1\"", MAX_OUT + 1),
            // uname, not try-and-fall-back (SPEC § Remote snippets fail closed).
            Op::Stat(_) => format!(
                "{DASH}case $(uname -s) in Linux) exec stat -c '%s %Y %f' -- \"$1\";; \
                 *) exec stat -f '%z %m %Xp' -- \"$1\";; esac"
            ),
            Op::Write(_, data) => write_body(data.len()),
        }
    }

    /// This op's share of a batch script, before the script exists.
    fn script_cost(&self) -> usize {
        (self.body().len() + self.path().len()).div_ceil(3) * 4 + 600
    }
}

/// `write`: replace exactly the file named, or fail.
///
/// `mv -f tmp dst` treats a `dst` that is a directory, or a symlink to one,
/// as a container: a write to a directory returned rc 0 and left the payload
/// in an oddly named child (review, 2026-11-20). BSD `mv` has no `-T`, so the
/// destination is checked before (not a symlink, not a directory, its parent
/// resolved with `pwd -P`) and after: a regular file of exactly the payload's
/// size at the resolved path, and the temp name gone. The temp file sits
/// in a `mktemp -d` directory of its own inside the destination's
/// directory, so the rename stays on one filesystem, and nothing in the
/// shared directory is ever opened or removed but what `mktemp` created
/// exclusively (`mkdir`, mode 0700). A fixed name created with `set -C`
/// and `rm -f`'d on failure deleted the caller's sibling on a collision,
/// and checking the name (`-e`, `-L`) before the `set -C` create did not
/// close the gap: `set -C` opens an existing FIFO, and one planted between
/// the two got the payload and was then removed (review, 2026-11-23,
/// Linux host). Only what this op created is cleaned up, the TERM trap's
/// included. The resolved path is the op's stdout, so the caller sees where it
/// went. The path is absolute: [`accept`] refuses a relative one, which the
/// host would resolve against whatever its login directory is.
///
/// The parent is resolved byte-for-byte: `$(pwd -P)` strips every trailing
/// newline, so `parent\n/file` resolved to `parent/file`, and a write there
/// replaced an unrelated file, rc 0 (review, 2026-11-20). A sentinel after
/// `pwd`'s output keeps them; only `pwd`'s own newline is cut. The temp
/// file's size is checked before the rename too: checked only after it, a
/// short temp file had already replaced the old one.
///
/// A replaced file keeps its mode, set on the temp file before the rename:
/// the rename publishes the temp file's inode, so a chmod after it left a
/// 0600 file's new bytes at 0644 until it ran, and for good when it failed
/// or the caller aborted (review, 2026-11-23, Linux host). The temp file is
/// unreadable to others meanwhile: its directory is `mktemp -d`'s 0700. No
/// `--` for `chmod`: BSD's takes it as a file; `$t` is absolute.
///
/// Both size checks are [`COUNT_FN`]'s: `$(($(wc -c < f)))` dropped `wc`'s
/// status and read its empty output as 0, so with `wc` failing an empty
/// write replaced the file, rc 0 (review, 2026-11-22, Linux host).
fn write_body(len: usize) -> String {
    format!(
        "{COUNT_FN}p=$1; b=${{p##*/}}; case $b in ''|.|..) echo \"mule-serve: no file name in $p\" >&2; exit 2;; esac\n\
         r=$(cd -P -- \"${{p%/*}}/\" && pwd -P && printf x) || exit 2; r=${{r%x}}; r=${{r%?}}; f=${{r%/}}/$b\n\
         if [ -L \"$f\" ] || [ -d \"$f\" ]; then \
         echo \"mule-serve: $f is a symlink or a directory; not replaced\" >&2; exit 2; fi\n\
         o=; trap '[ -z \"$o\" ] || {{ rm -f -- \"$t\"; rmdir -- \"$d\"; }}; exit 143' TERM\n\
         d=$(mktemp -d -- \"${{r%/}}/.$b.mule-serve.XXXXXX\") || \
         {{ echo \"mule-serve: $f not replaced: no temp dir could be created\" >&2; exit 1; }}; \
         t=$d/${{d##*/}}; o=1\n\
         cat > \"$t\" || {{ rm -f -- \"$t\"; rmdir -- \"$d\"; exit 1; }}\n\
         N \"$t\" {len} || {{ rm -f -- \"$t\"; rmdir -- \"$d\"; \
         echo \"mule-serve: $f not replaced: the temp file was written short\" >&2; exit 1; }}\n\
         if [ -e \"$f\" ]; then case $(uname -s) in Linux) m=$(stat -c %a -- \"$f\");; \
         *) m=$(stat -f %Lp -- \"$f\");; esac && case $m in ''|*[!0-7]*) false;; esac && chmod \"$m\" \"$t\" || \
         {{ rm -f -- \"$t\"; rmdir -- \"$d\"; echo \"mule-serve: $f not replaced: its mode could not be kept\" >&2; exit 1; }}; fi\n\
         mv -f -- \"$t\" \"$f\" || {{ rm -f -- \"$t\"; rmdir -- \"$d\"; exit 1; }}\n\
         if [ -L \"$f\" ] || [ ! -f \"$f\" ] || [ -e \"$t\" ] || ! N \"$f\" {len}; then \
         rm -f -- \"$t\" \"$f/${{t##*/}}\"; rmdir -- \"$d\"; \
         echo \"mule-serve: $f was not replaced as written\" >&2; exit 1; fi\n\
         rmdir -- \"$d\" || {{ echo \"mule-serve: $f replaced, but its temp dir $d is left\" >&2; exit 1; }}\n\
         printf '%s\\n' \"$f\""
    )
}

/// `N <file> <n>` succeeds only if `wc -c` of the file succeeded and
/// printed exactly `n`: a failed or garbled count is a failed check, never
/// 0. The padding macOS's `wc` prints is trimmed.
const COUNT_FN: &str = "N() { c=$(wc -c < \"$1\") || return 1; c=${c#\"${c%%[![:space:]]*}\"}; \
     c=${c%\"${c##*[![:space:]]}\"}; case $c in ''|*[!0-9]*) return 1;; esac; [ \"$c\" -eq \"$2\" ]; }\n";

fn quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', r"'\''"))
}

/// Runs one op: `$1` body, `$2` out, `$3` err, `$4` rc, `$5` stdin, `$6`
/// and `$7` the stdout and stderr caps, each one byte past the op's cap so
/// the delivery can tell a cut, `$8` the op's path, the body's `$1`.
///
/// Both streams go through `head -c` into files, followed by a `cat` that
/// keeps draining: the cap bounds the disk without killing the op by SIGPIPE
/// (SPEC § Log size: a bare `head` turned rc 4 into 141). The op's own rc is
/// written inside the pipeline's left side because `sh` has no PIPESTATUS.
///
/// The rc file is the host's proof the op completed. The shell writing it
/// traps TERM (a handler, which the body does not inherit; an ignore it
/// would), so a TERM that races the op's end cannot lose a result that
/// exists. The body gets no fd 3: a child it left behind held the stdout
/// pipe open and kept the runner alive until the deadline.
///
/// Each collector marks `<file>.ok` only if its `head` succeeded: the drain
/// after it hid a failed one, and a read whose stdout could not be spooled
/// (a full disk) replied rc 0, empty and whole (review, 2026-11-20). A
/// missing mark is a failure, so a mark that cannot be made fails closed.
fn runner() -> String {
    "{ { trap : TERM; sh -c \"$1\" mule-body \"$8\" < \"$5\" 3>&-; echo $? > \"$4\"; } 2>&1 1>&3 | \
     { head -c \"$7\" > \"$3\" && : > \"$3.ok\"; cat > /dev/null; }; } 3>&1 | \
     { head -c \"$6\" > \"$2\" && : > \"$2.ok\"; cat > /dev/null; }"
        .into()
}

/// The shell functions that end an op, shared by the batch, the deadline
/// and the watcher. They read `$d` (the batch dir), `$X` (set: ops are
/// sessions and `pgrep -s` lists them) and `$T` (set: `ps -o ppid` and `awk`
/// work).
///
/// `K <sig> <leader> <k> [t]` signals op `k`'s leader itself, unless its
/// supervisor has reaped it (`end.<k>`), its process group, its session
/// with `$X`, and every pid recorded in `kids.<k>`. The leader by its pid
/// because `$!` is published before it has made its session: until the op's
/// `setsid` runs, the leader is in the batch's group and leads nothing, so
/// a group or session signal misses it. One stopped there outlived its
/// deadline, its cancel and its caller's exit, and ran its op on resume,
/// rc 0 (review, 2026-11-22, a `setsid` wrapper that stopped first). With `t` (a deadline or
/// a cancel: the leader is still alive) it first walks the leader's process
/// tree and records every descendant, whatever its group -- first, because
/// the signal kills the leader and orphans them out of the tree. The op
/// bodies are serve's own and start no group, but the host tools they run
/// (`head`, `stat`, `cat`, `mv`, `wc`) are whatever the host's `PATH` finds,
/// and a wrapper there (a shim, a version manager's) may run the real tool
/// under GNU `timeout(1)`, which moves its child into a group of its own:
/// that child outlived both the TERM and the KILL of the op's group and
/// touched a file after the reply said rc 124 (review, 2026-11-20, local
/// repro). A recorded leader of its own group is signalled as a group too.
///
/// `L <k>` sets `p` to op `k`'s leader, from the `pid.<k>` its supervisor
/// writes, or fails if there is none yet.
///
/// `P <sig> <sid>` signals every process of session `sid`, as `pgrep -s`,
/// the probed tool, lists them: `pkill -s` was used unprobed, and a host
/// without it read as a clean session (review, 2026-11-22). It fails only
/// when `pgrep` says the session is empty (rc 1), never on `kill`'s status:
/// one `kill` over a snapshot returns 1 if any listed pid has exited, though
/// it signalled the rest, and read as "empty" that stopped the sweep while
/// a process forked into the session after the snapshot ran on after the
/// op's rc 0 (review, 2026-11-22, Linux host). A `pgrep` that errors is not an
/// empty session either: `P` returns 2, still signalling what it listed.
///
/// `Z <leader> <k>`, at the op's end, KILLs all of that until nothing is
/// left to signal: a kill that finds no process fails, and so does a `P`
/// that lists nobody, so a clean op costs one `kill` and one `pgrep`. It
/// tries 20 times 50ms rather than hold the batch, and then fails unless
/// `Q` shows that all it still found are zombies, which run nothing: a
/// zombie answers a kill until its parent reaps it. It fails outright if
/// the last `pgrep` errored, since what it did not list is unknown. The
/// supervisor marks `cl.<k>` only when `Z` succeeds, and an op without the
/// mark is not a success: the cap's failure was dropped, and with `pgrep`
/// erroring after a partial kill an op replied rc 0 while a process of its
/// session lived on (review, 2026-11-22, Linux host).
///
/// `Q <leader> <k>` succeeds only if `ps` lists no live (non-`Z`) process
/// of the op's group, of `kids.<k>`, or, with `$X`, of the session as a
/// fresh `pgrep -s` lists it after that `ps`; a `ps` or `pgrep` that fails
/// is not that proof, and a session member `ps` did not list was born after
/// it, so is live. Fresh, because the proof must come after the last kill:
/// the last sweep's `pgrep` snapshot, once kept for it, predates it, and a listed process that
/// forked into a new group of the session between that snapshot and the
/// KILL was in neither that snapshot nor the op's group, so the op replied rc 0
/// while it ran on (review, 2026-11-23, Linux host, a `pgrep` shim forking a
/// same-session child after each of 20 snapshots).
///
/// The leader's pid is reaped (by `wait`) before `Z` signals its group or
/// session: a pid is not reused while a group or session of that id has a
/// member, so the only stale target is an id emptied and reused within
/// these milliseconds. A recorded descendant's pid, though, can be reused
/// within the grace; on a host whose pids wrap in seconds that kill can
/// miss. So can the leader's, in the instant between its reap and the
/// supervisor's `end.<k>`.
const KILL_FNS: &str = r#"K() { [ "$4" = t ] && [ -n "$T" ] && ps -A -o pid= -o ppid= -o pgid= 2>/dev/null | awk -v r="$2" '{ p[NR] = $1; q[NR] = $2; g[NR] = $3 } END { s[r] = 1; do { n = 0; for (i = 1; i <= NR; i++) if (!(p[i] in s) && (q[i] in s)) { s[p[i]] = 1; print p[i], g[i]; n = 1 } } while (n) }' >> "$d/kids.$3"
[ -e "$d/end.$3" ] || kill -$1 $2 2>/dev/null; kill -$1 -$2 2>/dev/null; [ -n "$X" ] && P $1 $2
[ -s "$d/kids.$3" ] && while read -r q g; do kill -$1 $q; [ "$q" = "$g" ] && kill -$1 -$q; done < "$d/kids.$3" 2>/dev/null; :; }
P() { q=$(pgrep -s $2); pr=$?; [ $pr -eq 1 ] && return 1; [ -n "$q" ] && kill -$1 $q; [ $pr -eq 0 ] || return 2; } 2>/dev/null
Z() { n=0; while [ $n -lt 20 ]; do a=; u=; kill -KILL -$1 2>/dev/null && a=1
if [ -n "$X" ]; then P KILL $1; case $? in 0) a=1;; 2) u=1;; esac; fi
[ -s "$d/kids.$2" ] && while read -r q g; do kill -KILL $q 2>/dev/null && a=1; done < "$d/kids.$2"
[ -z "$a$u" ] && return 0; sleep 0.05; n=$((n+1)); done; [ -z "$u" ] && [ -n "$T" ] && Q $1 $2; }
Q() { o=$(ps -A -o pid= -o pgid= -o stat=) || return 1; l=; if [ -n "$X" ]; then q=$(pgrep -s $1); [ $? -le 1 ] || return 1; for q in $q; do l="$l $q"; done; fi
printf '%s\n' "$o" | awk -v g="$1" -v l="$l" -v f="$d/kids.$2" 'BEGIN { m = split(l, s, " "); for (i = 1; i <= m; i++) w[s[i]] = 1; l = l " "; while ((getline x < f) > 0) { split(x, y, " "); l = l y[1] " " } } { n++; if ($1 in w) delete w[$1]; if ($3 !~ /^Z/ && ($2 == g || index(l, " " $1 " "))) e = 1 } END { for (q in w) e = 1; exit (e || !n) }'; } 2>/dev/null
L() { p=; read -r p < "$d/pid.$1" 2>/dev/null; case $p in ''|*[!0-9]*) return 1;; esac; }
"#;

/// The deadline of every op, in its own process group: `$1` dir, `$2` and
/// `$3` the batch's `$X` and `$T`, `$4` the op count. At [`MAX_OP_SECS`]
/// each op with no rc is marked `to.<k>` and sent TERM; [`KILL_GRACE_SECS`]
/// later each op still without an rc is KILLed. One that got an rc in
/// between is its supervisor's to end. An op whose supervisor has not yet
/// reported a leader is KILLed by the supervisor when it does, as it finds
/// the mark: each side writes its file before it reads the other's.
///
/// This replaced a `timeout(1)` per op, which watches only its own child: a
/// TERM killed the runner, `timeout` saw it exit and never sent the KILL,
/// and a body ignoring TERM wrote a file after the reply said rc 124
/// (review, 2026-11-20, local CLI repro).
fn deadline() -> String {
    format!(
        "d=$1; X=$2; T=$3; n=$4\n{KILL_FNS}\
         sleep {MAX_OP_SECS}; k=0\n\
         while [ $k -lt $n ]; do [ -s \"$d/rc.$k\" ] || {{ : > \"$d/to.$k\"; L $k && K TERM $p $k t; }}; k=$((k+1)); done\n\
         sleep {KILL_GRACE_SECS}; k=0\n\
         while [ $k -lt $n ]; do [ -s \"$d/rc.$k\" ] || {{ L $k && K KILL $p $k t; }}; k=$((k+1)); done"
    )
}

/// The control-stream watcher: arguments as [`deadline`]'s; reads the
/// session's stdin. `<k>\n` cancels op `k`, EOF cancels them all. A cancel
/// marks `cx.<k>` and TERMs the op, then KILLs it after the grace -- unless
/// the op's rc already exists: a completed op is never cancelled.
fn watcher() -> String {
    format!(
        "d=$1; X=$2; T=$3; n=$4\n{KILL_FNS}\
         c() {{ [ -s \"$d/rc.$1\" ] && return; : > \"$d/cx.$1\"; L $1 || return; K TERM $p $1 t; \
         ( sleep {KILL_GRACE_SECS}; [ -s \"$d/rc.$1\" ] || K KILL $p $1 t ) & }}\n\
         while IFS= read -r l; do case $l in ''|*[!0-9]*) continue;; esac; [ $l -lt $n ] && c $l; done\n\
         k=0; while [ $k -lt $n ]; do c $k; k=$((k+1)); done; wait"
    )
}

/// Writes the frames: `@@ <k> <rc> <nout> <nerr> <cut> <state>\n`, the
/// bytes, and `@@ end\n` last, so a delivery cut by its timeout cannot pass
/// for whole. `$1` dir, `$2` op count, `$3...` every op's out and err file,
/// in order; `$d/caps` holds each op's two caps, a line each.
///
/// `state` is what the host knows: `d` the op completed (its rc file exists)
/// and both its outputs were spooled whole, `e` it completed but an output
/// collector failed (no `.ok` mark: its bytes are not the op's), `c` cancelled or `t` timed out (marked, and no rc, or a signal's rc: the
/// 143 our own TERM gave it, or the 141 of a TERM-proof tool whose output
/// readers that TERM killed -- reported completed, rc 141, in about 1 run in
/// 40 under load), `k` ended with neither; and `u`, whatever else, when its
/// supervisor could not prove the op's processes gone (no `cl.<k>`, see
/// [`KILL_FNS`]), with the rc the state before it had.
///
/// Every byte copy is checked: a failed one exits before `@@ end`, which
/// fails the batch rather than shift a frame. So is every bound, before its
/// op's frame: a size row or a cap that is missing or not a number exits
/// too. Unchecked, an empty `caps` left the bounds empty, the comparisons
/// errored and fell through uncut, and a read the host had stopped at its
/// cap + 1 replied rc 0, whole, a byte past the cap (review, 2026-11-23,
/// Linux host).
///
/// One `wc` for every size and `read` for the rcs: a fork per value made 20
/// ops of 200KB take 7.3s to deliver on a loaded macOS (spawns of 20-40ms).
/// `read` trims `wc`'s padding (macOS), and the total line is never read.
fn deliverer() -> String {
    "d=$1; n=$2; shift 2; wc -c \"$@\" > \"$d/sizes\" || exit 1; exec 4< \"$d/caps\"; k=0\n\
         while [ $k -lt $n ]; do\n\
         read -r o _ && read -r e _ && read -r mo me <&4 || exit 1\n\
         case \"$o.$e.$mo.$me\" in *[!0-9.]*|.*|*..*|*.) exit 1;; esac; t=0\n\
         [ $o -gt $mo ] && { o=$mo; t=1; }\n\
         [ $e -gt $me ] && { e=$me; t=1; }\n\
         if [ -s \"$d/rc.$k\" ]; then read -r r < \"$d/rc.$k\"; s=d\n\
         if [ \"$r\" -gt 128 ] && { [ -e \"$d/to.$k\" ] || [ -e \"$d/cx.$k\" ]; }; then s=t; [ -e \"$d/cx.$k\" ] && s=c\n\
         elif [ ! -e \"$d/out.$k.ok\" ] || [ ! -e \"$d/err.$k.ok\" ]; then s=e; fi\n\
         elif [ -e \"$d/cx.$k\" ]; then s=c; elif [ -e \"$d/to.$k\" ]; then s=t; else s=k; fi\n\
         case $s in c) r=143;; t) r=124;; k) r=125;; esac\n\
         [ -e \"$d/cl.$k\" ] || s=u\n\
         printf \"@@ %s %s %s %s %s %s\\n\" $k \"$r\" $o $e $t $s\n\
         if [ $o -gt 0 ]; then head -c $o \"$d/out.$k\" || exit 1; fi\n\
         if [ $e -gt 0 ]; then head -c $e \"$d/err.$k\" || exit 1; fi\n\
         k=$((k+1))\n\
         done < \"$d/sizes\"\n\
         printf \"@@ end\\n\""
        .into()
}

/// `TO <secs> <cmd>...` runs `cmd` bounded, as `timeout -k 1 <secs>` did:
/// TERM to its group (or session) at `secs`, KILL 1s later. `cmd` gets fd 3
/// as its stdin, since `sh` gives an async command `/dev/null` and dash does
/// so even under an explicit `<&0`; a caller with nothing to feed passes
/// `3</dev/null`. Like the ops, `cmd` and the timer each run in a group of
/// their own (`$S`, or `set -m`), so the TERM reaches `cmd`'s children and
/// the timer goes whole, its `sleep` included, once `cmd` ends.
///
/// Once `cmd` ends, what is left of its group is KILLed too, and that goes
/// past `timeout -k`, which stops at its child's exit: a TERM-proof `head`
/// under a delivery that the TERM killed kept the session's stdout open after
/// the batch exited (local repro, a `head` shim ignoring TERM). STOPped
/// first, then KILLed, until the group is empty (20 times 50ms at most): a
/// bare group KILL is not atomic on a loaded macOS, and a shim whose `sleep`
/// it killed first ran on and forked a `touch` the KILL never reached (about
/// 1 local run in 2). A stopped member runs nothing, and its parent is not
/// woken by the stop. A clean `cmd` leaves no group, so all this costs one
/// failed `kill`. The timer's `sh` is KILLed by pid before its group for the
/// same reason: with its `sleep` dead first, it would run on to its TERM.
///
/// Not `timeout(1)`: stock macOS before 27 has none, and with it required
/// every op on such a host failed `host_tool_missing` (CI, macOS 26 runner:
/// sshd's `PATH` has no `timeout`; the runner's shell had Homebrew's).
///
/// The timer's KILL can hit a reused pid in the instant between `wait`
/// reaping `cmd` and the timer's own KILL.
pub const BOUND_FN: &str = "TO() { tl=$1; shift; [ -z \"$S\" ] && set -m\n\
     exec $S \"$@\" <&3 & tp=$!\n\
     exec $S sh -c 'sleep $1; kill -TERM -$2 $2; sleep 1; kill -KILL -$2 $2' mule-timer $tl $tp \
     < /dev/null > /dev/null 2>&1 3<&- & tw=$!\n\
     [ -z \"$S\" ] && set +m\n\
     wait $tp; tr=$?; { kill -KILL $tw; kill -KILL -$tw; tn=0\n\
     while kill -STOP -$tp && [ $tn -lt 20 ]; do kill -KILL -$tp; sleep 0.05; tn=$((tn+1)); done; } 2>/dev/null\n\
     return $tr; }\n";

/// The command ssh sends for one batch: [`batch_body`] run by `/bin/sh`,
/// whatever the account's login shell is.
///
/// ssh hands its command to the login shell, and zsh aborted the body at
/// `set -m` (no job control without a tty) where `/bin/sh` on the same
/// macOS host has it: every op failed with an empty `batch_failed` (review,
/// 2026-11-20). The body travels base64'd inside one single-quoted word
/// holding no backslash, quote or newline, so sh, bash, zsh, fish and csh
/// all pass it to `/bin/sh` untouched. A failed decode runs nothing:
/// `eval` of a half-decoded body would run ops without their bounds.
pub fn batch_script(ops: &[Op]) -> String {
    format!(
        "exec /bin/sh -c 'command -v base64 >/dev/null 2>&1 || \
         {{ echo \"mule-serve: host lacks base64\" >&2; exit {RC_MISSING_TOOL}; }}; \
         s=$(printf %s {} | base64 -d) || exit 1; eval \"$s\"'",
        B64.encode(batch_body(ops))
    )
}

/// The POSIX `sh` script for one batch.
///
/// Nothing in it waits unbounded on anything but local files: the payload
/// read and the delivery run under [`BOUND_FN`], every op under the deadline
/// process, and the helpers are killed once the ops end.
///
/// Every op runs in its own session (`setsid`), or where the host has none
/// (macOS) its own process group (`set -m`: `/bin/sh` there is bash, which
/// has job control without a tty; dash has not). A supervisor per op waits
/// for its leader and then KILLs whatever is left of the op ([`KILL_FNS`]),
/// so op end means the op is gone: one op's leftovers do not live on for as
/// long as a slower sibling runs. A deadline or a cancel also catches
/// descendants that left the op's group, through the process tree.
///
/// Not caught (SPEC § Bounded synchronous ops): a descendant of a host tool
/// that starts a session of its own (`setsid`, a daemon) and is orphaned
/// before the op's end; without `setsid` and `pgrep -s`, also one that only moved to its
/// own group (`timeout ... &`) and was orphaned before the op's end. A
/// per-op cgroup would catch both, but an ssh session's cgroup belongs to
/// root (systemd's `session-N.scope`), so a user's serve cannot make one.
///
/// Outputs live in a private `mktemp -d`, removed on exit.
pub fn batch_body(ops: &[Op]) -> String {
    let total: usize = ops.iter().map(|op| op.payload().len()).sum();
    let mut s = String::new();
    // Fail closed: without `sleep`, process groups or a way to find an
    // op's processes nothing here is bounded, and without base64 or mktemp
    // nothing runs as written. Checked before anything runs. Sessions count
    // only if `pgrep -s 0` lists this shell: the tool that is probed is the
    // one that kills ([`KILL_FNS`]), and a probe on its exit code alone
    // passes a `pgrep` that lists nothing. macOS's pgrep has no `-s`. `ps` is
    // only looked for, not run: `-o pid,ppid,pgid` is POSIX, and one `ps`
    // costs 0.1-0.3s on a loaded macOS, on every batch.
    s.push_str(&format!(
        "for t in base64 mktemp head tail wc cat sleep; do \
         command -v \"$t\" >/dev/null 2>&1 || \
         {{ echo \"mule-serve: host lacks $t\" >&2; exit {RC_MISSING_TOOL}; }}; done\n\
         X=; if command -v setsid >/dev/null 2>&1; then S=setsid; \
         pgrep -s 0 2>/dev/null | {{ while read -r q; do [ \"$q\" = $$ ] && exit 0; done; exit 1; }} && X=1; else S=; set -m 2>/dev/null; \
         case $- in *m*) set +m;; *) echo 'mule-serve: host lacks setsid (and /bin/sh has no job control)' >&2; \
         exit {RC_MISSING_TOOL};; esac; fi\n\
         T=; command -v ps >/dev/null 2>&1 && command -v awk >/dev/null 2>&1 && T=1\n\
         [ -n \"$X$T\" ] || {{ echo 'mule-serve: host lacks pgrep -s, and ps -o or awk' >&2; \
         exit {RC_MISSING_TOOL}; }}\n"
    ));
    s.push_str(BOUND_FN);
    // Every body decoded here, before any op runs: decoded inside the async
    // op, the decode stretched the window in which a cancel hits a shell
    // that has not yet exec'd (bash 3.2, three ops cancelled at once: three
    // EXIT-trap runs, `$d` removed, every frame lost).
    // Each path too, as data: decoded with a sentinel after it, since
    // `$(...)` strips every trailing newline, and handed to its op as an
    // argument, never spliced into shell code.
    for (k, op) in ops.iter().enumerate() {
        s.push_str(&format!(
            "b{k}=$(printf %s {} | base64 -d) || exit 1\n\
             a{k}=$(printf %s {} | base64 -d && printf x) || exit 1; a{k}=${{a{k}%x}}\n",
            B64.encode(op.body()),
            B64.encode(op.path())
        ));
    }
    // Every spool file the setup writes is checked: each copy's status and
    // then its size, by [`COUNT_FN`]. Unchecked, a slice cut short by a
    // failing `tail` (a full disk) reached its write, which replaced the
    // old file with it (review, 2026-11-20); a copy that wrote every byte
    // and then failed was taken as whole, and an arithmetic count read a
    // failed `wc` as 0 (review, 2026-11-22). Any of them failing fails the
    // batch before an op runs.
    s.push_str(
        "d=$(mktemp -d \"${TMPDIR:-/tmp}/mule-serve.XXXXXX\") || \
         { echo 'mule-serve: mktemp failed' >&2; exit 98; }\n\
         exec 3<&0\n\
         B() { echo \"mule-serve: $1\" >&2; rm -rf \"$d\"; exit 99; }\n",
    );
    s.push_str(COUNT_FN);
    if total == 0 {
        s.push_str(": > \"$d/in\" || B 'spool write failed'\n");
    } else {
        s.push_str(&format!(
            "TO {PAYLOAD_SECS} head -c {total} > \"$d/in\" || B 'payload read failed'\n"
        ));
    }
    s.push_str(&format!("N \"$d/in\" {total} || B 'short payload'\n"));
    let caps: Vec<String> = ops
        .iter()
        .map(|op| {
            let (o, e) = op.caps();
            format!("{o} {e}")
        })
        .collect();
    s.push_str(&format!(
        "printf '%s %s\\n' {} > \"$d/caps\" || B 'spool write failed'\n",
        caps.join(" ")
    ));
    let mut off = 0usize;
    for (k, op) in ops.iter().enumerate() {
        // Made up front: an op cancelled before its runner opened them
        // would otherwise fail the delivery's one `wc`, and every frame.
        s.push_str(&format!(
            "{{ : > \"$d/out.{k}\" && : > \"$d/err.{k}\"; }} || B 'spool write failed'\n"
        ));
        let n = op.payload().len();
        if n == 0 {
            // BSD head rejects `-c 0`.
            s.push_str(&format!(": > \"$d/in.{k}\" || B 'spool write failed'\n"));
        } else {
            // The pipeline's status is `head`'s, the copy; `tail`'s is not
            // checked, as `head` closing the pipe after its `n` bytes may
            // end it by SIGPIPE. What `tail` got wrong, the size check sees.
            s.push_str(&format!(
                "tail -c +{} \"$d/in\" | head -c {n} > \"$d/in.{k}\" || B 'payload slice failed'\n\
                 N \"$d/in.{k}\" {n} || B 'short payload slice'\n",
                off + 1
            ));
        }
        off += n;
    }
    s.push_str(&format!("R={}\n", quote(&runner())));
    s.push_str(&format!("DL={}\n", quote(&deadline())));
    s.push_str(&format!("W={}\n", quote(&watcher())));
    s.push_str(KILL_FNS);
    for (k, op) in ops.iter().enumerate() {
        // A supervisor per op, a subshell in the batch's group: it forks
        // the op, reports its leader, and once the leader is gone KILLs
        // what is left (`Z`), so this op's end does not wait for another's
        // (review, 2026-11-20: one op's background child ran while a
        // slower sibling did).
        //
        // `exec`, so `$!` is the leader: a shell may run an async command
        // in a subshell that forks it, and `setsid` must run in the forked
        // child itself, which leads no group yet, or it forks again. Job
        // control, if that is how this host makes groups, only while the
        // op is forked: on, `$(...)` and foreground commands get groups
        // too. fd 3 (the session's stdin) closed and stdio on /dev/null:
        // an op, or anything it leaves behind, holds no fd of the session.
        //
        // A leader whose `pid.<k>` could not be written (a full disk) is
        // one the deadline, a cancel and the exit trap cannot find, so it
        // is KILLed at once and reports `lost`.
        let (o, e) = op.caps();
        s.push_str(&format!(
            "( [ -z \"$S\" ] && set -m\n\
             exec $S sh -c \"$R\" mule-op \"$b{k}\" \
             \"$d/out.{k}\" \"$d/err.{k}\" \"$d/rc.{k}\" \"$d/in.{k}\" {} {} \"$a{k}\" \
             < /dev/null > /dev/null 2>&1 & p=$!\n\
             [ -z \"$S\" ] && set +m\n\
             echo $p > \"$d/pid.{k}\"\n\
             {{ [ ! -s \"$d/pid.{k}\" ] || [ -e \"$d/to.{k}\" ] || [ -e \"$d/cx.{k}\" ]; }} && K KILL $p {k}\n\
             wait $p; : > \"$d/end.{k}\"; Z $p {k} && : > \"$d/cl.{k}\" ) < /dev/null > /dev/null 2>&1 3<&- & s{k}=$!\n",
            o + 1,
            e + 1
        ));
    }
    s.push_str(&format!(
        "[ -z \"$S\" ] && set -m\n\
         exec $S sh -c \"$DL\" mule-deadline \"$d\" \"$X\" \"$T\" {n} < /dev/null > /dev/null 2>&1 3<&- & dl=$!\n",
        n = ops.len()
    ));
    // The watcher reads fd 3, a dup of the session's stdin, because sh gives
    // a background job /dev/null as stdin, which read as EOF and killed
    // every op at once (the prototype's local-sh test: every op rc 143).
    s.push_str(&format!(
        "exec $S sh -c \"$W\" mule-watch \"$d\" \"$X\" \"$T\" {n} <&3 > /dev/null 2>&1 & w=$!\n\
         [ -z \"$S\" ] && set +m\n",
        n = ops.len()
    ));
    // Every trap only now, after every child is forked. A bash with a trap
    // installs its own handlers for fatal signals, and its forked children
    // keep them until they exec: a TERM trap set before the ops made them
    // ignore the cancel's TERM (7 local-sh runs of 8, 0 of 10 without it),
    // and the EXIT trap ran in the watcher when it ended on EOF, removing
    // `$d` under the ops writing to it. The traps let a hung-up session
    // still remove `$d`; one dying before here leaks it in $TMPDIR. Until
    // every supervisor is done the EXIT trap first KILLs every op: the
    // deadline and the watcher find the ops' leaders in `$d`, so removing
    // it would leave the ops unbounded. A SIGKILLed batch runs no trap and
    // leaves `$d`, and the deadline still finds them.
    s.push_str(&format!(
        "trap 'k=0; while [ $k -lt {n} ]; do L $k && K KILL $p $k t; k=$((k+1)); done; rm -rf \"$d\"' EXIT\n\
         trap 'exit 129' HUP\ntrap 'exit 143' TERM\n",
        n = ops.len()
    ));
    for k in 0..ops.len() {
        s.push_str(&format!("wait $s{k}\n"));
    }
    s.push_str("trap 'rm -rf \"$d\"' EXIT\n");
    // Every op has ended and been cleaned up by its supervisor; the helpers
    // and their timers go now.
    s.push_str("for p in $dl $w; do kill -KILL -$p; done 2>/dev/null\nexec 3<&-\n");
    s.push_str(&format!("F={}\n", quote(&deliverer())));
    s.push_str(&format!(
        "TO {DELIVER_SECS} sh -c \"$F\" mule-deliver \"$d\" {}{} 3< /dev/null\n",
        ops.len(),
        (0..ops.len())
            .map(|k| format!(" \"$d/out.{k}\" \"$d/err.{k}\""))
            .collect::<String>()
    ));
    s
}

/// What the host knows of how an op ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Ended {
    /// It ran to the end; `rc` is its own.
    Completed,
    /// A cancel killed it before it completed.
    Cancelled,
    /// The deadline killed it before it completed.
    TimedOut,
    /// It ended without a result and neither did.
    Lost,
    /// It completed, but the host could not spool its output whole (a
    /// collector failed: a full disk, a quota): its bytes are not its own.
    Unspooled,
    /// The host could not prove that every process of the op was gone when
    /// it ended (its `pgrep` errored, or something kept answering a KILL
    /// that was not a zombie): something it started may still run.
    Uncleaned,
}

/// One op's result from the frames.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    pub rc: i32,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    /// The host cut stdout or stderr at a cap.
    pub truncated: bool,
    pub ended: Ended,
}

/// Parse `n` frames and the end marker. `None` for anything malformed,
/// missing or trailing: the caller then fails the whole batch rather than
/// guess which op a byte belongs to.
pub fn parse_frames(mut buf: &[u8], n: usize) -> Option<Vec<Frame>> {
    let mut out = Vec::with_capacity(n);
    for k in 0..n {
        let nl = buf.iter().position(|&b| b == b'\n')?;
        let head = std::str::from_utf8(&buf[..nl]).ok()?;
        let f: Vec<&str> = head.split(' ').collect();
        if f.len() != 7 || f[0] != "@@" || f[1].parse::<usize>().ok()? != k {
            return None;
        }
        let rc: i32 = f[2].parse().ok()?;
        let (no, ne): (usize, usize) = (f[3].parse().ok()?, f[4].parse().ok()?);
        let truncated = match f[5] {
            "0" => false,
            "1" => true,
            _ => return None,
        };
        let ended = match f[6] {
            "d" => Ended::Completed,
            "c" => Ended::Cancelled,
            "t" => Ended::TimedOut,
            "k" => Ended::Lost,
            "e" => Ended::Unspooled,
            "u" => Ended::Uncleaned,
            _ => return None,
        };
        buf = &buf[nl + 1..];
        if buf.len() < no + ne {
            return None;
        }
        out.push(Frame {
            rc,
            stdout: buf[..no].to_vec(),
            stderr: buf[no..no + ne].to_vec(),
            truncated,
            ended,
        });
        buf = &buf[no + ne..];
    }
    (buf == b"@@ end\n").then_some(out)
}

/// The most stdout a batch can legitimately send: the host's budget plus a
/// header per op. Anything past it is not ours, and the session is killed.
fn max_batch_stdout(ops: usize) -> usize {
    MAX_BATCH_OUTPUT as usize + (ops + 1) * 64
}

/// One reply line. `error` and `kind` are set on failure; `kind` is stable for
/// programs: `bad_request`, `too_large`, `timeout`, `cancelled`,
/// `output_limit`, `lost`, `spool_failed`, `cleanup_failed`,
/// `host_tool_missing`, `batch_failed`, `master_gone`. `spool_failed` has rc -1 whatever the op's own rc (in
/// `error`): the op ran, so a write's effect may have happened, but its
/// output is not whole, and a caller reading rc 0 would trust it.
/// `cleanup_failed` is rc -1 likewise: the op ended, but something it
/// started may still be running.
///
/// `cancelled` is a fact: the op was dropped before dispatch, or the host
/// killed it before it completed. `cancel_requested` only says a cancel was
/// asked for; an op that completed anyway reports its real result, since a
/// caller retrying a "cancelled" effect that happened would repeat it.
#[derive(Debug, Serialize)]
pub struct Reply {
    pub id: u64,
    pub rc: i32,
    pub stdout_b64: String,
    pub stderr: String,
    pub truncated: bool,
    pub cancelled: bool,
    pub cancel_requested: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kind: Option<&'static str>,
    pub batch: u64,
    pub batch_size: usize,
}

fn error_reply(id: u64, kind: &'static str, error: String) -> Reply {
    Reply {
        id,
        rc: -1,
        stdout_b64: String::new(),
        stderr: String::new(),
        truncated: false,
        cancelled: false,
        cancel_requested: false,
        error: Some(error),
        kind: Some(kind),
        batch: 0,
        batch_size: 0,
    }
}

#[derive(Default)]
struct State {
    queue: VecDeque<(u64, Op)>,
    /// Op id -> index in the batch in flight.
    inflight: HashMap<u64, usize>,
    /// Ops in flight a cancel was sent for: asked, not confirmed.
    cancel_requested: HashSet<u64>,
    closed: bool,
}

type Shared = Arc<(Mutex<State>, Condvar)>;

enum Event {
    Line(String),
    /// The caller is gone: stdin EOF or error, or its stdout closed.
    CallerGone,
    /// The worker stopped, with serve's exit code.
    Worker(i32),
}

/// The serve loop over any transport, reader and writer, so tests drive it.
///
/// Returns the exit code: 0 when the caller goes, 3 when the master does.
/// Neither the caller's stdin nor its stdout can hold a session or the
/// ticket: requests are read on one thread, replies written on another, and
/// the worker never waits on either while it holds a session.
pub fn serve<T: Transport + Send + Sync + 'static>(
    transport: Arc<T>,
    host: Host,
    input: impl BufRead + Send + 'static,
    output: impl Write + Send + 'static,
) -> i32 {
    let state: Shared = Arc::default();
    let ctl: Control = Arc::default();
    let (events, inbox) = mpsc::channel();
    let (replies, outbox) = mpsc::channel::<Reply>();
    {
        let events = events.clone();
        std::thread::spawn(move || {
            for line in input.lines() {
                let Ok(line) = line else { break };
                if events.send(Event::Line(line)).is_err() {
                    return;
                }
            }
            let _ = events.send(Event::CallerGone);
        });
    }
    let writer = {
        let events = events.clone();
        std::thread::spawn(move || write_replies(output, outbox, events))
    };
    {
        let (state, ctl, replies, events) =
            (state.clone(), ctl.clone(), replies.clone(), events.clone());
        std::thread::spawn(move || {
            let code = worker(&*transport, &host, &state, &ctl, &replies);
            let _ = events.send(Event::Worker(code));
        });
    }
    drop(events);
    let code = loop {
        match inbox.recv() {
            Ok(Event::Line(line)) => accept(&line, &state, &ctl, &replies),
            Ok(Event::Worker(code)) => break code,
            Ok(Event::CallerGone) | Err(_) => {
                // Drop the queue, close the control stream (the remote kills
                // every op on EOF), refuse any further session, and give the
                // batch in flight a moment to end.
                {
                    let (lock, cv) = &*state;
                    let mut st = lock.lock().unwrap();
                    st.closed = true;
                    st.queue.clear();
                    cv.notify_all();
                }
                ctl.lock().unwrap().abandon();
                let deadline = std::time::Instant::now() + EXIT_GRACE;
                loop {
                    let left = deadline.saturating_duration_since(std::time::Instant::now());
                    match inbox.recv_timeout(left) {
                        Ok(Event::Worker(_)) | Err(_) => break,
                        Ok(_) => {}
                    }
                }
                break 0;
            }
        }
    };
    drop(replies);
    // The writer ends once every reply is out. A caller that stopped reading
    // can hold it forever; it holds no session, so serve exits without it.
    let _ = join_within(writer, EXIT_GRACE);
    code
}

fn join_within(handle: std::thread::JoinHandle<()>, limit: Duration) -> bool {
    let deadline = std::time::Instant::now() + limit;
    while !handle.is_finished() {
        if std::time::Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    let _ = handle.join();
    true
}

fn write_replies(mut output: impl Write, outbox: Receiver<Reply>, events: Sender<Event>) {
    for reply in outbox {
        let ok = serde_json::to_writer(&mut output, &reply).is_ok()
            && output.write_all(b"\n").is_ok()
            && output.flush().is_ok();
        if !ok {
            // EPIPE: the caller closed its end. Same as stdin EOF.
            let _ = events.send(Event::CallerGone);
            return;
        }
    }
}

/// Parse one request line and queue it, cancel it, or refuse it.
fn accept(line: &str, state: &Shared, ctl: &Control, replies: &Sender<Reply>) {
    if line.trim().is_empty() {
        return;
    }
    let req: Request = match serde_json::from_str(line) {
        Ok(r) => r,
        Err(e) => {
            let _ = replies.send(error_reply(0, "bad_request", format!("bad request: {e}")));
            return;
        }
    };
    let decode = |id, field: &str, b64: String| {
        B64.decode(b64)
            .map_err(|e| error_reply(id, "bad_request", format!("bad {field}: {e}")))
    };
    let op = match req {
        Request::Cancel { id } => {
            let (lock, _) = &**state;
            let mut st = lock.lock().unwrap();
            if let Some(pos) = st.queue.iter().position(|(q, _)| *q == id) {
                st.queue.remove(pos);
                let mut r = error_reply(id, "cancelled", "cancelled before dispatch".into());
                r.cancelled = true;
                r.cancel_requested = true;
                let _ = replies.send(r);
            } else if let Some(&k) = st.inflight.get(&id) {
                st.cancel_requested.insert(id);
                ctl.lock().unwrap().send(format!("{k}\n").as_bytes());
            }
            return;
        }
        Request::Read { id, path } => Ok((id, Op::Read(path))),
        Request::Stat { id, path } => Ok((id, Op::Stat(path))),
        Request::Write { id, path, .. } if !path.starts_with('/') => Err(error_reply(
            id,
            "bad_request",
            format!("write needs an absolute path, not {path:?}"),
        )),
        Request::Write { id, path, data_b64 } => {
            decode(id, "data_b64", data_b64).map(|d| (id, Op::Write(path, d)))
        }
    };
    let (id, op) = match op {
        Ok(op) => op,
        Err(reply) => {
            let _ = replies.send(reply);
            return;
        }
    };
    // No shell string holds a NUL: `a\0b` reached the host as `ab`.
    if op.path().contains('\0') {
        let _ = replies.send(error_reply(
            id,
            "bad_request",
            "a path cannot hold a NUL byte".into(),
        ));
        return;
    }
    // Refused here, before any ssh: one op past a batch's caps could never be
    // sent, and the caller should hear so rather than see a failed batch.
    if op.payload().len() > MAX_PAYLOAD || op.script_cost() > MAX_SCRIPT {
        let _ = replies.send(error_reply(
            id,
            "too_large",
            format!(
                "op too large for one bounded round trip (payload limit {MAX_PAYLOAD} bytes, \
                 script limit {MAX_SCRIPT}); put it in a file and run a detached job"
            ),
        ));
        return;
    }
    let (lock, cv) = &**state;
    lock.lock().unwrap().queue.push_back((id, op));
    cv.notify_all();
}

/// Take up to one batch's worth of queued ops.
fn take_batch(queue: &mut VecDeque<(u64, Op)>) -> Vec<(u64, Op)> {
    let mut batch = Vec::new();
    let (mut script, mut payload, mut spool) = (0usize, 0usize, 0u64);
    while let Some((_, op)) = queue.front() {
        let (cost, size, out) = (op.script_cost(), op.payload().len(), op.spool());
        if !batch.is_empty()
            && (batch.len() >= MAX_BATCH
                || script + cost > MAX_SCRIPT
                || payload + size > MAX_PAYLOAD
                || spool + out > MAX_BATCH_OUTPUT)
        {
            break;
        }
        script += cost;
        payload += size;
        spool += out;
        batch.push(queue.pop_front().unwrap());
    }
    batch
}

fn worker<T: Transport + ?Sized>(
    transport: &T,
    host: &Host,
    state: &Shared,
    ctl: &Control,
    replies: &Sender<Reply>,
) -> i32 {
    let (lock, cv) = &**state;
    let mut seq = 0u64;
    loop {
        {
            let mut st = lock.lock().unwrap();
            while st.queue.is_empty() && !st.closed {
                st = cv.wait(st).unwrap();
            }
            if st.closed {
                return 0;
            }
        }
        std::thread::sleep(WINDOW);
        let batch = {
            let mut st = lock.lock().unwrap();
            if st.closed {
                return 0;
            }
            let b = take_batch(&mut st.queue);
            st.inflight = b.iter().enumerate().map(|(k, (id, _))| (*id, k)).collect();
            ctl.lock().unwrap().reset();
            b
        };
        if batch.is_empty() {
            continue;
        }
        seq += 1;
        let ops: Vec<Op> = batch.iter().map(|(_, op)| op.clone()).collect();
        let payload: Vec<u8> = ops.iter().flat_map(|op| op.payload().to_vec()).collect();
        let script = batch_script(&ops);
        let feed = Feed {
            payload: &payload,
            control: ctl,
            kill_after: LOCAL_KILL,
            max_stdout: max_batch_stdout(ops.len()),
        };
        let result = transport.run_with_stdin(host, &script, &feed);
        let requested = {
            let mut st = lock.lock().unwrap();
            st.inflight.clear();
            std::mem::take(&mut st.cancel_requested)
        };
        let frames = match &result {
            Ok(out) => parse_frames(&out.stdout, ops.len()).ok_or_else(|| {
                let why = out.stderr.trim();
                if out.code == RC_MISSING_TOOL {
                    (
                        "host_tool_missing",
                        format!(
                            "{why}; serve needs base64, mktemp, sleep and \
                             setsid (or a /bin/sh with job control) and pgrep -s (or ps and \
                             awk) on the host"
                        ),
                    )
                } else {
                    (
                        "batch_failed",
                        format!("batch failed (rc {}): {why}", out.code),
                    )
                }
            }),
            Err(e) => Err(("batch_failed", format!("{e:#}"))),
        };
        let master_gone = frames.is_err() && !transport.master_alive(host);
        for (k, (id, _)) in batch.iter().enumerate() {
            let reply = match &frames {
                Ok(f) => op_reply(&f[k], *id, requested.contains(id), seq, batch.len()),
                Err((kind, e)) => Reply {
                    cancel_requested: requested.contains(id),
                    batch: seq,
                    batch_size: batch.len(),
                    ..error_reply(
                        *id,
                        if master_gone { "master_gone" } else { kind },
                        e.clone(),
                    )
                },
            };
            let _ = replies.send(reply);
        }
        if master_gone {
            // Exit 3 is a request for a human (SPEC § SSH master): answer
            // everything still queued, then stop rather than retry.
            let mut st = lock.lock().unwrap();
            st.closed = true;
            for (id, _) in st.queue.drain(..) {
                let _ = replies.send(error_reply(
                    id,
                    "master_gone",
                    format!("no control master for {}; not sent", host.name),
                ));
            }
            return crate::errors::EXIT_NO_MASTER;
        }
    }
}

/// One op's reply from its frame: the host's account of how it ended wins
/// over what was asked of it.
fn op_reply(f: &Frame, id: u64, cancel_requested: bool, batch: u64, size: usize) -> Reply {
    let (kind, error) = match f.ended {
        Ended::Cancelled => (Some("cancelled"), None),
        Ended::TimedOut => (
            Some("timeout"),
            Some(format!(
                "exceeded the {MAX_OP_SECS}s bounded-op limit; \
                 run it as a detached job (mule run, mule exec)"
            )),
        ),
        Ended::Lost => (
            Some("lost"),
            Some("the op ended on the host without a result".into()),
        ),
        Ended::Completed if f.truncated => (
            Some("output_limit"),
            Some(format!(
                "output passed the host's cap ({MAX_OUT} bytes of stdout, \
                 {MAX_SMALL} for a stat, a write or a read's stderr) and was \
                 cut; read it in parts"
            )),
        ),
        Ended::Unspooled => (
            Some("spool_failed"),
            Some(format!(
                "the op exited {} but the host could not spool its output whole \
                 (disk full?); a write may have landed",
                f.rc
            )),
        ),
        Ended::Uncleaned => (
            Some("cleanup_failed"),
            Some(format!(
                "the op ended (rc {}) but the host could not prove its processes \
                 gone; something it started may still run",
                f.rc
            )),
        ),
        Ended::Completed => (None, None),
    };
    let rc = if matches!(f.ended, Ended::Unspooled | Ended::Uncleaned) {
        -1
    } else {
        f.rc
    };
    Reply {
        id,
        rc,
        stdout_b64: B64.encode(&f.stdout),
        stderr: String::from_utf8_lossy(&f.stderr).into_owned(),
        truncated: f.truncated,
        cancelled: f.ended == Ended::Cancelled,
        cancel_requested,
        error,
        kind,
        batch,
        batch_size: size,
    }
}

/// CLI entry: `mule serve --stdio --host H`.
pub fn run_cli(host: &Host) -> Result<i32> {
    crate::lock::set_quiet(true);
    crate::errors::require_master(&crate::transport::Ssh, host)?;
    let code = serve(
        Arc::new(crate::transport::Ssh),
        host.clone(),
        std::io::BufReader::new(std::io::stdin()),
        std::io::stdout(),
    );
    if code == crate::errors::EXIT_NO_MASTER {
        eprintln!("mule: {}", crate::errors::no_master(host));
    }
    Ok(code)
}
