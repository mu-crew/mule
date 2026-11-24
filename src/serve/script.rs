//! The shell `mule serve` runs on the host: one batch script per batch of
//! ops, built from fixed functions and each op's body. The protocol, the
//! queue and the replies are in the parent module.

use super::*;
use crate::cli::shell_quote;

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
pub(super) fn write_body(len: usize) -> String {
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
         sleep {}; k=0\n\
         while [ $k -lt $n ]; do [ -s \"$d/rc.$k\" ] || {{ : > \"$d/to.$k\"; L $k && K TERM $p $k t; }}; k=$((k+1)); done\n\
         sleep {KILL_GRACE_SECS}; k=0\n\
         while [ $k -lt $n ]; do [ -s \"$d/rc.$k\" ] || {{ L $k && K KILL $p $k t; }}; k=$((k+1)); done",
        op_secs()
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
        "for t in {REQUIRED_TOOLS}; do \
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
    s.push_str(&format!("R={}\n", shell_quote(&runner())));
    s.push_str(&format!("DL={}\n", shell_quote(&deadline())));
    s.push_str(&format!("W={}\n", shell_quote(&watcher())));
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
    s.push_str(&format!("F={}\n", shell_quote(&deliverer())));
    s.push_str(&format!(
        "TO {DELIVER_SECS} sh -c \"$F\" mule-deliver \"$d\" {}{} 3< /dev/null\n",
        ops.len(),
        (0..ops.len())
            .map(|k| format!(" \"$d/out.{k}\" \"$d/err.{k}\""))
            .collect::<String>()
    ));
    s
}
