//! The fail-closed discipline every state-reading remote snippet follows.
//!
//! A snippet that reads or searches remote state (the probe behind `poll`,
//! `wait` and the lease; `ls`; prune; `rm`'s proof; the key lookup and the
//! keyed lease's claim) answers from external commands -- `ls`, `cat`,
//! `find`, `awk`, `stat`, `tmux` -- each of which can fail. One rule (SPEC §
//! Remote snippets fail closed): **each external command's exit status is
//! checked; any failure, partial output, permission or I/O error, or
//! unexpected output yields "cannot tell", and only an explicit positive
//! proof yields an absence** (`missing`, "no such key", "not live", "gone").
//!
//! Two review rounds of P7a found the rule broken one site at a time: a
//! `grep` exit 2 read as "not listed", a `cat` exit 2 whose partial `1` was
//! taken as the rc, a failing `find -exec test` that made prune delete a
//! keyed job, a symlinked key that `find -type f` silently skipped. So the
//! checks live here, once, as shell functions every snippet starts with, and
//! `tests/fault_injection.rs` fails every external command of every snippet
//! in turn to hold them to it.
//!
//! The helpers stop the snippet with [`CANNOT_TELL`] (or the snippet's own
//! `mule_u_code`) and a reason on stderr. Shell builtins (`[`, `printf`,
//! `case`, `read`) cannot fail that way, but `[ -e ]` and friends answer
//! "false" for "could not look", so a builtin's "no" is never an absence on
//! its own: [`PRELUDE`]'s `mule_absent` proves it with `ls -d`.

/// The exit status of a snippet that could not tell.
///
/// Distinct from ssh's 255, from a shell's own syntax and command-not-found
/// statuses (2, 126, 127) and from `rm`'s documented 1, so the probe can tell
/// "unknown" from a broken call.
pub const CANNOT_TELL: i32 = 86;

/// Shell functions every state-reading snippet begins with.
///
/// POSIX sh, and run as written by the login shell on the host (bash, zsh
/// and dash are all exercised): no `local`, no arrays, no reliance on word
/// splitting (zsh does not split unquoted expansions), and no globs (zsh
/// aborts on one that matches nothing). Set `mule_socket` before calling
/// the session helpers.
///
/// - `mule_u REASON`: stop, cannot tell.
/// - `mule_absent PATH [WHAT]`: 0 if PATH is proven not to exist (`ls -d`
///   says `No such file or directory`), 1 if it exists; anything else stops.
/// - `mule_regular PATH [WHAT]`: 0 for a regular file that is not a
///   symlink, 1 if proven absent; a symlink, a directory or anything that
///   cannot be looked at stops. Keys and `rc` are read only this way: a
///   symlink is refused, not followed. One `ls -ld` decides, from the type
///   its mode string names or from its "No such file" error: one
///   observation, so a file that appears mid-check (a job writing `rc` as
///   it ends) is seen either absent or regular, never "not a regular file".
///   The first version tested `[ -f ]` and then proved absence with a second
///   look, and `rc` landing between the two stopped `exec` and `wait`
///   (review of P7a, round 3).
/// - `mule_read PATH [WHAT]`: `REPLY` = its content, which must be
///   non-empty and read with `cat` exiting 0; a missing file stops too.
/// - `mule_sessions`: `SESSIONS` = the server's session names, one per
///   line; empty only on tmux's own "no server" errors. `server exited
///   unexpectedly` (the server went mid-request, as it does when the job
///   that just ended held its last session) is no answer either way, so it
///   is observed once more, and that second answer must be a proof.
///   Measured: an `ls` racing a fast job's end under a loaded suite.
/// - `mule_has NAME`: is NAME in `SESSIONS`? A `case`, not `grep`: nothing
///   to fail.
/// - `mule_mtime PATH`: `REPLY` = its mtime in epoch seconds, digits only
///   (`date -r`, which GNU, BSD and busybox all take as a file).
/// - `mule_stat_flavor`: `mule_sf`/`mule_sfmt` for an mtime-and-name `stat`,
///   from `uname -s`. Not by trying one flavor and falling back on failure:
///   a fallback is a failure read as an answer, which is what this module
///   exists to forbid.
/// - `mule_now`: `REPLY` = `date +%s`, digits only.
/// - `mule_size PATH`: `REPLY` = its size in bytes, 0 if proven absent.
pub const PRELUDE: &str = r#"mule_u_code=86
mule_nl='
'
mule_u() { printf 'mule: cannot tell: %s\n' "$*" >&2; exit "$mule_u_code"; }
mule_absent() {
  mule_e=$(LC_ALL=C ls -d "$1" 2>&1) && return 1
  case $mule_e in *'No such file or directory'*) return 0;; esac
  mule_u "cannot verify ${2:-path} $1: $mule_e"
}
mule_regular() {
  if mule_e=$(LC_ALL=C ls -ld "$1" 2>&1); then
    case $mule_e in
      -*) return 0;;
      l*) mule_u "${2:-file} $1 is a symlink";;
      *) mule_u "${2:-file} $1 is not a regular file";;
    esac
  fi
  case $mule_e in *'No such file or directory'*) return 1;; esac
  mule_u "cannot verify ${2:-file} $1: $mule_e"
}
mule_read() {
  mule_regular "$1" "$2" || mule_u "${2:-file} $1 is missing"
  REPLY=$(cat "$1") || mule_u "cannot read ${2:-file} $1"
  [ -n "$REPLY" ] || mule_u "${2:-file} $1 is empty"
}
mule_sessions_once() {
  SESSIONS=$(tmux -L "$mule_socket" list-sessions -F '#{session_name}' 2>&1) && return 0
  case $SESSIONS in
    *'no server running'*|*'error connecting to'*'No such file or directory'*) SESSIONS=; return 0;;
    *'server exited unexpectedly'*) return 2;;
  esac
  mule_u "cannot verify session state: $SESSIONS"
}
mule_sessions() {
  mule_sessions_once && return 0
  mule_sessions_once && return 0
  mule_u "cannot verify session state: $SESSIONS"
}
mule_has() {
  case "$mule_nl$SESSIONS$mule_nl" in *"$mule_nl$1$mule_nl"*) return 0;; esac
  return 1
}
mule_digits() {
  case $REPLY in ''|*[!0-9]*) mule_u "$1: not a number: $REPLY";; esac
}
mule_mtime() {
  REPLY=$(date -r "$1" +%s) || mule_u "cannot read the mtime of $1"
  mule_digits "mtime of $1"
}
mule_stat_flavor() {
  REPLY=$(uname -s) || mule_u "cannot tell the system"
  case $REPLY in
    Linux|*CYGWIN*|*MINGW*|*MSYS*) mule_sf=-c; mule_sfmt='%Y %n';;
    Darwin|*BSD|DragonFly) mule_sf=-f; mule_sfmt='%m %N';;
    *) mule_u "unknown system for stat: $REPLY";;
  esac
}
mule_now() {
  REPLY=$(date +%s) || mule_u "cannot read the clock"
  mule_digits "the clock"
}
mule_size() {
  mule_regular "$1" "${2:-file}" || { REPLY=0; return 0; }
  REPLY=$(wc -c < "$1") || mule_u "cannot size $1"
  case $REPLY in ''|*[!0-9' ']*) mule_u "size of $1: not a number: $REPLY";; esac
  REPLY=$((REPLY))
}
"#;
