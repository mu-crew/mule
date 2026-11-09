use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use base64::Engine;

use crate::config::{Config, Host};
use crate::probe::State;
use crate::transport::Transport;
use crate::wrapper::{JOBS_ROOT, JobId, state_dir};

/// How far back `ls` reaches for finished jobs, absent `--all`.
///
/// Long enough that a job you fired and forgot is still listed when you come
/// back to it, short enough that the default view does not become an archive.
/// Anything prune will eventually delete was therefore visible for its first
/// day.
const DEFAULT_LS_WINDOW_SECS: u64 = 24 * 60 * 60;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Row {
    pub id: String,
    pub host: String,
    pub state: State,
    pub age_secs: u64,
    pub runtime_secs: Option<u64>,
    pub cmd: String,
    /// The caller key the job was dispatched with (`run --key`), if any.
    pub key: Option<String>,
    /// The job has a `key` file that could not be read: whether it holds a
    /// given key cannot be told.
    pub key_unreadable: bool,
    /// Why this job's state cannot be told (`state` is then not an answer).
    pub unknown: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Unreachable {
    pub host: String,
    pub why: String,
    /// The command that would fix it, when there is one.
    ///
    /// `ls` treats a down master as information rather than an error, so it
    /// never built the typed `NoMaster` that carries this -- leaving the user a
    /// diagnosis with no remedy, unlike every other verb.
    pub remedy: Option<String>,
}

pub fn list(
    cfg: &Config,
    transport: &dyn Transport,
    host_filter: Option<&str>,
    all: bool,
) -> Result<(Vec<Row>, Vec<Unreachable>)> {
    let (rows, unreachable, _) = list_with_hidden(cfg, transport, host_filter, all)?;
    Ok((rows, unreachable))
}

pub fn list_with_hidden(
    cfg: &Config,
    transport: &dyn Transport,
    host_filter: Option<&str>,
    all: bool,
) -> Result<(Vec<Row>, Vec<Unreachable>, usize)> {
    let hosts: Vec<&Host> = match host_filter {
        Some(name) => vec![cfg.host(Some(name))?],
        None => cfg.hosts().iter().collect(),
    };
    let mut rows = Vec::new();
    let mut unreachable = Vec::new();
    let mut hidden = 0;

    // Each host takes the same ticket lock, so parallel calls would only queue
    // at the lock while making error reporting and ordering less predictable.
    for host in hosts {
        if !transport.master_alive(host) {
            unreachable.push(Unreachable {
                host: host.name.clone(),
                why: "no control master".into(),
                remedy: Some(crate::errors::master_command(host)),
            });
            continue;
        }
        let output = transport.run(host, &list_script(host))?;
        if output.code != 0 {
            bail!(
                "listing jobs on {} failed: {}",
                host.name,
                output.stderr.trim()
            );
        }
        hidden += parse_rows(host, &output.text(), all, &mut rows)?;
    }
    Ok((rows, unreachable, hidden))
}

/// One round trip, and a bounded number of processes regardless of job count.
///
/// The previous version was a shell loop forking four processes PER JOB -- a
/// `cat` for `rc`, a `tmux has-session`, a `stat`, and a `base64` for `cmd`.
/// Measured at 300 jobs: **14.1s**, all of it inside the ticket lock, so
/// nothing else mule-related could run. That breaks mule's own rule against
/// holding a capped channel for more than about a second, and since `keep_days`
/// defaults to 14, a few hundred jobs is ordinary rather than pathological.
///
/// Now three processes total -- one `tmux list-sessions`, one `find -exec stat`,
/// one `awk` -- and **0.13s** for the same 300 jobs, a 108x improvement. What
/// each step bought, measured separately: dropping per-job `has-session` took
/// 14.1s to 5.8s (each was a separate tmux client connection), batching `stat`
/// took it to 2.4s, and moving the file reads into awk took it to 0.13s.
///
/// Two portability notes, both load-bearing rather than defensive:
///
/// `stat`'s flags are mutually exclusive between BSD and GNU -- `-c` is an
/// illegal option on macOS and `-f` means "file system" on Linux -- so the
/// `||` fallback is required, not belt-and-braces.
///
/// `cmd` is base64-encoded inside awk. A command may contain a tab or newline,
/// either of which would corrupt the row format. Keeping the encoder in the
/// existing awk process avoids restoring the per-job `base64` forks that made
/// listing take 14.1s. The same batched stat includes `cmd` and `rc`: spawning
/// one stat per artifact would undo that improvement.
///
/// The live session names reach awk through its environment, not `-v`:
/// BSD awk (macOS) refuses a `-v` value holding a newline (`awk: newline in
/// string`), so with two live jobs on a macOS host every `ls` failed.
/// `ENVIRON` is POSIX and takes the value as it is.
fn list_script(host: &Host) -> String {
    format!(
        "{prelude}mule_socket={socket}; root={JOBS_ROOT}; \
         if mule_absent \"$root\" \"jobs root\"; then exit 0; fi; \
         [ -d \"$root\" ] || mule_u \"jobs root $root is not a directory\"; \
         mule_sessions; mule_now; now=$REPLY; \
         mule_stat_flavor; \
         listing=$(find \"$root\" -mindepth 1 -maxdepth 2 \\( \\( -type d ! -path \"$root/*/*\" \\) -o \\( -type f \\( -name cmd -o -name rc -o -name key \\) \\) \\) -exec stat $mule_sf \"$mule_sfmt\" {{}} +) \
           || mule_u \"cannot list the jobs under $root\"; \
         odd=$(find \"$root\" -mindepth 2 -maxdepth 2 \\( -name rc -o -name key \\) ! -type f) \
           || mule_u \"cannot list the jobs under $root\"; \
         printf '%s\\n' \"$listing\" | live=\"$SESSIONS\" odd=\"$odd\" awk -v now=\"$now\" '\
             BEGIN {{ \
               n = split(ENVIRON[\"live\"], L, \"\\n\"); \
               for (i = 1; i <= n; i++) if (sub(/^mule-/, \"\", L[i])) alive[L[i]] = 1; \
               n = split(ENVIRON[\"odd\"], O, \"\\n\"); \
               for (i = 1; i <= n; i++) if (O[i] != \"\") {{ \
                 p = O[i]; oid = p; sub(/\\/[^\\/]*$/, \"\", oid); sub(/.*\\//, \"\", oid); \
                 odd_why[oid] = p \" is not a regular file\"; \
                 if (p ~ /\\/key$/) odd_key[oid] = 1; \
               }} \
               alphabet = \"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/\"; \
               for (i = 0; i < 256; i++) ord[sprintf(\"%c\", i)] = i; \
             }} \
             function b64(s,   out, i, n, a, b, c) {{ \
               for (i = 1; i <= length(s); i += 3) {{ \
                 n = length(s) - i + 1; \
                 a = ord[substr(s, i, 1)]; \
                 b = n > 1 ? ord[substr(s, i + 1, 1)] : 0; \
                 c = n > 2 ? ord[substr(s, i + 2, 1)] : 0; \
                 out = out substr(alphabet, int(a / 4) + 1, 1); \
                 out = out substr(alphabet, (a % 4) * 16 + int(b / 16) + 1, 1); \
                 out = out (n > 1 ? substr(alphabet, (b % 16) * 4 + int(c / 64) + 1, 1) : \"=\"); \
                 out = out (n > 2 ? substr(alphabet, c % 64 + 1, 1) : \"=\"); \
               }} \
               return out; \
             }} \
             NF == 0 {{ next }} \
             {{ \
               mtime = $1; path = substr($0, length($1) + 2); \
               dir = path; kind = \"dir\"; \
               if (sub(/\\/cmd$/, \"\", dir)) kind = \"cmd\"; \
               else if (sub(/\\/rc$/, \"\", dir)) kind = \"rc\"; \
               else if (sub(/\\/key$/, \"\", dir)) kind = \"key\"; \
               id = dir; sub(/.*\\//, \"\", id); \
               dirs[id] = dir; \
               if (kind == \"dir\") dir_mtime[id] = mtime; \
               else if (kind == \"cmd\") cmd_mtime[id] = mtime; \
               else if (kind == \"rc\") rc_mtime[id] = mtime; \
               else key_mtime[id] = mtime; \
             }} \
             END {{ \
               for (id in dirs) {{ \
                 dir = dirs[id]; \
                 bad = (id in odd_why) ? odd_why[id] : \"\"; \
                 rc = \"\"; \
                 if (id in rc_mtime) {{ \
                   l = \"\"; if ((getline l < (dir \"/rc\")) > 0 && l != \"\") rc = l; \
                   else bad = \"cannot read \" dir \"/rc\"; \
                   close(dir \"/rc\"); \
                 }} \
                 cmd = \"\"; \
                 while ((getline l < (dir \"/cmd\")) > 0) cmd = (cmd == \"\") ? l : cmd \"\\n\" l; \
                 close(dir \"/cmd\"); \
                 key = \"\"; keyok = (id in odd_key) ? 0 : 1; \
                 if (id in key_mtime) {{ \
                   l = \"\"; if ((getline l < (dir \"/key\")) > 0 && l != \"\") key = l; \
                   else {{ keyok = 0; bad = \"cannot read \" dir \"/key\"; }} \
                   close(dir \"/key\"); \
                 }} \
                 if (bad == \"\" && rc == \"\" && !(id in cmd_mtime) && !(id in alive)) \
                   bad = \"no cmd: claimed by its key but not started\"; \
                 runtime = \"\"; \
                 if (rc != \"\" && (id in cmd_mtime) && (id in rc_mtime)) \
                   runtime = rc_mtime[id] - cmd_mtime[id]; \
                 else if (rc == \"\" && (id in alive) && (id in cmd_mtime)) \
                   runtime = now - cmd_mtime[id]; \
                 if (runtime != \"\" && runtime < 0) runtime = 0; \
                 printf \"%s\\t%s\\t%s\\t%s\\t%s\\t%s\\t%s\\t%s\\n\", \
                   id, now - dir_mtime[id], runtime, rc, (id in alive) ? 1 : 0, b64(cmd), \
                   keyok ? b64(key) : \"?\", b64(bad); \
               }} \
             }}' || mule_u \"cannot list the jobs under $root\"",
        prelude = crate::remote::PRELUDE,
        socket = host.tmux_socket,
    )
}

fn parse_rows(host: &Host, reply: &str, all: bool, rows: &mut Vec<Row>) -> Result<usize> {
    let mut hidden = 0;
    for line in reply.lines() {
        let mut fields = line.splitn(8, '\t');
        let id = fields.next().context("invalid ls reply: missing id")?;
        let age_secs = fields
            .next()
            .context("invalid ls reply: missing age")?
            .parse()
            .context("invalid age in ls reply")?;
        let runtime_text = fields.next().context("invalid ls reply: missing runtime")?;
        let runtime_secs = if runtime_text.is_empty() {
            None
        } else {
            Some(
                runtime_text
                    .parse()
                    .context("invalid runtime in ls reply")?,
            )
        };
        let rc_text = fields.next().context("invalid ls reply: missing rc")?;
        let alive = fields.next().context("invalid ls reply: missing alive")? == "1";
        let cmd = String::from_utf8_lossy(&decode_command(
            fields.next().context("invalid ls reply: missing cmd")?,
        )?)
        .into_owned();
        // Optional: a reply from before keys existed has six fields. Base64
        // in transit, like `cmd`, so a hand-edited `key` holding a tab cannot
        // shift a column. `?` (not in the base64 alphabet) is a key file that
        // exists but could not be read.
        let mut key_unreadable = false;
        let key = match fields.next() {
            Some("?") => {
                key_unreadable = true;
                None
            }
            Some(encoded) if !encoded.is_empty() => Some(
                String::from_utf8_lossy(&decode_command(encoded)?)
                    .trim_end()
                    .to_string(),
            ),
            _ => None,
        }
        .filter(|key| !key.is_empty());
        // Why the row's state cannot be told, if it cannot: an `rc` or `key`
        // that exists but would not read, or a claim with no `cmd`.
        let unknown = match fields.next() {
            Some(encoded) if !encoded.is_empty() => Some(
                String::from_utf8_lossy(&decode_command(encoded)?)
                    .trim_end()
                    .to_string(),
            ),
            _ => None,
        }
        .filter(|why| !why.is_empty());
        let rc = if rc_text.is_empty() {
            None
        } else {
            Some(rc_text.parse().context("invalid rc in ls reply")?)
        };
        let state = match rc {
            Some(code) => State::Done(code),
            None if alive => State::Running,
            None => State::Orphan,
        };
        // An orphan has no completion timestamp, so elapsed time since dispatch
        // is not its runtime. Keep the value absent rather than repeat AGE's
        // old mistake of giving one number two meanings.
        let runtime_secs = (!matches!(state, State::Orphan))
            .then_some(runtime_secs)
            .flatten();
        // Time-based, not state-based. Filtering on `done` treated finished
        // work as noise the caller had already seen -- true for a job watched
        // with `--wait`, false for every job dispatched and walked away from,
        // which is the mode this tool exists for. A short command is ALREADY
        // done when the user first looks, so a state filter made `ls` empty
        // exactly when it is the documented recovery path for a lost id.
        //
        // `running` and `orphan` are never hidden at any age: one is live, the
        // other is evidence.
        let recent = age_secs < DEFAULT_LS_WINDOW_SECS;
        if all || recent || unknown.is_some() || !matches!(state, State::Done(_)) {
            rows.push(Row {
                id: id.into(),
                host: host.name.clone(),
                state,
                age_secs,
                runtime_secs,
                cmd,
                key,
                key_unreadable,
                unknown,
            });
        } else {
            hidden += 1;
        }
    }
    Ok(hidden)
}

pub fn kill(transport: &dyn Transport, host: &Host, id: &JobId) -> Result<i32> {
    crate::errors::require_master(transport, host)?;
    let dir = state_dir(id);
    // Destroy is best-effort: a finished job, a second kill, or a watchdog that
    // already tore the session down must still return rc. `&& cat` made
    // kill-session's failure hide that no-op. Cancel watch-{id} too, or a
    // capped job's sleeper lives until max_secs and can overwrite rc with 124.
    let script = format!(
        "d={dir}; [ -f $d/rc ] || echo 137 > $d/rc; \
         tmux -L {socket} kill-session -t mule-{id} 2>/dev/null; \
         tmux -L {socket} kill-session -t watch-{id} 2>/dev/null; \
         cat $d/rc",
        socket = host.tmux_socket
    );
    let output = transport.run(host, &script)?;
    if output.code != 0 {
        bail!("kill failed: {}", output.stderr.trim());
    }
    output
        .text()
        .trim()
        .parse()
        .context("kill returned an invalid exit code")
}

/// What `rm` was asked to remove.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Target {
    One(JobId),
    /// Every finished job, ignoring `keep_days`.
    ///
    /// Deliberately NOT "everything": `--all` never stops work. A running job
    /// is spared, and so is an orphan -- it has no `rc`, and it is the one
    /// state that cannot be reconstructed, so it is evidence rather than mud.
    /// That is what makes `--all` safe enough to need no confirmation. `rm
    /// <id>` ends the named job; only this bulk path is non-destructive.
    AllDone,
}

/// The remote script behind `rm <id>`, exposed so layer 2 can drive it against
/// a private tmux server.
///
/// Exit 0 is a **proof**: neither `mule-<id>` nor `watch-<id>` exists, and the
/// state directory is gone. mu deletes its job handle and removes a worktree
/// only after this exits 0, so the old unconditional `exit 0` -- which ignored
/// a `kill-session` that did not take and an `rm -rf` that failed -- let a
/// still-running job lose its handle.
///
/// The caller named this job, so ending it is the intent; the check is what
/// makes that claim true. A missing job (no sessions, no directory) passes the
/// same checks and exits 0, so `rm` stays idempotent.
///
/// Failure keeps what is left. A surviving session is not killed a second
/// time, and the directory is not touched while it is alive: it is the live
/// job's log and the only way to find out why it would not die. A directory
/// that will not delete gets one `rm -rf` and no retry loop. The message names
/// which check failed; the exit is 1.
///
/// Absence has to be *seen*, not inferred from a failed command. `has-session`
/// exits 1 for "no such session", "no server" and also for a client/server
/// protocol mismatch or an unsafe socket directory -- the last two leave the
/// job running, so reading every non-zero as "gone" deleted a live job's
/// state (reproduced with a real session). So the check is `list-sessions`:
/// a successful list is authoritative for both names, and a failure counts as
/// absence only when tmux says there is no server (`no server running`, or a
/// missing or dead socket: `error connecting to ... (No such file or directory)`). Anything else exits 1.
///
/// The state directory gets the same treatment: `[ -d ]` is false for a
/// directory behind an unsearchable parent, so it cannot tell "missing" from
/// "cannot look". `ls -d` can, by its error (`LC_ALL=C` pins the text).
pub fn remove_one_script(host: &Host, id: &JobId) -> String {
    let dir = state_dir(id);
    let socket = &host.tmux_socket;
    // On the fail-closed prelude ([`crate::remote`]), with its stop status
    // set to 1, `rm`'s documented "not proven" exit. Each `kill-session`
    // must kill its session or prove it was not there; the sessions are
    // then seen gone in one list (a `case`, not a `grep` whose exit 2 once
    // read as "not listed"); the dir is seen gone with `ls -d` before and
    // after `rm -rf`.
    format!(
        "{prelude}mule_u_code=1; mule_socket={socket}; d={dir}; \
         {kill_mule}{kill_watch}\
         mule_sessions; alive=; \
         if mule_has mule-{id}; then alive=\"$alive mule-{id}\"; fi; \
         if mule_has watch-{id}; then alive=\"$alive watch-{id}\"; fi; \
         if [ -n \"$alive\" ]; then \
           echo \"session still alive after kill-session:$alive (state dir kept)\" >&2; exit 1; \
         fi; \
         if mule_absent \"$d\" \"state dir\"; then exit 0; fi; \
         rm -rf \"$d\" 2>/dev/null || {{ echo \"state dir $d could not be removed (kept)\" >&2; exit 1; }}; \
         if mule_absent \"$d\" \"state dir\"; then echo {id}; exit 0; fi; \
         echo \"state dir $d could not be removed (kept)\" >&2; exit 1",
        prelude = crate::remote::PRELUDE,
        kill_mule = kill_session_proven(socket, &format!("mule-{id}")),
        kill_watch = kill_session_proven(socket, &format!("watch-{id}")),
    )
}

/// `kill-session` of `name`, which must kill it or prove it was not there
/// (`can't find session`, or tmux's own "no server"); anything else stops
/// the script, cannot tell.
fn kill_session_proven(socket: &str, name: &str) -> String {
    format!(
        "e=$(tmux -L {socket} kill-session -t {name} 2>&1) || case $e in \
           *\"can't find session\"*|*'no server running'*|*'error connecting to'*'No such file or directory'*) ;; \
           *) mule_u \"cannot verify session state: cannot kill {name}: $e\";; \
         esac; "
    )
}

/// Remove job state. Returns the ids removed, so the caller can report them.
///
/// One round trip either way: enumeration and removal share a single remote
/// script, because a list-then-delete pair would take the lock twice and could
/// act on a job whose state changed in between.
pub fn remove(transport: &dyn Transport, host: &Host, target: &Target) -> Result<Vec<String>> {
    crate::errors::require_master(transport, host)?;

    let script = match target {
        Target::One(id) => remove_one_script(host, id),
        // Presence of `rc` IS the definition of finished, the same test prune
        // uses -- so this is "prune now, ignoring the horizon".
        Target::AllDone => format!(
            "root={JOBS_ROOT}; [ -d \"$root\" ] || exit 0; \
             for d in \"$root\"/*; do \
               [ -d \"$d\" ] && [ -f \"$d/rc\" ] || continue; \
               rm -rf \"$d\" && echo \"${{d##*/}}\"; \
             done; exit 0"
        ),
    };

    let output = transport.run(host, &script)?;
    if output.code != 0 {
        bail!("rm failed on {}: {}", host.name, output.stderr.trim());
    }
    Ok(output
        .text()
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(str::to_string)
        .collect())
}

/// How much longer an `orphan` is kept than a finished job.
///
/// An orphan is evidence -- the host rebooted, or something killed the session
/// -- and since `kill` writes rc 137, it means strictly "not mule's doing". So
/// it outlives ordinary output by a wide margin. But not forever: a disk-full
/// incident produces orphans holding the largest logs on the host, and those
/// were exactly the directories an unconditional exemption refused to touch,
/// leaving permanent residue only a human could clear.
const ORPHAN_KEEP_MULTIPLIER: u32 = 4;

pub fn prune(host: &Host) -> String {
    let orphan_days = host.keep_days.saturating_mul(ORPHAN_KEEP_MULTIPLIER);
    // Two passes, because the two states have different horizons.
    //
    // A `running` job has no `rc`, so the orphan pass would match it by age
    // alone. Skip live `mule-{id}` sessions: keep_days can be 1, which makes
    // the orphan horizon four days, and a multi-day job is legitimate work.
    //
    // Deleting is the one act here that cannot be undone, so every delete
    // needs a proof and anything less stops the whole prune, deleting
    // nothing more ([`crate::remote`]): a dir goes only with its `key` seen
    // not to exist (a keyed job is never pruned: its key must keep returning
    // it), and either an `rc` read as a regular file (done) or, for an
    // orphan, `rc` seen absent, `cmd` present and neither session in a list
    // tmux gave. A `find -exec test` that failed used to read as "no key"
    // and delete a keyed job (review of P7a, round 2). Runs in a subshell, so
    // a stop ends the prune and never the dispatch it rides in.
    format!(
        "( {prelude}mule_socket={socket}; root={JOBS_ROOT}; \
           if mule_absent \"$root\" \"jobs root\"; then exit 0; fi; \
           old=$(find \"$root\" -mindepth 1 -maxdepth 1 -type d -mtime +{keep}) \
             || mule_u \"cannot list old jobs\"; \
           printf '%s\\n' \"$old\" | while IFS= read -r d; do \
             [ -n \"$d\" ] || continue; \
             mule_absent \"$d/key\" key || continue; \
             mule_regular \"$d/rc\" rc || continue; \
             rm -rf \"$d\" || mule_u \"cannot remove $d\"; \
           done || exit 0; \
           old=$(find \"$root\" -mindepth 1 -maxdepth 1 -type d -mtime +{orphan_days}) \
             || mule_u \"cannot list old jobs\"; \
           [ -n \"$old\" ] || exit 0; \
           mule_sessions; \
           printf '%s\\n' \"$old\" | while IFS= read -r d; do \
             [ -n \"$d\" ] || continue; \
             id=\"${{d##*/}}\"; \
             mule_absent \"$d/key\" key || continue; \
             mule_regular \"$d/rc\" rc && continue; \
             mule_regular \"$d/cmd\" cmd || continue; \
             if mule_has \"mule-$id\" || mule_has \"watch-$id\"; then continue; fi; \
             rm -rf \"$d\" || mule_u \"cannot remove $d\"; \
           done ) 2>/dev/null",
        prelude = crate::remote::PRELUDE,
        socket = host.tmux_socket,
        keep = host.keep_days,
    )
}

pub fn decode_command(input: &str) -> Result<Vec<u8>> {
    base64::engine::general_purpose::STANDARD
        .decode(input)
        .context("invalid command encoding in ls reply")
}

/// How often a stop child re-checks a missing master. `-O check` is lock-free,
/// opens no session and is killed after 2s, so polling it costs the capped
/// channel nothing.
pub const MASTER_POLL: Duration = Duration::from_secs(5);

/// The backoff between failed `rm` attempts: 5, 10, 20, then 30s from there.
/// It runs after the ticket is released, so it holds no queue slot.
const RETRY_BACKOFF: [Duration; 4] = [
    Duration::from_secs(5),
    Duration::from_secs(10),
    Duration::from_secs(20),
    Duration::from_secs(30),
];

/// The stop-failure record's cap (cross-repo contract § 2): mu shows it in
/// `agent list`, escaped and cut again on its side.
pub const RECORD_MAX_BYTES: usize = 512;

/// A retrying stop, as `rm --retry-for` runs it.
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    /// Retry until this much monotonic time has passed. A soft deadline: no
    /// attempt starts once it has passed, but an attempt in flight has no
    /// bound (the ticket wait is unbounded by design, SPEC § Contention).
    pub retry_for: Duration,
    /// The generation the carrier owns (fixed before it owned the job, never
    /// read when signalled). `None` (exec's stop) is unfenced: nothing
    /// reattaches to an exec job.
    pub fence_gen: Option<u64>,
    /// Where to write the stop-failure record on giving up, and what to remove
    /// on success.
    pub failed_file: Option<PathBuf>,
}

/// Time, injected so layer 1 can drive a ten-minute deadline in no time.
pub trait Clock {
    /// Monotonic time since some fixed start.
    fn now(&self) -> Duration;
    fn sleep(&self, duration: Duration);
    /// Wall-clock seconds, for the record's `at unix <t>` only.
    fn unix_now(&self) -> u64;
}

/// The real clock: `Instant` for the deadline, so a wall-clock step cannot
/// cut a stop short or stretch it.
pub struct RealClock(std::time::Instant);

impl RealClock {
    pub fn new() -> Self {
        Self(std::time::Instant::now())
    }
}

impl Default for RealClock {
    fn default() -> Self {
        Self::new()
    }
}

impl Clock for RealClock {
    fn now(&self) -> Duration {
        self.0.elapsed()
    }
    fn sleep(&self, duration: Duration) {
        std::thread::sleep(duration);
    }
    fn unix_now(&self) -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |t| t.as_secs())
    }
}

/// How a retrying stop ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StopOutcome {
    /// `rm` proved the job gone; the ids removed (empty: it already was).
    Removed(Vec<String>),
    /// The fence generation moved: the job was reattached since this stop
    /// began. Nothing was removed, written or deleted.
    Fenced,
    /// The deadline passed. `line` is the record, written to `failed_file`
    /// when one was given; `code` is 3 if the master was never seen, else 1.
    GaveUp { line: String, code: i32 },
}

impl StopOutcome {
    /// `rm --retry-for`'s exit. Nobody reads a detached child's code, but a
    /// human running the flag by hand gets `rm`'s meanings: 0 the job is gone
    /// (or no longer this stop's to end), 3 no master, 1 anything else.
    pub fn exit_code(&self) -> i32 {
        match self {
            Self::Removed(_) | Self::Fenced => 0,
            Self::GaveUp { code, .. } => *code,
        }
    }
}

/// The lease's signal-path stop: retry `rm <id>` until it proves the job gone,
/// the fence says the job was reattached, or `policy.retry_for` runs out --
/// then record the failure rather than leave it silent.
///
/// The 2026-11-04 aiv-blog incident is why: a tmux restart hung up six lease
/// panes, and on the host whose master was down each detached `rm` failed once
/// in `require_master`, into `/dev/null`, and the jobs ran on unseen.
///
/// Master down: no `rm`, a lock-free check every [`MASTER_POLL`]. The child
/// never opens a master or prompts; it waits for the human the dead pane
/// already asked. Master up: one whole `rm` under the fence lock, through the
/// ticketed [`Transport::run`].
pub fn remove_with_retry(
    t: &dyn Transport,
    host: &Host,
    id: &JobId,
    policy: &RetryPolicy,
    clock: &dyn Clock,
    fence: &crate::stopfence::StopFence,
) -> Result<StopOutcome> {
    let started = clock.now();
    let deadline = started + policy.retry_for;
    let mut attempts = 0u32;
    let mut last_reason: Option<String> = None;
    let mut failures = 0usize;
    loop {
        // Fenced or not, the lock is held for the whole attempt: a reattach
        // waits it out, and an attempt never starts on a stale generation.
        let mut held = match policy.fence_gen {
            Some(generation) => {
                let mut lock = fence.lock()?;
                if lock.generation()? != generation {
                    return Ok(StopOutcome::Fenced);
                }
                Some(lock)
            }
            None => None,
        };
        let wait = if !t.master_alive(host) {
            drop(held.take());
            MASTER_POLL
        } else {
            attempts += 1;
            match remove(t, host, &Target::One(id.clone())) {
                Ok(removed) => {
                    // Proven gone, so the fence can go: a waiter on the old
                    // inode then probes Missing, which is harmless.
                    if let Some(lock) = held {
                        let _ = lock.unlink();
                    }
                    // Best effort, and only this job's own record.
                    if let Some(path) = &policy.failed_file {
                        let _ = std::fs::remove_file(path);
                    }
                    return Ok(StopOutcome::Removed(removed));
                }
                Err(error) => {
                    drop(held.take());
                    last_reason = Some(first_line(&format!("{error:#}")));
                    let wait = RETRY_BACKOFF[failures.min(RETRY_BACKOFF.len() - 1)];
                    failures += 1;
                    wait
                }
            }
        };
        let now = clock.now();
        if now >= deadline {
            break;
        }
        clock.sleep(wait.min(deadline - now));
        // Again after the sleep, immediately before the next attempt: the
        // clamped last sleep lands on the deadline, and a sleep stretched by
        // scheduling or SIGSTOP can wake far past it (review of mule_stop:
        // asked for 1s, woke at 900s, and a new `rm` ran). The soft bound
        // covers only an attempt already in flight, never a new one.
        if clock.now() >= deadline {
            break;
        }
    }

    let code = if last_reason.is_none() {
        crate::errors::EXIT_NO_MASTER
    } else {
        1
    };
    let line = record_line(
        id,
        &host.name,
        last_reason.as_deref().unwrap_or("no control master"),
        attempts,
        (clock.now() - started).as_secs(),
        clock.unix_now(),
    );
    if let Some(path) = &policy.failed_file {
        // Under the fence and after one more look at it: a reattach that
        // landed during the last sleep must not get a record saying its job
        // was abandoned.
        let _held = match policy.fence_gen {
            Some(generation) => {
                let mut lock = fence.lock()?;
                if lock.generation()? != generation {
                    return Ok(StopOutcome::Fenced);
                }
                Some(lock)
            }
            None => None,
        };
        crate::lease::write_line_file(path, &line)
            .with_context(|| format!("cannot write stop record {}: {line}", path.display()))?;
    }
    Ok(StopOutcome::GaveUp { line, code })
}

fn first_line(text: &str) -> String {
    text.lines().next().unwrap_or("").trim().to_string()
}

/// `<id> on <host> not stopped: <reason> (gave up after <n> attempts, <s>s; at
/// unix <t>)`, at most [`RECORD_MAX_BYTES`].
///
/// The reason is shortened first, on a UTF-8 char boundary, so the id, the
/// host and the counts survive: mu matches the record by its leading id.
pub fn record_line(
    id: &JobId,
    host: &str,
    reason: &str,
    attempts: u32,
    secs: u64,
    unix: u64,
) -> String {
    let head = format!("{id} on {host} not stopped: ");
    let tail = format!(" (gave up after {attempts} attempts, {secs}s; at unix {unix})");
    let room = RECORD_MAX_BYTES.saturating_sub(head.len() + tail.len());
    let line = format!("{head}{}{tail}", cut_utf8(reason, room));
    // Only reachable with a host name of hundreds of bytes; the id leads, so
    // it still survives the cut.
    cut_utf8(&line, RECORD_MAX_BYTES).to_string()
}

fn cut_utf8(text: &str, max: usize) -> &str {
    if text.len() <= max {
        return text;
    }
    let mut end = max;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    &text[..end]
}
