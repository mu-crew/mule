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
    pub cmd: String,
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
/// Measured at 400 jobs: **14.1s**, all of it inside the ticket lock, so
/// nothing else coop-related could run. That breaks coop's own rule against
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
/// listing take 14.1s.
fn list_script(host: &Host) -> String {
    format!(
        "root={JOBS_ROOT}; [ -d \"$root\" ] || exit 0; \
         live=$(tmux -L {} list-sessions -F '#{{session_name}}' 2>/dev/null | sed 's/^coop-//'); \
         {{ find \"$root\" -mindepth 1 -maxdepth 1 -type d -exec stat -c '%Y %n' {{}} + 2>/dev/null \
            || find \"$root\" -mindepth 1 -maxdepth 1 -type d -exec stat -f '%m %N' {{}} + ; }} \
         | awk -v now=\"$(date +%s)\" -v live=\"$live\" '\
             BEGIN {{ \
               n = split(live, L, \"\\n\"); \
               for (i = 1; i <= n; i++) if (L[i] != \"\") alive[L[i]] = 1; \
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
             {{ \
               mtime = $1; dir = substr($0, length($1) + 2); \
               id = dir; sub(/.*\\//, \"\", id); \
               rc = \"\"; if ((getline l < (dir \"/rc\")) > 0) rc = l; \
               close(dir \"/rc\"); \
               cmd = \"\"; \
               while ((getline l < (dir \"/cmd\")) > 0) cmd = (cmd == \"\") ? l : cmd \"\\n\" l; \
               close(dir \"/cmd\"); \
               printf \"%s\\t%s\\t%s\\t%s\\t%s\\n\", \
                 id, now - mtime, rc, (id in alive) ? 1 : 0, b64(cmd); \
             }}'",
        host.tmux_socket
    )
}

fn parse_rows(host: &Host, reply: &str, all: bool, rows: &mut Vec<Row>) -> Result<usize> {
    let mut hidden = 0;
    for line in reply.lines() {
        let mut fields = line.splitn(5, '\t');
        let id = fields.next().context("invalid ls reply: missing id")?;
        let age_secs = fields
            .next()
            .context("invalid ls reply: missing age")?
            .parse()
            .context("invalid age in ls reply")?;
        let rc_text = fields.next().context("invalid ls reply: missing rc")?;
        let alive = fields.next().context("invalid ls reply: missing alive")? == "1";
        let cmd = String::from_utf8_lossy(&decode_command(
            fields.next().context("invalid ls reply: missing cmd")?,
        )?)
        .into_owned();
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
        if all || recent || !matches!(state, State::Done(_)) {
            rows.push(Row {
                id: id.into(),
                host: host.name.clone(),
                state,
                age_secs,
                cmd,
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
         tmux -L {socket} kill-session -t coop-{id} 2>/dev/null; \
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

/// Remove job state. Returns the ids removed, so the caller can report them.
///
/// One round trip either way: enumeration and removal share a single remote
/// script, because a list-then-delete pair would take the lock twice and could
/// act on a job whose state changed in between.
pub fn remove(transport: &dyn Transport, host: &Host, target: &Target) -> Result<Vec<String>> {
    crate::errors::require_master(transport, host)?;

    let script = match target {
        // A single id still kills first: the caller named this job, so ending
        // it is the intent. Only the bulk path is non-destructive.
        Target::One(id) => {
            let dir = state_dir(id);
            format!(
                "tmux -L {} kill-session -t coop-{id} 2>/dev/null; \
                 if [ -d {dir} ]; then rm -rf {dir} && echo {id}; fi; exit 0",
                host.tmux_socket
            )
        }
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
/// -- and since `kill` writes rc 137, it means strictly "not coop's doing". So
/// it outlives ordinary output by a wide margin. But not forever: a disk-full
/// incident produces orphans holding the largest logs on the host, and those
/// were exactly the directories an unconditional exemption refused to touch,
/// leaving permanent residue only a human could clear.
const ORPHAN_KEEP_MULTIPLIER: u32 = 4;

pub fn prune(host: &Host) -> String {
    let orphan_days = host.keep_days.saturating_mul(ORPHAN_KEEP_MULTIPLIER);
    // Two passes, because the two states have different horizons and `find`
    // cannot express "has rc OR is much older" in one predicate without
    // becoming unreadable.
    //
    // A `running` job has no `rc`, so the orphan pass would match it by age
    // alone. Skip live `coop-{id}` sessions: keep_days can be 1, which makes
    // the orphan horizon four days, and a multi-day job is legitimate work.
    format!(
        "root={JOBS_ROOT}; [ ! -d \"$root\" ] || {{ \
         live=$(tmux -L {} list-sessions -F '#{{session_name}}' 2>/dev/null); \
         find \"$root\" -mindepth 1 -maxdepth 1 -type d -mtime +{} \
           -exec test -f '{{}}/rc' \\; -exec rm -rf '{{}}' + ; \
         find \"$root\" -mindepth 1 -maxdepth 1 -type d -mtime +{orphan_days} \
           -exec test ! -f '{{}}/rc' \\; -print | while IFS= read -r d; do \
             id=\"${{d##*/}}\"; \
             echo \"$live\" | grep -qx \"coop-$id\" && continue; \
             rm -rf \"$d\"; \
           done; }}",
        host.tmux_socket, host.keep_days
    )
}

pub fn decode_command(input: &str) -> Result<Vec<u8>> {
    base64::engine::general_purpose::STANDARD
        .decode(input)
        .context("invalid command encoding in ls reply")
}
