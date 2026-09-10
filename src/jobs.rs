use anyhow::{Context, Result, bail};

use crate::config::{Config, Host};
use crate::lock::with_lock;
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
    pub rc: Option<i32>,
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
    let hosts: Vec<&Host> = match host_filter {
        Some(name) => vec![cfg.host(Some(name))?],
        None => cfg.hosts().iter().collect(),
    };
    let mut rows = Vec::new();
    let mut unreachable = Vec::new();

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
        let output = with_lock(&host.name, || transport.run(host, &list_script(host)))??;
        if output.code != 0 {
            bail!(
                "listing jobs on {} failed: {}",
                host.name,
                output.stderr.trim()
            );
        }
        parse_rows(host, &output.text(), all, &mut rows)?;
    }
    Ok((rows, unreachable))
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
/// `cmd` is **hex**-encoded rather than base64. A command may contain a tab or
/// a newline, either of which would corrupt the row format, so it must be
/// encoded somehow; base64 would mean one `base64` fork per job, which is the
/// single most expensive thing this rewrite removes. Hex costs a 256-entry
/// lookup table in awk and decodes trivially on this side.
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
               for (i = 0; i < 256; i++) hex[sprintf(\"%c\", i)] = sprintf(\"%02x\", i); \
             }} \
             function tohex(s,   out, i) {{ \
               for (i = 1; i <= length(s); i++) out = out hex[substr(s, i, 1)]; \
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
                 id, now - mtime, rc, (id in alive) ? 1 : 0, tohex(cmd); \
             }}'",
        host.tmux_socket
    )
}

fn parse_rows(host: &Host, reply: &str, all: bool, rows: &mut Vec<Row>) -> Result<()> {
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
        let cmd = decode_hex(fields.next().context("invalid ls reply: missing cmd")?)?;
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
                rc,
                age_secs,
                cmd,
            });
        }
    }
    Ok(())
}

pub fn kill(transport: &dyn Transport, host: &Host, id: &JobId) -> Result<()> {
    crate::errors::require_master(transport, host)?;
    let dir = state_dir(id);
    let script = format!(
        "d={dir}; [ -f $d/rc ] || echo 137 > $d/rc; tmux -L {} kill-session -t coop-{id}",
        host.tmux_socket
    );
    run_mutation(transport, host, &script, "kill")
}

/// What `rm` was asked to remove.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Target {
    One(JobId),
    /// Every finished job, ignoring `keep_days`.
    ///
    /// Deliberately NOT "everything": `rm` never stops work. A running job is
    /// spared, and so is an orphan -- it has no `rc`, and it is the one state
    /// that cannot be reconstructed, so it is evidence rather than mud. `kill`
    /// is the only verb that ends a job, which is what makes `--all` safe
    /// enough to need no confirmation.
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

    let output = with_lock(&host.name, || transport.run(host, &script))??;
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

fn run_mutation(
    transport: &dyn Transport,
    host: &Host,
    script: &str,
    operation: &str,
) -> Result<()> {
    let output = with_lock(&host.name, || transport.run(host, script))??;
    if output.code != 0 {
        bail!("{operation} failed: {}", output.stderr.trim());
    }
    Ok(())
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
    // A `running` job is matched by neither: it has no `rc`, and its session is
    // alive, so it is skipped regardless of age. Directory mtime updates when
    // `rc` is written, so a long job's clock effectively starts when it
    // finishes rather than when it was dispatched.
    format!(
        "root={JOBS_ROOT}; [ ! -d \"$root\" ] || {{ \
         find \"$root\" -mindepth 1 -maxdepth 1 -type d -mtime +{} \
           -exec test -f '{{}}/rc' \\; -exec rm -rf '{{}}' + ; \
         find \"$root\" -mindepth 1 -maxdepth 1 -type d -mtime +{orphan_days} \
           -exec test ! -f '{{}}/rc' \\; -exec rm -rf '{{}}' + ; }}",
        host.keep_days
    )
}

/// Decode the hex `cmd` field from an `ls` reply.
///
/// Hex rather than base64 because the remote side encodes it in awk: base64
/// would mean forking `base64` once per job, which was the most expensive part
/// of the listing. A command can contain a tab or a newline, so it has to be
/// encoded either way.
fn decode_hex(input: &str) -> Result<String> {
    if !input.len().is_multiple_of(2) {
        bail!("invalid cmd encoding in ls reply: odd length");
    }
    let bytes = input
        .as_bytes()
        .chunks(2)
        .map(|pair| {
            let text = std::str::from_utf8(pair).context("invalid cmd encoding in ls reply")?;
            u8::from_str_radix(text, 16).context("invalid cmd encoding in ls reply")
        })
        .collect::<Result<Vec<u8>>>()?;
    // Lossy only here: this is the display copy for a human-readable table, and
    // a command that is not valid UTF-8 should still show as something rather
    // than failing the whole listing.
    Ok(String::from_utf8_lossy(&bytes).into_owned())
}
