use std::io::Write;

use anyhow::{Context, Result, bail};

use crate::config::Host;
use crate::key::JobKey;
use crate::transport::Transport;
use crate::wrapper::{JOBS_ROOT, Job, JobId, JobMetadata, JobMode, dispatch_script, new_id};

pub fn dispatch(
    transport: &dyn Transport,
    host: &Host,
    cmd: &str,
    cwd: Option<&str>,
    max_secs: Option<u64>,
    metadata: JobMetadata,
    mode: JobMode,
) -> Result<JobId> {
    dispatch_with_warnings(
        transport,
        host,
        cmd,
        cwd,
        max_secs,
        metadata,
        mode,
        &mut std::io::stderr(),
    )
}

#[doc(hidden)]
#[allow(clippy::too_many_arguments)] // The test seam mirrors run's independent CLI fields plus its warning sink.
pub fn dispatch_with_warnings(
    transport: &dyn Transport,
    host: &Host,
    cmd: &str,
    cwd: Option<&str>,
    max_secs: Option<u64>,
    metadata: JobMetadata,
    mode: JobMode,
    warnings: &mut dyn Write,
) -> Result<JobId> {
    // Generated, so it parses by construction; the parse is what keeps a
    // CLI-supplied id from reaching the remote shell unvalidated.
    let id = new_id().parse::<JobId>().expect("generated ids are valid");
    dispatch_with_id(
        transport, host, id, cmd, cwd, max_secs, metadata, mode, warnings,
    )
}

/// Dispatch under an id the caller already holds.
///
/// `mule exec` needs the id before the ssh call returns: a signal that lands
/// mid-dispatch must still be able to name the job it may have started, and
/// `rm` of an id that never dispatched is a no-op.
#[doc(hidden)]
#[allow(clippy::too_many_arguments)] // Same seam as `dispatch_with_warnings`, plus the id.
pub fn dispatch_with_id(
    transport: &dyn Transport,
    host: &Host,
    id: JobId,
    cmd: &str,
    cwd: Option<&str>,
    max_secs: Option<u64>,
    metadata: JobMetadata,
    mode: JobMode,
    warnings: &mut dyn Write,
) -> Result<JobId> {
    dispatch_keyed(
        transport, host, id, None, cmd, cwd, max_secs, metadata, mode, warnings,
    )
    .map(Dispatched::into_id)
}

/// What a keyed dispatch did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Dispatched {
    /// This call started the job, under the id it was handed.
    New(JobId),
    /// A job with the key already existed, so nothing was started: this is
    /// that job's id, whatever its state.
    Existing(JobId),
}

impl Dispatched {
    pub fn id(&self) -> &JobId {
        match self {
            Self::New(id) | Self::Existing(id) => id,
        }
    }

    pub fn into_id(self) -> JobId {
        match self {
            Self::New(id) | Self::Existing(id) => id,
        }
    }
}

/// [`dispatch_with_id`], with an optional caller key (`run --key`).
///
/// With a key, one round trip checks for a job already holding it and, only
/// if there is none, writes `$d/key` and starts the job: a dispatch whose key
/// exists returns that job and starts nothing. The id stays generated.
///
/// The check and the start share one remote script, so they are atomic with
/// respect to every other mule call on this client: each holds the host's
/// ticket lock for its whole round trip. That is the same guarantee every
/// other read-then-act script here rests on (`rm`'s proof, `rm --all`).
#[doc(hidden)]
#[allow(clippy::too_many_arguments)] // `dispatch_with_id`'s seam, plus the key.
pub fn dispatch_keyed(
    transport: &dyn Transport,
    host: &Host,
    id: JobId,
    key: Option<&JobKey>,
    cmd: &str,
    cwd: Option<&str>,
    max_secs: Option<u64>,
    metadata: JobMetadata,
    mode: JobMode,
    warnings: &mut dyn Write,
) -> Result<Dispatched> {
    // `ssh -O check` measured at 0s and opens no session channel, so it is the
    // one transport call deliberately outside the lock.
    if !transport.master_alive(host) {
        return Err(crate::errors::no_master(host));
    }

    let job = Job {
        id,
        cmd: cmd.to_owned(),
        cwd: cwd.map(str::to_owned),
        max_secs: max_secs.unwrap_or(host.max_job_secs),
        metadata,
        mode,
    };
    // The key is written before `new-session`, so no job ever runs without
    // the key that names it: a lookup that finds no key has proved no job
    // holds it.
    let (claim, write_key) = match key {
        Some(key) => (
            format!(
                "{} if [ -n \"$hit\" ]; then exit 0; fi; ",
                key_lookup_script(key)
            ),
            format!(
                "mkdir -p {dir} && printf '%s\\n' {key} > {dir}/key && ",
                dir = crate::wrapper::state_dir(&job.id)
            ),
        ),
        None => (String::new(), String::new()),
    };
    let script = format!(
        "{}; {claim}{write_key}{} && {{ tmux -L {} list-sessions -F '#{{session_name}}' 2>/dev/null | grep -c '^mule-' || true; }}",
        crate::jobs::prune(host),
        dispatch_script(host, &job),
        host.tmux_socket
    );

    // The retrospective count shares the measured 0s dispatch round trip. A
    // pre-flight warning would double both ssh round trips and lock cycles for
    // advisory backpressure.
    let output = transport.run(host, &script)?;
    if output.code != 0 {
        bail!(
            "dispatch failed for {}: {}",
            host.name,
            output.stderr.trim()
        );
    }
    let text = output.text();
    if let Some(key) = key
        && let Some(existing) = parse_hits(key, &text)?
    {
        return Ok(Dispatched::Existing(existing));
    }
    let running: u32 = text
        .trim()
        .parse()
        .context("invalid running-session count in dispatch reply")?;
    if running > host.max_running {
        writeln!(
            warnings,
            "mule: dispatched {}; {running} now running on {}, cap {}",
            job.id, host.name, host.max_running
        )?;
    }

    Ok(Dispatched::New(job.id))
}

/// The job holding `key` on `host`, if any: one locked round trip.
///
/// For `run --lease --key`, which must write its job file before the
/// dispatch and so must know first whether the key already names a job.
pub fn lookup(transport: &dyn Transport, host: &Host, key: &JobKey) -> Result<Option<JobId>> {
    crate::errors::require_master(transport, host)?;
    let output = transport.run(host, &key_lookup_script(key))?;
    if output.code != 0 {
        bail!(
            "looking up key {key} on {} failed: {}",
            host.name,
            output.stderr.trim()
        );
    }
    parse_hits(key, &output.text())
}

/// Sets `$hit` to one `existing=<id>` line per job holding `key`, and
/// prints them.
///
/// One `find` and one `awk` over every job's `key` file, however many jobs
/// there are: a per-job loop forking `cat` is the shape that took `ls` to
/// 14.1s at 300 jobs (SPEC § Listing). awk compares each file's first line
/// with the key exactly; the key's alphabet ([`JobKey`]) leaves nothing to
/// quote or escape.
///
/// No glob: the remote shell is the user's login shell, and zsh aborts a
/// command whose glob matches nothing (`no matches found`), which is every
/// first dispatch -- measured over the loopback sshd, whose user's shell is
/// zsh. `find` has no such case.
///
/// A key that cannot be checked refuses: a `key` file awk cannot open, or a
/// `find` that failed, exits 2 and fails the script, because "no hit" there
/// would start a second job for an effect that may already exist. A jobs
/// root that does not exist yet is simply no hit.
fn key_lookup_script(key: &JobKey) -> String {
    format!(
        "hit=; root={JOBS_ROOT}; \
         if [ -d \"$root\" ]; then \
           hit=$( {{ find \"$root\" -mindepth 2 -maxdepth 2 -type f -name key 2>/dev/null \
                     || echo //find-failed; }} \
                 | awk -v k={key} '\
                     $0 == \"//find-failed\" {{ bad = 1; next }} \
                     {{ f = $0; l = \"\"; if ((getline l < f) < 0) {{ bad = 1; next }} close(f); \
                       if (l == k) {{ id = f; sub(/\\/key$/, \"\", id); sub(/.*\\//, \"\", id); \
                                      print \"existing=\" id }} }} \
                     END {{ exit bad ? 2 : 0 }}'); \
           if [ $? -ne 0 ]; then echo \"cannot read the job keys under $root\" >&2; exit 1; fi; \
           if [ -n \"$hit\" ]; then printf '%s\\n' \"$hit\"; fi; \
         fi;"
    )
}

/// The one job a lookup found, or none. Two jobs under one key mean two
/// clients raced past each other's ticket locks; picking one would hide that,
/// so it is an error naming both.
fn parse_hits(key: &JobKey, reply: &str) -> Result<Option<JobId>> {
    let hits = reply
        .lines()
        .filter_map(|line| line.strip_prefix("existing="))
        .map(|id| {
            id.trim()
                .parse::<JobId>()
                .with_context(|| format!("invalid job id in key lookup reply: {id:?}"))
        })
        .collect::<Result<Vec<_>>>()?;
    match hits.as_slice() {
        [] => Ok(None),
        [one] => Ok(Some(one.clone())),
        many => bail!(
            "key {key} names {} jobs ({}); refusing to pick one",
            many.len(),
            many.iter()
                .map(JobId::as_str)
                .collect::<Vec<_>>()
                .join(", ")
        ),
    }
}
