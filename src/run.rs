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
            // A hit on this job's own id is its claim (`claim`, from a keyed
            // lease): start it. Any other hit is the job holding the key.
            format!(
                "{} if [ -n \"$hit\" ] && [ \"$hit\" != \"existing={}\" ]; then exit 0; fi; ",
                key_lookup_script(key),
                job.id
            ),
            format!(
                "mkdir -p {dir} && printf '%s\\n' {key} > {dir}/key && ",
                dir = crate::wrapper::state_dir(&job.id)
            ),
        ),
        None => (String::new(), String::new()),
    };
    // The count is advisory (a warning, never a state answer), so a count
    // that cannot be taken is `?` and no warning, not a failed dispatch: the
    // job has started by then.
    let script = format!(
        "{}; {claim}{write_key}{} && {{ n=$(tmux -L {} list-sessions -F '#{{session_name}}' 2>/dev/null) \
           && printf '%s\\n' \"$n\" | awk '/^mule-/ {{ c++ }} END {{ print c + 0 }}' || echo '?'; }}",
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
        && existing != job.id
    {
        return Ok(Dispatched::Existing(existing));
    }
    let count = text
        .lines()
        .filter(|line| !line.starts_with("existing="))
        .collect::<String>();
    let running: Option<u32> = match count.trim() {
        "?" => None,
        other => Some(
            other
                .parse()
                .context("invalid running-session count in dispatch reply")?,
        ),
    };
    if let Some(running) = running
        && running > host.max_running
    {
        writeln!(
            warnings,
            "mule: dispatched {}; {running} now running on {}, cap {}",
            job.id, host.name, host.max_running
        )?;
    }

    Ok(Dispatched::New(job.id))
}

/// What a keyed lease's claim found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Claim {
    /// The key was free; it is now `id`'s: `$d/key` exists, nothing started.
    Claimed,
    /// Another job holds the key.
    Held(JobId),
}

/// Claim `key` for `id` before anything names `id`: one locked round trip
/// that looks the key up and, only if it is free, makes `id`'s state dir and
/// writes its `key`. Nothing is started.
///
/// For `run --lease --key`, whose job file must never name a job that does
/// not hold its key. Two keyed leases that both looked first and wrote their
/// job files after (review of P7a) both found the key free; the second's
/// dispatch then found the first's job, and its job file already named an id
/// that would never run. Claimed first, under the ticket lock, the loser
/// sees the winner here and names only it. A claim whose dispatch never
/// comes (the lease died in between) is a state dir with a key and no
/// `cmd`: findable by the key, and `unknown`, never `missing`, to `poll`.
pub fn claim(transport: &dyn Transport, host: &Host, id: &JobId, key: &JobKey) -> Result<Claim> {
    crate::errors::require_master(transport, host)?;
    let dir = crate::wrapper::state_dir(id);
    let script = format!(
        "{} if [ -n \"$hit\" ]; then exit 0; fi; \
         mkdir -p {dir} && printf '%s\\n' {key} > {dir}/key",
        key_lookup_script(key)
    );
    let output = transport.run(host, &script)?;
    if output.code != 0 {
        bail!(
            "claiming key {key} on {} failed: {}",
            host.name,
            output.stderr.trim()
        );
    }
    Ok(match parse_hits(key, &output.text())? {
        Some(holder) => Claim::Held(holder),
        None => Claim::Claimed,
    })
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
/// with the key exactly and as a string: `l == k` alone compares two
/// numeric-looking values as numbers, so `01` and `1` (or `1e0` and `1`)
/// were one key and the second caller's command never ran (review of P7a).
/// Concatenating `""` forces string comparison in every POSIX awk. The key's
/// alphabet ([`JobKey`]) leaves nothing to quote or escape.
///
/// No glob: the remote shell is the user's login shell, and zsh aborts a
/// command whose glob matches nothing (`no matches found`), which is every
/// first dispatch -- measured over the loopback sshd, whose user's shell is
/// zsh. `find` has no such case.
///
/// A key that cannot be checked refuses: a `key` file awk cannot open or
/// that reads empty, a `find` that failed, or a jobs root that cannot be
/// looked at fails the script, because "no hit" there would start a second
/// job for an effect that may already exist. Only a jobs root seen not to
/// exist (`No such file or directory`) is no hit.
fn key_lookup_script(key: &JobKey) -> String {
    format!(
        "{prelude}hit=; root={JOBS_ROOT}; \
         if mule_absent \"$root\" \"jobs root\"; then :; else \
           [ -d \"$root\" ] || mule_u \"jobs root $root is not a directory\"; \
           odd=$(find \"$root\" -mindepth 2 -maxdepth 2 -name key ! -type f) \
             || mule_u \"cannot search the job keys under $root\"; \
           [ -z \"$odd\" ] || mule_u \"a job key is not a regular file: $odd\"; \
           keys=$(find \"$root\" -mindepth 2 -maxdepth 2 -name key -type f) \
             || mule_u \"cannot search the job keys under $root\"; \
           if [ -n \"$keys\" ]; then \
             hit=$(printf '%s\\n' \"$keys\" | awk -v k={key} '\
                 $0 == \"\" {{ next }} \
                 {{ f = $0; l = \"\"; if ((getline l < f) <= 0 || l == \"\") {{ bad = 1; next }} close(f); \
                   if ((l \"\") == (k \"\")) {{ id = f; sub(/\\/key$/, \"\", id); sub(/.*\\//, \"\", id); \
                                  print \"existing=\" id }} }} \
                 END {{ exit bad ? 2 : 0 }}') \
               || mule_u \"cannot read the job keys under $root\"; \
           fi; \
           if [ -n \"$hit\" ]; then printf '%s\\n' \"$hit\"; fi; \
         fi;",
        prelude = crate::remote::PRELUDE,
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
