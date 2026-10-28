use std::io::Write;

use anyhow::{Context, Result, bail};

use crate::config::Host;
use crate::transport::Transport;
use crate::wrapper::{Job, JobId, JobMetadata, JobMode, dispatch_script, new_id};

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
    let script = format!(
        "{}; {} && {{ tmux -L {} list-sessions -F '#{{session_name}}' 2>/dev/null | grep -c '^mule-' || true; }}",
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
    let running: u32 = output
        .text()
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

    Ok(job.id)
}
