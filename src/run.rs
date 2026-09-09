use std::io::Write;

use anyhow::{Context, Result, bail};
use thiserror::Error;

use crate::config::Host;
use crate::lock::with_lock;
use crate::transport::Transport;
use crate::wrapper::{Job, dispatch_script, new_id};

#[derive(Debug, Error)]
#[error("no control master for {host}\n  run: ssh -MNf -S {socket} -o ControlPersist=8h {target}")]
pub struct NoMaster {
    host: String,
    socket: String,
    target: String,
}

pub fn dispatch(
    transport: &dyn Transport,
    host: &Host,
    cmd: &str,
    cwd: Option<&str>,
) -> Result<String> {
    dispatch_with_warnings(transport, host, cmd, cwd, &mut std::io::stderr())
}

#[doc(hidden)]
pub fn dispatch_with_warnings(
    transport: &dyn Transport,
    host: &Host,
    cmd: &str,
    cwd: Option<&str>,
    warnings: &mut dyn Write,
) -> Result<String> {
    // `ssh -O check` measured at 0s and opens no session channel, so it is the
    // one transport call deliberately outside the lock.
    if !transport.master_alive(host) {
        return Err(NoMaster {
            host: host.name.clone(),
            socket: host.socket.display().to_string(),
            target: host.target.clone(),
        }
        .into());
    }

    let job = Job {
        id: new_id(),
        cmd: cmd.to_owned(),
        cwd: cwd.map(str::to_owned),
    };
    let script = format!(
        "{} && {{ tmux -L {} list-sessions -F '#{{session_name}}' 2>/dev/null | grep -c '^coop-' || true; }}",
        dispatch_script(host, &job),
        host.tmux_socket
    );

    // The retrospective count shares the measured 0s dispatch round trip. A
    // pre-flight warning would double both ssh round trips and lock cycles for
    // advisory backpressure.
    let output = with_lock(&host.name, || transport.run(host, &script))??;
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
            "coop: dispatched {}; {running} now running on {}, cap {}",
            job.id, host.name, host.max_running
        )?;
    }

    Ok(job.id)
}
