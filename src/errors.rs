use anyhow::Error as AnyhowError;
use anyhow::Result;
use thiserror::Error;

pub const EXIT_NO_MASTER: i32 = 3;
pub const EXIT_TIMEOUT: i32 = 4;
pub const EXIT_ORPHAN: i32 = 5;
pub const EXIT_DROPPED: i32 = 6;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum CoopError {
    #[error(
        "no control master for {host}\n  \
         run: ssh -MNf -S {socket} -o ControlPersist=8h {target}\n  \
         a human may need to tap a hardware key; ask rather than retrying"
    )]
    NoMaster {
        host: String,
        socket: String,
        target: String,
    },
    #[error(
        "the master is down or its one slot is held\n  run ssh -O check with coop's configured ControlPath to tell which"
    )]
    SessionChannelBusy,
    #[error("timed out waiting for job {id}; it is still running")]
    Timeout { id: String },
    #[error("job {id} is orphaned; no rc will ever arrive")]
    Orphan { id: String },
    #[error("lost contact while waiting; the job continues\n  resume: coop tail {id}")]
    Dropped { id: String },
}

pub fn classify(stderr: &str) -> Option<CoopError> {
    let stderr = stderr.to_ascii_lowercase();
    [
        "session request failed",
        "session open refused",
        "permission denied (keyboard-interactive)",
    ]
    .iter()
    .any(|pattern| stderr.contains(pattern))
    .then_some(CoopError::SessionChannelBusy)
}

pub fn waiting(error: AnyhowError, id: &str) -> AnyhowError {
    if error.downcast_ref::<CoopError>().is_some() {
        return error;
    }
    let message = format!("{error:#}");
    if message.contains("no rc will ever arrive") {
        CoopError::Orphan { id: id.to_owned() }.into()
    } else if message.contains("timed out waiting") {
        CoopError::Timeout { id: id.to_owned() }.into()
    } else if message.contains("lost contact while waiting") {
        CoopError::Dropped { id: id.to_owned() }.into()
    } else {
        error
    }
}

pub fn exit_code(error: &AnyhowError) -> i32 {
    match error.downcast_ref::<CoopError>() {
        Some(CoopError::NoMaster { .. }) => EXIT_NO_MASTER,
        Some(CoopError::Timeout { .. }) => EXIT_TIMEOUT,
        Some(CoopError::Orphan { .. }) => EXIT_ORPHAN,
        Some(CoopError::Dropped { .. }) => EXIT_DROPPED,
        Some(CoopError::SessionChannelBusy) | None => 1,
    }
}

/// Refuse to proceed without a control master, naming the command that opens
/// one.
///
/// Every job verb needs this, not just `run`: without it `poll`, `wait`,
/// `tail`, `kill` and `rm` fell through to ssh and reported a generic failure
/// with exit 1, instead of the documented exit 3 and the recovery command. The
/// exception is `coop host list`, whose whole job is to *report* which hosts
/// have a master.
pub fn require_master(
    t: &dyn crate::transport::Transport,
    host: &crate::config::Host,
) -> Result<()> {
    if t.master_alive(host) {
        return Ok(());
    }
    Err(CoopError::NoMaster {
        host: host.name.clone(),
        socket: host.socket.display().to_string(),
        target: host.target.clone(),
    }
    .into())
}
