use anyhow::Error as AnyhowError;
use thiserror::Error;

pub const EXIT_NO_MASTER: i32 = 3;
pub const EXIT_TIMEOUT: i32 = 4;
pub const EXIT_ORPHAN: i32 = 5;
pub const EXIT_DROPPED: i32 = 6;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum CoopError {
    #[error(
        "no control master for {host}\n  run: ssh -MNf -S {socket} -o ControlPersist=8h {target}"
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
