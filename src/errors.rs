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
    #[error(
        "the local ssh agent is unreachable; ssh-add -l cannot use SSH_AUTH_SOCK\n  re-establish or re-attach the agent; an authentication prompt may be waiting where you cannot see it"
    )]
    SshAgentUnreachable,
    #[error(
        "the local ssh agent has no keys loaded\n  add the required key with ssh-add; an authentication prompt may be waiting where you cannot see it"
    )]
    SshAgentHasNoKeys,
    #[error(
        "keyboard-interactive authentication failed; this is either a local ssh-agent problem or a busy control-master channel\n  run ssh-add -l, then ssh -O check with coop's configured ControlPath"
    )]
    KeyboardInteractiveAmbiguous,
    #[error("timed out waiting for job {id}; it is still running")]
    Timeout { id: String },
    #[error("job {id} is orphaned; no rc will ever arrive")]
    Orphan { id: String },
    #[error("lost contact while waiting; the job continues\n  resume: coop tail {id}")]
    Dropped { id: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentState {
    Keys,
    NoKeys,
    Unreachable,
    Unknown,
}

pub fn classify(stderr: &str, agent_state: impl FnOnce() -> AgentState) -> Option<CoopError> {
    let stderr = stderr.to_ascii_lowercase();
    if ["session request failed", "session open refused"]
        .iter()
        .any(|pattern| stderr.contains(pattern))
    {
        return Some(CoopError::SessionChannelBusy);
    }
    if !stderr.contains("permission denied (keyboard-interactive)") {
        return None;
    }
    Some(match agent_state() {
        AgentState::Keys => CoopError::SessionChannelBusy,
        AgentState::NoKeys => CoopError::SshAgentHasNoKeys,
        AgentState::Unreachable => CoopError::SshAgentUnreachable,
        AgentState::Unknown => CoopError::KeyboardInteractiveAmbiguous,
    })
}

// CLI call sites still route wait failures through this adapter. Keep it as a
// pass-through so only typed errors from the wait loop can select stable codes.
pub fn waiting(error: AnyhowError, _id: &str) -> AnyhowError {
    error
}

pub fn exit_code(error: &AnyhowError) -> i32 {
    match error.downcast_ref::<CoopError>() {
        Some(CoopError::NoMaster { .. }) => EXIT_NO_MASTER,
        Some(CoopError::Timeout { .. }) => EXIT_TIMEOUT,
        Some(CoopError::Orphan { .. }) => EXIT_ORPHAN,
        Some(CoopError::Dropped { .. }) => EXIT_DROPPED,
        Some(
            CoopError::SessionChannelBusy
            | CoopError::SshAgentUnreachable
            | CoopError::SshAgentHasNoKeys
            | CoopError::KeyboardInteractiveAmbiguous,
        )
        | None => 1,
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
    Err(no_master(host))
}

/// The missing-master error, with the socket directory prepared first.
///
/// ssh will not create the directory holding a control socket: it binds a
/// temporary name inside it and fails with
/// `unix_listener: cannot bind to path ...: No such file or directory`. Since
/// coop defaults `socket` to `~/.ssh/coop/<host>.sock` and never created that
/// directory, the command coop printed could not work -- and it failed *after*
/// the 2FA prompt, so the user paid a hardware-token tap to find out, and the
/// error read as a broken ssh config rather than a missing `mkdir`.
///
/// Done here rather than at config load so it is a consequence of asking for a
/// master, not a side effect of `coop --help`.
pub fn no_master(host: &crate::config::Host) -> AnyhowError {
    let mut hint = None;
    if let Some(parent) = host.socket.parent() {
        // 0700, because ssh refuses a control socket in a directory others can
        // write. A 0755 mkdir would trade this error for a subtler one.
        if let Err(e) = create_private_dir(parent) {
            hint = Some(format!("{}: {e}", parent.display()));
        }
    }
    let error: AnyhowError = CoopError::NoMaster {
        host: host.name.clone(),
        socket: host.socket.display().to_string(),
        target: host.target.clone(),
    }
    .into();
    match hint {
        // Say so rather than printing a command that cannot work.
        Some(why) => error.context(format!("cannot prepare the socket directory {why}")),
        None => error,
    }
}

/// The `ssh -MNf` line that opens a master for this host.
///
/// One renderer, used by every verb and by `host list`, so the advice cannot
/// drift between them. Preparing the socket directory is part of producing the
/// command: printing one that cannot work is worse than printing nothing, and
/// the failure arrives only after a 2FA prompt.
pub fn master_command(host: &crate::config::Host) -> String {
    if let Some(parent) = host.socket.parent() {
        let _ = create_private_dir(parent);
    }
    format!(
        "run: ssh -MNf -S {} -o ControlPersist=8h {}",
        host.socket.display(),
        host.target
    )
}

fn create_private_dir(dir: &std::path::Path) -> std::io::Result<()> {
    if dir.is_dir() {
        return Ok(());
    }
    std::fs::create_dir_all(dir)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}
