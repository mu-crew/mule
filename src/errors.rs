use anyhow::Error as AnyhowError;
use anyhow::Result;
use thiserror::Error;

pub const EXIT_NO_MASTER: i32 = 3;
pub const EXIT_TIMEOUT: i32 = 4;
pub const EXIT_ORPHAN: i32 = 5;
pub const EXIT_DROPPED: i32 = 6;
/// `mule exec` only: the output is incomplete -- truncated at `max_log_bytes`,
/// or a process still holds the job's stdout -- so what was printed is partial.
/// Reported instead of the job's own code so a caller that
/// parses stdout can never mistake a prefix for the whole answer.
pub const EXIT_TRUNCATED: i32 = 7;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum MuleError {
    #[error(
        "no control master for {host}\n  \
         run: ssh -MNf -S {socket} -o ControlPersist=8h -o StreamLocalBindUnlink=yes {target}\n  \
         a human may need to tap a hardware key; ask rather than retrying"
    )]
    NoMaster {
        host: String,
        socket: String,
        target: String,
    },
    #[error(
        "the master is down or its one slot is held\n  run ssh -O check with mule's configured ControlPath to tell which"
    )]
    SessionChannelBusy,
    /// The master refused to open a session, proven from ssh's OWN log (`-E`),
    /// with nothing on stdout: the remote script never started, so running it
    /// again cannot run it twice. Internal: [`crate::transport::Transport::run`]
    /// re-queues on it and reports [`MuleError::SessionChannelBusy`] once its
    /// recovery budget is spent, so a caller never sees this variant.
    #[error("the master refused a session before the script started")]
    MuxRefusedPreExec,
    #[error(
        "the local ssh agent is unreachable; ssh-add -l cannot use SSH_AUTH_SOCK\n  re-establish or re-attach the agent; an authentication prompt may be waiting where you cannot see it"
    )]
    SshAgentUnreachable,
    #[error(
        "the local ssh agent has no keys loaded\n  add the required key with ssh-add; an authentication prompt may be waiting where you cannot see it"
    )]
    SshAgentHasNoKeys,
    #[error(
        "keyboard-interactive authentication failed; this is either a local ssh-agent problem or a busy control-master channel\n  run ssh-add -l, then ssh -O check with mule's configured ControlPath"
    )]
    KeyboardInteractiveAmbiguous,
    /// A `-O forward` exited 0 and left nothing listening: the master's forward
    /// table disagrees with reality, and only a master restart recovers (SPEC §
    /// Leases). Exit 3 because it needs the same human act as a missing master,
    /// and a caller must treat it the same way: stop and ask.
    #[error(
        "forward table for {host} is wedged ({local} is not listening after a forward); \
         a human must restart the master\n  \
         run: {exit}\n  \
         then: {open}\n  \
         a human may need to tap a hardware key; ask rather than retrying"
    )]
    ForwardWedged {
        host: String,
        local: String,
        exit: String,
        open: String,
    },
    #[error("timed out waiting for job {id}; it is still running")]
    Timeout { id: String },
    #[error("job {id} is orphaned; no rc will ever arrive")]
    Orphan { id: String },
    #[error("job {id} not found")]
    MissingJob { id: String },
    #[error("lost contact while waiting; the job continues\n  resume: mule tail {id}")]
    Dropped { id: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentState {
    Keys,
    NoKeys,
    Unreachable,
    Unknown,
}

/// Does this text carry the mux client's refusal to open a session?
///
/// What ssh logs (measured, OpenSSH 10): `mux_client_request_session: session
/// request failed: Session open refused by peer`.
fn mux_refused(text: &str) -> bool {
    let text = text.to_ascii_lowercase();
    ["session request failed", "session open refused"]
        .iter()
        .any(|pattern| text.contains(pattern))
}

pub fn classify(stderr: &str, agent_state: impl FnOnce() -> AgentState) -> Option<MuleError> {
    if mux_refused(stderr) {
        return Some(MuleError::SessionChannelBusy);
    }
    let stderr = stderr.to_ascii_lowercase();
    if !stderr.contains("permission denied (keyboard-interactive)") {
        return None;
    }
    Some(match agent_state() {
        AgentState::Keys => MuleError::SessionChannelBusy,
        AgentState::NoKeys => MuleError::SshAgentHasNoKeys,
        AgentState::Unreachable => MuleError::SshAgentUnreachable,
        AgentState::Unknown => MuleError::KeyboardInteractiveAmbiguous,
    })
}

/// Classify one ssh call from ssh's own log and the remote's stdout.
///
/// `ssh_log` must be what `ssh -E` wrote, never the remote stderr. Remote
/// stderr has no provenance: a script that ran can print the refusal text and
/// exit 255, and treating that as a refusal re-ran it -- measured, two calls,
/// two side effects. `-E` sends ssh's diagnostics to a file and leaves the
/// remote's stderr on the pipe, so the spoof stays on stderr and the log stays
/// empty (measured).
///
/// Retryable ([`MuleError::MuxRefusedPreExec`]) only for a mux refusal with
/// empty stdout. Refusal with output means something ran, so it is reported
/// busy and never retried; everything else maps as [`classify`] does.
pub fn classify_attempt(
    ssh_log: &str,
    stdout: &[u8],
    agent_state: impl FnOnce() -> AgentState,
) -> Option<MuleError> {
    if mux_refused(ssh_log) && stdout.is_empty() {
        return Some(MuleError::MuxRefusedPreExec);
    }
    classify(ssh_log, agent_state)
}

pub fn exit_code(error: &AnyhowError) -> i32 {
    match error.downcast_ref::<MuleError>() {
        Some(MuleError::NoMaster { .. } | MuleError::ForwardWedged { .. }) => EXIT_NO_MASTER,
        Some(MuleError::Timeout { .. }) => EXIT_TIMEOUT,
        Some(MuleError::Orphan { .. }) => EXIT_ORPHAN,
        Some(MuleError::Dropped { .. }) => EXIT_DROPPED,
        Some(
            MuleError::MissingJob { .. }
            | MuleError::SessionChannelBusy
            | MuleError::MuxRefusedPreExec
            | MuleError::SshAgentUnreachable
            | MuleError::SshAgentHasNoKeys
            | MuleError::KeyboardInteractiveAmbiguous,
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
/// exception is `mule host list`, whose whole job is to *report* which hosts
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
/// mule defaults `socket` to `~/.ssh/mule/<host>.sock` and never created that
/// directory, the command mule printed could not work -- and it failed *after*
/// the 2FA prompt, so the user paid a hardware-token tap to find out, and the
/// error read as a broken ssh config rather than a missing `mkdir`.
///
/// Done here rather than at config load so it is a consequence of asking for a
/// master, not a side effect of `mule --help`.
pub fn no_master(host: &crate::config::Host) -> AnyhowError {
    let mut hint = None;
    if let Some(parent) = host.socket.parent() {
        // 0700, because ssh refuses a control socket in a directory others can
        // write. A 0755 mkdir would trade this error for a subtler one.
        if let Err(e) = create_private_dir(parent) {
            hint = Some(format!("{}: {e}", parent.display()));
        }
    }
    let error: AnyhowError = MuleError::NoMaster {
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
///
/// `StreamLocalBindUnlink=yes` is set on the master because the master's own
/// setting governs forwards, and the client-side option is ignored (measured):
/// over a stale socket file a master without it fails `-O forward` with 255,
/// and one with it rebinds.
pub fn master_command(host: &crate::config::Host) -> String {
    if let Some(parent) = host.socket.parent() {
        let _ = create_private_dir(parent);
    }
    format!(
        "run: ssh -MNf -S {} -o ControlPersist=8h -o StreamLocalBindUnlink=yes {}",
        host.socket.display(),
        host.target
    )
}

/// The `ssh -O exit` line that stops this host's master, for the wedge
/// hand-back: a wedged forward table survives everything but a restart.
pub fn master_exit_command(host: &crate::config::Host) -> String {
    format!("ssh -S {} -O exit {}", host.socket.display(), host.target)
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
