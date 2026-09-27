//! The seam between mule's logic and the ssh channel.
//!
//! Every invariant that matters was measured against a real capped host and is
//! unreachable from a plain unit test. This trait is what makes test layer 1
//! possible at all: `Fake` records the exact script mule would have run, so
//! wrapper construction and state mapping are tested as pure functions.
//!
//! One rule shapes the whole file: **`run` takes mule's ticket lock, and
//! `master_alive` does not.** `ssh -O check` talks only to the mux socket, opens
//! no session channel, and measured at 0s — so exempting it is safe, and having
//! it be a *different method* makes the exemption structural instead of a rule
//! someone has to remember.

use std::process::Command;

use anyhow::{Context, Result};

use crate::config::Host;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Output {
    /// Raw bytes, never a lossy `String`.
    ///
    /// A job may emit a tarball, or simply invalid UTF-8, and the probe reply
    /// carries log bytes inside this field. `String::from_utf8_lossy` would
    /// silently substitute replacement characters — corrupting the one artifact
    /// the entire design treats as the source of truth. Text-only would have
    /// been a defensible scope cut; silent corruption is not.
    pub stdout: Vec<u8>,
    /// Text, because this is ssh's own diagnostics and `errors` pattern-matches
    /// them. A non-UTF-8 ssh error message is not a case worth carrying bytes
    /// for.
    pub stderr: String,
    pub code: i32,
}

impl Output {
    pub fn ok(stdout: impl Into<Vec<u8>>) -> Self {
        Self {
            stdout: stdout.into(),
            stderr: String::new(),
            code: 0,
        }
    }

    /// The stdout as text, for the many call sites parsing a known-ASCII reply
    /// (`rc=0`, a session count). Lossy on purpose and only here: these fields
    /// are mule's own output, not the user's.
    pub fn text(&self) -> std::borrow::Cow<'_, str> {
        String::from_utf8_lossy(&self.stdout)
    }

    pub fn fail(code: i32, stderr: impl Into<String>) -> Self {
        Self {
            stdout: Vec::new(),
            stderr: stderr.into(),
            code,
        }
    }
}

pub trait Transport {
    /// Run a shell script on the host, WITHOUT taking the ticket lock.
    ///
    /// Callers outside this module want [`Transport::run`], which is the same
    /// thing with the lock held. This is the unlocked primitive an
    /// implementation provides; calling it directly opens a session channel
    /// that mule's own fairness gate cannot see, which on a `MaxSessions 1`
    /// host recreates the contention the whole tool exists to remove.
    fn run_unlocked(&self, host: &Host, script: &str) -> Result<Output>;

    /// Is there a usable multiplexing socket? `ssh -O check` only — it takes no
    /// session channel, so this probe never competes with anything, and it is
    /// the one call deliberately exempt from the lock.
    fn master_alive(&self, host: &Host) -> bool;

    /// Run a shell script on the host, holding the host's ticket lock.
    ///
    /// **This is the only way callers should reach a host.** The lock covers
    /// every ssh mule issues except `master_alive`, because two concurrent
    /// reads hit exactly the cap that motivated the tool -- but until now that
    /// was prose, enforced by six call sites each remembering to wrap
    /// `run_unlocked` in `with_lock`. A seventh that forgot would compile,
    /// pass every test, and quietly reintroduce the contention.
    ///
    /// Provided rather than required, so no implementation can weaken it: the
    /// lock is applied here, once, and an implementor supplies only the
    /// unlocked primitive.
    fn run(&self, host: &Host, script: &str) -> Result<Output> {
        crate::lock::with_lock(&host.name, || self.run_unlocked(host, script))?
    }
}

/// Every ssh mule runs, configured so it can never prompt.
///
/// `BatchMode=yes` alone is not enough. It gags *ssh's* own prompts, but a
/// `ProxyCommand` is a separate program with its own terminal: a site wrapper
/// doing 2FA (`ProxyCommand x2ssh ...`) prompts regardless, so every mule call
/// on a host with a dead master spawned a Duo passcode prompt into the user's
/// terminal -- repeatedly, since mule is expected to be called often, and with
/// no indication of which invocation was asking.
///
/// Three settings close it:
///   BatchMode=yes           - ssh itself never asks
///   ControlMaster=no        - never create a master as a side effect; mule
///                             requires one to exist and refuses otherwise
///   ProxyCommand=none       - do not run a site wrapper that can prompt
///
/// `ProxyCommand=none` is safe precisely because mule only ever multiplexes
/// over an EXISTING master: the socket is already connected, so no proxy is
/// needed to reach the host. The master the user opens by hand keeps its own
/// ProxyCommand, which is where 2FA belongs -- once per ControlPersist window,
/// deliberately, with the user watching.
fn base_args(host: &Host) -> Vec<String> {
    vec![
        "-S".into(),
        host.socket.display().to_string(),
        "-o".into(),
        "BatchMode=yes".into(),
        "-o".into(),
        "ControlMaster=no".into(),
        "-o".into(),
        "ProxyCommand=none".into(),
        host.target.clone(),
    ]
}

/// Arguments for the lock-exempt master probe. Exposed for tests, which assert
/// that no mule invocation can prompt.
pub fn probe_args(host: &Host) -> Vec<String> {
    let mut args = base_args(host);
    args.extend(["-O".into(), "check".into()]);
    args
}

/// Arguments for running a script over the master.
pub fn run_args(host: &Host, script: &str) -> Vec<String> {
    let mut args = base_args(host);
    args.push(script.to_string());
    args
}

fn base_command(host: &Host) -> Command {
    let mut cmd = Command::new("ssh");
    cmd.args(base_args(host));
    cmd
}

fn ssh_agent_state() -> crate::errors::AgentState {
    match Command::new("ssh-add").arg("-l").output() {
        Ok(output) if output.status.success() => crate::errors::AgentState::Keys,
        Ok(output) if output.status.code() == Some(1) => crate::errors::AgentState::NoKeys,
        Ok(output) if output.status.code() == Some(2) => crate::errors::AgentState::Unreachable,
        Ok(_) | Err(_) => crate::errors::AgentState::Unknown,
    }
}

/// The real thing.
#[derive(Debug, Default, Clone, Copy)]
pub struct Ssh;

impl Transport for Ssh {
    fn run_unlocked(&self, host: &Host, script: &str) -> Result<Output> {
        let out = base_command(host)
            .arg(script)
            .output()
            .with_context(|| format!("spawning ssh for host {}", host.name))?;
        let stderr = String::from_utf8_lossy(&out.stderr).into_owned();
        if let Some(error) = crate::errors::classify(&stderr, ssh_agent_state) {
            return Err(error.into());
        }
        Ok(Output {
            stdout: out.stdout,
            stderr,
            code: out.status.code().unwrap_or(-1),
        })
    }

    fn master_alive(&self, host: &Host) -> bool {
        base_command(host)
            .args(["-O", "check"])
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false)
    }
}

/// A recording transport for test layer 1.
///
/// Deliberately **not** `#[cfg(test)]`: integration tests live in their own
/// crate and could not see it otherwise, and layer 1 is where most of mule's
/// logic is actually verified.
#[derive(Debug, Default)]
pub struct Fake {
    scripts: std::sync::Mutex<Vec<String>>,
    outputs: std::sync::Mutex<std::collections::VecDeque<Output>>,
    master: bool,
}

impl Fake {
    /// A fake with a live master and no queued output (every `run` yields an
    /// empty success).
    pub fn new() -> Self {
        Self {
            master: true,
            ..Default::default()
        }
    }

    /// A fake whose master is down, for the exit-3 path.
    pub fn no_master() -> Self {
        Self::default()
    }

    /// Queue one reply. Replies are consumed in order.
    pub fn push(&self, out: Output) -> &Self {
        self.outputs.lock().unwrap().push_back(out);
        self
    }

    /// Every script handed to `run`, in order. This is the assertion surface
    /// for wrapper construction.
    pub fn scripts(&self) -> Vec<String> {
        self.scripts.lock().unwrap().clone()
    }
}

impl Transport for Fake {
    fn run_unlocked(&self, _host: &Host, script: &str) -> Result<Output> {
        self.scripts.lock().unwrap().push(script.to_string());
        Ok(self
            .outputs
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or_else(|| Output::ok("")))
    }

    fn master_alive(&self, _host: &Host) -> bool {
        self.master
    }
}
