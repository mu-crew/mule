//! The seam between mule's logic and the ssh channel.
//!
//! Every invariant that matters was measured against a real capped host and is
//! unreachable from a plain unit test. This trait is what makes test layer 1
//! possible at all: `Fake` records the exact script mule would have run, so
//! wrapper construction and state mapping are tested as pure functions.
//!
//! One rule shapes the whole file: **`run` takes mule's ticket lock, and
//! `mux_op` does not.** `ssh -O check`, and `-O forward`/`-O cancel` of a local
//! unix forward, talk only to the mux socket and are handled in the master's
//! own loop. None opens a session channel -- measured during a held session,
//! and `check` at 0s -- so exempting them is safe, and having them be a
//! *different method* makes the exemption structural instead of a rule someone
//! has to remember.
//!
//! The exemption is exactly as wide as [`MuxOp`], a closed enum. It has no
//! free-form arguments and no remote (`-R`) forward, because a remote forward
//! needs a server round trip and so belongs inside the lock. Widening the
//! exemption means editing the enum, in review, not passing a different string.

use std::path::PathBuf;
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

/// The mux operations that bypass the ticket lock.
///
/// Closed on purpose: see the module header. Each is answered by the master
/// itself without opening a session channel, so it cannot contend with a job
/// verb for the `MaxSessions` slot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MuxOp {
    /// `-O check`: is there a master, and which pid owns it.
    Check,
    /// `-O forward -L LOCAL:REMOTE` for unix sockets. LOCAL is bound by the
    /// master, so it must be absolute: a relative path would resolve against
    /// the master's working directory, not the caller's.
    ForwardUnix(PathBuf, String),
    /// `-O cancel -L LOCAL:REMOTE`.
    CancelUnix(PathBuf, String),
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

    /// Run one mux operation over the master, WITHOUT the ticket lock.
    ///
    /// This is the only lock-free path to the host, and [`MuxOp`] is what keeps
    /// it honest: every variant is handled in the master's mux loop and opens
    /// no session channel (measured during a held session). The exit code of a
    /// forward or cancel proves nothing -- callers verify by effect.
    fn mux_op(&self, host: &Host, op: MuxOp) -> Result<Output>;

    /// Is there a usable multiplexing socket? `-O check` only, so this probe
    /// never competes with anything.
    fn master_alive(&self, host: &Host) -> bool {
        self.mux_op(host, MuxOp::Check)
            .is_ok_and(|output| output.code == 0)
    }

    /// Run a shell script on the host, holding the host's ticket lock.
    ///
    /// **This is the only way callers should reach a host.** The lock covers
    /// every ssh mule issues except the [`MuxOp`]s, because two concurrent
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

/// Arguments for a lock-exempt mux op. Exposed for tests, which assert that no
/// mule invocation can prompt and that paths reach ssh as argv.
///
/// Paths are single argv elements: `-O` runs no remote shell, so nothing here
/// is ever re-parsed. (`:` in a path is rejected earlier, where the spec is
/// parsed, because ssh itself splits `-L` on it.)
pub fn mux_args(host: &Host, op: &MuxOp) -> Vec<String> {
    let mut args = base_args(host);
    match op {
        MuxOp::Check => args.extend(["-O".into(), "check".into()]),
        MuxOp::ForwardUnix(local, remote) => args.extend([
            "-O".into(),
            "forward".into(),
            "-L".into(),
            format!("{}:{remote}", local.display()),
        ]),
        MuxOp::CancelUnix(local, remote) => args.extend([
            "-O".into(),
            "cancel".into(),
            "-L".into(),
            format!("{}:{remote}", local.display()),
        ]),
    }
    args
}

/// Arguments for the lock-exempt master probe.
pub fn probe_args(host: &Host) -> Vec<String> {
    mux_args(host, &MuxOp::Check)
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

    fn mux_op(&self, host: &Host, op: MuxOp) -> Result<Output> {
        let mut cmd = Command::new("ssh");
        cmd.args(mux_args(host, &op));
        let out = cmd
            .output()
            .with_context(|| format!("spawning ssh for host {}", host.name))?;
        Ok(Output {
            stdout: out.stdout,
            // Not classified: a mux op opens no session, so the refused-channel
            // messages `classify` hunts for cannot come from it. ssh prints
            // `-O check`'s `Master running (pid=N)` here, and callers read it.
            stderr: String::from_utf8_lossy(&out.stderr).into_owned(),
            code: out.status.code().unwrap_or(-1),
        })
    }
}

/// A recording transport for test layer 1.
///
/// Deliberately **not** `#[cfg(test)]`: integration tests live in their own
/// crate and could not see it otherwise, and layer 1 is where most of mule's
/// logic is actually verified.
pub struct Fake {
    scripts: std::sync::Mutex<Vec<String>>,
    outputs: std::sync::Mutex<std::collections::VecDeque<Output>>,
    mux_ops: std::sync::Mutex<Vec<MuxOp>>,
    mux_outputs: std::sync::Mutex<std::collections::VecDeque<Output>>,
    mux_hook: std::sync::Mutex<Option<MuxHook>>,
    // Atomics so a test can take the master away and bring it back under a new
    // pid mid-run, as `ssh -O exit` and a human's `ssh -MNf` would.
    master: std::sync::atomic::AtomicBool,
    master_pid: std::sync::atomic::AtomicU32,
}

/// Runs on every `mux_op`, so a test can play the master: bind the LOCAL a
/// forward names, or refuse to.
type MuxHook = Box<dyn Fn(&MuxOp) + Send + Sync>;

impl std::fmt::Debug for Fake {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Fake")
            .field("scripts", &self.scripts)
            .field("mux_ops", &self.mux_ops)
            .field("master", &self.master)
            .finish_non_exhaustive()
    }
}

impl Default for Fake {
    fn default() -> Self {
        Self {
            scripts: Default::default(),
            outputs: Default::default(),
            mux_ops: Default::default(),
            mux_outputs: Default::default(),
            mux_hook: Default::default(),
            master: false.into(),
            master_pid: 4242.into(),
        }
    }
}

impl Fake {
    /// A fake with a live master and no queued output (every `run` yields an
    /// empty success).
    pub fn new() -> Self {
        Self {
            master: true.into(),
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

    /// Queue one reply for a non-`Check` mux op. Consumed in order; once the
    /// queue is empty every op succeeds.
    pub fn push_mux(&self, out: Output) -> &Self {
        self.mux_outputs.lock().unwrap().push_back(out);
        self
    }

    /// Install the master's side effects for forward/cancel.
    pub fn on_mux(&self, hook: impl Fn(&MuxOp) + Send + Sync + 'static) -> &Self {
        *self.mux_hook.lock().unwrap() = Some(Box::new(hook));
        self
    }

    /// The pid `-O check` reports.
    pub fn with_master_pid(self, pid: u32) -> Self {
        self.set_master(Some(pid));
        self
    }

    /// Take the master away (`None`) or bring one up with `pid`, mid-test.
    pub fn set_master(&self, pid: Option<u32>) {
        use std::sync::atomic::Ordering::SeqCst;
        if let Some(pid) = pid {
            self.master_pid.store(pid, SeqCst);
        }
        self.master.store(pid.is_some(), SeqCst);
    }

    /// Every script handed to `run`, in order. This is the assertion surface
    /// for wrapper construction.
    pub fn scripts(&self) -> Vec<String> {
        self.scripts.lock().unwrap().clone()
    }

    /// Every mux op, in order, `Check` included. The assertion surface for
    /// forward sequencing.
    pub fn mux_ops(&self) -> Vec<MuxOp> {
        self.mux_ops.lock().unwrap().clone()
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

    fn mux_op(&self, _host: &Host, op: MuxOp) -> Result<Output> {
        self.mux_ops.lock().unwrap().push(op.clone());
        if op == MuxOp::Check {
            use std::sync::atomic::Ordering::SeqCst;
            return Ok(if self.master.load(SeqCst) {
                Output {
                    stdout: Vec::new(),
                    stderr: format!("Master running (pid={})\r\n", self.master_pid.load(SeqCst)),
                    code: 0,
                }
            } else {
                Output::fail(255, "Control socket connect: No such file or directory")
            });
        }
        if let Some(hook) = self.mux_hook.lock().unwrap().as_ref() {
            hook(&op);
        }
        Ok(self
            .mux_outputs
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or_else(|| Output::ok("")))
    }
}
