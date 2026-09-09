//! The seam between coop's logic and the ssh channel.
//!
//! Every invariant that matters was measured against a real capped host and is
//! unreachable from a plain unit test. This trait is what makes test layer 1
//! possible at all: `Fake` records the exact script coop would have run, so
//! wrapper construction and state mapping are tested as pure functions.
//!
//! One rule shapes the whole file: **`run` takes coop's ticket lock, and
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
    /// are coop's own output, not the user's.
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
    /// Run a shell script on the host. Sub-second by construction: a detached
    /// dispatch or an artifact read, never the job itself.
    fn run(&self, host: &Host, script: &str) -> Result<Output>;

    /// Is there a usable multiplexing socket? `ssh -O check` only — it takes no
    /// session channel, so this probe never competes with anything.
    fn master_alive(&self, host: &Host) -> bool;
}

/// The real thing.
#[derive(Debug, Default, Clone, Copy)]
pub struct Ssh;

impl Transport for Ssh {
    fn run(&self, host: &Host, script: &str) -> Result<Output> {
        // `BatchMode=yes` so a missing master fails fast instead of prompting
        // into a background call that has no TTY to prompt on. coop checks
        // `master_alive` first and reports the `ssh -MNf` command; this is the
        // belt-and-braces half.
        let out = Command::new("ssh")
            .arg("-S")
            .arg(&host.socket)
            .args(["-o", "BatchMode=yes"])
            .arg(&host.target)
            .arg(script)
            .output()
            .with_context(|| format!("spawning ssh for host {}", host.name))?;
        let stderr = String::from_utf8_lossy(&out.stderr).into_owned();
        if let Some(error) = crate::errors::classify(&stderr) {
            return Err(error.into());
        }
        Ok(Output {
            stdout: out.stdout,
            stderr,
            code: out.status.code().unwrap_or(-1),
        })
    }

    fn master_alive(&self, host: &Host) -> bool {
        Command::new("ssh")
            .arg("-S")
            .arg(&host.socket)
            .args(["-O", "check"])
            .arg(&host.target)
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false)
    }
}

/// A recording transport for test layer 1.
///
/// Deliberately **not** `#[cfg(test)]`: integration tests live in their own
/// crate and could not see it otherwise, and layer 1 is where most of coop's
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
    fn run(&self, _host: &Host, script: &str) -> Result<Output> {
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
