//! Who claimed a key, and whether that claimer is provably gone.
//!
//! A keyed lease claims its key (`run::claim`) before it names or starts the
//! job. If it dies between the claim and the dispatch -- a signal before its
//! dispatch, a master that went away -- the claim stays: a state dir with a
//! `key` and no `cmd`. Every later `run --lease --key K` finds that claim,
//! and holding it as `unknown` forever is a liveness trap (review of P7a,
//! round 3): the job never starts.
//!
//! So the claim records its claimer -- this host's name, the pid and that
//! process's start time -- and a later lease may roll an unstarted claim
//! back and dispatch only on **proof** the claimer is gone: the same host,
//! and the pid absent or now a different process (another start time).
//! Anything else -- another host, a live claimer, a record that will not
//! read or parse, a `ps` that fails -- is not proof, and the claim stays
//! `unknown`, with `mule rm <id>` as the human path. This is mu's
//! creator-gone gate (mu plugins spec, D4) applied to mule's own claim.

use std::process::Command;

/// One claimer, as recorded in a claim's `claim` file: one line,
/// `pid=<pid> host=<host> start=<start>`, start last because it has spaces.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Claimer {
    pub pid: u32,
    pub host: String,
    /// `ps -o lstart=` of the pid, under `LC_ALL=C`.
    pub start: String,
}

impl Claimer {
    /// This process, as a claim records it. `None` if its own start time
    /// cannot be read: such a claim records nothing to prove against.
    pub fn current() -> Option<Self> {
        let pid = std::process::id();
        Some(Self {
            pid,
            host: local_host()?,
            start: start_time(pid)?,
        })
    }

    pub fn line(&self) -> String {
        format!("pid={} host={} start={}", self.pid, self.host, self.start)
    }

    pub fn parse(line: &str) -> Option<Self> {
        let rest = line.trim().strip_prefix("pid=")?;
        let (pid, rest) = rest.split_once(" host=")?;
        let (host, start) = rest.split_once(" start=")?;
        let pid = pid.parse().ok()?;
        (!host.is_empty() && !start.is_empty()).then(|| Self {
            pid,
            host: host.to_string(),
            start: start.to_string(),
        })
    }
}

/// Is `claimer` proven gone? Only for this host: a pid on another machine
/// says nothing here.
pub fn proven_gone(claimer: &Claimer) -> bool {
    if local_host().as_deref() != Some(claimer.host.as_str()) {
        return false;
    }
    // SAFETY: signal 0 only checks that the pid exists.
    let probe = unsafe { libc::kill(claimer.pid as libc::pid_t, 0) };
    if probe != 0 {
        // ESRCH is "no such process"; EPERM means one exists.
        return std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH);
    }
    // The pid exists: gone only if it is now another process. A `ps` that
    // fails proves nothing.
    start_time(claimer.pid).is_some_and(|start| start != claimer.start)
}

/// This host's name, as `gethostname` gives it.
pub fn local_host() -> Option<String> {
    let mut buf = [0u8; 256];
    // SAFETY: the buffer is valid for its length; the name is NUL-terminated
    // when it fits, and the last byte is never written.
    let rc = unsafe { libc::gethostname(buf.as_mut_ptr().cast(), buf.len() - 1) };
    if rc != 0 {
        return None;
    }
    let end = buf.iter().position(|&b| b == 0)?;
    let name = String::from_utf8(buf[..end].to_vec()).ok()?;
    (!name.is_empty() && !name.contains(char::is_whitespace)).then_some(name)
}

/// The start time of `pid`, from `ps -o lstart=`, or `None`.
pub fn start_time(pid: u32) -> Option<String> {
    let out = Command::new("ps")
        .args(["-o", "lstart=", "-p", &pid.to_string()])
        .env("LC_ALL", "C")
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let text = String::from_utf8(out.stdout).ok()?;
    let start = text.split_whitespace().collect::<Vec<_>>().join(" ");
    (!start.is_empty()).then_some(start)
}
