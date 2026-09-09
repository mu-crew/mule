use std::time::Duration;

use anyhow::{Context, Result, bail};

use crate::config::Host;
use crate::lock::with_lock;
use crate::transport::Transport;
use crate::wrapper::{JobId, state_dir};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum State {
    Running,
    Done(i32),
    Orphan,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Probe {
    pub state: State,
    pub log_size: u64,
    pub bytes: Vec<u8>,
}

/// Ask for state only, fetching no log bytes.
///
/// A plain `wait` wants `rc`, not output, and shipping the log to discard it
/// would hold the lock for the transfer. Expressed as its own type rather than
/// a sentinel offset: passing `u64::MAX` overflowed the `+1` that `tail -c +N`
/// needs and produced `tail: Invalid argument`, which surfaced as a spurious
/// "lost contact while waiting" on every `--wait --no-tail`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum From {
    Offset(u64),
    StateOnly,
}

impl From {
    /// The `tail -c +N` argument, or `None` when no bytes are wanted.
    fn tail_arg(self) -> Option<u64> {
        match self {
            // `tail -c +N` is 1-based, so byte offset 0 is `+1`. Saturating at
            // the top keeps a nonsensical offset from wrapping into a valid
            // one; it reads the last byte instead of the whole file.
            From::Offset(n) => Some(n.saturating_add(1)),
            From::StateOnly => None,
        }
    }
}

pub fn probe(t: &dyn Transport, host: &Host, id: &JobId, from: impl Into<From>) -> Result<Probe> {
    let dir = state_dir(id);
    let from = from.into();
    let script = format!(
        "d={dir}; printf 'rc=%s\\n' \"$(cat $d/rc 2>/dev/null)\"; \
         printf 'alive=%s\\n' \"$(tmux -L {} has-session -t coop-{id} 2>/dev/null && echo 1 || echo 0)\"; \
         printf 'size=%s\\n' \"$(wc -c < $d/log 2>/dev/null || echo 0)\"; \
         printf 'bytes:\\n'; {}",
        host.tmux_socket,
        match from.tail_arg() {
            Some(n) => format!("tail -c +{n} $d/log 2>/dev/null"),
            None => "true".to_string(),
        }
    );
    let mut output = with_lock(&host.name, || t.run(host, &script))??;
    if output.code != 0 {
        bail!("probe failed: {}", output.stderr.trim());
    }

    const MARKER: &[u8] = b"bytes:\n";
    let marker = output
        .stdout
        .windows(MARKER.len())
        .position(|window| window == MARKER)
        .context("invalid probe reply: missing bytes marker")?;
    let bytes = output.stdout[marker + MARKER.len()..].to_vec();
    output.stdout.truncate(marker);

    let mut rc = None;
    let mut alive = None;
    let mut log_size = None;
    for line in output.text().lines() {
        if let Some(value) = line.strip_prefix("rc=") {
            if !value.is_empty() {
                rc = Some(value.parse::<i32>().context("invalid rc in probe reply")?);
            }
        } else if let Some(value) = line.strip_prefix("alive=") {
            alive = Some(value == "1");
        } else if let Some(value) = line.strip_prefix("size=") {
            log_size = Some(
                value
                    .trim()
                    .parse()
                    .context("invalid size in probe reply")?,
            );
        }
    }

    let state = match rc {
        Some(code) => State::Done(code),
        None if alive.context("invalid probe reply: missing alive")? => State::Running,
        None => State::Orphan,
    };
    Ok(Probe {
        state,
        log_size: log_size.context("invalid probe reply: missing size")?,
        bytes,
    })
}

pub fn next_interval(current: Duration, new_bytes: bool) -> Duration {
    if new_bytes {
        Duration::from_secs(1)
    } else {
        current.saturating_mul(2).min(Duration::from_secs(5))
    }
}

impl core::convert::From<u64> for From {
    fn from(offset: u64) -> Self {
        From::Offset(offset)
    }
}
