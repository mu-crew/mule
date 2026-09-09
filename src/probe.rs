use std::time::Duration;

use anyhow::{Context, Result, bail};

use crate::config::Host;
use crate::lock::with_lock;
use crate::transport::Transport;
use crate::wrapper::state_dir;

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

pub fn probe(t: &dyn Transport, host: &Host, id: &str, offset: u64) -> Result<Probe> {
    let dir = state_dir(id);
    let script = format!(
        "d={dir}; printf 'rc=%s\\n' \"$(cat $d/rc 2>/dev/null)\"; \
         printf 'alive=%s\\n' \"$(tmux -L {} has-session -t coop-{id} 2>/dev/null && echo 1 || echo 0)\"; \
         printf 'size=%s\\n' \"$(wc -c < $d/log 2>/dev/null || echo 0)\"; \
         printf 'bytes:\\n'; tail -c +{} $d/log 2>/dev/null",
        host.tmux_socket,
        offset.saturating_add(1)
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
