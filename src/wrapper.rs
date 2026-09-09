use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::config::Host;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Job {
    pub id: JobId,
    pub cmd: String,
    pub cwd: Option<String>,
}

/// Remote per-job state, as a shell string (never a local `PathBuf`).
///
/// Jobs live under `jobs/` rather than directly in the state dir because the
/// ticket lock keeps `<host>.lock` in the same tree. Sharing one parent made
/// `coop ls` report `dev.lock` as an orphaned job, and would have let prune
/// delete a live lock. They collide whenever the orchestrator and the target
/// are the same machine, which is exactly the local-sshd test setup.
pub fn state_dir(id: &JobId) -> String {
    format!("{JOBS_ROOT}/{id}")
}

/// Parent of every job's state directory.
pub const JOBS_ROOT: &str = "$HOME/.local/state/coop/jobs";

pub fn dispatch_script(host: &Host, job: &Job) -> String {
    let dir = state_dir(&job.id);
    let command = base64(job.cmd.as_bytes());
    let cwd = job
        .cwd
        .as_deref()
        .or(host.default_cwd.as_deref())
        .unwrap_or("$HOME");

    // The cwd is encoded for the same reason the command is: it is user input
    // crossing the same three expansion layers. Interpolated raw, a path with a
    // space splits into two words and `cd` either fails or -- worse -- succeeds
    // against the wrong directory. `$HOME` is the one value coop supplies
    // itself, and it must stay unencoded so the remote shell expands it.
    let cd = match cwd {
        "$HOME" => "cd \"$HOME\"".to_string(),
        path => format!(
            "cd \"$(printf %s {} | base64 -d)\"",
            base64(path.as_bytes())
        ),
    };

    format!(
        "mkdir -p {dir} && printf %s {command} | base64 -d > {dir}/cmd && \
         tmux -L {} new-session -d -s coop-{} \
         '{cd} && printf %s {command} | base64 -d | sh > {dir}/log 2>&1; echo $? > {dir}/rc'",
        host.tmux_socket, job.id
    )
}

/// Width of a generated id, in hex digits.
pub const ID_HEX_LEN: usize = 6;

pub fn new_id() -> String {
    // Time plus pid avoids a dependency for a non-secret id; the odd step keeps
    // the low 24 bits unique until the six-hex-digit space wraps.
    static NEXT: OnceLock<AtomicU64> = OnceLock::new();
    let next = NEXT.get_or_init(|| {
        let time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
        AtomicU64::new(time ^ u64::from(std::process::id()))
    });
    let value = next.fetch_add(0x9e37_79b9_7f4a_7c15, Ordering::Relaxed);
    format!("{:06x}", value & 0x00ff_ffff)
}

fn base64(input: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut output = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let bits = u32::from(chunk[0]) << 16
            | u32::from(*chunk.get(1).unwrap_or(&0)) << 8
            | u32::from(*chunk.get(2).unwrap_or(&0));
        output.push(ALPHABET[((bits >> 18) & 63) as usize] as char);
        output.push(ALPHABET[((bits >> 12) & 63) as usize] as char);
        output.push(if chunk.len() > 1 {
            ALPHABET[((bits >> 6) & 63) as usize] as char
        } else {
            '='
        });
        output.push(if chunk.len() > 2 {
            ALPHABET[(bits & 63) as usize] as char
        } else {
            '='
        });
    }
    output
}

/// A validated job id: exactly six lowercase hex digits.
///
/// Every verb takes an id from the command line and interpolates it into a
/// remote path, a tmux target, and a shell script. Unvalidated, that is command
/// injection: `coop poll 'x$(touch /tmp/pwn)y'` reached the remote shell as
/// syntax and would have executed. Parsing at the boundary makes the unsafe
/// value unrepresentable rather than relying on every call site to quote.
///
/// Hex also avoids `:` and `.`, which tmux's target grammar reserves.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobId(String);

impl JobId {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::str::FromStr for JobId {
    type Err = anyhow::Error;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        let ok = raw.len() == ID_HEX_LEN
            && raw
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b));
        if !ok {
            anyhow::bail!(
                "invalid job id {raw:?}: expected {ID_HEX_LEN} lowercase hex digits, as printed by `coop run`"
            );
        }
        Ok(Self(raw.to_string()))
    }
}

impl std::fmt::Display for JobId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}
