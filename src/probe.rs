use std::time::Duration;

use anyhow::{Context, Result, bail};

use crate::config::Host;
use crate::errors::MuleError;
use crate::transport::Transport;
use crate::wrapper::{JobId, state_dir};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum State {
    Running,
    Done(i32),
    Orphan,
    Missing,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Probe {
    pub state: State,
    pub runtime_secs: Option<u64>,
    /// Whether the job's tmux session still exists. Independent of `state`: a
    /// job with `rc` is `Done` while its session lives on until the whole log
    /// pipeline has drained, and `exec` waits on exactly that gap.
    pub alive: bool,
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
    /// State plus the pane's current screen in `bytes` (`capture-pane`), or
    /// the saved final screen once the pane is gone. For the lease, which
    /// mirrors the remote screen while mu's startup scan reads it: folding the
    /// capture in keeps that one locked call per tick, not two.
    Screen,
}

impl From {
    /// The `tail -c +N` argument, or `None` when no bytes are wanted.
    fn tail_arg(self) -> Option<u64> {
        match self {
            // `tail -c +N` is 1-based, so byte offset 0 is `+1`. Saturating at
            // the top keeps a nonsensical offset from wrapping into a valid
            // one; it reads the last byte instead of the whole file.
            From::Offset(n) => Some(n.saturating_add(1)),
            From::StateOnly | From::Screen => None,
        }
    }

    /// The command that fills the `bytes:` section of the reply.
    fn bytes_command(self, socket: &str, id: &JobId) -> String {
        match self {
            // Never the transcript as a fallback: raw redraw bytes are not a
            // readable screen (TUI-jobs spec, `mule tail` contract). A live
            // pane is captured; a pane that went between the session read
            // and the capture leaves its saved screen, which is read only as
            // a regular file. Anything else cannot tell.
            From::Screen => format!(
                "if [ \"$alive\" = 1 ]; then \
                   tmux -L {socket} capture-pane -p -J -t mule-{id} 2>/dev/null \
                   || {{ mule_regular $d/screen screen && {{ cat $d/screen || mule_u \"cannot read $d/screen\"; }}; }} \
                   || mule_u \"cannot capture mule-{id}\"; \
                 elif mule_regular $d/screen screen; then \
                   cat $d/screen || mule_u \"cannot read $d/screen\"; \
                 fi"
            ),
            other => match other.tail_arg() {
                Some(n) => format!(
                    "if mule_regular $d/log log; then \
                       tail -c +{n} $d/log || mule_u \"cannot read $d/log\"; fi"
                ),
                None => "true".to_string(),
            },
        }
    }
}

pub fn probe(t: &dyn Transport, host: &Host, id: &JobId, from: impl Into<From>) -> Result<Probe> {
    let dir = state_dir(id);
    let from = from.into();
    // `alive` is read BEFORE `rc`, never after. Every path that ends a job
    // writes `rc` before its session goes (the wrapper's last act; `kill`
    // and the watchdog write it before `kill-session`), so "no session, then
    // no rc" can only be an orphan. The other order is a race: `rc` read
    // empty, the job finishes, `has-session` misses -- a done job reported as
    // an orphan. Under 3 parallel layer-3 suites the lease hit it 3 of 3 (a
    // job ending `exit 7` read as exit 5; `rc` held 7 a moment later), since
    // a loaded tmux client widens the gap. `list_script` reads the live set
    // first for the same reason.
    // Fail closed (SPEC § Remote snippets fail closed, [`crate::remote`]):
    // every external command's status is checked, and only a proof makes an
    // absence. `missing` is `rm`'s proof -- the state dir seen not to exist,
    // and neither `mule-<id>` nor `watch-<id>` in a session list tmux gave.
    // `orphan` is a dir, no session, an `rc` seen not to exist and a `cmd`
    // that does (a dir with no `cmd` is a keyed lease's claim not yet
    // started). `done` is an `rc` read as a regular file whose whole content
    // is a number. Everything else stops the script with `CANNOT_TELL`.
    //
    // One `list-sessions`, read before `rc` (see above), answers both
    // session questions; `has-session` exits 1 for a protocol mismatch as
    // for a missing session, so it was never a proof.
    let script = format!(
        "{prelude}\
         mule_socket={socket}; d={dir}; \
         if mule_absent $d \"state dir\"; then \
           mule_sessions; \
           if mule_has mule-{id} || mule_has watch-{id}; then \
             mule_u \"the state dir is gone but a session for the job is not proven gone\"; fi; \
           printf 'exists=0\\nrc=\\nalive=0\\nwatch=0\\nwhy=\\nruntime=\\nsize=0\\nbytes:\\n'; exit 0; \
         fi; \
         [ -d $d ] || mule_u \"state path $d is not a directory\"; \
         printf 'exists=1\\n'; \
         mule_sessions; alive=0; watch=0; \
         if mule_has mule-{id}; then alive=1; fi; \
         if mule_has watch-{id}; then watch=1; fi; \
         rc=; \
         if mule_regular $d/rc rc; then \
           mule_read $d/rc rc; rc=$REPLY; \
           case $rc in ''|*[!0-9]*) mule_u \"rc $d/rc is not a number: $rc\";; esac; \
         elif [ $alive = 0 ]; then \
           mule_regular $d/cmd cmd || mule_u \"the job is claimed by its key but has not started (no cmd)\"; \
         fi; \
         printf 'rc=%s\\nalive=%s\\nwatch=%s\\nwhy=\\n' \"$rc\" $alive $watch; \
         runtime=; \
         if mule_regular $d/cmd cmd; then \
           mule_mtime $d/cmd; start=$REPLY; end=; \
           if [ -n \"$rc\" ]; then mule_mtime $d/rc; end=$REPLY; \
           elif [ $alive = 1 ]; then mule_now; end=$REPLY; fi; \
           if [ -n \"$end\" ]; then runtime=$((end - start)); [ $runtime -lt 0 ] && runtime=0; fi; \
         fi; \
         printf 'runtime=%s\\n' \"$runtime\"; \
         mule_size $d/log log; printf 'size=%s\\n' $REPLY; \
         printf 'bytes:\\n'; {bytes}",
        prelude = crate::remote::PRELUDE,
        socket = host.tmux_socket,
        bytes = from.bytes_command(&host.tmux_socket, id)
    );
    let mut output = t.run(host, &script)?;
    if output.code == crate::remote::CANNOT_TELL {
        let why = output
            .stderr
            .lines()
            .find_map(|line| line.strip_prefix("mule: cannot tell: "))
            .unwrap_or("the probe could not tell")
            .trim()
            .to_string();
        return Err(MuleError::Unverifiable {
            id: id.to_string(),
            why,
        }
        .into());
    }
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

    let mut exists = None;
    let mut rc = None;
    let mut alive = None;
    let mut watch = Seen::No;
    let mut rc_read = true;
    let mut has_cmd = true;
    let mut why = String::new();
    let mut runtime_secs = None;
    let mut log_size = None;
    for line in output.text().lines() {
        if let Some(value) = line.strip_prefix("exists=") {
            exists = Some(Seen::parse(value));
        } else if let Some(value) = line.strip_prefix("rc=") {
            if !value.is_empty() {
                rc = Some(value.parse::<i32>().context("invalid rc in probe reply")?);
            }
        } else if let Some(value) = line.strip_prefix("alive=") {
            alive = Some(Seen::parse(value));
        } else if let Some(value) = line.strip_prefix("watch=") {
            watch = Seen::parse(value);
        } else if let Some(value) = line.strip_prefix("rcread=") {
            rc_read = value != "0";
        } else if let Some(value) = line.strip_prefix("cmd=") {
            has_cmd = value != "0";
        } else if let Some(value) = line.strip_prefix("why=") {
            why = value.trim().to_string();
        } else if let Some(value) = line.strip_prefix("runtime=") {
            if !value.is_empty() {
                runtime_secs = Some(value.parse().context("invalid runtime in probe reply")?);
            }
        } else if let Some(value) = line.strip_prefix("size=") {
            log_size = Some(
                value
                    .trim()
                    .parse()
                    .context("invalid size in probe reply")?,
            );
        }
    }

    let exists = exists.context("invalid probe reply: missing exists")?;
    let unverifiable = |fallback: &str| -> anyhow::Error {
        let why = if why.is_empty() { fallback } else { &why };
        MuleError::Unverifiable {
            id: id.to_string(),
            why: why.to_string(),
        }
        .into()
    };
    // A job with `rc` is `Done` whatever its session looks like; only it can
    // leave `alive` unknown, and then the session is assumed to live on, so
    // `exec` waits out the drain rather than read a log still being written.
    let alive_now = matches!(alive, Some(Seen::Yes | Seen::Unknown));
    let state = match (exists, rc) {
        (Seen::Unknown, _) => return Err(unverifiable("the state dir could not be checked")),
        (Seen::Yes, _) if !rc_read => {
            return Err(unverifiable("the job's rc could not be read"));
        }
        (Seen::No, _) => {
            if alive != Some(Seen::No) || watch != Seen::No {
                return Err(unverifiable(
                    "the state dir is gone but a session for the job is not proven gone",
                ));
            }
            State::Missing
        }
        (Seen::Yes, Some(code)) => State::Done(code),
        (Seen::Yes, None) => match alive.context("invalid probe reply: missing alive")? {
            Seen::Yes => State::Running,
            // Not started: no `cmd`, so this is a claim, not a job that died.
            Seen::No if !has_cmd => {
                return Err(unverifiable(
                    "the job is claimed by its key but has not started (no cmd)",
                ));
            }
            Seen::No => State::Orphan,
            Seen::Unknown => return Err(unverifiable("the session could not be checked")),
        },
    };
    Ok(Probe {
        state,
        runtime_secs,
        alive: alive_now,
        log_size: log_size.context("invalid probe reply: missing size")?,
        bytes,
    })
}

/// One fact as the probe script saw it: `1`, `0`, or `?` for "could not
/// tell". Anything unexpected is `?`: a reply that says neither cannot prove
/// either.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Seen {
    Yes,
    No,
    Unknown,
}

impl Seen {
    fn parse(value: &str) -> Self {
        match value {
            "1" => Self::Yes,
            "0" => Self::No,
            _ => Self::Unknown,
        }
    }
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
