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
            // readable screen (TUI-jobs spec, `mule tail` contract).
            From::Screen => format!(
                "tmux -L {socket} capture-pane -p -J -t mule-{id} 2>/dev/null || \
                 cat $d/screen 2>/dev/null || true"
            ),
            other => match other.tail_arg() {
                Some(n) => format!("tail -c +{n} $d/log 2>/dev/null"),
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
    // Absence is *seen*, not inferred, exactly as `rm <id>` proves it
    // ([`crate::jobs::remove_one_script`]): `[ -d ]` is false behind an
    // unsearchable parent, and `has-session` exits 1 for a tmux protocol
    // mismatch or an unsafe socket directory as it does for a missing
    // session. Each answer that rests on an absence -- `missing`, `orphan` --
    // is therefore checked a second way, and a check that cannot tell prints
    // `?` and a `why=` line. The second look costs a process only on those
    // paths: a running job passes `has-session`, a finished one has `rc`.
    // `missing` also needs `watch-<id>` gone, as `rm`'s proof does: a live
    // watchdog means the job's state is not all gone.
    //
    // Every tool's own failure counts as "cannot tell", never as a no:
    // `grep -Fxq` exits 1 for no match and 2 for an error, and reading the
    // 2 as "not listed" called a job missing while its watchdog lived (review
    // of P7a, an injected grep exit 2). An `rc` that exists but reads empty
    // or not at all is unknown too, never an absent one: that made a
    // finished job behind an unreadable `rc` an orphan. A state dir with no
    // `cmd` is a keyed lease's claim whose dispatch has not run (or never
    // will): no job is proven to exist or not, so that is unknown as well.
    let script = format!(
        "d={dir}; why=; watch=0; rcread=1; \
         if [ -d $d ]; then exists=1; else \
           e=$(LC_ALL=C ls -d \"$d\" 2>&1); \
           case $?:$e in \
             0:*) exists='?'; why=\"state path $d is not a directory\";; \
             *'No such file or directory'*) exists=0;; \
             *) exists='?'; why=\"cannot verify state dir $d: $e\";; \
           esac; \
         fi; printf 'exists=%s\\n' \"$exists\"; \
         alive=$(tmux -L {socket} has-session -t mule-{id} 2>/dev/null && echo 1 || echo 0); \
         rc=$(cat $d/rc 2>/dev/null); \
         if [ -z \"$rc\" ] && [ -e $d/rc ]; then rcread=0; why=\"cannot read $d/rc\"; fi; \
         printf 'rc=%s\\n' \"$rc\"; printf 'rcread=%s\\n' \"$rcread\"; \
         if [ -e $d/cmd ]; then cmd=1; else cmd=0; fi; printf 'cmd=%s\\n' \"$cmd\"; \
         if [ \"$alive\" = 0 ] && [ -z \"$rc\" ]; then \
           out=$(tmux -L {socket} list-sessions -F '#{{session_name}}' 2>&1); \
           if [ $? -eq 0 ]; then \
             printf '%s\\n' \"$out\" | grep -Fxq mule-{id}; \
             case $? in \
               0) alive='?'; why=\"has-session and list-sessions disagree about mule-{id}\";; \
               1) ;; \
               *) alive='?'; why=\"cannot search the session list for mule-{id}\";; \
             esac; \
             printf '%s\\n' \"$out\" | grep -Fxq watch-{id}; \
             case $? in \
               0) watch=1;; \
               1) ;; \
               *) watch='?'; why=\"cannot search the session list for watch-{id}\";; \
             esac; \
           else \
             case \"$out\" in \
               {no_server}) ;; \
               *) alive='?'; why=\"cannot verify session state: $out\";; \
             esac; \
           fi; \
         fi; \
         printf 'alive=%s\\n' \"$alive\"; printf 'watch=%s\\n' \"$watch\"; \
         printf 'why=%s\\n' \"$(printf %s \"$why\" | tr '\\n' ' ')\"; \
         cmd_mtime=$(stat -c %Y $d/cmd 2>/dev/null || stat -f %m $d/cmd 2>/dev/null); \
         if [ -n \"$rc\" ]; then end_mtime=$(stat -c %Y $d/rc 2>/dev/null || stat -f %m $d/rc 2>/dev/null); \
         elif [ \"$alive\" = 1 ]; then end_mtime=$(date +%s); else end_mtime=; fi; \
         if [ -n \"$cmd_mtime\" ] && [ -n \"$end_mtime\" ]; then runtime=$((end_mtime - cmd_mtime)); \
           [ \"$runtime\" -lt 0 ] && runtime=0; else runtime=; fi; \
         printf 'runtime=%s\\n' \"$runtime\"; \
         printf 'size=%s\\n' \"$(wc -c < $d/log 2>/dev/null || echo 0)\"; \
         printf 'bytes:\\n'; {bytes}",
        socket = host.tmux_socket,
        no_server = crate::jobs::NO_TMUX_SERVER,
        bytes = from.bytes_command(&host.tmux_socket, id)
    );
    let mut output = t.run(host, &script)?;
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
