//! `mule exec`: `ssh host cmd` semantics over mule's private connection.
//!
//! For a caller that needs a synchronous command and must not hold the default
//! master -- mu's remote `git worktree add` is budgeted at 120s, which would be
//! 120s of refused sessions on a capped host. The command is a normal detached
//! job, polled fast, and its output is brought back in one read.
//!
//! What differs from `run --wait` (SPEC § `mule exec`):
//!
//! - **Split streams.** A job's own artifact merges stdout and stderr; exec's
//!   wrapper mode writes stderr to `$d/err`, because a caller parses stdout and
//!   reads the reason from stderr's first line. The amendment is exec-only: exec
//!   is a synchronous stand-in, not an artifact reader.
//! - **Quiet.** No id line and no hints, and mule's non-fatal diagnostics
//!   (lock-wait notice, `max_running`, dispatch-pattern warnings) stay silent.
//!   Only the command's stderr and mule's fatal errors reach stderr.
//! - **Fast poll.** 100ms doubling to a 1s cap, not the 5s a detached job
//!   settles at: the caller is blocked on it.
//! - **Exit 7** when the output was capped, in place of the job's code.
//! - **The job never outlives the call**: removed afterwards, and stopped from a
//!   signal path by a detached `mule rm` ([`crate::cleanup`]).

use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};

use crate::cleanup::{self, Signals};
use crate::config::Host;
use crate::errors::{EXIT_NO_MASTER, EXIT_TRUNCATED, MuleError};
use crate::probe::{From as ProbeFrom, State, probe};
use crate::transport::{Ssh, Transport};
use crate::wrapper::{JobId, JobMetadata, JobMode, new_id, state_dir};

const FIRST_INTERVAL: Duration = Duration::from_millis(100);
const MAX_INTERVAL: Duration = Duration::from_secs(1);

/// Spacing of the probes that wait for a finished job's session to end.
const SETTLE_INTERVAL: Duration = Duration::from_millis(100);

/// How long a job with an `rc` may keep its session before exec gives up on the
/// rest of its output. Wall-clock, probe round trips included: counting only
/// the requested pauses let a slow probe stretch the bound well past 5s. A drain takes milliseconds; a session that outlives this
/// is held open by a process the command left behind holding its stdout (`sleep
/// 30 &`), and waiting longer would only be waiting for that process.
const SESSION_END_WAIT: Duration = Duration::from_secs(5);

/// How long a signal waits for an in-flight dispatch before cleaning up anyway.
///
/// Dispatch is sub-second by invariant, but a holder whose ssh fell back to a
/// direct connect can sit for a TCP timeout, and the signal path must not.
const DISPATCH_GRACE: Duration = Duration::from_secs(3);

/// The collect script's exit status when the read worked but removing the job
/// did not. Distinct from ssh's own 255 and from a read failure, so the caller
/// can still deliver the output it got and then fail naming the removal.
const REMOVAL_FAILED: i32 = 97;

/// How long a failure waits to learn whether a signal caused it.
const SIGNAL_GRACE: Duration = Duration::from_millis(250);

/// What to run.
#[derive(Debug, Clone, Copy)]
pub struct Request<'a> {
    pub cmd: &'a str,
    pub cwd: Option<&'a str>,
    pub max_secs: Option<u64>,
}

/// The poll cadence: 100ms doubling to 1s.
pub fn next_interval(current: Duration) -> Duration {
    current.saturating_mul(2).min(MAX_INTERVAL)
}

/// Start the job under `id`. Silent: warnings go nowhere.
pub fn start(transport: &dyn Transport, host: &Host, id: JobId, request: &Request) -> Result<()> {
    crate::run::dispatch_with_id(
        transport,
        host,
        id,
        request.cmd,
        request.cwd,
        request.max_secs,
        // Not an agent: `MU_MANAGED_AGENT` would make murmur list a one-shot
        // `git rev-parse` as crew.
        JobMetadata::Human,
        JobMode::Exec,
        &mut std::io::sink(),
    )
    .map(drop)
}

/// Wait for the job, print its output, remove it, and return the exit code.
///
/// `pause` is the sleep, injected so a test can observe the cadence without
/// spending it.
pub fn finish(
    transport: &dyn Transport,
    host: &Host,
    id: &JobId,
    pause: &dyn Fn(Duration),
    out: &mut dyn Write,
    err: &mut dyn Write,
) -> Result<i32> {
    finish_within(transport, host, id, SESSION_END_WAIT, pause, out, err)
}

/// [`finish`] with the settle bound as a parameter, so a test can pin the
/// wall-clock behaviour without spending 5s.
pub fn finish_within(
    transport: &dyn Transport,
    host: &Host,
    id: &JobId,
    settle: Duration,
    pause: &dyn Fn(Duration),
    out: &mut dyn Write,
    err: &mut dyn Write,
) -> Result<i32> {
    let mut interval = FIRST_INTERVAL;
    // When the first probe that saw `rc` began. Measured on the wall clock and
    // from before the probe, so the bound covers probe round trips too.
    let mut settling: Option<Instant> = None;
    let code = loop {
        pause(interval);
        let probed = Instant::now();
        // State only: the output comes back once, in the final read, so a
        // probe never ships bytes it would have to resend.
        let result = probe(transport, host, id, ProbeFrom::StateOnly).with_context(|| {
            format!(
                "lost contact with {} while waiting for the command",
                host.name
            )
        })?;
        match result.state {
            // `rc` is written by the left end of the log pipeline and the log
            // by the right end, so `rc` can land while the last bytes -- and
            // the `truncated` marker -- are still in flight. The session ends
            // only when the whole pipeline has, so wait for that here, one
            // short probe at a time, rather than inside the one locked read
            // (a 5s wait there held the ticket and starved every other caller).
            State::Done(code) => {
                let since = *settling.get_or_insert(probed);
                if !result.alive || since.elapsed() >= settle {
                    break code;
                }
                // A drain is milliseconds; the probe's own round trip is the
                // real spacing, so a short fixed pause is enough.
                interval = SETTLE_INTERVAL;
                continue;
            }
            State::Orphan => return Err(MuleError::Orphan { id: id.to_string() }.into()),
            State::Missing => return Err(MuleError::MissingJob { id: id.to_string() }.into()),
            State::Running => {}
        }
        interval = next_interval(interval);
    };
    collect(transport, host, id, code, out, err)
}

/// The final read and the removal, in one round trip.
///
/// No waiting in here: this runs under the ticket lock, so a sleep is a channel
/// outage for every other caller (a 5s wait was measured holding it ~14s with a
/// background child). [`finish`] has already waited for the session to end; the
/// script only *reports* whether it still lives (`live=`), read before the log
/// so that `live=0` proves the log was complete when it was read. A session
/// still alive means a process the command left behind holds its stdout: the
/// output cannot be known complete, so exec returns exit 7 rather than the
/// job's code beside a possibly-empty log, and still removes the job.
///
/// Removal is proved, not hoped for: it is `rm <id>`'s own script
/// ([`crate::jobs::remove_one_script`]), so exit 0 means both sessions and the
/// state directory are seen gone. If it cannot prove that, exec delivers what
/// it read and then fails with exit 1 naming the leftover -- never the
/// command's code, which would tell a caller (mu removes a worktree on success)
/// that the job is gone when a session or its state is still on the host.
fn collect(
    transport: &dyn Transport,
    host: &Host,
    id: &JobId,
    code: i32,
    out: &mut dyn Write,
    err: &mut dyn Write,
) -> Result<i32> {
    let dir = state_dir(id);
    let socket = &host.tmux_socket;
    let max = host.max_log_bytes;
    // `$(( ))` rather than the bare `wc -c` output, which BSD pads with spaces.
    // stderr has no wrapper-side cap, so it is bounded here on the way out; a
    // command whose error stream outgrows the cap is treated as truncated too.
    let script = format!(
        "d={dir}; \
         live=0; tmux -L {socket} has-session -t mule-{id} 2>/dev/null && live=1; \
         t=0; [ -f $d/truncated ] && t=1; \
         ls=$(( $(wc -c < $d/log 2>/dev/null || echo 0) )); \
         es=$(( $(wc -c < $d/err 2>/dev/null || echo 0) )); \
         if [ $es -gt {max} ]; then es={max}; t=1; fi; \
         printf 'trunc=%s\\nlive=%s\\nlog=%s\\nerr=%s\\nbytes:\\n' $t $live $ls $es; \
         head -c $ls $d/log 2>/dev/null; head -c $es $d/err 2>/dev/null; \
         ( {remove} ) >/dev/null || exit {REMOVAL_FAILED}; exit 0",
        remove = crate::jobs::remove_one_script(host, id),
    );
    let output = transport.run(host, &script)?;
    if output.code != 0 && output.code != REMOVAL_FAILED {
        bail!(
            "reading the command's output failed: {}",
            output.stderr.trim()
        );
    }

    const MARKER: &[u8] = b"bytes:\n";
    let marker = output
        .stdout
        .windows(MARKER.len())
        .position(|window| window == MARKER)
        .context("invalid exec reply: missing bytes marker")?;
    let (mut truncated, mut live, mut log_len, mut err_len) = (false, None, 0usize, 0usize);
    for line in String::from_utf8_lossy(&output.stdout[..marker]).lines() {
        if let Some(value) = line.strip_prefix("trunc=") {
            truncated = value == "1";
        } else if let Some(value) = line.strip_prefix("live=") {
            live = Some(value == "1");
        } else if let Some(value) = line.strip_prefix("log=") {
            log_len = value.parse().context("invalid log size in exec reply")?;
        } else if let Some(value) = line.strip_prefix("err=") {
            err_len = value.parse().context("invalid err size in exec reply")?;
        }
    }
    let live = live.context("invalid exec reply: missing live")?;
    let bytes = &output.stdout[marker + MARKER.len()..];
    if bytes.len() < log_len + err_len {
        bail!("exec reply is shorter than its header says");
    }
    out.write_all(&bytes[..log_len])?;
    out.flush()?;
    err.write_all(&bytes[log_len..log_len + err_len])?;
    if output.code == REMOVAL_FAILED {
        bail!(
            "could not remove job {id} on {}: {}",
            host.name,
            output.stderr.trim()
        );
    }
    if live {
        writeln!(
            err,
            "mule: output incomplete: a process still holds the command's stdout on {}; \
             what was printed is partial",
            host.name
        )?;
    }
    if truncated {
        writeln!(
            err,
            "mule: output exceeded max_log_bytes ({max}) on {}; what was printed is partial",
            host.name
        )?;
    }
    if truncated || live {
        return Ok(EXIT_TRUNCATED);
    }
    Ok(code)
}

/// Block this thread until the signal thread ends the process.
fn park() -> ! {
    loop {
        std::thread::park();
    }
}

/// Called when a step failed: if a stop signal is (or is about to be) pending,
/// the failure is its fallout and the signal thread owns the exit.
///
/// A group hangup or Ctrl-C reaches the in-flight `ssh` as well as mule, and
/// the ssh dies with 255 -- a probe error the main thread would turn into exit
/// 1 and a second, wrong cleanup, racing the 129/130 the signal thread is
/// about to give. The grace covers the gap between ssh's death being seen and
/// mule's own handler having run; it is paid only on an already-failed path.
fn park_if_signalled(signals: &Signals) {
    if signals.wait(Some(SIGNAL_GRACE)).is_some() {
        park();
    }
}

/// `mule exec` as the binary runs it: real ssh, real signals.
///
/// `config` is the effective `--config`, handed to the detached cleanup child so
/// it reads the same file.
pub fn run_cli(host: &Host, config: Option<PathBuf>, request: &Request) -> Result<i32> {
    // Exec's stderr belongs to the command; see `lock::set_quiet`.
    crate::lock::set_quiet(true);

    let id: JobId = new_id().parse().expect("generated ids are valid");
    let dispatching = Arc::new(AtomicBool::new(false));
    let signals = Arc::new(Signals::install(&cleanup::STOP_SIGNALS)?);
    {
        let dispatching = Arc::clone(&dispatching);
        let host_name = host.name.clone();
        let id = id.clone();
        let config = config.clone();
        signals.on_signal(move |signal| {
            // First, so nothing below can panic on a dead tty (see
            // `redirect_stdio_to_null`).
            cleanup::redirect_stdio_to_null();
            // A dispatch still in flight has not made the job yet; an `rm`
            // that overtakes it would be a no-op and the job would leak.
            let deadline = Instant::now() + DISPATCH_GRACE;
            while dispatching.load(Ordering::SeqCst) && Instant::now() < deadline {
                std::thread::sleep(Duration::from_millis(10));
            }
            let _ = cleanup::spawn_rm(config.as_deref(), &host_name, &id);
            std::process::exit(cleanup::exit_status_for(signal));
        });
    }

    dispatching.store(true, Ordering::SeqCst);
    let started = start(&Ssh, host, id.clone(), request);
    dispatching.store(false, Ordering::SeqCst);
    if let Err(error) = started {
        park_if_signalled(&signals);
        if matches!(
            error.downcast_ref::<MuleError>(),
            Some(MuleError::NoMaster { .. })
        ) {
            eprintln!("mule: {error}");
            return Ok(EXIT_NO_MASTER);
        }
        // The dispatch may have landed before the reply was lost.
        let _ = cleanup::spawn_rm(config.as_deref(), &host.name, &id);
        return Err(error);
    }

    let result = finish(
        &Ssh,
        host,
        &id,
        &|interval| {
            if signals.wait(Some(interval)).is_some() {
                // The signal thread is cleaning up and about to exit the
                // process. Returning would spin probes -- locked ssh calls --
                // for nothing in the meantime.
                park();
            }
        },
        &mut std::io::stdout().lock(),
        &mut std::io::stderr().lock(),
    );
    if result.is_err() {
        park_if_signalled(&signals);
        // No id was ever printed, so nobody else can remove this job.
        let _ = cleanup::spawn_rm(config.as_deref(), &host.name, &id);
    }
    result
}
