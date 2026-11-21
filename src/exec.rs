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
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};

use crate::cleanup::{self, Signals};
use crate::config::Host;
use crate::errors::{EXIT_NO_MASTER, EXIT_TRUNCATED, MuleError, strict_code, strict_note};
use crate::probe::{From as ProbeFrom, State, probe};
use crate::transport::{InFlight, Ssh, Transport};
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

/// How long a signal waits for the locked call in flight (a dispatch already
/// admitted, a probe, the collect) before cleaning up anyway.
///
/// A dispatch is sub-second by invariant; a probe under load was measured at
/// 2s. A dispatch still queued on its ticket, or about to re-queue after a
/// refused session (`transport::run_within`), is not waited for: the closed
/// gate refuses it.
pub(crate) const DISPATCH_GRACE: Duration = Duration::from_secs(3);

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

/// The largest remote script exec hands to one ssh call.
///
/// The script is a single argv element twice over: locally to `ssh`, and on the
/// host as the `sh -c` string sshd spawns. Two documented ceilings bound it:
/// Linux's per-argument limit `MAX_ARG_STRLEN` (32 pages = 131072 bytes
/// including the NUL, execve(2)), and OpenSSH's mux reader, which drops a
/// client message over 256 KiB (`channel_post_mux_client_read`, channels.c;
/// the ~267KB `mux_client_request_session: write packet: Broken pipe` that
/// spike_exec saw). The Linux limit is the smaller, so it governs; 16 KiB of
/// headroom below it covers the NUL, mux framing and anything the transport
/// adds. Not a measured end-to-end boundary (no sshd in the sandbox): policy
/// derived from those two sources. SPEC § The dispatch size cap.
pub const MAX_DISPATCH_BYTES: usize = 112 * 1024;

/// A transport that sends nothing: it records the script a dispatch would have
/// sent, so the size is measured on the exact string handed to ssh rather than
/// re-derived from the pieces that build it.
struct Measure(std::cell::RefCell<Option<usize>>);

impl Transport for Measure {
    fn run_unlocked(&self, _: &Host, script: &str) -> Result<crate::transport::Output> {
        *self.0.borrow_mut() = Some(script.len());
        bail!("measured, not sent")
    }
    fn mux_op(&self, _: &Host, _: crate::transport::MuxOp) -> Result<crate::transport::Output> {
        bail!("measuring makes no mux call")
    }
    fn master_alive(&self, _: &Host) -> bool {
        true
    }
    // In memory only: no ssh and no session channel, so there is nothing for
    // the ticket lock to order, and waiting on it would make a size check
    // block behind other callers.
    fn run(&self, host: &Host, script: &str) -> Result<crate::transport::Output> {
        self.run_unlocked(host, script)
    }
}

/// Refuse a dispatch too big for one ssh argument, before any transport call
/// (not even a `-O check` or a cleanup `rm`: nothing was dispatched).
pub fn preflight(host: &Host, id: &JobId, request: &Request) -> Result<()> {
    let measure = Measure(Default::default());
    let _ = dispatch(&measure, host, id.clone(), request);
    let size = measure.0.borrow().context("could not size the dispatch")?;
    if size > MAX_DISPATCH_BYTES {
        bail!(
            "command too large for one dispatch ({size} bytes of remote script, limit \
             {MAX_DISPATCH_BYTES}); put the payload in a file and run that"
        );
    }
    Ok(())
}

/// The one dispatch call, so [`preflight`] measures what [`start`] sends.
fn dispatch(transport: &dyn Transport, host: &Host, id: JobId, request: &Request) -> Result<JobId> {
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
}

/// Start the job under `id`. Silent: warnings go nowhere.
pub fn start(transport: &dyn Transport, host: &Host, id: JobId, request: &Request) -> Result<()> {
    preflight(host, &id, request)?;
    dispatch(transport, host, id, request).map(drop)
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
    finish_coded(transport, host, id, settle, pause, out, err, false)
}

/// [`finish_within`] with `--strict-codes` as a parameter: the command's exit 3
/// comes back as `EXIT_COMMAND_3`, with the note as the first line of `err`.
#[allow(clippy::too_many_arguments)] // finish_within's seven plus one flag; a struct would only rename them
pub fn finish_coded(
    transport: &dyn Transport,
    host: &Host,
    id: &JobId,
    settle: Duration,
    pause: &dyn Fn(Duration),
    out: &mut dyn Write,
    err: &mut dyn Write,
    strict: bool,
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
    collect(transport, host, id, code, out, err, strict)
}

/// The final read and the removal, in one round trip.
///
/// Fail closed ([`crate::remote`]): the session list, the `truncated`
/// marker, both sizes and both reads are checked, and any failure stops
/// the script before the removal, so a job is never deleted with its output
/// unread. A `wc` that failed used to read as size 0: exec exited 0 with
/// the command's output silently dropped, and removed the job (review of
/// P7a, round 3).
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
    strict: bool,
) -> Result<i32> {
    let dir = state_dir(id);
    let socket = &host.tmux_socket;
    let max = host.max_log_bytes;
    // `$(( ))` rather than the bare `wc -c` output, which BSD pads with spaces.
    // stderr has no wrapper-side cap, so it is bounded here on the way out; a
    // command whose error stream outgrows the cap is treated as truncated too.
    let script = format!(
        "{prelude}mule_socket={socket}; d={dir}; \
         mule_sessions; live=0; if mule_has mule-{id}; then live=1; fi; \
         t=0; if mule_regular $d/truncated truncated; then t=1; fi; \
         mule_size $d/log log; ls=$REPLY; \
         mule_size $d/err err; es=$REPLY; \
         if [ $es -gt {max} ]; then es={max}; t=1; fi; \
         printf 'trunc=%s\\nlive=%s\\nlog=%s\\nerr=%s\\nbytes:\\n' $t $live $ls $es; \
         if [ $ls -gt 0 ]; then head -c $ls $d/log || mule_u \"cannot read $d/log\"; fi; \
         if [ $es -gt 0 ]; then head -c $es $d/err || mule_u \"cannot read $d/err\"; fi; \
         ( {remove} ) >/dev/null || exit {REMOVAL_FAILED}; exit 0",
        prelude = crate::remote::PRELUDE,
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
    // First on stderr, ahead of the command's own bytes: the one stable place a
    // caller finds the original code. Only when the code is what exec returns
    // (not when 7 or a removal failure replaces it).
    if strict
        && !truncated
        && !live
        && output.code != REMOVAL_FAILED
        && let Some(note) = strict_note(code)
    {
        writeln!(err, "{note}")?;
    }
    err.write_all(&bytes[log_len..log_len + err_len])?;
    if output.code == REMOVAL_FAILED {
        // Cleanup failing says nothing about whether the bytes were clipped:
        // the caller reads the flag, not the (error) exit code.
        if truncated || live {
            note_partial();
        }
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
        note_partial();
        return Ok(EXIT_TRUNCATED);
    }
    Ok(if strict { strict_code(code) } else { code })
}

/// Env var naming a file `exec` creates when its output is partial (truncated
/// at `max_log_bytes`, or still held open). A caller that must tell that 7 from
/// a command's own 7 cannot use the exit code or the stderr note: both are
/// ambiguous, and the command's stderr is attacker-shaped. The file is the one
/// signal only mule writes. Best effort; `create_new` so it never follows a
/// symlink planted at the path.
pub const PARTIAL_FILE_ENV: &str = "MULE_PARTIAL_FILE";

fn note_partial() {
    if let Some(path) = std::env::var_os(PARTIAL_FILE_ENV) {
        let _ = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path);
    }
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

/// How exec stops its job when it cannot finish normally: a signal, a failed
/// dispatch, a failed wait.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Cancel {
    /// A detached child (`setsid`), retrying for up to ten minutes. It
    /// survives the hangup that killed the pane, which is what a terminal
    /// caller needs (SPEC § Detached cleanup).
    Detach,
    /// `--no-detach`: one `rm` attempt from this process, before the exit.
    ///
    /// For a caller that cancels by signalling exec's process group and
    /// kills the group after a grace -- a plugin host does exactly that, and
    /// counts anything that outlives the group as a leak. A detached child is
    /// such a leak by design: `setsid` takes it out of the group. Here every
    /// process mule starts, the `rm`'s ssh included, stays in the group, so
    /// the kill ends all of it. The cost is that the stop is not retried and
    /// can be cut short: `rm` queues on the ticket lock, whose wait is
    /// unbounded, so it can outlast any grace. A caller must read a killed
    /// exec as "the remote job may still run", never as "stopped".
    InProcess,
}

/// Stop `id` the way `cancel` says. Errors are dropped: on the signal path
/// stdio is already `/dev/null`, and on the others the caller is returning
/// its own error.
fn stop(cancel: Cancel, config: Option<&std::path::Path>, host: &Host, id: &JobId, retry: bool) {
    match cancel {
        Cancel::Detach => {
            // Retried like a lease's stop, but unfenced and unrecorded:
            // nothing reattaches to an exec job, and there is no job file to
            // put a record beside.
            let retry = retry.then(|| cleanup::StopRetry {
                retry_for: cleanup::stop_retry_for(),
                fence_gen: None,
                failed_file: None,
            });
            let _ = cleanup::spawn_rm(config, &host.name, id, retry.as_ref());
        }
        // A fresh `Ssh`, not the gated one: the gate is closed by now, and
        // this call is the one the signal path exists to make.
        Cancel::InProcess => {
            let _ = crate::jobs::remove(&Ssh, host, &crate::jobs::Target::One(id.clone()));
        }
    }
}

/// `mule exec` as the binary runs it: real ssh, real signals.
///
/// `config` is the effective `--config`, handed to the detached cleanup child so
/// it reads the same file.
pub fn run_cli(host: &Host, config: Option<PathBuf>, request: &Request) -> Result<i32> {
    run_cli_with(host, config, request, Cancel::Detach, false)
}

/// [`run_cli`] with the cancel mode chosen (`exec --no-detach`).
pub fn run_cli_with(
    host: &Host,
    config: Option<PathBuf>,
    request: &Request,
    cancel: Cancel,
    strict: bool,
) -> Result<i32> {
    let id: JobId = new_id().parse().expect("generated ids are valid");
    // Before the signal handlers, the gate and every transport call: an
    // oversize command was never dispatched, so there is nothing to clean up.
    preflight(host, &id, request)?;
    // Exec's stderr belongs to the command; see `lock::set_quiet`.
    crate::lock::set_quiet(true);

    let signals = Arc::new(Signals::install(&cleanup::STOP_SIGNALS)?);
    // Every locked call goes through one gate the signal path can close, as
    // the lease's does: a probe in flight holds the one session, and exiting
    // under it frees the ticket (pid death) but not the session -- the
    // remote script runs on -- so the detached `rm` would be refused.
    let ssh = InFlight::new(Ssh);
    let calls = ssh.stopper();
    {
        let host = host.clone();
        let id = id.clone();
        let config = config.clone();
        signals.on_signal(move |signal| {
            // First, so nothing below can panic on a dead tty (see
            // `redirect_stdio_to_null`).
            cleanup::redirect_stdio_to_null();
            // Close the gate first: no locked call starts from here on. A
            // dispatch still queued on its ticket is refused, so the job never
            // starts -- its side effects are what the `rm` below cannot undo,
            // while an `rm` of a job never made is a no-op. A dispatch already
            // admitted is the call in flight, and is waited for, as is a probe
            // or the collect: an `rm` that overtook the dispatch would leak the
            // job, and one behind a probe's session would be refused. A group
            // signal kills that ssh too, and the call returns at once.
            calls.stop(DISPATCH_GRACE);
            stop(cancel, config.as_deref(), &host, &id, true);
            std::process::exit(cleanup::exit_status_for(signal));
        });
    }

    let started = start(&ssh, host, id.clone(), request);
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
        stop(cancel, config.as_deref(), host, &id, false);
        return Err(error);
    }

    let result = finish_coded(
        &ssh,
        host,
        &id,
        SESSION_END_WAIT,
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
        strict,
    );
    if result.is_err() {
        park_if_signalled(&signals);
        // No id was ever printed, so nobody else can remove this job.
        stop(cancel, config.as_deref(), host, &id, false);
    }
    result
}
