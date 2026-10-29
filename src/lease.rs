//! `mule run --lease`: a foreground process that lives exactly as long as its job.
//!
//! mu runs a remote worker as a local pane whose death is the agent's death.
//! A held `ssh -t host pi` is that pane today, and on a `MaxSessions 1` host it
//! is the long session hold mule exists to remove. The lease replaces it:
//!
//! 1. it **names the job first**: the id is written to `--job-file` before the
//!    dispatch, so there is no window in which a job exists that nobody can
//!    name (`mule rm` of a never-dispatched id is a no-op);
//! 2. it dispatches a detached TUI job, exactly as `run --tui` does;
//! 3. it forwards mu's unix control socket over mule's master and keeps the
//!    forward verified for the job's life;
//! 4. it exits when the job ends, and ends the job when told to exit.
//!
//! It is a per-job foreground waiter, like `run --wait` (SPEC § Leases). It
//! never re-dispatches, retries, queues or places a job; the only repair it
//! does is idempotent forward repair. Every ssh it makes is a sub-second
//! probe (locked) or a mux op (lock-free).
//!
//! **It never exits while the job may be running**, except on a signal, which
//! stops the job. mu reads only pane liveness: an exit tells mu the agent died.
//! So a missing master, a dropped probe and a wedged forward table print a
//! hand-back line and keep holding.

use std::io::Write;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};

use crate::cleanup::{self, Signals};
use crate::config::Host;
use crate::errors::{EXIT_NO_MASTER, EXIT_ORPHAN, MuleError};
use crate::forward::{self, Spec};
use crate::probe::{From as ProbeFrom, State, probe};
use crate::transport::{InFlight, Ssh, Stopper, Transport};
use crate::wrapper::{JobId, JobMetadata, JobMode, new_id};

/// How long the remote screen is mirrored into the pane after the first
/// successful forward. Long enough for mu's startup health scan and `mu agent
/// read` to see real text (provider and auth errors show up in the first
/// seconds); after it, a capture every tick for hours would be pure channel
/// cost, so the screen is printed only on a change of state and at exit.
pub const MIRROR_FOR: Duration = Duration::from_secs(30);

/// The tick cadence: the poll shape's 1s settling to 5s, reset on change.
const FIRST_INTERVAL: Duration = Duration::from_secs(1);
const MAX_INTERVAL: Duration = Duration::from_secs(5);

/// How long the signal path waits for its own `unforward` before exiting
/// anyway. `unforward` is local work plus lock-free mux ops; the bound is for
/// a main thread that holds the LOCAL's flock mid-forward.
///
/// Not 1s: on a loaded machine each `ssh -O cancel` spawn took 0.25-0.45s, and
/// with three layer-3 suites in parallel 8 of 45 signal-path unforwards ran
/// past 1s (the slowest that finished took 0.997s). The process then exited
/// mid-unforward and left LOCAL and its record behind. 3s, the same bound as
/// `DISPATCH_GRACE`, keeps the exit prompt and still covers a forward in
/// progress on the main thread.
const UNFORWARD_GRACE: Duration = Duration::from_secs(3);

/// What to run.
#[derive(Debug, Clone)]
pub struct Request<'a> {
    pub cmd: &'a str,
    pub cwd: Option<&'a str>,
    pub max_secs: Option<u64>,
    pub metadata: JobMetadata,
}

/// How many fresh temp names `write_job_file` tries before giving up. Each is
/// random, so a collision means somebody is planting names in the directory;
/// a bound turns that into an error instead of a spin.
const TEMP_ATTEMPTS: u32 = 16;

/// Write `id` to `path` atomically: a fresh temp file in the same directory
/// (one line, fsynced), then a rename, then an fsync of the directory.
///
/// Atomic because mu polls for this file and parses its one line: a reader
/// must never see a half-written handle. Durable because the file is the only
/// pointer to the job if the lease is SIGKILLed a moment later.
///
/// The temp is created exclusively (`O_CREAT|O_EXCL`) and never through a
/// symlink (`O_NOFOLLOW`), under a random name, then `fchmod`ed to 0600. The
/// documented `--job-file /tmp/w1.job` sits in a world-writable directory, and
/// the old predictable `F.tmp<pid>` opened with create+truncate let a planted
/// symlink overwrite its target with the id and then become F itself, and let
/// a planted 0644 file lend its mode to F (both reproduced, review round 1).
/// An existing path is never opened, truncated or followed: a name that is
/// taken is skipped for another.
pub fn write_job_file(path: &Path, id: &JobId) -> Result<()> {
    let dir = match path.parent() {
        Some(dir) if !dir.as_os_str().is_empty() => dir.to_path_buf(),
        _ => PathBuf::from("."),
    };
    let name = path
        .file_name()
        .with_context(|| format!("job file {} has no file name", path.display()))?;
    let (tmp, mut file) = create_temp(&dir, name)
        .with_context(|| format!("cannot write job file {}", path.display()))?;
    let written = (|| -> std::io::Result<()> {
        // `mode` applies only at creation and is masked by the umask; the
        // explicit fchmod makes 0600 the mode whatever the umask.
        use std::os::unix::fs::PermissionsExt;
        file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
        file.write_all(format!("{id}\n").as_bytes())?;
        file.sync_all()?;
        drop(file);
        std::fs::rename(&tmp, path)?;
        // The rename is only durable once the directory entry is.
        std::fs::File::open(&dir)?.sync_all()
    })();
    if written.is_err() {
        // Ours: created exclusively above, so removing it removes nobody
        // else's file.
        let _ = std::fs::remove_file(&tmp);
    }
    written.with_context(|| format!("cannot write job file {}", path.display()))
}

/// Create a new, empty, private temp file beside the job file, under a name
/// nobody else holds.
fn create_temp(dir: &Path, name: &std::ffi::OsStr) -> std::io::Result<(PathBuf, std::fs::File)> {
    let mut last = None;
    for _ in 0..TEMP_ATTEMPTS {
        let mut tmp_name = std::ffi::OsString::from(".");
        tmp_name.push(name);
        tmp_name.push(format!(".tmp{}-{:016x}", std::process::id(), random_u64()));
        let tmp = dir.join(tmp_name);
        match std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .custom_flags(libc::O_NOFOLLOW)
            .mode(0o600)
            .open(&tmp)
        {
            Ok(file) => return Ok((tmp, file)),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => last = Some(error),
            Err(error) => return Err(error),
        }
    }
    Err(last.unwrap_or_else(|| std::io::Error::other("no temp name tried")))
}

/// Unpredictable enough that a temp name cannot be planted ahead of time:
/// `RandomState` is seeded from the OS per process and per instance. Not a
/// secret -- `O_EXCL|O_NOFOLLOW` are what make a planted name harmless; the
/// randomness only keeps a planter from forcing every retry to collide.
fn random_u64() -> u64 {
    use std::hash::{BuildHasher, Hasher};
    let mut hasher = std::collections::hash_map::RandomState::new().build_hasher();
    hasher.write_u128(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos(),
    );
    hasher.finish()
}

/// Name the job, then dispatch it as `run --tui` does.
///
/// The id is in `job_file` before the dispatch ssh starts; if that write fails
/// nothing is dispatched. `on_dispatch` brackets the ssh call (true before,
/// false after) so a signal path can wait for an in-flight dispatch rather than
/// overtake it with an `rm` that would be a no-op.
pub fn start(
    t: &dyn Transport,
    host: &Host,
    job_file: &Path,
    request: &Request,
    on_dispatch: &dyn Fn(Option<&JobId>),
    warnings: &mut dyn Write,
) -> Result<JobId> {
    let id: JobId = new_id().parse().expect("generated ids are valid");
    write_job_file(job_file, &id)?;
    on_dispatch(Some(&id));
    let dispatched = crate::run::dispatch_with_id(
        t,
        host,
        id,
        request.cmd,
        request.cwd,
        request.max_secs,
        request.metadata.clone(),
        JobMode::Tui,
        warnings,
    );
    on_dispatch(None);
    dispatched
}

/// The literal attach command, as `run --tui`'s hints print it. Printed, never
/// executed: murmur is the user's, not mule's.
fn attach_hint(id: &JobId) -> String {
    format!("murmur pick --all  # select mule-{id}")
}

fn stop_hint(host: &Host, id: &JobId) -> String {
    format!("mule rm --host {} {id}", host.name)
}

/// How the lease currently sees its link to the job.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Link {
    Ok,
    MasterGone,
    Dropped,
}

/// One lease's hold loop.
pub struct Hold<'a> {
    pub host: &'a Host,
    pub id: &'a JobId,
    pub specs: &'a [Spec],
    /// [`MIRROR_FOR`] in production; a parameter so a test can pin both sides.
    pub mirror_for: Duration,
}

struct Holder<'a, 'h> {
    t: &'a dyn Transport,
    h: &'a Hold<'h>,
    out: &'a mut dyn Write,
    link: Link,
    state: Option<State>,
    forwarded_at: Option<Instant>,
    /// The last forward failure printed, so a persistent one is said once.
    forward_error: Option<String>,
    last_screen: Option<Vec<u8>>,
    /// A change of state outside the mirror window earns one screen, fetched
    /// by the next tick's probe.
    want_screen: bool,
    /// The job's code, seen by a state-only probe. The final screen is the
    /// next tick's one locked call, not a second call in the seeing tick.
    ending: Option<i32>,
}

impl Holder<'_, '_> {
    /// All lease output is fallible and ignored: after a hangup the pane's tty
    /// is gone, and a write that fails must not become a panic or an exit.
    fn say(&mut self, text: &str) {
        let _ = self.out.write_all(text.as_bytes());
        let _ = self.out.flush();
    }

    fn status_line(&self) -> String {
        let state = match (&self.link, &self.state) {
            (Link::MasterGone, _) => "master gone".to_string(),
            (Link::Dropped, _) => "connection dropped".to_string(),
            (_, None) => "starting".to_string(),
            (_, Some(State::Running)) => "running".to_string(),
            (_, Some(State::Done(code))) => format!("done {code}"),
            (_, Some(State::Orphan)) => "orphan".to_string(),
            (_, Some(State::Missing)) => "missing".to_string(),
        };
        let forwards = if self.h.specs.is_empty() {
            "none".to_string()
        } else {
            self.h
                .specs
                .iter()
                .map(|s| format!("{} -> {}", s.local.display(), s.remote))
                .collect::<Vec<_>>()
                .join(", ")
        };
        format!(
            "mule lease {id} on {host}: {state}; forwards: {forwards}\n  \
             attach: {attach}\n  stop:   {stop}\n",
            id = self.h.id,
            host = self.h.host.name,
            attach = attach_hint(self.h.id),
            stop = stop_hint(self.h.host, self.h.id),
        )
    }

    /// Status line, then the remote screen.
    fn show(&mut self, screen: &[u8]) {
        let mut text = self.status_line();
        text.push_str("----\n");
        self.say(&text);
        let _ = self.out.write_all(screen);
        if !screen.ends_with(b"\n") && !screen.is_empty() {
            let _ = self.out.write_all(b"\n");
        }
        let _ = self.out.flush();
        self.last_screen = Some(screen.to_vec());
    }

    fn mirroring(&self) -> bool {
        self.forwarded_at
            .is_none_or(|at| at.elapsed() < self.h.mirror_for)
    }

    fn forward(&mut self, specs: &[Spec]) -> bool {
        match forward::forward(self.t, self.h.host, specs) {
            Ok(()) => {
                self.forwarded_at.get_or_insert_with(Instant::now);
                self.forward_error = None;
                true
            }
            Err(error) => {
                // A wedge or a vanished master is a hand-back; anything else
                // (a non-socket at LOCAL) is the operator's to fix. Neither
                // ends the lease: the job is still running, and exiting would
                // tell mu the agent died. The next tick's drift check retries.
                let text = match error.downcast_ref::<MuleError>() {
                    Some(MuleError::NoMaster { .. }) => {
                        if self.link == Link::MasterGone {
                            return false;
                        }
                        // Said here, so the next tick's check does not say it
                        // again: the hand-back line is printed once.
                        self.link = Link::MasterGone;
                        self.master_gone_line()
                    }
                    _ => format!("mule: {error:#}; the lease keeps holding\n"),
                };
                if self.forward_error.as_deref() != Some(text.as_str()) {
                    self.say(&text);
                    self.forward_error = Some(text);
                }
                false
            }
        }
    }

    fn master_gone_line(&self) -> String {
        format!(
            "mule: master for {} is gone; a human must {}\n  \
             the job keeps running; the lease keeps holding and re-forwards once it is back\n",
            self.h.host.name,
            crate::errors::master_command(self.h.host)
        )
    }

    /// The specs whose forward has drifted: LOCAL missing, its dev+inode not
    /// the recorded ones, or the master not the recorded master (a replaced
    /// master has none of the old forwards). Local stat and a record read
    /// only: no channel, no lock.
    fn drifted(&self, pid: Option<u32>) -> Vec<Spec> {
        self.h
            .specs
            .iter()
            .filter(|spec| {
                let Some(record) = forward::read_record(self.h.host, &spec.local) else {
                    return true;
                };
                record.remote != spec.remote
                    || record.master_pid != pid
                    || forward::identity(&spec.local) != Some((record.dev, record.ino))
            })
            .cloned()
            .collect()
    }

    /// The tick after a state-only probe saw `Done(code)`: its one locked call
    /// is the final capture. The code was decided by what was seen; a capture
    /// that fails, or finds the state already removed, costs only the screen.
    /// No drift repair: the job is over, and its forwards are about to go.
    fn finish(&mut self, code: i32) -> i32 {
        match probe(self.t, self.h.host, self.h.id, ProbeFrom::Screen) {
            Ok(result) if matches!(result.state, State::Done(_)) => self.show(&result.bytes),
            _ => {
                let line = self.status_line();
                self.say(&line);
            }
        }
        self.unforward();
        code
    }

    fn unforward(&mut self) {
        if let Err(error) = forward::unforward(self.t, self.h.host, self.h.specs) {
            self.say(&format!("mule: {error:#}\n"));
        }
    }

    /// One tick. `Some(code)` ends the lease; `None` means hold on, with
    /// whether anything changed (for the cadence).
    fn tick(&mut self) -> (Option<i32>, bool) {
        if let Some(code) = self.ending {
            return (Some(self.finish(code)), true);
        }
        // `-O check`: lock-free, no channel.
        let Some(pid) = forward::check_master(self.t, self.h.host) else {
            if self.link != Link::MasterGone {
                self.link = Link::MasterGone;
                let line = self.master_gone_line();
                self.say(&line);
            }
            return (None, false);
        };
        let mut changed = false;
        if self.link == Link::MasterGone {
            self.link = Link::Ok;
            self.say(&format!(
                "mule: master for {} is back; re-checking forwards\n",
                self.h.host.name
            ));
            changed = true;
        }

        let drifted = self.drifted(pid);
        if !drifted.is_empty() && self.forward(&drifted) {
            changed = true;
        }

        let from = if self.mirroring() || self.link != Link::Ok || self.want_screen {
            ProbeFrom::Screen
        } else {
            ProbeFrom::StateOnly
        };
        let result = match probe(self.t, self.h.host, self.h.id, from) {
            Ok(result) => result,
            Err(error) => {
                // Mid-probe failure: the connection dropped, or the master went
                // between the check and the probe. Either way the job is not
                // known to have ended, so hold on.
                if self.link == Link::Ok {
                    self.link = Link::Dropped;
                    self.say(&format!(
                        "mule: lost contact with {} mid-probe: {error:#}\n  \
                         the job keeps running; the lease keeps holding\n",
                        self.h.host.name
                    ));
                }
                return (None, false);
            }
        };
        let link_changed = self.link != Link::Ok;
        self.link = Link::Ok;
        if from == ProbeFrom::Screen {
            self.want_screen = false;
        }
        let state_changed = self.state.as_ref() != Some(&result.state);
        self.state = Some(result.state.clone());

        match result.state {
            State::Done(code) if from == ProbeFrom::Screen => {
                self.show(&result.bytes);
                self.unforward();
                (Some(code), true)
            }
            State::Done(code) => {
                // A state-only probe cannot know it will see the end, and a
                // capture now would be a second locked call this tick (SPEC §
                // Leases: one per tick, exit included). The next tick fetches
                // the screen; the code is already decided.
                self.ending = Some(code);
                (None, true)
            }
            State::Orphan => {
                let line = self.status_line();
                self.say(&line);
                self.say(&format!(
                    "mule: job {} is orphaned; no rc will ever arrive\n",
                    self.h.id
                ));
                self.unforward();
                (Some(EXIT_ORPHAN), true)
            }
            State::Missing => {
                let line = self.status_line();
                self.say(&line);
                self.say(&format!("mule: job {} not found\n", self.h.id));
                self.unforward();
                (Some(1), true)
            }
            State::Running => {
                let screen_changed = from == ProbeFrom::Screen
                    && self.last_screen.as_deref() != Some(result.bytes.as_slice());
                if from == ProbeFrom::Screen && (screen_changed || state_changed || link_changed) {
                    self.show(&result.bytes);
                } else if state_changed || link_changed {
                    // Out of the mirror window: a change of state earns one
                    // screen, on the next tick.
                    self.want_screen = true;
                }
                (
                    None,
                    changed || screen_changed || state_changed || link_changed,
                )
            }
        }
    }
}

/// The hold loop. Returns the lease's exit code: the job's own when it ends,
/// 5 for an orphan, 1 for a missing job. Never returns while the job may be
/// running: a missing master or a dropped connection keeps it holding at 5s.
///
/// The first thing it does is forward every spec, unconditionally: a stale
/// listener from a SIGKILLed predecessor is replaced, and REMOTE need not exist
/// yet (it is resolved per connection). `pause` is the sleep, injected so the
/// CLI can make it signal-interruptible and a test can drive the clock.
pub fn hold(
    t: &dyn Transport,
    h: &Hold,
    pause: &mut dyn FnMut(Duration),
    out: &mut dyn Write,
) -> i32 {
    let mut holder = Holder {
        t,
        h,
        out,
        link: Link::Ok,
        state: None,
        forwarded_at: None,
        forward_error: None,
        last_screen: None,
        want_screen: false,
        ending: None,
    };
    let line = holder.status_line();
    holder.say(&line);
    if !h.specs.is_empty() {
        holder.forward(h.specs);
    } else {
        holder.forwarded_at = Some(Instant::now());
    }
    let mut interval = FIRST_INTERVAL;
    loop {
        let (exit, changed) = holder.tick();
        if let Some(code) = exit {
            return code;
        }
        interval = if holder.link != Link::Ok {
            MAX_INTERVAL
        } else if changed {
            FIRST_INTERVAL
        } else {
            interval.saturating_mul(2).min(MAX_INTERVAL)
        };
        pause(interval);
    }
}

/// What the signal path does, as steps, so their order is testable.
pub trait SignalSteps {
    /// Point fds 1 and 2 at `/dev/null`: the pane's tty is gone after SIGHUP.
    fn redirect_stdio(&mut self);
    /// Spawn the detached `mule rm <id>`.
    fn spawn_stop(&mut self);
    /// Cancel the forwards (lock-free mux ops and local files).
    fn unforward(&mut self);
}

/// Run the signal path and return the exit status: redirect first, so nothing
/// after can panic on a dead tty; then the stop, which matters most; then the
/// forwards; then the caller exits at once, never waiting on the ticket lock.
pub fn on_signal(steps: &mut dyn SignalSteps, signal: i32) -> i32 {
    steps.redirect_stdio();
    steps.spawn_stop();
    steps.unforward();
    cleanup::exit_status_for(signal)
}

/// Block this thread until the signal thread ends the process.
fn park() -> ! {
    loop {
        std::thread::park();
    }
}

struct RealSignalSteps {
    host: Host,
    id: JobId,
    specs: Vec<Spec>,
    config: Option<PathBuf>,
    dispatching: Arc<AtomicBool>,
    /// The main thread's locked calls (dispatch, probes).
    calls: Stopper,
}

impl SignalSteps for RealSignalSteps {
    fn redirect_stdio(&mut self) {
        cleanup::redirect_stdio_to_null();
    }

    fn spawn_stop(&mut self) {
        // A dispatch still in flight has not made the job yet; an `rm` that
        // overtook it would be a no-op and the job would leak.
        let deadline = Instant::now() + crate::exec::DISPATCH_GRACE;
        while self.dispatching.load(Ordering::SeqCst) && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        // A probe in flight holds the one session, and exiting would not free
        // it: the remote script runs on, while the ticket goes with our pid.
        // The `rm` would be served a taken channel and refused -- under three
        // parallel layer-3 suites a probe took 2s, and a `mule rm` right after
        // a TERMed lease failed this way. So no new call starts from here on,
        // and the one running gets the rest of the same bound. A group signal
        // kills that ssh too, and its call returns at once.
        self.calls
            .stop(deadline.saturating_duration_since(Instant::now()));
        let _ = cleanup::spawn_rm(self.config.as_deref(), &self.host.name, &self.id);
    }

    fn unforward(&mut self) {
        // On a thread with a bound: the main thread may hold a LOCAL's flock
        // mid-forward, and the exit must not wait on it for long.
        let (host, specs) = (self.host.clone(), self.specs.clone());
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let _ = forward::unforward(&Ssh, &host, &specs);
            let _ = done_tx.send(());
        });
        let _ = done_rx.recv_timeout(UNFORWARD_GRACE);
    }
}

/// `mule run --lease` as the binary runs it: real ssh, real signals.
///
/// `config` is the effective `--config`, handed to the detached cleanup child.
pub fn run_cli(
    host: &Host,
    config: Option<PathBuf>,
    job_file: &Path,
    specs: Vec<Spec>,
    request: &Request,
) -> Result<i32> {
    let signals = Arc::new(Signals::install(&cleanup::STOP_SIGNALS)?);
    let dispatching = Arc::new(AtomicBool::new(false));
    let ssh = InFlight::new(Ssh);
    let calls = ssh.stopper();
    // The id is known only once `start` has generated it; until then a signal
    // has nothing to stop (nothing was dispatched) and just exits.
    let named: Arc<std::sync::Mutex<Option<JobId>>> = Arc::default();
    {
        let (host, specs, config) = (host.clone(), specs.clone(), config.clone());
        let (dispatching, named) = (Arc::clone(&dispatching), Arc::clone(&named));
        let calls = calls.clone();
        signals.on_signal(move |signal| {
            // Held to the exit when no id is named yet: `on_dispatch` takes the
            // same lock before the dispatch ssh, so a dispatch can never start
            // after this thread decided there was nothing to stop.
            let guard = named.lock();
            let id = guard.as_ref().ok().and_then(|id| (*id).clone());
            if id.is_some() {
                drop(guard);
            }
            let code = match id {
                Some(id) => on_signal(
                    &mut RealSignalSteps {
                        host,
                        id,
                        specs,
                        config,
                        dispatching,
                        calls,
                    },
                    signal,
                ),
                None => {
                    cleanup::redirect_stdio_to_null();
                    cleanup::exit_status_for(signal)
                }
            };
            std::process::exit(code);
        });
    }

    let mut stderr = std::io::stderr();
    let started = start(
        &ssh,
        host,
        job_file,
        request,
        &|id| {
            if let Some(id) = id {
                // `dispatching` is set under the lock, before the id is
                // visible: once the signal thread can see an id it also sees
                // the dispatch as in flight, so its `rm` waits for it rather
                // than overtaking it (a no-op `rm`, then a leaked job).
                if let Ok(mut named) = named.lock() {
                    dispatching.store(true, Ordering::SeqCst);
                    *named = Some(id.clone());
                }
            } else {
                dispatching.store(false, Ordering::SeqCst);
            }
        },
        &mut stderr,
    );
    let id = match started {
        Ok(id) => id,
        Err(error) => {
            if signals.pending().is_some() {
                park();
            }
            if matches!(
                error.downcast_ref::<MuleError>(),
                Some(MuleError::NoMaster { .. })
            ) {
                // The lease's only exit 3. The id in the job file was never
                // dispatched, so the stop template is a no-op on it.
                let _ = writeln!(stderr, "mule: {error}");
                return Ok(EXIT_NO_MASTER);
            }
            // The dispatch may have landed before its reply was lost; the id is
            // in the job file either way, and `rm` of a never-made job is a
            // no-op. (A job-file write failure never gets here with an id.)
            if let Ok(Some(id)) = named.lock().map(|id| id.clone()) {
                let _ = cleanup::spawn_rm(config.as_deref(), &host.name, &id);
            }
            return Err(error);
        }
    };

    let code = hold(
        &ssh,
        &Hold {
            host,
            id: &id,
            specs: &specs,
            mirror_for: MIRROR_FOR,
        },
        &mut |interval| {
            if signals.wait(Some(interval)).is_some() {
                // The signal thread is stopping the job and about to exit the
                // process; another probe would be a locked call for nothing.
                park();
            }
        },
        &mut std::io::stdout(),
    );
    if signals.pending().is_some() {
        park();
    }
    Ok(code)
}
