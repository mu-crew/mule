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
//! `--attach <id>` is the same process for a job already running, minus 1 and
//! 2 (see [`attach`]): a new pane for a job whose carrier died.
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
use crate::key::JobKey;
use crate::probe::{From as ProbeFrom, State, probe};
use crate::run::Dispatched;
use crate::stopfence::StopFence;
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

/// Write `id` to `path` as the job file: [`write_line_file`] with the id.
pub fn write_job_file(path: &Path, id: &JobId) -> Result<()> {
    write_line_file(path, id.as_str())
        .with_context(|| format!("cannot write job file {}", path.display()))
}

/// Write one `line` to `path` atomically: a fresh temp file in the same
/// directory (the line plus `\n`, fsynced), then a rename, then an fsync of
/// the directory. The job file and the stop-failure record share it.
///
/// Atomic because mu polls for these files and parses their one line: a
/// reader must never see a half-written handle. Durable because the job file
/// is the only pointer to the job if the lease is SIGKILLed a moment later.
///
/// The temp is created exclusively (`O_CREAT|O_EXCL`) and never through a
/// symlink (`O_NOFOLLOW`), under a random name, then `fchmod`ed to 0600. The
/// documented `--job-file /tmp/w1.job` sits in a world-writable directory, and
/// the old predictable `F.tmp<pid>` opened with create+truncate let a planted
/// symlink overwrite its target with the id and then become F itself, and let
/// a planted 0644 file lend its mode to F (both reproduced, review round 1).
/// An existing path is never opened, truncated or followed: a name that is
/// taken is skipped for another.
pub fn write_line_file(path: &Path, line: &str) -> Result<()> {
    // Callers add the context (which file, and what it is for); one layer
    // here would print the path twice.
    let (dir, tmp) = write_temp(path, line)?;
    let renamed = std::fs::rename(&tmp, path)
        // The rename is only durable once the directory entry is.
        .and_then(|()| std::fs::File::open(&dir)?.sync_all());
    if renamed.is_err() {
        // Ours: created exclusively, so removing it removes nobody else's file.
        let _ = std::fs::remove_file(&tmp);
    }
    Ok(renamed?)
}

/// `line` plus `\n` in a fresh, fsynced, 0600 temp file beside `path`.
/// Returns the directory and the temp's path; the caller renames or links it.
fn write_temp(path: &Path, line: &str) -> Result<(PathBuf, PathBuf)> {
    let dir = match path.parent() {
        Some(dir) if !dir.as_os_str().is_empty() => dir.to_path_buf(),
        _ => PathBuf::from("."),
    };
    let name = path
        .file_name()
        .with_context(|| format!("{} has no file name", path.display()))?;
    let (tmp, mut file) = create_temp(&dir, name)?;
    let written = (|| -> std::io::Result<()> {
        // `mode` applies only at creation and is masked by the umask; the
        // explicit fchmod makes 0600 the mode whatever the umask.
        use std::os::unix::fs::PermissionsExt;
        file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
        file.write_all(format!("{line}\n").as_bytes())?;
        file.sync_all()
    })();
    if let Err(error) = written {
        let _ = std::fs::remove_file(&tmp);
        return Err(error.into());
    }
    Ok((dir, tmp))
}

/// What [`publish_job_file`] found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Published {
    /// There was no job file; this call created it.
    Written,
    /// The job file already named this job, and was left alone.
    Kept,
}

/// How many times [`publish_job_file`] goes round read, then link. Each lap
/// means another process created the file and someone removed it again before
/// the re-read; a bound turns a fight over the path into an error.
const PUBLISH_ATTEMPTS: u32 = 4;

/// Publish `id` as the job file for an attach, never replacing anything.
///
/// The file is mu's handle to the job, and a reattach must not quietly retarget
/// it (spec 2026-11-04-lease-adopt § mule_attach step 4, review item mule 4):
///
/// - missing: write a temp, then `link(temp, path)`, which fails with EEXIST
///   rather than replace a file that appeared since the read. On EEXIST the
///   file is read again and that read decides;
/// - naming this id: kept as it is;
/// - naming another id, empty, invalid or unreadable: an error, file untouched.
///
/// `rename` would be the wrong primitive: it replaces whatever won the race.
pub fn publish_job_file(path: &Path, id: &JobId) -> Result<Published> {
    publish_job_file_with(path, id, &mut || {})
}

/// [`publish_job_file`] with a hook between the read that found no file and
/// the link, so a test can plant a racing writer exactly there.
#[doc(hidden)]
pub fn publish_job_file_with(
    path: &Path,
    id: &JobId,
    before_link: &mut dyn FnMut(),
) -> Result<Published> {
    let refuse = |why: String| {
        anyhow::anyhow!(
            "job file {} {why}; refusing to attach job {id} over it",
            path.display()
        )
    };
    for _ in 0..PUBLISH_ATTEMPTS {
        match std::fs::read_to_string(path) {
            Ok(text) => {
                let named = text.trim();
                if named.is_empty() {
                    return Err(refuse("is empty".to_string()));
                }
                return match named.parse::<JobId>() {
                    Ok(other) if other == *id => Ok(Published::Kept),
                    Ok(other) => Err(refuse(format!("names another job, {other}"))),
                    Err(_) => Err(refuse(format!("holds no job id: {named:?}"))),
                };
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(error).with_context(|| {
                    format!(
                        "cannot read job file {}; refusing to attach job {id}",
                        path.display()
                    )
                });
            }
        }
        before_link();
        let (dir, tmp) = write_temp(path, id.as_str())
            .with_context(|| format!("cannot write job file {}", path.display()))?;
        let linked = std::fs::hard_link(&tmp, path);
        let _ = std::fs::remove_file(&tmp);
        match linked {
            Ok(()) => {
                std::fs::File::open(&dir)
                    .and_then(|dir| dir.sync_all())
                    .with_context(|| format!("cannot write job file {}", path.display()))?;
                return Ok(Published::Written);
            }
            // Somebody else published first: their file decides, on the
            // next read.
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("cannot write job file {}", path.display()));
            }
        }
    }
    Err(refuse(
        "keeps appearing and disappearing under another writer".to_string(),
    ))
}

/// The stop-failure record beside `job_file`: `<job_file>.<id>.stop-failed`.
///
/// The path names the job, so a late write by an old stop child can never
/// overwrite -- or a late success delete -- a newer job's record under the
/// same name (cross-repo contract § 2).
pub fn stop_failed_path(job_file: &Path, id: &JobId) -> PathBuf {
    let mut path = job_file.as_os_str().to_os_string();
    path.push(format!(".{id}.stop-failed"));
    PathBuf::from(path)
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

/// The job a carrier owns, and the stop fence generation it owns it at.
///
/// Fixed before the carrier can own the job, and the only generation its
/// stop is ever fenced with: a lease reads it before its dispatch; an attach
/// owns the number it bumped to. Never re-read at signal time -- a signal
/// thread descheduled after its pane went could read an attach's new
/// generation and kill the reattached job, and a read error there left the
/// stop unfenced (review of mule_stop, both reproduced).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Owned {
    pub id: JobId,
    pub fence_gen: u64,
}

/// The retrying stop a signalled carrier hands its detached `rm`: always
/// fenced, with exactly the generation it owns, and recorded beside
/// `failed_file`'s job.
pub fn stop_retry<'a>(
    owned: &Owned,
    failed_file: &'a Path,
    retry_for: Duration,
) -> cleanup::StopRetry<'a> {
    cleanup::StopRetry {
        retry_for,
        fence_gen: Some(owned.fence_gen),
        failed_file: Some(failed_file),
    }
}

/// Name the job, own its fence generation, then dispatch it as `run --tui`
/// does.
///
/// The generation is read from `fences` (normally [`StopFence::dir`]) before
/// anything is written or dispatched; an unreadable or invalid fence is an
/// error with nothing launched, because the stop would have no generation to
/// be fenced with. The id is in `job_file` before the dispatch ssh starts; if
/// that write fails nothing is dispatched. `on_dispatch` brackets the ssh call
/// (the owned job before, `None` after) so a signal path can wait for an
/// in-flight dispatch rather than overtake it with an `rm` that would be a
/// no-op.
pub fn start(
    t: &dyn Transport,
    host: &Host,
    job_file: &Path,
    fences: &Path,
    request: &Request,
    on_dispatch: &dyn Fn(Option<&Owned>),
    warnings: &mut dyn Write,
) -> Result<Owned> {
    start_keyed(
        t,
        host,
        job_file,
        fences,
        request,
        None,
        on_dispatch,
        &|| {},
        warnings,
    )
}

/// [`start`] with a caller key (`run --lease --key K`): a job that already
/// holds `key` is held instead of a new one started.
///
/// The job file must only ever name a job that holds the key, so the key is
/// claimed first ([`crate::run::claim`]), in one locked round trip, before
/// the job file is written:
///
/// - **held by another job:** that job is owned as [`attach`] owns one --
///   the fence lock, a probe, the job file published without replacing
///   anything, the fence bumped -- except that any state is accepted,
///   `unknown` included (a claim whose dispatch is still to come). A finished
///   job is the answer to "start the effect with key K": the hold reports how
///   it ended. This is also the loser of a race between two keyed leases:
///   the claim is serialised by the ticket lock, so the second sees the
///   first's claim and holds that job. `on_dispatch` is called with the
///   owned job and then `None` inside the fence lock, bracketing no
///   dispatch: it only names the job for the signal path.
/// - **claimed:** exactly [`start`]: the job file names the fresh id, then
///   the dispatch, whose key check finds this job's own claim and starts it.
///
/// The fresh id is named (`on_dispatch(Some)`) before each claim, so a
/// signal while the claim is in flight removes a claim that landed. Once a
/// claim is known to have made nothing -- it found the key held, or it
/// stopped in its lookup, before `mkdir` -- `on_unnamed` takes the name back:
/// nothing of that id exists, and a later failure must not spawn an `rm`
/// for it (review of P7a, round 4: every injected failure past such a claim
/// spawned a detached `mule rm` of an id that never existed).
#[allow(clippy::too_many_arguments)] // `start`'s seam, plus the key.
pub fn start_keyed(
    t: &dyn Transport,
    host: &Host,
    job_file: &Path,
    fences: &Path,
    request: &Request,
    key: Option<&JobKey>,
    on_dispatch: &dyn Fn(Option<&Owned>),
    on_unnamed: &dyn Fn(),
    warnings: &mut dyn Write,
) -> Result<Owned> {
    let id: JobId = new_id().parse().expect("generated ids are valid");
    // Almost always missing (0) for a fresh id; read anyway, since a reused
    // id's leftover fence is exactly what the number must match. Read before
    // the claim, so a failure here leaves nothing on the host.
    let fence_gen = StopFence::in_dir(fences, &id)
        .generation_unlocked()
        .context("cannot own the job's stop fence; nothing was dispatched")?;
    if let Some(key) = key {
        // Three claims at most: another after rolling back a claim proven
        // abandoned, or after the holder vanished in between; past that the
        // holder is owned as it is, never rolled back in a loop.
        let mut rounds = 0;
        loop {
            rounds += 1;
            // Named for the signal path before the claim can land: a signal
            // from here on removes the claim (`rm` of this id), rather than
            // leave the key held by a job that will never start.
            on_dispatch(Some(&Owned {
                id: id.clone(),
                fence_gen,
            }));
            let existing = match crate::run::claim(t, host, &id, key) {
                Ok(crate::run::Claim::Claimed) => break,
                Ok(crate::run::Claim::Held(existing)) => {
                    on_unnamed();
                    existing
                }
                Err(error) => {
                    if crate::run::claimed_nothing(&error) {
                        on_unnamed();
                    }
                    return Err(error);
                }
            };
            match crate::run::claim_state(t, host, &existing)? {
                crate::run::ClaimState::Gone if rounds < 3 => continue,
                crate::run::ClaimState::Unstarted(Some(claimer))
                    if rounds < 3 && crate::claimer::proven_gone(&claimer) =>
                {
                    let _ = writeln!(
                        warnings,
                        "mule: job {existing} held key {key} but never started, and its \
                         claimer ({}) is gone; rolling the claim back",
                        claimer.line()
                    );
                    crate::run::rollback_claim(t, host, &existing, &claimer)?;
                    continue;
                }
                state => {
                    if let crate::run::ClaimState::Unstarted(claimer) = &state {
                        let by = claimer
                            .as_ref()
                            .map_or("an unrecorded claimer".to_string(), |c| c.line());
                        let _ = writeln!(
                            warnings,
                            "mule: job {existing} holds key {key} but has not started; its \
                             claimer ({by}) is not proven gone, so this lease holds it as \
                             unknown. If that claimer is gone: mule rm --host {} {existing}",
                            host.name
                        );
                    }
                    return own_existing(
                        t,
                        host,
                        &existing,
                        job_file,
                        &StopFence::in_dir(fences, &existing),
                        &|owned| {
                            on_dispatch(Some(owned));
                            on_dispatch(None);
                        },
                        Require::AnyState,
                    );
                }
            }
        }
    }
    if let Err(error) = write_job_file(job_file, &id) {
        // Nothing names the claim, so take it back rather than leave the key
        // held by a job that will never start. Best effort: a claim left
        // behind is still found by its key, and polls `unknown`.
        if key.is_some() {
            let _ = crate::jobs::remove(t, host, &crate::jobs::Target::One(id.clone()));
        }
        return Err(error);
    }
    let owned = Owned { id, fence_gen };
    on_dispatch(Some(&owned));
    let dispatched = crate::run::dispatch_keyed(
        t,
        host,
        owned.id.clone(),
        key,
        request.cmd,
        request.cwd,
        request.max_secs,
        request.metadata.clone(),
        JobMode::Tui,
        warnings,
    );
    on_dispatch(None);
    match dispatched? {
        Dispatched::New(_) => Ok(owned),
        // The claim made the key this job's, so only a second client
        // claiming it at the same moment gets here.
        Dispatched::Existing(other) => anyhow::bail!(
            "key {} is held by job {other} as well as by this lease's claim, job {}; \
             nothing was dispatched",
            key.map(JobKey::as_str).unwrap_or_default(),
            owned.id,
        ),
    }
}

/// Which states [`own_existing`] accepts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Require {
    /// `--attach`: a new carrier for a job that is running.
    Running,
    /// A keyed lease that found its key: the job is the answer, whatever its
    /// state, and the hold reports how it ended.
    AnyState,
}

/// Own a job that is already running: `run --lease --attach`, steps 1-5 of
/// spec 2026-11-04-lease-adopt § mule_attach. [`hold`] is the rest, unchanged.
///
/// 1. No master: [`MuleError::NoMaster`] (exit 3), with nothing written.
/// 2. Take `fence`'s lock, waiting out a stop attempt in flight: an old
///    carrier's detached `rm` holds it for each whole attempt, so after the
///    wait the probe sees what that attempt left.
/// 3. Probe once, state only. Anything but `Running`, or a probe error, is an
///    error (exit 1) with the job file untouched and the generation unchanged
///    (taking the lock may create the fence file, empty: generation 0, which
///    is what a missing one means).
/// 4. Publish the job file without replacing anything ([`publish_job_file`]).
/// 5. Bump the fence, hand the new generation to `on_owned` (the CLI stores
///    it where its signal path reads it), and only then release the lock.
///
/// The bump is what retires an old carrier's stop: its child wakes, locks,
/// sees a generation that is not its own and exits with no `rm`. The bumped
/// number is this carrier's own, so its signal-path stop is fenced with it
/// and still kills the job.
pub fn attach(
    t: &dyn Transport,
    host: &Host,
    id: &JobId,
    job_file: &Path,
    fence: &StopFence,
    on_owned: &dyn Fn(&Owned),
) -> Result<Owned> {
    own_existing(t, host, id, job_file, fence, on_owned, Require::Running)
}

/// [`attach`]'s steps, with the state check as a parameter.
fn own_existing(
    t: &dyn Transport,
    host: &Host,
    id: &JobId,
    job_file: &Path,
    fence: &StopFence,
    on_owned: &dyn Fn(&Owned),
    require: Require,
) -> Result<Owned> {
    crate::errors::require_master(t, host)?;
    let mut lock = fence.lock()?;
    // Flattened to text: a probe that fails is exit 1 for an attach whatever
    // the cause (spec step 3), not the code its error would map to elsewhere.
    let state = match probe(t, host, id, ProbeFrom::StateOnly) {
        Ok(probed) => probed.state,
        // A keyed lease holds whatever its key names, an unproven state
        // included: a claim whose dispatch is still to come reads `unknown`,
        // and the hold keeps probing until it is told.
        Err(error)
            if require == Require::AnyState
                && matches!(
                    error.downcast_ref::<MuleError>(),
                    Some(MuleError::Unverifiable { .. })
                ) =>
        {
            State::Running
        }
        Err(error) => anyhow::bail!("cannot probe job {id}: {error:#}"),
    };
    let state = match state {
        State::Running => None,
        _ if require == Require::AnyState => None,
        State::Done(code) => Some(format!("done {code}")),
        State::Orphan => Some("orphan".to_string()),
        State::Missing => Some("missing".to_string()),
    };
    if let Some(state) = state {
        anyhow::bail!("job {id} is not running ({state})");
    }
    publish_job_file(job_file, id)?;
    let owned = Owned {
        id: id.clone(),
        fence_gen: lock.bump()?,
    };
    // Before the lock goes: once it is released an old stop child may run its
    // next attempt, and it must already find the new generation.
    on_owned(&owned);
    drop(lock);
    Ok(owned)
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
    owned: Owned,
    /// Where the stop-failure record goes, beside it.
    job_file: PathBuf,
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
        // The generation this carrier owns, not one read now: see `Owned`.
        let failed_file = stop_failed_path(&self.job_file, &self.owned.id);
        let retry = stop_retry(&self.owned, &failed_file, cleanup::stop_retry_for());
        let _ = cleanup::spawn_rm(
            self.config.as_deref(),
            &self.host.name,
            &self.owned.id,
            Some(&retry),
        );
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

/// How a carrier comes to own its job.
enum Begin<'a> {
    /// `run --lease [--key K]`: name and dispatch a new one, or hold the one
    /// holding K.
    Start(&'a Request<'a>, Option<&'a JobKey>),
    /// `run --lease --attach <id>`: take over one that is running.
    Attach(&'a JobId),
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
    run_cli_keyed(host, config, job_file, specs, request, None)
}

/// `mule run --lease --key K`: [`run_cli`] through [`start_keyed`].
pub fn run_cli_keyed(
    host: &Host,
    config: Option<PathBuf>,
    job_file: &Path,
    specs: Vec<Spec>,
    request: &Request,
    key: Option<&JobKey>,
) -> Result<i32> {
    carrier_cli(host, config, job_file, specs, Begin::Start(request, key))
}

/// `mule run --lease --attach <id>`: [`attach`], then the same hold and signal
/// path as a lease. Not a re-dispatch: nothing is started, `dispatching` stays
/// false, and the exit codes are the lease's.
pub fn attach_cli(
    host: &Host,
    config: Option<PathBuf>,
    job_file: &Path,
    specs: Vec<Spec>,
    id: &JobId,
) -> Result<i32> {
    carrier_cli(host, config, job_file, specs, Begin::Attach(id))
}

fn carrier_cli(
    host: &Host,
    config: Option<PathBuf>,
    job_file: &Path,
    specs: Vec<Spec>,
    begin: Begin,
) -> Result<i32> {
    let signals = Arc::new(Signals::install(&cleanup::STOP_SIGNALS)?);
    let dispatching = Arc::new(AtomicBool::new(false));
    let ssh = InFlight::new(Ssh);
    let calls = ssh.stopper();
    // The id is known only once `start` has generated it; until then a signal
    // has nothing to stop (nothing was dispatched) and just exits.
    let named: Arc<std::sync::Mutex<Option<Owned>>> = Arc::default();
    {
        let (host, specs, config) = (host.clone(), specs.clone(), config.clone());
        let job_file = job_file.to_path_buf();
        let (dispatching, named) = (Arc::clone(&dispatching), Arc::clone(&named));
        let calls = calls.clone();
        signals.on_signal(move |signal| {
            // Held to the exit when no id is named yet: `on_dispatch` takes the
            // same lock before the dispatch ssh, so a dispatch can never start
            // after this thread decided there was nothing to stop.
            let guard = named.lock();
            let owned = guard.as_ref().ok().and_then(|owned| (*owned).clone());
            if owned.is_some() {
                drop(guard);
            }
            let code = match owned {
                Some(owned) => on_signal(
                    &mut RealSignalSteps {
                        host,
                        owned,
                        job_file,
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
    let started = match begin {
        Begin::Start(request, key) => start_keyed(
            &ssh,
            host,
            job_file,
            &StopFence::dir(),
            request,
            key,
            &|owned| {
                if let Some(owned) = owned {
                    // `dispatching` is set under the lock, before the id is
                    // visible: once the signal thread can see an id it also
                    // sees the dispatch as in flight, so its `rm` waits for it
                    // rather than overtaking it (a no-op `rm`, then a leaked
                    // job).
                    if let Ok(mut named) = named.lock() {
                        dispatching.store(true, Ordering::SeqCst);
                        *named = Some(owned.clone());
                    }
                } else {
                    dispatching.store(false, Ordering::SeqCst);
                }
            },
            // The claim made nothing of the named id: forget it, so a later
            // failure spawns no `rm` for an id that never existed.
            &|| {
                if let Ok(mut named) = named.lock() {
                    *named = None;
                }
                dispatching.store(false, Ordering::SeqCst);
            },
            &mut stderr,
        ),
        // An attach that fails owns nothing (`named` is set only on success),
        // so the error path below spawns no stop for it.
        Begin::Attach(id) => attach(
            &ssh,
            host,
            id,
            job_file,
            &StopFence::for_job(id),
            // Under the mutex the signal thread reads. A signal that took it
            // first holds it to the exit and stops nothing: if that lands
            // after the bump, the job runs on and mu keeps its reservation
            // (spec step 5).
            &|owned| {
                if let Ok(mut named) = named.lock() {
                    *named = Some(owned.clone());
                }
            },
        ),
    };
    let id = match started {
        Ok(owned) => owned.id,
        Err(error) => {
            if signals.pending().is_some() {
                park();
            }
            if matches!(
                error.downcast_ref::<MuleError>(),
                Some(MuleError::NoMaster { .. })
            ) {
                // The lease's only exit 3. The id in the job file was never
                // dispatched, so the stop template is a no-op on it. (An
                // attach checks the master before touching anything.)
                let _ = writeln!(stderr, "mule: {error}");
                return Ok(EXIT_NO_MASTER);
            }
            // The dispatch may have landed before its reply was lost; the id is
            // in the job file either way, and `rm` of a never-made job is a
            // no-op. (A job-file write failure never gets here with an id.)
            if let Ok(Some(owned)) = named.lock().map(|owned| owned.clone()) {
                let _ = cleanup::spawn_rm(config.as_deref(), &host.name, &owned.id, None);
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
