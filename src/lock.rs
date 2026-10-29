//! Fair, per-host serialization for access to mule's private ssh channel.

use std::fs::{self, OpenOptions};
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};

/// How often a waiter re-checks whether its ticket is being served.
///
/// This is added to *every* handoff, so it is a direct tax on the whole queue,
/// not just on one waiter. Measured with 4 callers x 5 rounds x 50ms of work
/// (1s of serialised work, so a perfectly fair queue peaks near 200ms):
///
/// | poll | p50 | worst observed |
/// | --- | --- | --- |
/// | 20ms | ~275ms | **443ms**, and it flaked a 600ms bound 1 run in 15 |
/// | 5ms | ~237ms | 261ms across 6 runs |
///
/// 5ms costs a few more syscalls on a critical section that is sub-second by
/// construction, and buys back most of the granularity overhead.
const POLL_INTERVAL: Duration = Duration::from_millis(5);

/// How long to wait before telling the user the queue is not dead.
///
/// Never a timeout: aborting would reintroduce the failure the ticket lock
/// exists to remove.
const WARN_AFTER: Duration = Duration::from_secs(5);

/// Suppresses the "waiting for lock ticket" notice, process-wide.
///
/// `mule exec` stands in for `ssh host cmd`, whose caller parses stderr and
/// reads the command's own first line as the reason (SPEC § exec). A lock-wait
/// notice is mule talking, not the command, so exec turns it off. A process
/// flag rather than an argument because the notice is printed deep inside
/// `Transport::run`, which every verb reaches through the same provided method.
static QUIET: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

pub fn set_quiet(quiet: bool) {
    QUIET.store(quiet, std::sync::atomic::Ordering::Relaxed);
}

/// `~/.local/state/mule` (or under `$XDG_STATE_HOME`): mule's local state root.
///
/// Holds the ticket locks and the forward records; never a job index.
pub fn state_dir() -> PathBuf {
    let state = std::env::var_os("XDG_STATE_HOME")
        .map(PathBuf::from)
        .or_else(|| directories::BaseDirs::new().map(|dirs| dirs.home_dir().join(".local/state")))
        .unwrap_or_else(|| PathBuf::from(".local/state"));
    state.join("mule")
}

/// The state directory for one host's ticket lock.
pub fn lock_path(host: &str) -> PathBuf {
    state_dir().join(format!("{host}.lock"))
}

/// Run `f` while holding the fair lock for `host`.
pub fn with_lock<T>(host: &str, f: impl FnOnce() -> T) -> Result<T> {
    with_lock_at(&lock_path(host), f)
}

/// [`with_lock`] on an explicit lock directory.
///
/// Exposed so a test can hold or queue on the ticket of a mule process it
/// spawned: that process's `$XDG_STATE_HOME` is not this one's, and mutating
/// the environment of a multi-threaded test binary is a race.
#[doc(hidden)]
pub fn with_lock_at<T>(path: &Path, f: impl FnOnce() -> T) -> Result<T> {
    fs::create_dir_all(path)
        .with_context(|| format!("cannot create lock directory {}", path.display()))?;

    let ticket = with_counter_lock(path, || {
        let next = read_counters(path)?.next;
        publish(path, "next", next + 1)?;
        // Record that a live process is waiting on this ticket. Without it, a
        // caller that dies *before* its turn arrives leaves a gap nothing can
        // detect: `serving` reaches its number, no `holder` was ever written,
        // and every later caller queues behind a ticket that will never be
        // claimed. Reproduced -- it wedged the host indefinitely.
        publish(path, &waiter_name(next), std::process::id())
            .context("cannot record lock waiter")?;
        Ok(next)
    })?;

    wait_for_turn(path, ticket)?;
    let guard = HeldLock {
        path: path.to_path_buf(),
    };
    let result = f();
    drop(guard);
    Ok(result)
}

fn waiter_name(ticket: u64) -> String {
    format!("waiter.{ticket}")
}

fn waiter_file(path: &Path, ticket: u64) -> PathBuf {
    path.join(waiter_name(ticket))
}

fn wait_for_turn(path: &Path, ticket: u64) -> Result<()> {
    let started = Instant::now();
    let mut warned = false;

    loop {
        let acquired = with_counter_lock(path, || {
            let read = read_counters(path)?.serving;
            // Step over every consecutive dead ticket in this one pass, not
            // one per poll: after a SIGKILL storm `serving` can trail by
            // hundreds of abandoned tickets, and one step per 5ms poll made
            // the next caller sleep through all of them (400 measured at
            // 3.96s; the storm test's 10s recovery bound flaked under load).
            let mut serving = read;
            while serving < ticket && ticket_is_abandoned(path, serving)? {
                serving += 1;
            }
            if serving != read {
                publish(path, "serving", serving)?;
            }
            if serving == ticket {
                publish(path, "holder", std::process::id()).context("cannot record lock holder")?;
                // Our turn: we are the holder now, not a waiter.
                fs::remove_file(waiter_file(path, ticket)).ok();
                return Ok(true);
            }
            Ok(false)
        })?;

        if acquired {
            return Ok(());
        }

        if !warned
            && started.elapsed() >= WARN_AFTER
            && !QUIET.load(std::sync::atomic::Ordering::Relaxed)
        {
            // Outside the counter lock, so only a hint: a publication is a
            // rename, so this sees an old or a new value, never a torn one.
            let serving = read_parsed::<u64>(&path.join("serving"))
                .ok()
                .flatten()
                .unwrap_or(ticket);
            let holder = read_parsed::<u32>(&path.join("holder")).ok().flatten();
            eprintln!(
                "mule: waiting for lock ticket {ticket} ({} ahead, holder pid {})",
                ticket.saturating_sub(serving),
                holder.map_or_else(|| "unknown".into(), |pid| pid.to_string())
            );
            warned = true;
        }
        thread::sleep(POLL_INTERVAL);
    }
}

/// Whether no live process owns ticket `serving`, removing what it left behind.
/// Call inside the counter lock.
fn ticket_is_abandoned(path: &Path, serving: u64) -> Result<bool> {
    match process(&path.join("holder"))? {
        // A holder that died mid-work, or whose pid never made it to disk.
        Process::Dead => {
            fs::remove_file(path.join("holder")).ok();
            Ok(true)
        }
        Process::Live => Ok(false),
        // Nobody holds the lock, so whoever owns `serving` is either waiting
        // for it or gone. Deciding by pid rather than by a timeout keeps this
        // deterministic: a live waiter writes `holder` inside the same counter
        // lock the caller is inside now, so it cannot be mid-claim here.
        Process::Missing => {
            let waiter = waiter_file(path, serving);
            // No waiter file (handed out, then lost), a dead pid, or a torn
            // file: no live process holds this ticket.
            if process(&waiter)? == Process::Live {
                return Ok(false);
            }
            fs::remove_file(&waiter).ok();
            Ok(true)
        }
    }
}

struct HeldLock {
    path: PathBuf,
}

impl Drop for HeldLock {
    fn drop(&mut self) {
        let result = with_counter_lock(&self.path, || {
            let serving = read_counters(&self.path)?.serving;
            fs::remove_file(self.path.join("holder")).ok();
            publish(&self.path, "serving", serving + 1)
        });
        if let Err(error) = result {
            eprintln!("mule: could not release lock: {error:#}");
        }
    }
}

#[cfg(unix)]
fn with_counter_lock<T>(path: &Path, f: impl FnOnce() -> Result<T>) -> Result<T> {
    use std::os::fd::AsRawFd;

    unsafe extern "C" {
        fn flock(fd: i32, operation: i32) -> i32;
    }

    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path.join("counter.lock"))
        .context("cannot open counter lock")?;
    // The OS releases flock when a process dies. This avoids a stale sentinel,
    // while keeping the ticket read-modify-write atomic without a dependency.
    if unsafe { flock(file.as_raw_fd(), 2) } != 0 {
        return Err(std::io::Error::last_os_error()).context("cannot acquire counter lock");
    }
    let result = f();
    drop(file);
    result
}

#[cfg(not(unix))]
fn with_counter_lock<T>(path: &Path, f: impl FnOnce() -> Result<T>) -> Result<T> {
    let sentinel = path.join("counter.lock");
    loop {
        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&sentinel)
        {
            Ok(file) => {
                let result = f();
                drop(file);
                fs::remove_file(&sentinel).ok();
                return result;
            }
            Err(error) if error.kind() == ErrorKind::AlreadyExists => {
                thread::sleep(Duration::from_millis(1));
            }
            Err(error) => return Err(error).context("cannot acquire counter lock"),
        }
    }
}

/// The temp file every publication is written to before its rename.
///
/// One fixed name, because every publication happens inside the counter lock:
/// there is never a second writer to collide with, and a writer SIGKILLed
/// mid-write leaves one stale temp that the next publication truncates, rather
/// than one per death.
const PUBLISH_TMP: &str = "publish.tmp";

/// Replace lock file `name` with `value`, atomically. Call inside the counter lock.
///
/// `fs::write` truncates and then writes, so a mule SIGKILLed between the two
/// left a 0-byte `holder`, `waiter.N`, `next` or `serving` (measured on APFS),
/// and every later locked call on the host failed on it until a human deleted
/// the file: 1 close in 9 smoke runs, and 12 of 12 SIGKILL-storm trials within
/// 7-252 rounds. A rename publishes all of the value or none of it.
///
/// No fsync, deliberately. Atomicity against a dead *process* needs only the
/// rename; an fsync buys durability across power loss, which this state does
/// not need: after a reboot every recorded pid is dead, and a file a crash
/// left empty or torn reads as dead or is rebuilt (see [`read_parsed`],
/// [`read_counters`]). It cost every mule ssh inside the counter flock: plain
/// `fsync(2)` took an uncontended locked call from 9ms to 14ms, and
/// `File::sync_all` (`F_FULLFSYNC` on macOS) to 37ms.
fn publish(path: &Path, name: &str, value: impl std::fmt::Display) -> Result<()> {
    use std::io::Write;

    let tmp = path.join(PUBLISH_TMP);
    let target = path.join(name);
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&tmp)
        .with_context(|| format!("cannot write {}", tmp.display()))?;
    writeln!(file, "{value}").with_context(|| format!("cannot write {}", tmp.display()))?;
    drop(file);
    fs::rename(&tmp, &target).with_context(|| format!("cannot write {}", target.display()))
}

/// A lock file's value: `Ok(None)` when it is missing, empty or unparsable.
///
/// Empty or unparsable means torn: a writer died mid-write. Since publication
/// is a rename, only an older mule can have left one, and it heals here.
/// Reading it as absent is safe inside the counter lock because every writer
/// holds that lock too, so no *live* writer can be mid-publication when this
/// looks.
fn read_parsed<T: std::str::FromStr>(path: &Path) -> Result<Option<T>> {
    match fs::read_to_string(path) {
        Ok(value) => Ok(value.trim().parse().ok()),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error).with_context(|| format!("cannot read {}", path.display())),
    }
}

/// What a `holder` or `waiter.N` file says about its process.
#[derive(Debug, PartialEq, Eq)]
enum Process {
    /// No file.
    Missing,
    Live,
    /// A pid that is gone, or a torn file: either way no live process.
    Dead,
}

fn process(path: &Path) -> Result<Process> {
    if !path.exists() {
        return Ok(Process::Missing);
    }
    Ok(match read_parsed::<u32>(path)? {
        Some(pid) if pid_is_alive(pid) => Process::Live,
        // Missing between `exists` and the read is impossible: every remover
        // holds the counter lock, as the caller does.
        _ => Process::Dead,
    })
}

struct Counters {
    next: u64,
    serving: u64,
}

/// Read `next` and `serving`, rebuilding either one that is missing or torn.
/// Call inside the counter lock.
///
/// Neither may default to 0. A `next` of 0 re-issues tickets that are still
/// out, and a `serving` read too high grants a ticket while its predecessor
/// still holds the lock: both are two holders. A `serving` read too low only
/// costs one step-over pass, which clears every dead ticket at once. So each
/// is rebuilt from what the directory proves:
///
/// - `next` is past every ticket on disk: `max(waiter.N + 1, serving + 1 if a
///   holder file exists, else serving)`. Re-issuing a dead ticket's number is
///   harmless; only a live one matters, and a live ticket has a waiter file or
///   is the holder's, which is `serving`. With `serving` torn too, a live
///   holder still means `next` is at least 1: its ticket is below `next`.
/// - `serving` is the lowest live ticket -- the lowest live waiter, or `next`
///   if there is none -- minus one while a live holder still has the lock. The
///   holder's own ticket is not on disk, but it is below every live waiter, so
///   its release (`serving + 1`) is what grants the next one.
///
/// The rebuilt value is published, so the next reader finds a whole file. This
/// always recovers; no torn counter needs a human.
fn read_counters(path: &Path) -> Result<Counters> {
    let next = read_parsed::<u64>(&path.join("next"))?;
    let serving = read_parsed::<u64>(&path.join("serving"))?;
    if let (Some(next), Some(serving)) = (next, serving) {
        return Ok(Counters { next, serving });
    }

    let holder = process(&path.join("holder"))?;
    let mut waiters = Vec::new();
    for entry in fs::read_dir(path)
        .with_context(|| format!("cannot read lock directory {}", path.display()))?
    {
        let entry =
            entry.with_context(|| format!("cannot read lock directory {}", path.display()))?;
        if let Some(ticket) = entry
            .file_name()
            .to_str()
            .and_then(|name| name.strip_prefix("waiter."))
            .and_then(|ticket| ticket.parse::<u64>().ok())
        {
            waiters.push((ticket, process(&entry.path())? == Process::Live));
        }
    }

    let next = match next {
        Some(next) => next,
        None => {
            let past_holder = serving.map_or(0, |s| s + u64::from(holder != Process::Missing));
            let rebuilt = waiters
                .iter()
                .map(|&(ticket, _)| ticket + 1)
                .fold(past_holder, u64::max)
                // A live holder's ticket is below `next`, so `next` is at least 1.
                .max(u64::from(holder == Process::Live));
            publish(path, "next", rebuilt)?;
            rebuilt
        }
    };
    let serving = match serving {
        Some(serving) => serving,
        None => {
            let lowest = waiters
                .iter()
                .filter(|&&(_, live)| live)
                .map(|&(ticket, _)| ticket)
                .fold(next, u64::min);
            let rebuilt = if holder == Process::Live {
                lowest.saturating_sub(1)
            } else {
                lowest
            };
            publish(path, "serving", rebuilt)?;
            rebuilt
        }
    };
    Ok(Counters { next, serving })
}

#[cfg(unix)]
fn pid_is_alive(pid: u32) -> bool {
    unsafe extern "C" {
        fn kill(pid: i32, signal: i32) -> i32;
    }

    let Ok(pid) = i32::try_from(pid) else {
        return false;
    };
    // Signal 0 performs the existence/permission check without sending a signal.
    if unsafe { kill(pid, 0) } == 0 {
        return true;
    }
    std::io::Error::last_os_error().raw_os_error() != Some(3)
}

#[cfg(not(unix))]
fn pid_is_alive(_pid: u32) -> bool {
    true
}
