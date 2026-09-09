//! Fair, per-host serialization for access to coop's private ssh channel.

use std::fs::{self, OpenOptions};
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};

const POLL_INTERVAL: Duration = Duration::from_millis(20);
const WARN_AFTER: Duration = Duration::from_secs(5);

/// The state directory for one host's ticket lock.
pub fn lock_path(host: &str) -> PathBuf {
    let state = std::env::var_os("XDG_STATE_HOME")
        .map(PathBuf::from)
        .or_else(|| directories::BaseDirs::new().map(|dirs| dirs.home_dir().join(".local/state")))
        .unwrap_or_else(|| PathBuf::from(".local/state"));
    state.join("coop").join(format!("{host}.lock"))
}

/// Run `f` while holding the fair lock for `host`.
pub fn with_lock<T>(host: &str, f: impl FnOnce() -> T) -> Result<T> {
    let path = lock_path(host);
    fs::create_dir_all(&path)
        .with_context(|| format!("cannot create lock directory {}", path.display()))?;

    let ticket = with_counter_lock(&path, || {
        let next = read_counter(&path.join("next"))?;
        write_counter(&path.join("next"), next + 1)?;
        if !path.join("serving").exists() {
            write_counter(&path.join("serving"), 0)?;
        }
        // Record that a live process is waiting on this ticket. Without it, a
        // caller that dies *before* its turn arrives leaves a gap nothing can
        // detect: `serving` reaches its number, no `holder` was ever written,
        // and every later caller queues behind a ticket that will never be
        // claimed. Reproduced -- it wedged the host indefinitely.
        fs::write(
            waiter_file(&path, next),
            format!("{}\n", std::process::id()),
        )
        .context("cannot record lock waiter")?;
        Ok(next)
    })?;

    wait_for_turn(&path, ticket)?;
    let guard = HeldLock { path };
    let result = f();
    drop(guard);
    Ok(result)
}

fn waiter_file(path: &Path, ticket: u64) -> PathBuf {
    path.join(format!("waiter.{ticket}"))
}

fn wait_for_turn(path: &Path, ticket: u64) -> Result<()> {
    let started = Instant::now();
    let mut warned = false;

    loop {
        let acquired = with_counter_lock(path, || {
            let serving = read_counter(&path.join("serving"))?;
            if serving == ticket {
                fs::write(path.join("holder"), format!("{}\n", std::process::id()))
                    .context("cannot record lock holder")?;
                // Our turn: we are the holder now, not a waiter.
                fs::remove_file(waiter_file(path, ticket)).ok();
                return Ok(true);
            }

            if serving < ticket {
                match read_pid(&path.join("holder"))? {
                    // A holder that died mid-work. Step over it.
                    Some(pid) if !pid_is_alive(pid) => {
                        fs::remove_file(path.join("holder")).ok();
                        write_counter(&path.join("serving"), serving + 1)?;
                    }
                    Some(_) => {}
                    // Nobody holds the lock, so whoever owns `serving` is
                    // either waiting for it or gone. Deciding by pid rather
                    // than by a timeout keeps this deterministic: a live
                    // waiter writes `holder` inside the same counter lock we
                    // are inside now, so it cannot be mid-claim here.
                    None => {
                        let waiter = waiter_file(path, serving);
                        let abandoned = match read_pid(&waiter)? {
                            Some(pid) => !pid_is_alive(pid),
                            // No waiter file at all: handed out, then lost.
                            None => true,
                        };
                        if abandoned {
                            fs::remove_file(&waiter).ok();
                            write_counter(&path.join("serving"), serving + 1)?;
                        }
                    }
                }
            }
            Ok(false)
        })?;

        if acquired {
            return Ok(());
        }

        if !warned && started.elapsed() >= WARN_AFTER {
            let serving = read_counter(&path.join("serving")).unwrap_or(ticket);
            let holder = read_pid(&path.join("holder")).ok().flatten();
            eprintln!(
                "coop: waiting for lock ticket {ticket} ({} ahead, holder pid {})",
                ticket.saturating_sub(serving),
                holder.map_or_else(|| "unknown".into(), |pid| pid.to_string())
            );
            warned = true;
        }
        thread::sleep(POLL_INTERVAL);
    }
}

struct HeldLock {
    path: PathBuf,
}

impl Drop for HeldLock {
    fn drop(&mut self) {
        let result = with_counter_lock(&self.path, || {
            let serving = read_counter(&self.path.join("serving"))?;
            fs::remove_file(self.path.join("holder")).ok();
            write_counter(&self.path.join("serving"), serving + 1)
        });
        if let Err(error) = result {
            eprintln!("coop: could not release lock: {error:#}");
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

fn read_counter(path: &Path) -> Result<u64> {
    match fs::read_to_string(path) {
        Ok(value) => value
            .trim()
            .parse()
            .with_context(|| format!("invalid counter in {}", path.display())),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(0),
        Err(error) => Err(error).with_context(|| format!("cannot read {}", path.display())),
    }
}

fn write_counter(path: &Path, value: u64) -> Result<()> {
    fs::write(path, format!("{value}\n"))
        .with_context(|| format!("cannot write {}", path.display()))
}

fn read_pid(path: &Path) -> Result<Option<u32>> {
    match fs::read_to_string(path) {
        Ok(value) => {
            Ok(Some(value.trim().parse().with_context(|| {
                format!("invalid pid in {}", path.display())
            })?))
        }
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error).with_context(|| format!("cannot read {}", path.display())),
    }
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
