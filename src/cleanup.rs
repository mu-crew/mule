//! What a foreground mule process does when it is told to die.
//!
//! `mule exec` (and, later, the lease) is a foreground waiter for a detached
//! job. When its terminal goes away or the caller sends TERM, the job must not
//! outlive it -- but the process cannot do the stop itself: `rm` queues on the
//! ticket lock, whose wait has no bound of its own, and may re-queue after a
//! refused session. So the stop is handed to a **detached child**
//! (this binary, `rm <id>`) and the waiter exits at once. The child queues like
//! any caller, and a dead waiter's ticket is skipped by `wait_for_turn`.
//!
//! Two pieces live here because both users need both:
//!
//! - [`Signals`]: a flag-only handler plus a self-pipe, so a sleep can be
//!   interrupted and nothing but an atomic store ever runs in signal context.
//! - [`spawn_rm`]: the detached stop.

use std::io;
use std::os::fd::AsRawFd;
use std::os::unix::net::UnixStream;
use std::os::unix::process::CommandExt;
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use crate::wrapper::JobId;

/// The signals a foreground waiter treats as "stop the job and go".
pub const STOP_SIGNALS: [i32; 3] = [libc::SIGHUP, libc::SIGINT, libc::SIGTERM];

/// The conventional shell exit status for death by `signal`.
pub fn exit_status_for(signal: i32) -> i32 {
    128 + signal
}

/// Signals seen so far, and a pipe that wakes a sleeper when one arrives.
///
/// The handler does two async-signal-safe things: store the signal number and
/// write a byte to a pipe (signal-hook's contract). Everything else -- spawning
/// the cleanup, exiting -- happens on an ordinary thread or the main loop.
pub struct Signals {
    flag: Arc<AtomicUsize>,
    wake: UnixStream,
}

impl Signals {
    /// Install handlers for `signals`. Replaces the default (terminate) action,
    /// so the caller must act on [`Signals::pending`] or the process becomes
    /// unkillable by those signals.
    pub fn install(signals: &[i32]) -> io::Result<Self> {
        let (wake, write) = UnixStream::pair()?;
        // Non-blocking on both ends: a handler must never block on a full
        // pipe, and `wait` drains without blocking.
        wake.set_nonblocking(true)?;
        write.set_nonblocking(true)?;
        let flag = Arc::new(AtomicUsize::new(0));
        for &signal in signals {
            signal_hook::flag::register_usize(signal, Arc::clone(&flag), signal as usize)?;
            signal_hook::low_level::pipe::register(signal, write.try_clone()?)?;
        }
        Ok(Self { flag, wake })
    }

    /// The most recent signal received, if any. Sticky once set.
    pub fn pending(&self) -> Option<i32> {
        match self.flag.load(Ordering::SeqCst) {
            0 => None,
            signal => Some(signal as i32),
        }
    }

    /// Sleep up to `timeout` (forever for `None`), returning early when a
    /// signal arrives. This is what makes a hold loop's sleep interruptible.
    pub fn wait(&self, timeout: Option<Duration>) -> Option<i32> {
        if let Some(signal) = self.pending() {
            return Some(signal);
        }
        let millis = timeout.map_or(-1, |t| t.as_millis().min(i32::MAX as u128) as i32);
        let mut fds = libc::pollfd {
            fd: self.wake.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        };
        // EINTR and spurious wakeups both just fall through to the flag check.
        unsafe { libc::poll(&mut fds, 1, millis) };
        let mut sink = [0u8; 64];
        while io::Read::read(&mut &self.wake, &mut sink).is_ok_and(|n| n > 0) {}
        self.pending()
    }

    /// Run `on_signal` on a thread when the first signal arrives.
    ///
    /// This is how a waiter abandons an in-flight ssh call: the main thread may
    /// be blocked in a probe or in the ticket queue, and a flag it only checks
    /// between calls would wait for all of that. The closure is expected to end
    /// in `process::exit`.
    pub fn on_signal(self: &Arc<Self>, on_signal: impl FnOnce(i32) + Send + 'static) {
        let signals = Arc::clone(self);
        std::thread::spawn(move || {
            loop {
                // Bounded, not forever: the main thread's own `wait` drains the
                // same pipe, and if it wins the byte this thread's poll would
                // sleep through the signal. The flag is what is trusted; the
                // pipe is only a wakeup, so a short timeout closes the race.
                if let Some(signal) = signals.wait(Some(Duration::from_millis(100))) {
                    on_signal(signal);
                    return;
                }
            }
        });
    }
}

/// Point fds 1 and 2 at `/dev/null`.
///
/// After SIGHUP the pane's tty is gone, and Rust's `print!`/`eprint!` panic on
/// the resulting EIO -- turning an orderly stop into a panic that skips the
/// cleanup. A closed pipe does the same with EPIPE. Redirecting first makes
/// every later write succeed into the void.
pub fn redirect_stdio_to_null() {
    let Ok(null) = std::fs::OpenOptions::new().write(true).open("/dev/null") else {
        return;
    };
    // Failure leaves the old fd in place; there is nothing better to do.
    unsafe {
        libc::dup2(null.as_raw_fd(), 1);
        libc::dup2(null.as_raw_fd(), 2);
    }
}

/// The detached stop: this mule, this config, `rm <id>`.
///
/// - `exe` is `current_exe()`, never `mule` from PATH: a worker or a test runs a
///   freshly built binary, and PATH may hold another one or none.
/// - `config` is the effective `--config` when one was given. Without it the
///   child would fall back to -- and, on a bare home, seed -- the default
///   config, which is not the file the parent used.
/// - `--quiet`: nothing here is read by anyone, and stdio is `/dev/null`.
pub fn rm_command(exe: &Path, config: Option<&Path>, host: &str, id: &JobId) -> Command {
    let mut command = Command::new(exe);
    if let Some(config) = config {
        // The child inherits the cwd, but a relative path is one `chdir` away
        // from naming a different file; make it absolute while it is true.
        let config = std::path::absolute(config).unwrap_or_else(|_| config.to_path_buf());
        command.arg("--config").arg(config);
    }
    command
        .args(["--quiet", "rm", "--host", host])
        .arg(id.as_str())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    command
}

/// Spawn `command` in its own session and do not wait for it.
///
/// `setsid()` via `pre_exec`: a pane that dies sends SIGHUP to its whole
/// process group, and the stop must survive it. macOS ships no `setsid`
/// executable, so shelling out to one is not an option.
pub fn spawn_detached(mut command: Command) -> io::Result<()> {
    // SAFETY: only async-signal-safe `setsid` runs between fork and exec.
    unsafe {
        command.pre_exec(|| {
            if libc::setsid() == -1 {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        });
    }
    // Dropping the Child leaves it running; the parent is about to exit and
    // init reaps it.
    command.spawn().map(drop)
}

/// Stop `id` from a signal path: build the command and detach it.
pub fn spawn_rm(config: Option<&Path>, host: &str, id: &JobId) -> io::Result<()> {
    spawn_detached(rm_command(&std::env::current_exe()?, config, host, id))
}
