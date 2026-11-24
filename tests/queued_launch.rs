//! A signal that lands while a named dispatch is still waiting for its ticket
//! means the job never starts: layer 2, the binary, with an `ssh` that runs
//! the real scripts here.
//!
//! The signal path used to wait for any dispatch it had named, admitted to the
//! ssh gate or not, so one still queued on the ticket ran after the SIGTERM
//! and its `rm` came after. That `rm` removes the job, but not what the
//! command did meanwhile. Refusing the queued dispatch costs only a no-op
//! `rm` of a job that was never made.

mod common;

use std::path::Path;
use std::time::Duration;

use common::local::{ShimHost, stderr, tmux_available, wait_until};

/// How many callers are queued behind the holder of the host's ticket lock:
/// one `waiter.N` file each.
fn waiters(lock: &Path) -> usize {
    std::fs::read_dir(lock).map_or(0, |dir| {
        dir.filter_map(Result::ok)
            .filter(|e| e.file_name().to_string_lossy().starts_with("waiter."))
            .count()
    })
}

/// Hold the ticket, start `args` (whose command touches `marker`), SIGTERM it
/// while its dispatch is queued, then release the ticket once the signal path
/// has closed the gate.
///
/// Ordered by events, not a pause: a fixed 200ms before the release let a
/// paused signal thread lose to the queued dispatch even when correct. The
/// signal path closes the gate before it issues its `rm`, and that `rm`
/// queues on the same ticket, so a second waiter is the gate seen closed.
/// A signal path that waits on the queued dispatch never queues its `rm`
/// while the ticket is held, and the watchdog fails it.
///
/// That leaves one regression this cannot see: a wait bounded by the 3s
/// grace (the original bug) that then closes the gate is, in this order,
/// indistinguishable from a slow signal thread; telling them apart takes a
/// stopwatch, which is what this replaced.
fn signal_a_queued_dispatch(host: &ShimHost, args: &[&str], marker: &Path, ready: &Path) {
    let lock = host.state.join("mule/shim.lock");
    let (held_tx, held_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
    let holder = {
        let lock = lock.clone();
        std::thread::spawn(move || {
            mule::lock::with_lock_at(&lock, || {
                held_tx.send(()).unwrap();
                let _ = release_rx.recv_timeout(Duration::from_secs(60));
            })
            .unwrap();
        })
    };
    held_rx.recv().unwrap();

    let child = host.command(args).spawn().expect("mule failed to spawn");
    // Named and queued: `ready` (the lease's job file) is written before the
    // dispatch takes its ticket.
    wait_until("the dispatch to queue on the ticket", 20, || {
        ready.exists() && waiters(&lock) == 1
    });
    unsafe { libc::kill(child.id() as i32, libc::SIGTERM) };
    // A watchdog, not the oracle: only a signal path that waits on the queued
    // dispatch takes anywhere near this long.
    wait_until(
        "the signal path's rm to queue behind the dispatch",
        30,
        || waiters(&lock) >= 2,
    );
    release_tx.send(()).unwrap();
    holder.join().unwrap();

    let out = child.wait_with_output().unwrap();
    assert_eq!(out.status.code(), Some(143), "{}", stderr(&out));
    // The stop's `rm` reached the shim, and it queued behind the dispatch:
    // had the dispatch been admitted, its script would be in the log first.
    wait_until("the stop's rm to run", 30, || {
        host.ssh_calls().iter().any(|c| c.contains("kill-session"))
    });
    let calls = host.ssh_calls();
    assert!(
        !calls.iter().any(|c| c.contains("new-session")),
        "the queued dispatch was attempted after SIGTERM: {calls:#?}"
    );
    assert!(!marker.exists(), "the queued command started after SIGTERM");
}

#[test]
fn a_lease_signalled_while_its_dispatch_queues_never_starts_the_job() {
    if !tmux_available() {
        eprintln!("skipping: tmux is not on PATH");
        return;
    }
    let host = ShimHost::new("queued-lease");
    let marker = host.dir.join("started");
    let job_file = host.dir.join("w.job");
    let cmd = format!("touch '{}'; sleep 60", marker.display());
    signal_a_queued_dispatch(
        &host,
        &[
            "run",
            "--lease",
            "--job-file",
            job_file.to_str().unwrap(),
            "--",
            &cmd,
        ],
        &marker,
        &job_file,
    );
}

#[test]
fn a_no_detach_exec_signalled_while_its_dispatch_queues_never_starts_the_job() {
    if !tmux_available() {
        eprintln!("skipping: tmux is not on PATH");
        return;
    }
    let host = ShimHost::new("queued-exec");
    let marker = host.dir.join("started");
    let cmd = format!("touch '{}'; sleep 60", marker.display());
    // exec names its id before dispatch but writes nothing locally: the
    // config file stands in for "ready", so only the waiter is waited for.
    let config = host.config.clone();
    signal_a_queued_dispatch(
        &host,
        &["exec", "--no-detach", "--", &cmd],
        &marker,
        &config,
    );
}
