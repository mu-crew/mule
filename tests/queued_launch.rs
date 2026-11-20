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
use std::time::{Duration, Instant};

use common::local::{ShimHost, stderr, tmux_available, wait_until};

/// A `waiter.N` file in the host's ticket lock: a caller queued behind the
/// holder.
fn someone_waits(lock: &Path) -> bool {
    std::fs::read_dir(lock).is_ok_and(|dir| {
        dir.filter_map(Result::ok)
            .any(|e| e.file_name().to_string_lossy().starts_with("waiter."))
    })
}

/// Hold the ticket, start `args` (whose command touches `marker`), SIGTERM it
/// while its dispatch is queued, then release the ticket.
fn signal_a_queued_dispatch(host: &ShimHost, args: &[&str], marker: &Path, ready: &Path) {
    let lock = host.state.join("mule/shim.lock");
    let (held_tx, held_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
    let holder = {
        let lock = lock.clone();
        std::thread::spawn(move || {
            mule::lock::with_lock_at(&lock, || {
                held_tx.send(()).unwrap();
                let _ = release_rx.recv_timeout(Duration::from_secs(30));
            })
            .unwrap();
        })
    };
    held_rx.recv().unwrap();

    let child = host.command(args).spawn().expect("mule failed to spawn");
    // Named and queued: `ready` (the lease's job file) is written before the
    // dispatch takes its ticket.
    wait_until("the dispatch to queue on the ticket", 20, || {
        ready.exists() && someone_waits(&lock)
    });
    unsafe { libc::kill(child.id() as i32, libc::SIGTERM) };
    // Long enough for a signal path that waits on the dispatch to be waiting.
    std::thread::sleep(Duration::from_millis(200));
    release_tx.send(()).unwrap();
    holder.join().unwrap();

    let out = child.wait_with_output().unwrap();
    assert_eq!(out.status.code(), Some(143), "{}", stderr(&out));
    // A dispatch admitted after the signal would touch it within milliseconds.
    let watch = Instant::now() + Duration::from_secs(2);
    while Instant::now() < watch {
        assert!(!marker.exists(), "the queued command started after SIGTERM");
        std::thread::sleep(Duration::from_millis(20));
    }
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
