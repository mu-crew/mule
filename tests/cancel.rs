//! `exec --no-detach`: a cancelled exec stops its job inside its own process
//! group, and never detaches (mu plugins spec, D15).
//!
//! A plugin host cancels a call by signalling the plugin's process group and
//! kills the group after a grace; anything that outlives the group is a leak.
//! Plain exec's signal stop is a `setsid` child, outside the group by design,
//! so it is exactly such a leak. Layer 2: the binary, with an `ssh` that runs
//! the real scripts here. `tests/plugin_sshd.rs` repeats it over a capped
//! sshd.

mod common;

use std::time::{Duration, Instant};

use common::local::{ShimHost, stderr, tmux_available, wait_until};

/// Detached `rm` children naming `id`, by their argv.
fn stop_children(id: &str) -> Vec<String> {
    let out = std::process::Command::new("ps")
        .args(["-ax", "-o", "args="])
        .output()
        .unwrap();
    String::from_utf8_lossy(&out.stdout)
        .lines()
        .filter(|line| line.contains(" rm ") && line.contains(id) && line.contains("--retry-for"))
        .map(str::to_string)
        .collect()
}

fn job_ids(host: &ShimHost) -> Vec<String> {
    std::fs::read_dir(host.jobs())
        .map(|dir| {
            dir.filter_map(Result::ok)
                .map(|e| e.file_name().to_string_lossy().into_owned())
                .collect()
        })
        .unwrap_or_default()
}

fn spawn_in_group(host: &ShimHost, args: &[&str]) -> std::process::Child {
    use std::os::unix::process::CommandExt;
    host.command(args)
        .process_group(0)
        .spawn()
        .expect("mule failed to spawn")
}

fn signal_group(leader: u32, signal: i32) {
    unsafe { libc::kill(-(leader as i32), signal) };
}

/// Does any process in group `pgid` survive?
fn group_alive(pgid: u32) -> bool {
    unsafe { libc::kill(-(pgid as i32), 0) == 0 }
}

#[test]
fn a_cancelled_no_detach_exec_has_stopped_its_job_by_the_time_it_exits() {
    if !tmux_available() {
        eprintln!("skipping: tmux is not on PATH");
        return;
    }
    for (signal, code) in [
        (libc::SIGTERM, 143),
        (libc::SIGINT, 130),
        (libc::SIGHUP, 129),
    ] {
        let host = ShimHost::new("cancel-inline");
        let mut exec = spawn_in_group(&host, &["exec", "--no-detach", "--", "sleep 60"]);
        wait_until("the job to start", 10, || !job_ids(&host).is_empty());
        let id = job_ids(&host).remove(0);
        wait_until("the job's session", 10, || {
            host.tmux.has_session(&format!("mule-{id}"))
        });

        let sent = Instant::now();
        signal_group(exec.id(), signal);
        let status = exec.wait().unwrap();
        let took = sent.elapsed();

        assert_eq!(status.code(), Some(code), "signal {signal}");
        // Stopped before the exit, not by something that runs on after it:
        // no waiting here.
        assert!(job_ids(&host).is_empty(), "signal {signal}: state dir left");
        assert!(
            !host.tmux.has_session(&format!("mule-{id}")),
            "signal {signal}: session left"
        );
        assert!(
            stop_children(&id).is_empty(),
            "signal {signal}: a detached stop child exists: {:?}",
            stop_children(&id)
        );
        // Inside a plugin host's grace (mu's is 5s), with room for a loaded
        // machine.
        assert!(
            took < Duration::from_secs(5),
            "signal {signal}: took {took:?}"
        );
        // Nothing of the group outlives the exit for a kill to miss.
        signal_group(exec.id(), libc::SIGKILL);
        assert!(
            !group_alive(exec.id()),
            "signal {signal}: group outlived exec"
        );
    }
}

#[test]
fn a_no_detach_exec_still_answers_like_exec() {
    if !tmux_available() {
        eprintln!("skipping: tmux is not on PATH");
        return;
    }
    let host = ShimHost::new("cancel-normal");

    let out = host.mule(&[
        "exec",
        "--no-detach",
        "--",
        "echo out; echo err >&2; exit 3",
    ]);

    assert_eq!(out.status.code(), Some(3), "{}", stderr(&out));
    assert_eq!(String::from_utf8_lossy(&out.stdout), "out\n");
    assert_eq!(stderr(&out), "err\n");
    assert!(
        job_ids(&host).is_empty(),
        "removed as plain exec removes it"
    );
}

#[test]
fn plain_exec_still_hands_its_stop_to_a_detached_child() {
    // The default is unchanged: a terminal caller's pane hangup must not take
    // the stop with it, so the stop runs outside the group.
    if !tmux_available() {
        eprintln!("skipping: tmux is not on PATH");
        return;
    }
    let host = ShimHost::new("cancel-detached");
    let mut exec = spawn_in_group(&host, &["exec", "--", "sleep 60"]);
    wait_until("the job to start", 10, || !job_ids(&host).is_empty());
    let id = job_ids(&host).remove(0);
    // Hold the ticket so the detached child is still queued when we look.
    let lock = host.state.join("mule/shim.lock");
    let (held_tx, held_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
    let holder = std::thread::spawn(move || {
        mule::lock::with_lock_at(&lock, || {
            held_tx.send(()).unwrap();
            let _ = release_rx.recv_timeout(Duration::from_secs(20));
        })
        .unwrap();
    });
    held_rx.recv().unwrap();

    signal_group(exec.id(), libc::SIGTERM);
    assert_eq!(exec.wait().unwrap().code(), Some(143));

    wait_until("a detached stop child", 10, || {
        !stop_children(&id).is_empty()
    });
    release_tx.send(()).unwrap();
    holder.join().unwrap();
    wait_until("the detached stop to remove the job", 20, || {
        job_ids(&host).is_empty()
    });
}
