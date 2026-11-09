//! A job whose session is torn down from outside writes no `rc`: layer 2,
//! real wrapper, real tmux.
//!
//! Tearing a session down hangs up every process in its pane. tmux leaves
//! that to the kernel when it closes the pane's master; vtmux, a tmux
//! reimplementation, also sends `killpg(pane, SIGHUP)` itself. A waiting
//! shell (bash 3.2 as `sh`, and zsh) does not die the moment that lands: the
//! child dies with 129 and the shell can still run the wrapper's
//! `echo $? > rc` on its way out, or get only as far as the redirection's
//! truncation. Measured before the fix, eight rounds per cell: under vtmux
//! `mule kill` kept 137 in 3/8 rounds with `sh` and 0/8 with zsh (the rest
//! 129, or an empty `rc` that `kill` failed to parse), and an external
//! `kill-session` left an orphan in 2/8 and 1/8. Under tmux 16/16 of each,
//! and these tests pass on the unfixed wrapper there; they are red under
//! vtmux, which is the `tmux` on this machine's PATH.

mod common;

use std::time::Duration;

use common::local::{ShimHost, stderr, stdout, tmux_available, wait_until};

const ROUNDS: usize = 4;

fn shells() -> Vec<&'static str> {
    ["/bin/sh", "/bin/zsh"]
        .into_iter()
        .filter(|shell| std::path::Path::new(shell).exists())
        .collect()
}

/// Start `sleep 60` as a job whose tmux server runs panes under `shell`.
fn start(host: &ShimHost, shell: &str) -> String {
    let out = host
        .command(&["run", "--quiet", "sleep 60"])
        .env("SHELL", shell)
        .output()
        .unwrap();
    assert!(out.status.success(), "{}", stderr(&out));
    let id = stdout(&out);
    wait_until("the job's session", 10, || {
        host.tmux.has_session(&format!("mule-{id}"))
    });
    // Past the shell's startup, into the wait for its child.
    std::thread::sleep(Duration::from_millis(300));
    id
}

/// Tear the job's session down from outside mule, then wait for it to go.
fn tear_down(host: &ShimHost, id: &str, how: &str) {
    let target = format!("mule-{id}");
    let args: &[&str] = match how {
        "kill-session" => &["kill-session", "-t", &target],
        _ => &["kill-server"],
    };
    let _ = host.tmux.command(args).status().unwrap();
    wait_until("the session to go", 10, || !host.tmux.has_session(&target));
    // Long enough for a surviving shell to have written anything it would.
    std::thread::sleep(Duration::from_millis(300));
}

fn rc(host: &ShimHost, id: &str) -> Option<String> {
    std::fs::read_to_string(host.jobs().join(id).join("rc")).ok()
}

#[test]
fn a_torn_down_job_is_an_orphan_with_no_rc() {
    if !tmux_available() {
        eprintln!("skipping: tmux is not on PATH");
        return;
    }
    for shell in shells() {
        for how in ["kill-session", "kill-server"] {
            for round in 0..ROUNDS {
                let host = ShimHost::new("hup-orphan");
                let id = start(&host, shell);

                tear_down(&host, &id, how);

                // Orphan means "not mule's doing": no rc may appear, not 129
                // and not an empty file.
                let what = format!("{shell} {how} round {round}");
                assert_eq!(rc(&host, &id), None, "{what}");
                assert_eq!(stdout(&host.mule(&["poll", &id])), "orphan", "{what}");
            }
        }
    }
}

#[test]
fn mule_kill_keeps_its_137_through_the_hangup_it_causes() {
    // `kill` writes 137, then `kill-session`; the wrapper the hangup wakes
    // must not overwrite it, nor leave `kill` an empty file to read back.
    if !tmux_available() {
        eprintln!("skipping: tmux is not on PATH");
        return;
    }
    for shell in shells() {
        for round in 0..ROUNDS {
            let host = ShimHost::new("hup-kill");
            let id = start(&host, shell);

            let out = host
                .command(&["kill", &id])
                .env("SHELL", shell)
                .output()
                .unwrap();

            let what = format!("{shell} round {round}");
            assert!(out.status.success(), "{what}: {}", stderr(&out));
            assert!(
                stderr(&out).contains(&format!("killed {id}; now done 137")),
                "{what}: {}",
                stderr(&out)
            );
            std::thread::sleep(Duration::from_millis(300));
            assert_eq!(rc(&host, &id).as_deref(), Some("137\n"), "{what}");
        }
    }
}

#[test]
fn a_job_that_exits_129_on_its_own_still_records_it() {
    // The fix must not swallow a real 129: only a hangup of the wrapper itself
    // is not the job's exit.
    if !tmux_available() {
        eprintln!("skipping: tmux is not on PATH");
        return;
    }
    let host = ShimHost::new("hup-own");
    let id = stdout(&host.mule(&["run", "--quiet", "exit 129"]));
    wait_until("rc", 10, || rc(&host, &id).is_some_and(|rc| !rc.is_empty()));
    assert_eq!(rc(&host, &id).as_deref(), Some("129\n"));
    // A child killed by SIGHUP that is not the wrapper's: still the job's code.
    let id = stdout(&host.mule(&["run", "--quiet", "sh -c 'kill -HUP $$'"]));
    wait_until("rc", 10, || rc(&host, &id).is_some_and(|rc| !rc.is_empty()));
    assert_eq!(rc(&host, &id).as_deref(), Some("129\n"));
}
