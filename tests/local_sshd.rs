//! Test layer 3: the invariants, against a real session-capped `sshd`.
//!
//! These are the measurements the design rests on, re-run as assertions. The
//! original notes concluded they needed a real remote host; a non-root `sshd`
//! on loopback reproduces all of them with no token and no network.
//!
//! Skips with a printed reason when `sshd` is absent. Never silently.

mod common;

use std::path::PathBuf;
use std::time::{Duration, Instant};

use common::sshd::Sshd;

/// Owns a private tmux server name, and tears the server down on drop.
///
/// A guard rather than a call at the end of each test, because a failing
/// assertion returns early and would skip the cleanup -- so the suite would
/// litter precisely when it is being debugged.
struct Tmux<'a> {
    sshd: &'a Sshd,
    name: String,
}

impl<'a> Tmux<'a> {
    fn new(sshd: &'a Sshd, tag: &str) -> Self {
        Self {
            sshd,
            name: format!("coop-l3-{}-{tag}", std::process::id()),
        }
    }
}

impl Drop for Tmux<'_> {
    fn drop(&mut self) {
        // `kill-server` stops the server but leaves the socket FILE behind, so
        // killing alone litters one file per run in the user's tmux directory.
        // Ask tmux where the socket is rather than reconstructing it: on macOS
        // `$TMPDIR` is per-user while tmux uses `/tmp`, so guessing misses.
        // Ask tmux for the socket path BEFORE killing, and fall back to the
        // conventional location when the server is already gone -- which is the
        // common case here, because a tmux server exits with its last job, so
        // by cleanup time `display-message` fails and returns nothing while the
        // socket FILE remains. Asking a dead server leaked four files per run
        // with every test still passing.
        //
        // One quoted shell string, not separate argv entries: ssh joins its
        // arguments and the REMOTE shell re-parses them, so an unquoted
        // `#{socket_path}` arrives with its braces stripped and tmux prints the
        // window list instead of a path.
        let script = format!(
            "p=$(tmux -L {name} display-message -p '#{{socket_path}}' 2>/dev/null); \
             tmux -L {name} kill-server 2>/dev/null; \
             for c in \"$p\" \"${{TMUX_TMPDIR:-/tmp}}/tmux-$(id -u)/{name}\"; do \
               [ -n \"$c\" ] && [ -S \"$c\" ] && rm -f \"$c\"; \
             done; exit 0",
            name = self.name
        );
        let out = self.sshd.ssh(&[&script]);
        assert!(
            out.status.success(),
            "tmux cleanup failed for {}: {}",
            self.name,
            String::from_utf8_lossy(&out.stderr)
        );
    }
}

/// Remove the job state this test wrote. The daemon is loopback, so "remote"
/// state is this machine's real state directory.
fn clean_jobs(sshd: &Sshd, ids: &[String]) {
    for id in ids {
        let _ = sshd.ssh(&["rm", "-rf", &format!("$HOME/.local/state/coop/jobs/{id}")]);
    }
}

fn stdout(out: &std::process::Output) -> String {
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

#[test]
fn the_cap_refuses_a_second_session_on_one_connection() {
    require_sshd!();
    let sshd = Sshd::start();
    let socket = sshd.dir.join("probe.sock");
    sshd.open_master(&socket);

    // Occupy the single channel.
    let mut holder = std::process::Command::new(sshd.dir.join("ssh"))
        .arg("-S")
        .arg(&socket)
        .args(["-o", "BatchMode=yes", "127.0.0.1", "sleep", "5"])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    std::thread::sleep(Duration::from_millis(400));

    // A second command on the SAME socket is refused, and the refusal is the
    // misleading one: ssh falls back to a fresh connection and fails there, so
    // the surviving message is about credentials rather than sessions. This is
    // the error that sends people to the wrong place.
    let refused = sshd.ssh_via(&socket, &["echo", "second"]);
    let text = String::from_utf8_lossy(&refused.stderr);
    assert!(
        text.contains("Session open refused")
            || text.contains("session request failed")
            || text.contains("Permission denied"),
        "expected a session refusal, got: {text}"
    );

    let _ = holder.kill();
    let _ = holder.wait();
}

#[test]
fn a_second_connection_is_unaffected_by_a_starved_first() {
    require_sshd!();
    let sshd = Sshd::start();
    let first = sshd.dir.join("first.sock");
    let second = sshd.dir.join("second.sock");
    sshd.open_master(&first);
    sshd.open_master(&second);

    // Invariant 1, the whole basis of the design: the cap is per CONNECTION,
    // not per user. Starve one socket and the other must answer normally in the
    // same instant.
    let mut holder = std::process::Command::new(sshd.dir.join("ssh"))
        .arg("-S")
        .arg(&first)
        .args(["-o", "BatchMode=yes", "127.0.0.1", "sleep", "5"])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    std::thread::sleep(Duration::from_millis(400));

    for attempt in 0..3 {
        let out = sshd.ssh_via(&second, &["echo", "ok"]);
        assert!(
            out.status.success(),
            "attempt {attempt} on the second connection failed while the first \
             was starved: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        assert_eq!(String::from_utf8_lossy(&out.stdout).trim(), "ok");
    }

    // And symmetrically: `ssh -O check` on the starved socket still answers,
    // because it opens no session channel. That is why it is the one call coop
    // exempts from the ticket lock.
    assert!(
        sshd.master_alive(&first),
        "-O check must not need a channel"
    );

    let _ = holder.kill();
    let _ = holder.wait();
}

#[test]
fn concurrent_dispatches_all_succeed_through_the_gate() {
    require_sshd!();
    let sshd = Sshd::start();
    sshd.open_master(&sshd.socket);
    let tmux = Tmux::new(&sshd, "gate");
    let config = sshd.write_config(&tmux.name);

    // THE defining measurement. Five concurrent ungated calls on one capped
    // connection produced 1 success in 5; through coop's lock, 5 of 5.
    let handles: Vec<_> = (0..5)
        .map(|_| {
            let bin = PathBuf::from(env!("CARGO_BIN_EXE_coop"));
            let config = config.clone();
            let path = sshd.path_env();
            std::thread::spawn(move || {
                std::process::Command::new(bin)
                    .arg("--config")
                    .arg(&config)
                    .args(["run", "echo concurrent"])
                    .env("PATH", path)
                    .stdin(std::process::Stdio::null())
                    .output()
                    .expect("coop failed to spawn")
            })
        })
        .collect();

    let mut ids = Vec::new();
    for handle in handles {
        let out = handle.join().unwrap();
        assert!(
            out.status.success(),
            "a gated dispatch failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        let id = stdout(&out);
        assert_eq!(id.len(), 6, "expected a job id, got {id:?}");
        ids.push(id);
    }
    assert_eq!(ids.len(), 5, "5 of 5 dispatches must succeed");

    clean_jobs(&sshd, &ids);
}

#[test]
fn run_prints_next_steps_on_stderr_and_only_the_id_on_stdout() {
    require_sshd!();
    let sshd = Sshd::start();
    sshd.open_master(&sshd.socket);
    let tmux = Tmux::new(&sshd, "run-hint");
    let config = sshd.write_config(&tmux.name);

    let out = sshd.coop(&config, &["run", "sleep 30"]);
    assert!(out.status.success());
    let output_text = String::from_utf8_lossy(&out.stdout);
    let id = output_text.trim();
    assert_eq!(
        output_text.len(),
        7,
        "stdout must be six hex characters and a newline: {output_text:?}"
    );
    assert!(
        id.len() == 6
            && id
                .chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains(&format!("coop wait {id}")), "{stderr}");
    assert!(stderr.contains(&format!("coop tail {id}")), "{stderr}");

    let quiet = sshd.coop(&config, &["--quiet", "run", "true"]);
    assert!(quiet.status.success());
    assert!(
        quiet.stderr.is_empty(),
        "--quiet must suppress hints: {}",
        String::from_utf8_lossy(&quiet.stderr)
    );
    let quiet_id = stdout(&quiet);

    let _ = sshd.coop(&config, &["--quiet", "kill", id]);
    clean_jobs(&sshd, &[id.to_string(), quiet_id]);
}

#[test]
fn dispatch_does_not_wait_for_the_job() {
    require_sshd!();
    let sshd = Sshd::start();
    sshd.open_master(&sshd.socket);
    let tmux = Tmux::new(&sshd, "latency");
    let config = sshd.write_config(&tmux.name);

    // Invariant 3: dispatch is detached, so its cost must not scale with the
    // job. Originally measured as returning in 0s while a full suite ran.
    let started = Instant::now();
    let out = sshd.coop(&config, &["run", "sleep 30"]);
    let elapsed = started.elapsed();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let id = stdout(&out);

    // Generous, because this asserts a shape rather than a number: it must be
    // nowhere near the job's 30s. Real dispatch measured ~125ms.
    assert!(
        elapsed < Duration::from_secs(5),
        "dispatch took {elapsed:?}; it must not wait for the job"
    );

    // The job really is still running.
    let poll = sshd.coop(&config, &["poll", &id]);
    assert_eq!(stdout(&poll), "running");

    let _ = sshd.coop(&config, &["kill", &id]);
    clean_jobs(&sshd, &[id]);
}

#[test]
fn a_job_exceeding_its_remote_cap_is_killed_with_124() {
    require_sshd!();
    let sshd = Sshd::start();
    sshd.open_master(&sshd.socket);
    let tmux = Tmux::new(&sshd, "job-timeout");
    let config = sshd.write_config(&tmux.name);
    let mut text = std::fs::read_to_string(&config).unwrap();
    text.push_str("max_job_secs = 1\n");
    std::fs::write(&config, text).unwrap();

    let out = sshd.coop(&config, &["run", "trap '' TERM; sleep 30"]);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let id = stdout(&out);

    let deadline = Instant::now() + Duration::from_secs(10);
    while stdout(&sshd.coop(&config, &["poll", &id])) == "running" {
        assert!(Instant::now() < deadline, "timed-out job never finished");
        std::thread::sleep(Duration::from_millis(50));
    }
    assert_eq!(stdout(&sshd.coop(&config, &["poll", &id])), "124");

    let sessions = sshd.ssh(&[
        "tmux",
        "-L",
        &tmux.name,
        "list-sessions",
        "-F",
        "#{session_name}",
    ]);
    assert!(
        !String::from_utf8_lossy(&sessions.stdout).contains(&format!("coop-{id}")),
        "timed-out job left its tmux session alive"
    );

    clean_jobs(&sshd, &[id]);
}

#[test]
fn a_job_finishing_inside_its_remote_cap_keeps_its_rc_and_no_watchdog() {
    require_sshd!();
    let sshd = Sshd::start();
    sshd.open_master(&sshd.socket);
    let tmux = Tmux::new(&sshd, "job-fast");
    let config = sshd.write_config(&tmux.name);

    // The cap must be long enough that DISPATCH cannot outlast it. At
    // `--max-secs 2` this test failed intermittently inside its own file with
    // `orphan` instead of rc 7: under a parallel suite the round trip alone
    // exceeded two seconds, so the watchdog killed the job before `exit 7`
    // ever ran. That is the machine, not the watchdog -- the property here is
    // "a job finishing inside its cap is untouched", and a cap the harness
    // itself can breach tests the harness instead.
    const CAP_SECS: u64 = 5;
    let out = sshd.coop(
        &config,
        &["run", "--max-secs", &CAP_SECS.to_string(), "exit 7"],
    );
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let id = stdout(&out);

    let deadline = Instant::now() + Duration::from_secs(10);
    while stdout(&sshd.coop(&config, &["poll", &id])) == "running" {
        assert!(Instant::now() < deadline, "fast job never finished");
        std::thread::sleep(Duration::from_millis(50));
    }
    assert_eq!(stdout(&sshd.coop(&config, &["poll", &id])), "7");

    // Now outlive the cap. This is the assertion the test exists for, and it
    // cannot be replaced by checking that the watchdog session is gone: the
    // watchdog exits on its own once `sleep` returns, so its absence AFTER the
    // cap is equally true whether or not cancellation works. Verified by
    // deleting the `kill-session` cancel from dispatch_script -- an absence
    // check still passed, this one is what fails.
    //
    // Deliberately a short cap despite the dispatch race above: the wait has
    // to outlast the cap, so a 20s cap would mean a 20s test. 5s is the
    // smallest value that comfortably clears a loaded dispatch.
    std::thread::sleep(Duration::from_secs(CAP_SECS + 2));
    assert_eq!(
        stdout(&sshd.coop(&config, &["poll", &id])),
        "7",
        "the watchdog fired after the command had already finished and \
         overwrote its rc"
    );
    let sessions = sshd.ssh(&[
        "tmux",
        "-L",
        &tmux.name,
        "list-sessions",
        "-F",
        "#{session_name}",
    ]);
    assert!(
        !String::from_utf8_lossy(&sessions.stdout).contains(&format!("coop-{id}")),
        "fast job left its tmux session or watchdog alive"
    );

    clean_jobs(&sshd, &[id]);
}

#[test]
fn ls_keeps_multiline_commands_on_one_row_without_shortening_json() {
    require_sshd!();
    let sshd = Sshd::start();
    sshd.open_master(&sshd.socket);
    let tmux = Tmux::new(&sshd, "multiline-ls");
    let config = sshd.write_config(&tmux.name);
    let command = "python3 -c \"print('ok')\n# this deliberately long comment makes the human listing truncate rather than wrap across the terminal\n#\ttabbed\"";

    let run = sshd.coop(&config, &["run", command]);
    assert!(
        run.status.success(),
        "{}",
        String::from_utf8_lossy(&run.stderr)
    );
    let id = stdout(&run);

    let table = stdout(&sshd.coop(&config, &["ls", "--all"]));
    let row = table
        .lines()
        .find(|line| line.starts_with(&id))
        .expect("dispatched job must appear in ls");
    assert!(row.contains("python3 -c \"print('ok') # this deliberately"));
    assert!(
        row.ends_with('…'),
        "long command must have a visible marker: {row}"
    );
    assert_eq!(
        table.lines().filter(|line| line.contains("tabbed")).count(),
        0,
        "embedded whitespace must not create a continuation row: {table}"
    );

    let json = stdout(&sshd.coop(&config, &["ls", "--all", "--json"]));
    assert!(
        json.contains(
            "\"cmd\":\"python3 -c \\\"print('ok')\\n# this deliberately long comment makes the human listing truncate rather than wrap across the terminal\\n#\\ttabbed\\\"\""
        ),
        "JSON must preserve the complete command byte-for-byte: {json}"
    );

    clean_jobs(&sshd, &[id]);
}

#[test]
fn a_job_survives_the_loss_of_its_tmux_server() {
    require_sshd!();
    let sshd = Sshd::start();
    sshd.open_master(&sshd.socket);
    let tmux = Tmux::new(&sshd, "durable");
    let config = sshd.write_config(&tmux.name);

    let out = sshd.coop(&config, &["run", "echo durable; exit 9"]);
    let id = stdout(&out);

    // Wait for completion, then destroy the tmux server entirely. `rc` and
    // `log` are files, so the record must outlive the process container.
    let deadline = Instant::now() + Duration::from_secs(10);
    while stdout(&sshd.coop(&config, &["poll", &id])) == "running" {
        assert!(Instant::now() < deadline, "job never finished");
        std::thread::sleep(Duration::from_millis(50));
    }
    drop(tmux); // destroy the server on purpose: the record must outlive it

    assert_eq!(stdout(&sshd.coop(&config, &["poll", &id])), "9");
    assert_eq!(stdout(&sshd.coop(&config, &["tail", &id])), "durable");
    let wait = sshd.coop(&config, &["wait", &id]);
    assert_eq!(wait.status.code(), Some(9), "wait returns the job's code");
    assert!(wait.stdout.is_empty(), "wait must keep stdout clean");
    let hint = String::from_utf8_lossy(&wait.stderr);
    assert!(hint.contains(&format!("coop tail {id}")), "{hint}");
    assert!(hint.contains(&format!("coop rm {id}")), "{hint}");

    clean_jobs(&sshd, &[id]);
}

#[test]
fn the_private_tmux_server_is_invisible_to_the_default_one() {
    require_sshd!();
    let sshd = Sshd::start();
    sshd.open_master(&sshd.socket);
    let tmux = Tmux::new(&sshd, "private");
    let config = sshd.write_config(&tmux.name);

    let id = stdout(&sshd.coop(&config, &["run", "sleep 30"]));

    // Invariant 2. `tmux ls` with no -L talks to the user's own server, which
    // must never show coop's sessions.
    let default = sshd.ssh(&["tmux", "ls"]);
    let listing = format!(
        "{}{}",
        String::from_utf8_lossy(&default.stdout),
        String::from_utf8_lossy(&default.stderr)
    );
    assert!(
        !listing.contains(&format!("coop-{id}")),
        "coop's session leaked into the default tmux server: {listing}"
    );

    // It is present on coop's own server, so the check above is meaningful.
    let private = sshd.ssh(&["tmux", "-L", &tmux.name, "ls"]);
    assert!(
        String::from_utf8_lossy(&private.stdout).contains(&format!("coop-{id}")),
        "expected the session on coop's private server"
    );

    let _ = sshd.coop(&config, &["kill", &id]);
    clean_jobs(&sshd, &[id]);
}

#[test]
fn a_missing_master_exits_three_with_the_recovery_command() {
    require_sshd!();
    let sshd = Sshd::start();
    // Deliberately no master.
    let tmux = Tmux::new(&sshd, "nomaster");
    let config = sshd.write_config(&tmux.name);

    for verb in [
        vec!["run", "echo hi"],
        vec!["poll", "abc123"],
        vec!["wait", "abc123"],
        vec!["tail", "abc123"],
        vec!["kill", "abc123"],
        vec!["rm", "abc123"],
    ] {
        let out = sshd.coop(&config, &verb);
        assert_eq!(
            out.status.code(),
            Some(3),
            "{verb:?} must exit 3 without a master"
        );
        let text = String::from_utf8_lossy(&out.stderr);
        assert!(text.contains("ssh -MNf"), "{verb:?}: {text}");
        assert!(
            text.contains("tap a hardware key"),
            "{verb:?} must say a human is needed: {text}"
        );
        assert!(
            out.stdout.is_empty(),
            "{verb:?} must keep stdout clean for scripting"
        );
    }

    // The two exceptions, asserted rather than trusted: "which of my hosts can
    // I use right now" is the question these verbs answer, so a down master is
    // their ANSWER, not their failure. Exit 3 here would make `ls` useless in
    // exactly the situation a caller reaches for it -- and both were covered
    // only over `Fake`, which has no process and so no exit status to check.
    for verb in [vec!["ls"], vec!["host", "list"]] {
        let out = sshd.coop(&config, &verb);
        assert_eq!(
            out.status.code(),
            Some(0),
            "{verb:?} reports a down master rather than failing on it"
        );
        let text = String::from_utf8_lossy(&out.stderr);
        assert!(
            text.contains("ssh -MNf"),
            "{verb:?} must still name the fix: {text}"
        );
    }
}
