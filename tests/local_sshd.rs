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
            name: format!("mule-l3-{}-{tag}", std::process::id()),
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
        let _ = sshd.ssh(&["rm", "-rf", &format!("$XDG_STATE_HOME/mule/jobs/{id}")]);
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
    let _master = sshd.open_master(&socket);

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
fn mule_classifies_a_refused_session_instead_of_raw_ssh_stderr() {
    require_sshd!();
    let sshd = Sshd::start();
    let _master = sshd.open_master(&sshd.socket);
    let tmux = Tmux::new(&sshd, "busy");
    let config = sshd.write_config(&tmux.name);

    // Occupy the single MaxSessions slot outside mule's lock. The next mule
    // verb still takes the lock, then ssh, and must classify the refusal
    // rather than dump Permission denied (keyboard-interactive).
    let mut holder = std::process::Command::new(sshd.dir.join("ssh"))
        .arg("-S")
        .arg(&sshd.socket)
        .args(["-o", "BatchMode=yes", "127.0.0.1", "sleep", "8"])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    std::thread::sleep(Duration::from_millis(400));

    let out = sshd.mule(&config, &["poll", "abc123"]);
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(
        err.contains("one slot is held") || err.contains("master is down"),
        "must surface SessionChannelBusy, not raw ssh: {err}"
    );
    assert!(
        !err.contains("Permission denied") || err.contains("slot"),
        "must not dump unclassified credentials-looking stderr alone: {err}"
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
    let _first_master = sshd.open_master(&first);
    let _second_master = sshd.open_master(&second);

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
    // because it opens no session channel. That is why it is the one call mule
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
    let _master = sshd.open_master(&sshd.socket);
    let tmux = Tmux::new(&sshd, "gate");
    let config = sshd.write_config(&tmux.name);

    // THE defining measurement. Five concurrent ungated calls on one capped
    // connection produced 1 success in 5; through mule's lock, 5 of 5.
    let handles: Vec<_> = (0..5)
        .map(|_| {
            let bin = PathBuf::from(env!("CARGO_BIN_EXE_mule"));
            let config = config.clone();
            let path = sshd.path_env();
            let state = sshd.state_root.clone();
            std::thread::spawn(move || {
                std::process::Command::new(bin)
                    .arg("--config")
                    .arg(&config)
                    .args(["run", "echo concurrent"])
                    .env("PATH", path)
                    .env("XDG_STATE_HOME", &state)
                    .stdin(std::process::Stdio::null())
                    .output()
                    .expect("mule failed to spawn")
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
    let _master = sshd.open_master(&sshd.socket);
    let tmux = Tmux::new(&sshd, "run-hint");
    let config = sshd.write_config(&tmux.name);

    let out = sshd.mule(&config, &["run", "sleep 30"]);
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
    assert!(stderr.contains(&format!("mule wait {id}")), "{stderr}");
    assert!(stderr.contains(&format!("mule tail {id}")), "{stderr}");

    let quiet = sshd.mule(&config, &["--quiet", "run", "true"]);
    assert!(quiet.status.success());
    assert!(
        quiet.stderr.is_empty(),
        "--quiet must suppress hints: {}",
        String::from_utf8_lossy(&quiet.stderr)
    );
    let quiet_id = stdout(&quiet);

    let _ = sshd.mule(&config, &["--quiet", "kill", id]);
    clean_jobs(&sshd, &[id.to_string(), quiet_id]);
}

#[test]
fn tui_dispatch_hints_are_pasteable_with_a_hostile_forwarded_workstream() {
    require_sshd!();
    let sshd = Sshd::start();
    let _master = sshd.open_master(&sshd.socket);
    let tmux = Tmux::new(&sshd, "tui-hint-quote");
    let config = sshd.write_config(&tmux.name);
    let sentinel = sshd.dir.join("hint-expanded-workstream");
    let workstream = format!("crew '$(touch {})'", sentinel.display());

    let out = std::process::Command::new(env!("CARGO_BIN_EXE_mule"))
        .arg("--config")
        .arg(&config)
        .args(["run", "--tui", "sleep 30"])
        .env("PATH", sshd.path_env())
        .env("XDG_STATE_HOME", &sshd.state_root)
        .env("MU_WORKSTREAM", &workstream)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let id = stdout(&out);
    let hints = String::from_utf8_lossy(&out.stderr);
    assert!(
        hints.contains(&format!("TUI job {id} is interactive")),
        "{hints}"
    );
    assert!(hints.contains(&format!("mule tail {id}")), "{hints}");
    assert!(hints.contains(&format!("# select mule-{id}")), "{hints}");
    assert!(hints.contains("--host '127.0.0.1'"), "{hints}");
    assert!(
        hints.contains(&format!(
            "-w 'crew '\\''$(touch {})'\\'''",
            sentinel.display()
        )),
        "{hints}"
    );
    assert!(hints.contains(&format!("--agent mule-{id})\"")), "{hints}");
    assert!(hints.contains(&format!("mule kill --rm {id}")), "{hints}");
    assert!(
        !sentinel.exists(),
        "rendering the hint executed workstream text"
    );

    let quiet = std::process::Command::new(env!("CARGO_BIN_EXE_mule"))
        .arg("--config")
        .arg(&config)
        .args(["--quiet", "run", "--tui", "true"])
        .env("PATH", sshd.path_env())
        .env("XDG_STATE_HOME", &sshd.state_root)
        .env("MU_WORKSTREAM", &workstream)
        .output()
        .unwrap();
    assert!(quiet.status.success());
    assert!(quiet.stderr.is_empty(), "--quiet must suppress TUI hints");

    let _ = sshd.mule(&config, &["--quiet", "kill", "--rm", &id]);
    clean_jobs(&sshd, &[stdout(&quiet)]);
}

#[test]
fn suspicious_dispatch_warns_on_stderr_without_changing_the_id() {
    require_sshd!();
    let sshd = Sshd::start();
    let _master = sshd.open_master(&sshd.socket);
    let tmux = Tmux::new(&sshd, "dispatch-warning");
    let config = sshd.write_config(&tmux.name);

    let out = sshd.mule(&config, &["run", "printf ok | tail -1"]);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let id = stdout(&out);
    assert!(
        id.len() == 6
            && id
                .chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()),
        "stdout must contain only the job id: {:?}",
        String::from_utf8_lossy(&out.stdout)
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("final head/tail pipeline"), "{stderr}");
    // The advice must name THIS job, not a `<id>` placeholder. The warning
    // used to print before dispatch, at the one moment no real id existed --
    // so it reported a problem the reader could not act on, while the job ran
    // anyway. Every suggested command has to be pasteable.
    assert!(
        stderr.contains(&format!("mule tail {id} -n 3")),
        "the hint must name the real id: {stderr}"
    );
    assert!(
        stderr.contains(&format!("mule kill --rm {id}")),
        "a warning about a running job must say how to end it: {stderr}"
    );
    assert!(
        !stderr.contains("<id>"),
        "no placeholder ids in a message about a job that exists: {stderr}"
    );

    // `--quiet` drops the HINTS and keeps the WARNING -- see
    // quiet_drops_hints_but_keeps_warnings for the full contract. This test
    // previously required stderr to be empty, which made the documented way
    // to get a clean id also disable the safety net.
    let quiet = sshd.mule(&config, &["--quiet", "run", "printf ok | tail -1"]);
    assert!(quiet.status.success());
    let quiet_err = String::from_utf8_lossy(&quiet.stderr);
    assert!(
        !quiet_err.contains("next: mule"),
        "--quiet must suppress the hints: {quiet_err}"
    );
    assert!(
        quiet_err.contains("may hide the job's failure"),
        "--quiet must keep the warning: {quiet_err}"
    );
    let quiet_id = stdout(&quiet);

    clean_jobs(&sshd, &[id, quiet_id]);
}

#[test]
fn run_wait_hints_when_a_missing_tool_fails_without_touching_stdout() {
    require_sshd!();
    let sshd = Sshd::start();
    let _master = sshd.open_master(&sshd.socket);
    let tmux = Tmux::new(&sshd, "missing-path-hint");
    let config = sshd.write_config(&tmux.name);

    let failed = sshd.mule(
        &config,
        &["run", "--wait", "mule_definitely_missing_binary 2>&1"],
    );
    assert_eq!(failed.status.code(), Some(127));
    let failed_stdout = String::from_utf8_lossy(&failed.stdout);
    let mut lines = failed_stdout.lines();
    let failed_id = lines.next().expect("run must print its recovery id");
    assert_eq!(failed_id.len(), 6);
    let log_lines = lines.collect::<Vec<_>>();
    assert_eq!(
        log_lines.len(),
        1,
        "the hint must not alter stdout: {failed_stdout:?}"
    );
    assert!(
        log_lines[0].contains("mule_definitely_missing_binary: command not found"),
        "{failed_stdout:?}"
    );
    let hint = String::from_utf8_lossy(&failed.stderr);
    assert!(hint.contains("shell is non-login"), "{hint}");
    assert!(hint.contains("If this is a missing tool"), "{hint}");
    assert!(hint.contains("export PATH=$HOME/.elan/bin:$PATH"), "{hint}");

    let quiet = sshd.mule(
        &config,
        &[
            "--quiet",
            "run",
            "--wait",
            "mule_definitely_missing_binary 2>&1",
        ],
    );
    assert_eq!(quiet.status.code(), Some(127));
    assert!(
        quiet.stderr.is_empty(),
        "--quiet must suppress the PATH hint: {}",
        String::from_utf8_lossy(&quiet.stderr)
    );
    let quiet_id = stdout(&quiet)
        .lines()
        .next()
        .expect("run must print its recovery id")
        .to_string();

    let successful = sshd.mule(&config, &["run", "--wait", "printf 'not found on PATH\\n'"]);
    assert!(successful.status.success());
    let successful_stdout = String::from_utf8_lossy(&successful.stdout);
    let mut lines = successful_stdout.lines();
    let successful_id = lines.next().expect("run must print its recovery id");
    assert_eq!(lines.collect::<Vec<_>>(), ["not found on PATH"]);
    assert!(
        !String::from_utf8_lossy(&successful.stderr).contains("shell is non-login"),
        "a successful job must not scan its log or draw the hint"
    );

    clean_jobs(
        &sshd,
        &[failed_id.to_string(), quiet_id, successful_id.to_string()],
    );
}

#[test]
fn dispatch_preserves_command_argument_boundaries() {
    require_sshd!();
    let sshd = Sshd::start();
    let _master = sshd.open_master(&sshd.socket);
    let tmux = Tmux::new(&sshd, "command-args");
    let config = sshd.write_config(&tmux.name);
    let cases = [
        (
            &["printf", "[%s]", "a b", "c"][..],
            "[a b][c]",
            "'printf' '[%s]' 'a b' 'c'",
        ),
        (
            &["printf", "[%s]", "", "x"][..],
            "[][x]",
            "'printf' '[%s]' '' 'x'",
        ),
    ];
    let mut ids = Vec::new();

    for (args, expected_log, expected_cmd) in cases {
        let mut run_args = vec!["run", "--wait"];
        run_args.extend_from_slice(args);
        let output = sshd.mule(&config, &run_args);
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let stdout = String::from_utf8_lossy(&output.stdout);
        let (id, log) = stdout
            .split_once('\n')
            .expect("run --wait must print the id before the log");
        assert_eq!(
            log, expected_log,
            "arguments must reach the remote shell intact"
        );

        let recorded = sshd.ssh(&["cat", &format!("$XDG_STATE_HOME/mule/jobs/{id}/cmd")]);
        assert!(recorded.status.success());
        assert_eq!(String::from_utf8_lossy(&recorded.stdout), expected_cmd);
        ids.push(id.to_string());
    }

    clean_jobs(&sshd, &ids);
}

#[test]
fn run_applies_crew_metadata_only_to_managed_job_shells() {
    require_sshd!();
    let sshd = Sshd::start();
    let _master = sshd.open_master(&sshd.socket);
    let tmux = Tmux::new(&sshd, "crew-metadata");
    let config = sshd.write_config(&tmux.name);
    let sentinel = sshd.dir.join("workstream-was-executed");
    let hostile = format!("crew ' \"$(touch {})\"\nnext", sentinel.display());
    let command =
        "printf '%s|%s|<%s>' \"$MU_MANAGED_AGENT\" \"$MU_AGENT_NAME\" \"${MU_WORKSTREAM-}\"";

    let managed = std::process::Command::new(env!("CARGO_BIN_EXE_mule"))
        .arg("--config")
        .arg(&config)
        .args(["run", "--wait", command])
        .env("PATH", sshd.path_env())
        .env("XDG_STATE_HOME", &sshd.state_root)
        .env("MU_WORKSTREAM", &hostile)
        .output()
        .unwrap();
    assert!(
        managed.status.success(),
        "{}",
        String::from_utf8_lossy(&managed.stderr)
    );
    let managed_stdout = String::from_utf8_lossy(&managed.stdout);
    let (managed_id, managed_log) = managed_stdout.split_once('\n').unwrap();
    assert_eq!(managed_log, format!("1|mule-{managed_id}|<{hostile}>"));
    assert!(
        !sentinel.exists(),
        "workstream text executed as shell syntax"
    );

    let absent = std::process::Command::new(env!("CARGO_BIN_EXE_mule"))
        .arg("--config")
        .arg(&config)
        .args([
            "run",
            "--wait",
            "printf '%s|%s|%s' \"$MU_MANAGED_AGENT\" \"$MU_AGENT_NAME\" \"${MU_WORKSTREAM+x}\"",
        ])
        .env("PATH", sshd.path_env())
        .env("XDG_STATE_HOME", &sshd.state_root)
        .env_remove("MU_WORKSTREAM")
        .output()
        .unwrap();
    assert!(
        absent.status.success(),
        "{}",
        String::from_utf8_lossy(&absent.stderr)
    );
    let absent_stdout = String::from_utf8_lossy(&absent.stdout);
    let (absent_id, absent_log) = absent_stdout.split_once('\n').unwrap();
    assert_eq!(absent_log, format!("1|mule-{absent_id}|"));

    let human = std::process::Command::new(env!("CARGO_BIN_EXE_mule"))
        .arg("--config")
        .arg(&config)
        .args([
            "run",
            "--human",
            "--wait",
            "printf '%s|%s|%s' \"${MU_MANAGED_AGENT+x}\" \"${MU_AGENT_NAME+x}\" \"${MU_WORKSTREAM+x}\"",
        ])
        .env("PATH", sshd.path_env())
        .env("XDG_STATE_HOME", &sshd.state_root)
        .env("MU_MANAGED_AGENT", "local")
        .env("MU_AGENT_NAME", "local")
        .env("MU_WORKSTREAM", &hostile)
        .output()
        .unwrap();
    assert!(
        human.status.success(),
        "{}",
        String::from_utf8_lossy(&human.stderr)
    );
    let human_stdout = String::from_utf8_lossy(&human.stdout);
    let (human_id, human_log) = human_stdout.split_once('\n').unwrap();
    assert_eq!(human_log, "||");

    clean_jobs(
        &sshd,
        &[
            managed_id.to_string(),
            absent_id.to_string(),
            human_id.to_string(),
        ],
    );
}

#[test]
fn tui_job_uses_the_remote_pane_pty_and_accepts_input() {
    require_sshd!();
    let sshd = Sshd::start();
    let _master = sshd.open_master(&sshd.socket);
    let tmux = Tmux::new(&sshd, "tui-pty");
    let config = sshd.write_config(&tmux.name);
    let command = "[ -t 0 ] && [ -t 1 ] && [ -t 2 ] || exit 9; printf 'ready\\n'; IFS= read -r answer; printf 'answer:%s\\n' \"$answer\"";

    let out = sshd.mule(&config, &["run", "--tui", command]);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let id = stdout(&out);
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let pane = sshd.ssh(&[
            "tmux",
            "-L",
            &tmux.name,
            "capture-pane",
            "-p",
            "-t",
            &format!("mule-{id}"),
        ]);
        if String::from_utf8_lossy(&pane.stdout).contains("ready") {
            break;
        }
        assert!(Instant::now() < deadline, "early output never reached pane");
        std::thread::sleep(Duration::from_millis(20));
    }
    let sent = sshd.ssh(&[
        "tmux",
        "-L",
        &tmux.name,
        "send-keys",
        "-t",
        &format!("mule-{id}"),
        "remote-input",
        "Enter",
    ]);
    assert!(sent.status.success());
    while stdout(&sshd.mule(&config, &["poll", &id])) == "running" {
        assert!(Instant::now() < deadline, "TUI job never completed");
        std::thread::sleep(Duration::from_millis(20));
    }

    let artifacts = sshd.ssh(&[&format!(
        "d=$XDG_STATE_HOME/mule/jobs/{id}; cat $d/mode; cat $d/log; cat $d/screen"
    )]);
    assert!(artifacts.status.success());
    let text = String::from_utf8_lossy(&artifacts.stdout);
    assert!(text.contains("tui"), "{text:?}");
    assert!(text.contains("ready"), "{text:?}");
    assert!(text.contains("answer:remote-input"), "{text:?}");

    clean_jobs(&sshd, &[id]);
}

#[test]
fn tui_tail_reads_screen_by_default_and_raw_transcript_only_when_requested() {
    require_sshd!();
    let sshd = Sshd::start();
    let _master = sshd.open_master(&sshd.socket);
    let tmux = Tmux::new(&sshd, "tui-tail");
    let config = sshd.write_config(&tmux.name);
    let command = "printf 'first\\n'; printf '\\033[2Jvisible\\n'; IFS= read -r answer; printf 'final:%s\\n' \"$answer\"";

    let id = stdout(&sshd.mule(&config, &["--quiet", "run", "--tui", command]));
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let screen = sshd.mule(&config, &["tail", &id]);
        if String::from_utf8_lossy(&screen.stdout).contains("visible") {
            assert!(screen.status.success());
            assert!(
                !screen.stdout.windows(4).any(|bytes| bytes == b"\x1b[2J"),
                "default TUI tail must be a readable screen snapshot: {:?}",
                screen.stdout
            );
            break;
        }
        assert!(
            Instant::now() < deadline,
            "running screen never became visible"
        );
        std::thread::sleep(Duration::from_millis(20));
    }

    let sent = sshd.ssh(&[
        "tmux",
        "-L",
        &tmux.name,
        "send-keys",
        "-t",
        &format!("mule-{id}"),
        "done",
        "Enter",
    ]);
    assert!(sent.status.success());
    while stdout(&sshd.mule(&config, &["poll", &id])) == "running" {
        assert!(Instant::now() < deadline, "TUI job never completed");
        std::thread::sleep(Duration::from_millis(20));
    }

    let completed = sshd.mule(&config, &["tail", &id]);
    assert!(completed.status.success());
    assert!(
        String::from_utf8_lossy(&completed.stdout).contains("final:done"),
        "completed tail must read the saved screen: {:?}",
        completed.stdout
    );

    let transcript = sshd.mule(&config, &["tail", "--transcript", "--all", &id]);
    assert!(transcript.status.success());
    assert!(
        transcript
            .stdout
            .windows(4)
            .any(|bytes| bytes == b"\x1b[2J"),
        "explicit transcript must preserve terminal bytes: {:?}",
        transcript.stdout
    );

    clean_jobs(&sshd, &[id]);
}

#[test]
fn tui_follow_refuses_with_screen_and_picker_hints() {
    require_sshd!();
    let sshd = Sshd::start();
    let _master = sshd.open_master(&sshd.socket);
    let tmux = Tmux::new(&sshd, "tui-follow");
    let config = sshd.write_config(&tmux.name);
    let id = stdout(&sshd.mule(&config, &["--quiet", "run", "--tui", "sleep 30"]));

    let out = sshd.mule(&config, &["tail", "-f", &id]);
    assert!(!out.status.success());
    assert!(out.stdout.is_empty());
    let error = String::from_utf8_lossy(&out.stderr);
    assert!(error.contains(&format!("mule tail {id}")), "{error}");
    assert!(error.contains("murmur pick --all"), "{error}");
    assert!(!error.contains("--transcript"), "{error}");

    let _ = sshd.mule(&config, &["--quiet", "kill", "--rm", &id]);
}

#[test]
fn tui_timeout_and_kill_keep_existing_rc_and_cleanup_contracts() {
    require_sshd!();
    let sshd = Sshd::start();
    let _master = sshd.open_master(&sshd.socket);
    let tmux = Tmux::new(&sshd, "tui-lifecycle");
    let config = sshd.write_config(&tmux.name);

    let timed = stdout(&sshd.mule(
        &config,
        &["run", "--tui", "--max-secs", "1", "trap '' TERM; sleep 30"],
    ));
    let deadline = Instant::now() + Duration::from_secs(10);
    while stdout(&sshd.mule(&config, &["poll", &timed])) == "running" {
        assert!(Instant::now() < deadline, "TUI timeout never fired");
        std::thread::sleep(Duration::from_millis(50));
    }
    assert_eq!(stdout(&sshd.mule(&config, &["poll", &timed])), "124");

    let killed = stdout(&sshd.mule(&config, &["run", "--tui", "sleep 30"]));
    let kill = sshd.mule(&config, &["kill", &killed]);
    assert!(kill.status.success());
    assert_eq!(stdout(&sshd.mule(&config, &["poll", &killed])), "137");

    let sessions = sshd.ssh(&[
        "tmux",
        "-L",
        &tmux.name,
        "list-sessions",
        "-F",
        "#{session_name}",
    ]);
    let names = String::from_utf8_lossy(&sessions.stdout);
    for id in [&timed, &killed] {
        assert!(!names.contains(&format!("mule-{id}")), "{names}");
        assert!(!names.contains(&format!("watch-{id}")), "{names}");
    }
    clean_jobs(&sshd, &[timed, killed]);
}

#[test]
fn dispatch_returns_before_the_job_finishes() {
    require_sshd!();
    let sshd = Sshd::start();
    let _master = sshd.open_master(&sshd.socket);
    let tmux = Tmux::new(&sshd, "latency");
    let config = sshd.write_config(&tmux.name);
    let release = sshd.dir.join("release-job");
    let command = format!("while [ ! -f '{}' ]; do sleep 0.1; done", release.display());

    // Invariant 3: dispatch is detached, so it must return before the job can
    // finish. Originally measured as returning in 0s while a full suite ran;
    // real dispatch measured ~125ms. Those measurements explain the design,
    // but elapsed time also measures scheduler load. Block the job on a file
    // instead and assert the ordering directly.
    let dispatch = std::process::Command::new(env!("CARGO_BIN_EXE_mule"))
        .arg("--config")
        .arg(&config)
        .args(["run", &command])
        .env("PATH", sshd.path_env())
        .env("XDG_STATE_HOME", &sshd.state_root)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    let release_on_failure = release.clone();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let watchdog = std::thread::spawn(move || {
        if done_rx.recv_timeout(Duration::from_secs(15)).is_err() {
            std::fs::write(release_on_failure, "").unwrap();
        }
    });
    let output = dispatch.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        !release.exists(),
        "dispatch waited until the watchdog released the job"
    );
    let id = stdout(&output);
    assert_eq!(
        stdout(&sshd.mule(&config, &["poll", &id])),
        "running",
        "the detached job must still be blocked on the release file"
    );
    std::fs::write(&release, "").unwrap();
    done_tx.send(()).unwrap();
    watchdog.join().unwrap();

    let _ = sshd.mule(&config, &["kill", &id]);
    clean_jobs(&sshd, &[id]);
}

#[test]
fn a_job_exceeding_its_remote_cap_is_killed_with_124() {
    require_sshd!();
    let sshd = Sshd::start();
    let _master = sshd.open_master(&sshd.socket);
    let tmux = Tmux::new(&sshd, "job-timeout");
    let config = sshd.write_config(&tmux.name);
    let mut text = std::fs::read_to_string(&config).unwrap();
    text.push_str("max_job_secs = 1\n");
    std::fs::write(&config, text).unwrap();

    let out = sshd.mule(&config, &["run", "trap '' TERM; sleep 30"]);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let id = stdout(&out);

    let deadline = Instant::now() + Duration::from_secs(10);
    while stdout(&sshd.mule(&config, &["poll", &id])) == "running" {
        assert!(Instant::now() < deadline, "timed-out job never finished");
        std::thread::sleep(Duration::from_millis(50));
    }
    assert_eq!(stdout(&sshd.mule(&config, &["poll", &id])), "124");

    let sessions = sshd.ssh(&[
        "tmux",
        "-L",
        &tmux.name,
        "list-sessions",
        "-F",
        "#{session_name}",
    ]);
    assert!(
        !String::from_utf8_lossy(&sessions.stdout).contains(&format!("mule-{id}")),
        "timed-out job left its tmux session alive"
    );

    clean_jobs(&sshd, &[id]);
}

#[test]
fn a_job_finishing_inside_its_remote_cap_keeps_its_rc_and_no_watchdog() {
    require_sshd!();
    let sshd = Sshd::start();
    let _master = sshd.open_master(&sshd.socket);
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
    let out = sshd.mule(
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
    while stdout(&sshd.mule(&config, &["poll", &id])) == "running" {
        assert!(Instant::now() < deadline, "fast job never finished");
        std::thread::sleep(Duration::from_millis(50));
    }
    assert_eq!(stdout(&sshd.mule(&config, &["poll", &id])), "7");

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
        stdout(&sshd.mule(&config, &["poll", &id])),
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
        !String::from_utf8_lossy(&sessions.stdout).contains(&format!("mule-{id}")),
        "fast job left its tmux session or watchdog alive"
    );

    clean_jobs(&sshd, &[id]);
}

#[test]
fn ls_keeps_multiline_commands_on_one_row_without_shortening_json() {
    require_sshd!();
    let sshd = Sshd::start();
    let _master = sshd.open_master(&sshd.socket);
    let tmux = Tmux::new(&sshd, "multiline-ls");
    let config = sshd.write_config(&tmux.name);
    let command = "python3 -c \"print('ok')\n# this deliberately long comment makes the human listing truncate rather than wrap across the terminal\n#\ttabbed\"";

    let run = sshd.mule(&config, &["run", command]);
    assert!(
        run.status.success(),
        "{}",
        String::from_utf8_lossy(&run.stderr)
    );
    let id = stdout(&run);

    let table = stdout(&sshd.mule(&config, &["ls", "--all"]));
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

    let json = stdout(&sshd.mule(&config, &["ls", "--all", "--json"]));
    assert!(
        json.contains(
            "\"cmd\":\"python3 -c \\\"print('ok')\\n# this deliberately long comment makes the human listing truncate rather than wrap across the terminal\\n#\\ttabbed\\\"\""
        ),
        "json must keep the complete command: {json}"
    );

    let full = stdout(&sshd.mule(&config, &["ls", "--all", "--full"]));
    let full_row = full
        .lines()
        .find(|line| line.starts_with(&id))
        .expect("dispatched job must appear in ls --full");
    assert!(
        full_row.contains("tabbed"),
        "--full must keep the whole command: {full_row}"
    );
    assert!(
        !full_row.contains('\u{2026}'),
        "--full must not truncate: {full_row}"
    );

    clean_jobs(&sshd, &[id]);
}

#[test]
fn a_job_survives_the_loss_of_its_tmux_server() {
    require_sshd!();
    let sshd = Sshd::start();
    let _master = sshd.open_master(&sshd.socket);
    let tmux = Tmux::new(&sshd, "durable");
    let config = sshd.write_config(&tmux.name);

    let out = sshd.mule(&config, &["run", "echo durable; exit 9"]);
    let id = stdout(&out);

    // Wait for completion, then destroy the tmux server entirely. `rc` and
    // `log` are files, so the record must outlive the process container.
    let deadline = Instant::now() + Duration::from_secs(10);
    while stdout(&sshd.mule(&config, &["poll", &id])) == "running" {
        assert!(Instant::now() < deadline, "job never finished");
        std::thread::sleep(Duration::from_millis(50));
    }
    drop(tmux); // destroy the server on purpose: the record must outlive it

    assert_eq!(stdout(&sshd.mule(&config, &["poll", &id])), "9");
    assert_eq!(stdout(&sshd.mule(&config, &["tail", &id])), "durable");
    let wait = sshd.mule(&config, &["wait", &id]);
    assert_eq!(wait.status.code(), Some(9), "wait returns the job's code");
    assert!(wait.stdout.is_empty(), "wait must keep stdout clean");
    let hint = String::from_utf8_lossy(&wait.stderr);
    assert!(hint.contains(&format!("mule tail {id}")), "{hint}");
    assert!(hint.contains(&format!("mule rm {id}")), "{hint}");

    clean_jobs(&sshd, &[id]);
}

#[test]
fn the_private_tmux_server_is_invisible_to_the_default_one() {
    require_sshd!();
    let sshd = Sshd::start();
    let _master = sshd.open_master(&sshd.socket);
    let tmux = Tmux::new(&sshd, "private");
    let config = sshd.write_config(&tmux.name);

    let id = stdout(&sshd.mule(&config, &["run", "sleep 30"]));

    // Invariant 2. `tmux ls` with no -L talks to the user's own server, which
    // must never show mule's sessions.
    let default = sshd.ssh(&["tmux", "ls"]);
    let listing = format!(
        "{}{}",
        String::from_utf8_lossy(&default.stdout),
        String::from_utf8_lossy(&default.stderr)
    );
    assert!(
        !listing.contains(&format!("mule-{id}")),
        "mule's session leaked into the default tmux server: {listing}"
    );

    // It is present on mule's own server, so the check above is meaningful.
    let private = sshd.ssh(&["tmux", "-L", &tmux.name, "ls"]);
    assert!(
        String::from_utf8_lossy(&private.stdout).contains(&format!("mule-{id}")),
        "expected the session on mule's private server"
    );

    let _ = sshd.mule(&config, &["kill", &id]);
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
        let out = sshd.mule(&config, &verb);
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
        let out = sshd.mule(&config, &verb);
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

#[test]
fn host_info_probes_real_capabilities_and_reports_a_down_master() {
    require_sshd!();
    let sshd = Sshd::start();
    let _master = sshd.open_master(&sshd.socket);
    let config = sshd.dir.join("host-info.toml");
    std::fs::write(
        &config,
        format!(
            "[hosts.live]\ntarget = \"127.0.0.1\"\nsocket = \"{}\"\n\
             [hosts.down]\ntarget = \"127.0.0.1\"\nsocket = \"{}\"\n",
            sshd.socket.display(),
            sshd.dir.join("down.sock").display()
        ),
    )
    .unwrap();

    let list = sshd.mule(&config, &["host", "list"]);
    assert!(list.status.success());
    // The hint names the verb, in the surface the caller asked for -- a table
    // reader gets the table form. `--json` is asserted against `host list
    // --json` in tests/host_list.rs, where both branches are covered.
    assert!(
        String::from_utf8_lossy(&list.stderr).contains("mule host info"),
        "host list must point agents at the opt-in probe"
    );

    let expected_os = stdout(&sshd.ssh(&["uname", "-s"]));
    let expected_arch = stdout(&sshd.ssh(&["uname", "-m"]));
    let expected_cores = stdout(&sshd.ssh(&["nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null"]));
    let expected_ram = stdout(&sshd.ssh(&[
        "if [ -r /proc/meminfo ]; then awk '/MemTotal/{printf \"%.0f\", $2/1048576}' /proc/meminfo; else echo $(( $(sysctl -n hw.memsize) / 1073741824 )); fi",
    ]));
    let out = sshd.mule(&config, &["host", "info", "--json"]);
    assert!(
        out.status.success(),
        "host info failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let json = String::from_utf8_lossy(&out.stdout);
    for field in [
        "\"name\"",
        "\"target\"",
        "\"master\"",
        "\"os\"",
        "\"arch\"",
        "\"cores\"",
        "\"ram_gb\"",
        "\"gpu\"",
        "\"remedy\"",
    ] {
        assert!(json.contains(field), "missing {field}: {json}");
    }
    assert!(
        json.contains(&format!(
            "\"name\":\"live\",\"target\":\"127.0.0.1\",\"master\":true,\"os\":\"{expected_os}\",\"arch\":\"{expected_arch}\""
        )),
        "live host capabilities did not come from the loopback host: {json}"
    );
    assert!(
        json.contains(&format!("\"cores\":{expected_cores}")),
        "{json}"
    );
    assert!(
        json.contains(&format!("\"ram_gb\":{expected_ram}")),
        "{json}"
    );
    let live = json.split("\"name\":\"live\"").nth(1).unwrap_or_default();
    assert!(
        !live.contains("\"gpu\":\"\""),
        "GPU must be explicit: {json}"
    );
    assert!(
        !live.contains("\"gpu\":\"unknown\""),
        "a reachable host without a detected GPU must report none: {json}"
    );
    assert!(
        json.contains("\"name\":\"down\",\"target\":\"127.0.0.1\",\"master\":false,\"os\":\"unknown\",\"arch\":\"unknown\",\"cores\":null,\"ram_gb\":null,\"gpu\":\"unknown\""),
        "a down master must remain visible without a probe: {json}"
    );
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("ssh -MNf"),
        "down host must include its remedy"
    );
}

#[test]
fn kill_rm_ends_the_job_and_drops_its_state() {
    require_sshd!();
    let sshd = Sshd::start();
    let _master = sshd.open_master(&sshd.socket);
    let tmux = Tmux::new(&sshd, "kill-rm");
    let config = sshd.write_config(&tmux.name);

    let out = sshd.mule(&config, &["run", "sleep 60"]);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let id = stdout(&out);
    assert_eq!(stdout(&sshd.mule(&config, &["poll", &id])), "running");

    // One verb, one decision. `kill` then `rm` was two round trips on the
    // capped channel for what is nearly always a single intent: ending a job
    // you did not mean to start also means discarding its output.
    let killed = sshd.mule(&config, &["kill", "--rm", &id]);
    assert!(
        killed.status.success(),
        "{}",
        String::from_utf8_lossy(&killed.stderr)
    );
    assert!(
        String::from_utf8_lossy(&killed.stderr).contains("killed and removed"),
        "{}",
        String::from_utf8_lossy(&killed.stderr)
    );

    // The state directory is gone, so the job is absent from `ls --all`
    // entirely rather than listed as done.
    let listed = stdout(&sshd.mule(&config, &["ls", "--all", "--quiet"]));
    assert!(
        !listed.contains(&id),
        "kill --rm must leave no job state: {listed}"
    );

    // And the session is gone, so nothing is still running.
    let sessions = sshd.ssh(&[
        "tmux",
        "-L",
        &tmux.name,
        "list-sessions",
        "-F",
        "#{session_name}",
    ]);
    assert!(
        !String::from_utf8_lossy(&sessions.stdout).contains(&format!("mule-{id}")),
        "kill --rm must end the job, not just forget it"
    );
}

#[test]
fn removed_jobs_are_missing_not_orphaned() {
    require_sshd!();
    let sshd = Sshd::start();
    let _master = sshd.open_master(&sshd.socket);
    let tmux = Tmux::new(&sshd, "missing-job");
    let config = sshd.write_config(&tmux.name);

    let id = stdout(&sshd.mule(&config, &["run", "sleep 60"]));
    let removed = sshd.mule(&config, &["kill", "--rm", &id]);
    assert!(removed.status.success());

    for args in [
        vec!["poll", &id],
        vec!["poll", &id, "--json"],
        vec!["wait", &id],
        vec!["tail", &id],
    ] {
        let out = sshd.mule(&config, &args);
        assert_eq!(out.status.code(), Some(1), "{args:?}");
        assert!(out.stdout.is_empty(), "{args:?}: {:?}", out.stdout);
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(
            stderr.contains(&format!("job {id} not found")),
            "{args:?}: {stderr}"
        );
        assert!(!stderr.contains("orphan"), "{args:?}: {stderr}");
    }
}

#[test]
fn quiet_drops_hints_but_keeps_warnings() {
    require_sshd!();
    let sshd = Sshd::start();
    let _master = sshd.open_master(&sshd.socket);
    let tmux = Tmux::new(&sshd, "quiet-warn");
    let config = sshd.write_config(&tmux.name);

    // A real shape an agent writes when exploring a host, pasted from a
    // session: the pipeline's `head` becomes the job's rc.
    let command = "grep -rn proxy /etc/hosts 2>/dev/null | head -12";

    let loud = sshd.mule(&config, &["run", command]);
    assert!(loud.status.success());
    let loud_err = String::from_utf8_lossy(&loud.stderr);
    assert!(
        loud_err.contains("may hide the job's failure"),
        "{loud_err}"
    );
    assert!(loud_err.contains("next: mule wait"), "{loud_err}");

    // `--quiet` means "stop holding my hand", not "disable the safety net".
    // Those are different classes: a hint is convenience, a warning is
    // correctness. Sharing one flag meant the documented way to get a clean
    // id -- which is what a caller piping mule through `tail -1` is after --
    // also silenced the warning about their own command.
    let quiet = sshd.mule(&config, &["--quiet", "run", command]);
    assert!(quiet.status.success());
    let quiet_err = String::from_utf8_lossy(&quiet.stderr);
    assert!(
        quiet_err.contains("may hide the job's failure"),
        "--quiet must keep the warning: {quiet_err:?}"
    );
    assert!(
        !quiet_err.contains("next: mule wait"),
        "--quiet must drop the hints: {quiet_err:?}"
    );

    // stdout stays exactly the id on both paths, so `id=$(mule run ...)` is
    // the clean way to get a handle and needs no piping at all.
    for out in [&loud, &quiet] {
        let id = stdout(out);
        assert_eq!(id.len(), 6, "stdout must be only the id: {id:?}");
    }

    clean_jobs(&sshd, &[stdout(&loud), stdout(&quiet)]);
}

#[test]
fn ls_running_excludes_finished_and_orphaned_jobs() {
    require_sshd!();
    let sshd = Sshd::start();
    let _master = sshd.open_master(&sshd.socket);
    let tmux = Tmux::new(&sshd, "ls-running");
    let config = sshd.write_config(&tmux.name);

    let running = stdout(&sshd.mule(&config, &["run", "sleep 60"]));
    let done = stdout(&sshd.mule(&config, &["run", "exit 0"]));
    let orphan = stdout(&sshd.mule(&config, &["run", "sleep 60"]));

    // Wait for the short job to finish; ordering, not a guessed sleep.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while stdout(&sshd.mule(&config, &["poll", &done])) == "running" {
        assert!(
            std::time::Instant::now() < deadline,
            "short job never finished"
        );
        std::thread::sleep(std::time::Duration::from_millis(50));
    }

    // Destroy one session without writing rc: that is an orphan, and
    // "running" must mean a live process rather than merely "not done".
    let killed = sshd.ssh(&[
        "tmux",
        "-L",
        &tmux.name,
        "kill-session",
        "-t",
        &format!("mule-{orphan}"),
    ]);
    assert!(killed.status.success());

    let table = stdout(&sshd.mule(&config, &["ls", "--running", "--quiet"]));
    assert!(table.contains(&running), "running job missing: {table}");
    assert!(
        !table.contains(&done),
        "finished job leaked into --running: {table}"
    );
    assert!(
        !table.contains(&orphan),
        "orphan leaked into --running: {table}"
    );

    let json = stdout(&sshd.mule(&config, &["ls", "--running", "--json", "--quiet"]));
    let value: serde_json::Value = serde_json::from_str(&json).unwrap();
    assert_eq!(value["items"].as_array().unwrap().len(), 1);
    assert_eq!(value["items"][0]["id"], running);
    assert_eq!(value["items"][0]["state"], "running");

    let _ = sshd.mule(&config, &["kill", "--rm", &running]);
    clean_jobs(&sshd, &[done, orphan]);
}

fn job_state_dir(sshd: &Sshd, id: &str) -> PathBuf {
    sshd.state_root.join("mule").join("jobs").join(id)
}

fn session_exists(sshd: &Sshd, tmux: &Tmux, name: &str) -> bool {
    sshd.ssh(&["tmux", "-L", &tmux.name, "has-session", "-t", name])
        .status
        .success()
}

#[test]
fn rm_id_is_a_proof_of_stop_over_the_capped_channel() {
    require_sshd!();
    let sshd = Sshd::start();
    let _master = sshd.open_master(&sshd.socket);
    let tmux = Tmux::new(&sshd, "rm-stop");
    let config = sshd.write_config(&tmux.name);

    // A capped job gets a watch-<id> session too, so both must go.
    let id = stdout(&sshd.mule(&config, &["run", "--max-secs", "120", "sleep 60"]));
    assert_eq!(stdout(&sshd.mule(&config, &["poll", &id])), "running");
    assert!(session_exists(&sshd, &tmux, &format!("mule-{id}")));
    assert!(session_exists(&sshd, &tmux, &format!("watch-{id}")));

    let removed = sshd.mule(&config, &["rm", &id]);
    assert_eq!(
        removed.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&removed.stderr)
    );
    assert!(!session_exists(&sshd, &tmux, &format!("mule-{id}")));
    assert!(!session_exists(&sshd, &tmux, &format!("watch-{id}")));
    assert!(!job_state_dir(&sshd, &id).exists());

    // Idempotent, and a never-seen id is the same.
    for id in [id.as_str(), "abc123"] {
        let again = sshd.mule(&config, &["rm", id]);
        assert_eq!(again.status.code(), Some(0), "rm {id}");
    }
}

#[test]
fn rm_id_that_cannot_remove_the_state_dir_exits_one_and_keeps_the_evidence() {
    require_sshd!();
    use std::os::unix::fs::PermissionsExt;
    let sshd = Sshd::start();
    let _master = sshd.open_master(&sshd.socket);
    let tmux = Tmux::new(&sshd, "rm-fail");
    let config = sshd.write_config(&tmux.name);

    let id = stdout(&sshd.mule(&config, &["run", "true"]));
    let dir = job_state_dir(&sshd, &id);
    let deadline = Instant::now() + Duration::from_secs(10);
    while stdout(&sshd.mule(&config, &["poll", &id])) == "running" {
        assert!(Instant::now() < deadline, "job never finished");
        std::thread::sleep(Duration::from_millis(40));
    }
    let locked = dir.join("locked");
    std::fs::create_dir_all(&locked).unwrap();
    std::fs::write(locked.join("evidence"), "x").unwrap();
    std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o500)).unwrap();
    let enforced = std::fs::write(locked.join("probe"), "x").is_err();

    let out = sshd.mule(&config, &["rm", &id]);

    std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o700)).unwrap();
    if !enforced {
        eprintln!("skipping: file modes are not enforced for this user");
        clean_jobs(&sshd, &[id]);
        return;
    }
    assert_eq!(out.status.code(), Some(1));
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("could not be removed"), "{stderr}");
    assert!(
        locked.join("evidence").exists(),
        "the directory's evidence must be kept"
    );
    assert!(out.stdout.is_empty(), "no id reported as removed");
    clean_jobs(&sshd, &[id]);
}

// -- mule forward / unforward -------------------------------------------------

/// What mule's printed master command sets (errors::master_command).
const MASTER_OPTS: [&str; 2] = ["-o", "StreamLocalBindUnlink=yes"];

/// A unix-socket server that answers every connection with `reply`, standing in
/// for the socket a remote job serves. On loopback "remote" is this machine, so
/// REMOTE is a local path and the sshd's channel connects to it.
struct Serve {
    path: PathBuf,
}

impl Serve {
    fn start(path: PathBuf, reply: &'static str) -> Self {
        let listener = std::os::unix::net::UnixListener::bind(&path).unwrap();
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { return };
                use std::io::Write;
                let _ = stream.write_all(reply.as_bytes());
            }
        });
        Self { path }
    }
}

impl Drop for Serve {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

/// Connect to `local` and read until EOF. Retries briefly: the forward is bound
/// before `mule forward` returns, but the remote hop races the first read.
fn through(local: &std::path::Path) -> String {
    use std::io::Read;
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Ok(mut stream) = std::os::unix::net::UnixStream::connect(local) {
            let mut text = String::new();
            let _ = stream.read_to_string(&mut text);
            if !text.is_empty() || Instant::now() > deadline {
                return text;
            }
        } else if Instant::now() > deadline {
            return String::new();
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn unix_spec(local: &std::path::Path, remote: &std::path::Path) -> String {
    format!("{}:{}", local.display(), remote.display())
}

fn holding_the_only_session(sshd: &Sshd, secs: &str) -> std::process::Child {
    let holder = std::process::Command::new(sshd.dir.join("ssh"))
        .arg("-S")
        .arg(&sshd.socket)
        .args(["-o", "BatchMode=yes", "127.0.0.1", "sleep", secs])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    std::thread::sleep(Duration::from_millis(400));
    holder
}

#[test]
fn a_forward_carries_traffic_while_the_only_session_is_held() {
    require_sshd!();
    let sshd = Sshd::start();
    let _master = sshd.open_master_with(&sshd.socket, &MASTER_OPTS);
    let tmux = Tmux::new(&sshd, "fwdheld");
    let config = sshd.write_config(&tmux.name);
    let (local, remote) = (sshd.dir.join("l.sock"), sshd.dir.join("r.sock"));
    let _serve = Serve::start(remote.clone(), "hello");

    let mut holder = holding_the_only_session(&sshd, "8");
    let started = Instant::now();
    let out = sshd.mule(&config, &["forward", "--unix", &unix_spec(&local, &remote)]);
    assert!(
        out.status.success(),
        "forward during a held session: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        started.elapsed() < Duration::from_secs(3),
        "a forward is not a session and must not queue behind one"
    );
    assert_eq!(through(&local), "hello");
    // Three concurrent forwarded connections also succeed (measured).
    let all: Vec<_> = (0..3)
        .map(|_| {
            let local = local.clone();
            std::thread::spawn(move || through(&local))
        })
        .collect();
    for connection in all {
        assert_eq!(connection.join().unwrap(), "hello");
    }

    let out = sshd.mule(
        &config,
        &["unforward", "--unix", &unix_spec(&local, &remote)],
    );
    assert!(out.status.success());
    assert!(!local.exists(), "unforward removes LOCAL");
    let _ = holder.kill();
    let _ = holder.wait();
}

#[test]
fn removing_local_then_forwarding_again_rebinds() {
    // Measured: after `rm L`, a bare `-O forward` exits 0 and binds nothing,
    // because the master still lists the forward. mule cancels first.
    require_sshd!();
    let sshd = Sshd::start();
    let _master = sshd.open_master_with(&sshd.socket, &MASTER_OPTS);
    let tmux = Tmux::new(&sshd, "fwdrm");
    let config = sshd.write_config(&tmux.name);
    let (local, remote) = (sshd.dir.join("l.sock"), sshd.dir.join("r.sock"));
    let _serve = Serve::start(remote.clone(), "again");
    let spec = unix_spec(&local, &remote);

    assert!(
        sshd.mule(&config, &["forward", "--unix", &spec])
            .status
            .success()
    );
    std::fs::remove_file(&local).unwrap();
    let out = sshd.mule(&config, &["forward", "--unix", &spec]);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(through(&local), "again");
}

#[test]
fn a_hand_opened_master_without_bind_unlink_still_forwards_over_a_stale_file() {
    // Step 3 (unlink) is what makes the master command's option a convenience
    // rather than a requirement.
    require_sshd!();
    let sshd = Sshd::start();
    let _master = sshd.open_master(&sshd.socket);
    let tmux = Tmux::new(&sshd, "fwdstale");
    let config = sshd.write_config(&tmux.name);
    let (local, remote) = (sshd.dir.join("l.sock"), sshd.dir.join("r.sock"));
    let _serve = Serve::start(remote.clone(), "ok");
    drop(std::os::unix::net::UnixListener::bind(&local).unwrap());

    let out = sshd.mule(&config, &["forward", "--unix", &unix_spec(&local, &remote)]);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(through(&local), "ok");
}

#[test]
fn changing_remote_for_one_local_is_avoided_through_the_record() {
    // The wedge: L:R, then L:R2, then cancel one. A naive cancel -> unlink ->
    // forward reproduces it. The record makes mule cancel the OLD spec first.
    require_sshd!();
    let sshd = Sshd::start();
    let _master = sshd.open_master_with(&sshd.socket, &MASTER_OPTS);
    let tmux = Tmux::new(&sshd, "fwdwedge");
    let config = sshd.write_config(&tmux.name);
    let local = sshd.dir.join("l.sock");
    let (r1, r2) = (sshd.dir.join("r1.sock"), sshd.dir.join("r2.sock"));
    let _s1 = Serve::start(r1.clone(), "one");
    let _s2 = Serve::start(r2.clone(), "two");

    let forward = |remote: &std::path::Path| {
        sshd.mule(&config, &["forward", "--unix", &unix_spec(&local, remote)])
    };
    assert!(forward(&r1).status.success());
    assert_eq!(through(&local), "one");
    assert!(forward(&r2).status.success());
    assert_eq!(through(&local), "two");
    let out = sshd.mule(&config, &["unforward", "--unix", &unix_spec(&local, &r2)]);
    assert!(out.status.success());
    // The step that was unrecoverable without the record.
    let out = forward(&r1);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(through(&local), "one");
}

#[test]
fn a_forced_wedge_is_detected_as_exit_three() {
    require_sshd!();
    let sshd = Sshd::start();
    let _master = sshd.open_master_with(&sshd.socket, &MASTER_OPTS);
    let tmux = Tmux::new(&sshd, "fwdforced");
    let config = sshd.write_config(&tmux.name);
    let local = sshd.dir.join("l.sock");
    let (r1, r2) = (sshd.dir.join("r1.sock"), sshd.dir.join("r2.sock"));
    let _s1 = Serve::start(r1.clone(), "one");

    // By hand, behind mule's back, so there is no record to avoid it.
    let by_hand = |op: &str, remote: &std::path::Path| {
        sshd.ssh_via(&sshd.socket, &["-O", op, "-L", &unix_spec(&local, remote)])
    };
    assert!(by_hand("forward", &r1).status.success());
    let _ = by_hand("forward", &r2);
    // Cancelling the LATER spec is what wedges (measured: cancelling the
    // earlier one leaves a working table). Both listeners close, and the table
    // keeps listing one.
    let _ = by_hand("cancel", &r2);
    let _ = std::fs::remove_file(&local);

    let out = sshd.mule(&config, &["forward", "--unix", &unix_spec(&local, &r1)]);
    let text = String::from_utf8_lossy(&out.stderr);
    assert_eq!(
        out.status.code(),
        Some(3),
        "a table that lists a forward nothing listens on is a hand-back: {text}"
    );
    assert!(text.contains("wedged"), "{text}");
    assert!(text.contains("-O exit"), "{text}");
    assert!(text.contains("ssh -MNf"), "{text}");
    assert!(out.stdout.is_empty());
}

#[test]
fn forward_exits_three_without_a_master_and_unforward_does_not_need_one() {
    require_sshd!();
    let sshd = Sshd::start();
    let tmux = Tmux::new(&sshd, "fwdnomaster");
    let config = sshd.write_config(&tmux.name);
    let (local, remote) = (sshd.dir.join("l.sock"), sshd.dir.join("r.sock"));
    let spec = unix_spec(&local, &remote);

    let out = sshd.mule(&config, &["forward", "--unix", &spec]);
    assert_eq!(out.status.code(), Some(3));
    assert!(String::from_utf8_lossy(&out.stderr).contains("ssh -MNf"));

    let out = sshd.mule(&config, &["unforward", "--unix", &spec]);
    assert_eq!(out.status.code(), Some(0), "idempotent, master or not");
}

#[test]
fn unforward_exits_one_naming_a_real_unlink_failure_and_keeps_the_record() {
    use std::os::unix::fs::PermissionsExt;
    require_sshd!();
    let sshd = Sshd::start();
    let _master = sshd.open_master_with(&sshd.socket, &MASTER_OPTS);
    let tmux = Tmux::new(&sshd, "fwdrofail");
    let config = sshd.write_config(&tmux.name);
    // A private directory, so it can be made unwritable without touching the
    // fixture's own.
    let dir = sshd.dir.join("readonly");
    std::fs::create_dir(&dir).unwrap();
    let (local, remote) = (dir.join("l.sock"), sshd.dir.join("r.sock"));
    let spec = unix_spec(&local, &remote);
    assert!(
        sshd.mule(&config, &["forward", "--unix", &spec])
            .status
            .success()
    );

    std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o500)).unwrap();
    let out = sshd.mule(&config, &["unforward", "--unix", &spec]);
    let remains = local.exists();
    std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700)).unwrap();

    if !remains {
        eprintln!("skipping: permissions are not enforced here (root?)");
        return;
    }
    assert_eq!(
        out.status.code(),
        Some(1),
        "LOCAL is still there: not success"
    );
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("l.sock"), "the failure names LOCAL: {err}");

    // The record survived, so the retry finishes the job.
    let out = sshd.mule(&config, &["unforward", "--unix", &spec]);
    assert_eq!(
        out.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(!local.exists());
}

// -- mule exec ----------------------------------------------------------------

/// The mule binary under test, as a spawnable (not yet run) child, so a test
/// can signal it. `extra_env` lets a test point HOME and XDG_CONFIG_HOME
/// somewhere private.
fn spawn_mule(
    sshd: &Sshd,
    config: &std::path::Path,
    args: &[&str],
    extra_env: &[(&str, &std::path::Path)],
) -> std::process::Child {
    mule_command(sshd, config, args, extra_env)
        .spawn()
        .expect("mule failed to spawn")
}

fn mule_command(
    sshd: &Sshd,
    config: &std::path::Path,
    args: &[&str],
    extra_env: &[(&str, &std::path::Path)],
) -> std::process::Command {
    let mut command = std::process::Command::new(env!("CARGO_BIN_EXE_mule"));
    command
        .arg("--config")
        .arg(config)
        .args(args)
        .env("PATH", sshd.path_env())
        .env("XDG_STATE_HOME", &sshd.state_root)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());
    for (key, value) in extra_env {
        command.env(key, value);
    }
    command
}

/// Spawn mule as the leader of its own process group, as a pane's foreground
/// job is: the group holds mule and every child it starts, so a signal to the
/// group reaches all of them at once, the way a Ctrl-C or a pane hangup does.
fn spawn_mule_in_group(
    sshd: &Sshd,
    config: &std::path::Path,
    args: &[&str],
    extra_env: &[(&str, &std::path::Path)],
) -> std::process::Child {
    use std::os::unix::process::CommandExt;
    mule_command(sshd, config, args, extra_env)
        .process_group(0)
        .spawn()
        .expect("mule failed to spawn")
}

/// Signal the whole process group led by `leader`. A missing group (everyone
/// already gone) is not an error: repeated signals race the group's death.
fn signal_group(leader: u32, signal: i32) {
    unsafe { libc::kill(-(leader as i32), signal) };
}

fn signal(child: &std::process::Child, name: &str) {
    let status = std::process::Command::new("kill")
        .args([&format!("-{name}"), &child.id().to_string()])
        .status()
        .unwrap();
    assert!(status.success());
}

/// Job ids currently on the "remote" (this machine, under the daemon's state).
fn job_ids(sshd: &Sshd) -> Vec<String> {
    std::fs::read_dir(sshd.state_root.join("mule/jobs"))
        .map(|dir| {
            dir.filter_map(Result::ok)
                .map(|e| e.file_name().to_string_lossy().into_owned())
                .collect()
        })
        .unwrap_or_default()
}

fn wait_until(what: &str, limit: Duration, mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + limit;
    while !done() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn sessions(sshd: &Sshd, tmux: &str) -> String {
    // One quoted script: ssh joins its arguments and the remote shell re-parses
    // them, so a bare `#{session_name}` would start a comment.
    let out = sshd.ssh(&[&format!(
        "tmux -L {tmux} list-sessions -F '#{{session_name}}' 2>/dev/null; true"
    )]);
    String::from_utf8_lossy(&out.stdout).into_owned()
}

#[test]
fn exec_splits_the_real_streams_and_prints_no_id_or_hint() {
    require_sshd!();
    let sshd = Sshd::start();
    let _master = sshd.open_master(&sshd.socket);
    let tmux = Tmux::new(&sshd, "exec-split");
    let config = sshd.write_config(&tmux.name);

    let out = sshd.mule(
        &config,
        &[
            "exec",
            "--",
            "sh",
            "-c",
            "echo to-out; echo to-err >&2; exit 5",
        ],
    );

    assert_eq!(out.status.code(), Some(5), "exec exits with the job's code");
    assert_eq!(String::from_utf8_lossy(&out.stdout), "to-out\n");
    assert_eq!(
        String::from_utf8_lossy(&out.stderr),
        "to-err\n",
        "stderr is the command's alone: no id, no `next:` hints"
    );
    assert!(job_ids(&sshd).is_empty(), "exec removes its job");
}

#[test]
fn exec_applies_env_for_a_multi_word_command_and_runs_in_cwd() {
    require_sshd!();
    let sshd = Sshd::start();
    let _master = sshd.open_master(&sshd.socket);
    let tmux = Tmux::new(&sshd, "exec-env");
    let config = sshd.write_config(&tmux.name);
    let cwd = sshd.dir.join("a dir");
    std::fs::create_dir_all(&cwd).unwrap();

    let out = sshd.mule(
        &config,
        &[
            "exec",
            "--cwd",
            cwd.to_str().unwrap(),
            "--",
            "env",
            "K=v w",
            "sh",
            "-c",
            "printf '%s|' \"$K\"; pwd",
        ],
    );

    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(text.starts_with("v w|"), "{text}");
    assert!(text.trim_end().ends_with("/a dir"), "{text}");
}

#[test]
fn exec_exits_seven_when_the_log_is_capped() {
    require_sshd!();
    let sshd = Sshd::start();
    let _master = sshd.open_master(&sshd.socket);
    let tmux = Tmux::new(&sshd, "exec-cap");
    let config = sshd.write_config(&tmux.name);
    let mut text = std::fs::read_to_string(&config).unwrap();
    text.push_str("max_log_bytes = 1024\n");
    std::fs::write(&config, text).unwrap();

    let out = sshd.mule(
        &config,
        &["exec", "i=0; while [ $i -lt 500 ]; do echo 0123456789012345678901234567890123456789; i=$((i+1)); done"],
    );

    assert_eq!(out.status.code(), Some(7));
    assert_eq!(out.stdout.len(), 1024);
    assert!(String::from_utf8_lossy(&out.stderr).contains("max_log_bytes"));
}

#[test]
fn a_long_exec_leaves_a_default_master_usable_and_a_signal_leaves_no_job() {
    require_sshd!();
    let sshd = Sshd::start();
    let _master = sshd.open_master(&sshd.socket);
    // A second, separate control master standing in for the user's default
    // one. The cap is per connection, so only a call multiplexed over a master
    // can be refused by it: a bare `ssh host cmd` opens a fresh connection that
    // no `MaxSessions` can see, and passes whatever exec does.
    let default_socket = sshd.dir.join("default.sock");
    let _default_master = sshd.open_master(&default_socket);
    let tmux = Tmux::new(&sshd, "exec-held");
    let config = sshd.write_config(&tmux.name);

    let exec = spawn_mule(&sshd, &config, &["exec", "sleep 30"], &[]);
    wait_until("the job to start", Duration::from_secs(10), || {
        sessions(&sshd, &tmux.name).contains("mule-")
    });

    // The point of exec: it holds nothing on the default connection, so an
    // ordinary `ssh host cmd` over it works for the whole 30s -- here, three
    // times, during it. (A held `ssh host 'sleep 30'` would have refused every
    // one.)
    for attempt in 0..3 {
        let out = sshd.ssh_via(&default_socket, &["echo", "ok"]);
        assert!(
            out.status.success(),
            "default-master ssh {attempt} refused during exec: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        std::thread::sleep(Duration::from_millis(300));
    }
    // And mule's own channel is not held either: a poll-shaped call is quick.
    let started = Instant::now();
    let listed = sshd.mule(&config, &["ls", "--quiet"]);
    assert!(listed.status.success());
    assert!(started.elapsed() < Duration::from_secs(5));

    // SIGTERM mid-exec: no job left.
    signal(&exec, "TERM");
    let out = exec.wait_with_output().unwrap();
    assert_eq!(out.status.code(), Some(143));
    wait_until("the job to be removed", Duration::from_secs(15), || {
        job_ids(&sshd).is_empty() && !sessions(&sshd, &tmux.name).contains("mule-")
    });
}

#[test]
fn sigint_mid_exec_leaves_no_job_and_exits_130() {
    require_sshd!();
    let sshd = Sshd::start();
    let _master = sshd.open_master(&sshd.socket);
    let tmux = Tmux::new(&sshd, "exec-int");
    let config = sshd.write_config(&tmux.name);

    let exec = spawn_mule(&sshd, &config, &["exec", "sleep 60"], &[]);
    wait_until("the job to start", Duration::from_secs(10), || {
        !job_ids(&sshd).is_empty()
    });
    signal(&exec, "INT");

    let out = exec.wait_with_output().unwrap();
    assert_eq!(out.status.code(), Some(130));
    wait_until("the job to be removed", Duration::from_secs(15), || {
        job_ids(&sshd).is_empty() && !sessions(&sshd, &tmux.name).contains("mule-")
    });
}

/// A directory holding an `ssh` that pauses inside every *probe* and drops a
/// flag file when it does, then runs the real shim. It makes "a probe is in
/// flight right now" an observable fact instead of a race: a group signal
/// delivered after the flag kills that ssh along with mule, which is what a
/// pane hangup or Ctrl-C does.
fn slow_probe_ssh(sshd: &Sshd, flag: &std::path::Path) -> PathBuf {
    use std::os::unix::fs::PermissionsExt;
    let dir = sshd.dir.join("slow-probe");
    std::fs::create_dir_all(&dir).unwrap();
    let shim = dir.join("ssh");
    std::fs::write(
        &shim,
        format!(
            "#!/bin/sh\ncase \"$*\" in *exists=*) echo $$ > '{}'; sleep 5;; esac\nexec '{}' \"$@\"\n",
            flag.display(),
            sshd.dir.join("ssh").display()
        ),
    )
    .unwrap();
    std::fs::set_permissions(&shim, std::fs::Permissions::from_mode(0o755)).unwrap();
    dir
}

/// A signal to exec's whole process group lands while a probe's ssh is in
/// flight, so the ssh dies with it and the probe fails. That failure is the
/// signal's fallout: the exit must still be 128+signal, with the job removed,
/// not 1 from a probe error.
///
/// The kernel delivers a group signal to its members in no fixed order, and
/// whichever of "ssh died" and "mule's handler ran" comes second decides the
/// exit -- left to chance, the unfixed code passed this about as often as it
/// failed. So the order is pinned to the bad one: the ssh is killed first, and
/// the group signal follows 50ms later, after the probe error has already
/// reached the main thread.
fn group_signal_mid_probe_exits_with_the_signal(tag: &str, signal: i32, expected: i32) {
    require_sshd!();
    let sshd = Sshd::start();
    let _master = sshd.open_master(&sshd.socket);
    let tmux = Tmux::new(&sshd, tag);
    let config = sshd.write_config(&tmux.name);
    let flag = sshd.dir.join("probe-in-flight");
    let slow = slow_probe_ssh(&sshd, &flag);
    let path = PathBuf::from(format!("{}:{}", slow.display(), sshd.path_env()));

    let mut exec = spawn_mule_in_group(
        &sshd,
        &config,
        &["exec", "sleep 60"],
        &[("PATH", path.as_path())],
    );
    wait_until("a probe to be in flight", Duration::from_secs(15), || {
        std::fs::read_to_string(&flag).is_ok_and(|pid| !pid.trim().is_empty())
    });
    let ssh_pid = std::fs::read_to_string(&flag).unwrap();
    let _ = std::process::Command::new("kill")
        .args(["-KILL", ssh_pid.trim()])
        .status();
    std::thread::sleep(Duration::from_millis(50));
    signal_group(exec.id(), signal);

    let status = exec.wait().unwrap();
    assert_eq!(
        status.code(),
        Some(expected),
        "a probe killed by the signal must not turn into exit 1"
    );
    wait_until("the job to be removed", Duration::from_secs(20), || {
        job_ids(&sshd).is_empty() && !sessions(&sshd, &tmux.name).contains("mule-")
    });
}

#[test]
fn a_group_sigint_mid_probe_exits_130_and_removes_the_job() {
    group_signal_mid_probe_exits_with_the_signal("exec-gint", libc::SIGINT, 130);
}

#[test]
fn a_group_sighup_mid_probe_exits_129_and_removes_the_job() {
    group_signal_mid_probe_exits_with_the_signal("exec-ghup", libc::SIGHUP, 129);
}

#[test]
fn the_detached_cleanup_survives_repeated_group_hangups_and_interrupts() {
    require_sshd!();
    let sshd = Sshd::start();
    let _master = sshd.open_master(&sshd.socket);
    let tmux = Tmux::new(&sshd, "exec-setsid");
    let config = sshd.write_config(&tmux.name);

    let mut exec = spawn_mule_in_group(&sshd, &config, &["exec", "sleep 60"], &[]);
    wait_until("the job to start", Duration::from_secs(10), || {
        !job_ids(&sshd).is_empty()
    });

    // Hold the ticket so the cleanup `rm` is alive and queued -- not finished --
    // for the whole barrage. Without `setsid()` it sits in exec's process group,
    // and the first of these kills it: the job then runs for its full 60s.
    let lock = sshd.state_root.join("mule/live.lock");
    let (held_tx, held_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
    let holder = std::thread::spawn(move || {
        mule::lock::with_lock_at(&lock, || {
            held_tx.send(()).unwrap();
            let _ = release_rx.recv();
        })
        .unwrap();
    });
    held_rx.recv().unwrap();
    std::thread::sleep(Duration::from_millis(500));

    signal_group(exec.id(), libc::SIGINT);
    assert_eq!(exec.wait().unwrap().code(), Some(130));
    // A pane's hangup is not one signal: the tty driver and the shell each send
    // theirs. Keep them coming for as long as the cleanup is queued.
    let barrage = Instant::now();
    while barrage.elapsed() < Duration::from_millis(1500) {
        signal_group(exec.id(), libc::SIGHUP);
        signal_group(exec.id(), libc::SIGINT);
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(
        !job_ids(&sshd).is_empty(),
        "the stop is still queued behind the ticket"
    );

    release_tx.send(()).unwrap();
    holder.join().unwrap();
    wait_until(
        "the queued cleanup to survive the barrage and run",
        Duration::from_secs(15),
        || job_ids(&sshd).is_empty() && !sessions(&sshd, &tmux.name).contains("mule-"),
    );
}

#[test]
fn the_detached_cleanup_uses_the_private_config_and_never_the_default_one() {
    require_sshd!();
    let sshd = Sshd::start();
    let _master = sshd.open_master(&sshd.socket);
    let tmux = Tmux::new(&sshd, "exec-cfg");
    let config = sshd.write_config(&tmux.name);
    // A home with no mule config. A cleanup child that dropped --config would
    // fall back to <xdg>/mule/config.toml, seed a template there, exit 1, and
    // leave the job running.
    let home = sshd.dir.join("empty-home");
    std::fs::create_dir_all(&home).unwrap();
    let xdg = home.join("config");
    let env = [("HOME", home.as_path()), ("XDG_CONFIG_HOME", xdg.as_path())];

    let exec = spawn_mule(&sshd, &config, &["exec", "sleep 60"], &env);
    wait_until("the job to start", Duration::from_secs(10), || {
        !job_ids(&sshd).is_empty()
    });
    signal(&exec, "TERM");
    let out = exec.wait_with_output().unwrap();
    assert_eq!(out.status.code(), Some(143));

    wait_until("the job to be removed", Duration::from_secs(15), || {
        job_ids(&sshd).is_empty() && !sessions(&sshd, &tmux.name).contains("mule-")
    });
    assert!(
        !xdg.join("mule/config.toml").exists() && !home.join(".config/mule").exists(),
        "the cleanup child must not read or seed the default config"
    );
}

#[test]
fn a_queued_cleanup_runs_after_the_ticket_frees_and_exec_exits_without_waiting() {
    require_sshd!();
    let sshd = Sshd::start();
    let _master = sshd.open_master(&sshd.socket);
    let tmux = Tmux::new(&sshd, "exec-queue");
    let config = sshd.write_config(&tmux.name);

    let exec = spawn_mule(&sshd, &config, &["exec", "sleep 60"], &[]);
    wait_until("the job to start", Duration::from_secs(10), || {
        !job_ids(&sshd).is_empty()
    });

    // Hold the host's ticket, as a caller whose ssh hangs would. exec's next
    // probe now queues behind it: that is the in-flight probe the signal must
    // abandon, and the cleanup `rm` must queue the same way.
    let lock = sshd.state_root.join("mule/live.lock");
    let (held_tx, held_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
    let holder = std::thread::spawn(move || {
        mule::lock::with_lock_at(&lock, || {
            held_tx.send(()).unwrap();
            let _ = release_rx.recv();
        })
        .unwrap();
    });
    held_rx.recv().unwrap();
    std::thread::sleep(Duration::from_millis(500));

    let started = Instant::now();
    signal(&exec, "TERM");
    let out = exec.wait_with_output().unwrap();
    assert_eq!(out.status.code(), Some(143));
    assert!(
        started.elapsed() < Duration::from_secs(3),
        "exec must not block its exit on the ticket lock"
    );

    // The stop is queued, not done: the job is still there while the ticket is.
    std::thread::sleep(Duration::from_millis(800));
    assert!(
        !job_ids(&sshd).is_empty(),
        "cleanup must queue behind the ticket"
    );

    release_tx.send(()).unwrap();
    holder.join().unwrap();
    wait_until("the queued cleanup to run", Duration::from_secs(15), || {
        job_ids(&sshd).is_empty() && !sessions(&sshd, &tmux.name).contains("mule-")
    });

    // The dead exec's abandoned probe ticket did not wedge the queue.
    let started = Instant::now();
    let listed = sshd.mule(&config, &["ls", "--quiet"]);
    assert!(listed.status.success());
    assert!(started.elapsed() < Duration::from_secs(5));
}

#[test]
fn a_signal_with_stdout_and_stderr_closed_does_not_panic() {
    require_sshd!();
    let sshd = Sshd::start();
    let _master = sshd.open_master(&sshd.socket);
    let tmux = Tmux::new(&sshd, "exec-hup");
    let config = sshd.write_config(&tmux.name);

    let mut exec = spawn_mule(&sshd, &config, &["exec", "sleep 60"], &[]);
    wait_until("the job to start", Duration::from_secs(10), || {
        !job_ids(&sshd).is_empty()
    });
    // The reader goes away, as a pane's tty does on hangup: every later write
    // is EPIPE.
    drop(exec.stdout.take());
    drop(exec.stderr.take());
    signal(&exec, "HUP");

    let status = exec.wait().unwrap();
    assert_eq!(
        status.code(),
        Some(129),
        "a panic on a dead stdout would exit 101"
    );
    wait_until("the job to be removed", Duration::from_secs(15), || {
        job_ids(&sshd).is_empty() && !sessions(&sshd, &tmux.name).contains("mule-")
    });
}

#[test]
fn exec_exits_three_without_a_master_and_says_what_to_run() {
    require_sshd!();
    let sshd = Sshd::start();
    let tmux = Tmux::new(&sshd, "exec-nomaster");
    let config = sshd.write_config(&tmux.name);

    let out = sshd.mule(&config, &["exec", "true"]);

    assert_eq!(out.status.code(), Some(3));
    assert!(String::from_utf8_lossy(&out.stderr).contains("ssh -MNf"));
    assert!(out.stdout.is_empty());
}

#[test]
fn the_cleanup_child_works_with_no_mule_on_path() {
    require_sshd!();
    let sshd = Sshd::start();
    let _master = sshd.open_master(&sshd.socket);
    let tmux = Tmux::new(&sshd, "exec-path");
    let config = sshd.write_config(&tmux.name);
    // ssh shim plus the system dirs only. The child must come from
    // `current_exe()`: a lookup of `mule` here finds nothing.
    let path = std::path::PathBuf::from(format!("{}:/usr/bin:/bin", sshd.dir.display()));
    let found = std::process::Command::new("sh")
        .args(["-c", "command -v mule"])
        .env("PATH", &path)
        .output()
        .unwrap();
    assert!(
        !found.status.success(),
        "a mule on this PATH voids the test"
    );

    let exec = spawn_mule(&sshd, &config, &["exec", "sleep 60"], &[("PATH", &path)]);
    wait_until("the job to start", Duration::from_secs(10), || {
        !job_ids(&sshd).is_empty()
    });
    signal(&exec, "TERM");
    assert_eq!(exec.wait_with_output().unwrap().status.code(), Some(143));

    wait_until("the job to be removed", Duration::from_secs(15), || {
        job_ids(&sshd).is_empty() && !sessions(&sshd, &tmux.name).contains("mule-")
    });
}

#[test]
fn exec_keeps_stderr_clean_through_a_lock_wait_longer_than_the_notice_threshold() {
    require_sshd!();
    let sshd = Sshd::start();
    let _master = sshd.open_master(&sshd.socket);
    let tmux = Tmux::new(&sshd, "exec-quiet");
    let config = sshd.write_config(&tmux.name);

    let lock = sshd.state_root.join("mule/live.lock");
    let (held_tx, held_rx) = std::sync::mpsc::channel();
    let holder = std::thread::spawn(move || {
        mule::lock::with_lock_at(&lock, || {
            held_tx.send(()).unwrap();
            // Past the 5s after which any other verb prints "waiting for lock".
            std::thread::sleep(Duration::from_secs(6));
        })
        .unwrap();
    });
    held_rx.recv().unwrap();

    let out = sshd.mule(&config, &["exec", "echo done"]);
    holder.join().unwrap();

    assert_eq!(String::from_utf8_lossy(&out.stdout), "done\n");
    assert_eq!(
        String::from_utf8_lossy(&out.stderr),
        "",
        "the lock-wait notice is mule talking, not the command"
    );
}

// -- mule run --lease -----------------------------------------------------------

/// A running lease: the child, its job file, and where its stdout went.
///
/// stdout goes to a file, not a pipe: the lease mirrors the remote screen, and
/// a pipe nobody drains would fill and block it mid-write.
struct Lease {
    child: std::process::Child,
    job_file: PathBuf,
    out: PathBuf,
}

impl Lease {
    fn start(
        sshd: &Sshd,
        config: &std::path::Path,
        tag: &str,
        specs: &[String],
        cmd: &[&str],
    ) -> Self {
        let job_file = sshd.dir.join(format!("{tag}.job"));
        let out = sshd.dir.join(format!("{tag}.out"));
        let mut args = vec![
            "run",
            "--lease",
            "--human",
            "--job-file",
            job_file.to_str().unwrap(),
        ];
        for spec in specs {
            args.extend(["--forward-unix", spec.as_str()]);
        }
        args.push("--");
        args.extend(cmd);
        let mut command = mule_command(sshd, config, &args, &[]);
        command.stdout(std::fs::File::create(&out).unwrap());
        let child = command.spawn().expect("mule failed to spawn");
        Self {
            child,
            job_file,
            out,
        }
    }

    /// The id from the job file, once the lease has written it.
    fn id(&self) -> String {
        wait_until("the job file", Duration::from_secs(10), || {
            std::fs::read_to_string(&self.job_file).is_ok_and(|id| id.ends_with('\n'))
        });
        std::fs::read_to_string(&self.job_file)
            .unwrap()
            .trim()
            .to_string()
    }

    fn output(&self) -> String {
        std::fs::read_to_string(&self.out).unwrap_or_default()
    }

    fn alive(&mut self) -> bool {
        self.child.try_wait().unwrap().is_none()
    }

    fn exit(mut self) -> (Option<i32>, String) {
        let status = self.child.wait().unwrap();
        let mut stderr = String::new();
        if let Some(mut err) = self.child.stderr.take() {
            use std::io::Read;
            let _ = err.read_to_string(&mut stderr);
        }
        (status.code(), stderr)
    }
}

impl Drop for Lease {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn record_dir(sshd: &Sshd) -> PathBuf {
    sshd.state_root.join("mule/forwards/live")
}

fn records(sshd: &Sshd) -> usize {
    std::fs::read_dir(record_dir(sshd))
        .map(|dir| {
            dir.filter_map(Result::ok)
                .filter(|e| e.path().extension().is_none())
                .count()
        })
        .unwrap_or(0)
}

#[test]
fn a_lease_forwards_traffic_and_leaves_another_master_usable_while_it_holds() {
    require_sshd!();
    let sshd = Sshd::start();
    let _master = sshd.open_master_with(&sshd.socket, &MASTER_OPTS);
    let default_socket = sshd.dir.join("default.sock");
    let _default_master = sshd.open_master(&default_socket);
    let tmux = Tmux::new(&sshd, "lease-traffic");
    let config = sshd.write_config(&tmux.name);
    let (local, remote) = (sshd.dir.join("l.sock"), sshd.dir.join("r.sock"));

    let mut lease = Lease::start(
        &sshd,
        &config,
        "traffic",
        &[unix_spec(&local, &remote)],
        &["sleep 60"],
    );
    let id = lease.id();
    // Forwarded without waiting for REMOTE: LOCAL listens before anything
    // serves the far end (connect succeeds and reads EOF).
    wait_until("LOCAL to listen", Duration::from_secs(10), || {
        std::os::unix::net::UnixStream::connect(&local).is_ok()
    });
    // On loopback "remote" is this machine, so the test plays the job's server.
    let _serve = Serve::start(remote.clone(), "from-the-job");
    assert_eq!(through(&local), "from-the-job");

    // The lease holds nothing on another master: a session over it works,
    // repeatedly, during the hold.
    for attempt in 0..3 {
        let out = sshd.ssh_via(&default_socket, &["true"]);
        assert!(
            out.status.success(),
            "ssh over another master refused during the lease ({attempt}): {}",
            String::from_utf8_lossy(&out.stderr)
        );
        std::thread::sleep(Duration::from_millis(300));
    }
    assert!(lease.alive());
    let text = lease.output();
    assert!(
        text.contains(&format!("mule lease {id} on live: running")),
        "{text}"
    );
    assert!(
        text.contains(&format!("murmur pick --all  # select mule-{id}")),
        "{text}"
    );
    assert!(
        text.contains(&format!("mule rm --host live {id}")),
        "{text}"
    );

    signal(&lease.child, "TERM");
    let job_file = lease.job_file.clone();
    let (code, stderr) = lease.exit();
    assert_eq!(code, Some(143), "{stderr}");
    assert!(
        job_file.exists(),
        "the handle outlives the lease; mu removes it"
    );
    wait_until("the job to be stopped", Duration::from_secs(15), || {
        job_ids(&sshd).is_empty() && !sessions(&sshd, &tmux.name).contains("mule-")
    });
    assert!(!local.exists(), "the forward is cancelled");
    assert_eq!(records(&sshd), 0);
}

#[test]
fn a_lease_exits_with_the_jobs_code_and_prints_its_final_screen() {
    require_sshd!();
    let sshd = Sshd::start();
    let _master = sshd.open_master_with(&sshd.socket, &MASTER_OPTS);
    let tmux = Tmux::new(&sshd, "lease-done");
    let config = sshd.write_config(&tmux.name);
    let (local, remote) = (sshd.dir.join("l.sock"), sshd.dir.join("r.sock"));

    // The template mu fills: `env {env} sh -c {cmd}`, several words, so each
    // is quoted on its own and `env` applies the assignment.
    let lease = Lease::start(
        &sshd,
        &config,
        "done",
        &[unix_spec(&local, &remote)],
        &[
            "env",
            "K=v w",
            "sh",
            "-c",
            "printf 'env:%s|\\n' \"$K\"; sleep 1; exit 7",
        ],
    );
    let id = lease.id();
    let out = lease.out.clone();
    let (code, stderr) = lease.exit();

    assert_eq!(code, Some(7), "{stderr}");
    let text = std::fs::read_to_string(out).unwrap();
    assert!(
        text.contains("env:v w|"),
        "the final screen, env applied: {text}"
    );
    assert!(text.contains("done 7"), "{text}");
    assert!(!local.exists(), "unforwarded on the way out");
    assert_eq!(records(&sshd), 0);
    clean_jobs(&sshd, &[id]);
}

#[test]
fn a_lease_exits_three_only_at_startup_without_a_master() {
    require_sshd!();
    let sshd = Sshd::start();
    let tmux = Tmux::new(&sshd, "lease-nomaster");
    let config = sshd.write_config(&tmux.name);

    let lease = Lease::start(&sshd, &config, "nomaster", &[], &["sleep 60"]);
    let job_file = lease.job_file.clone();
    let out = lease.out.clone();
    let (code, stderr) = lease.exit();

    assert_eq!(code, Some(3), "{stderr}");
    assert!(stderr.contains("ssh -MNf"), "{stderr}");
    assert!(std::fs::read_to_string(out).unwrap().is_empty());
    // Named before the dispatch was refused; `rm` of it is a no-op.
    let id = std::fs::read_to_string(&job_file).unwrap();
    assert!(job_ids(&sshd).is_empty());
    let _master = sshd.open_master(&sshd.socket);
    assert_eq!(
        sshd.mule(&config, &["rm", id.trim()]).status.code(),
        Some(0)
    );
}

#[test]
fn removing_local_while_leased_is_re_forwarded_on_a_later_tick() {
    require_sshd!();
    let sshd = Sshd::start();
    let _master = sshd.open_master_with(&sshd.socket, &MASTER_OPTS);
    let tmux = Tmux::new(&sshd, "lease-rmlocal");
    let config = sshd.write_config(&tmux.name);
    let (local, remote) = (sshd.dir.join("l.sock"), sshd.dir.join("r.sock"));
    let _serve = Serve::start(remote.clone(), "back");

    let mut lease = Lease::start(
        &sshd,
        &config,
        "rmlocal",
        &[unix_spec(&local, &remote)],
        &["sleep 60"],
    );
    let id = lease.id();
    assert_eq!(through(&local), "back");

    std::fs::remove_file(&local).unwrap();
    wait_until(
        "the lease to re-forward LOCAL",
        Duration::from_secs(15),
        || std::os::unix::net::UnixStream::connect(&local).is_ok(),
    );
    assert_eq!(through(&local), "back");
    assert!(lease.alive());

    let _ = sshd.mule(&config, &["rm", &id]);
}

#[test]
fn a_lease_survives_a_master_restart_and_re_forwards_on_the_new_pid() {
    require_sshd!();
    let sshd = Sshd::start();
    let master = sshd.open_master_with(&sshd.socket, &MASTER_OPTS);
    let tmux = Tmux::new(&sshd, "lease-restart");
    let config = sshd.write_config(&tmux.name);
    let (local, remote) = (sshd.dir.join("l.sock"), sshd.dir.join("r.sock"));
    let _serve = Serve::start(remote.clone(), "alive");

    let mut lease = Lease::start(
        &sshd,
        &config,
        "restart",
        &[unix_spec(&local, &remote)],
        &["sleep 60"],
    );
    let id = lease.id();
    assert_eq!(through(&local), "alive");

    // `ssh -O exit`: the master and every forward it held are gone.
    drop(master);
    wait_until("the hand-back line", Duration::from_secs(15), || {
        lease.output().contains("is gone; a human must")
    });
    std::thread::sleep(Duration::from_secs(2));
    assert!(lease.alive(), "a lost master must not end the lease");
    assert_eq!(
        lease.output().matches("is gone; a human must").count(),
        1,
        "said once"
    );

    let _master = sshd.open_master_with(&sshd.socket, &MASTER_OPTS);
    wait_until(
        "the forward on the new master",
        Duration::from_secs(20),
        || through_once(&local) == "alive",
    );
    assert!(lease.alive());
    assert_eq!(stdout(&sshd.mule(&config, &["poll", &id])), "running");

    let _ = sshd.mule(&config, &["rm", &id]);
}

/// One connect-and-read, no retry: `through` retries for 5s, which would hide
/// the moment the forward comes back.
fn through_once(local: &std::path::Path) -> String {
    use std::io::Read;
    let Ok(mut stream) = std::os::unix::net::UnixStream::connect(local) else {
        return String::new();
    };
    let _ = stream.set_read_timeout(Some(Duration::from_secs(2)));
    let mut text = String::new();
    let _ = stream.read_to_string(&mut text);
    text
}

#[test]
fn kill_9_of_a_lease_leaves_the_job_named_and_a_new_forward_of_local_works() {
    require_sshd!();
    let sshd = Sshd::start();
    let _master = sshd.open_master_with(&sshd.socket, &MASTER_OPTS);
    let tmux = Tmux::new(&sshd, "lease-kill9");
    let config = sshd.write_config(&tmux.name);
    let (local, remote) = (sshd.dir.join("l.sock"), sshd.dir.join("r.sock"));
    let _serve = Serve::start(remote.clone(), "still");
    let spec = unix_spec(&local, &remote);

    let mut first = Lease::start(
        &sshd,
        &config,
        "kill9",
        std::slice::from_ref(&spec),
        &["sleep 60"],
    );
    let id = first.id();
    assert_eq!(through(&local), "still");
    first.child.kill().unwrap();
    first.child.wait().unwrap();

    // SIGKILL cannot be handled: the job keeps running.
    std::thread::sleep(Duration::from_millis(500));
    assert_eq!(stdout(&sshd.mule(&config, &["poll", &id])), "running");

    // A new lease forwarding the same LOCAL replaces the stale listener.
    let second = Lease::start(
        &sshd,
        &config,
        "kill9b",
        std::slice::from_ref(&spec),
        &["sleep 60"],
    );
    let second_id = second.id();
    wait_until(
        "the second lease's forward",
        Duration::from_secs(10),
        || records(&sshd) == 1 && through_once(&local) == "still",
    );
    signal(&second.child, "TERM");
    assert_eq!(second.exit().0, Some(143));

    // The job file is what lets mu's stop template end the first job.
    let named = std::fs::read_to_string(sshd.dir.join("kill9.job")).unwrap();
    assert_eq!(named.trim(), id);
    let out = sshd.mule(&config, &["rm", named.trim()]);
    assert_eq!(
        out.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(!job_state_dir(&sshd, &id).exists());
    assert!(!sessions(&sshd, &tmux.name).contains(&format!("mule-{id}")));
    wait_until(
        "the second job to be stopped",
        Duration::from_secs(15),
        || !job_state_dir(&sshd, &second_id).exists(),
    );
}

/// A private local tmux server, standing in for the pane mu runs a lease in.
/// Never the developer's server or config; torn down (socket file included)
/// on drop.
struct Pane {
    name: String,
    /// Asked for while the server lives: killing the only pane ends the server,
    /// and a dead server cannot say where its socket file is.
    socket: Option<PathBuf>,
}

impl Pane {
    fn new(tag: &str) -> Self {
        Self {
            name: format!("mule-l3p-{}-{tag}", std::process::id()),
            socket: None,
        }
    }

    fn remember_socket(&mut self) {
        let out = self.tmux(&["display-message", "-p", "#{socket_path}"]);
        let path = String::from_utf8_lossy(&out.stdout).trim().to_string();
        self.socket = (!path.is_empty()).then(|| PathBuf::from(path));
    }

    fn tmux(&self, args: &[&str]) -> std::process::Output {
        std::process::Command::new("tmux")
            .args(["-L", &self.name, "-f", "/dev/null"])
            .args(args)
            .output()
            .unwrap()
    }
}

impl Drop for Pane {
    fn drop(&mut self) {
        let _ = self.tmux(&["kill-server"]);
        if let Some(path) = &self.socket {
            let _ = std::fs::remove_file(path);
        }
    }
}

/// An `ssh` shim that logs `s` when an ssh starts and `e` when it ends, so a
/// test can see that nothing is in flight and how long since anything ran.
fn activity_ssh(sshd: &Sshd, log: &std::path::Path) -> PathBuf {
    use std::os::unix::fs::PermissionsExt;
    let dir = sshd.dir.join("activity");
    std::fs::create_dir_all(&dir).unwrap();
    let shim = dir.join("ssh");
    std::fs::write(
        &shim,
        format!(
            "#!/bin/sh
echo s >> '{log}'
'{ssh}' \"$@\"\nrc=$?\necho e >> '{log}'\nexit $rc\n",
            log = log.display(),
            ssh = sshd.dir.join("ssh").display()
        ),
    )
    .unwrap();
    std::fs::set_permissions(&shim, std::fs::Permissions::from_mode(0o755)).unwrap();
    dir
}

/// Which `ssh` the lease in the pane runs.
#[derive(Clone, Copy)]
enum Shim {
    /// Every probe sleeps 5s after writing its pid to the flag file.
    SlowProbe,
    /// Every ssh is logged to the flag file as it starts and ends.
    Activity,
}

/// Run a lease in a tmux pane, kill the pane when `ready` says so, and check
/// what a pane death must leave behind: exit 129 (no panic), the job's session
/// and state gone, the forward cancelled, LOCAL gone.
///
/// `ready` gets the pane (to send it keys) and the shim's flag file.
fn kill_pane_ends_the_lease_and_its_job(
    tag: &str,
    shim: Shim,
    ready: &dyn Fn(&Pane, &std::path::Path) -> bool,
) {
    require_sshd!();
    if std::process::Command::new("tmux")
        .arg("-V")
        .output()
        .is_err()
    {
        eprintln!("skipping: tmux is not on PATH");
        return;
    }
    let sshd = Sshd::start();
    let _master = sshd.open_master_with(&sshd.socket, &MASTER_OPTS);
    let tmux = Tmux::new(&sshd, tag);
    let config = sshd.write_config(&tmux.name);
    let (local, remote) = (sshd.dir.join("l.sock"), sshd.dir.join("r.sock"));
    let _serve = Serve::start(remote.clone(), "pane");
    let job_file = sshd.dir.join("pane.job");
    let flag = sshd.dir.join("shim-flag");
    let shim_dir = match shim {
        Shim::SlowProbe => slow_probe_ssh(&sshd, &flag),
        Shim::Activity => activity_ssh(&sshd, &flag),
    };
    let path = format!("{}:{}", shim_dir.display(), sshd.path_env());
    let status = sshd.dir.join("status");
    let stderr = sshd.dir.join("stderr");

    // Two shells, as measured on macOS: the pane's own process (the session
    // leader) dies of the hangup, which is what makes the kernel hang up the
    // foreground group, lease included. The inner shell traps HUP only to
    // outlive the pane and record the lease's exit status. A trapping session
    // leader would survive, and nothing would hang up the lease at all. The
    // trailing `; :` stops `sh -c` from exec'ing into the inner shell, which
    // would make it the leader again.
    let wrap = sshd.dir.join("wrap.sh");
    std::fs::write(
        &wrap,
        format!(
            "trap 'h=1' HUP\n\
             export PATH='{path}' XDG_STATE_HOME='{state}'\n\
             '{mule}' --config '{config}' run --lease --human --job-file '{job}' \
               --forward-unix '{spec}' -- \
               'i=0; while :; do i=$((i+1)); echo line $i; sleep 0.2; done' 2>'{stderr}'\n\
             echo $? > '{status}'\n",
            state = sshd.state_root.display(),
            mule = env!("CARGO_BIN_EXE_mule"),
            config = config.display(),
            job = job_file.display(),
            spec = unix_spec(&local, &remote),
            stderr = stderr.display(),
            status = status.display(),
        ),
    )
    .unwrap();
    let mut pane = Pane::new(tag);
    let started = pane.tmux(&[
        "new-session",
        "-d",
        "-s",
        "lease",
        "-x",
        "120",
        "-y",
        "40",
        &format!("sh -c \"sh '{}'; :\"", wrap.display()),
    ]);
    assert!(
        started.status.success(),
        "{}",
        String::from_utf8_lossy(&started.stderr)
    );
    pane.remember_socket();

    wait_until("the job file", Duration::from_secs(10), || {
        std::fs::read_to_string(&job_file).is_ok_and(|id| id.ends_with('\n'))
    });
    let id = std::fs::read_to_string(&job_file)
        .unwrap()
        .trim()
        .to_string();
    wait_until("the forward", Duration::from_secs(10), || {
        records(&sshd) == 1
    });
    wait_until(
        "the moment to kill the pane",
        Duration::from_secs(20),
        || ready(&pane, &flag),
    );

    let killed = pane.tmux(&["kill-pane", "-t", "lease"]);
    assert!(
        killed.status.success(),
        "{}",
        String::from_utf8_lossy(&killed.stderr)
    );

    wait_until("the lease's exit status", Duration::from_secs(10), || {
        std::fs::read_to_string(&status).is_ok_and(|s| s.ends_with('\n'))
    });
    let err = std::fs::read_to_string(&stderr).unwrap_or_default();
    assert_eq!(
        std::fs::read_to_string(&status).unwrap().trim(),
        "129",
        "a panic on the dead tty would be 101: {err}"
    );
    assert!(!err.contains("panicked"), "{err}");
    // `rm` removes the state dir, so no rc 137 is left to read: absence is
    // the proof the stop ran.
    wait_until(
        "the job's session and state to be gone",
        Duration::from_secs(20),
        || {
            !job_state_dir(&sshd, &id).exists()
                && !sessions(&sshd, &tmux.name).contains(&format!("mule-{id}"))
        },
    );
    assert!(!local.exists(), "LOCAL is gone");
    assert_eq!(records(&sshd), 0, "the forward was cancelled and forgotten");
    // And the master's table is not left holding it: a fresh forward of the
    // same LOCAL to another REMOTE is not a wedge.
    let other = sshd.dir.join("r2.sock");
    let _serve2 = Serve::start(other.clone(), "fresh");
    let out = sshd.mule(&config, &["forward", "--unix", &unix_spec(&local, &other)]);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(through(&local), "fresh");
}

/// The quiet that proves the lease is stuck in a write: longer than the hold
/// loop's longest pause (5s) plus margin. Every tick starts with an `ssh -O
/// check`, so a lease that is neither in an ssh nor in its pause runs one at
/// least every 5s; with nothing in flight and nothing run for this long, the
/// only place left for it to be is a blocked write to the pane.
const STUCK_IN_WRITE: Duration = Duration::from_millis(6500);

#[test]
fn kill_pane_mid_print_stops_the_job_and_cancels_the_forward_without_a_panic() {
    // XOFF (C-s) freezes the pane's tty: output queues and, once the queue is
    // full, the lease's next write blocks in the kernel (measured: a writer in
    // a frozen tmux pane makes no progress until C-q). The job prints every
    // 200ms, so every mirrored screen differs and the lease writes ~5KB each
    // second -- the queue fills within a few ticks. The pane is killed only
    // once the activity log shows the lease is stuck there, so the hangup
    // lands on a write in progress, not on a sleep or a probe.
    let frozen: std::cell::Cell<Option<std::time::Instant>> = std::cell::Cell::default();
    let seen: std::cell::RefCell<(String, std::time::Instant)> =
        std::cell::RefCell::new((String::new(), std::time::Instant::now()));
    kill_pane_ends_the_lease_and_its_job("lease-kpprint", Shim::Activity, &|pane, log| {
        if frozen.get().is_none() {
            // Let the mirror get going (screens are being written) first.
            std::thread::sleep(Duration::from_millis(1500));
            let out = pane.tmux(&["send-keys", "-t", "lease", "C-s"]);
            assert!(
                out.status.success(),
                "{}",
                String::from_utf8_lossy(&out.stderr)
            );
            frozen.set(Some(std::time::Instant::now()));
            return false;
        }
        let text = std::fs::read_to_string(log).unwrap_or_default();
        let mut seen = seen.borrow_mut();
        if text != seen.0 {
            *seen = (text, std::time::Instant::now());
            return false;
        }
        let in_flight = seen.0.matches('s').count() != seen.0.matches('e').count();
        !in_flight && seen.1.elapsed() >= STUCK_IN_WRITE
    });
}

#[test]
fn kill_pane_while_holding_stops_the_job_and_cancels_the_forward() {
    // The ordinary hangup: the pane dies at whatever moment the lease is in,
    // almost always its pause between ticks.
    kill_pane_ends_the_lease_and_its_job("lease-kphold", Shim::Activity, &|_, _| {
        std::thread::sleep(Duration::from_millis(2500));
        true
    });
}

#[test]
fn kill_pane_mid_probe_abandons_the_probe_and_still_stops_the_job() {
    kill_pane_ends_the_lease_and_its_job("lease-kpprobe", Shim::SlowProbe, &|_, flag| {
        std::fs::read_to_string(flag).is_ok_and(|pid| !pid.trim().is_empty())
    });
}
