//! Test layer 2 for `mule exec`: the real wrapper and the real collect script,
//! on a private tmux server (`-L ... -f /dev/null`), with no ssh.
//!
//! `Fake` answers every script the same way, so it cannot see whether the
//! script exec builds actually splits the streams, caps the log, or removes the
//! job. Here the transport is `sh -c` on this machine and the host's job root
//! is a temp dir, so the scripts run for real.

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

use mule::config::Host;
use mule::exec::{Request, finish, start};
use mule::transport::{MuxOp, Output, Transport};
use mule::wrapper::JobId;

fn tmux_available() -> bool {
    Command::new("tmux").arg("-V").output().is_ok()
}

/// Runs every script locally, with the job root pointed at `state`.
struct Local {
    state: PathBuf,
    /// The longest any one script ran. `Transport::run` holds the ticket for
    /// exactly this long, so it is the channel outage a caller can cause.
    longest: Mutex<Duration>,
}

impl Transport for Local {
    fn run_unlocked(&self, _: &Host, script: &str) -> anyhow::Result<Output> {
        let started = Instant::now();
        let out = Command::new("sh")
            .args(["-c", script])
            .env("XDG_STATE_HOME", &self.state)
            .stdin(Stdio::null())
            .output()?;
        let mut longest = self.longest.lock().unwrap();
        *longest = (*longest).max(started.elapsed());
        Ok(Output {
            stdout: out.stdout,
            stderr: String::from_utf8_lossy(&out.stderr).into_owned(),
            code: out.status.code().unwrap_or(-1),
        })
    }

    fn mux_op(&self, _: &Host, _: MuxOp) -> anyhow::Result<Output> {
        Ok(Output::ok(""))
    }
}

struct Server {
    host: Host,
    transport: Local,
    root: PathBuf,
}

impl Server {
    fn start(max_log_bytes: u64) -> Self {
        static SEQ: AtomicU32 = AtomicU32::new(0);
        let n = SEQ.fetch_add(1, Ordering::Relaxed);
        let suffix = format!("{}-{n}", std::process::id());
        let root = std::env::temp_dir().join(format!("mule-exec-l2-{suffix}"));
        std::fs::create_dir_all(&root).unwrap();
        // The lock directory is keyed off XDG_STATE_HOME too; give the whole
        // binary one private root before any thread reads it.
        static ONCE: std::sync::Once = std::sync::Once::new();
        ONCE.call_once(|| {
            let dir =
                std::env::temp_dir().join(format!("mule-exec-l2-lock-{}", std::process::id()));
            std::fs::create_dir_all(&dir).unwrap();
            unsafe { std::env::set_var("XDG_STATE_HOME", &dir) };
        });
        let host = Host {
            name: format!("exec-l2-{suffix}"),
            target: "local".into(),
            socket: PathBuf::new(),
            tmux_socket: format!("mule-exec-l2-{suffix}"),
            max_running: 4,
            default_cwd: None,
            keep_days: 14,
            max_log_bytes,
            max_job_secs: 0,
        };
        Self {
            host,
            transport: Local {
                state: root.clone(),
                longest: Mutex::new(Duration::ZERO),
            },
            root,
        }
    }

    /// The whole exec, as the CLI composes it, minus signals.
    fn exec(&self, id: &str, cmd: &str, cwd: Option<&str>) -> (i32, String, String) {
        let id: JobId = id.parse().unwrap();
        let request = Request {
            cmd,
            cwd,
            max_secs: None,
        };
        start(&self.transport, &self.host, id.clone(), &request).unwrap();
        let (mut out, mut err) = (Vec::new(), Vec::new());
        let code = finish(
            &self.transport,
            &self.host,
            &id,
            // The real pause: the settle bound is wall-clock.
            &std::thread::sleep,
            &mut out,
            &mut err,
        )
        .unwrap();
        (
            code,
            String::from_utf8(out).unwrap(),
            String::from_utf8(err).unwrap(),
        )
    }

    fn job_dir(&self, id: &str) -> PathBuf {
        self.root.join("mule/jobs").join(id)
    }

    fn has_session(&self, name: &str) -> bool {
        Command::new("tmux")
            .args(["-L", &self.host.tmux_socket, "has-session", "-t", name])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .is_ok_and(|s| s.success())
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        // Ask tmux for the socket path before killing it; fall back to the
        // conventional location when the server already exited with its last
        // job, which leaves the socket file behind.
        let asked = Command::new("tmux")
            .args([
                "-L",
                &self.host.tmux_socket,
                "display-message",
                "-p",
                "#{socket_path}",
            ])
            .output()
            .ok()
            .filter(|o| o.status.success())
            .map(|o| PathBuf::from(String::from_utf8_lossy(&o.stdout).trim()));
        let _ = Command::new("tmux")
            .args(["-L", &self.host.tmux_socket, "kill-server"])
            .stderr(Stdio::null())
            .status();
        let uid = Command::new("id").arg("-u").output().unwrap().stdout;
        let fallback = Path::new(&std::env::var("TMUX_TMPDIR").unwrap_or("/tmp".into()))
            .join(format!("tmux-{}", String::from_utf8_lossy(&uid).trim()))
            .join(&self.host.tmux_socket);
        for path in asked.into_iter().chain([fallback]) {
            let _ = std::fs::remove_file(path);
        }
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

#[test]
fn the_real_wrapper_splits_stdout_and_stderr_and_keeps_the_exit_code() {
    if !tmux_available() {
        eprintln!("skipping: tmux is not on PATH");
        return;
    }
    let server = Server::start(1 << 20);

    let (code, out, err) = server.exec("e20001", "echo to-out; echo to-err >&2; exit 3", None);

    assert_eq!(code, 3);
    assert_eq!(out, "to-out\n", "stdout carries only the command's stdout");
    assert_eq!(err, "to-err\n", "stderr carries only the command's stderr");
}

#[test]
fn the_job_and_its_sessions_are_gone_afterwards() {
    if !tmux_available() {
        return;
    }
    let server = Server::start(1 << 20);

    server.exec("e20002", "echo hi", None);

    assert!(
        !server.job_dir("e20002").exists(),
        "state dir must be removed"
    );
    assert!(!server.has_session("mule-e20002"));
    assert!(!server.has_session("watch-e20002"));
}

#[test]
fn a_bad_cwd_leaves_its_reason_on_the_first_line_of_stderr() {
    if !tmux_available() {
        return;
    }
    let server = Server::start(1 << 20);

    let (code, out, err) = server.exec("e20003", "echo must-not-run", Some("/mule/nonexistent"));

    assert_ne!(code, 0);
    assert!(out.is_empty(), "the command must not have run: {out:?}");
    // The reason, as `ssh host cmd` would give it, and nothing from mule first.
    let first = err.lines().next().unwrap_or_default();
    assert!(first.contains("nonexistent"), "{err:?}");
}

#[test]
fn a_log_past_the_cap_exits_seven_with_the_capped_prefix_on_stdout() {
    if !tmux_available() {
        return;
    }
    let server = Server::start(1024);

    let (code, out, err) = server.exec(
        "e20004",
        "i=0; while [ $i -lt 4000 ]; do echo 0123456789012345678901234567890123456789; i=$((i+1)); done; exit 4",
        None,
    );

    assert_eq!(code, 7, "truncated output must not report the job's code");
    assert_eq!(out.len(), 1024);
    assert!(err.contains("max_log_bytes"), "{err:?}");
    assert!(!server.job_dir("e20004").exists());
}

#[test]
fn an_error_stream_past_the_cap_is_also_truncated_and_exits_seven() {
    if !tmux_available() {
        return;
    }
    let server = Server::start(1024);

    let (code, _, err) = server.exec(
        "e20005",
        "i=0; while [ $i -lt 400 ]; do echo 0123456789012345678901234567890123456789 >&2; i=$((i+1)); done",
        None,
    );

    assert_eq!(code, 7);
    assert!(err.starts_with("0123456789"), "{err:?}");
    assert!(
        err.len() < 1024 + 200,
        "stderr is bounded: {} bytes",
        err.len()
    );
}

#[test]
fn env_prefixed_argv_reaches_the_remote_shell_with_the_env_applied() {
    if !tmux_available() {
        return;
    }
    let server = Server::start(1 << 20);
    let args: Vec<String> = [
        "env",
        "K=v",
        "sh",
        "-c",
        "printf '%s|%s' \"$K\" \"$0\"",
        "a b",
    ]
    .map(String::from)
    .to_vec();
    let command = mule::cli::command_from_args(&args);

    let (code, out, _) = server.exec("e20006", &command, None);

    assert_eq!((code, out.as_str()), (0, "v|a b"));
}

#[test]
fn a_binary_log_survives_the_round_trip() {
    if !tmux_available() {
        return;
    }
    let server = Server::start(1 << 20);

    let id: JobId = "e20007".parse().unwrap();
    start(
        &server.transport,
        &server.host,
        id.clone(),
        &Request {
            cmd: "printf '\\000\\377a\\n'",
            cwd: None,
            max_secs: None,
        },
    )
    .unwrap();
    let mut out = Vec::new();
    finish(
        &server.transport,
        &server.host,
        &id,
        &std::thread::sleep,
        &mut out,
        &mut Vec::new(),
    )
    .unwrap();

    assert_eq!(out, b"\x00\xffa\n");
}

#[test]
fn a_background_child_holding_stdout_is_exit_seven_and_no_locked_call_runs_long() {
    if !tmux_available() {
        return;
    }
    let server = Server::start(1 << 20);

    // The reviewer's repro: rc lands at once, but `sleep 30 &` keeps the log
    // pipeline -- and so the session -- open. This used to hold the ticket for
    // ~14s in the collect and then return rc 0 with an empty stdout.
    let started = Instant::now();
    let (code, out, err) = server.exec("e20008", "sleep 30 & echo hi", None);

    assert_eq!(code, 7, "stdout may be incomplete, so not the job's rc 0");
    assert!(err.contains("output incomplete"), "{err:?}");
    assert!(
        out.is_empty() || out == "hi\n",
        "a prefix of the output, never garbage: {out:?}"
    );
    let longest = *server.transport.longest.lock().unwrap();
    assert!(
        longest < Duration::from_secs(1),
        "a locked call ran {longest:?}; the ticket is held for all of it"
    );
    // The settle bound is 5s of wall clock, probes included; the rest is one
    // collect. What this shows is that exec stops waiting well short of the
    // 30s child. Whether the bound is wall-clock rather than a pause count is
    // pinned deterministically, by probe count, in layer 1
    // (`the_settle_bound_counts_probe_time_not_just_requested_pauses`). A 6s
    // ceiling here measured the machine instead: 7.1s and 8.7s under 3
    // parallel layer-3 suites at load average 54-140.
    let elapsed = started.elapsed();
    assert!(
        elapsed < Duration::from_secs(20),
        "the settle bound is wall-clock: {elapsed:?}"
    );
    // Removed deliberately, background child included.
    assert!(!server.job_dir("e20008").exists());
    assert!(!server.has_session("mule-e20008"));
}

#[test]
fn a_removal_that_fails_is_a_fatal_error_not_the_commands_exit_code() {
    if !tmux_available() {
        return;
    }
    use std::os::unix::fs::PermissionsExt;
    let server = Server::start(1 << 20);
    let id: JobId = "e2000a".parse().unwrap();
    start(
        &server.transport,
        &server.host,
        id.clone(),
        &Request {
            cmd: "true",
            cwd: None,
            max_secs: None,
        },
    )
    .unwrap();
    // A file inside a 0500 directory cannot be unlinked, so `rm -rf` fails
    // partway. (Root ignores modes, so this cannot bite as root.)
    let dir = server.job_dir("e2000a");
    let locked = dir.join("locked");
    std::fs::create_dir_all(&locked).unwrap();
    std::fs::write(locked.join("evidence"), "x").unwrap();
    std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o500)).unwrap();
    let rooted = std::fs::write(locked.join("probe"), "x").is_ok();

    let result = finish(
        &server.transport,
        &server.host,
        &id,
        &std::thread::sleep,
        &mut Vec::new(),
        &mut Vec::new(),
    );

    // Restore so the fixture can clean up.
    std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o700)).unwrap();
    if rooted {
        eprintln!("skipping: modes are not enforced for this user");
        return;
    }
    // Before: Ok(0) with only a stderr line -- mu would read success and
    // delete its handle while the job's state was still on the host.
    let error = result.expect_err("a failed removal must not report the command's rc");
    let text = format!("{error:#}");
    assert!(text.contains("could not remove"), "{text}");
    assert!(text.contains("could not be removed"), "{text}");
    assert_eq!(mule::errors::exit_code(&error), 1);
    assert!(locked.join("evidence").exists(), "evidence must be kept");
}

#[test]
fn an_ordinary_exec_holds_no_locked_call_past_a_second() {
    if !tmux_available() {
        return;
    }
    let server = Server::start(1 << 20);

    let (code, out, _) = server.exec("e20009", "sleep 1; echo late", None);

    assert_eq!((code, out.as_str()), (0, "late\n"));
    let longest = *server.transport.longest.lock().unwrap();
    assert!(longest < Duration::from_secs(1), "{longest:?}");
}
