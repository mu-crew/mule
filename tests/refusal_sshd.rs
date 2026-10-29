//! Test layer 3 for refusal recovery, driven through the real `Ssh` transport.
//!
//! Separate from `local_sshd.rs` because it calls `mule::transport::Ssh`
//! in-process: that is the only way to hand the transport a script whose
//! remote stderr *spoofs* a refusal, and to observe the retry loop directly.
//! `Ssh` runs `ssh` from `PATH` and the lock reads `XDG_STATE_HOME`, so this
//! binary sets both once, in its single test, before any thread exists.
//!
//! Every case is a measurement from the design review (lease_kill9_slot,
//! refutations r1-r3), kept as an assertion:
//! - a remote script that prints the refusal text and exits 255 ran once and is
//!   never re-run (remote stderr has no provenance; `ssh -E` does);
//! - under a held session, and with no master at all, the script never runs
//!   (`ProxyCommand=/usr/bin/false` kills ssh's direct-connect fallback; with
//!   `ProxyCommand=none` the fallback ran it -- shown here as the control);
//! - a session held ~2s is waited out, and the script runs exactly once;
//! - a stopped master fails `-O check` within its bound instead of hanging.

mod common;

use std::path::Path;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use common::sshd::Sshd;
use mule::config::{Config, Host};
use mule::errors::MuleError;
use mule::transport::{Ssh, Transport, run_within};

/// What OpenSSH's mux client logs when the master refuses a session.
const REFUSAL: &str =
    "mux_client_request_session: session request failed: Session open refused by peer";

fn host(sshd: &Sshd) -> Host {
    Config::parse(&format!(
        "[hosts.refusal{}]\ntarget = \"127.0.0.1\"\nsocket = \"{}\"\n",
        sshd.port,
        sshd.socket.display()
    ))
    .unwrap()
    .host(None)
    .unwrap()
    .clone()
}

fn lines(path: &Path) -> usize {
    std::fs::read_to_string(path).map_or(0, |s| s.lines().count())
}

/// Hold the master's one session for `secs`, returning once it is held.
fn hold(sshd: &Sshd, secs: u32, tag: &str) -> std::process::Child {
    let flag = sshd.dir.join(format!("held-{tag}"));
    let child = Command::new(sshd.dir.join("ssh"))
        .arg("-S")
        .arg(&sshd.socket)
        .args(["-o", "BatchMode=yes", "127.0.0.1"])
        .arg(format!(
            "touch '{flag}'; sleep {secs}; touch '{flag}.done'",
            flag = flag.display()
        ))
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(30);
    while !flag.exists() {
        assert!(
            Instant::now() < deadline,
            "the holder never got the session"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    child
}

fn master_pid(sshd: &Sshd) -> i32 {
    let out = Command::new(sshd.dir.join("ssh"))
        .arg("-S")
        .arg(&sshd.socket)
        .args(["-O", "check", "127.0.0.1"])
        .output()
        .unwrap();
    let text = String::from_utf8_lossy(&out.stderr);
    let pid = text
        .split("pid=")
        .nth(1)
        .and_then(|rest| rest.split(')').next())
        .unwrap_or_else(|| panic!("no pid in {text:?}"));
    pid.trim().parse().unwrap()
}

fn kill(pid: i32, signal: &str) {
    assert!(
        Command::new("kill")
            .args([signal, &pid.to_string()])
            .status()
            .unwrap()
            .success()
    );
}

/// One test, so the process environment is set before any other thread runs.
#[test]
fn refusals_are_retried_only_when_proven_and_never_execute_twice() {
    require_sshd!();
    let sshd = Sshd::start();
    // Safe: the only test in this binary, before it spawns any thread.
    unsafe {
        std::env::set_var("PATH", sshd.path_env());
        std::env::set_var("XDG_STATE_HOME", sshd.dir.join("local-state"));
    }
    let _master = sshd.open_master_with(&sshd.socket, &["-o", "StreamLocalBindUnlink=yes"]);
    let host = host(&sshd);

    a_spoofed_refusal_on_remote_stderr_is_not_retried(&sshd, &host);
    a_held_session_is_waited_out_and_the_script_runs_once(&sshd, &host);
    a_refusal_never_falls_back_to_a_direct_connection(&sshd, &host);
    a_stopped_master_fails_the_check_within_its_bound(&sshd, &host);
    no_master_never_falls_back_to_a_direct_connection(&sshd, &host, _master);
}

fn a_spoofed_refusal_on_remote_stderr_is_not_retried(sshd: &Sshd, host: &Host) {
    // The refuter's fixture: an accepted script prints the refusal and exits
    // 255. Judged by remote stderr, the old classifier called it a refusal
    // with a live master and no stdout -- retryable -- and re-ran it.
    let marker = sshd.dir.join("spoof-executions");
    let script = format!(
        "echo execution >> '{}'; printf '{REFUSAL}\\n' >&2; exit 255",
        marker.display()
    );
    let out = Ssh
        .run(host, &script)
        .expect("a script that ran is not an error");
    assert_eq!(out.code, 255);
    assert!(out.stderr.contains(REFUSAL), "{}", out.stderr);
    assert_eq!(lines(&marker), 1, "the spoofed refusal was retried");
}

fn a_held_session_is_waited_out_and_the_script_runs_once(sshd: &Sshd, host: &Host) {
    let marker = sshd.dir.join("recovered-executions");
    let mut holder = hold(sshd, 2, "recover");
    let started = Instant::now();
    let out = Ssh
        .run(
            host,
            &format!("echo once >> '{}'; echo ok", marker.display()),
        )
        .unwrap_or_else(|e| panic!("refused after {:?}: {e:#}", started.elapsed()));
    assert_eq!(String::from_utf8_lossy(&out.stdout).trim(), "ok");
    assert_eq!(lines(&marker), 1);
    // Proves it was refused first and recovered, not handed a free channel:
    // the holder's last act, before its session closes, already happened.
    assert!(
        sshd.dir.join("held-recover.done").exists(),
        "the script ran while the holder still held the only session"
    );
    assert!(started.elapsed() < mule::transport::REFUSAL_RECOVERY);
    let _ = holder.wait();
}

fn a_refusal_never_falls_back_to_a_direct_connection(sshd: &Sshd, host: &Host) {
    let marker = sshd.dir.join("fallback-executions");
    let script = format!("echo ran >> '{}'", marker.display());
    let mut holder = hold(sshd, 30, "fallback");

    // Control: the fixture CAN detect a fallback. With mule's old
    // `ProxyCommand=none`, ssh's own fallback after the refusal connects
    // directly (the shim supplies the key and port) and runs the script.
    let mut args = mule::transport::run_args(host, &script);
    let proxy = args
        .iter()
        .position(|a| a == "ProxyCommand=/usr/bin/false")
        .expect("mule's args set the fail-closed proxy");
    args[proxy] = "ProxyCommand=none".into();
    let control = Command::new("ssh")
        .args(&args)
        .stdin(Stdio::null())
        .output()
        .unwrap();
    assert_eq!(
        lines(&marker),
        1,
        "the control did not fall back, so this case proves nothing: {}",
        String::from_utf8_lossy(&control.stderr)
    );

    let error = run_within(&Ssh, host, &script, Duration::from_millis(600)).unwrap_err();
    assert_eq!(
        error.downcast_ref::<MuleError>(),
        Some(&MuleError::SessionChannelBusy),
        "{error:#}"
    );
    assert_eq!(lines(&marker), 1, "mule's call ran outside the master");
    let _ = holder.kill();
    let _ = holder.wait();
}

fn a_stopped_master_fails_the_check_within_its_bound(sshd: &Sshd, host: &Host) {
    // A plain `-O check` against a SIGSTOPped master was still blocked at
    // 12.31s (measured); every verb checks before it queues.
    // On a thread, so a regression fails here instead of hanging the suite.
    let pid = master_pid(sshd);
    kill(pid, "-STOP");
    let started = Instant::now();
    let (tx, rx) = std::sync::mpsc::channel();
    let probe = host.clone();
    std::thread::spawn(move || {
        let _ = tx.send(Ssh.master_alive(&probe));
    });
    let alive = rx.recv_timeout(Duration::from_secs(6));
    let took = started.elapsed();
    kill(pid, "-CONT");
    let alive = alive.expect("-O check on a stopped master is unbounded");
    assert!(!alive);
    assert!(took < Duration::from_secs(4), "{took:?}");
    assert!(took >= mule::transport::CHECK_TIMEOUT, "{took:?}");
    assert!(Ssh.master_alive(host), "the resumed master answers again");
}

fn no_master_never_falls_back_to_a_direct_connection(
    sshd: &Sshd,
    host: &Host,
    master: common::sshd::Master,
) {
    drop(master);
    let deadline = Instant::now() + Duration::from_secs(5);
    while sshd.master_alive(&sshd.socket) {
        assert!(Instant::now() < deadline, "master never went away");
        std::thread::sleep(Duration::from_millis(20));
    }
    let marker = sshd.dir.join("no-master-executions");
    let out = Ssh.run(host, &format!("echo ran >> '{}'", marker.display()));
    assert!(out.as_ref().map_or(true, |o| o.code != 0), "{out:?}");
    assert_eq!(lines(&marker), 0, "ran with no master: a direct connection");
}
