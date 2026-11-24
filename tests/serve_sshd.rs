//! `mule serve --stdio`, layer 3: the real binary against a loopback `sshd`
//! with `MaxSessions 1`, where every claim about the only session can be
//! measured. Each case proves that something which could hold the session
//! -- a dead caller, a cancelled op, a wedged master, a caller that stops
//! reading -- releases it, by taking that session with a bare ssh afterwards.
//!
//! The bare ssh probes run outside mule's ticket lock on purpose: they
//! are the measurement, not a mule caller.

mod common;

use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::time::{Duration, Instant};

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as B64;
use common::local::Fifo;
use common::sshd::{Master, Sshd};
use serde_json::{Value, json};

struct Serve {
    child: Child,
    w: Option<ChildStdin>,
    r: BufReader<ChildStdout>,
}

impl Serve {
    fn send(&mut self, v: Value) {
        writeln!(self.w.as_mut().unwrap(), "{v}").unwrap();
    }

    fn reply(&mut self) -> Value {
        let mut line = String::new();
        self.r.read_line(&mut line).unwrap();
        serde_json::from_str(&line).unwrap_or_else(|e| panic!("{e}: {line:?}"))
    }

    /// Wait for exit within `limit`, returning the code.
    fn exit_within(&mut self, limit: Duration) -> Option<i32> {
        let t = Instant::now();
        loop {
            if let Some(s) = self.child.try_wait().unwrap() {
                return s.code();
            }
            assert!(t.elapsed() < limit, "serve still running after {limit:?}");
            std::thread::sleep(Duration::from_millis(20));
        }
    }
}

impl Drop for Serve {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn setup() -> (Sshd, Master, PathBuf) {
    let sshd = Sshd::start();
    let master = sshd.open_master(&sshd.socket);
    let config = sshd.write_config(&format!("mule-serve-{}", std::process::id()));
    (sshd, master, config)
}

fn serve(sshd: &Sshd, config: &Path) -> Serve {
    serve_with(sshd, config, &[])
}

fn serve_with(sshd: &Sshd, config: &Path, env: &[(&str, &str)]) -> Serve {
    let mut child = Command::new(env!("CARGO_BIN_EXE_mule"))
        .arg("--config")
        .arg(config)
        .args(["serve", "--stdio"])
        .envs(env.iter().copied())
        .env("PATH", sshd.path_env())
        .env("XDG_STATE_HOME", &sshd.state_root)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let w = child.stdin.take();
    let r = BufReader::new(child.stdout.take().unwrap());
    Serve { child, w, r }
}

/// Is the master's one session free? A bare `true` over mule's socket, with
/// no fallback to a connection of its own ([`Sshd::mux_true`]).
fn session_free(sshd: &Sshd) -> bool {
    sshd.mux_true(&sshd.socket, Duration::from_secs(5))
}

/// Wait up to `limit` for the session to be free; returns how long it took.
fn session_frees_within(sshd: &Sshd, limit: Duration) -> Duration {
    let t = Instant::now();
    while !session_free(sshd) {
        assert!(
            t.elapsed() < limit,
            "the only session was still held after {limit:?}"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    t.elapsed()
}

/// A blocked read of a fresh FIFO: `(fifo, request)`. The [`Fifo`] releases
/// the op's reader when dropped, so a failing test leaves no `head` blocked
/// in the host's `open`.
fn stuck_read(dir: &Path, tag: &str, id: u64) -> (Fifo, Value) {
    let f = Fifo::new(dir.join(format!("fifo-{tag}")));
    let req = json!({"op":"read","id":id,"path":f.path()});
    (f, req)
}

/// Wait for the op's `head` to open the FIFO, and hold its write end. Held,
/// not opened and closed: closing it would hand the reader EOF and end the
/// op, and a test would see that as the kill it is checking for.
fn reading(f: &Path) -> std::fs::File {
    use std::os::unix::fs::OpenOptionsExt;
    let t = Instant::now();
    loop {
        // ENXIO until a reader has it open.
        if let Ok(w) = std::fs::OpenOptions::new()
            .write(true)
            .custom_flags(libc::O_NONBLOCK)
            .open(f)
        {
            return w;
        }
        assert!(t.elapsed() < Duration::from_secs(5), "the op never started");
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// The op's reader is gone within `limit`: a write to the held end fails
/// with EPIPE once nothing reads (Rust ignores SIGPIPE). A byte the live
/// reader takes does not end it: it waits for a megabyte.
fn reader_gone_within(w: &mut std::fs::File, limit: Duration) {
    let t = Instant::now();
    loop {
        match w.write(b"x") {
            Err(e) if e.kind() == std::io::ErrorKind::BrokenPipe => return,
            _ => {}
        }
        assert!(t.elapsed() < limit, "the op still runs after {limit:?}");
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// Holds the master's only session from a bare ssh until `release` exists
/// (or 60s pass, so a failed test cannot leave it holding), and returns once
/// the holder's command runs.
///
/// Gated, not a `sleep 3`: the probe then cannot race the holder's own end,
/// and the release is an event the test makes rather than one it guesses.
fn hold_the_session(sshd: &Sshd) -> (Child, PathBuf) {
    let flag = sshd.dir.join("held");
    let release = sshd.dir.join("release");
    let holder = Command::new(sshd.dir.join("ssh"))
        .arg("-S")
        .arg(&sshd.socket)
        .args(["-o", "BatchMode=yes", "127.0.0.1"])
        .arg(format!(
            "touch '{}'; i=0; while [ ! -e '{}' ] && [ $i -lt 1200 ]; do sleep 0.05; i=$((i+1)); done",
            flag.display(),
            release.display()
        ))
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let t = Instant::now();
    while !flag.exists() {
        assert!(
            t.elapsed() < Duration::from_secs(10),
            "the holder never ran"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    (holder, release)
}

/// Only a hang guard: what the cases below wait for is an event (a reply, a
/// closed channel, a gone reader), and a correct build meets it in well under
/// a second.
const WATCHDOG: Duration = Duration::from_secs(30);

/// The oracle every case here rests on: it must see a held session.
#[test]
fn the_session_probe_sees_a_held_session() {
    require_sshd!();
    let (sshd, _master, _config) = setup();
    let (mut holder, release) = hold_the_session(&sshd);
    let held = session_free(&sshd);
    // Let the holder's command end, and wait for its ssh to exit: that is the
    // channel closed. Killing the local ssh instead left the remote command
    // holding the session until its own `sleep 3` ended, and the free check
    // then raced macOS channel teardown (CI, macos-latest: still held at 3s).
    std::fs::write(&release, "").unwrap();
    let t = Instant::now();
    while holder.try_wait().unwrap().is_none() {
        if t.elapsed() > WATCHDOG {
            let _ = holder.kill();
            panic!("the holder did not end after its release");
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(!held, "the probe reported a held session free");
    session_frees_within(&sshd, WATCHDOG);
}

#[test]
fn ops_round_trip_on_a_capped_host_and_leave_the_session_free() {
    require_sshd!();
    let (sshd, _master, config) = setup();
    let mut s = serve(&sshd, &config);
    let path = sshd.dir.join("w.bin");
    let data: Vec<u8> = (0..200 * 1024).map(|i| (i % 251) as u8).collect();
    s.send(json!({"op":"write","id":1,"path":path,"data_b64":B64.encode(&data)}));
    assert_eq!(s.reply()["rc"], 0);
    assert_eq!(std::fs::read(&path).unwrap(), data);
    s.send(json!({"op":"read","id":2,"path":path}));
    let v = s.reply();
    assert_eq!(B64.decode(v["stdout_b64"].as_str().unwrap()).unwrap(), data);
    s.send(json!({"op":"stat","id":3,"path":path}));
    let v = s.reply();
    assert_eq!(v["rc"], 0, "{v}");
    let (f, req) = stuck_read(&sshd.dir, "timeout", 4);
    s.send(req);
    let mut w = reading(f.path());
    let v = s.reply();
    assert_eq!(v["kind"], "timeout", "{v}");
    reader_gone_within(&mut w, Duration::from_secs(1));
    assert!(
        session_free(&sshd),
        "serve held the session between batches"
    );
}

#[test]
fn two_callers_share_the_one_session_without_a_refusal() {
    require_sshd!();
    let (sshd, _master, config) = setup();
    let mut a = serve(&sshd, &config);
    let mut b = serve(&sshd, &config);
    for i in 0..10 {
        a.send(json!({"op":"stat","id":i,"path":sshd.dir}));
        b.send(json!({"op":"stat","id":i,"path":sshd.dir}));
    }
    for s in [&mut a, &mut b] {
        for _ in 0..10 {
            let v = s.reply();
            assert_eq!(v["rc"], 0, "a refused session reached a caller: {v}");
        }
    }
}

#[test]
fn a_sigkilled_caller_frees_the_only_session_and_its_op_never_lands() {
    require_sshd!();
    let (sshd, _master, config) = setup();
    let mut s = serve(&sshd, &config);
    let (f, req) = stuck_read(&sshd.dir, "kill", 1);
    s.send(req);
    let mut w = reading(f.path());
    s.child.kill().unwrap();
    s.child.wait().unwrap();
    session_frees_within(&sshd, Duration::from_secs(3));
    reader_gone_within(&mut w, Duration::from_secs(1));
}

#[test]
fn caller_eof_frees_the_only_session_and_serve_exits() {
    require_sshd!();
    let (sshd, _master, config) = setup();
    let mut s = serve(&sshd, &config);
    let (f, req) = stuck_read(&sshd.dir, "eof", 1);
    s.send(req);
    let mut w = reading(f.path());
    drop(s.w.take());
    assert_eq!(s.exit_within(Duration::from_secs(4)), Some(0));
    session_frees_within(&sshd, Duration::from_secs(3));
    reader_gone_within(&mut w, Duration::from_secs(1));
}

#[test]
fn cancel_stops_the_op_on_the_same_session() {
    require_sshd!();
    let (sshd, _master, config) = setup();
    // The subject is the cancel, not the deadline: with the host's 1s already
    // ticking when the reader is seen, a delayed test thread lost to a correct
    // timeout. The longest deadline the seam allows takes that race away; the
    // deadline itself is covered by the timeout cases.
    let secs = mule::serve::TEST_OP_SECS_MAX.to_string();
    let mut s = serve_with(&sshd, &config, &[(mule::serve::TEST_OP_SECS_ENV, &secs)]);
    let (f, req) = stuck_read(&sshd.dir, "cancel", 1);
    s.send(req);
    let mut w = reading(f.path());
    s.send(json!({"op":"cancel","id":1}));
    let v = s.reply();
    assert_eq!(
        (&v["kind"], &v["cancelled"]),
        (&json!("cancelled"), &json!(true)),
        "{v}"
    );
    // Released, not merely replied to: the op's reader is gone and the
    // session can be taken. Each within a hang guard, not a latency bound.
    reader_gone_within(&mut w, WATCHDOG);
    session_frees_within(&sshd, WATCHDOG);
}

#[test]
fn a_caller_that_stops_reading_does_not_hold_the_session() {
    require_sshd!();
    let (sshd, _master, config) = setup();
    let mut s = serve(&sshd, &config);
    // ~6MB of replies into a pipe nobody reads.
    let big = sshd.dir.join("big");
    std::fs::write(&big, vec![0u8; 1_000_000]).unwrap();
    for i in 0..6 {
        s.send(json!({"op":"read","id":i,"path":big}));
    }
    std::thread::sleep(Duration::from_secs(3));
    assert!(session_free(&sshd), "a stalled reader kept the session");
    drop(s.w.take());
    assert_eq!(s.exit_within(Duration::from_secs(6)), Some(0));
}

#[test]
fn a_master_that_exits_makes_serve_exit_3() {
    require_sshd!();
    let (sshd, master, config) = setup();
    let mut s = serve(&sshd, &config);
    s.send(json!({"op":"stat","id":1,"path":sshd.dir}));
    assert_eq!(s.reply()["rc"], 0);
    drop(master); // ssh -O exit
    s.send(json!({"op":"stat","id":2,"path":sshd.dir}));
    let v = s.reply();
    assert_eq!(v["kind"], "master_gone", "{v}");
    assert_eq!(s.exit_within(Duration::from_secs(5)), Some(3));
}

fn master_pid(sshd: &Sshd) -> i32 {
    let out = Command::new(sshd.dir.join("ssh"))
        .arg("-S")
        .arg(&sshd.socket)
        .args(["-O", "check", "127.0.0.1"])
        .output()
        .unwrap();
    let text = String::from_utf8_lossy(&out.stderr);
    let pid = text.split("pid=").nth(1).unwrap();
    pid[..pid.find(')').unwrap()].parse().unwrap()
}

/// Resumes a stopped master even if the test unwinds, so `Master`'s drop
/// (`ssh -O exit`) cannot hang on it.
struct Resume(i32);

impl Drop for Resume {
    fn drop(&mut self) {
        // SAFETY: kill(2) on a pid we stopped.
        unsafe { libc::kill(self.0, libc::SIGCONT) };
    }
}

#[test]
fn a_wedged_master_costs_one_bounded_failure_and_the_session_comes_back() {
    require_sshd!();
    let (sshd, _master, config) = setup();
    let mut s = serve(&sshd, &config);
    s.send(json!({"op":"stat","id":1,"path":sshd.dir}));
    assert_eq!(s.reply()["rc"], 0);
    let pid = master_pid(&sshd);
    let (f, req) = stuck_read(&sshd.dir, "wedged", 2);
    s.send(req);
    let _w = reading(f.path());
    // SAFETY: kill(2) on the test's own master.
    unsafe { libc::kill(pid, libc::SIGSTOP) };
    let resume = Resume(pid);
    let t = Instant::now();
    let v = s.reply();
    let waited = t.elapsed();
    assert!(v["error"].is_string(), "{v}");
    // The local hard kill (15s) plus the 2s -O check bound.
    assert!(waited < Duration::from_secs(20), "{waited:?}");
    // A master that cannot answer -O check is a master gone: exit 3.
    assert_eq!(s.exit_within(Duration::from_secs(5)), Some(3));
    drop(resume);
    // The remote bounds ran while the master was stopped, so the session
    // frees as soon as the master tears the dead client's channel down.
    session_frees_within(&sshd, Duration::from_secs(8));
}
