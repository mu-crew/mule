//! `mule serve --stdio`, layers 1 and 2: the ticket lock on the stdin path,
//! the local bounds of `spawn_fed`, and the batch script itself run under a
//! local `sh` (a `Fake` returns canned output, so it cannot see a wrong
//! script). The binary is driven whole through the `ssh` shim of
//! `tests/common/local.rs`. Real-sshd cases are in `tests/serve_sshd.rs`.

mod common;

use std::io::{BufRead, BufReader, Read, Write};
use std::os::unix::process::CommandExt;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::Result;
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as B64;
use common::local::{Fifo, ShimHost, isolate_state, scratch, wait_until};
use mule::config::{Config, Host};
use mule::serve::{Ended, MAX_OUT, MAX_SMALL, Op, batch_body, batch_script, parse_frames};
use mule::transport::{Control, Fake, Feed, MuxOp, Output, Transport, spawn_fed};
use serde_json::{Value, json};

fn host(name: &str) -> Host {
    isolate_state();
    Config::parse(&format!("[hosts.{name}]\n"))
        .unwrap()
        .host(Some(name))
        .unwrap()
        .clone()
}

fn uniq(tag: &str) -> String {
    static SEQ: AtomicUsize = AtomicUsize::new(0);
    format!(
        "serve-{tag}-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    )
}

fn feed<'a>(payload: &'a [u8], control: &'a Control) -> Feed<'a> {
    Feed {
        payload,
        control,
        kill_after: mule::serve::LOCAL_KILL,
        max_stdout: 64 << 20,
    }
}

fn live_check() -> Output {
    Output {
        stdout: Vec::new(),
        stderr: "Master running (pid=1)\r\n".into(),
        code: 0,
    }
}

/// Is the host's ticket free? Takes it, from another thread, within `limit`.
fn ticket_free(h: &Host, limit: Duration) -> bool {
    let name = h.name.clone();
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = mule::lock::with_lock(&name, || ());
        let _ = tx.send(());
    });
    rx.recv_timeout(limit).is_ok()
}

// ---------------------------------------------------------------- the lock

/// Counts how many stdin round trips are inside the unlocked primitive.
#[derive(Default)]
struct Overlap {
    inside: AtomicUsize,
    peak: AtomicUsize,
    calls: AtomicUsize,
}

impl Transport for Overlap {
    fn run_unlocked(&self, _: &Host, _: &str) -> Result<Output> {
        anyhow::bail!("serve never uses run")
    }
    fn run_with_stdin_unlocked(&self, _: &Host, _: &str, _: &Feed) -> Result<Output> {
        let now = self.inside.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(now, Ordering::SeqCst);
        std::thread::sleep(Duration::from_millis(15));
        self.inside.fetch_sub(1, Ordering::SeqCst);
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(Output::ok(""))
    }
    fn mux_op(&self, _: &Host, _: MuxOp) -> Result<Output> {
        Ok(live_check())
    }
}

#[test]
fn concurrent_stdin_round_trips_never_overlap_inside_the_unlocked_primitive() {
    let h = host(&uniq("overlap"));
    let t = Arc::new(Overlap::default());
    let threads: Vec<_> = (0..6)
        .map(|_| {
            let (t, h) = (t.clone(), h.clone());
            std::thread::spawn(move || {
                let ctl = Control::default();
                for _ in 0..3 {
                    t.run_with_stdin(&h, "true", &feed(b"", &ctl)).unwrap();
                }
            })
        })
        .collect();
    for th in threads {
        th.join().unwrap();
    }
    assert_eq!(t.calls.load(Ordering::SeqCst), 18);
    assert_eq!(
        t.peak.load(Ordering::SeqCst),
        1,
        "two stdin round trips held the channel at once"
    );
}

#[test]
fn a_stdin_round_trip_waits_for_another_callers_ticket() {
    let h = host(&uniq("queue"));
    let t = Arc::new(Overlap::default());
    let (held_tx, held_rx) = std::sync::mpsc::channel();
    let (go_tx, go_rx) = std::sync::mpsc::channel::<()>();
    let name = h.name.clone();
    // Any other mule verb holding the ticket, e.g. a `poll`.
    let holder = std::thread::spawn(move || {
        mule::lock::with_lock(&name, || {
            held_tx.send(()).unwrap();
            go_rx.recv().unwrap();
        })
        .unwrap()
    });
    held_rx.recv().unwrap();
    let (t2, h2) = (t.clone(), h.clone());
    let rt = std::thread::spawn(move || {
        t2.run_with_stdin(&h2, "true", &feed(b"", &Control::default()))
            .unwrap()
    });
    std::thread::sleep(Duration::from_millis(150));
    assert_eq!(
        t.calls.load(Ordering::SeqCst),
        0,
        "serve ran while another caller held the ticket"
    );
    go_tx.send(()).unwrap();
    holder.join().unwrap();
    rt.join().unwrap();
    assert_eq!(t.calls.load(Ordering::SeqCst), 1);
}

/// Refuses the first attempt as a proven pre-exec refusal, then runs a real
/// child that echoes the first `n` bytes of its stdin.
struct RefusedOnce {
    attempts: AtomicUsize,
}

impl Transport for RefusedOnce {
    fn run_unlocked(&self, _: &Host, _: &str) -> Result<Output> {
        anyhow::bail!("unused")
    }
    fn run_with_stdin_unlocked(&self, _: &Host, script: &str, feed: &Feed) -> Result<Output> {
        if self.attempts.fetch_add(1, Ordering::SeqCst) == 0 {
            return Err(mule::errors::MuleError::MuxRefusedPreExec.into());
        }
        spawn_fed(Command::new("sh").arg("-c").arg(script), feed)
    }
    fn mux_op(&self, _: &Host, _: MuxOp) -> Result<Output> {
        Ok(live_check())
    }
}

#[test]
fn a_retried_attempt_replays_the_control_lines_the_refused_one_swallowed() {
    let h = host(&uniq("replay"));
    let ctl = Control::default();
    // A cancel sent before (or while) the refused attempt ran must reach the
    // attempt that does run, or the op it names runs anyway.
    ctl.lock().unwrap().send(b"3\n");
    let t = RefusedOnce { attempts: 0.into() };
    let out = t
        .run_with_stdin(&h, "head -c 5", &feed(b"pay", &ctl))
        .unwrap();
    assert_eq!(t.attempts.load(Ordering::SeqCst), 2);
    assert_eq!(out.stdout, b"pay3\n");
}

// -------------------------------------------------------- local bounds

#[test]
fn a_session_that_never_reads_its_stdin_is_killed_on_time() {
    // The prototype armed its kill after the payload write, so a payload
    // bigger than the pipe into a session nobody read blocked it forever.
    let ctl = Control::default();
    let payload = vec![b'x'; 4 << 20];
    let t = Instant::now();
    let err = spawn_fed(
        Command::new("sh").args(["-c", "sleep 30"]).process_group(0),
        &Feed {
            payload: &payload,
            control: &ctl,
            kill_after: Duration::from_secs(1),
            max_stdout: 1 << 20,
        },
    )
    .unwrap_err();
    assert!(t.elapsed() < Duration::from_secs(4), "{:?}", t.elapsed());
    assert!(format!("{err:#}").contains("wedged"), "{err:#}");
}

#[test]
fn a_session_that_floods_stdout_is_killed_at_the_cap() {
    let ctl = Control::default();
    let t = Instant::now();
    let err = spawn_fed(
        Command::new("sh").args(["-c", "yes"]).process_group(0),
        &Feed {
            payload: b"",
            control: &ctl,
            kill_after: Duration::from_secs(10),
            max_stdout: 1 << 20,
        },
    )
    .unwrap_err();
    assert!(t.elapsed() < Duration::from_secs(4), "{:?}", t.elapsed());
    assert!(format!("{err:#}").contains("output passed"), "{err:#}");
}

#[test]
fn an_abandoned_control_stream_starts_no_session() {
    let ctl = Control::default();
    ctl.lock().unwrap().abandon();
    let marker = scratch("abandon").join("ran");
    let err = spawn_fed(
        Command::new("sh")
            .arg("-c")
            .arg(format!("touch '{}'", marker.display())),
        &feed(b"", &ctl),
    )
    .unwrap_err();
    assert!(format!("{err:#}").contains("caller is gone"), "{err:#}");
    assert!(!marker.exists());
}

// ---------------------------------------------------------- the script

#[test]
fn every_op_runs_in_its_own_group_under_the_deadline_and_payloads_stay_out_of_argv() {
    let big = vec![b'x'; 200 * 1024];
    let ops = vec![
        Op::Read("a b'c".into()),
        Op::Stat("/x".into()),
        Op::Write("/y".into(), big),
        Op::Read("/never/plain-in-the-script".into()),
    ];
    let s = batch_body(&ops);
    assert_eq!(s.matches("exec $S sh -c \"$R\"").count(), ops.len(), "{s}");
    // A supervisor per op ends it whole; one deadline and one watcher for
    // every op.
    for k in 0..ops.len() {
        assert!(
            s.contains(&format!(
                "wait $p; : > \"$d/end.{k}\"; Z $p {k} && : > \"$d/cl.{k}\" )"
            )),
            "{s}"
        );
    }
    let n = ops.len();
    assert!(
        s.contains(&format!("mule-deadline \"$d\" \"$X\" \"$T\" {n} ")),
        "{s}"
    );
    assert!(
        s.contains(&format!("mule-watch \"$d\" \"$X\" \"$T\" {n} ")),
        "{s}"
    );
    // The payload read and the delivery are bounded by the script itself:
    // the host need not have `timeout(1)`.
    assert!(s.contains(&format!("TO {} head -c", mule::serve::PAYLOAD_SECS)));
    assert!(s.contains(&format!("TO {} sh -c \"$F\"", mule::serve::DELIVER_SECS)));
    assert!(!s.contains("timeout"), "{s}");
    assert!(
        // The write body is the largest op body (~2KB); a 200KB payload
        // in the script would be far past this.
        s.len() < 16 * 1024,
        "payload leaked into the script: {} bytes",
        s.len()
    );
    assert!(
        !s.contains("plain-in-the-script"),
        "a path reached the script un-encoded"
    );
    // What ssh sends is the body for /bin/sh, as one word no login shell
    // reinterprets.
    let wire = batch_script(&ops);
    assert!(wire.starts_with("exec /bin/sh -c '"), "{wire}");
    let word = &wire["exec /bin/sh -c '".len()..wire.len() - 1];
    assert!(
        !word.contains(['\'', '\\', '\n']),
        "the word a login shell parses holds a quote, backslash or newline: {word}"
    );
    assert!(word.contains(&B64.encode(&s)));
    // The largest op a batch admits still fits the dispatch cap once
    // base64'd for /bin/sh: the ops' share is costed before that.
    let n = (mule::serve::MAX_SCRIPT - 400) / 4 * 3 - 32;
    let wire = batch_script(&[Op::Read(format!("/{}", "x".repeat(n)))]);
    assert!(
        wire.len() < mule::exec::MAX_DISPATCH_BYTES,
        "{} bytes on the wire",
        wire.len()
    );
}

#[test]
fn frames_need_the_end_marker() {
    let whole = b"@@ 0 0 2 0 0 d\nhi@@ end\n";
    let f = &parse_frames(whole, 1).unwrap()[0];
    assert_eq!(
        (f.stdout.as_slice(), f.ended),
        (&b"hi"[..], Ended::Completed)
    );
    assert!(
        parse_frames(b"@@ 0 0 2 0 0 d\nhi", 1).is_none(),
        "cut delivery"
    );
    assert!(parse_frames(b"@@ 0 0 2 0 0 d\nhi@@ end\nx", 1).is_none());
    assert!(
        parse_frames(b"@@ 0 0 2 0 0\nhi@@ end\n", 1).is_none(),
        "no end state"
    );
}

/// Runs the batch script under a local `sh` in its own process group: the
/// same `spawn_fed` plumbing ssh uses, minus the network.
#[derive(Default)]
struct LocalSh {
    /// Replaces `PATH` for the "host", to take a tool away.
    path: Option<String>,
    /// The account's login shell, which ssh hands the script to; `sh` if
    /// unset.
    login: Option<&'static str>,
    /// Stands in for the host's `/bin/sh`, which the script names.
    bin_sh: Option<&'static str>,
    /// `TMPDIR` for the "host", so a test can watch the batch's spool.
    tmpdir: Option<std::path::PathBuf>,
    /// The "host" login directory, against which a relative path resolves.
    cwd: Option<std::path::PathBuf>,
    rounds: AtomicUsize,
}

impl Transport for LocalSh {
    fn run_unlocked(&self, _: &Host, _: &str) -> Result<Output> {
        anyhow::bail!("serve never uses run")
    }
    fn run_with_stdin_unlocked(&self, _: &Host, script: &str, feed: &Feed) -> Result<Output> {
        self.rounds.fetch_add(1, Ordering::SeqCst);
        let script = match self.bin_sh {
            Some(sh) => script.replacen("exec /bin/sh -c ", &format!("exec {sh} -c "), 1),
            None => script.to_string(),
        };
        let mut cmd = Command::new(self.login.unwrap_or("sh"));
        cmd.arg("-c").arg(script).process_group(0);
        if let Some(path) = &self.path {
            cmd.env("PATH", path);
        }
        if let Some(dir) = &self.tmpdir {
            cmd.env("TMPDIR", dir);
        }
        if let Some(dir) = &self.cwd {
            cmd.current_dir(dir);
        }
        spawn_fed(&mut cmd, feed)
    }
    fn mux_op(&self, _: &Host, _: MuxOp) -> Result<Output> {
        Ok(live_check())
    }
}

fn have_tools() -> bool {
    let ok = Command::new("sh")
        .arg("-c")
        .arg("command -v timeout && printf eA== | base64 -d")
        .output()
        .is_ok_and(|o| o.status.success() && o.stdout.ends_with(b"x"));
    if !ok {
        // The script needs no timeout(1); the tests' tool shims do.
        eprintln!("skipping: no timeout(1) or base64 -d on this machine");
    }
    ok
}

fn round_trip(t: &LocalSh, ops: &[Op]) -> Output {
    let payload: Vec<u8> = ops.iter().flat_map(|op| op_payload(op).to_vec()).collect();
    t.run_with_stdin(
        &host(&uniq("rt")),
        &batch_script(ops),
        &feed(&payload, &Control::default()),
    )
    .unwrap()
}

fn op_payload(op: &Op) -> &[u8] {
    match op {
        Op::Write(_, d) => d,
        _ => &[],
    }
}

/// A FIFO nobody writes, released and removed when dropped ([`Fifo`]).
fn fifo(tag: &str) -> Fifo {
    Fifo::new(scratch(tag).join("fifo"))
}

/// Does anything still hold `fifo` open for reading? A non-blocking open
/// for writing fails with ENXIO exactly when no reader is left, so this
/// sees a blocked `head` of an op that should be dead.
fn fifo_has_reader(fifo: &std::path::Path) -> bool {
    use std::os::unix::fs::OpenOptionsExt;
    std::fs::OpenOptions::new()
        .write(true)
        .custom_flags(libc::O_NONBLOCK)
        .open(fifo)
        .is_ok()
}

/// A directory holding one host tool, `tool`, as the `sh` script `body`:
/// what a host whose `PATH` finds a wrapper (a shim, a version manager's)
/// runs instead of the real tool. Serve's ops are its own, but these tools
/// are the host's, and are how a test makes a read, stat or write slow,
/// TERM-proof, or leave children behind.
fn tool_dir(tag: &str, tool: &str, body: &str) -> std::path::PathBuf {
    let dir = scratch(tag);
    common::local::write_exec(
        dir.join(tool),
        format!(
            "#!/bin/sh
{body}
"
        ),
    )
    .unwrap();
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(dir.join(tool), std::fs::Permissions::from_mode(0o755)).unwrap();
    dir
}

/// `t` with `dir` first on its `PATH`.
fn with_tools(t: LocalSh, dir: &std::path::Path) -> LocalSh {
    let rest = t
        .path
        .clone()
        .unwrap_or_else(|| std::env::var("PATH").unwrap());
    LocalSh {
        path: Some(format!("{}:{rest}", dir.display())),
        ..t
    }
}

/// A host whose `stat` is `body`.
fn stat_host(tag: &str, body: &str) -> LocalSh {
    with_tools(LocalSh::default(), &tool_dir(tag, "stat", body))
}

/// The real `tool`'s path here, for a wrapper to hand on to.
fn real(tool: &str) -> String {
    let out = Command::new("sh")
        .arg("-c")
        .arg(format!("command -v {tool}"))
        .output()
        .unwrap();
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

/// An `mv` that takes 0.5s for a destination with `slow` in its name: a
/// write whose effect lands late enough to cancel.
fn slow_mv_dir(tag: &str) -> std::path::PathBuf {
    tool_dir(
        tag,
        "mv",
        &format!(
            "case \"$*\" in *slow*) sleep 0.5;; esac; exec {} \"$@\"",
            real("mv")
        ),
    )
}

/// An `mv` that, for a destination with `slow` in its name, touches `ready`
/// and then waits to be killed: a write a cancel always catches in flight,
/// however late a loaded host starts the watcher.
fn stuck_mv_dir(tag: &str, ready: &std::path::Path) -> std::path::PathBuf {
    tool_dir(
        tag,
        "mv",
        &format!(
            "case \"$*\" in *slow*) touch '{}'; sleep 5;; esac; exec {} \"$@\"",
            ready.display(),
            real("mv")
        ),
    )
}

#[test]
fn local_sh_round_trip_writes_reads_and_frames_binary_output() {
    if !have_tools() {
        return;
    }
    let p = scratch("frames").join("w.bin");
    let data: Vec<u8> = (0..200 * 1024).map(|i| (i % 251) as u8).collect();
    let t = LocalSh::default();
    let out = round_trip(
        &t,
        &[
            Op::Write(p.display().to_string(), data.clone()),
            Op::Write(
                p.with_extension("2").display().to_string(),
                b"x\x00y".to_vec(),
            ),
            Op::Read("/nonexistent/mule-serve".into()),
        ],
    );
    let f = parse_frames(&out.stdout, 3).expect(&out.stderr);
    assert_eq!(f[0].rc, 0, "{}", String::from_utf8_lossy(&f[0].stderr));
    assert_eq!(std::fs::read(&p).unwrap(), data);
    assert_eq!(f[1].rc, 0, "{}", String::from_utf8_lossy(&f[1].stderr));
    assert_eq!(std::fs::read(p.with_extension("2")).unwrap(), b"x\x00y");
    // A failed op's own rc and stderr come back.
    assert_ne!(f[2].rc, 0);
    assert!(!f[2].stderr.is_empty() && f[2].stdout.is_empty(), "{f:?}");
    let p = p.display().to_string();
    let out = round_trip(&t, &[Op::Read(p.clone()), Op::Stat(p)]);
    let f = parse_frames(&out.stdout, 2).unwrap();
    assert_eq!((f[0].stdout.as_slice(), f[0].truncated), (&data[..], false));
    // Size first, whichever stat flavour the host has.
    let stat = String::from_utf8_lossy(&f[1].stdout).into_owned();
    assert_eq!(stat.split(' ').next(), Some("204800"), "{stat}");
}

#[test]
fn an_op_past_the_limit_is_killed_by_the_host_side_timeout() {
    if !have_tools() {
        return;
    }
    let f = fifo("limit");
    let t = Instant::now();
    let out = round_trip(
        &LocalSh::default(),
        &[Op::Read(f.path().display().to_string())],
    );
    assert_eq!(parse_frames(&out.stdout, 1).unwrap()[0].rc, 124);
    assert!(!fifo_has_reader(f.path()), "the timed-out read still runs");
    assert!(t.elapsed() < Duration::from_secs(4), "{:?}", t.elapsed());
}

#[test]
fn output_is_capped_on_the_host_and_flagged() {
    if !have_tools() {
        return;
    }
    let dir = scratch("cap");
    let (big, small) = (dir.join("big"), dir.join("small"));
    std::fs::write(&big, vec![0u8; 3_000_000]).unwrap();
    std::fs::write(&small, "small\n").unwrap();
    // A stat whose stderr floods: the small cap holds, and the rc is the
    // tool's own, not SIGPIPE's 141.
    let t = stat_host(
        "cap-stat",
        "head -c 200000 /dev/zero >&2; echo '1 2 3'; exit 4",
    );
    let out = round_trip(
        &t,
        &[
            Op::Read(big.display().to_string()),
            Op::Read(small.display().to_string()),
            Op::Stat("/x".into()),
        ],
    );
    let f = parse_frames(&out.stdout, 3).expect(&out.stderr);
    assert_eq!(f[0].rc, 0, "the cap must not turn the op's rc into SIGPIPE");
    assert_eq!(f[0].stdout.len() as u64, MAX_OUT);
    assert!(f[0].truncated);
    assert_eq!(f[2].rc, 4, "the cap must not turn the op's rc into SIGPIPE");
    assert_eq!(f[2].stderr.len() as u64, MAX_SMALL);
    assert!(f[2].truncated);
    assert_eq!(
        (f[1].stdout.as_slice(), f[1].truncated),
        (&b"small\n"[..], false)
    );
}

/// A `PATH` of symlinks to every tool the script and the test ops need,
/// minus `missing`. `timeout` is not the script's but the shims': tests
/// wrap a host tool in GNU `timeout` to make a child leave the op's group.
fn path_without(missing: &str) -> String {
    let dir = scratch(&format!("nopath-{missing}"));
    for tool in [
        "sh", "timeout", "base64", "mktemp", "head", "tail", "wc", "cat", "rm", "rmdir", "mv",
        "chmod", "touch", "sleep", "uname", "stat", "ps", "awk", "pgrep", "pkill",
    ] {
        if tool == missing {
            continue;
        }
        let found = Command::new("sh")
            .arg("-c")
            .arg(format!("command -v {tool}"))
            .output()
            .unwrap();
        let found = String::from_utf8_lossy(&found.stdout).trim().to_string();
        if found.is_empty() {
            continue; // pgrep and pkill are optional on the host
        }
        std::os::unix::fs::symlink(found, dir.join(tool)).unwrap();
    }
    dir.display().to_string()
}

#[test]
fn a_host_without_base64_or_mktemp_fails_every_op_closed() {
    if !have_tools() {
        return;
    }
    for missing in ["base64", "mktemp"] {
        let marker = scratch("tool").join("ran");
        let t = Arc::new(LocalSh {
            path: Some(path_without(missing)),
            ..Default::default()
        });
        let (mut w, mut r, done) = spawn_serve(t);
        let data = B64.encode(b"x");
        writeln!(
            w,
            "{}",
            json!({"op":"write","id":1,"path":marker,"data_b64":data})
        )
        .unwrap();
        writeln!(w, "{}", json!({"op":"read","id":2,"path":"/etc/hosts"})).unwrap();
        for _ in 0..2 {
            let v = reply(&mut r);
            assert_eq!(v["kind"], "host_tool_missing", "{missing}: {v}");
            assert!(
                v["error"].as_str().unwrap().contains(missing),
                "{missing}: {v}"
            );
        }
        assert!(!marker.exists(), "{missing}: an op ran without its bounds");
        drop(w);
        assert_eq!(done.join().unwrap(), 0);
    }
}

/// A `PATH` of one directory: a symlink to the first of each command `path`
/// finds, `timeout` excepted. Stock macOS before 27 has no `timeout(1)`, and
/// on the CI runner only the runner's shell `PATH` (Homebrew) had one, not
/// sshd's `/usr/bin:/bin:/usr/sbin:/sbin`: every op of every batch failed
/// `host_tool_missing` there, while these local-sh tests passed.
fn without_timeout(path: &str) -> String {
    let dir = scratch("no-timeout");
    for d in path.split(':').filter(|d| !d.is_empty()) {
        let Ok(entries) = std::fs::read_dir(d) else {
            continue;
        };
        for e in entries.flatten() {
            let name = e.file_name();
            if name == "timeout" || dir.join(&name).symlink_metadata().is_ok() {
                continue;
            }
            let _ = std::os::unix::fs::symlink(e.path(), dir.join(&name));
        }
    }
    assert!(!dir.join("timeout").exists());
    dir.display().to_string()
}

/// Every local "host" flavour, none with a `timeout(1)` on its `PATH`.
fn hosts_without_timeout() -> Vec<(&'static str, LocalSh)> {
    local_hosts()
        .into_iter()
        .map(|(flavour, t)| {
            let path = t
                .path
                .clone()
                .unwrap_or_else(|| std::env::var("PATH").unwrap());
            let t = LocalSh {
                path: Some(without_timeout(&path)),
                ..t
            };
            (flavour, t)
        })
        .collect()
}

#[test]
fn a_host_without_timeout_serves_every_op() {
    if !have_tools() {
        return;
    }
    for (flavour, t) in hosts_without_timeout() {
        let p = scratch("no-timeout-rt").join("w");
        let out = round_trip(&t, &[Op::Write(p.display().to_string(), b"hi".to_vec())]);
        let f = parse_frames(&out.stdout, 1)
            .unwrap_or_else(|| panic!("{flavour}: rc {}: {}", out.code, out.stderr));
        assert_eq!(
            f[0].rc,
            0,
            "{flavour}: {}",
            String::from_utf8_lossy(&f[0].stderr)
        );
        let p = p.display().to_string();
        let out = round_trip(&t, &[Op::Read(p.clone()), Op::Stat(p)]);
        let f = parse_frames(&out.stdout, 2)
            .unwrap_or_else(|| panic!("{flavour}: rc {}: {}", out.code, out.stderr));
        assert_eq!(
            (f[0].rc, f[0].stdout.as_slice()),
            (0, &b"hi"[..]),
            "{flavour}"
        );
        assert_eq!(f[1].rc, 0, "{flavour}");
    }
}

/// A payload that never arrives whole: the host's own bound on reading it
/// fails the batch, before any op, with no `timeout(1)` to lean on.
#[test]
fn the_payload_read_is_bounded_on_a_host_without_timeout() {
    if !have_tools() {
        return;
    }
    let secs = u64::from(mule::serve::PAYLOAD_SECS);
    let runs: Vec<_> = hosts_without_timeout()
        .into_iter()
        .map(|(flavour, t)| {
            std::thread::spawn(move || {
                let marker = scratch("no-timeout-pay").join("w");
                let op = Op::Write(marker.display().to_string(), vec![b'x'; 10]);
                let ctl = Control::default();
                let start = Instant::now();
                // Half the payload, and the stream left open.
                let out = t
                    .run_with_stdin(
                        &host(&uniq("pay")),
                        &batch_script(&[op]),
                        &feed(b"xxxxx", &ctl),
                    )
                    .unwrap_or_else(|e| panic!("{flavour}: {e:#}"));
                (flavour, start.elapsed(), out, marker.exists())
            })
        })
        .collect();
    for run in runs {
        let (flavour, took, out, wrote) = run.join().unwrap();
        assert!(
            out.stderr.contains("payload read failed"),
            "{flavour}: rc {}: {}",
            out.code,
            out.stderr
        );
        assert!(!wrote, "{flavour}: the op ran");
        assert!(
            took >= Duration::from_secs(secs) && took < Duration::from_secs(secs + 3),
            "{flavour}: {took:?}"
        );
    }
}

/// A delivery that stalls, TERM-proof: KILLed whole one second after its
/// bound, as `timeout -k 1` did, with no `timeout(1)` on the host.
#[test]
fn the_delivery_is_bounded_on_a_host_without_timeout() {
    if !have_tools() {
        return;
    }
    let secs = u64::from(mule::serve::DELIVER_SECS);
    let real_head = real("head");
    let runs: Vec<_> = hosts_without_timeout()
        .into_iter()
        .map(|(flavour, t)| {
            let late = scratch("no-timeout-deliver").join("late");
            // Only the delivery names a spool file `out.<k>` as an argument.
            let dir = tool_dir(
                "no-timeout-head",
                "head",
                &format!(
                    "case \"$*\" in *mule-serve.*/out.*) trap '' TERM; sleep {}; touch '{}';; esac\n\
                     exec {real_head} \"$@\"",
                    secs + 3,
                    late.display()
                ),
            );
            let t = with_tools(t, &dir);
            std::thread::spawn(move || {
                let file = scratch("no-timeout-deliver-src").join("f");
                std::fs::write(&file, "data").unwrap();
                let start = Instant::now();
                let out = round_trip(&t, &[Op::Read(file.display().to_string())]);
                (flavour, start.elapsed(), out, late)
            })
        })
        .collect();
    let mut lates = Vec::new();
    for run in runs {
        let (flavour, took, out, late) = run.join().unwrap();
        assert!(
            parse_frames(&out.stdout, 1).is_none(),
            "{flavour}: a cut delivery passed"
        );
        assert!(
            took >= Duration::from_secs(secs) && took < Duration::from_secs(secs + 3),
            "{flavour}: {took:?}: {}",
            out.stderr
        );
        lates.push((flavour, late));
    }
    std::thread::sleep(Duration::from_secs(4));
    for (flavour, late) in lates {
        assert!(
            !late.exists(),
            "{flavour}: the stalled delivery outlived its KILL"
        );
    }
}

// ------------------------------------------------------- the serve loop

/// Drives the whole serve loop over pipes with `t`.
fn spawn_serve<T: Transport + Send + Sync + 'static>(
    t: Arc<T>,
) -> (
    std::io::PipeWriter,
    BufReader<std::io::PipeReader>,
    std::thread::JoinHandle<i32>,
) {
    let (in_r, in_w) = std::io::pipe().unwrap();
    let (out_r, out_w) = std::io::pipe().unwrap();
    let h = host(&uniq("loop"));
    let done = std::thread::spawn(move || mule::serve::serve(t, h, BufReader::new(in_r), out_w));
    (in_w, BufReader::new(out_r), done)
}

fn reply(r: &mut impl BufRead) -> Value {
    let mut line = String::new();
    r.read_line(&mut line).unwrap();
    serde_json::from_str(&line).unwrap_or_else(|e| panic!("{e}: {line:?}"))
}

/// Join `done` within `limit`, or fail.
fn joined(done: std::thread::JoinHandle<i32>, limit: Duration) -> i32 {
    let t = Instant::now();
    while !done.is_finished() {
        assert!(t.elapsed() < limit, "serve did not exit within {limit:?}");
        std::thread::sleep(Duration::from_millis(10));
    }
    done.join().unwrap()
}

#[test]
fn cancel_mid_op_kills_it_and_leaves_no_side_effect() {
    if !have_tools() {
        return;
    }
    let dir = scratch("cancel");
    let marker = dir.join("late-slow");
    let ready = dir.join("ready");
    let t = with_tools(LocalSh::default(), &stuck_mv_dir("cancel-mv", &ready));
    let (mut w, mut r, done) = spawn_serve(Arc::new(t));
    let data = B64.encode(b"x");
    writeln!(
        w,
        "{}",
        json!({"op":"write","id":1,"path":marker,"data_b64":data})
    )
    .unwrap();
    wait_until("the write to reach its mv", 5, || ready.exists());
    writeln!(w, "{}", json!({"op":"cancel","id":1})).unwrap();
    let v = reply(&mut r);
    assert_eq!(
        (v["id"].as_u64(), &v["cancelled"]),
        (Some(1), &json!(true)),
        "{v}"
    );
    assert_eq!(v["kind"], "cancelled", "{v}");
    std::thread::sleep(Duration::from_millis(1200));
    assert!(
        !marker.exists(),
        "cancelled op still ran to completion: {v}"
    );
    // Control: a write does land uncancelled, so the absence above is the
    // cancel's doing. Without the delay: under a loaded parallel suite the
    // runner's own spawns pushed a 0.3s op past the 1s bound.
    let landed = dir.join("landed");
    writeln!(
        w,
        "{}",
        json!({"op":"write","id":2,"path":landed,"data_b64":data})
    )
    .unwrap();
    let v = reply(&mut r);
    assert_eq!(
        (v["id"].as_u64(), v["rc"].as_i64()),
        (Some(2), Some(0)),
        "{v}"
    );
    assert!(landed.exists());
    drop(w);
    assert_eq!(joined(done, Duration::from_secs(5)), 0);
}

/// Blocks every round trip until released, so ops stay queued.
struct Gate {
    open: Mutex<bool>,
    cv: std::sync::Condvar,
    rounds: AtomicUsize,
}

impl Transport for Gate {
    fn run_unlocked(&self, _: &Host, _: &str) -> Result<Output> {
        anyhow::bail!("unused")
    }
    fn run_with_stdin_unlocked(&self, _: &Host, _: &str, _: &Feed) -> Result<Output> {
        self.rounds.fetch_add(1, Ordering::SeqCst);
        let mut open = self.open.lock().unwrap();
        while !*open {
            open = self.cv.wait(open).unwrap();
        }
        Ok(Output::fail(1, "gated"))
    }
    fn mux_op(&self, _: &Host, _: MuxOp) -> Result<Output> {
        Ok(live_check())
    }
}

#[test]
fn cancel_before_dispatch_drops_the_op_without_a_round_trip() {
    let gate = Arc::new(Gate {
        open: Mutex::new(false),
        cv: Default::default(),
        rounds: 0.into(),
    });
    let (mut w, mut r, done) = spawn_serve(gate.clone());
    writeln!(w, "{}", json!({"op":"read","id":1,"path":"/a"})).unwrap();
    wait_until("first round trip", 5, || {
        gate.rounds.load(Ordering::SeqCst) == 1
    });
    writeln!(w, "{}", json!({"op":"read","id":2,"path":"/b"})).unwrap();
    writeln!(w, "{}", json!({"op":"cancel","id":2})).unwrap();
    let v = reply(&mut r);
    assert_eq!(
        (v["id"].as_u64(), &v["kind"]),
        (Some(2), &json!("cancelled")),
        "{v}"
    );
    *gate.open.lock().unwrap() = true;
    gate.cv.notify_all();
    let v = reply(&mut r);
    assert_eq!(v["id"], 1, "{v}");
    drop(w);
    assert_eq!(joined(done, Duration::from_secs(5)), 0);
    assert_eq!(
        gate.rounds.load(Ordering::SeqCst),
        1,
        "a cancelled op was sent"
    );
}

#[test]
fn a_burst_coalesces_into_few_round_trips() {
    if !have_tools() {
        return;
    }
    let dir = scratch("burst");
    let (mut w, mut r, done) = spawn_serve(Arc::new(LocalSh::default()));
    for i in 0..20 {
        let p = dir.join(format!("f{i}"));
        std::fs::write(&p, format!("v{i}")).unwrap();
        writeln!(w, "{}", json!({"op":"read","id":i,"path":p})).unwrap();
    }
    let mut batches = std::collections::HashSet::new();
    for _ in 0..20 {
        let v = reply(&mut r);
        let i = v["id"].as_u64().unwrap();
        let got = B64.decode(v["stdout_b64"].as_str().unwrap()).unwrap();
        assert_eq!(got, format!("v{i}").into_bytes());
        batches.insert(v["batch"].as_u64().unwrap());
    }
    // Each read reserves its caps out of the batch's spool budget, so a
    // batch holds only so many; one more for a first batch sent alone.
    let per = (mule::serve::MAX_BATCH_OUTPUT / (MAX_OUT + mule::serve::MAX_SMALL + 2)) as usize;
    assert!(
        batches.len() <= 20usize.div_ceil(per) + 1,
        "20 reads took {} round trips, at {per} a batch",
        batches.len()
    );
    drop(w);
    assert_eq!(joined(done, Duration::from_secs(5)), 0);
}

#[test]
fn an_oversize_payload_is_refused_before_any_ssh() {
    let fake = Arc::new(Fake::new());
    let (mut w, mut r, done) = spawn_serve(fake.clone());
    let big = B64.encode(vec![0u8; mule::serve::MAX_PAYLOAD + 1]);
    writeln!(
        w,
        "{}",
        json!({"op":"write","id":7,"path":"/x","data_b64":big})
    )
    .unwrap();
    let path = format!("/{}", "x".repeat(mule::serve::MAX_SCRIPT));
    writeln!(w, "{}", json!({"op":"read","id":8,"path":path})).unwrap();
    writeln!(w, "not json").unwrap();
    for (id, kind) in [(7, "too_large"), (8, "too_large"), (0, "bad_request")] {
        let v = reply(&mut r);
        assert_eq!(
            (v["id"].as_u64(), &v["kind"]),
            (Some(id), &json!(kind)),
            "{v}"
        );
    }
    drop(w);
    assert_eq!(joined(done, Duration::from_secs(5)), 0);
    assert!(
        fake.scripts().is_empty(),
        "an oversize op reached the transport"
    );
}

#[test]
fn master_loss_answers_every_op_and_exits_3() {
    let fake = Arc::new(Fake::new());
    fake.set_master(None);
    fake.push(Output::fail(255, "Control socket connect: No such file"));
    let (mut w, mut r, done) = spawn_serve(fake.clone());
    writeln!(w, "{}", json!({"op":"read","id":1,"path":"/a"})).unwrap();
    let v = reply(&mut r);
    assert_eq!(
        (v["id"].as_u64(), &v["kind"]),
        (Some(1), &json!("master_gone")),
        "{v}"
    );
    // Exit 3 does not wait for the caller to hang up.
    assert_eq!(joined(done, Duration::from_secs(5)), 3);
    drop(w);
}

#[test]
fn a_caller_that_stops_reading_holds_no_session_or_ticket() {
    if !have_tools() {
        return;
    }
    let t = Arc::new(LocalSh::default());
    let (in_r, mut w) = std::io::pipe().unwrap();
    let (out_r, out_w) = std::io::pipe().unwrap();
    let h = host(&uniq("stalled-out"));
    let hh = h.clone();
    let tt = t.clone();
    let done = std::thread::spawn(move || mule::serve::serve(tt, hh, BufReader::new(in_r), out_w));
    // Far more reply bytes than a pipe holds, and nobody reading them.
    let big = scratch("stalled-out").join("big");
    std::fs::write(&big, vec![0u8; 1_000_000]).unwrap();
    for i in 0..6 {
        writeln!(w, "{}", json!({"op":"read","id":i,"path":big})).unwrap();
    }
    wait_until("the batches to run", 10, || {
        t.rounds.load(Ordering::SeqCst) >= 1
    });
    std::thread::sleep(Duration::from_millis(1500));
    assert!(
        ticket_free(&h, Duration::from_secs(2)),
        "a stalled reader kept the ticket"
    );
    // The caller goes without ever reading: serve exits even though its
    // reply writer is still blocked.
    drop(w);
    assert_eq!(joined(done, Duration::from_secs(6)), 0);
    drop(out_r);
}

#[test]
fn a_caller_that_stalls_mid_request_sends_nothing() {
    let fake = Arc::new(Fake::new());
    let (mut w, _r, done) = spawn_serve(fake.clone());
    write!(w, "{{\"op\":\"read\",\"id\":1,").unwrap();
    w.flush().unwrap();
    std::thread::sleep(Duration::from_millis(300));
    assert!(fake.scripts().is_empty());
    drop(w);
    assert_eq!(joined(done, Duration::from_secs(5)), 0);
    assert!(fake.scripts().is_empty(), "a half-written request was sent");
}

#[test]
fn caller_eof_mid_op_cancels_it_and_exits() {
    if !have_tools() {
        return;
    }
    let marker = scratch("eof").join("late-slow");
    let t = with_tools(LocalSh::default(), &slow_mv_dir("eof-mv"));
    let (mut w, _r, done) = spawn_serve(Arc::new(t));
    let data = B64.encode(b"x");
    writeln!(
        w,
        "{}",
        json!({"op":"write","id":1,"path":marker,"data_b64":data})
    )
    .unwrap();
    std::thread::sleep(Duration::from_millis(150));
    drop(w);
    assert_eq!(joined(done, Duration::from_secs(4)), 0);
    std::thread::sleep(Duration::from_millis(1200));
    assert!(!marker.exists(), "a dead caller's op ran to completion");
}

// ---------------------------------------------------------- the binary

fn spawn_binary(shim: &ShimHost) -> std::process::Child {
    shim.command(&["serve", "--stdio"])
        .stdin(Stdio::piped())
        .spawn()
        .unwrap()
}

fn no_process_matching(pattern: &str) -> bool {
    !Command::new("pgrep")
        .args(["-f", pattern])
        .output()
        .unwrap()
        .status
        .success()
}

#[test]
fn the_binary_answers_ops_and_a_sigkilled_caller_leaves_nothing_running() {
    if !have_tools() {
        return;
    }
    let shim = ShimHost::new("serve-bin");
    let file = shim.dir.join("hello");
    std::fs::write(&file, "hi\n").unwrap();
    let mut child = shim
        .command(&["serve", "--stdio"])
        .env("PATH", shim.path_env(Some(&slow_mv_dir("bin-mv"))))
        .stdin(Stdio::piped())
        .spawn()
        .unwrap();
    let mut w = child.stdin.take().unwrap();
    let mut r = BufReader::new(child.stdout.take().unwrap());
    writeln!(w, "{}", json!({"op":"read","id":1,"path":file})).unwrap();
    let v = reply(&mut r);
    assert_eq!(
        B64.decode(v["stdout_b64"].as_str().unwrap()).unwrap(),
        b"hi\n",
        "{v}"
    );

    // A unique marker, so pgrep finds only this test's op: the slow `mv`
    // has it in its argv.
    let marker = shim.dir.join(format!("late-slow-{}", uniq("kill")));
    let data = B64.encode(b"x");
    writeln!(
        w,
        "{}",
        json!({"op":"write","id":2,"path":marker,"data_b64":data})
    )
    .unwrap();
    std::thread::sleep(Duration::from_millis(200));
    child.kill().unwrap();
    child.wait().unwrap();
    let pattern = marker.display().to_string();
    wait_until("the dead caller's op to be killed", 3, || {
        no_process_matching(&pattern)
    });
    std::thread::sleep(Duration::from_millis(1000));
    assert!(
        !marker.exists(),
        "a SIGKILLed caller's op ran to completion"
    );
}

#[test]
fn the_binary_exits_3_when_the_master_goes() {
    if !have_tools() {
        return;
    }
    let shim = ShimHost::new("serve-master");
    let mut child = spawn_binary(&shim);
    let mut w = child.stdin.take().unwrap();
    let mut r = BufReader::new(child.stdout.take().unwrap());
    writeln!(w, "{}", json!({"op":"stat","id":1,"path":shim.dir})).unwrap();
    assert_eq!(reply(&mut r)["rc"], 0);
    shim.master_down(true);
    writeln!(w, "{}", json!({"op":"stat","id":2,"path":shim.dir})).unwrap();
    let v = reply(&mut r);
    assert_eq!(v["kind"], "master_gone", "{v}");
    let t = Instant::now();
    let status = loop {
        if let Some(s) = child.try_wait().unwrap() {
            break s;
        }
        assert!(t.elapsed() < Duration::from_secs(5), "serve kept running");
        std::thread::sleep(Duration::from_millis(20));
    };
    assert_eq!(status.code(), Some(3));
    let mut err = String::new();
    child
        .stderr
        .take()
        .unwrap()
        .read_to_string(&mut err)
        .unwrap();
    assert!(err.contains("no control master"), "{err}");
    drop(w);
}

#[test]
fn the_binary_exits_3_with_no_master_at_start() {
    let shim = ShimHost::new("serve-nomaster");
    shim.master_down(true);
    let out = shim.mule(&["serve", "--stdio"]);
    assert_eq!(
        out.status.code(),
        Some(3),
        "{}",
        common::local::stderr(&out)
    );
}

// ------------------------------------- every op's whole process group dies

/// A `PATH` whose `sh` is dash, which has no job control without a tty, and
/// whose `setsid` is a perl stand-in for util-linux's: the Linux-host path,
/// on any machine with dash and perl. With it, dash to stand in for the
/// host's `/bin/sh`. `None` (and a note) without them.
fn path_dash_setsid() -> Option<(String, &'static str)> {
    let dash = ["/bin/dash", "/usr/bin/dash", "/opt/homebrew/bin/dash"]
        .into_iter()
        .find(|p| std::path::Path::new(p).exists());
    let perl = Command::new("perl").args(["-MPOSIX", "-e", "1"]).status();
    let (Some(dash), Ok(s)) = (dash, perl) else {
        eprintln!("skipping the dash leg: no dash or perl here");
        return None;
    };
    if !s.success() {
        return None;
    }
    let dir = std::path::PathBuf::from(path_without("sh"));
    std::os::unix::fs::symlink(dash, dir.join("sh")).unwrap();
    let setsid = dir.join("setsid");
    std::fs::write(
        &setsid,
        "#!/usr/bin/env perl\nuse POSIX; POSIX::setsid() or die \"setsid: $!\"; exec @ARGV or die;\n",
    )
    .unwrap();
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(&setsid, std::fs::Permissions::from_mode(0o755)).unwrap();
    Some((
        format!("{}:{}", dir.display(), std::env::var("PATH").unwrap()),
        dash,
    ))
}

/// A host whose account's login shell is zsh. Where this machine's
/// `/bin/sh` has job control without a tty (macOS, a bash `/bin/sh`), its
/// `PATH` also lacks `setsid`, so the batch takes the `set -m` fallback that
/// zsh aborted on (review, 2026-11-20); elsewhere it keeps `setsid`, since
/// that host's honest answer is `host_tool_missing`. `None` without zsh.
fn zsh_host() -> Option<LocalSh> {
    let zsh = ["/bin/zsh", "/usr/bin/zsh", "/opt/homebrew/bin/zsh"]
        .into_iter()
        .find(|p| std::path::Path::new(p).exists());
    let Some(zsh) = zsh else {
        eprintln!("skipping the zsh leg: no zsh here");
        return None;
    };
    let job_control = Command::new("/bin/sh")
        .arg("-c")
        .arg("set -m 2>/dev/null; case $- in *m*) exit 0;; esac; exit 1")
        .stdin(Stdio::null())
        .status()
        .is_ok_and(|s| s.success());
    Some(LocalSh {
        login: Some(zsh),
        path: job_control.then(|| path_without("setsid")),
        ..Default::default()
    })
}

/// Every flavour of local "host" this machine can stand in for.
fn local_hosts() -> Vec<(&'static str, LocalSh)> {
    let mut hosts = vec![("sh", LocalSh::default())];
    if let Some((path, dash)) = path_dash_setsid() {
        hosts.push((
            "dash+setsid",
            LocalSh {
                path: Some(path),
                bin_sh: Some(dash),
                ..Default::default()
            },
        ));
    }
    if let Some(zsh) = zsh_host() {
        hosts.push(("zsh login", zsh));
    }
    hosts
}

#[test]
fn a_zsh_login_shell_runs_the_batch_under_sh() {
    if !have_tools() {
        return;
    }
    let Some(t) = zsh_host() else { return };
    let file = scratch("zsh-rt").join("ok");
    std::fs::write(&file, "OK").unwrap();
    let out = round_trip(&t, &[Op::Read(file.display().to_string())]);
    let f =
        parse_frames(&out.stdout, 1).unwrap_or_else(|| panic!("rc {}: {}", out.code, out.stderr));
    assert_eq!((f[0].rc, f[0].stdout.as_slice()), (0, &b"OK"[..]));
}

/// The production CLI over an ssh whose remote end runs the script as a zsh
/// login shell does (`zsh -c <command>`), with no `setsid` where `/bin/sh`
/// can do without.
#[test]
fn the_binary_serves_a_host_whose_login_shell_is_zsh() {
    if !have_tools() {
        return;
    }
    let Some(t) = zsh_host() else { return };
    let shim = ShimHost::new("serve-zsh");
    let ssh = shim.dir.join("ssh");
    let path = t.path.map(|p| format!("PATH='{p}' ")).unwrap_or_default();
    let body = std::fs::read_to_string(&ssh).unwrap().replace(
        "exec sh -c \"$last\"",
        &format!("{path}exec {} -c \"$last\"", t.login.unwrap()),
    );
    assert!(body.contains(t.login.unwrap()), "the shim changed shape");
    std::fs::write(&ssh, body).unwrap();
    let file = shim.dir.join("ok");
    std::fs::write(&file, "OK").unwrap();
    let mut child = spawn_binary(&shim);
    let mut w = child.stdin.take().unwrap();
    let mut r = BufReader::new(child.stdout.take().unwrap());
    writeln!(w, "{}", json!({"op":"read","id":1,"path":file})).unwrap();
    let v = reply(&mut r);
    assert_eq!(v["rc"], 0, "{v}");
    assert_eq!(
        B64.decode(v["stdout_b64"].as_str().unwrap()).unwrap(),
        b"OK"
    );
    drop(w);
    assert_eq!(child.wait().unwrap().code(), Some(0));
}

#[test]
fn a_child_that_leaves_the_ops_group_dies_with_it_at_the_deadline() {
    if !have_tools() {
        return;
    }
    for (flavour, t) in local_hosts() {
        let marker = scratch("own-group").join("late");
        // A host `stat` that wraps its work in GNU timeout, which puts its
        // child in a group of its own; this one outlived the op's TERM and
        // KILL (review, 2026-11-20, then through a command op).
        let body = format!(
            "timeout 5 sh -c 'sleep 2.8; touch \"$1\"' x '{}'",
            marker.display()
        );
        let t = with_tools(t, &tool_dir("own-group-stat", "stat", &body));
        let out = round_trip(&t, &[Op::Stat("/x".into())]);
        let f = parse_frames(&out.stdout, 1).expect(&out.stderr);
        assert_eq!((f[0].rc, f[0].ended), (124, Ended::TimedOut), "{flavour}");
        std::thread::sleep(Duration::from_millis(3500));
        assert!(
            !marker.exists(),
            "{flavour}: a timed-out op's child in its own group ran on"
        );
    }
}

#[test]
fn an_ops_leftovers_die_at_its_own_end_not_its_slowest_siblings() {
    if !have_tools() {
        return;
    }
    for (flavour, t) in local_hosts() {
        let marker = scratch("sibling").join("late");
        // A host `stat` that leaves a child behind for one path, and for the
        // other ignores TERM and lasts until the deadline's KILL (2s). The
        // leftover child is due at 0.6s, which leaves its own op's end room
        // on a loaded host (0.25s did not, in a full parallel run).
        let body = format!(
            "case \"$*\" in *slow*) trap '' TERM; sleep 5;; esac\n\
             (sleep 0.6; : > '{}') </dev/null >/dev/null 2>&1 & echo '1 2 3'",
            marker.display()
        );
        let t = with_tools(t, &tool_dir("sibling-stat", "stat", &body));
        let out = round_trip(&t, &[Op::Stat("/x".into()), Op::Stat("/slow".into())]);
        let f = parse_frames(&out.stdout, 2).expect(&out.stderr);
        assert_eq!((f[0].rc, f[1].rc), (0, 124), "{flavour}");
        assert!(
            !marker.exists(),
            "{flavour}: a completed op's child ran while its sibling did"
        );
    }
}

#[test]
fn an_op_ignoring_term_dies_with_its_group_at_the_deadline() {
    if !have_tools() {
        return;
    }
    for (flavour, t) in local_hosts() {
        let marker = scratch("deadline").join("late");
        // Past the deadline's KILL (2s) by more than a loaded host's jitter.
        let body = format!("trap '' TERM; sleep 3.5; touch '{}'", marker.display());
        let t = with_tools(t, &tool_dir("deadline-stat", "stat", &body));
        let start = Instant::now();
        let out = round_trip(&t, &[Op::Stat("/x".into())]);
        let f = parse_frames(&out.stdout, 1).expect(&out.stderr);
        assert_eq!(f[0].rc, 124, "{flavour}: {}", out.stderr);
        assert!(start.elapsed() < Duration::from_secs(4), "{flavour}");
        std::thread::sleep(Duration::from_millis(3000));
        assert!(
            !marker.exists(),
            "{flavour}: a TERM-ignoring op outlived its deadline"
        );
    }
}

#[test]
fn a_completed_op_leaves_no_descendant_behind() {
    if !have_tools() {
        return;
    }
    for (flavour, t) in local_hosts() {
        let marker = scratch("leftover").join("late");
        let body = format!(
            "(trap '' TERM; sleep 3; touch '{}') </dev/null >/dev/null 2>&1 & echo started",
            marker.display()
        );
        let t = with_tools(t, &tool_dir("leftover-stat", "stat", &body));
        let out = round_trip(&t, &[Op::Stat("/x".into())]);
        let f = parse_frames(&out.stdout, 1).expect(&out.stderr);
        assert_eq!((f[0].rc, f[0].stdout.as_slice()), (0, &b"started\n"[..]));
        std::thread::sleep(Duration::from_millis(4000));
        assert!(
            !marker.exists(),
            "{flavour}: a bounded op's background child outlived it"
        );
    }
}

#[test]
fn cancel_kills_a_term_ignoring_op_and_its_descendants() {
    if !have_tools() {
        return;
    }
    let dir = scratch("cancel-group");
    let (ready, marker) = (dir.join("ready"), dir.join("late"));
    let body = format!(
        "trap '' TERM; (sleep 3; touch '{m}.child') & touch '{}'; sleep 3; touch '{m}'",
        ready.display(),
        m = marker.display()
    );
    let t = stat_host("cancel-group-stat", &body);
    let (mut w, mut r, done) = spawn_serve(Arc::new(t));
    writeln!(w, "{}", json!({"op":"stat","id":1,"path":"/x"})).unwrap();
    wait_until("the op to start", 5, || ready.exists());
    writeln!(w, "{}", json!({"op":"cancel","id":1})).unwrap();
    let v = reply(&mut r);
    assert_eq!(
        (&v["kind"], &v["cancelled"]),
        (&json!("cancelled"), &json!(true)),
        "{v}"
    );
    std::thread::sleep(Duration::from_millis(3500));
    assert!(!marker.exists(), "a cancelled op ran on: {v}");
    assert!(
        !dir.join("late.child").exists(),
        "a cancelled op's child ran on: {v}"
    );
    drop(w);
    assert_eq!(joined(done, Duration::from_secs(5)), 0);
}

/// `t`'s `setsid`, if its `PATH` has one.
fn setsid_of(t: &LocalSh) -> Option<String> {
    let mut cmd = Command::new("sh");
    cmd.arg("-c").arg("command -v setsid");
    if let Some(p) = &t.path {
        cmd.env("PATH", p);
    }
    let out = cmd.output().unwrap();
    let found = String::from_utf8_lossy(&out.stdout).trim().to_string();
    (!found.is_empty()).then_some(found)
}

/// A host whose op stops itself before it reaches `setsid(2)`, so its
/// leader is still in the batch's group and leads no session: what a
/// loaded host's scheduler can do to any op. The `setsid` there is a
/// wrapper which, for an op only, records its pid in `dir/leader`, touches
/// `dir/ready`, arranges its own `SIGCONT` 4s later from a process outside
/// its tree (two forks, a session of its own: nothing the op's kill
/// reaches, and finite), and stops. Its `stat` touches `dir/ran`. Resumed,
/// a leader nobody killed runs the op after its deadline or its cancel.
fn stalled_leader_host(t: LocalSh, real: &str, dir: &std::path::Path) -> LocalSh {
    let wrapper = format!(
        "case \" $* \" in *\" mule-op \"*)\n\
         echo $$ > '{d}/leader'; : > '{d}/ready'\n\
         ( {real} sh -c 'sleep 4; kill -CONT \"$1\"' x $$ </dev/null >/dev/null 2>&1 & )\n\
         kill -STOP $$;; esac\n\
         exec {real} \"$@\"",
        d = dir.display()
    );
    let tools = tool_dir("pregroup-setsid", "setsid", &wrapper);
    common::local::write_exec(
        tools.join("stat"),
        format!("#!/bin/sh\n: > '{}/ran'; echo '1 2 3'\n", dir.display()),
    )
    .unwrap();
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(tools.join("stat"), std::fs::Permissions::from_mode(0o755)).unwrap();
    with_tools(t, &tools)
}

/// Every local host with a `setsid`, each with a stalled leader, and the
/// directory its wrapper reports into.
fn stalled_leader_hosts(tag: &str) -> Vec<(&'static str, LocalSh, std::path::PathBuf)> {
    let hosts: Vec<_> = local_hosts()
        .into_iter()
        .filter_map(|(flavour, t)| {
            let real = setsid_of(&t)?;
            let dir = scratch(tag);
            Some((flavour, stalled_leader_host(t, &real, &dir), dir))
        })
        .collect();
    if hosts.is_empty() {
        eprintln!("skipping: no setsid on any local host");
    }
    hosts
}

/// After `end` has ended the stalled op, its leader is dead and the op never
/// runs, even once its resume is due.
fn stalled_leader_never_runs(flavour: &str, dir: &std::path::Path) {
    let leader = std::fs::read_to_string(dir.join("leader")).unwrap();
    let leader = leader.trim();
    wait_until("the stalled leader to die", 3, || !pid_alive(leader));
    std::thread::sleep(Duration::from_millis(4500));
    assert!(
        !dir.join("ran").exists(),
        "{flavour}: an op killed before its setsid ran on"
    );
}

#[test]
fn the_deadline_kills_an_op_stalled_before_its_setsid() {
    if !have_tools() {
        return;
    }
    for (flavour, t, dir) in stalled_leader_hosts("pregroup-dl") {
        let start = Instant::now();
        let out = round_trip(&t, &[Op::Stat("/x".into())]);
        let f = parse_frames(&out.stdout, 1).expect(&out.stderr);
        assert_eq!((f[0].rc, f[0].ended), (124, Ended::TimedOut), "{flavour}");
        assert!(start.elapsed() < Duration::from_secs(4), "{flavour}");
        stalled_leader_never_runs(flavour, &dir);
    }
}

#[test]
fn cancel_kills_an_op_stalled_before_its_setsid() {
    if !have_tools() {
        return;
    }
    for (flavour, t, dir) in stalled_leader_hosts("pregroup-cx") {
        let (mut w, mut r, done) = spawn_serve(Arc::new(t));
        writeln!(w, "{}", json!({"op":"stat","id":1,"path":"/x"})).unwrap();
        wait_until("the op to stall", 5, || dir.join("ready").exists());
        writeln!(w, "{}", json!({"op":"cancel","id":1})).unwrap();
        let v = reply(&mut r);
        assert_eq!(v["kind"], "cancelled", "{flavour}: {v}");
        stalled_leader_never_runs(flavour, &dir);
        drop(w);
        assert_eq!(joined(done, Duration::from_secs(5)), 0, "{flavour}");
    }
}

#[test]
fn caller_eof_kills_an_op_stalled_before_its_setsid() {
    if !have_tools() {
        return;
    }
    for (flavour, t, dir) in stalled_leader_hosts("pregroup-eof") {
        let (mut w, _r, done) = spawn_serve(Arc::new(t));
        writeln!(w, "{}", json!({"op":"stat","id":1,"path":"/x"})).unwrap();
        wait_until("the op to stall", 5, || dir.join("ready").exists());
        drop(w);
        assert_eq!(joined(done, Duration::from_secs(5)), 0, "{flavour}");
        stalled_leader_never_runs(flavour, &dir);
    }
}

/// A host with `setsid` and `pgrep -s` but no `pkill`. Session cleanup was
/// admitted on `pgrep`'s word and done with `pkill`: a missing `pkill` read
/// as a clean session, and a helper that left the op's group but not its
/// session ran after the op's success (review, 2026-11-22).
#[test]
fn a_completed_ops_session_is_cleaned_with_the_tool_that_was_probed() {
    if !have_tools() {
        return;
    }
    let real_setsid = real("setsid");
    let probe = Command::new("sh")
        .arg("-c")
        .arg("pgrep -s 0 >/dev/null")
        .status()
        .is_ok_and(|s| s.success());
    if real_setsid.is_empty() || !probe {
        eprintln!("skipping: no setsid or no pgrep -s here");
        return;
    }
    let dir = std::path::PathBuf::from(path_without("pkill"));
    std::os::unix::fs::symlink(&real_setsid, dir.join("setsid")).unwrap();
    let marker = scratch("no-pkill").join("late");
    // GNU timeout puts itself and its child in a group of their own.
    let body = format!(
        "timeout 5 sh -c 'sleep 1; touch \"$1\"' x '{}' </dev/null >/dev/null 2>&1 &\n\
         echo '1 2 3'",
        marker.display()
    );
    let t = LocalSh {
        path: Some(dir.display().to_string()),
        ..Default::default()
    };
    let t = with_tools(t, &tool_dir("no-pkill-stat", "stat", &body));
    let out = round_trip(&t, &[Op::Stat("/x".into())]);
    let f = parse_frames(&out.stdout, 1).expect(&out.stderr);
    assert_eq!(f[0].rc, 0, "{}", out.stderr);
    std::thread::sleep(Duration::from_millis(2000));
    assert!(
        !marker.exists(),
        "a completed op's helper in its session ran on"
    );
}

/// `setsid` here and a `pgrep -s` that lists this shell, or `None`.
fn session_tools() -> Option<String> {
    let real_setsid = real("setsid");
    let probe = Command::new("sh")
        .arg("-c")
        .arg("pgrep -s 0 >/dev/null")
        .status()
        .is_ok_and(|s| s.success());
    (!real_setsid.is_empty() && probe).then_some(real_setsid)
}

/// A session sweep whose `kill` signals some of its targets and fails on
/// another is not a clean session. One shell `kill` over a `pgrep -s`
/// snapshot returns 1 if any listed pid has exited, though it killed the
/// rest; read as "nothing left", the sweep stopped, and a process forked
/// into the op's session after the snapshot ran on after the op's rc 0
/// (review, 2026-11-22). Here the host's `pgrep` widens that gap: its first
/// session listing waits 0.3s after the snapshot, which the op's helper
/// uses to fork, and every listing names a pid already gone.
#[test]
fn a_partial_session_kill_keeps_sweeping() {
    if !have_tools() {
        return;
    }
    let Some(real_setsid) = session_tools() else {
        eprintln!("skipping: no setsid or no pgrep -s here");
        return;
    };
    let dir = scratch("partial-sweep");
    let pgrep = format!(
        "case \"$*\" in '-s 0') exec {real} \"$@\";; esac\n\
         out=$({real} \"$@\"); rc=$?\n\
         if [ $rc -eq 0 ] && [ ! -e '{d}/go' ]; then : > '{d}/go'; sleep 0.3; fi\n\
         sh -c : & gone=$!; wait $gone\n\
         [ -n \"$out\" ] && printf '%s\\n' \"$out\"; [ $rc -eq 0 ] && echo $gone; exit $rc",
        real = real("pgrep"),
        d = dir.display()
    );
    let tools = tool_dir("partial-sweep-pgrep", "pgrep", &pgrep);
    std::os::unix::fs::symlink(&real_setsid, tools.join("setsid")).unwrap();
    // GNU timeout puts the helper in a group of its own, in the op's
    // session; the op ends once it is up.
    let helper = format!(
        ": > '{d}/up'; while [ ! -e '{d}/go' ]; do sleep 0.01; done\n\
         (sleep 1; : > '{d}/late') & sleep 5",
        d = dir.display()
    );
    let stat = format!(
        "timeout 5 sh -c \"$(printf %s '{}' | base64 -d)\" </dev/null >/dev/null 2>&1 &\n\
         while [ ! -e '{d}/up' ]; do sleep 0.01; done; echo '1 2 3'",
        B64.encode(&helper),
        d = dir.display()
    );
    common::local::write_exec(tools.join("stat"), format!("#!/bin/sh\n{stat}\n")).unwrap();
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(tools.join("stat"), std::fs::Permissions::from_mode(0o755)).unwrap();
    let t = with_tools(LocalSh::default(), &tools);
    let out = round_trip(&t, &[Op::Stat("/x".into())]);
    let f = parse_frames(&out.stdout, 1).expect(&out.stderr);
    assert_eq!(f[0].rc, 0, "{}", out.stderr);
    assert!(dir.join("go").exists(), "the session was never listed");
    std::thread::sleep(Duration::from_millis(2000));
    assert!(
        !dir.join("late").exists(),
        "a process forked into the op's session ran on after a partial kill"
    );
}

// ------------------------------- the local hard kill outlives the caller

/// A shim host whose sessions start, record their pid, and never read
/// stdin or answer: a wedged link with the only session slot taken.
fn stalled_shim(tag: &str) -> (ShimHost, std::path::PathBuf) {
    use std::os::unix::fs::PermissionsExt;
    let shim = ShimHost::new(tag);
    let pidfile = shim.dir.join("session.pid");
    std::fs::write(
        shim.dir.join("ssh"),
        format!(
            "#!/bin/sh\n\
             for arg; do if [ \"$arg\" = -O ]; then echo 'Master running (pid=1)' >&2; exit 0; fi; done\n\
             echo $$ > '{}.tmp' && mv '{p}.tmp' '{p}'\n\
             exec sleep 60\n",
            pidfile.display(),
            p = pidfile.display()
        ),
    )
    .unwrap();
    std::fs::set_permissions(shim.dir.join("ssh"), std::fs::Permissions::from_mode(0o755)).unwrap();
    (shim, pidfile)
}

fn pid_alive(pid: &str) -> bool {
    Command::new("kill")
        .args(["-0", pid])
        .stderr(Stdio::null())
        .status()
        .unwrap()
        .success()
}

/// Start a 2 MiB write into a stalled session, end the caller with `end`,
/// and require the session gone within the local bound plus slack.
fn stalled_session_dies_after(tag: &str, end: impl FnOnce(&mut std::process::Child)) {
    let (shim, pidfile) = stalled_shim(tag);
    let mut child = spawn_binary(&shim);
    let mut w = child.stdin.take().unwrap();
    let start = Instant::now();
    let data = B64.encode(vec![7u8; 2 << 20]);
    let path = shim.dir.join("never");
    writeln!(
        w,
        "{}",
        json!({"op":"write","id":1,"path":path,"data_b64":data})
    )
    .unwrap();
    wait_until("the session to start", 5, || pidfile.exists());
    let pid = std::fs::read_to_string(&pidfile)
        .unwrap()
        .trim()
        .to_string();
    assert!(pid_alive(&pid));
    std::thread::sleep(Duration::from_millis(300));
    let mut w = Some(w);
    end(&mut child);
    drop(w.take());
    let _ = child.wait();
    let bound = mule::serve::LOCAL_KILL + Duration::from_secs(4);
    while pid_alive(&pid) && start.elapsed() < bound {
        std::thread::sleep(Duration::from_millis(100));
    }
    let alive = pid_alive(&pid);
    let _ = Command::new("kill").args(["-KILL", &pid]).status();
    assert!(!alive, "the session outlived its caller past {:?}", bound);
}

#[test]
fn a_stalled_session_dies_on_time_after_its_caller_hangs_up() {
    if !have_tools() {
        return;
    }
    // Stdin EOF, and a SIGHUP as a dying terminal sends.
    stalled_session_dies_after("stall-eof", |child| {
        let _ = Command::new("kill")
            .args(["-HUP", &child.id().to_string()])
            .status();
    });
}

#[test]
fn a_stalled_session_dies_on_time_after_serve_is_sigkilled() {
    if !have_tools() {
        return;
    }
    stalled_session_dies_after("stall-kill", |child| child.kill().unwrap());
}

// --------------------------------- a completed op is never "cancelled"

#[test]
fn a_cancel_that_loses_the_race_reports_the_real_result() {
    if !have_tools() {
        return;
    }
    let p = scratch("cancel-race").join("done");
    let t = stat_host("cancel-race-stat", "sleep 0.6; echo '1 2 3'");
    let (mut w, mut r, done) = spawn_serve(Arc::new(t));
    let data = B64.encode(b"landed");
    // One write(2), so both are in the same batch.
    let both = format!(
        "{}\n{}\n",
        json!({"op":"write","id":1,"path":p,"data_b64":data}),
        json!({"op":"stat","id":2,"path":"/x"})
    );
    w.write_all(both.as_bytes()).unwrap();
    wait_until("the write to land", 5, || p.exists());
    std::thread::sleep(Duration::from_millis(150));
    writeln!(w, "{}", json!({"op":"cancel","id":1})).unwrap();
    let mut got = std::collections::HashMap::new();
    for _ in 0..2 {
        let v = reply(&mut r);
        got.insert(v["id"].as_u64().unwrap(), v);
    }
    let v = &got[&1];
    assert_eq!(v["batch_size"], 2, "{v}");
    assert_eq!(
        (v["rc"].as_i64(), &v["cancelled"], v.get("kind")),
        (Some(0), &json!(false), None),
        "a completed write was reported cancelled: {v}"
    );
    assert_eq!(v["cancel_requested"], true, "{v}");
    assert_eq!(std::fs::read(&p).unwrap(), b"landed");
    assert_eq!(got[&2]["rc"], 0, "{}", got[&2]);
    drop(w);
    assert_eq!(joined(done, Duration::from_secs(5)), 0);
}

// ------------------------------------------- the spool is bounded on disk

/// Bytes of op output spooled under `tmp` right now.
fn spooled(tmp: &std::path::Path) -> u64 {
    let mut total = 0;
    for d in std::fs::read_dir(tmp).into_iter().flatten().flatten() {
        for f in std::fs::read_dir(d.path()).into_iter().flatten().flatten() {
            let name = f.file_name().to_string_lossy().into_owned();
            if name.starts_with("out.") || name.starts_with("err.") {
                total += f.metadata().map(|m| m.len()).unwrap_or(0);
            }
        }
    }
    total
}

#[test]
fn a_batch_never_spools_more_than_its_output_budget() {
    if !have_tools() {
        return;
    }
    let tmp = scratch("spool");
    let src = scratch("spool-src").join("spoolsrc");
    std::fs::write(&src, vec![0u8; MAX_OUT as usize]).unwrap();
    // A `head` that lingers 0.3s after reading this file, so the ops'
    // spools overlap for the sampler to see; every other `head` (the
    // script's own) is the real one.
    let head = tool_dir(
        "spool-head",
        "head",
        &format!(
            "{} \"$@\"; rc=$?; case \"$*\" in *spoolsrc*) sleep 0.3;; esac; exit $rc",
            real("head")
        ),
    );
    let t = Arc::new(with_tools(
        LocalSh {
            tmpdir: Some(tmp.clone()),
            ..Default::default()
        },
        &head,
    ));
    let (mut w, mut r, done) = spawn_serve(t);
    let peak = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let sampler = {
        let (tmp, peak, stop) = (tmp.clone(), peak.clone(), stop.clone());
        std::thread::spawn(move || {
            while !stop.load(Ordering::SeqCst) {
                peak.fetch_max(spooled(&tmp), Ordering::SeqCst);
                std::thread::sleep(Duration::from_millis(5));
            }
        })
    };
    let mut burst = String::new();
    for i in 0..10 {
        burst.push_str(&format!("{}\n", json!({"op":"read","id":i,"path":src})));
    }
    w.write_all(burst.as_bytes()).unwrap();
    for _ in 0..10 {
        let v = reply(&mut r);
        let out = B64.decode(v["stdout_b64"].as_str().unwrap()).unwrap();
        assert_eq!(
            (v["rc"].as_i64(), out.len() as u64, &v["truncated"]),
            (Some(0), MAX_OUT, &json!(false)),
            "{}",
            v["error"]
        );
    }
    stop.store(true, Ordering::SeqCst);
    sampler.join().unwrap();
    let peak = peak.load(Ordering::SeqCst);
    assert!(
        peak <= mule::serve::MAX_BATCH_OUTPUT,
        "spooled {peak} bytes on the host, past the {} budget",
        mule::serve::MAX_BATCH_OUTPUT
    );
    assert!(peak >= MAX_OUT, "the sampler saw no spool ({peak})");
    drop(w);
    assert_eq!(joined(done, Duration::from_secs(5)), 0);
    assert_eq!(spooled(&tmp), 0);
    assert_eq!(
        std::fs::read_dir(&tmp).unwrap().count(),
        0,
        "a batch left its spool behind"
    );
}

#[test]
fn an_op_past_its_output_cap_fails() {
    if !have_tools() {
        return;
    }
    let big = scratch("past-cap").join("big");
    std::fs::write(&big, vec![0u8; MAX_OUT as usize * 2]).unwrap();
    let (mut w, mut r, done) = spawn_serve(Arc::new(LocalSh::default()));
    writeln!(w, "{}", json!({"op":"read","id":1,"path":big})).unwrap();
    let v = reply(&mut r);
    assert_eq!(
        (&v["truncated"], &v["kind"]),
        (&json!(true), &json!("output_limit")),
        "{v}"
    );
    assert!(v["error"].as_str().is_some(), "{v}");
    drop(w);
    assert_eq!(joined(done, Duration::from_secs(5)), 0);
}

// ----------------------------------- write replaces exactly what it names

#[test]
fn write_replaces_only_the_named_file() {
    if !have_tools() {
        return;
    }
    let dir = scratch("write-dest");
    let sub = dir.join("sub");
    std::fs::create_dir(&sub).unwrap();
    let target = dir.join("target");
    std::fs::write(&target, "keep").unwrap();
    let link = dir.join("link");
    std::os::unix::fs::symlink(&target, &link).unwrap();
    let file = dir.join("file");
    let (mut w, mut r, done) = spawn_serve(Arc::new(LocalSh::default()));
    let data = B64.encode(b"new");
    let mut send = |id: u64, path: &str| {
        writeln!(
            w,
            "{}",
            json!({"op":"write","id":id,"path":path,"data_b64":data})
        )
        .unwrap();
        reply(&mut r)
    };
    let v = send(1, sub.to_str().unwrap());
    assert_ne!(v["rc"], 0, "a write onto a directory succeeded: {v}");
    assert!(sub.is_dir());
    assert_eq!(
        std::fs::read_dir(&sub).unwrap().count(),
        0,
        "a write landed inside the directory it named"
    );
    let v = send(2, link.to_str().unwrap());
    assert_ne!(v["rc"], 0, "a write through a symlink succeeded: {v}");
    assert!(std::fs::symlink_metadata(&link).unwrap().is_symlink());
    assert_eq!(std::fs::read(&target).unwrap(), b"keep");
    let v = send(3, "relative/path");
    assert_eq!(v["kind"], "bad_request", "{v}");
    let v = send(4, file.to_str().unwrap());
    assert_eq!(v["rc"], 0, "{v}");
    assert_eq!(std::fs::read(&file).unwrap(), b"new");
    // The reply names the path written, resolved.
    let named = B64.decode(v["stdout_b64"].as_str().unwrap()).unwrap();
    assert_eq!(
        String::from_utf8(named).unwrap().trim_end(),
        file.canonicalize().unwrap().to_str().unwrap()
    );
    let names: Vec<_> = std::fs::read_dir(&dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    assert_eq!(names.len(), 4, "a temp file was left behind: {names:?}");
    drop(w);
    assert_eq!(joined(done, Duration::from_secs(5)), 0);
}

/// A replaced file keeps its mode from the moment its new bytes are
/// visible. The rename publishes the temp file's inode, so its mode must be
/// the target's before `mv`: restored by a chmod after it, a 0600 file sat
/// at 0644 until the chmod ran, and stayed there when it failed or the
/// caller aborted (review, 2026-11-23, Linux host). An `mv` shim records the
/// temp file's mode as it publishes it; a second shim fails the `mv`, and the
/// old bytes and mode stay.
#[test]
fn a_write_publishes_the_target_mode_never_a_wider_one() {
    if !have_tools() {
        return;
    }
    use std::os::unix::fs::PermissionsExt;
    let dir = scratch("write-mode");
    let seen = dir.join("seen");
    // `ls -ln` is the same on GNU and BSD: the mode string comes first.
    let shim = |fail: bool| {
        tool_dir(
            &format!("write-mode-mv-{fail}"),
            "mv",
            &format!(
                "for a; do t=$p; p=$a; done; ls -ln \"$t\" | cut -c1-10 >> '{}'; {}",
                seen.display(),
                if fail {
                    "exit 1".into()
                } else {
                    format!("exec {} \"$@\"", real("mv"))
                }
            ),
        )
    };
    let modes = [
        (0o600, "-rw-------"),
        (0o640, "-rw-r-----"),
        (0o755, "-rwxr-xr-x"),
    ];
    for fail in [false, true] {
        let t = with_tools(LocalSh::default(), &shim(fail));
        let _ = std::fs::remove_file(&seen);
        let mut ops = Vec::new();
        for (mode, _) in modes {
            let f = dir.join(format!("f{mode:o}"));
            std::fs::write(&f, "old").unwrap();
            std::fs::set_permissions(&f, std::fs::Permissions::from_mode(mode)).unwrap();
            ops.push(Op::Write(f.display().to_string(), b"new".to_vec()));
        }
        let out = round_trip(&t, &ops);
        let f = parse_frames(&out.stdout, 3).expect(&out.stderr);
        // The batch's ops run concurrently: the order is any.
        let mut published: Vec<String> = std::fs::read_to_string(&seen)
            .unwrap()
            .lines()
            .map(str::to_owned)
            .collect();
        published.sort();
        let mut want: Vec<&str> = modes.iter().map(|m| m.1).collect();
        want.sort();
        assert_eq!(
            published, want,
            "a temp file was published wider than its target"
        );
        for (i, (mode, _)) in modes.iter().enumerate() {
            let p = dir.join(format!("f{mode:o}"));
            let got = std::fs::metadata(&p).unwrap().permissions().mode() & 0o7777;
            assert_eq!(got, *mode, "{}", p.display());
            let body = std::fs::read(&p).unwrap();
            if fail {
                assert_ne!(f[i].rc, 0);
                assert_eq!(body, b"old");
            } else {
                assert_eq!(f[i].rc, 0, "{}", String::from_utf8_lossy(&f[i].stderr));
                assert_eq!(body, b"new");
            }
        }
    }
}

/// A sibling already holding a name the write would use for its temp file
/// is not the write's: it is neither written through nor deleted. The temp
/// name was `.<b>.mule-serve.$$`, created under `set -C` and `rm -f`'d when
/// that failed, so a collision deleted the caller's file, rc 1 (review,
/// 2026-11-23, Linux host). An `sh` shim plants the siblings at the body's own
/// pid, then execs the real shell, which keeps that pid.
#[test]
fn a_write_never_touches_a_sibling_on_its_temp_name() {
    if !have_tools() {
        return;
    }
    let dir = scratch("temp-collide").canonicalize().unwrap();
    let target = dir.join("file");
    std::fs::write(&target, "keep").unwrap();
    let sh = tool_dir(
        "temp-collide-sh",
        "sh",
        &format!(
            "if [ \"$3\" = mule-body ]; then s=\"${{4%/*}}/.${{4##*/}}.mule-serve.$$\"; \
             for n in \"$s\" \"$s.0\" \"$s.1\"; do printf sentinel > \"$n\"; done; fi\n\
             exec {} \"$@\"",
            real("sh")
        ),
    );
    let v = one_reply(
        with_tools(LocalSh::default(), &sh),
        json!({"op":"write","id":1,"path":target,"data_b64":B64.encode(b"new")}),
    );
    let mut planted = 0;
    for e in std::fs::read_dir(&dir).unwrap() {
        let p = e.unwrap().path();
        if p != target {
            planted += 1;
            assert_eq!(std::fs::read(&p).unwrap(), b"sentinel", "{p:?}: {v}");
        }
    }
    assert_eq!(
        planted, 3,
        "a planted sibling is gone, or a temp file left: {v}"
    );
    assert_eq!(v["rc"], 0, "{v}");
    assert_eq!(std::fs::read(&target).unwrap(), b"new", "{v}");
}

/// A name in the destination's directory is anyone's until an exclusive
/// create takes it. The temp file was checked (`-e`, `-L`) and then created
/// under `set -C`, which opens an existing FIFO: one planted between the two
/// received the payload, and the failed size check then `rm -f`'d it
/// (review, 2026-11-23, Linux host, an `LD_PRELOAD` pause). Here a bash DEBUG
/// trap is that stranger: before any command that names the temp path `$t`
/// while it lies in the destination's directory `$r`, other than a `[`
/// test, it plants a FIFO there with a reader. A write that never opens a
/// shared-directory name it did not create exclusively gives it no chance.
#[test]
fn a_fifo_planted_on_the_temp_name_after_its_check_is_never_opened() {
    if !have_tools() || !std::path::Path::new("/bin/bash").exists() {
        eprintln!("skipping: needs /bin/bash");
        return;
    }
    let dir = scratch("temp-race").canonicalize().unwrap();
    let side = scratch("temp-race-side");
    let target = dir.join("file");
    std::fs::write(&target, "keep").unwrap();
    let (planted, cap) = (side.join("planted"), side.join("cap"));
    let hook = side.join("hook");
    std::fs::write(
        &hook,
        format!(
            "set -T\n\
             trap 'case $BASH_COMMAND in \"[\"*) ;; *\\\"\\$t\\\"*) \
             if [ -n \"$t\" ] && [ \"${{t%/*}}\" = \"${{r%/}}\" ] && [ ! -e \"$t\" ] \
             && [ ! -e {p} ] && mkfifo \"$t\"; then : > {p}; \
             {{ cat \"$t\"; cat \"$t\"; : > \"$t\"; }} >> {c} 2>/dev/null & fi;; esac' DEBUG\n",
            p = planted.display(),
            c = cap.display(),
        ),
    )
    .unwrap();
    let sh = tool_dir(
        "temp-race-sh",
        "sh",
        &format!(
            "if [ \"$3\" = mule-body ]; then exec /bin/bash -c \". '{}'\n$2\" \"$3\" \"$4\"; fi\n\
             exec {} \"$@\"",
            hook.display(),
            real("sh")
        ),
    );
    let v = one_reply(
        with_tools(LocalSh::default(), &sh),
        json!({"op":"write","id":1,"path":target,"data_b64":B64.encode(b"new")}),
    );
    let cap = std::fs::read(&cap).unwrap_or_default();
    assert!(cap.is_empty(), "the planted FIFO got {cap:?}: {v}");
    let fifo_left = std::fs::read_dir(&dir).unwrap().any(|e| {
        use std::os::unix::fs::FileTypeExt;
        e.unwrap().file_type().unwrap().is_fifo()
    });
    assert_eq!(
        fifo_left,
        planted.exists(),
        "a planted FIFO was removed: {v}"
    );
    assert_eq!(v["rc"], 0, "{v}");
    assert_eq!(std::fs::read(&target).unwrap(), b"new", "{v}");
    let left = std::fs::read_dir(&dir).unwrap().count();
    assert_eq!(
        left,
        1 + usize::from(fifo_left),
        "a temp was left behind: {v}"
    );
}

// --------------------------- paths are data, and spool I/O is checked

/// One request through a fresh serve over `t`, and its reply.
fn one_reply(t: LocalSh, req: Value) -> Value {
    let (mut w, mut r, done) = spawn_serve(Arc::new(t));
    writeln!(w, "{req}").unwrap();
    let v = reply(&mut r);
    drop(w);
    assert_eq!(joined(done, Duration::from_secs(5)), 0);
    v
}

/// A parent whose name ends in a newline is that parent: `$(pwd -P)` strips
/// every trailing newline, and a write to `parent\n/file` replaced
/// `parent/file`, rc 0 (review, 2026-11-20).
#[test]
fn a_write_under_a_parent_ending_in_a_newline_lands_there() {
    if !have_tools() {
        return;
    }
    let dir = scratch("nl-parent");
    let (plain, nl) = (dir.join("parent"), dir.join("parent\n"));
    for d in [&plain, &nl] {
        std::fs::create_dir(d).unwrap();
        std::fs::write(d.join("file"), "keep").unwrap();
    }
    let path = nl.join("file");
    let v = one_reply(
        LocalSh::default(),
        json!({"op":"write","id":1,"path":path,"data_b64":B64.encode(b"new")}),
    );
    assert_eq!(v["rc"], 0, "{v}");
    assert_eq!(std::fs::read(plain.join("file")).unwrap(), b"keep", "{v}");
    assert_eq!(std::fs::read(&path).unwrap(), b"new", "{v}");
    let named = B64.decode(v["stdout_b64"].as_str().unwrap()).unwrap();
    let want = format!("{}\n", path.canonicalize().unwrap().display());
    assert_eq!(String::from_utf8_lossy(&named), want);
}

/// A shell string cannot hold a NUL: `a\0b` reached the host as `ab`.
#[test]
fn a_path_holding_a_nul_is_refused_before_any_ssh() {
    let dir = scratch("nul-path");
    let path = format!("{}/a\u{0}b", dir.display());
    for req in [
        json!({"op":"write","id":1,"path":path,"data_b64":B64.encode(b"x")}),
        json!({"op":"read","id":1,"path":path}),
        json!({"op":"stat","id":1,"path":path}),
    ] {
        let t = Arc::new(LocalSh::default());
        let (mut w, mut r, done) = spawn_serve(t.clone());
        writeln!(w, "{req}").unwrap();
        let v = reply(&mut r);
        drop(w);
        assert_eq!(joined(done, Duration::from_secs(5)), 0);
        assert_eq!(v["kind"], "bad_request", "{v}");
        assert_eq!(t.rounds.load(Ordering::SeqCst), 0, "{v}");
    }
    assert!(!dir.join("ab").exists());
}

/// A host whose `tail` cuts the batch's payload slice short, and fails: the
/// write replaced `keep` with `abc` and returned rc 1 (review, 2026-11-20).
/// A short slice now fails the batch before any op runs.
#[test]
fn a_short_payload_slice_replaces_nothing() {
    if !have_tools() {
        return;
    }
    let target = scratch("short-slice").join("file");
    std::fs::write(&target, "keep").unwrap();
    let tail = tool_dir(
        "short-slice-tail",
        "tail",
        &format!(
            "case \"$*\" in */in) printf abc; exit 1;; esac; exec {} \"$@\"",
            real("tail")
        ),
    );
    let v = one_reply(
        with_tools(LocalSh::default(), &tail),
        json!({"op":"write","id":1,"path":target,"data_b64":B64.encode(b"payload")}),
    );
    assert_ne!(v["rc"], 0, "{v}");
    assert!(v["kind"].is_string(), "{v}");
    assert_eq!(std::fs::read(&target).unwrap(), b"keep", "{v}");
}

/// A host whose `cat` writes the temp file short and exits 0: checked only
/// after the rename, the short file had already replaced the old one.
#[test]
fn a_short_temp_file_is_never_renamed_over_the_target() {
    if !have_tools() {
        return;
    }
    let dir = scratch("short-temp");
    let target = dir.join("file");
    std::fs::write(&target, "keep").unwrap();
    let hit = dir.join("hit");
    // The write's `cat` reads the payload file; the collectors' drain reads
    // a pipe.
    let cat = tool_dir(
        "short-temp-cat",
        "cat",
        &format!(
            "if [ -f /dev/stdin ]; then : > '{}'; head -c 3; exit 0; fi; exec {} \"$@\"",
            hit.display(),
            real("cat")
        ),
    );
    let v = one_reply(
        with_tools(LocalSh::default(), &cat),
        json!({"op":"write","id":1,"path":target,"data_b64":B64.encode(b"payload")}),
    );
    if !hit.exists() {
        eprintln!("skipping: /dev/stdin is not a regular file here");
        return;
    }
    assert_ne!(v["rc"], 0, "{v}");
    assert_eq!(std::fs::read(&target).unwrap(), b"keep", "{v}");
}

/// A host whose stdout collector fails (a full disk): the drain after it
/// hid the failure, and a read replied rc 0, empty, whole (review,
/// 2026-11-20).
#[test]
fn a_failed_output_collector_is_no_result() {
    if !have_tools() {
        return;
    }
    let src = scratch("spool-fail").join("src");
    std::fs::write(&src, "hello").unwrap();
    // The collector is the only `head -c <cap+1>` with no file operand.
    let head = tool_dir(
        "spool-fail-head",
        "head",
        &format!(
            "case \"$*\" in '-c {}') {} > /dev/null; \
             echo 'head: write error: No space left on device' >&2; exit 1;; esac\n\
             exec {} \"$@\"",
            MAX_OUT + 1,
            real("cat"),
            real("head")
        ),
    );
    let v = one_reply(
        with_tools(LocalSh::default(), &head),
        json!({"op":"read","id":1,"path":src}),
    );
    assert_eq!(v["kind"], "spool_failed", "{v}");
    assert_ne!(v["rc"], 0, "{v}");
    assert!(v["error"].is_string(), "{v}");
}

// ------------------- every check must succeed; a '-' path is the file '-'

/// A `wc` that fails, printing nothing, on a stdin-only count: of every
/// file, or with `only_write` only in the write op's body (its parent shell
/// is `mule-body`). It touches `hit` when it fails. Arithmetic
/// `$(($(wc ...)))` dropped the status and read the empty output as 0, so
/// an empty write replaced the file, rc 0 (review, 2026-11-22, Linux host).
fn failing_wc(tag: &str, only_write: bool, hit: &std::path::Path) -> std::path::PathBuf {
    let only = if only_write {
        "case $(ps -o args= -p $PPID) in *mule-body*) ;; *) exec /usr/bin/env -i wc \"$@\";; esac; "
    } else {
        ""
    };
    let only = only.replace("/usr/bin/env -i wc", &real("wc"));
    tool_dir(
        tag,
        "wc",
        &format!(
            "if [ \"$*\" = -c ]; then {only}: > '{}'; echo 'wc: read error: Input/output error' >&2; exit 1; fi\n\
             exec {} \"$@\"",
            hit.display(),
            real("wc")
        ),
    )
}

#[test]
fn a_failed_length_check_replaces_nothing() {
    if !have_tools() {
        return;
    }
    for only_write in [true, false] {
        let dir = scratch("wc-fail");
        let target = dir.join("file");
        std::fs::write(&target, "keep").unwrap();
        let hit = dir.join("hit");
        let wc = failing_wc("wc-fail-tool", only_write, &hit);
        let v = one_reply(
            with_tools(LocalSh::default(), &wc),
            json!({"op":"write","id":1,"path":target,"data_b64":""}),
        );
        assert!(
            hit.exists(),
            "only_write={only_write}: the fault never ran: {v}"
        );
        assert_ne!(v["rc"], 0, "only_write={only_write}: {v}");
        assert_eq!(std::fs::read(&target).unwrap(), b"keep", "{v}");
    }
}

/// A `head` that copies the whole payload and then fails, on its `nth`
/// payload copy: 1 is the input spool, 2 the op's slice. Both statuses were
/// dropped, and a copy that failed after its bytes replaced the file, rc 0
/// (review, 2026-11-22, Linux host).
#[test]
fn a_failed_payload_copy_replaces_nothing() {
    if !have_tools() {
        return;
    }
    for nth in [1, 2] {
        let dir = scratch("copy-fail");
        let target = dir.join("file");
        std::fs::write(&target, "keep").unwrap();
        let count = dir.join("count");
        let head = tool_dir(
            "copy-fail-head",
            "head",
            &format!(
                "if [ \"$*\" = '-c 7' ]; then n=$(cat '{c}' 2>/dev/null); n=$((${{n:-0}}+1)); \
                 echo $n > '{c}'; if [ $n -eq {nth} ]; then {h} \"$@\"; \
                 echo 'head: write error: No space left on device' >&2; exit 1; fi; fi\n\
                 exec {h} \"$@\"",
                c = count.display(),
                h = real("head")
            ),
        );
        let v = one_reply(
            with_tools(LocalSh::default(), &head),
            json!({"op":"write","id":1,"path":target,"data_b64":B64.encode(b"payload")}),
        );
        assert!(count.exists(), "the fault never ran");
        assert_ne!(v["rc"], 0, "copy {nth}: {v}");
        assert!(v["kind"].is_string(), "{v}");
        assert_eq!(std::fs::read(&target).unwrap(), b"keep", "copy {nth}: {v}");
    }
}

/// A host whose `pgrep -s <sid>` is `sweep` (the probe's `pgrep -s 0` is
/// the real one), and so whose ops are sessions. Where this machine has no
/// `setsid` (macOS) one is made from `python3`'s `os.setsid`, and the probe
/// answered with the batch shell's pid, so the sweep is driven the same
/// way; `None` if neither works.
fn sweep_host(tag: &str, sweep: &str, extra: &[(&str, &str)]) -> Option<LocalSh> {
    if real("pgrep").is_empty() || real("ps").is_empty() {
        return None;
    }
    let native = setsid_of(&LocalSh::default()).is_some();
    if !native && real("python3").is_empty() {
        return None;
    }
    let probe = if native {
        format!("exec {} \"$@\"", real("pgrep"))
    } else {
        "echo $PPID; exit 0".into()
    };
    let dir = tool_dir(
        tag,
        "pgrep",
        &format!(
            "case \"$*\" in '-s 0') {probe};; -s*) {sweep};; esac\nexec {} \"$@\"",
            real("pgrep")
        ),
    );
    use std::os::unix::fs::PermissionsExt;
    for (tool, body) in extra {
        common::local::write_exec(dir.join(tool), format!("#!/bin/sh\n{body}\n")).unwrap();
        std::fs::set_permissions(dir.join(tool), std::fs::Permissions::from_mode(0o755)).unwrap();
    }
    if !native {
        common::local::write_exec(
            dir.join("setsid"),
            format!(
                "#!/bin/sh\nexec {} -c 'import os, sys; os.setsid(); os.execvp(sys.argv[1], sys.argv[1:])' \"$@\"\n",
                real("python3")
            ),
        )
        .unwrap();
        std::fs::set_permissions(dir.join("setsid"), std::fs::Permissions::from_mode(0o755))
            .unwrap();
    }
    Some(with_tools(LocalSh::default(), &dir))
}

/// A host whose `pgrep -s` passes the probe and then errors on every op
/// session: the sweep can never prove the session empty. Its retry cap was
/// dropped and the op replied rc 0 while a process of its session could
/// live on (review, 2026-11-22, Linux host).
#[test]
fn a_sweep_that_cannot_prove_the_session_empty_is_no_success() {
    if !have_tools() {
        return;
    }
    let Some(t) = sweep_host(
        "sweep-err-pgrep",
        "echo 'pgrep: fatal error' >&2; exit 3",
        &[],
    ) else {
        eprintln!("skipping: no pgrep, ps, or setsid (or python3)");
        return;
    };
    let dir = scratch("sweep-err");
    let v = one_reply(t, json!({"op":"stat","id":1,"path":dir}));
    assert_eq!(v["kind"], "cleanup_failed", "{v}");
    assert_ne!(v["rc"], 0, "{v}");
}

/// What the sweep still finds after it KILLed everything may be zombies a
/// parent outside the op has not reaped; they run nothing, so an op whose
/// session lists only zombies is clean.
#[test]
fn a_sweep_that_finds_only_zombies_is_clean() {
    if !have_tools() {
        return;
    }
    // A child of this test, never waited on until the end: a zombie.
    let mut zombie = Command::new("true").spawn().unwrap();
    let zpid = zombie.id();
    wait_until("a zombie", 5, || {
        Command::new("ps")
            .args(["-o", "stat=", "-p", &zpid.to_string()])
            .output()
            .is_ok_and(|o| {
                String::from_utf8_lossy(&o.stdout)
                    .trim_start()
                    .starts_with('Z')
            })
    });
    let Some(t) = sweep_host("sweep-zombie-pgrep", &format!("echo {zpid}; exit 0"), &[]) else {
        eprintln!("skipping: no pgrep, ps, or setsid (or python3)");
        zombie.wait().unwrap();
        return;
    };
    let dir = scratch("sweep-zombie");
    let v = one_reply(t, json!({"op":"stat","id":1,"path":dir}));
    zombie.wait().unwrap();
    assert_eq!(v["rc"], 0, "{v}");
    assert!(v["kind"].is_null(), "{v}");
}

/// A sweep certifies the op's session clean only from an observation taken
/// after its last kill. At the retry cap `Q` checked the pids of the last
/// `pgrep` snapshot, taken before that kill, and the op's first group: a
/// listed process that forked into a new group of the same session between
/// the snapshot and the KILL was missed, and the op replied rc 0 while that
/// child ran on (review, 2026-11-23, Linux host). Here the op leaves a helper
/// in a group of its own, and the host's `pgrep` makes it hand over to a
/// fresh child in another new group after every snapshot, so no snapshot
/// ever names the process alive after the kill.
#[test]
fn a_sweep_proof_looks_at_the_session_after_its_last_kill() {
    if !have_tools() {
        return;
    }
    if real("perl").is_empty() {
        eprintln!("skipping: no perl");
        return;
    }
    let dir = scratch("stale-sweep");
    let d = dir.display();
    // Bounded: 10s, or `stop`. A new group, same session: perl's setpgrp.
    let fork = format!(
        "{} -e 'setpgrp(0,0); exec @ARGV' sh '{d}/h' </dev/null >/dev/null 2>&1 &",
        real("perl")
    );
    std::fs::write(
        dir.join("h"),
        format!(
            "echo $$ > '{d}/cur'; n=0; while [ $n -lt 1000 ]; do [ -e '{d}/stop' ] && exit 0\n\
             if [ -e '{d}/go' ]; then rm -f '{d}/go'; {fork} : > '{d}/ack'; exit 0; fi\n\
             sleep 0.01; n=$((n+1)); done"
        ),
    )
    .unwrap();
    let sweep = format!(
        "c=$(cat '{d}/cur'); rm -f '{d}/ack'; : > '{d}/go'; n=0\n\
         while [ ! -e '{d}/ack' ] && [ $n -lt 200 ]; do sleep 0.01; n=$((n+1)); done\n\
         echo $c; exit 0"
    );
    let stat = format!("{fork}\nwhile [ ! -s '{d}/cur' ]; do sleep 0.01; done; echo '1 2 3'");
    let Some(t) = sweep_host("stale-sweep-pgrep", &sweep, &[("stat", &stat)]) else {
        eprintln!("skipping: no pgrep, ps, or setsid (or python3)");
        return;
    };
    let v = one_reply(t, json!({"op":"stat","id":1,"path":dir}));
    let handed_over = dir.join("ack").exists();
    std::fs::write(dir.join("stop"), "").unwrap();
    assert!(handed_over, "the helper never forked: {v}");
    assert_eq!(v["kind"], "cleanup_failed", "{v}");
    assert_ne!(v["rc"], 0, "{v}");
}

/// A read is whole only if every byte up to EOF was delivered. The
/// deliverer ignored a failed read of the caps spool: the bounds stayed
/// empty, the comparisons errored and fell through uncut, and a read the
/// host had stopped at its cap + 1 replied rc 0, whole, one byte past the
/// cap (review, 2026-11-23, Linux host). Here the delivery's `wc` empties
/// `caps`, or prints no size rows, just before the deliverer reads them.
#[test]
fn a_short_caps_or_sizes_spool_is_no_whole_read() {
    if !have_tools() {
        return;
    }
    let dir = scratch("short-caps");
    let src = dir.join("src");
    std::fs::write(&src, vec![b'x'; MAX_OUT as usize + 256]).unwrap();
    for fault in ["caps", "sizes"] {
        let hit = dir.join(format!("hit-{fault}"));
        // The delivery's `wc` is the only one with file operands: out, err.
        let act = match fault {
            "caps" => format!(": > \"${{2%/*}}/caps\"; exec {} \"$@\"", real("wc")),
            _ => "exit 0".into(),
        };
        let wc = tool_dir(
            "short-caps-wc",
            "wc",
            &format!(
                "if [ \"$1\" = -c ] && [ $# -ge 3 ]; then : > '{}'; {act}; fi\nexec {} \"$@\"",
                hit.display(),
                real("wc")
            ),
        );
        let v = one_reply(
            with_tools(LocalSh::default(), &wc),
            json!({"op":"read","id":1,"path":src}),
        );
        assert!(hit.exists(), "{fault}: the fault never ran: {v}");
        assert!(
            v["kind"].is_string(),
            "{fault}: a short spool read as whole: {v}"
        );
        assert_ne!(v["rc"], 0, "{fault}: {v}");
        let got = B64.decode(v["stdout_b64"].as_str().unwrap()).unwrap();
        assert!(
            got.len() as u64 <= MAX_OUT,
            "{fault}: past the cap: {}",
            got.len()
        );
    }
}

/// `--` ends option parsing but leaves `-` meaning stdin to `head` and
/// `stat`: a read of the file named `-` replied empty, a stat size 0, both
/// rc 0 (review, 2026-11-22, Linux host).
#[test]
fn a_path_named_dash_is_the_file_named_dash() {
    if !have_tools() {
        return;
    }
    let dir = scratch("dash-path");
    std::fs::write(dir.join("-"), "sixteen bytes!!\n").unwrap();
    // BSD `head` and `stat` read a `-` operand as a file; GNU's, as stdin.
    // Where this machine is not GNU but has GNU's as `ghead` and `gstat`
    // (Homebrew), a host of those is checked too.
    let mut hosts = vec![("native", None)];
    if !real("ghead").is_empty() && !real("gstat").is_empty() {
        let gnu = scratch("dash-gnu");
        for (tool, to) in [("head", "ghead"), ("stat", "gstat")] {
            std::os::unix::fs::symlink(real(to), gnu.join(tool)).unwrap();
        }
        common::local::write_exec(gnu.join("uname"), "#!/bin/sh\necho Linux\n").unwrap();
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(gnu.join("uname"), std::fs::Permissions::from_mode(0o755))
            .unwrap();
        hosts.push(("gnu", Some(gnu)));
    }
    for (name, tools) in hosts {
        let host = || {
            let t = LocalSh {
                cwd: Some(dir.clone()),
                ..LocalSh::default()
            };
            match &tools {
                Some(d) => with_tools(t, d),
                None => t,
            }
        };
        let v = one_reply(host(), json!({"op":"read","id":1,"path":"-"}));
        assert_eq!(v["rc"], 0, "{name}: {v}");
        let got = B64.decode(v["stdout_b64"].as_str().unwrap()).unwrap();
        assert_eq!(got, b"sixteen bytes!!\n", "{name}: {v}");
        let v = one_reply(host(), json!({"op":"stat","id":1,"path":"-"}));
        assert_eq!(v["rc"], 0, "{name}: {v}");
        let out = B64.decode(v["stdout_b64"].as_str().unwrap()).unwrap();
        assert!(out.starts_with(b"16 "), "{name}: {v}");
    }
}
