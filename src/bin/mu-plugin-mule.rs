//! mu-plugin-mule: serves mu's plugin protocol (v1) for the link seam, over
//! the `mule` binary installed beside it. See docs/usage.md, "As a mu plugin".
//!
//!   mu plugin add mule -- /abs/path/mu-plugin-mule [--config PATH]
//!   mu host add box --plugin mule --fetch '<fetch cmd>'
//!
//! The mu host's name is the mule host's name. Declared: `exec` only
//! (`fetch: false`, so the host profile keeps its own `--fetch`), `slots:
//! "self"` (mule's ticket lock arbitrates; mu holds no channel), and
//! `quiescence: "provable"`.
//!
//! Every call is a `mule` child in THIS process group, never detached
//! (protocol, "Process"): `exec --no-detach` stops the remote job from inside
//! that group, and mu kills the group a grace after it cancels.

use std::collections::HashMap;
use std::io::{BufRead, Read, Write};
use std::os::unix::process::ExitStatusExt;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use serde_json::{Value, json};

/// sha256 of `schemas/plugin-protocol-v1.json` in mu: mu refuses any other.
const SCHEMA: &str = "sha256:6ea6e13cabf6baebe44e961e21370a66ddf523082bfd73bcf169ec627fc71a25";
/// Per stream, read from the child. Not the reply bound: see `ENCODED_STREAM`.
const MAX_OUTPUT: usize = 128 * 1024;
/// Per stream, AFTER JSON escaping. mu rejects a protocol line over 1 MiB
/// (1,048,576) and poisons the plugin for the invocation, so the bound is on
/// the encoded reply: NUL is six bytes (`\u0000`), measured 1,572,953 for two
/// 128 KiB NUL streams. 2 x 480,000 leaves 88 KiB for the envelope.
const ENCODED_STREAM: usize = 480_000;
/// How long a terminated `mule exec` gets to run its in-process `rm`. mu's own
/// deadline is `timeoutMs` plus 5 s, so the reply must land inside that.
const STOP_GRACE: Duration = Duration::from_secs(3);

type Out = Arc<Mutex<std::io::Stdout>>;

/// A call's cancel state. Registered by the reader BEFORE its worker starts,
/// so a `$/cancel` right behind the request always finds it; `pid` is 0 until
/// the child exists.
#[derive(Default)]
struct Live {
    pid: AtomicI32,
    cancelled: AtomicBool,
}

type Calls = Arc<Mutex<HashMap<String, Arc<Live>>>>;

/// Everything one call needs; cloned into its thread.
#[derive(Clone)]
struct Ctx {
    out: Out,
    calls: Calls,
    mule: Arc<Vec<String>>,
}

fn send(out: &Out, msg: &Value) {
    let mut o = out.lock().unwrap();
    let _ = writeln!(o, "{msg}");
    let _ = o.flush();
}

fn reply(out: &Out, id: &Value, result: Value) {
    send(out, &json!({"jsonrpc": "2.0", "id": id, "result": result}));
}

fn fail(out: &Out, id: &Value, kind: &str, message: &str, remedy: Option<&str>) {
    let mut data = json!({ "kind": kind });
    if let Some(r) = remedy {
        data["remedy"] = json!(r);
    }
    send(
        out,
        &json!({"jsonrpc": "2.0", "id": id, "error": {"code": -32000, "message": message, "data": data}}),
    );
}

/// The `mule` to run: the one beside this binary (a `cargo install` or a build
/// puts both in one directory), else the first on PATH.
fn mule_bin() -> std::path::PathBuf {
    std::env::current_exe()
        .ok()
        .and_then(|p| p.parent().map(|d| d.join("mule")))
        .filter(|p| p.is_file())
        .unwrap_or_else(|| "mule".into())
}

fn first_line(s: &str) -> String {
    s.lines()
        .find(|l| !l.trim().is_empty())
        .unwrap_or("")
        .chars()
        .take(300)
        .collect()
}

/// Keep at most `MAX_OUTPUT` bytes of a stream and drain the rest, so a chatty
/// child never blocks on a full pipe. Returns the kept bytes and whether any
/// were dropped.
fn drain(mut r: impl Read + Send + 'static) -> thread::JoinHandle<(Vec<u8>, bool)> {
    thread::spawn(move || {
        let (mut kept, mut cut) = (Vec::new(), false);
        let mut buf = [0u8; 8192];
        while let Ok(n) = r.read(&mut buf) {
            if n == 0 {
                break;
            }
            let room = MAX_OUTPUT - kept.len();
            if n > room {
                cut = true;
            }
            kept.extend_from_slice(&buf[..n.min(room)]);
        }
        (kept, cut)
    })
}

struct Ran {
    code: Option<i32>,
    stdout: String,
    stderr: String,
    partial: bool,
    timed_out: bool,
    cancelled: bool,
}

fn signal(pid: i32, sig: i32) {
    // SAFETY: plain kill(2); a stale pid at worst fails with ESRCH.
    unsafe { libc::kill(pid, sig) };
}

/// `ps` rows as (pid, ppid). `ps` rather than /proc: macOS has none.
fn process_table() -> Vec<(i32, i32)> {
    let Ok(out) = Command::new("ps")
        .args(["-A", "-o", "pid=,ppid="])
        .stdin(Stdio::null())
        .stderr(Stdio::null())
        .output()
    else {
        return Vec::new();
    };
    String::from_utf8_lossy(&out.stdout)
        .lines()
        .filter_map(|l| {
            let mut f = l.split_whitespace().map(|n| n.parse::<i32>().ok());
            Some((f.next()??, f.next()??))
        })
        .collect()
}

/// Every live descendant of `root`. mule's own signal handling only reaches
/// what it knows; the `ssh` it is blocked in is a grandchild we must stop too,
/// from here, because it is a helper of ours that nothing else will reap when
/// mu dies and leaves us only an EOF.
fn descendants(root: i32) -> Vec<i32> {
    let table = process_table();
    let mut found = vec![root];
    let mut i = 0;
    while i < found.len() {
        let parent = found[i];
        found.extend(table.iter().filter(|r| r.1 == parent).map(|r| r.0));
        i += 1;
    }
    found.remove(0);
    found
}

fn orphaned(pid: i32) -> bool {
    process_table().iter().any(|r| r.0 == pid && r.1 == 1)
}

static PARTIALS: AtomicU64 = AtomicU64::new(0);

/// Run `mule <args>` to completion, to `timeout`, or to a cancel (`live`).
/// Starts nothing if the cancel already arrived. `Ok(None)` is that case.
fn run_mule(
    ctx: &Ctx,
    live: &Live,
    args: &[String],
    timeout: Duration,
) -> std::io::Result<Option<Ran>> {
    if live.cancelled.load(Ordering::SeqCst) {
        return Ok(None);
    }
    // mule's own word that output was cut (exec.rs): the exit code 7 and the
    // stderr note are ambiguous with the command's.
    let partial_file = std::env::temp_dir().join(format!(
        "mu-plugin-mule-{}-{}.partial",
        std::process::id(),
        PARTIALS.fetch_add(1, Ordering::SeqCst)
    ));
    let mut child: Child = Command::new(&*ctx.mule[0])
        .args(&ctx.mule[1..])
        .args(args)
        .env("MULE_PARTIAL_FILE", &partial_file)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    let pid = child.id() as i32;
    // Publish, then re-read the flag: a cancel that set it just before we
    // published saw pid 0 and signalled nothing, so this check is its signal.
    live.pid.store(pid, Ordering::SeqCst);
    let so = drain(child.stdout.take().unwrap());
    let se = drain(child.stderr.take().unwrap());
    let start = Instant::now();
    let mut stopping: Option<Instant> = None;
    let mut timed_out = false;
    let mut tracked: Vec<i32> = Vec::new();
    let mut last_scan = Instant::now();
    let status = loop {
        if let Some(s) = child.try_wait()? {
            break s;
        }
        if stopping.is_none() {
            let cancelled = live.cancelled.load(Ordering::SeqCst);
            if cancelled || start.elapsed() >= timeout {
                timed_out = !cancelled;
                stopping = Some(Instant::now());
                // TERM mule and the helper it is blocked in, as a group
                // signal would; the replacement `rm` ssh it starts next is
                // left alone so the stop can finish.
                let kids = descendants(pid);
                signal(pid, libc::SIGTERM);
                for k in &kids {
                    signal(*k, libc::SIGTERM);
                }
                tracked.extend(kids);
            }
        } else if let Some(t) = stopping {
            if t.elapsed() >= STOP_GRACE {
                signal(pid, libc::SIGKILL);
                for k in descendants(pid) {
                    signal(k, libc::SIGKILL);
                }
            } else if last_scan.elapsed() >= Duration::from_millis(100) {
                last_scan = Instant::now();
                tracked.extend(descendants(pid));
            }
        }
        thread::sleep(Duration::from_millis(10));
    };
    // mule is gone; whatever it left behind is an orphan of ours.
    for k in tracked {
        if orphaned(k) {
            signal(k, libc::SIGKILL);
        }
    }
    let (so, cut_o) = so.join().unwrap_or_default();
    let (se, cut_e) = se.join().unwrap_or_default();
    let upstream = partial_file.exists();
    let _ = std::fs::remove_file(&partial_file);
    Ok(Some(Ran {
        code: status.code().filter(|_| status.signal().is_none()),
        stdout: String::from_utf8_lossy(&so).into_owned(),
        stderr: String::from_utf8_lossy(&se).into_owned(),
        partial: cut_o || cut_e || upstream,
        timed_out,
        cancelled: live.cancelled.load(Ordering::SeqCst),
    }))
}

/// Keep a prefix of `s` whose JSON encoding is at most `max` bytes (quotes
/// excluded), cut on a char boundary. Returns whether anything was dropped.
fn clip_encoded(s: String, max: usize) -> (String, bool) {
    let mut used = 0;
    for (i, c) in s.char_indices() {
        let w = match c {
            '"' | '\\' | '\n' | '\r' | '\t' | '\u{8}' | '\u{c}' => 2,
            c if (c as u32) < 0x20 => 6,
            c => c.len_utf8(),
        };
        if used + w > max {
            return (s[..i].to_string(), true);
        }
        used += w;
    }
    (s, false)
}

/// A `mule exec --strict-codes` result as a protocol answer: 3 is mule's
/// hand-back (a person must open the master, so `human`, never a command
/// result). Anything else is the command's, 8 included: it is the command's
/// own 3 or its own 8, and mule documents that the stderr note cannot tell
/// them apart, so the plugin does not guess.
fn exec_answer(host: &str, r: Ran) -> Result<Value, (&'static str, String, String)> {
    let (stderr, code) = (r.stderr, r.code);
    if code == Some(3) && !r.timed_out {
        let remedy = stderr
            .lines()
            .find_map(|l| l.trim().strip_prefix("run: ").map(str::to_string))
            .unwrap_or_else(|| format!("open mule's ssh control master for {host}"));
        return Err(("human", format!("no ssh control master for {host}"), remedy));
    }
    if code.is_none() && !r.timed_out {
        // Killed by a signal we did not send for a timeout: whether the
        // script ran is unknown.
        return Err((
            "unknown-outcome",
            format!("mule exec for {host} was killed"),
            String::new(),
        ));
    }
    let (stdout, cut_o) = clip_encoded(r.stdout, ENCODED_STREAM);
    let (stderr, cut_e) = clip_encoded(stderr, ENCODED_STREAM);
    Ok(json!({
        "exitCode": if r.timed_out { None } else { code },
        "stdout": stdout,
        "stderr": stderr,
        "partial": r.partial || cut_o || cut_e,
    }))
}

fn host_of(params: &Value) -> Option<String> {
    params["target"]["host"]["name"]
        .as_str()
        .map(str::to_string)
}

/// The time the wire deadline leaves (`deadline` is epoch ms; mu cancels past
/// it). `None` field: unbounded. `Err` when it has already passed: nothing may
/// start, a call delayed in delivery must not begin side effects after the
/// caller gave up.
fn remaining(params: &Value) -> Result<Option<Duration>, ()> {
    let Some(deadline) = params["deadline"].as_u64() else {
        return Ok(None);
    };
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64);
    match deadline.checked_sub(now) {
        Some(ms) if ms > 0 => Ok(Some(Duration::from_millis(ms))),
        _ => Err(()),
    }
}

fn expired(out: &Out, id: &Value) {
    fail(
        out,
        id,
        "permanent",
        "the call's deadline passed before it started; nothing ran",
        None,
    );
}

fn link_exec(ctx: &Ctx, live: &Live, id: &Value, params: &Value) {
    let (Some(host), Some(script)) = (host_of(params), params["script"].as_str()) else {
        return fail(
            &ctx.out,
            id,
            "permanent",
            "link.exec needs a host and a script",
            None,
        );
    };
    let Ok(left) = remaining(params) else {
        return expired(&ctx.out, id);
    };
    let cwd = params["cwd"].as_str().unwrap_or(".");
    let mut timeout = Duration::from_millis(params["timeoutMs"].as_u64().unwrap_or(20_000));
    if let Some(left) = left {
        timeout = timeout.min(left);
    }
    let args: Vec<String> = [
        "exec",
        "--host",
        &host,
        "--cwd",
        cwd,
        "--no-detach",
        "--strict-codes",
        "--",
        script,
    ]
    .map(String::from)
    .into();
    match run_mule(ctx, live, &args, timeout) {
        Err(e) => fail(
            &ctx.out,
            id,
            "permanent",
            &format!("cannot run mule: {e}"),
            None,
        ),
        Ok(None) => {}                   // cancelled before dispatch: nothing ran
        Ok(Some(r)) if r.cancelled => {} // mu ignores any answer now
        Ok(Some(r)) => match exec_answer(&host, r) {
            Ok(v) => reply(&ctx.out, id, v),
            Err((kind, msg, remedy)) => fail(
                &ctx.out,
                id,
                kind,
                &msg,
                Some(remedy.as_str()).filter(|r| !r.is_empty()),
            ),
        },
    }
}

/// `host info --json` exits 0 with `master: false` when the master is down:
/// that is a hand-back step, not a failure of the probe.
fn check_steps(ctx: &Ctx, live: &Live, id: &Value, params: &Value) {
    let Some(host) = host_of(params) else {
        return fail(
            &ctx.out,
            id,
            "permanent",
            "link.checkSteps needs a host",
            None,
        );
    };
    if params["phase"] != "reach" {
        return reply(&ctx.out, id, json!([]));
    }
    let Ok(left) = remaining(params) else {
        return expired(&ctx.out, id);
    };
    let budget = left.map_or(Duration::from_secs(8), |l| l.min(Duration::from_secs(8)));
    let args = ["host", "info", "--host", &host, "--json"].map(String::from);
    let t0 = Instant::now();
    let steps = match run_mule(ctx, live, &args, budget) {
        Err(e) => {
            vec![json!({"step": "master", "ok": false, "detail": format!("cannot run mule: {e}")})]
        }
        Ok(None) => return,
        Ok(Some(r)) if r.cancelled => return,
        Ok(Some(r)) if r.code != Some(0) => {
            vec![json!({"step": "master", "ok": false, "detail": first_line(&r.stderr)})]
        }
        Ok(Some(r)) => {
            let v: Value = serde_json::from_str(&r.stdout).unwrap_or(Value::Null);
            let row = &v["items"][0];
            match row["master"].as_bool() {
                Some(true) => {
                    let mut steps = vec![json!({"step": "master", "ok": true,
                        "detail": format!("mule {host}: master up, {} ms", t0.elapsed().as_millis())})];
                    // The probe is one locked session and dispatches no job:
                    // it ran `tmux -V` on the remote. A host that cannot run
                    // tmux fails every job-backed probe after this one.
                    match row["tmux"].as_bool() {
                        Some(true) => steps.push(
                            json!({"step": "tmux", "ok": true, "detail": "tmux runs on the remote"}),
                        ),
                        Some(false) => steps.push(json!({"step": "tmux", "ok": false,
                            "detail": format!("tmux is not runnable on {host}'s remote shell (not on its non-login PATH); mule jobs need it")})),
                        None => steps.push(json!({"step": "tmux", "ok": false,
                            "detail": "mule host info did not report tmux"})),
                    }
                    steps
                }
                Some(false) => vec![json!({"step": "handback", "ok": false,
                    "detail": row["remedy"].as_str().filter(|r| !r.is_empty())
                        .unwrap_or("open mule's ssh control master")})],
                None => vec![json!({"step": "master", "ok": false,
                    "detail": "mule host info gave no master field"})],
            }
        }
    };
    reply(&ctx.out, id, Value::Array(steps));
}

fn initialize(ctx: &Ctx, id: &Value, params: &Value) {
    let min = params["protocol"]["min"].as_u64().unwrap_or(0);
    let max = params["protocol"]["max"].as_u64().unwrap_or(0);
    if min > 1 || max < 1 {
        return fail(
            &ctx.out,
            id,
            "permanent",
            "mu-plugin-mule speaks protocol 1 only",
            None,
        );
    }
    reply(&ctx.out, id, handshake(&instance(&ctx.mule)));
}

/// FNV-1a: stable across Rust versions, unlike `DefaultHasher`, and the
/// instance must compare equal across plugin restarts.
fn fnv(parts: &[&[u8]]) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for p in parts {
        for b in p.iter().chain(&[0u8]) {
            h = (h ^ u64::from(*b)).wrapping_mul(0x100000001b3);
        }
    }
    h
}

/// The backend's identity, opaque: a hash over what decides where a job goes
/// -- the `--config` file's bytes (or its absence), and every host's name,
/// target and master socket as `mule host list --json` resolves them (that
/// verb is lock-exempt and never opens a session). Retarget a host and the
/// instance changes, so mu's recovery and collect fence refuses the old
/// jobs; a hash discloses no path or secret.
fn instance(mule: &[String]) -> String {
    let config = mule
        .iter()
        .position(|a| a == "--config")
        .and_then(|i| mule.get(i + 1))
        .map(|p| std::fs::read(p).unwrap_or_else(|e| format!("unreadable:{}", e.kind()).into()))
        .unwrap_or_default();
    let hosts = Command::new(&mule[0])
        .args(&mule[1..])
        .args(["host", "list", "--json"])
        .stdin(Stdio::null())
        .stderr(Stdio::null())
        .output()
        .ok()
        .and_then(|o| serde_json::from_slice::<Value>(&o.stdout).ok())
        .map(|v| {
            v["items"]
                .as_array()
                .into_iter()
                .flatten()
                .map(|h| format!("{}\t{}\t{}\n", h["name"], h["target"], h["socket"]))
                .collect::<String>()
        })
        .unwrap_or_default();
    format!("mule-{:016x}", fnv(&[&config, hosts.as_bytes()]))
}

fn handshake(instance: &str) -> Value {
    json!({
        "protocol": 1,
        "plugin": {"name": "mule", "version": env!("CARGO_PKG_VERSION")},
        "instance": instance,
        "schema": SCHEMA,
        "seams": {"link": {
            "fetch": false,
            "slots": "self",
            // A cancelled exec runs `rm` in-process, which can outlast the grace.
            "remoteOnTimeout": "unknown",
            "quiescence": "provable",
            "maxScriptBytes": 128 * 1024,
            "checks": ["reach"],
        }},
    })
}

fn on_message(ctx: &Ctx, msg: Value) {
    let id = msg["id"].clone();
    let params = msg["params"].clone();
    match msg["method"].as_str().unwrap_or("") {
        "initialize" => initialize(ctx, &id, &params),
        "$/cancel" => {
            // Only flags: the call's own worker stops its tree (run_mule).
            if let Some(live) = ctx.calls.lock().unwrap().get(&params["id"].to_string()) {
                live.cancelled.store(true, Ordering::SeqCst);
            }
        }
        "shutdown" => reply(&ctx.out, &id, Value::Null),
        m @ ("link.exec" | "link.checkSteps") => {
            let ctx = ctx.clone();
            let m = m.to_string();
            // Registered here, on the reader, before the worker exists: a
            // `$/cancel` on the next line must find it.
            let key = id.to_string();
            let live = Arc::new(Live::default());
            ctx.calls.lock().unwrap().insert(key.clone(), live.clone());
            thread::spawn(move || {
                if m == "link.exec" {
                    link_exec(&ctx, &live, &id, &params)
                } else {
                    check_steps(&ctx, &live, &id, &params)
                }
                ctx.calls.lock().unwrap().remove(&key);
            });
        }
        m => fail(
            &ctx.out,
            &id,
            "permanent",
            &format!("mu-plugin-mule has no method {m}"),
            None,
        ),
    }
}

fn main() {
    // `--config PATH` is the only option, and goes to every mule call.
    let mut mule = vec![mule_bin().to_string_lossy().into_owned(), "--quiet".into()];
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        match (a.as_str(), args.next()) {
            ("--config", Some(p)) => mule.extend(["--config".to_string(), p]),
            _ => {
                eprintln!("usage: mu-plugin-mule [--config PATH]");
                std::process::exit(2);
            }
        }
    }
    let ctx = Ctx {
        out: Arc::new(Mutex::new(std::io::stdout())),
        calls: Arc::default(),
        mule: Arc::new(mule),
    };
    for line in std::io::stdin().lock().lines() {
        let Ok(line) = line else { break };
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str::<Value>(&line) {
            Ok(msg) => on_message(&ctx, msg),
            Err(e) => eprintln!("mu-plugin-mule: bad message: {e}"),
        }
    }
    // stdin EOF: mu is done or dead. Flag every call cancelled: each worker
    // TERMs its mule tree, KILLs after the grace and reaps what mule left
    // behind (run_mule). Then wait for them, and KILL any tree still up.
    let in_flight = {
        let calls = ctx.calls.lock().unwrap();
        for live in calls.values() {
            live.cancelled.store(true, Ordering::SeqCst);
        }
        !calls.is_empty()
    };
    let t = Instant::now();
    while !ctx.calls.lock().unwrap().is_empty() && t.elapsed() < STOP_GRACE + Duration::from_secs(2)
    {
        thread::sleep(Duration::from_millis(20));
    }
    if !in_flight {
        return; // every mule was waited for; nothing of ours can be left
    }
    // Hard bound, independent of `ps`: a call was in flight, so a helper may
    // have outlived its mule (reparented, so no tree walk finds it -- and a
    // failing `ps` finds nothing at all). Everything we spawned shares our
    // process group, never detached, so when we lead it (mu starts a plugin
    // as its own group leader) one SIGKILL to the group takes every one,
    // ourselves last. Measured: with `ps` failing, the stalled ssh survived
    // the plugin 3 of 3 runs before this.
    // SAFETY: plain getpgrp/getpid/kill; no memory is touched.
    unsafe {
        if libc::getpgrp() == libc::getpid() {
            libc::kill(-libc::getpid(), libc::SIGKILL);
        }
    }
    // Not a leader (a harness that shares its group with us): the best left is
    // the tree walk, which only works when `ps` does.
    for live in ctx.calls.lock().unwrap().values() {
        let pid = live.pid.load(Ordering::SeqCst);
        if pid > 0 {
            for k in descendants(pid) {
                signal(k, libc::SIGKILL);
            }
            signal(pid, libc::SIGKILL);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ran(code: Option<i32>, stderr: &str) -> Ran {
        Ran {
            code,
            stdout: "o".into(),
            stderr: stderr.into(),
            partial: false,
            timed_out: false,
            cancelled: false,
        }
    }

    #[test]
    fn strict_3_is_a_handback_with_the_remedy() {
        let e = exec_answer(
            "h",
            ran(Some(3), "mule: no control master\n  run: ssh -MNf x\n"),
        )
        .unwrap_err();
        assert_eq!(e.0, "human");
        assert_eq!(e.2, "ssh -MNf x");
    }

    #[test]
    fn every_other_code_is_the_commands() {
        for c in [0, 1, 7, 8, 124, 143] {
            let v = exec_answer("h", ran(Some(c), "")).unwrap();
            assert_eq!(v["exitCode"], c);
        }
    }

    #[test]
    fn a_timeout_has_no_exit_code_and_a_stray_signal_is_unknown() {
        let mut t = ran(None, "");
        t.timed_out = true;
        assert!(exec_answer("h", t).unwrap()["exitCode"].is_null());
        assert_eq!(
            exec_answer("h", ran(None, "")).unwrap_err().0,
            "unknown-outcome"
        );
    }

    #[test]
    fn the_handshake_declares_the_link_only() {
        let h = handshake("mule-x");
        assert_eq!(h["seams"].as_object().unwrap().len(), 1);
        assert_eq!(h["seams"]["link"]["fetch"], false);
        assert_eq!(h["seams"]["link"]["slots"], "self");
    }
}
