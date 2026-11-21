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
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use serde_json::{Value, json};

/// sha256 of `schemas/plugin-protocol-v1.json` in mu: mu refuses any other.
const SCHEMA: &str = "sha256:6ea6e13cabf6baebe44e961e21370a66ddf523082bfd73bcf169ec627fc71a25";
/// Per stream. Lossy UTF-8 can triple a byte, and a protocol line is capped at
/// 1 MiB with both streams in it: 2 x 128 KiB x 3 stays under.
const MAX_OUTPUT: usize = 128 * 1024;
/// How long a terminated `mule exec` gets to run its in-process `rm`. mu's own
/// deadline is `timeoutMs` plus 5 s, so the reply must land inside that.
const STOP_GRACE: Duration = Duration::from_secs(3);

type Out = Arc<Mutex<std::io::Stdout>>;

struct Live {
    pid: i32,
    cancelled: Arc<AtomicBool>,
}

type Calls = Arc<Mutex<HashMap<String, Live>>>;

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
    // SAFETY: plain kill(2) on a child pid this process spawned and has not
    // yet reaped (we are the one waiting on it).
    unsafe { libc::kill(pid, sig) };
}

/// Run `mule <args>` to completion, or to `timeout`, registering it under `key`
/// so `$/cancel` and EOF can stop it.
fn run_mule(ctx: &Ctx, key: &str, args: &[String], timeout: Duration) -> std::io::Result<Ran> {
    let mut child: Child = Command::new(&*ctx.mule[0])
        .args(&ctx.mule[1..])
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    let pid = child.id() as i32;
    let cancelled = Arc::new(AtomicBool::new(false));
    ctx.calls.lock().unwrap().insert(
        key.to_string(),
        Live {
            pid,
            cancelled: cancelled.clone(),
        },
    );
    let so = drain(child.stdout.take().unwrap());
    let se = drain(child.stderr.take().unwrap());
    let start = Instant::now();
    let mut stopping: Option<Instant> = None;
    let mut timed_out = false;
    let status = loop {
        if let Some(s) = child.try_wait()? {
            break s;
        }
        if cancelled.load(Ordering::SeqCst) && stopping.is_none() {
            stopping = Some(Instant::now());
        }
        if stopping.is_none() && start.elapsed() >= timeout {
            timed_out = true;
            signal(pid, libc::SIGTERM);
            stopping = Some(Instant::now());
        }
        if stopping.is_some_and(|t| t.elapsed() >= STOP_GRACE) {
            signal(pid, libc::SIGKILL);
        }
        thread::sleep(Duration::from_millis(10));
    };
    ctx.calls.lock().unwrap().remove(key);
    let (so, cut_o) = so.join().unwrap_or_default();
    let (se, cut_e) = se.join().unwrap_or_default();
    Ok(Ran {
        code: status.code().filter(|_| status.signal().is_none()),
        stdout: String::from_utf8_lossy(&so).into_owned(),
        stderr: String::from_utf8_lossy(&se).into_owned(),
        partial: cut_o || cut_e,
        timed_out,
        cancelled: cancelled.load(Ordering::SeqCst),
    })
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
    Ok(json!({
        "exitCode": if r.timed_out { None } else { code },
        "stdout": r.stdout,
        "stderr": stderr,
        "partial": r.partial,
    }))
}

fn host_of(params: &Value) -> Option<String> {
    params["target"]["host"]["name"]
        .as_str()
        .map(str::to_string)
}

fn link_exec(ctx: &Ctx, id: &Value, params: &Value) {
    let (Some(host), Some(script)) = (host_of(params), params["script"].as_str()) else {
        return fail(
            &ctx.out,
            id,
            "permanent",
            "link.exec needs a host and a script",
            None,
        );
    };
    let cwd = params["cwd"].as_str().unwrap_or(".");
    let timeout = Duration::from_millis(params["timeoutMs"].as_u64().unwrap_or(20_000));
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
    match run_mule(ctx, &id.to_string(), &args, timeout) {
        Err(e) => fail(
            &ctx.out,
            id,
            "permanent",
            &format!("cannot run mule: {e}"),
            None,
        ),
        Ok(r) if r.cancelled => {} // mu ignores any answer now
        Ok(r) => match exec_answer(&host, r) {
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
fn check_steps(ctx: &Ctx, id: &Value, params: &Value) {
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
    let args = ["host", "info", "--host", &host, "--json"].map(String::from);
    let t0 = Instant::now();
    let step = match run_mule(ctx, &id.to_string(), &args, Duration::from_secs(8)) {
        Err(e) => json!({"step": "master", "ok": false, "detail": format!("cannot run mule: {e}")}),
        Ok(r) if r.cancelled => return,
        Ok(r) if r.code != Some(0) => {
            json!({"step": "master", "ok": false, "detail": first_line(&r.stderr)})
        }
        Ok(r) => {
            let v: Value = serde_json::from_str(&r.stdout).unwrap_or(Value::Null);
            let row = &v["items"][0];
            match row["master"].as_bool() {
                Some(true) => json!({"step": "master", "ok": true,
                    "detail": format!("mule {host}: master up, {} ms", t0.elapsed().as_millis())}),
                Some(false) => json!({"step": "handback", "ok": false,
                    "detail": row["remedy"].as_str().filter(|r| !r.is_empty())
                        .unwrap_or("open mule's ssh control master")}),
                None => json!({"step": "master", "ok": false,
                    "detail": "mule host info gave no master field"}),
            }
        }
    };
    reply(&ctx.out, id, json!([step]));
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
    reply(&ctx.out, id, handshake());
}

fn handshake() -> Value {
    json!({
        "protocol": 1,
        "plugin": {"name": "mule", "version": env!("CARGO_PKG_VERSION")},
        "instance": "mule",
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
            let key = params["id"].to_string();
            if let Some(live) = ctx.calls.lock().unwrap().get(&key) {
                live.cancelled.store(true, Ordering::SeqCst);
                signal(live.pid, libc::SIGTERM);
            }
        }
        "shutdown" => reply(&ctx.out, &id, Value::Null),
        m @ ("link.exec" | "link.checkSteps") => {
            let ctx = ctx.clone();
            let m = m.to_string();
            thread::spawn(move || {
                if m == "link.exec" {
                    link_exec(&ctx, &id, &params)
                } else {
                    check_steps(&ctx, &id, &params)
                }
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
    // stdin EOF: mu is done or dead. Stop what runs: TERM lets `exec
    // --no-detach` run its rm, KILL after the grace, then leave.
    for live in ctx.calls.lock().unwrap().values() {
        signal(live.pid, libc::SIGTERM);
    }
    let t = Instant::now();
    while !ctx.calls.lock().unwrap().is_empty() && t.elapsed() < STOP_GRACE + Duration::from_secs(1)
    {
        thread::sleep(Duration::from_millis(20));
    }
    for live in ctx.calls.lock().unwrap().values() {
        signal(live.pid, libc::SIGKILL);
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
        let h = handshake();
        assert_eq!(h["seams"].as_object().unwrap().len(), 1);
        assert_eq!(h["seams"]["link"]["fetch"], false);
        assert_eq!(h["seams"]["link"]["slots"], "self");
    }
}
