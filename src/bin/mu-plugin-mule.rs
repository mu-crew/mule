//! mu-plugin-mule: serves mu's link and bound lease job seams (protocol v1), over
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
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use serde_json::{Value, json};

mod mule_lease;

/// sha256 of mu's `schemas/plugin-protocol-v1.json` with the D15 file-handle wire.
const SCHEMA: &str = "sha256:54d56d9a865c4e9302f2b63158cb595010ecbb0b59037550609a54d79392ac36";
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
    /// The companion binary takes precedence over PATH.
    mule: Option<PathBuf>,
    /// The `--config PATH` this plugin was started with, read once, at the
    /// handshake.
    config_path: Option<std::path::PathBuf>,
    /// The config as the handshake saw it; `None` before it. Every dispatch
    /// uses this and nothing else (see `Snap`).
    snap: Arc<Mutex<Option<Arc<Snap>>>>,
    /// Set once any `mule` child has been spawned: the process group may then
    /// hold helpers that outlive their mule (see `main`).
    spawned: Arc<AtomicBool>,
}

impl Ctx {
    fn mule_path(&self) -> std::io::Result<&Path> {
        self.mule
            .as_deref()
            .map_or_else(|| mule::helper::MULE.path(), Ok)
    }
}

/// The mule config, read and validated ONCE at the handshake. The instance mu
/// fingerprints is derived from these bytes, and every `mule` child is handed
/// a private copy of them (`--config <copy>`) instead of the mutable pathname:
/// an atomic replace of the real file between the check and the use can then
/// never retarget a dispatch (reviewed: exec ran against `two/two.sock` while
/// mu held `one`'s fingerprint). A changed file is seen only by a new plugin
/// process, i.e. when mu re-handshakes.
struct Snap {
    instance: String,
    hosts: Option<mule::config::Config>,
    /// The private copy; `None` when the file was unreadable at the handshake,
    /// in which case nothing dispatches.
    config: Option<std::path::PathBuf>,
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

/// A `cargo install` or a build puts both binaries in one directory.
fn bundled_mule() -> Option<PathBuf> {
    std::env::current_exe()
        .ok()
        .and_then(|p| p.parent().map(|d| d.join("mule")))
        .filter(|p| p.is_file())
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
    let Ok(out) = mule::helper::PS.command().and_then(|mut cmd| {
        cmd.args(["-A", "-o", "pid=,ppid="])
            .stdin(Stdio::null())
            .stderr(Stdio::null())
            .output()
    }) else {
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

enum Run {
    Done(Ran),
    /// Cancelled before dispatch: nothing ran.
    Cancelled,
    /// The wire deadline had passed at the moment of dispatch: nothing ran.
    Expired,
}

/// Run `mule <args>` to completion, to `timeout` (capped by the absolute wire
/// `deadline`), or to a cancel (`live`). The deadline is checked HERE,
/// immediately before the spawn and after every other preparation: a budget
/// taken earlier and measured from the spawn would hand a late call a fresh
/// copy of the old budget. Starts nothing if the cancel or the deadline
/// already arrived.
fn run_mule(
    ctx: &Ctx,
    snap: &Snap,
    live: &Live,
    args: &[String],
    mut timeout: Duration,
    deadline: Option<u64>,
) -> std::io::Result<Run> {
    let Some(config) = &snap.config else {
        return Err(std::io::Error::other("no config snapshot"));
    };
    if live.cancelled.load(Ordering::SeqCst) {
        return Ok(Run::Cancelled);
    }
    // mule's own word that output was cut (exec.rs): the exit code 7 and the
    // stderr note are ambiguous with the command's.
    let partial_file = std::env::temp_dir().join(format!(
        "mu-plugin-mule-{}-{}.partial",
        std::process::id(),
        PARTIALS.fetch_add(1, Ordering::SeqCst)
    ));
    let mule = ctx.mule_path();
    match remaining_at(deadline, now_ms()) {
        Err(()) => return Ok(Run::Expired),
        Ok(Some(left)) => timeout = timeout.min(left),
        Ok(None) => {}
    }
    ctx.spawned.store(true, Ordering::SeqCst);
    let mut child: Child = Command::new(mule?)
        .arg("--quiet")
        .arg("--config")
        .arg(config)
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
    Ok(Run::Done(Ran {
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

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
}

/// The time the wire deadline leaves (`deadline` is epoch ms; mu cancels past
/// it). `None`: unbounded. `Err` when it has already passed: nothing may
/// start, a call delayed in delivery must not begin side effects after the
/// caller gave up.
fn remaining_at(deadline: Option<u64>, now: u64) -> Result<Option<Duration>, ()> {
    let Some(deadline) = deadline else {
        return Ok(None);
    };
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
    let Some(snap) = snapshot(ctx, id) else {
        return;
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
    match run_mule(
        ctx,
        &snap,
        live,
        &args,
        timeout,
        params["deadline"].as_u64(),
    ) {
        Err(e) => fail(
            &ctx.out,
            id,
            "permanent",
            &format!("cannot run mule: {e}"),
            None,
        ),
        Ok(Run::Cancelled) => {} // cancelled before dispatch: nothing ran
        Ok(Run::Expired) => expired(&ctx.out, id),
        Ok(Run::Done(r)) if r.cancelled => {} // mu ignores any answer now
        Ok(Run::Done(r)) => match exec_answer(&host, r) {
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
    let Some(snap) = snapshot(ctx, id) else {
        return;
    };
    let args = ["host", "info", "--host", &host, "--json"].map(String::from);
    let t0 = Instant::now();
    let run = run_mule(
        ctx,
        &snap,
        live,
        &args,
        Duration::from_secs(8),
        params["deadline"].as_u64(),
    );
    let steps = match run {
        Err(e) => {
            vec![json!({"step": "master", "ok": false, "detail": format!("cannot run mule: {e}")})]
        }
        Ok(Run::Cancelled) => return,
        Ok(Run::Expired) => return expired(&ctx.out, id),
        Ok(Run::Done(r)) if r.cancelled => return,
        Ok(Run::Done(r)) if r.code != Some(0) => {
            vec![json!({"step": "master", "ok": false, "detail": first_line(&r.stderr)})]
        }
        Ok(Run::Done(r)) => {
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
    let snap = {
        let mut g = ctx.snap.lock().unwrap();
        g.get_or_insert_with(|| Arc::new(take_snapshot(ctx)))
            .clone()
    };
    reply(&ctx.out, id, handshake(&snap.instance));
}

/// The one read of the config file, at the handshake.
fn take_snapshot(ctx: &Ctx) -> Snap {
    let path = ctx
        .config_path
        .clone()
        .or_else(|| mule::config::default_path().ok());
    let read = path.map(std::fs::read);
    let (bytes, ok) = match read {
        Some(Ok(b)) => (b, true),
        Some(Err(e)) => (format!("unreadable:{}", e.kind()).into(), false),
        None => (b"unreadable:nopath".to_vec(), false),
    };
    let config = ok.then(|| write_copy(&bytes)).flatten();
    let hosts = std::str::from_utf8(&bytes)
        .ok()
        .and_then(|text| mule::config::Config::parse(text).ok());
    Snap {
        instance: instance(&bytes, hosts.as_ref()),
        hosts,
        config,
    }
}

fn copy_path() -> std::path::PathBuf {
    std::env::temp_dir().join(format!("mu-plugin-mule-{}.config.toml", std::process::id()))
}

/// A private (0600) copy of the snapshot bytes for the `mule` children.
fn write_copy(bytes: &[u8]) -> Option<std::path::PathBuf> {
    use std::os::unix::fs::OpenOptionsExt;
    let p = copy_path();
    let _ = std::fs::remove_file(&p); // a stale file from a recycled pid
    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&p)
        .ok()?;
    f.write_all(bytes).ok()?;
    Some(p)
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
/// -- the config bytes (or why they were unreadable) and every host's name,
/// target and master socket as mule resolves them (`~` expanded). Computed
/// in-process from the snapshot: a `mule host list` child probes every master
/// in series (2 s each, measured 15.3 s with six wedged peers, past mu's 10 s
/// initialize cap), and the identity needs no master state. Retarget a host
/// and the instance changes, so mu's recovery and collect fence refuses the
/// old jobs; a hash discloses no path or secret.
fn instance(bytes: &[u8], config: Option<&mule::config::Config>) -> String {
    let hosts = config
        .map(|c| {
            c.hosts()
                .iter()
                .map(|h| {
                    let socket =
                        std::path::absolute(&h.socket).unwrap_or_else(|_| h.socket.clone());
                    format!("{}\t{}\t{}\n", h.name, h.target, socket.display())
                })
                .collect::<String>()
        })
        .unwrap_or_default();
    format!("mule-{:016x}", fnv(&[bytes, hosts.as_bytes()]))
}

/// The snapshot a dispatch must use; replies with a refusal (nothing ran) and
/// returns `None` when there is none to use.
fn snapshot(ctx: &Ctx, id: &Value) -> Option<Arc<Snap>> {
    let snap = ctx.snap.lock().unwrap().clone();
    match snap {
        Some(s) if s.config.is_some() => Some(s),
        Some(_) => {
            fail(
                &ctx.out,
                id,
                "permanent",
                "mule's configuration was unreadable at this session's handshake; nothing ran",
                None,
            );
            None
        }
        None => {
            fail(
                &ctx.out,
                id,
                "permanent",
                "plugin call before initialize; nothing ran",
                None,
            );
            None
        }
    }
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
        }, "job": {
            "kind": "lease",
            "view": "bound",
            "handle": "file",
            "start": {"lookup": true, "fenceKey": false},
            // No plugin channel: mu drives pi on its built-in ctl socket.
            "honours": {"command": true, "env": true, "sockets": true,
                         "roles": ["full-access", "read-only"]},
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
        m @ ("link.exec" | "link.checkSteps" | "job.start" | "job.attach" | "job.lookup"
        | "job.status" | "job.stop") => {
            let ctx = ctx.clone();
            let m = m.to_string();
            // Registered here, on the reader, before the worker exists: a
            // `$/cancel` on the next line must find it.
            let key = id.to_string();
            let live = Arc::new(Live::default());
            ctx.calls.lock().unwrap().insert(key.clone(), live.clone());
            thread::spawn(move || {
                match m.as_str() {
                    "link.exec" => link_exec(&ctx, &live, &id, &params),
                    "link.checkSteps" => check_steps(&ctx, &live, &id, &params),
                    m => mule_lease::call(&ctx, &live, &id, m, &params),
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
    mule::helper::snapshot_path();
    // `--config PATH` is the only option; it is read once, at the handshake.
    let mut config_path = None;
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        match (a.as_str(), args.next()) {
            ("--config", Some(p)) => config_path = Some(std::path::PathBuf::from(p)),
            _ => {
                eprintln!("usage: mu-plugin-mule [--config PATH]");
                std::process::exit(2);
            }
        }
    }
    let ctx = Ctx {
        out: Arc::new(Mutex::new(std::io::stdout())),
        calls: Arc::default(),
        mule: bundled_mule(),
        config_path,
        snap: Arc::default(),
        spawned: Arc::default(),
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
    for live in ctx.calls.lock().unwrap().values() {
        live.cancelled.store(true, Ordering::SeqCst);
    }
    let t = Instant::now();
    while !ctx.calls.lock().unwrap().is_empty() && t.elapsed() < STOP_GRACE + Duration::from_secs(2)
    {
        thread::sleep(Duration::from_millis(20));
    }
    // The private config copy goes first: the group kill below ends us.
    if let Some(s) = ctx.snap.lock().unwrap().as_ref()
        && let Some(c) = &s.config
    {
        let _ = std::fs::remove_file(c);
    }
    if !ctx.spawned.load(Ordering::SeqCst) {
        return; // no mule was ever started: nothing of ours can be left
    }
    // Hard bound, independent of `ps`, in EVERY state -- an empty call map
    // proves only that each direct mule was waited for, not that its helper
    // died: after a timeout reply the call is gone yet a reparented ssh may
    // still hold the capped channel (a failing `ps` finds nothing at all). Everything we spawned shares our
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
    fn a_passed_deadline_spawns_nothing() {
        use std::os::unix::fs::PermissionsExt;
        let dir = std::env::temp_dir().join(format!("mpm-unit-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let marker = dir.join("ran");
        let bin = dir.join("mule");
        std::fs::write(&bin, format!("#!/bin/sh\ntouch {}\n", marker.display())).unwrap();
        std::fs::set_permissions(&bin, std::fs::Permissions::from_mode(0o755)).unwrap();
        let ctx = Ctx {
            out: Arc::new(Mutex::new(std::io::stdout())),
            calls: Arc::default(),
            mule: Some(bin),
            config_path: None,
            snap: Arc::default(),
            spawned: Arc::default(),
        };
        let snap = Snap {
            instance: String::new(),
            hosts: None,
            config: Some(dir.join("c.toml")),
        };
        let live = Live::default();
        let past = Some(now_ms() - 1);
        let r = run_mule(&ctx, &snap, &live, &[], Duration::from_secs(5), past).unwrap();
        assert!(matches!(r, Run::Expired));
        assert!(!marker.exists());
        assert!(!ctx.spawned.load(Ordering::SeqCst));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn the_handshake_does_not_claim_fetch_or_a_channel() {
        let h = handshake("mule-x");
        assert!(h["seams"].get("channel").is_none());
        assert_eq!(h["seams"]["link"]["fetch"], false);
        assert_eq!(h["seams"]["link"]["slots"], "self");
    }
}
