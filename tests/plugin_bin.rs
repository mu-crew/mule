//! `mu-plugin-mule` over stdio, against the ssh shim (layer 2): the handshake,
//! `link.exec` result mapping and the hand-back. mu's own `link-contract`
//! suite drives the same binary through mu's adapter.

mod common;

use std::io::{BufRead, BufReader, Write};
use std::os::unix::process::CommandExt;
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use common::local::{SPAWN_GATE, ShimHost, tmux_available, write_exec};
use serde_json::{Value, json};

struct Plugin {
    child: Child,
    stdin: Option<ChildStdin>,
    stdout: BufReader<ChildStdout>,
    next: u64,
    /// Bytes of the last reply line, newline included: mu's frame bound.
    last_len: usize,
    /// Held for the plugin's life; see `Permit`.
    _permit: Permit,
    /// Set on drop: stands the watchdog down.
    done: Arc<AtomicBool>,
}

/// At most `LIVE` plugins at once. 15 tests each spawning mule, ssh shims and
/// tmux saturate the machine, and mule's `-O check` gives up after 2 s
/// (CHECK_TIMEOUT): under that load a healthy shim master read as down, 2 of
/// 10 runs. Bounded concurrency keeps the suite honest about what it tests.
const LIVE: usize = 4;

struct Permit;

static LIVE_COUNT: std::sync::Mutex<usize> = std::sync::Mutex::new(0);
static LIVE_FREED: std::sync::Condvar = std::sync::Condvar::new();

impl Permit {
    fn take() -> Self {
        let mut n = LIVE_COUNT.lock().unwrap();
        while *n >= LIVE {
            n = LIVE_FREED.wait(n).unwrap();
        }
        *n += 1;
        Permit
    }
}

impl Drop for Permit {
    fn drop(&mut self) {
        *LIVE_COUNT.lock().unwrap() -= 1;
        LIVE_FREED.notify_one();
    }
}

/// No test may wait unbounded on its plugin: a missing reply, or a plugin that
/// never exits on EOF, would otherwise hang `cargo test` forever. After this
/// long the plugin's group is killed, so a blocked read sees EOF and the test
/// fails instead.
const WATCHDOG: Duration = Duration::from_secs(120);

fn watchdog(pgid: i32, done: Arc<AtomicBool>) {
    std::thread::spawn(move || {
        let t = Instant::now();
        while t.elapsed() < WATCHDOG {
            if done.load(Ordering::SeqCst) {
                return;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        // SAFETY: plain kill(2) on the plugin's own group.
        unsafe { libc::kill(-pgid, libc::SIGKILL) };
    });
}

impl Plugin {
    /// Spawn and complete the handshake: a dispatch needs one.
    fn start(h: &ShimHost) -> Self {
        let mut p = Self::start_raw(h);
        let v = p.call(
            "initialize",
            json!({"protocol": {"min": 1, "max": 1}, "mu": "5"}),
        );
        assert!(v["result"]["instance"].is_string(), "{v}");
        p
    }

    fn start_raw(h: &ShimHost) -> Self {
        let permit = Permit::take();
        // Not while another test holds a shim's write fd open: a fork here
        // would inherit it and the shim's exec would fail ETXTBSY.
        let gate = SPAWN_GATE.read().unwrap();
        let mut child = Command::new(env!("CARGO_BIN_EXE_mu-plugin-mule"))
            .arg("--config")
            .arg(&h.config)
            .env("PATH", h.path_env(None))
            .env("XDG_STATE_HOME", &h.state)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            // Its own group, as mu runs a plugin: mu kills that group.
            .process_group(0)
            .spawn()
            .unwrap();
        drop(gate);
        let done = Arc::new(AtomicBool::new(false));
        watchdog(child.id() as i32, done.clone());
        Self {
            _permit: permit,
            done,
            stdin: child.stdin.take(),
            stdout: BufReader::new(child.stdout.take().unwrap()),
            child,
            next: 1,
            last_len: 0,
        }
    }

    fn call(&mut self, method: &str, params: Value) -> Value {
        let id = self.next;
        self.next += 1;
        let line = json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params});
        writeln!(self.stdin.as_mut().unwrap(), "{line}").unwrap();
        let mut reply = String::new();
        self.stdout.read_line(&mut reply).unwrap();
        self.last_len = reply.len();
        let v: Value = serde_json::from_str(&reply).unwrap();
        assert_eq!(v["id"], id);
        v
    }

    fn exec(&mut self, script: &str, timeout_ms: u64) -> Value {
        self.exec_by(script, timeout_ms, now_ms() + timeout_ms + 60_000)
    }

    fn exec_by(&mut self, script: &str, timeout_ms: u64, deadline: u64) -> Value {
        self.call("link.exec", exec_params(script, timeout_ms, deadline))
    }

    /// Write a request without waiting for its reply.
    fn send(&mut self, id: u64, method: &str, params: Value) {
        let line = json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params});
        writeln!(self.stdin.as_mut().unwrap(), "{line}").unwrap();
    }
}

impl Drop for Plugin {
    fn drop(&mut self) {
        drop(self.stdin.take()); // EOF is the stop signal
        let _ = self.child.wait();
        self.done.store(true, Ordering::SeqCst);
    }
}

fn job_params(key: &str, handle: Option<&str>) -> Value {
    let mut p = json!({"target": {"host": {"name": "shim"}, "job": {"kind": "lease", "key": key}},
                       "deadline": now_ms() + 60_000});
    if let Some(handle) = handle {
        p["target"]["job"]["handle"] = json!(handle);
    }
    p
}

fn start_lease(
    p: &mut Plugin,
    key: &str,
    cwd: &std::path::Path,
    file: &std::path::Path,
    cmd: &str,
) -> Value {
    let mut params = job_params(key, None);
    params["key"] = json!(key);
    params["spec"] = json!({"workstream": "demo", "name": "worker", "harness": "pi",
        "role": "read-only", "commandSource": "explicit", "cmd": cmd, "cwd": cwd,
        "env": {"LEASE_TEST": "a'b $HOME", "MU_AGENT_NAME": "worker"}, "jobFile": file});
    let r = p.call("job.start", params);
    assert!(r["result"]["view"]["command"].is_string(), "{r}");
    assert!(r["result"].get("handle").is_none(), "{r}");
    r["result"]["view"].clone()
}

fn lookup(p: &mut Plugin, key: &str) -> Value {
    let mut params = job_params(key, None);
    params["key"] = json!(key);
    params["fence"] = json!(false);
    p.call("job.lookup", params)
}

/// A launched carrier, and everything it has printed so far.
struct Carrier(Child, Arc<std::sync::Mutex<String>>);

impl Carrier {
    fn start(h: &ShimHost, view: &Value) -> Self {
        let mut cmd = Command::new("sh");
        cmd.arg("-c").arg(view["command"].as_str().unwrap());
        for (k, v) in view["env"].as_object().unwrap() {
            cmd.env(k, v.as_str().unwrap());
        }
        let _gate = SPAWN_GATE.read().unwrap();
        let mut child = cmd
            .env("PATH", h.path_env(None))
            .env("XDG_STATE_HOME", &h.state)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .process_group(0)
            .spawn()
            .unwrap();
        let out = Arc::<std::sync::Mutex<String>>::default();
        let (stdout, sink) = (child.stdout.take().unwrap(), Arc::clone(&out));
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                let mut sink = sink.lock().unwrap();
                sink.push_str(&line);
                sink.push('\n');
            }
        });
        Self(child, out)
    }

    /// Wait until the carrier holds: `hold` prints its first status line only
    /// once the start or attach has returned, so its signal path knows the
    /// job by then.
    fn wait_holding(&self) {
        common::local::wait_until("carrier holding", 20, || {
            self.1.lock().unwrap().contains(": starting; forwards:")
        });
    }

    fn signal(&self, signal: i32) {
        // SAFETY: this is a fresh process group owned by the test.
        unsafe { libc::kill(-(self.0.id() as i32), signal) };
    }
}

impl Drop for Carrier {
    fn drop(&mut self) {
        if self.0.try_wait().ok().flatten().is_none() {
            self.signal(libc::SIGKILL);
            let _ = self.0.wait();
        }
    }
}

#[test]
fn lease_view_recovers_by_file_and_key_and_uses_the_handshake_host() {
    let Some(h) = host("pb-lease") else { return };
    let log = log_ssh(&h);
    let mut p = Plugin::start(&h);
    let cfg = std::fs::read_to_string(&h.config).unwrap();
    std::fs::write(&h.config, cfg.replace("mule.sock", "other.sock")).unwrap();
    let cwd = h.dir.join("quoted ' cwd");
    std::fs::create_dir(&cwd).unwrap();
    let file = h.dir.join("quoted ' handle");
    let key = "lease-a";
    let view = start_lease(
        &mut p,
        key,
        &cwd,
        &file,
        "printf '%s|%s' \"$LEASE_TEST\" \"$MU_AGENT_NAME\" > proof; sleep 90",
    );
    assert!(
        !file.exists() && !log.exists(),
        "start RPC must only return a launcher"
    );
    assert_eq!(lookup(&mut p, key)["result"], json!({"none": true}));
    let mut carrier = Carrier::start(&h, &view);
    common::local::wait_until("remote command", 20, || cwd.join("proof").exists());
    assert_eq!(
        std::fs::read_to_string(cwd.join("proof")).unwrap(),
        "a'b $HOME|worker"
    );
    carrier.signal(libc::SIGKILL);
    carrier.0.wait().unwrap();
    h.master_down(true);
    let handle = std::fs::read_to_string(&file).unwrap();
    let job = job_params(key, Some(handle.trim()));
    for method in ["job.status", "job.stop"] {
        let result = p.call(method, job.clone());
        assert_eq!(result["error"]["data"]["kind"], "human", "{result}");
    }
    assert_eq!(lookup(&mut p, key)["error"]["data"]["kind"], "human");
    assert_eq!(std::fs::read_to_string(&file).unwrap(), handle);
    h.master_down(false);
    assert_eq!(
        lookup(&mut p, key)["result"],
        json!({"found": {"handle": handle.trim()}})
    );
    assert_eq!(
        lookup(&mut p, "lease")["result"],
        json!({"none": true}),
        "not a prefix match"
    );
    assert_eq!(
        p.call("job.status", job.clone())["result"],
        json!({"state":"running"})
    );
    let mut attach = job.clone();
    attach["jobFile"] = json!(file);
    let attached = p.call("job.attach", attach);
    let view = &attached["result"];
    assert!(view["command"].is_string(), "{attached}");
    drop(p); // A returned launcher must not need the protocol process or its temp copy.
    let mut attached = Carrier::start(&h, view);
    let fence = h.state.join("mule/stopping").join(handle.trim());
    common::local::wait_until("attach owns generation", 20, || {
        std::fs::read_to_string(&fence).is_ok_and(|s| s.trim() == "1")
    });
    assert_eq!(std::fs::read_to_string(&file).unwrap(), handle);
    // The fence is bumped just before the signal path is handed the job, and
    // a TERM between the two stops nothing (by design: the job runs on).
    attached.wait_holding();
    attached.signal(libc::SIGTERM);
    // Reap it, as the pane's shell would. A carrier TERMed while its probe
    // is queued exits leaving `waiter.<n>` with its pid, and the lock steps
    // over that ticket only once `kill(pid, 0)` fails, which it never does
    // for an unreaped zombie: the detached `rm` then queued behind it
    // forever. Caught under load as `waiter.8` = the carrier's pid in `ps`
    // state Z, the `rm` on ticket 9 (CI runs 45195619097, 45359523640).
    let mut code = None;
    common::local::wait_until("attached carrier exited", 20, || {
        code = attached.0.try_wait().unwrap().map(|s| s.code());
        code.is_some()
    });
    assert_eq!(code, Some(Some(143)), "a TERMed carrier exits 128+15");
    common::local::wait_until("attached carrier stopped the job", 20, || {
        !h.jobs().join(handle.trim()).exists()
    });
    let mut p = Plugin::start(&h);
    assert_eq!(p.call("job.status", job.clone())["result"]["state"], "lost");
    let stopped = p.call("job.stop", job);
    assert_eq!(stopped.get("result"), Some(&Value::Null), "{stopped}");
    let ssh = std::fs::read_to_string(log).unwrap();
    // Only the final status and idempotent stop used the restarted plugin.
    let (old, new) = ssh.split_once("other.sock").unwrap();
    assert!(
        old.contains("mule.sock") && !new.contains("mule.sock"),
        "{ssh}"
    );
}

#[test]
fn lease_claim_before_file_publication_is_found_exactly_by_key() {
    let Some(h) = host("pb-claim") else { return };
    let claimed = h.dir.join("claim-landed");
    let shim = h.dir.join("ssh");
    let script = std::fs::read_to_string(&shim).unwrap();
    write_exec(&shim, script.replace("exec sh -c \"$last\"", &format!(
        "case \"$last\" in *'if [ -n \"$hit\" ]; then exit 0; fi;'*)\n  sh -c \"$last\" || exit $?; : > '{}'; exec sleep 60;;\nesac\nexec sh -c \"$last\"", claimed.display()))).unwrap();
    let mut p = Plugin::start(&h);
    let file = h.dir.join("handle");
    // mu keys a job by its record token, a UUID.
    let key = "6f1c2a9e-3b4d-4e5f-8a7b-0c1d2e3f4a5b";
    let view = start_lease(&mut p, key, &h.dir, &file, "echo should-not-dispatch");
    let mut carrier = Carrier::start(&h, &view);
    common::local::wait_until("remote claim before its reply", 20, || claimed.exists());
    carrier.signal(libc::SIGKILL);
    carrier.0.wait().unwrap();
    assert!(!file.exists());
    let found = lookup(&mut p, key);
    let handle = found["result"]["found"]["handle"]
        .as_str()
        .unwrap_or_else(|| panic!("{found}"));
    let job = job_params(key, Some(handle));
    assert_eq!(
        p.call("job.status", job.clone())["result"]["state"],
        "unknown"
    );
    let stopped = p.call("job.stop", job);
    assert_eq!(stopped.get("result"), Some(&Value::Null), "{stopped}");
    assert_eq!(lookup(&mut p, key)["result"], json!({"none": true}));
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

fn exec_params(script: &str, timeout_ms: u64, deadline: u64) -> Value {
    json!({"target": {"host": {"name": "shim"}}, "script": script, "cwd": ".",
           "timeoutMs": timeout_ms, "deadline": deadline})
}

fn reach(p: &mut Plugin) -> Value {
    p.call(
        "link.checkSteps",
        json!({"target": {"host": {"name": "shim"}}, "phase": "reach",
               "deadline": now_ms() + 60_000}),
    )
}

fn host(tag: &str) -> Option<ShimHost> {
    if !tmux_available() {
        eprintln!("skip: no tmux");
        return None;
    }
    Some(ShimHost::new(tag))
}

#[test]
fn handshake_matches_mu_d15_schema() {
    let Some(h) = host("pb-hello") else { return };
    let mut p = Plugin::start_raw(&h);
    let v = p.call(
        "initialize",
        json!({"protocol": {"min": 1, "max": 1}, "mu": "5"}),
    );
    let r = &v["result"];
    assert_eq!(r["plugin"]["name"], "mule");
    assert!(r["instance"].as_str().unwrap().starts_with("mule-"));
    assert_eq!(r["protocol"], 1);
    let seams = r["seams"].as_object().unwrap();
    assert_eq!(
        seams["job"],
        json!({
            "kind": "lease", "view": "bound", "handle": "file",
            "start": {"lookup": true, "fenceKey": false},
            "honours": {"command": true, "env": true, "sockets": true,
                        "roles": ["full-access", "read-only"]}
        })
    );
    // mu 69619eba: digest of the published schema that added jobFile and
    // the attach socket pair. A sha256 prefix alone doesn't pass loadRefusals.
    assert_eq!(
        r["schema"],
        "sha256:54d56d9a865c4e9302f2b63158cb595010ecbb0b59037550609a54d79392ac36"
    );
    assert!(
        !seams.contains_key("channel"),
        "mu selects its built-in channel"
    );
    assert_eq!(seams["link"]["fetch"], false);
    assert_eq!(seams["link"]["slots"], "self");
    assert_eq!(seams["link"]["quiescence"], "provable");
}

#[test]
fn exec_maps_codes_and_hands_back_on_strict_3() {
    let Some(h) = host("pb-exec") else { return };
    let mut p = Plugin::start(&h);
    let v = p.exec("echo out; echo err >&2; exit 7", 20_000);
    assert_eq!(v["result"]["exitCode"], 7);
    assert_eq!(v["result"]["stdout"], "out\n");
    assert_eq!(v["result"]["stderr"], "err\n");
    // A command's own 3 is strict-codes' 8: a command result, not a hand-back.
    assert_eq!(p.exec("exit 3", 20_000)["result"]["exitCode"], 8);

    h.master_down(true);
    let v = p.exec("touch ran", 20_000);
    assert_eq!(v["error"]["data"]["kind"], "human");
    assert!(
        v["error"]["data"]["remedy"]
            .as_str()
            .unwrap()
            .starts_with("ssh -MNf"),
        "{v}"
    );
}

#[test]
fn a_timeout_has_no_exit_code() {
    let Some(h) = host("pb-timeout") else { return };
    let mut p = Plugin::start(&h);
    let v = p.exec("exec sleep 30", 500);
    assert!(v["result"]["exitCode"].is_null(), "{v}");
}

#[test]
fn an_escaped_reply_stays_inside_mus_frame() {
    // NUL is six bytes once JSON-escaped: 2 x 128 KiB decoded was a
    // 1,572,953-byte reply, which mu refuses (and then drops the plugin).
    let Some(h) = host("pb-frame") else { return };
    let mut p = Plugin::start(&h);
    let v = p.exec(
        "head -c 131072 /dev/zero; head -c 131072 /dev/zero >&2",
        20_000,
    );
    assert!(p.last_len <= 1_048_576, "reply is {} bytes", p.last_len);
    assert_eq!(v["result"]["exitCode"], 0, "{v}");
    assert_eq!(v["result"]["partial"], true);
    assert!(!v["result"]["stdout"].as_str().unwrap().is_empty());
    // And a reply that fits is untouched.
    let v = p.exec("printf 'a\\0b'", 20_000);
    assert_eq!(v["result"]["stdout"], "a\u{0}b");
    assert_eq!(v["result"]["partial"], false);
}

#[test]
fn mules_own_truncation_is_partial_and_a_commands_7_is_not() {
    let Some(h) = host("pb-upstream") else { return };
    let mut config = std::fs::read_to_string(&h.config).unwrap();
    config.push_str("max_log_bytes = 1024\n");
    std::fs::write(&h.config, config).unwrap();
    let mut p = Plugin::start(&h);
    let v = p.exec("head -c 4096 /dev/zero | tr '\\000' x >&2", 20_000);
    assert_eq!(v["result"]["exitCode"], 7, "{v}");
    assert_eq!(v["result"]["partial"], true, "{v}");
    let v = p.exec(
        "echo 'mule: output exceeded max_log_bytes' >&2; exit 7",
        20_000,
    );
    assert_eq!(v["result"]["exitCode"], 7, "{v}");
    assert_eq!(
        v["result"]["partial"], false,
        "a command's own 7 and its stderr: {v}"
    );
}

#[test]
fn reach_proves_remote_tmux_and_a_down_master_stops_at_the_handback() {
    let Some(h) = host("pb-reach") else { return };
    let mut p = Plugin::start(&h);
    let steps = reach(&mut p)["result"].clone();
    let names: Vec<_> = steps
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s["step"].as_str().unwrap())
        .collect();
    assert_eq!(names, ["master", "tmux"], "{steps}");
    assert!(
        steps.as_array().unwrap().iter().all(|s| s["ok"] == true),
        "{steps}"
    );

    // The master answers `-O check`, but the remote shell has no tmux.
    let shim = h.dir.join("ssh");
    let text = std::fs::read_to_string(&shim).unwrap();
    write_exec(
        &shim,
        text.replace("exec sh -c", "PATH=/nonexistent exec /bin/sh -c"),
    )
    .unwrap();
    let steps = reach(&mut p)["result"].clone();
    let steps = steps.as_array().unwrap();
    assert_eq!(steps[0]["ok"], true);
    let tmux = steps
        .iter()
        .find(|s| s["step"] == "tmux")
        .expect("a tmux step");
    assert_eq!(tmux["ok"], false);

    h.master_down(true);
    let steps = reach(&mut p)["result"].clone();
    let steps = steps.as_array().unwrap();
    assert_eq!(steps.len(), 1);
    assert_eq!(steps[0]["step"], "handback");
}

#[test]
fn nothing_starts_after_the_wire_deadline() {
    let Some(h) = host("pb-deadline") else { return };
    let mut p = Plugin::start(&h);
    let ran = h.dir.join("ran");
    let v = p.exec_by(
        &format!("touch {}", ran.display()),
        12_000,
        now_ms() - 1_000,
    );
    assert!(v["error"].is_object(), "{v}");
    assert!(!ran.exists(), "an expired call dispatched");
    // A live deadline also bounds the run: 700 ms left of a 30 s timeoutMs.
    let t = std::time::Instant::now();
    let v = p.exec_by("exec sleep 30", 30_000, now_ms() + 700);
    assert!(v["result"]["exitCode"].is_null(), "{v}");
    // Not the 30 s timeoutMs. Alone it is 2-4 s (700 ms plus the stop); in a
    // loaded 12-test run (load 12) it went past 8 s, hence the headroom.
    assert!(t.elapsed() < std::time::Duration::from_secs(20));
}

#[test]
fn a_cancel_right_behind_its_request_runs_nothing() {
    let Some(h) = host("pb-cancel") else { return };
    let mut p = Plugin::start(&h);
    let started = h.dir.join("started");
    let params = exec_params(
        &format!("touch {}", started.display()),
        20_000,
        now_ms() + 60_000,
    );
    // One write, so the reader sees the cancel before the worker can register.
    let mut both = String::new();
    both.push_str(
        &json!({"jsonrpc": "2.0", "id": 100, "method": "link.exec", "params": params}).to_string(),
    );
    both.push('\n');
    both.push_str(
        &json!({"jsonrpc": "2.0", "method": "$/cancel", "params": {"id": 100}}).to_string(),
    );
    both.push('\n');
    p.stdin
        .as_mut()
        .unwrap()
        .write_all(both.as_bytes())
        .unwrap();
    let v = p.call("shutdown", json!({}));
    assert!(v["result"].is_null());
    // Long enough for an un-cancelled exec to dispatch and answer.
    std::thread::sleep(std::time::Duration::from_millis(1500));
    drop(p.stdin.take());
    p.child.wait().unwrap();
    let mut rest = String::new();
    std::io::Read::read_to_string(&mut p.stdout, &mut rest).unwrap();
    assert_eq!(rest, "", "a cancelled call answered");
    assert!(!started.exists(), "a cancelled call was dispatched");
}

#[test]
fn eof_kills_the_local_ssh_a_stalled_call_holds() {
    let Some(h) = host("pb-eof") else { return };
    let pids = h.dir.join("pids");
    let shim = h.dir.join("ssh");
    let text = std::fs::read_to_string(&shim).unwrap();
    // Every non-`-O` ssh records itself and stalls (a hung session).
    write_exec(
        &shim,
        text.replace(
            "exec sh -c \"$last\"",
            &format!("echo $$ >> {}; exec sleep 300", pids.display()),
        ),
    )
    .unwrap();
    // And `ps` is useless (absent, or failing): shutdown must not depend on
    // discovering the tree. A stalled helper has to die regardless.
    let ps = h.dir.join("ps");
    write_exec(&ps, "#!/bin/sh\nexit 1\n").unwrap();
    std::fs::set_permissions(&ps, std::os::unix::fs::PermissionsExt::from_mode(0o755)).unwrap();
    let mut p = Plugin::start(&h);
    p.send(
        1,
        "link.exec",
        exec_params("true", 120_000, now_ms() + 600_000),
    );
    let t = std::time::Instant::now();
    while std::fs::read_to_string(&pids).map_or(true, |t| t.is_empty()) {
        assert!(
            t.elapsed() < std::time::Duration::from_secs(10),
            "ssh never ran"
        );
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    drop(p.stdin.take());
    let t = std::time::Instant::now();
    p.child.wait().unwrap();
    assert!(t.elapsed() < std::time::Duration::from_secs(10));
    std::thread::sleep(std::time::Duration::from_millis(300));
    let all = std::fs::read_to_string(&pids).unwrap();
    for pid in all.lines().map(|l| l.trim().parse::<i32>().unwrap()) {
        // SAFETY: signal 0 only probes for existence.
        let alive = unsafe { libc::kill(pid, 0) } == 0;
        if alive {
            unsafe { libc::kill(pid, libc::SIGKILL) };
        }
        assert!(!alive, "ssh {pid} outlived the plugin");
    }
}

#[test]
fn the_instance_names_the_backend_not_the_plugin() {
    let Some(h) = host("pb-instance") else { return };
    let hello = |h: &ShimHost| {
        let mut p = Plugin::start_raw(h);
        let v = p.call(
            "initialize",
            json!({"protocol": {"min": 1, "max": 1}, "mu": "5"}),
        );
        v["result"]["instance"].as_str().unwrap().to_string()
    };
    let a = hello(&h);
    assert_eq!(a, hello(&h), "stable across restarts");
    assert!(a.starts_with("mule-"), "{a}");
    assert!(
        !a.contains(h.dir.to_str().unwrap()),
        "discloses a path: {a}"
    );
    let config = std::fs::read_to_string(&h.config).unwrap();
    std::fs::write(
        &h.config,
        config.replace("target = \"shim\"", "target = \"elsewhere\""),
    )
    .unwrap();
    assert_ne!(a, hello(&h), "a retargeted host kept its instance");
}

#[test]
fn wedged_peers_do_not_delay_the_handshake() {
    let Some(h) = host("pb-wedged") else { return };
    // Six more configured hosts, and an ssh that never answers a `-O check`.
    let mut config = std::fs::read_to_string(&h.config).unwrap();
    for i in 0..6 {
        config.push_str(&format!(
            "[hosts.peer{i}]\ntarget = \"peer{i}\"\nsocket = \"{}\"\n",
            h.dir.join(format!("peer{i}.sock")).display()
        ));
    }
    std::fs::write(&h.config, config).unwrap();
    write_exec(h.dir.join("ssh"), "#!/bin/sh\nexec sleep 300\n").unwrap();
    let mut p = Plugin::start_raw(&h);
    let t = std::time::Instant::now();
    let v = p.call(
        "initialize",
        json!({"protocol": {"min": 1, "max": 1}, "mu": "5"}),
    );
    assert!(v["result"]["instance"].is_string(), "{v}");
    // mu caps the initialize read at 10 s; before, six wedged peers took 15 s.
    assert!(
        t.elapsed() < std::time::Duration::from_secs(2),
        "initialize took {:?}",
        t.elapsed()
    );
}

/// Make the shim ssh record the argv of every non-`-O` call.
fn log_ssh(h: &ShimHost) -> std::path::PathBuf {
    let log = h.dir.join("ssh.log");
    let shim = h.dir.join("ssh");
    let text = std::fs::read_to_string(&shim).unwrap();
    write_exec(
        &shim,
        text.replacen(
            "for last; do :; done\n",
            &format!("for last; do :; done\necho \"$*\" >> {}\n", log.display()),
            1,
        ),
    )
    .unwrap();
    // Warm the rewritten script as `ShimHost::new` warms the first: a fresh
    // script's first exec on macOS can outlast mule's 2s `-O check`, and a
    // loaded local run lost this test's first carrier to "no control master".
    let _gate = SPAWN_GATE.read().unwrap();
    let _ = Command::new(&shim).arg("-O").output();
    log
}

#[test]
fn a_retargeted_config_never_redirects_a_live_session() {
    let Some(h) = host("pb-snapshot") else { return };
    let log = log_ssh(&h);
    let mut p = Plugin::start(&h);
    assert_eq!(
        p.exec("printf before", 20_000)["result"]["stdout"],
        "before"
    );
    // An atomic replace, as an editor or a deploy does it.
    let config = std::fs::read_to_string(&h.config).unwrap();
    let new = h.dir.join("config.new");
    std::fs::write(
        &new,
        config
            .replace("target = \"shim\"", "target = \"elsewhere\"")
            .replace("mule.sock", "other.sock"),
    )
    .unwrap();
    std::fs::rename(&new, &h.config).unwrap();
    let v = p.exec("printf after", 20_000);
    assert_eq!(v["result"]["stdout"], "after", "{v}");
    let v = reach(&mut p);
    assert!(v["result"].is_array(), "{v}");
    let log = std::fs::read_to_string(&log).unwrap();
    assert!(
        !log.contains("elsewhere") && !log.contains("other.sock"),
        "{log}"
    );
    assert!(log.contains("mule.sock"), "{log}");
}

#[test]
fn a_dispatch_before_the_handshake_runs_nothing() {
    let Some(h) = host("pb-nohandshake") else {
        return;
    };
    let mut p = Plugin::start_raw(&h);
    let ran = h.dir.join("ran");
    let v = p.exec(&format!("touch {}", ran.display()), 20_000);
    assert!(v["error"].is_object(), "{v}");
    assert!(!ran.exists());
}

#[test]
fn preflight_uses_the_snapshot_and_dispatches_before_the_deadline() {
    let Some(h) = host("pb-late") else { return };
    let mut p = Plugin::start(&h);
    // A per-call config read would wait on this FIFO until the writer opens
    // at 20 s, after the 12 s wire deadline. The handshake snapshot lets the
    // command run and reply before that deadline without reopening the path.
    std::fs::remove_file(&h.config).unwrap();
    let c = std::ffi::CString::new(h.config.to_str().unwrap()).unwrap();
    assert_eq!(unsafe { libc::mkfifo(c.as_ptr(), 0o600) }, 0);
    let deadline = now_ms() + 12_000;
    let ran = h.dir.join("ran");
    let writer = std::thread::spawn({
        let cfg = h.config.clone();
        move || {
            std::thread::sleep(std::time::Duration::from_millis(20_000));
            // Release a reader, if any ever blocked.
            // O_NONBLOCK: without a reader a plain open blocks forever.
            let _ = std::os::unix::fs::OpenOptionsExt::custom_flags(
                std::fs::OpenOptions::new().write(true),
                libc::O_NONBLOCK,
            )
            .open(cfg);
        }
    });
    let v = p.exec_by(&format!("touch {}", ran.display()), 20_000, deadline);
    drop(writer); // detached: it only releases a reader if one blocked
    assert!(now_ms() < deadline, "reply missed the wire deadline: {v}");
    assert_eq!(v["result"]["exitCode"], 0, "{v}");
    assert!(ran.exists(), "the command did not run: {v}");
}

#[test]
fn eof_after_a_timeout_reply_still_kills_the_stalled_helper() {
    let Some(h) = host("pb-eof-late") else { return };
    let pids = h.dir.join("pids");
    let shim = h.dir.join("ssh");
    let text = std::fs::read_to_string(&shim).unwrap();
    write_exec(
        &shim,
        text.replace(
            "exec sh -c \"$last\"",
            &format!("echo $$ >> {}; exec sleep 300", pids.display()),
        ),
    )
    .unwrap();
    let ps = h.dir.join("ps");
    write_exec(&ps, "#!/bin/sh\nexit 1\n").unwrap();
    std::fs::set_permissions(&ps, std::os::unix::fs::PermissionsExt::from_mode(0o755)).unwrap();
    let mut p = Plugin::start(&h);
    // The timeout reply empties the call map; the helper may still live.
    let v = p.exec("true", 4_000);
    assert!(v["result"]["exitCode"].is_null(), "{v}");
    std::thread::sleep(std::time::Duration::from_millis(200));
    drop(p.stdin.take());
    p.child.wait().unwrap();
    std::thread::sleep(std::time::Duration::from_millis(300));
    let all = std::fs::read_to_string(&pids).unwrap();
    assert!(!all.trim().is_empty(), "ssh never ran");
    for pid in all.lines().map(|l| l.trim().parse::<i32>().unwrap()) {
        // SAFETY: signal 0 only probes for existence.
        let alive = unsafe { libc::kill(pid, 0) } == 0;
        if alive {
            unsafe { libc::kill(pid, libc::SIGKILL) };
        }
        assert!(
            !alive,
            "ssh {pid} outlived the plugin after a timeout reply"
        );
    }
}
