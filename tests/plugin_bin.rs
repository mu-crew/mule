//! `mu-plugin-mule` over stdio, against the ssh shim (layer 2): the handshake,
//! `link.exec` result mapping and the hand-back. mu's own `link-contract`
//! suite drives the same binary through mu's adapter.

mod common;

use std::io::{BufRead, BufReader, Write};
use std::os::unix::process::CommandExt;
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};

use common::local::{ShimHost, tmux_available};
use serde_json::{Value, json};

struct Plugin {
    child: Child,
    stdin: Option<ChildStdin>,
    stdout: BufReader<ChildStdout>,
    next: u64,
    /// Bytes of the last reply line, newline included: mu's frame bound.
    last_len: usize,
}

impl Plugin {
    fn start(h: &ShimHost) -> Self {
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
        Self {
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
    }
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
fn handshake_declares_the_link_only() {
    let Some(h) = host("pb-hello") else { return };
    let mut p = Plugin::start(&h);
    let v = p.call(
        "initialize",
        json!({"protocol": {"min": 1, "max": 1}, "mu": "5"}),
    );
    let r = &v["result"];
    assert_eq!(r["plugin"]["name"], "mule");
    assert!(r["instance"].as_str().unwrap().starts_with("mule-"));
    assert!(r["schema"].as_str().unwrap().starts_with("sha256:"));
    let seams = r["seams"].as_object().unwrap();
    assert_eq!(seams.keys().collect::<Vec<_>>(), ["link"]);
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
    std::fs::write(
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
    assert!(t.elapsed() < std::time::Duration::from_secs(8));
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
    std::fs::write(
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
    std::fs::write(&ps, "#!/bin/sh\nexit 1\n").unwrap();
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
        let mut p = Plugin::start(h);
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
