//! `mu-plugin-mule` over stdio, against the ssh shim (layer 2): the handshake,
//! `link.exec` result mapping and the hand-back. mu's own `link-contract`
//! suite drives the same binary through mu's adapter.

mod common;

use std::io::{BufRead, BufReader, Write};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};

use common::local::{ShimHost, tmux_available};
use serde_json::{Value, json};

struct Plugin {
    child: Child,
    stdin: Option<ChildStdin>,
    stdout: BufReader<ChildStdout>,
    next: u64,
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
            .spawn()
            .unwrap();
        Self {
            stdin: child.stdin.take(),
            stdout: BufReader::new(child.stdout.take().unwrap()),
            child,
            next: 1,
        }
    }

    fn call(&mut self, method: &str, params: Value) -> Value {
        let id = self.next;
        self.next += 1;
        let line = json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params});
        writeln!(self.stdin.as_mut().unwrap(), "{line}").unwrap();
        let mut reply = String::new();
        self.stdout.read_line(&mut reply).unwrap();
        let v: Value = serde_json::from_str(&reply).unwrap();
        assert_eq!(v["id"], id);
        v
    }

    fn exec(&mut self, script: &str, timeout_ms: u64) -> Value {
        self.call(
            "link.exec",
            json!({"target": {"host": {"name": "shim"}}, "script": script, "cwd": ".",
                   "timeoutMs": timeout_ms, "deadline": 0}),
        )
    }
}

impl Drop for Plugin {
    fn drop(&mut self) {
        drop(self.stdin.take()); // EOF is the stop signal
        let _ = self.child.wait();
    }
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
    assert_eq!(r["instance"], "mule");
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
