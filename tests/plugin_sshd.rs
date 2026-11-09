//! Test layer 3 for the plugin surface (mu plugins spec, D15), against a
//! real session-capped `sshd`: `run --key` dispatch dedupe, the five-state
//! `poll --json`, `ls --json --key-prefix`, a keyed lease, and
//! `exec --no-detach`.
//!
//! Layers 1 and 2 (`tests/keys.rs`, `tests/status.rs`, `tests/cancel.rs`)
//! run the same scripts under a local `sh`; this runs them through mule's
//! private master on a `MaxSessions 1` connection, with the ticket lock and
//! the real ssh in the way. Skips with a printed reason when `sshd` is absent.

mod common;

use std::time::{Duration, Instant};

use common::sshd::Sshd;

/// A private tmux server name, torn down on drop, socket file included.
struct Tmux<'a> {
    sshd: &'a Sshd,
    name: String,
}

impl<'a> Tmux<'a> {
    fn new(sshd: &'a Sshd, tag: &str) -> Self {
        Self {
            sshd,
            name: format!("mule-p3-{}-{tag}", std::process::id()),
        }
    }

    fn has_session(&self, name: &str) -> bool {
        self.sshd
            .ssh(&["tmux", "-L", &self.name, "has-session", "-t", name])
            .status
            .success()
    }
}

impl Drop for Tmux<'_> {
    fn drop(&mut self) {
        let script = format!(
            "p=$(tmux -L {name} display-message -p '#{{socket_path}}' 2>/dev/null); \
             tmux -L {name} kill-server 2>/dev/null; \
             for c in \"$p\" \"${{TMUX_TMPDIR:-/tmp}}/tmux-$(id -u)/{name}\"; do \
               [ -n \"$c\" ] && [ -S \"$c\" ] && rm -f \"$c\"; \
             done; exit 0",
            name = self.name
        );
        let _ = self.sshd.ssh(&[&script]);
    }
}

fn stdout(out: &std::process::Output) -> String {
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

fn stderr(out: &std::process::Output) -> String {
    String::from_utf8_lossy(&out.stderr).into_owned()
}

fn json(out: &std::process::Output) -> serde_json::Value {
    serde_json::from_slice(&out.stdout)
        .unwrap_or_else(|e| panic!("not json ({e}): {:?} / {}", stdout(out), stderr(out)))
}

fn job_ids(sshd: &Sshd) -> Vec<String> {
    let mut ids: Vec<String> = std::fs::read_dir(sshd.state_root.join("mule/jobs"))
        .map(|dir| {
            dir.filter_map(Result::ok)
                .map(|e| e.file_name().to_string_lossy().into_owned())
                .collect()
        })
        .unwrap_or_default();
    ids.sort();
    ids
}

fn wait_until(what: &str, limit: Duration, mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + limit;
    while !done() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn mule_command(sshd: &Sshd, config: &std::path::Path, args: &[&str]) -> std::process::Command {
    let mut command = std::process::Command::new(env!("CARGO_BIN_EXE_mule"));
    command
        .arg("--config")
        .arg(config)
        .args(args)
        .env("PATH", sshd.path_env())
        .env("XDG_STATE_HOME", &sshd.state_root)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());
    command
}

#[test]
fn five_concurrent_dispatches_of_one_key_make_one_job() {
    // The check and the start are one locked round trip, so racing callers
    // on one client serialise on the ticket and the second sees the first's
    // key. Five at once over a MaxSessions 1 connection, as the gate test
    // dispatches five.
    require_sshd!();
    let sshd = Sshd::start();
    let _master = sshd.open_master(&sshd.socket);
    let tmux = Tmux::new(&sshd, "key-race");
    let config = sshd.write_config(&tmux.name);

    let children: Vec<_> = (0..5)
        .map(|_| {
            mule_command(
                &sshd,
                &config,
                &["run", "--quiet", "--key", "mu.race", "sleep 30"],
            )
            .spawn()
            .unwrap()
        })
        .collect();
    let outs: Vec<_> = children
        .into_iter()
        .map(|c| c.wait_with_output().unwrap())
        .collect();

    for out in &outs {
        assert!(out.status.success(), "{}", stderr(out));
    }
    let ids: std::collections::BTreeSet<_> = outs.iter().map(stdout).collect();
    assert_eq!(ids.len(), 1, "one id for one key: {ids:?}");
    let id = ids.into_iter().next().unwrap();
    assert_eq!(job_ids(&sshd), vec![id.clone()], "one job started");
    let found = sshd.mule(&config, &["ls", "--json", "--key-prefix", "mu."]);
    let rows = json(&found)["items"].as_array().unwrap().clone();
    assert_eq!(rows.len(), 1, "{rows:?}");
    assert_eq!(rows[0]["id"], id.as_str());
    assert_eq!(rows[0]["key"], "mu.race");
    assert!(sshd.mule(&config, &["rm", &id]).status.success());
}

#[test]
fn poll_json_answers_five_states_over_the_capped_channel() {
    require_sshd!();
    let sshd = Sshd::start();
    let master = sshd.open_master(&sshd.socket);
    let tmux = Tmux::new(&sshd, "five");
    let config = sshd.write_config(&tmux.name);
    let poll = |id: &str| {
        let out = sshd.mule(&config, &["poll", id, "--json"]);
        assert_eq!(out.status.code(), Some(0), "{id}: {}", stderr(&out));
        json(&out)
    };

    let running = stdout(&sshd.mule(&config, &["run", "--quiet", "sleep 30"]));
    let done = stdout(&sshd.mule(&config, &["run", "--quiet", "exit 6"]));
    wait_until("the job to finish", Duration::from_secs(15), || {
        poll(&done)["state"] == "done"
    });
    assert_eq!(poll(&running)["state"], "running");
    assert_eq!(poll(&done)["rc"], 6);
    assert!(sshd.mule(&config, &["rm", &done]).status.success());
    assert_eq!(poll(&done)["state"], "missing", "after rm's proof");
    assert_eq!(poll("0b0b0b")["state"], "missing", "never dispatched");

    // Unknown: the state dir cannot be searched, so absence cannot be seen.
    use std::os::unix::fs::PermissionsExt;
    let root = sshd.state_root.join("mule/jobs");
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o000)).unwrap();
    let enforced = std::fs::read_dir(&root).is_err();
    let unknown = enforced.then(|| poll(&running));
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700)).unwrap();
    if let Some(value) = unknown {
        assert_eq!(value["state"], "unknown", "{value}");
        assert!(value["reason"].as_str().is_some(), "{value}");
    } else {
        eprintln!("skipping the unknown case: modes are not enforced for this user");
    }

    // Orphan: the session is gone without an rc. SIGKILL to the pane's whole
    // process group, not `kill-session`: a hangup lets the wrapper's shell
    // write rc 129 first under some shells (zsh, here), and the job is then
    // done, not orphaned.
    let killed = sshd.ssh(&[&format!(
        "kill -9 -- -$(tmux -L {} list-panes -t mule-{running} -F '#{{pane_pid}}')",
        tmux.name
    )]);
    assert!(killed.status.success(), "{}", stderr(&killed));
    wait_until("the session to go", Duration::from_secs(10), || {
        !tmux.has_session(&format!("mule-{running}"))
    });
    assert_eq!(poll(&running)["state"], "orphan");

    // No master is still the hand-back, never an answer.
    drop(master);
    let out = sshd.mule(&config, &["poll", &running, "--json"]);
    assert_eq!(out.status.code(), Some(3), "{}", stderr(&out));
    assert!(out.stdout.is_empty());
}

#[test]
fn a_keyed_lease_holds_the_job_its_key_names() {
    require_sshd!();
    let sshd = Sshd::start();
    let _master = sshd.open_master(&sshd.socket);
    let tmux = Tmux::new(&sshd, "key-lease");
    let config = sshd.write_config(&tmux.name);
    let id = stdout(&sshd.mule(
        &config,
        &["run", "--quiet", "--key", "mu.lease", "sleep 3; exit 7"],
    ));
    let file = sshd.dir.join("w.job");

    let out = sshd.mule(
        &config,
        &[
            "run",
            "--lease",
            "--key",
            "mu.lease",
            "--job-file",
            file.to_str().unwrap(),
            "--",
            "sleep 30",
        ],
    );

    assert_eq!(out.status.code(), Some(7), "{}", stderr(&out));
    assert_eq!(std::fs::read_to_string(&file).unwrap(), format!("{id}\n"));
    assert_eq!(
        job_ids(&sshd),
        vec![id.clone()],
        "the lease started nothing"
    );
    assert!(sshd.mule(&config, &["rm", &id]).status.success());
}

#[test]
fn a_cancelled_no_detach_exec_stops_its_job_inside_its_group() {
    require_sshd!();
    use std::os::unix::process::CommandExt;
    let sshd = Sshd::start();
    let _master = sshd.open_master(&sshd.socket);
    let tmux = Tmux::new(&sshd, "no-detach");
    let config = sshd.write_config(&tmux.name);

    let mut exec = mule_command(&sshd, &config, &["exec", "--no-detach", "--", "sleep 60"])
        .process_group(0)
        .spawn()
        .unwrap();
    wait_until("the job to start", Duration::from_secs(15), || {
        !job_ids(&sshd).is_empty()
    });
    let id = job_ids(&sshd).remove(0);
    wait_until("its session", Duration::from_secs(15), || {
        tmux.has_session(&format!("mule-{id}"))
    });

    let sent = Instant::now();
    unsafe { libc::kill(-(exec.id() as i32), libc::SIGTERM) };
    let status = exec.wait().unwrap();

    assert_eq!(status.code(), Some(143));
    assert!(
        sent.elapsed() < Duration::from_secs(5),
        "{:?}",
        sent.elapsed()
    );
    // Gone when exec exits, with nothing left running to finish the job.
    assert!(job_ids(&sshd).is_empty(), "state dir left");
    assert!(!tmux.has_session(&format!("mule-{id}")), "session left");
    let ps = std::process::Command::new("ps")
        .args(["-ax", "-o", "args="])
        .output()
        .unwrap();
    assert!(
        !String::from_utf8_lossy(&ps.stdout)
            .lines()
            .any(|line| line.contains(&id) && line.contains("--retry-for")),
        "a detached stop child exists"
    );
    assert!(
        unsafe { libc::kill(-(exec.id() as i32), 0) } != 0,
        "something in exec's group outlived it"
    );
}
