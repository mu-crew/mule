//! `poll --json`'s five states, exit 0: layers 1 and 2.
//!
//! The mu plugins spec (D15) needs a status a caller can trust after its own
//! crash: `running`, `done`, `orphan`, `missing` -- proven the way `rm`
//! proves a stop -- and `unknown` for everything that cannot be proven. Before
//! this, `missing` was exit 1 (the same code as a refused session), and the
//! probe read "cannot look" as absence: `[ -d ]` is false behind an
//! unsearchable parent, and `has-session` exits 1 for a tmux protocol
//! mismatch, so a live job could read `missing` or `orphan`.
//!
//! Layer 1 pins the mapping over `Fake`; layer 2 runs the mule binary with an
//! `ssh` that executes the real scripts here, against a private tmux server.

mod common;

use common::local::{ShimHost, scratch, stderr, stdout, tmux_available, wait_until};
use mule::config::{Config, Host};
use mule::errors::{MuleError, exit_code};
use mule::probe::{From, State, probe};
use mule::remote::CANNOT_TELL;
use mule::transport::{Fake, Output};
use mule::wrapper::JobId;

fn host() -> Host {
    common::local::isolate_state();
    Config::parse("[hosts.status]\ntarget = \"h\"\n")
        .unwrap()
        .host(None)
        .unwrap()
        .clone()
}

fn id() -> JobId {
    "abc123".parse().unwrap()
}

fn probed(reply: &str) -> anyhow::Result<State> {
    let fake = Fake::new();
    fake.push(Output::ok(reply));
    probe(&fake, &host(), &id(), From::StateOnly).map(|p| p.state)
}

fn json(out: &std::process::Output) -> serde_json::Value {
    serde_json::from_slice(&out.stdout)
        .unwrap_or_else(|e| panic!("not json ({e}): {:?}", stdout(out)))
}

// -- layer 1 ------------------------------------------------------------------

#[test]
fn a_check_that_could_not_tell_is_unverifiable_never_missing_or_orphan() {
    let why = "cannot verify session state: protocol version mismatch";
    let fake = Fake::new();
    fake.push(Output {
        // The script may already have started writing a reply when a check fails.
        stdout: b"exists=1\n".to_vec(),
        ..Output::fail(CANNOT_TELL, format!("mule: cannot tell: {why}\n"))
    });
    let error = probe(&fake, &host(), &id(), From::StateOnly).unwrap_err();
    assert!(
        matches!(error.downcast_ref::<MuleError>(), Some(MuleError::Unverifiable { why: reason, .. }) if reason == why),
        "{error:#}"
    );
    assert_eq!(exit_code(&error), 1, "every verb but poll --json: exit 1");
}

#[test]
fn proven_facts_map_as_before_and_rc_decides_done() {
    for (reply, state) in [
        (
            "exists=0\nrc=\nalive=0\nwatch=0\nwhy=\nsize=0\nbytes:\n",
            State::Missing,
        ),
        (
            "exists=1\nrc=\nalive=0\nwatch=0\nwhy=\nsize=0\nbytes:\n",
            State::Orphan,
        ),
        (
            "exists=1\nrc=\nalive=1\nwatch=0\nwhy=\nsize=0\nbytes:\n",
            State::Running,
        ),
        // An orphan's leftover watchdog does not make it anything else.
        (
            "exists=1\nrc=\nalive=0\nwatch=1\nwhy=\nsize=0\nbytes:\n",
            State::Orphan,
        ),
        (
            "exists=1\nrc=4\nalive=0\nwatch=0\nwhy=\nsize=0\nbytes:\n",
            State::Done(4),
        ),
    ] {
        assert_eq!(probed(reply).unwrap(), state, "{reply:?}");
    }
}

#[test]
fn poll_json_answers_missing_and_unknown_with_exit_zero_and_keeps_exit_three() {
    for reply in [
        Output::ok("exists=0\nrc=\nalive=0\nwatch=0\nwhy=\nsize=0\nbytes:\n"),
        Output::fail(CANNOT_TELL, "mule: cannot tell: state dir is unreadable"),
        // A transport failure is "cannot tell" too, not "gone".
        Output::fail(255, "Connection to h closed by remote host."),
        Output::ok("garbage"),
    ] {
        let fake = Fake::new();
        fake.push(reply.clone());
        assert_eq!(
            mule::cli::poll(&fake, &host(), &id(), true).unwrap(),
            0,
            "{reply:?}"
        );
    }
    // Plain `poll` is unchanged: a missing job is an error, exit 1.
    let fake = Fake::new();
    fake.push(Output::ok(
        "exists=0\nrc=\nalive=0\nwatch=0\nwhy=\nsize=0\nbytes:\n",
    ));
    let error = mule::cli::poll(&fake, &host(), &id(), false).unwrap_err();
    assert_eq!(exit_code(&error), 1);
    // No master is the hand-back, never an answer.
    let error = mule::cli::poll(&Fake::no_master(), &host(), &id(), true).unwrap_err();
    assert_eq!(exit_code(&error), 3);
}

// -- layer 2: the binary over the real scripts ---------------------------------

fn skip() -> bool {
    if !tmux_available() {
        eprintln!("skipping: tmux is not on PATH");
        return true;
    }
    false
}

fn poll_json(host: &ShimHost, id: &str) -> (Option<i32>, serde_json::Value) {
    let out = host.mule(&["poll", id, "--json"]);
    (out.status.code(), json(&out))
}

#[test]
fn poll_json_reports_all_five_states_with_exit_zero() {
    if skip() {
        return;
    }
    let host = ShimHost::new("status-five");

    let running = stdout(&host.mule(&["run", "--quiet", "sleep 30"]));
    let (code, value) = poll_json(&host, &running);
    assert_eq!(code, Some(0));
    assert_eq!(value["state"], "running", "{value}");
    assert!(value["log_size"].is_u64(), "{value}");

    let done = stdout(&host.mule(&["run", "--quiet", "exit 4"]));
    wait_until("the job to finish", 10, || {
        host.jobs().join(&done).join("rc").exists()
    });
    let (code, value) = poll_json(&host, &done);
    assert_eq!(code, Some(0));
    assert_eq!(value["state"], "done", "{value}");
    assert_eq!(value["rc"], 4, "{value}");

    // Never dispatched: nothing on disk, no session. Exit 0, not 1.
    let (code, value) = poll_json(&host, "0a0a0a");
    assert_eq!(code, Some(0));
    assert_eq!(
        value,
        serde_json::json!({"state": "missing", "rc": null, "runtime_secs": null, "log_size": null})
    );

    // Removed by `rm`: missing, the same answer `rm`'s own proof gives.
    assert!(host.mule(&["rm", &done]).status.success());
    let (code, value) = poll_json(&host, &done);
    assert_eq!((code, value["state"].as_str()), (Some(0), Some("missing")));

    // The session dies without writing rc: an orphan, proven by tmux saying
    // there is no server at all. SIGKILL to the pane's process group, not a
    // hangup, which lets some shells (zsh) write rc 129 first and make the
    // job `done`.
    let pane = host
        .tmux
        .command(&[
            "list-panes",
            "-t",
            &format!("mule-{running}"),
            "-F",
            "#{pane_pid}",
        ])
        .output()
        .unwrap();
    let pane: i32 = stdout(&pane).parse().unwrap();
    unsafe { libc::kill(-pane, libc::SIGKILL) };
    wait_until("the session to go", 10, || {
        !host.tmux.has_session(&format!("mule-{running}"))
    });
    let (code, value) = poll_json(&host, &running);
    assert_eq!(
        (code, value["state"].as_str()),
        (Some(0), Some("orphan")),
        "{value}"
    );

    // Unknown: the jobs root cannot be searched, so absence cannot be seen.
    if let Some((code, value)) = with_unsearchable_jobs_root(&host, || poll_json(&host, &running)) {
        assert_eq!(code, Some(0));
        assert_eq!(value["state"], "unknown", "{value}");
        assert!(
            value["reason"]
                .as_str()
                .unwrap()
                .contains("cannot verify state dir"),
            "{value}"
        );
    }
}

/// `rc` lands before the log pipeline drains: a child the command left
/// holding stdout keeps writing after it. `drained` says when the log (and
/// a `truncated` marker) is final, so a caller never takes an early read
/// as the whole output.
#[test]
fn poll_json_says_when_a_done_job_has_drained() {
    if skip() {
        return;
    }
    let host = ShimHost::new("status-drain");
    let gate = host.dir.join("release");
    let id = stdout(&host.mule(&[
        "run",
        "--quiet",
        &format!(
            "echo early; (while [ ! -f '{}' ]; do sleep 0.05; done; echo late) & exit 7",
            gate.display()
        ),
    ]));
    wait_until("rc", 10, || host.jobs().join(&id).join("rc").exists());
    let (_, value) = poll_json(&host, &id);
    assert_eq!(
        (
            value["state"].as_str(),
            value["rc"].as_i64(),
            value["drained"].as_bool()
        ),
        (Some("done"), Some(7), Some(false)),
        "{value}"
    );
    std::fs::write(&gate, "").unwrap();
    wait_until("the drain", 10, || {
        poll_json(&host, &id).1["drained"] == serde_json::json!(true)
    });
    let log = std::fs::read_to_string(host.jobs().join(&id).join("log")).unwrap();
    assert_eq!(log, "early\nlate\n");
    // Only `done` has a drain to report.
    let running = stdout(&host.mule(&["run", "--quiet", "sleep 30"]));
    assert!(poll_json(&host, &running).1.get("drained").is_none());
    assert!(host.mule(&["kill", &running]).status.success());
}

/// Run `f` with the jobs root at mode 0000, or `None` if modes are not
/// enforced for this user (root).
fn with_unsearchable_jobs_root<T>(host: &ShimHost, f: impl FnOnce() -> T) -> Option<T> {
    use std::os::unix::fs::PermissionsExt;
    let root = host.jobs();
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o000)).unwrap();
    let enforced = std::fs::read_dir(&root).is_err();
    let got = enforced.then(f);
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700)).unwrap();
    if !enforced {
        eprintln!("skipping the unsearchable case: modes are not enforced for this user");
    }
    got
}

#[test]
fn a_job_behind_an_unsearchable_parent_is_unknown_not_missing() {
    // Before: `[ -d ]` was false, so poll said missing -- and a caller that
    // reads missing as "never started" starts the job again.
    if skip() {
        return;
    }
    let host = ShimHost::new("status-parent");
    let id = stdout(&host.mule(&["run", "--quiet", "sleep 30"]));

    let Some(outs) = with_unsearchable_jobs_root(&host, || {
        (
            host.mule(&["poll", &id, "--json"]),
            host.mule(&["poll", &id]),
            host.mule(&["wait", &id, "--timeout", "1"]),
        )
    }) else {
        return;
    };
    let (as_json, plain, wait) = outs;

    assert_eq!(as_json.status.code(), Some(0));
    assert_eq!(json(&as_json)["state"], "unknown");
    // Every other verb fails, as it always did on a probe it could not read;
    // none says the job is gone.
    for out in [&plain, &wait] {
        assert_ne!(out.status.code(), Some(0), "{}", stderr(out));
        assert!(!stderr(out).contains("not found"), "{}", stderr(out));
        assert!(
            stderr(out).contains("cannot tell the state"),
            "{}",
            stderr(out)
        );
    }
}

#[test]
fn a_tmux_that_cannot_answer_makes_a_live_job_unknown_not_orphan() {
    // `has-session` exits 1 for a client/server protocol mismatch just as for
    // a missing session. Read as absence, that called a running job an
    // orphan.
    if skip() {
        return;
    }
    use std::os::unix::fs::PermissionsExt;
    let host = ShimHost::new("status-mismatch");
    let id = stdout(&host.mule(&["run", "--quiet", "sleep 30"]));
    let bin = scratch("status-tmux-shim");
    std::fs::write(
        bin.join("tmux"),
        "#!/bin/sh\necho 'protocol version mismatch (client 8, server 7)' >&2\nexit 1\n",
    )
    .unwrap();
    std::fs::set_permissions(bin.join("tmux"), std::fs::Permissions::from_mode(0o755)).unwrap();

    let out = host
        .command(&["poll", &id, "--json"])
        .env("PATH", host.path_env(Some(&bin)))
        .output()
        .unwrap();

    assert_eq!(out.status.code(), Some(0), "{}", stderr(&out));
    let value = json(&out);
    assert_eq!(value["state"], "unknown", "{value}");
    assert!(
        value["reason"]
            .as_str()
            .unwrap()
            .contains("protocol version mismatch"),
        "{value}"
    );
    // The real server still has the job: the answer was right not to say
    // otherwise.
    assert!(host.tmux.has_session(&format!("mule-{id}")));
}

#[test]
fn a_state_dir_removed_under_a_live_session_is_unknown_not_missing() {
    if skip() {
        return;
    }
    let host = ShimHost::new("status-dirgone");
    let flag = host.dir.join("started");
    let id = stdout(&host.mule(&[
        "run",
        "--quiet",
        &format!("touch '{}'; sleep 30", flag.display()),
    ]));
    // Started for real first: moved before the job's shell opened `cmd`, the
    // job just fails, and then `missing` is the truth.
    wait_until("the job to start", 10, || flag.exists());
    // Moved away, not deleted: the job is still writing into it, and a
    // recursive delete races those writes (`Directory not empty`).
    std::fs::rename(host.jobs().join(&id), host.dir.join("moved-away")).unwrap();

    let (code, value) = poll_json(&host, &id);

    assert_eq!(code, Some(0));
    assert_eq!(value["state"], "unknown", "{value}");
    assert!(
        host.tmux.has_session(&format!("mule-{id}")),
        "it still runs"
    );
}

#[test]
fn poll_json_without_a_master_is_still_exit_three() {
    if skip() {
        return;
    }
    let host = ShimHost::new("status-nomaster");
    host.master_down(true);

    let out = host.mule(&["poll", "abc123", "--json"]);

    assert_eq!(out.status.code(), Some(3), "{}", stderr(&out));
    assert!(
        out.stdout.is_empty(),
        "no answer is printed for a hand-back"
    );
}

#[test]
fn ls_with_two_live_jobs_works_where_bsd_awk_refused_the_session_list() {
    // `awk -v live=<two lines>` is `awk: newline in string` on macOS, so
    // every `ls` failed once two jobs ran. The key lookup is an `ls`.
    if skip() {
        return;
    }
    let host = ShimHost::new("status-ls2");
    let a = stdout(&host.mule(&["run", "--quiet", "sleep 30"]));
    let b = stdout(&host.mule(&["run", "--quiet", "sleep 30"]));

    let out = host.mule(&["ls", "--json"]);

    assert!(out.status.success(), "{}", stderr(&out));
    let value = json(&out);
    let mut running: Vec<_> = value["items"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|row| row["state"] == "running")
        .map(|row| row["id"].as_str().unwrap().to_string())
        .collect();
    running.sort();
    let mut want = vec![a, b];
    want.sort();
    assert_eq!(running, want);
}

#[test]
fn a_server_that_exits_mid_request_is_observed_once_more_and_never_read_as_gone() {
    // `server exited unexpectedly` means the server went while the client
    // asked: no answer either way. Seen under a loaded suite, an `ls`
    // racing a fast job's end. One more look decides, and only a proof does.
    if skip() {
        return;
    }
    use std::os::unix::fs::PermissionsExt;
    let real = String::from_utf8(
        std::process::Command::new("sh")
            .args(["-c", "command -v tmux"])
            .output()
            .unwrap()
            .stdout,
    )
    .unwrap()
    .trim()
    .to_string();
    for (always, want) in [(false, "running"), (true, "unknown")] {
        let host = ShimHost::new("status-exited");
        let id = stdout(&host.mule(&["run", "--quiet", "sleep 30"]));
        let bin = scratch("status-exited-shim");
        let flag = bin.join("once");
        let gate = if always {
            String::new()
        } else {
            format!(
                "[ -e '{}' ] && exec '{real}' \"$@\"; : > '{}'\n",
                flag.display(),
                flag.display()
            )
        };
        std::fs::write(
            bin.join("tmux"),
            format!(
                "#!/bin/sh\ncase \"$*\" in *list-sessions*) {gate}echo 'server exited unexpectedly' >&2; exit 1;; esac\nexec '{real}' \"$@\"\n"
            ),
        )
        .unwrap();
        std::fs::set_permissions(bin.join("tmux"), std::fs::Permissions::from_mode(0o755)).unwrap();

        let out = host
            .command(&["poll", &id, "--json"])
            .env("PATH", host.path_env(Some(&bin)))
            .output()
            .unwrap();

        assert_eq!(
            json(&out)["state"],
            want,
            "always={always}: {}",
            stdout(&out)
        );
    }
}
