//! Regressions from the review of the plugin surface (mu plugins spec D15),
//! each driven through the binary with an `ssh` that runs the real scripts
//! here (layer 2). The rule they pin: a failure to read or search remote
//! state is `unknown` (or a failed listing), never `missing` or absent;
//! absence is proven, as `rm` proves it. Keys compare as strings, a keyed
//! lease that loses a race holds the winner, and a keyed job stays out of
//! age prune until `rm`.

mod common;

use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::time::Duration;

use common::local::{ShimHost, scratch, stderr, stdout, tmux_available, wait_until};

fn skip() -> bool {
    if !tmux_available() {
        eprintln!("skipping: tmux is not on PATH");
        return true;
    }
    false
}

fn json(out: &std::process::Output) -> serde_json::Value {
    serde_json::from_slice(&out.stdout)
        .unwrap_or_else(|e| panic!("not json ({e}): {:?} / {}", stdout(out), stderr(out)))
}

/// A directory holding one executable `name` with `body`.
fn tool(name: &str, body: &str) -> PathBuf {
    let dir = scratch("tool");
    let path = dir.join(name);
    std::fs::write(&path, body).unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
    dir
}

fn with_tool(host: &ShimHost, dir: &Path, args: &[&str]) -> std::process::Output {
    host.command(args)
        .env("PATH", host.path_env(Some(dir)))
        .output()
        .unwrap()
}

fn job_ids(host: &ShimHost) -> Vec<String> {
    let mut ids: Vec<String> = std::fs::read_dir(host.jobs())
        .map(|dir| {
            dir.filter_map(Result::ok)
                .map(|e| e.file_name().to_string_lossy().into_owned())
                .collect()
        })
        .unwrap_or_default();
    ids.sort();
    ids
}

// -- critical: a failed grep is not a proven absence -------------------------

#[test]
fn a_failing_grep_never_proves_a_session_gone() {
    // Review repro `grep`: with grep exiting 2, poll said `missing` while
    // `watch-abc123` was alive, and `rm` "proved" it gone.
    if skip() {
        return;
    }
    let host = ShimHost::new("rg-grep");
    // A second session keeps the server up, so `rm`'s list-sessions answers
    // after it kills `watch-abc123` and its grep has to be consulted.
    for name in ["keeper", "watch-abc123"] {
        let made = host
            .tmux
            .command(&["new-session", "-d", "-s", name, "sleep 30"])
            .status()
            .unwrap();
        assert!(made.success());
    }
    let grep = tool(
        "grep",
        "#!/bin/sh\necho 'grep: transient read failure' >&2\nexit 2\n",
    );

    let poll = with_tool(&host, &grep, &["poll", "abc123", "--json"]);
    assert!(host.tmux.has_session("watch-abc123"));
    let rm = with_tool(&host, &grep, &["rm", "abc123"]);

    assert_eq!(poll.status.code(), Some(0), "{}", stderr(&poll));
    assert_eq!(json(&poll)["state"], "unknown", "{}", stdout(&poll));
    assert_eq!(
        rm.status.code(),
        Some(1),
        "rm must not prove a stop it could not check: {}",
        stderr(&rm)
    );
}

#[test]
fn a_failing_grep_never_lets_prune_delete_a_live_jobs_state() {
    // Prune's orphan pass spares a dir whose `mule-<id>` is live, by grep;
    // a grep that fails must spare it too, not delete it.
    if skip() {
        return;
    }
    let host = ShimHost::new("rg-prune-grep");
    let id = stdout(&host.mule(&["run", "--quiet", "sleep 60"]));
    // Older than the orphan horizon (4 x 14 days).
    let old = std::process::Command::new("touch")
        .args(["-t", "202001010000"])
        .arg(host.jobs().join(&id))
        .status()
        .unwrap();
    assert!(old.success());
    // Only prune's live-session check fails; the dispatch's own count is
    // advisory and uses another pattern.
    let grep = tool(
        "grep",
        "#!/bin/sh\ncase \"$*\" in *-qx*) exit 2;; esac\nexec /usr/bin/grep \"$@\"\n",
    );

    let trigger = with_tool(&host, &grep, &["run", "--quiet", "true"]);

    assert!(trigger.status.success(), "{}", stderr(&trigger));
    assert!(
        host.jobs().join(&id).exists(),
        "a running job's state was pruned on a failed grep"
    );
}

// -- critical: an unreadable key is not an unkeyed row --------------------------

#[test]
fn an_unreadable_key_makes_a_prefix_lookup_fail_not_come_back_empty() {
    // Review repro `unreadable`: `ls --json --key-prefix mu.` exited 0 with
    // no items, an authoritative "no such job" for a job that exists.
    if skip() {
        return;
    }
    let host = ShimHost::new("rg-unreadable");
    let id = stdout(&host.mule(&["run", "--quiet", "--key", "mu.unreadable", "sleep 30"]));
    let key = host.jobs().join(&id).join("key");
    std::fs::set_permissions(&key, std::fs::Permissions::from_mode(0o000)).unwrap();
    if std::fs::read(&key).is_ok() {
        eprintln!("skipping: modes are not enforced for this user");
        return;
    }

    let found = host.mule(&["ls", "--json", "--key-prefix", "mu."]);
    let plain = host.mule(&["ls", "--json"]);
    let again = host.mule(&["run", "--quiet", "--key", "mu.unreadable", "sleep 30"]);

    std::fs::set_permissions(&key, std::fs::Permissions::from_mode(0o600)).unwrap();
    assert_ne!(found.status.code(), Some(0), "{}", stdout(&found));
    assert!(found.stdout.is_empty(), "no answer: {}", stdout(&found));
    assert!(
        stderr(&found).contains(&id),
        "names the job: {}",
        stderr(&found)
    );
    // Plain ls still lists it, as unknown rather than as unkeyed.
    assert!(plain.status.success(), "{}", stderr(&plain));
    let rows = json(&plain)["items"].as_array().unwrap().clone();
    let row = rows
        .iter()
        .find(|r| r["id"] == id.as_str())
        .expect("listed");
    assert_eq!(row["state"], "unknown", "{row}");
    // And a keyed dispatch refuses rather than start a second job.
    assert_ne!(again.status.code(), Some(0), "{}", stdout(&again));
    assert_eq!(job_ids(&host), vec![id]);
}

#[test]
fn an_unreadable_rc_is_unknown_never_an_orphan() {
    if skip() {
        return;
    }
    let host = ShimHost::new("rg-rc");
    let id = stdout(&host.mule(&["run", "--quiet", "exit 3"]));
    let rc = host.jobs().join(&id).join("rc");
    wait_until("rc", 10, || rc.exists());
    wait_until("the session to end", 10, || {
        !host.tmux.has_session(&format!("mule-{id}"))
    });
    std::fs::set_permissions(&rc, std::fs::Permissions::from_mode(0o000)).unwrap();
    if std::fs::read(&rc).is_ok() {
        eprintln!("skipping: modes are not enforced for this user");
        return;
    }

    let poll = host.mule(&["poll", &id, "--json"]);
    let ls = host.mule(&["ls", "--json"]);

    std::fs::set_permissions(&rc, std::fs::Permissions::from_mode(0o600)).unwrap();
    assert_eq!(json(&poll)["state"], "unknown", "{}", stdout(&poll));
    let rows = json(&ls)["items"].as_array().unwrap().clone();
    let row = rows
        .iter()
        .find(|r| r["id"] == id.as_str())
        .expect("listed");
    assert_eq!(row["state"], "unknown", "{row}");
}

// -- high: keys compare as strings ---------------------------------------------

#[test]
fn keys_that_look_numeric_compare_as_strings() {
    // Review repro `numeric`: awk compared `01` and `1` as numbers, so the
    // second caller's command never ran.
    if skip() {
        return;
    }
    let host = ShimHost::new("rg-numeric");
    for (a, b) in [("01", "1"), ("2.0", "2.00"), ("3e0", "3")] {
        let first = stdout(&host.mule(&["run", "--quiet", "--key", a, "sleep 30"]));
        let second = host.mule(&["run", "--quiet", "--key", b, "sleep 30"]);
        assert!(second.status.success(), "{}", stderr(&second));
        assert_ne!(stdout(&second), first, "{a} and {b} are different keys");
        assert!(
            !stderr(&second).contains("already names"),
            "{}",
            stderr(&second)
        );
    }
}

// -- high: a keyed lease that loses a race holds the winner ----------------------

#[test]
fn two_racing_keyed_leases_both_hold_the_one_job_and_publish_only_its_id() {
    // Review repro `lease`: with each lookup delayed, the loser exited 1
    // and its job file named an id that was never dispatched.
    if skip() {
        return;
    }
    let host = ShimHost::new("rg-lease");
    let shim = host.dir.join("ssh");
    let slow = std::fs::read_to_string(&shim).unwrap().replace(
        "for last; do :; done\n",
        "for last; do :; done\ncase \"$last\" in *new-session*) ;; *'awk -v k='*) sleep 0.3;; esac\n",
    );
    std::fs::write(&shim, slow).unwrap();
    let files = [host.dir.join("job0"), host.dir.join("job1")];

    let children: Vec<_> = files
        .iter()
        .map(|file| {
            host.command(&[
                "run",
                "--lease",
                "--key",
                "mu.race",
                "--job-file",
                file.to_str().unwrap(),
                "--",
                "sleep 2; exit 4",
            ])
            .spawn()
            .unwrap()
        })
        .collect();
    let outs: Vec<_> = children
        .into_iter()
        .map(|c| c.wait_with_output().unwrap())
        .collect();

    let ids = job_ids(&host);
    let errors: Vec<_> = outs.iter().map(stderr).collect();
    assert_eq!(ids.len(), 1, "one job: {ids:?}; {errors:?}");
    for (out, file) in outs.iter().zip(&files) {
        assert_eq!(out.status.code(), Some(4), "{}", stderr(out));
        assert_eq!(
            std::fs::read_to_string(file).unwrap().trim(),
            ids[0],
            "the job file names the job holding the key"
        );
    }
}

// -- medium: the five-state test's orphan, under every pane shell ---------------

#[test]
fn a_killed_pane_is_an_orphan_under_sh_zsh_and_dash() {
    // The review saw `done 137` where the five-state test expected an
    // orphan. Reproduced at f06426c with the pane shell `/bin/dash`: a
    // shell recorded its child's SIGKILL. 49df188's wrapper records rc only
    // while its session lives.
    if skip() {
        return;
    }
    for shell in ["/bin/sh", "/bin/zsh", "/bin/dash"] {
        if !Path::new(shell).exists() {
            continue;
        }
        for round in 0..3 {
            let host = ShimHost::new("rg-orphan");
            let out = host
                .command(&["run", "--quiet", "sleep 30"])
                .env("SHELL", shell)
                .output()
                .unwrap();
            let id = stdout(&out);
            wait_until("the session", 10, || {
                host.tmux.has_session(&format!("mule-{id}"))
            });
            std::thread::sleep(Duration::from_millis(300));
            let pane = host
                .tmux
                .command(&[
                    "list-panes",
                    "-t",
                    &format!("mule-{id}"),
                    "-F",
                    "#{pane_pid}",
                ])
                .output()
                .unwrap();
            let pane: i32 = stdout(&pane).parse().unwrap();
            unsafe { libc::kill(-pane, libc::SIGKILL) };
            wait_until("the session to go", 10, || {
                !host.tmux.has_session(&format!("mule-{id}"))
            });
            std::thread::sleep(Duration::from_millis(300));

            let poll = host.mule(&["poll", &id, "--json"]);
            assert_eq!(
                json(&poll)["state"],
                "orphan",
                "{shell} round {round}: {}",
                stdout(&poll)
            );
        }
    }
}

// -- medium: a keyed job stays out of age prune ---------------------------------

#[test]
fn a_keyed_job_is_kept_out_of_age_prune_until_rm() {
    // Review repro `prune`: a finished keyed job aged past keep_days was
    // pruned by the next dispatch, and the same key ran its command again.
    if skip() {
        return;
    }
    let host = ShimHost::new("rg-prune-key");
    let id = stdout(&host.mule(&["run", "--quiet", "--key", "mu.old", "true"]));
    let dir = host.jobs().join(&id);
    wait_until("rc", 10, || dir.join("rc").exists());
    wait_until("the session to end", 10, || {
        !host.tmux.has_session(&format!("mule-{id}"))
    });
    let plain = stdout(&host.mule(&["run", "--quiet", "true"]));
    wait_until("rc", 10, || host.jobs().join(&plain).join("rc").exists());
    // Its log pipeline touches the dir until the session ends.
    wait_until("the session to end", 10, || {
        !host.tmux.has_session(&format!("mule-{plain}"))
    });
    for job in [&id, &plain] {
        let aged = std::process::Command::new("touch")
            .args(["-t", "202001010000"])
            .arg(host.jobs().join(job))
            .status()
            .unwrap();
        assert!(aged.success());
    }

    let again = host.mule(&["run", "--quiet", "--key", "mu.old", "sleep 30"]);

    assert!(again.status.success(), "{}", stderr(&again));
    assert_eq!(stdout(&again), id, "the key still returns its job");
    assert!(dir.join("key").exists());
    assert!(
        !host.jobs().join(&plain).exists(),
        "unkeyed jobs still age out"
    );
    // `rm` is what frees the key.
    assert!(host.mule(&["rm", &id]).status.success());
    let fresh = stdout(&host.mule(&["run", "--quiet", "--key", "mu.old", "true"]));
    assert_ne!(fresh, id);
}

// -- low: SPEC describes the lookup the code runs --------------------------------

#[test]
fn spec_describes_the_key_lookup_the_code_runs() {
    let spec = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/SPEC.md")).unwrap();
    let code = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/src/run.rs")).unwrap();
    let section = spec
        .split("### Caller keys")
        .nth(1)
        .and_then(|rest| rest.split("\n## ").next())
        .expect("SPEC has a Caller keys section");
    assert!(!code.contains("grep -Flx"), "the lookup is not grep");
    for stale in [
        "grep -Flx",
        "grep -Fx` matches",
        "grep's exit 2",
        "(or prune)",
    ] {
        assert!(
            !section.contains(stale),
            "SPEC § Caller keys still says {stale:?}, which the code no longer does"
        );
    }
    assert!(
        section.contains("awk"),
        "SPEC § Caller keys names the awk lookup"
    );
    assert!(
        section.contains("prune"),
        "SPEC § Caller keys says what prune does to a key"
    );
}
