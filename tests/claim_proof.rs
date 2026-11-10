//! The claim proof path (P7a review round 3, ruling 3).
//!
//! A keyed lease that dies between its claim and its dispatch leaves a claim:
//! a state dir with a `key`, a `claim` record and no `cmd`. A later keyed
//! lease may roll that claim back and dispatch only on proof its claimer is
//! gone -- the same host, the pid absent or a different process -- and no
//! session exists. Otherwise the claim stays `unknown` and is held, with
//! `mule rm <id>` as the human path. Layer 2: the binary over an `ssh` that
//! runs the real scripts here.

mod common;

use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use common::local::{ShimHost, stderr, tmux_available};
use mule::claimer::{Claimer, local_host, proven_gone, start_time};

const CLAIM: &str = "0c1a10";

fn skip() -> bool {
    if !tmux_available() {
        eprintln!("skipping: tmux is not on PATH");
        return true;
    }
    false
}

fn claim_by(h: &ShimHost, claimer: &Claimer) {
    let d = h.jobs().join(CLAIM);
    std::fs::create_dir_all(&d).unwrap();
    std::fs::write(d.join("claim"), format!("{}\n", claimer.line())).unwrap();
    std::fs::write(d.join("key"), "mu.proof\n").unwrap();
}

fn dead_pid() -> u32 {
    let mut child = Command::new("true").spawn().unwrap();
    let pid = child.id();
    child.wait().unwrap();
    pid
}

fn jobs(h: &ShimHost) -> Vec<String> {
    let mut ids: Vec<String> = std::fs::read_dir(h.jobs())
        .map(|d| {
            d.filter_map(Result::ok)
                .map(|e| e.file_name().to_string_lossy().into_owned())
                .collect()
        })
        .unwrap_or_default();
    ids.sort();
    ids
}

fn lease(h: &ShimHost) -> std::process::Command {
    let mut command = h.command(&[
        "run",
        "--lease",
        "--key",
        "mu.proof",
        "--job-file",
        h.dir.join("w.job").to_str().unwrap(),
        "--",
        "exit 4",
    ]);
    command.stdin(Stdio::null());
    command
}

/// A lease that must hold the claim: it is still running after 3s, has
/// started nothing and named the claim. Killed with SIGKILL so its
/// signal-path stop does not remove the claim.
fn assert_held(h: &ShimHost) {
    let mut child = lease(h).spawn().unwrap();
    let deadline = Instant::now() + Duration::from_secs(3);
    while Instant::now() < deadline {
        assert!(
            child.try_wait().unwrap().is_none(),
            "the lease ended instead of holding the claim"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
    let _ = child.kill();
    let out = child.wait_with_output().unwrap();
    assert_eq!(jobs(h), vec![CLAIM.to_string()], "nothing else was started");
    assert!(
        !h.jobs().join(CLAIM).join("cmd").exists(),
        "the claim was not dispatched by this lease"
    );
    assert_eq!(
        std::fs::read_to_string(h.dir.join("w.job")).unwrap(),
        format!("{CLAIM}\n"),
        "the job file names the claim"
    );
    let err = stderr(&out);
    assert!(err.contains("not proven gone"), "{err}");
    assert!(
        err.contains(&format!("mule rm --host shim {CLAIM}")),
        "{err}"
    );
    let poll = h.mule(&["poll", CLAIM, "--json"]);
    let value: serde_json::Value = serde_json::from_slice(&poll.stdout).unwrap();
    assert_eq!(value["state"], "unknown", "{value}");
}

/// A lease that must roll the claim back and dispatch once: it ends with
/// the job's code, the claim is gone, and exactly one new job holds the key.
fn assert_rolled_back_and_dispatched_once(h: &ShimHost) {
    let out = lease(h).output().unwrap();
    assert_eq!(out.status.code(), Some(4), "{}", stderr(&out));
    assert!(
        stderr(&out).contains("rolling the claim back"),
        "{}",
        stderr(&out)
    );
    let now = jobs(h);
    assert!(
        !now.contains(&CLAIM.to_string()),
        "the claim was rolled back"
    );
    assert_eq!(now.len(), 1, "one job dispatched: {now:?}");
    let new = &now[0];
    assert_eq!(
        std::fs::read_to_string(h.jobs().join(new).join("key")).unwrap(),
        "mu.proof\n"
    );
    assert_eq!(
        std::fs::read_to_string(h.dir.join("w.job")).unwrap(),
        format!("{new}\n"),
        "the job file names the job that ran"
    );
    assert_eq!(
        std::fs::read_to_string(h.jobs().join(new).join("rc")).unwrap(),
        "4\n"
    );
}

#[test]
fn a_claim_whose_claimer_is_alive_is_held_as_unknown() {
    if skip() {
        return;
    }
    let h = ShimHost::new("proof-alive");
    let me = Claimer {
        pid: std::process::id(),
        host: local_host().unwrap(),
        start: start_time(std::process::id()).unwrap(),
    };
    assert!(!proven_gone(&me));
    claim_by(&h, &me);

    assert_held(&h);
}

#[test]
fn a_claim_whose_claimer_is_gone_on_this_host_is_rolled_back_and_dispatched_once() {
    if skip() {
        return;
    }
    let h = ShimHost::new("proof-gone");
    let gone = Claimer {
        pid: dead_pid(),
        host: local_host().unwrap(),
        start: "Thu Jan  1 00:00:00 1970".into(),
    };
    assert!(proven_gone(&gone));
    claim_by(&h, &gone);

    assert_rolled_back_and_dispatched_once(&h);
}

#[test]
fn a_claim_whose_pid_is_now_another_process_is_rolled_back() {
    // The pid lives, but its start time is not the claimer's: reused.
    if skip() {
        return;
    }
    let h = ShimHost::new("proof-reused");
    let reused = Claimer {
        pid: std::process::id(),
        host: local_host().unwrap(),
        start: "Thu Jan  1 00:00:00 1970".into(),
    };
    assert!(proven_gone(&reused));
    claim_by(&h, &reused);

    assert_rolled_back_and_dispatched_once(&h);
}

#[test]
fn a_claim_from_another_host_is_held_as_unknown() {
    // A dead pid here says nothing about a pid on another machine.
    if skip() {
        return;
    }
    let h = ShimHost::new("proof-elsewhere");
    let elsewhere = Claimer {
        pid: dead_pid(),
        host: "elsewhere.invalid".into(),
        start: "Thu Jan  1 00:00:00 1970".into(),
    };
    assert!(!proven_gone(&elsewhere));
    claim_by(&h, &elsewhere);

    assert_held(&h);
}

#[test]
fn a_gone_claimers_claim_with_a_session_is_never_rolled_back() {
    // "No session" is half the proof: a session named for the claim means
    // something of it runs, whoever made it.
    if skip() {
        return;
    }
    let h = ShimHost::new("proof-session");
    let gone = Claimer {
        pid: dead_pid(),
        host: local_host().unwrap(),
        start: "Thu Jan  1 00:00:00 1970".into(),
    };
    claim_by(&h, &gone);
    let made = h
        .tmux
        .command(&[
            "new-session",
            "-d",
            "-s",
            &format!("mule-{CLAIM}"),
            "sleep 60",
        ])
        .status()
        .unwrap();
    assert!(made.success());

    let mut child = lease(&h).spawn().unwrap();
    std::thread::sleep(Duration::from_secs(3));
    let _ = child.kill();
    let _ = child.wait();

    assert_eq!(jobs(&h), vec![CLAIM.to_string()]);
    assert!(h.tmux.has_session(&format!("mule-{CLAIM}")));
}

#[test]
fn an_unrecorded_claim_is_held_as_unknown() {
    // A claim with no `claim` record (an older mule's) proves nothing.
    if skip() {
        return;
    }
    let h = ShimHost::new("proof-unrecorded");
    let d = h.jobs().join(CLAIM);
    std::fs::create_dir_all(&d).unwrap();
    std::fs::write(d.join("key"), "mu.proof\n").unwrap();

    let mut child = lease(&h).spawn().unwrap();
    std::thread::sleep(Duration::from_secs(3));
    assert!(child.try_wait().unwrap().is_none(), "it holds");
    let _ = child.kill();
    let out = child.wait_with_output().unwrap();
    assert_eq!(jobs(&h), vec![CLAIM.to_string()]);
    assert!(
        stderr(&out).contains("an unrecorded claimer"),
        "{}",
        stderr(&out)
    );
}

#[test]
fn a_lease_records_its_claimer_with_its_claim() {
    if skip() {
        return;
    }
    let h = ShimHost::new("proof-record");
    let mut child = lease(&h).spawn().unwrap();
    // `exit 4` finishes at once; the claim dir is the job's.
    let _ = child.wait();
    let ids = jobs(&h);
    assert_eq!(ids.len(), 1, "{ids:?}");
    let record = std::fs::read_to_string(h.jobs().join(&ids[0]).join("claim")).unwrap();
    let claimer = Claimer::parse(&record).expect("a claim record");
    assert_eq!(claimer.host, local_host().unwrap());
    assert!(claimer.pid > 0);
    assert!(!claimer.start.is_empty());
}

#[test]
fn a_claim_record_round_trips() {
    let claimer = Claimer {
        pid: 42,
        host: "box.example".into(),
        start: "Mon Nov  9 18:35:24 2026".into(),
    };
    assert_eq!(Claimer::parse(&claimer.line()), Some(claimer.clone()));
    assert_eq!(
        Claimer::parse(&format!("{}\n", claimer.line())),
        Some(claimer)
    );
    for bad in [
        "",
        "pid=x host=h start=s",
        "pid=1 host= start=s",
        "pid=1 start=s",
    ] {
        assert_eq!(Claimer::parse(bad), None, "{bad:?}");
    }
}
