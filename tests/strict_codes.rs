//! `--strict-codes`: a command's own exit 3 is reported as 8, so 3 only ever
//! means mule's handback. Layer 2 for the binary (shim ssh, real scripts).

mod common;

use common::local::{ShimHost, stderr, stdout, tmux_available};

fn code(out: &std::process::Output) -> i32 {
    out.status.code().expect("exited by signal")
}

fn host(tag: &str) -> Option<ShimHost> {
    if !tmux_available() {
        eprintln!("skip: no tmux");
        return None;
    }
    Some(ShimHost::new(tag))
}

#[test]
fn default_exec_is_unchanged() {
    let Some(h) = host("sc-default") else { return };
    let out = h.mule(&["exec", "--", "echo hi; exit 3"]);
    assert_eq!(code(&out), 3);
    assert_eq!(stdout(&out), "hi");
    assert!(!stderr(&out).contains("remapped"), "{}", stderr(&out));
    h.master_down(true);
    let out = h.mule(&["exec", "--", "true"]);
    assert_eq!(code(&out), 3);
}

#[test]
fn strict_exec_remaps_command_3_and_names_it_first_on_stderr() {
    let Some(h) = host("sc-exec3") else { return };
    let out = h.mule(&[
        "exec",
        "--strict-codes",
        "--",
        "echo hi; echo oops >&2; exit 3",
    ]);
    assert_eq!(code(&out), 8);
    assert_eq!(stdout(&out), "hi");
    let err = stderr(&out);
    assert_eq!(
        err.lines().next(),
        Some("mule: command exited 3 (remapped to 8)"),
        "{err}"
    );
    assert!(err.contains("oops"), "{err}");
}

#[test]
fn strict_exec_leaves_other_codes_alone() {
    let Some(h) = host("sc-exec-other") else {
        return;
    };
    for (cmd, want) in [("true", 0), ("exit 1", 1), ("exit 7", 7), ("exit 8", 8)] {
        let out = h.mule(&["exec", "--strict-codes", "--", cmd]);
        assert_eq!(code(&out), want, "{cmd}");
        assert!(
            !stderr(&out).contains("remapped"),
            "{cmd}: {}",
            stderr(&out)
        );
    }
}

#[test]
fn strict_exec_no_master_is_still_3() {
    let Some(h) = host("sc-exec-nomaster") else {
        return;
    };
    h.master_down(true);
    let out = h.mule(&["exec", "--strict-codes", "--", "exit 3"]);
    assert_eq!(code(&out), 3);
    let err = stderr(&out);
    assert!(err.contains("no control master"), "{err}");
    assert!(!err.contains("remapped"), "{err}");
}

#[test]
fn run_wait_and_wait_remap_only_under_the_flag() {
    let Some(h) = host("sc-wait") else { return };
    let out = h.mule(&["--quiet", "run", "--wait", "exit 3"]);
    assert_eq!(code(&out), 3);
    let out = h.mule(&["--quiet", "run", "--wait", "--strict-codes", "exit 3"]);
    assert_eq!(code(&out), 8);
    assert_eq!(
        stderr(&out).lines().next(),
        Some("mule: command exited 3 (remapped to 8)")
    );
    let out = h.mule(&["--quiet", "run", "--wait", "--strict-codes", "exit 5"]);
    assert_eq!(code(&out), 5);

    let id = stdout(&h.mule(&["--quiet", "run", "exit 3"]));
    let out = h.mule(&["wait", &id]);
    assert_eq!(code(&out), 3);
    let out = h.mule(&["wait", "--strict-codes", &id]);
    assert_eq!(code(&out), 8);
    assert_eq!(
        stderr(&out).lines().next(),
        Some("mule: command exited 3 (remapped to 8)")
    );
}

#[test]
fn strict_wait_no_master_is_still_3() {
    let Some(h) = host("sc-wait-nomaster") else {
        return;
    };
    let id = stdout(&h.mule(&["--quiet", "run", "exit 3"]));
    h.master_down(true);
    let out = h.mule(&["wait", "--strict-codes", &id]);
    assert_eq!(code(&out), 3);
    assert!(!stderr(&out).contains("remapped"));
}

#[test]
fn strict_codes_needs_wait_on_run() {
    let Some(h) = host("sc-run-requires") else {
        return;
    };
    let out = h.mule(&["run", "--strict-codes", "true"]);
    assert_eq!(code(&out), 2);
}
