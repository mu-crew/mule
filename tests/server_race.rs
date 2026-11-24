//! A dispatch that meets a tmux server on its way out (layer 2).
//!
//! A tmux server exits with its last session. A `new-session` client that
//! connects in the moment between that session's end and the server's exit
//! is never accepted: the server closes it with no reply, and tmux prints
//! `server exited unexpectedly`. Measured with a stock tmux, a session of
//! `sleep 0.02` and a second `new-session` 19ms later: 30 of 300 rounds.
//! Under mule that is a job ending while another dispatches -- Pi's parallel
//! tools over one master failed on it in CI (`pi/loopback.test.ts`, "Pi's
//! tools in parallel over one capped master").
//!
//! The race cannot be timed reliably, so a `tmux` on PATH stands in for the
//! dying server: its first job `new-session` answers exactly as tmux does,
//! then it hands over to the real tmux.

mod common;

use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

use common::local::{ShimHost, scratch, stderr, stdout, tmux_available, wait_until, write_exec};

/// A `tmux` that loses its first `new-session -s mule-*` as a dying server
/// would, leaving `lost` behind, and is the real tmux for everything else.
fn dying_server(lost: &Path) -> PathBuf {
    let real = std::process::Command::new("sh")
        .args(["-c", "command -v tmux"])
        .output()
        .unwrap();
    let real = String::from_utf8(real.stdout).unwrap().trim().to_string();
    let dir = scratch("dying-tmux");
    write_exec(
        dir.join("tmux"),
        format!(
            "#!/bin/sh\n\
             case \"$*\" in *'new-session -d -s mule-'*)\n\
               if [ ! -e '{lost}' ]; then : > '{lost}'; echo 'server exited unexpectedly' >&2; exit 1; fi;;\n\
             esac\n\
             exec '{real}' \"$@\"\n",
            lost = lost.display(),
        ),
    )
    .unwrap();
    std::fs::set_permissions(dir.join("tmux"), std::fs::Permissions::from_mode(0o755)).unwrap();
    dir
}

fn dispatch_survives_a_dying_server(tag: &str, mode: &[&str]) {
    if !tmux_available() {
        eprintln!("skipping: tmux is not on PATH");
        return;
    }
    let host = ShimHost::new(tag);
    let lost = host.dir.join("lost");
    let tools = dying_server(&lost);
    let cwd = host.dir.to_str().unwrap();
    let mut args = vec!["run", "--quiet", "--cwd", cwd];
    args.extend_from_slice(mode);
    args.push("echo ran >> count; echo ran");
    let out = host
        .command(&args)
        .env("PATH", host.path_env(Some(&tools)))
        .output()
        .unwrap();

    assert!(lost.exists(), "the first new-session met the dying server");
    assert!(out.status.success(), "dispatch failed: {}", stderr(&out));
    let id = stdout(&out);
    let rc = host.jobs().join(&id).join("rc");
    wait_until("the job's rc", 10, || rc.exists());
    assert_eq!(std::fs::read_to_string(&rc).unwrap().trim(), "0");
    // The lost attempt never reached a server, so the job ran exactly once.
    let ran = std::fs::read_to_string(host.dir.join("count")).unwrap();
    assert_eq!(ran, "ran\n");
}

#[test]
fn a_detached_dispatch_retries_a_server_that_exited_under_it() {
    dispatch_survives_a_dying_server("race-detached", &[]);
}

#[test]
fn a_tui_dispatch_retries_a_server_that_exited_under_it() {
    dispatch_survives_a_dying_server("race-tui", &["--tui"]);
}
