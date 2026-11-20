//! The `exec` dispatch size cap (SPEC § The dispatch size cap) and the orphan
//! exit code. Layer 2 for the binary (shim ssh, real scripts, no sshd).

mod common;

use common::local::{ShimHost, stderr, tmux_available};

/// Put a logging layer in front of the shim: one line per ssh call, `mux` for
/// `-O` and `session <bytes>` (the last argument, which is the remote script)
/// otherwise. Returns the log's path.
fn log_ssh(host: &ShimHost) -> std::path::PathBuf {
    use std::os::unix::fs::PermissionsExt;
    let ssh = host.dir.join("ssh");
    let real = host.dir.join("ssh-real");
    std::fs::rename(&ssh, &real).unwrap();
    let log = host.dir.join("ssh.log");
    std::fs::write(
        &ssh,
        format!(
            "#!/bin/sh\nfor arg; do if [ \"$arg\" = -O ]; then echo mux >> '{log}'; exec '{real}' \"$@\"; fi; done\n\
             for last; do :; done\n\
             echo \"session $(printf %s \"$last\" | wc -c | tr -d ' ')\" >> '{log}'\n\
             exec '{real}' \"$@\"\n",
            log = log.display(),
            real = real.display()
        ),
    )
    .unwrap();
    std::fs::set_permissions(&ssh, std::fs::Permissions::from_mode(0o755)).unwrap();
    log
}

/// `exec` args that put `bytes` of command into many small argv elements, so
/// the test never needs one argument past the OS's own per-string limit.
fn spread(bytes: usize) -> Vec<String> {
    let mut args = vec!["echo".to_owned()];
    args.extend((0..bytes / 1000).map(|_| "x".repeat(999)));
    args
}

fn exec_spread(host: &ShimHost, extra: &[&str], bytes: usize) -> std::process::Output {
    let spread = spread(bytes);
    let mut full = vec!["exec"];
    full.extend_from_slice(extra);
    full.push("--");
    full.extend(spread.iter().map(String::as_str));
    host.mule(&full)
}

#[test]
fn an_orphaned_job_exits_five() {
    if !tmux_available() {
        eprintln!("skipping: tmux is not on PATH");
        return;
    }
    let host = ShimHost::new("size-orphan");
    // The job's own session is killed from inside: a job directory with no
    // session and no `rc`.
    let kill = format!("tmux -L {} kill-server", host.tmux.socket);

    let out = host.mule(&["exec", "--", &kill]);

    assert_eq!(out.status.code(), Some(5), "{}", stderr(&out));
}

#[test]
fn the_script_ssh_receives_stays_within_the_limit_near_the_boundary() {
    if !tmux_available() {
        eprintln!("skipping: tmux is not on PATH");
        return;
    }
    let host = ShimHost::new("size-wire");
    let log = log_ssh(&host);
    // About 80KB of command is ~110KB of base64'd script plus the prune
    // prefix and session count: just under the limit.
    let out = exec_spread(&host, &[], 80_000);

    assert_eq!(out.status.code(), Some(0), "{}", stderr(&out));
    let sent: Vec<usize> = std::fs::read_to_string(&log)
        .unwrap()
        .lines()
        .filter_map(|l| l.strip_prefix("session ")?.parse().ok())
        .collect();
    let biggest = *sent.iter().max().unwrap();
    assert!(biggest <= mule::exec::MAX_DISPATCH_BYTES, "{biggest}");
    assert!(
        biggest > mule::exec::MAX_DISPATCH_BYTES - 8 * 1024,
        "not near: {biggest}"
    );

    // The same command a little bigger is refused, and the cutoff is on the
    // whole argument, not the inner wrapper the old check counted.
    let out = exec_spread(&host, &[], 86_000);
    assert_eq!(out.status.code(), Some(1), "{}", stderr(&out));
    assert!(
        stderr(&out).contains("command too large for one dispatch"),
        "{}",
        stderr(&out)
    );
}

#[test]
fn an_oversize_command_makes_no_ssh_call_at_all() {
    if !tmux_available() {
        eprintln!("skipping: tmux is not on PATH");
        return;
    }
    for extra in [&[][..], &["--no-detach"][..]] {
        let host = ShimHost::new("size-nossh");
        let log = log_ssh(&host);

        let out = exec_spread(&host, extra, 200 * 1024);

        assert_eq!(out.status.code(), Some(1), "{}", stderr(&out));
        assert!(
            stderr(&out).contains("command too large for one dispatch ("),
            "{}",
            stderr(&out)
        );
        assert!(!stderr(&out).contains("Broken pipe"));
        assert!(
            !log.exists(),
            "ssh was called: {:?}",
            std::fs::read_to_string(&log)
        );
        // A cleanup `mule rm` is detached: give one time to show itself.
        std::thread::sleep(std::time::Duration::from_millis(300));
        assert!(!log.exists(), "a cleanup ssh ran");
    }
}
