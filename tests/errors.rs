use clap::CommandFactory;

use coop::cli::Cli;
use coop::errors::{
    CoopError, EXIT_DROPPED, EXIT_NO_MASTER, EXIT_ORPHAN, EXIT_TIMEOUT, classify, exit_code,
};

/// Point the lock directory at a temp dir for the whole test binary.
///
/// Keep this test isolated even though its current cases do not take the lock;
/// additions should not accidentally write into the developer's live state.
fn isolate_state() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let dir = std::env::temp_dir().join(format!("coop-state-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        unsafe { std::env::set_var("XDG_STATE_HOME", &dir) };
    });
}

#[test]
fn refused_session_diagnostics_are_classified_case_insensitively() {
    isolate_state();
    for stderr in [
        "session request failed",
        "SESSION OPEN REFUSED",
        "Permission denied (keyboard-interactive)",
    ] {
        assert_eq!(classify(stderr), Some(CoopError::SessionChannelBusy));
    }
}

#[test]
fn genuine_auth_failure_is_not_classified_as_a_busy_channel() {
    isolate_state();
    assert_eq!(classify("Permission denied (publickey)"), None);
}

#[test]
fn no_master_names_the_exact_recovery_command() {
    isolate_state();
    let error = CoopError::NoMaster {
        host: "dev".into(),
        socket: "/tmp/coop-dev.sock".into(),
        target: "build.example".into(),
    };

    let text = error.to_string();
    assert_eq!(
        text,
        "no control master for dev\n  \
         run: ssh -MNf -S /tmp/coop-dev.sock -o ControlPersist=8h build.example\n  \
         a human may need to tap a hardware key; ask rather than retrying"
    );
    // The last line is for automated callers, which are the primary users: this
    // is the one failure no amount of retrying resolves, because it waits on a
    // physical act. An agent that retries instead of escalating hangs forever.
    assert!(text.contains("ask rather than retrying"), "{text}");
}

#[test]
fn typed_failures_have_stable_distinct_exit_codes() {
    isolate_state();
    let cases = [
        (
            CoopError::NoMaster {
                host: "dev".into(),
                socket: "/tmp/coop.sock".into(),
                target: "dev.example".into(),
            },
            EXIT_NO_MASTER,
        ),
        (
            CoopError::Timeout {
                id: "abc123".into(),
            },
            EXIT_TIMEOUT,
        ),
        (
            CoopError::Orphan {
                id: "abc123".into(),
            },
            EXIT_ORPHAN,
        ),
        (
            CoopError::Dropped {
                id: "abc123".into(),
            },
            EXIT_DROPPED,
        ),
    ];

    for (error, expected) in cases {
        assert_eq!(exit_code(&anyhow::Error::new(error)), expected);
    }
    assert_eq!(
        [EXIT_NO_MASTER, EXIT_TIMEOUT, EXIT_ORPHAN, EXIT_DROPPED],
        [3, 4, 5, 6]
    );
    assert_eq!(exit_code(&anyhow::anyhow!("unclassified")), 1);
}

#[test]
fn top_level_help_documents_the_operational_contract_and_exit_table() {
    isolate_state();
    let help = Cli::command().render_long_help().to_string();

    for text in [
        "does NOT open the ssh master",
        "one token tap per",
        "ControlPersist window",
        "NON-login, NON-interactive shell",
        "stdout and stderr are MERGED into one log",
        "poll and wait print NO job output",
        "coop tail <id>",
        "never lent to local",
        "commands like rsync or git fetch",
        "3   no ssh control master",
        "4   timed out waiting",
        "5   orphaned job",
        "6   connection dropped while waiting",
        "<n> wait/--wait return the job's own exit code",
    ] {
        assert!(help.contains(text), "help missing {text:?}:\n{help}");
    }
}

#[test]
fn every_job_verb_demands_a_master_with_exit_three() {
    // Previously only `run` checked. The rest fell through to ssh and reported
    // a generic failure with exit 1, instead of exit 3 and the one command that
    // fixes it. A caller cannot script around an error it cannot recognise.
    isolate_state();
    let cfg = coop::config::Config::parse("[hosts.dev]\ntarget = \"h\"\n").unwrap();
    let host = cfg.host(None).unwrap();
    let down = coop::transport::Fake::no_master();
    let id = "abc123".parse().unwrap();
    let mut sink = Vec::new();

    let failures: Vec<anyhow::Error> = vec![
        coop::cli::poll(&down, host, &id, false).unwrap_err(),
        coop::cli::wait(&down, host, &id, None).unwrap_err(),
        coop::jobs::kill(&down, host, &id).unwrap_err(),
        coop::jobs::rm(&down, host, &id).unwrap_err(),
        coop::tail::once(
            &down,
            host,
            &id,
            coop::tail::Selection::LastBytes,
            &mut sink,
        )
        .unwrap_err(),
    ];

    for error in &failures {
        assert_eq!(
            coop::errors::exit_code(error),
            3,
            "expected exit 3, got: {error:#}"
        );
        let text = format!("{error:#}");
        assert!(text.contains("ssh -MNf"), "must name the fix: {text}");
    }
    assert!(
        down.scripts().is_empty(),
        "a verb must not touch the channel when the master is down"
    );
}
