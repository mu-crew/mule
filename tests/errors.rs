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

    assert_eq!(
        error.to_string(),
        "no control master for dev\n  run: ssh -MNf -S /tmp/coop-dev.sock -o ControlPersist=8h build.example"
    );
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
