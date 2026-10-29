use clap::{CommandFactory, Parser};

use mule::cli::Cli;
use mule::errors::{
    AgentState, EXIT_DROPPED, EXIT_NO_MASTER, EXIT_ORPHAN, EXIT_TIMEOUT, MuleError, classify,
    classify_attempt, exit_code,
};

/// Point the lock directory at a temp dir for the whole test binary.
///
/// Keep this test isolated even though its current cases do not take the lock;
/// additions should not accidentally write into the developer's live state.
fn isolate_state() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let dir = std::env::temp_dir().join(format!("mule-state-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        unsafe { std::env::set_var("XDG_STATE_HOME", &dir) };
    });
}

#[test]
fn refused_session_diagnostics_are_classified_case_insensitively() {
    isolate_state();
    for stderr in ["session request failed", "SESSION OPEN REFUSED"] {
        assert_eq!(
            classify(stderr, || panic!(
                "unambiguous errors must not probe the agent"
            )),
            Some(MuleError::SessionChannelBusy)
        );
    }
}

/// What OpenSSH 10's mux client logs when the master refuses a session.
const REFUSAL: &str =
    "mux_client_request_session: session request failed: Session open refused by peer\n";

#[test]
fn only_a_refusal_in_sshs_own_log_with_no_stdout_is_retryable() {
    isolate_state();
    let no_agent = || -> AgentState { panic!("a mux refusal must not probe the agent") };
    // (ssh -E log, remote stdout, expected)
    let table: [(&str, &[u8], Option<MuleError>); 6] = [
        // The one retryable shape: refused before the script ran.
        (REFUSAL, b"", Some(MuleError::MuxRefusedPreExec)),
        (
            "SESSION OPEN REFUSED",
            b"",
            Some(MuleError::MuxRefusedPreExec),
        ),
        // Something ran and printed: never re-run it, but still say busy.
        (REFUSAL, b"rc=0\n", Some(MuleError::SessionChannelBusy)),
        // A clean call: ssh's log empty, whatever the remote did.
        ("", b"", None),
        ("", b"exists=1\n", None),
        // ssh's own non-refusal noise is not a refusal.
        ("Connection closed by UNKNOWN port 65535\n", b"", None),
    ];
    for (log, stdout, expected) in table {
        assert_eq!(
            classify_attempt(log, stdout, no_agent),
            expected,
            "log={log:?} stdout={stdout:?}"
        );
    }
}

#[test]
fn keyboard_interactive_in_sshs_log_is_never_retryable() {
    isolate_state();
    for (state, expected) in [
        (AgentState::Keys, MuleError::SessionChannelBusy),
        (AgentState::NoKeys, MuleError::SshAgentHasNoKeys),
        (AgentState::Unreachable, MuleError::SshAgentUnreachable),
        (AgentState::Unknown, MuleError::KeyboardInteractiveAmbiguous),
    ] {
        assert_eq!(
            classify_attempt("Permission denied (keyboard-interactive)", b"", || state),
            Some(expected)
        );
    }
}

#[test]
fn keyboard_interactive_with_an_unreachable_agent_names_the_credential_problem() {
    isolate_state();
    let error = classify("Permission denied (keyboard-interactive)", || {
        AgentState::Unreachable
    })
    .unwrap();

    assert_eq!(error, MuleError::SshAgentUnreachable);
    let text = error.to_string();
    assert!(text.contains("ssh-add -l"), "{text}");
    assert!(text.contains("SSH_AUTH_SOCK"), "{text}");
    assert!(text.contains("prompt"), "{text}");
    assert!(text.contains("cannot see"), "{text}");
}

#[test]
fn keyboard_interactive_with_keys_is_likely_a_busy_channel() {
    isolate_state();
    assert_eq!(
        classify("Permission denied (keyboard-interactive)", || {
            AgentState::Keys
        }),
        Some(MuleError::SessionChannelBusy)
    );
}

#[test]
fn keyboard_interactive_with_no_keys_names_the_credential_problem() {
    isolate_state();
    let error = classify("Permission denied (keyboard-interactive)", || {
        AgentState::NoKeys
    })
    .unwrap();

    assert_eq!(error, MuleError::SshAgentHasNoKeys);
    assert!(error.to_string().contains("ssh-add"));
}

#[test]
fn keyboard_interactive_with_unknown_agent_state_reports_both_possibilities() {
    isolate_state();
    let text = classify("Permission denied (keyboard-interactive)", || {
        AgentState::Unknown
    })
    .unwrap()
    .to_string();

    assert!(text.contains("ssh-add -l"), "{text}");
    assert!(text.contains("ssh -O check"), "{text}");
    assert!(text.contains("either"), "{text}");
}

#[test]
fn genuine_auth_failure_is_not_classified_as_a_busy_channel() {
    isolate_state();
    assert_eq!(
        classify("Permission denied (publickey)", || {
            panic!("publickey failures must not probe the agent")
        }),
        None
    );
}

#[test]
fn no_master_names_the_exact_recovery_command() {
    isolate_state();
    let error = MuleError::NoMaster {
        host: "dev".into(),
        socket: "/tmp/mule-dev.sock".into(),
        target: "build.example".into(),
    };

    let text = error.to_string();
    assert_eq!(
        text,
        "no control master for dev\n  \
         run: ssh -MNf -S /tmp/mule-dev.sock -o ControlPersist=8h -o StreamLocalBindUnlink=yes build.example\n  \
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
            MuleError::NoMaster {
                host: "dev".into(),
                socket: "/tmp/mule.sock".into(),
                target: "dev.example".into(),
            },
            EXIT_NO_MASTER,
        ),
        (
            MuleError::Timeout {
                id: "abc123".into(),
            },
            EXIT_TIMEOUT,
        ),
        (
            MuleError::Orphan {
                id: "abc123".into(),
            },
            EXIT_ORPHAN,
        ),
        (
            MuleError::Dropped {
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
fn ls_all_and_running_are_mutually_exclusive() {
    let error = Cli::try_parse_from(["mule", "ls", "--all", "--running"])
        .expect_err("--all and --running answer opposite questions");
    let text = error.to_string();
    assert!(text.contains("cannot be used with"), "{text}");
    assert!(text.contains("--all"), "{text}");
    assert!(text.contains("--running"), "{text}");
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
        "stdout and stderr are merged into one log",
        "poll and wait print NO job output",
        "do NOT pipe your command into head or tail",
        "mule tail <id> -n 3",
        "mule tail <id>",
        "never lent to local",
        "commands like rsync or git fetch",
        "3   no ssh control master",
        "4   timed out waiting",
        "5   orphaned job",
        "6   connection dropped while waiting",
        "7   exec only: output incomplete -- truncated at max_log_bytes, or a process",
        "still holds the job's stdout",
        "<n> wait/--wait/--lease return the job's own exit code",
        "129/130/143  exec and run --lease after SIGHUP/SIGINT/SIGTERM",
        "run --lease exits 3 only at startup",
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
    let cfg = mule::config::Config::parse("[hosts.dev]\ntarget = \"h\"\n").unwrap();
    let host = cfg.host(None).unwrap();
    let down = mule::transport::Fake::no_master();
    let id = "abc123".parse().unwrap();
    let mut sink = Vec::new();

    let failures: Vec<anyhow::Error> = vec![
        mule::cli::poll(&down, host, &id, false).unwrap_err(),
        mule::cli::wait(&down, host, &id, None).unwrap_err(),
        mule::jobs::kill(&down, host, &id).unwrap_err(),
        mule::jobs::remove(&down, host, &mule::jobs::Target::One(id.clone())).unwrap_err(),
        mule::tail::once(
            &down,
            host,
            &id,
            mule::tail::Selection::LastBytes,
            &mut sink,
        )
        .unwrap_err(),
    ];

    for error in &failures {
        assert_eq!(
            mule::errors::exit_code(error),
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

#[test]
fn reporting_a_missing_master_prepares_the_socket_directory() {
    // ssh will NOT create the directory holding a control socket: it binds a
    // temporary name inside it and fails with "unix_listener: cannot bind to
    // path ...: No such file or directory". mule defaults the socket to
    // ~/.ssh/mule/<host>.sock and never created that directory, so the command
    // mule printed could not work -- and it failed AFTER the 2FA prompt, so the
    // user paid a hardware-token tap to discover mule's own advice was wrong.
    isolate_state();
    let base = std::env::temp_dir().join(format!("mule-sockdir-{}", std::process::id()));
    std::fs::remove_dir_all(&base).ok();
    let sock = base.join("nested").join("dev.sock");

    let cfg = mule::config::Config::parse(&format!(
        "[hosts.dev]\ntarget = \"h\"\nsocket = \"{}\"\n",
        sock.display()
    ))
    .unwrap();
    let host = cfg.host(None).unwrap();

    assert!(!sock.parent().unwrap().exists(), "precondition");

    let err = mule::errors::require_master(&mule::transport::Fake::no_master(), host).unwrap_err();
    assert_eq!(mule::errors::exit_code(&err), 3);

    let parent = sock.parent().unwrap();
    assert!(
        parent.is_dir(),
        "the socket directory must exist afterwards"
    );

    // 0700: ssh refuses a control socket in a directory others can write, so a
    // 0755 mkdir would trade one error for a subtler one.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(parent).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o700, "socket directory must be private");
    }

    // Idempotent: a second report must not fail on the existing directory.
    let again =
        mule::errors::require_master(&mule::transport::Fake::no_master(), host).unwrap_err();
    assert_eq!(mule::errors::exit_code(&again), 3);

    std::fs::remove_dir_all(&base).ok();
}

#[test]
fn the_master_command_is_rendered_in_one_place() {
    // `host list` and `ls` both print this advice; three copies would drift.
    isolate_state();
    let cfg = mule::config::Config::parse(
        "[hosts.dev]\ntarget = \"build.example\"\nsocket = \"/tmp/mule-render/dev.sock\"\n",
    )
    .unwrap();
    let rendered = mule::errors::master_command(cfg.host(None).unwrap());
    assert!(rendered.contains("ssh -MNf"), "{rendered}");
    assert!(rendered.contains("/tmp/mule-render/dev.sock"), "{rendered}");
    assert!(rendered.contains("build.example"), "{rendered}");
    assert!(rendered.contains("ControlPersist=8h"), "{rendered}");
    std::fs::remove_dir_all("/tmp/mule-render").ok();
}

#[test]
fn every_verb_that_touches_the_log_says_the_streams_are_merged() {
    // The top-level help is the page a reader sees once and forgets. The
    // sentence is reused VERBATIM rather than paraphrased per verb, so an
    // agent grepping any one of these gets the same answer -- and so three
    // copies cannot drift into three different claims.
    isolate_state();
    const MERGED: &str = "merged into one log, in the order the job wrote them";

    for verb in ["run", "tail", "wait", "poll"] {
        let help = Cli::command()
            .find_subcommand_mut(verb)
            .expect("verb exists")
            .render_long_help()
            .to_string();
        assert!(
            help.contains(MERGED),
            "`mule {verb} --help` must state the merging:\n{help}"
        );
        assert!(
            help.contains("redirect inside your command"),
            "`mule {verb} --help` must say how to separate them:\n{help}"
        );
    }
}

#[test]
fn run_help_warns_that_piping_the_command_destroys_the_exit_code() {
    // Observed from a real agent: `mule run 'lake build 2>&1 | tail -3 && ...'`.
    // rc becomes the pipe's, so a failed build reports 0 and the `&&` proceeds.
    // rc is the artifact the whole design rests on, so this is silent.
    isolate_state();
    let help = Cli::command()
        .find_subcommand_mut("run")
        .expect("run exists")
        .render_long_help()
        .to_string();

    for text in [
        "Do NOT pipe the command into head or tail",
        "exits 0",
        "mule tail <id> -n 3",
        "set -o pipefail",
        "not portable POSIX",
    ] {
        assert!(help.contains(text), "run help missing {text:?}:\n{help}");
    }
}

#[test]
fn exit_seven_help_names_both_causes_at_the_top_level_and_on_exec() {
    isolate_state();
    let top = Cli::command().render_long_help().to_string();
    let exec = Cli::command()
        .find_subcommand_mut("exec")
        .expect("exec exists")
        .render_long_help()
        .to_string();

    for (what, help) in [("mule --help", &top), ("mule exec --help", &exec)] {
        let flat = help.split_whitespace().collect::<Vec<_>>().join(" ");
        assert!(flat.contains("max_log_bytes"), "{what}:\n{help}");
        assert!(
            flat.contains("process") && flat.contains("stdout"),
            "{what} must say a leftover process holding stdout also gives exit 7:\n{help}"
        );
        assert!(flat.contains("incomplete"), "{what}:\n{help}");
    }
}
