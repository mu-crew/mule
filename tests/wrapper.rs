use std::collections::HashSet;
use std::path::PathBuf;

use coop::config::Host;
use coop::wrapper::{Job, dispatch_script, new_id, state_dir};

fn host() -> Host {
    Host {
        name: "dev".into(),
        target: "dev".into(),
        socket: PathBuf::from("/tmp/coop.sock"),
        tmux_socket: "coop".into(),
        max_running: 4,
        default_cwd: None,
        keep_days: 14,
        max_log_bytes: 100 * 1024 * 1024,
        max_job_secs: 0,
    }
}

#[test]
fn default_jobs_root_stays_in_the_remote_home() {
    assert_eq!(
        coop::wrapper::JOBS_ROOT,
        "${XDG_STATE_HOME:-$HOME/.local/state}/coop/jobs"
    );
}

#[test]
fn dispatch_uses_canonical_padded_base64() {
    let cases: &[(&[u8], &str)] = &[
        (b"", ""),
        (b"a", "YQ=="),
        (b"ab", "YWI="),
        (b"abc", "YWJj"),
        (b"line one\nline two\tend", "bGluZSBvbmUKbGluZSB0d28JZW5k"),
        (&[0, 0xff, 0x80], "AP+A"),
    ];

    for (input, expected) in cases {
        assert_eq!(coop::wrapper::encode_command(input), *expected);
    }
}

#[test]
fn dispatch_encodes_the_command_in_a_detached_tmux_job() {
    let command = r#"printf '%s "quoted"' "$HOME/a b""#;
    let job = Job {
        id: "a1b2c3".parse().unwrap(),
        cmd: command.into(),
        cwd: None,
        max_secs: 0,
    };

    let script = dispatch_script(&host(), &job);

    assert!(script.contains("tmux -L coop -f /dev/null new-session -d -s coop-a1b2c3"));
    // `-f /dev/null` is not cosmetic: a cold server that sources the user's
    // ~/.tmux.conf took 4.5s to start against 0.03s with an empty config,
    // measured. Every job pays it, and status hooks that shell out are the
    // usual cause.
    assert!(
        script.contains("-f /dev/null"),
        "must not read the user's tmux.conf"
    );
    assert!(!script.contains(command));
    assert_eq!(script.matches('\'').count() % 2, 0);
    // Under `jobs/`, not the state dir root: the ticket lock keeps
    // `<host>.lock` in that tree, and sharing one parent made `coop ls` report
    // `dev.lock` as an orphaned job.
    assert_eq!(
        state_dir(&job.id),
        "${XDG_STATE_HOME:-$HOME/.local/state}/coop/jobs/a1b2c3"
    );
}

#[test]
fn generated_ids_are_short_lowercase_hex_and_effectively_unique() {
    let ids = (0..1000).map(|_| new_id()).collect::<Vec<_>>();

    assert!(ids.iter().all(|id| {
        id.len() == 6
            && id
                .bytes()
                .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
    }));
    assert!(ids.iter().collect::<HashSet<_>>().len() > 990);
}

#[test]
fn a_cwd_with_a_space_is_quoted() {
    // `--cwd` is user input and lands in the tmux argument unquoted, so a path
    // with a space splits into two words and `cd` runs somewhere else -- or
    // succeeds against the wrong directory. The command itself is base64'd and
    // safe; this was the one interpolation left.
    let job = Job {
        id: "abc123".parse().unwrap(),
        cmd: "echo hi".into(),
        cwd: Some("/tmp/my dir".into()),
        max_secs: 0,
    };
    let script = dispatch_script(&host(), &job);
    assert!(
        !script.contains("cd /tmp/my dir &&"),
        "cwd must not be interpolated raw: {script}"
    );
}

#[test]
fn the_truncation_check_cannot_become_the_jobs_exit_code() {
    // `[ -s .overflow ] && echo 1 > truncated` is the last command in the
    // pipeline's right-hand side, so with an empty overflow file it exits 1 --
    // and that became the exit status of the whole wrapper. Every successful
    // job reported `rc 1`, `coop run true` included. An `if` form has no such
    // result.
    let script = dispatch_script(
        &host(),
        &Job {
            id: "abc123".parse().unwrap(),
            cmd: "true".into(),
            cwd: None,
            max_secs: 0,
        },
    );
    assert!(
        script.contains("if [ -s"),
        "the truncation check must not be a trailing && test: {script}"
    );
    assert!(
        !script.contains("] && echo 1 >"),
        "a trailing && test leaks its status into the job's rc: {script}"
    );
}

#[test]
fn every_spelling_of_the_home_directory_expands() {
    // The cwd is encoded so a space or metacharacter is inert -- but encoding
    // also stops `~` and `$HOME` expanding, and only the REMOTE shell knows the
    // remote home. Previously only the exact string `$HOME` was handled, so
    // every other spelling became a literal directory name that cannot exist:
    // `cd` failed, the `&&` short-circuited, and the job reported rc 1 with an
    // empty log. Measured broken: `~/`, `~/work`, `$HOME/work`, `${HOME}/work`.
    for cwd in ["~", "~/", "$HOME", "${HOME}", "${HOME}/"] {
        let script = script_for(cwd);
        assert!(
            script.contains("cd \"$HOME\""),
            "cwd {cwd:?} must cd to the remote home: {script}"
        );
    }

    // A sub-path keeps `$HOME` unquoted (so the remote shell expands it) while
    // the remainder stays encoded (so a space cannot split it). Both properties
    // have to hold together -- an earlier fix for one broke the other.
    for cwd in ["~/sub dir", "$HOME/sub dir", "${HOME}/sub dir"] {
        let script = script_for(cwd);
        assert!(
            script.contains("cd \"$HOME/$(printf %s"),
            "cwd {cwd:?} must expand $HOME and encode the rest: {script}"
        );
        assert!(
            !script.contains("sub dir"),
            "cwd {cwd:?} must not interpolate the remainder raw: {script}"
        );
    }

    // A quoted tilde or literal $HOME never expands, so neither may survive
    // into the script.
    for cwd in ["~/work", "$HOME/work"] {
        let script = script_for(cwd);
        assert!(!script.contains("cd \"~"), "{cwd}: {script}");
        assert!(!script.contains("cd \"$HOME/work"), "{cwd}: {script}");
    }

    // Not home-relative: another user's home, a different variable, and an
    // absolute path all stay fully encoded.
    for cwd in ["~other/work", "$HOMEDIR/work", "/abs/path"] {
        let script = script_for(cwd);
        assert!(
            script.contains("cd \"$(printf %s"),
            "cwd {cwd:?} must be treated as a literal path: {script}"
        );
        assert!(!script.contains("$HOME/$("), "{cwd}: {script}");
    }
}

fn script_for(cwd: &str) -> String {
    dispatch_script(
        &host(),
        &Job {
            id: "abc123".parse().unwrap(),
            cmd: "true".into(),
            cwd: Some(cwd.into()),
            max_secs: 0,
        },
    )
}
