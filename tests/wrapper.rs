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
    }
}

#[test]
fn dispatch_encodes_the_command_in_a_detached_tmux_job() {
    let command = r#"printf '%s "quoted"' "$HOME/a b""#;
    let job = Job {
        id: "a1b2c3".parse().unwrap(),
        cmd: command.into(),
        cwd: None,
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
    assert_eq!(state_dir(&job.id), "$HOME/.local/state/coop/jobs/a1b2c3");
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
    };
    let script = dispatch_script(&host(), &job);
    assert!(
        !script.contains("cd /tmp/my dir &&"),
        "cwd must not be interpolated raw: {script}"
    );
}
