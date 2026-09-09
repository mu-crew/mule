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
    }
}

#[test]
fn dispatch_encodes_the_command_in_a_detached_tmux_job() {
    let command = r#"printf '%s "quoted"' "$HOME/a b""#;
    let job = Job {
        id: "a1b2c3".into(),
        cmd: command.into(),
        cwd: None,
    };

    let script = dispatch_script(&host(), &job);

    assert!(script.contains("tmux -L coop new-session -d -s coop-a1b2c3"));
    assert!(!script.contains(command));
    assert_eq!(script.matches('\'').count() % 2, 0);
    assert_eq!(state_dir(&job.id), "$HOME/.local/state/coop/a1b2c3");
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
