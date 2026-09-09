use std::path::PathBuf;
use std::sync::Mutex;

use coop::config::{Config, Host};
use coop::jobs::{kill, list, prune};
use coop::probe::State;
use coop::transport::{Fake, Output, Transport};

fn isolate_state() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let dir = std::env::temp_dir().join(format!("coop-state-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        unsafe { std::env::set_var("XDG_STATE_HOME", &dir) };
    });
}

fn host() -> Host {
    isolate_state();
    Host {
        name: format!("jobs-test-{}", std::process::id()),
        target: "dev".into(),
        socket: PathBuf::from("/tmp/coop.sock"),
        tmux_socket: "coop".into(),
        max_running: 4,
        default_cwd: None,
        keep_days: 14,
    }
}

#[test]
fn list_parses_remote_jobs_and_excludes_done_by_default() {
    let fake = Fake::new();
    fake.push(Output::ok(
        "abc123\t12\t\t1\tZWNobyBoaQ==\n\
         def456\t34\t9\t0\tZmFsc2U=\n\
         fed987\t56\t\t0\tdHJ1ZQ==\n",
    ));
    let cfg = Config::parse("[hosts.dev]\nsocket = \"/tmp/coop.sock\"\n").unwrap();

    let (rows, unreachable) = list(&cfg, &fake, None, false).unwrap();

    assert!(unreachable.is_empty());
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].id, "abc123");
    assert_eq!(rows[0].host, "dev");
    assert_eq!(rows[0].state, State::Running);
    assert_eq!(rows[0].rc, None);
    assert_eq!(rows[0].age_secs, 12);
    assert_eq!(rows[0].cmd, "echo hi");
    assert_eq!(rows[1].state, State::Orphan);
    assert_eq!(rows[1].cmd, "true");

    let all = Fake::new();
    all.push(Output::ok("def456\t34\t9\t0\tZmFsc2U=\n"));
    let (rows, _) = list(&cfg, &all, None, true).unwrap();
    assert_eq!(rows[0].state, State::Done(9));
    assert_eq!(rows[0].rc, Some(9));
}

#[derive(Default)]
struct HostsFake {
    scripts: Mutex<Vec<(String, String)>>,
}

impl Transport for HostsFake {
    fn run(&self, host: &Host, script: &str) -> anyhow::Result<Output> {
        self.scripts
            .lock()
            .unwrap()
            .push((host.name.clone(), script.to_owned()));
        Ok(Output::ok(format!(
            "{}01\t1\t\t1\tdHJ1ZQ==\n",
            &host.name[..3]
        )))
    }

    fn master_alive(&self, host: &Host) -> bool {
        host.name != "down"
    }
}

#[test]
fn list_reports_unreachable_hosts_and_visits_live_hosts_sequentially() {
    isolate_state();
    let cfg = Config::parse(
        "[hosts.alpha]\nsocket = \"/tmp/a\"\n\
         [hosts.down]\nsocket = \"/tmp/d\"\n\
         [hosts.zulu]\nsocket = \"/tmp/z\"\n",
    )
    .unwrap();
    let fake = HostsFake::default();

    let (rows, unreachable) = list(&cfg, &fake, None, false).unwrap();

    assert_eq!(rows.len(), 2);
    assert_eq!(unreachable.len(), 1);
    assert_eq!(unreachable[0].host, "down");
    assert_eq!(unreachable[0].why, "no control master");
    let calls = fake.scripts.lock().unwrap();
    assert_eq!(calls.len(), 2, "one round trip for each reachable host");
    assert_eq!(calls[0].0, "alpha");
    assert_eq!(calls[1].0, "zulu");
}

#[test]
fn kill_records_rc_before_destroying_the_session() {
    let fake = Fake::new();
    kill(&fake, &host(), &"abc123".parse().unwrap()).unwrap();

    let script = &fake.scripts()[0];
    let rc = script.find("[ -f $d/rc ] || echo 137 > $d/rc").unwrap();
    let kill = script.find("kill-session").unwrap();
    assert!(rc < kill);
}

#[test]
fn prune_selects_only_old_directories_with_an_rc() {
    let script = prune(&host());

    assert!(script.contains("-mtime +14"));
    assert!(script.contains("-exec test -f '{}/rc'"));
    assert!(script.contains("-exec rm -rf '{}'"));
    assert!(!script.contains("log"));
}
