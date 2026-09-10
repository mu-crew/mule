use std::path::PathBuf;
use std::sync::Mutex;

use coop::config::{Config, Host};
use coop::jobs::{kill, list, list_with_hidden, prune};
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
        max_log_bytes: 100 * 1024 * 1024,
        max_job_secs: 0,
    }
}

#[test]
fn list_parses_remote_jobs_and_keeps_recent_finished_ones() {
    let fake = Fake::new();
    // ages in seconds: running/12s, done/34s, orphan/56s. `cmd` is hex, matching
    // the remote encoding -- awk cannot base64 without a fork per job.
    fake.push(Output::ok(
        "abc123\t12\t\t1\t6563686f206869\n\
         def456\t34\t9\t0\t66616c7365\n\
         fed987\t56\t\t0\t74727565\n",
    ));
    let cfg = Config::parse("[hosts.dev]\nsocket = \"/tmp/coop.sock\"\n").unwrap();

    let (rows, unreachable) = list(&cfg, &fake, None, false).unwrap();

    assert!(unreachable.is_empty());
    // All three are recent, so the default view shows the finished one too.
    // This is the fix: a short job is already `done` when the user first looks,
    // and `ls` is the documented recovery path for a lost id.
    assert_eq!(rows.len(), 3, "a recent finished job must not be hidden");
    assert_eq!(rows[0].id, "abc123");
    assert_eq!(rows[0].host, "dev");
    assert_eq!(rows[0].state, State::Running);
    assert_eq!(rows[0].rc, None);
    assert_eq!(rows[0].age_secs, 12);
    assert_eq!(rows[0].cmd, "echo hi");
    assert_eq!(rows[1].state, State::Done(9));
    assert_eq!(rows[1].rc, Some(9));
    assert_eq!(rows[1].cmd, "false");
    assert_eq!(rows[2].state, State::Orphan);
    assert_eq!(rows[2].cmd, "true");
}

#[test]
fn old_finished_jobs_need_all_but_running_and_orphan_never_do() {
    isolate_state();
    let cfg = Config::parse("[hosts.dev]\nsocket = \"/tmp/coop.sock\"\n").unwrap();
    let week = 7 * 24 * 60 * 60;

    // A week-old job in each state.
    let reply = format!(
        "aaaaaa\t{week}\t\t1\t6563686f206869\n\
         bbbbbb\t{week}\t0\t0\t66616c7365\n\
         cccccc\t{week}\t\t0\t74727565\n"
    );

    let fake = Fake::new();
    fake.push(Output::ok(reply.clone()));
    let (rows, _, hidden) = list_with_hidden(&cfg, &fake, None, false).unwrap();
    let ids: Vec<&str> = rows.iter().map(|r| r.id.as_str()).collect();
    assert_eq!(
        ids,
        ["aaaaaa", "cccccc"],
        "an old `done` job needs --all; running and orphan are never hidden"
    );
    assert_eq!(hidden, 1, "the renderer must know how many rows --all adds");

    let all = Fake::new();
    all.push(Output::ok(reply));
    let (rows, _) = list(&cfg, &all, None, true).unwrap();
    assert_eq!(rows.len(), 3, "--all shows the old finished job");
}

#[derive(Default)]
struct HostsFake {
    scripts: Mutex<Vec<(String, String)>>,
}

impl Transport for HostsFake {
    fn run_unlocked(&self, host: &Host, script: &str) -> anyhow::Result<Output> {
        self.scripts
            .lock()
            .unwrap()
            .push((host.name.clone(), script.to_owned()));
        Ok(Output::ok(format!(
            "{}01\t1\t\t1\t74727565\n",
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
    fake.push(Output::ok("137\n"));
    assert_eq!(
        kill(&fake, &host(), &"abc123".parse().unwrap()).unwrap(),
        137
    );

    let script = &fake.scripts()[0];
    let rc = script.find("[ -f $d/rc ] || echo 137 > $d/rc").unwrap();
    let kill = script.find("kill-session").unwrap();
    assert!(rc < kill);
}

#[test]
fn prune_uses_two_horizons_and_never_touches_running_jobs() {
    let script = prune(&host());

    // Finished jobs: the ordinary horizon.
    assert!(script.contains("-mtime +14"));
    assert!(script.contains("-exec test -f '{}/rc'"));

    // Orphans: kept four times as long, because an orphan is evidence -- but
    // bounded, since a disk-full incident leaves orphans holding the biggest
    // logs on the host and an outright exemption made that residue permanent.
    assert!(
        script.contains("-mtime +56"),
        "orphans need a longer horizon"
    );
    assert!(
        script.contains("-exec test ! -f '{}/rc'"),
        "the second pass must select rc-LESS directories"
    );

    assert!(script.contains("-exec rm -rf '{}'"));
    assert!(
        !script.contains("log"),
        "prune must not look at log contents"
    );

    // A running job has no `rc`, so only the orphan pass can match it -- and
    // that is safe only because the orphan horizon is far beyond the ordinary
    // one. Derive both from the same host config rather than restating them, so
    // a change to keep_days cannot silently narrow the gap.
    let doubled = prune(&Host {
        keep_days: 30,
        ..host()
    });
    assert!(doubled.contains("-mtime +30"), "{doubled}");
    assert!(doubled.contains("-mtime +120"), "{doubled}");
}
