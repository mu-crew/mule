//! Caller keys (`run --key K`, `ls --key-prefix P`): layers 1 and 2.
//!
//! The mu plugins spec (D15) asks mule for three things a caller recovering
//! from its own crash needs: a key stored with the job as `$d/key`, a
//! dispatch whose key exists returning that job instead of starting another,
//! and a listing that carries the key. Layer 1 pins the logic over `Fake`;
//! layer 2 runs the real scripts under `sh` against a private tmux server,
//! because a `Fake` answers whatever it was told and cannot see a wrong
//! script. `tests/plugin_sshd.rs` drives the binary over a capped sshd.

mod common;

use std::path::Path;

use common::local::{Local, LocalTmux, scratch, tmux_available, wait_until};
use mule::config::{Config, Host};
use mule::errors::{MuleError, exit_code};
use mule::key::{JobKey, KeyPrefix};
use mule::run::{Dispatched, dispatch_keyed, lookup};
use mule::stopfence::StopFence;
use mule::transport::{Fake, Output};
use mule::wrapper::{JobId, JobMetadata, JobMode};

fn host() -> Host {
    common::local::isolate_state();
    Config::parse("[hosts.keys]\ntarget = \"h\"\n")
        .unwrap()
        .host(None)
        .unwrap()
        .clone()
}

fn key(raw: &str) -> JobKey {
    raw.parse().unwrap()
}

fn id(raw: &str) -> JobId {
    raw.parse().unwrap()
}

fn dispatch(
    t: &dyn mule::transport::Transport,
    host: &Host,
    fresh: &str,
    k: &JobKey,
    cmd: &str,
) -> anyhow::Result<Dispatched> {
    dispatch_keyed(
        t,
        host,
        id(fresh),
        Some(k),
        cmd,
        None,
        None,
        JobMetadata::Human,
        JobMode::Pipe,
        &mut std::io::sink(),
    )
}

fn request() -> mule::lease::Request<'static> {
    mule::lease::Request {
        cmd: "pi",
        cwd: None,
        max_secs: None,
        metadata: JobMetadata::Human,
    }
}

fn lease_start(fake: &Fake, dir: &Path, k: &JobKey) -> anyhow::Result<mule::lease::Owned> {
    mule::lease::start_keyed(
        fake,
        &host(),
        &dir.join("w.job"),
        &dir.join("stopping"),
        &request(),
        Some(k),
        &|_| {},
        &mut std::io::sink(),
    )
}

fn probe_reply(exists: u8, rc: &str, alive: u8) -> Output {
    Output::ok(format!(
        "exists={exists}\nrc={rc}\nalive={alive}\nsize=0\nbytes:\n"
    ))
}

// -- layer 1 ------------------------------------------------------------------

#[test]
fn a_key_is_validated_at_the_boundary() {
    // mu's keys are UUIDs; a caller may prefix them.
    for good in [
        "3f2c9a1e-7b4d-4e8f-9a0b-1c2d3e4f5a6b",
        "mu.ws-1_x",
        "a",
        &"k".repeat(128),
    ] {
        assert!(good.parse::<JobKey>().is_ok(), "{good:?}");
        assert!(good.parse::<KeyPrefix>().is_ok(), "{good:?}");
    }
    // Each would be syntax somewhere between the local argv and `grep -Fx`:
    // a quote, a space, a substitution, a newline (a second line of
    // `$d/key`), an option.
    for bad in [
        "",
        "a b",
        "a'b",
        "x$(touch /tmp/pwn)y",
        "a\nb",
        "-x",
        "a/b",
        "a*",
        &"k".repeat(129),
    ] {
        assert!(bad.parse::<JobKey>().is_err(), "{bad:?}");
        assert!(bad.parse::<KeyPrefix>().is_err(), "{bad:?}");
    }
}

#[test]
fn a_prefix_matches_only_keys_that_start_with_it() {
    let prefix: KeyPrefix = "mu.ws1.".parse().unwrap();
    assert!(prefix.matches(Some("mu.ws1.abc")));
    assert!(prefix.matches(Some("mu.ws1.")));
    assert!(!prefix.matches(Some("mu.ws2.abc")));
    assert!(!prefix.matches(Some("xmu.ws1.abc")));
    assert!(!prefix.matches(None), "an unkeyed job never matches");
}

#[test]
fn a_dispatch_whose_key_exists_returns_that_job() {
    let fake = Fake::new();
    fake.push(Output::ok("existing=abc123\n"));

    let got = dispatch(&fake, &host(), "f00001", &key("k1"), "sleep 1").unwrap();

    assert_eq!(got, Dispatched::Existing(id("abc123")));
    let scripts = fake.scripts();
    assert_eq!(
        scripts.len(),
        1,
        "the check and the start are one round trip"
    );
    // Order inside that one script: look, bail on a hit, then write the key
    // before tmux can start anything.
    let script = &scripts[0];
    let look = script.find("awk -v k=k1").expect("looks up the key");
    let bail = script.find("then exit 0").expect("stops on a hit");
    let write = script.find("printf '%s\\n' k1 >").expect("writes $d/key");
    let start = script.find("new-session").expect("starts the job");
    assert!(look < bail && bail < write && write < start, "{script}");
    assert!(script.contains("jobs/f00001/key"), "{script}");
}

#[test]
fn a_dispatch_with_no_hit_is_a_new_job_under_the_generated_id() {
    let fake = Fake::new();
    fake.push(Output::ok("1\n"));

    let got = dispatch(&fake, &host(), "f00002", &key("k2"), "true").unwrap();

    assert_eq!(got, Dispatched::New(id("f00002")));
}

#[test]
fn two_jobs_under_one_key_are_an_error_naming_both() {
    let fake = Fake::new();
    fake.push(Output::ok("existing=abc123\nexisting=def456\n"));

    let error = dispatch(&fake, &host(), "f00003", &key("k3"), "true").unwrap_err();

    let text = format!("{error:#}");
    assert!(text.contains("abc123") && text.contains("def456"), "{text}");
}

#[test]
fn a_keyed_lease_that_finds_its_key_holds_that_job_and_starts_nothing() {
    // Running, and also finished: a finished job is the answer to "start the
    // effect with key K", and the hold reports how it ended.
    for (reply, what) in [
        (probe_reply(1, "", 1), "running"),
        (probe_reply(1, "0", 0), "done"),
    ] {
        let (dir, fake) = (scratch("keys-lease"), Fake::new());
        fake.push(Output::ok("existing=abc123\n")).push(reply);

        let owned = lease_start(&fake, &dir, &key("k4")).unwrap();

        assert_eq!(owned.id, id("abc123"), "{what}");
        assert_eq!(
            std::fs::read_to_string(dir.join("w.job")).unwrap(),
            "abc123\n",
            "{what}: the job file names the job the key found"
        );
        // Owned as an attach owns one: the fence bumped, so a stop an older
        // carrier left retrying stands down.
        let fence = StopFence::in_dir(&dir.join("stopping"), &owned.id);
        assert_eq!(owned.fence_gen, 1, "{what}");
        assert_eq!(fence.generation_unlocked().unwrap(), 1, "{what}");
        let scripts = fake.scripts();
        assert_eq!(scripts.len(), 2, "{what}: the lookup and one probe");
        assert!(
            scripts.iter().all(|s| !s.contains("new-session")),
            "{what}: nothing is dispatched"
        );
    }
}

#[test]
fn a_keyed_lease_with_no_hit_names_its_job_before_the_keyed_dispatch() {
    let (dir, fake) = (scratch("keys-lease"), Fake::new());
    fake.push(Output::ok("")).push(Output::ok("1\n"));
    let file = dir.join("w.job");
    let seen = std::sync::Mutex::new(Vec::new());

    let owned = mule::lease::start_keyed(
        &fake,
        &host(),
        &file,
        &dir.join("stopping"),
        &request(),
        Some(&key("k5")),
        &|owned| {
            // Write-ahead: when the dispatch starts, the file already names it.
            if let Some(owned) = owned {
                seen.lock()
                    .unwrap()
                    .push(std::fs::read_to_string(&file).unwrap());
                assert_eq!(fake.scripts().len(), 1, "after the lookup only");
                let _ = owned;
            }
        },
        &mut std::io::sink(),
    )
    .unwrap();

    assert_eq!(seen.into_inner().unwrap(), vec![format!("{}\n", owned.id)]);
    let scripts = fake.scripts();
    assert_eq!(scripts.len(), 2);
    assert!(scripts[1].contains("awk -v k=k5"), "re-checked");
    assert!(scripts[1].contains("new-session"));
}

#[test]
fn a_lease_whose_claim_finds_the_key_held_names_only_the_holder() {
    // The loser of a race between two keyed leases: its claim finds the
    // winner's claim, whose dispatch has not run yet (a state dir with a key
    // and no `cmd`, which probes `unknown`). It holds that job; its job file
    // never names an id of its own (review of P7a: it used to name a fresh
    // id that was never dispatched, and exit 1).
    let (dir, fake) = (scratch("keys-lease"), Fake::new());
    fake.push(Output::ok("existing=ffff00\n")).push(Output::ok(
        "exists=1\nrc=\nrcread=1\ncmd=0\nalive=0\nwatch=0\nwhy=\nsize=0\nbytes:\n",
    ));

    let owned = lease_start(&fake, &dir, &key("k6")).unwrap();

    assert_eq!(owned.id, id("ffff00"));
    assert_eq!(
        std::fs::read_to_string(dir.join("w.job")).unwrap(),
        "ffff00\n"
    );
    let scripts = fake.scripts();
    assert_eq!(scripts.len(), 2, "the claim and one probe");
    assert!(scripts.iter().all(|s| !s.contains("new-session")));
}

#[test]
fn a_claim_is_made_before_the_job_file_and_the_dispatch_starts_its_own_claim() {
    let (dir, fake) = (scratch("keys-lease"), Fake::new());
    // The claim finds nothing; the dispatch's key check finds the claim.
    fake.push(Output::ok(""));
    let file = dir.join("w.job");
    let owned = mule::lease::start_keyed(
        &fake,
        &host(),
        &file,
        &dir.join("stopping"),
        &request(),
        Some(&key("k6b")),
        &|owned| {
            if let Some(owned) = owned {
                fake.push(Output::ok(format!("existing={}\n1\n", owned.id)));
            }
        },
        &mut std::io::sink(),
    )
    .unwrap();

    let scripts = fake.scripts();
    assert_eq!(scripts.len(), 2);
    assert!(
        scripts[0].contains(&format!("jobs/{}/key", owned.id))
            && !scripts[0].contains("new-session"),
        "the claim writes the key and starts nothing: {}",
        scripts[0]
    );
    assert!(scripts[1].contains("new-session"), "then the dispatch");
    assert_eq!(
        std::fs::read_to_string(&file).unwrap(),
        format!("{}\n", owned.id)
    );
}

#[test]
fn a_keyed_lease_without_a_master_is_exit_three_with_nothing_written() {
    let (dir, fake) = (scratch("keys-lease"), Fake::no_master());

    let error = lease_start(&fake, &dir, &key("k7")).unwrap_err();

    assert!(matches!(
        error.downcast_ref::<MuleError>(),
        Some(MuleError::NoMaster { .. })
    ));
    assert_eq!(exit_code(&error), 3);
    assert!(!dir.join("w.job").exists());
    assert!(fake.scripts().is_empty());
}

#[test]
fn lookup_returns_the_one_job_or_none() {
    let fake = Fake::new();
    fake.push(Output::ok("existing=abc123\n"))
        .push(Output::ok(""));

    assert_eq!(
        lookup(&fake, &host(), &key("k8")).unwrap(),
        Some(id("abc123"))
    );
    assert_eq!(lookup(&fake, &host(), &key("k9")).unwrap(), None);
}

#[test]
fn ls_rows_carry_the_key_and_old_six_field_rows_still_parse() {
    let cfg = Config::parse("[hosts.keys]\ntarget = \"h\"\n").unwrap();
    common::local::isolate_state();
    let fake = Fake::new();
    // `azE=` is base64 for `k1`. The second row predates keys: six fields.
    fake.push(Output::ok(
        "abc123\t12\t12\t\t1\tZWNobyBoaQ==\tazE=\n\
         def456\t34\t9\t9\t0\tZmFsc2U=\n\
         fed987\t56\t\t\t0\tdHJ1ZQ==\t\n",
    ));

    let (rows, _) = mule::jobs::list(&cfg, &fake, None, true).unwrap();

    let keys: Vec<_> = rows
        .iter()
        .map(|r| (r.id.as_str(), r.key.as_deref()))
        .collect();
    assert_eq!(
        keys,
        vec![("abc123", Some("k1")), ("def456", None), ("fed987", None)]
    );
}

#[test]
fn the_cli_refuses_a_bad_key_and_a_key_with_attach() {
    use clap::Parser;
    let parse = |args: &[&str]| mule::cli::Cli::try_parse_from(args);
    assert!(parse(&["mule", "run", "--key", "k1", "true"]).is_ok());
    assert!(parse(&["mule", "run", "--key", "a b", "true"]).is_err());
    assert!(parse(&["mule", "ls", "--json", "--key-prefix", "mu."]).is_ok());
    assert!(parse(&["mule", "ls", "--key-prefix", "$x"]).is_err());
    assert!(
        parse(&[
            "mule",
            "run",
            "--lease",
            "--job-file",
            "/tmp/f",
            "--attach",
            "abc123",
            "--key",
            "k1",
        ])
        .is_err(),
        "an attach names its job by id; a key would be a second answer"
    );
}

// -- layer 2: the real scripts --------------------------------------------------

#[test]
fn a_second_keyed_dispatch_returns_the_first_job_and_starts_nothing() {
    if !tmux_available() {
        eprintln!("skipping: tmux is not on PATH");
        return;
    }
    let tmux = LocalTmux::new("key-dedupe");
    let host = tmux.host();
    let local = Local::new(&scratch("keys-state"));
    let k = key("mu.3f2c9a1e-7b4d-4e8f-9a0b-1c2d3e4f5a6b");

    let first = dispatch(&local, &host, "a00001", &k, "sleep 30").unwrap();
    let second = dispatch(&local, &host, "a00002", &k, "sleep 30").unwrap();

    assert_eq!(first, Dispatched::New(id("a00001")));
    assert_eq!(second, Dispatched::Existing(id("a00001")));
    assert_eq!(
        std::fs::read_to_string(local.jobs().join("a00001/key")).unwrap(),
        format!("{k}\n"),
        "stored as $d/key"
    );
    assert!(!local.jobs().join("a00002").exists(), "no second state dir");
    assert!(tmux.has_session("mule-a00001"));
    assert!(!tmux.has_session("mule-a00002"), "no second job");
    assert_eq!(lookup(&local, &host, &k).unwrap(), Some(id("a00001")));
    assert_eq!(lookup(&local, &host, &key("other")).unwrap(), None);
}

#[test]
fn a_finished_job_keeps_its_key_until_rm() {
    if !tmux_available() {
        eprintln!("skipping: tmux is not on PATH");
        return;
    }
    let tmux = LocalTmux::new("key-done");
    let host = tmux.host();
    let local = Local::new(&scratch("keys-state"));
    let k = key("k-done");

    assert_eq!(
        dispatch(&local, &host, "b00001", &k, "exit 3").unwrap(),
        Dispatched::New(id("b00001"))
    );
    wait_until("the job to finish", 10, || {
        local.jobs().join("b00001/rc").exists()
    });
    // The effect happened: asking again returns it, not a re-run.
    assert_eq!(
        dispatch(&local, &host, "b00002", &k, "exit 3").unwrap(),
        Dispatched::Existing(id("b00001"))
    );
    // `rm` proves the job and its key gone, so the key is free again.
    mule::jobs::remove(&local, &host, &mule::jobs::Target::One(id("b00001"))).unwrap();
    assert_eq!(lookup(&local, &host, &k).unwrap(), None);
    assert_eq!(
        dispatch(&local, &host, "b00003", &k, "true").unwrap(),
        Dispatched::New(id("b00003"))
    );
}

#[test]
fn a_key_that_cannot_be_checked_refuses_the_dispatch() {
    if !tmux_available() {
        eprintln!("skipping: tmux is not on PATH");
        return;
    }
    use std::os::unix::fs::PermissionsExt;
    let tmux = LocalTmux::new("key-unreadable");
    let host = tmux.host();
    let local = Local::new(&scratch("keys-state"));
    dispatch(&local, &host, "c00001", &key("k-a"), "true").unwrap();
    let file = local.jobs().join("c00001/key");
    std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o000)).unwrap();
    if std::fs::read(&file).is_ok() {
        eprintln!("skipping: modes are not enforced for this user");
        return;
    }

    // "No hit" from a grep that could not read every key would start a second
    // job for an effect that may already exist.
    let error = dispatch(&local, &host, "c00002", &key("k-b"), "true").unwrap_err();

    std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o600)).unwrap();
    assert!(
        format!("{error:#}").contains("cannot read the job keys"),
        "{error:#}"
    );
    assert!(!local.jobs().join("c00002").exists(), "nothing dispatched");
}

#[test]
fn the_real_listing_carries_the_key_and_none_for_an_unkeyed_job() {
    if !tmux_available() {
        eprintln!("skipping: tmux is not on PATH");
        return;
    }
    let tmux = LocalTmux::new("key-ls");
    let host = tmux.host();
    let local = Local::new(&scratch("keys-state"));
    let cfg = Config::parse(&format!(
        "[hosts.local]\ntarget = \"local\"\ntmux_socket = \"{}\"\n",
        tmux.socket
    ))
    .unwrap();
    dispatch(&local, &host, "d00001", &key("mu.a.1"), "sleep 30").unwrap();
    mule::run::dispatch_with_id(
        &local,
        &host,
        id("d00002"),
        "sleep 30",
        None,
        None,
        JobMetadata::Human,
        JobMode::Pipe,
        &mut std::io::sink(),
    )
    .unwrap();
    // A hand-edited key with a tab must not shift a column: keys travel
    // base64-encoded, like commands.
    dispatch(&local, &host, "d00003", &key("mu.b.1"), "sleep 30").unwrap();
    std::fs::write(local.jobs().join("d00003/key"), "mu.b\t1\n").unwrap();

    let (mut rows, unreachable) = mule::jobs::list(&cfg, &local, None, true).unwrap();
    rows.sort_by(|a, b| a.id.cmp(&b.id));

    assert!(unreachable.is_empty());
    let got: Vec<_> = rows
        .iter()
        .map(|r| (r.id.as_str(), r.key.as_deref(), r.cmd.as_str()))
        .collect();
    assert_eq!(
        got,
        vec![
            ("d00001", Some("mu.a.1"), "sleep 30"),
            ("d00002", None, "sleep 30"),
            ("d00003", Some("mu.b\t1"), "sleep 30"),
        ]
    );
    assert!(rows.iter().all(|r| r.state == mule::probe::State::Running));
}

// -- layer 2: the binary --------------------------------------------------------

fn shim_jobs(host: &common::local::ShimHost) -> Vec<String> {
    let mut ids: Vec<String> = std::fs::read_dir(host.jobs())
        .map(|dir| {
            dir.filter_map(Result::ok)
                .map(|e| e.file_name().to_string_lossy().into_owned())
                .collect()
        })
        .unwrap_or_default();
    ids.sort();
    ids
}

#[test]
fn run_key_twice_prints_one_id_and_says_nothing_was_dispatched() {
    if !tmux_available() {
        eprintln!("skipping: tmux is not on PATH");
        return;
    }
    use common::local::{ShimHost, stderr, stdout};
    let host = ShimHost::new("keys-cli");

    let first = host.mule(&["run", "--quiet", "--key", "mu.k1", "sleep 30"]);
    let second = host.mule(&["run", "--quiet", "--key", "mu.k1", "sleep 30"]);

    assert!(first.status.success(), "{}", stderr(&first));
    assert!(second.status.success(), "{}", stderr(&second));
    let id = stdout(&first);
    assert_eq!(stdout(&second), id, "the same job, by its generated id");
    assert!(
        stderr(&second).contains(&format!(
            "key mu.k1 already names job {id}; nothing was dispatched"
        )),
        "said even under --quiet: {}",
        stderr(&second)
    );
    assert_eq!(shim_jobs(&host), vec![id]);
}

#[test]
fn ls_key_prefix_lists_only_matching_jobs_at_any_age_with_their_key() {
    if !tmux_available() {
        eprintln!("skipping: tmux is not on PATH");
        return;
    }
    use common::local::{ShimHost, stderr, stdout};
    let host = ShimHost::new("keys-ls");
    let old = stdout(&host.mule(&["run", "--quiet", "--key", "mu.old", "exit 2"]));
    let other = stdout(&host.mule(&["run", "--quiet", "--key", "ot.x", "sleep 30"]));
    let plain = stdout(&host.mule(&["run", "--quiet", "sleep 30"]));
    wait_until("the old job to finish", 10, || {
        host.jobs().join(&old).join("rc").exists()
    });
    // Three days old: outside ls's default 24-hour window, inside keep_days.
    let touched = std::process::Command::new("touch")
        .args(["-t", &stamp_days_ago(3)])
        .arg(host.jobs().join(&old))
        .status()
        .unwrap();
    assert!(touched.success());
    let rows = |args: &[&str]| -> Vec<serde_json::Value> {
        let out = host.mule(args);
        assert!(out.status.success(), "{args:?}: {}", stderr(&out));
        let value: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
        value["items"].as_array().unwrap().clone()
    };

    let default = rows(&["ls", "--json"]);
    let found = rows(&["ls", "--json", "--key-prefix", "mu."]);

    assert!(
        default.iter().all(|row| row["id"] != old.as_str()),
        "the default window hides the old finished job"
    );
    // A lookup must not miss it: missing it would start the effect again.
    assert_eq!(found.len(), 1, "{found:?}");
    assert_eq!(found[0]["id"], old.as_str());
    assert_eq!(found[0]["key"], "mu.old");
    assert_eq!(found[0]["state"], "done");
    assert_eq!(found[0]["rc"], 2);
    // The key is a field of every keyed row, and absent from an unkeyed one.
    let all = rows(&["ls", "--json", "--all"]);
    let key_of = |id: &str| {
        all.iter()
            .find(|row| row["id"] == id)
            .map(|row| row.get("key").cloned())
    };
    assert_eq!(key_of(&other), Some(Some(serde_json::json!("ot.x"))));
    assert_eq!(key_of(&plain), Some(None));
}

/// A `touch -t` stamp `days` in the past (BSD `date -v`, else GNU `-d`).
fn stamp_days_ago(days: u64) -> String {
    let bsd = std::process::Command::new("date")
        .args([&format!("-v-{days}d"), "+%Y%m%d0000"])
        .output()
        .unwrap();
    let out = if bsd.status.success() {
        bsd
    } else {
        std::process::Command::new("date")
            .args(["-d", &format!("{days} days ago"), "+%Y%m%d0000"])
            .output()
            .unwrap()
    };
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

#[test]
fn a_keyed_lease_holds_the_job_its_key_names_and_exits_with_its_code() {
    if !tmux_available() {
        eprintln!("skipping: tmux is not on PATH");
        return;
    }
    use common::local::{ShimHost, stderr, stdout};
    let host = ShimHost::new("keys-lease-cli");
    let id = stdout(&host.mule(&["run", "--quiet", "--key", "mu.l1", "sleep 2; exit 7"]));
    let file = host.dir.join("w.job");

    let out = host.mule(&[
        "run",
        "--lease",
        "--key",
        "mu.l1",
        "--job-file",
        file.to_str().unwrap(),
        "--",
        "sleep 30",
    ]);

    assert_eq!(out.status.code(), Some(7), "{}", stderr(&out));
    assert_eq!(std::fs::read_to_string(&file).unwrap(), format!("{id}\n"));
    assert_eq!(shim_jobs(&host), vec![id], "the lease started nothing");
}

#[test]
fn the_key_scripts_run_under_zsh_with_no_jobs_yet_and_with_some() {
    // The remote shell is the user's login shell. zsh aborts a command whose
    // glob matches nothing (`no matches found`), and the first version's
    // `"$root"/*/key` failed every first keyed dispatch on a zsh host --
    // found over the loopback sshd, never by `sh`.
    if !tmux_available()
        || std::process::Command::new("zsh")
            .arg("-c")
            .arg("true")
            .output()
            .is_err()
    {
        eprintln!("skipping: tmux or zsh is not on PATH");
        return;
    }
    let tmux = LocalTmux::new("key-zsh");
    let host = tmux.host();
    let mut local = Local::new(&scratch("keys-state"));
    local.shell = "zsh";
    // An empty jobs root: the glob case.
    std::fs::create_dir_all(local.jobs()).unwrap();

    assert_eq!(lookup(&local, &host, &key("z1")).unwrap(), None);
    assert_eq!(
        dispatch(&local, &host, "e00001", &key("z1"), "sleep 30").unwrap(),
        Dispatched::New(id("e00001"))
    );
    assert_eq!(
        dispatch(&local, &host, "e00002", &key("z1"), "sleep 30").unwrap(),
        Dispatched::Existing(id("e00001"))
    );
}
