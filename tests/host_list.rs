//! `mule host list`, and the master-detection seam beneath it.
//!
//! Test layer 1: no network, no ssh, no tmux.

use mule::config::Config;
use mule::transport::{Fake, Transport};

#[test]
fn host_list_json_escapes_every_free_text_field() {
    // Host names now have a filename-component grammar, so the free-text
    // target and socket fields carry the hostile JSON characters.
    let dir = std::env::temp_dir().join(format!("mule-host-json-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let config = dir.join("config.toml");
    std::fs::write(&config, "[hosts.safe-name]\ntarget = \"quote\\\" slash\\\\ newline\\n tab\\t control\\u0001 café\"\nsocket = \"/tmp/quote\\\"-slash\\\\-newline\\n-tab\\t-control\\u0001-café.sock\"\n").unwrap();
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_mule"))
        .arg("--config")
        .arg(&config)
        .args(["host", "list", "--json"])
        .output()
        .unwrap();
    assert!(output.status.success());
    let value: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("host list must emit valid JSON");
    let host = &value["items"][0];
    assert_eq!(host["name"], "safe-name");
    assert_eq!(
        host["target"],
        "quote\" slash\\ newline\n tab\t control\u{1} café"
    );
    assert_eq!(
        host["socket"],
        "/tmp/quote\"-slash\\-newline\n-tab\t-control\u{1}-café.sock"
    );
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn master_detection_is_a_separate_method_from_run() {
    // The exemption of `ssh -O check` from the ticket lock is only structural
    // if it is a different method. If someone ever routes master detection
    // through `run`, this fails: `run` records scripts, `master_alive` must not.
    let cfg = Config::parse("[hosts.dev]\n").unwrap();
    let host = cfg.host(None).unwrap();
    let fake = Fake::new();

    assert!(fake.master_alive(host));
    assert!(
        fake.scripts().is_empty(),
        "master_alive must not issue a script: it is the one call that takes \
         no session channel and no lock"
    );
}

#[test]
fn a_down_master_is_reported_not_fatal() {
    // "Which hosts can I use right now" is the question this verb answers, so a
    // down master is information and the verb still exits 0. Every other verb
    // treats it as exit 3.
    let cfg = Config::parse("[hosts.a]\n[hosts.b]\n").unwrap();
    let fake = Fake::no_master();
    assert!(mule::cli::host_list(&cfg, &fake, false).is_ok());
    assert!(mule::cli::host_list(&cfg, &fake, true).is_ok());
}

#[test]
fn every_configured_host_is_probed() {
    struct Counting(std::cell::Cell<usize>);
    impl Transport for Counting {
        fn run_unlocked(
            &self,
            _h: &mule::config::Host,
            _s: &str,
        ) -> anyhow::Result<mule::transport::Output> {
            panic!("host list must not run scripts");
        }
        fn master_alive(&self, _h: &mule::config::Host) -> bool {
            self.0.set(self.0.get() + 1);
            true
        }
    }

    let cfg = Config::parse("[hosts.a]\n[hosts.b]\n[hosts.c]\n").unwrap();
    let t = Counting(std::cell::Cell::new(0));
    mule::cli::host_list(&cfg, &t, false).unwrap();
    assert_eq!(t.0.get(), 3);
}

#[test]
fn fake_replays_queued_outputs_in_order() {
    // The queue is how later tasks assert multi-round-trip flows (tail, ls).
    use mule::transport::Output;
    let cfg = Config::parse("[hosts.dev]\n").unwrap();
    let host = cfg.host(None).unwrap();
    let fake = Fake::new();
    fake.push(Output::ok("first")).push(Output::fail(7, "boom"));

    assert_eq!(fake.run(host, "s1").unwrap().text(), "first");
    let second = fake.run(host, "s2").unwrap();
    assert_eq!(second.code, 7);
    assert_eq!(second.stderr, "boom");
    // Past the queue, a Fake yields empty successes rather than blocking.
    assert_eq!(fake.run(host, "s3").unwrap().code, 0);
    assert_eq!(fake.scripts(), vec!["s1", "s2", "s3"]);
}

#[test]
fn stdout_survives_invalid_utf8() {
    // The probe reply carries LOG BYTES in `stdout`, so a job emitting a
    // tarball or invalid UTF-8 must round-trip exactly. An earlier version ran
    // `String::from_utf8_lossy` here and silently substituted replacement
    // characters, corrupting the one artifact the design calls the truth.
    use mule::transport::Output;

    let raw: Vec<u8> = vec![0x00, 0xff, 0xfe, b'h', b'i', 0x80, 0x0a];
    let cfg = Config::parse("[hosts.dev]\n").unwrap();
    let host = cfg.host(None).unwrap();
    let fake = Fake::new();
    fake.push(Output::ok(raw.clone()));

    let got = fake.run(host, "cat some.tar").unwrap().stdout;
    assert_eq!(got, raw, "log bytes must survive verbatim");
    // Sanity: this is genuinely not valid UTF-8, so a lossy path would differ.
    assert!(String::from_utf8(raw.clone()).is_err());
    assert_ne!(String::from_utf8_lossy(&raw).as_bytes(), raw.as_slice());
}

#[test]
fn every_ssh_invocation_is_incapable_of_prompting() {
    // `BatchMode=yes` gags ssh's OWN prompts but not a `ProxyCommand`, which is
    // a separate program with its own terminal. A site wrapper that performs
    // 2FA (`ProxyCommand x2ssh ...`, as a corporate devserver typically sets)
    // will prompt regardless -- so a mule call against a host whose master died
    // could spawn a passcode prompt into the caller's terminal, with nothing
    // naming which invocation was asking.
    //
    // Asserted on the argv rather than by running ssh, because the failure is
    // the presence of a capability, and a passing run proves only that this
    // particular host had no proxy configured.
    let cfg = Config::parse("[hosts.dev]\ntarget = \"h\"\nsocket = \"/tmp/x.sock\"\n").unwrap();
    let host = cfg.host(None).unwrap();

    for args in [
        mule::transport::probe_args(host),
        mule::transport::run_args(host, "echo hi"),
    ] {
        let flat = args.join(" ");
        assert!(flat.contains("BatchMode=yes"), "{flat}");
        // Never create a master as a side effect: mule requires one to exist
        // and refuses otherwise, so creating one here would be both a surprise
        // and the thing that needs 2FA.
        assert!(flat.contains("ControlMaster=no"), "{flat}");
        // Safe because mule only multiplexes over an EXISTING master: the
        // socket is already connected, so no proxy is needed to reach the host.
        // The user's hand-opened master keeps its own ProxyCommand, which is
        // where 2FA belongs -- once per ControlPersist window, deliberately.
        assert!(flat.contains("ProxyCommand=none"), "{flat}");
    }
}

/// Every human table mule prints has a header and aligned columns.
///
/// `host list` had neither: it printed bare rows, so a reader had to know that
/// the second field was master state and the fourth a socket path. And
/// `host info` printed a FIXED-WIDTH header over unpadded rows, so the two
/// disagreed about where each column began as soon as a value was wider than
/// its title -- which is every real hostname.
///
/// `mule ls` had already solved this with one width-measuring renderer. This
/// asserts the other two use it too.
#[test]
fn human_tables_have_a_header_with_aligned_columns() {
    let dir = std::env::temp_dir().join(format!("mule-table-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let config = dir.join("config.toml");
    // A short name beside a long one: the long row is what exposes a
    // fixed-width header.
    std::fs::write(
        &config,
        "[hosts.a]\ntarget = \"short\"\n\
         [hosts.a-much-longer-name]\ntarget = \"a-considerably-longer-target\"\n",
    )
    .unwrap();

    // Start of each whitespace-separated column, so a header and a row can be
    // compared without knowing the widths.
    fn starts(line: &str) -> Vec<usize> {
        line.char_indices()
            .filter(|(i, c)| *c != ' ' && (*i == 0 || line.as_bytes()[i - 1] == b' '))
            .map(|(i, _)| i)
            .collect()
    }

    for verb in [vec!["host", "list"], vec!["host", "info"]] {
        let out = std::process::Command::new(env!("CARGO_BIN_EXE_mule"))
            .arg("--config")
            .arg(&config)
            .args(&verb)
            .output()
            .unwrap();
        let text = String::from_utf8_lossy(&out.stdout);
        let mut lines = text.lines().filter(|l| !l.trim().is_empty());

        let header = lines
            .next()
            .unwrap_or_else(|| panic!("{verb:?} printed no header"));
        assert!(
            header.starts_with("NAME"),
            "{verb:?} must lead with a NAME column: {header:?}"
        );
        for column in ["MASTER", "TARGET"] {
            assert!(
                header.contains(column),
                "{verb:?} header must name {column}: {header:?}"
            );
        }

        let want = starts(header);
        for row in lines {
            assert_eq!(
                starts(row),
                want,
                "{verb:?} column starts must match the header\n  header: {header:?}\n  row:    {row:?}"
            );
        }
    }

    std::fs::remove_dir_all(&dir).ok();
}

/// A next-step hint offers the surface the caller actually used.
///
/// `host list` hardcoded `--json`, so a human who had just read a table was
/// told to run the machine format. The verb is the same either way; only the
/// rendering differs, and the hint should follow the caller.
#[test]
fn the_host_list_hint_matches_the_callers_surface() {
    let dir = std::env::temp_dir().join(format!("mule-hint-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let config = dir.join("config.toml");
    std::fs::write(&config, "[hosts.dev]\ntarget = \"h\"\n").unwrap();

    let run = |args: &[&str]| -> String {
        let out = std::process::Command::new(env!("CARGO_BIN_EXE_mule"))
            .arg("--config")
            .arg(&config)
            .args(args)
            .output()
            .unwrap();
        String::from_utf8_lossy(&out.stderr).into_owned()
    };

    let table = run(&["host", "list"]);
    assert!(
        table.contains("next: mule host info for"),
        "a table reader gets the table form: {table:?}"
    );

    let json = run(&["host", "list", "--json"]);
    assert!(
        json.contains("next: mule host info --json for"),
        "a JSON reader gets the JSON form: {json:?}"
    );

    // --quiet still silences both.
    assert!(
        !run(&["--quiet", "host", "list"]).contains("next:"),
        "--quiet must suppress the hint"
    );

    std::fs::remove_dir_all(&dir).ok();
}
