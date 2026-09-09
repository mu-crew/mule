//! `coop host list`, and the master-detection seam beneath it.
//!
//! Test layer 1: no network, no ssh, no tmux.

use coop::config::Config;
use coop::transport::{Fake, Transport};

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
    assert!(coop::cli::host_list(&cfg, &fake, false).is_ok());
    assert!(coop::cli::host_list(&cfg, &fake, true).is_ok());
}

#[test]
fn every_configured_host_is_probed() {
    struct Counting(std::cell::Cell<usize>);
    impl Transport for Counting {
        fn run(
            &self,
            _h: &coop::config::Host,
            _s: &str,
        ) -> anyhow::Result<coop::transport::Output> {
            panic!("host list must not run scripts");
        }
        fn master_alive(&self, _h: &coop::config::Host) -> bool {
            self.0.set(self.0.get() + 1);
            true
        }
    }

    let cfg = Config::parse("[hosts.a]\n[hosts.b]\n[hosts.c]\n").unwrap();
    let t = Counting(std::cell::Cell::new(0));
    coop::cli::host_list(&cfg, &t, false).unwrap();
    assert_eq!(t.0.get(), 3);
}

#[test]
fn fake_replays_queued_outputs_in_order() {
    // The queue is how later tasks assert multi-round-trip flows (tail, ls).
    use coop::transport::Output;
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
    use coop::transport::Output;

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
