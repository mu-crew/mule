//! A truncated result stays partial even when the removal that follows fails.
//! Its own file: it sets `MULE_PARTIAL_FILE` for the whole process, which
//! would make any other test's truncation look like this one's.

use mule::config::Config;
use mule::exec::{PARTIAL_FILE_ENV, finish};
use mule::transport::{Fake, Output};

#[test]
fn a_truncated_log_is_partial_even_when_cleanup_fails() {
    let dir = std::env::temp_dir().join(format!("mule-exec-partial-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let flag = dir.join("partial");
    let _ = std::fs::remove_file(&flag);
    // SAFETY: the only test in this process.
    unsafe {
        std::env::set_var("XDG_STATE_HOME", &dir);
        std::env::set_var(PARTIAL_FILE_ENV, &flag);
    }
    let host =
        Config::parse("[hosts.dev]\ntarget = \"build.example\"\nsocket = \"/tmp/mule.sock\"\n")
            .unwrap()
            .host(None)
            .unwrap()
            .clone();
    let fake = Fake::new();
    fake.push(Output::ok(
        "exists=1\nrc=0\nalive=0\nsize=0\nbytes:\n".to_string(),
    ));
    // 97: the collect read the bytes, then its `rm` failed.
    fake.push(Output {
        code: 97,
        stdout: b"trunc=1\nlive=0\nlog=4\nerr=0\nbytes:\nxxxx".to_vec(),
        stderr: "rm: nope".into(),
    });
    let (mut out, mut err) = (Vec::new(), Vec::new());
    let result = finish(
        &fake,
        &host,
        &"a1b2c3".parse().unwrap(),
        &|_| {},
        &mut out,
        &mut err,
    );
    let e = result.expect_err("cleanup failure is still an error");
    assert!(format!("{e:#}").contains("could not remove"));
    assert_eq!(out, b"xxxx", "the clipped bytes were delivered");
    assert!(
        flag.exists(),
        "clipped bytes delivered, but no partial flag"
    );
}
