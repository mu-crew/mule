//! Test layer 3 for `mule forward`, driven in-process.
//!
//! Separate from `local_sshd.rs` because it calls `mule::forward` directly (the
//! race needs a hook between two of its mux ops, which no CLI invocation
//! offers), and the library reads its state directory from `XDG_STATE_HOME`.
//! This binary sets that once, before any thread starts; in `local_sshd.rs`
//! other tests spawn processes concurrently, and a `set_var` there would race
//! them.
//!
//! The master is a real `ssh -M` with `StreamLocalBindUnlink=yes` against a
//! loopback `MaxSessions 1` sshd. Nothing is canned.

mod common;

use std::io::{Read, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Condvar, Mutex};
use std::time::{Duration, Instant};

use common::sshd::Sshd;
use mule::config::{Config, Host};
use mule::forward::{Spec, forward, read_record, unforward};
use mule::transport::{MuxOp, Output, Transport};

fn isolate_state() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let dir = std::env::temp_dir().join(format!("mule-fwd-l3-state-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        // Safe: the first thing every test here does, before it spawns a thread.
        unsafe { std::env::set_var("XDG_STATE_HOME", &dir) };
    });
}

/// Two callers meet here, or give up after `wait`.
///
/// A plain `Barrier` would deadlock exactly when the code under test is
/// correct: with the per-LOCAL lock the second caller cannot arrive while the
/// first sits at the rendezvous, so the first must be allowed to time out and
/// carry on. Without the lock both arrive, and `wait` returns `true` for each.
struct Rendezvous {
    arrived: Mutex<usize>,
    cv: Condvar,
}

impl Rendezvous {
    fn meet(&self, wait: Duration) -> bool {
        let mut arrived = self.arrived.lock().unwrap();
        *arrived += 1;
        self.cv.notify_all();
        let (arrived, _) = self
            .cv
            .wait_timeout_while(arrived, wait, |n| *n < 2)
            .unwrap();
        *arrived >= 2
    }
}

/// The real `ssh -O ...`, with an optional rendezvous just before ForwardUnix.
struct Real<'a> {
    sshd: &'a Sshd,
    rendezvous: Option<Rendezvous>,
}

impl Transport for Real<'_> {
    fn run_unlocked(&self, _: &Host, _: &str) -> anyhow::Result<Output> {
        panic!("forwards must open no session");
    }

    fn mux_op(&self, host: &Host, op: MuxOp) -> anyhow::Result<Output> {
        if let MuxOp::ForwardUnix(local, remote) = &op
            && let Some(rendezvous) = &self.rendezvous
        {
            // Both callers have, by now, read the record, cancelled and
            // unlinked; neither has added its forward. That is the window.
            let both = rendezvous.meet(Duration::from_millis(750));
            if both && remote.ends_with("r2.sock") {
                // Pin the order: R1's record lands before R2 returns, so the
                // final record names R2 -- the reviewer's wedge ordering.
                let deadline = Instant::now() + Duration::from_secs(5);
                while read_record(host, local).is_none() {
                    assert!(Instant::now() < deadline, "R1 never saved its record");
                    std::thread::sleep(Duration::from_millis(5));
                }
            }
        }
        let out = Command::new(self.sshd.dir.join("ssh"))
            .args(mule::transport::mux_args(host, &op))
            .output()?;
        Ok(Output {
            code: out.status.code().unwrap_or(-1),
            stdout: out.stdout,
            stderr: String::from_utf8_lossy(&out.stderr).into_owned(),
        })
    }
}

fn host(sshd: &Sshd) -> Host {
    Config::parse(&format!(
        "[hosts.fwdrace{}]\ntarget = \"127.0.0.1\"\nsocket = \"{}\"\n",
        sshd.port,
        sshd.socket.display()
    ))
    .unwrap()
    .host(None)
    .unwrap()
    .clone()
}

fn spec(local: &Path, remote: &Path) -> Spec {
    Spec::parse(&format!("{}:{}", local.display(), remote.display())).unwrap()
}

fn serve(path: PathBuf, reply: &'static str) {
    let listener = UnixListener::bind(path).unwrap();
    std::thread::spawn(move || {
        for mut stream in listener.incoming().flatten() {
            let _ = stream.write_all(reply.as_bytes());
        }
    });
}

fn through(path: &Path) -> String {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Ok(mut stream) = UnixStream::connect(path) {
            stream
                .set_read_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            let mut text = String::new();
            let _ = stream.read_to_string(&mut text);
            if !text.is_empty() || Instant::now() > deadline {
                return text;
            }
        } else if Instant::now() > deadline {
            return String::new();
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// Reviewer repro (high): two `forward`s of one LOCAL to different REMOTEs,
/// held at the point where each has passed its pre-cancel. Unserialised, both
/// returned Ok, the record named R2, and the master carried R1: `unforward R2`
/// then wedged it (exit 3, master restart). Serialised, one wins cleanly and
/// the table stays usable.
#[test]
fn concurrent_forwards_of_one_local_leave_one_consistent_record_and_no_wedge() {
    if let Some(why) = common::sshd::unavailable() {
        eprintln!("skipping: {why}");
        return;
    }
    isolate_state();
    let sshd = Sshd::start();
    let _master = sshd.open_master_with(&sshd.socket, &["-o", "StreamLocalBindUnlink=yes"]);
    let host = host(&sshd);
    let local = sshd.dir.join("l.sock");
    let (r1, r2) = (sshd.dir.join("r1.sock"), sshd.dir.join("r2.sock"));
    serve(r1.clone(), "one");
    serve(r2.clone(), "two");

    let t = Real {
        sshd: &sshd,
        rendezvous: Some(Rendezvous {
            arrived: Mutex::new(0),
            cv: Condvar::new(),
        }),
    };
    std::thread::scope(|scope| {
        let a = scope.spawn(|| forward(&t, &host, &[spec(&local, &r1)]));
        let b = scope.spawn(|| forward(&t, &host, &[spec(&local, &r2)]));
        a.join().unwrap().unwrap();
        b.join().unwrap().unwrap();
    });

    // The record must describe what the master actually carries.
    let recorded = read_record(&host, &local).expect("a record");
    let expect = if recorded.remote.ends_with("r1.sock") {
        "one"
    } else {
        "two"
    };
    assert_eq!(through(&local), expect, "record says {}", recorded.remote);

    // And the table is not wedged: undo what the record says, forward fresh.
    let serial = Real {
        sshd: &sshd,
        rendezvous: None,
    };
    unforward(
        &serial,
        &host,
        &[Spec {
            local: local.clone(),
            remote: recorded.remote,
        }],
    )
    .unwrap();
    forward(&serial, &host, &[spec(&local, &r1)]).unwrap();
    assert_eq!(through(&local), "one");
}
