//! Test layer 1 for `mule forward` / `unforward`: the sequence, over `Fake`.
//!
//! `Fake::on_mux` plays the master. Like the real one it binds LOCAL on a
//! forward, leaves the file behind on a cancel, and fails to bind over an
//! existing file -- so a missing unlink shows up as a failure here, not only
//! against a real sshd (layer 3 covers that).

use std::collections::HashMap;
use std::os::unix::net::UnixListener;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use mule::config::{Config, Host};
use mule::errors::{EXIT_NO_MASTER, exit_code};
use mule::forward::{Spec, forward, lock_local, read_record, unforward};
use mule::transport::{Fake, MuxOp};

fn isolate_state() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let dir = std::env::temp_dir().join(format!("mule-fwd-state-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        // Safe: runs once, before any thread reads it.
        unsafe { std::env::set_var("XDG_STATE_HOME", &dir) };
    });
}

fn host() -> Host {
    Config::parse("[hosts.fwd]\ntarget = \"h\"\n")
        .unwrap()
        .host(None)
        .unwrap()
        .clone()
}

/// A short directory: unix socket paths are capped near 104 bytes on macOS and
/// `$TMPDIR` there is already ~50.
fn scratch() -> PathBuf {
    static SEQ: AtomicU32 = AtomicU32::new(0);
    let dir = PathBuf::from(format!(
        "/tmp/mf-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

type Listeners = Arc<Mutex<HashMap<PathBuf, UnixListener>>>;

/// A master that binds on forward and drops the listener (not the file) on
/// cancel.
fn master(fake: &Fake) -> Listeners {
    let listeners: Listeners = Arc::default();
    let held = Arc::clone(&listeners);
    fake.on_mux(move |op| match op {
        MuxOp::ForwardUnix(local, _) => {
            if let Ok(listener) = UnixListener::bind(local) {
                held.lock().unwrap().insert(local.clone(), listener);
            }
        }
        MuxOp::CancelUnix(local, _) => {
            held.lock().unwrap().remove(local);
        }
        MuxOp::Check => {}
    });
    listeners
}

fn spec(local: &Path, remote: &str) -> Spec {
    Spec::parse(&format!("{}:{remote}", local.display())).unwrap()
}

fn kinds(ops: &[MuxOp]) -> Vec<String> {
    ops.iter()
        .map(|op| match op {
            MuxOp::Check => "check".to_string(),
            MuxOp::ForwardUnix(_, r) => format!("forward {r}"),
            MuxOp::CancelUnix(_, r) => format!("cancel {r}"),
        })
        .collect()
}

#[test]
fn forward_runs_check_cancel_unlink_forward_verify_record_in_that_order() {
    isolate_state();
    let dir = scratch();
    let local = dir.join("a.sock");
    // A stale socket file from an earlier forward: step 3 must unlink it, or
    // the fake master (like a master without StreamLocalBindUnlink) cannot bind.
    drop(UnixListener::bind(&local).unwrap());
    let fake = Fake::new();
    let _held = master(&fake);

    forward(&fake, &host(), &[spec(&local, "/r/a")]).unwrap();

    assert_eq!(
        kinds(&fake.mux_ops()),
        ["check", "cancel /r/a", "forward /r/a"]
    );
    assert!(fake.scripts().is_empty(), "no session was opened");
    let record = read_record(&host(), &local).expect("verified, so recorded");
    assert_eq!(record.remote, "/r/a");
    assert_eq!(record.master_pid, Some(4242));
    let meta = std::fs::metadata(&local).unwrap();
    use std::os::unix::fs::MetadataExt;
    assert_eq!((record.dev, record.ino), (meta.dev(), meta.ino()));
}

#[test]
fn a_changed_remote_cancels_the_recorded_spec_first_when_the_master_is_the_same() {
    isolate_state();
    let dir = scratch();
    let local = dir.join("b.sock");
    let fake = Fake::new();
    let _held = master(&fake);
    forward(&fake, &host(), &[spec(&local, "/r/one")]).unwrap();
    let before = fake.mux_ops().len();

    forward(&fake, &host(), &[spec(&local, "/r/two")]).unwrap();

    assert_eq!(
        kinds(&fake.mux_ops()[before..]),
        [
            "check",
            // The recorded spec: without this, the table wedges.
            "cancel /r/one",
            "cancel /r/two",
            "forward /r/two"
        ]
    );
    assert_eq!(read_record(&host(), &local).unwrap().remote, "/r/two");
}

#[test]
fn a_record_from_another_master_is_not_cancelled() {
    // A replaced master has none of the old forwards; cancelling one there is
    // noise at best.
    isolate_state();
    let dir = scratch();
    let local = dir.join("c.sock");
    let first = Fake::new().with_master_pid(1);
    let _held = master(&first);
    forward(&first, &host(), &[spec(&local, "/r/one")]).unwrap();

    let second = Fake::new().with_master_pid(2);
    let _held2 = master(&second);
    forward(&second, &host(), &[spec(&local, "/r/two")]).unwrap();

    assert_eq!(
        kinds(&second.mux_ops()),
        ["check", "cancel /r/two", "forward /r/two"]
    );
}

#[test]
fn a_non_socket_local_is_refused_untouched_with_exit_one() {
    isolate_state();
    let dir = scratch();
    let local = dir.join("data");
    std::fs::write(&local, "precious").unwrap();
    let fake = Fake::new();
    let _held = master(&fake);

    let error = forward(&fake, &host(), &[spec(&local, "/r/x")]).unwrap_err();

    assert_eq!(exit_code(&error), 1);
    assert!(format!("{error:#}").contains("not a socket"), "{error:#}");
    assert_eq!(std::fs::read_to_string(&local).unwrap(), "precious");
    assert!(
        !kinds(&fake.mux_ops())
            .iter()
            .any(|op| op.starts_with("forward")),
        "nothing was forwarded over a file that is not ours"
    );
    assert!(read_record(&host(), &local).is_none());
}

#[test]
fn a_failed_verify_is_exit_three_with_the_wedge_hand_back() {
    isolate_state();
    let dir = scratch();
    let local = dir.join("d.sock");
    // A master that says yes and binds nothing: the measured wedge.
    let fake = Fake::new();

    let error = forward(&fake, &host(), &[spec(&local, "/r/x")]).unwrap_err();

    assert_eq!(exit_code(&error), EXIT_NO_MASTER);
    let text = format!("{error:#}");
    assert!(text.contains("wedged"), "{text}");
    assert!(text.contains("-O exit"), "{text}");
    assert!(text.contains("ssh -MNf"), "{text}");
    assert!(text.contains("StreamLocalBindUnlink=yes"), "{text}");
    assert!(
        read_record(&host(), &local).is_none(),
        "unverified: no record"
    );
}

#[test]
fn a_wedge_does_not_roll_back_forwards_that_did_verify() {
    // The table is wedged, not the earlier forward: tearing it down would only
    // add cancels to a table that cannot be trusted. The human restarts the
    // master anyway.
    isolate_state();
    let dir = scratch();
    let (good, bad) = (dir.join("good.sock"), dir.join("bad.sock"));
    let fake = Fake::new();
    let held = Arc::clone(&master(&fake));
    let bad_path = bad.clone();
    let inner = Arc::clone(&held);
    fake.on_mux(move |op| {
        if let MuxOp::ForwardUnix(local, _) = op
            && *local != bad_path
        {
            inner
                .lock()
                .unwrap()
                .insert(local.clone(), UnixListener::bind(local).unwrap());
        }
    });

    let error = forward(&fake, &host(), &[spec(&good, "/r/g"), spec(&bad, "/r/b")]).unwrap_err();

    assert_eq!(exit_code(&error), EXIT_NO_MASTER);
    assert!(read_record(&host(), &good).is_some());
}

#[test]
fn another_failure_cancels_what_this_call_added_and_names_the_first_failure() {
    isolate_state();
    let dir = scratch();
    let (first, blocker) = (dir.join("first.sock"), dir.join("blocker"));
    std::fs::write(&blocker, "x").unwrap();
    let fake = Fake::new();
    let held = master(&fake);

    let error = forward(
        &fake,
        &host(),
        &[spec(&first, "/r/1"), spec(&blocker, "/r/2")],
    )
    .unwrap_err();

    assert_eq!(exit_code(&error), 1);
    let text = format!("{error:#}");
    assert!(text.contains("blocker"), "names the failure: {text}");
    assert!(
        kinds(&fake.mux_ops()).contains(&"cancel /r/1".to_string()),
        "{:?}",
        fake.mux_ops()
    );
    assert!(held.lock().unwrap().is_empty(), "listener cancelled");
    assert!(!first.exists(), "its socket file is removed too");
    assert!(read_record(&host(), &first).is_none());
}

#[test]
fn a_refused_forward_is_exit_one_not_three() {
    isolate_state();
    let dir = scratch();
    let local = dir.join("e.sock");
    let fake = Fake::new();
    fake.push_mux(mule::transport::Output::ok("")) // the cancel
        .push_mux(mule::transport::Output::fail(
            255,
            "mux_client_forward: refused",
        ));

    let error = forward(&fake, &host(), &[spec(&local, "/r/x")]).unwrap_err();

    assert_eq!(exit_code(&error), 1);
    assert!(format!("{error:#}").contains("refused"), "{error:#}");
}

#[test]
fn no_master_is_exit_three_and_nothing_is_forwarded() {
    isolate_state();
    let dir = scratch();
    let fake = Fake::no_master();

    let error = forward(&fake, &host(), &[spec(&dir.join("f.sock"), "/r/x")]).unwrap_err();

    assert_eq!(exit_code(&error), EXIT_NO_MASTER);
    assert_eq!(kinds(&fake.mux_ops()), ["check"]);
}

#[test]
fn unforward_cancels_unlinks_forgets_and_is_idempotent() {
    isolate_state();
    let dir = scratch();
    let local = dir.join("g.sock");
    let fake = Fake::new();
    let held = master(&fake);
    forward(&fake, &host(), &[spec(&local, "/r/x")]).unwrap();
    assert!(local.exists());

    unforward(&fake, &host(), &[spec(&local, "/r/x")]).unwrap();
    assert!(!local.exists());
    assert!(read_record(&host(), &local).is_none());
    assert!(held.lock().unwrap().is_empty());

    // Again, and with no master at all.
    unforward(&Fake::no_master(), &host(), &[spec(&local, "/r/x")]).unwrap();
}

#[test]
fn unforward_leaves_a_non_socket_alone() {
    isolate_state();
    let dir = scratch();
    let local = dir.join("h");
    std::fs::write(&local, "keep").unwrap();
    unforward(&Fake::new(), &host(), &[spec(&local, "/r/x")]).unwrap();
    assert_eq!(std::fs::read_to_string(&local).unwrap(), "keep");
}

#[test]
fn specs_are_parsed_strictly() {
    assert!(Spec::parse("nocolon").is_err());
    assert!(Spec::parse(":/r").is_err());
    assert!(Spec::parse("/l:").is_err());
    assert!(Spec::parse("/l:/r:/x").is_err(), "ssh splits -L on ':'");
    assert!(
        Spec::parse("rel.sock:/r").unwrap().local.is_absolute(),
        "the master binds LOCAL against its own cwd, so it must be absolute"
    );
    assert!(
        Spec::parse_all(&["/l:/r1".into(), "/l:/r2".into()]).is_err(),
        "one LOCAL to two REMOTEs is the wedge"
    );
    // A space is just a character: paths go to ssh as argv, never a shell.
    assert_eq!(Spec::parse("/a b:/c d").unwrap().remote, "/c d");
}

#[test]
fn mux_args_carry_paths_as_single_argv_elements_and_never_a_remote_forward() {
    let host = host();
    let args = mule::transport::mux_args(
        &host,
        &MuxOp::ForwardUnix("/tmp/a b.sock".into(), "/r/c d".into()),
    );
    let tail = &args[args.len() - 4..];
    assert_eq!(tail, ["-O", "forward", "-L", "/tmp/a b.sock:/r/c d"]);
    for op in [
        MuxOp::Check,
        MuxOp::ForwardUnix("/l".into(), "/r".into()),
        MuxOp::CancelUnix("/l".into(), "/r".into()),
    ] {
        let args = mule::transport::mux_args(&host, &op);
        assert!(!args.iter().any(|a| a == "-R"), "{args:?}");
        assert!(args.contains(&"BatchMode=yes".to_string()), "{args:?}");
        assert!(args.contains(&"ProxyCommand=none".to_string()), "{args:?}");
    }
}

#[test]
fn the_master_command_sets_stream_local_bind_unlink() {
    // The master's own setting governs forwards; the client's is ignored
    // (measured). Without it a stale socket makes `-O forward` fail with 255.
    let command = mule::errors::master_command(&host());
    assert!(
        command.contains("-o StreamLocalBindUnlink=yes"),
        "{command}"
    );
    assert!(command.contains("ssh -MNf"), "{command}");
}

/// Run `f` on a thread and report whether it finished within `wait`.
fn finishes_within<T: Send + 'static>(
    wait: std::time::Duration,
    f: impl FnOnce() -> T + Send + 'static,
) -> (bool, std::thread::JoinHandle<T>) {
    let done = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let flag = Arc::clone(&done);
    let handle = std::thread::spawn(move || {
        let out = f();
        flag.store(true, Ordering::SeqCst);
        out
    });
    std::thread::sleep(wait);
    (done.load(Ordering::SeqCst), handle)
}

/// While another holder has LOCAL's lock, `op` must not finish; once released
/// it must. Layer-1 evidence for lock acquisition order.
fn assert_waits_for_the_lock(name: &str, op: impl FnOnce(&Path) + Send + 'static) {
    use std::time::Duration;
    isolate_state();
    let dir = scratch();
    let local = dir.join("lk.sock");
    let lock = lock_local(&host(), &local).unwrap();
    let target = local.clone();
    let (finished, handle) = finishes_within(Duration::from_millis(300), move || op(&target));
    assert!(!finished, "{name} must wait for the LOCAL's lock");
    drop(lock);
    handle.join().unwrap();
}

#[test]
fn forward_waits_for_the_locals_lock() {
    assert_waits_for_the_lock("forward", |local| {
        let fake = Fake::new();
        master(&fake);
        forward(&fake, &host(), &[spec(local, "/r/x")]).unwrap();
    });
}

#[test]
fn unforward_waits_for_the_locals_lock() {
    assert_waits_for_the_lock("unforward", |local| {
        unforward(&Fake::new(), &host(), &[spec(local, "/r/x")]).unwrap();
    });
}

#[test]
fn a_held_lock_on_another_local_does_not_block() {
    use std::time::Duration;
    isolate_state();
    let dir = scratch();
    let _held = lock_local(&host(), &dir.join("other.sock")).unwrap();
    let s = spec(&dir.join("lk.sock"), "/r/x");
    let (finished, handle) = finishes_within(Duration::from_millis(300), move || {
        unforward(&Fake::new(), &host(), &[s]).unwrap()
    });
    assert!(finished, "a different LOCAL's lock must not block this one");
    handle.join().unwrap();
}

#[test]
fn unforward_fails_with_exit_one_on_a_real_unlink_error_and_keeps_the_record() {
    use std::os::unix::fs::PermissionsExt;
    isolate_state();
    let dir = scratch();
    let sub = dir.join("ro");
    std::fs::create_dir(&sub).unwrap();
    let local = sub.join("g.sock");
    let fake = Fake::new();
    master(&fake);
    forward(&fake, &host(), &[spec(&local, "/r/x")]).unwrap();
    std::fs::set_permissions(&sub, std::fs::Permissions::from_mode(0o500)).unwrap();

    let result = unforward(&fake, &host(), &[spec(&local, "/r/x")]);
    let kept = read_record(&host(), &local).is_some();
    let remains = local.exists();
    std::fs::set_permissions(&sub, std::fs::Permissions::from_mode(0o700)).unwrap();

    if !remains {
        eprintln!("skipping: permissions are not enforced here (root?)");
        return;
    }
    let error = result.expect_err("a failed unlink is not success");
    assert_eq!(exit_code(&error), 1);
    assert!(format!("{error:#}").contains("g.sock"), "{error:#}");
    assert!(kept, "the record is the only trace of what is left");

    // And once the cause is fixed, the same call finishes the job.
    unforward(&fake, &host(), &[spec(&local, "/r/x")]).unwrap();
    assert!(!local.exists());
    assert!(read_record(&host(), &local).is_none());
}
