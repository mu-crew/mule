//! Test layer 1 for `mule run --lease --attach <id>`: the logic, over `Fake`.
//!
//! Spec 2026-11-04-lease-adopt § mule_attach, steps 1-5: no master is exit 3
//! with nothing written; the stop fence is locked (waiting out a stop attempt
//! in flight) before the one probe; anything but Running is exit 1 with the
//! fence and the job file untouched; the job file is published without
//! replacing anything; the bump happens, and the carrier owns the bumped
//! generation, before the fence is released. `tests/local_sshd.rs` drives the
//! real binary for what a `Fake` cannot see.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use mule::config::{Config, Host};
use mule::errors::{MuleError, exit_code};
use mule::lease::{
    Hold, Owned, Published, attach, hold, publish_job_file, publish_job_file_with,
    stop_failed_path, stop_retry,
};
use mule::stopfence::StopFence;
use mule::transport::{Fake, Output};
use mule::wrapper::JobId;

fn isolate_state() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let dir = std::env::temp_dir().join(format!("mule-attach-state-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        // Safe: runs once, before any thread reads it.
        unsafe { std::env::set_var("XDG_STATE_HOME", &dir) };
    });
}

fn host() -> Host {
    isolate_state();
    Config::parse("[hosts.attach]\ntarget = \"h\"\n")
        .unwrap()
        .host(None)
        .unwrap()
        .clone()
}

fn id() -> JobId {
    "a1b2c3".parse().unwrap()
}

fn other() -> JobId {
    "ffff00".parse().unwrap()
}

fn scratch() -> PathBuf {
    static SEQ: AtomicU32 = AtomicU32::new(0);
    let dir = std::env::temp_dir().join(format!(
        "mule-attach-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn reply(exists: u8, rc: &str, alive: u8) -> Output {
    Output::ok(format!(
        "exists={exists}\nrc={rc}\nalive={alive}\nsize=0\nbytes:\n"
    ))
}

fn running() -> Output {
    reply(1, "", 1)
}

/// A fence at `generation` under `dir`, as an old carrier left it.
fn fence_at(dir: &Path, generation: u64) -> StopFence {
    let fence = StopFence::at(dir.join("stopping").join(id().as_str()));
    let mut lock = fence.lock().unwrap();
    for _ in 0..generation {
        lock.bump().unwrap();
    }
    fence
}

/// `attach` with a recording `on_owned`.
fn attach_recording(
    fake: &Fake,
    job_file: &Path,
    fence: &StopFence,
) -> (anyhow::Result<Owned>, Vec<Owned>) {
    let seen = Mutex::new(Vec::new());
    let result = attach(fake, &host(), &id(), job_file, fence, &|owned| {
        seen.lock().unwrap().push(owned.clone())
    });
    (result, seen.into_inner().unwrap())
}

#[test]
fn a_running_job_is_held_its_job_file_written_and_its_fence_bumped() {
    let (dir, fake) = (scratch(), Fake::new());
    let (file, fence) = (dir.join("w.job"), fence_at(&dir, 2));
    fake.push(running());

    let (owned, seen) = attach_recording(&fake, &file, &fence);

    let owned = owned.unwrap();
    assert_eq!(
        owned,
        Owned {
            id: id(),
            fence_gen: 3
        }
    );
    assert_eq!(seen, vec![owned.clone()], "handed to the signal state once");
    assert_eq!(std::fs::read_to_string(&file).unwrap(), "a1b2c3\n");
    use std::os::unix::fs::PermissionsExt;
    let mode = std::fs::metadata(&file).unwrap().permissions().mode() & 0o777;
    assert_eq!(mode, 0o600);
    let names: Vec<_> = std::fs::read_dir(&dir)
        .unwrap()
        .map(|e| e.unwrap().file_name())
        .collect();
    assert_eq!(
        names.len(),
        2,
        "the job file and the fence dir, no temp: {names:?}"
    );
    assert_eq!(fence.generation_unlocked().unwrap(), 3);
    let scripts = fake.scripts();
    assert_eq!(scripts.len(), 1, "one probe, no dispatch: {scripts:?}");
    assert!(!scripts[0].contains("new-session"), "nothing is started");

    // Then it holds like a lease: the job's own code at its end.
    fake.push(running()).push(reply(1, "7", 0));
    let hold_for = Hold {
        host: &host(),
        id: &owned.id,
        specs: &[],
        mirror_for: Duration::from_secs(3600),
    };
    assert_eq!(hold(&fake, &hold_for, &mut |_| {}, &mut std::io::sink()), 7);
}

#[test]
fn a_job_that_is_not_running_is_exit_one_with_nothing_touched() {
    for (reply, state) in [
        (reply(1, "0", 0), "done 0"),
        (reply(0, "", 0), "missing"),
        (reply(1, "", 0), "orphan"),
    ] {
        let (dir, fake) = (scratch(), Fake::new());
        let (file, fence) = (dir.join("w.job"), fence_at(&dir, 1));
        fake.push(reply);

        let (result, seen) = attach_recording(&fake, &file, &fence);

        let error = result.unwrap_err();
        assert_eq!(
            format!("{error:#}"),
            format!("job a1b2c3 is not running ({state})")
        );
        assert_eq!(exit_code(&error), 1);
        assert!(seen.is_empty(), "{state}: owns nothing, so stops nothing");
        assert_eq!(fake.scripts().len(), 1, "{state}: the probe only, no rm");
        assert!(!file.exists(), "{state}: no job file");
        assert_eq!(fence.generation_unlocked().unwrap(), 1, "{state}");
    }
}

#[test]
fn a_probe_that_fails_is_exit_one_with_nothing_touched() {
    let (dir, fake) = (scratch(), Fake::new());
    let (file, fence) = (dir.join("w.job"), fence_at(&dir, 0));
    fake.push(Output::fail(1, "boom"));

    let (result, seen) = attach_recording(&fake, &file, &fence);

    let error = result.unwrap_err();
    assert!(
        format!("{error:#}").contains("cannot probe job a1b2c3"),
        "{error:#}"
    );
    assert_eq!(exit_code(&error), 1);
    assert!(seen.is_empty());
    assert!(!file.exists());
    assert_eq!(fence.generation_unlocked().unwrap(), 0);
}

#[test]
fn no_master_is_exit_three_before_the_fence_or_the_job_file() {
    let (dir, fake) = (scratch(), Fake::no_master());
    let file = dir.join("w.job");
    let fence = StopFence::at(dir.join("stopping").join(id().as_str()));

    let (result, seen) = attach_recording(&fake, &file, &fence);

    let error = result.unwrap_err();
    assert!(matches!(
        error.downcast_ref::<MuleError>(),
        Some(MuleError::NoMaster { .. })
    ));
    assert_eq!(exit_code(&error), 3);
    assert!(seen.is_empty());
    assert!(fake.scripts().is_empty());
    assert!(!file.exists());
    assert!(!fence.path().exists(), "the fence is not even created");
}

#[test]
fn a_job_file_naming_another_job_empty_invalid_or_unreadable_is_refused_untouched() {
    for (name, plant) in [
        ("another id", Some("ffff00\n")),
        ("empty", Some("")),
        ("invalid", Some("not a job id\n")),
        ("unreadable", None),
    ] {
        let (dir, fake) = (scratch(), Fake::new());
        let (file, fence) = (dir.join("w.job"), fence_at(&dir, 1));
        match plant {
            Some(text) => std::fs::write(&file, text).unwrap(),
            // A directory: reading it fails whoever runs the test, root too.
            None => std::fs::create_dir(&file).unwrap(),
        }
        fake.push(running());

        let (result, seen) = attach_recording(&fake, &file, &fence);

        let error = result.unwrap_err();
        assert!(
            format!("{error:#}").contains("refusing to attach"),
            "{name}: {error:#}"
        );
        assert_eq!(exit_code(&error), 1, "{name}");
        assert!(seen.is_empty(), "{name}");
        match plant {
            Some(text) => assert_eq!(std::fs::read_to_string(&file).unwrap(), text, "{name}"),
            None => assert!(file.is_dir(), "{name}"),
        }
        assert_eq!(
            fence.generation_unlocked().unwrap(),
            1,
            "{name}: not bumped"
        );
    }
}

#[test]
fn a_job_file_that_already_names_the_job_is_kept() {
    let (dir, fake) = (scratch(), Fake::new());
    let (file, fence) = (dir.join("w.job"), fence_at(&dir, 0));
    std::fs::write(&file, "a1b2c3\n").unwrap();
    use std::os::unix::fs::MetadataExt;
    let before = std::fs::metadata(&file).unwrap().ino();
    fake.push(running());

    let (result, _) = attach_recording(&fake, &file, &fence);

    assert_eq!(result.unwrap().fence_gen, 1);
    assert_eq!(
        std::fs::metadata(&file).unwrap().ino(),
        before,
        "not rewritten"
    );
    assert_eq!(publish_job_file(&file, &id()).unwrap(), Published::Kept);
}

#[test]
fn a_racing_writer_wins_the_publish_and_the_re_read_decides() {
    // Another id appears between the read that found no file and the link:
    // the link fails with EEXIST instead of replacing it, and the re-read
    // refuses.
    let file = scratch().join("w.job");
    let error = publish_job_file_with(&file, &id(), &mut || {
        std::fs::write(&file, "ffff00\n").unwrap()
    })
    .unwrap_err();
    assert!(
        format!("{error:#}").contains("names another job, ffff00"),
        "{error:#}"
    );
    assert_eq!(
        std::fs::read_to_string(&file).unwrap(),
        "ffff00\n",
        "theirs stands"
    );

    // The same id appearing is simply kept.
    let file = scratch().join("w.job");
    let published = publish_job_file_with(&file, &id(), &mut || {
        std::fs::write(&file, "a1b2c3\n").unwrap()
    })
    .unwrap();
    assert_eq!(published, Published::Kept);

    // No temp is left behind by the lost link.
    let dir = file.parent().unwrap();
    assert_eq!(std::fs::read_dir(dir).unwrap().count(), 1);
    assert_eq!(
        publish_job_file(&scratch().join("w.job"), &other()).unwrap(),
        Published::Written
    );
}

#[test]
fn the_signal_state_gets_the_bumped_generation_while_the_fence_is_still_held() {
    // A signal after `named` stops the job fenced with the bumped generation;
    // and `named` is set before the fence is released, so an old stop child
    // that runs its next attempt once the lock goes already finds the bump.
    let (dir, fake) = (scratch(), Fake::new());
    let (file, fence) = (dir.join("w.job"), fence_at(&dir, 4));
    fake.push(running());
    let held_while_named = Mutex::new(None);

    let owned = attach(&fake, &host(), &id(), &file, &fence, &|_| {
        *held_while_named.lock().unwrap() = Some(fence.try_lock().unwrap().is_none());
    })
    .unwrap();

    assert_eq!(*held_while_named.lock().unwrap(), Some(true));
    let failed = stop_failed_path(&file, &owned.id);
    let retry = stop_retry(&owned, &failed, Duration::from_secs(600));
    let command =
        mule::cleanup::rm_command(Path::new("/m"), None, "attach", &owned.id, Some(&retry));
    let args: Vec<_> = command.get_args().map(|a| a.to_str().unwrap()).collect();
    let at = args
        .iter()
        .position(|a| *a == "--fence-gen")
        .expect("fenced");
    assert_eq!(args[at + 1], "5", "the generation it bumped to: {args:?}");
}

#[test]
fn attach_waits_while_a_stop_attempt_holds_the_fence() {
    let (dir, fake) = (Arc::new(scratch()), Arc::new(Fake::new()));
    let fence = fence_at(&dir, 0);
    fake.push(running());
    let attempt = fence.lock().unwrap();

    let attacher = {
        let (dir, fake, fence) = (Arc::clone(&dir), Arc::clone(&fake), fence.clone());
        std::thread::spawn(move || {
            attach(&*fake, &host(), &id(), &dir.join("w.job"), &fence, &|_| {})
        })
    };
    std::thread::sleep(Duration::from_millis(200));
    assert!(!attacher.is_finished(), "attach waits out the attempt");
    assert!(fake.scripts().is_empty(), "and probes only after it");
    assert!(!dir.join("w.job").exists());
    drop(attempt);

    let owned = attacher.join().unwrap().unwrap();
    assert_eq!(owned.fence_gen, 1);
    assert_eq!(fake.scripts().len(), 1);
}
