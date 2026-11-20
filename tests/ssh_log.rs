//! The per-call `ssh -E` log lives in a private directory, never a shared one.
//!
//! The log is what `Transport::run`'s refusal proof reads (SPEC.md, Recovery
//! exception): a file another user could plant, swap or read under a shared
//! temp dir would let them drive or observe that proof. So the directory is
//! checked, not just created, and a directory that is not ours and 0700 is
//! refused.

use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::PathBuf;

use mule::transport::SshLog;

fn scratch(name: &str) -> PathBuf {
    let root = std::path::Path::new(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("mule-ssh-log-{}-{name}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
    root
}

#[test]
fn the_log_sits_in_a_0700_directory_owned_by_the_user() {
    let root = scratch("fresh");
    let dir = root.join("ssh");
    let log = SshLog::create_in(&dir).unwrap();
    let parent = log.path().parent().unwrap();
    assert_eq!(parent, dir);
    let meta = std::fs::symlink_metadata(parent).unwrap();
    assert!(meta.is_dir());
    assert_eq!(meta.permissions().mode() & 0o777, 0o700);
    assert_eq!(meta.uid(), unsafe { libc::geteuid() });
    drop(log);
    std::fs::remove_dir_all(&root).unwrap();
}

#[test]
fn a_planted_world_writable_directory_is_refused() {
    let root = scratch("planted");
    let dir = root.join("ssh");
    std::fs::create_dir(&dir).unwrap();
    std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o777)).unwrap();
    assert!(SshLog::create_in(&dir).is_err());
    // Refused, not repaired: nothing was written into it.
    assert_eq!(std::fs::read_dir(&dir).unwrap().count(), 0);
    std::fs::remove_dir_all(&root).unwrap();
}

#[test]
fn a_symlink_in_place_of_the_directory_is_refused() {
    let root = scratch("symlink");
    let target = root.join("elsewhere");
    std::fs::create_dir(&target).unwrap();
    std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o700)).unwrap();
    let dir = root.join("ssh");
    std::os::unix::fs::symlink(&target, &dir).unwrap();
    assert!(SshLog::create_in(&dir).is_err());
    assert_eq!(std::fs::read_dir(&target).unwrap().count(), 0);
    std::fs::remove_dir_all(&root).unwrap();
}

#[test]
fn a_group_writable_ancestor_is_refused() {
    let root = scratch("ancestor");
    let mid = root.join("mid");
    std::fs::create_dir(&mid).unwrap();
    std::fs::set_permissions(&mid, std::fs::Permissions::from_mode(0o775)).unwrap();
    let dir = mid.join("ssh");
    let err = SshLog::create_in(&dir).err().expect("must fail closed");
    assert!(err.to_string().contains(&*mid.to_string_lossy()), "{err}");
    std::fs::set_permissions(&mid, std::fs::Permissions::from_mode(0o755)).unwrap();
    std::fs::remove_dir_all(&root).unwrap();
}

#[test]
fn a_root_owned_sticky_world_writable_ancestor_is_accepted() {
    let tmp = std::path::Path::new("/tmp");
    let m = std::fs::metadata(tmp).unwrap();
    if m.uid() != 0 || m.mode() & 0o1002 != 0o1002 {
        eprintln!("skip: /tmp is not a root-owned sticky world-writable dir");
        return;
    }
    let root = tmp.join(format!("mule-ssh-log-sticky-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir(&root).unwrap();
    let res = SshLog::create_in(&root.join("ssh"));
    std::fs::remove_dir_all(&root).unwrap();
    res.expect("sticky root-owned /tmp ancestor must pass");
}
