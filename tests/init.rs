mod common;

use std::fs;
use std::os::unix::fs::{MetadataExt, PermissionsExt, symlink};
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use common::local::{SPAWN_GATE, scratch, write_exec};

struct Install(PathBuf);

impl Install {
    fn new() -> Self {
        let root = scratch("init").canonicalize().unwrap();
        fs::create_dir(root.join("bin")).unwrap();
        fs::copy(env!("CARGO_BIN_EXE_mule"), root.join("bin/mule")).unwrap();
        Self(root)
    }

    fn script(&self, name: &str, body: &str) {
        let path = self.0.join("bin").join(name);
        write_exec(&path, format!("#!/bin/sh\nset -eu\n{body}\n")).unwrap();
        fs::set_permissions(path, fs::Permissions::from_mode(0o755)).unwrap();
    }

    fn config(&self) -> PathBuf {
        self.0.join(".config/mule/config.toml")
    }

    fn meta(&self, version: &str, schema: &str) {
        fs::write(self.0.join("version"), version).unwrap();
        fs::write(self.0.join("schema"), schema).unwrap();
    }

    fn fake_mu(&self) {
        self.script(
            "mu",
            r#"
case "$*" in
    'plugin list --json')
        if [ -f "$HOME/registry" ]; then /bin/cat "$HOME/registry";
        else printf '{"ok":true,"data":{"items":[],"count":0},"next":null}\n'; fi ;;
    'plugin add mule -- '*)
        [ "$#" = 5 ]
        [ "$5" = "$HOME/bin/mu-plugin-mule" ]
        version=$(/bin/cat "$HOME/version")
        schema=$(/bin/cat "$HOME/schema")
        printf '{"ok":true,"data":{"items":[{"name":"mule","argv":["%s"],"version":"%s","schemaDigest":"%s"}],"count":1},"next":null}\n' \
            "$5" "$version" "$schema" > "$HOME/registry"
        echo add >> "$HOME/adds" ;;
    *) echo "unexpected mu call: $*" >&2; exit 42 ;;
esac
"#,
        );
    }

    fn init(&self, args: &[&str]) -> Output {
        // A parallel test may be writing an executable; don't inherit its fd.
        let _gate = SPAWN_GATE.read().unwrap();
        Command::new(self.0.join("bin/mule"))
            .arg("init")
            .args(args)
            .env_clear()
            .env("HOME", &self.0)
            .env("PATH", self.0.join("bin"))
            .output()
            .unwrap()
    }
}

impl Drop for Install {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn success(out: Output) -> String {
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(out.status.success(), "{}: {stderr}", out.status);
    assert!(stderr.contains("next:"), "{stderr}");
    String::from_utf8(out.stdout).unwrap()
}

#[test]
fn no_mu_seeds_once_without_needing_the_plugin() {
    let h = Install::new();
    let out = success(h.init(&[]));
    assert_eq!(
        out.lines().filter(|l| l.contains("mu not on PATH")).count(),
        1
    );
    let seeded = fs::read_to_string(h.config()).unwrap();
    assert_eq!(seeded, mule::config::TEMPLATE);
    let master = seeded
        .lines()
        .find(|line| line.starts_with("#   ssh -MNf "))
        .expect("the template has a master setup example");
    assert!(
        master.contains("-o StreamLocalBindUnlink=yes"),
        "template master must replace stale forward sockets: {master}"
    );
    let edited = "# keep this\n";
    fs::write(h.config(), edited).unwrap();
    success(h.init(&[]));
    assert_eq!(fs::read_to_string(h.config()).unwrap(), edited);
}

#[test]
fn prints_each_master_command_without_running_ssh() {
    let h = Install::new();
    h.script("ssh", "echo ssh >> \"$HOME/ssh-called\"; exit 42");
    let path = h.0.join("custom.toml");
    let config = "[hosts.dev]\ntarget = 'build'\n[hosts.box]\n";
    fs::write(&path, config).unwrap();
    let out = success(h.init(&["--config", path.to_str().unwrap()]));
    assert_eq!(out.matches("ssh -MNf").count(), 2, "{out}");
    assert!(
        out.contains("dev.sock") && out.contains("box.sock"),
        "{out}"
    );
    assert!(!h.0.join("ssh-called").exists());
    assert!(!h.config().exists());
    assert_eq!(fs::read_to_string(path).unwrap(), config);
}

#[test]
fn adds_on_first_run_and_refreshes_only_when_version_or_schema_changes() {
    let h = Install::new();
    h.fake_mu();
    h.script(
        "mu-plugin-mule",
        r#"
IFS= read -r request
case "$request" in *initialize*) ;; *) exit 42 ;; esac
version=$(/bin/cat "$HOME/version")
schema=$(/bin/cat "$HOME/schema")
printf '{"jsonrpc":"2.0","id":1,"result":{"plugin":{"name":"mule","version":"%s"},"schema":"%s"}}\n' "$version" "$schema"
"#,
    );
    let adds = h.0.join("adds");
    for (version, schema, expected_adds) in [
        ("1.0.0", "sha256:one", 1),
        ("1.0.0", "sha256:one", 1),
        ("1.1.0", "sha256:one", 2),
        ("1.1.0", "sha256:two", 3),
        ("1.1.0", "sha256:two", 3),
    ] {
        h.meta(version, schema);
        success(h.init(&[]));
        assert_eq!(
            fs::read_to_string(&adds).unwrap().lines().count(),
            expected_adds
        );
    }
    let row: serde_json::Value =
        serde_json::from_slice(&fs::read(h.0.join("registry")).unwrap()).unwrap();
    let row = &row["data"]["items"][0];
    assert_eq!(row["name"], "mule");
    assert_eq!(row["version"], "1.1.0");
    assert_eq!(row["schemaDigest"], "sha256:two");
}

#[test]
fn refuses_a_root_owned_target_with_a_copy_remedy() {
    let root_owned = Path::new("/bin/sh");
    if unsafe { libc::getuid() } == fs::metadata(root_owned).unwrap().uid() {
        // A root-run suite cannot use this system executable as a foreign owner.
        return;
    }
    let h = Install::new();
    h.fake_mu();
    symlink(root_owned, h.0.join("bin/mu-plugin-mule")).unwrap();
    let out = h.init(&[]);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert_eq!(out.status.code(), Some(9), "{stderr}");
    assert!(stderr.contains("not owned by you"), "{stderr}");
    assert!(
        stderr.contains("install -m 0755") && stderr.contains("next:"),
        "{stderr}"
    );
    assert!(!h.0.join("adds").exists());
}

#[test]
fn refuses_group_writable_plugin() {
    let h = Install::new();
    h.fake_mu();
    h.script("mu-plugin-mule", "exit 42");
    fs::set_permissions(
        h.0.join("bin/mu-plugin-mule"),
        fs::Permissions::from_mode(0o775),
    )
    .unwrap();
    let out = h.init(&[]);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert_eq!(out.status.code(), Some(9), "{stderr}");
    assert!(stderr.contains("group- or world-writable"), "{stderr}");
    assert!(!h.0.join("adds").exists());
}
