//! Explicit local setup. mu owns its registry; use only its public CLI.

use std::io::{self, Write};
use std::os::unix::fs::MetadataExt;
use std::path::Path;
use std::process::{Command, Output, Stdio};

use anyhow::{Context, Result, anyhow, bail, ensure};
use serde::Deserialize;
use serde_json::{Value, json};

use crate::cli::shell_quote;
use crate::config::{self, Config};

const EXIT_REGISTRATION: i32 = 9;
const PLUGIN: &str = "mu-plugin-mule";

pub(crate) fn run(config_path: Option<&Path>, quiet: bool) -> Result<i32> {
    let path = match config_path {
        Some(path) => std::path::absolute(path)?,
        None => config::default_path()?,
    };
    let explicit = config_path.map(|_| path.as_path());
    let init = format!("mule init{}", config_arg(explicit));
    if config::seed(&path).with_context(|| {
        format!(
            "next: check that you can create {} and rerun `{init}`",
            path.display()
        )
    })? {
        writeln!(io::stdout(), "wrote {}", path.display())?;
    }
    let config = Config::load_for_init(&path)
        .with_context(|| format!("next: fix {} and rerun `{init}`", path.display()))?;
    for host in config.hosts() {
        writeln!(
            io::stdout(),
            "{}: {}",
            host.name,
            crate::errors::master_command(host)
        )?;
    }

    let mu = crate::helper::mu_on_path();
    if let Some(mu) = mu {
        if let Err(e) = register(mu, explicit) {
            let _ = writeln!(io::stderr(), "mule: {e:#}");
            return Ok(EXIT_REGISTRATION);
        }
    } else {
        writeln!(io::stdout(), "mu not on PATH; skipping plugin registration")?;
    }

    if !quiet {
        let hint = if config.hosts().is_empty() {
            format!(
                "edit {} to configure a host, then rerun `{init}`",
                path.display()
            )
        } else if mu.is_some() {
            "open the needed masters; add each mu host with `mu host add <name> --plugin mule --fetch '<fetch command>'`".into()
        } else {
            "open the needed masters, then try `mule host list`".into()
        };
        let _ = writeln!(io::stderr(), "next: {hint}");
    }
    Ok(0)
}

#[derive(Deserialize)]
struct RegistryReply {
    ok: bool,
    data: Registry,
}

#[derive(Deserialize)]
struct Registry {
    items: Vec<Registration>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Registration {
    name: String,
    argv: Vec<String>,
    version: String,
    schema_digest: String,
}

fn register(mu: &Path, config: Option<&Path>) -> Result<()> {
    // The npm launcher runs the native mule from its platform package. Only
    // use the companion beside that binary, not some other install on PATH.
    let mule = std::env::current_exe()?.canonicalize()?;
    let plugin = mule.with_file_name(PLUGIN);
    let meta = std::fs::metadata(&plugin).with_context(|| {
        format!("missing companion {}\nnext: install a mule package that includes both binaries and rerun `mule init`", plugin.display())
    })?;
    check_trust(&mule, &plugin, &meta, config)?;

    let utf8 = |p: &Path| {
        p.to_str().map(str::to_owned).ok_or_else(|| {
            anyhow!("mu cannot register a non-UTF-8 path\nnext: move {} to a UTF-8 path and rerun `mule init`", p.display())
        })
    };
    let mut argv = vec![utf8(&plugin)?];
    if let Some(config) = config {
        argv.extend(["--config".into(), utf8(config)?]);
    }

    let registered = mu_output(mu, &["plugin", "list", "--json"])
        .and_then(|out| {
            let reply: RegistryReply =
                serde_json::from_slice(&out).context("reading mu's plugin list")?;
            ensure!(reply.ok, "mu plugin list did not report success");
            Ok(reply.data)
        })
        .context("next: check that `mu plugin list --json` works, then rerun `mule init`")?;
    if let Some(row) = registered.items.into_iter().find(|row| row.name == "mule") {
        // mu re-checks the same argv, but will not replace it. In particular,
        // do not delete a manual registration (it may back live hosts/jobs).
        ensure!(
            row.argv == argv,
            "mu already has a mule plugin with different arguments: {:?}\nnext: keep that registration, or remove it with `mu plugin rm mule` once unused and rerun `mule init`",
            row.argv
        );
        let (version, schema) = installed_metadata(&argv)
            .context("next: check the installed mule/plugin pair and rerun `mule init`")?;
        if row.version == version && row.schema_digest == schema {
            return Ok(());
        }
    }
    let mut args = vec!["plugin", "add", "mule", "--"];
    args.extend(argv.iter().map(String::as_str));
    mu_output(mu, &args).context("next: fix the `mu plugin add` error and rerun `mule init`")?;
    writeln!(io::stdout(), "registered mule with mu")?;
    Ok(())
}

fn mu_output(mu: &Path, args: &[&str]) -> Result<Vec<u8>> {
    let out = Command::new(mu).args(args).stdin(Stdio::null()).output()?;
    checked_output(&format!("mu {} {}", args[0], args[1]), out)
}

fn checked_output(command: &str, out: Output) -> Result<Vec<u8>> {
    ensure!(
        out.status.success(),
        "{command} failed ({}): {}",
        out.status,
        String::from_utf8_lossy(&out.stderr).trim()
    );
    Ok(out.stdout)
}

fn installed_metadata(argv: &[String]) -> Result<(String, String)> {
    // Compare what this installed binary actually declares with what mu last
    // checked. Comparing to mule's own version would miss version skew.
    let mut child = Command::new(&argv[0])
        .args(&argv[1..])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .context("starting mu-plugin-mule for its handshake")?;
    let write = writeln!(
        child.stdin.take().expect("piped stdin"),
        "{}",
        json!({"jsonrpc":"2.0", "id":1, "method":"initialize",
               "params":{"protocol":{"min":1,"max":1},"mu":"mule init"}})
    ); // EOF after the one request also shuts down the child.
    let out = checked_output(PLUGIN, child.wait_with_output()?)?;
    write?;
    let msg: Value = serde_json::from_slice(&out).context("reading mu-plugin-mule handshake")?;
    let result = &msg["result"];
    ensure!(
        msg["jsonrpc"] == "2.0" && msg["id"] == 1 && result["plugin"]["name"] == "mule",
        "unexpected mu-plugin-mule handshake reply"
    );
    let version = result["plugin"]["version"].as_str();
    let schema = result["schema"].as_str();
    match (version, schema) {
        (Some(version), Some(schema)) => Ok((version.into(), schema.into())),
        _ => bail!("mu-plugin-mule handshake lacks version or schema"),
    }
}

fn check_trust(
    mule: &Path,
    plugin: &Path,
    meta: &std::fs::Metadata,
    config: Option<&Path>,
) -> Result<()> {
    let path = plugin.display();
    ensure!(
        meta.is_file(),
        "{path} is not a regular file\nnext: reinstall mule with its companion binary"
    );
    ensure!(
        meta.mode() & 0o022 == 0,
        "{path} is group- or world-writable\nnext: reinstall mule into a user-owned directory with both binaries mode 0755"
    );
    // Match mu's getuid(), including when the path names a symlink.
    if meta.uid() == unsafe { libc::getuid() } {
        ensure!(
            meta.mode() & 0o100 != 0,
            "{path} is not executable by you\nnext: run `chmod u+x {}` and rerun `mule init`",
            shell_quote(&plugin.to_string_lossy())
        );
        return Ok(());
    }
    let config_arg = config_arg(config);
    // Keep both binaries together so the copied plugin runs the matching mule
    // rather than falling back to whichever mule happens to be on mu's PATH.
    bail!(
        "{path} is not owned by you; mu won't register it\nnext: as your normal user, install a private copy of both binaries:\n  \
         mkdir -p -m 0700 \"$HOME/.local/libexec/mule\"\n  \
         install -m 0755 {} {} \"$HOME/.local/libexec/mule/\"\n  \
         \"$HOME/.local/libexec/mule/mule\" init{config_arg}",
        shell_quote(&mule.to_string_lossy()),
        shell_quote(&plugin.to_string_lossy())
    );
}

fn config_arg(config: Option<&Path>) -> String {
    config.map_or_else(String::new, |path| {
        format!(" --config {}", shell_quote(&path.to_string_lossy()))
    })
}
