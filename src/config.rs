//! Host configuration: a hand-edited TOML file, not a state database.
//!
//! This reverses an early instinct to copy murmur's `peers` table. murmur's
//! peers carry *discovered* state (snapshots, `fetched_at`, `last_error`),
//! which is why they need a store. coop's hosts are pure user intent, so a
//! file is editable, diffable, and needs no migration story.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow, bail};
use serde::Deserialize;

/// Warn past this many running jobs on one host. Backpressure, not a queue.
const DEFAULT_MAX_RUNNING: u32 = 4;

/// Prune `done` jobs older than this. Generous on purpose: deleting a log
/// someone still wants costs more than the disk it saves.
const DEFAULT_KEEP_DAYS: u32 = 14;

/// The private tmux server name. Jobs run under `tmux -L coop`, which does not
/// appear in the user's `tmux ls`.
const DEFAULT_TMUX_SOCKET: &str = "coop";

/// A configured host, after name-derived defaults have been applied.
///
/// Every field is resolved here so no downstream code has to know a default:
/// `socket` is `~`-expanded because it is handed to `ssh -S`, which does not
/// expand it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Host {
    /// The config section name, and the handle the user passes to `--host`.
    pub name: String,
    /// The ssh target. Defaults to `name`.
    pub target: String,
    /// coop's *private* `ControlPath`. Everything else on the machine uses the
    /// default `~/.ssh/control/...` and so cannot contend with it.
    pub socket: PathBuf,
    /// `tmux -L <this>`: a private server, invisible to the user's `tmux ls`.
    pub tmux_socket: String,
    /// Warn past this count; never block.
    pub max_running: u32,
    /// Where `run` starts, unless `--cwd`. `None` means the remote `$HOME`.
    pub default_cwd: Option<String>,
    /// Prune horizon for `done` jobs.
    pub keep_days: u32,
}

/// The raw `[hosts.<name>]` table. Everything is optional; `Host` fills in the
/// defaults that serde cannot, because serde cannot see the section name.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawHost {
    target: Option<String>,
    socket: Option<String>,
    tmux_socket: Option<String>,
    max_running: Option<u32>,
    default_cwd: Option<String>,
    keep_days: Option<u32>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawConfig {
    /// `BTreeMap` rather than `HashMap` so `coop host list` does not reshuffle
    /// between invocations.
    #[serde(default)]
    hosts: BTreeMap<String, RawHost>,
}

#[derive(Debug, Clone)]
pub struct Config {
    hosts: Vec<Host>,
}

/// Expand a leading `~/` against the real home directory.
///
/// Only a leading `~/` (or a bare `~`): `~user` is deliberately unsupported,
/// since resolving another user's home is a different problem and silently
/// treating it as a literal path would be worse than refusing.
fn expand_tilde(raw: &str) -> Result<PathBuf> {
    let Some(rest) = raw.strip_prefix('~') else {
        return Ok(PathBuf::from(raw));
    };
    if !(rest.is_empty() || rest.starts_with('/')) {
        bail!("cannot expand {raw:?}: only a leading `~/` is supported, not `~user`");
    }
    let home = directories::BaseDirs::new()
        .ok_or_else(|| anyhow!("cannot locate the home directory to expand {raw:?}"))?
        .home_dir()
        .to_path_buf();
    Ok(home.join(rest.trim_start_matches('/')))
}

/// Where the config lives: `~/.config/coop/config.toml`.
/// Written to the config path the first time coop runs without one.
///
/// Every host is commented out, so the file is a prompt rather than a guess:
/// coop cannot know a host name, and inventing one would produce confusing
/// failures against a target that does not exist.
pub const TEMPLATE: &str = "\
# coop hosts. Uncomment and edit -- the section name is what you pass to --host.
#
# One block per host. `target` is the only key worth setting by hand; every
# other line below shows its default and can stay commented out.
#
# [hosts.build]
# target      = \"build\"                    # ssh target (default: section name)
# socket      = \"~/.ssh/coop/build.sock\"   # coop's own ControlPath
# tmux_socket = \"coop\"                     # private tmux server
# max_running = 4                          # warn past this; not a queue
# default_cwd = \"~/work\"                   # where `run` starts, unless --cwd
# keep_days   = 14                         # prune finished jobs older than this
#
# Then open the control master, once per ControlPersist window. This may ask
# you to touch a hardware key; coop cannot do it for you:
#
#   ssh -MNf -S ~/.ssh/coop/build.sock -o ControlPersist=8h build
";

/// Write [`TEMPLATE`] to `path` unless something is already there.
///
/// Returns whether it created the file. Uses `create_new`, so a race with
/// another coop process cannot clobber a real config.
pub fn seed(path: &Path) -> Result<bool> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating {}", parent.display()))?;
    }
    match std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
    {
        Ok(mut file) => {
            use std::io::Write;
            file.write_all(TEMPLATE.as_bytes())
                .with_context(|| format!("writing {}", path.display()))?;
            Ok(true)
        }
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => Ok(false),
        Err(e) => Err(e).with_context(|| format!("creating {}", path.display())),
    }
}

pub fn default_path() -> Result<PathBuf> {
    let dirs =
        directories::BaseDirs::new().ok_or_else(|| anyhow!("cannot locate a home directory"))?;
    // `config_dir()` is `~/Library/Application Support` on macOS, which is not
    // where a hand-edited dotfile belongs. coop is a terminal tool, so it uses
    // the XDG layout on every platform and stays greppable.
    let base = std::env::var_os("XDG_CONFIG_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| dirs.home_dir().join(".config"));
    Ok(base.join("coop").join("config.toml"))
}

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("reading config {}", path.display()))?;
        Self::parse(&text).with_context(|| format!("in config {}", path.display()))
    }

    pub fn parse(text: &str) -> Result<Self> {
        let raw: RawConfig = toml::from_str(text)?;
        if raw.hosts.is_empty() {
            // Reached via the template, whose hosts are all commented out, so
            // point at the two lines that actually turn it into a config.
            bail!(
                "no hosts configured\n  \
                 uncomment a block, or add:\n\n    \
                 [hosts.dev]\n    target = \"dev\""
            );
        }
        let hosts = raw
            .hosts
            .into_iter()
            .map(|(name, h)| {
                let tmux_socket = h
                    .tmux_socket
                    .unwrap_or_else(|| DEFAULT_TMUX_SOCKET.to_string());
                // Interpolated unquoted into every remote script, so a socket
                // name containing shell syntax would be command injection from
                // a config file. tmux socket names are a filename component,
                // so this grammar loses nothing real.
                if tmux_socket.is_empty()
                    || !tmux_socket
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_' || b == b'.')
                {
                    bail!(
                        "host {name:?}: invalid tmux_socket {tmux_socket:?}; \
                         use letters, digits, dot, dash or underscore"
                    );
                }
                let socket = match h.socket {
                    Some(s) => expand_tilde(&s)?,
                    None => expand_tilde(&format!("~/.ssh/coop/{name}.sock"))?,
                };
                Ok(Host {
                    target: h.target.unwrap_or_else(|| name.clone()),
                    socket,
                    tmux_socket,
                    max_running: h.max_running.unwrap_or(DEFAULT_MAX_RUNNING),
                    default_cwd: h.default_cwd,
                    keep_days: h.keep_days.unwrap_or(DEFAULT_KEEP_DAYS),
                    name,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self { hosts })
    }

    pub fn hosts(&self) -> &[Host] {
        &self.hosts
    }

    /// Resolve `--host`. `None` is the single configured host, or an error that
    /// names the choices — the fix is one flag away, so the user should never
    /// have to open the config to learn the names.
    pub fn host(&self, name: Option<&str>) -> Result<&Host> {
        let names = || {
            self.hosts
                .iter()
                .map(|h| h.name.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        };
        match name {
            Some(n) => self
                .hosts
                .iter()
                .find(|h| h.name == n)
                .ok_or_else(|| anyhow!("unknown host {n:?}; configured: {}", names())),
            None if self.hosts.len() == 1 => Ok(&self.hosts[0]),
            None => Err(anyhow!(
                "several hosts configured; pass --host <name>: {}",
                names()
            )),
        }
    }
}
