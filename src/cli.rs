//! The command surface.
//!
//! The command shapes are settled by the spec and declared up front so
//! `--help` stays honest while each implementation lands.

use std::time::{Duration, Instant};

use anyhow::{Result, bail};
use clap::{Args, Parser, Subcommand};

use crate::config::{Config, Host};
use crate::probe::{State, next_interval, probe};
use crate::transport::{Ssh, Transport};

/// Exit code for "no ssh control master".
///
/// 3 matches `mu-sync-dev`, which takes the same posture for the same reason:
/// `ssh -MNf` needs a TTY for a hardware token and cannot prompt from a
/// background call, so a tool that tries anyway fails opaquely.
pub const EXIT_NO_MASTER: i32 = 3;

#[derive(Parser, Debug)]
#[command(
    name = "coop",
    about = "Fire remote jobs down a private ssh channel nothing else can take.",
    long_about = "\
Hand coop a command, get an id back, then poll, wait or tail against that id.
You never see ssh, never see tmux, and never hold a connection.

coop uses its OWN ssh ControlPath, so it cannot contend with git fetch, rsync or
anything else on the default socket. Jobs run detached under a private tmux
server and write their exit code to a file, which is what makes a completion
signal survive a dropped connection.

Two things that surprise people, stated here rather than discovered:

  * coop does NOT open the ssh master. `ssh -MNf` needs a TTY for a hardware
    token and cannot prompt from a background call, so coop exits 3 and prints
    the command to run. Costs one token tap per ControlPersist window.

  * jobs run in a NON-login, NON-interactive shell: no ~/.profile, so no nvm or
    cargo on PATH unless your command sources it. Use --cwd for the directory;
    the rest is yours to arrange."
)]
pub struct Cli {
    /// Config file (default: ~/.config/coop/config.toml)
    #[arg(long, global = true, value_name = "PATH")]
    pub config: Option<std::path::PathBuf>,

    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Args, Debug)]
pub struct HostArg {
    /// Which configured host. Optional when exactly one is configured.
    #[arg(long, value_name = "H")]
    pub host: Option<String>,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Dispatch a command and print its job id
    Run {
        #[command(flatten)]
        host: HostArg,
        /// Directory to run in (default: the host's default_cwd, else $HOME)
        #[arg(long, value_name = "D")]
        cwd: Option<String>,
        /// Block until the job finishes, tailing output, and exit with its code
        #[arg(long)]
        wait: bool,
        /// With --wait: print the log once at the end instead of streaming
        #[arg(long)]
        no_tail: bool,
        /// The command to run
        #[arg(trailing_var_arg = true, required = true)]
        cmd: Vec<String>,
    },
    /// Print state, but no job output; prints nothing from the job; use coop tail <id>
    Poll {
        id: String,
        #[arg(long)]
        json: bool,
    },
    /// Block until done; prints nothing; use coop tail <id>
    Wait {
        id: String,
        #[arg(long, value_name = "S")]
        timeout: Option<u64>,
    },
    /// Print a job's output
    Tail {
        id: String,
        /// Follow until the job finishes
        #[arg(short, long)]
        follow: bool,
    },
    /// List jobs
    Ls {
        #[command(flatten)]
        host: HostArg,
        /// Include finished jobs
        #[arg(long)]
        all: bool,
        #[arg(long)]
        json: bool,
    },
    /// Kill a running job
    Kill { id: String },
    /// Drop a job's state directory
    Rm { id: String },
    /// Inspect configured hosts
    #[command(subcommand)]
    Host(HostCmd),
}

#[derive(Subcommand, Debug)]
pub enum HostCmd {
    /// List configured hosts and whether each has a control master
    List {
        #[arg(long)]
        json: bool,
    },
}

pub fn load_config(path: Option<&std::path::Path>) -> Result<Config> {
    match path {
        Some(p) => Config::load(p),
        None => Config::load(&crate::config::default_path()?),
    }
}

/// `coop host list`.
///
/// A down master is *information* for this verb, not an error: "which of my
/// hosts can I use right now" is the question being asked, so it prints the
/// state and exits 0. Every other verb treats a down master as exit 3.
pub fn host_list(cfg: &Config, t: &dyn Transport, json: bool) -> Result<()> {
    let rows: Vec<(&crate::config::Host, bool)> =
        cfg.hosts().iter().map(|h| (h, t.master_alive(h))).collect();

    if json {
        // Hand-rolled rather than pulling in serde_json for one object: the
        // shape is three flat fields and this keeps the dependency list short.
        let items: Vec<String> = rows
            .iter()
            .map(|(h, up)| {
                format!(
                    r#"{{"name":"{}","target":"{}","socket":"{}","master":{}}}"#,
                    h.name,
                    h.target,
                    h.socket.display(),
                    up
                )
            })
            .collect();
        println!(
            "{{\"items\":[{}],\"count\":{}}}",
            items.join(","),
            rows.len()
        );
        return Ok(());
    }

    let name_w = rows.iter().map(|(h, _)| h.name.len()).max().unwrap_or(4);
    let target_w = rows.iter().map(|(h, _)| h.target.len()).max().unwrap_or(6);
    for (h, up) in &rows {
        println!(
            "{:<name_w$}  {:<4}  {:<target_w$}  {}",
            h.name,
            if *up { "up" } else { "down" },
            h.target,
            h.socket.display(),
        );
    }
    if rows.iter().any(|(_, up)| !up) {
        eprintln!(
            "\nsome hosts have no control master. coop cannot open one \
             (ssh -MNf needs a TTY for a hardware token):"
        );
        for (h, up) in &rows {
            if !up {
                eprintln!(
                    "  ssh -MNf -S {} -o ControlPersist=8h {}",
                    h.socket.display(),
                    h.target
                );
            }
        }
    }
    Ok(())
}

pub fn poll(t: &dyn Transport, host: &Host, id: &str, json: bool) -> Result<i32> {
    let result = probe(t, host, id, 0)?;
    if json {
        let (state, rc) = match result.state {
            State::Running => ("running", "null".to_string()),
            State::Done(code) => ("done", code.to_string()),
            State::Orphan => ("orphan", "null".to_string()),
        };
        println!(
            "{{\"state\":\"{state}\",\"rc\":{rc},\"log_size\":{}}}",
            result.log_size
        );
    } else {
        match result.state {
            State::Running => println!("running"),
            State::Done(code) => println!("{code}"),
            State::Orphan => println!("orphan"),
        }
    }
    Ok(0)
}

pub fn wait(t: &dyn Transport, host: &Host, id: &str, timeout: Option<u64>) -> Result<i32> {
    let started = Instant::now();
    let timeout = timeout.map(Duration::from_secs);
    let mut interval = Duration::from_secs(1);
    let mut offset = 0;
    loop {
        let result = probe(t, host, id, offset)?;
        match result.state {
            State::Done(code) => return Ok(code),
            State::Orphan => bail!("job {id} is orphaned; no rc will ever arrive"),
            State::Running => {}
        }
        if timeout.is_some_and(|limit| started.elapsed() >= limit) {
            bail!("timed out waiting for job {id}; it is still running");
        }
        let new_bytes = !result.bytes.is_empty();
        offset = offset.saturating_add(result.bytes.len() as u64);
        let sleep = timeout
            .map(|limit| interval.min(limit.saturating_sub(started.elapsed())))
            .unwrap_or(interval);
        std::thread::sleep(sleep);
        interval = next_interval(interval, new_bytes);
    }
}

pub fn dispatch(cli: Cli) -> Result<i32> {
    let cfg = load_config(cli.config.as_deref())?;
    match cli.command {
        Commands::Run {
            host,
            cwd,
            wait,
            no_tail,
            cmd,
        } => {
            if wait || no_tail {
                anyhow::bail!("`run --wait` is not implemented yet");
            }
            let cfg = load_config(cli.config.as_deref())?;
            let host = cfg.host(host.host.as_deref())?;
            match crate::run::dispatch(&Ssh, host, &cmd.join(" "), cwd.as_deref()) {
                Ok(id) => {
                    println!("{id}");
                    Ok(0)
                }
                Err(error) if error.downcast_ref::<crate::run::NoMaster>().is_some() => {
                    eprintln!("coop: {error}");
                    Ok(EXIT_NO_MASTER)
                }
                Err(error) => Err(error),
            }
        }
        Commands::Host(HostCmd::List { json }) => {
            host_list(&cfg, &Ssh, json)?;
            Ok(0)
        }
        Commands::Poll { id, json } => poll(&Ssh, cfg.host(None)?, &id, json),
        Commands::Wait { id, timeout } => wait(&Ssh, cfg.host(None)?, &id, timeout),
        other => anyhow::bail!("`{}` is not implemented yet", verb_of(&other)),
    }
}

fn verb_of(c: &Commands) -> &'static str {
    match c {
        Commands::Run { .. } => "run",
        Commands::Poll { .. } => "poll",
        Commands::Wait { .. } => "wait",
        Commands::Tail { .. } => "tail",
        Commands::Ls { .. } => "ls",
        Commands::Kill { .. } => "kill",
        Commands::Rm { .. } => "rm",
        Commands::Host(_) => "host",
    }
}
