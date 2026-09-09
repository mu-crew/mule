//! The command surface.
//!
//! The command shapes are settled by the spec and declared up front so
//! `--help` stays honest while each implementation lands.

use std::io::Write;

use anyhow::Result;
use clap::{Args, Parser, Subcommand};

use crate::config::{Config, Host};
use crate::errors::{CoopError, EXIT_NO_MASTER};
use crate::probe::{State, probe};
use crate::transport::{Ssh, Transport};

#[derive(Parser, Debug)]
#[command(
    name = "coop",
    about = "Fire remote jobs down a private ssh channel nothing else can take.",
    long_about = "\
Hand coop a command, get an id back, then poll, wait or tail against that id.
You never see ssh, never see tmux, and never hold a connection.

coop uses its OWN ssh ControlPath, so it cannot contend with git fetch, rsync or
anything else on the default socket. The coop channel is never lent to local
commands like rsync or git fetch.

Operational facts:

  * coop does NOT open the ssh master. `ssh -MNf` needs a TTY for a hardware
    token and cannot prompt from a background call. This costs one token tap per
    ControlPersist window.
  * jobs run in a NON-login, NON-interactive shell: no ~/.profile, so no nvm or
    cargo on PATH unless your command sources it.
  * stdout and stderr are MERGED into one log. Redirect inside your command if
    you need them apart.
  * poll and wait print NO job output; `coop tail <id>` is the output verb.

Exit status:
  0   coop operation or job succeeded
  3   no ssh control master
  4   timed out waiting
  5   orphaned job
  6   connection dropped while waiting
  <n> wait/--wait return the job's own exit code"
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
        #[arg(long, requires = "wait")]
        no_tail: bool,
        /// The command to run
        #[arg(trailing_var_arg = true, required = true)]
        cmd: Vec<String>,
    },
    /// Print state, but no job output; prints nothing from the job; use coop tail <id>
    Poll {
        id: crate::wrapper::JobId,
        #[command(flatten)]
        host: HostArg,
        #[arg(long)]
        json: bool,
    },
    /// Block until done; prints nothing; use coop tail <id>
    Wait {
        id: crate::wrapper::JobId,
        #[command(flatten)]
        host: HostArg,
        #[arg(long, value_name = "S")]
        timeout: Option<u64>,
    },
    /// Print a job's merged stdout and stderr as raw bytes
    Tail {
        id: crate::wrapper::JobId,
        #[command(flatten)]
        host: HostArg,
        /// Follow until the job finishes
        #[arg(short, long)]
        follow: bool,
        /// Print the whole log instead of the last 64KB
        #[arg(long, conflicts_with_all = ["lines", "follow"])]
        all: bool,
        /// Print the last N lines instead of the last 64KB
        #[arg(short = 'n', value_name = "LINES", conflicts_with_all = ["all", "follow"])]
        lines: Option<u64>,
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
    Kill {
        id: crate::wrapper::JobId,
        #[command(flatten)]
        host: HostArg,
    },
    /// Drop a job's state directory
    Rm {
        id: crate::wrapper::JobId,
        #[command(flatten)]
        host: HostArg,
    },
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

pub fn poll(t: &dyn Transport, host: &Host, id: &crate::wrapper::JobId, json: bool) -> Result<i32> {
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

pub fn wait(
    t: &dyn Transport,
    host: &Host,
    id: &crate::wrapper::JobId,
    timeout: Option<u64>,
) -> Result<i32> {
    crate::tail::wait_only(t, host, id, timeout)
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
            let host = cfg.host(host.host.as_deref())?;
            match crate::run::dispatch(&Ssh, host, &cmd.join(" "), cwd.as_deref()) {
                Ok(id) => {
                    println!("{id}");
                    std::io::stdout().flush()?;
                    if !wait {
                        return Ok(0);
                    }
                    let mut stdout = std::io::stdout().lock();
                    let result = if no_tail {
                        crate::tail::follow_deferred(&Ssh, host, &id, &mut stdout)
                    } else {
                        crate::tail::follow(&Ssh, host, &id, 0, &mut stdout)
                    };
                    result.map_err(|error| crate::errors::waiting(error, id.as_str()))
                }
                Err(error)
                    if matches!(
                        error.downcast_ref::<CoopError>(),
                        Some(CoopError::NoMaster { .. })
                    ) =>
                {
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
        Commands::Poll { id, host, json } => poll(&Ssh, cfg.host(host.host.as_deref())?, &id, json),
        Commands::Wait { id, host, timeout } => {
            wait(&Ssh, cfg.host(host.host.as_deref())?, &id, timeout)
                .map_err(|error| crate::errors::waiting(error, id.as_str()))
        }
        Commands::Tail {
            id,
            host,
            follow,
            all,
            lines,
        } => {
            let host = cfg.host(host.host.as_deref())?;
            let mut stdout = std::io::stdout().lock();
            if follow {
                crate::tail::follow(&Ssh, host, &id, 0, &mut stdout)
            } else {
                let selection = match lines {
                    Some(lines) => crate::tail::Selection::Lines(lines),
                    None if all => crate::tail::Selection::All,
                    None => crate::tail::Selection::LastBytes,
                };
                crate::tail::once(&Ssh, host, &id, selection, &mut stdout)?;
                Ok(0)
            }
        }
        Commands::Ls { host, all, json } => {
            let (rows, unreachable) = crate::jobs::list(&cfg, &Ssh, host.host.as_deref(), all)?;
            print_jobs(&rows, &unreachable, json);
            Ok(0)
        }
        Commands::Kill { id, host } => {
            crate::jobs::kill(&Ssh, cfg.host(host.host.as_deref())?, &id)?;
            Ok(0)
        }
        Commands::Rm { id, host } => {
            crate::jobs::rm(&Ssh, cfg.host(host.host.as_deref())?, &id)?;
            Ok(0)
        }
    }
}

fn print_jobs(rows: &[crate::jobs::Row], unreachable: &[crate::jobs::Unreachable], json: bool) {
    for host in unreachable {
        eprintln!("{}: unreachable ({})", host.host, host.why);
    }
    if json {
        let items = rows
            .iter()
            .map(|row| {
                let (state, rc) = match row.state {
                    State::Running => ("running", "null".to_string()),
                    State::Done(code) => ("done", code.to_string()),
                    State::Orphan => ("orphan", "null".to_string()),
                };
                format!(
                    r#"{{"id":"{}","host":"{}","state":"{state}","rc":{rc},"age_secs":{},"cmd":"{}"}}"#,
                    json_escape(&row.id),
                    json_escape(&row.host),
                    row.age_secs,
                    json_escape(&row.cmd)
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        let down = unreachable
            .iter()
            .map(|host| {
                format!(
                    r#"{{"host":"{}","why":"{}"}}"#,
                    json_escape(&host.host),
                    json_escape(&host.why)
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        println!(r#"{{"items":[{items}],"unreachable":[{down}]}}"#);
        return;
    }

    for row in rows {
        let (state, rc) = match row.state {
            State::Running => ("running", "-".to_string()),
            State::Done(code) => ("done", code.to_string()),
            State::Orphan => ("orphan", "-".to_string()),
        };
        println!(
            "{}\t{}\t{}\t{}\t{}\t{}",
            row.id, row.host, state, rc, row.age_secs, row.cmd
        );
    }
}

fn json_escape(input: &str) -> String {
    let mut escaped = String::with_capacity(input.len());
    for c in input.chars() {
        match c {
            '"' => escaped.push_str("\\\""),
            '\\' => escaped.push_str("\\\\"),
            '\u{08}' => escaped.push_str("\\b"),
            '\u{0c}' => escaped.push_str("\\f"),
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            '\t' => escaped.push_str("\\t"),
            c if c < '\u{20}' => escaped.push_str(&format!("\\u{:04x}", c as u32)),
            c => escaped.push(c),
        }
    }
    escaped
}
