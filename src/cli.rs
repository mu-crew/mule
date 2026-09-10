//! The command surface.
//!
//! The command shapes are settled by the spec and declared up front so
//! `--help` stays honest while each implementation lands.

use std::collections::VecDeque;
use std::io::Write;

use anyhow::Result;
use clap::{Args, Parser, Subcommand};
use serde::Serialize;

use crate::config::{Config, Host};
use crate::errors::{CoopError, EXIT_NO_MASTER};
use crate::probe::{State, probe};
use crate::transport::{Ssh, Transport};

#[derive(Serialize)]
struct JsonItems<T> {
    items: T,
    count: usize,
}

#[derive(Serialize)]
struct HostListJson<'a> {
    name: &'a str,
    target: &'a str,
    socket: String,
    master: bool,
}

#[derive(Serialize)]
struct HostInfoJson<'a> {
    name: &'a str,
    target: &'a str,
    master: bool,
    os: &'a str,
    arch: &'a str,
    cores: Option<u64>,
    ram_gb: Option<u64>,
    gpu: &'a str,
    socket: String,
    remedy: &'a str,
}

#[derive(Serialize)]
struct JobJson<'a> {
    id: &'a str,
    host: &'a str,
    state: &'static str,
    rc: Option<i32>,
    age_secs: u64,
    cmd: &'a str,
}

#[derive(Serialize)]
struct UnreachableJson<'a> {
    host: &'a str,
    why: &'a str,
    remedy: &'a str,
}

#[derive(Serialize)]
struct JobsJson<'a> {
    items: Vec<JobJson<'a>>,
    unreachable: Vec<UnreachableJson<'a>>,
}

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

    Exit 3 means the master is missing, and it needs a HUMAN: someone may have
    to touch a hardware key. If you are an agent or a script, STOP and ask the
    operator to run the printed command. Do not retry, do not run `ssh -MNf`
    yourself, and do not fall back to `ssh host command` -- that holds a session
    channel for the whole job, which is the failure coop exists to remove.
  * jobs run in a NON-login, NON-interactive shell, so login profiles do not
    run. Bash still sources ~/.bashrc over ssh, so a PATH set there does reach
    a job; ~/.bash_profile does not run, so a version manager's `activate` has
    not happened. Put its shims dir on PATH in ~/.bashrc, or source what you
    need in the command: coop run 'source ~/.zshrc && npm test'.
  * stdout and stderr are merged into one log, in the order the job wrote them;
    redirect inside your command to separate them.
  * poll and wait print NO job output; `coop tail <id>` is the output verb.
  * do NOT pipe your command into head or tail. `rc` becomes the pipe's, so a
    failed build reports 0 and every `&&` after it proceeds. coop already
    shapes the output for you: `coop tail <id> -n 3` instead of `| tail -3`.

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

    /// Suppress successful next-step hints on stderr
    #[arg(long, global = true)]
    pub quiet: bool,

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
        /// Kill the remote job after S seconds (0 means unbounded)
        #[arg(long, value_name = "S")]
        max_secs: Option<u64>,
        /// Block locally until the job finishes; unlike --max-secs, this does not kill it
        #[arg(long)]
        wait: bool,
        /// With --wait: print the log once at the end instead of streaming
        #[arg(long, requires = "wait")]
        no_tail: bool,
        /// The command to run.
        ///
        /// Everything after the first word is part of the command, so coop's
        /// own flags go BEFORE it: `coop run --wait ls`, not
        /// `coop run ls --wait`. Use `--` when the command takes flags coop
        /// also has: `coop run -- ls --all`.
        ///
        /// stdout and stderr are merged into one log, in the order the job
        /// wrote them; redirect inside your command to separate them.
        ///
        /// Do NOT pipe the command into head or tail to keep the log small.
        /// `rc` becomes the pipe's -- measured: `sh -c 'echo x; exit 1' |
        /// tail -3` exits 0 -- so a failed job reports success and any `&&`
        /// after it runs anyway, and `rc` is the artifact coop's whole design
        /// rests on. Let the job be the work and let coop shape the output:
        /// `coop tail <id> -n 3`, or plain `coop tail <id>`, which already
        /// caps the read at 64KB. If your remote sh supports it,
        /// `set -o pipefail` keeps a genuine pipeline honest; it is not
        /// portable POSIX, so coop does not add it for you -- the command is
        /// yours.
        #[arg(trailing_var_arg = true, required = true)]
        cmd: Vec<String>,
    },
    /// Print state, but no job output; prints nothing from the job; use coop tail <id>
    ///
    /// What `coop tail` gives you is one log: stdout and stderr are merged into
    /// one log, in the order the job wrote them; redirect inside your command
    /// to separate them.
    Poll {
        id: crate::wrapper::JobId,
        #[command(flatten)]
        host: HostArg,
        #[arg(long)]
        json: bool,
    },
    /// Block until done; prints nothing; use coop tail <id>
    ///
    /// What `coop tail` gives you is one log: stdout and stderr are merged into
    /// one log, in the order the job wrote them; redirect inside your command
    /// to separate them.
    Wait {
        id: crate::wrapper::JobId,
        #[command(flatten)]
        host: HostArg,
        /// Stop waiting locally after S seconds; the remote job keeps running
        #[arg(long, value_name = "S")]
        timeout: Option<u64>,
    },
    /// Print a job's merged stdout and stderr as raw bytes
    ///
    /// stdout and stderr are merged into one log, in the order the job wrote
    /// them; redirect inside your command to separate them. There is one
    /// artifact per job on purpose: splitting it would mean two files, two
    /// probe offsets, and a lost interleaving, to serve a case a redirect in
    /// your own command already covers.
    Tail {
        /// The job whose log to print.
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
    ///
    /// The human table collapses whitespace and truncates commands to keep one
    /// job on one scannable line. Use --json for each complete command.
    Ls {
        #[command(flatten)]
        host: HostArg,
        /// Include finished jobs
        #[arg(long)]
        all: bool,
        /// Emit machine-readable rows with complete, unmodified commands
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
        /// The job to remove. Omit with --all.
        id: Option<crate::wrapper::JobId>,
        /// Remove every FINISHED job, ignoring keep_days.
        ///
        /// Running jobs and orphans are kept: `rm` never stops work, and an
        /// orphan is evidence rather than mud. Use `coop kill` to end a job.
        #[arg(long, conflicts_with = "id")]
        all: bool,
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
    /// Probe host OS, architecture, cores, memory, and GPU
    Info {
        /// Probe only this configured host
        #[arg(long, value_name = "H")]
        host: Option<String>,
        /// Emit every capability as machine-readable JSON
        #[arg(long)]
        json: bool,
    },
}

pub fn load_config(path: Option<&std::path::Path>) -> Result<Config> {
    // An explicit --config is the user asserting the file exists; a missing one
    // is their typo to see, not ours to paper over with a template.
    if let Some(p) = path {
        return Config::load(p);
    }

    let default = crate::config::default_path()?;
    if !default.exists() {
        // First run. A bare "No such file or directory" is a dead end: it names
        // a path but not what belongs in it. Seed a commented template so the
        // next step is to edit a file that already exists.
        crate::config::seed(&default)?;
        anyhow::bail!(
            "no hosts configured yet\n  \
             wrote a template to {}\n  \
             edit it to name a host, then run `coop host list`",
            default.display()
        );
    }
    Config::load(&default)
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
        // JSON is an agent-facing contract, and this is now one of three
        // emitters. Typed serialization makes escaping mandatory instead of a
        // convention each new field can forget.
        let items = rows
            .iter()
            .map(|(host, master)| HostListJson {
                name: &host.name,
                target: &host.target,
                socket: host.socket.to_string_lossy().into_owned(),
                master: *master,
            })
            .collect::<Vec<_>>();
        println!(
            "{}",
            serde_json::to_string(&JsonItems {
                count: items.len(),
                items,
            })?
        );
        return Ok(());
    }

    print_table(
        &["NAME", "MASTER", "TARGET", "SOCKET"],
        &rows
            .iter()
            .map(|(h, up)| {
                vec![
                    h.name.clone(),
                    if *up { "up" } else { "down" }.to_string(),
                    h.target.clone(),
                    h.socket.display().to_string(),
                ]
            })
            .collect::<Vec<_>>(),
    );
    if rows.iter().any(|(_, up)| !up) {
        eprintln!(
            "\nsome hosts have no control master. coop cannot open one \
             (ssh -MNf needs a TTY for a hardware token).\n\
             A human may need to tap a key; ask rather than retrying:"
        );
        for (h, up) in &rows {
            if !up {
                // Rendered by the same code every other verb uses, so the
                // socket directory is prepared here too: ssh cannot create it
                // and the printed command fails without it, after the 2FA
                // prompt.
                eprintln!("  {}", crate::errors::master_command(h));
            }
        }
    }
    Ok(())
}

#[derive(Debug)]
struct HostInfo<'a> {
    host: &'a Host,
    master: bool,
    os: String,
    arch: String,
    cores: Option<u64>,
    ram_gb: Option<u64>,
    gpu: String,
    remedy: Option<String>,
}

/// Probe only on explicit `host info`: `host list` is a lock-exempt
/// `ssh -O check`, so adding a session there would make the reachability check
/// contend with jobs. The full probe measured 0.27s and stays one round trip.
fn host_info(cfg: &Config, t: &dyn Transport, host_filter: Option<&str>, json: bool) -> Result<()> {
    let hosts: Vec<&Host> = match host_filter {
        Some(name) => vec![cfg.host(Some(name))?],
        None => cfg.hosts().iter().collect(),
    };
    let mut rows = Vec::with_capacity(hosts.len());
    for host in hosts {
        if !t.master_alive(host) {
            rows.push(HostInfo {
                host,
                master: false,
                os: "unknown".into(),
                arch: "unknown".into(),
                cores: None,
                ram_gb: None,
                gpu: "unknown".into(),
                remedy: Some(crate::errors::master_command(host)),
            });
            continue;
        }
        let output = t.run(host, host_info_script())?;
        if output.code != 0 {
            anyhow::bail!(
                "probing host {} failed: {}",
                host.name,
                output.stderr.trim()
            );
        }
        rows.push(parse_host_info(host, &output.text()));
    }

    for row in rows.iter().filter(|row| !row.master) {
        eprintln!("{}: unreachable (no control master)", row.host.name);
        if let Some(remedy) = &row.remedy {
            eprintln!("  {remedy}");
            eprintln!("  a human may need to tap a hardware key; ask rather than retrying");
        }
    }

    if json {
        let items = rows
            .iter()
            .map(|row| HostInfoJson {
                name: &row.host.name,
                target: &row.host.target,
                master: row.master,
                os: &row.os,
                arch: &row.arch,
                cores: row.cores,
                ram_gb: row.ram_gb,
                gpu: &row.gpu,
                socket: row.host.socket.to_string_lossy().into_owned(),
                remedy: row.remedy.as_deref().unwrap_or(""),
            })
            .collect::<Vec<_>>();
        println!(
            "{}",
            serde_json::to_string(&JsonItems {
                count: items.len(),
                items,
            })?
        );
        return Ok(());
    }

    print_table(
        &[
            "NAME", "MASTER", "OS", "ARCH", "CORES", "RAM", "GPU", "TARGET",
        ],
        &rows
            .iter()
            .map(|row| {
                vec![
                    row.host.name.clone(),
                    if row.master { "up" } else { "down" }.to_string(),
                    row.os.clone(),
                    row.arch.clone(),
                    row.cores
                        .map_or_else(|| "unknown".into(), |n| n.to_string()),
                    row.ram_gb
                        .map_or_else(|| "unknown".into(), |n| format!("{n}GB")),
                    row.gpu.clone(),
                    row.host.target.clone(),
                ]
            })
            .collect::<Vec<_>>(),
    );
    Ok(())
}

/// One aligned table with a header, for every human-readable listing.
///
/// There were three renderers and two of them were wrong: `host list` printed
/// bare rows with no header, so the reader had to know that field two was the
/// master state, and `host info` printed a FIXED-WIDTH header over unpadded
/// rows -- so the header and the data disagreed about where a column began as
/// soon as a value was wider than its title, which is every real hostname.
/// `ls` was the only correct one, and this is its logic, shared.
///
/// The last column is never padded, so it can run long without trailing
/// whitespace on every line. Widths count CHARACTERS, not bytes: a command or
/// a hostname can be non-ASCII, and byte widths would misalign it.
fn print_table(head: &[&str], rows: &[Vec<String>]) {
    let mut width: Vec<usize> = head.iter().map(|h| h.chars().count()).collect();
    for row in rows {
        for (w, cell) in width.iter_mut().zip(row) {
            *w = (*w).max(cell.chars().count());
        }
    }

    let render = |cells: &[String]| {
        let last = cells.len().saturating_sub(1);
        let mut line = String::new();
        for (i, cell) in cells.iter().enumerate() {
            if i == last {
                line.push_str(cell);
            } else {
                line.push_str(&format!("{cell:<width$}  ", width = width[i]));
            }
        }
        line
    };

    println!(
        "{}",
        render(&head.iter().map(|h| (*h).to_string()).collect::<Vec<_>>())
    );
    for row in rows {
        println!("{}", render(row));
    }
}

/// One portable best-effort script. Every platform-specific command falls
/// back, and `command -v` guards nvidia-smi because `missing | awk` succeeds.
fn host_info_script() -> &'static str {
    "os=$(uname -s 2>/dev/null || echo unknown); \
     arch=$(uname -m 2>/dev/null || echo unknown); \
     cores=$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo unknown); \
     if [ -r /proc/meminfo ]; then \
       ram=$(awk '/MemTotal/{printf \"%.0f\", $2/1048576}' /proc/meminfo 2>/dev/null); \
     elif command -v sysctl >/dev/null 2>&1; then \
       bytes=$(sysctl -n hw.memsize 2>/dev/null); \
       case $bytes in *[!0-9]*|'') ram=unknown;; *) ram=$((bytes / 1073741824));; esac; \
     else ram=unknown; fi; \
     [ -n \"$ram\" ] || ram=unknown; \
     if command -v nvidia-smi >/dev/null 2>&1; then \
       gpu=$(nvidia-smi --query-gpu=name,memory.total --format=csv,noheader 2>/dev/null | \
             awk 'NR <= 2 { if (NR > 1) printf \"; \"; printf \"%s\", $0 }'); \
     elif command -v system_profiler >/dev/null 2>&1; then \
       gpu=$(system_profiler SPDisplaysDataType 2>/dev/null | \
             awk -F: '/Chipset Model/{sub(/^[[:space:]]*/, \"\", $2); print $2; exit}'); \
     else gpu=none; fi; \
     [ -n \"$gpu\" ] || gpu=none; \
     printf '%s\\t%s\\t%s\\t%s\\t%s\\n' \"$os\" \"$arch\" \"$cores\" \"$ram\" \"$gpu\""
}

fn parse_host_info<'a>(host: &'a Host, text: &str) -> HostInfo<'a> {
    let mut fields = text.trim_end().splitn(5, '\t');
    let os = fields
        .next()
        .filter(|s| !s.is_empty())
        .unwrap_or("unknown")
        .to_string();
    let arch = fields
        .next()
        .filter(|s| !s.is_empty())
        .unwrap_or("unknown")
        .to_string();
    let cores = fields.next().and_then(|s| s.parse().ok());
    let ram_gb = fields.next().and_then(|s| s.parse().ok());
    let gpu = fields
        .next()
        .filter(|s| !s.is_empty())
        .unwrap_or("unknown")
        .to_string();
    HostInfo {
        host,
        master: true,
        os,
        arch,
        cores,
        ram_gb,
        gpu,
        remedy: None,
    }
}

pub fn poll(t: &dyn Transport, host: &Host, id: &crate::wrapper::JobId, json: bool) -> Result<i32> {
    poll_with_hint(t, host, id, json, true)
}

fn poll_with_hint(
    t: &dyn Transport,
    host: &Host,
    id: &crate::wrapper::JobId,
    json: bool,
    quiet: bool,
) -> Result<i32> {
    // State only: `poll` discards log bytes, so asking for them would transfer
    // the whole log -- potentially hundreds of MB -- while holding the single
    // session channel and the ticket lock. That is invariant 3 violated by the
    // cheapest verb in the tool.
    crate::errors::require_master(t, host)?;
    let result = probe(t, host, id, crate::probe::From::StateOnly)?;
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
    if !quiet {
        match result.state {
            State::Running => {
                eprintln!("next: coop wait {id} to block; coop tail {id} -f to follow")
            }
            State::Done(_) => {
                eprintln!("next: coop tail {id} for output; coop rm {id} to drop its state")
            }
            State::Orphan => eprintln!(
                "orphan: no exit code will arrive\nnext: coop tail {id} for output; coop rm {id} to drop its state"
            ),
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
    crate::errors::require_master(t, host)?;
    crate::tail::wait_only(t, host, id, timeout)
}

pub fn dispatch(cli: Cli) -> Result<i32> {
    let cfg = load_config(cli.config.as_deref())?;
    let quiet = cli.quiet;
    match cli.command {
        Commands::Run {
            host,
            cwd,
            max_secs,
            wait,
            no_tail,
            cmd,
        } => {
            let host = cfg.host(host.host.as_deref())?;
            // `--` is the caller saying "everything after this is the
            // command", so an explicit separator silences the warning. clap
            // strips it, so look at the raw arguments.
            if !std::env::args().any(|a| a == "--") {
                warn_about_swallowed_flags(&cmd);
            }
            let command = command_from_args(&cmd);
            match crate::run::dispatch(&Ssh, host, &command, cwd.as_deref(), max_secs) {
                Ok(id) => {
                    println!("{id}");
                    std::io::stdout().flush()?;
                    if !wait {
                        if !quiet {
                            eprintln!("next: coop wait {id} for the exit code");
                            eprintln!("      coop tail {id} for output");
                        }
                        return Ok(0);
                    }
                    let stdout = std::io::stdout().lock();
                    let mut output = HintWriter::new(stdout);
                    let result = if no_tail {
                        crate::tail::follow_deferred(&Ssh, host, &id, &mut output)
                    } else {
                        crate::tail::follow(&Ssh, host, &id, 0, &mut output)
                    }
                    .map_err(|error| crate::errors::waiting(error, id.as_str()));
                    if let Ok(code) = result {
                        // The exit code is the cheap gate. Only a failed job
                        // earns even the bounded log-tail scan below.
                        if code != 0 && !quiet {
                            missing_tool_hint(&output.tail());
                        }
                        if !quiet {
                            eprintln!("next: coop rm {id} to drop its state");
                        }
                    }
                    result
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
            if !quiet {
                // Mirror the surface the caller actually asked for. Handing
                // `--json` to someone who just read a table gives a human a
                // machine format, and dropping it for someone parsing JSON
                // gives a parser a table. Same verb either way.
                let next = if json {
                    "coop host info --json"
                } else {
                    "coop host info"
                };
                eprintln!("next: {next} for OS, cores, RAM, and GPU");
            }
            Ok(0)
        }
        Commands::Host(HostCmd::Info { host, json }) => {
            host_info(&cfg, &Ssh, host.as_deref(), json)?;
            Ok(0)
        }
        Commands::Poll { id, host, json } => {
            poll_with_hint(&Ssh, cfg.host(host.host.as_deref())?, &id, json, quiet)
        }
        Commands::Wait { id, host, timeout } => {
            let result = wait(&Ssh, cfg.host(host.host.as_deref())?, &id, timeout)
                .map_err(|error| crate::errors::waiting(error, id.as_str()));
            if result.is_ok() && !quiet {
                eprintln!("next: coop tail {id} for output; coop rm {id} to drop its state");
            }
            result
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
            let (rows, unreachable, hidden) =
                crate::jobs::list_with_hidden(&cfg, &Ssh, host.host.as_deref(), all)?;
            print_jobs(&rows, &unreachable, hidden, json, quiet);
            Ok(0)
        }
        Commands::Kill { id, host } => {
            let rc = crate::jobs::kill(&Ssh, cfg.host(host.host.as_deref())?, &id)?;
            if !quiet {
                eprintln!("killed {id}; now done {rc}");
                eprintln!("next: coop rm {id} to drop its state");
            }
            Ok(0)
        }
        Commands::Rm { id, all, host } => {
            let host = cfg.host(host.host.as_deref())?;
            let target = match (id, all) {
                (Some(id), _) => crate::jobs::Target::One(id),
                (None, true) => crate::jobs::Target::AllDone,
                // clap cannot express "one of these is required" across a
                // positional and a flag, so say what to do rather than
                // printing a bare usage error.
                (None, false) => anyhow::bail!(
                    "name a job, or pass --all to remove every finished one\n  \
                     coop rm <id>\n  coop rm --all"
                ),
            };
            let removed = crate::jobs::remove(&Ssh, host, &target)?;
            // Report what happened: `--all` on a clean host is silent
            // otherwise, which reads as a failure.
            match removed.len() {
                0 => eprintln!("coop: nothing to remove"),
                1 => println!("{}", removed[0]),
                n => {
                    for id in &removed {
                        println!("{id}");
                    }
                    eprintln!("coop: removed {n} finished jobs");
                }
            }
            Ok(0)
        }
    }
}

const HINT_SCAN_BYTES: usize = 64 * 1024;

struct HintWriter<W> {
    inner: W,
    tail: VecDeque<u8>,
}

impl<W> HintWriter<W> {
    fn new(inner: W) -> Self {
        Self {
            inner,
            tail: VecDeque::with_capacity(HINT_SCAN_BYTES),
        }
    }

    fn tail(&self) -> Vec<u8> {
        self.tail.iter().copied().collect()
    }
}

impl<W: Write> Write for HintWriter<W> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let written = self.inner.write(bytes)?;
        self.tail.extend(&bytes[..written]);
        if self.tail.len() > HINT_SCAN_BYTES {
            self.tail.drain(..self.tail.len() - HINT_SCAN_BYTES);
        }
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

fn missing_tool_hint(log_tail: &[u8]) {
    let text = String::from_utf8_lossy(log_tail).to_ascii_lowercase();
    if [
        "command not found",
        "not found on path",
        "no such file or directory",
    ]
    .iter()
    .any(|pattern| text.contains(pattern))
    {
        eprintln!(
            "coop: the job's shell is non-login, so ~/.bash_profile did not run. If this is a missing tool, put its shims dir on PATH: coop run 'export PATH=$HOME/.elan/bin:$PATH; <cmd>'"
        );
    }
}

fn print_jobs(
    rows: &[crate::jobs::Row],
    unreachable: &[crate::jobs::Unreachable],
    hidden: usize,
    json: bool,
    quiet: bool,
) {
    for host in unreachable {
        eprintln!("{}: unreachable ({})", host.host, host.why);
        if let Some(remedy) = &host.remedy {
            eprintln!("  {remedy}");
            eprintln!("  a human may need to tap a hardware key; ask rather than retrying");
        }
    }
    if json {
        let items = rows
            .iter()
            .map(|row| {
                let (state, rc) = match row.state {
                    State::Running => ("running", None),
                    State::Done(code) => ("done", Some(code)),
                    State::Orphan => ("orphan", None),
                };
                JobJson {
                    id: &row.id,
                    host: &row.host,
                    state,
                    rc,
                    age_secs: row.age_secs,
                    cmd: &row.cmd,
                }
            })
            .collect();
        let down = unreachable
            .iter()
            .map(|host| UnreachableJson {
                host: &host.host,
                why: &host.why,
                // Carry the remedy in JSON too: a script cannot parse the
                // stderr prose, and "unreachable" without the fix is not
                // actionable for an agent either.
                remedy: host.remedy.as_deref().unwrap_or(""),
            })
            .collect();
        println!(
            "{}",
            serde_json::to_string(&JobsJson {
                items,
                unreachable: down,
            })
            .expect("serializing string-backed job rows cannot fail")
        );
        if rows.is_empty() && hidden == 0 && !quiet {
            eprintln!("no jobs; next: coop run <cmd>");
        }
        return;
    }

    if rows.is_empty() {
        if !quiet {
            if hidden == 0 {
                eprintln!("no jobs; next: coop run <cmd>");
            } else {
                eprintln!("{hidden} older finished jobs hidden; next: coop ls --all");
            }
        }
        return;
    }

    // Aligned columns with a header. Tab-separated output with no header left
    // the reader counting fields to work out which number was the exit code and
    // which the age -- and `--json` already covers the machine case, so this
    // one is for a person.
    // COMMAND is last so it can run long without padding every line -- which
    // is why `print_table` never pads its final column.
    print_table(
        &["ID", "HOST", "STATE", "RC", "AGE", "COMMAND"],
        &rows
            .iter()
            .map(|row| {
                let (state, rc) = match row.state {
                    State::Running => ("running", "-".to_string()),
                    State::Done(code) => ("done", code.to_string()),
                    State::Orphan => ("orphan", "-".to_string()),
                };
                vec![
                    row.id.clone(),
                    row.host.clone(),
                    state.to_string(),
                    rc,
                    format_age(row.age_secs),
                    display_command(&row.cmd),
                ]
            })
            .collect::<Vec<_>>(),
    );
    if !quiet {
        let id = &rows[0].id;
        eprintln!("next: coop poll {id}; coop tail {id}");
        if hidden > 0 {
            eprintln!("{hidden} older finished jobs hidden; next: coop ls --all");
        }
    }
}

/// Preserve the two documented command forms: one argument is a shell string;
/// multiple arguments are an argv-style command whose boundaries must survive
/// the remote shell. The local shell has already removed the caller's quoting,
/// so joining with spaces cannot distinguish `"a b"` from `a b`.
fn command_from_args(args: &[String]) -> String {
    match args {
        [command] => command.clone(),
        _ => args
            .iter()
            .map(|arg| format!("'{}'", arg.replace('\'', "'\\''")))
            .collect::<Vec<_>>()
            .join(" "),
    }
}

/// Warn when the command contains something that looks like a coop flag.
///
/// `run` takes the command as trailing arguments, so `coop run ls --wait` sends
/// `--wait` to `ls` rather than to coop. That has to be true -- otherwise you
/// could not run a command that takes flags -- but it fails silently: the job
/// dispatches, no output appears because `--wait` never reached coop, and the
/// exit code is whatever the command made of the stray argument. `ls` exits 1
/// on an unknown flag, which reads as a coop bug.
///
/// So this warns rather than erroring: the command really might want the flag,
/// and refusing would break `coop run -- rsync --delete ...`.
fn warn_about_swallowed_flags(cmd: &[String]) {
    const COOP_FLAGS: [&str; 8] = [
        "--wait",
        "--no-tail",
        "--max-secs",
        "--quiet",
        "--cwd",
        "--host",
        "--json",
        "--all",
    ];
    let found: Vec<&str> = cmd
        .iter()
        .skip(1)
        .filter_map(|arg| COOP_FLAGS.iter().find(|f| *f == arg).copied())
        .collect();
    if found.is_empty() {
        return;
    }
    eprintln!(
        "coop: warning: {} went to the command, not to coop",
        found.join(", ")
    );
    eprintln!(
        "  coop flags go before the command: coop run {} {}",
        found.join(" "),
        cmd.first().map(String::as_str).unwrap_or("<cmd>")
    );
    eprintln!(
        "  to silence this, separate them explicitly: coop run -- {}",
        command_from_args(cmd)
    );
}

/// Compact relative age: `45s`, `12m`, `3h`, `2d`.
///
/// Raw seconds made the reader do arithmetic to answer the only question they
/// were asking -- is this recent? -- and got worse the older the job was.
fn format_age(secs: u64) -> String {
    match secs {
        s if s < 60 => format!("{s}s"),
        s if s < 3600 => format!("{}m", s / 60),
        s if s < 86400 => format!("{}h", s / 3600),
        s => format!("{}d", s / 86400),
    }
}

fn display_command(command: &str) -> String {
    const WIDTH: usize = 80;

    let collapsed = command.split_whitespace().collect::<Vec<_>>().join(" ");
    if collapsed.chars().count() <= WIDTH {
        return collapsed;
    }

    collapsed.chars().take(WIDTH - 1).chain(['…']).collect()
}

#[cfg(test)]
mod tests {
    use super::{command_from_args, display_command, host_info};
    use crate::config::Config;
    use crate::transport::{Fake, Output};

    #[test]
    fn host_info_uses_one_round_trip_per_reachable_host() {
        let cfg = Config::parse("[hosts.one]\n[hosts.two]\n").unwrap();
        let fake = Fake::new();
        fake.push(Output::ok("Linux\tx86_64\t8\t16\tnone\n"))
            .push(Output::ok("Darwin\tarm64\t10\t32\tApple GPU\n"));

        host_info(&cfg, &fake, None, true).unwrap();

        assert_eq!(fake.scripts().len(), 2);
    }

    #[test]
    fn command_arguments_are_shell_quoted_without_changing_shell_strings() {
        assert_eq!(
            command_from_args(&["printf '[%s]' 'a b' c; echo".into()]),
            "printf '[%s]' 'a b' c; echo"
        );
        assert_eq!(
            command_from_args(&[
                "printf".into(),
                "[%s]".into(),
                "a b".into(),
                "".into(),
                "it's".into(),
            ]),
            "'printf' '[%s]' 'a b' '' 'it'\\''s'"
        );
    }

    #[test]
    fn display_command_truncates_on_character_boundaries() {
        const LIMIT: usize = 80;
        let exact = "é".repeat(LIMIT);
        let over = "é".repeat(LIMIT + 1);

        assert_eq!(display_command(&exact), exact);
        assert_eq!(
            display_command(&over),
            format!("{}…", "é".repeat(LIMIT - 1))
        );
    }

    #[test]
    fn display_command_collapses_whitespace() {
        assert_eq!(display_command("one\n\ttwo   three"), "one two three");
        assert_eq!(display_command(""), "");
    }
}
