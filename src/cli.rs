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

pub fn poll(t: &dyn Transport, host: &Host, id: &crate::wrapper::JobId, json: bool) -> Result<i32> {
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
            match crate::run::dispatch(&Ssh, host, &cmd.join(" "), cwd.as_deref(), max_secs) {
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

fn print_jobs(rows: &[crate::jobs::Row], unreachable: &[crate::jobs::Unreachable], json: bool) {
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
                // Carry the remedy in JSON too: a script cannot parse the
                // stderr prose, and "unreachable" without the fix is not
                // actionable for an agent either.
                format!(
                    r#"{{"host":"{}","why":"{}","remedy":"{}"}}"#,
                    json_escape(&host.host),
                    json_escape(&host.why),
                    json_escape(host.remedy.as_deref().unwrap_or(""))
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        println!(r#"{{"items":[{items}],"unreachable":[{down}]}}"#);
        return;
    }

    if rows.is_empty() {
        return;
    }

    // Aligned columns with a header. Tab-separated output with no header left
    // the reader counting fields to work out which number was the exit code and
    // which the age -- and `--json` already covers the machine case, so this
    // one is for a person.
    let cells: Vec<[String; 6]> = rows
        .iter()
        .map(|row| {
            let (state, rc) = match row.state {
                State::Running => ("running", "-".to_string()),
                State::Done(code) => ("done", code.to_string()),
                State::Orphan => ("orphan", "-".to_string()),
            };
            [
                row.id.clone(),
                row.host.clone(),
                state.to_string(),
                rc,
                format_age(row.age_secs),
                row.cmd.clone(),
            ]
        })
        .collect();

    let head = ["ID", "HOST", "STATE", "RC", "AGE", "COMMAND"];
    // Width the first five columns; the command is last so it can run long
    // without padding the line.
    let mut width = head.map(str::len);
    for row in &cells {
        for (w, cell) in width.iter_mut().zip(row) {
            *w = (*w).max(cell.chars().count());
        }
    }

    let render = |cols: &[String; 6]| {
        let mut line = String::new();
        for (i, cell) in cols.iter().enumerate().take(5) {
            line.push_str(&format!("{:<width$}  ", cell, width = width[i]));
        }
        line.push_str(&cols[5]);
        line
    };

    println!("{}", render(&head.map(String::from)));
    for row in &cells {
        println!("{}", render(row));
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
    const COOP_FLAGS: [&str; 7] = [
        "--wait",
        "--no-tail",
        "--max-secs",
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
        cmd.join(" ")
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
