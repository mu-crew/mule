use anyhow::{Context, Result, bail};

use crate::config::{Config, Host};
use crate::lock::with_lock;
use crate::probe::State;
use crate::transport::Transport;
use crate::wrapper::state_dir;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Row {
    pub id: String,
    pub host: String,
    pub state: State,
    pub rc: Option<i32>,
    pub age_secs: u64,
    pub cmd: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Unreachable {
    pub host: String,
    pub why: String,
}

pub fn list(
    cfg: &Config,
    transport: &dyn Transport,
    host_filter: Option<&str>,
    all: bool,
) -> Result<(Vec<Row>, Vec<Unreachable>)> {
    let hosts: Vec<&Host> = match host_filter {
        Some(name) => vec![cfg.host(Some(name))?],
        None => cfg.hosts().iter().collect(),
    };
    let mut rows = Vec::new();
    let mut unreachable = Vec::new();

    // Each host takes the same ticket lock, so parallel calls would only queue
    // at the lock while making error reporting and ordering less predictable.
    for host in hosts {
        if !transport.master_alive(host) {
            unreachable.push(Unreachable {
                host: host.name.clone(),
                why: "no control master".into(),
            });
            continue;
        }
        let output = with_lock(&host.name, || transport.run(host, &list_script(host)))??;
        if output.code != 0 {
            bail!(
                "listing jobs on {} failed: {}",
                host.name,
                output.stderr.trim()
            );
        }
        parse_rows(host, &output.text(), all, &mut rows)?;
    }
    Ok((rows, unreachable))
}

fn list_script(host: &Host) -> String {
    format!(
        "root=$HOME/.local/state/coop; now=$(date +%s); \
         for d in \"$root\"/*; do [ -d \"$d\" ] || continue; \
         id=${{d##*/}}; rc=$(cat \"$d/rc\" 2>/dev/null || true); \
         alive=$(tmux -L {} has-session -t \"coop-$id\" 2>/dev/null && echo 1 || echo 0); \
         modified=$(stat -c %Y \"$d\" 2>/dev/null || stat -f %m \"$d\"); \
         cmd=$(base64 < \"$d/cmd\" 2>/dev/null | tr -d '\\n'); \
         printf '%s\\t%s\\t%s\\t%s\\t%s\\n' \"$id\" \"$((now-modified))\" \"$rc\" \"$alive\" \"$cmd\"; done",
        host.tmux_socket
    )
}

fn parse_rows(host: &Host, reply: &str, all: bool, rows: &mut Vec<Row>) -> Result<()> {
    for line in reply.lines() {
        let mut fields = line.splitn(5, '\t');
        let id = fields.next().context("invalid ls reply: missing id")?;
        let age_secs = fields
            .next()
            .context("invalid ls reply: missing age")?
            .parse()
            .context("invalid age in ls reply")?;
        let rc_text = fields.next().context("invalid ls reply: missing rc")?;
        let alive = fields.next().context("invalid ls reply: missing alive")? == "1";
        let cmd = decode_base64(fields.next().context("invalid ls reply: missing cmd")?)?;
        let rc = if rc_text.is_empty() {
            None
        } else {
            Some(rc_text.parse().context("invalid rc in ls reply")?)
        };
        let state = match rc {
            Some(code) => State::Done(code),
            None if alive => State::Running,
            None => State::Orphan,
        };
        if all || !matches!(state, State::Done(_)) {
            rows.push(Row {
                id: id.into(),
                host: host.name.clone(),
                state,
                rc,
                age_secs,
                cmd,
            });
        }
    }
    Ok(())
}

pub fn kill(transport: &dyn Transport, host: &Host, id: &str) -> Result<()> {
    let dir = state_dir(id);
    let script = format!(
        "d={dir}; [ -f $d/rc ] || echo 137 > $d/rc; tmux -L {} kill-session -t coop-{id}",
        host.tmux_socket
    );
    run_mutation(transport, host, &script, "kill")
}

pub fn rm(transport: &dyn Transport, host: &Host, id: &str) -> Result<()> {
    let dir = state_dir(id);
    let script = format!(
        "tmux -L {} kill-session -t coop-{id} 2>/dev/null || true; rm -rf {dir}",
        host.tmux_socket
    );
    run_mutation(transport, host, &script, "rm")
}

fn run_mutation(
    transport: &dyn Transport,
    host: &Host,
    script: &str,
    operation: &str,
) -> Result<()> {
    let output = with_lock(&host.name, || transport.run(host, script))??;
    if output.code != 0 {
        bail!("{operation} failed: {}", output.stderr.trim());
    }
    Ok(())
}

pub fn prune(host: &Host) -> String {
    format!(
        "root=$HOME/.local/state/coop; [ ! -d \"$root\" ] || \
         find \"$root\" -mindepth 1 -maxdepth 1 -type d -mtime +{} \
         -exec test -f '{{}}/rc' \\; -exec rm -rf '{{}}' +",
        host.keep_days
    )
}

fn decode_base64(input: &str) -> Result<String> {
    let mut bytes = Vec::with_capacity(input.len() / 4 * 3);
    let mut chunk = [0_u8; 4];
    let mut n = 0;
    for byte in input.bytes() {
        let value = match byte {
            b'A'..=b'Z' => byte - b'A',
            b'a'..=b'z' => byte - b'a' + 26,
            b'0'..=b'9' => byte - b'0' + 52,
            b'+' => 62,
            b'/' => 63,
            b'=' => 64,
            _ => bail!("invalid base64 in ls reply"),
        };
        chunk[n] = value;
        n += 1;
        if n == 4 {
            bytes.push((chunk[0] << 2) | (chunk[1] >> 4));
            if chunk[2] != 64 {
                bytes.push((chunk[1] << 4) | (chunk[2] >> 2));
            }
            if chunk[3] != 64 {
                bytes.push((chunk[2] << 6) | chunk[3]);
            }
            n = 0;
        }
    }
    if n != 0 {
        bail!("invalid base64 length in ls reply");
    }
    String::from_utf8(bytes).context("job command is not UTF-8")
}
