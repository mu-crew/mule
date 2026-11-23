//! The RPC process answers questions and hands out launchers. Only the pane
//! runs a lease: putting a carrier in the RPC process group would tie its
//! lifetime to the mu invocation and let its detached stop escape the group.

use std::path::Path;

use anyhow::{Context, Result, bail, ensure};
use mule::cli::shell_quote;
use mule::config::{Host, LEASE_CONFIG_ENV};
use mule::key::JobKey;
use mule::wrapper::JobId;

use super::{
    Ctx, Live, Run, Snap, Value, expired, fail, json, now_ms, remaining_at, reply, run_mule,
};
use std::sync::atomic::Ordering;
use std::time::Duration;

pub(super) fn call(ctx: &Ctx, live: &Live, id: &Value, method: &str, params: &Value) {
    if live.cancelled.load(Ordering::SeqCst) {
        return;
    }
    let deadline = params["deadline"].as_u64();
    if remaining_at(deadline, now_ms()).is_err() {
        return expired(&ctx.out, id);
    }
    let Some(snap) = super::snapshot(ctx, id) else {
        return;
    };
    let target = &params["target"];
    let result = (|| -> Result<Action> {
        ensure!(
            target["job"]["kind"] == "lease",
            "job target must name the lease kind"
        );
        let name = text(&target["host"], "name")?;
        let host = snap
            .hosts
            .as_ref()
            .context("invalid mule configuration at handshake")?
            .host(Some(name))?;
        let action = match method {
            "job.start" => Action::Reply(json!({"view": start_view(ctx, host, params)?})),
            "job.attach" => {
                let id = handle(target)?;
                let view = launcher(ctx, host, params, &["--attach", id.as_str()], None)?;
                Action::Reply(view)
            }
            "job.lookup" => {
                ensure!(params["fence"] == false, "mule cannot fence a lease key");
                Action::Run(
                    Check::Lookup,
                    vec![
                        "lookup".into(),
                        "--host".into(),
                        name.into(),
                        key(params)?.as_str().into(),
                    ],
                )
            }
            "job.status" => Action::Run(
                Check::Status,
                ["poll", "--host", name, "--json", handle(target)?.as_str()]
                    .map(String::from)
                    .into(),
            ),
            "job.stop" => Action::Run(
                Check::Stop,
                ["rm", "--host", name, handle(target)?.as_str()]
                    .map(String::from)
                    .into(),
            ),
            _ => unreachable!("dispatcher matches the job methods"),
        };
        Ok(action)
    })();
    match result {
        Ok(Action::Reply(v)) if !live.cancelled.load(Ordering::SeqCst) => {
            if remaining_at(deadline, now_ms()).is_err() {
                expired(&ctx.out, id);
            } else {
                reply(&ctx.out, id, v);
            }
        }
        Ok(Action::Reply(_)) => {}
        Ok(Action::Run(check, args)) => run(ctx, live, id, &snap, check, args, deadline),
        Err(e) => fail(&ctx.out, id, "permanent", &e.to_string(), None),
    }
}

enum Action {
    Reply(Value),
    Run(Check, Vec<String>),
}

fn text<'a>(obj: &'a Value, field: &str) -> Result<&'a str> {
    obj[field]
        .as_str()
        .filter(|s| !s.is_empty() && !s.contains('\0'))
        .with_context(|| format!("invalid or missing {field}"))
}

fn key(params: &Value) -> Result<JobKey> {
    let key = text(params, "key")?;
    ensure!(
        params["target"]["job"]["key"] == key,
        "target and request keys differ"
    );
    key.parse().ok().context("invalid mule job key")
}

fn handle(target: &Value) -> Result<JobId> {
    text(&target["job"], "handle")?
        .parse()
        .ok()
        .context("invalid mule job handle")
}

fn start_view(ctx: &Ctx, host: &Host, params: &Value) -> Result<Value> {
    let spec = &params["spec"];
    ensure!(
        spec.get("firstPrompt").is_none(),
        "mule delivers the first prompt through the channel, not job.start"
    );
    ensure!(
        matches!(spec["role"].as_str(), Some("full-access" | "read-only")),
        "invalid or missing job role"
    );
    let key = key(params)?;
    let cwd = text(spec, "cwd")?;
    // mu has already chosen the harness, model and role-correct command. As
    // with the lease template, the env it supplies (not mule's own metadata)
    // is installed inside the remote job, not on the local SSH client.
    let env = spec["env"].as_object().context("missing job environment")?;
    let mut command = String::from("exec env");
    for (name, value) in env {
        ensure!(valid_env_name(name), "invalid job environment name");
        let value = value
            .as_str()
            .filter(|v| !v.contains('\0'))
            .context("invalid job environment value")?;
        command.push(' ');
        command.push_str(&shell_quote(&format!("{name}={value}")));
    }
    command.push_str(" sh -c ");
    command.push_str(&shell_quote(text(spec, "cmd")?));
    launcher(
        ctx,
        host,
        spec,
        &["--key", key.as_str(), "--human", "--cwd", cwd],
        Some(&command),
    )
}

fn valid_env_name(name: &str) -> bool {
    let mut bytes = name.bytes();
    matches!(bytes.next(), Some(b'a'..=b'z' | b'A'..=b'Z' | b'_'))
        && bytes.all(|b| b.is_ascii_alphanumeric() || b == b'_')
}

fn launcher(
    ctx: &Ctx,
    host: &Host,
    fields: &Value,
    mode: &[&str],
    script: Option<&str>,
) -> Result<Value> {
    let job_file = text(fields, "jobFile")?;
    ensure!(
        Path::new(job_file).is_absolute(),
        "jobFile must be an absolute local path"
    );
    let executable = ctx
        .mule_path()?
        .to_str()
        .context("mule path is not UTF-8")?;
    let mut args = vec![
        executable,
        "--lease-config-env",
        "run",
        "--lease",
        "--host",
        &host.name,
        "--job-file",
        job_file,
    ];
    args.extend_from_slice(mode);
    let forward;
    if let Some(sockets) = fields.get("sockets") {
        let local = text(sockets, "local")?;
        let remote = text(sockets, "remote")?;
        ensure!(
            Path::new(local).is_absolute() && !local.contains(':'),
            "sockets.local must be an absolute path without ':'"
        );
        forward = format!("{local}:{remote}");
        mule::forward::Spec::parse(&forward)
            .ok()
            .context("invalid lease socket pair")?;
        args.extend(["--forward-unix", &forward]);
    }
    if let Some(script) = script {
        args.extend(["--", script]);
    }
    let command = format!(
        "exec {}",
        args.into_iter()
            .map(shell_quote)
            .collect::<Vec<_>>()
            .join(" ")
    );
    ensure!(
        command.len() <= 65_536,
        "lease launcher exceeds mu's command limit"
    );
    let config = host.lease_snapshot()?;
    // Avoid emitting a frame mu would reject if the config was extraordinarily large.
    ensure!(config.len() <= 65_536, "lease host snapshot is too large");
    Ok(json!({"command": command, "env": {LEASE_CONFIG_ENV: config}}))
}

#[derive(Clone, Copy)]
enum Check {
    Lookup,
    Status,
    Stop,
}

fn run(
    ctx: &Ctx,
    live: &Live,
    id: &Value,
    snap: &Snap,
    check: Check,
    args: Vec<String>,
    deadline: Option<u64>,
) {
    let answer = match run_mule(ctx, snap, live, &args, Duration::from_secs(20), deadline) {
        Ok(Run::Cancelled) => return,
        Ok(Run::Expired) => return expired(&ctx.out, id),
        Ok(Run::Done(r)) if r.cancelled => return,
        Ok(Run::Done(r)) if r.code == Some(3) && !r.timed_out => {
            let host = &args[2];
            let remedy = r
                .stderr
                .lines()
                .find_map(|s| s.trim().strip_prefix("run: "));
            return fail(
                &ctx.out,
                id,
                "human",
                &format!("no ssh control master for {host}"),
                remedy,
            );
        }
        Ok(Run::Done(r)) if r.code == Some(0) && !r.timed_out && !r.partial => {
            check.answer(&r.stdout)
        }
        _ => Err(anyhow::anyhow!("mule did not prove the job's state")),
    };
    match answer {
        Ok(result) => reply(&ctx.out, id, result),
        Err(_) => match check {
            Check::Lookup => reply(
                &ctx.out,
                id,
                json!({"unknown": "mule could not look up the job key"}),
            ),
            Check::Status => reply(
                &ctx.out,
                id,
                json!({"state": "unknown", "reason": "mule could not determine job status"}),
            ),
            Check::Stop => fail(
                &ctx.out,
                id,
                "unknown-outcome",
                "mule did not prove the job was removed",
                None,
            ),
        },
    }
}

impl Check {
    fn answer(self, out: &str) -> Result<Value> {
        match self {
            Self::Lookup => match serde_json::from_str::<Option<String>>(out)? {
                Some(found) => {
                    let id: JobId = found.parse()?;
                    Ok(json!({"found": {"handle": id.as_str()}}))
                }
                None => Ok(json!({"none": true})),
            },
            Self::Status => {
                let status: Value = serde_json::from_str(out)?;
                match status["state"].as_str() {
                    Some("running") => Ok(json!({"state": "running"})),
                    Some("done") => Ok(
                        json!({"state": "ended", "exit": status["rc"].as_u64().context("missing job exit code")?}),
                    ),
                    Some("missing") => {
                        Ok(json!({"state": "lost", "reason": "no mule job or session found"}))
                    }
                    Some("orphan") => Ok(
                        json!({"state": "lost", "reason": "mule job has no session or exit code"}),
                    ),
                    _ => bail!("job status not known"),
                }
            }
            Self::Stop => Ok(Value::Null),
        }
    }
}
