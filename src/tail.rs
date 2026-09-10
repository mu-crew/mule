use std::io::Write;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};

use crate::config::Host;
use crate::lock::with_lock;
use crate::probe::{From as ProbeFrom, State, next_interval, probe};
use crate::transport::Transport;
use crate::wrapper::{JobId, state_dir};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Selection {
    LastBytes,
    All,
    Lines(u64),
}

pub fn once(
    transport: &dyn Transport,
    host: &Host,
    id: &JobId,
    selection: Selection,
    out: &mut dyn Write,
) -> Result<()> {
    crate::errors::require_master(transport, host)?;
    let dir = state_dir(id);
    let read = match selection {
        // Payload size is lock hold time. 64KB is deliberately conservative
        // until measurements from real suite logs justify a different cap.
        Selection::LastBytes => format!("tail -c 65536 {dir}/log"),
        Selection::All => format!("cat {dir}/log"),
        Selection::Lines(lines) => format!("tail -n {lines} {dir}/log"),
    };
    // Ask about truncation in the SAME round trip -- a second call would take
    // the lock twice to answer a question that is one byte on disk.
    let script = format!("{read}; printf '\\037%s' \"$(cat {dir}/truncated 2>/dev/null)\"");
    let output = with_lock(&host.name, || transport.run(host, &script))??;
    if output.code != 0 {
        bail!("tail failed: {}", output.stderr.trim());
    }

    // Split on the unit separator: log bytes are arbitrary, so the marker must
    // be a byte the log cannot contain ambiguously at the very end.
    let (body, truncated) = match output.stdout.iter().rposition(|&b| b == 0x1f) {
        Some(i) => (&output.stdout[..i], output.stdout[i + 1..] == *b"1"),
        None => (&output.stdout[..], false),
    };
    out.write_all(body)?;

    if truncated {
        // stderr, so it cannot corrupt `out=$(coop tail id)`.
        eprintln!(
            "coop: log was capped at {} bytes; the job ran to completion but \
             later output was discarded",
            host.max_log_bytes
        );
    }
    Ok(())
}

pub fn follow(
    transport: &dyn Transport,
    host: &Host,
    id: &JobId,
    from: u64,
    out: &mut dyn Write,
) -> Result<i32> {
    wait_loop(
        transport,
        host,
        id,
        ProbeFrom::Offset(from),
        Some(out),
        None,
    )
}

pub fn follow_deferred(
    transport: &dyn Transport,
    host: &Host,
    id: &JobId,
    out: &mut dyn Write,
) -> Result<i32> {
    let code = wait_only(transport, host, id, None)?;
    once(transport, host, id, Selection::All, out)?;
    Ok(code)
}

pub fn wait_only(
    transport: &dyn Transport,
    host: &Host,
    id: &JobId,
    timeout: Option<u64>,
) -> Result<i32> {
    // Ask for state only: a plain wait wants rc, not output, so shipping the
    // log to discard it would hold the lock for the transfer.
    wait_loop(
        transport,
        host,
        id,
        ProbeFrom::StateOnly,
        None,
        timeout.map(Duration::from_secs),
    )
}

fn wait_loop(
    transport: &dyn Transport,
    host: &Host,
    id: &JobId,
    mut from: ProbeFrom,
    mut out: Option<&mut dyn Write>,
    timeout: Option<Duration>,
) -> Result<i32> {
    let started = Instant::now();
    let mut interval = Duration::from_secs(1);
    loop {
        let result = probe(transport, host, id, from).with_context(|| {
            format!("lost contact while waiting; the job continues\n  resume: coop tail {id}")
        })?;
        let new_bytes = !result.bytes.is_empty();
        if let Some(writer) = out.as_deref_mut() {
            writer.write_all(&result.bytes)?;
        }
        // Advance by bytes received, not the reported remote size: a truncated
        // response must not create a permanent hole in streamed output. A
        // state-only wait has no offset to advance.
        if let ProbeFrom::Offset(offset) = from {
            from = ProbeFrom::Offset(offset.saturating_add(result.bytes.len() as u64));
        }

        match result.state {
            // One more read before returning. `rc` and `log` are written by
            // different ends of a pipeline, so `rc` can land while the log's
            // final bytes are still in flight -- measured: rc present with the
            // log file not yet created. Returning on the first `Done` therefore
            // dropped the output of any job short enough to finish inside one
            // probe interval, which is most of them: `coop run --wait ls`
            // printed the id and nothing else.
            //
            // A single extra round trip, only on the terminal path, and only
            // when someone is actually reading the output.
            State::Done(code) => {
                if let Some(writer) = out.as_deref_mut()
                    && let ProbeFrom::Offset(offset) = from
                {
                    let tail = probe(transport, host, id, ProbeFrom::Offset(offset))?;
                    writer.write_all(&tail.bytes)?;
                }
                return Ok(code);
            }
            State::Orphan => bail!("job {id} is orphaned; no rc will ever arrive"),
            State::Running => {}
        }
        if timeout.is_some_and(|limit| started.elapsed() >= limit) {
            bail!("timed out waiting for job {id}; it is still running");
        }
        let sleep = timeout
            .map(|limit| interval.min(limit.saturating_sub(started.elapsed())))
            .unwrap_or(interval);
        std::thread::sleep(sleep);
        interval = next_interval(interval, new_bytes);
    }
}
