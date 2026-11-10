use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use base64::Engine;

use crate::config::Host;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Job {
    pub id: JobId,
    pub cmd: String,
    pub cwd: Option<String>,
    pub max_secs: u64,
    pub metadata: JobMetadata,
    pub mode: JobMode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobMode {
    Pipe,
    Tui,
    /// `mule exec`: like `Pipe`, but the command's stderr goes to its own
    /// `$d/err` file instead of the pane's tty (SPEC § Split stdout and stderr).
    Exec,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JobMetadata {
    Managed { workstream: Option<String> },
    Human,
}

/// Remote per-job state, as a shell string (never a local `PathBuf`).
///
/// Jobs live under `jobs/` rather than directly in the state dir because the
/// ticket lock keeps `<host>.lock` in the same tree. Sharing one parent made
/// `mule ls` report `dev.lock` as an orphaned job, and would have let prune
/// delete a live lock. They collide whenever the orchestrator and the target
/// are the same machine, which is exactly the local-sshd test setup.
pub fn state_dir(id: &JobId) -> String {
    format!("{JOBS_ROOT}/{id}")
}

/// Parent of every job's state directory.
pub const JOBS_ROOT: &str = "${XDG_STATE_HOME:-$HOME/.local/state}/mule/jobs";

pub fn dispatch_script(host: &Host, job: &Job) -> String {
    dispatch_script_with(host, job, "")
}

/// [`dispatch_script`], with `first` (a `cmd && ` prefix, e.g. writing the
/// job's key) run inside the guarded body, so its failure rolls back too.
pub fn dispatch_script_with(host: &Host, job: &Job, first: &str) -> String {
    let body = match job.mode {
        JobMode::Pipe => pipe_dispatch_script(host, job, false),
        JobMode::Exec => pipe_dispatch_script(host, job, true),
        JobMode::Tui => tui_dispatch_script(host, job),
    };
    // A dispatch that fails leaves no definite state (review of P7a, round
    // 3). `pending` is made first and goes only once the job's session has
    // started, so a dir with no `rc` and `pending` still in it reads
    // `unknown`, never `orphan` or `running`. A failure anywhere in the body
    // (`first` included) kills whatever sessions it made and removes the dir
    // -- the key with it, so a retry of the key starts afresh -- and the
    // dispatch fails. Before, a failed `base64` or `new-session` left a `cmd`
    // and a key, and the key's next `run` returned that never-started job as
    // an orphan; a failed `pipe-pane` left a TUI session hung on `wait-for`.
    //
    // The dir goes only once both sessions are proven gone: killed, or
    // tmux's own "can't find session" / "no server" (matched without the
    // apostrophe, which would unbalance the job's single quotes). Removing
    // it beside a session that would not die would take the key from a job
    // that may run, and the key's next `run` would start a second one; kept,
    // with `pending`, the job reads `unknown` and `mule rm` is the way out.
    //
    // The last `rm` of `pending` is the one tolerated failure: the job has
    // started, and a `pending` left behind only makes it read `unknown`
    // until its `rc` lands. Silenced on stdout too: that `rm` printing `1`
    // before it failed reached the dispatch reply, which then failed to
    // parse beside a started job (fault injection).
    let dir = state_dir(&job.id);
    format!(
        "mkdir -p {dir} && : > {dir}/pending && \
         if {{ {first}{body}; }}; then rm -f {dir}/pending >/dev/null 2>&1; :; \
         else \
           mule_k=0; \
           for mule_s in mule-{id} watch-{id}; do \
             mule_e=$(tmux -L {socket} kill-session -t $mule_s 2>&1) || case $mule_e in \
               *'t find session'*|*'no server running'*|*'error connecting to'*'No such file or directory'*) ;; \
               *) mule_k=1;; \
             esac; \
           done; \
           if [ $mule_k = 0 ]; then rm -rf {dir}; fi; false; \
         fi",
        socket = host.tmux_socket,
        id = job.id,
    )
}

fn pipe_dispatch_script(host: &Host, job: &Job, split_err: bool) -> String {
    let dir = state_dir(&job.id);
    let command = encode_command(job.cmd.as_bytes());
    let cwd = job
        .cwd
        .as_deref()
        .or(host.default_cwd.as_deref())
        .unwrap_or("$HOME");

    // The cwd is encoded for the same reason the command is: it is user input
    // crossing the same four expansion layers. Interpolated raw, a path with a
    // space splits into two words and `cd` either fails or -- worse -- succeeds
    // against the wrong directory. `$HOME` is the one value mule supplies
    // itself, and it must stay unencoded so the remote shell expands it.
    // The cwd is a PATH, not a shell expression, and those two goals conflict:
    // encoding it keeps a space or a `$(...)` from being interpreted, but it
    // also stops `~` and `$HOME` from expanding. Since only the remote shell
    // knows the remote home, a home-relative path has to be emitted as an
    // unquoted `$HOME` with the remainder still encoded.
    //
    // Previously only the exact string `$HOME` was special-cased, so every
    // other spelling became a literal directory name that cannot exist: `cd`
    // failed, the `&&` short-circuited, and the job reported rc 1 with an empty
    // log. Measured as broken: `~/`, `~/work`, `$HOME/work`, `${HOME}/work` --
    // and mule's own config template suggested `~/work`, so following the
    // documentation produced a host where nothing ran.
    let cd = match home_relative(cwd) {
        // Nothing after the home directory.
        Some("") => "cd \"$HOME\"".to_string(),
        // `$HOME` unquoted so the remote shell expands it; the rest encoded so
        // a space or a metacharacter in the path is still inert.
        Some(rest) => format!(
            "cd \"$HOME/$(printf %s {} | base64 -d)\"",
            encode_command(rest.as_bytes())
        ),
        None => format!(
            "cd \"$(printf %s {} | base64 -d)\"",
            encode_command(cwd.as_bytes())
        ),
    };

    let shell = match &job.metadata {
        JobMetadata::Managed { workstream } => {
            let agent = format!("mule-{}", job.id);
            match workstream {
                Some(workstream) => format!(
                    "env MU_MANAGED_AGENT=1 MU_AGENT_NAME={agent} MU_WORKSTREAM=\"$(printf %s {} | base64 -d)\" sh",
                    encode_command(workstream.as_bytes())
                ),
                None => format!("env -u MU_WORKSTREAM MU_MANAGED_AGENT=1 MU_AGENT_NAME={agent} sh"),
            }
        }
        JobMetadata::Human => {
            "env -u MU_MANAGED_AGENT -u MU_AGENT_NAME -u MU_WORKSTREAM sh".to_string()
        }
    };

    // exec appends the command's stderr to `err` and nothing else's: the group
    // covers `cd` too, so a bad `--cwd` leaves its reason on the first line of
    // stderr, as `ssh host cmd` would. The watchdog and tmux calls sit outside
    // the group, so their diagnostics never reach a caller that parses stderr.
    // Appending (`>>`) keeps a re-run of one id from clobbering evidence, though
    // ids are never reused while their state dir exists.
    let body = |cd: &str, shell: &str| {
        if split_err {
            format!("{{ {cd} && {shell} < {dir}/cmd; }} 2>> {dir}/err")
        } else {
            format!("{cd} && {shell} < {dir}/cmd")
        }
    };

    // The job reads its script from the `cmd` file written before tmux starts,
    // never from the tmux command string: tmux rejects a `new-session` command
    // over ~16KB with "command too long", so inlining the payload capped every
    // job at about 12KB of script. Redirecting the file keeps the old stdin
    // semantics exactly -- `sh` still reads its script from standard input.
    //
    // `rc` is recorded only while the job's own session still exists, and
    // only if nothing recorded one first. A session torn down from outside
    // (`kill-session`, `kill-server`) hangs up every process in the pane,
    // but bash and zsh do not die at once while they wait for a child: the
    // child dies with 129, and the shell can still run this write on its way
    // out -- 129 over `kill`'s 137, 129 where the job should be an orphan, or
    // only the redirection's truncation, an empty `rc` that `kill` then
    // failed to parse. Whether it got that far was the server's signal order:
    // under tmux, which leaves the hangup to the kernel, it never did (16/16
    // rounds kept 137 and left orphans); vtmux closes the master and then
    // signals the pane's group itself, and `mule kill` kept 137 in 3/8 rounds
    // under `sh` and 0/8 under zsh. Asking the server is the fact that does
    // not race: a torn-down session is gone before its panes are hung up,
    // and a job that ends by itself is still in it. A trap cannot do this:
    // bash 3.2 ran `trap ... HUP` only after the write. The write goes
    // through a temp file and a rename, so no reader sees it half done: the
    // probe reads an `rc` that exists but is empty as `unknown`, so every
    // writer here (job, watchdog, TUI wrapper) renames. `kill`'s write runs
    // inside its locked call, where no mule probe can see it.
    let record = |code: &str| {
        format!(
            "if [ ! -f {dir}/rc ] && tmux -L {socket} has-session -t mule-{id} 2>/dev/null; then \
             echo {code} > {dir}/rc.tmp && mv -f {dir}/rc.tmp {dir}/rc; fi",
            socket = host.tmux_socket,
            id = job.id,
        )
    };
    let run = if job.max_secs == 0 {
        format!("{}; rc=$?; {}", body(&cd, &shell), record("$rc"))
    } else {
        // POSIX sh has no portable process-group primitive, so the inner tmux
        // session supplies one: `kill-session` terminates the command and all
        // descendants. The watchdog runs in a separate tmux session, because a
        // background `sleep` inside the job session would keep that session
        // alive after a fast command exits. Whichever path finishes first
        // destroys the other session. 124 follows GNU timeout and is distinct
        // from mule kill's 137. Write 124 only when rc is missing, matching
        // the job path, so a concurrent kill's 137 is not overwritten.
        let watchdog = format!(
            "sleep {secs}; if tmux -L {socket} has-session -t mule-{id} 2>/dev/null; then \
             if [ ! -f {dir}/rc ]; then echo 124 > {dir}/rc.wd && mv -f {dir}/rc.wd {dir}/rc; fi; \
             tmux -L {socket} kill-session -t mule-{id}; fi",
            socket = host.tmux_socket,
            id = job.id,
            secs = job.max_secs,
        );
        format!(
            "tmux -L {socket} -f /dev/null new-session -d -s watch-{id} \
             \"printf %s {watchdog} | base64 -d | sh\"; \
             {body}; rc=$?; \
             tmux -L {socket} kill-session -t watch-{id} 2>/dev/null; \
             {record}",
            socket = host.tmux_socket,
            id = job.id,
            watchdog = encode_command(watchdog.as_bytes()),
            body = body(&cd, &shell),
            record = record("$rc"),
        )
    };

    format!(
        "printf %s {command} | base64 -d > {dir}/cmd.tmp && mv -f {dir}/cmd.tmp {dir}/cmd && \
         tmux -L {} -f /dev/null new-session -d -s mule-{} \
         '{{ {run}; }} \
          | {{ head -c {} > {dir}/log; cat > {dir}/.overflow; \
               if [ -s {dir}/.overflow ]; then echo 1 > {dir}/truncated; fi; \
               rm -f {dir}/.overflow; }}'",
        host.tmux_socket, job.id, host.max_log_bytes
    )
}

fn tui_dispatch_script(host: &Host, job: &Job) -> String {
    let dir = state_dir(&job.id);
    let command = encode_command(job.cmd.as_bytes());
    let cwd = job
        .cwd
        .as_deref()
        .or(host.default_cwd.as_deref())
        .unwrap_or("$HOME");
    let cd = match home_relative(cwd) {
        Some("") => "cd \"$HOME\"".to_string(),
        Some(rest) => format!(
            "cd \"$HOME/$(printf %s {} | base64 -d)\"",
            encode_command(rest.as_bytes())
        ),
        None => format!(
            "cd \"$(printf %s {} | base64 -d)\"",
            encode_command(cwd.as_bytes())
        ),
    };
    let shell = match &job.metadata {
        JobMetadata::Managed { workstream } => {
            let agent = format!("mule-{}", job.id);
            match workstream {
                Some(workstream) => format!(
                    "env MU_MANAGED_AGENT=1 MU_AGENT_NAME={agent} MU_WORKSTREAM=\"$(printf %s {} | base64 -d)\" sh \"$job_dir/cmd\"",
                    encode_command(workstream.as_bytes())
                ),
                None => format!(
                    "env -u MU_WORKSTREAM MU_MANAGED_AGENT=1 MU_AGENT_NAME={agent} sh \"$job_dir/cmd\""
                ),
            }
        }
        JobMetadata::Human => {
            "env -u MU_MANAGED_AGENT -u MU_AGENT_NAME -u MU_WORKSTREAM sh \"$job_dir/cmd\""
                .to_string()
        }
    };
    let watchdog = if job.max_secs == 0 {
        String::new()
    } else {
        let body = format!(
            "sleep {secs}; if tmux -L {socket} has-session -t mule-{id} 2>/dev/null; then \
             if [ ! -f {dir}/rc ]; then echo 124 > {dir}/rc.wd && mv -f {dir}/rc.wd {dir}/rc; fi; \
             tmux -L {socket} kill-session -t mule-{id}; fi",
            socket = host.tmux_socket,
            id = job.id,
            secs = job.max_secs,
        );
        format!(
            "tmux -L {socket} -f /dev/null new-session -d -s watch-{id} \
             \"printf %s {body} | base64 -d | sh\"; ",
            socket = host.tmux_socket,
            id = job.id,
            body = encode_command(body.as_bytes()),
        )
    };
    let wrapper = format!(
        "job_dir=$PWD; tmux -L {socket} wait-for tui-{id}; rm -f \"$job_dir/wrapper\"; \
         {watchdog}{cd} && {shell}; rc=$?; \
         tmux -L {socket} capture-pane -p -J -t mule-{id} > \"$job_dir/screen\" 2>/dev/null || :; \
         tmux -L {socket} pipe-pane -t mule-{id}; \
         while [ ! -f \"$job_dir/.pipe-done\" ]; do sleep 0.01; done; \
         rm -f \"$job_dir/.pipe-done\"; \
         tmux -L {socket} kill-session -t watch-{id} 2>/dev/null; \
         if [ ! -f \"$job_dir/rc\" ]; then echo $rc > \"$job_dir/rc.tmp\" && mv -f \"$job_dir/rc.tmp\" \"$job_dir/rc\"; fi",
        socket = host.tmux_socket,
        id = job.id,
    );
    let consumer = format!(
        "rm -f ./pipe; head -c {max} > ./log; cat > ./.overflow; \
         if [ -s ./.overflow ]; then echo 1 > ./truncated; fi; \
         rm -f ./.overflow; echo 1 > ./.pipe-done",
        max = host.max_log_bytes
    );

    format!(
        "printf %s {command} | base64 -d > {dir}/cmd.tmp && mv -f {dir}/cmd.tmp {dir}/cmd && \
         printf %s {wrapper} | base64 -d > {dir}/wrapper && \
         printf %s {consumer} | base64 -d > {dir}/pipe && echo tui > {dir}/mode && \
         tmux -L {socket} -f /dev/null new-session -d -s mule-{id} -c {dir} \
           \"sh ./wrapper\" && \
         {{ tmux -L {socket} set-option -g extended-keys on 2>/dev/null && \
            tmux -L {socket} set-option -g extended-keys-format csi-u 2>/dev/null || \
            {{ tmux -L {socket} kill-session -t mule-{id} 2>/dev/null; \
               echo 'mule: tmux 3.2 or newer is required for --tui' >&2; false; }}; }} && \
         tmux -L {socket} pipe-pane -O -t mule-{id} \"cd {dir} && sh ./pipe\" && \
         tmux -L {socket} wait-for -S tui-{id}",
        socket = host.tmux_socket,
        id = job.id,
        wrapper = encode_command(wrapper.as_bytes()),
        consumer = encode_command(consumer.as_bytes()),
    )
}

/// The part of `path` after the user's home directory, if it is home-relative.
///
/// Recognises the spellings people actually write. Deliberately a fixed set
/// rather than general shell expansion: expanding arbitrary `$(...)` in a
/// configured path would hand the shell back the injection surface the base64
/// encoding exists to remove.
fn home_relative(path: &str) -> Option<&str> {
    for prefix in ["~", "$HOME", "${HOME}"] {
        if let Some(rest) = path.strip_prefix(prefix) {
            // `~foo` is another user's home, a different problem; `$HOMEDIR` is
            // simply a different variable. Neither is home-relative.
            if rest.is_empty() {
                return Some("");
            }
            if let Some(rest) = rest.strip_prefix('/') {
                return Some(rest.trim_end_matches('/'));
            }
        }
    }
    None
}

/// Width of a generated id, in hex digits.
pub const ID_HEX_LEN: usize = 6;

pub fn new_id() -> String {
    // Time plus pid avoids a dependency for a non-secret id; the odd step keeps
    // the low 24 bits unique until the six-hex-digit space wraps.
    static NEXT: OnceLock<AtomicU64> = OnceLock::new();
    let next = NEXT.get_or_init(|| {
        let time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
        AtomicU64::new(time ^ u64::from(std::process::id()))
    });
    let value = next.fetch_add(0x9e37_79b9_7f4a_7c15, Ordering::Relaxed);
    format!("{:06x}", value & 0x00ff_ffff)
}

pub fn encode_command(input: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(input)
}

/// A validated job id: exactly six lowercase hex digits.
///
/// Every verb takes an id from the command line and interpolates it into a
/// remote path, a tmux target, and a shell script. Unvalidated, that is command
/// injection: `mule poll 'x$(touch /tmp/pwn)y'` reached the remote shell as
/// syntax and would have executed. Parsing at the boundary makes the unsafe
/// value unrepresentable rather than relying on every call site to quote.
///
/// Hex also avoids `:` and `.`, which tmux's target grammar reserves.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobId(String);

impl JobId {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::str::FromStr for JobId {
    type Err = anyhow::Error;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        let ok = raw.len() == ID_HEX_LEN
            && raw
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b));
        if !ok {
            anyhow::bail!(
                "invalid job id {raw:?}: expected {ID_HEX_LEN} lowercase hex digits, as printed by `mule run`"
            );
        }
        Ok(Self(raw.to_string()))
    }
}

impl std::fmt::Display for JobId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}
