#!/usr/bin/env node
// A process boundary double for mule, not for the registered Pi tools. Run
// the actual scripts against the disposable workspace (no SSH or real mu).
import { spawn, spawnSync } from "node:child_process";
import { randomBytes } from "node:crypto";
import { appendFileSync, chmodSync, existsSync, lstatSync, mkdirSync, readFileSync, realpathSync, renameSync, statSync, writeFileSync } from "node:fs";
import { basename, dirname, join } from "node:path";
import { createInterface } from "node:readline";

const [quiet, verb, hostFlag, host, ...args] = process.argv.slice(2);
if (quiet !== "--quiet" || hostFlag !== "--host" || host !== "fixture") process.exit(99);
const dir = process.env.MULE_TOOL_TEST_DIR;
appendFileSync(join(dir, "calls"), `${verb}\n`);
const down = join(dir, "down");
if (existsSync(down)) {
	const code = Number(readFileSync(down, "utf8"));
	console.error(code === 3 ? "Open the master: ssh -MNf fixture" : "Fixture startup I/O failure");
	process.exit(code);
}
const flag = (name) => args.includes(name) ? args[args.indexOf(name) + 1] : undefined;
if (verb === "exec") {
	const cwd = flag("--cwd") ?? join(dir, "remote [root]");
	const result = spawnSync("/bin/sh", ["-c", args.at(-1)], { cwd, timeout: 5000 });
	if (result.error) throw result.error;
	process.stdout.write(result.stdout);
	process.stderr.write(result.stderr);
	process.exitCode = result.status === 3 ? 8 : result.status;
} else if (verb === "run") {
	// Detached like mule: the job outlives this process, and a key names
	// the job it first started, at any state.
	const key = flag("--key");
	appendFileSync(join(dir, "runs"), `${JSON.stringify({ key, maxSecs: flag("--max-secs") })}\n`);
	mkdirSync(join(dir, "keys"), { recursive: true });
	if (key && existsSync(join(dir, "keys", key))) {
		console.log(readFileSync(join(dir, "keys", key), "utf8"));
		process.exit(0);
	}
	const id = randomBytes(3).toString("hex");
	const job = join(dir, "jobs", id);
	mkdirSync(job, { recursive: true });
	if (key) writeFileSync(join(dir, "keys", key), id);
	// mule's pipeline: the left end writes rc when the command exits, the
	// right end caps the log and marks a cap only when every writer is gone,
	// so rc can land before the log is final (poll's `drained`).
	const max = existsSync(join(dir, "max_log")) ? readFileSync(join(dir, "max_log"), "utf8").trim() : "1000000000000";
	writeFileSync(join(job, "max"), max);
	const child = spawn("/bin/sh", ["-c", `{ /bin/sh -c "$1" 2>&1; echo $? >"$2/rc.tmp"; mv "$2/rc.tmp" "$2/rc"; } | {
head -c "$3" >"$2/log"; cat >"$2/.overflow"; if [ -s "$2/.overflow" ]; then echo 1 >"$2/truncated"; fi; rm -f "$2/.overflow"; : >"$2/drained"; }`, "sh", args.at(-1), job, max], {
		cwd: flag("--cwd") ?? join(dir, "remote [root]"), detached: true, stdio: "ignore",
	});
	writeFileSync(join(job, "pid"), String(child.pid));
	child.unref();
	// Settle a quick command before the first poll, as a real round trip would.
	for (let i = 0; i < 40 && !existsSync(join(job, "rc")); i++) spawnSync("sleep", ["0.005"]);
	console.log(id);
} else if (verb === "poll") {
	const job = join(dir, "jobs", args[0]);
	if (!existsSync(job)) process.exit(99);
	// Drained read before rc, as mule reads `alive` before `rc`.
	const drained = existsSync(join(job, "drained"));
	const rc = existsSync(join(job, "rc")) ? Number(readFileSync(join(job, "rc"), "utf8")) : null;
	const size = existsSync(join(job, "log")) ? statSync(join(job, "log")).size : 0;
	process.stdout.write(JSON.stringify(rc === null ? { state: "running", rc: null, log_size: size } : { state: "done", rc, log_size: size, drained }));
} else if (verb === "tail") {
	const job = join(dir, "jobs", args[0]);
	if (!existsSync(job)) process.exit(99);
	// mule's caps: the log stops at max_log_bytes (noted on stderr) and one
	// tail reads its last 64 KiB.
	const log = existsSync(join(job, "log")) ? readFileSync(join(job, "log")) : Buffer.alloc(0);
	process.stdout.write(log.subarray(Math.max(0, log.length - 65536)));
	if (existsSync(join(job, "truncated"))) console.error(`mule: log was capped at ${readFileSync(join(job, "max"), "utf8")} bytes; the job ran to completion but later output was discarded`);
} else if (verb === "kill") {
	const job = join(dir, "jobs", args[0]);
	if (!existsSync(job)) process.exit(99);
	appendFileSync(join(dir, "killed"), `${args[0]}\n`);
	if (!existsSync(join(job, "rc"))) writeFileSync(join(job, "rc"), "137");
	// tmux kill-session hangs up the pane: SIGHUP to its process group, and
	// nothing more. A group the job made for itself (GNU timeout's) is not
	// signalled. The SIGKILL after it only clears what ignored the hangup.
	const group = -Number(readFileSync(join(job, "pid"), "utf8"));
	try { process.kill(group, "SIGHUP"); } catch { /* already done */ }
	for (let i = 0; i < 50; i++) {
		try { process.kill(group, 0); } catch { break; }
		spawnSync("sleep", ["0.02"]);
	}
	try { process.kill(group, "SIGKILL"); } catch { /* gone */ }
	writeFileSync(join(job, "drained"), "");
} else if (verb === "lookup") {
	const key = join(dir, "keys", args[0]);
	console.log(JSON.stringify(existsSync(key) ? readFileSync(key, "utf8") : null));
} else if (verb === "serve") {
	// The reply shapes of mule serve's read, stat and write, run locally.
	// Real serve is exercised against sshd by the loopback test.
	const answer = (id, rc, stdout = Buffer.alloc(0), stderr = "") => process.stdout.write(`${JSON.stringify({
		id, rc, stdout_b64: stdout.toString("base64"), stderr, truncated: false, cancelled: false, cancel_requested: false, batch: 1, batch_size: 1,
	})}\n`);
	const absent = (error) => error.code === "ENOENT" || error.code === "ENOTDIR" ? "No such file or directory" : String(error);
	createInterface({ input: process.stdin }).on("line", (line) => {
		const { op, id, path, data_b64 } = JSON.parse(line);
		appendFileSync(join(dir, "calls"), `serve:${op}\n`);
		try {
			if (op === "read") answer(id, 0, readFileSync(path));
			else if (op === "stat") {
				const s = lstatSync(path);
				answer(id, 0, Buffer.from(`${s.size} ${Math.floor(s.mtimeMs / 1000)} ${s.mode.toString(16)}\n`));
			} else if (op === "write") {
				const file = join(realpathSync(dirname(path)), basename(path));
				if (existsSync(file) && !lstatSync(file).isFile()) return answer(id, 2, undefined, "not replaced");
				// Like serve, the old mode is set before the rename publishes it.
				writeFileSync(`${file}.tmp`, Buffer.from(data_b64, "base64"));
				if (existsSync(file)) chmodSync(`${file}.tmp`, lstatSync(file).mode & 0o7777);
				renameSync(`${file}.tmp`, file);
				answer(id, 0, Buffer.from(`${file}\n`));
			}
		} catch (error) { answer(id, 1, undefined, `${op}: ${absent(error)}`); }
	});
} else process.exitCode = 99;
