#!/usr/bin/env node
// A process boundary double for mule, not for the registered Pi tools. Run
// the actual scripts against the disposable workspace (no SSH or real mu).
import { spawnSync } from "node:child_process";
import { appendFileSync, chmodSync, existsSync, lstatSync, readFileSync, realpathSync, renameSync, writeFileSync } from "node:fs";
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
if (verb === "exec" || verb === "run") {
	const cwd = args.includes("--cwd") ? args[args.indexOf("--cwd") + 1] : join(dir, "remote [root]");
	const result = spawnSync("/bin/sh", ["-c", args.at(-1)], { cwd, timeout: 5000 });
	if (result.error) throw result.error;
	if (verb === "exec") {
		process.stdout.write(result.stdout);
		process.stderr.write(result.stderr);
		process.exitCode = result.status === 3 ? 8 : result.status;
	} else {
		const log = Buffer.concat([result.stdout, result.stderr]);
		writeFileSync(join(dir, "log"), log);
		writeFileSync(join(dir, "poll"), JSON.stringify({ state: "done", rc: result.status, log_size: log.length }));
		console.log("abcdef");
	}
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
} else if (verb === "poll" || verb === "tail") {
	if (args[0] !== "abcdef") process.exit(99);
	process.stdout.write(readFileSync(join(dir, verb === "tail" ? "log" : "poll")));
} else process.exitCode = 99;
