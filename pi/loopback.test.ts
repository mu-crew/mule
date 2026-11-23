import assert from "node:assert/strict";
import { execFileSync, spawnSync } from "node:child_process";
import { appendFileSync, chmodSync, existsSync, mkdirSync, readFileSync, statSync, symlinkSync, writeFileSync } from "node:fs";
import { setTimeout as delay } from "node:timers/promises";
import { join } from "node:path";
import test from "node:test";
import { MuleExecBackend, quote } from "./exec-backend.ts";
import { MasterRequired } from "./ops.ts";
import { bashJob } from "./policy.ts";
import { jobKey, loopResources, runJob, workspaceRouter } from "./routing.ts";
import { MuleServeBackend } from "./serve-backend.ts";
import { havePi, runPiCase } from "./test/run-pi.ts";

// Launched by the Rust loopback fixture, never by a real configured host.
const fixture = process.env.MULE_PI_TEST_ROOT;
const noFixture = fixture && !process.env.MULE_PI_TEST_NO_MASTER ? false : "requires the MaxSessions=1 loopback master";
const options = () => ({ bin: process.env.MULE_PI_TEST_BIN!, host: "live", config: process.env.MULE_PI_TEST_CONFIG });
const backend = () => new MuleExecBackend(options());
const verbs = (from: number) => readFileSync(process.env.MULE_PI_TEST_CALLS!, "utf8").slice(from);
const mark = () => readFileSync(process.env.MULE_PI_TEST_CALLS!, "utf8").length;

test("workspace ops work over one capped SSH master", { skip: noFixture, timeout: 45_000 }, async () => {
	const root = fixture!;
	const ops = backend();
	assert.equal(await ops.resolveRoot(root), root);
	const router = workspaceRouter("/loop/workspace", root, loopResources("/loop/workspace", "/loop/home", []), ops);
	const path = "a ' $dollar; $(false) ü.txt";
	const file = join(root, path);
	const bytes = Buffer.alloc(203_000);
	for (let i = 0; i < bytes.length; i++) bytes[i] = i % 251;
	await router.write(path, bytes);
	assert.deepEqual(readFileSync(file), bytes);
	assert.deepEqual(await router.read(path), bytes);
	await router.write(path, Buffer.alloc(0));
	assert.equal(readFileSync(file).length, 0);

	const all = Array.from({ length: 5 }, (_, i) => {
		const body = Buffer.from([0, 10, 255, i]);
		writeFileSync(join(root, `file${i}`), body);
		return body;
	});
	const cancelled = new AbortController();
	const log = process.env.MULE_PI_TEST_CALLS!;
	const before = readFileSync(log, "utf8").length;
	const burst = Promise.allSettled([
		...all.map((_, i) => ops.read(join(root, `file${i}`))),
		ops.read(join(root, "not-here")), ops.stat(root), ops.read(root),
		ops.read(join(root, "file0"), cancelled.signal),
	]);
	cancelled.abort();
	const replies = await burst;
	for (const [i, value] of all.entries()) assert.deepEqual(replies[i], { status: "fulfilled", value });
	assert.deepEqual(replies[5], { status: "fulfilled", value: undefined });
	assert.deepEqual(replies[6], { status: "fulfilled", value: "directory" });
	assert.equal(replies[7].status, "rejected");
	assert.equal(replies[8].status, "rejected");
	assert.equal(readFileSync(log, "utf8").slice(before), "exec\n");

	// The bash job waits on a file written only after a read returns, so it
	// cannot share a batch barrier with the read. Its actual rc 3 is not a handback.
	const gate = join(root, "gate");
	const request = { cwd: root, key: "pi-loopback-gate", timeout: 10,
		command: `while [ ! -f ${quote(gate)} ]; do sleep 0.1; done; echo body; exit 3` };
	const job = await ops.exec(request);
	assert.equal((await ops.poll(job)).state, "running");
	assert.deepEqual(await ops.read(join(root, "file1")), all[1]);
	await ops.write(gate, Buffer.from("open"));
	const output: Buffer[] = [];
	assert.deepEqual(await runJob(ops, request, { onData: (data) => output.push(data) }), { exitCode: 3 });
	assert.equal(Buffer.concat(output).toString(), "body\n");
	assert.equal(await ops.exec(request), job);

	const limited = process.env.MULE_PI_TEST_CONFIG!;
	appendFileSync(limited, "max_log_bytes = 10\n");
	try {
		const result = await Promise.allSettled([ops.stat(root), ops.read(join(root, "file0"))]);
		for (const member of result) {
			assert.equal(member.status, "rejected");
			assert.match(String(member.reason), /incomplete \(exit 7\)/);
		}
	} finally {
		// The Rust harness reuses the config to check master loss.
		const config = readFileSync(limited, "utf8").replace("max_log_bytes = 10\n", "");
		writeFileSync(limited, config);
	}
});

test("policy over one capped master: kill, reattach, key prefix, caps, own exit codes, muzzle", { skip: noFixture, timeout: 60_000 }, async () => {
	const root = join(fixture!, "policy");
	mkdirSync(root, { recursive: true });
	const ops = backend();
	const collect = async (request: Parameters<typeof ops.exec>[0]) => {
		const parts: Buffer[] = [];
		const result = await runJob(ops, request, { onData: (data) => parts.push(data) });
		return { ...result, text: Buffer.concat(parts).toString() };
	};

	// Reattach: the same key is the same job, so a replay never runs twice.
	const once = { cwd: root, key: "pi-loopback-once", command: "echo ran >> count; echo answer" };
	assert.deepEqual(await collect(once), { exitCode: 0, text: "answer\n" });
	assert.deepEqual(await collect(once), { exitCode: 0, text: "answer\n" });
	assert.equal(readFileSync(join(root, "count"), "utf8"), "ran\n");
	// A restarted Pi finds an interrupted call's job by its key, without
	// starting one for a key that never ran.
	assert.equal(await ops.lookup(once.key), await ops.exec(once));
	assert.equal(await ops.lookup("pi-loopback-never-ran"), undefined);
	assert.equal(readFileSync(join(root, "count"), "utf8"), "ran\n");

	// MULE_PI_JOB_PREFIX's contract: mu finds a worker's jobs by key prefix.
	const key = jobKey("session", "call", "mu.loopback.1.");
	const prefixed = await ops.exec({ cwd: root, key, command: "true" });
	const listed = execFileSync(process.env.MULE_PI_TEST_BIN!, ["--quiet", "--config", process.env.MULE_PI_TEST_CONFIG!, "ls", "--host", "live", "--key-prefix", "mu.loopback.1.", "--json"]).toString();
	assert.ok(listed.includes(key) && listed.includes(prefixed), listed);

	// Abort: `mule kill` stops the job; nothing of it runs on.
	const controller = new AbortController();
	const marker = "sleep 20.4321";
	const running = runJob(ops, { cwd: root, key: "pi-loopback-kill", command: `touch started; ${marker}; touch finished` }, { signal: controller.signal, onData: () => {} });
	for (let i = 0; i < 400 && !existsSync(join(root, "started")); i++) await delay(25);
	assert.ok(existsSync(join(root, "started")));
	const before = mark();
	controller.abort();
	await assert.rejects(running, /aborted/);
	assert.match(verbs(before), /^kill$/m);
	await delay(500);
	assert.ok(!existsSync(join(root, "finished")));
	assert.notEqual(spawnSync("pgrep", ["-f", marker]).status, 0, "no process of the killed job is left");

	// A command's own 3 and 7 are results, never a handback or a partial answer.
	assert.deepEqual(await collect({ cwd: root, key: "pi-loopback-own", command: "echo own; exit 3" }), { exitCode: 3, text: "own\n" });
	assert.deepEqual(await collect({ cwd: root, key: "pi-loopback-seven", command: "echo seven; exit 7" }), { exitCode: 7, text: "seven\n" });
	for (const code of [3, 7]) {
		await assert.rejects(ops.command(`exit ${code}`), (error) => !(error instanceof MasterRequired) && !/incomplete/.test(String(error)) && /mule exec failed/.test(String(error)), `exec ${code}`);
	}

	// mule's cap on a detached log is a note, beside the command's own code.
	const config = process.env.MULE_PI_TEST_CONFIG!;
	appendFileSync(config, "max_log_bytes = 10\n");
	try {
		assert.deepEqual(await collect({ cwd: root, key: "pi-loopback-capped", command: "printf 0123456789abcdef; exit 7" }), {
			exitCode: 7, text: "0123456789\n[mule: the log reached max_log_bytes (10 bytes); later output was discarded]\n",
		});
		// rc lands while a child the command left still writes: the cap
		// marker comes only when the log drains, and the answer waits for it.
		assert.deepEqual(await collect({ cwd: root, key: "pi-loopback-late-cap", command: "echo early; (sleep 1; printf 0123456789abcdef) & exit 7" }), {
			exitCode: 7, text: "early\n0123\n[mule: the log reached max_log_bytes (10 bytes); later output was discarded]\n",
		});
	} finally {
		writeFileSync(config, readFileSync(config, "utf8").replace("max_log_bytes = 10\n", ""));
	}

	// The host's muzzle: found in the root probe, heavy commands queue there.
	const fake = join(root, "fake muzzle");
	writeFileSync(fake, `#!/bin/bash
echo "$*" >> ${quote(join(root, "muzzle-calls"))}
case $1 in
  wrap) if [[ \${@: -1} == *HEAVY* ]]; then echo '{"command":"x","rule":"fake-heavy","slots":2}'; else echo '{"command":null}'; fi ;;
  run) while [ "$1" != -- ]; do shift; done; shift; echo "muzzle: admitted (fake)"; exec "$@" ;;
esac
`, { mode: 0o755 });
	assert.deepEqual(await ops.probe(root, fake), { root, muzzle: fake });
	assert.deepEqual(await ops.probe(root, "off"), { root, muzzle: undefined });
	assert.deepEqual(await ops.probe(root, join(root, "absent")), { root, muzzle: undefined });
	const heavy = bashJob("echo HEAVY", { muzzle: fake, timeout: 5 });
	assert.deepEqual(await collect({ cwd: root, key: "pi-loopback-heavy", ...heavy }), { exitCode: 0, text: "muzzle: admitted (fake)\nHEAVY\n" });
	assert.match(readFileSync(join(root, "muzzle-calls"), "utf8"), /^run --rule=fake-heavy --slots 2 --timeout 900 --run-timeout 5 -- bash -c echo HEAVY$/m);
	const light = bashJob("echo light; exit 3", { muzzle: fake, timeout: 5 });
	assert.deepEqual(await collect({ cwd: root, key: "pi-loopback-light", ...light }), { exitCode: 3, text: "light\n" });
	assert.equal(readFileSync(join(root, "muzzle-calls"), "utf8").match(/^run /gm)?.length, 1);

	// Abort a light command under Pi's timeout: timeout(1) leads a process
	// group of its own, which the hangup of `mule kill` does not reach. The
	// job's shell must stop it, or the command runs on and writes.
	const timedAbort = new AbortController();
	const pid = join(root, "child.pid");
	const timed = bashJob(`echo $$ > ${quote(pid)}; sleep 2; touch ${quote(join(root, "survived"))}`, { muzzle: fake, timeout: 7 });
	const aborted = runJob(ops, { cwd: root, key: "pi-loopback-timed-kill", ...timed }, { signal: timedAbort.signal, onData: () => {} });
	for (let i = 0; i < 400 && !(existsSync(pid) && readFileSync(pid, "utf8").trim()); i++) await delay(25);
	timedAbort.abort();
	await assert.rejects(aborted, /aborted/);
	await delay(2_500);
	assert.ok(!existsSync(join(root, "survived")), "nothing of the aborted job ran on");
	assert.throws(() => process.kill(Number(readFileSync(pid, "utf8")), 0), "the timed child is gone");
});

test("no-master is a handback, not an empty successful batch or a retry", { skip: !process.env.MULE_PI_TEST_NO_MASTER }, async () => {
	const log = process.env.MULE_PI_TEST_CALLS!;
	const before = readFileSync(log, "utf8").length;
	const ops = backend();
	const results = await Promise.allSettled([ops.stat(fixture!), ops.read(join(fixture!, "file0"))]);
	for (const member of results) {
		assert.equal(member.status, "rejected");
		assert.ok(member.reason instanceof MasterRequired);
		assert.match(member.reason.message, /ssh .*-(?:MNf|fNM)/);
	}
	assert.equal(readFileSync(log, "utf8").slice(before), "exec\n");
});

test("serve carries file ops in one child, a burst in shared round trips", { skip: noFixture, timeout: 45_000 }, async () => {
	const root = join(fixture!, "serve");
	mkdirSync(root, { recursive: true });
	const ops = new MuleServeBackend(options());
	try {
		const before = mark();
		const router = workspaceRouter("/loop/workspace", root, loopResources("/loop/workspace", "/loop/home", []), ops);
		const bytes = Buffer.alloc(203_000);
		for (let i = 0; i < bytes.length; i++) bytes[i] = i % 251;
		const path = "a ' $dollar; $(false) ü.txt";
		await router.write(path, bytes);
		assert.deepEqual(readFileSync(join(root, path)), bytes);
		assert.deepEqual(await router.read(path), bytes);
		assert.equal(verbs(before), "serve\n", "write and read ride serve");

		const files = Array.from({ length: 20 }, (_, i) => {
			const body = Buffer.from([0, 10, 255, i]);
			writeFileSync(join(root, `file${i}`), body);
			return body;
		});
		const cancelled = new AbortController();
		const started = performance.now();
		const burst = Promise.allSettled([
			...files.map((_, i) => ops.read(join(root, `file${i}`))),
			ops.read(join(root, "not-here")), ops.stat(root), ops.read(root), ops.stat(join(root, "file0")),
			ops.read(join(root, "file0"), cancelled.signal),
		]);
		cancelled.abort();
		const replies = await burst;
		const elapsed = performance.now() - started;
		for (const [i, value] of files.entries()) assert.deepEqual(replies[i], { status: "fulfilled", value });
		assert.deepEqual(replies.slice(20, 22), [{ status: "fulfilled", value: undefined }, { status: "fulfilled", value: "directory" }]);
		assert.equal(replies[22].status, "rejected");
		assert.deepEqual(replies[23], { status: "fulfilled", value: "file" });
		assert.equal(replies[24].status, "rejected");
		// Loopback has no network: this bounds the batching, not latency.
		assert.ok(elapsed < 5_000, `burst took ${elapsed}ms`);
		assert.equal(verbs(before), "serve\n", "no exec, no second child");

		// A private file's new bytes are never visible wider than it was,
		// even when nothing after serve's write can run: a chmod after the
		// rename left them at 0644 when it failed (review, 2026-11-23).
		writeFileSync(join(root, "secret"), "old\n");
		chmodSync(join(root, "secret"), 0o600);
		const command = ops.jobs.command;
		ops.jobs.command = () => Promise.reject(new MasterRequired("no follow-up may run"));
		try {
			const quiet = mark();
			await ops.write(join(root, "secret"), Buffer.from("new\n"));
			assert.equal(verbs(quiet), "", "a kept mode is serve's alone");
		} finally {
			ops.jobs.command = command;
		}
		assert.equal(readFileSync(join(root, "secret"), "utf8"), "new\n");
		assert.equal(statSync(join(root, "secret")).mode & 0o777, 0o600);

		// Edge writes: a kept mode, a followed symlink, a created parent.
		writeFileSync(join(root, "tool.sh"), "old\n");
		chmodSync(join(root, "tool.sh"), 0o755);
		writeFileSync(join(root, "target"), "target\n");
		symlinkSync("target", join(root, "link"));
		await Promise.all([
			ops.write(join(root, "tool.sh"), Buffer.from("new\n")),
			ops.write(join(root, "link"), Buffer.from("linked\n")),
			ops.write(join(root, "deep/er/file"), Buffer.from("deep\n")),
		]);
		assert.equal(statSync(join(root, "tool.sh")).mode & 0o777, 0o755);
		assert.equal(readFileSync(join(root, "tool.sh"), "utf8"), "new\n");
		assert.equal(readFileSync(join(root, "target"), "utf8"), "linked\n");
		assert.equal(readFileSync(join(root, "deep/er/file"), "utf8"), "deep\n");
		await assert.rejects(ops.write(root, Buffer.from("x")), /directory/);
	} finally {
		ops.close();
	}
});

test("Pi's tools in parallel over one capped master", { skip: noFixture || (!havePi() && "requires a standalone Pi (MULE_TEST_PI)"), timeout: 90_000 }, () => {
	const result = runPiCase("loopback", (dir) => {
		mkdirSync(join(dir, "config/mule"), { recursive: true });
		writeFileSync(join(dir, "config/mule/config.toml"), readFileSync(process.env.MULE_PI_TEST_CONFIG!));
		return {
			MULE_PI_BIN: process.env.MULE_PI_TEST_BIN!, MULE_PI_REAL_BIN: process.env.MULE_PI_REAL_BIN!, MULE_PI_TEST_CALLS: join(dir, "calls"),
		};
	});
	assert.equal(result.status, 0, `${result.error ?? ""}\n${result.stdout}\n${result.stderr}`);
	assert.match(result.stdout, /mule-tool-check: ok/);
});

test("serve without a master is a handback for every queued op", { skip: !process.env.MULE_PI_TEST_NO_MASTER }, async () => {
	const before = mark();
	const ops = new MuleServeBackend(options());
	const results = await Promise.allSettled([ops.stat(fixture!), ops.read(join(fixture!, "file0"))]);
	for (const member of results) {
		assert.equal(member.status, "rejected");
		assert.ok(member.reason instanceof MasterRequired, String(member.reason));
		assert.match(member.reason.message, /ssh .*-(?:MNf|fNM)/);
	}
	assert.equal(verbs(before), "serve\n");
});
