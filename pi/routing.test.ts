import assert from "node:assert/strict";
import { mkdirSync, mkdtempSync, realpathSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { muleBinary } from "./binary.ts";
import { checkedKey, jobKey, loopResources, runJob, watchJob, workspaceRouter } from "./routing.ts";
import { bashJob, MUZZLE_QUEUE_SECS, readOnlyRefusal } from "./policy.ts";
import { MasterRequired, type WorkspaceOps } from "./ops.ts";

test("workspace paths use the provider and loop resources do not", async () => {
	const files = new Map([["/remote/tree/AGENTS.md", Buffer.from("remote instructions")]]);
	const ops = {
		async read(path) { return files.get(path); },
		async write(path, data) { files.set(path, data); },
	} satisfies Pick<WorkspaceOps, "read" | "write">;
	const loop = loopResources("/loop/tree", "/loop/home", ["/packages/pi", "/loop/tree/.pi"]);
	const router = workspaceRouter("/loop/tree", "/remote/tree", loop, ops);
	assert.equal((await router.read("AGENTS.md"))?.toString(), "remote instructions");
	await router.write("/loop/tree/sub/file", Buffer.from("new"));
	assert.equal(files.get("/remote/tree/sub/file")?.toString(), "new");
	for (const path of ["~/.pi/agent/settings.json", "/loop/home/.agents/skills/a/SKILL.md", "/packages/pi/tool.ts", ".pi/settings.json", "~/.ssh/config"]) {
		assert.equal(router.path(path).side, "local", path);
		await assert.rejects(router.write(path, Buffer.from("wrong")), /loop resource/);
	}
	assert.deepEqual(router.path("/loop/tree-next/file"), { side: "remote", path: "/loop/tree-next/file" });
	assert.deepEqual(router.path("/remote/tree/file"), { side: "remote", path: "/remote/tree/file" });
	loop.keepLocal("/extra/package/skills/lint");
	assert.equal(router.path("/extra/package/skills/lint/SKILL.md").side, "local");
});

test("abort during dispatch stops only the returned job, without polling or resubmitting", async () => {
	const controller = new AbortController();
	const unrelated = "abcdef";
	const running = new Set([unrelated]);
	const request = { command: "build", cwd: "/remote/tree", key: jobKey("session", "call") };
	const ops = {
		async exec() {
			controller.abort();
			running.add("123abc");
			return "123abc";
		},
		async kill(id) { assert.ok(running.delete(id)); },
		async output() { assert.fail("output after abort"); },
		async poll() { assert.fail("poll after abort"); },
	} satisfies Pick<WorkspaceOps, "exec" | "kill" | "output" | "poll">;
	await assert.rejects(runJob(ops, request, { signal: controller.signal, onData: () => assert.fail("output after abort") }), /aborted/);
	assert.deepEqual([...running], [unrelated]);
});

test("a poll handback remains a handback even when the caller aborts", async () => {
	const controller = new AbortController();
	const handback = new MasterRequired("Open the master");
	const ops = {
		async exec() { return "123abc"; },
		async kill() { assert.fail("no extra call after a handback"); },
		async output() { assert.fail("no extra call after a handback"); },
		async poll() { controller.abort(); throw handback; },
	} satisfies Pick<WorkspaceOps, "exec" | "kill" | "output" | "poll">;
	await assert.rejects(runJob(ops, { command: "build", cwd: "/remote/tree", key: "test" }, {
		signal: controller.signal, onData: () => assert.fail("output after a handback"),
	}), (error) => error === handback);
});

test("the mule binary comes from the package or the checkout, never PATH", () => {
	const dir = realpathSync(mkdtempSync(join(tmpdir(), "mule-binary-test-")));
	try {
		// A checkout: pi/ beside target/, no package manifest.
		assert.equal(muleBinary(join(dir, "checkout/pi")), join(dir, "checkout/target/debug/mule"));
		assert.equal(muleBinary(join(dir, "checkout/pi"), "/opt/mule"), "/opt/mule");
		assert.throws(() => muleBinary(join(dir, "checkout/pi"), "mule"), /absolute/);

		// The npm layout: the platform package resolved from @mu-crew/mule.
		const modules = join(dir, "node_modules/@mu-crew");
		const platform = `mule-${process.platform}-${process.arch}`;
		mkdirSync(join(modules, "mule/pi"), { recursive: true });
		const manifest = (deps: string[]) => writeFileSync(join(modules, "mule/package.json"), JSON.stringify({
			name: "@mu-crew/mule", optionalDependencies: Object.fromEntries(deps.map((d) => [`@mu-crew/${d}`, "0.0.0"])),
		}));
		manifest([platform]);
		assert.throws(() => muleBinary(join(modules, "mule/pi")), /reinstall @mu-crew\/mule/);
		mkdirSync(join(modules, platform, "bin"), { recursive: true });
		writeFileSync(join(modules, platform, "package.json"), JSON.stringify({ name: `@mu-crew/${platform}` }));
		writeFileSync(join(modules, platform, "bin/mule"), "");
		assert.equal(muleBinary(join(modules, "mule/pi")), join(modules, platform, "bin/mule"));
		manifest(["mule-other-cpu"]);
		assert.throws(() => muleBinary(join(modules, "mule/pi")), /no package for/);
	} finally {
		rmSync(dir, { recursive: true, force: true });
	}
});

test("job keys: deterministic per session and call, MULE_PI_JOB_PREFIX verbatim, always valid for mule", () => {
	assert.equal(jobKey("s", "call"), jobKey("s", "call"));
	assert.notEqual(jobKey("s", "call"), jobKey("s", "other"));
	assert.notEqual(jobKey("s", "call"), jobKey("t", "call"));
	assert.equal(jobKey("s", "call", "mu.m1.3."), `mu.m1.3.${jobKey("s", "call")}`);
	assert.equal(checkedKey("p.", "pi-user-x"), "p.pi-user-x");
	assert.throws(() => jobKey("s", "call", "has space"), /MULE_PI_JOB_PREFIX/);
	assert.throws(() => jobKey("s", "call", "x".repeat(62)), /at most 61/);
	assert.equal(jobKey("s", "call", "x".repeat(61)).length, 128);
});

const finished = (exitCode: number, data: string, extra: { logBytes?: number; cappedAt?: number } = {}) => ({
	async exec() { return "123abc"; },
	async poll() { return { state: "done" as const, exitCode, logBytes: extra.logBytes ?? data.length, drained: true }; },
	async output() { return { data: Buffer.from(data), ...(extra.cappedAt === undefined ? {} : { cappedAt: extra.cappedAt }) }; },
	async kill() { assert.fail("no kill"); },
}) satisfies Pick<WorkspaceOps, "exec" | "kill" | "output" | "poll">;

test("mule's output caps are notes after the output, never an exit code", async () => {
	const collect = async (ops: ReturnType<typeof finished>) => {
		const parts: Buffer[] = [];
		const result = await runJob(ops, { command: "c", cwd: "/r", key: "k" }, { onData: (d) => parts.push(d) });
		return { ...result, text: Buffer.concat(parts).toString() };
	};
	// A command's own 7 is its own: no cap note.
	assert.deepEqual(await collect(finished(7, "out\n")), { exitCode: 7, text: "out\n" });
	assert.deepEqual(await collect(finished(0, "abc", { cappedAt: 3 })), {
		exitCode: 0, text: "abc\n[mule: the log reached max_log_bytes (3 bytes); later output was discarded]\n",
	});
	assert.deepEqual(await collect(finished(1, "tail\n", { logBytes: 99 })), {
		exitCode: 1, text: "tail\n[mule: showing the last 5 of 99 log bytes]\n",
	});
});

test("a done job's log is read once it has drained, so a cap that lands after rc is not lost", async () => {
	// rc lands first; a child the command left still writes, and mule's cap
	// marker comes at the end of the drain.
	let polls = 0;
	let reads = 0;
	const ops = {
		async exec() { return "123abc"; },
		async poll() {
			const drained = ++polls >= 3;
			return { state: "done" as const, exitCode: 7, logBytes: drained ? 10 : 6, drained };
		},
		async output() {
			reads++;
			return polls >= 3 ? { data: Buffer.from("early\n0123"), cappedAt: 10 } : { data: Buffer.from("early\n") };
		},
		async kill() { assert.fail("no kill"); },
	} satisfies Pick<WorkspaceOps, "exec" | "kill" | "output" | "poll">;
	const parts: Buffer[] = [];
	assert.deepEqual(await runJob(ops, { command: "c", cwd: "/r", key: "k" }, { onData: (d) => parts.push(d) }), { exitCode: 7 });
	assert.equal(Buffer.concat(parts).toString(), "early\n0123\n[mule: the log reached max_log_bytes (10 bytes); later output was discarded]\n");
	assert.equal(reads, 1, "one read, after the drain");

	// A log that never drains is not passed off as whole.
	const stuck = { ...ops, async poll() { return { state: "done" as const, exitCode: 7, logBytes: 6, drained: false }; }, async output() { return { data: Buffer.from("early\n") }; } };
	const text: Buffer[] = [];
	assert.deepEqual(await watchJob(stuck, "123abc", { onData: (d) => text.push(d), drainWait: 50 }), { exitCode: 7 });
	assert.equal(Buffer.concat(text).toString(), "early\n[mule: the command exited 7, but a process it started still holds its output; the log may be incomplete]\n");
});

test("abort kills the running job; after a handback it leaves the job running for its key", async () => {
	for (const handedBack of [false, true]) {
		const controller = new AbortController();
		const killed: string[] = [];
		let polls = 0;
		const ops = {
			async exec() { return "123abc"; },
			async poll() { if (++polls === 2) controller.abort(); return { state: "running" as const }; },
			async output() { assert.fail("no output"); },
			async kill(id: string) { killed.push(id); },
		} satisfies Pick<WorkspaceOps, "exec" | "kill" | "output" | "poll">;
		const run = runJob(ops, { command: "c", cwd: "/r", key: "k" }, { signal: controller.signal, onData: () => {}, handedBack: () => handedBack });
		if (handedBack) await assert.rejects(run, (error) => error instanceof MasterRequired && /123abc was left running/.test(error.message));
		else await assert.rejects(run, /aborted/);
		assert.deepEqual(killed, handedBack ? [] : ["123abc"]);
	}
	// A kill that meets a dropped master is a handback, not a quiet abort.
	const controller = new AbortController();
	const ops = {
		async exec() { controller.abort(); return "123abc"; },
		async poll() { return { state: "running" as const }; },
		async output() { assert.fail("no output"); },
		async kill() { throw new MasterRequired("Open the master"); },
	} satisfies Pick<WorkspaceOps, "exec" | "kill" | "output" | "poll">;
	await assert.rejects(runJob(ops, { command: "c", cwd: "/r", key: "k" }, { signal: controller.signal, onData: () => {} }), MasterRequired);
});

test("read-only bash: readers pass, anything that may write is refused", () => {
	for (const command of [
		"cat a.txt", "ls -la src", "rg -n 'a|b' src", "grep -c x a 2>/dev/null", "git log --oneline -5 | head -3",
		"git -C sub diff --stat", "cd src && ls", "find . -name '*.ts' -type f", "sed -n 1,5p a", "awk '{print $1}' a",
		"wc -l a; echo done", "true", "cat a >/dev/null 2>&1", "echo \"a > b\"", "fd -e ts", "sort a | uniq -c",
		"sed -n '/err/p; 3q' a", "sed -E 's/(a|b)+/x/g' a", "sed -e '1d' -e 's|/|:|2' a", "sed ':a;N;$!ba;s/\\n/ /g' a",
		"sed -n '/^start/,/^end/{/x/p}' a", "awk -F: '$3 > 100 {print $1}' /etc/passwd", "awk '/a|b/ {n++} END {print n}' a",
		"awk -v x=1 'NR > x' a", "awk '{print \"a > b\", $2}' a", "uniq -c a", "xxd -l 64 a", "tree -L 2", "git grep -n x",
	]) assert.equal(readOnlyRefusal(command), undefined, command);
	for (const [command, why] of [
		["echo x > a", /redirection/], ["echo x >> a", /redirection/], ["cat a | tee b", /tee/], ["rm -rf src", /rm/],
		["sed -i s/a/b/ f", /sed -i/], ["sed -Ei s/a/b/ f", /sed -i/], ["find . -delete", /find/], ["find . -exec rm {} ;", /find/],
		["fd -x rm", /fd/], ["sort -o a a", /sort -o/], ["git commit -m x", /git commit/], ["git push", /git push/],
		["git -C . checkout main", /git checkout/], ["git diff --output=x", /output/], ["awk 'BEGIN{system(\"rm a\")}'", /awk/],
		["echo $(rm a)", /substitution/], ["echo `rm a`", /substitution/], ["echo \"$(rm a)\"", /substitution/],
		["cat <<EOF\nx\nEOF", /heredoc/], ["sed -n 'w report.txt' a", /sed writing/],
		["sed 's/a/b/w out' a", /sed writing/], ["sed '1e date' a", /sed writing/], ["sed -n '/x/W out' a", /sed writing/],
		["sed -f script.sed a", /sed option/], ["sed --in-place=.bak s/a/b/ a", /sed -i/], ["sed '1r other' a", /sed command/],
		["awk '{print > \"out\"}' a", /awk writing/], ["awk '{print $1 >> \"out\"}' a", /awk writing/],
		["awk '{printf \"%s\\n\", $1 > \"out\"}' a", /awk writing/], ["awk '{print | \"sh\"}' a", /awk piping/],
		["awk '{\"date\" | getline d}' a", /awk/], ["awk -f prog.awk a", /awk option/], ["awk -i inplace '{print}' a", /awk option/],
		["uniq a out", /output file/], ["xxd a out", /output file/], ["tree -o out", /tree -o/], ["rg --pre ./run x", /preprocessor/],
		["git -c core.pager=sh log", /git -c/], ["git grep -O x", /pager/], ["diff <(ls) a", /process substitution/], ["X=1 ls", /assignment/],
		["bash -c 'rm a'", /bash/], ["npm test", /npm/], ["make", /make/], ["ls; python3 x.py", /python3/],
		["echo 'unterminated", /quote/], ["ls && (( x = 1 ))", /arithmetic/], ["xargs rm < list", /xargs/],
	] as const) assert.match(readOnlyRefusal(command) ?? "allowed", why, command);
});

test("bash jobs: plain without a muzzle; with one, heavy commands queue and the deadline counts from admission", () => {
	assert.deepEqual(bashJob("make", {}), { command: "make", timeout: undefined });
	assert.deepEqual(bashJob("make", { timeout: 4.2 }), { command: "make", timeout: 5 });
	const job = bashJob("it's heavy", { muzzle: "/opt/m z/muzzle", timeout: 10 });
	assert.equal(job.timeout, 10 + MUZZLE_QUEUE_SECS + 30);
	assert.match(job.command, /^c='it'\\''s heavy'\nm='\/opt\/m z\/muzzle'\n/);
	assert.match(job.command, /exec "\$m" run --rule="\$r" --slots "\$\{BASH_REMATCH\[1\]\}" --timeout 900 --run-timeout 10 -- bash -c "\$c"/);
	// Light: timeout's own process group is killed on the hangup mule kill sends.
	assert.match(job.command, /trap '\[ -n "\$p" \] && kill -KILL -- -"\$p" "\$p" 2>\/dev\/null; exit 129' HUP TERM INT\n  timeout 10 bash -c "\$c" & p=\$!\n  wait "\$p"; exit\nfi\nexec bash -c "\$c"$/);
	const untimed = bashJob("x", { muzzle: "/m" });
	assert.equal(untimed.timeout, undefined);
	assert.doesNotMatch(untimed.command, /run-timeout|command -v timeout/);
});
