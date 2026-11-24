import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { chmodSync, existsSync, lstatSync, mkdirSync, readFileSync, readlinkSync, rmSync, statSync, symlinkSync, writeFileSync, writeSync } from "node:fs";
import { join } from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { inspect } from "node:util";
import type { ExtensionAPI, ExtensionContext, ToolDefinition } from "@earendil-works/pi-coding-agent";
import { createFindToolDefinition } from "@earendil-works/pi-coding-agent";
import { quote } from "../exec-backend.ts";
import mule from "../mule.ts";
import { jobKey } from "../routing.ts";

const builtins = ["read", "write", "edit", "bash", "grep", "find", "ls", "powershell"];
const dir = process.env.MULE_TOOL_TEST_DIR!;
const cwd = join(dir, "local");
const root = join(dir, "remote [root]");
const calls = () => readFileSync(join(dir, "calls"), "utf8");

function register(flag?: string, settings: Record<string, unknown> = {}, flags: Record<string, string> = {}, branch: unknown[] = []) {
	const tools = new Map<string, ToolDefinition>();
	const handlers = new Map<string, Function>();
	const statuses: string[] = [];
	const notices: string[] = [];
	const localMu: unknown[] = [];
	const messages: { content: string; details?: Record<string, unknown> }[] = [];
	let active = [...builtins];
	let id = 0;
	const aborts: number[] = [];
	let idle = true;
	const ctx = {
		cwd, ui: { setStatus: (_name: string, status: string) => statuses.push(status), notify: (message: string) => notices.push(message) },
		sessionManager: { getSessionId: () => "fixture-session", getBranch: () => branch },
		isIdle: () => idle, abort: () => aborts.push(Date.now()),
	} as unknown as ExtensionContext;
	mule({
		registerFlag() {}, getFlag: (name: string) => name === "mule" ? flag : flags[name], getSettings: () => settings,
		on: (name: string, handler: Function) => handlers.set(name, handler),
		registerTool: (tool: ToolDefinition) => tools.set(tool.name, tool),
		getActiveTools: () => active,
		sendMessage: (message: { content: string; details?: Record<string, unknown> }) => messages.push(message),
		setActiveTools: (names: string[]) => { active = names; },
		exec: async (program: string, args: string[], options: { cwd: string }) => {
			localMu.push([program, args, options.cwd]);
			return { stdout: "local mu", stderr: "", code: 0 };
		},
	} as unknown as ExtensionAPI);
	return {
		tools, active: () => active, statuses, notices, localMu, aborts, messages,
		/** A turn is running: a handback may stop it. */
		busy(value = true) { idle = !value; },
		start: () => handlers.get("session_start")!({}, ctx),
		stop: () => handlers.get("session_shutdown")!({}, ctx),
		async prompt() {
			const options = { cwd, sections: {} as Record<string, string>, contextFiles: [{ path: join(cwd, "AGENTS.md") }] };
			await handlers.get("before_agent_start")!({ systemPromptOptions: options }, ctx);
			return options;
		},
		async run(name: string, params: Record<string, unknown>, options: { id?: string; signal?: AbortSignal } = {}) {
			const result = await tools.get(name)!.execute(options.id ?? `call-${++id}`, params, options.signal, undefined, ctx);
			return result.content.map((part) => part.type === "text" ? part.text : "[image]").join("\n");
		},
		async bang(command: string) {
			const result = await handlers.get("user_bash")!({ command }, ctx);
			if (!result) return;
			const parts: Buffer[] = [];
			await result.operations.exec(command, cwd, { onData: (part: Buffer) => parts.push(part) });
			return Buffer.concat(parts).toString().trim();
		},
	};
}

async function waitFor(what: string, done: () => boolean, ms = 10_000) {
	for (const end = Date.now() + ms; !done(); await delay(25)) if (Date.now() > end) assert.fail(`timed out waiting for ${what}`);
}

// A tool's text whether Pi returns it or throws it (a nonzero exit).
const settle = (result: Promise<string>) => result.catch((error: Error) => `[threw] ${error.message}`);
const enabled = () => register(`fixture:${root}`);
function put(base: string, name: string, content: string) {
	const path = join(base, name);
	mkdirSync(join(path, ".."), { recursive: true });
	writeFileSync(path, content);
}

async function checkFindGitNewline(s: ReturnType<typeof register>) {
	const outer = join(dir, "find-newline");
	const repo = join(outer, "repo\n\n");
	put(outer, ".gitignore", "*.ts\n");
	put(repo, "src/a.ts", "tracked\n");
	for (const args of [["init", "-q"], ["add", "--", "src/a.ts"]]) {
		const git = spawnSync("git", args, { cwd: repo, encoding: "utf8", timeout: 5_000 });
		assert.equal(git.status, 0, git.error?.message ?? git.stderr);
	}
	const params = { pattern: "./*.ts", path: join(repo, "src") };
	const oracle = await createFindToolDefinition(dir).execute("oracle", params, undefined, undefined, {} as ExtensionContext);
	const local = oracle.content.map((part) => part.type === "text" ? part.text : "").join("\n");
	assert.match(local, /(?:^|\/)a\.ts$/m, "Pi finds the tracked file despite the ignore above the repo");
	assert.match(await s.run("find", params), /(?:^|\/)a\.ts$/m, "remote find must also find the tracked file");
}

const checks = {
	async routing() {
		const off = register();
		const before = calls();
		await off.start();
		assert.equal(off.tools.size, 0);
		assert.deepEqual(off.active(), builtins);
		assert.equal(await off.bang("pwd"), undefined);
		assert.deepEqual((await off.prompt()).sections, {});
		assert.equal(calls(), before);

		// A project setting turns it on as the flag does; the flag wins.
		const set = register(undefined, { mule: `fixture:${root}` });
		await set.start();
		assert.equal(set.statuses.at(-1), `mule fixture:${root}`);
		const wins = register(`fixture:${join(root, "src")}`, { mule: `fixture:${root}` });
		await wins.start();
		assert.equal(wins.statuses.at(-1), `mule fixture:${join(root, "src")}`);
		await set.stop();
		await wins.stop();

		const session = calls().length;
		const s = enabled();
		await s.start();
		assert.deepEqual([...s.tools.keys()].sort(), [...builtins, "loop_mu"].sort());
		assert.deepEqual(s.active(), [...builtins.filter((name) => name !== "powershell"), "loop_mu"]);
		assert.equal(s.tools.get("powershell")!.exposure, "hidden");
		await assert.rejects(s.run("powershell", { command: "pwd" }), /unavailable/);
		assert.equal((await s.prompt()).cwd, root);
		assert.equal((await s.prompt()).contextFiles.length, 0);
		assert.match((await s.prompt()).sections.mule, /remote instructions/);
		assert.match(await s.run("read", { path: "shared.txt" }), /remote text/);
		await s.run("write", { path: "added.txt", content: "new text" });
		await s.run("edit", { path: "shared.txt", edits: [{ oldText: "remote text", newText: "edited text" }] });
		assert.equal(readFileSync(join(root, "added.txt"), "utf8"), "new text");
		assert.equal(readFileSync(join(root, "shared.txt"), "utf8"), "edited text\n");
		assert.equal(readFileSync(join(cwd, "shared.txt"), "utf8"), "local text\n");
		assert.ok(!existsSync(join(cwd, "added.txt")));
		assert.equal(calls().slice(session).match(/^exec$/gm)?.length, 1, "file tools ride serve; only the root probe is an exec");
		assert.equal(calls().slice(session).match(/^serve$/gm)?.length, 1, "one serve child for the session");

		// serve replaces by rename and keeps the mode itself, with no exec
		// after it; the backend follows a symlink and creates missing
		// parents through a fixed exec.
		chmodSync(join(root, "tool.sh"), 0o755);
		put(root, "secret.txt", "old\n");
		chmodSync(join(root, "secret.txt"), 0o600);
		const kept = calls().length;
		await s.run("edit", { path: "tool.sh", edits: [{ oldText: "old", newText: "new" }] });
		await s.run("edit", { path: "secret.txt", edits: [{ oldText: "old", newText: "new" }] });
		assert.equal(readFileSync(join(root, "tool.sh"), "utf8"), "#!/bin/sh\necho new\n");
		assert.equal(statSync(join(root, "tool.sh")).mode & 0o777, 0o755);
		assert.equal(readFileSync(join(root, "secret.txt"), "utf8"), "new\n");
		assert.equal(statSync(join(root, "secret.txt")).mode & 0o777, 0o600);
		assert.equal(calls().slice(kept).match(/^exec$/gm), null, "a kept mode needs no exec");
		symlinkSync("shared.txt", join(root, "link.txt"));
		await s.run("write", { path: "link.txt", content: "through the link" });
		assert.equal(readlinkSync(join(root, "link.txt")), "shared.txt");
		assert.equal(readFileSync(join(root, "shared.txt"), "utf8"), "through the link");
		await s.run("write", { path: "new/dir/file.txt", content: "nested" });
		assert.equal(readFileSync(join(root, "new/dir/file.txt"), "utf8"), "nested");
		assert.ok(lstatSync(join(root, "new/dir")).isDirectory());
		await assert.rejects(s.run("write", { path: "src", content: "x" }), /directory/);
		await assert.rejects(s.run("read", { path: "absent.txt" }), /not found/);
		assert.equal((await s.run("bash", { command: "pwd -P" })).trim(), root);
		assert.equal(await s.bang("pwd -P"), root);
		assert.match(calls(), /run\npoll\ntail/);

		const beforeLocal = calls();
		assert.match(await s.run("read", { path: ".pi/loop.txt" }), /loop-only text/);
		assert.match(await s.run("grep", { path: ".pi", pattern: "loop-only text" }), /loop\.txt/);
		assert.match(await s.run("find", { path: ".pi", pattern: "*.txt" }), /loop\.txt/);
		assert.match(await s.run("ls", { path: ".pi" }), /loop\.txt/);
		await assert.rejects(s.run("write", { path: ".pi/loop.txt", content: "bad" }), /loop resource/);
		await assert.rejects(s.run("edit", { path: ".pi/loop.txt", edits: [{ oldText: "loop-only text", newText: "bad" }] }), /loop resource/);
		await assert.rejects(s.run("bash", { command: "mu task show fixture" }), /loop_mu/);
		await assert.rejects(s.bang(" 'mu' agent list"), /loop_mu/);
		for (const args of [["task", "show", "fixture"], ["wait", "--all"], ["agent", "list"]]) {
			assert.match(await s.run("loop_mu", { args }), /local mu/);
		}
		assert.deepEqual(s.localMu, [
			["mu", ["task", "show", "fixture"], cwd], ["mu", ["wait", "--all"], cwd], ["mu", ["agent", "list"], cwd],
		]);
		assert.equal(calls(), beforeLocal);
	},
	async find() {
		const s = enabled();
		await s.start();
		const find = async (pattern: string, path = ".") => (await s.run("find", { pattern, path })).split("\n").map((file) => file.replace(`${join(root, path)}/`, "").replace(/^\.\//, "")).sort();
		assert.deepEqual(await find("**/*.ts"), [".hidden.ts", "outer/src/other.ts", "src/a.ts", "src/deep/b.ts"]);
		assert.deepEqual(await find("src/*.ts"), ["src/a.ts"]);
		assert.deepEqual(await find("./src/**/*.ts"), ["src/a.ts", "src/deep/b.ts"]);
		assert.deepEqual(await find("./*.ts"), [".hidden.ts", "outer/src/other.ts", "src/a.ts", "src/deep/b.ts"]);
		assert.deepEqual(await find("././*.ts"), await find("*.ts"));
		assert.deepEqual(await find("deep/*.ts", "src"), ["deep/b.ts"]);
		assert.deepEqual(await find("*.ts"), [".hidden.ts", "outer/src/other.ts", "src/a.ts", "src/deep/b.ts"]);
		assert.equal((await s.run("find", { pattern: "**/*.ts", limit: 1 })).split("\n").length, 1);
		assert.deepEqual(await find("src/*.rs"), ["No results"]);
	},
	async findgit() {
		// Oracle: Pi's own local find over the same tree. A .gitignore above the
		// repo root must not hide tracked files; outside any repo it must apply.
		const s = enabled();
		await s.start();
		const local = createFindToolDefinition(dir);
		for (const [name, files] of [["inrepo", ["repo/.git/HEAD", "repo/src/a.ts", "repo/n/src/b.ts"]], ["norepo", ["src/a.ts", "n/b.ts"]]] as const) {
			put(dir, `${name}/.gitignore`, "*.ts\n");
			for (const file of files) put(dir, `${name}/${file}`, "x\n");
		}
		const norm = (text: string, base: string) => text.split("\n").filter((f) => f !== "No results" && f !== "No files found matching pattern").map((f) => f.replace(`${base}/`, "").replace(/^\.\//, "")).sort();
		for (const [path, pattern] of [["inrepo/repo", "*.ts"], ["inrepo/repo/n", "*.ts"], ["inrepo/repo", "src/*.ts"], ["norepo", "*.ts"], ["norepo", "src/*.ts"]]) {
			const abs = join(dir, path);
			const want = await local.execute("oracle", { pattern, path: abs }, undefined, undefined, {} as ExtensionContext);
			const got = await s.run("find", { pattern, path: join(root, "..", path) });
			assert.deepEqual(norm(got, abs), norm(want.content.map((p) => p.type === "text" ? p.text : "").join("\n"), abs), `${path} ${pattern}`);
		}
		assert.equal(norm(await s.run("find", { pattern: "*.ts", path: join(dir, "inrepo/repo") }), join(dir, "inrepo/repo")).length, 2, "tracked files survive a parent .gitignore");
		assert.equal(norm(await s.run("find", { pattern: "*.ts", path: join(dir, "norepo") }), join(dir, "norepo")).length, 0, "outside a repo the .gitignore applies");
		await checkFindGitNewline(s);
	},
	async search() {
		const s = enabled();
		await s.start();
		const grep = await s.run("grep", { pattern: "a.b", literal: true, ignoreCase: true, glob: "*.ts", context: 1 });
		assert.match(grep, /src\/a\.ts:2:A\.B/);
		assert.match(grep, /src\/a\.ts-1-before/);
		assert.doesNotMatch(grep, /:3:axb|excluded|\.git\//);
		assert.equal(await s.run("grep", { pattern: "absent token" }), "No results");
		assert.equal((await s.run("grep", { pattern: "A.B", literal: true, glob: "*.ts", limit: 1 })).split("\n").length, 1);
		assert.deepEqual((await s.run("ls", { path: "src" })).split("\n").sort(), ["a.ts", "deep/", "excluded.txt"]);
		assert.equal((await s.run("ls", { path: "src", limit: 1 })).split("\n").length, 1);
		await assert.rejects(s.run("ls", { path: "missing" }));
	},
	async loopback() {
		// The real mule against a MaxSessions=1 loopback sshd (pi/loopback.test.ts):
		// the remote tree is this machine's, under the same path.
		const s = register(`live:${root}`);
		await s.start();
		assert.equal(s.statuses.at(-1), `mule live:${root}`);
		assert.equal((await s.prompt()).cwd, root);
		assert.match((await s.prompt()).sections.mule, /remote instructions/);
		chmodSync(join(root, "tool.sh"), 0o755);
		put(root, "secret.txt", "old\n");
		chmodSync(join(root, "secret.txt"), 0o600);
		put(root, "target.txt", "target\n");
		symlinkSync("target.txt", join(root, "link.txt"));
		const before = calls();
		// One burst, as a model's parallel tool calls arrive: every call shares
		// the one capped master, and the reads share serve's round trips.
		const [reads, nested, added, edited, secret, linked, bash, ls, bang] = await Promise.all([
			Promise.all(Array.from({ length: 12 }, (_, i) => s.run("read", { path: i % 2 ? "shared.txt" : "src/deep/b.ts" }))),
			s.run("write", { path: "burst/dir/new.txt", content: "nested" }),
			s.run("write", { path: "added.txt", content: "added" }),
			s.run("edit", { path: "tool.sh", edits: [{ oldText: "old", newText: "new" }] }),
			s.run("edit", { path: "secret.txt", edits: [{ oldText: "old", newText: "new" }] }),
			s.run("write", { path: "link.txt", content: "through the link" }),
			s.run("bash", { command: "printf 'bash %s' ok" }),
			s.run("ls", { path: "src" }),
			s.bang("pwd -P"),
		]);
		reads.forEach((text, i) => assert.match(text, i % 2 ? /remote text/ : /A\.B/));
		for (const result of [nested, added, edited, secret, linked]) assert.doesNotMatch(result, /error/i);
		assert.equal(readFileSync(join(root, "burst/dir/new.txt"), "utf8"), "nested");
		assert.equal(readFileSync(join(root, "added.txt"), "utf8"), "added");
		assert.equal(readFileSync(join(root, "tool.sh"), "utf8"), "#!/bin/sh\necho new\n");
		assert.equal(statSync(join(root, "tool.sh")).mode & 0o777, 0o755);
		assert.equal(readFileSync(join(root, "secret.txt"), "utf8"), "new\n");
		assert.equal(statSync(join(root, "secret.txt")).mode & 0o777, 0o600);
		assert.equal(readlinkSync(join(root, "link.txt")), "target.txt");
		assert.equal(readFileSync(join(root, "target.txt"), "utf8"), "through the link");
		assert.match(bash, /bash ok/);
		assert.deepEqual(ls.split("\n").sort(), ["a.ts", "deep/", "excluded.txt"]);
		assert.equal(bang, root);
		const burst = calls().slice(before.length);
		assert.equal(burst.match(/^serve$/gm), null, "the startup serve child carries the burst");
		// exec only for what serve cannot do: mkdir -p and a symlink. serve
		// keeps a mode itself.
		assert.equal(burst.match(/^exec$/gm)?.length, 2, burst);
		assert.equal(burst.match(/^run$/gm)?.length, 3, burst);
		await checkFindGitNewline(s);

		// Esc on a running bash: the real `mule kill` stops the remote job.
		const controller = new AbortController();
		const running = s.run("bash", { command: "touch pi-started; sleep 20.9876; touch pi-finished" }, { signal: controller.signal });
		for (let i = 0; i < 400 && !existsSync(join(root, "pi-started")); i++) await delay(25);
		assert.ok(existsSync(join(root, "pi-started")));
		const beforeKill = calls().length;
		controller.abort();
		await assert.rejects(running, /abort/i);
		assert.match(calls().slice(beforeKill), /^kill$/m);
		await delay(500);
		assert.ok(!existsSync(join(root, "pi-finished")));
		await s.stop();
	},
	async cancel() {
		// Abort stops exactly the running remote job with `mule kill`.
		const s = enabled();
		await s.start();
		const controller = new AbortController();
		const started = join(root, "started");
		const running = s.run("bash", { command: `touch ${quote(started)}; sleep 30; touch finished` }, { signal: controller.signal });
		for (let i = 0; i < 200 && !existsSync(started); i++) await delay(25);
		assert.ok(existsSync(started), "the job started");
		const before = calls().length;
		controller.abort();
		await assert.rejects(running, /abort/i);
		assert.match(calls().slice(before), /^kill$/m);
		assert.equal(readFileSync(join(dir, "killed"), "utf8").trim().split("\n").length, 1);
		await delay(300);
		assert.ok(!existsSync(join(root, "finished")), "the job was stopped, not left to finish");
		// An abort before anything ran starts nothing.
		const runs = readFileSync(join(dir, "runs"), "utf8");
		await assert.rejects(s.run("bash", { command: "touch never" }, { signal: AbortSignal.abort() }), /abort/i);
		assert.equal(readFileSync(join(dir, "runs"), "utf8"), runs);
		assert.ok(!existsSync(join(root, "never")));
		// A serve read cancelled in flight is cancelled alone. The fixture holds
		// `held.txt` until it is cancelled, and the abort waits for serve to have
		// it, so this is the in-flight cancel, not an op never sent.
		put(root, "held.txt", "held text");
		const read = new AbortController();
		const from = calls().length;
		const pending = s.run("read", { path: "held.txt" }, { signal: read.signal });
		pending.catch(() => { /* asserted below */ });
		await waitFor("serve to receive the held read", () => /^serve:held \d+$/m.test(calls().slice(from)));
		const held = /^serve:held (\d+)$/m.exec(calls().slice(from))![1];
		const serves = calls().match(/^serve$/gm)?.length;
		// A sibling on the same child finishes while the held read is in flight.
		assert.match(await s.run("read", { path: "shared.txt" }), /remote text/);
		read.abort();
		await assert.rejects(pending);
		await waitFor(`serve to get the cancel for op ${held}`, () => new RegExp(`^serve:cancel ${held}$`, "m").test(calls().slice(from)));
		assert.deepEqual(calls().slice(from).match(/^serve:cancel \d+$/gm), [`serve:cancel ${held}`], "only the aborted op is cancelled");
		assert.match(await s.run("read", { path: "shared.txt" }), /remote text/);
		assert.equal(calls().match(/^serve$/gm)?.length, serves, "same serve child");
		await s.stop();
	},
	async reattach() {
		// A restarted Pi replays a call with the same session and call id: it
		// gets the first job's result, and the command does not run again.
		const command = `echo ran >> ${join(dir, "count")}; echo first answer`;
		const first = enabled();
		await first.start();
		assert.match(await first.run("bash", { command }, { id: "toolu_same" }), /first answer/);
		await first.stop();
		const again = enabled();
		await again.start();
		assert.match(await again.run("bash", { command }, { id: "toolu_same" }), /first answer/);
		assert.equal(readFileSync(join(dir, "count"), "utf8"), "ran\n");
		// A new call id is a new job.
		await again.run("bash", { command }, { id: "toolu_other" });
		assert.equal(readFileSync(join(dir, "count"), "utf8"), "ran\nran\n");
		const keys = () => readFileSync(join(dir, "runs"), "utf8").trim().split("\n").map((line) => JSON.parse(line).key as string);
		assert.equal(keys()[0], keys()[1]);
		assert.notEqual(keys()[1], keys()[2]);
		assert.ok(keys().every((key) => /^pi-[0-9a-f]{64}$/.test(key)), "no prefix unless asked for");
		await again.stop();

		// MULE_PI_JOB_PREFIX goes verbatim on every detached job key, `!` and searches too.
		process.env.MULE_PI_JOB_PREFIX = "mu.machine-1.7.";
		try {
			const s = enabled();
			await s.start();
			const from = keys().length;
			await s.run("bash", { command: "true" });
			await s.bang("true");
			await s.run("ls", { path: "src" });
			assert.deepEqual(keys().slice(from).map((key) => key.replace(/(?:[0-9a-f]{64}|[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12})$/, "")), ["mu.machine-1.7.pi-", "mu.machine-1.7.pi-user-", "mu.machine-1.7.pi-"]);
			await s.stop();
			process.env.MULE_PI_JOB_PREFIX = "bad prefix/";
			const bad = enabled();
			await bad.start();
			const before = keys().length;
			await assert.rejects(bad.run("bash", { command: "true" }), /MULE_PI_JOB_PREFIX/);
			assert.equal(keys().length, before, "an invalid key starts nothing");
			await bad.stop();
		} finally {
			delete process.env.MULE_PI_JOB_PREFIX;
		}

		// Pi stopped mid-call: the branch holds the call with no result, and
		// its job runs on. Pi does not replay the call on reopen, so the
		// extension finds the job by its key and reports its result.
		const prefix = process.env.MULE_PI_JOB_PREFIX = "mu.machine-1.8.";
		try {
			const gate = join(dir, "lost-gate");
			const lost = `while [ ! -f ${quote(gate)} ]; do sleep 0.05; done; echo ran >> ${quote(join(dir, "lost-count"))}; echo lost answer; exit 4`;
			const seeded = spawnSync(join(dir, "mule"), ["--quiet", "run", "--host", "fixture", "--key", jobKey("fixture-session", "toolu_lost", prefix), "--cwd", root, "--", `exec bash -c ${quote(lost)}`], { encoding: "utf8" });
			assert.equal(seeded.status, 0, seeded.stderr);
			const job = seeded.stdout.trim();
			const call = (id: string, command: string) => ({ type: "toolCall", id, name: "bash", arguments: { command } });
			const branch: unknown[] = [
				{ type: "message", message: { role: "user", content: "go" } },
				{ type: "message", message: { role: "assistant", content: [call("toolu_lost", lost), call("toolu_never", "touch never"), call("toolu_done", "true")] } },
				{ type: "message", message: { role: "toolResult", toolCallId: "toolu_done", content: [] } },
			];
			const runs = readFileSync(join(dir, "runs"), "utf8");
			const before = calls().length;
			const reopened = register(`fixture:${root}`, {}, {}, branch);
			await reopened.start();
			await waitFor("the reattach notice", () => reopened.messages.length > 0);
			assert.match(reopened.messages[0].content, new RegExp(`toolu_lost[\\s\\S]*job ${job} kept running[\\s\\S]*Do not run it again`));
			writeFileSync(gate, "");
			await waitFor("the reattached result", () => reopened.messages.length > 1);
			assert.match(reopened.messages[1].content, /exited with code 4[\s\S]*lost answer/);
			assert.deepEqual(reopened.messages[1].details, { toolCallId: "toolu_lost", job, exitCode: 4 });
			assert.equal(readFileSync(join(dir, "lost-count"), "utf8"), "ran\n", "watched, never run again");
			assert.equal(readFileSync(join(dir, "runs"), "utf8"), runs, "nothing dispatched: not the lost call, not the one never started");
			assert.equal(calls().slice(before).match(/^lookup$/gm)?.length, 2, "one lookup per call without a result");
			assert.ok(!existsSync(join(root, "never")));
			await reopened.stop();

			// Reported once: the next reopen sees the result in the branch.
			const after = calls().length;
			const again = register(`fixture:${root}`, {}, {}, [...branch.slice(0, 3), { type: "custom_message", customType: "mule-reattach", details: reopened.messages[1].details }]);
			await again.start();
			await delay(300);
			assert.equal(calls().slice(after).match(/^lookup$/gm)?.length, 1, "only the call never started is looked up");
			assert.deepEqual(again.messages, []);
			await again.stop();

			// The master drops while a reattached job is watched: the user is
			// told, nothing retries in the background, and the next workspace
			// call after the master is back resumes the watch. The result is
			// reported once, and the job is never dispatched again.
			const gate2 = join(dir, "lost-gate-2");
			const lost2 = `while [ ! -f ${quote(gate2)} ]; do sleep 0.05; done; echo ran >> ${quote(join(dir, "lost-count-2"))}; echo second answer; exit 5`;
			const seeded2 = spawnSync(join(dir, "mule"), ["--quiet", "run", "--host", "fixture", "--key", jobKey("fixture-session", "toolu_lost2", prefix), "--cwd", root, "--", `exec bash -c ${quote(lost2)}`], { encoding: "utf8" });
			assert.equal(seeded2.status, 0, seeded2.stderr);
			const job2 = seeded2.stdout.trim();
			const flaky = register(`fixture:${root}`, {}, {}, [
				{ type: "message", message: { role: "user", content: "go" } },
				{ type: "message", message: { role: "assistant", content: [call("toolu_lost2", lost2)] } },
			]);
			await flaky.start();
			await waitFor("the second reattach notice", () => flaky.messages.length > 0);
			const down = join(dir, "down");
			writeFileSync(down, "3");
			await waitFor("the master-loss notice", () => flaky.notices.some((notice) => /SSH master/.test(notice) && notice.includes(job2)));
			assert.match(flaky.statuses.at(-1)!, /SSH master down/);
			assert.deepEqual(flaky.aborts, [], "a background watch does not stop a turn");
			rmSync(down);
			writeFileSync(gate2, "");
			await waitFor("the job to finish", () => existsSync(join(dir, "jobs", job2, "drained")));
			const idle = calls().length;
			await delay(400);
			assert.equal(calls().slice(idle), "", "no background retry");
			assert.equal(flaky.messages.length, 1, "no result yet");
			const runs2 = readFileSync(join(dir, "runs"), "utf8");
			await flaky.run("ls", { path: "src" });
			await waitFor("the resumed result", () => flaky.messages.length > 1);
			assert.match(flaky.messages[1].content, /exited with code 5[\s\S]*second answer/);
			assert.deepEqual(flaky.messages[1].details, { toolCallId: "toolu_lost2", job: job2, exitCode: 5 });
			assert.equal(flaky.statuses.at(-1), `mule fixture:${root}`);
			await flaky.run("ls", { path: "src" });
			await delay(300);
			assert.equal(flaky.messages.length, 2, "reported once, no second notice");
			assert.equal(readFileSync(join(dir, "lost-count-2"), "utf8"), "ran\n");
			assert.equal(readFileSync(join(dir, "runs"), "utf8").slice(runs2.length).trim().split("\n").length, 2, "only the two ls jobs were dispatched");
			await flaky.stop();
		} finally {
			delete process.env.MULE_PI_JOB_PREFIX;
		}
	},
	async handback() {
		const s = enabled();
		await s.start();
		s.busy();
		// The command's own 3 is a result, not a handback.
		const own = await settle(s.run("bash", { command: "echo own; exit 3" }));
		assert.match(own, /^own\n[\s\S]*Command exited with code 3$/);
		assert.doesNotMatch(own, /HANDBACK|master/i);
		assert.deepEqual(s.aborts, []);
		assert.doesNotMatch(s.statuses.at(-1)!, /master/);

		// mule's 3: the turn stops, the user is told once, nothing retries.
		const down = join(dir, "down");
		writeFileSync(down, "3");
		const before = calls().length;
		await assert.rejects(s.run("bash", { command: "echo hi" }), /^Error: HANDBACK: .*ask the user/s);
		assert.equal(calls().slice(before), "run\n", "one attempt, no spin");
		assert.equal(s.aborts.length, 1);
		assert.match(s.notices.at(-1)!, /SSH master for fixture[\s\S]*ssh -MNf fixture/);
		assert.match(s.statuses.at(-1)!, /SSH master down/);
		const notices = s.notices.length;
		await assert.rejects(s.run("ls", { path: "src" }), /HANDBACK/);
		await assert.rejects(s.run("grep", { pattern: "x" }), /HANDBACK/);
		assert.equal(s.aborts.length, 1, "one stop per turn");
		assert.equal(s.notices.length, notices, "one notice per turn");
		assert.match((await s.prompt()).sections.mule, /HANDBACK, stop/);

		// The master is back: the next call reconnects, with no restart.
		rmSync(down);
		assert.match(await s.run("bash", { command: "echo back" }), /back/);
		assert.equal(s.statuses.at(-1), `mule fixture:${root}`);
		await s.stop();
	},
	async readonly() {
		for (const s of [register(`fixture:${root}`, {}, { "mule-role": "read-only" }), register(`fixture:${root}`, { muleRole: "read-only" }), register(`fixture:${root}`, {}, { "mule-role": "reviewer" })]) {
			await s.start();
			assert.deepEqual(s.active(), ["read", "bash", "grep", "find", "ls", "loop_mu"]);
			assert.match((await s.prompt()).sections.mule, /Role: read-only/);
			const before = calls();
			await assert.rejects(s.run("write", { path: "shared.txt", content: "x" }), /Read-only role/);
			await assert.rejects(s.run("edit", { path: "shared.txt", edits: [{ oldText: "remote", newText: "x" }] }), /Read-only role/);
			for (const command of ["echo x > shared.txt", "rm -rf src", "sed -i s/a/b/ shared.txt", "cat shared.txt | tee copy", "git commit -m x", "find . -delete", "echo $(rm x)", "make", "X=1 cat a",
				"sed -n 'w report.txt' shared.txt", "awk '{print > \"report.txt\"}' shared.txt"]) {
				await assert.rejects(s.run("bash", { command }), /Read-only role: refused/, command);
			}
			assert.equal(calls(), before, "refused before any mule call");
			assert.equal(readFileSync(join(root, "shared.txt"), "utf8"), "remote text\n");
			assert.ok(!existsSync(join(root, "report.txt")));
			assert.match(await s.run("bash", { command: "cat shared.txt 2>/dev/null | grep -c 'remote|x' ; ls src >/dev/null && echo ok" }), /ok/);
			assert.match(await s.run("read", { path: "shared.txt" }), /remote text/);
			// `!` is the user's own command, not the agent's.
			assert.equal(await s.bang("touch by-user && echo done"), "done");
			rmSync(join(root, "by-user"));
			await s.stop();
		}
		process.env.MULE_PI_ROLE = "read-only";
		try {
			const s = enabled();
			await s.start();
			assert.ok(!s.active().includes("write"));
			await s.stop();
		} finally {
			delete process.env.MULE_PI_ROLE;
		}
		const unknown = register(`fixture:${root}`, {}, { "mule-role": "reviewer" });
		await unknown.start();
		assert.match(unknown.notices.join("\n"), /Unknown mule role reviewer: running read-only/);
		const full = register(`fixture:${root}`, {}, { "mule-role": "full-access" });
		await full.start();
		assert.ok(full.active().includes("write") && full.active().includes("edit"));
		await full.run("bash", { command: "echo full > full.txt" });
		assert.equal(readFileSync(join(root, "full.txt"), "utf8"), "full\n");
	},
	async muzzle() {
		// The host's muzzle, found once at startup: heavy commands queue there,
		// light ones run as they are.
		const fake = join(dir, "bin/muzzle");
		put(dir, "bin/muzzle", `#!/bin/bash
echo "$*" >> ${join(dir, "muzzle-calls")}
case $1 in
  wrap) if [[ \${@: -1} == *HEAVY* ]]; then echo '{"command":"muzzle run --rule=fake-heavy --slots 2 -- bash -c x","rule":"fake-heavy","slots":2}'; else echo '{"command":null}'; fi ;;
  run) while [ "$1" != -- ]; do shift; done; shift; echo "muzzle: admitted (fake)" >&2; exec "$@" ;;
esac
`);
		chmodSync(fake, 0o755);
		const muzzleCalls = () => existsSync(join(dir, "muzzle-calls")) ? readFileSync(join(dir, "muzzle-calls"), "utf8") : "";
		const runs = () => readFileSync(join(dir, "runs"), "utf8").trim().split("\n").map((line) => JSON.parse(line));
		process.env.MULE_PI_MUZZLE = fake;
		try {
			const s = enabled();
			const execs = calls().match(/^exec$/gm)?.length ?? 0;
			await s.start();
			assert.equal((calls().match(/^exec$/gm)?.length ?? 0) - execs, 1, "found in the root probe's round trip");
			const heavy = await s.run("bash", { command: "echo HEAVY 'it''s' \"$((1+1))\"", timeout: 5 });
			assert.match(heavy, /muzzle: admitted \(fake\)[\s\S]*HEAVY its 2/);
			assert.match(muzzleCalls(), /^run --rule=fake-heavy --slots 2 --timeout 900 --run-timeout 5 -- bash -c echo HEAVY/m);
			assert.equal(runs().at(-1).maxSecs, "935", "mule's deadline is a backstop past the queue");
			const light = await s.run("bash", { command: "echo light" });
			assert.equal(light.trim(), "light");
			assert.equal(muzzleCalls().match(/^run /gm)?.length, 1, "a light command is not queued");
			assert.equal(runs().at(-1).maxSecs, undefined);
			// Esc on a light command with a timeout: timeout(1) leads a process
			// group of its own, and the abort must stop that too.
			const controller = new AbortController();
			const pid = join(root, "child.pid");
			const timed = s.run("bash", { command: `echo $$ > ${quote(pid)}; sleep 2; touch ${quote(join(root, "survived"))}`, timeout: 7 }, { signal: controller.signal });
			await waitFor("the timed child", () => existsSync(pid) && readFileSync(pid, "utf8").trim() !== "");
			controller.abort();
			await assert.rejects(timed, /abort/i);
			await delay(2_500);
			assert.ok(!existsSync(join(root, "survived")), "nothing of the aborted job ran on");
			assert.throws(() => process.kill(Number(readFileSync(pid, "utf8")), 0), "the timed child is gone");
			// Gone since startup: the command still runs, unwrapped.
			rmSync(fake);
			assert.equal((await s.run("bash", { command: "echo HEAVY gone" })).trim(), "HEAVY gone");
			await s.stop();
		} finally {
			delete process.env.MULE_PI_MUZZLE;
		}
		// No muzzle on the host: nothing is wrapped and Pi's timeout is mule's.
		process.env.MULE_PI_MUZZLE = join(dir, "bin/absent");
		try {
			const s = enabled();
			await s.start();
			assert.equal((await s.run("bash", { command: "echo HEAVY", timeout: 5 }, { id: "unmuzzled" })).trim(), "HEAVY");
			assert.equal(runs().at(-1).maxSecs, "5");
			await s.stop();
		} finally {
			delete process.env.MULE_PI_MUZZLE;
		}
	},
	async caps() {
		const s = enabled();
		await s.start();
		// A command's own 7 is its result, with no word of a cap.
		const seven = await settle(s.run("bash", { command: "echo seven; exit 7" }));
		assert.match(seven, /^seven\n[\s\S]*Command exited with code 7$/);
		assert.doesNotMatch(seven, /max_log_bytes|incomplete|mule:/);
		// mule's cap is reported as such, beside the command's own 0.
		writeFileSync(join(dir, "max_log"), "1000");
		const capped = await s.run("bash", { command: "head -c 5000 /dev/zero | tr '\\0' x; echo" });
		assert.equal(capped.trimEnd(), `${"x".repeat(1000)}\n[mule: the log reached max_log_bytes (1000 bytes); later output was discarded]`);
		// The cap lands after rc: a child the command left still writes, and
		// mule marks the cap only when the log drains. The answer waits for it.
		writeFileSync(join(dir, "max_log"), "10");
		const late = await settle(s.run("bash", { command: "echo early; (sleep 0.5; printf 0123456789abcdef) & exit 7" }));
		assert.match(late, /^early\n0123\n\[mule: the log reached max_log_bytes \(10 bytes\); later output was discarded\]\n[\s\S]*Command exited with code 7$/);
		rmSync(join(dir, "max_log"));
		// One tail reads the last 64 KiB of a longer log, and says so.
		const long = await s.run("bash", { command: "head -c 100000 /dev/zero | tr '\\0' y; echo end" });
		// Pi keeps a long result's tail (50 KB); the note is there, so it survives.
		assert.match(long, /\[mule: showing the last 65536 of 100004 log bytes\]\n/);
		await s.stop();
	},
	async startup() {
		for (const [code, message] of [[3, /ssh -MNf fixture/], [1, /Fixture startup I\/O failure/]] as const) {
			const down = join(dir, "down");
			writeFileSync(down, String(code));
			const s = enabled();
			const beforeStart = calls();
			await s.start();
			assert.equal(calls().slice(beforeStart.length), "exec\n");
			assert.match(s.notices.at(-1)!, message);
			const prompt = await s.prompt();
			assert.match(prompt.sections.mule, /unavailable/i);
			assert.equal(prompt.contextFiles.length, 0, "don't expose the local checkout's instructions while down");
			const beforeLocal = calls();
			assert.match(await s.run("read", { path: ".pi/loop.txt" }), /loop-only text/);
			assert.match(await s.run("grep", { path: ".pi", pattern: "loop-only text" }), /loop\.txt/);
			assert.match(await s.run("find", { path: ".pi", pattern: "*.txt" }), /loop\.txt/);
			assert.match(await s.run("ls", { path: ".pi" }), /loop\.txt/);
			assert.equal(calls(), beforeLocal, "loop resources don't need remote startup");
			const beforeRetry = calls();
			await assert.rejects(s.run("read", { path: "shared.txt" }), message);
			assert.equal(calls().slice(beforeRetry.length), "exec\n", "retry once, without falling back to the local file");
			assert.match(s.statuses.at(-1)!, code === 3 ? /SSH master down/ : /unavailable.*retry/i);
			assert.match(await s.run("loop_mu", { args: ["wait", "--all"] }), /local mu/);
			assert.equal(calls().slice(beforeRetry.length), "exec\n", "loop_mu needs no remote startup");
			rmSync(down);
			const beforeRecovery = calls();
			const [text, bash] = await Promise.all([
				s.run("read", { path: "shared.txt" }), s.bang("pwd -P"),
			]);
			assert.match(text, /remote text/);
			assert.equal(bash, root);
			const recovery = calls().slice(beforeRecovery.length);
			assert.equal(recovery.match(/^exec$/gm)?.length, 1, "one root probe");
			assert.equal(recovery.match(/^serve:read$/gm)?.length, 2, "one AGENTS read, one tool read");
			assert.match(s.statuses.at(-1)!, /fixture:.*remote/);
			assert.equal((await s.prompt()).cwd, root);
			assert.match((await s.prompt()).sections.mule, /remote instructions/);
		}
	},
};

// Load through Pi so this exercises its real built-in definitions and their
// operation hooks. Only the extension's registrations are captured by a fake API.
export default function (pi: ExtensionAPI) {
	pi.on("session_start", async () => {
		try {
			put(cwd, "shared.txt", "local text\n");
			put(cwd, ".pi/loop.txt", "loop-only text\n");
			put(root, "shared.txt", "remote text\n");
			put(root, "AGENTS.md", "remote instructions\n");
			put(root, "tool.sh", "#!/bin/sh\necho old\n");
			put(root, "src/a.ts", "before\nA.B\naxb\n");
			put(root, "src/deep/b.ts", "A.B\n");
			put(root, "outer/src/other.ts", "other\n");
			put(root, "src/excluded.txt", "A.B excluded\n");
			put(root, ".git/config.ts", "A.B excluded\n");
			put(root, ".hidden.ts", "hidden\n");
			await checks[process.env.MULE_TOOL_TEST_CASE as keyof typeof checks]();
			writeSync(1, "mule-tool-check: ok\n");
			process.exit(0);
		} catch (error) {
			writeSync(2, `${inspect(error)}\n`);
			process.exit(1);
		}
	});
}
