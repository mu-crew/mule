import assert from "node:assert/strict";
import { chmodSync, existsSync, lstatSync, mkdirSync, readFileSync, readlinkSync, rmSync, statSync, symlinkSync, writeFileSync, writeSync } from "node:fs";
import { join } from "node:path";
import { inspect } from "node:util";
import type { ExtensionAPI, ExtensionContext, ToolDefinition } from "@earendil-works/pi-coding-agent";
import { createFindToolDefinition } from "@earendil-works/pi-coding-agent";
import mule from "../mule.ts";

const builtins = ["read", "write", "edit", "bash", "grep", "find", "ls", "powershell"];
const dir = process.env.MULE_TOOL_TEST_DIR!;
const cwd = join(dir, "local");
const root = join(dir, "remote [root]");
const calls = () => readFileSync(join(dir, "calls"), "utf8");

function register(flag?: string, settings: Record<string, unknown> = {}) {
	const tools = new Map<string, ToolDefinition>();
	const handlers = new Map<string, Function>();
	const statuses: string[] = [];
	const notices: string[] = [];
	const localMu: unknown[] = [];
	let active = [...builtins];
	let id = 0;
	const ctx = {
		cwd, ui: { setStatus: (_name: string, status: string) => statuses.push(status), notify: (message: string) => notices.push(message) },
		sessionManager: { getSessionId: () => "fixture-session" },
	} as unknown as ExtensionContext;
	mule({
		registerFlag() {}, getFlag: () => flag, getSettings: () => settings,
		on: (name: string, handler: Function) => handlers.set(name, handler),
		registerTool: (tool: ToolDefinition) => tools.set(tool.name, tool),
		getActiveTools: () => active,
		setActiveTools: (names: string[]) => { active = names; },
		exec: async (program: string, args: string[], options: { cwd: string }) => {
			localMu.push([program, args, options.cwd]);
			return { stdout: "local mu", stderr: "", code: 0 };
		},
	} as unknown as ExtensionAPI);
	return {
		tools, active: () => active, statuses, notices, localMu,
		start: () => handlers.get("session_start")!({}, ctx),
		stop: () => handlers.get("session_shutdown")!({}, ctx),
		async prompt() {
			const options = { cwd, sections: {} as Record<string, string>, contextFiles: [{ path: join(cwd, "AGENTS.md") }] };
			await handlers.get("before_agent_start")!({ systemPromptOptions: options }, ctx);
			return options;
		},
		async run(name: string, params: Record<string, unknown>) {
			const result = await tools.get(name)!.execute(`call-${++id}`, params, undefined, undefined, ctx);
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

const enabled = () => register(`fixture:${root}`);
function put(base: string, name: string, content: string) {
	const path = join(base, name);
	mkdirSync(join(path, ".."), { recursive: true });
	writeFileSync(path, content);
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
			assert.match(s.statuses.at(-1)!, /unavailable.*retry/i);
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
