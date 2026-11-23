import assert from "node:assert/strict";
import { mkdirSync, mkdtempSync, realpathSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { muleBinary } from "./binary.ts";
import { jobKey, loopResources, runJob, workspaceRouter } from "./routing.ts";
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
