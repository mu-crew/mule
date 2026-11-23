import { spawnSync } from "node:child_process";
import { chmodSync, copyFileSync, existsSync, mkdirSync, mkdtempSync, readdirSync, readFileSync, realpathSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

// A standalone Pi, never a site wrapper (MULE_TEST_PI), else `pi` on PATH.
export const pi = process.env.MULE_TEST_PI ?? "pi";
export const havePi = () => spawnSync(pi, ["--version"], { timeout: 30_000 }).status === 0;

// Launch Pi only as a loader; no prompt, model request, real config or SSH
// unless a case brings its own mule. `setup` may return env overrides.
export function runPiCase(name: string, setup?: (dir: string) => Record<string, string>) {
	const dir = realpathSync(mkdtempSync(join(tmpdir(), "mule-tool-test-")));
	try {
		const bin = join(dir, "mule");
		copyFileSync(new URL("./fixture-mule.mjs", import.meta.url), bin);
		chmodSync(bin, 0o700);
		mkdirSync(join(dir, "home"));
		// A routing regression must not run an installed mu, even with an
		// isolated HOME. The fake ExtensionAPI records local argv separately.
		writeFileSync(join(dir, "mu"), "#!/bin/sh\necho unexpected shell mu\n", { mode: 0o700 });
		writeFileSync(join(dir, "calls"), "");
		const env = {
			PATH: `${dir}:${process.env.PATH}`, HOME: join(dir, "home"), PI_CODING_AGENT_DIR: join(dir, "agent"),
			XDG_CONFIG_HOME: join(dir, "config"), XDG_STATE_HOME: join(dir, "state"),
			LC_ALL: "C", PI_OFFLINE: "1", PI_SKIP_VERSION_CHECK: "1",
			MULE_PI_BIN: bin, MULE_TOOL_TEST_DIR: dir, MULE_TOOL_TEST_CASE: name, MULE_PI_MUZZLE: "off",
			...setup?.(dir),
		};
		return spawnSync(pi, ["--mode", "rpc", "--no-session", "--no-extensions", "--no-skills",
			"-e", fileURLToPath(new URL("./tool-checks.ts", import.meta.url))], {
			cwd: dir, encoding: "utf8", timeout: 60_000, killSignal: "SIGKILL", env,
		});
	} finally {
		// The fixture's jobs are detached; none may outlive the case.
		for (const job of existsSync(join(dir, "jobs")) ? readdirSync(join(dir, "jobs")) : []) {
			try { process.kill(-Number(readFileSync(join(dir, "jobs", job, "pid"), "utf8")), "SIGKILL"); } catch { /* finished */ }
		}
		rmSync(dir, { recursive: true, force: true, maxRetries: 3 });
	}
}
