import { createHash } from "node:crypto";
import { realpathSync } from "node:fs";
import { isAbsolute, relative, resolve, sep } from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { MasterRequired, type WorkspaceOps } from "./ops.ts";

function inside(root: string, path: string) {
	const rel = relative(root, path);
	return !isAbsolute(rel) && rel !== ".." && !rel.startsWith(`..${sep}`);
}

function realpath(path: string) {
	try { return realpathSync(path); } catch { return path; }
}

const unmark = (input: string) => input.startsWith("@") ? input.slice(1) : input;

// Placement of known loop resources does not need a reachable workspace.
export function loopResources(cwd: string, home: string, localRoots: string[]) {
	const roots = [...localRoots, resolve(home, ".pi"), resolve(home, ".agents"), resolve(home, ".ssh"), resolve(cwd, ".agents"), resolve(cwd, ".pi")];
	const canonicalRoots = roots.map(realpath);
	return {
		path(input: string) {
			const value = unmark(input);
			const local = value === "~" ? home : value.startsWith("~/") ? resolve(home, value.slice(2)) : resolve(cwd, value);
			if (roots.some((dir) => inside(dir, local))) return local;
			const canonical = realpath(local);
			return canonicalRoots.some((dir) => inside(dir, canonical)) ? local : undefined;
		},
		keepLocal(dir: string) {
			if (!isAbsolute(dir) || roots.includes(dir)) return;
			roots.push(dir);
			canonicalRoots.push(realpath(dir));
		},
	};
}

export function workspaceRouter(cwd: string, root: string, loop: ReturnType<typeof loopResources>, ops: Pick<WorkspaceOps, "read" | "write">) {
	function path(input: string): { side: "local" | "remote"; path: string } {
		const loopPath = loop.path(input);
		if (loopPath) return { side: "local", path: loopPath };
		const value = unmark(input);
		if (value.startsWith("~")) throw new Error("Use an absolute remote path; ~ is only for loop resources in file tools");
		if (!isAbsolute(value)) return { side: "remote", path: resolve(root, value) };
		// Native remote paths win even when the two workspaces overlap lexically.
		const absolute = resolve(value);
		return { side: "remote", path: inside(cwd, absolute) && !inside(root, absolute) ? resolve(root, relative(cwd, absolute)) : absolute };
	}

	function remotePath(input: string) {
		const target = path(input);
		if (target.side === "local") throw new Error("Not a workspace path: loop resource");
		return target.path;
	}

	return {
		path,
		remotePath,
		read(input: string, signal?: AbortSignal) {
			return ops.read(remotePath(input), signal);
		},
		async write(input: string, data: Buffer, signal?: AbortSignal) {
			await ops.write(remotePath(input), data, signal);
		},
	};
}

export function jobKey(session: string, invocation: string) {
	// Both identifiers survive a replay of the same tool call. Hashing keeps the
	// key valid for mule and avoids putting session paths or commands in it.
	return `pi-${createHash("sha256").update(JSON.stringify([session, invocation])).digest("hex")}`;
}

export async function runJob(ops: Pick<WorkspaceOps, "exec" | "poll" | "output" | "kill">, request: Parameters<WorkspaceOps["exec"]>[0], options: {
	signal?: AbortSignal; onData: (data: Buffer) => void;
}) {
	const { signal, onData } = options;
	if (signal?.aborted) throw new Error("aborted");
	// Even on abort, collect the dispatch answer so we can stop the exact job.
	const job = await ops.exec(request);
	try {
		for (;;) {
			signal?.throwIfAborted();
			const poll = await ops.poll(job);
			signal?.throwIfAborted();
			if (poll.state === "done") {
				const data = await ops.output(job);
				signal?.throwIfAborted();
				if (poll.logBytes > data.length) onData(Buffer.from("[Remote log clipped; showing its tail]\n"));
				onData(data);
				return { exitCode: poll.exitCode };
			}
			await delay(250, undefined, { signal });
		}
	} catch (error) {
		// Don't issue another mule call if a concurrent abort met a handback.
		if (signal?.aborted && !(error instanceof MasterRequired)) {
			await ops.kill(job);
			throw new Error("aborted", { cause: error });
		}
		throw error;
	}
}
