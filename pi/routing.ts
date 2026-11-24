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

const KEY = /^[A-Za-z0-9._-]{1,128}$/;

/**
 * mule's idempotency key for one tool call. Both identifiers survive a
 * replay of the same call, so a restarted Pi that runs it again gets the
 * job already running (or its result), never a second run. Hashing keeps
 * the key valid for mule and keeps session paths and commands out of it.
 *
 * `prefix` is MULE_PI_JOB_PREFIX, prepended verbatim: mu sets it per worker
 * so closing the worker can find its outstanding jobs with
 * `mule ls --key-prefix` across Pi sessions.
 */
export function jobKey(session: string, invocation: string, prefix = "") {
	return checkedKey(prefix, `pi-${createHash("sha256").update(JSON.stringify([session, invocation])).digest("hex")}`);
}

export function checkedKey(prefix: string, key: string) {
	if (!KEY.test(prefix + key)) throw new Error(`MULE_PI_JOB_PREFIX makes an invalid mule key: use at most ${128 - key.length} of A-Z a-z 0-9 . _ -`);
	return prefix + key;
}

// How long a done job may take to drain its log before the answer is given
// without that proof. exec waits the same 5 s (SESSION_END_WAIT): a drain is
// milliseconds; a process the command left holding stdout can take forever.
export const DRAIN_WAIT_MS = 5_000;
const POLL_MS = 250;
const DRAIN_POLL_MS = 100;

type JobOps = Pick<WorkspaceOps, "poll" | "output">;

/**
 * Poll one job to its end and hand its tail to `onData`. A job is over when
 * `rc` exists and its log has drained: `rc` lands first, and a tail read
 * then can miss the last bytes and mule's cap marker, so a capped log would
 * pass for whole and a command's own 7 for mule's cap.
 * A log still undrained after DRAIN_WAIT_MS is reported as unfinished.
 */
export async function watchJob(ops: JobOps, job: string, options: { signal?: AbortSignal; onData: (data: Buffer) => void; drainWait?: number }) {
	const { signal, onData, drainWait = DRAIN_WAIT_MS } = options;
	let settling: number | undefined;
	for (;;) {
		signal?.throwIfAborted();
		const poll = await ops.poll(job);
		signal?.throwIfAborted();
		if (poll.state === "done") {
			settling ??= performance.now();
			if (!poll.drained && performance.now() - settling < drainWait) {
				await delay(DRAIN_POLL_MS, undefined, { signal });
				continue;
			}
			const { data, cappedAt } = await ops.output(job);
			signal?.throwIfAborted();
			// mule's caps, reported as text beside the command's own rc: a job
			// that exits 7 exited 7, and a capped log is not an exit code.
			// Notes go last: Pi keeps the tail of a long result.
			onData(data);
			const notes: string[] = [];
			if (cappedAt !== undefined) notes.push(`[mule: the log reached max_log_bytes (${cappedAt} bytes); later output was discarded]`);
			if (poll.logBytes > data.length) notes.push(`[mule: showing the last ${data.length} of ${poll.logBytes} log bytes]`);
			// Undrained: a process the command started still holds its output,
			// or a kill cut the log pipeline before it finished. mule cannot
			// tell these apart, and neither is a whole log.
			if (!poll.drained) notes.push(`[mule: the command exited ${poll.exitCode}, but its log was not finalized (a process it started still holds its output, or the job was killed); the log may be incomplete]`);
			if (notes.length) onData(Buffer.from(`${data.length && data[data.length - 1] !== 10 ? "\n" : ""}${notes.join("\n")}\n`));
			return { exitCode: poll.exitCode };
		}
		await delay(POLL_MS, undefined, { signal });
	}
}

/**
 * One detached job, start to collected tail. Abort stops exactly this job
 * with `mule kill` (the dispatch answer is awaited first so the id is
 * known), unless `handedBack()` says the master is gone: then the job keeps
 * running on the host, a kill could not reach it anyway, and its key lets
 * the next call (or mu's close) find it. A handback is never retried here.
 */
export async function runJob(ops: Pick<WorkspaceOps, "exec" | "poll" | "output" | "kill">, request: Parameters<WorkspaceOps["exec"]>[0], options: {
	signal?: AbortSignal; onData: (data: Buffer) => void; handedBack?: () => boolean;
}) {
	const { signal, onData, handedBack } = options;
	if (signal?.aborted) throw new Error("aborted");
	// Even on abort, collect the dispatch answer so we can stop the exact job.
	const job = await ops.exec(request);
	try {
		return await watchJob(ops, job, { signal, onData });
	} catch (error) {
		// Don't issue another mule call if a concurrent abort met a handback.
		if (signal?.aborted && !(error instanceof MasterRequired) && handedBack?.()) {
			throw new MasterRequired(`mule's SSH master is gone; job ${job} was left running on the host`, { cause: error });
		}
		if (signal?.aborted && !(error instanceof MasterRequired)) {
			await ops.kill(job);
			throw new Error("aborted", { cause: error });
		}
		throw error;
	}
}
