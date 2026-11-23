import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import { posix } from "node:path";
import { createInterface } from "node:readline";
import { MuleExecBackend } from "./exec-backend.ts";
import { MasterRequired, MAX_FILE_BYTES, type FileKind, type WorkspaceOps } from "./ops.ts";

type Reply = {
	id: number; rc: number; stdout_b64: string; stderr: string;
	truncated: boolean; cancelled: boolean; error?: string; kind?: string;
};
type Waiter = { resolve(reply: Reply): void; reject(error: unknown): void };

// serve's own messages for a path that is not there (GNU and BSD head/stat).
// Anything else is an error, never "missing": an unreadable AGENTS.md must
// not read as an absent one. A localized host fails closed the same way.
const ABSENT = /No such file or directory|Not a directory/;
const STDERR_TAIL = 4096;

/**
 * File ops over one `mule serve --stdio` child: one ticket-locked round trip
 * per batch, against about four for an `exec`. serve itself coalesces what
 * arrives while a batch is in flight, so requests go out as they come.
 *
 * Commands stay detached jobs (human decision, 2026-11-20): exec, poll,
 * output and kill are the exec backend's, and so is what serve cannot do
 * whole -- a read past its 1 MB cap or its 1 s op deadline, a write that
 * needs its parent created or a symlink followed. serve keeps a replaced
 * file's mode itself, before its rename publishes the new bytes.
 */
export class MuleServeBackend implements WorkspaceOps {
	#child?: ChildProcessWithoutNullStreams;
	#waiters = new Map<number, Waiter>();
	#next = 1;
	readonly #bin: string;
	readonly #host: string;
	readonly #config?: string;
	readonly jobs: MuleExecBackend;

	constructor(options: { bin: string; host: string; config?: string }) {
		this.jobs = new MuleExecBackend(options);
		this.#bin = options.bin;
		this.#host = options.host;
		this.#config = options.config;
	}

	resolveRoot(cwd?: string) { return this.jobs.resolveRoot(cwd); }

	// The child is per caller and lives only as long as it is needed: a
	// dropped master ends it (exit 3) and the next op starts a fresh one.
	#serve() {
		if (this.#child) return this.#child;
		const child = spawn(this.#bin, ["--quiet", ...(this.#config ? ["--config", this.#config] : []), "serve", "--host", this.#host, "--stdio"],
			{ stdio: ["pipe", "pipe", "pipe"] });
		this.#child = child;
		let stderr = "";
		let gone: Waiter[] = [];
		child.stderr.setEncoding("utf8").on("data", (part: string) => { stderr = (stderr + part).slice(-STDERR_TAIL); });
		child.stdin.on("error", () => { /* the exit handler reports it */ });
		createInterface({ input: child.stdout }).on("line", (line) => {
			let reply: Reply;
			try { reply = JSON.parse(line); } catch { return; }
			const waiter = this.#waiters.get(reply.id);
			if (!waiter) return;
			this.#waiters.delete(reply.id);
			// The master command arrives on stderr just before exit 3.
			if (reply.kind === "master_gone") gone.push(waiter);
			else waiter.resolve(reply);
		});
		const finish = (code: number | null, error?: Error) => {
			if (this.#child !== child) return;
			this.#child = undefined;
			const detail = stderr.trim();
			const failure = code === 3
				? new MasterRequired(detail || "Mule needs an operator to open its SSH master")
				: error ?? new Error(`mule serve exited (${code ?? "signal"})${detail ? `: ${detail}` : ""}`);
			const waiters = [...gone, ...this.#waiters.values()];
			gone = [];
			this.#waiters.clear();
			for (const waiter of waiters) waiter.reject(failure);
		};
		child.on("error", (error) => finish(null, error));
		child.on("close", (code) => finish(code));
		return child;
	}

	#op(request: { op: "read" | "stat"; path: string } | { op: "write"; path: string; data_b64: string }, signal?: AbortSignal): Promise<Reply> {
		signal?.throwIfAborted();
		const child = this.#serve();
		const id = this.#next++;
		return new Promise((resolve, reject) => {
			// Cancel only this op: serve drops it if queued and kills it alone
			// if in flight. The caller does not wait for that answer.
			const abort = () => {
				if (!this.#waiters.delete(id)) return;
				child.stdin.write(`${JSON.stringify({ op: "cancel", id })}\n`);
				reject(signal!.reason);
			};
			const done = () => signal?.removeEventListener("abort", abort);
			this.#waiters.set(id, {
				resolve: (reply) => { done(); resolve(reply); },
				reject: (error) => { done(); reject(error); },
			});
			signal?.addEventListener("abort", abort, { once: true });
			child.stdin.write(`${JSON.stringify({ ...request, id })}\n`);
		});
	}

	async read(path: string, signal?: AbortSignal) {
		const reply = await this.#op({ op: "read", path }, signal);
		// serve's caps are its own (1 MB, 1 s); a job reads up to MAX_FILE_BYTES.
		if (reply.kind === "output_limit" || reply.kind === "timeout") return this.jobs.read(path, signal);
		if (!reply.kind && reply.rc === 0) return Buffer.from(reply.stdout_b64, "base64");
		if (!reply.kind && ABSENT.test(reply.stderr)) return undefined;
		throw new Error(`Cannot read remote file: ${path}${detail(reply)}`);
	}

	// No -L: a symlink is reported as one, so a write can tell it apart.
	async #lstat(path: string, signal?: AbortSignal): Promise<FileKind | "symlink"> {
		const reply = await this.#op({ op: "stat", path }, signal);
		if (!reply.kind && reply.rc !== 0 && ABSENT.test(reply.stderr)) return "missing";
		const mode = /^\d+ -?\d+ ([0-9a-f]+)\n?$/.exec(Buffer.from(reply.stdout_b64, "base64").toString());
		if (reply.kind || reply.rc !== 0 || !mode) throw new Error(`Cannot stat remote path: ${path}${detail(reply)}`);
		const type = parseInt(mode[1], 16) & 0o170000;
		return type === 0o100000 ? "file" : type === 0o040000 ? "directory" : type === 0o120000 ? "symlink" : "other";
	}

	async stat(path: string, signal?: AbortSignal): Promise<FileKind> {
		const kind = await this.#lstat(path, signal);
		return kind === "symlink" ? this.jobs.stat(path, signal) : kind;
	}

	async write(path: string, data: Buffer, signal?: AbortSignal) {
		if (data.length > MAX_FILE_BYTES) throw new Error(`Remote writes are limited to ${MAX_FILE_BYTES} bytes`);
		// serve replaces the file by rename and creates no directories. One
		// round trip tells which write this is.
		const [target, parent] = await Promise.all([this.#lstat(path, signal), this.#lstat(posix.dirname(path), signal)]);
		if (target === "directory") throw new Error(`Cannot write remote path: ${path} is a directory`);
		// A symlink keeps its usual write-through semantics, and a missing
		// parent is created: both are the exec backend's write.
		if (target === "symlink" || target === "other" || parent !== "directory") return this.jobs.write(path, data, signal);
		const reply = await this.#op({ op: "write", path, data_b64: data.toString("base64") }, signal);
		// serve sets the old mode on its temp file before the rename. A chmod
		// here, after it, left a 0600 file's new bytes at 0644 until it ran,
		// and for good when it failed or the call was aborted (review,
		// 2026-11-23).
		if (reply.kind || reply.rc !== 0) throw new Error(`Cannot write remote file: ${path}${detail(reply)}`);
	}

	exec(request: Parameters<WorkspaceOps["exec"]>[0]) { return this.jobs.exec(request); }
	poll(job: string) { return this.jobs.poll(job); }
	output(job: string) { return this.jobs.output(job); }
	kill(job: string) { return this.jobs.kill(job); }

	/** Hang up: serve exits when its stdin closes. */
	close() {
		this.#child?.stdin.end();
	}
}

function detail(reply: Reply) {
	const why = (reply.error ?? reply.stderr).trim().slice(0, 2000);
	return why ? ` (${reply.kind ?? `rc ${reply.rc}`}: ${why})` : "";
}
