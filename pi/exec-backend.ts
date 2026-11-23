import { execFile } from "node:child_process";
import { isAbsolute, posix } from "node:path";
import { MasterRequired, MAX_FILE_BYTES, READ_PAGE_BYTES, type FileKind, type JobOutput, type JobState, type WorkspaceOps } from "./ops.ts";

export const quote = (s: string) => `'${s.replaceAll("'", "'\\''")}'`;
// mule base64-wraps the script and caps the full dispatch at 112 KiB. Leave
// room for cwd, wrapper and prune; account for shell quoting BEFORE measuring.
const SCRIPT_BYTES = 72 * 1024;
const WRITE_CHUNK = 40 * 1024;
const BATCH_OUTPUT_BYTES = 512 * 1024;
const FILE_OP_SECONDS = 10;
const MISSING = 44;

type Reply = { code: number; data: Buffer };
type Member = {
	script: string; bytes: number; signal?: AbortSignal;
	resolve(reply: Reply): void; reject(error: unknown): void;
};

function scriptFor(batch: Member[]) {
	const members = batch.map(({ script }, i) => `(
( ${script}\n) >"$d/${i}.out" 2>/dev/null
printf '%s' "$?" >"$d/${i}.rc"
) &`).join("\n");
	const frames = batch.map((_, i) => `
rc=$(cat "$d/${i}.rc") && size=$(wc -c <"$d/${i}.out") || exit 1
printf '%s %s %s\\n' ${i} "$rc" "$size" && cat "$d/${i}.out" || exit 1`).join("\n");
	return `d=$(mktemp -d) || exit 1\ntrap 'rm -rf "$d"' EXIT\n${members}\nwait\n${frames}`;
}

function decode(data: Buffer, batch: Member[]): Reply[] {
	let at = 0;
	const replies = batch.map(({ bytes }, i) => {
		const end = data.indexOf(10, at);
		const frame = end >= at && /^(\d+) (\d+) +([0-9]+)$/.exec(data.toString("ascii", at, end));
		if (!frame || Number(frame[1]) !== i) throw new Error("Invalid file-op frame");
		const code = Number(frame[2]);
		const size = Number(frame[3]);
		at = end + 1;
		if (code > 255 || size > bytes || at + size > data.length) throw new Error("Incomplete file-op frame");
		const body = data.subarray(at, at + size);
		at += size;
		return { code, data: body };
	});
	if (at !== data.length) throw new Error("Unexpected file-op output after last frame");
	return replies;
}

export class MuleExecBackend implements WorkspaceOps {
	#pending: Member[] = [];
	#timer?: ReturnType<typeof setTimeout>;
	readonly #bin: string;
	readonly #host: string;
	readonly #config?: string;

	constructor({ bin, host, config }: { bin: string; host: string; config?: string }) {
		if (!isAbsolute(bin)) throw new Error("Use an absolute mule binary path");
		this.#bin = bin;
		this.#host = host;
		this.#config = config;
	}

	#call(verb: string, args: string[], budget = 128 * 1024): Promise<Buffer> {
		return this.#run(verb, args, budget).then(({ stdout }) => stdout);
	}

	#run(verb: string, args: string[], budget = 128 * 1024): Promise<{ stdout: Buffer; stderr: string }> {
		return new Promise((resolve, reject) => {
			execFile(this.#bin, ["--quiet", ...(this.#config ? ["--config", this.#config] : []), verb, "--host", this.#host, ...args],
				{ encoding: "buffer", maxBuffer: budget }, (error, stdout, stderr) => {
					// With --strict-codes (exec) or a detached verb, 3 is only ever
					// mule's own: a job's 3 arrives as 8, or as `rc` in poll's JSON.
					if (!error) resolve({ stdout, stderr: stderr.toString() });
					else if (error.code === 3) reject(new MasterRequired(stderr.toString().trim() || "Mule needs an operator to open its SSH master"));
					else if (verb === "exec" && error.code === 7) reject(new Error("Mule file-op output incomplete (exit 7)"));
					else {
						const detail = stderr.toString().trim().slice(0, 2000);
						reject(new Error(`mule ${verb} failed (${error.code ?? error.signal ?? "no answer"})${detail ? `: ${detail}` : ""}`));
					}
				});
		});
	}

	#fixed(script: string, cwd: string | null = "/") {
		// exec's 7 means mule cut the output. Keep it that: a script's own 7
		// leaves as 1, so a partial answer can never pass for a script error
		// and no script error can pass for a partial answer.
		const guarded = `(\n${script}\n)\nrc=$?; [ "$rc" -eq 7 ] && rc=1; exit "$rc"`;
		if (Buffer.byteLength(guarded) + Buffer.byteLength(cwd ?? "") > SCRIPT_BYTES) throw new Error("File operation exceeds dispatch budget");
		return this.#call("exec", ["--strict-codes", "--max-secs", String(FILE_OP_SECONDS), ...(cwd ? ["--cwd", cwd] : []), "--", guarded], BATCH_OUTPUT_BYTES + 4096);
	}

	/** A short fixed script on the host, its output discarded. */
	async command(script: string) { await this.#fixed(script); }

	async resolveRoot(cwd?: string) {
		return (await this.probe(cwd, "off")).root;
	}

	/**
	 * The workspace root and, in the same round trip, the host's muzzle: an
	 * absolute path the jobs will run, or undefined. `muzzle` is "off", an
	 * absolute path to check, or undefined to look it up on the jobs' PATH.
	 */
	async probe(cwd?: string, muzzle?: string) {
		if (muzzle !== undefined && muzzle !== "off" && !muzzle.startsWith("/")) throw new Error("MULE_PI_MUZZLE must be off or an absolute path");
		const find = muzzle === "off" ? "" : muzzle === undefined
			? `\nm=$(command -v muzzle 2>/dev/null) && [ -x "$m" ] && printf '%s\\n' "$m"; :`
			: `\n[ -x ${quote(muzzle)} ] && printf '%s\\n' ${quote(muzzle)}; :`;
		const [root, found] = (await this.#fixed(`pwd -P${find}`, cwd ?? null)).toString().replace(/\n$/, "").split("\n");
		const valid = (path?: string) => path !== undefined && path.startsWith("/") && !/[\x00-\x1f\x7f]/.test(path);
		if (!valid(root)) throw new Error("Invalid remote workspace root");
		return { root, muzzle: valid(found) ? found : undefined };
	}

	#submit(script: string, bytes: number, signal?: AbortSignal): Promise<Reply> {
		signal?.throwIfAborted();
		return new Promise((resolve, reject) => {
			const finish = () => signal?.removeEventListener("abort", abort);
			const abort = () => { finish(); reject(signal?.reason); };
			this.#pending.push({ script, bytes, signal,
				resolve: (reply) => { finish(); resolve(reply); },
				reject: (error) => { finish(); reject(error); },
			});
			signal?.addEventListener("abort", abort, { once: true });
			this.#timer ??= setTimeout(() => this.#flush(), 5);
		});
	}

	#flush() {
		this.#timer = undefined;
		const pending = this.#pending;
		this.#pending = [];
		let batch: Member[] = [];
		let bytes = 0;
		for (const op of pending) {
			if (op.signal?.aborted) continue;
			if (batch.length && (batch.length === 20 || bytes + op.bytes > BATCH_OUTPUT_BYTES || Buffer.byteLength(scriptFor([...batch, op])) > SCRIPT_BYTES)) {
				void this.#send(batch);
				batch = [];
				bytes = 0;
			}
			batch.push(op);
			bytes += op.bytes;
		}
		if (batch.length) void this.#send(batch);
	}

	async #send(batch: Member[]) {
		// Only bounded read-only operations get here. A caller may discard its
		// own reply without stopping peers; no cancelled member can mutate files.
		// Unknown-duration jobs and writes never wait behind the batch barrier.
		try {
			const replies = decode(await this.#fixed(scriptFor(batch)), batch);
			batch.forEach((member, i) => member.resolve(replies[i]));
		} catch (error) {
			for (const member of batch) member.reject(error);
		}
	}

	async read(path: string, signal?: AbortSignal) {
		const pages: Buffer[] = [];
		for (let page = 0; page <= MAX_FILE_BYTES / READ_PAGE_BYTES; page++) {
			signal?.throwIfAborted();
			const data = await this.#page(path, page, signal);
			if (!data && page === 0) return undefined;
			if (!data || data.length > READ_PAGE_BYTES) throw new Error(`Incomplete remote read: ${path}`);
			if (page * READ_PAGE_BYTES + data.length > MAX_FILE_BYTES) throw new Error(`Remote file exceeds ${MAX_FILE_BYTES} bytes: ${path}`);
			pages.push(data);
			if (data.length < READ_PAGE_BYTES) return Buffer.concat(pages);
		}
		throw new Error(`Incomplete remote read: ${path}`);
	}

	async #page(path: string, page: number, signal?: AbortSignal) {
		if (!Number.isSafeInteger(page) || page < 0 || page > MAX_FILE_BYTES / READ_PAGE_BYTES) throw new Error("Invalid file page");
		const p = quote(path);
		const { code, data } = await this.#submit(`
if [ ! -e ${p} ] && [ ! -L ${p} ]; then exit ${MISSING}; fi
[ -f ${p} ] || exit 1
dd if=${p} bs=${READ_PAGE_BYTES} skip=${page} count=1`, READ_PAGE_BYTES, signal);
		if (code === MISSING) return undefined;
		if (code !== 0) throw new Error(`Cannot read remote file: ${path}`);
		return data;
	}

	async stat(path: string, signal?: AbortSignal): Promise<FileKind> {
		const p = quote(path);
		const { code, data } = await this.#submit(`
if [ -f ${p} ]; then printf file
elif [ -d ${p} ]; then printf directory
elif [ -e ${p} ] || [ -L ${p} ]; then printf other
else printf missing; fi`, 9, signal);
		const kind = data.toString();
		if (code !== 0 || !["file", "directory", "other", "missing"].includes(kind)) throw new Error(`Cannot stat remote path: ${path}`);
		return kind as FileKind;
	}

	async write(path: string, data: Buffer, signal?: AbortSignal) {
		if (data.length > MAX_FILE_BYTES) throw new Error(`Remote writes are limited to ${MAX_FILE_BYTES} bytes`);
		const p = quote(path);
		const parent = quote(posix.dirname(path));
		// Refuse FIFOs/devices/directories; ordinary symlinks to files keep their
		// usual write semantics. The same guard runs before the final copy.
		const guard = `[ -f ${p} ] || { [ ! -e ${p} ] && [ ! -L ${p} ]; }`;
		const decodeChunk = (chunk: Buffer) => `printf %s '${chunk.toString("base64")}' | base64 -d`;
		signal?.throwIfAborted();
		if (data.length <= WRITE_CHUNK) {
			await this.#fixed(`mkdir -p ${parent} && { ${guard}; } && ${decodeChunk(data)} > ${p}`);
			return;
		}
		const temp = (await this.#fixed(`mkdir -p ${parent} && { ${guard}; } && mktemp ${parent}/.mule-write.XXXXXXXXXX`)).toString().replace(/\n$/, "");
		if (posix.dirname(temp) !== posix.dirname(path) || !/^\.mule-write\.[A-Za-z0-9]+$/.test(posix.basename(temp))) throw new Error("Invalid remote staging path");
		const staging = quote(temp);
		try {
			for (let i = 0; i < data.length; i += WRITE_CHUNK) {
				signal?.throwIfAborted();
				await this.#fixed(`${decodeChunk(data.subarray(i, i + WRITE_CHUNK))} >> ${staging}`);
			}
			signal?.throwIfAborted();
			// Don't expose unassembled chunks at the destination, and don't
			// replace its inode (edit must preserve mode and follow symlinks).
			await this.#fixed(`trap 'rm -f -- "$staging"' EXIT\nstaging=${staging}\n{ ${guard}; } && cat "$staging" > ${p}`);
		} catch (error) {
			if (!(error instanceof MasterRequired)) {
				try { await this.#fixed(`rm -f -- ${staging}`); }
				catch (cleanup) {
					if (cleanup instanceof MasterRequired) throw cleanup;
					throw new Error(`${String(error)}; could not remove staging file ${temp}`, { cause: cleanup });
				}
			}
			throw error;
		}
	}

	async exec({ command, cwd, key, timeout }: Parameters<WorkspaceOps["exec"]>[0]) {
		// mule's worker wrapper runs POSIX sh. Bash tools expect bash syntax.
		const id = (await this.#call("run", ["--human", "--cwd", cwd, "--key", key,
			...(timeout && timeout > 0 ? ["--max-secs", String(Math.ceil(timeout))] : []), "--", `exec bash -c ${quote(command)}`])).toString().trim();
		if (!/^[0-9a-f]{6}$/.test(id)) throw new Error("Mule did not return a job id");
		return id;
	}

	async poll(job: string): Promise<JobState> {
		const status = JSON.parse((await this.#call("poll", [job, "--json"])).toString());
		if (status.state === "running") return { state: "running" };
		if (status.state === "done" && Number.isInteger(status.rc) && Number.isSafeInteger(status.log_size) && status.log_size >= 0 && typeof status.drained === "boolean") {
			return { state: "done", exitCode: status.rc, logBytes: status.log_size, drained: status.drained };
		}
		throw new Error(`Mule cannot collect job ${job} (${String(status.state)})`);
	}

	async output(job: string): Promise<JobOutput> {
		// tail reads the last 64 KiB and says on stderr when the job outran
		// max_log_bytes. The note is mule's, not the job's: the log is stdout.
		const { stdout, stderr } = await this.#run("tail", [job]);
		const capped = /log was capped at (\d+) bytes/.exec(stderr);
		return { data: stdout, ...(capped ? { cappedAt: Number(capped[1]) } : {}) };
	}
	async kill(job: string) { await this.#call("kill", [job]); }

	async lookup(key: string) {
		// mule's exact key lookup: the id as JSON, or null. It ignores ls's
		// 24-hour window, so an old finished job is still found.
		const id: unknown = JSON.parse((await this.#call("lookup", [key])).toString());
		if (id === null) return undefined;
		if (typeof id !== "string" || !/^[0-9a-f]{6}$/.test(id)) throw new Error("Mule did not return a job id");
		return id;
	}
}
