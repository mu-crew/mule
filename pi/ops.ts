export const READ_PAGE_BYTES = 64 * 1024;
export const MAX_FILE_BYTES = 4 * 1024 * 1024;

// mule's exit 3: no SSH master. Only an operator can open it (a hardware
// key may need a tap), so this is a handback: stop and ask, never retry.
export class MasterRequired extends Error {}

// A finished job's log as `mule tail` returns it: the last 64 KiB, and
// whether the job outran max_log_bytes (mule then discarded later output).
export type JobOutput = { data: Buffer; cappedAt?: number };

export type FileKind = "file" | "directory" | "other" | "missing";
export type JobState = { state: "running" } | { state: "done"; exitCode: number; logBytes: number };

// Paths and cwd are absolute on the workspace host. A read returns the whole
// file (at most MAX_FILE_BYTES), or undefined only for an absent path.
export interface WorkspaceOps {
	read(path: string, signal?: AbortSignal): Promise<Buffer | undefined>;
	stat(path: string, signal?: AbortSignal): Promise<FileKind>;
	write(path: string, data: Buffer, signal?: AbortSignal): Promise<void>;
	exec(request: { command: string; cwd: string; key: string; timeout?: number }): Promise<string>;
	poll(job: string): Promise<JobState>;
	output(job: string): Promise<JobOutput>;
	kill(job: string): Promise<void>;
}
