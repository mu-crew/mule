import { randomUUID } from "node:crypto";
import { homedir } from "node:os";
import { dirname, join } from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { fileURLToPath } from "node:url";
import { Type } from "@earendil-works/pi-ai";
import {
	createBashToolDefinition, createEditToolDefinition, createFindToolDefinition, createGrepToolDefinition,
	createLsToolDefinition, createPowerShellToolDefinition, createReadToolDefinition, createWriteToolDefinition,
	getAgentDir, type BashOperations, type ExtensionAPI, type ExtensionContext,
} from "@earendil-works/pi-coding-agent";
import { muleBinary } from "./binary.ts";
import { quote } from "./exec-backend.ts";
import { MuleServeBackend } from "./serve-backend.ts";
import { checkedKey, jobKey, loopResources, runJob, workspaceRouter } from "./routing.ts";
import { MasterRequired } from "./ops.ts";
import { bashJob, readOnlyRefusal } from "./policy.ts";

const extensionDir = dirname(fileURLToPath(import.meta.url));
const AGENTS_BYTES = 64 * 1024;
const STARTUP_BACKOFF_MS = 250;
const MAX_STARTUP_BACKOFF_MS = 2_000;

function fdPattern(raw: string, path: string) {
	// Same order as pi's local find: strip leading ./ before deciding whether
	// the pattern is a path, so `./*.ts` stays a recursive basename glob.
	const pattern = raw.replace(/^(?:\.\/)+/, "");
	if (!pattern.includes("/")) return "-- " + quote(pattern);
	// fd matches absolute paths. Anchor the glob to the search directory
	// without letting its characters change the glob or fd's smart case.
	const base = path.replace(/\/$/, "").replace(/[\\*?\[\]{}]/g, "\\$&");
	const full = pattern.startsWith("/") ? pattern : base + "/" + pattern;
	return "--full-path " + (/\p{Lu}/u.test(pattern) ? "--case-sensitive" : "--ignore-case") + " -- " + quote(full);
}

// rg -m counts per file. Count matches separately from context, recognizing
// the numbered records after NUL-delimited paths (which can contain colons).
// Drain rg so pipefail reports search errors, not a broken pipe.
function capGrep(limit: number, context: number) {
	return ` | {
count=0 last_file= last_line=0
while IFS= read -r -d '' file && IFS= read -r line; do
  sep=-
  if [[ $line =~ ^([0-9]+): ]]; then
    (( count >= ${limit} )) && continue
    (( count += 1 ))
    last_file=$file last_line=\${BASH_REMATCH[1]}
    sep=:
  elif (( count >= ${limit} )); then
    [[ $file == "$last_file" && $line =~ ^([0-9]+)- ]] || continue
    (( BASH_REMATCH[1] <= last_line + ${context} )) || continue
  fi
  printf '%s%s%s\\n' "$file" "$sep" "$line"
done
}`;
}

function imageMime(data: Buffer) {
	if (data.subarray(0, 8).equals(Buffer.from("89504e470d0a1a0a", "hex"))) return "image/png";
	if (data.subarray(0, 3).equals(Buffer.from("ffd8ff", "hex"))) return "image/jpeg";
	if (/^GIF8[79]a/.test(data.toString("ascii", 0, 6))) return "image/gif";
	if (data.toString("ascii", 0, 4) === "RIFF" && data.toString("ascii", 8, 12) === "WEBP") return "image/webp";
	return null;
}

export default function (pi: ExtensionAPI) {
	pi.registerFlag("mule", { type: "string", description: "Remote workspace via mule: host[:/root]" });
	pi.registerFlag("mule-role", { type: "string", description: "full-access (default) or read-only: no write, edit or mutating bash" });
	let enabled = false;
	let bash: ((key: string, agent: boolean) => BashOperations) | undefined;
	let handedBack = false;
	let role: string | undefined;
	let userBash: ((context: ExtensionContext) => BashOperations) | undefined;
	let prompt: { host: string; root: string; agents?: string } | undefined;
	let startupError: unknown;
	let loop: ReturnType<typeof loopResources> | undefined;
	let serve: MuleServeBackend | undefined;

	pi.on("session_start", async (_event, ctx) => {
		// Off unless asked for: the flag, else a "mule" key in settings
		// (project .pi/settings.json, which Pi reads only for a trusted project).
		const flag = pi.getFlag("mule");
		const option = typeof flag === "string" && flag ? flag : (pi.getSettings?.() as { mule?: unknown } | undefined)?.mule;
		if (typeof option !== "string" || !option) return;
		enabled = true;
		const cwd = ctx.cwd;
		const separator = option.indexOf(":");
		const host = separator === -1 ? option : option.slice(0, separator);
		const requestedRoot = separator === -1 ? undefined : option.slice(separator + 1);
		const resources = loop = loopResources(cwd, homedir(), [getAgentDir(), extensionDir]);
		// mu records a spawn's role but does not pass it to the pane, so the
		// spawn command carries it: --mule-role, else MULE_PI_ROLE, else the
		// "muleRole" setting. An unknown value fails closed, as read-only.
		const flagRole = pi.getFlag("mule-role");
		const askedRole = (typeof flagRole === "string" && flagRole) || process.env.MULE_PI_ROLE
			|| (pi.getSettings?.() as { muleRole?: unknown } | undefined)?.muleRole;
		const readOnly = askedRole !== undefined && askedRole !== "" && askedRole !== "full-access";
		if (readOnly && askedRole !== "read-only") ctx.ui.notify(`Unknown mule role ${String(askedRole)}: running read-only`, "error");
		const prefix = process.env.MULE_PI_JOB_PREFIX ?? "";
		let muzzle: string | undefined;
		let masterDown = false;
		// Set when a tool met mule's exit 3 in this turn: the turn is stopped,
		// and the jobs its abort reaches are left running rather than killed
		// through a master that is not there. A new prompt clears it.
		handedBack = false;
		const handBack = (error: MasterRequired, context: { ui: typeof ctx.ui; isIdle?: () => boolean; abort?: () => void }) => {
			const first = !handedBack;
			handedBack = masterDown = true;
			context.ui.setStatus("mule", `mule ${host}: SSH master down; open it, then continue`);
			if (first) {
				context.ui.notify(`mule needs its SSH master for ${host}. Open it, then send a message to continue.\n${error.message}`, "error");
				// Stop the turn: a model that retries a handback only spins.
				if (context.isIdle?.() === false) context.abort?.();
			}
			return new Error(`HANDBACK: mule's SSH master for ${host} is not open. Stop and ask the user to open it; do not retry workspace tools until they say it is back. The next tool call after that reconnects.\n${error.message}`, { cause: error });
		};
		// Every workspace tool goes through this, so a handback is handled the
		// same everywhere and never reaches the model as an ordinary failure.
		const guarded = <A extends unknown[], R>(run: (...args: A) => Promise<R>, contextOf: (args: A) => Parameters<typeof handBack>[1] | undefined) =>
			async (...args: A) => {
				try {
					const result = await run(...args);
					if (masterDown && prompt) {
						masterDown = false;
						(contextOf(args) ?? ctx).ui.setStatus("mule", `mule ${host}:${prompt.root}`);
					}
					return result;
				} catch (error) {
					if (error instanceof MasterRequired) throw handBack(error, contextOf(args) ?? ctx);
					throw error;
				}
			};
		const tool = <T extends { execute: (...args: any[]) => Promise<any> }>(definition: T): T =>
			({ ...definition, execute: guarded(definition.execute, (args) => args[4]) });
		const initialize = async () => {
			if (process.env.MU_ORCHESTRATOR) throw new Error("An orchestrator cannot use a remote workspace loop");
			if (!/^[A-Za-z0-9_][A-Za-z0-9_.-]*$/.test(host) || (requestedRoot !== undefined && !requestedRoot.startsWith("/"))) {
				throw new Error("Expected --mule host[:/absolute/remote/root]");
			}
			serve?.close();
			const ops = serve = new MuleServeBackend({ host, bin: muleBinary(extensionDir, process.env.MULE_PI_BIN) });
			const { root, muzzle: found } = await ops.probe(requestedRoot, process.env.MULE_PI_MUZZLE);
			muzzle = found;
			const router = workspaceRouter(cwd, root, resources, ops);
			const agents = await ops.read(join(root, "AGENTS.md"));
			if (agents && agents.length >= AGENTS_BYTES) throw new Error("Remote AGENTS.md must be under 64 KiB");
			return { ops, root, router, agents: agents?.toString("utf8") };
		};
		let pending: ReturnType<typeof initialize> | undefined;
		let retryAt = 0;
		let backoff = STARTUP_BACKOFF_MS;
		const ready = () => pending ??= (async () => {
			const wait = retryAt - performance.now();
			if (wait > 0) {
				ctx.ui.setStatus("mule", `mule ${host}: retrying workspace startup`);
				await delay(wait);
			}
			try {
				const workspace = await initialize();
				prompt = { host, root: workspace.root, agents: workspace.agents };
				startupError = undefined;
				ctx.ui.setStatus("mule", `mule ${host}:${workspace.root}`);
				return workspace;
			} catch (error) {
				// Share the attempt (including its backoff) among concurrent calls,
				// but don't replay it until a new tool call actually needs it.
				pending = undefined;
				startupError = error;
				retryAt = performance.now() + backoff;
				backoff = Math.min(backoff * 2, MAX_STARTUP_BACKOFF_MS);
				const reason = error instanceof MasterRequired ? "SSH master unavailable" : "workspace unavailable";
				ctx.ui.setStatus("mule", `mule ${host}: ${reason} (retry on next workspace call)`);
				ctx.ui.notify(String(error), "error");
				throw error;
			}
		})();

		// Install the overrides even if startup fails; a dropped or missing
		// master must never make a workspace operation hit the local checkout.
		const active = pi.getActiveTools();
		const local = {
			read: createReadToolDefinition(cwd), edit: createEditToolDefinition(cwd), write: createWriteToolDefinition(cwd),
			bash: createBashToolDefinition(cwd), grep: createGrepToolDefinition(cwd),
			find: createFindToolDefinition(cwd), ls: createLsToolDefinition(cwd), powershell: createPowerShellToolDefinition(cwd),
		};
		const sessionKey = (id: string) => jobKey(ctx.sessionManager.getSessionId(), id, prefix);
		const isHandedBack = () => handedBack;
		const number = (value: number | undefined, fallback: number) => Number.isFinite(value) ? Math.max(1, Math.floor(value!)) : fallback;
		const remoteSearch = async (id: string, command: string, signal?: AbortSignal, noMatches = false) => {
			const { ops, root } = await ready();
			const parts: Buffer[] = [];
			const { exitCode } = await runJob(ops, { command, cwd: root, key: sessionKey(id), timeout: 30 }, { signal, onData: (data) => parts.push(data), handedBack: isHandedBack });
			const output = Buffer.concat(parts).toString().trimEnd();
			if (exitCode && !(noMatches && exitCode === 1)) throw new Error(output || `Remote search exited ${exitCode}`);
			return { content: [{ type: "text" as const, text: output || "No results" }], details: undefined };
		};

		pi.registerTool(tool({ ...local.read, async execute(id, params, signal, update, context) {
			const loopPath = resources.path(params.path);
			if (loopPath) return local.read.execute(id, { ...params, path: loopPath }, signal, update, context);
			const { router } = await ready();
			// Pi may call access, mime detection and readFile; share one transfer
			// within this invocation, never a cross-invocation snapshot.
			let pending: Promise<Buffer> | undefined;
			const readFile = () => pending ??= router.read(params.path, signal).then((bytes) => {
				if (!bytes) throw new Error(`Remote file not found: ${params.path}`);
				return bytes;
			});
			return createReadToolDefinition(cwd, { operations: {
				readFile, access: async () => { await readFile(); }, detectImageMimeType: async () => imageMime(await readFile()),
			} }).execute(id, params, signal, update, context);
		} }));
		const refuseWrite = () => {
			throw new Error("Read-only role: write and edit are disabled in this session");
		};
		pi.registerTool(tool({ ...local.write, async execute(id, params, signal, update, context) {
			if (readOnly) refuseWrite();
			const { router } = await ready();
			return createWriteToolDefinition(cwd, { operations: {
				mkdir: async () => {}, // The provider creates parents with the write.
				writeFile: (_path, data) => router.write(params.path, Buffer.from(data), signal),
			} }).execute(id, params, signal, update, context);
		} }));
		pi.registerTool(tool({ ...local.edit, async execute(id, params, signal, update, context) {
			if (readOnly) refuseWrite();
			const { router } = await ready();
			let pending: Promise<Buffer> | undefined;
			const readFile = () => pending ??= router.read(params.path, signal).then((bytes) => {
				if (!bytes) throw new Error(`Remote file not found: ${params.path}`);
				return bytes;
			});
			return createEditToolDefinition(cwd, { operations: {
				access: async () => { await readFile(); }, readFile,
				writeFile: (_path, data) => router.write(params.path, Buffer.from(data), signal),
			} }).execute(id, params, signal, update, context);
		} }));
		bash = (key, agent) => ({ async exec(command, _cwd, { signal, onData, timeout }) {
			// Reserve direct mu calls for the loop. Never guess which parts of
			// a shell program to evaluate locally.
			if (/^\s*(?:mu|'mu'|"mu")(?=[\s;&|<>]|$)/.test(command)) {
				throw new Error("Use loop_mu to run mu on Pi's local loop");
			}
			// The role binds the agent's bash; `!` is the user's own command.
			const refusal = agent && readOnly ? readOnlyRefusal(command) : undefined;
			if (refusal) throw new Error(`Read-only role: refused a bash command that may modify the workspace (${refusal}). Use read, grep, find, ls, or plain readers such as cat, rg, git diff and git log.`);
			const { ops, root } = await ready();
			const job = bashJob(command, { muzzle, timeout });
			return runJob(ops, { command: job.command, cwd: root, key, timeout: job.timeout }, { signal, onData, handedBack: isHandedBack });
		} });
		userBash = (context) => {
			const operations = bash!(checkedKey(prefix, `pi-user-${randomUUID()}`), false);
			return { exec: guarded(operations.exec, () => context) };
		};
		pi.registerTool(tool({ ...local.bash, async execute(id, params, signal, update, context) {
			return createBashToolDefinition(cwd, { operations: bash!(sessionKey(id), true), exposeSessionEnvironment: false })
				.execute(id, params, signal, update, context);
		} }));
		pi.registerTool(tool({ ...local.grep, async execute(id, params, signal, update, context) {
			const path = params.path ?? ".";
			const loopPath = resources.path(path);
			if (loopPath) return local.grep.execute(id, { ...params, path: loopPath }, signal, update, context);
			const target = (await ready()).router.remotePath(path);
			const limit = number(params.limit, 100);
			const lines = params.context ? number(params.context, 1) : 0;
			return remoteSearch(id, `set -o pipefail; rg --no-config --color=never --line-number --with-filename --null --no-context-separator --hidden --no-ignore -m ${limit}${params.literal ? " -F" : ""}${params.ignoreCase ? " -i" : ""}${lines ? ` -C ${lines}` : ""}${params.glob ? ` --glob=${quote(params.glob)}` : ""} --glob '!.git' -- ${quote(params.pattern)} ${quote(target)}${capGrep(limit, lines)}`, signal, true);
		} }));
		pi.registerTool(tool({ ...local.find, async execute(id, params, signal, update, context) {
			const path = params.path ?? ".";
			const loopPath = resources.path(path);
			if (loopPath) return local.find.execute(id, { ...params, path: loopPath }, signal, update, context);
			const target = (await ready()).router.remotePath(path);
			// Like pi's local find: --no-require-git only when no ancestor has .git.
			// Inside a repo it would make fd obey a .gitignore above the repo root.
			const walk = `nrg=--no-require-git; d=${quote(target)}; while :; do if [ -e "$d/.git" ]; then nrg=; break; fi; case "$d" in /|.) break;; esac; d=$(dirname -- "$d"); done; `;
			return remoteSearch(id, `${walk}fd --glob --hidden $nrg --exclude .git --max-results ${number(params.limit, 1000)} ${fdPattern(params.pattern, target)} ${quote(target)}`, signal);
		} }));
		pi.registerTool(tool({ ...local.ls, async execute(id, params, signal, update, context) {
			const path = params.path ?? ".";
			const loopPath = resources.path(path);
			if (loopPath) return local.ls.execute(id, { ...params, path: loopPath }, signal, update, context);
			const target = (await ready()).router.remotePath(path);
			const result = await remoteSearch(id, `ls -1Ap -- ${quote(target)}`, signal);
			result.content[0].text = result.content[0].text.split("\n").slice(0, number(params.limit, 500)).join("\n");
			return result;
		} }));
		pi.registerTool({ ...local.powershell, exposure: "hidden", async execute() {
			throw new Error("PowerShell is unavailable in a mule workspace; use bash");
		} });
		pi.registerTool({
			name: "loop_mu", label: "Loop mu", description: "Run a mu command on Pi's local loop, without a shell. For workspace commands use bash.",
			parameters: Type.Object({ args: Type.Array(Type.String(), { description: "Arguments to mu, without the executable" }) }),
			async execute(_id, { args }, signal) {
				if (process.env.MU_ORCHESTRATOR) throw new Error("An orchestrator cannot use a remote workspace loop");
				const result = await pi.exec("mu", args, { signal, cwd, maxBuffer: 64 * 1024 });
				return { content: [{ type: "text", text: `${result.stdout}${result.stderr}\n[mu exit ${result.code}${result.truncated ? ", output clipped" : ""}]` }], details: undefined };
			},
		});
		const hidden = new Set(["powershell", ...(readOnly ? ["write", "edit"] : [])]);
		pi.setActiveTools([...active.filter((name) => !hidden.has(name)), "loop_mu"]);
		role = readOnly ? "read-only" : undefined;

		prompt = undefined;
		startupError = undefined;
		await ready().catch(() => { /* Overrides stay installed; ready reports the failure. */ });
	});

	pi.on("session_shutdown", () => {
		serve?.close();
		serve = undefined;
	});
	pi.on("user_bash", (_event, ctx) => {
		if (!enabled) return;
		if (!userBash) throw startupError ?? new Error("Mule workspace not ready");
		return { operations: userBash(ctx) };
	});
	pi.on("before_agent_start", (event) => {
		if (!enabled) return;
		// A new prompt is the user's go-ahead after a handback.
		handedBack = false;
		const options = event.systemPromptOptions;
		for (const skill of options.skills ?? []) loop?.keepLocal(dirname(skill.filePath));
		options.contextFiles = (options.contextFiles ?? []).filter((file) => file.path === join(homedir(), "AGENTS.md") || loop?.path(file.path));
		if (!prompt) {
			options.sections.mule = `Remote workspace unavailable. Do not substitute local workspace tools. Read the remote AGENTS.md first when the workspace recovers. ${String(startupError ?? "Still starting")}`;
			return;
		}
		const { host, root, agents } = prompt;
		options.cwd = root;
		options.sections.mule = `Workspace and shell: ${host}:${root}. Use loop_mu for mu commands; mu_delegate and Pi resources stay local.${role === "read-only" ? " Role: read-only. write and edit are disabled, and bash runs only read-only commands." : ""} If a tool reports HANDBACK, stop and ask the user to open the SSH master; do not retry.${agents ? `\n\nInstructions from ${root}/AGENTS.md:\n${agents}` : ""}`;
	});
}
