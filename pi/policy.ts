import { quote } from "./exec-backend.ts";

// How long a heavy command may queue for the host's muzzle slots before
// muzzle gives up (exit 75). muzzle recommends 900 s for managed agents.
export const MUZZLE_QUEUE_SECS = 900;
// Slack for the `muzzle wrap` probe and process start inside the job.
const MUZZLE_SLACK_SECS = 30;

/**
 * The bash program a detached job runs for one `bash` call, and the
 * deadline mule enforces on the whole job.
 *
 * Without a muzzle the deadline is Pi's timeout. With one, the job asks the
 * host's own muzzle whether the command is heavy (its config is the only
 * judge) and, if so, queues it there. A Pi timeout must then count from
 * admission, not from the queue (`--run-timeout`), so mule's deadline is
 * only a backstop: timeout + queue + slack. A light command keeps the Pi
 * timeout through `timeout(1)` where the host has it. A muzzle that has
 * disappeared since startup, or answers nothing usable, runs the command
 * unwrapped: admission is advice, never a reason not to run.
 */
export function bashJob(command: string, options: { muzzle?: string; timeout?: number }) {
	const timeout = options.timeout && options.timeout > 0 ? Math.ceil(options.timeout) : undefined;
	if (!options.muzzle) return { command, timeout };
	const limit = timeout ? ` --run-timeout ${timeout}` : "";
	const light = timeout
		? `if command -v timeout >/dev/null 2>&1; then exec timeout ${timeout} bash -c "$c"; fi\n`
		: "";
	const script = `c=${quote(command)}
m=${quote(options.muzzle)}
if [ -x "$m" ] && w=$("$m" wrap --json --cwd "$PWD" -- "$c" 2>/dev/null) \\
  && [[ $w =~ \\"rule\\":\\"([A-Za-z0-9._:-]+)\\" ]] && r=\${BASH_REMATCH[1]} \\
  && [[ $w =~ \\"slots\\":([0-9]+) ]]; then
  exec "$m" run --rule="$r" --slots "\${BASH_REMATCH[1]}" --timeout ${MUZZLE_QUEUE_SECS}${limit} -- bash -c "$c"
fi
${light}exec bash -c "$c"`;
	return { command: script, timeout: timeout && timeout + MUZZLE_QUEUE_SECS + MUZZLE_SLACK_SECS };
}

// Read-only role. This is a guard against a reviewer's accidents, not a
// sandbox: mu's role is advisory (VOCABULARY: "not a security boundary").
// It fails closed: a command is allowed only when every simple command in
// it is a known reader, so an unknown program is refused, not guessed at.
const READERS = new Set([
	"cat", "ls", "head", "tail", "wc", "grep", "egrep", "fgrep", "rg", "fd", "find", "pwd", "echo", "printf",
	"stat", "file", "du", "df", "tree", "diff", "cmp", "sort", "uniq", "cut", "tr", "sed", "awk", "jq", "git",
	"cd", "true", "false", "test", "[", "which", "type", "date", "uname", "whoami", "id", "hostname",
	"basename", "dirname", "realpath", "readlink", "nl", "xxd", "od", "hexdump", "sha256sum", "sha1sum",
	"md5sum", "cksum", "column", "comm", "paste", "fold", "rev", "tac", "ps",
]);
const GIT_READERS = new Set([
	"status", "log", "show", "diff", "blame", "grep", "ls-files", "ls-tree", "rev-parse", "describe",
	"shortlog", "cat-file", "rev-list", "merge-base", "name-rev",
]);
function refuseSimple(words: string[]): string | undefined {
	const [program, ...args] = words;
	if (/^[A-Za-z_][A-Za-z0-9_]*=/.test(program)) return "an environment assignment";
	if (!READERS.has(program)) return `\`${program}\` is not a known read-only command`;
	if (program === "sed" && args.some((a) => /^-[a-zA-Z]*i|^--in-place/.test(a))) return "`sed -i`";
	if (program === "awk" && args.some((a) => /system\s*\(|getline|fflush/.test(a))) return "awk running commands";
	if (program === "sort" && args.some((a) => /^-[a-zA-Z]*o|^--output/.test(a))) return "`sort -o`";
	if (program === "find" && args.some((a) => /^-(?:delete|exec|execdir|ok|okdir|fprint|fprint0|fprintf|fls)$/.test(a))) {
		return "find with an action that runs or writes";
	}
	if (program === "fd" && args.some((a) => /^(?:-x|-X|--exec|--exec-batch)$/.test(a))) return "fd running commands";
	if (program === "git") {
		let i = 0;
		// Global options before the subcommand: -C dir, -c k=v, --no-pager, ...
		while (i < args.length && args[i].startsWith("-")) i += args[i] === "-C" || args[i] === "-c" ? 2 : 1;
		const sub = args[i];
		if (sub === undefined || !GIT_READERS.has(sub)) return `\`git ${sub ?? ""}\` is not a read-only git command`;
		if (args.some((a) => /^--output(?:=|$)|^--ext-diff$/.test(a))) return "git writing output to a file";
	}
	return undefined;
}

type Lexed = { commands: string[][] } | { refused: string };

// Shell words and command boundaries, quotes honoured, so `rg 'a|b'` is one
// command. Anything this lexer does not model is refused, not skipped.
function lex(command: string): Lexed {
	const commands: string[][] = [[]];
	let word: string | undefined;
	const end = () => { if (word !== undefined) commands.at(-1)!.push(word); word = undefined; };
	for (let i = 0; i < command.length; i++) {
		const ch = command[i];
		if (ch === "'") {
			const close = command.indexOf("'", i + 1);
			if (close < 0) return { refused: "an unterminated quote" };
			word = (word ?? "") + command.slice(i + 1, close);
			i = close;
		} else if (ch === '"') {
			let j = i + 1;
			let text = "";
			for (; j < command.length && command[j] !== '"'; j++) {
				if (command[j] === "\\") text += command[++j] ?? "";
				else if (command[j] === "`" || (command[j] === "$" && command[j + 1] === "(")) return { refused: "command substitution" };
				else text += command[j];
			}
			if (j >= command.length) return { refused: "an unterminated quote" };
			word = (word ?? "") + text;
			i = j;
		} else if (ch === "\\") {
			word = (word ?? "") + (command[++i] ?? "");
		} else if (ch === "`" || (ch === "$" && command[i + 1] === "(")) {
			return { refused: "command substitution" };
		} else if (ch === "<" && (command[i + 1] === "(" || command[i + 1] === "<")) {
			return { refused: "process substitution or a heredoc" };
		} else if (ch === ">") {
			// Only redirections that cannot write a workspace file: 2>&1, >/dev/null.
			const rest = command.slice(i + 1);
			const harmless = /^&\d/.exec(rest) ?? /^>?\s*\/dev\/null(?=[\s;&|)]|$)/.exec(rest);
			if (!harmless) return { refused: "an output redirection" };
			if (word !== undefined && /^\d$|^&$/.test(word)) word = undefined;
			end();
			i += harmless[0].length;
		} else if (ch === "(" && command[i + 1] === "(") {
			return { refused: "arithmetic evaluation" };
		} else if (/[|;&\n(){}!]/.test(ch)) {
			end();
			if (commands.at(-1)!.length) commands.push([]);
		} else if (/\s/.test(ch)) {
			end();
		} else {
			word = (word ?? "") + ch;
		}
	}
	end();
	return { commands: commands.filter((c) => c.length) };
}

/** Why a read-only role refuses this bash command, or undefined to allow it. */
export function readOnlyRefusal(command: string): string | undefined {
	const lexed = lex(command);
	if ("refused" in lexed) return lexed.refused;
	for (const words of lexed.commands) {
		const why = refuseSimple(words);
		if (why) return why;
	}
	return undefined;
}
