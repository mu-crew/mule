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
 * only a backstop: timeout + queue + slack. Light commands (including a
 * missing muzzle or unusable wrap answer) get a separate Pi deadline.
 */
export function bashJob(command: string, options: { muzzle?: string; timeout?: number }) {
	const timeout = options.timeout && options.timeout > 0 ? Math.ceil(options.timeout) : undefined;
	if (!options.muzzle) return { command, timeout };
	const limit = timeout ? ` --run-timeout ${timeout}` : "";
	// Without timeout(1), Bash 3.2 job control gives the command and timer
	// their own groups. In both branches this shell stays in the pane's
	// group to relay mule's hangup to the command group.
	const light = timeout ? `if ! command -v timeout >/dev/null 2>&1; then
  stop_group() {
    [ -n "$1" ] || return 0
    # Stop before killing: a child woken by another member's death can fork.
    kill -STOP -- -"$1" || :
    kill -KILL -- -"$1" || :
  } 2>/dev/null
  finish() {
    trap '' HUP TERM INT USR1
    stop_group "$p"; stop_group "$timer"; exit "$1"
  }
  # A trap between an async launch and $! must not lose that child's group.
  interrupted() { if [ -n "$timer" ]; then finish "$1"; else pending=$1; fi; }
  p= timer= pending=
  trap 'interrupted 129' HUP TERM INT
  trap 'interrupted 124' USR1
  set -m
  bash -c "$c" </dev/null & p=$!
  ( sleep ${timeout} || :; kill -USR1 "$$" ) </dev/null >/dev/null 2>&1 & timer=$!
  set +m
  [ -z "$pending" ] || finish "$pending"
  rc=0
  wait "$p" || rc=$?
  finish "$rc"
else
  p=
  trap '[ -n "$p" ] && kill -KILL -- -"$p" "$p" 2>/dev/null; exit 129' HUP TERM INT
  timeout ${timeout} bash -c "$c" & p=$!
  wait "$p"; exit
fi
` : "";
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
	if (program === "sed") return refuseSed(args);
	if (program === "awk") return refuseAwk(args);
	if (program === "tree" && args.some((a) => /^-[a-zA-Z]*o|^--output/.test(a))) return "`tree -o`";
	if (program === "rg" && args.some((a) => /^--pre(?:=|$)/.test(a))) return "rg running a preprocessor";
	if ((program === "uniq" || program === "xxd") && operands(args, program === "uniq" ? /^-[fsw]$/ : /^-[cglosnC]$/).length > 1) {
		return `\`${program}\` with an output file`;
	}
	if (program === "sort" && args.some((a) => /^-[a-zA-Z]*o|^--output/.test(a))) return "`sort -o`";
	if (program === "find" && args.some((a) => /^-(?:delete|exec|execdir|ok|okdir|fprint|fprint0|fprintf|fls)$/.test(a))) {
		return "find with an action that runs or writes";
	}
	if (program === "fd" && args.some((a) => /^(?:-x|-X|--exec|--exec-batch)$/.test(a))) return "fd running commands";
	if (program === "git") {
		let i = 0;
		// Global options before the subcommand: -C dir, -c k=v, --no-pager, ...
		// -c can set a pager, an external diff or an fsmonitor: a command to run.
		while (i < args.length && args[i].startsWith("-")) {
			if (/^-c|^--config-env/.test(args[i])) return "`git -c`, which can make git run a command";
			i += args[i] === "-C" ? 2 : 1;
		}
		const sub = args[i];
		if (sub === undefined || !GIT_READERS.has(sub)) return `\`git ${sub ?? ""}\` is not a read-only git command`;
		if (args.some((a) => /^--output(?:=|$)|^--ext-diff$/.test(a))) return "git writing output to a file";
		if (sub === "grep" && args.some((a) => /^-O|^--open-files-in-pager/.test(a))) return "git grep running a pager";
	}
	return undefined;
}

// Arguments that are not options, skipping the values of `valued` options.
function operands(args: string[], valued: RegExp) {
	const found: string[] = [];
	for (let i = 0; i < args.length; i++) {
		if (args[i] === "--") { found.push(...args.slice(i + 1)); break; }
		if (valued.test(args[i])) i++;
		else if (!args[i].startsWith("-") || args[i] === "-") found.push(args[i]);
	}
	return found;
}

// sed and awk are languages, and both can write files from inside a script
// (`sed -n 'w out'`, `awk '{print > "out"}'`): an option check alone let those
// through. Allow a conservative subset, refuse the rest.
const SED_OPTIONS = /^-[nErsuz]+$|^--(?:quiet|silent|regexp-extended|separate|unbuffered|null-data|posix|debug|sandbox)$/;

function refuseSed(args: string[]): string | undefined {
	const scripts: string[] = [];
	let files = false;
	for (let i = 0; i < args.length; i++) {
		const arg = args[i];
		if (arg === "--") { files = true; continue; }
		if (files || !arg.startsWith("-") || arg === "-") {
			if (!scripts.length && !args.some((a) => /^-[a-zA-Z]*e|^--expression/.test(a))) scripts.push(arg);
			continue;
		}
		if (SED_OPTIONS.test(arg)) continue;
		const e = /^-([nErsuz]*)e(.*)$/.exec(arg) ?? /^--expression(?:=(.*))?$/.exec(arg);
		if (e) {
			const inline = e.length === 3 ? e[2] : e[1];
			if (inline) scripts.push(inline);
			else if (i + 1 < args.length) scripts.push(args[++i]);
			continue;
		}
		if (/^-[a-zA-Z]*i|^--in-place/.test(arg)) return "`sed -i`";
		return `sed option \`${arg}\` (a script file, or not known to be read-only)`;
	}
	for (const script of scripts) {
		const why = sedScriptRefusal(script);
		if (why) return why;
	}
	return undefined;
}

// Commands that only print, edit the pattern space or move between lines
// (with blocks, labels, s and y handled apart). Missing on purpose: w and W (write a file), e (run a command), r
// and R (read one), and a, i and c, whose text this does not parse.
const SED_SIMPLE = new Set("pPdDnNgGhHxlz=qQ}".split(""));

function sedScriptRefusal(script: string): string | undefined {
	let i = 0;
	// The body of a regex or s/// part up to its unescaped delimiter.
	const until = (delim: string) => {
		for (; i < script.length && script[i] !== delim; i++) {
			if (script[i] === "\\") i++;
			else if (script[i] === "\n") return false;
		}
		if (i >= script.length) return false;
		i++;
		return true;
	};
	const address = () => {
		if (/\d/.test(script[i] ?? "")) { while (/[\d~]/.test(script[i] ?? "")) i++; return true; }
		if (script[i] === "$") { i++; return true; }
		if (script[i] === "/" || script[i] === "\\") {
			const delim = script[i] === "\\" ? script[++i] : "/";
			i++;
			if (!delim || !until(delim)) return false;
			while (script[i] === "I" || script[i] === "M") i++;
			return true;
		}
		return true;
	};
	while (i < script.length) {
		while (/[\s;]/.test(script[i] ?? "")) i++;
		if (i >= script.length) break;
		if (!address()) return "a sed script this guard cannot read";
		if (script[i] === ",") { i++; if (script[i] === "+" || script[i] === "~") i++; if (!address()) return "a sed script this guard cannot read"; }
		while (/[\s!]/.test(script[i] ?? "")) i++;
		const command = script[i++];
		if (command === "s" || command === "y") {
			const delim = script[i++];
			if (!delim || delim === "\\" || delim === "\n" || !until(delim) || !until(delim)) return "a sed script this guard cannot read";
			if (command === "s") {
				const flags = /^[gpiImM0-9]*/.exec(script.slice(i))![0];
				i += flags.length;
				if (/^[we]/.test(script.slice(i))) return "sed writing a file or running a command (`s///w`, `s///e`)";
			}
		} else if (command === "{") {
			continue;
		} else if (command === ":" || command === "b" || command === "t" || command === "T") {
			// A label or a branch to one: control flow, no I/O.
			while (i < script.length && !/[;\n]/.test(script[i])) i++;
		} else if (!command || !SED_SIMPLE.has(command)) {
			return command && /[wWe]/.test(command) ? "sed writing a file or running a command (`w`, `W`, `e`)" : `sed command \`${command ?? ""}\``;
		}
		if (command === "q" || command === "Q") while (/\d/.test(script[i] ?? "")) i++;
		while (/[ \t]/.test(script[i] ?? "")) i++;
		if (i < script.length && !/[;\n}]/.test(script[i])) return "a sed script this guard cannot read";
	}
	return undefined;
}

function refuseAwk(args: string[]): string | undefined {
	let program: string | undefined;
	for (let i = 0; i < args.length; i++) {
		const arg = args[i];
		if (arg === "--") { program ??= args[i + 1]; break; }
		if (/^-[Fv]$/.test(arg)) { i++; continue; }
		if (/^-[Fv]./.test(arg)) continue;
		if (arg.startsWith("-") && arg !== "-") return `awk option \`${arg}\` (a program file, in-place edit, or not known to be read-only)`;
		program = arg;
		break;
	}
	if (program === undefined) return "awk without a program";
	// Strings and regex literals may hold any character (a backslash-newline
	// too); only code counts. Then join lines the way awk does before it
	// parses: drop comments and backslash-newlines, and continue a line ending
	// in `,`, `&&`, `||`, `do` or `else`. Otherwise `print \` and the `>` on
	// the next line are two statements here and one to awk.
	const code = program
		.replace(/"(?:[^"\\\n]|\\[\s\S])*"/g, '""')
		.replace(/(^|[\s{(,;!~&|])\/(?:[^/\\\n]|\\[\s\S])*\//g, "$1//")
		.replace(/#[^\n]*/g, "")
		.replace(/\\\n/g, " ")
		.replace(/(,|&&|\|\||\bdo|\belse)\s*\n/g, "$1 ");
	if (/system\s*\(|getline|fflush|close\s*\(/.test(code)) return "awk running commands";
	if (/(?<!\|)\|(?!\|)/.test(code)) return "awk piping to a command";
	// In a print statement, > is output redirection: `print $1 > 5` writes
	// to a file named 5. Elsewhere it is a comparison.
	for (const statement of code.split(/[;{}\n]/)) {
		const print = /(?:^|[^\w$])printf?(?!\w)/.exec(statement);
		if (print && statement.slice(print.index).includes(">")) {
			return "awk writing a file (`print > file`)";
		}
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
