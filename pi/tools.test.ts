import assert from "node:assert/strict";
import test from "node:test";
import { runPiCase } from "./test/run-pi.ts";

for (const name of ["routing", "find", "findgit", "search", "startup", "racestart", "cancel", "reattach", "handback", "readonly", "muzzle", "caps"]) {
	test(`Pi tools: ${name}`, () => {
		const result = runPiCase(name);
		assert.equal(result.status, 0, `${result.error ?? ""}\n${result.stdout}\n${result.stderr}`);
		assert.match(result.stdout, /mule-tool-check: ok/);
	});
}
