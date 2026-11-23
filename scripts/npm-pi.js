#!/usr/bin/env node
// prepack for npm/mule: copy the Pi extension's runtime files from the
// checkout's pi/ into the package, so `npm pack ./npm/mule` and
// `npm publish ./npm/mule` ship exactly them. Tests and their doubles stay out.
const fs = require("node:fs");
const path = require("node:path");

const source = path.resolve(__dirname, "../pi");
const target = path.resolve(__dirname, "../npm/mule/pi");
const shipped = (name) => (name.endsWith(".ts") && !name.endsWith(".test.ts")) || name === "README.md";

fs.rmSync(target, { recursive: true, force: true });
fs.mkdirSync(target);
const files = fs.readdirSync(source, { withFileTypes: true })
  .filter((entry) => entry.isFile() && shipped(entry.name))
  .map((entry) => entry.name);
for (const name of files) fs.copyFileSync(path.join(source, name), path.join(target, name));
if (!files.includes("mule.ts")) {
  console.error("npm-pi: pi/mule.ts missing; nothing to ship");
  process.exit(1);
}
console.log(`npm-pi: ${files.length} files into npm/mule/pi`);
