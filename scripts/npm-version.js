#!/usr/bin/env node

const fs = require("node:fs");
const path = require("node:path");

const root = path.resolve(__dirname, "..");
const cargo = fs.readFileSync(path.join(root, "Cargo.toml"), "utf8");
const version = cargo.match(/^version = "([^"]+)"$/m)?.[1];
if (!version) throw new Error("could not read package version from Cargo.toml");

const packages = [
  "npm/mule/package.json",
  "npm/mule-linux-x64/package.json",
  "npm/mule-linux-arm64/package.json",
  "npm/mule-darwin-arm64/package.json",
];

if (process.argv[2] === "--check") {
  const tag = process.argv[3]?.replace(/^refs\/tags\//, "").replace(/^v/, "");
  const mismatches = packages
    .map((file) => [file, JSON.parse(fs.readFileSync(path.join(root, file))).version])
    .filter(([, packageVersion]) => packageVersion !== version);
  if (tag !== version) mismatches.unshift(["tag", tag ?? "missing"]);
  if (mismatches.length) {
    for (const [source, found] of mismatches) {
      console.error(`${source}: expected ${version}, found ${found}`);
    }
    process.exit(1);
  }
  console.log(`release versions agree: ${version}`);
} else {
  for (const file of packages) {
    const filename = path.join(root, file);
    const contents = JSON.parse(fs.readFileSync(filename));
    contents.version = version;
    if (contents.optionalDependencies) {
      for (const dependency of Object.keys(contents.optionalDependencies)) {
        contents.optionalDependencies[dependency] = version;
      }
    }
    fs.writeFileSync(filename, `${JSON.stringify(contents, null, 2)}\n`);
  }
  console.log(`stamped npm packages at ${version}`);
}
