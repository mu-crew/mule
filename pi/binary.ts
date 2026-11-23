import { existsSync, readFileSync } from "node:fs";
import { createRequire } from "node:module";
import { isAbsolute, join } from "node:path";

// The mule this extension ships with, never one found on PATH: an explicit
// absolute MULE_PI_BIN, else the platform package beside the npm package,
// else (a checkout) its own debug build.
export function muleBinary(extensionDir: string, override?: string) {
	if (override !== undefined) {
		if (!isAbsolute(override)) throw new Error("MULE_PI_BIN must be an absolute path");
		return override;
	}
	const manifest = join(extensionDir, "../package.json");
	if (!existsSync(manifest)) return join(extensionDir, "../target/debug/mule");
	const pkg = JSON.parse(readFileSync(manifest, "utf8"));
	const platform = `${pkg.name}-${process.platform}-${process.arch}`;
	if (!Object.hasOwn(pkg.optionalDependencies ?? {}, platform)) throw new Error(`mule has no package for ${process.platform}-${process.arch}`);
	try {
		return createRequire(manifest).resolve(`${platform}/bin/mule`);
	} catch {
		throw new Error(`mule could not find ${platform}; reinstall ${pkg.name}`);
	}
}
