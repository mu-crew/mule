"""Build and check the two native release binaries from the repo root."""

import os
import re
import subprocess
import sys
from pathlib import Path


# Check common build roots as well as the actual prefixes passed to rustc.
LOCAL_PATH = re.compile(
    rb"/(?:Users|home|root|tmp|private/tmp|(?:private/)?var/folders|"
    rb"__w|github/workspace|builds?|workspaces?)/[^/\x00\s]"
)


def check_paths(binaries, prefixes):
    forbidden = [os.fsencode(prefix) for prefix in prefixes]
    for binary in binaries:
        data = binary.read_bytes()
        if LOCAL_PATH.search(data) or any(prefix in data for prefix in forbidden):
            # Do not echo the embedded path into the release log.
            sys.exit(f"{binary.name}: contains a local home or build path")
        print(f"{binary.name}: no local home or build paths found", flush=True)


def main(target):
    home = Path.home()
    workspace = Path.cwd()
    remaps = {}
    for path, replacement in (
        (home, "release-home"),
        (Path(os.environ.get("CARGO_HOME") or home / ".cargo"), "cargo-src"),
        (Path(os.environ.get("PWD") or workspace), "mule-src"),
        (workspace, "mule-src"),
    ):
        for prefix in (path.absolute(), path.resolve()):
            if prefix == Path("/"):
                sys.exit("cannot remap the filesystem root")
            remaps[prefix] = replacement

    # rustc's last matching prefix wins. Apply broader prefixes first so the
    # registry and worktree get stable names even when they live under HOME.
    flags = [
        f"--remap-path-prefix={prefix}={remaps[prefix]}"
        for prefix in sorted(remaps, key=lambda prefix: len(prefix.parts))
    ]
    command = ["cargo", "build", "--release", "--locked", "--bins"]
    subprocess.run(
        command + ["--target", target, "--target-dir", "target"],
        env={**os.environ, "CARGO_ENCODED_RUSTFLAGS": "\x1f".join(flags)},
        check=True,
    )
    output = Path("target") / target / "release"
    check_paths((output / "mule", output / "mu-plugin-mule"), remaps)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit("usage: python3 scripts/build-release.py <rust-target>")
    main(sys.argv[1])
