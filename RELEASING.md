# Releasing

A `v*` tag publishes the prebuilt npm packages and the Rust crate. There are
no tokens: npm and crates.io trust this repo's `.github/workflows/release.yml`
through OIDC (trusted publishing).

1. Bump `version` in `Cargo.toml` and update `Cargo.lock`.
2. Stamp the npm manifests:

   ```sh
   node scripts/npm-version.js
   ```

3. Run the commit gate from `AGENTS.md` and commit the release.
4. Tag the same version and push the commit and tag:

   ```sh
   git tag -a v0.2.2 -m "mule 0.2.2"
   git push origin main v0.2.2
   ```

The workflow checks that the tag, Cargo version and all npm package versions
agree, publishes the three platform packages, then `@mu-crew/mule`, then
`mule-cli`.

## Local release builds

Use the same build and path check as CI, from the repo root:

```sh
# Substitute the matching musl target on Linux; see release.yml.
python3 scripts/build-release.py aarch64-apple-darwin
install -d npm/mule-darwin-arm64/bin
install -m 0755 target/aarch64-apple-darwin/release/mule \
  target/aarch64-apple-darwin/release/mu-plugin-mule npm/mule-darwin-arm64/bin/
npm pack ./npm/mule-darwin-arm64 ./npm/mule
```

The build remaps the home, Cargo home, and worktree prefixes and checks both
binaries before they can be packaged. The root npm package launches the native
`mule`, which finds `mu-plugin-mule` beside itself; keep the pair together.

## Trust setup

Each package trusts owner `mu-crew`, repository `mule`, workflow `release.yml`:

- npmjs.com, per package (`@mu-crew/mule`, `@mu-crew/mule-linux-x64`,
  `@mu-crew/mule-linux-arm64`, `@mu-crew/mule-darwin-arm64`): Settings →
  Trusted publishing → GitHub Actions.
- crates.io, `mule-cli`: Settings → Trusted Publishing.

A new package (a new platform, say) cannot be trusted before it exists. Publish
its first version by hand (`npm login`, then `npm publish --access public`),
add the trusted publisher, and let CI publish from then on.

The workflow also creates the GitHub release for the tag, with the matching
CHANGELOG section as its notes. It fails if that section is missing, so add
the CHANGELOG entry before tagging. The release carries a `mule-<version>-<target>.tar.gz`
per platform (Linux x64/arm64 musl, macOS arm64), each containing both
executables, and a `SHA256SUMS` covering those archives.
