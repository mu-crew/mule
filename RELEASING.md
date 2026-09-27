# Releasing

A release publishes the prebuilt npm packages and the Rust crate from a `v*`
tag. The repository needs `NPM_TOKEN` and `CARGO_REGISTRY_TOKEN` Actions
secrets.

1. Bump `version` in `Cargo.toml` and update `Cargo.lock`.
2. Stamp the npm manifests:

   ```sh
   node scripts/npm-version.js
   ```

3. Run the commit gate from `AGENTS.md` and commit the release.
4. Tag the same version and push the commit and tag:

   ```sh
   git tag v0.2.0
   git push origin main v0.2.0
   ```

The tag workflow checks that the tag, Cargo version, and all npm package
versions agree. It publishes the three platform packages before
`@mu-crew/mule`, then publishes `mule-cli` to crates.io.
