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

## Trust setup

Each package trusts owner `mu-crew`, repository `mule`, workflow `release.yml`:

- npmjs.com, per package (`@mu-crew/mule`, `@mu-crew/mule-linux-x64`,
  `@mu-crew/mule-linux-arm64`, `@mu-crew/mule-darwin-arm64`): Settings →
  Trusted publishing → GitHub Actions.
- crates.io, `mule-cli`: Settings → Trusted Publishing.

A new package (a new platform, say) cannot be trusted before it exists. Publish
its first version by hand (`npm login`, then `npm publish --access public`),
add the trusted publisher, and let CI publish from then on.
