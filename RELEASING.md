# Releasing

Each package is versioned and published to Hex separately. A package can be
published only after the versions of the `slap_*` packages it depends on are on
Hex. The `projects` list in the `justfile` is in dependency order:

```
slap_slatedb, slap_cluster, slap_streams, slap_snapshot_log, slap_yjs,
slap_kv, slap_files, slap
```

The `just` recipes below run in `nix develop`, from the repository root.

## Steps

1. **Decide what to release.**

   ```sh
   just release-status
   ```

   For each package, this shows its `mix.exs` version, whether that version is
   tagged and on Hex, whether its `CHANGELOG.md` has an entry for it, and the
   commits since the package's latest tag that touch its directory. Read the
   commits and decide which packages have changes worth releasing; a commit
   such as adding a lock file does not need one.

2. **Prepare the versions.** For each package you are releasing, set `@version`
   in its `mix.exs` and add a `## <version>` entry to its `CHANGELOG.md`. If a
   new version no longer meets a requirement in another package (for example,
   `slap_cluster` 0.2.0 and `slap_kv`'s `~> 0.1.0`), `just release-status`
   reports it: update the requirement and release that package as well. Run
   `just check`, commit, and push `main`.

3. **Release the NIF**, if you are releasing `slap_slatedb`. See
   [The SlateDB NIF](#the-slatedb-nif) below. Do this before tagging the other
   packages, because it adds a commit.

4. **Tag the packages.**

   ```sh
   just release-tags
   git push origin --tags
   ```

   `just release-tags` tags HEAD `<package>-v<version>` for each package whose
   `mix.exs` version has no tag yet. It lists the tags and asks you to confirm
   before creating them. It refuses to run with uncommitted changes, and
   refuses a package whose `CHANGELOG.md` has no entry for its version or
   whose requirements are not met by its dependencies' current versions.
   HexDocs links each module's source to its package's tag, so push the tags
   before publishing.

5. **Publish.** Authenticate with `mix hex.user auth` once per machine, then:

   ```sh
   just publish
   ```

   `just publish` publishes, in dependency order, each package whose version is
   not on Hex yet. It refuses to run with uncommitted changes, and refuses a
   package whose version tag is missing or not pushed to `origin`. It runs in
   a temporary `git worktree` of HEAD with `SLAP_LOCAL_DEPS=0` (dependencies
   from Hex) and `SLAP_SLATEDB_BUILD=0` (the prebuilt NIF), so this checkout's
   `mix.lock` files, `deps` and `_build` are not touched. `mix hex.publish`
   shows each package's files and asks you to confirm before publishing it and
   its documentation. A package published earlier in the run can take a few
   minutes to appear in the Hex registry, so `mix deps.get` is retried for up
   to five minutes. If a package fails, fix the problem and run `just publish`
   again: packages already on Hex are skipped.

6. **Check the release.** Each package's page on <https://hex.pm> should show
   the new version, and its documentation should load on <https://hexdocs.pm>.
   Then run the examples in the root README's Usage Examples, which use the Hex
   packages. That step changes `slap/mix.lock`; restore it afterwards with
   `git restore slap/mix.lock`.

## Updating documentation

HexDocs can replace a released version's documentation without a new release,
for example after a README change. Commit the change, then:

```sh
just publish-docs [package...]
```

This builds each package's documentation from HEAD and replaces the
documentation of its `mix.exs` version on HexDocs, in the same kind of
worktree as `just publish`. It refuses to run with uncommitted changes, and
refuses a package whose version is not on Hex. Before asking you to confirm,
it lists the commits since each package's tag that touch its `lib/`: the
documentation shows them, so publish only if they don't change the API. The
README inside the released package is not updated until its next version.

## The SlateDB NIF

`slap_slatedb` loads a Rust NIF. Its Hex package downloads a prebuilt library
for the user's platform from the package's GitHub release, and checks it
against `slap_slatedb/checksum-Elixir.Slap.SlateDB.Native.exs`. Prebuilt
libraries cover six targets (`aarch64` and `x86_64` macOS, Linux gnu, and Linux
musl) at NIF version 2.15, which runs on OTP 22 and later. The targets and NIF
version are listed in `Slap.SlateDB.Native` and in
`.github/workflows/slap_slatedb_release.yml`; keep the two in sync. Users on
other platforms build the NIF from source with `SLAP_SLATEDB_BUILD=1`, from the
Rust source in the package.

1. **Tag and push `slap_slatedb` alone:**

   ```sh
   just release-tags slap_slatedb
   git push origin slap_slatedb-v<version>
   ```

   The tag starts the release workflow. It checks that the tag matches
   `@version`, builds the six libraries (the Linux ones with `cross`), and
   attaches them to the GitHub release `slap_slatedb-v<version>`. Wait for its
   six jobs to pass, and check that the release has six files:
   `gh release view slap_slatedb-v<version>`.

2. **Write the checksum file**, from `slap_slatedb/`:

   ```sh
   mix rustler_precompiled.download Slap.SlateDB.Native --all --print
   ```

   This downloads the six libraries and writes their SHA-256 checksums to
   `checksum-Elixir.Slap.SlateDB.Native.exs`.

3. **Check the prebuilt library**, from `slap_slatedb/`:

   ```sh
   SLAP_SLATEDB_BUILD=0 MIX_ENV=test mix compile --force
   SLAP_SLATEDB_BUILD=0 mix test
   MIX_ENV=test mix compile --force
   ```

   The first command loads the library for this machine from the release and
   checks it against the checksum file, instead of building it with Cargo. The
   last command builds the NIF from source again, as the Nix shell expects;
   Mix does not recompile on its own when `SLAP_SLATEDB_BUILD` changes.

4. **Commit the checksum file** and push `main`. The tag does not need it; the
   Hex package does.

Then continue with step 4 of [Steps](#steps). `just release-tags` skips
`slap_slatedb`, which is already tagged.

## Upgrading SlateDB

SlateDB is pinned to an exact version in
`slap_slatedb/native/slatedb_nif/Cargo.toml` because its APIs and on-disk
format can change between minor versions. Upgrade one minor version at a time,
read its release notes for format changes, and run
`mix test --include slow --include crash` in `slap_slatedb/` with the S3 and
Azure tests enabled. The crash test checks that every write reported durable
survives an unclean shutdown.
