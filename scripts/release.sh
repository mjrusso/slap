#!/usr/bin/env bash
# Release steps for the packages, run through `just release-status`,
# `just release-tags` and `just publish`. See RELEASING.md.
#
#     scripts/release.sh status|tags|publish "<all packages, in dependency order>" [package...]

set -euo pipefail
cd "$(dirname "$0")/.."

command=$1
all=$2
shift 2
requested=("$@")

for p in "${requested[@]}"; do
  [[ " $all " == *" $p "* ]] || { echo "unknown package: $p" >&2; exit 1; }
done

packages=()
for p in $all; do
  if [ ${#requested[@]} -eq 0 ] || [[ " ${requested[*]} " == *" $p "* ]]; then
    packages+=("$p")
  fi
done

version() { sed -n 's/^  @version "\(.*\)"$/\1/p' "$1/mix.exs"; }

for p in $all; do
  [ -n "$(version "$p")" ] || { echo "no @version line found in $p/mix.exs" >&2; exit 1; }
done

tagged() { git rev-parse -q --verify "refs/tags/$1-v$(version "$1")" > /dev/null; }

# 200: published; 404: not published. Anything else (no network, an outage)
# stops the script rather than being taken for "not published".
on_hex() {
  local status
  status=$(curl -s -o /dev/null -w '%{http_code}' \
    "https://hex.pm/api/packages/$1/releases/$(version "$1")")
  case "$status" in
    200) return 0 ;;
    404) return 1 ;;
    *) echo "could not check $1 on Hex (HTTP $status)" >&2; exit 1 ;;
  esac
}

has_changelog_entry() { grep -qx "## $(version "$1")" "$1/CHANGELOG.md"; }

# Tags and publishing use HEAD, so the files checked here must be committed.
require_clean() {
  [ -z "$(git status --porcelain)" ] ||
    { echo "commit or stash your changes first: $1 uses HEAD" >&2; exit 1; }
}

# Prints "<dependency> <its version> <requirement>" for each slap_* dependency
# whose current version does not meet the package's requirement.
stale_requirements() {
  sed -n 's/^ *slap_dep(:\([a-z_]*\), "\([^"]*\)").*$/\1 \2/p' "$1/mix.exs" |
    while read -r dep requirement; do
      echo "$dep $(version "$dep") $requirement"
    done |
    elixir -e '
      for line <- IO.stream(:stdio, :line),
          [dep, version, requirement] = String.split(line, " ", parts: 3),
          requirement = String.trim(requirement),
          not Version.match?(version, requirement),
          do: IO.puts("#{dep} #{version} #{requirement}")
    '
}

status() {
  for p in "${packages[@]}"; do
    v=$(version "$p")
    if on_hex "$p"; then state="on Hex"
    elif tagged "$p"; then state="tagged, not on Hex"
    else state="not tagged"
    fi
    has_changelog_entry "$p" && changelog="has a CHANGELOG entry" ||
      changelog="NO CHANGELOG ENTRY"
    echo "$p $v: $state, $changelog"

    latest=$(git tag -l "$p-v*" --sort=-v:refname | head -n 1)
    if [ -n "$latest" ]; then
      commits=$(git log --oneline "$latest..HEAD" -- "$p/")
      if [ -n "$commits" ]; then
        echo "  commits since $latest:"
        while read -r line; do echo "    $line"; done <<< "$commits"
      else
        echo "  no commits since $latest"
      fi
    fi

    stale_requirements "$p" | while read -r dep dep_version requirement; do
      echo "  requires $dep $requirement, but $dep is at $dep_version"
    done
  done
}

tags() {
  require_clean "tagging"
  to_tag=()
  for p in "${packages[@]}"; do
    if tagged "$p"; then
      echo "$p-v$(version "$p") already exists"
    else
      to_tag+=("$p")
    fi
  done
  [ ${#to_tag[@]} -gt 0 ] || { echo "nothing to tag"; return; }

  problems=0
  for p in "${to_tag[@]}"; do
    if ! has_changelog_entry "$p"; then
      echo "$p: $p/CHANGELOG.md has no \"## $(version "$p")\" entry" >&2
      problems=1
    fi
    # Assigned first, so that a failure of the check stops the script.
    stale=$(stale_requirements "$p")
    while read -r dep dep_version requirement; do
      [ -n "$dep" ] || continue
      echo "$p: requires $dep $requirement, but $dep is at $dep_version" >&2
      problems=1
    done <<< "$stale"
  done
  [ "$problems" = 0 ] || exit 1

  echo "Tags to create on HEAD ($(git rev-parse --short HEAD)):"
  for p in "${to_tag[@]}"; do echo "  $p-v$(version "$p")"; done
  read -r -p "Create these tags? [y/N] " answer
  [[ "$answer" == [yY] ]] || { echo "no tags created"; exit 1; }
  for p in "${to_tag[@]}"; do git tag "$p-v$(version "$p")"; done
  echo "Created. Push them with: git push origin --tags"
}

publish() {
  require_clean "publishing"
  selected=()
  for p in "${packages[@]}"; do
    if on_hex "$p"; then
      echo "$p $(version "$p") is already on Hex"
      continue
    fi
    tag="$p-v$(version "$p")"
    tagged "$p" || { echo "$tag is not tagged: run \`just release-tags\` first" >&2; exit 1; }
    # HexDocs links each module's source to this tag on GitHub.
    git ls-remote --exit-code --tags origin "refs/tags/$tag" > /dev/null ||
      { echo "$tag is not on origin: run \`git push origin --tags\` first" >&2; exit 1; }
    selected+=("$p")
  done
  [ ${#selected[@]} -gt 0 ] || { echo "nothing to publish"; return; }

  # A separate worktree, because SLAP_LOCAL_DEPS=0 rewrites mix.lock files.
  dir=$(mktemp -d)
  git worktree add --detach "$dir" HEAD
  trap 'git worktree remove --force "$dir"' EXIT
  export SLAP_LOCAL_DEPS=0 SLAP_SLATEDB_BUILD=0
  for p in "${selected[@]}"; do
    echo "==> $p"
    (
      cd "$dir/$p"
      # A package published earlier in this run can take a few minutes to
      # appear in the Hex registry, so retry until its dependents resolve.
      for attempt in $(seq 20); do
        mix deps.get && break
        [ "$attempt" -lt 20 ] || exit 1
        echo "retrying mix deps.get in 15 s ($attempt/20)"
        sleep 15
      done
      mix hex.publish
    )
  done
}

case "$command" in
  status | tags | publish) "$command" ;;
  *) echo "unknown command: $command" >&2; exit 1 ;;
esac
