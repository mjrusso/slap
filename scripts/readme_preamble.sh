#!/usr/bin/env bash
# Writes scripts/readme_preamble.md into each package README, between the
# `<!-- slap-preamble -->` and `<!-- /slap-preamble -->` lines, or checks that
# every README already contains it. Run through `just readme-preamble` and
# `just readme-preamble-check`.
#
#     scripts/readme_preamble.sh write|check "<packages>"

set -euo pipefail
cd "$(dirname "$0")/.."

command=$1
packages=$2
source=scripts/readme_preamble.md

render() {
  awk -v source="$source" '
    /^<!-- slap-preamble -->$/ {
      print
      while ((getline line < source) > 0) print line
      inside = 1
      found++
      next
    }
    /^<!-- \/slap-preamble -->$/ { inside = 0 }
    !inside { print }
    END { if (found != 1 || inside) exit 1 }
  ' "$1"
}

failed=0
for p in $packages; do
  readme="$p/README.md"
  if ! rendered=$(render "$readme"); then
    echo "$readme: needs one <!-- slap-preamble --> ... <!-- /slap-preamble --> block" >&2
    failed=1
    continue
  fi
  case "$command" in
    write) printf '%s\n' "$rendered" > "$readme" ;;
    check)
      if ! cmp -s "$readme" <(printf '%s\n' "$rendered"); then
        echo "$readme: preamble differs from $source; run \`just readme-preamble\`" >&2
        failed=1
      fi
      ;;
    *) echo "unknown command: $command" >&2; exit 1 ;;
  esac
done
exit $failed
