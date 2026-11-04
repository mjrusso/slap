#!/usr/bin/env bash
# Run through `just readme-preamble` and `just readme-preamble-check`.
#
#     scripts/readme_preamble.sh write|check "<packages>"

set -euo pipefail
cd "$(dirname "$0")/.."

command=$1
packages=$2
source=scripts/readme_preamble.md

render() {
  awk -v source="$source" -v package="$2" '
    /^<!-- slap-preamble -->$/ {
      print
      if (package != "slap") {
        print "> #### About Slap {: .info}"
        print ">"
        print "> This package is part of [Slap](https://github.com/mjrusso/slap)."
        print ">"
      }
      while ((getline line < source) > 0) {
        if (package == "slap") print line
        else print (line == "" ? ">" : "> " line)
      }
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
  case "$p" in
    slap) document="$p/guides/overview.md" ;;
    *) document="$p/README.md" ;;
  esac
  if ! rendered=$(render "$document" "$p"); then
    echo "$document: needs one <!-- slap-preamble --> ... <!-- /slap-preamble --> block" >&2
    failed=1
    continue
  fi
  case "$command" in
    write) printf '%s\n' "$rendered" > "$document" ;;
    check)
      if ! cmp -s "$document" <(printf '%s\n' "$rendered"); then
        echo "$document: preamble differs from $source; run \`just readme-preamble\`" >&2
        failed=1
      fi
      ;;
    *) echo "unknown command: $command" >&2; exit 1 ;;
  esac
done
exit $failed
