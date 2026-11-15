#!/usr/bin/env bash
set -euo pipefail

revgrep=$1
base=$(git merge-base "$2" HEAD)
shift 2
output=$(mktemp)
trap 'rm -f "$output"' EXIT

status=0
"$@" > "$output" 2>&1 || status=$?
# singlechecker exits 3 for diagnostics; loading and execution failures must stay fatal.
if [[ "$status" != 0 && "$status" != 3 ]]; then
  cat "$output" >&2
  exit "$status"
fi

"$revgrep" "$base" < "$output"
