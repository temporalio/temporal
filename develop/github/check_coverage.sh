#!/usr/bin/env bash
# Enforce a minimum unit-test coverage floor on a fixed set of packages and,
# when a base ref is supplied, that the coverage of each package does not
# decrease relative to that base.
#
# Usage:
#   check_coverage.sh [--min PCT] [--base REF] [PACKAGE ...]
#
# Flags:
#   --min PCT    Minimum per-package coverage percentage (default: 80, or the
#                MIN_COVERAGE env var if set).
#   --base REF   Base git ref to compare against; when set, the base revision is
#                checked out into a throwaway git worktree and measured with the
#                same logic, so the comparison reflects the net effect of the
#                to-be-merged changes.
#
# Any positional arguments override the default package list (the replication
# and ndc packages).

set -euo pipefail

MIN_COVERAGE="${MIN_COVERAGE:-80}"
PACKAGES="${PACKAGES:-./service/history/replication/ ./service/history/ndc/}"
BASE_REF=""

positional=()
while [ $# -gt 0 ]; do
  case "$1" in
    --min)
      MIN_COVERAGE="$2"; shift 2 ;;
    --min=*)
      MIN_COVERAGE="${1#*=}"; shift ;;
    --base)
      BASE_REF="$2"; shift 2 ;;
    --base=*)
      BASE_REF="${1#*=}"; shift ;;
    --)
      shift; positional+=("$@"); break ;;
    -*)
      echo "ERROR: unknown flag: $1" >&2; exit 2 ;;
    *)
      positional+=("$1"); shift ;;
  esac
done
if [ "${#positional[@]}" -gt 0 ]; then
  PACKAGES="${positional[*]}"
fi

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

# measure_coverage <dir> <out_file>
# Writes "<package> <percent>" lines to <out_file>, running `go test` from <dir>.
# A package with no tests (e.g. a package that does not exist on the base
# revision) is recorded as 0.0 so it can never look like a regression.
measure_coverage() {
  local dir="$1" out_file="$2" pkg output pct
  : > "$out_file"
  for pkg in $PACKAGES; do
    if [ ! -d "$dir/${pkg#./}" ]; then
      echo "$pkg 0.0" >> "$out_file"
      continue
    fi
    output="$(cd "$dir" && go test -count=1 -cover "$pkg" 2>&1)"
    # "ok  pkg  1.2s  coverage: 87.9% of statements" -> 87.9
    pct="$(echo "$output" | grep -oE 'coverage: [0-9.]+%' | head -1 | grep -oE '[0-9.]+' || true)"
    if [ -z "$pct" ]; then
      # No coverage line: either no test files or a build/test failure.
      if echo "$output" | grep -q 'no test files'; then
        pct="0.0"
      else
        echo "ERROR: failed to measure coverage for $pkg in $dir:" >&2
        echo "$output" >&2
        exit 1
      fi
    fi
    echo "$pkg $pct" >> "$out_file"
  done
}

# get_pct <file> <pkg> -> percentage for pkg, or 0.0 if absent.
get_pct() {
  awk -v p="$2" '$1 == p { print $2 }' "$1" | head -1
}

head_file="$(mktemp)"
base_file="$(mktemp)"
trap 'rm -f "$head_file" "$base_file"' EXIT

echo "==> Measuring head coverage (min ${MIN_COVERAGE}%)"
measure_coverage "$REPO_ROOT" "$head_file"

have_base=false
if [ -n "$BASE_REF" ]; then
  worktree_dir="$(mktemp -d)"
  # shellcheck disable=SC2064
  trap "git -C '$REPO_ROOT' worktree remove --force '$worktree_dir' >/dev/null 2>&1 || true; rm -f '$head_file' '$base_file'" EXIT
  echo "==> Measuring base coverage at ${BASE_REF}"
  git -C "$REPO_ROOT" worktree add --quiet --detach "$worktree_dir" "$BASE_REF"
  measure_coverage "$worktree_dir" "$base_file"
  have_base=true
fi

failed=false
printf '\n%-45s %10s %10s %s\n' "PACKAGE" "HEAD" "BASE" "STATUS"
for pkg in $PACKAGES; do
  head_pct="$(get_pct "$head_file" "$pkg")"
  base_pct="0.0"
  $have_base && base_pct="$(get_pct "$base_file" "$pkg")"

  status="ok"
  if awk "BEGIN { exit !($head_pct < $MIN_COVERAGE) }"; then
    status="FAIL: below ${MIN_COVERAGE}% floor"
    failed=true
  elif $have_base && awk "BEGIN { exit !($head_pct < $base_pct) }"; then
    status="FAIL: decreased from ${base_pct}%"
    failed=true
  fi

  printf '%-45s %9s%% %9s%% %s\n' "$pkg" "$head_pct" "$base_pct" "$status"
done

if $failed; then
  echo
  echo "Coverage check failed. Add or restore unit tests so each package stays" >&2
  echo "at or above ${MIN_COVERAGE}% and does not regress against the base branch." >&2
  exit 1
fi

echo
echo "Coverage check passed."
