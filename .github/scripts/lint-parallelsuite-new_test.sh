#!/usr/bin/env bash
set -euo pipefail

filter=$(cd "$(dirname "$0")" && pwd)/lint-parallelsuite-new.sh
revgrep=$(command -v "${1:-revgrep}")
fixture=$(mktemp -d)
trap 'rm -rf "$fixture"' EXIT
cd "$fixture"

git init -q
git config user.name "Test"
git config user.email "test@example.com"
printf 'package fixture\n\nfunc existing() {}\n' > fixture.go
git add fixture.go
git commit -qm "Add existing code"
base=$(git rev-parse HEAD)
printf '\nfunc added() {}\n' >> fixture.go
git add fixture.go
git commit -qm "Add new code"

check() {
  local expected=$1 line=$2 status=$3
  local actual=0
  bash "$filter" "$revgrep" "$base" bash -c 'printf "%s\n" "$1"; exit "$2"' _ "$line" "$status" > output 2>&1 || actual=$?
  if [[ "$actual" != "$expected" ]]; then
    cat output >&2
    printf 'Expected exit %s, got %s\n' "$expected" "$actual" >&2
    exit 1
  fi
}

check 0 'fixture.go:3:1: existing diagnostic' 3
[[ ! -s output ]]
check 1 'fixture.go:5:1: new diagnostic' 3
grep -q 'new diagnostic' output
check 0 '' 0
check 1 'loading packages failed' 1
grep -q 'loading packages failed' output
check 1 'fixture.go:3:1: compile error in unchanged code' 1
grep -q 'compile error in unchanged code' output

printf 'Changed-line filtering and analyzer error propagation passed.\n'
