#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SELECTOR="$SCRIPT_DIR/golangci-lint-targets.sh"
TEST_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/golangci-lint-targets.XXXXXX")"
trap 'rm -rf "$TEST_ROOT"' EXIT

new_repo() {
  local name="$1"
  local repo="$TEST_ROOT/$name"

  mkdir -p "$repo/pkg/a" "$repo/pkg/b" "$repo/.github"
  git init -q "$repo"
  git -C "$repo" config user.name Test
  git -C "$repo" config user.email test@example.com
  printf 'module example.com/test\n\ngo 1.23.0\n' >"$repo/go.mod"
  printf 'version: "2"\n' >"$repo/.github/.golangci.yml"
  printf 'package a\n' >"$repo/pkg/a/a.go"
  printf 'package a\n' >"$repo/pkg/a/other.go"
  printf 'package b\n' >"$repo/pkg/b/b.go"
  git -C "$repo" add .
  git -C "$repo" commit -qm baseline
  printf '%s\n' "$repo"
}

assert_targets() {
  local want="$1"
  local repo="$2"
  local got

  got="$(cd "$repo" && "$SELECTOR" HEAD)"
  if [[ "$got" != "$want" ]]; then
    printf 'want targets %q, got %q\n' "$want" "$got" >&2
    exit 1
  fi
}

test_returns_no_targets_for_no_go_changes() {
  local repo
  repo="$(new_repo no-changes)"
  assert_targets "" "$repo"
}

test_returns_unique_changed_package_targets() {
  local repo
  repo="$(new_repo changed-packages)"
  printf '\nfunc Changed() {}\n' >>"$repo/pkg/a/a.go"
  printf '\nfunc AlsoChanged() {}\n' >>"$repo/pkg/a/other.go"
  printf 'package b\n' >"$repo/pkg/b/untracked.go"
  assert_targets "./pkg/a ./pkg/b" "$repo"
}

test_falls_back_for_module_or_linter_config_changes() {
  local repo
  repo="$(new_repo module-change)"
  printf '\n// changed\n' >>"$repo/go.mod"
  assert_targets "./..." "$repo"

  repo="$(new_repo config-change)"
  printf '\n# changed\n' >>"$repo/.github/.golangci.yml"
  assert_targets "./..." "$repo"
}

test_falls_back_for_deleted_or_renamed_go_files() {
  local repo
  repo="$(new_repo deleted-file)"
  git -C "$repo" rm -q pkg/a/a.go
  assert_targets "./..." "$repo"

  repo="$(new_repo renamed-file)"
  git -C "$repo" mv pkg/a/a.go pkg/b/moved.go
  assert_targets "./..." "$repo"
}

test_returns_no_targets_for_no_go_changes
test_returns_unique_changed_package_targets
test_falls_back_for_module_or_linter_config_changes
test_falls_back_for_deleted_or_renamed_go_files
