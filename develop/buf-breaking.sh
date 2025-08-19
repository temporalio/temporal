#!/usr/bin/env bash
#
# This script is intended to be run from the Makefile by the `buf-breaking` target.
#
# This uses the buf tool (https://buf.build/) to check our proto definitions for invalid
# changes. Because of limitations of buf (it doesn't support using images for
# dependencies), we use it by building our protos into images and then calling buf to
# compare images, instead of using buf's built-in git support. That means we have to use
# git manually to collect the versions we want to compare.
#
# We run two breaking checks: this commit against its PR merge base, and also this commit
# against the current head of main. Checking against the merge base is the one that will
# find most breakage, but it doesn't handle the case where protos are evolved in
# compatible ways in different directions on a feature branch vs the main branch, that are
# not compatible with each other. That case would hopefully be detected at merge time, but
# it's better to detect it early during feature branch development. The check against main
# handles that.

set -eu -o pipefail

# These are passed in by the Makefile:
: "${MAKE:=make}"
: "${BUF:=buf}"
: "${API_BINPB:=proto/api.binpb}"
: "${INTERNAL_BINPB:=proto/image.bin}"
: "${CHASM_BINPB:=proto/chasm.bin}"
: "${MAIN_BRANCH:=main}"

: "${COMMIT:=$(git rev-parse HEAD)}"
: "${PR_BASE_COMMIT:=$(git merge-base HEAD main)}"

color()  { printf "\e[1;35m%s\e[0m\n" "$*" ; }
yellow() { printf "\e[1;33m%s\e[0m\n" "$*" ; }
red()    { printf "\e[1;31m%s\e[0m\n" "$*" ; }

if [[ -n "$(git status --porcelain --untracked-files=no)" ]]; then
  red "Commit all local changes before running buf-breaking"
  git status
  # Exit with success here.
  # Interactively: the user will see this and know what to do. In CI: this is run as
  # part of ci-build-misc, which has a "ensure-no-changes at the end". Fail there
  # instead of here since we can run more tests and the error message is nicer.
  exit 0
fi

# If invoked from the Makefile, this should already be done. This is just in
# case this is being run manually.
$MAKE "$INTERNAL_BINPB"
$MAKE "$CHASM_BINPB"

tmp=$(mktemp --tmpdir -d temporal-buf-breaking.XXXXXXXXX)
trap 'rm -rf $tmp' EXIT

check_against_commit() {
  local commit=$1 name=$2
  color "Breaking check against $name:"
  git -C "$tmp" checkout --detach "$commit"
  # Note that we're now in a different commit of this repo, so we're relying on different
  # versions of the Makefile to do what we expect given this make target. Check that it's
  # least new enough to handle this by looking for the string INTERNAL_BINPB.
  if grep -q INTERNAL_BINPB "$tmp/Makefile"; then
    $MAKE -C "$tmp" "$INTERNAL_BINPB"
    $BUF breaking "$INTERNAL_BINPB" --against "$tmp/$INTERNAL_BINPB" --config proto/internal/buf.yaml
  else
    yellow "$name commit is too old to support breaking check for internal protos"
  fi

  if grep -q CHASM_BINPB "$tmp/Makefile"; then
    $MAKE -C "$tmp" "$CHASM_BINPB"
    $BUF breaking "$CHASM_BINPB" --against "$tmp/$CHASM_BINPB" --config chasm/lib/buf.yaml
  else
    yellow "$name commit is too old to support breaking check for chasm protos"
  fi
}

if [[ "$PR_BASE_COMMIT" != "$COMMIT" ]]; then
  # We're running in GHA, using shallow clone. Fetch some commits from the PR
  # base so we can try to find the merge base.
  color "Fetching more commits from $PR_BASE_COMMIT..."
  git fetch --no-tags --no-recurse-submodules --depth=100 origin "$PR_BASE_COMMIT"

  color "Cloning repo to temp dir..."
  git clone . "$tmp"

  # First check against merge base commit:
  git -C "$tmp" fetch origin "$PR_BASE_COMMIT"
  if base=$(git -C "$tmp" merge-base HEAD "$PR_BASE_COMMIT"); then
    check_against_commit "$base" "merge base"
  else
    yellow "Can't find merge base for breaking check, checking against main only"
  fi

  # Next check against main:
  # git -C "$tmp" fetch --depth=1 https://github.com/temporalio/temporal.git "$MAIN_BRANCH"
  # check_against_commit FETCH_HEAD "main"
fi
