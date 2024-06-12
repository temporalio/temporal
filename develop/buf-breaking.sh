#!/usr/bin/env bash

set -eu -o pipefail

: "${MAKE:=make}"
: "${BUF:=buf}"
: "${API_BINPB:=proto/api.binpb}"
: "${INTERNAL_BINPB:=proto/image.bin}"

color()  { printf "\e[1;35m%s\e[0m\n" "$*" ; }
yellow() { printf "\e[1;33m%s\e[0m\n" "$*" ; }
red()    { printf "\e[1;31m%s\e[0m\n" "$*" ; }

if ! git diff-index --quiet HEAD --; then
  red "Commit all local changes before running buf-breaking"
  # Exit with success here.
  # Interactively: the user will see this and know what to do. In CI: this is run as
  # part of ci-build-misc, which has a "ensure-no-changes at the end". Fail there
  # instead of here since we can run more tests and the error message is nicer.
  exit
fi

# If invoked from the Makefile, this should already be done. This is just in
# case this is being run manually.
$MAKE "$INTERNAL_BINPB"

tmp=$(mktemp --tmpdir -d temporal-buf-breaking.XXXXXXXXX)
trap 'rm -rf $tmp' EXIT

color "Cloning repo to temp dir..."
git clone . "$tmp"

check_against_commit() {
  local commit=$1 name=$2
  color "Breaking check against $name:"
  git -C "$tmp" checkout --detach "$commit"
  if grep -q INTERNAL_BINPB "$tmp/Makefile"; then
    $MAKE -C "$tmp" "$INTERNAL_BINPB"
    $BUF breaking "$INTERNAL_BINPB" --against "$tmp/$INTERNAL_BINPB"
  else
    yellow "$name commit is too old to support breaking check"
  fi
}

# First check against parent commit:
check_against_commit HEAD^ "parent"

# Next check against main:
git -C "$tmp" fetch https://github.com/temporalio/temporal.git main
check_against_commit FETCH_HEAD "main"
