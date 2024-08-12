#!/bin/sh

green() { printf "\e[1;32m%s\e[0m\n" "$*" ; }
red()   { printf "\e[1;31m%s\e[0m\n" "$*" ; }

binary=$1

if go tool objdump -S "$binary" | grep -C 2 go.temporal.io/server/common/assert; then
  red "Error: One or more assertion calls from 'go.temporal.io/server/common/assert' were found in the build."
  red "Please make sure that no assertion calls make it into the build (consult the 'assert' package documentation)."
  exit 1
else
  green "Verified that there are no assertion calls in the build."
  exit 0
fi