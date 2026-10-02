#!/bin/sh

set -e

# Run exportapidescriptors (go) in a loop until it successfully resolves all imports
while :; do
	out=$(go run ./cmd/tools/exportapidescriptors "$@")
	ret=$?
	if [ "$out" != "<rerun>" ]; then
		exit $ret
	fi
done
