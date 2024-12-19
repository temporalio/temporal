#!/bin/sh

set -e

# Run getproto (go) in a loop until it successfully resolves all imports
while :; do
	out=$(go run ./cmd/tools/getproto "$@")
	ret=$?
	if [ "$out" != "<rerun>" ]; then
		exit $ret
	fi
done
