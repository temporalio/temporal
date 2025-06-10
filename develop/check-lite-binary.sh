#!/usr/bin/env bash
#
# Check that heavy dependencies are not present in the lite build binary
#

set -eu -o pipefail

binary_path='test_binary'

# Clean and build the lite binary
echo "Building binary with lite tag..."
#BUILD_TAG=lite make clean temporal-server
rm -f test_binary && go test -tags test_dep,lite -count=0 -c -o $binary_path ./tests

# Check if the binary exists
if [[ ! -f "$binary_path" ]]; then
    echo "Error: Binary not found at $binary_path" >&2
    echo "Please verify the correct path to the built binary" >&2
    exit 1
fi

echo "Checking binary for disallowed dependencies..."

# Run objdump once and store the output with preserved newlines
symbols="$(objdump -t "$binary_path" 2>/dev/null || {
    echo "Error: Unable to extract symbols from binary" >&2
    exit 1
})"

fail=0
dependencies=(
    'github.com/aws/aws-sdk-go'
    'cloud.google.com/go'
    'github.com/jackc/pgx'
)

for dep in "${dependencies[@]}"; do
    if grep -q "$dep" <<< "$symbols"; then
        echo "❌ Found disallowed dependency: $dep" >&2
        fail=1
    else
        echo "✅ Dependency not found: $dep"
    fi
done

if [[ $fail -eq 0 ]]; then
    echo "✅ All checks passed"
else
    echo "❌ Build verification failed - disallowed dependencies detected" >&2
fi

exit $fail
