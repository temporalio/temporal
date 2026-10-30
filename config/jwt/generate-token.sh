#!/bin/bash
# Generate a test JWT signed with the local private key
# Usage: ./generate-token.sh [subject] [permissions...]
#
# Examples:
#   ./generate-token.sh                                    # Default: test-user@example.com with system:admin
#   ./generate-token.sh alice@company.com                  # Custom subject with system:admin
#   ./generate-token.sh alice@company.com default:admin    # Custom subject with namespace permission
#   ./generate-token.sh alice@company.com system:admin default:writer  # Multiple permissions
#
# The generated JWT has the following structure:
#   Header: {"alg": "RS256", "typ": "JWT", "kid": "test-key-1"}
#   Payload: {"sub": "<subject>", "permissions": ["<perm1>", ...], "iat": <now>, "exp": <now+1h>}

set -euo pipefail

base64url() { base64 | tr -d '\n' | tr '+/' '-_' | tr -d '='; }

KEY_DIR="/tmp/temporal-jwt-test"
PRIVATE_KEY="$KEY_DIR/private-key.pem"

if [ ! -f "$PRIVATE_KEY" ]; then
    echo "Private key not found at $PRIVATE_KEY, running setup-keys.sh..." >&2
    "$(dirname "$0")/setup-keys.sh"
fi

if [ ! -f "$PRIVATE_KEY" ]; then
    echo "Error: Private key still not found at $PRIVATE_KEY" >&2
    exit 1
fi

SUBJECT="${1:-test-user@example.com}"
shift 2>/dev/null || true

# Collect permissions (default to system:admin if none specified)
if [ $# -eq 0 ]; then
    PERMISSIONS='["system:admin"]'
else
    PERMISSIONS=$(printf '%s\n' "$@" | jq -R . | jq -s .)
fi

NOW=$(date +%s)
EXP=$((NOW + 3600))

HEADER='{"alg":"RS256","typ":"JWT","kid":"test-key-1"}'
HEADER_B64=$(echo -n "$HEADER" | base64url)

PAYLOAD=$(jq -n -c --arg sub "$SUBJECT" --argjson perms "$PERMISSIONS" \
    --argjson iat "$NOW" --argjson exp "$EXP" \
    '{sub: $sub, permissions: $perms, iat: $iat, exp: $exp}')
PAYLOAD_B64=$(echo -n "$PAYLOAD" | base64url)

SIGNATURE=$(echo -n "${HEADER_B64}.${PAYLOAD_B64}" | \
    openssl dgst -sha256 -sign "$PRIVATE_KEY" | \
    base64url)

# Output the complete JWT
echo "${HEADER_B64}.${PAYLOAD_B64}.${SIGNATURE}"
