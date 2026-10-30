#!/bin/bash
# Generate RSA key pair and JWKS for local JWT testing
# This script is idempotent - it only generates keys if they don't exist

set -euo pipefail

KEY_DIR="/tmp/temporal-jwt-test"
PRIVATE_KEY="$KEY_DIR/private-key.pem"
JWKS_DIR="$KEY_DIR/.well-known"
JWKS_FILE="$JWKS_DIR/jwks.json"

mkdir -p "$JWKS_DIR"

# Generate private key if it doesn't exist
if [ ! -f "$PRIVATE_KEY" ]; then
    echo "Generating RSA private key..."
    openssl genrsa -out "$PRIVATE_KEY" 2048 2>/dev/null
    # Remove JWKS so it gets regenerated with the new key
    rm -f "$JWKS_FILE"
fi

# Generate JWKS from private key if it doesn't exist
if [ ! -f "$JWKS_FILE" ]; then
    echo "Generating JWKS from private key..."

    # Extract modulus in hex
    MODULUS_HEX=$(openssl rsa -in "$PRIVATE_KEY" -noout -modulus 2>/dev/null | cut -d= -f2)

    # Convert hex to binary, then to base64url
    N=$(echo "$MODULUS_HEX" | xxd -r -p | base64 | tr '+/' '-_' | tr -d '=' | tr -d '\n')

    # RSA public exponent is typically 65537 = 0x010001
    # In base64url: AQAB
    E="AQAB"

    cat > "$JWKS_FILE" << EOF
{
  "keys": [
    {
      "kty": "RSA",
      "kid": "test-key-1",
      "use": "sig",
      "alg": "RS256",
      "n": "$N",
      "e": "$E"
    }
  ]
}
EOF
    echo "Created $JWKS_FILE"
fi

echo "Keys ready:"
echo "  Private key: $PRIVATE_KEY"
echo "  JWKS file:   $JWKS_FILE"
