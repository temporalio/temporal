# Local JWT Development Setup

This document describes how to run Temporal server locally with JWT authentication enabled for test and development purposes.

## Overview

The setup uses:
- RSA key pair for signing/verifying JWTs
- JWKS file loaded directly from disk via `file://` URI
- Custom config file `development-jwt.yaml` with authorization enabled

## Files

- `config/development-jwt.yaml` - Server config with JWT auth enabled
- `config/jwt/setup-keys.sh` - Generates RSA key pair and JWKS
- `config/jwt/generate-token.sh` - Helper script to generate test JWTs

**Generated files** (created by `setup-keys.sh` in `/tmp/temporal-jwt-test/`):

- `/tmp/temporal-jwt-test/private-key.pem` - RSA private key for signing test JWTs
- `/tmp/temporal-jwt-test/.well-known/jwks.json` - JWKS file with RSA public key

## Makefile Targets

- `make start-jwt` - Start Temporal with JWT auth (no `--allow-no-auth` flag)

## Usage

### 1. Start Temporal with JWT Auth

```bash
make start-jwt
```

Note: Unlike other `start-*` targets, this does NOT use the `--allow-no-auth` flag, so authentication is enforced.

### 2. Generate Test JWTs

```bash
# Default: test-user@example.com with system:admin
./config/jwt/generate-token.sh

# Custom subject with system:admin
./config/jwt/generate-token.sh alice@company.com

# Custom subject with namespace permission
./config/jwt/generate-token.sh alice@company.com default:admin

# Multiple permissions
./config/jwt/generate-token.sh alice@company.com system:admin default:writer
```

### 3. Use the Token

```bash
TOKEN=$(./config/jwt/generate-token.sh alice@example.com default:admin)
temporal --tls=false --api-key "$TOKEN" workflow list
```

