#!/bin/bash

set -euo pipefail

# Copy schema directory from the repo root
# Usage: ./copy-schema.sh

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
BUILD_DIR="${SCRIPT_DIR}/build"

echo "Copying schema directory..."

# Create build directory if it doesn't exist
mkdir -p "${BUILD_DIR}/temporal"

# Copy schema directory from repo root
cp -r "${REPO_ROOT}/schema" "${BUILD_DIR}/temporal/"

echo "Schema directory copied to ${BUILD_DIR}/temporal/schema"
