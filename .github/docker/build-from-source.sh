#!/bin/bash

set -euo pipefail

# Build binaries from source by cloning temporal repo at specific SHA using goreleaser
# Usage: ./build-from-source.sh <cli-version> <arch> [temporal-sha]
# Example: ./build-from-source.sh 1.5.0 amd64
# Example: ./build-from-source.sh 1.5.0 amd64 abc123def456

CLI_VERSION="${1:?CLI version required (e.g., 1.5.0)}"
ARCH="${2:-amd64}"
# Default to latest commit on main if not specified
TEMPORAL_SHA="${3:-$(git ls-remote https://github.com/temporalio/temporal.git HEAD | awk '{print $1}')}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR="${SCRIPT_DIR}/build/${ARCH}"
TEMP_DIR="${SCRIPT_DIR}/build/temp"
TEMPORAL_CLONE_DIR="${SCRIPT_DIR}/build/temporal"

echo "Building binaries from source for ${ARCH}..."
echo "  Temporal SHA: ${TEMPORAL_SHA}"
echo "  CLI version: ${CLI_VERSION}"

# Create build directories
mkdir -p "${BUILD_DIR}"
mkdir -p "${TEMP_DIR}"

# Clone temporal repo at specific SHA (only once, not per architecture)
if [ ! -d "${TEMPORAL_CLONE_DIR}/.git" ]; then
  echo "Cloning temporal repository at SHA ${TEMPORAL_SHA}..."
  rm -rf "${TEMPORAL_CLONE_DIR}"
  git clone https://github.com/temporalio/temporal.git "${TEMPORAL_CLONE_DIR}"
  cd "${TEMPORAL_CLONE_DIR}"
  git checkout "${TEMPORAL_SHA}"
else
  echo "Using existing temporal clone at ${TEMPORAL_CLONE_DIR}"
  cd "${TEMPORAL_CLONE_DIR}"
  # Ensure we're at the correct SHA
  CURRENT_SHA=$(git rev-parse HEAD)
  if [ "${CURRENT_SHA}" != "${TEMPORAL_SHA}" ]; then
    echo "Updating temporal clone to SHA ${TEMPORAL_SHA}..."
    git fetch origin
    git checkout "${TEMPORAL_SHA}"
  fi
fi

# Verify elasticsearch tool exists
if [ ! -d "./cmd/tools/elasticsearch" ]; then
  echo "Error: temporal-elasticsearch-tool source not found at ./cmd/tools/elasticsearch"
  exit 1
fi

# Build temporal server binaries individually using goreleaser
echo "Building temporal binaries with goreleaser for linux/${ARCH}..."
# Use goreleaser v1 if available in /tmp, otherwise use system goreleaser
GORELEASER_BIN="goreleaser"
if [ -f "/tmp/goreleaser" ]; then
  GORELEASER_BIN="/tmp/goreleaser"
fi

# Build each binary individually with --single-target and --id
# Set GOOS and GOARCH to ensure we build for Linux
echo "Building temporal-server..."
GOOS=linux GOARCH=${ARCH} ${GORELEASER_BIN} build --single-target --id temporal-server --output "${BUILD_DIR}/temporal-server" --snapshot --clean

echo "Building temporal-cassandra-tool..."
GOOS=linux GOARCH=${ARCH} ${GORELEASER_BIN} build --single-target --id temporal-cassandra-tool --output "${BUILD_DIR}/temporal-cassandra-tool" --snapshot --clean

echo "Building temporal-sql-tool..."
GOOS=linux GOARCH=${ARCH} ${GORELEASER_BIN} build --single-target --id temporal-sql-tool --output "${BUILD_DIR}/temporal-sql-tool" --snapshot --clean

echo "Building temporal-elasticsearch-tool..."
GOOS=linux GOARCH=${ARCH} ${GORELEASER_BIN} build --single-target --id temporal-elasticsearch-tool --output "${BUILD_DIR}/temporal-elasticsearch-tool" --snapshot --clean

echo "Building tdbg..."
GOOS=linux GOARCH=${ARCH} ${GORELEASER_BIN} build --single-target --id tdbg --output "${BUILD_DIR}/tdbg" --snapshot --clean

# Download temporal CLI from releases (separate repository)
echo "Downloading temporal CLI..."
curl -fsSL "https://github.com/temporalio/cli/releases/download/v${CLI_VERSION}/temporal_cli_${CLI_VERSION}_linux_${ARCH}.tar.gz" \
  | tar -xz -C "${BUILD_DIR}" temporal

# Schema is already available in the cloned repo at build/temporal/schema
# The Dockerfile expects it at ./build/temporal/schema which is where we cloned it
echo "Schema directory available at ${TEMPORAL_CLONE_DIR}/schema"


echo "Done building binaries for ${ARCH}"
ls -lh "${BUILD_DIR}"
