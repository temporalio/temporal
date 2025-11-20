#!/bin/bash

set -euo pipefail

# Download binaries from GitHub release assets and build tdbg
# Usage: ./download-binaries.sh <server-version> <cli-version> <tctl-version> <arch>
# Example: ./download-binaries.sh 1.29.1 1.5.0 1.18.4 amd64

SERVER_VERSION="${1:?Server version required (e.g., 1.29.1)}"
CLI_VERSION="${2:?CLI version required (e.g., 1.5.0)}"
TCTL_VERSION="${3:?tctl version required (e.g., 1.18.4)}"
ARCH="${4:-amd64}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR="${SCRIPT_DIR}/build/${ARCH}"
TEMP_DIR="${SCRIPT_DIR}/build/temp"

echo "Downloading binaries for ${ARCH}..."
echo "  Server: ${SERVER_VERSION}"
echo "  CLI: ${CLI_VERSION}"
echo "  tctl: ${TCTL_VERSION}"

# Create build directory
mkdir -p "${BUILD_DIR}"

# Download Temporal server tools
echo "Downloading Temporal server tools..."
curl -fsSL "https://github.com/temporalio/temporal/releases/download/v${SERVER_VERSION}/temporal_${SERVER_VERSION}_linux_${ARCH}.tar.gz" \
  | tar -xz -C "${BUILD_DIR}" temporal-server temporal-cassandra-tool temporal-sql-tool

# Download temporal CLI
echo "Downloading temporal CLI..."
curl -fsSL "https://github.com/temporalio/cli/releases/download/v${CLI_VERSION}/temporal_cli_${CLI_VERSION}_linux_${ARCH}.tar.gz" \
  | tar -xz -C "${BUILD_DIR}" temporal

# Download tctl
echo "Downloading tctl..."
curl -fsSL "https://github.com/temporalio/tctl/releases/download/v${TCTL_VERSION}/tctl_${TCTL_VERSION}_linux_${ARCH}.tar.gz" \
  | tar -xz -C "${BUILD_DIR}" tctl tctl-authorization-plugin

# Build tdbg using GoReleaser
echo "Building tdbg from source..."
mkdir -p "${TEMP_DIR}"
cd "${TEMP_DIR}"

# Remove any existing temporal directory to ensure we get the correct version
rm -rf temporal

# Clone the temporal repo at the specific version
git clone --depth 1 --branch "v${SERVER_VERSION}" https://github.com/temporalio/temporal.git

cd temporal

# Build tdbg using Go
echo "Building tdbg for ${ARCH}..."
GOOS=linux GOARCH=${ARCH} CGO_ENABLED=0 go build -o "${BUILD_DIR}/tdbg" ./cmd/tools/tdbg

# Copy config template from the cloned temporal repo (version-specific)
if [ "${ARCH}" = "amd64" ]; then
  echo "Copying config template from temporal repo..."
  # config_template.yaml is only needed for legacy-server (dockerize templating)
  cp "${TEMP_DIR}/temporal/docker/config_template.yaml" "${SCRIPT_DIR}/build/config_template.yaml"
fi

# Clean up temp directory
cd "${SCRIPT_DIR}"
rm -rf "${TEMP_DIR}"

# Make binaries executable
chmod +x "${BUILD_DIR}"/*

echo "Done downloading and building binaries for ${ARCH}"
ls -lh "${BUILD_DIR}"
