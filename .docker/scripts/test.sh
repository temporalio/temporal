#!/usr/bin/env bash

set -euo pipefail

IMAGE_SHA_TAG="${IMAGE_SHA_TAG:-sha-test}"
IMAGE_REPO="${IMAGE_REPO:-ghcr.io/chaptersix}"

echo "Testing images with tag: ${IMAGE_SHA_TAG}"

# Test server image
echo "Testing server image..."
docker run --rm "${IMAGE_REPO}/server:${IMAGE_SHA_TAG}" temporal-server --version

# Test admin-tools image
echo "Testing admin-tools image..."
docker run --rm "${IMAGE_REPO}/admin-tools:${IMAGE_SHA_TAG}" temporal --version
docker run --rm "${IMAGE_REPO}/admin-tools:${IMAGE_SHA_TAG}" tdbg --version
docker run --rm "${IMAGE_REPO}/admin-tools:${IMAGE_SHA_TAG}" temporal-cassandra-tool --version
docker run --rm "${IMAGE_REPO}/admin-tools:${IMAGE_SHA_TAG}" temporal-sql-tool --version

echo "✅ All image tests passed!"
