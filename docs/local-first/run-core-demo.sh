#!/usr/bin/env bash
set -euo pipefail

server_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
workspace_dir="${LOCAL_FIRST_WORKSPACE_DIR:-$(dirname "${server_dir}")}"
test_filter="${1:-local_first}"
server_binary="$(mktemp /tmp/local-first-demo-server.XXXXXX)"
cli_binary="$(mktemp /tmp/local-first-temporal-cli.XXXXXX)"

cleanup() {
  rm -f "${server_binary}"
  rm -f "${cli_binary}"
}
trap cleanup EXIT

(
  cd "${server_dir}"
  GOCACHE=/tmp/local-first-go-cache go build -tags test_dep \
    -o "${server_binary}" \
    ./temporaltest/cmd/local-first-demo-server
)

(
  cd "${workspace_dir}/temporal-cli"
  GOCACHE=/tmp/local-first-cli-go-cache go build -o "${cli_binary}" ./cmd/temporal
)

(
  cd "${workspace_dir}/temporal-sdk-rust"
  LOCAL_FIRST_DEMO_SERVER="${server_binary}" LOCAL_FIRST_TEMPORAL_CLI="${cli_binary}" \
    timeout 180 cargo integ-test \
    -t local_first_bridge_tests \
    --server-kind external \
    "${test_filter}" \
    -- --nocapture --test-threads=1
)
