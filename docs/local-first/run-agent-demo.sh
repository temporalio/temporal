#!/usr/bin/env bash
set -euo pipefail

server_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
workspace_dir="${LOCAL_FIRST_WORKSPACE_DIR:-$(dirname "${server_dir}")}"
server_binary="$(mktemp /tmp/local-first-agent-demo-server.XXXXXX)"
ui_binary="$(mktemp /tmp/local-first-agent-demo-ui.XXXXXX)"

cleanup() {
  rm -f "${server_binary}"
  rm -f "${ui_binary}"
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
  GOCACHE=/tmp/local-first-cli-go-cache go build \
    -o "${ui_binary}" \
    ./cmd/local-first-demo-ui
)

(
  cd "${workspace_dir}/temporal-sdk-rust"
  LOCAL_FIRST_DEMO_SERVER="${server_binary}" \
    LOCAL_FIRST_DEMO_UI_SERVER="${ui_binary}" cargo run \
    -p temporalio-sdk-core \
    --example local-first-agent-demo \
    --features test-utilities \
    -- "$@"
)
