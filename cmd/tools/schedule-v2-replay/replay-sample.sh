#!/usr/bin/env bash

set -u -o pipefail

address="localhost:7233"
namespace=""
sample_size="10"
history_dir="schedule-v1-replay-histories"
timeout="1m"
tls="false"
tls_server_name=""

usage() {
  echo "Usage: $0 [--namespace NAME] [--sample-size N] [--history-dir DIR] [--address HOST:PORT] [--timeout DURATION] [--tls] [--tls-server-name NAME]"
}

while (($# > 0)); do
  case "$1" in
    --address)
      address="$2"
      shift 2
      ;;
    --namespace)
      namespace="$2"
      shift 2
      ;;
    --sample-size)
      sample_size="$2"
      shift 2
      ;;
    --history-dir)
      history_dir="$2"
      shift 2
      ;;
    --timeout)
      timeout="$2"
      shift 2
      ;;
    --tls)
      tls="true"
      shift
      ;;
    --tls-server-name)
      tls_server_name="$2"
      shift 2
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/../../.." && pwd)"
args=(
  go run ./cmd/tools/schedule-v2-replay
  -batch
  -address "$address"
  -sample-size "$sample_size"
  -history-dir "$history_dir"
  -timeout "$timeout"
)

if [[ -n "$namespace" ]]; then
  args+=(-namespace "$namespace")
else
  args+=(-all-namespaces)
fi
if [[ "$tls" == "true" ]]; then
  args+=(-tls)
fi
if [[ -n "$tls_server_name" ]]; then
  args+=(-tls-server-name "$tls_server_name")
fi

cd "$repo_root" || exit 1
if ! "${args[@]}"; then
  exit 1
fi

export SCHEDULE_V1_HISTORY_DIR="$history_dir"
exec go test -tags test_dep ./chasm/lib/scheduler \
  -run '^TestDownloadedV1HistoriesAgainstCHASM$' \
  -count=1
