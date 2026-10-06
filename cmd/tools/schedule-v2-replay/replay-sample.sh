#!/usr/bin/env bash

set -u -o pipefail

address="localhost:7233"
namespace=""
sample_size="10"
cohort_size="2"
sample_seed="schedule-v2-replay"
max_runs="3"
max_run_age="720h"
max_scan="0"
max_history_events="100000"
max_history_bytes="52428800"
max_replay_deadlines="10000"
max_replay_tasks="100000"
max_replay_starts="10000"
requests_per_second="2"
concurrency="2"
resume="true"
history_dir="schedule-v1-replay-histories"
timeout="1m"
replay_timeout="30m"
checkpoint_every="250"
tls="false"
tls_server_name=""
report="schedule-v2-replay-report.json"
fail_on="significant"
mode="both"
redact="true"
sensitive_data_ack="false"

usage() {
  echo "Usage: $0 [--collect-only|--replay-only] [--acknowledge-sensitive-data] [--namespace NAME] [--sample-size N] [--cohort-size N] [--sample-seed SEED] [--max-runs N] [--max-run-age DURATION] [--max-scan N] [--max-replay-deadlines N] [--max-replay-tasks N] [--max-replay-starts N] [--checkpoint-every N] [--replay-timeout DURATION] [--requests-per-second N] [--concurrency N] [--history-dir DIR] [--report FILE] [--unredacted-report] [--fail-on significant|all|none] [--address HOST:PORT] [--timeout DURATION] [--tls] [--tls-server-name NAME]"
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
    --cohort-size)
      cohort_size="$2"
      shift 2
      ;;
    --sample-seed)
      sample_seed="$2"
      shift 2
      ;;
    --max-runs)
      max_runs="$2"
      shift 2
      ;;
    --max-run-age)
      max_run_age="$2"
      shift 2
      ;;
    --max-scan)
      max_scan="$2"
      shift 2
      ;;
    --max-history-events)
      max_history_events="$2"
      shift 2
      ;;
    --max-history-bytes)
      max_history_bytes="$2"
      shift 2
      ;;
    --max-replay-deadlines)
      max_replay_deadlines="$2"
      shift 2
      ;;
    --max-replay-tasks)
      max_replay_tasks="$2"
      shift 2
      ;;
    --max-replay-starts)
      max_replay_starts="$2"
      shift 2
      ;;
    --checkpoint-every)
      checkpoint_every="$2"
      shift 2
      ;;
    --replay-timeout)
      replay_timeout="$2"
      shift 2
      ;;
    --requests-per-second)
      requests_per_second="$2"
      shift 2
      ;;
    --concurrency)
      concurrency="$2"
      shift 2
      ;;
    --no-resume)
      resume="false"
      shift
      ;;
    --collect-only)
      if [[ "$mode" != "both" ]]; then
        echo "--collect-only and --replay-only are mutually exclusive" >&2
        exit 2
      fi
      mode="collect"
      shift
      ;;
    --replay-only)
      if [[ "$mode" != "both" ]]; then
        echo "--collect-only and --replay-only are mutually exclusive" >&2
        exit 2
      fi
      mode="replay"
      shift
      ;;
    --history-dir)
      history_dir="$2"
      shift 2
      ;;
    --timeout)
      timeout="$2"
      shift 2
      ;;
    --report)
      report="$2"
      shift 2
      ;;
    --fail-on)
      fail_on="$2"
      shift 2
      ;;
    --unredacted-report)
      redact="false"
      shift
      ;;
    --acknowledge-sensitive-data)
      sensitive_data_ack="true"
      shift
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

case "$fail_on" in
  significant|all|none) ;;
  *)
    echo "Invalid --fail-on value: $fail_on" >&2
    exit 2
    ;;
esac
for replay_limit in "$max_replay_deadlines" "$max_replay_tasks" "$max_replay_starts"; do
  if [[ ! "$replay_limit" =~ ^[1-9][0-9]*$ ]]; then
    echo "Replay limits must be positive integers" >&2
    exit 2
  fi
done
if [[ "$mode" != "replay" && "$sensitive_data_ack" != "true" ]]; then
  echo "--acknowledge-sensitive-data is required when collecting production histories" >&2
  exit 2
fi

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/../../.." && pwd)"
collector_version="$(git -C "$repo_root" rev-parse HEAD 2>/dev/null || echo unknown)"
args=(
  go run ./cmd/tools/schedule-v2-replay
  -batch
  -address "$address"
  -sample-size "$sample_size"
  -cohort-size "$cohort_size"
  -sample-seed "$sample_seed"
  -max-runs "$max_runs"
  -max-run-age "$max_run_age"
  -max-scan "$max_scan"
  -max-history-events "$max_history_events"
  -max-history-bytes "$max_history_bytes"
  -requests-per-second "$requests_per_second"
  -concurrency "$concurrency"
  -collector-version "$collector_version"
  -resume="$resume"
  -history-dir "$history_dir"
  -timeout "$timeout"
)
if [[ "$sensitive_data_ack" == "true" ]]; then
  args+=(-acknowledge-sensitive-data)
fi

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
collection_failed="false"
if [[ "$mode" != "replay" ]]; then
  if ! "${args[@]}"; then
    collection_failed="true"
  fi
fi
if [[ "$mode" == "collect" ]]; then
  [[ "$collection_failed" == "false" ]]
  exit $?
fi

history_test_dir="$history_dir"
report_path="$report"
if [[ "$history_test_dir" != /* ]]; then
  history_test_dir="$repo_root/$history_test_dir"
fi
if [[ "$report_path" != /* ]]; then
  report_path="$repo_root/$report_path"
fi
export SCHEDULE_V1_HISTORY_DIR="$history_test_dir"
export SCHEDULE_V2_REPLAY_REPORT="$report_path"
export SCHEDULE_V2_REPLAY_FAIL_ON="$fail_on"
export SCHEDULE_V2_REPLAY_REDACT="$redact"
export SCHEDULE_V2_REPLAY_MAX_DEADLINES="$max_replay_deadlines"
export SCHEDULE_V2_REPLAY_MAX_TASKS="$max_replay_tasks"
export SCHEDULE_V2_REPLAY_MAX_STARTS="$max_replay_starts"
export SCHEDULE_V2_REPLAY_CHECKPOINT_EVERY="$checkpoint_every"
replay_failed="false"
if ! go test -tags test_dep ./chasm/lib/scheduler \
  -run '^TestDownloadedV1HistoriesAgainstCHASM$' \
  -count=1 -timeout "$replay_timeout"; then
  replay_failed="true"
fi
if [[ "$collection_failed" == "true" || "$replay_failed" == "true" ]]; then
  exit 1
fi
