#!/usr/bin/env bash

set -euo pipefail

cell=""
namespace=""
sample_size="3"
max_runs="1"
history_dir=""
address=""
sample_seed="schedule-v2-replay"

usage() {
  echo "Usage: $0 --cell CELL --history-dir DIR [--namespace NAME] [--sample-size N] [--max-runs N] [--sample-seed SEED] [--address HOST:PORT]"
}

while (($# > 0)); do
  case "$1" in
    --cell)
      cell="$2"
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
    --max-runs)
      max_runs="$2"
      shift 2
      ;;
    --sample-seed)
      sample_seed="$2"
      shift 2
      ;;
    --history-dir)
      history_dir="$2"
      shift 2
      ;;
    --address)
      address="$2"
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

if [[ -z "$cell" || -z "$history_dir" ]]; then
  usage >&2
  exit 2
fi
for value in "$sample_size" "$max_runs"; do
  if [[ ! "$value" =~ ^[1-9][0-9]*$ ]]; then
    echo "--sample-size and --max-runs must be positive integers" >&2
    exit 2
  fi
done
for dependency in ct jq gzip shasum; do
  if ! command -v "$dependency" >/dev/null 2>&1; then
    echo "Missing required command: $dependency" >&2
    exit 1
  fi
done

umask 077
mkdir -p "$history_dir"
chmod 0700 "$history_dir"
history_dir="$(cd "$history_dir" && pwd)"
index="$history_dir/collection.tsv"
if [[ ! -e "$index" ]]; then
  printf 'namespace\tschedule_id\trun_index\thistory\n' >"$index"
fi

remote_temporal() {
  local args=(admintools --context "$cell" -- temporal)
  if [[ -n "$address" ]]; then
    args+=(--address "$address")
  fi
  ct "${args[@]}" "$@" --color never --output json
}

opaque_id() {
  printf '%s' "$1" | shasum -a 256 | awk '{print $1}'
}

json_items() {
  local collection="$1"
  jq -c --arg collection "$collection" '
    if type == "array" then .[]
    elif .[$collection] != null then .[$collection][]
    elif .items != null then .items[]
    else empty
    end
  '
}

list_namespaces() {
  if [[ -n "$namespace" ]]; then
    printf '%s\n' "$namespace"
    return
  fi
  remote_temporal operator namespace list |
    json_items namespaces |
    jq -r '.namespaceInfo.name // .namespace_info.name // .name // .namespace // empty'
}

list_schedules() {
  local namespace_name="$1"
  remote_temporal schedule list --namespace "$namespace_name" |
    json_items schedules |
    jq -r '.scheduleId // .schedule_id // .id // empty'
}

sample_schedules() {
  local namespace_name="$1"
  while IFS= read -r schedule_id; do
    [[ -n "$schedule_id" ]] || continue
    printf '%s\t%s\n' "$(opaque_id "$sample_seed"$'\0'"$namespace_name"$'\0'"$schedule_id")" "$schedule_id"
  done < <(list_schedules "$namespace_name") | sort | sed -n "1,${sample_size}p" | cut -f2-
}

history_events() {
  jq -c 'if .events != null then .events elif .history.events != null then .history.events else empty end'
}

download_schedule() {
  local namespace_name="$1"
  local schedule_id="$2"
  local namespace_hash schedule_hash workflow_id run_id run_index namespace_dir output temporary previous_run
  namespace_hash="$(opaque_id "$namespace_name")"
  schedule_hash="$(opaque_id "$schedule_id")"
  namespace_dir="$history_dir/$namespace_hash"
  workflow_id="temporal-sys-scheduler:$schedule_id"
  run_id=""
  mkdir -p "$namespace_dir"
  chmod 0700 "$namespace_dir"

  for ((run_index = 0; run_index < max_runs; run_index++)); do
    output="$namespace_dir/$schedule_hash-run$run_index.json.gz"
    if [[ -e "$output" ]]; then
      break
    fi
    temporary="$(mktemp "$namespace_dir/.history.XXXXXX.json")"
    local command=(workflow show --namespace "$namespace_name" --workflow-id "$workflow_id")
    if [[ -n "$run_id" ]]; then
      command+=(--run-id "$run_id")
    fi
    if ! remote_temporal "${command[@]}" >"$temporary"; then
      rm -f "$temporary"
      return 0
    fi
    if ! history_events <"$temporary" | jq -e 'length > 0' >/dev/null; then
      echo "Skipping invalid or empty history: namespace=$namespace_name schedule=$schedule_id" >&2
      rm -f "$temporary"
      return 0
    fi
    previous_run="$(history_events <"$temporary" | jq -r '.[0].workflowExecutionStartedEventAttributes.continuedExecutionRunId // .[0].workflow_execution_started_event_attributes.continued_execution_run_id // empty')"
    history_events <"$temporary" | jq '{events: .}' | gzip -9 >"$output"
    chmod 0600 "$output"
    rm -f "$temporary"
    printf '%s\t%s\t%d\t%s\n' "$namespace_name" "$schedule_id" "$run_index" "${output#"$history_dir/"}" >>"$index"
    [[ -n "$previous_run" ]] || break
    run_id="$previous_run"
  done
}

while IFS= read -r namespace_name; do
  [[ -n "$namespace_name" ]] || continue
  selected=0
  while IFS= read -r schedule_id; do
    [[ -n "$schedule_id" ]] || continue
    download_schedule "$namespace_name" "$schedule_id"
    selected=$((selected + 1))
  done < <(sample_schedules "$namespace_name")
  printf 'COLLECTION_SUMMARY namespace=%q selected=%d\n' "$namespace_name" "$selected"
done < <(list_namespaces)

echo "Histories saved under $history_dir"
echo "Replay with: cmd/tools/schedule-v2-replay/replay-sample.sh --replay-only --history-dir $history_dir --report $history_dir/report.json --fail-on significant"
