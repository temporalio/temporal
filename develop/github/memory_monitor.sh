#!/bin/bash
#
# Memory Monitor
#
# 1. Snapshot status:
#       Samples memory every SNAPSHOT_INTERVAL_SECONDS and writes every sample
#       to SNAPSHOT_HISTORY_FILE. Logs status every
#       SNAPSHOT_PRINT_INTERVAL_SECONDS, records those lines in
#       SNAPSHOT_STATUS_HISTORY_FILE, and writes the highest-memory snapshot
#       report to SNAPSHOT_FILE.
#
# 2. Profile capture:
#       When usage crosses PROFILE_CAPTURE_THRESHOLD, captures a heap profile in
#       PROFILE_OUTPUT_DIR before running analysis, then repeats every
#       PROFILE_INTERVAL_SECONDS while usage remains above the threshold.
#
# 3. OOM prevention:
#       When usage crosses OOM_TERMINATION_THRESHOLD, reuses any previously
#       captured profile, writes the latest snapshot and a synthetic JUnit
#       report, then terminates the monitored test process group so post-test
#       artifact upload can still run. If no profile has been captured yet, it
#       captures one before terminating.
#
# Usage:
#   ./memory_monitor.sh [snapshot-file]
#
set -euo pipefail

if [[ $# -gt 1 ]]; then
  echo "Usage: $0 [snapshot-file]" >&2
  exit 1
fi

## Snapshot config.
MEMORY_OUTPUT_DIR="${MEMORY_OUTPUT_DIR:-.testoutput/memory}"
# Sample every second so short OOM ramps still leave history, but print less
# often to keep CI logs readable.
SNAPSHOT_INTERVAL_SECONDS="${SNAPSHOT_INTERVAL_SECONDS:-1}"
SNAPSHOT_PRINT_INTERVAL_SECONDS="${SNAPSHOT_PRINT_INTERVAL_SECONDS:-30}"
SNAPSHOT_FILE="${1:-${SNAPSHOT_FILE:-$MEMORY_OUTPUT_DIR/snapshot.txt}}"
SNAPSHOT_HISTORY_FILE="${SNAPSHOT_HISTORY_FILE:-$MEMORY_OUTPUT_DIR/snapshot-history.txt}"
SNAPSHOT_STATUS_HISTORY_FILE="${SNAPSHOT_STATUS_HISTORY_FILE:-$MEMORY_OUTPUT_DIR/snapshot-status-history.txt}"

## Profile config.
PROFILE_INTERVAL_SECONDS="${PROFILE_INTERVAL_SECONDS:-30}"
# Capture before the termination threshold so the diagnostic profile is usually
# available even if the runner kills the job before our termination path runs.
PROFILE_CAPTURE_THRESHOLD="${PROFILE_CAPTURE_THRESHOLD:-90}"
PROFILE_OUTPUT_DIR="${PROFILE_OUTPUT_DIR:-$MEMORY_OUTPUT_DIR/profile}"
PPROF_HOST="${PPROF_HOST:-localhost:7000}"

## OOM prevention config.
# Terminate late enough to avoid masking near-finished tests, but before the
# runner OOM killer skips post-test artifact upload.
OOM_TERMINATION_THRESHOLD="${OOM_TERMINATION_THRESHOLD:-98}"
OOM_JUNIT_FILE="${OOM_JUNIT_FILE:-.testoutput/junit.oom.xml}"

# State.
SNAPSHOT_HIGH_WATER_MARK=0
LAST_SNAPSHOT_PRINT_TIME=0
LAST_PROFILE_CAPTURE_TIME=0
WAS_ABOVE_PROFILE_CAPTURE_THRESHOLD=false
HAS_CAPTURED_PROFILE=false
OOM_TERMINATED=false

ensure_snapshot_dirs() {
  mkdir -p "$(dirname "$SNAPSHOT_FILE")" "$(dirname "$SNAPSHOT_HISTORY_FILE")" "$(dirname "$SNAPSHOT_STATUS_HISTORY_FILE")"
}

# Clear history on start
ensure_snapshot_dirs
: > "$SNAPSHOT_HISTORY_FILE"
: > "$SNAPSHOT_STATUS_HISTORY_FILE"

# Fetch a pprof profile and save to file
# Usage: fetch_pprof <profile_type> <output_file>
# Returns 0 on success, 1 on failure
fetch_pprof() {
  local profile_type="$1"
  local output_file="$2"
  curl -s --max-time 10 "http://${PPROF_HOST}/debug/pprof/${profile_type}" -o "$output_file" 2>/dev/null
}

# Print pprof top analysis.
# Usage: pprof_top <profile_file> <lines> [extra_flags...]
pprof_top() {
  local profile_file="$1"
  local lines="$2"
  shift 2

  go tool pprof -top "$@" "$profile_file" 2>/dev/null | head -"$lines" || true
}

# Print heap profile analysis.
print_heap() {
  local profile_file="$1"

  echo "--- Go Heap Profile ---"
  if [[ -s "$profile_file" ]]; then
    echo "=== inuse_space (what's currently held) ==="
    # Keep the artifact focused on retained memory; allocation totals are noisy
    # for this CI OOM investigation.
    pprof_top "$profile_file" 15 -inuse_space
  else
    echo "(heap profile not available)"
  fi
}

should_capture_profile() {
  local pct="$1"
  local now="$2"

  if [[ "$pct" -lt "$PROFILE_CAPTURE_THRESHOLD" ]]; then
    WAS_ABOVE_PROFILE_CAPTURE_THRESHOLD=false
    return 1
  fi
  if [[ "$WAS_ABOVE_PROFILE_CAPTURE_THRESHOLD" == "false" ]]; then
    WAS_ABOVE_PROFILE_CAPTURE_THRESHOLD=true
    return 0
  fi

  [[ $(( now - LAST_PROFILE_CAPTURE_TIME )) -ge "$PROFILE_INTERVAL_SECONDS" ]]
}

terminate_monitored_processes() {
  local pct="$1"

  if [[ -n "${MONITORED_PROCESS_GROUP:-}" ]]; then
    echo "Terminating monitored process group ${MONITORED_PROCESS_GROUP} at ${pct}% memory to preserve diagnostics artifacts."
    kill -TERM "-$MONITORED_PROCESS_GROUP" 2>/dev/null || true
    return
  fi

  echo "No monitored process group set at ${pct}% memory; leaving processes running."
}

write_oom_junit() {
  local pct="$1"

  mkdir -p "$(dirname "$OOM_JUNIT_FILE")"
  cat > "$OOM_JUNIT_FILE" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<testsuites tests="1" failures="1" errors="0" skipped="0" time="0">
  <testsuite name="memory_monitor" tests="1" failures="1" errors="0" skipped="0" time="0">
    <testcase classname="memory_monitor" name="OOM prevention" time="0">
      <failure type="OOM" message="OOM prevention threshold reached">Memory monitor terminated the test process at ${pct}% memory before the runner OOM kill. See memory diagnostics artifacts.</failure>
    </testcase>
  </testsuite>
</testsuites>
EOF
}

build_snapshot_report() {
  local pct="$1"

  cat <<EOF
Memory snapshot at $(date '+%Y-%m-%d %H:%M:%S') (usage ${pct}%)

$(cat "$SNAPSHOT_STATUS_HISTORY_FILE")

--- Top Processes ---
$(ps -eo pid,%mem,rss:10,comm --sort=-rss | head -10)

--- Memory Summary ---
$(free -m)
EOF
}

print_snapshot_status() {
  local now="$1"
  local line="$2"

  if [[ $(( now - LAST_SNAPSHOT_PRINT_TIME )) -lt "$SNAPSHOT_PRINT_INTERVAL_SECONDS" ]]; then
    return
  fi

  echo "$line"
  echo "$line" >> "$SNAPSHOT_STATUS_HISTORY_FILE"
  LAST_SNAPSHOT_PRINT_TIME="$now"
}

capture_profile() {
  local pct="$1"
  local pprof_output profile_prefix

  profile_prefix="$PROFILE_OUTPUT_DIR/$(date '+%Y%m%d-%H%M%S')-${pct}pct"
  mkdir -p "$(dirname "$profile_prefix")"
  fetch_pprof "heap" "${profile_prefix}.heap.pb.gz" || true
  pprof_output="$(print_heap "${profile_prefix}.heap.pb.gz")"

  cat <<EOF

$pprof_output
EOF
}

snapshot() {
  local memtotal_kb memavail_kb memused_kb memused_mb pct profile_report report should_terminate
  ensure_snapshot_dirs

  memtotal_kb="$(awk '/MemTotal/ {print $2}' /proc/meminfo)"
  memavail_kb="$(awk '/MemAvailable/ {print $2}' /proc/meminfo)"
  memused_kb=$(( memtotal_kb - memavail_kb ))
  memused_mb=$(( memused_kb / 1024 ))
  pct=$(( memused_kb * 100 / memtotal_kb ))

  # Get the top memory-heavy processes, format as "name (MB)".
  local top_procs
  top_procs="$(ps -eo rss,comm --sort=-rss | awk 'NR>1 && NR<=6 {printf "%s (%dMB), ", $2, $1/1024}' | sed 's/, $//')"

  local timestamp
  timestamp="$(date '+%H:%M:%S')"

  local status_line
  status_line="$(printf "%s used=%s%% mem=%sMB procs=[%s]" "$timestamp" "$pct" "$memused_mb" "$top_procs")"
  echo "$status_line" >> "$SNAPSHOT_HISTORY_FILE"

  local now
  now="$(date +%s)"
  print_snapshot_status "$now" "$status_line"

  profile_report=""
  should_terminate=false
  if [[ "$pct" -ge "$OOM_TERMINATION_THRESHOLD" ]] && [[ "$OOM_TERMINATED" == "false" ]]; then
    should_terminate=true
  fi

  if [[ "$should_terminate" == "true" && "$HAS_CAPTURED_PROFILE" == "true" ]]; then
    :
  elif should_capture_profile "$pct" "$now"; then
    LAST_PROFILE_CAPTURE_TIME="$now"
    profile_report="$(capture_profile "$pct")"
    HAS_CAPTURED_PROFILE=true
  elif [[ "$should_terminate" == "true" ]]; then
    LAST_PROFILE_CAPTURE_TIME="$now"
    profile_report="$(capture_profile "$pct")"
    HAS_CAPTURED_PROFILE=true
  fi

  report="$(build_snapshot_report "$pct")"
  report+="$profile_report"

  # Write the snapshot only at new memory highs so the final artifact represents
  # the worst observed point without emitting one file per sample.
  if [[ "$pct" -gt "$SNAPSHOT_HIGH_WATER_MARK" ]]; then
    echo "$report" > "$SNAPSHOT_FILE"
    SNAPSHOT_HIGH_WATER_MARK="$pct"
  fi

  if [[ "$should_terminate" == "true" ]]; then
    OOM_TERMINATED=true
    write_oom_junit "$pct"
    terminate_monitored_processes "$pct"
  fi
}

# Take snapshots until killed.
while true; do
  snapshot
  sleep "$SNAPSHOT_INTERVAL_SECONDS"
done
