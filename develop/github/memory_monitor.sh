#!/bin/bash
#
# Memory Monitor
#
# 1. Snapshot status:
#       Samples memory every SNAPSHOT_INTERVAL_SECONDS and writes every sample
#       to SNAPSHOT_HISTORY_FILE. Logs status every SNAPSHOT_PRINT_INTERVAL_SECONDS
#       and writes the highest-memory snapshot report to SNAPSHOT_FILE.
#
# 2. Profile capture:
#       When usage crosses HEAP_PROFILE_CAPTURE_THRESHOLD, captures a heap
#       profile in HEAP_PROFILES_DIR before running analysis.
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

# Snapshot config.
SNAPSHOT_DIR="${SNAPSHOT_DIR:-.testoutput/memory}"
# Sample every second so short OOM ramps still leave history, but print less
# often to keep CI logs readable.
SNAPSHOT_INTERVAL_SECONDS="${SNAPSHOT_INTERVAL_SECONDS:-1}"
SNAPSHOT_PRINT_INTERVAL_SECONDS="${SNAPSHOT_PRINT_INTERVAL_SECONDS:-30}"
SNAPSHOT_FILE="${1:-${SNAPSHOT_FILE:-$SNAPSHOT_DIR/memory-snapshot.txt}}"
SNAPSHOT_HISTORY_FILE="${SNAPSHOT_HISTORY_FILE:-$SNAPSHOT_DIR/memory-history.txt}"

# Heap profile config.
# Capture before the termination threshold so the diagnostic profile is usually
# available even if the runner kills the job before our termination path runs.
HEAP_PROFILE_CAPTURE_THRESHOLD="${HEAP_PROFILE_CAPTURE_THRESHOLD:-90}"
HEAP_PROFILES_DIR="${HEAP_PROFILES_DIR:-$SNAPSHOT_DIR/heap-profiles}"
PPROF_HOST="${PPROF_HOST:-localhost:7000}"

# OOM prevention config.
# Terminate late enough to avoid masking near-finished tests, but before the
# runner OOM killer skips post-test artifact upload.
OOM_TERMINATION_THRESHOLD="${OOM_TERMINATION_THRESHOLD:-98}"
OOM_JUNIT_FILE="${OOM_JUNIT_FILE:-.testoutput/junit.oom.xml}"

# State.
MEMORY_HIGH_WATER_MARK=0
LAST_SNAPSHOT_PRINT_TIME=0
HAS_CAPTURED_HEAP_PROFILE=false
OOM_TERMINATED=false

ensure_snapshot_dirs() {
  mkdir -p "$(dirname "$SNAPSHOT_FILE")" "$(dirname "$SNAPSHOT_HISTORY_FILE")"
}

# Clear history on start
ensure_snapshot_dirs
: > "$SNAPSHOT_HISTORY_FILE"

# Fetch a pprof profile and save to file
# Usage: fetch_pprof <pprof_profile> <output_file>
# Returns 0 on success, 1 on failure
fetch_pprof() {
  local pprof_profile="$1"
  local output_file="$2"
  curl -s --max-time 10 "http://${PPROF_HOST}/debug/pprof/${pprof_profile}" -o "$output_file" 2>/dev/null
}

# Print pprof top analysis.
# Usage: pprof_top <heap_profile_file> <lines> [extra_flags...]
pprof_top() {
  local heap_profile_file="$1"
  local lines="$2"
  shift 2

  go tool pprof -top "$@" "$heap_profile_file" 2>/dev/null | head -"$lines" || true
}

# Print heap profile analysis.
print_heap_profile_summary() {
  local heap_profile_file="$1"

  echo "--- Go Heap Profile ---"
  if [[ -s "$heap_profile_file" ]]; then
    echo "=== inuse_space (what's currently held) ==="
    # Keep the artifact focused on retained memory; allocation totals are noisy
    # for this CI OOM investigation.
    pprof_top "$heap_profile_file" 15 -inuse_space
  else
    echo "(heap profile not available)"
  fi
}

terminate_monitored_processes() {
  local memory_pct="$1"

  if [[ -n "${MONITORED_PROCESS_GROUP:-}" ]]; then
    echo "Terminating monitored process group ${MONITORED_PROCESS_GROUP} at ${memory_pct}% memory to preserve diagnostics artifacts."
    kill -TERM "-$MONITORED_PROCESS_GROUP" 2>/dev/null || true
    return
  fi

  echo "No monitored process group set at ${memory_pct}% memory; leaving processes running."
}

write_oom_junit() {
  local memory_pct="$1"

  mkdir -p "$(dirname "$OOM_JUNIT_FILE")"
  cat > "$OOM_JUNIT_FILE" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<testsuites tests="1" failures="1" errors="0" skipped="0" time="0">
  <testsuite name="memory_monitor" tests="1" failures="1" errors="0" skipped="0" time="0">
    <testcase classname="memory_monitor" name="OOM prevention" time="0">
      <failure type="OOM" message="OOM prevention threshold reached">Memory monitor terminated the test process at ${memory_pct}% memory before the runner OOM kill. See memory diagnostics artifacts.</failure>
    </testcase>
  </testsuite>
</testsuites>
EOF
}

write_snapshot_report() {
  local memory_pct="$1"
  local optional_heap_profile_section="$2"

  cat > "$SNAPSHOT_FILE" <<EOF
Memory snapshot at $(date '+%Y-%m-%d %H:%M:%S') (usage ${memory_pct}%)

$(tail -120 "$SNAPSHOT_HISTORY_FILE")

--- Top Processes ---
$(ps -eo pid,%mem,rss:10,comm --sort=-rss | head -10)

--- Memory Summary ---
$(free -m)

$optional_heap_profile_section
EOF
}

print_snapshot_status() {
  local now="$1"
  local status_line="$2"

  if [[ $(( now - LAST_SNAPSHOT_PRINT_TIME )) -lt "$SNAPSHOT_PRINT_INTERVAL_SECONDS" ]]; then
    return
  fi

  echo "$status_line"
  LAST_SNAPSHOT_PRINT_TIME="$now"
}

capture_heap_profile() {
  local memory_pct="$1"
  local heap_profile_path_prefix heap_profile_summary

  heap_profile_path_prefix="$HEAP_PROFILES_DIR/$(date '+%Y%m%d-%H%M%S')-${memory_pct}pct"
  mkdir -p "$(dirname "$heap_profile_path_prefix")"
  fetch_pprof "heap" "${heap_profile_path_prefix}.pb.gz" || true
  heap_profile_summary="$(print_heap_profile_summary "${heap_profile_path_prefix}.pb.gz")"

  cat <<EOF

$heap_profile_summary
EOF
}

snapshot() {
  local heap_profile_section="" memory_total_kb memory_available_kb memory_used_kb memory_used_mb memory_pct should_terminate_process_group

  memory_total_kb="$(awk '/MemTotal/ {print $2}' /proc/meminfo)"
  memory_available_kb="$(awk '/MemAvailable/ {print $2}' /proc/meminfo)"
  memory_used_kb=$(( memory_total_kb - memory_available_kb ))
  memory_used_mb=$(( memory_used_kb / 1024 ))
  memory_pct=$(( memory_used_kb * 100 / memory_total_kb ))

  # Get the top memory-heavy processes, format as "name (MB)".
  local top_processes
  top_processes="$(ps -eo rss,comm --sort=-rss | awk 'NR>1 && NR<=6 {printf "%s (%dMB), ", $2, $1/1024}' | sed 's/, $//')"

  local timestamp
  timestamp="$(date '+%H:%M:%S')"
  local now
  now="$(date +%s)"

  local status_line
  status_line="$(printf "%s used=%s%% mem=%sMB procs=[%s]" "$timestamp" "$memory_pct" "$memory_used_mb" "$top_processes")"
  echo "$status_line" >> "$SNAPSHOT_HISTORY_FILE"
  print_snapshot_status "$now" "$status_line"

  should_terminate_process_group=false
  if [[ "$memory_pct" -ge "$OOM_TERMINATION_THRESHOLD" ]] && [[ "$OOM_TERMINATED" == "false" ]]; then
    should_terminate_process_group=true
  fi

  if [[ "$HAS_CAPTURED_HEAP_PROFILE" == "false" ]] &&
    [[ "$memory_pct" -ge "$HEAP_PROFILE_CAPTURE_THRESHOLD" || "$should_terminate_process_group" == "true" ]]; then
    heap_profile_section="$(capture_heap_profile "$memory_pct")"
    HAS_CAPTURED_HEAP_PROFILE=true
  fi

  # Write the snapshot only at new memory highs so the final artifact represents
  # the worst observed point without emitting one file per sample.
  if [[ "$memory_pct" -gt "$MEMORY_HIGH_WATER_MARK" ]]; then
    write_snapshot_report "$memory_pct" "$heap_profile_section"
    MEMORY_HIGH_WATER_MARK="$memory_pct"
  fi

  if [[ "$should_terminate_process_group" == "true" ]]; then
    OOM_TERMINATED=true
    write_oom_junit "$memory_pct"
    terminate_monitored_processes "$memory_pct"
  fi
}

# Take snapshots until killed.
while true; do
  snapshot
  sleep "$SNAPSHOT_INTERVAL_SECONDS"
done
