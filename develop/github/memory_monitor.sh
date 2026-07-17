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
#       profile plus related Go pprof outputs in HEAP_PROFILES_DIR before
#       running analysis.
#
# 3. OOM kill:
#       When usage crosses OOM_TERMINATION_THRESHOLD, reuses any previously
#       captured profile, writes the latest snapshot and a synthetic JUnit
#       report, then terminates the monitored test process group so post-test
#       artifact upload can still run. If no profile has been captured yet, it
#       captures one before terminating.
#
# Usage:
#   ./memory_monitor.sh
#
set -euo pipefail

if [[ $# -ne 0 ]]; then
  echo "Usage: $0" >&2
  exit 1
fi

# Snapshot config.
readonly SNAPSHOT_DIR="${SNAPSHOT_DIR:-.testoutput/memory}"
# Sample every second so short OOM ramps still leave history, but print less
# often to keep CI logs readable.
readonly SNAPSHOT_INTERVAL_SECONDS="${SNAPSHOT_INTERVAL_SECONDS:-1}"
readonly SNAPSHOT_PRINT_INTERVAL_SECONDS="${SNAPSHOT_PRINT_INTERVAL_SECONDS:-30}"
readonly SNAPSHOT_FILE="${SNAPSHOT_FILE:-$SNAPSHOT_DIR/memory-snapshot.txt}"
readonly SNAPSHOT_HISTORY_FILE="${SNAPSHOT_HISTORY_FILE:-$SNAPSHOT_DIR/memory-history.txt}"
readonly SYSTEM_DIAGNOSTICS_FILE="${SYSTEM_DIAGNOSTICS_FILE:-$SNAPSHOT_DIR/system-diagnostics.txt}"

# Heap profile config.
# Capture before the termination threshold so the diagnostic profile is usually
# available even if the runner kills the job before our termination path runs.
readonly HEAP_PROFILE_CAPTURE_THRESHOLD="${HEAP_PROFILE_CAPTURE_THRESHOLD:-90}"
readonly HEAP_PROFILE_REFRESH_INTERVAL_SECONDS="${HEAP_PROFILE_REFRESH_INTERVAL_SECONDS:-30}"
readonly HEAP_PROFILES_DIR="${HEAP_PROFILES_DIR:-$SNAPSHOT_DIR/heap-profiles}"
readonly PPROF_HOST="${PPROF_HOST:-localhost:7000}"

# OOM kill config.
# Terminate late enough to avoid masking near-finished tests, but before the
# runner OOM killer skips post-test artifact upload.
readonly OOM_TERMINATION_THRESHOLD="${OOM_TERMINATION_THRESHOLD:-99}"
readonly OOM_JUNIT_FILE="${OOM_JUNIT_FILE:-.testoutput/junit.oom.xml}"

# State.
MEMORY_HIGH_WATER_MARK=0
LAST_SNAPSHOT_PRINT_TIME=0
LAST_HEAP_PROFILE_CAPTURE_TIME=0
HEAP_PROFILE_SECTION=""
HAS_CAPTURED_HEAP_PROFILE=false
OOM_TERMINATED=false

ensure_snapshot_dirs() {
  mkdir -p "$(dirname "$SNAPSHOT_FILE")" "$(dirname "$SNAPSHOT_HISTORY_FILE")"
}

init_snapshot_files() {
  ensure_snapshot_dirs
  : > "$SNAPSHOT_HISTORY_FILE"
}

# Fetch a pprof profile and save to file
# Usage: fetch_pprof <pprof_profile> <output_file>
# Returns 0 on success, 1 on failure
fetch_pprof() {
  local pprof_profile="$1"
  local output_file="$2"
  curl -s --max-time 10 "http://${PPROF_HOST}/debug/pprof/${pprof_profile}" -o "$output_file" 2>/dev/null
}

write_file_section() {
  local title="$1"
  local file="$2"

  echo "--- ${title} ---"
  if [[ ! -r "$file" ]] || ! cat "$file" 2>/dev/null; then
    echo "(${file} not available)"
  fi
  echo
}

write_command_section() {
  local title="$1"
  shift

  echo "--- ${title} ---"
  "$@" || true
  echo
}

top_memory_pids() {
  ps -eo pid=,rss= --sort=-rss | awk 'NR<=8 {print $1}'
}

write_process_diagnostics() {
  local pid

  write_command_section "Processes by RSS" ps -eo pid,ppid,pgid,%mem,rss:10,vsz:10,etime,stat,comm,args --sort=-rss

  if [[ -n "${MONITORED_PROCESS_GROUP:-}" ]]; then
    echo "--- Monitored Process Group ---"
    ps -eo pid,ppid,pgid,%mem,rss:10,vsz:10,etime,stat,comm,args --sort=-rss |
      awk -v pgid="$MONITORED_PROCESS_GROUP" 'NR == 1 || $3 == pgid'
    echo
  fi

  for pid in $(top_memory_pids); do
    if [[ ! -d "/proc/$pid" ]]; then
      continue
    fi

    echo "--- Process ${pid}: status ---"
    cat "/proc/$pid/status" 2>/dev/null || true
    echo

    write_file_section "Process ${pid}: smaps_rollup" "/proc/$pid/smaps_rollup"

    echo "--- Process ${pid}: file descriptors ---"
    find "/proc/$pid/fd" -maxdepth 1 -type l -printf '%f -> %l\n' 2>/dev/null | sort | head -200 || true
    echo
  done
}

write_system_diagnostics() {
  {
    echo "System diagnostics at $(date '+%Y-%m-%d %H:%M:%S')"
    echo
    write_file_section "/proc/meminfo" "/proc/meminfo"
    write_file_section "/proc/pressure/memory" "/proc/pressure/memory"
    write_file_section "/proc/vmstat" "/proc/vmstat"
    write_command_section "free -m" free -m
    write_process_diagnostics
  } > "$SYSTEM_DIAGNOSTICS_FILE"
}

# Print heap profile analysis.
print_heap_profile_summary() {
  local heap_profile_file="$1"

  echo "--- Go Heap Profile ---"
  if [[ -s "$heap_profile_file" ]]; then
    echo "=== inuse_space (what's currently held) ==="
    # Keep the artifact focused on retained memory; allocation totals are noisy
    # for this CI OOM investigation.
    go tool pprof -top -inuse_space "$heap_profile_file" 2>/dev/null | head -15 || true
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
    <testcase classname="memory_monitor" name="OOM kill" time="0">
      <failure type="OOM" message="OOM kill threshold reached">Memory monitor terminated the test process at ${memory_pct}% memory before the runner OOM kill. See memory diagnostics artifacts.</failure>
    </testcase>
  </testsuite>
</testsuites>
EOF
}

write_snapshot_report() {
  local memory_pct="$1"
  local optional_heap_profile_section="$2"

  ensure_snapshot_dirs
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
  fetch_pprof "allocs" "${heap_profile_path_prefix}-allocs.pb.gz" || true
  fetch_pprof "goroutine" "${heap_profile_path_prefix}-goroutine.pb.gz" || true
  fetch_pprof "goroutine?debug=2" "${heap_profile_path_prefix}-goroutine-debug-2.txt" || true
  fetch_pprof "threadcreate?debug=1" "${heap_profile_path_prefix}-threadcreate.txt" || true
  heap_profile_summary="$(print_heap_profile_summary "${heap_profile_path_prefix}.pb.gz")"

  printf '\n%s\n' "$heap_profile_summary"
}

should_capture_heap_profile() {
  local now="$1"
  local memory_pct="$2"
  local is_new_high="$3"
  local should_terminate_process_group="$4"

  if [[ "$should_terminate_process_group" == "true" ]] && [[ "$HAS_CAPTURED_HEAP_PROFILE" == "false" ]]; then
    return 0
  fi

  if [[ "$is_new_high" != "true" ]] || [[ "$memory_pct" -lt "$HEAP_PROFILE_CAPTURE_THRESHOLD" ]]; then
    return 1
  fi

  if [[ $(( now - LAST_HEAP_PROFILE_CAPTURE_TIME )) -lt "$HEAP_PROFILE_REFRESH_INTERVAL_SECONDS" ]]; then
    return 1
  fi

  return 0
}

snapshot() {
  local memory_total_kb memory_available_kb memory_used_kb memory_used_mb memory_pct is_new_high should_terminate_process_group wrote_snapshot

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
  ensure_snapshot_dirs
  echo "$status_line" >> "$SNAPSHOT_HISTORY_FILE"
  print_snapshot_status "$now" "$status_line"

  is_new_high=false
  if [[ "$memory_pct" -gt "$MEMORY_HIGH_WATER_MARK" ]]; then
    is_new_high=true
  fi

  should_terminate_process_group=false
  if [[ "$memory_pct" -ge "$OOM_TERMINATION_THRESHOLD" ]] && [[ "$OOM_TERMINATED" == "false" ]]; then
    should_terminate_process_group=true
  fi

  # Capture lightweight diagnostics before slower pprof fetches. Near test exit
  # or OOM, the test process can disappear while profiles are being collected.
  wrote_snapshot=false
  if [[ "$is_new_high" == "true" ]] || [[ ! -e "$SNAPSHOT_FILE" ]]; then
    write_snapshot_report "$memory_pct" "$HEAP_PROFILE_SECTION"
    write_system_diagnostics
    MEMORY_HIGH_WATER_MARK="$memory_pct"
    wrote_snapshot=true
  fi

  if should_capture_heap_profile "$now" "$memory_pct" "$is_new_high" "$should_terminate_process_group"; then
    HEAP_PROFILE_SECTION="$(capture_heap_profile "$memory_pct")"
    LAST_HEAP_PROFILE_CAPTURE_TIME="$now"
    HAS_CAPTURED_HEAP_PROFILE=true

    if [[ "$wrote_snapshot" == "true" ]]; then
      write_snapshot_report "$memory_pct" "$HEAP_PROFILE_SECTION"
    fi
  fi

  if [[ "$should_terminate_process_group" == "true" ]]; then
    OOM_TERMINATED=true
    write_oom_junit "$memory_pct"
    terminate_monitored_processes "$memory_pct"
  fi
}

init_snapshot_files

# Take snapshots until killed.
while true; do
  snapshot
  sleep "$SNAPSHOT_INTERVAL_SECONDS"
done
