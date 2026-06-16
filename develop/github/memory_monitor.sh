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
#       When usage crosses PROFILE_CAPTURE_THRESHOLD, captures
#       pprof, debug, process, and system profiles in PROFILE_OUTPUT_DIR
#       before running analysis, then repeats every PROFILE_INTERVAL_SECONDS
#       while usage remains above the threshold.
#
# 3. OOM prevention:
#       When usage crosses OOM_TERMINATION_THRESHOLD, captures a final profile
#       if needed, writes the latest snapshot and a synthetic JUnit report,
#       then terminates the monitored test process group so post-test artifact
#       upload can still run.
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
MEMORY_OUTPUT_DIR="${MEMORY_OUTPUT_DIR:-.testoutput/memory}"
SNAPSHOT_INTERVAL_SECONDS="${SNAPSHOT_INTERVAL_SECONDS:-5}"
SNAPSHOT_PRINT_INTERVAL_SECONDS="${SNAPSHOT_PRINT_INTERVAL_SECONDS:-30}"
SNAPSHOT_FILE="${1:-${SNAPSHOT_FILE:-$MEMORY_OUTPUT_DIR/snapshot.txt}}"
SNAPSHOT_HISTORY_FILE="${SNAPSHOT_HISTORY_FILE:-$MEMORY_OUTPUT_DIR/snapshot-history.txt}"
SNAPSHOT_STATUS_HISTORY_FILE="${SNAPSHOT_STATUS_HISTORY_FILE:-$MEMORY_OUTPUT_DIR/snapshot-status-history.txt}"

# Profile config.
PROFILE_INTERVAL_SECONDS="${PROFILE_INTERVAL_SECONDS:-30}"
PROFILE_CAPTURE_THRESHOLD="${PROFILE_CAPTURE_THRESHOLD:-90}"
PROFILE_OUTPUT_DIR="${PROFILE_OUTPUT_DIR:-$MEMORY_OUTPUT_DIR/profile}"
PPROF_HOST="${PPROF_HOST:-localhost:7000}"

# OOM prevention config.
OOM_TERMINATION_THRESHOLD="${OOM_TERMINATION_THRESHOLD:-97}"
OOM_JUNIT_FILE="${OOM_JUNIT_FILE:-.testoutput/junit.oom.xml}"

# State.
SNAPSHOT_HIGH_WATER_MARK=0
LAST_SNAPSHOT_PRINT_TIME=0
LAST_PROFILE_CAPTURE_TIME=0
WAS_ABOVE_PROFILE_CAPTURE_THRESHOLD=false
OOM_TERMINATED=false

# Clear history on start
mkdir -p "$(dirname "$SNAPSHOT_FILE")" "$(dirname "$SNAPSHOT_HISTORY_FILE")" "$(dirname "$SNAPSHOT_STATUS_HISTORY_FILE")"
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

# Fetch a debug endpoint and save to file
# Usage: fetch_debug <endpoint> <output_file>
# Returns 0 on success, 1 on failure
fetch_debug() {
  local endpoint="$1"
  local output_file="$2"
  curl -s --max-time 10 "http://${PPROF_HOST}/debug/${endpoint}" -o "$output_file" 2>/dev/null
}

# Print pprof top analysis.
# Usage: pprof_top <profile_file> <lines> [extra_flags...]
pprof_top() {
  local profile_file="$1"
  local lines="$2"
  shift 2

  go tool pprof -top "$@" "$profile_file" 2>/dev/null | head -"$lines" || true
}

# Print goroutine profile analysis.
print_goroutines() {
  local profile_file="$1"

  if [[ -s "$profile_file" ]]; then
    echo "=== top functions by goroutine count ==="
    pprof_top "$profile_file" 30
  else
    echo "(goroutine profile not available)"
  fi
}

# Extract goroutine count from the saved goroutine debug profile.
count_goroutines() {
  local profile_file="$1"
  local count

  count="$(grep -c '^goroutine ' "$profile_file" 2>/dev/null || true)"
  echo "${count:-?}"
}

# Print heap profile analysis.
print_heap() {
  local profile_file="$1"

  echo "--- Go Heap Profile ---"
  if [[ -s "$profile_file" ]]; then
    echo "=== inuse_space (what's currently held) ==="
    pprof_top "$profile_file" 30 -inuse_space
    echo ""
    echo "=== alloc_space (total allocations) ==="
    pprof_top "$profile_file" 30 -alloc_space
    echo ""
    echo "=== alloc_objects (total objects allocated) ==="
    pprof_top "$profile_file" 30 -alloc_objects
  else
    echo "(heap profile not available)"
  fi
}

test_binary_pid() {
  ps -eo pid=,rss=,comm= --sort=-rss | awk '$3 == "tests.test" {print $1; exit}'
}

format_process_profile() {
  local pid="$1"
  local prefix="$2"

  if [[ -z "$pid" ]] || [[ ! -s "${prefix}.status.txt" ]]; then
    echo "(tests.test process not found)"
    return
  fi

  echo "--- tests.test /proc/$pid/status ---"
  cat "${prefix}.status.txt" 2>/dev/null || true
  echo ""
  echo "--- tests.test /proc/$pid/smaps_rollup ---"
  cat "${prefix}.smaps_rollup.txt" 2>/dev/null || true
  echo ""
  echo "--- tests.test pmap ---"
  tail -40 "${prefix}.pmap.txt" 2>/dev/null || true
}

save_process_profile_files() {
  local pid="$1"
  local prefix="$2"

  if [[ -z "$pid" ]] || [[ ! -d "/proc/$pid" ]]; then
    return
  fi

  cat "/proc/$pid/status" > "${prefix}.status.txt" 2>/dev/null || true
  cat "/proc/$pid/maps" > "${prefix}.maps.txt" 2>/dev/null || true
  cat "/proc/$pid/smaps" > "${prefix}.smaps.txt" 2>/dev/null || true
  cat "/proc/$pid/smaps_rollup" > "${prefix}.smaps_rollup.txt" 2>/dev/null || true
  pmap -x "$pid" > "${prefix}.pmap.txt" 2>/dev/null || true
}

save_system_profile_files() {
  local prefix="$1"
  local cgroup_file="${prefix}.system-cgroup-memory.txt"

  mkdir -p "$(dirname "$prefix")"
  ps -eo pid,ppid,pgid,%mem,rss,vsz,comm,args --sort=-rss > "${prefix}.system-processes.txt" 2>/dev/null || true
  free -m > "${prefix}.system-free.txt" 2>/dev/null || true
  cat /proc/meminfo > "${prefix}.system-meminfo.txt" 2>/dev/null || true
  cat /proc/vmstat > "${prefix}.system-vmstat.txt" 2>/dev/null || true
  cat /proc/self/cgroup > "${prefix}.system-cgroup.txt" 2>/dev/null || true

  : > "$cgroup_file"
  for file in \
    /sys/fs/cgroup/memory.current \
    /sys/fs/cgroup/memory.max \
    /sys/fs/cgroup/memory.events \
    /sys/fs/cgroup/memory.stat \
    /sys/fs/cgroup/memory.swap.current \
    /sys/fs/cgroup/memory.swap.max; do
    if [[ -r "$file" ]]; then
      {
        echo "=== $file ==="
        cat "$file"
        echo ""
      } >> "$cgroup_file" 2>/dev/null || true
    fi
  done
}

save_debug_files() {
  local prefix="$1"

  mkdir -p "$(dirname "$prefix")"
  fetch_debug "vars" "${prefix}.debug-vars.json" || true
}

save_pprof_files() {
  local prefix="$1"

  mkdir -p "$(dirname "$prefix")"
  fetch_pprof "heap" "${prefix}.heap.pb.gz" || true
  fetch_pprof "allocs" "${prefix}.allocs.pb.gz" || true
  fetch_pprof "goroutine" "${prefix}.goroutine.pb.gz" || true
  fetch_pprof "threadcreate" "${prefix}.threadcreate.pb.gz" || true
  fetch_pprof "goroutine?debug=2" "${prefix}.goroutine-debug-2.txt" || true
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

  local test_pid
  test_pid="$(test_binary_pid)"
  if [[ -n "$test_pid" ]]; then
    echo "Terminating tests.test process ${test_pid} at ${pct}% memory to preserve diagnostics artifacts."
    kill -TERM "$test_pid" 2>/dev/null || true
  fi
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
$(ps -eo pid,%mem,rss:10,comm --sort=-%mem | head -20)

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
  local goroutine_output goroutine_count pprof_output test_pid profile_prefix process_profile_output

  profile_prefix="$PROFILE_OUTPUT_DIR/$(date '+%Y%m%d-%H%M%S')-${pct}pct"
  test_pid="$(test_binary_pid)"
  save_pprof_files "$profile_prefix"
  save_debug_files "$profile_prefix"
  save_process_profile_files "$test_pid" "$profile_prefix"
  save_system_profile_files "$profile_prefix"
  goroutine_output="$(print_goroutines "${profile_prefix}.goroutine.pb.gz")"
  goroutine_count="$(count_goroutines "${profile_prefix}.goroutine-debug-2.txt")"
  pprof_output="$(print_heap "${profile_prefix}.heap.pb.gz")"
  process_profile_output="$(format_process_profile "$test_pid" "$profile_prefix")"

  cat <<EOF

Captured goroutines: $goroutine_count

$process_profile_output

$pprof_output

$goroutine_output
EOF
}

snapshot() {
  local memtotal_kb memavail_kb memused_kb memused_mb pct profile_report report should_terminate
  memtotal_kb="$(awk '/MemTotal/ {print $2}' /proc/meminfo)"
  memavail_kb="$(awk '/MemAvailable/ {print $2}' /proc/meminfo)"
  memused_kb=$(( memtotal_kb - memavail_kb ))
  memused_mb=$(( memused_kb / 1024 ))
  pct=$(( memused_kb * 100 / memtotal_kb ))

  # Get processes with >=1% memory, format as "name (MB)"
  local top_procs
  top_procs="$(ps -eo %mem,rss,comm --sort=-%mem | awk 'NR>1 && $1>=1.0 {printf "%s (%dMB), ", $3, $2/1024}' | sed 's/, $//')"

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

  if should_capture_profile "$pct" "$now"; then
    LAST_PROFILE_CAPTURE_TIME="$now"
    profile_report="$(capture_profile "$pct")"
  elif [[ "$should_terminate" == "true" ]]; then
    LAST_PROFILE_CAPTURE_TIME="$now"
    profile_report="$(capture_profile "$pct")"
  fi

  report="$(build_snapshot_report "$pct")"
  report+="$profile_report"

  # Write report to disk only if memory usage is at or above high water mark.
  if [[ "$pct" -ge "$SNAPSHOT_HIGH_WATER_MARK" ]]; then
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
