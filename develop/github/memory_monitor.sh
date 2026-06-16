#!/bin/bash
#
# Memory Monitor
#
# Logs cheap memory status every POLL_INTERVAL_SECONDS and writes the
# highest-memory snapshot to a file. When usage crosses PROFILE_CAPTURE_THRESHOLD,
# captures pprof and process memory diagnostics in MEMORY_DIAGNOSTICS_DIR, then
# repeats diagnostic capture every PROFILE_INTERVAL_SECONDS while usage remains
# above the threshold.
#
# Usage:
#   ./memory_monitor.sh <snapshot-file>
#
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <snapshot-file>" >&2
  exit 1
fi

SNAPSHOT_FILE="$1"
HISTORY_FILE="/tmp/memory_history.txt"
POLL_INTERVAL_SECONDS="${POLL_INTERVAL_SECONDS:-5}"
PROFILE_INTERVAL_SECONDS="${PROFILE_INTERVAL_SECONDS:-30}"
PROFILE_CAPTURE_THRESHOLD="${PROFILE_CAPTURE_THRESHOLD:-85}"
MEMORY_DIAGNOSTICS_DIR="${MEMORY_DIAGNOSTICS_DIR:-.testoutput/memory}"
REPORT_PRINT_THRESHOLD=95
PPROF_HOST="${PPROF_HOST:-localhost:7000}"
REPORT_PRINTED=false
SNAPSHOT_HIGH_WATER_MARK=0
LAST_PROFILE_CAPTURE_TIME=0
WAS_ABOVE_PROFILE_CAPTURE_THRESHOLD=false

# Clear history on start
: > "$HISTORY_FILE"

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

# Print goroutine profile analysis.
print_goroutines() {
  local tmp_file
  tmp_file="$(mktemp)"
  trap 'rm -f "$tmp_file"' RETURN

  if fetch_pprof "goroutine" "$tmp_file"; then
    echo "=== top functions by goroutine count ==="
    pprof_top "$tmp_file" 30
  fi
}

# Extract goroutine count from pprof output ("... of N total").
count_goroutines() {
  sed -n 's/.*of \([0-9]*\) total.*/\1/p' | head -1 || echo '?'
}

# Print heap profile analysis.
print_heap() {
  local tmp_file
  tmp_file="$(mktemp)"
  trap 'rm -f "$tmp_file"' RETURN

  echo "--- Go Heap Profile ---"
  if fetch_pprof "heap" "$tmp_file"; then
    echo "=== inuse_space (what's currently held) ==="
    pprof_top "$tmp_file" 30 -inuse_space
    echo ""
    echo "=== alloc_space (total allocations) ==="
    pprof_top "$tmp_file" 30 -alloc_space
    echo ""
    echo "=== alloc_objects (total objects allocated) ==="
    pprof_top "$tmp_file" 30 -alloc_objects
  else
    echo "(pprof endpoint not available)"
  fi
}

test_binary_pid() {
  ps -eo pid=,rss=,comm= --sort=-rss | awk '$3 == "tests.test" {print $1; exit}'
}

format_process_memory() {
  local pid="$1"

  if [[ -z "$pid" ]] || [[ ! -d "/proc/$pid" ]]; then
    echo "(tests.test process not found)"
    return
  fi

  echo "--- tests.test /proc/$pid/status ---"
  cat "/proc/$pid/status" 2>/dev/null || true
  echo ""
  echo "--- tests.test /proc/$pid/smaps_rollup ---"
  cat "/proc/$pid/smaps_rollup" 2>/dev/null || true
  echo ""
  echo "--- tests.test pmap ---"
  pmap -x "$pid" 2>/dev/null | tail -40 || true
}

save_process_memory_files() {
  local pid="$1"
  local prefix="$2"

  if [[ -z "$pid" ]] || [[ ! -d "/proc/$pid" ]]; then
    return
  fi

  cat "/proc/$pid/status" > "${prefix}.status.txt" 2>/dev/null || true
  cat "/proc/$pid/smaps_rollup" > "${prefix}.smaps_rollup.txt" 2>/dev/null || true
  pmap -x "$pid" > "${prefix}.pmap.txt" 2>/dev/null || true
}

save_pprof_files() {
  local prefix="$1"

  mkdir -p "$MEMORY_DIAGNOSTICS_DIR"
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

build_snapshot_report() {
  local pct="$1"

  cat <<EOF
Memory snapshot at $(date '+%Y-%m-%d %H:%M:%S') (usage ${pct}%)

$(cat "$HISTORY_FILE")

--- Top Processes ---
$(ps -eo pid,%mem,rss:10,comm --sort=-%mem | head -20)

--- Memory Summary ---
$(free -m)
EOF
}

capture_diagnostics() {
  local pct="$1"
  local goroutine_output goroutine_count pprof_output test_pid diagnostic_prefix process_memory_output

  goroutine_output="$(print_goroutines)"
  goroutine_count="$(count_goroutines <<< "$goroutine_output")"
  pprof_output="$(print_heap)"
  test_pid="$(test_binary_pid)"
  process_memory_output="$(format_process_memory "$test_pid")"
  diagnostic_prefix="$MEMORY_DIAGNOSTICS_DIR/$(date '+%Y%m%d-%H%M%S')-${pct}pct"
  save_pprof_files "$diagnostic_prefix"
  save_process_memory_files "$test_pid" "$diagnostic_prefix"

  cat <<EOF

Captured goroutines: $goroutine_count

$process_memory_output

$pprof_output

$goroutine_output
EOF
}

snapshot() {
  local memtotal_kb memavail_kb memused_kb memused_mb pct report
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

  # stdout preserves info in CI logs in case of crash; history file is used for snapshot.
  printf "%s used=%s%% mem=%sMB procs=[%s]\n" \
    "$timestamp" "$pct" "$memused_mb" "$top_procs" | tee -a "$HISTORY_FILE"

  local now
  now="$(date +%s)"
  report="$(build_snapshot_report "$pct")"
  if should_capture_profile "$pct" "$now"; then
    LAST_PROFILE_CAPTURE_TIME="$now"

    # Collect pprof data only when thresholds are crossed, or periodically after that.
    report+=$(capture_diagnostics "$pct")
  fi

  # Print report to stdout when memory threshold is reached (only once per run).
  if [[ "$pct" -ge "$REPORT_PRINT_THRESHOLD" ]] && [[ "$REPORT_PRINTED" == "false" ]]; then
    echo ""
    echo "$report"
    echo ""
    REPORT_PRINTED=true
  fi

  # Write report to disk only if memory usage is at or above high water mark.
  if [[ "$pct" -ge "$SNAPSHOT_HIGH_WATER_MARK" ]]; then
    echo "$report" > "$SNAPSHOT_FILE"
    SNAPSHOT_HIGH_WATER_MARK="$pct"
  fi
}

# Take snapshots until killed.
while true; do
  snapshot
  sleep "$POLL_INTERVAL_SECONDS"
done
