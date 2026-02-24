#!/bin/bash
#
# Memory Monitor
#
# Takes periodic memory snapshots. Captures system memory stats, top processes,
# and for Go processes, heap profiles via pprof.
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
HIGH_MEMORY_THRESHOLD=95
PPROF_HOST="${PPROF_HOST:-localhost:7000}"
HEAP_PRINTED=false
HIGH_WATER_MARK=0

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

snapshot() {
  local memtotal_kb memavail_kb memused_kb memused_mb pct
  memtotal_kb="$(awk '/MemTotal/ {print $2}' /proc/meminfo)"
  memavail_kb="$(awk '/MemAvailable/ {print $2}' /proc/meminfo)"
  memused_kb=$(( memtotal_kb - memavail_kb ))
  memused_mb=$(( memused_kb / 1024 ))
  pct=$(( memused_kb * 100 / memtotal_kb ))

  # Collect pprof data once per tick.
  local goroutine_output goroutine_count pprof_output
  goroutine_output="$(print_goroutines)"
  goroutine_count="$(count_goroutines <<< "$goroutine_output")"
  pprof_output="$(print_heap)"

  # Get processes with >=1% memory, format as "name (MB)"
  local top_procs
  top_procs="$(ps -eo %mem,rss,comm --sort=-%mem | awk 'NR>1 && $1>=1.0 {printf "%s (%dMB), ", $3, $2/1024}' | sed 's/, $//')"

  local timestamp
  timestamp="$(date '+%H:%M:%S')"

  # stdout preserves info in CI logs in case of crash; history file is used for snapshot.
  printf "%s used=%s%% mem=%sMB goroutines=%s procs=[%s]\n" \
    "$timestamp" "$pct" "$memused_mb" "$goroutine_count" "$top_procs" | tee -a "$HISTORY_FILE"

  # Build report.
  local report
  report="$(cat <<EOF
Memory snapshot at $(date '+%Y-%m-%d %H:%M:%S') (usage ${pct}%)

$(cat "$HISTORY_FILE")

--- Top Processes ---
$(ps -eo pid,%mem,rss:10,comm --sort=-%mem | head -20)

--- Memory Summary ---
$(free -m)

$pprof_output

$goroutine_output
EOF
)"

  # Print report to stdout when memory threshold is reached (only once per run).
  if [[ "$pct" -ge "$HIGH_MEMORY_THRESHOLD" ]] && [[ "$HEAP_PRINTED" == "false" ]]; then
    echo ""
    echo "$report"
    echo ""
    HEAP_PRINTED=true
  fi

  # Write report to disk only if memory usage is at or above high water mark.
  if [[ "$pct" -ge "$HIGH_WATER_MARK" ]]; then
    echo "$report" > "$SNAPSHOT_FILE"
    HIGH_WATER_MARK="$pct"
  fi
}

# Take snapshots every 30s until killed.
while true; do
  snapshot
  sleep 30
done
