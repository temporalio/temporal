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

# Print heap analysis from `go tool pprof`.
# Usage: print_heap_analysis <profile_file> <mode> <lines>
# mode: inuse_space, alloc_space, inuse_objects, alloc_objects
print_heap_analysis() {
  local profile_file="$1"
  local mode="$2"
  local lines="${3:-40}"

  go tool pprof -top "-${mode}" "$profile_file" 2>/dev/null | head -"$lines" || true
}

# Get goroutine count from pprof.
print_goroutine_count() {
  local tmp_file
  tmp_file="$(mktemp)"
  trap 'rm -f "$tmp_file"' RETURN

  local count="?"
  if curl -s --max-time 5 "http://${PPROF_HOST}/debug/pprof/goroutine?debug=1" -o "$tmp_file" 2>/dev/null; then
    count="$(head -1 "$tmp_file" | grep -o '[0-9]*' || echo '?')"
  fi
  echo "$count"
}

# Print pprof heap analysis.
print_pprof_analysis() {
  local tmp_file
  tmp_file="$(mktemp)"
  trap 'rm -f "$tmp_file"' RETURN

  echo "--- Go Heap Profile ---"
  if fetch_pprof "heap" "$tmp_file"; then
    echo "=== inuse_space (what's currently held) ==="
    print_heap_analysis "$tmp_file" "inuse_space" 30
    echo ""
    echo "=== alloc_space (total allocations) ==="
    print_heap_analysis "$tmp_file" "alloc_space" 30
    echo ""
    echo "=== alloc_objects (total objects allocated) ==="
    print_heap_analysis "$tmp_file" "alloc_objects" 30
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

  local goroutines
  goroutines="$(print_goroutine_count)"

  # Get processes with >=1% memory, format as "name (MB)"
  local top_procs
  top_procs="$(ps -eo %mem,rss,comm --sort=-%mem | awk 'NR>1 && $1>=1.0 {printf "%s (%dMB), ", $3, $2/1024}' | sed 's/, $//')"

  local timestamp
  timestamp="$(date '+%H:%M:%S')"

  # stdout preserves info in CI logs in case of crash; history file is used for snapshot.
  printf "%s used=%s%% mem=%sMB goroutines=%s procs=[%s]\n" \
    "$timestamp" "$pct" "$memused_mb" "$goroutines" "$top_procs" | tee -a "$HISTORY_FILE"

  # Collect pprof analysis once per tick.
  local pprof_output
  pprof_output="$(print_pprof_analysis)"

  # If memory threshold was reached, print Go heap details to stdout. But only once per run.
  if [[ "$pct" -ge "$HIGH_MEMORY_THRESHOLD" ]] && [[ "$HEAP_PRINTED" == "false" ]]; then
    echo ""
    echo "=== HIGH MEMORY WARNING: ${pct}% used (threshold: ${HIGH_MEMORY_THRESHOLD}%) ==="
    echo "$pprof_output"
    echo "=== END HIGH MEMORY WARNING ==="
    echo ""
    HEAP_PRINTED=true
  fi

  # Write snapshot to disk.
  {
    echo "Memory snapshot at $(date '+%Y-%m-%d %H:%M:%S')"
    echo ""
    cat "$HISTORY_FILE"
    echo ""
    echo "--- Top Processes ---"
    ps -eo pid,%mem,rss:10,comm --sort=-%mem | head -20
    echo ""
    echo "--- Memory Summary ---"
    free -m
    echo ""
    echo "$pprof_output"
  } > "$SNAPSHOT_FILE"
}

# Take snapshots every 30s until killed.
while true; do
  snapshot
  sleep 30
done
