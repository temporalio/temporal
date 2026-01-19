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
HEAP_PRINTED_FLAG="/tmp/heap_details_printed.flag"
HIGH_MEMORY_THRESHOLD=95
PPROF_HOST="${PPROF_HOST:-localhost:7000}"

# Clear history and flag on start
: > "$HISTORY_FILE"
rm -f "$HEAP_PRINTED_FLAG"

# Fetch a pprof profile and save to file
# Usage: fetch_pprof <profile_type> <output_file>
# Returns 0 on success, 1 on failure
fetch_pprof() {
  local profile_type="$1"
  local output_file="$2"
  curl -s --max-time 10 "http://${PPROF_HOST}/debug/pprof/${profile_type}" -o "$output_file" 2>/dev/null
}

# Analyze a heap profile with go tool pprof
# Usage: analyze_heap <profile_file> <mode> <lines>
# mode: inuse_space, alloc_space, inuse_objects, alloc_objects
analyze_heap() {
  local profile_file="$1"
  local mode="$2"
  local lines="${3:-40}"
  go tool pprof -top "-${mode}" "$profile_file" 2>/dev/null | head -"$lines" || true
}

# Get goroutine count from pprof
get_goroutine_count() {
  local count="?"
  if curl -s --max-time 5 "http://${PPROF_HOST}/debug/pprof/goroutine?debug=1" -o /tmp/goroutine.out 2>/dev/null; then
    count="$(head -1 /tmp/goroutine.out | grep -o '[0-9]*' || echo '?')"
  fi
  echo "$count"
}

# Print pprof heap analysis to stdout
print_pprof_analysis() {
  local heap_file="/tmp/heap.out"

  echo "--- Go Heap Profile ---"
  if fetch_pprof "heap" "$heap_file"; then
    echo "=== inuse_space (what's currently held) ==="
    analyze_heap "$heap_file" "inuse_space" 60
    echo ""
    echo "=== alloc_space (total allocations) ==="
    analyze_heap "$heap_file" "alloc_space" 60
    echo ""
    echo "=== alloc_objects (total objects allocated) ==="
    analyze_heap "$heap_file" "alloc_objects" 30
  else
    echo "(pprof endpoint not available)"
  fi
}

write_snapshot() {
  local memtotal_kb memavail_kb memused_kb memused_mb pct
  memtotal_kb="$(awk '/MemTotal/ {print $2}' /proc/meminfo)"
  memavail_kb="$(awk '/MemAvailable/ {print $2}' /proc/meminfo)"
  memused_kb=$(( memtotal_kb - memavail_kb ))
  memused_mb=$(( memused_kb / 1024 ))
  pct=$(( memused_kb * 100 / memtotal_kb ))

  local goroutines
  goroutines="$(get_goroutine_count)"

  # Get processes with >=1% memory, format as "name (MB)"
  local top_procs
  top_procs="$(ps -eo %mem,rss,comm --sort=-%mem | awk 'NR>1 && $1>=1.0 {printf "%s (%dMB), ", $3, $2/1024}' | sed 's/, $//')"

  local timestamp
  timestamp="$(date '+%H:%M:%S')"

  # Append to history.
  local row="$timestamp $pct $memused_mb $goroutines $top_procs"
  echo "$row" >> "$HISTORY_FILE"

  # Print to stdout (with descriptors) to preserve the information in case of a crash.
  echo "$timestamp used=${pct}% mem=${memused_mb}MB goroutines=${goroutines} procs=[${top_procs}]"

  # If memory threshold was reached, print Go heap details. But only once per run.
  if [[ "$pct" -ge "$HIGH_MEMORY_THRESHOLD" ]] && [[ ! -f "$HEAP_PRINTED_FLAG" ]]; then
    echo ""
    echo "=== HIGH MEMORY WARNING: ${pct}% used (threshold: ${HIGH_MEMORY_THRESHOLD}%) ==="
    print_pprof_analysis
    echo "=== END HIGH MEMORY WARNING ==="
    echo ""
    touch "$HEAP_PRINTED_FLAG"
  fi

  {
    echo "Memory snapshot at $(date '+%Y-%m-%d %H:%M:%S')"
    echo ""
    echo "Time      Used(%)  Used(MB)  Goroutines  Top Processes"
    echo "---------------------------------------------------------------"
    while read -r t p mb g procs; do
      printf "%-10s %3s%%     %5s   %7s     %s\n" "$t" "$p" "$mb" "$g" "$procs"
    done < "$HISTORY_FILE"
    echo ""
    echo "--- Top Processes ---"
    ps -eo pid,%mem,rss:10,comm --sort=-%mem | head -20
    echo ""
    echo "--- Memory Summary ---"
    free -m
    echo ""
  } > "$SNAPSHOT_FILE"
  print_pprof_analysis >> "$SNAPSHOT_FILE"
}

# Take snapshots every 30s until killed
while true; do
  write_snapshot
  sleep 30
done
