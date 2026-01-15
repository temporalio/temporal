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

# Clear history on start
: > "$HISTORY_FILE"

write_snapshot() {
  local memtotal_kb memavail_kb memused_kb memused_mb pct
  memtotal_kb="$(awk '/MemTotal/ {print $2}' /proc/meminfo)"
  memavail_kb="$(awk '/MemAvailable/ {print $2}' /proc/meminfo)"
  memused_kb=$(( memtotal_kb - memavail_kb ))
  memused_mb=$(( memused_kb / 1024 ))
  pct=$(( memused_kb * 100 / memtotal_kb ))

  local goroutines="?"
  if curl -s --max-time 5 'http://localhost:7000/debug/pprof/goroutine?debug=1' -o /tmp/goroutine.out 2>/dev/null; then
    goroutines="$(head -1 /tmp/goroutine.out | grep -o '[0-9]*' || echo '?')"
  fi

  # Get processes with >=1% memory, format as "name (MB)"
  local top_procs
  top_procs="$(ps -eo %mem,rss,comm --sort=-%mem | awk 'NR>1 && $1>=1.0 {printf "%s (%dMB), ", $3, $2/1024}' | sed 's/, $//')"

  local timestamp
  timestamp="$(date '+%H:%M:%S')"

  # Append to history
  echo "$timestamp $pct $memused_mb $goroutines $top_procs" >> "$HISTORY_FILE"

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
    echo "--- Go Heap Profile ---"
    if curl -s --max-time 10 "http://localhost:7000/debug/pprof/heap" -o /tmp/heap.out 2>/dev/null; then
      go tool pprof -top -inuse_space /tmp/heap.out 2>/dev/null | head -60 || true
      echo ""
      go tool pprof -top -alloc_space /tmp/heap.out 2>/dev/null | head -60 || true
    else
      echo "(pprof endpoint not available)"
    fi
  } > "$SNAPSHOT_FILE"
}

# Take snapshots every 30s until killed
while true; do
  write_snapshot
  sleep 30
done
