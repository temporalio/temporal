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
CONTAINER_PEAKS_FILE="/tmp/container_peaks.txt"
HIGH_MEMORY_THRESHOLD=95
PPROF_HOST="${PPROF_HOST:-localhost:7000}"
HEAP_PRINTED=false
HIGH_WATER_MARK=0
SYSTEM_PEAK_MB=0
SYSTEM_PEAK_PCT=0
SYSTEM_PEAK_TS=""

# Clear state files on start
: > "$HISTORY_FILE"
: > "$CONTAINER_PEAKS_FILE"

# On exit, emit a final summary so CI logs surface peaks even when the run was OK.
on_exit() {
  echo ""
  echo "=== Memory monitor summary ==="
  if [[ -n "$SYSTEM_PEAK_TS" ]]; then
    echo "System peak: ${SYSTEM_PEAK_MB}MB (${SYSTEM_PEAK_PCT}%) at ${SYSTEM_PEAK_TS}"
  fi
  if [[ -s "$CONTAINER_PEAKS_FILE" ]]; then
    echo "Container peaks (MB):"
    sort -t= -k2 -n -r "$CONTAINER_PEAKS_FILE" | sed 's/^/  /'
  fi
}
trap on_exit EXIT

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

# Print container memory snapshot. Updates per-container peaks in CONTAINER_PEAKS_FILE.
# Outputs one "name:NNNMB" pair per running container, comma-separated, on a single line.
print_containers() {
  command -v docker >/dev/null 2>&1 || return 0
  # docker stats --no-stream takes ~1s; format yields lines like "cassandra-1 123MiB / 1.9GiB".
  local raw
  raw="$(docker stats --no-stream --format '{{.Name}} {{.MemUsage}}' 2>/dev/null)" || return 0
  [[ -z "$raw" ]] && return 0
  # Convert each line to "name:NNNMB" and update peaks. Strip "github-" / "-1" wrappers
  # for readability; convert MiB/GiB/KiB to MB.
  awk '
    function to_mb(v,   n, u) {
      if (match(v, /[0-9.]+/)) n = substr(v, RSTART, RLENGTH); else return 0
      u = substr(v, RSTART+RLENGTH)
      if (u ~ /^GiB/ || u ~ /^GB/) return int(n * 1024 + 0.5)
      if (u ~ /^MiB/ || u ~ /^MB/) return int(n + 0.5)
      if (u ~ /^KiB/ || u ~ /^KB/) return int(n / 1024 + 0.5)
      if (u ~ /^B/)                return 0
      return int(n + 0.5)
    }
    {
      name = $1; sub(/^github-/, "", name); sub(/-[0-9]+$/, "", name)
      mb = to_mb($2)
      printf "%s:%dMB\n", name, mb
    }
  ' <<< "$raw" | while IFS=: read -r name rest; do
      mb="${rest%MB}"
      # Update peak for this container.
      prev="$(awk -F= -v n="$name" '$1==n{print $2; exit}' "$CONTAINER_PEAKS_FILE")"
      if [[ -z "$prev" || "$mb" -gt "$prev" ]]; then
        # Rewrite peaks file with updated value.
        awk -F= -v n="$name" -v v="$mb" 'BEGIN{found=0} $1==n{print n"="v; found=1; next} {print} END{if(!found) print n"="v}' \
          "$CONTAINER_PEAKS_FILE" > "$CONTAINER_PEAKS_FILE.tmp" && mv "$CONTAINER_PEAKS_FILE.tmp" "$CONTAINER_PEAKS_FILE"
      fi
      printf "%s:%dMB " "$name" "$mb"
  done | sed 's/ $//'
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

  # Get per-container memory; updates peaks as a side-effect.
  local containers
  containers="$(print_containers)"

  local timestamp
  timestamp="$(date '+%H:%M:%S')"

  # Track system memory peak over the whole run.
  if (( memused_mb > SYSTEM_PEAK_MB )); then
    SYSTEM_PEAK_MB="$memused_mb"
    SYSTEM_PEAK_PCT="$pct"
    SYSTEM_PEAK_TS="$timestamp"
  fi

  # stdout preserves info in CI logs in case of crash; history file is used for snapshot.
  printf "%s used=%s%% mem=%sMB goroutines=%s containers=[%s] procs=[%s]\n" \
    "$timestamp" "$pct" "$memused_mb" "$goroutine_count" "$containers" "$top_procs" | tee -a "$HISTORY_FILE"

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
