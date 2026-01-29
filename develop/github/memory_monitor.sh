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
HIGH_GOROUTINE_THRESHOLD=10000
PPROF_HOST="${PPROF_HOST:-localhost:7000}"
HEAP_PRINTED=false
GOROUTINE_DUMP_PRINTED=false

# Clear history on start
: > "$HISTORY_FILE"

# Store previous CPU stats for delta calculation
CPU_STATS_FILE="/tmp/cpu_stats_prev.txt"

# Get CPU usage per core by comparing two /proc/stat samples.
# Returns format: "avg:XX% c0:XX% c1:XX% ..."
get_cpu_usage() {
  local prev_stats curr_stats

  # Read current stats
  curr_stats="$(grep '^cpu' /proc/stat)"

  # If we have previous stats, calculate usage
  if [[ -f "$CPU_STATS_FILE" ]]; then
    prev_stats="$(cat "$CPU_STATS_FILE")"

    # Calculate per-CPU usage
    local result=""
    while IFS= read -r curr_line; do
      local cpu_name
      cpu_name="$(echo "$curr_line" | awk '{print $1}')"
      local prev_line
      prev_line="$(echo "$prev_stats" | grep "^${cpu_name} ")"

      if [[ -n "$prev_line" ]]; then
        # Extract values: user nice system idle iowait irq softirq steal
        local p_user p_nice p_sys p_idle p_iowait p_irq p_softirq p_steal
        local c_user c_nice c_sys c_idle c_iowait c_irq c_softirq c_steal
        read -r _ p_user p_nice p_sys p_idle p_iowait p_irq p_softirq p_steal _ <<< "$prev_line"
        read -r _ c_user c_nice c_sys c_idle c_iowait c_irq c_softirq c_steal _ <<< "$curr_line"

        # Calculate deltas
        local prev_total curr_total prev_idle_total curr_idle_total
        prev_idle_total=$(( p_idle + p_iowait ))
        curr_idle_total=$(( c_idle + c_iowait ))
        prev_total=$(( p_user + p_nice + p_sys + p_idle + p_iowait + p_irq + p_softirq + p_steal ))
        curr_total=$(( c_user + c_nice + c_sys + c_idle + c_iowait + c_irq + c_softirq + c_steal ))

        local total_delta idle_delta usage_pct
        total_delta=$(( curr_total - prev_total ))
        idle_delta=$(( curr_idle_total - prev_idle_total ))

        if [[ "$total_delta" -gt 0 ]]; then
          usage_pct=$(( (total_delta - idle_delta) * 100 / total_delta ))
        else
          usage_pct=0
        fi

        # Format: "cpu" -> "avg", "cpu0" -> "c0", etc.
        local label
        if [[ "$cpu_name" == "cpu" ]]; then
          label="avg"
        else
          label="${cpu_name/cpu/c}"
        fi
        result="${result}${label}:${usage_pct}% "
      fi
    done <<< "$curr_stats"

    echo "${result% }"  # trim trailing space
  else
    echo "n/a"
  fi

  # Save current stats for next iteration
  echo "$curr_stats" > "$CPU_STATS_FILE"
}

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

# Print goroutines grouped by stack trace, sorted by count (debug=1).
# This is more useful for identifying leaks since it shows which stacks have the most goroutines.
print_goroutine_summary() {
  local lines="${1:-200}"
  echo "--- Goroutines by Stack (top $lines lines, grouped and sorted by count) ---"
  curl -s --max-time 30 "http://${PPROF_HOST}/debug/pprof/goroutine?debug=1" 2>/dev/null | head -"$lines" || echo "(pprof endpoint not available)"
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

  # Get CPU usage per core (requires two samples to calculate delta).
  # Format: "cpu0:23% cpu1:45% ..." or just total if per-core not available.
  local cpu_usage
  cpu_usage="$(get_cpu_usage)"

  # Get processes with >=1% memory, format as "name (MB)"
  local top_procs
  top_procs="$(ps -eo %mem,rss,comm --sort=-%mem | awk 'NR>1 && $1>=1.0 {printf "%s (%dMB), ", $3, $2/1024}' | sed 's/, $//')"

  local timestamp
  timestamp="$(date '+%H:%M:%S')"

  # stdout preserves info in CI logs in case of crash; history file is used for snapshot.
  printf "%s mem=%s%% (%sMB) cpu=[%s] goroutines=%s procs=[%s]\n" \
    "$timestamp" "$pct" "$memused_mb" "$cpu_usage" "$goroutines" "$top_procs" | tee -a "$HISTORY_FILE"

  # Collect pprof analysis once per tick.
  local pprof_output
  pprof_output="$(print_pprof_analysis)"

  # If memory threshold was reached, print Go heap details and goroutine stacks to stdout. But only once per run.
  if [[ "$pct" -ge "$HIGH_MEMORY_THRESHOLD" ]] && [[ "$HEAP_PRINTED" == "false" ]]; then
    echo ""
    echo "=== HIGH MEMORY WARNING: ${pct}% used (threshold: ${HIGH_MEMORY_THRESHOLD}%) ==="
    if [[ -f /tmp/temporal_cluster_stats.txt ]]; then
      cat /tmp/temporal_cluster_stats.txt
      echo ""
    fi
    echo "$pprof_output"
    echo ""
    print_goroutine_summary 500
    echo "=== END HIGH MEMORY WARNING ==="
    echo ""
    HEAP_PRINTED=true
    GOROUTINE_DUMP_PRINTED=true  # Don't print again for high goroutine count
  fi

  # If goroutine count is too high, dump goroutine stacks to help debug leaks. But only once per run.
  if [[ "$goroutines" != "?" ]] && [[ "$goroutines" -ge "$HIGH_GOROUTINE_THRESHOLD" ]] && [[ "$GOROUTINE_DUMP_PRINTED" == "false" ]]; then
    echo ""
    echo "=== HIGH GOROUTINE WARNING: ${goroutines} goroutines (threshold: ${HIGH_GOROUTINE_THRESHOLD}) ==="
    if [[ -f /tmp/temporal_cluster_stats.txt ]]; then
      cat /tmp/temporal_cluster_stats.txt
      echo ""
    fi
    print_goroutine_summary 500
    echo "=== END HIGH GOROUTINE WARNING ==="
    echo ""
    GOROUTINE_DUMP_PRINTED=true
  fi

  # Write snapshot to disk.
  {
    echo "Resource snapshot at $(date '+%Y-%m-%d %H:%M:%S')"
    echo ""
    cat "$HISTORY_FILE"
    echo ""
    echo "--- Top Processes (by CPU) ---"
    ps -eo pid,%cpu,%mem,rss:10,comm --sort=-%cpu | head -15
    echo ""
    echo "--- Top Processes (by Memory) ---"
    ps -eo pid,%cpu,%mem,rss:10,comm --sort=-%mem | head -15
    echo ""
    echo "--- CPU Summary ---"
    lscpu 2>/dev/null | grep -E '^CPU\(s\)|^Model name|^CPU MHz' || nproc
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
