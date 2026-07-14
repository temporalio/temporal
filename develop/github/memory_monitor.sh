#!/bin/bash
#
# Memory Monitor
#
# 1. Snapshot status:
#       Samples memory every SNAPSHOT_INTERVAL_SECONDS and writes every sample
#       to SNAPSHOT_HISTORY_FILE. Logs status every SNAPSHOT_PRINT_INTERVAL_SECONDS
#       and writes the highest-memory snapshot report to SNAPSHOT_FILE.
#
# 2. Diagnostic capture:
#       When usage crosses HEAP_PROFILE_CAPTURE_THRESHOLD, captures a heap
#       profile in HEAP_PROFILES_DIR and process/runtime diagnostics in
#       RUNTIME_DIAGNOSTICS_DIR before running analysis.
#
# 3. OOM prevention:
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
readonly CLUSTER_EVENTS_FILE="${CLUSTER_EVENTS_FILE:-${TEMPORAL_TEST_CLUSTER_EVENTS_FILE:-.testoutput/test-cluster-events.jsonl}}"

# Heap profile config.
# Capture before the termination threshold so the diagnostic profile is usually
# available even if the runner kills the job before our termination path runs.
readonly HEAP_PROFILE_CAPTURE_THRESHOLD="${HEAP_PROFILE_CAPTURE_THRESHOLD:-90}"
readonly HEAP_PROFILE_REFRESH_INTERVAL_SECONDS="${HEAP_PROFILE_REFRESH_INTERVAL_SECONDS:-30}"
readonly HEAP_PROFILES_DIR="${HEAP_PROFILES_DIR:-$SNAPSHOT_DIR/heap-profiles}"
readonly RUNTIME_DIAGNOSTICS_DIR="${RUNTIME_DIAGNOSTICS_DIR:-$SNAPSHOT_DIR/runtime-diagnostics}"
readonly PPROF_HOST="${PPROF_HOST:-localhost:7000}"

# OOM prevention config.
# Terminate late enough to avoid masking near-finished tests, but before the
# runner OOM killer skips post-test artifact upload.
readonly OOM_TERMINATION_THRESHOLD="${OOM_TERMINATION_THRESHOLD:-99}"
readonly OOM_JUNIT_FILE="${OOM_JUNIT_FILE:-.testoutput/junit.oom.xml}"

# State.
MEMORY_HIGH_WATER_MARK=0
LAST_SNAPSHOT_PRINT_TIME=0
LAST_HEAP_PROFILE_CAPTURE_TIME=0
HEAP_PROFILE_SECTION=""
RUNTIME_DIAGNOSTICS_SECTION=""
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

monitored_process_pids() {
  if [[ -z "${MONITORED_PROCESS_GROUP:-}" ]]; then
    return
  fi

  ps -e -o pid=,pgid= | awk -v pgid="$MONITORED_PROCESS_GROUP" '$2 == pgid {print $1}'
}

top_rss_pids() {
  ps -eo pid= --sort=-rss | head -5
}

diagnostic_pids() {
  {
    monitored_process_pids
    top_rss_pids
  } | awk 'NF && !seen[$1]++ {print $1}' | head -10
}

print_proc_file() {
  local pid="$1"
  local file="$2"
  local path="/proc/$pid/$file"

  echo "--- $path ---"
  if [[ -r "$path" ]]; then
    cat "$path"
  else
    echo "(not available)"
  fi
}

proc_comm() {
  local pid="$1"

  if [[ -r "/proc/$pid/comm" ]]; then
    tr -d '\n' < "/proc/$pid/comm"
  else
    echo "pid$pid"
  fi
}

safe_file_part() {
  tr -c '[:alnum:]_.-' '_' | sed 's/_$//'
}

print_smaps_summary() {
  local smaps_file="$1"

  echo "Process memory totals:"
  awk '
    /^(Size|Rss|Pss|Shared_Clean|Shared_Dirty|Private_Clean|Private_Dirty|Referenced|Anonymous|AnonHugePages|Swap):/ {
      field = $1
      sub(/:$/, "", field)
      totals[field] += $2
    }
    END {
      fields = "Size Rss Pss Shared_Clean Shared_Dirty Private_Clean Private_Dirty Referenced Anonymous AnonHugePages Swap"
      field_count = split(fields, ordered_fields, " ")
      printf "%18s %12s\n", "field", "kb"
      for (i = 1; i <= field_count; i++) {
        field = ordered_fields[i]
        printf "%18s %12d\n", field, totals[field]
      }
    }
  ' "$smaps_file"

  echo
  echo "Top mappings by private dirty memory:"
  awk '
    function flush() {
      if (name == "") {
        return
      }
      size_by_name[name] += size
      rss_by_name[name] += rss
      private_dirty_by_name[name] += private_dirty
      anonymous_by_name[name] += anonymous
      count_by_name[name]++
    }
    function mapping_name(  i, n) {
      if (NF <= 5) {
        return "[anon]"
      }
      n = $6
      for (i = 7; i <= NF; i++) {
        n = n " " $i
      }
      return n
    }
    /^[0-9a-fA-F]+-[0-9a-fA-F]+ / {
      flush()
      name = mapping_name()
      size = 0
      rss = 0
      private_dirty = 0
      anonymous = 0
      next
    }
    /^Size:/ { size = $2; next }
    /^Rss:/ { rss = $2; next }
    /^Private_Dirty:/ { private_dirty = $2; next }
    /^Anonymous:/ { anonymous = $2; next }
    END {
      flush()
      for (name in private_dirty_by_name) {
        printf "%d\t%d\t%d\t%d\t%d\t%s\n",
          private_dirty_by_name[name],
          rss_by_name[name],
          anonymous_by_name[name],
          size_by_name[name],
          count_by_name[name],
          name
      }
    }
  ' "$smaps_file" \
    | sort -nr -k1,1 \
    | awk -F '\t' '
      BEGIN {
        printf "%12s %12s %12s %12s %8s %s\n", "private_kb", "rss_kb", "anon_kb", "size_kb", "count", "mapping"
      }
      NR <= 30 {
        printf "%12d %12d %12d %12d %8d %s\n", $1, $2, $3, $4, $5, $6
      }
    '

  echo
  echo "Top anonymous address regions by private dirty memory:"
  awk '
    function flush() {
      if (addr == "" || name != "[anon]") {
        return
      }
      split(addr, parts, "-")
      region = substr(parts[1], 1, 3) "*"
      size_by_region[region] += size
      rss_by_region[region] += rss
      private_dirty_by_region[region] += private_dirty
      count_by_region[region]++
    }
    function mapping_name(  i, n) {
      if (NF <= 5) {
        return "[anon]"
      }
      n = $6
      for (i = 7; i <= NF; i++) {
        n = n " " $i
      }
      return n
    }
    /^[0-9a-fA-F]+-[0-9a-fA-F]+ / {
      flush()
      addr = $1
      name = mapping_name()
      size = 0
      rss = 0
      private_dirty = 0
      next
    }
    /^Size:/ { size = $2; next }
    /^Rss:/ { rss = $2; next }
    /^Private_Dirty:/ { private_dirty = $2; next }
    END {
      flush()
      for (region in private_dirty_by_region) {
        printf "%d\t%d\t%d\t%d\t%s\n",
          private_dirty_by_region[region],
          rss_by_region[region],
          size_by_region[region],
          count_by_region[region],
          region
      }
    }
  ' "$smaps_file" \
    | sort -nr -k1,1 \
    | awk -F '\t' '
      BEGIN {
        printf "%12s %12s %12s %8s %s\n", "private_kb", "rss_kb", "size_kb", "count", "region"
      }
      NR <= 30 {
        printf "%12d %12d %12d %8d %s\n", $1, $2, $3, $4, $5
      }
    '

  echo
  echo "Top anonymous VM flag groups by private dirty memory:"
  awk '
    function flush() {
      if (addr == "" || name != "[anon]") {
        return
      }
      size_by_flags[flags] += size
      rss_by_flags[flags] += rss
      private_dirty_by_flags[flags] += private_dirty
      count_by_flags[flags]++
    }
    function mapping_name(  i, n) {
      if (NF <= 5) {
        return "[anon]"
      }
      n = $6
      for (i = 7; i <= NF; i++) {
        n = n " " $i
      }
      return n
    }
    /^[0-9a-fA-F]+-[0-9a-fA-F]+ / {
      flush()
      addr = $1
      name = mapping_name()
      size = 0
      rss = 0
      private_dirty = 0
      flags = "(none)"
      next
    }
    /^Size:/ { size = $2; next }
    /^Rss:/ { rss = $2; next }
    /^Private_Dirty:/ { private_dirty = $2; next }
    /^VmFlags:/ {
      flags = $0
      sub(/^VmFlags:[[:space:]]*/, "", flags)
      next
    }
    END {
      flush()
      for (flags in private_dirty_by_flags) {
        printf "%d\t%d\t%d\t%d\t%s\n",
          private_dirty_by_flags[flags],
          rss_by_flags[flags],
          size_by_flags[flags],
          count_by_flags[flags],
          flags
      }
    }
  ' "$smaps_file" \
    | sort -nr -k1,1 \
    | awk -F '\t' '
      BEGIN {
        printf "%12s %12s %12s %8s %s\n", "private_kb", "rss_kb", "size_kb", "count", "vm_flags"
      }
      NR <= 30 {
        printf "%12d %12d %12d %8d %s\n", $1, $2, $3, $4, $5
      }
    '

  echo
  echo "Top individual VMAs by private dirty memory:"
  awk '
    function flush() {
      if (addr == "") {
        return
      }
      printf "%d\t%d\t%d\t%d\t%s\t%s\n", private_dirty, rss, anonymous, size, addr, name
    }
    function mapping_name(  i, n) {
      if (NF <= 5) {
        return "[anon]"
      }
      n = $6
      for (i = 7; i <= NF; i++) {
        n = n " " $i
      }
      return n
    }
    /^[0-9a-fA-F]+-[0-9a-fA-F]+ / {
      flush()
      addr = $1
      name = mapping_name()
      size = 0
      rss = 0
      private_dirty = 0
      anonymous = 0
      next
    }
    /^Size:/ { size = $2; next }
    /^Rss:/ { rss = $2; next }
    /^Private_Dirty:/ { private_dirty = $2; next }
    /^Anonymous:/ { anonymous = $2; next }
    END { flush() }
  ' "$smaps_file" \
    | sort -nr -k1,1 \
    | awk -F '\t' '
      BEGIN {
        printf "%12s %12s %12s %12s %-39s %s\n", "private_kb", "rss_kb", "anon_kb", "size_kb", "address", "mapping"
      }
      NR <= 30 {
        printf "%12d %12d %12d %12d %-39s %s\n", $1, $2, $3, $4, $5, $6
      }
    '
}

save_proc_mapping_file() {
  local pid="$1"
  local proc_file="$2"
  local output_file="$3"
  local path="/proc/$pid/$proc_file"

  if [[ ! -r "$path" ]]; then
    return
  fi

  cat "$path" > "$output_file" 2>/dev/null || true
}

save_proc_snapshot_files() {
  local pid="$1"
  local output_prefix="$2"

  save_proc_mapping_file "$pid" "status" "${output_prefix}-status.txt"
  save_proc_mapping_file "$pid" "statm" "${output_prefix}-statm.txt"
  save_proc_mapping_file "$pid" "limits" "${output_prefix}-limits.txt"
  save_proc_mapping_file "$pid" "smaps_rollup" "${output_prefix}-smaps-rollup.txt"
}

capture_proc_mapping_diagnostics() {
  local diagnostics_path_prefix="$1"

  echo "--- Process Mapping Diagnostics ---"
  for pid in $(diagnostic_pids); do
    local comm safe_comm output_prefix smaps_file summary_file
    comm="$(proc_comm "$pid")"
    safe_comm="$(printf "%s" "$comm" | safe_file_part)"
    output_prefix="${diagnostics_path_prefix}-pid${pid}-${safe_comm}"
    smaps_file="${output_prefix}-smaps.txt"
    summary_file="${output_prefix}-smaps-summary.txt"

    echo
    echo "pid $pid ($comm)"
    save_proc_snapshot_files "$pid" "$output_prefix"
    save_proc_mapping_file "$pid" "maps" "${output_prefix}-maps.txt"
    save_proc_mapping_file "$pid" "smaps" "$smaps_file"
    save_proc_mapping_file "$pid" "numa_maps" "${output_prefix}-numa-maps.txt"
    if command -v pmap >/dev/null 2>&1; then
      pmap -x "$pid" > "${output_prefix}-pmap-x.txt" 2>/dev/null || true
    fi

    echo "Saved full mapping artifacts with prefix ${output_prefix}"
    if [[ -s "$smaps_file" ]]; then
      print_smaps_summary "$smaps_file" > "$summary_file"
      cat "$summary_file"
    else
      echo "(smaps not available)"
    fi
  done
}

print_runtime_memstats() {
  local heap_debug_file="$1"

  echo "--- Go runtime.MemStats ---"
  if [[ ! -s "$heap_debug_file" ]]; then
    echo "(heap debug profile not available)"
    return
  fi

  if ! grep -q '^# runtime.MemStats' "$heap_debug_file"; then
    echo "(runtime.MemStats section not found; full heap debug profile saved to $heap_debug_file)"
    return
  fi

  awk '
    /^# runtime.MemStats/ {inside = 1}
    inside {print}
  ' "$heap_debug_file" | head -120
}

print_process_diagnostics() {
  echo "--- Monitored Process Group ---"
  if [[ -n "${MONITORED_PROCESS_GROUP:-}" ]]; then
    ps -e -o pid,ppid,pgid,%mem,rss:10,vsz:10,stat,comm,args | awk -v pgid="$MONITORED_PROCESS_GROUP" 'NR == 1 || $3 == pgid'
  else
    echo "(MONITORED_PROCESS_GROUP is not set)"
  fi

  echo
  echo "--- Top RSS Processes ---"
  ps -eo pid,ppid,pgid,%mem,rss:10,vsz:10,stat,comm,args --sort=-rss | head -10

  for pid in $(diagnostic_pids); do
    echo
    print_proc_file "$pid" "status"
    echo
    print_proc_file "$pid" "smaps_rollup"
  done
}

print_cluster_event_summary() {
  echo "--- Cluster Events ---"
  if [[ ! -s "$CLUSTER_EVENTS_FILE" ]]; then
    echo "(cluster events not available at $CLUSTER_EVENTS_FILE)"
    return
  fi

  if ! command -v jq >/dev/null 2>&1; then
    echo "(jq not available; showing raw recent events)"
    tail -20 "$CLUSTER_EVENTS_FILE"
    return
  fi

  echo "Top cluster creations by suite/kind/reason:"
  jq -r 'select(.event == "created") | [.suite, .kind, .reason] | @tsv' "$CLUSTER_EVENTS_FILE" \
    | sort \
    | uniq -c \
    | sort -nr \
    | head -20 \
    || true

  echo
  echo "Recent cluster lifecycle events:"
  tail -200 "$CLUSTER_EVENTS_FILE" \
    | jq -r '
      [
        .timestamp,
        .event,
        ("cluster=" + (.cluster_id|tostring)),
        ("seq=" + (.cluster_sequence|tostring)),
        .suite,
        .cluster_kind,
        .cluster_reason,
        ("created=" + (.clusters_created|tostring)),
        ("live=" + (.live_clusters|tostring)),
        ("rssMB=" + (((.proc_VmRSS_kb // 0) / 1024)|floor|tostring)),
        ("heapAllocMB=" + (((.heap_alloc_bytes // 0) / 1048576)|floor|tostring)),
        ("heapSysMB=" + (((.heap_sys_bytes // 0) / 1048576)|floor|tostring)),
        ("stackSysMB=" + (((.stack_sys_bytes // 0) / 1048576)|floor|tostring)),
        .test
      ] | @tsv
    ' \
    | tail -25 \
    || true
}

capture_runtime_diagnostics() {
  local memory_pct="$1"
  local diagnostics_path_prefix heap_debug_file diagnostics_file

  diagnostics_path_prefix="$RUNTIME_DIAGNOSTICS_DIR/$(date '+%Y%m%d-%H%M%S')-${memory_pct}pct"
  mkdir -p "$RUNTIME_DIAGNOSTICS_DIR"
  heap_debug_file="${diagnostics_path_prefix}-heap-debug.txt"
  diagnostics_file="${diagnostics_path_prefix}.txt"

  fetch_pprof "heap?debug=1&gc=0" "$heap_debug_file" || true

  {
    echo "Runtime diagnostics at $(date '+%Y-%m-%d %H:%M:%S') (usage ${memory_pct}%)"
    echo
    print_runtime_memstats "$heap_debug_file"
    echo
    echo "--- System Memory Summary ---"
    free -m
    echo
    echo "--- /proc/meminfo ---"
    grep -E '^(MemTotal|MemFree|MemAvailable|Buffers|Cached|SwapTotal|SwapFree|CommitLimit|Committed_AS|VmallocTotal|VmallocUsed|AnonHugePages|Shmem|Slab|SReclaimable|SUnreclaim)' /proc/meminfo || true
    echo
    echo "--- Monitor Process Status ---"
    print_proc_file "$$" "status"
    echo
    print_process_diagnostics
    echo
    capture_proc_mapping_diagnostics "$diagnostics_path_prefix"
  } > "$diagnostics_file"

  echo "--- Runtime Diagnostics ---"
  echo "Saved runtime diagnostics to $diagnostics_file"
  grep -E '^(# (Alloc|Sys|HeapSys|HeapReleased|HeapInuse|StackSys|MSpanSys|MCacheSys|BuckHashSys|GCSys|OtherSys) =|VmRSS:|RssAnon:|RssFile:|RssShmem:|VmData:|VmSwap:|Threads:)' "$diagnostics_file" | head -40 || true
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
  local optional_runtime_diagnostics_section="$3"

  ensure_snapshot_dirs
  cat > "$SNAPSHOT_FILE" <<EOF
Memory snapshot at $(date '+%Y-%m-%d %H:%M:%S') (usage ${memory_pct}%)

$(tail -120 "$SNAPSHOT_HISTORY_FILE")

--- Top Processes ---
$(ps -eo pid,%mem,rss:10,comm --sort=-rss | head -10)

--- Memory Summary ---
$(free -m)

$(print_cluster_event_summary)

$optional_heap_profile_section

$optional_runtime_diagnostics_section
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
  local memory_total_kb memory_available_kb memory_used_kb memory_used_mb memory_pct is_new_high should_terminate_process_group

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

  if should_capture_heap_profile "$now" "$memory_pct" "$is_new_high" "$should_terminate_process_group"; then
    HEAP_PROFILE_SECTION="$(capture_heap_profile "$memory_pct")"
    RUNTIME_DIAGNOSTICS_SECTION="$(capture_runtime_diagnostics "$memory_pct")"
    LAST_HEAP_PROFILE_CAPTURE_TIME="$now"
    HAS_CAPTURED_HEAP_PROFILE=true
  fi

  # Write the snapshot only at new memory highs so the final artifact represents
  # the worst observed point without emitting one file per sample.
  if [[ "$is_new_high" == "true" ]] || [[ ! -e "$SNAPSHOT_FILE" ]]; then
    write_snapshot_report "$memory_pct" "$HEAP_PROFILE_SECTION" "$RUNTIME_DIAGNOSTICS_SECTION"
    MEMORY_HIGH_WATER_MARK="$memory_pct"
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
