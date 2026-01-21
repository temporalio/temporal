#!/bin/bash
#
# Monitor Test
#
# Runs the given command with background monitoring:
# (1) Memory monitor to track memory usage
# (2) Timeout monitor to terminate process shortly before timeout
#
# Usage:
#   ./monitor_test.sh <command> [args...]
#
# Environment variables:
#   TIMEOUT_MINUTES - Step timeout in minutes (required)
#
# Example:
#   TIMEOUT_MINUTES=35 ./monitor_test.sh make functional-test-coverage
#
set -euo pipefail

if [[ -z "${TIMEOUT_MINUTES:-}" ]]; then
  echo "Error: TIMEOUT_MINUTES environment variable is required" >&2
  exit 1
fi

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <command> [args...]" >&2
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Start memory monitor
"$SCRIPT_DIR/memory_monitor.sh" /tmp/memory_snapshot.txt &
MONITOR_PID=$!

# Run command in background so we can monitor it
"$@" &
CMD_PID=$!

cleanup() {
  kill "$MONITOR_PID" 2>/dev/null || true
  [[ -n "${TIMEOUT_MONITOR_PID:-}" ]] && kill "$TIMEOUT_MONITOR_PID" 2>/dev/null || true
}
trap cleanup EXIT

# Timeout monitor
"$SCRIPT_DIR/timeout_monitor.sh" "$CMD_PID" "$TIMEOUT_MINUTES" &
TIMEOUT_MONITOR_PID=$!

# Wait for command and capture exit code
wait "$CMD_PID" || EXIT_CODE=$?

exit "${EXIT_CODE:-0}"
