#!/bin/bash
#
# Monitor Test
#
# Runs the given command with background monitoring.
#
# Usage:
#   ./monitor_test.sh <command> [args...]
#
# Example:
#   ./monitor_test.sh make functional-test-coverage
#
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <command> [args...]" >&2
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MEMORY_OUTPUT_DIR=".testoutput/memory"
SNAPSHOT_FILE="$MEMORY_OUTPUT_DIR/snapshot.txt"
mkdir -p "$MEMORY_OUTPUT_DIR"

# Start monitor
SNAPSHOT_HISTORY_FILE="$MEMORY_OUTPUT_DIR/snapshot-history.txt" "$SCRIPT_DIR/memory_monitor.sh" "$SNAPSHOT_FILE" &
MONITOR_PID=$!
trap 'kill "$MONITOR_PID" 2>/dev/null' EXIT

# Run command
"$@"
