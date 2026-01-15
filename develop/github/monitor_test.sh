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

# Start monitor
"$SCRIPT_DIR/memory_monitor.sh" /tmp/memory_snapshot.txt &
MONITOR_PID=$!
trap 'kill "$MONITOR_PID" 2>/dev/null' EXIT

# Run command
"$@"
