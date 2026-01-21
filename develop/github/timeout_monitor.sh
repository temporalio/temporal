#!/bin/bash
#
# Timeout Monitor
#
# Monitors a process and kills it gracefully when approaching a deadline.
# Sends SIGQUIT first to dump goroutine stacks, then SIGKILL if needed.
#
# Usage:
#   ./timeout_monitor.sh <pid> <timeout_minutes>
#
# Arguments:
#   pid             - Process ID to monitor
#   timeout_minutes - Total timeout in minutes
#
# Example:
#   ./timeout_monitor.sh 12345 35  # Kill process 12345 after ~34 minutes
#
set -euo pipefail

if [[ $# -lt 2 ]]; then
  echo "Usage: $0 <pid> <timeout_minutes>" >&2
  exit 1
fi

PID="$1"
TIMEOUT_MINUTES="$2"

# time to reserve before external timeout
BUFFER_SECONDS=30

DEADLINE_SECONDS=$((TIMEOUT_MINUTES * 60 - BUFFER_SECONDS))

while kill -0 "$PID" 2>/dev/null; do
  if [[ $SECONDS -ge $DEADLINE_SECONDS ]]; then
    echo "::warning::Timeout monitor: ~1 minute until timeout. Dumping goroutine stacks..."
    kill -QUIT "$PID" 2>/dev/null || true
    sleep 5  # allow time for stack dump to print

    if kill -0 "$PID" 2>/dev/null; then
      echo "::error::Process still running after SIGQUIT. Sending SIGKILL..."
      kill -KILL -"$PID" 2>/dev/null || kill -KILL "$PID" 2>/dev/null || true
    fi
    exit 0
  fi
  sleep 5
done
