#!/usr/bin/env bash
set -euo pipefail

# TemporalZFS Research Agent Demo — end-to-end runner
# Usage: ./run-demo.sh [--workflows N] [--concurrency N] [--failure-rate F] [--seed S]

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKFLOWS=200
CONCURRENCY=50
FAILURE_RATE=1.0
SEED=12345
DATA_DIR="/tmp/tfs-demo"
TEMPORAL_ADDR="localhost:7233"
TEMPORAL_PID=""
CONTINUOUS=""

# Parse flags.
while [[ $# -gt 0 ]]; do
  case $1 in
    --workflows)    WORKFLOWS="$2"; shift 2 ;;
    --concurrency)  CONCURRENCY="$2"; shift 2 ;;
    --failure-rate) FAILURE_RATE="$2"; shift 2 ;;
    --seed)         SEED="$2"; shift 2 ;;
    --data-dir)     DATA_DIR="$2"; shift 2 ;;
    --continuous)   CONTINUOUS="true"; shift ;;
    -h|--help)
      echo "Usage: $0 [--workflows N] [--concurrency N] [--failure-rate F] [--seed S] [--data-dir DIR] [--continuous]"
      exit 0
      ;;
    *) echo "Unknown flag: $1"; exit 1 ;;
  esac
done

DEMO_BIN="/tmp/research-demo-$$"
REPORT_HTML="${SCRIPT_DIR}/report.html"

cleanup() {
  echo ""
  echo "Cleaning up..."
  if [[ -n "$TEMPORAL_PID" ]] && kill -0 "$TEMPORAL_PID" 2>/dev/null; then
    kill "$TEMPORAL_PID" 2>/dev/null || true
    wait "$TEMPORAL_PID" 2>/dev/null || true
    echo "  Temporal dev server stopped."
  fi
  rm -f "$DEMO_BIN"
  echo "Done."
}
trap cleanup EXIT

# Colors.
BOLD="\033[1m"
CYAN="\033[36m"
GREEN="\033[32m"
YELLOW="\033[33m"
DIM="\033[2m"
RESET="\033[0m"

step() {
  echo ""
  echo -e "${BOLD}${CYAN}═══ $1 ═══${RESET}"
  echo ""
}

# ─────────────────────────────────────────────────────────────
step "Step 1: Build the demo"

echo "  Building from ${SCRIPT_DIR}..."
(cd "$SCRIPT_DIR" && go build -o "$DEMO_BIN" .)
echo -e "  ${GREEN}Build successful.${RESET}"

# ─────────────────────────────────────────────────────────────
step "Step 2: Start Temporal dev server"

if temporal workflow list --address "$TEMPORAL_ADDR" >/dev/null 2>&1; then
  echo -e "  ${YELLOW}Temporal server already running at ${TEMPORAL_ADDR}.${RESET}"
else
  echo "  Starting temporal server start-dev..."
  temporal server start-dev --port 7233 --ui-port 8233 2>/dev/null &
  TEMPORAL_PID=$!
  # Wait for server to be ready.
  for i in $(seq 1 30); do
    if temporal workflow list --address "$TEMPORAL_ADDR" >/dev/null 2>&1; then
      break
    fi
    sleep 1
  done
  if ! temporal workflow list --address "$TEMPORAL_ADDR" >/dev/null 2>&1; then
    echo "  ERROR: Temporal server failed to start after 30 seconds."
    exit 1
  fi
  echo -e "  ${GREEN}Temporal server ready.${RESET}"
fi
echo -e "  Temporal UI: ${CYAN}http://localhost:8233${RESET}"

# ─────────────────────────────────────────────────────────────
if [[ -n "$CONTINUOUS" ]]; then
  step "Step 3: Run research agent workflows (continuous mode — Ctrl+C to stop)"
else
  step "Step 3: Run ${WORKFLOWS} research agent workflows"
fi

rm -rf "$DATA_DIR"
echo -e "  ${DIM}Workflows: ${WORKFLOWS}  Concurrency: ${CONCURRENCY}  Failure rate: ${FAILURE_RATE}  Seed: ${SEED}${RESET}"
echo ""

RUN_FLAGS=(
  --concurrency "$CONCURRENCY"
  --failure-rate "$FAILURE_RATE"
  --seed "$SEED"
  --data-dir "$DATA_DIR"
)
if [[ -n "$CONTINUOUS" ]]; then
  RUN_FLAGS+=(--continuous)
else
  RUN_FLAGS+=(--workflows "$WORKFLOWS")
fi

"$DEMO_BIN" run "${RUN_FLAGS[@]}"

# ─────────────────────────────────────────────────────────────
step "Step 4: Temporal workflow list"

echo "  Total workflows in Temporal:"
temporal workflow count --address "$TEMPORAL_ADDR"
echo ""
echo "  Last 5 completed:"
temporal workflow list --address "$TEMPORAL_ADDR" --limit 5

# ─────────────────────────────────────────────────────────────
step "Step 5: Browse a workflow's filesystem"

echo -e "  ${DIM}Topic: quantum-computing${RESET}"
echo ""
"$DEMO_BIN" browse --data-dir "$DATA_DIR" --topic quantum-computing 2>/dev/null

# ─────────────────────────────────────────────────────────────
step "Step 6: Generate HTML report"

"$DEMO_BIN" report --data-dir "$DATA_DIR" --output "$REPORT_HTML" 2>/dev/null
echo -e "  Report: ${CYAN}${REPORT_HTML}${RESET}"
echo -e "  Size:   $(du -h "$REPORT_HTML" | cut -f1)"

# Open report if on macOS.
if command -v open &>/dev/null; then
  echo ""
  echo -e "  ${DIM}Opening report in browser...${RESET}"
  open "$REPORT_HTML"
fi

# ─────────────────────────────────────────────────────────────
step "Demo complete"

echo -e "  Data directory: ${DATA_DIR} ($(du -sh "$DATA_DIR" | cut -f1))"
echo -e "  HTML report:    ${REPORT_HTML}"
echo -e "  Temporal UI:    ${CYAN}http://localhost:8233${RESET}"
echo ""
echo -e "  ${DIM}To browse another topic:${RESET}"
echo "    $DEMO_BIN browse --data-dir $DATA_DIR --topic <slug>"
echo ""
echo -e "  ${DIM}To re-run with the live dashboard:${RESET}"
echo "    $DEMO_BIN run --workflows $WORKFLOWS --concurrency $CONCURRENCY --data-dir /tmp/tfs-demo-live"
