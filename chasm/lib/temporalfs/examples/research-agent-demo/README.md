# TemporalFS Research Agent Demo

A scale demo of AI research agent workflows using TemporalFS — a durable, versioned
filesystem for agent workflows. Each workflow simulates a 5-step research pipeline
that writes files and MVCC snapshots through TemporalFS, with injected random failures
handled automatically by Temporal's retry mechanism.

## What It Does

Each workflow runs 5 activities in sequence:

| Step | Activity | Writes | Failure Rate |
|------|----------|--------|--------------|
| 1 | **WebResearch** | 3-5 source files in `/research/{topic}/sources/` | 20% |
| 2 | **Summarize** | `summary.md` | 15% |
| 3 | **FactCheck** | `fact-check.md` | 10% |
| 4 | **FinalReport** | `report.md` | 10% |
| 5 | **PeerReview** | `review.md` | 5% |

After each step, a named MVCC snapshot is created (e.g., `step-1-research`,
`step-2-summary`). Every workflow gets its own isolated TemporalFS partition backed
by a shared PebbleDB instance.

## Prerequisites

- Go 1.23+
- [Temporal CLI](https://docs.temporal.io/cli) (`temporal server start-dev`)

## Quick Start

```bash
# Terminal 1: Start the Temporal dev server
temporal server start-dev

# Terminal 2: Run the demo (200 workflows, live dashboard)
cd chasm/lib/temporalfs/examples/research-agent-demo
go run . run --workflows 200 --concurrency 50
```

The live terminal dashboard shows real-time progress, retry counts, throughput
metrics, and an activity feed. Open http://localhost:8233 to see workflows in the
Temporal UI.

## Commands

### `run` — Execute workflows with live dashboard

```
go run . run [flags]
```

| Flag | Default | Description |
|------|---------|-------------|
| `--workflows` | 200 | Number of research workflows to run |
| `--concurrency` | 50 | Max concurrent workflows |
| `--failure-rate` | 1.0 | Failure rate multiplier (0 = none, 2 = double) |
| `--data-dir` | /tmp/tfs-demo | PebbleDB data directory |
| `--seed` | 0 | Random seed (0 = random) |
| `--task-queue` | research-demo | Temporal task queue name |
| `--temporal-addr` | localhost:7233 | Temporal server address |
| `--no-dashboard` | false | Disable live terminal dashboard |

### `report` — Generate HTML report

```bash
go run . report --data-dir /tmp/tfs-demo --output demo-report.html
open demo-report.html
```

Produces a self-contained HTML file with:
- Run summary (workflows, files, snapshots, data volume)
- Workflow table with file counts and snapshot counts
- Expandable filesystem explorer showing file contents and snapshots

### `browse` — Inspect a workflow's filesystem

```bash
go run . browse --data-dir /tmp/tfs-demo --topic quantum-computing
```

Prints the directory tree for a specific workflow's TemporalFS partition, including
file sizes and snapshot names.

## Demo Script

### Setup (30 seconds)

```bash
# Terminal 1
temporal server start-dev

# Terminal 2
cd chasm/lib/temporalfs/examples/research-agent-demo
```

### Run (2-3 minutes)

```bash
go run . run --workflows 200 --concurrency 50
```

While running:
- Watch the live dashboard fill up with progress, retries, and throughput stats
- Open http://localhost:8233 to see workflows in the Temporal UI
- Click any workflow to see the activity timeline with retry attempts

### After Completion

```bash
# Generate and open HTML report
go run . report --output demo-report.html
open demo-report.html

# Browse a specific workflow's filesystem
go run . browse --topic quantum-computing
```

### Key Demo Points

- **Durability**: Kill the process mid-run, restart — workflows resume from last snapshot
- **Scale**: 200 workflows, 50 concurrent, thousands of files, single PebbleDB
- **Versioning**: Each activity creates an MVCC snapshot; browse them in the report
- **Failure resilience**: Random failures are retried automatically by Temporal
- **Temporal UI**: Full workflow history with retries and timing at http://localhost:8233

## Architecture

```
temporal server start-dev
        |
        v
+-------------------+     +---------------------------+
|  Scale Runner     |---->|  Temporal Server (local)  |
|  (starts N wfs)   |     |  - Workflow history        |
+-------------------+     |  - Retry scheduling        |
        |                 |  - Web UI (:8233)           |
        v                 +-------------+--------------+
+-------------------+                   |
|  Live Dashboard   |<--   +------------v--------------+
|  (terminal TUI)   |     |  Worker (activities)       |
+-------------------+     |  - 5 activities per wf     |
                          |  - Random failure injection |
                          |  - TemporalFS file I/O     |
                          +------------+---------------+
                                       |
                          +------------v---------------+
                          |  PebbleDB (shared)         |
                          |  - PrefixedStore per wf    |
                          |  - MVCC snapshots          |
                          +----------------------------+
```

## File Structure

| File | Description |
|------|-------------|
| `main.go` | Entry point with `run`, `report`, `browse` subcommands |
| `workflow.go` | Temporal workflow definition chaining 5 activities |
| `activities.go` | Activity implementations with FS ops + failure injection |
| `content.go` | Template-based markdown content generators |
| `topics.go` | 120+ research topics with display names and slugs |
| `runner.go` | Scale runner — starts N workflows via Temporal SDK |
| `dashboard.go` | Live ANSI terminal dashboard (no external deps) |
| `report.go` | Post-run HTML report generator |
| `store.go` | Shared PebbleDB wrapper + manifest management |
