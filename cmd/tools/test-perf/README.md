# test-perf

A local CLI tool for running unit tests with performance analysis. It uses `gotestsum` to run tests, generates timing artifacts, produces a concise performance report, and emits Go execution traces for the slowest packages.

## Prerequisites

Install gotestsum:

```bash
go install gotest.tools/gotestsum@latest
```

## Usage

### Basic Usage

Run all tests with default settings:

```bash
go run ./cmd/tools/test-perf -- ./...
```

### With Options

```bash
# Specify package pattern and report limits
go run ./cmd/tools/test-perf --pkg ./service/... --topk 5 --topn 100

# Pass additional go test flags after --
go run ./cmd/tools/test-perf --pkg ./... -- -race -timeout 30m

# Skip the tracing phase (faster for quick analysis)
go run ./cmd/tools/test-perf --pkg ./... --skip-trace

# Custom output directory
go run ./cmd/tools/test-perf --output ./my-test-results -- ./...
```

### CLI Options

| Flag | Default | Description |
|------|---------|-------------|
| `--pkg` | `./...` | Package pattern to test |
| `--topk` | `3` | Number of slowest packages to trace |
| `--topn` | `50` | Number of slowest tests/packages to show in report |
| `--output` | `.testoutput` | Output directory for artifacts |
| `--skip-trace` | `false` | Skip tracing phase |

Arguments after `--` are passed directly to `go test`.

## Output Artifacts

All artifacts are written to `.testoutput/` (or your custom `--output` directory):

```
.testoutput/
├── junit.xml          # JUnit XML report (CI integration)
├── test2json.json     # Raw go test -json stream
├── report.md          # Markdown performance report
└── traces/            # Execution traces for slowest packages
    ├── go_temporal_io_server_service_history.trace
    ├── go_temporal_io_server_service_matching.trace
    └── ...
```

## Sample Output

```
=== Running tests with gotestsum ===
... test output ...

=== Analyzing test results ===

=== Generating traces for slowest packages ===
Tracing package go.temporal.io/server/service/history (45.23s elapsed)...
Tracing package go.temporal.io/server/service/matching (32.15s elapsed)...
Tracing package go.temporal.io/server/tests (28.91s elapsed)...

============================================================
TEST PERFORMANCE SUMMARY
============================================================

Status: PASS
Wall Time: 2m34.567s
Tests: 1234 passed, 0 failed, 12 skipped (total: 1246)

Parallelism:
  Max concurrent: 8
  Avg concurrency: 5.23
  Single-threaded: 12.4%

Top 5 Slowest Packages:
  1. go.temporal.io/server/service/history (45.23s)
  2. go.temporal.io/server/service/matching (32.15s)
  3. go.temporal.io/server/tests (28.91s)
  4. go.temporal.io/server/service/frontend (21.07s)
  5. go.temporal.io/server/common/persistence (15.43s)

Top 5 Slowest Tests:
  1. go.temporal.io/server/service/history/TestWorkflowExecution (12.34s)
  2. go.temporal.io/server/service/matching/TestTaskQueueManager (8.91s)
  3. go.temporal.io/server/tests/TestIntegration (7.23s)
  4. go.temporal.io/server/service/frontend/TestHandler (5.67s)
  5. go.temporal.io/server/common/persistence/TestCassandra (4.12s)

Artifacts:
  Report: .testoutput/report.md
  JUnit XML: .testoutput/junit.xml
  Raw JSON: .testoutput/test2json.json

Trace Files:
  go tool trace .testoutput/traces/go_temporal_io_server_service_history.trace
  go tool trace .testoutput/traces/go_temporal_io_server_service_matching.trace
  go tool trace .testoutput/traces/go_temporal_io_server_tests.trace
============================================================
```

## Viewing Traces

After the tool runs, open traces in the browser:

```bash
go tool trace .testoutput/traces/go_temporal_io_server_service_history.trace
```

This opens an interactive trace viewer showing:
- Goroutine scheduling
- GC activity
- System calls
- Network blocking
- Synchronization primitives

## Parallelism Metrics

The tool calculates several concurrency metrics from the test event stream:

- **Max Concurrent Tests**: Peak number of tests running simultaneously
- **Avg Concurrency (time-weighted)**: Average number of concurrent tests weighted by duration
- **Time at Concurrency=1**: Percentage of wall time where only one test was running (indicates serialization bottlenecks)

## Exit Codes

- `0`: All tests passed
- `1`: One or more tests failed (artifacts are still generated)

## Adding to .gitignore

You may want to add the output directory to your `.gitignore`:

```
.testoutput/
```
