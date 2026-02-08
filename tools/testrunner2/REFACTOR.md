# testrunner2 Refactoring Plan

## Codebase Overview

| File | Lines | Purpose |
|------|-------|---------|
| testrunner.go | 1300 | Core orchestration (too large) |
| package.go | 390 | Test discovery, filter patterns |
| junit.go | 400 | JUnit report generation and merging |
| log_parse.go | 381 | Alert parsing (race, panic, timeout) |
| log_capture.go | 268 | Output capture and filtering |
| event_stream.go | 227 | Test event streaming, stuck detection |
| args.go | 213 | CLI argument parsing |
| exec.go | 198 | Process execution |
| scheduler.go | 103 | Work queue |

---

## 1. Dead Code

### `buildRetryUnitExcluding()` (testrunner.go:1102-1125)
This function is defined but never called. The compiled-mode retry logic in
`compiledExecConfig` builds retry units inline using `buildRetryUnit` and
`buildRetryPlans`. Remove it.

### `describeUnit()` (testrunner.go:1016-1018)
Trivially returns `unit.label`. Either inline it or give it meaningful logic.

### `testCasesFromFiles()` (package.go:224-230)
Unused helper. No call sites exist. Remove it.

### `alert.Details` field (log_parse.go:22)
Populated by `tryParseDataRace`, `tryParsePanic`, `tryParseFatal`, and
`tryParseTimeout` but only ever read in `appendAlerts` where it's written to
JUnit `Data` field. The `appendAlerts` method doesn't use `Details` at all —
it only uses `Tests` and `Summary`. Check if `Details` is actually useful in
the JUnit alerts suite; if not, remove it.

---

## 2. Simplification Opportunities

### Split `testrunner.go` (~1300 lines)
The file mixes several concerns:

- **Console output** (`writeConsoleResult`, `formatWorkUnits*`): move to `console.go`
- **Retry logic** (`buildRetryHooks`, `buildRetryPlans`, `buildRetryUnit`,
  `filterEmitted`, `filterParentFailures`, quarantine helpers): move to `retry.go`
- **Compiled mode setup** (`compiledExecConfig`, `createCompileItems`,
  `newCompileItem`): move to `compiled.go`
- **Direct mode setup** (`runDirectMode`, `directExecConfig`): move to `direct.go`

This leaves `testrunner.go` with ~300 lines: `runner` struct, `Main`,
`runTests`, `newExecItem`, `finalizeReport`.

### Unify timeout calculation (testrunner.go:712-715, 840-843)
Identical code in `compiledExecConfig` and `directExecConfig`:
```go
timeout := r.timeout
if timeout == 0 {
    timeout = r.runTimeout
}
```
Extract to `(r *runner) effectiveTimeout() time.Duration`.

### Consolidate `onCommand` callbacks (exec.go:60, 122, 164)
All three exec functions accept `onCommand func(string)` with identical
nil-check + call pattern. Consider making it a field on the input structs or
using a wrapper that handles the nil check.

### Simplify `newExecItem` (testrunner.go:399-554)
This 155-line closure handles 7 distinct phases. Consider extracting:
1. `runTestProcess(cfg) → (testResults, alerts, commandResult)` — phases 1-4
2. `handleTestResult(cfg, results, alerts) → retryItems` — phases 5-7

This also makes the retry logic independently testable.

### Replace string-key deduplication (log_parse.go:35)
```go
key := string(alert.Kind) + "\n" + alert.Summary + "\n" + strings.Join(alert.Tests, ",")
```
Use a struct key instead:
```go
type alertKey struct{ kind failureKind; summary string; tests string }
```
This avoids fragile string concatenation that could break if test names contain
newlines or commas.

---

## 3. Design Issues

### Double-parsing of test output (junit.go:300-349, 381-389)
`newJUnitReport` parses the output once for JUnit XML (line 304), then
`extractResultsClean` parses it again with markers stripped (line 314) to
extract `testResults`. This is because test2json markers prevent the first
parse from correctly attributing output to tests.

**Fix**: Parse once with markers stripped, then use that single `gtr.Report`
for both JUnit generation and result extraction. The JUnit generation only
needs the parsed report, not the raw marker-prefixed output.

### `extractResults` vs `extractResultsClean` (junit.go:354-389)
`extractResults` takes a `gtr.Report` and extracts failures/passes.
`extractResultsClean` strips markers, re-parses, and calls `extractResults`.
The `extractResults` function is only called from `extractResultsClean`.

**Fix**: Inline `extractResults` into `extractResultsClean` since it's only
used in one place, or rename to clarify the relationship.

### Mixed execution model in `execConfig` (testrunner.go:376-396)
The struct serves both compiled mode (per-test binary) and direct mode (full
`go test`). Fields like `logHeader` are only used in compiled mode, and
`streamRetries` is always `false` for compiled / `true` for direct.

This isn't a critical issue since the struct is internal, but documenting which
fields apply to which mode (or using a mode-specific wrapper) would improve
readability.

---

## 4. Naming Improvements

### Test name parsing functions (log_parse.go)
Current names are inconsistent:
- `parseTripleDashTestName` (line 150)
- `parseFullyQualifiedTestName` (line 167)
- `parsePlainTestName` (line 183)

Better: `parseFromFailLine`, `parseFromQualifiedName`, `parseFromCallSite` —
indicating the _input_ format rather than the parsing mechanism.

### Variable abbreviations (testrunner.go)
- `tp` for `testPackage` (line 225) — use `pkg` or `testPkg`
- `lc` for `logCapture` (line 405) — use `logCap` or `capture`
- `wu` for `workUnit` (line 735) — use `unit`

### `groupByMode` method (package.go:86)
Name suggests "group by mode" but it's "get work units for this mode".
Better: `workUnits(mode)` or `unitsByMode(mode)`.

---

## 5. Robustness

### Event stream line parsing (event_stream.go:87-114)
Parses `=== RUN`, `--- PASS:`, etc. by splitting on whitespace and accessing
`fields[2]` without bounds checking. If Go's test output format ever changes
or produces truncated lines, this silently drops events.

**Fix**: Add `len(fields) >= 3` check before each case (the function already
returns early for `len(fields) < 3`, but the check is only at the top — the
switch cases assume it).

Actually, on review, the `len(fields) < 3` guard at line 92-93 covers all
cases. This is fine. No change needed.

### Inconsistent file close error handling
Multiple places use `defer func() { _ = f.Close() }()` (junit.go:29, 67;
log_capture.go). This is acceptable for read-only operations, but for writes
(junit.go:67, log_capture.go:210), silently ignoring close errors could mask
data loss. Consider logging close errors in write paths.

---

## 6. Test Coverage Gaps

### `buildRetryPlans` (testrunner.go:1046)
Complex quarantine logic with multiple code paths. Has good unit tests in
`scheduler_test.go` (`TestBuildRetryPlans*`) but doesn't test the interaction
with `buildTestFilterPattern` (i.e., verifying the generated skip patterns
actually work with Go's test framework).

### `parseAlerts` (log_parse.go:82)
Well-tested in `log_parse_test.go`, but the `tryParseTimeout` function creates
individual alerts per stuck test — the behavior when multiple tests time out
simultaneously could use a dedicated test.

### Direct mode retry with overlapping failures
The `TestIntegration/direct_mode:_multiple_failures_retried_without_file_collision`
test was added but tests a narrow case (exactly 2 packages). Consider adding a
test with 3+ concurrent failures to verify the `directRetrySeq` counter scales.

---

## 7. Suggested Refactoring Order

1. **Remove dead code** (low risk): `buildRetryUnitExcluding`, `testCasesFromFiles`,
   inline `describeUnit`
2. **Split testrunner.go** (medium risk): Extract retry, console, compiled, direct
   into separate files — pure file moves, no logic changes
3. **Extract `effectiveTimeout()`** (low risk): Deduplicate the 2 identical blocks
4. **Simplify `newExecItem`** (medium risk): Extract `runTestProcess` and
   `handleTestResult` — needs careful testing since this is the core execution path
5. **Unify JUnit parsing** (medium risk): Single-parse approach for
   `newJUnitReport` + `extractResults`
6. **Rename functions** (low risk): Improve naming consistency across the codebase
