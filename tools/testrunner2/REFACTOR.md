# testrunner2 Refactoring Plan

## Codebase Overview

| File | Lines | Purpose |
|------|-------|---------|
| testrunner.go | 530 | Core orchestration (runner, Main, newExecItem, retry dispatch) |
| retry.go | 330 | Retry logic (retryHandler, plans, quarantine, helpers) |
| compiled.go | 190 | Compiled mode (compiledExecConfig, compile items) |
| direct.go | 130 | Direct mode (runDirectMode, directExecConfig) |
| console.go | 110 | Console output formatting |
| package.go | 380 | Test discovery, filter patterns |
| junit.go | 420 | JUnit report generation, merging, validation |
| log_parse.go | 380 | Alert parsing (race, panic, timeout) |
| log_capture.go | 268 | Output capture and filtering |
| event_stream.go | 235 | Test event streaming, stuck detection |
| args.go | 213 | CLI argument parsing |
| exec.go | 198 | Process execution |
| scheduler.go | 103 | Work queue |

---

## Completed

### 1. Dead Code Removal ✓
- Removed `buildRetryUnitExcluding()` (dead in production, tests removed)
- Removed `testCasesFromFiles()` (no call sites)
- Inlined `describeUnit()` (trivially returned `unit.label`)
- Note: `testCasesToRunPattern` was listed as dead but is actually used in exec.go

### 2. Split `testrunner.go` ✓
Extracted into separate files by concern:
- `retry.go` — retry plans, quarantine, helper functions
- `compiled.go` — compiled mode exec config, compile items
- `direct.go` — direct mode exec config, runDirectMode
- `console.go` — writeConsoleResult, formatWorkUnits

testrunner.go reduced from 1300 → 500 lines.

### 3. Extract `effectiveTimeout()` ✓
Deduplicated identical timeout calculation in `compiledExecConfig` and
`directExecConfig` into `(r *runner) effectiveTimeout() time.Duration`.

### 4. Remove `alert.Details` field ✓
Removed write-only field from `alert` struct. Never read in production;
only populated by tryParse* functions and written to JUnit Data field
(which was already constructed from `alert.Tests` instead).

### 5. Unify `tryParsePanic`/`tryParseFatal` ✓
Extracted shared logic into `tryParsePrefixedAlert(lines, i, line, prefix, kind)`
helper. Both callers now delegate after their prefix check.

### 6. Add `testEventAction` type constants ✓
Replaced magic strings `"run"`, `"pass"`, `"fail"`, `"skip"` with typed constants
`actionRun`, `actionPass`, `actionFail`, `actionSkip`.

### 7. Fix swallowed error in `logCapture.Close()` ✓
Previously, when `Sync()` failed, `Close()` error was silently discarded.
Now both errors are captured and the sync error takes precedence.

### 8. Rename `buildRetryUnit` → `buildRetryUnitFromFailures` ✓
Clarifies that this function builds a work unit from failed test cases.

### 9. Unify retry callbacks into `retryHandler` struct ✓
Replaced three separate function fields (`retryForFailures`, `retryForCrash`,
`retryForUnknown`) in `execConfig` with a single `retryHandler` struct.
Renamed `buildRetryHooks` → `buildRetryHandler`.

### 10. Extract merge validation from `mergeReports()` ✓
Moved retry validation logic into `validateRetries()` function.
`mergeReports()` no longer needs `nolint:revive` annotation.

### 11. Split `newExecItem` into smaller functions ✓
Extracted from the 155-line closure:
- `midStreamRetryHandler()` — event handler for direct-mode stream retries
- `collectAlerts()` — parse output alerts + stuck test alerts
- `collectJUnitResult()` — read JUnit report + classify outcome
- `emitPostExitRetries()` — post-exit retry decision logic

### 12. Remove `goto` in `extractErrorTrace` ✓
Extracted `isTestifySection()` helper to eliminate goto/label pattern.

---

## Remaining (lower priority)

### Unify JUnit parsing (medium risk)
`newJUnitReport` parses the output once for JUnit XML, then
`extractResultsClean` parses it again with markers stripped to
extract `testResults`.

**Fix**: Parse once with markers stripped, then use that single `gtr.Report`
for both JUnit generation and result extraction.

### Consolidate `onCommand` callbacks (exec.go)
All three exec functions accept `onCommand func(string)` with identical
nil-check + call pattern.

### Replace string-key deduplication (log_parse.go)
Use a struct key instead of fragile string concatenation for alert dedup.

### Naming improvements
- `groupByMode` → `unitsByMode` (package.go)
- Test name parsing functions: use input-format-based names
- Variable abbreviations (`tp`, `lc`, `wu`) → more descriptive
