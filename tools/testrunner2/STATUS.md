# Testrunner2 CI Fix Status

PR: https://github.com/temporalio/temporal/pull/9160

## Identified Issues

### Lint Issues (11 total) — blocks `golangci / All Linters Succeed`
- [x] `junit.go:340` — revive: switch must have default case
- [x] `package.go:87` — revive: identical switch branches
- [x] `package.go:91` — forbidigo: panic is forbidden; changed to return nil
- [x] `testrunner.go:203` — revive: cognitive complexity 40 > 25
- [x] `testrunner.go:912` — revive: cognitive complexity 28 > 25 (formatWorkUnits); split into helpers
- [x] `testrunner_test.go:778` — revive: switch must have default case
- [x] 5x testpkg `time.Sleep` — forbidigo violations

### Logic Bugs
- [x] Post-exit retry emits redundant retries for parent tests whose children were already retried mid-stream (causes duplicate/wasted runs)
- [x] `buildTestFilterPattern` with mixed-depth names drops shallower names from skip filter (causes already-passed tests to re-run on retry)
- [x] **Critical: Skip list not accumulated across retry attempts** — on timeout/crash retry, the new skip list REPLACES the previous one instead of merging. Subtests that passed in attempt N are re-run in attempt N+2 because their skip entries were lost.

### Timeout Issues (functional tests)
- [x] Root cause identified: skip list loss across retries means suites never make enough progress across attempts. Fixed by merging skip lists with `mergeUnique`.

### Unit Test Failure — `go test` exits non-zero despite all tests passing
- [x] Root cause: in direct mode (GroupByNone), the runner uses `--run-timeout` (2m) as `go test -timeout`, but this is the per-test timeout meant for compiled mode. The 2m timeout is far too short for 2430+ unit tests with `-race`. Go's test timeout panic kills the process. Fixed: direct mode now uses `-timeout` (35m, the overall timeout) for `go test -timeout` and passes through extra base args like `-shuffle`.

### fmt-imports CI failure
- [x] Comment indentation was off by one tab level (3 tabs instead of 4).

## Commits
1. Lint fixes + filterEmitted parent handling + collapseForSkip
2. Skip list accumulation fix (mergeUnique) — the critical fix for functional test timeouts
3. Fix comment indentation for fmt-imports CI check
4. Fix remaining lint issues (panic forbidigo, cognitive complexity)
5. Fix direct mode timeout: use overall timeout, pass through base args
