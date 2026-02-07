# Testrunner2 CI Fix Status

PR: https://github.com/temporalio/temporal/pull/9160

## Identified Issues

### Lint Issues (9 total) — blocks `golangci / All Linters Succeed`
- [ ] `junit.go:340` — revive: switch must have default case
- [ ] `package.go:87` — revive: identical switch branches
- [ ] `testrunner.go:203` — revive: cognitive complexity 40 > 25
- [ ] `testrunner_test.go:778` — revive: switch must have default case
- [ ] 5x testpkg `time.Sleep` — forbidigo violations

### Logic Bugs
- [ ] Post-exit retry emits redundant retries for parent tests whose children were already retried mid-stream (causes duplicate/wasted runs)
- [ ] `buildTestFilterPattern` with mixed-depth names drops shallower names from skip filter (causes already-passed tests to re-run on retry)

### Timeout Issues (functional tests)
- [ ] Suites with 80-114+ subtests consistently time out at 5m per-suite (investigating root cause in testrunner)

## Progress Log
- Starting with lint fixes, then logic bugs, then timeout handling
