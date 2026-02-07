# Testrunner2 CI Fix Status

PR: https://github.com/temporalio/temporal/pull/9160

## Identified Issues

### Lint Issues (9 total) — blocks `golangci / All Linters Succeed`
- [x] `junit.go:340` — revive: switch must have default case
- [x] `package.go:87` — revive: identical switch branches
- [x] `testrunner.go:203` — revive: cognitive complexity 40 > 25
- [x] `testrunner_test.go:778` — revive: switch must have default case
- [x] 5x testpkg `time.Sleep` — forbidigo violations

### Logic Bugs
- [x] Post-exit retry emits redundant retries for parent tests whose children were already retried mid-stream (causes duplicate/wasted runs)
- [x] `buildTestFilterPattern` with mixed-depth names drops shallower names from skip filter (causes already-passed tests to re-run on retry)
- [x] **Critical: Skip list not accumulated across retry attempts** — on timeout/crash retry, the new skip list REPLACES the previous one instead of merging. Subtests that passed in attempt N are re-run in attempt N+2 because their skip entries were lost.

### Timeout Issues (functional tests)
- [x] Root cause identified: skip list loss across retries means suites never make enough progress across attempts. Fixed by merging skip lists with `mergeUnique`.

## Commits
1. Lint fixes + filterEmitted parent handling + collapseForSkip
2. Skip list accumulation fix (mergeUnique) — the critical fix for functional test timeouts
