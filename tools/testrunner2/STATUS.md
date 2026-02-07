# Testrunner2 CI Fix Status

PR: https://github.com/temporalio/temporal/pull/9160

## Constraint

Under no circumstances do we ever skip over a test in CI. All tests must eventually
succeed (or we give up after all attempts). Never silently ignore a test.

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

### Functional Test Timeout (sqlite shards 1-2) — quarantine skip pattern bug
- [x] `buildRetryPlans` added quarantined parent names (depth 2) to `regularSkip` alongside depth-3 passed tests. `buildPerLevelPattern` dropped shallower names from the pattern, so quarantined parents were NOT actually skipped. Result: regular retry re-ran quarantined subtests, wasting time and causing timeouts.
  - Failing suites: `TestVersioning3FunctionalSuiteV0`, `TestTaskQueueStats_Classic_Suite`, `TestTaskQueueStats_Pri_Suite`, `TestWorkerDeploymentSuiteV0`
  - Fix: instead of adding parent names at a shallower depth, add `passedTests + quarantinedTests` (all at leaf depth) to regularSkip. This keeps all skip entries at the same depth, avoiding the per-level pattern bug.
  - Also removed `collapseForSkip` (which over-skipped by collapsing to parent names — violates the "never skip a test" constraint) and `filterNotByPrefix` (dead code after this fix).

### Functional Test Timeout (all shards) — cross-product skip limitation
- [x] Root cause: Go's `-test.skip` matches per-level (split by `/`), creating a cross-product of level-2 and level-3 names. For suites with subtests × permutations (e.g., 30 subtests × 8 permutations = 240 leaf tests), the skip pattern can only skip subtests where ALL permutations passed. Partially-completed subtests must be re-run from scratch, wasting significant retry time.
  - Failing suites: `TestVersioning3FunctionalSuiteV0`, `TestVersioning3FunctionalSuiteV2`, `TestVersioningFunctionalSuite`, `TestWorkerDeploymentSuiteV0/V2` across all shards and backends
  - Fix (v1, replaced): 4× `--run-timeout` for regular retry plans.
  - Fix (v2): Add "all-stuck" monitoring to `testEventStream`. Track when the last `=== RUN` event was seen; if no new test has started for > `--run-timeout` duration and tests are still running, cancel — remaining tests are all stuck. This replaces the 4× timeout multiplier with a smarter detection mechanism. CI should increase `--run-timeout` to 10m for functional tests.

### gci formatter
- [x] Comment alignment was reformatted by `gci` (Go Code Imports formatter).

## Current CI Status
- All linters: **PASS** (golangci, fmt-imports, All Linters Succeed)
- Misc checks: **PASS**
- Unit test: **PASS**
- Integration test: **PASS**
- All smoke tests: **PASS** (cass_es, cass_es8, cass_os2, mysql8, postgres12, postgres12_pgx)
- NDC tests: **PASS** (sqlite, cass_os3)
- Functional test (sqlite, shard 2): **PASS** (fixed by quarantine skip fix)
- Functional test (cass_os3, shard 2): **PASS** (fixed by quarantine skip fix)
- Functional test (sqlite, shards 1,3): **awaiting re-run** with retry timeout fix
- Functional test (cass_os3, shards 1,3): **awaiting re-run** with retry timeout fix
- XDC tests (sqlite, cass_os3): **awaiting re-run** — TestFuncClustersTestSuite/TestLocalNamespaceMigration times out (same issue)

## Commits
1. Lint fixes + filterEmitted parent handling + collapseForSkip
2. Skip list accumulation fix (mergeUnique) — the critical fix for functional test timeouts
3. Fix comment indentation for fmt-imports CI check
4. Fix remaining lint issues (panic forbidigo, cognitive complexity)
5. Fix direct mode timeout: use overall timeout, pass through base args
6. Fix quarantine skip pattern: use leaf-depth skip entries, remove collapseForSkip
7. Fix comment alignment for gci formatter
8. Use extended timeout for regular retry plans (4× run-timeout, capped at overall timeout) — replaced by commit 9
9. Replace 4× timeout hack with all-stuck monitoring (track last test started, cancel when no new test for --run-timeout)
