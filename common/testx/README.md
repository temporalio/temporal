# go.temporal.io/testx

Collection of general-purpose Go test helpers.

- `await`: context-bounded polling assertions and channel operations.
- `failpoint`: change the behavior of code under test at named points.
- `freeport`: reserve available ports for tests.
- `parallelsuite`: isolated parallel test suites with fail-fast assertions.
- `parallelsuitelint`: checks Go tests for correct `parallelsuite` usage. Run it directly or as a `golangci-lint` plugin.
- `softassert`: log failed invariants without stopping execution.
- `testcontext`: shared test contexts with deadlines and decorators.
- `testhooks`: typed, scoped hooks enabled with `-tags test_dep`.
