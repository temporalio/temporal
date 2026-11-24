# go.temporal.io/testx

Collection of general-purpose Go test helpers.

- `await`: context-bounded polling assertions and channel operations.
- `failpoint`: inject faults and timing control at named points in code under test; see
  [failpoint/README.md](failpoint/README.md).
- `freeport`: reserve available ports for tests.
- `parallelsuite`: isolated parallel test suites with fail-fast assertions.
- `softassert`: log failed invariants without stopping execution.
- `testcontext`: shared test contexts with deadlines and decorators.
- `testhooks`: typed, scoped hooks and a failpoint registry, enabled with `-tags test_dep`.
