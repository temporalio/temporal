# go.temporal.io/testx

Collection of general-purpose Go test helpers.

- `await`: context-bounded polling assertions and channel operations.
- `freeport`: reserve available ports for tests.
- `parallelsuite`: isolated parallel test suites with fail-fast assertions.
- `parallelsuitelint`: checks Go tests for correct `parallelsuite` usage. Run it directly or as a `golangci-lint` plugin.
- `testcontext`: shared test contexts with deadlines and decorators.
