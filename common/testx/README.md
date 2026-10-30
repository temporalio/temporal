# go.temporal.io/testx

Collection of general-purpose Go test helpers.

- `parallelsuite`: isolated parallel test suites with fail-fast assertions.
- `await`: context-bounded polling assertions and channel operations.
- `testcontext`: shared test contexts with deadlines and decorators.
- `freeport`: reserve available ports for tests.

`parallelsuite.Suite` includes testify's require assertions. Server-specific
protobuf and history assertions remain in `go.temporal.io/server/common/testing/parallelsuite`.
