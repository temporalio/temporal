# go.temporal.io/testx

Collection of general-purpose Go test helpers.

- `await`: context-bounded polling assertions and channel operations.
- `freeport`: reserve available ports for tests.
- `testcontext`: shared test contexts with deadlines and decorators.
- `testlogger`: backend-independent test logging with expectations, captures, and failure diagnostics.
- `testlogger/zap`: Zap logger and field integration for `testlogger`.
