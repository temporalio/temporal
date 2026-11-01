# go.temporal.io/testx

Collection of general-purpose Go test helpers.

- `testlogger`: test logging with expectations, captures, and failure detection.

```go
logger := testlogger.NewTestLogger(t, testlogger.FailOnAnyUnexpectedError)
logger.Expect(testlogger.Error, "connection refused", zap.String("service", "frontend"))
logger.Error("connection refused", zap.String("service", "frontend"))
```

`testlogger` accepts Zap fields and can wrap an existing `*zap.Logger` with
`WrapLogger`. Temporal-specific logging environment variables remain supported by
the server's `common/testing/testlogger` adapter.
