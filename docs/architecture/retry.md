# Retry

The `go.temporal.io/server/common/backoff` package contains the retry primitives used across Temporal.

`backoff.ThrottleRetry` or, when there is a `context.Context` available
`backoff.ThrottleRetryContext`, can be used to retry any operation that returns an error.

Both are configured via:
- `backoff.IsRetryable` which decides whether to retry based on the error type
- `backoff.RetryPolicy` which decides how long to backoff first - or not retry at all

It's important to note that a special retry policy is used for `ResourceExhausted` service errors.

## Service Error

Service errors are specific Go errors that can generate a gRPC `Status` (see [status.proto](https://github.com/grpc/grpc/blob/master/src/proto/grpc/status/status.proto)). 
A gRPC status contains a gRPC `Code` (see [code.proto](https://github.com/googleapis/googleapis/blob/master/google/rpc/code.proto)), a message and (optionally) a payload with more details.

```go
type ServiceError interface {
    error
    Status() *status.Status
}
```

The [api-go](https://github.com/temporalio/api-go/tree/master/serviceerror) repository defines most service errors:
- general-purpose errors
  (such as `Canceled`, `NotFound` or `Unavailable`)
- specialized errors which carry more details
  (such as `NamespaceNotActive` with the gRPC code `FailedPrecondition`)

Furthermore, a few more Server-specific service errors are defined in this repository, such as
`ShardOwnershipLost` or `TaskAlreadyStarted`.

## gRPC

A failed gRPC request can be retried by the (server-side) gRPC handler and the service client.
Both use the aforementioned `backoff` package to configure and execute the retries.

**gRPC handler**: All Temporal services wrap their API handlers with the gRPC interceptor
`interceptor.RetryableInterceptor` to retry a failed gRPC request with certain errors.
Look for `NewRetryableInterceptor` to see the configuration for each service.

**gRPC client**: Similarly, each service client can retry a failed gRPC request with certain errors.
Look for `NewRetryableClient` to see the configuration for each service client.

For example, for a gRPC request from Frontend to History,
there are 3 places an error can be retried (marked with 🔁):

<img src="../_assets/retries.svg">

NOTE: gRPC handler retries can be more efficient since they avoid a round-trip,
but note that retry behavior multiplies between server and client, and client-side is more flexible
(e.g. can direct a request to a different server). So gRPC handler retries should be used sparingly
and are configured to do no more than one extra attempt.