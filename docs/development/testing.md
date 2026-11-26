# Testing

This document describes the project's testing setup, utilities and best practices.

## Setup

### Build tags
- `test_dep`: This Go build tag enables the test hooks implementation. Only very few tests require it; they will fail if not enabled. It does not control failpoints, which are compiled into every build; see [Failpoints](failpoints.md).
- `TEMPORAL_DEBUG`: Extends functional test timeouts to allow sufficient time for debugging sessions.

### Environment variables
- `CGO_ENABLED`: Set to `0` to disable CGO, which can significantly speed up compilation time.
- `TEMPORAL_TEST_LOG_FORMAT`: Controls the console output format for test logs. Available options: `json` or `console` (default)
- `TEMPORAL_TEST_LOG_LEVEL`: Sets the minimum verbosity level written to console output. Available levels: `debug` (default), `info`, `warn`, `error`, `fatal`
- `TEMPORAL_TEST_LOG_FILE`: Path to a file that receives a separate copy of test logs. When unset (default), file logging is disabled. Useful in CI to route debug-level logs to a downloadable artifact without flooding the job log.
- `TEMPORAL_TEST_LOG_FILE_FORMAT`: Output format for the log file. Available options: `json` (default) or `console`
- `TEMPORAL_TEST_LOG_FILE_LEVEL`: Minimum verbosity level written to the log file. Available levels: `debug` (default), `info`, `warn`, `error`, `fatal`
- `TEMPORAL_TEST_OTEL_OUTPUT`: Enables OpenTelemetry (OTEL) trace output for failed tests to the provided file path.
- `TEMPORAL_TEST_SHARED_CLUSTERS`: Number of shared clusters in the pool. Each can be used by multiple tests simultaneously.
- `TEMPORAL_TEST_DEDICATED_CLUSTERS`: Number of dedicated clusters in the pool. Each can be used by one test only at a time.
- `TEMPORAL_TEST_TIMEOUT`: Sets the duration timeout per test (e.g., `90s` for 90 seconds). This can be overridden per-test using `testcore.WithTimeout()`.
- `TEMPORAL_TEST_DATA_ENCODING`: Controls the encoding used for persistence DataBlobs. Available options: `proto3` (default) or `json`.

### Debugging via IDE

#### GoLand

For general instructions, see [GoLand Debugging](https://www.jetbrains.com/help/go/debugging-code.html).
To pass in the required build tags, add them to the "Go tool arguments" field in the Run/Debug configuration:

```
-tags disable_grpc_modules,test_dep
```

## Best Practices

### Use `require` instead of `assert`

Always use `require.X` (and `protorequire.X`) instead of `assert.X` (and `protoassert.X`).
`assert` records a failure but lets the test continue, which often leads to confusing
cascading errors.

### Polling with await.Require

For polling/retry loops in tests, use `await.Require` (or `await.Requiref`)
from `common/testing/await` instead of testify's `EventuallyWithT`.

Use `t.Context()` inside the callback for a context derived from the parent
context and canceled when the parent context is canceled or the await timeout
expires.

```go
await.Require(ctx, t, func(t *await.T) {
    resp, err := client.GetStatus(t.Context())
    require.NoError(t, err)
    require.Equal(t, "ready", resp.Status)
}, 5*time.Second, 200*time.Millisecond)
```

Use `RequireTrue` instead of testify's `Eventually` for simple local bool-returning predicates.

```go
await.RequireTrue(t, func() bool {
    return cache.Ready()
}, 5*time.Second, 200*time.Millisecond)
```

`RequireTrue` is the wrong tool when dealing with errors or assertions; use `Require` instead.

### Blocking on channels

Use `await.Rcv` and `await.Snd` for blocking channel operations.
They fail the test if its context ends or the channel closes.

```go
headers := await.Rcv(t, requestHeaders)
await.Snd(t, responses, response)
```

Keep an explicit `select` when testing non-blocking behavior or waiting on multiple channels.

### Parallelization

All tests (and subtests!) should use `t.Parallel()` to be run concurrently;
unless there is a reason not to.

`make parallelize-tests` can be used to automatically add `t.Parallel()`.
Use `//parallelize:ignore` to opt your test out of it.

## Test helpers

Test helpers can be found in the [common/testing](../../common/testing) package.

### parallelsuite package

Use `parallelsuite.Suite` to ensure your test suite is fast and safe: it runs all test methods and sub-tests in parallel by default;
and provides assertion helpers and safety mechanisms.

It replaces all use of `testify`'s `Suite`.

#### Context shorthand

```go
ctx := s.Context()
```

`s.Context()` returns the subtest-scoped context - equivalent to `testcontext.New(s.T())`.

#### Await shorthand

```go
s.Await(func(s *MySuite) {
    resp, err := client.GetStatus(s.Context())
    s.NoError(err)
    s.Equal("ready", resp.Status)
}, 5*time.Second, 200*time.Millisecond)
```

Inside an `s.Await` callback, `s.Context()` is capped to that await's timeout.

### testvars package

Instead of creating identifiers like task queue name, namespace or worker identity by hand,
use the `testvars` package.

Example:

```go
func TestFoo(t *testing.T) {
    tv := testvars.New(t)

    req := &workflowservice.SignalWithStartWorkflowExecutionRequest{
        RequestId:    tv.Any().String(),
        Namespace:    tv.NamespaceName().String(),
        WorkflowId:   tv.WorkflowID(),
        WorkflowType: tv.WorkflowType(),
        TaskQueue:    tv.TaskQueue(),
        SignalName:   tv.SignalName(),
    }
}
```
Later you can assert on the generated values. `testvars` guarantees to provide the same value every time you call the same method. 

```go
require.Equal(t, tv.WorkflowID(), startedWorkflow.WorkflowId)
```

If you need more than one value for the same entity in one test, you can use `WithEntityNumber()` method to
get a new instance of `testvars` with a different value.

```go
func TestFoo(t *testing.T) {

    tv := testvars.New(t)
    tv1 := tv.WithUpdateIDNumber(1)
    tv2 := tv.WithUpdateIDNumber(2)

    req1 := &workflowservice.UpdateWorkflowExecutionRequest{
        Namespace:         tv1.NamespaceName().String(),
        WorkflowExecution: tv1.WorkflowExecution(),
        Request: &updatepb.Request{
            Meta: &updatepb.Meta{UpdateId: tv1.UpdateID()},
            Input: &updatepb.Input{
                Name: tv1.HandlerName(),
                Args: payloads.EncodeString("args-value-of-" + tv1.UpdateID()),
            },
        },
    }

	req2 := &workflowservice.UpdateWorkflowExecutionRequest{
        Namespace:         tv2.NamespaceName().String(),
        WorkflowExecution: tv2.WorkflowExecution(),
        Request: &updatepb.Request{
            Meta: &updatepb.Meta{UpdateId: tv2.UpdateID()},
            Input: &updatepb.Input{
                Name: tv2.HandlerName(),
                Args: payloads.EncodeString("args-value-of-" + tv2.UpdateID()),
            },
        },
    }
}
```

If you don't care about specific value, you can use `Any()` method to generate a random value.
It indicates that value doesn't matter for this test and will never be asserted on (but required for API, for example).

### testcontext package

There's no need to create your own `context.Context` via `context.WithTimeout`; use `testcontext.New(t)` instead. It returns a test-scoped `context.Context`, memoized per `*testing.T` and canceled on test end or timeout.

### taskpoller package

For end-to-end testing, consider using `taskpoller.TaskPoller` to handle workflow tasks. This is
useful when you need full control over the worker behavior in a way that the SDK cannot provide;
or if there's no SDK support for that API available yet.

You'll find a fully initialized task poller in any functional test suite, look for `s.TaskPoller`.

_NOTE: The previous `testcore.TaskPoller` has been deprecated and should not be used in new code._

### gRPC fault injection

The `testcore` package injects faults into gRPC calls by intercepting requests and responses.
The fault function determines which RPCs trigger a fault and returns the error to inject.
It returns a cleanup function that can be used to remove the fault again.

**Example:**

```go
env.InjectRequestFault(
    func(req any) error {
        if _, ok := req.(*matchingservice.AddWorkflowTaskRequest); ok {
            return serviceerror.NewNotFound("injected fault")
        }
        return nil
    })
```

`InjectRequestFault` runs _before_ the handler, so an injected error prevents the operation from
executing. `InjectResponseFault` runs _after_ the handler, so an injected error can model an
operation that executed but whose response was lost. The test fails if the fault never triggers.
Only unary RPCs are intercepted; streaming RPCs are unaffected.

The `env` methods scope faults to the test's namespace. They are callbacks of the
`serverfailpoints.AnyGRPCKey` and `serverfailpoints.AnyGRPCResponseKey` failpoints (see the
[failpoint package](#failpoint-package)), so they see requests that carry the namespace ID, and
requests that only carry its name: a service resolves the name to the ID from its namespace cache
if it can, and otherwise matches the name scope directly. Several faults injected by a test all
run, in order, until one returns an error.

### HTTP fault injection

`InjectHTTPRequestFault` and `InjectHTTPResponseFault` inject faults into HTTP clients that install
the test fault generator. Callback delivery and Nexus operation clients support these faults. The
faults are scoped to the test namespace. They are
callbacks of the `serverfailpoints.AnyHTTPKey` and `serverfailpoints.AnyHTTPResponseKey` failpoints.

A fault returns a response and an error. Return:

- `nil, nil` to leave the call alone.
- A response and no error to return a synthetic HTTP response without an error. After the call,
  the response replaces the result of the call; it closes the response of the call, unless it is
  that response itself, so a replacement must not share its body.
- An error to return it as a transport error. An error wins over a response.

```go
env.InjectHTTPRequestFault(func(ctx context.Context, req *http.Request) (*http.Response, error) {
    if req.URL.Path != "/callback" {
        return nil, nil
    }
    return httpfaults.NewResponse(http.StatusServiceUnavailable, "injected"), nil
})
```

### Persistence fault injection

Use `testcore.InjectPersistenceFault` to return custom errors from persistence calls. The fault is
active before the test cluster starts. You can filter it by store and method.

```go
option := testcore.InjectPersistenceFault(
    func(config.FaultInjectionTarget) error {
        return context.DeadlineExceeded
    },
    testcore.WithStore(config.ShardStoreName),
    testcore.WithMethod("UpdateShard"),
)
env := testcore.NewEnv(t, option)
```

The test fails if no matching call reaches the faulty path. This ensures that if we end up refactoring
the code (i.e., "UpdateShard" doesn't exist anymore), and the path isn't reachable, that the test
that injects fault there will intentionally fail.

### Persistence order enforcement

Use `testcore.NewPersistenceCallGate` when a test must control call order. It returns a
`*testcore.PersistenceCallGate` and a `TestOption` that installs the gate. Matching calls block at
the gate. Call `WaitForPersistenceCall` before you check other state. Call `Release` on the gate to
continue the persistence operation. Call `ReleaseWithError` to return an error from the blocked
operation. 

```go
gate, option := testcore.NewPersistenceCallGate(
    testcore.WithStore(config.ShardStoreName),
    testcore.WithMethod("UpdateShard"),
)
env := testcore.NewEnv(t, option)

// Wait until one UpdateShard call blocks at the gate.
testcore.WaitForPersistenceCall(t, gate, 10*time.Second)

// Check state, do work, make assertions, etc... while the call is blocked.

// Later, release the gate manually.
gate.Release() // Or gate.ReleaseWithError(err) to fail the blocked call.
```

The test fails if no matching call reaches the gate.

### testhooks package

The `testhooks` package injects test-specific behavior into production code paths that are otherwise
difficult to test. This is a **last resort** - prefer mocking and dependency injection when possible.

_NOTE: Tests using testhooks must be run with `-tags=test_dep`._

Failpoints (see below) replace testhooks that only delay, call or fail at a point in the code. The
testhooks that stay are those that return a value to the code, such as a batch size or a forced
partition, and interceptors that wrap a call and decide whether to run it. Convert a hook that only
delays, calls or fails to a failpoint when a test needs to change it, instead of adding hooks of
that kind.

### failpoint package

The `failpoint` package injects faults and timing control at named points in server code. Prefer
it over a new testhooks key when the test only needs to fail, delay, block, or run a callback at
a point in the code.

The server installs failpoints only if the startup switch is on, see
[Failpoints](failpoints.md). Test clusters (`testcore`) have it on.

A failpoint is declared once as a `failpoint.Key[T]`, where `T` is the type of the value it
returns, in the package that executes it (such as `service/matching` or
`service/history/api/multioperation`), with a comment that says where it fires and what tests use
it for. Server code executes it through the server's `*failpoint.Registry`, which fx provides to
every service as its `FailpointSource`, scoped to a namespace ID:

```go
if err := failpoint.CheckError(f.failpointSource, matching.ForwardTaskKey.ForScope(nsID)); err != nil {
    return err
}
```

`CheckError` returns an error only if the failpoint returns one. Use `failpoint.Execute` when
the failpoint returns a value of another type. Without failpoints enabled, the registry is nil,
so both do nothing.

Tests configure the failpoint for the test's namespace. It is removed when the test ends:

```go
fp := testcore.InjectFailpoint(env, matching.ForwardTaskKey)
fp.Pause(100*time.Millisecond)  // Delay every forward.
fp.Return(serviceerror.NewUnavailable("injected"), failpoint.Trigger{MaxFires: 1})  // Fail the next forward.
fp.Block(failpoint.Trigger{MaxFires: 1})                         // Hold the next forward ...
require.NoError(t, fp.Wait(ctx))                 // ... until it arrives ...
fp.Disable()                                     // ... then release it.
```

The test fails if the failpoint never fired, so a wrong key, scope or configuration can't pass
unnoticed; pass `testcore.MayNotFire()` to opt out. Use `testcore.InjectFailpointIn` to inject into
another cluster of the test, such as the standby cluster of a replication test, and
`testcore.InjectUnscopedFailpoint` for a failpoint of all scopes, which needs a dedicated cluster.

`Call` runs a callback at the failpoint. For example, Update-with-Start has a race window between
releasing the workflow lock and starting the workflow, in which another request could start the
same workflow first. `multioperation.BetweenLockAndStartKey` lets a test start a
conflicting workflow at exactly this point:

```go
testcore.InjectFailpoint(env, multioperation.BetweenLockAndStartKey).Call(func() error {
    // Start the conflicting workflow here.
    return nil
}, failpoint.Trigger{MaxFires: 1})
```

Failpoints also run at the gRPC and HTTP fault injection points, without a declared key. The keys
are in `serverfailpoints`:

- `serverfailpoints.GRPCKey(fullMethod)` fires before the handler of a unary gRPC method on every
  service; `serverfailpoints.GRPCResponseKey(fullMethod)` fires after it. An error from the latter
  replaces the result of the handler. Streaming methods, such as replication streams, run no
  failpoints.
- `serverfailpoints.HTTPKey("POST /path")` fires before an outgoing callback or Nexus HTTP request;
  `serverfailpoints.HTTPResponseKey("POST /path")` fires after it. The error of these failpoints
  fails the call. A callback that answers the call with a response calls `Replace` on its argument
  (the `*httpfaults.Request` before the call, the `*httpfaults.Result` after it) and returns no
  error. The last response that it sets wins; `Wrap` closes the body of every other response that
  the fault set, and of the ones that an error discards, once.
- `serverfailpoints.AnyGRPCKey`, `serverfailpoints.AnyGRPCResponseKey`,
  `serverfailpoints.AnyHTTPKey` and `serverfailpoints.AnyHTTPResponseKey` fire for every method or
  request, if the failpoint of the method or request did not fire with any action (a pause or a
  callback that returns nil also counts). A `CallWithArg` callback receives the request before the
  handler, and a `*grpcfaults.Result` or `*httpfaults.Result` after it.

gRPC and HTTP failpoints are scoped to the namespace ID of the request when it has one (frontend
resolves the namespace name of a request to its ID), so `testcore.InjectFailpoint` works for them
on a shared cluster. A request that only carries a namespace name uses the ID if the service
resolves it from its namespace cache, and otherwise is scoped to
`failpointadapter.NameScope(name)`; the `Inject*Fault` helpers of `TestEnv` register their faults
for the ID and the name of the test's namespace. To inject a failpoint for a request that may not
resolve, such as one that frontend sends to history or matching, scope its key with
`failpointadapter.NameScope(name)` and use `testcore.InjectFailpointIn`. A failpoint without a scope applies to all namespaces, but only
to requests whose namespace has no failpoint registered for the same key. A failpoint registered
for the namespace takes precedence, even when it is disabled.

### softassert package

`softassert.That` is a "soft" assertion that logs an error if the given condition is false.

It is useful to highlight invariant violations in production code.
It is *not* a substitute for regular error handling, validation, or control flow.

In functional tests, a failed soft assertion will not stop the test execution immediately, but it
will ultimately fail the test.

### protorequire package

Use `protorequire.ProtoEqual` to compare proto messages with proto semantics.
Prefer a single `ProtoEqual` call over asserting fields one-by-one, since it catches unexpected field changes and keeps the expected value next to the assertion.

To ignore specific fields on the top-level message (e.g. non-deterministic timestamps), pass `protorequire.IgnoreFields`:

```go
protorequire.ProtoEqual(t, expected, actual,
    protorequire.IgnoreFields(
        "execution_duration",
        "schedule_time",
    ),
)
```

### historyrequire package

`historyrequire` has assertions to verify workflow event histories.

Use `EqualHistoryEvents` to assert the full event sequence:

```go
events := env.GetHistory(env.Namespace().String(), workflowExecution)
s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled {"Attempt": 1}
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowExecutionCompleted`, events)
```

Optional inline JSON (e.g. `{"Attempt": 1}`) can be used to assert on specific attributes.

Use `ContainsHistoryEvents` when you only care about a particular segment:

```go
s.ContainsHistoryEvents(`
  4 WorkflowTaskFailed {"Identity": "worker-1"}
  5 WorkflowTaskScheduled
  6 WorkflowTaskStarted`, events)
```

Use `RequireHistoryEvent` when you only care about a single event type:

```go
completed := s.RequireHistoryEvent(events, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED)
require.Equal(t, "expected-result", completed.GetWorkflowExecutionCompletedEventAttributes().Result)
```

Or use `RequireNoHistoryEvent` when you expect no event of a given type to be present.

### Test Cluster

Use `testcore.NewEnv(t)` to create a test environment with access to a Temporal cluster for end-to-end testing.

```go
func (s* TestMyFeatureSuite) func TestXYZ(t *testing.T) {
    s := testcore.NewEnv(t)
    // ...
}
```

Note that each test has its own namespace (`s.Namespace()`) for isolation.

## OpenTelemetry (OTEL)

To debug your test by analysing observability traces, set the following environment variables:

```bash
export OTEL_BSP_SCHEDULE_DELAY=100
export OTEL_EXPORTER_OTLP_TRACES_INSECURE=true
export OTEL_TRACES_EXPORTER=otlp
export TEMPORAL_OTEL_DEBUG=true
```

And have an OTEL collector running, such as Grafana Tempo (`make start-dependencies`).

See [tracing.md](../../docs/development/tracing.md) for more details.

## Code coverage

You'll find the code coverage reporting in Codecov: https://app.codecov.io/gh/temporalio/temporal.

Consider installing the [Codecov Browser Extension](https://docs.codecov.com/docs/the-codecov-browser-extension)
to see code coverage directly in GitHub PRs.
