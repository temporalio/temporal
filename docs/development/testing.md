# Testing

This document describes the project's testing setup, utilities and best practices.

## Setup

### Build tags
- `test_dep`: This Go build tag enables the test hooks implementation. Only very few tests require it; they will fail if not enabled.
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
`failpoint.AnyGRPCKey` and `failpoint.AnyGRPCResponseKey` failpoints (see the
[failpoint package](#failpoint-package)), so they only see requests that carry the namespace ID, or
the namespace name on frontend. Several faults injected by a test all run, in order, until one
returns an error.

### HTTP fault injection

`InjectHTTPRequestFault` and `InjectHTTPResponseFault` inject faults into HTTP clients that install
the test fault generator. Callback delivery and Nexus operation clients support these faults. The
faults are scoped to the test namespace and are unavailable in builds without `test_dep`. They are
callbacks of the `failpoint.AnyHTTPKey` and `failpoint.AnyHTTPResponseKey` failpoints.

Return one of:

- `nil` to leave the call alone.
- An `httpfaults.ResponseOverride` to return a synthetic HTTP response without an error. An override
  without a response is returned as an error.
- Any other error to return it as a transport error.

```go
env.InjectHTTPRequestFault(func(ctx context.Context, req *http.Request) error {
    if req.URL.Path != "/callback" {
        return nil
    }
    return httpfaults.ResponseOverride{
        Response: httpfaults.NewResponse(http.StatusServiceUnavailable, "injected"),
    }
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

### failpoint package

The `failpoint` package injects faults and timing control at named points in server code. Prefer
it over a new testhooks key when the test only needs to fail, delay, block, or run a callback at
a point in the code.

A failpoint is declared once as a `failpoint.Key` in `common/testing/failpoints`, with a
comment that says where it fires and what tests use it for. Server code executes it through the
server's test hooks, scoped to a namespace ID:

```go
if err := failpoint.Checkpoint(f.testHooks, failpoints.MatchingForwardTask.ForScope(nsID)); err != nil {
    return err
}
```

`Checkpoint` returns an error only if the failpoint returns one. Use `failpoint.Execute[T]` when
the failpoint must return a value of another type. Without `-tags=test_dep`, the test hooks have
no failpoint registry, so both do nothing.

Tests configure the failpoint for the test's namespace. It is removed when the test ends:

```go
fp := testcore.InjectFailpoint[error](env, failpoints.MatchingForwardTask)
fp.Pause(100*time.Millisecond, failpoint.Always)  // Delay every forward.
fp.Return(serviceerror.NewUnavailable("injected"), failpoint.Once)  // Fail the next forward.
fp.Block(failpoint.Once)                         // Hold the next forward ...
require.NoError(t, fp.Wait(ctx))                 // ... until it arrives ...
fp.Disable()                                     // ... then release it.
```

`Call` runs a callback at the failpoint. For example, Update-with-Start has a race window between
releasing the workflow lock and starting the workflow, in which another request could start the
same workflow first. `failpoints.UpdateWithStartBetweenLockAndStart` lets a test start a
conflicting workflow at exactly this point:

```go
testcore.InjectFailpoint[error](env, failpoints.UpdateWithStartBetweenLockAndStart).Call(func() error {
    // Start the conflicting workflow here.
    return nil
}, failpoint.Once)
```

Failpoints also run at the gRPC, HTTP, and persistence fault injection points, without a declared
key:

- `failpoint.GRPCKey(fullMethod)` fires before the handler of a unary gRPC method on frontend,
  history, and matching; `failpoint.GRPCResponseKey(fullMethod)` fires after it. An error from the
  latter replaces the result of the handler.
- `failpoint.HTTPKey("POST /path")` fires before an outgoing callback or Nexus HTTP request;
  `failpoint.HTTPResponseKey("POST /path")` fires after it. An `httpfaults.ResponseOverride` error
  makes the call return its response.
- `failpoint.AnyGRPCKey`, `failpoint.AnyGRPCResponseKey`, `failpoint.AnyHTTPKey` and
  `failpoint.AnyHTTPResponseKey` fire for every method or request, if the failpoint of the method
  or request did not fire with any action (a pause or a callback that returns nil also counts). A
  `CallWith` callback receives the request before the handler, and a `grpcfaults.Result` or
  `httpfaults.Result` after it. The methods of the `FailpointService` run no gRPC failpoints, so a
  failpoint for every method can't block the requests that remove it.
- `failpoints.PersistenceKey("ExecutionStore", "UpdateWorkflowExecution")` fires before a
  persistence store method.

gRPC, HTTP, and persistence failpoints are scoped to the namespace ID of the request when it has
one (frontend resolves the namespace name of a request to its ID), so `testcore.InjectFailpoint` works for them on a shared cluster. A failpoint without a scope
applies to all namespaces, but only to requests whose namespace has no failpoint registered for the
same key. A failpoint registered for the namespace takes precedence, even when it is disabled.

Only store methods that take a request after the context have a persistence failpoint. Of these,
the requests of the following methods have no namespace, so only failpoints without a scope fire for
them:

- `ShardStore`, `ClusterMetadataStore`, `Queue`, `QueueV2`, and `NexusEndpointStore`: all methods.
- `ExecutionStore`: history branch methods (`AppendHistoryNodes`, `ReadHistoryBranch`, ...), history
  task methods (`GetHistoryTasks`, `CompleteHistoryTask`, `RangeCompleteHistoryTasks`),
  `ListConcreteExecutions`, and the replication DLQ methods except `PutReplicationTaskToDLQ`.
- `MetadataStore`: `ListNamespaces`, `DeleteNamespaceByName`, and `GetNamespace` by name.
- `TaskStore`: `ListTaskQueue`.

Inject failpoints for these methods with `testcore.InjectGlobalFailpoint`, which requires a
dedicated cluster. `common/persistence/faultinjection/namespace.go` maps each request type to its
namespace.

With `-tags=test_dep`, every server process also serves the `FailpointService` gRPC API
(`common/testx/proto`) on its gRPC port, so tests and tools outside the process can configure
failpoints by key name. Only system admins may call it on a frontend. History, matching, and
worker serve their own failpoints. Frontends fan each request out: they apply it locally and
forward it to every member of the frontend, history, matching, and worker services, so one call
toggles a failpoint in the whole cluster. Services that share a registry, such as the services of
a test cluster, get each request once.

- Inject and disable requests fail if any host fails, but the hosts that succeeded keep the
  change, so a failed request can be partially applied.
- `WaitForBlock` succeeds as soon as any host reports that an execution reached the block. It
  fails only if every host fails, such as with `NotFound` or `FailedPrecondition` where the
  failpoint is not registered or not configured to block.
- `ListFailpoints` reports the hosts that serve the registry of each failpoint.

`testcore.FailpointClient(env)` returns a client for the test cluster's frontend:

```go
client := testcore.FailpointClient(env)
_, err := client.InjectReturn(ctx, &failpointpb.InjectReturnRequest{
	Key:       failpoint.GRPCKey(historyservice.HistoryService_StartWorkflowExecution_FullMethodName).Name(),
	ErrorName: "InvalidArgument", // A name from the failpoint error catalog.
})
```

#### Failpoint rules in dynamic config

With `-tags=test_dep`, a server process applies the failpoint rules of the `testing.failpointRules`
dynamic config setting to its failpoints. This configures failpoints without code, for example in a
development server's dynamic config file:

```yaml
testing.failpointRules:
  - value:
      - key: "grpc:/temporal.server.api.historyservice.v1.HistoryService/StartWorkflowExecution"
        scope: "<namespace ID>"   # Optional.
        action: Return            # Return, Panic, Block, Pause, Call or On.
        error: InvalidArgument    # A name from the failpoint error catalog, for Return and Panic.
        message: "injected"
        recurrence: Always        # Once or Always (default).
        trigger: {probability: 0.5, after: 1, max: 10}
      - key: "grpc:/temporal.server.api.matchingservice.v1.MatchingService/AddWorkflowTask"
        action: Pause
        delay: 2s
        enabled: false            # Keeps the rule configured but suspended.
```

`Call` names a behavior registered with `failpoint.RegisterBehavior` in `behavior`. `On` makes the
failpoint fire without a value, for call sites that check `failpoint.Active`. Changing the setting
creates, reconfigures and removes the failpoints of the rules; a rule that didn't change keeps its
failpoint's state. Rules and other injections stay separate: if a rule names the key of a failpoint
that a test or the `FailpointService` registered, the server logs an error and keeps the previous
rules, and the `FailpointService` and `testcore.InjectFailpoint` refuse to change a failpoint of a
rule. An invalid `testing.failpointRules` value stops the server: it fails to start, or exits when
the value changes. A dynamic config file with a non-string map key in a rule is rejected as a
whole by the file loader, which keeps the previous dynamic config and logs an error. Functional
tests set the setting with `env.OverrideDynamicConfig(failpointrules.Setting, ...)` on a dedicated
cluster.

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
