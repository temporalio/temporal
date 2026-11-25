# Testing

This document describes the project's testing setup, utilities and best practices.

## Setup

### Build tags
- `test_dep`: This Go build tag enables the test hooks implementation. Only very few tests require it; they will fail if not enabled. It does not control failpoints, which are compiled into every build; see [Enabling failpoints](#enabling-failpoints).
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
- `TEMPORAL_FAILPOINTS`: Set to `1`, together with the `failpoints.enabled` server config setting, to enable failpoints in a server process. See [Enabling failpoints](#enabling-failpoints). Test clusters enable them themselves.
- `FAILPOINT_BUGGIFY_SEED`, `FAILPOINT_BUGGIFY_ACTIVATION`, `FAILPOINT_BUGGIFY_FIRE`, `FAILPOINT_GOMAD_EVENTS`: Run servers with BUGGIFY (only with failpoints enabled). Meant for test clusters and gomad, not functional test runs. See [BUGGIFY](#buggify).

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
faults are scoped to the test namespace. They are
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

#### Enabling failpoints

Failpoints are compiled into every build, including release binaries; there is no separate test
image. A server process only installs them if both of these are set, each read once at startup:

- `failpoints.enabled: true` in the server config (`config.Failpoints`), and
- the `TEMPORAL_FAILPOINTS=1` environment variable.

Neither is enough alone, and dynamic config cannot enable failpoints. With the switch off, nothing
is installed: there is no failpoint registry, no gRPC fault interceptor, HTTP fault wrapper or
persistence failpoint injector, and the rules, BUGGIFY and
knob randomization (`testing.failpointRules`, `testing.buggify`) do nothing. Call sites in server
code cost a nil check. The `test_dep` tag is not involved.

When the switch is on, the server logs a warning that names the inputs of the decision, and
the `failpoints_enabled` gauge is 1 (0 otherwise). Test clusters (`testcore`) run the normal server
with the switch on: they set the config setting and supply the environment variable to each server
through `temporal.WithEnvLookup`, so clusters in one process don't change the process environment.

A failpoint is declared once as a `failpoint.Key` in `common/testing/failpoints`, with a
comment that says where it fires and what tests use it for. Server code executes it through the
server's `*failpoints.Source`, which fx provides to every service, scoped to a namespace ID:

```go
if err := failpoint.Checkpoint(f.failpointSource, failpoints.MatchingForwardTask.ForScope(nsID)); err != nil {
    return err
}
```

`Checkpoint` returns an error only if the failpoint returns one. Use `failpoint.Execute[T]` when
the failpoint must return a value of another type. Without failpoints enabled, the source has
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
  `httpfaults.Result` after it. - `failpoints.PersistenceKey("ExecutionStore", "UpdateWorkflowExecution")` fires before a
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

The server does not serve a gRPC API to configure failpoints. Tests configure the failpoints of
their cluster in process (`testcore.InjectFailpoint`), and a server or test cluster is configured
from outside through the `testing.failpointRules` dynamic config setting, see below.

#### Failpoint rules in dynamic config

With failpoints enabled, a server process applies the failpoint rules of the `testing.failpointRules`
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
that a test registered, the server logs an error and keeps the previous rules, and
`testcore.InjectFailpoint` refuses to change a failpoint of a rule. An invalid `testing.failpointRules` value stops the server: it fails to start, or exits when
the value changes. A dynamic config file with a non-string map key in a rule is rejected as a
whole by the file loader, which keeps the previous dynamic config and logs an error. Functional
tests set the setting with `env.OverrideDynamicConfig(failpointrules.Setting, ...)` on a dedicated
cluster.

#### BUGGIFY

BUGGIFY, after [FoundationDB](https://apple.github.io/foundationdb/testing.html), runs a server
"mostly normal, with a few weird things". It is meant for test clusters (a development server or a
test cell with failpoints enabled) and gomad runs, not for the functional tests, which run
without it, except `TestBuggify`. With a seed, each failpoint key name that has
nothing registered is activated for the whole run with some probability, and each execution of an
activated key fires with another probability. Both decisions are pure functions of the seed, the key
name and the number of the execution of the key in its namespace, so the same seed activates the
same keys in every namespace, and one namespace's traffic doesn't change which executions fire in
another. Which executions fire also depends on the order of executions of a key in a namespace, so
outside gomad a seed reproduces the set of activated keys, not the whole run.

When an activated key fires:

- `grpc:` and `http:` keys (10% of the methods are activated, 1% of their executions fire) fail
  the request before it is handled with `Unavailable`, which callers retry. Only calls between
  services are failed, the requests that history, matching, worker and the internal frontend
  receive, and outgoing HTTP requests. Requests from clients to the public frontend are not: an
  error there would test the client's retries, not the server (see
  `buggify.InjectsInboundGRPCErrors`). Registered failpoints still apply to the frontend.
- Hand-placed keys (25% activated, 25% fire) take the branch of their `buggify.Fires` call
  site, such as `failpoints.MatchingSkipSyncMatch`, which persists a task to the backlog instead
  of offering it to a waiting poller. A `buggify.Fires` branch must only make legal but
  unlikely states more likely, never produce one that cannot happen in production.
- Dynamic config settings marked with `dynamicconfig.Buggifiable` (25% of them) get one of the
  alternative values that their owner listed, such as a batch size of 1, for the whole run.

Persistence keys, response keys and health checks are never buggified (see
`buggify.Defaults` in `common/testing/buggify`), nor are the failpoints of any method or request,
whose names (`grpc-any:request` and so on) don't match `grpc:*` or `http:*`. A failpoint registered
for a key, by a test or a rule, takes precedence over BUGGIFY even while it
is disabled, and so does an explicitly set dynamic config value over a randomized one. For gRPC and
HTTP requests, a failpoint registered for any method or request also does: for example
`InjectRequestFault` keeps BUGGIFY away from the gRPC requests of the test's namespace.

Enable it on a server with failpoints enabled with one of:

- `FAILPOINT_BUGGIFY_SEED=N` in the environment of the server process, read with
  `buggify.ConfigFromEnv` (`go.temporal.io/testx/failpoint/buggify`). `FAILPOINT_BUGGIFY_SEED=random` picks a seed, which the server
  logs. Under gomad, `FAILPOINT_BUGGIFY_SEED=gomad` derives the seed from `GOMADSEED` (which
  alone doesn't enable BUGGIFY), and `FAILPOINT_GOMAD_EVENTS=1` prints a `GOMAD failpoint ...`
  line per activation decision and per execution that fired. `FAILPOINT_BUGGIFY_ACTIVATION` and
  `FAILPOINT_BUGGIFY_FIRE` replace the probabilities of hand-placed keys (default 0.25); the
  per-prefix defaults and the setting below apply on top.
- The `testing.buggify` dynamic config setting, which also overrides the defaults:

```yaml
testing.buggify:
  - value:
      seed: 42                    # 0: FAILPOINT_BUGGIFY_SEED.
      disabled: false             # true turns BUGGIFY off, also with a seed in the environment.
      prefixes:                   # Replace or add per-prefix probabilities and errors.
        - {prefix: "grpc:", activation: 0.2, fire: 0.05, error: Unavailable}
        - {prefix: "dynamicconfig:", activation: 0}  # Don't randomize settings.
      exclude: ["persistence:*", "grpc-response:*", "http-response:*"]  # Replaces the defaults.
```

The server logs the seed (`BUGGIFY enabled; reproduce with FAILPOINT_BUGGIFY_SEED=N`), every key it
activates, and every randomized setting. Tests can read, per registry, each key name that BUGGIFY saw with its
activation, hits and fires from `buggify.For(registry).FallbackStats()`, for example to check that a
run reached the keys it meant to stress. An invalid setting or
BUGGIFY environment variable stops the server. Settings are randomized once, when the server starts. To
reproduce a run, start the server with the logged seed and the same `testing.buggify` overrides.

A functional test of BUGGIFY itself sets `buggify.Setting` with `testcore.WithDynamicConfig`, which
gives it a dedicated cluster, since BUGGIFY applies to the whole cluster (see `TestBuggify`).
`testcore.FailpointRegistry(env)` lets it subscribe to the BUGGIFY events. Unit tests that run
with BUGGIFY and need exact behavior call `buggify.For(registry).Suppress()`.

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
