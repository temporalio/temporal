# Testing

This document describes the project's testing setup, utilities and best practices.

## Setup

### Build tags
- `test_dep` (required): This Go build tag is required for running functional tests.
- `TEMPORAL_DEBUG`: Extends functional test timeouts to allow sufficient time for debugging sessions.

### Environment variables
- `CGO_ENABLED`: Set to `0` to disable CGO, which can significantly speed up compilation time.
- `TEMPORAL_TEST_LOG_FORMAT`: Controls the output format for test logs. Available options: `json` or `console`
- `TEMPORAL_TEST_LOG_LEVEL`:  Sets the verbosity level for test logging. Available levels: `debug`, `info`, `warn`, `error`, `fatal`
- `TEMPORAL_TEST_OTEL_OUTPUT`: Enables OpenTelemetry (OTEL) trace output for failed tests to the provided file path.

## Test helpers

Test helpers can be found in the [common/testing](../../common/testing) package.

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
assert.Equal(t, tv.WorkflowID(), startedWorkflow.WorkflowId)
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

### taskpoller package

For end-to-end testing, consider using `taskpoller.TaskPoller` to handle workflow tasks. This is
useful when you need full control over the worker behavior in a way that the SDK cannot provide;
or if there's no SDK support for that API available yet.

You'll find a fully initialized task poller in any functional test suite, look for `s.TaskPoller`.

_NOTE: The previous `testcore.TaskPoller` has been deprecated and should not be used in new code._

### softassert package

`softassert.That` is a "soft" assertion that logs an error if the given condition is false.

It is useful to highlight invariant violations in production code.
It is *not* a substitute for regular error handling, validation, or control flow.

In functional tests, a failed soft assertion will not stop the test execution immediately, but it
will ultimately fail the test.

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