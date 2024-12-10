# Testing

This document describes the project's testing utilities and best practices.

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
        RequestId:    uuid.New(),
        Namespace:    tv.NamespaceName().String(),
        WorkflowId:   tv.WorkflowID(),
        WorkflowType: tv.WorkflowType(),
        TaskQueue:    tv.TaskQueue(),
        SignalName:   "foo",
    }
}
```

### taskpoller package

For end-to-end testing, consider using `taskpoller.TaskPoller` to handle workflow tasks. This is
useful when you need full control over the worker behavior in a way that the SDK cannot provide;
or if there's no SDK support for that API available yet.

You'll find a fully initialized task poller in any functional test suite, look for `s.TaskPoller`.

_NOTE: The previous `testcore.TaskPoller` has been deprecated and should not be used in new code._