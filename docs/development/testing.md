# Testing

This document describes the project's testing utilities and best practices.

## Test helpers

Test helpers can be found in the [common/testing](../../common/testing) package.

### testvars helper

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

### assertions

The `go.temporal.io/server/common/assert` package provides in-code assertions that are disabled
by default. It is recommended to enable them during development/testing to catch any potential 
issues early. To enable them, use the `-tags=with_assertions` build tag.