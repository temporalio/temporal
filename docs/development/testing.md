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
If you don't care about specific value, you can use `Any()` method to generate a random value.
It indicates that value doesn't matter for this test and will never be asserted on (but required for API, for example).

If you need more than one value of the same type in the same test you can use `AppendTo*()` methods.

```go
func TestFoo(t *testing.T) {

    tv := testvars.New(t)
    tv1 := tv.AppendToUpdateID("1")
    tv2 := tv.AppendToUpdateID("2")

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