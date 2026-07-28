package testcore

import (
	"github.com/google/uuid"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/testing/protobuilder"
	"go.temporal.io/server/common/testing/testvars"
)

// RequestFactory builds common frontend requests, filling in defaults derived
// from the test environment (namespace and test variables) for any field the
// caller leaves unset. Callers write an ordinary proto literal with only the
// fields they care about:
//
//	req := env.Requests().StartWorkflowExecution(&workflowservice.StartWorkflowExecutionRequest{
//		RequestEagerExecution: true,
//	})
//
// The caller's literal is not modified. For messages without a dedicated method
// here, use protobuilder.WithDefaults directly.
type RequestFactory struct {
	e  *TestEnv
	tv *testvars.TestVars
}

// Requests returns a factory for common frontend requests defaulted from this
// environment. Defaults are drawn from the environment's test variables, or
// from tv when provided — pass a specific TestVars (e.g. tv.Sub(...) or
// tv.WithWorkflowIDNumber(n)) when a test builds requests for several distinct
// workflows.
func (e *TestEnv) Requests(tv ...*testvars.TestVars) RequestFactory {
	v := e.Tv()
	if len(tv) > 0 {
		v = tv[0]
	}
	return RequestFactory{e: e, tv: v}
}

// StartWorkflowExecution fills req with defaults for this environment's
// namespace and test variables. A fresh request ID is generated unless req sets
// one.
func (f RequestFactory) StartWorkflowExecution(
	req *workflowservice.StartWorkflowExecutionRequest,
) *workflowservice.StartWorkflowExecutionRequest {
	return protobuilder.WithDefaults(req, &workflowservice.StartWorkflowExecutionRequest{
		Namespace:    f.e.Namespace().String(),
		WorkflowId:   f.tv.WorkflowID(),
		WorkflowType: f.tv.WorkflowType(),
		TaskQueue:    f.tv.TaskQueue(),
		Identity:     f.tv.ClientIdentity(),
		RequestId:    uuid.NewString(),
	})
}

// PollWorkflowTaskQueue fills req with defaults for this environment's
// namespace, task queue, and identity.
func (f RequestFactory) PollWorkflowTaskQueue(
	req *workflowservice.PollWorkflowTaskQueueRequest,
) *workflowservice.PollWorkflowTaskQueueRequest {
	return protobuilder.WithDefaults(req, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: f.e.Namespace().String(),
		TaskQueue: f.tv.TaskQueue(),
		Identity:  f.tv.ClientIdentity(),
	})
}

// RespondWorkflowTaskCompleted fills req with defaults for this environment's
// namespace and identity. Callers supply the task token and commands.
func (f RequestFactory) RespondWorkflowTaskCompleted(
	req *workflowservice.RespondWorkflowTaskCompletedRequest,
) *workflowservice.RespondWorkflowTaskCompletedRequest {
	return protobuilder.WithDefaults(req, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Namespace: f.e.Namespace().String(),
		Identity:  f.tv.ClientIdentity(),
	})
}
