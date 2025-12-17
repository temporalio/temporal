package interceptor

import (
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/tasktoken"
)

type WorkflowIDExtractor struct {
	serializer tasktoken.Serializer
}

func NewWorkflowIDExtractor() WorkflowIDExtractor {
	return WorkflowIDExtractor{
		serializer: *tasktoken.NewSerializer(),
	}
}

// Interfaces for extracting workflow ID from different request types.
type (
	workflowIDGetter interface {
		GetWorkflowId() string
	}

	workflowExecutionGetter interface {
		GetWorkflowExecution() *commonpb.WorkflowExecution
	}

	executionGetter interface {
		GetExecution() *commonpb.WorkflowExecution
	}

	taskTokenGetter interface {
		GetTaskToken() []byte
	}
)

// Extract extracts workflow ID from the request using the specified pattern.
// Returns the workflow ID or namespace.EmptyBusinessID if not found.
func (e WorkflowIDExtractor) Extract(req any, pattern WorkflowIDPattern) string {
	if req == nil {
		return namespace.EmptyBusinessID
	}

	switch pattern {
	case PatternNone:
		return namespace.EmptyBusinessID

	case PatternWorkflowID:
		if getter, ok := req.(workflowIDGetter); ok {
			return getter.GetWorkflowId()
		}

	case PatternWorkflowExecution:
		if getter, ok := req.(workflowExecutionGetter); ok {
			if exec := getter.GetWorkflowExecution(); exec != nil {
				return exec.GetWorkflowId()
			}
		}

	case PatternExecution:
		if getter, ok := req.(executionGetter); ok {
			if exec := getter.GetExecution(); exec != nil {
				return exec.GetWorkflowId()
			}
		}

	case PatternTaskToken:
		if getter, ok := req.(taskTokenGetter); ok {
			if tokenBytes := getter.GetTaskToken(); len(tokenBytes) > 0 {
				if taskToken, err := e.serializer.Deserialize(tokenBytes); err == nil {
					return taskToken.GetWorkflowId()
				}
			}
		}

	case PatternMultiOperation:
		return e.extractMultiOperation(req)
	}

	return namespace.EmptyBusinessID
}

// extractMultiOperation extracts workflow ID from ExecuteMultiOperationRequest.
// The workflow ID is extracted from the first operation's StartWorkflow request.
func (e WorkflowIDExtractor) extractMultiOperation(req any) string {
	multiOpReq, ok := req.(*workflowservice.ExecuteMultiOperationRequest)
	if !ok {
		return namespace.EmptyBusinessID
	}

	ops := multiOpReq.GetOperations()
	if len(ops) == 0 {
		return namespace.EmptyBusinessID
	}

	firstOp := ops[0]
	if firstOp == nil {
		return namespace.EmptyBusinessID
	}

	startWorkflow := firstOp.GetStartWorkflow()
	if startWorkflow == nil {
		// First operation is not StartWorkflow - try to get from UpdateWorkflow
		updateWorkflow := firstOp.GetUpdateWorkflow()
		if updateWorkflow != nil && updateWorkflow.GetWorkflowExecution() != nil {
			return updateWorkflow.GetWorkflowExecution().GetWorkflowId()
		}
		return namespace.EmptyBusinessID
	}

	return startWorkflow.GetWorkflowId()
}
