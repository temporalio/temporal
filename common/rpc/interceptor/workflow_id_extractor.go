package interceptor

import (
	commonpb "go.temporal.io/api/common/v1"
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

// Extract returns workflow ID from request using multiple patterns:
// 1. Direct GetWorkflowId() field
// 2. GetWorkflowExecution().GetWorkflowId()
// 3. GetExecution().GetWorkflowId()
// 4. TaskToken deserialization
// Returns namespace.EmptyBusinessID if no workflow ID can be extracted.
func (e WorkflowIDExtractor) Extract(req any) string {
	if req == nil {
		return namespace.EmptyBusinessID
	}

	// Pattern 1: Direct WorkflowId field
	if getter, ok := req.(workflowIDGetter); ok {
		if wfID := getter.GetWorkflowId(); wfID != "" {
			return wfID
		}
	}

	// Pattern 2: WorkflowExecution.WorkflowId field
	if getter, ok := req.(workflowExecutionGetter); ok {
		if exec := getter.GetWorkflowExecution(); exec != nil {
			if wfID := exec.GetWorkflowId(); wfID != "" {
				return wfID
			}
		}
	}

	// Pattern 3: Execution.WorkflowId field
	if getter, ok := req.(executionGetter); ok {
		if exec := getter.GetExecution(); exec != nil {
			if wfID := exec.GetWorkflowId(); wfID != "" {
				return wfID
			}
		}
	}

	// Pattern 4: TaskToken containing WorkflowId
	if getter, ok := req.(taskTokenGetter); ok {
		if tokenBytes := getter.GetTaskToken(); len(tokenBytes) > 0 {
			if taskToken, err := e.serializer.Deserialize(tokenBytes); err == nil {
				return taskToken.GetWorkflowId()
			}
		}
	}

	return namespace.EmptyBusinessID
}
