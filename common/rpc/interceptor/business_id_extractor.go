package interceptor

import (
	"context"
	"strings"

	commonpb "go.temporal.io/api/common/v1"
	deploymentpb "go.temporal.io/api/deployment/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	updatepb "go.temporal.io/api/update/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/api"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/tasktoken"
)

type BusinessIDExtractor struct {
	serializer tasktoken.Serializer
}

func NewBusinessIDExtractor() BusinessIDExtractor {
	return BusinessIDExtractor{
		serializer: *tasktoken.NewSerializer(),
	}
}

// WorkflowServiceExtractor returns a BusinessIDExtractorFunc that extracts business ID
// from WorkflowService API requests using the provided BusinessIDExtractor.
func WorkflowServiceExtractor(extractor BusinessIDExtractor) BusinessIDExtractorFunc {
	return func(_ context.Context, req any, fullMethod string) string {
		// Only process WorkflowService APIs
		if !strings.HasPrefix(fullMethod, api.WorkflowServicePrefix) {
			return ""
		}

		methodName := api.MethodName(fullMethod)
		pattern, hasPattern := methodToPattern[methodName]
		if !hasPattern {
			return ""
		}

		return extractor.Extract(req, pattern)
	}
}

// Interfaces for extracting business ID from different request types.
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

	taskQueueNameGetter interface {
		GetTaskQueue() string
	}

	taskQueueNameFromMessageGetter interface {
		GetTaskQueue() *taskqueuepb.TaskQueue
	}

	deploymentNameGetter interface {
		GetDeploymentName() string
	}

	deploymentVersionGetter interface {
		GetDeploymentVersion() *deploymentpb.WorkerDeploymentVersion
	}

	pollerGroupIDGetter interface {
		GetPollerGroupId() string
	}

	namespaceGetter interface {
		GetNamespace() string
	}

	updateRefGetter interface {
		GetUpdateRef() *updatepb.UpdateRef
	}
)

// Extract extracts business ID from the request using the specified pattern.
// Returns the business ID or namespace.EmptyBusinessID if not found.
func (e BusinessIDExtractor) Extract(req any, pattern BusinessIDPattern) string {
	if req == nil {
		return namespace.EmptyBusinessID
	}

	//nolint:revive // identical-switch-branches: PatternNone and default both fall through intentionally
	switch pattern {
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

	case PatternTaskQueueName:
		if getter, ok := req.(taskQueueNameGetter); ok {
			return getter.GetTaskQueue()
		}

	case PatternTaskQueueNameFromMessage:
		if getter, ok := req.(taskQueueNameFromMessageGetter); ok {
			if tq := getter.GetTaskQueue(); tq != nil {
				return tq.GetName()
			}
		}

	case PatternDeploymentName:
		if getter, ok := req.(deploymentNameGetter); ok {
			return getter.GetDeploymentName()
		}

	case PatternDeploymentVersion:
		if getter, ok := req.(deploymentVersionGetter); ok {
			if dv := getter.GetDeploymentVersion(); dv != nil {
				return dv.GetDeploymentName()
			}
		}

	case PatternPollerGroupID:
		if getter, ok := req.(pollerGroupIDGetter); ok {
			return getter.GetPollerGroupId()
		}

	case PatternNamespace:
		if getter, ok := req.(namespaceGetter); ok {
			return getter.GetNamespace()
		}

	case PatternUpdateRef:
		if getter, ok := req.(updateRefGetter); ok {
			return getter.GetUpdateRef().GetWorkflowExecution().GetWorkflowId()
		}

	case PatternNone:
		// No extraction needed

	default:
		// Unknown pattern
	}

	return namespace.EmptyBusinessID
}

// extractMultiOperation extracts business ID from ExecuteMultiOperationRequest.
// The business ID is extracted from the first operation's StartWorkflow request.
func (e BusinessIDExtractor) extractMultiOperation(req any) string {
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
