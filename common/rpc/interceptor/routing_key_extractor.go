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

type RoutingKeyExtractor struct {
	serializer tasktoken.Serializer
}

func NewRoutingKeyExtractor() RoutingKeyExtractor {
	return RoutingKeyExtractor{
		serializer: *tasktoken.NewSerializer(),
	}
}

// WorkflowServiceExtractor returns a RoutingKeyExtractorFunc that extracts business ID
// from WorkflowService API requests using the provided RoutingKeyExtractor.
func WorkflowServiceExtractor(extractor RoutingKeyExtractor) RoutingKeyExtractorFunc {
	return func(_ context.Context, req any, fullMethod string) namespace.RoutingKey {
		// Only process WorkflowService APIs
		if !strings.HasPrefix(fullMethod, api.WorkflowServicePrefix) {
			return namespace.RoutingKey{}
		}

		methodName := api.MethodName(fullMethod)
		pattern, hasPattern := methodToPattern[methodName]
		if !hasPattern {
			return namespace.RoutingKey{}
		}

		return extractor.Extract(req, pattern)
	}
}

// Interfaces for extracting routing key from different request types.
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

// Extract extracts routing key from the request using the specified pattern.
// Returns a zero-value namespace.RoutingKey if not found.
//
//nolint:revive // cognitive-complexity
func (e RoutingKeyExtractor) Extract(req any, pattern RoutingKeyPattern) namespace.RoutingKey {
	if req == nil {
		return namespace.RoutingKey{}
	}

	//nolint:revive // identical-switch-branches: PatternNone and default both fall through intentionally
	switch pattern {
	case PatternWorkflowID:
		if getter, ok := req.(workflowIDGetter); ok {
			return namespace.RoutingKey{ID: getter.GetWorkflowId()}
		}

	case PatternWorkflowExecution:
		if getter, ok := req.(workflowExecutionGetter); ok {
			if exec := getter.GetWorkflowExecution(); exec != nil {
				return namespace.RoutingKey{ID: exec.GetWorkflowId()}
			}
		}

	case PatternExecution:
		if getter, ok := req.(executionGetter); ok {
			if exec := getter.GetExecution(); exec != nil {
				return namespace.RoutingKey{ID: exec.GetWorkflowId()}
			}
		}

	case PatternTaskToken:
		if getter, ok := req.(taskTokenGetter); ok {
			if tokenBytes := getter.GetTaskToken(); len(tokenBytes) > 0 {
				if taskToken, err := e.serializer.Deserialize(tokenBytes); err == nil {
					return namespace.RoutingKey{ID: taskToken.GetWorkflowId()}
				}
			}
		}

	case PatternMultiOperation:
		return e.extractMultiOperation(req)

	case PatternTaskQueueName:
		if getter, ok := req.(taskQueueNameGetter); ok {
			return namespace.RoutingKey{ID: getter.GetTaskQueue()}
		}

	case PatternTaskQueueNameFromMessage:
		if getter, ok := req.(taskQueueNameFromMessageGetter); ok {
			if tq := getter.GetTaskQueue(); tq != nil {
				return namespace.RoutingKey{ID: tq.GetName()}
			}
		}

	case PatternDeploymentName:
		if getter, ok := req.(deploymentNameGetter); ok {
			return namespace.RoutingKey{ID: getter.GetDeploymentName()}
		}

	case PatternDeploymentVersion:
		if getter, ok := req.(deploymentVersionGetter); ok {
			if dv := getter.GetDeploymentVersion(); dv != nil {
				return namespace.RoutingKey{ID: dv.GetDeploymentName()}
			}
		}

	case PatternPollerGroupID:
		if getter, ok := req.(pollerGroupIDGetter); ok {
			return namespace.RoutingKey{ID: getter.GetPollerGroupId(), Strategy: namespace.RoutingStrategyPollerGroup}
		}

	case PatternNamespace:
		if getter, ok := req.(namespaceGetter); ok {
			return namespace.RoutingKey{ID: getter.GetNamespace()}
		}

	case PatternUpdateRef:
		if getter, ok := req.(updateRefGetter); ok {
			return namespace.RoutingKey{ID: getter.GetUpdateRef().GetWorkflowExecution().GetWorkflowId()}
		}

	case PatternNone:
		// No extraction needed

	default:
		// Unknown pattern
	}

	return namespace.RoutingKey{}
}

// extractMultiOperation extracts routing key from ExecuteMultiOperationRequest.
// The routing key is extracted from the first operation's StartWorkflow request.
func (e RoutingKeyExtractor) extractMultiOperation(req any) namespace.RoutingKey {
	multiOpReq, ok := req.(*workflowservice.ExecuteMultiOperationRequest)
	if !ok {
		return namespace.RoutingKey{}
	}

	ops := multiOpReq.GetOperations()
	if len(ops) == 0 {
		return namespace.RoutingKey{}
	}

	firstOp := ops[0]
	if firstOp == nil {
		return namespace.RoutingKey{}
	}

	startWorkflow := firstOp.GetStartWorkflow()
	if startWorkflow == nil {
		// First operation is not StartWorkflow - try to get from UpdateWorkflow
		updateWorkflow := firstOp.GetUpdateWorkflow()
		if updateWorkflow != nil && updateWorkflow.GetWorkflowExecution() != nil {
			return namespace.RoutingKey{ID: updateWorkflow.GetWorkflowExecution().GetWorkflowId()}
		}
		return namespace.RoutingKey{}
	}

	return namespace.RoutingKey{ID: startWorkflow.GetWorkflowId()}
}
