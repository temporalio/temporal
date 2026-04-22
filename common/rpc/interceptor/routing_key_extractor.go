//go:generate go run ../../../cmd/tools/genroutingkeyextractor -out .

package interceptor

import (
	"context"
	"strings"

	commonpb "go.temporal.io/api/common/v1"
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

// WorkflowServiceExtractor returns a RoutingKeyExtractorFunc that extracts the
// routing key from WorkflowService API requests using the provided
// RoutingKeyExtractor.
func WorkflowServiceExtractor(extractor RoutingKeyExtractor) RoutingKeyExtractorFunc {
	return func(_ context.Context, req any, fullMethod string) namespace.RoutingKey {
		// Only process WorkflowService APIs
		if !strings.HasPrefix(fullMethod, api.WorkflowServicePrefix) {
			return namespace.RoutingKey{}
		}

		// Prefer the generated extractor driven by temporal-resource-id proto
		// annotations.
		if key := workflowServiceRequestRoutingKey(req); key.ID != "" {
			return key
		}

		// Fall back to pattern-based logic for methods the codegen can't cover
		// (task token deserialization, multi-operation, namespace-level
		// routing) and as a compatibility path for methods whose callers
		// haven't populated the resource_id field yet.
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

	executionGetter interface {
		GetExecution() *commonpb.WorkflowExecution
	}

	taskTokenGetter interface {
		GetTaskToken() []byte
	}

	namespaceGetter interface {
		GetNamespace() string
	}
)

// Extract extracts routing key from the request using the specified pattern.
// Returns a zero-value namespace.RoutingKey if not found.
func (e RoutingKeyExtractor) Extract(req any, pattern RoutingKeyPattern) namespace.RoutingKey {
	if req == nil {
		return namespace.RoutingKey{}
	}

	switch pattern {
	case PatternWorkflowID:
		if getter, ok := req.(workflowIDGetter); ok {
			return namespace.RoutingKey{ID: getter.GetWorkflowId()}
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

	case PatternNamespace:
		if getter, ok := req.(namespaceGetter); ok {
			return namespace.RoutingKey{ID: getter.GetNamespace()}
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

// routingIDFromResourceID extracts the routing ID from a resource_id field value.
// The resource_id field has the format "prefix:<routingID>".
func routingIDFromResourceID(resourceID string) string {
	_, routingID, _ := strings.Cut(resourceID, ":")
	return routingID
}
