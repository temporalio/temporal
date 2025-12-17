package interceptor

import (
	"context"
	"strings"

	"go.temporal.io/server/common/api"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	"google.golang.org/grpc"
)

type (
	workflowIDContextKey struct{}

	// WorkflowIDPattern defines the expected interface pattern for extracting workflow ID
	WorkflowIDPattern int

	// WorkflowIDInterceptor extracts workflow ID from requests and adds it to context
	WorkflowIDInterceptor struct {
		extractor WorkflowIDExtractor
		logger    log.Logger
	}
)

var workflowIDCtxKey = workflowIDContextKey{}

const (
	// PatternNone indicates no workflow ID extraction is needed
	PatternNone WorkflowIDPattern = iota
	// PatternWorkflowID indicates extraction via GetWorkflowId() method
	PatternWorkflowID
	// PatternWorkflowExecution indicates extraction via GetWorkflowExecution().GetWorkflowId()
	PatternWorkflowExecution
	// PatternExecution indicates extraction via GetExecution().GetWorkflowId()
	PatternExecution
	// PatternTaskToken indicates extraction via deserializing GetTaskToken()
	PatternTaskToken
	// PatternMultiOperation indicates extraction from ExecuteMultiOperationRequest
	PatternMultiOperation
)

// methodToPattern maps API method names to their expected workflow ID extraction pattern.
// Methods not in this map are treated as PatternNone (no workflow ID extraction needed).
var methodToPattern = map[string]WorkflowIDPattern{
	// Pattern: GetWorkflowId() - direct WorkflowId field
	"StartWorkflowExecution":           PatternWorkflowID,
	"SignalWithStartWorkflowExecution": PatternWorkflowID,
	"PauseWorkflowExecution":           PatternWorkflowID,
	"UnpauseWorkflowExecution":         PatternWorkflowID,
	"RecordActivityTaskHeartbeatById":  PatternWorkflowID,
	"RespondActivityTaskCompletedById": PatternWorkflowID,
	"RespondActivityTaskCanceledById":  PatternWorkflowID,
	"RespondActivityTaskFailedById":    PatternWorkflowID,

	// Pattern: GetWorkflowExecution().GetWorkflowId()
	"DeleteWorkflowExecution":        PatternWorkflowExecution,
	"RequestCancelWorkflowExecution": PatternWorkflowExecution,
	"ResetWorkflowExecution":         PatternWorkflowExecution,
	"SignalWorkflowExecution":        PatternWorkflowExecution,
	"TerminateWorkflowExecution":     PatternWorkflowExecution,
	"UpdateWorkflowExecution":        PatternWorkflowExecution,
	"UpdateWorkflowExecutionOptions": PatternWorkflowExecution,

	// Pattern: GetExecution().GetWorkflowId()
	"DescribeWorkflowExecution":          PatternExecution,
	"GetWorkflowExecutionHistory":        PatternExecution,
	"GetWorkflowExecutionHistoryReverse": PatternExecution,
	"QueryWorkflow":                      PatternExecution,
	"ResetStickyTaskQueue":               PatternExecution,
	"ResetActivity":                      PatternExecution,
	"PauseActivity":                      PatternExecution,
	"UnpauseActivity":                    PatternExecution,
	"UpdateActivityOptions":              PatternExecution,
	"TriggerWorkflowRule":                PatternExecution,

	// Pattern: TaskToken deserialization
	"RecordActivityTaskHeartbeat":  PatternTaskToken,
	"RespondActivityTaskCompleted": PatternTaskToken,
	"RespondActivityTaskCanceled":  PatternTaskToken,
	"RespondActivityTaskFailed":    PatternTaskToken,
	"RespondWorkflowTaskCompleted": PatternTaskToken,
	"RespondWorkflowTaskFailed":    PatternTaskToken,

	// Pattern: ExecuteMultiOperation special handling
	"ExecuteMultiOperation": PatternMultiOperation,
}

func NewWorkflowIDInterceptor(
	extractor WorkflowIDExtractor,
	logger log.Logger,
) *WorkflowIDInterceptor {
	return &WorkflowIDInterceptor{
		extractor: extractor,
		logger:    logger,
	}
}

var _ grpc.UnaryServerInterceptor = (*WorkflowIDInterceptor)(nil).Intercept

// Intercept extracts workflow ID from the request and adds it to the context
func (i *WorkflowIDInterceptor) Intercept(
	ctx context.Context,
	req any,
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (any, error) {
	// Only process WorkflowService APIs
	if !strings.HasPrefix(info.FullMethod, api.WorkflowServicePrefix) {
		return handler(ctx, req)
	}

	methodName := api.MethodName(info.FullMethod)
	pattern, hasPattern := methodToPattern[methodName]
	if !hasPattern {
		return handler(ctx, req)
	}

	workflowID := i.extractor.Extract(req, pattern)

	i.logger.Debug("workflow ID extraction: adding workflow ID to context",
		tag.Operation(methodName),
		tag.WorkflowID(workflowID),
	)

	ctx = AddWorkflowIDContext(ctx, workflowID)
	return handler(ctx, req)
}

// AddWorkflowIDContext adds the workflow ID to the context
func AddWorkflowIDContext(ctx context.Context, workflowID string) context.Context {
	return context.WithValue(ctx, workflowIDCtxKey, workflowID)
}

// GetWorkflowIDFromContext retrieves the workflow ID from the context.
// Returns namespace.EmptyBusinessID if not found.
func GetWorkflowIDFromContext(ctx context.Context) string {
	if workflowID, ok := ctx.Value(workflowIDCtxKey).(string); ok {
		return workflowID
	}
	return namespace.EmptyBusinessID
}
