package interceptor

import (
	"context"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	"google.golang.org/grpc"
)

type (
	businessIDContextKey struct{}

	// BusinessIDPattern defines the expected interface pattern for extracting business ID
	BusinessIDPattern int

	// BusinessIDExtractorFunc extracts business ID from a request.
	// Returns empty string if this extractor doesn't handle the request.
	BusinessIDExtractorFunc func(ctx context.Context, req any, fullMethod string) string

	// BusinessIDInterceptor extracts business ID from requests and adds it to context.
	// It iterates through a list of extractor functions until one returns a non-empty business ID.
	BusinessIDInterceptor struct {
		extractors []BusinessIDExtractorFunc
		logger     log.Logger
	}
)

var businessIDCtxKey = businessIDContextKey{}

const (
	// PatternNone indicates no business ID extraction is needed
	PatternNone BusinessIDPattern = iota
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

// methodToPattern maps API method names to their expected business ID extraction pattern.
// Methods not in this map are treated as PatternNone (no business ID extraction needed).
var methodToPattern = map[string]BusinessIDPattern{
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

// NewBusinessIDInterceptor creates a new BusinessIDInterceptor with the given extractor functions.
// Extractors are called in order until one returns a non-empty business ID.
func NewBusinessIDInterceptor(
	extractors []BusinessIDExtractorFunc,
	logger log.Logger,
) *BusinessIDInterceptor {
	return &BusinessIDInterceptor{
		extractors: extractors,
		logger:     logger,
	}
}

// WithExtractors returns a new interceptor with additional extractors prepended.
// The new extractors will be tried before the existing ones.
func (i *BusinessIDInterceptor) WithExtractors(extractors ...BusinessIDExtractorFunc) *BusinessIDInterceptor {
	return &BusinessIDInterceptor{
		extractors: append(extractors, i.extractors...),
		logger:     i.logger,
	}
}

var _ grpc.UnaryServerInterceptor = (*BusinessIDInterceptor)(nil).Intercept

// Intercept extracts business ID from the request and adds it to the context.
// It tries each extractor in order until one returns a non-empty business ID.
func (i *BusinessIDInterceptor) Intercept(
	ctx context.Context,
	req any,
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (any, error) {
	// Try each extractor until one returns a non-empty businessID
	for _, extractor := range i.extractors {
		if businessID := extractor(ctx, req, info.FullMethod); businessID != "" {
			i.logger.Debug("business ID extraction: adding business ID to context",
				tag.WorkflowID(businessID),
				tag.NewStringTag("grpc-method", info.FullMethod),
			)
			ctx = AddBusinessIDToContext(ctx, businessID)
			break
		}
	}

	return handler(ctx, req)
}

// AddBusinessIDToContext adds the business ID to the context
func AddBusinessIDToContext(ctx context.Context, businessID string) context.Context {
	return context.WithValue(ctx, businessIDCtxKey, businessID)
}

// GetBusinessIDFromContext retrieves the business ID from the context.
// Returns namespace.EmptyBusinessID if not found.
func GetBusinessIDFromContext(ctx context.Context) string {
	if businessID, ok := ctx.Value(businessIDCtxKey).(string); ok {
		return businessID
	}
	return namespace.EmptyBusinessID
}
