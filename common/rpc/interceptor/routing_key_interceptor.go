package interceptor

import (
	"context"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	"google.golang.org/grpc"
)

type (
	routingKeyContextKey struct{}

	// RoutingKeyPattern defines the expected interface pattern for extracting routing key
	RoutingKeyPattern int

	// RoutingKeyExtractorFunc extracts routing key from a request.
	// Returns a zero-value RoutingKey if this extractor doesn't handle the request.
	RoutingKeyExtractorFunc func(ctx context.Context, req any, fullMethod string) namespace.RoutingKey

	// RoutingKeyInterceptor extracts routing key from requests and adds it to context.
	// It iterates through a list of extractor functions until one returns a non-empty routing key.
	RoutingKeyInterceptor struct {
		extractors []RoutingKeyExtractorFunc
		logger     log.Logger
	}
)

var routingKeyCtxKey = routingKeyContextKey{}

const (
	// PatternNone indicates no routing key extraction is needed
	PatternNone RoutingKeyPattern = iota
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
	// PatternTaskQueueName indicates extraction via GetTaskQueue() string method
	PatternTaskQueueName
	// PatternTaskQueueNameFromMessage indicates extraction via GetTaskQueue().GetName() (TaskQueue message)
	PatternTaskQueueNameFromMessage
	// PatternDeploymentName indicates extraction via GetDeploymentName() method
	PatternDeploymentName
	// PatternDeploymentVersion indicates extraction via GetDeploymentVersion().GetDeploymentName()
	PatternDeploymentVersion
	// PatternPollerGroupID indicates extraction via GetPollerGroupId() directly
	PatternPollerGroupID
	// PatternNamespace indicates extraction via GetNamespace() - used when we want to send all calls to a particular api and namespace to a single cell at a time.
	PatternNamespace
	// PatternUpdateRef indicates extraction via GetUpdateRef().GetWorkflowExecution().GetWorkflowId()
	PatternUpdateRef
)

// methodToPattern maps API method names to their expected routing key extraction pattern.
// Methods not in this map are treated as PatternNone (no routing key extraction needed).
var methodToPattern = map[string]RoutingKeyPattern{
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

	// task queue name
	"UpdateTaskQueueConfig": PatternTaskQueueName,

	// task queue name (from TaskQueue message)
	"ListTaskQueuePartitions": PatternTaskQueueNameFromMessage,

	// deployment name
	"DescribeWorkerDeployment":          PatternDeploymentName,
	"DeleteWorkerDeployment":            PatternDeploymentName,
	"SetWorkerDeploymentCurrentVersion": PatternDeploymentName,
	"SetWorkerDeploymentManager":        PatternDeploymentName,
	"SetWorkerDeploymentRampingVersion": PatternDeploymentName,

	// deployment name (from WorkerDeploymentVersion message)
	"DescribeWorkerDeploymentVersion":       PatternDeploymentVersion,
	"DeleteWorkerDeploymentVersion":         PatternDeploymentVersion,
	"UpdateWorkerDeploymentVersionMetadata": PatternDeploymentVersion,

	// namespace (deterministic routing to a single cell for the namespace)
	// TODO: Switch to worker_grouping_key when available for load balancing
	"FetchWorkerConfig":     PatternNamespace,
	"UpdateWorkerConfig":    PatternNamespace,
	"DescribeWorker":        PatternNamespace,
	"RecordWorkerHeartbeat": PatternNamespace,

	// workflow ID (from UpdateRef)
	"PollWorkflowExecutionUpdate": PatternUpdateRef,

	"PollWorkflowTaskQueue":     PatternPollerGroupID,
	"PollActivityTaskQueue":     PatternPollerGroupID,
	"PollNexusTaskQueue":        PatternPollerGroupID,
	"RespondQueryTaskCompleted": PatternPollerGroupID,
	"RespondNexusTaskCompleted": PatternPollerGroupID,
	"RespondNexusTaskFailed":    PatternPollerGroupID,
}

// NewRoutingKeyInterceptor creates a new RoutingKeyInterceptor with the given extractor functions.
// Extractors are called in order until one returns a non-empty routing key.
func NewRoutingKeyInterceptor(
	extractors []RoutingKeyExtractorFunc,
	logger log.Logger,
) *RoutingKeyInterceptor {
	return &RoutingKeyInterceptor{
		extractors: extractors,
		logger:     logger,
	}
}

// WithExtractors returns a new interceptor with additional extractors prepended.
// The new extractors will be tried before the existing ones.
func (i *RoutingKeyInterceptor) WithExtractors(extractors ...RoutingKeyExtractorFunc) *RoutingKeyInterceptor {
	return &RoutingKeyInterceptor{
		extractors: append(extractors, i.extractors...),
		logger:     i.logger,
	}
}

var _ grpc.UnaryServerInterceptor = (*RoutingKeyInterceptor)(nil).Intercept

// Intercept extracts routing key from the request and adds it to the context.
// It tries each extractor in order until one returns a non-empty routing key.
func (i *RoutingKeyInterceptor) Intercept(
	ctx context.Context,
	req any,
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (any, error) {
	// Try each extractor until one returns a non-empty businessID
	for _, extractor := range i.extractors {
		if key := extractor(ctx, req, info.FullMethod); key.ID != "" || key.Strategy != namespace.RoutingStrategyDefault {
			i.logger.Debug("routing key extraction: adding routing key to context",
				tag.WorkflowID(key.ID),
				tag.String("grpc-method", info.FullMethod),
			)
			ctx = AddRoutingKeyToContext(ctx, key)
			break
		}
	}

	return handler(ctx, req)
}

// AddRoutingKeyToContext adds the routing Key to the context
func AddRoutingKeyToContext(ctx context.Context, routingKey namespace.RoutingKey) context.Context {
	return context.WithValue(ctx, routingKeyCtxKey, routingKey)
}

// GetRoutingKeyFromContext retrieves the routing Key from the context.
// Returns a zero-value RoutingKey if not found.
func GetRoutingKeyFromContext(ctx context.Context) namespace.RoutingKey {
	if key, ok := ctx.Value(routingKeyCtxKey).(namespace.RoutingKey); ok {
		return key
	}
	return namespace.RoutingKey{}
}
