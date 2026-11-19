package interceptor

import (
	"context"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/tasktoken"
	"go.temporal.io/server/common/tqid"
	"google.golang.org/grpc"
)

var (
	ErrInternalPerNSNamespaceRateLimitServerBusy = &serviceerror.ResourceExhausted{
		Cause:   enumspb.RESOURCE_EXHAUSTED_CAUSE_RPS_LIMIT,
		Scope:   enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE,
		Message: "namespace rate limit exceeded for per-namespace system worker requests",
	}
)

type (
	// InternalPerNSNamespaceRateLimitInterceptor charges the runtime requests of per-namespace system workers to a
	// separate namespace rate limiter, so system workflow overhead neither consumes nor is starved by the
	// namespace's regular quota. Actions that system workflows take on customer workflows keep regular accounting.
	// Callers of Allow and Wait without a request, such as Nexus HTTP handlers, always use the regular limiter.
	InternalPerNSNamespaceRateLimitInterceptor struct {
		NamespaceRateLimitInterceptor

		internalPerNS   NamespaceRateLimitInterceptor
		enabled         dynamicconfig.BoolPropertyFnWithNamespaceFilter
		tokenSerializer *tasktoken.Serializer
	}
)

var _ NamespaceRateLimitInterceptor = (*InternalPerNSNamespaceRateLimitInterceptor)(nil)

func NewInternalPerNSNamespaceRateLimitInterceptor(
	regular NamespaceRateLimitInterceptor,
	internalPerNS NamespaceRateLimitInterceptor,
	enabled dynamicconfig.BoolPropertyFnWithNamespaceFilter,
) *InternalPerNSNamespaceRateLimitInterceptor {
	return &InternalPerNSNamespaceRateLimitInterceptor{
		NamespaceRateLimitInterceptor: regular,
		internalPerNS:                 internalPerNS,
		enabled:                       enabled,
		tokenSerializer:               tasktoken.NewSerializer(),
	}
}

func (i *InternalPerNSNamespaceRateLimitInterceptor) Intercept(
	ctx context.Context,
	req any,
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (any, error) {
	return i.InterceptN(ctx, req, info, handler, NamespaceRateLimitDefaultToken)
}

func (i *InternalPerNSNamespaceRateLimitInterceptor) InterceptN(
	ctx context.Context,
	req any,
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
	numToken int,
) (any, error) {
	if i.isInternalPerNSRequest(req) {
		resp, err := i.internalPerNS.InterceptN(ctx, req, info, handler, numToken)
		// A distinct error tells operators which budget throttled the namespace without adding metric series.
		if err == ErrNamespaceRateLimitServerBusy {
			return nil, ErrInternalPerNSNamespaceRateLimitServerBusy
		}
		return resp, err
	}
	return i.NamespaceRateLimitInterceptor.InterceptN(ctx, req, info, handler, numToken)
}

// isInternalPerNSRequest classifies by request shape, like the internal per-namespace concurrency budget. The
// shape is client-controlled: a client can poll an internal task queue or give its own workflows a "temporal-sys-"
// ID, so the internal limiter must stay bounded and is opt-in per namespace. The enabled check runs before any task
// token is decoded so that namespaces without the split never pay for decoding; where the shape check is a cheap
// string comparison it runs first, so customer requests skip the dynamic config lookup.
//
// Not covered, so they stay on the regular limiter: sticky query responses (the query token names the sticky
// queue), Nexus task responses (their token carries no task queue), and batch operation workflows (their ID is the
// customer's job ID).
func (i *InternalPerNSNamespaceRateLimitInterceptor) isInternalPerNSRequest(req any) bool {
	switch r := req.(type) {
	case *workflowservice.PollWorkflowTaskQueueRequest,
		*workflowservice.PollActivityTaskQueueRequest,
		*workflowservice.PollNexusTaskQueueRequest:
		return isInternalPerNSPoll(req) && i.enabledFor(req)
	case *workflowservice.GetWorkflowExecutionHistoryRequest:
		return primitives.IsInternalWorkflowID(r.GetExecution().GetWorkflowId()) && i.enabledFor(req)
	case *workflowservice.RespondQueryTaskCompletedRequest:
		if !i.enabledFor(req) {
			return false
		}
		// An undecodable token is left to the regular limiter; the handler rejects it.
		token, err := i.tokenSerializer.DeserializeQueryTaskToken(r.GetTaskToken())
		if err != nil {
			return false
		}
		// Load-balanced queries are matched on any partition, so the token holds the partition's RPC name.
		partition, err := tqid.NormalPartitionFromRpcName(
			token.GetTaskQueue(),
			token.GetNamespaceId(),
			enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		)
		return err == nil && primitives.IsInternalPerNsTaskQueue(partition.TaskQueue().Name())
	case *workflowservice.RespondWorkflowTaskCompletedRequest,
		*workflowservice.RespondWorkflowTaskFailedRequest,
		*workflowservice.RespondActivityTaskCompletedRequest,
		*workflowservice.RespondActivityTaskFailedRequest,
		*workflowservice.RespondActivityTaskCanceledRequest,
		*workflowservice.RecordActivityTaskHeartbeatRequest:
		if !i.enabledFor(req) {
			return false
		}
		// An undecodable token is left to the regular limiter; the handler rejects it.
		getter, ok := req.(taskTokenGetter)
		if !ok {
			return false
		}
		token, err := i.tokenSerializer.Deserialize(getter.GetTaskToken())
		return err == nil && primitives.IsInternalWorkflowID(token.GetWorkflowId())
	default:
		return false
	}
}

func (i *InternalPerNSNamespaceRateLimitInterceptor) enabledFor(req any) bool {
	r, ok := req.(NamespaceNameGetter)
	return ok && i.enabled(r.GetNamespace())
}
