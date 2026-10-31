package interceptor

import (
	"context"
	"time"

	"go.temporal.io/server/common/api"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/rpc/interceptor/logtags"
	"go.temporal.io/server/common/tasktoken"
	"google.golang.org/grpc"
)

type SlowRequestLoggerInterceptor struct {
	logger               log.Logger
	workflowTags         *logtags.WorkflowTags
	namespaceRegistry    namespace.Registry
	slowRequestThreshold dynamicconfig.DurationPropertyFn
}

func NewSlowRequestLoggerInterceptor(
	logger log.Logger,
	namespaceRegistry namespace.Registry,
	slowRequestThreshold dynamicconfig.DurationPropertyFn,
) *SlowRequestLoggerInterceptor {
	return &SlowRequestLoggerInterceptor{
		logger:               logger,
		workflowTags:         logtags.NewWorkflowTags(tasktoken.NewSerializer(), logger),
		namespaceRegistry:    namespaceRegistry,
		slowRequestThreshold: slowRequestThreshold,
	}
}

func (i *SlowRequestLoggerInterceptor) Intercept(
	ctx context.Context,
	request any,
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (any, error) {
	// Long-polled methods aren't useful logged.
	if api.GetMethodMetadata(info.FullMethod).Polling == api.PollingNone {
		startTime := time.Now()

		defer func() {
			elapsed := time.Since(startTime)
			if elapsed > i.slowRequestThreshold() {
				i.logSlowRequest(request, info, elapsed)
			}
		}()
	}

	return handler(ctx, request)
}

func (i *SlowRequestLoggerInterceptor) logSlowRequest(
	request any,
	info *grpc.UnaryServerInfo,
	elapsed time.Duration,
) {
	method := info.FullMethod

	tags := i.workflowTags.Extract(request, method)
	// WorkflowTags.Extract only surfaces workflow/run/activity identifiers found on the
	// request message; it does not include the namespace. Resolve it the same way other
	// interceptors (e.g. TelemetryInterceptor, CallerInfoInterceptor) do, so slow-request
	// logs can reliably be filtered/queried by namespace.
	if nsName := MustGetNamespaceName(i.namespaceRegistry, request); nsName != namespace.EmptyName {
		tags = append(tags, tag.WorkflowNamespace(nsName.String()))
	}
	tags = append(tags, tag.Duration("duration", elapsed))
	tags = append(tags, tag.String("method", method))

	i.logger.Warn("Slow gRPC call", tags...)
}
