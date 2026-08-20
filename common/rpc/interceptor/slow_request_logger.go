package interceptor

import (
	"context"
	"time"

	"go.temporal.io/server/common/api"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/rpc/interceptor/logtags"
	"go.temporal.io/server/common/rpc/interceptor/nexus"
	"go.temporal.io/server/common/tasktoken"
	"google.golang.org/grpc"
)

type SlowRequestLoggerInterceptor struct {
	logger               log.Logger
	workflowTags         *logtags.WorkflowTags
	slowRequestThreshold dynamicconfig.DurationPropertyFn
}

func NewSlowRequestLoggerInterceptor(
	logger log.Logger,
	slowRequestThreshold dynamicconfig.DurationPropertyFn,
) *SlowRequestLoggerInterceptor {
	return &SlowRequestLoggerInterceptor{
		logger:               logger,
		workflowTags:         logtags.NewWorkflowTags(tasktoken.NewSerializer(), logger),
		slowRequestThreshold: slowRequestThreshold,
	}
}

func (i *SlowRequestLoggerInterceptor) Intercept(
	ctx context.Context,
	request any,
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (any, error) {
	tracker := i.trackSlowRequestFn(info.FullMethod, request)
	defer tracker()

	return handler(ctx, request)
}

func (i *SlowRequestLoggerInterceptor) InterceptNexus(
	ctx context.Context,
	in nexus.InterceptorInput,
	next nexus.HandlerFunc,
) (any, error) {
	tracker := i.trackSlowRequestFn(in.OperationName(), in)
	defer tracker()
	return next(ctx, in)
}

func (i *SlowRequestLoggerInterceptor) trackSlowRequestFn(operationName string, req any) func() {
	// Long-polled methods aren't useful logged.
	// If it's a polled method, return a no-op function to defer
	if api.GetMethodMetadata(operationName).Polling != api.PollingNone {
		return func() {}
	}

	startTime := time.Now()

	// Return the cleanup closure for the parent to defer
	return func() {
		elapsed := time.Since(startTime)
		if elapsed > i.slowRequestThreshold() {
			i.logSlowRequest(req, operationName, elapsed)
		}
	}
}

func (i *SlowRequestLoggerInterceptor) logSlowRequest(
	request any,
	method string,
	elapsed time.Duration,
) {

	tags := i.workflowTags.Extract(request, method)
	tags = append(tags, tag.Duration("duration", elapsed))
	tags = append(tags, tag.String("method", method))

	i.logger.Warn("Slow gRPC call", tags...)
}
