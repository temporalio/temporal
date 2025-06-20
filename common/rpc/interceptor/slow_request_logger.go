package interceptor

import (
	"context"
	"time"

	"go.temporal.io/server/common/api"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/rpc/interceptor/logtags"
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
	request interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
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
	request interface{},
	info *grpc.UnaryServerInfo,
	elapsed time.Duration,
) {
	method := info.FullMethod

	tags := i.workflowTags.Extract(request, method)
	tags = append(tags, tag.NewDurationTag("duration", elapsed))
	tags = append(tags, tag.NewStringTag("method", method))

	i.logger.Warn("Slow gRPC call", tags...)
}
