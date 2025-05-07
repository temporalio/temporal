package interceptor

import (
	"context"
	"strings"
	"time"

	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/rpc/interceptor/logtags"
	"go.temporal.io/server/common/tasktoken"
	"google.golang.org/grpc"
)

// Certain types of methods are ignored as a rule.
var ignoredMethodSubstrings = []string{"Poll", "GetWorkflowExecutionHistory"}

type SlowRequestLoggerInterceptor struct {
	logger       log.Logger
	workflowTags *logtags.WorkflowTags
	dc           *dynamicconfig.Collection
}

func NewSlowRequestLoggerInterceptor(
	logger log.Logger,
	dc *dynamicconfig.Collection,
) *SlowRequestLoggerInterceptor {
	return &SlowRequestLoggerInterceptor{
		logger:       logger,
		workflowTags: logtags.NewWorkflowTags(tasktoken.NewSerializer(), logger),
		dc:           dc,
	}
}

func (i *SlowRequestLoggerInterceptor) Intercept(
	ctx context.Context,
	request interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	threshold := dynamicconfig.SlowRequestLoggingThreshold.Get(i.dc)()
	startTime := time.Now()

	defer func() {
		elapsed := time.Since(startTime)
		if elapsed > threshold {
			i.logSlowRequest(request, info, elapsed)
		}
	}()

	return handler(ctx, request)
}

func (i *SlowRequestLoggerInterceptor) logSlowRequest(
	request interface{},
	info *grpc.UnaryServerInfo,
	elapsed time.Duration,
) {
	method := info.FullMethod

	// Certain methods aren't useful logged.
	for _, substr := range ignoredMethodSubstrings {
		if strings.Contains(method, substr) {
			return
		}
	}

	tags := i.workflowTags.Extract(request, method)
	tags = append(tags, tag.NewDurationTag("duration", elapsed))
	tags = append(tags, tag.NewStringTag("method", method))

	i.logger.Warn("Slow gRPC call", tags...)
}
