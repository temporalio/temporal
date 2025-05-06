package interceptor

import (
	"context"
	"fmt"
	"strings"
	"time"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/rpc/interceptor/logtags"
	"go.temporal.io/server/common/tasktoken"
	"google.golang.org/grpc"
)

var (
	// Requests over this threshold will be logged.
	SlowRequestThreshold = 5 * time.Second

	// Certain types of methods are ignored as a rule.
	ignoredMethodSubstrings = []string{"Poll"}
)

type (
	SlowRequestLoggerInterceptor struct {
		logger       log.Logger
		workflowTags *logtags.WorkflowTags
	}
)

func NewSlowRequestLoggerInterceptor(
	logger log.Logger,
) *SlowRequestLoggerInterceptor {
	return &SlowRequestLoggerInterceptor{
		logger:       logger,
		workflowTags: logtags.NewWorkflowTags(tasktoken.NewSerializer(), logger),
	}
}

func (i *SlowRequestLoggerInterceptor) Intercept(
	ctx context.Context,
	request interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	startTime := time.Now()

	defer func() {
		elapsed := time.Now().Sub(startTime)
		if elapsed > SlowRequestThreshold {
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

	wfTags := i.workflowTags.Extract(request, method)
	i.logger.Warn(
		fmt.Sprintf(
			"Slow gRPC call for '%s', took %s",
			method,
			elapsed.String(),
		),
		wfTags...,
	)
}
