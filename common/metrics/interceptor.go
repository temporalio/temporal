package metrics

import (
	"context"
	"strings"
	"time"

	"google.golang.org/grpc"
)

type (
	metricsClientInterceptor struct {
		handler MetricsHandler
		reqs    CounterMetric
		timers  TimerMetric
		errs    CounterMetric
	}

	MetricsClientInterceptor = grpc.UnaryClientInterceptor
)

const (
	methodSeparator  = "/"
	serviceSeparator = "."
)

var _ grpc.UnaryClientInterceptor = (*metricsClientInterceptor)(nil).Intercept

func NewMetricsClientInterceptor(h MetricsHandler) *metricsClientInterceptor {
	return &metricsClientInterceptor{
		handler: h,
		reqs:    h.Counter("client_requests"),
		timers:  h.Timer("client_latency"),
		errs:    h.Counter("client_errors"),
	}
}

// Intercept emits metrics for client grpc requests
// Counter for request
// Counter for errors
// Timer for request latency
// Operation tag extracted from request method
func (mci *metricsClientInterceptor) Intercept(
	ctx context.Context,
	method string,
	req interface{},
	reply interface{},
	cc *grpc.ClientConn,
	invoker grpc.UnaryInvoker,
	opts ...grpc.CallOption,
) error {
	m := splitMethod(method)
	mci.reqs.Record(1, OperationTag(m))

	defer func(t time.Time) {
		mci.timers.Record(time.Since(t), OperationTag(m))
	}(time.Now().UTC())

	err := invoker(ctx, method, req, reply, cc, opts...)
	if err != nil {
		mci.errs.Record(1, OperationTag(m), ErrorTypeTag(err), ErrorTag(err))
	}

	return err
}

// return service name and method name only
// example: "/temporal.server.api.adminservice.v1.AdminService/GetTaskQueueTasks"
// returns: "AdminServiceGetTaskQueueTasks"
// if markers are not found the entire method is used to help with backfilling
func splitMethod(method string) string {
	if s := strings.LastIndex(method, serviceSeparator); s != -1 {
		svc := method[s+1:]
		if i := strings.LastIndex(svc, methodSeparator); i != -1 {
			return strings.Replace(svc, methodSeparator, "", 1)
		}
	}

	return method
}
