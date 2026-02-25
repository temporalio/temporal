package interceptor

import (
	"context"
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/aggregate"
	"go.temporal.io/server/common/api"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"google.golang.org/grpc"
)

type (

	// HealthCheckInterceptor is a gRPC interceptor that records health metrics
	HealthCheckInterceptor struct {
		healthSignalAggregator HealthSignalAggregator
	}

	// HealthSignalAggregator interface for aggregating health signals
	HealthSignalAggregator interface {
		Record(latency time.Duration, err error)
		AverageLatency() float64
		ErrorRatio() float64
	}

	// HealthSignalAggregatorImpl implements HealthSignalAggregator
	healthSignalAggregatorImpl struct {
		status int32

		aggregatorEnabled dynamicconfig.BoolPropertyFn

		latencyAverage aggregate.MovingWindowAverage
		errorRatio     aggregate.MovingWindowAverage

		logger log.Logger
	}
)

var excludedAPIsForHealthSignal = map[string]struct{}{
	"DeepHealthCheck":              {},
	"PollMutableState":             {},
	"PollWorkflowExecutionUpdate":  {},
	"PollWorkflowExecutionHistory": {},
	"UpdateWorkflowExecution":      {},
	// QueryWorkflow is excluded because its latency reflects worker availability,
	// not history node health. With no workers polling, queries block ~30s until
	// context deadline, which can push AverageLatency() past the threshold
	// and cause healthy nodes to report HEALTH_STATE_NOT_SERVING.
	"QueryWorkflow": {},
}

var getWorkflowExecutionHistoryAPI = "GetWorkflowExecutionHistory"

// NewHealthCheckInterceptor creates a new health check interceptor
func NewHealthCheckInterceptor(healthSignalAggregator HealthSignalAggregator) *HealthCheckInterceptor {
	return &HealthCheckInterceptor{
		healthSignalAggregator: healthSignalAggregator,
	}
}

// UnaryIntercept implements the gRPC unary interceptor interface
func (h *HealthCheckInterceptor) UnaryIntercept(
	ctx context.Context,
	req any,
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (any, error) {
	startTime := time.Now()
	resp, err := handler(ctx, req)
	elapsed := time.Since(startTime)

	// Skip health check recording for specific methods
	methodName := api.MethodName(info.FullMethod)

	// Skip GetWorkflowExecutionHistory polling request
	if methodName == getWorkflowExecutionHistoryAPI {
		if request, ok := req.(*historyservice.GetWorkflowExecutionHistoryRequest); ok {
			if r := request.GetRequest(); r != nil && r.GetWaitNewEvent() {
				return resp, err
			}
		}
	}

	if _, ok := excludedAPIsForHealthSignal[methodName]; !ok {
		h.healthSignalAggregator.Record(elapsed, err)
	}
	return resp, err
}

// NewHealthSignalAggregator creates a new instance of HealthSignalAggregatorImpl
func NewHealthSignalAggregator(
	logger log.Logger,
	aggregatorEnabled dynamicconfig.BoolPropertyFn,
	windowSize time.Duration,
	maxBufferSize int,
) *healthSignalAggregatorImpl {
	ret := &healthSignalAggregatorImpl{
		logger:            logger,
		aggregatorEnabled: aggregatorEnabled,
		latencyAverage:    aggregate.NewMovingWindowAvgImpl(windowSize, maxBufferSize),
		errorRatio:        aggregate.NewMovingWindowAvgImpl(windowSize, maxBufferSize),
	}
	return ret
}

func (s *healthSignalAggregatorImpl) Record(latency time.Duration, err error) {
	if !s.aggregatorEnabled() {
		s.logger.Debug("health signal aggregator is disabled")
		return
	}
	s.latencyAverage.Record(latency.Milliseconds())

	if isUnhealthyError(err) {
		s.errorRatio.Record(1)
	} else {
		s.errorRatio.Record(0)
	}
}

func (s *healthSignalAggregatorImpl) AverageLatency() float64 {
	if !s.aggregatorEnabled() {
		s.logger.Debug("health signal aggregator is disabled")
	}
	return s.latencyAverage.Average()
}

func (s *healthSignalAggregatorImpl) ErrorRatio() float64 {
	if !s.aggregatorEnabled() {
		s.logger.Debug("health signal aggregator is disabled")
	}
	return s.errorRatio.Average()
}

func isUnhealthyError(err error) bool {
	if err == nil {
		return false
	}
	if common.IsContextCanceledErr(err) {
		return true
	}
	if common.IsContextDeadlineExceededErr(err) {
		return true
	}

	switch err.(type) {
	case *serviceerror.Unavailable,
		*serviceerror.DeadlineExceeded,
		*serviceerror.Canceled:
		return true
	}
	return false
}
