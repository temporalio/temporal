package interceptor

import (
	"context"
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/aggregate"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"google.golang.org/grpc"
)

// HealthCheckInterceptor is a gRPC interceptor that records health metrics
type HealthCheckInterceptor struct {
	historyHealthSignalAggregator HealthSignalAggregator
}

// NewHealthCheckInterceptor creates a new health check interceptor
func NewHealthCheckInterceptor(historyHealthSignalAggregator HealthSignalAggregator) *HealthCheckInterceptor {
	return &HealthCheckInterceptor{
		historyHealthSignalAggregator: historyHealthSignalAggregator,
	}
}

// UnaryIntercept implements the gRPC unary interceptor interface
func (h *HealthCheckInterceptor) UnaryIntercept(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	startTime := time.Now()
	resp, err := handler(ctx, req)
	elapsed := time.Since(startTime)

	h.historyHealthSignalAggregator.Record(elapsed, err)

	return resp, err
}

type (
	// HealthSignalAggregator interface for aggregating health signals
	HealthSignalAggregator interface {
		Record(latency time.Duration, err error)
		AverageLatency() float64
		ErrorRatio() float64
	}

	// HealthSignalAggregatorImpl implements HealthSignalAggregator
	HealthSignalAggregatorImpl struct {
		status int32

		aggregatorEnabled dynamicconfig.BoolPropertyFn

		latencyAverage aggregate.MovingWindowAverage
		errorRatio     aggregate.MovingWindowAverage

		logger log.Logger
	}

	NoopSignalAggregator struct{}
)

// NewHealthSignalAggregator creates a new instance of HealthSignalAggregatorImpl
func NewHealthSignalAggregator(
	logger log.Logger,
	aggregatorEnabled dynamicconfig.BoolPropertyFn,
	windowSize time.Duration,
	maxBufferSize int,
) *HealthSignalAggregatorImpl {
	ret := &HealthSignalAggregatorImpl{
		logger:            logger,
		aggregatorEnabled: aggregatorEnabled,
		latencyAverage:    aggregate.NewMovingWindowAvgImpl(windowSize, maxBufferSize),
		errorRatio:        aggregate.NewMovingWindowAvgImpl(windowSize, maxBufferSize),
	}
	return ret
}

func (s *HealthSignalAggregatorImpl) Record(latency time.Duration, err error) {
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

func (s *HealthSignalAggregatorImpl) AverageLatency() float64 {
	if !s.aggregatorEnabled() {
		s.logger.Debug("health signal aggregator is disabled")
	}
	return s.latencyAverage.Average()
}

func (s *HealthSignalAggregatorImpl) ErrorRatio() float64 {
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
