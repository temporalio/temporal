package history

import (
	"sync"
	"sync/atomic"
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/aggregate"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
)

const (
	emitMetricsInterval = 30 * time.Second
)

var (
	HistoryServiceRPS = metrics.NewTimerDef(
		"history_service_rps",
		metrics.WithDescription("The number of requests per second for each history service method."),
	)
)

var NoopHealthSignalAggregator HealthSignalAggregator = newNoopSignalAggregator()

type (
	// HealthSignalAggregator interface for tracking RPC health signals
	HealthSignalAggregator interface {
		Record(method string, latency time.Duration, err error)
		AverageLatency() float64
		ErrorRatio() float64
		Start()
		Stop()
	}

	// HealthSignalAggregatorImpl implements HealthSignalAggregator
	HealthSignalAggregatorImpl struct {
		status     int32
		shutdownCh chan struct{}

		// map of method -> request count
		requestCounts map[string]int64
		requestsLock  sync.Mutex

		aggregationEnabled bool
		latencyAverage     aggregate.MovingWindowAverage
		errorRatio         aggregate.MovingWindowAverage

		metricsHandler          metrics.Handler
		emitMetricsTimer        *time.Ticker
		perMethodRPSWarnLimit   dynamicconfig.IntPropertyFn
		perMethodErrorWarnLimit dynamicconfig.FloatPropertyFn

		logger log.Logger
	}

	noopSignalAggregator struct{}
)

// NewHealthSignalAggregatorImpl creates a new instance of HealthSignalAggregatorImpl
func NewHealthSignalAggregatorImpl(
	aggregationEnabled bool,
	windowSize time.Duration,
	maxBufferSize int,
	metricsHandler metrics.Handler,
	perMethodRPSWarnLimit dynamicconfig.IntPropertyFn,
	perMethodErrorWarnLimit dynamicconfig.FloatPropertyFn,
	logger log.Logger,
) *HealthSignalAggregatorImpl {
	ret := &HealthSignalAggregatorImpl{
		status:                  common.DaemonStatusInitialized,
		shutdownCh:              make(chan struct{}),
		requestCounts:           make(map[string]int64),
		metricsHandler:          metricsHandler,
		emitMetricsTimer:        time.NewTicker(emitMetricsInterval),
		perMethodRPSWarnLimit:   perMethodRPSWarnLimit,
		perMethodErrorWarnLimit: perMethodErrorWarnLimit,
		logger:                  logger,
		aggregationEnabled:      aggregationEnabled,
	}

	if aggregationEnabled {
		ret.latencyAverage = aggregate.NewMovingWindowAvgImpl(windowSize, maxBufferSize)
		ret.errorRatio = aggregate.NewMovingWindowAvgImpl(windowSize, maxBufferSize)
	} else {
		ret.latencyAverage = aggregate.NoopMovingWindowAverage
		ret.errorRatio = aggregate.NoopMovingWindowAverage
	}

	return ret
}

func (s *HealthSignalAggregatorImpl) Start() {
	if !atomic.CompareAndSwapInt32(&s.status, common.DaemonStatusInitialized, common.DaemonStatusStarted) {
		return
	}
	go s.emitMetricsLoop()
}

func (s *HealthSignalAggregatorImpl) Stop() {
	if !atomic.CompareAndSwapInt32(&s.status, common.DaemonStatusStarted, common.DaemonStatusStopped) {
		return
	}
	close(s.shutdownCh)
	s.emitMetricsTimer.Stop()
}

func (s *HealthSignalAggregatorImpl) Record(method string, latency time.Duration, err error) {
	if s.aggregationEnabled {
		s.latencyAverage.Record(latency.Milliseconds())

		if isUnhealthyError(err) {
			s.errorRatio.Record(1)
		} else {
			s.errorRatio.Record(0)
		}
	}

	s.incrementMethodRequestCount(method)
}

func (s *HealthSignalAggregatorImpl) AverageLatency() float64 {
	return s.latencyAverage.Average()
}

func (s *HealthSignalAggregatorImpl) ErrorRatio() float64 {
	return s.errorRatio.Average()
}

func (s *HealthSignalAggregatorImpl) incrementMethodRequestCount(method string) {
	s.requestsLock.Lock()
	defer s.requestsLock.Unlock()
	s.requestCounts[method]++
}

func (s *HealthSignalAggregatorImpl) emitMetricsLoop() {
	for {
		select {
		case <-s.shutdownCh:
			return
		case <-s.emitMetricsTimer.C:
			s.requestsLock.Lock()
			requestCounts := s.requestCounts
			s.requestCounts = make(map[string]int64, len(requestCounts))
			s.requestsLock.Unlock()

			for method, count := range requestCounts {
				methodRPS := int64(float64(count) / emitMetricsInterval.Seconds())
				s.metricsHandler.Histogram(HistoryServiceRPS.Name(), HistoryServiceRPS.Unit()).Record(methodRPS)
				if methodRPS > int64(s.perMethodRPSWarnLimit()) {
					s.logger.Warn("Per method RPS warn limit exceeded",
						tag.NewStringTag("method", method),
						tag.NewInt64("rps", methodRPS))
				}
			}
		}
	}
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

func newNoopSignalAggregator() *noopSignalAggregator { return &noopSignalAggregator{} }

func (a *noopSignalAggregator) Start() {}

func (a *noopSignalAggregator) Stop() {}

func (a *noopSignalAggregator) Record(_ string, _ time.Duration, _ error) {}

func (a *noopSignalAggregator) AverageLatency() float64 {
	return 0
}

func (*noopSignalAggregator) ErrorRatio() float64 {
	return 0
}
