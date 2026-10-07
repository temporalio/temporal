package persistence

import (
	"sync"
	"sync/atomic"
	"time"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/aggregate"
	"go.temporal.io/server/common/health"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
)

const (
	emitMetricsInterval = 30 * time.Second
)

type (
	HealthSignalAggregator interface {
		health.SignalReader

		// Record takes the persistence operation (the metrics scope of the call, e.g.
		// metrics.PersistenceGetOrCreateShardScope) as the key the health signals are
		// grouped by
		Record(operation string, callerSegment int32, latency time.Duration, err error)
		AverageLatency() float64
		Start()
		Stop()
	}

	healthSignalAggregatorImpl struct {
		status     int32
		shutdownCh chan struct{}

		// map of shardID -> request count
		requestCounts map[int32]int64
		requestsLock  sync.Mutex

		aggregationEnabled bool

		latencyAverage aggregate.MovingWindowAverage
		errorRatio     aggregate.MovingWindowAverage

		signals *health.SignalAggregator

		metricsHandler   metrics.Handler
		emitMetricsTimer *time.Ticker

		logger log.Logger
	}
)

var _ health.SignalReader = (*healthSignalAggregatorImpl)(nil)

func NewHealthSignalAggregator(
	aggregationEnabled bool,
	windowSize time.Duration,
	maxBufferSize int,
	metricsHandler metrics.Handler,
	logger log.Logger,
	getSettings func() health.Settings,
) *healthSignalAggregatorImpl {
	signals := health.NewSignalAggregator(logger, getSettings, health.WithIsUnhealthy(isUnhealthyError))

	ret := &healthSignalAggregatorImpl{
		signals:            signals,
		status:             common.DaemonStatusInitialized,
		shutdownCh:         make(chan struct{}),
		requestCounts:      make(map[int32]int64),
		metricsHandler:     metricsHandler,
		emitMetricsTimer:   time.NewTicker(emitMetricsInterval),
		logger:             logger,
		aggregationEnabled: aggregationEnabled,
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

func (s *healthSignalAggregatorImpl) Start() {
	if !atomic.CompareAndSwapInt32(&s.status, common.DaemonStatusInitialized, common.DaemonStatusStarted) {
		return
	}
	s.signals.Start()
	go s.emitMetricsLoop()
}

func (s *healthSignalAggregatorImpl) Stop() {
	if !atomic.CompareAndSwapInt32(&s.status, common.DaemonStatusStarted, common.DaemonStatusStopped) {
		return
	}

	s.signals.Stop()
	close(s.shutdownCh)
	s.emitMetricsTimer.Stop()
}

func (s *healthSignalAggregatorImpl) Record(operation string, callerSegment int32, latency time.Duration, err error) {
	if s.aggregationEnabled {
		s.latencyAverage.Record(latency.Milliseconds())

		s.signals.Record(operation, latency, err)

		if isUnhealthyError(err) {
			s.errorRatio.Record(1)
		} else {
			s.errorRatio.Record(0)
		}
	}

	if callerSegment != CallerSegmentMissing {
		s.incrementShardRequestCount(callerSegment)
	}
}

func (s *healthSignalAggregatorImpl) AverageLatency() float64 {
	return s.latencyAverage.Average()
}

func (s *healthSignalAggregatorImpl) LatencyQuantile(quantile float64) (float64, bool) {
	if !s.aggregationEnabled {
		return 0, false
	}

	return s.signals.LatencyQuantile(quantile)
}

func (s *healthSignalAggregatorImpl) LatencyQuantileByGroup(groupName string, quantile float64) (float64, bool) {
	if !s.aggregationEnabled {
		return 0, false
	}

	return s.signals.LatencyQuantileByGroup(groupName, quantile)
}

// NOTE: this reads the original moving average rather than the signal aggregator's overall
// bucket, since the dynamic rate limiter compares it against thresholds operators have
// already tuned. it will move over once signals is proven out
func (s *healthSignalAggregatorImpl) ErrorRatio() (float64, bool) {
	if !s.aggregationEnabled {
		return 0, false
	}

	return s.errorRatio.Average(), true
}

func (s *healthSignalAggregatorImpl) ErrorRatioByGroup(groupName string) (float64, bool) {
	if !s.aggregationEnabled {
		return 0, false
	}

	return s.signals.ErrorRatioByGroup(groupName)
}

func (s *healthSignalAggregatorImpl) incrementShardRequestCount(shardID int32) {
	s.requestsLock.Lock()
	defer s.requestsLock.Unlock()
	s.requestCounts[shardID]++
}

// Traverse through all shards and get the per-namespace persistence RPS for all shards.
// If that is over the limit, print a log line. Per-shard-per-namespace RPC limit for namespaces
// is configured in dynamic config. This will allow us to see if some namespaces had hit
// this limit in any of the shards.
func (s *healthSignalAggregatorImpl) emitMetricsLoop() {
	for {
		select {
		case <-s.shutdownCh:
			return
		case <-s.emitMetricsTimer.C:
			s.requestsLock.Lock()
			requestCounts := s.requestCounts
			s.requestCounts = make(map[int32]int64, len(requestCounts))
			s.requestsLock.Unlock()

			for _, count := range requestCounts {
				shardRPS := int64(float64(count) / emitMetricsInterval.Seconds())
				s.metricsHandler.Histogram(metrics.PersistenceShardRPS.Name(), metrics.PersistenceShardRPS.Unit()).Record(shardRPS)
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
	case *AppendHistoryTimeoutError,
		*TimeoutError:
		return true
	}
	return false
}
