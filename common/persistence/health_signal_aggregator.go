package persistence

import (
	"sync"
	"sync/atomic"
	"time"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/aggregate"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
)

const (
	emitMetricsInterval = 30 * time.Second
)

type (
	HealthSignalAggregator interface {
		Record(callerSegment int32, latency time.Duration, err error)
		AverageLatency() float64
		ErrorRatio() float64
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
		latencyAverage     aggregate.MovingWindowAverage
		errorRatio         aggregate.MovingWindowAverage

		metricsHandler   metrics.Handler
		emitMetricsTimer *time.Ticker

		logger log.Logger
	}
)

func NewHealthSignalAggregator(
	aggregationEnabled bool,
	windowSize time.Duration,
	maxBufferSize int,
	metricsHandler metrics.Handler,
	logger log.Logger,
) *healthSignalAggregatorImpl {
	ret := &healthSignalAggregatorImpl{
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
	go s.emitMetricsLoop()
}

func (s *healthSignalAggregatorImpl) Stop() {
	if !atomic.CompareAndSwapInt32(&s.status, common.DaemonStatusStarted, common.DaemonStatusStopped) {
		return
	}
	close(s.shutdownCh)
	s.emitMetricsTimer.Stop()
}

func (s *healthSignalAggregatorImpl) Record(callerSegment int32, latency time.Duration, err error) {
	if s.aggregationEnabled {
		s.latencyAverage.Record(latency.Milliseconds())

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

func (s *healthSignalAggregatorImpl) ErrorRatio() float64 {
	return s.errorRatio.Average()
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
