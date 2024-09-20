// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package persistence

import (
	"sync"
	"sync/atomic"
	"time"

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

type (
	HealthSignalAggregator interface {
		Record(callerSegment int32, namespace string, latency time.Duration, err error)
		AverageLatency() float64
		ErrorRatio() float64
		Start()
		Stop()
	}

	HealthSignalAggregatorImpl struct {
		status     int32
		shutdownCh chan struct{}

		// map of shardID -> map of namespace -> request count
		requestCounts map[int32]map[string]int64
		requestsLock  sync.Mutex

		aggregationEnabled bool
		latencyAverage     aggregate.MovingWindowAverage
		errorRatio         aggregate.MovingWindowAverage

		metricsHandler            metrics.Handler
		emitMetricsTimer          *time.Ticker
		perShardRPSWarnLimit      dynamicconfig.IntPropertyFn
		perShardPerNsRPSWarnLimit dynamicconfig.FloatPropertyFn

		logger log.Logger
	}
)

func NewHealthSignalAggregatorImpl(
	aggregationEnabled bool,
	windowSize time.Duration,
	maxBufferSize int,
	metricsHandler metrics.Handler,
	perShardRPSWarnLimit dynamicconfig.IntPropertyFn,
	perShardPerNsRPSWarnLimit dynamicconfig.FloatPropertyFn,
	logger log.Logger,
) *HealthSignalAggregatorImpl {
	ret := &HealthSignalAggregatorImpl{
		status:                    common.DaemonStatusInitialized,
		shutdownCh:                make(chan struct{}),
		requestCounts:             make(map[int32]map[string]int64),
		metricsHandler:            metricsHandler,
		emitMetricsTimer:          time.NewTicker(emitMetricsInterval),
		perShardRPSWarnLimit:      perShardRPSWarnLimit,
		perShardPerNsRPSWarnLimit: perShardPerNsRPSWarnLimit,
		logger:                    logger,
		aggregationEnabled:        aggregationEnabled,
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

func (s *HealthSignalAggregatorImpl) Record(callerSegment int32, namespace string, latency time.Duration, err error) {
	if s.aggregationEnabled {
		s.latencyAverage.Record(latency.Milliseconds())

		if isUnhealthyError(err) {
			s.errorRatio.Record(1)
		} else {
			s.errorRatio.Record(0)
		}
	}

	if callerSegment != CallerSegmentMissing {
		s.incrementShardRequestCount(callerSegment, namespace)
	}
}

func (s *HealthSignalAggregatorImpl) AverageLatency() float64 {
	return s.latencyAverage.Average()
}

func (s *HealthSignalAggregatorImpl) ErrorRatio() float64 {
	return s.errorRatio.Average()
}

func (s *HealthSignalAggregatorImpl) incrementShardRequestCount(shardID int32, namespace string) {
	s.requestsLock.Lock()
	defer s.requestsLock.Unlock()
	if s.requestCounts[shardID] == nil {
		s.requestCounts[shardID] = make(map[string]int64)
	}
	s.requestCounts[shardID][namespace]++
}

func (s *HealthSignalAggregatorImpl) emitMetricsLoop() {
	for {
		select {
		case <-s.shutdownCh:
			return
		case <-s.emitMetricsTimer.C:
			s.requestsLock.Lock()
			requestCounts := s.requestCounts
			s.requestCounts = make(map[int32]map[string]int64, len(requestCounts))
			s.requestsLock.Unlock()

			for shardID, requestCountPerNS := range requestCounts {
				shardRequestCount := int64(0)
				for namespace, count := range requestCountPerNS {
					shardRequestCount += count
					shardRPSPerNS := int64(float64(count) / emitMetricsInterval.Seconds())
					if s.perShardPerNsRPSWarnLimit() > 0.0 && shardRPSPerNS > int64(s.perShardPerNsRPSWarnLimit()*float64(s.perShardRPSWarnLimit())) {
						s.logger.Warn("Per shard per namespace RPS warn limit exceeded", tag.ShardID(shardID), tag.WorkflowNamespace(namespace), tag.RPS(shardRPSPerNS))
					}
				}

				shardRPS := int64(float64(shardRequestCount) / emitMetricsInterval.Seconds())
				s.metricsHandler.Histogram(metrics.PersistenceShardRPS.Name(), metrics.PersistenceShardRPS.Unit()).Record(shardRPS)
				if shardRPS > int64(s.perShardRPSWarnLimit()) {
					s.logger.Warn("Per shard RPS warn limit exceeded", tag.ShardID(shardID), tag.RPS(shardRPS))
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

	switch err.(type) {
	case *AppendHistoryTimeoutError,
		*TimeoutError:
		return true

	default:
		return false
	}
}
