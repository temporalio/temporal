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

type (
	HealthSignalAggregator interface {
		common.Daemon
		Record(callerSegment int32, latency time.Duration, err error)
		AverageLatency() float64
		ErrorRatio() float64
	}

	HealthSignalAggregatorImpl struct {
		status     int32
		shutdownCh chan struct{}

		requestsPerShard map[int32]int64
		requestsLock     sync.Mutex

		latencyAverage aggregate.MovingWindowAverage
		errorRatio     aggregate.MovingWindowAverage

		metricsHandler       metrics.Handler
		emitMetricsTimer     *time.Ticker
		perShardRPSWarnLimit dynamicconfig.IntPropertyFn

		logger log.Logger
	}
)

func NewHealthSignalAggregatorImpl(
	windowSize time.Duration,
	maxBufferSize int,
	metricsHandler metrics.Handler,
	perShardRPSWarnLimit dynamicconfig.IntPropertyFn,
	logger log.Logger,
) *HealthSignalAggregatorImpl {
	return &HealthSignalAggregatorImpl{
		status:               common.DaemonStatusInitialized,
		shutdownCh:           make(chan struct{}),
		requestsPerShard:     make(map[int32]int64),
		latencyAverage:       aggregate.NewMovingWindowAvgImpl(windowSize, maxBufferSize),
		errorRatio:           aggregate.NewMovingWindowAvgImpl(windowSize, maxBufferSize),
		metricsHandler:       metricsHandler,
		emitMetricsTimer:     time.NewTicker(emitMetricsInterval),
		perShardRPSWarnLimit: perShardRPSWarnLimit,
		logger:               logger,
	}
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

func (s *HealthSignalAggregatorImpl) Record(callerSegment int32, latency time.Duration, err error) {
	s.latencyAverage.Record(latency.Milliseconds())

	if isUnhealthyError(err) {
		s.errorRatio.Record(1)
	} else {
		s.errorRatio.Record(0)
	}

	if callerSegment != CallerSegmentMissing {
		s.incrementShardRequestCount(callerSegment)
	}
}

func (s *HealthSignalAggregatorImpl) AverageLatency() float64 {
	return s.latencyAverage.Average()
}

func (s *HealthSignalAggregatorImpl) ErrorRatio() float64 {
	return s.errorRatio.Average()
}

func (s *HealthSignalAggregatorImpl) incrementShardRequestCount(shardID int32) {
	s.requestsLock.Lock()
	defer s.requestsLock.Unlock()
	s.requestsPerShard[shardID]++
}

func (s *HealthSignalAggregatorImpl) emitMetricsLoop() {
	for {
		select {
		case <-s.shutdownCh:
			return
		case <-s.emitMetricsTimer.C:
			s.requestsLock.Lock()
			requestCounts := s.requestsPerShard
			s.requestsPerShard = make(map[int32]int64, len(requestCounts))
			s.requestsLock.Unlock()

			for shardID, count := range requestCounts {
				shardRPS := int64(float64(count) / emitMetricsInterval.Seconds())
				s.metricsHandler.Histogram(metrics.PersistenceShardRPS.GetMetricName(), metrics.PersistenceShardRPS.GetMetricUnit()).Record(shardRPS)
				if shardRPS > int64(s.perShardRPSWarnLimit()) {
					s.logger.Warn("Per shard RPS warn limit exceeded", tag.ShardID(shardID))
				}
			}
		}
	}
}

func isUnhealthyError(err error) bool {
	if err == nil {
		return false
	}
	switch err.(type) {
	case *AppendHistoryTimeoutError,
		*TimeoutError,
		*serviceerror.DeadlineExceeded:
		return true

	default:
		return false
	}
}
