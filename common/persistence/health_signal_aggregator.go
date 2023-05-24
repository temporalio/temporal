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
	"go.temporal.io/server/common/aggregate"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/quotas"
)

type (
	HealthSignalAggregator interface {
		GetRecordFn(req quotas.Request) func(err error)
		AverageLatency() float64
		ErrorRatio() float64
	}

	HealthSignalAggregatorImpl struct {
		requestsPerShardAndNs map[perShardPerNamespaceKey]*atomic.Int64
		requestsLock          sync.RWMutex

		latencyAverage aggregate.MovingWindowAverage
		errorRatio     aggregate.MovingWindowAverage

		metricsHandler      metrics.Handler
		emitMetricsInterval time.Duration
		emitMetricsTimer    *time.Ticker
	}

	perShardPerNamespaceKey struct {
		namespace string
		shard     int32
	}
)

func NewHealthSignalAggregatorImpl(
	windowSize dynamicconfig.DurationPropertyFn,
	maxBufferSize dynamicconfig.IntPropertyFn,
	metricsHandler metrics.Handler,
) *HealthSignalAggregatorImpl {
	return &HealthSignalAggregatorImpl{
		requestsPerShardAndNs: make(map[perShardPerNamespaceKey]*atomic.Int64),
		latencyAverage:        aggregate.NewMovingWindowAvgImpl(windowSize(), maxBufferSize()),
		errorRatio:            aggregate.NewMovingWindowAvgImpl(windowSize(), maxBufferSize()),
		metricsHandler:        metricsHandler,
		emitMetricsInterval:   windowSize(),
		emitMetricsTimer:      time.NewTicker(windowSize()),
	}
}

func (s *HealthSignalAggregatorImpl) GetRecordFn(req quotas.Request) func(err error) {
	start := time.Now()
	return func(err error) {
		s.getOrInitRequestCount(req).Add(1)
		s.latencyAverage.Record(time.Since(start).Milliseconds())
		if isUnhealthyError(err) {
			s.errorRatio.Record(1)
		} else {
			s.errorRatio.Record(0)
		}
	}
}

func (s *HealthSignalAggregatorImpl) AverageLatency() float64 {
	return s.latencyAverage.Average()
}

func (s *HealthSignalAggregatorImpl) ErrorRatio() float64 {
	return s.errorRatio.Average()
}

func (s *HealthSignalAggregatorImpl) getOrInitRequestCount(req quotas.Request) *atomic.Int64 {
	key := getPerShardPerNsKey(req)

	s.requestsLock.RLock()
	count, ok := s.requestsPerShardAndNs[key]
	s.requestsLock.RUnlock()
	if ok {
		return count
	}

	newCount := &atomic.Int64{}

	s.requestsLock.Lock()
	defer s.requestsLock.Unlock()

	count, ok = s.requestsPerShardAndNs[key]
	if ok {
		return count
	}

	s.requestsPerShardAndNs[key] = newCount
	return newCount
}

func (s *HealthSignalAggregatorImpl) emitMetricsLoop() {
	for {
		select {
		case <-s.emitMetricsTimer.C:
			s.requestsLock.RLock()
			for key, count := range s.requestsPerShardAndNs {
				shardRPS := int64(float64(count.Swap(0)) / s.emitMetricsInterval.Seconds())
				s.metricsHandler.Histogram(metrics.PersistenceShardRPS.GetMetricName(), metrics.PersistenceShardRPS.GetMetricUnit()).Record(shardRPS, metrics.NamespaceTag(key.namespace))
			}
			s.requestsLock.RUnlock()
		}
	}
}

func getPerShardPerNsKey(req quotas.Request) perShardPerNamespaceKey {
	return perShardPerNamespaceKey{
		namespace: req.Caller,
		shard:     req.CallerSegment,
	}
}

func isUnhealthyError(err error) bool {
	if err == nil {
		return false
	}
	switch err.(type) {
	case *ShardOwnershipLostError,
		*AppendHistoryTimeoutError,
		*TimeoutError,
		*serviceerror.Unavailable:
		return true

	default:
		return false
	}
}
