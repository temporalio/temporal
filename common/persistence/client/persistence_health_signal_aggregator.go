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

package client

import (
	"sync"
	"sync/atomic"
	"time"

	"go.temporal.io/server/common/aggregate"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/quotas"
)

type (
	PersistenceHealthSignalAggregator[K aggregate.SignalKey] struct {
		keyMapper aggregate.SignalKeyMapperFn[quotas.Request, K]

		totalRequests     map[K]*atomic.Int64
		totalRequestsLock sync.RWMutex

		latencyAverages map[K]aggregate.MovingWindowAverage
		latencyLock     sync.RWMutex

		errorRatios map[K]aggregate.MovingWindowAverage
		errorLock   sync.RWMutex

		windowSize    time.Duration
		maxBufferSize int

		metricsHandler   metrics.Handler
		emitMetricsTimer *time.Ticker
	}

	perShardPerNsHealthSignalKey struct {
		namespace string
		shardID   int32
	}
)

func NewPersistenceHealthSignalAggregator[K aggregate.SignalKey](
	keyMapper aggregate.SignalKeyMapperFn[quotas.Request, K],
	windowSize time.Duration,
	maxBufferSize int,
	metricsHandler metrics.Handler,
) *PersistenceHealthSignalAggregator[K] {
	return &PersistenceHealthSignalAggregator[K]{
		keyMapper:        keyMapper,
		totalRequests:    make(map[K]*atomic.Int64),
		latencyAverages:  make(map[K]aggregate.MovingWindowAverage),
		errorRatios:      make(map[K]aggregate.MovingWindowAverage),
		windowSize:       windowSize,
		maxBufferSize:    maxBufferSize,
		metricsHandler:   metricsHandler,
		emitMetricsTimer: time.NewTicker(windowSize),
	}
}

func NewPerShardPerNsHealthSignalAggregator(
	windowSize dynamicconfig.DurationPropertyFn,
	maxBufferSize dynamicconfig.IntPropertyFn,
	metricsHandler metrics.Handler,
) *PersistenceHealthSignalAggregator[perShardPerNsHealthSignalKey] {
	return NewPersistenceHealthSignalAggregator[perShardPerNsHealthSignalKey](
		perShardPerNsKeyMapperFn,
		windowSize(),
		maxBufferSize(),
		metricsHandler,
	)
}

func perShardPerNsKeyMapperFn(req quotas.Request) perShardPerNsHealthSignalKey {
	return perShardPerNsHealthSignalKey{
		namespace: req.Caller,
		shardID:   req.CallerSegment,
	}
}

func (k perShardPerNsHealthSignalKey) GetNamespace() string {
	return k.namespace
}

func (s *PersistenceHealthSignalAggregator[_]) GetRecordFn(req quotas.Request) func(err error) {
	start := time.Now()
	return func(err error) {
		s.getOrInitRequestCount(req).Add(1)
		s.getOrInitLatencyAverage(req).Record(time.Since(start).Milliseconds())
		errorRatio := s.getOrInitErrorRatio(req)
		if err != nil {
			errorRatio.Record(1)
		} else {
			errorRatio.Record(0)
		}
	}
}

func (s *PersistenceHealthSignalAggregator[_]) AverageLatency(req quotas.Request) float64 {
	return s.getOrInitLatencyAverage(req).Average()
}

func (s *PersistenceHealthSignalAggregator[_]) ErrorRatio(req quotas.Request) float64 {
	return s.getOrInitErrorRatio(req).Average()
}

func (s *PersistenceHealthSignalAggregator[_]) getOrInitLatencyAverage(req quotas.Request) aggregate.MovingWindowAverage {
	return s.getOrInitAverage(req, &s.latencyAverages, &s.latencyLock)
}

func (s *PersistenceHealthSignalAggregator[_]) getOrInitErrorRatio(req quotas.Request) aggregate.MovingWindowAverage {
	return s.getOrInitAverage(req, &s.errorRatios, &s.errorLock)
}

func (s *PersistenceHealthSignalAggregator[K]) getOrInitAverage(
	req quotas.Request,
	averages *map[K]aggregate.MovingWindowAverage,
	lock *sync.RWMutex,
) aggregate.MovingWindowAverage {
	key := s.keyMapper(req)

	lock.RLock()
	avg, ok := (*averages)[key]
	lock.RUnlock()
	if ok {
		return avg
	}

	newAvg := aggregate.NewMovingWindowAvgImpl(s.windowSize, s.maxBufferSize)

	lock.Lock()
	defer lock.Unlock()

	avg, ok = (*averages)[key]
	if ok {
		return avg
	}

	(*averages)[key] = newAvg
	return newAvg
}

func (s *PersistenceHealthSignalAggregator[_]) getOrInitRequestCount(
	req quotas.Request,
) *atomic.Int64 {
	key := s.keyMapper(req)

	s.totalRequestsLock.RLock()
	count, ok := s.totalRequests[key]
	s.totalRequestsLock.RUnlock()
	if ok {
		return count
	}

	newCount := &atomic.Int64{}

	s.totalRequestsLock.Lock()
	defer s.totalRequestsLock.Unlock()

	count, ok = s.totalRequests[key]
	if ok {
		return count
	}

	s.totalRequests[key] = newCount
	return newCount
}

func (s *PersistenceHealthSignalAggregator[_]) emitMetricsLoop() {
	for {
		select {
		case <-s.emitMetricsTimer.C:
			s.totalRequestsLock.RLock()
			for key, count := range s.totalRequests {
				shardRPS := int64(float64(count.Swap(0)) / s.windowSize.Seconds())
				s.metricsHandler.Histogram(metrics.PersistenceShardRPS.GetMetricName(), metrics.PersistenceShardRPS.GetMetricUnit()).Record(shardRPS, metrics.NamespaceTag(key.GetNamespace()))
			}
			s.totalRequestsLock.RUnlock()
		}
	}
}
