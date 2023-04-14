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

package health

import (
	"context"
	"fmt"
	"github.com/dgryski/go-farm"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/quotas"
	"time"
)

// TODO: placeholders. should actually come from fx/dynamicconfig
const (
	rate               = 100
	burst              = 100
	defaultConcurrency = 32
	defaultMapSize     = 1024
)

var _ HealthRateLimiter = (*HealthRateLimiterImpl)(nil)

type (
	HealthRateLimiterImpl struct {
		hostRateLimiter   *quotas.RateLimiterImpl
		nsRateLimiters    collection.ConcurrentTxMap
		shardRateLimiters collection.ConcurrentTxMap

		// errorLimit indicates the ratio of requests that return an error before enforcing backoff
		errorLimit        float64
		errorWindowSize   time.Duration
		errorBufferSize   int
		errorRatioTotal   MovingWindowAvg
		errorRatioByNS    collection.ConcurrentTxMap
		errorRatioByShard collection.ConcurrentTxMap

		// latencyLimit indicates the maximum average latency before enforcing backoff
		latencyLimit      float64
		latencyWindowSize time.Duration
		latencyBufferSize int
		latencyAvgTotal   MovingWindowAvg
		latencyAvgByNS    collection.ConcurrentTxMap
		latencyAvgByShard collection.ConcurrentTxMap
	}
)

func NewHealthRateLimiter(
	errorLimit float64,
	errorWindowSize time.Duration,
	errorBufferSize int,
	latencyLimit float64,
	latencyWindowSize time.Duration,
	latencyBufferSize int,
) *HealthRateLimiterImpl {
	return &HealthRateLimiterImpl{
		// TODO: placeholders
		hostRateLimiter:   quotas.NewRateLimiter(rate, burst),
		nsRateLimiters:    collection.NewShardedConcurrentTxMap(defaultMapSize, hashFn),
		shardRateLimiters: collection.NewShardedConcurrentTxMap(defaultMapSize, hashFn),

		errorLimit:        errorLimit,
		errorRatioTotal:   NewMovingWindowAvgImpl(errorWindowSize, errorBufferSize),
		errorRatioByNS:    collection.NewShardedConcurrentTxMap(defaultMapSize, hashFn),
		errorRatioByShard: collection.NewShardedConcurrentTxMap(defaultMapSize, hashFn),

		latencyLimit:      latencyLimit,
		latencyAvgTotal:   NewMovingWindowAvgImpl(latencyWindowSize, latencyBufferSize),
		latencyAvgByNS:    collection.NewShardedConcurrentTxMap(defaultMapSize, hashFn),
		latencyAvgByShard: collection.NewShardedConcurrentTxMap(defaultMapSize, hashFn),
	}
}

func (rl *HealthRateLimiterImpl) getOrInitNamespaceRateLimiter(
	namespace string,
) *quotas.RateLimiterImpl {

}

func (rl *HealthRateLimiterImpl) getOrInitShardRateLimiter(
	shard int32,
) *quotas.RateLimiterImpl {

}

func (rl *HealthRateLimiterImpl) getOrInitNamespaceErrorWindowAvg(
	namespace string,
) MovingWindowAvg {

}

func (rl *HealthRateLimiterImpl) getOrInitShardErrorWindowAvg(
	shard int32,
) MovingWindowAvg {

}

func (rl *HealthRateLimiterImpl) getOrInitNamespaceLatencyWindowAvg(
	namespace string,
) MovingWindowAvg {

}

func (rl *HealthRateLimiterImpl) getOrInitShardLatencyWindowAvg(
	shard int32,
) MovingWindowAvg {

}

func (rl *HealthRateLimiterImpl) Allow(now time.Time, request quotas.Request) bool {
	globalReservation := rl.globalRateLimiter.Reserve(now, request)
	if !globalReservation.OK() || globalReservation.DelayFrom(now) > 0 {
		// global limit reached, system unhealthy
		if globalReservation.OK() {
			globalReservation.CancelAt(now)
		}
		return false
	}

	nsReservation := rl.nsRateLimiter.Reserve(now, request)
	if !nsReservation.OK() || nsReservation.DelayFrom(now) > 0 {
		// per ns rate limit exceeded, throttle to prevent noisy neighbor
		globalReservation.CancelAt(now)
		if nsReservation.OK() {
			nsReservation.CancelAt(now)
		}
		return false
	}

	return true
}

func (rl *HealthRateLimiterImpl) Allow(now time.Time, request quotas.Request) bool {
	globalReservation := rl.globalRateLimiter.Reserve(now, request)
	if !globalReservation.OK() || globalReservation.DelayFrom(now) > 0 {
		// global limit reached, system unhealthy
		if globalReservation.OK() {
			globalReservation.CancelAt(now)
		}
		return false
	}
}

func (rl *HealthRateLimiterImpl) Reserve(now time.Time, request quotas.Request) quotas.Reservation {
	globalReservation := rl.globalRateLimiter.Reserve(now, request)
	if !globalReservation.OK() {
		return quotas.NewMultiReservation(false, nil)
	}

	nsReservation := rl.nsRateLimiter.Reserve(now, request)
	if !nsReservation.OK() {
		globalReservation.CancelAt(now)
		return quotas.NewMultiReservation(false, nil)
	}

	return quotas.NewMultiReservation(true, []quotas.Reservation{globalReservation, nsReservation})
}

func (rl *HealthRateLimiterImpl) Wait(ctx context.Context, request quotas.Request) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	now := time.Now().UTC()
	reservation := rl.Reserve(now, request)
	if !reservation.OK() {
		return fmt.Errorf("rate: Wait(n=%d) would exceed context deadline", request.Token)
	}

	delay := reservation.DelayFrom(now)
	if delay == 0 {
		return nil
	}
	waitLimit := quotas.InfDuration
	if deadline, ok := ctx.Deadline(); ok {
		waitLimit = deadline.Sub(now)
	}
	if waitLimit < delay {
		reservation.CancelAt(now)
		return fmt.Errorf("rate: Wait(n=%d) would exceed context deadline", request.Token)
	}

	t := time.NewTimer(delay)
	defer t.Stop()
	select {
	case <-t.C:
		return nil

	case <-ctx.Done():
		reservation.CancelAt(time.Now())
		return ctx.Err()
	}
}

// RecordLatency should be called before the code to be measured, and the returned function executed after
func (rl *HealthRateLimiterImpl) RecordLatency(api string) func() {
	start := time.Now()
	return func() {
		rl.errorRatioTotal.Add(0)
		rl.updateLatencyAvgTotal(time.Since(start).Milliseconds())
	}
}

func (rl *HealthRateLimiterImpl) updateLatencyAvgTotal(ms int64) {
	rl.latencyAvgTotal.Add(ms)
	if rl.latencyAvgTotal.Average() > rl.latencyLimit {
		rl.backoffGlobalLimit()
	}
}

func (rl *HealthRateLimiterImpl) RecordLatencyByShard(api string, shardID int32) func() {
	start := time.Now()
	return func() {
		totalTime := time.Since(start).Milliseconds()
		rl.errorRatioTotal.Add(0)
		rl.updateLatencyAvgTotal(totalTime)
		shardLatencies := rl.getOrInitShardLatencyWindowAvg(shardID)
		shardLatencies.Add(totalTime)
		if shardLatencies.Average() > rl.latencyLimit {
			rl.backoffShardLimit(shardID)
		}
	}
}

// TODO: some functions have only Name or ID available, how to reconcile metrics?
func (rl *HealthRateLimiterImpl) RecordLatencyByNamespace(api string, nsID string) func() {
	start := time.Now()
	return func() {
		totalTime := time.Since(start).Milliseconds()
		rl.errorRatioTotal.Add(0)
		rl.updateLatencyAvgTotal(totalTime)
		namespaceLatencies := rl.getOrInitNamespaceLatencyWindowAvg(nsID)
		namespaceLatencies.Add(totalTime)
		if namespaceLatencies.Average() > rl.latencyLimit {
			rl.backoffNamespaceLimit(nsID)
		}
	}
}

func (rl *HealthRateLimiterImpl) RecordError(api string) {
	rl.errorRatioTotal.Add(1)
	if rl.errorRatioTotal.Average() > rl.errorLimit {
		rl.backoffGlobalLimit()
	}
}

func (rl *HealthRateLimiterImpl) RecordErrorByShard(api string, shardID int32) {
	rl.RecordError(api)
	shardErrors := rl.getOrInitShardErrorWindowAvg(shardID)
	shardErrors.Add(1)
	if shardErrors.Average() > rl.errorLimit {
		rl.backoffShardLimit(shardID)
	}
}

// TODO: some functions have only Name or ID available, how to reconcile metrics?
func (rl *HealthRateLimiterImpl) RecordErrorByNamespace(api string, nsID string) {
	rl.RecordError(api)
	namespaceErrors := rl.getOrInitNamespaceErrorWindowAvg(nsID)
	namespaceErrors.Add(1)
	if namespaceErrors.Average() > rl.errorLimit {
		rl.backoffNamespaceLimit(nsID)
	}
}

// TODO: implement backoff algorithm
func (rl *HealthRateLimiterImpl) backoffGlobalLimit() {

}

func (rl *HealthRateLimiterImpl) backoffNamespaceLimit(namespace string) {

}

func (rl *HealthRateLimiterImpl) backoffShardLimit(shard int32) {

}

func hashFn(key interface{}) uint32 {
	id, ok := key.(string)
	if !ok {
		return 0
	}
	idBytes := []byte(id)
	hash := farm.Hash32(idBytes)
	return hash % defaultConcurrency
}
