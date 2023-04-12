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

package quotas

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"
)

var _ HealthRateLimiter = (*HealthRateLimiterImpl)(nil)

type (
	HealthRateLimiterImpl struct {
		globalRateLimiter RequestRateLimiter
		nsRateLimiter     RequestRateLimiter

		// windowSize indicates the sliding time window used to calculate current error and latency rates
		windowSize int
		// errorRatio indicates the ratio of requests that return an error before enforcing backoff
		errorRatio int
		// TODO: maybe should use a ratio of latencies above some max
		avgLatencyThreshold int

		latencies []time.Duration
		errors    atomic.Int32
	}
)

func NewHealthRateLimiter(
	globalRateLimiter RequestRateLimiter,
	nsRateLimiter RequestRateLimiter,
) *HealthRateLimiterImpl {
	return &HealthRateLimiterImpl{
		globalRateLimiter: globalRateLimiter,
		nsRateLimiter:     nsRateLimiter,
	}
}

func (rl *HealthRateLimiterImpl) Allow(now time.Time, request Request) bool {
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

func (rl *HealthRateLimiterImpl) Reserve(now time.Time, request Request) Reservation {
	globalReservation := rl.globalRateLimiter.Reserve(now, request)
	if !globalReservation.OK() {
		return NewMultiReservation(false, nil)
	}

	nsReservation := rl.nsRateLimiter.Reserve(now, request)
	if !nsReservation.OK() {
		globalReservation.CancelAt(now)
		return NewMultiReservation(false, nil)
	}

	return NewMultiReservation(true, []Reservation{globalReservation, nsReservation})
}

func (rl *HealthRateLimiterImpl) Wait(ctx context.Context, request Request) error {
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
	waitLimit := InfDuration
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
		rl.latencies = append(rl.latencies, time.Since(start))
	}
}

func (rl *HealthRateLimiterImpl) RecordLatencyByShard(api string, shardID int32) func() {
	start := time.Now()
	return func() {
		rl.latencies = append(rl.latencies, time.Since(start))
	}
}

// TODO: some functions have only Name or ID available, how to reconcile metrics?
func (rl *HealthRateLimiterImpl) RecordLatencyByNamespace(api string, nsID string) func() {
	start := time.Now()
	return func() {
		rl.latencies = append(rl.latencies, time.Since(start))
	}
}

func (rl *HealthRateLimiterImpl) RecordError(api string) {
	rl.errors.Add(1)
}

func (rl *HealthRateLimiterImpl) RecordErrorByShard(api string, shardID int32) {
	rl.errors.Add(1)
}

// TODO: some functions have only Name or ID available, how to reconcile metrics?
func (rl *HealthRateLimiterImpl) RecordErrorByNamespace(api string, nsID string) {
	rl.errors.Add(1)
}

// TODO: need to add functions to adjust rate limits based on some coefficient

func (rl *HealthRateLimiterImpl) ReduceGlobalLimit() {

}
