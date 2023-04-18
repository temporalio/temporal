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
	"go.temporal.io/server/common/backpressure"
	"math"
	"time"
)

type (
	HealthRequestRateLimiterImpl struct {
		rateLimiter   *RateLimiterImpl
		healthSignals backpressure.HealthSignalAggregator

		latencyThreshold    float64
		errorRatioThreshold float64

		refreshTimer    *time.Timer
		refreshInterval time.Duration

		// reductionMultiplier will be used to get the new rate limit by multiplying it with the current rate
		reductionMultiplier float64
		// increaseStepSize is a ratio of the maxRate that will be added to the current rate when increasing
		increaseStepSize float64

		maxRate  float64
		curRate  float64
		maxBurst int
		curBurst int
	}
)

var _ RequestRateLimiter = (*HealthRequestRateLimiterImpl)(nil)

func NewHealthRateLimiterImpl(
	healthSignals backpressure.HealthSignalAggregator,
	refreshInterval time.Duration,
	reductionMultiplier float64,
	increaseRatio float64,
	maxRate float64,
	maxBurst int,
) *HealthRequestRateLimiterImpl {
	return &HealthRequestRateLimiterImpl{
		rateLimiter:         NewRateLimiter(maxRate, maxBurst),
		healthSignals:       healthSignals,
		refreshTimer:        time.NewTimer(refreshInterval),
		refreshInterval:     refreshInterval,
		reductionMultiplier: reductionMultiplier,
		increaseStepSize:    increaseRatio * maxRate,
		maxRate:             maxRate,
		curRate:             maxRate,
		maxBurst:            maxBurst,
		curBurst:            maxBurst,
	}
}

func (rl *HealthRequestRateLimiterImpl) Allow(now time.Time, request Request) bool {
	rl.maybeRefresh()
	return rl.rateLimiter.AllowN(now, request.Token)
}

func (rl *HealthRequestRateLimiterImpl) Reserve(now time.Time, request Request) Reservation {
	rl.maybeRefresh()
	return rl.rateLimiter.ReserveN(now, request.Token)
}

func (rl *HealthRequestRateLimiterImpl) Wait(ctx context.Context, request Request) error {
	rl.maybeRefresh()
	return rl.rateLimiter.WaitN(ctx, request.Token)
}

func (rl *HealthRequestRateLimiterImpl) maybeRefresh() {
	select {
	case <-rl.refreshTimer.C:
		rl.refreshTimer.Reset(rl.refreshInterval)
		rl.refresh()

	default:
		// no-op
	}
}

func (rl *HealthRequestRateLimiterImpl) refresh() {
	if rl.healthSignals.GetLatencyAverage() > rl.latencyThreshold || rl.healthSignals.GetErrorRatio() > rl.errorRatioThreshold {
		rl.curRate = rl.reductionMultiplier * rl.curRate
		rl.rateLimiter.SetRate(rl.curRate)
	} else if rl.curRate < rl.maxRate {
		rl.curRate = math.Min(rl.maxRate, rl.curRate+rl.increaseStepSize)
		rl.rateLimiter.SetRate(rl.curRate)
	}
}
