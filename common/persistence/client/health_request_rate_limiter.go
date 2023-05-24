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
	"context"
	"math"
	"time"

	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/quotas"
)

const (
	DefaultRateBurstRatio    = 1.0
	DefaultMinRateMultiplier = 0.1
	DefaultMaxRateMultiplier = 1.0
)

type (
	HealthRequestRateLimiterImpl struct {
		rateLimiter   *quotas.RateLimiterImpl
		healthSignals persistence.HealthSignalAggregator

		refreshTimer *time.Ticker

		// thresholds which should trigger backoff if exceeded
		latencyThreshold float64
		errorThreshold   float64

		rateFn           quotas.RateFn
		rateToBurstRatio float64

		minRateMultiplier float64
		maxRateMultiplier float64
		curRateMultiplier float64

		// if either threshold is exceeded, the current rate multiplier will be reduced by this amount
		rateBackoffStepSize float64
		// when the system is healthy and current rate < max rate, the current rate multiplier will be
		// increased by this amount
		rateIncreaseStepSize float64
	}
)

var _ quotas.RequestRateLimiter = (*HealthRequestRateLimiterImpl)(nil)

func NewHealthRequestRateLimiterImpl(
	healthSignals persistence.HealthSignalAggregator,
	refreshInterval time.Duration,
	rateFn quotas.RateFn,
	latencyThreshold float64,
	errorThreshold float64,
	rateBackoffStepSize float64,
	rateIncreaseStepSize float64,
) *HealthRequestRateLimiterImpl {
	return &HealthRequestRateLimiterImpl{
		rateLimiter:          quotas.NewRateLimiter(rateFn(), int(DefaultRateBurstRatio*rateFn())),
		healthSignals:        healthSignals,
		refreshTimer:         time.NewTicker(refreshInterval),
		rateFn:               rateFn,
		rateToBurstRatio:     DefaultRateBurstRatio,
		latencyThreshold:     latencyThreshold,
		errorThreshold:       errorThreshold,
		minRateMultiplier:    DefaultMinRateMultiplier,
		maxRateMultiplier:    DefaultMaxRateMultiplier,
		curRateMultiplier:    DefaultMaxRateMultiplier,
		rateBackoffStepSize:  rateBackoffStepSize,
		rateIncreaseStepSize: rateIncreaseStepSize,
	}
}

func (rl *HealthRequestRateLimiterImpl) Allow(now time.Time, request quotas.Request) bool {
	rl.maybeRefresh()
	return rl.rateLimiter.AllowN(now, request.Token)
}

func (rl *HealthRequestRateLimiterImpl) Reserve(now time.Time, request quotas.Request) quotas.Reservation {
	rl.maybeRefresh()
	return rl.rateLimiter.ReserveN(now, request.Token)
}

func (rl *HealthRequestRateLimiterImpl) Wait(ctx context.Context, request quotas.Request) error {
	rl.maybeRefresh()
	return rl.rateLimiter.WaitN(ctx, request.Token)
}

func (rl *HealthRequestRateLimiterImpl) maybeRefresh() {
	select {
	case <-rl.refreshTimer.C:
		rl.refresh()

	default:
		// no-op
	}
}

func (rl *HealthRequestRateLimiterImpl) refresh() {
	if rl.healthSignals.AverageLatency() > rl.latencyThreshold || rl.healthSignals.ErrorRatio() > rl.errorThreshold {
		// limits exceeded, do backoff
		rl.curRateMultiplier = math.Max(rl.minRateMultiplier, rl.curRateMultiplier-rl.rateBackoffStepSize)
		rl.rateLimiter.SetRate(rl.curRateMultiplier * rl.rateFn())
		rl.rateLimiter.SetBurst(int(rl.rateToBurstRatio * rl.rateFn()))
	} else if rl.curRateMultiplier < rl.maxRateMultiplier {
		// already doing backoff and under thresholds, increase limit
		rl.curRateMultiplier = math.Min(rl.maxRateMultiplier, rl.curRateMultiplier+rl.rateIncreaseStepSize)
		rl.rateLimiter.SetRate(rl.curRateMultiplier * rl.rateFn())
		rl.rateLimiter.SetBurst(int(rl.rateToBurstRatio * rl.rateFn()))
	}
}
