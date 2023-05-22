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
	"math"
	"time"

	"go.temporal.io/server/common/aggregate"
)

type (
	HealthRequestRateLimiterImpl struct {
		rateLimiter   *RateLimiterImpl
		healthSignals aggregate.SignalAggregator[Request]

		refreshTimer *time.Ticker

		// thresholds which should trigger backoff if exceeded
		latencyThreshold float64
		errorThreshold   float64

		// if either threshold is exceeded, the current rate and burst will be lowered by
		// multiplying the current value with this
		reductionMultiplier float64

		maxRate float64
		curRate float64
		// when the system is healthy, and the curRate < maxRate, the curRate will be
		// additively increased by this amount each refresh. Determined as a ratio of the
		// maxRate on creation
		rateIncreaseStepSize float64

		maxBurst float64
		curBurst float64
		// when the system is healthy, and the curBurst < maxBurst, the curBurst will be
		// additively increased by this amount each refresh. Determined as a ratio of the
		// maxBurst on creation
		burstIncreaseStepSize float64
	}
)

func NewHealthRequestRateLimiterImpl(
	healthSignals aggregate.SignalAggregator[Request],
	refreshInterval time.Duration,
	latencyThreshold float64,
	errorThreshold float64,
	reductionMultiplier float64,
	maxRate float64,
	rateIncreaseRatio float64,
	maxBurst float64,
	burstIncreaseRatio float64,
) *HealthRequestRateLimiterImpl {
	return &HealthRequestRateLimiterImpl{
		healthSignals:         healthSignals,
		refreshTimer:          time.NewTicker(refreshInterval),
		latencyThreshold:      latencyThreshold,
		errorThreshold:        errorThreshold,
		reductionMultiplier:   reductionMultiplier,
		maxRate:               maxRate,
		curRate:               maxRate,
		rateIncreaseStepSize:  maxRate * rateIncreaseRatio,
		maxBurst:              maxBurst,
		curBurst:              maxBurst,
		burstIncreaseStepSize: maxBurst * burstIncreaseRatio,
	}
}

func (rl *HealthRequestRateLimiterImpl) Allow(now time.Time, request Request) bool {
	rl.maybeRefresh(request)
	return rl.rateLimiter.AllowN(now, request.Token)
}

func (rl *HealthRequestRateLimiterImpl) Reserve(now time.Time, request Request) Reservation {
	rl.maybeRefresh(request)
	return rl.rateLimiter.ReserveN(now, request.Token)
}

func (rl *HealthRequestRateLimiterImpl) Wait(ctx context.Context, request Request) error {
	rl.maybeRefresh(request)
	return rl.rateLimiter.WaitN(ctx, request.Token)
}

func (rl *HealthRequestRateLimiterImpl) maybeRefresh(request Request) {
	select {
	case <-rl.refreshTimer.C:
		rl.refresh(request)

	default:
		// no-op
	}
}

func (rl *HealthRequestRateLimiterImpl) refresh(request Request) {
	if rl.healthSignals.AverageLatency(request) > rl.latencyThreshold ||
		rl.healthSignals.ErrorRatio(request) > rl.errorThreshold {
		// limits exceeded, do backoff
		rl.curRate = rl.curRate * rl.reductionMultiplier
		rl.curBurst = rl.curBurst * rl.reductionMultiplier
		rl.rateLimiter.SetRateBurst(rl.curRate, int(rl.curBurst))
	} else if rl.curRate < rl.maxRate {
		// already doing backoff and under thresholds, increase limit
		rl.curRate = math.Min(rl.maxRate, rl.curRate+rl.rateIncreaseStepSize)
		rl.curBurst = math.Min(rl.maxBurst, rl.maxBurst+rl.burstIncreaseStepSize)
		rl.rateLimiter.SetRateBurst(rl.curRate, int(rl.curBurst))
	}
}
