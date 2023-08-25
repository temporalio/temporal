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
	"encoding/json"
	"math"
	"sync/atomic"
	"time"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/quotas"
)

const (
	DefaultRefreshInterval   = 10 * time.Second
	DefaultRateBurstRatio    = 1.0
	DefaultMinRateMultiplier = 0.1
	DefaultMaxRateMultiplier = 1.0
)

type (
	HealthRequestRateLimiterImpl struct {
		enabled    *atomic.Bool
		params     DynamicRateLimitingParams  // dynamic config map
		curOptions dynamicRateLimitingOptions // current dynamic config values (updated on refresh)

		rateLimiter   *quotas.RateLimiterImpl
		healthSignals persistence.HealthSignalAggregator

		refreshTimer *time.Ticker

		rateFn           quotas.RateFn
		rateToBurstRatio float64

		minRateMultiplier float64
		maxRateMultiplier float64
		curRateMultiplier float64

		logger log.Logger
	}

	dynamicRateLimitingOptions struct {
		Enabled bool

		RefreshInterval string // string returned by json.Unmarshal will be parsed into a duration

		// thresholds which should trigger backoff if exceeded
		LatencyThreshold float64
		ErrorThreshold   float64

		// if either threshold is exceeded, the current rate multiplier will be reduced by this amount
		RateBackoffStepSize float64
		// when the system is healthy and current rate < max rate, the current rate multiplier will be
		// increased by this amount
		RateIncreaseStepSize float64
	}
)

var _ quotas.RequestRateLimiter = (*HealthRequestRateLimiterImpl)(nil)

func NewHealthRequestRateLimiterImpl(
	healthSignals persistence.HealthSignalAggregator,
	rateFn quotas.RateFn,
	params DynamicRateLimitingParams,
	logger log.Logger,
) *HealthRequestRateLimiterImpl {
	limiter := &HealthRequestRateLimiterImpl{
		enabled:           &atomic.Bool{},
		rateLimiter:       quotas.NewRateLimiter(rateFn(), int(DefaultRateBurstRatio*rateFn())),
		healthSignals:     healthSignals,
		rateFn:            rateFn,
		params:            params,
		refreshTimer:      time.NewTicker(DefaultRefreshInterval),
		rateToBurstRatio:  DefaultRateBurstRatio,
		minRateMultiplier: DefaultMinRateMultiplier,
		maxRateMultiplier: DefaultMaxRateMultiplier,
		curRateMultiplier: DefaultMaxRateMultiplier,
		logger:            logger,
	}
	limiter.refreshDynamicParams()
	return limiter
}

func (rl *HealthRequestRateLimiterImpl) Allow(now time.Time, request quotas.Request) bool {
	rl.maybeRefresh()
	if !rl.enabled.Load() {
		return true
	}
	return rl.rateLimiter.AllowN(now, request.Token)
}

func (rl *HealthRequestRateLimiterImpl) Reserve(now time.Time, request quotas.Request) quotas.Reservation {
	rl.maybeRefresh()
	if !rl.enabled.Load() {
		return quotas.NoopReservation
	}
	return rl.rateLimiter.ReserveN(now, request.Token)
}

func (rl *HealthRequestRateLimiterImpl) Wait(ctx context.Context, request quotas.Request) error {
	rl.maybeRefresh()
	if !rl.enabled.Load() {
		return nil
	}
	return rl.rateLimiter.WaitN(ctx, request.Token)
}

func (rl *HealthRequestRateLimiterImpl) maybeRefresh() {
	select {
	case <-rl.refreshTimer.C:
		rl.refreshDynamicParams()
		if rl.enabled.Load() {
			rl.refreshRate()
		}
		rl.updateRefreshTimer()

	default:
		// no-op
	}
}

func (rl *HealthRequestRateLimiterImpl) refreshRate() {
	if rl.latencyThresholdExceeded() || rl.errorThresholdExceeded() {
		// limit exceeded, do backoff
		rl.curRateMultiplier = math.Max(rl.minRateMultiplier, rl.curRateMultiplier-rl.curOptions.RateBackoffStepSize)
		rl.rateLimiter.SetRate(rl.curRateMultiplier * rl.rateFn())
		rl.rateLimiter.SetBurst(int(rl.rateToBurstRatio * rl.rateFn()))
		rl.logger.Info("Health threshold exceeded, reducing rate limit.", tag.NewFloat64("newMulti", rl.curRateMultiplier), tag.NewFloat64("newRate", rl.rateLimiter.Rate()), tag.NewFloat64("latencyAvg", rl.healthSignals.AverageLatency()), tag.NewFloat64("errorRatio", rl.healthSignals.ErrorRatio()))
	} else if rl.curRateMultiplier < rl.maxRateMultiplier {
		// already doing backoff and under thresholds, increase limit
		rl.curRateMultiplier = math.Min(rl.maxRateMultiplier, rl.curRateMultiplier+rl.curOptions.RateIncreaseStepSize)
		rl.rateLimiter.SetRate(rl.curRateMultiplier * rl.rateFn())
		rl.rateLimiter.SetBurst(int(rl.rateToBurstRatio * rl.rateFn()))
		rl.logger.Info("System healthy, increasing rate limit.", tag.NewFloat64("newMulti", rl.curRateMultiplier), tag.NewFloat64("newRate", rl.rateLimiter.Rate()), tag.NewFloat64("latencyAvg", rl.healthSignals.AverageLatency()), tag.NewFloat64("errorRatio", rl.healthSignals.ErrorRatio()))
	}
}

func (rl *HealthRequestRateLimiterImpl) refreshDynamicParams() {
	var options dynamicRateLimitingOptions
	b, err := json.Marshal(rl.params())
	if err != nil {
		rl.logger.Warn("Error marshalling dynamic rate limiting params. Dynamic rate limiting is disabled.", tag.Error(err))
		rl.enabled.Store(false)
		return
	}

	err = json.Unmarshal(b, &options)
	if err != nil {
		rl.logger.Warn("Error unmarshalling dynamic rate limiting params. Dynamic rate limiting is disabled.", tag.Error(err))
		rl.enabled.Store(false)
		return
	}

	rl.enabled.Store(options.Enabled)
	rl.curOptions = options
}

func (rl *HealthRequestRateLimiterImpl) updateRefreshTimer() {
	if len(rl.curOptions.RefreshInterval) > 0 {
		if refreshDuration, err := timestamp.ParseDurationDefaultSeconds(rl.curOptions.RefreshInterval); err != nil {
			rl.logger.Warn("Error parsing dynamic rate limit refreshInterval timestamp. Using previous value.", tag.Error(err))
		} else {
			rl.refreshTimer.Reset(refreshDuration)
		}
	}
}

func (rl *HealthRequestRateLimiterImpl) latencyThresholdExceeded() bool {
	return rl.curOptions.LatencyThreshold > 0 && rl.healthSignals.AverageLatency() > rl.curOptions.LatencyThreshold
}

func (rl *HealthRequestRateLimiterImpl) errorThresholdExceeded() bool {
	return rl.curOptions.ErrorThreshold > 0 && rl.healthSignals.ErrorRatio() > rl.curOptions.ErrorThreshold
}
