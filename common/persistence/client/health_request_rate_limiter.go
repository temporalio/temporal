package client

import (
	"context"
	"math"
	"sync/atomic"
	"time"

	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/quotas"
)

const (
	DefaultRefreshInterval       = 10 * time.Second
	DefaultRateBurstRatio        = 1.0
	DefaultInitialRateMultiplier = 1.0
)

type (
	HealthRequestRateLimiterImpl struct {
		enabled    atomic.Bool
		params     DynamicRateLimitingParams                               // dynamic config struct
		curOptions atomic.Pointer[dynamicconfig.DynamicRateLimitingParams] // current dynamic config values (updated on refresh)

		rateLimiter   *quotas.RateLimiterImpl
		healthSignals persistence.HealthSignalAggregator

		refreshTimer *time.Ticker

		rateBurst quotas.RateBurst

		curRateMultiplier atomic.Pointer[float64]

		metricsHandler metrics.Handler
		logger         log.Logger
	}
)

var _ quotas.RequestRateLimiter = (*HealthRequestRateLimiterImpl)(nil)

func NewHealthRequestRateLimiterImpl(
	healthSignals persistence.HealthSignalAggregator,
	rateFn quotas.RateFn,
	params DynamicRateLimitingParams,
	burstRatio PersistenceBurstRatio,
	metricsHandler metrics.Handler,
	logger log.Logger,
) *HealthRequestRateLimiterImpl {
	rateBurst := quotas.NewDefaultRateBurst(rateFn, quotas.BurstRatioFn(burstRatio))
	limiter := &HealthRequestRateLimiterImpl{
		enabled:        atomic.Bool{},
		rateLimiter:    quotas.NewRateLimiter(rateBurst.Rate(), rateBurst.Burst()),
		healthSignals:  healthSignals,
		rateBurst:      rateBurst,
		params:         params,
		refreshTimer:   time.NewTicker(DefaultRefreshInterval),
		metricsHandler: metricsHandler,
		logger:         logger,
	}
	curRateMultiplier := new(float64)
	*curRateMultiplier = DefaultInitialRateMultiplier
	limiter.curRateMultiplier.Store(curRateMultiplier)
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
	curOptions := *rl.curOptions.Load()
	curRateMultiplier := *rl.curRateMultiplier.Load()
	if rl.latencyThresholdExceeded() || rl.errorThresholdExceeded() {
		// limit exceeded, do backoff
		curRateMultiplier = math.Max(curOptions.RateMultiMin, curRateMultiplier-curOptions.RateBackoffStepSize)
		metrics.DynamicRateLimiterMultiplier.With(rl.metricsHandler).Record(curRateMultiplier)
		rl.logger.Info(
			"Health threshold exceeded, reducing rate limit.",
			tag.NewFloat64("newMulti", curRateMultiplier),
			tag.NewFloat64("newRate", rl.rateLimiter.Rate()),
			tag.NewFloat64("latencyAvg", rl.healthSignals.AverageLatency()),
			tag.NewFloat64("errorRatio", rl.healthSignals.ErrorRatio()),
		)
	} else if curRateMultiplier < curOptions.RateMultiMax {
		// already doing backoff and under thresholds, increase limit
		curRateMultiplier = math.Min(curOptions.RateMultiMax, curRateMultiplier+curOptions.RateIncreaseStepSize)
		metrics.DynamicRateLimiterMultiplier.With(rl.metricsHandler).Record(curRateMultiplier)
		rl.logger.Info(
			"System healthy, increasing rate limit.",
			tag.NewFloat64("newMulti", curRateMultiplier),
			tag.NewFloat64("newRate", rl.rateLimiter.Rate()),
			tag.NewFloat64("latencyAvg", rl.healthSignals.AverageLatency()),
			tag.NewFloat64("errorRatio", rl.healthSignals.ErrorRatio()),
		)
	}
	rl.curRateMultiplier.Store(&curRateMultiplier)
	// Always set rate to pickup changes to underlying rate limit dynamic config
	rl.rateLimiter.SetRPS(curRateMultiplier * rl.rateBurst.Rate())
	rl.rateLimiter.SetBurst(int(curRateMultiplier * float64(rl.rateBurst.Burst())))
}

func (rl *HealthRequestRateLimiterImpl) refreshDynamicParams() {
	options := rl.params()
	rl.enabled.Store(options.Enabled)
	rl.curOptions.Store(&options)
}

func (rl *HealthRequestRateLimiterImpl) updateRefreshTimer() {
	rl.refreshTimer.Reset(rl.curOptions.Load().RefreshInterval)
}

func (rl *HealthRequestRateLimiterImpl) latencyThresholdExceeded() bool {
	curOptions := *rl.curOptions.Load()
	return curOptions.LatencyThreshold > 0 && rl.healthSignals.AverageLatency() > curOptions.LatencyThreshold
}

func (rl *HealthRequestRateLimiterImpl) errorThresholdExceeded() bool {
	curOptions := *rl.curOptions.Load()
	return curOptions.ErrorThreshold > 0 && rl.healthSignals.ErrorRatio() > curOptions.ErrorThreshold
}
