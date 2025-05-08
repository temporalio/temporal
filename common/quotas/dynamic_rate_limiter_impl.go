package quotas

import (
	"context"
	"time"
)

const (
	defaultRefreshInterval        = time.Minute
	defaultIncomingRateBurstRatio = float64(2)
	defaultOutgoingRateBurstRatio = float64(1)
)

type (
	// DynamicRateLimiterImpl implements a dynamic config wrapper around the rate limiter
	DynamicRateLimiterImpl struct {
		rateBurstFn     RateBurst
		refreshInterval time.Duration

		refreshTimer *time.Timer
		rateLimiter  *RateLimiterImpl
	}
)

var _ RateLimiter = (*DynamicRateLimiterImpl)(nil)

// NewDynamicRateLimiter returns a rate limiter which handles dynamic config
func NewDynamicRateLimiter(
	rateBurstFn RateBurst,
	refreshInterval time.Duration,
) *DynamicRateLimiterImpl {
	rateLimiter := &DynamicRateLimiterImpl{
		rateBurstFn:     rateBurstFn,
		refreshInterval: refreshInterval,

		refreshTimer: time.NewTimer(refreshInterval),
		rateLimiter:  NewRateLimiter(rateBurstFn.Rate(), rateBurstFn.Burst()),
	}
	return rateLimiter
}

// NewDefaultIncomingRateLimiter returns a default rate limiter
// for incoming traffic, using fixed burst ratio of 2
// and fixed 1 minute refresh interval
func NewDefaultIncomingRateLimiter(
	rateFn RateFn,
) *DynamicRateLimiterImpl {
	return NewDynamicRateLimiter(
		NewDefaultIncomingRateBurst(rateFn),
		defaultRefreshInterval,
	)
}

// NewDefaultOutgoingRateLimiter returns a default rate limiter
// for outgoing traffic, using fixed burst ratio of 2
// and fixed 1 minute refresh interval
func NewDefaultOutgoingRateLimiter(
	rateFn RateFn,
) *DynamicRateLimiterImpl {
	return NewDynamicRateLimiter(
		NewDefaultOutgoingRateBurst(rateFn),
		defaultRefreshInterval,
	)
}

// NewDefaultRateLimiter returns a default rate limiter with a dynamic burst ratio
// and fixed 1 minute refresh interval
func NewDefaultRateLimiter(
	rateFn RateFn,
	burstRatioFn BurstRatioFn,
) *DynamicRateLimiterImpl {
	return NewDynamicRateLimiter(
		NewDefaultRateBurst(rateFn, burstRatioFn),
		defaultRefreshInterval,
	)
}

// Allow immediately returns with true or false indicating if a rate limit
// token is available or not
func (d *DynamicRateLimiterImpl) Allow() bool {
	d.maybeRefresh()
	return d.rateLimiter.Allow()
}

// AllowN immediately returns with true or false indicating if n rate limit
// token is available or not
func (d *DynamicRateLimiterImpl) AllowN(now time.Time, numToken int) bool {
	d.maybeRefresh()
	return d.rateLimiter.AllowN(now, numToken)
}

// Reserve reserves a rate limit token
func (d *DynamicRateLimiterImpl) Reserve() Reservation {
	d.maybeRefresh()
	return d.rateLimiter.Reserve()
}

// ReserveN reserves n rate limit token
func (d *DynamicRateLimiterImpl) ReserveN(now time.Time, numToken int) Reservation {
	d.maybeRefresh()
	return d.rateLimiter.ReserveN(now, numToken)
}

// Wait waits up till deadline for a rate limit token
func (d *DynamicRateLimiterImpl) Wait(ctx context.Context) error {
	d.maybeRefresh()
	return d.rateLimiter.Wait(ctx)
}

// WaitN waits up till deadline for n rate limit token
func (d *DynamicRateLimiterImpl) WaitN(ctx context.Context, numToken int) error {
	d.maybeRefresh()
	return d.rateLimiter.WaitN(ctx, numToken)
}

// Rate returns the rate per second for this rate limiter
func (d *DynamicRateLimiterImpl) Rate() float64 {
	return d.rateLimiter.Rate()
}

// Burst returns the burst for this rate limiter
func (d *DynamicRateLimiterImpl) Burst() int {
	return d.rateLimiter.Burst()
}

func (d *DynamicRateLimiterImpl) Refresh() {
	d.rateLimiter.SetRateBurst(d.rateBurstFn.Rate(), d.rateBurstFn.Burst())
}

func (d *DynamicRateLimiterImpl) maybeRefresh() {
	select {
	case <-d.refreshTimer.C:
		d.refreshTimer.Reset(d.refreshInterval)
		d.Refresh()

	default:
		// noop
	}
}

func (d *DynamicRateLimiterImpl) TokensAt(t time.Time) int {
	return d.rateLimiter.TokensAt(t)
}

// RecycleToken returns a token to the rate limiter
func (d *DynamicRateLimiterImpl) RecycleToken() {
	d.rateLimiter.RecycleToken()
}
