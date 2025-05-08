package quotas

import (
	"sync"
	"time"

	"go.temporal.io/server/common/clock"
	"golang.org/x/time/rate"
)

type (
	// RateLimiterImpl is a wrapper around the golang rate limiter
	RateLimiterImpl struct {
		sync.RWMutex
		rps        float64
		burst      int
		timeSource clock.TimeSource
		ClockedRateLimiter
	}
)

var _ RateLimiter = (*RateLimiterImpl)(nil)

// NewRateLimiter returns a new rate limiter that can handle dynamic
// configuration updates
func NewRateLimiter(newRPS float64, newBurst int) *RateLimiterImpl {
	limiter := rate.NewLimiter(rate.Limit(newRPS), newBurst)
	ts := clock.NewRealTimeSource()
	rl := &RateLimiterImpl{
		rps:                newRPS,
		burst:              newBurst,
		timeSource:         ts,
		ClockedRateLimiter: NewClockedRateLimiter(limiter, ts),
	}

	return rl
}

// SetRPS sets the rate of the rate limiter
func (rl *RateLimiterImpl) SetRPS(rps float64) {
	rl.refreshInternalRateLimiterImpl(&rps, nil)
}

// SetBurst sets the burst of the rate limiter
func (rl *RateLimiterImpl) SetBurst(burst int) {
	rl.refreshInternalRateLimiterImpl(nil, &burst)
}

func (rl *RateLimiterImpl) Reserve() Reservation {
	return rl.ClockedRateLimiter.Reserve()
}

func (rl *RateLimiterImpl) ReserveN(now time.Time, n int) Reservation {
	return rl.ClockedRateLimiter.ReserveN(now, n)
}

// SetRateBurst sets the rps & burst of the rate limiter
func (rl *RateLimiterImpl) SetRateBurst(rps float64, burst int) {
	rl.refreshInternalRateLimiterImpl(&rps, &burst)
}

// Rate returns the rps for this rate limiter
func (rl *RateLimiterImpl) Rate() float64 {
	rl.Lock()
	defer rl.Unlock()

	return rl.rps
}

// Burst returns the burst for this rate limiter
func (rl *RateLimiterImpl) Burst() int {
	rl.Lock()
	defer rl.Unlock()

	return rl.burst
}

// TokensAt returns the number of tokens that will be available at time t
func (rl *RateLimiterImpl) TokensAt(t time.Time) int {
	rl.Lock()
	defer rl.Unlock()

	return rl.ClockedRateLimiter.TokensAt(t)
}

func (rl *RateLimiterImpl) refreshInternalRateLimiterImpl(
	newRate *float64,
	newBurst *int,
) {
	rl.Lock()
	defer rl.Unlock()

	refresh := false

	if newRate != nil && rl.rps != *newRate {
		rl.rps = *newRate
		refresh = true
	}

	if newBurst != nil && rl.burst != *newBurst {
		rl.burst = *newBurst
		refresh = true
	}

	if refresh {
		now := rl.timeSource.Now()
		rl.SetLimitAt(now, rate.Limit(rl.rps))
		rl.SetBurstAt(now, rl.burst)
	}
}

// RecycleToken returns a token to the rate limiter
func (rl *RateLimiterImpl) RecycleToken() {
	rl.ClockedRateLimiter.RecycleToken()
}
