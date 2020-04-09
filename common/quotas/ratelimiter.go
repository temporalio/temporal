package quotas

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"
)

const (
	_defaultRPSTTL = 60 * time.Second
	_burstSize     = 1
)

// RateLimiter is a wrapper around the golang rate limiter handling dynamic
// configuration updates of the max dispatch per second. This has comparable
// performance to the token bucket rate limiter.
// BenchmarkSimpleRateLimiter-4   	10000000	       114 ns/op (tokenbucket)
// BenchmarkRateLimiter-4         	10000000	       148 ns/op (this)
type RateLimiter struct {
	sync.RWMutex
	maxDispatchPerSecond *float64
	goRateLimiter        atomic.Value
	// TTL is used to determine whether to update the limit. Until TTL, pick
	// lower(existing TTL, input TTL). After TTL, pick input TTL if different from existing TTL
	ttlTimer *time.Timer
	ttl      time.Duration
	minBurst int
}

// NewSimpleRateLimiter returns a new rate limiter backed by the golang rate
// limiter
func NewSimpleRateLimiter(rps int) *RateLimiter {
	initialRps := float64(rps)
	return NewRateLimiter(&initialRps, _defaultRPSTTL, _burstSize)
}

// NewRateLimiter returns a new rate limiter that can handle dynamic
// configuration updates
func NewRateLimiter(maxDispatchPerSecond *float64, ttl time.Duration, minBurst int) *RateLimiter {
	rl := &RateLimiter{
		maxDispatchPerSecond: maxDispatchPerSecond,
		ttl:                  ttl,
		ttlTimer:             time.NewTimer(ttl),
		minBurst:             minBurst,
	}
	rl.storeLimiter(maxDispatchPerSecond)
	return rl
}

// UpdateMaxDispatch updates the max dispatch rate of the rate limiter
func (rl *RateLimiter) UpdateMaxDispatch(maxDispatchPerSecond *float64) {
	if rl.shouldUpdate(maxDispatchPerSecond) {
		rl.Lock()
		rl.maxDispatchPerSecond = maxDispatchPerSecond
		rl.storeLimiter(maxDispatchPerSecond)
		rl.Unlock()
	}
}

// Wait waits up till deadline for a rate limit token
func (rl *RateLimiter) Wait(ctx context.Context) error {
	limiter := rl.goRateLimiter.Load().(*rate.Limiter)
	return limiter.Wait(ctx)
}

// Reserve reserves a rate limit token
func (rl *RateLimiter) Reserve() *rate.Reservation {
	limiter := rl.goRateLimiter.Load().(*rate.Limiter)
	return limiter.Reserve()
}

// Allow immediately returns with true or false indicating if a rate limit
// token is available or not
func (rl *RateLimiter) Allow() bool {
	limiter := rl.goRateLimiter.Load().(*rate.Limiter)
	return limiter.Allow()
}

// Limit returns the current rate per second limit for this ratelimiter
func (rl *RateLimiter) Limit() float64 {
	if rl.maxDispatchPerSecond != nil {
		return *rl.maxDispatchPerSecond
	}
	return math.MaxFloat64
}

func (rl *RateLimiter) storeLimiter(maxDispatchPerSecond *float64) {
	burst := int(*maxDispatchPerSecond)
	// If throttling is zero, burst also has to be 0
	if *maxDispatchPerSecond != 0 && burst <= rl.minBurst {
		burst = rl.minBurst
	}
	limiter := rate.NewLimiter(rate.Limit(*maxDispatchPerSecond), burst)
	rl.goRateLimiter.Store(limiter)
}

func (rl *RateLimiter) shouldUpdate(maxDispatchPerSecond *float64) bool {
	if maxDispatchPerSecond == nil {
		return false
	}
	select {
	case <-rl.ttlTimer.C:
		rl.ttlTimer.Reset(rl.ttl)
		rl.RLock()
		defer rl.RUnlock()
		return *maxDispatchPerSecond != *rl.maxDispatchPerSecond
	default:
		rl.RLock()
		defer rl.RUnlock()
		return *maxDispatchPerSecond < *rl.maxDispatchPerSecond
	}
}

// DynamicRateLimiter implements a dynamic config wrapper around the rate limiter
type DynamicRateLimiter struct {
	rps RPSFunc
	rl  *RateLimiter
}

// NewDynamicRateLimiter returns a rate limiter which handles dynamic config
func NewDynamicRateLimiter(rps RPSFunc) *DynamicRateLimiter {
	initialRps := rps()
	rl := NewRateLimiter(&initialRps, _defaultRPSTTL, _burstSize)
	return &DynamicRateLimiter{rps, rl}
}

// Allow immediately returns with true or false indicating if a rate limit
// token is available or not
func (d *DynamicRateLimiter) Allow() bool {
	rps := d.rps()
	d.rl.UpdateMaxDispatch(&rps)
	return d.rl.Allow()
}

// Wait waits up till deadline for a rate limit token
func (d *DynamicRateLimiter) Wait(ctx context.Context) error {
	rps := d.rps()
	d.rl.UpdateMaxDispatch(&rps)
	return d.rl.Wait(ctx)
}

// Reserve reserves a rate limit token
func (d *DynamicRateLimiter) Reserve() *rate.Reservation {
	rps := d.rps()
	d.rl.UpdateMaxDispatch(&rps)
	return d.rl.Reserve()
}
