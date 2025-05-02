package quotas

import (
	"context"
	"sync/atomic"
	"time"
)

// RequestRateLimiterDelegator is a request rate limiter that delegates to another rate limiter. The delegate can be
// changed at runtime by calling SetRateLimiter. This rate limiter is useful for cases where you want to substitute one
// rate limiter implementation for another at runtime. All methods of this type are thread-safe.
type RequestRateLimiterDelegator struct {
	// delegate is an atomic.Value so that it can be safely read from and written to concurrently. It stores the rate
	// limiter that this rate limiter delegates to as a monomorphicRequestRateLimiter.
	delegate atomic.Value
}

// monomorphicRequestRateLimiter is a workaround for the fact that the value stored in an atomic.Value must always be
// the same type, but we want to allow the rate limiter delegate to be any type that implements the RequestRateLimiter
// interface.
type monomorphicRequestRateLimiter struct {
	RequestRateLimiter
}

// SetRateLimiter sets the rate limiter to delegate to.
func (d *RequestRateLimiterDelegator) SetRateLimiter(rl RequestRateLimiter) {
	d.delegate.Store(monomorphicRequestRateLimiter{rl})
}

// loadDelegate returns the rate limiter that this rate limiter delegates to.
func (d *RequestRateLimiterDelegator) loadDelegate() RequestRateLimiter {
	return d.delegate.Load().(RequestRateLimiter)
}

// The following methods just delegate to the underlying rate limiter.

func (d *RequestRateLimiterDelegator) Allow(now time.Time, request Request) bool {
	return d.loadDelegate().Allow(now, request)
}

func (d *RequestRateLimiterDelegator) Reserve(now time.Time, request Request) Reservation {
	return d.loadDelegate().Reserve(now, request)
}

func (d *RequestRateLimiterDelegator) Wait(ctx context.Context, request Request) error {
	return d.loadDelegate().Wait(ctx, request)
}
