package quotas

import (
	"context"
	"time"
)

type (
	// RoutingRateLimiterImpl is a rate limiter special built for multi-tenancy
	RoutingRateLimiterImpl struct {
		apiToRateLimiter map[string]RequestRateLimiter
	}
)

var _ RequestRateLimiter = (*RoutingRateLimiterImpl)(nil)

func NewRoutingRateLimiter(
	apiToRateLimiter map[string]RequestRateLimiter,
) *RoutingRateLimiterImpl {
	return &RoutingRateLimiterImpl{
		apiToRateLimiter: apiToRateLimiter,
	}
}

// Allow attempts to allow a request to go through. The method returns
// immediately with a true or false indicating if the request can make
// progress
func (r *RoutingRateLimiterImpl) Allow(
	now time.Time,
	request Request,
) bool {
	rateLimiter, ok := r.apiToRateLimiter[request.API]
	if !ok {
		return true
	}
	return rateLimiter.Allow(now, request)
}

// Reserve returns a Reservation that indicates how long the caller
// must wait before event happen.
func (r *RoutingRateLimiterImpl) Reserve(
	now time.Time,
	request Request,
) Reservation {
	rateLimiter, ok := r.apiToRateLimiter[request.API]
	if !ok {
		return NoopReservation
	}
	return rateLimiter.Reserve(now, request)
}

// Wait waits till the deadline for a rate limit token to allow the request
// to go through.
func (r *RoutingRateLimiterImpl) Wait(
	ctx context.Context,
	request Request,
) error {
	rateLimiter, ok := r.apiToRateLimiter[request.API]
	if !ok {
		return nil
	}
	return rateLimiter.Wait(ctx, request)
}
