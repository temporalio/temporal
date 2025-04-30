package quotas

import (
	"context"
	"time"
)

type (
	RequestRateLimiterAdapterImpl struct {
		rateLimiter RateLimiter
	}
)

var _ RequestRateLimiter = (*RequestRateLimiterAdapterImpl)(nil)

func NewRequestRateLimiterAdapter(
	rateLimiter RateLimiter,
) RequestRateLimiter {
	return &RequestRateLimiterAdapterImpl{
		rateLimiter: rateLimiter,
	}
}

func (r *RequestRateLimiterAdapterImpl) Allow(
	now time.Time,
	request Request,
) bool {
	return r.rateLimiter.AllowN(now, request.Token)
}

func (r *RequestRateLimiterAdapterImpl) Reserve(
	now time.Time,
	request Request,
) Reservation {
	return r.rateLimiter.ReserveN(now, request.Token)
}

func (r *RequestRateLimiterAdapterImpl) Wait(
	ctx context.Context,
	request Request,
) error {
	return r.rateLimiter.WaitN(ctx, request.Token)
}
