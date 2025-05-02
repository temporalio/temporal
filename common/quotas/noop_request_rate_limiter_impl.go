package quotas

import (
	"context"
	"time"
)

type (
	// NoopRequestRateLimiterImpl is a no-op implementation for RequestRateLimiter
	NoopRequestRateLimiterImpl struct{}
)

var NoopRequestRateLimiter RequestRateLimiter = &NoopRequestRateLimiterImpl{}

func (r *NoopRequestRateLimiterImpl) Allow(
	_ time.Time,
	_ Request,
) bool {
	return true
}

func (r *NoopRequestRateLimiterImpl) Reserve(
	_ time.Time,
	_ Request,
) Reservation {
	return NoopReservation
}

func (r *NoopRequestRateLimiterImpl) Wait(
	_ context.Context,
	_ Request,
) error {
	return nil
}
