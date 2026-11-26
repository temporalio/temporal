package quotas

import (
	"context"
	"time"
)

type contextualRequestRateLimiter interface {
	AllowWithContext(context.Context, time.Time, Request) bool
}

// AllowWithContext preserves attribution through an immediate admission probe.
func AllowWithContext(ctx context.Context, limiter RequestRateLimiter, now time.Time, request Request) bool {
	if contextual, ok := limiter.(contextualRequestRateLimiter); ok {
		return contextual.AllowWithContext(ctx, now, request)
	}
	return limiter.Allow(now, request)
}

func (r *MapRequestRateLimiterImpl[K]) AllowWithContext(ctx context.Context, now time.Time, request Request) bool {
	return AllowWithContext(ctx, r.getOrInitRateLimiter(now, request), now, request)
}
