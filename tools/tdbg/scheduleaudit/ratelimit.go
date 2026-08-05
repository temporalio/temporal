package scheduleaudit

import (
	"context"
	"sync"

	"golang.org/x/time/rate"
)

// NamespaceRateLimiter bounds the outbound RPC rate per namespace. Every loader RPC calls Wait before issuing, so the
// total call rate against any single namespace stays at or below rps requests per second regardless of how many of that
// namespace's schedules are being processed concurrently. A separate limiter is kept per namespace so a slow or
// throttled namespace can't consume another namespace's budget.
type NamespaceRateLimiter struct {
	rps  int
	mu   sync.Mutex
	byNS map[string]*rate.Limiter
}

// NewNamespaceRateLimiter returns a limiter allowing rps requests per second per namespace. A non-positive rps disables
// limiting (Wait returns immediately).
func NewNamespaceRateLimiter(rps int) *NamespaceRateLimiter {
	return &NamespaceRateLimiter{rps: rps, byNS: map[string]*rate.Limiter{}}
}

// Wait blocks until a request to namespace may proceed under its rate limit, or until ctx is cancelled.
func (n *NamespaceRateLimiter) Wait(ctx context.Context, namespace string) error {
	if n == nil || n.rps <= 0 {
		return nil
	}
	return n.limiterFor(namespace).Wait(ctx)
}

func (n *NamespaceRateLimiter) limiterFor(namespace string) *rate.Limiter {
	n.mu.Lock()
	defer n.mu.Unlock()
	l, ok := n.byNS[namespace]
	if !ok {
		l = rate.NewLimiter(rate.Limit(n.rps), n.rps)
		n.byNS[namespace] = l
	}
	return l
}
