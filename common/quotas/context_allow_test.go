package quotas

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type contextualTestLimiter struct {
	RequestRateLimiter
	context context.Context
	request Request
	at      time.Time
}

func (l *contextualTestLimiter) AllowWithContext(ctx context.Context, at time.Time, request Request) bool {
	l.context, l.at, l.request = ctx, at, request
	return false
}

func TestMapAllowWithContext(t *testing.T) {
	t.Parallel()
	inner := &contextualTestLimiter{RequestRateLimiter: NoopRequestRateLimiter}
	limiter := NewNamespaceRequestRateLimiter(func(Request) RequestRateLimiter { return inner })
	t.Cleanup(limiter.cleanupTicker.Stop)
	request := Request{Caller: "namespace", Token: 1}
	at := time.Now()
	require.False(t, AllowWithContext(t.Context(), limiter, at, request))
	require.Same(t, t.Context(), inner.context)
	require.Equal(t, request, inner.request)
	require.Equal(t, at, inner.at)
	require.True(t, AllowWithContext(t.Context(), NoopRequestRateLimiter, at, request))
}
