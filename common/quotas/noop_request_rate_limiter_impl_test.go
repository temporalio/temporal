package quotas_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.temporal.io/server/common/quotas"
)

func TestNoopRequestRateLimiterImpl(t *testing.T) {
	t.Parallel()

	testNoopRequestRateLimiterImpl(t, quotas.NoopRequestRateLimiter)
}

func testNoopRequestRateLimiterImpl(t *testing.T, rl quotas.RequestRateLimiter) {
	assert.True(t, rl.Allow(time.Now(), quotas.Request{}))
	assert.Equal(t, quotas.NoopReservation, rl.Reserve(time.Now(), quotas.Request{}))
	assert.NoError(t, rl.Wait(context.Background(), quotas.Request{}))
}
