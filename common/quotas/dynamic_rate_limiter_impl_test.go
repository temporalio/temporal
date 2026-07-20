package quotas_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/quotas"
)

func TestDynamicRateLimiterRateAndBurstRefreshAfterInterval(t *testing.T) {
	t.Parallel()

	const (
		refreshInterval = 50 * time.Millisecond
		initialRate     = 10.0
		initialBurst    = 20
		updatedRate     = 30.0
		updatedBurst    = 40
		nextRate        = 50.0
		nextBurst       = 60
	)

	t.Run("rate_refreshes_without_token_operation", func(t *testing.T) {
		rateBurst := quotas.NewMutableRateBurst(initialRate, initialBurst)
		limiter := quotas.NewDynamicRateLimiter(rateBurst, refreshInterval)

		require.InDelta(t, initialRate, limiter.Rate(), 1e-9)

		rateBurst.SetRPS(updatedRate)
		rateBurst.SetBurst(updatedBurst)

		require.Eventually(t, func() bool {
			return limiter.Rate() == updatedRate
		}, time.Second, 5*time.Millisecond)

		rateBurst.SetRPS(nextRate)
		rateBurst.SetBurst(nextBurst)

		require.InDelta(t, updatedRate, limiter.Rate(), 1e-9)
	})

	t.Run("burst_refreshes_without_token_operation", func(t *testing.T) {
		rateBurst := quotas.NewMutableRateBurst(initialRate, initialBurst)
		limiter := quotas.NewDynamicRateLimiter(rateBurst, refreshInterval)

		require.Equal(t, initialBurst, limiter.Burst())

		rateBurst.SetRPS(updatedRate)
		rateBurst.SetBurst(updatedBurst)

		require.Eventually(t, func() bool {
			return limiter.Burst() == updatedBurst
		}, time.Second, 5*time.Millisecond)

		rateBurst.SetRPS(nextRate)
		rateBurst.SetBurst(nextBurst)

		require.Equal(t, updatedBurst, limiter.Burst())
	})
}
