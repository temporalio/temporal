package quotas_test

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/quotas"
)

func TestDynamicRateLimiterInitialZeroQuota(t *testing.T) {
	t.Parallel()

	rateBurst := quotas.NewMutableRateBurst(0, 0)
	limiter := quotas.NewDynamicRateLimiter(rateBurst, time.Hour)
	now := time.Now()
	require.False(t, limiter.AllowN(now, 1))
	require.False(t, limiter.ReserveN(now, 1).OK())
	limiter.Refresh()
	require.False(t, limiter.AllowN(now.Add(time.Hour), 1))

	rateBurst.SetRPS(1)
	rateBurst.SetBurst(2)
	limiter.Refresh()
	require.True(t, limiter.AllowN(now, 2))
	require.False(t, limiter.AllowN(now, 1))
	limiter.Refresh()
	require.False(t, limiter.AllowN(now, 1))
}

func TestDynamicRateLimiterPausePreservesReservations(t *testing.T) {
	t.Parallel()

	for _, initialRate := range []float64{0, 1} {
		t.Run(fmt.Sprintf("initial_rate_%g", initialRate), func(t *testing.T) {
			t.Parallel()

			rateBurst := quotas.NewMutableRateBurst(initialRate, int(initialRate)*2)
			limiter := quotas.NewDynamicRateLimiter(rateBurst, time.Hour)
			rateBurst.SetRPS(1)
			rateBurst.SetBurst(2)
			limiter.Refresh()
			now := time.Now()
			require.True(t, limiter.AllowN(now, 2))
			reservation := limiter.ReserveN(now, 1)
			require.True(t, reservation.OK())

			rateBurst.SetRPS(0)
			rateBurst.SetBurst(0)
			limiter.Refresh()
			require.False(t, limiter.AllowN(now, 1))
			rateBurst.SetRPS(1)
			rateBurst.SetBurst(2)
			limiter.Refresh()
			require.False(t, limiter.AllowN(now, 1))
			require.Greater(t, limiter.ReserveN(now, 1).DelayFrom(now), time.Second)
		})
	}
}

func TestDynamicRateLimiterZeroRateWithBurstDoesNotReinitialize(t *testing.T) {
	t.Parallel()

	rateBurst := quotas.NewMutableRateBurst(0, 1)
	limiter := quotas.NewDynamicRateLimiter(rateBurst, time.Hour)
	now := time.Now()
	require.True(t, limiter.AllowN(now, 1))
	rateBurst.SetRPS(1)
	rateBurst.SetBurst(2)
	limiter.Refresh()
	require.False(t, limiter.AllowN(now, 1))
}

func TestDynamicRateLimiterZeroRateReservationDoesNotReinitialize(t *testing.T) {
	t.Parallel()

	rateBurst := quotas.NewMutableRateBurst(0, 0)
	limiter := quotas.NewDynamicRateLimiter(rateBurst, time.Hour)
	now := time.Now()
	rateBurst.SetBurst(1)
	limiter.Refresh()
	require.True(t, limiter.ReserveN(now, 1).OK())

	rateBurst.SetRPS(1)
	rateBurst.SetBurst(2)
	limiter.Refresh()
	require.False(t, limiter.AllowN(now, 1))
	require.Greater(t, limiter.ReserveN(now, 1).DelayFrom(now), time.Second)
}

func TestDynamicRateLimiterInitialZeroQuotaRefreshOnRequest(t *testing.T) {
	t.Parallel()

	rateBurst := quotas.NewMutableRateBurst(0, 0)
	limiter := quotas.NewDynamicRateLimiter(rateBurst, 0)
	now := time.Now()
	require.False(t, limiter.AllowN(now, 1))
	rateBurst.SetRPS(1)
	rateBurst.SetBurst(2)
	require.True(t, limiter.AllowN(now, 2))
	require.False(t, limiter.AllowN(now, 1))
}

func TestDynamicRateLimiterInitialZeroQuotaConcurrentRefresh(t *testing.T) {
	t.Parallel()

	rateBurst := quotas.NewMutableRateBurst(0, 0)
	limiter := quotas.NewDynamicRateLimiter(rateBurst, time.Hour)
	rateBurst.SetRPS(1)
	rateBurst.SetBurst(8)
	now := time.Now()
	var allowed atomic.Int32
	var wg sync.WaitGroup
	for range 64 {
		wg.Go(func() {
			limiter.Refresh()
			if limiter.AllowN(now, 1) {
				allowed.Add(1)
			}
		})
	}
	wg.Wait()
	require.EqualValues(t, 8, allowed.Load())
}

func TestDynamicRateLimiterInitialZeroQuotaPriorityIsolation(t *testing.T) {
	t.Parallel()

	rateBurst := quotas.NewMutableRateBurst(0, 0)
	high := quotas.NewDynamicRateLimiter(rateBurst, time.Hour)
	low := quotas.NewDynamicRateLimiter(rateBurst, time.Hour)
	limiter := quotas.NewPriorityRateLimiter(
		func(request quotas.Request) int { return int(request.CallerSegment) },
		map[int]quotas.RequestRateLimiter{
			0: quotas.NewRequestRateLimiterAdapter(high),
			1: quotas.NewRequestRateLimiterAdapter(low),
		},
	)
	highRequest := quotas.NewRequest("test-api", 1, "test-namespace", "", 0, "")
	lowRequest := quotas.NewRequest("test-api", 1, "test-namespace", "", 1, "")
	now := time.Now()
	rateBurst.SetRPS(1)
	rateBurst.SetBurst(2)
	low.Refresh()
	require.True(t, limiter.Allow(now, lowRequest))
	require.True(t, limiter.Allow(now, lowRequest))
	require.False(t, limiter.Allow(now, lowRequest))

	// The higher priority remains idle until after the lower priority has spent its burst.
	high.Refresh()
	require.True(t, limiter.Allow(now, highRequest))
	require.True(t, limiter.Allow(now, highRequest))
	require.False(t, limiter.Allow(now, highRequest))
	low.Refresh()
	require.False(t, limiter.Allow(now, lowRequest))
	require.Greater(t, low.ReserveN(now, 1).DelayFrom(now), 2*time.Second)
}

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
