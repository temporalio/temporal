package quotas_test

import (
	"sync"
	"sync/atomic"
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

type countingRateBurst struct {
	rate  atomic.Uint64
	calls atomic.Int64
}

func (c *countingRateBurst) Rate() float64 {
	c.calls.Add(1)
	return float64(c.rate.Load())
}

func (c *countingRateBurst) Burst() int {
	return int(c.rate.Load())
}

func TestDynamicRateLimiterRefreshesOnceAcrossConcurrentCallers(t *testing.T) {
	t.Parallel()

	const (
		// Wide enough that the goroutine burst below cannot straddle the
		// deadline and trigger a second refresh on a loaded machine.
		refreshInterval = 500 * time.Millisecond
		goroutines      = 16
	)

	rateBurst := &countingRateBurst{}
	rateBurst.rate.Store(10)
	limiter := quotas.NewDynamicRateLimiter(rateBurst, refreshInterval)

	// One Rate() call per Refresh(); the constructor already consumed one.
	callsAfterInit := rateBurst.calls.Load()

	time.Sleep(refreshInterval + 100*time.Millisecond)

	var start, done sync.WaitGroup
	start.Add(1)
	done.Add(goroutines)
	for range goroutines {
		go func() {
			defer done.Done()
			start.Wait()
			limiter.AllowN(time.Now(), 1)
		}()
	}
	start.Done()
	done.Wait()

	require.Equal(t, callsAfterInit+1, rateBurst.calls.Load(),
		"concurrent callers past the deadline must trigger exactly one refresh")
}

func TestDynamicRateLimiterRefreshesRepeatedlyAcrossIntervals(t *testing.T) {
	t.Parallel()

	const refreshInterval = 20 * time.Millisecond

	rateBurst := quotas.NewMutableRateBurst(10, 10)
	limiter := quotas.NewDynamicRateLimiter(rateBurst, refreshInterval)

	for _, want := range []float64{20, 30, 40} {
		rateBurst.SetRPS(want)
		rateBurst.SetBurst(int(want))
		require.Eventually(t, func() bool {
			return limiter.Rate() == want
		}, time.Second, 2*time.Millisecond)
	}
}
