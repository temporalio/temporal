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
	rate  float64
	burst int
	calls atomic.Int64
}

func (c *countingRateBurst) Rate() float64 {
	c.calls.Add(1)
	return c.rate
}

func (c *countingRateBurst) Burst() int {
	return c.burst
}

func TestDynamicRateLimiterRefreshesOnceAfterIdleIntervals(t *testing.T) {
	t.Parallel()

	const (
		// Wide enough that the goroutine burst below cannot straddle the
		// deadline and trigger a second refresh on a loaded machine.
		refreshInterval = 300 * time.Millisecond
		idleIntervals   = 3
		goroutines      = 64
	)

	rateBurst := &countingRateBurst{rate: 10, burst: 10}
	limiter := quotas.NewDynamicRateLimiter(rateBurst, refreshInterval)

	// One Rate() call per Refresh(); the constructor already consumed one.
	callsAfterInit := rateBurst.calls.Load()

	// The refresh deadline is anchored to a package-level monotonic epoch with
	// no injectable clock, so real time has to pass to cross it.
	//nolint:forbidigo // no await helper can advance the limiter's own clock
	time.Sleep(idleIntervals*refreshInterval + 50*time.Millisecond)

	// A WaitGroup broadcast wakes goroutines one at a time, which lets the first
	// caller advance the deadline before the rest even load it. Spinning keeps
	// them hot so they reach the compare-and-swap together.
	var gate atomic.Bool
	var ready, done sync.WaitGroup
	ready.Add(goroutines)
	done.Add(goroutines)
	for range goroutines {
		go func() {
			defer done.Done()
			ready.Done()
			for !gate.Load() {
			}
			limiter.AllowN(time.Now(), 1)
		}()
	}
	ready.Wait()
	gate.Store(true)
	done.Wait()

	require.Equal(t, callsAfterInit+1, rateBurst.calls.Load(),
		"concurrent callers past several idle intervals must trigger exactly one refresh")
}

func TestDynamicRateLimiterRefreshesOncePerInterval(t *testing.T) {
	t.Parallel()

	const (
		refreshInterval  = 200 * time.Millisecond
		intervals        = 3
		callsPerInterval = 20
	)

	rateBurst := &countingRateBurst{rate: 10, burst: 10}
	limiter := quotas.NewDynamicRateLimiter(rateBurst, refreshInterval)
	callsAfterInit := rateBurst.calls.Load()

	for i := 1; i <= intervals; i++ {
		//nolint:forbidigo // no await helper can advance the limiter's own clock
		time.Sleep(refreshInterval + 40*time.Millisecond)

		for range callsPerInterval {
			limiter.AllowN(time.Now(), 1)
		}

		require.Equal(t, callsAfterInit+int64(i), rateBurst.calls.Load(),
			"interval %d: many calls within one interval must refresh exactly once", i)
	}
}
