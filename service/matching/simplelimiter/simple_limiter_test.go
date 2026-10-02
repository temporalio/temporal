package simplelimiter

import (
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSimpleLimiter(t *testing.T) {
	p := MakeParams(10, time.Second)

	base := time.Now().UnixNano()
	now := base
	var ready Ready

	// can consume 11 tokens immediately (1 since we're starting from 0 and 10 burst)
	for range 11 {
		require.GreaterOrEqual(t, now, ready)
		ready = ready.Consume(p, now, 1)
	}
	// now not ready anymore
	require.Less(t, now, ready)

	// after 100 ms, we can consume one more
	now += int64(99 * time.Millisecond)
	require.Less(t, now, ready)
	now += int64(1 * time.Millisecond)
	require.GreaterOrEqual(t, now, ready)
	ready = ready.Consume(p, now, 1)
	require.Less(t, now, ready)
}

func TestSimpleLimiterOverTime(t *testing.T) {
	p := MakeParams(10, time.Second)

	base := time.Now().UnixNano()
	now := base
	var ready Ready

	consumed := int64(0)
	for range 10000 {
		// sleep for some random time, average < 100ms, so we are limited on average
		// but have some gaps too.
		now += (70 + rand.Int63n(50)) * int64(time.Millisecond)

		if now >= int64(ready) {
			ready = ready.Consume(p, now, 1)
			consumed++
		}
	}

	effectiveRate := float64(consumed) / float64(now-base) * float64(time.Second)
	require.InEpsilon(t, 10, effectiveRate, 0.01)
}

func TestSimpleLimiterRecycle(t *testing.T) {
	p := MakeParams(10, time.Second)

	base := time.Now().UnixNano()
	now := base
	var ready Ready

	consumed := int64(0)
	for range 10000 {
		// sleep for some random time, always < 100ms, so we are always limited
		now += (30 + rand.Int63n(30)) * int64(time.Millisecond)

		if now >= int64(ready) {
			ready = ready.Consume(p, now, 1)
			consumed++

			// 20% of the time, recycle the token we took
			if rand.Intn(100) < 20 {
				now += int64(5 * time.Millisecond)
				ready = ready.Consume(p, now, -1)
				consumed--
			}
		}
	}

	effectiveRate := float64(consumed) / float64(now-base) * float64(time.Second)
	require.InEpsilon(t, 10, effectiveRate, 0.01)
}

func TestSimpleLimiterUnlimited(t *testing.T) {
	now := time.Now().UnixNano()
	var ready Ready

	pInf := MakeParams(1e12, 0)
	require.False(t, pInf.Never())
	require.False(t, pInf.Limited())

	for range 1000 {
		ready = ready.Consume(pInf, now, 1)
		require.LessOrEqual(t, ready.Delay(now), time.Duration(0))
	}
}

func TestSimpleLimiterLowToHigh(t *testing.T) {
	for _, lowRate := range []float64{
		0,
		1e-8, // 1 per 1000+ days
	} {
		pLow := MakeParams(lowRate, time.Second)
		require.Equal(t, pLow.Never(), (lowRate == 0))

		now := time.Now().UnixNano()
		var ready Ready
		ready = ready.Consume(pLow, now, 1)
		// not ready yet
		require.Greater(t, ready.Delay(now), time.Duration(0))
		// not ready even after 1 day
		require.Greater(t, ready.Delay(now+(24*time.Hour).Nanoseconds()), time.Duration(0))

		// try clipping using the low limit
		ready = ready.Clip(pLow, now, 1)
		// still not ready now or in 1 day
		require.Greater(t, ready.Delay(now), time.Duration(0))
		require.Greater(t, ready.Delay(now+(24*time.Hour).Nanoseconds()), time.Duration(0))

		// switch to higher rate limit
		pHigh := MakeParams(10, time.Second)
		require.False(t, pHigh.Never())
		require.True(t, pHigh.Limited())

		// clip to high limit
		ready = ready.Clip(pHigh, now, 1)
		// not ready yet
		require.Greater(t, ready.Delay(now), time.Duration(0))
		// ready within one minute
		require.Less(t, ready.Delay(now+time.Minute.Nanoseconds()), time.Duration(0))
	}
}

func TestAvailableSimpleLimiterTokens(t *testing.T) {
	nowNs := time.Now().UnixNano()
	require.Equal(t, int32(11), Ready(0).Available(MakeParams(10, time.Second), nowNs))
	require.Equal(t, int32(0), Ready(nowNs+1).Available(MakeParams(10, time.Second), nowNs))
	require.Equal(t, int32(0), Ready(0).Available(MakeParams(0, time.Second), nowNs))
	require.Equal(t, int32(math.MaxInt32), Ready(0).Available(MakeParams(1e12, 0), nowNs))
}
