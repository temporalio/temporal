package queues

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/clock"
)

func TestThrottleState_FailedSubmitAfterWindowDoesNotIncreaseRate(t *testing.T) {
	o := defaultThrottleOverrides()
	o.initialRate = 1
	state, timeSource := newTestThrottleState(o)
	key := testKey()

	allowed, permit, _ := state.Admit(key)
	require.True(t, allowed)
	timeSource.Update(timeSource.Now().Add(testThrottleWindow))
	state.Finish(permit, false)

	require.Equal(t, 1.0, throttleRate(state, key))
	require.True(t, admitOK(state, key), "the unused token must be returned")
}

func TestThrottleState_SuccessfulSubmitCommitsRelease(t *testing.T) {
	state, _ := newTestThrottleState(defaultThrottleOverrides())
	key := testKey()
	allowed, permit, _ := state.Admit(key)
	require.True(t, allowed)

	state.Finish(permit, true)
	permit.Lock()
	defer permit.Unlock()
	require.Equal(t, int64(1), permit.releases)
	require.Zero(t, permit.pending)
}

func TestThrottleState_FailedSubmitRefundsWithoutRelease(t *testing.T) {
	o := defaultThrottleOverrides()
	o.initialRate = 1
	state, _ := newTestThrottleState(o)
	key := testKey()
	allowed, permit, _ := state.Admit(key)
	require.True(t, allowed)

	state.Finish(permit, false)
	permit.Lock()
	defer permit.Unlock()
	require.Equal(t, 1.0, permit.tokens)
	require.Zero(t, permit.releases)
	require.Zero(t, permit.pending)
}

func TestThrottleState_StalePermitChargesCurrentEntry(t *testing.T) {
	o := defaultThrottleOverrides()
	o.keyTTL = time.Second
	state, timeSource := newTestThrottleState(o)
	key := testKey()
	allowed, permit, _ := state.Admit(key)
	require.True(t, allowed)
	state.Finish(permit, true)

	timeSource.Update(timeSource.Now().Add(2 * o.keyTTL))
	state.getOrCreate(apsKey("other"))
	require.Nil(t, state.peek(key))

	state.ReportThrottled(key, permit)
	current := state.peek(key)
	require.NotNil(t, current)
	require.NotSame(t, permit, current)
	current.Lock()
	defer current.Unlock()
	require.Equal(t, int64(1), current.rejections)
}

func TestThrottleState_DeniedAdmitReportsTokenETA(t *testing.T) {
	o := defaultThrottleOverrides()
	o.initialRate = 4
	state, _ := newTestThrottleState(o)
	key := testKey()

	for range 4 {
		allowed, permit, retryAfter := state.Admit(key)
		require.True(t, allowed)
		require.Zero(t, retryAfter)
		state.Finish(permit, true)
	}

	allowed, permit, retryAfter := state.Admit(key)
	require.False(t, allowed)
	require.Nil(t, permit)
	require.Equal(t, 250*time.Millisecond, retryAfter)
}

func TestThrottleState_FailOpenAdmitHasNoPermit(t *testing.T) {
	o := defaultThrottleOverrides()
	o.maxKeys = 1
	state, _ := newTestThrottleState(o)

	require.True(t, admitOK(state, apsKey("tracked")))
	allowed, permit, _ := state.Admit(apsKey("overflow"))
	require.True(t, allowed)
	require.Nil(t, permit)
}

func TestThrottleState_RefillUsesRateFromElapsedWindow(t *testing.T) {
	o := defaultThrottleOverrides()
	o.initialRate = 100
	state, timeSource := newTestThrottleState(o)
	key := testKey()

	drained := 0
	for admitOK(state, key) {
		drained++
	}
	require.Equal(t, 100, drained)

	timeSource.Update(timeSource.Now().Add(testThrottleWindow))
	refilled := 0
	for admitOK(state, key) {
		refilled++
	}

	require.InEpsilon(t, 110.0, throttleRate(state, key), 1e-9)
	require.Equal(t, 100, refilled)
}

func TestThrottleState_IndependentInstancesConvergeOnSharedBudget(t *testing.T) {
	const (
		hostCount    = 8
		sharedBudget = 200
		windowCount  = 120
		warmup       = 20
	)

	states := make([]*ThrottleState, hostCount)
	clocks := make([]*clock.EventTimeSource, hostCount)
	for i := range states {
		o := defaultThrottleOverrides()
		o.initialRate = sharedBudget / hostCount
		o.maxRate = sharedBudget
		states[i], clocks[i] = newTestThrottleState(o)
	}

	key := apsKey("ns-1")
	now := clocks[0].Now()
	offered := [hostCount]int{60, 50, 45, 40, 35, 30, 25, 20}
	previous := make([]float64, hostCount)
	sawIncrease := make([]bool, hostCount)
	sawDecrease := make([]bool, hostCount)
	tail := make([]float64, 0, windowCount-warmup)
	minAggregate := math.Inf(1)

	for window := range windowCount {
		now = now.Add(testThrottleWindow)
		for _, timeSource := range clocks {
			timeSource.Update(now)
		}

		permits := make([][]*throttleEntry, hostCount)
		total := 0
		for i, state := range states {
			for range offered[i] {
				allowed, permit, _ := state.Admit(key)
				if !allowed {
					continue
				}
				state.Finish(permit, true)
				permits[i] = append(permits[i], permit)
				total++
			}
		}

		if total > sharedBudget {
			overflow := total - sharedBudget
			for i, state := range states {
				rejected := int(math.Round(float64(overflow*len(permits[i])) / float64(total)))
				for _, permit := range permits[i][:rejected] {
					state.ReportThrottled(key, permit)
				}
			}
		}

		aggregate := 0.0
		for i, state := range states {
			rate := throttleRate(state, key)
			aggregate += rate
			if previous[i] > 0 {
				sawIncrease[i] = sawIncrease[i] || rate > previous[i]
				sawDecrease[i] = sawDecrease[i] || rate < previous[i]
			}
			previous[i] = rate
		}
		if window >= warmup {
			tail = append(tail, aggregate)
			minAggregate = min(minAggregate, aggregate)
		}
	}

	sum := 0.0
	for _, aggregate := range tail {
		sum += aggregate
	}
	mean := sum / float64(len(tail))
	require.Greater(t, mean, sharedBudget*0.4)
	require.Less(t, mean, sharedBudget*2.0)
	require.Greater(t, minAggregate, sharedBudget*0.1)
	for i, state := range states {
		require.True(t, sawIncrease[i], "host %d never increased", i)
		require.True(t, sawDecrease[i], "host %d never decreased", i)
		require.Greater(t, throttleRate(state, key), defaultThrottleMinRate,
			"host %d collapsed to the minimum rate", i)
	}
}

func TestThrottleState_IdleKeyRestartsAtInitialRate(t *testing.T) {
	o := defaultThrottleOverrides()
	o.keyTTL = time.Minute
	state, timeSource := newTestThrottleState(o)
	key := testKey()

	for range 40 {
		reportThrottle(state, key, true)
		closeWindow(state, timeSource, key)
	}
	require.InEpsilon(t, o.minRate, throttleRate(state, key), 1e-9)

	timeSource.Update(timeSource.Now().Add(2 * o.keyTTL))
	require.True(t, admitOK(state, key))
	require.InEpsilon(t, o.initialRate, throttleRate(state, key), 1e-9)
}

func TestThrottleState_BackwardClockDoesNotResetActiveKey(t *testing.T) {
	o := defaultThrottleOverrides()
	o.initialRate = 5
	o.keyTTL = 5 * time.Minute
	state, timeSource := newTestThrottleState(o)
	key := testKey()
	start := timeSource.Now()

	for i := range 6 {
		timeSource.Update(start.Add(time.Duration(i+1) * time.Second))
		reportThrottle(state, key, true)
		admitOK(state, key)
	}
	driven := throttleRate(state, key)
	require.Less(t, driven, o.initialRate)

	timeSource.Update(start.Add(-10 * time.Minute))
	admitOK(state, key)
	timeSource.Update(start.Add(10 * time.Second))
	admitOK(state, key)
	require.Less(t, throttleRate(state, key), o.initialRate)
}

func TestThrottleState_InvalidLiveConfigUsesDefaults(t *testing.T) {
	state, timeSource := newTestThrottleState(defaultThrottleOverrides())
	state.options.Beta = func() float64 { return math.NaN() }
	state.options.IncreaseRatio = func() float64 { return -1 }
	state.options.LossThreshold = func() float64 { return 2 }
	state.options.Window = func() time.Duration { return 0 }
	state.options.MaxKeys = func() int { return 0 }
	key := testKey()

	reportThrottle(state, key, true)
	closeWindow(state, timeSource, key)
	require.InEpsilon(t, 85.0, throttleRate(state, key), 1e-9)
	require.Equal(t, defaultThrottleWindow, state.Window())
	require.Equal(t, defaultThrottleMaxKeys, state.maxKeys())
}

func TestThrottleState_LossThresholdOfOneStillDecreases(t *testing.T) {
	state, timeSource := newTestThrottleState(defaultThrottleOverrides())
	state.options.LossThreshold = func() float64 { return 1 }
	key := testKey()

	reportThrottle(state, key, true)
	closeWindow(state, timeSource, key)
	require.InEpsilon(t, 85.0, throttleRate(state, key), 1e-9)
}

func TestThrottleState_ThrottledWindowDoesNotIncrease(t *testing.T) {
	state, timeSource := newTestThrottleState(defaultThrottleOverrides())
	key := testKey()
	allowed, permit, _ := state.Admit(key)
	require.True(t, allowed)
	state.Finish(permit, true)
	state.ReportThrottled(key, permit)
	closeWindow(state, timeSource, key)

	require.InEpsilon(t, 85.0, throttleRate(state, key), 1e-9)
}

func TestThrottleEntry_NonPositiveRateHasNoTokenETA(t *testing.T) {
	for _, rate := range []float64{0, -1, math.SmallestNonzeroFloat64} {
		entry := throttleEntry{rate: rate}
		require.Zero(t, entry.tokenETALocked())
	}
}

func TestThrottleState_ReportSuccessClosesCleanWindow(t *testing.T) {
	state, timeSource := newTestThrottleState(defaultThrottleOverrides())
	key := testKey()
	require.True(t, admitOK(state, key))
	timeSource.Update(timeSource.Now().Add(testThrottleWindow))

	state.ReportSuccess(key)
	require.InEpsilon(t, 110.0, throttleRate(state, key), 1e-9)
}
