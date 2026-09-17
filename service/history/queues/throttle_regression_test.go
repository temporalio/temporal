package queues

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/clock"
	ctasks "go.temporal.io/server/common/tasks"
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

	require.InEpsilon(t, 1.0, throttleRate(state, key), 1e-9)
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
	require.InEpsilon(t, 1.0, permit.tokens, 1e-9)
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
	_, _, lossThreshold := state.controlLaw()
	for i := int64(0); i < minDecisionReleases(lossThreshold); i++ {
		allowed, permit, _ := state.Admit(key)
		require.True(t, allowed)
		state.Finish(permit, true)
		state.ReportThrottled(key, permit)
	}
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
	cleanWindow(state, key)
	timeSource.Update(timeSource.Now().Add(testThrottleWindow))

	state.ReportSuccess(key)
	require.InEpsilon(t, 110.0, throttleRate(state, key), 1e-9)
}

// A class is only asking for a higher rate when the gate refuses it. Raising the rate of a
// class that never ran out of tokens would climb to the ceiling on clean windows alone, and the
// burst that buys is what the class dumps the moment its demand returns.
func TestThrottleState_DemandBelowTheRateDoesNotRaiseIt(t *testing.T) {
	state, timeSource := newTestThrottleState(defaultThrottleOverrides())
	key := testKey()

	for w := 0; w < 40; w++ {
		for i := 0; i < 5; i++ {
			require.True(t, admitOK(state, key))
		}
		closeWindow(state, timeSource, key)
	}
	require.InEpsilon(t, defaultThrottleOverrides().initialRate, throttleRate(state, key), 1e-9,
		"a class well under its rate has shown no demand for more")
}

// A loss ratio cannot resolve a 5% threshold from ten releases: the smallest non-zero ratio it
// can express is already 10%, so a class whose true loss is under the threshold would be cut
// every window that happened to contain a rejection, and would drift below a rate it could
// sustain. Evidence carries across windows until the ratio means something.
func TestThrottleState_LowRateClassIsNotCutByAnUnresolvableRatio(t *testing.T) {
	// 4% loss, under the 5% threshold. At a rate of 15 a window holds too few releases for the
	// ratio to say so: one rejection reads as 6.7%, and most windows contain one, so deciding
	// per window drives the class below a rate it was sustaining.
	const rejectEveryNth = 25

	o := defaultThrottleOverrides()
	o.initialRate = 15
	state, timeSource := newTestThrottleState(o)
	key := testKey()

	admitted := 0
	for w := 0; w < 40; w++ {
		for i := 0; i < 200; i++ { // demand above the rate, so the class is asking for more
			admit := admitOK
			if (admitted+1)%rejectEveryNth == 0 {
				admit = admitAndReject
			}
			if !admit(state, key) {
				continue
			}
			admitted++
		}
		closeWindow(state, timeSource, key)
	}
	require.Greater(t, throttleRate(state, key), o.initialRate,
		"a class losing 4% against a 5% threshold must not be cut at any rate")
}

// The control loop asks whether the releases this class issued are getting through. One that
// failed under a different budget did not get through, so it belongs to the class that issued
// it; charging the cause the error reported would leave the issuing class reading perfectly
// clean while every one of its releases was refused.
func TestThrottleState_RejectionUnderAnotherCauseChargesTheIssuingClass(t *testing.T) {
	state, _ := newTestThrottleState(defaultThrottleOverrides())
	issuing := NewThrottleKey(enumspb.RESOURCE_EXHAUSTED_CAUSE_APS_LIMIT, "ns-1", ctasks.PriorityHigh)
	other := NewThrottleKey(enumspb.RESOURCE_EXHAUSTED_CAUSE_PERSISTENCE_LIMIT, "ns-1", ctasks.PriorityHigh)

	allowed, permit, _ := state.Admit(issuing)
	require.True(t, allowed)
	state.Finish(permit, true)
	state.ReportThrottled(other, permit)

	releases, rejections := throttleCounters(state, issuing)
	require.Equal(t, int64(1), releases)
	require.Equal(t, int64(1), rejections, "the class that issued the release must see the loss")

	_, otherRejections := throttleCounters(state, other)
	require.Zero(t, otherRejections, "the reported cause issued nothing, so it learns nothing")
}

// Priority is part of the key so that each priority paces itself. Sharing one bucket across
// priorities let a high priority backlog hold every token while the rescheduler, which offers
// the budget in strict priority order, never reached the lower priority class at all.
func TestThrottleKey_PriorityIsItsOwnClass(t *testing.T) {
	state, _ := newTestThrottleState(defaultThrottleOverrides())
	high := NewThrottleKey(enumspb.RESOURCE_EXHAUSTED_CAUSE_APS_LIMIT, "ns-1", ctasks.PriorityHigh)
	preemptable := NewThrottleKey(enumspb.RESOURCE_EXHAUSTED_CAUSE_APS_LIMIT, "ns-1", ctasks.PriorityPreemptable)

	cleanWindow(state, high)
	require.False(t, admitOK(state, high), "high priority has spent its whole bucket")
	require.True(t, admitOK(state, preemptable), "a lower priority class holds its own tokens")
}

// A namespace token bucket refuses what exceeds the budget, so the loss this class sees rises
// with its own rate. That feedback is what the control law needs: it settles just above the
// share other traffic leaves it, rather than at the floor or the ceiling.
func TestThrottleState_ConvergesOnTheShareLeftByOtherTraffic(t *testing.T) {
	const budget = 200.0

	for _, other := range []float64{0, 100, 150, 190} {
		sustainable := budget - other
		o := defaultThrottleOverrides()
		o.initialRate = 1000
		state, timeSource := newTestThrottleState(o)
		key := testKey()

		for w := 0; w < 400; w++ {
			permits := make([]*throttleEntry, 0, 512)
			for {
				allowed, permit, _ := state.Admit(key)
				if !allowed {
					break
				}
				state.Finish(permit, true)
				permits = append(permits, permit)
			}
			if aggregate := other + float64(len(permits)); aggregate > budget && len(permits) > 0 {
				// The enforcer refuses the overflow; this class owns its share of it.
				rejected := int((aggregate - budget) * float64(len(permits)) / aggregate)
				for i := 0; i < rejected && i < len(permits); i++ {
					state.ReportThrottled(key, permits[i])
				}
			}
			timeSource.Update(timeSource.Now().Add(testThrottleWindow))
		}

		settled := throttleRate(state, key)
		require.GreaterOrEqual(t, settled, sustainable,
			"the class must claim the share left to it; other=%v", other)
		require.Less(t, settled, budget*1.5,
			"the class must not run away above the budget; other=%v", other)
		require.Less(t, settled, o.maxRate,
			"a class under real back pressure must not reach the ceiling; other=%v", other)
	}
}

// A rejection that arrives after its class was evicted lands on a recreated entry that has
// issued nothing. "No releases at all" must not be read as "every release lost": a single
// in-flight task outliving its class's TTL would otherwise cut a class that never ran.
func TestThrottleState_UnmatchedRejectionDoesNotCutAFreshClass(t *testing.T) {
	o := defaultThrottleOverrides()
	o.keyTTL = time.Second
	state, timeSource := newTestThrottleState(o)
	key := testKey()

	allowed, permit, _ := state.Admit(key)
	require.True(t, allowed)
	state.Finish(permit, true)

	timeSource.Update(timeSource.Now().Add(2 * o.keyTTL))
	state.getOrCreate(apsKey("other"))
	require.Nil(t, state.peek(key), "the class must have been swept")

	state.ReportThrottled(key, permit)
	closeWindow(state, timeSource, key)

	require.InEpsilon(t, o.initialRate, throttleRate(state, key), 1e-9,
		"one rejection against no releases is not evidence of loss")
}
