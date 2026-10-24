package queues

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics"
	"go.uber.org/mock/gomock"
)

func TestThrottleState_FailedSubmitAfterWindowDoesNotIncreaseRate(t *testing.T) {
	o := defaultThrottleOverrides()
	o.initialRate = 1
	state, timeSource := newTestThrottleState(o)
	key := testKey()

	allowed, _ := state.Admit(key)
	require.True(t, allowed)
	timeSource.Update(timeSource.Now().Add(testThrottleWindow))
	state.Return(key)

	require.InEpsilon(t, 1.0, throttleRate(state, key), 1e-9)
	require.True(t, admitOK(state, key), "the unused token must be returned")
}

func TestThrottleState_SuccessfulSubmitCommitsRelease(t *testing.T) {
	state, _ := newTestThrottleState(defaultThrottleOverrides())
	key := testKey()
	allowed, metered := state.Admit(key)
	require.True(t, allowed)
	require.True(t, metered, "the gate tracked this release")

	releases, _ := throttleCounters(state, key)
	require.Equal(t, int64(1), releases)
}

func TestThrottleState_FailedSubmitRefundsWithoutRelease(t *testing.T) {
	o := defaultThrottleOverrides()
	o.initialRate = 1
	state, _ := newTestThrottleState(o)
	key := testKey()
	allowed, _ := state.Admit(key)
	require.True(t, allowed)

	state.Return(key)

	releases, _ := throttleCounters(state, key)
	require.Zero(t, releases, "a submit that never happened is not a release")
	require.True(t, admitOK(state, key), "and its token is back")
}

func TestThrottleState_RejectionAfterEvictionChargesTheCurrentEntry(t *testing.T) {
	o := defaultThrottleOverrides()
	o.keyTTL = time.Second
	state, timeSource := newTestThrottleState(o)
	key := testKey()
	allowed, metered := state.Admit(key)
	require.True(t, allowed)

	timeSource.Update(timeSource.Now().Add(2 * o.keyTTL))
	state.getOrCreate(apsKey("other"))
	require.Nil(t, state.peek(key))

	state.ReportThrottled(key, metered)
	current := state.peek(key)
	require.NotNil(t, current)
	current.Lock()
	defer current.Unlock()
	require.Equal(t, int64(1), current.rejections)
}

func TestThrottleState_FailOpenAdmitHasNoPermit(t *testing.T) {
	o := defaultThrottleOverrides()
	o.maxKeys = 1
	state, _ := newTestThrottleState(o)

	require.True(t, admitOK(state, apsKey("tracked")))
	allowed, metered := state.Admit(apsKey("overflow"))
	require.True(t, allowed, "past the cap the gate fails open")
	require.False(t, metered, "but it tracked nothing, so a rejection is not evidence")
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

		admitted := make([]int, hostCount)
		total := 0
		for i, state := range states {
			for range offered[i] {
				allowed, _ := state.Admit(key)
				if !allowed {
					continue
				}
				admitted[i]++
				total++
			}
		}

		if total > sharedBudget {
			overflow := total - sharedBudget
			for i, state := range states {
				rejected := int(math.Round(float64(overflow*admitted[i]) / float64(total)))
				for range rejected {
					state.ReportThrottled(key, true)
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
		require.Greater(t, throttleRate(state, key), state.settings().MinRate,
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

// A class is only asking for a higher rate when the gate refuses it. Raising the rate of a
// class that never ran out of tokens would climb to the ceiling on clean windows alone, and the
// burst that buys is what the class dumps the moment its demand returns.
func TestThrottleState_DemandBelowTheRateDoesNotRaiseIt(t *testing.T) {
	state, timeSource := newTestThrottleState(defaultThrottleOverrides())
	key := testKey()

	for range 40 {
		for range 5 {
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
	for range 40 {
		for range 200 { // demand above the rate, so the class is asking for more
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

// Loss rises with the class's own rate, which is the feedback the law needs: it settles just
// above the share other traffic leaves it.
func TestThrottleState_ConvergesOnTheShareLeftByOtherTraffic(t *testing.T) {
	const budget = 200.0

	for _, other := range []float64{0, 100, 150, 190} {
		sustainable := budget - other
		o := defaultThrottleOverrides()
		o.initialRate = 1000
		state, timeSource := newTestThrottleState(o)
		key := testKey()

		for range 400 {
			admitted := 0
			for {
				allowed, _ := state.Admit(key)
				if !allowed {
					break
				}
				admitted++
			}
			if aggregate := other + float64(admitted); aggregate > budget && admitted > 0 {
				// The enforcer refuses the overflow; this class owns its share of it.
				rejected := int((aggregate - budget) * float64(admitted) / aggregate)
				for range rejected {
					state.ReportThrottled(key, true)
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

// Carrying the demand signal through an idle reset buys an increase on demand already spent.
func TestThrottleState_IdleResetClearsTheDemandSignal(t *testing.T) {
	o := defaultThrottleOverrides()
	o.keyTTL = time.Minute
	state, timeSource := newTestThrottleState(o)
	key := testKey()

	cleanWindow(state, key) // drains the bucket, so the gate refuses and demand is recorded
	entry := state.peek(key)
	entry.Lock()
	require.Positive(t, entry.suppressions, "the drain must have recorded demand")
	entry.Unlock()

	timeSource.Update(timeSource.Now().Add(2 * o.keyTTL))
	require.True(t, admitOK(state, key), "the idle class is reset on its next touch")

	entry.Lock()
	suppressions := entry.suppressions
	entry.Unlock()
	require.Zero(t, suppressions, "demand from before the reset must not survive it")
}

// A release committed while on keeps its rejection after the flag goes off, or the window
// reads clean while every release was failing.
func TestThrottleState_RejectionSurvivesTheFlagGoingOff(t *testing.T) {
	enabled := true
	timeSource := clock.NewEventTimeSource()
	timeSource.Update(time.Unix(0, 0))
	state := NewThrottleState(
		func() dynamicconfig.TaskThrottleControllerSettings {
			return dynamicconfig.TaskThrottleControllerSettings{
				Enabled:       enabled,
				Beta:          0.85,
				IncreaseRatio: 0.10,
				LossThreshold: 0.05,
				Window:        testThrottleWindow,
				MaxKeys:       1024,
				MinRate:       1,
				MaxRate:       10000,
				InitialRate:   100,
				KeyTTL:        5 * time.Minute,
			}
		},
		timeSource,
		metrics.NoopMetricsHandler,
	)
	key := testKey()

	lossThreshold := state.settings().LossThreshold
	samples := minDecisionReleases(lossThreshold)
	for range samples {
		allowed, _ := state.Admit(key)
		require.True(t, allowed)
	}

	enabled = false
	for range samples {
		state.ReportThrottled(key, true)
	}
	enabled = true
	closeWindow(state, timeSource, key)

	require.InEpsilon(t, 85.0, throttleRate(state, key), 1e-9,
		"every release failed, so the window must not read clean")
}

// The threshold is the loss the class may run at, so meeting it is not grounds to back off.
func TestThrottleState_LossExactlyAtTheThresholdDoesNotDecrease(t *testing.T) {
	o := defaultThrottleOverrides()
	state, timeSource := newTestThrottleState(o)
	key := testKey()

	// 1 rejection in 20 releases is exactly the 5% threshold.
	lossThreshold := state.settings().LossThreshold
	samples := minDecisionReleases(lossThreshold)
	for range samples {
		require.True(t, admitOK(state, key))
	}
	state.ReportThrottled(key, true)
	closeWindow(state, timeSource, key)

	require.GreaterOrEqual(t, throttleRate(state, key), o.initialRate,
		"loss at the threshold is the budget the class is allowed, not a reason to back off")
}

// Loss is 1 - budget/(other + rate), so once competing traffic alone exceeds
// budget/(1 - threshold) it stays above the threshold at every rate, including the floor.
// Whether pinning there is the right answer is a design question; the boundary is arithmetic.
func TestThrottleState_CompetingTrafficAboveTheBudgetPinsTheClassAtTheFloor(t *testing.T) {
	const budget = 200.0

	settle := func(other float64) float64 {
		o := defaultThrottleOverrides()
		o.initialRate = budget
		state, timeSource := newTestThrottleState(o)
		key := testKey()

		for range 400 {
			admitted := 0
			for {
				allowed, _ := state.Admit(key)
				if !allowed {
					break
				}
				admitted++
			}
			if total := other + float64(admitted); total > budget && admitted > 0 {
				rejected := int((total - budget) * float64(admitted) / total)
				for range rejected {
					state.ReportThrottled(key, true)
				}
			}
			timeSource.Update(timeSource.Now().Add(testThrottleWindow))
		}
		return throttleRate(state, key)
	}

	o := defaultThrottleOverrides()
	boundary := budget / (1 - o.lossThresh)

	below := settle(boundary * 0.75)
	require.Greater(t, below, budget*0.1,
		"below the boundary the class still claims the share left to it")

	above := settle(boundary * 2)
	require.Less(t, above, o.minRate*4,
		"above it the class sits at the floor, whatever rate it started from")
	require.Less(t, above, below/10,
		"the two regimes are not close; the boundary is a cliff, not a slope")
}

// The starting rate goes through the same clamp, or an initial rate above the ceiling hands
// the class a full window's burst at it.
func TestThrottleState_InitialRateIsClamped(t *testing.T) {
	o := defaultThrottleOverrides()
	o.maxRate = 100
	o.initialRate = 50000
	state, _ := newTestThrottleState(o)
	key := testKey()

	require.True(t, admitOK(state, key))
	require.LessOrEqual(t, throttleRate(state, key), o.maxRate,
		"a class may not start above the ceiling")

	entry := state.peek(key)
	entry.Lock()
	defer entry.Unlock()
	require.LessOrEqual(t, entry.tokens, o.maxRate,
		"nor hold a burst larger than the ceiling allows")
}

// A release refused by anything but the governed budgets must not change the rate. Counting
// it inflates the ratio by 1/(1 - contention) and drives a healthy class to the floor. The
// contended dispatches go through the real classification path, so deleting the cause filter
// in reportThrottle fails this.
func TestThrottleState_FailuresOutsideTheBudgetDoNotSlowTheClass(t *testing.T) {
	settle := func(t *testing.T, contention int) float64 {
		ctrl := gomock.NewController(t)
		o := defaultThrottleOverrides()
		o.initialRate = 200
		state, timeSource := newTestThrottleState(o)
		key := testKey()
		locked := newThrottleTestExecutable(ctrl, state)

		issued := 0
		for range 120 {
			for {
				allowed, metered := state.Admit(key)
				if !allowed {
					break
				}
				issued++
				switch {
				case issued%50 == 0:
					state.ReportThrottled(key, metered) // 2% budget loss, under the threshold
				case issued%100 < contention:
					// Failed on a workflow lock, holding a release this class issued.
					locked.throttleKey, locked.throttleAdmitted = key, metered
					locked.reportThrottle(
						enumspb.RESOURCE_EXHAUSTED_CAUSE_BUSY_WORKFLOW,
						enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE,
					)
				default:
					// The release was spent without a rejection.
				}
			}
			closeWindow(state, timeSource, key)
		}
		return throttleRate(state, key)
	}

	quiet := settle(t, 0)
	for _, contention := range []int{50, 80, 95} {
		require.InEpsilon(t, quiet, settle(t, contention), 1e-9,
			"%d%% lock contention must not change the rate; only the budget decides it", contention)
	}
}

// A zero floor would let a class decay until it can no longer earn the releases a decision
// needs, stalling it for good while the rescheduler keeps polling the gate.
func TestThrottleState_ZeroFloorCannotStallAClass(t *testing.T) {
	o := defaultThrottleOverrides()
	o.minRate = 0
	o.initialRate = 2
	state, timeSource := newTestThrottleState(o)
	key := testKey()

	// Far past the point a 0.85 decay would otherwise reach zero.
	for range 200 {
		reportThrottle(state, key, true)
		closeWindow(state, timeSource, key)
	}

	rate := throttleRate(state, key)
	require.GreaterOrEqual(t, rate, minThrottleRate,
		"a configured floor below one release per second must not be honoured")
	require.True(t, admitOK(state, key), "a floored class must still release")
}
