package queues

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
)

func TestThrottleState_FailedSubmitAfterWindowDoesNotIncreaseRate(t *testing.T) {
	o := defaultThrottleOverrides()
	o.initialRate = 1
	state, timeSource := newTestThrottleState(o)
	key := testKey()

	allowed, _, _ := state.Admit(key)
	require.True(t, allowed)
	timeSource.Update(timeSource.Now().Add(testThrottleWindow))
	state.Return(key)

	require.InEpsilon(t, 1.0, throttleRate(state, key), 1e-9)
	require.True(t, admitOK(state, key), "the unused token must be returned")
}

func TestThrottleState_SuccessfulSubmitCommitsRelease(t *testing.T) {
	state, _ := newTestThrottleState(defaultThrottleOverrides())
	key := testKey()
	allowed, metered, _ := state.Admit(key)
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
	allowed, _, _ := state.Admit(key)
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
	allowed, metered, _ := state.Admit(key)
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

func TestThrottleState_DeniedAdmitReportsTokenETA(t *testing.T) {
	o := defaultThrottleOverrides()
	o.initialRate = 4
	state, _ := newTestThrottleState(o)
	key := testKey()

	for range 4 {
		allowed, _, retryAfter := state.Admit(key)
		require.True(t, allowed)
		require.Zero(t, retryAfter)
	}

	allowed, metered, retryAfter := state.Admit(key)
	require.False(t, allowed)
	require.False(t, metered)
	require.Equal(t, 250*time.Millisecond, retryAfter)
}

func TestThrottleState_FailOpenAdmitHasNoPermit(t *testing.T) {
	o := defaultThrottleOverrides()
	o.maxKeys = 1
	state, _ := newTestThrottleState(o)

	require.True(t, admitOK(state, apsKey("tracked")))
	allowed, metered, _ := state.Admit(apsKey("overflow"))
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
				allowed, _, _ := state.Admit(key)
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
		allowed, metered, _ := state.Admit(key)
		require.True(t, allowed)
		state.ReportThrottled(key, metered)
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
	issuing := NewThrottleKey(enumspb.RESOURCE_EXHAUSTED_CAUSE_APS_LIMIT, "ns-1")
	other := NewThrottleKey(enumspb.RESOURCE_EXHAUSTED_CAUSE_PERSISTENCE_LIMIT, "ns-1")

	allowed, metered, _ := state.Admit(issuing)
	require.True(t, allowed)
	// The executable charges the class that issued the release, not the cause reported.
	require.True(t, metered)
	state.ReportThrottled(issuing, true)

	releases, rejections := throttleCounters(state, issuing)
	require.Equal(t, int64(1), releases)
	require.Equal(t, int64(1), rejections, "the class that issued the release must see the loss")

	_, otherRejections := throttleCounters(state, other)
	require.Zero(t, otherRejections, "the reported cause issued nothing, so it learns nothing")
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
			admitted := 0
			for {
				allowed, _, _ := state.Admit(key)
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

// A rejection that arrives after its class was swept lands on a recreated entry that has
// issued nothing. With no releases to measure them against there is no ratio, so any number
// of them must leave the rate alone rather than score as total loss.
func TestThrottleState_UnmatchedRejectionsDoNotCutAClassThatIssuedNothing(t *testing.T) {
	o := defaultThrottleOverrides()
	state, timeSource := newTestThrottleState(o)
	key := testKey()

	entry := state.getOrCreate(key)
	_, _, lossThreshold := state.controlLaw()
	entry.Lock()
	entry.rejections = minDecisionReleases(lossThreshold) * 5
	entry.Unlock()

	closeWindow(state, timeSource, key)

	require.InEpsilon(t, o.initialRate, throttleRate(state, key), 1e-9,
		"rejections with no releases behind them are not evidence of loss")
}

// The demand signal is per decision, like the counters beside it. Carrying it through an idle
// reset buys the revived class an increase on demand it showed before it went quiet.
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

// The increase ratio is a fraction of the current rate. An unbounded one would reach the
// ceiling in a single window, which is a config mistake rather than an instruction.
func TestThrottleState_AbsurdIncreaseRatioFallsBackToTheDefault(t *testing.T) {
	o := defaultThrottleOverrides()
	o.increase = 1e6
	state, timeSource := newTestThrottleState(o)
	key := testKey()

	cleanWindow(state, key)
	closeWindow(state, timeSource, key)

	require.InEpsilon(t, o.initialRate*(1+defaultThrottleIncreaseRatio), throttleRate(state, key), 1e-9)
}

// The load-bearing invariant of the design: loss on traffic the gate never sent must not move
// the rate. Without it a busy namespace's parked tasks are driven to the floor by rejections
// belonging to the traffic actually consuming the budget. The rejections here outnumber the
// releases five to one, so counting them at all is unmissable.
func TestThrottleState_UnadmittedRejectionsCannotDriveADecision(t *testing.T) {
	o := defaultThrottleOverrides()
	state, timeSource := newTestThrottleState(o)
	key := testKey()

	_, _, lossThreshold := state.controlLaw()
	samples := minDecisionReleases(lossThreshold)
	for i := int64(0); i < samples; i++ {
		require.True(t, admitOK(state, key))
	}
	for i := int64(0); i < samples*5; i++ {
		state.ReportThrottled(key, false)
	}
	closeWindow(state, timeSource, key)

	require.InEpsilon(t, o.initialRate, throttleRate(state, key), 1e-9,
		"rejections the gate did not issue are not evidence about its own releases")
}

// A release committed while the controller was on has to be matched even if the flag goes off
// before its rejection arrives. Otherwise the window it belongs to reads clean, and a class
// whose releases were all failing raises its rate while an operator is mid-toggle.
func TestThrottleState_RejectionSurvivesTheFlagGoingOff(t *testing.T) {
	enabled := true
	timeSource := clock.NewEventTimeSource()
	timeSource.Update(time.Unix(0, 0))
	state := NewThrottleState(
		ThrottleStateOptions{
			Enabled:       func() bool { return enabled },
			Beta:          dynamicconfig.GetFloatPropertyFn(0.85),
			IncreaseRatio: dynamicconfig.GetFloatPropertyFn(0.10),
			LossThreshold: dynamicconfig.GetFloatPropertyFn(0.05),
			Window:        dynamicconfig.GetDurationPropertyFn(testThrottleWindow),
			MaxKeys:       dynamicconfig.GetIntPropertyFn(1024),
			MinRate:       dynamicconfig.GetFloatPropertyFn(1),
			MaxRate:       dynamicconfig.GetFloatPropertyFn(10000),
			InitialRate:   dynamicconfig.GetFloatPropertyFn(100),
			KeyTTL:        dynamicconfig.GetDurationPropertyFn(5 * time.Minute),
		},
		timeSource,
		log.NewTestLogger(),
		metrics.NoopMetricsHandler,
	)
	key := testKey()

	_, _, lossThreshold := state.controlLaw()
	samples := minDecisionReleases(lossThreshold)
	for i := int64(0); i < samples; i++ {
		allowed, _, _ := state.Admit(key)
		require.True(t, allowed)
	}

	enabled = false
	for i := int64(0); i < samples; i++ {
		state.ReportThrottled(key, true)
	}
	enabled = true
	closeWindow(state, timeSource, key)

	require.InEpsilon(t, 85.0, throttleRate(state, key), 1e-9,
		"every release failed, so the window must not read clean")
}

// A threshold of zero makes any single rejection a decrease and demands a perfectly clean
// window for an increase. That is the rule the design argues against, reached from below;
// commit 073471ef1 closed the same hole at the top of the range.
func TestThrottleState_LossThresholdOfZeroFallsBackToTheDefault(t *testing.T) {
	o := defaultThrottleOverrides()
	o.lossThresh = 0
	state, timeSource := newTestThrottleState(o)
	key := testKey()

	require.True(t, admitOK(state, key))
	state.ReportThrottled(key, true)
	closeWindow(state, timeSource, key)

	require.InEpsilon(t, o.initialRate, throttleRate(state, key), 1e-9,
		"one rejection out of one release must not decide anything at the default threshold")
}

// Loss exactly at the threshold is tolerated, not punished: the threshold is the amount of
// loss the class is allowed to run at, so meeting it is not grounds for backing off.
func TestThrottleState_LossExactlyAtTheThresholdDoesNotDecrease(t *testing.T) {
	o := defaultThrottleOverrides()
	state, timeSource := newTestThrottleState(o)
	key := testKey()

	// 1 rejection in 20 releases is exactly the 5% threshold.
	_, _, lossThreshold := state.controlLaw()
	samples := minDecisionReleases(lossThreshold)
	for i := int64(0); i < samples; i++ {
		require.True(t, admitOK(state, key))
	}
	state.ReportThrottled(key, true)
	closeWindow(state, timeSource, key)

	require.GreaterOrEqual(t, throttleRate(state, key), o.initialRate,
		"loss at the threshold is the budget the class is allowed, not a reason to back off")
}

// The burst floor is the difference between a slow class and a wedged one: below one token a
// window the bucket can never reach the whole token an admit needs, and the class stops
// releasing entirely however long it waits.
func TestThrottleEntry_BurstNeverFallsBelowOneToken(t *testing.T) {
	o := defaultThrottleOverrides()
	o.minRate = 0.01
	o.initialRate = 0.01
	state, timeSource := newTestThrottleState(o)
	key := testKey()

	require.True(t, admitOK(state, key), "a class below one release a window still releases")
	require.False(t, admitOK(state, key), "and then has to earn the next token")

	// The floor caps the burst, not the refill: at 0.01/s a whole token takes 100 windows.
	timeSource.Update(timeSource.Now().Add(100 * testThrottleWindow))
	require.True(t, admitOK(state, key), "which it can still reach, because a burst is never under one")
}

// Loss is 1 - budget/(other + rate), so once competing traffic alone exceeds
// budget/(1 - threshold) the loss is above the threshold at every rate the class can reach,
// including the floor. It decreases every decision and pins there, reacting to a signal it
// cannot influence. Whether backing off or holding a share is the right answer when a
// namespace is genuinely over budget is a design question, but the boundary is arithmetic and
// must not move by accident.
func TestThrottleState_CompetingTrafficAboveTheBudgetPinsTheClassAtTheFloor(t *testing.T) {
	const budget = 200.0

	settle := func(other float64) float64 {
		o := defaultThrottleOverrides()
		o.initialRate = budget
		state, timeSource := newTestThrottleState(o)
		key := testKey()

		for w := 0; w < 400; w++ {
			admitted := 0
			for {
				allowed, _, _ := state.Admit(key)
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

// The rate a class starts at goes through the same clamp as every rate the control law
// produces. Without that, an initial rate above the ceiling hands the class a burst of one
// window at that rate the first time it is touched.
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

// The ceiling and the idle TTL are live, like the floor and the initial rate. Two rounds of
// review pointed at these knobs as the ones an operator reaches for mid-incident.
func TestThrottleState_CeilingIsLive(t *testing.T) {
	ceiling := 10000.0
	timeSource := clock.NewEventTimeSource()
	timeSource.Update(time.Unix(0, 0))
	state := NewThrottleState(
		ThrottleStateOptions{
			Enabled:       dynamicconfig.GetBoolPropertyFn(true),
			Beta:          dynamicconfig.GetFloatPropertyFn(0.85),
			IncreaseRatio: dynamicconfig.GetFloatPropertyFn(0.10),
			LossThreshold: dynamicconfig.GetFloatPropertyFn(0.05),
			Window:        dynamicconfig.GetDurationPropertyFn(testThrottleWindow),
			MaxKeys:       dynamicconfig.GetIntPropertyFn(1024),
			MinRate:       dynamicconfig.GetFloatPropertyFn(1),
			MaxRate:       func() float64 { return ceiling },
			InitialRate:   dynamicconfig.GetFloatPropertyFn(100),
			KeyTTL:        dynamicconfig.GetDurationPropertyFn(5 * time.Minute),
		},
		timeSource,
		log.NewTestLogger(),
		metrics.NoopMetricsHandler,
	)
	key := testKey()

	for i := 0; i < 60; i++ {
		cleanWindow(state, key)
		closeWindow(state, timeSource, key)
	}
	require.Greater(t, throttleRate(state, key), 200.0, "the class must have climbed")

	ceiling = 150
	cleanWindow(state, key)
	closeWindow(state, timeSource, key)
	require.InEpsilon(t, 150.0, throttleRate(state, key), 1e-9,
		"lowering the ceiling must pull a class already above it back down")
}

// Only the namespace APS and persistence budgets are evidence. A release refused by anything
// else - a contended workflow lock above all - says nothing about the budget this class paces,
// so it must not change how fast the class is allowed to go. Counting such a failure as loss
// inflates the ratio by 1/(1 - contention) and drives a healthy class toward the floor.
func TestThrottleState_FailuresOutsideTheBudgetDoNotSlowTheClass(t *testing.T) {
	settle := func(contention int) float64 {
		o := defaultThrottleOverrides()
		o.initialRate = 200
		state, timeSource := newTestThrottleState(o)
		key := testKey()

		issued := 0
		for w := 0; w < 120; w++ {
			for {
				allowed, metered, _ := state.Admit(key)
				if !allowed {
					break
				}
				issued++
				if issued%50 == 0 {
					state.ReportThrottled(key, metered) // 2% budget loss, under the threshold
				}
				// The other issued%100 < contention dispatches fail on a workflow lock. The
				// controller is told nothing about them, which is the whole point.
			}
			closeWindow(state, timeSource, key)
		}
		return throttleRate(state, key)
	}

	quiet := settle(0)
	for _, contention := range []int{50, 80, 95} {
		require.InEpsilon(t, quiet, settle(contention), 1e-9,
			"%d%% lock contention must not change the rate; only the budget decides it", contention)
	}
}
