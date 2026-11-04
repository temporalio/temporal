package queues

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
)

const testThrottleWindow = time.Second

type throttleTestOverrides struct {
	enabled     bool
	beta        float64
	increase    float64
	lossThresh  float64
	minRate     float64
	maxRate     float64
	initialRate float64
	maxKeys     int
	keyTTL      time.Duration
}

func defaultThrottleOverrides() throttleTestOverrides {
	return throttleTestOverrides{
		enabled:     true,
		beta:        0.85,
		increase:    0.10,
		lossThresh:  0.05,
		minRate:     1,
		maxRate:     10000,
		initialRate: 100,
		maxKeys:     1024,
		keyTTL:      5 * time.Minute,
	}
}

func newTestThrottleState(o throttleTestOverrides) (*ThrottleState, *clock.EventTimeSource) {
	return newTestThrottleStateWithWindow(o, testThrottleWindow)
}

func newTestThrottleStateWithWindow(
	o throttleTestOverrides,
	window time.Duration,
) (*ThrottleState, *clock.EventTimeSource) {
	timeSource := clock.NewEventTimeSource()
	timeSource.Update(time.Unix(0, 0))
	settings := dynamicconfig.TaskThrottleControllerSettings{
		Enabled:       o.enabled,
		Beta:          o.beta,
		IncreaseRatio: o.increase,
		LossThreshold: o.lossThresh,
		Window:        window,
		MinRate:       o.minRate,
		MaxRate:       o.maxRate,
		InitialRate:   o.initialRate,
		MaxKeys:       o.maxKeys,
		KeyTTL:        o.keyTTL,
	}
	state := NewThrottleState(
		func() dynamicconfig.TaskThrottleControllerSettings { return settings },
		timeSource,
		metrics.NoopMetricsHandler,
	)
	return state, timeSource
}

// minDecisionReleases is the release count a window needs before the control law will decide,
// mirroring the evidence gate in advanceWindowLocked.
func minDecisionReleases(lossThreshold float64) int64 {
	return int64(math.Ceil(1 / lossThreshold))
}

// overrideThrottleSetting re-applies mutate on every read, so a test can keep changing the
// value it closes over.
func overrideThrottleSetting(
	state *ThrottleState,
	mutate func(*dynamicconfig.TaskThrottleControllerSettings),
) {
	base := state.settings()
	state.settings = func() dynamicconfig.TaskThrottleControllerSettings {
		cfg := base
		mutate(&cfg)
		return cfg
	}
}

func TestIsControllerInput(t *testing.T) {
	ns := enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE
	system := enumspb.RESOURCE_EXHAUSTED_SCOPE_SYSTEM
	unspecified := enumspb.RESOURCE_EXHAUSTED_SCOPE_UNSPECIFIED

	for _, tc := range []struct {
		name  string
		cause enumspb.ResourceExhaustedCause
		scope enumspb.ResourceExhaustedScope
		want  bool
	}{
		{name: "aps at namespace scope", cause: enumspb.RESOURCE_EXHAUSTED_CAUSE_APS_LIMIT, scope: ns, want: true},
		{name: "persistence at namespace scope", cause: enumspb.RESOURCE_EXHAUSTED_CAUSE_PERSISTENCE_LIMIT, scope: ns, want: true},
		{name: "shard persistence at namespace scope", cause: enumspb.RESOURCE_EXHAUSTED_CAUSE_SHARD_PERSISTENCE_LIMIT, scope: ns, want: true},

		{name: "aps at system scope", cause: enumspb.RESOURCE_EXHAUSTED_CAUSE_APS_LIMIT, scope: system},
		{name: "persistence at system scope", cause: enumspb.RESOURCE_EXHAUSTED_CAUSE_PERSISTENCE_LIMIT, scope: system},
		{name: "unspecified scope is not namespace", cause: enumspb.RESOURCE_EXHAUSTED_CAUSE_APS_LIMIT, scope: unspecified},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, IsControllerInput(tc.cause, tc.scope))
		})
	}
}

func TestNewThrottleKey_ShardIDOnlyForPerShardBudgets(t *testing.T) {
	require.Equal(t,
		ThrottleKey{Cause: enumspb.RESOURCE_EXHAUSTED_CAUSE_SHARD_PERSISTENCE_LIMIT, NamespaceName: "ns-1", ShardID: 7},
		NewThrottleKey(enumspb.RESOURCE_EXHAUSTED_CAUSE_SHARD_PERSISTENCE_LIMIT, "ns-1", 7),
	)
	require.Equal(t,
		ThrottleKey{Cause: enumspb.RESOURCE_EXHAUSTED_CAUSE_PERSISTENCE_LIMIT, NamespaceName: "ns-1"},
		NewThrottleKey(enumspb.RESOURCE_EXHAUSTED_CAUSE_PERSISTENCE_LIMIT, "ns-1", 7),
	)
	require.Equal(t,
		ThrottleKey{Cause: enumspb.RESOURCE_EXHAUSTED_CAUSE_APS_LIMIT, NamespaceName: "ns-1"},
		NewThrottleKey(enumspb.RESOURCE_EXHAUSTED_CAUSE_APS_LIMIT, "ns-1", 7),
	)
}

func testKey() ThrottleKey {
	return NewThrottleKey(enumspb.RESOURCE_EXHAUSTED_CAUSE_APS_LIMIT, "ns-1", 0)
}

func admitOK(c *ThrottleState, key ThrottleKey) bool {
	allowed, _ := c.Admit(key)
	return allowed
}

// One decision's worth of total loss. Plants the sample rather than admitting, so tests about
// the decision keep their bucket; the admission path is covered by admitOK and admitAndReject.
func reportThrottle(c *ThrottleState, key ThrottleKey, admitted bool) {
	if !c.Enabled() || !admitted {
		c.ReportThrottled(key, false)
		return
	}
	entry := c.getOrCreate(key)
	if entry == nil {
		c.ReportThrottled(key, false)
		return
	}
	lossThreshold := c.settings().LossThreshold
	samples := minDecisionReleases(lossThreshold)
	entry.Lock()
	entry.releases += samples
	entry.rejections += samples - 1
	entry.Unlock()
	c.ReportThrottled(key, true)
}

// One metered release and its rejection: the pair the control law measures.
func admitAndReject(c *ThrottleState, key ThrottleKey) bool {
	allowed, metered := c.Admit(key)
	if !allowed {
		return false
	}
	c.ReportThrottled(key, metered)
	return true
}

// The open window's release and rejection counts.
func throttleCounters(state *ThrottleState, key ThrottleKey) (releases, rejections int64) {
	entry := state.peek(key)
	if entry == nil {
		return 0, 0
	}
	entry.Lock()
	defer entry.Unlock()
	return entry.releases, entry.rejections
}

// Drains the bucket so the gate refuses, which is the demand signal, with no rejections.
func cleanWindow(c *ThrottleState, key ThrottleKey) {
	for admitOK(c, key) { //nolint:revive // draining, body intentionally empty
	}
}

func closeWindow(state *ThrottleState, ts *clock.EventTimeSource, key ThrottleKey) {
	ts.Update(ts.Now().Add(testThrottleWindow))
	entry := state.peek(key)
	if entry == nil {
		return
	}
	entry.Lock()
	defer entry.Unlock()
	state.touchLocked(entry, ts.Now(), state.settings().Window)
	state.advanceWindowLocked(key, entry, ts.Now(), state.settings().Window)
}

func throttleRate(state *ThrottleState, key ThrottleKey) float64 {
	entry := state.peek(key)
	if entry == nil {
		return 0
	}
	entry.Lock()
	defer entry.Unlock()
	return entry.rate
}

func throttleLen(state *ThrottleState) int {
	state.mu.RLock()
	defer state.mu.RUnlock()
	return len(state.entries)
}

func TestThrottleState_OneDecreasePerWindow(t *testing.T) {
	state, timeSource := newTestThrottleState(defaultThrottleOverrides())
	key := testKey()

	for range 500 {
		reportThrottle(state, key, true)
		timeSource.Update(timeSource.Now().Add(time.Millisecond))
	}
	closeWindow(state, timeSource, key)

	require.InEpsilon(t, 85.0, throttleRate(state, key), 1e-9)
}

func TestThrottleState_ControlLawUpdatesAreLive(t *testing.T) {
	beta := 0.5
	state, timeSource := newTestThrottleState(defaultThrottleOverrides())
	overrideThrottleSetting(state, func(c *dynamicconfig.TaskThrottleControllerSettings) { c.Beta = beta })
	key := testKey()

	reportThrottle(state, key, true)
	closeWindow(state, timeSource, key)
	require.InEpsilon(t, 50.0, throttleRate(state, key), 1e-9)

	beta = 0.8
	reportThrottle(state, key, true)
	closeWindow(state, timeSource, key)
	require.InEpsilon(t, 40.0, throttleRate(state, key), 1e-9)
}

func TestThrottleState_IncreaseAfterCleanWindow(t *testing.T) {
	state, timeSource := newTestThrottleState(defaultThrottleOverrides())
	key := testKey()

	reportThrottle(state, key, true)
	closeWindow(state, timeSource, key)
	require.InEpsilon(t, 85.0, throttleRate(state, key), 1e-9)

	cleanWindow(state, key)
	closeWindow(state, timeSource, key)

	require.InEpsilon(t, 85.0*1.1, throttleRate(state, key), 1e-9)
}

func TestThrottleState_ClampsRate(t *testing.T) {
	overrides := defaultThrottleOverrides()
	overrides.minRate = 20
	overrides.maxRate = 120
	state, timeSource := newTestThrottleState(overrides)
	key := testKey()

	for range 100 {
		reportThrottle(state, key, true)
		closeWindow(state, timeSource, key)
	}
	require.InEpsilon(t, 20.0, throttleRate(state, key), 1e-9, "floor keeps the class making forward progress")

	for range 200 {
		cleanWindow(state, key)
		closeWindow(state, timeSource, key)
	}
	require.InEpsilon(t, 120.0, throttleRate(state, key), 1e-9, "ceiling bounds what a recovering class can climb to")
}

func TestThrottleState_AdmitEnforcesRate(t *testing.T) {
	overrides := defaultThrottleOverrides()
	overrides.initialRate = 10
	state, timeSource := newTestThrottleState(overrides)
	key := testKey()

	admitted := 0
	for range 100 {
		if admitOK(state, key) {
			admitted++
		}
	}
	require.Equal(t, 10, admitted, "burst is capped at one window of the current rate")

	require.False(t, admitOK(state, key))
	timeSource.Update(timeSource.Now().Add(200 * time.Millisecond))
	require.True(t, admitOK(state, key))
	require.True(t, admitOK(state, key))
	require.False(t, admitOK(state, key))
}

func TestThrottleState_DecreaseTrimsAccumulatedTokens(t *testing.T) {
	overrides := defaultThrottleOverrides()
	overrides.initialRate = 100
	state, timeSource := newTestThrottleState(overrides)
	key := testKey()

	require.True(t, admitOK(state, key))
	timeSource.Update(timeSource.Now().Add(10 * testThrottleWindow))
	reportThrottle(state, key, true)

	admitted := 0
	for range 1000 {
		if admitOK(state, key) {
			admitted++
		}
	}
	require.Equal(t, 85, admitted)
}

func TestThrottleState_DisabledAlwaysAdmits(t *testing.T) {
	overrides := defaultThrottleOverrides()
	overrides.enabled = false
	state, _ := newTestThrottleState(overrides)
	key := testKey()

	for range 100 {
		allowed, metered := state.Admit(key)
		require.True(t, allowed)
		require.False(t, metered)
		state.ReportThrottled(key, metered)
	}
	require.Zero(t, throttleLen(state), "a disabled controller must not accumulate state")
}

func TestThrottleState_FailsOpenPastKeyCap(t *testing.T) {
	overrides := defaultThrottleOverrides()
	overrides.maxKeys = 2
	overrides.initialRate = 1
	state, _ := newTestThrottleState(overrides)

	tracked := make([]ThrottleKey, 0, 2)
	for i := range 2 {
		key := testKey()
		key.NamespaceName = namespace.Name(string(rune('a' + i)))
		tracked = append(tracked, key)
		reportThrottle(state, key, true)
		require.True(t, admitOK(state, key))
		require.False(t, admitOK(state, key))
	}
	require.Equal(t, 2, throttleLen(state))

	overflow := testKey()
	overflow.NamespaceName = "overflow"
	reportThrottle(state, overflow, true)
	for range 100 {
		require.True(t, admitOK(state, overflow), "past the cap the real limiter stays the enforcement point")
	}
	require.Equal(t, 2, throttleLen(state))
	require.Zero(t, throttleRate(state, overflow))

	require.False(t, admitOK(state, tracked[0]))
}

func TestThrottleState_SweepsIdleKeys(t *testing.T) {
	overrides := defaultThrottleOverrides()
	overrides.keyTTL = time.Minute
	state, timeSource := newTestThrottleState(overrides)

	idle := testKey()
	idle.NamespaceName = "idle"
	reportThrottle(state, idle, true)
	require.Equal(t, 1, throttleLen(state))

	timeSource.Update(timeSource.Now().Add(2 * time.Minute))

	active := testKey()
	active.NamespaceName = "active"
	reportThrottle(state, active, true)

	require.Equal(t, 1, throttleLen(state))
	require.Zero(t, throttleRate(state, idle))
	require.Positive(t, throttleRate(state, active))
}

func TestThrottleState_ClassesAreIndependent(t *testing.T) {
	state, timeSource := newTestThrottleState(defaultThrottleOverrides())

	namespaceKey := testKey()
	otherCause := NewThrottleKey(enumspb.RESOURCE_EXHAUSTED_CAUSE_PERSISTENCE_LIMIT, "ns-1", 0)

	reportThrottle(state, namespaceKey, true)
	closeWindow(state, timeSource, namespaceKey)

	require.InEpsilon(t, 85.0, throttleRate(state, namespaceKey), 1e-9)
	require.Zero(t, throttleRate(state, otherCause))
	require.Equal(t, 1, throttleLen(state))
}

func TestThrottleState_ShardsAreIndependent(t *testing.T) {
	state, timeSource := newTestThrottleState(defaultThrottleOverrides())

	shard1 := NewThrottleKey(enumspb.RESOURCE_EXHAUSTED_CAUSE_SHARD_PERSISTENCE_LIMIT, "ns-1", 1)
	shard2 := NewThrottleKey(enumspb.RESOURCE_EXHAUSTED_CAUSE_SHARD_PERSISTENCE_LIMIT, "ns-1", 2)

	reportThrottle(state, shard1, true)
	closeWindow(state, timeSource, shard1)

	require.InEpsilon(t, 85.0, throttleRate(state, shard1), 1e-9)
	require.Zero(t, throttleRate(state, shard2))
	require.Equal(t, 1, throttleLen(state))
}

func TestThrottleState_UnadmittedRejectionsDoNotMoveTheRate(t *testing.T) {
	state, timeSource := newTestThrottleState(defaultThrottleOverrides())
	key := testKey()

	// The class has to exist before this means anything: an unmetered rejection does not
	// create one, since a class the gate never issued for would only hold a key slot.
	require.True(t, admitOK(state, key))
	require.Equal(t, 1, throttleLen(state))

	for range 100 {
		reportThrottle(state, key, false)
		timeSource.Update(timeSource.Now().Add(testThrottleWindow))
	}

	require.InEpsilon(t, 100.0, throttleRate(state, key), 1e-9)
	require.Equal(t, 1, throttleLen(state), "the class is still tracked, it is just not being driven")
}

// Past the key cap every new class fails open, so a slot spent on a class the gate never
// issued for is taken from one it would have paced. A rejection with no permit behind it
// is a first dispatch off the reader, which the controller does not govern.
func TestThrottleState_UnadmittedRejectionsDoNotAllocateKeys(t *testing.T) {
	state, _ := newTestThrottleState(defaultThrottleOverrides())

	for i := range 8 {
		reportThrottle(state, apsKey(namespace.Name(fmt.Sprintf("ns-%d", i))), false)
	}
	require.Zero(t, throttleLen(state), "unmetered rejections must not cost key slots")
}

func TestThrottleState_IdleWindowsDoNotMoveTheRate(t *testing.T) {
	state, timeSource := newTestThrottleState(defaultThrottleOverrides())
	key := testKey()

	require.True(t, admitOK(state, key))
	closeWindow(state, timeSource, key)
	rate := throttleRate(state, key)

	for range 50 {
		closeWindow(state, timeSource, key)
	}

	require.InEpsilon(t, rate, throttleRate(state, key), 1e-9, "idle windows must leave the rate alone")
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
