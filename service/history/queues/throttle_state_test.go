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
	"go.temporal.io/server/service/history/consts"
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
	return NewThrottleState(
		ThrottleStateOptions{
			Enabled:       dynamicconfig.GetBoolPropertyFn(o.enabled),
			Beta:          dynamicconfig.GetFloatPropertyFn(o.beta),
			IncreaseRatio: dynamicconfig.GetFloatPropertyFn(o.increase),
			LossThreshold: dynamicconfig.GetFloatPropertyFn(o.lossThresh),
			Window:        dynamicconfig.GetDurationPropertyFn(window),
			MinRate:       o.minRate,
			MaxRate:       o.maxRate,
			InitialRate:   o.initialRate,
			MaxKeys:       dynamicconfig.GetIntPropertyFn(o.maxKeys),
			KeyTTL:        o.keyTTL,
		},
		timeSource,
		log.NewTestLogger(),
		metrics.NoopMetricsHandler,
	), timeSource
}

// The controller governs a deliberately short list, and only where the budget belongs to the
// namespace. This table is the whole admission rule; widening coverage should show up here as
// a row rather than as a new branch somewhere else.
func TestIsControllerInput(t *testing.T) {
	ns := enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE
	system := enumspb.RESOURCE_EXHAUSTED_SCOPE_SYSTEM
	unspecified := enumspb.RESOURCE_EXHAUSTED_SCOPE_UNSPECIFIED

	for _, tc := range []struct {
		name  string
		err   error
		cause enumspb.ResourceExhaustedCause
		scope enumspb.ResourceExhaustedScope
		want  bool
	}{
		{name: "aps at namespace scope", cause: enumspb.RESOURCE_EXHAUSTED_CAUSE_APS_LIMIT, scope: ns, want: true},
		{name: "persistence at namespace scope", cause: enumspb.RESOURCE_EXHAUSTED_CAUSE_PERSISTENCE_LIMIT, scope: ns, want: true},

		// A budget this namespace only partly owns: its own rejections cannot tell it the shape
		// of a limit that other namespaces are also consuming.
		{name: "aps at system scope", cause: enumspb.RESOURCE_EXHAUSTED_CAUSE_APS_LIMIT, scope: system},
		{name: "persistence at system scope", cause: enumspb.RESOURCE_EXHAUSTED_CAUSE_PERSISTENCE_LIMIT, scope: system},
		{name: "unspecified scope is not namespace", cause: enumspb.RESOURCE_EXHAUSTED_CAUSE_APS_LIMIT, scope: unspecified},

		// Per workflow lock contention, and a controller in its own right.
		{name: "busy workflow", cause: enumspb.RESOURCE_EXHAUSTED_CAUSE_BUSY_WORKFLOW, scope: ns},
		{name: "circuit breaker", cause: enumspb.RESOURCE_EXHAUSTED_CAUSE_CIRCUIT_BREAKER_OPEN, scope: ns},

		// Outside the governed list for now.
		{name: "system overloaded", cause: enumspb.RESOURCE_EXHAUSTED_CAUSE_SYSTEM_OVERLOADED, scope: ns},
		{name: "concurrent limit", cause: enumspb.RESOURCE_EXHAUSTED_CAUSE_CONCURRENT_LIMIT, scope: ns},
		{name: "rps limit", cause: enumspb.RESOURCE_EXHAUSTED_CAUSE_RPS_LIMIT, scope: ns},

		// Pinned against a *governed* cause on purpose. Its real cause is RPS_LIMIT, which the
		// allowlist already rejects, so pairing it with APS_LIMIT is what actually exercises the
		// errors.Is guard: delete that guard and this row fails, and only this row.
		{
			name:  "business id reuse reports a cause the controller does not govern",
			err:   consts.ErrBusinessIDRateLimitExceeded,
			cause: enumspb.RESOURCE_EXHAUSTED_CAUSE_RPS_LIMIT,
			scope: ns,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, IsControllerInput(tc.err, tc.cause, tc.scope))
		})
	}
}

// A namespace budget is one budget however the traffic is spread, so every task in the
// namespace draws on the same class whatever its shard or category.
func TestNewThrottleKey_OneClassPerNamespaceAndCause(t *testing.T) {
	aps := NewThrottleKey(enumspb.RESOURCE_EXHAUSTED_CAUSE_APS_LIMIT, "ns-1")

	require.Equal(t, "ns-1", aps.NamespaceID)
	require.Equal(t, enumspb.RESOURCE_EXHAUSTED_CAUSE_APS_LIMIT, aps.Cause)

	require.Equal(t, aps, NewThrottleKey(enumspb.RESOURCE_EXHAUSTED_CAUSE_APS_LIMIT, "ns-1"))
	require.NotEqual(t, aps, NewThrottleKey(enumspb.RESOURCE_EXHAUSTED_CAUSE_APS_LIMIT, "ns-2"),
		"one namespace's budget must not gate another's")
	require.NotEqual(t, aps, NewThrottleKey(enumspb.RESOURCE_EXHAUSTED_CAUSE_PERSISTENCE_LIMIT, "ns-1"),
		"two budgets a namespace holds independently must not share a class")
}

func testKey() ThrottleKey {
	return NewThrottleKey(enumspb.RESOURCE_EXHAUSTED_CAUSE_APS_LIMIT, "ns-1")
}

// closeWindow advances past the control window that is currently open and triggers the single
// rate decision for it. The rate only ever moves at a window boundary, so a test that reports
// evidence without closing the window is asserting on a decision that has not been made yet.
// admitOK is the bool-only form of Admit, which is all most of these tests need. Admit itself
// carries the metered flag and the retry estimate because production depends on both.
func admitOK(c ThrottleController, key ThrottleKey) bool {
	allowed, _, _ := c.Admit(key)
	return allowed
}

func closeWindow(state *ThrottleState, ts *clock.EventTimeSource, key ThrottleKey) {
	ts.Update(ts.Now().Add(testThrottleWindow))
	state.ReportSuccess(key)
}

func TestThrottleState_OneDecreasePerWindow(t *testing.T) {
	state, timeSource := newTestThrottleState(defaultThrottleOverrides())
	key := testKey()

	for i := 0; i < 500; i++ {
		state.ReportThrottled(key, true)
		timeSource.Update(timeSource.Now().Add(time.Millisecond))
	}
	closeWindow(state, timeSource, key)

	decreases, _ := state.Counters(key)
	require.Equal(t, int64(1), decreases, "a namespace at its budget rejects routinely; that is not 500 signals")
	require.InEpsilon(t, 85.0, state.AdmittedRate(key), 1e-9)
}

func TestThrottleState_DecreasesAcrossWindows(t *testing.T) {
	state, timeSource := newTestThrottleState(defaultThrottleOverrides())
	key := testKey()

	for i := 0; i < 4; i++ {
		state.ReportThrottled(key, true)
		closeWindow(state, timeSource, key)
	}

	decreases, _ := state.Counters(key)
	require.Equal(t, int64(4), decreases)
	require.InEpsilon(t, 100*0.85*0.85*0.85*0.85, state.AdmittedRate(key), 1e-9)
}

func TestThrottleState_IncreaseAfterCleanWindow(t *testing.T) {
	state, timeSource := newTestThrottleState(defaultThrottleOverrides())
	key := testKey()

	state.ReportThrottled(key, true)
	closeWindow(state, timeSource, key)
	require.InEpsilon(t, 85.0, state.AdmittedRate(key), 1e-9)

	// A window that released work and saw no rejection is what earns an increase. An idle
	// window earns nothing, so the release here is the point of the test, not setup.
	require.True(t, admitOK(state, key))
	closeWindow(state, timeSource, key)

	_, increases := state.Counters(key)
	require.Equal(t, int64(1), increases)
	require.InEpsilon(t, 85.0*1.1, state.AdmittedRate(key), 1e-9)
}

func TestThrottleState_NoIncreaseInAThrottledWindow(t *testing.T) {
	state, timeSource := newTestThrottleState(defaultThrottleOverrides())
	key := testKey()

	for i := 0; i < 5; i++ {
		state.ReportThrottled(key, true)
		timeSource.Update(timeSource.Now().Add(testThrottleWindow / 2))
		state.ReportSuccess(key)
		timeSource.Update(timeSource.Now().Add(testThrottleWindow / 2))
	}

	decreases, increases := state.Counters(key)
	require.Zero(t, increases, "a window carrying loss above the threshold must not increase")
	// Five reports, but the first lands before any window has elapsed, so four windows close.
	require.Equal(t, int64(4), decreases, "it must decrease instead, once per window")
	require.InEpsilon(t, 100*math.Pow(0.85, 4), state.AdmittedRate(key), 1e-9)
}

func TestThrottleState_ClampsRate(t *testing.T) {
	overrides := defaultThrottleOverrides()
	overrides.minRate = 20
	overrides.maxRate = 120
	state, timeSource := newTestThrottleState(overrides)
	key := testKey()

	for i := 0; i < 100; i++ {
		state.ReportThrottled(key, true)
		closeWindow(state, timeSource, key)
	}
	require.InEpsilon(t, 20.0, state.AdmittedRate(key), 1e-9, "floor keeps the class making forward progress")

	for i := 0; i < 200; i++ {
		admitOK(state, key)
		closeWindow(state, timeSource, key)
	}
	require.InEpsilon(t, 120.0, state.AdmittedRate(key), 1e-9, "ceiling bounds what a recovering class can climb to")
}

func TestThrottleState_AdmitEnforcesRate(t *testing.T) {
	overrides := defaultThrottleOverrides()
	overrides.initialRate = 10
	state, timeSource := newTestThrottleState(overrides)
	key := testKey()

	admitted := 0
	for i := 0; i < 100; i++ {
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
	state.ReportThrottled(key, true)

	admitted := 0
	for i := 0; i < 1000; i++ {
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

	for i := 0; i < 100; i++ {
		state.ReportThrottled(key, true)
		require.True(t, admitOK(state, key))
	}
	require.Zero(t, state.Len(), "a disabled controller must not accumulate state")
}

func TestThrottleState_FailsOpenPastKeyCap(t *testing.T) {
	overrides := defaultThrottleOverrides()
	overrides.maxKeys = 2
	overrides.initialRate = 1
	state, _ := newTestThrottleState(overrides)

	tracked := make([]ThrottleKey, 0, 2)
	for i := 0; i < 2; i++ {
		key := testKey()
		key.NamespaceID = string(rune('a' + i))
		tracked = append(tracked, key)
		state.ReportThrottled(key, true)
		require.True(t, admitOK(state, key))
		require.False(t, admitOK(state, key))
	}
	require.Equal(t, 2, state.Len())

	overflow := testKey()
	overflow.NamespaceID = "overflow"
	state.ReportThrottled(overflow, true)
	for i := 0; i < 100; i++ {
		require.True(t, admitOK(state, overflow), "past the cap the real limiter stays the enforcement point")
	}
	require.Equal(t, 2, state.Len())
	require.Zero(t, state.AdmittedRate(overflow))

	// The tracked keys keep their own budgets.
	require.False(t, admitOK(state, tracked[0]))
}

func TestThrottleState_SweepsIdleKeys(t *testing.T) {
	overrides := defaultThrottleOverrides()
	overrides.keyTTL = time.Minute
	state, timeSource := newTestThrottleState(overrides)

	idle := testKey()
	idle.NamespaceID = "idle"
	state.ReportThrottled(idle, true)
	require.Equal(t, 1, state.Len())

	timeSource.Update(timeSource.Now().Add(2 * time.Minute))

	active := testKey()
	active.NamespaceID = "active"
	state.ReportThrottled(active, true)

	require.Equal(t, 1, state.Len())
	require.Zero(t, state.AdmittedRate(idle))
	require.Positive(t, state.AdmittedRate(active))
}

func TestThrottleState_ClassesAreIndependent(t *testing.T) {
	state, timeSource := newTestThrottleState(defaultThrottleOverrides())

	namespaceKey := testKey()
	otherCause := NewThrottleKey(enumspb.RESOURCE_EXHAUSTED_CAUSE_PERSISTENCE_LIMIT, "ns-1")

	state.ReportThrottled(namespaceKey, true)
	closeWindow(state, timeSource, namespaceKey)

	require.InEpsilon(t, 85.0, state.AdmittedRate(namespaceKey), 1e-9)
	require.Zero(t, state.AdmittedRate(otherCause))
	require.Equal(t, 1, state.Len())
}

func TestThrottleState_ConvergesTowardEnforcedBudget(t *testing.T) {
	overrides := defaultThrottleOverrides()
	overrides.initialRate = 1000
	state, timeSource := newTestThrottleState(overrides)
	key := testKey()

	// A token bucket enforcing 200/s rejects whenever the class asks for more than that.
	const enforcedBudget = 200.0
	if admitOK(state, key) {
		state.ReportThrottled(key, true)
	}
	for window := 0; window < 60; window++ {
		timeSource.Update(timeSource.Now().Add(testThrottleWindow))
		if state.AdmittedRate(key) > enforcedBudget {
			state.ReportThrottled(key, true)
		} else {
			admitOK(state, key)
			state.ReportSuccess(key)
		}
	}

	rate := state.AdmittedRate(key)
	require.Greater(t, rate, enforcedBudget*0.5)
	require.Less(t, rate, enforcedBudget*1.5)

	_, increases := state.Counters(key)
	require.Positive(t, increases, "a converging class must exercise the increase, not only decay")
}

// Only rejections from releases the gate metered may move the rate. Otherwise a namespace with
// a steady inflow of fresh tasks decreases every window, never has a clean window, and ratchets
// its parked backlog to the floor while the traffic actually consuming the budget flows past.
func TestThrottleState_UnadmittedRejectionsDoNotMoveTheRate(t *testing.T) {
	state, timeSource := newTestThrottleState(defaultThrottleOverrides())
	key := testKey()

	for i := 0; i < 100; i++ {
		state.ReportThrottled(key, false)
		timeSource.Update(timeSource.Now().Add(testThrottleWindow))
	}

	decreases, _ := state.Counters(key)
	require.Zero(t, decreases)
	require.InEpsilon(t, 100.0, state.AdmittedRate(key), 1e-9)
	require.Equal(t, 1, state.Len(), "the class is still tracked, it is just not being driven")
}

func TestThrottleState_UnadmittedRejectionsDoNotBlockIncrease(t *testing.T) {
	state, timeSource := newTestThrottleState(defaultThrottleOverrides())
	key := testKey()

	state.ReportThrottled(key, true)
	closeWindow(state, timeSource, key)
	require.InEpsilon(t, 85.0, state.AdmittedRate(key), 1e-9)

	for i := 0; i < 4; i++ {
		require.True(t, admitOK(state, key))
		state.ReportThrottled(key, false)
		closeWindow(state, timeSource, key)
	}

	_, increases := state.Counters(key)
	require.Positive(t, increases, "windows with only ungated rejections still count as clean")
}

func TestThrottleState_ReturnRestoresAToken(t *testing.T) {
	overrides := defaultThrottleOverrides()
	overrides.initialRate = 2
	state, _ := newTestThrottleState(overrides)
	key := testKey()

	require.True(t, admitOK(state, key))
	require.True(t, admitOK(state, key))
	require.False(t, admitOK(state, key))

	state.Return(key)
	require.True(t, admitOK(state, key))
	require.False(t, admitOK(state, key))
}

func TestThrottleState_ReturnCannotExceedBurst(t *testing.T) {
	overrides := defaultThrottleOverrides()
	overrides.initialRate = 2
	state, _ := newTestThrottleState(overrides)
	key := testKey()

	require.True(t, admitOK(state, key))
	for i := 0; i < 10; i++ {
		state.Return(key)
	}

	admitted := 0
	for i := 0; i < 10; i++ {
		if admitOK(state, key) {
			admitted++
		}
	}
	require.Equal(t, 2, admitted)
}

// An idle class must not climb. Increasing a class that released nothing only banks credit it
// will spend the instant it becomes due, which is the burst the cap exists to bound.
func TestThrottleState_IdleWindowsDoNotMoveTheRate(t *testing.T) {
	state, timeSource := newTestThrottleState(defaultThrottleOverrides())
	key := testKey()

	require.True(t, admitOK(state, key))
	closeWindow(state, timeSource, key)
	rate := state.AdmittedRate(key)

	for i := 0; i < 50; i++ {
		closeWindow(state, timeSource, key)
	}

	require.InEpsilon(t, rate, state.AdmittedRate(key), 1e-9, "idle windows must leave the rate alone")
}

// A class must not be punished for being busy. The enforcer here rejects a fixed fraction of
// what it is offered, the same fraction whatever the class size, and that fraction sits below
// the loss threshold - so every class should be told it may go faster, whatever its size.
//
// This is the property the previous law lacked. It decreased on any single rejection but
// increased only on a perfectly clean window, and the chance of a clean window is (1-p)^n: at
// 2% loss a class releasing 100 per window sees one 13% of the time and one releasing 1000
// essentially never, so both collapsed to the floor while a quiet class climbed. The rate a
// class settled at was decided by its size rather than by the loss it was actually seeing.
//
// A hard capacity hides this, because loss then rises with the rate instead of staying
// constant, so the enforcer below is deliberately probabilistic.
func TestThrottleState_BusyClassIsNotPunishedForItsSize(t *testing.T) {
	const rejectEveryNth = 50 // exactly 2% loss for any class size, under the 5% threshold

	rateFor := func(demandPerWindow int) float64 {
		state, timeSource := newTestThrottleState(defaultThrottleOverrides())
		key := testKey()
		admitted := 0
		for w := 0; w < 20; w++ {
			for i := 0; i < demandPerWindow; i++ {
				if !admitOK(state, key) {
					continue
				}
				admitted++
				if admitted%rejectEveryNth == 0 {
					state.ReportThrottled(key, true)
				}
			}
			closeWindow(state, timeSource, key)
		}
		return state.AdmittedRate(key)
	}

	// Both are large enough that 2% is representable within a single window; a class releasing
	// only a handful per window can observe 0% or 10% and nothing in between, and that
	// quantisation, not the control law, would decide where it settled.
	// Deliberately short of MaxRate: a clamp would make any two arms compare equal and hide a
	// size dependence of any size short of collapse.
	o := defaultThrottleOverrides()
	want := o.initialRate * math.Pow(1+o.increase, 20)
	require.Less(t, want, o.maxRate, "the run must stay below the ceiling to mean anything")

	rates := make([]float64, 0, 2)
	for _, demand := range []int{100, 1000} {
		rate := rateFor(demand)
		require.InEpsilon(t, want, rate, 1e-9,
			"a class seeing 2%% loss against a 5%% threshold must be allowed to speed up every "+
				"window, whether it releases 100 or 1000; demand=%d", demand)
		rates = append(rates, rate)
	}
	require.InEpsilon(t, rates[0], rates[1], 1e-9,
		"two classes seeing the same loss must settle at the same rate, whatever their size")
}
