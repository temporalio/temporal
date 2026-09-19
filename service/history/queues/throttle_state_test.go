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
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
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
	state := NewThrottleState(
		ThrottleStateOptions{
			Enabled:       dynamicconfig.GetBoolPropertyFn(o.enabled),
			Beta:          dynamicconfig.GetFloatPropertyFn(o.beta),
			IncreaseRatio: dynamicconfig.GetFloatPropertyFn(o.increase),
			LossThreshold: dynamicconfig.GetFloatPropertyFn(o.lossThresh),
			Window:        dynamicconfig.GetDurationPropertyFn(window),
			MinRate:       dynamicconfig.GetFloatPropertyFn(o.minRate),
			MaxRate:       dynamicconfig.GetFloatPropertyFn(o.maxRate),
			InitialRate:   dynamicconfig.GetFloatPropertyFn(o.initialRate),
			MaxKeys:       dynamicconfig.GetIntPropertyFn(o.maxKeys),
			KeyTTL:        dynamicconfig.GetDurationPropertyFn(o.keyTTL),
		},
		timeSource,
		log.NewTestLogger(),
		metrics.NoopMetricsHandler,
	)
	return state, timeSource
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

		{name: "aps at system scope", cause: enumspb.RESOURCE_EXHAUSTED_CAUSE_APS_LIMIT, scope: system},
		{name: "persistence at system scope", cause: enumspb.RESOURCE_EXHAUSTED_CAUSE_PERSISTENCE_LIMIT, scope: system},
		{name: "unspecified scope is not namespace", cause: enumspb.RESOURCE_EXHAUSTED_CAUSE_APS_LIMIT, scope: unspecified},

		{name: "busy workflow", cause: enumspb.RESOURCE_EXHAUSTED_CAUSE_BUSY_WORKFLOW, scope: ns},
		{name: "circuit breaker", cause: enumspb.RESOURCE_EXHAUSTED_CAUSE_CIRCUIT_BREAKER_OPEN, scope: ns},

		{name: "system overloaded", cause: enumspb.RESOURCE_EXHAUSTED_CAUSE_SYSTEM_OVERLOADED, scope: ns},
		{name: "concurrent limit", cause: enumspb.RESOURCE_EXHAUSTED_CAUSE_CONCURRENT_LIMIT, scope: ns},
		{name: "rps limit", cause: enumspb.RESOURCE_EXHAUSTED_CAUSE_RPS_LIMIT, scope: ns},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, IsControllerInput(tc.cause, tc.scope))
		})
	}
}

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

// Shares another controller's class map, so the enabled flag can change without losing it.
func newTestThrottleStateWithEntries(
	o throttleTestOverrides,
	from *ThrottleState,
) (*ThrottleState, *clock.EventTimeSource) {
	state, timeSource := newTestThrottleState(o)
	state.entries = from.entries
	return state, timeSource
}

func testKey() ThrottleKey {
	return NewThrottleKey(enumspb.RESOURCE_EXHAUSTED_CAUSE_APS_LIMIT, "ns-1")
}

func admitOK(c *ThrottleState, key ThrottleKey) bool {
	allowed, _, _ := c.Admit(key)
	return allowed
}

// reportThrottle feeds rejections in for one control decision. A decision needs a sample the
// loss ratio can resolve, so the admitted form supplies the releases the rejections are
// measured against: one call is one window of total loss.
// reportThrottle drives one decision's worth of total loss through the real admission path:
// enough metered releases for the ratio to resolve the threshold, every one of them refused.
// The unadmitted form reports a rejection the gate never issued, which must stay inert.
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
	_, _, lossThreshold := c.controlLaw()
	samples := minDecisionReleases(lossThreshold)
	entry.Lock()
	entry.releases += samples
	entry.rejections += samples - 1
	entry.Unlock()
	c.ReportThrottled(key, true)
}

// One metered release and its rejection: the pair the control law measures.
func admitAndReject(c *ThrottleState, key ThrottleKey) bool {
	allowed, metered, _ := c.Admit(key)
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
	state.touchLocked(entry, ts.Now(), state.Window())
	state.advanceWindowLocked(entry, ts.Now(), state.Window())
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

	for i := 0; i < 500; i++ {
		reportThrottle(state, key, true)
		timeSource.Update(timeSource.Now().Add(time.Millisecond))
	}
	closeWindow(state, timeSource, key)

	require.InEpsilon(t, 85.0, throttleRate(state, key), 1e-9)
}

func TestThrottleState_DecreasesAcrossWindows(t *testing.T) {
	state, timeSource := newTestThrottleState(defaultThrottleOverrides())
	key := testKey()

	for i := 0; i < 4; i++ {
		reportThrottle(state, key, true)
		closeWindow(state, timeSource, key)
	}

	require.InEpsilon(t, 100*0.85*0.85*0.85*0.85, throttleRate(state, key), 1e-9)
}

func TestThrottleState_ControlLawUpdatesAreLive(t *testing.T) {
	beta := 0.5
	state, timeSource := newTestThrottleState(defaultThrottleOverrides())
	state.options.Beta = func() float64 { return beta }
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

	for i := 0; i < 100; i++ {
		reportThrottle(state, key, true)
		closeWindow(state, timeSource, key)
	}
	require.InEpsilon(t, 20.0, throttleRate(state, key), 1e-9, "floor keeps the class making forward progress")

	for i := 0; i < 200; i++ {
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
	reportThrottle(state, key, true)

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
		reportThrottle(state, key, true)
		require.True(t, admitOK(state, key))
	}
	require.Zero(t, throttleLen(state), "a disabled controller must not accumulate state")
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
		reportThrottle(state, key, true)
		require.True(t, admitOK(state, key))
		require.False(t, admitOK(state, key))
	}
	require.Equal(t, 2, throttleLen(state))

	overflow := testKey()
	overflow.NamespaceID = "overflow"
	reportThrottle(state, overflow, true)
	for i := 0; i < 100; i++ {
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
	idle.NamespaceID = "idle"
	reportThrottle(state, idle, true)
	require.Equal(t, 1, throttleLen(state))

	timeSource.Update(timeSource.Now().Add(2 * time.Minute))

	active := testKey()
	active.NamespaceID = "active"
	reportThrottle(state, active, true)

	require.Equal(t, 1, throttleLen(state))
	require.Zero(t, throttleRate(state, idle))
	require.Positive(t, throttleRate(state, active))
}

func TestThrottleState_ClassesAreIndependent(t *testing.T) {
	state, timeSource := newTestThrottleState(defaultThrottleOverrides())

	namespaceKey := testKey()
	otherCause := NewThrottleKey(enumspb.RESOURCE_EXHAUSTED_CAUSE_PERSISTENCE_LIMIT, "ns-1")

	reportThrottle(state, namespaceKey, true)
	closeWindow(state, timeSource, namespaceKey)

	require.InEpsilon(t, 85.0, throttleRate(state, namespaceKey), 1e-9)
	require.Zero(t, throttleRate(state, otherCause))
	require.Equal(t, 1, throttleLen(state))
}

func TestThrottleState_ConvergesTowardEnforcedBudget(t *testing.T) {
	overrides := defaultThrottleOverrides()
	overrides.initialRate = 1000
	state, timeSource := newTestThrottleState(overrides)
	key := testKey()

	const enforcedBudget = 200.0
	if admitOK(state, key) {
		reportThrottle(state, key, true)
	}
	for window := 0; window < 60; window++ {
		timeSource.Update(timeSource.Now().Add(testThrottleWindow))
		if throttleRate(state, key) > enforcedBudget {
			reportThrottle(state, key, true)
		} else {
			admitOK(state, key)
		}
	}

	rate := throttleRate(state, key)
	require.Greater(t, rate, enforcedBudget*0.5)
	require.Less(t, rate, enforcedBudget*1.5)
}

func TestThrottleState_UnadmittedRejectionsDoNotMoveTheRate(t *testing.T) {
	state, timeSource := newTestThrottleState(defaultThrottleOverrides())
	key := testKey()

	// The class has to exist before this means anything: an unmetered rejection does not
	// create one, since a class the gate never issued for would only hold a key slot.
	require.True(t, admitOK(state, key))
	require.Equal(t, 1, throttleLen(state))

	for i := 0; i < 100; i++ {
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

	for i := 0; i < 8; i++ {
		reportThrottle(state, apsKey(fmt.Sprintf("ns-%d", i)), false)
	}
	require.Zero(t, throttleLen(state), "unmetered rejections must not cost key slots")
}

func TestThrottleState_UnadmittedRejectionsDoNotBlockIncrease(t *testing.T) {
	state, timeSource := newTestThrottleState(defaultThrottleOverrides())
	key := testKey()

	reportThrottle(state, key, true)
	closeWindow(state, timeSource, key)
	require.InEpsilon(t, 85.0, throttleRate(state, key), 1e-9)

	// Rejections the gate did not issue are loss on traffic it never sent, so they must not
	// hold the rate down: each of these windows has to climb as though it saw none of them.
	want := 85.0
	for i := 0; i < 4; i++ {
		cleanWindow(state, key)
		reportThrottle(state, key, false)
		closeWindow(state, timeSource, key)

		want *= 1.1
		require.InEpsilon(t, want, throttleRate(state, key), 1e-9,
			"unadmitted rejections blocked the increase in window %d", i)
	}
}

func TestThrottleState_IdleWindowsDoNotMoveTheRate(t *testing.T) {
	state, timeSource := newTestThrottleState(defaultThrottleOverrides())
	key := testKey()

	require.True(t, admitOK(state, key))
	closeWindow(state, timeSource, key)
	rate := throttleRate(state, key)

	for i := 0; i < 50; i++ {
		closeWindow(state, timeSource, key)
	}

	require.InEpsilon(t, rate, throttleRate(state, key), 1e-9, "idle windows must leave the rate alone")
}

func TestThrottleState_BusyClassIsNotPunishedForItsSize(t *testing.T) {
	const rejectEveryNth = 50 // exactly 2% loss for any class size, under the 5% threshold

	rateFor := func(demandPerWindow int) float64 {
		state, timeSource := newTestThrottleState(defaultThrottleOverrides())
		key := testKey()
		admitted := 0
		for w := 0; w < 20; w++ {
			for i := 0; i < demandPerWindow; i++ {
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
		return throttleRate(state, key)
	}

	o := defaultThrottleOverrides()
	want := o.initialRate * math.Pow(1+o.increase, 20)
	require.Less(t, want, o.maxRate, "the run must stay below the ceiling to mean anything")

	rates := make([]float64, 0, 2)
	for _, demand := range []int{700, 7000} {
		rate := rateFor(demand)
		require.InEpsilon(t, want, rate, 1e-9,
			"a class seeing 2%% loss against a 5%% threshold must be allowed to speed up every "+
				"window, whatever its size; demand=%d", demand)
		rates = append(rates, rate)
	}
	require.InEpsilon(t, rates[0], rates[1], 1e-9,
		"two classes seeing the same loss must settle at the same rate, whatever their size")
}
