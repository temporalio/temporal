package queues

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/tasks"
)

const testThrottleWindow = time.Second

type throttleTestOverrides struct {
	enabled     bool
	beta        float64
	increase    float64
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
	), timeSource
}

func TestNewThrottleKey_NamespaceBudgetCausesIgnoreCategory(t *testing.T) {
	transfer := NewThrottleKey(
		enumspb.RESOURCE_EXHAUSTED_CAUSE_APS_LIMIT,
		enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE,
		"ns-1", 7, tasks.CategoryTransfer.Name(),
	)
	timer := NewThrottleKey(
		enumspb.RESOURCE_EXHAUSTED_CAUSE_APS_LIMIT,
		enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE,
		"ns-1", 9, tasks.CategoryTimer.Name(),
	)

	require.Equal(t, ThrottleScopeNamespace, transfer.Scope)
	require.Empty(t, transfer.Category)
	require.Zero(t, transfer.ShardID)
	require.Equal(t, transfer, timer, "namespace budgets are shared across categories and shards")
}

func TestNewThrottleKey_InfrastructureCausesKeepCategory(t *testing.T) {
	transfer := NewThrottleKey(
		enumspb.RESOURCE_EXHAUSTED_CAUSE_SYSTEM_OVERLOADED,
		enumspb.RESOURCE_EXHAUSTED_SCOPE_SYSTEM,
		"ns-1", 7, tasks.CategoryTransfer.Name(),
	)
	visibility := NewThrottleKey(
		enumspb.RESOURCE_EXHAUSTED_CAUSE_SYSTEM_OVERLOADED,
		enumspb.RESOURCE_EXHAUSTED_SCOPE_SYSTEM,
		"ns-1", 7, tasks.CategoryVisibility.Name(),
	)

	require.Equal(t, ThrottleScopeHost, transfer.Scope)
	require.Equal(t, tasks.CategoryTransfer.Name(), transfer.Category)
	require.Empty(t, transfer.NamespaceID, "host scope must not fan out per namespace")
	require.NotEqual(t, transfer, visibility, "an Elasticsearch overload must not gate transfer tasks")
}

func TestNewThrottleKey_ConcurrentLimitIsShardScoped(t *testing.T) {
	shard7 := NewThrottleKey(
		enumspb.RESOURCE_EXHAUSTED_CAUSE_CONCURRENT_LIMIT,
		enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE,
		"ns-1", 7, tasks.CategoryTransfer.Name(),
	)
	shard8 := NewThrottleKey(
		enumspb.RESOURCE_EXHAUSTED_CAUSE_CONCURRENT_LIMIT,
		enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE,
		"ns-1", 8, tasks.CategoryTransfer.Name(),
	)

	require.Equal(t, ThrottleScopeNamespaceShard, shard7.Scope)
	require.NotEqual(t, shard7, shard8)
}

func TestNewThrottleKey_PersistenceLimitScopeDecidesRouting(t *testing.T) {
	namespaceScoped := NewThrottleKey(
		enumspb.RESOURCE_EXHAUSTED_CAUSE_PERSISTENCE_LIMIT,
		enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE,
		"ns-1", 7, tasks.CategoryTransfer.Name(),
	)
	systemScoped := NewThrottleKey(
		enumspb.RESOURCE_EXHAUSTED_CAUSE_PERSISTENCE_LIMIT,
		enumspb.RESOURCE_EXHAUSTED_SCOPE_SYSTEM,
		"ns-1", 7, tasks.CategoryTransfer.Name(),
	)

	require.Equal(t, ThrottleScopeNamespace, namespaceScoped.Scope)
	require.Equal(t, ThrottleScopeHost, systemScoped.Scope)
}

func TestIsControllerInput(t *testing.T) {
	require.False(t, IsControllerInput(nil, enumspb.RESOURCE_EXHAUSTED_CAUSE_BUSY_WORKFLOW))
	require.False(t, IsControllerInput(nil, enumspb.RESOURCE_EXHAUSTED_CAUSE_CIRCUIT_BREAKER_OPEN))
	require.True(t, IsControllerInput(nil, enumspb.RESOURCE_EXHAUSTED_CAUSE_APS_LIMIT))
	require.True(t, IsControllerInput(nil, enumspb.RESOURCE_EXHAUSTED_CAUSE_SYSTEM_OVERLOADED))

	// Enforced per (namespace, businessID, archetype) despite reporting namespace scope, so it
	// must not drive a namespace wide class even though its cause is otherwise controller input.
	require.True(t, IsControllerInput(nil, enumspb.RESOURCE_EXHAUSTED_CAUSE_RPS_LIMIT))
	require.False(t, IsControllerInput(
		consts.ErrBusinessIDRateLimitExceeded, enumspb.RESOURCE_EXHAUSTED_CAUSE_RPS_LIMIT))
	require.False(t, IsControllerInput(
		fmt.Errorf("wrapped: %w", consts.ErrBusinessIDRateLimitExceeded),
		enumspb.RESOURCE_EXHAUSTED_CAUSE_RPS_LIMIT))
}

func testKey() ThrottleKey {
	return ThrottleKey{
		Scope:       ThrottleScopeNamespace,
		Cause:       enumspb.RESOURCE_EXHAUSTED_CAUSE_APS_LIMIT,
		NamespaceID: "ns-1",
	}
}

func TestThrottleState_OneDecreasePerWindow(t *testing.T) {
	state, timeSource := newTestThrottleState(defaultThrottleOverrides())
	key := testKey()

	for i := 0; i < 500; i++ {
		state.ReportThrottled(key, true)
		timeSource.Update(timeSource.Now().Add(time.Millisecond))
	}

	decreases, _ := state.Counters(key)
	require.Equal(t, int64(1), decreases, "a namespace at its budget rejects routinely; that is not 500 signals")
	require.InEpsilon(t, 85.0, state.AdmittedRate(key), 1e-9)
}

func TestThrottleState_DecreasesAcrossWindows(t *testing.T) {
	state, timeSource := newTestThrottleState(defaultThrottleOverrides())
	key := testKey()

	for i := 0; i < 4; i++ {
		state.ReportThrottled(key, true)
		timeSource.Update(timeSource.Now().Add(testThrottleWindow))
	}

	decreases, _ := state.Counters(key)
	require.Equal(t, int64(4), decreases)
	require.InEpsilon(t, 100*0.85*0.85*0.85*0.85, state.AdmittedRate(key), 1e-9)
}

func TestThrottleState_AdditiveIncreaseAfterCleanWindow(t *testing.T) {
	state, timeSource := newTestThrottleState(defaultThrottleOverrides())
	key := testKey()

	state.ReportThrottled(key, true)
	require.InEpsilon(t, 85.0, state.AdmittedRate(key), 1e-9)

	// Two clean windows: the first closes the window the throttle landed in, the second is clean.
	timeSource.Update(timeSource.Now().Add(testThrottleWindow))
	state.ReportSuccess(key)
	timeSource.Update(timeSource.Now().Add(testThrottleWindow))
	state.ReportSuccess(key)

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

	_, increases := state.Counters(key)
	require.Zero(t, increases)
}

func TestThrottleState_ClampsRate(t *testing.T) {
	overrides := defaultThrottleOverrides()
	overrides.minRate = 20
	overrides.maxRate = 120
	state, timeSource := newTestThrottleState(overrides)
	key := testKey()

	for i := 0; i < 100; i++ {
		state.ReportThrottled(key, true)
		timeSource.Update(timeSource.Now().Add(testThrottleWindow))
	}
	require.InEpsilon(t, 20.0, state.AdmittedRate(key), 1e-9, "floor keeps the class making forward progress")

	for i := 0; i < 200; i++ {
		timeSource.Update(timeSource.Now().Add(testThrottleWindow))
		state.ReportSuccess(key)
	}
	require.InEpsilon(t, 120.0, state.AdmittedRate(key), 1e-9, "ceiling stops an idle class from dumping on the next burst")
}

func TestThrottleState_AdmitEnforcesRate(t *testing.T) {
	overrides := defaultThrottleOverrides()
	overrides.initialRate = 10
	state, timeSource := newTestThrottleState(overrides)
	key := testKey()

	admitted := 0
	for i := 0; i < 100; i++ {
		if state.Admit(key) {
			admitted++
		}
	}
	require.Equal(t, 10, admitted, "burst is capped at one window of the current rate")

	require.False(t, state.Admit(key))
	timeSource.Update(timeSource.Now().Add(200 * time.Millisecond))
	require.True(t, state.Admit(key))
	require.True(t, state.Admit(key))
	require.False(t, state.Admit(key))
}

func TestThrottleState_DecreaseTrimsAccumulatedTokens(t *testing.T) {
	overrides := defaultThrottleOverrides()
	overrides.initialRate = 100
	state, timeSource := newTestThrottleState(overrides)
	key := testKey()

	require.True(t, state.Admit(key))
	timeSource.Update(timeSource.Now().Add(10 * testThrottleWindow))
	state.ReportThrottled(key, true)

	admitted := 0
	for i := 0; i < 1000; i++ {
		if state.Admit(key) {
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
		require.True(t, state.Admit(key))
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
		require.True(t, state.Admit(key))
		require.False(t, state.Admit(key))
	}
	require.Equal(t, 2, state.Len())

	overflow := testKey()
	overflow.NamespaceID = "overflow"
	state.ReportThrottled(overflow, true)
	for i := 0; i < 100; i++ {
		require.True(t, state.Admit(overflow), "past the cap the real limiter stays the enforcement point")
	}
	require.Equal(t, 2, state.Len())
	require.Zero(t, state.AdmittedRate(overflow))

	// The tracked keys keep their own budgets.
	require.False(t, state.Admit(tracked[0]))
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

func TestThrottleState_ScopesAreIndependent(t *testing.T) {
	state, _ := newTestThrottleState(defaultThrottleOverrides())

	namespaceKey := testKey()
	hostKey := ThrottleKey{
		Scope:    ThrottleScopeHost,
		Cause:    enumspb.RESOURCE_EXHAUSTED_CAUSE_SYSTEM_OVERLOADED,
		Category: tasks.CategoryTransfer.Name(),
	}

	state.ReportThrottled(namespaceKey, true)

	require.InEpsilon(t, 85.0, state.AdmittedRate(namespaceKey), 1e-9)
	require.Zero(t, state.AdmittedRate(hostKey))
	require.Equal(t, 1, state.Len())
}

func TestThrottleState_ConvergesTowardEnforcedBudget(t *testing.T) {
	overrides := defaultThrottleOverrides()
	overrides.initialRate = 1000
	state, timeSource := newTestThrottleState(overrides)
	key := testKey()

	// A token bucket enforcing 200/s rejects whenever the class asks for more than that.
	const enforcedBudget = 200.0
	state.ReportThrottled(key, true)
	for window := 0; window < 60; window++ {
		timeSource.Update(timeSource.Now().Add(testThrottleWindow))
		if state.AdmittedRate(key) > enforcedBudget {
			state.ReportThrottled(key, true)
		} else {
			state.ReportSuccess(key)
		}
	}

	rate := state.AdmittedRate(key)
	require.Greater(t, rate, enforcedBudget*0.5)
	require.Less(t, rate, enforcedBudget*1.5)
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
	require.InEpsilon(t, 85.0, state.AdmittedRate(key), 1e-9)

	for i := 0; i < 4; i++ {
		timeSource.Update(timeSource.Now().Add(testThrottleWindow))
		state.ReportThrottled(key, false)
		state.ReportSuccess(key)
	}

	_, increases := state.Counters(key)
	require.Positive(t, increases, "windows with only ungated rejections still count as clean")
}

func TestThrottleState_ReturnRestoresAToken(t *testing.T) {
	overrides := defaultThrottleOverrides()
	overrides.initialRate = 2
	state, _ := newTestThrottleState(overrides)
	key := testKey()

	require.True(t, state.Admit(key))
	require.True(t, state.Admit(key))
	require.False(t, state.Admit(key))

	state.Return(key)
	require.True(t, state.Admit(key))
	require.False(t, state.Admit(key))
}

func TestThrottleState_ReturnCannotExceedBurst(t *testing.T) {
	overrides := defaultThrottleOverrides()
	overrides.initialRate = 2
	state, _ := newTestThrottleState(overrides)
	key := testKey()

	require.True(t, state.Admit(key))
	for i := 0; i < 10; i++ {
		state.Return(key)
	}

	admitted := 0
	for i := 0; i < 10; i++ {
		if state.Admit(key) {
			admitted++
		}
	}
	require.Equal(t, 2, admitted)
}
