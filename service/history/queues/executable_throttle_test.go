package queues

import (
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/metrics"
	ctasks "go.temporal.io/server/common/tasks"
	"go.temporal.io/server/service/history/tasks"
	"go.uber.org/mock/gomock"
)

func newThrottleTestExecutable(ctrl *gomock.Controller, state *ThrottleState) *executableImpl {
	task := tasks.NewMockTask(ctrl)
	task.EXPECT().GetNamespaceID().Return("ns-1").AnyTimes()
	return &executableImpl{
		Task:                task,
		priority:            ctasks.PriorityHigh,
		throttleState:       state,
		chasmMetricsHandler: metrics.NoopMetricsHandler,
	}
}

// A release refused by a second budget belongs to the class that issued it.
func TestExecutable_RejectionUnderAnotherBudgetChargesTheIssuingClass(t *testing.T) {
	ctrl := gomock.NewController(t)
	state, _ := newTestThrottleState(defaultThrottleOverrides())
	e := newThrottleTestExecutable(ctrl, state)

	// An earlier APS refusal parks the task in the APS class...
	e.reportThrottle(
		enumspb.RESOURCE_EXHAUSTED_CAUSE_APS_LIMIT,
		enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE,
	)
	issuing := NewThrottleKey(enumspb.RESOURCE_EXHAUSTED_CAUSE_APS_LIMIT, "ns-1")
	require.Equal(t, issuing, e.ThrottleKey())

	// ...and the rescheduler then releases it from that class.
	allowed, _, _ := state.Admit(issuing)
	require.True(t, allowed)
	e.SetThrottleAdmitted(true)

	e.reportThrottle(
		enumspb.RESOURCE_EXHAUSTED_CAUSE_PERSISTENCE_LIMIT,
		enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE,
	)

	_, rejections := throttleCounters(state, issuing)
	require.Equal(t, int64(1), rejections, "the class that issued the release must see the loss")
}

// A lock is not a budget: releasing slower cannot clear it, so charging it never lifts.
func TestExecutable_BusyWorkflowDoesNotChargeTheIssuingClass(t *testing.T) {
	ctrl := gomock.NewController(t)
	state, _ := newTestThrottleState(defaultThrottleOverrides())
	e := newThrottleTestExecutable(ctrl, state)

	// An earlier APS refusal parks the task in the APS class...
	e.reportThrottle(
		enumspb.RESOURCE_EXHAUSTED_CAUSE_APS_LIMIT,
		enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE,
	)
	issuing := NewThrottleKey(enumspb.RESOURCE_EXHAUSTED_CAUSE_APS_LIMIT, "ns-1")
	require.Equal(t, issuing, e.ThrottleKey())

	// ...and the rescheduler then releases it from that class.
	allowed, _, _ := state.Admit(issuing)
	require.True(t, allowed)
	e.SetThrottleAdmitted(true)

	e.reportThrottle(
		enumspb.RESOURCE_EXHAUSTED_CAUSE_BUSY_WORKFLOW,
		enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE,
	)

	_, rejections := throttleCounters(state, issuing)
	require.Zero(t, rejections, "lock contention is not evidence about a shared budget")
}

// Classification runs while off, so parked work is already classified when the flag flips.
func TestExecutable_ClassifiesWhileTheControllerIsOff(t *testing.T) {
	ctrl := gomock.NewController(t)
	o := defaultThrottleOverrides()
	o.enabled = false
	state, _ := newTestThrottleState(o)
	e := newThrottleTestExecutable(ctrl, state)

	e.reportThrottle(
		enumspb.RESOURCE_EXHAUSTED_CAUSE_APS_LIMIT,
		enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE,
	)

	key := e.ThrottleKey()
	require.NotEqual(t, ThrottleKey{}, key, "the task must know its class before the flag is on")
	require.Equal(t, enumspb.RESOURCE_EXHAUSTED_CAUSE_APS_LIMIT, key.Cause)
	require.Zero(t, throttleLen(state), "a disabled controller must track nothing")
}

// An ungoverned cause means the task leaves the gated class.
func TestExecutable_UngovernedCauseDropsTheKey(t *testing.T) {
	ctrl := gomock.NewController(t)
	state, _ := newTestThrottleState(defaultThrottleOverrides())
	e := newThrottleTestExecutable(ctrl, state)

	e.reportThrottle(
		enumspb.RESOURCE_EXHAUSTED_CAUSE_APS_LIMIT,
		enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE,
	)
	require.NotEqual(t, ThrottleKey{}, e.ThrottleKey())

	e.reportThrottle(
		enumspb.RESOURCE_EXHAUSTED_CAUSE_APS_LIMIT,
		enumspb.RESOURCE_EXHAUSTED_SCOPE_SYSTEM,
	)
	require.Equal(t, ThrottleKey{}, e.ThrottleKey(),
		"a system scoped limit is not this namespace's budget")
}

// Lock contention must never be charged, whichever side of the flag it lands on.
func TestExecutable_BusyWorkflowIsNeverChargedWhateverTheFlagSays(t *testing.T) {
	for _, enabled := range []bool{true, false} {
		ctrl := gomock.NewController(t)
		state, _ := newTestThrottleState(defaultThrottleOverrides())
		e := newThrottleTestExecutable(ctrl, state)

		issuing := NewThrottleKey(enumspb.RESOURCE_EXHAUSTED_CAUSE_APS_LIMIT, "ns-1")
		allowed, _, _ := state.Admit(issuing)
		require.True(t, allowed)
		e.SetThrottleAdmitted(true)

		// The flag moves after the release was committed, which is what an operator toggling
		// it mid-incident does.
		o := defaultThrottleOverrides()
		o.enabled = enabled
		flagged, _ := newTestThrottleStateWithEntries(o, state)
		e.throttleState = flagged

		e.reportThrottle(
			enumspb.RESOURCE_EXHAUSTED_CAUSE_BUSY_WORKFLOW,
			enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE,
		)

		_, rejections := throttleCounters(flagged, issuing)
		require.Zero(t, rejections, "enabled=%v: lock contention is not budget evidence", enabled)
	}
}
