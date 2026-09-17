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

// The control loop asks whether the releases this class issued are getting through, so a
// release refused by a second budget belongs to the class that issued it. This is the path
// the rescheduler actually drives: a permit set on the executable, then a rejection reported
// under a cause the permit was not issued for.
func TestExecutable_RejectionUnderAnotherBudgetChargesTheIssuingClass(t *testing.T) {
	ctrl := gomock.NewController(t)
	state, _ := newTestThrottleState(defaultThrottleOverrides())
	e := newThrottleTestExecutable(ctrl, state)

	issuing := NewThrottleKey(enumspb.RESOURCE_EXHAUSTED_CAUSE_APS_LIMIT, "ns-1", ctasks.PriorityHigh)
	allowed, permit, _ := state.Admit(issuing)
	require.True(t, allowed)
	state.Finish(permit, true)
	e.SetThrottlePermit(permit)

	e.reportThrottle(
		enumspb.RESOURCE_EXHAUSTED_CAUSE_PERSISTENCE_LIMIT,
		enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE,
	)

	releases, rejections := throttleCounters(state, issuing)
	require.Equal(t, int64(1), releases)
	require.Equal(t, int64(1), rejections, "the class that issued the release must see the loss")
}

// A contended workflow lock is not a shared budget. Releasing slower cannot clear it, so
// charging it drives the class toward the floor with no feedback that lifts it again.
func TestExecutable_BusyWorkflowDoesNotChargeTheIssuingClass(t *testing.T) {
	ctrl := gomock.NewController(t)
	state, _ := newTestThrottleState(defaultThrottleOverrides())
	e := newThrottleTestExecutable(ctrl, state)

	issuing := NewThrottleKey(enumspb.RESOURCE_EXHAUSTED_CAUSE_APS_LIMIT, "ns-1", ctasks.PriorityHigh)
	allowed, permit, _ := state.Admit(issuing)
	require.True(t, allowed)
	state.Finish(permit, true)
	e.SetThrottlePermit(permit)

	e.reportThrottle(
		enumspb.RESOURCE_EXHAUSTED_CAUSE_BUSY_WORKFLOW,
		enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE,
	)

	_, rejections := throttleCounters(state, issuing)
	require.Zero(t, rejections, "lock contention is not evidence about a shared budget")
}

// Classification runs even while the controller is off, so a task parked before the flag was
// turned on already knows which budget refused it and can be paced rather than released in
// one wave. Nothing may be reported to the controller from that path.
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

	key, known := e.ThrottleKey()
	require.True(t, known, "the task must know its class before the flag is turned on")
	require.Equal(t, enumspb.RESOURCE_EXHAUSTED_CAUSE_APS_LIMIT, key.Cause)
	require.Zero(t, throttleLen(state), "a disabled controller must track nothing")
}

// An ungoverned cause means this task is no longer waiting on a budget the controller paces,
// so it must leave the gated class rather than sit behind a rate it is not blocked on.
func TestExecutable_UngovernedCauseDropsTheKey(t *testing.T) {
	ctrl := gomock.NewController(t)
	state, _ := newTestThrottleState(defaultThrottleOverrides())
	e := newThrottleTestExecutable(ctrl, state)

	e.reportThrottle(
		enumspb.RESOURCE_EXHAUSTED_CAUSE_APS_LIMIT,
		enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE,
	)
	_, known := e.ThrottleKey()
	require.True(t, known)

	e.reportThrottle(
		enumspb.RESOURCE_EXHAUSTED_CAUSE_APS_LIMIT,
		enumspb.RESOURCE_EXHAUSTED_SCOPE_SYSTEM,
	)
	_, known = e.ThrottleKey()
	require.False(t, known, "a system scoped limit is not this namespace's budget")
}
