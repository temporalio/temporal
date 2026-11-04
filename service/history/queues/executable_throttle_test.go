package queues

import (
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	ctasks "go.temporal.io/server/common/tasks"
	"go.temporal.io/server/service/history/tasks"
	"go.uber.org/mock/gomock"
)

// ThrottleKeyOf exposes the budget a task is parked on to the queues_test package, which
// asserts it but cannot reach the field.
func ThrottleKeyOf(e Executable) ThrottleKey {
	return e.(*executableImpl).throttleKey
}

func newThrottleTestExecutable(ctrl *gomock.Controller, state *ThrottleState) *executableImpl {
	task := tasks.NewMockTask(ctrl)
	task.EXPECT().GetNamespaceID().Return("ns-1-id").AnyTimes()
	registry := namespace.NewMockRegistry(ctrl)
	registry.EXPECT().GetNamespaceName(namespace.ID("ns-1-id")).Return(namespace.Name("ns-1"), nil).AnyTimes()
	return &executableImpl{
		Task:                task,
		priority:            ctasks.PriorityHigh,
		throttleState:       state,
		namespaceRegistry:   registry,
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
	issuing := NewThrottleKey(enumspb.RESOURCE_EXHAUSTED_CAUSE_APS_LIMIT, "ns-1", 0)
	require.Equal(t, issuing, e.throttleKey)

	// ...and the rescheduler then releases it from that class.
	allowed, _ := state.Admit(issuing)
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
	issuing := NewThrottleKey(enumspb.RESOURCE_EXHAUSTED_CAUSE_APS_LIMIT, "ns-1", 0)
	require.Equal(t, issuing, e.throttleKey)

	// ...and the rescheduler then releases it from that class.
	allowed, _ := state.Admit(issuing)
	require.True(t, allowed)
	e.SetThrottleAdmitted(true)

	e.reportThrottle(
		enumspb.RESOURCE_EXHAUSTED_CAUSE_BUSY_WORKFLOW,
		enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE,
	)

	_, rejections := throttleCounters(state, issuing)
	require.Zero(t, rejections, "lock contention is not evidence about a shared budget")
}

// With the controller off a throttled task stays on its plain task channel.
func TestExecutable_DoesNotClassifyWhileTheControllerIsOff(t *testing.T) {
	ctrl := gomock.NewController(t)
	o := defaultThrottleOverrides()
	o.enabled = false
	state, _ := newTestThrottleState(o)
	e := newThrottleTestExecutable(ctrl, state)

	e.reportThrottle(
		enumspb.RESOURCE_EXHAUSTED_CAUSE_APS_LIMIT,
		enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE,
	)

	require.Equal(t, ThrottleKey{}, e.throttleKey)
	require.Zero(t, throttleLen(state))
}

// A namespace the registry cannot name has no budget to pace against.
func TestExecutable_UnresolvedNamespaceStaysUngoverned(t *testing.T) {
	ctrl := gomock.NewController(t)
	state, _ := newTestThrottleState(defaultThrottleOverrides())
	e := newThrottleTestExecutable(ctrl, state)
	registry := namespace.NewMockRegistry(ctrl)
	registry.EXPECT().GetNamespaceName(gomock.Any()).
		Return(namespace.EmptyName, serviceerror.NewNamespaceNotFound("ns-1-id")).AnyTimes()
	e.namespaceRegistry = registry

	e.reportThrottle(
		enumspb.RESOURCE_EXHAUSTED_CAUSE_APS_LIMIT,
		enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE,
	)

	require.Equal(t, ThrottleKey{}, e.throttleKey)
	require.Zero(t, throttleLen(state))
}
