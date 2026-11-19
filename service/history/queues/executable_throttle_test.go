package queues

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	ctasks "go.temporal.io/server/common/tasks"
	"go.temporal.io/server/common/telemetry"
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
	issuing := NewThrottleKey(enumspb.RESOURCE_EXHAUSTED_CAUSE_APS_LIMIT, "ns-1")
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
	issuing := NewThrottleKey(enumspb.RESOURCE_EXHAUSTED_CAUSE_APS_LIMIT, "ns-1")
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

func newOutboundTestExecutable(t *testing.T, timeSource clock.TimeSource, source, destination, group string) (*executableImpl, *MockExecutor) {
	t.Helper()
	ctrl := gomock.NewController(t)
	registry := namespace.NewMockRegistry(ctrl)
	registry.EXPECT().GetNamespaceName(gomock.Any()).Return(namespace.Name(source+"-name"), nil).AnyTimes()
	registry.EXPECT().GetNamespaceByID(gomock.Any()).Return(namespace.NewNamespaceForTest(
		&persistencespb.NamespaceInfo{Id: source, Name: source + "-name"}, nil, false, nil, 0,
	), nil).AnyTimes()
	clusterMetadata := cluster.NewMockMetadata(ctrl)
	clusterMetadata.EXPECT().GetCurrentClusterName().Return("active").AnyTimes()
	executor := NewMockExecutor(ctrl)
	executable := NewExecutable(0, &tasks.StateMachineOutboundTask{
		StateMachineTask: tasks.StateMachineTask{
			WorkflowKey:         definition.NewWorkflowKey(source, "workflow", "run"),
			Info:                &persistencespb.StateMachineTaskInfo{Type: group},
			VisibilityTimestamp: timeSource.Now(),
		},
		Destination: destination,
	}, executor, nil, nil, NewNoopPriorityAssigner(), timeSource, registry, clusterMetadata, nil,
		GetTaskTypeTagValue, log.NewTestLogger(), metrics.NoopMetricsHandler, telemetry.NoopTracer)
	return executable.(*executableImpl), executor
}

func outboundTestChannel(e Executable) TaskChannelKey {
	return TaskChannelKey{NamespaceID: e.GetNamespaceID(), Priority: e.GetPriority()}
}

func TestOutboundCallerRPSRetainsTaskAndPacesRetries(t *testing.T) {
	t.Parallel()
	options := defaultThrottleOverrides()
	options.initialRate = 1
	state, timeSource := newTestThrottleState(options)
	timeSource.Advance(time.Second)
	scheduler := NewMockScheduler(gomock.NewController(t))
	scheduler.EXPECT().TaskChannelKeyFn().Return(outboundTestChannel).AnyTimes()
	rescheduler := NewRescheduler(scheduler, timeSource, log.NewTestLogger(), metrics.NoopMetricsHandler, state)
	executable, executor := newOutboundTestExecutable(t, timeSource, "caller", "endpoint", "invocation")
	executable.throttleState = state
	executable.scheduler, executable.rescheduler = scheduler, rescheduler
	executor.EXPECT().Execute(gomock.Any(), executable).Return(ExecuteResponse{
		ExecutionErr: &serviceerror.ResourceExhausted{
			Cause: enumspb.RESOURCE_EXHAUSTED_CAUSE_CALLER_RPS_LIMIT,
			Scope: enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE,
		},
	}).Times(2)

	var retryTime time.Time
	for attempt := range 2 {
		err := executable.Execute()
		require.Error(t, err)
		err = executable.HandleErr(err)
		executable.Nack(err)
		require.Equal(t, ctasks.TaskStatePending, executable.State())
		require.Equal(t, 1, rescheduler.Len())
		for _, pq := range rescheduler.pqMap {
			retryTime = pq.Peek().rescheduleTime
			require.True(t, retryTime.After(timeSource.Now()))
		}
		rescheduler.reschedule()
		require.Equal(t, 1, rescheduler.Len())
		if attempt == 0 {
			timeSource.Update(retryTime)
			scheduler.EXPECT().TrySubmit(executable).Return(true)
			rescheduler.reschedule()
			require.Zero(t, rescheduler.Len())
		}
	}
	key := executable.throttleKey
	require.Equal(t, ThrottleKey{
		Cause:         enumspb.RESOURCE_EXHAUSTED_CAUSE_CALLER_RPS_LIMIT,
		NamespaceName: "caller-name", Destination: "endpoint",
	}, key)
	releases, rejections := throttleCounters(state, key)
	require.Equal(t, int64(1), releases)
	require.Equal(t, int64(1), rejections)
	timeSource.Update(retryTime)
	require.True(t, admitOK(state, key))
	rescheduler.reschedule()
	require.Equal(t, 1, rescheduler.Len())
	timeSource.Advance(time.Second)
	scheduler.EXPECT().TrySubmit(executable).Return(true)
	rescheduler.reschedule()
	require.Zero(t, rescheduler.Len())
}
