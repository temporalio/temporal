package scheduler_test

import (
	"context"
	"testing"
	"time"

	schedulepb "go.temporal.io/api/schedule/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/testing/testlogger"
	"go.temporal.io/server/common/testing/testvars"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/durationpb"
)

const (
	namespace   = "ns"
	namespaceID = "ns-id"
	scheduleID  = "sched-id"

	defaultInterval      = 1 * time.Minute
	defaultCatchupWindow = 5 * time.Minute
)

// defaultSchedule returns a protobuf definition for a schedule matching this
// package's other testing defaults.
func defaultSchedule() *schedulepb.Schedule {
	return &schedulepb.Schedule{
		Spec: &schedulepb.ScheduleSpec{
			Interval: []*schedulepb.IntervalSpec{
				{
					Interval: durationpb.New(defaultInterval),
					Phase:    durationpb.New(0),
				},
			},
		},
		Action: &schedulepb.ScheduleAction{
			Action: &schedulepb.ScheduleAction_StartWorkflow{
				StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
					WorkflowId: "scheduled-wf",
				},
			},
		},
		Policies: &schedulepb.SchedulePolicies{
			CatchupWindow: durationpb.New(defaultCatchupWindow),
		},
		State: &schedulepb.ScheduleState{
			Paused:           false,
			LimitedActions:   false,
			RemainingActions: 0,
		},
	}
}

func defaultConfig() *scheduler.Config {
	return &scheduler.Config{
		Tweakables: func(_ string) scheduler.Tweakables {
			return scheduler.DefaultTweakables
		},
		ServiceCallTimeout: func() time.Duration {
			return 5 * time.Second
		},
		RetryPolicy: func() backoff.RetryPolicy {
			return backoff.NewExponentialRetryPolicy(1 * time.Second)
		},
	}
}

func setupSchedulerForTest(t *testing.T) (*scheduler.Scheduler, chasm.MutableContext, *chasm.Node) {
	controller := gomock.NewController(t)
	nodeBackend := chasm.NewMockNodeBackend(controller)
	logger := testlogger.NewTestLogger(t, testlogger.FailOnExpectedErrorOnly)
	nodePathEncoder := chasm.DefaultPathEncoder

	registry := chasm.NewRegistry(logger)
	err := registry.Register(&scheduler.Library{})
	if err != nil {
		t.Fatalf("failed to register scheduler library: %v", err)
	}

	timeSource := clock.NewEventTimeSource()
	timeSource.Update(time.Now())

	tv := testvars.New(t)
	nodeBackend.EXPECT().NextTransitionCount().Return(int64(2)).AnyTimes()
	nodeBackend.EXPECT().GetCurrentVersion().Return(int64(1)).AnyTimes()
	nodeBackend.EXPECT().UpdateWorkflowStateStatus(gomock.Any(), gomock.Any()).Return(true, nil).AnyTimes()
	nodeBackend.EXPECT().GetWorkflowKey().Return(tv.Any().WorkflowKey()).AnyTimes()
	nodeBackend.EXPECT().IsWorkflow().Return(false).AnyTimes()
	currentVT := &persistencespb.VersionedTransition{
		NamespaceFailoverVersion: 1,
		TransitionCount:          1,
	}
	nodeBackend.EXPECT().CurrentVersionedTransition().Return(currentVT).AnyTimes()
	nodeBackend.EXPECT().AddTasks(gomock.Any()).Return().AnyTimes()

	node := chasm.NewEmptyTree(registry, timeSource, nodeBackend, nodePathEncoder, logger)
	ctx := chasm.NewMutableContext(context.Background(), node)
	sched := scheduler.NewScheduler(ctx, namespace, namespaceID, scheduleID, defaultSchedule(), nil)
	node.SetRootComponent(sched)
	_, err = node.CloseTransaction()
	if err != nil {
		t.Fatalf("failed to close initial transaction: %v", err)
	}

	ctx = chasm.NewMutableContext(context.Background(), node)

	return sched, ctx, node
}
