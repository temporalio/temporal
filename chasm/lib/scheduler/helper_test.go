package scheduler_test

import (
	"context"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/testing/testlogger"
	"go.temporal.io/server/common/testing/testvars"
	legacyscheduler "go.temporal.io/server/service/worker/scheduler"
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
					WorkflowId:   "scheduled-wf",
					WorkflowType: &commonpb.WorkflowType{Name: "scheduled-wf-type"},
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

func newTestLibrary(logger log.Logger, specProcessor scheduler.SpecProcessor) *scheduler.Library {
	config := defaultConfig()
	invokerOpts := scheduler.InvokerTaskExecutorOptions{
		Config:         config,
		MetricsHandler: metrics.NoopMetricsHandler,
		BaseLogger:     logger,
		SpecProcessor:  specProcessor,
	}
	return scheduler.NewLibrary(
		nil,
		scheduler.NewSchedulerIdleTaskExecutor(scheduler.SchedulerIdleTaskExecutorOptions{
			Config: config,
		}),
		scheduler.NewGeneratorTaskExecutor(scheduler.GeneratorTaskExecutorOptions{
			Config:         config,
			MetricsHandler: metrics.NoopMetricsHandler,
			BaseLogger:     logger,
			SpecProcessor:  specProcessor,
		}),
		scheduler.NewInvokerExecuteTaskExecutor(invokerOpts),
		scheduler.NewInvokerProcessBufferTaskExecutor(invokerOpts),
		scheduler.NewBackfillerTaskExecutor(scheduler.BackfillerTaskExecutorOptions{
			Config:         config,
			MetricsHandler: metrics.NoopMetricsHandler,
			BaseLogger:     logger,
			SpecProcessor:  specProcessor,
		}),
	)
}

func setupSchedulerForTest(t *testing.T) (*scheduler.Scheduler, chasm.MutableContext, *chasm.Node) {
	nodeBackend := &chasm.MockNodeBackend{}
	logger := testlogger.NewTestLogger(t, testlogger.FailOnExpectedErrorOnly)
	nodePathEncoder := chasm.DefaultPathEncoder

	// Create mock spec processor with default expectations for setup.
	ctrl := gomock.NewController(t)
	specProcessor := scheduler.NewMockSpecProcessor(ctrl)
	specProcessor.EXPECT().ProcessTimeRange(
		gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(),
	).Return(&scheduler.ProcessedTimeRange{
		NextWakeupTime: time.Now().Add(time.Hour),
		LastActionTime: time.Now(),
	}, nil).AnyTimes()
	specProcessor.EXPECT().NextTime(gomock.Any(), gomock.Any()).Return(legacyscheduler.GetNextTimeResult{
		Next:    time.Now().Add(time.Hour),
		Nominal: time.Now().Add(time.Hour),
	}, nil).AnyTimes()

	registry := chasm.NewRegistry(logger)
	err := registry.Register(&chasm.CoreLibrary{})
	if err != nil {
		t.Fatalf("failed to register core library: %v", err)
	}
	err = registry.Register(newTestLibrary(logger, specProcessor))
	if err != nil {
		t.Fatalf("failed to register scheduler library: %v", err)
	}

	timeSource := clock.NewEventTimeSource()
	timeSource.Update(time.Now())

	tv := testvars.New(t)
	nodeBackend.HandleNextTransitionCount = func() int64 { return 2 }
	nodeBackend.HandleGetCurrentVersion = func() int64 { return 1 }
	nodeBackend.HandleGetWorkflowKey = tv.Any().WorkflowKey
	nodeBackend.HandleIsWorkflow = func() bool { return false }
	nodeBackend.HandleCurrentVersionedTransition = func() *persistencespb.VersionedTransition {
		return &persistencespb.VersionedTransition{
			NamespaceFailoverVersion: 1,
			TransitionCount:          1,
		}
	}

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
