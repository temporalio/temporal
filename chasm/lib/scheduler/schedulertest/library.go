// Package schedulertest provides a lifecycle-driving harness for the CHASM
// scheduler. Unlike the per-handler unit tests in package scheduler_test, this
// package builds a real component on top of [chasmtest.Engine] and steps it
// forward through every task it schedules for itself, so that invariants can be
// asserted across sequences of transitions (e.g. "the scheduler never gets
// stuck without a task to drive the next state").
//
// It deliberately lives in a non-test package so that property tests
// (pgregory.net/rapid) and the V1<->V2 migration round-trip test can import the
// driver. The helpers below mirror chasm/lib/scheduler/helper_test.go, but are
// exported and accept the mock clients the side-effect task handlers need.
package schedulertest

import (
	"time"

	commonpb "go.temporal.io/api/common/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/workflowservice/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/server/chasm/lib/scheduler"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/searchattribute"
	legacyscheduler "go.temporal.io/server/service/worker/scheduler"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/durationpb"
)

const (
	Namespace   = "ns"
	NamespaceID = "ns-id"
	ScheduleID  = "sched-id"

	DefaultInterval      = 1 * time.Minute
	DefaultCatchupWindow = 5 * time.Minute
)

// DefaultSchedule returns a schedule with a single 1-minute interval and a
// 5-minute catchup window, matching the package's other defaults.
func DefaultSchedule() *schedulepb.Schedule {
	return &schedulepb.Schedule{
		Spec: &schedulepb.ScheduleSpec{
			Interval: []*schedulepb.IntervalSpec{
				{
					Interval: durationpb.New(DefaultInterval),
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
			CatchupWindow: durationpb.New(DefaultCatchupWindow),
		},
		State: &schedulepb.ScheduleState{
			Paused:           false,
			LimitedActions:   false,
			RemainingActions: 0,
		},
	}
}

// DefaultConfig returns a scheduler config wired with the package defaults.
func DefaultConfig() *scheduler.Config {
	return &scheduler.Config{
		Tweakables: func(_ string) scheduler.Tweakables {
			return scheduler.DefaultTweakables
		},
		ServiceCallTimeout: func() time.Duration {
			return 5 * time.Second
		},
		EncodeInternalTokenWithEnvelope: func(string) bool {
			return true
		},
		RetryPolicy: func() backoff.RetryPolicy {
			return backoff.NewExponentialRetryPolicy(1 * time.Second)
		},
	}
}

// NewRealSpecProcessor builds a real SpecProcessor with no-op metrics, matching
// production spec processing so generated buffered starts and next-times are
// realistic.
func NewRealSpecProcessor(ctrl *gomock.Controller, logger log.Logger) scheduler.SpecProcessor {
	mockMetrics := metrics.NewMockHandler(ctrl)
	mockMetrics.EXPECT().Counter(gomock.Any()).Return(metrics.NoopCounterMetricFunc).AnyTimes()
	mockMetrics.EXPECT().WithTags(gomock.Any()).Return(mockMetrics).AnyTimes()
	mockMetrics.EXPECT().Timer(gomock.Any()).Return(metrics.NoopTimerMetricFunc).AnyTimes()

	return scheduler.NewSpecProcessor(
		DefaultConfig(),
		mockMetrics,
		logger,
		legacyscheduler.NewSpecBuilder(
		dynamicconfig.SchedulerSpecWarnIterations.Get(dynamicconfig.NewNoopCollection()),
		dynamicconfig.SchedulerSpecMaxIterations.Get(dynamicconfig.NewNoopCollection()),
	),
	)
}

// NewTestLibrary builds a scheduler Library with all task handlers wired to the
// supplied spec processor and mock clients. The clients back the side-effect
// task handlers (execute/callbacks/migrate); pass mocks whose StartWorkflow /
// Terminate / Cancel expectations the test controls.
func NewTestLibrary(
	config *scheduler.Config,
	logger log.Logger,
	specProcessor scheduler.SpecProcessor,
	frontendClient workflowservice.WorkflowServiceClient,
	historyClient resource.HistoryClient,
) *scheduler.Library {
	specBuilder := legacyscheduler.NewSpecBuilder(
		dynamicconfig.SchedulerSpecWarnIterations.Get(dynamicconfig.NewNoopCollection()),
		dynamicconfig.SchedulerSpecMaxIterations.Get(dynamicconfig.NewNoopCollection()),
	)
	invokerOpts := scheduler.InvokerTaskHandlerOptions{
		Config:         config,
		MetricsHandler: metrics.NoopMetricsHandler,
		BaseLogger:     logger,
		SpecProcessor:  specProcessor,
		HistoryClient:  historyClient,
		FrontendClient: frontendClient,
	}
	return scheduler.NewLibrary(
		config,
		nil,
		scheduler.NewSchedulerIdleTaskHandler(scheduler.SchedulerIdleTaskHandlerOptions{
			Config:         config,
			MetricsHandler: metrics.NoopMetricsHandler,
			BaseLogger:     logger,
		}),
		scheduler.NewSchedulerCallbacksTaskHandler(scheduler.SchedulerCallbacksTaskHandlerOptions{
			Config:         config,
			HistoryClient:  historyClient,
			FrontendClient: frontendClient,
		}),
		scheduler.NewGeneratorTaskHandler(scheduler.GeneratorTaskHandlerOptions{
			Config:         config,
			MetricsHandler: metrics.NoopMetricsHandler,
			BaseLogger:     logger,
			SpecProcessor:  specProcessor,
			SpecBuilder:    specBuilder,
		}),
		scheduler.NewInvokerExecuteTaskHandler(invokerOpts),
		scheduler.NewInvokerProcessBufferTaskHandler(invokerOpts),
		scheduler.NewBackfillerTaskHandler(scheduler.BackfillerTaskHandlerOptions{
			Config:         config,
			MetricsHandler: metrics.NoopMetricsHandler,
			BaseLogger:     logger,
			SpecProcessor:  specProcessor,
		}),
		scheduler.NewSchedulerMigrateToWorkflowTaskHandler(scheduler.SchedulerMigrateToWorkflowTaskHandlerOptions{
			Config:           config,
			MetricsHandler:   metrics.NoopMetricsHandler,
			BaseLogger:       logger,
			HistoryClient:    historyClient,
			SaMapperProvider: searchattribute.NewTestMapperProvider(nil),
		}),
	)
}
