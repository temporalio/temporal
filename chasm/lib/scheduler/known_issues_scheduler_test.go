package scheduler_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler"
	"go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/testing/mockapi/workflowservicemock/v1"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestKnownIssue_CanceledWorkflowDoesNotPauseByDefault(t *testing.T) {
	sched, ctx, _ := setupSchedulerForTest(t)
	sched.Schedule.Policies.PauseOnFailure = true
	invoker := sched.Invoker.Get(ctx)
	invoker.BufferedStarts = []*schedulespb.BufferedStart{{
		RequestId:  "request",
		WorkflowId: "workflow",
		RunId:      "run",
		Attempt:    1,
	}}

	err := sched.HandleNexusCompletion(ctx, &persistencespb.ChasmNexusCompletion{
		RequestId: "request",
		Outcome: &persistencespb.ChasmNexusCompletion_Failure{
			Failure: &failurepb.Failure{
				FailureInfo: &failurepb.Failure_CanceledFailureInfo{
					CanceledFailureInfo: &failurepb.CanceledFailureInfo{},
				},
			},
		},
		CloseTime: timestamppb.New(time.Now()),
	})
	require.NoError(t, err)
	require.False(t, sched.Schedule.State.Paused,
		"CanceledTerminatedCountAsFailures defaults to false, so cancellation must not pause")
}

func TestKnownIssue_UpdateDuringMigrationIsAtomic(t *testing.T) {
	sched, ctx, _ := setupSchedulerForTest(t)
	visibility := sched.Visibility.Get(ctx)
	visibility.ReplaceCustomSearchAttributes(ctx, map[string]*commonpb.Payload{
		"KeywordField": payload.EncodeString("before"),
	})
	before := visibility.CustomSearchAttributes(ctx)
	_, err := sched.MigrateToWorkflow(ctx, &schedulerpb.MigrateToWorkflowRequest{})
	require.NoError(t, err)

	_, err = sched.Update(ctx, &schedulerpb.UpdateScheduleRequest{
		FrontendRequest: &workflowservice.UpdateScheduleRequest{
			Schedule: defaultSchedule(),
			SearchAttributes: &commonpb.SearchAttributes{IndexedFields: map[string]*commonpb.Payload{
				"KeywordField": payload.EncodeString("after"),
			}},
		},
	})
	require.ErrorIs(t, err, scheduler.ErrMigrationPending)
	require.Equal(t, before, sched.Visibility.Get(ctx).CustomSearchAttributes(ctx),
		"a rejected migration-pending update must not mutate visibility state")
}

func TestKnownIssue_TriggerRejectedDuringMigration(t *testing.T) {
	sched, ctx, _ := setupSchedulerForTest(t)
	_, err := sched.MigrateToWorkflow(ctx, &schedulerpb.MigrateToWorkflowRequest{})
	require.NoError(t, err)

	_, err = sched.Patch(ctx, &schedulerpb.PatchScheduleRequest{
		FrontendRequest: &workflowservice.PatchScheduleRequest{
			Patch: &schedulepb.SchedulePatch{
				TriggerImmediately: &schedulepb.TriggerImmediatelyRequest{},
			},
		},
	})
	require.ErrorIs(t, err, scheduler.ErrMigrationPending)
	require.Empty(t, sched.Backfillers, "rejected trigger must not create migration-unfenced work")
}

func TestKnownIssue_MigrationReconciliationUsesNormalFailureTransition(t *testing.T) {
	env := newTestEnv(t, withMockEngine())
	ctx := env.MutableContext()
	env.Scheduler.Schedule.Policies.PauseOnFailure = true
	invoker := env.Scheduler.Invoker.Get(ctx)
	invoker.BufferedStarts = []*schedulespb.BufferedStart{{
		RequestId:   "request",
		WorkflowId:  "workflow",
		RunId:       "run",
		Attempt:     1,
		HasCallback: false,
	}}

	historyClient := historyservicemock.NewMockHistoryServiceClient(env.Ctrl)
	frontendClient := workflowservicemock.NewMockWorkflowServiceClient(env.Ctrl)
	historyClient.EXPECT().
		DescribeWorkflowExecution(gomock.Any(), gomock.Any()).
		Return(&historyservice.DescribeWorkflowExecutionResponse{
			WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{
				Status:    enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
				CloseTime: timestamppb.New(env.TimeSource.Now()),
			},
		}, nil)
	env.ExpectReadComponent(ctx, env.Scheduler)
	env.ExpectUpdateComponent(ctx, env.Scheduler)
	handler := scheduler.NewSchedulerCallbacksTaskHandler(scheduler.SchedulerCallbacksTaskHandlerOptions{
		Config:         defaultConfig(),
		HistoryClient:  historyClient,
		FrontendClient: frontendClient,
	})

	err := handler.Execute(env.EngineContext(), chasm.ComponentRef{}, chasm.TaskAttributes{}, &schedulerpb.SchedulerCallbacksTask{})
	require.NoError(t, err)
	require.True(t, env.Scheduler.Schedule.State.Paused,
		"a reconciled failed workflow must apply the same pause-on-failure transition as a callback")
	require.NotNil(t, env.Scheduler.LastCompletionResult.Get(ctx).GetFailure(),
		"reconciliation must propagate the terminal failure")
}

func TestKnownIssue_LastCompletionUsesCloseTimeNotDeliveryOrder(t *testing.T) {
	sched, ctx, _ := setupSchedulerForTest(t)
	invoker := sched.Invoker.Get(ctx)
	invoker.BufferedStarts = []*schedulespb.BufferedStart{
		{RequestId: "older", WorkflowId: "older-wf", RunId: "older-run", Attempt: 1},
		{RequestId: "newer", WorkflowId: "newer-wf", RunId: "newer-run", Attempt: 1},
	}
	base := time.Now()
	newerPayload := &commonpb.Payload{Data: []byte("newer")}
	require.NoError(t, sched.HandleNexusCompletion(ctx, &persistencespb.ChasmNexusCompletion{
		RequestId: "newer",
		Outcome:   &persistencespb.ChasmNexusCompletion_Success{Success: newerPayload},
		CloseTime: timestamppb.New(base.Add(time.Minute)),
	}))
	require.NoError(t, sched.HandleNexusCompletion(ctx, &persistencespb.ChasmNexusCompletion{
		RequestId: "older",
		Outcome: &persistencespb.ChasmNexusCompletion_Success{
			Success: &commonpb.Payload{Data: []byte("older")},
		},
		CloseTime: timestamppb.New(base),
	}))

	require.Equal(t, newerPayload, sched.LastCompletionResult.Get(ctx).GetSuccess(),
		"late delivery of an older completion must not replace the newest close-time result")
}

func TestKnownIssue_RunningRecentActionsAreBounded(t *testing.T) {
	sched, ctx, _ := setupSchedulerForTest(t)
	invoker := sched.Invoker.Get(ctx)
	for i := range scheduler.RecentActionCount + 1 {
		invoker.BufferedStarts = append(invoker.BufferedStarts, &schedulespb.BufferedStart{
			RequestId:  fmt.Sprintf("request-%d", i),
			WorkflowId: fmt.Sprintf("workflow-%d", i),
			RunId:      fmt.Sprintf("run-%d", i),
			Attempt:    1,
		})
	}

	response, err := sched.Describe(ctx, &schedulerpb.DescribeScheduleRequest{}, newLegacySpecBuilder(0, 0))
	require.NoError(t, err)
	require.LessOrEqual(t, len(response.FrontendResponse.Info.RecentActions), scheduler.RecentActionCount,
		"running actions must not make RecentActions grow without bound")
}

func TestKnownIssue_IdleValidationUsesCurrentConfiguration(t *testing.T) {
	env := newTestEnv(t)
	now := env.TimeSource.Now()
	anchorLastEventTo(env.Scheduler, now.Add(-10*time.Minute))
	handler := newIdleHandler(20 * time.Minute)

	valid, err := handler.Validate(
		env.ReadContext(),
		env.Scheduler,
		chasm.TaskInvocation{TaskAttributes: chasm.TaskAttributes{ScheduledTime: now}},
		&schedulerpb.SchedulerIdleTask{IdleTimeTotal: durationpb.New(10 * time.Minute)},
	)
	require.NoError(t, err)
	require.False(t, valid, "an old idle task must be invalid after IdleTime increases")
}

func TestKnownIssue_DescribeUsesConfiguredDefaultCatchupWindow(t *testing.T) {
	const configuredDefault = 42 * time.Minute
	config := defaultConfig()
	config.Tweakables = func(string) scheduler.Tweakables {
		tweakables := scheduler.DefaultTweakables
		tweakables.DefaultCatchupWindow = configuredDefault
		return tweakables
	}
	env := newTestEnv(t, withSchedulerConfig(config))
	env.Scheduler.Schedule.Policies.CatchupWindow = nil

	response, err := env.Scheduler.Describe(
		env.ReadContext(),
		&schedulerpb.DescribeScheduleRequest{},
		newLegacySpecBuilder(0, 0),
	)
	require.NoError(t, err)
	require.Equal(t, configuredDefault,
		response.FrontendResponse.Schedule.Policies.CatchupWindow.AsDuration())
}

func TestKnownIssue_NegativeEventLogLimitsDoNotPanic(t *testing.T) {
	tweakables := scheduler.DefaultTweakables
	config := defaultConfig()
	config.Tweakables = func(string) scheduler.Tweakables {
		return tweakables
	}
	env := newTestEnv(t, withSchedulerConfig(config))
	ctx := env.MutableContext()
	tweakables.EventLogMaxEntries = -1
	tweakables.EventLogMaxMessageLen = -1

	require.NotPanics(t, func() {
		env.Scheduler.EventLog.Get(ctx).LogEvent(ctx, "event")
	})
}
