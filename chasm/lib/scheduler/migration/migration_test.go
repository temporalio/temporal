package migration

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	schedulerpb "go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func newTestSchedule() *schedulepb.Schedule {
	return &schedulepb.Schedule{
		Spec: &schedulepb.ScheduleSpec{
			Interval: []*schedulepb.IntervalSpec{{Interval: durationpb.New(time.Minute)}},
		},
		Action: &schedulepb.ScheduleAction{
			Action: &schedulepb.ScheduleAction_StartWorkflow{
				StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
					WorkflowId:   "test-wf",
					WorkflowType: &commonpb.WorkflowType{Name: "test-wf-type"},
				},
			},
		},
		Policies: &schedulepb.SchedulePolicies{CatchupWindow: durationpb.New(5 * time.Minute)},
		State:    &schedulepb.ScheduleState{},
	}
}

func TestLegacyToSchedulerMigrationState(t *testing.T) {
	now := time.Now().UTC()
	state := &schedulespb.InternalState{
		Namespace:         "test-ns",
		NamespaceId:       "test-ns-id",
		ScheduleId:        "test-sched-id",
		LastProcessedTime: timestamppb.New(now),
		ConflictToken:     42,
		BufferedStarts: []*schedulespb.BufferedStart{
			{
				NominalTime:   timestamppb.New(now),
				ActualTime:    timestamppb.New(now.Add(time.Second)),
				OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
			},
		},
		OngoingBackfills: []*schedulepb.BackfillRequest{
			{
				StartTime:     timestamppb.New(now.Add(-time.Hour)),
				EndTime:       timestamppb.New(now),
				OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
			},
		},
		LastCompletionResult: &commonpb.Payloads{
			Payloads: []*commonpb.Payload{{Data: []byte("result")}},
		},
		ContinuedFailure: &failurepb.Failure{Message: "last failure"},
	}
	info := &schedulepb.ScheduleInfo{
		ActionCount: 5,
		RunningWorkflows: []*commonpb.WorkflowExecution{
			{WorkflowId: "wf-1", RunId: "run-1"},
		},
		RecentActions: []*schedulepb.ScheduleActionResult{
			{
				ScheduleTime:        timestamppb.New(now.Add(-time.Hour)),
				ActualTime:          timestamppb.New(now.Add(-time.Millisecond)),
				StartWorkflowResult: &commonpb.WorkflowExecution{WorkflowId: "wf-2", RunId: "run-2"},
				StartWorkflowStatus: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
			},
		},
	}
	searchAttrs := map[string]*commonpb.Payload{"Attr": {Data: []byte("value")}}
	memo := map[string]*commonpb.Payload{"Memo": {Data: []byte("memo")}}

	migrationState := LegacyToSchedulerMigrationState(newTestSchedule(), info, state, searchAttrs, memo, now)

	// Scheduler state
	require.NotNil(t, migrationState)
	require.NotNil(t, migrationState.SchedulerState)
	require.False(t, migrationState.SchedulerState.Schedule.State.Paused, "schedule should preserve unpaused state")
	require.Equal(t, "test-ns", migrationState.SchedulerState.Namespace)
	require.Equal(t, "test-ns-id", migrationState.SchedulerState.NamespaceId)
	require.Equal(t, "test-sched-id", migrationState.SchedulerState.ScheduleId)
	require.Equal(t, int64(42), migrationState.SchedulerState.ConflictToken)
	require.False(t, migrationState.SchedulerState.Closed)

	// Generator state
	require.NotNil(t, migrationState.GeneratorState)
	require.Equal(t, now, migrationState.GeneratorState.LastProcessedTime.AsTime())

	// Invoker state - buffered starts + running workflows + completed
	require.NotNil(t, migrationState.InvokerState)
	require.Len(t, migrationState.InvokerState.BufferedStarts, 3) // 1 buffered + 1 running + 1 completed

	var buffered, running, completed int
	for _, start := range migrationState.InvokerState.BufferedStarts {
		require.NotEmpty(t, start.RequestId)
		require.NotEmpty(t, start.WorkflowId)
		switch {
		case start.RunId == "" && start.Completed == nil:
			buffered++
		case start.RunId != "" && start.Completed == nil:
			running++
			require.Equal(t, "wf-1", start.WorkflowId)
			require.Equal(t, "run-1", start.RunId)
		case start.Completed != nil:
			completed++
			require.Equal(t, "wf-2", start.WorkflowId)
			require.Equal(t, "run-2", start.RunId)
			require.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, start.Completed.Status)
		default:
			t.Fatalf("unexpected buffered start state: RunId=%q, Completed=%v", start.RunId, start.Completed)
		}
	}
	require.Equal(t, 1, buffered, "expected 1 pending buffered start")
	require.Equal(t, 1, running, "expected 1 running workflow")
	require.Equal(t, 1, completed, "expected 1 completed workflow")

	// Backfillers
	require.Len(t, migrationState.Backfillers, 1)
	for id, backfiller := range migrationState.Backfillers {
		require.Equal(t, id, backfiller.BackfillId)
		require.NotNil(t, backfiller.GetBackfillRequest())
		require.Equal(t, now.Add(-time.Hour), backfiller.GetBackfillRequest().StartTime.AsTime())
	}

	// Last completion result
	require.NotNil(t, migrationState.LastCompletionResult)
	require.Equal(t, []byte("result"), migrationState.LastCompletionResult.Success.Data)
	require.Equal(t, "last failure", migrationState.LastCompletionResult.Failure.Message)

	// Search attributes and memo
	require.Equal(t, searchAttrs, migrationState.SearchAttributes)
	require.Equal(t, memo, migrationState.Memo)
}

func TestCHASMToLegacyStartScheduleArgs(t *testing.T) {
	now := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)
	scheduler := &schedulerpb.SchedulerState{
		Namespace:     "ns",
		NamespaceId:   "ns-id",
		ScheduleId:    "sched-id",
		ConflictToken: 7,
		Schedule:      newTestSchedule(),
		Info:          &schedulepb.ScheduleInfo{ActionCount: 12},
	}
	generator := &schedulerpb.GeneratorState{LastProcessedTime: timestamppb.New(now.Add(-time.Minute))}
	invoker := &schedulerpb.InvokerState{
		BufferedStarts: []*schedulespb.BufferedStart{
			{
				NominalTime:   timestamppb.New(now.Add(-10 * time.Minute)),
				ActualTime:    timestamppb.New(now.Add(-9 * time.Minute)),
				OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
			},
			{
				NominalTime: timestamppb.New(now.Add(-5 * time.Minute)),
				ActualTime:  timestamppb.New(now.Add(-5 * time.Minute)),
				StartTime:   timestamppb.New(now.Add(-5 * time.Minute)),
				WorkflowId:  "wf-running",
				RunId:       "run-running",
			},
			{
				NominalTime: timestamppb.New(now.Add(-20 * time.Minute)),
				ActualTime:  timestamppb.New(now.Add(-20 * time.Minute)),
				StartTime:   timestamppb.New(now.Add(-20 * time.Minute)),
				WorkflowId:  "wf-done",
				RunId:       "run-done",
				Completed: &schedulespb.CompletedResult{
					Status:    enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
					CloseTime: timestamppb.New(now.Add(-10 * time.Minute)),
				},
			},
		},
	}
	backfillProgress := timestamppb.New(now.Add(-2 * time.Hour))
	backfillers := map[string]*schedulerpb.BackfillerState{
		"bf-1": {
			BackfillId:        "bf-1",
			LastProcessedTime: backfillProgress,
			Attempt:           2,
			Request: &schedulerpb.BackfillerState_BackfillRequest{
				BackfillRequest: &schedulepb.BackfillRequest{
					StartTime:     timestamppb.New(now.Add(-6 * time.Hour)),
					EndTime:       timestamppb.New(now.Add(-1 * time.Hour)),
					OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
				},
			},
		},
		"trigger-1": {
			BackfillId:        "trigger-1",
			LastProcessedTime: timestamppb.New(now.Add(-30 * time.Second)),
			Request: &schedulerpb.BackfillerState_TriggerRequest{
				TriggerRequest: &schedulepb.TriggerImmediatelyRequest{
					OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
				},
			},
		},
	}
	lastCompletion := &schedulerpb.LastCompletionResult{
		Success: &commonpb.Payload{Data: []byte("ok")},
		Failure: &failurepb.Failure{Message: "last failure"},
	}

	migrationState := &schedulerpb.SchedulerMigrationState{
		SchedulerState:       scheduler,
		GeneratorState:       generator,
		InvokerState:         invoker,
		Backfillers:          backfillers,
		LastCompletionResult: lastCompletion,
	}

	args := SchedulerMigrationStateToLegacyStartScheduleArgs(migrationState, now)

	require.Equal(t, "ns-id", args.State.NamespaceId)
	require.Equal(t, "sched-id", args.State.ScheduleId)
	require.Equal(t, int64(7), args.State.ConflictToken)
	require.Equal(t, generator.LastProcessedTime.AsTime(), args.State.LastProcessedTime.AsTime())

	require.Len(t, args.Info.RunningWorkflows, 1)
	require.Equal(t, "wf-running", args.Info.RunningWorkflows[0].WorkflowId)
	require.Equal(t, "run-running", args.Info.RunningWorkflows[0].RunId)

	require.Len(t, args.Info.RecentActions, 2)
	require.Len(t, args.State.BufferedStarts, 2) // pending + trigger
	require.Len(t, args.State.OngoingBackfills, 1)
	require.Equal(t, backfillProgress.AsTime(), args.State.OngoingBackfills[0].StartTime.AsTime())

	require.NotNil(t, args.State.LastCompletionResult)
	require.Equal(t, []byte("ok"), args.State.LastCompletionResult.Payloads[0].Data)
	require.Equal(t, "last failure", args.State.ContinuedFailure.Message)

	var triggerFound bool
	for _, start := range args.State.BufferedStarts {
		if start.Manual {
			triggerFound = true
		}
	}
	require.True(t, triggerFound)
}
