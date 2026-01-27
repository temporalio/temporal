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

func testSchedule() *schedulepb.Schedule {
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

func TestLegacyToMigrateScheduleRequest(t *testing.T) {
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
	}
	searchAttrs := map[string]*commonpb.Payload{"Attr": {Data: []byte("value")}}
	memo := map[string]*commonpb.Payload{"Memo": {Data: []byte("memo")}}

	req := LegacyToMigrateScheduleRequest(testSchedule(), info, state, searchAttrs, memo, now)

	// Basic fields
	require.Equal(t, "test-ns-id", req.NamespaceId)
	require.Equal(t, "test-ns", req.Namespace)
	require.Equal(t, "test-sched-id", req.ScheduleId)

	// Scheduler state
	require.NotNil(t, req.MigrationState)
	require.NotNil(t, req.MigrationState.SchedulerState)
	require.Equal(t, int64(42), req.MigrationState.SchedulerState.ConflictToken)
	require.False(t, req.MigrationState.SchedulerState.Closed)

	// Generator state
	require.NotNil(t, req.MigrationState.GeneratorState)
	require.Equal(t, now, req.MigrationState.GeneratorState.LastProcessedTime.AsTime())

	// Invoker state - buffered starts + running workflows
	require.NotNil(t, req.MigrationState.InvokerState)
	require.Len(t, req.MigrationState.InvokerState.BufferedStarts, 2) // 1 buffered + 1 running
	for _, start := range req.MigrationState.InvokerState.BufferedStarts {
		require.NotEmpty(t, start.RequestId)
		require.NotEmpty(t, start.WorkflowId)
	}

	// Backfillers
	require.Len(t, req.MigrationState.Backfillers, 1)
	for id, backfiller := range req.MigrationState.Backfillers {
		require.Equal(t, id, backfiller.BackfillId)
		require.NotNil(t, backfiller.GetBackfillRequest())
	}

	// Last completion result
	require.NotNil(t, req.MigrationState.LastCompletionResult)
	require.Equal(t, []byte("result"), req.MigrationState.LastCompletionResult.Success.Data)
	require.Equal(t, "last failure", req.MigrationState.LastCompletionResult.Failure.Message)

	// Search attributes and memo
	require.Equal(t, searchAttrs, req.MigrationState.SearchAttributes)
	require.Equal(t, memo, req.MigrationState.Memo)
}

func TestSchedulerMigrationStateToMigrateScheduleRequest(t *testing.T) {
	now := time.Now().UTC()
	scheduler := &schedulerpb.SchedulerState{
		Namespace:     "ns",
		NamespaceId:   "ns-id",
		ScheduleId:    "sched-id",
		ConflictToken: 42,
	}
	generator := &schedulerpb.GeneratorState{LastProcessedTime: timestamppb.New(now)}
	invoker := &schedulerpb.InvokerState{}

	migrationState := CHASMToSchedulerMigrationState(scheduler, generator, invoker, nil, nil, nil, nil)
	req := SchedulerMigrationStateToMigrateScheduleRequest(migrationState)

	require.Equal(t, "ns-id", req.NamespaceId)
	require.Equal(t, "ns", req.Namespace)
	require.Equal(t, "sched-id", req.ScheduleId)
	require.Equal(t, int64(42), req.MigrationState.SchedulerState.ConflictToken)
	require.NotNil(t, req.MigrationState.GeneratorState)
	require.NotNil(t, req.MigrationState.InvokerState)
}

func TestCHASMToLegacyStartScheduleArgs(t *testing.T) {
	now := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)
	scheduler := &schedulerpb.SchedulerState{
		Namespace:     "ns",
		NamespaceId:   "ns-id",
		ScheduleId:    "sched-id",
		ConflictToken: 7,
		Schedule:      testSchedule(),
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
