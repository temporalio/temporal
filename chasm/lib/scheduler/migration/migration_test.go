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

const (
	testNamespace   = "test-ns"
	testNamespaceID = "test-ns-id"
	testScheduleID  = "test-sched-id"
)

func testSchedule() *schedulepb.Schedule {
	return &schedulepb.Schedule{
		Spec: &schedulepb.ScheduleSpec{
			Interval: []*schedulepb.IntervalSpec{
				{
					Interval: durationpb.New(time.Minute),
					Phase:    durationpb.New(0),
				},
			},
		},
		Action: &schedulepb.ScheduleAction{
			Action: &schedulepb.ScheduleAction_StartWorkflow{
				StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
					WorkflowId:   "test-wf",
					WorkflowType: &commonpb.WorkflowType{Name: "test-wf-type"},
				},
			},
		},
		Policies: &schedulepb.SchedulePolicies{
			CatchupWindow: durationpb.New(5 * time.Minute),
		},
		State: &schedulepb.ScheduleState{},
	}
}

func testLegacyState() *LegacyState {
	now := time.Now().UTC()
	return &LegacyState{
		Schedule: testSchedule(),
		Info: &schedulepb.ScheduleInfo{
			ActionCount: 5,
		},
		State: &schedulespb.InternalState{
			Namespace:         testNamespace,
			NamespaceId:       testNamespaceID,
			ScheduleId:        testScheduleID,
			LastProcessedTime: timestamppb.New(now),
			ConflictToken:     1,
		},
	}
}

func TestLegacyToCHASM_BasicState(t *testing.T) {
	v1 := testLegacyState()
	migrationTime := time.Now().UTC()

	v2, err := LegacyToCHASM(v1, migrationTime)
	require.NoError(t, err)
	require.NotNil(t, v2)

	// Verify scheduler state
	require.Equal(t, testNamespace, v2.Scheduler.Namespace)
	require.Equal(t, testNamespaceID, v2.Scheduler.NamespaceId)
	require.Equal(t, testScheduleID, v2.Scheduler.ScheduleId)
	require.Equal(t, int64(1), v2.Scheduler.ConflictToken)
	require.False(t, v2.Scheduler.Closed)

	// Verify generator state
	require.NotNil(t, v2.Generator)
	require.Equal(t, v1.State.LastProcessedTime.AsTime(), v2.Generator.LastProcessedTime.AsTime())

	// Verify invoker state
	require.NotNil(t, v2.Invoker)
}

func TestCHASMToLegacy_BasicState(t *testing.T) {
	now := time.Now().UTC()
	v2 := &CHASMState{
		Scheduler: &schedulerpb.SchedulerState{
			Schedule:      testSchedule(),
			Info:          &schedulepb.ScheduleInfo{ActionCount: 10},
			Namespace:     testNamespace,
			NamespaceId:   testNamespaceID,
			ScheduleId:    testScheduleID,
			ConflictToken: 2,
		},
		Generator: &schedulerpb.GeneratorState{
			LastProcessedTime: timestamppb.New(now),
		},
		Invoker: &schedulerpb.InvokerState{
			BufferedStarts: []*schedulespb.BufferedStart{},
		},
	}
	migrationTime := time.Now().UTC()

	v1, err := CHASMToLegacy(v2, migrationTime)
	require.NoError(t, err)
	require.NotNil(t, v1)

	// Verify state
	require.Equal(t, testNamespace, v1.State.Namespace)
	require.Equal(t, testNamespaceID, v1.State.NamespaceId)
	require.Equal(t, testScheduleID, v1.State.ScheduleId)
	require.Equal(t, int64(2), v1.State.ConflictToken)
	require.Equal(t, now, v1.State.LastProcessedTime.AsTime())
	require.True(t, v1.State.NeedRefresh) // Should be set for V1 to refresh watchers
}

func TestLegacyToCHASM_BufferedStarts(t *testing.T) {
	v1 := testLegacyState()
	now := time.Now().UTC()

	// Add buffered starts without V2-specific fields
	v1.State.BufferedStarts = []*schedulespb.BufferedStart{
		{
			NominalTime:   timestamppb.New(now),
			ActualTime:    timestamppb.New(now.Add(time.Second)),
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
			Manual:        false,
		},
		{
			NominalTime:   timestamppb.New(now.Add(time.Minute)),
			ActualTime:    timestamppb.New(now.Add(time.Minute + time.Second)),
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
			Manual:        true,
		},
	}

	v2, err := LegacyToCHASM(v1, now)
	require.NoError(t, err)

	// Verify buffered starts have V2-specific fields populated
	require.Len(t, v2.Invoker.BufferedStarts, 2)
	for _, start := range v2.Invoker.BufferedStarts {
		require.NotEmpty(t, start.RequestId, "request_id should be populated")
		require.NotEmpty(t, start.WorkflowId, "workflow_id should be populated")
		require.Equal(t, int64(0), start.Attempt, "attempt should be 0")
		require.Nil(t, start.BackoffTime, "backoff_time should be nil")
	}

	// Verify buffered starts preserved
	require.Len(t, v2.Invoker.BufferedStarts, 2)
}

func TestCHASMToLegacy_BufferedStarts(t *testing.T) {
	now := time.Now().UTC()
	v2 := &CHASMState{
		Scheduler: &schedulerpb.SchedulerState{
			Schedule:      testSchedule(),
			Info:          &schedulepb.ScheduleInfo{},
			Namespace:     testNamespace,
			NamespaceId:   testNamespaceID,
			ScheduleId:    testScheduleID,
			ConflictToken: 1,
		},
		Generator: &schedulerpb.GeneratorState{
			LastProcessedTime: timestamppb.New(now),
		},
		Invoker: &schedulerpb.InvokerState{
			BufferedStarts: []*schedulespb.BufferedStart{
				{
					NominalTime:   timestamppb.New(now),
					ActualTime:    timestamppb.New(now.Add(time.Second)),
					OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
					RequestId:     "req-1",
					WorkflowId:    "wf-1",
					Attempt:       2,
					BackoffTime:   timestamppb.New(now.Add(time.Minute)),
				},
			},
		},
	}

	v1, err := CHASMToLegacy(v2, now)
	require.NoError(t, err)

	// Verify V2-specific fields are dropped
	require.Len(t, v1.State.BufferedStarts, 1)
	start := v1.State.BufferedStarts[0]
	require.Empty(t, start.RequestId, "request_id should be cleared")
	require.Empty(t, start.WorkflowId, "workflow_id should be cleared")
	require.Equal(t, int64(0), start.Attempt, "attempt should be cleared")
	require.Nil(t, start.BackoffTime, "backoff_time should be cleared")

	// Verify other fields preserved
	require.Equal(t, now, start.NominalTime.AsTime())
	require.Equal(t, enumspb.SCHEDULE_OVERLAP_POLICY_SKIP, start.OverlapPolicy)
}

func TestLegacyToCHASM_OngoingBackfills(t *testing.T) {
	v1 := testLegacyState()
	now := time.Now().UTC()

	// Add ongoing backfills
	v1.State.OngoingBackfills = []*schedulepb.BackfillRequest{
		{
			StartTime:     timestamppb.New(now.Add(-time.Hour)),
			EndTime:       timestamppb.New(now),
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
		},
	}

	v2, err := LegacyToCHASM(v1, now)
	require.NoError(t, err)

	// Verify backfills converted to Backfiller states
	require.Len(t, v2.Backfillers, 1)
	for _, backfiller := range v2.Backfillers {
		require.NotEmpty(t, backfiller.BackfillId)
		require.Nil(t, backfiller.LastProcessedTime, "last_processed_time should be nil per design doc")
		require.Equal(t, int64(0), backfiller.Attempt)

		req := backfiller.GetBackfillRequest()
		require.NotNil(t, req)
		require.Equal(t, now.Add(-time.Hour), req.StartTime.AsTime())
	}
}

func TestCHASMToLegacy_Backfillers(t *testing.T) {
	now := time.Now().UTC()
	progressTime := now.Add(-30 * time.Minute)

	v2 := &CHASMState{
		Scheduler: &schedulerpb.SchedulerState{
			Schedule:      testSchedule(),
			Info:          &schedulepb.ScheduleInfo{},
			Namespace:     testNamespace,
			NamespaceId:   testNamespaceID,
			ScheduleId:    testScheduleID,
			ConflictToken: 1,
		},
		Generator: &schedulerpb.GeneratorState{
			LastProcessedTime: timestamppb.New(now),
		},
		Invoker: &schedulerpb.InvokerState{},
		Backfillers: map[string]*schedulerpb.BackfillerState{
			"bf-1": {
				BackfillId: "bf-1",
				Request: &schedulerpb.BackfillerState_BackfillRequest{
					BackfillRequest: &schedulepb.BackfillRequest{
						StartTime:     timestamppb.New(now.Add(-time.Hour)),
						EndTime:       timestamppb.New(now),
						OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
					},
				},
				LastProcessedTime: timestamppb.New(progressTime),
				Attempt:           1,
			},
		},
	}

	v1, err := CHASMToLegacy(v2, now)
	require.NoError(t, err)

	// Verify backfillers converted to ongoing backfills
	require.Len(t, v1.State.OngoingBackfills, 1)
	backfill := v1.State.OngoingBackfills[0]

	// StartTime should be set to LastProcessedTime per design doc
	require.Equal(t, progressTime, backfill.StartTime.AsTime())
	require.Equal(t, now, backfill.EndTime.AsTime())
}

func TestLegacyToCHASM_LastCompletionResult(t *testing.T) {
	v1 := testLegacyState()
	now := time.Now().UTC()

	// Add last completion result (V1 uses Payloads - plural)
	v1.State.LastCompletionResult = &commonpb.Payloads{
		Payloads: []*commonpb.Payload{
			{Data: []byte("result-data")},
		},
	}
	v1.State.ContinuedFailure = &failurepb.Failure{
		Message: "last failure",
	}

	v2, err := LegacyToCHASM(v1, now)
	require.NoError(t, err)

	// V2 uses single Payload
	require.NotNil(t, v2.LastCompletionResult)
	require.NotNil(t, v2.LastCompletionResult.Success)
	require.Equal(t, []byte("result-data"), v2.LastCompletionResult.Success.Data)
	require.NotNil(t, v2.LastCompletionResult.Failure)
	require.Equal(t, "last failure", v2.LastCompletionResult.Failure.Message)
}

func TestCHASMToLegacy_LastCompletionResult(t *testing.T) {
	now := time.Now().UTC()
	v2 := &CHASMState{
		Scheduler: &schedulerpb.SchedulerState{
			Schedule:      testSchedule(),
			Info:          &schedulepb.ScheduleInfo{},
			Namespace:     testNamespace,
			NamespaceId:   testNamespaceID,
			ScheduleId:    testScheduleID,
			ConflictToken: 1,
		},
		Generator: &schedulerpb.GeneratorState{
			LastProcessedTime: timestamppb.New(now),
		},
		Invoker: &schedulerpb.InvokerState{},
		LastCompletionResult: &schedulerpb.LastCompletionResult{
			Success: &commonpb.Payload{Data: []byte("success-data")},
			Failure: &failurepb.Failure{Message: "failure-msg"},
		},
	}

	v1, err := CHASMToLegacy(v2, now)
	require.NoError(t, err)

	// V1 uses Payloads (plural)
	require.NotNil(t, v1.State.LastCompletionResult)
	require.Len(t, v1.State.LastCompletionResult.Payloads, 1)
	require.Equal(t, []byte("success-data"), v1.State.LastCompletionResult.Payloads[0].Data)
	require.NotNil(t, v1.State.ContinuedFailure)
	require.Equal(t, "failure-msg", v1.State.ContinuedFailure.Message)
}

func TestRoundTrip_LegacyToCHASMToLegacy(t *testing.T) {
	original := testLegacyState()
	now := time.Now().UTC()

	// Add some state
	original.State.LastCompletionResult = &commonpb.Payloads{
		Payloads: []*commonpb.Payload{{Data: []byte("data")}},
	}
	original.State.BufferedStarts = []*schedulespb.BufferedStart{
		{
			NominalTime:   timestamppb.New(now),
			ActualTime:    timestamppb.New(now),
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
		},
	}

	// V1 -> V2
	v2, err := LegacyToCHASM(original, now)
	require.NoError(t, err)

	// V2 -> V1
	roundTripped, err := CHASMToLegacy(v2, now)
	require.NoError(t, err)

	// Verify key fields preserved
	require.Equal(t, original.State.Namespace, roundTripped.State.Namespace)
	require.Equal(t, original.State.NamespaceId, roundTripped.State.NamespaceId)
	require.Equal(t, original.State.ScheduleId, roundTripped.State.ScheduleId)
	require.Equal(t, original.State.ConflictToken, roundTripped.State.ConflictToken)
	require.Equal(t, original.State.LastProcessedTime.AsTime(), roundTripped.State.LastProcessedTime.AsTime())
	require.Len(t, roundTripped.State.BufferedStarts, 1)
	require.NotNil(t, roundTripped.State.LastCompletionResult)
}

func TestLegacyToCHASM_RecentActionsAndRunningWorkflows(t *testing.T) {
	v1 := testLegacyState()
	now := time.Now().UTC()

	// Add RecentActions and RunningWorkflows to legacy state
	v1.Info.RecentActions = []*schedulepb.ScheduleActionResult{
		{
			ScheduleTime: timestamppb.New(now.Add(-5 * time.Minute)),
			ActualTime:   timestamppb.New(now.Add(-5 * time.Minute)),
			StartWorkflowResult: &commonpb.WorkflowExecution{
				WorkflowId: "wf-1",
				RunId:      "run-1",
			},
			StartWorkflowStatus: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		},
		{
			ScheduleTime: timestamppb.New(now.Add(-3 * time.Minute)),
			ActualTime:   timestamppb.New(now.Add(-3 * time.Minute)),
			StartWorkflowResult: &commonpb.WorkflowExecution{
				WorkflowId: "wf-2",
				RunId:      "run-2",
			},
			StartWorkflowStatus: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		},
	}

	v1.Info.RunningWorkflows = []*commonpb.WorkflowExecution{
		{WorkflowId: "wf-2", RunId: "run-2"},
		{WorkflowId: "wf-3", RunId: "run-3"},
	}

	v2, err := LegacyToCHASM(v1, now)
	require.NoError(t, err)

	// In V2, RecentActions and RunningWorkflows are computed from BufferedStarts, not stored in Info
	require.NotNil(t, v2.Scheduler.Info)
	require.Nil(t, v2.Scheduler.Info.RecentActions)
	require.Nil(t, v2.Scheduler.Info.RunningWorkflows)

	// Verify BufferedStarts contains converted RecentActions and RunningWorkflows
	bufferedStarts := v2.Invoker.GetBufferedStarts()
	require.GreaterOrEqual(t, len(bufferedStarts), 4) // 2 recent actions + 2 running workflows

	// Find completed workflow (wf-1) in BufferedStarts
	var completedStart *schedulespb.BufferedStart
	for _, start := range bufferedStarts {
		if start.GetWorkflowId() == "wf-1" && start.GetRunId() == "run-1" {
			completedStart = start
			break
		}
	}
	require.NotNil(t, completedStart, "completed workflow wf-1 should be in BufferedStarts")
	require.NotNil(t, completedStart.Completed, "wf-1 should have Completed field set")
	require.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, completedStart.Completed.Status)

	// Find running workflows (wf-2, wf-3) in BufferedStarts
	runningCount := 0
	for _, start := range bufferedStarts {
		if (start.GetWorkflowId() == "wf-2" || start.GetWorkflowId() == "wf-3") &&
			start.GetRunId() != "" && start.GetCompleted() == nil {
			runningCount++
		}
	}
	require.Equal(t, 2, runningCount, "both running workflows should be in BufferedStarts without Completed field")
}

func TestCHASMToLegacy_RecentActionsAndRunningWorkflows(t *testing.T) {
	now := time.Now().UTC()
	v2 := &CHASMState{
		Scheduler: &schedulerpb.SchedulerState{
			Schedule:      testSchedule(),
			Namespace:     testNamespace,
			NamespaceId:   testNamespaceID,
			ScheduleId:    testScheduleID,
			ConflictToken: 1,
			Info: &schedulepb.ScheduleInfo{
				ActionCount: 5,
			},
		},
		Generator: &schedulerpb.GeneratorState{
			LastProcessedTime: timestamppb.New(now),
		},
		Invoker: &schedulerpb.InvokerState{
			BufferedStarts: []*schedulespb.BufferedStart{
				// Completed workflow
				{
					NominalTime: timestamppb.New(now.Add(-10 * time.Minute)),
					ActualTime:  timestamppb.New(now.Add(-10 * time.Minute)),
					StartTime:   timestamppb.New(now.Add(-10 * time.Minute)),
					WorkflowId:  "chasm-wf-1",
					RunId:       "chasm-run-1",
					Completed: &schedulespb.CompletedResult{
						Status:    enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
						CloseTime: timestamppb.New(now.Add(-9 * time.Minute)),
					},
				},
				// Running workflow
				{
					NominalTime: timestamppb.New(now.Add(-5 * time.Minute)),
					ActualTime:  timestamppb.New(now.Add(-5 * time.Minute)),
					StartTime:   timestamppb.New(now.Add(-5 * time.Minute)),
					WorkflowId:  "chasm-wf-2",
					RunId:       "chasm-run-2",
					Completed:   nil, // Still running
				},
				// Another running workflow
				{
					NominalTime: timestamppb.New(now.Add(-3 * time.Minute)),
					ActualTime:  timestamppb.New(now.Add(-3 * time.Minute)),
					StartTime:   timestamppb.New(now.Add(-3 * time.Minute)),
					WorkflowId:  "chasm-wf-3",
					RunId:       "chasm-run-3",
					Completed:   nil, // Still running
				},
			},
		},
	}

	v1, err := CHASMToLegacy(v2, now)
	require.NoError(t, err)

	// Verify RecentActions are preserved (only completed workflows)
	require.NotNil(t, v1.Info)
	require.Len(t, v1.Info.RecentActions, 1)
	require.Equal(t, "chasm-wf-1", v1.Info.RecentActions[0].StartWorkflowResult.WorkflowId)
	require.Equal(t, "chasm-run-1", v1.Info.RecentActions[0].StartWorkflowResult.RunId)
	require.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, v1.Info.RecentActions[0].StartWorkflowStatus)

	// Verify RunningWorkflows are preserved (workflows with RunId but no Completed)
	require.Len(t, v1.Info.RunningWorkflows, 2)
	require.Equal(t, "chasm-wf-2", v1.Info.RunningWorkflows[0].WorkflowId)
	require.Equal(t, "chasm-run-2", v1.Info.RunningWorkflows[0].RunId)
	require.Equal(t, "chasm-wf-3", v1.Info.RunningWorkflows[1].WorkflowId)
	require.Equal(t, "chasm-run-3", v1.Info.RunningWorkflows[1].RunId)

	// Verify NeedRefresh is set to re-arm watchers for running workflows
	require.True(t, v1.State.NeedRefresh)
}

func TestRoundTrip_PreservesRecentActionsAndRunningWorkflows(t *testing.T) {
	original := testLegacyState()
	now := time.Now().UTC()

	// Add RecentActions and RunningWorkflows
	original.Info.RecentActions = []*schedulepb.ScheduleActionResult{
		{
			ScheduleTime: timestamppb.New(now.Add(-15 * time.Minute)),
			ActualTime:   timestamppb.New(now.Add(-15 * time.Minute)),
			StartWorkflowResult: &commonpb.WorkflowExecution{
				WorkflowId: "original-wf-1",
				RunId:      "original-run-1",
			},
			StartWorkflowStatus: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		},
	}
	original.Info.RunningWorkflows = []*commonpb.WorkflowExecution{
		{WorkflowId: "original-wf-2", RunId: "original-run-2"},
	}

	// V1 -> V2
	v2, err := LegacyToCHASM(original, now)
	require.NoError(t, err)

	// V2 -> V1
	roundTripped, err := CHASMToLegacy(v2, now)
	require.NoError(t, err)

	// Verify RecentActions preserved through round trip
	require.Len(t, roundTripped.Info.RecentActions, 1)
	require.Equal(t, "original-wf-1", roundTripped.Info.RecentActions[0].StartWorkflowResult.WorkflowId)
	require.Equal(t, "original-run-1", roundTripped.Info.RecentActions[0].StartWorkflowResult.RunId)
	require.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, roundTripped.Info.RecentActions[0].StartWorkflowStatus)

	// Verify RunningWorkflows preserved through round trip
	require.Len(t, roundTripped.Info.RunningWorkflows, 1)
	require.Equal(t, "original-wf-2", roundTripped.Info.RunningWorkflows[0].WorkflowId)
	require.Equal(t, "original-run-2", roundTripped.Info.RunningWorkflows[0].RunId)
}
