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
	"go.temporal.io/server/common/searchattribute/sadefs"
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

func TestLegacyToCreateFromMigrationStateRequest(t *testing.T) {
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
	searchAttrs := &commonpb.SearchAttributes{IndexedFields: map[string]*commonpb.Payload{"Attr": {Data: []byte("value")}}}
	memo := &commonpb.Memo{Fields: map[string]*commonpb.Payload{"Memo": {Data: []byte("memo")}}}

	req := LegacyToCreateFromMigrationStateRequest(newTestSchedule(), info, state, searchAttrs, memo, now)

	require.NotNil(t, req)
	require.Equal(t, "test-ns-id", req.NamespaceId)

	migrationState := req.State
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
			require.False(t, start.HasCallback)
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

	// Search attributes and memo: user-defined SAs are preserved as-is.
	require.Equal(t, searchAttrs.GetIndexedFields(), migrationState.SearchAttributes)
	require.Equal(t, memo.GetFields(), migrationState.Memo)
}

func TestLegacyToCreateFromMigrationStateRequest_StripsSystemSearchAttributes(t *testing.T) {
	// V1 scheduler workflows carry TemporalNamespaceDivision ('TemporalScheduler')
	// and TemporalSchedulePaused in their search attributes. Both are system SAs
	// that the CHASM framework manages independently, so they must not be stored in
	// the CHASM entity's custom SA map. Storing them there causes a spurious
	// warn-level log when the custom SA mapper finds no per-namespace mapping for them.
	userSA := &commonpb.Payload{Data: []byte(`"my-value"`)}

	tests := []struct {
		name        string
		searchAttrs *commonpb.SearchAttributes
		wantKeys    []string
		absentKeys  []string
	}{
		{
			name:        "nil search attributes",
			searchAttrs: nil,
		},
		{
			name:        "empty IndexedFields",
			searchAttrs: &commonpb.SearchAttributes{},
		},
		{
			name: "only system SAs — all stripped",
			searchAttrs: &commonpb.SearchAttributes{
				IndexedFields: map[string]*commonpb.Payload{
					sadefs.TemporalNamespaceDivision: {Data: []byte(`"TemporalScheduler"`)},
					sadefs.TemporalSchedulePaused:    {Data: []byte(`false`)},
				},
			},
			absentKeys: []string{sadefs.TemporalNamespaceDivision, sadefs.TemporalSchedulePaused},
		},
		{
			name: "user SA preserved, system SAs stripped",
			searchAttrs: &commonpb.SearchAttributes{
				IndexedFields: map[string]*commonpb.Payload{
					sadefs.TemporalNamespaceDivision: {Data: []byte(`"TemporalScheduler"`)},
					sadefs.TemporalSchedulePaused:    {Data: []byte(`false`)},
					"MyCustomAttr":                   userSA,
				},
			},
			wantKeys:   []string{"MyCustomAttr"},
			absentKeys: []string{sadefs.TemporalNamespaceDivision, sadefs.TemporalSchedulePaused},
		},
		{
			name: "only user SAs — all preserved",
			searchAttrs: &commonpb.SearchAttributes{
				IndexedFields: map[string]*commonpb.Payload{"MyCustomAttr": userSA},
			},
			wantKeys: []string{"MyCustomAttr"},
		},
	}

	now := time.Now().UTC()
	state := &schedulespb.InternalState{
		Namespace:   "test-ns",
		NamespaceId: "test-ns-id",
		ScheduleId:  "test-sched-id",
	}
	info := &schedulepb.ScheduleInfo{}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := LegacyToCreateFromMigrationStateRequest(newTestSchedule(), info, state, tc.searchAttrs, nil, now)
			got := req.State.SearchAttributes
			for _, k := range tc.wantKeys {
				require.Contains(t, got, k)
			}
			for _, k := range tc.absentKeys {
				require.NotContains(t, got, k)
			}
		})
	}
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

	args := CHASMToLegacyStartScheduleArgs(scheduler, generator, invoker, backfillers, lastCompletion, nil, nil, now)

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

func TestCHASMToLegacyStartScheduleArgs_ExcludesAllowAllFromRunningWorkflows(t *testing.T) {
	// Regression test: workflows started under ALLOW_ALL are tracked in V2 as
	// BufferedStarts with a RunId (and no Completed) while they run. Modern V1
	// (version >= DontTrackOverlapping) intentionally keeps ALLOW_ALL runs out
	// of Info.RunningWorkflows, because they don't participate in overlap
	// resolution (see recordAction in the V1 scheduler workflow). The V2->V1
	// migration must therefore not export them into RunningWorkflows: doing so
	// would make a rolled-back schedule treat itself as busy and mis-apply
	// SKIP/BUFFER/CANCEL/TERMINATE to later, non-ALLOW_ALL starts. The ALLOW_ALL
	// runs must still appear in RecentActions, matching V1.
	now := time.Now().UTC()
	scheduler := &schedulerpb.SchedulerState{
		Namespace:     "test-ns",
		NamespaceId:   "test-ns-id",
		ScheduleId:    "test-sched-id",
		ConflictToken: 1,
		Schedule:      newTestSchedule(),
		Info:          &schedulepb.ScheduleInfo{},
	}
	invoker := &schedulerpb.InvokerState{
		BufferedStarts: []*schedulespb.BufferedStart{
			{
				// Running, started under ALLOW_ALL: excluded from RunningWorkflows,
				// but present in RecentActions.
				NominalTime:   timestamppb.New(now.Add(-5 * time.Minute)),
				ActualTime:    timestamppb.New(now.Add(-5 * time.Minute)),
				StartTime:     timestamppb.New(now.Add(-5 * time.Minute)),
				WorkflowId:    "wf-allow-all",
				RunId:         "run-allow-all",
				OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
			},
			{
				// Running, non-ALLOW_ALL: tracked in RunningWorkflows, as in V1.
				NominalTime:   timestamppb.New(now.Add(-3 * time.Minute)),
				ActualTime:    timestamppb.New(now.Add(-3 * time.Minute)),
				StartTime:     timestamppb.New(now.Add(-3 * time.Minute)),
				WorkflowId:    "wf-skip",
				RunId:         "run-skip",
				OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
			},
			{
				// Completed ALLOW_ALL: a recent action only, never running.
				NominalTime:   timestamppb.New(now.Add(-20 * time.Minute)),
				ActualTime:    timestamppb.New(now.Add(-20 * time.Minute)),
				StartTime:     timestamppb.New(now.Add(-20 * time.Minute)),
				WorkflowId:    "wf-allow-all-done",
				RunId:         "run-allow-all-done",
				OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
				Completed: &schedulespb.CompletedResult{
					Status:    enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
					CloseTime: timestamppb.New(now.Add(-15 * time.Minute)),
				},
			},
		},
	}

	args := CHASMToLegacyStartScheduleArgs(scheduler, nil, invoker, nil, nil, nil, nil, now)

	// Only the non-ALLOW_ALL running workflow is exported to RunningWorkflows.
	require.Len(t, args.Info.RunningWorkflows, 1,
		"ALLOW_ALL run must not be exported to RunningWorkflows")
	require.Equal(t, "wf-skip", args.Info.RunningWorkflows[0].WorkflowId)
	require.Equal(t, "run-skip", args.Info.RunningWorkflows[0].RunId)

	// All three started executions still appear in RecentActions, matching V1,
	// which records every action regardless of overlap policy.
	recentStatusByRunID := make(map[string]enumspb.WorkflowExecutionStatus, len(args.Info.RecentActions))
	for _, action := range args.Info.RecentActions {
		recentStatusByRunID[action.GetStartWorkflowResult().GetRunId()] = action.GetStartWorkflowStatus()
	}
	require.Len(t, args.Info.RecentActions, 3)
	require.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, recentStatusByRunID["run-allow-all"],
		"running ALLOW_ALL execution should still be recorded in RecentActions as RUNNING")
	require.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, recentStatusByRunID["run-skip"])
	require.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, recentStatusByRunID["run-allow-all-done"])

	// NeedRefresh mirrors the (correctly reduced) running set.
	require.True(t, args.State.NeedRefresh)
}

func TestLegacyToCreateFromMigrationStateRequest_DeduplicatesRunningWorkflows(t *testing.T) {
	// V1's recordAction puts the same workflow in both RecentActions (with
	// RUNNING status) and RunningWorkflows. The migration should not create
	// duplicate BufferedStarts for the same execution.
	now := time.Now().UTC()
	state := &schedulespb.InternalState{
		Namespace:     "test-ns",
		NamespaceId:   "test-ns-id",
		ScheduleId:    "test-sched-id",
		ConflictToken: 1,
	}
	info := &schedulepb.ScheduleInfo{
		RunningWorkflows: []*commonpb.WorkflowExecution{
			{WorkflowId: "wf-1", RunId: "run-1"},
		},
		RecentActions: []*schedulepb.ScheduleActionResult{
			{
				// Completed action - should be kept.
				ScheduleTime:        timestamppb.New(now.Add(-2 * time.Hour)),
				ActualTime:          timestamppb.New(now.Add(-2 * time.Hour)),
				StartWorkflowResult: &commonpb.WorkflowExecution{WorkflowId: "wf-old", RunId: "run-old"},
				StartWorkflowStatus: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
			},
			{
				// Same workflow as RunningWorkflows - should be deduplicated.
				ScheduleTime:        timestamppb.New(now.Add(-time.Hour)),
				ActualTime:          timestamppb.New(now.Add(-time.Hour)),
				StartWorkflowResult: &commonpb.WorkflowExecution{WorkflowId: "wf-1", RunId: "run-1"},
				StartWorkflowStatus: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			},
		},
	}

	req := LegacyToCreateFromMigrationStateRequest(newTestSchedule(), info, state, nil, nil, now)

	// Should have 2 BufferedStarts: 1 running (from RunningWorkflows) + 1 completed (from RecentActions).
	// The running entry in RecentActions should be excluded since it duplicates RunningWorkflows.
	require.Len(t, req.State.InvokerState.BufferedStarts, 2)

	var running, completed int
	for _, start := range req.State.InvokerState.BufferedStarts {
		switch {
		case start.RunId != "" && start.Completed == nil:
			running++
			require.Equal(t, "wf-1", start.WorkflowId)
			require.Equal(t, "run-1", start.RunId)
		case start.Completed != nil:
			completed++
			require.Equal(t, "wf-old", start.WorkflowId)
			require.Equal(t, "run-old", start.RunId)
		default:
			t.Fatalf("unexpected buffered start state: RunId=%q, Completed=%v", start.RunId, start.Completed)
		}
	}
	require.Equal(t, 1, running, "expected exactly 1 running workflow (not duplicated)")
	require.Equal(t, 1, completed, "expected 1 completed workflow from recent actions")

	// Verify the round-trip: converting back to legacy should also have no
	// duplicate RunIds in RecentActions.
	_, _, recentActions := splitBufferedStartsForLegacy(req.State.InvokerState.BufferedStarts)
	seen := make(map[string]bool)
	for _, action := range recentActions {
		runID := action.GetStartWorkflowResult().GetRunId()
		require.False(t, seen[runID], "duplicate RunId %q in round-tripped RecentActions", runID)
		seen[runID] = true
	}
}

func TestConvertRunningWorkflowsToBufferedStarts_UniqueRequestIDs(t *testing.T) {
	// With ALLOW_ALL overlap policy, multiple workflows can be running
	// concurrently. Each must get a unique RequestId so that
	// recordCompletedAction matches the correct BufferedStart.
	now := time.Now().UTC()
	running := []*commonpb.WorkflowExecution{
		{WorkflowId: "wf-1", RunId: "run-aaa"},
		{WorkflowId: "wf-2", RunId: "run-bbb"},
		{WorkflowId: "wf-3", RunId: "run-ccc"},
	}

	starts := convertRunningWorkflowsToBufferedStarts(
		running, "ns-id", "sched-id", 1, now,
	)
	require.Len(t, starts, 3)

	requestIDs := make(map[string]string) // requestId -> runId
	for _, start := range starts {
		if prev, ok := requestIDs[start.RequestId]; ok {
			t.Fatalf("duplicate RequestId %q: used by both RunId %q and %q",
				start.RequestId, prev, start.RunId)
		}
		requestIDs[start.RequestId] = start.RunId
	}
}
