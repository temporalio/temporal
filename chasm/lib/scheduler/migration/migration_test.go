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
	require.NotNil(t, v2.Invoker.RequestIdToWorkflowId)
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
			BufferedStarts:        []*schedulespb.BufferedStart{},
			RequestIdToWorkflowId: map[string]string{},
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

	// Verify RequestIdToWorkflowId map is populated
	require.Len(t, v2.Invoker.RequestIdToWorkflowId, 2)
}

func TestCHASMToLegacy_BufferedStarts(t *testing.T) {
	now := time.Now().UTC()
	v2 := &CHASMState{
		Scheduler: &schedulerpb.SchedulerState{
			Schedule:      testSchedule(),
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
			RequestIdToWorkflowId: map[string]string{"req-1": "wf-1"},
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
			Namespace:     testNamespace,
			NamespaceId:   testNamespaceID,
			ScheduleId:    testScheduleID,
			ConflictToken: 1,
		},
		Generator: &schedulerpb.GeneratorState{
			LastProcessedTime: timestamppb.New(now),
		},
		Invoker: &schedulerpb.InvokerState{
			RequestIdToWorkflowId: map[string]string{},
		},
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
			Namespace:     testNamespace,
			NamespaceId:   testNamespaceID,
			ScheduleId:    testScheduleID,
			ConflictToken: 1,
		},
		Generator: &schedulerpb.GeneratorState{
			LastProcessedTime: timestamppb.New(now),
		},
		Invoker: &schedulerpb.InvokerState{
			RequestIdToWorkflowId: map[string]string{},
		},
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

func TestValidation_InvalidLegacyState(t *testing.T) {
	tests := []struct {
		name    string
		v1      *LegacyState
		wantErr string
	}{
		{
			name:    "nil state",
			v1:      nil,
			wantErr: "legacy state is nil",
		},
		{
			name:    "nil schedule",
			v1:      &LegacyState{State: &schedulespb.InternalState{}},
			wantErr: "schedule is required",
		},
		{
			name: "zero conflict token",
			v1: &LegacyState{
				Schedule: testSchedule(),
				State: &schedulespb.InternalState{
					Namespace:     testNamespace,
					NamespaceId:   testNamespaceID,
					ScheduleId:    testScheduleID,
					ConflictToken: 0,
				},
			},
			wantErr: "conflict_token must be positive",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := LegacyToCHASM(tt.v1, time.Now())
			require.Error(t, err)
			require.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

func TestValidation_InvalidCHASMState(t *testing.T) {
	now := time.Now().UTC()

	tests := []struct {
		name    string
		v2      *CHASMState
		wantErr string
	}{
		{
			name:    "nil state",
			v2:      nil,
			wantErr: "CHASM state is nil",
		},
		{
			name: "nil generator",
			v2: &CHASMState{
				Scheduler: &schedulerpb.SchedulerState{
					Schedule:      testSchedule(),
					Namespace:     testNamespace,
					NamespaceId:   testNamespaceID,
					ScheduleId:    testScheduleID,
					ConflictToken: 1,
				},
				Invoker: &schedulerpb.InvokerState{},
			},
			wantErr: "generator state is required",
		},
		{
			name: "buffered start missing request_id",
			v2: &CHASMState{
				Scheduler: &schedulerpb.SchedulerState{
					Schedule:      testSchedule(),
					Namespace:     testNamespace,
					NamespaceId:   testNamespaceID,
					ScheduleId:    testScheduleID,
					ConflictToken: 1,
				},
				Generator: &schedulerpb.GeneratorState{LastProcessedTime: timestamppb.New(now)},
				Invoker: &schedulerpb.InvokerState{
					BufferedStarts: []*schedulespb.BufferedStart{
						{
							NominalTime: timestamppb.New(now),
							ActualTime:  timestamppb.New(now),
							WorkflowId:  "wf-1",
							// RequestId missing
						},
					},
				},
			},
			wantErr: "request_id is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := CHASMToLegacy(tt.v2, time.Now())
			require.Error(t, err)
			require.Contains(t, err.Error(), tt.wantErr)
		})
	}
}
