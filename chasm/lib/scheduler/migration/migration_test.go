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
	require.NotNil(t, req.SchedulerState)
	require.Equal(t, int64(42), req.SchedulerState.ConflictToken)
	require.False(t, req.SchedulerState.Closed)

	// Generator state
	require.NotNil(t, req.GeneratorState)
	require.Equal(t, now, req.GeneratorState.LastProcessedTime.AsTime())

	// Invoker state - buffered starts + running workflows
	require.NotNil(t, req.InvokerState)
	require.Len(t, req.InvokerState.BufferedStarts, 2) // 1 buffered + 1 running
	for _, start := range req.InvokerState.BufferedStarts {
		require.NotEmpty(t, start.RequestId)
		require.NotEmpty(t, start.WorkflowId)
	}

	// Backfillers
	require.Len(t, req.Backfillers, 1)
	for id, backfiller := range req.Backfillers {
		require.Equal(t, id, backfiller.BackfillId)
		require.NotNil(t, backfiller.GetBackfillRequest())
	}

	// Last completion result
	require.NotNil(t, req.LastCompletionResult)
	require.Equal(t, []byte("result"), req.LastCompletionResult.Success.Data)
	require.Equal(t, "last failure", req.LastCompletionResult.Failure.Message)

	// Search attributes and memo
	require.Equal(t, searchAttrs, req.SearchAttributes)
	require.Equal(t, memo, req.Memo)
}

func TestCHASMToMigrateScheduleRequest(t *testing.T) {
	now := time.Now().UTC()
	scheduler := &schedulerpb.SchedulerState{
		Namespace:     "ns",
		NamespaceId:   "ns-id",
		ScheduleId:    "sched-id",
		ConflictToken: 42,
	}
	generator := &schedulerpb.GeneratorState{LastProcessedTime: timestamppb.New(now)}
	invoker := &schedulerpb.InvokerState{}

	req := CHASMToMigrateScheduleRequest(scheduler, generator, invoker, nil, nil, nil, nil)

	require.Equal(t, "ns-id", req.NamespaceId)
	require.Equal(t, "ns", req.Namespace)
	require.Equal(t, "sched-id", req.ScheduleId)
	require.Equal(t, int64(42), req.SchedulerState.ConflictToken)
	require.NotNil(t, req.GeneratorState)
	require.NotNil(t, req.InvokerState)
}
