package scheduler_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler"
	schedulerpb "go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/common/testing/protorequire"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestListInfo(t *testing.T) {
	scheduler, ctx, _ := setupSchedulerForTest(t)

	// Generator maintains the FutureActionTimes list, set that up first.
	generator := scheduler.Generator.Get(ctx)
	expectedFutureTimes := []*timestamppb.Timestamp{timestamppb.Now(), timestamppb.Now()}
	generator.FutureActionTimes = expectedFutureTimes

	listInfo := scheduler.ListInfo(ctx)

	// Should return a populated info block.
	require.NotNil(t, listInfo)
	require.NotNil(t, listInfo.Spec)
	require.NotEmpty(t, listInfo.Spec.Interval)
	protorequire.ProtoEqual(t, listInfo.Spec.Interval[0], scheduler.Schedule.Spec.Interval[0])
	require.NotNil(t, listInfo.WorkflowType)
	require.NotEmpty(t, listInfo.FutureActionTimes)
	require.Equal(t, expectedFutureTimes, listInfo.FutureActionTimes)
}

// Tests for CreateSchedulerFromMigration - creates scheduler from migrated V1 state

func testMigrateScheduleRequest() *schedulerpb.MigrateScheduleRequest {
	now := time.Now().UTC()
	return &schedulerpb.MigrateScheduleRequest{
		NamespaceId: namespaceID,
		State: &schedulerpb.SchedulerMigrationState{
			SchedulerState: &schedulerpb.SchedulerState{
				Schedule:      defaultSchedule(),
				Info:          &schedulepb.ScheduleInfo{ActionCount: 10},
				Namespace:     namespace,
				NamespaceId:   namespaceID,
				ScheduleId:    scheduleID,
				ConflictToken: 42, // Non-initial conflict token
				Closed:        false,
			},
			GeneratorState: &schedulerpb.GeneratorState{
				LastProcessedTime: timestamppb.New(now),
			},
			InvokerState: &schedulerpb.InvokerState{
				BufferedStarts: []*schedulespb.BufferedStart{
					{
						NominalTime:   timestamppb.New(now),
						ActualTime:    timestamppb.New(now),
						OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
						RequestId:     "req-1",
						WorkflowId:    "wf-1",
					},
				},
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
				},
			},
			LastCompletionResult: &schedulerpb.LastCompletionResult{
				Success: &commonpb.Payload{Data: []byte("result-data")},
			},
			SearchAttributes: map[string]*commonpb.Payload{
				"CustomAttr": {Data: []byte("attr-value")},
			},
			Memo: map[string]*commonpb.Payload{
				"MemoKey": {Data: []byte("memo-value")},
			},
		},
	}
}

func TestCreateSchedulerFromMigration_InitializesComponents(t *testing.T) {
	_, _, node := setupSchedulerForTest(t)
	req := testMigrateScheduleRequest()

	// Create a new context for the migration (we need a fresh transaction)
	ctx := chasm.NewMutableContext(context.Background(), node)

	sched, err := scheduler.CreateSchedulerFromMigration(ctx, req)
	require.NoError(t, err)
	require.NotNil(t, sched)

	// Verify scheduler state
	require.Equal(t, namespace, sched.Namespace)
	require.Equal(t, namespaceID, sched.NamespaceId)
	require.Equal(t, scheduleID, sched.ScheduleId)
	require.False(t, sched.Closed)

	// Verify generator initialized
	generator := sched.Generator.Get(ctx)
	require.NotNil(t, generator)
	require.NotNil(t, generator.LastProcessedTime)

	// Verify invoker initialized with buffered starts
	invoker := sched.Invoker.Get(ctx)
	require.NotNil(t, invoker)
	require.Len(t, invoker.BufferedStarts, 1)
	require.Equal(t, "req-1", invoker.BufferedStarts[0].RequestId)

	// Verify backfillers initialized
	require.Len(t, sched.Backfillers, 1)
	backfiller := sched.Backfillers["bf-1"]
	require.NotNil(t, backfiller)
	bf := backfiller.Get(ctx)
	require.Equal(t, "bf-1", bf.BackfillId)

	// Verify last completion result
	lastResult := sched.LastCompletionResult.Get(ctx)
	require.NotNil(t, lastResult)
	require.NotNil(t, lastResult.Success)
	require.Equal(t, []byte("result-data"), lastResult.Success.Data)

	// Close transaction to finalize
	node.SetRootComponent(sched)
	_, err = node.CloseTransaction()
	require.NoError(t, err)
}

func TestCreateSchedulerFromMigration_PreservesConflictToken(t *testing.T) {
	_, _, node := setupSchedulerForTest(t)
	req := testMigrateScheduleRequest()
	req.State.SchedulerState.ConflictToken = 99 // Specific conflict token to verify

	// Create a new context for the migration
	ctx := chasm.NewMutableContext(context.Background(), node)

	sched, err := scheduler.CreateSchedulerFromMigration(ctx, req)
	require.NoError(t, err)

	node.SetRootComponent(sched)
	_, err = node.CloseTransaction()
	require.NoError(t, err)

	// Conflict token must be preserved for client compatibility
	require.Equal(t, int64(99), sched.ConflictToken)
}

func TestCreateSchedulerFromMigration_ProcessesBufferedStarts(t *testing.T) {
	_, _, node := setupSchedulerForTest(t)
	req := testMigrateScheduleRequest()

	// Add multiple buffered starts
	now := time.Now().UTC()
	req.State.InvokerState.BufferedStarts = []*schedulespb.BufferedStart{
		{
			NominalTime:   timestamppb.New(now),
			ActualTime:    timestamppb.New(now),
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
			RequestId:     "req-1",
			WorkflowId:    "wf-1",
		},
		{
			NominalTime:   timestamppb.New(now.Add(time.Minute)),
			ActualTime:    timestamppb.New(now.Add(time.Minute)),
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
			RequestId:     "req-2",
			WorkflowId:    "wf-2",
		},
	}

	// Create a new context for the migration
	ctx := chasm.NewMutableContext(context.Background(), node)

	sched, err := scheduler.CreateSchedulerFromMigration(ctx, req)
	require.NoError(t, err)

	node.SetRootComponent(sched)
	_, err = node.CloseTransaction()
	require.NoError(t, err)

	// Reopen context for reading
	ctx = chasm.NewMutableContext(context.Background(), node)

	// Verify all buffered starts are preserved
	invoker := sched.Invoker.Get(ctx)
	require.Len(t, invoker.BufferedStarts, 2)
	require.Equal(t, "req-1", invoker.BufferedStarts[0].RequestId)
	require.Equal(t, "req-2", invoker.BufferedStarts[1].RequestId)
}

func TestCreateSchedulerFromMigration_HandlesEmptyState(t *testing.T) {
	_, _, node := setupSchedulerForTest(t)

	// Minimal request with empty optional fields
	req := &schedulerpb.MigrateScheduleRequest{
		NamespaceId: namespaceID,
		State: &schedulerpb.SchedulerMigrationState{
			SchedulerState: &schedulerpb.SchedulerState{
				Schedule:      defaultSchedule(),
				Info:          &schedulepb.ScheduleInfo{},
				Namespace:     namespace,
				NamespaceId:   namespaceID,
				ScheduleId:    scheduleID,
				ConflictToken: 1,
			},
			GeneratorState: &schedulerpb.GeneratorState{},
			InvokerState:   &schedulerpb.InvokerState{},
			Backfillers:    nil,
		},
	}

	// Create a new context for the migration
	ctx := chasm.NewMutableContext(context.Background(), node)

	sched, err := scheduler.CreateSchedulerFromMigration(ctx, req)
	require.NoError(t, err)

	node.SetRootComponent(sched)
	_, err = node.CloseTransaction()
	require.NoError(t, err)

	// Reopen context for reading
	ctx = chasm.NewMutableContext(context.Background(), node)

	// Verify minimal state is valid
	require.Equal(t, int64(1), sched.ConflictToken)
	require.Empty(t, sched.Backfillers)

	invoker := sched.Invoker.Get(ctx)
	require.Empty(t, invoker.BufferedStarts)
}
