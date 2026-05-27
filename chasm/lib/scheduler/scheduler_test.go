package scheduler_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
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

func TestCreateSchedulerFromMigration(t *testing.T) {
	now := time.Now().UTC()
	_, _, node := setupSchedulerForTest(t)

	req := &schedulerpb.CreateFromMigrationStateRequest{
		NamespaceId: namespaceID,
		State: &schedulerpb.SchedulerMigrationState{
			SchedulerState: &schedulerpb.SchedulerState{
				Schedule:      defaultSchedule(),
				Info:          &schedulepb.ScheduleInfo{ActionCount: 10},
				Namespace:     namespace,
				NamespaceId:   namespaceID,
				ScheduleId:    scheduleID,
				ConflictToken: 42,
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
					{
						NominalTime:   timestamppb.New(now.Add(time.Minute)),
						ActualTime:    timestamppb.New(now.Add(time.Minute)),
						OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
						RequestId:     "req-2",
						WorkflowId:    "wf-2",
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

	ctx := chasm.NewMutableContext(context.Background(), node)
	sched, err := scheduler.CreateSchedulerFromMigration(ctx, req)
	require.NoError(t, err)

	// Scheduler state
	require.Equal(t, namespace, sched.Namespace)
	require.Equal(t, namespaceID, sched.NamespaceId)
	require.Equal(t, scheduleID, sched.ScheduleId)
	require.Equal(t, int64(42), sched.ConflictToken)
	require.False(t, sched.Closed)

	// Generator
	generator := sched.Generator.Get(ctx)
	require.Equal(t, now, generator.LastProcessedTime.AsTime())

	// Invoker buffered starts
	invoker := sched.Invoker.Get(ctx)
	require.Len(t, invoker.BufferedStarts, 2)
	require.Equal(t, "req-1", invoker.BufferedStarts[0].RequestId)
	require.Equal(t, "req-2", invoker.BufferedStarts[1].RequestId)

	// Backfillers
	require.Len(t, sched.Backfillers, 1)
	bf := sched.Backfillers["bf-1"].Get(ctx)
	require.Equal(t, "bf-1", bf.BackfillId)

	// Last completion result
	lastResult := sched.LastCompletionResult.Get(ctx)
	require.Equal(t, []byte("result-data"), lastResult.Success.Data)

	require.NoError(t, node.SetRootComponent(sched))
	_, err = node.CloseTransaction()
	require.NoError(t, err)
}

func TestUpdate_WithMemo(t *testing.T) {
	sched, ctx, _ := setupSchedulerForTest(t)
	memoValue := &commonpb.Payload{Data: []byte("test-value")}

	_, err := sched.Update(ctx, &schedulerpb.UpdateScheduleRequest{
		NamespaceId: namespaceID,
		FrontendRequest: &workflowservice.UpdateScheduleRequest{
			Namespace:  namespace,
			ScheduleId: scheduleID,
			Schedule:   defaultSchedule(),
			Memo: &commonpb.Memo{
				Fields: map[string]*commonpb.Payload{"key1": memoValue},
			},
		},
	})
	require.NoError(t, err)

	visibility := sched.Visibility.Get(ctx)
	memo := visibility.CustomMemo(ctx)
	protorequire.ProtoEqual(t, memoValue, memo["key1"])
}

func TestUpdate_WithNilMemo(t *testing.T) {
	sched, ctx, node := setupSchedulerForTest(t)

	// Set initial memo.
	visibility := sched.Visibility.Get(ctx)
	visibility.MergeCustomMemo(ctx, map[string]*commonpb.Payload{
		"existing": {Data: []byte("value")},
	})
	_, err := node.CloseTransaction()
	require.NoError(t, err)

	// Update without memo (nil) should preserve existing memo.
	ctx = chasm.NewMutableContext(context.Background(), node)
	_, err = sched.Update(ctx, &schedulerpb.UpdateScheduleRequest{
		NamespaceId: namespaceID,
		FrontendRequest: &workflowservice.UpdateScheduleRequest{
			Namespace:  namespace,
			ScheduleId: scheduleID,
			Schedule:   defaultSchedule(),
		},
	})
	require.NoError(t, err)

	visibility = sched.Visibility.Get(ctx)
	memo := visibility.CustomMemo(ctx)
	protorequire.ProtoEqual(t, &commonpb.Payload{Data: []byte("value")}, memo["existing"])
}

func TestUpdate_MemoReplaceSemantics(t *testing.T) {
	sched, ctx, node := setupSchedulerForTest(t)

	// Set initial memo with keys A and B.
	visibility := sched.Visibility.Get(ctx)
	visibility.MergeCustomMemo(ctx, map[string]*commonpb.Payload{
		"A": {Data: []byte("1")},
		"B": {Data: []byte("2")},
	})
	_, err := node.CloseTransaction()
	require.NoError(t, err)

	// Update with only C: should fully replace memo (A and B are gone).
	ctx = chasm.NewMutableContext(context.Background(), node)
	_, err = sched.Update(ctx, &schedulerpb.UpdateScheduleRequest{
		NamespaceId: namespaceID,
		FrontendRequest: &workflowservice.UpdateScheduleRequest{
			Namespace:  namespace,
			ScheduleId: scheduleID,
			Schedule:   defaultSchedule(),
			Memo: &commonpb.Memo{
				Fields: map[string]*commonpb.Payload{
					"C": {Data: []byte("3")},
				},
			},
		},
	})
	require.NoError(t, err)

	visibility = sched.Visibility.Get(ctx)
	memo := visibility.CustomMemo(ctx)
	require.Nil(t, memo["A"], "A should be gone after replace")
	require.Nil(t, memo["B"], "B should be gone after replace")
	protorequire.ProtoEqual(t, &commonpb.Payload{Data: []byte("3")}, memo["C"])

	// Update with empty memo: should clear all memo fields.
	_, err = node.CloseTransaction()
	require.NoError(t, err)
	ctx = chasm.NewMutableContext(context.Background(), node)
	_, err = sched.Update(ctx, &schedulerpb.UpdateScheduleRequest{
		NamespaceId: namespaceID,
		FrontendRequest: &workflowservice.UpdateScheduleRequest{
			Namespace:  namespace,
			ScheduleId: scheduleID,
			Schedule:   defaultSchedule(),
			Memo: &commonpb.Memo{
				Fields: map[string]*commonpb.Payload{},
			},
		},
	})
	require.NoError(t, err)

	visibility = sched.Visibility.Get(ctx)
	memo = visibility.CustomMemo(ctx)
	require.Empty(t, memo, "memo should be empty after replace with empty map")
}

func TestCreateSchedulerFromMigration_EmptyState(t *testing.T) {
	_, _, node := setupSchedulerForTest(t)

	req := &schedulerpb.CreateFromMigrationStateRequest{
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
		},
	}

	ctx := chasm.NewMutableContext(context.Background(), node)
	sched, err := scheduler.CreateSchedulerFromMigration(ctx, req)
	require.NoError(t, err)

	require.Equal(t, int64(1), sched.ConflictToken)
	require.Empty(t, sched.Backfillers)

	invoker := sched.Invoker.Get(ctx)
	require.Empty(t, invoker.BufferedStarts)

	require.NoError(t, node.SetRootComponent(sched))
	_, err = node.CloseTransaction()
	require.NoError(t, err)
}

func TestContextMetadata(t *testing.T) {
	t.Run("returns workflow type and task queue", func(t *testing.T) {
		sched, ctx, _ := setupSchedulerForTest(t)
		sched.Schedule.Action = &schedulepb.ScheduleAction{
			Action: &schedulepb.ScheduleAction_StartWorkflow{
				StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
					WorkflowType: &commonpb.WorkflowType{Name: "my-workflow"},
					TaskQueue:    &taskqueuepb.TaskQueue{Name: "my-task-queue"},
				},
			},
		}

		md := sched.ContextMetadata(ctx)
		require.Equal(t, map[string]string{
			"workflow-type":       "my-workflow",
			"workflow-task-queue": "my-task-queue",
		}, md)
	})

	t.Run("returns only workflow type when task queue is empty", func(t *testing.T) {
		sched, ctx, _ := setupSchedulerForTest(t)
		// defaultSchedule sets WorkflowType but no TaskQueue
		md := sched.ContextMetadata(ctx)
		require.Equal(t, map[string]string{
			"workflow-type": "scheduled-wf-type",
		}, md)
	})

	t.Run("returns nil when action is empty", func(t *testing.T) {
		sched, ctx, _ := setupSchedulerForTest(t)
		sched.Schedule.Action = nil

		md := sched.ContextMetadata(ctx)
		require.Nil(t, md)
	})
}

func TestSearchAttributes_NextFireTimeAndCloseTime(t *testing.T) {
	const (
		nextFireAlias = "TemporalScheduleNextFireTime"
		closeAlias    = "TemporalScheduleCloseTime"
	)

	t.Run("open with future actions emits NextFireTime, not CloseTime", func(t *testing.T) {
		sched, ctx, _ := setupSchedulerForTest(t)

		nextFire := time.Now().Add(15 * time.Minute).UTC().Truncate(time.Second)
		later := nextFire.Add(time.Hour)
		generator := sched.Generator.Get(ctx)
		generator.FutureActionTimes = []*timestamppb.Timestamp{
			timestamppb.New(nextFire),
			timestamppb.New(later),
		}

		sas := sched.SearchAttributes(ctx)
		val, ok := findSearchAttribute(t, sas, nextFireAlias)
		require.True(t, ok, "expected %s to be present", nextFireAlias)
		require.True(t, nextFire.Equal(val.(time.Time)), "want %v, got %v", nextFire, val)

		_, hasClose := findSearchAttribute(t, sas, closeAlias)
		require.False(t, hasClose, "expected %s to be absent while open", closeAlias)
	})

	t.Run("open with no future actions emits neither", func(t *testing.T) {
		sched, ctx, _ := setupSchedulerForTest(t)

		generator := sched.Generator.Get(ctx)
		generator.FutureActionTimes = nil

		sas := sched.SearchAttributes(ctx)
		_, hasNext := findSearchAttribute(t, sas, nextFireAlias)
		require.False(t, hasNext, "expected %s to be absent when there is no upcoming action", nextFireAlias)
		_, hasClose := findSearchAttribute(t, sas, closeAlias)
		require.False(t, hasClose, "expected %s to be absent while open", closeAlias)
	})

	t.Run("closed emits CloseTime, not NextFireTime", func(t *testing.T) {
		sched, ctx, _ := setupSchedulerForTest(t)

		closedAt := time.Now().Add(-5 * time.Minute).UTC().Truncate(time.Second)
		sched.Closed = true
		sched.ClosedTime = timestamppb.New(closedAt)
		// Future action times left over from before close must not leak through.
		generator := sched.Generator.Get(ctx)
		generator.FutureActionTimes = []*timestamppb.Timestamp{timestamppb.New(time.Now().Add(time.Hour))}

		sas := sched.SearchAttributes(ctx)
		val, ok := findSearchAttribute(t, sas, closeAlias)
		require.True(t, ok, "expected %s to be present once closed", closeAlias)
		require.True(t, closedAt.Equal(val.(time.Time)), "want %v, got %v", closedAt, val)

		_, hasNext := findSearchAttribute(t, sas, nextFireAlias)
		require.False(t, hasNext, "expected %s to be absent once closed", nextFireAlias)
	})

	t.Run("closed with no ClosedTime emits neither", func(t *testing.T) {
		sched, ctx, _ := setupSchedulerForTest(t)

		sched.Closed = true
		sched.ClosedTime = nil

		sas := sched.SearchAttributes(ctx)
		_, hasNext := findSearchAttribute(t, sas, nextFireAlias)
		require.False(t, hasNext)
		_, hasClose := findSearchAttribute(t, sas, closeAlias)
		require.False(t, hasClose, "expected %s to be absent when ClosedTime unset", closeAlias)
	})

	t.Run("sentinel emits neither", func(t *testing.T) {
		sentinel, ctx, _ := setupSentinelForTest(t)

		sas := sentinel.SearchAttributes(ctx)
		_, hasNext := findSearchAttribute(t, sas, nextFireAlias)
		require.False(t, hasNext)
		_, hasClose := findSearchAttribute(t, sas, closeAlias)
		require.False(t, hasClose)
	})
}

func findSearchAttribute(t *testing.T, sas []chasm.SearchAttributeKeyValue, alias string) (any, bool) {
	t.Helper()
	for _, sa := range sas {
		if sa.Alias == alias {
			return sa.Value.Value(), true
		}
	}
	return nil, false
}

func TestDelete_StampsClosedTime(t *testing.T) {
	sched, ctx, _ := setupSchedulerForTest(t)

	require.False(t, sched.Closed)
	require.Nil(t, sched.ClosedTime)

	before := time.Now()
	_, err := sched.Delete(ctx, &schedulerpb.DeleteScheduleRequest{
		NamespaceId: "ns-id",
	})
	require.NoError(t, err)
	after := time.Now()

	require.True(t, sched.Closed)
	require.NotNil(t, sched.ClosedTime)
	stamped := sched.ClosedTime.AsTime()
	require.False(t, stamped.Before(before.Add(-time.Second)),
		"ClosedTime %v is before test start %v", stamped, before)
	require.False(t, stamped.After(after.Add(time.Second)),
		"ClosedTime %v is after test end %v", stamped, after)
}

// TestSearchAttributes_RoundTripThroughCloseTransaction verifies that the new
// SAs survive registration/serialization by flowing through the same CHASM
// pipeline that runs in production — closeTransactionForceUpdateVisibility,
// which calls SearchAttributes() on the root component and snapshots the
// result. If the new attributes were unregistered or had a bad value type, the
// CloseTransaction call would error or panic.
func TestSearchAttributes_RoundTripThroughCloseTransaction(t *testing.T) {
	sched, ctx, node := setupSchedulerForTest(t)

	nextFire := time.Now().Add(time.Hour).UTC().Truncate(time.Second)
	generator := sched.Generator.Get(ctx)
	generator.FutureActionTimes = []*timestamppb.Timestamp{timestamppb.New(nextFire)}

	require.NoError(t, node.SetRootComponent(sched))
	_, err := node.CloseTransaction()
	require.NoError(t, err, "CloseTransaction should accept TemporalScheduleNextFireTime")

	// Now flip to closed and ensure the close-time variant also survives a
	// CloseTransaction.
	ctx = chasm.NewMutableContext(context.Background(), node)
	sched.Closed = true
	sched.ClosedTime = timestamppb.New(time.Now().UTC().Truncate(time.Second))
	generator = sched.Generator.Get(ctx)
	generator.FutureActionTimes = nil

	require.NoError(t, node.SetRootComponent(sched))
	_, err = node.CloseTransaction()
	require.NoError(t, err, "CloseTransaction should accept TemporalScheduleCloseTime")
}
