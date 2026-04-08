package scheduler_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
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
