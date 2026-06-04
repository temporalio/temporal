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
	"go.temporal.io/server/common/testing/testlogger"
	"go.temporal.io/server/service/history/tasks"
	"go.uber.org/mock/gomock"
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

func TestCreateSchedulerFromMigration_NoRunning(t *testing.T) {
	ctrl := gomock.NewController(t)
	infra := setupTestInfra(t, newRealSpecProcessor(ctrl, testlogger.NewTestLogger(t, testlogger.FailOnExpectedErrorOnly)))
	ctx := chasm.NewMutableContext(context.Background(), infra.node)

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
			GeneratorState: &schedulerpb.GeneratorState{
				LastProcessedTime: timestamppb.New(time.Now().Add(-time.Hour)),
			},
			InvokerState: &schedulerpb.InvokerState{},
		},
	}

	sched, err := scheduler.CreateSchedulerFromMigration(ctx, req)
	require.NoError(t, err)
	require.NoError(t, infra.node.SetRootComponent(sched))
	_, err = infra.node.CloseTransaction()
	require.NoError(t, err)

	hasGeneratorTask := false
outer:
	for _, taskList := range infra.nodeBackend.TasksByCategory {
		for _, task := range taskList {
			chasmTask, ok := task.(*tasks.ChasmTask)
			if ok && chasmTask.GetVisibilityTime().Equal(chasm.TaskScheduledTimeImmediate) {
				hasGeneratorTask = true
				break outer
			}
		}
	}
	require.True(t, hasGeneratorTask,
		"expected an immediate GeneratorTask after migration with no running workflows")
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

func TestSearchAttributes_NextActionTime(t *testing.T) {

	t.Run("open with future actions emits NextActionTime", func(t *testing.T) {
		sched, ctx, _ := setupSchedulerForTest(t)

		nextAction := time.Now().Add(15 * time.Minute).UTC().Truncate(time.Second)
		later := nextAction.Add(time.Hour)
		generator := sched.Generator.Get(ctx)
		generator.FutureActionTimes = []*timestamppb.Timestamp{
			timestamppb.New(nextAction),
			timestamppb.New(later),
		}

		sas := sched.SearchAttributes(ctx)
		val, ok := findSearchAttribute(t, sas, scheduler.ScheduleNextActionTimeName)
		require.True(t, ok, "expected %s to be present", scheduler.ScheduleNextActionTimeName)
		require.True(t, nextAction.Equal(val.(time.Time)), "want %v, got %v", nextAction, val)
	})

	t.Run("open with no future actions does not emit", func(t *testing.T) {
		sched, ctx, _ := setupSchedulerForTest(t)

		generator := sched.Generator.Get(ctx)
		generator.FutureActionTimes = nil

		sas := sched.SearchAttributes(ctx)
		_, hasNext := findSearchAttribute(t, sas, scheduler.ScheduleNextActionTimeName)
		require.False(t, hasNext, "expected %s to be absent when there is no upcoming action", scheduler.ScheduleNextActionTimeName)
	})

	t.Run("closed does not emit NextActionTime", func(t *testing.T) {
		sched, ctx, _ := setupSchedulerForTest(t)

		sched.Closed = true
		// Future action times left over from before close must not leak through.
		generator := sched.Generator.Get(ctx)
		generator.FutureActionTimes = []*timestamppb.Timestamp{timestamppb.New(time.Now().Add(time.Hour))}

		sas := sched.SearchAttributes(ctx)
		_, hasNext := findSearchAttribute(t, sas, scheduler.ScheduleNextActionTimeName)
		require.False(t, hasNext, "expected %s to be absent once closed", scheduler.ScheduleNextActionTimeName)
	})

	t.Run("sentinel does not emit", func(t *testing.T) {
		sentinel, ctx, _ := setupSentinelForTest(t)

		sas := sentinel.SearchAttributes(ctx)
		_, hasNext := findSearchAttribute(t, sas, scheduler.ScheduleNextActionTimeName)
		require.False(t, hasNext)
	})
}

func TestSearchAttributes_IdleCloseTime(t *testing.T) {

	t.Run("idle schedule emits IdleCloseTime", func(t *testing.T) {
		sched, ctx, _ := setupSchedulerForTest(t)

		closeTime := time.Now().Add(7 * 24 * time.Hour).UTC().Truncate(time.Second)
		sched.IdleCloseTime = timestamppb.New(closeTime)

		sas := sched.SearchAttributes(ctx)
		val, ok := findSearchAttribute(t, sas, scheduler.ScheduleIdleCloseTimeName)
		require.True(t, ok, "expected %s to be present", scheduler.ScheduleIdleCloseTimeName)
		require.True(t, closeTime.Equal(val.(time.Time)), "want %v, got %v", closeTime, val)
	})

	t.Run("schedule with no idle deadline does not emit", func(t *testing.T) {
		sched, ctx, _ := setupSchedulerForTest(t)

		sched.IdleCloseTime = nil

		sas := sched.SearchAttributes(ctx)
		_, has := findSearchAttribute(t, sas, scheduler.ScheduleIdleCloseTimeName)
		require.False(t, has, "expected %s to be absent when not idle", scheduler.ScheduleIdleCloseTimeName)
	})

	t.Run("closed does not emit IdleCloseTime", func(t *testing.T) {
		sched, ctx, _ := setupSchedulerForTest(t)

		sched.Closed = true
		// A deadline left over from before close must not leak through.
		sched.IdleCloseTime = timestamppb.New(time.Now().Add(7 * 24 * time.Hour))

		sas := sched.SearchAttributes(ctx)
		_, has := findSearchAttribute(t, sas, scheduler.ScheduleIdleCloseTimeName)
		require.False(t, has, "expected %s to be absent once closed", scheduler.ScheduleIdleCloseTimeName)
	})

	t.Run("sentinel does not emit", func(t *testing.T) {
		sentinel, ctx, _ := setupSentinelForTest(t)

		sas := sentinel.SearchAttributes(ctx)
		_, has := findSearchAttribute(t, sas, scheduler.ScheduleIdleCloseTimeName)
		require.False(t, has)
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

// TestSearchAttributes_RoundTripThroughCloseTransaction verifies that
// TemporalScheduleNextActionTime survives registration/serialization by
// flowing through the same CHASM pipeline that runs in production —
// closeTransactionForceUpdateVisibility, which calls SearchAttributes() on
// the root component and snapshots the result. If the attribute were
// unregistered or had a bad value type, the CloseTransaction call would
// error or panic.
func TestSearchAttributes_RoundTripThroughCloseTransaction(t *testing.T) {
	sched, ctx, node := setupSchedulerForTest(t)

	nextAction := time.Now().Add(time.Hour).UTC().Truncate(time.Second)
	generator := sched.Generator.Get(ctx)
	generator.FutureActionTimes = []*timestamppb.Timestamp{timestamppb.New(nextAction)}
	sched.IdleCloseTime = timestamppb.New(time.Now().Add(7 * 24 * time.Hour))

	require.NoError(t, node.SetRootComponent(sched))
	_, err := node.CloseTransaction()
	require.NoError(t, err, "CloseTransaction should accept the scheduler search attributes")
}
