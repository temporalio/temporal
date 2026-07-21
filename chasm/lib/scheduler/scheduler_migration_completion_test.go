package scheduler_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler"
	"go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestMigrationCompletion_ClosedFailedExecutionUsesTerminalTransition(t *testing.T) {
	sched, ctx, _ := setupSchedulerForTest(t)
	closeTime := time.Now()
	invoker := sched.Invoker.Get(ctx)
	sched.Schedule.Policies.PauseOnFailure = true
	sched.LastCompletionResult = chasm.NewDataField(ctx, &schedulerpb.LastCompletionResult{
		Success: &commonpb.Payload{Data: []byte("previous success")},
		Failure: &failurepb.Failure{Message: "previous failure"},
	})

	invoker.BufferedStarts = []*schedulespb.BufferedStart{
		{
			RequestId:  "closed",
			WorkflowId: "closed-workflow",
			RunId:      "closed-run",
			Attempt:    1,
			ActualTime: timestamppb.New(closeTime.Add(-time.Minute)),
			StartTime:  timestamppb.New(closeTime.Add(-30 * time.Second)),
		},
		{
			RequestId:  "deferred",
			WorkflowId: "deferred-workflow",
			Attempt:    -1,
			ActualTime: timestamppb.New(closeTime),
		},
	}
	for n := 0; n < scheduler.RecentActionCount; n++ {
		invoker.BufferedStarts = append(invoker.BufferedStarts, &schedulespb.BufferedStart{
			RequestId:  "retained",
			WorkflowId: "retained-workflow",
			Completed: &schedulespb.CompletedResult{
				Status:    enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
				CloseTime: timestamppb.New(closeTime.Add(time.Duration(-n-1) * time.Minute)),
			},
		})
	}

	recorded := sched.RecordMigrationCompletion(ctx, &schedulespb.CompletedResult{
		Status:    enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
		CloseTime: timestamppb.New(closeTime),
	}, "closed")
	require.True(t, recorded)

	require.True(t, sched.Schedule.State.Paused)
	require.Contains(t, sched.Schedule.State.Notes, "closed-workflow")
	last := sched.LastCompletionResult.Get(ctx)
	require.Equal(t, []byte("previous success"), last.Success.Data)
	require.Equal(t, "previous failure", last.Failure.Message)

	require.Len(t, invoker.BufferedStarts, scheduler.RecentActionCount+1) // deferred start plus retained completions
	var deferred, closed *schedulespb.BufferedStart
	for _, start := range invoker.BufferedStarts {
		switch start.RequestId {
		case "deferred":
			deferred = start
		case "closed":
			closed = start
		}
	}
	require.NotNil(t, closed)
	require.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_FAILED, closed.Completed.Status)
	require.True(t, closed.HasCallback)
	require.NotNil(t, deferred)
	require.Equal(t, int64(0), deferred.Attempt)
	require.Equal(t, closeTime.Unix(), deferred.DesiredTime.AsTime().Unix())
}

func TestMigrationCompletion_ClosedSuccessfulExecutionDoesNotFabricateResult(t *testing.T) {
	sched, ctx, _ := setupSchedulerForTest(t)
	invoker := sched.Invoker.Get(ctx)
	sched.LastCompletionResult = chasm.NewDataField(ctx, &schedulerpb.LastCompletionResult{
		Success: &commonpb.Payload{Data: []byte("previous success")},
		Failure: &failurepb.Failure{Message: "previous failure"},
	})
	invoker.BufferedStarts = []*schedulespb.BufferedStart{{
		RequestId:  "closed",
		WorkflowId: "closed-workflow",
		RunId:      "closed-run",
		Attempt:    1,
	}}

	require.True(t, sched.RecordMigrationCompletion(ctx, &schedulespb.CompletedResult{
		Status:    enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		CloseTime: timestamppb.Now(),
	}, "closed"))
	last := sched.LastCompletionResult.Get(ctx)
	require.Equal(t, []byte("previous success"), last.Success.Data)
	require.Equal(t, "previous failure", last.Failure.Message)
	require.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, invoker.BufferedStarts[0].Completed.Status)
}
