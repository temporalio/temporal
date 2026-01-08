package scheduler_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type recordCompletedActionTestCase struct {
	name           string
	setupScheduler func(*scheduler.Scheduler, chasm.MutableContext)
	requestID      string
	completed      *schedulespb.CompletedResult
	validate       func(*testing.T, *scheduler.Scheduler, chasm.Context)
}

func executeRecordCompletedAction(t *testing.T, tc recordCompletedActionTestCase) {
	sched, ctx, node := setupSchedulerForTest(t)

	if tc.setupScheduler != nil {
		tc.setupScheduler(sched, ctx)
	}

	sched.RecordCompletedAction(ctx, tc.completed, tc.requestID)

	_, err := node.CloseTransaction()
	require.NoError(t, err)

	readCtx := chasm.NewContext(context.Background(), node)
	if tc.validate != nil {
		tc.validate(t, sched, readCtx)
	}
}

// TestRecordCompletedAction_SingleRunningWorkflow verifies that when
// a workflow exists in BufferedStarts with a RunId (running), completing it
// sets the Completed field.
func TestRecordCompletedAction_SingleRunningWorkflow(t *testing.T) {
	closeTime := time.Now()
	tc := recordCompletedActionTestCase{
		name: "single running workflow",
		setupScheduler: func(sched *scheduler.Scheduler, ctx chasm.MutableContext) {
			invoker := sched.Invoker.Get(ctx)
			invoker.BufferedStarts = []*schedulespb.BufferedStart{
				{
					RequestId:  "req-1",
					WorkflowId: "wf-1",
					RunId:      "run-1",
					Attempt:    1,
					ActualTime: timestamppb.New(time.Now().Add(-1 * time.Minute)),
					StartTime:  timestamppb.New(time.Now().Add(-30 * time.Second)),
				},
			}
		},
		requestID: "req-1",
		completed: &schedulespb.CompletedResult{
			Status:    enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
			CloseTime: timestamppb.New(closeTime),
		},
		validate: func(t *testing.T, sched *scheduler.Scheduler, ctx chasm.Context) {
			invoker := sched.Invoker.Get(ctx)
			require.Len(t, invoker.BufferedStarts, 1)
			require.NotNil(t, invoker.BufferedStarts[0].Completed)
			require.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, invoker.BufferedStarts[0].Completed.Status)
		},
	}

	executeRecordCompletedAction(t, tc)
}

// TestRecordCompletedAction_MultipleWorkflows verifies that when multiple
// workflows exist in BufferedStarts, only the one with the matching requestID
// is marked as completed.
func TestRecordCompletedAction_MultipleWorkflows(t *testing.T) {
	closeTime := time.Now()
	tc := recordCompletedActionTestCase{
		name: "multiple workflows, complete one",
		setupScheduler: func(sched *scheduler.Scheduler, ctx chasm.MutableContext) {
			invoker := sched.Invoker.Get(ctx)
			invoker.BufferedStarts = []*schedulespb.BufferedStart{
				{
					RequestId:  "req-1",
					WorkflowId: "wf-1",
					RunId:      "run-1",
					Attempt:    1,
					ActualTime: timestamppb.New(time.Now().Add(-2 * time.Minute)),
					StartTime:  timestamppb.New(time.Now().Add(-90 * time.Second)),
				},
				{
					RequestId:  "req-2",
					WorkflowId: "wf-2",
					RunId:      "run-2",
					Attempt:    1,
					ActualTime: timestamppb.New(time.Now().Add(-1 * time.Minute)),
					StartTime:  timestamppb.New(time.Now().Add(-30 * time.Second)),
				},
			}
		},
		requestID: "req-1",
		completed: &schedulespb.CompletedResult{
			Status:    enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
			CloseTime: timestamppb.New(closeTime),
		},
		validate: func(t *testing.T, sched *scheduler.Scheduler, ctx chasm.Context) {
			invoker := sched.Invoker.Get(ctx)
			require.Len(t, invoker.BufferedStarts, 2)

			// Find workflows by RequestId since applyCompletedRetention reorders
			// (non-completed first, then completed)
			var req1Start, req2Start *schedulespb.BufferedStart
			for _, start := range invoker.BufferedStarts {
				switch start.RequestId {
				case "req-1":
					req1Start = start
				case "req-2":
					req2Start = start
				default:
				}
			}
			require.NotNil(t, req1Start)
			require.NotNil(t, req2Start)

			// First workflow (req-1) should be completed
			require.NotNil(t, req1Start.Completed)
			require.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_FAILED, req1Start.Completed.Status)

			// Second workflow (req-2) should still be running
			require.Nil(t, req2Start.Completed)
		},
	}

	executeRecordCompletedAction(t, tc)
}

// TestRecordCompletedAction_UpdatesDesiredTimeOnNextPending verifies that
// completing a workflow updates the DesiredTime on the first pending start
// (Attempt == 0) to the close time of the completed workflow.
func TestRecordCompletedAction_UpdatesDesiredTimeOnNextPending(t *testing.T) {
	closeTime := time.Now()
	tc := recordCompletedActionTestCase{
		name: "updates DesiredTime on next pending start",
		setupScheduler: func(sched *scheduler.Scheduler, ctx chasm.MutableContext) {
			invoker := sched.Invoker.Get(ctx)
			invoker.BufferedStarts = []*schedulespb.BufferedStart{
				{
					RequestId:  "req-1",
					WorkflowId: "wf-1",
					RunId:      "run-1",
					Attempt:    1,
					ActualTime: timestamppb.New(time.Now().Add(-2 * time.Minute)),
					StartTime:  timestamppb.New(time.Now().Add(-90 * time.Second)),
				},
				{
					RequestId:   "req-2",
					WorkflowId:  "wf-2",
					Attempt:     0, // pending
					ActualTime:  timestamppb.New(time.Now()),
					DesiredTime: timestamppb.New(time.Now()),
				},
			}
		},
		requestID: "req-1",
		completed: &schedulespb.CompletedResult{
			Status:    enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
			CloseTime: timestamppb.New(closeTime),
		},
		validate: func(t *testing.T, sched *scheduler.Scheduler, ctx chasm.Context) {
			invoker := sched.Invoker.Get(ctx)
			require.Len(t, invoker.BufferedStarts, 2)

			// Find the pending start (req-2) by RequestId since applyCompletedRetention reorders
			// (non-completed first, then completed)
			var req2Start *schedulespb.BufferedStart
			for _, start := range invoker.BufferedStarts {
				if start.RequestId == "req-2" {
					req2Start = start
					break
				}
			}
			require.NotNil(t, req2Start)

			// Pending start (req-2) should have DesiredTime updated to closeTime
			require.Equal(t, closeTime.Unix(), req2Start.DesiredTime.AsTime().Unix())
		},
	}

	executeRecordCompletedAction(t, tc)
}

// TestRecordCompletedAction_NoMatchingRequest verifies that when the requestID
// doesn't match any BufferedStart, no changes are made.
func TestRecordCompletedAction_NoMatchingRequest(t *testing.T) {
	closeTime := time.Now()
	tc := recordCompletedActionTestCase{
		name: "no matching request",
		setupScheduler: func(sched *scheduler.Scheduler, ctx chasm.MutableContext) {
			invoker := sched.Invoker.Get(ctx)
			invoker.BufferedStarts = []*schedulespb.BufferedStart{
				{
					RequestId:  "req-1",
					WorkflowId: "wf-1",
					RunId:      "run-1",
					Attempt:    1,
					ActualTime: timestamppb.New(time.Now().Add(-1 * time.Minute)),
					StartTime:  timestamppb.New(time.Now().Add(-30 * time.Second)),
				},
			}
		},
		requestID: "req-nonexistent",
		completed: &schedulespb.CompletedResult{
			Status:    enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
			CloseTime: timestamppb.New(closeTime),
		},
		validate: func(t *testing.T, sched *scheduler.Scheduler, ctx chasm.Context) {
			invoker := sched.Invoker.Get(ctx)
			require.Len(t, invoker.BufferedStarts, 1)
			// Original workflow should still be running (not completed)
			require.Nil(t, invoker.BufferedStarts[0].Completed)
		},
	}

	executeRecordCompletedAction(t, tc)
}
