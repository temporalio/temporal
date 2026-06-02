package scheduler_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type nexusCompletionTestCase struct {
	name              string
	setupInvoker      func(*scheduler.Invoker)
	setupScheduler    func(*scheduler.Scheduler)
	completion        *persistencespb.ChasmNexusCompletion
	expectPaused      bool
	expectStatus      enumspb.WorkflowExecutionStatus
	expectNoOp        bool
	validateInvoker   func(*testing.T, *scheduler.Invoker)
	validateScheduler func(*testing.T, *scheduler.Scheduler, chasm.Context)
}

func executeNexusCompletion(t *testing.T, tc nexusCompletionTestCase) {
	sched, ctx, node := setupSchedulerForTest(t)

	invoker := sched.Invoker.Get(ctx)

	if tc.setupInvoker != nil {
		tc.setupInvoker(invoker)
	}
	if tc.setupScheduler != nil {
		tc.setupScheduler(sched)
	}

	initialLastCompletion := sched.LastCompletionResult.Get(ctx)

	err := sched.HandleNexusCompletion(ctx, tc.completion)
	require.NoError(t, err)

	_, err = node.CloseTransaction()
	require.NoError(t, err)

	readCtx := chasm.NewContext(context.Background(), node)

	if tc.expectNoOp {
		currentLastCompletion := sched.LastCompletionResult.Get(readCtx)
		require.Equal(t, initialLastCompletion, currentLastCompletion)
		return
	}

	lastCompletion := sched.LastCompletionResult.Get(readCtx)
	require.NotNil(t, lastCompletion)

	if tc.completion.GetSuccess() != nil {
		require.NotNil(t, lastCompletion.GetSuccess())
	} else if tc.completion.GetFailure() != nil {
		require.NotNil(t, lastCompletion.GetFailure())
	}

	require.Equal(t, tc.expectPaused, sched.Schedule.State.Paused)
	if tc.expectPaused {
		require.NotEmpty(t, sched.Schedule.State.Notes)
		require.Contains(t, sched.Schedule.State.Notes, "wf-1")
	}

	// Check that workflow ID lookup now returns empty (request completed)
	require.Empty(t, invoker.RunningWorkflowID(tc.completion.RequestId))

	// If we expect a specific status, verify the BufferedStart has Completed set
	if tc.expectStatus != enumspb.WORKFLOW_EXECUTION_STATUS_UNSPECIFIED {
		found := false
		for _, start := range invoker.BufferedStarts {
			if start.GetWorkflowId() == "wf-1" && start.GetCompleted() != nil {
				require.Equal(t, tc.expectStatus, start.GetCompleted().GetStatus())
				found = true
				break
			}
		}
		require.True(t, found, "expected to find completed BufferedStart with workflow ID wf-1")
	}

	if tc.validateInvoker != nil {
		tc.validateInvoker(t, invoker)
	}
	if tc.validateScheduler != nil {
		tc.validateScheduler(t, sched, readCtx)
	}
}

// TestHandleNexusCompletion_Success verifies that a successful workflow completion
// is properly recorded with the success payload and workflow status is updated.
func TestHandleNexusCompletion_Success(t *testing.T) {
	tc := nexusCompletionTestCase{
		name: "successful completion",
		setupInvoker: func(invoker *scheduler.Invoker) {
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
		completion: &persistencespb.ChasmNexusCompletion{
			RequestId: "req-1",
			Outcome: &persistencespb.ChasmNexusCompletion_Success{
				Success: &commonpb.Payload{Data: []byte("success-data")},
			},
			CloseTime: timestamppb.New(time.Now()),
		},
		expectPaused: false,
		expectStatus: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
	}

	executeNexusCompletion(t, tc)
}

// TestHandleNexusCompletion_Failure verifies that a failed workflow completion
// is properly recorded with the failure payload and workflow status is updated.
func TestHandleNexusCompletion_Failure(t *testing.T) {
	tc := nexusCompletionTestCase{
		name: "failed completion",
		setupInvoker: func(invoker *scheduler.Invoker) {
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
		completion: &persistencespb.ChasmNexusCompletion{
			RequestId: "req-1",
			Outcome: &persistencespb.ChasmNexusCompletion_Failure{
				Failure: &failurepb.Failure{
					Message: "workflow failed",
				},
			},
			CloseTime: timestamppb.New(time.Now()),
		},
		expectPaused: false,
		expectStatus: enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
	}

	executeNexusCompletion(t, tc)
}

// TestHandleNexusCompletion_PauseOnFailure verifies that when PauseOnFailure is enabled,
// a workflow failure causes the schedule to be paused and notes to be set.
func TestHandleNexusCompletion_PauseOnFailure(t *testing.T) {
	tc := nexusCompletionTestCase{
		name: "pause on failure",
		setupInvoker: func(invoker *scheduler.Invoker) {
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
		setupScheduler: func(sched *scheduler.Scheduler) {
			sched.Schedule.Policies.PauseOnFailure = true
		},
		completion: &persistencespb.ChasmNexusCompletion{
			RequestId: "req-1",
			Outcome: &persistencespb.ChasmNexusCompletion_Failure{
				Failure: &failurepb.Failure{
					Message: "workflow failed",
				},
			},
			CloseTime: timestamppb.New(time.Now()),
		},
		expectPaused: true,
		expectStatus: enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
	}

	executeNexusCompletion(t, tc)
}

// TestHandleNexusCompletion_Idempotent verifies that handling a completion for an
// already-processed request ID (not in BufferedStarts) is a no-op.
func TestHandleNexusCompletion_Idempotent(t *testing.T) {
	tc := nexusCompletionTestCase{
		name: "idempotent completion",
		setupInvoker: func(invoker *scheduler.Invoker) {
			// Empty BufferedStarts - request was already processed
			invoker.BufferedStarts = []*schedulespb.BufferedStart{}
		},
		completion: &persistencespb.ChasmNexusCompletion{
			RequestId: "req-1",
			Outcome: &persistencespb.ChasmNexusCompletion_Success{
				Success: &commonpb.Payload{Data: []byte("success-data")},
			},
			CloseTime: timestamppb.New(time.Now()),
		},
		expectNoOp: true,
	}

	executeNexusCompletion(t, tc)
}

// TestHandleNexusCompletion_Canceled verifies that a canceled workflow completion
// is properly recorded with CANCELED status.
func TestHandleNexusCompletion_Canceled(t *testing.T) {
	tc := nexusCompletionTestCase{
		name: "canceled completion",
		setupInvoker: func(invoker *scheduler.Invoker) {
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
		completion: &persistencespb.ChasmNexusCompletion{
			RequestId: "req-1",
			Outcome: &persistencespb.ChasmNexusCompletion_Failure{
				Failure: &failurepb.Failure{
					Message: "workflow canceled",
					FailureInfo: &failurepb.Failure_CanceledFailureInfo{
						CanceledFailureInfo: &failurepb.CanceledFailureInfo{},
					},
				},
			},
			CloseTime: timestamppb.New(time.Now()),
		},
		expectPaused: false,
		expectStatus: enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED,
	}

	executeNexusCompletion(t, tc)
}

// TestHandleNexusCompletion_CompletionBeforeStart verifies that a workflow can
// complete before its start is recorded (workflow has a BufferedStart but no RunId yet).
func TestHandleNexusCompletion_CompletionBeforeStart(t *testing.T) {
	desiredTime := time.Now()
	tc := nexusCompletionTestCase{
		name: "completion before start",
		setupInvoker: func(invoker *scheduler.Invoker) {
			invoker.BufferedStarts = []*schedulespb.BufferedStart{
				{
					RequestId:   "req-1",
					WorkflowId:  "wf-1",
					Attempt:     1,
					ActualTime:  timestamppb.New(desiredTime),
					DesiredTime: timestamppb.New(desiredTime),
					// No RunId - workflow hasn't been started yet in our records
				},
			}
		},
		completion: &persistencespb.ChasmNexusCompletion{
			RequestId: "req-1",
			Outcome: &persistencespb.ChasmNexusCompletion_Success{
				Success: &commonpb.Payload{Data: []byte("success-data")},
			},
			CloseTime: timestamppb.New(time.Now()),
		},
		expectPaused: false,
		expectStatus: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		validateInvoker: func(t *testing.T, invoker *scheduler.Invoker) {
			require.Len(t, invoker.BufferedStarts, 1)
			require.NotNil(t, invoker.BufferedStarts[0].Completed)
		},
	}

	executeNexusCompletion(t, tc)
}
