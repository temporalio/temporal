package scheduler_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
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
	validateScheduler func(*testing.T, *scheduler.Scheduler)
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

	initialRunningWorkflows := len(sched.Info.RunningWorkflows)
	initialRecentActions := len(sched.Info.RecentActions)
	initialLastCompletion := sched.LastCompletionResult.Get(ctx)

	err := sched.HandleNexusCompletion(ctx, tc.completion)
	require.NoError(t, err)

	_, err = node.CloseTransaction()
	require.NoError(t, err)

	if tc.expectNoOp {
		require.Len(t, sched.Info.RunningWorkflows, initialRunningWorkflows)
		require.Len(t, sched.Info.RecentActions, initialRecentActions)
		currentLastCompletion := sched.LastCompletionResult.Get(ctx)
		require.Equal(t, initialLastCompletion, currentLastCompletion)
		return
	}

	lastCompletion := sched.LastCompletionResult.Get(ctx)
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

	for _, wf := range sched.Info.RunningWorkflows {
		require.NotEqual(t, "wf-1", wf.WorkflowId)
	}

	if tc.expectStatus != enumspb.WORKFLOW_EXECUTION_STATUS_UNSPECIFIED {
		found := false
		for _, action := range sched.Info.RecentActions {
			if action.StartWorkflowResult.WorkflowId == "wf-1" {
				require.Equal(t, tc.expectStatus, action.StartWorkflowStatus)
				found = true
				break
			}
		}
		require.True(t, found)
	}

	require.Empty(t, invoker.WorkflowID(tc.completion.RequestId))

	if tc.validateInvoker != nil {
		tc.validateInvoker(t, invoker)
	}
	if tc.validateScheduler != nil {
		tc.validateScheduler(t, sched)
	}
}

// TestHandleNexusCompletion_Success verifies that a successful workflow completion
// is properly recorded with the success payload and workflow status is updated.
func TestHandleNexusCompletion_Success(t *testing.T) {
	tc := nexusCompletionTestCase{
		name: "successful completion",
		setupInvoker: func(invoker *scheduler.Invoker) {
			invoker.RequestIdToWorkflowId = map[string]string{
				"req-1": "wf-1",
			}
		},
		setupScheduler: func(sched *scheduler.Scheduler) {
			sched.Info.RunningWorkflows = []*commonpb.WorkflowExecution{
				{WorkflowId: "wf-1", RunId: "run-1"},
			}
			sched.Info.RecentActions = []*schedulepb.ScheduleActionResult{
				{
					StartWorkflowResult: &commonpb.WorkflowExecution{
						WorkflowId: "wf-1",
						RunId:      "run-1",
					},
					StartWorkflowStatus: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
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
			invoker.RequestIdToWorkflowId = map[string]string{
				"req-1": "wf-1",
			}
		},
		setupScheduler: func(sched *scheduler.Scheduler) {
			sched.Info.RunningWorkflows = []*commonpb.WorkflowExecution{
				{WorkflowId: "wf-1", RunId: "run-1"},
			}
			sched.Info.RecentActions = []*schedulepb.ScheduleActionResult{
				{
					StartWorkflowResult: &commonpb.WorkflowExecution{
						WorkflowId: "wf-1",
						RunId:      "run-1",
					},
					StartWorkflowStatus: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
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
			invoker.RequestIdToWorkflowId = map[string]string{
				"req-1": "wf-1",
			}
		},
		setupScheduler: func(sched *scheduler.Scheduler) {
			sched.Schedule.Policies.PauseOnFailure = true
			sched.Info.RunningWorkflows = []*commonpb.WorkflowExecution{
				{WorkflowId: "wf-1", RunId: "run-1"},
			}
			sched.Info.RecentActions = []*schedulepb.ScheduleActionResult{
				{
					StartWorkflowResult: &commonpb.WorkflowExecution{
						WorkflowId: "wf-1",
						RunId:      "run-1",
					},
					StartWorkflowStatus: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
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
		expectPaused: true,
		expectStatus: enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
	}

	executeNexusCompletion(t, tc)
}

// TestHandleNexusCompletion_Idempotent verifies that handling a completion for an
// already-processed request ID is a no-op and doesn't modify scheduler state.
func TestHandleNexusCompletion_Idempotent(t *testing.T) {
	tc := nexusCompletionTestCase{
		name: "idempotent completion",
		setupInvoker: func(invoker *scheduler.Invoker) {
			invoker.RequestIdToWorkflowId = map[string]string{}
		},
		setupScheduler: func(sched *scheduler.Scheduler) {
			sched.Info.RunningWorkflows = []*commonpb.WorkflowExecution{
				{WorkflowId: "other-wf", RunId: "other-run"},
			}
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
			invoker.RequestIdToWorkflowId = map[string]string{
				"req-1": "wf-1",
			}
		},
		setupScheduler: func(sched *scheduler.Scheduler) {
			sched.Info.RunningWorkflows = []*commonpb.WorkflowExecution{
				{WorkflowId: "wf-1", RunId: "run-1"},
			}
			sched.Info.RecentActions = []*schedulepb.ScheduleActionResult{
				{
					StartWorkflowResult: &commonpb.WorkflowExecution{
						WorkflowId: "wf-1",
						RunId:      "run-1",
					},
					StartWorkflowStatus: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
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
// complete before its start is recorded, and the completion is properly handled
// with the workflow removed from BufferedStarts.
func TestHandleNexusCompletion_CompletionBeforeStart(t *testing.T) {
	desiredTime := time.Now()
	tc := nexusCompletionTestCase{
		name: "completion before start",
		setupInvoker: func(invoker *scheduler.Invoker) {
			invoker.RequestIdToWorkflowId = map[string]string{
				"req-1": "wf-1",
			}
			invoker.BufferedStarts = []*schedulespb.BufferedStart{
				{
					RequestId:   "req-1",
					WorkflowId:  "wf-1",
					DesiredTime: timestamppb.New(desiredTime),
				},
			}
		},
		setupScheduler: func(sched *scheduler.Scheduler) {},
		completion: &persistencespb.ChasmNexusCompletion{
			RequestId: "req-1",
			Outcome: &persistencespb.ChasmNexusCompletion_Success{
				Success: &commonpb.Payload{Data: []byte("success-data")},
			},
			CloseTime: timestamppb.New(time.Now()),
		},
		expectPaused: false,
		expectStatus: enumspb.WORKFLOW_EXECUTION_STATUS_UNSPECIFIED,
		validateInvoker: func(t *testing.T, invoker *scheduler.Invoker) {
			require.Empty(t, invoker.BufferedStarts)
		},
		validateScheduler: func(t *testing.T, sched *scheduler.Scheduler) {
			found := false
			for _, action := range sched.Info.RecentActions {
				if action.StartWorkflowResult.WorkflowId == "wf-1" {
					found = true
					break
				}
			}
			require.True(t, found, "workflow should be in RecentActions")
		},
	}

	executeNexusCompletion(t, tc)
}
