package scheduler_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/chasm/lib/scheduler"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type schedulerNexusCompletionSuite struct {
	schedulerSuite
}

func TestSchedulerNexusCompletionSuite(t *testing.T) {
	suite.Run(t, &schedulerNexusCompletionSuite{})
}

func (s *schedulerNexusCompletionSuite) SetupTest() {
	s.SetupSuite()
}

type nexusCompletionTestCase struct {
	SetupInvoker   func(*scheduler.Invoker)
	SetupScheduler func(*scheduler.Scheduler)
	Completion     *persistencespb.ChasmNexusCompletion

	ExpectPaused          bool
	ExpectStatus          enumspb.WorkflowExecutionStatus
	ExpectInRecentActions bool
	ExpectNoOp            bool // For idempotent case
	ValidateInvoker       func(*testing.T, *scheduler.Invoker)
}

func (s *schedulerNexusCompletionSuite) TestHandleNexusCompletion_Success() {
	s.runTestCase(&nexusCompletionTestCase{
		SetupInvoker: func(invoker *scheduler.Invoker) {
			invoker.RequestIdToWorkflowId = map[string]string{
				"req-1": "wf-1",
			}
		},
		SetupScheduler: func(sched *scheduler.Scheduler) {
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
		Completion: &persistencespb.ChasmNexusCompletion{
			RequestId: "req-1",
			Outcome: &persistencespb.ChasmNexusCompletion_Success{
				Success: &commonpb.Payload{Data: []byte("success-data")},
			},
			CloseTime: timestamppb.New(time.Now()),
		},
		ExpectPaused:          false,
		ExpectStatus:          enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		ExpectInRecentActions: true,
	})
}

func (s *schedulerNexusCompletionSuite) TestHandleNexusCompletion_Failure() {
	s.runTestCase(&nexusCompletionTestCase{
		SetupInvoker: func(invoker *scheduler.Invoker) {
			invoker.RequestIdToWorkflowId = map[string]string{
				"req-1": "wf-1",
			}
		},
		SetupScheduler: func(sched *scheduler.Scheduler) {
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
		Completion: &persistencespb.ChasmNexusCompletion{
			RequestId: "req-1",
			Outcome: &persistencespb.ChasmNexusCompletion_Failure{
				Failure: &failurepb.Failure{
					Message: "workflow failed",
				},
			},
			CloseTime: timestamppb.New(time.Now()),
		},
		ExpectPaused:          false,
		ExpectStatus:          enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
		ExpectInRecentActions: true,
	})
}

func (s *schedulerNexusCompletionSuite) TestHandleNexusCompletion_PauseOnFailure() {
	s.runTestCase(&nexusCompletionTestCase{
		SetupInvoker: func(invoker *scheduler.Invoker) {
			invoker.RequestIdToWorkflowId = map[string]string{
				"req-1": "wf-1",
			}
		},
		SetupScheduler: func(sched *scheduler.Scheduler) {
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
		Completion: &persistencespb.ChasmNexusCompletion{
			RequestId: "req-1",
			Outcome: &persistencespb.ChasmNexusCompletion_Failure{
				Failure: &failurepb.Failure{
					Message: "workflow failed",
				},
			},
			CloseTime: timestamppb.New(time.Now()),
		},
		ExpectPaused:          true,
		ExpectStatus:          enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
		ExpectInRecentActions: true,
	})
}

func (s *schedulerNexusCompletionSuite) TestHandleNexusCompletion_Idempotent() {
	s.runTestCase(&nexusCompletionTestCase{
		SetupInvoker: func(invoker *scheduler.Invoker) {
			// Empty RequestIdToWorkflowId map simulates already processed
			invoker.RequestIdToWorkflowId = map[string]string{}
		},
		SetupScheduler: func(sched *scheduler.Scheduler) {
			// Setup some state to verify it doesn't change
			sched.Info.RunningWorkflows = []*commonpb.WorkflowExecution{
				{WorkflowId: "other-wf", RunId: "other-run"},
			}
		},
		Completion: &persistencespb.ChasmNexusCompletion{
			RequestId: "req-1",
			Outcome: &persistencespb.ChasmNexusCompletion_Success{
				Success: &commonpb.Payload{Data: []byte("success-data")},
			},
			CloseTime: timestamppb.New(time.Now()),
		},
		ExpectNoOp: true,
	})
}

func (s *schedulerNexusCompletionSuite) TestHandleNexusCompletion_Canceled() {
	s.runTestCase(&nexusCompletionTestCase{
		SetupInvoker: func(invoker *scheduler.Invoker) {
			invoker.RequestIdToWorkflowId = map[string]string{
				"req-1": "wf-1",
			}
		},
		SetupScheduler: func(sched *scheduler.Scheduler) {
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
		Completion: &persistencespb.ChasmNexusCompletion{
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
		ExpectPaused:          false,
		ExpectStatus:          enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED,
		ExpectInRecentActions: true,
	})
}

func (s *schedulerNexusCompletionSuite) TestHandleNexusCompletion_CompletionBeforeStart() {
	desiredTime := time.Now()
	s.runTestCase(&nexusCompletionTestCase{
		SetupInvoker: func(invoker *scheduler.Invoker) {
			invoker.RequestIdToWorkflowId = map[string]string{
				"req-1": "wf-1",
			}
			// Workflow is still in BufferedStarts (hasn't been started yet)
			invoker.BufferedStarts = []*schedulespb.BufferedStart{
				{
					RequestId:   "req-1",
					WorkflowId:  "wf-1",
					DesiredTime: timestamppb.New(desiredTime),
				},
			}
		},
		SetupScheduler: func(sched *scheduler.Scheduler) {
			// Workflow NOT in RunningWorkflows or RecentActions
		},
		Completion: &persistencespb.ChasmNexusCompletion{
			RequestId: "req-1",
			Outcome: &persistencespb.ChasmNexusCompletion_Success{
				Success: &commonpb.Payload{Data: []byte("success-data")},
			},
			CloseTime: timestamppb.New(time.Now()),
		},
		ExpectPaused:          false,
		ExpectInRecentActions: true,
		ValidateInvoker: func(t *testing.T, invoker *scheduler.Invoker) {
			s.Empty(invoker.BufferedStarts, "BufferedStart should be removed after completion")
		},
	})
}

func (s *schedulerNexusCompletionSuite) runTestCase(tc *nexusCompletionTestCase) {
	ctx := s.newMutableContext()
	sched := s.scheduler
	invoker, err := sched.Invoker.Get(ctx)
	s.NoError(err)

	if tc.SetupInvoker != nil {
		tc.SetupInvoker(invoker)
	}
	if tc.SetupScheduler != nil {
		tc.SetupScheduler(sched)
	}

	// Capture initial state for no-op test.
	initialRunningWorkflows := len(sched.Info.RunningWorkflows)
	initialRecentActions := len(sched.Info.RecentActions)
	initialLastCompletion, err := sched.LastCompletionState.Get(ctx)
	s.NoError(err)

	// Execute the handler.
	err = sched.HandleNexusCompletion(ctx, tc.Completion)
	s.NoError(err)

	if tc.ExpectNoOp {
		// Verify no changes
		s.Equal(initialRunningWorkflows, len(sched.Info.RunningWorkflows))
		s.Equal(initialRecentActions, len(sched.Info.RecentActions))
		// Verify LastCompletionState wasn't changed
		currentLastCompletion, _ := sched.LastCompletionState.Get(ctx)
		s.Equal(initialLastCompletion, currentLastCompletion)
		return
	}

	lastCompletion, err := sched.LastCompletionState.Get(ctx)
	s.NoError(err)
	s.NotNil(lastCompletion)

	if tc.Completion.GetSuccess() != nil {
		s.NotNil(lastCompletion.GetSuccess())
	} else if tc.Completion.GetFailure() != nil {
		s.NotNil(lastCompletion.GetFailure())
	}

	s.Equal(tc.ExpectPaused, sched.Schedule.State.Paused)
	if tc.ExpectPaused {
		s.NotEmpty(sched.Schedule.State.Notes)
		s.Contains(sched.Schedule.State.Notes, "wf-1")
	}

	// Verify workflow removed from RunningWorkflows.
	for _, wf := range sched.Info.RunningWorkflows {
		s.NotEqual("wf-1", wf.WorkflowId)
	}

	// Verify RecentActions was updated.
	if tc.ExpectInRecentActions {
		found := false
		for _, action := range sched.Info.RecentActions {
			if action.StartWorkflowResult.WorkflowId == "wf-1" {
				s.Equal(tc.ExpectStatus, action.StartWorkflowStatus)
				found = true
				break
			}
		}
		s.True(found, "workflow should be in RecentActions")
	}

	// Verify RequestId cleaned up from Invoker.
	s.Empty(invoker.GetWorkflowID(tc.Completion.RequestId))

	if tc.ValidateInvoker != nil {
		tc.ValidateInvoker(s.T(), invoker)
	}
}
