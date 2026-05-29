package tests

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/api/applicationservice/v1"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/api/workflowservice/v1/workflowservicenexus"
	"go.temporal.io/sdk/client"
	"go.temporal.io/server/common/dynamicconfig"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

type GetWorkflowExecutionResultTestSuite struct {
	testcore.FunctionalTestBase
}

func TestGetWorkflowExecutionResultTestSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(GetWorkflowExecutionResultTestSuite))
}

func (s *GetWorkflowExecutionResultTestSuite) SetupSuite() {
	s.SetupSuiteWithCluster(
		testcore.WithDynamicConfigOverrides(map[dynamicconfig.Key]any{
			dynamicconfig.EnableChasm.Key():                      true,
			dynamicconfig.EnableCHASMCallbacks.Key():             true,
			dynamicconfig.EnableGetWorkflowExecutionResult.Key(): true,
		}),
	)
}

func (s *GetWorkflowExecutionResultTestSuite) TestGetWorkflowExecutionResult_TargetCompletesAfterStart() {
	ctx := testcore.NewContext()
	callerTaskQueue := testcore.RandomizeStr(s.T().Name())
	targetTaskQueue := testcore.RandomizeStr(s.T().Name() + "-target")
	targetWorkflowID := testcore.RandomizeStr(s.T().Name())

	// Start caller workflow.
	callerRun, err := s.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		TaskQueue: callerTaskQueue,
	}, "caller-workflow")
	s.NoError(err)
	s.T().Cleanup(func() {
		_ = s.SdkClient().TerminateWorkflow(ctx, callerRun.GetID(), callerRun.GetRunID(), "test cleanup")
	})

	// Start target workflow.
	startResp, err := s.FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
		Namespace:    s.Namespace().String(),
		WorkflowId:   targetWorkflowID,
		WorkflowType: &commonpb.WorkflowType{Name: "target-workflow"},
		TaskQueue:    &taskqueuepb.TaskQueue{Name: targetTaskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		RequestId:    uuid.NewString(),
	})
	s.NoError(err)
	targetRunID := startResp.RunId

	// Poll caller's first WFT and schedule GetWorkflowExecutionResult as a nexus operation
	// on the system endpoint.
	pollResp, err := s.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{Name: callerTaskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:  "test",
	})
	s.NoError(err)
	_, err = s.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity:  "test",
		TaskToken: pollResp.TaskToken,
		Commands: []*commandpb.Command{
			{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_NEXUS_OPERATION,
				Attributes: &commandpb.Command_ScheduleNexusOperationCommandAttributes{
					ScheduleNexusOperationCommandAttributes: &commandpb.ScheduleNexusOperationCommandAttributes{
						Endpoint:  commonnexus.SystemEndpoint,
						Service:   workflowservicenexus.TemporalAPIApplicationserviceV1ApplicationService.ServiceName,
						Operation: workflowservicenexus.TemporalAPIApplicationserviceV1ApplicationService.GetWorkflowExecutionResult.Name(),
						Input: payloads.MustEncodeSingle(&applicationservice.GetWorkflowExecutionResultRequest{
							Namespace: s.Namespace().String(),
							Execution: &commonpb.WorkflowExecution{
								WorkflowId: targetWorkflowID,
								RunId:      targetRunID,
							},
						}),
					},
				},
			},
		},
	})
	s.NoError(err)

	// Poll caller's second WFT — this fires when NexusOperationStarted is recorded after the
	// nexus executor asynchronously calls Start(). Respond with no commands to let the workflow
	// keep waiting for the nexus operation to complete.
	pollResp, err = s.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{Name: callerTaskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:  "test",
	})
	s.NoError(err)
	s.ContainsHistoryEvents(`NexusOperationStarted`, pollResp.History.Events)
	_, err = s.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity:  "test",
		TaskToken: pollResp.TaskToken,
	})
	s.NoError(err)

	// At this point the callback is registered on the TARGET. Describing the target must surface a
	// completion callback whose WorkflowEvent link points back to the CALLER (not the target
	// itself) — this is the target -> caller discovery path that lets tooling answer "which
	// workflows are waiting on this target?".
	var callerLink *commonpb.Link_WorkflowEvent
	s.EventuallyWithT(func(t *assert.CollectT) {
		descResp, descErr := s.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: s.Namespace().String(),
			Execution: &commonpb.WorkflowExecution{WorkflowId: targetWorkflowID, RunId: targetRunID},
		})
		assert.NoError(t, descErr)
		assert.NotEmpty(t, descResp.GetCallbacks(), "target should have a completion callback registered")
		callerLink = nil
		for _, cbInfo := range descResp.GetCallbacks() {
			for _, link := range cbInfo.GetCallback().GetLinks() {
				if we := link.GetWorkflowEvent(); we != nil {
					callerLink = we
				}
			}
		}
		assert.NotNil(t, callerLink, "target's callback must carry a WorkflowEvent link identifying the caller")
	}, 10*time.Second, 200*time.Millisecond)
	s.Equal(callerRun.GetID(), callerLink.GetWorkflowId(), "callback link must identify the caller workflow")
	s.Equal(callerRun.GetRunID(), callerLink.GetRunId(), "callback link must identify the caller run")
	s.Equal(s.Namespace().String(), callerLink.GetNamespace())
	s.NotEqual(targetWorkflowID, callerLink.GetWorkflowId(), "callback link must not point back at the target")

	// Complete the target workflow. The callback registered by GetWorkflowExecutionResult.Start
	// will fire and deliver the nexus operation result back to the caller.
	targetPollResp, err := s.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{Name: targetTaskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:  "test",
	})
	s.NoError(err)
	_, err = s.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity:  "test",
		TaskToken: targetPollResp.TaskToken,
		Commands: []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
				CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{},
			},
		}},
	})
	s.NoError(err)

	// Poll the WFT triggered by NexusOperationCompleted. NexusOperationCompleted is a buffered
	// event so it won't appear as the last persisted event; we rely on PollWorkflowTaskQueue
	// as the blocking wait. Buffered events are flushed when the WFT starts, so they will
	// appear in the poll response history.
	pollResp, err = s.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{Name: callerTaskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:  "test",
	})
	s.NoError(err)
	s.ContainsHistoryEvents(`
NexusOperationScheduled
NexusOperationStarted`, pollResp.History.Events)
	s.ContainsHistoryEvents(`NexusOperationCompleted`, pollResp.History.Events)

	// Complete the caller workflow.
	_, err = s.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity:  "test",
		TaskToken: pollResp.TaskToken,
		Commands: []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
				CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{},
			},
		}},
	})
	s.NoError(err)
	s.NoError(callerRun.Get(ctx, nil))
}

// TestGetWorkflowExecutionResult_TargetResetAfterCallbackAdded verifies that when the target
// workflow is reset after the GetWorkflowExecutionResult callback has been registered, the
// callback is carried over to the post-reset run and fires when that run completes — delivering
// the nexus operation result back to the caller.
//
// TODO: This test currently fails. The Nexus handler's Start() runs against the initial run and
// registers a callback via w.AddCompletionCallbacks (stored in the CHASM Workflow's Callbacks
// map). When the target is reset:
//   - The handler's Start() is not re-invoked against the new run, so no callback is registered
//     there.
//   - The original (now TERMINATED) run does not fire its callback either, so the caller never
//     receives NexusOperationCompleted.
//
// The continued-as-new path propagates runtime-added callbacks correctly (see
// TestGetWorkflowExecutionResult_TargetContinuedAsNew), but the reset path does not. Skipping
// until the reset implementation is updated to carry over runtime-registered completion
// callbacks the same way it does for callbacks registered via StartWorkflowExecution.
func (s *GetWorkflowExecutionResultTestSuite) TestGetWorkflowExecutionResult_TargetResetAfterCallbackAdded() {
	s.T().Skip("reset does not yet propagate runtime-registered completion callbacks; see comment above")

	ctx := testcore.NewContext()
	callerTaskQueue := testcore.RandomizeStr(s.T().Name())
	targetTaskQueue := testcore.RandomizeStr(s.T().Name() + "-target")
	targetWorkflowID := testcore.RandomizeStr(s.T().Name())

	callerRun, err := s.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		TaskQueue: callerTaskQueue,
	}, "caller-workflow")
	s.NoError(err)
	s.T().Cleanup(func() {
		_ = s.SdkClient().TerminateWorkflow(ctx, callerRun.GetID(), callerRun.GetRunID(), "test cleanup")
	})

	startResp, err := s.FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
		Namespace:    s.Namespace().String(),
		WorkflowId:   targetWorkflowID,
		WorkflowType: &commonpb.WorkflowType{Name: "target-workflow"},
		TaskQueue:    &taskqueuepb.TaskQueue{Name: targetTaskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		RequestId:    uuid.NewString(),
	})
	s.NoError(err)
	targetRunID := startResp.RunId

	// Schedule GetWorkflowExecutionResult against the initial target run.
	pollResp, err := s.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{Name: callerTaskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:  "test",
	})
	s.NoError(err)
	_, err = s.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity:  "test",
		TaskToken: pollResp.TaskToken,
		Commands: []*commandpb.Command{
			{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_NEXUS_OPERATION,
				Attributes: &commandpb.Command_ScheduleNexusOperationCommandAttributes{
					ScheduleNexusOperationCommandAttributes: &commandpb.ScheduleNexusOperationCommandAttributes{
						Endpoint:  commonnexus.SystemEndpoint,
						Service:   workflowservicenexus.TemporalAPIApplicationserviceV1ApplicationService.ServiceName,
						Operation: workflowservicenexus.TemporalAPIApplicationserviceV1ApplicationService.GetWorkflowExecutionResult.Name(),
						Input: payloads.MustEncodeSingle(&applicationservice.GetWorkflowExecutionResultRequest{
							Namespace: s.Namespace().String(),
							Execution: &commonpb.WorkflowExecution{
								WorkflowId: targetWorkflowID,
								RunId:      targetRunID,
							},
						}),
					},
				},
			},
		},
	})
	s.NoError(err)

	// Wait for NexusOperationStarted — callback is now registered on the initial target run.
	pollResp, err = s.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{Name: callerTaskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:  "test",
	})
	s.NoError(err)
	s.ContainsHistoryEvents(`NexusOperationStarted`, pollResp.History.Events)
	_, err = s.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity:  "test",
		TaskToken: pollResp.TaskToken,
	})
	s.NoError(err)

	// Drive the target's first WFT with no commands so the initial run has a WftCompleted event
	// we can reset to.
	targetPollResp, err := s.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{Name: targetTaskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:  "test",
	})
	s.NoError(err)
	_, err = s.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity:  "test",
		TaskToken: targetPollResp.TaskToken,
	})
	s.NoError(err)

	// Find the WorkflowTaskCompleted event ID for the reset point.
	targetExecution := &commonpb.WorkflowExecution{WorkflowId: targetWorkflowID, RunId: targetRunID}
	var wftCompletedEventID int64
	for _, ev := range s.GetHistory(s.Namespace().String(), targetExecution) {
		if ev.EventType == enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED {
			wftCompletedEventID = ev.EventId
			break
		}
	}
	s.NotZero(wftCompletedEventID)

	// Reset the target. The callback registered on the initial run must be propagated to the new run.
	resetResp, err := s.FrontendClient().ResetWorkflowExecution(ctx, &workflowservice.ResetWorkflowExecutionRequest{
		Namespace:                 s.Namespace().String(),
		WorkflowExecution:         targetExecution,
		Reason:                    "test reset",
		WorkflowTaskFinishEventId: wftCompletedEventID,
		RequestId:                 uuid.NewString(),
	})
	s.NoError(err)

	// Complete the post-reset target run. This must fire the callback that was carried over.
	targetPollResp, err = s.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{Name: targetTaskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:  "test",
	})
	s.NoError(err)
	s.Equal(resetResp.RunId, targetPollResp.WorkflowExecution.RunId)
	_, err = s.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity:  "test",
		TaskToken: targetPollResp.TaskToken,
		Commands: []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
				CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{},
			},
		}},
	})
	s.NoError(err)

	pollResp, err = s.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{Name: callerTaskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:  "test",
	})
	s.NoError(err)
	s.ContainsHistoryEvents(`NexusOperationCompleted`, pollResp.History.Events)

	_, err = s.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity:  "test",
		TaskToken: pollResp.TaskToken,
		Commands: []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
				CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{},
			},
		}},
	})
	s.NoError(err)
	s.NoError(callerRun.Get(ctx, nil))
}

// terminalStateTestCase parameterizes TestGetWorkflowExecutionResult_TerminalStates: each case
// drives a target workflow into a distinct terminal state and asserts the synchronous
// getTerminalState path maps that state to the expected GetWorkflowExecutionResultResponse status.
type terminalStateTestCase struct {
	name string
	// workflowExecutionTimeout, when > 0, is set on the target's StartWorkflowExecution so the
	// target can time out without a worker (used for the TIMED_OUT case).
	workflowExecutionTimeout time.Duration
	// drive transitions the target run into its terminal state. It may be a no-op (e.g. TIMED_OUT,
	// where the workflow closes on its own).
	drive          func(ctx context.Context, targetTaskQueue, targetWorkflowID, targetRunID string)
	expectedStatus enumspb.WorkflowExecutionStatus
	// verify runs additional assertions on the decoded response (e.g. result/failure payloads).
	verify func(resp *applicationservice.GetWorkflowExecutionResultResponse)
}

// TestGetWorkflowExecutionResult_TerminalStates exercises the synchronous getTerminalState path for
// every terminal workflow state. For each state it drives a target workflow to that state, waits
// until it is fully closed, then schedules GetWorkflowExecutionResult so Start() short-circuits to
// getTerminalState. The nexus operation result is surfaced as the caller workflow's own result and
// decoded back into a GetWorkflowExecutionResultResponse to assert the reported status.
func (s *GetWorkflowExecutionResultTestSuite) TestGetWorkflowExecutionResult_TerminalStates() {
	completedResult := payloads.MustEncodeSingle("the-result")
	testFailure := &failurepb.Failure{Message: "boom"}

	testCases := []terminalStateTestCase{
		{
			name:           "Completed",
			expectedStatus: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
			drive: func(ctx context.Context, targetTaskQueue, _, _ string) {
				s.respondTargetWorkflowTask(ctx, targetTaskQueue, []*commandpb.Command{{
					CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
					Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
						CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
							Result: &commonpb.Payloads{Payloads: []*commonpb.Payload{completedResult}},
						},
					},
				}})
			},
			verify: func(resp *applicationservice.GetWorkflowExecutionResultResponse) {
				s.NotNil(resp.GetResult(), "completed result payload should be carried back to the caller")
			},
		},
		{
			name:           "Failed",
			expectedStatus: enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
			drive: func(ctx context.Context, targetTaskQueue, _, _ string) {
				s.respondTargetWorkflowTask(ctx, targetTaskQueue, []*commandpb.Command{{
					CommandType: enumspb.COMMAND_TYPE_FAIL_WORKFLOW_EXECUTION,
					Attributes: &commandpb.Command_FailWorkflowExecutionCommandAttributes{
						FailWorkflowExecutionCommandAttributes: &commandpb.FailWorkflowExecutionCommandAttributes{
							Failure: testFailure,
						},
					},
				}})
			},
			verify: func(resp *applicationservice.GetWorkflowExecutionResultResponse) {
				s.NotNil(resp.GetFailure(), "failure should be carried back to the caller")
			},
		},
		{
			name:                     "TimedOut",
			workflowExecutionTimeout: 1 * time.Second,
			expectedStatus:           enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT,
			// No drive: with no worker and a short execution timeout, the target times out on its own.
			drive: func(context.Context, string, string, string) {},
		},
		{
			name:           "Canceled",
			expectedStatus: enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED,
			drive: func(ctx context.Context, targetTaskQueue, targetWorkflowID, targetRunID string) {
				_, err := s.FrontendClient().RequestCancelWorkflowExecution(ctx, &workflowservice.RequestCancelWorkflowExecutionRequest{
					Namespace:         s.Namespace().String(),
					WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: targetWorkflowID, RunId: targetRunID},
				})
				s.NoError(err)
				s.respondTargetWorkflowTask(ctx, targetTaskQueue, []*commandpb.Command{{
					CommandType: enumspb.COMMAND_TYPE_CANCEL_WORKFLOW_EXECUTION,
					Attributes: &commandpb.Command_CancelWorkflowExecutionCommandAttributes{
						CancelWorkflowExecutionCommandAttributes: &commandpb.CancelWorkflowExecutionCommandAttributes{},
					},
				}})
			},
		},
		{
			name:           "Terminated",
			expectedStatus: enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
			drive: func(ctx context.Context, _, targetWorkflowID, targetRunID string) {
				_, err := s.FrontendClient().TerminateWorkflowExecution(ctx, &workflowservice.TerminateWorkflowExecutionRequest{
					Namespace:         s.Namespace().String(),
					WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: targetWorkflowID, RunId: targetRunID},
					Reason:            "test terminate",
				})
				s.NoError(err)
			},
		},
		{
			name:           "ContinuedAsNew",
			expectedStatus: enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW,
			drive: func(ctx context.Context, targetTaskQueue, _, _ string) {
				// The initial run closes with CONTINUED_AS_NEW; we query that closed run below. The
				// post-CAN run keeps running and is cleaned up by terminating the workflow ID.
				s.respondTargetWorkflowTask(ctx, targetTaskQueue, []*commandpb.Command{{
					CommandType: enumspb.COMMAND_TYPE_CONTINUE_AS_NEW_WORKFLOW_EXECUTION,
					Attributes: &commandpb.Command_ContinueAsNewWorkflowExecutionCommandAttributes{
						ContinueAsNewWorkflowExecutionCommandAttributes: &commandpb.ContinueAsNewWorkflowExecutionCommandAttributes{
							WorkflowType: &commonpb.WorkflowType{Name: "target-workflow"},
							TaskQueue:    &taskqueuepb.TaskQueue{Name: targetTaskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
						},
					},
				}})
			},
		},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			s.runTerminalStateCase(tc)
		})
	}
}

// respondTargetWorkflowTask polls a single workflow task from the target task queue and responds
// with the given commands.
func (s *GetWorkflowExecutionResultTestSuite) respondTargetWorkflowTask(ctx context.Context, taskQueue string, commands []*commandpb.Command) {
	s.T().Helper()
	pollResp, err := s.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:  "test",
	})
	s.NoError(err)
	_, err = s.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity:  "test",
		TaskToken: pollResp.TaskToken,
		Commands:  commands,
	})
	s.NoError(err)
}

func (s *GetWorkflowExecutionResultTestSuite) runTerminalStateCase(tc terminalStateTestCase) {
	ctx := testcore.NewContext()
	callerTaskQueue := testcore.RandomizeStr(s.T().Name())
	targetTaskQueue := testcore.RandomizeStr(s.T().Name() + "-target")
	targetWorkflowID := testcore.RandomizeStr(s.T().Name())

	// Start the target workflow.
	startReq := &workflowservice.StartWorkflowExecutionRequest{
		Namespace:    s.Namespace().String(),
		WorkflowId:   targetWorkflowID,
		WorkflowType: &commonpb.WorkflowType{Name: "target-workflow"},
		TaskQueue:    &taskqueuepb.TaskQueue{Name: targetTaskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		RequestId:    uuid.NewString(),
	}
	if tc.workflowExecutionTimeout > 0 {
		startReq.WorkflowExecutionTimeout = durationpb.New(tc.workflowExecutionTimeout)
	}
	startResp, err := s.FrontendClient().StartWorkflowExecution(ctx, startReq)
	s.NoError(err)
	targetRunID := startResp.RunId
	s.T().Cleanup(func() {
		_, _ = s.FrontendClient().TerminateWorkflowExecution(ctx, &workflowservice.TerminateWorkflowExecutionRequest{
			Namespace:         s.Namespace().String(),
			WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: targetWorkflowID},
			Reason:            "test cleanup",
		})
	})

	// Drive the target into the terminal state.
	tc.drive(ctx, targetTaskQueue, targetWorkflowID, targetRunID)

	// Wait until the target run is fully closed in the expected state. This guarantees the nexus
	// operation's Start() hits the synchronous getTerminalState path instead of registering a callback.
	s.EventuallyWithT(func(t *assert.CollectT) {
		descResp, descErr := s.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: s.Namespace().String(),
			Execution: &commonpb.WorkflowExecution{WorkflowId: targetWorkflowID, RunId: targetRunID},
		})
		assert.NoError(t, descErr)
		assert.Equal(t, tc.expectedStatus, descResp.GetWorkflowExecutionInfo().GetStatus())
	}, 20*time.Second, 200*time.Millisecond)

	// Start the caller workflow.
	callerRun, err := s.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		TaskQueue: callerTaskQueue,
	}, "caller-workflow")
	s.NoError(err)
	s.T().Cleanup(func() {
		_ = s.SdkClient().TerminateWorkflow(ctx, callerRun.GetID(), callerRun.GetRunID(), "test cleanup")
	})

	// Schedule GetWorkflowExecutionResult against the closed target run.
	pollResp, err := s.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{Name: callerTaskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:  "test",
	})
	s.NoError(err)
	_, err = s.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity:  "test",
		TaskToken: pollResp.TaskToken,
		Commands: []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_SCHEDULE_NEXUS_OPERATION,
			Attributes: &commandpb.Command_ScheduleNexusOperationCommandAttributes{
				ScheduleNexusOperationCommandAttributes: &commandpb.ScheduleNexusOperationCommandAttributes{
					Endpoint:  commonnexus.SystemEndpoint,
					Service:   workflowservicenexus.TemporalAPIApplicationserviceV1ApplicationService.ServiceName,
					Operation: workflowservicenexus.TemporalAPIApplicationserviceV1ApplicationService.GetWorkflowExecutionResult.Name(),
					Input: payloads.MustEncodeSingle(&applicationservice.GetWorkflowExecutionResultRequest{
						Namespace: s.Namespace().String(),
						Execution: &commonpb.WorkflowExecution{WorkflowId: targetWorkflowID, RunId: targetRunID},
					}),
				},
			},
		}},
	})
	s.NoError(err)

	// The operation completes synchronously via getTerminalState, so the next WFT carries
	// NexusOperationCompleted (and no NexusOperationStarted — the async callback path is skipped).
	pollResp, err = s.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{Name: callerTaskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:  "test",
	})
	s.NoError(err)
	s.ContainsHistoryEvents(`NexusOperationScheduled`, pollResp.History.Events)
	s.ContainsHistoryEvents(`NexusOperationCompleted`, pollResp.History.Events)

	completedIdx := slices.IndexFunc(pollResp.History.Events, func(e *historypb.HistoryEvent) bool {
		return e.GetNexusOperationCompletedEventAttributes() != nil
	})
	s.Positive(completedIdx, "caller history should contain a NexusOperationCompleted event")
	result := pollResp.History.Events[completedIdx].GetNexusOperationCompletedEventAttributes().GetResult()
	s.NotNil(result)

	// Complete the caller, surfacing the nexus result as its own result so we can decode it.
	_, err = s.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity:  "test",
		TaskToken: pollResp.TaskToken,
		Commands: []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
				CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
					Result: &commonpb.Payloads{Payloads: []*commonpb.Payload{result}},
				},
			},
		}},
	})
	s.NoError(err)

	// Decode the nexus result back into a GetWorkflowExecutionResultResponse and assert the status.
	var resultResp applicationservice.GetWorkflowExecutionResultResponse
	s.NoError(callerRun.Get(ctx, &resultResp))
	s.Equal(tc.expectedStatus, resultResp.GetStatus())
	if tc.verify != nil {
		tc.verify(&resultResp)
	}
}

// TestGetWorkflowExecutionResult_TargetContinuedAsNew verifies that when the target workflow
// continues-as-new after the callback has been registered, the caller's nexus operation does
// NOT complete on the CONTINUED_AS_NEW close of the initial run — the callback transfers to the
// new run and fires only when that run completes.
func (s *GetWorkflowExecutionResultTestSuite) TestGetWorkflowExecutionResult_TargetContinuedAsNew() {
	ctx := testcore.NewContext()
	callerTaskQueue := testcore.RandomizeStr(s.T().Name())
	targetTaskQueue := testcore.RandomizeStr(s.T().Name() + "-target")
	targetWorkflowID := testcore.RandomizeStr(s.T().Name())

	callerRun, err := s.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		TaskQueue: callerTaskQueue,
	}, "caller-workflow")
	s.NoError(err)
	s.T().Cleanup(func() {
		_ = s.SdkClient().TerminateWorkflow(ctx, callerRun.GetID(), callerRun.GetRunID(), "test cleanup")
	})

	startResp, err := s.FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
		Namespace:    s.Namespace().String(),
		WorkflowId:   targetWorkflowID,
		WorkflowType: &commonpb.WorkflowType{Name: "target-workflow"},
		TaskQueue:    &taskqueuepb.TaskQueue{Name: targetTaskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		RequestId:    uuid.NewString(),
	})
	s.NoError(err)
	targetRunID := startResp.RunId

	// Schedule GetWorkflowExecutionResult against the initial target run.
	pollResp, err := s.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{Name: callerTaskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:  "test",
	})
	s.NoError(err)
	_, err = s.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity:  "test",
		TaskToken: pollResp.TaskToken,
		Commands: []*commandpb.Command{
			{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_NEXUS_OPERATION,
				Attributes: &commandpb.Command_ScheduleNexusOperationCommandAttributes{
					ScheduleNexusOperationCommandAttributes: &commandpb.ScheduleNexusOperationCommandAttributes{
						Endpoint:  commonnexus.SystemEndpoint,
						Service:   workflowservicenexus.TemporalAPIApplicationserviceV1ApplicationService.ServiceName,
						Operation: workflowservicenexus.TemporalAPIApplicationserviceV1ApplicationService.GetWorkflowExecutionResult.Name(),
						Input: payloads.MustEncodeSingle(&applicationservice.GetWorkflowExecutionResultRequest{
							Namespace: s.Namespace().String(),
							Execution: &commonpb.WorkflowExecution{
								WorkflowId: targetWorkflowID,
								RunId:      targetRunID,
							},
						}),
					},
				},
			},
		},
	})
	s.NoError(err)

	// Wait for NexusOperationStarted — callback is registered on the initial target run.
	pollResp, err = s.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{Name: callerTaskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:  "test",
	})
	s.NoError(err)
	s.ContainsHistoryEvents(`NexusOperationStarted`, pollResp.History.Events)
	_, err = s.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity:  "test",
		TaskToken: pollResp.TaskToken,
	})
	s.NoError(err)

	// Drive the initial target run to continue-as-new.
	targetPollResp, err := s.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{Name: targetTaskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:  "test",
	})
	s.NoError(err)
	_, err = s.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity:  "test",
		TaskToken: targetPollResp.TaskToken,
		Commands: []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_CONTINUE_AS_NEW_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_ContinueAsNewWorkflowExecutionCommandAttributes{
				ContinueAsNewWorkflowExecutionCommandAttributes: &commandpb.ContinueAsNewWorkflowExecutionCommandAttributes{
					WorkflowType: &commonpb.WorkflowType{Name: "target-workflow"},
					TaskQueue:    &taskqueuepb.TaskQueue{Name: targetTaskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
				},
			},
		}},
	})
	s.NoError(err)

	// Poll the new run's first WFT (same task queue, new run ID) — this confirms CAN succeeded.
	targetPollResp, err = s.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{Name: targetTaskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:  "test",
	})
	s.NoError(err)
	s.NotEqual(targetRunID, targetPollResp.WorkflowExecution.RunId,
		"poll should pick up the post-CAN run, not the original")

	// Before completing the new run, verify the caller's persisted history does NOT yet contain
	// NexusOperationCompleted. The initial run closing with CONTINUED_AS_NEW must not fire the
	// callback — it must be carried over to the new run.
	callerHist := s.GetHistory(s.Namespace().String(), &commonpb.WorkflowExecution{
		WorkflowId: callerRun.GetID(),
		RunId:      callerRun.GetRunID(),
	})
	for _, ev := range callerHist {
		s.NotEqual(enumspb.EVENT_TYPE_NEXUS_OPERATION_COMPLETED, ev.EventType,
			"NexusOperationCompleted must not be present until the post-CAN run completes")
	}

	// Complete the new target run.
	_, err = s.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity:  "test",
		TaskToken: targetPollResp.TaskToken,
		Commands: []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
				CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{},
			},
		}},
	})
	s.NoError(err)

	// Now the caller's nexus operation should complete.
	pollResp, err = s.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{Name: callerTaskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:  "test",
	})
	s.NoError(err)
	s.ContainsHistoryEvents(`NexusOperationCompleted`, pollResp.History.Events)

	_, err = s.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity:  "test",
		TaskToken: pollResp.TaskToken,
		Commands: []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
				CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{},
			},
		}},
	})
	s.NoError(err)
	s.NoError(callerRun.Get(ctx, nil))
}

// TestGetWorkflowExecutionResult_AlreadyCompleted covers the synchronous path: the target workflow
// is already completed when GetWorkflowExecutionResult.Start() is called, so getTerminalState
// returns the result immediately and the nexus operation completes synchronously (no
// NexusOperationStarted event — only NexusOperationCompleted).
func (s *GetWorkflowExecutionResultTestSuite) TestGetWorkflowExecutionResult_AlreadyCompleted() {
	ctx := testcore.NewContext()
	callerTaskQueue := testcore.RandomizeStr(s.T().Name())
	targetTaskQueue := testcore.RandomizeStr(s.T().Name() + "-target")
	targetWorkflowID := testcore.RandomizeStr(s.T().Name())

	// Start and immediately complete the target workflow before the caller schedules the nexus op.
	startResp, err := s.FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
		Namespace:    s.Namespace().String(),
		WorkflowId:   targetWorkflowID,
		WorkflowType: &commonpb.WorkflowType{Name: "target-workflow"},
		TaskQueue:    &taskqueuepb.TaskQueue{Name: targetTaskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		RequestId:    uuid.NewString(),
	})
	s.NoError(err)
	targetRunID := startResp.RunId

	targetPollResp, err := s.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{Name: targetTaskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:  "test",
	})
	s.NoError(err)
	_, err = s.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity:  "test",
		TaskToken: targetPollResp.TaskToken,
		Commands: []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
				CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{},
			},
		}},
	})
	s.NoError(err)

	// Wait for the target to be fully closed before starting the caller. This ensures the nexus
	// operation always hits the synchronous getTerminalState path rather than the async callback path.
	targetHandle := s.SdkClient().GetWorkflow(ctx, targetWorkflowID, targetRunID)
	s.NoError(targetHandle.Get(ctx, nil))

	// Now start the caller workflow.
	callerRun, err := s.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		TaskQueue: callerTaskQueue,
	}, "caller-workflow")
	s.NoError(err)
	s.T().Cleanup(func() {
		_ = s.SdkClient().TerminateWorkflow(ctx, callerRun.GetID(), callerRun.GetRunID(), "test cleanup")
	})

	// Poll caller's first WFT and schedule GetWorkflowExecutionResult — the target is already done.
	pollResp, err := s.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{Name: callerTaskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:  "test",
	})
	s.NoError(err)
	_, err = s.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity:  "test",
		TaskToken: pollResp.TaskToken,
		Commands: []*commandpb.Command{
			{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_NEXUS_OPERATION,
				Attributes: &commandpb.Command_ScheduleNexusOperationCommandAttributes{
					ScheduleNexusOperationCommandAttributes: &commandpb.ScheduleNexusOperationCommandAttributes{
						Endpoint:  commonnexus.SystemEndpoint,
						Service:   workflowservicenexus.TemporalAPIApplicationserviceV1ApplicationService.ServiceName,
						Operation: workflowservicenexus.TemporalAPIApplicationserviceV1ApplicationService.GetWorkflowExecutionResult.Name(),
						Input: payloads.MustEncodeSingle(&applicationservice.GetWorkflowExecutionResultRequest{
							Namespace: s.Namespace().String(),
							Execution: &commonpb.WorkflowExecution{
								WorkflowId: targetWorkflowID,
								RunId:      targetRunID,
							},
						}),
					},
				},
			},
		},
	})
	s.NoError(err)

	// The nexus operation completes synchronously (getTerminalState), so the next WFT will have
	// NexusOperationCompleted but no NexusOperationStarted.
	pollResp, err = s.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{Name: callerTaskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:  "test",
	})
	s.NoError(err)
	s.ContainsHistoryEvents(`NexusOperationScheduled`, pollResp.History.Events)
	s.ContainsHistoryEvents(`NexusOperationCompleted`, pollResp.History.Events)

	// Complete the caller workflow.
	_, err = s.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity:  "test",
		TaskToken: pollResp.TaskToken,
		Commands: []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
				CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{},
			},
		}},
	})
	s.NoError(err)
	s.NoError(callerRun.Get(ctx, nil))
}
