package tests

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflownexusservice/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/api/workflowservice/v1/workflowservicenexus"
	"go.temporal.io/sdk/client"
	"go.temporal.io/server/common/dynamicconfig"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/tests/testcore"
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
			dynamicconfig.EnableChasm.Key():                       true,
			dynamicconfig.EnableCHASMCallbacks.Key():              true,
			dynamicconfig.EnableGetWorkflowExecutionResult.Key():  true,
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
						Service:   workflowservicenexus.WorkflowService.ServiceName,
						Operation: workflowservicenexus.WorkflowNexusService.GetWorkflowExecutionResult.Name(),
						Input: payloads.MustEncodeSingle(&workflownexusservice.GetWorkflowExecutionResultRequest{
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
						Service:   workflowservicenexus.WorkflowService.ServiceName,
						Operation: workflowservicenexus.WorkflowNexusService.GetWorkflowExecutionResult.Name(),
						Input: payloads.MustEncodeSingle(&workflownexusservice.GetWorkflowExecutionResultRequest{
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
						Service:   workflowservicenexus.WorkflowService.ServiceName,
						Operation: workflowservicenexus.WorkflowNexusService.GetWorkflowExecutionResult.Name(),
						Input: payloads.MustEncodeSingle(&workflownexusservice.GetWorkflowExecutionResultRequest{
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
						Service:   workflowservicenexus.WorkflowService.ServiceName,
						Operation: workflowservicenexus.WorkflowNexusService.GetWorkflowExecutionResult.Name(),
						Input: payloads.MustEncodeSingle(&workflownexusservice.GetWorkflowExecutionResultRequest{
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
