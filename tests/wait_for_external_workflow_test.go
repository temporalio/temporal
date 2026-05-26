package tests

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/api/workflowservice/v1/workflowservicenexus"
	"go.temporal.io/sdk/client"
	"go.temporal.io/server/chasm/lib/nexusoperation"
	"go.temporal.io/server/common/dynamicconfig"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/tests/testcore"
)

type WaitForExternalWorkflowTestSuite struct {
	testcore.FunctionalTestBase
}

func TestWaitForExternalWorkflowTestSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(WaitForExternalWorkflowTestSuite))
}

func (s *WaitForExternalWorkflowTestSuite) SetupSuite() {
	s.SetupSuiteWithCluster(
		testcore.WithDynamicConfigOverrides(map[dynamicconfig.Key]any{
			dynamicconfig.EnableChasm.Key():          true,
			dynamicconfig.EnableCHASMCallbacks.Key(): true,
			nexusoperation.EnableChasmNexus.Key():    true,
		}),
	)
}

func (s *WaitForExternalWorkflowTestSuite) TestWaitForExternalWorkflow() {
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

	// Poll caller's first WFT and schedule WaitForExternalWorkflow as a nexus operation
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
						Operation: workflowservicenexus.WorkflowService.WaitForExternalWorkflow.Name(),
						Input: payloads.MustEncodeSingle(&workflowservice.WaitForExternalWorkflowRequest{
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

	// Complete the target workflow. The callback registered by WaitForExternalWorkflow.Start
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

// TestWaitForExternalWorkflow_AlreadyCompleted covers the synchronous path: the target workflow
// is already completed when WaitForExternalWorkflow.Start() is called, so getTerminalState
// returns the result immediately and the nexus operation completes synchronously (no
// NexusOperationStarted event — only NexusOperationCompleted).
func (s *WaitForExternalWorkflowTestSuite) TestWaitForExternalWorkflow_AlreadyCompleted() {
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

	// Poll caller's first WFT and schedule WaitForExternalWorkflow — the target is already done.
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
						Operation: workflowservicenexus.WorkflowService.WaitForExternalWorkflow.Name(),
						Input: payloads.MustEncodeSingle(&workflowservice.WaitForExternalWorkflowRequest{
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
