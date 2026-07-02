package tests

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
		require.NoError(t, descErr)
		require.NotEmpty(t, descResp.GetCallbacks(), "target should have a completion callback registered")
		callerLink = nil
		for _, cbInfo := range descResp.GetCallbacks() {
			for _, link := range cbInfo.GetCallback().GetLinks() {
				if we := link.GetWorkflowEvent(); we != nil {
					callerLink = we
				}
			}
		}
		require.NotNil(t, callerLink, "target's callback must carry a WorkflowEvent link identifying the caller")
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
// The Nexus handler's Start() attaches the completion callback by emitting a
// WorkflowExecutionOptionsUpdated event (via wf.AttachCompletionCallbacks), which records it in the
// target's history. Reset therefore recovers it via event reapply, the same way it does for
// StartWorkflow- and update-registered callbacks.
func (s *GetWorkflowExecutionResultTestSuite) TestGetWorkflowExecutionResult_TargetResetAfterCallbackAdded() {
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

	// The callback must be delivered exactly once — carrying it over to the reset run must not
	// duplicate the StartWorkflow/runtime registration.
	completedCount := 0
	for _, ev := range pollResp.History.Events {
		if ev.GetNexusOperationCompletedEventAttributes() != nil {
			completedCount++
		}
	}
	s.Equal(1, completedCount, "callback should be delivered exactly once after reset")

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

// TestGetWorkflowExecutionResult_TargetResetAfterCallbackFired verifies the callback's behavior when
// the target is reset AFTER the GetWorkflowExecutionResult callback has already FIRED (delivered the
// nexus result). Because the callback is recorded in history via a WorkflowExecutionOptionsUpdated
// event, reset reapplies it onto the new run as a fresh STANDBY callback (the same as
// StartWorkflow- and update-registered callbacks), so the reset run DOES carry the callback and it
// re-fires when the reset run re-completes. This must NOT result in a second delivery to the caller:
// the caller's nexus operation already completed, so the re-fired delivery is dropped and the caller
// sees exactly one NexusOperationCompleted.
//
// This is the counterpart to TestGetWorkflowExecutionResult_TargetResetAfterCallbackAdded (the
// not-yet-fired case). Together they pin the contract: the callback is durable across reset via
// history reapply, and re-firing after reset never double-notifies the caller.
func (s *GetWorkflowExecutionResultTestSuite) TestGetWorkflowExecutionResult_TargetResetAfterCallbackFired() {
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

	// Wait for NexusOperationStarted — callback is now registered (STANDBY) on the initial target run.
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

	// Complete the initial target run. This fires the callback, which delivers the result back to the
	// caller and leaves the callback in a non-STANDBY (succeeded) state.
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

	// The caller's nexus operation completes from the fired callback. Drain that WFT and complete the
	// caller so the callback is confirmed delivered exactly once before the reset.
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

	// Find the first WorkflowTaskCompleted event of the (now completed) target run to use as the reset
	// point.
	targetExecution := &commonpb.WorkflowExecution{WorkflowId: targetWorkflowID, RunId: targetRunID}
	var wftCompletedEventID int64
	for _, ev := range s.GetHistory(s.Namespace().String(), targetExecution) {
		if ev.EventType == enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED {
			wftCompletedEventID = ev.EventId
			break
		}
	}
	s.NotZero(wftCompletedEventID)

	// Reset the completed target. The callback is in history (WorkflowExecutionOptionsUpdated), so it
	// is reapplied onto the new run as a fresh STANDBY callback.
	resetResp, err := s.FrontendClient().ResetWorkflowExecution(ctx, &workflowservice.ResetWorkflowExecutionRequest{
		Namespace:                 s.Namespace().String(),
		WorkflowExecution:         targetExecution,
		Reason:                    "test reset",
		WorkflowTaskFinishEventId: wftCompletedEventID,
		RequestId:                 uuid.NewString(),
	})
	s.NoError(err)

	// The post-reset run carries the completion callback again (STANDBY), reapplied from the
	// WorkflowExecutionOptionsUpdated event, just like StartWorkflow- and update-registered callbacks.
	resetExecution := &commonpb.WorkflowExecution{WorkflowId: targetWorkflowID, RunId: resetResp.RunId}
	s.EventuallyWithT(func(t *assert.CollectT) {
		descResp, descErr := s.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: s.Namespace().String(),
			Execution: resetExecution,
		})
		require.NoError(t, descErr)
		assert.NotEmpty(t, descResp.GetCallbacks(), "reset run must reapply the callback from history")
	}, 10*time.Second, 200*time.Millisecond)

	// Complete the post-reset run. The callback re-fires, but the caller's nexus operation already
	// completed, so the re-fired delivery is dropped and the caller is not notified a second time.
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

	// The caller's history must still contain exactly one NexusOperationCompleted: even though the
	// callback re-fires on the reset run, the caller's nexus operation is already terminal, so the
	// re-fired delivery cannot add a second completion.
	completedCount := 0
	for _, ev := range s.GetHistory(s.Namespace().String(), &commonpb.WorkflowExecution{WorkflowId: callerRun.GetID(), RunId: callerRun.GetRunID()}) {
		if ev.GetNexusOperationCompletedEventAttributes() != nil {
			completedCount++
		}
	}
	s.Equal(1, completedCount, "callback must not be re-delivered to a caller whose nexus operation already completed")
}

// TestGetWorkflowExecutionResult_IgnoresRunIDUsesLatestRun verifies that the RunID on the request is
// ignored: the operation always resolves against the most recent run for the workflow ID.
//
// It starts two runs of the same workflow ID: the first completes with a distinctive result, the
// second (started with ALLOW_DUPLICATE) stays running. The caller schedules GetWorkflowExecutionResult
// targeting the EARLIER, already-completed run's RunID. Because RunID is ignored, the operation must
// attach asynchronously to the still-running latest run (not synchronously return the earlier run's
// completion). Failing the latest run then delivers ITS failure to the caller, proving the earlier
// run's completion result was not used.
func (s *GetWorkflowExecutionResultTestSuite) TestGetWorkflowExecutionResult_IgnoresRunIDUsesLatestRun() {
	ctx := testcore.NewContext()
	callerTaskQueue := testcore.RandomizeStr(s.T().Name())
	targetTaskQueue := testcore.RandomizeStr(s.T().Name() + "-target")
	targetWorkflowID := testcore.RandomizeStr(s.T().Name())

	firstResult := payloads.MustEncodeSingle("first-run-result")

	// First run: start and complete it with a distinctive result.
	startResp1, err := s.FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
		Namespace:    s.Namespace().String(),
		WorkflowId:   targetWorkflowID,
		WorkflowType: &commonpb.WorkflowType{Name: "target-workflow"},
		TaskQueue:    &taskqueuepb.TaskQueue{Name: targetTaskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		RequestId:    uuid.NewString(),
	})
	s.NoError(err)
	firstRunID := startResp1.RunId

	s.respondTargetWorkflowTask(ctx, targetTaskQueue, []*commandpb.Command{{
		CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
		Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
			CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
				Result: &commonpb.Payloads{Payloads: []*commonpb.Payload{firstResult}},
			},
		},
	}})

	// Wait until the first run is fully closed as COMPLETED.
	s.EventuallyWithT(func(t *assert.CollectT) {
		descResp, descErr := s.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: s.Namespace().String(),
			Execution: &commonpb.WorkflowExecution{WorkflowId: targetWorkflowID, RunId: firstRunID},
		})
		require.NoError(t, descErr)
		require.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, descResp.GetWorkflowExecutionInfo().GetStatus())
	}, 20*time.Second, 200*time.Millisecond)

	// Second run: same workflow ID, left running.
	startResp2, err := s.FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
		Namespace:             s.Namespace().String(),
		WorkflowId:            targetWorkflowID,
		WorkflowType:          &commonpb.WorkflowType{Name: "target-workflow"},
		TaskQueue:             &taskqueuepb.TaskQueue{Name: targetTaskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		RequestId:             uuid.NewString(),
		WorkflowIdReusePolicy: enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
	})
	s.NoError(err)
	secondRunID := startResp2.RunId
	s.NotEqual(firstRunID, secondRunID, "second run must be a distinct, newer run")
	s.T().Cleanup(func() {
		_, _ = s.FrontendClient().TerminateWorkflowExecution(ctx, &workflowservice.TerminateWorkflowExecutionRequest{
			Namespace:         s.Namespace().String(),
			WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: targetWorkflowID},
			Reason:            "test cleanup",
		})
	})

	// Caller schedules GetWorkflowExecutionResult targeting the EARLIER (completed) run's RunID.
	callerRun, err := s.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		TaskQueue: callerTaskQueue,
	}, "caller-workflow")
	s.NoError(err)
	s.T().Cleanup(func() {
		_ = s.SdkClient().TerminateWorkflow(ctx, callerRun.GetID(), callerRun.GetRunID(), "test cleanup")
	})

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
						Execution: &commonpb.WorkflowExecution{
							WorkflowId: targetWorkflowID,
							RunId:      firstRunID, // earlier, completed run — must be ignored
						},
					}),
				},
			},
		}},
	})
	s.NoError(err)

	// The operation must attach ASYNCHRONOUSLY to the still-running latest run, not return the earlier
	// run's completion synchronously: NexusOperationStarted confirms the async attach.
	pollResp, err = s.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{Name: callerTaskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:  "test",
	})
	s.NoError(err)
	s.ContainsHistoryEvents(`NexusOperationStarted`, pollResp.History.Events)
	for _, ev := range pollResp.History.Events {
		s.Nil(ev.GetNexusOperationCompletedEventAttributes(),
			"operation must not complete synchronously from the earlier completed run")
	}
	_, err = s.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity:  "test",
		TaskToken: pollResp.TaskToken,
	})
	s.NoError(err)

	// The completion callback must be registered on the LATEST run, not the earlier one.
	s.EventuallyWithT(func(t *assert.CollectT) {
		descResp, descErr := s.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: s.Namespace().String(),
			Execution: &commonpb.WorkflowExecution{WorkflowId: targetWorkflowID, RunId: secondRunID},
		})
		require.NoError(t, descErr)
		require.NotEmpty(t, descResp.GetCallbacks(), "latest run must carry the completion callback")
	}, 10*time.Second, 200*time.Millisecond)

	// Fail the latest run.
	s.respondTargetWorkflowTask(ctx, targetTaskQueue, []*commandpb.Command{{
		CommandType: enumspb.COMMAND_TYPE_FAIL_WORKFLOW_EXECUTION,
		Attributes: &commandpb.Command_FailWorkflowExecutionCommandAttributes{
			FailWorkflowExecutionCommandAttributes: &commandpb.FailWorkflowExecutionCommandAttributes{
				Failure: &failurepb.Failure{Message: "second-run-failure"},
			},
		},
	}})

	// The caller's nexus operation must reflect the LATEST run's outcome: because that run FAILED, the
	// operation FAILS (NexusOperationFailed). Had it (incorrectly) used the earlier run's RunID, the
	// operation would have COMPLETED with that run's successful result instead.
	pollResp, err = s.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{Name: callerTaskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:  "test",
	})
	s.NoError(err)
	s.ContainsHistoryEvents(`NexusOperationFailed`, pollResp.History.Events)

	// Assert over the full caller history: the operation failed with the latest run's failure and
	// never completed with the earlier run's result.
	callerHist := s.GetHistory(s.Namespace().String(), &commonpb.WorkflowExecution{
		WorkflowId: callerRun.GetID(),
		RunId:      callerRun.GetRunID(),
	})
	var failedAttrs *historypb.NexusOperationFailedEventAttributes
	for _, ev := range callerHist {
		s.Nil(ev.GetNexusOperationCompletedEventAttributes(),
			"operation must not complete with the earlier run's result")
		if fa := ev.GetNexusOperationFailedEventAttributes(); fa != nil {
			failedAttrs = fa
		}
	}
	s.NotNil(failedAttrs, "operation must fail with the latest run's failure")
	s.Contains(failedAttrs.GetFailure().String(), "second-run-failure",
		"failure must originate from the latest run, not the earlier completed run")

	// Drain the caller's final WFT so it finishes cleanly.
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
// drives a target workflow into a distinct terminal state and asserts the synchronous close-event
// path (workflowResultFromCloseEvent) maps that state to the expected GetWorkflowExecutionResultResponse
// (a Result payload for success, or a Failure payload for the non-success terminal states).
type terminalStateTestCase struct {
	name string
	// workflowExecutionTimeout, when > 0, is set on the target's StartWorkflowExecution so the
	// target can time out without a worker (used for the TIMED_OUT case).
	workflowExecutionTimeout time.Duration
	// drive transitions the target run into its terminal state. It may be a no-op (e.g. TIMED_OUT,
	// where the workflow closes on its own).
	drive func(ctx context.Context, targetTaskQueue, targetWorkflowID, targetRunID string)
	// expectedStatus is the workflow execution status the target must reach (used to wait until the
	// target is fully closed before scheduling the operation). It is not part of the response.
	expectedStatus enumspb.WorkflowExecutionStatus
	// verify asserts the decoded response carries the expected Result or Failure payload.
	verify func(resp *applicationservice.GetWorkflowExecutionResultResponse)
}

// TestGetWorkflowExecutionResult_TerminalStates exercises the synchronous close-event path for
// every terminal workflow state. For each state it drives a target workflow to that state, waits
// until it is fully closed, then schedules GetWorkflowExecutionResult so Start() returns the result
// synchronously from the close event (workflowResultFromCloseEvent). The nexus operation result is
// surfaced as the caller workflow's own result and decoded back into a
// GetWorkflowExecutionResultResponse to assert the Result/Failure payload.
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
			verify: func(resp *applicationservice.GetWorkflowExecutionResultResponse) {
				s.NotNil(resp.GetFailure(), "timed-out workflow should surface a failure")
				s.NotNil(resp.GetFailure().GetTimeoutFailureInfo(), "failure should carry timeout failure info")
			},
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
			verify: func(resp *applicationservice.GetWorkflowExecutionResultResponse) {
				s.NotNil(resp.GetFailure(), "canceled workflow should surface a failure")
				s.NotNil(resp.GetFailure().GetCanceledFailureInfo(), "failure should carry canceled failure info")
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
			verify: func(resp *applicationservice.GetWorkflowExecutionResultResponse) {
				s.NotNil(resp.GetFailure(), "terminated workflow should surface a failure")
				s.NotNil(resp.GetFailure().GetTerminatedFailureInfo(), "failure should carry terminated failure info")
			},
		},
		// CONTINUED_AS_NEW is intentionally omitted: the operation resolves against the latest run, so
		// a run that continued-as-new is not a terminal result and no longer maps to a response status
		// (workflowResultFromCloseEvent returns an internal error for it, mirroring GetNexusCompletion).
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
	// operation's Start() hits the synchronous close-event path instead of registering a callback.
	s.EventuallyWithT(func(t *assert.CollectT) {
		descResp, descErr := s.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: s.Namespace().String(),
			Execution: &commonpb.WorkflowExecution{WorkflowId: targetWorkflowID, RunId: targetRunID},
		})
		require.NoError(t, descErr)
		require.Equal(t, tc.expectedStatus, descResp.GetWorkflowExecutionInfo().GetStatus())
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
						Execution: &commonpb.WorkflowExecution{WorkflowId: targetWorkflowID, RunId: targetRunID},
					}),
				},
			},
		}},
	})
	s.NoError(err)

	// The operation completes synchronously from the close event, so the next WFT carries
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

	// Decode the nexus result back into a GetWorkflowExecutionResultResponse and assert the payload.
	var resultResp applicationservice.GetWorkflowExecutionResultResponse
	s.NoError(callerRun.Get(ctx, &resultResp))
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
// is already completed when GetWorkflowExecutionResult.Start() is called, so workflowResultFromCloseEvent
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
	// operation always hits the synchronous close-event path rather than the async callback path.
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

	// The nexus operation completes synchronously from the close event, so the next WFT will have
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
