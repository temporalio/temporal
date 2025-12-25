package tests

import (
	"maps"
	"slices"
	"testing"

	"github.com/stretchr/testify/suite"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/tests/testcore"
)

type SignalWithStartFromWorkflowTestSuite struct {
	testcore.FunctionalTestBase
}

func TestSignalWithStartFromWorkflowTestSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(SignalWithStartFromWorkflowTestSuite))
}

// NOTE: This test cannot use the SDK workflow package because there is a restriction that prevents setting the
// __temporal_system endpoint.
func (s *SignalWithStartFromWorkflowTestSuite) TestHappyPath() {
	ctx := testcore.NewContext()
	taskQueue := testcore.RandomizeStr(s.T().Name())

	run, err := s.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		TaskQueue: taskQueue,
	}, "workflow")
	s.NoError(err)

	workflowID := testcore.RandomizeStr(s.T().Name())

	pollResp, err := s.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: taskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		Identity: "test",
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
						Service:   "temporal.api.workflowservice.v1.WorkflowService",
						Operation: "SignalWithStartWorkflowExecution",
						Input: payloads.MustEncodeSingle(&workflowservice.SignalWithStartWorkflowExecutionRequest{
							WorkflowId: workflowID,
							SignalName: "test-signal",
							WorkflowType: &commonpb.WorkflowType{
								Name: "workflow",
							},
							TaskQueue: &taskqueuepb.TaskQueue{
								Name: s.T().Name(),
							},
						}),
					},
				},
			},
		},
	})
	s.NoError(err)

	// Poll for the completion
	pollResp, err = s.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: taskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		Identity: "test",
	})
	s.NoError(err)

	// Find the NexusOperationCompleted event
	completedEventIdx := slices.IndexFunc(pollResp.History.Events, func(e *historypb.HistoryEvent) bool {
		return e.GetNexusOperationCompletedEventAttributes() != nil
	})
	s.Positive(completedEventIdx, "Should have a NexusOperationCompleted event")

	// Verify the result contains the echoed request ID
	completedEvent := pollResp.History.Events[completedEventIdx]
	result := completedEvent.GetNexusOperationCompletedEventAttributes().Result
	s.NotNil(result)

	// Complete the workflow
	_, err = s.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity:  "test",
		TaskToken: pollResp.TaskToken,
		Commands: []*commandpb.Command{
			{
				CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
					CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
						Result: &commonpb.Payloads{
							Payloads: []*commonpb.Payload{result},
						},
					},
				},
			},
		},
	})
	s.NoError(err)
	var response *workflowservice.SignalWithStartWorkflowExecutionResponse
	s.NoError(run.Get(ctx, &response))
	s.True(response.Started)

	err = s.SdkClient().TerminateWorkflow(ctx, workflowID, response.GetRunId(), "test cleanup")
	s.NoError(err)

	// Verify the linkage from the handler workflow in the caller's history.
	it := s.SdkClient().GetWorkflowHistory(ctx, run.GetID(), run.GetRunID(), false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	var opScheduledEvent *historypb.HistoryEvent
	var opCompletedEvent *historypb.HistoryEvent
	for it.HasNext() {
		ev, err := it.Next()
		s.NoError(err)
		if ev.GetNexusOperationScheduledEventAttributes() != nil {
			opScheduledEvent = ev
		}
		if ev.GetNexusOperationCompletedEventAttributes() != nil {
			opCompletedEvent = ev
			break
		}
	}
	s.NotNil(opScheduledEvent, "Should have found NexusOperationScheduled event in history")
	s.NotNil(opCompletedEvent, "Should have found NexusOperationCompleted event in history")
	s.Len(opCompletedEvent.Links, 1)
	link := opCompletedEvent.Links[0]
	s.Equal(workflowID, link.GetWorkflowEvent().GetWorkflowId())
	s.Equal(response.GetRunId(), link.GetWorkflowEvent().GetRunId())
	s.Equal(opScheduledEvent.GetNexusOperationScheduledEventAttributes().GetRequestId(), link.GetWorkflowEvent().GetRequestIdRef().GetRequestId())

	// Verify the linkage from the caller workflow in the handler's history.
	it = s.SdkClient().GetWorkflowHistory(ctx, workflowID, response.GetRunId(), false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	var wfStartedEvent *historypb.HistoryEvent
	for it.HasNext() {
		ev, err := it.Next()
		s.NoError(err)
		if ev.GetWorkflowExecutionStartedEventAttributes() != nil {
			wfStartedEvent = ev
			break
		}
	}
	s.NotNil(wfStartedEvent, "Should have found WorkflowExecutionStarted event in history")
	s.Len(wfStartedEvent.Links, 1)
	link = wfStartedEvent.Links[0]
	s.Equal(run.GetID(), link.GetWorkflowEvent().GetWorkflowId())
	s.Equal(run.GetRunID(), link.GetWorkflowEvent().GetRunId())
	s.Equal(opScheduledEvent.GetEventId(), link.GetWorkflowEvent().GetEventRef().EventId)

	// Verify the request ID info is recorded correctly in the handler workflow's description.
	desc, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowID, response.GetRunId())
	s.NoError(err)
	requestIDInfos := desc.GetWorkflowExtendedInfo().GetRequestIdInfos()
	requestID := slices.Collect(maps.Keys(requestIDInfos))[0]
	s.Equal(opScheduledEvent.GetNexusOperationScheduledEventAttributes().GetRequestId(), requestID)
}
