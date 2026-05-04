package tests

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/tests/testcore"
)

type LinksSuite struct {
	parallelsuite.Suite[*LinksSuite]
}

func TestLinksTestSuite(t *testing.T) {
	parallelsuite.Run(t, &LinksSuite{})
}

var links = []*commonpb.Link{
	{
		Variant: &commonpb.Link_WorkflowEvent_{
			WorkflowEvent: &commonpb.Link_WorkflowEvent{
				Namespace:  "dont-care",
				WorkflowId: "whatever",
				RunId:      uuid.NewString(),
			},
		},
	},
}

func enableSignalBacklinkOpts() []testcore.TestOption {
	return []testcore.TestOption{
		testcore.WithDynamicConfig(dynamicconfig.EnableChasm, true),
		testcore.WithDynamicConfig(dynamicconfig.EnableCHASMSignalBacklinks, true),
	}
}

func (s *LinksSuite) TestTerminateWorkflow_LinksAttachedToEvent() {
	env := testcore.NewEnv(s.T())
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	run, err := env.SdkClient().ExecuteWorkflow(
		ctx,
		client.StartWorkflowOptions{
			TaskQueue: "dont-care",
		},
		"test-workflow-type",
	)
	s.NoError(err)

	_, err = env.FrontendClient().TerminateWorkflowExecution(ctx, &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: run.GetID(),
		},
		Reason: "test",
		Links:  links,
	})
	s.NoError(err)

	// TODO(bergundy): Use SdkClient if and when it exposes links on TerminateWorkflow.
	history := env.SdkClient().GetWorkflowHistory(ctx, run.GetID(), "", false, enumspb.HISTORY_EVENT_FILTER_TYPE_CLOSE_EVENT)
	event, err := history.Next()
	s.NoError(err)
	protorequire.ProtoSliceEqual(s.T(), links, event.Links)
}

func (s *LinksSuite) TestRequestCancelWorkflow_LinksAttachedToEvent() {
	env := testcore.NewEnv(s.T())
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	run, err := env.SdkClient().ExecuteWorkflow(
		ctx,
		client.StartWorkflowOptions{
			TaskQueue: "dont-care",
		},
		"test-workflow-type",
	)
	s.NoError(err)

	_, err = env.FrontendClient().RequestCancelWorkflowExecution(ctx, &workflowservice.RequestCancelWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: run.GetID(),
		},
		Reason: "test",
		Links:  links,
	})
	s.NoError(err)

	// TODO(bergundy): Use SdkClient if and when it exposes links on CancelWorkflow.
	history := env.SdkClient().GetWorkflowHistory(ctx, run.GetID(), "", false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	foundEvent := false
	for history.HasNext() {
		event, err := history.Next()
		s.NoError(err)
		if event.EventType != enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED {
			continue
		}
		foundEvent = true
		protorequire.ProtoSliceEqual(s.T(), links, event.Links)
	}
	s.True(foundEvent)
}

func (s *LinksSuite) TestSignalWorkflowExecution_LinksAttachedToEvent() {
	env := testcore.NewEnv(s.T(), enableSignalBacklinkOpts()...)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	run, err := env.SdkClient().ExecuteWorkflow(
		ctx,
		client.StartWorkflowOptions{
			TaskQueue: "dont-care",
		},
		"test-workflow-type",
	)
	s.NoError(err)

	req := &workflowservice.SignalWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: run.GetID(),
		},
		SignalName: "dont-care",
		Identity:   "test",
		RequestId:  uuid.NewString(),
		Links:      links,
	}
	expectedLink := &commonpb.Link{
		Variant: &commonpb.Link_WorkflowEvent_{
			WorkflowEvent: &commonpb.Link_WorkflowEvent{
				Namespace:  env.Namespace().String(),
				WorkflowId: run.GetID(),
				RunId:      run.GetRunID(),
				Reference: &commonpb.Link_WorkflowEvent_RequestIdRef{
					RequestIdRef: &commonpb.Link_WorkflowEvent_RequestIdReference{
						RequestId: req.RequestId,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
					},
				},
			},
		},
	}

	// TODO(bergundy): Use SdkClient if and when it exposes links on SignalWorkflow.
	resp, err := env.FrontendClient().SignalWorkflowExecution(ctx, req)
	s.NoError(err)
	protorequire.ProtoEqual(s.T(), expectedLink, resp.GetLink())

	// Second call with same RequestId hits the dedup path but must still return the same link.
	resp, err = env.FrontendClient().SignalWorkflowExecution(ctx, req)
	s.NoError(err)
	protorequire.ProtoEqual(s.T(), expectedLink, resp.GetLink())

	history := env.SdkClient().GetWorkflowHistory(ctx, run.GetID(), "", false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	foundEvent := false
	foundDuplicatedEvent := false
	var signaledEventID int64
	for history.HasNext() {
		event, err := history.Next()
		s.NoError(err)
		if event.EventType != enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED {
			continue
		}
		if foundEvent {
			foundDuplicatedEvent = true
		} else {
			signaledEventID = event.GetEventId()
		}
		foundEvent = true
		protorequire.ProtoSliceEqual(s.T(), links, event.Links)
	}
	s.True(foundEvent)
	s.False(foundDuplicatedEvent, "second signal with same RequestId should be deduped and not produce a second event")

	// Verify the requestID is tracked and resolves to the correct event ID.
	descResp, err := env.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: run.GetID(),
		},
	})
	s.NoError(err)
	requestIDInfos := descResp.GetWorkflowExtendedInfo().GetRequestIdInfos()
	s.Contains(requestIDInfos, req.RequestId)
	info := requestIDInfos[req.RequestId]
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED, info.GetEventType())
	s.Equal(signaledEventID, info.GetEventId(), "requestID map entry must point to the SIGNALED event in history")
}

// TestSignalWorkflowExecution_BacklinkSurvivesReset verifies that after a workflow is reset,
// the new run's CHASM IncomingSignals map is rebuilt from history so that DescribeWorkflow
// continues to return a valid requestID → event-ID backlink for signals that occurred before
// the reset point.
//
// This exercises the rebuild/replay path through ApplyWorkflowExecutionSignaled, which uses
// the event's real event ID (not common.BufferedEventID) when writing to the CHASM tree.
func (s *LinksSuite) TestSignalWorkflowExecution_BacklinkSurvivesReset() {
	env := testcore.NewEnv(s.T(), enableSignalBacklinkOpts()...)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	taskQueue := testcore.RandomizeStr(s.T().Name())
	workflowID := testcore.RandomizeStr(s.T().Name())

	// Start the workflow.
	run, err := env.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: taskQueue,
	}, "dont-care")
	s.NoError(err)
	runID := run.GetRunID()

	signalRequestID := uuid.NewString()

	// Signal the workflow. The signal will be included in the first WFT batch, so it will
	// appear in history before the WFT completion event.
	_, err = env.FrontendClient().SignalWorkflowExecution(ctx, &workflowservice.SignalWorkflowExecutionRequest{
		Namespace:         env.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: runID},
		SignalName:        "dont-care",
		Identity:          "test",
		RequestId:         signalRequestID,
		Links:             links,
	})
	s.NoError(err)

	// Poll and complete the WFT so the signal is flushed to history with a real event ID.
	s.Eventually(func() bool {
		pollResp, pollErr := env.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			Identity:  "test",
		})
		if pollErr != nil || pollResp.GetTaskToken() == nil {
			return false
		}
		_, completeErr := env.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
			Namespace: env.Namespace().String(),
			Identity:  "test",
			TaskToken: pollResp.TaskToken,
		})
		return completeErr == nil
	}, 20*time.Second, 200*time.Millisecond)

	// Find the WFT completed event ID in the original run's history.
	var wftCompletedEventID int64
	history := env.SdkClient().GetWorkflowHistory(ctx, workflowID, runID, false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	for history.HasNext() {
		event, histErr := history.Next()
		s.NoError(histErr)
		if event.EventType == enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED {
			wftCompletedEventID = event.EventId
			break
		}
	}
	s.Positive(wftCompletedEventID, "WFT completed event not found in history")

	// Reset the workflow to the first WFT completion. The signal event is before this point,
	// so it will be included in the new run's replayed history.
	resetResp, err := env.FrontendClient().ResetWorkflowExecution(ctx, &workflowservice.ResetWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
		Reason:                    "testing-backlink-survival",
		RequestId:                 uuid.NewString(),
		WorkflowTaskFinishEventId: wftCompletedEventID,
	})
	s.NoError(err)
	newRunID := resetResp.RunId
	s.NotEmpty(newRunID)

	// Wait for DescribeWorkflow on the new run to return the signal backlink.
	// During reset, ApplyWorkflowExecutionSignaled rebuilds the CHASM IncomingSignals map
	// from history, so the backlink should be present once the new run is created.
	s.Eventually(func() bool {
		descResp, descErr := env.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: env.Namespace().String(),
			Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: newRunID},
		})
		if descErr != nil {
			return false
		}
		_, ok := descResp.GetWorkflowExtendedInfo().GetRequestIdInfos()[signalRequestID]
		return ok
	}, 20*time.Second, 200*time.Millisecond)

	// Verify the backlink on the new run points to a real (non-buffered) SIGNALED event.
	descResp, err := env.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: newRunID},
	})
	s.NoError(err)
	requestIDInfos := descResp.GetWorkflowExtendedInfo().GetRequestIdInfos()
	s.Contains(requestIDInfos, signalRequestID)
	info := requestIDInfos[signalRequestID]
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED, info.GetEventType())
	s.Positive(info.GetEventId(), "backlink event ID must be a real, non-buffered event ID in the new run's history")
	s.False(info.GetBuffered())
}

func (s *LinksSuite) TestSignalWithStartWorkflowExecution_LinksAttachedToRelevantEvents() {
	env := testcore.NewEnv(s.T(), enableSignalBacklinkOpts()...)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	workflowID := testcore.RandomizeStr(s.T().Name())

	request := &workflowservice.SignalWithStartWorkflowExecutionRequest{
		Namespace:  env.Namespace().String(),
		WorkflowId: workflowID,
		WorkflowType: &commonpb.WorkflowType{
			Name: "dont-care",
		},
		SignalName: "dont-care",
		Identity:   "test",
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: "dont-care",
		},
		RequestId: uuid.NewString(),
		Links:     links,
	}

	// TODO(bergundy): Use SdkClient if and when it exposes links on SignalWithStartWorkflow.
	resp, err := env.FrontendClient().SignalWithStartWorkflowExecution(ctx, request)
	s.NoError(err)
	firstRunID := resp.GetRunId()
	protorequire.ProtoEqual(
		s.T(),
		&commonpb.Link{
			Variant: &commonpb.Link_WorkflowEvent_{
				WorkflowEvent: &commonpb.Link_WorkflowEvent{
					Namespace:  env.Namespace().String(),
					WorkflowId: workflowID,
					RunId:      firstRunID,
					Reference: &commonpb.Link_WorkflowEvent_RequestIdRef{
						RequestIdRef: &commonpb.Link_WorkflowEvent_RequestIdReference{
							RequestId: request.RequestId,
							EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
						},
					},
				},
			},
		},
		resp.GetSignalLink(),
	)

	firstRequestID := request.RequestId

	// Send a second request and verify that the new signal has links attached to it too.
	request.RequestId = uuid.NewString()
	resp, err = env.FrontendClient().SignalWithStartWorkflowExecution(ctx, request)
	s.NoError(err)
	// Expect backlinks with the same RunID as before since the workflow execution didn't change,
	// but the signal requestID should differ since this is a different request.
	protorequire.ProtoEqual(
		s.T(),
		&commonpb.Link{
			Variant: &commonpb.Link_WorkflowEvent_{
				WorkflowEvent: &commonpb.Link_WorkflowEvent{
					Namespace:  env.Namespace().String(),
					WorkflowId: workflowID,
					RunId:      resp.GetRunId(),
					Reference: &commonpb.Link_WorkflowEvent_RequestIdRef{
						RequestIdRef: &commonpb.Link_WorkflowEvent_RequestIdReference{
							RequestId: request.RequestId, // This requestID should differ from the first backlink.
							EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
						},
					},
				},
			},
		},
		resp.GetSignalLink(),
	)

	history := env.SdkClient().GetWorkflowHistory(ctx, workflowID, "", false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	foundStartEvent := false
	foundFirstSignal := false
	foundSecondSignal := false
	var firstSignalEventID, secondSignalEventID int64
	for history.HasNext() {
		event, err := history.Next()
		s.NoError(err)
		if event.EventType == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED {
			if foundFirstSignal {
				foundSecondSignal = true
				secondSignalEventID = event.GetEventId()
			} else {
				foundFirstSignal = true
				firstSignalEventID = event.GetEventId()
			}
			protorequire.ProtoSliceEqual(s.T(), links, event.Links)
		}
		if event.EventType == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED {
			foundStartEvent = true
			protorequire.ProtoSliceEqual(s.T(), links, event.Links)
		}
	}
	s.True(foundStartEvent)
	s.True(foundFirstSignal)
	s.True(foundSecondSignal)

	// Verify both requestIDs are tracked and resolve to the correct signal event IDs.
	descResp, err := env.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
		},
	})
	s.NoError(err)
	requestIDInfos := descResp.GetWorkflowExtendedInfo().GetRequestIdInfos()

	s.Contains(requestIDInfos, firstRequestID)
	firstInfo := requestIDInfos[firstRequestID]
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED, firstInfo.GetEventType())
	s.Equal(firstSignalEventID, firstInfo.GetEventId(), "first requestID map entry must point to the first SIGNALED event in history")

	s.Contains(requestIDInfos, request.RequestId)
	secondInfo := requestIDInfos[request.RequestId]
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED, secondInfo.GetEventType())
	s.Equal(secondSignalEventID, secondInfo.GetEventId(), "second requestID map entry must point to the second SIGNALED event in history")
}
