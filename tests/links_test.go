package tests

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/server/common/dynamicconfig"
	commonnexus "go.temporal.io/server/common/nexus"
	sdkconverter "go.temporal.io/server/common/sdk"
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
		testcore.WithDynamicConfig(dynamicconfig.EnableSignalWithStartFromWorkflow, true),
	}
}

// getWorkflowRunRequestInfo calls DescribeWorkflowExecution and returns the ExtendedInfo's RequestIDInfo
// corresponding to the supplied request ID. Fails the current test if there are any errors or if there is
// no RequestIDInfo corresponding to the supplied RequestID found.
func (s *LinksSuite) getWorkflowRunRequestInfo(
	ctx context.Context, env *testcore.TestEnv,
	workflowEx *commonpb.WorkflowExecution, wantRequestID string) *workflowpb.RequestIdInfo {
	s.T().Helper()

	descResp, err := env.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		Execution: workflowEx,
	})
	s.NoError(err, "error describing workflow")

	requestIDInfos := descResp.GetWorkflowExtendedInfo().GetRequestIdInfos()
	s.Contains(requestIDInfos, wantRequestID, "No request with ID %s found in workflow execution requestIdInfos", wantRequestID)

	return requestIDInfos[wantRequestID]
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

	signalTest := newSignalWorkflowTest(env, s)

	// Start the workflow.
	startResult := signalTest.startTargetWorkflow(ctx)
	targetWorkflowID, targetWorkflowRunID := startResult.WorkflowID, startResult.WorkflowRunID

	// Signal the workflow (for the first time).
	signalWorkflowRequestID := uuid.NewString()
	signalResp := signalTest.signalWorkflow(ctx, targetWorkflowID, signalWorkflowRequestID)
	gotLink := signalResp.GetLink()

	wantLink := &commonpb.Link{
		Variant: &commonpb.Link_WorkflowEvent_{
			WorkflowEvent: &commonpb.Link_WorkflowEvent{
				Namespace:  env.Namespace().String(),
				WorkflowId: targetWorkflowID,
				RunId:      targetWorkflowRunID,
				Reference: &commonpb.Link_WorkflowEvent_RequestIdRef{
					RequestIdRef: &commonpb.Link_WorkflowEvent_RequestIdReference{
						RequestId: signalWorkflowRequestID,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
					},
				},
			},
		},
	}
	protorequire.ProtoEqual(s.T(), wantLink, gotLink)

	// Second call with same RequestId hits the dedup path but must still return the same link.
	signalResp2 := signalTest.signalWorkflow(ctx, targetWorkflowID, signalWorkflowRequestID)
	protorequire.ProtoEqual(s.T(), wantLink, signalResp2.GetLink())

	// Confirm no duplicate events in the Workflow's history.
	history := env.SdkClient().GetWorkflowHistory(ctx, targetWorkflowID, "", false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
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
	workflowEx := &commonpb.WorkflowExecution{
		WorkflowId: targetWorkflowID,
	}
	gotRequestInfo := s.getWorkflowRunRequestInfo(ctx, env, workflowEx, signalWorkflowRequestID)
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED, gotRequestInfo.GetEventType())
	s.Equal(signaledEventID, gotRequestInfo.GetEventId(), "requestID map entry must point to the SIGNALED event in history")
}

func (s *LinksSuite) TestSignalWithStartWorkflowExecution_LinksAttachedToEvent() {
	// Body of the test. We run it twice, where the workflow targeted by SignalWithStart
	// is or is-not running.
	testImpl := func(ls *LinksSuite, signalExistingWorkflow bool) {
		env := testcore.NewEnv(ls.T(), enableSignalBacklinkOpts()...)
		ctx := ls.Context()

		signalTest := newSignalWorkflowTest(env, ls)

		// Potentially start the workflow.
		targetWorkflowID := uuid.NewString()
		if signalExistingWorkflow {
			signalTest.startTargetWorkflowWithWorkflowID(ctx, targetWorkflowID)
		}

		// Send a signal to the new or existing workflow, get its RunID.
		signalResp := signalTest.signalWithStartWorkflow(ctx, targetWorkflowID)
		if signalExistingWorkflow {
			ls.False(signalResp.Started)
		} else {
			ls.True(signalResp.Started)
		}

		targetWorkflowRunID := signalResp.GetRunId()
		gotLink := signalResp.GetSignalLink()
		ls.NotNil(gotLink, "no SignalLink in response")

		// We don't know the RequestID for the StartWithSignal call until after it is made.
		signalWorkflowRequestID := gotLink.GetWorkflowEvent().GetRequestIdRef().GetRequestId()
		ls.NotEmpty(signalWorkflowRequestID, "didn't get RequestID from SignalWithStart response")

		wantLink := &commonpb.Link{
			Variant: &commonpb.Link_WorkflowEvent_{
				WorkflowEvent: &commonpb.Link_WorkflowEvent{
					Namespace:  env.Namespace().String(),
					WorkflowId: targetWorkflowID,
					RunId:      targetWorkflowRunID,
					Reference: &commonpb.Link_WorkflowEvent_RequestIdRef{
						RequestIdRef: &commonpb.Link_WorkflowEvent_RequestIdReference{
							RequestId: signalWorkflowRequestID,
							EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
						},
					},
				},
			},
		}
		protorequire.ProtoEqual(ls.T(), wantLink, gotLink)

		// NOTE: Unlike the SignalWorkflow- version of this test, we do not verify
		// any dedupe paths, because calling SignalWithStart twice via the Nexus
		// endpoint will result in sending two signals to the workflow with no way
		// to make the request idempotent. (Which is expected and by-design.)

		// Verify the requestID is tracked and resolves to the correct event ID.
		workflowEx := &commonpb.WorkflowExecution{
			WorkflowId: targetWorkflowID,
		}
		gotRequestInfo := ls.getWorkflowRunRequestInfo(ctx, env, workflowEx, signalWorkflowRequestID)
		ls.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED, gotRequestInfo.GetEventType())
		ls.Positive(gotRequestInfo.GetEventId())
	}

	s.Run("SignalExistingWorkflow", func(ls *LinksSuite) {
		testImpl(ls, true)
	})
	s.Run("SignalStartsNewWorkflow", func(ls *LinksSuite) {
		testImpl(ls, false)
	})
}

// TestSignalWorkflowExecution_BacklinkSurvivesReset verifies that after a workflow is reset,
// the new run's CHASM IncomingSignals map is rebuilt from history so that DescribeWorkflow
// continues to return a valid requestID -> event-ID backlink for signals that occurred before
// the reset point.
//
// This exercises the rebuild/replay path through ApplyWorkflowExecutionSignaled, which uses
// the event's real event ID (not common.BufferedEventID) when writing to the CHASM tree.
func (s *LinksSuite) TestSignalWorkflowExecution_BacklinkSurvivesReset() {
	env := testcore.NewEnv(s.T(), enableSignalBacklinkOpts()...)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	signalTest := newSignalWorkflowTest(env, s)

	// Start the workflow.
	startResult := signalTest.startTargetWorkflow(ctx)
	targetWorkflowID, targetWorkflowRunID := startResult.WorkflowID, startResult.WorkflowRunID

	// Signal the workflow. The signal will be included in the first WFT batch, so it will
	// appear in history before the WFT completion event.
	signalRequestID := uuid.NewString()
	signalTest.signalWorkflow(ctx, targetWorkflowID, signalRequestID)

	// Poll and complete the WFT so the signal is flushed to history with a real event ID.
	pollResp, pollErr := env.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: env.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{Name: signalTest.taskQueueName, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:  "test",
	})
	s.NoError(pollErr)
	s.NotNil(pollResp.GetTaskToken())
	_, completeErr := env.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Namespace: env.Namespace().String(),
		Identity:  "test",
		TaskToken: pollResp.TaskToken,
	})
	s.NoError(completeErr)

	// Find the WFT completed event ID in the original run's history.
	var wftCompletedEventID int64
	history := env.SdkClient().GetWorkflowHistory(ctx, targetWorkflowID, targetWorkflowRunID, false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
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
			WorkflowId: targetWorkflowID,
			RunId:      targetWorkflowRunID,
		},
		Reason:                    "testing-backlink-survival",
		RequestId:                 uuid.NewString(),
		WorkflowTaskFinishEventId: wftCompletedEventID,
	})
	s.NoError(err)
	newRunID := resetResp.RunId
	s.NotEmpty(newRunID)

	// During reset, ApplyWorkflowExecutionSignaled rebuilds the CHASM IncomingSignals map
	// from history, so the backlink should be present once the new run is created.
	descResp, descErr := env.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: targetWorkflowID, RunId: newRunID},
	})
	s.NoError(descErr)
	_, signalExists := descResp.GetWorkflowExtendedInfo().GetRequestIdInfos()[signalRequestID]
	s.True(signalExists)

	// Verify the backlink on the new run points to a real (non-buffered) SIGNALED event.
	workflowEx := &commonpb.WorkflowExecution{
		WorkflowId: targetWorkflowID,
		RunId:      newRunID,
	}
	gotRequestIDInfo := s.getWorkflowRunRequestInfo(ctx, env, workflowEx, signalRequestID)
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED, gotRequestIDInfo.GetEventType())
	s.Positive(gotRequestIDInfo.GetEventId(), "backlink event ID must be a real, non-buffered event ID in the new run's history")
	s.False(gotRequestIDInfo.GetBuffered())
}

func (s *LinksSuite) TestSignalWithStartWorkflowExecution_BacklinkSurvivesReset() {
	// Body of the test. We run it twice, where the workflow targeted by SignalWithStart
	// is or is-not running.
	testImpl := func(ls *LinksSuite, signalExistingWorkflow bool) {
		env := testcore.NewEnv(ls.T(), enableSignalBacklinkOpts()...)
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()

		signalTest := newSignalWorkflowTest(env, ls)

		// Start the workflow depending on the test scenario.
		targetWorkflowID := uuid.NewString()
		var targetWorkflowRunID string

		if signalExistingWorkflow {
			startResp := signalTest.startTargetWorkflowWithWorkflowID(ctx, targetWorkflowID)
			gotWfID, gotRunID := startResp.WorkflowID, startResp.WorkflowRunID
			ls.Equal(targetWorkflowID, gotWfID)
			targetWorkflowRunID = gotRunID
		}

		// Signal the workflow. The signal will be included in the first WFT batch, so it will
		// appear in history before the WFT completion event.
		signalWithStartResp := signalTest.signalWithStartWorkflow(ctx, targetWorkflowID)
		if signalExistingWorkflow {
			ls.False(signalWithStartResp.Started)
		} else {
			ls.True(signalWithStartResp.Started)
		}

		// We don't know the RequestID for the StartWithSignal call until after it is made.
		ls.NotNil(signalWithStartResp.GetSignalLink())
		signalWorkflowRequestID := signalWithStartResp.GetSignalLink().GetWorkflowEvent().GetRequestIdRef().GetRequestId()
		ls.NotEmpty(signalWorkflowRequestID, "didn't get RequestID from SignalWithStart response")

		if signalExistingWorkflow {
			ls.False(signalWithStartResp.Started)
			ls.Equal(targetWorkflowRunID, signalWithStartResp.RunId)
		} else {
			ls.True(signalWithStartResp.Started)
			targetWorkflowRunID = signalWithStartResp.GetRunId()
		}

		// Poll and complete the WFT so the signal is flushed to history with a real event ID.
		pollResp, pollErr := env.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: signalTest.taskQueueName, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			Identity:  "test",
		})
		ls.NoError(pollErr)
		ls.NotNil(pollResp.GetTaskToken())
		_, completeErr := env.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
			Namespace: env.Namespace().String(),
			Identity:  "test",
			TaskToken: pollResp.TaskToken,
		})
		ls.NoError(completeErr)

		// Find the WFT completed event ID in the original run's history.
		var wftCompletedEventID int64
		history := env.SdkClient().GetWorkflowHistory(ctx, targetWorkflowID, targetWorkflowRunID, false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
		for history.HasNext() {
			event, histErr := history.Next()
			ls.NoError(histErr)
			if event.EventType == enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED {
				wftCompletedEventID = event.EventId
				break
			}
		}
		ls.Positive(wftCompletedEventID, "WFT completed event not found in history")

		// Reset the workflow to the first WFT completion. The signal event is before this point,
		// so it will be included in the new run's replayed history.
		resetResp, err := env.FrontendClient().ResetWorkflowExecution(ctx, &workflowservice.ResetWorkflowExecutionRequest{
			Namespace: env.Namespace().String(),
			WorkflowExecution: &commonpb.WorkflowExecution{
				WorkflowId: targetWorkflowID,
				RunId:      targetWorkflowRunID,
			},
			Reason:                    "testing-backlink-survival",
			RequestId:                 uuid.NewString(),
			WorkflowTaskFinishEventId: wftCompletedEventID,
		})
		ls.NoError(err)
		newRunID := resetResp.RunId
		ls.NotEmpty(newRunID)

		// Confirm the original signal is in the original workflow run's RequestID map.
		originalWorkflowEx := &commonpb.WorkflowExecution{
			WorkflowId: targetWorkflowID,
			RunId:      targetWorkflowRunID,
		}
		origSignalInfo := ls.getWorkflowRunRequestInfo(ctx, env, originalWorkflowEx, signalWorkflowRequestID)
		ls.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED, origSignalInfo.GetEventType())
		ls.False(origSignalInfo.GetBuffered())

		// During reset, ApplyWorkflowExecutionSignaled rebuilds the CHASM IncomingSignals map
		// from history, so the backlink should be present once the new run is created.
		// Verify the backlink on the new run points to a real (non-buffered) SIGNALED event.
		resetWorkflowEx := &commonpb.WorkflowExecution{
			WorkflowId: targetWorkflowID,
			RunId:      newRunID,
		}
		resetSignalInfo := ls.getWorkflowRunRequestInfo(ctx, env, resetWorkflowEx, signalWorkflowRequestID)
		ls.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED, resetSignalInfo.GetEventType())
		ls.Positive(resetSignalInfo.GetEventId(), "backlink event ID must be a real, non-buffered event ID in the new run's history")
		ls.False(resetSignalInfo.GetBuffered())
	}

	s.Run("SignalExistingWorkflow", func(ls *LinksSuite) {
		testImpl(ls, true)
	})
	s.Run("SignalStartsNewWorkflow", func(ls *LinksSuite) {
		testImpl(ls, false)
	})
}

// TestSignalWorkflowExecution_BufferedDuringWorkflowTask verifies that when a signal arrives
// while a workflow task is being processed, DescribeWorkflow reports the backlink as buffered.
// Once the workflow task completes and the signal is flushed to history, the backlink must
// reflect a real (non-buffered) event ID.
func (s *LinksSuite) TestSignalWorkflowExecution_BufferedDuringWorkflowTask() {
	env := testcore.NewEnv(s.T(), enableSignalBacklinkOpts()...)
	ctx := s.Context()

	signalTest := newSignalWorkflowTest(env, s)

	// Start the workflow.
	startResult := signalTest.startTargetWorkflow(ctx)
	targetWorkflowID, targetWorkflowRunID := startResult.WorkflowID, startResult.WorkflowRunID

	// Poll to move the WFT into "started" state to have the server wait for us to complete it.
	// This will force the signal to stay in the buffer until the task is finished.
	pollResp, err := env.FrontendClient().PollWorkflowTaskQueue(env.Context(), &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: env.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{Name: signalTest.taskQueueName, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:  "test",
	})
	s.NoError(err)
	s.NotNil(pollResp.GetTaskToken())

	// This signal will be buffered since there is a WFT in-flight.
	signalRequestID := uuid.NewString()
	signalTest.signalWorkflow(ctx, targetWorkflowID, signalRequestID)

	// WFT is still running: backlink must be present and marked buffered.
	workflowEx := &commonpb.WorkflowExecution{
		WorkflowId: targetWorkflowID,
		RunId:      targetWorkflowRunID,
	}
	gotRequestInfo := s.getWorkflowRunRequestInfo(ctx, env, workflowEx, signalRequestID)
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED, gotRequestInfo.GetEventType())
	s.True(gotRequestInfo.GetBuffered(), "backlink must be buffered while WFT is in progress")

	// Complete the WFT, which flushes the signal to DB with a concrete EventID.
	_, err = env.FrontendClient().RespondWorkflowTaskCompleted(env.Context(), &workflowservice.RespondWorkflowTaskCompletedRequest{
		Namespace: env.Namespace().String(),
		Identity:  "test",
		TaskToken: pollResp.TaskToken,
	})
	s.NoError(err)

	// After WFT completion the backlink must resolve to a real, non-buffered event.
	gotRequestInfo2 := s.getWorkflowRunRequestInfo(ctx, env, workflowEx, signalRequestID)
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED, gotRequestInfo2.GetEventType())
	s.False(gotRequestInfo2.GetBuffered(), "backlink must not be buffered after WFT completion")
	s.Positive(gotRequestInfo2.GetEventId(), "backlink must reference a real event ID after WFT completion")
}

func (s *LinksSuite) TestSignalWithStartWorkflowExecution_BufferedDuringWorkflowTask() {
	// Body of the test. We run it twice, where the workflow targeted by SignalWithStart
	// is or is-not running.
	testImpl := func(ls *LinksSuite, signalExistingWorkflow bool) {
		env := testcore.NewEnv(ls.T(), enableSignalBacklinkOpts()...)
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()

		signalTest := newSignalWorkflowTest(env, ls)

		// Potentially start the workflow.
		targetWorkflowID := uuid.NewString()
		if signalExistingWorkflow {
			signalTest.startTargetWorkflowWithWorkflowID(ctx, targetWorkflowID)
		}

		// Poll to move the WFT into "started" state to have the server wait for us to complete it.
		// This will force the signal to stay in the buffer until the task is finished.
		//
		// We skip this step if there is no existing workflow to target with SignalWithStart, because
		// that would have the Poll call hang until the deadline is hit. (Because there is no workflow
		// with tasks to be executed.)
		var pollTaskToken []byte
		if signalExistingWorkflow {
			pollResp, err := env.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
				Namespace: env.Namespace().String(),
				TaskQueue: &taskqueuepb.TaskQueue{Name: signalTest.taskQueueName, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
				Identity:  "test",
			})
			ls.NoError(err)
			ls.NotNil(pollResp.GetTaskToken())

			pollTaskToken = pollResp.GetTaskToken()
		}

		// Call SignalWithStart. This will result in the event getting buffered (if the workflow
		// is already running), or simply starting as new workflow execution.
		signalWithStartResp := signalTest.signalWithStartWorkflow(ctx, targetWorkflowID)
		if signalExistingWorkflow {
			ls.False(signalWithStartResp.Started)
		} else {
			ls.True(signalWithStartResp.Started)
		}

		ls.NotNil(signalWithStartResp.GetSignalLink())
		targetWorkflowRunID := signalWithStartResp.GetRunId()

		signalWorkflowRequestID := signalWithStartResp.GetSignalLink().GetWorkflowEvent().GetRequestIdRef().GetRequestId()
		ls.NotEmpty(signalWorkflowRequestID, "didn't get RequestID from SignalWithStart response")

		// Get the RequestIDInfos for the running workflow.
		workflowEx := &commonpb.WorkflowExecution{
			WorkflowId: targetWorkflowID,
			RunId:      targetWorkflowRunID,
		}
		gotRequestInfo := ls.getWorkflowRunRequestInfo(ctx, env, workflowEx, signalWorkflowRequestID)
		ls.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED, gotRequestInfo.GetEventType())

		if signalExistingWorkflow {
			// If the signal was sent to an existing workflow, we expect the new event to be buffered.
			ls.True(gotRequestInfo.GetBuffered(), "backlink must be buffered while WFT is in progress")

			// Complete the WFT, which flushes the signal to DB with a concrete EventID.
			_, err := env.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
				Namespace: env.Namespace().String(),
				Identity:  "test",
				TaskToken: pollTaskToken,
			})
			ls.NoError(err)

			// After WFT completion the backlink must resolve to a real, non-buffered event.
			gotRequestInfo2 := ls.getWorkflowRunRequestInfo(ctx, env, workflowEx, signalWorkflowRequestID)
			ls.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED, gotRequestInfo2.GetEventType())
			ls.False(gotRequestInfo2.GetBuffered(), "backlink must not be buffered after WFT completion")
			ls.Positive(gotRequestInfo2.GetEventId(), "backlink must reference a real event ID after WFT completion")
		} else {
			// If the call to SignalWithStart triggered spinning up a new workflow execution, then no buffering is necessary.
			// We don't need to complete the WTF, because there isn't a WFT that we were polling on.
			ls.False(gotRequestInfo.GetBuffered(), "did not expect event to be buffered")
			ls.Positive(gotRequestInfo.GetEventId())
		}
	}

	s.Run("SignalExistingWorkflow", func(ls *LinksSuite) {
		testImpl(ls, true)
	})
	s.Run("SignalStartsNewWorkflow", func(ls *LinksSuite) {
		testImpl(ls, false)
	})
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

// signalWorkflowTest provides common operations for starting and signaling workflows.
type signalWorkflowTest struct {
	taskQueueName string
	workflowName  string

	s   *LinksSuite
	env *testcore.TestEnv
}

func newSignalWorkflowTest(env *testcore.TestEnv, s *LinksSuite) *signalWorkflowTest {
	return &signalWorkflowTest{
		taskQueueName: "test-task-queue",
		workflowName:  "test-workflow",
		s:             s,
		env:           env,
	}
}

type startTargetWorkflowOutput struct {
	WorkflowID    string
	WorkflowRunID string
}

// startTargetWorkflow starts a generic workflow.
func (swt *signalWorkflowTest) startTargetWorkflow(ctx context.Context) startTargetWorkflowOutput {
	swt.s.T().Helper()
	// By not supplying a WorkflowID, it will default to UUID.
	return swt.startTargetWorkflowWithWorkflowID(ctx, "")
}

// startTargetWorkflowWithWorkflowID starts a workflow using the supplied Workflow ID.
func (swt *signalWorkflowTest) startTargetWorkflowWithWorkflowID(ctx context.Context, workflowID string) startTargetWorkflowOutput {
	swt.s.T().Helper()
	run, err := swt.env.SdkClient().ExecuteWorkflow(
		ctx,
		client.StartWorkflowOptions{
			ID:        workflowID,
			TaskQueue: swt.taskQueueName,
		},
		"test-workflow-type",
	)
	swt.s.NoError(err)

	return startTargetWorkflowOutput{
		WorkflowID:    run.GetID(),
		WorkflowRunID: run.GetRunID(),
	}
}

func (swt *signalWorkflowTest) signalWorkflow(ctx context.Context, targetWorkflowID, requestID string) *workflowservice.SignalWorkflowExecutionResponse {
	swt.s.T().Helper()
	req := &workflowservice.SignalWorkflowExecutionRequest{
		Namespace: swt.env.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: targetWorkflowID,
			// Target the latest execution of the workflow.
			RunId: "",
		},
		SignalName: "dont care",
		Identity:   "test",
		RequestId:  requestID,
		Links:      links,
	}
	// TODO(bergundy): Use SdkClient if and when it exposes links on SignalWorkflow.
	resp, err := swt.env.FrontendClient().SignalWorkflowExecution(ctx, req)
	swt.s.NoError(err)

	return resp
}

// signalWithStartWorkflow invokes the SignalWithStart handler using the System Nexus Endpoint,
// and NOT the typical frontend API directly. This is a newer codepath that wraps the History
// service's API within the Nexus machinery.
//
// IMPORTANT: The RequestID CANNOT be supplied when using the Nexus variant of the SignalWithStart
// call, because the RequestID will be set when when doing the execution.
func (swt *signalWorkflowTest) signalWithStartWorkflow(ctx context.Context, targetWorkflowID string) *workflowservice.SignalWithStartWorkflowExecutionResponse {
	swt.s.T().Helper()
	startWithSignalRequest := &workflowservice.SignalWithStartWorkflowExecutionRequest{
		Namespace:    swt.env.Namespace().String(),
		WorkflowId:   targetWorkflowID,
		WorkflowType: &commonpb.WorkflowType{Name: swt.workflowName},
		TaskQueue:    &taskqueuepb.TaskQueue{Name: swt.taskQueueName, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		SignalName:   "dont care",
		Identity:     "test",

		// QUIRK: When using the system Nexus endpoint, we cannot suppply new links.
		Links: nil,
		// QUIRK: Must be left empty when making the call via Nexus.
		RequestId: "",
	}

	// HERE BE DRAGONS
	//
	// To call the SignalWithStartWorkflowExecution found within the system Nexus endpoint, we start
	// and wait on a trivial workflow which will actually make the call.
	//
	// ... HOWEVER, the go.temporal.io/sdk@v1.41.1 (and earlier) panics in workflow.NewNexusClient when
	//  the endpoint name starts with the reserved "__temporal_" prefix. (Which is the case when trying
	// to target the system Nexus endpoint.)
	//
	// So instead, we work around this by following after signal_with_start_from_workflow_test.go's
	// TestBothWorkflowsVisibleAfterSWSFromWorkflowProtoBinary, which drives the workflow task manually
	// sending the command.COMMAND_TYPE_SCHEDULE_NEXUS_OPERATION.
	//
	// We can remove this when the SDK allows creating a Nexus client which can target the system handler.
	const nexusCallerTaskQueue = "totally-different-task-queue"

	// Start a caller workflow to obtain an initial workflow task.
	callerRun, err := swt.env.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		TaskQueue: nexusCallerTaskQueue,
	}, "caller-workflow")
	swt.s.NoError(err)
	defer func() {
		_ = swt.env.SdkClient().TerminateWorkflow(ctx, callerRun.GetID(), callerRun.GetRunID(), "test cleanup")
	}()

	// Encode the SWS request as binary/protobuf. PreferProtoDataConverter places
	// ProtoPayloadConverter first, so proto messages are marshalled to binary/protobuf
	// rather than the JSON proto encoding that the SDK uses by default.
	pls, err := sdkconverter.PreferProtoDataConverter.ToPayloads(startWithSignalRequest)
	swt.s.NoError(err)
	swt.s.Len(pls.Payloads, 1)

	protoBinaryPayload := pls.Payloads[0]
	swt.s.Equal("binary/protobuf", string(protoBinaryPayload.Metadata["encoding"]))

	// First poll: respond with a ScheduleNexusOperation command carrying the proto binary input.
	pollResp, err := swt.env.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: swt.env.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{Name: nexusCallerTaskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
	})
	swt.s.NoError(err)

	_, err = swt.env.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		TaskToken: pollResp.TaskToken,
		Commands: []*commandpb.Command{
			{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_NEXUS_OPERATION,
				Attributes: &commandpb.Command_ScheduleNexusOperationCommandAttributes{
					ScheduleNexusOperationCommandAttributes: &commandpb.ScheduleNexusOperationCommandAttributes{
						Endpoint:  commonnexus.SystemEndpoint,
						Service:   "WorkflowService",
						Operation: "SignalWithStartWorkflowExecution",
						Input:     protoBinaryPayload,
					},
				},
			},
		},
	})
	swt.s.NoError(err)

	// Second poll: wait for NexusOperationCompleted or NexusOperationFailed.
	pollResp, err = swt.env.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: swt.env.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{Name: nexusCallerTaskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
	})
	swt.s.NoError(err)

	// Pull the StartWithSignalWorkflowExecutionResponse from the history events.
	var (
		startWithSignalResponse workflowservice.SignalWithStartWorkflowExecutionResponse
		found                   bool
	)
	for _, event := range pollResp.History.Events {
		if attrs := event.GetNexusOperationCompletedEventAttributes(); attrs != nil {
			found = true
			convErr := sdkconverter.PreferProtoDataConverter.FromPayloads(
				&commonpb.Payloads{Payloads: []*commonpb.Payload{attrs.Result}},
				&startWithSignalResponse,
			)
			swt.s.NoError(convErr)
		}
		if attrs := event.GetNexusOperationFailedEventAttributes(); attrs != nil {
			swt.s.Fail("expected NexusOperationCompleted but got NexusOperationFailed: " + attrs.Failure.GetMessage())
		}
	}
	swt.s.True(found, "did not see Nexus operation complete in workflow events")

	return &startWithSignalResponse
}
