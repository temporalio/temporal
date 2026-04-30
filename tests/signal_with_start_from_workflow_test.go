package tests

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/temporalproto"
	"go.temporal.io/api/workflowservice/v1"
	workflowservicev1 "go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/api/workflowservice/v1/workflowservicenexus"
	"go.temporal.io/sdk/client"
	sdkworker "go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/dynamicconfig"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/payloads"
	sdkconverter "go.temporal.io/server/common/sdk"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

// systemNexusSWSWorkflow is an SDK workflow that calls SignalWithStartWorkflowExecution
// via the __temporal_system Nexus endpoint and returns the RunID of the started/signaled
// target workflow. It is used by TestBothWorkflowsVisibleAfterSWSFromWorkflow to verify
// end-to-end SDK serialization against the real server.
func systemNexusSWSWorkflow(ctx workflow.Context, req *workflowservicev1.SignalWithStartWorkflowExecutionRequest) (string, error) {
	nc := workflow.NewNexusClient(commonnexus.SystemEndpoint, workflowservicenexus.WorkflowService.ServiceName)
	// fut := nc.ExecuteOperation(ctx, systemnexus.WorkflowService.SignalWithStartWorkflowExecution, req, workflow.NexusOperationOptions{})
	fut := nc.ExecuteOperation(ctx, workflowservicenexus.WorkflowService.SignalWithStartWorkflowExecution,
		req,
		workflow.NexusOperationOptions{})
	var result workflowservicev1.SignalWorkflowExecutionResponse
	if err := fut.Get(ctx, &result); err != nil {
		return "", err
	}
	return "some-run-id", nil
	// return result.RunId, nil
}

// sysNexusSWSTargetWorkflow is the workflow started by TestBothWorkflowsVisibleAfterSWSFromWorkflow
// as the SWS target. It waits for "test-signal" and returns the received value. Completing the
// workflow (rather than leaving it running) ensures the Nexus SWS operation's async callback fires
// so that fut.Get() in systemNexusSWSWorkflow can resolve.
func sysNexusSWSTargetWorkflow(ctx workflow.Context) (string, error) {
	var received string
	workflow.GetSignalChannel(ctx, "test-signal").Receive(ctx, &received)
	return received, nil
}

type SignalWithStartFromWorkflowTestSuite struct {
	testcore.FunctionalTestBase
}

func TestSignalWithStartFromWorkflowTestSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(SignalWithStartFromWorkflowTestSuite))
}

func (s *SignalWithStartFromWorkflowTestSuite) SetupSuite() {
	s.SetupSuiteWithCluster(
		testcore.WithDynamicConfigOverrides(map[dynamicconfig.Key]any{
			dynamicconfig.EnableChasm.Key():                 true,
			dynamicconfig.EnableSystemNexusOperations.Key(): true,
		}),
	)
}

// scheduleAndGetSWSResult dispatches a SignalWithStartWorkflowExecution Nexus operation
// from within a fresh caller workflow via the __temporal_system endpoint, waits for the
// operation to complete or fail, and returns the result.
//
// The caller workflow is terminated before this function returns.
// swsReq must NOT set Namespace, RequestId, or Links — the processor populates those from
// the Nexus operation context.
func (s *SignalWithStartFromWorkflowTestSuite) scheduleAndGetSWSResult(
	ctx context.Context,
	callerTaskQueue string,
	swsReq *workflowservice.SignalWithStartWorkflowExecutionRequest,
) (*workflowservice.SignalWithStartWorkflowExecutionResponse, *failurepb.Failure) {
	callerRun, err := s.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		TaskQueue: callerTaskQueue,
	}, "caller-workflow")
	s.NoError(err)
	defer func() {
		_ = s.SdkClient().TerminateWorkflow(ctx, callerRun.GetID(), callerRun.GetRunID(), "test cleanup")
	}()

	// First poll: schedule the SWS Nexus operation.
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
						Service:   "WorkflowService",
						Operation: "SignalWithStartWorkflowExecution",
						Input:     payloads.MustEncodeSingle(swsReq),
					},
				},
			},
		},
	})
	s.NoError(err)

	// Second poll: wait for the NexusOperationCompleted or NexusOperationFailed event.
	pollResp, err = s.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{Name: callerTaskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:  "test",
	})
	s.NoError(err)

	for _, event := range pollResp.History.Events {
		if attrs := event.GetNexusOperationCompletedEventAttributes(); attrs != nil {
			var resp workflowservice.SignalWithStartWorkflowExecutionResponse
			s.NoError((temporalproto.CustomJSONUnmarshalOptions{}).Unmarshal(attrs.Result.GetData(), &resp))
			return &resp, nil
		}
		if attrs := event.GetNexusOperationFailedEventAttributes(); attrs != nil {
			return nil, attrs.Failure
		}
	}
	s.Fail("expected NexusOperationCompleted or NexusOperationFailed event in workflow history")
	return nil, nil
}

// startAndCompleteWorkflow starts a workflow and immediately completes it by responding to
// its first workflow task. Returns the run ID of the completed execution.
func (s *SignalWithStartFromWorkflowTestSuite) startAndCompleteWorkflow(
	ctx context.Context,
	workflowID, taskQueue string,
) string {
	_, err := s.FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
		Namespace:    s.Namespace().String(),
		WorkflowId:   workflowID,
		WorkflowType: &commonpb.WorkflowType{Name: "target-workflow"},
		TaskQueue:    &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		RequestId:    uuid.NewString(),
	})
	s.NoError(err)

	pollResp, err := s.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:  "test",
	})
	s.NoError(err)
	runID := pollResp.WorkflowExecution.RunId

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
	return runID
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
						Service:   "WorkflowService",
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
	var response workflowservice.SignalWithStartWorkflowExecutionResponse
	s.NoError(run.Get(ctx, &response))
	s.True(response.Started)

	// err = s.SdkClient().TerminateWorkflow(ctx, workflowID, response.RunID, "test cleanup")
	err = s.SdkClient().TerminateWorkflow(ctx, workflowID, "response.RunID", "test cleanup")
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
	// s.Equal(response.RunID, link.GetWorkflowEvent().GetRunId())
	s.Equal(opScheduledEvent.GetNexusOperationScheduledEventAttributes().GetRequestId(), link.GetWorkflowEvent().GetRequestIdRef().GetRequestId())

	// Verify the linkage from the caller workflow in the handler's history.
	// it = s.SdkClient().GetWorkflowHistory(ctx, workflowID, response.RunID, false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	it = s.SdkClient().GetWorkflowHistory(ctx, workflowID, "", false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
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
	// desc, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowID, response.RunID)
	// s.NoError(err)
	// requestIDInfos := desc.GetWorkflowExtendedInfo().GetRequestIdInfos()
	// requestID := slices.Collect(maps.Keys(requestIDInfos))[0]
	// s.Equal(opScheduledEvent.GetNexusOperationScheduledEventAttributes().GetRequestId(), requestID)
}

// TestSignalExistingWorkflow verifies that SWS called from a workflow signals an already-running
// target workflow without starting a new one (Started=false, RunId unchanged).
func (s *SignalWithStartFromWorkflowTestSuite) TestSignalExistingWorkflow() {
	ctx := testcore.NewContext()
	callerTaskQueue := testcore.RandomizeStr(s.T().Name())
	targetTaskQueue := testcore.RandomizeStr(s.T().Name() + "-target")
	targetWorkflowID := testcore.RandomizeStr(s.T().Name())

	// Start the target workflow and leave it running.
	startResp, err := s.FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
		Namespace:    s.Namespace().String(),
		WorkflowId:   targetWorkflowID,
		WorkflowType: &commonpb.WorkflowType{Name: "target-workflow"},
		TaskQueue:    &taskqueuepb.TaskQueue{Name: targetTaskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		RequestId:    uuid.NewString(),
	})
	s.T().Cleanup(func() {
		_ = s.SdkClient().TerminateWorkflow(ctx, targetWorkflowID, startResp.RunId, "test cleanup")
	})
	s.NoError(err)
	originalRunID := startResp.RunId

	resp, failure := s.scheduleAndGetSWSResult(ctx, callerTaskQueue, &workflowservice.SignalWithStartWorkflowExecutionRequest{
		WorkflowId:            targetWorkflowID,
		SignalName:            "test-signal",
		WorkflowType:          &commonpb.WorkflowType{Name: "target-workflow"},
		TaskQueue:             &taskqueuepb.TaskQueue{Name: targetTaskQueue},
		WorkflowIdReusePolicy: enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
	})

	s.Nil(failure)
	s.False(resp.Started, "expected Started=false when signaling an existing workflow")
	s.Equal(originalRunID, resp.RunId)
}

// TestStartNewWorkflow verifies that SWS called from a workflow starts a new execution when no
// workflow with the given ID exists (Started=true).
func (s *SignalWithStartFromWorkflowTestSuite) TestStartNewWorkflow() {
	ctx := testcore.NewContext()
	callerTaskQueue := testcore.RandomizeStr(s.T().Name())
	targetTaskQueue := testcore.RandomizeStr(s.T().Name() + "-target")
	targetWorkflowID := testcore.RandomizeStr(s.T().Name())

	resp, failure := s.scheduleAndGetSWSResult(ctx, callerTaskQueue, &workflowservice.SignalWithStartWorkflowExecutionRequest{
		WorkflowId:   targetWorkflowID,
		SignalName:   "test-signal",
		WorkflowType: &commonpb.WorkflowType{Name: "target-workflow"},
		TaskQueue:    &taskqueuepb.TaskQueue{Name: targetTaskQueue},
	})
	s.T().Cleanup(func() {
		_ = s.SdkClient().TerminateWorkflow(ctx, targetWorkflowID, resp.RunId, "test cleanup")
	})

	s.Nil(failure)
	s.True(resp.Started, "expected Started=true when starting a new workflow")
	s.NotEmpty(resp.RunId)
}

// TestSignalTerminatedWorkflow verifies that SWS starts a fresh run when the target workflow
// has been terminated (Started=true, new RunId).
func (s *SignalWithStartFromWorkflowTestSuite) TestSignalTerminatedWorkflow() {
	ctx := testcore.NewContext()
	callerTaskQueue := testcore.RandomizeStr(s.T().Name())
	targetTaskQueue := testcore.RandomizeStr(s.T().Name() + "-target")
	targetWorkflowID := testcore.RandomizeStr(s.T().Name())

	// Start and terminate the target workflow.
	startResp, err := s.FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
		Namespace:    s.Namespace().String(),
		WorkflowId:   targetWorkflowID,
		WorkflowType: &commonpb.WorkflowType{Name: "target-workflow"},
		TaskQueue:    &taskqueuepb.TaskQueue{Name: targetTaskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		RequestId:    uuid.NewString(),
	})
	s.NoError(err)
	originalRunID := startResp.RunId

	err = s.SdkClient().TerminateWorkflow(ctx, targetWorkflowID, originalRunID, "setup")
	s.NoError(err)

	resp, failure := s.scheduleAndGetSWSResult(ctx, callerTaskQueue, &workflowservice.SignalWithStartWorkflowExecutionRequest{
		WorkflowId:   targetWorkflowID,
		SignalName:   "test-signal",
		WorkflowType: &commonpb.WorkflowType{Name: "target-workflow"},
		TaskQueue:    &taskqueuepb.TaskQueue{Name: targetTaskQueue},
	})

	s.Nil(failure)
	s.True(resp.Started, "expected Started=true when target was terminated")
	s.NotEqual(originalRunID, resp.RunId, "expected a new RunId after termination")
}

// TestIDReusePolicy_RejectDuplicate verifies that SWS fails with WorkflowExecutionAlreadyStarted
// when the target workflow has completed and the reuse policy is REJECT_DUPLICATE.
func (s *SignalWithStartFromWorkflowTestSuite) TestIDReusePolicy_RejectDuplicate() {
	s.OverrideDynamicConfig(dynamicconfig.WorkflowIdReuseMinimalInterval, 0)

	ctx := testcore.NewContext()
	callerTaskQueue := testcore.RandomizeStr(s.T().Name())
	targetTaskQueue := testcore.RandomizeStr(s.T().Name() + "-target")
	targetWorkflowID := testcore.RandomizeStr(s.T().Name())

	s.startAndCompleteWorkflow(ctx, targetWorkflowID, targetTaskQueue)

	_, failure := s.scheduleAndGetSWSResult(ctx, callerTaskQueue, &workflowservice.SignalWithStartWorkflowExecutionRequest{
		WorkflowId:            targetWorkflowID,
		SignalName:            "test-signal",
		WorkflowType:          &commonpb.WorkflowType{Name: "target-workflow"},
		TaskQueue:             &taskqueuepb.TaskQueue{Name: targetTaskQueue},
		WorkflowIdReusePolicy: enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
	})

	s.NotNil(failure, "expected the Nexus operation to fail")
	s.Contains(failure.GetCause().GetMessage()+failure.GetMessage(), "duplicate")
}

// TestIDReusePolicy_AllowDuplicate verifies that SWS starts a new run when the target has
// completed and the reuse policy is ALLOW_DUPLICATE (Started=true).
func (s *SignalWithStartFromWorkflowTestSuite) TestIDReusePolicy_AllowDuplicate() {
	s.OverrideDynamicConfig(dynamicconfig.WorkflowIdReuseMinimalInterval, 0)

	ctx := testcore.NewContext()
	callerTaskQueue := testcore.RandomizeStr(s.T().Name())
	targetTaskQueue := testcore.RandomizeStr(s.T().Name() + "-target")
	targetWorkflowID := testcore.RandomizeStr(s.T().Name())

	s.startAndCompleteWorkflow(ctx, targetWorkflowID, targetTaskQueue)

	resp, failure := s.scheduleAndGetSWSResult(ctx, callerTaskQueue, &workflowservice.SignalWithStartWorkflowExecutionRequest{
		WorkflowId:            targetWorkflowID,
		SignalName:            "test-signal",
		WorkflowType:          &commonpb.WorkflowType{Name: "target-workflow"},
		TaskQueue:             &taskqueuepb.TaskQueue{Name: targetTaskQueue},
		WorkflowIdReusePolicy: enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
	})
	s.T().Cleanup(func() {
		_ = s.SdkClient().TerminateWorkflow(ctx, targetWorkflowID, resp.RunId, "test cleanup")
	})

	s.Nil(failure)
	s.True(resp.Started, "expected Started=true with ALLOW_DUPLICATE after completion")
	s.NotEmpty(resp.RunId)
}

// TestIDReusePolicy_AllowDuplicateFailedOnly covers two sub-cases for ALLOW_DUPLICATE_FAILED_ONLY:
//  1. Target completed successfully → SWS fails (already started error).
//  2. Target was terminated → SWS starts a new run (Started=true).
func (s *SignalWithStartFromWorkflowTestSuite) TestIDReusePolicy_AllowDuplicateFailedOnly() {
	s.OverrideDynamicConfig(dynamicconfig.WorkflowIdReuseMinimalInterval, 0)

	ctx := testcore.NewContext()
	targetTaskQueue := testcore.RandomizeStr(s.T().Name() + "-target")
	targetWorkflowID := testcore.RandomizeStr(s.T().Name())

	// Sub-case 1: target completed successfully → should fail.
	s.startAndCompleteWorkflow(ctx, targetWorkflowID, targetTaskQueue)

	_, failure := s.scheduleAndGetSWSResult(
		ctx,
		testcore.RandomizeStr(s.T().Name()),
		&workflowservice.SignalWithStartWorkflowExecutionRequest{
			WorkflowId:            targetWorkflowID,
			SignalName:            "test-signal",
			WorkflowType:          &commonpb.WorkflowType{Name: "target-workflow"},
			TaskQueue:             &taskqueuepb.TaskQueue{Name: targetTaskQueue},
			WorkflowIdReusePolicy: enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY,
		},
	)
	s.NotNil(failure, "expected failure when completed workflow + ALLOW_DUPLICATE_FAILED_ONLY")

	// Sub-case 2: target terminated → should start a new run.
	startResp, err := s.FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
		Namespace:    s.Namespace().String(),
		WorkflowId:   targetWorkflowID,
		WorkflowType: &commonpb.WorkflowType{Name: "target-workflow"},
		TaskQueue:    &taskqueuepb.TaskQueue{Name: targetTaskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		RequestId:    uuid.NewString(),
	})
	s.NoError(err)
	err = s.SdkClient().TerminateWorkflow(ctx, targetWorkflowID, startResp.RunId, "setup")
	s.NoError(err)

	resp, failure := s.scheduleAndGetSWSResult(
		ctx,
		testcore.RandomizeStr(s.T().Name()),
		&workflowservice.SignalWithStartWorkflowExecutionRequest{
			WorkflowId:            targetWorkflowID,
			SignalName:            "test-signal",
			WorkflowType:          &commonpb.WorkflowType{Name: "target-workflow"},
			TaskQueue:             &taskqueuepb.TaskQueue{Name: targetTaskQueue},
			WorkflowIdReusePolicy: enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY,
		},
	)
	s.T().Cleanup(func() {
		_ = s.SdkClient().TerminateWorkflow(ctx, targetWorkflowID, resp.RunId, "test cleanup")
	})
	s.Nil(failure)
	s.True(resp.Started, "expected Started=true after terminated workflow + ALLOW_DUPLICATE_FAILED_ONLY")
}

// TestIDConflictPolicy_TerminateExisting verifies that SWS terminates a running workflow and
// starts a new one when the conflict policy is TERMINATE_EXISTING (Started=true, new RunId,
// original run terminated).
func (s *SignalWithStartFromWorkflowTestSuite) TestIDConflictPolicy_TerminateExisting() {
	ctx := testcore.NewContext()
	callerTaskQueue := testcore.RandomizeStr(s.T().Name())
	targetTaskQueue := testcore.RandomizeStr(s.T().Name() + "-target")
	targetWorkflowID := testcore.RandomizeStr(s.T().Name())

	startResp, err := s.FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
		Namespace:    s.Namespace().String(),
		WorkflowId:   targetWorkflowID,
		WorkflowType: &commonpb.WorkflowType{Name: "target-workflow"},
		TaskQueue:    &taskqueuepb.TaskQueue{Name: targetTaskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		RequestId:    uuid.NewString(),
	})
	s.NoError(err)
	originalRunID := startResp.RunId

	resp, failure := s.scheduleAndGetSWSResult(ctx, callerTaskQueue, &workflowservice.SignalWithStartWorkflowExecutionRequest{
		WorkflowId:               targetWorkflowID,
		SignalName:               "test-signal",
		WorkflowType:             &commonpb.WorkflowType{Name: "target-workflow"},
		TaskQueue:                &taskqueuepb.TaskQueue{Name: targetTaskQueue},
		WorkflowIdConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_TERMINATE_EXISTING,
	})
	s.T().Cleanup(func() {
		_ = s.SdkClient().TerminateWorkflow(ctx, targetWorkflowID, resp.RunId, "test cleanup")
	})

	s.Nil(failure)
	s.True(resp.Started, "expected Started=true with TERMINATE_EXISTING")
	s.NotEqual(originalRunID, resp.RunId, "expected a new RunId")

	// Verify the original run was terminated.
	desc, err := s.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: s.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: targetWorkflowID, RunId: originalRunID},
	})
	s.NoError(err)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED, desc.WorkflowExecutionInfo.Status)
}

// TestIDConflictPolicy_UseExisting verifies that SWS signals an existing running workflow and
// returns its RunId without starting a new one (Started=false) when the conflict policy is
// USE_EXISTING.
func (s *SignalWithStartFromWorkflowTestSuite) TestIDConflictPolicy_UseExisting() {
	ctx := testcore.NewContext()
	callerTaskQueue := testcore.RandomizeStr(s.T().Name())
	targetTaskQueue := testcore.RandomizeStr(s.T().Name() + "-target")
	targetWorkflowID := testcore.RandomizeStr(s.T().Name())

	startResp, err := s.FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
		Namespace:    s.Namespace().String(),
		WorkflowId:   targetWorkflowID,
		WorkflowType: &commonpb.WorkflowType{Name: "target-workflow"},
		TaskQueue:    &taskqueuepb.TaskQueue{Name: targetTaskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		RequestId:    uuid.NewString(),
	})
	s.T().Cleanup(func() {
		_ = s.SdkClient().TerminateWorkflow(ctx, targetWorkflowID, startResp.RunId, "test cleanup")
	})
	s.NoError(err)
	originalRunID := startResp.RunId

	resp, failure := s.scheduleAndGetSWSResult(ctx, callerTaskQueue, &workflowservice.SignalWithStartWorkflowExecutionRequest{
		WorkflowId:               targetWorkflowID,
		SignalName:               "test-signal",
		WorkflowType:             &commonpb.WorkflowType{Name: "target-workflow"},
		TaskQueue:                &taskqueuepb.TaskQueue{Name: targetTaskQueue},
		WorkflowIdConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
	})
	s.T().Cleanup(func() {
		_ = s.SdkClient().TerminateWorkflow(ctx, targetWorkflowID, resp.RunId, "test cleanup")
	})
	s.Nil(failure)
	s.False(resp.Started, "expected Started=false with USE_EXISTING")
	s.Equal(originalRunID, resp.RunId)
}

// TestIDConflictPolicy_Fail verifies that SWS fails with WorkflowExecutionAlreadyStarted when
// a workflow with the same ID is already running and the conflict policy is FAIL.
func (s *SignalWithStartFromWorkflowTestSuite) TestIDConflictPolicy_Fail() {
	ctx := testcore.NewContext()
	callerTaskQueue := testcore.RandomizeStr(s.T().Name())
	targetTaskQueue := testcore.RandomizeStr(s.T().Name() + "-target")
	targetWorkflowID := testcore.RandomizeStr(s.T().Name())

	startResp, err := s.FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
		Namespace:    s.Namespace().String(),
		WorkflowId:   targetWorkflowID,
		WorkflowType: &commonpb.WorkflowType{Name: "target-workflow"},
		TaskQueue:    &taskqueuepb.TaskQueue{Name: targetTaskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		RequestId:    uuid.NewString(),
	})
	s.T().Cleanup(func() {
		_ = s.SdkClient().TerminateWorkflow(ctx, targetWorkflowID, startResp.RunId, "test cleanup")
	})
	s.NoError(err)

	_, failure := s.scheduleAndGetSWSResult(ctx, callerTaskQueue, &workflowservice.SignalWithStartWorkflowExecutionRequest{
		WorkflowId:               targetWorkflowID,
		SignalName:               "test-signal",
		WorkflowType:             &commonpb.WorkflowType{Name: "target-workflow"},
		TaskQueue:                &taskqueuepb.TaskQueue{Name: targetTaskQueue},
		WorkflowIdConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL,
	})
	s.NotNil(failure, "expected the Nexus operation to fail with CONFLICT_POLICY_FAIL")
	s.Contains(failure.GetCause().GetMessage()+failure.GetMessage(), "WorkflowExecutionAlreadyStarted")
}

// TestBothWorkflowsVisibleAfterSWSFromWorkflow verifies that when SignalWithStart is invoked
// from a real SDK workflow via the __temporal_system Nexus endpoint:
//  1. A new target workflow is started (the caller workflow returns its RunID).
//  2. Both the caller (completed) and target (completed after receiving the signal) are visible.
//  3. The memo passed in the SWS request appears on the target workflow.
//  4. The signal arrives in the target with the correct name and input payload.
//
// Unlike the other tests in this file, this test exercises the SDK's payload-serialization
// path (the system-nexus payload converter) end-to-end against the real embedded server,
// complementing the injector-based SDK unit test in sdk-go#2293.
func (s *SignalWithStartFromWorkflowTestSuite) TestBothWorkflowsVisibleAfterSWSFromWorkflow() {
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	s.T().Cleanup(cancel)
	callerTaskQueue := testcore.RandomizeStr(s.T().Name())
	targetTaskQueue := testcore.RandomizeStr(s.T().Name() + "-target")
	targetWorkflowID := testcore.RandomizeStr(s.T().Name())

	// Stand up dedicated SDK workers for the caller and target workflows.
	callerWorker := sdkworker.New(s.SdkClient(), callerTaskQueue, sdkworker.Options{})
	callerWorker.RegisterWorkflow(systemNexusSWSWorkflow)
	s.NoError(callerWorker.Start())
	s.T().Cleanup(func() { callerWorker.Stop() })

	targetWorker := sdkworker.New(s.SdkClient(), targetTaskQueue, sdkworker.Options{})
	targetWorker.RegisterWorkflow(sysNexusSWSTargetWorkflow)
	s.NoError(targetWorker.Start())
	s.T().Cleanup(func() { targetWorker.Stop() })

	// Execute the caller workflow. It calls SWS via the system Nexus endpoint and returns
	// the RunID of the newly-started target workflow.
	callerRun, err := s.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		TaskQueue: callerTaskQueue,
	}, systemNexusSWSWorkflow, workflowservicev1.SignalWithStartWorkflowExecutionRequest{
		WorkflowId:   targetWorkflowID,
		SignalName:   "test-signal",
		WorkflowType: &commonpb.WorkflowType{Name: "sysNexusSWSTargetWorkflow"},
		TaskQueue:    &taskqueuepb.TaskQueue{Name: targetTaskQueue},
		Input:        &commonpb.Payloads{Payloads: []*commonpb.Payload{{Data: []byte("workflow-input")}}},
		SignalInput:  &commonpb.Payloads{Payloads: []*commonpb.Payload{{Data: []byte("signal-input")}}},
		Memo:         &commonpb.Memo{Fields: map[string]*commonpb.Payload{"memo-key": {Data: []byte("memo-value")}}},
	})
	s.NoError(err)
	s.NotEmpty(callerRun.GetID())
	s.NotEmpty(callerRun.GetRunID())
	s.T().Cleanup(func() {
		_ = s.SdkClient().TerminateWorkflow(ctx, targetWorkflowID, "", "test cleanup")
	})

	// --- Assertion 1: Caller workflow completes and returns the target's RunID. ---
	// callerRun.Get blocks until the caller workflow finishes (or the context times out),
	// implicitly asserting it reaches COMPLETED status.
	var targetRunID string
	s.NoError(callerRun.Get(ctx, &targetRunID))
	s.NotEmpty(targetRunID)

	// Confirm COMPLETED via Describe now that we know the caller has finished.
	callerDesc, err := s.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: s.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: callerRun.GetID(), RunId: callerRun.GetRunID()},
	})
	s.NoError(err)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, callerDesc.WorkflowExecutionInfo.Status)

	// --- Assertion 2: Target workflow completes and returns the signal input value. ---
	// GetWorkflow(...).Get blocks until the target workflow finishes, implicitly asserting
	// it reaches COMPLETED status. The target returns whatever signal payload it received.
	var targetResult string
	s.NoError(s.SdkClient().GetWorkflow(ctx, targetWorkflowID, targetRunID).Get(ctx, &targetResult))
	s.Equal("signal-input", targetResult)

	// Confirm COMPLETED via Describe now that we know the target has finished.
	targetDesc, err := s.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: s.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: targetWorkflowID, RunId: targetRunID},
	})
	s.NoError(err)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, targetDesc.WorkflowExecutionInfo.Status)

	// --- Assertion 3: Target carries the memo passed in the SWS request. ---
	s.Require().NotNil(targetDesc.WorkflowExecutionInfo.Memo)
	s.Contains(targetDesc.WorkflowExecutionInfo.Memo.Fields, "memo-key")

	// --- Assertion 4: Signal was delivered with the correct name and input. ---
	// Since the target has already completed, its full history is available without polling.
	histResp, err := s.FrontendClient().GetWorkflowExecutionHistory(ctx, &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace: s.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: targetWorkflowID, RunId: targetRunID},
	})
	s.NoError(err)
	var signalEvent *historypb.HistoryEvent
	for _, event := range histResp.History.Events {
		if event.GetWorkflowExecutionSignaledEventAttributes() != nil {
			signalEvent = event
			break
		}
	}
	s.Require().NotNil(signalEvent, "expected WorkflowExecutionSignaled event in target history")
	s.Equal("test-signal", signalEvent.GetWorkflowExecutionSignaledEventAttributes().SignalName)
	var signalInputVal string
	s.NoError(payloads.Decode(signalEvent.GetWorkflowExecutionSignaledEventAttributes().Input, &signalInputVal))
	s.Equal("signal-input", signalInputVal)
}

// TestBothWorkflowsVisibleAfterSWSFromWorkflowProtoBinary is identical to
// TestBothWorkflowsVisibleAfterSWSFromWorkflow but sends the SWS request as a proto binary
// (binary/protobuf) payload instead of relying on the SDK's default JSON-proto encoding.
// This exercises the binary/protobuf decode path in nexusOperationProcessorAdapter and
// verifies that the server accepts and correctly processes such requests — matching what
// the Python SDK (and other SDKs that prefer proto binary) sends.
func (s *SignalWithStartFromWorkflowTestSuite) TestBothWorkflowsVisibleAfterSWSFromWorkflowProtoBinary() {
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	s.T().Cleanup(cancel)

	callerTaskQueue := testcore.RandomizeStr(s.T().Name())
	targetTaskQueue := testcore.RandomizeStr(s.T().Name() + "-target")
	targetWorkflowID := testcore.RandomizeStr(s.T().Name())

	// Start a caller workflow to obtain an initial workflow task.
	callerRun, err := s.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		TaskQueue: callerTaskQueue,
	}, "caller-workflow")
	s.NoError(err)
	s.T().Cleanup(func() {
		_ = s.SdkClient().TerminateWorkflow(ctx, callerRun.GetID(), callerRun.GetRunID(), "test cleanup")
	})

	// Encode the SWS request as binary/protobuf. PreferProtoDataConverter places
	// ProtoPayloadConverter first, so proto messages are marshalled to binary/protobuf
	// rather than the JSON proto encoding that the SDK uses by default.
	swsReq := &workflowservice.SignalWithStartWorkflowExecutionRequest{
		WorkflowId:   targetWorkflowID,
		SignalName:   "test-signal",
		WorkflowType: &commonpb.WorkflowType{Name: "target-workflow"},
		TaskQueue:    &taskqueuepb.TaskQueue{Name: targetTaskQueue},
		Memo:         &commonpb.Memo{Fields: map[string]*commonpb.Payload{"memo-key": {Data: []byte("memo-value")}}},
	}
	pls, err := sdkconverter.PreferProtoDataConverter.ToPayloads(swsReq)
	s.NoError(err)
	s.Require().Len(pls.Payloads, 1)
	protoBinaryPayload := pls.Payloads[0]
	s.Equal("binary/protobuf", string(protoBinaryPayload.Metadata["encoding"]))

	// First poll: respond with a ScheduleNexusOperation command carrying the proto binary input.
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
						Service:   "WorkflowService",
						Operation: "SignalWithStartWorkflowExecution",
						Input:     protoBinaryPayload,
					},
				},
			},
		},
	})
	s.NoError(err)

	// Second poll: wait for NexusOperationCompleted or NexusOperationFailed.
	pollResp, err = s.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{Name: callerTaskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:  "test",
	})
	s.NoError(err)

	var sswResp workflowservice.SignalWithStartWorkflowExecutionResponse
	for _, event := range pollResp.History.Events {
		if attrs := event.GetNexusOperationCompletedEventAttributes(); attrs != nil {
			s.NoError(sdkconverter.PreferProtoDataConverter.FromPayloads(
				&commonpb.Payloads{Payloads: []*commonpb.Payload{attrs.Result}},
				&sswResp,
			))
		}
		if attrs := event.GetNexusOperationFailedEventAttributes(); attrs != nil {
			s.Fail("expected NexusOperationCompleted but got NexusOperationFailed: " + attrs.Failure.GetMessage())
		}
	}

	// The operation must have started a new workflow.
	s.True(sswResp.Started, "expected Started=true for proto binary encoded SWS request")
	s.NotEmpty(sswResp.RunId)

	s.T().Cleanup(func() {
		_ = s.SdkClient().TerminateWorkflow(ctx, targetWorkflowID, sswResp.RunId, "test cleanup")
	})

	// Both workflows must be visible.
	callerDesc, err := s.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: s.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: callerRun.GetID(), RunId: callerRun.GetRunID()},
	})
	s.NoError(err)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, callerDesc.WorkflowExecutionInfo.Status)

	targetDesc, err := s.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: s.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: targetWorkflowID, RunId: sswResp.RunId},
	})
	s.NoError(err)
	s.Require().NotNil(targetDesc.WorkflowExecutionInfo.Memo)
	s.Contains(targetDesc.WorkflowExecutionInfo.Memo.Fields, "memo-key")
}

// TestStartDelay verifies that SWS with WorkflowStartDelay completes successfully from a
// workflow (Started=true) and that the target workflow eventually becomes running.
func (s *SignalWithStartFromWorkflowTestSuite) TestStartDelay() {
	ctx := testcore.NewContext()
	callerTaskQueue := testcore.RandomizeStr(s.T().Name())
	targetTaskQueue := testcore.RandomizeStr(s.T().Name() + "-target")
	targetWorkflowID := testcore.RandomizeStr(s.T().Name())

	startDelay := 2 * time.Second

	resp, failure := s.scheduleAndGetSWSResult(ctx, callerTaskQueue, &workflowservice.SignalWithStartWorkflowExecutionRequest{
		WorkflowId:         targetWorkflowID,
		SignalName:         "test-signal",
		WorkflowType:       &commonpb.WorkflowType{Name: "target-workflow"},
		TaskQueue:          &taskqueuepb.TaskQueue{Name: targetTaskQueue},
		WorkflowStartDelay: durationpb.New(startDelay),
	})
	s.T().Cleanup(func() {
		_ = s.SdkClient().TerminateWorkflow(ctx, targetWorkflowID, resp.RunId, "test cleanup")
	})
	s.Nil(failure)
	s.True(resp.Started, "expected Started=true with WorkflowStartDelay")
	s.NotEmpty(resp.RunId)

	// Verify the workflow eventually becomes running after the delay.
	s.Eventually(func() bool {
		desc, err := s.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: s.Namespace().String(),
			Execution: &commonpb.WorkflowExecution{WorkflowId: targetWorkflowID, RunId: resp.RunId},
		})
		if err != nil {
			return false
		}
		return desc.WorkflowExecutionInfo.Status == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
	}, startDelay+5*time.Second, 200*time.Millisecond)
}
