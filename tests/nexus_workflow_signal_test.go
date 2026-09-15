package tests

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/nexus-rpc/sdk-go/nexus"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/log"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexustest"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/tests/testcore"
)

const (
	nexusSignalName  = "nexus-signal"
	finishSignalName = "finish"
)

type signalOperationArgs struct {
	HandlerWorkflowID string
	SignalName        string
	RequestID         string
}

func (s *NexusWorkflowTestSuite) TestNexusOperationBackedBySignal(chasmEnabled bool) {
	env := s.newTestEnv(chasmEnabled)
	ctx := s.Context()
	taskQueue := testcore.RandomizeStr(s.T().Name())
	handlerWorkflowID := testcore.RandomizeStr(s.T().Name() + "-handler")
	nexusHandler := nexustest.Handler{
		OnStartOperation: func(
			ctx context.Context,
			service string,
			operation string,
			input *nexus.LazyValue,
			options nexus.StartOperationOptions,
		) (nexus.HandlerStartOperationResult[any], error) {
			var args signalOperationArgs
			if err := input.Consume(&args); err != nil {
				return nil, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "invalid signal arguments: %v", err)
			}
			resp, err := env.FrontendClient().SignalWorkflowExecution(ctx, &workflowservice.SignalWorkflowExecutionRequest{
				Namespace:         env.Namespace().String(),
				WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: args.HandlerWorkflowID},
				SignalName:        args.SignalName,
				RequestId:         args.RequestID,
				Links:             commonnexus.ConvertNexusLinksToProtoLinks(options.Links, log.NewNoopLogger()),
			})
			if err != nil {
				// Note: The handler could choose to return a non-retryable error instead.
				return nil, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeInternal, "signal failed: %v", err)
			}
			workflowEventLink := resp.GetLink().GetWorkflowEvent()
			if workflowEventLink == nil {
				return nil, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeInternal, "signal response did not contain a workflow event link")
			}
			nexus.AddHandlerLinks(ctx, commonnexus.ConvertLinkWorkflowEventToNexusLink(workflowEventLink))
			return &nexus.HandlerStartOperationResultSync[any]{Value: workflowEventLink.GetRunId()}, nil
		},
	}
	endpointName := env.createRandomExternalNexusServer(ctx, s.T(), nexusHandler)
	w := worker.New(env.SdkClient(), taskQueue, worker.Options{})
	w.RegisterWorkflow(signalHandlerWorkflow)
	w.RegisterWorkflow(signalCallerWorkflow)
	s.NoError(w.Start())
	defer w.Stop()

	handlerRun, err := env.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: handlerWorkflowID, TaskQueue: taskQueue}, signalHandlerWorkflow)
	s.NoError(err)
	args := signalOperationArgs{HandlerWorkflowID: handlerWorkflowID, SignalName: nexusSignalName, RequestID: uuid.NewString()}

	s.RunSequential("Signals on running workflows complete", func(s *NexusWorkflowTestSuite) {
		callerRun := s.startCaller(ctx, env, taskQueue, endpointName, "signal", args)
		s.assertSignalLinks(ctx, env, chasmEnabled, callerRun, handlerRun, args.RequestID)
	})
	s.RunSequential("Signals de-dup on requestID", func(s *NexusWorkflowTestSuite) {
		callerRun := s.startCaller(ctx, env, taskQueue, endpointName, "signal", args)
		s.assertSignalForwardLink(ctx, env, callerRun, handlerRun, args.RequestID)
		// Verify no duplicates in handler history as well.
		s.handlerSignalEvent(env, handlerRun, args.RequestID)
	})
	s.RunSequential("Workflow reset preserves signal with links", func(s *NexusWorkflowTestSuite) {
		preResetLinks := s.handlerSignalEvent(env, handlerRun, args.RequestID).GetLinks()
		handlerRun = s.resetHandlerWorkflowAfterSignal(ctx, env, handlerRun, args.RequestID)
		signaledEvent := s.handlerSignalEvent(env, handlerRun, args.RequestID)
		protorequire.ProtoSliceEqual(s.T(), preResetLinks, signaledEvent.GetLinks())
		s.assertHandlerSignalRequestIDResolves(ctx, env, chasmEnabled, handlerRun, args.RequestID, signaledEvent)
	})
	// Intentionally skip unknown workflow/run IDs. Although frontend rejects these requests, the Nexus
	// handler could choose to retry- maybe until a fixed version of the handler is deployed.
}

func (s *NexusWorkflowTestSuite) TestNexusOperationBackedBySignalWithStart(chasmEnabled bool) {
	env := s.newTestEnv(chasmEnabled)
	ctx := s.Context()
	taskQueue := testcore.RandomizeStr(s.T().Name())
	handlerWorkflowID := testcore.RandomizeStr(s.T().Name() + "-handler")
	const handlerWorkflowType = "nexus-signal-with-start-handler"
	nexusHandler := nexustest.Handler{
		OnStartOperation: func(
			ctx context.Context,
			service string,
			operation string,
			input *nexus.LazyValue,
			options nexus.StartOperationOptions,
		) (nexus.HandlerStartOperationResult[any], error) {
			var args signalOperationArgs
			if err := input.Consume(&args); err != nil {
				return nil, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "invalid signal with start arguments: %v", err)
			}
			resp, err := env.FrontendClient().SignalWithStartWorkflowExecution(ctx, &workflowservice.SignalWithStartWorkflowExecutionRequest{
				Namespace:    env.Namespace().String(),
				WorkflowId:   args.HandlerWorkflowID,
				WorkflowType: &commonpb.WorkflowType{Name: handlerWorkflowType},
				TaskQueue:    &taskqueuepb.TaskQueue{Name: taskQueue},
				SignalName:   args.SignalName,
				RequestId:    args.RequestID,
				Links:        commonnexus.ConvertNexusLinksToProtoLinks(options.Links, log.NewNoopLogger()),
			})
			if err != nil {
				// Note: The handler could choose to return a non-retryable error instead.
				return nil, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeInternal, "signal with start failed: %v", err)
			}
			workflowEventLink := resp.GetSignalLink().GetWorkflowEvent()
			if workflowEventLink == nil {
				return nil, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeInternal, "signal with start response did not contain a workflow event link")
			}
			nexus.AddHandlerLinks(ctx, commonnexus.ConvertLinkWorkflowEventToNexusLink(workflowEventLink))
			return &nexus.HandlerStartOperationResultSync[any]{Value: resp.GetRunId()}, nil
		},
	}
	endpointName := env.createRandomExternalNexusServer(ctx, s.T(), nexusHandler)
	w := worker.New(env.SdkClient(), taskQueue, worker.Options{})
	w.RegisterWorkflowWithOptions(signalHandlerWorkflow, workflow.RegisterOptions{Name: handlerWorkflowType})
	w.RegisterWorkflow(signalCallerWorkflow)
	s.NoError(w.Start())
	defer w.Stop()

	handlerRun, err := env.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: handlerWorkflowID, TaskQueue: taskQueue}, handlerWorkflowType)
	s.NoError(err)
	args := signalOperationArgs{HandlerWorkflowID: handlerWorkflowID, SignalName: nexusSignalName, RequestID: uuid.NewString()}

	s.RunSequential("Signals on running workflows complete", func(s *NexusWorkflowTestSuite) {
		callerRun := s.startCaller(ctx, env, taskQueue, endpointName, "signal-with-start", args)
		var handlerRunID string
		s.NoError(callerRun.Get(ctx, &handlerRunID))
		s.Equal(handlerRun.GetRunID(), handlerRunID)
		s.assertSignalLinks(ctx, env, chasmEnabled, callerRun, handlerRun, args.RequestID)
	})
	s.RunSequential("Signals de-dup on requestID", func(s *NexusWorkflowTestSuite) {
		callerRun := s.startCaller(ctx, env, taskQueue, endpointName, "signal-with-start", args)
		var handlerRunID string
		s.NoError(callerRun.Get(ctx, &handlerRunID))
		s.Equal(handlerRun.GetRunID(), handlerRunID)
		s.assertSignalForwardLink(ctx, env, callerRun, handlerRun, args.RequestID)
		s.handlerSignalEvent(env, handlerRun, args.RequestID)
	})
	s.RunSequential("Workflow reset preserves signal with links", func(s *NexusWorkflowTestSuite) {
		preResetLinks := s.handlerSignalEvent(env, handlerRun, args.RequestID).GetLinks()
		handlerRun = s.resetHandlerWorkflowAfterSignal(ctx, env, handlerRun, args.RequestID)
		signaledEvent := s.handlerSignalEvent(env, handlerRun, args.RequestID)
		protorequire.ProtoSliceEqual(s.T(), preResetLinks, signaledEvent.GetLinks())
		s.assertHandlerSignalRequestIDResolves(ctx, env, chasmEnabled, handlerRun, args.RequestID, signaledEvent)
	})
	s.RunSequential("Completes handler workflow", func(s *NexusWorkflowTestSuite) {
		s.NoError(env.SdkClient().SignalWorkflow(ctx, handlerRun.GetID(), handlerRun.GetRunID(), finishSignalName, nil))
		s.NoError(handlerRun.Get(ctx, nil))
	})
	s.RunSequential("Signals to completed workflows restart handlers", func(s *NexusWorkflowTestSuite) {
		callerRun := s.startCaller(ctx, env, taskQueue, endpointName, "signal-with-start", args)
		var replacementRunID string
		s.NoError(callerRun.Get(ctx, &replacementRunID))
		s.NotEqual(handlerRun.GetRunID(), replacementRunID)
		replacementRun := env.SdkClient().GetWorkflow(ctx, handlerRun.GetID(), replacementRunID)
		signaledEvent := s.assertSignalLinks(ctx, env, chasmEnabled, callerRun, replacementRun, args.RequestID)
		handlerHistory := env.GetHistory(env.Namespace().String(), &commonpb.WorkflowExecution{WorkflowId: replacementRun.GetID(), RunId: replacementRun.GetRunID()})
		startedEvent := s.RequireHistoryEvent(handlerHistory, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED)
		s.Require().Len(startedEvent.GetLinks(), 1)
		protorequire.ProtoEqual(s.T(), signaledEvent.GetLinks()[0].GetWorkflowEvent(), startedEvent.GetLinks()[0].GetWorkflowEvent())
		s.NoError(env.SdkClient().SignalWorkflow(ctx, replacementRun.GetID(), replacementRun.GetRunID(), finishSignalName, nil))
		s.NoError(replacementRun.Get(ctx, nil))
	})
	// Intentionally skip unknown workflow/run IDs and REJECT_DUPLICATE. Although frontend rejects these
	// requests, the Nexus handler could choose to retry- maybe until a fixed version of the handler is deployed.
}

func (s *NexusWorkflowTestSuite) startCaller(
	ctx context.Context,
	env *NexusTestEnv,
	taskQueue string,
	endpointName string,
	operation string,
	args signalOperationArgs,
) client.WorkflowRun {
	s.T().Helper()
	callerRun, err := env.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{TaskQueue: taskQueue}, signalCallerWorkflow, endpointName, operation, args)
	s.NoError(err)
	return callerRun
}

func (s *NexusWorkflowTestSuite) assertSignalLinks(
	ctx context.Context,
	env *NexusTestEnv,
	chasmEnabled bool,
	callerRun client.WorkflowRun,
	handlerRun client.WorkflowRun,
	signalRequestID string,
) *historypb.HistoryEvent {
	s.T().Helper()
	s.assertSignalForwardLink(ctx, env, callerRun, handlerRun, signalRequestID)
	callerHistory := env.GetHistory(env.Namespace().String(), &commonpb.WorkflowExecution{WorkflowId: callerRun.GetID(), RunId: callerRun.GetRunID()})
	scheduledEvent := s.RequireHistoryEvent(callerHistory, enumspb.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED)
	signaledEvent := s.handlerSignalEvent(env, handlerRun, signalRequestID)
	s.assertSignalBackwardLink(env, callerRun, scheduledEvent, signaledEvent)
	s.assertHandlerSignalRequestIDResolves(ctx, env, chasmEnabled, handlerRun, signalRequestID, signaledEvent)
	return signaledEvent
}

func (s *NexusWorkflowTestSuite) assertSignalBackwardLink(env *NexusTestEnv, callerRun client.WorkflowRun, scheduledEvent, signaledEvent *historypb.HistoryEvent) {
	s.T().Helper()
	backwardLink := &commonpb.Link_WorkflowEvent{
		Namespace:  env.Namespace().String(),
		WorkflowId: callerRun.GetID(),
		RunId:      callerRun.GetRunID(),
		Reference: &commonpb.Link_WorkflowEvent_EventRef{
			EventRef: &commonpb.Link_WorkflowEvent_EventReference{
				EventId:   scheduledEvent.GetEventId(),
				EventType: enumspb.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED,
			},
		},
	}
	s.Require().Len(signaledEvent.GetLinks(), 1)
	protorequire.ProtoEqual(s.T(), backwardLink, signaledEvent.GetLinks()[0].GetWorkflowEvent())
}

func (s *NexusWorkflowTestSuite) assertHandlerSignalRequestIDResolves(
	ctx context.Context,
	env *NexusTestEnv,
	chasmEnabled bool,
	handlerRun client.WorkflowRun,
	signalRequestID string,
	signaledEvent *historypb.HistoryEvent,
) {
	s.T().Helper()
	if !chasmEnabled {
		// DescribeWorkflow exposes signal request-ID resolution only for CHASM.
		return
	}
	descResp, err := env.FrontendClient().DescribeWorkflowExecution(
		ctx,
		&workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: env.Namespace().String(),
			Execution: &commonpb.WorkflowExecution{WorkflowId: handlerRun.GetID(), RunId: handlerRun.GetRunID()},
		},
	)
	s.NoError(err)
	requestInfo := descResp.GetWorkflowExtendedInfo().GetRequestIdInfos()[signalRequestID]
	s.NotNil(requestInfo)
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED, requestInfo.GetEventType())
	s.Equal(signaledEvent.GetEventId(), requestInfo.GetEventId())
}

func (s *NexusWorkflowTestSuite) assertSignalForwardLink(ctx context.Context, env *NexusTestEnv, callerRun, handlerRun client.WorkflowRun, signalRequestID string) {
	s.T().Helper()
	s.NoError(callerRun.Get(ctx, nil))
	callerHistory := env.GetHistory(env.Namespace().String(), &commonpb.WorkflowExecution{WorkflowId: callerRun.GetID(), RunId: callerRun.GetRunID()})
	completedEvent := s.RequireHistoryEvent(callerHistory, enumspb.EVENT_TYPE_NEXUS_OPERATION_COMPLETED)
	s.Require().Len(completedEvent.GetLinks(), 1)
	protorequire.ProtoEqual(s.T(), &commonpb.Link_WorkflowEvent{
		Namespace:  env.Namespace().String(),
		WorkflowId: handlerRun.GetID(),
		RunId:      handlerRun.GetRunID(),
		Reference: &commonpb.Link_WorkflowEvent_RequestIdRef{
			RequestIdRef: &commonpb.Link_WorkflowEvent_RequestIdReference{
				RequestId: signalRequestID,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
			},
		},
	}, completedEvent.GetLinks()[0].GetWorkflowEvent())
}

func (s *NexusWorkflowTestSuite) handlerSignalEvent(env *NexusTestEnv, handlerRun client.WorkflowRun, signalRequestID string) *historypb.HistoryEvent {
	s.T().Helper()
	handlerHistory := env.GetHistory(env.Namespace().String(), &commonpb.WorkflowExecution{WorkflowId: handlerRun.GetID(), RunId: handlerRun.GetRunID()})
	var signaledEvent *historypb.HistoryEvent
	for _, event := range handlerHistory {
		if event.GetWorkflowExecutionSignaledEventAttributes().GetRequestId() == signalRequestID {
			s.Require().Nil(signaledEvent, "expected exactly one signal event for request ID %q", signalRequestID)
			signaledEvent = event
		}
	}
	s.Require().NotNil(signaledEvent, "no signal event for request ID %q", signalRequestID)
	return signaledEvent
}

func (s *NexusWorkflowTestSuite) resetHandlerWorkflowAfterSignal(
	ctx context.Context,
	env *NexusTestEnv,
	handlerRun client.WorkflowRun,
	signalRequestID string,
) client.WorkflowRun {
	s.T().Helper()
	var workflowTaskCompletedEventID int64
	// Get the first completed event after the signal.
	s.Await(func(s *NexusWorkflowTestSuite) {
		handlerHistory := env.GetHistory(env.Namespace().String(), &commonpb.WorkflowExecution{WorkflowId: handlerRun.GetID(), RunId: handlerRun.GetRunID()})
		signalReqEventFound := false
		for _, event := range handlerHistory {
			if event.GetWorkflowExecutionSignaledEventAttributes().GetRequestId() == signalRequestID {
				signalReqEventFound = true
			} else if signalReqEventFound && event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED {
				workflowTaskCompletedEventID = event.GetEventId()
				break
			}
		}
		s.Positive(workflowTaskCompletedEventID, "expected WorkflowTaskCompleted after signal")
	}, 5*time.Second, 100*time.Millisecond)
	resetResp, err := env.FrontendClient().ResetWorkflowExecution(ctx, &workflowservice.ResetWorkflowExecutionRequest{
		Namespace:                 env.Namespace().String(),
		WorkflowExecution:         &commonpb.WorkflowExecution{WorkflowId: handlerRun.GetID(), RunId: handlerRun.GetRunID()},
		Reason:                    "reset after Nexus signal",
		RequestId:                 uuid.NewString(),
		WorkflowTaskFinishEventId: workflowTaskCompletedEventID,
	})
	s.NoError(err)
	s.NotEmpty(resetResp.GetRunId())
	return env.SdkClient().GetWorkflow(ctx, handlerRun.GetID(), resetResp.GetRunId())
}

func signalHandlerWorkflow(ctx workflow.Context) error {
	workflow.GetSignalChannel(ctx, nexusSignalName).Receive(ctx, nil)
	workflow.GetSignalChannel(ctx, finishSignalName).Receive(ctx, nil)
	return nil
}

func signalCallerWorkflow(ctx workflow.Context, endpointName, operation string, args signalOperationArgs) (string, error) {
	var result string
	err := workflow.NewNexusClient(endpointName, "service").
		ExecuteOperation(ctx, operation, args, workflow.NexusOperationOptions{}).
		Get(ctx, &result)
	return result, err
}
