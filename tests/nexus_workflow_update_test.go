package tests

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	updatepb "go.temporal.io/api/update/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/chasm/lib/callback"
	"go.temporal.io/server/chasm/lib/nexusoperation"
	chasmworkflow "go.temporal.io/server/chasm/lib/workflow"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/callbacks"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexustest"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

type NexusWorkflowUpdateTestSuite struct {
	parallelsuite.Suite[*NexusWorkflowUpdateTestSuite]
}

func TestNexusWorkflowUpdateTestSuiteWorkflowCaller(t *testing.T) {
	parallelsuite.Run(t, &NexusWorkflowUpdateTestSuite{}, false)
}

func TestNexusWorkflowUpdateTestSuiteStandaloneCaller(t *testing.T) {
	parallelsuite.Run(t, &NexusWorkflowUpdateTestSuite{}, true)
}

// updateNexusTestConfig holds configuration for workflow update + nexus integration tests.
type updateNexusTestConfig struct {
	taskQueue string
	childWfID string
	updateID  string
	// helper to assert on the expected event type for repeated update cases
	nextExpectedEventType func() enumspb.EventType
}

// newUpdateNexusTestConfig creates a config with names from the test vars of env.
func newUpdateNexusTestConfig(env *NexusTestEnv) updateNexusTestConfig {
	return updateNexusTestConfig{
		taskQueue: env.Tv().TaskQueue().Name,
		childWfID: env.Tv().WorkflowID(),
		updateID:  "update-id",
	}
}

// makeUpdateWithCallbackHandler creates a nexus handler that sends a workflow update with
// completion callbacks to the specified child workflow. onStart is an optional callback
// invoked at the start of each operation (e.g. for counting invocations).
// If the update is already completed (e.g., the workflow has finished), the handler returns
// the result synchronously instead of starting an async operation with callbacks.
func makeUpdateWithCallbackHandler(
	env *NexusTestEnv,
	t *testing.T,
	cfg updateNexusTestConfig,
	onStart func(),
) nexustest.Handler {
	if cfg.nextExpectedEventType == nil {
		// by default, always return an accepted event for verifications.
		cfg.nextExpectedEventType = func() enumspb.EventType {
			return enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED
		}
	}
	return nexustest.Handler{
		OnStartOperation: func(
			ctx context.Context,
			service, operation string,
			input *nexus.LazyValue,
			options nexus.StartOperationOptions,
		) (nexus.HandlerStartOperationResult[any], error) {
			if onStart != nil {
				onStart()
			}
			// Same as the SDK: the caller links go on the update request and on the callback.
			links := commonnexus.ConvertNexusLinksToProtoLinks(options.Links, log.NewNoopLogger())
			resp, err := env.FrontendClient().UpdateWorkflowExecution(
				ctx,
				&workflowservice.UpdateWorkflowExecutionRequest{
					Namespace: env.Namespace().String(),
					WorkflowExecution: &commonpb.WorkflowExecution{
						WorkflowId: cfg.childWfID,
					},
					WaitPolicy: &updatepb.WaitPolicy{
						LifecycleStage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ACCEPTED,
					},
					Request: &updatepb.Request{
						Meta: &updatepb.Meta{
							UpdateId: cfg.updateID,
						},
						Input: &updatepb.Input{
							Name: "update",
							Args: &commonpb.Payloads{
								Payloads: []*commonpb.Payload{testcore.MustToPayload(t, "test")},
							},
						},
						RequestId: uuid.NewString(),
						CompletionCallbacks: []*commonpb.Callback{
							{
								Variant: &commonpb.Callback_Nexus_{
									Nexus: &commonpb.Callback_Nexus{
										Url:    options.CallbackURL,
										Header: options.CallbackHeader,
									},
								},
								Links: links,
							},
						},
						Links: links,
					},
				},
			)
			if err != nil {
				return nil, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeInternal, "update call failed: %v", err)
			}
			// Verify the response contains a link.
			link := resp.GetLink()
			require.NotNil(t, link, "update response should contain a link")
			if workflowEvent := link.GetWorkflowEvent(); workflowEvent != nil {
				require.Equal(t, cfg.childWfID, workflowEvent.GetWorkflowId())
				if workflowEvent.GetRequestIdRef() != nil {
					// Accepted update: link points to either the accepted event or the options updated event.
					require.Equal(t, cfg.nextExpectedEventType(), workflowEvent.GetRequestIdRef().GetEventType())
				} else {
					// Completed update: link points to the accepted event.
					require.Equal(t, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED, workflowEvent.GetEventRef().GetEventType())
				}
				nexus.AddHandlerLinks(ctx, commonnexus.ConvertLinkWorkflowEventToNexusLink(workflowEvent))
			} else if wfLink := link.GetWorkflow(); wfLink != nil {
				// Rejected update: link points to the workflow with a reason.
				require.Equal(t, cfg.childWfID, wfLink.GetWorkflowId())
				require.Equal(t, "Update rejected", wfLink.GetReason())
			} else {
				require.Fail(t, "link should be a workflow event or workflow link")
			}
			// If the update is already completed, return the result synchronously.
			if outcome := resp.GetOutcome(); outcome != nil {
				if failure := outcome.GetFailure(); failure != nil {
					return nil, &nexus.OperationError{
						State:   nexus.OperationStateFailed,
						Message: failure.GetMessage(),
					}
				}
				if success := outcome.GetSuccess(); success != nil && len(success.GetPayloads()) > 0 {
					var result string
					if jsonErr := json.Unmarshal(success.GetPayloads()[0].GetData(), &result); jsonErr == nil {
						return &nexus.HandlerStartOperationResultSync[any]{Value: result}, nil
					}
				}
			}
			return &nexus.HandlerStartOperationResultAsync{
				OperationToken: "test",
			}, nil
		},
	}
}

func enableUpdateCallbacksOpts() []testcore.TestOption {
	return []testcore.TestOption{
		testcore.WithDynamicConfig(dynamicconfig.EnableChasm, true),
		testcore.WithDynamicConfig(dynamicconfig.EnableCHASMCallbacks, true),
		testcore.WithDynamicConfig(dynamicconfig.EnableWorkflowUpdateCallbacks, true),
		testcore.WithDynamicConfig(nexusoperation.Enabled, true),
		testcore.WithDynamicConfig(
			callback.AllowedAddresses,
			[]any{map[string]any{"Pattern": "*", "AllowInsecure": true}},
		),
	}
}

// newUpdateChildWorkflow returns a child workflow function that registers an "update"
// handler and waits for a "stop" signal. If blockOnSignal is true, the update handler
// blocks on a "complete-update" signal before returning, which is useful for ensuring
// the update goes through the async path.
func newUpdateChildWorkflow(blockOnSignal bool) func(workflow.Context, string) (string, error) {
	return func(ctx workflow.Context, input string) (string, error) {
		if err := workflow.SetUpdateHandler(ctx, "update", func(ctx workflow.Context, input string) (string, error) {
			if blockOnSignal {
				signalCh := workflow.GetSignalChannel(ctx, "complete-update")
				signalCh.Receive(ctx, nil)
			}
			return "updated: " + input, nil
		}); err != nil {
			return "", err
		}
		signalCh := workflow.GetSignalChannel(ctx, "stop")
		signalCh.Receive(ctx, nil)
		return "done: " + input, nil
	}
}

// getFirstWFTaskCompleteEventID scans the workflow history and returns the event ID
// of the first WorkflowTaskCompleted event.
func (s *NexusWorkflowUpdateTestSuite) getFirstWFTaskCompleteEventID(env *NexusTestEnv, workflowID, runID string) int64 {
	event := s.findHistoryEvent(env, workflowID, runID, enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED)
	s.NotNil(event, "couldn't find a WorkflowTaskCompleted event: workflowID=%s runID=%s", workflowID, runID)
	return event.GetEventId()
}

// updateNexusCaller starts Nexus operations that target a workflow update. The caller is a
// workflow, or a standalone Nexus operation when standalone is true.
type updateNexusCaller struct {
	env           *NexusTestEnv
	standalone    bool
	endpointName  string
	taskQueue     string
	sdkClient     client.Client
	dataConverter converter.DataConverter
}

// updateNexusOperation is one Nexus operation started by an updateNexusCaller.
// workflowRun is set for a workflow caller. operationID and runID are set for a standalone caller.
type updateNexusOperation struct {
	workflowRun client.WorkflowRun
	operationID string
	runID       string
}

// updateNexusCallerWorkflow executes one Nexus operation and returns its result.
// The "cancel-op" signal cancels the operation.
func updateNexusCallerWorkflow(ctx workflow.Context, endpointName, targetWorkflowID string) (string, error) {
	opCtx, cancelOp := workflow.WithCancel(ctx)
	workflow.Go(ctx, func(ctx workflow.Context) {
		workflow.GetSignalChannel(ctx, "cancel-op").Receive(ctx, nil)
		cancelOp()
	})
	fut := workflow.NewNexusClient(endpointName, "test").
		ExecuteOperation(opCtx, "operation", targetWorkflowID, workflow.NexusOperationOptions{})
	var result string
	err := fut.Get(ctx, &result)
	return result, err
}

// newUpdateNexusCaller returns a caller that uses the default SDK client of env.
func (s *NexusWorkflowUpdateTestSuite) newUpdateNexusCaller(
	env *NexusTestEnv,
	standalone bool,
	endpointName string,
	taskQueue string,
) *updateNexusCaller {
	return s.newUpdateNexusCallerWithClient(env, standalone, endpointName, taskQueue, env.SdkClient(), converter.GetDefaultDataConverter())
}

// newUpdateNexusCallerWithClient returns a caller that uses sdkClient. A workflow caller runs on
// a worker of sdkClient. A standalone caller decodes its result with dataConverter.
func (s *NexusWorkflowUpdateTestSuite) newUpdateNexusCallerWithClient(
	env *NexusTestEnv,
	standalone bool,
	endpointName string,
	taskQueue string,
	sdkClient client.Client,
	dataConverter converter.DataConverter,
) *updateNexusCaller {
	if !standalone {
		w := worker.New(sdkClient, taskQueue, worker.Options{})
		w.RegisterWorkflow(updateNexusCallerWorkflow)
		s.NoError(w.Start())
		s.T().Cleanup(w.Stop)
	}
	return &updateNexusCaller{
		env:           env,
		standalone:    standalone,
		endpointName:  endpointName,
		taskQueue:     taskQueue,
		sdkClient:     sdkClient,
		dataConverter: dataConverter,
	}
}

// startOperation starts a Nexus operation that sends an update to targetWorkflowID.
func (s *NexusWorkflowUpdateTestSuite) startOperation(
	caller *updateNexusCaller,
	targetWorkflowID string,
) updateNexusOperation {
	if caller.standalone {
		operationID := caller.env.Tv().Any().String()
		resp, err := caller.env.startNexusOperation(s.Context(), &workflowservice.StartNexusOperationExecutionRequest{
			OperationId:            operationID,
			Endpoint:               caller.endpointName,
			Service:                "test",
			Operation:              "operation",
			Input:                  testcore.MustToPayload(s.T(), targetWorkflowID),
			RequestId:              uuid.NewString(),
			ScheduleToCloseTimeout: durationpb.New(30 * time.Second),
		})
		s.NoError(err)
		s.True(resp.GetStarted())
		return updateNexusOperation{operationID: operationID, runID: resp.GetRunId()}
	}
	run, err := caller.sdkClient.ExecuteWorkflow(s.Context(), client.StartWorkflowOptions{
		TaskQueue:                caller.taskQueue,
		WorkflowExecutionTimeout: 30 * time.Second,
	}, updateNexusCallerWorkflow, caller.endpointName, targetWorkflowID)
	s.NoError(err)
	return updateNexusOperation{workflowRun: run}
}

// getOperationResult waits for the operation to close and decodes its result into valuePtr.
// On failure it returns the operation failure: the NexusOperationError for a workflow caller, or
// the converted failure for a standalone caller.
func (s *NexusWorkflowUpdateTestSuite) getOperationResult(
	caller *updateNexusCaller,
	op updateNexusOperation,
	valuePtr any,
) error {
	if !caller.standalone {
		err := op.workflowRun.Get(s.Context(), valuePtr)
		if noe, ok := errors.AsType[*temporal.NexusOperationError](err); ok {
			return noe
		}
		return err
	}
	var resp *workflowservice.PollNexusOperationExecutionResponse
	// The long poll returns an empty response when it times out before the operation closes.
	s.Await(func(s *NexusWorkflowUpdateTestSuite) {
		var err error
		resp, err = caller.env.FrontendClient().PollNexusOperationExecution(s.Context(), &workflowservice.PollNexusOperationExecutionRequest{
			Namespace:   caller.env.Namespace().String(),
			OperationId: op.operationID,
			RunId:       op.runID,
			WaitStage:   enumspb.NEXUS_OPERATION_WAIT_STAGE_CLOSED,
		})
		s.NoError(err)
		s.Equal(enumspb.NEXUS_OPERATION_WAIT_STAGE_CLOSED, resp.GetWaitStage(), "operation not closed")
	}, 30*time.Second, 100*time.Millisecond)
	if failure := resp.GetFailure(); failure != nil {
		return temporal.GetDefaultFailureConverter().FailureToError(failure)
	}
	s.NoError(caller.dataConverter.FromPayload(resp.GetResult(), valuePtr))
	return nil
}

// awaitOperationStarted waits until the caller records that the operation started.
func (s *NexusWorkflowUpdateTestSuite) awaitOperationStarted(caller *updateNexusCaller, op updateNexusOperation) {
	if caller.standalone {
		// The long poll returns an empty response when it times out before the operation starts.
		// A started or closed operation reports a stage.
		s.Await(func(s *NexusWorkflowUpdateTestSuite) {
			resp, err := caller.env.FrontendClient().PollNexusOperationExecution(s.Context(), &workflowservice.PollNexusOperationExecutionRequest{
				Namespace:   caller.env.Namespace().String(),
				OperationId: op.operationID,
				RunId:       op.runID,
				WaitStage:   enumspb.NEXUS_OPERATION_WAIT_STAGE_STARTED,
			})
			s.NoError(err)
			s.NotEqual(enumspb.NEXUS_OPERATION_WAIT_STAGE_UNSPECIFIED, resp.GetWaitStage(), "operation not started")
		}, 10*time.Second, 100*time.Millisecond)
		return
	}
	s.Await(func(s *NexusWorkflowUpdateTestSuite) {
		s.NotNil(s.findHistoryEvent(caller.env, op.workflowRun.GetID(), op.workflowRun.GetRunID(), enumspb.EVENT_TYPE_NEXUS_OPERATION_STARTED))
	}, 10*time.Second, 200*time.Millisecond)
}

// cancelOperation requests cancellation of the operation.
func (s *NexusWorkflowUpdateTestSuite) cancelOperation(caller *updateNexusCaller, op updateNexusOperation) {
	if caller.standalone {
		_, err := caller.env.FrontendClient().RequestCancelNexusOperationExecution(s.Context(), &workflowservice.RequestCancelNexusOperationExecutionRequest{
			Namespace:   caller.env.Namespace().String(),
			OperationId: op.operationID,
			RunId:       op.runID,
			RequestId:   uuid.NewString(),
		})
		s.NoError(err)
		return
	}
	s.NoError(caller.sdkClient.SignalWorkflow(s.Context(), op.workflowRun.GetID(), op.workflowRun.GetRunID(), "cancel-op", nil))
}

// findHistoryEvent returns the first event of eventType in the workflow history, or nil.
func (s *NexusWorkflowUpdateTestSuite) findHistoryEvent(
	env *NexusTestEnv,
	workflowID, runID string,
	eventType enumspb.EventType,
) *historypb.HistoryEvent {
	hist := env.SdkClient().GetWorkflowHistory(s.Context(), workflowID, runID, false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	for hist.HasNext() {
		event, err := hist.Next()
		s.NoError(err)
		if event.GetEventType() == eventType {
			return event
		}
	}
	return nil
}

// expectedCallerLink returns the link that points back to the caller of op.
func (s *NexusWorkflowUpdateTestSuite) expectedCallerLink(caller *updateNexusCaller, op updateNexusOperation) *commonpb.Link {
	namespace := caller.env.Namespace().String()
	if caller.standalone {
		return &commonpb.Link{
			Variant: &commonpb.Link_NexusOperation_{
				NexusOperation: &commonpb.Link_NexusOperation{
					Namespace:   namespace,
					OperationId: op.operationID,
					RunId:       op.runID,
				},
			},
		}
	}
	scheduledEvent := s.findHistoryEvent(caller.env, op.workflowRun.GetID(), op.workflowRun.GetRunID(), enumspb.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED)
	s.NotNil(scheduledEvent)
	return &commonpb.Link{
		Variant: &commonpb.Link_WorkflowEvent_{
			WorkflowEvent: &commonpb.Link_WorkflowEvent{
				Namespace:  namespace,
				WorkflowId: op.workflowRun.GetID(),
				RunId:      op.workflowRun.GetRunID(),
				Reference: &commonpb.Link_WorkflowEvent_EventRef{
					EventRef: &commonpb.Link_WorkflowEvent_EventReference{
						EventId:   scheduledEvent.GetEventId(),
						EventType: enumspb.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED,
					},
				},
			},
		},
	}
}

// operationLinks returns the links the caller recorded for the operation. A workflow caller records
// them on the NexusOperationStarted event for an async operation, or on the NexusOperationCompleted
// event for a sync operation.
func (s *NexusWorkflowUpdateTestSuite) operationLinks(caller *updateNexusCaller, op updateNexusOperation) []*commonpb.Link {
	if caller.standalone {
		return caller.env.describeNexusOperation(s.Context(), s.T(), op.operationID).GetInfo().GetLinks()
	}
	workflowID, runID := op.workflowRun.GetID(), op.workflowRun.GetRunID()
	event := s.findHistoryEvent(caller.env, workflowID, runID, enumspb.EVENT_TYPE_NEXUS_OPERATION_STARTED)
	if event == nil {
		// A sync operation has no NexusOperationStarted event. Its links are on the NexusOperationCompleted event.
		event = s.findHistoryEvent(caller.env, workflowID, runID, enumspb.EVENT_TYPE_NEXUS_OPERATION_COMPLETED)
	}
	s.NotNil(event, "no NexusOperationStarted or NexusOperationCompleted event in the caller workflow")
	return event.GetLinks()
}

// updateLinkRef is the reference in a caller link to an update.
type updateLinkRef int

const (
	// updateLinkRefAccepted is a request ID reference to the UpdateAccepted event. The server
	// returns it when the request sends the update.
	updateLinkRefAccepted updateLinkRef = iota
	// updateLinkRefOptionsUpdated is a request ID reference to the OptionsUpdated event. The server
	// returns it when the request attaches a callback to an update that is in-flight.
	updateLinkRefOptionsUpdated
	// updateLinkRefAcceptedEventID is an event ID reference to the UpdateAccepted event. The server
	// returns it when the update already completed. The target run records no event for the request.
	updateLinkRefAcceptedEventID
)

// assertCallerLinkToUpdate verifies that the caller has one link to the update on the target run,
// and that the link resolves to the event that want names.
func (s *NexusWorkflowUpdateTestSuite) assertCallerLinkToUpdate(
	caller *updateNexusCaller,
	op updateNexusOperation,
	targetWorkflowID, targetRunID string,
	want updateLinkRef,
) {
	env := caller.env
	operationLinks := s.operationLinks(caller, op)
	s.Require().Len(operationLinks, 1)
	targetLink := operationLinks[0].GetWorkflowEvent()
	s.NotNil(targetLink)
	s.Equal(env.Namespace().String(), targetLink.GetNamespace())
	s.Equal(targetWorkflowID, targetLink.GetWorkflowId())
	s.Equal(targetRunID, targetLink.GetRunId())

	if want == updateLinkRefAcceptedEventID {
		eventRef := targetLink.GetEventRef()
		s.NotNil(eventRef, "link must reference the UpdateAccepted event by event ID: %v", targetLink)
		s.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED, eventRef.GetEventType())
		acceptedEvent := s.findHistoryEvent(env, targetWorkflowID, targetRunID, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED)
		s.NotNil(acceptedEvent)
		s.Equal(acceptedEvent.GetEventId(), eventRef.GetEventId())
		return
	}

	wantEventType := enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED
	if want == updateLinkRefOptionsUpdated {
		wantEventType = enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED
	}
	requestIDRef := targetLink.GetRequestIdRef()
	s.NotNil(requestIDRef, "link must reference the update by request ID: %v", targetLink)
	s.Equal(wantEventType, requestIDRef.GetEventType())

	desc, err := env.FrontendClient().DescribeWorkflowExecution(s.Context(), &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: targetWorkflowID, RunId: targetRunID},
	})
	s.NoError(err)
	requestIDInfo, ok := desc.GetWorkflowExtendedInfo().GetRequestIdInfos()[requestIDRef.GetRequestId()]
	s.True(ok, "request ID in the caller link must resolve on the target workflow")
	s.Equal(wantEventType, requestIDInfo.GetEventType())
}

// assertOptionsUpdatedCallbackLink verifies that the run has one OptionsUpdated event that attaches a
// callback of the caller to updateID. The attached callback has a link back to the caller.
func (s *NexusWorkflowUpdateTestSuite) assertOptionsUpdatedCallbackLink(
	caller *updateNexusCaller,
	op updateNexusOperation,
	targetWorkflowID, targetRunID, updateID string,
) {
	wantCallerLink := s.expectedCallerLink(caller, op)
	hist := caller.env.SdkClient().GetWorkflowHistory(s.Context(), targetWorkflowID, targetRunID, false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	var attached *historypb.WorkflowExecutionOptionsUpdatedEventAttributes_WorkflowUpdateOptionsUpdate
	for hist.HasNext() {
		event, err := hist.Next()
		s.NoError(err)
		for _, updateOptions := range event.GetWorkflowExecutionOptionsUpdatedEventAttributes().GetWorkflowUpdateOptions() {
			for _, cb := range updateOptions.GetAttachedCompletionCallbacks() {
				links := cb.GetLinks()
				if len(links) == 1 && proto.Equal(wantCallerLink, links[0]) {
					s.Nil(attached, "more than one OptionsUpdated event attaches a callback of the caller")
					attached = updateOptions
				}
			}
		}
	}
	s.NotNil(attached, "no OptionsUpdated event attaches a callback of the caller")
	s.Equal(updateID, attached.GetUpdateId())
	s.Len(attached.GetAttachedCompletionCallbacks(), 1)
}

// assertUpdateAcceptedCallbackLink verifies that the UpdateAccepted event on the run has a
// callback with a link back to the caller.
func (s *NexusWorkflowUpdateTestSuite) assertUpdateAcceptedCallbackLink(
	caller *updateNexusCaller,
	op updateNexusOperation,
	targetWorkflowID, targetRunID string,
) {
	wantCallerLink := s.expectedCallerLink(caller, op)
	acceptedEvent := s.findHistoryEvent(caller.env, targetWorkflowID, targetRunID, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED)
	s.NotNil(acceptedEvent)
	acceptedCallbacks := acceptedEvent.GetWorkflowExecutionUpdateAcceptedEventAttributes().GetAcceptedRequest().GetCompletionCallbacks()
	s.Require().Len(acceptedCallbacks, 1)
	protorequire.ProtoSliceEqual(s.T(), []*commonpb.Link{wantCallerLink}, acceptedCallbacks[0].GetLinks())
}

// assertUpdateCallbackDelivered waits until the run has an update callback with a link back to
// the caller, and the callback is delivered.
func (s *NexusWorkflowUpdateTestSuite) assertUpdateCallbackDelivered(
	caller *updateNexusCaller,
	op updateNexusOperation,
	targetWorkflowID, targetRunID, updateID string,
) {
	env := caller.env
	wantCallerLink := s.expectedCallerLink(caller, op)
	s.Await(func(s *NexusWorkflowUpdateTestSuite) {
		desc, err := env.FrontendClient().DescribeWorkflowExecution(s.Context(), &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: env.Namespace().String(),
			Execution: &commonpb.WorkflowExecution{WorkflowId: targetWorkflowID, RunId: targetRunID},
		})
		s.NoError(err)
		var cb *workflowpb.CallbackInfo
		for _, c := range desc.GetCallbacks() {
			links := c.GetCallback().GetLinks()
			if len(links) == 1 && proto.Equal(wantCallerLink, links[0]) {
				s.Nil(cb, "more than one callback links to the caller")
				cb = c
			}
		}
		s.NotNil(cb, "no callback links to the caller: %v", desc.GetCallbacks())
		s.Equal(updateID, cb.GetTrigger().GetUpdateWorkflowExecutionCompleted().GetUpdateId())
		s.Equal(enumspb.CALLBACK_STATE_SUCCEEDED, cb.GetState(), "callback: %v", cb)
	}, 10*time.Second, 200*time.Millisecond)
}

// assertUpdateLinksAndCallback verifies the links in both directions and the update callback:
//   - The caller has a link to the UpdateAccepted event of the target workflow.
//   - The update callback on the target workflow has a link back to the caller.
//   - The update callback is delivered.
func (s *NexusWorkflowUpdateTestSuite) assertUpdateLinksAndCallback(
	caller *updateNexusCaller,
	op updateNexusOperation,
	targetWorkflowID, targetRunID, updateID string,
) {
	s.assertCallerLinkToUpdate(caller, op, targetWorkflowID, targetRunID, updateLinkRefAccepted)
	s.assertUpdateAcceptedCallbackLink(caller, op, targetWorkflowID, targetRunID)
	s.assertUpdateCallbackDelivered(caller, op, targetWorkflowID, targetRunID, updateID)
}

// awaitUpdateAccepted polls the workflow history until a WorkflowExecutionUpdateAccepted
// event is found, failing the test if it does not appear within 10 seconds.
func (s *NexusWorkflowUpdateTestSuite) awaitUpdateAccepted(env *NexusTestEnv, workflowID, runID string) {
	s.Await(func(s *NexusWorkflowUpdateTestSuite) {
		s.NotNil(s.findHistoryEvent(env, workflowID, runID, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED), "update not yet accepted")
	}, 10*time.Second, 500*time.Millisecond)
}

// startWorker creates a worker on the given task queue, registers wf, starts it,
// and schedules cleanup.
func (s *NexusWorkflowUpdateTestSuite) startWorker(env *NexusTestEnv, taskQueue string, wf any) {
	w := worker.New(env.SdkClient(), taskQueue, worker.Options{})
	w.RegisterWorkflow(wf)
	s.NoError(w.Start())
	s.T().Cleanup(w.Stop)
}

// assertAcceptedUpdateCompletedWorkflowError asserts that the operation failure err contains
// ApplicationError{Type: "AcceptedUpdateCompletedWorkflow"}. The callback sends this failure when the
// workflow closes before the update completes.
func (s *NexusWorkflowUpdateTestSuite) assertAcceptedUpdateCompletedWorkflowError(err error) {
	var appErr *temporal.ApplicationError
	s.ErrorAs(err, &appErr)
	s.Equal("AcceptedUpdateCompletedWorkflow", appErr.Type())
	s.Contains(appErr.Error(), "completed before the Update completed")
}

// assertReappliedUpdateInNewRun verifies that updateID appears as an UpdateAdmitted event
// in runID's history with the completion callback preserved. The callback keeps its link back
// to the caller of op.
func (s *NexusWorkflowUpdateTestSuite) assertReappliedUpdateInNewRun(
	caller *updateNexusCaller,
	op updateNexusOperation,
	workflowID, runID, updateID string,
) {
	wantCallerLink := s.expectedCallerLink(caller, op)
	hist := caller.env.SdkClient().GetWorkflowHistory(s.Context(), workflowID, runID, false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	found := false
	for hist.HasNext() {
		event, err := hist.Next()
		s.NoError(err)
		if event.EventType == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ADMITTED {
			attrs := event.GetWorkflowExecutionUpdateAdmittedEventAttributes()
			if attrs.GetRequest().GetMeta().GetUpdateId() == updateID {
				found = true
				admittedCallbacks := attrs.GetRequest().GetCompletionCallbacks()
				s.Require().Len(admittedCallbacks, 1, "reapplied update should preserve completion callbacks")
				protorequire.ProtoSliceEqual(s.T(), []*commonpb.Link{wantCallerLink}, admittedCallbacks[0].GetLinks())
			}
		}
	}
	s.True(found, "expected reapplied UpdateAdmitted event in new run")
}

func (s *NexusWorkflowUpdateTestSuite) TestWorkflowUpdateAsyncNexusOperation(standalone bool) {
	env := newNexusTestEnv(s.T(), true, enableUpdateCallbacksOpts()...)
	ctx := s.Context()
	cfg := newUpdateNexusTestConfig(env)

	capture := env.StartNamespaceMetricCapture()

	h := makeUpdateWithCallbackHandler(env, s.T(), cfg, nil)
	endpointName := env.createRandomExternalNexusServer(ctx, s.T(), h)

	targetTaskQueue := env.Tv().WithTaskQueueNumber(1).TaskQueue().Name
	// The update blocks until the operation starts. This makes the operation async.
	targetWF := newUpdateChildWorkflow(true)
	s.startWorker(env, targetTaskQueue, targetWF)
	targetRun, err := env.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        cfg.childWfID,
		TaskQueue: targetTaskQueue,
	}, targetWF, "initial input")
	s.NoError(err)

	caller := s.newUpdateNexusCaller(env, standalone, endpointName, cfg.taskQueue)
	op := s.startOperation(caller, cfg.childWfID)
	s.awaitOperationStarted(caller, op)
	s.NoError(env.SdkClient().SignalWorkflow(ctx, cfg.childWfID, targetRun.GetRunID(), "complete-update", nil))
	var result string
	s.NoError(s.getOperationResult(caller, op, &result))
	s.Equal("updated: test", result)

	// Verify the target workflow's history contains the update accepted event with callbacks.
	acceptedEvent := s.findHistoryEvent(env, cfg.childWfID, targetRun.GetRunID(), enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED)
	s.NotNil(acceptedEvent, "expected to find WorkflowExecutionUpdateAccepted event in target workflow history")
	attrs := acceptedEvent.GetWorkflowExecutionUpdateAcceptedEventAttributes()
	s.Equal(cfg.updateID, attrs.GetAcceptedRequest().GetMeta().GetUpdateId())
	s.NotEmpty(attrs.GetAcceptedRequest().GetCompletionCallbacks())

	s.assertUpdateLinksAndCallback(caller, op, cfg.childWfID, targetRun.GetRunID(), cfg.updateID)

	requireNexusCompletionSource(s.T(), capture, "workflow.update")

	s.NoError(env.SdkClient().SignalWorkflow(ctx, cfg.childWfID, targetRun.GetRunID(), "stop", nil))
}

// TestWorkflowUpdateAsyncAttachedNexusOperation verifies that a second operation for the same
// update attaches to the in-flight update, and both operations get the update result.
func (s *NexusWorkflowUpdateTestSuite) TestWorkflowUpdateAsyncAttachedNexusOperation(standalone bool) {
	env := newNexusTestEnv(s.T(), true, enableUpdateCallbacksOpts()...)
	ctx := s.Context()
	cfg := newUpdateNexusTestConfig(env)

	var operationCount atomic.Int32
	cfg.nextExpectedEventType = func() enumspb.EventType {
		if operationCount.Load() > 1 { // for all duplicates
			return enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED
		}
		return enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED
	}

	h := makeUpdateWithCallbackHandler(env, s.T(), cfg, func() { operationCount.Add(1) })
	endpointName := env.createRandomExternalNexusServer(ctx, s.T(), h)

	targetTaskQueue := env.Tv().WithTaskQueueNumber(1).TaskQueue().Name
	targetWF := newUpdateChildWorkflow(true)
	s.startWorker(env, targetTaskQueue, targetWF)
	targetRun, err := env.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        cfg.childWfID,
		TaskQueue: targetTaskQueue,
	}, targetWF, "initial input")
	s.NoError(err)

	caller := s.newUpdateNexusCaller(env, standalone, endpointName, cfg.taskQueue)
	op1 := s.startOperation(caller, cfg.childWfID)
	s.awaitOperationStarted(caller, op1)
	// The second operation attaches to the update that is already in-flight.
	op2 := s.startOperation(caller, cfg.childWfID)
	s.awaitOperationStarted(caller, op2)

	// Complete the update now that both operations are attached.
	s.NoError(env.SdkClient().SignalWorkflow(ctx, cfg.childWfID, targetRun.GetRunID(), "complete-update", nil))

	var result1, result2 string
	s.NoError(s.getOperationResult(caller, op1, &result1))
	s.NoError(s.getOperationResult(caller, op2, &result2))
	s.Equal("updated: test", result1)
	s.Equal("updated: test", result2)

	// op1 sent the update and op2 attached to it. Each has its own callback.
	s.assertUpdateLinksAndCallback(caller, op1, cfg.childWfID, targetRun.GetRunID(), cfg.updateID)
	// The attach of op2 writes an OptionsUpdated event. The caller link of op2 points to that event, and
	// the callback that the event attaches links back to op2.
	s.assertCallerLinkToUpdate(caller, op2, cfg.childWfID, targetRun.GetRunID(), updateLinkRefOptionsUpdated)
	s.assertOptionsUpdatedCallbackLink(caller, op2, cfg.childWfID, targetRun.GetRunID(), cfg.updateID)
	s.assertUpdateCallbackDelivered(caller, op2, cfg.childWfID, targetRun.GetRunID(), cfg.updateID)

	s.NoError(env.SdkClient().SignalWorkflow(ctx, cfg.childWfID, targetRun.GetRunID(), "stop", nil))
}

// TestWorkflowUpdateNoCallbackAttachedOnAlreadyCompletedUpdate verifies that when a second caller
// sends an update request with the same update ID after the update has already completed,
// the second request returns the result synchronously without attaching a new callback.
// The target workflow should only have one update callback (from the first request).
func (s *NexusWorkflowUpdateTestSuite) TestWorkflowUpdateNoCallbackAttachedOnAlreadyCompletedUpdate(standalone bool) {
	env := newNexusTestEnv(s.T(), true, enableUpdateCallbacksOpts()...)
	ctx := s.Context()
	cfg := newUpdateNexusTestConfig(env)
	cfg.updateID = "already-completed-update-id"

	var operationCount atomic.Int32
	h := makeUpdateWithCallbackHandler(env, s.T(), cfg, func() { operationCount.Add(1) })
	endpointName := env.createRandomExternalNexusServer(ctx, s.T(), h)

	targetTaskQueue := env.Tv().WithTaskQueueNumber(1).TaskQueue().Name
	// The update blocks until the first operation starts. This makes the first operation async.
	targetWF := newUpdateChildWorkflow(true)
	s.startWorker(env, targetTaskQueue, targetWF)
	targetRun, err := env.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        cfg.childWfID,
		TaskQueue: targetTaskQueue,
	}, targetWF, "initial input")
	s.NoError(err)

	caller := s.newUpdateNexusCaller(env, standalone, endpointName, cfg.taskQueue)

	// The first operation sends the update.
	op1 := s.startOperation(caller, cfg.childWfID)
	s.awaitOperationStarted(caller, op1)
	s.NoError(env.SdkClient().SignalWorkflow(ctx, cfg.childWfID, targetRun.GetRunID(), "complete-update", nil))
	var result1 string
	s.NoError(s.getOperationResult(caller, op1, &result1))
	s.Equal("updated: test", result1)

	// The second operation targets the same update after it completed.
	op2 := s.startOperation(caller, cfg.childWfID)
	var result2 string
	s.NoError(s.getOperationResult(caller, op2, &result2))
	s.Equal("updated: test", result2)
	s.Equal(int32(2), operationCount.Load(), "expected two nexus operations to be started")

	// Verify the target workflow has exactly one update callback (from the first request).
	// The second request returns synchronously because the update is already completed,
	// so no additional callback is attached.
	descResp, err := env.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: cfg.childWfID,
		},
	})
	s.NoError(err)
	updateCallbackCount := 0
	for _, cb := range descResp.GetCallbacks() {
		if cb.GetTrigger().GetUpdateWorkflowExecutionCompleted() != nil {
			updateCallbackCount++
		}
	}
	s.Equal(1, updateCallbackCount, "expected exactly one update callback on the target workflow")

	// Verify the target workflow has the correct request ID infos.
	// Each nexus operation generates a unique request ID. If the second operation
	// (targeting the already-completed update) had attached its request ID, we would
	// see 3 entries instead of 2, or an OPTIONS_UPDATED entry. The count of 2 with
	// only STARTED and UPDATE_ACCEPTED types proves the second request ID was not attached.
	sdkDescResp, err := env.SdkClient().DescribeWorkflowExecution(ctx, cfg.childWfID, "")
	s.NoError(err)
	requestIDInfos := sdkDescResp.GetWorkflowExtendedInfo().GetRequestIdInfos()
	s.NotNil(requestIDInfos)
	s.Len(requestIDInfos, 2, "expected exactly 2 request ID infos: second operation should not attach")
	cntStarted := 0
	cntAccepted := 0
	for _, info := range requestIDInfos {
		s.False(info.Buffered)
		s.GreaterOrEqual(info.EventId, common.FirstEventID)
		s.NotEqual(
			enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED,
			info.EventType,
			"second operation targeting completed update should not create an OPTIONS_UPDATED request ID",
		)
		switch info.EventType {
		case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED:
			cntStarted++
		case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED:
			cntAccepted++
		default:
			s.Failf("unexpected event type in request ID info", "got %v", info.EventType)
		}
	}
	s.Equal(1, cntStarted, "expected one STARTED request ID info")
	s.Equal(1, cntAccepted, "expected one UPDATE_ACCEPTED request ID info from first update acceptance")

	s.assertUpdateLinksAndCallback(caller, op1, cfg.childWfID, targetRun.GetRunID(), cfg.updateID)
	// op2 completed sync. Its link points to the UpdateAccepted event by event ID.
	s.assertCallerLinkToUpdate(caller, op2, cfg.childWfID, targetRun.GetRunID(), updateLinkRefAcceptedEventID)

	s.NoError(env.SdkClient().SignalWorkflow(ctx, cfg.childWfID, targetRun.GetRunID(), "stop", nil))
}

// TestDescribeWorkflowShowsUpdateCallbacks verifies that DescribeWorkflowExecution
// returns update-level callbacks after an update with callbacks is sent.
func (s *NexusWorkflowUpdateTestSuite) TestDescribeWorkflowShowsUpdateCallbacks(standalone bool) {
	if standalone {
		s.T().Skip("test has no Nexus caller")
	}
	env := newNexusTestEnv(s.T(), true, enableUpdateCallbacksOpts()...)
	ctx := s.Context()
	taskQueue := env.Tv().TaskQueue().Name
	updateID := "describe-callback-update-id"
	callbackURL := "http://localhost:9999/callback"

	wf := func(ctx workflow.Context) (string, error) {
		if err := workflow.SetUpdateHandler(ctx, "update", func(ctx workflow.Context, input string) (string, error) {
			// Wait for a signal so update stays in-progress while we describe.
			signalCh := workflow.GetSignalChannel(ctx, "complete-update")
			signalCh.Receive(ctx, nil)
			return "updated: " + input, nil
		}); err != nil {
			return "", err
		}
		signalCh := workflow.GetSignalChannel(ctx, "stop")
		signalCh.Receive(ctx, nil)
		return "done", nil
	}

	s.startWorker(env, taskQueue, wf)

	run, err := env.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		TaskQueue: taskQueue,
	}, wf)
	s.NoError(err)

	// Send update with completion callbacks (don't wait for completion).
	testPayload := testcore.MustToPayload(s.T(), "test")
	updateDone := make(chan struct{})
	go func() {
		defer close(updateDone)
		_, _ = env.FrontendClient().UpdateWorkflowExecution(ctx, &workflowservice.UpdateWorkflowExecutionRequest{
			Namespace: env.Namespace().String(),
			WorkflowExecution: &commonpb.WorkflowExecution{
				WorkflowId: run.GetID(),
				RunId:      run.GetRunID(),
			},
			WaitPolicy: &updatepb.WaitPolicy{
				LifecycleStage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED,
			},
			Request: &updatepb.Request{
				Meta: &updatepb.Meta{
					UpdateId: updateID,
				},
				Input: &updatepb.Input{
					Name: "update",
					Args: &commonpb.Payloads{
						Payloads: []*commonpb.Payload{testPayload},
					},
				},
				RequestId: uuid.NewString(),
				CompletionCallbacks: []*commonpb.Callback{
					{
						Variant: &commonpb.Callback_Nexus_{
							Nexus: &commonpb.Callback_Nexus{
								Url: callbackURL,
							},
						},
					},
				},
			},
		})
	}()

	// Wait until the update is accepted by checking DescribeWorkflowExecution.
	s.Await(func(s *NexusWorkflowUpdateTestSuite) {
		desc, err := env.SdkClient().DescribeWorkflowExecution(s.Context(), run.GetID(), run.GetRunID())
		s.NoError(err)
		s.NotNil(desc.GetCallbacks(), "callbacks should be present")
		found := false
		for _, cb := range desc.GetCallbacks() {
			if cb.GetCallback().GetNexus().GetUrl() == callbackURL {
				found = true
				// Verify the trigger references the update.
				trigger := cb.GetTrigger()
				s.NotNil(trigger)
				updateTrigger := trigger.GetUpdateWorkflowExecutionCompleted()
				if updateTrigger != nil {
					s.Equal(updateID, updateTrigger.GetUpdateId())
				}
			}
		}
		s.True(found, "expected to find callback with URL %s", callbackURL)
	}, 10*time.Second, 500*time.Millisecond)

	// Complete the update and stop the workflow.
	s.NoError(env.SdkClient().SignalWorkflow(ctx, run.GetID(), run.GetRunID(), "complete-update", nil))
	<-updateDone
	s.NoError(env.SdkClient().SignalWorkflow(ctx, run.GetID(), run.GetRunID(), "stop", nil))
}

// TestWorkflowUpdateCallbackAfterResetInflightUpdate verifies that when a workflow is
// reset while an update with completion callbacks is in-flight (accepted but not completed),
// the update is reapplied in the new run and the callback fires when the update completes.
func (s *NexusWorkflowUpdateTestSuite) TestWorkflowUpdateCallbackAfterResetInflightUpdate(standalone bool) {
	env := newNexusTestEnv(s.T(), true, enableUpdateCallbacksOpts()...)
	ctx := s.Context()
	cfg := newUpdateNexusTestConfig(env)

	h := makeUpdateWithCallbackHandler(env, s.T(), cfg, nil)
	endpointName := env.createRandomExternalNexusServer(ctx, s.T(), h)

	targetTaskQueue := env.Tv().WithTaskQueueNumber(1).TaskQueue().Name

	// Target workflow: update handler blocks on "complete-update" signal so the update
	// stays in-flight while we perform the reset.
	targetWF := func(ctx workflow.Context, input string) (string, error) {
		if err := workflow.SetUpdateHandler(ctx, "update", func(ctx workflow.Context, input string) (string, error) {
			signalCh := workflow.GetSignalChannel(ctx, "complete-update")
			signalCh.Receive(ctx, nil)
			return "updated: " + input, nil
		}); err != nil {
			return "", err
		}
		signalCh := workflow.GetSignalChannel(ctx, "stop")
		signalCh.Receive(ctx, nil)
		return "done: " + input, nil
	}

	// Start target workflow independently (not as child) to avoid parent-child
	// complications during reset.
	s.startWorker(env, targetTaskQueue, targetWF)

	targetRun, err := env.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        cfg.childWfID,
		TaskQueue: targetTaskQueue,
	}, targetWF, "initial input")
	s.NoError(err)

	caller := s.newUpdateNexusCaller(env, standalone, endpointName, cfg.taskQueue)
	op := s.startOperation(caller, cfg.childWfID)

	// Wait for the update to be accepted on the target workflow.
	s.awaitUpdateAccepted(env, cfg.childWfID, targetRun.GetRunID())
	s.awaitOperationStarted(caller, op)

	// Reset the target workflow to the first WFT completed event (before the update).
	resetResp, err := env.FrontendClient().ResetWorkflowExecution(ctx, &workflowservice.ResetWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: cfg.childWfID,
			RunId:      targetRun.GetRunID(),
		},
		Reason:                    "test reset with inflight update",
		RequestId:                 uuid.NewString(),
		WorkflowTaskFinishEventId: s.getFirstWFTaskCompleteEventID(env, cfg.childWfID, targetRun.GetRunID()),
	})
	s.NoError(err)

	// Verify the update was reapplied in the new run's history.
	s.assertReappliedUpdateInNewRun(caller, op, cfg.childWfID, resetResp.RunId, cfg.updateID)

	// Signal the new run to complete the update, which should trigger the callback.
	s.NoError(env.SdkClient().SignalWorkflow(ctx, cfg.childWfID, resetResp.RunId, "complete-update", nil))

	// The callback fires -> nexus operation completes with the result.
	var result string
	s.NoError(s.getOperationResult(caller, op, &result))
	s.Equal("updated: test", result)

	// The caller links to the update in the first run. The new run delivers the callback.
	s.assertCallerLinkToUpdate(caller, op, cfg.childWfID, targetRun.GetRunID(), updateLinkRefAccepted)
	s.assertUpdateAcceptedCallbackLink(caller, op, cfg.childWfID, targetRun.GetRunID())
	s.assertUpdateCallbackDelivered(caller, op, cfg.childWfID, resetResp.RunId, cfg.updateID)

	// Clean up: stop the new run of the target workflow.
	s.NoError(env.SdkClient().SignalWorkflow(ctx, cfg.childWfID, resetResp.RunId, "stop", nil))
}

// TestWorkflowUpdateCallbackAfterResetRejectedUpdate verifies that when a workflow is
// reset while an update with completion callbacks is in-flight (accepted but not completed),
// and the new run's workflow code rejects the reapplied update via a validator, the
// completion callback fires with a failure and the caller's nexus operation fails.
func (s *NexusWorkflowUpdateTestSuite) TestWorkflowUpdateCallbackAfterResetRejectedUpdate(standalone bool) {
	env := newNexusTestEnv(s.T(), true, enableUpdateCallbacksOpts()...)
	ctx := s.Context()
	cfg := newUpdateNexusTestConfig(env)

	h := makeUpdateWithCallbackHandler(env, s.T(), cfg, nil)
	endpointName := env.createRandomExternalNexusServer(ctx, s.T(), h)

	targetTaskQueue := env.Tv().WithTaskQueueNumber(1).TaskQueue().Name

	// Use a shared flag to switch behavior between runs. In the first run the
	// update is accepted (and blocks); after we flip the flag the validator
	// rejects every update.
	var shouldReject atomic.Bool

	// Single workflow function used for both runs.
	targetWF := func(ctx workflow.Context, input string) (string, error) {
		err := workflow.SetUpdateHandlerWithOptions(ctx, "update",
			func(ctx workflow.Context, input string) (string, error) {
				signalCh := workflow.GetSignalChannel(ctx, "complete-update")
				signalCh.Receive(ctx, nil)
				return "updated: " + input, nil
			},
			workflow.UpdateHandlerOptions{
				Validator: func(ctx workflow.Context, input string) error {
					if shouldReject.Load() {
						return errors.New("update rejected after reset")
					}
					return nil
				},
			},
		)
		if err != nil {
			return "", err
		}
		signalCh := workflow.GetSignalChannel(ctx, "stop")
		signalCh.Receive(ctx, nil)
		return "done: " + input, nil
	}

	s.startWorker(env, targetTaskQueue, targetWF)

	targetRun, err := env.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        cfg.childWfID,
		TaskQueue: targetTaskQueue,
	}, targetWF, "initial input")
	s.NoError(err)

	caller := s.newUpdateNexusCaller(env, standalone, endpointName, cfg.taskQueue)
	op := s.startOperation(caller, cfg.childWfID)

	// Wait for the update to be accepted on the target workflow.
	s.awaitUpdateAccepted(env, cfg.childWfID, targetRun.GetRunID())
	s.awaitOperationStarted(caller, op)

	// Flip the flag so the validator rejects updates in the new run.
	shouldReject.Store(true)

	// Reset the target workflow to the first WFT completed event (before the update).
	resetResp, err := env.FrontendClient().ResetWorkflowExecution(ctx, &workflowservice.ResetWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: cfg.childWfID,
			RunId:      targetRun.GetRunID(),
		},
		Reason:                    "test reset with inflight update expecting rejection",
		RequestId:                 uuid.NewString(),
		WorkflowTaskFinishEventId: s.getFirstWFTaskCompleteEventID(env, cfg.childWfID, targetRun.GetRunID()),
	})
	s.NoError(err)

	// Verify the update was reapplied in the new run's history.
	s.assertReappliedUpdateInNewRun(caller, op, cfg.childWfID, resetResp.RunId, cfg.updateID)

	// The reapplied update is rejected by the validator -> callback fires with failure ->
	// nexus operation fails.
	var result string
	err = s.getOperationResult(caller, op, &result)
	s.Error(err, "expected the operation to fail because the reapplied update was rejected")

	s.ErrorContains(err, "update rejected after reset")

	// The caller links to the update in the first run. The new run rejects the reapplied update and
	// delivers the callback with the failure.
	s.assertCallerLinkToUpdate(caller, op, cfg.childWfID, targetRun.GetRunID(), updateLinkRefAccepted)
	s.assertUpdateAcceptedCallbackLink(caller, op, cfg.childWfID, targetRun.GetRunID())
	s.assertUpdateCallbackDelivered(caller, op, cfg.childWfID, resetResp.RunId, cfg.updateID)

	// Clean up: stop the new run of the target workflow.
	s.NoError(env.SdkClient().SignalWorkflow(ctx, cfg.childWfID, resetResp.RunId, "stop", nil))
}

// TestWorkflowUpdateCallbackAfterResetCompletedUpdate verifies that when a workflow is
// reset after an update with callbacks has already completed, the update is reapplied in
// the new run, completes again, and a new nexus operation targeting the same update ID
// receives the result via the AttachCallbacks path.
func (s *NexusWorkflowUpdateTestSuite) TestWorkflowUpdateCallbackAfterResetCompletedUpdate(standalone bool) {
	env := newNexusTestEnv(s.T(), true, enableUpdateCallbacksOpts()...)
	ctx := s.Context()
	cfg := newUpdateNexusTestConfig(env)
	cfg.updateID = "reset-completed-update-id"

	var operationCount atomic.Int32
	h := makeUpdateWithCallbackHandler(env, s.T(), cfg, func() { operationCount.Add(1) })
	endpointName := env.createRandomExternalNexusServer(ctx, s.T(), h)

	targetTaskQueue := env.Tv().WithTaskQueueNumber(1).TaskQueue().Name

	// Target workflow: the update blocks until the first operation starts. This makes the first
	// operation async.
	targetWF := newUpdateChildWorkflow(true)

	s.startWorker(env, targetTaskQueue, targetWF)

	targetRun, err := env.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        cfg.childWfID,
		TaskQueue: targetTaskQueue,
	}, targetWF, "initial input")
	s.NoError(err)

	caller := s.newUpdateNexusCaller(env, standalone, endpointName, cfg.taskQueue)

	// First operation: triggers the update, it completes, callback fires.
	op1 := s.startOperation(caller, cfg.childWfID)
	s.awaitOperationStarted(caller, op1)
	s.NoError(env.SdkClient().SignalWorkflow(ctx, cfg.childWfID, targetRun.GetRunID(), "complete-update", nil))
	var result1 string
	s.NoError(s.getOperationResult(caller, op1, &result1))
	s.Equal("updated: test", result1)

	// Reset the target workflow to before the update.
	resetResp, err := env.FrontendClient().ResetWorkflowExecution(ctx, &workflowservice.ResetWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: cfg.childWfID,
			RunId:      targetRun.GetRunID(),
		},
		Reason:                    "test reset with completed update",
		RequestId:                 uuid.NewString(),
		WorkflowTaskFinishEventId: s.getFirstWFTaskCompleteEventID(env, cfg.childWfID, targetRun.GetRunID()),
	})
	s.NoError(err)

	// The update is reapplied in the new run. Complete it there.
	s.NoError(env.SdkClient().SignalWorkflow(ctx, cfg.childWfID, resetResp.RunId, "complete-update", nil))
	// The update is reapplied and completes again in the new run.
	// Wait for the update to complete in the new run before sending the second operation.
	s.Await(func(s *NexusWorkflowUpdateTestSuite) {
		s.NotNil(s.findHistoryEvent(env, cfg.childWfID, resetResp.RunId, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_COMPLETED), "update not yet completed in new run")
	}, 10*time.Second, 500*time.Millisecond)

	// Second operation: targets the same update ID.
	// Since the update is already completed in the new run, AttachCallbacks fires the callback.
	op2 := s.startOperation(caller, cfg.childWfID)
	var result2 string
	s.NoError(s.getOperationResult(caller, op2, &result2))
	s.Equal("updated: test", result2)

	s.Equal(int32(2), operationCount.Load(), "expected two nexus operations to be started")

	s.assertUpdateLinksAndCallback(caller, op1, cfg.childWfID, targetRun.GetRunID(), cfg.updateID)
	// op2 completed sync on the new run. Its link points to the UpdateAccepted event of the new run by
	// event ID.
	s.assertCallerLinkToUpdate(caller, op2, cfg.childWfID, resetResp.RunId, updateLinkRefAcceptedEventID)

	// Clean up: stop the new run of the target workflow.
	s.NoError(env.SdkClient().SignalWorkflow(ctx, cfg.childWfID, resetResp.RunId, "stop", nil))
}

// TestWorkflowUpdateSyncReturnForCompletedWorkflow verifies that when a second nexus
// operation targets the same update ID on a workflow that has already completed, the
// handler detects the update is already completed and returns the result synchronously
// (instead of starting an async operation with callbacks).
func (s *NexusWorkflowUpdateTestSuite) TestWorkflowUpdateSyncReturnForCompletedWorkflow(standalone bool) {
	env := newNexusTestEnv(s.T(), true, enableUpdateCallbacksOpts()...)
	ctx := s.Context()
	cfg := newUpdateNexusTestConfig(env)
	cfg.updateID = "sync-return-completed-wf-update-id"

	var operationCount atomic.Int32
	h := makeUpdateWithCallbackHandler(env, s.T(), cfg, func() { operationCount.Add(1) })
	endpointName := env.createRandomExternalNexusServer(ctx, s.T(), h)

	targetTaskQueue := env.Tv().WithTaskQueueNumber(1).TaskQueue().Name

	// Target workflow: the update blocks until the first operation starts. This makes the first
	// operation async.
	targetWF := newUpdateChildWorkflow(true)

	s.startWorker(env, targetTaskQueue, targetWF)

	targetRun, err := env.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        cfg.childWfID,
		TaskQueue: targetTaskQueue,
	}, targetWF, "initial input")
	s.NoError(err)

	caller := s.newUpdateNexusCaller(env, standalone, endpointName, cfg.taskQueue)

	// First operation: triggers the update, it completes, callback fires.
	op1 := s.startOperation(caller, cfg.childWfID)
	s.awaitOperationStarted(caller, op1)
	s.NoError(env.SdkClient().SignalWorkflow(ctx, cfg.childWfID, targetRun.GetRunID(), "complete-update", nil))
	var result1 string
	s.NoError(s.getOperationResult(caller, op1, &result1))
	s.Equal("updated: test", result1)

	// Complete the target workflow by sending the "stop" signal.
	s.NoError(env.SdkClient().SignalWorkflow(ctx, cfg.childWfID, targetRun.GetRunID(), "stop", nil))

	// Wait for the target workflow to complete.
	var targetResult string
	s.NoError(targetRun.Get(ctx, &targetResult))

	// Second operation: targets the same update ID.
	// Since the workflow is completed and the update was already completed,
	// UpdateWorkflowExecution returns the outcome directly -> handler returns sync.
	op2 := s.startOperation(caller, cfg.childWfID)
	var result2 string
	s.NoError(s.getOperationResult(caller, op2, &result2))
	s.Equal("updated: test", result2)

	s.Equal(int32(2), operationCount.Load(), "expected two nexus operations to be started")

	s.assertUpdateLinksAndCallback(caller, op1, cfg.childWfID, targetRun.GetRunID(), cfg.updateID)
	// op2 completed sync on the closed run. Its link points to the UpdateAccepted event by event ID.
	s.assertCallerLinkToUpdate(caller, op2, cfg.childWfID, targetRun.GetRunID(), updateLinkRefAcceptedEventID)
}

// TestWorkflowUpdateCallbackOnFailedUpdate verifies that when an update handler returns
// an error (update completes with a failure outcome), the completion callback fires and
// the caller's nexus operation completes with a failure.
func (s *NexusWorkflowUpdateTestSuite) TestWorkflowUpdateCallbackOnFailedUpdate(standalone bool) {
	env := newNexusTestEnv(s.T(), true, enableUpdateCallbacksOpts()...)
	ctx := s.Context()
	cfg := newUpdateNexusTestConfig(env)
	cfg.updateID = "failed-update-id"

	h := makeUpdateWithCallbackHandler(env, s.T(), cfg, nil)
	endpointName := env.createRandomExternalNexusServer(ctx, s.T(), h)

	targetTaskQueue := env.Tv().WithTaskQueueNumber(1).TaskQueue().Name

	// Target workflow: update handler returns an error after acceptance.
	targetWF := func(ctx workflow.Context, input string) (string, error) {
		if err := workflow.SetUpdateHandler(ctx, "update", func(ctx workflow.Context, input string) (string, error) {
			workflow.GetSignalChannel(ctx, "fail-update").Receive(ctx, nil)
			return "", temporal.NewApplicationError("update handler failed", "UpdateFailed", nil)
		}); err != nil {
			return "", err
		}
		signalCh := workflow.GetSignalChannel(ctx, "stop")
		signalCh.Receive(ctx, nil)
		return "done: " + input, nil
	}

	s.startWorker(env, targetTaskQueue, targetWF)

	targetRun, err := env.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        cfg.childWfID,
		TaskQueue: targetTaskQueue,
	}, targetWF, "initial input")
	s.NoError(err)

	caller := s.newUpdateNexusCaller(env, standalone, endpointName, cfg.taskQueue)
	op := s.startOperation(caller, cfg.childWfID)

	s.awaitUpdateAccepted(env, cfg.childWfID, "")
	s.awaitOperationStarted(caller, op)
	s.NoError(env.SdkClient().SignalWorkflow(ctx, cfg.childWfID, "", "fail-update", nil))

	// The update is accepted but the handler returns an error -> update completes with
	// failure -> callback fires -> nexus operation fails.
	var result string
	err = s.getOperationResult(caller, op, &result)
	s.Error(err, "expected the operation to fail because the update failed")

	s.ErrorContains(err, "update handler failed")

	s.assertUpdateLinksAndCallback(caller, op, cfg.childWfID, targetRun.GetRunID(), cfg.updateID)

	// Clean up: stop the target workflow.
	s.NoError(env.SdkClient().SignalWorkflow(ctx, cfg.childWfID, "", "stop", nil))
}

// TestWorkflowUpdateCallbackOnWorkflowTerminate verifies that when a workflow is
// terminated while an update with completion callbacks is in-flight (accepted, handler
// blocking), the ProcessCloseCallbacks mechanism fires the callback and the caller's
// nexus operation completes.
func (s *NexusWorkflowUpdateTestSuite) TestWorkflowUpdateCallbackOnWorkflowTerminate(standalone bool) {
	env := newNexusTestEnv(s.T(), true, enableUpdateCallbacksOpts()...)
	ctx := s.Context()
	cfg := newUpdateNexusTestConfig(env)
	cfg.updateID = "terminate-update-id"

	h := makeUpdateWithCallbackHandler(env, s.T(), cfg, nil)
	endpointName := env.createRandomExternalNexusServer(ctx, s.T(), h)

	targetTaskQueue := env.Tv().WithTaskQueueNumber(1).TaskQueue().Name

	// Target workflow: update handler blocks on a signal so it stays in-flight.
	targetWF := func(ctx workflow.Context, input string) (string, error) {
		if err := workflow.SetUpdateHandler(ctx, "update", func(ctx workflow.Context, input string) (string, error) {
			signalCh := workflow.GetSignalChannel(ctx, "complete-update")
			signalCh.Receive(ctx, nil)
			return "updated: " + input, nil
		}); err != nil {
			return "", err
		}
		signalCh := workflow.GetSignalChannel(ctx, "stop")
		signalCh.Receive(ctx, nil)
		return "done: " + input, nil
	}

	s.startWorker(env, targetTaskQueue, targetWF)

	targetRun, err := env.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        cfg.childWfID,
		TaskQueue: targetTaskQueue,
	}, targetWF, "initial input")
	s.NoError(err)

	caller := s.newUpdateNexusCaller(env, standalone, endpointName, cfg.taskQueue)
	op := s.startOperation(caller, cfg.childWfID)

	// Wait for the update to be accepted on the target.
	s.awaitUpdateAccepted(env, cfg.childWfID, "")
	s.awaitOperationStarted(caller, op)

	// Terminate the target workflow while the update is in-flight.
	// ProcessCloseCallbacks should fire the update-level callbacks.
	s.NoError(env.SdkClient().TerminateWorkflow(ctx, cfg.childWfID, "", "testing terminate with inflight update callback"))

	// The callback fires -> nexus operation completes.
	// The caller should get an error (the nexus operation failed because the
	// target was terminated).
	var result string
	err = s.getOperationResult(caller, op, &result)
	s.Error(err, "expected the operation to fail because the target was terminated")
	s.assertAcceptedUpdateCompletedWorkflowError(err)
	s.assertUpdateLinksAndCallback(caller, op, cfg.childWfID, targetRun.GetRunID(), cfg.updateID)
}

// TestWorkflowUpdateCallbackOnWorkflowCancel verifies that if the target workflow is canceled,
// we will fail an accepted update and its Nexus operation.
func (s *NexusWorkflowUpdateTestSuite) TestWorkflowUpdateCallbackOnWorkflowCancel(standalone bool) {
	env := newNexusTestEnv(s.T(), true, enableUpdateCallbacksOpts()...)
	ctx := s.Context()
	cfg := newUpdateNexusTestConfig(env)
	cfg.updateID = "cancel-update-id"

	h := makeUpdateWithCallbackHandler(env, s.T(), cfg, nil)
	endpointName := env.createRandomExternalNexusServer(ctx, s.T(), h)

	targetTaskQueue := env.Tv().WithTaskQueueNumber(1).TaskQueue().Name

	// Keep the update pending until the workflow is canceled.
	targetWF := func(ctx workflow.Context, input string) (string, error) {
		if err := workflow.SetUpdateHandler(ctx, "update", func(ctx workflow.Context, input string) (string, error) {
			signalCh := workflow.GetSignalChannel(ctx, "complete-update")
			signalCh.Receive(ctx, nil)
			return "updated: " + input, nil
		}); err != nil {
			return "", err
		}
		ctx.Done().Receive(ctx, nil)
		return "", ctx.Err()
	}

	s.startWorker(env, targetTaskQueue, targetWF)

	targetRun, err := env.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        cfg.childWfID,
		TaskQueue: targetTaskQueue,
	}, targetWF, "initial input")
	s.NoError(err)

	caller := s.newUpdateNexusCaller(env, standalone, endpointName, cfg.taskQueue)
	op := s.startOperation(caller, cfg.childWfID)

	s.awaitUpdateAccepted(env, cfg.childWfID, "")
	s.awaitOperationStarted(caller, op)

	s.NoError(env.SdkClient().CancelWorkflow(ctx, cfg.childWfID, ""))

	var result string
	err = s.getOperationResult(caller, op, &result)
	s.Error(err, "expected the operation to fail because the target was canceled")
	s.assertAcceptedUpdateCompletedWorkflowError(err)
	s.assertUpdateLinksAndCallback(caller, op, cfg.childWfID, targetRun.GetRunID(), cfg.updateID)
}

// TestWorkflowUpdateCallbackOnWorkflowComplete verifies that when a workflow completes
// normally while an update with completion callbacks is in-flight (accepted, handler
// blocking), the ProcessCloseCallbacks mechanism fires the callback and the caller's
// nexus operation completes with a failure (the run closes without completing the update).
// This exercises mutable_state_impl.go processCloseCallbacksChasm -> wf.ProcessCloseCallbacks.
func (s *NexusWorkflowUpdateTestSuite) TestWorkflowUpdateCallbackOnWorkflowComplete(standalone bool) {
	env := newNexusTestEnv(s.T(), true, enableUpdateCallbacksOpts()...)
	ctx := s.Context()
	cfg := newUpdateNexusTestConfig(env)
	cfg.updateID = "complete-wf-update-id"

	h := makeUpdateWithCallbackHandler(env, s.T(), cfg, nil)
	endpointName := env.createRandomExternalNexusServer(ctx, s.T(), h)

	targetTaskQueue := env.Tv().WithTaskQueueNumber(1).TaskQueue().Name

	// Update handler blocks on "complete-update" signal so the update stays in-flight
	// while the workflow itself completes via the "stop" signal.
	targetWF := newUpdateChildWorkflow(true)

	s.startWorker(env, targetTaskQueue, targetWF)

	targetRun, err := env.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        cfg.childWfID,
		TaskQueue: targetTaskQueue,
	}, targetWF, "initial input")
	s.NoError(err)

	caller := s.newUpdateNexusCaller(env, standalone, endpointName, cfg.taskQueue)
	op := s.startOperation(caller, cfg.childWfID)

	// Wait for the update to be accepted on the target.
	s.awaitUpdateAccepted(env, cfg.childWfID, "")
	s.awaitOperationStarted(caller, op)

	// Complete the target workflow normally while the update is still in-flight.
	// processCloseCallbacksChasm fires the update-level callbacks on workflow close.
	s.NoError(env.SdkClient().SignalWorkflow(ctx, cfg.childWfID, "", "stop", nil))

	// The callback fires -> nexus operation completes with failure.
	var result string
	err = s.getOperationResult(caller, op, &result)
	s.Error(err, "expected the operation to fail because the target completed while update was in-flight")
	s.assertAcceptedUpdateCompletedWorkflowError(err)
	s.assertUpdateLinksAndCallback(caller, op, cfg.childWfID, targetRun.GetRunID(), cfg.updateID)
}

// TestWorkflowUpdateCallbackOnWorkflowContinueAsNew verifies that when a workflow
// continues-as-new while an update with completion callbacks is in-flight (accepted,
// handler blocking), the update callbacks are fired and the caller's nexus operation
// completes with a failure (the old run is closed).
func (s *NexusWorkflowUpdateTestSuite) TestWorkflowUpdateCallbackOnWorkflowContinueAsNew(standalone bool) {
	env := newNexusTestEnv(s.T(), true, enableUpdateCallbacksOpts()...)
	ctx := s.Context()
	cfg := newUpdateNexusTestConfig(env)
	cfg.updateID = "continue-as-new-update-id"

	h := makeUpdateWithCallbackHandler(env, s.T(), cfg, nil)
	endpointName := env.createRandomExternalNexusServer(ctx, s.T(), h)

	targetTaskQueue := env.Tv().WithTaskQueueNumber(1).TaskQueue().Name

	// Target workflow: update handler blocks on a signal so it stays in-flight.
	// When "continue-as-new" signal is received, the workflow continues as new.
	var targetWF func(ctx workflow.Context, input string) (string, error)
	targetWF = func(ctx workflow.Context, input string) (string, error) {
		if err := workflow.SetUpdateHandler(ctx, "update", func(ctx workflow.Context, input string) (string, error) {
			signalCh := workflow.GetSignalChannel(ctx, "complete-update")
			signalCh.Receive(ctx, nil)
			return "updated: " + input, nil
		}); err != nil {
			return "", err
		}
		signalCh := workflow.GetSignalChannel(ctx, "continue-as-new")
		signalCh.Receive(ctx, nil)
		return "", workflow.NewContinueAsNewError(ctx, targetWF, "continued")
	}

	s.startWorker(env, targetTaskQueue, targetWF)

	targetRun, err := env.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        cfg.childWfID,
		TaskQueue: targetTaskQueue,
	}, targetWF, "initial input")
	s.NoError(err)

	caller := s.newUpdateNexusCaller(env, standalone, endpointName, cfg.taskQueue)
	op := s.startOperation(caller, cfg.childWfID)

	// Wait for the update to be accepted on the target.
	s.awaitUpdateAccepted(env, cfg.childWfID, "")
	s.awaitOperationStarted(caller, op)

	// Signal the target workflow to continue-as-new while the update is in-flight.
	s.NoError(env.SdkClient().SignalWorkflow(ctx, cfg.childWfID, "", "continue-as-new", nil))

	// The callback fires -> nexus operation completes.
	// The caller should get an error (the nexus operation failed because the
	// target continued as new and the update was aborted).
	var result string
	err = s.getOperationResult(caller, op, &result)
	s.Error(err, "expected the operation to fail because the target continued as new")
	s.assertAcceptedUpdateCompletedWorkflowError(err)
	s.assertUpdateLinksAndCallback(caller, op, cfg.childWfID, targetRun.GetRunID(), cfg.updateID)
}

// TestWorkflowUpdateCallbackOnWorkflowFailedWithRetry verifies that when a workflow
// fails with a retry policy (RetryState=IN_PROGRESS) while an update with completion
// callbacks is in-flight (accepted, handler blocking), the update callbacks are fired
// and the caller's nexus operation completes with a failure (the old run is closed).
func (s *NexusWorkflowUpdateTestSuite) TestWorkflowUpdateCallbackOnWorkflowFailedWithRetry(standalone bool) {
	env := newNexusTestEnv(s.T(), true, enableUpdateCallbacksOpts()...)
	ctx := s.Context()
	cfg := newUpdateNexusTestConfig(env)
	cfg.updateID = "failed-retry-update-id"

	h := makeUpdateWithCallbackHandler(env, s.T(), cfg, nil)
	endpointName := env.createRandomExternalNexusServer(ctx, s.T(), h)

	targetTaskQueue := env.Tv().WithTaskQueueNumber(1).TaskQueue().Name

	// Target workflow: update handler blocks on a signal so it stays in-flight.
	// When "fail" signal is received, the workflow returns an error (which will
	// be retried due to the retry policy).
	targetWF := func(ctx workflow.Context, input string) (string, error) {
		if err := workflow.SetUpdateHandler(ctx, "update", func(ctx workflow.Context, input string) (string, error) {
			signalCh := workflow.GetSignalChannel(ctx, "complete-update")
			signalCh.Receive(ctx, nil)
			return "updated: " + input, nil
		}); err != nil {
			return "", err
		}
		signalCh := workflow.GetSignalChannel(ctx, "fail")
		signalCh.Receive(ctx, nil)
		return "", errors.New("intentional failure for retry test")
	}

	s.startWorker(env, targetTaskQueue, targetWF)

	targetRun, err := env.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        cfg.childWfID,
		TaskQueue: targetTaskQueue,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    1 * time.Second,
			MaximumAttempts:    3,
			BackoffCoefficient: 1,
		},
	}, targetWF, "initial input")
	s.NoError(err)

	caller := s.newUpdateNexusCaller(env, standalone, endpointName, cfg.taskQueue)
	op := s.startOperation(caller, cfg.childWfID)

	// Wait for the update to be accepted on the target.
	s.awaitUpdateAccepted(env, cfg.childWfID, "")
	s.awaitOperationStarted(caller, op)

	// Signal the target workflow to fail while the update is in-flight.
	// The retry policy will cause a new run to be created.
	s.NoError(env.SdkClient().SignalWorkflow(ctx, cfg.childWfID, "", "fail", nil))

	// The callback fires -> nexus operation completes.
	// The caller should get an error (the nexus operation failed because the
	// target failed and the update was aborted).
	var result string
	err = s.getOperationResult(caller, op, &result)
	s.Error(err, "expected the operation to fail because the target workflow failed with retry")
	s.assertAcceptedUpdateCompletedWorkflowError(err)
	s.assertUpdateLinksAndCallback(caller, op, cfg.childWfID, targetRun.GetRunID(), cfg.updateID)
}

// TestWorkflowUpdateCallbackOnNexusOperationCancel verifies that a canceled Nexus
// operation can complete once when the update completes.
func (s *NexusWorkflowUpdateTestSuite) TestWorkflowUpdateCallbackOnNexusOperationCancel(standalone bool) {
	env := newNexusTestEnv(s.T(), true, enableUpdateCallbacksOpts()...)
	ctx := s.Context()
	cfg := newUpdateNexusTestConfig(env)
	cfg.updateID = "cancel-op-update-id"

	var cancelReceived atomic.Bool
	h := makeUpdateWithCallbackHandler(env, s.T(), cfg, nil)
	h.OnCancelOperation = func(ctx context.Context, service, operation, token string, options nexus.CancelOperationOptions) error {
		cancelReceived.Store(true)
		return nil
	}

	// Start the target workflow, whose update handler blocks until the "complete-update" signal.
	endpointName := env.createRandomExternalNexusServer(ctx, s.T(), h)
	targetTaskQueue := env.Tv().WithTaskQueueNumber(1).TaskQueue().Name
	targetWF := newUpdateChildWorkflow(true)
	s.startWorker(env, targetTaskQueue, targetWF)
	targetRun, err := env.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        cfg.childWfID,
		TaskQueue: targetTaskQueue,
	}, targetWF, "initial input")
	s.NoError(err)

	// Start the operation, wait for update to be accepted, then cancel the nexus operation.
	caller := s.newUpdateNexusCaller(env, standalone, endpointName, cfg.taskQueue)
	op := s.startOperation(caller, cfg.childWfID)
	s.awaitUpdateAccepted(env, cfg.childWfID, "")
	s.awaitOperationStarted(caller, op)
	s.cancelOperation(caller, op)

	// The cancel request should be delivered to the nexus handler.
	s.AwaitTruef(cancelReceived.Load, 10*time.Second, 100*time.Millisecond, "cancel request was not delivered")

	// Signal the update handler to complete the update, then signal the target workflow to stop.
	s.NoError(env.SdkClient().SignalWorkflow(ctx, cfg.childWfID, "", "complete-update", nil))
	s.NoError(env.SdkClient().SignalWorkflow(ctx, cfg.childWfID, "", "stop", nil))

	// Wait for the operation to close before checking history.
	var callerResult string
	s.NoError(s.getOperationResult(caller, op, &callerResult))

	if !standalone {
		// Caller workflow history must have no WFT failure events.
		callerRun := op.workflowRun
		s.Nil(
			s.findHistoryEvent(env, callerRun.GetID(), callerRun.GetRunID(), enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED),
			"canceling the nexus operation must not crash the caller's workflow task",
		)
	}

	// Target workflow history must show that the update completed, despite the nexus operation getting canceled
	// (since we've already accepted the update).
	s.Await(func(s *NexusWorkflowUpdateTestSuite) {
		targetHist := env.SdkClient().GetWorkflowHistory(s.Context(), cfg.childWfID, "", false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
		completedCount := 0
		for targetHist.HasNext() {
			event, err := targetHist.Next()
			s.NoError(err)
			if event.EventType == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_COMPLETED {
				completedCount++
			}
		}
		s.Equal(1, completedCount, "the update must complete exactly once; canceling the caller's nexus operation must not duplicate delivery")
	}, 10*time.Second, 500*time.Millisecond)

	s.assertUpdateLinksAndCallback(caller, op, cfg.childWfID, targetRun.GetRunID(), cfg.updateID)
}

// TestWorkflowUpdateCallbackCustomDataConverter verifies callback completion with
// encoded update input and output.
func (s *NexusWorkflowUpdateTestSuite) TestWorkflowUpdateCallbackCustomDataConverter(standalone bool) {
	env := newNexusTestEnv(s.T(), true, enableUpdateCallbacksOpts()...)
	ctx := s.Context()
	cfg := newUpdateNexusTestConfig(env)
	cfg.updateID = "custom-converter-update-id"

	dataConverter := converter.NewCodecDataConverter(
		converter.GetDefaultDataConverter(),
		converter.NewZlibCodec(converter.ZlibCodecOptions{AlwaysEncode: true}),
	)

	inputPayload, err := dataConverter.ToPayload("custom-converter-input")
	s.NoError(err)

	h := nexustest.Handler{
		OnStartOperation: func(
			ctx context.Context,
			service, operation string,
			input *nexus.LazyValue,
			options nexus.StartOperationOptions,
		) (nexus.HandlerStartOperationResult[any], error) {
			links := commonnexus.ConvertNexusLinksToProtoLinks(options.Links, log.NewNoopLogger())
			resp, err := env.FrontendClient().UpdateWorkflowExecution(ctx, &workflowservice.UpdateWorkflowExecutionRequest{
				Namespace: env.Namespace().String(),
				WorkflowExecution: &commonpb.WorkflowExecution{
					WorkflowId: cfg.childWfID,
				},
				WaitPolicy: &updatepb.WaitPolicy{
					LifecycleStage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ACCEPTED,
				},
				Request: &updatepb.Request{
					Meta: &updatepb.Meta{UpdateId: cfg.updateID},
					Input: &updatepb.Input{
						Name: "update",
						Args: &commonpb.Payloads{Payloads: []*commonpb.Payload{inputPayload}},
					},
					RequestId: uuid.NewString(),
					CompletionCallbacks: []*commonpb.Callback{
						{
							Variant: &commonpb.Callback_Nexus_{
								Nexus: &commonpb.Callback_Nexus{
									Url:    options.CallbackURL,
									Header: options.CallbackHeader,
								},
							},
							Links: links,
						},
					},
					Links: links,
				},
			})
			if err != nil {
				return nil, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeInternal, "update call failed: %v", err)
			}
			nexus.AddHandlerLinks(ctx, commonnexus.ConvertLinkWorkflowEventToNexusLink(resp.GetLink().GetWorkflowEvent()))
			return &nexus.HandlerStartOperationResultAsync{OperationToken: "test"}, nil
		},
	}
	endpointName := env.createRandomExternalNexusServer(ctx, s.T(), h)

	targetTaskQueue := env.Tv().WithTaskQueueNumber(1).TaskQueue().Name

	targetClient, err := client.Dial(client.Options{
		HostPort:      env.FrontendGRPCAddress(),
		Namespace:     env.Namespace().String(),
		DataConverter: dataConverter,
	})
	s.NoError(err)
	s.T().Cleanup(targetClient.Close)

	targetWF := func(ctx workflow.Context, input string) (string, error) {
		if err := workflow.SetUpdateHandler(ctx, "update", func(ctx workflow.Context, input string) (string, error) {
			workflow.GetSignalChannel(ctx, "complete-update").Receive(ctx, nil)
			return "converted: " + input, nil
		}); err != nil {
			return "", err
		}
		workflow.GetSignalChannel(ctx, "stop").Receive(ctx, nil)
		return "done: " + input, nil
	}

	targetWorker := worker.New(targetClient, targetTaskQueue, worker.Options{})
	targetWorker.RegisterWorkflow(targetWF)
	s.NoError(targetWorker.Start())
	s.T().Cleanup(targetWorker.Stop)

	targetRun, err := targetClient.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        cfg.childWfID,
		TaskQueue: targetTaskQueue,
	}, targetWF, "initial input")
	s.NoError(err)

	callerClient, err := client.Dial(client.Options{
		HostPort:      env.FrontendGRPCAddress(),
		Namespace:     env.Namespace().String(),
		DataConverter: dataConverter,
	})
	s.NoError(err)
	s.T().Cleanup(callerClient.Close)

	caller := s.newUpdateNexusCallerWithClient(env, standalone, endpointName, cfg.taskQueue, callerClient, dataConverter)
	op := s.startOperation(caller, cfg.childWfID)

	s.awaitUpdateAccepted(env, cfg.childWfID, "")
	s.awaitOperationStarted(caller, op)
	s.NoError(env.SdkClient().SignalWorkflow(ctx, cfg.childWfID, "", "complete-update", nil))

	var result string
	s.NoError(s.getOperationResult(caller, op, &result))
	s.Equal("converted: custom-converter-input", result)

	s.assertUpdateLinksAndCallback(caller, op, cfg.childWfID, targetRun.GetRunID(), cfg.updateID)
}

// TestWorkflowUpdateCallbackOnRejectedUpdate verifies that when an update is rejected
// by the workflow's validator, the nexus handler detects the rejection (which is returned
// as a completed update with a failure outcome) and returns a synchronous failure to the
// caller. This tests the proper handling of rejection in the callback flow.
func (s *NexusWorkflowUpdateTestSuite) TestWorkflowUpdateCallbackOnRejectedUpdate(standalone bool) {
	env := newNexusTestEnv(s.T(), true, enableUpdateCallbacksOpts()...)
	ctx := s.Context()
	cfg := newUpdateNexusTestConfig(env)
	cfg.updateID = "rejected-update-id"

	h := makeUpdateWithCallbackHandler(env, s.T(), cfg, nil)
	endpointName := env.createRandomExternalNexusServer(ctx, s.T(), h)

	targetTaskQueue := env.Tv().WithTaskQueueNumber(1).TaskQueue().Name

	// Target workflow: validator rejects all updates.
	targetWF := func(ctx workflow.Context, input string) (string, error) {
		err := workflow.SetUpdateHandlerWithOptions(ctx, "update",
			func(ctx workflow.Context, input string) (string, error) {
				return "updated: " + input, nil
			},
			workflow.UpdateHandlerOptions{
				Validator: func(ctx workflow.Context, input string) error {
					return errors.New("update rejected by validator")
				},
			},
		)
		if err != nil {
			return "", err
		}
		signalCh := workflow.GetSignalChannel(ctx, "stop")
		signalCh.Receive(ctx, nil)
		return "done: " + input, nil
	}

	s.startWorker(env, targetTaskQueue, targetWF)

	targetRun, err := env.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        cfg.childWfID,
		TaskQueue: targetTaskQueue,
	}, targetWF, "initial input")
	s.NoError(err)

	caller := s.newUpdateNexusCaller(env, standalone, endpointName, cfg.taskQueue)
	op := s.startOperation(caller, cfg.childWfID)

	// The update is rejected by the validator -> nexus handler detects rejection and
	// returns sync failure -> nexus operation fails.
	var result string
	err = s.getOperationResult(caller, op, &result)
	s.Error(err, "expected the operation to fail because the update was rejected")

	s.ErrorContains(err, "update rejected by validator")

	// A rejected update does not register a callback on the target workflow.
	desc, err := env.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: cfg.childWfID, RunId: targetRun.GetRunID()},
	})
	s.NoError(err)
	s.Empty(desc.GetCallbacks())

	// Clean up: stop the target workflow.
	s.NoError(env.SdkClient().SignalWorkflow(ctx, cfg.childWfID, "", "stop", nil))
}

// TestWorkflowUpdateRequestIDInAcceptedEvent verifies that when an update request includes
// a RequestId, it is preserved in the WorkflowExecutionUpdateAccepted event's AcceptedRequest.
func (s *NexusWorkflowUpdateTestSuite) TestWorkflowUpdateRequestIDInAcceptedEvent(standalone bool) {
	if standalone {
		s.T().Skip("test has no Nexus caller")
	}
	env := newNexusTestEnv(s.T(), true, enableUpdateCallbacksOpts()...)
	ctx := s.Context()
	taskQueue := env.Tv().TaskQueue().Name
	updateID := "request-id-accepted-test"
	requestID := uuid.NewString()

	wf := newUpdateChildWorkflow(false)
	s.startWorker(env, taskQueue, wf)

	run, err := env.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		TaskQueue: taskQueue,
	}, wf, "initial input")
	s.NoError(err)

	// Send an update with a specific RequestId and wait for completion.
	_, err = env.FrontendClient().UpdateWorkflowExecution(ctx, &workflowservice.UpdateWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: run.GetID(),
			RunId:      run.GetRunID(),
		},
		WaitPolicy: &updatepb.WaitPolicy{
			LifecycleStage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED,
		},
		Request: &updatepb.Request{
			Meta: &updatepb.Meta{
				UpdateId: updateID,
			},
			Input: &updatepb.Input{
				Name: "update",
				Args: &commonpb.Payloads{
					Payloads: []*commonpb.Payload{testcore.MustToPayload(s.T(), "test")},
				},
			},
			RequestId: requestID,
		},
	})
	s.NoError(err)

	// Verify the accepted event contains the request ID in the AcceptedRequest.
	hist := env.SdkClient().GetWorkflowHistory(ctx, run.GetID(), run.GetRunID(), false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	foundAccepted := false
	for hist.HasNext() {
		event, err := hist.Next()
		s.NoError(err)
		if event.EventType == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED {
			foundAccepted = true
			attrs := event.GetWorkflowExecutionUpdateAcceptedEventAttributes()
			s.NotNil(attrs)
			s.Equal(updateID, attrs.GetAcceptedRequest().GetMeta().GetUpdateId())
			s.Equal(requestID, attrs.GetAcceptedRequest().GetRequestId())
			break
		}
	}
	s.True(foundAccepted, "expected to find WorkflowExecutionUpdateAccepted event")

	// Clean up.
	s.NoError(env.SdkClient().SignalWorkflow(ctx, run.GetID(), run.GetRunID(), "stop", nil))
}

// TestWorkflowNexusHandlerCallbackLinks verifies that bidirectional links are properly
// set when attaching a NexusHandler-variant completion callback to a Workflow.
func (s *NexusWorkflowUpdateTestSuite) TestWorkflowNexusHandlerCallbackLinks(standalone bool) {
	if standalone {
		s.T().Skip("test has no Nexus caller")
	}
	enableNexusHandlerCallbacks := testcore.WithDynamicConfig(
		chasmworkflow.EnabledCallbackKinds,
		[]callbacks.Kind{callbacks.KindNexus, callbacks.KindNexusHandler},
	)
	env := newNexusTestEnv(s.T(), true, append(
		enableUpdateCallbacksOpts(),
		enableNexusHandlerCallbacks,
	)...)
	ctx := s.Context()

	handlerTaskQueue := env.Tv().WithTaskQueueNumber(1).TaskQueue().Name

	// Stands in for a workflow the handler started to process the completion.
	handlerReturnLink := &commonpb.Link_WorkflowEvent{
		Namespace:  env.Namespace().String(),
		WorkflowId: "nh-callback-handler-wf-id",
		RunId:      uuid.NewString(),
		Reference: &commonpb.Link_WorkflowEvent_EventRef{
			EventRef: &commonpb.Link_WorkflowEvent_EventReference{
				EventId:   1,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
			},
		},
	}
	inboundLinks := make(chan []*nexuspb.Link, 1)
	pollerErrCh := env.nexusTaskPoller(ctx, s.T(), handlerTaskQueue, func(
		_ *testing.T,
		res *workflowservice.PollNexusTaskQueueResponse,
	) (*nexusTaskResponse, error) {
		inboundLinks <- res.GetRequest().GetStartOperation().GetLinks()
		return &nexusTaskResponse{
			StartResult: &nexus.HandlerStartOperationResultAsync{OperationToken: "nh-callback-op-token"},
			Links:       []nexus.Link{commonnexus.ConvertLinkWorkflowEventToNexusLink(handlerReturnLink)},
		}, nil
	})

	wfExec := &workflowExecutionType{}
	workflowID, err := wfExec.startAndCompleteEx(s.T(), env.TestEnv, &commonpb.Callback{
		Variant: &commonpb.Callback_NexusHandler_{
			NexusHandler: &commonpb.Callback_NexusHandler{
				TaskQueueName: handlerTaskQueue,
				// The shared poller only accepts tasks addressed to "test-service".
				Service:   "test-service",
				Operation: "OnComplete",
			},
		},
	})
	s.NoError(err)
	s.NoError(s.Rcv(pollerErrCh))

	cbInfo := wfExec.awaitCallbackState(s.T(), workflowID, env.TestEnv, enumspb.CALLBACK_STATE_SUCCEEDED, nil)
	protorequire.ProtoSliceEqual(s.T(),
		[]*commonpb.Link{{Variant: &commonpb.Link_WorkflowEvent_{WorkflowEvent: handlerReturnLink}}},
		cbInfo.GetCallback().GetLinks())

	desc, err := env.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID},
	})
	s.NoError(err)

	// TODO(https://github.com/temporalio/temporal/issues/11958): Callback invocations should have their own request ID, since its used as an idempotency key.
	startRequestInfo, ok := desc.GetWorkflowExtendedInfo().GetRequestIdInfos()[cbInfo.GetRequestId()]
	s.Require().True(ok, "callback request ID should be the one that started the workflow")
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED, startRequestInfo.GetEventType())

	gotLinks := s.Rcv(inboundLinks)
	s.Require().Len(gotLinks, 1)
	gotCallbackLink, err := commonnexus.ConvertNexusLinkToLinkCallback(commonnexus.ConvertLinksFromProto(gotLinks)[0])
	s.NoError(err)
	protorequire.ProtoEqual(s.T(), &commonpb.Link_Callback{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.Execution{
			Type:       enumspb.EXECUTION_TYPE_WORKFLOW,
			BusinessId: workflowID,
			RunId:      desc.GetWorkflowExecutionInfo().GetExecution().GetRunId(),
		},
		RequestId: cbInfo.GetRequestId(),
	}, gotCallbackLink)
}

// TestWorkflowUpdateNexusHandlerCallbackLinks verifies that bidirectional links are properly
// set when attaching a NexusHandler-variant completion callback to a Workflow update.
func (s *NexusWorkflowUpdateTestSuite) TestWorkflowUpdateNexusHandlerCallbackLinks(standalone bool) {
	if standalone {
		s.T().Skip("test has no Nexus caller")
	}
	enableNexusHandlerCallbacks := testcore.WithDynamicConfig(
		chasmworkflow.EnabledCallbackKinds,
		[]callbacks.Kind{callbacks.KindNexus, callbacks.KindNexusHandler},
	)
	env := newNexusTestEnv(s.T(), true, append(
		enableUpdateCallbacksOpts(),
		enableNexusHandlerCallbacks,
	)...)
	ctx := s.Context()

	workflowTaskQueue := env.Tv().TaskQueue().Name
	handlerTaskQueue := env.Tv().WithTaskQueueNumber(1).TaskQueue().Name
	updateID := "nh-callback-links-update-id"
	requestID := uuid.NewString()

	wf := newUpdateChildWorkflow(false)
	s.startWorker(env, workflowTaskQueue, wf)

	// Start the initial workflow.
	run, err := env.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		TaskQueue: workflowTaskQueue,
	}, wf, "initial input")
	s.NoError(err)

	// The link the handler reports back, standing in for a workflow it started to process the
	// completion.
	handlerReturnLink := &commonpb.Link_WorkflowEvent{
		Namespace:  env.Namespace().String(),
		WorkflowId: "nh-callback-handler-wf-id",
		RunId:      uuid.NewString(),
		Reference: &commonpb.Link_WorkflowEvent_EventRef{
			EventRef: &commonpb.Link_WorkflowEvent_EventReference{
				EventId:   1,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
			},
		},
	}

	// Poll as the Nexus handler the NexusHandler-variant callback targets. The poller starts before
	// the update is sent so that it is already waiting when the completed update schedules the delivery.
	//  It answers from its own goroutine, so the links it received come back over a channel.
	inboundLinks := make(chan []*nexuspb.Link, 1)
	pollerErrCh := env.nexusTaskPoller(ctx, s.T(), handlerTaskQueue, func(
		_ *testing.T,
		res *workflowservice.PollNexusTaskQueueResponse,
	) (*nexusTaskResponse, error) {
		inboundLinks <- res.GetRequest().GetStartOperation().GetLinks()
		// Return an ACK of the Nexus operation, returning handlerReturnLink for
		// the resources it (hypothetically) spawned.
		return &nexusTaskResponse{
			StartResult: &nexus.HandlerStartOperationResultAsync{OperationToken: "nh-callback-op-token"},
			Links:       []nexus.Link{commonnexus.ConvertLinkWorkflowEventToNexusLink(handlerReturnLink)},
		}, nil
	})

	// Trigger a Workflow Update with a completion callback. When the Workflow Update completes,
	// the Nexus Handler above will be invoked
	_, err = env.FrontendClient().UpdateWorkflowExecution(ctx, &workflowservice.UpdateWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: run.GetID(),
			RunId:      run.GetRunID(),
		},
		WaitPolicy: &updatepb.WaitPolicy{
			LifecycleStage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED,
		},
		Request: &updatepb.Request{
			Meta: &updatepb.Meta{
				UpdateId: updateID,
			},
			Input: &updatepb.Input{
				Name: "update",
				Args: &commonpb.Payloads{
					Payloads: []*commonpb.Payload{testcore.MustToPayload(s.T(), "test")},
				},
			},
			RequestId: requestID,
			CompletionCallbacks: []*commonpb.Callback{{
				Variant: &commonpb.Callback_NexusHandler_{
					NexusHandler: &commonpb.Callback_NexusHandler{
						TaskQueueName: handlerTaskQueue,
						// The shared poller only accepts tasks addressed to "test-service".
						Service:   "test-service",
						Operation: "OnComplete",
					},
				},
			}},
		},
	})
	s.NoError(err)
	s.NoError(s.Rcv(pollerErrCh))

	// Outbound half: the handler was handed a Link_Callback addressing the Update's callback. The
	// component path is what distinguishes it from a callback attached to the workflow itself.
	gotLinks := s.Rcv(inboundLinks)
	s.Require().Len(gotLinks, 1)
	gotCallbackLink, err := commonnexus.ConvertNexusLinkToLinkCallback(commonnexus.ConvertLinksFromProto(gotLinks)[0])
	s.NoError(err)

	wantInboundLink := &commonpb.Link_Callback{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.Execution{
			Type:       enumspb.EXECUTION_TYPE_WORKFLOW,
			BusinessId: run.GetID(),
			RunId:      run.GetRunID(),
		},
		ComponentPath: []string{"Updates", updateID},
		RequestId:     requestID,
	}
	protorequire.ProtoEqual(s.T(), wantInboundLink, gotCallbackLink)

	// Inbound half: the handler's link is recorded on the callback. Polling because the delivery's
	// outcome is persisted in a transaction that follows the handler's response.
	var callbackInfo *workflowpb.CallbackInfo
	s.Await(func(s *NexusWorkflowUpdateTestSuite) {
		desc, err := env.FrontendClient().DescribeWorkflowExecution(s.Context(), &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: env.Namespace().String(),
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: run.GetID(),
				RunId:      run.GetRunID(),
			},
		})
		s.NoError(err)
		s.Len(desc.GetCallbacks(), 1)
		callbackInfo = desc.GetCallbacks()[0]
		s.Equal(enumspb.CALLBACK_STATE_SUCCEEDED, callbackInfo.GetState())
	}, 10*time.Second, 200*time.Millisecond)

	s.Equal(updateID, callbackInfo.GetTrigger().GetUpdateWorkflowExecutionCompleted().GetUpdateId())
	// TODO(https://github.com/temporalio/temporal/issues/11958): Callback invocations should have their own request ID, since its used as an idempotency key.
	s.Equal(requestID, callbackInfo.GetRequestId())

	// The link we expect to be on the Workflow Update's CallbackInfo. It is the
	// link that was returned from the end Nexus handler.
	wantLinkOnCallback := &commonpb.Link{
		Variant: &commonpb.Link_WorkflowEvent_{
			WorkflowEvent: handlerReturnLink,
		},
	}

	protorequire.ProtoSliceEqual(
		s.T(),
		[]*commonpb.Link{wantLinkOnCallback},
		callbackInfo.GetCallback().GetLinks())

	// Clean up.
	s.NoError(env.SdkClient().SignalWorkflow(ctx, run.GetID(), run.GetRunID(), "stop", nil))
}

func (s *NexusWorkflowUpdateTestSuite) TestLinkOnUpdateReadmittedAfterRegistryCleared(standalone bool) {
	if standalone {
		s.T().Skip("test has no Nexus caller")
	}
	env := newNexusTestEnv(s.T(), true, append(
		enableUpdateCallbacksOpts(),
		testcore.WithDedicatedCluster(),
	)...)
	ctx := s.Context()
	taskQueue := testcore.RandomizeStr(s.T().Name())
	updateID := "readmitted-update-link-test"
	requestID := uuid.NewString()

	wf := newUpdateChildWorkflow(false)

	run, err := env.SdkClient().ExecuteWorkflow(
		ctx, client.StartWorkflowOptions{
			TaskQueue: taskQueue,
		},
		wf,
		"initial input",
	)
	s.Require().NoError(err)

	// Delay starting the worker so the update cannot be accepted before the shard reload.

	updateArgs := &commonpb.Payloads{Payloads: []*commonpb.Payload{testcore.MustToPayload(s.T(), "test")}}
	resultCh := make(chan updateResponseErr, 1)
	go func() {
		resp, err := env.FrontendClient().UpdateWorkflowExecution(ctx, &workflowservice.UpdateWorkflowExecutionRequest{
			Namespace: env.Namespace().String(),
			WorkflowExecution: &commonpb.WorkflowExecution{
				WorkflowId: run.GetID(),
				RunId:      run.GetRunID(),
			},
			WaitPolicy: &updatepb.WaitPolicy{LifecycleStage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ACCEPTED},
			Request: &updatepb.Request{
				Meta: &updatepb.Meta{UpdateId: updateID},
				Input: &updatepb.Input{
					Name: "update",
					Args: updateArgs,
				},
				RequestId: requestID,
				CompletionCallbacks: []*commonpb.Callback{{
					Variant: &commonpb.Callback_Nexus_{Nexus: &commonpb.Callback_Nexus{Url: "http://localhost:9999/callback"}},
				}},
			},
		})
		resultCh <- updateResponseErr{response: resp, err: err}
	}()

	waitUpdateAdmitted := func() {
		s.AwaitTrue(func() bool {
			resp, err := env.FrontendClient().PollWorkflowExecutionUpdate(ctx, &workflowservice.PollWorkflowExecutionUpdateRequest{
				Namespace: env.Namespace().String(),
				UpdateRef: &updatepb.UpdateRef{
					WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: run.GetID(), RunId: run.GetRunID()},
					UpdateId:          updateID,
				},
				WaitPolicy: &updatepb.WaitPolicy{LifecycleStage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_UNSPECIFIED},
			})
			return err == nil && resp.GetStage() >= enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ADMITTED
		}, 10*time.Second, 10*time.Millisecond)
	}
	waitUpdateAdmitted()

	// Reload the shard before the Update is accepted, while none of its state is
	// durable. The server-side retry of the still-open API request must recreate
	// the Update with the same request ID.
	env.CloseShard(env.NamespaceID().String(), run.GetID())
	waitUpdateAdmitted()

	s.startWorker(env, taskQueue, wf)
	var result updateResponseErr
	select {
	case result = <-resultCh:
	case <-time.After(10 * time.Second):
		s.FailNow("timed out waiting for the Update response")
	}
	s.Require().NoError(result.err)
	requestIDRef := result.response.GetLink().GetWorkflowEvent().GetRequestIdRef()
	s.Require().NotNil(requestIDRef, "link should be a RequestIdRef")
	s.Equal(requestID, requestIDRef.GetRequestId())
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED, requestIDRef.GetEventType())

	// Clean up.
	s.NoError(env.SdkClient().SignalWorkflow(ctx, run.GetID(), run.GetRunID(), "stop", nil))
}

func (s *NexusWorkflowUpdateTestSuite) TestLinksOnRepeatedUpdates(standalone bool) {
	if standalone {
		s.T().Skip("test has no Nexus caller")
	}
	env := newNexusTestEnv(s.T(), true, enableUpdateCallbacksOpts()...)
	ctx := s.Context()
	taskQueue := testcore.RandomizeStr(s.T().Name())
	updateID := "repeated-update-links-test"

	// blockOnSignal keeps the update Accepted-but-not-Completed, so a duplicate
	// can attach another callback before the update completes.
	wf := newUpdateChildWorkflow(true)
	s.startWorker(env, taskQueue, wf)

	run, err := env.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		TaskQueue: taskQueue,
	}, wf, "initial input")
	s.Require().NoError(err)

	callbackLink := &commonpb.Link{
		Variant: &commonpb.Link_WorkflowEvent_{
			WorkflowEvent: &commonpb.Link_WorkflowEvent{
				Namespace:  env.Namespace().String(),
				WorkflowId: run.GetID(),
				RunId:      run.GetRunID(),
				Reference: &commonpb.Link_WorkflowEvent_EventRef{
					EventRef: &commonpb.Link_WorkflowEvent_EventReference{
						EventId:   common.FirstEventID,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
					},
				},
			},
		},
	}

	sendUpdate := func(requestID string, callbackLinks []*commonpb.Link) (*workflowservice.UpdateWorkflowExecutionResponse, error) {
		return env.FrontendClient().UpdateWorkflowExecution(ctx, &workflowservice.UpdateWorkflowExecutionRequest{
			Namespace: env.Namespace().String(),
			WorkflowExecution: &commonpb.WorkflowExecution{
				WorkflowId: run.GetID(),
				RunId:      run.GetRunID(),
			},
			WaitPolicy: &updatepb.WaitPolicy{LifecycleStage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ACCEPTED},
			Request: &updatepb.Request{
				Meta: &updatepb.Meta{UpdateId: updateID},
				Input: &updatepb.Input{
					Name: "update",
					Args: &commonpb.Payloads{Payloads: []*commonpb.Payload{testcore.MustToPayload(s.T(), "test")}},
				},
				RequestId: requestID,
				CompletionCallbacks: []*commonpb.Callback{{
					Variant: &commonpb.Callback_Nexus_{Nexus: &commonpb.Callback_Nexus{Url: "http://localhost:9999/callback"}},
					Links:   callbackLinks,
				}},
			},
		})
	}

	firstRequestID := uuid.NewString()
	firstResp, err := sendUpdate(firstRequestID, nil)
	s.Require().NoError(err)

	// Second call reuses the same update ID once the first is already accepted, so
	// it resolves immediately as a duplicate rather than waiting on a new WFT.
	secondRequestID := uuid.NewString()
	secondResp, err := sendUpdate(secondRequestID, []*commonpb.Link{callbackLink})
	s.Require().NoError(err)

	requireRequestIDLink := func(resp *workflowservice.UpdateWorkflowExecutionResponse, requestID string, eventType enumspb.EventType) {
		requestIDRef := resp.GetLink().GetWorkflowEvent().GetRequestIdRef()
		s.Require().NotNil(requestIDRef, "link should be a RequestIdReference")
		s.Require().Equal(requestID, requestIDRef.GetRequestId())
		s.Require().Equal(eventType, requestIDRef.GetEventType())
	}
	requireRequestIDLink(firstResp, firstRequestID, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED)
	requireRequestIDLink(secondResp, secondRequestID, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED)

	// Complete the update before sending another update to verify completed updates linking to their original Accepted event.
	s.NoError(env.SdkClient().SignalWorkflow(ctx, run.GetID(), run.GetRunID(), "complete-update", nil))
	_, err = env.FrontendClient().PollWorkflowExecutionUpdate(ctx, &workflowservice.PollWorkflowExecutionUpdateRequest{
		Namespace: env.Namespace().String(),
		UpdateRef: &updatepb.UpdateRef{
			WorkflowExecution: &commonpb.WorkflowExecution{
				WorkflowId: run.GetID(),
				RunId:      run.GetRunID(),
			},
			UpdateId: updateID,
		},
		WaitPolicy: &updatepb.WaitPolicy{LifecycleStage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED},
	})
	s.Require().NoError(err)

	// Verify history has the OptionsUpdated event with the second requestID attached.
	hist := env.GetHistory(env.Namespace().String(), &commonpb.WorkflowExecution{WorkflowId: run.GetID()})
	updatedEvent := s.RequireHistoryEvent(hist, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED)
	updateOptions := updatedEvent.GetWorkflowExecutionOptionsUpdatedEventAttributes().GetWorkflowUpdateOptions()
	s.Require().Len(updateOptions, 1)
	s.Require().Equal(secondRequestID, updateOptions[0].GetAttachedRequestId())
	s.Require().Len(updateOptions[0].GetAttachedCompletionCallbacks(), 1)
	protorequire.ProtoSliceEqual(
		s.T(),
		[]*commonpb.Link{callbackLink},
		updateOptions[0].GetAttachedCompletionCallbacks()[0].GetLinks(),
	)

	thirdRequestID := uuid.NewString()
	thirdResp, err := sendUpdate(thirdRequestID, nil)
	s.Require().NoError(err)
	eventRef := thirdResp.GetLink().GetWorkflowEvent().GetEventRef()
	s.Require().NotNil(eventRef, "link should be an EventReference")

	updateAcceptedEvent := s.RequireHistoryEvent(hist, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED)
	s.Require().Equal(updateAcceptedEvent.EventId, eventRef.EventId, "eventID should match the Accepted eventID of the original request")
	s.Require().Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED, eventRef.EventType)

	// Clean up.
	s.NoError(env.SdkClient().SignalWorkflow(ctx, run.GetID(), run.GetRunID(), "stop", nil))
}
