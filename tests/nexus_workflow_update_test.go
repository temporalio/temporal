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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	updatepb "go.temporal.io/api/update/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/nexus/nexustest"
	"go.temporal.io/server/tests/testcore"
)

type NexusWorkflowUpdateTestSuite struct {
	NexusTestBaseSuite
}

func TestNexusWorkflowUpdateTestSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, &NexusWorkflowUpdateTestSuite{
		NexusTestBaseSuite: NexusTestBaseSuite{
			useTemporalFailures: true,
		},
	})
}

// updateNexusTestConfig holds configuration for workflow update + nexus integration tests.
type updateNexusTestConfig struct {
	taskQueue    string
	endpointName string
	childWfID    string
	updateID     string
}

// newUpdateNexusTestConfig creates a config with randomized names to avoid collisions.
func newUpdateNexusTestConfig(t *testing.T) updateNexusTestConfig {
	return updateNexusTestConfig{
		taskQueue:    testcore.RandomizeStr(t.Name()),
		endpointName: testcore.RandomizedNexusEndpoint(t.Name()),
		childWfID:    testcore.RandomizeStr("child-workflow-id"),
		updateID:     "update-id",
	}
}

// makeUpdateWithCallbackHandler creates a nexus handler that sends a workflow update with
// completion callbacks to the specified child workflow. onStart is an optional callback
// invoked at the start of each operation (e.g. for counting invocations).
// If the update is already completed (e.g., the workflow has finished), the handler returns
// the result synchronously instead of starting an async operation with callbacks.
func (s *NexusWorkflowUpdateTestSuite) makeUpdateWithCallbackHandler(
	cfg updateNexusTestConfig,
	onStart func(),
) nexustest.Handler {
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
			resp, err := s.FrontendClient().UpdateWorkflowExecution(
				ctx,
				&workflowservice.UpdateWorkflowExecutionRequest{
					Namespace: s.Namespace().String(),
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
								Payloads: []*commonpb.Payload{testcore.MustToPayload(s.T(), "test")},
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
							},
						},
					},
				},
			)
			if err != nil {
				return nil, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeInternal, "update call failed: %v", err)
			}
			// Verify the response contains a link.
			link := resp.GetLink()
			s.NotNil(link, "update response should contain a link")
			if workflowEvent := link.GetWorkflowEvent(); workflowEvent != nil {
				// Accepted/completed update: link points to the accepted event.
				s.Equal(cfg.childWfID, workflowEvent.GetWorkflowId())
				s.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED, workflowEvent.GetRequestIdRef().GetEventType())
			} else if wfLink := link.GetWorkflow(); wfLink != nil {
				// Rejected update: link points to the workflow with a reason.
				s.Equal(cfg.childWfID, wfLink.GetWorkflowId())
				s.Equal(enumspb.LINK_REASON_UPDATE_REJECTED, wfLink.GetReason())
			} else {
				s.Fail("link should be a workflow event or workflow link")
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

// enableUpdateCallbacks enables all CHASM flags needed for update callback tests.
func (s *NexusWorkflowUpdateTestSuite) enableUpdateCallbacks() {
	s.OverrideDynamicConfig(dynamicconfig.EnableChasm, true)
	s.OverrideDynamicConfig(dynamicconfig.EnableCHASMCallbacks, true)
	s.OverrideDynamicConfig(dynamicconfig.EnableWorkflowUpdateCallbacks, true)
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

func (s *NexusWorkflowUpdateTestSuite) TestWorkflowUpdateAsyncNexusOperation() {
	s.enableUpdateCallbacks()
	ctx := testcore.NewContext()
	cfg := newUpdateNexusTestConfig(s.T())

	h := s.makeUpdateWithCallbackHandler(cfg, nil)
	s.setupExternalNexusEndpoint(ctx, cfg.endpointName, h)

	w := worker.New(s.SdkClient(), cfg.taskQueue, worker.Options{})
	childWF := newUpdateChildWorkflow(false)

	callerWF := func(ctx workflow.Context) (string, error) {
		cwf := workflow.ExecuteChildWorkflow(
			workflow.WithWorkflowID(ctx, cfg.childWfID),
			childWF,
			"initial input",
		)
		var childWE workflow.Execution
		if err := cwf.GetChildWorkflowExecution().Get(ctx, &childWE); err != nil {
			return "", err
		}
		nexusClient := workflow.NewNexusClient(cfg.endpointName, "test")
		fut := nexusClient.ExecuteOperation(ctx, "operation", childWE.ID, workflow.NexusOperationOptions{})
		var result string
		err := fut.Get(ctx, &result)
		return result, err
	}

	w.RegisterWorkflow(callerWF)
	w.RegisterWorkflow(childWF)
	s.NoError(w.Start())
	s.T().Cleanup(w.Stop)

	run, err := s.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		TaskQueue:                cfg.taskQueue,
		WorkflowExecutionTimeout: 30 * time.Second,
	}, callerWF)
	s.NoError(err)
	var result string
	s.NoError(run.Get(ctx, &result))
	s.Equal("updated: test", result)

	// Verify the child workflow's history contains the update accepted event with callbacks.
	childHistory := s.SdkClient().GetWorkflowHistory(ctx, cfg.childWfID, "", false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	foundUpdateAccepted := false
	for childHistory.HasNext() {
		event, err := childHistory.Next()
		s.NoError(err)
		if event.EventType == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED {
			foundUpdateAccepted = true
			attrs := event.GetWorkflowExecutionUpdateAcceptedEventAttributes()
			s.NotNil(attrs)
			s.Equal(cfg.updateID, attrs.GetAcceptedRequest().GetMeta().GetUpdateId())
			s.NotEmpty(attrs.GetAcceptedRequest().GetCompletionCallbacks())
			break
		}
	}
	s.True(foundUpdateAccepted, "expected to find WorkflowExecutionUpdateAccepted event in child workflow history")
}

func (s *NexusWorkflowUpdateTestSuite) TestWorkflowUpdateAsyncAttachedNexusOperation() {
	s.enableUpdateCallbacks()
	ctx := testcore.NewContext()
	cfg := newUpdateNexusTestConfig(s.T())

	h := s.makeUpdateWithCallbackHandler(cfg, nil)
	s.setupExternalNexusEndpoint(ctx, cfg.endpointName, h)

	w := worker.New(s.SdkClient(), cfg.taskQueue, worker.Options{})
	childWF := newUpdateChildWorkflow(true)

	callerWF := func(ctx workflow.Context) (string, error) {
		cwf := workflow.ExecuteChildWorkflow(
			workflow.WithWorkflowID(ctx, cfg.childWfID),
			childWF,
			"initial input",
		)
		var childWE workflow.Execution
		if err := cwf.GetChildWorkflowExecution().Get(ctx, &childWE); err != nil {
			return "", err
		}
		nexusClient := workflow.NewNexusClient(cfg.endpointName, "test")
		fut := nexusClient.ExecuteOperation(ctx, "operation", childWE.ID, workflow.NexusOperationOptions{})
		var exec workflow.NexusOperationExecution
		if err := fut.GetNexusOperationExecution().Get(ctx, &exec); err != nil {
			return "", err
		}
		// Send a second update to verify attaching after starting works.
		afut := nexusClient.ExecuteOperation(ctx, "operation", childWE.ID, workflow.NexusOperationOptions{})
		var aexec workflow.NexusOperationExecution
		if err := afut.GetNexusOperationExecution().Get(ctx, &aexec); err != nil {
			return "", err
		}
		// Signal the child to complete the update now that both operations are attached.
		if err := workflow.SignalExternalWorkflow(ctx, childWE.ID, "", "complete-update", nil).Get(ctx, nil); err != nil {
			return "", err
		}
		var aresult string
		if err := afut.Get(ctx, &aresult); err != nil {
			return "", err
		}

		var result string
		err := fut.Get(ctx, &result)
		return result, err
	}

	w.RegisterWorkflow(callerWF)
	w.RegisterWorkflow(childWF)
	s.NoError(w.Start())
	s.T().Cleanup(w.Stop)

	run, err := s.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		TaskQueue:                cfg.taskQueue,
		WorkflowExecutionTimeout: 10 * time.Second,
	}, callerWF)
	s.NoError(err)
	var result string
	s.NoError(run.Get(ctx, &result))
	s.Equal("updated: test", result)
}

// TestWorkflowUpdateCallbackOnAlreadyCompletedUpdate verifies that when a second caller
// sends an update request with the same update ID after the update has already completed,
// the second request returns the result synchronously without attaching a new callback.
// The child workflow should only have one update callback (from the first request).
func (s *NexusWorkflowUpdateTestSuite) TestWorkflowUpdateNoCallbackAttachedOnAlreadyCompletedUpdate() {
	s.enableUpdateCallbacks()
	ctx := testcore.NewContext()
	cfg := newUpdateNexusTestConfig(s.T())
	cfg.updateID = "already-completed-update-id"

	var operationCount atomic.Int32
	h := s.makeUpdateWithCallbackHandler(cfg, func() { operationCount.Add(1) })
	s.setupExternalNexusEndpoint(ctx, cfg.endpointName, h)

	w := worker.New(s.SdkClient(), cfg.taskQueue, worker.Options{})
	childWF := newUpdateChildWorkflow(false)

	// Caller workflow sends two nexus operations targeting the same update.
	// The first one triggers the update, the second one arrives after it completes
	// and should still get the result via AttachCallbacks.
	callerWF := func(ctx workflow.Context) (string, error) {
		cwf := workflow.ExecuteChildWorkflow(
			workflow.WithWorkflowID(ctx, cfg.childWfID),
			childWF,
			"initial input",
		)
		var childWE workflow.Execution
		if err := cwf.GetChildWorkflowExecution().Get(ctx, &childWE); err != nil {
			return "", err
		}
		nexusClient := workflow.NewNexusClient(cfg.endpointName, "test")

		// First nexus operation: triggers the update.
		fut1 := nexusClient.ExecuteOperation(ctx, "operation", childWE.ID, workflow.NexusOperationOptions{})
		var result1 string
		if err := fut1.Get(ctx, &result1); err != nil {
			return "", err
		}

		// Second nexus operation: targets the same already-completed update.
		fut2 := nexusClient.ExecuteOperation(ctx, "operation", childWE.ID, workflow.NexusOperationOptions{})
		var result2 string
		if err := fut2.Get(ctx, &result2); err != nil {
			return "", err
		}

		return result1 + " | " + result2, nil
	}

	w.RegisterWorkflow(callerWF)
	w.RegisterWorkflow(childWF)
	s.NoError(w.Start())
	s.T().Cleanup(w.Stop)

	run, err := s.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		TaskQueue:                cfg.taskQueue,
		WorkflowExecutionTimeout: 30 * time.Second,
	}, callerWF)
	s.NoError(err)
	var result string
	s.NoError(run.Get(ctx, &result))
	s.Equal("updated: test | updated: test", result)
	s.Equal(int32(2), operationCount.Load(), "expected two nexus operations to be started")

	// Verify the child workflow has exactly one update callback (from the first request).
	// The second request returns synchronously because the update is already completed,
	// so no additional callback is attached.
	descResp, err := s.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: s.Namespace().String(),
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
	s.Equal(1, updateCallbackCount, "expected exactly one update callback on the child workflow")

	// Verify the child workflow has the correct request ID infos.
	// Each nexus operation generates a unique request ID. If the second operation
	// (targeting the already-completed update) had attached its request ID, we would
	// see 3 entries instead of 2, or an OPTIONS_UPDATED entry. The count of 2 with
	// only STARTED and UPDATE_ACCEPTED types proves the second request ID was not attached.
	sdkDescResp, err := s.SdkClient().DescribeWorkflowExecution(ctx, cfg.childWfID, "")
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
}

// TestDescribeWorkflowShowsUpdateCallbacks verifies that DescribeWorkflowExecution
// returns update-level callbacks after an update with callbacks is sent.
func (s *NexusWorkflowUpdateTestSuite) TestDescribeWorkflowShowsUpdateCallbacks() {
	s.enableUpdateCallbacks()
	ctx := testcore.NewContext()
	taskQueue := testcore.RandomizeStr(s.T().Name())
	updateID := "describe-callback-update-id"
	callbackURL := "http://localhost:9999/callback"

	w := worker.New(
		s.SdkClient(),
		taskQueue,
		worker.Options{},
	)

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

	w.RegisterWorkflow(wf)
	s.NoError(w.Start())
	s.T().Cleanup(w.Stop)

	run, err := s.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		TaskQueue: taskQueue,
	}, wf)
	s.NoError(err)

	// Send update with completion callbacks (don't wait for completion).
	testPayload := testcore.MustToPayload(s.T(), "test")
	updateDone := make(chan struct{})
	go func() {
		defer close(updateDone)
		_, _ = s.FrontendClient().UpdateWorkflowExecution(ctx, &workflowservice.UpdateWorkflowExecutionRequest{
			Namespace: s.Namespace().String(),
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
	s.EventuallyWithT(func(t *assert.CollectT) {
		desc, err := s.SdkClient().DescribeWorkflowExecution(ctx, run.GetID(), run.GetRunID())
		require.NoError(t, err)
		require.NotNil(t, desc.GetCallbacks(), "callbacks should be present")
		found := false
		for _, cb := range desc.GetCallbacks() {
			if cb.GetCallback().GetNexus().GetUrl() == callbackURL {
				found = true
				// Verify the trigger references the update.
				trigger := cb.GetTrigger()
				require.NotNil(t, trigger)
				updateTrigger := trigger.GetUpdateWorkflowExecutionCompleted()
				if updateTrigger != nil {
					require.Equal(t, updateID, updateTrigger.GetUpdateId())
				}
			}
		}
		require.True(t, found, "expected to find callback with URL %s", callbackURL)
	}, 10*time.Second, 500*time.Millisecond)

	// Complete the update and stop the workflow.
	s.NoError(s.SdkClient().SignalWorkflow(ctx, run.GetID(), run.GetRunID(), "complete-update", nil))
	<-updateDone
	s.NoError(s.SdkClient().SignalWorkflow(ctx, run.GetID(), run.GetRunID(), "stop", nil))
}

// getFirstWFTaskCompleteEventID scans the workflow history and returns the event ID
// of the first WorkflowTaskCompleted event.
func (s *NexusWorkflowUpdateTestSuite) getFirstWFTaskCompleteEventID(ctx context.Context, workflowID, runID string) int64 {
	hist := s.SdkClient().GetWorkflowHistory(ctx, workflowID, runID, false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	for hist.HasNext() {
		event, err := hist.Next()
		s.NoError(err)
		if event.EventType == enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED {
			return event.EventId
		}
	}
	s.FailNow("couldn't find a WorkflowTaskCompleted event", "workflowID=%s runID=%s", workflowID, runID)
	return 0
}

// TestWorkflowUpdateCallbackAfterResetInflightUpdate verifies that when a workflow is
// reset while an update with completion callbacks is in-flight (accepted but not completed),
// the update is reapplied in the new run and the callback fires when the update completes.
func (s *NexusWorkflowUpdateTestSuite) TestWorkflowUpdateCallbackAfterResetInflightUpdate() {
	s.enableUpdateCallbacks()
	ctx := testcore.NewContext()
	cfg := newUpdateNexusTestConfig(s.T())

	h := s.makeUpdateWithCallbackHandler(cfg, nil)
	s.setupExternalNexusEndpoint(ctx, cfg.endpointName, h)

	targetTaskQueue := testcore.RandomizeStr("target-" + s.T().Name())

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
	targetWorker := worker.New(s.SdkClient(), targetTaskQueue, worker.Options{})
	targetWorker.RegisterWorkflow(targetWF)
	s.NoError(targetWorker.Start())
	s.T().Cleanup(targetWorker.Stop)

	targetRun, err := s.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        cfg.childWfID,
		TaskQueue: targetTaskQueue,
	}, targetWF, "initial input")
	s.NoError(err)

	// Caller workflow sends a nexus operation that triggers the update with callbacks.
	callerWF := func(ctx workflow.Context) (string, error) {
		nexusClient := workflow.NewNexusClient(cfg.endpointName, "test")
		fut := nexusClient.ExecuteOperation(ctx, "operation", cfg.childWfID, workflow.NexusOperationOptions{})
		var result string
		err := fut.Get(ctx, &result)
		return result, err
	}

	callerWorker := worker.New(s.SdkClient(), cfg.taskQueue, worker.Options{})
	callerWorker.RegisterWorkflow(callerWF)
	s.NoError(callerWorker.Start())
	s.T().Cleanup(callerWorker.Stop)

	callerRun, err := s.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		TaskQueue:                cfg.taskQueue,
		WorkflowExecutionTimeout: 30 * time.Second,
	}, callerWF)
	s.NoError(err)

	// Wait for the update to be accepted on the target workflow.
	s.EventuallyWithT(func(t *assert.CollectT) {
		hist := s.SdkClient().GetWorkflowHistory(ctx, cfg.childWfID, targetRun.GetRunID(), false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
		for hist.HasNext() {
			event, err := hist.Next()
			require.NoError(t, err)
			if event.EventType == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED {
				return
			}
		}
		require.Fail(t, "update not yet accepted")
	}, 10*time.Second, 500*time.Millisecond)

	// Reset the target workflow to the first WFT completed event (before the update).
	resetResp, err := s.FrontendClient().ResetWorkflowExecution(ctx, &workflowservice.ResetWorkflowExecutionRequest{
		Namespace: s.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: cfg.childWfID,
			RunId:      targetRun.GetRunID(),
		},
		Reason:                    "test reset with inflight update",
		RequestId:                 uuid.NewString(),
		WorkflowTaskFinishEventId: s.getFirstWFTaskCompleteEventID(ctx, cfg.childWfID, targetRun.GetRunID()),
	})
	s.NoError(err)

	// Verify the update was reapplied in the new run's history.
	newHist := s.SdkClient().GetWorkflowHistory(ctx, cfg.childWfID, resetResp.RunId, false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	foundReappliedUpdate := false
	for newHist.HasNext() {
		event, err := newHist.Next()
		s.NoError(err)
		if event.EventType == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ADMITTED {
			attrs := event.GetWorkflowExecutionUpdateAdmittedEventAttributes()
			if attrs.GetRequest().GetMeta().GetUpdateId() == cfg.updateID {
				foundReappliedUpdate = true
				// Verify callbacks are preserved in the reapplied request.
				s.NotEmpty(attrs.GetRequest().GetCompletionCallbacks(),
					"reapplied update should preserve completion callbacks")
			}
		}
	}
	s.True(foundReappliedUpdate, "expected reapplied UpdateAdmitted event in new run")

	// Signal the new run to complete the update, which should trigger the callback.
	s.NoError(s.SdkClient().SignalWorkflow(ctx, cfg.childWfID, resetResp.RunId, "complete-update", nil))

	// The callback fires → nexus operation completes → caller gets the result.
	var result string
	s.NoError(callerRun.Get(ctx, &result))
	s.Equal("updated: test", result)

	// Clean up: stop the new run of the target workflow.
	s.NoError(s.SdkClient().SignalWorkflow(ctx, cfg.childWfID, resetResp.RunId, "stop", nil))
}

// TestWorkflowUpdateCallbackAfterResetRejectedUpdate verifies that when a workflow is
// reset while an update with completion callbacks is in-flight (accepted but not completed),
// and the new run's workflow code rejects the reapplied update via a validator, the
// completion callback fires with a failure and the caller's nexus operation fails.
func (s *NexusWorkflowUpdateTestSuite) TestWorkflowUpdateCallbackAfterResetRejectedUpdate() {
	s.enableUpdateCallbacks()
	ctx := testcore.NewContext()
	cfg := newUpdateNexusTestConfig(s.T())

	h := s.makeUpdateWithCallbackHandler(cfg, nil)
	s.setupExternalNexusEndpoint(ctx, cfg.endpointName, h)

	targetTaskQueue := testcore.RandomizeStr("target-" + s.T().Name())

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

	targetWorker := worker.New(s.SdkClient(), targetTaskQueue, worker.Options{})
	targetWorker.RegisterWorkflow(targetWF)
	s.NoError(targetWorker.Start())
	s.T().Cleanup(targetWorker.Stop)

	targetRun, err := s.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        cfg.childWfID,
		TaskQueue: targetTaskQueue,
	}, targetWF, "initial input")
	s.NoError(err)

	// Caller workflow sends a nexus operation that triggers the update with callbacks.
	callerWF := func(ctx workflow.Context) (string, error) {
		nexusClient := workflow.NewNexusClient(cfg.endpointName, "test")
		fut := nexusClient.ExecuteOperation(ctx, "operation", cfg.childWfID, workflow.NexusOperationOptions{})
		var result string
		err := fut.Get(ctx, &result)
		return result, err
	}

	callerWorker := worker.New(s.SdkClient(), cfg.taskQueue, worker.Options{})
	callerWorker.RegisterWorkflow(callerWF)
	s.NoError(callerWorker.Start())
	s.T().Cleanup(callerWorker.Stop)

	callerRun, err := s.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		TaskQueue:                cfg.taskQueue,
		WorkflowExecutionTimeout: 30 * time.Second,
	}, callerWF)
	s.NoError(err)

	// Wait for the update to be accepted on the target workflow.
	s.EventuallyWithT(func(t *assert.CollectT) {
		hist := s.SdkClient().GetWorkflowHistory(ctx, cfg.childWfID, targetRun.GetRunID(), false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
		for hist.HasNext() {
			event, err := hist.Next()
			require.NoError(t, err)
			if event.EventType == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED {
				return
			}
		}
		require.Fail(t, "update not yet accepted")
	}, 10*time.Second, 500*time.Millisecond)

	// Flip the flag so the validator rejects updates in the new run.
	shouldReject.Store(true)

	// Reset the target workflow to the first WFT completed event (before the update).
	resetResp, err := s.FrontendClient().ResetWorkflowExecution(ctx, &workflowservice.ResetWorkflowExecutionRequest{
		Namespace: s.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: cfg.childWfID,
			RunId:      targetRun.GetRunID(),
		},
		Reason:                    "test reset with inflight update expecting rejection",
		RequestId:                 uuid.NewString(),
		WorkflowTaskFinishEventId: s.getFirstWFTaskCompleteEventID(ctx, cfg.childWfID, targetRun.GetRunID()),
	})
	s.NoError(err)

	// Verify the update was reapplied in the new run's history.
	newHist := s.SdkClient().GetWorkflowHistory(ctx, cfg.childWfID, resetResp.RunId, false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	foundReappliedUpdate := false
	for newHist.HasNext() {
		event, err := newHist.Next()
		s.NoError(err)
		if event.EventType == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ADMITTED {
			attrs := event.GetWorkflowExecutionUpdateAdmittedEventAttributes()
			if attrs.GetRequest().GetMeta().GetUpdateId() == cfg.updateID {
				foundReappliedUpdate = true
				s.NotEmpty(attrs.GetRequest().GetCompletionCallbacks(),
					"reapplied update should preserve completion callbacks")
			}
		}
	}
	s.True(foundReappliedUpdate, "expected reapplied UpdateAdmitted event in new run")

	// The reapplied update is rejected by the validator → callback fires with failure →
	// nexus operation fails → caller workflow fails.
	var result string
	err = callerRun.Get(ctx, &result)
	s.Error(err, "expected caller workflow to fail because the reapplied update was rejected")

	// Verify it's a NexusOperationError wrapping the rejection failure.
	var wee *temporal.WorkflowExecutionError
	s.ErrorAs(err, &wee)
	var noe *temporal.NexusOperationError
	s.ErrorAs(wee, &noe)

	// Clean up: stop the new run of the target workflow.
	s.NoError(s.SdkClient().SignalWorkflow(ctx, cfg.childWfID, resetResp.RunId, "stop", nil))
}

// TestWorkflowUpdateCallbackAfterResetCompletedUpdate verifies that when a workflow is
// reset after an update with callbacks has already completed, the update is reapplied in
// the new run, completes again, and a new nexus operation targeting the same update ID
// receives the result via the AttachCallbacks path.
func (s *NexusWorkflowUpdateTestSuite) TestWorkflowUpdateCallbackAfterResetCompletedUpdate() {
	s.enableUpdateCallbacks()
	ctx := testcore.NewContext()
	cfg := newUpdateNexusTestConfig(s.T())
	cfg.updateID = "reset-completed-update-id"

	var operationCount atomic.Int32
	h := s.makeUpdateWithCallbackHandler(cfg, func() { operationCount.Add(1) })
	s.setupExternalNexusEndpoint(ctx, cfg.endpointName, h)

	targetTaskQueue := testcore.RandomizeStr("target-" + s.T().Name())

	// Target workflow: update handler completes immediately.
	targetWF := newUpdateChildWorkflow(false)

	targetWorker := worker.New(s.SdkClient(), targetTaskQueue, worker.Options{})
	targetWorker.RegisterWorkflow(targetWF)
	s.NoError(targetWorker.Start())
	s.T().Cleanup(targetWorker.Stop)

	targetRun, err := s.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        cfg.childWfID,
		TaskQueue: targetTaskQueue,
	}, targetWF, "initial input")
	s.NoError(err)

	// Caller workflow sends a single nexus operation.
	callerWF := func(ctx workflow.Context) (string, error) {
		nexusClient := workflow.NewNexusClient(cfg.endpointName, "test")
		fut := nexusClient.ExecuteOperation(ctx, "operation", cfg.childWfID, workflow.NexusOperationOptions{})
		var result string
		err := fut.Get(ctx, &result)
		return result, err
	}

	callerWorker := worker.New(s.SdkClient(), cfg.taskQueue, worker.Options{})
	callerWorker.RegisterWorkflow(callerWF)
	s.NoError(callerWorker.Start())
	s.T().Cleanup(callerWorker.Stop)

	// First caller: triggers the update, it completes, callback fires.
	run1, err := s.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		TaskQueue:                cfg.taskQueue,
		WorkflowExecutionTimeout: 30 * time.Second,
	}, callerWF)
	s.NoError(err)
	var result1 string
	s.NoError(run1.Get(ctx, &result1))
	s.Equal("updated: test", result1)

	// Reset the target workflow to before the update.
	resetResp, err := s.FrontendClient().ResetWorkflowExecution(ctx, &workflowservice.ResetWorkflowExecutionRequest{
		Namespace: s.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: cfg.childWfID,
			RunId:      targetRun.GetRunID(),
		},
		Reason:                    "test reset with completed update",
		RequestId:                 uuid.NewString(),
		WorkflowTaskFinishEventId: s.getFirstWFTaskCompleteEventID(ctx, cfg.childWfID, targetRun.GetRunID()),
	})
	s.NoError(err)

	// The update is reapplied and completes again in the new run.
	// Wait for the update to complete in the new run before sending the second operation.
	s.EventuallyWithT(func(t *assert.CollectT) {
		hist := s.SdkClient().GetWorkflowHistory(ctx, cfg.childWfID, resetResp.RunId, false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
		for hist.HasNext() {
			event, err := hist.Next()
			require.NoError(t, err)
			if event.EventType == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_COMPLETED {
				return
			}
		}
		require.Fail(t, "update not yet completed in new run")
	}, 10*time.Second, 500*time.Millisecond)

	// Second caller: sends a new nexus operation targeting the same update ID.
	// Since the update is already completed in the new run, AttachCallbacks fires the callback.
	run2, err := s.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		TaskQueue:                cfg.taskQueue,
		WorkflowExecutionTimeout: 30 * time.Second,
	}, callerWF)
	s.NoError(err)
	var result2 string
	s.NoError(run2.Get(ctx, &result2))
	s.Equal("updated: test", result2)

	s.Equal(int32(2), operationCount.Load(), "expected two nexus operations to be started")

	// Clean up: stop the new run of the target workflow.
	s.NoError(s.SdkClient().SignalWorkflow(ctx, cfg.childWfID, resetResp.RunId, "stop", nil))
}

// TestWorkflowUpdateSyncReturnForCompletedWorkflow verifies that when a second nexus
// operation targets the same update ID on a workflow that has already completed, the
// handler detects the update is already completed and returns the result synchronously
// (instead of starting an async operation with callbacks).
func (s *NexusWorkflowUpdateTestSuite) TestWorkflowUpdateSyncReturnForCompletedWorkflow() {
	s.enableUpdateCallbacks()
	ctx := testcore.NewContext()
	cfg := newUpdateNexusTestConfig(s.T())
	cfg.updateID = "sync-return-completed-wf-update-id"

	var operationCount atomic.Int32
	h := s.makeUpdateWithCallbackHandler(cfg, func() { operationCount.Add(1) })
	s.setupExternalNexusEndpoint(ctx, cfg.endpointName, h)

	targetTaskQueue := testcore.RandomizeStr("target-" + s.T().Name())

	// Target workflow: update handler completes immediately.
	targetWF := newUpdateChildWorkflow(false)

	targetWorker := worker.New(s.SdkClient(), targetTaskQueue, worker.Options{})
	targetWorker.RegisterWorkflow(targetWF)
	s.NoError(targetWorker.Start())
	s.T().Cleanup(targetWorker.Stop)

	targetRun, err := s.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        cfg.childWfID,
		TaskQueue: targetTaskQueue,
	}, targetWF, "initial input")
	s.NoError(err)

	// Caller workflow sends a single nexus operation.
	callerWF := func(ctx workflow.Context) (string, error) {
		nexusClient := workflow.NewNexusClient(cfg.endpointName, "test")
		fut := nexusClient.ExecuteOperation(ctx, "operation", cfg.childWfID, workflow.NexusOperationOptions{})
		var result string
		err := fut.Get(ctx, &result)
		return result, err
	}

	callerWorker := worker.New(s.SdkClient(), cfg.taskQueue, worker.Options{})
	callerWorker.RegisterWorkflow(callerWF)
	s.NoError(callerWorker.Start())
	s.T().Cleanup(callerWorker.Stop)

	// First caller: triggers the update, it completes, callback fires.
	run1, err := s.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		TaskQueue:                cfg.taskQueue,
		WorkflowExecutionTimeout: 30 * time.Second,
	}, callerWF)
	s.NoError(err)
	var result1 string
	s.NoError(run1.Get(ctx, &result1))
	s.Equal("updated: test", result1)

	// Complete the target workflow by sending the "stop" signal.
	s.NoError(s.SdkClient().SignalWorkflow(ctx, cfg.childWfID, targetRun.GetRunID(), "stop", nil))

	// Wait for the target workflow to complete.
	var targetResult string
	s.NoError(targetRun.Get(ctx, &targetResult))

	// Second caller: sends a new nexus operation targeting the same update ID.
	// Since the workflow is completed and the update was already completed,
	// UpdateWorkflowExecution returns the outcome directly → handler returns sync.
	run2, err := s.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		TaskQueue:                cfg.taskQueue,
		WorkflowExecutionTimeout: 30 * time.Second,
	}, callerWF)
	s.NoError(err)
	var result2 string
	s.NoError(run2.Get(ctx, &result2))
	s.Equal("updated: test", result2)

	s.Equal(int32(2), operationCount.Load(), "expected two nexus operations to be started")
}

// TestWorkflowUpdateCallbackOnFailedUpdate verifies that when an update handler returns
// an error (update completes with a failure outcome), the completion callback fires and
// the caller's nexus operation completes with a failure.
func (s *NexusWorkflowUpdateTestSuite) TestWorkflowUpdateCallbackOnFailedUpdate() {
	s.enableUpdateCallbacks()
	ctx := testcore.NewContext()
	cfg := newUpdateNexusTestConfig(s.T())
	cfg.updateID = "failed-update-id"

	h := s.makeUpdateWithCallbackHandler(cfg, nil)
	s.setupExternalNexusEndpoint(ctx, cfg.endpointName, h)

	targetTaskQueue := testcore.RandomizeStr("target-" + s.T().Name())

	// Target workflow: update handler returns an error after acceptance.
	targetWF := func(ctx workflow.Context, input string) (string, error) {
		if err := workflow.SetUpdateHandler(ctx, "update", func(ctx workflow.Context, input string) (string, error) {
			return "", temporal.NewApplicationError("update handler failed", "UpdateFailed", nil)
		}); err != nil {
			return "", err
		}
		signalCh := workflow.GetSignalChannel(ctx, "stop")
		signalCh.Receive(ctx, nil)
		return "done: " + input, nil
	}

	targetWorker := worker.New(s.SdkClient(), targetTaskQueue, worker.Options{})
	targetWorker.RegisterWorkflow(targetWF)
	s.NoError(targetWorker.Start())
	s.T().Cleanup(targetWorker.Stop)

	_, err := s.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        cfg.childWfID,
		TaskQueue: targetTaskQueue,
	}, targetWF, "initial input")
	s.NoError(err)

	// Caller workflow sends a nexus operation targeting the child.
	callerWF := func(ctx workflow.Context) (string, error) {
		nexusClient := workflow.NewNexusClient(cfg.endpointName, "test")
		fut := nexusClient.ExecuteOperation(ctx, "operation", cfg.childWfID, workflow.NexusOperationOptions{})
		var result string
		err := fut.Get(ctx, &result)
		return result, err
	}

	callerWorker := worker.New(s.SdkClient(), cfg.taskQueue, worker.Options{})
	callerWorker.RegisterWorkflow(callerWF)
	s.NoError(callerWorker.Start())
	s.T().Cleanup(callerWorker.Stop)

	callerRun, err := s.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		TaskQueue:                cfg.taskQueue,
		WorkflowExecutionTimeout: 30 * time.Second,
	}, callerWF)
	s.NoError(err)

	// The update is accepted but the handler returns an error → update completes with
	// failure → callback fires → nexus operation fails → caller workflow fails.
	var result string
	err = callerRun.Get(ctx, &result)
	s.Error(err, "expected caller workflow to fail because the update failed")

	// Verify it's a NexusOperationError wrapping the update failure.
	var wee *temporal.WorkflowExecutionError
	s.ErrorAs(err, &wee)
	var noe *temporal.NexusOperationError
	s.ErrorAs(wee, &noe)

	// Clean up: stop the target workflow.
	s.NoError(s.SdkClient().SignalWorkflow(ctx, cfg.childWfID, "", "stop", nil))
}

// TestWorkflowUpdateCallbackOnWorkflowTerminate verifies that when a workflow is
// terminated while an update with completion callbacks is in-flight (accepted, handler
// blocking), the ProcessCloseCallbacks mechanism fires the callback and the caller's
// nexus operation completes.
func (s *NexusWorkflowUpdateTestSuite) TestWorkflowUpdateCallbackOnWorkflowTerminate() {
	s.enableUpdateCallbacks()
	ctx := testcore.NewContext()
	cfg := newUpdateNexusTestConfig(s.T())
	cfg.updateID = "terminate-update-id"

	h := s.makeUpdateWithCallbackHandler(cfg, nil)
	s.setupExternalNexusEndpoint(ctx, cfg.endpointName, h)

	targetTaskQueue := testcore.RandomizeStr("target-" + s.T().Name())

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

	targetWorker := worker.New(s.SdkClient(), targetTaskQueue, worker.Options{})
	targetWorker.RegisterWorkflow(targetWF)
	s.NoError(targetWorker.Start())
	s.T().Cleanup(targetWorker.Stop)

	_, err := s.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        cfg.childWfID,
		TaskQueue: targetTaskQueue,
	}, targetWF, "initial input")
	s.NoError(err)

	// Caller workflow sends a nexus operation targeting the child.
	callerWF := func(ctx workflow.Context) (string, error) {
		nexusClient := workflow.NewNexusClient(cfg.endpointName, "test")
		fut := nexusClient.ExecuteOperation(ctx, "operation", cfg.childWfID, workflow.NexusOperationOptions{})
		var result string
		err := fut.Get(ctx, &result)
		return result, err
	}

	callerWorker := worker.New(s.SdkClient(), cfg.taskQueue, worker.Options{})
	callerWorker.RegisterWorkflow(callerWF)
	s.NoError(callerWorker.Start())
	s.T().Cleanup(callerWorker.Stop)

	callerRun, err := s.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		TaskQueue:                cfg.taskQueue,
		WorkflowExecutionTimeout: 30 * time.Second,
	}, callerWF)
	s.NoError(err)

	// Wait for the update to be accepted on the target.
	s.EventuallyWithT(func(t *assert.CollectT) {
		hist := s.SdkClient().GetWorkflowHistory(ctx, cfg.childWfID, "", false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
		for hist.HasNext() {
			event, err := hist.Next()
			require.NoError(t, err)
			if event.EventType == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED {
				return
			}
		}
		require.Fail(t, "update not yet accepted")
	}, 10*time.Second, 500*time.Millisecond)

	// Terminate the target workflow while the update is in-flight.
	// ProcessCloseCallbacks should fire the update-level callbacks.
	s.NoError(s.SdkClient().TerminateWorkflow(ctx, cfg.childWfID, "", "testing terminate with inflight update callback"))

	// The callback fires → nexus operation completes → caller workflow finishes.
	// The caller should get an error (the nexus operation failed because the
	// target was terminated).
	var result string
	err = callerRun.Get(ctx, &result)
	s.Error(err, "expected caller workflow to fail because the target was terminated")
}

// TestWorkflowUpdateCallbackOnWorkflowContinueAsNew verifies that when a workflow
// continues-as-new while an update with completion callbacks is in-flight (accepted,
// handler blocking), the update callbacks are fired and the caller's nexus operation
// completes with a failure (the old run is closed).
func (s *NexusWorkflowUpdateTestSuite) TestWorkflowUpdateCallbackOnWorkflowContinueAsNew() {
	s.enableUpdateCallbacks()
	ctx := testcore.NewContext()
	cfg := newUpdateNexusTestConfig(s.T())
	cfg.updateID = "continue-as-new-update-id"

	h := s.makeUpdateWithCallbackHandler(cfg, nil)
	s.setupExternalNexusEndpoint(ctx, cfg.endpointName, h)

	targetTaskQueue := testcore.RandomizeStr("target-" + s.T().Name())

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

	targetWorker := worker.New(s.SdkClient(), targetTaskQueue, worker.Options{})
	targetWorker.RegisterWorkflow(targetWF)
	s.NoError(targetWorker.Start())
	s.T().Cleanup(targetWorker.Stop)

	_, err := s.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        cfg.childWfID,
		TaskQueue: targetTaskQueue,
	}, targetWF, "initial input")
	s.NoError(err)

	// Caller workflow sends a nexus operation targeting the child.
	callerWF := func(ctx workflow.Context) (string, error) {
		nexusClient := workflow.NewNexusClient(cfg.endpointName, "test")
		fut := nexusClient.ExecuteOperation(ctx, "operation", cfg.childWfID, workflow.NexusOperationOptions{})
		var result string
		err := fut.Get(ctx, &result)
		return result, err
	}

	callerWorker := worker.New(s.SdkClient(), cfg.taskQueue, worker.Options{})
	callerWorker.RegisterWorkflow(callerWF)
	s.NoError(callerWorker.Start())
	s.T().Cleanup(callerWorker.Stop)

	callerRun, err := s.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		TaskQueue:                cfg.taskQueue,
		WorkflowExecutionTimeout: 30 * time.Second,
	}, callerWF)
	s.NoError(err)

	// Wait for the update to be accepted on the target.
	s.EventuallyWithT(func(t *assert.CollectT) {
		hist := s.SdkClient().GetWorkflowHistory(ctx, cfg.childWfID, "", false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
		for hist.HasNext() {
			event, err := hist.Next()
			require.NoError(t, err)
			if event.EventType == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED {
				return
			}
		}
		require.Fail(t, "update not yet accepted")
	}, 10*time.Second, 500*time.Millisecond)

	// Signal the target workflow to continue-as-new while the update is in-flight.
	s.NoError(s.SdkClient().SignalWorkflow(ctx, cfg.childWfID, "", "continue-as-new", nil))

	// The callback fires → nexus operation completes → caller workflow finishes.
	// The caller should get an error (the nexus operation failed because the
	// target continued as new and the update was aborted).
	var result string
	err = callerRun.Get(ctx, &result)
	s.Error(err, "expected caller workflow to fail because the target continued as new")
}

// TestWorkflowUpdateCallbackOnWorkflowFailedWithRetry verifies that when a workflow
// fails with a retry policy (RetryState=IN_PROGRESS) while an update with completion
// callbacks is in-flight (accepted, handler blocking), the update callbacks are fired
// and the caller's nexus operation completes with a failure (the old run is closed).
func (s *NexusWorkflowUpdateTestSuite) TestWorkflowUpdateCallbackOnWorkflowFailedWithRetry() {
	s.enableUpdateCallbacks()
	ctx := testcore.NewContext()
	cfg := newUpdateNexusTestConfig(s.T())
	cfg.updateID = "failed-retry-update-id"

	h := s.makeUpdateWithCallbackHandler(cfg, nil)
	s.setupExternalNexusEndpoint(ctx, cfg.endpointName, h)

	targetTaskQueue := testcore.RandomizeStr("target-" + s.T().Name())

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

	targetWorker := worker.New(s.SdkClient(), targetTaskQueue, worker.Options{})
	targetWorker.RegisterWorkflow(targetWF)
	s.NoError(targetWorker.Start())
	s.T().Cleanup(targetWorker.Stop)

	_, err := s.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        cfg.childWfID,
		TaskQueue: targetTaskQueue,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    1 * time.Second,
			MaximumAttempts:    3,
			BackoffCoefficient: 1,
		},
	}, targetWF, "initial input")
	s.NoError(err)

	// Caller workflow sends a nexus operation targeting the child.
	callerWF := func(ctx workflow.Context) (string, error) {
		nexusClient := workflow.NewNexusClient(cfg.endpointName, "test")
		fut := nexusClient.ExecuteOperation(ctx, "operation", cfg.childWfID, workflow.NexusOperationOptions{})
		var result string
		err := fut.Get(ctx, &result)
		return result, err
	}

	callerWorker := worker.New(s.SdkClient(), cfg.taskQueue, worker.Options{})
	callerWorker.RegisterWorkflow(callerWF)
	s.NoError(callerWorker.Start())
	s.T().Cleanup(callerWorker.Stop)

	callerRun, err := s.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		TaskQueue:                cfg.taskQueue,
		WorkflowExecutionTimeout: 30 * time.Second,
	}, callerWF)
	s.NoError(err)

	// Wait for the update to be accepted on the target.
	s.EventuallyWithT(func(t *assert.CollectT) {
		hist := s.SdkClient().GetWorkflowHistory(ctx, cfg.childWfID, "", false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
		for hist.HasNext() {
			event, err := hist.Next()
			require.NoError(t, err)
			if event.EventType == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED {
				return
			}
		}
		require.Fail(t, "update not yet accepted")
	}, 10*time.Second, 500*time.Millisecond)

	// Signal the target workflow to fail while the update is in-flight.
	// The retry policy will cause a new run to be created.
	s.NoError(s.SdkClient().SignalWorkflow(ctx, cfg.childWfID, "", "fail", nil))

	// The callback fires → nexus operation completes → caller workflow finishes.
	// The caller should get an error (the nexus operation failed because the
	// target failed and the update was aborted).
	var result string
	err = callerRun.Get(ctx, &result)
	s.Error(err, "expected caller workflow to fail because the target workflow failed with retry")
}

// TestWorkflowUpdateCallbackOnRejectedUpdate verifies that when an update is rejected
// by the workflow's validator, the nexus handler detects the rejection (which is returned
// as a completed update with a failure outcome) and returns a synchronous failure to the
// caller. This tests the proper handling of rejection in the callback flow.
func (s *NexusWorkflowUpdateTestSuite) TestWorkflowUpdateCallbackOnRejectedUpdate() {
	s.enableUpdateCallbacks()
	ctx := testcore.NewContext()
	cfg := newUpdateNexusTestConfig(s.T())
	cfg.updateID = "rejected-update-id"

	h := s.makeUpdateWithCallbackHandler(cfg, nil)
	s.setupExternalNexusEndpoint(ctx, cfg.endpointName, h)

	targetTaskQueue := testcore.RandomizeStr("target-" + s.T().Name())

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

	targetWorker := worker.New(s.SdkClient(), targetTaskQueue, worker.Options{})
	targetWorker.RegisterWorkflow(targetWF)
	s.NoError(targetWorker.Start())
	s.T().Cleanup(targetWorker.Stop)

	_, err := s.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        cfg.childWfID,
		TaskQueue: targetTaskQueue,
	}, targetWF, "initial input")
	s.NoError(err)

	// Caller workflow sends a nexus operation targeting the child.
	callerWF := func(ctx workflow.Context) (string, error) {
		nexusClient := workflow.NewNexusClient(cfg.endpointName, "test")
		fut := nexusClient.ExecuteOperation(ctx, "operation", cfg.childWfID, workflow.NexusOperationOptions{})
		var result string
		err := fut.Get(ctx, &result)
		return result, err
	}

	callerWorker := worker.New(s.SdkClient(), cfg.taskQueue, worker.Options{})
	callerWorker.RegisterWorkflow(callerWF)
	s.NoError(callerWorker.Start())
	s.T().Cleanup(callerWorker.Stop)

	callerRun, err := s.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		TaskQueue:                cfg.taskQueue,
		WorkflowExecutionTimeout: 30 * time.Second,
	}, callerWF)
	s.NoError(err)

	// The update is rejected by the validator → nexus handler detects rejection and
	// returns sync failure → nexus operation fails → caller workflow fails.
	var result string
	err = callerRun.Get(ctx, &result)
	s.Error(err, "expected caller workflow to fail because the update was rejected")

	// Verify it's a NexusOperationError containing the rejection message.
	var wee *temporal.WorkflowExecutionError
	s.ErrorAs(err, &wee)
	var noe *temporal.NexusOperationError
	s.ErrorAs(wee, &noe)
	s.Contains(noe.Error(), "update rejected by validator")

	// Clean up: stop the target workflow.
	s.NoError(s.SdkClient().SignalWorkflow(ctx, cfg.childWfID, "", "stop", nil))
}

// TestWorkflowUpdateRequestIDInAcceptedEvent verifies that when an update request includes
// a RequestId, it is preserved in the WorkflowExecutionUpdateAccepted event's AcceptedRequest.
func (s *NexusWorkflowUpdateTestSuite) TestWorkflowUpdateRequestIDInAcceptedEvent() {
	s.enableUpdateCallbacks()
	ctx := testcore.NewContext()
	taskQueue := testcore.RandomizeStr(s.T().Name())
	updateID := "request-id-accepted-test"
	requestID := uuid.NewString()

	w := worker.New(s.SdkClient(), taskQueue, worker.Options{})
	wf := newUpdateChildWorkflow(false)

	w.RegisterWorkflow(wf)
	s.NoError(w.Start())
	s.T().Cleanup(w.Stop)

	run, err := s.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		TaskQueue: taskQueue,
	}, wf, "initial input")
	s.NoError(err)

	// Send an update with a specific RequestId and wait for completion.
	_, err = s.FrontendClient().UpdateWorkflowExecution(ctx, &workflowservice.UpdateWorkflowExecutionRequest{
		Namespace: s.Namespace().String(),
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
	hist := s.SdkClient().GetWorkflowHistory(ctx, run.GetID(), run.GetRunID(), false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
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
	s.NoError(s.SdkClient().SignalWorkflow(ctx, run.GetID(), run.GetRunID(), "stop", nil))
}
