package tests

import (
	"context"
	"errors"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/nexus/nexusrpc"
	"go.temporal.io/server/common/testing/protoassert"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/components/callbacks"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

type completionHandler struct {
	requestCh         chan *nexusrpc.CompletionRequest
	requestCompleteCh chan error
}

func (h *completionHandler) CompleteOperation(ctx context.Context, request *nexusrpc.CompletionRequest) error {
	h.requestCh <- request
	return <-h.requestCompleteCh
}

type CallbacksSuite struct {
	testcore.FunctionalTestBase

	chasmEnabled bool
}

func TestCallbacksSuiteHSM(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(CallbacksSuite))
}

func TestCallbacksSuiteCHASM(t *testing.T) {
	t.Parallel()
	suite.Run(t, &CallbacksSuite{chasmEnabled: true})
}

func (s *CallbacksSuite) SetupSuite() {
	s.SetupSuiteWithCluster(
		testcore.WithDynamicConfigOverrides(map[dynamicconfig.Key]any{
			dynamicconfig.EnableChasm.Key():          s.chasmEnabled,
			dynamicconfig.EnableCHASMCallbacks.Key(): s.chasmEnabled,
		}),
	)
}

func (s *CallbacksSuite) runNexusCompletionHTTPServer(t *testing.T, h *completionHandler) string {
	hh := nexusrpc.NewCompletionHTTPHandler(nexusrpc.CompletionHandlerOptions{Handler: h})
	srv := httptest.NewServer(hh)
	t.Cleanup(func() {
		srv.Close()
	})
	return srv.URL
}

func (s *CallbacksSuite) TestWorkflowCallbacks_InvalidArgument() {
	ctx := testcore.NewContext()
	taskQueue := testcore.RandomizeStr(s.T().Name())
	workflowType := "test"

	cases := []struct {
		name    string
		urls    []string
		header  map[string]string
		message string
		allow   bool
	}{
		{
			name:    "disabled",
			urls:    []string{"http://some-ignored-address"},
			allow:   false,
			message: "attaching workflow callbacks is disabled for this namespace",
		},
		{
			name:    "invalid-scheme",
			urls:    []string{"invalid"},
			allow:   true,
			message: "invalid url: unknown scheme: invalid",
		},
		{
			name:    "url-length-too-long",
			urls:    []string{"http://some-very-very-very-very-very-very-very-long-url"},
			allow:   true,
			message: "invalid url: url length longer than max length allowed of 50",
		},
		{
			name:    "header-size-too-large",
			urls:    []string{"http://some-ignored-address"},
			header:  map[string]string{"too": "long"},
			allow:   true,
			message: "invalid header: header size longer than max allowed size of 6",
		},
		{
			name:    "too many callbacks",
			urls:    []string{"http://url-1", "http://url-2", "http://url-3"},
			allow:   true,
			message: "cannot attach more than 2 callbacks to a workflow",
		},
		{
			name:    "url not configured",
			urls:    []string{"http://some-unconfigured-address"},
			allow:   true,
			message: "invalid url: url does not match any configured callback address: http://some-unconfigured-address",
		},
		{
			name:    "https required",
			urls:    []string{"http://some-secure-address"},
			allow:   true,
			message: "invalid url: callback address does not allow insecure connections: http://some-secure-address",
		},
	}

	s.OverrideDynamicConfig(dynamicconfig.FrontendCallbackURLMaxLength, 50)
	s.OverrideDynamicConfig(dynamicconfig.FrontendCallbackHeaderMaxSize, 6)
	s.OverrideDynamicConfig(dynamicconfig.MaxCallbacksPerWorkflow, 2)
	s.OverrideDynamicConfig(
		callbacks.AllowedAddresses,
		[]any{map[string]any{"Pattern": "some-ignored-address", "AllowInsecure": true}, map[string]any{"Pattern": "some-secure-address", "AllowInsecure": false}},
	)

	for _, tc := range cases {
		s.Run(tc.name, func() {
			s.OverrideDynamicConfig(dynamicconfig.EnableNexus, tc.allow)
			cbs := make([]*commonpb.Callback, 0, len(tc.urls))
			for _, url := range tc.urls {
				cbs = append(cbs, &commonpb.Callback{
					Variant: &commonpb.Callback_Nexus_{
						Nexus: &commonpb.Callback_Nexus{
							Url:    url,
							Header: tc.header,
						},
					},
				})
			}
			request := &workflowservice.StartWorkflowExecutionRequest{
				RequestId:           uuid.NewString(),
				Namespace:           s.Namespace().String(),
				WorkflowId:          testcore.RandomizeStr(s.T().Name()),
				WorkflowType:        &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
				Input:               nil,
				WorkflowRunTimeout:  durationpb.New(100 * time.Second),
				Identity:            s.T().Name(),
				CompletionCallbacks: cbs,
			}

			_, err := s.FrontendClient().StartWorkflowExecution(ctx, request)
			var invalidArgument *serviceerror.InvalidArgument
			s.ErrorAs(err, &invalidArgument)
			s.Equal(tc.message, err.Error())
		})
	}
}

func (s *CallbacksSuite) TestWorkflowNexusCallbacks_CarriedOver() {
	s.OverrideDynamicConfig(
		callbacks.AllowedAddresses,
		[]any{map[string]any{"Pattern": "*", "AllowInsecure": true}},
	)

	cases := []struct {
		name       string
		wf         func(workflow.Context) (int, error)
		runTimeout time.Duration
	}{
		{
			name: "ContinueAsNew",
			wf: func(ctx workflow.Context) (int, error) {
				if workflow.GetInfo(ctx).ContinuedExecutionRunID == "" {
					workflow.GetSignalChannel(ctx, "continue").Receive(ctx, nil)
					return 0, workflow.NewContinueAsNewError(ctx, "test")
				}
				return 666, nil
			},
			runTimeout: 100 * time.Second,
		},
		{
			name: "WorkflowRunTimeout",
			wf: func(ctx workflow.Context) (int, error) {
				info := workflow.GetInfo(ctx)
				if info.FirstRunID == info.WorkflowExecution.RunID {
					workflow.GetSignalChannel(ctx, "continue").Receive(ctx, nil)
					return 0, workflow.Sleep(ctx, 10*time.Second)
				}
				s.Greater(info.Attempt, int32(1))
				return 666, nil
			},
			runTimeout: 500 * time.Millisecond,
		},
		{
			name: "WorkflowFailureRetry",
			wf: func(ctx workflow.Context) (int, error) {
				info := workflow.GetInfo(ctx)
				if info.FirstRunID == info.WorkflowExecution.RunID {
					workflow.GetSignalChannel(ctx, "continue").Receive(ctx, nil)
					return 0, errors.New("intentional workflow failure")
				}
				s.Greater(info.Attempt, int32(1))
				return 666, nil
			},
			runTimeout: 100 * time.Second,
		},
	}

	for _, tc := range cases {
		s.Run(tc.name, func() {
			tv := testvars.New(s.T())
			ctx := testcore.NewContext()
			sdkClient, err := client.Dial(client.Options{
				HostPort:  s.FrontendGRPCAddress(),
				Namespace: s.Namespace().String(),
			})
			s.NoError(err)

			taskQueue := testcore.RandomizeStr(s.T().Name())
			workflowType := "test"
			workflowID := tv.WorkflowID()

			ch := &completionHandler{
				requestCh:         make(chan *nexusrpc.CompletionRequest, 2),
				requestCompleteCh: make(chan error, 2),
			}
			defer func() {
				close(ch.requestCh)
				close(ch.requestCompleteCh)
			}()
			callbackAddress := s.runNexusCompletionHTTPServer(s.T(), ch)

			w := worker.New(sdkClient, taskQueue, worker.Options{})
			w.RegisterWorkflowWithOptions(tc.wf, workflow.RegisterOptions{Name: workflowType})
			s.NoError(w.Start())
			defer w.Stop()

			links := []*commonpb.Link{
				{
					Variant: &commonpb.Link_WorkflowEvent_{
						WorkflowEvent: &commonpb.Link_WorkflowEvent{
							Namespace:  s.Namespace().String(),
							WorkflowId: "some-caller-wfid-1",
							RunId:      "some-caller-runid-1",
						},
					},
				},
				{
					Variant: &commonpb.Link_WorkflowEvent_{
						WorkflowEvent: &commonpb.Link_WorkflowEvent{
							Namespace:  s.Namespace().String(),
							WorkflowId: "some-caller-wfid-2",
							RunId:      "some-caller-runid-2",
						},
					},
				},
			}

			cbs := []*commonpb.Callback{
				{
					Variant: &commonpb.Callback_Nexus_{
						Nexus: &commonpb.Callback_Nexus{
							Url: callbackAddress + "/cb1",
						},
					},
					Links: []*commonpb.Link{links[0]},
				},
				{
					Variant: &commonpb.Callback_Nexus_{
						Nexus: &commonpb.Callback_Nexus{
							Url: callbackAddress + "/cb2",
						},
					},
					Links: []*commonpb.Link{links[1]},
				},
			}

			request := &workflowservice.StartWorkflowExecutionRequest{
				RequestId:          uuid.NewString(),
				Namespace:          s.Namespace().String(),
				WorkflowId:         workflowID,
				WorkflowType:       &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:          &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
				Input:              nil,
				WorkflowRunTimeout: durationpb.New(tc.runTimeout),
				Identity:           s.T().Name(),
				RetryPolicy: &commonpb.RetryPolicy{
					InitialInterval:    durationpb.New(1 * time.Second),
					MaximumInterval:    durationpb.New(1 * time.Second),
					BackoffCoefficient: 1,
				},
				CompletionCallbacks: []*commonpb.Callback{cbs[0]},
				Links:               []*commonpb.Link{links[0]},
			}

			response1, err := s.FrontendClient().StartWorkflowExecution(ctx, request)
			s.NoError(err)

			workflowExecution := &commonpb.WorkflowExecution{
				WorkflowId: workflowID,
				RunId:      response1.RunId,
			}

			// Send another request to attach callback
			request2 := proto.Clone(request).(*workflowservice.StartWorkflowExecutionRequest)
			request2.RequestId = uuid.NewString()
			request2.WorkflowIdConflictPolicy = enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING
			request2.OnConflictOptions = &workflowpb.OnConflictOptions{
				AttachRequestId:           true,
				AttachCompletionCallbacks: true,
			}
			request2.CompletionCallbacks = []*commonpb.Callback{cbs[1]}
			request2.Links = []*commonpb.Link{links[1]}

			response2, err := s.FrontendClient().StartWorkflowExecution(ctx, request2)
			s.NoError(err)
			s.False(response2.Started)
			s.Equal(workflowExecution.RunId, response2.RunId)

			_, err = s.FrontendClient().SignalWorkflowExecution(
				ctx,
				&workflowservice.SignalWorkflowExecutionRequest{
					Namespace:         s.Namespace().String(),
					WorkflowExecution: workflowExecution,
					SignalName:        "continue",
				},
			)
			s.NoError(err)

			// Wait for workflow to complete.
			run := sdkClient.GetWorkflow(ctx, workflowID, "")
			s.NoError(run.Get(ctx, nil))

			numAttempts := 2
			for attempt := 1; attempt <= numAttempts; attempt++ {
				for range cbs {
					completion := <-ch.requestCh
					s.Equal(nexus.OperationStateSucceeded, completion.State)
					var result int
					s.NoError(completion.Result.Consume(&result))
					s.Equal(666, result)
				}

				for range cbs {
					var err error
					if attempt < numAttempts {
						// force retry
						err = nexus.HandlerErrorf(nexus.HandlerErrorTypeInternal, "intentional error")
					}
					ch.requestCompleteCh <- err
				}

				getHistoryResponse, err := s.FrontendClient().GetWorkflowExecutionHistory(
					ctx,
					&workflowservice.GetWorkflowExecutionHistoryRequest{
						Namespace: s.Namespace().String(),
						Execution: &commonpb.WorkflowExecution{
							WorkflowId: workflowID,
						},
						MaximumPageSize: 1, // only interested in the start event
					},
				)
				s.NoError(err)
				s.Len(getHistoryResponse.History.Events, 1)
				startEvent := getHistoryResponse.History.Events[0]
				// Start event links is empty since it's deduped.
				s.Empty(startEvent.Links)
				startEventAttr := startEvent.GetWorkflowExecutionStartedEventAttributes()
				s.NotNil(startEventAttr)
				// Start event contains all callbacks attached to the first workflow.
				s.ProtoElementsMatch(cbs, startEventAttr.CompletionCallbacks)

				s.EventuallyWithT(func(col *assert.CollectT) {
					description, err := sdkClient.DescribeWorkflowExecution(ctx, workflowID, "")
					require.NoError(col, err)
					require.Equal(col, len(cbs), len(description.Callbacks))
					descCbs := make([]*commonpb.Callback, 0, len(description.Callbacks))
					for _, callbackInfo := range description.Callbacks {
						protorequire.ProtoEqual(
							col,
							&workflowpb.CallbackInfo_Trigger{
								Variant: &workflowpb.CallbackInfo_Trigger_WorkflowClosed{
									WorkflowClosed: &workflowpb.CallbackInfo_WorkflowClosed{},
								},
							},
							callbackInfo.Trigger,
						)
						require.Equal(col, int32(attempt), callbackInfo.Attempt)
						// Loose check to see that this is set.
						require.Greater(
							col,
							callbackInfo.LastAttemptCompleteTime.AsTime(),
							time.Now().Add(-time.Hour),
						)
						if attempt < numAttempts {
							require.Equal(col, enumspb.CALLBACK_STATE_BACKING_OFF, callbackInfo.State)
							require.Equal(
								col,
								"handler error (INTERNAL): intentional error",
								callbackInfo.LastAttemptFailure.Message,
							)
						} else {
							require.Equal(col, enumspb.CALLBACK_STATE_SUCCEEDED, callbackInfo.State)
							require.Nil(col, callbackInfo.LastAttemptFailure)
						}
						descCbs = append(descCbs, callbackInfo.Callback)
					}
					protoassert.ProtoElementsMatch(col, cbs, descCbs)
				}, 2*time.Second, 100*time.Millisecond)
			}
		})
	}
}

func (s *CallbacksSuite) TestNexusResetWorkflowWithCallback() {
	s.OverrideDynamicConfig(
		callbacks.AllowedAddresses,
		[]any{map[string]any{"Pattern": "*", "AllowInsecure": true}},
	)

	tv := testvars.New(s.T())
	ctx := testcore.NewContext()
	sdkClient, err := client.Dial(client.Options{
		HostPort:  s.FrontendGRPCAddress(),
		Namespace: s.Namespace().String(),
	})
	s.NoError(err)

	taskQueue := tv.TaskQueue()
	workflowID := tv.WorkflowID()

	ch := &completionHandler{
		requestCh:         make(chan *nexusrpc.CompletionRequest, 2),
		requestCompleteCh: make(chan error, 2),
	}
	defer func() {
		close(ch.requestCh)
		close(ch.requestCompleteCh)
	}()
	callbackAddress := s.runNexusCompletionHTTPServer(s.T(), ch)

	w := worker.New(sdkClient, taskQueue.GetName(), worker.Options{})

	// A workflow that completes once it has been reset.
	longRunningWorkflow := func(ctx workflow.Context) error {
		return workflow.Await(ctx, func() bool {
			info := workflow.GetInfo(ctx)

			return info.OriginalRunID != info.WorkflowExecution.RunID
		})
	}

	w.RegisterWorkflowWithOptions(longRunningWorkflow, workflow.RegisterOptions{
		Name: "longRunningWorkflow",
	})
	s.NoError(w.Start())
	defer w.Stop()

	cbs := []*commonpb.Callback{
		{
			Variant: &commonpb.Callback_Nexus_{
				Nexus: &commonpb.Callback_Nexus{
					Url: callbackAddress + "/cb1",
				},
			},
		},
		{
			Variant: &commonpb.Callback_Nexus_{
				Nexus: &commonpb.Callback_Nexus{
					Url: callbackAddress + "/cb2",
				},
			},
		},
	}

	request1 := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          workflowID,
		WorkflowType:        &commonpb.WorkflowType{Name: "longRunningWorkflow"},
		TaskQueue:           taskQueue,
		Input:               nil,
		Identity:            s.T().Name(),
		CompletionCallbacks: []*commonpb.Callback{cbs[0]},
	}

	startResponse1, err := s.FrontendClient().StartWorkflowExecution(ctx, request1)
	s.NoError(err)

	// Get history, iterate to ensure workflow task completed event exists.
	workflowExecution := &commonpb.WorkflowExecution{
		WorkflowId: workflowID,
		RunId:      startResponse1.RunId,
	}
	s.WaitForHistoryEvents(`
			1 WorkflowExecutionStarted
  			2 WorkflowTaskScheduled
  			3 WorkflowTaskStarted
  			4 WorkflowTaskCompleted`,
		s.GetHistoryFunc(s.Namespace().String(), workflowExecution),
		5*time.Second,
		10*time.Millisecond)

	// Try starting another workflow, which will have the callback attached to the previous workflow.
	request2 := proto.Clone(request1).(*workflowservice.StartWorkflowExecutionRequest)
	request2.RequestId = uuid.NewString()
	request2.WorkflowIdConflictPolicy = enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING
	request2.OnConflictOptions = &workflowpb.OnConflictOptions{
		AttachRequestId:           true,
		AttachCompletionCallbacks: true,
	}
	request2.CompletionCallbacks = []*commonpb.Callback{cbs[1]}

	startResponse2, err := s.FrontendClient().StartWorkflowExecution(ctx, request2)
	s.NoError(err)
	s.False(startResponse2.Started)
	s.Equal(workflowExecution.RunId, startResponse2.RunId)

	// Get history, iterate to ensure workflow execution options updated event exists.
	s.WaitForHistoryEvents(`
			1 WorkflowExecutionStarted
  			2 WorkflowTaskScheduled
  			3 WorkflowTaskStarted
  			4 WorkflowTaskCompleted
     		5 WorkflowExecutionOptionsUpdated`,
		s.GetHistoryFunc(s.Namespace().String(), workflowExecution),
		5*time.Second,
		10*time.Millisecond)

	// Reset workflow must copy all callbacks even after the reset point.
	resetWfResponse, err := sdkClient.ResetWorkflowExecution(ctx, &workflowservice.ResetWorkflowExecutionRequest{
		Namespace: s.Namespace().String(),

		WorkflowExecution:         workflowExecution,
		Reason:                    "TestNexusResetWorkflowWithCallback",
		WorkflowTaskFinishEventId: 3,
		RequestId:                 "test_id",
	})
	s.NoError(err)

	// Get the description of the run that was reset and ensure that its callback is still in STANDBY state.
	description, err := sdkClient.DescribeWorkflowExecution(ctx, workflowID, startResponse1.RunId)
	s.NoError(err)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED, description.WorkflowExecutionInfo.Status)

	// Should not be invoked during a reset
	s.Equal(len(cbs), len(description.Callbacks))
	descCbs := make([]*commonpb.Callback, 0, len(description.Callbacks))
	for _, callbackInfo := range description.Callbacks {
		s.Equal(enumspb.CALLBACK_STATE_STANDBY, callbackInfo.State)
		s.Equal(int32(0), callbackInfo.Attempt)
		descCbs = append(descCbs, callbackInfo.Callback)
	}
	s.ProtoElementsMatch(cbs, descCbs)

	resetWorkflowRun := sdkClient.GetWorkflow(ctx, workflowID, resetWfResponse.RunId)
	err = resetWorkflowRun.Get(ctx, nil)
	s.NoError(err)

	for range cbs {
		select {
		case completion := <-ch.requestCh:
			s.Equal(nexus.OperationStateSucceeded, completion.State)
		case <-time.After(time.Second):
			s.Fail("timeout waiting for callback")
		}
		select {
		case ch.requestCompleteCh <- nil:
		case <-time.After(time.Second):
			s.Fail("timeout writing to completion channel")
		}
	}

	s.EventuallyWithT(
		func(t *assert.CollectT) {
			// Get the description of the run post-reset and ensure its callbacks are in SUCCEEDED
			// state.
			description, err = sdkClient.DescribeWorkflowExecution(ctx, resetWorkflowRun.GetID(), "")
			require.NoError(t, err)
			require.Equal(
				t,
				enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
				description.WorkflowExecutionInfo.Status,
			)

			require.Equal(t, len(cbs), len(description.Callbacks))
			descCbs = make([]*commonpb.Callback, 0, len(description.Callbacks))
			for _, callbackInfo := range description.Callbacks {
				require.Equal(t, enumspb.CALLBACK_STATE_SUCCEEDED, callbackInfo.State)
				descCbs = append(descCbs, callbackInfo.Callback)
			}
			protoassert.ProtoElementsMatch(t, cbs, descCbs)
		},
		2*time.Second,
		100*time.Millisecond,
	)
}

func blockingWorkflow(ctx workflow.Context) error {
	return workflow.Await(ctx, func() bool {
		return false
	})
}

func (s *CallbacksSuite) TestNexusResetWorkflowWithCallback_ResetToNotBaseRun() {
	s.OverrideDynamicConfig(
		callbacks.AllowedAddresses,
		[]any{map[string]any{"Pattern": "*", "AllowInsecure": true}},
	)

	/*
	 * 1. Start WF w/ no callbacks and immediately terminate
	 * 2. Start WF second time w/ a callback
	 * 3. Reset WF back to the first run
	 * 4. Verify callback is called
	 */

	tv := testvars.New(s.T())
	ctx := testcore.NewContext()
	sdkClient, err := client.Dial(client.Options{
		HostPort:  s.FrontendGRPCAddress(),
		Namespace: s.Namespace().String(),
	})
	s.NoError(err)

	taskQueue := tv.TaskQueue()
	workflowID := tv.WorkflowID()

	ch := &completionHandler{
		requestCh:         make(chan *nexusrpc.CompletionRequest, 1),
		requestCompleteCh: make(chan error, 1),
	}
	defer func() {
		close(ch.requestCh)
		close(ch.requestCompleteCh)
	}()
	callbackAddress := s.runNexusCompletionHTTPServer(s.T(), ch)

	w := worker.New(sdkClient, taskQueue.GetName(), worker.Options{})

	w.RegisterWorkflow(blockingWorkflow)
	s.NoError(w.Start())
	defer w.Stop()

	// 1. Start WF w/ no callbacks and immediately terminate
	request1 := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:          uuid.NewString(),
		Namespace:          s.Namespace().String(),
		WorkflowId:         workflowID,
		WorkflowType:       &commonpb.WorkflowType{Name: "blockingWorkflow"},
		TaskQueue:          taskQueue,
		Input:              nil,
		WorkflowRunTimeout: durationpb.New(20 * time.Second),
		Identity:           s.T().Name(),
	}

	startResponse1, err := s.FrontendClient().StartWorkflowExecution(ctx, request1)
	s.NoError(err)

	// Validate the workflow started, then terminate it
	workflowExecution := &commonpb.WorkflowExecution{
		WorkflowId: workflowID,
		RunId:      startResponse1.RunId,
	}
	s.WaitForHistoryEvents(`
			1 WorkflowExecutionStarted
			2 WorkflowTaskScheduled
			3 WorkflowTaskStarted
			4 WorkflowTaskCompleted`,
		s.GetHistoryFunc(s.Namespace().String(), workflowExecution),
		5*time.Second,
		10*time.Millisecond)

	_, err = s.FrontendClient().TerminateWorkflowExecution(ctx, &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace:         s.Namespace().String(),
		WorkflowExecution: workflowExecution,
		Reason:            s.T().Name(),
		Identity:          tv.WorkerIdentity(),
	})
	s.NoError(err)

	// 2. Start WF second time w/ callbacks (new run)
	cbs := []*commonpb.Callback{
		{Variant: &commonpb.Callback_Nexus_{Nexus: &commonpb.Callback_Nexus{Url: callbackAddress + "/cb1"}}},
	}

	request2 := proto.Clone(request1).(*workflowservice.StartWorkflowExecutionRequest)
	request2.RequestId = uuid.NewString()
	request2.CompletionCallbacks = cbs

	_, err = s.FrontendClient().StartWorkflowExecution(ctx, request2)
	s.NoError(err)

	// 3. Reset workflow back to the first (terminated) run as base; must copy callbacks
	_, err = sdkClient.ResetWorkflowExecution(ctx, &workflowservice.ResetWorkflowExecutionRequest{
		Namespace:                 s.Namespace().String(),
		WorkflowExecution:         workflowExecution, // base = first (terminated) run
		Reason:                    s.T().Name(),
		WorkflowTaskFinishEventId: 4,
		RequestId:                 "test_id",
	})
	s.NoError(err)

	// 4. Wait for callback deliveries via the handler channels
	select {
	case completion := <-ch.requestCh:
		s.Equal(nexus.OperationStateFailed, completion.State)
		ch.requestCompleteCh <- nil
	case <-ctx.Done():
		s.FailNow("timed out waiting for callback")
	}

	// Ensure the original workflow runs to completion to avoid leaving dangling runs
	_, err = s.FrontendClient().TerminateWorkflowExecution(ctx, &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace: s.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
		},
		Reason:   s.T().Name(),
		Identity: tv.WorkerIdentity(),
	})
	s.NoError(err)
}
