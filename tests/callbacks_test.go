// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package tests

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
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
	"go.temporal.io/server/common/testing/freeport"
	"go.temporal.io/server/common/testing/protoassert"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/components/callbacks"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

type completionHandler struct {
	requestCh         chan *nexus.CompletionRequest
	requestCompleteCh chan error
}

func (h *completionHandler) CompleteOperation(ctx context.Context, request *nexus.CompletionRequest) error {
	h.requestCh <- request
	return <-h.requestCompleteCh
}

type CallbacksSuite struct {
	testcore.FunctionalTestSuite
}

func TestCallbacksSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(CallbacksSuite))
}

func (s *CallbacksSuite) runNexusCompletionHTTPServer(h *completionHandler, listenAddr string) func() error {
	hh := nexus.NewCompletionHTTPHandler(nexus.CompletionHandlerOptions{Handler: h})
	srv := &http.Server{Addr: listenAddr, Handler: hh}
	listener, err := net.Listen("tcp", listenAddr)
	s.NoError(err)

	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.Serve(listener)
	}()

	return func() error {
		// Graceful shutdown
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(ctx); err != nil {
			return err
		}
		if err := <-errCh; err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	}
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
		s.T().Run(tc.name, func(t *testing.T) {
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
		s.T().Run(tc.name, func(t *testing.T) {
			tv := testvars.New(t)
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
				requestCh:         make(chan *nexus.CompletionRequest, 2),
				requestCompleteCh: make(chan error, 2),
			}
			callbackAddress := fmt.Sprintf("localhost:%d", freeport.MustGetFreePort())
			shutdownServer := s.runNexusCompletionHTTPServer(ch, callbackAddress)
			t.Cleanup(func() {
				require.NoError(t, shutdownServer())
			})

			w := worker.New(sdkClient, taskQueue, worker.Options{})
			w.RegisterWorkflowWithOptions(tc.wf, workflow.RegisterOptions{Name: workflowType})
			s.NoError(w.Start())
			defer w.Stop()

			cbs := []*commonpb.Callback{
				{
					Variant: &commonpb.Callback_Nexus_{
						Nexus: &commonpb.Callback_Nexus{
							Url: "http://" + callbackAddress + "/cb1",
						},
					},
				},
				{
					Variant: &commonpb.Callback_Nexus_{
						Nexus: &commonpb.Callback_Nexus{
							Url: "http://" + callbackAddress + "/cb2",
						},
					},
				},
			}

			startLink := &commonpb.Link{
				Variant: &commonpb.Link_WorkflowEvent_{
					WorkflowEvent: &commonpb.Link_WorkflowEvent{
						Namespace:  s.Namespace().String(),
						WorkflowId: "some-caller-wfid",
						RunId:      "some-caller-runid",
						Reference: &commonpb.Link_WorkflowEvent_EventRef{
							EventRef: &commonpb.Link_WorkflowEvent_EventReference{
								EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
							},
						},
					},
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
				Links:               []*commonpb.Link{startLink},
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
				// Also checks workflow execution started event links are copied over.
				s.ProtoElementsMatch(request.Links, startEvent.Links)
				startEventAttr := startEvent.GetWorkflowExecutionStartedEventAttributes()
				s.NotNil(startEventAttr)
				// Start event contains all callbacks attached to the first workflow.
				s.ProtoElementsMatch(cbs, startEventAttr.CompletionCallbacks)

				s.EventuallyWithT(func(col *assert.CollectT) {
					description, err := sdkClient.DescribeWorkflowExecution(ctx, workflowID, "")
					assert.NoError(col, err)
					assert.Equal(col, len(cbs), len(description.Callbacks))
					descCbs := make([]*commonpb.Callback, 0, len(description.Callbacks))
					for _, callbackInfo := range description.Callbacks {
						protoassert.ProtoEqual(
							col,
							&workflowpb.CallbackInfo_Trigger{
								Variant: &workflowpb.CallbackInfo_Trigger_WorkflowClosed{
									WorkflowClosed: &workflowpb.CallbackInfo_WorkflowClosed{},
								},
							},
							callbackInfo.Trigger,
						)
						if !assert.Equal(col, int32(attempt), callbackInfo.Attempt) {
							// Return early to avoid evaluating further assertions.
							return
						}
						// Loose check to see that this is set.
						assert.Greater(
							col,
							callbackInfo.LastAttemptCompleteTime.AsTime(),
							time.Now().Add(-time.Hour),
						)
						if attempt < numAttempts {
							assert.Equal(col, enumspb.CALLBACK_STATE_BACKING_OFF, callbackInfo.State)
							assert.Equal(
								col,
								"handler error (INTERNAL): intentional error",
								callbackInfo.LastAttemptFailure.Message,
							)
						} else {
							assert.Equal(col, enumspb.CALLBACK_STATE_SUCCEEDED, callbackInfo.State)
							assert.Nil(col, callbackInfo.LastAttemptFailure)
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
		requestCh:         make(chan *nexus.CompletionRequest, 2),
		requestCompleteCh: make(chan error, 2),
	}
	callbackAddress := fmt.Sprintf("localhost:%d", freeport.MustGetFreePort())
	shutdownServer := s.runNexusCompletionHTTPServer(ch, callbackAddress)
	s.T().Cleanup(func() {
		require.NoError(s.T(), shutdownServer())
	})

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
					Url: "http://" + callbackAddress + "/cb1",
				},
			},
		},
		{
			Variant: &commonpb.Callback_Nexus_{
				Nexus: &commonpb.Callback_Nexus{
					Url: "http://" + callbackAddress + "/cb2",
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
		WorkflowRunTimeout:  durationpb.New(20 * time.Second),
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
		completion := <-ch.requestCh
		s.Equal(nexus.OperationStateSucceeded, completion.State)
		ch.requestCompleteCh <- nil
	}

	s.EventuallyWithT(
		func(t *assert.CollectT) {
			// Get the description of the run post-reset and ensure its callbacks are in SUCCEEDED
			// state.
			description, err = sdkClient.DescribeWorkflowExecution(ctx, resetWorkflowRun.GetID(), "")
			assert.NoError(t, err)
			assert.Equal(
				t,
				enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
				description.WorkflowExecutionInfo.Status,
			)

			assert.Equal(t, len(cbs), len(description.Callbacks))
			descCbs = make([]*commonpb.Callback, 0, len(description.Callbacks))
			for _, callbackInfo := range description.Callbacks {
				assert.Equal(t, enumspb.CALLBACK_STATE_SUCCEEDED, callbackInfo.State)
				descCbs = append(descCbs, callbackInfo.Callback)
			}
			protoassert.ProtoElementsMatch(t, cbs, descCbs)
		},
		2*time.Second,
		100*time.Millisecond,
	)
}
