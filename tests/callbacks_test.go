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

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/pborman/uuid"
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
	"go.temporal.io/server/components/callbacks"
	"go.temporal.io/server/internal/temporalite"
	"go.temporal.io/server/tests/testcore"
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
	testcore.FunctionalSuite
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
				RequestId:           uuid.New(),
				Namespace:           s.Namespace(),
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
	s.OverrideDynamicConfig(dynamicconfig.EnableNexus, true)
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
					return 0, workflow.Sleep(ctx, 1*time.Second)
				}
				s.Greater(info.Attempt, int32(1))
				return 666, nil
			},
			runTimeout: 100 * time.Millisecond,
		},
		{
			name: "WorkflowFailureRetry",
			wf: func(ctx workflow.Context) (int, error) {
				info := workflow.GetInfo(ctx)
				if info.FirstRunID == info.WorkflowExecution.RunID {
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
			ctx := testcore.NewContext()
			sdkClient, err := client.Dial(client.Options{
				HostPort:  s.FrontendGRPCAddress(),
				Namespace: s.Namespace(),
			})
			s.NoError(err)
			pp := temporalite.NewPortProvider()

			taskQueue := testcore.RandomizeStr(s.T().Name())
			workflowType := "test"

			ch := &completionHandler{
				requestCh:         make(chan *nexus.CompletionRequest, 1),
				requestCompleteCh: make(chan error, 1),
			}
			callbackAddress := fmt.Sprintf("localhost:%d", pp.MustGetFreePort())
			s.NoError(pp.Close())
			shutdownServer := s.runNexusCompletionHTTPServer(ch, callbackAddress)
			t.Cleanup(func() {
				require.NoError(t, shutdownServer())
			})

			w := worker.New(sdkClient, taskQueue, worker.Options{})
			w.RegisterWorkflowWithOptions(tc.wf, workflow.RegisterOptions{Name: workflowType})
			s.NoError(w.Start())
			defer w.Stop()

			request := &workflowservice.StartWorkflowExecutionRequest{
				RequestId:          uuid.New(),
				Namespace:          s.Namespace(),
				WorkflowId:         testcore.RandomizeStr(s.T().Name()),
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
				CompletionCallbacks: []*commonpb.Callback{
					{
						Variant: &commonpb.Callback_Nexus_{
							Nexus: &commonpb.Callback_Nexus{
								Url: "http://" + callbackAddress,
							},
						},
					},
				},
			}

			_, err = s.FrontendClient().StartWorkflowExecution(ctx, request)
			s.NoError(err)

			run := sdkClient.GetWorkflow(ctx, request.WorkflowId, "")
			s.NoError(run.Get(ctx, nil))

			numAttempts := 2
			for attempt := 1; attempt <= numAttempts; attempt++ {
				completion := <-ch.requestCh
				s.Equal(nexus.OperationStateSucceeded, completion.State)
				var result int
				s.NoError(completion.Result.Consume(&result))
				s.Equal(666, result)
				var err error
				if attempt < numAttempts {
					// force retry
					err = nexus.HandlerErrorf(nexus.HandlerErrorTypeInternal, "intentional error")
				}
				ch.requestCompleteCh <- err
				description, err := sdkClient.DescribeWorkflowExecution(ctx, request.WorkflowId, "")
				s.NoError(err)
				s.Equal(1, len(description.Callbacks))
				callbackInfo := description.Callbacks[0]
				s.ProtoEqual(request.CompletionCallbacks[0], callbackInfo.Callback)
				s.ProtoEqual(&workflowpb.CallbackInfo_Trigger{Variant: &workflowpb.CallbackInfo_Trigger_WorkflowClosed{WorkflowClosed: &workflowpb.CallbackInfo_WorkflowClosed{}}}, callbackInfo.Trigger)
				s.Equal(int32(attempt), callbackInfo.Attempt)
				// Loose check to see that this is set.
				s.Greater(callbackInfo.LastAttemptCompleteTime.AsTime(), time.Now().Add(-time.Hour))
				if attempt < numAttempts {
					s.Equal(enumspb.CALLBACK_STATE_BACKING_OFF, callbackInfo.State)
					s.Equal("request failed with: 500 Internal Server Error", callbackInfo.LastAttemptFailure.Message)
				} else {
					s.Equal(enumspb.CALLBACK_STATE_SUCCEEDED, callbackInfo.State)
					s.Nil(callbackInfo.LastAttemptFailure)
				}
			}
		})
	}
}

func (s *CallbacksSuite) TestNexusResetWorkflowWithCallback() {
	s.OverrideDynamicConfig(dynamicconfig.EnableNexus, true)
	s.OverrideDynamicConfig(
		callbacks.AllowedAddresses,
		[]any{map[string]any{"Pattern": "*", "AllowInsecure": true}},
	)

	ctx := testcore.NewContext()
	sdkClient, err := client.Dial(client.Options{
		HostPort:  s.FrontendGRPCAddress(),
		Namespace: s.Namespace(),
	})
	s.NoError(err)
	pp := temporalite.NewPortProvider()

	taskQueue := testcore.RandomizeStr(s.T().Name())

	ch := &completionHandler{
		requestCh:         make(chan *nexus.CompletionRequest, 1),
		requestCompleteCh: make(chan error, 1),
	}
	callbackAddress := fmt.Sprintf("localhost:%d", pp.MustGetFreePort())
	s.NoError(pp.Close())
	shutdownServer := s.runNexusCompletionHTTPServer(ch, callbackAddress)
	s.T().Cleanup(func() {
		require.NoError(s.T(), shutdownServer())
	})

	w := worker.New(sdkClient, taskQueue, worker.Options{})

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

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:          uuid.New(),
		Namespace:          s.Namespace(),
		WorkflowId:         testcore.RandomizeStr(s.T().Name()),
		WorkflowType:       &commonpb.WorkflowType{Name: "longRunningWorkflow"},
		TaskQueue:          &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Input:              nil,
		WorkflowRunTimeout: durationpb.New(20 * time.Second),
		Identity:           s.T().Name(),
		CompletionCallbacks: []*commonpb.Callback{
			{
				Variant: &commonpb.Callback_Nexus_{
					Nexus: &commonpb.Callback_Nexus{
						Url: "http://" + callbackAddress,
					},
				},
			},
		},
	}

	startResponse, err := s.FrontendClient().StartWorkflowExecution(ctx, request)
	s.NoError(err)

	// Get history, iterate to ensure workflow task completed event exists. then reset
	workflowExecution := &commonpb.WorkflowExecution{
		WorkflowId: request.WorkflowId,
		RunId:      startResponse.RunId,
	}
	s.WaitForHistoryEvents(`
			1 WorkflowExecutionStarted
  			2 WorkflowTaskScheduled
  			3 WorkflowTaskStarted
  			4 WorkflowTaskCompleted`,
		s.GetHistoryFunc(s.Namespace(), workflowExecution),
		5*time.Second,
		10*time.Millisecond)

	resetWfResponse, err := sdkClient.ResetWorkflowExecution(ctx, &workflowservice.ResetWorkflowExecutionRequest{
		Namespace: s.Namespace(),

		WorkflowExecution:         workflowExecution,
		Reason:                    "TestNexusResetWorkflowWithCallback",
		WorkflowTaskFinishEventId: 3,
		RequestId:                 "test_id",
	})
	s.NoError(err)

	// Get the description of the run that was reset and ensure that its callback is still in STANDBY state.
	description, err := sdkClient.DescribeWorkflowExecution(ctx, request.WorkflowId, startResponse.RunId)
	s.NoError(err)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED, description.WorkflowExecutionInfo.Status)

	// Should not be invoked during a reset
	s.Equal(1, len(description.Callbacks))
	callbackInfo := description.Callbacks[0]
	s.ProtoEqual(request.CompletionCallbacks[0], callbackInfo.Callback)
	s.Equal(enumspb.CALLBACK_STATE_STANDBY, callbackInfo.State)
	s.Equal(int32(0), callbackInfo.Attempt)

	resetWorkflowRun := sdkClient.GetWorkflow(ctx, request.WorkflowId, resetWfResponse.RunId)
	err = resetWorkflowRun.Get(ctx, nil)
	s.NoError(err)

	completion := <-ch.requestCh
	s.Equal(nexus.OperationStateSucceeded, completion.State)
	ch.requestCompleteCh <- err

	// Get the description of the run post-reset and ensure that its callback is in SUCCEEDED state.
	description, err = sdkClient.DescribeWorkflowExecution(ctx, resetWorkflowRun.GetID(), "")
	s.NoError(err)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, description.WorkflowExecutionInfo.Status)

	s.Equal(1, len(description.Callbacks))
	callbackInfo = description.Callbacks[0]
	s.ProtoEqual(request.CompletionCallbacks[0], callbackInfo.Callback)
	s.Equal(enumspb.CALLBACK_STATE_SUCCEEDED, callbackInfo.State)
}
