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
	"go.temporal.io/server/internal/temporalite"
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

func (s *FunctionalSuite) runNexusCompletionHTTPServer(h *completionHandler, listenAddr string) func() error {
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

func (s *FunctionalSuite) TestWorkflowCallbacks_InvalidArgument() {
	ctx := NewContext()
	taskQueue := s.randomizeStr(s.T().Name())
	workflowType := "test"

	cases := []struct {
		name    string
		urls    []string
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
			name:    "too many callbacks",
			urls:    []string{"http://url-1", "http://url-2", "http://url-3"},
			allow:   true,
			message: "cannot attach more than 2 callbacks to a workflow",
		},
	}

	dc := s.testCluster.host.dcClient
	dc.OverrideValue(s.T(), dynamicconfig.FrontendCallbackURLMaxLength, 50)
	dc.OverrideValue(s.T(), dynamicconfig.FrontendMaxCallbacksPerWorkflow, 2)
	defer dc.RemoveOverride(dynamicconfig.FrontendEnableCallbackAttachment)
	defer dc.RemoveOverride(dynamicconfig.FrontendCallbackURLMaxLength)
	defer dc.RemoveOverride(dynamicconfig.FrontendMaxCallbacksPerWorkflow)

	for _, tc := range cases {
		s.T().Run(tc.name, func(t *testing.T) {
			dc.OverrideValue(s.T(), dynamicconfig.FrontendEnableCallbackAttachment, tc.allow)
			cbs := make([]*commonpb.Callback, 0, len(tc.urls))
			for _, url := range tc.urls {
				cbs = append(cbs, &commonpb.Callback{
					Variant: &commonpb.Callback_Nexus_{
						Nexus: &commonpb.Callback_Nexus{
							Url: url,
						},
					},
				})
			}
			request := &workflowservice.StartWorkflowExecutionRequest{
				RequestId:           uuid.New(),
				Namespace:           s.namespace,
				WorkflowId:          s.randomizeStr(s.T().Name()),
				WorkflowType:        &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
				Input:               nil,
				WorkflowRunTimeout:  durationpb.New(100 * time.Second),
				Identity:            s.T().Name(),
				CompletionCallbacks: cbs,
			}

			_, err := s.engine.StartWorkflowExecution(ctx, request)
			var invalidArgument *serviceerror.InvalidArgument
			s.ErrorAs(err, &invalidArgument)
			s.Equal(tc.message, err.Error())
		})
	}
}

func (s *FunctionalSuite) TestWorkflowNexusCallbacks_CarriedOverContinueAsNew() {
	dc := s.testCluster.host.dcClient
	dc.OverrideValue(s.T(), dynamicconfig.FrontendEnableCallbackAttachment, true)
	defer dc.RemoveOverride(dynamicconfig.FrontendEnableCallbackAttachment)

	ctx := NewContext()
	sdkClient, err := client.Dial(client.Options{
		HostPort:  s.testCluster.GetHost().FrontendGRPCAddress(),
		Namespace: s.namespace,
	})
	s.NoError(err)
	pp := temporalite.NewPortProvider()

	taskQueue := s.randomizeStr(s.T().Name())
	workflowType := "test"

	w := worker.New(sdkClient, taskQueue, worker.Options{})
	wf := func(ctx workflow.Context) (int, error) {
		// Verify that the callback is carried over the CAN boundary.
		if workflow.GetInfo(ctx).ContinuedExecutionRunID == "" {
			return 0, workflow.NewContinueAsNewError(ctx, workflowType)
		}
		return 666, nil
	}
	ch := &completionHandler{
		requestCh:         make(chan *nexus.CompletionRequest, 1),
		requestCompleteCh: make(chan error, 1),
	}
	callbackAddress := fmt.Sprintf("localhost:%d", pp.MustGetFreePort())
	s.NoError(pp.Close())
	shutdownServer := s.runNexusCompletionHTTPServer(ch, callbackAddress)
	defer func() {
		err := shutdownServer()
		if err != nil {
			panic(err)
		}
	}()
	w.RegisterWorkflowWithOptions(wf, workflow.RegisterOptions{Name: workflowType})
	s.NoError(w.Start())
	defer w.Stop()

	// TODO: use sdkClient instead of directly calling the history engine when callbacks are exposed
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:          uuid.New(),
		Namespace:          s.namespace,
		WorkflowId:         s.randomizeStr(s.T().Name()),
		WorkflowType:       &commonpb.WorkflowType{Name: workflowType},
		TaskQueue:          &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Input:              nil,
		WorkflowRunTimeout: durationpb.New(100 * time.Second),
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

	_, err = s.engine.StartWorkflowExecution(ctx, request)
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
		if attempt < numAttempts {
			s.Equal(enumspb.CALLBACK_STATE_BACKING_OFF, callbackInfo.State)
			s.Equal("request failed with: 500 Internal Server Error", callbackInfo.LastAttemptFailure.Message)
		} else {
			s.Equal(enumspb.CALLBACK_STATE_SUCCEEDED, callbackInfo.State)
			s.Nil(callbackInfo.LastAttemptFailure)
		}
	}
}
