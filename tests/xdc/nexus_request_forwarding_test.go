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

package xdc

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"

	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	cnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/tests"
)

var op = nexus.NewOperationReference[string, string]("my-operation")

type NexusRequestForwardingSuite struct {
	xdcBaseSuite
}

func TestNexusRequestForwardingTestSuite(t *testing.T) {
	flag.Parse()
	suite.Run(t, new(NexusRequestForwardingSuite))
}

func (s *NexusRequestForwardingSuite) SetupSuite() {
	s.dynamicConfigOverrides = map[dynamicconfig.Key]any{
		// Make sure we don't hit the rate limiter in tests
		dynamicconfig.FrontendGlobalNamespaceNamespaceReplicationInducingAPIsRPS.Key(): 1000,
		dynamicconfig.EnableNexus.Key():                  true,
		dynamicconfig.RefreshNexusEndpointsMinWait.Key(): 1 * time.Millisecond,
	}
	s.setupSuite([]string{"nexus_request_forwarding_active", "nexus_request_forwarding_standby"})
}

func (s *NexusRequestForwardingSuite) SetupTest() {
	s.setupTest()
}

func (s *NexusRequestForwardingSuite) TearDownSuite() {
	s.tearDownSuite()
}

// Only tests dispatch by namespace+task_queue.
// TODO: Add test cases for dispatch by endpoint ID once endpoints support replication.
func (s *NexusRequestForwardingSuite) TestStartOperationForwardedFromStandbyToActive() {
	ns := s.createGlobalNamespace()

	testCases := []struct {
		name      string
		taskQueue string
		header    nexus.Header
		handler   func(*workflowservice.PollNexusTaskQueueResponse) (*nexuspb.Response, *nexuspb.HandlerError)
		assertion func(*testing.T, *nexus.ClientStartOperationResult[string], error, map[string][]*metricstest.CapturedRecording, map[string][]*metricstest.CapturedRecording)
	}{
		{
			name:      "success",
			taskQueue: fmt.Sprintf("%v-%v", "test-task-queue", uuid.New()),
			handler: func(res *workflowservice.PollNexusTaskQueueResponse) (*nexuspb.Response, *nexuspb.HandlerError) {
				s.Equal("true", res.Request.Header["xdc-redirection-api"])
				return &nexuspb.Response{
					Variant: &nexuspb.Response_StartOperation{
						StartOperation: &nexuspb.StartOperationResponse{
							Variant: &nexuspb.StartOperationResponse_SyncSuccess{
								SyncSuccess: &nexuspb.StartOperationResponse_Sync{
									Payload: res.Request.GetStartOperation().GetPayload()}}}},
				}, nil
			},
			assertion: func(t *testing.T, result *nexus.ClientStartOperationResult[string], retErr error, activeSnap map[string][]*metricstest.CapturedRecording, passiveSnap map[string][]*metricstest.CapturedRecording) {
				require.NoError(t, retErr)
				require.Equal(t, "input", result.Successful)
				requireExpectedMetricsCaptured(t, activeSnap, ns, "StartNexusOperation", "sync_success")
				requireExpectedMetricsCaptured(t, passiveSnap, ns, "StartNexusOperation", "request_forwarded")
			},
		},
		{
			name:      "operation error",
			taskQueue: fmt.Sprintf("%v-%v", "test-task-queue", uuid.New()),
			handler: func(res *workflowservice.PollNexusTaskQueueResponse) (*nexuspb.Response, *nexuspb.HandlerError) {
				s.Equal("true", res.Request.Header["xdc-redirection-api"])
				return &nexuspb.Response{
					Variant: &nexuspb.Response_StartOperation{
						StartOperation: &nexuspb.StartOperationResponse{
							Variant: &nexuspb.StartOperationResponse_OperationError{
								OperationError: &nexuspb.UnsuccessfulOperationError{
									OperationState: string(nexus.OperationStateFailed),
									Failure: &nexuspb.Failure{
										Message:  "deliberate test failure",
										Metadata: map[string]string{"k": "v"},
										Details:  []byte(`"details"`),
									}}}}},
				}, nil
			},
			assertion: func(t *testing.T, result *nexus.ClientStartOperationResult[string], retErr error, activeSnap map[string][]*metricstest.CapturedRecording, passiveSnap map[string][]*metricstest.CapturedRecording) {
				var operationError *nexus.UnsuccessfulOperationError
				require.ErrorAs(t, retErr, &operationError)
				require.Equal(t, nexus.OperationStateFailed, operationError.State)
				require.Equal(t, "deliberate test failure", operationError.Failure.Message)
				require.Equal(t, map[string]string{"k": "v"}, operationError.Failure.Metadata)
				var details string
				err := json.Unmarshal(operationError.Failure.Details, &details)
				require.NoError(t, err)
				require.Equal(t, "details", details)
				requireExpectedMetricsCaptured(t, activeSnap, ns, "StartNexusOperation", "operation_error")
				requireExpectedMetricsCaptured(t, passiveSnap, ns, "StartNexusOperation", "forwarded_request_error")
			},
		},
		{
			name:      "handler error",
			taskQueue: fmt.Sprintf("%v-%v", "test-task-queue", uuid.New()),
			handler: func(res *workflowservice.PollNexusTaskQueueResponse) (*nexuspb.Response, *nexuspb.HandlerError) {
				s.Equal("true", res.Request.Header["xdc-redirection-api"])
				return nil, &nexuspb.HandlerError{
					ErrorType: string(nexus.HandlerErrorTypeInternal),
					Failure:   &nexuspb.Failure{Message: "deliberate internal failure"},
				}
			},
			assertion: func(t *testing.T, result *nexus.ClientStartOperationResult[string], retErr error, activeSnap map[string][]*metricstest.CapturedRecording, passiveSnap map[string][]*metricstest.CapturedRecording) {
				var unexpectedError *nexus.UnexpectedResponseError
				require.ErrorAs(t, retErr, &unexpectedError)
				require.Equal(t, http.StatusInternalServerError, unexpectedError.Response.StatusCode)
				require.Equal(t, "worker", unexpectedError.Response.Header.Get("Temporal-Nexus-Failure-Source"))
				require.Equal(t, "deliberate internal failure", unexpectedError.Failure.Message)
				requireExpectedMetricsCaptured(t, activeSnap, ns, "StartNexusOperation", "handler_error")
				requireExpectedMetricsCaptured(t, passiveSnap, ns, "StartNexusOperation", "forwarded_request_error")
			},
		},
		{
			name:      "redirect disabled by header",
			taskQueue: fmt.Sprintf("%v-%v", "test-task-queue", uuid.New()),
			header:    nexus.Header{"xdc-redirection": "false"},
			handler: func(res *workflowservice.PollNexusTaskQueueResponse) (*nexuspb.Response, *nexuspb.HandlerError) {
				s.FailNow("nexus task handler invoked when redirection should be disabled")
				return nil, &nexuspb.HandlerError{
					ErrorType: string(nexus.HandlerErrorTypeInternal),
					Failure:   &nexuspb.Failure{Message: "redirection not allowed"},
				}
			},
			assertion: func(t *testing.T, result *nexus.ClientStartOperationResult[string], retErr error, activeSnap map[string][]*metricstest.CapturedRecording, passiveSnap map[string][]*metricstest.CapturedRecording) {
				var unexpectedError *nexus.UnexpectedResponseError
				require.ErrorAs(t, retErr, &unexpectedError)
				require.Equal(t, http.StatusServiceUnavailable, unexpectedError.Response.StatusCode)
				require.Equal(t, "cluster inactive", unexpectedError.Failure.Message)
				requireExpectedMetricsCaptured(t, passiveSnap, ns, "StartNexusOperation", "namespace_inactive_forwarding_disabled")
			},
		},
	}

	for _, tc := range testCases {
		tc := tc
		s.T().Run(tc.name, func(t *testing.T) {
			dispatchURL := fmt.Sprintf("http://%s/%s", s.cluster2.GetHost().FrontendHTTPAddress(), cnexus.RouteDispatchNexusTaskByNamespaceAndTaskQueue.Path(cnexus.NamespaceAndTaskQueue{Namespace: ns, TaskQueue: tc.taskQueue}))
			nexusClient, err := nexus.NewClient(nexus.ClientOptions{BaseURL: dispatchURL, Service: "test-service"})
			s.NoError(err)

			activeMetricsHandler, ok := s.cluster1.GetHost().GetMetricsHandler().(*metricstest.CaptureHandler)
			s.True(ok)
			activeCapture := activeMetricsHandler.StartCapture()
			defer activeMetricsHandler.StopCapture(activeCapture)

			passiveMetricsHandler, ok := s.cluster2.GetHost().GetMetricsHandler().(*metricstest.CaptureHandler)
			s.True(ok)
			passiveCapture := passiveMetricsHandler.StartCapture()
			defer passiveMetricsHandler.StopCapture(passiveCapture)

			ctx, cancel := context.WithCancel(tests.NewContext())
			defer cancel()

			go s.nexusTaskPoller(ctx, s.cluster1.GetFrontendClient(), ns, tc.taskQueue, tc.handler)

			startResult, err := nexus.StartOperation(ctx, nexusClient, op, "input", nexus.StartOperationOptions{
				CallbackURL: "http://localhost/callback",
				RequestID:   "request-id",
				Header:      tc.header,
			})
			tc.assertion(t, startResult, err, activeCapture.Snapshot(), passiveCapture.Snapshot())
		})
	}
}

// Only tests dispatch by namespace+task_queue.
// TODO: Add test cases for dispatch by endpoint ID once endpoints support replication.
func (s *NexusRequestForwardingSuite) TestCancelOperationForwardedFromStandbyToActive() {
	ns := s.createGlobalNamespace()

	testCases := []struct {
		name      string
		taskQueue string
		header    nexus.Header
		handler   func(*workflowservice.PollNexusTaskQueueResponse) (*nexuspb.Response, *nexuspb.HandlerError)
		assertion func(*testing.T, error, map[string][]*metricstest.CapturedRecording, map[string][]*metricstest.CapturedRecording)
	}{
		{
			name:      "success",
			taskQueue: fmt.Sprintf("%v-%v", "test-task-queue", uuid.New()),
			handler: func(res *workflowservice.PollNexusTaskQueueResponse) (*nexuspb.Response, *nexuspb.HandlerError) {
				s.Equal("true", res.Request.Header["xdc-redirection-api"])
				return &nexuspb.Response{
					Variant: &nexuspb.Response_CancelOperation{
						CancelOperation: &nexuspb.CancelOperationResponse{},
					},
				}, nil
			},
			assertion: func(t *testing.T, retErr error, activeSnap map[string][]*metricstest.CapturedRecording, passiveSnap map[string][]*metricstest.CapturedRecording) {
				require.NoError(t, retErr)
				requireExpectedMetricsCaptured(t, activeSnap, ns, "CancelNexusOperation", "success")
				requireExpectedMetricsCaptured(t, passiveSnap, ns, "CancelNexusOperation", "request_forwarded")
			},
		},
		{
			name:      "handler error",
			taskQueue: fmt.Sprintf("%v-%v", "test-task-queue", uuid.New()),
			handler: func(res *workflowservice.PollNexusTaskQueueResponse) (*nexuspb.Response, *nexuspb.HandlerError) {
				s.Equal("true", res.Request.Header["xdc-redirection-api"])
				return nil, &nexuspb.HandlerError{
					ErrorType: string(nexus.HandlerErrorTypeInternal),
					Failure:   &nexuspb.Failure{Message: "deliberate internal failure"},
				}
			},
			assertion: func(t *testing.T, retErr error, activeSnap map[string][]*metricstest.CapturedRecording, passiveSnap map[string][]*metricstest.CapturedRecording) {
				var unexpectedError *nexus.UnexpectedResponseError
				require.ErrorAs(t, retErr, &unexpectedError)
				require.Equal(t, http.StatusInternalServerError, unexpectedError.Response.StatusCode)
				require.Equal(t, "worker", unexpectedError.Response.Header.Get("Temporal-Nexus-Failure-Source"))
				require.Equal(t, "deliberate internal failure", unexpectedError.Failure.Message)
				requireExpectedMetricsCaptured(t, activeSnap, ns, "CancelNexusOperation", "handler_error")
				requireExpectedMetricsCaptured(t, passiveSnap, ns, "CancelNexusOperation", "forwarded_request_error")
			},
		},
		{
			name:      "redirect disabled by header",
			taskQueue: fmt.Sprintf("%v-%v", "test-task-queue", uuid.New()),
			header:    nexus.Header{"xdc-redirection": "false"},
			handler: func(res *workflowservice.PollNexusTaskQueueResponse) (*nexuspb.Response, *nexuspb.HandlerError) {
				s.FailNow("nexus task handler invoked when redirection should be disabled")
				return nil, &nexuspb.HandlerError{
					ErrorType: string(nexus.HandlerErrorTypeInternal),
					Failure:   &nexuspb.Failure{Message: "redirection should be disabled"},
				}
			},
			assertion: func(t *testing.T, retErr error, activeSnap map[string][]*metricstest.CapturedRecording, passiveSnap map[string][]*metricstest.CapturedRecording) {
				var unexpectedError *nexus.UnexpectedResponseError
				require.ErrorAs(t, retErr, &unexpectedError)
				require.Equal(t, http.StatusServiceUnavailable, unexpectedError.Response.StatusCode)
				require.Equal(t, "cluster inactive", unexpectedError.Failure.Message)
				requireExpectedMetricsCaptured(t, passiveSnap, ns, "CancelNexusOperation", "namespace_inactive_forwarding_disabled")
			},
		},
	}

	for _, tc := range testCases {
		tc := tc
		s.T().Run(tc.name, func(t *testing.T) {
			dispatchURL := fmt.Sprintf("http://%s/%s", s.cluster2.GetHost().FrontendHTTPAddress(), cnexus.RouteDispatchNexusTaskByNamespaceAndTaskQueue.Path(cnexus.NamespaceAndTaskQueue{Namespace: ns, TaskQueue: tc.taskQueue}))
			nexusClient, err := nexus.NewClient(nexus.ClientOptions{BaseURL: dispatchURL, Service: "test-service"})
			s.NoError(err)

			activeMetricsHandler, ok := s.cluster1.GetHost().GetMetricsHandler().(*metricstest.CaptureHandler)
			s.True(ok)
			activeCapture := activeMetricsHandler.StartCapture()
			defer activeMetricsHandler.StopCapture(activeCapture)

			passiveMetricsHandler, ok := s.cluster2.GetHost().GetMetricsHandler().(*metricstest.CaptureHandler)
			s.True(ok)
			passiveCapture := passiveMetricsHandler.StartCapture()
			defer passiveMetricsHandler.StopCapture(passiveCapture)

			ctx, cancel := context.WithCancel(tests.NewContext())
			defer cancel()

			go s.nexusTaskPoller(ctx, s.cluster1.GetFrontendClient(), ns, tc.taskQueue, tc.handler)

			handle, err := nexusClient.NewHandle("operation", "id")
			require.NoError(t, err)
			err = handle.Cancel(ctx, nexus.CancelOperationOptions{Header: tc.header})
			tc.assertion(t, err, activeCapture.Snapshot(), passiveCapture.Snapshot())
		})
	}
}

func (s *NexusRequestForwardingSuite) nexusTaskPoller(ctx context.Context, frontendClient tests.FrontendClient, ns string, taskQueue string, handler func(*workflowservice.PollNexusTaskQueueResponse) (*nexuspb.Response, *nexuspb.HandlerError)) {
	res, err := frontendClient.PollNexusTaskQueue(ctx, &workflowservice.PollNexusTaskQueueRequest{
		Namespace: ns,
		Identity:  uuid.NewString(),
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: taskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
	})
	if ctx.Err() != nil {
		// Test doesn't expect poll to get unblocked.
		return
	}
	s.NoError(err)

	response, handlerErr := handler(res)

	if handlerErr != nil {
		_, err = frontendClient.RespondNexusTaskFailed(ctx, &workflowservice.RespondNexusTaskFailedRequest{
			Namespace: ns,
			Identity:  uuid.NewString(),
			TaskToken: res.TaskToken,
			Error:     handlerErr,
		})
	} else if response != nil {
		_, err = frontendClient.RespondNexusTaskCompleted(ctx, &workflowservice.RespondNexusTaskCompletedRequest{
			Namespace: ns,
			Identity:  uuid.NewString(),
			TaskToken: res.TaskToken,
			Response:  response,
		})
	}

	s.NoError(err)
}

func requireExpectedMetricsCaptured(t *testing.T, snap map[string][]*metricstest.CapturedRecording, ns string, method string, expectedOutcome string) {
	require.Equal(t, 1, len(snap["nexus_requests"]))
	require.Subset(t, snap["nexus_requests"][0].Tags, map[string]string{"namespace": ns, "method": method, "outcome": expectedOutcome})
	require.Equal(t, int64(1), snap["nexus_requests"][0].Value)
	require.Equal(t, metrics.MetricUnit(""), snap["nexus_requests"][0].Unit)
	require.Equal(t, 1, len(snap["nexus_latency"]))
	require.Subset(t, snap["nexus_latency"][0].Tags, map[string]string{"namespace": ns, "method": method, "outcome": expectedOutcome})
	require.Equal(t, metrics.MetricUnit(metrics.Milliseconds), snap["nexus_latency"][0].Unit)
}
