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
	"encoding/json"
	"flag"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/protobuf/types/known/durationpb"

	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/namespace"
	cnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/components/nexusoperations"
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
	s.dynamicConfigOverrides = map[dynamicconfig.Key]interface{}{
		// Make sure we don't hit the rate limiter in tests
		dynamicconfig.FrontendGlobalNamespaceNamespaceReplicationInducingAPIsRPS: 1000,
		dynamicconfig.FrontendEnableNexusAPIs:                                    true,
		nexusoperations.Enabled:                                                  true,
		dynamicconfig.FrontendRefreshNexusIncomingServicesMinWait:                5 * time.Millisecond,
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
// TODO: Add test cases for dispatch by incoming service ID once incoming services support replication.
func (s *NexusRequestForwardingSuite) TestStartOperationForwardedFromStandbyToActive() {
	ns := s.createNexusRequestForwardingNamespace()

	testCases := []struct {
		name      string
		taskQueue string
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
				requireExpectedMetricsCaptured(t, activeSnap, ns, "StartOperation", "sync_success")
				requireExpectedMetricsCaptured(t, passiveSnap, ns, "StartOperation", "request_forwarded")
			},
		},
		{
			name:      "operation_error",
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
				requireExpectedMetricsCaptured(t, activeSnap, ns, "StartOperation", "operation_error")
				requireExpectedMetricsCaptured(t, passiveSnap, ns, "StartOperation", "forwarded_request_error")
			},
		},
		{
			name:      "handler_error",
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
				// TODO: nexus should export this
				require.Equal(t, 520, unexpectedError.Response.StatusCode)
				require.Equal(t, "deliberate internal failure", unexpectedError.Failure.Message)
				requireExpectedMetricsCaptured(t, activeSnap, ns, "StartOperation", "handler_error")
				requireExpectedMetricsCaptured(t, passiveSnap, ns, "StartOperation", "forwarded_request_error")
			},
		},
	}

	for _, tc := range testCases {
		tc := tc
		s.T().Run(tc.name, func(t *testing.T) {
			dispatchURL := fmt.Sprintf("http://%s/%s", s.cluster2.GetHost().FrontendHTTPAddress(), cnexus.RouteDispatchNexusTaskByNamespaceAndTaskQueue.Path(cnexus.NamespaceAndTaskQueue{Namespace: ns, TaskQueue: tc.taskQueue}))
			nexusClient, err := nexus.NewClient(nexus.ClientOptions{ServiceBaseURL: dispatchURL})
			s.NoError(err)

			activeMetricsHandler, ok := s.cluster1.GetHost().GetMetricsHandler().(*metricstest.CaptureHandler)
			s.True(ok)
			activeCapture := activeMetricsHandler.StartCapture()
			defer activeMetricsHandler.StopCapture(activeCapture)

			passiveMetricsHandler, ok := s.cluster2.GetHost().GetMetricsHandler().(*metricstest.CaptureHandler)
			s.True(ok)
			passiveCapture := passiveMetricsHandler.StartCapture()
			defer passiveMetricsHandler.StopCapture(passiveCapture)

			go s.nexusTaskPoller(s.cluster1.GetFrontendClient(), ns, tc.taskQueue, tc.handler)

			ctx := tests.NewContext()
			startResult, err := nexus.StartOperation(ctx, nexusClient, op, "input", nexus.StartOperationOptions{
				CallbackURL: "http://localhost/callback",
				RequestID:   "request-id",
				Header:      nexus.Header{"key": "value"},
			})
			tc.assertion(t, startResult, err, activeCapture.Snapshot(), passiveCapture.Snapshot())
		})
	}
}

// Only tests dispatch by namespace+task_queue.
// TODO: Add test cases for dispatch by incoming service ID once incoming services support replication.
func (s *NexusRequestForwardingSuite) TestCancelOperationForwardedFromStandbyToActive() {
	ns := s.createNexusRequestForwardingNamespace()

	testCases := []struct {
		name      string
		taskQueue string
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
				requireExpectedMetricsCaptured(t, activeSnap, ns, "CancelOperation", "success")
				requireExpectedMetricsCaptured(t, passiveSnap, ns, "CancelOperation", "request_forwarded")
			},
		},
		{
			name:      "handler_error",
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
				// TODO: nexus should export this
				require.Equal(t, 520, unexpectedError.Response.StatusCode)
				require.Equal(t, "deliberate internal failure", unexpectedError.Failure.Message)
				requireExpectedMetricsCaptured(t, activeSnap, ns, "CancelOperation", "handler_error")
				requireExpectedMetricsCaptured(t, passiveSnap, ns, "CancelOperation", "forwarded_request_error")
			},
		},
	}

	for _, tc := range testCases {
		tc := tc
		s.T().Run(tc.name, func(t *testing.T) {
			dispatchURL := fmt.Sprintf("http://%s/%s", s.cluster2.GetHost().FrontendHTTPAddress(), cnexus.RouteDispatchNexusTaskByNamespaceAndTaskQueue.Path(cnexus.NamespaceAndTaskQueue{Namespace: ns, TaskQueue: tc.taskQueue}))
			nexusClient, err := nexus.NewClient(nexus.ClientOptions{ServiceBaseURL: dispatchURL})
			s.NoError(err)

			activeMetricsHandler, ok := s.cluster1.GetHost().GetMetricsHandler().(*metricstest.CaptureHandler)
			s.True(ok)
			activeCapture := activeMetricsHandler.StartCapture()
			defer activeMetricsHandler.StopCapture(activeCapture)

			passiveMetricsHandler, ok := s.cluster2.GetHost().GetMetricsHandler().(*metricstest.CaptureHandler)
			s.True(ok)
			passiveCapture := passiveMetricsHandler.StartCapture()
			defer passiveMetricsHandler.StopCapture(passiveCapture)

			go s.nexusTaskPoller(s.cluster1.GetFrontendClient(), ns, tc.taskQueue, tc.handler)

			ctx := tests.NewContext()
			handle, err := nexusClient.NewHandle("operation", "id")
			require.NoError(t, err)
			err = handle.Cancel(ctx, nexus.CancelOperationOptions{})
			tc.assertion(t, err, activeCapture.Snapshot(), passiveCapture.Snapshot())
		})
	}
}

func (s *NexusRequestForwardingSuite) nexusTaskPoller(frontendClient tests.FrontendClient, ns string, taskQueue string, handler func(*workflowservice.PollNexusTaskQueueResponse) (*nexuspb.Response, *nexuspb.HandlerError)) {
	ctx := tests.NewContext()
	res, err := frontendClient.PollNexusTaskQueue(ctx, &workflowservice.PollNexusTaskQueueRequest{
		Namespace: ns,
		Identity:  uuid.NewString(),
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: taskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
	})
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

func (s *NexusRequestForwardingSuite) createNexusRequestForwardingNamespace() string {
	ctx := tests.NewContext()
	ns := fmt.Sprintf("%v-%v", "test-namespace", uuid.New())

	regReq := &workflowservice.RegisterNamespaceRequest{
		Namespace:                        ns,
		IsGlobalNamespace:                true,
		Clusters:                         s.clusterReplicationConfig(),
		ActiveClusterName:                s.clusterNames[0],
		WorkflowExecutionRetentionPeriod: durationpb.New(7 * time.Hour * 24),
	}
	_, err := s.cluster1.GetFrontendClient().RegisterNamespace(ctx, regReq)
	s.NoError(err)

	s.EventuallyWithT(func(t *assert.CollectT) {
		// Wait for namespace record to be replicated and loaded into memory.
		_, err := s.cluster2.GetHost().GetFrontendNamespaceRegistry().GetNamespace(namespace.Name(ns))
		assert.NoError(t, err)
	}, 15*time.Second, 500*time.Millisecond)

	return ns
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
