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
	"io"
	"net/http"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/operatorservice/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"google.golang.org/protobuf/types/known/durationpb"

	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/namespace"
	cnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexustest"
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
	ns := s.createNexusRequestForwardingNamespace()

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
	ns := s.createNexusRequestForwardingNamespace()

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

func (s *NexusRequestForwardingSuite) TestCompleteOperationForwardedFromStandbyToActive() {
	// Override templates to always return passive cluster in callback URL
	s.cluster1.GetHost().OverrideDCValue(
		s.T(),
		nexusoperations.CallbackURLTemplate,
		"http://"+s.cluster2.GetHost().FrontendHTTPAddress()+"/namespaces/{{.NamespaceName}}/nexus/callback")
	s.cluster2.GetHost().OverrideDCValue(
		s.T(),
		nexusoperations.CallbackURLTemplate,
		"http://"+s.cluster2.GetHost().FrontendHTTPAddress()+"/namespaces/{{.NamespaceName}}/nexus/callback")

	ctx := tests.NewContext()
	ns := s.createNexusRequestForwardingNamespace()
	taskQueue := fmt.Sprintf("%v-%v", "test-task-queue", uuid.New())
	endpointName := fmt.Sprintf("%v-%v", "test-endpoint", uuid.New())

	var callbackToken, publicCallbackUrl string

	h := nexustest.Handler{
		OnStartOperation: func(ctx context.Context, service, operation string, input *nexus.LazyValue, options nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
			callbackToken = options.CallbackHeader.Get(cnexus.CallbackTokenHeader)
			publicCallbackUrl = options.CallbackURL
			return &nexus.HandlerStartOperationResultAsync{OperationID: "test"}, nil
		},
	}
	listenAddr := nexustest.AllocListenAddress(s.T())
	nexustest.NewNexusServer(s.T(), listenAddr, h)

	createEndpointReq := &operatorservice.CreateNexusEndpointRequest{
		Spec: &nexuspb.EndpointSpec{
			Name: endpointName,
			Target: &nexuspb.EndpointTarget{
				Variant: &nexuspb.EndpointTarget_External_{
					External: &nexuspb.EndpointTarget_External{
						Url: "http://" + listenAddr,
					},
				},
			},
		},
	}

	_, err := s.cluster1.GetOperatorClient().CreateNexusEndpoint(ctx, createEndpointReq)
	s.NoError(err)

	_, err = s.cluster2.GetOperatorClient().CreateNexusEndpoint(ctx, createEndpointReq)
	s.NoError(err)

	activeSDKClient, err := client.Dial(client.Options{
		HostPort:  s.cluster1.GetHost().FrontendGRPCAddress(),
		Namespace: ns,
		Logger:    log.NewSdkLogger(s.logger),
	})
	s.NoError(err)

	run, err := activeSDKClient.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		TaskQueue: taskQueue,
	}, "workflow")
	s.NoError(err)

	feClient1 := s.cluster1.GetFrontendClient()
	feClient2 := s.cluster2.GetFrontendClient()

	pollResp, err := feClient1.PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: ns,
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: taskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		Identity: "test",
	})
	s.NoError(err)
	_, err = feClient1.RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity:  "test",
		TaskToken: pollResp.TaskToken,
		Commands: []*commandpb.Command{
			{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_NEXUS_OPERATION,
				Attributes: &commandpb.Command_ScheduleNexusOperationCommandAttributes{
					ScheduleNexusOperationCommandAttributes: &commandpb.ScheduleNexusOperationCommandAttributes{
						Endpoint:  endpointName,
						Service:   "service",
						Operation: "operation",
						Input:     s.mustToPayload("input"),
					},
				},
			},
		},
	})
	s.NoError(err)

	// Poll and verify that the "started" event was recorded.
	pollResp, err = feClient1.PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: ns,
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: taskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		Identity: "test",
	})
	s.NoError(err)
	_, err = feClient1.RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity:  "test",
		TaskToken: pollResp.TaskToken,
	})
	s.NoError(err)

	startedEventIdx := slices.IndexFunc(pollResp.History.Events, func(e *historypb.HistoryEvent) bool {
		return e.GetNexusOperationStartedEventAttributes() != nil
	})
	s.Greater(startedEventIdx, 0)

	// Wait for Nexus operation to be replicated
	s.Eventually(func() bool {
		resp, err := feClient2.DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: ns,
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: run.GetID(),
				RunId:      run.GetRunID(),
			},
		})
		return err == nil && len(resp.PendingNexusOperations) > 0
	}, 5*time.Second, 500*time.Millisecond)

	// Send a valid - successful completion request to standby cluster.
	completion, err := nexus.NewOperationCompletionSuccessful(s.mustToPayload("result"), nexus.OperationCompletionSuccesfulOptions{
		Serializer: cnexus.PayloadSerializer,
	})
	s.NoError(err)
	res, snap := s.sendNexusCompletionRequest(ctx, s.T(), s.cluster2, publicCallbackUrl, completion, callbackToken)
	s.Equal(http.StatusOK, res.StatusCode)
	s.Equal(1, len(snap["nexus_completion_requests"]))
	s.Subset(snap["nexus_completion_requests"][0].Tags, map[string]string{"namespace": ns, "outcome": "request_forwarded"})

	// Ensure that CompleteOperation request is tracked as part of normal service telemetry metrics
	s.Condition(func() bool {
		for _, m := range snap["service_requests"] {
			if opTag, ok := m.Tags["operation"]; ok && opTag == "CompleteNexusOperation" {
				return true
			}
		}
		return false
	})

	// Resend the request and verify we get a not found error since the operation has already completed.
	res, snap = s.sendNexusCompletionRequest(ctx, s.T(), s.cluster1, publicCallbackUrl, completion, callbackToken)
	s.Equal(http.StatusNotFound, res.StatusCode)
	s.Equal(1, len(snap["nexus_completion_requests"]))
	s.Subset(snap["nexus_completion_requests"][0].Tags, map[string]string{"namespace": ns, "outcome": "error_not_found"})

	// Poll active cluster and verify the completion is recorded and triggers workflow progress.
	pollResp, err = feClient1.PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: ns,
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: taskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		Identity: "test",
	})
	s.NoError(err)
	completedEventIdx := slices.IndexFunc(pollResp.History.Events, func(e *historypb.HistoryEvent) bool {
		return e.GetNexusOperationCompletedEventAttributes() != nil
	})
	s.Greater(completedEventIdx, 0)

	_, err = feClient1.RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity:  "test",
		TaskToken: pollResp.TaskToken,
		Commands: []*commandpb.Command{
			{
				CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
					CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
						Result: &commonpb.Payloads{
							Payloads: []*commonpb.Payload{
								pollResp.History.Events[completedEventIdx].GetNexusOperationCompletedEventAttributes().Result,
							},
						},
					},
				},
			},
		},
	})
	s.NoError(err)
	var result string
	s.NoError(run.Get(ctx, &result))
	s.Equal("result", result)
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

func (s *NexusRequestForwardingSuite) sendNexusCompletionRequest(
	ctx context.Context,
	t *testing.T,
	testCluster *tests.TestCluster,
	url string,
	completion nexus.OperationCompletion,
	callbackToken string,
) (*http.Response, map[string][]*metricstest.CapturedRecording) {
	metricsHandler, ok := testCluster.GetHost().GetMetricsHandler().(*metricstest.CaptureHandler)
	s.True(ok)
	capture := metricsHandler.StartCapture()
	defer metricsHandler.StopCapture(capture)

	req, err := nexus.NewCompletionHTTPRequest(ctx, url, completion)
	require.NoError(t, err)
	if callbackToken != "" {
		req.Header.Add(cnexus.CallbackTokenHeader, callbackToken)
	}

	res, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	_, err = io.ReadAll(res.Body)
	require.NoError(t, err)
	defer res.Body.Close()
	return res, capture.Snapshot()
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

func (s *NexusRequestForwardingSuite) mustToPayload(v any) *commonpb.Payload {
	conv := converter.GetDefaultDataConverter()
	payload, err := conv.ToPayload(v)
	s.NoError(err)
	return payload
}
