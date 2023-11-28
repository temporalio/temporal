// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
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
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/google/uuid"
	"github.com/nexus-rpc/sdk-go/nexus"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/server/common/authorization"
	"go.temporal.io/server/common/metrics"
	"google.golang.org/protobuf/types/known/structpb"
)

var op = nexus.NewOperationReference[string, string]("my-operation")

func (s *clientFunctionalSuite) mustToPayload(v any) *commonpb.Payload {
	conv := converter.GetDefaultDataConverter()
	payload, err := conv.ToPayload(v)
	s.Require().NoError(err)
	return payload
}

func (s *clientFunctionalSuite) TestNexusStartOperation_WithNamespaceAndTaskQueue_Outcomes() {

	type testcase struct {
		outcome   string
		handler   func(*workflowservice.PollNexusTaskQueueResponse) (*nexuspb.Response, *nexuspb.HandlerError)
		assertion func(*nexus.ClientStartOperationResult[string], error)
	}

	testCases := []testcase{
		{
			outcome: "sync_success",
			handler: nexusEchoHandler,
			assertion: func(res *nexus.ClientStartOperationResult[string], err error) {
				s.Require().NoError(err)
				s.Require().Equal("input", res.Successful)
			},
		},
		{
			outcome: "async_success",
			handler: func(res *workflowservice.PollNexusTaskQueueResponse) (*nexuspb.Response, *nexuspb.HandlerError) {
				return &nexuspb.Response{
					Variant: &nexuspb.Response_StartOperation{
						StartOperation: &nexuspb.StartOperationResponse{
							Variant: &nexuspb.StartOperationResponse_AsyncSuccess{
								AsyncSuccess: &nexuspb.StartOperationResponse_Async{
									OperationId: "test-id",
								},
							},
						},
					},
				}, nil
			},
			assertion: func(res *nexus.ClientStartOperationResult[string], err error) {
				s.Require().NoError(err)
				s.Require().Equal("test-id", res.Pending.ID)
			},
		},
		{
			outcome: "operation_error",
			handler: func(res *workflowservice.PollNexusTaskQueueResponse) (*nexuspb.Response, *nexuspb.HandlerError) {
				return &nexuspb.Response{
					Variant: &nexuspb.Response_StartOperation{
						StartOperation: &nexuspb.StartOperationResponse{
							Variant: &nexuspb.StartOperationResponse_OperationError{
								OperationError: &nexuspb.UnsuccessfulOperationError{
									OperationState: string(nexus.OperationStateFailed),
									Failure: &nexuspb.Failure{
										Message:  "deliberate test failure",
										Metadata: map[string]string{"k": "v"},
										Details:  structpb.NewStringValue("details"),
									},
								},
							},
						},
					},
				}, nil
			},
			assertion: func(res *nexus.ClientStartOperationResult[string], err error) {
				var operationError *nexus.UnsuccessfulOperationError
				s.Require().ErrorAs(err, &operationError)
				s.Require().Equal(nexus.OperationStateFailed, operationError.State)
				s.Require().Equal("deliberate test failure", operationError.Failure.Message)
				s.Require().Equal(map[string]string{"k": "v"}, operationError.Failure.Metadata)
				var details string
				err = json.Unmarshal(operationError.Failure.Details, &details)
				s.Require().NoError(err)
				s.Require().Equal("details", details)
			},
		},
		{
			outcome: "handler_error",
			handler: func(res *workflowservice.PollNexusTaskQueueResponse) (*nexuspb.Response, *nexuspb.HandlerError) {
				return nil, &nexuspb.HandlerError{
					ErrorType: string(nexus.HandlerErrorTypeInternal),
					Failure:   &nexuspb.Failure{Message: "deliberate internal failure"},
				}
			},
			assertion: func(res *nexus.ClientStartOperationResult[string], err error) {
				var unexpectedError *nexus.UnexpectedResponseError
				s.Require().ErrorAs(err, &unexpectedError)
				// TODO: nexus should export this
				s.Require().Equal(520, unexpectedError.Response.StatusCode)
				s.Require().Equal("deliberate internal failure", unexpectedError.Failure.Message)
			},
		},
		// TODO:
		// {
		// 	outcome: "handler_timeout",
		// 	handler: func(res *workflowservice.PollNexusTaskQueueResponse) (*nexuspb.Response, *nexuspb.HandlerError) {
		// 		time.Sleep(time.Minute)
		// 		return nil, nil
		// 	},
		// 	assertion: func(res *nexus.ClientStartOperationResult[string], err error) {
		// 		var unexpectedError *nexus.UnexpectedResponseError
		// 		s.Require().ErrorAs(err, &unexpectedError)
		// 		// TODO: nexus should export this
		// 		s.Require().Equal(520, unexpectedError.Response.StatusCode)
		// 		s.Require().Equal("deliberate internal failure", unexpectedError.Failure.Message)
		// 	},
		// },
	}

	for _, tc := range testCases {
		tc := tc
		s.T().Run(tc.outcome, func(t *testing.T) {
			taskQueue := s.randomizeStr("task-queue")
			ctx := NewContext()

			url := fmt.Sprintf("http://%s/api/v1/namespaces/%s/task-queues/%s/dispatch-nexus-task", s.httpAPIAddress, s.namespace, taskQueue)
			client, err := nexus.NewClient(nexus.ClientOptions{ServiceBaseURL: url})
			s.Require().NoError(err)
			capture := s.testCluster.host.captureMetricsHandler.StartCapture()
			defer s.testCluster.host.captureMetricsHandler.StopCapture(capture)

			go s.nexusTaskPoller(ctx, taskQueue, tc.handler)

			result, err := nexus.StartOperation(ctx, client, op, "input", nexus.StartOperationOptions{})
			tc.assertion(result, err)

			snap := capture.Snapshot()

			s.Equal(1, len(snap["nexus_requests"]))
			s.Equal(map[string]string{"namespace": s.namespace, "method": "StartOperation", "outcome": tc.outcome}, snap["nexus_requests"][0].Tags)
			s.Equal(int64(1), snap["nexus_requests"][0].Value)
			s.Equal(metrics.MetricUnit(""), snap["nexus_requests"][0].Unit)

			s.Equal(1, len(snap["nexus_latency"]))
			s.Equal(map[string]string{"namespace": s.namespace, "method": "StartOperation", "outcome": tc.outcome}, snap["nexus_latency"][0].Tags)
			s.Equal(metrics.MetricUnit(metrics.Milliseconds), snap["nexus_latency"][0].Unit)
		})
	}
}

func (s *clientFunctionalSuite) TestNexusStartOperation_WithNamespaceAndTaskQueue_NamespaceNotFound() {
	taskQueue := s.randomizeStr("task-queue")
	url := fmt.Sprintf("http://%s/api/v1/namespaces/%s/task-queues/%s/dispatch-nexus-task", s.httpAPIAddress, "namespace-not-found", taskQueue)
	client, err := nexus.NewClient(nexus.ClientOptions{ServiceBaseURL: url})
	s.Require().NoError(err)
	ctx := NewContext()
	capture := s.testCluster.host.captureMetricsHandler.StartCapture()
	defer s.testCluster.host.captureMetricsHandler.StopCapture(capture)
	_, err = nexus.StartOperation(ctx, client, op, "input", nexus.StartOperationOptions{})
	var unexpectedResponse *nexus.UnexpectedResponseError
	s.Require().ErrorAs(err, &unexpectedResponse)
	s.Require().Equal(http.StatusNotFound, unexpectedResponse.Response.StatusCode)
	s.Require().Equal(`namespace not found: "namespace-not-found"`, unexpectedResponse.Failure.Message)

	snap := capture.Snapshot()

	s.Equal(1, len(snap["nexus_requests"]))
	s.Equal(map[string]string{"namespace": "namespace-not-found", "method": "StartOperation", "outcome": "namespace_not_found"}, snap["nexus_requests"][0].Tags)
	s.Equal(int64(1), snap["nexus_requests"][0].Value)
}

func (s *clientFunctionalSuite) TestNexusStartOperation_WithNamespaceAndTaskQueue_Forbidden() {
	type testcase struct {
		name           string
		onAuthorize    func(context.Context, *authorization.Claims, *authorization.CallTarget) (authorization.Result, error)
		failureMessage string
	}
	testCases := []testcase{
		{
			name: "deny with reason",
			onAuthorize: func(ctx context.Context, c *authorization.Claims, ct *authorization.CallTarget) (authorization.Result, error) {
				if ct.APIName == "/temporal.api.nexusservice.v1/DispatchNexusTask" {
					return authorization.Result{Decision: authorization.DecisionDeny, Reason: "unauthorized in test"}, nil
				}
				return authorization.Result{Decision: authorization.DecisionAllow}, nil
			},
			failureMessage: `permission denied: unauthorized in test`,
		},
		{
			name: "deny without reason",
			onAuthorize: func(ctx context.Context, c *authorization.Claims, ct *authorization.CallTarget) (authorization.Result, error) {
				if ct.APIName == "/temporal.api.nexusservice.v1/DispatchNexusTask" {
					return authorization.Result{Decision: authorization.DecisionDeny}, nil
				}
				return authorization.Result{Decision: authorization.DecisionAllow}, nil
			},
			failureMessage: "permission denied",
		},
		{
			name: "deny with generic error",
			onAuthorize: func(ctx context.Context, c *authorization.Claims, ct *authorization.CallTarget) (authorization.Result, error) {
				if ct.APIName == "/temporal.api.nexusservice.v1/DispatchNexusTask" {
					return authorization.Result{}, errors.New("some generic error")
				}
				return authorization.Result{Decision: authorization.DecisionAllow}, nil
			},
			failureMessage: "permission denied",
		},
	}

	taskQueue := s.randomizeStr("task-queue")
	url := fmt.Sprintf("http://%s/api/v1/namespaces/%s/task-queues/%s/dispatch-nexus-task", s.httpAPIAddress, s.namespace, taskQueue)
	client, err := nexus.NewClient(nexus.ClientOptions{ServiceBaseURL: url})
	s.Require().NoError(err)
	ctx := NewContext()

	for _, tc := range testCases {
		tc := tc
		s.T().Run(tc.name, func(t *testing.T) {
			s.testCluster.host.SetOnAuthorize(tc.onAuthorize)
			defer s.testCluster.host.SetOnAuthorize(nil)

			capture := s.testCluster.host.captureMetricsHandler.StartCapture()
			defer s.testCluster.host.captureMetricsHandler.StopCapture(capture)
			_, err = nexus.StartOperation(ctx, client, op, "input", nexus.StartOperationOptions{})
			var unexpectedResponse *nexus.UnexpectedResponseError
			s.Require().ErrorAs(err, &unexpectedResponse)
			s.Require().Equal(http.StatusForbidden, unexpectedResponse.Response.StatusCode)
			s.Require().Equal(tc.failureMessage, unexpectedResponse.Failure.Message)

			snap := capture.Snapshot()

			s.Equal(1, len(snap["nexus_requests"]))
			s.Equal(map[string]string{"namespace": s.namespace, "method": "StartOperation", "outcome": "unauthorized"}, snap["nexus_requests"][0].Tags)
			s.Equal(int64(1), snap["nexus_requests"][0].Value)
		})
	}
}

func (s *clientFunctionalSuite) TestNexusStartOperation_WithNamespaceAndTaskQueue_Claims() {
	type testcase struct {
		name      string
		header    nexus.Header
		assertion func(*nexus.ClientStartOperationResult[string], error)
	}
	testCases := []testcase{
		{
			name: "no header",
			assertion: func(res *nexus.ClientStartOperationResult[string], err error) {
				var unexpectedResponse *nexus.UnexpectedResponseError
				s.Require().ErrorAs(err, &unexpectedResponse)
				s.Require().Equal(http.StatusForbidden, unexpectedResponse.Response.StatusCode)
				s.Require().Equal("permission denied", unexpectedResponse.Failure.Message)
			},
		},
		{
			name: "invalid bearer",
			header: nexus.Header{
				"authorization": "Bearer invalid",
			},
			assertion: func(res *nexus.ClientStartOperationResult[string], err error) {
				var unexpectedResponse *nexus.UnexpectedResponseError
				s.Require().ErrorAs(err, &unexpectedResponse)
				s.Require().Equal(http.StatusUnauthorized, unexpectedResponse.Response.StatusCode)
				s.Require().Equal("unauthorized", unexpectedResponse.Failure.Message)
			},
		},
		{
			name: "valid bearer",
			header: nexus.Header{
				"authorization": "Bearer test",
			},
			assertion: func(res *nexus.ClientStartOperationResult[string], err error) {
				s.Require().NoError(err)
				s.Require().Equal("input", res.Successful)
			},
		},
	}

	s.testCluster.host.SetOnAuthorize(func(ctx context.Context, c *authorization.Claims, ct *authorization.CallTarget) (authorization.Result, error) {
		if ct.APIName == "/temporal.api.nexusservice.v1/DispatchNexusTask" && (c == nil || c.Subject != "test") {
			return authorization.Result{Decision: authorization.DecisionDeny}, nil
		}
		return authorization.Result{Decision: authorization.DecisionAllow}, nil
	})
	defer s.testCluster.host.SetOnAuthorize(nil)
	s.testCluster.host.SetOnGetClaims(func(ai *authorization.AuthInfo) (*authorization.Claims, error) {
		if ai.AuthToken != "Bearer test" {
			return nil, errors.New("invalid auth token")
		}
		return &authorization.Claims{Subject: "test"}, nil
	})
	defer s.testCluster.host.SetOnGetClaims(nil)

	taskQueue := s.randomizeStr("task-queue")
	ctx := NewContext()

	go s.echoNexusTaskPoller(ctx, taskQueue)

	url := fmt.Sprintf("http://%s/api/v1/namespaces/%s/task-queues/%s/dispatch-nexus-task", s.httpAPIAddress, s.namespace, taskQueue)
	client, err := nexus.NewClient(nexus.ClientOptions{ServiceBaseURL: url})
	s.Require().NoError(err)

	for _, tc := range testCases {
		tc := tc
		s.T().Run(tc.name, func(t *testing.T) {
			result, err := nexus.StartOperation(ctx, client, op, "input", nexus.StartOperationOptions{
				Header: tc.header,
			})
			tc.assertion(result, err)
		})
	}
}

func (s *clientFunctionalSuite) echoNexusTaskPoller(ctx context.Context, taskQueue string) {
	s.nexusTaskPoller(ctx, taskQueue, nexusEchoHandler)
}

func (s *clientFunctionalSuite) nexusTaskPoller(ctx context.Context, taskQueue string, handler func(*workflowservice.PollNexusTaskQueueResponse) (*nexuspb.Response, *nexuspb.HandlerError)) {
	res, err := s.testCluster.GetFrontendClient().PollNexusTaskQueue(ctx, &workflowservice.PollNexusTaskQueueRequest{
		Namespace: s.namespace,
		Identity:  uuid.NewString(),
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: taskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
	})
	s.Require().NoError(err)
	response, handlerError := handler(res)
	if handlerError != nil {
		_, err = s.testCluster.GetFrontendClient().RespondNexusTaskFailed(ctx, &workflowservice.RespondNexusTaskFailedRequest{
			Namespace: s.namespace,
			Identity:  uuid.NewString(),
			TaskToken: res.TaskToken,
			Error:     handlerError,
		})
		s.Require().NoError(err)
	} else if response != nil {
		_, err = s.testCluster.GetFrontendClient().RespondNexusTaskCompleted(ctx, &workflowservice.RespondNexusTaskCompletedRequest{
			Namespace: s.namespace,
			Identity:  uuid.NewString(),
			TaskToken: res.TaskToken,
			Response:  response,
		})
		s.Require().NoError(err)
	}
}

func nexusEchoHandler(res *workflowservice.PollNexusTaskQueueResponse) (*nexuspb.Response, *nexuspb.HandlerError) {
	return &nexuspb.Response{
		Variant: &nexuspb.Response_StartOperation{
			StartOperation: &nexuspb.StartOperationResponse{
				Variant: &nexuspb.StartOperationResponse_SyncSuccess{
					SyncSuccess: &nexuspb.StartOperationResponse_Sync{
						Payload: res.Request.GetStartOperation().GetPayload(),
					},
				},
			},
		},
	}, nil
}
