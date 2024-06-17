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
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/operatorservice/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"

	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/common/authorization"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	cnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/service/frontend/configs"
)

var op = nexus.NewOperationReference[string, string]("my-operation")

func (s *ClientFunctionalSuite) mustToPayload(v any) *commonpb.Payload {
	conv := converter.GetDefaultDataConverter()
	payload, err := conv.ToPayload(v)
	s.NoError(err)
	return payload
}

func (s *ClientFunctionalSuite) TestNexusStartOperation_Outcomes() {
	type testcase struct {
		outcome   string
		endpoint  *nexuspb.Endpoint
		timeout   time.Duration
		handler   func(*workflowservice.PollNexusTaskQueueResponse) (*nexuspb.Response, *nexuspb.HandlerError)
		assertion func(*testing.T, *nexus.ClientStartOperationResult[string], error)
	}

	testCases := []testcase{
		{
			outcome:  "sync_success",
			endpoint: s.createNexusEndpoint(s.randomizeStr("test-endpoint"), s.randomizeStr("task-queue")),
			handler:  nexusEchoHandler,
			assertion: func(t *testing.T, res *nexus.ClientStartOperationResult[string], err error) {
				require.NoError(t, err)
				require.Equal(t, "input", res.Successful)
			},
		},
		{
			outcome:  "async_success",
			endpoint: s.createNexusEndpoint(s.randomizeStr("test-endpoint"), s.randomizeStr("task-queue")),
			handler: func(res *workflowservice.PollNexusTaskQueueResponse) (*nexuspb.Response, *nexuspb.HandlerError) {
				// Choose an arbitrary test case to assert that all of the input is delivered to the
				// poll response.
				start := res.Request.Variant.(*nexuspb.Request_StartOperation).StartOperation
				s.Equal(op.Name(), start.Operation)
				s.Equal("http://localhost/callback", start.Callback)
				s.Equal("request-id", start.RequestId)
				s.Equal("value", res.Request.Header["key"])
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
			assertion: func(t *testing.T, res *nexus.ClientStartOperationResult[string], err error) {
				require.NoError(t, err)
				require.Equal(t, "test-id", res.Pending.ID)
			},
		},
		{
			outcome:  "operation_error",
			endpoint: s.createNexusEndpoint(s.randomizeStr("test-endpoint"), s.randomizeStr("task-queue")),
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
										Details:  []byte(`"details"`),
									},
								},
							},
						},
					},
				}, nil
			},
			assertion: func(t *testing.T, res *nexus.ClientStartOperationResult[string], err error) {
				var operationError *nexus.UnsuccessfulOperationError
				require.ErrorAs(t, err, &operationError)
				require.Equal(t, nexus.OperationStateFailed, operationError.State)
				require.Equal(t, "deliberate test failure", operationError.Failure.Message)
				require.Equal(t, map[string]string{"k": "v"}, operationError.Failure.Metadata)
				var details string
				err = json.Unmarshal(operationError.Failure.Details, &details)
				require.NoError(t, err)
				require.Equal(t, "details", details)
			},
		},
		{
			outcome:  "handler_error",
			endpoint: s.createNexusEndpoint(s.randomizeStr("test-endpoint"), s.randomizeStr("task-queue")),
			handler: func(res *workflowservice.PollNexusTaskQueueResponse) (*nexuspb.Response, *nexuspb.HandlerError) {
				return nil, &nexuspb.HandlerError{
					ErrorType: string(nexus.HandlerErrorTypeInternal),
					Failure:   &nexuspb.Failure{Message: "deliberate internal failure"},
				}
			},
			assertion: func(t *testing.T, res *nexus.ClientStartOperationResult[string], err error) {
				var unexpectedError *nexus.UnexpectedResponseError
				require.ErrorAs(t, err, &unexpectedError)
				require.Equal(t, http.StatusInternalServerError, unexpectedError.Response.StatusCode)
				require.Equal(t, "worker", unexpectedError.Response.Header.Get("Temporal-Nexus-Failure-Source"))
				require.Equal(t, "deliberate internal failure", unexpectedError.Failure.Message)
			},
		},
		{
			outcome:  "handler_timeout",
			endpoint: s.createNexusEndpoint(s.randomizeStr("test-service"), s.randomizeStr("task-queue")),
			timeout:  1 * time.Second,
			handler: func(res *workflowservice.PollNexusTaskQueueResponse) (*nexuspb.Response, *nexuspb.HandlerError) {
				timeoutStr, set := res.Request.Header[nexus.HeaderRequestTimeout]
				s.True(set)
				timeout, err := time.ParseDuration(timeoutStr)
				s.NoError(err)
				time.Sleep(timeout) //nolint:forbidigo // Allow time.Sleep for timeout tests
				return nil, nil
			},
			assertion: func(t *testing.T, res *nexus.ClientStartOperationResult[string], err error) {
				var unexpectedError *nexus.UnexpectedResponseError
				require.ErrorAs(t, err, &unexpectedError)
				require.Equal(t, nexus.StatusDownstreamTimeout, unexpectedError.Response.StatusCode)
				require.Equal(t, "downstream timeout", unexpectedError.Failure.Message)
			},
		},
	}

	testFn := func(t *testing.T, tc testcase, dispatchURL string) {
		ctx, cancel := context.WithCancel(NewContext())
		defer cancel()

		client, err := nexus.NewClient(nexus.ClientOptions{BaseURL: dispatchURL, Service: "test-service"})
		require.NoError(t, err)
		capture := s.testCluster.host.captureMetricsHandler.StartCapture()
		defer s.testCluster.host.captureMetricsHandler.StopCapture(capture)

		go s.nexusTaskPoller(ctx, tc.endpoint.Spec.Target.GetWorker().TaskQueue, tc.handler)

		eventuallyTick := 500 * time.Millisecond
		header := nexus.Header{"key": "value"}
		if tc.timeout > 0 {
			eventuallyTick = tc.timeout + (100 * time.Millisecond)
			header[nexus.HeaderRequestTimeout] = tc.timeout.String()
		}

		var result *nexus.ClientStartOperationResult[string]

		// Wait until the endpoint is loaded into the registry.
		s.Eventually(func() bool {
			result, err = nexus.StartOperation(ctx, client, op, "input", nexus.StartOperationOptions{
				CallbackURL: "http://localhost/callback",
				RequestID:   "request-id",
				Header:      header,
			})
			var unexpectedResponseErr *nexus.UnexpectedResponseError
			return err == nil || !(errors.As(err, &unexpectedResponseErr) && unexpectedResponseErr.Response.StatusCode == http.StatusNotFound)
		}, 10*time.Second, eventuallyTick)

		tc.assertion(t, result, err)

		snap := capture.Snapshot()

		require.Equal(t, 1, len(snap["nexus_requests"]))
		require.Subset(t, snap["nexus_requests"][0].Tags, map[string]string{"namespace": s.namespace, "method": "StartNexusOperation", "outcome": tc.outcome})
		require.Contains(t, snap["nexus_requests"][0].Tags, "nexus_endpoint")
		require.Equal(t, int64(1), snap["nexus_requests"][0].Value)
		require.Equal(t, metrics.MetricUnit(""), snap["nexus_requests"][0].Unit)

		require.Equal(t, 1, len(snap["nexus_latency"]))
		require.Subset(t, snap["nexus_latency"][0].Tags, map[string]string{"namespace": s.namespace, "method": "StartNexusOperation", "outcome": tc.outcome})
		require.Contains(t, snap["nexus_latency"][0].Tags, "nexus_endpoint")
		require.Equal(t, metrics.MetricUnit(metrics.Milliseconds), snap["nexus_latency"][0].Unit)

		// Ensure that StartOperation request is tracked as part of normal service telemetry metrics
		require.Condition(t, func() bool {
			for _, m := range snap["service_requests"] {
				if opTag, ok := m.Tags["operation"]; ok && opTag == "StartNexusOperation" {
					return true
				}
			}
			return false
		})
	}

	for _, tc := range testCases {
		s.T().Run(tc.outcome, func(t *testing.T) {
			t.Run("ByNamespaceAndTaskQueue", func(t *testing.T) {
				testFn(t, tc, getDispatchByNsAndTqURL(s.httpAPIAddress, s.namespace, tc.endpoint.Spec.Target.GetWorker().TaskQueue))
			})
			t.Run("ByEndpoint", func(t *testing.T) {
				testFn(t, tc, getDispatchByEndpointURL(s.httpAPIAddress, tc.endpoint.Id))
			})
		})
	}
}

func (s *ClientFunctionalSuite) TestNexusStartOperation_WithNamespaceAndTaskQueue_NamespaceNotFound() {
	// Also use this test to verify that namespaces are unescaped in the path.
	taskQueue := s.randomizeStr("task-queue")
	namespace := "namespace not/found"
	u := getDispatchByNsAndTqURL(s.httpAPIAddress, namespace, taskQueue)
	client, err := nexus.NewClient(nexus.ClientOptions{BaseURL: u, Service: "test-service"})
	s.NoError(err)
	ctx := NewContext()
	capture := s.testCluster.host.captureMetricsHandler.StartCapture()
	defer s.testCluster.host.captureMetricsHandler.StopCapture(capture)
	_, err = nexus.StartOperation(ctx, client, op, "input", nexus.StartOperationOptions{})
	var unexpectedResponse *nexus.UnexpectedResponseError
	s.ErrorAs(err, &unexpectedResponse)
	s.Equal(http.StatusNotFound, unexpectedResponse.Response.StatusCode)
	s.Equal(fmt.Sprintf("namespace not found: %q", namespace), unexpectedResponse.Failure.Message)

	snap := capture.Snapshot()

	s.Equal(1, len(snap["nexus_requests"]))
	s.Equal(map[string]string{"namespace": namespace, "method": "StartNexusOperation", "outcome": "namespace_not_found", "nexus_endpoint": "_unknown_"}, snap["nexus_requests"][0].Tags)
	s.Equal(int64(1), snap["nexus_requests"][0].Value)
}

func (s *ClientFunctionalSuite) TestNexusStartOperation_WithNamespaceAndTaskQueue_NamespaceTooLong() {
	taskQueue := s.randomizeStr("task-queue")

	var namespace string
	for i := 0; i < 500; i++ {
		namespace += "namespace-is-a-very-long-string"
	}

	u := getDispatchByNsAndTqURL(s.httpAPIAddress, namespace, taskQueue)
	client, err := nexus.NewClient(nexus.ClientOptions{BaseURL: u, Service: "test-service"})
	s.NoError(err)
	ctx := NewContext()
	capture := s.testCluster.host.captureMetricsHandler.StartCapture()
	defer s.testCluster.host.captureMetricsHandler.StopCapture(capture)
	_, err = nexus.StartOperation(ctx, client, op, "input", nexus.StartOperationOptions{})
	var unexpectedResponse *nexus.UnexpectedResponseError
	s.ErrorAs(err, &unexpectedResponse)
	s.Equal(http.StatusBadRequest, unexpectedResponse.Response.StatusCode)
	// I wish we'd never put periods in error messages :(
	s.Equal("Namespace length exceeds limit.", unexpectedResponse.Failure.Message)

	snap := capture.Snapshot()

	s.Equal(1, len(snap["nexus_request_preprocess_errors"]))
}

func (s *ClientFunctionalSuite) TestNexusStartOperation_Forbidden() {
	taskQueue := s.randomizeStr("task-queue")
	testEndpoint := s.createNexusEndpoint(s.randomizeStr("test-endpoint"), taskQueue)

	type testcase struct {
		name           string
		onAuthorize    func(context.Context, *authorization.Claims, *authorization.CallTarget) (authorization.Result, error)
		failureMessage string
	}
	testCases := []testcase{
		{
			name: "deny with reason",
			onAuthorize: func(ctx context.Context, c *authorization.Claims, ct *authorization.CallTarget) (authorization.Result, error) {
				if ct.APIName == configs.DispatchNexusTaskByNamespaceAndTaskQueueAPIName {
					return authorization.Result{Decision: authorization.DecisionDeny, Reason: "unauthorized in test"}, nil
				}
				if ct.APIName == configs.DispatchNexusTaskByEndpointAPIName {
					if ct.NexusEndpointName != testEndpoint.Spec.Name {
						panic("expected nexus endpoint name")
					}
					return authorization.Result{Decision: authorization.DecisionDeny, Reason: "unauthorized in test"}, nil
				}
				return authorization.Result{Decision: authorization.DecisionAllow}, nil
			},
			failureMessage: `permission denied: unauthorized in test`,
		},
		{
			name: "deny without reason",
			onAuthorize: func(ctx context.Context, c *authorization.Claims, ct *authorization.CallTarget) (authorization.Result, error) {
				if ct.APIName == configs.DispatchNexusTaskByNamespaceAndTaskQueueAPIName {
					return authorization.Result{Decision: authorization.DecisionDeny}, nil
				}
				if ct.APIName == configs.DispatchNexusTaskByEndpointAPIName {
					if ct.NexusEndpointName != testEndpoint.Spec.Name {
						panic("expected nexus endpoint name")
					}
					return authorization.Result{Decision: authorization.DecisionDeny}, nil
				}
				return authorization.Result{Decision: authorization.DecisionAllow}, nil
			},
			failureMessage: "permission denied",
		},
		{
			name: "deny with generic error",
			onAuthorize: func(ctx context.Context, c *authorization.Claims, ct *authorization.CallTarget) (authorization.Result, error) {
				if ct.APIName == configs.DispatchNexusTaskByNamespaceAndTaskQueueAPIName {
					return authorization.Result{}, errors.New("some generic error")
				}
				if ct.APIName == configs.DispatchNexusTaskByEndpointAPIName {
					if ct.NexusEndpointName != testEndpoint.Spec.Name {
						panic("expected nexus endpoint name")
					}
					return authorization.Result{}, errors.New("some generic error")
				}
				return authorization.Result{Decision: authorization.DecisionAllow}, nil
			},
			failureMessage: "permission denied",
		},
	}

	testFn := func(t *testing.T, tc testcase, dispatchURL string) {
		client, err := nexus.NewClient(nexus.ClientOptions{BaseURL: dispatchURL, Service: "test-service"})
		require.NoError(t, err)
		ctx := NewContext()

		capture := s.testCluster.host.captureMetricsHandler.StartCapture()
		defer s.testCluster.host.captureMetricsHandler.StopCapture(capture)

		// Wait until the endpoint is loaded into the registry.
		s.Eventually(func() bool {
			_, err = nexus.StartOperation(ctx, client, op, "input", nexus.StartOperationOptions{})
			var unexpectedResponseErr *nexus.UnexpectedResponseError
			return err == nil || !(errors.As(err, &unexpectedResponseErr) && unexpectedResponseErr.Response.StatusCode == http.StatusNotFound)
		}, 10*time.Second, 1*time.Second)

		var unexpectedResponse *nexus.UnexpectedResponseError
		require.ErrorAs(t, err, &unexpectedResponse)
		require.Equal(t, http.StatusForbidden, unexpectedResponse.Response.StatusCode)
		require.Equal(t, tc.failureMessage, unexpectedResponse.Failure.Message)

		snap := capture.Snapshot()

		require.Equal(t, 1, len(snap["nexus_requests"]))
		require.Subset(t, snap["nexus_requests"][0].Tags, map[string]string{"namespace": s.namespace, "method": "StartNexusOperation", "outcome": "unauthorized"})
		require.Equal(t, int64(1), snap["nexus_requests"][0].Value)
	}

	for _, tc := range testCases {
		s.T().Run(tc.name, func(t *testing.T) {
			s.testCluster.host.SetOnAuthorize(tc.onAuthorize)
			defer s.testCluster.host.SetOnAuthorize(nil)

			t.Run("ByNamespaceAndTaskQueue", func(t *testing.T) {
				testFn(t, tc, getDispatchByNsAndTqURL(s.httpAPIAddress, s.namespace, taskQueue))
			})
			t.Run("ByEndpoint", func(t *testing.T) {
				testFn(t, tc, getDispatchByEndpointURL(s.httpAPIAddress, testEndpoint.Id))
			})
		})
	}
}

func (s *ClientFunctionalSuite) TestNexusStartOperation_Claims() {
	taskQueue := s.randomizeStr("task-queue")
	testEndpoint := s.createNexusEndpoint(s.randomizeStr("test-endpoint"), taskQueue)

	type testcase struct {
		name      string
		header    nexus.Header
		handler   func(*workflowservice.PollNexusTaskQueueResponse) (*nexuspb.Response, *nexuspb.HandlerError)
		assertion func(*testing.T, *nexus.ClientStartOperationResult[string], error, map[string][]*metricstest.CapturedRecording)
	}
	testCases := []testcase{
		{
			name: "no header",
			assertion: func(t *testing.T, res *nexus.ClientStartOperationResult[string], err error, snap map[string][]*metricstest.CapturedRecording) {
				var unexpectedResponse *nexus.UnexpectedResponseError
				require.ErrorAs(t, err, &unexpectedResponse)
				require.Equal(t, http.StatusForbidden, unexpectedResponse.Response.StatusCode)
				require.Equal(t, "permission denied", unexpectedResponse.Failure.Message)
				require.Equal(t, 0, len(snap["nexus_request_preprocess_errors"]))
			},
		},
		{
			name: "invalid bearer",
			header: nexus.Header{
				"authorization": "Bearer invalid",
			},
			assertion: func(t *testing.T, res *nexus.ClientStartOperationResult[string], err error, snap map[string][]*metricstest.CapturedRecording) {
				var unexpectedResponse *nexus.UnexpectedResponseError
				require.ErrorAs(t, err, &unexpectedResponse)
				require.Equal(t, http.StatusUnauthorized, unexpectedResponse.Response.StatusCode)
				require.Equal(t, "unauthorized", unexpectedResponse.Failure.Message)
				require.Equal(t, 1, len(snap["nexus_request_preprocess_errors"]))
			},
		},
		{
			name: "valid bearer",
			header: nexus.Header{
				"authorization": "Bearer test",
			},
			handler: nexusEchoHandler,
			assertion: func(t *testing.T, res *nexus.ClientStartOperationResult[string], err error, snap map[string][]*metricstest.CapturedRecording) {
				require.NoError(t, err)
				require.Equal(t, "input", res.Successful)
				require.Equal(t, 0, len(snap["nexus_request_preprocess_errors"]))
			},
		},
	}

	s.testCluster.host.SetOnAuthorize(func(ctx context.Context, c *authorization.Claims, ct *authorization.CallTarget) (authorization.Result, error) {
		if ct.APIName == configs.DispatchNexusTaskByNamespaceAndTaskQueueAPIName && (c == nil || c.Subject != "test") {
			return authorization.Result{Decision: authorization.DecisionDeny}, nil
		}
		if ct.APIName == configs.DispatchNexusTaskByEndpointAPIName && (c == nil || c.Subject != "test") {
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

	testFn := func(t *testing.T, tc testcase, dispatchURL string) {
		ctx, cancel := context.WithCancel(NewContext())
		defer cancel()

		client, err := nexus.NewClient(nexus.ClientOptions{BaseURL: dispatchURL, Service: "test-service"})
		s.NoError(err)

		if tc.handler != nil {
			// only set on valid request
			go s.nexusTaskPoller(ctx, taskQueue, tc.handler)
		}

		var result *nexus.ClientStartOperationResult[string]
		var snap map[string][]*metricstest.CapturedRecording

		// Wait until the endpoint is loaded into the registry.
		s.Eventually(func() bool {
			capture := s.testCluster.host.captureMetricsHandler.StartCapture()
			defer s.testCluster.host.captureMetricsHandler.StopCapture(capture)

			result, err = nexus.StartOperation(ctx, client, op, "input", nexus.StartOperationOptions{
				Header: tc.header,
			})
			snap = capture.Snapshot()
			var unexpectedResponseErr *nexus.UnexpectedResponseError
			return err == nil || !(errors.As(err, &unexpectedResponseErr) && unexpectedResponseErr.Response.StatusCode == http.StatusNotFound)
		}, 10*time.Second, 1*time.Second)

		tc.assertion(t, result, err, snap)
	}

	for _, tc := range testCases {
		s.T().Run(tc.name, func(t *testing.T) {
			t.Run("ByNamespaceAndTaskQueue", func(t *testing.T) {
				testFn(t, tc, getDispatchByNsAndTqURL(s.httpAPIAddress, s.namespace, taskQueue))
			})
			t.Run("ByEndpoint", func(t *testing.T) {
				testFn(t, tc, getDispatchByEndpointURL(s.httpAPIAddress, testEndpoint.Id))
			})
		})
	}
}

func (s *ClientFunctionalSuite) TestNexusStartOperation_PayloadSizeLimit() {
	taskQueue := s.randomizeStr("task-queue")
	testEndpoint := s.createNexusEndpoint(s.randomizeStr("test-endpoint"), taskQueue)

	// Use -10 to avoid hitting MaxNexusAPIRequestBodyBytes. Actual payload will still exceed limit because of
	// additional Content headers. See common/rpc/grpc.go:66
	input := strings.Repeat("a", (2*1024*1024)-10)

	testFn := func(t *testing.T, dispatchURL string) {
		ctx, cancel := context.WithCancel(NewContext())
		defer cancel()

		client, err := nexus.NewClient(nexus.ClientOptions{BaseURL: dispatchURL, Service: "test-service"})
		require.NoError(t, err)
		capture := s.testCluster.host.captureMetricsHandler.StartCapture()
		defer s.testCluster.host.captureMetricsHandler.StopCapture(capture)

		var result *nexus.ClientStartOperationResult[string]

		// Wait until the endpoint is loaded into the registry.
		s.Eventually(func() bool {
			result, err = nexus.StartOperation(ctx, client, op, input, nexus.StartOperationOptions{
				CallbackURL: "http://localhost/callback",
				RequestID:   "request-id",
			})
			var unexpectedResponseErr *nexus.UnexpectedResponseError
			return err == nil || !(errors.As(err, &unexpectedResponseErr) && unexpectedResponseErr.Response.StatusCode == http.StatusNotFound)
		}, 10*time.Second, 500*time.Millisecond)

		require.Nil(t, result)
		var unexpectedError *nexus.UnexpectedResponseError
		require.ErrorAs(t, err, &unexpectedError)
		require.Equal(t, http.StatusBadRequest, unexpectedError.Response.StatusCode)
		require.Equal(t, "input exceeds size limit", unexpectedError.Failure.Message)
	}

	s.T().Run("ByNamespaceAndTaskQueue", func(t *testing.T) {
		testFn(t, getDispatchByNsAndTqURL(s.httpAPIAddress, s.namespace, taskQueue))
	})
	s.T().Run("ByEndpoint", func(t *testing.T) {
		testFn(t, getDispatchByEndpointURL(s.httpAPIAddress, testEndpoint.Id))
	})
}

func (s *ClientFunctionalSuite) TestNexusCancelOperation_Outcomes() {
	type testcase struct {
		outcome   string
		endpoint  *nexuspb.Endpoint
		timeout   time.Duration
		handler   func(*workflowservice.PollNexusTaskQueueResponse) (*nexuspb.Response, *nexuspb.HandlerError)
		assertion func(*testing.T, error)
	}

	testCases := []testcase{
		{
			outcome:  "success",
			endpoint: s.createNexusEndpoint(s.randomizeStr("test-endpoint"), s.randomizeStr("task-queue")),
			handler: func(res *workflowservice.PollNexusTaskQueueResponse) (*nexuspb.Response, *nexuspb.HandlerError) {
				// Choose an arbitrary test case to assert that all of the input is delivered to the
				// poll response.
				op := res.Request.Variant.(*nexuspb.Request_CancelOperation).CancelOperation
				s.Equal("operation", op.Operation)
				s.Equal("id", op.OperationId)
				s.Equal("value", res.Request.Header["key"])
				return &nexuspb.Response{
					Variant: &nexuspb.Response_CancelOperation{
						CancelOperation: &nexuspb.CancelOperationResponse{},
					},
				}, nil
			},
			assertion: func(t *testing.T, err error) {
				require.NoError(t, err)
			},
		},
		{
			outcome:  "handler_error",
			endpoint: s.createNexusEndpoint(s.randomizeStr("test-endpoint"), s.randomizeStr("task-queue")),
			handler: func(res *workflowservice.PollNexusTaskQueueResponse) (*nexuspb.Response, *nexuspb.HandlerError) {
				return nil, &nexuspb.HandlerError{
					ErrorType: string(nexus.HandlerErrorTypeInternal),
					Failure:   &nexuspb.Failure{Message: "deliberate internal failure"},
				}
			},
			assertion: func(t *testing.T, err error) {
				var unexpectedError *nexus.UnexpectedResponseError
				require.ErrorAs(t, err, &unexpectedError)
				require.Equal(t, http.StatusInternalServerError, unexpectedError.Response.StatusCode)
				require.Equal(t, "worker", unexpectedError.Response.Header.Get("Temporal-Nexus-Failure-Source"))
				require.Equal(t, "deliberate internal failure", unexpectedError.Failure.Message)
			},
		},
		{
			outcome:  "handler_timeout",
			endpoint: s.createNexusEndpoint(s.randomizeStr("test-service"), s.randomizeStr("task-queue")),
			timeout:  1 * time.Second,
			handler: func(res *workflowservice.PollNexusTaskQueueResponse) (*nexuspb.Response, *nexuspb.HandlerError) {
				timeoutStr, set := res.Request.Header[nexus.HeaderRequestTimeout]
				s.True(set)
				timeout, err := time.ParseDuration(timeoutStr)
				s.NoError(err)
				time.Sleep(timeout) //nolint:forbidigo // Allow time.Sleep for timeout tests
				return nil, nil
			},
			assertion: func(t *testing.T, err error) {
				var unexpectedError *nexus.UnexpectedResponseError
				require.ErrorAs(t, err, &unexpectedError)
				require.Equal(t, nexus.StatusDownstreamTimeout, unexpectedError.Response.StatusCode)
				require.Equal(t, "downstream timeout", unexpectedError.Failure.Message)
			},
		},
	}

	testFn := func(t *testing.T, tc testcase, dispatchURL string) {
		ctx, cancel := context.WithCancel(NewContext())
		defer cancel()

		client, err := nexus.NewClient(nexus.ClientOptions{BaseURL: dispatchURL, Service: "test-service"})
		require.NoError(t, err)
		capture := s.testCluster.host.captureMetricsHandler.StartCapture()
		defer s.testCluster.host.captureMetricsHandler.StopCapture(capture)

		go s.nexusTaskPoller(ctx, tc.endpoint.Spec.Target.GetWorker().TaskQueue, tc.handler)

		handle, err := client.NewHandle("operation", "id")
		require.NoError(t, err)

		eventuallyTick := 500 * time.Millisecond
		header := nexus.Header{"key": "value"}
		if tc.timeout > 0 {
			eventuallyTick = tc.timeout + (100 * time.Millisecond)
			header[nexus.HeaderRequestTimeout] = tc.timeout.String()
		}

		// Wait until the endpoint is loaded into the registry.
		s.Eventually(func() bool {
			err = handle.Cancel(ctx, nexus.CancelOperationOptions{Header: header})
			var unexpectedResponseErr *nexus.UnexpectedResponseError
			return err == nil || !(errors.As(err, &unexpectedResponseErr) && unexpectedResponseErr.Response.StatusCode == http.StatusNotFound)
		}, 10*time.Second, eventuallyTick)

		tc.assertion(t, err)

		snap := capture.Snapshot()

		require.Equal(t, 1, len(snap["nexus_requests"]))
		require.Subset(t, snap["nexus_requests"][0].Tags, map[string]string{"namespace": s.namespace, "method": "CancelNexusOperation", "outcome": tc.outcome})
		require.Contains(t, snap["nexus_requests"][0].Tags, "nexus_endpoint")
		require.Equal(t, int64(1), snap["nexus_requests"][0].Value)
		require.Equal(t, metrics.MetricUnit(""), snap["nexus_requests"][0].Unit)

		require.Equal(t, 1, len(snap["nexus_latency"]))
		require.Subset(t, snap["nexus_latency"][0].Tags, map[string]string{"namespace": s.namespace, "method": "CancelNexusOperation", "outcome": tc.outcome})
		require.Contains(t, snap["nexus_latency"][0].Tags, "nexus_endpoint")
		require.Equal(t, metrics.MetricUnit(metrics.Milliseconds), snap["nexus_latency"][0].Unit)

		// Ensure that CancelOperation request is tracked as part of normal service telemetry metrics
		require.Condition(t, func() bool {
			for _, m := range snap["service_requests"] {
				if opTag, ok := m.Tags["operation"]; ok && opTag == "CancelNexusOperation" {
					return true
				}
			}
			return false
		})
	}

	for _, tc := range testCases {
		s.T().Run(tc.outcome, func(t *testing.T) {
			t.Run("ByNamespaceAndTaskQueue", func(t *testing.T) {
				testFn(t, tc, getDispatchByNsAndTqURL(s.httpAPIAddress, s.namespace, tc.endpoint.Spec.Target.GetWorker().TaskQueue))
			})
			t.Run("ByEndpoint", func(t *testing.T) {
				testFn(t, tc, getDispatchByEndpointURL(s.httpAPIAddress, tc.endpoint.Id))
			})
		})
	}
}

func (s *ClientFunctionalSuite) TestNexusStartOperation_WithNamespaceAndTaskQueue_SupportsVersioning() {
	ctx, cancel := context.WithCancel(NewContext())
	defer cancel()
	taskQueue := s.randomizeStr("task-queue")
	err := s.sdkClient.UpdateWorkerBuildIdCompatibility(ctx, &sdkclient.UpdateWorkerBuildIdCompatibilityOptions{
		TaskQueue: taskQueue,
		Operation: &sdkclient.BuildIDOpAddNewIDInNewDefaultSet{BuildID: "old-build-id"},
	})
	s.NoError(err)
	err = s.sdkClient.UpdateWorkerBuildIdCompatibility(ctx, &sdkclient.UpdateWorkerBuildIdCompatibilityOptions{
		TaskQueue: taskQueue,
		Operation: &sdkclient.BuildIDOpAddNewIDInNewDefaultSet{BuildID: "new-build-id"},
	})
	s.NoError(err)

	u := getDispatchByNsAndTqURL(s.httpAPIAddress, s.namespace, taskQueue)
	client, err := nexus.NewClient(nexus.ClientOptions{BaseURL: u, Service: "test-service"})
	s.NoError(err)
	// Versioned poller gets task
	go s.versionedNexusTaskPoller(ctx, taskQueue, "new-build-id", nexusEchoHandler)

	result, err := nexus.StartOperation(ctx, client, op, "input", nexus.StartOperationOptions{})
	s.NoError(err)
	s.Equal("input", result.Successful)

	// Unversioned poller doesn't get a task
	go s.nexusTaskPoller(ctx, taskQueue, nexusEchoHandler)
	// Versioned poller gets task with wrong build ID
	go s.versionedNexusTaskPoller(ctx, taskQueue, "old-build-id", nexusEchoHandler)

	ctx, cancel = context.WithTimeout(ctx, time.Second*2)
	defer cancel()
	_, err = nexus.StartOperation(ctx, client, op, "input", nexus.StartOperationOptions{})
	s.ErrorIs(err, context.DeadlineExceeded)
}

func (s *ClientFunctionalSuite) TestNexus_RespondNexusTaskMethods_VerifiesTaskTokenMatchesRequestNamespace() {
	ctx := NewContext()

	tt := tokenspb.NexusTask{
		NamespaceId: s.getNamespaceID(s.namespace),
		TaskQueue:   "test",
		TaskId:      uuid.NewString(),
	}
	ttBytes, err := tt.Marshal()
	s.NoError(err)

	_, err = s.testCluster.GetFrontendClient().RespondNexusTaskCompleted(ctx, &workflowservice.RespondNexusTaskCompletedRequest{
		Namespace: s.foreignNamespace,
		Identity:  uuid.NewString(),
		TaskToken: ttBytes,
		Response:  &nexuspb.Response{},
	})
	s.ErrorContains(err, "Operation requested with a token from a different namespace.")

	_, err = s.testCluster.GetFrontendClient().RespondNexusTaskFailed(ctx, &workflowservice.RespondNexusTaskFailedRequest{
		Namespace: s.foreignNamespace,
		Identity:  uuid.NewString(),
		TaskToken: ttBytes,
		Error:     &nexuspb.HandlerError{},
	})
	s.ErrorContains(err, "Operation requested with a token from a different namespace.")
}

func (s *ClientFunctionalSuite) TestNexusStartOperation_ByEndpoint_EndpointNotFound() {
	u := getDispatchByEndpointURL(s.httpAPIAddress, uuid.NewString())
	client, err := nexus.NewClient(nexus.ClientOptions{BaseURL: u, Service: "test-service"})
	s.NoError(err)
	ctx := NewContext()
	capture := s.testCluster.host.captureMetricsHandler.StartCapture()
	defer s.testCluster.host.captureMetricsHandler.StopCapture(capture)
	_, err = nexus.StartOperation(ctx, client, op, "input", nexus.StartOperationOptions{})
	var unexpectedResponse *nexus.UnexpectedResponseError
	s.ErrorAs(err, &unexpectedResponse)
	s.Equal(http.StatusNotFound, unexpectedResponse.Response.StatusCode)
	s.Equal("nexus endpoint not found", unexpectedResponse.Failure.Message)
	snap := capture.Snapshot()
	s.Equal(1, len(snap["nexus_request_preprocess_errors"]))
}

func (s *ClientFunctionalSuite) nexusTaskPoller(ctx context.Context, taskQueue string, handler func(*workflowservice.PollNexusTaskQueueResponse) (*nexuspb.Response, *nexuspb.HandlerError)) {
	s.versionedNexusTaskPoller(ctx, taskQueue, "", handler)
}

func (s *ClientFunctionalSuite) versionedNexusTaskPoller(ctx context.Context, taskQueue, buildID string, handler func(*workflowservice.PollNexusTaskQueueResponse) (*nexuspb.Response, *nexuspb.HandlerError)) {
	var vc *commonpb.WorkerVersionCapabilities

	if buildID != "" {
		vc = &commonpb.WorkerVersionCapabilities{
			BuildId:       buildID,
			UseVersioning: true,
		}
	}
	res, err := s.testCluster.GetFrontendClient().PollNexusTaskQueue(ctx, &workflowservice.PollNexusTaskQueueRequest{
		Namespace: s.namespace,
		Identity:  uuid.NewString(),
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: taskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		WorkerVersionCapabilities: vc,
	})
	// The test is written in a way that it doesn't expect the poll to be unblocked and it may cancel this context when it completes.
	if ctx.Err() != nil {
		return
	}
	// There's no clean way to propagate this error back to the test that's worthwhile. Panic is good enough.
	if err != nil {
		panic(err)
	}
	if res.Request.GetStartOperation().GetService() != "test-service" && res.Request.GetCancelOperation().GetService() != "test-service" {
		panic("expected service to be test-service")
	}
	response, handlerError := handler(res)
	if handlerError != nil {
		_, err = s.testCluster.GetFrontendClient().RespondNexusTaskFailed(ctx, &workflowservice.RespondNexusTaskFailedRequest{
			Namespace: s.namespace,
			Identity:  uuid.NewString(),
			TaskToken: res.TaskToken,
			Error:     handlerError,
		})
		// There's no clean way to propagate this error back to the test that's worthwhile. Panic is good enough.
		if err != nil && ctx.Err() == nil {
			panic(err)
		}
	} else if response != nil {
		_, err = s.testCluster.GetFrontendClient().RespondNexusTaskCompleted(ctx, &workflowservice.RespondNexusTaskCompletedRequest{
			Namespace: s.namespace,
			Identity:  uuid.NewString(),
			TaskToken: res.TaskToken,
			Response:  response,
		})
		// There's no clean way to propagate this error back to the test that's worthwhile. Panic is good enough.
		if err != nil && ctx.Err() == nil {
			panic(err)
		}
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

func getDispatchByNsAndTqURL(address string, namespace string, taskQueue string) string {
	return fmt.Sprintf(
		"http://%s/%s",
		address,
		cnexus.RouteDispatchNexusTaskByNamespaceAndTaskQueue.
			Path(cnexus.NamespaceAndTaskQueue{
				Namespace: namespace,
				TaskQueue: taskQueue,
			}),
	)
}

func (s *ClientFunctionalSuite) createNexusEndpoint(name string, taskQueue string) *nexuspb.Endpoint {
	resp, err := s.operatorClient.CreateNexusEndpoint(NewContext(), &operatorservice.CreateNexusEndpointRequest{
		Spec: &nexuspb.EndpointSpec{
			Name: name,
			Target: &nexuspb.EndpointTarget{
				Variant: &nexuspb.EndpointTarget_Worker_{
					Worker: &nexuspb.EndpointTarget_Worker{
						Namespace: s.namespace,
						TaskQueue: taskQueue,
					},
				},
			},
		},
	})
	s.NoError(err)
	return resp.Endpoint
}

func getDispatchByEndpointURL(address string, endpoint string) string {
	return fmt.Sprintf("http://%s/%s", address, cnexus.RouteDispatchNexusTaskByEndpoint.Path(endpoint))
}
