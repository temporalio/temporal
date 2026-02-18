package tests

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/server/common/authorization"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexusrpc"
	"go.temporal.io/server/components/nexusoperations"
	"go.temporal.io/server/service/frontend/configs"
	"go.temporal.io/server/tests/testcore"
)

type headerCapture struct {
	lastHeaders http.Header
}

func newHeaderCaptureCaller() (func(*http.Request) (*http.Response, error), *headerCapture) {
	capture := &headerCapture{}
	caller := func(req *http.Request) (*http.Response, error) {
		resp, err := http.DefaultClient.Do(req)
		if resp != nil {
			capture.lastHeaders = resp.Header
		}
		return resp, err
	}
	return caller, capture
}

var op = nexus.NewOperationReference[string, string]("my-operation")

type NexusApiTestSuite struct {
	NexusTestBaseSuite
}

func TestNexusApiTestSuiteWithLegacyErrorPaths(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(NexusApiTestSuite))
}

func TestNexusApiTestSuiteWithTemporalFailures(t *testing.T) {
	t.Parallel()
	s := new(NexusApiTestSuite)
	s.useTemporalFailures = true
	suite.Run(t, s)
}

func (s *NexusApiTestSuite) TestNexusStartOperation_Outcomes() {
	callerLink := &commonpb.Link_WorkflowEvent{
		Namespace:  "caller-ns",
		WorkflowId: "caller-wf-id",
		RunId:      "caller-run-id",
		Reference: &commonpb.Link_WorkflowEvent_EventRef{
			EventRef: &commonpb.Link_WorkflowEvent_EventReference{
				EventId:   5,
				EventType: enumspb.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED,
			},
		},
	}
	callerNexusLink := commonnexus.ConvertLinkWorkflowEventToNexusLink(callerLink)

	handlerLink := &commonpb.Link_WorkflowEvent{
		Namespace:  "handler-ns",
		WorkflowId: "handler-wf-id",
		RunId:      "handler-run-id",
		Reference: &commonpb.Link_WorkflowEvent_EventRef{
			EventRef: &commonpb.Link_WorkflowEvent_EventReference{
				EventId:   5,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
			},
		},
	}
	handlerNexusLink := commonnexus.ConvertLinkWorkflowEventToNexusLink(handlerLink)
	asyncSuccessEndpoint := testcore.RandomizeStr("test-endpoint")

	operationErrorOutcome := "operation_error"
	if s.useTemporalFailures {
		operationErrorOutcome = "failure"
	}

	type testcase struct {
		name           string
		outcome        string
		endpoint       *nexuspb.Endpoint
		timeout        time.Duration
		handler        nexusTaskHandler
		assertion      func(*testing.T, *nexusrpc.ClientStartOperationResponse[string], error, http.Header)
		onlyByEndpoint bool
	}

	testCases := []testcase{
		{
			name:     "sync_success",
			outcome:  "sync_success",
			endpoint: s.createNexusEndpoint(testcore.RandomizeStr("test-endpoint"), testcore.RandomizeStr("task-queue")),
			handler:  nexusEchoHandler,
			assertion: func(t *testing.T, res *nexusrpc.ClientStartOperationResponse[string], err error, headers http.Header) {
				require.NoError(t, err)
				require.Equal(t, "input", res.Successful)
			},
		},
		{
			name:           "async_success",
			outcome:        "async_success",
			onlyByEndpoint: true,
			endpoint:       s.createNexusEndpoint(asyncSuccessEndpoint, testcore.RandomizeStr("task-queue")),
			handler: func(res *workflowservice.PollNexusTaskQueueResponse) (*nexusTaskResponse, error) {
				// Choose an arbitrary test case to assert that all of the input is delivered to the
				// poll response.
				s.Equal(asyncSuccessEndpoint, res.Request.Endpoint)
				start := res.Request.Variant.(*nexuspb.Request_StartOperation).StartOperation
				s.Equal(op.Name(), start.Operation)
				s.Equal("http://localhost/callback", start.Callback)
				s.Equal("request-id", start.RequestId)
				s.Equal("value", res.Request.Header["key"])
				s.Len(start.GetLinks(), 1)
				s.Equal(callerNexusLink.URL.String(), start.Links[0].GetUrl())
				s.Equal(callerNexusLink.Type, start.Links[0].Type)
				return &nexusTaskResponse{
					StartResult: &nexus.HandlerStartOperationResultAsync{OperationToken: "test-token"},
					Links:       []nexus.Link{handlerNexusLink},
				}, nil
			},
			assertion: func(t *testing.T, res *nexusrpc.ClientStartOperationResponse[string], err error, headers http.Header) {
				require.NoError(t, err)
				require.Equal(t, "test-token", res.Pending.Token)
				require.Len(t, res.Links, 1)
				require.Equal(t, handlerNexusLink.URL.String(), res.Links[0].URL.String())
				require.Equal(t, handlerNexusLink.Type, res.Links[0].Type)
			},
		},
		{
			name:     "operation_error",
			outcome:  operationErrorOutcome,
			endpoint: s.createNexusEndpoint(testcore.RandomizeStr("test-endpoint"), testcore.RandomizeStr("task-queue")),
			handler: func(_ *workflowservice.PollNexusTaskQueueResponse) (*nexusTaskResponse, error) {
				return nil, &nexus.OperationError{
					State: nexus.OperationStateFailed,
					Cause: &nexus.FailureError{
						Failure: nexus.Failure{
							Message:  "deliberate test failure",
							Metadata: map[string]string{"k": "v"},
							Details:  json.RawMessage(`"details"`),
						},
					},
				}
			},
			assertion: func(t *testing.T, res *nexusrpc.ClientStartOperationResponse[string], err error, headers http.Header) {
				var operationError *nexus.OperationError
				require.ErrorAs(t, err, &operationError)
				require.Equal(t, nexus.OperationStateFailed, operationError.State)
				if s.useTemporalFailures {
					// Through the Temporal failure round-trip, the cause chain has an extra wrapper
					// for the OperationError's ApplicationFailureInfo.
					var failureErr *nexus.FailureError
					require.ErrorAs(t, operationError.Cause, &failureErr)
					var innerErr *nexus.FailureError
					require.ErrorAs(t, failureErr.Cause, &innerErr)
					tFailure, err := commonnexus.NexusFailureToTemporalFailure(innerErr.Failure)
					require.NoError(t, err)
					convErr := temporal.GetDefaultFailureConverter().FailureToError(tFailure)
					var appErr *temporal.ApplicationError
					require.ErrorAs(t, convErr, &appErr)
					require.Equal(t, "deliberate test failure", appErr.Message())
					var details nexus.Failure
					require.NoError(t, appErr.Details(&details))
					require.Equal(t, "v", details.Metadata["k"])
				} else {
					require.Equal(t, "deliberate test failure", operationError.Cause.Error())
					var failureErr *nexus.FailureError
					require.ErrorAs(t, operationError.Cause, &failureErr)
					require.Equal(t, map[string]string{"k": "v"}, failureErr.Failure.Metadata)
					var details string
					err = json.Unmarshal(failureErr.Failure.Details, &details)
					require.NoError(t, err)
					require.Equal(t, "details", details)
				}
			},
		},
		{
			name:     "handler_error",
			outcome:  "handler_error:INTERNAL",
			endpoint: s.createNexusEndpoint(testcore.RandomizeStr("test-endpoint"), testcore.RandomizeStr("task-queue")),
			handler: func(_ *workflowservice.PollNexusTaskQueueResponse) (*nexusTaskResponse, error) {
				return nil, &nexus.HandlerError{
					Type: nexus.HandlerErrorTypeInternal,
					Cause: &nexus.FailureError{
						Failure: nexus.Failure{Message: "deliberate internal failure"},
					},
				}
			},
			assertion: func(t *testing.T, res *nexusrpc.ClientStartOperationResponse[string], err error, headers http.Header) {
				var handlerErr *nexus.HandlerError
				require.ErrorAs(t, err, &handlerErr)
				require.Equal(t, nexus.HandlerErrorTypeInternal, handlerErr.Type)
				require.Equal(t, nexus.HandlerErrorRetryBehaviorUnspecified, handlerErr.RetryBehavior)
				require.Equal(t, "worker", headers.Get("Temporal-Nexus-Failure-Source"))
				require.Empty(t, handlerErr.Message)
				require.Error(t, handlerErr.Cause)
				require.Equal(t, "deliberate internal failure", handlerErr.Cause.Error())
			},
		},
		{
			name:     "handler_error_non_retryable",
			outcome:  "handler_error:INTERNAL",
			endpoint: s.createNexusEndpoint(testcore.RandomizeStr("test-endpoint"), testcore.RandomizeStr("task-queue")),
			handler: func(_ *workflowservice.PollNexusTaskQueueResponse) (*nexusTaskResponse, error) {
				return nil, &nexus.HandlerError{
					Type:          nexus.HandlerErrorTypeInternal,
					RetryBehavior: nexus.HandlerErrorRetryBehaviorNonRetryable,
					Cause: &nexus.FailureError{
						Failure: nexus.Failure{Message: "deliberate internal failure"},
					},
				}
			},
			assertion: func(t *testing.T, res *nexusrpc.ClientStartOperationResponse[string], err error, headers http.Header) {
				var handlerErr *nexus.HandlerError
				require.ErrorAs(t, err, &handlerErr)
				require.Equal(t, nexus.HandlerErrorTypeInternal, handlerErr.Type)
				require.Equal(t, nexus.HandlerErrorRetryBehaviorNonRetryable, handlerErr.RetryBehavior)
				require.Equal(t, "worker", headers.Get("Temporal-Nexus-Failure-Source"))
				require.Empty(t, handlerErr.Message)
				require.Error(t, handlerErr.Cause)
				require.Equal(t, "deliberate internal failure", handlerErr.Cause.Error())
			},
		},
		{
			name:     "handler_timeout",
			outcome:  "handler_timeout",
			endpoint: s.createNexusEndpoint(testcore.RandomizeStr("test-service"), testcore.RandomizeStr("task-queue")),
			timeout:  2 * time.Second,
			handler: func(res *workflowservice.PollNexusTaskQueueResponse) (*nexusTaskResponse, error) {
				timeoutStr, set := res.Request.Header[nexus.HeaderRequestTimeout]
				s.True(set)
				timeout, err := time.ParseDuration(timeoutStr)

				var dispatchTimeoutBuffer = nexusoperations.MinDispatchTaskTimeout.Get(dynamicconfig.NewNoopCollection())("test")
				expectedMaxTimeout := 2*time.Second - dispatchTimeoutBuffer
				s.LessOrEqual(timeout, expectedMaxTimeout, "timeout should be buffered")

				s.NoError(err)
				time.Sleep(timeout) //nolint:forbidigo // Allow time.Sleep for timeout tests
				return nil, nil
			},
			assertion: func(t *testing.T, res *nexusrpc.ClientStartOperationResponse[string], err error, header http.Header) {
				var handlerErr *nexus.HandlerError
				require.ErrorAs(t, err, &handlerErr)
				require.Equal(t, nexus.HandlerErrorTypeUpstreamTimeout, handlerErr.Type)
				require.Equal(t, "upstream timeout", handlerErr.Message)
			},
		},
	}

	testFn := func(t *testing.T, tc testcase, dispatchURL string) {
		ctx, cancel := context.WithCancel(testcore.NewContext())
		defer cancel()

		httpCaller, headerCapture := newHeaderCaptureCaller()
		client, err := nexusrpc.NewHTTPClient(nexusrpc.HTTPClientOptions{
			BaseURL:    dispatchURL,
			Service:    "test-service",
			HTTPCaller: httpCaller,
		})
		require.NoError(t, err)
		capture := s.GetTestCluster().Host().CaptureMetricsHandler().StartCapture()
		defer s.GetTestCluster().Host().CaptureMetricsHandler().StopCapture(capture)

		pollerErrCh := s.nexusTaskPoller(ctx, tc.endpoint.Spec.Target.GetWorker().TaskQueue, tc.handler)

		eventuallyTick := 500 * time.Millisecond
		header := nexus.Header{"key": "value"}
		if tc.timeout > 0 {
			eventuallyTick = tc.timeout + (100 * time.Millisecond)
			header[nexus.HeaderRequestTimeout] = tc.timeout.String()
		}

		var result *nexusrpc.ClientStartOperationResponse[string]

		// Wait until the endpoint is loaded into the registry.
		s.Eventually(func() bool {
			result, err = nexusrpc.StartOperation(ctx, client, op, "input", nexus.StartOperationOptions{
				CallbackURL: "http://localhost/callback",
				RequestID:   "request-id",
				Header:      header,
				Links:       []nexus.Link{callerNexusLink},
			})
			var handlerErr *nexus.HandlerError
			return err == nil || !(errors.As(err, &handlerErr) && handlerErr.Type == nexus.HandlerErrorTypeNotFound)
		}, 10*time.Second, eventuallyTick)

		tc.assertion(t, result, err, headerCapture.lastHeaders)
		s.NoError(<-pollerErrCh)

		snap := capture.Snapshot()

		require.Equal(t, 1, len(snap["nexus_requests"]))
		require.Subset(t, snap["nexus_requests"][0].Tags, map[string]string{"namespace": s.Namespace().String(), "method": "StartNexusOperation", "outcome": tc.outcome})
		require.Contains(t, snap["nexus_requests"][0].Tags, "nexus_endpoint")
		require.Equal(t, int64(1), snap["nexus_requests"][0].Value)
		require.Equal(t, metrics.MetricUnit(""), snap["nexus_requests"][0].Unit)

		require.Equal(t, 1, len(snap["nexus_latency"]))
		require.Subset(t, snap["nexus_latency"][0].Tags, map[string]string{"namespace": s.Namespace().String(), "method": "StartNexusOperation", "outcome": tc.outcome})
		require.Contains(t, snap["nexus_latency"][0].Tags, "nexus_endpoint")

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
		s.T().Run(tc.name, func(t *testing.T) {
			if !tc.onlyByEndpoint {
				t.Run("ByNamespaceAndTaskQueue", func(t *testing.T) {
					testFn(t, tc, getDispatchByNsAndTqURL(s.HttpAPIAddress(), s.Namespace().String(), tc.endpoint.Spec.Target.GetWorker().TaskQueue))
				})
			}
			t.Run("ByEndpoint", func(t *testing.T) {
				testFn(t, tc, getDispatchByEndpointURL(s.HttpAPIAddress(), tc.endpoint.Id))
			})
		})
	}
}

func (s *NexusApiTestSuite) TestNexusStartOperation_Claims() {
	taskQueue := testcore.RandomizeStr("task-queue")
	testEndpoint := s.createNexusEndpoint(testcore.RandomizeStr("test-endpoint"), taskQueue)

	type testcase struct {
		name      string
		header    nexus.Header
		handler   nexusTaskHandler
		assertion func(*testing.T, *nexusrpc.ClientStartOperationResponse[string], error, map[string][]*metricstest.CapturedRecording)
	}
	testCases := []testcase{
		{
			name: "no header",
			assertion: func(t *testing.T, res *nexusrpc.ClientStartOperationResponse[string], err error, snap map[string][]*metricstest.CapturedRecording) {
				var handlerErr *nexus.HandlerError
				require.ErrorAs(t, err, &handlerErr)
				require.Equal(t, nexus.HandlerErrorTypeUnauthorized, handlerErr.Type)
				require.Equal(t, "permission denied", handlerErr.Message)
				require.Equal(t, 0, len(snap["nexus_request_preprocess_errors"]))
			},
		},
		{
			name: "invalid bearer",
			header: nexus.Header{
				"authorization": "Bearer invalid",
			},
			assertion: func(t *testing.T, res *nexusrpc.ClientStartOperationResponse[string], err error, snap map[string][]*metricstest.CapturedRecording) {
				var handlerErr *nexus.HandlerError
				require.ErrorAs(t, err, &handlerErr)
				require.Equal(t, nexus.HandlerErrorTypeUnauthenticated, handlerErr.Type)
				require.Equal(t, "unauthorized", handlerErr.Message)
				require.Equal(t, 1, len(snap["nexus_request_preprocess_errors"]))
			},
		},
		{
			name: "valid bearer",
			header: nexus.Header{
				"authorization": "Bearer test",
			},
			handler: nexusEchoHandler,
			assertion: func(t *testing.T, res *nexusrpc.ClientStartOperationResponse[string], err error, snap map[string][]*metricstest.CapturedRecording) {
				require.NoError(t, err)
				require.Equal(t, "input", res.Successful)
				require.Equal(t, 0, len(snap["nexus_request_preprocess_errors"]))
			},
		},
	}

	s.GetTestCluster().Host().SetOnAuthorize(func(ctx context.Context, c *authorization.Claims, ct *authorization.CallTarget) (authorization.Result, error) {
		if ct.APIName == configs.DispatchNexusTaskByNamespaceAndTaskQueueAPIName && (c == nil || c.Subject != "test") {
			return authorization.Result{Decision: authorization.DecisionDeny}, nil
		}
		if ct.APIName == configs.DispatchNexusTaskByEndpointAPIName && (c == nil || c.Subject != "test") {
			return authorization.Result{Decision: authorization.DecisionDeny}, nil
		}
		return authorization.Result{Decision: authorization.DecisionAllow}, nil
	})
	defer s.GetTestCluster().Host().SetOnAuthorize(nil)

	s.GetTestCluster().Host().SetOnGetClaims(func(ai *authorization.AuthInfo) (*authorization.Claims, error) {
		if ai.AuthToken != "Bearer test" {
			return nil, errors.New("invalid auth token")
		}
		return &authorization.Claims{Subject: "test"}, nil
	})
	defer s.GetTestCluster().Host().SetOnGetClaims(nil)

	testFn := func(t *testing.T, tc testcase, dispatchURL string) {
		ctx, cancel := context.WithCancel(testcore.NewContext())
		defer cancel()

		client, err := nexusrpc.NewHTTPClient(nexusrpc.HTTPClientOptions{BaseURL: dispatchURL, Service: "test-service"})
		s.NoError(err)

		var pollerErrCh <-chan error
		if tc.handler != nil {
			// only set on valid request
			pollerErrCh = s.nexusTaskPoller(ctx, taskQueue, tc.handler)
		}

		capture := s.GetTestCluster().Host().CaptureMetricsHandler().StartCapture()
		result, err := nexusrpc.StartOperation(ctx, client, op, "input", nexus.StartOperationOptions{
			Header: tc.header,
		})
		snap := capture.Snapshot()
		s.GetTestCluster().Host().CaptureMetricsHandler().StopCapture(capture)

		tc.assertion(t, result, err, snap)
		if pollerErrCh != nil {
			s.NoError(<-pollerErrCh)
		}
	}

	for _, tc := range testCases {
		s.T().Run(tc.name, func(t *testing.T) {
			t.Run("ByNamespaceAndTaskQueue", func(t *testing.T) {
				testFn(t, tc, getDispatchByNsAndTqURL(s.HttpAPIAddress(), s.Namespace().String(), taskQueue))
			})
			t.Run("ByEndpoint", func(t *testing.T) {
				testFn(t, tc, getDispatchByEndpointURL(s.HttpAPIAddress(), testEndpoint.Id))
			})
		})
	}
}

func (s *NexusApiTestSuite) TestNexusCancelOperation_Outcomes() {
	asyncSuccessEndpoint := testcore.RandomizeStr("async-success-endpoint")

	type testcase struct {
		outcome        string
		onlyByEndpoint bool
		endpoint       *nexuspb.Endpoint
		timeout        time.Duration
		handler        nexusTaskHandler
		assertion      func(*testing.T, error, http.Header)
	}

	testCases := []testcase{
		{
			outcome:        "success",
			onlyByEndpoint: true,
			endpoint:       s.createNexusEndpoint(asyncSuccessEndpoint, testcore.RandomizeStr("task-queue")),
			handler: func(res *workflowservice.PollNexusTaskQueueResponse) (*nexusTaskResponse, error) {
				s.Equal(asyncSuccessEndpoint, res.Request.Endpoint)
				// Choose an arbitrary test case to assert that all of the input is delivered to the
				// poll response.
				op, ok := res.Request.Variant.(*nexuspb.Request_CancelOperation)
				s.True(ok)
				s.Equal("test-service", op.CancelOperation.Service)
				s.Equal("operation", op.CancelOperation.Operation)
				s.Equal("token", op.CancelOperation.OperationToken)
				s.Equal("value", res.Request.Header["key"])
				return &nexusTaskResponse{CancelResult: new(struct{})}, nil
			},
			assertion: func(t *testing.T, err error, headers http.Header) {
				require.NoError(t, err)
			},
		},
		{
			outcome:  "handler_error:INTERNAL",
			endpoint: s.createNexusEndpoint(testcore.RandomizeStr("test-endpoint"), testcore.RandomizeStr("task-queue")),
			handler: func(_ *workflowservice.PollNexusTaskQueueResponse) (*nexusTaskResponse, error) {
				return nil, &nexus.HandlerError{
					Type: nexus.HandlerErrorTypeInternal,
					Cause: &nexus.FailureError{
						Failure: nexus.Failure{Message: "deliberate internal failure"},
					},
				}
			},
			assertion: func(t *testing.T, err error, headers http.Header) {
				var handlerErr *nexus.HandlerError
				require.ErrorAs(t, err, &handlerErr)
				require.Equal(t, nexus.HandlerErrorTypeInternal, handlerErr.Type)
				require.Equal(t, "worker", headers.Get("Temporal-Nexus-Failure-Source"))
				require.Empty(t, handlerErr.Message)
				require.Error(t, handlerErr.Cause)
				require.Equal(t, "deliberate internal failure", handlerErr.Cause.Error())
			},
		},
		{
			outcome:  "handler_timeout",
			endpoint: s.createNexusEndpoint(testcore.RandomizeStr("test-service"), testcore.RandomizeStr("task-queue")),
			timeout:  2 * time.Second,
			handler: func(res *workflowservice.PollNexusTaskQueueResponse) (*nexusTaskResponse, error) {
				timeoutStr, set := res.Request.Header[nexus.HeaderRequestTimeout]
				s.True(set)
				timeout, err := time.ParseDuration(timeoutStr)
				s.NoError(err)
				time.Sleep(timeout) //nolint:forbidigo // Allow time.Sleep for timeout tests
				return nil, nil
			},
			assertion: func(t *testing.T, err error, headers http.Header) {
				var handlerErr *nexus.HandlerError
				require.ErrorAs(t, err, &handlerErr)
				require.Equal(t, nexus.HandlerErrorTypeUpstreamTimeout, handlerErr.Type)
				require.Equal(t, "upstream timeout", handlerErr.Message)
			},
		},
	}

	testFn := func(t *testing.T, tc testcase, dispatchURL string) {
		ctx, cancel := context.WithCancel(testcore.NewContext())
		defer cancel()

		httpCaller, headerCapture := newHeaderCaptureCaller()
		client, err := nexusrpc.NewHTTPClient(nexusrpc.HTTPClientOptions{
			BaseURL:    dispatchURL,
			Service:    "test-service",
			HTTPCaller: httpCaller,
		})
		require.NoError(t, err)
		capture := s.GetTestCluster().Host().CaptureMetricsHandler().StartCapture()
		defer s.GetTestCluster().Host().CaptureMetricsHandler().StopCapture(capture)

		pollerErrCh := s.nexusTaskPoller(ctx, tc.endpoint.Spec.Target.GetWorker().TaskQueue, tc.handler)

		handle, err := client.NewOperationHandle("operation", "token")
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
			var handlerErr *nexus.HandlerError
			return err == nil || !(errors.As(err, &handlerErr) && handlerErr.Type == nexus.HandlerErrorTypeNotFound)
		}, 10*time.Second, eventuallyTick)

		tc.assertion(t, err, headerCapture.lastHeaders)
		s.NoError(<-pollerErrCh)

		snap := capture.Snapshot()

		require.Equal(t, 1, len(snap["nexus_requests"]))
		require.Subset(t, snap["nexus_requests"][0].Tags, map[string]string{"namespace": s.Namespace().String(), "method": "CancelNexusOperation", "outcome": tc.outcome})
		require.Contains(t, snap["nexus_requests"][0].Tags, "nexus_endpoint")
		require.Equal(t, int64(1), snap["nexus_requests"][0].Value)
		require.Equal(t, metrics.MetricUnit(""), snap["nexus_requests"][0].Unit)

		require.Equal(t, 1, len(snap["nexus_latency"]))
		require.Subset(t, snap["nexus_latency"][0].Tags, map[string]string{"namespace": s.Namespace().String(), "method": "CancelNexusOperation", "outcome": tc.outcome})
		require.Contains(t, snap["nexus_latency"][0].Tags, "nexus_endpoint")

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
			if !tc.onlyByEndpoint {
				t.Run("ByNamespaceAndTaskQueue", func(t *testing.T) {
					testFn(t, tc, getDispatchByNsAndTqURL(s.HttpAPIAddress(), s.Namespace().String(), tc.endpoint.Spec.Target.GetWorker().TaskQueue))
				})
			}
			t.Run("ByEndpoint", func(t *testing.T) {
				testFn(t, tc, getDispatchByEndpointURL(s.HttpAPIAddress(), tc.endpoint.Id))
			})
		})
	}
}

func (s *NexusApiTestSuite) TestNexusStartOperation_WithNamespaceAndTaskQueue_SupportsVersioning() {
	ctx, cancel := context.WithCancel(testcore.NewContext())
	defer cancel()
	taskQueue := testcore.RandomizeStr("task-queue")
	err := s.SdkClient().UpdateWorkerBuildIdCompatibility(ctx, &sdkclient.UpdateWorkerBuildIdCompatibilityOptions{
		TaskQueue: taskQueue,
		Operation: &sdkclient.BuildIDOpAddNewIDInNewDefaultSet{BuildID: "old-build-id"},
	})
	s.NoError(err)
	err = s.SdkClient().UpdateWorkerBuildIdCompatibility(ctx, &sdkclient.UpdateWorkerBuildIdCompatibilityOptions{
		TaskQueue: taskQueue,
		Operation: &sdkclient.BuildIDOpAddNewIDInNewDefaultSet{BuildID: "new-build-id"},
	})
	s.NoError(err)

	u := getDispatchByNsAndTqURL(s.HttpAPIAddress(), s.Namespace().String(), taskQueue)
	client, err := nexusrpc.NewHTTPClient(nexusrpc.HTTPClientOptions{BaseURL: u, Service: "test-service"})
	s.NoError(err)
	// Versioned poller gets task
	pollerErrCh1 := s.versionedNexusTaskPoller(ctx, taskQueue, "new-build-id", nexusEchoHandler)

	result, err := nexusrpc.StartOperation(ctx, client, op, "input", nexus.StartOperationOptions{})
	s.NoError(err)
	s.Equal("input", result.Successful)
	s.NoError(<-pollerErrCh1)

	// Unversioned poller doesn't get a task
	pollerErrCh2 := s.nexusTaskPoller(ctx, taskQueue, nexusEchoHandler)
	// Versioned poller gets task with wrong build ID
	pollerErrCh3 := s.versionedNexusTaskPoller(ctx, taskQueue, "old-build-id", nexusEchoHandler)

	timeoutCtx, timeoutCancel := context.WithTimeout(ctx, time.Second*2)
	defer timeoutCancel()
	_, err = nexusrpc.StartOperation(timeoutCtx, client, op, "input", nexus.StartOperationOptions{})
	if !errors.Is(err, context.DeadlineExceeded) {
		var handlerErr *nexus.HandlerError
		if !errors.As(err, &handlerErr) || handlerErr.Type != nexus.HandlerErrorTypeUpstreamTimeout {
			s.T().Fatal("expected a DeadlineExceeded or upstream timeout error")
		}
	}
	// Cancel the parent context to unblock the pollers that didn't receive a task.
	cancel()
	s.NoError(<-pollerErrCh2)
	s.NoError(<-pollerErrCh3)
}

func nexusEchoHandler(res *workflowservice.PollNexusTaskQueueResponse) (*nexusTaskResponse, error) {
	return &nexusTaskResponse{StartResult: &nexus.HandlerStartOperationResultSync[*commonpb.Payload]{Value: res.Request.GetStartOperation().GetPayload()}}, nil
}

func getDispatchByNsAndTqURL(address string, namespace string, taskQueue string) string {
	return fmt.Sprintf(
		"http://%s/%s",
		address,
		commonnexus.RouteDispatchNexusTaskByNamespaceAndTaskQueue.
			Path(commonnexus.NamespaceAndTaskQueue{
				Namespace: namespace,
				TaskQueue: taskQueue,
			}),
	)
}

func getDispatchByEndpointURL(address string, endpoint string) string {
	return fmt.Sprintf("http://%s/%s", address, commonnexus.RouteDispatchNexusTaskByEndpoint.Path(endpoint))
}
