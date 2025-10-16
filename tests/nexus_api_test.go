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
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/common/authorization"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	cnexus "go.temporal.io/server/common/nexus"
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

func TestNexusApiTestSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(NexusApiTestSuite))
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
	callerNexusLink := nexusoperations.ConvertLinkWorkflowEventToNexusLink(callerLink)

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
	handlerNexusLink := nexusoperations.ConvertLinkWorkflowEventToNexusLink(handlerLink)
	asyncSuccessEndpoint := testcore.RandomizeStr("test-endpoint")

	type testcase struct {
		name           string
		outcome        string
		endpoint       *nexuspb.Endpoint
		timeout        time.Duration
		handler        func(*workflowservice.PollNexusTaskQueueResponse) (*nexuspb.Response, *nexuspb.HandlerError)
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
			handler: func(res *workflowservice.PollNexusTaskQueueResponse) (*nexuspb.Response, *nexuspb.HandlerError) {
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
				return &nexuspb.Response{
					Variant: &nexuspb.Response_StartOperation{
						StartOperation: &nexuspb.StartOperationResponse{
							Variant: &nexuspb.StartOperationResponse_AsyncSuccess{
								AsyncSuccess: &nexuspb.StartOperationResponse_Async{
									OperationToken: "test-token",
									Links: []*nexuspb.Link{{
										Url:  handlerNexusLink.URL.String(),
										Type: string(handlerLink.ProtoReflect().Descriptor().FullName()),
									}},
								},
							},
						},
					},
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
			outcome:  "operation_error",
			endpoint: s.createNexusEndpoint(testcore.RandomizeStr("test-endpoint"), testcore.RandomizeStr("task-queue")),
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
			assertion: func(t *testing.T, res *nexusrpc.ClientStartOperationResponse[string], err error, headers http.Header) {
				var operationError *nexus.OperationError
				require.ErrorAs(t, err, &operationError)
				require.Equal(t, nexus.OperationStateFailed, operationError.State)
				require.Equal(t, "deliberate test failure", operationError.Cause.Error())
				var failureErr *nexus.FailureError
				require.ErrorAs(t, operationError.Cause, &failureErr)
				require.Equal(t, map[string]string{"k": "v"}, failureErr.Failure.Metadata)
				var details string
				err = json.Unmarshal(failureErr.Failure.Details, &details)
				require.NoError(t, err)
				require.Equal(t, "details", details)
			},
		},
		{
			name:     "handler_error",
			outcome:  "handler_error:INTERNAL",
			endpoint: s.createNexusEndpoint(testcore.RandomizeStr("test-endpoint"), testcore.RandomizeStr("task-queue")),
			handler: func(res *workflowservice.PollNexusTaskQueueResponse) (*nexuspb.Response, *nexuspb.HandlerError) {
				return nil, &nexuspb.HandlerError{
					ErrorType: string(nexus.HandlerErrorTypeInternal),
					Failure:   &nexuspb.Failure{Message: "deliberate internal failure"},
				}
			},
			assertion: func(t *testing.T, res *nexusrpc.ClientStartOperationResponse[string], err error, headers http.Header) {
				var handlerErr *nexus.HandlerError
				require.ErrorAs(t, err, &handlerErr)
				require.Equal(t, nexus.HandlerErrorTypeInternal, handlerErr.Type)
				require.Equal(t, nexus.HandlerErrorRetryBehaviorUnspecified, handlerErr.RetryBehavior)
				require.Equal(t, "worker", headers.Get("Temporal-Nexus-Failure-Source"))
				require.Equal(t, "deliberate internal failure", handlerErr.Cause.Error())
			},
		},
		{
			name:     "handler_error_non_retryable",
			outcome:  "handler_error:INTERNAL",
			endpoint: s.createNexusEndpoint(testcore.RandomizeStr("test-endpoint"), testcore.RandomizeStr("task-queue")),
			handler: func(res *workflowservice.PollNexusTaskQueueResponse) (*nexuspb.Response, *nexuspb.HandlerError) {
				return nil, &nexuspb.HandlerError{
					ErrorType:     string(nexus.HandlerErrorTypeInternal),
					Failure:       &nexuspb.Failure{Message: "deliberate internal failure"},
					RetryBehavior: enumspb.NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_NON_RETRYABLE,
				}
			},
			assertion: func(t *testing.T, res *nexusrpc.ClientStartOperationResponse[string], err error, headers http.Header) {
				var handlerErr *nexus.HandlerError
				require.ErrorAs(t, err, &handlerErr)
				require.Equal(t, nexus.HandlerErrorTypeInternal, handlerErr.Type)
				require.Equal(t, nexus.HandlerErrorRetryBehaviorNonRetryable, handlerErr.RetryBehavior)
				require.Equal(t, "worker", headers.Get("Temporal-Nexus-Failure-Source"))
				require.Equal(t, "deliberate internal failure", handlerErr.Cause.Error())
			},
		},
		{
			name:     "handler_timeout",
			outcome:  "handler_timeout",
			endpoint: s.createNexusEndpoint(testcore.RandomizeStr("test-service"), testcore.RandomizeStr("task-queue")),
			timeout:  2 * time.Second,
			handler: func(res *workflowservice.PollNexusTaskQueueResponse) (*nexuspb.Response, *nexuspb.HandlerError) {
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
				require.Equal(t, "upstream timeout", handlerErr.Cause.Error())
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

		go s.nexusTaskPoller(ctx, tc.endpoint.Spec.Target.GetWorker().TaskQueue, tc.handler)

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

func (s *NexusApiTestSuite) TestNexusStartOperation_WithNamespaceAndTaskQueue_NamespaceNotFound() {
	// Also use this test to verify that namespaces are unescaped in the path.
	taskQueue := testcore.RandomizeStr("task-queue")
	namespace := "namespace not/found"
	u := getDispatchByNsAndTqURL(s.HttpAPIAddress(), namespace, taskQueue)
	client, err := nexusrpc.NewHTTPClient(nexusrpc.HTTPClientOptions{BaseURL: u, Service: "test-service"})
	s.NoError(err)
	ctx := testcore.NewContext()
	capture := s.GetTestCluster().Host().CaptureMetricsHandler().StartCapture()
	defer s.GetTestCluster().Host().CaptureMetricsHandler().StopCapture(capture)
	_, err = nexusrpc.StartOperation(ctx, client, op, "input", nexus.StartOperationOptions{})
	var handlerError *nexus.HandlerError
	s.ErrorAs(err, &handlerError)
	s.Equal(nexus.HandlerErrorTypeNotFound, handlerError.Type)
	s.Equal(fmt.Sprintf("namespace not found: %q", namespace), handlerError.Cause.Error())

	snap := capture.Snapshot()

	s.Equal(1, len(snap["nexus_requests"]))
	s.Equal(map[string]string{"namespace": namespace, "method": "StartNexusOperation", "outcome": "namespace_not_found", "nexus_endpoint": "_unknown_"}, snap["nexus_requests"][0].Tags)
	s.Equal(int64(1), snap["nexus_requests"][0].Value)
}

func (s *NexusApiTestSuite) TestNexusStartOperation_WithNamespaceAndTaskQueue_NamespaceTooLong() {
	taskQueue := testcore.RandomizeStr("task-queue")

	var namespace string
	for i := 0; i < 500; i++ {
		namespace += "namespace-is-a-very-long-string"
	}

	u := getDispatchByNsAndTqURL(s.HttpAPIAddress(), namespace, taskQueue)
	client, err := nexusrpc.NewHTTPClient(nexusrpc.HTTPClientOptions{BaseURL: u, Service: "test-service"})
	s.NoError(err)
	ctx := testcore.NewContext()
	capture := s.GetTestCluster().Host().CaptureMetricsHandler().StartCapture()
	defer s.GetTestCluster().Host().CaptureMetricsHandler().StopCapture(capture)
	_, err = nexusrpc.StartOperation(ctx, client, op, "input", nexus.StartOperationOptions{})
	var handlerErr *nexus.HandlerError
	s.ErrorAs(err, &handlerErr)
	s.Equal(nexus.HandlerErrorTypeBadRequest, handlerErr.Type)
	// I wish we'd never put periods in error messages :(
	s.Equal("Namespace length exceeds limit.", handlerErr.Cause.Error())

	snap := capture.Snapshot()

	s.Equal(1, len(snap["nexus_request_preprocess_errors"]))
}

func (s *NexusApiTestSuite) TestNexusStartOperation_Forbidden() {
	taskQueue := testcore.RandomizeStr("task-queue")
	testEndpoint := s.createNexusEndpoint(testcore.RandomizeStr("test-endpoint"), taskQueue)

	type testcase struct {
		name                   string
		onAuthorize            func(context.Context, *authorization.Claims, *authorization.CallTarget) (authorization.Result, error)
		checkFailure           func(t *testing.T, handlerErr *nexus.HandlerError)
		exposeAuthorizerErrors bool
		expectedOutcomeMetric  string
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
			checkFailure: func(t *testing.T, handlerErr *nexus.HandlerError) {
				require.Equal(t, nexus.HandlerErrorTypeUnauthorized, handlerErr.Type)
				require.Equal(t, "permission denied: unauthorized in test", handlerErr.Cause.Error())
			},
			expectedOutcomeMetric:  "unauthorized",
			exposeAuthorizerErrors: false,
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
			checkFailure: func(t *testing.T, handlerErr *nexus.HandlerError) {
				require.Equal(t, nexus.HandlerErrorTypeUnauthorized, handlerErr.Type)
				require.Equal(t, "permission denied", handlerErr.Cause.Error())
			},
			expectedOutcomeMetric:  "unauthorized",
			exposeAuthorizerErrors: false,
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
			checkFailure: func(t *testing.T, handlerErr *nexus.HandlerError) {
				require.Equal(t, nexus.HandlerErrorTypeUnauthorized, handlerErr.Type)
				require.Equal(t, "permission denied", handlerErr.Cause.Error())
			},
			expectedOutcomeMetric:  "unauthorized",
			exposeAuthorizerErrors: false,
		},
		{
			name: "deny with exposed error",
			onAuthorize: func(ctx context.Context, c *authorization.Claims, ct *authorization.CallTarget) (authorization.Result, error) {
				if ct.APIName == configs.DispatchNexusTaskByNamespaceAndTaskQueueAPIName {
					return authorization.Result{}, nexus.HandlerErrorf(nexus.HandlerErrorTypeUnavailable, "exposed error")
				}
				if ct.APIName == configs.DispatchNexusTaskByEndpointAPIName {
					if ct.NexusEndpointName != testEndpoint.Spec.Name {
						panic("expected nexus endpoint name")
					}
					return authorization.Result{}, nexus.HandlerErrorf(nexus.HandlerErrorTypeUnavailable, "exposed error")
				}
				return authorization.Result{Decision: authorization.DecisionAllow}, nil
			},
			checkFailure: func(t *testing.T, handlerErr *nexus.HandlerError) {
				require.Equal(t, nexus.HandlerErrorTypeUnavailable, handlerErr.Type)
				require.Equal(t, "exposed error", handlerErr.Cause.Error())
			},
			expectedOutcomeMetric:  "internal_auth_error",
			exposeAuthorizerErrors: true,
		},
	}

	testFn := func(t *testing.T, tc testcase, dispatchURL string) {
		client, err := nexusrpc.NewHTTPClient(nexusrpc.HTTPClientOptions{BaseURL: dispatchURL, Service: "test-service"})
		require.NoError(t, err)
		ctx := testcore.NewContext()

		capture := s.GetTestCluster().Host().CaptureMetricsHandler().StartCapture()
		defer s.GetTestCluster().Host().CaptureMetricsHandler().StopCapture(capture)

		s.OverrideDynamicConfig(dynamicconfig.ExposeAuthorizerErrors, tc.exposeAuthorizerErrors)

		// Wait until the endpoint is loaded into the registry.
		s.Eventually(func() bool {
			_, err = nexusrpc.StartOperation(ctx, client, op, "input", nexus.StartOperationOptions{})
			var handlerErr *nexus.HandlerError
			return err == nil || !(errors.As(err, &handlerErr) && handlerErr.Type == nexus.HandlerErrorTypeNotFound)
		}, 10*time.Second, 1*time.Second)

		var handlerErr *nexus.HandlerError
		require.ErrorAs(t, err, &handlerErr)
		tc.checkFailure(t, handlerErr)

		snap := capture.Snapshot()

		require.Equal(t, 1, len(snap["nexus_requests"]))
		require.Subset(t, snap["nexus_requests"][0].Tags, map[string]string{"namespace": s.Namespace().String(), "method": "StartNexusOperation", "outcome": tc.expectedOutcomeMetric})
		require.Equal(t, int64(1), snap["nexus_requests"][0].Value)
	}

	for _, tc := range testCases {
		s.T().Run(tc.name, func(t *testing.T) {
			s.GetTestCluster().Host().SetOnAuthorize(tc.onAuthorize)
			defer s.GetTestCluster().Host().SetOnAuthorize(nil)

			t.Run("ByNamespaceAndTaskQueue", func(t *testing.T) {
				testFn(t, tc, getDispatchByNsAndTqURL(s.HttpAPIAddress(), s.Namespace().String(), taskQueue))
			})
			t.Run("ByEndpoint", func(t *testing.T) {
				testFn(t, tc, getDispatchByEndpointURL(s.HttpAPIAddress(), testEndpoint.Id))
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
		handler   func(*workflowservice.PollNexusTaskQueueResponse) (*nexuspb.Response, *nexuspb.HandlerError)
		assertion func(*testing.T, *nexusrpc.ClientStartOperationResponse[string], error, map[string][]*metricstest.CapturedRecording)
	}
	testCases := []testcase{
		{
			name: "no header",
			assertion: func(t *testing.T, res *nexusrpc.ClientStartOperationResponse[string], err error, snap map[string][]*metricstest.CapturedRecording) {
				var handlerErr *nexus.HandlerError
				require.ErrorAs(t, err, &handlerErr)
				require.Equal(t, nexus.HandlerErrorTypeUnauthorized, handlerErr.Type)
				require.Equal(t, "permission denied", handlerErr.Cause.Error())
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
				require.Equal(t, "unauthorized", handlerErr.Cause.Error())
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

		if tc.handler != nil {
			// only set on valid request
			go s.nexusTaskPoller(ctx, taskQueue, tc.handler)
		}

		var result *nexusrpc.ClientStartOperationResponse[string]
		var snap map[string][]*metricstest.CapturedRecording

		// Wait until the endpoint is loaded into the registry.
		s.Eventually(func() bool {
			capture := s.GetTestCluster().Host().CaptureMetricsHandler().StartCapture()
			defer s.GetTestCluster().Host().CaptureMetricsHandler().StopCapture(capture)

			result, err = nexusrpc.StartOperation(ctx, client, op, "input", nexus.StartOperationOptions{
				Header: tc.header,
			})
			snap = capture.Snapshot()
			var handlerErr *nexus.HandlerError
			return err == nil || !(errors.As(err, &handlerErr) && handlerErr.Type == nexus.HandlerErrorTypeNotFound)
		}, 10*time.Second, 1*time.Second)

		tc.assertion(t, result, err, snap)
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

func (s *NexusApiTestSuite) TestNexusStartOperation_PayloadSizeLimit() {
	taskQueue := testcore.RandomizeStr("task-queue")
	testEndpoint := s.createNexusEndpoint(testcore.RandomizeStr("test-endpoint"), taskQueue)

	// Use -10 to avoid hitting MaxNexusAPIRequestBodyBytes. Actual payload will still exceed limit because of
	// additional Content headers. See common/rpc/grpc.go:66
	input := strings.Repeat("a", (2*1024*1024)-10)

	testFn := func(t *testing.T, dispatchURL string) {
		ctx, cancel := context.WithCancel(testcore.NewContext())
		defer cancel()

		client, err := nexusrpc.NewHTTPClient(nexusrpc.HTTPClientOptions{BaseURL: dispatchURL, Service: "test-service"})
		require.NoError(t, err)
		capture := s.GetTestCluster().Host().CaptureMetricsHandler().StartCapture()
		defer s.GetTestCluster().Host().CaptureMetricsHandler().StopCapture(capture)

		var result *nexusrpc.ClientStartOperationResponse[string]

		// Wait until the endpoint is loaded into the registry.
		s.Eventually(func() bool {
			result, err = nexusrpc.StartOperation(ctx, client, op, input, nexus.StartOperationOptions{
				CallbackURL: "http://localhost/callback",
				RequestID:   "request-id",
			})
			var handlerErr *nexus.HandlerError
			return err == nil || !(errors.As(err, &handlerErr) && handlerErr.Type == nexus.HandlerErrorTypeNotFound)
		}, 10*time.Second, 500*time.Millisecond)

		require.Nil(t, result)
		var handlerErr *nexus.HandlerError
		require.ErrorAs(t, err, &handlerErr)
		require.Equal(t, nexus.HandlerErrorTypeBadRequest, handlerErr.Type)
		require.Equal(t, "input exceeds size limit", handlerErr.Cause.Error())
	}

	s.T().Run("ByNamespaceAndTaskQueue", func(t *testing.T) {
		testFn(t, getDispatchByNsAndTqURL(s.HttpAPIAddress(), s.Namespace().String(), taskQueue))
	})
	s.T().Run("ByEndpoint", func(t *testing.T) {
		testFn(t, getDispatchByEndpointURL(s.HttpAPIAddress(), testEndpoint.Id))
	})
}

func (s *NexusApiTestSuite) TestNexusCancelOperation_Outcomes() {
	asyncSuccessEndpoint := testcore.RandomizeStr("async-success-endpoint")

	type testcase struct {
		outcome        string
		onlyByEndpoint bool
		endpoint       *nexuspb.Endpoint
		timeout        time.Duration
		handler        func(*workflowservice.PollNexusTaskQueueResponse) (*nexuspb.Response, *nexuspb.HandlerError)
		assertion      func(*testing.T, error, http.Header)
	}

	testCases := []testcase{
		{
			outcome:        "success",
			onlyByEndpoint: true,
			endpoint:       s.createNexusEndpoint(asyncSuccessEndpoint, testcore.RandomizeStr("task-queue")),
			handler: func(res *workflowservice.PollNexusTaskQueueResponse) (*nexuspb.Response, *nexuspb.HandlerError) {
				s.Equal(asyncSuccessEndpoint, res.Request.Endpoint)
				// Choose an arbitrary test case to assert that all of the input is delivered to the
				// poll response.
				op := res.Request.Variant.(*nexuspb.Request_CancelOperation).CancelOperation
				s.Equal("operation", op.Operation)
				s.Equal("token", op.OperationToken)
				s.Equal("value", res.Request.Header["key"])
				return &nexuspb.Response{
					Variant: &nexuspb.Response_CancelOperation{
						CancelOperation: &nexuspb.CancelOperationResponse{},
					},
				}, nil
			},
			assertion: func(t *testing.T, err error, headers http.Header) {
				require.NoError(t, err)
			},
		},
		{
			outcome:  "handler_error:INTERNAL",
			endpoint: s.createNexusEndpoint(testcore.RandomizeStr("test-endpoint"), testcore.RandomizeStr("task-queue")),
			handler: func(res *workflowservice.PollNexusTaskQueueResponse) (*nexuspb.Response, *nexuspb.HandlerError) {
				return nil, &nexuspb.HandlerError{
					ErrorType: string(nexus.HandlerErrorTypeInternal),
					Failure:   &nexuspb.Failure{Message: "deliberate internal failure"},
				}
			},
			assertion: func(t *testing.T, err error, headers http.Header) {
				var handlerErr *nexus.HandlerError
				require.ErrorAs(t, err, &handlerErr)
				require.Equal(t, nexus.HandlerErrorTypeInternal, handlerErr.Type)
				require.Equal(t, "worker", headers.Get("Temporal-Nexus-Failure-Source"))
				require.Equal(t, "deliberate internal failure", handlerErr.Cause.Error())
			},
		},
		{
			outcome:  "handler_timeout",
			endpoint: s.createNexusEndpoint(testcore.RandomizeStr("test-service"), testcore.RandomizeStr("task-queue")),
			timeout:  2 * time.Second,
			handler: func(res *workflowservice.PollNexusTaskQueueResponse) (*nexuspb.Response, *nexuspb.HandlerError) {
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
				require.Equal(t, "upstream timeout", handlerErr.Cause.Error())
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

		go s.nexusTaskPoller(ctx, tc.endpoint.Spec.Target.GetWorker().TaskQueue, tc.handler)

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
	go s.versionedNexusTaskPoller(ctx, taskQueue, "new-build-id", nexusEchoHandler)

	result, err := nexusrpc.StartOperation(ctx, client, op, "input", nexus.StartOperationOptions{})
	s.NoError(err)
	s.Equal("input", result.Successful)

	// Unversioned poller doesn't get a task
	go s.nexusTaskPoller(ctx, taskQueue, nexusEchoHandler)
	// Versioned poller gets task with wrong build ID
	go s.versionedNexusTaskPoller(ctx, taskQueue, "old-build-id", nexusEchoHandler)

	ctx, cancel = context.WithTimeout(ctx, time.Second*2)
	defer cancel()
	_, err = nexusrpc.StartOperation(ctx, client, op, "input", nexus.StartOperationOptions{})
	if !errors.Is(err, context.DeadlineExceeded) {
		var handlerErr *nexus.HandlerError
		if !errors.As(err, &handlerErr) || handlerErr.Type != nexus.HandlerErrorTypeUpstreamTimeout {
			s.T().Fatal("expected a DeadlineExceeded or upstream timeout error")
		}
	}
}

func (s *NexusApiTestSuite) TestNexus_RespondNexusTaskMethods_VerifiesTaskTokenMatchesRequestNamespace() {
	ctx := testcore.NewContext()

	tt := tokenspb.NexusTask{
		NamespaceId: s.NamespaceID().String(),
		TaskQueue:   "test",
		TaskId:      uuid.NewString(),
	}
	ttBytes, err := tt.Marshal()
	s.NoError(err)

	_, err = s.GetTestCluster().FrontendClient().RespondNexusTaskCompleted(ctx, &workflowservice.RespondNexusTaskCompletedRequest{
		Namespace: s.ExternalNamespace().String(),
		Identity:  uuid.NewString(),
		TaskToken: ttBytes,
		Response:  &nexuspb.Response{},
	})
	s.ErrorContains(err, "Operation requested with a token from a different namespace.")

	_, err = s.GetTestCluster().FrontendClient().RespondNexusTaskFailed(ctx, &workflowservice.RespondNexusTaskFailedRequest{
		Namespace: s.ExternalNamespace().String(),
		Identity:  uuid.NewString(),
		TaskToken: ttBytes,
		Error:     &nexuspb.HandlerError{},
	})
	s.ErrorContains(err, "Operation requested with a token from a different namespace.")
}

func (s *NexusApiTestSuite) TestNexus_RespondNexusTaskCompleted_ValidateOperationTokenLength() {
	ctx := testcore.NewContext()

	tt := tokenspb.NexusTask{
		NamespaceId: s.NamespaceID().String(),
		TaskQueue:   "test",
		TaskId:      uuid.NewString(),
	}
	ttBytes, err := tt.Marshal()
	s.NoError(err)

	_, err = s.GetTestCluster().FrontendClient().RespondNexusTaskCompleted(ctx, &workflowservice.RespondNexusTaskCompletedRequest{
		Namespace: s.Namespace().String(),
		Identity:  uuid.NewString(),
		TaskToken: ttBytes,
		Response: &nexuspb.Response{
			Variant: &nexuspb.Response_StartOperation{
				StartOperation: &nexuspb.StartOperationResponse{
					Variant: &nexuspb.StartOperationResponse_AsyncSuccess{
						AsyncSuccess: &nexuspb.StartOperationResponse_Async{
							OperationToken: strings.Repeat("long", 2000),
						},
					},
				},
			},
		},
	})
	var invalidArgumentErr *serviceerror.InvalidArgument
	s.ErrorAs(err, &invalidArgumentErr)
	s.Equal("operation token length exceeds allowed limit (8000/4096)", invalidArgumentErr.Message)
}

func (s *NexusApiTestSuite) TestNexus_RespondNexusTaskMethods_ValidateFailureDetailsJSON() {
	ctx := testcore.NewContext()

	tt := tokenspb.NexusTask{
		NamespaceId: s.NamespaceID().String(),
		TaskQueue:   "test",
		TaskId:      uuid.NewString(),
	}
	ttBytes, err := tt.Marshal()
	s.NoError(err)

	_, err = s.GetTestCluster().FrontendClient().RespondNexusTaskCompleted(ctx, &workflowservice.RespondNexusTaskCompletedRequest{
		Namespace: s.Namespace().String(),
		Identity:  uuid.NewString(),
		TaskToken: ttBytes,
		Response: &nexuspb.Response{
			Variant: &nexuspb.Response_StartOperation{
				StartOperation: &nexuspb.StartOperationResponse{
					Variant: &nexuspb.StartOperationResponse_OperationError{
						OperationError: &nexuspb.UnsuccessfulOperationError{
							OperationState: string(nexus.OperationStateFailed),
							Failure: &nexuspb.Failure{
								Details: []byte("not valid JSON"),
							},
						},
					},
				},
			},
		},
	})
	var invalidArgumentErr *serviceerror.InvalidArgument
	s.ErrorAs(err, &invalidArgumentErr)
	s.Equal("failure details must be JSON serializable", invalidArgumentErr.Message)

	_, err = s.GetTestCluster().FrontendClient().RespondNexusTaskFailed(ctx, &workflowservice.RespondNexusTaskFailedRequest{
		Namespace: s.Namespace().String(),
		Identity:  uuid.NewString(),
		TaskToken: ttBytes,
		Error: &nexuspb.HandlerError{
			Failure: &nexuspb.Failure{
				Details: []byte("not valid JSON"),
			},
		},
	})
	s.ErrorAs(err, &invalidArgumentErr)
	s.Equal("failure details must be JSON serializable", invalidArgumentErr.Message)
}

func (s *NexusApiTestSuite) TestNexusStartOperation_ByEndpoint_EndpointNotFound() {
	u := getDispatchByEndpointURL(s.HttpAPIAddress(), uuid.NewString())
	client, err := nexusrpc.NewHTTPClient(nexusrpc.HTTPClientOptions{BaseURL: u, Service: "test-service"})
	s.NoError(err)
	ctx := testcore.NewContext()
	capture := s.GetTestCluster().Host().CaptureMetricsHandler().StartCapture()
	defer s.GetTestCluster().Host().CaptureMetricsHandler().StopCapture(capture)
	_, err = nexusrpc.StartOperation(ctx, client, op, "input", nexus.StartOperationOptions{})
	var handlerErr *nexus.HandlerError
	s.ErrorAs(err, &handlerErr)
	s.Equal(nexus.HandlerErrorTypeNotFound, handlerErr.Type)
	s.Equal("nexus endpoint not found", handlerErr.Cause.Error())
	snap := capture.Snapshot()
	s.Equal(1, len(snap["nexus_request_preprocess_errors"]))
}

func (s *NexusApiTestSuite) versionedNexusTaskPoller(ctx context.Context, taskQueue, buildID string, handler func(*workflowservice.PollNexusTaskQueueResponse) (*nexuspb.Response, *nexuspb.HandlerError)) {
	var vc *commonpb.WorkerVersionCapabilities

	if buildID != "" {
		vc = &commonpb.WorkerVersionCapabilities{
			BuildId:       buildID,
			UseVersioning: true,
		}
	}
	res, err := s.GetTestCluster().FrontendClient().PollNexusTaskQueue(ctx, &workflowservice.PollNexusTaskQueueRequest{
		Namespace: s.Namespace().String(),
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
	// Got an empty response, just return.
	if res.TaskToken == nil {
		return
	}
	if res.Request.GetStartOperation().GetService() != "test-service" && res.Request.GetCancelOperation().GetService() != "test-service" {
		panic("expected service to be test-service")
	}
	response, handlerError := handler(res)
	if handlerError != nil {
		_, err = s.GetTestCluster().FrontendClient().RespondNexusTaskFailed(ctx, &workflowservice.RespondNexusTaskFailedRequest{
			Namespace: s.Namespace().String(),
			Identity:  uuid.NewString(),
			TaskToken: res.TaskToken,
			Error:     handlerError,
		})
		// There's no clean way to propagate this error back to the test that's worthwhile. Panic is good enough.
		// NotFound is possible if the task got timed out/canceled while we were processing it.
		if err != nil && ctx.Err() == nil && !errors.As(err, new(*serviceerror.NotFound)) {
			panic(err)
		}
	} else if response != nil {
		_, err = s.GetTestCluster().FrontendClient().RespondNexusTaskCompleted(ctx, &workflowservice.RespondNexusTaskCompletedRequest{
			Namespace: s.Namespace().String(),
			Identity:  uuid.NewString(),
			TaskToken: res.TaskToken,
			Response:  response,
		})
		// There's no clean way to propagate this error back to the test that's worthwhile. Panic is good enough.
		// NotFound is possible if the task got timed out/canceled while we were processing it.
		if err != nil && ctx.Err() == nil && !errors.As(err, new(*serviceerror.NotFound)) {
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

func (s *NexusApiTestSuite) createNexusEndpoint(name string, taskQueue string) *nexuspb.Endpoint {
	resp, err := s.OperatorClient().CreateNexusEndpoint(testcore.NewContext(), &operatorservice.CreateNexusEndpointRequest{
		Spec: &nexuspb.EndpointSpec{
			Name: name,
			Target: &nexuspb.EndpointTarget{
				Variant: &nexuspb.EndpointTarget_Worker_{
					Worker: &nexuspb.EndpointTarget_Worker{
						Namespace: s.Namespace().String(),
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
