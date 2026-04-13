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
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/server/common/authorization"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexusrpc"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/components/nexusoperations"
	"go.temporal.io/server/service/frontend/configs"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/grpc/metadata"
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
	parallelsuite.Suite[*NexusApiTestSuite]
}

func TestNexusApiTestSuiteWithLegacyErrorPaths(t *testing.T) {
	parallelsuite.Run(t, &NexusApiTestSuite{}, false) // useTemporalFailures = false
}

func TestNexusApiTestSuiteWithTemporalFailures(t *testing.T) {
	parallelsuite.Run(t, &NexusApiTestSuite{}, true) // useTemporalFailures = true
}

func (s *NexusApiTestSuite) TestNexusStartOperation_Outcomes(useTemporalFailures bool) {
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
	if useTemporalFailures {
		operationErrorOutcome = "failure"
	}

	type testcase struct {
		name           string
		outcome        string
		endpointName   string
		timeout        time.Duration
		handler        nexusTaskHandler
		assertion      func(*NexusApiTestSuite, *nexusrpc.ClientStartOperationResponse[string], error, http.Header)
		onlyByEndpoint bool
	}

	testCases := []testcase{
		{
			name:         "sync_success",
			outcome:      "sync_success",
			endpointName: testcore.RandomizeStr("test-endpoint"),
			handler:      nexusEchoHandler,
			assertion: func(s *NexusApiTestSuite, res *nexusrpc.ClientStartOperationResponse[string], err error, headers http.Header) {
				s.NoError(err)
				s.Equal("input", res.Successful)
			},
		},
		{
			name:           "async_success",
			outcome:        "async_success",
			onlyByEndpoint: true,
			endpointName:   asyncSuccessEndpoint,
			handler: func(t *testing.T, res *workflowservice.PollNexusTaskQueueResponse) (*nexusTaskResponse, error) {
				// Choose an arbitrary test case to assert that all of the input is delivered to the
				// poll response.
				require.Equal(t, asyncSuccessEndpoint, res.Request.Endpoint)
				start := res.Request.Variant.(*nexuspb.Request_StartOperation).StartOperation
				require.Equal(t, op.Name(), start.Operation)
				require.Equal(t, "http://localhost/callback", start.Callback)
				require.Equal(t, "request-id", start.RequestId)
				require.Equal(t, "value", res.Request.Header["key"])
				require.NotContains(t, res.Request.Header, "temporal-nexus-failure-support")
				require.Len(t, start.GetLinks(), 1)
				require.Equal(t, callerNexusLink.URL.String(), start.Links[0].GetUrl())
				require.Equal(t, callerNexusLink.Type, start.Links[0].Type)
				return &nexusTaskResponse{
					StartResult: &nexus.HandlerStartOperationResultAsync{OperationToken: "test-token"},
					Links:       []nexus.Link{handlerNexusLink},
				}, nil
			},
			assertion: func(s *NexusApiTestSuite, res *nexusrpc.ClientStartOperationResponse[string], err error, headers http.Header) {
				s.NoError(err)
				s.Equal("test-token", res.Pending.Token)
				s.Len(res.Links, 1)
				s.Equal(handlerNexusLink.URL.String(), res.Links[0].URL.String())
				s.Equal(handlerNexusLink.Type, res.Links[0].Type)
			},
		},
		{
			name:         "operation_error",
			outcome:      operationErrorOutcome,
			endpointName: testcore.RandomizeStr("test-endpoint"),
			handler: func(_ *testing.T, _ *workflowservice.PollNexusTaskQueueResponse) (*nexusTaskResponse, error) {
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
			assertion: func(s *NexusApiTestSuite, res *nexusrpc.ClientStartOperationResponse[string], err error, headers http.Header) {
				var operationError *nexus.OperationError
				s.ErrorAs(err, &operationError)
				s.Equal(nexus.OperationStateFailed, operationError.State)
				if useTemporalFailures {
					// Through the Temporal failure round-trip, the cause chain has an extra wrapper
					// for the OperationError's ApplicationFailureInfo.
					var failureErr *nexus.FailureError
					s.ErrorAs(operationError.Cause, &failureErr)
					var innerErr *nexus.FailureError
					s.ErrorAs(failureErr.Cause, &innerErr)
					tFailure, err := commonnexus.NexusFailureToTemporalFailure(innerErr.Failure)
					s.NoError(err)
					convErr := temporal.GetDefaultFailureConverter().FailureToError(tFailure)
					var appErr *temporal.ApplicationError
					s.ErrorAs(convErr, &appErr)
					s.Equal("deliberate test failure", appErr.Message())
					var details nexus.Failure
					s.NoError(appErr.Details(&details))
					s.Equal("v", details.Metadata["k"])
				} else {
					s.Equal("deliberate test failure", operationError.Cause.Error())
					var failureErr *nexus.FailureError
					s.ErrorAs(operationError.Cause, &failureErr)
					s.Equal(map[string]string{"k": "v"}, failureErr.Failure.Metadata)
					var details string
					err = json.Unmarshal(failureErr.Failure.Details, &details)
					s.NoError(err)
					s.Equal("details", details)
				}
			},
		},
		{
			name:         "handler_error",
			outcome:      "handler_error:INTERNAL",
			endpointName: testcore.RandomizeStr("test-endpoint"),
			handler: func(_ *testing.T, _ *workflowservice.PollNexusTaskQueueResponse) (*nexusTaskResponse, error) {
				return nil, &nexus.HandlerError{
					Type: nexus.HandlerErrorTypeInternal,
					Cause: &nexus.FailureError{
						Failure: nexus.Failure{Message: "deliberate internal failure"},
					},
				}
			},
			assertion: func(s *NexusApiTestSuite, res *nexusrpc.ClientStartOperationResponse[string], err error, headers http.Header) {
				var handlerErr *nexus.HandlerError
				s.ErrorAs(err, &handlerErr)
				s.Equal(nexus.HandlerErrorTypeInternal, handlerErr.Type)
				s.Equal(nexus.HandlerErrorRetryBehaviorUnspecified, handlerErr.RetryBehavior)
				s.Equal("worker", headers.Get("Temporal-Nexus-Failure-Source"))
				s.Empty(handlerErr.Message)
				s.Error(handlerErr.Cause)
				s.Equal("deliberate internal failure", handlerErr.Cause.Error())
			},
		},
		{
			name:         "handler_error_non_retryable",
			outcome:      "handler_error:INTERNAL",
			endpointName: testcore.RandomizeStr("test-endpoint"),
			handler: func(_ *testing.T, _ *workflowservice.PollNexusTaskQueueResponse) (*nexusTaskResponse, error) {
				return nil, &nexus.HandlerError{
					Type:          nexus.HandlerErrorTypeInternal,
					RetryBehavior: nexus.HandlerErrorRetryBehaviorNonRetryable,
					Cause: &nexus.FailureError{
						Failure: nexus.Failure{Message: "deliberate internal failure"},
					},
				}
			},
			assertion: func(s *NexusApiTestSuite, res *nexusrpc.ClientStartOperationResponse[string], err error, headers http.Header) {
				var handlerErr *nexus.HandlerError
				s.ErrorAs(err, &handlerErr)
				s.Equal(nexus.HandlerErrorTypeInternal, handlerErr.Type)
				s.Equal(nexus.HandlerErrorRetryBehaviorNonRetryable, handlerErr.RetryBehavior)
				s.Equal("worker", headers.Get("Temporal-Nexus-Failure-Source"))
				s.Empty(handlerErr.Message)
				s.Error(handlerErr.Cause)
				s.Equal("deliberate internal failure", handlerErr.Cause.Error())
			},
		},
		{
			name:         "handler_timeout",
			outcome:      "handler_timeout",
			endpointName: testcore.RandomizeStr("test-service"),
			timeout:      2 * time.Second,
			handler: func(t *testing.T, res *workflowservice.PollNexusTaskQueueResponse) (*nexusTaskResponse, error) {
				timeoutStr, set := res.Request.Header[nexus.HeaderRequestTimeout]
				require.True(t, set)
				timeout, err := time.ParseDuration(timeoutStr)

				var dispatchTimeoutBuffer = nexusoperations.MinDispatchTaskTimeout.Get(dynamicconfig.NewNoopCollection())("test")
				expectedMaxTimeout := 2*time.Second - dispatchTimeoutBuffer
				require.LessOrEqual(t, timeout, expectedMaxTimeout, "timeout should be buffered")

				require.NoError(t, err)
				time.Sleep(timeout) //nolint:forbidigo // Allow time.Sleep for timeout tests
				return nil, nil
			},
			assertion: func(s *NexusApiTestSuite, res *nexusrpc.ClientStartOperationResponse[string], err error, header http.Header) {
				var handlerErr *nexus.HandlerError
				s.ErrorAs(err, &handlerErr)
				s.Equal(nexus.HandlerErrorTypeUpstreamTimeout, handlerErr.Type)
				s.Equal("upstream timeout", handlerErr.Message)
			},
		},
	}

	testFn := func(s *NexusApiTestSuite, tc testcase, dispatchOnlyByEndpoint bool) {
		env := newNexusTestEnv(s.T(), useTemporalFailures, testcore.WithDedicatedCluster())
		endpoint := env.createNexusEndpoint(s.T(), tc.endpointName, testcore.RandomizeStr("task-queue"))
		var dispatchURL string
		if dispatchOnlyByEndpoint {
			dispatchURL = getDispatchByEndpointURL(env.HttpAPIAddress(), endpoint.Id)
		} else {
			dispatchURL = getDispatchByNsAndTqURL(env.HttpAPIAddress(), env.Namespace().String(), endpoint.Spec.Target.GetWorker().TaskQueue)
		}
		ctx, cancel := context.WithCancel(testcore.NewContext())
		defer cancel()

		httpCaller, headerCapture := newHeaderCaptureCaller()
		client, err := nexusrpc.NewHTTPClient(nexusrpc.HTTPClientOptions{
			BaseURL:    dispatchURL,
			Service:    "test-service",
			HTTPCaller: httpCaller,
		})
		s.NoError(err)
		capture := env.GetTestCluster().Host().CaptureMetricsHandler().StartCapture()
		defer env.GetTestCluster().Host().CaptureMetricsHandler().StopCapture(capture)

		pollerErrCh := env.nexusTaskPoller(ctx, s.T(), endpoint.Spec.Target.GetWorker().TaskQueue, tc.handler)

		eventuallyTick := 500 * time.Millisecond
		header := nexus.Header{"key": "value", "temporal-nexus-failure-support": "true"}
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

		tc.assertion(s, result, err, headerCapture.lastHeaders)
		s.NoError(<-pollerErrCh)

		snap := capture.Snapshot()

		s.Len(snap["nexus_requests"], 1)
		s.Subset(snap["nexus_requests"][0].Tags, map[string]string{"namespace": env.Namespace().String(), "method": "StartNexusOperation", "outcome": tc.outcome})
		s.Contains(snap["nexus_requests"][0].Tags, "nexus_endpoint")
		s.Equal(int64(1), snap["nexus_requests"][0].Value)
		s.Equal(metrics.MetricUnit(""), snap["nexus_requests"][0].Unit)

		s.Len(snap["nexus_latency"], 1)
		s.Subset(snap["nexus_latency"][0].Tags, map[string]string{"namespace": env.Namespace().String(), "method": "StartNexusOperation", "outcome": tc.outcome})
		s.Contains(snap["nexus_latency"][0].Tags, "nexus_endpoint")

		// Ensure that StartOperation request is tracked as part of normal service telemetry metrics
		s.Condition(func() bool {
			for _, m := range snap["service_requests"] {
				if opTag, ok := m.Tags["operation"]; ok && opTag == "StartNexusOperation" {
					return true
				}
			}
			return false
		})
	}

	for _, tc := range testCases {
		s.Run(tc.name, func(s *NexusApiTestSuite) {
			if !tc.onlyByEndpoint {
				s.Run("ByNamespaceAndTaskQueue", func(s *NexusApiTestSuite) { testFn(s, tc, false) })
			}
			s.Run("ByEndpoint", func(s *NexusApiTestSuite) { testFn(s, tc, true) })
		})
	}
}

func (s *NexusApiTestSuite) TestNexusStartOperation_Claims(useTemporalFailures bool) {
	taskQueue := testcore.RandomizeStr("task-queue")

	type testcase struct {
		name      string
		header    nexus.Header
		handler   nexusTaskHandler
		assertion func(*NexusApiTestSuite, *nexusrpc.ClientStartOperationResponse[string], error, map[string][]*metricstest.CapturedRecording)
	}
	testCases := []testcase{
		{
			name: "no header",
			assertion: func(s *NexusApiTestSuite, res *nexusrpc.ClientStartOperationResponse[string], err error, snap map[string][]*metricstest.CapturedRecording) {
				var handlerErr *nexus.HandlerError
				s.ErrorAs(err, &handlerErr)
				s.Equal(nexus.HandlerErrorTypeUnauthorized, handlerErr.Type)
				s.Equal("permission denied", handlerErr.Message)
				s.Empty(snap["nexus_request_preprocess_errors"])
			},
		},
		{
			name: "invalid bearer",
			header: nexus.Header{
				"authorization": "Bearer invalid",
			},
			assertion: func(s *NexusApiTestSuite, res *nexusrpc.ClientStartOperationResponse[string], err error, snap map[string][]*metricstest.CapturedRecording) {
				var handlerErr *nexus.HandlerError
				s.ErrorAs(err, &handlerErr)
				s.Equal(nexus.HandlerErrorTypeUnauthenticated, handlerErr.Type)
				s.Equal("unauthorized", handlerErr.Message)
				s.Len(snap["nexus_request_preprocess_errors"], 1)
			},
		},
		{
			name: "valid bearer",
			header: nexus.Header{
				"authorization": "Bearer test",
			},
			handler: nexusEchoHandler,
			assertion: func(s *NexusApiTestSuite, res *nexusrpc.ClientStartOperationResponse[string], err error, snap map[string][]*metricstest.CapturedRecording) {
				s.NoError(err)
				s.Equal("input", res.Successful)
				s.Empty(snap["nexus_request_preprocess_errors"])
			},
		},
	}

	testFn := func(s *NexusApiTestSuite, tc testcase, dispatchOnlyByEndpoint bool) {
		env := newNexusTestEnv(s.T(), useTemporalFailures, testcore.WithDedicatedCluster())
		env.GetTestCluster().Host().SetOnAuthorize(func(ctx context.Context, c *authorization.Claims, ct *authorization.CallTarget) (authorization.Result, error) {
			if ct.APIName == configs.DispatchNexusTaskByNamespaceAndTaskQueueAPIName && (c == nil || c.Subject != "test") {
				return authorization.Result{Decision: authorization.DecisionDeny}, nil
			}
			if ct.APIName == configs.DispatchNexusTaskByEndpointAPIName && (c == nil || c.Subject != "test") {
				return authorization.Result{Decision: authorization.DecisionDeny}, nil
			}
			return authorization.Result{Decision: authorization.DecisionAllow}, nil
		})
		defer env.GetTestCluster().Host().SetOnAuthorize(nil)
		env.GetTestCluster().Host().SetOnGetClaims(func(ai *authorization.AuthInfo) (*authorization.Claims, error) {
			if ai.AuthToken != "Bearer test" {
				return nil, errors.New("invalid auth token")
			}
			return &authorization.Claims{Subject: "test"}, nil
		})
		defer env.GetTestCluster().Host().SetOnGetClaims(nil)

		testEndpoint := env.createNexusEndpoint(s.T(), testcore.RandomizeStr("test-endpoint"), taskQueue)
		var dispatchURL string
		if dispatchOnlyByEndpoint {
			dispatchURL = getDispatchByEndpointURL(env.HttpAPIAddress(), testEndpoint.Id)
		} else {
			dispatchURL = getDispatchByNsAndTqURL(env.HttpAPIAddress(), env.Namespace().String(), taskQueue)
		}

		ctx, cancel := context.WithCancel(testcore.NewContext())
		defer cancel()

		client, err := nexusrpc.NewHTTPClient(nexusrpc.HTTPClientOptions{BaseURL: dispatchURL, Service: "test-service"})
		s.NoError(err)

		var pollerErrCh <-chan error
		if tc.handler != nil {
			// only set on valid request
			pollerErrCh = env.nexusTaskPoller(ctx, s.T(), taskQueue, tc.handler)
		}

		capture := env.GetTestCluster().Host().CaptureMetricsHandler().StartCapture()
		result, err := nexusrpc.StartOperation(ctx, client, op, "input", nexus.StartOperationOptions{
			Header: tc.header,
		})
		snap := capture.Snapshot()
		env.GetTestCluster().Host().CaptureMetricsHandler().StopCapture(capture)

		tc.assertion(s, result, err, snap)
		if pollerErrCh != nil {
			s.NoError(<-pollerErrCh)
		}
	}

	for _, tc := range testCases {
		s.Run(tc.name, func(s *NexusApiTestSuite) {
			s.Run("ByNamespaceAndTaskQueue", func(s *NexusApiTestSuite) { testFn(s, tc, false) })
			s.Run("ByEndpoint", func(s *NexusApiTestSuite) { testFn(s, tc, true) })
		})
	}
}

func (s *NexusApiTestSuite) TestNexusCancelOperation_Outcomes(useTemporalFailures bool) {
	asyncSuccessEndpoint := testcore.RandomizeStr("async-success-endpoint")

	type testcase struct {
		outcome        string
		onlyByEndpoint bool
		endpointName   string
		timeout        time.Duration
		handler        nexusTaskHandler
		assertion      func(*NexusApiTestSuite, error, http.Header)
	}

	testCases := []testcase{
		{
			outcome:        "success",
			onlyByEndpoint: true,
			endpointName:   asyncSuccessEndpoint,
			handler: func(t *testing.T, res *workflowservice.PollNexusTaskQueueResponse) (*nexusTaskResponse, error) {
				require.Equal(t, asyncSuccessEndpoint, res.Request.Endpoint)
				// Choose an arbitrary test case to assert that all of the input is delivered to the
				// poll response.
				op, ok := res.Request.Variant.(*nexuspb.Request_CancelOperation)
				require.True(t, ok)
				require.Equal(t, "test-service", op.CancelOperation.Service)
				require.Equal(t, "operation", op.CancelOperation.Operation)
				require.Equal(t, "token", op.CancelOperation.OperationToken)
				require.Equal(t, "value", res.Request.Header["key"])
				return &nexusTaskResponse{CancelResult: new(struct{})}, nil
			},
			assertion: func(s *NexusApiTestSuite, err error, headers http.Header) {
				s.NoError(err)
			},
		},
		{
			outcome:      "handler_error:INTERNAL",
			endpointName: testcore.RandomizeStr("test-endpoint"),
			handler: func(_ *testing.T, _ *workflowservice.PollNexusTaskQueueResponse) (*nexusTaskResponse, error) {
				return nil, &nexus.HandlerError{
					Type: nexus.HandlerErrorTypeInternal,
					Cause: &nexus.FailureError{
						Failure: nexus.Failure{Message: "deliberate internal failure"},
					},
				}
			},
			assertion: func(s *NexusApiTestSuite, err error, headers http.Header) {
				var handlerErr *nexus.HandlerError
				s.ErrorAs(err, &handlerErr)
				s.Equal(nexus.HandlerErrorTypeInternal, handlerErr.Type)
				s.Equal("worker", headers.Get("Temporal-Nexus-Failure-Source"))
				s.Empty(handlerErr.Message)
				s.Error(handlerErr.Cause)
				s.Equal("deliberate internal failure", handlerErr.Cause.Error())
			},
		},
		{
			outcome:      "handler_timeout",
			endpointName: testcore.RandomizeStr("test-service"),
			timeout:      2 * time.Second,
			handler: func(t *testing.T, res *workflowservice.PollNexusTaskQueueResponse) (*nexusTaskResponse, error) {
				timeoutStr, set := res.Request.Header[nexus.HeaderRequestTimeout]
				require.True(t, set)
				timeout, err := time.ParseDuration(timeoutStr)
				require.NoError(t, err)
				time.Sleep(timeout) //nolint:forbidigo // Allow time.Sleep for timeout tests
				return nil, nil
			},
			assertion: func(s *NexusApiTestSuite, err error, headers http.Header) {
				var handlerErr *nexus.HandlerError
				s.ErrorAs(err, &handlerErr)
				s.Equal(nexus.HandlerErrorTypeUpstreamTimeout, handlerErr.Type)
				s.Equal("upstream timeout", handlerErr.Message)
			},
		},
	}

	testFn := func(s *NexusApiTestSuite, tc testcase, dispatchOnlyByEndpoint bool) {
		env := newNexusTestEnv(s.T(), useTemporalFailures, testcore.WithDedicatedCluster())
		endpoint := env.createNexusEndpoint(s.T(), tc.endpointName, testcore.RandomizeStr("task-queue"))
		var dispatchURL string
		if dispatchOnlyByEndpoint {
			dispatchURL = getDispatchByEndpointURL(env.HttpAPIAddress(), endpoint.Id)
		} else {
			dispatchURL = getDispatchByNsAndTqURL(env.HttpAPIAddress(), env.Namespace().String(), endpoint.Spec.Target.GetWorker().TaskQueue)
		}
		ctx, cancel := context.WithCancel(testcore.NewContext())
		defer cancel()

		httpCaller, headerCapture := newHeaderCaptureCaller()
		client, err := nexusrpc.NewHTTPClient(nexusrpc.HTTPClientOptions{
			BaseURL:    dispatchURL,
			Service:    "test-service",
			HTTPCaller: httpCaller,
		})
		s.NoError(err)
		capture := env.GetTestCluster().Host().CaptureMetricsHandler().StartCapture()
		defer env.GetTestCluster().Host().CaptureMetricsHandler().StopCapture(capture)

		pollerErrCh := env.nexusTaskPoller(ctx, s.T(), endpoint.Spec.Target.GetWorker().TaskQueue, tc.handler)

		handle, err := client.NewOperationHandle("operation", "token")
		s.NoError(err)

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

		tc.assertion(s, err, headerCapture.lastHeaders)
		s.NoError(<-pollerErrCh)

		snap := capture.Snapshot()

		s.Len(snap["nexus_requests"], 1)
		s.Subset(snap["nexus_requests"][0].Tags, map[string]string{"namespace": env.Namespace().String(), "method": "CancelNexusOperation", "outcome": tc.outcome})
		s.Contains(snap["nexus_requests"][0].Tags, "nexus_endpoint")
		s.Equal(int64(1), snap["nexus_requests"][0].Value)
		s.Equal(metrics.MetricUnit(""), snap["nexus_requests"][0].Unit)

		s.Len(snap["nexus_latency"], 1)
		s.Subset(snap["nexus_latency"][0].Tags, map[string]string{"namespace": env.Namespace().String(), "method": "CancelNexusOperation", "outcome": tc.outcome})
		s.Contains(snap["nexus_latency"][0].Tags, "nexus_endpoint")

		// Ensure that CancelOperation request is tracked as part of normal service telemetry metrics
		s.Condition(func() bool {
			for _, m := range snap["service_requests"] {
				if opTag, ok := m.Tags["operation"]; ok && opTag == "CancelNexusOperation" {
					return true
				}
			}
			return false
		})
	}

	for _, tc := range testCases {
		s.Run(tc.outcome, func(s *NexusApiTestSuite) {
			if !tc.onlyByEndpoint {
				s.Run("ByNamespaceAndTaskQueue", func(s *NexusApiTestSuite) { testFn(s, tc, false) })
			}
			s.Run("ByEndpoint", func(s *NexusApiTestSuite) { testFn(s, tc, true) })
		})
	}
}

func (s *NexusApiTestSuite) TestNexusStartOperation_WithNamespaceAndTaskQueue_SupportsVersioning(useTemporalFailures bool) {
	env := newNexusTestEnv(s.T(), useTemporalFailures,
		testcore.WithDedicatedCluster(),
		testcore.WithDynamicConfig(dynamicconfig.FrontendEnableWorkerVersioningRuleAPIs, true),
		// UpdateWorkerBuildIdCompatibility is the v0.1 (Version Set-based) API gated by DataAPIs.
		testcore.WithDynamicConfig(dynamicconfig.FrontendEnableWorkerVersioningDataAPIs, true),
	)

	ctx, cancel := context.WithCancel(testcore.NewContext())
	defer cancel()
	taskQueue := testcore.RandomizeStr("task-queue")
	err := env.SdkClient().UpdateWorkerBuildIdCompatibility(ctx, &sdkclient.UpdateWorkerBuildIdCompatibilityOptions{ //nolint:staticcheck // SA1019 deprecated
		TaskQueue: taskQueue,
		Operation: &sdkclient.BuildIDOpAddNewIDInNewDefaultSet{BuildID: "old-build-id"},
	})
	s.NoError(err)
	err = env.SdkClient().UpdateWorkerBuildIdCompatibility(ctx, &sdkclient.UpdateWorkerBuildIdCompatibilityOptions{ //nolint:staticcheck // SA1019 deprecated
		TaskQueue: taskQueue,
		Operation: &sdkclient.BuildIDOpAddNewIDInNewDefaultSet{BuildID: "new-build-id"},
	})
	s.NoError(err)

	u := getDispatchByNsAndTqURL(env.HttpAPIAddress(), env.Namespace().String(), taskQueue)
	client, err := nexusrpc.NewHTTPClient(nexusrpc.HTTPClientOptions{BaseURL: u, Service: "test-service"})
	s.NoError(err)
	// Versioned poller gets task
	pollerErrCh1 := env.versionedNexusTaskPoller(ctx, s.T(), taskQueue, "new-build-id", nexusEchoHandler)

	result, err := nexusrpc.StartOperation(ctx, client, op, "input", nexus.StartOperationOptions{})
	s.NoError(err)
	s.Equal("input", result.Successful)
	s.NoError(<-pollerErrCh1)

	// Unversioned poller doesn't get a task
	pollerErrCh2 := env.nexusTaskPoller(ctx, s.T(), taskQueue, nexusEchoHandler)
	// Versioned poller gets task with wrong build ID
	pollerErrCh3 := env.versionedNexusTaskPoller(ctx, s.T(), taskQueue, "old-build-id", nexusEchoHandler)

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

// TestNexusClientNameMetricPropagation verifies that when an SDK worker polls for Nexus tasks
// with client-name in gRPC metadata, the matching service emits nexus_task_requests with a
// client_name tag. This proves the header propagates e2e: SDK → frontend → matching.
func (s *NexusApiTestSuite) TestNexusClientNameMetricPropagation(useTemporalFailures bool) {
	env := newNexusTestEnv(s.T(), useTemporalFailures, testcore.WithDedicatedCluster())
	const expectedClientName = "temporal-go"
	taskQueue := testcore.RandomizeStr("tq")
	endpoint := env.createNexusEndpoint(s.T(), testcore.RandomizeStr("endpoint"), taskQueue)

	capture := env.GetTestCluster().Host().CaptureMetricsHandler().StartCapture()
	defer env.GetTestCluster().Host().CaptureMetricsHandler().StopCapture(capture)

	ctx, cancel := context.WithCancel(testcore.NewContext())
	defer cancel()

	// Start a poller that simulates an SDK worker with a specific client-name.
	// We build the outgoing metadata from scratch (instead of using NewContext which
	// sets client-name=temporal-server) so the SDK name is the only value.
	pollerCtx := metadata.NewOutgoingContext(ctx, metadata.Pairs(
		"client-name", expectedClientName,
		"client-version", "1.0.0",
		"supported-server-versions", headers.SupportedServerVersions,
		"supported-features", headers.AllFeatures,
	))
	pollerErrCh := env.nexusTaskPoller(pollerCtx, s.T(), taskQueue, nexusEchoHandler)

	// Trigger a Nexus start operation via HTTP to unblock the poller.
	client, err := nexusrpc.NewHTTPClient(nexusrpc.HTTPClientOptions{
		BaseURL: getDispatchByEndpointURL(env.HttpAPIAddress(), endpoint.Id),
		Service: "test-service",
	})
	s.NoError(err)

	s.Eventually(func() bool {
		_, err = nexusrpc.StartOperation(ctx, client, op, "input", nexus.StartOperationOptions{})
		var handlerErr *nexus.HandlerError
		return err == nil || (!errors.As(err, &handlerErr) || handlerErr.Type != nexus.HandlerErrorTypeNotFound)
	}, 10*time.Second, 500*time.Millisecond)
	s.NoError(err)
	s.NoError(<-pollerErrCh)

	// Verify that the matching service emitted nexus_task_requests with client_name tag.
	snap := capture.Snapshot()
	var found bool
	for _, rec := range snap["nexus_task_requests"] {
		if rec.Tags["client_name"] == expectedClientName {
			found = true
			break
		}
	}
	s.True(found, "expected nexus_task_requests metric with client_name=%s, got entries: %v",
		expectedClientName, snap["nexus_task_requests"])
}

func nexusEchoHandler(_ *testing.T, res *workflowservice.PollNexusTaskQueueResponse) (*nexusTaskResponse, error) {
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
