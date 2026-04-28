package tests

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nexus-rpc/sdk-go/nexus"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/common/authorization"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/nexus/nexusrpc"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/service/frontend/configs"
	"go.temporal.io/server/tests/testcore"
)

type NexusAPIValidationTestSuite struct {
	parallelsuite.Suite[*NexusAPIValidationTestSuite]
}

func TestNexusAPIValidationTestSuite(t *testing.T) {
	parallelsuite.Run(t, &NexusAPIValidationTestSuite{})
}

func (s *NexusAPIValidationTestSuite) TestNexusStartOperation_WithNamespaceAndTaskQueue_NamespaceNotFound() {
	env := newNexusTestEnv(s.T(), false)
	// Also use this test to verify that namespaces are unescaped in the path.
	taskQueue := testcore.RandomizeStr("task-queue")
	namespace := "namespace not/found"
	u := getDispatchByNsAndTqURL(env.HttpAPIAddress(), namespace, taskQueue)
	client, err := nexusrpc.NewHTTPClient(nexusrpc.HTTPClientOptions{BaseURL: u, Service: "test-service"})
	s.NoError(err)
	ctx := env.Context()
	capture := env.StartNamespaceMetricCaptureFor(namespace)
	_, err = nexusrpc.StartOperation(ctx, client, op, "input", nexus.StartOperationOptions{})
	var handlerError *nexus.HandlerError
	s.ErrorAs(err, &handlerError)
	s.Equal(nexus.HandlerErrorTypeNotFound, handlerError.Type)
	s.Equal(fmt.Sprintf("namespace not found: %q", namespace), handlerError.Message)

	requests := capture.Metric("nexus_requests")
	s.Len(requests, 1)
	s.Equal(map[string]string{"namespace": namespace, "method": "StartNexusOperation", "outcome": "namespace_not_found", "nexus_endpoint": "_unknown_"}, requests[0].Tags)
	s.Equal(int64(1), requests[0].Value)
}

func (s *NexusAPIValidationTestSuite) TestNexusStartOperation_WithNamespaceAndTaskQueue_NamespaceTooLong() {
	env := newNexusTestEnv(s.T(), false, testcore.WithDedicatedCluster())
	taskQueue := testcore.RandomizeStr("task-queue")

	var namespace string
	for range 500 {
		namespace += "namespace-is-a-very-long-string"
	}

	u := getDispatchByNsAndTqURL(env.HttpAPIAddress(), namespace, taskQueue)
	client, err := nexusrpc.NewHTTPClient(nexusrpc.HTTPClientOptions{BaseURL: u, Service: "test-service"})
	s.NoError(err)
	ctx := env.Context()
	capture := env.StartGlobalMetricCapture()
	_, err = nexusrpc.StartOperation(ctx, client, op, "input", nexus.StartOperationOptions{})
	var handlerErr *nexus.HandlerError
	s.ErrorAs(err, &handlerErr)
	s.Equal(nexus.HandlerErrorTypeBadRequest, handlerErr.Type)
	// I wish we'd never put periods in error messages :(
	s.Equal("Namespace length exceeds limit.", handlerErr.Message)

	s.Len(capture.Metric("nexus_request_preprocess_errors"), 1)
}

func (s *NexusAPIValidationTestSuite) TestNexusStartOperation_Forbidden() {
	type testcase struct {
		name                   string
		onAuthorize            func(endpointName string) func(context.Context, *authorization.Claims, *authorization.CallTarget) (authorization.Result, error)
		checkFailure           func(s *NexusAPIValidationTestSuite, handlerErr *nexus.HandlerError)
		exposeAuthorizerErrors bool
		expectedOutcomeMetric  string
	}
	testCases := []testcase{
		{
			name: "deny with reason",
			onAuthorize: func(endpointName string) func(context.Context, *authorization.Claims, *authorization.CallTarget) (authorization.Result, error) {
				return func(ctx context.Context, c *authorization.Claims, ct *authorization.CallTarget) (authorization.Result, error) {
					if ct.APIName == configs.DispatchNexusTaskByNamespaceAndTaskQueueAPIName {
						return authorization.Result{Decision: authorization.DecisionDeny, Reason: "unauthorized in test"}, nil
					}
					if ct.APIName == configs.DispatchNexusTaskByEndpointAPIName {
						if ct.NexusEndpointName != endpointName {
							panic("expected nexus endpoint name")
						}
						return authorization.Result{Decision: authorization.DecisionDeny, Reason: "unauthorized in test"}, nil
					}
					return authorization.Result{Decision: authorization.DecisionAllow}, nil
				}
			},
			checkFailure: func(s *NexusAPIValidationTestSuite, handlerErr *nexus.HandlerError) {
				s.Equal(nexus.HandlerErrorTypeUnauthorized, handlerErr.Type)
				s.Equal("permission denied: unauthorized in test", handlerErr.Message)
			},
			expectedOutcomeMetric:  "unauthorized",
			exposeAuthorizerErrors: false,
		},
		{
			name: "deny without reason",
			onAuthorize: func(endpointName string) func(context.Context, *authorization.Claims, *authorization.CallTarget) (authorization.Result, error) {
				return func(ctx context.Context, c *authorization.Claims, ct *authorization.CallTarget) (authorization.Result, error) {
					if ct.APIName == configs.DispatchNexusTaskByNamespaceAndTaskQueueAPIName {
						return authorization.Result{Decision: authorization.DecisionDeny}, nil
					}
					if ct.APIName == configs.DispatchNexusTaskByEndpointAPIName {
						if ct.NexusEndpointName != endpointName {
							panic("expected nexus endpoint name")
						}
						return authorization.Result{Decision: authorization.DecisionDeny}, nil
					}
					return authorization.Result{Decision: authorization.DecisionAllow}, nil
				}
			},
			checkFailure: func(s *NexusAPIValidationTestSuite, handlerErr *nexus.HandlerError) {
				s.Equal(nexus.HandlerErrorTypeUnauthorized, handlerErr.Type)
				s.Equal("permission denied", handlerErr.Message)
			},
			expectedOutcomeMetric:  "unauthorized",
			exposeAuthorizerErrors: false,
		},
		{
			name: "deny with generic error",
			onAuthorize: func(endpointName string) func(context.Context, *authorization.Claims, *authorization.CallTarget) (authorization.Result, error) {
				return func(ctx context.Context, c *authorization.Claims, ct *authorization.CallTarget) (authorization.Result, error) {
					if ct.APIName == configs.DispatchNexusTaskByNamespaceAndTaskQueueAPIName {
						return authorization.Result{}, errors.New("some generic error")
					}
					if ct.APIName == configs.DispatchNexusTaskByEndpointAPIName {
						if ct.NexusEndpointName != endpointName {
							panic("expected nexus endpoint name")
						}
						return authorization.Result{}, errors.New("some generic error")
					}
					return authorization.Result{Decision: authorization.DecisionAllow}, nil
				}
			},
			checkFailure: func(s *NexusAPIValidationTestSuite, handlerErr *nexus.HandlerError) {
				s.Equal(nexus.HandlerErrorTypeUnauthorized, handlerErr.Type)
				s.Equal("permission denied", handlerErr.Message)
			},
			expectedOutcomeMetric:  "unauthorized",
			exposeAuthorizerErrors: false,
		},
		{
			name: "deny with exposed error",
			onAuthorize: func(endpointName string) func(context.Context, *authorization.Claims, *authorization.CallTarget) (authorization.Result, error) {
				return func(ctx context.Context, c *authorization.Claims, ct *authorization.CallTarget) (authorization.Result, error) {
					if ct.APIName == configs.DispatchNexusTaskByNamespaceAndTaskQueueAPIName {
						return authorization.Result{}, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeUnavailable, "exposed error")
					}
					if ct.APIName == configs.DispatchNexusTaskByEndpointAPIName {
						if ct.NexusEndpointName != endpointName {
							panic("expected nexus endpoint name")
						}
						return authorization.Result{}, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeUnavailable, "exposed error")
					}
					return authorization.Result{Decision: authorization.DecisionAllow}, nil
				}
			},
			checkFailure: func(s *NexusAPIValidationTestSuite, handlerErr *nexus.HandlerError) {
				s.Equal(nexus.HandlerErrorTypeUnavailable, handlerErr.Type)
				s.Equal("exposed error", handlerErr.Message)
			},
			expectedOutcomeMetric:  "internal_auth_error",
			exposeAuthorizerErrors: true,
		},
	}

	testFn := func(s *NexusAPIValidationTestSuite, tc testcase, dispatchOnlyByEndpoint bool) {
		env := newNexusTestEnv(s.T(), false, testcore.WithDedicatedCluster())
		taskQueue := testcore.RandomizeStr("task-queue")
		ctx := env.Context()
		testEndpoint := env.createNexusEndpoint(ctx, s.T(), testcore.RandomizeStr("test-endpoint"), taskQueue)

		env.GetTestCluster().Host().SetOnAuthorize(tc.onAuthorize(testEndpoint.Spec.Name))
		s.T().Cleanup(func() { env.GetTestCluster().Host().SetOnAuthorize(nil) })

		env.OverrideDynamicConfig(dynamicconfig.ExposeAuthorizerErrors, tc.exposeAuthorizerErrors)

		var dispatchURL string
		if dispatchOnlyByEndpoint {
			dispatchURL = getDispatchByEndpointURL(env.HttpAPIAddress(), testEndpoint.Id)
		} else {
			dispatchURL = getDispatchByNsAndTqURL(env.HttpAPIAddress(), env.Namespace().String(), taskQueue)
		}

		client, err := nexusrpc.NewHTTPClient(nexusrpc.HTTPClientOptions{BaseURL: dispatchURL, Service: "test-service"})
		s.NoError(err)

		capture := env.StartNamespaceMetricCapture()

		// Wait until the endpoint is loaded into the registry.
		s.Eventually(func() bool {
			_, err = nexusrpc.StartOperation(ctx, client, op, "input", nexus.StartOperationOptions{})
			var handlerErr *nexus.HandlerError
			return err == nil || (!errors.As(err, &handlerErr) || handlerErr.Type != nexus.HandlerErrorTypeNotFound)
		}, 10*time.Second, 1*time.Second)

		var handlerErr *nexus.HandlerError
		s.ErrorAs(err, &handlerErr)
		tc.checkFailure(s, handlerErr)

		requests := capture.Metric("nexus_requests")
		s.Len(requests, 1)
		s.Subset(requests[0].Tags, map[string]string{"namespace": env.Namespace().String(), "method": "StartNexusOperation", "outcome": tc.expectedOutcomeMetric})
		s.Equal(int64(1), requests[0].Value)
	}

	for _, tc := range testCases {
		s.Run(tc.name, func(s *NexusAPIValidationTestSuite) {
			s.Run("ByNamespaceAndTaskQueue", func(s *NexusAPIValidationTestSuite) { testFn(s, tc, false) })
			s.Run("ByEndpoint", func(s *NexusAPIValidationTestSuite) { testFn(s, tc, true) })
		})
	}
}

func (s *NexusAPIValidationTestSuite) TestNexusStartOperation_PayloadSizeLimit() {
	// Use -10 to avoid hitting MaxNexusAPIRequestBodyBytes. Actual payload will still exceed limit because of
	// additional Content headers. See common/rpc/grpc.go:66
	input := strings.Repeat("a", (2*1024*1024)-10)

	testFn := func(s *NexusAPIValidationTestSuite, dispatchOnlyByEndpoint bool) {
		env := newNexusTestEnv(s.T(), false)
		taskQueue := testcore.RandomizeStr("task-queue")
		ctx, cancel := context.WithCancel(env.Context())
		defer cancel()
		testEndpoint := env.createNexusEndpoint(ctx, s.T(), testcore.RandomizeStr("test-endpoint"), taskQueue)

		var dispatchURL string
		if dispatchOnlyByEndpoint {
			dispatchURL = getDispatchByEndpointURL(env.HttpAPIAddress(), testEndpoint.Id)
		} else {
			dispatchURL = getDispatchByNsAndTqURL(env.HttpAPIAddress(), env.Namespace().String(), taskQueue)
		}

		client, err := nexusrpc.NewHTTPClient(nexusrpc.HTTPClientOptions{BaseURL: dispatchURL, Service: "test-service"})
		s.NoError(err)
		var result *nexusrpc.ClientStartOperationResponse[string]

		// Wait until the endpoint is loaded into the registry.
		s.Eventually(func() bool {
			result, err = nexusrpc.StartOperation(ctx, client, op, input, nexus.StartOperationOptions{
				CallbackURL: "http://localhost/callback",
				RequestID:   "request-id",
			})
			var handlerErr *nexus.HandlerError
			return err == nil || (!errors.As(err, &handlerErr) || handlerErr.Type != nexus.HandlerErrorTypeNotFound)
		}, 10*time.Second, 500*time.Millisecond)

		s.Nil(result)
		var handlerErr *nexus.HandlerError
		s.ErrorAs(err, &handlerErr)
		s.Equal(nexus.HandlerErrorTypeBadRequest, handlerErr.Type)
		s.Equal("input exceeds size limit", handlerErr.Message)
	}

	s.Run("ByNamespaceAndTaskQueue", func(s *NexusAPIValidationTestSuite) { testFn(s, false) })
	s.Run("ByEndpoint", func(s *NexusAPIValidationTestSuite) { testFn(s, true) })
}

func (s *NexusAPIValidationTestSuite) TestNexus_RespondNexusTaskMethods_VerifiesTaskTokenMatchesRequestNamespace() {
	env := newNexusTestEnv(s.T(), false)
	ctx := env.Context()

	tt := tokenspb.NexusTask{
		NamespaceId: env.NamespaceID().String(),
		TaskQueue:   "test",
		TaskId:      uuid.NewString(),
	}
	ttBytes, err := tt.Marshal()
	s.NoError(err)

	_, err = env.FrontendClient().RespondNexusTaskCompleted(ctx, &workflowservice.RespondNexusTaskCompletedRequest{
		Namespace: env.ExternalNamespace().String(),
		Identity:  uuid.NewString(),
		TaskToken: ttBytes,
		Response:  &nexuspb.Response{},
	})
	s.ErrorContains(err, "Operation requested with a token from a different namespace.")

	_, err = env.FrontendClient().RespondNexusTaskFailed(ctx, &workflowservice.RespondNexusTaskFailedRequest{
		Namespace: env.ExternalNamespace().String(),
		Identity:  uuid.NewString(),
		TaskToken: ttBytes,
		Error:     &nexuspb.HandlerError{},
	})
	s.ErrorContains(err, "Operation requested with a token from a different namespace.")
}

func (s *NexusAPIValidationTestSuite) TestNexus_RespondNexusTaskCompleted_ValidateOperationTokenLength() {
	env := newNexusTestEnv(s.T(), false)
	ctx := env.Context()

	tt := tokenspb.NexusTask{
		NamespaceId: env.NamespaceID().String(),
		TaskQueue:   "test",
		TaskId:      uuid.NewString(),
	}
	ttBytes, err := tt.Marshal()
	s.NoError(err)

	_, err = env.FrontendClient().RespondNexusTaskCompleted(ctx, &workflowservice.RespondNexusTaskCompletedRequest{
		Namespace: env.Namespace().String(),
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

func (s *NexusAPIValidationTestSuite) TestNexus_RespondNexusTaskMethods_ValidateFailureDetailsJSON() {
	env := newNexusTestEnv(s.T(), false)
	ctx := env.Context()

	tt := tokenspb.NexusTask{
		NamespaceId: env.NamespaceID().String(),
		TaskQueue:   "test",
		TaskId:      uuid.NewString(),
	}
	ttBytes, err := tt.Marshal()
	s.NoError(err)

	_, err = env.FrontendClient().RespondNexusTaskCompleted(ctx, &workflowservice.RespondNexusTaskCompletedRequest{
		Namespace: env.Namespace().String(),
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

	_, err = env.FrontendClient().RespondNexusTaskFailed(ctx, &workflowservice.RespondNexusTaskFailedRequest{
		Namespace: env.Namespace().String(),
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

func (s *NexusAPIValidationTestSuite) TestNexusStartOperation_ByEndpoint_EndpointNotFound() {
	env := newNexusTestEnv(s.T(), false, testcore.WithDedicatedCluster())
	u := getDispatchByEndpointURL(env.HttpAPIAddress(), uuid.NewString())
	client, err := nexusrpc.NewHTTPClient(nexusrpc.HTTPClientOptions{BaseURL: u, Service: "test-service"})
	s.NoError(err)
	ctx := env.Context()
	capture := env.StartGlobalMetricCapture()
	_, err = nexusrpc.StartOperation(ctx, client, op, "input", nexus.StartOperationOptions{})
	var handlerErr *nexus.HandlerError
	s.ErrorAs(err, &handlerErr)
	s.Equal(nexus.HandlerErrorTypeNotFound, handlerErr.Type)
	s.Equal("nexus endpoint not found", handlerErr.Message)
	s.Len(capture.Metric("nexus_request_preprocess_errors"), 1)
}
