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
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/common/authorization"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/nexus/nexusrpc"
	"go.temporal.io/server/service/frontend/configs"
	"go.temporal.io/server/tests/testcore"
)

type NexusAPIValidationTestSuite struct {
	NexusTestBaseSuite
}

func TestNexusAPIValidationTestSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(NexusAPIValidationTestSuite))
}

func (s *NexusAPIValidationTestSuite) TestNexusStartOperation_WithNamespaceAndTaskQueue_NamespaceNotFound() {
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
	s.Equal(fmt.Sprintf("namespace not found: %q", namespace), handlerError.Message)

	snap := capture.Snapshot()

	s.Len(snap["nexus_requests"], 1)
	s.Equal(map[string]string{"namespace": namespace, "method": "StartNexusOperation", "outcome": "namespace_not_found", "nexus_endpoint": "_unknown_"}, snap["nexus_requests"][0].Tags)
	s.Equal(int64(1), snap["nexus_requests"][0].Value)
}

func (s *NexusAPIValidationTestSuite) TestNexusStartOperation_WithNamespaceAndTaskQueue_NamespaceTooLong() {
	taskQueue := testcore.RandomizeStr("task-queue")

	var namespace string
	for range 500 {
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
	s.Equal("Namespace length exceeds limit.", handlerErr.Message)

	snap := capture.Snapshot()

	s.Len(snap["nexus_request_preprocess_errors"], 1)
}

func (s *NexusAPIValidationTestSuite) TestNexusStartOperation_Forbidden() {
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
				require.Equal(t, "permission denied: unauthorized in test", handlerErr.Message)
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
				require.Equal(t, "permission denied", handlerErr.Message)
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
				require.Equal(t, "permission denied", handlerErr.Message)
			},
			expectedOutcomeMetric:  "unauthorized",
			exposeAuthorizerErrors: false,
		},
		{
			name: "deny with exposed error",
			onAuthorize: func(ctx context.Context, c *authorization.Claims, ct *authorization.CallTarget) (authorization.Result, error) {
				if ct.APIName == configs.DispatchNexusTaskByNamespaceAndTaskQueueAPIName {
					return authorization.Result{}, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeUnavailable, "exposed error")
				}
				if ct.APIName == configs.DispatchNexusTaskByEndpointAPIName {
					if ct.NexusEndpointName != testEndpoint.Spec.Name {
						panic("expected nexus endpoint name")
					}
					return authorization.Result{}, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeUnavailable, "exposed error")
				}
				return authorization.Result{Decision: authorization.DecisionAllow}, nil
			},
			checkFailure: func(t *testing.T, handlerErr *nexus.HandlerError) {
				require.Equal(t, nexus.HandlerErrorTypeUnavailable, handlerErr.Type)
				require.Equal(t, "exposed error", handlerErr.Message)
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
			return err == nil || (!errors.As(err, &handlerErr) || handlerErr.Type != nexus.HandlerErrorTypeNotFound)
		}, 10*time.Second, 1*time.Second)

		var handlerErr *nexus.HandlerError
		require.ErrorAs(t, err, &handlerErr)
		tc.checkFailure(t, handlerErr)

		snap := capture.Snapshot()

		require.Len(t, snap["nexus_requests"], 1)
		require.Subset(t, snap["nexus_requests"][0].Tags, map[string]string{"namespace": s.Namespace().String(), "method": "StartNexusOperation", "outcome": tc.expectedOutcomeMetric})
		require.Equal(t, int64(1), snap["nexus_requests"][0].Value)
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			s.GetTestCluster().Host().SetOnAuthorize(tc.onAuthorize)
			defer s.GetTestCluster().Host().SetOnAuthorize(nil)

			s.Run("ByNamespaceAndTaskQueue", func() {
				testFn(s.T(), tc, getDispatchByNsAndTqURL(s.HttpAPIAddress(), s.Namespace().String(), taskQueue))
			})
			s.Run("ByEndpoint", func() {
				testFn(s.T(), tc, getDispatchByEndpointURL(s.HttpAPIAddress(), testEndpoint.Id))
			})
		})
	}
}

func (s *NexusAPIValidationTestSuite) TestNexusStartOperation_PayloadSizeLimit() {
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
			return err == nil || (!errors.As(err, &handlerErr) || handlerErr.Type != nexus.HandlerErrorTypeNotFound)
		}, 10*time.Second, 500*time.Millisecond)

		require.Nil(t, result)
		var handlerErr *nexus.HandlerError
		require.ErrorAs(t, err, &handlerErr)
		require.Equal(t, nexus.HandlerErrorTypeBadRequest, handlerErr.Type)
		require.Equal(t, "input exceeds size limit", handlerErr.Message)
	}

	s.Run("ByNamespaceAndTaskQueue", func() {
		testFn(s.T(), getDispatchByNsAndTqURL(s.HttpAPIAddress(), s.Namespace().String(), taskQueue))
	})
	s.Run("ByEndpoint", func() {
		testFn(s.T(), getDispatchByEndpointURL(s.HttpAPIAddress(), testEndpoint.Id))
	})
}

func (s *NexusAPIValidationTestSuite) TestNexus_RespondNexusTaskMethods_VerifiesTaskTokenMatchesRequestNamespace() {
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

func (s *NexusAPIValidationTestSuite) TestNexus_RespondNexusTaskCompleted_ValidateOperationTokenLength() {
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

func (s *NexusAPIValidationTestSuite) TestNexus_RespondNexusTaskMethods_ValidateFailureDetailsJSON() {
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

func (s *NexusAPIValidationTestSuite) TestNexusStartOperation_ByEndpoint_EndpointNotFound() {
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
	s.Equal("nexus endpoint not found", handlerErr.Message)
	snap := capture.Snapshot()
	s.Len(snap["nexus_request_preprocess_errors"], 1)
}
