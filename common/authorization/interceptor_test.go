package authorization

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

const (
	testNamespace string = "test-namespace"
)

var (
	ctx                           = context.Background()
	describeNamespaceRequest      = &workflowservice.DescribeNamespaceRequest{Namespace: testNamespace}
	describeNamespaceTarget       = &CallTarget{Namespace: testNamespace, Request: describeNamespaceRequest, APIName: "/temporal.api.workflowservice.v1.WorkflowService/DescribeNamespace"}
	describeNamespaceInfo         = &grpc.UnaryServerInfo{FullMethod: "/temporal.api.workflowservice.v1.WorkflowService/DescribeNamespace"}
	startWorkflowExecutionRequest = &workflowservice.StartWorkflowExecutionRequest{Namespace: testNamespace}
	startWorkflowExecutionTarget  = &CallTarget{Namespace: testNamespace, Request: startWorkflowExecutionRequest, APIName: "/temporal.api.workflowservice.v1.WorkflowService/StartWorkflowExecution"}
	startWorkflowExecutionInfo    = &grpc.UnaryServerInfo{FullMethod: "/temporal.api.workflowservice.v1.WorkflowService/StartWorkflowExecution"}
)

type (
	authorizerInterceptorSuite struct {
		suite.Suite
		*require.Assertions

		controller         *gomock.Controller
		mockAuthorizer     *MockAuthorizer
		mockMetricsHandler *metrics.MockHandler
		interceptor        *Interceptor
		handler            grpc.UnaryHandler
		mockClaimMapper    *MockClaimMapper
	}

	mockNamespaceChecker namespace.Name
)

func TestAuthorizerInterceptorSuite(t *testing.T) {
	s := new(authorizerInterceptorSuite)
	suite.Run(t, s)
}

func (s *authorizerInterceptorSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.controller = gomock.NewController(s.T())

	s.mockAuthorizer = NewMockAuthorizer(s.controller)
	s.mockMetricsHandler = metrics.NewMockHandler(s.controller)
	s.mockMetricsHandler.EXPECT().WithTags(
		metrics.OperationTag(metrics.AuthorizationScope),
		metrics.NamespaceTag(testNamespace),
	).Return(s.mockMetricsHandler).AnyTimes()
	s.mockMetricsHandler.EXPECT().Timer(metrics.ServiceAuthorizationLatency.Name()).Return(metrics.NoopTimerMetricFunc).AnyTimes()

	s.mockClaimMapper = NewMockClaimMapper(s.controller)
	s.interceptor = NewInterceptor(
		s.mockClaimMapper,
		s.mockAuthorizer,
		s.mockMetricsHandler,
		log.NewNoopLogger(),
		mockNamespaceChecker(testNamespace),
		nil,
		"",
		"",
		dynamicconfig.GetBoolPropertyFn(false),
	)
	s.handler = func(ctx context.Context, req interface{}) (interface{}, error) { return true, nil }
}

func (s *authorizerInterceptorSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *authorizerInterceptorSuite) TestIsAuthorized() {
	s.mockAuthorizer.EXPECT().Authorize(ctx, nil, describeNamespaceTarget).
		Return(Result{Decision: DecisionAllow}, nil)

	res, err := s.interceptor.Intercept(ctx, describeNamespaceRequest, describeNamespaceInfo, s.handler)
	s.True(res.(bool))
	s.NoError(err)
}

func (s *authorizerInterceptorSuite) TestIsAuthorizedWithNamespace() {
	s.mockAuthorizer.EXPECT().Authorize(ctx, nil, startWorkflowExecutionTarget).
		Return(Result{Decision: DecisionAllow}, nil)

	res, err := s.interceptor.Intercept(ctx, startWorkflowExecutionRequest, startWorkflowExecutionInfo, s.handler)
	s.True(res.(bool))
	s.NoError(err)
}

func (s *authorizerInterceptorSuite) TestIsUnauthorized() {
	s.mockAuthorizer.EXPECT().Authorize(ctx, nil, describeNamespaceTarget).
		Return(Result{Decision: DecisionDeny}, nil)
	s.mockMetricsHandler.EXPECT().Counter(metrics.ServiceErrUnauthorizedCounter.Name()).Return(metrics.NoopCounterMetricFunc)

	res, err := s.interceptor.Intercept(ctx, describeNamespaceRequest, describeNamespaceInfo, s.handler)
	s.Nil(res)
	s.Error(err)
}

func (s *authorizerInterceptorSuite) TestIsUnknown() {
	request := &workflowservice.DescribeNamespaceRequest{Namespace: "unknown-namespace"}
	target := &CallTarget{Namespace: "unknown-namespace", Request: request, APIName: "/temporal.api.workflowservice.v1.WorkflowService/DescribeNamespace"}
	s.mockAuthorizer.EXPECT().Authorize(ctx, nil, target).Return(Result{Decision: DecisionDeny}, nil)
	handler := metrics.NewMockHandler(s.controller)
	s.mockMetricsHandler.EXPECT().WithTags(
		metrics.OperationTag(metrics.AuthorizationScope),
		metrics.NamespaceUnknownTag(), // note: should use unknown tag since unknown-namespace is not registered
	).Return(handler).AnyTimes()
	handler.EXPECT().Counter(metrics.ServiceErrUnauthorizedCounter.Name()).Return(metrics.NoopCounterMetricFunc)
	handler.EXPECT().Timer(metrics.ServiceAuthorizationLatency.Name()).Return(metrics.NoopTimerMetricFunc)

	res, err := s.interceptor.Intercept(ctx, request, describeNamespaceInfo, s.handler)
	s.Nil(res)
	s.Error(err)
}

func (s *authorizerInterceptorSuite) TestAuthorizationFailed() {
	s.mockAuthorizer.EXPECT().Authorize(ctx, nil, describeNamespaceTarget).
		Return(Result{Decision: DecisionDeny}, errUnauthorized)
	s.mockMetricsHandler.EXPECT().Counter(metrics.ServiceErrAuthorizeFailedCounter.Name()).Return(metrics.NoopCounterMetricFunc)

	res, err := s.interceptor.Intercept(ctx, describeNamespaceRequest, describeNamespaceInfo, s.handler)
	s.Nil(res)
	s.Error(err)
}

func (s *authorizerInterceptorSuite) TestAuthorizationFailedExposed() {
	interceptor := NewInterceptor(
		s.mockClaimMapper,
		s.mockAuthorizer,
		s.mockMetricsHandler,
		log.NewNoopLogger(),
		mockNamespaceChecker(testNamespace),
		nil,
		"",
		"",
		dynamicconfig.GetBoolPropertyFn(true),
	)

	authErr := serviceerror.NewInternal("intentional test failure")
	s.mockAuthorizer.EXPECT().Authorize(ctx, nil, describeNamespaceTarget).
		Return(Result{Decision: DecisionDeny}, authErr)
	s.mockMetricsHandler.EXPECT().Counter(metrics.ServiceErrAuthorizeFailedCounter.Name()).Return(metrics.NoopCounterMetricFunc)

	res, err := interceptor.Intercept(ctx, describeNamespaceRequest, describeNamespaceInfo, s.handler)
	s.Nil(res)
	s.ErrorIs(err, authErr)
}

func (s *authorizerInterceptorSuite) TestNoopClaimMapperWithoutTLS() {
	admin := &Claims{System: RoleAdmin}
	s.mockAuthorizer.EXPECT().Authorize(gomock.Any(), admin, describeNamespaceTarget).
		DoAndReturn(func(ctx context.Context, caller *Claims, target *CallTarget) (Result, error) {
			// check that claims are present in ctx also
			s.Equal(admin, ctx.Value(MappedClaims))

			return Result{Decision: DecisionAllow}, nil
		})

	interceptor := NewInterceptor(
		NewNoopClaimMapper(),
		s.mockAuthorizer,
		s.mockMetricsHandler,
		log.NewNoopLogger(),
		mockNamespaceChecker(testNamespace),
		nil,
		"",
		"",
		dynamicconfig.GetBoolPropertyFn(false),
	)
	_, err := interceptor.Intercept(ctx, describeNamespaceRequest, describeNamespaceInfo, s.handler)
	s.NoError(err)
}

func (s *authorizerInterceptorSuite) TestAlternateHeaders() {
	interceptor := NewInterceptor(
		s.mockClaimMapper,
		NewNoopAuthorizer(),
		s.mockMetricsHandler,
		log.NewNoopLogger(),
		mockNamespaceChecker(testNamespace),
		nil,
		"custom-header",
		"custom-extra-header",
		dynamicconfig.GetBoolPropertyFn(false),
	)

	cases := []struct {
		md       metadata.MD
		authInfo *AuthInfo
	}{
		{
			metadata.Pairs(
				"custom-header", "the-token",
				"custom-extra-header", "more stuff",
			),
			&AuthInfo{
				AuthToken: "the-token",
				ExtraData: "more stuff",
			},
		},
		{
			metadata.Pairs(
				"custom-header", "the-token",
				"authorization-extras", "this gets ignored",
			),
			&AuthInfo{
				AuthToken: "the-token",
			},
		},
		{
			metadata.Pairs(
				"custom-extra-header", "missing main header, this gets ignored",
			),
			nil,
		},
	}
	for _, testCase := range cases {
		if testCase.authInfo != nil {
			s.mockClaimMapper.EXPECT().GetClaims(testCase.authInfo).Return(&Claims{System: RoleAdmin}, nil)
		}
		inCtx := metadata.NewIncomingContext(ctx, testCase.md)
		_, err := interceptor.Intercept(inCtx, describeNamespaceRequest, describeNamespaceInfo, s.handler)
		s.NoError(err)
	}
}

func (n mockNamespaceChecker) Exists(name namespace.Name) error {
	if name == namespace.Name(n) {
		return nil
	}
	return errors.New("doesn't exist")
}
