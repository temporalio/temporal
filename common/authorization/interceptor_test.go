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

package authorization

import (
	"context"
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/api/workflowservicemock/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
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

		controller          *gomock.Controller
		mockFrontendHandler *workflowservicemock.MockWorkflowServiceServer
		mockAuthorizer      *MockAuthorizer
		mockMetricsHandler  *metrics.MockHandler
		interceptor         *Interceptor
		handler             grpc.UnaryHandler
		mockClaimMapper     *MockClaimMapper
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

	s.mockFrontendHandler = workflowservicemock.NewMockWorkflowServiceServer(s.controller)
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
