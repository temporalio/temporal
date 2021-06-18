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
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/api/workflowservicemock/v1"
	"google.golang.org/grpc"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
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
		mockMetricsClient   *metrics.MockClient
		mockMetricsScope    *metrics.MockScope
		mockStopwatch       *metrics.MockStopwatch
		interceptor         grpc.UnaryServerInterceptor
		handler             grpc.UnaryHandler
		mockClaimMapper     *MockClaimMapper
	}
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
	s.mockMetricsScope = metrics.NewMockScope(s.controller)
	s.mockMetricsClient = metrics.NewMockClient(s.controller)
	s.mockStopwatch = metrics.NewMockStopwatch(s.controller)
	s.mockStopwatch.EXPECT().Stop().AnyTimes()
	s.mockMetricsClient.EXPECT().Scope(metrics.AuthorizationScope).Return(s.mockMetricsScope)
	s.mockMetricsScope.EXPECT().Tagged(metrics.NamespaceTag(testNamespace)).Return(s.mockMetricsScope)
	s.mockMetricsScope.EXPECT().StartTimer(metrics.ServiceAuthorizationLatency).Return(s.mockStopwatch)
	s.mockClaimMapper = NewMockClaimMapper(s.controller)
	s.interceptor = NewAuthorizationInterceptor(
		s.mockClaimMapper,
		s.mockAuthorizer,
		s.mockMetricsClient,
		log.NewNoopLogger(),
		nil)
	s.handler = func(ctx context.Context, req interface{}) (interface{}, error) { return true, nil }
}

func (s *authorizerInterceptorSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *authorizerInterceptorSuite) TestIsAuthorized() {
	s.mockAuthorizer.EXPECT().Authorize(ctx, nil, describeNamespaceTarget).
		Return(Result{Decision: DecisionAllow}, nil)

	res, err := s.interceptor(ctx, describeNamespaceRequest, describeNamespaceInfo, s.handler)
	s.True(res.(bool))
	s.NoError(err)
}

func (s *authorizerInterceptorSuite) TestIsAuthorizedWithNamespace() {
	s.mockAuthorizer.EXPECT().Authorize(ctx, nil, startWorkflowExecutionTarget).
		Return(Result{Decision: DecisionAllow}, nil)

	res, err := s.interceptor(ctx, startWorkflowExecutionRequest, startWorkflowExecutionInfo, s.handler)
	s.True(res.(bool))
	s.NoError(err)
}

func (s *authorizerInterceptorSuite) TestIsUnauthorized() {
	s.mockAuthorizer.EXPECT().Authorize(ctx, nil, describeNamespaceTarget).
		Return(Result{Decision: DecisionDeny}, nil)
	s.mockMetricsScope.EXPECT().IncCounter(metrics.ServiceErrUnauthorizedCounter)

	res, err := s.interceptor(ctx, describeNamespaceRequest, describeNamespaceInfo, s.handler)
	s.Nil(res)
	s.Error(err)
}

func (s *authorizerInterceptorSuite) TestAuthorizationFailed() {
	s.mockAuthorizer.EXPECT().Authorize(ctx, nil, describeNamespaceTarget).
		Return(Result{Decision: DecisionDeny}, errUnauthorized)
	s.mockMetricsScope.EXPECT().IncCounter(metrics.ServiceErrAuthorizeFailedCounter)

	res, err := s.interceptor(ctx, describeNamespaceRequest, describeNamespaceInfo, s.handler)
	s.Nil(res)
	s.Error(err)
}
