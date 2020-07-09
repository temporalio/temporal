// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2019 Uber Technologies, Inc.
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

package frontend

import (
	"context"
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/api/workflowservicemock/v1"

	"go.temporal.io/server/common/authorization"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/mocks"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/service/dynamicconfig"
)

type (
	accessControlledHandlerSuite struct {
		suite.Suite
		*require.Assertions

		controller          *gomock.Controller
		mockFrontendHandler *workflowservicemock.MockWorkflowServiceServer
		mockAuthorizer      *authorization.MockAuthorizer
		mockMetricsScope    *mocks.Scope

		handler *AccessControlledWorkflowHandler
	}
)

func TestAccessControlledHandlerSuite(t *testing.T) {
	s := new(accessControlledHandlerSuite)
	suite.Run(t, s)
}

func (s *accessControlledHandlerSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.controller = gomock.NewController(s.T())

	mockResource := resource.NewTest(s.controller, metrics.Frontend)
	config := NewConfig(dynamicconfig.NewCollection(dynamicconfig.NewNopClient(), mockResource.GetLogger()), 0, false)

	frontendHandlerGRPC := NewWorkflowHandler(mockResource, config, nil)
	s.mockFrontendHandler = workflowservicemock.NewMockWorkflowServiceServer(s.controller)
	s.mockAuthorizer = authorization.NewMockAuthorizer(s.controller)
	s.mockMetricsScope = &mocks.Scope{}
	s.handler = NewAccessControlledHandlerImpl(frontendHandlerGRPC, s.mockAuthorizer)
}

func (s *accessControlledHandlerSuite) TearDownTest() {
	s.controller.Finish()
	s.mockMetricsScope.AssertExpectations(s.T())
}

func (s *accessControlledHandlerSuite) TestIsAuthorized() {
	ctx := context.Background()
	attr := &authorization.Attributes{}

	s.mockMetricsScope.On("StartTimer", metrics.ServiceAuthorizationLatency).
		Return(metrics.Stopwatch{}).Once()
	s.mockAuthorizer.EXPECT().Authorize(ctx, attr).
		Return(authorization.Result{Decision: authorization.DecisionAllow}, nil).Times(1)

	res, err := s.handler.isAuthorized(ctx, attr, s.mockMetricsScope)
	s.True(res)
	s.NoError(err)
}

func (s *accessControlledHandlerSuite) TestIsAuthorized_Failed() {
	ctx := context.Background()
	attr := &authorization.Attributes{}

	s.mockMetricsScope.On("StartTimer", metrics.ServiceAuthorizationLatency).
		Return(metrics.Stopwatch{}).Once()
	s.mockAuthorizer.EXPECT().Authorize(ctx, attr).
		Return(authorization.Result{Decision: authorization.DecisionDeny}, errors.New("test")).
		Times(1)
	s.mockMetricsScope.On("IncCounter", metrics.ServiceErrAuthorizeFailedCounter).Once()

	res, err := s.handler.isAuthorized(ctx, attr, s.mockMetricsScope)
	s.False(res)
	s.Error(err)
}

func (s *accessControlledHandlerSuite) TestIsAuthorized_Unauthorized() {
	ctx := context.Background()
	attr := &authorization.Attributes{}

	s.mockMetricsScope.On("StartTimer", metrics.ServiceAuthorizationLatency).
		Return(metrics.Stopwatch{}).Once()
	s.mockAuthorizer.EXPECT().Authorize(ctx, attr).
		Return(authorization.Result{Decision: authorization.DecisionDeny}, nil).
		Times(1)
	s.mockMetricsScope.On("IncCounter", metrics.ServiceErrUnauthorizedCounter).Once()

	res, err := s.handler.isAuthorized(ctx, attr, s.mockMetricsScope)
	s.False(res)
	s.NoError(err)
}
