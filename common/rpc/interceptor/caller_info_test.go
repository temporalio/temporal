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

package interceptor

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"

	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/namespace"
)

type (
	callerInfoSuite struct {
		suite.Suite
		*require.Assertions

		controller   *gomock.Controller
		mockRegistry *namespace.MockRegistry

		interceptor *CallerInfoInterceptor
	}
)

func TestCallerInfoSuite(t *testing.T) {
	s := new(callerInfoSuite)
	suite.Run(t, s)
}

func (s *callerInfoSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.mockRegistry = namespace.NewMockRegistry(s.controller)

	s.interceptor = NewCallerInfoInterceptor(s.mockRegistry)
}

func (s *callerInfoSuite) TearDownSuite() {
	s.controller.Finish()
}

func (s *callerInfoSuite) TestIntercept_CallerName() {
	// testNamespaceID := namespace.NewID()
	testNamespaceName := namespace.Name("test-namespace")
	// s.mockRegistry.EXPECT().GetNamespaceName(testNamespaceID).Return(testNamespaceName, nil).AnyTimes()
	s.mockRegistry.EXPECT().GetNamespace(gomock.Any()).Return(nil, nil).AnyTimes()

	testCases := []struct {
		setupIncomingCtx   func() context.Context
		request            interface{}
		expectedCallerName string
	}{
		{
			// test context with no caller info
			setupIncomingCtx: func() context.Context {
				return context.Background()
			},
			request: &workflowservice.StartWorkflowExecutionRequest{
				Namespace: testNamespaceName.String(),
			},
			expectedCallerName: testNamespaceName.String(),
		},
		{
			// test context with caller type but no caller name
			setupIncomingCtx: func() context.Context {
				return headers.SetCallerType(context.Background(), headers.CallerTypeBackground)
			},
			request: &workflowservice.StartWorkflowExecutionRequest{
				Namespace: testNamespaceName.String(),
			},
			expectedCallerName: testNamespaceName.String(),
		},
		{
			// test context with caller name
			setupIncomingCtx: func() context.Context {
				return headers.SetCallerName(context.Background(), headers.CallerNameSystem)
			},
			request: &workflowservice.StartWorkflowExecutionRequest{
				Namespace: testNamespaceName.String(),
			},
			expectedCallerName: headers.CallerNameSystem,
		},
		{
			// test context with empty caller name
			setupIncomingCtx: func() context.Context {
				return headers.SetCallerName(context.Background(), "")
			},
			request: &workflowservice.StartWorkflowExecutionRequest{
				Namespace: testNamespaceName.String(),
			},
			expectedCallerName: testNamespaceName.String(),
		},
	}

	for _, testCase := range testCases {
		ctx := testCase.setupIncomingCtx()

		var resultingCtx context.Context
		_, err := s.interceptor.Intercept(
			ctx,
			testCase.request,
			&grpc.UnaryServerInfo{},
			func(ctx context.Context, req interface{}) (interface{}, error) {
				resultingCtx = ctx
				return nil, nil
			},
		)
		s.NoError(err)

		actualCallerName := headers.GetCallerInfo(resultingCtx).CallerName
		s.Equal(testCase.expectedCallerName, actualCallerName)
	}
}

func (s *callerInfoSuite) TestIntercept_CallerType() {
	s.mockRegistry.EXPECT().GetNamespace(gomock.Any()).Return(nil, nil).AnyTimes()

	testCases := []struct {
		setupIncomingCtx   func() context.Context
		request            interface{}
		expectedCallerType string
	}{
		{
			// test context with no caller info
			setupIncomingCtx: func() context.Context {
				return context.Background()
			},
			request:            &workflowservice.StartWorkflowExecutionRequest{},
			expectedCallerType: headers.CallerTypeAPI,
		},
		{
			// test context with caller name but no caller type
			setupIncomingCtx: func() context.Context {
				return headers.SetCallerName(context.Background(), "test-namespace")
			},
			request:            &workflowservice.StartWorkflowExecutionRequest{},
			expectedCallerType: headers.CallerTypeAPI,
		},
		{
			// test context with caller type
			setupIncomingCtx: func() context.Context {
				return headers.SetCallerType(context.Background(), headers.CallerTypeBackground)
			},
			request:            &workflowservice.StartWorkflowExecutionRequest{},
			expectedCallerType: headers.CallerTypeBackground,
		},
		{
			// test context with empty caller type
			setupIncomingCtx: func() context.Context {
				return headers.SetCallerType(context.Background(), "")
			},
			request:            &workflowservice.StartWorkflowExecutionRequest{},
			expectedCallerType: headers.CallerTypeAPI,
		},
	}

	for _, testCase := range testCases {
		ctx := testCase.setupIncomingCtx()

		var resultingCtx context.Context
		_, err := s.interceptor.Intercept(
			ctx,
			testCase.request,
			&grpc.UnaryServerInfo{},
			func(ctx context.Context, req interface{}) (interface{}, error) {
				resultingCtx = ctx
				return nil, nil
			},
		)
		s.NoError(err)

		actualCallerType := headers.GetCallerInfo(resultingCtx).CallerType
		s.Equal(testCase.expectedCallerType, actualCallerType)
	}
}

func (s *callerInfoSuite) TestIntercept_CallOrigin() {
	method := "startWorkflowExecutionRequest"
	serverInfo := &grpc.UnaryServerInfo{
		FullMethod: "/temporal/" + method,
	}
	s.mockRegistry.EXPECT().GetNamespace(gomock.Any()).Return(nil, nil).AnyTimes()

	testCases := []struct {
		setupIncomingCtx   func() context.Context
		request            interface{}
		expectedCallOrigin string
	}{
		{
			// test context with no caller info
			setupIncomingCtx: func() context.Context {
				return context.Background()
			},
			request:            &workflowservice.StartWorkflowExecutionRequest{},
			expectedCallOrigin: method,
		},
		{
			// test context with api caller type but no call initiation
			setupIncomingCtx: func() context.Context {
				return headers.SetCallerName(context.Background(), "test-namespace")
			},
			request:            &workflowservice.StartWorkflowExecutionRequest{},
			expectedCallOrigin: method,
		},
		{
			// test context with background caller type but no call initiation
			setupIncomingCtx: func() context.Context {
				return headers.SetCallerInfo(context.Background(), headers.SystemBackgroundCallerInfo)
			},
			request:            &workflowservice.StartWorkflowExecutionRequest{},
			expectedCallOrigin: "",
		},
		{
			// test context with call initiation
			setupIncomingCtx: func() context.Context {
				return headers.SetOrigin(context.Background(), "test-method")
			},
			request:            &workflowservice.StartWorkflowExecutionRequest{},
			expectedCallOrigin: "test-method",
		},
		{
			// test context with empty call initiation
			setupIncomingCtx: func() context.Context {
				return headers.SetOrigin(context.Background(), "")
			},
			request:            &workflowservice.StartWorkflowExecutionRequest{},
			expectedCallOrigin: method,
		},
	}

	for _, testCase := range testCases {
		ctx := testCase.setupIncomingCtx()

		var resultingCtx context.Context
		_, err := s.interceptor.Intercept(
			ctx,
			testCase.request,
			serverInfo,
			func(ctx context.Context, req interface{}) (interface{}, error) {
				resultingCtx = ctx
				return nil, nil
			},
		)
		s.NoError(err)

		actualCallOrigin := headers.GetCallerInfo(resultingCtx).CallOrigin
		s.Equal(testCase.expectedCallOrigin, actualCallOrigin)
	}
}
