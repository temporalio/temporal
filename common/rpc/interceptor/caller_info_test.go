package interceptor

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/namespace"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
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
	testNamespaceName := namespace.Name("test-namespace").String()
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
				Namespace: testNamespaceName,
			},
			expectedCallerName: testNamespaceName,
		},
		{
			// test context with caller type but no caller name
			setupIncomingCtx: func() context.Context {
				return headers.SetCallerType(context.Background(), headers.CallerTypeBackgroundHigh)
			},
			request: &workflowservice.StartWorkflowExecutionRequest{
				Namespace: testNamespaceName,
			},
			expectedCallerName: testNamespaceName,
		},
		{
			// test context with matching caller name
			setupIncomingCtx: func() context.Context {
				return headers.SetCallerName(context.Background(), testNamespaceName)
			},
			request: &workflowservice.StartWorkflowExecutionRequest{
				Namespace: testNamespaceName,
			},
			expectedCallerName: testNamespaceName,
		},
		{
			// test context with empty caller name
			setupIncomingCtx: func() context.Context {
				return headers.SetCallerName(context.Background(), "")
			},
			request: &workflowservice.StartWorkflowExecutionRequest{
				Namespace: testNamespaceName,
			},
			expectedCallerName: testNamespaceName,
		},
		{
			// test context with invalid caller name
			setupIncomingCtx: func() context.Context {
				return headers.SetCallerName(context.Background(), "some-random-value")
			},
			request: &workflowservice.StartWorkflowExecutionRequest{
				Namespace: testNamespaceName,
			},
			expectedCallerName: testNamespaceName,
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
				return headers.SetCallerType(context.Background(), headers.CallerTypeBackgroundHigh)
			},
			request:            &workflowservice.StartWorkflowExecutionRequest{},
			expectedCallerType: headers.CallerTypeBackgroundHigh,
		},
		{
			// test context with empty caller type
			setupIncomingCtx: func() context.Context {
				return headers.SetCallerType(context.Background(), "")
			},
			request:            &workflowservice.StartWorkflowExecutionRequest{},
			expectedCallerType: headers.CallerTypeAPI,
		},
		{
			// test context with invalid caller type
			setupIncomingCtx: func() context.Context {
				return headers.SetCallerType(context.Background(), "some-random-value")
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
			// test context with api caller name but no call origin
			setupIncomingCtx: func() context.Context {
				return headers.SetCallerName(context.Background(), "test-namespace")
			},
			request:            &workflowservice.StartWorkflowExecutionRequest{},
			expectedCallOrigin: method,
		},
		{
			// test context with background caller type but no call origin
			setupIncomingCtx: func() context.Context {
				return headers.SetCallerInfo(context.Background(), headers.SystemBackgroundHighCallerInfo)
			},
			request:            &workflowservice.StartWorkflowExecutionRequest{},
			expectedCallOrigin: "",
		},
		{
			// test context with matchcing call origin
			setupIncomingCtx: func() context.Context {
				return headers.SetOrigin(context.Background(), method)
			},
			request:            &workflowservice.StartWorkflowExecutionRequest{},
			expectedCallOrigin: method,
		},
		{
			// test context with empty call origin
			setupIncomingCtx: func() context.Context {
				return headers.SetOrigin(context.Background(), "")
			},
			request:            &workflowservice.StartWorkflowExecutionRequest{},
			expectedCallOrigin: method,
		},
		{
			// test context with invalid call origin
			setupIncomingCtx: func() context.Context {
				return headers.SetOrigin(context.Background(), "some-random-value")
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
