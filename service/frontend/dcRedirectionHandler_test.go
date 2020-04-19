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

package frontend

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	tasklistpb "go.temporal.io/temporal-proto/tasklist"
	"go.temporal.io/temporal-proto/workflowservice"
	"go.temporal.io/temporal-proto/workflowservicemock"

	tokengenpb "github.com/temporalio/temporal/.gen/proto/token"
	"github.com/temporalio/temporal/common/cluster"
	"github.com/temporalio/temporal/common/metrics"
	"github.com/temporalio/temporal/common/primitives"
	"github.com/temporalio/temporal/common/resource"
	"github.com/temporalio/temporal/common/service/config"
	"github.com/temporalio/temporal/common/service/dynamicconfig"

	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

type (
	dcRedirectionHandlerSuite struct {
		suite.Suite
		*require.Assertions

		controller               *gomock.Controller
		mockResource             *resource.Test
		mockFrontendHandler      *workflowservicemock.MockWorkflowServiceServer
		mockRemoteFrontendClient *workflowservicemock.MockWorkflowServiceClient
		mockClusterMetadata      *cluster.MockMetadata

		mockDCRedirectionPolicy *MockDCRedirectionPolicy

		namespace              string
		namespaceID            string
		currentClusterName     string
		alternativeClusterName string
		config                 *Config

		handler *DCRedirectionHandlerImpl
	}

	testServerHandler struct {
		*workflowservicemock.MockWorkflowServiceServer
	}
)

func newTestServerHandler(mockHandler *workflowservicemock.MockWorkflowServiceServer) ServerHandler {
	return &testServerHandler{mockHandler}
}

func TestDCRedirectionHandlerSuite(t *testing.T) {
	s := new(dcRedirectionHandlerSuite)
	suite.Run(t, s)
}

func (s *dcRedirectionHandlerSuite) SetupSuite() {
}

func (s *dcRedirectionHandlerSuite) TearDownSuite() {
}

func (s *dcRedirectionHandlerSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.namespace = "some random namespace name"
	s.namespaceID = "deadbeef-0123-4567-aaaa-bcdef0123456"
	s.currentClusterName = cluster.TestCurrentClusterName
	s.alternativeClusterName = cluster.TestAlternativeClusterName

	s.mockDCRedirectionPolicy = &MockDCRedirectionPolicy{}

	s.controller = gomock.NewController(s.T())
	s.mockResource = resource.NewTest(s.controller, metrics.Frontend)
	s.mockClusterMetadata = s.mockResource.ClusterMetadata
	s.mockRemoteFrontendClient = s.mockResource.RemoteFrontendClient

	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(s.currentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(true).AnyTimes()

	s.config = NewConfig(dynamicconfig.NewCollection(dynamicconfig.NewNopClient(), s.mockResource.GetLogger()), 0, false)

	frontendHandlerGRPC := NewWorkflowHandler(s.mockResource, s.config, nil)

	s.mockFrontendHandler = workflowservicemock.NewMockWorkflowServiceServer(s.controller)
	s.handler = NewDCRedirectionHandler(frontendHandlerGRPC, config.DCRedirectionPolicy{})
	s.handler.frontendHandler = newTestServerHandler(s.mockFrontendHandler)
	s.handler.redirectionPolicy = s.mockDCRedirectionPolicy
}

func (s *dcRedirectionHandlerSuite) TearDownTest() {
	s.controller.Finish()
	s.mockResource.Finish(s.T())
	s.mockDCRedirectionPolicy.AssertExpectations(s.T())
}

func (s *dcRedirectionHandlerSuite) TestDescribeTaskList() {
	apiName := "DescribeTaskList"

	s.mockDCRedirectionPolicy.On("WithNamespaceRedirect",
		s.namespace, apiName, mock.Anything).Return(nil).Times(1)

	req := &workflowservice.DescribeTaskListRequest{
		Namespace: s.namespace,
	}
	resp, err := s.handler.DescribeTaskList(context.Background(), req)
	s.Nil(err)
	// the resp is initialized to nil, since inner function is not called
	s.Nil(resp)

	callFn := s.mockDCRedirectionPolicy.Calls[0].Arguments[2].(func(string) error)
	s.mockFrontendHandler.EXPECT().DescribeTaskList(gomock.Any(), req).Return(&workflowservice.DescribeTaskListResponse{}, nil).Times(1)
	err = callFn(s.currentClusterName)
	s.Nil(err)
	s.mockRemoteFrontendClient.EXPECT().DescribeTaskList(gomock.Any(), req).Return(&workflowservice.DescribeTaskListResponse{}, nil).Times(1)
	err = callFn(s.alternativeClusterName)
	s.Nil(err)
}

func (s *dcRedirectionHandlerSuite) TestDescribeWorkflowExecution() {
	apiName := "DescribeWorkflowExecution"

	s.mockDCRedirectionPolicy.On("WithNamespaceRedirect",
		s.namespace, apiName, mock.Anything).Return(nil).Times(1)

	req := &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: s.namespace,
	}
	resp, err := s.handler.DescribeWorkflowExecution(context.Background(), req)
	s.Nil(err)
	// the resp is initialized to nil, since inner function is not called
	s.Nil(resp)

	callFn := s.mockDCRedirectionPolicy.Calls[0].Arguments[2].(func(string) error)
	s.mockFrontendHandler.EXPECT().DescribeWorkflowExecution(gomock.Any(), req).Return(&workflowservice.DescribeWorkflowExecutionResponse{}, nil).Times(1)
	err = callFn(s.currentClusterName)
	s.Nil(err)
	s.mockRemoteFrontendClient.EXPECT().DescribeWorkflowExecution(gomock.Any(), req).Return(&workflowservice.DescribeWorkflowExecutionResponse{}, nil).Times(1)
	err = callFn(s.alternativeClusterName)
	s.Nil(err)
}

func (s *dcRedirectionHandlerSuite) TestGetWorkflowExecutionHistory() {
	apiName := "GetWorkflowExecutionHistory"

	s.mockDCRedirectionPolicy.On("WithNamespaceRedirect",
		s.namespace, apiName, mock.Anything).Return(nil).Times(1)

	req := &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace: s.namespace,
	}
	resp, err := s.handler.GetWorkflowExecutionHistory(context.Background(), req)
	s.Nil(err)
	// the resp is initialized to nil, since inner function is not called
	s.Nil(resp)

	callFn := s.mockDCRedirectionPolicy.Calls[0].Arguments[2].(func(string) error)
	s.mockFrontendHandler.EXPECT().GetWorkflowExecutionHistory(gomock.Any(), req).Return(&workflowservice.GetWorkflowExecutionHistoryResponse{}, nil).Times(1)
	err = callFn(s.currentClusterName)
	s.Nil(err)
	s.mockRemoteFrontendClient.EXPECT().GetWorkflowExecutionHistory(gomock.Any(), req).Return(&workflowservice.GetWorkflowExecutionHistoryResponse{}, nil).Times(1)
	err = callFn(s.alternativeClusterName)
	s.Nil(err)
}

func (s *dcRedirectionHandlerSuite) TestListArchivedWorkflowExecutions() {
	apiName := "ListArchivedWorkflowExecutions"

	s.mockDCRedirectionPolicy.On("WithNamespaceRedirect",
		s.namespace, apiName, mock.Anything).Return(nil).Times(1)

	req := &workflowservice.ListArchivedWorkflowExecutionsRequest{
		Namespace: s.namespace,
	}
	resp, err := s.handler.ListArchivedWorkflowExecutions(context.Background(), req)
	s.Nil(err)
	// the resp is initialized to nil, since inner function is not called
	s.Nil(resp)

	callFn := s.mockDCRedirectionPolicy.Calls[0].Arguments[2].(func(string) error)
	s.mockFrontendHandler.EXPECT().ListArchivedWorkflowExecutions(gomock.Any(), req).Return(&workflowservice.ListArchivedWorkflowExecutionsResponse{}, nil).Times(1)
	err = callFn(s.currentClusterName)
	s.Nil(err)
	s.mockRemoteFrontendClient.EXPECT().ListArchivedWorkflowExecutions(gomock.Any(), req).Return(&workflowservice.ListArchivedWorkflowExecutionsResponse{}, nil).Times(1)
	err = callFn(s.alternativeClusterName)
	s.Nil(err)
}

func (s *dcRedirectionHandlerSuite) TestListClosedWorkflowExecutions() {
	apiName := "ListClosedWorkflowExecutions"

	s.mockDCRedirectionPolicy.On("WithNamespaceRedirect",
		s.namespace, apiName, mock.Anything).Return(nil).Times(1)

	req := &workflowservice.ListClosedWorkflowExecutionsRequest{
		Namespace: s.namespace,
	}
	resp, err := s.handler.ListClosedWorkflowExecutions(context.Background(), req)
	s.Nil(err)
	// the resp is initialized to nil, since inner function is not called
	s.Nil(resp)

	callFn := s.mockDCRedirectionPolicy.Calls[0].Arguments[2].(func(string) error)
	s.mockFrontendHandler.EXPECT().ListClosedWorkflowExecutions(gomock.Any(), req).Return(&workflowservice.ListClosedWorkflowExecutionsResponse{}, nil).Times(1)
	err = callFn(s.currentClusterName)
	s.Nil(err)
	s.mockRemoteFrontendClient.EXPECT().ListClosedWorkflowExecutions(gomock.Any(), req).Return(&workflowservice.ListClosedWorkflowExecutionsResponse{}, nil).Times(1)
	err = callFn(s.alternativeClusterName)
	s.Nil(err)
}

func (s *dcRedirectionHandlerSuite) TestListOpenWorkflowExecutions() {
	apiName := "ListOpenWorkflowExecutions"

	s.mockDCRedirectionPolicy.On("WithNamespaceRedirect",
		s.namespace, apiName, mock.Anything).Return(nil).Times(1)

	req := &workflowservice.ListOpenWorkflowExecutionsRequest{
		Namespace: s.namespace,
	}
	resp, err := s.handler.ListOpenWorkflowExecutions(context.Background(), req)
	s.Nil(err)
	// the resp is initialized to nil, since inner function is not called
	s.Nil(resp)

	callFn := s.mockDCRedirectionPolicy.Calls[0].Arguments[2].(func(string) error)
	s.mockFrontendHandler.EXPECT().ListOpenWorkflowExecutions(gomock.Any(), req).Return(&workflowservice.ListOpenWorkflowExecutionsResponse{}, nil).Times(1)
	err = callFn(s.currentClusterName)
	s.Nil(err)
	s.mockRemoteFrontendClient.EXPECT().ListOpenWorkflowExecutions(gomock.Any(), req).Return(&workflowservice.ListOpenWorkflowExecutionsResponse{}, nil).Times(1)
	err = callFn(s.alternativeClusterName)
	s.Nil(err)
}

func (s *dcRedirectionHandlerSuite) TestListWorkflowExecutions() {
	apiName := "ListWorkflowExecutions"

	s.mockDCRedirectionPolicy.On("WithNamespaceRedirect",
		s.namespace, apiName, mock.Anything).Return(nil).Times(1)

	req := &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: s.namespace,
	}
	resp, err := s.handler.ListWorkflowExecutions(context.Background(), req)
	s.Nil(err)
	// the resp is initialized to nil, since inner function is not called
	s.Nil(resp)

	callFn := s.mockDCRedirectionPolicy.Calls[0].Arguments[2].(func(string) error)
	s.mockFrontendHandler.EXPECT().ListWorkflowExecutions(gomock.Any(), req).Return(&workflowservice.ListWorkflowExecutionsResponse{}, nil).Times(1)
	err = callFn(s.currentClusterName)
	s.Nil(err)
	s.mockRemoteFrontendClient.EXPECT().ListWorkflowExecutions(gomock.Any(), req).Return(&workflowservice.ListWorkflowExecutionsResponse{}, nil).Times(1)
	err = callFn(s.alternativeClusterName)
	s.Nil(err)
}

func (s *dcRedirectionHandlerSuite) TestScanWorkflowExecutions() {
	apiName := "ScanWorkflowExecutions"

	s.mockDCRedirectionPolicy.On("WithNamespaceRedirect",
		s.namespace, apiName, mock.Anything).Return(nil).Times(1)

	req := &workflowservice.ScanWorkflowExecutionsRequest{
		Namespace: s.namespace,
	}
	resp, err := s.handler.ScanWorkflowExecutions(context.Background(), req)
	s.Nil(err)
	// the resp is initialized to nil, since inner function is not called
	s.Nil(resp)

	callFn := s.mockDCRedirectionPolicy.Calls[0].Arguments[2].(func(string) error)
	s.mockFrontendHandler.EXPECT().ScanWorkflowExecutions(gomock.Any(), req).Return(&workflowservice.ScanWorkflowExecutionsResponse{}, nil).Times(1)
	err = callFn(s.currentClusterName)
	s.Nil(err)
	s.mockRemoteFrontendClient.EXPECT().ScanWorkflowExecutions(gomock.Any(), req).Return(&workflowservice.ScanWorkflowExecutionsResponse{}, nil).Times(1)
	err = callFn(s.alternativeClusterName)
	s.Nil(err)
}

func (s *dcRedirectionHandlerSuite) TestCountWorkflowExecutions() {
	apiName := "CountWorkflowExecutions"

	s.mockDCRedirectionPolicy.On("WithNamespaceRedirect",
		s.namespace, apiName, mock.Anything).Return(nil).Times(1)

	req := &workflowservice.CountWorkflowExecutionsRequest{
		Namespace: s.namespace,
	}
	resp, err := s.handler.CountWorkflowExecutions(context.Background(), req)
	s.Nil(err)
	// the resp is initialized to nil, since inner function is not called
	s.Nil(resp)

	callFn := s.mockDCRedirectionPolicy.Calls[0].Arguments[2].(func(string) error)
	s.mockFrontendHandler.EXPECT().CountWorkflowExecutions(gomock.Any(), req).Return(&workflowservice.CountWorkflowExecutionsResponse{}, nil).Times(1)
	err = callFn(s.currentClusterName)
	s.Nil(err)
	s.mockRemoteFrontendClient.EXPECT().CountWorkflowExecutions(gomock.Any(), req).Return(&workflowservice.CountWorkflowExecutionsResponse{}, nil).Times(1)
	err = callFn(s.alternativeClusterName)
	s.Nil(err)
}

func (s *dcRedirectionHandlerSuite) TestPollForActivityTask() {
	apiName := "PollForActivityTask"

	s.mockDCRedirectionPolicy.On("WithNamespaceRedirect",
		s.namespace, apiName, mock.Anything).Return(nil).Times(1)

	req := &workflowservice.PollForActivityTaskRequest{
		Namespace: s.namespace,
	}
	resp, err := s.handler.PollForActivityTask(context.Background(), req)
	s.Nil(err)
	// the resp is initialized to nil, since inner function is not called
	s.Nil(resp)

	callFn := s.mockDCRedirectionPolicy.Calls[0].Arguments[2].(func(string) error)
	s.mockFrontendHandler.EXPECT().PollForActivityTask(gomock.Any(), req).Return(&workflowservice.PollForActivityTaskResponse{}, nil).Times(1)
	err = callFn(s.currentClusterName)
	s.Nil(err)
	s.mockRemoteFrontendClient.EXPECT().PollForActivityTask(gomock.Any(), req).Return(&workflowservice.PollForActivityTaskResponse{}, nil).Times(1)
	err = callFn(s.alternativeClusterName)
	s.Nil(err)
}

func (s *dcRedirectionHandlerSuite) TestPollForDecisionTask() {
	apiName := "PollForDecisionTask"

	s.mockDCRedirectionPolicy.On("WithNamespaceRedirect",
		s.namespace, apiName, mock.Anything).Return(nil).Times(1)

	req := &workflowservice.PollForDecisionTaskRequest{
		Namespace: s.namespace,
	}
	resp, err := s.handler.PollForDecisionTask(context.Background(), req)
	s.Nil(err)
	// the resp is initialized to nil, since inner function is not called
	s.Nil(resp)

	callFn := s.mockDCRedirectionPolicy.Calls[0].Arguments[2].(func(string) error)
	s.mockFrontendHandler.EXPECT().PollForDecisionTask(gomock.Any(), req).Return(&workflowservice.PollForDecisionTaskResponse{}, nil).Times(1)
	err = callFn(s.currentClusterName)
	s.Nil(err)
	s.mockRemoteFrontendClient.EXPECT().PollForDecisionTask(gomock.Any(), req).Return(&workflowservice.PollForDecisionTaskResponse{}, nil).Times(1)
	err = callFn(s.alternativeClusterName)
	s.Nil(err)
}

func (s *dcRedirectionHandlerSuite) TestQueryWorkflow() {
	apiName := "QueryWorkflow"

	s.mockDCRedirectionPolicy.On("WithNamespaceRedirect",
		s.namespace, apiName, mock.Anything).Return(nil).Times(1)

	req := &workflowservice.QueryWorkflowRequest{
		Namespace: s.namespace,
	}
	resp, err := s.handler.QueryWorkflow(context.Background(), req)
	s.Nil(err)
	// the resp is initialized to nil, since inner function is not called
	s.Nil(resp)

	callFn := s.mockDCRedirectionPolicy.Calls[0].Arguments[2].(func(string) error)
	s.mockFrontendHandler.EXPECT().QueryWorkflow(gomock.Any(), req).Return(&workflowservice.QueryWorkflowResponse{}, nil).Times(1)
	err = callFn(s.currentClusterName)
	s.Nil(err)
	s.mockRemoteFrontendClient.EXPECT().QueryWorkflow(gomock.Any(), req).Return(&workflowservice.QueryWorkflowResponse{}, nil).Times(1)
	err = callFn(s.alternativeClusterName)
	s.Nil(err)
}

func (s *dcRedirectionHandlerSuite) TestRecordActivityTaskHeartbeat() {
	apiName := "RecordActivityTaskHeartbeat"

	s.mockDCRedirectionPolicy.On("WithNamespaceIDRedirect",
		s.namespaceID, apiName, mock.Anything).Return(nil).Times(1)

	taskToken, err := s.handler.tokenSerializer.Serialize(&tokengenpb.Task{
		NamespaceId: primitives.MustParseUUID(s.namespaceID),
	})
	s.Nil(err)
	req := &workflowservice.RecordActivityTaskHeartbeatRequest{
		TaskToken: taskToken,
	}
	resp, err := s.handler.RecordActivityTaskHeartbeat(context.Background(), req)
	s.Nil(err)
	// the resp is initialized to nil, since inner function is not called
	s.Nil(resp)

	callFn := s.mockDCRedirectionPolicy.Calls[0].Arguments[2].(func(string) error)
	s.mockFrontendHandler.EXPECT().RecordActivityTaskHeartbeat(gomock.Any(), req).Return(&workflowservice.RecordActivityTaskHeartbeatResponse{}, nil).Times(1)
	err = callFn(s.currentClusterName)
	s.Nil(err)
	s.mockRemoteFrontendClient.EXPECT().RecordActivityTaskHeartbeat(gomock.Any(), req).Return(&workflowservice.RecordActivityTaskHeartbeatResponse{}, nil).Times(1)
	err = callFn(s.alternativeClusterName)
	s.Nil(err)
}

func (s *dcRedirectionHandlerSuite) TestRecordActivityTaskHeartbeatById() {
	apiName := "RecordActivityTaskHeartbeatById"

	s.mockDCRedirectionPolicy.On("WithNamespaceRedirect",
		s.namespace, apiName, mock.Anything).Return(nil).Times(1)

	req := &workflowservice.RecordActivityTaskHeartbeatByIdRequest{
		Namespace: s.namespace,
	}
	resp, err := s.handler.RecordActivityTaskHeartbeatById(context.Background(), req)
	s.Nil(err)
	// the resp is initialized to nil, since inner function is not called
	s.Nil(resp)

	callFn := s.mockDCRedirectionPolicy.Calls[0].Arguments[2].(func(string) error)
	s.mockFrontendHandler.EXPECT().RecordActivityTaskHeartbeatById(gomock.Any(), req).Return(&workflowservice.RecordActivityTaskHeartbeatByIdResponse{}, nil).Times(1)
	err = callFn(s.currentClusterName)
	s.Nil(err)
	s.mockRemoteFrontendClient.EXPECT().RecordActivityTaskHeartbeatById(gomock.Any(), req).Return(&workflowservice.RecordActivityTaskHeartbeatByIdResponse{}, nil).Times(1)
	err = callFn(s.alternativeClusterName)
	s.Nil(err)
}

func (s *dcRedirectionHandlerSuite) TestRequestCancelWorkflowExecution() {
	apiName := "RequestCancelWorkflowExecution"

	s.mockDCRedirectionPolicy.On("WithNamespaceRedirect",
		s.namespace, apiName, mock.Anything).Return(nil).Times(1)

	req := &workflowservice.RequestCancelWorkflowExecutionRequest{
		Namespace: s.namespace,
	}
	resp, err := s.handler.RequestCancelWorkflowExecution(context.Background(), req)
	s.Nil(err)
	// the resp is initialized to nil, since inner function is not called
	s.Nil(resp)

	callFn := s.mockDCRedirectionPolicy.Calls[0].Arguments[2].(func(string) error)
	s.mockFrontendHandler.EXPECT().RequestCancelWorkflowExecution(gomock.Any(), req).Return(&workflowservice.RequestCancelWorkflowExecutionResponse{}, nil).Times(1)
	err = callFn(s.currentClusterName)
	s.Nil(err)
	s.mockRemoteFrontendClient.EXPECT().RequestCancelWorkflowExecution(gomock.Any(), req).Return(&workflowservice.RequestCancelWorkflowExecutionResponse{}, nil).Times(1)
	err = callFn(s.alternativeClusterName)
	s.Nil(err)
}

func (s *dcRedirectionHandlerSuite) TestResetStickyTaskList() {
	apiName := "ResetStickyTaskList"

	s.mockDCRedirectionPolicy.On("WithNamespaceRedirect",
		s.namespace, apiName, mock.Anything).Return(nil).Times(1)

	req := &workflowservice.ResetStickyTaskListRequest{
		Namespace: s.namespace,
	}
	resp, err := s.handler.ResetStickyTaskList(context.Background(), req)
	s.Nil(err)
	// the resp is initialized to nil, since inner function is not called
	s.Nil(resp)

	callFn := s.mockDCRedirectionPolicy.Calls[0].Arguments[2].(func(string) error)
	s.mockFrontendHandler.EXPECT().ResetStickyTaskList(gomock.Any(), req).Return(&workflowservice.ResetStickyTaskListResponse{}, nil).Times(1)
	err = callFn(s.currentClusterName)
	s.Nil(err)
	s.mockRemoteFrontendClient.EXPECT().ResetStickyTaskList(gomock.Any(), req).Return(&workflowservice.ResetStickyTaskListResponse{}, nil).Times(1)
	err = callFn(s.alternativeClusterName)
	s.Nil(err)
}

func (s *dcRedirectionHandlerSuite) TestResetWorkflowExecution() {
	apiName := "ResetWorkflowExecution"

	s.mockDCRedirectionPolicy.On("WithNamespaceRedirect",
		s.namespace, apiName, mock.Anything).Return(nil).Times(1)

	req := &workflowservice.ResetWorkflowExecutionRequest{
		Namespace: s.namespace,
	}
	resp, err := s.handler.ResetWorkflowExecution(context.Background(), req)
	s.Nil(err)
	// the resp is initialized to nil, since inner function is not called
	s.Nil(resp)

	callFn := s.mockDCRedirectionPolicy.Calls[0].Arguments[2].(func(string) error)
	s.mockFrontendHandler.EXPECT().ResetWorkflowExecution(gomock.Any(), req).Return(&workflowservice.ResetWorkflowExecutionResponse{}, nil).Times(1)
	err = callFn(s.currentClusterName)
	s.Nil(err)
	s.mockRemoteFrontendClient.EXPECT().ResetWorkflowExecution(gomock.Any(), req).Return(&workflowservice.ResetWorkflowExecutionResponse{}, nil).Times(1)
	err = callFn(s.alternativeClusterName)
	s.Nil(err)
}

func (s *dcRedirectionHandlerSuite) TestRespondActivityTaskCanceled() {
	apiName := "RespondActivityTaskCanceled"

	s.mockDCRedirectionPolicy.On("WithNamespaceIDRedirect",
		s.namespaceID, apiName, mock.Anything).Return(nil).Times(1)

	token, err := s.handler.tokenSerializer.Serialize(&tokengenpb.Task{
		NamespaceId: primitives.MustParseUUID(s.namespaceID),
	})
	s.Nil(err)
	req := &workflowservice.RespondActivityTaskCanceledRequest{
		TaskToken: token,
	}
	resp, err := s.handler.RespondActivityTaskCanceled(context.Background(), req)
	s.Nil(err)
	s.Nil(resp)

	callFn := s.mockDCRedirectionPolicy.Calls[0].Arguments[2].(func(string) error)
	s.mockFrontendHandler.EXPECT().RespondActivityTaskCanceled(gomock.Any(), req).Return(&workflowservice.RespondActivityTaskCanceledResponse{}, nil).Times(1)
	err = callFn(s.currentClusterName)
	s.Nil(err)
	s.mockRemoteFrontendClient.EXPECT().RespondActivityTaskCanceled(gomock.Any(), req).Return(&workflowservice.RespondActivityTaskCanceledResponse{}, nil).Times(1)
	err = callFn(s.alternativeClusterName)
	s.Nil(err)
}

func (s *dcRedirectionHandlerSuite) TestRespondActivityTaskCanceledById() {
	apiName := "RespondActivityTaskCanceledById"

	s.mockDCRedirectionPolicy.On("WithNamespaceRedirect",
		s.namespace, apiName, mock.Anything).Return(nil).Times(1)

	req := &workflowservice.RespondActivityTaskCanceledByIdRequest{
		Namespace: s.namespace,
	}
	resp, err := s.handler.RespondActivityTaskCanceledById(context.Background(), req)
	s.Nil(err)
	s.Nil(resp)

	callFn := s.mockDCRedirectionPolicy.Calls[0].Arguments[2].(func(string) error)
	s.mockFrontendHandler.EXPECT().RespondActivityTaskCanceledById(gomock.Any(), req).Return(&workflowservice.RespondActivityTaskCanceledByIdResponse{}, nil).Times(1)
	err = callFn(s.currentClusterName)
	s.Nil(err)
	s.mockRemoteFrontendClient.EXPECT().RespondActivityTaskCanceledById(gomock.Any(), req).Return(&workflowservice.RespondActivityTaskCanceledByIdResponse{}, nil).Times(1)
	err = callFn(s.alternativeClusterName)
	s.Nil(err)
}

func (s *dcRedirectionHandlerSuite) TestRespondActivityTaskCompleted() {
	apiName := "RespondActivityTaskCompleted"

	s.mockDCRedirectionPolicy.On("WithNamespaceIDRedirect",
		s.namespaceID, apiName, mock.Anything).Return(nil).Times(1)

	taskToken, err := s.handler.tokenSerializer.Serialize(&tokengenpb.Task{
		NamespaceId: primitives.MustParseUUID(s.namespaceID),
	})
	s.Nil(err)
	req := &workflowservice.RespondActivityTaskCompletedRequest{
		TaskToken: taskToken,
	}
	resp, err := s.handler.RespondActivityTaskCompleted(context.Background(), req)
	s.Nil(err)
	s.Nil(resp)

	callFn := s.mockDCRedirectionPolicy.Calls[0].Arguments[2].(func(string) error)
	s.mockFrontendHandler.EXPECT().RespondActivityTaskCompleted(gomock.Any(), req).Return(&workflowservice.RespondActivityTaskCompletedResponse{}, nil).Times(1)
	err = callFn(s.currentClusterName)
	s.Nil(err)
	s.mockRemoteFrontendClient.EXPECT().RespondActivityTaskCompleted(gomock.Any(), req).Return(&workflowservice.RespondActivityTaskCompletedResponse{}, nil).Times(1)
	err = callFn(s.alternativeClusterName)
	s.Nil(err)
}

func (s *dcRedirectionHandlerSuite) TestRespondActivityTaskCompletedById() {
	apiName := "RespondActivityTaskCompletedById"

	s.mockDCRedirectionPolicy.On("WithNamespaceRedirect",
		s.namespace, apiName, mock.Anything).Return(nil).Times(1)

	req := &workflowservice.RespondActivityTaskCompletedByIdRequest{
		Namespace: s.namespace,
	}
	resp, err := s.handler.RespondActivityTaskCompletedById(context.Background(), req)
	s.Nil(err)
	s.Nil(resp)

	callFn := s.mockDCRedirectionPolicy.Calls[0].Arguments[2].(func(string) error)
	s.mockFrontendHandler.EXPECT().RespondActivityTaskCompletedById(gomock.Any(), req).Return(&workflowservice.RespondActivityTaskCompletedByIdResponse{}, nil).Times(1)
	err = callFn(s.currentClusterName)
	s.Nil(err)
	s.mockRemoteFrontendClient.EXPECT().RespondActivityTaskCompletedById(gomock.Any(), req).Return(&workflowservice.RespondActivityTaskCompletedByIdResponse{}, nil).Times(1)
	err = callFn(s.alternativeClusterName)
	s.Nil(err)
}

func (s *dcRedirectionHandlerSuite) TestRespondActivityTaskFailed() {
	apiName := "RespondActivityTaskFailed"

	s.mockDCRedirectionPolicy.On("WithNamespaceIDRedirect",
		s.namespaceID, apiName, mock.Anything).Return(nil).Times(1)

	taskToken, err := s.handler.tokenSerializer.Serialize(&tokengenpb.Task{
		NamespaceId: primitives.MustParseUUID(s.namespaceID),
	})
	s.Nil(err)
	req := &workflowservice.RespondActivityTaskFailedRequest{
		TaskToken: taskToken,
	}
	resp, err := s.handler.RespondActivityTaskFailed(context.Background(), req)
	s.Nil(err)
	s.Nil(resp)

	callFn := s.mockDCRedirectionPolicy.Calls[0].Arguments[2].(func(string) error)
	s.mockFrontendHandler.EXPECT().RespondActivityTaskFailed(gomock.Any(), req).Return(&workflowservice.RespondActivityTaskFailedResponse{}, nil).Times(1)
	err = callFn(s.currentClusterName)
	s.Nil(err)
	s.mockRemoteFrontendClient.EXPECT().RespondActivityTaskFailed(gomock.Any(), req).Return(&workflowservice.RespondActivityTaskFailedResponse{}, nil).Times(1)
	err = callFn(s.alternativeClusterName)
	s.Nil(err)
}

func (s *dcRedirectionHandlerSuite) TestRespondActivityTaskFailedById() {
	apiName := "RespondActivityTaskFailedById"

	s.mockDCRedirectionPolicy.On("WithNamespaceRedirect",
		s.namespace, apiName, mock.Anything).Return(nil).Times(1)

	req := &workflowservice.RespondActivityTaskFailedByIdRequest{
		Namespace: s.namespace,
	}
	resp, err := s.handler.RespondActivityTaskFailedById(context.Background(), req)
	s.Nil(err)
	s.Nil(resp)

	callFn := s.mockDCRedirectionPolicy.Calls[0].Arguments[2].(func(string) error)
	s.mockFrontendHandler.EXPECT().RespondActivityTaskFailedById(gomock.Any(), req).Return(&workflowservice.RespondActivityTaskFailedByIdResponse{}, nil).Times(1)
	err = callFn(s.currentClusterName)
	s.Nil(err)
	s.mockRemoteFrontendClient.EXPECT().RespondActivityTaskFailedById(gomock.Any(), req).Return(&workflowservice.RespondActivityTaskFailedByIdResponse{}, nil).Times(1)
	err = callFn(s.alternativeClusterName)
	s.Nil(err)
}

func (s *dcRedirectionHandlerSuite) TestRespondDecisionTaskCompleted() {
	apiName := "RespondDecisionTaskCompleted"

	s.mockDCRedirectionPolicy.On("WithNamespaceIDRedirect",
		s.namespaceID, apiName, mock.Anything).Return(nil).Times(1)

	taskToken, err := s.handler.tokenSerializer.Serialize(&tokengenpb.Task{
		NamespaceId: primitives.MustParseUUID(s.namespaceID),
	})
	s.Nil(err)
	req := &workflowservice.RespondDecisionTaskCompletedRequest{
		TaskToken: taskToken,
	}
	resp, err := s.handler.RespondDecisionTaskCompleted(context.Background(), req)
	s.Nil(err)
	// the resp is initialized to nil, since inner function is not called
	s.Nil(resp)

	callFn := s.mockDCRedirectionPolicy.Calls[0].Arguments[2].(func(string) error)
	s.mockFrontendHandler.EXPECT().RespondDecisionTaskCompleted(gomock.Any(), req).Return(&workflowservice.RespondDecisionTaskCompletedResponse{}, nil).Times(1)
	err = callFn(s.currentClusterName)
	s.Nil(err)
	s.mockRemoteFrontendClient.EXPECT().RespondDecisionTaskCompleted(gomock.Any(), req).Return(&workflowservice.RespondDecisionTaskCompletedResponse{}, nil).Times(1)
	err = callFn(s.alternativeClusterName)
	s.Nil(err)
}

func (s *dcRedirectionHandlerSuite) TestRespondDecisionTaskFailed() {
	apiName := "RespondDecisionTaskFailed"

	s.mockDCRedirectionPolicy.On("WithNamespaceIDRedirect",
		s.namespaceID, apiName, mock.Anything).Return(nil).Times(1)

	token, err := s.handler.tokenSerializer.Serialize(&tokengenpb.Task{
		NamespaceId: primitives.MustParseUUID(s.namespaceID),
	})
	s.Nil(err)
	req := &workflowservice.RespondDecisionTaskFailedRequest{
		TaskToken: token,
	}
	resp, err := s.handler.RespondDecisionTaskFailed(context.Background(), req)
	s.Nil(err)
	s.Nil(resp)

	callFn := s.mockDCRedirectionPolicy.Calls[0].Arguments[2].(func(string) error)
	s.mockFrontendHandler.EXPECT().RespondDecisionTaskFailed(gomock.Any(), req).Return(&workflowservice.RespondDecisionTaskFailedResponse{}, nil).Times(1)
	err = callFn(s.currentClusterName)
	s.Nil(err)
	s.mockRemoteFrontendClient.EXPECT().RespondDecisionTaskFailed(gomock.Any(), req).Return(&workflowservice.RespondDecisionTaskFailedResponse{}, nil).Times(1)
	err = callFn(s.alternativeClusterName)
	s.Nil(err)
}

func (s *dcRedirectionHandlerSuite) TestRespondQueryTaskCompleted() {
	apiName := "RespondQueryTaskCompleted"

	s.mockDCRedirectionPolicy.On("WithNamespaceIDRedirect",
		s.namespaceID, apiName, mock.Anything).Return(nil).Times(1)

	taskToken, err := s.handler.tokenSerializer.SerializeQueryTaskToken(&tokengenpb.QueryTask{
		NamespaceId: s.namespaceID,
	})
	req := &workflowservice.RespondQueryTaskCompletedRequest{
		TaskToken: taskToken,
	}
	resp, err := s.handler.RespondQueryTaskCompleted(context.Background(), req)
	s.Nil(err)
	s.Nil(resp)

	callFn := s.mockDCRedirectionPolicy.Calls[0].Arguments[2].(func(string) error)
	s.mockFrontendHandler.EXPECT().RespondQueryTaskCompleted(gomock.Any(), req).Return(&workflowservice.RespondQueryTaskCompletedResponse{}, nil).Times(1)
	err = callFn(s.currentClusterName)
	s.Nil(err)
	s.mockRemoteFrontendClient.EXPECT().RespondQueryTaskCompleted(gomock.Any(), req).Return(&workflowservice.RespondQueryTaskCompletedResponse{}, nil).Times(1)
	err = callFn(s.alternativeClusterName)
	s.Nil(err)
}

func (s *dcRedirectionHandlerSuite) TestSignalWithStartWorkflowExecution() {
	apiName := "SignalWithStartWorkflowExecution"

	s.mockDCRedirectionPolicy.On("WithNamespaceRedirect",
		s.namespace, apiName, mock.Anything).Return(nil).Times(1)

	req := &workflowservice.SignalWithStartWorkflowExecutionRequest{
		Namespace: s.namespace,
	}
	resp, err := s.handler.SignalWithStartWorkflowExecution(context.Background(), req)
	s.Nil(err)
	// the resp is initialized to nil, since inner function is not called
	s.Nil(resp)

	callFn := s.mockDCRedirectionPolicy.Calls[0].Arguments[2].(func(string) error)
	s.mockFrontendHandler.EXPECT().SignalWithStartWorkflowExecution(gomock.Any(), req).Return(&workflowservice.SignalWithStartWorkflowExecutionResponse{}, nil).Times(1)
	err = callFn(s.currentClusterName)
	s.Nil(err)
	s.mockRemoteFrontendClient.EXPECT().SignalWithStartWorkflowExecution(gomock.Any(), req).Return(&workflowservice.SignalWithStartWorkflowExecutionResponse{}, nil).Times(1)
	err = callFn(s.alternativeClusterName)
	s.Nil(err)
}

func (s *dcRedirectionHandlerSuite) TestSignalWorkflowExecution() {
	apiName := "SignalWorkflowExecution"

	s.mockDCRedirectionPolicy.On("WithNamespaceRedirect",
		s.namespace, apiName, mock.Anything).Return(nil).Times(1)

	req := &workflowservice.SignalWorkflowExecutionRequest{
		Namespace: s.namespace,
	}
	resp, err := s.handler.SignalWorkflowExecution(context.Background(), req)
	s.Nil(err)
	s.Nil(resp)

	callFn := s.mockDCRedirectionPolicy.Calls[0].Arguments[2].(func(string) error)
	s.mockFrontendHandler.EXPECT().SignalWorkflowExecution(gomock.Any(), req).Return(&workflowservice.SignalWorkflowExecutionResponse{}, nil).Times(1)
	err = callFn(s.currentClusterName)
	s.Nil(err)
	s.mockRemoteFrontendClient.EXPECT().SignalWorkflowExecution(gomock.Any(), req).Return(&workflowservice.SignalWorkflowExecutionResponse{}, nil).Times(1)
	err = callFn(s.alternativeClusterName)
	s.Nil(err)
}

func (s *dcRedirectionHandlerSuite) TestStartWorkflowExecution() {
	apiName := "StartWorkflowExecution"

	s.mockDCRedirectionPolicy.On("WithNamespaceRedirect",
		s.namespace, apiName, mock.Anything).Return(nil).Times(1)

	req := &workflowservice.StartWorkflowExecutionRequest{
		Namespace: s.namespace,
	}
	resp, err := s.handler.StartWorkflowExecution(context.Background(), req)
	s.Nil(err)
	// the resp is initialized to nil, since inner function is not called
	s.Nil(resp)

	callFn := s.mockDCRedirectionPolicy.Calls[0].Arguments[2].(func(string) error)
	s.mockFrontendHandler.EXPECT().StartWorkflowExecution(gomock.Any(), req).Return(&workflowservice.StartWorkflowExecutionResponse{}, nil).Times(1)
	err = callFn(s.currentClusterName)
	s.Nil(err)
	s.mockRemoteFrontendClient.EXPECT().StartWorkflowExecution(gomock.Any(), req).Return(&workflowservice.StartWorkflowExecutionResponse{}, nil).Times(1)
	err = callFn(s.alternativeClusterName)
	s.Nil(err)
}

func (s *dcRedirectionHandlerSuite) TestTerminateWorkflowExecution() {
	apiName := "TerminateWorkflowExecution"

	s.mockDCRedirectionPolicy.On("WithNamespaceRedirect",
		s.namespace, apiName, mock.Anything).Return(nil).Times(1)

	req := &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace: s.namespace,
	}
	resp, err := s.handler.TerminateWorkflowExecution(context.Background(), req)
	s.Nil(err)
	s.Nil(resp)

	callFn := s.mockDCRedirectionPolicy.Calls[0].Arguments[2].(func(string) error)
	s.mockFrontendHandler.EXPECT().TerminateWorkflowExecution(gomock.Any(), req).Return(&workflowservice.TerminateWorkflowExecutionResponse{}, nil).Times(1)
	err = callFn(s.currentClusterName)
	s.Nil(err)
	s.mockRemoteFrontendClient.EXPECT().TerminateWorkflowExecution(gomock.Any(), req).Return(&workflowservice.TerminateWorkflowExecutionResponse{}, nil).Times(1)
	err = callFn(s.alternativeClusterName)
	s.Nil(err)
}

func (s *dcRedirectionHandlerSuite) TestListTaskListPartitions() {
	apiName := "ListTaskListPartitions"

	s.mockDCRedirectionPolicy.On("WithNamespaceRedirect",
		s.namespace, apiName, mock.Anything).Return(nil).Times(1)

	req := &workflowservice.ListTaskListPartitionsRequest{
		Namespace: s.namespace,
		TaskList: &tasklistpb.TaskList{
			Name: "test_tesk_list",
			Kind: 0,
		},
	}
	resp, err := s.handler.ListTaskListPartitions(context.Background(), req)
	s.Nil(err)
	// the resp is initialized to nil, since inner function is not called
	s.Nil(resp)

	callFn := s.mockDCRedirectionPolicy.Calls[0].Arguments[2].(func(string) error)
	s.mockFrontendHandler.EXPECT().ListTaskListPartitions(gomock.Any(), req).Return(&workflowservice.ListTaskListPartitionsResponse{}, nil).Times(1)
	err = callFn(s.currentClusterName)
	s.Nil(err)
	s.mockRemoteFrontendClient.EXPECT().ListTaskListPartitions(gomock.Any(), req).Return(&workflowservice.ListTaskListPartitionsResponse{}, nil).Times(1)
	err = callFn(s.alternativeClusterName)
	s.Nil(err)
}

func (serverHandler *testServerHandler) Start() {
}

func (serverHandler *testServerHandler) Stop() {
}

func (serverHandler *testServerHandler) Check(context.Context, *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
	return nil, nil
}

func (serverHandler *testServerHandler) Watch(*healthpb.HealthCheckRequest, healthpb.Health_WatchServer) error {
	return nil
}

func (serverHandler *testServerHandler) UpdateHealthStatus(status HealthStatus) {
}
