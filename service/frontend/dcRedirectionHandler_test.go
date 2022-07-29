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

	"go.temporal.io/server/common/clock"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/api/workflowservicemock/v1"
	"google.golang.org/grpc/health"

	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/resource"
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

		namespace              namespace.Name
		namespaceID            namespace.ID
		currentClusterName     string
		alternativeClusterName string
		config                 *Config

		handler *DCRedirectionHandlerImpl
	}

	testServerHandler struct {
		*workflowservicemock.MockWorkflowServiceServer
	}
)

func newTestServerHandler(mockHandler *workflowservicemock.MockWorkflowServiceServer) Handler {
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

	s.controller = gomock.NewController(s.T())

	s.mockDCRedirectionPolicy = NewMockDCRedirectionPolicy(s.controller)
	s.mockResource = resource.NewTest(s.controller, metrics.Frontend)
	s.mockClusterMetadata = s.mockResource.ClusterMetadata
	s.mockRemoteFrontendClient = s.mockResource.RemoteFrontendClient

	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(s.currentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(true).AnyTimes()

	s.config = NewConfig(dynamicconfig.NewCollection(dynamicconfig.NewNoopClient(), s.mockResource.GetLogger()), 0, "", false)

	frontendHandlerGRPC := NewWorkflowHandler(
		s.config,
		nil,
		nil,
		s.mockResource.Logger,
		s.mockResource.GetThrottledLogger(),
		s.mockResource.GetExecutionManager(),
		s.mockResource.GetClusterMetadataManager(),
		s.mockResource.GetMetadataManager(),
		s.mockResource.GetHistoryClient(),
		s.mockResource.GetMatchingClient(),
		s.mockResource.GetArchiverProvider(),
		s.mockResource.GetPayloadSerializer(),
		s.mockResource.GetNamespaceRegistry(),
		s.mockResource.GetSearchAttributesMapper(),
		s.mockResource.GetSearchAttributesProvider(),
		s.mockResource.GetClusterMetadata(),
		s.mockResource.GetArchivalMetadata(),
		health.NewServer(),
		clock.NewRealTimeSource(),
	)

	s.mockFrontendHandler = workflowservicemock.NewMockWorkflowServiceServer(s.controller)
	s.handler = NewDCRedirectionHandler(
		frontendHandlerGRPC,
		config.DCRedirectionPolicy{},
		s.mockResource.Logger,
		s.mockResource.GetClientBean(),
		s.mockResource.GetMetricsClient(),
		s.mockResource.GetTimeSource(),
		s.mockResource.GetNamespaceRegistry(),
		s.mockResource.GetClusterMetadata(),
	)

	s.handler.frontendHandler = newTestServerHandler(s.mockFrontendHandler)
	s.handler.redirectionPolicy = s.mockDCRedirectionPolicy
}

func (s *dcRedirectionHandlerSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *dcRedirectionHandlerSuite) TestDescribeTaskQueue() {
	apiName := "DescribeTaskQueue"

	req := &workflowservice.DescribeTaskQueueRequest{
		Namespace: s.namespace.String(),
	}

	s.mockDCRedirectionPolicy.EXPECT().WithNamespaceRedirect(gomock.Any(), s.namespace, apiName, gomock.Any()).DoAndReturn(
		func(ctx context.Context, namespace namespace.Name, apiName string, callFn func(string) error) error {
			s.mockFrontendHandler.EXPECT().DescribeTaskQueue(gomock.Any(), req).Return(&workflowservice.DescribeTaskQueueResponse{}, nil)
			err := callFn(s.currentClusterName)
			s.NoError(err)
			s.mockRemoteFrontendClient.EXPECT().DescribeTaskQueue(gomock.Any(), req).Return(&workflowservice.DescribeTaskQueueResponse{}, nil)
			err = callFn(s.alternativeClusterName)
			s.NoError(err)
			return nil
		})

	resp, err := s.handler.DescribeTaskQueue(context.Background(), req)
	s.NoError(err)
	s.Equal(&workflowservice.DescribeTaskQueueResponse{}, resp)
}

func (s *dcRedirectionHandlerSuite) TestDescribeWorkflowExecution() {
	apiName := "DescribeWorkflowExecution"

	req := &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: s.namespace.String(),
	}
	s.mockDCRedirectionPolicy.EXPECT().WithNamespaceRedirect(gomock.Any(), s.namespace, apiName, gomock.Any()).DoAndReturn(
		func(ctx context.Context, namespace namespace.Name, apiName string, callFn func(string) error) error {
			s.mockFrontendHandler.EXPECT().DescribeWorkflowExecution(gomock.Any(), req).Return(&workflowservice.DescribeWorkflowExecutionResponse{}, nil)
			err := callFn(s.currentClusterName)
			s.NoError(err)
			s.mockRemoteFrontendClient.EXPECT().DescribeWorkflowExecution(gomock.Any(), req).Return(&workflowservice.DescribeWorkflowExecutionResponse{}, nil)
			err = callFn(s.alternativeClusterName)
			s.NoError(err)
			return nil
		})

	resp, err := s.handler.DescribeWorkflowExecution(context.Background(), req)
	s.NoError(err)
	s.Equal(&workflowservice.DescribeWorkflowExecutionResponse{}, resp)
}

func (s *dcRedirectionHandlerSuite) TestGetWorkflowExecutionHistory() {
	apiName := "GetWorkflowExecutionHistory"

	req := &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace: s.namespace.String(),
	}

	s.mockDCRedirectionPolicy.EXPECT().WithNamespaceRedirect(gomock.Any(), s.namespace, apiName, gomock.Any()).DoAndReturn(
		func(ctx context.Context, namespace namespace.Name, apiName string, callFn func(string) error) error {
			s.mockFrontendHandler.EXPECT().GetWorkflowExecutionHistory(gomock.Any(), req).Return(&workflowservice.GetWorkflowExecutionHistoryResponse{}, nil)
			err := callFn(s.currentClusterName)
			s.NoError(err)
			s.mockRemoteFrontendClient.EXPECT().GetWorkflowExecutionHistory(gomock.Any(), req).Return(&workflowservice.GetWorkflowExecutionHistoryResponse{}, nil)
			err = callFn(s.alternativeClusterName)
			s.NoError(err)
			return nil
		})

	resp, err := s.handler.GetWorkflowExecutionHistory(context.Background(), req)
	s.NoError(err)
	s.Equal(&workflowservice.GetWorkflowExecutionHistoryResponse{}, resp)
}

func (s *dcRedirectionHandlerSuite) TestListArchivedWorkflowExecutions() {
	apiName := "ListArchivedWorkflowExecutions"

	req := &workflowservice.ListArchivedWorkflowExecutionsRequest{
		Namespace: s.namespace.String(),
	}
	s.mockDCRedirectionPolicy.EXPECT().WithNamespaceRedirect(gomock.Any(), s.namespace, apiName, gomock.Any()).DoAndReturn(
		func(ctx context.Context, namespace namespace.Name, apiName string, callFn func(string) error) error {
			s.mockFrontendHandler.EXPECT().ListArchivedWorkflowExecutions(gomock.Any(), req).Return(&workflowservice.ListArchivedWorkflowExecutionsResponse{}, nil)
			err := callFn(s.currentClusterName)
			s.NoError(err)
			s.mockRemoteFrontendClient.EXPECT().ListArchivedWorkflowExecutions(gomock.Any(), req).Return(&workflowservice.ListArchivedWorkflowExecutionsResponse{}, nil)
			err = callFn(s.alternativeClusterName)
			s.NoError(err)
			return nil
		})

	resp, err := s.handler.ListArchivedWorkflowExecutions(context.Background(), req)
	s.NoError(err)
	s.Equal(&workflowservice.ListArchivedWorkflowExecutionsResponse{}, resp)
}

func (s *dcRedirectionHandlerSuite) TestListClosedWorkflowExecutions() {
	apiName := "ListClosedWorkflowExecutions"

	req := &workflowservice.ListClosedWorkflowExecutionsRequest{
		Namespace: s.namespace.String(),
	}

	s.mockDCRedirectionPolicy.EXPECT().WithNamespaceRedirect(gomock.Any(), s.namespace, apiName, gomock.Any()).DoAndReturn(
		func(ctx context.Context, namespace namespace.Name, apiName string, callFn func(string) error) error {
			s.mockFrontendHandler.EXPECT().ListClosedWorkflowExecutions(gomock.Any(), req).Return(&workflowservice.ListClosedWorkflowExecutionsResponse{}, nil)
			err := callFn(s.currentClusterName)
			s.NoError(err)
			s.mockRemoteFrontendClient.EXPECT().ListClosedWorkflowExecutions(gomock.Any(), req).Return(&workflowservice.ListClosedWorkflowExecutionsResponse{}, nil)
			err = callFn(s.alternativeClusterName)
			s.NoError(err)
			return nil
		})

	resp, err := s.handler.ListClosedWorkflowExecutions(context.Background(), req)
	s.NoError(err)
	s.Equal(&workflowservice.ListClosedWorkflowExecutionsResponse{}, resp)
}

func (s *dcRedirectionHandlerSuite) TestListOpenWorkflowExecutions() {
	apiName := "ListOpenWorkflowExecutions"

	req := &workflowservice.ListOpenWorkflowExecutionsRequest{
		Namespace: s.namespace.String(),
	}

	s.mockDCRedirectionPolicy.EXPECT().WithNamespaceRedirect(gomock.Any(), s.namespace, apiName, gomock.Any()).DoAndReturn(
		func(ctx context.Context, namespace namespace.Name, apiName string, callFn func(string) error) error {
			s.mockFrontendHandler.EXPECT().ListOpenWorkflowExecutions(gomock.Any(), req).Return(&workflowservice.ListOpenWorkflowExecutionsResponse{}, nil)
			err := callFn(s.currentClusterName)
			s.NoError(err)
			s.mockRemoteFrontendClient.EXPECT().ListOpenWorkflowExecutions(gomock.Any(), req).Return(&workflowservice.ListOpenWorkflowExecutionsResponse{}, nil)
			err = callFn(s.alternativeClusterName)
			s.NoError(err)
			return nil
		})

	resp, err := s.handler.ListOpenWorkflowExecutions(context.Background(), req)
	s.NoError(err)
	s.Equal(&workflowservice.ListOpenWorkflowExecutionsResponse{}, resp)
}

func (s *dcRedirectionHandlerSuite) TestListWorkflowExecutions() {
	apiName := "ListWorkflowExecutions"

	req := &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: s.namespace.String(),
	}

	s.mockDCRedirectionPolicy.EXPECT().WithNamespaceRedirect(gomock.Any(), s.namespace, apiName, gomock.Any()).DoAndReturn(
		func(ctx context.Context, namespace namespace.Name, apiName string, callFn func(string) error) error {
			s.mockFrontendHandler.EXPECT().ListWorkflowExecutions(gomock.Any(), req).Return(&workflowservice.ListWorkflowExecutionsResponse{}, nil)
			err := callFn(s.currentClusterName)
			s.NoError(err)
			s.mockRemoteFrontendClient.EXPECT().ListWorkflowExecutions(gomock.Any(), req).Return(&workflowservice.ListWorkflowExecutionsResponse{}, nil)
			err = callFn(s.alternativeClusterName)
			s.NoError(err)
			return nil
		})

	resp, err := s.handler.ListWorkflowExecutions(context.Background(), req)
	s.NoError(err)
	s.Equal(&workflowservice.ListWorkflowExecutionsResponse{}, resp)
}

func (s *dcRedirectionHandlerSuite) TestScanWorkflowExecutions() {
	apiName := "ScanWorkflowExecutions"

	req := &workflowservice.ScanWorkflowExecutionsRequest{
		Namespace: s.namespace.String(),
	}

	s.mockDCRedirectionPolicy.EXPECT().WithNamespaceRedirect(gomock.Any(), s.namespace, apiName, gomock.Any()).DoAndReturn(
		func(ctx context.Context, namespace namespace.Name, apiName string, callFn func(string) error) error {
			s.mockFrontendHandler.EXPECT().ScanWorkflowExecutions(gomock.Any(), req).Return(&workflowservice.ScanWorkflowExecutionsResponse{}, nil)
			err := callFn(s.currentClusterName)
			s.NoError(err)
			s.mockRemoteFrontendClient.EXPECT().ScanWorkflowExecutions(gomock.Any(), req).Return(&workflowservice.ScanWorkflowExecutionsResponse{}, nil)
			err = callFn(s.alternativeClusterName)
			s.NoError(err)
			return nil
		})

	resp, err := s.handler.ScanWorkflowExecutions(context.Background(), req)
	s.NoError(err)
	s.Equal(&workflowservice.ScanWorkflowExecutionsResponse{}, resp)
}

func (s *dcRedirectionHandlerSuite) TestCountWorkflowExecutions() {
	apiName := "CountWorkflowExecutions"

	req := &workflowservice.CountWorkflowExecutionsRequest{
		Namespace: s.namespace.String(),
	}

	s.mockDCRedirectionPolicy.EXPECT().WithNamespaceRedirect(gomock.Any(), s.namespace, apiName, gomock.Any()).DoAndReturn(
		func(ctx context.Context, namespace namespace.Name, apiName string, callFn func(string) error) error {
			s.mockFrontendHandler.EXPECT().CountWorkflowExecutions(gomock.Any(), req).Return(&workflowservice.CountWorkflowExecutionsResponse{}, nil)
			err := callFn(s.currentClusterName)
			s.NoError(err)
			s.mockRemoteFrontendClient.EXPECT().CountWorkflowExecutions(gomock.Any(), req).Return(&workflowservice.CountWorkflowExecutionsResponse{}, nil)
			err = callFn(s.alternativeClusterName)
			s.NoError(err)
			return nil
		})

	resp, err := s.handler.CountWorkflowExecutions(context.Background(), req)
	s.NoError(err)
	s.Equal(&workflowservice.CountWorkflowExecutionsResponse{}, resp)
}

func (s *dcRedirectionHandlerSuite) TestPollActivityTaskQueue() {
	apiName := "PollActivityTaskQueue"

	req := &workflowservice.PollActivityTaskQueueRequest{
		Namespace: s.namespace.String(),
	}

	s.mockDCRedirectionPolicy.EXPECT().WithNamespaceRedirect(gomock.Any(), s.namespace, apiName, gomock.Any()).DoAndReturn(
		func(ctx context.Context, namespace namespace.Name, apiName string, callFn func(string) error) error {
			s.mockFrontendHandler.EXPECT().PollActivityTaskQueue(gomock.Any(), req).Return(&workflowservice.PollActivityTaskQueueResponse{}, nil)
			err := callFn(s.currentClusterName)
			s.NoError(err)
			s.mockRemoteFrontendClient.EXPECT().PollActivityTaskQueue(gomock.Any(), req).Return(&workflowservice.PollActivityTaskQueueResponse{}, nil)
			err = callFn(s.alternativeClusterName)
			s.NoError(err)
			return nil
		})

	resp, err := s.handler.PollActivityTaskQueue(context.Background(), req)
	s.NoError(err)
	s.Equal(&workflowservice.PollActivityTaskQueueResponse{}, resp)
}

func (s *dcRedirectionHandlerSuite) TestPollWorkflowTaskQueue() {
	apiName := "PollWorkflowTaskQueue"

	req := &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.namespace.String(),
	}

	s.mockDCRedirectionPolicy.EXPECT().WithNamespaceRedirect(gomock.Any(), s.namespace, apiName, gomock.Any()).DoAndReturn(
		func(ctx context.Context, namespace namespace.Name, apiName string, callFn func(string) error) error {
			s.mockFrontendHandler.EXPECT().PollWorkflowTaskQueue(gomock.Any(), req).Return(&workflowservice.PollWorkflowTaskQueueResponse{}, nil)
			err := callFn(s.currentClusterName)
			s.NoError(err)
			s.mockRemoteFrontendClient.EXPECT().PollWorkflowTaskQueue(gomock.Any(), req).Return(&workflowservice.PollWorkflowTaskQueueResponse{}, nil)
			err = callFn(s.alternativeClusterName)
			s.NoError(err)
			return nil
		})

	resp, err := s.handler.PollWorkflowTaskQueue(context.Background(), req)
	s.NoError(err)
	s.Equal(&workflowservice.PollWorkflowTaskQueueResponse{}, resp)
}

func (s *dcRedirectionHandlerSuite) TestQueryWorkflow() {
	apiName := "QueryWorkflow"

	req := &workflowservice.QueryWorkflowRequest{
		Namespace: s.namespace.String(),
	}

	s.mockDCRedirectionPolicy.EXPECT().WithNamespaceRedirect(gomock.Any(), s.namespace, apiName, gomock.Any()).DoAndReturn(
		func(ctx context.Context, namespace namespace.Name, apiName string, callFn func(string) error) error {
			s.mockFrontendHandler.EXPECT().QueryWorkflow(gomock.Any(), req).Return(&workflowservice.QueryWorkflowResponse{}, nil)
			err := callFn(s.currentClusterName)
			s.NoError(err)
			s.mockRemoteFrontendClient.EXPECT().QueryWorkflow(gomock.Any(), req).Return(&workflowservice.QueryWorkflowResponse{}, nil)
			err = callFn(s.alternativeClusterName)
			s.NoError(err)
			return nil
		})

	resp, err := s.handler.QueryWorkflow(context.Background(), req)
	s.NoError(err)
	s.Equal(&workflowservice.QueryWorkflowResponse{}, resp)
}

func (s *dcRedirectionHandlerSuite) TestRecordActivityTaskHeartbeat() {
	apiName := "RecordActivityTaskHeartbeat"

	taskToken, err := s.handler.tokenSerializer.Serialize(&tokenspb.Task{
		Attempt:     1,
		NamespaceId: s.namespaceID.String(),
	})
	s.NoError(err)
	req := &workflowservice.RecordActivityTaskHeartbeatRequest{
		TaskToken: taskToken,
	}

	s.mockDCRedirectionPolicy.EXPECT().WithNamespaceIDRedirect(gomock.Any(), s.namespaceID, apiName, gomock.Any()).DoAndReturn(
		func(ctx context.Context, namespace namespace.ID, apiName string, callFn func(string) error) error {
			s.mockFrontendHandler.EXPECT().RecordActivityTaskHeartbeat(gomock.Any(), req).Return(&workflowservice.RecordActivityTaskHeartbeatResponse{}, nil)
			err := callFn(s.currentClusterName)
			s.NoError(err)
			s.mockRemoteFrontendClient.EXPECT().RecordActivityTaskHeartbeat(gomock.Any(), req).Return(&workflowservice.RecordActivityTaskHeartbeatResponse{}, nil)
			err = callFn(s.alternativeClusterName)
			s.NoError(err)
			return nil
		})

	resp, err := s.handler.RecordActivityTaskHeartbeat(context.Background(), req)
	s.NoError(err)
	s.Equal(&workflowservice.RecordActivityTaskHeartbeatResponse{}, resp)
}

func (s *dcRedirectionHandlerSuite) TestRecordActivityTaskHeartbeatById() {
	apiName := "RecordActivityTaskHeartbeatById"

	req := &workflowservice.RecordActivityTaskHeartbeatByIdRequest{
		Namespace: s.namespace.String(),
	}

	s.mockDCRedirectionPolicy.EXPECT().WithNamespaceRedirect(gomock.Any(), s.namespace, apiName, gomock.Any()).DoAndReturn(
		func(ctx context.Context, namespace namespace.Name, apiName string, callFn func(string) error) error {
			s.mockFrontendHandler.EXPECT().RecordActivityTaskHeartbeatById(gomock.Any(), req).Return(&workflowservice.RecordActivityTaskHeartbeatByIdResponse{}, nil)
			err := callFn(s.currentClusterName)
			s.NoError(err)
			s.mockRemoteFrontendClient.EXPECT().RecordActivityTaskHeartbeatById(gomock.Any(), req).Return(&workflowservice.RecordActivityTaskHeartbeatByIdResponse{}, nil)
			err = callFn(s.alternativeClusterName)
			s.NoError(err)
			return nil
		})

	resp, err := s.handler.RecordActivityTaskHeartbeatById(context.Background(), req)
	s.NoError(err)
	s.Equal(&workflowservice.RecordActivityTaskHeartbeatByIdResponse{}, resp)
}

func (s *dcRedirectionHandlerSuite) TestRequestCancelWorkflowExecution() {
	apiName := "RequestCancelWorkflowExecution"

	req := &workflowservice.RequestCancelWorkflowExecutionRequest{
		Namespace: s.namespace.String(),
	}

	s.mockDCRedirectionPolicy.EXPECT().WithNamespaceRedirect(gomock.Any(), s.namespace, apiName, gomock.Any()).DoAndReturn(
		func(ctx context.Context, namespace namespace.Name, apiName string, callFn func(string) error) error {
			s.mockFrontendHandler.EXPECT().RequestCancelWorkflowExecution(gomock.Any(), req).Return(&workflowservice.RequestCancelWorkflowExecutionResponse{}, nil)
			err := callFn(s.currentClusterName)
			s.NoError(err)
			s.mockRemoteFrontendClient.EXPECT().RequestCancelWorkflowExecution(gomock.Any(), req).Return(&workflowservice.RequestCancelWorkflowExecutionResponse{}, nil)
			err = callFn(s.alternativeClusterName)
			s.NoError(err)
			return nil
		})

	resp, err := s.handler.RequestCancelWorkflowExecution(context.Background(), req)
	s.NoError(err)
	s.Equal(&workflowservice.RequestCancelWorkflowExecutionResponse{}, resp)
}

func (s *dcRedirectionHandlerSuite) TestResetStickyTaskQueue() {
	apiName := "ResetStickyTaskQueue"

	req := &workflowservice.ResetStickyTaskQueueRequest{
		Namespace: s.namespace.String(),
	}

	s.mockDCRedirectionPolicy.EXPECT().WithNamespaceRedirect(gomock.Any(), s.namespace, apiName, gomock.Any()).DoAndReturn(
		func(ctx context.Context, namespace namespace.Name, apiName string, callFn func(string) error) error {
			s.mockFrontendHandler.EXPECT().ResetStickyTaskQueue(gomock.Any(), req).Return(&workflowservice.ResetStickyTaskQueueResponse{}, nil)
			err := callFn(s.currentClusterName)
			s.NoError(err)
			s.mockRemoteFrontendClient.EXPECT().ResetStickyTaskQueue(gomock.Any(), req).Return(&workflowservice.ResetStickyTaskQueueResponse{}, nil)
			err = callFn(s.alternativeClusterName)
			s.NoError(err)
			return nil
		})

	resp, err := s.handler.ResetStickyTaskQueue(context.Background(), req)
	s.NoError(err)
	s.Equal(&workflowservice.ResetStickyTaskQueueResponse{}, resp)
}

func (s *dcRedirectionHandlerSuite) TestResetWorkflowExecution() {
	apiName := "ResetWorkflowExecution"

	req := &workflowservice.ResetWorkflowExecutionRequest{
		Namespace: s.namespace.String(),
	}

	s.mockDCRedirectionPolicy.EXPECT().WithNamespaceRedirect(gomock.Any(), s.namespace, apiName, gomock.Any()).DoAndReturn(
		func(ctx context.Context, namespace namespace.Name, apiName string, callFn func(string) error) error {
			s.mockFrontendHandler.EXPECT().ResetWorkflowExecution(gomock.Any(), req).Return(&workflowservice.ResetWorkflowExecutionResponse{}, nil)
			err := callFn(s.currentClusterName)
			s.NoError(err)
			s.mockRemoteFrontendClient.EXPECT().ResetWorkflowExecution(gomock.Any(), req).Return(&workflowservice.ResetWorkflowExecutionResponse{}, nil)
			err = callFn(s.alternativeClusterName)
			s.NoError(err)
			return nil
		})

	resp, err := s.handler.ResetWorkflowExecution(context.Background(), req)
	s.NoError(err)
	s.Equal(&workflowservice.ResetWorkflowExecutionResponse{}, resp)
}

func (s *dcRedirectionHandlerSuite) TestRespondActivityTaskCanceled() {
	apiName := "RespondActivityTaskCanceled"

	token, err := s.handler.tokenSerializer.Serialize(&tokenspb.Task{
		Attempt:     1,
		NamespaceId: s.namespaceID.String(),
	})
	s.NoError(err)
	req := &workflowservice.RespondActivityTaskCanceledRequest{
		TaskToken: token,
	}

	s.mockDCRedirectionPolicy.EXPECT().WithNamespaceIDRedirect(gomock.Any(), s.namespaceID, apiName, gomock.Any()).DoAndReturn(
		func(ctx context.Context, namespace namespace.ID, apiName string, callFn func(string) error) error {
			s.mockFrontendHandler.EXPECT().RespondActivityTaskCanceled(gomock.Any(), req).Return(&workflowservice.RespondActivityTaskCanceledResponse{}, nil)
			err := callFn(s.currentClusterName)
			s.NoError(err)
			s.mockRemoteFrontendClient.EXPECT().RespondActivityTaskCanceled(gomock.Any(), req).Return(&workflowservice.RespondActivityTaskCanceledResponse{}, nil)
			err = callFn(s.alternativeClusterName)
			s.NoError(err)
			return nil
		})

	resp, err := s.handler.RespondActivityTaskCanceled(context.Background(), req)
	s.NoError(err)
	s.Equal(&workflowservice.RespondActivityTaskCanceledResponse{}, resp)
}

func (s *dcRedirectionHandlerSuite) TestRespondActivityTaskCanceledById() {
	apiName := "RespondActivityTaskCanceledById"

	req := &workflowservice.RespondActivityTaskCanceledByIdRequest{
		Namespace: s.namespace.String(),
	}

	s.mockDCRedirectionPolicy.EXPECT().WithNamespaceRedirect(gomock.Any(), s.namespace, apiName, gomock.Any()).DoAndReturn(
		func(ctx context.Context, namespace namespace.Name, apiName string, callFn func(string) error) error {
			s.mockFrontendHandler.EXPECT().RespondActivityTaskCanceledById(gomock.Any(), req).Return(&workflowservice.RespondActivityTaskCanceledByIdResponse{}, nil)
			err := callFn(s.currentClusterName)
			s.NoError(err)
			s.mockRemoteFrontendClient.EXPECT().RespondActivityTaskCanceledById(gomock.Any(), req).Return(&workflowservice.RespondActivityTaskCanceledByIdResponse{}, nil)
			err = callFn(s.alternativeClusterName)
			s.NoError(err)
			return nil
		})

	resp, err := s.handler.RespondActivityTaskCanceledById(context.Background(), req)
	s.NoError(err)
	s.Equal(&workflowservice.RespondActivityTaskCanceledByIdResponse{}, resp)
}

func (s *dcRedirectionHandlerSuite) TestRespondActivityTaskCompleted() {
	apiName := "RespondActivityTaskCompleted"

	taskToken, err := s.handler.tokenSerializer.Serialize(&tokenspb.Task{
		Attempt:     1,
		NamespaceId: s.namespaceID.String(),
	})
	s.NoError(err)
	req := &workflowservice.RespondActivityTaskCompletedRequest{
		TaskToken: taskToken,
	}

	s.mockDCRedirectionPolicy.EXPECT().WithNamespaceIDRedirect(gomock.Any(), s.namespaceID, apiName, gomock.Any()).DoAndReturn(
		func(ctx context.Context, namespace namespace.ID, apiName string, callFn func(string) error) error {
			s.mockFrontendHandler.EXPECT().RespondActivityTaskCompleted(gomock.Any(), req).Return(&workflowservice.RespondActivityTaskCompletedResponse{}, nil)
			err := callFn(s.currentClusterName)
			s.NoError(err)
			s.mockRemoteFrontendClient.EXPECT().RespondActivityTaskCompleted(gomock.Any(), req).Return(&workflowservice.RespondActivityTaskCompletedResponse{}, nil)
			err = callFn(s.alternativeClusterName)
			s.NoError(err)
			return nil
		})

	resp, err := s.handler.RespondActivityTaskCompleted(context.Background(), req)
	s.NoError(err)
	s.Equal(&workflowservice.RespondActivityTaskCompletedResponse{}, resp)
}

func (s *dcRedirectionHandlerSuite) TestRespondActivityTaskCompletedById() {
	apiName := "RespondActivityTaskCompletedById"

	req := &workflowservice.RespondActivityTaskCompletedByIdRequest{
		Namespace: s.namespace.String(),
	}

	s.mockDCRedirectionPolicy.EXPECT().WithNamespaceRedirect(gomock.Any(), s.namespace, apiName, gomock.Any()).DoAndReturn(
		func(ctx context.Context, namespace namespace.Name, apiName string, callFn func(string) error) error {
			s.mockFrontendHandler.EXPECT().RespondActivityTaskCompletedById(gomock.Any(), req).Return(&workflowservice.RespondActivityTaskCompletedByIdResponse{}, nil)
			err := callFn(s.currentClusterName)
			s.NoError(err)
			s.mockRemoteFrontendClient.EXPECT().RespondActivityTaskCompletedById(gomock.Any(), req).Return(&workflowservice.RespondActivityTaskCompletedByIdResponse{}, nil)
			err = callFn(s.alternativeClusterName)
			s.NoError(err)
			return nil
		})

	resp, err := s.handler.RespondActivityTaskCompletedById(context.Background(), req)
	s.NoError(err)
	s.Equal(&workflowservice.RespondActivityTaskCompletedByIdResponse{}, resp)
}

func (s *dcRedirectionHandlerSuite) TestRespondActivityTaskFailed() {
	apiName := "RespondActivityTaskFailed"

	taskToken, err := s.handler.tokenSerializer.Serialize(&tokenspb.Task{
		Attempt:     1,
		NamespaceId: s.namespaceID.String(),
	})
	s.NoError(err)
	req := &workflowservice.RespondActivityTaskFailedRequest{
		TaskToken: taskToken,
	}

	s.mockDCRedirectionPolicy.EXPECT().WithNamespaceIDRedirect(gomock.Any(), s.namespaceID, apiName, gomock.Any()).DoAndReturn(
		func(ctx context.Context, namespace namespace.ID, apiName string, callFn func(string) error) error {
			s.mockFrontendHandler.EXPECT().RespondActivityTaskFailed(gomock.Any(), req).Return(&workflowservice.RespondActivityTaskFailedResponse{}, nil)
			err := callFn(s.currentClusterName)
			s.NoError(err)
			s.mockRemoteFrontendClient.EXPECT().RespondActivityTaskFailed(gomock.Any(), req).Return(&workflowservice.RespondActivityTaskFailedResponse{}, nil)
			err = callFn(s.alternativeClusterName)
			s.NoError(err)
			return nil
		})

	resp, err := s.handler.RespondActivityTaskFailed(context.Background(), req)
	s.NoError(err)
	s.Equal(&workflowservice.RespondActivityTaskFailedResponse{}, resp)
}

func (s *dcRedirectionHandlerSuite) TestRespondActivityTaskFailedById() {
	apiName := "RespondActivityTaskFailedById"

	req := &workflowservice.RespondActivityTaskFailedByIdRequest{
		Namespace: s.namespace.String(),
	}

	s.mockDCRedirectionPolicy.EXPECT().WithNamespaceRedirect(gomock.Any(), s.namespace, apiName, gomock.Any()).DoAndReturn(
		func(ctx context.Context, namespace namespace.Name, apiName string, callFn func(string) error) error {
			s.mockFrontendHandler.EXPECT().RespondActivityTaskFailedById(gomock.Any(), req).Return(&workflowservice.RespondActivityTaskFailedByIdResponse{}, nil)
			err := callFn(s.currentClusterName)
			s.NoError(err)
			s.mockRemoteFrontendClient.EXPECT().RespondActivityTaskFailedById(gomock.Any(), req).Return(&workflowservice.RespondActivityTaskFailedByIdResponse{}, nil)
			err = callFn(s.alternativeClusterName)
			s.NoError(err)
			return nil
		})

	resp, err := s.handler.RespondActivityTaskFailedById(context.Background(), req)
	s.NoError(err)
	s.Equal(&workflowservice.RespondActivityTaskFailedByIdResponse{}, resp)
}

func (s *dcRedirectionHandlerSuite) TestRespondWorkflowTaskCompleted() {
	apiName := "RespondWorkflowTaskCompleted"

	taskToken, err := s.handler.tokenSerializer.Serialize(&tokenspb.Task{
		Attempt:     1,
		NamespaceId: s.namespaceID.String(),
	})
	s.NoError(err)
	req := &workflowservice.RespondWorkflowTaskCompletedRequest{
		TaskToken: taskToken,
	}

	s.mockDCRedirectionPolicy.EXPECT().WithNamespaceIDRedirect(gomock.Any(), s.namespaceID, apiName, gomock.Any()).DoAndReturn(
		func(ctx context.Context, namespace namespace.ID, apiName string, callFn func(string) error) error {
			s.mockFrontendHandler.EXPECT().RespondWorkflowTaskCompleted(gomock.Any(), req).Return(&workflowservice.RespondWorkflowTaskCompletedResponse{}, nil)
			err := callFn(s.currentClusterName)
			s.NoError(err)
			s.mockRemoteFrontendClient.EXPECT().RespondWorkflowTaskCompleted(gomock.Any(), req).Return(&workflowservice.RespondWorkflowTaskCompletedResponse{}, nil)
			err = callFn(s.alternativeClusterName)
			s.NoError(err)
			return nil
		})

	resp, err := s.handler.RespondWorkflowTaskCompleted(context.Background(), req)
	s.NoError(err)
	s.Equal(&workflowservice.RespondWorkflowTaskCompletedResponse{}, resp)
}

func (s *dcRedirectionHandlerSuite) TestRespondWorkflowTaskFailed() {
	apiName := "RespondWorkflowTaskFailed"

	token, err := s.handler.tokenSerializer.Serialize(&tokenspb.Task{
		Attempt:     1,
		NamespaceId: s.namespaceID.String(),
	})
	s.NoError(err)
	req := &workflowservice.RespondWorkflowTaskFailedRequest{
		TaskToken: token,
	}

	s.mockDCRedirectionPolicy.EXPECT().WithNamespaceIDRedirect(gomock.Any(), s.namespaceID, apiName, gomock.Any()).DoAndReturn(
		func(ctx context.Context, namespace namespace.ID, apiName string, callFn func(string) error) error {
			s.mockFrontendHandler.EXPECT().RespondWorkflowTaskFailed(gomock.Any(), req).Return(&workflowservice.RespondWorkflowTaskFailedResponse{}, nil)
			err := callFn(s.currentClusterName)
			s.NoError(err)
			s.mockRemoteFrontendClient.EXPECT().RespondWorkflowTaskFailed(gomock.Any(), req).Return(&workflowservice.RespondWorkflowTaskFailedResponse{}, nil)
			err = callFn(s.alternativeClusterName)
			s.NoError(err)
			return nil
		})

	resp, err := s.handler.RespondWorkflowTaskFailed(context.Background(), req)
	s.NoError(err)
	s.Equal(&workflowservice.RespondWorkflowTaskFailedResponse{}, resp)
}

func (s *dcRedirectionHandlerSuite) TestRespondQueryTaskCompleted() {
	apiName := "RespondQueryTaskCompleted"

	taskToken, err := s.handler.tokenSerializer.SerializeQueryTaskToken(&tokenspb.QueryTask{
		NamespaceId: s.namespaceID.String(),
	})
	s.NoError(err)
	req := &workflowservice.RespondQueryTaskCompletedRequest{
		TaskToken: taskToken,
	}

	s.mockDCRedirectionPolicy.EXPECT().WithNamespaceIDRedirect(gomock.Any(), s.namespaceID, apiName, gomock.Any()).DoAndReturn(
		func(ctx context.Context, namespace namespace.ID, apiName string, callFn func(string) error) error {
			s.mockFrontendHandler.EXPECT().RespondQueryTaskCompleted(gomock.Any(), req).Return(&workflowservice.RespondQueryTaskCompletedResponse{}, nil)
			err := callFn(s.currentClusterName)
			s.NoError(err)
			s.mockRemoteFrontendClient.EXPECT().RespondQueryTaskCompleted(gomock.Any(), req).Return(&workflowservice.RespondQueryTaskCompletedResponse{}, nil)
			err = callFn(s.alternativeClusterName)
			s.NoError(err)
			return nil
		})

	resp, err := s.handler.RespondQueryTaskCompleted(context.Background(), req)
	s.NoError(err)
	s.Equal(&workflowservice.RespondQueryTaskCompletedResponse{}, resp)
}

func (s *dcRedirectionHandlerSuite) TestSignalWithStartWorkflowExecution() {
	apiName := "SignalWithStartWorkflowExecution"

	req := &workflowservice.SignalWithStartWorkflowExecutionRequest{
		Namespace: s.namespace.String(),
	}

	s.mockDCRedirectionPolicy.EXPECT().WithNamespaceRedirect(gomock.Any(), s.namespace, apiName, gomock.Any()).DoAndReturn(
		func(ctx context.Context, namespace namespace.Name, apiName string, callFn func(string) error) error {
			s.mockFrontendHandler.EXPECT().SignalWithStartWorkflowExecution(gomock.Any(), req).Return(&workflowservice.SignalWithStartWorkflowExecutionResponse{}, nil)
			err := callFn(s.currentClusterName)
			s.NoError(err)
			s.mockRemoteFrontendClient.EXPECT().SignalWithStartWorkflowExecution(gomock.Any(), req).Return(&workflowservice.SignalWithStartWorkflowExecutionResponse{}, nil)
			err = callFn(s.alternativeClusterName)
			s.NoError(err)
			return nil
		})

	resp, err := s.handler.SignalWithStartWorkflowExecution(context.Background(), req)
	s.NoError(err)
	s.Equal(&workflowservice.SignalWithStartWorkflowExecutionResponse{}, resp)
}

func (s *dcRedirectionHandlerSuite) TestSignalWorkflowExecution() {
	apiName := "SignalWorkflowExecution"

	req := &workflowservice.SignalWorkflowExecutionRequest{
		Namespace: s.namespace.String(),
	}

	s.mockDCRedirectionPolicy.EXPECT().WithNamespaceRedirect(gomock.Any(), s.namespace, apiName, gomock.Any()).DoAndReturn(
		func(ctx context.Context, namespace namespace.Name, apiName string, callFn func(string) error) error {
			s.mockFrontendHandler.EXPECT().SignalWorkflowExecution(gomock.Any(), req).Return(&workflowservice.SignalWorkflowExecutionResponse{}, nil)
			err := callFn(s.currentClusterName)
			s.NoError(err)
			s.mockRemoteFrontendClient.EXPECT().SignalWorkflowExecution(gomock.Any(), req).Return(&workflowservice.SignalWorkflowExecutionResponse{}, nil)
			err = callFn(s.alternativeClusterName)
			s.NoError(err)
			return nil
		})

	resp, err := s.handler.SignalWorkflowExecution(context.Background(), req)
	s.NoError(err)
	s.Equal(&workflowservice.SignalWorkflowExecutionResponse{}, resp)
}

func (s *dcRedirectionHandlerSuite) TestStartWorkflowExecution() {
	apiName := "StartWorkflowExecution"

	req := &workflowservice.StartWorkflowExecutionRequest{
		Namespace: s.namespace.String(),
	}

	s.mockDCRedirectionPolicy.EXPECT().WithNamespaceRedirect(gomock.Any(), s.namespace, apiName, gomock.Any()).DoAndReturn(
		func(ctx context.Context, namespace namespace.Name, apiName string, callFn func(string) error) error {
			s.mockFrontendHandler.EXPECT().StartWorkflowExecution(gomock.Any(), req).Return(&workflowservice.StartWorkflowExecutionResponse{}, nil)
			err := callFn(s.currentClusterName)
			s.NoError(err)
			s.mockRemoteFrontendClient.EXPECT().StartWorkflowExecution(gomock.Any(), req).Return(&workflowservice.StartWorkflowExecutionResponse{}, nil)
			err = callFn(s.alternativeClusterName)
			s.NoError(err)
			return nil
		})

	resp, err := s.handler.StartWorkflowExecution(context.Background(), req)
	s.NoError(err)
	s.Equal(&workflowservice.StartWorkflowExecutionResponse{}, resp)
}

func (s *dcRedirectionHandlerSuite) TestTerminateWorkflowExecution() {
	apiName := "TerminateWorkflowExecution"

	req := &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace: s.namespace.String(),
	}

	s.mockDCRedirectionPolicy.EXPECT().WithNamespaceRedirect(gomock.Any(), s.namespace, apiName, gomock.Any()).DoAndReturn(
		func(ctx context.Context, namespace namespace.Name, apiName string, callFn func(string) error) error {
			s.mockFrontendHandler.EXPECT().TerminateWorkflowExecution(gomock.Any(), req).Return(&workflowservice.TerminateWorkflowExecutionResponse{}, nil)
			err := callFn(s.currentClusterName)
			s.NoError(err)
			s.mockRemoteFrontendClient.EXPECT().TerminateWorkflowExecution(gomock.Any(), req).Return(&workflowservice.TerminateWorkflowExecutionResponse{}, nil)
			err = callFn(s.alternativeClusterName)
			s.NoError(err)
			return nil
		})

	resp, err := s.handler.TerminateWorkflowExecution(context.Background(), req)
	s.NoError(err)
	s.Equal(&workflowservice.TerminateWorkflowExecutionResponse{}, resp)
}

func (s *dcRedirectionHandlerSuite) TestListTaskQueuePartitions() {
	apiName := "ListTaskQueuePartitions"

	req := &workflowservice.ListTaskQueuePartitionsRequest{
		Namespace: s.namespace.String(),
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: "test_tesk_list",
			Kind: 0,
		},
	}

	s.mockDCRedirectionPolicy.EXPECT().WithNamespaceRedirect(gomock.Any(), s.namespace, apiName, gomock.Any()).DoAndReturn(
		func(ctx context.Context, namespace namespace.Name, apiName string, callFn func(string) error) error {
			s.mockFrontendHandler.EXPECT().ListTaskQueuePartitions(gomock.Any(), req).Return(&workflowservice.ListTaskQueuePartitionsResponse{}, nil)
			err := callFn(s.currentClusterName)
			s.NoError(err)
			s.mockRemoteFrontendClient.EXPECT().ListTaskQueuePartitions(gomock.Any(), req).Return(&workflowservice.ListTaskQueuePartitionsResponse{}, nil)
			err = callFn(s.alternativeClusterName)
			s.NoError(err)
			return nil
		})

	resp, err := s.handler.ListTaskQueuePartitions(context.Background(), req)
	s.NoError(err)
	s.Equal(&workflowservice.ListTaskQueuePartitionsResponse{}, resp)
}

func (serverHandler *testServerHandler) Start() {
}

func (serverHandler *testServerHandler) Stop() {
}

func (serverHandler *testServerHandler) GetConfig() *Config {
	return nil
}
