// Copyright (c) 2017 Uber Technologies, Inc.
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

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/client"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/archiver/provider"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/loggerimpl"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/mocks"
	"github.com/uber/cadence/common/service"
	"github.com/uber/cadence/common/service/config"
	"github.com/uber/cadence/common/service/dynamicconfig"
)

type (
	dcRedirectionHandlerSuite struct {
		suite.Suite
		logger                 log.Logger
		domainName             string
		domainID               string
		currentClusterName     string
		alternativeClusterName string
		config                 *Config
		service                service.Service
		domainCache            cache.DomainCache

		mockDCRedirectionPolicy  *MockDCRedirectionPolicy
		mockClusterMetadata      *mocks.ClusterMetadata
		mockMetadataMgr          *mocks.MetadataManager
		mockClientBean           *client.MockClientBean
		mockFrontendHandler      *MockWorkflowHandler
		mockRemoteFrontendClient *mocks.FrontendClient
		mockArchiverProvider     *provider.ArchiverProviderMock

		frontendHandler *WorkflowHandler
		handler         *DCRedirectionHandlerImpl
	}
)

func TestDCRedirectionHandlerSuite(t *testing.T) {
	s := new(dcRedirectionHandlerSuite)
	suite.Run(t, s)
}

func (s *dcRedirectionHandlerSuite) SetupSuite() {
}

func (s *dcRedirectionHandlerSuite) TearDownSuite() {

}

func (s *dcRedirectionHandlerSuite) SetupTest() {
	var err error
	s.logger, err = loggerimpl.NewDevelopment()
	s.Require().NoError(err)
	s.domainName = "some random domain name"
	s.domainID = "some random domain ID"
	s.currentClusterName = cluster.TestCurrentClusterName
	s.alternativeClusterName = cluster.TestAlternativeClusterName
	s.config = NewConfig(dynamicconfig.NewCollection(dynamicconfig.NewNopClient(), s.logger), 0, false)
	s.mockMetadataMgr = &mocks.MetadataManager{}

	s.mockClusterMetadata = &mocks.ClusterMetadata{}
	s.mockClusterMetadata.On("GetCurrentClusterName").Return(s.currentClusterName)
	s.mockClusterMetadata.On("IsGlobalDomainEnabled").Return(true)
	metricsClient := metrics.NewClient(tally.NoopScope, metrics.Frontend)
	s.mockClientBean = &client.MockClientBean{}
	s.mockRemoteFrontendClient = &mocks.FrontendClient{}
	s.mockClientBean.On("GetRemoteFrontendClient", s.alternativeClusterName).Return(s.mockRemoteFrontendClient)
	s.service = service.NewTestService(s.mockClusterMetadata, nil, metricsClient, s.mockClientBean)

	s.mockArchiverProvider = &provider.ArchiverProviderMock{}
	s.mockArchiverProvider.On("RegisterBootstrapContainer", common.FrontendServiceName, mock.Anything, mock.Anything)

	frontendHandler := NewWorkflowHandler(s.service, s.config, s.mockMetadataMgr, nil, nil, nil, nil, nil, s.mockArchiverProvider)
	frontendHandler.metricsClient = metricsClient
	frontendHandler.startWG.Done()

	s.handler = NewDCRedirectionHandler(frontendHandler, config.DCRedirectionPolicy{})
	s.mockDCRedirectionPolicy = &MockDCRedirectionPolicy{}
	s.mockFrontendHandler = &MockWorkflowHandler{}
	s.handler.frontendHandler = s.mockFrontendHandler
	s.handler.redirectionPolicy = s.mockDCRedirectionPolicy

}

func (s *dcRedirectionHandlerSuite) TearDownTest() {
	s.mockMetadataMgr.AssertExpectations(s.T())
	s.mockDCRedirectionPolicy.AssertExpectations(s.T())
	s.mockFrontendHandler.AssertExpectations(s.T())
	s.mockRemoteFrontendClient.AssertExpectations(s.T())
}

func (s *dcRedirectionHandlerSuite) TestDescribeTaskList() {
	apiName := "DescribeTaskList"

	s.mockDCRedirectionPolicy.On("WithDomainNameRedirect",
		s.domainName, apiName, mock.Anything).Return(nil).Once()

	req := &shared.DescribeTaskListRequest{
		Domain: common.StringPtr(s.domainName),
	}
	resp, err := s.handler.DescribeTaskList(context.Background(), req)
	s.Nil(err)
	// the resp is initialized to nil, since inner function is not called
	s.Nil(resp)

	callFn := s.mockDCRedirectionPolicy.Calls[0].Arguments[2].(func(string) error)
	s.mockFrontendHandler.On(apiName, mock.Anything, req).Return(&shared.DescribeTaskListResponse{}, nil).Once()
	err = callFn(s.currentClusterName)
	s.Nil(err)
	s.mockRemoteFrontendClient.On(apiName, mock.Anything, req).Return(&shared.DescribeTaskListResponse{}, nil).Once()
	err = callFn(s.alternativeClusterName)
	s.Nil(err)
}

func (s *dcRedirectionHandlerSuite) TestDescribeWorkflowExecution() {
	apiName := "DescribeWorkflowExecution"

	s.mockDCRedirectionPolicy.On("WithDomainNameRedirect",
		s.domainName, apiName, mock.Anything).Return(nil).Once()

	req := &shared.DescribeWorkflowExecutionRequest{
		Domain: common.StringPtr(s.domainName),
	}
	resp, err := s.handler.DescribeWorkflowExecution(context.Background(), req)
	s.Nil(err)
	// the resp is initialized to nil, since inner function is not called
	s.Nil(resp)

	callFn := s.mockDCRedirectionPolicy.Calls[0].Arguments[2].(func(string) error)
	s.mockFrontendHandler.On(apiName, mock.Anything, req).Return(&shared.DescribeWorkflowExecutionResponse{}, nil).Once()
	err = callFn(s.currentClusterName)
	s.Nil(err)
	s.mockRemoteFrontendClient.On(apiName, mock.Anything, req).Return(&shared.DescribeWorkflowExecutionResponse{}, nil).Once()
	err = callFn(s.alternativeClusterName)
	s.Nil(err)
}

func (s *dcRedirectionHandlerSuite) TestGetWorkflowExecutionHistory() {
	apiName := "GetWorkflowExecutionHistory"

	s.mockDCRedirectionPolicy.On("WithDomainNameRedirect",
		s.domainName, apiName, mock.Anything).Return(nil).Once()

	req := &shared.GetWorkflowExecutionHistoryRequest{
		Domain: common.StringPtr(s.domainName),
	}
	resp, err := s.handler.GetWorkflowExecutionHistory(context.Background(), req)
	s.Nil(err)
	// the resp is initialized to nil, since inner function is not called
	s.Nil(resp)

	callFn := s.mockDCRedirectionPolicy.Calls[0].Arguments[2].(func(string) error)
	s.mockFrontendHandler.On(apiName, mock.Anything, req).Return(&shared.GetWorkflowExecutionHistoryResponse{}, nil).Once()
	err = callFn(s.currentClusterName)
	s.Nil(err)
	s.mockRemoteFrontendClient.On(apiName, mock.Anything, req).Return(&shared.GetWorkflowExecutionHistoryResponse{}, nil).Once()
	err = callFn(s.alternativeClusterName)
	s.Nil(err)
}

func (s *dcRedirectionHandlerSuite) TestListClosedWorkflowExecutions() {
	apiName := "ListClosedWorkflowExecutions"

	s.mockDCRedirectionPolicy.On("WithDomainNameRedirect",
		s.domainName, apiName, mock.Anything).Return(nil).Once()

	req := &shared.ListClosedWorkflowExecutionsRequest{
		Domain: common.StringPtr(s.domainName),
	}
	resp, err := s.handler.ListClosedWorkflowExecutions(context.Background(), req)
	s.Nil(err)
	// the resp is initialized to nil, since inner function is not called
	s.Nil(resp)

	callFn := s.mockDCRedirectionPolicy.Calls[0].Arguments[2].(func(string) error)
	s.mockFrontendHandler.On(apiName, mock.Anything, req).Return(&shared.ListClosedWorkflowExecutionsResponse{}, nil).Once()
	err = callFn(s.currentClusterName)
	s.Nil(err)
	s.mockRemoteFrontendClient.On(apiName, mock.Anything, req).Return(&shared.ListClosedWorkflowExecutionsResponse{}, nil).Once()
	err = callFn(s.alternativeClusterName)
	s.Nil(err)
}

func (s *dcRedirectionHandlerSuite) TestListOpenWorkflowExecutions() {
	apiName := "ListOpenWorkflowExecutions"

	s.mockDCRedirectionPolicy.On("WithDomainNameRedirect",
		s.domainName, apiName, mock.Anything).Return(nil).Once()

	req := &shared.ListOpenWorkflowExecutionsRequest{
		Domain: common.StringPtr(s.domainName),
	}
	resp, err := s.handler.ListOpenWorkflowExecutions(context.Background(), req)
	s.Nil(err)
	// the resp is initialized to nil, since inner function is not called
	s.Nil(resp)

	callFn := s.mockDCRedirectionPolicy.Calls[0].Arguments[2].(func(string) error)
	s.mockFrontendHandler.On(apiName, mock.Anything, req).Return(&shared.ListOpenWorkflowExecutionsResponse{}, nil).Once()
	err = callFn(s.currentClusterName)
	s.Nil(err)
	s.mockRemoteFrontendClient.On(apiName, mock.Anything, req).Return(&shared.ListOpenWorkflowExecutionsResponse{}, nil).Once()
	err = callFn(s.alternativeClusterName)
	s.Nil(err)
}

func (s *dcRedirectionHandlerSuite) TestListWorkflowExecutions() {
	apiName := "ListWorkflowExecutions"

	s.mockDCRedirectionPolicy.On("WithDomainNameRedirect",
		s.domainName, apiName, mock.Anything).Return(nil).Once()

	req := &shared.ListWorkflowExecutionsRequest{
		Domain: common.StringPtr(s.domainName),
	}
	resp, err := s.handler.ListWorkflowExecutions(context.Background(), req)
	s.Nil(err)
	// the resp is initialized to nil, since inner function is not called
	s.Nil(resp)

	callFn := s.mockDCRedirectionPolicy.Calls[0].Arguments[2].(func(string) error)
	s.mockFrontendHandler.On(apiName, mock.Anything, req).Return(&shared.ListWorkflowExecutionsResponse{}, nil).Once()
	err = callFn(s.currentClusterName)
	s.Nil(err)
	s.mockRemoteFrontendClient.On(apiName, mock.Anything, req).Return(&shared.ListWorkflowExecutionsResponse{}, nil).Once()
	err = callFn(s.alternativeClusterName)
	s.Nil(err)
}

func (s *dcRedirectionHandlerSuite) TestScanWorkflowExecutions() {
	apiName := "ScanWorkflowExecutions"

	s.mockDCRedirectionPolicy.On("WithDomainNameRedirect",
		s.domainName, apiName, mock.Anything).Return(nil).Once()

	req := &shared.ListWorkflowExecutionsRequest{
		Domain: common.StringPtr(s.domainName),
	}
	resp, err := s.handler.ScanWorkflowExecutions(context.Background(), req)
	s.Nil(err)
	// the resp is initialized to nil, since inner function is not called
	s.Nil(resp)

	callFn := s.mockDCRedirectionPolicy.Calls[0].Arguments[2].(func(string) error)
	s.mockFrontendHandler.On(apiName, mock.Anything, req).Return(&shared.ListWorkflowExecutionsResponse{}, nil).Once()
	err = callFn(s.currentClusterName)
	s.Nil(err)
	s.mockRemoteFrontendClient.On(apiName, mock.Anything, req).Return(&shared.ListWorkflowExecutionsResponse{}, nil).Once()
	err = callFn(s.alternativeClusterName)
	s.Nil(err)
}

func (s *dcRedirectionHandlerSuite) TestCountWorkflowExecutions() {
	apiName := "CountWorkflowExecutions"

	s.mockDCRedirectionPolicy.On("WithDomainNameRedirect",
		s.domainName, apiName, mock.Anything).Return(nil).Once()

	req := &shared.CountWorkflowExecutionsRequest{
		Domain: common.StringPtr(s.domainName),
	}
	resp, err := s.handler.CountWorkflowExecutions(context.Background(), req)
	s.Nil(err)
	// the resp is initialized to nil, since inner function is not called
	s.Nil(resp)

	callFn := s.mockDCRedirectionPolicy.Calls[0].Arguments[2].(func(string) error)
	s.mockFrontendHandler.On(apiName, mock.Anything, req).Return(&shared.CountWorkflowExecutionsResponse{}, nil).Once()
	err = callFn(s.currentClusterName)
	s.Nil(err)
	s.mockRemoteFrontendClient.On(apiName, mock.Anything, req).Return(&shared.CountWorkflowExecutionsResponse{}, nil).Once()
	err = callFn(s.alternativeClusterName)
	s.Nil(err)
}

func (s *dcRedirectionHandlerSuite) TestPollForActivityTask() {
	apiName := "PollForActivityTask"

	s.mockDCRedirectionPolicy.On("WithDomainNameRedirect",
		s.domainName, apiName, mock.Anything).Return(nil).Once()

	req := &shared.PollForActivityTaskRequest{
		Domain: common.StringPtr(s.domainName),
	}
	resp, err := s.handler.PollForActivityTask(context.Background(), req)
	s.Nil(err)
	// the resp is initialized to nil, since inner function is not called
	s.Nil(resp)

	callFn := s.mockDCRedirectionPolicy.Calls[0].Arguments[2].(func(string) error)
	s.mockFrontendHandler.On(apiName, mock.Anything, req).Return(&shared.PollForActivityTaskResponse{}, nil).Once()
	err = callFn(s.currentClusterName)
	s.Nil(err)
	s.mockRemoteFrontendClient.On(apiName, mock.Anything, req).Return(&shared.PollForActivityTaskResponse{}, nil).Once()
	err = callFn(s.alternativeClusterName)
	s.Nil(err)
}

func (s *dcRedirectionHandlerSuite) TestPollForDecisionTask() {
	apiName := "PollForDecisionTask"

	s.mockDCRedirectionPolicy.On("WithDomainNameRedirect",
		s.domainName, apiName, mock.Anything).Return(nil).Once()

	req := &shared.PollForDecisionTaskRequest{
		Domain: common.StringPtr(s.domainName),
	}
	resp, err := s.handler.PollForDecisionTask(context.Background(), req)
	s.Nil(err)
	// the resp is initialized to nil, since inner function is not called
	s.Nil(resp)

	callFn := s.mockDCRedirectionPolicy.Calls[0].Arguments[2].(func(string) error)
	s.mockFrontendHandler.On(apiName, mock.Anything, req).Return(&shared.PollForDecisionTaskResponse{}, nil).Once()
	err = callFn(s.currentClusterName)
	s.Nil(err)
	s.mockRemoteFrontendClient.On(apiName, mock.Anything, req).Return(&shared.PollForDecisionTaskResponse{}, nil).Once()
	err = callFn(s.alternativeClusterName)
	s.Nil(err)
}

func (s *dcRedirectionHandlerSuite) TestQueryWorkflow() {
	apiName := "QueryWorkflow"

	s.mockDCRedirectionPolicy.On("WithDomainNameRedirect",
		s.domainName, apiName, mock.Anything).Return(nil).Once()

	req := &shared.QueryWorkflowRequest{
		Domain: common.StringPtr(s.domainName),
	}
	resp, err := s.handler.QueryWorkflow(context.Background(), req)
	s.Nil(err)
	// the resp is initialized to nil, since inner function is not called
	s.Nil(resp)

	callFn := s.mockDCRedirectionPolicy.Calls[0].Arguments[2].(func(string) error)
	s.mockFrontendHandler.On(apiName, mock.Anything, req).Return(&shared.QueryWorkflowResponse{}, nil).Once()
	err = callFn(s.currentClusterName)
	s.Nil(err)
	s.mockRemoteFrontendClient.On(apiName, mock.Anything, req).Return(&shared.QueryWorkflowResponse{}, nil).Once()
	err = callFn(s.alternativeClusterName)
	s.Nil(err)
}

func (s *dcRedirectionHandlerSuite) TestRecordActivityTaskHeartbeat() {
	apiName := "RecordActivityTaskHeartbeat"

	s.mockDCRedirectionPolicy.On("WithDomainIDRedirect",
		s.domainID, apiName, mock.Anything).Return(nil).Once()

	token, err := s.handler.tokenSerializer.Serialize(&common.TaskToken{
		DomainID: s.domainID,
	})
	s.Nil(err)
	req := &shared.RecordActivityTaskHeartbeatRequest{
		TaskToken: token,
	}
	resp, err := s.handler.RecordActivityTaskHeartbeat(context.Background(), req)
	s.Nil(err)
	// the resp is initialized to nil, since inner function is not called
	s.Nil(resp)

	callFn := s.mockDCRedirectionPolicy.Calls[0].Arguments[2].(func(string) error)
	s.mockFrontendHandler.On(apiName, mock.Anything, req).Return(&shared.RecordActivityTaskHeartbeatResponse{}, nil).Once()
	err = callFn(s.currentClusterName)
	s.Nil(err)
	s.mockRemoteFrontendClient.On(apiName, mock.Anything, req).Return(&shared.RecordActivityTaskHeartbeatResponse{}, nil).Once()
	err = callFn(s.alternativeClusterName)
	s.Nil(err)
}

func (s *dcRedirectionHandlerSuite) TestRecordActivityTaskHeartbeatByID() {
	apiName := "RecordActivityTaskHeartbeatByID"

	s.mockDCRedirectionPolicy.On("WithDomainNameRedirect",
		s.domainName, apiName, mock.Anything).Return(nil).Once()

	req := &shared.RecordActivityTaskHeartbeatByIDRequest{
		Domain: common.StringPtr(s.domainName),
	}
	resp, err := s.handler.RecordActivityTaskHeartbeatByID(context.Background(), req)
	s.Nil(err)
	// the resp is initialized to nil, since inner function is not called
	s.Nil(resp)

	callFn := s.mockDCRedirectionPolicy.Calls[0].Arguments[2].(func(string) error)
	s.mockFrontendHandler.On(apiName, mock.Anything, req).Return(&shared.RecordActivityTaskHeartbeatResponse{}, nil).Once()
	err = callFn(s.currentClusterName)
	s.Nil(err)
	s.mockRemoteFrontendClient.On(apiName, mock.Anything, req).Return(&shared.RecordActivityTaskHeartbeatResponse{}, nil).Once()
	err = callFn(s.alternativeClusterName)
	s.Nil(err)
}

func (s *dcRedirectionHandlerSuite) TestRequestCancelWorkflowExecution() {
	apiName := "RequestCancelWorkflowExecution"

	s.mockDCRedirectionPolicy.On("WithDomainNameRedirect",
		s.domainName, apiName, mock.Anything).Return(nil).Once()

	req := &shared.RequestCancelWorkflowExecutionRequest{
		Domain: common.StringPtr(s.domainName),
	}
	err := s.handler.RequestCancelWorkflowExecution(context.Background(), req)
	s.Nil(err)

	callFn := s.mockDCRedirectionPolicy.Calls[0].Arguments[2].(func(string) error)
	s.mockFrontendHandler.On(apiName, mock.Anything, req).Return(nil).Once()
	err = callFn(s.currentClusterName)
	s.Nil(err)
	s.mockRemoteFrontendClient.On(apiName, mock.Anything, req).Return(nil).Once()
	err = callFn(s.alternativeClusterName)
	s.Nil(err)
}

func (s *dcRedirectionHandlerSuite) TestResetStickyTaskList() {
	apiName := "ResetStickyTaskList"

	s.mockDCRedirectionPolicy.On("WithDomainNameRedirect",
		s.domainName, apiName, mock.Anything).Return(nil).Once()

	req := &shared.ResetStickyTaskListRequest{
		Domain: common.StringPtr(s.domainName),
	}
	resp, err := s.handler.ResetStickyTaskList(context.Background(), req)
	s.Nil(err)
	// the resp is initialized to nil, since inner function is not called
	s.Nil(resp)

	callFn := s.mockDCRedirectionPolicy.Calls[0].Arguments[2].(func(string) error)
	s.mockFrontendHandler.On(apiName, mock.Anything, req).Return(&shared.ResetStickyTaskListResponse{}, nil).Once()
	err = callFn(s.currentClusterName)
	s.Nil(err)
	s.mockRemoteFrontendClient.On(apiName, mock.Anything, req).Return(&shared.ResetStickyTaskListResponse{}, nil).Once()
	err = callFn(s.alternativeClusterName)
	s.Nil(err)
}

func (s *dcRedirectionHandlerSuite) TestResetWorkflowExecution() {
	apiName := "ResetWorkflowExecution"

	s.mockDCRedirectionPolicy.On("WithDomainNameRedirect",
		s.domainName, apiName, mock.Anything).Return(nil).Once()

	req := &shared.ResetWorkflowExecutionRequest{
		Domain: common.StringPtr(s.domainName),
	}
	resp, err := s.handler.ResetWorkflowExecution(context.Background(), req)
	s.Nil(err)
	// the resp is initialized to nil, since inner function is not called
	s.Nil(resp)

	callFn := s.mockDCRedirectionPolicy.Calls[0].Arguments[2].(func(string) error)
	s.mockFrontendHandler.On(apiName, mock.Anything, req).Return(&shared.ResetWorkflowExecutionResponse{}, nil).Once()
	err = callFn(s.currentClusterName)
	s.Nil(err)
	s.mockRemoteFrontendClient.On(apiName, mock.Anything, req).Return(&shared.ResetWorkflowExecutionResponse{}, nil).Once()
	err = callFn(s.alternativeClusterName)
	s.Nil(err)
}

func (s *dcRedirectionHandlerSuite) TestRespondActivityTaskCanceled() {
	apiName := "RespondActivityTaskCanceled"

	s.mockDCRedirectionPolicy.On("WithDomainIDRedirect",
		s.domainID, apiName, mock.Anything).Return(nil).Once()

	token, err := s.handler.tokenSerializer.Serialize(&common.TaskToken{
		DomainID: s.domainID,
	})
	s.Nil(err)
	req := &shared.RespondActivityTaskCanceledRequest{
		TaskToken: token,
	}
	err = s.handler.RespondActivityTaskCanceled(context.Background(), req)
	s.Nil(err)

	callFn := s.mockDCRedirectionPolicy.Calls[0].Arguments[2].(func(string) error)
	s.mockFrontendHandler.On(apiName, mock.Anything, req).Return(nil).Once()
	err = callFn(s.currentClusterName)
	s.Nil(err)
	s.mockRemoteFrontendClient.On(apiName, mock.Anything, req).Return(nil).Once()
	err = callFn(s.alternativeClusterName)
	s.Nil(err)
}

func (s *dcRedirectionHandlerSuite) TestRespondActivityTaskCanceledByID() {
	apiName := "RespondActivityTaskCanceledByID"

	s.mockDCRedirectionPolicy.On("WithDomainNameRedirect",
		s.domainName, apiName, mock.Anything).Return(nil).Once()

	req := &shared.RespondActivityTaskCanceledByIDRequest{
		Domain: common.StringPtr(s.domainName),
	}
	err := s.handler.RespondActivityTaskCanceledByID(context.Background(), req)
	s.Nil(err)

	callFn := s.mockDCRedirectionPolicy.Calls[0].Arguments[2].(func(string) error)
	s.mockFrontendHandler.On(apiName, mock.Anything, req).Return(nil).Once()
	err = callFn(s.currentClusterName)
	s.Nil(err)
	s.mockRemoteFrontendClient.On(apiName, mock.Anything, req).Return(nil).Once()
	err = callFn(s.alternativeClusterName)
	s.Nil(err)
}

func (s *dcRedirectionHandlerSuite) TestRespondActivityTaskCompleted() {
	apiName := "RespondActivityTaskCompleted"

	s.mockDCRedirectionPolicy.On("WithDomainIDRedirect",
		s.domainID, apiName, mock.Anything).Return(nil).Once()

	token, err := s.handler.tokenSerializer.Serialize(&common.TaskToken{
		DomainID: s.domainID,
	})
	s.Nil(err)
	req := &shared.RespondActivityTaskCompletedRequest{
		TaskToken: token,
	}
	err = s.handler.RespondActivityTaskCompleted(context.Background(), req)
	s.Nil(err)

	callFn := s.mockDCRedirectionPolicy.Calls[0].Arguments[2].(func(string) error)
	s.mockFrontendHandler.On(apiName, mock.Anything, req).Return(nil).Once()
	err = callFn(s.currentClusterName)
	s.Nil(err)
	s.mockRemoteFrontendClient.On(apiName, mock.Anything, req).Return(nil).Once()
	err = callFn(s.alternativeClusterName)
	s.Nil(err)
}

func (s *dcRedirectionHandlerSuite) TestRespondActivityTaskCompletedByID() {
	apiName := "RespondActivityTaskCompletedByID"

	s.mockDCRedirectionPolicy.On("WithDomainNameRedirect",
		s.domainName, apiName, mock.Anything).Return(nil).Once()

	req := &shared.RespondActivityTaskCompletedByIDRequest{
		Domain: common.StringPtr(s.domainName),
	}
	err := s.handler.RespondActivityTaskCompletedByID(context.Background(), req)
	s.Nil(err)

	callFn := s.mockDCRedirectionPolicy.Calls[0].Arguments[2].(func(string) error)
	s.mockFrontendHandler.On(apiName, mock.Anything, req).Return(nil).Once()
	err = callFn(s.currentClusterName)
	s.Nil(err)
	s.mockRemoteFrontendClient.On(apiName, mock.Anything, req).Return(nil).Once()
	err = callFn(s.alternativeClusterName)
	s.Nil(err)
}

func (s *dcRedirectionHandlerSuite) TestRespondActivityTaskFailed() {
	apiName := "RespondActivityTaskFailed"

	s.mockDCRedirectionPolicy.On("WithDomainIDRedirect",
		s.domainID, apiName, mock.Anything).Return(nil).Once()

	token, err := s.handler.tokenSerializer.Serialize(&common.TaskToken{
		DomainID: s.domainID,
	})
	s.Nil(err)
	req := &shared.RespondActivityTaskFailedRequest{
		TaskToken: token,
	}
	err = s.handler.RespondActivityTaskFailed(context.Background(), req)
	s.Nil(err)

	callFn := s.mockDCRedirectionPolicy.Calls[0].Arguments[2].(func(string) error)
	s.mockFrontendHandler.On(apiName, mock.Anything, req).Return(nil).Once()
	err = callFn(s.currentClusterName)
	s.Nil(err)
	s.mockRemoteFrontendClient.On(apiName, mock.Anything, req).Return(nil).Once()
	err = callFn(s.alternativeClusterName)
	s.Nil(err)
}

func (s *dcRedirectionHandlerSuite) TestRespondActivityTaskFailedByID() {
	apiName := "RespondActivityTaskFailedByID"

	s.mockDCRedirectionPolicy.On("WithDomainNameRedirect",
		s.domainName, apiName, mock.Anything).Return(nil).Once()

	req := &shared.RespondActivityTaskFailedByIDRequest{
		Domain: common.StringPtr(s.domainName),
	}
	err := s.handler.RespondActivityTaskFailedByID(context.Background(), req)
	s.Nil(err)

	callFn := s.mockDCRedirectionPolicy.Calls[0].Arguments[2].(func(string) error)
	s.mockFrontendHandler.On(apiName, mock.Anything, req).Return(nil).Once()
	err = callFn(s.currentClusterName)
	s.Nil(err)
	s.mockRemoteFrontendClient.On(apiName, mock.Anything, req).Return(nil).Once()
	err = callFn(s.alternativeClusterName)
	s.Nil(err)
}

func (s *dcRedirectionHandlerSuite) TestRespondDecisionTaskCompleted() {
	apiName := "RespondDecisionTaskCompleted"

	s.mockDCRedirectionPolicy.On("WithDomainIDRedirect",
		s.domainID, apiName, mock.Anything).Return(nil).Once()

	token, err := s.handler.tokenSerializer.Serialize(&common.TaskToken{
		DomainID: s.domainID,
	})
	s.Nil(err)
	req := &shared.RespondDecisionTaskCompletedRequest{
		TaskToken: token,
	}
	resp, err := s.handler.RespondDecisionTaskCompleted(context.Background(), req)
	s.Nil(err)
	// the resp is initialized to nil, since inner function is not called
	s.Nil(resp)

	callFn := s.mockDCRedirectionPolicy.Calls[0].Arguments[2].(func(string) error)
	s.mockFrontendHandler.On(apiName, mock.Anything, req).Return(&shared.RespondDecisionTaskCompletedResponse{}, nil).Once()
	err = callFn(s.currentClusterName)
	s.Nil(err)
	s.mockRemoteFrontendClient.On(apiName, mock.Anything, req).Return(&shared.RespondDecisionTaskCompletedResponse{}, nil).Once()
	err = callFn(s.alternativeClusterName)
	s.Nil(err)
}

func (s *dcRedirectionHandlerSuite) TestRespondDecisionTaskFailed() {
	apiName := "RespondDecisionTaskFailed"

	s.mockDCRedirectionPolicy.On("WithDomainIDRedirect",
		s.domainID, apiName, mock.Anything).Return(nil).Once()

	token, err := s.handler.tokenSerializer.Serialize(&common.TaskToken{
		DomainID: s.domainID,
	})
	s.Nil(err)
	req := &shared.RespondDecisionTaskFailedRequest{
		TaskToken: token,
	}
	err = s.handler.RespondDecisionTaskFailed(context.Background(), req)
	s.Nil(err)

	callFn := s.mockDCRedirectionPolicy.Calls[0].Arguments[2].(func(string) error)
	s.mockFrontendHandler.On(apiName, mock.Anything, req).Return(nil).Once()
	err = callFn(s.currentClusterName)
	s.Nil(err)
	s.mockRemoteFrontendClient.On(apiName, mock.Anything, req).Return(nil).Once()
	err = callFn(s.alternativeClusterName)
	s.Nil(err)
}

func (s *dcRedirectionHandlerSuite) TestRespondQueryTaskCompleted() {
	apiName := "RespondQueryTaskCompleted"

	s.mockDCRedirectionPolicy.On("WithDomainIDRedirect",
		s.domainID, apiName, mock.Anything).Return(nil).Once()

	token, err := s.handler.tokenSerializer.SerializeQueryTaskToken(&common.QueryTaskToken{
		DomainID: s.domainID,
	})
	req := &shared.RespondQueryTaskCompletedRequest{
		TaskToken: token,
	}
	err = s.handler.RespondQueryTaskCompleted(context.Background(), req)
	s.Nil(err)

	callFn := s.mockDCRedirectionPolicy.Calls[0].Arguments[2].(func(string) error)
	s.mockFrontendHandler.On(apiName, mock.Anything, req).Return(nil).Once()
	err = callFn(s.currentClusterName)
	s.Nil(err)
	s.mockRemoteFrontendClient.On(apiName, mock.Anything, req).Return(nil).Once()
	err = callFn(s.alternativeClusterName)
	s.Nil(err)
}

func (s *dcRedirectionHandlerSuite) TestSignalWithStartWorkflowExecution() {
	apiName := "SignalWithStartWorkflowExecution"

	s.mockDCRedirectionPolicy.On("WithDomainNameRedirect",
		s.domainName, apiName, mock.Anything).Return(nil).Once()

	req := &shared.SignalWithStartWorkflowExecutionRequest{
		Domain: common.StringPtr(s.domainName),
	}
	resp, err := s.handler.SignalWithStartWorkflowExecution(context.Background(), req)
	s.Nil(err)
	// the resp is initialized to nil, since inner function is not called
	s.Nil(resp)

	callFn := s.mockDCRedirectionPolicy.Calls[0].Arguments[2].(func(string) error)
	s.mockFrontendHandler.On(apiName, mock.Anything, req).Return(&shared.StartWorkflowExecutionResponse{}, nil).Once()
	err = callFn(s.currentClusterName)
	s.Nil(err)
	s.mockRemoteFrontendClient.On(apiName, mock.Anything, req).Return(&shared.StartWorkflowExecutionResponse{}, nil).Once()
	err = callFn(s.alternativeClusterName)
	s.Nil(err)
}

func (s *dcRedirectionHandlerSuite) TestSignalWorkflowExecution() {
	apiName := "SignalWorkflowExecution"

	s.mockDCRedirectionPolicy.On("WithDomainNameRedirect",
		s.domainName, apiName, mock.Anything).Return(nil).Once()

	req := &shared.SignalWorkflowExecutionRequest{
		Domain: common.StringPtr(s.domainName),
	}
	err := s.handler.SignalWorkflowExecution(context.Background(), req)
	s.Nil(err)

	callFn := s.mockDCRedirectionPolicy.Calls[0].Arguments[2].(func(string) error)
	s.mockFrontendHandler.On(apiName, mock.Anything, req).Return(nil).Once()
	err = callFn(s.currentClusterName)
	s.Nil(err)
	s.mockRemoteFrontendClient.On(apiName, mock.Anything, req).Return(nil).Once()
	err = callFn(s.alternativeClusterName)
	s.Nil(err)
}

func (s *dcRedirectionHandlerSuite) TestStartWorkflowExecution() {
	apiName := "StartWorkflowExecution"

	s.mockDCRedirectionPolicy.On("WithDomainNameRedirect",
		s.domainName, apiName, mock.Anything).Return(nil).Once()

	req := &shared.StartWorkflowExecutionRequest{
		Domain: common.StringPtr(s.domainName),
	}
	resp, err := s.handler.StartWorkflowExecution(context.Background(), req)
	s.Nil(err)
	// the resp is initialized to nil, since inner function is not called
	s.Nil(resp)

	callFn := s.mockDCRedirectionPolicy.Calls[0].Arguments[2].(func(string) error)
	s.mockFrontendHandler.On(apiName, mock.Anything, req).Return(&shared.StartWorkflowExecutionResponse{}, nil).Once()
	err = callFn(s.currentClusterName)
	s.Nil(err)
	s.mockRemoteFrontendClient.On(apiName, mock.Anything, req).Return(&shared.StartWorkflowExecutionResponse{}, nil).Once()
	err = callFn(s.alternativeClusterName)
	s.Nil(err)
}

func (s *dcRedirectionHandlerSuite) TestTerminateWorkflowExecution() {
	apiName := "TerminateWorkflowExecution"

	s.mockDCRedirectionPolicy.On("WithDomainNameRedirect",
		s.domainName, apiName, mock.Anything).Return(nil).Once()

	req := &shared.TerminateWorkflowExecutionRequest{
		Domain: common.StringPtr(s.domainName),
	}
	err := s.handler.TerminateWorkflowExecution(context.Background(), req)
	s.Nil(err)

	callFn := s.mockDCRedirectionPolicy.Calls[0].Arguments[2].(func(string) error)
	s.mockFrontendHandler.On(apiName, mock.Anything, req).Return(nil).Once()
	err = callFn(s.currentClusterName)
	s.Nil(err)
	s.mockRemoteFrontendClient.On(apiName, mock.Anything, req).Return(nil).Once()
	err = callFn(s.alternativeClusterName)
	s.Nil(err)
}
