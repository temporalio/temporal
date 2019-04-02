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
	"os"
	"reflect"
	"testing"

	"github.com/uber/cadence/common/service/config"

	"github.com/pborman/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"github.com/uber-common/bark"
	"github.com/uber-go/tally"
	h "github.com/uber/cadence/.gen/go/history"
	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/client"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/mocks"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/service"
	"github.com/uber/cadence/common/service/dynamicconfig"
)

type (
	dcRedirectionHandlerSuite struct {
		suite.Suite
		logger                 bark.Logger
		currentClusterName     string
		alternativeClusterName string
		config                 *Config
		redirectionPolicy      config.DCRedirectionPolicy
		service                service.Service
		domainCache            cache.DomainCache

		mockClusterMetadata      *mocks.ClusterMetadata
		mockMetadataMgr          *mocks.MetadataManager
		mockClientBean           *client.MockClientBean
		mockHistoryClient        *mocks.HistoryClient
		mockRemoteFrontendClient *mocks.FrontendClient

		frontendHandler *WorkflowHandler
		handler         *DCRedirectionHandlerImpl
	}
)

func TestDCRedirectionHandlerSuite(t *testing.T) {
	s := new(dcRedirectionHandlerSuite)
	suite.Run(t, s)
}

func (s *dcRedirectionHandlerSuite) SetupSuite() {
	if testing.Verbose() {
		log.SetOutput(os.Stdout)
	}
}

func (s *dcRedirectionHandlerSuite) TearDownSuite() {

}

func (s *dcRedirectionHandlerSuite) SetupTest() {
	log2 := log.New()
	log2.Level = log.DebugLevel
	s.logger = bark.NewLoggerFromLogrus(log2)
	s.currentClusterName = cluster.TestCurrentClusterName
	s.alternativeClusterName = cluster.TestAlternativeClusterName
	s.config = NewConfig(dynamicconfig.NewCollection(dynamicconfig.NewNopClient(), s.logger), false, false)
	s.redirectionPolicy = config.DCRedirectionPolicy{}
	s.mockMetadataMgr = &mocks.MetadataManager{}

	s.mockClusterMetadata = &mocks.ClusterMetadata{}
	s.mockClusterMetadata.On("GetCurrentClusterName").Return(s.currentClusterName)
	s.mockClusterMetadata.On("IsGlobalDomainEnabled").Return(true)
	metricsClient := metrics.NewClient(tally.NoopScope, metrics.Frontend)
	s.mockClientBean = &client.MockClientBean{}
	s.mockHistoryClient = &mocks.HistoryClient{}
	s.mockClientBean.On("GetHistoryClient").Return(s.mockHistoryClient)
	s.mockRemoteFrontendClient = &mocks.FrontendClient{}
	s.mockClientBean.On("GetRemoteFrontendClient", s.alternativeClusterName).Return(s.mockRemoteFrontendClient)
	s.service = service.NewTestService(s.mockClusterMetadata, nil, metricsClient, s.mockClientBean, s.logger)
	s.frontendHandler = NewWorkflowHandler(s.service, s.config, s.mockMetadataMgr, nil, nil, nil, nil, nil)
	s.frontendHandler.metricsClient = metricsClient
	s.frontendHandler.history = s.mockHistoryClient
	s.frontendHandler.startWG.Done()

	s.handler = NewDCRedirectionHandler(s.frontendHandler, s.redirectionPolicy)
}

func (s *dcRedirectionHandlerSuite) TearDownTest() {
	s.mockMetadataMgr.AssertExpectations(s.T())
	s.mockHistoryClient.AssertExpectations(s.T())
}

func (s *dcRedirectionHandlerSuite) TestSignalWorkflow_DomainActive_LocalDomain() {
	s.setupLocalDomain()
	req := s.getSignalRequest()

	s.mockHistoryClient.On("SignalWorkflowExecution", mock.Anything, mock.MatchedBy(func(input *h.SignalWorkflowExecutionRequest) bool {
		return reflect.DeepEqual(req, input.SignalRequest)
	})).Return(nil).Once()
	err := s.handler.SignalWorkflowExecution(context.Background(), req)
	s.Nil(err)
}

func (s *dcRedirectionHandlerSuite) TestSignalWorkflow_DomainNotActive_LocalDomain() {
	s.setupLocalDomain()
	req := s.getSignalRequest()

	domainNotActiveErr := &shared.DomainNotActiveError{}
	s.mockHistoryClient.On("SignalWorkflowExecution", mock.Anything, mock.MatchedBy(func(input *h.SignalWorkflowExecutionRequest) bool {
		return reflect.DeepEqual(req, input.SignalRequest)
	})).Return(domainNotActiveErr).Once()
	err := s.handler.SignalWorkflowExecution(context.Background(), req)
	s.Equal(domainNotActiveErr, err)
}

func (s *dcRedirectionHandlerSuite) TestSignalWorkflow_DomainActive_GlobalDomain_OneCluster() {
	s.setupGlobalDomainWithOneReplicationCluster()
	req := s.getSignalRequest()

	s.mockHistoryClient.On("SignalWorkflowExecution", mock.Anything, mock.MatchedBy(func(input *h.SignalWorkflowExecutionRequest) bool {
		return reflect.DeepEqual(req, input.SignalRequest)
	})).Return(nil).Once()
	err := s.handler.SignalWorkflowExecution(context.Background(), req)
	s.Nil(err)
}

func (s *dcRedirectionHandlerSuite) TestSignalWorkflow_DomainNotActive_GlobalDomain_OneCluster() {
	s.setupGlobalDomainWithOneReplicationCluster()
	req := s.getSignalRequest()

	domainNotActiveErr := &shared.DomainNotActiveError{}
	s.mockHistoryClient.On("SignalWorkflowExecution", mock.Anything, mock.MatchedBy(func(input *h.SignalWorkflowExecutionRequest) bool {
		return reflect.DeepEqual(req, input.SignalRequest)
	})).Return(domainNotActiveErr).Once()
	err := s.handler.SignalWorkflowExecution(context.Background(), req)
	s.Equal(domainNotActiveErr, err)
}

func (s *dcRedirectionHandlerSuite) TestSignalWorkflow_DomainActive_GlobalDomain_TwoCluster_ForwardingNotEnabled_DomainRecordActive() {
	s.setupGlobalDomainWithTwoReplicationCluster(false, true)
	req := s.getSignalRequest()

	s.mockHistoryClient.On("SignalWorkflowExecution", mock.Anything, mock.MatchedBy(func(input *h.SignalWorkflowExecutionRequest) bool {
		return reflect.DeepEqual(req, input.SignalRequest)
	})).Return(nil).Once()
	err := s.handler.SignalWorkflowExecution(context.Background(), req)
	s.Nil(err)
}

func (s *dcRedirectionHandlerSuite) TestSignalWorkflow_DomainActive_GlobalDomain_TwoCluster_ForwardingNotEnabled_DomainRecordNotActive() {
	s.setupGlobalDomainWithTwoReplicationCluster(false, false)
	req := s.getSignalRequest()

	s.mockHistoryClient.On("SignalWorkflowExecution", mock.Anything, mock.MatchedBy(func(input *h.SignalWorkflowExecutionRequest) bool {
		return reflect.DeepEqual(req, input.SignalRequest)
	})).Return(nil).Once()
	err := s.handler.SignalWorkflowExecution(context.Background(), req)
	s.Nil(err)
}

func (s *dcRedirectionHandlerSuite) TestSignalWorkflow_DomainNotActive_GlobalDomain_TwoCluster_ForwardingNotEnabled_DomainRecordActive() {
	s.setupGlobalDomainWithTwoReplicationCluster(false, true)
	req := s.getSignalRequest()

	domainNotActiveErr := &shared.DomainNotActiveError{}
	s.mockHistoryClient.On("SignalWorkflowExecution", mock.Anything, mock.MatchedBy(func(input *h.SignalWorkflowExecutionRequest) bool {
		return reflect.DeepEqual(req, input.SignalRequest)
	})).Return(domainNotActiveErr).Once()
	err := s.handler.SignalWorkflowExecution(context.Background(), req)
	s.Equal(domainNotActiveErr, err)
}

func (s *dcRedirectionHandlerSuite) TestSignalWorkflow_DomainNotActive_GlobalDomain_TwoCluster_ForwardingNotEnabled_DomainRecordNotActive() {
	s.setupGlobalDomainWithTwoReplicationCluster(false, false)
	req := s.getSignalRequest()

	domainNotActiveErr := &shared.DomainNotActiveError{}
	s.mockHistoryClient.On("SignalWorkflowExecution", mock.Anything, mock.MatchedBy(func(input *h.SignalWorkflowExecutionRequest) bool {
		return reflect.DeepEqual(req, input.SignalRequest)
	})).Return(domainNotActiveErr).Once()
	err := s.handler.SignalWorkflowExecution(context.Background(), req)
	s.Equal(domainNotActiveErr, err)
}

func (s *dcRedirectionHandlerSuite) TestSignalWorkflow_DomainActive_GlobalDomain_TwoCluster_ForwardingEnabled_DomainRecordActive() {
	req := s.getSignalRequest()

	s.setupGlobalDomainWithTwoReplicationCluster(true, true)
	s.mockHistoryClient.On("SignalWorkflowExecution", mock.Anything, mock.MatchedBy(func(input *h.SignalWorkflowExecutionRequest) bool {
		return reflect.DeepEqual(req, input.SignalRequest)
	})).Return(nil).Once()
	err := s.handler.SignalWorkflowExecution(context.Background(), req)
	s.Nil(err)
}

func (s *dcRedirectionHandlerSuite) TestSignalWorkflow_DomainActive_GlobalDomain_TwoCluster_ForwardingEnabled_DomainRecordNotActive() {
	req := s.getSignalRequest()

	s.setupGlobalDomainWithTwoReplicationCluster(true, false)
	s.mockRemoteFrontendClient.On("SignalWorkflowExecution", mock.Anything, req).Return(nil).Once()
	err := s.handler.SignalWorkflowExecution(context.Background(), req)
	s.Nil(err)
}

func (s *dcRedirectionHandlerSuite) TestSignalWorkflow_DomainNotActive_GlobalDomain_TwoCluster_ForwardingEnabled_DomainRecordActive() {
	req := s.getSignalRequest()

	s.setupGlobalDomainWithTwoReplicationCluster(true, true)
	domainNotActiveErr := &shared.DomainNotActiveError{ActiveCluster: s.alternativeClusterName}
	s.mockHistoryClient.On("SignalWorkflowExecution", mock.Anything, mock.MatchedBy(func(input *h.SignalWorkflowExecutionRequest) bool {
		return reflect.DeepEqual(req, input.SignalRequest)
	})).Return(domainNotActiveErr).Once()
	s.mockRemoteFrontendClient.On("SignalWorkflowExecution", mock.Anything, req).Return(nil).Once()
	err := s.handler.SignalWorkflowExecution(context.Background(), req)
	s.Nil(err)
}

func (s *dcRedirectionHandlerSuite) TestSignalWorkflow_DomainNotActive_GlobalDomain_TwoCluster_ForwardingEnabled_DomainRecordNotActive() {
	req := s.getSignalRequest()

	s.setupGlobalDomainWithTwoReplicationCluster(true, false)
	domainNotActiveErr := &shared.DomainNotActiveError{ActiveCluster: s.currentClusterName}
	s.mockRemoteFrontendClient.On("SignalWorkflowExecution", mock.Anything, req).Return(domainNotActiveErr).Once()
	s.mockHistoryClient.On("SignalWorkflowExecution", mock.Anything, mock.MatchedBy(func(input *h.SignalWorkflowExecutionRequest) bool {
		return reflect.DeepEqual(req, input.SignalRequest)
	})).Return(nil).Once()
	err := s.handler.SignalWorkflowExecution(context.Background(), req)
	s.Nil(err)
}

func (s *dcRedirectionHandlerSuite) TestSignalWithStartWorkflow_DomainActive_LocalDomain() {
	s.setupLocalDomain()
	req := s.getSignalWithStartRequest()
	resp := &shared.StartWorkflowExecutionResponse{}

	s.mockHistoryClient.On("SignalWithStartWorkflowExecution", mock.Anything, mock.MatchedBy(func(input *h.SignalWithStartWorkflowExecutionRequest) bool {
		return reflect.DeepEqual(req, input.SignalWithStartRequest)
	})).Return(resp, nil).Once()
	result, err := s.handler.SignalWithStartWorkflowExecution(context.Background(), req)
	s.Nil(err)
	s.Equal(resp, result)
}

func (s *dcRedirectionHandlerSuite) TestSignalWithStartWorkflow_DomainNotActive_LocalDomain() {
	s.setupLocalDomain()
	req := s.getSignalWithStartRequest()

	domainNotActiveErr := &shared.DomainNotActiveError{}
	s.mockHistoryClient.On("SignalWithStartWorkflowExecution", mock.Anything, mock.MatchedBy(func(input *h.SignalWithStartWorkflowExecutionRequest) bool {
		return reflect.DeepEqual(req, input.SignalWithStartRequest)
	})).Return(nil, domainNotActiveErr).Once()
	result, err := s.handler.SignalWithStartWorkflowExecution(context.Background(), req)
	s.Equal(domainNotActiveErr, err)
	s.Nil(result)
}

func (s *dcRedirectionHandlerSuite) TestSignalWithStartWorkflow_DomainActive_GlobalDomain_OneCluster() {
	s.setupGlobalDomainWithOneReplicationCluster()
	req := s.getSignalWithStartRequest()
	resp := &shared.StartWorkflowExecutionResponse{}

	s.mockHistoryClient.On("SignalWithStartWorkflowExecution", mock.Anything, mock.MatchedBy(func(input *h.SignalWithStartWorkflowExecutionRequest) bool {
		return reflect.DeepEqual(req, input.SignalWithStartRequest)
	})).Return(resp, nil).Once()
	result, err := s.handler.SignalWithStartWorkflowExecution(context.Background(), req)
	s.Nil(err)
	s.Equal(resp, result)
}

func (s *dcRedirectionHandlerSuite) TestSignalWithStartWorkflow_DomainNotActive_GlobalDomain_OneCluster() {
	s.setupGlobalDomainWithOneReplicationCluster()
	req := s.getSignalWithStartRequest()

	domainNotActiveErr := &shared.DomainNotActiveError{}
	s.mockHistoryClient.On("SignalWithStartWorkflowExecution", mock.Anything, mock.MatchedBy(func(input *h.SignalWithStartWorkflowExecutionRequest) bool {
		return reflect.DeepEqual(req, input.SignalWithStartRequest)
	})).Return(nil, domainNotActiveErr).Once()
	result, err := s.handler.SignalWithStartWorkflowExecution(context.Background(), req)
	s.Equal(domainNotActiveErr, err)
	s.Nil(result)
}

func (s *dcRedirectionHandlerSuite) TestSignalWithStartWorkflow_DomainActive_GlobalDomain_TwoCluster_ForwardingNotEnabled_DomainRecordActive() {
	s.setupGlobalDomainWithTwoReplicationCluster(false, true)
	req := s.getSignalWithStartRequest()
	resp := &shared.StartWorkflowExecutionResponse{}

	s.mockHistoryClient.On("SignalWithStartWorkflowExecution", mock.Anything, mock.MatchedBy(func(input *h.SignalWithStartWorkflowExecutionRequest) bool {
		return reflect.DeepEqual(req, input.SignalWithStartRequest)
	})).Return(resp, nil).Once()
	result, err := s.handler.SignalWithStartWorkflowExecution(context.Background(), req)
	s.Nil(err)
	s.Equal(resp, result)
}

func (s *dcRedirectionHandlerSuite) TestSignalWithStartWorkflow_DomainActive_GlobalDomain_TwoCluster_ForwardingNotEnabled_DomainRecordNotActive() {
	s.setupGlobalDomainWithTwoReplicationCluster(false, false)
	req := s.getSignalWithStartRequest()
	resp := &shared.StartWorkflowExecutionResponse{}

	s.mockHistoryClient.On("SignalWithStartWorkflowExecution", mock.Anything, mock.MatchedBy(func(input *h.SignalWithStartWorkflowExecutionRequest) bool {
		return reflect.DeepEqual(req, input.SignalWithStartRequest)
	})).Return(resp, nil).Once()
	result, err := s.handler.SignalWithStartWorkflowExecution(context.Background(), req)
	s.Nil(err)
	s.Equal(resp, result)
}

func (s *dcRedirectionHandlerSuite) TestSignalWithStartWorkflow_DomainNotActive_GlobalDomain_TwoCluster_ForwardingNotEnabled_DomainRecordActive() {
	s.setupGlobalDomainWithTwoReplicationCluster(false, true)
	req := s.getSignalWithStartRequest()

	domainNotActiveErr := &shared.DomainNotActiveError{}
	s.mockHistoryClient.On("SignalWithStartWorkflowExecution", mock.Anything, mock.MatchedBy(func(input *h.SignalWithStartWorkflowExecutionRequest) bool {
		return reflect.DeepEqual(req, input.SignalWithStartRequest)
	})).Return(nil, domainNotActiveErr).Once()
	result, err := s.handler.SignalWithStartWorkflowExecution(context.Background(), req)
	s.Equal(domainNotActiveErr, err)
	s.Nil(result)
}

func (s *dcRedirectionHandlerSuite) TestSignalWithStartWorkflow_DomainNotActive_GlobalDomain_TwoCluster_ForwardingNotEnabled_DomainRecordNotActive() {
	s.setupGlobalDomainWithTwoReplicationCluster(false, false)
	req := s.getSignalWithStartRequest()

	domainNotActiveErr := &shared.DomainNotActiveError{}
	s.mockHistoryClient.On("SignalWithStartWorkflowExecution", mock.Anything, mock.MatchedBy(func(input *h.SignalWithStartWorkflowExecutionRequest) bool {
		return reflect.DeepEqual(req, input.SignalWithStartRequest)
	})).Return(nil, domainNotActiveErr).Once()
	result, err := s.handler.SignalWithStartWorkflowExecution(context.Background(), req)
	s.Equal(domainNotActiveErr, err)
	s.Nil(result)
}

func (s *dcRedirectionHandlerSuite) TestSignalWithStartWorkflow_DomainActive_GlobalDomain_TwoCluster_ForwardingEnabled_DomainRecordActive() {
	req := s.getSignalWithStartRequest()
	resp := &shared.StartWorkflowExecutionResponse{}

	s.setupGlobalDomainWithTwoReplicationCluster(true, true)
	s.mockHistoryClient.On("SignalWithStartWorkflowExecution", mock.Anything, mock.MatchedBy(func(input *h.SignalWithStartWorkflowExecutionRequest) bool {
		return reflect.DeepEqual(req, input.SignalWithStartRequest)
	})).Return(resp, nil).Once()
	result, err := s.handler.SignalWithStartWorkflowExecution(context.Background(), req)
	s.Nil(err)
	s.Equal(resp, result)
}

func (s *dcRedirectionHandlerSuite) TestSignalWithStartWorkflow_DomainActive_GlobalDomain_TwoCluster_ForwardingEnabled_DomainRecordNotActive() {
	req := s.getSignalWithStartRequest()
	resp := &shared.StartWorkflowExecutionResponse{}

	s.setupGlobalDomainWithTwoReplicationCluster(true, false)
	s.mockRemoteFrontendClient.On("SignalWithStartWorkflowExecution", mock.Anything, req).Return(resp, nil).Once()
	result, err := s.handler.SignalWithStartWorkflowExecution(context.Background(), req)
	s.Nil(err)
	s.Equal(resp, result)
}

func (s *dcRedirectionHandlerSuite) TestSignalWithStartWorkflow_DomainNotActive_GlobalDomain_TwoCluster_ForwardingEnabled_DomainRecordActive() {
	req := s.getSignalWithStartRequest()
	resp := &shared.StartWorkflowExecutionResponse{}

	s.setupGlobalDomainWithTwoReplicationCluster(true, true)
	domainNotActiveErr := &shared.DomainNotActiveError{ActiveCluster: s.alternativeClusterName}
	s.mockHistoryClient.On("SignalWithStartWorkflowExecution", mock.Anything, mock.MatchedBy(func(input *h.SignalWithStartWorkflowExecutionRequest) bool {
		return reflect.DeepEqual(req, input.SignalWithStartRequest)
	})).Return(nil, domainNotActiveErr).Once()
	s.mockRemoteFrontendClient.On("SignalWithStartWorkflowExecution", mock.Anything, req).Return(resp, nil).Once()
	result, err := s.handler.SignalWithStartWorkflowExecution(context.Background(), req)
	s.Nil(err)
	s.Equal(resp, result)
}

func (s *dcRedirectionHandlerSuite) TestSignalWithStartWorkflow_DomainNotActive_GlobalDomain_TwoCluster_ForwardingEnabled_DomainRecordNotActive() {
	req := s.getSignalWithStartRequest()
	resp := &shared.StartWorkflowExecutionResponse{}

	s.setupGlobalDomainWithTwoReplicationCluster(true, false)
	domainNotActiveErr := &shared.DomainNotActiveError{ActiveCluster: s.currentClusterName}
	s.mockRemoteFrontendClient.On("SignalWithStartWorkflowExecution", mock.Anything, req).Return(nil, domainNotActiveErr).Once()
	s.mockHistoryClient.On("SignalWithStartWorkflowExecution", mock.Anything, mock.MatchedBy(func(input *h.SignalWithStartWorkflowExecutionRequest) bool {
		return reflect.DeepEqual(req, input.SignalWithStartRequest)
	})).Return(resp, nil).Once()
	result, err := s.handler.SignalWithStartWorkflowExecution(context.Background(), req)
	s.Nil(err)
	s.Equal(resp, result)
}

func (s *dcRedirectionHandlerSuite) TestStartWorkflow_DomainActive_LocalDomain() {
	s.setupLocalDomain()
	req := s.getStartRequest()
	resp := &shared.StartWorkflowExecutionResponse{}

	s.mockHistoryClient.On("StartWorkflowExecution", mock.Anything, mock.MatchedBy(func(input *h.StartWorkflowExecutionRequest) bool {
		return reflect.DeepEqual(req, input.StartRequest)
	})).Return(resp, nil).Once()
	result, err := s.handler.StartWorkflowExecution(context.Background(), req)
	s.Nil(err)
	s.Equal(resp, result)
}

func (s *dcRedirectionHandlerSuite) TestStartWorkflow_DomainNotActive_LocalDomain() {
	s.setupLocalDomain()
	req := s.getStartRequest()

	domainNotActiveErr := &shared.DomainNotActiveError{}
	s.mockHistoryClient.On("StartWorkflowExecution", mock.Anything, mock.MatchedBy(func(input *h.StartWorkflowExecutionRequest) bool {
		return reflect.DeepEqual(req, input.StartRequest)
	})).Return(nil, domainNotActiveErr).Once()
	result, err := s.handler.StartWorkflowExecution(context.Background(), req)
	s.Equal(domainNotActiveErr, err)
	s.Nil(result)
}

func (s *dcRedirectionHandlerSuite) TestStartWorkflow_DomainActive_GlobalDomain_OneCluster() {
	s.setupGlobalDomainWithOneReplicationCluster()
	req := s.getStartRequest()
	resp := &shared.StartWorkflowExecutionResponse{}

	s.mockHistoryClient.On("StartWorkflowExecution", mock.Anything, mock.MatchedBy(func(input *h.StartWorkflowExecutionRequest) bool {
		return reflect.DeepEqual(req, input.StartRequest)
	})).Return(resp, nil).Once()
	result, err := s.handler.StartWorkflowExecution(context.Background(), req)
	s.Nil(err)
	s.Equal(resp, result)
}

func (s *dcRedirectionHandlerSuite) TestStartWorkflow_DomainNotActive_GlobalDomain_OneCluster() {
	s.setupGlobalDomainWithOneReplicationCluster()
	req := s.getStartRequest()

	domainNotActiveErr := &shared.DomainNotActiveError{}
	s.mockHistoryClient.On("StartWorkflowExecution", mock.Anything, mock.MatchedBy(func(input *h.StartWorkflowExecutionRequest) bool {
		return reflect.DeepEqual(req, input.StartRequest)
	})).Return(nil, domainNotActiveErr).Once()
	result, err := s.handler.StartWorkflowExecution(context.Background(), req)
	s.Equal(domainNotActiveErr, err)
	s.Nil(result)
}

func (s *dcRedirectionHandlerSuite) TestStartWorkflow_DomainActive_GlobalDomain_TwoCluster_ForwardingNotEnabled_DomainRecordActive() {
	s.setupGlobalDomainWithTwoReplicationCluster(false, true)
	req := s.getStartRequest()
	resp := &shared.StartWorkflowExecutionResponse{}

	s.mockHistoryClient.On("StartWorkflowExecution", mock.Anything, mock.MatchedBy(func(input *h.StartWorkflowExecutionRequest) bool {
		return reflect.DeepEqual(req, input.StartRequest)
	})).Return(resp, nil).Once()
	result, err := s.handler.StartWorkflowExecution(context.Background(), req)
	s.Nil(err)
	s.Equal(resp, result)
}

func (s *dcRedirectionHandlerSuite) TestStartWorkflow_DomainActive_GlobalDomain_TwoCluster_ForwardingNotEnabled_DomainRecordNotActive() {
	s.setupGlobalDomainWithTwoReplicationCluster(false, true)
	req := s.getStartRequest()
	resp := &shared.StartWorkflowExecutionResponse{}

	s.mockHistoryClient.On("StartWorkflowExecution", mock.Anything, mock.MatchedBy(func(input *h.StartWorkflowExecutionRequest) bool {
		return reflect.DeepEqual(req, input.StartRequest)
	})).Return(resp, nil).Once()
	result, err := s.handler.StartWorkflowExecution(context.Background(), req)
	s.Nil(err)
	s.Equal(resp, result)
}

func (s *dcRedirectionHandlerSuite) TestStartWorkflow_DomainNotActive_GlobalDomain_TwoCluster_ForwardingNotEnabled_DomainRecordActive() {
	s.setupGlobalDomainWithTwoReplicationCluster(false, true)
	req := s.getStartRequest()

	domainNotActiveErr := &shared.DomainNotActiveError{}
	s.mockHistoryClient.On("StartWorkflowExecution", mock.Anything, mock.MatchedBy(func(input *h.StartWorkflowExecutionRequest) bool {
		return reflect.DeepEqual(req, input.StartRequest)
	})).Return(nil, domainNotActiveErr).Once()
	result, err := s.handler.StartWorkflowExecution(context.Background(), req)
	s.Equal(domainNotActiveErr, err)
	s.Nil(result)
}

func (s *dcRedirectionHandlerSuite) TestStartWorkflow_DomainNotActive_GlobalDomain_TwoCluster_ForwardingNotEnabled_DomainRecordNotActive() {
	s.setupGlobalDomainWithTwoReplicationCluster(false, false)
	req := s.getStartRequest()

	domainNotActiveErr := &shared.DomainNotActiveError{}
	s.mockHistoryClient.On("StartWorkflowExecution", mock.Anything, mock.MatchedBy(func(input *h.StartWorkflowExecutionRequest) bool {
		return reflect.DeepEqual(req, input.StartRequest)
	})).Return(nil, domainNotActiveErr).Once()
	result, err := s.handler.StartWorkflowExecution(context.Background(), req)
	s.Equal(domainNotActiveErr, err)
	s.Nil(result)
}

func (s *dcRedirectionHandlerSuite) TestStartWorkflow_DomainActive_GlobalDomain_TwoCluster_ForwardingEnabled_DomainRecordActive() {
	req := s.getStartRequest()
	resp := &shared.StartWorkflowExecutionResponse{}

	s.setupGlobalDomainWithTwoReplicationCluster(true, true)
	s.mockHistoryClient.On("StartWorkflowExecution", mock.Anything, mock.MatchedBy(func(input *h.StartWorkflowExecutionRequest) bool {
		return reflect.DeepEqual(req, input.StartRequest)
	})).Return(resp, nil).Once()
	result, err := s.handler.StartWorkflowExecution(context.Background(), req)
	s.Nil(err)
	s.Equal(resp, result)
}

func (s *dcRedirectionHandlerSuite) TestStartWorkflow_DomainActive_GlobalDomain_TwoCluster_ForwardingEnabled_DomainRecordNotActive() {
	req := s.getStartRequest()
	resp := &shared.StartWorkflowExecutionResponse{}

	s.setupGlobalDomainWithTwoReplicationCluster(true, false)
	s.mockRemoteFrontendClient.On("StartWorkflowExecution", mock.Anything, req).Return(resp, nil).Once()
	result, err := s.handler.StartWorkflowExecution(context.Background(), req)
	s.Nil(err)
	s.Equal(resp, result)
}

func (s *dcRedirectionHandlerSuite) TestStartWorkflow_DomainNotActive_GlobalDomain_TwoCluster_ForwardingEnabled_DomainRecordActive() {
	req := s.getStartRequest()
	resp := &shared.StartWorkflowExecutionResponse{}

	s.setupGlobalDomainWithTwoReplicationCluster(true, true)
	domainNotActiveErr := &shared.DomainNotActiveError{ActiveCluster: s.alternativeClusterName}
	s.mockHistoryClient.On("StartWorkflowExecution", mock.Anything, mock.MatchedBy(func(input *h.StartWorkflowExecutionRequest) bool {
		return reflect.DeepEqual(req, input.StartRequest)
	})).Return(nil, domainNotActiveErr).Once()
	s.mockRemoteFrontendClient.On("StartWorkflowExecution", mock.Anything, req).Return(resp, nil).Once()
	result, err := s.handler.StartWorkflowExecution(context.Background(), req)
	s.Nil(err)
	s.Equal(resp, result)
}

func (s *dcRedirectionHandlerSuite) TestStartWorkflow_DomainNotActive_GlobalDomain_TwoCluster_ForwardingEnabled_DomainRecordNotActive() {
	req := s.getStartRequest()
	resp := &shared.StartWorkflowExecutionResponse{}

	s.setupGlobalDomainWithTwoReplicationCluster(true, false)
	domainNotActiveErr := &shared.DomainNotActiveError{ActiveCluster: s.currentClusterName}
	s.mockRemoteFrontendClient.On("StartWorkflowExecution", mock.Anything, req).Return(nil, domainNotActiveErr).Once()
	s.mockHistoryClient.On("StartWorkflowExecution", mock.Anything, mock.MatchedBy(func(input *h.StartWorkflowExecutionRequest) bool {
		return reflect.DeepEqual(req, input.StartRequest)
	})).Return(resp, nil).Once()
	result, err := s.handler.StartWorkflowExecution(context.Background(), req)
	s.Nil(err)
	s.Equal(resp, result)
}

func (s *dcRedirectionHandlerSuite) setupLocalDomain() {
	domainName := "some random domain name"
	domainID := "some random domain ID"
	domainRecord := &persistence.GetDomainResponse{
		Info:   &persistence.DomainInfo{ID: domainID, Name: domainName},
		Config: &persistence.DomainConfig{},
		ReplicationConfig: &persistence.DomainReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []*persistence.ClusterReplicationConfig{
				&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
			},
		},
		IsGlobalDomain: false,
		TableVersion:   persistence.DomainTableVersionV1,
	}

	s.mockMetadataMgr.On("GetDomain", mock.Anything).Return(domainRecord, nil)
}

func (s *dcRedirectionHandlerSuite) setupGlobalDomainWithOneReplicationCluster() {
	domainName := "some random domain name"
	domainID := "some random domain ID"
	domainRecord := &persistence.GetDomainResponse{
		Info:   &persistence.DomainInfo{ID: domainID, Name: domainName},
		Config: &persistence.DomainConfig{},
		ReplicationConfig: &persistence.DomainReplicationConfig{
			ActiveClusterName: cluster.TestAlternativeClusterName,
			Clusters: []*persistence.ClusterReplicationConfig{
				&persistence.ClusterReplicationConfig{ClusterName: cluster.TestAlternativeClusterName},
			},
		},
		IsGlobalDomain: true,
		TableVersion:   persistence.DomainTableVersionV1,
	}

	s.mockMetadataMgr.On("GetDomain", mock.Anything).Return(domainRecord, nil)
}

func (s *dcRedirectionHandlerSuite) setupGlobalDomainWithTwoReplicationCluster(forwardingEnabled bool, isRecordActive bool) {
	domainName := "some random domain name"
	domainID := "some random domain ID"
	activeCluster := s.alternativeClusterName
	if isRecordActive {
		activeCluster = s.currentClusterName
	}
	domainRecord := &persistence.GetDomainResponse{
		Info:   &persistence.DomainInfo{ID: domainID, Name: domainName},
		Config: &persistence.DomainConfig{},
		ReplicationConfig: &persistence.DomainReplicationConfig{
			ActiveClusterName: activeCluster,
			Clusters: []*persistence.ClusterReplicationConfig{
				&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				&persistence.ClusterReplicationConfig{ClusterName: cluster.TestAlternativeClusterName},
			},
		},
		IsGlobalDomain: true,
		TableVersion:   persistence.DomainTableVersionV1,
	}

	s.mockMetadataMgr.On("GetDomain", mock.Anything).Return(domainRecord, nil)
	s.config.EnableDomainNotActiveAutoForwarding = dynamicconfig.GetBoolPropertyFnFilteredByDomain(forwardingEnabled)
}

func (s *dcRedirectionHandlerSuite) getSignalRequest() *shared.SignalWorkflowExecutionRequest {
	return &shared.SignalWorkflowExecutionRequest{
		Domain: common.StringPtr("some random domain name"),
		WorkflowExecution: &shared.WorkflowExecution{
			WorkflowId: common.StringPtr("some random workflow ID"),
			RunId:      common.StringPtr(uuid.New()),
		},
		SignalName: common.StringPtr("some random signal name"),
		Input:      []byte("some random signal input"),
		Identity:   common.StringPtr("some random signal identity"),
	}
}

func (s *dcRedirectionHandlerSuite) getSignalWithStartRequest() *shared.SignalWithStartWorkflowExecutionRequest {
	workflowType := "some random workflow type"
	taskList := "some random task list"
	return &shared.SignalWithStartWorkflowExecutionRequest{
		Domain:                              common.StringPtr("some random domain name"),
		WorkflowId:                          common.StringPtr("some random workflow ID"),
		WorkflowType:                        &shared.WorkflowType{Name: common.StringPtr(workflowType)},
		TaskList:                            &shared.TaskList{Name: common.StringPtr(taskList)},
		Input:                               []byte("some random workflow input"),
		ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(123),
		TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(234),
		Identity:                            common.StringPtr("some random signal identity"),
		RequestId:                           common.StringPtr(uuid.New()),
		SignalInput:                         []byte("some random workflow input"),
		SignalName:                          common.StringPtr("some random signal name"),
	}
}

func (s *dcRedirectionHandlerSuite) getStartRequest() *shared.StartWorkflowExecutionRequest {
	workflowType := "some random workflow type"
	taskList := "some random task list"
	return &shared.StartWorkflowExecutionRequest{
		Domain:                              common.StringPtr("some random domain name"),
		WorkflowId:                          common.StringPtr("some random workflow ID"),
		WorkflowType:                        &shared.WorkflowType{Name: common.StringPtr(workflowType)},
		TaskList:                            &shared.TaskList{Name: common.StringPtr(taskList)},
		Input:                               []byte("some random workflow input"),
		ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(123),
		TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(234),
		Identity:                            common.StringPtr("some random signal identity"),
		RequestId:                           common.StringPtr(uuid.New()),
	}
}
