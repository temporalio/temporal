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
	"encoding/json"
	"errors"
	"github.com/uber/cadence/common/cluster"
	"log"
	"os"
	"testing"
	"time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"github.com/uber-common/bark"
	"github.com/uber-go/tally"

	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/client"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/blobstore"
	"github.com/uber/cadence/common/blobstore/blob"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/messaging"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/mocks"
	"github.com/uber/cadence/common/persistence"
	cs "github.com/uber/cadence/common/service"
	dc "github.com/uber/cadence/common/service/dynamicconfig"
	"github.com/uber/cadence/service/worker/sysworkflow"
)

type (
	workflowHandlerSuite struct {
		suite.Suite
		logger              bark.Logger
		mockClusterMetadata *mocks.ClusterMetadata
		mockProducer        *mocks.KafkaProducer
		mockMetricClient    metrics.Client
		mockMessagingClient messaging.Client
		mockMetadataMgr     *mocks.MetadataManager
		mockHistoryMgr      *mocks.HistoryManager
		mockHistoryV2Mgr    *mocks.HistoryV2Manager
		mockVisibilityMgr   *mocks.VisibilityManager
		mockDomainCache     *cache.DomainCacheMock
		mockClientBean      *client.MockClientBean
		mockService         cs.Service
		mockBlobstoreClient *mocks.BlobstoreClient
	}
)

func TestWorkflowHandlerSuite(t *testing.T) {
	s := new(workflowHandlerSuite)
	suite.Run(t, s)
}

func (s *workflowHandlerSuite) SetupSuite() {
	if testing.Verbose() {
		log.SetOutput(os.Stdout)
	}
}

func (s *workflowHandlerSuite) TearDownSuite() {

}

func (s *workflowHandlerSuite) SetupTest() {
	s.logger = bark.NewNopLogger()
	s.mockClusterMetadata = &mocks.ClusterMetadata{}
	s.mockProducer = &mocks.KafkaProducer{}
	s.mockMetricClient = metrics.NewClient(tally.NoopScope, metrics.Frontend)
	s.mockMessagingClient = mocks.NewMockMessagingClient(s.mockProducer, nil)
	s.mockMetadataMgr = &mocks.MetadataManager{}
	s.mockHistoryMgr = &mocks.HistoryManager{}
	s.mockHistoryV2Mgr = &mocks.HistoryV2Manager{}
	s.mockVisibilityMgr = &mocks.VisibilityManager{}
	s.mockClientBean = &client.MockClientBean{}
	s.mockService = cs.NewTestService(s.mockClusterMetadata, s.mockMessagingClient, s.mockMetricClient, s.mockClientBean, s.logger)
	s.mockBlobstoreClient = &mocks.BlobstoreClient{}
}

func (s *workflowHandlerSuite) TearDownTest() {
	s.mockProducer.AssertExpectations(s.T())
	s.mockMetadataMgr.AssertExpectations(s.T())
	s.mockHistoryMgr.AssertExpectations(s.T())
	s.mockHistoryV2Mgr.AssertExpectations(s.T())
	s.mockVisibilityMgr.AssertExpectations(s.T())
	s.mockClientBean.AssertExpectations(s.T())
	s.mockBlobstoreClient.AssertExpectations(s.T())
}

func (s *workflowHandlerSuite) TestMergeDomainData_Overriding() {
	wh := &WorkflowHandler{}
	out := wh.mergeDomainData(
		map[string]string{
			"k0": "v0",
		},
		map[string]string{
			"k0": "v2",
		},
	)

	assert.Equal(s.T(), map[string]string{
		"k0": "v2",
	}, out)
}

func (s *workflowHandlerSuite) TestMergeDomainData_Adding() {
	wh := &WorkflowHandler{}
	out := wh.mergeDomainData(
		map[string]string{
			"k0": "v0",
		},
		map[string]string{
			"k1": "v2",
		},
	)

	assert.Equal(s.T(), map[string]string{
		"k0": "v0",
		"k1": "v2",
	}, out)
}

func (s *workflowHandlerSuite) TestMergeDomainData_Merging() {
	wh := &WorkflowHandler{}
	out := wh.mergeDomainData(
		map[string]string{
			"k0": "v0",
		},
		map[string]string{
			"k0": "v1",
			"k1": "v2",
		},
	)

	assert.Equal(s.T(), map[string]string{
		"k0": "v1",
		"k1": "v2",
	}, out)
}

func (s *workflowHandlerSuite) TestMergeDomainData_Nil() {
	wh := &WorkflowHandler{}
	out := wh.mergeDomainData(
		nil,
		map[string]string{
			"k0": "v1",
			"k1": "v2",
		},
	)

	assert.Equal(s.T(), map[string]string{
		"k0": "v1",
		"k1": "v2",
	}, out)
}

func (s *workflowHandlerSuite) getWorkflowHandler(config *Config) *WorkflowHandler {
	return NewWorkflowHandler(s.mockService, config, s.mockMetadataMgr, s.mockHistoryMgr,
		s.mockHistoryV2Mgr, s.mockVisibilityMgr, s.mockProducer, s.mockBlobstoreClient)
}

func (s *workflowHandlerSuite) TestDisableListVisibilityByFilter() {
	domain := "test-domain"
	domainID := uuid.New()
	config := s.newConfig()
	config.DisableListVisibilityByFilter = dc.GetBoolPropertyFnFilteredByDomain(true)

	wh := s.getWorkflowHandler(config)
	mockDomainCache := &cache.DomainCacheMock{}
	wh.metricsClient = wh.Service.GetMetricsClient()
	wh.domainCache = mockDomainCache
	wh.startWG.Done()

	mockDomainCache.On("GetDomainID", mock.Anything).Return(domainID, nil)

	// test list open by wid
	listRequest := &shared.ListOpenWorkflowExecutionsRequest{
		Domain: common.StringPtr(domain),
		StartTimeFilter: &shared.StartTimeFilter{
			EarliestTime: common.Int64Ptr(0),
			LatestTime:   common.Int64Ptr(time.Now().UnixNano()),
		},
		ExecutionFilter: &shared.WorkflowExecutionFilter{
			WorkflowId: common.StringPtr("wid"),
		},
	}
	_, err := wh.ListOpenWorkflowExecutions(context.Background(), listRequest)
	assert.Error(s.T(), err)
	assert.Equal(s.T(), errNoPermission, err)

	// test list open by workflow type
	listRequest.ExecutionFilter = nil
	listRequest.TypeFilter = &shared.WorkflowTypeFilter{
		Name: common.StringPtr("workflow-type"),
	}
	_, err = wh.ListOpenWorkflowExecutions(context.Background(), listRequest)
	assert.Error(s.T(), err)
	assert.Equal(s.T(), errNoPermission, err)

	// test list close by wid
	listRequest2 := &shared.ListClosedWorkflowExecutionsRequest{
		Domain: common.StringPtr(domain),
		StartTimeFilter: &shared.StartTimeFilter{
			EarliestTime: common.Int64Ptr(0),
			LatestTime:   common.Int64Ptr(time.Now().UnixNano()),
		},
		ExecutionFilter: &shared.WorkflowExecutionFilter{
			WorkflowId: common.StringPtr("wid"),
		},
	}
	_, err = wh.ListClosedWorkflowExecutions(context.Background(), listRequest2)
	assert.Error(s.T(), err)
	assert.Equal(s.T(), errNoPermission, err)

	// test list close by workflow type
	listRequest2.ExecutionFilter = nil
	listRequest2.TypeFilter = &shared.WorkflowTypeFilter{
		Name: common.StringPtr("workflow-type"),
	}
	_, err = wh.ListClosedWorkflowExecutions(context.Background(), listRequest2)
	assert.Error(s.T(), err)
	assert.Equal(s.T(), errNoPermission, err)

	// test list close by workflow status
	listRequest2.TypeFilter = nil
	failedStatus := shared.WorkflowExecutionCloseStatusFailed
	listRequest2.StatusFilter = &failedStatus
	_, err = wh.ListClosedWorkflowExecutions(context.Background(), listRequest2)
	assert.Error(s.T(), err)
	assert.Equal(s.T(), errNoPermission, err)
}

func (s *workflowHandlerSuite) TestStartWorkflowExecution_Failed_RequestIdNotSet() {
	config := s.newConfig()
	config.RPS = dc.GetIntPropertyFn(10)
	wh := s.getWorkflowHandler(config)
	wh.metricsClient = wh.Service.GetMetricsClient()
	wh.startWG.Done()

	startWorkflowExecutionRequest := &shared.StartWorkflowExecutionRequest{
		Domain:     common.StringPtr("test-domain"),
		WorkflowId: common.StringPtr("workflow-id"),
		WorkflowType: &shared.WorkflowType{
			Name: common.StringPtr("workflow-type"),
		},
		TaskList: &shared.TaskList{
			Name: common.StringPtr("task-list"),
		},
		ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(1),
		TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
		RetryPolicy: &shared.RetryPolicy{
			InitialIntervalInSeconds:    common.Int32Ptr(1),
			BackoffCoefficient:          common.Float64Ptr(2),
			MaximumIntervalInSeconds:    common.Int32Ptr(2),
			MaximumAttempts:             common.Int32Ptr(1),
			ExpirationIntervalInSeconds: common.Int32Ptr(1),
		},
	}
	_, err := wh.StartWorkflowExecution(context.Background(), startWorkflowExecutionRequest)
	assert.Error(s.T(), err)
	assert.Equal(s.T(), errRequestIDNotSet, err)
}

func (s *workflowHandlerSuite) TestStartWorkflowExecution_Failed_StartRequestNotSet() {
	config := s.newConfig()
	config.RPS = dc.GetIntPropertyFn(10)
	wh := s.getWorkflowHandler(config)
	wh.metricsClient = wh.Service.GetMetricsClient()
	wh.startWG.Done()

	_, err := wh.StartWorkflowExecution(context.Background(), nil)
	assert.Error(s.T(), err)
	assert.Equal(s.T(), errRequestNotSet, err)
}

func (s *workflowHandlerSuite) TestStartWorkflowExecution_Failed_DomainNotSet() {
	config := s.newConfig()
	config.RPS = dc.GetIntPropertyFn(10)
	wh := s.getWorkflowHandler(config)
	wh.metricsClient = wh.Service.GetMetricsClient()
	wh.startWG.Done()

	startWorkflowExecutionRequest := &shared.StartWorkflowExecutionRequest{
		WorkflowId: common.StringPtr("workflow-id"),
		WorkflowType: &shared.WorkflowType{
			Name: common.StringPtr("workflow-type"),
		},
		TaskList: &shared.TaskList{
			Name: common.StringPtr("task-list"),
		},
		ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(1),
		TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
		RetryPolicy: &shared.RetryPolicy{
			InitialIntervalInSeconds:    common.Int32Ptr(1),
			BackoffCoefficient:          common.Float64Ptr(2),
			MaximumIntervalInSeconds:    common.Int32Ptr(2),
			MaximumAttempts:             common.Int32Ptr(1),
			ExpirationIntervalInSeconds: common.Int32Ptr(1),
		},
		RequestId: common.StringPtr(uuid.New()),
	}
	_, err := wh.StartWorkflowExecution(context.Background(), startWorkflowExecutionRequest)
	assert.Error(s.T(), err)
	assert.Equal(s.T(), errDomainNotSet, err)
}

func (s *workflowHandlerSuite) TestStartWorkflowExecution_Failed_WorkflowIdNotSet() {
	config := s.newConfig()
	config.RPS = dc.GetIntPropertyFn(10)
	wh := s.getWorkflowHandler(config)
	wh.metricsClient = wh.Service.GetMetricsClient()
	wh.startWG.Done()

	startWorkflowExecutionRequest := &shared.StartWorkflowExecutionRequest{
		Domain: common.StringPtr("test-domain"),
		WorkflowType: &shared.WorkflowType{
			Name: common.StringPtr("workflow-type"),
		},
		TaskList: &shared.TaskList{
			Name: common.StringPtr("task-list"),
		},
		ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(1),
		TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
		RetryPolicy: &shared.RetryPolicy{
			InitialIntervalInSeconds:    common.Int32Ptr(1),
			BackoffCoefficient:          common.Float64Ptr(2),
			MaximumIntervalInSeconds:    common.Int32Ptr(2),
			MaximumAttempts:             common.Int32Ptr(1),
			ExpirationIntervalInSeconds: common.Int32Ptr(1),
		},
		RequestId: common.StringPtr(uuid.New()),
	}
	_, err := wh.StartWorkflowExecution(context.Background(), startWorkflowExecutionRequest)
	assert.Error(s.T(), err)
	assert.Equal(s.T(), errWorkflowIDNotSet, err)
}

func (s *workflowHandlerSuite) TestStartWorkflowExecution_Failed_WorkflowTypeNotSet() {
	config := s.newConfig()
	config.RPS = dc.GetIntPropertyFn(10)
	wh := s.getWorkflowHandler(config)
	wh.metricsClient = wh.Service.GetMetricsClient()
	wh.startWG.Done()

	startWorkflowExecutionRequest := &shared.StartWorkflowExecutionRequest{
		Domain:     common.StringPtr("test-domain"),
		WorkflowId: common.StringPtr("workflow-id"),
		WorkflowType: &shared.WorkflowType{
			Name: common.StringPtr(""),
		},
		TaskList: &shared.TaskList{
			Name: common.StringPtr("task-list"),
		},
		ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(1),
		TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
		RetryPolicy: &shared.RetryPolicy{
			InitialIntervalInSeconds:    common.Int32Ptr(1),
			BackoffCoefficient:          common.Float64Ptr(2),
			MaximumIntervalInSeconds:    common.Int32Ptr(2),
			MaximumAttempts:             common.Int32Ptr(1),
			ExpirationIntervalInSeconds: common.Int32Ptr(1),
		},
		RequestId: common.StringPtr(uuid.New()),
	}
	_, err := wh.StartWorkflowExecution(context.Background(), startWorkflowExecutionRequest)
	assert.Error(s.T(), err)
	assert.Equal(s.T(), errWorkflowTypeNotSet, err)
}

func (s *workflowHandlerSuite) TestStartWorkflowExecution_Failed_TaskListNotSet() {
	config := s.newConfig()
	config.RPS = dc.GetIntPropertyFn(10)
	wh := s.getWorkflowHandler(config)
	wh.metricsClient = wh.Service.GetMetricsClient()
	wh.startWG.Done()

	startWorkflowExecutionRequest := &shared.StartWorkflowExecutionRequest{
		Domain:     common.StringPtr("test-domain"),
		WorkflowId: common.StringPtr("workflow-id"),
		WorkflowType: &shared.WorkflowType{
			Name: common.StringPtr("workflow-type"),
		},
		TaskList: &shared.TaskList{
			Name: common.StringPtr(""),
		},
		ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(1),
		TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
		RetryPolicy: &shared.RetryPolicy{
			InitialIntervalInSeconds:    common.Int32Ptr(1),
			BackoffCoefficient:          common.Float64Ptr(2),
			MaximumIntervalInSeconds:    common.Int32Ptr(2),
			MaximumAttempts:             common.Int32Ptr(1),
			ExpirationIntervalInSeconds: common.Int32Ptr(1),
		},
		RequestId: common.StringPtr(uuid.New()),
	}
	_, err := wh.StartWorkflowExecution(context.Background(), startWorkflowExecutionRequest)
	assert.Error(s.T(), err)
	assert.Equal(s.T(), errTaskListNotSet, err)
}

func (s *workflowHandlerSuite) TestStartWorkflowExecution_Failed_InvalidExecutionStartToCloseTimeout() {
	config := s.newConfig()
	config.RPS = dc.GetIntPropertyFn(10)
	wh := s.getWorkflowHandler(config)
	wh.metricsClient = wh.Service.GetMetricsClient()
	wh.startWG.Done()

	startWorkflowExecutionRequest := &shared.StartWorkflowExecutionRequest{
		Domain:     common.StringPtr("test-domain"),
		WorkflowId: common.StringPtr("workflow-id"),
		WorkflowType: &shared.WorkflowType{
			Name: common.StringPtr("workflow-type"),
		},
		TaskList: &shared.TaskList{
			Name: common.StringPtr("task-list"),
		},
		ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(0),
		TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
		RetryPolicy: &shared.RetryPolicy{
			InitialIntervalInSeconds:    common.Int32Ptr(1),
			BackoffCoefficient:          common.Float64Ptr(2),
			MaximumIntervalInSeconds:    common.Int32Ptr(2),
			MaximumAttempts:             common.Int32Ptr(1),
			ExpirationIntervalInSeconds: common.Int32Ptr(1),
		},
		RequestId: common.StringPtr(uuid.New()),
	}
	_, err := wh.StartWorkflowExecution(context.Background(), startWorkflowExecutionRequest)
	assert.Error(s.T(), err)
	assert.Equal(s.T(), errInvalidExecutionStartToCloseTimeoutSeconds, err)
}

func (s *workflowHandlerSuite) TestStartWorkflowExecution_Failed_InvalidTaskStartToCloseTimeout() {
	config := s.newConfig()
	config.RPS = dc.GetIntPropertyFn(10)
	wh := s.getWorkflowHandler(config)
	wh.metricsClient = wh.Service.GetMetricsClient()
	wh.startWG.Done()

	startWorkflowExecutionRequest := &shared.StartWorkflowExecutionRequest{
		Domain:     common.StringPtr("test-domain"),
		WorkflowId: common.StringPtr("workflow-id"),
		WorkflowType: &shared.WorkflowType{
			Name: common.StringPtr("workflow-type"),
		},
		TaskList: &shared.TaskList{
			Name: common.StringPtr("task-list"),
		},
		ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(1),
		TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(0),
		RetryPolicy: &shared.RetryPolicy{
			InitialIntervalInSeconds:    common.Int32Ptr(1),
			BackoffCoefficient:          common.Float64Ptr(2),
			MaximumIntervalInSeconds:    common.Int32Ptr(2),
			MaximumAttempts:             common.Int32Ptr(1),
			ExpirationIntervalInSeconds: common.Int32Ptr(1),
		},
		RequestId: common.StringPtr(uuid.New()),
	}
	_, err := wh.StartWorkflowExecution(context.Background(), startWorkflowExecutionRequest)
	assert.Error(s.T(), err)
	assert.Equal(s.T(), errInvalidTaskStartToCloseTimeoutSeconds, err)
}

func (s *workflowHandlerSuite) getWorkflowHandlerWithParams(mService cs.Service, config *Config,
	mMetadataManager persistence.MetadataManager, blobStore blobstore.Client) *WorkflowHandler {
	return NewWorkflowHandler(mService, config, mMetadataManager, s.mockHistoryMgr, s.mockHistoryV2Mgr,
		s.mockVisibilityMgr, s.mockProducer, blobStore)
}

func (s *workflowHandlerSuite) TestRegisterDomain_Success_EnabledWithNoBucket() {
	config := s.newConfig()
	clusterMetadata := &mocks.ClusterMetadata{}
	clusterMetadata.On("IsGlobalDomainEnabled").Return(false)
	clusterMetadata.On("GetCurrentClusterName").Return("active")
	clusterMetadata.On("ArchivalConfig").Return(cluster.NewArchivalConfig(cluster.ArchivalEnabled, "test-archival-bucket"))
	clusterMetadata.On("GetNextFailoverVersion", mock.Anything, mock.Anything).Return(int64(0))
	mMetadataManager := &mocks.MetadataManager{}
	mMetadataManager.On("GetDomain", mock.Anything).Return(nil, &shared.EntityNotExistsError{})
	mMetadataManager.On("CreateDomain", mock.Anything).Return(&persistence.CreateDomainResponse{
		ID: "test-id",
	}, nil)

	mService := cs.NewTestService(clusterMetadata, s.mockMessagingClient, s.mockMetricClient, s.mockClientBean, s.logger)
	wh := s.getWorkflowHandlerWithParams(mService, config, mMetadataManager, s.mockBlobstoreClient)
	wh.metricsClient = wh.Service.GetMetricsClient()
	wh.startWG.Done()

	req := registerDomainRequest(common.ArchivalStatusPtr(shared.ArchivalStatusEnabled), nil)
	err := wh.RegisterDomain(context.Background(), req)
	assert.NoError(s.T(), err)
}

func (s *workflowHandlerSuite) TestRegisterDomain_Success_EnabledWithBucket() {
	config := s.newConfig()
	clusterMetadata := &mocks.ClusterMetadata{}
	clusterMetadata.On("IsGlobalDomainEnabled").Return(false)
	clusterMetadata.On("GetCurrentClusterName").Return("active")
	clusterMetadata.On("ArchivalConfig").Return(cluster.NewArchivalConfig(cluster.ArchivalEnabled, "test-archival-bucket"))
	clusterMetadata.On("GetNextFailoverVersion", mock.Anything, mock.Anything).Return(int64(0))
	mMetadataManager := &mocks.MetadataManager{}
	mMetadataManager.On("GetDomain", mock.Anything).Return(nil, &shared.EntityNotExistsError{})
	mMetadataManager.On("CreateDomain", mock.Anything).Return(&persistence.CreateDomainResponse{
		ID: "test-id",
	}, nil)

	mService := cs.NewTestService(clusterMetadata, s.mockMessagingClient, s.mockMetricClient, s.mockClientBean, s.logger)
	wh := s.getWorkflowHandlerWithParams(mService, config, mMetadataManager, s.mockBlobstoreClient)
	wh.metricsClient = wh.Service.GetMetricsClient()
	wh.startWG.Done()

	req := registerDomainRequest(common.ArchivalStatusPtr(shared.ArchivalStatusEnabled), common.StringPtr("bucket-name"))
	err := wh.RegisterDomain(context.Background(), req)
	assert.NoError(s.T(), err)
}

func (s *workflowHandlerSuite) TestRegisterDomain_Success_ClusterNotConfiguredForArchival() {
	config := s.newConfig()
	clusterMetadata := &mocks.ClusterMetadata{}
	clusterMetadata.On("IsGlobalDomainEnabled").Return(false)
	clusterMetadata.On("GetCurrentClusterName").Return("active")
	clusterMetadata.On("ArchivalConfig").Return(cluster.NewArchivalConfig(cluster.ArchivalDisabled, ""))
	clusterMetadata.On("GetNextFailoverVersion", mock.Anything, mock.Anything).Return(int64(0))
	mMetadataManager := &mocks.MetadataManager{}
	mMetadataManager.On("GetDomain", mock.Anything).Return(nil, &shared.EntityNotExistsError{})
	mMetadataManager.On("CreateDomain", mock.Anything).Return(&persistence.CreateDomainResponse{
		ID: "test-id",
	}, nil)

	mService := cs.NewTestService(clusterMetadata, s.mockMessagingClient, s.mockMetricClient, s.mockClientBean, s.logger)
	wh := s.getWorkflowHandlerWithParams(mService, config, mMetadataManager, s.mockBlobstoreClient)
	wh.metricsClient = wh.Service.GetMetricsClient()
	wh.startWG.Done()

	req := registerDomainRequest(common.ArchivalStatusPtr(shared.ArchivalStatusEnabled), common.StringPtr("bucket-name"))
	err := wh.RegisterDomain(context.Background(), req)
	assert.NoError(s.T(), err)
}

func (s *workflowHandlerSuite) TestRegisterDomain_Success_NotEnabled() {
	config := s.newConfig()
	clusterMetadata := &mocks.ClusterMetadata{}
	clusterMetadata.On("IsGlobalDomainEnabled").Return(false)
	clusterMetadata.On("GetCurrentClusterName").Return("active")
	clusterMetadata.On("ArchivalConfig").Return(cluster.NewArchivalConfig(cluster.ArchivalEnabled, "test-archival-bucket"))
	clusterMetadata.On("GetNextFailoverVersion", mock.Anything, mock.Anything).Return(int64(0))
	mMetadataManager := &mocks.MetadataManager{}
	mMetadataManager.On("GetDomain", mock.Anything).Return(nil, &shared.EntityNotExistsError{})
	mMetadataManager.On("CreateDomain", mock.Anything).Return(&persistence.CreateDomainResponse{
		ID: "test-id",
	}, nil)

	mService := cs.NewTestService(clusterMetadata, s.mockMessagingClient, s.mockMetricClient, s.mockClientBean, s.logger)
	wh := s.getWorkflowHandlerWithParams(mService, config, mMetadataManager, s.mockBlobstoreClient)
	wh.metricsClient = wh.Service.GetMetricsClient()
	wh.startWG.Done()

	req := registerDomainRequest(nil, nil)
	err := wh.RegisterDomain(context.Background(), req)
	assert.NoError(s.T(), err)
}

func (s *workflowHandlerSuite) TestDescribeDomain_Success_ArchivalNeverEnabled() {
	config := s.newConfig()
	clusterMetadata := &mocks.ClusterMetadata{}
	clusterMetadata.On("ArchivalConfig").Return(cluster.NewArchivalConfig(cluster.ArchivalDisabled, ""))
	mMetadataManager := &mocks.MetadataManager{}
	mMetadataManager.On("GetDomain", mock.Anything).Return(persistenceGetDomainResponse("", shared.ArchivalStatusDisabled), nil)
	mBlobstore := &mocks.BlobstoreClient{}
	mService := cs.NewTestService(clusterMetadata, s.mockMessagingClient, s.mockMetricClient, s.mockClientBean, s.logger)
	wh := s.getWorkflowHandlerWithParams(mService, config, mMetadataManager, s.mockBlobstoreClient)
	wh.metricsClient = wh.Service.GetMetricsClient()
	wh.startWG.Done()

	req := &shared.DescribeDomainRequest{
		Name: common.StringPtr("test-domain"),
	}
	result, err := wh.DescribeDomain(context.Background(), req)

	assert.NoError(s.T(), err)
	assert.NotNil(s.T(), result)
	assert.NotNil(s.T(), result.Configuration)
	assert.Equal(s.T(), result.Configuration.GetArchivalStatus(), shared.ArchivalStatusDisabled)
	assert.Equal(s.T(), result.Configuration.GetArchivalBucketName(), "")
	assert.Equal(s.T(), result.Configuration.GetArchivalRetentionPeriodInDays(), int32(0))
	assert.Equal(s.T(), result.Configuration.GetArchivalBucketOwner(), "")
	mBlobstore.AssertNotCalled(s.T(), "BucketMetadata", mock.Anything, mock.Anything)
}

func (s *workflowHandlerSuite) TestDescribeDomain_Success_ArchivalEnabled() {
	config := s.newConfig()
	clusterMetadata := &mocks.ClusterMetadata{}
	clusterMetadata.On("ArchivalConfig").Return(cluster.NewArchivalConfig(cluster.ArchivalPaused, "default-bucket-name"))
	mMetadataManager := &mocks.MetadataManager{}
	mMetadataManager.On("GetDomain", mock.Anything).Return(persistenceGetDomainResponse("bucket-name", shared.ArchivalStatusEnabled), nil)
	mBlobstore := &mocks.BlobstoreClient{}
	mBlobstore.On("BucketMetadata", mock.Anything, mock.Anything).Return(bucketMetadataResponse("test-owner", 10), nil)
	mService := cs.NewTestService(clusterMetadata, s.mockMessagingClient, s.mockMetricClient, s.mockClientBean, s.logger)
	wh := s.getWorkflowHandlerWithParams(mService, config, mMetadataManager, mBlobstore)
	wh.metricsClient = wh.Service.GetMetricsClient()
	wh.startWG.Done()

	req := &shared.DescribeDomainRequest{
		Name: common.StringPtr("test-domain"),
	}
	result, err := wh.DescribeDomain(context.Background(), req)

	assert.NoError(s.T(), err)
	assert.NotNil(s.T(), result)
	assert.NotNil(s.T(), result.Configuration)
	assert.Equal(s.T(), result.Configuration.GetArchivalStatus(), shared.ArchivalStatusEnabled)
	assert.Equal(s.T(), result.Configuration.GetArchivalBucketName(), "bucket-name")
	assert.Equal(s.T(), result.Configuration.GetArchivalRetentionPeriodInDays(), int32(10))
	assert.Equal(s.T(), result.Configuration.GetArchivalBucketOwner(), "test-owner")
	mBlobstore.AssertCalled(s.T(), "BucketMetadata", mock.Anything, mock.Anything)
}

func (s *workflowHandlerSuite) TestDescribeDomain_Success_ArchivalDisabled() {
	config := s.newConfig()
	clusterMetadata := &mocks.ClusterMetadata{}
	clusterMetadata.On("ArchivalConfig").Return(cluster.NewArchivalConfig(cluster.ArchivalPaused, "default-bucket-name"))
	mMetadataManager := &mocks.MetadataManager{}
	mMetadataManager.On("GetDomain", mock.Anything).Return(persistenceGetDomainResponse("bucket-name", shared.ArchivalStatusDisabled), nil)
	mBlobstore := &mocks.BlobstoreClient{}
	mBlobstore.On("BucketMetadata", mock.Anything, mock.Anything).Return(bucketMetadataResponse("test-owner", 10), nil)
	mService := cs.NewTestService(clusterMetadata, s.mockMessagingClient, s.mockMetricClient, s.mockClientBean, s.logger)
	wh := s.getWorkflowHandlerWithParams(mService, config, mMetadataManager, mBlobstore)
	wh.metricsClient = wh.Service.GetMetricsClient()
	wh.startWG.Done()

	req := &shared.DescribeDomainRequest{
		Name: common.StringPtr("test-domain"),
	}
	result, err := wh.DescribeDomain(context.Background(), req)

	assert.NoError(s.T(), err)
	assert.NotNil(s.T(), result)
	assert.NotNil(s.T(), result.Configuration)
	assert.Equal(s.T(), result.Configuration.GetArchivalStatus(), shared.ArchivalStatusDisabled)
	assert.Equal(s.T(), result.Configuration.GetArchivalBucketName(), "bucket-name")
	assert.Equal(s.T(), result.Configuration.GetArchivalRetentionPeriodInDays(), int32(10))
	assert.Equal(s.T(), result.Configuration.GetArchivalBucketOwner(), "test-owner")
	mBlobstore.AssertCalled(s.T(), "BucketMetadata", mock.Anything, mock.Anything)
}

func (s *workflowHandlerSuite) TestDescribeDomain_Success_BlobstoreReturnsError() {
	config := s.newConfig()
	clusterMetadata := &mocks.ClusterMetadata{}
	clusterMetadata.On("ArchivalConfig").Return(cluster.NewArchivalConfig(cluster.ArchivalPaused, "default-bucket-name"))
	mMetadataManager := &mocks.MetadataManager{}
	mMetadataManager.On("GetDomain", mock.Anything).Return(persistenceGetDomainResponse("bucket-name", shared.ArchivalStatusDisabled), nil)
	mBlobstore := &mocks.BlobstoreClient{}
	mBlobstore.On("BucketMetadata", mock.Anything, mock.Anything).Return(nil, errors.New("blobstore error"))
	mService := cs.NewTestService(clusterMetadata, s.mockMessagingClient, s.mockMetricClient, s.mockClientBean, s.logger)
	wh := s.getWorkflowHandlerWithParams(mService, config, mMetadataManager, mBlobstore)
	wh.metricsClient = wh.Service.GetMetricsClient()
	wh.startWG.Done()

	req := &shared.DescribeDomainRequest{
		Name: common.StringPtr("test-domain"),
	}
	result, err := wh.DescribeDomain(context.Background(), req)

	assert.NoError(s.T(), err)
	assert.NotNil(s.T(), result)
	assert.NotNil(s.T(), result.Configuration)
	assert.Equal(s.T(), result.Configuration.GetArchivalStatus(), shared.ArchivalStatusDisabled)
	assert.Equal(s.T(), result.Configuration.GetArchivalBucketName(), "bucket-name")
	assert.Equal(s.T(), result.Configuration.GetArchivalRetentionPeriodInDays(), int32(0))
	assert.Equal(s.T(), result.Configuration.GetArchivalBucketOwner(), "")
	mBlobstore.AssertCalled(s.T(), "BucketMetadata", mock.Anything, mock.Anything)
}

func (s *workflowHandlerSuite) TestUpdateDomain_Failure_UpdateExistingBucketName() {
	config := s.newConfig()
	mMetadataManager := &mocks.MetadataManager{}
	mMetadataManager.On("GetMetadata").Return(&persistence.GetMetadataResponse{
		NotificationVersion: int64(0),
	}, nil)
	mMetadataManager.On("GetDomain", mock.Anything).Return(persistenceGetDomainResponse("bucket-name", shared.ArchivalStatusDisabled), nil)
	clusterMetadata := &mocks.ClusterMetadata{}
	clusterMetadata.On("IsGlobalDomainEnabled").Return(false)
	clusterMetadata.On("ArchivalConfig").Return(cluster.NewArchivalConfig(cluster.ArchivalPaused, "test-archival-bucket"))
	mService := cs.NewTestService(clusterMetadata, s.mockMessagingClient, s.mockMetricClient, s.mockClientBean, s.logger)
	wh := s.getWorkflowHandlerWithParams(mService, config, mMetadataManager, s.mockBlobstoreClient)
	wh.metricsClient = wh.Service.GetMetricsClient()
	wh.startWG.Done()

	updateReq := updateRequest(common.StringPtr("new-bucket"), common.ArchivalStatusPtr(shared.ArchivalStatusEnabled), nil, nil)
	_, err := wh.UpdateDomain(context.Background(), updateReq)
	assert.Error(s.T(), err)
	assert.Equal(s.T(), errBucketNameUpdate, err)
}

func (s *workflowHandlerSuite) TestUpdateDomain_Success_ArchivalEnabledToArchivalDisabledWithoutSettingBucket() {
	config := s.newConfig()
	mMetadataManager := &mocks.MetadataManager{}
	mMetadataManager.On("GetMetadata").Return(&persistence.GetMetadataResponse{
		NotificationVersion: int64(0),
	}, nil)
	mMetadataManager.On("GetDomain", mock.Anything).Return(persistenceGetDomainResponse("bucket-name", shared.ArchivalStatusEnabled), nil)
	mMetadataManager.On("UpdateDomain", mock.Anything).Return(nil)
	clusterMetadata := &mocks.ClusterMetadata{}
	clusterMetadata.On("IsGlobalDomainEnabled").Return(false)
	clusterMetadata.On("ArchivalConfig").Return(cluster.NewArchivalConfig(cluster.ArchivalEnabled, "test-archival-bucket"))
	mService := cs.NewTestService(clusterMetadata, s.mockMessagingClient, s.mockMetricClient, s.mockClientBean, s.logger)
	mBlobstore := &mocks.BlobstoreClient{}
	mBlobstore.On("BucketMetadata", mock.Anything, mock.Anything).Return(bucketMetadataResponse("test-owner", 10), nil)
	wh := s.getWorkflowHandlerWithParams(mService, config, mMetadataManager, mBlobstore)
	wh.metricsClient = wh.Service.GetMetricsClient()
	wh.startWG.Done()

	updateReq := updateRequest(nil, common.ArchivalStatusPtr(shared.ArchivalStatusDisabled), nil, nil)
	result, err := wh.UpdateDomain(context.Background(), updateReq)
	assert.NoError(s.T(), err)
	assert.NotNil(s.T(), result)
	assert.NotNil(s.T(), result.Configuration)
	assert.Equal(s.T(), result.Configuration.GetArchivalStatus(), shared.ArchivalStatusDisabled)
	assert.Equal(s.T(), result.Configuration.GetArchivalBucketName(), "bucket-name")
	assert.Equal(s.T(), result.Configuration.GetArchivalBucketOwner(), "test-owner")
	assert.Equal(s.T(), result.Configuration.GetArchivalRetentionPeriodInDays(), int32(10))
}

func (s *workflowHandlerSuite) TestUpdateDomain_Success_ClusterNotConfiguredForArchival() {
	config := s.newConfig()
	mMetadataManager := &mocks.MetadataManager{}
	mMetadataManager.On("GetMetadata").Return(&persistence.GetMetadataResponse{
		NotificationVersion: int64(0),
	}, nil)
	mMetadataManager.On("GetDomain", mock.Anything).Return(persistenceGetDomainResponse("bucket-name", shared.ArchivalStatusEnabled), nil)
	mMetadataManager.On("UpdateDomain", mock.Anything).Return(nil)
	clusterMetadata := &mocks.ClusterMetadata{}
	clusterMetadata.On("IsGlobalDomainEnabled").Return(false)
	clusterMetadata.On("ArchivalConfig").Return(cluster.NewArchivalConfig(cluster.ArchivalDisabled, ""))
	mService := cs.NewTestService(clusterMetadata, s.mockMessagingClient, s.mockMetricClient, s.mockClientBean, s.logger)
	wh := s.getWorkflowHandlerWithParams(mService, config, mMetadataManager, s.mockBlobstoreClient)
	wh.metricsClient = wh.Service.GetMetricsClient()
	wh.startWG.Done()

	updateReq := updateRequest(nil, common.ArchivalStatusPtr(shared.ArchivalStatusDisabled), nil, nil)
	result, err := wh.UpdateDomain(context.Background(), updateReq)
	assert.NoError(s.T(), err)
	assert.NotNil(s.T(), result)
	assert.NotNil(s.T(), result.Configuration)
	assert.Equal(s.T(), result.Configuration.GetArchivalStatus(), shared.ArchivalStatusEnabled)
	assert.Equal(s.T(), result.Configuration.GetArchivalBucketName(), "bucket-name")
	assert.Equal(s.T(), result.Configuration.GetArchivalBucketOwner(), "")
	assert.Equal(s.T(), result.Configuration.GetArchivalRetentionPeriodInDays(), int32(0))
}

func (s *workflowHandlerSuite) TestUpdateDomain_Success_ArchivalEnabledToArchivalDisabledWithSettingBucket() {
	config := s.newConfig()
	mMetadataManager := &mocks.MetadataManager{}
	mMetadataManager.On("GetMetadata").Return(&persistence.GetMetadataResponse{
		NotificationVersion: int64(0),
	}, nil)
	mMetadataManager.On("GetDomain", mock.Anything).Return(persistenceGetDomainResponse("bucket-name", shared.ArchivalStatusEnabled), nil)
	mMetadataManager.On("UpdateDomain", mock.Anything).Return(nil)
	clusterMetadata := &mocks.ClusterMetadata{}
	clusterMetadata.On("IsGlobalDomainEnabled").Return(false)
	clusterMetadata.On("ArchivalConfig").Return(cluster.NewArchivalConfig(cluster.ArchivalEnabled, "test-archival-bucket"))
	mService := cs.NewTestService(clusterMetadata, s.mockMessagingClient, s.mockMetricClient, s.mockClientBean, s.logger)
	mBlobstore := &mocks.BlobstoreClient{}
	mBlobstore.On("BucketMetadata", mock.Anything, mock.Anything).Return(bucketMetadataResponse("test-owner", 10), nil)
	wh := s.getWorkflowHandlerWithParams(mService, config, mMetadataManager, mBlobstore)
	wh.metricsClient = wh.Service.GetMetricsClient()
	wh.startWG.Done()

	updateReq := updateRequest(common.StringPtr("bucket-name"), common.ArchivalStatusPtr(shared.ArchivalStatusDisabled), nil, nil)
	result, err := wh.UpdateDomain(context.Background(), updateReq)
	assert.NoError(s.T(), err)
	assert.NotNil(s.T(), result)
	assert.NotNil(s.T(), result.Configuration)
	assert.Equal(s.T(), result.Configuration.GetArchivalStatus(), shared.ArchivalStatusDisabled)
	assert.Equal(s.T(), result.Configuration.GetArchivalBucketName(), "bucket-name")
	assert.Equal(s.T(), result.Configuration.GetArchivalBucketOwner(), "test-owner")
	assert.Equal(s.T(), result.Configuration.GetArchivalRetentionPeriodInDays(), int32(10))
}

func (s *workflowHandlerSuite) TestUpdateDomain_Success_ArchivalEnabledToEnabled() {
	config := s.newConfig()
	mMetadataManager := &mocks.MetadataManager{}
	mMetadataManager.On("GetMetadata").Return(&persistence.GetMetadataResponse{
		NotificationVersion: int64(0),
	}, nil)
	mMetadataManager.On("GetDomain", mock.Anything).Return(persistenceGetDomainResponse("bucket-name", shared.ArchivalStatusEnabled), nil)
	mMetadataManager.On("UpdateDomain", mock.Anything).Return(nil)
	clusterMetadata := &mocks.ClusterMetadata{}
	clusterMetadata.On("IsGlobalDomainEnabled").Return(false)
	clusterMetadata.On("ArchivalConfig").Return(cluster.NewArchivalConfig(cluster.ArchivalEnabled, "test-archival-bucket"))
	mService := cs.NewTestService(clusterMetadata, s.mockMessagingClient, s.mockMetricClient, s.mockClientBean, s.logger)
	mBlobstore := &mocks.BlobstoreClient{}
	mBlobstore.On("BucketMetadata", mock.Anything, mock.Anything).Return(bucketMetadataResponse("test-owner", 10), nil)
	wh := s.getWorkflowHandlerWithParams(mService, config, mMetadataManager, mBlobstore)
	wh.metricsClient = wh.Service.GetMetricsClient()
	wh.startWG.Done()

	updateReq := updateRequest(common.StringPtr("bucket-name"), common.ArchivalStatusPtr(shared.ArchivalStatusEnabled), nil, nil)
	result, err := wh.UpdateDomain(context.Background(), updateReq)
	assert.NoError(s.T(), err)
	assert.NotNil(s.T(), result)
	assert.NotNil(s.T(), result.Configuration)
	assert.Equal(s.T(), result.Configuration.GetArchivalStatus(), shared.ArchivalStatusEnabled)
	assert.Equal(s.T(), result.Configuration.GetArchivalBucketName(), "bucket-name")
	assert.Equal(s.T(), result.Configuration.GetArchivalBucketOwner(), "test-owner")
	assert.Equal(s.T(), result.Configuration.GetArchivalRetentionPeriodInDays(), int32(10))
}

func (s *workflowHandlerSuite) TestUpdateDomain_Success_ArchivalNeverEnabledToEnabled() {
	config := s.newConfig()
	mMetadataManager := &mocks.MetadataManager{}
	mMetadataManager.On("GetMetadata").Return(&persistence.GetMetadataResponse{
		NotificationVersion: int64(0),
	}, nil)
	mMetadataManager.On("GetDomain", mock.Anything).Return(persistenceGetDomainResponse("", shared.ArchivalStatusDisabled), nil)
	mMetadataManager.On("UpdateDomain", mock.Anything).Return(nil)
	clusterMetadata := &mocks.ClusterMetadata{}
	clusterMetadata.On("IsGlobalDomainEnabled").Return(false)
	clusterMetadata.On("ArchivalConfig").Return(cluster.NewArchivalConfig(cluster.ArchivalEnabled, "test-archival-bucket"))
	mService := cs.NewTestService(clusterMetadata, s.mockMessagingClient, s.mockMetricClient, s.mockClientBean, s.logger)
	mBlobstore := &mocks.BlobstoreClient{}
	mBlobstore.On("BucketMetadata", mock.Anything, mock.Anything).Return(bucketMetadataResponse("test-owner", 10), nil)
	wh := s.getWorkflowHandlerWithParams(mService, config, mMetadataManager, mBlobstore)
	wh.metricsClient = wh.Service.GetMetricsClient()
	wh.startWG.Done()

	updateReq := updateRequest(common.StringPtr("custom-bucket"), common.ArchivalStatusPtr(shared.ArchivalStatusEnabled), nil, nil)
	result, err := wh.UpdateDomain(context.Background(), updateReq)
	assert.NoError(s.T(), err)
	assert.NotNil(s.T(), result)
	assert.NotNil(s.T(), result.Configuration)
	assert.Equal(s.T(), shared.ArchivalStatusEnabled, *result.GetConfiguration().ArchivalStatus)
	assert.Equal(s.T(), "custom-bucket", result.GetConfiguration().GetArchivalBucketName())
}

func (s *workflowHandlerSuite) TestHistoryArchived() {
	wh := &WorkflowHandler{}
	getHistoryRequest := &shared.GetWorkflowExecutionHistoryRequest{}
	s.False(wh.historyArchived(context.Background(), getHistoryRequest, "test-domain"))

	getHistoryRequest = &shared.GetWorkflowExecutionHistoryRequest{
		Execution: &shared.WorkflowExecution{},
	}
	s.False(wh.historyArchived(context.Background(), getHistoryRequest, "test-domain"))

	mockHistoryClient := &mocks.HistoryClient{}
	mockHistoryClient.On("GetMutableState", mock.Anything, mock.Anything).Return(nil, nil).Once()
	wh = &WorkflowHandler{
		history: mockHistoryClient,
	}
	getHistoryRequest = &shared.GetWorkflowExecutionHistoryRequest{
		Execution: &shared.WorkflowExecution{
			WorkflowId: common.StringPtr("test-workflow-id"),
			RunId:      common.StringPtr("test-run-id"),
		},
	}
	s.False(wh.historyArchived(context.Background(), getHistoryRequest, "test-domain"))

	mockHistoryClient.On("GetMutableState", mock.Anything, mock.Anything).Return(nil, &shared.EntityNotExistsError{Message: "got archival indication error"}).Once()
	wh = &WorkflowHandler{
		history: mockHistoryClient,
	}
	getHistoryRequest = &shared.GetWorkflowExecutionHistoryRequest{
		Execution: &shared.WorkflowExecution{
			WorkflowId: common.StringPtr("test-workflow-id"),
			RunId:      common.StringPtr("test-run-id"),
		},
	}
	s.True(wh.historyArchived(context.Background(), getHistoryRequest, "test-domain"))

	mockHistoryClient.On("GetMutableState", mock.Anything, mock.Anything).Return(nil, errors.New("got non-archival indication error")).Once()
	wh = &WorkflowHandler{
		history: mockHistoryClient,
	}
	getHistoryRequest = &shared.GetWorkflowExecutionHistoryRequest{
		Execution: &shared.WorkflowExecution{
			WorkflowId: common.StringPtr("test-workflow-id"),
			RunId:      common.StringPtr("test-run-id"),
		},
	}
	s.False(wh.historyArchived(context.Background(), getHistoryRequest, "test-domain"))
}

func (s *workflowHandlerSuite) TestGetArchivedHistory_Failure_DomainCacheEntryError() {
	config := s.newConfig()
	mMetadataManager := &mocks.MetadataManager{}
	mMetadataManager.On("GetDomain", mock.Anything).Return(nil, errors.New("error getting domain"))
	wh := s.getWorkflowHandlerWithParams(s.mockService, config, mMetadataManager, s.mockBlobstoreClient)
	wh.metricsClient = wh.Service.GetMetricsClient()
	wh.startWG.Done()
	resp, err := wh.getArchivedHistory(context.Background(), getHistoryRequest(nil), "test-domain-id", 0)
	s.Nil(resp)
	s.Error(err)
}

func (s *workflowHandlerSuite) TestGetArchivedHistory_Failure_ArchivalBucketEmpty() {
	config := s.newConfig()
	mMetadataManager := &mocks.MetadataManager{}
	mMetadataManager.On("GetDomain", mock.Anything).Return(persistenceGetDomainResponse("", shared.ArchivalStatusDisabled), nil)
	clusterMetadata := &mocks.ClusterMetadata{}
	clusterMetadata.On("IsGlobalDomainEnabled").Return(false)
	mService := cs.NewTestService(clusterMetadata, s.mockMessagingClient, s.mockMetricClient, s.mockClientBean, s.logger)
	wh := s.getWorkflowHandlerWithParams(mService, config, mMetadataManager, s.mockBlobstoreClient)
	wh.metricsClient = wh.Service.GetMetricsClient()
	wh.startWG.Done()
	resp, err := wh.getArchivedHistory(context.Background(), getHistoryRequest(nil), "test-domain-id", 0)
	s.Nil(resp)
	s.Error(err)
}

func (s *workflowHandlerSuite) TestGetArchivedHistory_Failure_InvalidPageToken() {
	config := s.newConfig()
	mMetadataManager := &mocks.MetadataManager{}
	mMetadataManager.On("GetDomain", mock.Anything).Return(persistenceGetDomainResponse("test-bucket", shared.ArchivalStatusEnabled), nil)
	clusterMetadata := &mocks.ClusterMetadata{}
	clusterMetadata.On("IsGlobalDomainEnabled").Return(false)
	mService := cs.NewTestService(clusterMetadata, s.mockMessagingClient, s.mockMetricClient, s.mockClientBean, s.logger)
	wh := s.getWorkflowHandlerWithParams(mService, config, mMetadataManager, s.mockBlobstoreClient)
	wh.metricsClient = wh.Service.GetMetricsClient()
	wh.startWG.Done()
	resp, err := wh.getArchivedHistory(context.Background(), getHistoryRequest([]byte{3, 4, 5, 1}), "test-domain-id", 0)
	s.Nil(resp)
	s.Error(err)
	s.Equal(errInvalidNextArchivalPageToken, err)
}

func (s *workflowHandlerSuite) TestGetArchivedHistory_Failure_InvalidBlobKey() {
	config := s.newConfig()
	mMetadataManager := &mocks.MetadataManager{}
	mMetadataManager.On("GetDomain", mock.Anything).Return(persistenceGetDomainResponse("test-bucket", shared.ArchivalStatusEnabled), nil)
	clusterMetadata := &mocks.ClusterMetadata{}
	clusterMetadata.On("IsGlobalDomainEnabled").Return(false)
	mService := cs.NewTestService(clusterMetadata, s.mockMessagingClient, s.mockMetricClient, s.mockClientBean, s.logger)
	wh := s.getWorkflowHandlerWithParams(mService, config, mMetadataManager, s.mockBlobstoreClient)
	wh.metricsClient = wh.Service.GetMetricsClient()
	wh.startWG.Done()
	getHistoryRequest := getHistoryRequest(nil)
	getHistoryRequest.Execution.WorkflowId = common.StringPtr("")
	resp, err := wh.getArchivedHistory(context.Background(), getHistoryRequest, "test-domain-id", 0)
	s.Nil(resp)
	s.Error(err)
}

func (s *workflowHandlerSuite) TestGetArchivedHistory_Failure_FailedToDownload() {
	config := s.newConfig()
	mMetadataManager := &mocks.MetadataManager{}
	mMetadataManager.On("GetDomain", mock.Anything).Return(persistenceGetDomainResponse("test-bucket", shared.ArchivalStatusEnabled), nil)
	clusterMetadata := &mocks.ClusterMetadata{}
	clusterMetadata.On("IsGlobalDomainEnabled").Return(false)
	mService := cs.NewTestService(clusterMetadata, s.mockMessagingClient, s.mockMetricClient, s.mockClientBean, s.logger)
	mBlobstore := &mocks.BlobstoreClient{}
	mBlobstore.On("Download", mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("failed to download blob"))
	wh := s.getWorkflowHandlerWithParams(mService, config, mMetadataManager, mBlobstore)
	wh.metricsClient = wh.Service.GetMetricsClient()
	wh.startWG.Done()
	resp, err := wh.getArchivedHistory(context.Background(), getHistoryRequest(nil), "test-domain-id", 0)
	s.Nil(resp)
	s.Error(err)
}

func (s *workflowHandlerSuite) TestGetArchivedHistory_Success_GetFirstPage() {
	config := s.newConfig()
	mMetadataManager := &mocks.MetadataManager{}
	mMetadataManager.On("GetDomain", mock.Anything).Return(persistenceGetDomainResponse("test-bucket", shared.ArchivalStatusEnabled), nil)
	clusterMetadata := &mocks.ClusterMetadata{}
	clusterMetadata.On("IsGlobalDomainEnabled").Return(false)
	mService := cs.NewTestService(clusterMetadata, s.mockMessagingClient, s.mockMetricClient, s.mockClientBean, s.logger)
	mBlobstore := &mocks.BlobstoreClient{}
	unwrappedBlob := &sysworkflow.HistoryBlob{
		Header: &sysworkflow.HistoryBlobHeader{
			CurrentPageToken: common.IntPtr(common.FirstBlobPageToken),
			NextPageToken:    common.IntPtr(common.FirstBlobPageToken + 1),
		},
		Body: &shared.History{},
	}
	bytes, err := json.Marshal(unwrappedBlob)
	s.NoError(err)
	historyBlob, err := blob.Wrap(blob.NewBlob(bytes, map[string]string{}), blob.JSONEncoded())
	s.NoError(err)
	mBlobstore.On("Download", mock.Anything, mock.Anything, mock.Anything).Return(historyBlob, nil)
	wh := s.getWorkflowHandlerWithParams(mService, config, mMetadataManager, mBlobstore)
	wh.metricsClient = wh.Service.GetMetricsClient()
	wh.startWG.Done()
	resp, err := wh.getArchivedHistory(context.Background(), getHistoryRequest(nil), "test-domain-id", 0)
	s.NoError(err)
	s.NotNil(resp)
	s.NotNil(resp.History)
	expectedNextPageToken := &getHistoryContinuationTokenArchival{
		BlobstorePageToken: 2,
	}
	expectedSerializedNextPageToken, err := serializeHistoryTokenArchival(expectedNextPageToken)
	s.NoError(err)
	s.Equal(expectedSerializedNextPageToken, resp.NextPageToken)
}

func (s *workflowHandlerSuite) TestGetArchivedHistory_Success_GetLastPage() {
	config := s.newConfig()
	mMetadataManager := &mocks.MetadataManager{}
	mMetadataManager.On("GetDomain", mock.Anything).Return(persistenceGetDomainResponse("test-bucket", shared.ArchivalStatusEnabled), nil)
	clusterMetadata := &mocks.ClusterMetadata{}
	clusterMetadata.On("IsGlobalDomainEnabled").Return(false)
	mService := cs.NewTestService(clusterMetadata, s.mockMessagingClient, s.mockMetricClient, s.mockClientBean, s.logger)
	mBlobstore := &mocks.BlobstoreClient{}
	unwrappedBlob := &sysworkflow.HistoryBlob{
		Header: &sysworkflow.HistoryBlobHeader{
			CurrentPageToken: common.IntPtr(5),
			NextPageToken:    common.IntPtr(common.LastBlobNextPageToken),
		},
		Body: &shared.History{},
	}
	bytes, err := json.Marshal(unwrappedBlob)
	s.NoError(err)
	historyBlob, err := blob.Wrap(blob.NewBlob(bytes, map[string]string{}), blob.JSONEncoded())
	s.NoError(err)
	mBlobstore.On("Download", mock.Anything, mock.Anything, mock.Anything).Return(historyBlob, nil)
	wh := s.getWorkflowHandlerWithParams(mService, config, mMetadataManager, mBlobstore)
	wh.metricsClient = wh.Service.GetMetricsClient()
	wh.startWG.Done()
	resp, err := wh.getArchivedHistory(context.Background(), getHistoryRequest(nil), "test-domain-id", 0)
	s.NoError(err)
	s.NotNil(resp)
	s.NotNil(resp.History)
	s.Nil(resp.NextPageToken)
}

func (s *workflowHandlerSuite) newConfig() *Config {
	return NewConfig(dc.NewCollection(dc.NewNopClient(), s.logger), false)
}

func bucketMetadataResponse(owner string, retentionDays int) *blobstore.BucketMetadataResponse {
	return &blobstore.BucketMetadataResponse{
		Owner:         owner,
		RetentionDays: retentionDays,
	}
}

func updateRequest(archivalBucketName *string, archivalStatus *shared.ArchivalStatus, archivalRetentionDays *int32, archivalOwner *string) *shared.UpdateDomainRequest {
	return &shared.UpdateDomainRequest{
		Name: common.StringPtr("test-name"),
		Configuration: &shared.DomainConfiguration{
			ArchivalBucketName:            archivalBucketName,
			ArchivalStatus:                archivalStatus,
			ArchivalRetentionPeriodInDays: archivalRetentionDays,
			ArchivalBucketOwner:           archivalOwner,
		},
	}
}

func persistenceGetDomainResponse(archivalBucket string, archivalStatus shared.ArchivalStatus) *persistence.GetDomainResponse {
	return &persistence.GetDomainResponse{
		Info: &persistence.DomainInfo{
			ID:          "test-id",
			Name:        "test-name",
			Status:      0,
			Description: "test-description",
			OwnerEmail:  "test-owner-email",
			Data:        make(map[string]string),
		},
		Config: &persistence.DomainConfig{
			Retention:      0,
			EmitMetric:     true,
			ArchivalBucket: archivalBucket,
			ArchivalStatus: archivalStatus,
		},
		ReplicationConfig: &persistence.DomainReplicationConfig{
			ActiveClusterName: "active",
			Clusters: []*persistence.ClusterReplicationConfig{
				{
					ClusterName: "active",
				},
				{
					ClusterName: "standby",
				},
			},
		},
		IsGlobalDomain:              false,
		ConfigVersion:               0,
		FailoverVersion:             0,
		FailoverNotificationVersion: 0,
		NotificationVersion:         0,
		TableVersion:                persistence.DomainTableVersionV1,
	}
}

func registerDomainRequest(archivalStatus *shared.ArchivalStatus, bucketName *string) *shared.RegisterDomainRequest {
	return &shared.RegisterDomainRequest{
		Name:                                   common.StringPtr("test-domain"),
		Description:                            common.StringPtr("test-description"),
		OwnerEmail:                             common.StringPtr("test-owner-email"),
		WorkflowExecutionRetentionPeriodInDays: common.Int32Ptr(10),
		EmitMetric:                             common.BoolPtr(true),
		Clusters: []*shared.ClusterReplicationConfiguration{
			{
				ClusterName: common.StringPtr("active"),
			},
			{
				ClusterName: common.StringPtr("standby"),
			},
		},
		ActiveClusterName:  common.StringPtr("active"),
		Data:               make(map[string]string),
		SecurityToken:      common.StringPtr("token"),
		ArchivalStatus:     archivalStatus,
		ArchivalBucketName: bucketName,
	}
}

func getHistoryRequest(nextPageToken []byte) *shared.GetWorkflowExecutionHistoryRequest {
	return &shared.GetWorkflowExecutionHistoryRequest{
		Execution: &shared.WorkflowExecution{
			WorkflowId: common.StringPtr("test-workflow-id"),
			RunId:      common.StringPtr("test-run-id"),
		},
		NextPageToken: nextPageToken,
	}
}
