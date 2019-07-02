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
	"testing"
	"time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	"github.com/uber/cadence/.gen/go/shared"
	gen "github.com/uber/cadence/.gen/go/shared"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/client"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/blobstore/blob"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/loggerimpl"
	"github.com/uber/cadence/common/messaging"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/mocks"
	"github.com/uber/cadence/common/persistence"
	cs "github.com/uber/cadence/common/service"
	dc "github.com/uber/cadence/common/service/dynamicconfig"
	"github.com/uber/cadence/service/worker/archiver"
)

const (
	numHistoryShards = 10

	testArchivalBucket = "test-bucket"
	testWorkflowID     = "test-workflow-id"
	testRunID          = "test-run-id"
)

type (
	workflowHandlerSuite struct {
		suite.Suite
		testDomain          string
		testDomainID        string
		logger              log.Logger
		config              *Config
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
}

func (s *workflowHandlerSuite) TearDownSuite() {

}

func (s *workflowHandlerSuite) SetupTest() {
	s.testDomain = "test-domain"
	s.testDomainID = "e4f90ec0-1313-45be-9877-8aa41f72a45a"
	s.logger = loggerimpl.NewNopLogger()
	s.mockClusterMetadata = &mocks.ClusterMetadata{}
	s.mockProducer = &mocks.KafkaProducer{}
	s.mockMetricClient = metrics.NewClient(tally.NoopScope, metrics.Frontend)
	s.mockMessagingClient = mocks.NewMockMessagingClient(s.mockProducer, nil)
	s.mockMetadataMgr = &mocks.MetadataManager{}
	s.mockHistoryMgr = &mocks.HistoryManager{}
	s.mockHistoryV2Mgr = &mocks.HistoryV2Manager{}
	s.mockVisibilityMgr = &mocks.VisibilityManager{}
	s.mockDomainCache = &cache.DomainCacheMock{}
	s.mockClientBean = &client.MockClientBean{}
	s.mockService = cs.NewTestService(s.mockClusterMetadata, s.mockMessagingClient, s.mockMetricClient, s.mockClientBean)
	s.mockBlobstoreClient = &mocks.BlobstoreClient{}
}

func (s *workflowHandlerSuite) TearDownTest() {
	s.mockProducer.AssertExpectations(s.T())
	s.mockMetadataMgr.AssertExpectations(s.T())
	s.mockHistoryMgr.AssertExpectations(s.T())
	s.mockHistoryV2Mgr.AssertExpectations(s.T())
	s.mockVisibilityMgr.AssertExpectations(s.T())
	s.mockDomainCache.AssertExpectations(s.T())
	s.mockClientBean.AssertExpectations(s.T())
	s.mockBlobstoreClient.AssertExpectations(s.T())
}

func (s *workflowHandlerSuite) getWorkflowHandler(config *Config) *WorkflowHandler {
	return NewWorkflowHandler(s.mockService, config, s.mockMetadataMgr, s.mockHistoryMgr,
		s.mockHistoryV2Mgr, s.mockVisibilityMgr, s.mockProducer, s.mockBlobstoreClient, nil, nil)
}

func (s *workflowHandlerSuite) getWorkflowHandlerHelper() *WorkflowHandler {
	s.config = s.newConfig()
	wh := s.getWorkflowHandler(s.config)
	wh.metricsClient = wh.Service.GetMetricsClient()
	wh.domainCache = s.mockDomainCache
	wh.visibilityMgr = s.mockVisibilityMgr
	wh.startWG.Done()
	return wh
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

func (s *workflowHandlerSuite) TestPollForTask_Failed_ContextTimeoutTooShort() {
	config := s.newConfig()
	wh := s.getWorkflowHandler(config)
	wh.metricsClient = wh.Service.GetMetricsClient()
	wh.startWG.Done()

	bgCtx := context.Background()
	_, err := wh.PollForDecisionTask(bgCtx, &shared.PollForDecisionTaskRequest{})
	assert.Error(s.T(), err)
	assert.Equal(s.T(), common.ErrContextTimeoutNotSet, err)

	_, err = wh.PollForActivityTask(bgCtx, &shared.PollForActivityTaskRequest{})
	assert.Error(s.T(), err)
	assert.Equal(s.T(), common.ErrContextTimeoutNotSet, err)

	shortCtx, cancel := context.WithTimeout(bgCtx, common.MinLongPollTimeout-time.Millisecond)
	defer cancel()

	_, err = wh.PollForDecisionTask(shortCtx, &shared.PollForDecisionTaskRequest{})
	assert.Error(s.T(), err)
	assert.Equal(s.T(), common.ErrContextTimeoutTooShort, err)

	_, err = wh.PollForActivityTask(shortCtx, &shared.PollForActivityTaskRequest{})
	assert.Error(s.T(), err)
	assert.Equal(s.T(), common.ErrContextTimeoutTooShort, err)
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
	mMetadataManager persistence.MetadataManager, blobStore *mocks.BlobstoreClient) *WorkflowHandler {
	s.mockBlobstoreClient = blobStore
	return NewWorkflowHandler(mService, config, mMetadataManager, s.mockHistoryMgr, s.mockHistoryV2Mgr,
		s.mockVisibilityMgr, s.mockProducer, blobStore, nil, nil)
}

func (s *workflowHandlerSuite) TestRegisterDomain_Failure_BucketNotExists() {
	config := s.newConfig()
	clusterMetadata := &mocks.ClusterMetadata{}
	clusterMetadata.On("IsGlobalDomainEnabled").Return(false)
	clusterMetadata.On("GetAllClusterInfo").Return(cluster.TestAllClusterInfo)
	clusterMetadata.On("GetCurrentClusterName").Return(cluster.TestCurrentClusterName)
	clusterMetadata.On("ArchivalConfig").Return(cluster.NewArchivalConfig(cluster.ArchivalEnabled, "test-archival-bucket", true))
	clusterMetadata.On("GetNextFailoverVersion", mock.Anything, mock.Anything).Return(int64(0))
	mMetadataManager := &mocks.MetadataManager{}
	mMetadataManager.On("GetDomain", mock.Anything).Return(nil, &shared.EntityNotExistsError{})
	mMetadataManager.On("CreateDomain", mock.Anything).Return(&persistence.CreateDomainResponse{
		ID: "test-id",
	}, nil)
	mBlobstore := &mocks.BlobstoreClient{}
	mBlobstore.On("BucketExists", mock.Anything, mock.Anything).Return(false, nil)

	mService := cs.NewTestService(clusterMetadata, s.mockMessagingClient, s.mockMetricClient, s.mockClientBean)
	wh := s.getWorkflowHandlerWithParams(mService, config, mMetadataManager, mBlobstore)
	wh.metricsClient = wh.Service.GetMetricsClient()
	wh.startWG.Done()

	req := registerDomainRequest(common.ArchivalStatusPtr(shared.ArchivalStatusEnabled), common.StringPtr("not-exists-bucket"))
	err := wh.RegisterDomain(context.Background(), req)
	assert.Error(s.T(), err)
	assert.Equal(s.T(), err, errBucketDoesNotExist)
}

func (s *workflowHandlerSuite) TestRegisterDomain_Success_EnabledWithNoBucket() {
	config := s.newConfig()
	clusterMetadata := &mocks.ClusterMetadata{}
	clusterMetadata.On("IsGlobalDomainEnabled").Return(false)
	clusterMetadata.On("GetAllClusterInfo").Return(cluster.TestAllClusterInfo)
	clusterMetadata.On("GetCurrentClusterName").Return(cluster.TestCurrentClusterName)
	clusterMetadata.On("ArchivalConfig").Return(cluster.NewArchivalConfig(cluster.ArchivalEnabled, "test-archival-bucket", true))
	clusterMetadata.On("GetNextFailoverVersion", mock.Anything, mock.Anything).Return(int64(0))
	mMetadataManager := &mocks.MetadataManager{}
	mMetadataManager.On("GetDomain", mock.Anything).Return(nil, &shared.EntityNotExistsError{})
	mMetadataManager.On("CreateDomain", mock.Anything).Return(&persistence.CreateDomainResponse{
		ID: "test-id",
	}, nil)
	mBlobstore := &mocks.BlobstoreClient{}
	mBlobstore.On("BucketExists", mock.Anything, mock.Anything).Return(true, nil)

	mService := cs.NewTestService(clusterMetadata, s.mockMessagingClient, s.mockMetricClient, s.mockClientBean)
	wh := s.getWorkflowHandlerWithParams(mService, config, mMetadataManager, mBlobstore)
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
	clusterMetadata.On("GetAllClusterInfo").Return(cluster.TestAllClusterInfo)
	clusterMetadata.On("GetCurrentClusterName").Return(cluster.TestCurrentClusterName)
	clusterMetadata.On("ArchivalConfig").Return(cluster.NewArchivalConfig(cluster.ArchivalEnabled, "test-archival-bucket", true))
	clusterMetadata.On("GetNextFailoverVersion", mock.Anything, mock.Anything).Return(int64(0))
	mMetadataManager := &mocks.MetadataManager{}
	mMetadataManager.On("GetDomain", mock.Anything).Return(nil, &shared.EntityNotExistsError{})
	mMetadataManager.On("CreateDomain", mock.Anything).Return(&persistence.CreateDomainResponse{
		ID: "test-id",
	}, nil)
	mBlobstore := &mocks.BlobstoreClient{}
	mBlobstore.On("BucketExists", mock.Anything, mock.Anything).Return(true, nil)

	mService := cs.NewTestService(clusterMetadata, s.mockMessagingClient, s.mockMetricClient, s.mockClientBean)
	wh := s.getWorkflowHandlerWithParams(mService, config, mMetadataManager, mBlobstore)
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
	clusterMetadata.On("GetAllClusterInfo").Return(cluster.TestAllClusterInfo)
	clusterMetadata.On("GetCurrentClusterName").Return(cluster.TestCurrentClusterName)
	clusterMetadata.On("ArchivalConfig").Return(cluster.NewArchivalConfig(cluster.ArchivalDisabled, "", false))
	clusterMetadata.On("GetNextFailoverVersion", mock.Anything, mock.Anything).Return(int64(0))
	mMetadataManager := &mocks.MetadataManager{}
	mMetadataManager.On("GetDomain", mock.Anything).Return(nil, &shared.EntityNotExistsError{})
	mMetadataManager.On("CreateDomain", mock.Anything).Return(&persistence.CreateDomainResponse{
		ID: "test-id",
	}, nil)

	mService := cs.NewTestService(clusterMetadata, s.mockMessagingClient, s.mockMetricClient, s.mockClientBean)
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
	clusterMetadata.On("GetAllClusterInfo").Return(cluster.TestAllClusterInfo)
	clusterMetadata.On("GetCurrentClusterName").Return(cluster.TestCurrentClusterName)
	clusterMetadata.On("ArchivalConfig").Return(cluster.NewArchivalConfig(cluster.ArchivalEnabled, "test-archival-bucket", true))
	clusterMetadata.On("GetNextFailoverVersion", mock.Anything, mock.Anything).Return(int64(0))
	mMetadataManager := &mocks.MetadataManager{}
	mMetadataManager.On("GetDomain", mock.Anything).Return(nil, &shared.EntityNotExistsError{})
	mMetadataManager.On("CreateDomain", mock.Anything).Return(&persistence.CreateDomainResponse{
		ID: "test-id",
	}, nil)

	mService := cs.NewTestService(clusterMetadata, s.mockMessagingClient, s.mockMetricClient, s.mockClientBean)
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
	clusterMetadata.On("ArchivalConfig").Return(cluster.NewArchivalConfig(cluster.ArchivalDisabled, "", false))
	mMetadataManager := &mocks.MetadataManager{}
	mMetadataManager.On("GetDomain", mock.Anything).Return(persistenceGetDomainResponse("", shared.ArchivalStatusDisabled), nil)
	mService := cs.NewTestService(clusterMetadata, s.mockMessagingClient, s.mockMetricClient, s.mockClientBean)
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
}

func (s *workflowHandlerSuite) TestDescribeDomain_Success_ArchivalEnabled() {
	config := s.newConfig()
	clusterMetadata := &mocks.ClusterMetadata{}
	clusterMetadata.On("ArchivalConfig").Return(cluster.NewArchivalConfig(cluster.ArchivalPaused, "default-bucket-name", true))
	mMetadataManager := &mocks.MetadataManager{}
	mMetadataManager.On("GetDomain", mock.Anything).Return(persistenceGetDomainResponse("bucket-name", shared.ArchivalStatusEnabled), nil)
	mBlobstore := &mocks.BlobstoreClient{}
	mService := cs.NewTestService(clusterMetadata, s.mockMessagingClient, s.mockMetricClient, s.mockClientBean)
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
}

func (s *workflowHandlerSuite) TestDescribeDomain_Success_ArchivalDisabled() {
	config := s.newConfig()
	clusterMetadata := &mocks.ClusterMetadata{}
	clusterMetadata.On("ArchivalConfig").Return(cluster.NewArchivalConfig(cluster.ArchivalPaused, "default-bucket-name", true))
	mMetadataManager := &mocks.MetadataManager{}
	mMetadataManager.On("GetDomain", mock.Anything).Return(persistenceGetDomainResponse("bucket-name", shared.ArchivalStatusDisabled), nil)
	mBlobstore := &mocks.BlobstoreClient{}
	mService := cs.NewTestService(clusterMetadata, s.mockMessagingClient, s.mockMetricClient, s.mockClientBean)
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
}

func (s *workflowHandlerSuite) TestDescribeDomain_Success_BlobstoreReturnsError() {
	config := s.newConfig()
	clusterMetadata := &mocks.ClusterMetadata{}
	clusterMetadata.On("ArchivalConfig").Return(cluster.NewArchivalConfig(cluster.ArchivalPaused, "default-bucket-name", true))
	mMetadataManager := &mocks.MetadataManager{}
	mMetadataManager.On("GetDomain", mock.Anything).Return(persistenceGetDomainResponse("bucket-name", shared.ArchivalStatusDisabled), nil)
	mBlobstore := &mocks.BlobstoreClient{}
	mService := cs.NewTestService(clusterMetadata, s.mockMessagingClient, s.mockMetricClient, s.mockClientBean)
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
	clusterMetadata.On("GetAllClusterInfo").Return(cluster.TestAllClusterInfo)
	clusterMetadata.On("GetCurrentClusterName").Return(cluster.TestCurrentClusterName)
	clusterMetadata.On("ArchivalConfig").Return(cluster.NewArchivalConfig(cluster.ArchivalPaused, "test-archival-bucket", true))
	mService := cs.NewTestService(clusterMetadata, s.mockMessagingClient, s.mockMetricClient, s.mockClientBean)
	wh := s.getWorkflowHandlerWithParams(mService, config, mMetadataManager, s.mockBlobstoreClient)
	wh.metricsClient = wh.Service.GetMetricsClient()
	wh.startWG.Done()

	updateReq := updateRequest(common.StringPtr("new-bucket"), common.ArchivalStatusPtr(shared.ArchivalStatusEnabled), nil, nil)
	_, err := wh.UpdateDomain(context.Background(), updateReq)
	assert.Error(s.T(), err)
	assert.Equal(s.T(), errBucketNameUpdate, err)
}

func (s *workflowHandlerSuite) TestUpdateDomain_Failure_BucketNotExists() {
	config := s.newConfig()
	mMetadataManager := &mocks.MetadataManager{}
	mMetadataManager.On("GetMetadata").Return(&persistence.GetMetadataResponse{
		NotificationVersion: int64(0),
	}, nil)
	mMetadataManager.On("GetDomain", mock.Anything).Return(persistenceGetDomainResponse("", shared.ArchivalStatusDisabled), nil)
	clusterMetadata := &mocks.ClusterMetadata{}
	clusterMetadata.On("IsGlobalDomainEnabled").Return(false)
	clusterMetadata.On("GetAllClusterInfo").Return(cluster.TestAllClusterInfo)
	clusterMetadata.On("GetCurrentClusterName").Return(cluster.TestCurrentClusterName)
	mBlobstore := &mocks.BlobstoreClient{}
	mBlobstore.On("BucketExists", mock.Anything, mock.Anything).Return(false, nil)
	clusterMetadata.On("ArchivalConfig").Return(cluster.NewArchivalConfig(cluster.ArchivalEnabled, "test-archival-bucket", true))
	mService := cs.NewTestService(clusterMetadata, s.mockMessagingClient, s.mockMetricClient, s.mockClientBean)
	wh := s.getWorkflowHandlerWithParams(mService, config, mMetadataManager, mBlobstore)
	wh.metricsClient = wh.Service.GetMetricsClient()
	wh.startWG.Done()

	updateReq := updateRequest(common.StringPtr("bucket-not-exists"), common.ArchivalStatusPtr(shared.ArchivalStatusEnabled), nil, nil)
	_, err := wh.UpdateDomain(context.Background(), updateReq)
	assert.Error(s.T(), err)
	assert.Equal(s.T(), errBucketDoesNotExist, err)
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
	clusterMetadata.On("GetAllClusterInfo").Return(cluster.TestAllClusterInfo)
	clusterMetadata.On("GetCurrentClusterName").Return(cluster.TestCurrentClusterName)
	clusterMetadata.On("ArchivalConfig").Return(cluster.NewArchivalConfig(cluster.ArchivalEnabled, "test-archival-bucket", true))
	mService := cs.NewTestService(clusterMetadata, s.mockMessagingClient, s.mockMetricClient, s.mockClientBean)
	mBlobstore := &mocks.BlobstoreClient{}
	mBlobstore.On("BucketExists", mock.Anything, mock.Anything).Return(true, nil)
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
	clusterMetadata.On("GetAllClusterInfo").Return(cluster.TestAllClusterInfo)
	clusterMetadata.On("GetCurrentClusterName").Return(cluster.TestCurrentClusterName)
	clusterMetadata.On("ArchivalConfig").Return(cluster.NewArchivalConfig(cluster.ArchivalDisabled, "", true))
	mService := cs.NewTestService(clusterMetadata, s.mockMessagingClient, s.mockMetricClient, s.mockClientBean)
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
	clusterMetadata.On("GetAllClusterInfo").Return(cluster.TestAllClusterInfo)
	clusterMetadata.On("GetCurrentClusterName").Return(cluster.TestCurrentClusterName)
	clusterMetadata.On("ArchivalConfig").Return(cluster.NewArchivalConfig(cluster.ArchivalEnabled, "test-archival-bucket", true))
	mService := cs.NewTestService(clusterMetadata, s.mockMessagingClient, s.mockMetricClient, s.mockClientBean)
	mBlobstore := &mocks.BlobstoreClient{}
	mBlobstore.On("BucketExists", mock.Anything, mock.Anything).Return(true, nil)
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
	clusterMetadata.On("GetAllClusterInfo").Return(cluster.TestAllClusterInfo)
	clusterMetadata.On("GetCurrentClusterName").Return(cluster.TestCurrentClusterName)
	clusterMetadata.On("ArchivalConfig").Return(cluster.NewArchivalConfig(cluster.ArchivalEnabled, "test-archival-bucket", true))
	mService := cs.NewTestService(clusterMetadata, s.mockMessagingClient, s.mockMetricClient, s.mockClientBean)
	mBlobstore := &mocks.BlobstoreClient{}
	mBlobstore.On("BucketExists", mock.Anything, mock.Anything).Return(true, nil)
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
	clusterMetadata.On("GetAllClusterInfo").Return(cluster.TestAllClusterInfo)
	clusterMetadata.On("GetCurrentClusterName").Return(cluster.TestCurrentClusterName)
	clusterMetadata.On("ArchivalConfig").Return(cluster.NewArchivalConfig(cluster.ArchivalEnabled, "test-archival-bucket", true))
	mService := cs.NewTestService(clusterMetadata, s.mockMessagingClient, s.mockMetricClient, s.mockClientBean)
	mBlobstore := &mocks.BlobstoreClient{}
	mBlobstore.On("BucketExists", mock.Anything, mock.Anything).Return(true, nil)
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
			WorkflowId: common.StringPtr(testWorkflowID),
			RunId:      common.StringPtr(testRunID),
		},
	}
	s.False(wh.historyArchived(context.Background(), getHistoryRequest, "test-domain"))

	mockHistoryClient.On("GetMutableState", mock.Anything, mock.Anything).Return(nil, &shared.EntityNotExistsError{Message: "got archival indication error"}).Once()
	wh = &WorkflowHandler{
		history: mockHistoryClient,
	}
	getHistoryRequest = &shared.GetWorkflowExecutionHistoryRequest{
		Execution: &shared.WorkflowExecution{
			WorkflowId: common.StringPtr(testWorkflowID),
			RunId:      common.StringPtr(testRunID),
		},
	}
	s.True(wh.historyArchived(context.Background(), getHistoryRequest, "test-domain"))

	mockHistoryClient.On("GetMutableState", mock.Anything, mock.Anything).Return(nil, errors.New("got non-archival indication error")).Once()
	wh = &WorkflowHandler{
		history: mockHistoryClient,
	}
	getHistoryRequest = &shared.GetWorkflowExecutionHistoryRequest{
		Execution: &shared.WorkflowExecution{
			WorkflowId: common.StringPtr(testWorkflowID),
			RunId:      common.StringPtr(testRunID),
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
	resp, err := wh.getArchivedHistory(context.Background(), getHistoryRequest(nil), s.testDomainID, metrics.NoopScope(metrics.Frontend))
	s.Nil(resp)
	s.Error(err)
}

func (s *workflowHandlerSuite) TestGetArchivedHistory_Failure_ArchivalBucketEmpty() {
	config := s.newConfig()
	mMetadataManager := &mocks.MetadataManager{}
	mMetadataManager.On("GetDomain", mock.Anything).Return(persistenceGetDomainResponse("", shared.ArchivalStatusDisabled), nil)
	clusterMetadata := &mocks.ClusterMetadata{}
	clusterMetadata.On("IsGlobalDomainEnabled").Return(false)
	clusterMetadata.On("GetAllClusterInfo").Return(cluster.TestAllClusterInfo)
	clusterMetadata.On("GetCurrentClusterName").Return(cluster.TestCurrentClusterName)
	mService := cs.NewTestService(clusterMetadata, s.mockMessagingClient, s.mockMetricClient, s.mockClientBean)
	wh := s.getWorkflowHandlerWithParams(mService, config, mMetadataManager, s.mockBlobstoreClient)
	wh.metricsClient = wh.Service.GetMetricsClient()
	wh.startWG.Done()
	resp, err := wh.getArchivedHistory(context.Background(), getHistoryRequest(nil), s.testDomainID, metrics.NoopScope(metrics.Frontend))
	s.Nil(resp)
	s.Error(err)
}

func (s *workflowHandlerSuite) TestGetArchivedHistory_Failure_InvalidPageToken() {
	config := s.newConfig()
	mMetadataManager := &mocks.MetadataManager{}
	mMetadataManager.On("GetDomain", mock.Anything).Return(persistenceGetDomainResponse(testArchivalBucket, shared.ArchivalStatusEnabled), nil)
	clusterMetadata := &mocks.ClusterMetadata{}
	clusterMetadata.On("IsGlobalDomainEnabled").Return(false)
	clusterMetadata.On("GetAllClusterInfo").Return(cluster.TestAllClusterInfo)
	clusterMetadata.On("GetCurrentClusterName").Return(cluster.TestCurrentClusterName)
	mService := cs.NewTestService(clusterMetadata, s.mockMessagingClient, s.mockMetricClient, s.mockClientBean)
	wh := s.getWorkflowHandlerWithParams(mService, config, mMetadataManager, s.mockBlobstoreClient)
	wh.metricsClient = wh.Service.GetMetricsClient()
	wh.startWG.Done()
	resp, err := wh.getArchivedHistory(context.Background(), getHistoryRequest([]byte{3, 4, 5, 1}), s.testDomainID, metrics.NoopScope(metrics.Frontend))
	s.Nil(resp)
	s.Error(err)
}

func (s *workflowHandlerSuite) TestGetArchivedHistory_Failure_InvalidBlobKey() {
	config := s.newConfig()
	mMetadataManager := &mocks.MetadataManager{}
	mMetadataManager.On("GetDomain", mock.Anything).Return(persistenceGetDomainResponse(testArchivalBucket, shared.ArchivalStatusEnabled), nil)
	clusterMetadata := &mocks.ClusterMetadata{}
	clusterMetadata.On("IsGlobalDomainEnabled").Return(false)
	clusterMetadata.On("GetAllClusterInfo").Return(cluster.TestAllClusterInfo)
	clusterMetadata.On("GetCurrentClusterName").Return(cluster.TestCurrentClusterName)
	mService := cs.NewTestService(clusterMetadata, s.mockMessagingClient, s.mockMetricClient, s.mockClientBean)
	wh := s.getWorkflowHandlerWithParams(mService, config, mMetadataManager, s.mockBlobstoreClient)
	wh.metricsClient = wh.Service.GetMetricsClient()
	wh.startWG.Done()
	getHistoryRequest := getHistoryRequest(nil)
	getHistoryRequest.Execution.WorkflowId = common.StringPtr("")
	resp, err := wh.getArchivedHistory(context.Background(), getHistoryRequest, s.testDomainID, metrics.NoopScope(metrics.Frontend))
	s.Nil(resp)
	s.Error(err)
}

func (s *workflowHandlerSuite) TestGetArchivedHistory_Failure_FailedToGetVersions() {
	config := s.newConfig()
	mMetadataManager := &mocks.MetadataManager{}
	mMetadataManager.On("GetDomain", mock.Anything).Return(persistenceGetDomainResponse(testArchivalBucket, shared.ArchivalStatusEnabled), nil)
	clusterMetadata := &mocks.ClusterMetadata{}
	clusterMetadata.On("IsGlobalDomainEnabled").Return(false)
	clusterMetadata.On("GetAllClusterInfo").Return(cluster.TestAllClusterInfo)
	clusterMetadata.On("GetCurrentClusterName").Return(cluster.TestCurrentClusterName)
	mService := cs.NewTestService(clusterMetadata, s.mockMessagingClient, s.mockMetricClient, s.mockClientBean)
	mBlobstore := &mocks.BlobstoreClient{}
	mBlobstore.On("GetTags", mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("failed to get tags for index blob"))
	wh := s.getWorkflowHandlerWithParams(mService, config, mMetadataManager, mBlobstore)
	wh.metricsClient = wh.Service.GetMetricsClient()
	wh.startWG.Done()
	resp, err := wh.getArchivedHistory(context.Background(), getHistoryRequest(nil), s.testDomainID, metrics.NoopScope(metrics.Frontend))
	s.Nil(resp)
	s.Error(err)
}

func (s *workflowHandlerSuite) TestGetArchivedHistory_Failure_FailedToDownload() {
	config := s.newConfig()
	mMetadataManager := &mocks.MetadataManager{}
	mMetadataManager.On("GetDomain", mock.Anything).Return(persistenceGetDomainResponse(testArchivalBucket, shared.ArchivalStatusEnabled), nil)
	clusterMetadata := &mocks.ClusterMetadata{}
	clusterMetadata.On("IsGlobalDomainEnabled").Return(false)
	clusterMetadata.On("GetAllClusterInfo").Return(cluster.TestAllClusterInfo)
	clusterMetadata.On("GetCurrentClusterName").Return(cluster.TestCurrentClusterName)
	mService := cs.NewTestService(clusterMetadata, s.mockMessagingClient, s.mockMetricClient, s.mockClientBean)
	mBlobstore := &mocks.BlobstoreClient{}
	mBlobstore.On("GetTags", mock.Anything, mock.Anything, mock.Anything).Return(map[string]string{"10": ""}, nil)
	mBlobstore.On("Download", mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("failed to download blob"))
	wh := s.getWorkflowHandlerWithParams(mService, config, mMetadataManager, mBlobstore)
	wh.metricsClient = wh.Service.GetMetricsClient()
	wh.startWG.Done()
	resp, err := wh.getArchivedHistory(context.Background(), getHistoryRequest(nil), s.testDomainID, metrics.NoopScope(metrics.Frontend))
	s.Nil(resp)
	s.Error(err)
}

func (s *workflowHandlerSuite) TestGetArchivedHistory_Success_GetFirstPage() {
	config := s.newConfig()
	mMetadataManager := &mocks.MetadataManager{}
	mMetadataManager.On("GetDomain", mock.Anything).Return(persistenceGetDomainResponse(testArchivalBucket, shared.ArchivalStatusEnabled), nil)
	clusterMetadata := &mocks.ClusterMetadata{}
	clusterMetadata.On("IsGlobalDomainEnabled").Return(false)
	clusterMetadata.On("GetAllClusterInfo").Return(cluster.TestAllClusterInfo)
	clusterMetadata.On("GetCurrentClusterName").Return(cluster.TestCurrentClusterName)
	mService := cs.NewTestService(clusterMetadata, s.mockMessagingClient, s.mockMetricClient, s.mockClientBean)
	mBlobstore := &mocks.BlobstoreClient{}
	unwrappedBlob := &archiver.HistoryBlob{
		Header: &archiver.HistoryBlobHeader{
			CurrentPageToken: common.IntPtr(common.FirstBlobPageToken),
			NextPageToken:    common.IntPtr(common.FirstBlobPageToken + 1),
			IsLast:           common.BoolPtr(false),
		},
		Body: &shared.History{},
	}
	bytes, err := json.Marshal(unwrappedBlob)
	s.NoError(err)
	historyBlob, err := blob.Wrap(blob.NewBlob(bytes, map[string]string{}), blob.JSONEncoded())
	s.NoError(err)
	mBlobstore.On("GetTags", mock.Anything, mock.Anything, mock.Anything).Return(map[string]string{"10": ""}, nil)
	historyKey, _ := archiver.NewHistoryBlobKey(s.testDomainID, testWorkflowID, testRunID, 10, common.FirstBlobPageToken)
	mBlobstore.On("Download", mock.Anything, mock.Anything, historyKey).Return(historyBlob, nil)
	wh := s.getWorkflowHandlerWithParams(mService, config, mMetadataManager, mBlobstore)
	wh.metricsClient = wh.Service.GetMetricsClient()
	wh.startWG.Done()
	resp, err := wh.getArchivedHistory(context.Background(), getHistoryRequest(nil), s.testDomainID, metrics.NoopScope(metrics.Frontend))
	s.NoError(err)
	s.NotNil(resp)
	s.NotNil(resp.History)
	s.True(resp.GetArchived())
}

func (s *workflowHandlerSuite) TestGetHistory() {
	config := s.newConfig()
	domainID := uuid.New()
	firstEventID := int64(100)
	nextEventID := int64(101)
	we := gen.WorkflowExecution{
		WorkflowId: common.StringPtr("wid"),
		RunId:      common.StringPtr("rid"),
	}
	shardID := common.WorkflowIDToHistoryShard(*we.WorkflowId, numHistoryShards)
	req := &persistence.ReadHistoryBranchRequest{
		BranchToken:   []byte{},
		MinEventID:    firstEventID,
		MaxEventID:    nextEventID,
		PageSize:      0,
		NextPageToken: []byte{},
		ShardID:       common.IntPtr(shardID),
	}
	s.mockHistoryV2Mgr.On("ReadHistoryBranch", req).Return(&persistence.ReadHistoryBranchResponse{
		HistoryEvents: []*workflow.HistoryEvent{
			{
				EventId: common.Int64Ptr(int64(1)),
			},
		},
		NextPageToken:    []byte{},
		Size:             1,
		LastFirstEventID: nextEventID,
	}, nil).Once()
	clusterMetadata := &mocks.ClusterMetadata{}
	mService := cs.NewTestService(clusterMetadata, s.mockMessagingClient, s.mockMetricClient, s.mockClientBean)
	mMetadataManager := &mocks.MetadataManager{}
	mBlobstore := &mocks.BlobstoreClient{}
	wh := s.getWorkflowHandlerWithParams(mService, config, mMetadataManager, mBlobstore)
	wh.metricsClient = wh.Service.GetMetricsClient()
	scope := wh.metricsClient.Scope(0)
	history, token, err := wh.getHistory(scope, domainID, we, firstEventID, nextEventID, 0, []byte{}, nil, persistence.EventStoreVersionV2, []byte{})
	s.NotNil(history)
	s.Equal([]byte{}, token)
	s.NoError(err)
}

func (s *workflowHandlerSuite) TestGetSearchAttributes() {
	wh := s.getWorkflowHandlerHelper()

	ctx := context.Background()
	resp, err := wh.GetSearchAttributes(ctx)
	s.NoError(err)
	s.NotNil(resp)
}

func (s *workflowHandlerSuite) TestListWorkflowExecutions() {
	wh := s.getWorkflowHandlerHelper()

	s.mockDomainCache.On("GetDomainID", mock.Anything).Return(s.testDomainID, nil)
	s.mockVisibilityMgr.On("ListWorkflowExecutions", mock.Anything).Return(&persistence.ListWorkflowExecutionsResponse{}, nil).Once()

	listRequest := &shared.ListWorkflowExecutionsRequest{
		Domain:   common.StringPtr(s.testDomain),
		PageSize: common.Int32Ptr(int32(s.config.ESIndexMaxResultWindow())),
	}
	ctx := context.Background()

	query := "WorkflowID = 'wid'"
	listRequest.Query = common.StringPtr(query)
	_, err := wh.ListWorkflowExecutions(ctx, listRequest)
	s.NoError(err)
	s.Equal(query, listRequest.GetQuery())

	query = "InvalidKey = 'a'"
	listRequest.Query = common.StringPtr(query)
	_, err = wh.ListWorkflowExecutions(ctx, listRequest)
	s.NotNil(err)

	listRequest.PageSize = common.Int32Ptr(int32(s.config.ESIndexMaxResultWindow() + 1))
	_, err = wh.ListWorkflowExecutions(ctx, listRequest)
	s.NotNil(err)
}

func (s *workflowHandlerSuite) TestScantWorkflowExecutions() {
	wh := s.getWorkflowHandlerHelper()

	s.mockDomainCache.On("GetDomainID", mock.Anything).Return(s.testDomainID, nil)
	s.mockVisibilityMgr.On("ScanWorkflowExecutions", mock.Anything).Return(&persistence.ListWorkflowExecutionsResponse{}, nil).Once()

	listRequest := &shared.ListWorkflowExecutionsRequest{
		Domain:   common.StringPtr(s.testDomain),
		PageSize: common.Int32Ptr(int32(s.config.ESIndexMaxResultWindow())),
	}
	ctx := context.Background()

	query := "WorkflowID = 'wid'"
	listRequest.Query = common.StringPtr(query)
	_, err := wh.ScanWorkflowExecutions(ctx, listRequest)
	s.NoError(err)
	s.Equal(query, listRequest.GetQuery())

	query = "InvalidKey = 'a'"
	listRequest.Query = common.StringPtr(query)
	_, err = wh.ScanWorkflowExecutions(ctx, listRequest)
	s.NotNil(err)

	listRequest.PageSize = common.Int32Ptr(int32(s.config.ESIndexMaxResultWindow() + 1))
	_, err = wh.ListWorkflowExecutions(ctx, listRequest)
	s.NotNil(err)
}

func (s *workflowHandlerSuite) TestCountWorkflowExecutions() {
	wh := s.getWorkflowHandlerHelper()

	s.mockDomainCache.On("GetDomainID", mock.Anything).Return(s.testDomainID, nil)
	s.mockVisibilityMgr.On("CountWorkflowExecutions", mock.Anything).Return(&persistence.CountWorkflowExecutionsResponse{}, nil).Once()

	countRequest := &shared.CountWorkflowExecutionsRequest{
		Domain: common.StringPtr(s.testDomain),
	}
	ctx := context.Background()

	query := "WorkflowID = 'wid'"
	countRequest.Query = common.StringPtr(query)
	_, err := wh.CountWorkflowExecutions(ctx, countRequest)
	s.NoError(err)
	s.Equal(query, countRequest.GetQuery())

	query = "InvalidKey = 'a'"
	countRequest.Query = common.StringPtr(query)
	_, err = wh.CountWorkflowExecutions(ctx, countRequest)
	s.NotNil(err)
}

func (s *workflowHandlerSuite) newConfig() *Config {
	return NewConfig(dc.NewCollection(dc.NewNopClient(), s.logger), numHistoryShards, false)
}

func updateRequest(archivalBucketName *string, archivalStatus *shared.ArchivalStatus, archivalRetentionDays *int32, archivalOwner *string) *shared.UpdateDomainRequest {
	return &shared.UpdateDomainRequest{
		Name: common.StringPtr("test-name"),
		Configuration: &shared.DomainConfiguration{
			ArchivalBucketName: archivalBucketName,
			ArchivalStatus:     archivalStatus,
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
			Retention:      1,
			EmitMetric:     true,
			ArchivalBucket: archivalBucket,
			ArchivalStatus: archivalStatus,
		},
		ReplicationConfig: &persistence.DomainReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []*persistence.ClusterReplicationConfig{
				{
					ClusterName: cluster.TestCurrentClusterName,
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
				ClusterName: common.StringPtr(cluster.TestCurrentClusterName),
			},
		},
		ActiveClusterName:  common.StringPtr(cluster.TestCurrentClusterName),
		Data:               make(map[string]string),
		SecurityToken:      common.StringPtr("token"),
		ArchivalStatus:     archivalStatus,
		ArchivalBucketName: bucketName,
		IsGlobalDomain:     common.BoolPtr(false),
	}
}

func getHistoryRequest(nextPageToken []byte) *shared.GetWorkflowExecutionHistoryRequest {
	return &shared.GetWorkflowExecutionHistoryRequest{
		Execution: &shared.WorkflowExecution{
			WorkflowId: common.StringPtr(testWorkflowID),
			RunId:      common.StringPtr(testRunID),
		},
		NextPageToken: nextPageToken,
	}
}
