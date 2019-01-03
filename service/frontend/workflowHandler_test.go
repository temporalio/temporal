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
	"errors"
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
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/messaging"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/mocks"
	"github.com/uber/cadence/common/persistence"
	cs "github.com/uber/cadence/common/service"
	dc "github.com/uber/cadence/common/service/dynamicconfig"
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
		mockBlobstoreClient *mocks.Client
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
	s.mockBlobstoreClient = &mocks.Client{}
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

func (s *workflowHandlerSuite) TestDisableListVisibilityByFilter() {
	domain := "test-domain"
	domainID := uuid.New()
	config := NewConfig(dc.NewCollection(dc.NewNopClient(), s.logger))
	config.DisableListVisibilityByFilter = dc.GetBoolPropertyFnFilteredByDomain(true)

	wh := NewWorkflowHandler(s.mockService, config, s.mockMetadataMgr, s.mockHistoryMgr,
		s.mockHistoryV2Mgr, s.mockVisibilityMgr, s.mockProducer, s.mockBlobstoreClient)
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
	config := NewConfig(dc.NewCollection(dc.NewNopClient(), s.logger))
	config.RPS = dc.GetIntPropertyFn(10)
	wh := NewWorkflowHandler(s.mockService, config, s.mockMetadataMgr, s.mockHistoryMgr,
		s.mockHistoryV2Mgr, s.mockVisibilityMgr, s.mockProducer, s.mockBlobstoreClient)
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
	config := NewConfig(dc.NewCollection(dc.NewNopClient(), s.logger))
	config.RPS = dc.GetIntPropertyFn(10)
	wh := NewWorkflowHandler(s.mockService, config, s.mockMetadataMgr, s.mockHistoryMgr,
		s.mockHistoryV2Mgr, s.mockVisibilityMgr, s.mockProducer, s.mockBlobstoreClient)
	wh.metricsClient = wh.Service.GetMetricsClient()
	wh.startWG.Done()

	_, err := wh.StartWorkflowExecution(context.Background(), nil)
	assert.Error(s.T(), err)
	assert.Equal(s.T(), errRequestNotSet, err)
}

func (s *workflowHandlerSuite) TestStartWorkflowExecution_Failed_DomainNotSet() {
	config := NewConfig(dc.NewCollection(dc.NewNopClient(), s.logger))
	config.RPS = dc.GetIntPropertyFn(10)
	wh := NewWorkflowHandler(s.mockService, config, s.mockMetadataMgr, s.mockHistoryMgr,
		s.mockHistoryV2Mgr, s.mockVisibilityMgr, s.mockProducer, s.mockBlobstoreClient)
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
	config := NewConfig(dc.NewCollection(dc.NewNopClient(), s.logger))
	config.RPS = dc.GetIntPropertyFn(10)
	wh := NewWorkflowHandler(s.mockService, config, s.mockMetadataMgr, s.mockHistoryMgr, s.mockHistoryV2Mgr,
		s.mockVisibilityMgr, s.mockProducer, s.mockBlobstoreClient)
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
	config := NewConfig(dc.NewCollection(dc.NewNopClient(), s.logger))
	config.RPS = dc.GetIntPropertyFn(10)
	wh := NewWorkflowHandler(s.mockService, config, s.mockMetadataMgr, s.mockHistoryMgr, s.mockHistoryV2Mgr,
		s.mockVisibilityMgr, s.mockProducer, s.mockBlobstoreClient)
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
	config := NewConfig(dc.NewCollection(dc.NewNopClient(), s.logger))
	config.RPS = dc.GetIntPropertyFn(10)
	wh := NewWorkflowHandler(s.mockService, config, s.mockMetadataMgr, s.mockHistoryMgr, s.mockHistoryV2Mgr,
		s.mockVisibilityMgr, s.mockProducer, s.mockBlobstoreClient)
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
	config := NewConfig(dc.NewCollection(dc.NewNopClient(), s.logger))
	config.RPS = dc.GetIntPropertyFn(10)
	wh := NewWorkflowHandler(s.mockService, config, s.mockMetadataMgr, s.mockHistoryMgr, s.mockHistoryV2Mgr,
		s.mockVisibilityMgr, s.mockProducer, s.mockBlobstoreClient)
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
	config := NewConfig(dc.NewCollection(dc.NewNopClient(), s.logger))
	config.RPS = dc.GetIntPropertyFn(10)
	wh := NewWorkflowHandler(s.mockService, config, s.mockMetadataMgr, s.mockHistoryMgr, s.mockHistoryV2Mgr,
		s.mockVisibilityMgr, s.mockProducer, s.mockBlobstoreClient)
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

func (s *workflowHandlerSuite) TestRegisterDomain_Failed_CustomBucketGivenButArchivalNotEnabled() {
	config := NewConfig(dc.NewCollection(dc.NewNopClient(), s.logger))
	wh := NewWorkflowHandler(s.mockService, config, s.mockMetadataMgr, s.mockHistoryMgr, s.mockHistoryV2Mgr,
		s.mockVisibilityMgr, s.mockProducer, s.mockBlobstoreClient)
	wh.metricsClient = wh.Service.GetMetricsClient()
	wh.startWG.Done()

	req := registerDomainRequest(false, common.StringPtr("custom-bucket-name"))
	err := wh.RegisterDomain(context.Background(), req)
	assert.Error(s.T(), err)
	assert.Equal(s.T(), errSettingBucketNameWithoutEnabling, err)
	s.mockMetadataMgr.AssertNotCalled(s.T(), "CreateDomain", mock.Anything)
}

func (s *workflowHandlerSuite) TestRegisterDomain_Success_CustomBucketAndArchivalEnabled() {
	config := NewConfig(dc.NewCollection(dc.NewNopClient(), s.logger))
	clusterMetadata := &mocks.ClusterMetadata{}
	clusterMetadata.On("IsGlobalDomainEnabled").Return(false)
	clusterMetadata.On("GetCurrentClusterName").Return("active")
	clusterMetadata.On("GetNextFailoverVersion", mock.Anything, mock.Anything).Return(int64(0))
	mMetadataManager := &mocks.MetadataManager{}
	mMetadataManager.On("GetDomain", mock.Anything).Return(nil, &shared.EntityNotExistsError{})
	mMetadataManager.On("CreateDomain", mock.Anything).Return(&persistence.CreateDomainResponse{
		ID: "test-id",
	}, nil)

	mService := cs.NewTestService(clusterMetadata, s.mockMessagingClient, s.mockMetricClient, s.mockClientBean, s.logger)
	wh := NewWorkflowHandler(mService, config, mMetadataManager, s.mockHistoryMgr, s.mockHistoryV2Mgr,
		s.mockVisibilityMgr, s.mockProducer, s.mockBlobstoreClient)
	wh.metricsClient = wh.Service.GetMetricsClient()
	wh.startWG.Done()

	req := registerDomainRequest(true, common.StringPtr("custom-bucket-name"))
	err := wh.RegisterDomain(context.Background(), req)
	assert.NoError(s.T(), err)
	mMetadataManager.AssertCalled(s.T(), "CreateDomain", mock.Anything)
	clusterMetadata.AssertNotCalled(s.T(), "GetDefaultArchivalBucket")
}

func (s *workflowHandlerSuite) TestRegisterDomain_Success_ArchivalEnabledWithoutCustomBucket() {
	config := NewConfig(dc.NewCollection(dc.NewNopClient(), s.logger))
	clusterMetadata := &mocks.ClusterMetadata{}
	clusterMetadata.On("IsGlobalDomainEnabled").Return(false)
	clusterMetadata.On("GetCurrentClusterName").Return("active")
	clusterMetadata.On("GetNextFailoverVersion", mock.Anything, mock.Anything).Return(int64(0))
	clusterMetadata.On("GetDefaultArchivalBucket").Return("test-archival-bucket")
	mMetadataManager := &mocks.MetadataManager{}
	mMetadataManager.On("GetDomain", mock.Anything).Return(nil, &shared.EntityNotExistsError{})
	mMetadataManager.On("CreateDomain", mock.Anything).Return(&persistence.CreateDomainResponse{
		ID: "test-id",
	}, nil)

	mService := cs.NewTestService(clusterMetadata, s.mockMessagingClient, s.mockMetricClient, s.mockClientBean, s.logger)
	wh := NewWorkflowHandler(mService, config, mMetadataManager, s.mockHistoryMgr, s.mockHistoryV2Mgr,
		s.mockVisibilityMgr, s.mockProducer, s.mockBlobstoreClient)
	wh.metricsClient = wh.Service.GetMetricsClient()
	wh.startWG.Done()

	req := registerDomainRequest(true, common.StringPtr(""))
	err := wh.RegisterDomain(context.Background(), req)
	assert.NoError(s.T(), err)
	mMetadataManager.AssertCalled(s.T(), "CreateDomain", mock.Anything)
	clusterMetadata.AssertCalled(s.T(), "GetDefaultArchivalBucket")
}

func (s *workflowHandlerSuite) TestRegisterDomain_Success_ArchivalNotEnabled() {
	config := NewConfig(dc.NewCollection(dc.NewNopClient(), s.logger))
	clusterMetadata := &mocks.ClusterMetadata{}
	clusterMetadata.On("IsGlobalDomainEnabled").Return(false)
	clusterMetadata.On("GetCurrentClusterName").Return("active")
	clusterMetadata.On("GetNextFailoverVersion", mock.Anything, mock.Anything).Return(int64(0))
	mMetadataManager := &mocks.MetadataManager{}
	mMetadataManager.On("GetDomain", mock.Anything).Return(nil, &shared.EntityNotExistsError{})
	mMetadataManager.On("CreateDomain", mock.Anything).Return(&persistence.CreateDomainResponse{
		ID: "test-id",
	}, nil)

	mService := cs.NewTestService(clusterMetadata, s.mockMessagingClient, s.mockMetricClient, s.mockClientBean, s.logger)
	wh := NewWorkflowHandler(mService, config, mMetadataManager, s.mockHistoryMgr, s.mockHistoryV2Mgr,
		s.mockVisibilityMgr, s.mockProducer, s.mockBlobstoreClient)
	wh.metricsClient = wh.Service.GetMetricsClient()
	wh.startWG.Done()

	req := registerDomainRequest(false, common.StringPtr(""))
	err := wh.RegisterDomain(context.Background(), req)
	assert.NoError(s.T(), err)
	mMetadataManager.AssertCalled(s.T(), "CreateDomain", mock.Anything)
	clusterMetadata.AssertNotCalled(s.T(), "GetDefaultArchivalBucket")
}

func (s *workflowHandlerSuite) TestDescribeDomain_Success_ArchivalNeverEnabled() {
	config := NewConfig(dc.NewCollection(dc.NewNopClient(), s.logger))
	mMetadataManager := &mocks.MetadataManager{}
	mMetadataManager.On("GetDomain", mock.Anything).Return(persistenceGetDomainResponse("", shared.ArchivalStatusNeverEnabled), nil)
	mBlobstore := &mocks.Client{}
	wh := NewWorkflowHandler(s.mockService, config, mMetadataManager, s.mockHistoryMgr, s.mockHistoryV2Mgr,
		s.mockVisibilityMgr, s.mockProducer, mBlobstore)
	wh.metricsClient = wh.Service.GetMetricsClient()
	wh.startWG.Done()

	req := &shared.DescribeDomainRequest{
		Name: common.StringPtr("test-domain"),
	}
	result, err := wh.DescribeDomain(context.Background(), req)

	assert.NoError(s.T(), err)
	assert.NotNil(s.T(), result)
	assert.NotNil(s.T(), result.Configuration)
	assert.Equal(s.T(), result.Configuration.GetArchivalStatus(), shared.ArchivalStatusNeverEnabled)
	assert.Equal(s.T(), result.Configuration.GetArchivalBucketName(), "")
	assert.Equal(s.T(), result.Configuration.GetArchivalRetentionPeriodInDays(), int32(0))
	assert.Equal(s.T(), result.Configuration.GetArchivalBucketOwner(), "")
	mBlobstore.AssertNotCalled(s.T(), "BucketMetadata", mock.Anything, mock.Anything)
}

func (s *workflowHandlerSuite) TestDescribeDomain_Success_ArchivalEnabled() {
	config := NewConfig(dc.NewCollection(dc.NewNopClient(), s.logger))
	mMetadataManager := &mocks.MetadataManager{}
	mMetadataManager.On("GetDomain", mock.Anything).Return(persistenceGetDomainResponse("bucket-name", shared.ArchivalStatusEnabled), nil)
	mBlobstore := &mocks.Client{}
	mBlobstore.On("BucketMetadata", mock.Anything, mock.Anything).Return(bucketMetadataResponse("test-owner", 10), nil)
	wh := NewWorkflowHandler(s.mockService, config, mMetadataManager, s.mockHistoryMgr, s.mockHistoryV2Mgr,
		s.mockVisibilityMgr, s.mockProducer, mBlobstore)
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
	config := NewConfig(dc.NewCollection(dc.NewNopClient(), s.logger))
	mMetadataManager := &mocks.MetadataManager{}
	mMetadataManager.On("GetDomain", mock.Anything).Return(persistenceGetDomainResponse("bucket-name", shared.ArchivalStatusDisabled), nil)
	mBlobstore := &mocks.Client{}
	mBlobstore.On("BucketMetadata", mock.Anything, mock.Anything).Return(bucketMetadataResponse("test-owner", 10), nil)
	wh := NewWorkflowHandler(s.mockService, config, mMetadataManager, s.mockHistoryMgr, s.mockHistoryV2Mgr,
		s.mockVisibilityMgr, s.mockProducer, mBlobstore)
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
	config := NewConfig(dc.NewCollection(dc.NewNopClient(), s.logger))
	mMetadataManager := &mocks.MetadataManager{}
	mMetadataManager.On("GetDomain", mock.Anything).Return(persistenceGetDomainResponse("bucket-name", shared.ArchivalStatusDisabled), nil)
	mBlobstore := &mocks.Client{}
	mBlobstore.On("BucketMetadata", mock.Anything, mock.Anything).Return(nil, errors.New("blobstore error"))
	wh := NewWorkflowHandler(s.mockService, config, mMetadataManager, s.mockHistoryMgr, s.mockHistoryV2Mgr,
		s.mockVisibilityMgr, s.mockProducer, mBlobstore)
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

func (s *workflowHandlerSuite) TestUpdateDomain_Failure_StatusChangeToNeverEnabled() {
	config := NewConfig(dc.NewCollection(dc.NewNopClient(), s.logger))
	wh := NewWorkflowHandler(s.mockService, config, s.mockMetadataMgr, s.mockHistoryMgr, s.mockHistoryV2Mgr,
		s.mockVisibilityMgr, s.mockProducer, s.mockBlobstoreClient)
	wh.metricsClient = wh.Service.GetMetricsClient()
	wh.startWG.Done()

	_, err := wh.UpdateDomain(context.Background(), updateRequest(nil, common.ArchivalStatusPtr(shared.ArchivalStatusNeverEnabled), nil, nil))
	assert.Error(s.T(), err)
	assert.Equal(s.T(), errDisallowedStatusChange, err)
}

func (s *workflowHandlerSuite) TestUpdateDomain_Failure_IllegalBucketOwnerUpdate() {
	config := NewConfig(dc.NewCollection(dc.NewNopClient(), s.logger))
	wh := NewWorkflowHandler(s.mockService, config, s.mockMetadataMgr, s.mockHistoryMgr, s.mockHistoryV2Mgr,
		s.mockVisibilityMgr, s.mockProducer, s.mockBlobstoreClient)
	wh.metricsClient = wh.Service.GetMetricsClient()
	wh.startWG.Done()

	_, err := wh.UpdateDomain(context.Background(), updateRequest(nil, common.ArchivalStatusPtr(shared.ArchivalStatusEnabled), nil, common.StringPtr("updated-owner")))
	assert.Error(s.T(), err)
	assert.Equal(s.T(), errDisallowedBucketMetadata, err)
}

func (s *workflowHandlerSuite) TestUpdateDomain_Failure_IllegalArchivalRetentionUpdate() {
	config := NewConfig(dc.NewCollection(dc.NewNopClient(), s.logger))
	wh := NewWorkflowHandler(s.mockService, config, s.mockMetadataMgr, s.mockHistoryMgr, s.mockHistoryV2Mgr,
		s.mockVisibilityMgr, s.mockProducer, s.mockBlobstoreClient)
	wh.metricsClient = wh.Service.GetMetricsClient()
	wh.startWG.Done()

	_, err := wh.UpdateDomain(context.Background(), updateRequest(nil, common.ArchivalStatusPtr(shared.ArchivalStatusEnabled), common.Int32Ptr(10), nil))
	assert.Error(s.T(), err)
	assert.Equal(s.T(), errDisallowedBucketMetadata, err)
}

func (s *workflowHandlerSuite) TestUpdateDomain_Failure_ProvidedBucketWithoutEnabling() {
	config := NewConfig(dc.NewCollection(dc.NewNopClient(), s.logger))
	wh := NewWorkflowHandler(s.mockService, config, s.mockMetadataMgr, s.mockHistoryMgr, s.mockHistoryV2Mgr,
		s.mockVisibilityMgr, s.mockProducer, s.mockBlobstoreClient)
	wh.metricsClient = wh.Service.GetMetricsClient()
	wh.startWG.Done()

	_, err := wh.UpdateDomain(context.Background(), updateRequest(common.StringPtr("custom-bucket"), common.ArchivalStatusPtr(shared.ArchivalStatusDisabled), nil, nil))
	assert.Error(s.T(), err)
	assert.Equal(s.T(), errSettingBucketNameWithoutEnabling, err)
}

func (s *workflowHandlerSuite) TestUpdateDomain_Failure_UpdateExistingBucketName() {
	config := NewConfig(dc.NewCollection(dc.NewNopClient(), s.logger))
	mMetadataManager := &mocks.MetadataManager{}
	mMetadataManager.On("GetMetadata").Return(&persistence.GetMetadataResponse{
		NotificationVersion: int64(0),
	}, nil)
	mMetadataManager.On("GetDomain", mock.Anything).Return(persistenceGetDomainResponse("bucket-name", shared.ArchivalStatusDisabled), nil)
	clusterMetadata := &mocks.ClusterMetadata{}
	clusterMetadata.On("IsGlobalDomainEnabled").Return(false)
	mService := cs.NewTestService(clusterMetadata, s.mockMessagingClient, s.mockMetricClient, s.mockClientBean, s.logger)
	wh := NewWorkflowHandler(mService, config, mMetadataManager, s.mockHistoryMgr, s.mockHistoryV2Mgr,
		s.mockVisibilityMgr, s.mockProducer, s.mockBlobstoreClient)
	wh.metricsClient = wh.Service.GetMetricsClient()
	wh.startWG.Done()

	updateReq := updateRequest(common.StringPtr("new-bucket"), common.ArchivalStatusPtr(shared.ArchivalStatusEnabled), nil, nil)
	_, err := wh.UpdateDomain(context.Background(), updateReq)
	assert.Error(s.T(), err)
	assert.Equal(s.T(), errBucketNameUpdate, err)
}

func (s *workflowHandlerSuite) TestUpdateDomain_Failure_ArchivalEnabledToArchivalEnabled() {
	config := NewConfig(dc.NewCollection(dc.NewNopClient(), s.logger))
	mMetadataManager := &mocks.MetadataManager{}
	mMetadataManager.On("GetMetadata").Return(&persistence.GetMetadataResponse{
		NotificationVersion: int64(0),
	}, nil)
	mMetadataManager.On("GetDomain", mock.Anything).Return(persistenceGetDomainResponse("bucket-name", shared.ArchivalStatusEnabled), nil)
	clusterMetadata := &mocks.ClusterMetadata{}
	clusterMetadata.On("IsGlobalDomainEnabled").Return(false)
	mService := cs.NewTestService(clusterMetadata, s.mockMessagingClient, s.mockMetricClient, s.mockClientBean, s.logger)
	wh := NewWorkflowHandler(mService, config, mMetadataManager, s.mockHistoryMgr, s.mockHistoryV2Mgr,
		s.mockVisibilityMgr, s.mockProducer, s.mockBlobstoreClient)
	wh.metricsClient = wh.Service.GetMetricsClient()
	wh.startWG.Done()

	updateReq := updateRequest(nil, common.ArchivalStatusPtr(shared.ArchivalStatusEnabled), nil, nil)
	_, err := wh.UpdateDomain(context.Background(), updateReq)
	assert.Error(s.T(), err)
	assert.Equal(s.T(), errDisallowedStatusChange, err)
}

func (s *workflowHandlerSuite) TestUpdateDomain_Failure_ArchivalDisabledToArchivalDisabled() {
	config := NewConfig(dc.NewCollection(dc.NewNopClient(), s.logger))
	mMetadataManager := &mocks.MetadataManager{}
	mMetadataManager.On("GetMetadata").Return(&persistence.GetMetadataResponse{
		NotificationVersion: int64(0),
	}, nil)
	mMetadataManager.On("GetDomain", mock.Anything).Return(persistenceGetDomainResponse("bucket-name", shared.ArchivalStatusDisabled), nil)
	clusterMetadata := &mocks.ClusterMetadata{}
	clusterMetadata.On("IsGlobalDomainEnabled").Return(false)
	mService := cs.NewTestService(clusterMetadata, s.mockMessagingClient, s.mockMetricClient, s.mockClientBean, s.logger)
	wh := NewWorkflowHandler(mService, config, mMetadataManager, s.mockHistoryMgr, s.mockHistoryV2Mgr,
		s.mockVisibilityMgr, s.mockProducer, s.mockBlobstoreClient)
	wh.metricsClient = wh.Service.GetMetricsClient()
	wh.startWG.Done()

	updateReq := updateRequest(nil, common.ArchivalStatusPtr(shared.ArchivalStatusDisabled), nil, nil)
	_, err := wh.UpdateDomain(context.Background(), updateReq)
	assert.Error(s.T(), err)
	assert.Equal(s.T(), errDisallowedStatusChange, err)
}

func (s *workflowHandlerSuite) TestUpdateDomain_Failure_ArchivalNeverEnabledToArchivalDisabled() {
	config := NewConfig(dc.NewCollection(dc.NewNopClient(), s.logger))
	mMetadataManager := &mocks.MetadataManager{}
	mMetadataManager.On("GetMetadata").Return(&persistence.GetMetadataResponse{
		NotificationVersion: int64(0),
	}, nil)
	mMetadataManager.On("GetDomain", mock.Anything).Return(persistenceGetDomainResponse("bucket-name", shared.ArchivalStatusNeverEnabled), nil)
	clusterMetadata := &mocks.ClusterMetadata{}
	clusterMetadata.On("IsGlobalDomainEnabled").Return(false)
	mService := cs.NewTestService(clusterMetadata, s.mockMessagingClient, s.mockMetricClient, s.mockClientBean, s.logger)
	wh := NewWorkflowHandler(mService, config, mMetadataManager, s.mockHistoryMgr, s.mockHistoryV2Mgr,
		s.mockVisibilityMgr, s.mockProducer, s.mockBlobstoreClient)
	wh.metricsClient = wh.Service.GetMetricsClient()
	wh.startWG.Done()

	updateReq := updateRequest(nil, common.ArchivalStatusPtr(shared.ArchivalStatusDisabled), nil, nil)
	_, err := wh.UpdateDomain(context.Background(), updateReq)
	assert.Error(s.T(), err)
	assert.Equal(s.T(), errDisallowedStatusChange, err)
}

func (s *workflowHandlerSuite) TestUpdateDomain_Success_ArchivalEnabledToArchivalDisabled() {
	config := NewConfig(dc.NewCollection(dc.NewNopClient(), s.logger))
	mMetadataManager := &mocks.MetadataManager{}
	mMetadataManager.On("GetMetadata").Return(&persistence.GetMetadataResponse{
		NotificationVersion: int64(0),
	}, nil)
	mMetadataManager.On("GetDomain", mock.Anything).Return(persistenceGetDomainResponse("bucket-name", shared.ArchivalStatusEnabled), nil)
	clusterMetadata := &mocks.ClusterMetadata{}
	clusterMetadata.On("IsGlobalDomainEnabled").Return(false)
	mService := cs.NewTestService(clusterMetadata, s.mockMessagingClient, s.mockMetricClient, s.mockClientBean, s.logger)
	mBlobstore := &mocks.Client{}
	mBlobstore.On("BucketMetadata", mock.Anything, mock.Anything).Return(bucketMetadataResponse("test-owner", 10), nil)
	wh := NewWorkflowHandler(mService, config, mMetadataManager, s.mockHistoryMgr, s.mockHistoryV2Mgr,
		s.mockVisibilityMgr, s.mockProducer, mBlobstore)
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

func (s *workflowHandlerSuite) TestUpdateDomain_Success_ArchivalDisabledToArchivalEnabled() {
	config := NewConfig(dc.NewCollection(dc.NewNopClient(), s.logger))
	mMetadataManager := &mocks.MetadataManager{}
	mMetadataManager.On("GetMetadata").Return(&persistence.GetMetadataResponse{
		NotificationVersion: int64(0),
	}, nil)
	mMetadataManager.On("GetDomain", mock.Anything).Return(persistenceGetDomainResponse("bucket-name", shared.ArchivalStatusDisabled), nil)
	clusterMetadata := &mocks.ClusterMetadata{}
	clusterMetadata.On("IsGlobalDomainEnabled").Return(false)
	mService := cs.NewTestService(clusterMetadata, s.mockMessagingClient, s.mockMetricClient, s.mockClientBean, s.logger)
	mBlobstore := &mocks.Client{}
	mBlobstore.On("BucketMetadata", mock.Anything, mock.Anything).Return(bucketMetadataResponse("test-owner", 10), nil)
	wh := NewWorkflowHandler(mService, config, mMetadataManager, s.mockHistoryMgr, s.mockHistoryV2Mgr,
		s.mockVisibilityMgr, s.mockProducer, mBlobstore)
	wh.metricsClient = wh.Service.GetMetricsClient()
	wh.startWG.Done()

	updateReq := updateRequest(nil, common.ArchivalStatusPtr(shared.ArchivalStatusEnabled), nil, nil)
	result, err := wh.UpdateDomain(context.Background(), updateReq)
	assert.NoError(s.T(), err)
	assert.NotNil(s.T(), result)
	assert.NotNil(s.T(), result.Configuration)
	assert.Equal(s.T(), result.Configuration.GetArchivalStatus(), shared.ArchivalStatusEnabled)
	assert.Equal(s.T(), result.Configuration.GetArchivalBucketName(), "bucket-name")
	assert.Equal(s.T(), result.Configuration.GetArchivalBucketOwner(), "test-owner")
	assert.Equal(s.T(), result.Configuration.GetArchivalRetentionPeriodInDays(), int32(10))
}

func (s *workflowHandlerSuite) TestUpdateDomain_Success_ArchivalNeverEnabledToEnabledWithoutCustomBucket() {
	config := NewConfig(dc.NewCollection(dc.NewNopClient(), s.logger))
	mMetadataManager := &mocks.MetadataManager{}
	mMetadataManager.On("GetMetadata").Return(&persistence.GetMetadataResponse{
		NotificationVersion: int64(0),
	}, nil)
	mMetadataManager.On("GetDomain", mock.Anything).Return(persistenceGetDomainResponse("", shared.ArchivalStatusNeverEnabled), nil)
	clusterMetadata := &mocks.ClusterMetadata{}
	clusterMetadata.On("IsGlobalDomainEnabled").Return(false)
	clusterMetadata.On("GetDefaultArchivalBucket").Return("test-archival-bucket")
	mService := cs.NewTestService(clusterMetadata, s.mockMessagingClient, s.mockMetricClient, s.mockClientBean, s.logger)
	mBlobstore := &mocks.Client{}
	mBlobstore.On("BucketMetadata", mock.Anything, mock.Anything).Return(bucketMetadataResponse("test-owner", 10), nil)
	wh := NewWorkflowHandler(mService, config, mMetadataManager, s.mockHistoryMgr, s.mockHistoryV2Mgr,
		s.mockVisibilityMgr, s.mockProducer, mBlobstore)
	wh.metricsClient = wh.Service.GetMetricsClient()
	wh.startWG.Done()

	updateReq := updateRequest(nil, common.ArchivalStatusPtr(shared.ArchivalStatusEnabled), nil, nil)
	result, err := wh.UpdateDomain(context.Background(), updateReq)
	assert.NoError(s.T(), err)
	assert.NotNil(s.T(), result)
	assert.NotNil(s.T(), result.Configuration)
	assert.Equal(s.T(), result.Configuration.GetArchivalStatus(), shared.ArchivalStatusEnabled)
	assert.Equal(s.T(), result.Configuration.GetArchivalBucketName(), "test-archival-bucket")
	assert.Equal(s.T(), result.Configuration.GetArchivalBucketOwner(), "test-owner")
	assert.Equal(s.T(), result.Configuration.GetArchivalRetentionPeriodInDays(), int32(10))
}

func (s *workflowHandlerSuite) TestUpdateDomain_Success_ArchivalNeverEnabledToEnabledWithCustomBucket() {
	config := NewConfig(dc.NewCollection(dc.NewNopClient(), s.logger))
	mMetadataManager := &mocks.MetadataManager{}
	mMetadataManager.On("GetMetadata").Return(&persistence.GetMetadataResponse{
		NotificationVersion: int64(0),
	}, nil)
	mMetadataManager.On("GetDomain", mock.Anything).Return(persistenceGetDomainResponse("", shared.ArchivalStatusNeverEnabled), nil)
	clusterMetadata := &mocks.ClusterMetadata{}
	clusterMetadata.On("IsGlobalDomainEnabled").Return(false)
	mService := cs.NewTestService(clusterMetadata, s.mockMessagingClient, s.mockMetricClient, s.mockClientBean, s.logger)
	mBlobstore := &mocks.Client{}
	mBlobstore.On("BucketMetadata", mock.Anything, mock.Anything).Return(bucketMetadataResponse("test-owner", 10), nil)
	wh := NewWorkflowHandler(mService, config, mMetadataManager, s.mockHistoryMgr, s.mockHistoryV2Mgr,
		s.mockVisibilityMgr, s.mockProducer, mBlobstore)
	wh.metricsClient = wh.Service.GetMetricsClient()
	wh.startWG.Done()

	updateReq := updateRequest(common.StringPtr("custom-bucket"), common.ArchivalStatusPtr(shared.ArchivalStatusEnabled), nil, nil)
	result, err := wh.UpdateDomain(context.Background(), updateReq)
	assert.NoError(s.T(), err)
	assert.NotNil(s.T(), result)
	assert.NotNil(s.T(), result.Configuration)
	assert.Equal(s.T(), result.Configuration.GetArchivalStatus(), shared.ArchivalStatusEnabled)
	assert.Equal(s.T(), result.Configuration.GetArchivalBucketName(), "custom-bucket")
	assert.Equal(s.T(), result.Configuration.GetArchivalBucketOwner(), "test-owner")
	assert.Equal(s.T(), result.Configuration.GetArchivalRetentionPeriodInDays(), int32(10))
	clusterMetadata.AssertNotCalled(s.T(), "GetDefaultArchivalBucket")
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
		TableVersion:                0,
	}
}

func registerDomainRequest(enableArchival bool, customBucketName *string) *shared.RegisterDomainRequest {
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
		ActiveClusterName:        common.StringPtr("active"),
		Data:                     make(map[string]string),
		SecurityToken:            common.StringPtr("token"),
		EnableArchival:           common.BoolPtr(enableArchival),
		CustomArchivalBucketName: customBucketName,
	}
}
