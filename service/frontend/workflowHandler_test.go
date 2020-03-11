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
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonproto "go.temporal.io/temporal-proto/common"
	"go.temporal.io/temporal-proto/enums"
	"go.temporal.io/temporal-proto/serviceerror"
	"go.temporal.io/temporal-proto/workflowservice"

	"github.com/temporalio/temporal/.gen/proto/historyservicemock"
	"github.com/temporalio/temporal/common"
	"github.com/temporalio/temporal/common/archiver"
	"github.com/temporalio/temporal/common/archiver/provider"
	"github.com/temporalio/temporal/common/cache"
	"github.com/temporalio/temporal/common/cluster"
	"github.com/temporalio/temporal/common/domain"
	"github.com/temporalio/temporal/common/messaging"
	"github.com/temporalio/temporal/common/metrics"
	"github.com/temporalio/temporal/common/mocks"
	"github.com/temporalio/temporal/common/persistence"
	"github.com/temporalio/temporal/common/resource"
	dc "github.com/temporalio/temporal/common/service/dynamicconfig"
)

const (
	numHistoryShards = 10

	testWorkflowID            = "test-workflow-id"
	testRunID                 = "test-run-id"
	testHistoryArchivalURI    = "testScheme://history/URI"
	testVisibilityArchivalURI = "testScheme://visibility/URI"
)

type (
	workflowHandlerSuite struct {
		suite.Suite
		*require.Assertions

		controller          *gomock.Controller
		mockResource        *resource.Test
		mockDomainCache     *cache.MockDomainCache
		mockHistoryClient   *historyservicemock.MockHistoryServiceClient
		mockClusterMetadata *cluster.MockMetadata

		mockProducer           *mocks.KafkaProducer
		mockMessagingClient    messaging.Client
		mockMetadataMgr        *mocks.MetadataManager
		mockHistoryV2Mgr       *mocks.HistoryV2Manager
		mockVisibilityMgr      *mocks.VisibilityManager
		mockArchivalMetadata   *archiver.MockArchivalMetadata
		mockArchiverProvider   *provider.MockArchiverProvider
		mockHistoryArchiver    *archiver.HistoryArchiverMock
		mockVisibilityArchiver *archiver.VisibilityArchiverMock

		testDomain   string
		testDomainID string
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
	s.Assertions = require.New(s.T())

	s.testDomain = "test-domain"
	s.testDomainID = "e4f90ec0-1313-45be-9877-8aa41f72a45a"

	s.controller = gomock.NewController(s.T())
	s.mockResource = resource.NewTest(s.controller, metrics.Frontend)
	s.mockDomainCache = s.mockResource.DomainCache
	s.mockHistoryClient = s.mockResource.HistoryClient
	s.mockClusterMetadata = s.mockResource.ClusterMetadata
	s.mockMetadataMgr = s.mockResource.MetadataMgr
	s.mockHistoryV2Mgr = s.mockResource.HistoryMgr
	s.mockVisibilityMgr = s.mockResource.VisibilityMgr
	s.mockArchivalMetadata = s.mockResource.ArchivalMetadata
	s.mockArchiverProvider = s.mockResource.ArchiverProvider

	s.mockProducer = &mocks.KafkaProducer{}
	s.mockMessagingClient = mocks.NewMockMessagingClient(s.mockProducer, nil)
	s.mockHistoryArchiver = &archiver.HistoryArchiverMock{}
	s.mockVisibilityArchiver = &archiver.VisibilityArchiverMock{}
}

func (s *workflowHandlerSuite) TearDownTest() {
	s.controller.Finish()
	s.mockResource.Finish(s.T())
	s.mockProducer.AssertExpectations(s.T())
	s.mockHistoryArchiver.AssertExpectations(s.T())
	s.mockVisibilityArchiver.AssertExpectations(s.T())
}

func (s *workflowHandlerSuite) getWorkflowHandler(config *Config) *WorkflowHandler {
	return NewWorkflowHandler(s.mockResource, config, s.mockProducer)
}

func (s *workflowHandlerSuite) TestDisableListVisibilityByFilter() {
	testDomain := "test-domain"
	domainID := uuid.New()
	config := s.newConfig()
	config.DisableListVisibilityByFilter = dc.GetBoolPropertyFnFilteredByDomain(true)

	wh := s.getWorkflowHandler(config)

	s.mockDomainCache.EXPECT().GetDomainID(gomock.Any()).Return(domainID, nil).AnyTimes()

	// test list open by wid
	listRequest := &workflowservice.ListOpenWorkflowExecutionsRequest{
		Domain: testDomain,
		StartTimeFilter: &commonproto.StartTimeFilter{
			EarliestTime: 0,
			LatestTime:   time.Now().UnixNano(),
		},
		Filters: &workflowservice.ListOpenWorkflowExecutionsRequest_ExecutionFilter{ExecutionFilter: &commonproto.WorkflowExecutionFilter{
			WorkflowId: "wid",
		}},
	}
	_, err := wh.ListOpenWorkflowExecutions(context.Background(), listRequest)
	s.Error(err)
	s.Equal(errNoPermission, err)

	// test list open by workflow type
	listRequest.Filters = &workflowservice.ListOpenWorkflowExecutionsRequest_TypeFilter{TypeFilter: &commonproto.WorkflowTypeFilter{
		Name: "workflow-type",
	}}
	_, err = wh.ListOpenWorkflowExecutions(context.Background(), listRequest)
	s.Error(err)
	s.Equal(errNoPermission, err)

	// test list close by wid
	listRequest2 := &workflowservice.ListClosedWorkflowExecutionsRequest{
		Domain: testDomain,
		StartTimeFilter: &commonproto.StartTimeFilter{
			EarliestTime: 0,
			LatestTime:   time.Now().UnixNano(),
		},
		Filters: &workflowservice.ListClosedWorkflowExecutionsRequest_ExecutionFilter{ExecutionFilter: &commonproto.WorkflowExecutionFilter{
			WorkflowId: "wid",
		}},
	}
	_, err = wh.ListClosedWorkflowExecutions(context.Background(), listRequest2)
	s.Error(err)
	s.Equal(errNoPermission, err)

	// test list close by workflow type
	listRequest2.Filters = &workflowservice.ListClosedWorkflowExecutionsRequest_TypeFilter{TypeFilter: &commonproto.WorkflowTypeFilter{
		Name: "workflow-type",
	}}
	_, err = wh.ListClosedWorkflowExecutions(context.Background(), listRequest2)
	s.Error(err)
	s.Equal(errNoPermission, err)

	// test list close by workflow status
	failedStatus := enums.WorkflowExecutionCloseStatusFailed
	listRequest2.Filters = &workflowservice.ListClosedWorkflowExecutionsRequest_StatusFilter{StatusFilter: &commonproto.StatusFilter{CloseStatus: failedStatus}}
	_, err = wh.ListClosedWorkflowExecutions(context.Background(), listRequest2)
	s.Error(err)
	s.Equal(errNoPermission, err)
}

func (s *workflowHandlerSuite) TestPollForTask_Failed_ContextTimeoutTooShort() {
	config := s.newConfig()
	wh := s.getWorkflowHandler(config)

	bgCtx := context.Background()
	_, err := wh.PollForDecisionTask(bgCtx, &workflowservice.PollForDecisionTaskRequest{})
	s.Error(err)
	s.Equal(common.ErrContextTimeoutNotSet, err)

	_, err = wh.PollForActivityTask(bgCtx, &workflowservice.PollForActivityTaskRequest{})
	s.Error(err)
	s.Equal(common.ErrContextTimeoutNotSet, err)

	shortCtx, cancel := context.WithTimeout(bgCtx, common.MinLongPollTimeout-time.Millisecond)
	defer cancel()

	_, err = wh.PollForDecisionTask(shortCtx, &workflowservice.PollForDecisionTaskRequest{})
	s.Error(err)
	s.Equal(common.ErrContextTimeoutTooShort, err)

	_, err = wh.PollForActivityTask(shortCtx, &workflowservice.PollForActivityTaskRequest{})
	s.Error(err)
	s.Equal(common.ErrContextTimeoutTooShort, err)
}

func (s *workflowHandlerSuite) TestStartWorkflowExecution_Failed_RequestIdNotSet() {
	config := s.newConfig()
	config.RPS = dc.GetIntPropertyFn(10)
	wh := s.getWorkflowHandler(config)

	startWorkflowExecutionRequest := &workflowservice.StartWorkflowExecutionRequest{
		Domain:     "test-domain",
		WorkflowId: "workflow-id",
		WorkflowType: &commonproto.WorkflowType{
			Name: "workflow-type",
		},
		TaskList: &commonproto.TaskList{
			Name: "task-list",
		},
		ExecutionStartToCloseTimeoutSeconds: 1,
		TaskStartToCloseTimeoutSeconds:      1,
		RetryPolicy: &commonproto.RetryPolicy{
			InitialIntervalInSeconds:    1,
			BackoffCoefficient:          2,
			MaximumIntervalInSeconds:    2,
			MaximumAttempts:             1,
			ExpirationIntervalInSeconds: 1,
		},
	}
	_, err := wh.StartWorkflowExecution(context.Background(), startWorkflowExecutionRequest)
	s.Error(err)
	s.Equal(errRequestIDNotSet, err)
}

func (s *workflowHandlerSuite) TestStartWorkflowExecution_Failed_StartRequestNotSet() {
	config := s.newConfig()
	config.RPS = dc.GetIntPropertyFn(10)
	wh := s.getWorkflowHandler(config)

	_, err := wh.StartWorkflowExecution(context.Background(), nil)
	s.Error(err)
	s.Equal(errRequestNotSet, err)
}

func (s *workflowHandlerSuite) TestStartWorkflowExecution_Failed_DomainNotSet() {
	config := s.newConfig()
	config.RPS = dc.GetIntPropertyFn(10)
	wh := s.getWorkflowHandler(config)

	startWorkflowExecutionRequest := &workflowservice.StartWorkflowExecutionRequest{
		WorkflowId: "workflow-id",
		WorkflowType: &commonproto.WorkflowType{
			Name: "workflow-type",
		},
		TaskList: &commonproto.TaskList{
			Name: "task-list",
		},
		ExecutionStartToCloseTimeoutSeconds: 1,
		TaskStartToCloseTimeoutSeconds:      1,
		RetryPolicy: &commonproto.RetryPolicy{
			InitialIntervalInSeconds:    1,
			BackoffCoefficient:          2,
			MaximumIntervalInSeconds:    2,
			MaximumAttempts:             1,
			ExpirationIntervalInSeconds: 1,
		},
		RequestId: uuid.New(),
	}
	_, err := wh.StartWorkflowExecution(context.Background(), startWorkflowExecutionRequest)
	s.Error(err)
	s.Equal(errDomainNotSet, err)
}

func (s *workflowHandlerSuite) TestStartWorkflowExecution_Failed_WorkflowIdNotSet() {
	config := s.newConfig()
	config.RPS = dc.GetIntPropertyFn(10)
	wh := s.getWorkflowHandler(config)

	startWorkflowExecutionRequest := &workflowservice.StartWorkflowExecutionRequest{
		Domain: "test-domain",
		WorkflowType: &commonproto.WorkflowType{
			Name: "workflow-type",
		},
		TaskList: &commonproto.TaskList{
			Name: "task-list",
		},
		ExecutionStartToCloseTimeoutSeconds: 1,
		TaskStartToCloseTimeoutSeconds:      1,
		RetryPolicy: &commonproto.RetryPolicy{
			InitialIntervalInSeconds:    1,
			BackoffCoefficient:          2,
			MaximumIntervalInSeconds:    2,
			MaximumAttempts:             1,
			ExpirationIntervalInSeconds: 1,
		},
		RequestId: uuid.New(),
	}
	_, err := wh.StartWorkflowExecution(context.Background(), startWorkflowExecutionRequest)
	s.Error(err)
	s.Equal(errWorkflowIDNotSet, err)
}

func (s *workflowHandlerSuite) TestStartWorkflowExecution_Failed_WorkflowTypeNotSet() {
	config := s.newConfig()
	config.RPS = dc.GetIntPropertyFn(10)
	wh := s.getWorkflowHandler(config)

	startWorkflowExecutionRequest := &workflowservice.StartWorkflowExecutionRequest{
		Domain:     "test-domain",
		WorkflowId: "workflow-id",
		WorkflowType: &commonproto.WorkflowType{
			Name: "",
		},
		TaskList: &commonproto.TaskList{
			Name: "task-list",
		},
		ExecutionStartToCloseTimeoutSeconds: 1,
		TaskStartToCloseTimeoutSeconds:      1,
		RetryPolicy: &commonproto.RetryPolicy{
			InitialIntervalInSeconds:    1,
			BackoffCoefficient:          2,
			MaximumIntervalInSeconds:    2,
			MaximumAttempts:             1,
			ExpirationIntervalInSeconds: 1,
		},
		RequestId: uuid.New(),
	}
	_, err := wh.StartWorkflowExecution(context.Background(), startWorkflowExecutionRequest)
	s.Error(err)
	s.Equal(errWorkflowTypeNotSet, err)
}

func (s *workflowHandlerSuite) TestStartWorkflowExecution_Failed_TaskListNotSet() {
	config := s.newConfig()
	config.RPS = dc.GetIntPropertyFn(10)
	wh := s.getWorkflowHandler(config)

	startWorkflowExecutionRequest := &workflowservice.StartWorkflowExecutionRequest{
		Domain:     "test-domain",
		WorkflowId: "workflow-id",
		WorkflowType: &commonproto.WorkflowType{
			Name: "workflow-type",
		},
		TaskList: &commonproto.TaskList{
			Name: "",
		},
		ExecutionStartToCloseTimeoutSeconds: 1,
		TaskStartToCloseTimeoutSeconds:      1,
		RetryPolicy: &commonproto.RetryPolicy{
			InitialIntervalInSeconds:    1,
			BackoffCoefficient:          2,
			MaximumIntervalInSeconds:    2,
			MaximumAttempts:             1,
			ExpirationIntervalInSeconds: 1,
		},
		RequestId: uuid.New(),
	}
	_, err := wh.StartWorkflowExecution(context.Background(), startWorkflowExecutionRequest)
	s.Error(err)
	s.Equal(errTaskListNotSet, err)
}

func (s *workflowHandlerSuite) TestStartWorkflowExecution_Failed_InvalidExecutionStartToCloseTimeout() {
	config := s.newConfig()
	config.RPS = dc.GetIntPropertyFn(10)
	wh := s.getWorkflowHandler(config)

	startWorkflowExecutionRequest := &workflowservice.StartWorkflowExecutionRequest{
		Domain:     "test-domain",
		WorkflowId: "workflow-id",
		WorkflowType: &commonproto.WorkflowType{
			Name: "workflow-type",
		},
		TaskList: &commonproto.TaskList{
			Name: "task-list",
		},
		ExecutionStartToCloseTimeoutSeconds: 0,
		TaskStartToCloseTimeoutSeconds:      1,
		RetryPolicy: &commonproto.RetryPolicy{
			InitialIntervalInSeconds:    1,
			BackoffCoefficient:          2,
			MaximumIntervalInSeconds:    2,
			MaximumAttempts:             1,
			ExpirationIntervalInSeconds: 1,
		},
		RequestId: uuid.New(),
	}
	_, err := wh.StartWorkflowExecution(context.Background(), startWorkflowExecutionRequest)
	s.Error(err)
	s.Equal(errInvalidExecutionStartToCloseTimeoutSeconds, err)
}

func (s *workflowHandlerSuite) TestStartWorkflowExecution_Failed_InvalidTaskStartToCloseTimeout() {
	config := s.newConfig()
	config.RPS = dc.GetIntPropertyFn(10)
	wh := s.getWorkflowHandler(config)

	startWorkflowExecutionRequest := &workflowservice.StartWorkflowExecutionRequest{
		Domain:     "test-domain",
		WorkflowId: "workflow-id",
		WorkflowType: &commonproto.WorkflowType{
			Name: "workflow-type",
		},
		TaskList: &commonproto.TaskList{
			Name: "task-list",
		},
		ExecutionStartToCloseTimeoutSeconds: 1,
		TaskStartToCloseTimeoutSeconds:      0,
		RetryPolicy: &commonproto.RetryPolicy{
			InitialIntervalInSeconds:    1,
			BackoffCoefficient:          2,
			MaximumIntervalInSeconds:    2,
			MaximumAttempts:             1,
			ExpirationIntervalInSeconds: 1,
		},
		RequestId: uuid.New(),
	}
	_, err := wh.StartWorkflowExecution(context.Background(), startWorkflowExecutionRequest)
	s.Error(err)
	s.Equal(errInvalidTaskStartToCloseTimeoutSeconds, err)
}

func (s *workflowHandlerSuite) TestRegisterDomain_Failure_InvalidArchivalURI() {
	s.mockClusterMetadata.EXPECT().IsGlobalDomainEnabled().Return(false)
	s.mockArchivalMetadata.On("GetHistoryConfig").Return(archiver.NewArchivalConfig("enabled", dc.GetStringPropertyFn("enabled"), dc.GetBoolPropertyFn(true), "disabled", "random URI"))
	s.mockArchivalMetadata.On("GetVisibilityConfig").Return(archiver.NewArchivalConfig("enabled", dc.GetStringPropertyFn("enabled"), dc.GetBoolPropertyFn(true), "disabled", "random URI"))
	s.mockMetadataMgr.On("GetDomain", mock.Anything).Return(nil, serviceerror.NewNotFound(""))
	s.mockHistoryArchiver.On("ValidateURI", mock.Anything).Return(nil)
	s.mockVisibilityArchiver.On("ValidateURI", mock.Anything).Return(errors.New("invalid URI"))
	s.mockArchiverProvider.On("GetHistoryArchiver", mock.Anything, mock.Anything).Return(s.mockHistoryArchiver, nil)
	s.mockArchiverProvider.On("GetVisibilityArchiver", mock.Anything, mock.Anything).Return(s.mockVisibilityArchiver, nil)

	wh := s.getWorkflowHandler(s.newConfig())

	req := registerDomainRequest(
		enums.ArchivalStatusEnabled,
		testHistoryArchivalURI,
		enums.ArchivalStatusEnabled,
		testVisibilityArchivalURI,
	)
	_, err := wh.RegisterDomain(context.Background(), req)
	s.Error(err)
}

func (s *workflowHandlerSuite) TestRegisterDomain_Success_EnabledWithNoArchivalURI() {
	s.mockClusterMetadata.EXPECT().IsGlobalDomainEnabled().Return(false)
	s.mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestAllClusterInfo).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockArchivalMetadata.On("GetHistoryConfig").Return(archiver.NewArchivalConfig("enabled", dc.GetStringPropertyFn("enabled"), dc.GetBoolPropertyFn(true), "disabled", testHistoryArchivalURI))
	s.mockArchivalMetadata.On("GetVisibilityConfig").Return(archiver.NewArchivalConfig("enabled", dc.GetStringPropertyFn("enabled"), dc.GetBoolPropertyFn(true), "disabled", testVisibilityArchivalURI))
	s.mockMetadataMgr.On("GetDomain", mock.Anything).Return(nil, serviceerror.NewNotFound(""))
	s.mockMetadataMgr.On("CreateDomain", mock.Anything).Return(&persistence.CreateDomainResponse{
		ID: "test-id",
	}, nil)
	s.mockHistoryArchiver.On("ValidateURI", mock.Anything).Return(nil)
	s.mockVisibilityArchiver.On("ValidateURI", mock.Anything).Return(nil)
	s.mockArchiverProvider.On("GetHistoryArchiver", mock.Anything, mock.Anything).Return(s.mockHistoryArchiver, nil)
	s.mockArchiverProvider.On("GetVisibilityArchiver", mock.Anything, mock.Anything).Return(s.mockVisibilityArchiver, nil)

	wh := s.getWorkflowHandler(s.newConfig())

	req := registerDomainRequest(enums.ArchivalStatusEnabled, "", enums.ArchivalStatusEnabled, "")
	_, err := wh.RegisterDomain(context.Background(), req)
	s.NoError(err)
}

func (s *workflowHandlerSuite) TestRegisterDomain_Success_EnabledWithArchivalURI() {
	s.mockClusterMetadata.EXPECT().IsGlobalDomainEnabled().Return(false)
	s.mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestAllClusterInfo).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockArchivalMetadata.On("GetHistoryConfig").Return(archiver.NewArchivalConfig("enabled", dc.GetStringPropertyFn("enabled"), dc.GetBoolPropertyFn(true), "disabled", "invalidURI"))
	s.mockArchivalMetadata.On("GetVisibilityConfig").Return(archiver.NewArchivalConfig("enabled", dc.GetStringPropertyFn("enabled"), dc.GetBoolPropertyFn(true), "disabled", "invalidURI"))
	s.mockMetadataMgr.On("GetDomain", mock.Anything).Return(nil, serviceerror.NewNotFound(""))
	s.mockMetadataMgr.On("CreateDomain", mock.Anything).Return(&persistence.CreateDomainResponse{
		ID: "test-id",
	}, nil)
	s.mockHistoryArchiver.On("ValidateURI", mock.Anything).Return(nil)
	s.mockVisibilityArchiver.On("ValidateURI", mock.Anything).Return(nil)
	s.mockArchiverProvider.On("GetHistoryArchiver", mock.Anything, mock.Anything).Return(s.mockHistoryArchiver, nil)
	s.mockArchiverProvider.On("GetVisibilityArchiver", mock.Anything, mock.Anything).Return(s.mockVisibilityArchiver, nil)

	wh := s.getWorkflowHandler(s.newConfig())

	req := registerDomainRequest(
		enums.ArchivalStatusEnabled,
		testHistoryArchivalURI,
		enums.ArchivalStatusEnabled,
		testVisibilityArchivalURI,
	)
	_, err := wh.RegisterDomain(context.Background(), req)
	s.NoError(err)
}

func (s *workflowHandlerSuite) TestRegisterDomain_Success_ClusterNotConfiguredForArchival() {
	s.mockClusterMetadata.EXPECT().IsGlobalDomainEnabled().Return(false)
	s.mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestAllClusterInfo).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockArchivalMetadata.On("GetHistoryConfig").Return(archiver.NewDisabledArchvialConfig())
	s.mockArchivalMetadata.On("GetVisibilityConfig").Return(archiver.NewDisabledArchvialConfig())
	s.mockMetadataMgr.On("GetDomain", mock.Anything).Return(nil, serviceerror.NewNotFound(""))
	s.mockMetadataMgr.On("CreateDomain", mock.Anything).Return(&persistence.CreateDomainResponse{
		ID: "test-id",
	}, nil)

	wh := s.getWorkflowHandler(s.newConfig())

	req := registerDomainRequest(
		enums.ArchivalStatusEnabled,
		testVisibilityArchivalURI,
		enums.ArchivalStatusEnabled,
		"invalidURI",
	)
	_, err := wh.RegisterDomain(context.Background(), req)
	s.NoError(err)
}

func (s *workflowHandlerSuite) TestRegisterDomain_Success_NotEnabled() {
	s.mockClusterMetadata.EXPECT().IsGlobalDomainEnabled().Return(false)
	s.mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestAllClusterInfo).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockArchivalMetadata.On("GetHistoryConfig").Return(archiver.NewArchivalConfig("enabled", dc.GetStringPropertyFn("enabled"), dc.GetBoolPropertyFn(true), "disabled", "some random URI"))
	s.mockArchivalMetadata.On("GetVisibilityConfig").Return(archiver.NewArchivalConfig("enabled", dc.GetStringPropertyFn("enabled"), dc.GetBoolPropertyFn(true), "disabled", "some random URI"))
	s.mockMetadataMgr.On("GetDomain", mock.Anything).Return(nil, serviceerror.NewNotFound(""))
	s.mockMetadataMgr.On("CreateDomain", mock.Anything).Return(&persistence.CreateDomainResponse{
		ID: "test-id",
	}, nil)

	wh := s.getWorkflowHandler(s.newConfig())

	req := registerDomainRequest(enums.ArchivalStatusDefault, "", enums.ArchivalStatusDefault, "")
	_, err := wh.RegisterDomain(context.Background(), req)
	s.NoError(err)
}

func (s *workflowHandlerSuite) TestDescribeDomain_Success_ArchivalDisabled() {
	getDomainResp := persistenceGetDomainResponse(
		&domain.ArchivalState{Status: enums.ArchivalStatusDisabled, URI: ""},
		&domain.ArchivalState{Status: enums.ArchivalStatusDisabled, URI: ""},
	)
	s.mockMetadataMgr.On("GetDomain", mock.Anything).Return(getDomainResp, nil)

	wh := s.getWorkflowHandler(s.newConfig())

	req := &workflowservice.DescribeDomainRequest{
		Name: "test-domain",
	}
	result, err := wh.DescribeDomain(context.Background(), req)

	s.NoError(err)
	s.NotNil(result)
	s.NotNil(result.Configuration)
	s.Equal(enums.ArchivalStatusDisabled, result.Configuration.GetHistoryArchivalStatus())
	s.Equal("", result.Configuration.GetHistoryArchivalURI())
	s.Equal(enums.ArchivalStatusDisabled, result.Configuration.GetVisibilityArchivalStatus())
	s.Equal("", result.Configuration.GetVisibilityArchivalURI())
}

func (s *workflowHandlerSuite) TestDescribeDomain_Success_ArchivalEnabled() {
	getDomainResp := persistenceGetDomainResponse(
		&domain.ArchivalState{Status: enums.ArchivalStatusEnabled, URI: testHistoryArchivalURI},
		&domain.ArchivalState{Status: enums.ArchivalStatusEnabled, URI: testVisibilityArchivalURI},
	)
	s.mockMetadataMgr.On("GetDomain", mock.Anything).Return(getDomainResp, nil)

	wh := s.getWorkflowHandler(s.newConfig())

	req := &workflowservice.DescribeDomainRequest{
		Name: "test-domain",
	}
	result, err := wh.DescribeDomain(context.Background(), req)

	s.NoError(err)
	s.NotNil(result)
	s.NotNil(result.Configuration)
	s.Equal(enums.ArchivalStatusEnabled, result.Configuration.GetHistoryArchivalStatus())
	s.Equal(testHistoryArchivalURI, result.Configuration.GetHistoryArchivalURI())
	s.Equal(enums.ArchivalStatusEnabled, result.Configuration.GetVisibilityArchivalStatus())
	s.Equal(testVisibilityArchivalURI, result.Configuration.GetVisibilityArchivalURI())
}

func (s *workflowHandlerSuite) TestUpdateDomain_Failure_UpdateExistingArchivalURI() {
	s.mockMetadataMgr.On("GetMetadata").Return(&persistence.GetMetadataResponse{
		NotificationVersion: int64(0),
	}, nil)
	getDomainResp := persistenceGetDomainResponse(
		&domain.ArchivalState{Status: enums.ArchivalStatusEnabled, URI: testHistoryArchivalURI},
		&domain.ArchivalState{Status: enums.ArchivalStatusEnabled, URI: testVisibilityArchivalURI},
	)
	s.mockMetadataMgr.On("GetDomain", mock.Anything).Return(getDomainResp, nil)
	s.mockArchivalMetadata.On("GetHistoryConfig").Return(archiver.NewArchivalConfig("enabled", dc.GetStringPropertyFn("enabled"), dc.GetBoolPropertyFn(true), "disabled", "some random URI"))
	s.mockArchivalMetadata.On("GetVisibilityConfig").Return(archiver.NewArchivalConfig("enabled", dc.GetStringPropertyFn("enabled"), dc.GetBoolPropertyFn(true), "disabled", "some random URI"))
	s.mockHistoryArchiver.On("ValidateURI", mock.Anything).Return(nil)
	s.mockArchiverProvider.On("GetHistoryArchiver", mock.Anything, mock.Anything).Return(s.mockHistoryArchiver, nil)

	wh := s.getWorkflowHandler(s.newConfig())

	updateReq := updateRequest(
		"",
		enums.ArchivalStatusDefault,
		"updated visibility URI",
		enums.ArchivalStatusDefault,
	)
	_, err := wh.UpdateDomain(context.Background(), updateReq)
	s.Error(err)
}

func (s *workflowHandlerSuite) TestUpdateDomain_Failure_InvalidArchivalURI() {
	s.mockMetadataMgr.On("GetMetadata").Return(&persistence.GetMetadataResponse{
		NotificationVersion: int64(0),
	}, nil)
	getDomainResp := persistenceGetDomainResponse(
		&domain.ArchivalState{Status: enums.ArchivalStatusDisabled, URI: ""},
		&domain.ArchivalState{Status: enums.ArchivalStatusDisabled, URI: ""},
	)
	s.mockMetadataMgr.On("GetDomain", mock.Anything).Return(getDomainResp, nil)
	s.mockArchivalMetadata.On("GetHistoryConfig").Return(archiver.NewArchivalConfig("enabled", dc.GetStringPropertyFn("enabled"), dc.GetBoolPropertyFn(true), "disabled", "some random URI"))
	s.mockHistoryArchiver.On("ValidateURI", mock.Anything).Return(errors.New("invalid URI"))
	s.mockArchiverProvider.On("GetHistoryArchiver", mock.Anything, mock.Anything).Return(s.mockHistoryArchiver, nil)

	wh := s.getWorkflowHandler(s.newConfig())

	updateReq := updateRequest(
		"testScheme://invalid/updated/history/URI",
		enums.ArchivalStatusEnabled,
		"",
		enums.ArchivalStatusDefault,
	)
	_, err := wh.UpdateDomain(context.Background(), updateReq)
	s.Error(err)
}

func (s *workflowHandlerSuite) TestUpdateDomain_Success_ArchivalEnabledToArchivalDisabledWithoutSettingURI() {
	s.mockMetadataMgr.On("GetMetadata").Return(&persistence.GetMetadataResponse{
		NotificationVersion: int64(0),
	}, nil)
	getDomainResp := persistenceGetDomainResponse(
		&domain.ArchivalState{Status: enums.ArchivalStatusEnabled, URI: testHistoryArchivalURI},
		&domain.ArchivalState{Status: enums.ArchivalStatusEnabled, URI: testVisibilityArchivalURI},
	)
	s.mockMetadataMgr.On("GetDomain", mock.Anything).Return(getDomainResp, nil)
	s.mockMetadataMgr.On("UpdateDomain", mock.Anything).Return(nil)
	s.mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestAllClusterInfo).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockArchivalMetadata.On("GetHistoryConfig").Return(archiver.NewArchivalConfig("enabled", dc.GetStringPropertyFn("enabled"), dc.GetBoolPropertyFn(true), "disabled", "some random URI"))
	s.mockArchivalMetadata.On("GetVisibilityConfig").Return(archiver.NewArchivalConfig("enabled", dc.GetStringPropertyFn("enabled"), dc.GetBoolPropertyFn(true), "disabled", "some random URI"))
	s.mockHistoryArchiver.On("ValidateURI", mock.Anything).Return(nil)
	s.mockVisibilityArchiver.On("ValidateURI", mock.Anything).Return(nil)
	s.mockArchiverProvider.On("GetHistoryArchiver", mock.Anything, mock.Anything).Return(s.mockHistoryArchiver, nil)
	s.mockArchiverProvider.On("GetVisibilityArchiver", mock.Anything, mock.Anything).Return(s.mockVisibilityArchiver, nil)

	wh := s.getWorkflowHandler(s.newConfig())

	updateReq := updateRequest(
		"",
		enums.ArchivalStatusDisabled,
		"",
		enums.ArchivalStatusDisabled,
	)
	result, err := wh.UpdateDomain(context.Background(), updateReq)
	s.NoError(err)
	s.NotNil(result)
	s.NotNil(result.Configuration)
	s.Equal(enums.ArchivalStatusDisabled, result.Configuration.GetHistoryArchivalStatus())
	s.Equal(testHistoryArchivalURI, result.Configuration.GetHistoryArchivalURI())
	s.Equal(enums.ArchivalStatusDisabled, result.Configuration.GetVisibilityArchivalStatus())
	s.Equal(testVisibilityArchivalURI, result.Configuration.GetVisibilityArchivalURI())
}

func (s *workflowHandlerSuite) TestUpdateDomain_Success_ClusterNotConfiguredForArchival() {
	s.mockMetadataMgr.On("GetMetadata").Return(&persistence.GetMetadataResponse{
		NotificationVersion: int64(0),
	}, nil)
	getDomainResp := persistenceGetDomainResponse(
		&domain.ArchivalState{Status: enums.ArchivalStatusEnabled, URI: "some random history URI"},
		&domain.ArchivalState{Status: enums.ArchivalStatusEnabled, URI: "some random visibility URI"},
	)
	s.mockMetadataMgr.On("GetDomain", mock.Anything).Return(getDomainResp, nil)
	s.mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestAllClusterInfo).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockArchivalMetadata.On("GetHistoryConfig").Return(archiver.NewDisabledArchvialConfig())
	s.mockArchivalMetadata.On("GetVisibilityConfig").Return(archiver.NewDisabledArchvialConfig())

	wh := s.getWorkflowHandler(s.newConfig())

	updateReq := updateRequest("", enums.ArchivalStatusDisabled, "", enums.ArchivalStatusDefault)
	result, err := wh.UpdateDomain(context.Background(), updateReq)
	s.NoError(err)
	s.NotNil(result)
	s.NotNil(result.Configuration)
	s.Equal(enums.ArchivalStatusEnabled, result.Configuration.GetHistoryArchivalStatus())
	s.Equal("some random history URI", result.Configuration.GetHistoryArchivalURI())
	s.Equal(enums.ArchivalStatusEnabled, result.Configuration.GetVisibilityArchivalStatus())
	s.Equal("some random visibility URI", result.Configuration.GetVisibilityArchivalURI())
}

func (s *workflowHandlerSuite) TestUpdateDomain_Success_ArchivalEnabledToArchivalDisabledWithSettingBucket() {
	s.mockMetadataMgr.On("GetMetadata").Return(&persistence.GetMetadataResponse{
		NotificationVersion: int64(0),
	}, nil)
	getDomainResp := persistenceGetDomainResponse(
		&domain.ArchivalState{Status: enums.ArchivalStatusEnabled, URI: testHistoryArchivalURI},
		&domain.ArchivalState{Status: enums.ArchivalStatusEnabled, URI: testVisibilityArchivalURI},
	)
	s.mockMetadataMgr.On("GetDomain", mock.Anything).Return(getDomainResp, nil)
	s.mockMetadataMgr.On("UpdateDomain", mock.Anything).Return(nil)
	s.mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestAllClusterInfo).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockArchivalMetadata.On("GetHistoryConfig").Return(archiver.NewArchivalConfig("enabled", dc.GetStringPropertyFn("enabled"), dc.GetBoolPropertyFn(true), "disabled", "some random URI"))
	s.mockArchivalMetadata.On("GetVisibilityConfig").Return(archiver.NewArchivalConfig("enabled", dc.GetStringPropertyFn("enabled"), dc.GetBoolPropertyFn(true), "disabled", "some random URI"))
	s.mockHistoryArchiver.On("ValidateURI", mock.Anything).Return(nil)
	s.mockVisibilityArchiver.On("ValidateURI", mock.Anything).Return(nil)
	s.mockArchiverProvider.On("GetHistoryArchiver", mock.Anything, mock.Anything).Return(s.mockHistoryArchiver, nil)
	s.mockArchiverProvider.On("GetVisibilityArchiver", mock.Anything, mock.Anything).Return(s.mockVisibilityArchiver, nil)

	wh := s.getWorkflowHandler(s.newConfig())

	updateReq := updateRequest(
		testHistoryArchivalURI,
		enums.ArchivalStatusDisabled,
		testVisibilityArchivalURI,
		enums.ArchivalStatusDisabled,
	)
	result, err := wh.UpdateDomain(context.Background(), updateReq)
	s.NoError(err)
	s.NotNil(result)
	s.NotNil(result.Configuration)
	s.Equal(enums.ArchivalStatusDisabled, result.Configuration.GetHistoryArchivalStatus())
	s.Equal(testHistoryArchivalURI, result.Configuration.GetHistoryArchivalURI())
	s.Equal(enums.ArchivalStatusDisabled, result.Configuration.GetVisibilityArchivalStatus())
	s.Equal(testVisibilityArchivalURI, result.Configuration.GetVisibilityArchivalURI())
}

func (s *workflowHandlerSuite) TestUpdateDomain_Success_ArchivalEnabledToEnabled() {
	s.mockMetadataMgr.On("GetMetadata").Return(&persistence.GetMetadataResponse{
		NotificationVersion: int64(0),
	}, nil)
	getDomainResp := persistenceGetDomainResponse(
		&domain.ArchivalState{Status: enums.ArchivalStatusEnabled, URI: testHistoryArchivalURI},
		&domain.ArchivalState{Status: enums.ArchivalStatusEnabled, URI: testVisibilityArchivalURI},
	)
	s.mockMetadataMgr.On("GetDomain", mock.Anything).Return(getDomainResp, nil)
	s.mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestAllClusterInfo).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockArchivalMetadata.On("GetHistoryConfig").Return(archiver.NewArchivalConfig("enabled", dc.GetStringPropertyFn("enabled"), dc.GetBoolPropertyFn(true), "disabled", "some random URI"))
	s.mockArchivalMetadata.On("GetVisibilityConfig").Return(archiver.NewArchivalConfig("enabled", dc.GetStringPropertyFn("enabled"), dc.GetBoolPropertyFn(true), "disabled", "some random URI"))
	s.mockHistoryArchiver.On("ValidateURI", mock.Anything).Return(nil)
	s.mockVisibilityArchiver.On("ValidateURI", mock.Anything).Return(nil)
	s.mockArchiverProvider.On("GetHistoryArchiver", mock.Anything, mock.Anything).Return(s.mockHistoryArchiver, nil)
	s.mockArchiverProvider.On("GetVisibilityArchiver", mock.Anything, mock.Anything).Return(s.mockVisibilityArchiver, nil)

	wh := s.getWorkflowHandler(s.newConfig())

	updateReq := updateRequest(
		testHistoryArchivalURI,
		enums.ArchivalStatusEnabled,
		testVisibilityArchivalURI,
		enums.ArchivalStatusEnabled,
	)
	result, err := wh.UpdateDomain(context.Background(), updateReq)
	s.NoError(err)
	s.NotNil(result)
	s.NotNil(result.Configuration)
	s.Equal(enums.ArchivalStatusEnabled, result.Configuration.GetHistoryArchivalStatus())
	s.Equal(testHistoryArchivalURI, result.Configuration.GetHistoryArchivalURI())
	s.Equal(enums.ArchivalStatusEnabled, result.Configuration.GetVisibilityArchivalStatus())
	s.Equal(testVisibilityArchivalURI, result.Configuration.GetVisibilityArchivalURI())
}

func (s *workflowHandlerSuite) TestUpdateDomain_Success_ArchivalNeverEnabledToEnabled() {
	s.mockMetadataMgr.On("GetMetadata").Return(&persistence.GetMetadataResponse{
		NotificationVersion: int64(0),
	}, nil)
	getDomainResp := persistenceGetDomainResponse(
		&domain.ArchivalState{Status: enums.ArchivalStatusDisabled, URI: ""},
		&domain.ArchivalState{Status: enums.ArchivalStatusDisabled, URI: ""},
	)
	s.mockMetadataMgr.On("GetDomain", mock.Anything).Return(getDomainResp, nil)
	s.mockMetadataMgr.On("UpdateDomain", mock.Anything).Return(nil)
	s.mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestAllClusterInfo).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockArchivalMetadata.On("GetHistoryConfig").Return(archiver.NewArchivalConfig("enabled", dc.GetStringPropertyFn("enabled"), dc.GetBoolPropertyFn(true), "disabled", "some random URI"))
	s.mockArchivalMetadata.On("GetVisibilityConfig").Return(archiver.NewArchivalConfig("enabled", dc.GetStringPropertyFn("enabled"), dc.GetBoolPropertyFn(true), "disabled", "some random URI"))
	s.mockHistoryArchiver.On("ValidateURI", mock.Anything).Return(nil)
	s.mockVisibilityArchiver.On("ValidateURI", mock.Anything).Return(nil)
	s.mockArchiverProvider.On("GetHistoryArchiver", mock.Anything, mock.Anything).Return(s.mockHistoryArchiver, nil)
	s.mockArchiverProvider.On("GetVisibilityArchiver", mock.Anything, mock.Anything).Return(s.mockVisibilityArchiver, nil)

	wh := s.getWorkflowHandler(s.newConfig())

	updateReq := updateRequest(
		testHistoryArchivalURI,
		enums.ArchivalStatusEnabled,
		testVisibilityArchivalURI,
		enums.ArchivalStatusEnabled,
	)
	result, err := wh.UpdateDomain(context.Background(), updateReq)
	s.NoError(err)
	s.NotNil(result)
	s.NotNil(result.Configuration)
	s.Equal(enums.ArchivalStatusEnabled, result.Configuration.GetHistoryArchivalStatus())
	s.Equal(testHistoryArchivalURI, result.Configuration.GetHistoryArchivalURI())
	s.Equal(enums.ArchivalStatusEnabled, result.Configuration.GetVisibilityArchivalStatus())
	s.Equal(testVisibilityArchivalURI, result.Configuration.GetVisibilityArchivalURI())
}

func (s *workflowHandlerSuite) TestHistoryArchived() {
	wh := s.getWorkflowHandler(s.newConfig())

	getHistoryRequest := &workflowservice.GetWorkflowExecutionHistoryRequest{}
	s.False(wh.historyArchived(context.Background(), getHistoryRequest, "test-domain"))

	getHistoryRequest = &workflowservice.GetWorkflowExecutionHistoryRequest{
		Execution: &commonproto.WorkflowExecution{},
	}
	s.False(wh.historyArchived(context.Background(), getHistoryRequest, "test-domain"))

	s.mockHistoryClient.EXPECT().GetMutableState(gomock.Any(), gomock.Any()).Return(nil, nil).Times(1)
	getHistoryRequest = &workflowservice.GetWorkflowExecutionHistoryRequest{
		Execution: &commonproto.WorkflowExecution{
			WorkflowId: testWorkflowID,
			RunId:      testRunID,
		},
	}
	s.False(wh.historyArchived(context.Background(), getHistoryRequest, "test-domain"))

	s.mockHistoryClient.EXPECT().GetMutableState(gomock.Any(), gomock.Any()).Return(nil, serviceerror.NewNotFound("got archival indication error")).Times(1)
	getHistoryRequest = &workflowservice.GetWorkflowExecutionHistoryRequest{
		Execution: &commonproto.WorkflowExecution{
			WorkflowId: testWorkflowID,
			RunId:      testRunID,
		},
	}
	s.True(wh.historyArchived(context.Background(), getHistoryRequest, "test-domain"))

	s.mockHistoryClient.EXPECT().GetMutableState(gomock.Any(), gomock.Any()).Return(nil, errors.New("got non-archival indication error")).Times(1)
	getHistoryRequest = &workflowservice.GetWorkflowExecutionHistoryRequest{
		Execution: &commonproto.WorkflowExecution{
			WorkflowId: testWorkflowID,
			RunId:      testRunID,
		},
	}
	s.False(wh.historyArchived(context.Background(), getHistoryRequest, "test-domain"))
}

func (s *workflowHandlerSuite) TestGetArchivedHistory_Failure_DomainCacheEntryError() {
	s.mockDomainCache.EXPECT().GetDomainByID(gomock.Any()).Return(nil, errors.New("error getting domain")).Times(1)

	wh := s.getWorkflowHandler(s.newConfig())

	resp, err := wh.getArchivedHistory(context.Background(), getHistoryRequest(nil), s.testDomainID, metrics.NoopScope(metrics.Frontend))
	s.Nil(resp)
	s.Error(err)
}

func (s *workflowHandlerSuite) TestGetArchivedHistory_Failure_ArchivalURIEmpty() {
	domainEntry := cache.NewLocalDomainCacheEntryForTest(
		&persistence.DomainInfo{Name: "test-domain"},
		&persistence.DomainConfig{
			HistoryArchivalStatus:    enums.ArchivalStatusDisabled,
			HistoryArchivalURI:       "",
			VisibilityArchivalStatus: enums.ArchivalStatusDisabled,
			VisibilityArchivalURI:    "",
		},
		"",
		nil)
	s.mockDomainCache.EXPECT().GetDomainByID(gomock.Any()).Return(domainEntry, nil).AnyTimes()

	wh := s.getWorkflowHandler(s.newConfig())

	resp, err := wh.getArchivedHistory(context.Background(), getHistoryRequest(nil), s.testDomainID, metrics.NoopScope(metrics.Frontend))
	s.Nil(resp)
	s.Error(err)
}

func (s *workflowHandlerSuite) TestGetArchivedHistory_Failure_InvalidURI() {
	domainEntry := cache.NewLocalDomainCacheEntryForTest(
		&persistence.DomainInfo{Name: "test-domain"},
		&persistence.DomainConfig{
			HistoryArchivalStatus:    enums.ArchivalStatusEnabled,
			HistoryArchivalURI:       "uri without scheme",
			VisibilityArchivalStatus: enums.ArchivalStatusDisabled,
			VisibilityArchivalURI:    "",
		},
		"",
		nil)
	s.mockDomainCache.EXPECT().GetDomainByID(gomock.Any()).Return(domainEntry, nil).AnyTimes()

	wh := s.getWorkflowHandler(s.newConfig())

	resp, err := wh.getArchivedHistory(context.Background(), getHistoryRequest(nil), s.testDomainID, metrics.NoopScope(metrics.Frontend))
	s.Nil(resp)
	s.Error(err)
}

func (s *workflowHandlerSuite) TestGetArchivedHistory_Success_GetFirstPage() {
	domainEntry := cache.NewLocalDomainCacheEntryForTest(
		&persistence.DomainInfo{Name: "test-domain"},
		&persistence.DomainConfig{
			HistoryArchivalStatus:    enums.ArchivalStatusEnabled,
			HistoryArchivalURI:       testHistoryArchivalURI,
			VisibilityArchivalStatus: enums.ArchivalStatusDisabled,
			VisibilityArchivalURI:    "",
		},
		"",
		nil)
	s.mockDomainCache.EXPECT().GetDomainByID(gomock.Any()).Return(domainEntry, nil).AnyTimes()

	nextPageToken := []byte{'1', '2', '3'}
	historyBatch1 := &commonproto.History{
		Events: []*commonproto.HistoryEvent{
			{EventId: 1},
			{EventId: 2},
		},
	}
	historyBatch2 := &commonproto.History{
		Events: []*commonproto.HistoryEvent{
			{EventId: 3},
			{EventId: 4},
			{EventId: 5},
		},
	}
	history := &commonproto.History{}
	history.Events = append(history.Events, historyBatch1.Events...)
	history.Events = append(history.Events, historyBatch2.Events...)
	s.mockHistoryArchiver.On("Get", mock.Anything, mock.Anything, mock.Anything).Return(&archiver.GetHistoryResponse{
		NextPageToken:  nextPageToken,
		HistoryBatches: []*commonproto.History{historyBatch1, historyBatch2},
	}, nil)
	s.mockArchiverProvider.On("GetHistoryArchiver", mock.Anything, mock.Anything).Return(s.mockHistoryArchiver, nil)

	wh := s.getWorkflowHandler(s.newConfig())

	resp, err := wh.getArchivedHistory(context.Background(), getHistoryRequest(nil), s.testDomainID, metrics.NoopScope(metrics.Frontend))
	s.NoError(err)
	s.NotNil(resp)
	s.NotNil(resp.History)
	s.Equal(history, resp.History)
	s.Equal(nextPageToken, resp.NextPageToken)
	s.True(resp.GetArchived())
}

func (s *workflowHandlerSuite) TestGetHistory() {
	domainID := uuid.New()
	firstEventID := int64(100)
	nextEventID := int64(101)
	branchToken := []byte{1}
	we := commonproto.WorkflowExecution{
		WorkflowId: "wid",
		RunId:      "rid",
	}
	shardID := common.WorkflowIDToHistoryShard(we.WorkflowId, numHistoryShards)
	req := &persistence.ReadHistoryBranchRequest{
		BranchToken:   branchToken,
		MinEventID:    firstEventID,
		MaxEventID:    nextEventID,
		PageSize:      0,
		NextPageToken: []byte{},
		ShardID:       common.IntPtr(shardID),
	}
	s.mockHistoryV2Mgr.On("ReadHistoryBranch", req).Return(&persistence.ReadHistoryBranchResponse{
		HistoryEvents: []*commonproto.HistoryEvent{
			{
				EventId: int64(100),
			},
		},
		NextPageToken:    []byte{},
		Size:             1,
		LastFirstEventID: nextEventID,
	}, nil).Once()

	wh := s.getWorkflowHandler(s.newConfig())

	scope := metrics.NoopScope(metrics.Frontend)
	history, token, err := wh.getHistory(scope, domainID, we, firstEventID, nextEventID, 0, []byte{}, nil, branchToken)
	s.NoError(err)
	s.NotNil(history)
	s.Equal([]byte{}, token)
}

func (s *workflowHandlerSuite) TestListArchivedVisibility_Failure_InvalidRequest() {
	wh := s.getWorkflowHandler(s.newConfig())

	resp, err := wh.ListArchivedWorkflowExecutions(context.Background(), &workflowservice.ListArchivedWorkflowExecutionsRequest{})
	s.Nil(resp)
	s.Error(err)
}

func (s *workflowHandlerSuite) TestListArchivedVisibility_Failure_ClusterNotConfiguredForArchival() {
	s.mockArchivalMetadata.On("GetVisibilityConfig").Return(archiver.NewDisabledArchvialConfig())

	wh := s.getWorkflowHandler(s.newConfig())

	resp, err := wh.ListArchivedWorkflowExecutions(context.Background(), listArchivedWorkflowExecutionsTestRequest())
	s.Nil(resp)
	s.Error(err)
}

func (s *workflowHandlerSuite) TestListArchivedVisibility_Failure_DomainCacheEntryError() {
	s.mockDomainCache.EXPECT().GetDomain(gomock.Any()).Return(nil, errors.New("error getting domain"))
	s.mockArchivalMetadata.On("GetVisibilityConfig").Return(archiver.NewArchivalConfig("enabled", dc.GetStringPropertyFn("enabled"), dc.GetBoolPropertyFn(true), "disabled", "random URI"))

	wh := s.getWorkflowHandler(s.newConfig())

	resp, err := wh.ListArchivedWorkflowExecutions(context.Background(), listArchivedWorkflowExecutionsTestRequest())
	s.Nil(resp)
	s.Error(err)
}

func (s *workflowHandlerSuite) TestListArchivedVisibility_Failure_DomainNotConfiguredForArchival() {
	s.mockDomainCache.EXPECT().GetDomain(gomock.Any()).Return(cache.NewLocalDomainCacheEntryForTest(
		nil,
		&persistence.DomainConfig{
			VisibilityArchivalStatus: enums.ArchivalStatusDisabled,
		},
		"",
		nil,
	), nil)
	s.mockArchivalMetadata.On("GetVisibilityConfig").Return(archiver.NewArchivalConfig("enabled", dc.GetStringPropertyFn("enabled"), dc.GetBoolPropertyFn(true), "disabled", "random URI"))

	wh := s.getWorkflowHandler(s.newConfig())

	resp, err := wh.ListArchivedWorkflowExecutions(context.Background(), listArchivedWorkflowExecutionsTestRequest())
	s.Nil(resp)
	s.Error(err)
}

func (s *workflowHandlerSuite) TestListArchivedVisibility_Failure_InvalidURI() {
	s.mockDomainCache.EXPECT().GetDomain(gomock.Any()).Return(cache.NewLocalDomainCacheEntryForTest(
		&persistence.DomainInfo{Name: "test-domain"},
		&persistence.DomainConfig{
			VisibilityArchivalStatus: enums.ArchivalStatusDisabled,
			VisibilityArchivalURI:    "uri without scheme",
		},
		"",
		nil,
	), nil)
	s.mockArchivalMetadata.On("GetVisibilityConfig").Return(archiver.NewArchivalConfig("enabled", dc.GetStringPropertyFn("enabled"), dc.GetBoolPropertyFn(true), "disabled", "random URI"))

	wh := s.getWorkflowHandler(s.newConfig())

	resp, err := wh.ListArchivedWorkflowExecutions(context.Background(), listArchivedWorkflowExecutionsTestRequest())
	s.Nil(resp)
	s.Error(err)
}

func (s *workflowHandlerSuite) TestListArchivedVisibility_Success() {
	s.mockDomainCache.EXPECT().GetDomain(gomock.Any()).Return(cache.NewLocalDomainCacheEntryForTest(
		&persistence.DomainInfo{Name: "test-domain"},
		&persistence.DomainConfig{
			VisibilityArchivalStatus: enums.ArchivalStatusEnabled,
			VisibilityArchivalURI:    testVisibilityArchivalURI,
		},
		"",
		nil,
	), nil).AnyTimes()
	s.mockArchivalMetadata.On("GetVisibilityConfig").Return(archiver.NewArchivalConfig("enabled", dc.GetStringPropertyFn("enabled"), dc.GetBoolPropertyFn(true), "disabled", "random URI"))
	s.mockVisibilityArchiver.On("Query", mock.Anything, mock.Anything, mock.Anything).Return(&archiver.QueryVisibilityResponse{}, nil)
	s.mockArchiverProvider.On("GetVisibilityArchiver", mock.Anything, mock.Anything).Return(s.mockVisibilityArchiver, nil)

	wh := s.getWorkflowHandler(s.newConfig())

	resp, err := wh.ListArchivedWorkflowExecutions(context.Background(), listArchivedWorkflowExecutionsTestRequest())
	s.NotNil(resp)
	s.NoError(err)
}

func (s *workflowHandlerSuite) TestGetSearchAttributes() {
	wh := s.getWorkflowHandler(s.newConfig())

	ctx := context.Background()
	resp, err := wh.GetSearchAttributes(ctx, &workflowservice.GetSearchAttributesRequest{})
	s.NoError(err)
	s.NotNil(resp)
}

func (s *workflowHandlerSuite) TestListWorkflowExecutions() {
	config := s.newConfig()
	wh := s.getWorkflowHandler(config)

	s.mockDomainCache.EXPECT().GetDomainID(gomock.Any()).Return(s.testDomainID, nil).AnyTimes()
	s.mockVisibilityMgr.On("ListWorkflowExecutions", mock.Anything).Return(&persistence.ListWorkflowExecutionsResponse{}, nil).Once()

	listRequest := &workflowservice.ListWorkflowExecutionsRequest{
		Domain:   s.testDomain,
		PageSize: int32(config.ESIndexMaxResultWindow()),
	}
	ctx := context.Background()

	query := "WorkflowID = 'wid'"
	listRequest.Query = query
	_, err := wh.ListWorkflowExecutions(ctx, listRequest)
	s.NoError(err)
	s.Equal(query, listRequest.GetQuery())

	query = "InvalidKey = 'a'"
	listRequest.Query = query
	_, err = wh.ListWorkflowExecutions(ctx, listRequest)
	s.NotNil(err)

	listRequest.PageSize = int32(config.ESIndexMaxResultWindow() + 1)
	_, err = wh.ListWorkflowExecutions(ctx, listRequest)
	s.NotNil(err)
}

func (s *workflowHandlerSuite) TestScantWorkflowExecutions() {
	config := s.newConfig()
	wh := s.getWorkflowHandler(config)

	s.mockDomainCache.EXPECT().GetDomainID(gomock.Any()).Return(s.testDomainID, nil).AnyTimes()
	s.mockVisibilityMgr.On("ScanWorkflowExecutions", mock.Anything).Return(&persistence.ListWorkflowExecutionsResponse{}, nil).Once()

	scanRequest := &workflowservice.ScanWorkflowExecutionsRequest{
		Domain:   s.testDomain,
		PageSize: int32(config.ESIndexMaxResultWindow()),
	}
	listRequest := &workflowservice.ListWorkflowExecutionsRequest{
		Domain:   s.testDomain,
		PageSize: int32(config.ESIndexMaxResultWindow()),
	}
	ctx := context.Background()

	query := "WorkflowID = 'wid'"
	listRequest.Query = query
	_, err := wh.ScanWorkflowExecutions(ctx, scanRequest)
	s.NoError(err)
	s.Equal(query, listRequest.GetQuery())

	query = "InvalidKey = 'a'"
	listRequest.Query = query
	_, err = wh.ScanWorkflowExecutions(ctx, scanRequest)
	s.NotNil(err)

	listRequest.PageSize = int32(config.ESIndexMaxResultWindow() + 1)
	_, err = wh.ListWorkflowExecutions(ctx, listRequest)
	s.NotNil(err)
}

func (s *workflowHandlerSuite) TestCountWorkflowExecutions() {
	wh := s.getWorkflowHandler(s.newConfig())

	s.mockDomainCache.EXPECT().GetDomainID(gomock.Any()).Return(s.testDomainID, nil).AnyTimes()
	s.mockVisibilityMgr.On("CountWorkflowExecutions", mock.Anything).Return(&persistence.CountWorkflowExecutionsResponse{}, nil).Once()

	countRequest := &workflowservice.CountWorkflowExecutionsRequest{
		Domain: s.testDomain,
	}
	ctx := context.Background()

	query := "WorkflowID = 'wid'"
	countRequest.Query = query
	_, err := wh.CountWorkflowExecutions(ctx, countRequest)
	s.NoError(err)
	s.Equal(query, countRequest.GetQuery())

	query = "InvalidKey = 'a'"
	countRequest.Query = query
	_, err = wh.CountWorkflowExecutions(ctx, countRequest)
	s.NotNil(err)
}

func (s *workflowHandlerSuite) TestConvertIndexedKeyToThrift() {
	wh := s.getWorkflowHandler(s.newConfig())
	m := map[string]interface{}{
		"key1":  float64(0),
		"key2":  float64(1),
		"key3":  float64(2),
		"key4":  float64(3),
		"key5":  float64(4),
		"key6":  float64(5),
		"key1i": 0,
		"key2i": 1,
		"key3i": 2,
		"key4i": 3,
		"key5i": 4,
		"key6i": 5,
		"key1t": enums.IndexedValueTypeString,
		"key2t": enums.IndexedValueTypeKeyword,
		"key3t": enums.IndexedValueTypeInt,
		"key4t": enums.IndexedValueTypeDouble,
		"key5t": enums.IndexedValueTypeBool,
		"key6t": enums.IndexedValueTypeDatetime,
	}
	result := wh.convertIndexedKeyToProto(m)
	s.Equal(enums.IndexedValueTypeString, result["key1"])
	s.Equal(enums.IndexedValueTypeKeyword, result["key2"])
	s.Equal(enums.IndexedValueTypeInt, result["key3"])
	s.Equal(enums.IndexedValueTypeDouble, result["key4"])
	s.Equal(enums.IndexedValueTypeBool, result["key5"])
	s.Equal(enums.IndexedValueTypeDatetime, result["key6"])
	s.Equal(enums.IndexedValueTypeString, result["key1i"])
	s.Equal(enums.IndexedValueTypeKeyword, result["key2i"])
	s.Equal(enums.IndexedValueTypeInt, result["key3i"])
	s.Equal(enums.IndexedValueTypeDouble, result["key4i"])
	s.Equal(enums.IndexedValueTypeBool, result["key5i"])
	s.Equal(enums.IndexedValueTypeDatetime, result["key6i"])
	s.Equal(enums.IndexedValueTypeString, result["key1t"])
	s.Equal(enums.IndexedValueTypeKeyword, result["key2t"])
	s.Equal(enums.IndexedValueTypeInt, result["key3t"])
	s.Equal(enums.IndexedValueTypeDouble, result["key4t"])
	s.Equal(enums.IndexedValueTypeBool, result["key5t"])
	s.Equal(enums.IndexedValueTypeDatetime, result["key6t"])
	s.Panics(func() {
		wh.convertIndexedKeyToProto(map[string]interface{}{
			"invalidType": "unknown",
		})
	})
}

func (s *workflowHandlerSuite) TestVerifyHistoryIsComplete() {
	wh := s.getWorkflowHandler(s.newConfig())

	events := make([]*commonproto.HistoryEvent, 50)
	for i := 0; i < len(events); i++ {
		events[i] = &commonproto.HistoryEvent{EventId: int64(i + 1)}
	}
	var eventsWithHoles []*commonproto.HistoryEvent
	eventsWithHoles = append(eventsWithHoles, events[9:12]...)
	eventsWithHoles = append(eventsWithHoles, events[20:31]...)

	testCases := []struct {
		events       []*commonproto.HistoryEvent
		firstEventID int64
		lastEventID  int64
		isFirstPage  bool
		isLastPage   bool
		pageSize     int
		isResultErr  bool
	}{
		{events[:1], 1, 1, true, true, 1000, false},
		{events[:5], 1, 5, true, true, 1000, false},
		{events[9:31], 10, 31, true, true, 1000, false},
		{events[9:29], 10, 50, true, false, 20, false},
		{events[9:30], 10, 50, true, false, 20, false},

		{events[9:29], 1, 50, false, false, 20, false},
		{events[9:29], 1, 29, false, true, 20, false},

		{eventsWithHoles, 1, 50, false, false, 22, true},
		{eventsWithHoles, 10, 50, true, false, 22, true},
		{eventsWithHoles, 1, 31, false, true, 22, true},
		{eventsWithHoles, 10, 31, true, true, 1000, true},

		{events[9:31], 9, 31, true, true, 1000, true},
		{events[9:31], 9, 50, true, false, 22, true},
		{events[9:31], 11, 31, true, true, 1000, true},
		{events[9:31], 11, 50, true, false, 22, true},

		{events[9:31], 10, 30, true, true, 1000, true},
		{events[9:31], 1, 30, false, true, 22, true},
		{events[9:31], 10, 32, true, true, 1000, true},
		{events[9:31], 1, 32, false, true, 22, true},
	}

	for i, tc := range testCases {
		err := wh.verifyHistoryIsComplete(tc.events, tc.firstEventID, tc.lastEventID, tc.isFirstPage, tc.isLastPage, tc.pageSize)
		if tc.isResultErr {
			s.Error(err, "testcase %v failed", i)
		} else {
			s.NoError(err, "testcase %v failed", i)
		}
	}
}

func (s *workflowHandlerSuite) newConfig() *Config {
	return NewConfig(dc.NewCollection(dc.NewNopClient(), s.mockResource.GetLogger()), numHistoryShards, false)
}

func updateRequest(
	historyArchivalURI string,
	historyArchivalStatus enums.ArchivalStatus,
	visibilityArchivalURI string,
	visibilityArchivalStatus enums.ArchivalStatus,
) *workflowservice.UpdateDomainRequest {
	return &workflowservice.UpdateDomainRequest{
		Name: "test-name",
		Configuration: &commonproto.DomainConfiguration{
			HistoryArchivalStatus:    historyArchivalStatus,
			HistoryArchivalURI:       historyArchivalURI,
			VisibilityArchivalStatus: visibilityArchivalStatus,
			VisibilityArchivalURI:    visibilityArchivalURI,
		},
	}
}

func persistenceGetDomainResponse(historyArchivalState, visibilityArchivalState *domain.ArchivalState) *persistence.GetDomainResponse {
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
			Retention:                1,
			EmitMetric:               true,
			HistoryArchivalStatus:    historyArchivalState.Status,
			HistoryArchivalURI:       historyArchivalState.URI,
			VisibilityArchivalStatus: visibilityArchivalState.Status,
			VisibilityArchivalURI:    visibilityArchivalState.URI,
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
	}
}

func registerDomainRequest(
	historyArchivalStatus enums.ArchivalStatus,
	historyArchivalURI string,
	visibilityArchivalStatus enums.ArchivalStatus,
	visibilityArchivalURI string,
) *workflowservice.RegisterDomainRequest {
	return &workflowservice.RegisterDomainRequest{
		Name:                                   "test-domain",
		Description:                            "test-description",
		OwnerEmail:                             "test-owner-email",
		WorkflowExecutionRetentionPeriodInDays: 10,
		EmitMetric:                             true,
		Clusters: []*commonproto.ClusterReplicationConfiguration{
			{
				ClusterName: cluster.TestCurrentClusterName,
			},
		},
		ActiveClusterName:        cluster.TestCurrentClusterName,
		Data:                     make(map[string]string),
		SecurityToken:            "token",
		HistoryArchivalStatus:    historyArchivalStatus,
		HistoryArchivalURI:       historyArchivalURI,
		VisibilityArchivalStatus: visibilityArchivalStatus,
		VisibilityArchivalURI:    visibilityArchivalURI,
		IsGlobalDomain:           false,
	}
}

func getHistoryRequest(nextPageToken []byte) *workflowservice.GetWorkflowExecutionHistoryRequest {
	return &workflowservice.GetWorkflowExecutionHistoryRequest{
		Execution: &commonproto.WorkflowExecution{
			WorkflowId: testWorkflowID,
			RunId:      testRunID,
		},
		NextPageToken: nextPageToken,
	}
}

func listArchivedWorkflowExecutionsTestRequest() *workflowservice.ListArchivedWorkflowExecutionsRequest {
	return &workflowservice.ListArchivedWorkflowExecutionsRequest{
		Domain:   "some random domain name",
		PageSize: 10,
		Query:    "some random query string",
	}
}
