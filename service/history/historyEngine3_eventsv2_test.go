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

package history

import (
	"context"
	"testing"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	h "github.com/uber/cadence/.gen/go/history"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/client"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/loggerimpl"
	"github.com/uber/cadence/common/messaging"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/mocks"
	p "github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/service"
	"github.com/uber/cadence/service/worker/archiver"
)

type (
	engine3Suite struct {
		suite.Suite
		// override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test,
		// not merely log an error
		*require.Assertions
		historyEngine       *historyEngineImpl
		mockMatchingClient  *mocks.MatchingClient
		mockHistoryClient   *mocks.HistoryClient
		mockMetadataMgr     *mocks.MetadataManager
		mockVisibilityMgr   *mocks.VisibilityManager
		mockExecutionMgr    *mocks.ExecutionManager
		mockHistoryMgr      *mocks.HistoryManager
		mockHistoryV2Mgr    *mocks.HistoryV2Manager
		mockShardManager    *mocks.ShardManager
		mockClusterMetadata *mocks.ClusterMetadata
		mockProducer        *mocks.KafkaProducer
		mockMessagingClient messaging.Client
		mockService         service.Service
		mockDomainCache     *cache.DomainCacheMock
		mockClientBean      *client.MockClientBean
		mockArchivalClient  *archiver.ClientMock
		mockEventsCache     *MockEventsCache

		shardClosedCh chan int
		config        *Config
		logger        log.Logger
	}
)

func TestEngine3Suite(t *testing.T) {
	s := new(engine3Suite)
	suite.Run(t, s)
}

func (s *engine3Suite) SetupSuite() {
	s.logger = loggerimpl.NewDevelopmentForTest(s.Suite)
	s.config = NewDynamicConfigForEventsV2Test()
}

func (s *engine3Suite) TearDownSuite() {
}

func (s *engine3Suite) SetupTest() {
	// Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	s.Assertions = require.New(s.T())

	shardID := 0
	s.mockMatchingClient = &mocks.MatchingClient{}
	s.mockHistoryClient = &mocks.HistoryClient{}
	s.mockMetadataMgr = &mocks.MetadataManager{}
	s.mockVisibilityMgr = &mocks.VisibilityManager{}
	s.mockExecutionMgr = &mocks.ExecutionManager{}
	s.mockHistoryMgr = &mocks.HistoryManager{}
	s.mockHistoryV2Mgr = &mocks.HistoryV2Manager{}
	s.mockShardManager = &mocks.ShardManager{}
	s.mockClusterMetadata = &mocks.ClusterMetadata{}
	s.mockProducer = &mocks.KafkaProducer{}
	s.shardClosedCh = make(chan int, 100)
	metricsClient := metrics.NewClient(tally.NoopScope, metrics.History)
	s.mockMessagingClient = mocks.NewMockMessagingClient(s.mockProducer, nil)
	s.mockClientBean = &client.MockClientBean{}
	s.mockService = service.NewTestService(s.mockClusterMetadata, s.mockMessagingClient, metricsClient, s.mockClientBean)
	s.mockClusterMetadata.On("GetCurrentClusterName").Return(cluster.TestCurrentClusterName)
	s.mockClusterMetadata.On("GetAllClusterFailoverVersions").Return(cluster.TestSingleDCAllClusterFailoverVersions)
	s.mockClusterMetadata.On("IsGlobalDomainEnabled").Return(false)
	s.mockDomainCache = &cache.DomainCacheMock{}
	s.mockArchivalClient = &archiver.ClientMock{}
	s.mockEventsCache = &MockEventsCache{}
	s.mockEventsCache.On("putEvent", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything).Return()

	mockShard := &shardContextImpl{
		service:                   s.mockService,
		shardInfo:                 &p.ShardInfo{ShardID: shardID, RangeID: 1, TransferAckLevel: 0},
		transferSequenceNumber:    1,
		executionManager:          s.mockExecutionMgr,
		historyMgr:                s.mockHistoryMgr,
		historyV2Mgr:              s.mockHistoryV2Mgr,
		domainCache:               s.mockDomainCache,
		eventsCache:               s.mockEventsCache,
		shardManager:              s.mockShardManager,
		maxTransferSequenceNumber: 100000,
		closeCh:                   s.shardClosedCh,
		config:                    s.config,
		logger:                    s.logger,
		metricsClient:             metrics.NewClient(tally.NoopScope, metrics.History),
	}

	historyCache := newHistoryCache(mockShard)
	h := &historyEngineImpl{
		currentClusterName: mockShard.GetService().GetClusterMetadata().GetCurrentClusterName(),
		shard:              mockShard,
		executionManager:   s.mockExecutionMgr,
		historyMgr:         s.mockHistoryMgr,
		historyV2Mgr:       s.mockHistoryV2Mgr,
		historyCache:       historyCache,
		logger:             s.logger,
		metricsClient:      metrics.NewClient(tally.NoopScope, metrics.History),
		tokenSerializer:    common.NewJSONTaskTokenSerializer(),
		config:             s.config,
		archivalClient:     s.mockArchivalClient,
	}
	h.txProcessor = newTransferQueueProcessor(mockShard, h, s.mockVisibilityMgr, s.mockMatchingClient, s.mockHistoryClient, s.logger)
	h.timerProcessor = newTimerQueueProcessor(mockShard, h, s.mockMatchingClient, s.logger)
	s.historyEngine = h
}

func (s *engine3Suite) TearDownTest() {
	s.mockMatchingClient.AssertExpectations(s.T())
	s.mockExecutionMgr.AssertExpectations(s.T())
	s.mockHistoryMgr.AssertExpectations(s.T())
	s.mockHistoryV2Mgr.AssertExpectations(s.T())
	s.mockShardManager.AssertExpectations(s.T())
	s.mockVisibilityMgr.AssertExpectations(s.T())
	s.mockProducer.AssertExpectations(s.T())
	s.mockClientBean.AssertExpectations(s.T())
	s.mockArchivalClient.AssertExpectations(s.T())
}

func (s *engine3Suite) TestRecordDecisionTaskStartedSuccessStickyEnabled() {
	testDomainEntry := cache.NewDomainCacheEntryForTest(&p.DomainInfo{ID: validDomainID}, &p.DomainConfig{Retention: 1})
	s.mockDomainCache.On("GetDomainByID", mock.Anything).Return(testDomainEntry, nil)
	s.mockDomainCache.On("GetDomain", mock.Anything).Return(testDomainEntry, nil)

	domainID := validDomainID
	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(validRunID),
	}
	tl := "testTaskList"
	stickyTl := "stickyTaskList"
	identity := "testIdentity"

	msBuilder := newMutableStateBuilderWithEventV2("test", s.historyEngine.shard, s.mockEventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	executionInfo := msBuilder.GetExecutionInfo()
	executionInfo.StickyTaskList = stickyTl

	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 200, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)

	ms := createMutableState(msBuilder)

	gwmsResponse := &p.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(nil, nil).Once()
	s.mockMetadataMgr.On("GetDomain", mock.Anything).Return(
		&p.GetDomainResponse{
			Info:   &p.DomainInfo{ID: domainID},
			Config: &p.DomainConfig{Retention: 1},
			ReplicationConfig: &p.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*p.ClusterReplicationConfig{
					&p.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				},
			},
			TableVersion: p.DomainTableVersionV1,
		},
		nil,
	)

	request := h.RecordDecisionTaskStartedRequest{
		DomainUUID:        common.StringPtr(domainID),
		WorkflowExecution: &we,
		ScheduleId:        common.Int64Ptr(2),
		TaskId:            common.Int64Ptr(100),
		RequestId:         common.StringPtr("reqId"),
		PollRequest: &workflow.PollForDecisionTaskRequest{
			TaskList: &workflow.TaskList{
				Name: common.StringPtr(stickyTl),
			},
			Identity: common.StringPtr(identity),
		},
	}

	expectedResponse := h.RecordDecisionTaskStartedResponse{}
	expectedResponse.WorkflowType = msBuilder.GetWorkflowType()
	executionInfo = msBuilder.GetExecutionInfo()
	if executionInfo.LastProcessedEvent != common.EmptyEventID {
		expectedResponse.PreviousStartedEventId = common.Int64Ptr(executionInfo.LastProcessedEvent)
	}
	expectedResponse.ScheduledEventId = common.Int64Ptr(di.ScheduleID)
	expectedResponse.StartedEventId = common.Int64Ptr(di.ScheduleID + 1)
	expectedResponse.StickyExecutionEnabled = common.BoolPtr(true)
	expectedResponse.NextEventId = common.Int64Ptr(msBuilder.GetNextEventID() + 1)
	expectedResponse.Attempt = common.Int64Ptr(di.Attempt)
	expectedResponse.WorkflowExecutionTaskList = common.TaskListPtr(workflow.TaskList{
		Name: &executionInfo.TaskList,
		Kind: common.TaskListKindPtr(workflow.TaskListKindNormal),
	})
	expectedResponse.EventStoreVersion = common.Int32Ptr(p.EventStoreVersionV2)
	expectedResponse.BranchToken = msBuilder.GetCurrentBranch()

	response, err := s.historyEngine.RecordDecisionTaskStarted(context.Background(), &request)
	s.Nil(err)
	s.NotNil(response)
	s.Equal(&expectedResponse, response)
}

func (s *engine3Suite) TestStartWorkflowExecution_BrandNew() {
	testDomainEntry := cache.NewDomainCacheEntryForTest(&p.DomainInfo{ID: validDomainID}, &p.DomainConfig{Retention: 1})
	s.mockDomainCache.On("GetDomainByID", mock.Anything).Return(testDomainEntry, nil)
	s.mockDomainCache.On("GetDomain", mock.Anything).Return(testDomainEntry, nil)

	domainID := validDomainID
	workflowID := "workflowID"
	workflowType := "workflowType"
	taskList := "testTaskList"
	identity := "testIdentity"

	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("CreateWorkflowExecution", mock.Anything).Return(&p.CreateWorkflowExecutionResponse{}, nil).Once()
	s.mockMetadataMgr.On("GetDomain", mock.Anything).Return(
		&p.GetDomainResponse{
			Info:   &p.DomainInfo{ID: domainID},
			Config: &p.DomainConfig{Retention: 1},
			ReplicationConfig: &p.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*p.ClusterReplicationConfig{
					&p.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				},
			},
			TableVersion: p.DomainTableVersionV1,
		},
		nil,
	)
	requestID := uuid.New()
	resp, err := s.historyEngine.StartWorkflowExecution(context.Background(), &h.StartWorkflowExecutionRequest{
		DomainUUID: common.StringPtr(domainID),
		StartRequest: &workflow.StartWorkflowExecutionRequest{
			Domain:                              common.StringPtr(domainID),
			WorkflowId:                          common.StringPtr(workflowID),
			WorkflowType:                        &workflow.WorkflowType{Name: common.StringPtr(workflowType)},
			TaskList:                            &workflow.TaskList{Name: common.StringPtr(taskList)},
			ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(1),
			TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(2),
			Identity:                            common.StringPtr(identity),
			RequestId:                           common.StringPtr(requestID),
		},
	})
	s.Nil(err)
	s.NotNil(resp.RunId)
}

func (s *engine3Suite) TestSignalWithStartWorkflowExecution_JustSignal() {
	testDomainEntry := cache.NewDomainCacheEntryForTest(&p.DomainInfo{ID: validDomainID}, &p.DomainConfig{Retention: 1})
	s.mockDomainCache.On("GetDomainByID", mock.Anything).Return(testDomainEntry, nil)
	s.mockDomainCache.On("GetDomain", mock.Anything).Return(testDomainEntry, nil)

	sRequest := &h.SignalWithStartWorkflowExecutionRequest{}
	_, err := s.historyEngine.SignalWithStartWorkflowExecution(context.Background(), sRequest)
	s.EqualError(err, "BadRequestError{Message: Missing domain UUID.}")

	domainID := validDomainID
	workflowID := "wId"
	runID := validRunID
	identity := "testIdentity"
	signalName := "my signal name"
	input := []byte("test input")
	sRequest = &h.SignalWithStartWorkflowExecutionRequest{
		DomainUUID: common.StringPtr(domainID),
		SignalWithStartRequest: &workflow.SignalWithStartWorkflowExecutionRequest{
			Domain:     common.StringPtr(domainID),
			WorkflowId: common.StringPtr(workflowID),
			Identity:   common.StringPtr(identity),
			SignalName: common.StringPtr(signalName),
			Input:      input,
		},
	}

	msBuilder := newMutableStateBuilderWithEventV2(s.mockClusterMetadata.GetCurrentClusterName(), s.historyEngine.shard, s.mockEventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), runID)
	ms := createMutableState(msBuilder)
	gwmsResponse := &p.GetWorkflowExecutionResponse{State: ms}
	gceResponse := &p.GetCurrentExecutionResponse{RunID: runID}

	s.mockExecutionMgr.On("GetCurrentExecution", mock.Anything).Return(gceResponse, nil).Once()
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(nil, nil).Once()
	s.mockMetadataMgr.On("GetDomain", mock.Anything).Return(
		&p.GetDomainResponse{
			Info:   &p.DomainInfo{ID: domainID},
			Config: &p.DomainConfig{Retention: 1},
			ReplicationConfig: &p.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*p.ClusterReplicationConfig{
					&p.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				},
			},
			TableVersion: p.DomainTableVersionV1,
		},
		nil,
	)

	resp, err := s.historyEngine.SignalWithStartWorkflowExecution(context.Background(), sRequest)
	s.Nil(err)
	s.Equal(runID, resp.GetRunId())
}

func (s *engine3Suite) TestSignalWithStartWorkflowExecution_WorkflowNotExist() {
	testDomainEntry := cache.NewDomainCacheEntryForTest(&p.DomainInfo{ID: validDomainID}, &p.DomainConfig{Retention: 1})
	s.mockDomainCache.On("GetDomainByID", mock.Anything).Return(testDomainEntry, nil)
	s.mockDomainCache.On("GetDomain", mock.Anything).Return(testDomainEntry, nil)

	sRequest := &h.SignalWithStartWorkflowExecutionRequest{}
	_, err := s.historyEngine.SignalWithStartWorkflowExecution(context.Background(), sRequest)
	s.EqualError(err, "BadRequestError{Message: Missing domain UUID.}")

	domainID := validDomainID
	workflowID := "wId"
	workflowType := "workflowType"
	taskList := "testTaskList"
	identity := "testIdentity"
	signalName := "my signal name"
	input := []byte("test input")
	requestID := uuid.New()
	sRequest = &h.SignalWithStartWorkflowExecutionRequest{
		DomainUUID: common.StringPtr(domainID),
		SignalWithStartRequest: &workflow.SignalWithStartWorkflowExecutionRequest{
			Domain:                              common.StringPtr(domainID),
			WorkflowId:                          common.StringPtr(workflowID),
			WorkflowType:                        &workflow.WorkflowType{Name: common.StringPtr(workflowType)},
			TaskList:                            &workflow.TaskList{Name: common.StringPtr(taskList)},
			ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(1),
			TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(2),
			Identity:                            common.StringPtr(identity),
			SignalName:                          common.StringPtr(signalName),
			Input:                               input,
			RequestId:                           common.StringPtr(requestID),
		},
	}

	notExistErr := &workflow.EntityNotExistsError{Message: "Workflow not exist"}

	s.mockExecutionMgr.On("GetCurrentExecution", mock.Anything).Return(nil, notExistErr).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("CreateWorkflowExecution", mock.Anything).Return(&p.CreateWorkflowExecutionResponse{}, nil).Once()
	s.mockMetadataMgr.On("GetDomain", mock.Anything).Return(
		&p.GetDomainResponse{
			Info:   &p.DomainInfo{ID: domainID},
			Config: &p.DomainConfig{Retention: 1},
			ReplicationConfig: &p.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*p.ClusterReplicationConfig{
					&p.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				},
			},
			TableVersion: p.DomainTableVersionV1,
		},
		nil,
	)

	resp, err := s.historyEngine.SignalWithStartWorkflowExecution(context.Background(), sRequest)
	s.Nil(err)
	s.NotNil(resp.GetRunId())
}
