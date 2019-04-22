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
	"encoding/json"
	"errors"
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
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/messaging"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/mocks"
	p "github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/service"
	"github.com/uber/cadence/service/worker/archiver"
)

type (
	engine2Suite struct {
		suite.Suite
		// override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test,
		// not merely log an error
		*require.Assertions
		historyEngine       *historyEngineImpl
		mockMatchingClient  *mocks.MatchingClient
		mockArchivalClient  *archiver.ClientMock
		mockHistoryClient   *mocks.HistoryClient
		mockMetadataMgr     *mocks.MetadataManager
		mockVisibilityMgr   *mocks.VisibilityManager
		mockExecutionMgr    *mocks.ExecutionManager
		mockHistoryMgr      *mocks.HistoryManager
		mockHistoryV2Mgr    *mocks.HistoryV2Manager
		mockShardManager    *mocks.ShardManager
		mockClusterMetadata *mocks.ClusterMetadata
		mockProducer        *mocks.KafkaProducer
		mockClientBean      *client.MockClientBean
		mockMessagingClient messaging.Client
		mockService         service.Service
		mockDomainCache     *cache.DomainCacheMock
		mockEventsCache     *MockEventsCache

		shardClosedCh chan int
		config        *Config
		logger        log.Logger
	}
)

func TestEngine2Suite(t *testing.T) {
	s := new(engine2Suite)
	suite.Run(t, s)
}

func (s *engine2Suite) SetupSuite() {
	s.logger = loggerimpl.NewDevelopmentForTest(s.Suite)
	s.config = NewDynamicConfigForTest()
}

func (s *engine2Suite) TearDownSuite() {
}

func (s *engine2Suite) SetupTest() {
	// Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	s.Assertions = require.New(s.T())

	shardID := 0
	s.mockMatchingClient = &mocks.MatchingClient{}
	s.mockArchivalClient = &archiver.ClientMock{}
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
	s.mockDomainCache.On("GetDomainByID", mock.Anything).Return(cache.NewDomainCacheEntryForTest(&p.DomainInfo{ID: validDomainID}, nil), nil)
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
		shardManager:              s.mockShardManager,
		maxTransferSequenceNumber: 100000,
		closeCh:                   s.shardClosedCh,
		config:                    s.config,
		logger:                    s.logger,
		metricsClient:             metrics.NewClient(tally.NoopScope, metrics.History),
		eventsCache:               s.mockEventsCache,
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

func (s *engine2Suite) TearDownTest() {
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

func (s *engine2Suite) TestRecordDecisionTaskStartedSuccessStickyEnabled() {
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

func (s *engine2Suite) TestRecordDecisionTaskStartedIfNoExecution() {
	domainID := validDomainID
	workflowExecution := &workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(validRunID),
	}

	identity := "testIdentity"
	tl := "testTaskList"

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(nil, &workflow.EntityNotExistsError{}).Once()
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

	response, err := s.historyEngine.RecordDecisionTaskStarted(context.Background(), &h.RecordDecisionTaskStartedRequest{
		DomainUUID:        common.StringPtr(domainID),
		WorkflowExecution: workflowExecution,
		ScheduleId:        common.Int64Ptr(2),
		TaskId:            common.Int64Ptr(100),
		RequestId:         common.StringPtr("reqId"),
		PollRequest: &workflow.PollForDecisionTaskRequest{
			TaskList: &workflow.TaskList{
				Name: common.StringPtr(tl),
			},
			Identity: common.StringPtr(identity),
		},
	})
	s.Nil(response)
	s.NotNil(err)
	s.IsType(&workflow.EntityNotExistsError{}, err)
}

func (s *engine2Suite) TestRecordDecisionTaskStartedIfGetExecutionFailed() {
	domainID := validDomainID
	workflowExecution := &workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(validRunID),
	}

	identity := "testIdentity"
	tl := "testTaskList"

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(nil, errors.New("FAILED")).Once()
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

	response, err := s.historyEngine.RecordDecisionTaskStarted(context.Background(), &h.RecordDecisionTaskStartedRequest{
		DomainUUID:        common.StringPtr(domainID),
		WorkflowExecution: workflowExecution,
		ScheduleId:        common.Int64Ptr(2),
		TaskId:            common.Int64Ptr(100),
		RequestId:         common.StringPtr("reqId"),
		PollRequest: &workflow.PollForDecisionTaskRequest{
			TaskList: &workflow.TaskList{
				Name: common.StringPtr(tl),
			},
			Identity: common.StringPtr(identity),
		},
	})
	s.Nil(response)
	s.NotNil(err)
	s.EqualError(err, "FAILED")
}

func (s *engine2Suite) TestRecordDecisionTaskStartedIfTaskAlreadyStarted() {
	domainID := validDomainID
	workflowExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(validRunID),
	}

	identity := "testIdentity"
	tl := "testTaskList"

	msBuilder := s.createExecutionStartedState(workflowExecution, tl, identity, true)
	ms := createMutableState(msBuilder)
	gwmsResponse := &p.GetWorkflowExecutionResponse{State: ms}
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
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

	response, err := s.historyEngine.RecordDecisionTaskStarted(context.Background(), &h.RecordDecisionTaskStartedRequest{
		DomainUUID:        common.StringPtr(domainID),
		WorkflowExecution: &workflowExecution,
		ScheduleId:        common.Int64Ptr(2),
		TaskId:            common.Int64Ptr(100),
		RequestId:         common.StringPtr("reqId"),
		PollRequest: &workflow.PollForDecisionTaskRequest{
			TaskList: &workflow.TaskList{
				Name: common.StringPtr(tl),
			},
			Identity: common.StringPtr(identity),
		},
	})
	s.Nil(response)
	s.NotNil(err)
	s.IsType(&h.EventAlreadyStartedError{}, err)
	s.logger.Error("RecordDecisionTaskStarted failed with", tag.Error(err))
}

func (s *engine2Suite) TestRecordDecisionTaskStartedIfTaskAlreadyCompleted() {
	domainID := validDomainID
	workflowExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(validRunID),
	}

	identity := "testIdentity"
	tl := "testTaskList"

	msBuilder := s.createExecutionStartedState(workflowExecution, tl, identity, true)
	addDecisionTaskCompletedEvent(msBuilder, int64(2), int64(3), nil, identity)

	ms := createMutableState(msBuilder)
	gwmsResponse := &p.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
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

	response, err := s.historyEngine.RecordDecisionTaskStarted(context.Background(), &h.RecordDecisionTaskStartedRequest{
		DomainUUID:        common.StringPtr(domainID),
		WorkflowExecution: &workflowExecution,
		ScheduleId:        common.Int64Ptr(2),
		TaskId:            common.Int64Ptr(100),
		RequestId:         common.StringPtr("reqId"),
		PollRequest: &workflow.PollForDecisionTaskRequest{
			TaskList: &workflow.TaskList{
				Name: common.StringPtr(tl),
			},
			Identity: common.StringPtr(identity),
		},
	})
	s.Nil(response)
	s.NotNil(err)
	s.IsType(&workflow.EntityNotExistsError{}, err)
	s.logger.Error("RecordDecisionTaskStarted failed with", tag.Error(err))
}

func (s *engine2Suite) TestRecordDecisionTaskStartedConflictOnUpdate() {
	domainID := validDomainID
	workflowExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(validRunID),
	}

	identity := "testIdentity"
	tl := "testTaskList"

	msBuilder := s.createExecutionStartedState(workflowExecution, tl, identity, false)

	ms := createMutableState(msBuilder)
	gwmsResponse := &p.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(nil, &p.ConditionFailedError{}).Once()

	ms2 := createMutableState(msBuilder)
	gwmsResponse2 := &p.GetWorkflowExecutionResponse{State: ms2}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse2, nil).Once()
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

	response, err := s.historyEngine.RecordDecisionTaskStarted(context.Background(), &h.RecordDecisionTaskStartedRequest{
		DomainUUID:        common.StringPtr(domainID),
		WorkflowExecution: &workflowExecution,
		ScheduleId:        common.Int64Ptr(2),
		TaskId:            common.Int64Ptr(100),
		RequestId:         common.StringPtr("reqId"),
		PollRequest: &workflow.PollForDecisionTaskRequest{
			TaskList: &workflow.TaskList{
				Name: common.StringPtr(tl),
			},
			Identity: common.StringPtr(identity),
		},
	})
	s.Nil(err)
	s.NotNil(response)
	s.Equal("wType", *response.WorkflowType.Name)
	s.True(response.PreviousStartedEventId == nil)
	s.Equal(int64(3), *response.StartedEventId)
}

func (s *engine2Suite) TestRecordDecisionTaskRetrySameRequest() {
	domainID := validDomainID
	workflowExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(validRunID),
	}

	tl := "testTaskList"
	identity := "testIdentity"
	requestID := "testRecordDecisionTaskRetrySameRequestID"

	msBuilder := s.createExecutionStartedState(workflowExecution, tl, identity, false)
	ms := createMutableState(msBuilder)
	gwmsResponse := &p.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(nil, &p.ConditionFailedError{}).Once()

	startedEventID := addDecisionTaskStartedEventWithRequestID(msBuilder, int64(2), requestID, tl, identity)
	ms2 := createMutableState(msBuilder)
	gwmsResponse2 := &p.GetWorkflowExecutionResponse{State: ms2}
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse2, nil).Once()
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

	response, err := s.historyEngine.RecordDecisionTaskStarted(context.Background(), &h.RecordDecisionTaskStartedRequest{
		DomainUUID:        common.StringPtr(domainID),
		WorkflowExecution: &workflowExecution,
		ScheduleId:        common.Int64Ptr(2),
		TaskId:            common.Int64Ptr(100),
		RequestId:         common.StringPtr(requestID),
		PollRequest: &workflow.PollForDecisionTaskRequest{
			TaskList: &workflow.TaskList{
				Name: common.StringPtr(tl),
			},
			Identity: common.StringPtr(identity),
		},
	})

	s.Nil(err)
	s.NotNil(response)
	s.Equal("wType", *response.WorkflowType.Name)
	s.True(response.PreviousStartedEventId == nil)
	s.Equal(startedEventID.EventId, response.StartedEventId)
}

func (s *engine2Suite) TestRecordDecisionTaskRetryDifferentRequest() {
	domainID := validDomainID
	workflowExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(validRunID),
	}

	tl := "testTaskList"
	identity := "testIdentity"
	requestID := "testRecordDecisionTaskRetrySameRequestID"

	msBuilder := s.createExecutionStartedState(workflowExecution, tl, identity, false)
	ms := createMutableState(msBuilder)
	gwmsResponse := &p.GetWorkflowExecutionResponse{State: ms}
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(nil, &p.ConditionFailedError{}).Once()

	// Add event.
	addDecisionTaskStartedEventWithRequestID(msBuilder, int64(2), "some_other_req", tl, identity)
	ms2 := createMutableState(msBuilder)
	gwmsResponse2 := &p.GetWorkflowExecutionResponse{State: ms2}
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse2, nil).Once()
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

	response, err := s.historyEngine.RecordDecisionTaskStarted(context.Background(), &h.RecordDecisionTaskStartedRequest{
		DomainUUID:        common.StringPtr(domainID),
		WorkflowExecution: &workflowExecution,
		ScheduleId:        common.Int64Ptr(2),
		TaskId:            common.Int64Ptr(100),
		RequestId:         common.StringPtr(requestID),
		PollRequest: &workflow.PollForDecisionTaskRequest{
			TaskList: &workflow.TaskList{
				Name: common.StringPtr(tl),
			},
			Identity: common.StringPtr(identity),
		},
	})

	s.Nil(response)
	s.NotNil(err)
	s.IsType(&h.EventAlreadyStartedError{}, err)
	s.logger.Info("Failed with error", tag.Error(err))
}

func (s *engine2Suite) TestRecordDecisionTaskStartedMaxAttemptsExceeded() {
	domainID := validDomainID
	workflowExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(validRunID),
	}

	tl := "testTaskList"
	identity := "testIdentity"

	msBuilder := s.createExecutionStartedState(workflowExecution, tl, identity, false)
	for i := 0; i < conditionalRetryCount; i++ {
		ms := createMutableState(msBuilder)
		gwmsResponse := &p.GetWorkflowExecutionResponse{State: ms}

		s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	}

	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Times(
		conditionalRetryCount)
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(nil,
		&p.ConditionFailedError{}).Times(conditionalRetryCount)
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

	response, err := s.historyEngine.RecordDecisionTaskStarted(context.Background(), &h.RecordDecisionTaskStartedRequest{
		DomainUUID:        common.StringPtr(domainID),
		WorkflowExecution: &workflowExecution,
		ScheduleId:        common.Int64Ptr(2),
		TaskId:            common.Int64Ptr(100),
		RequestId:         common.StringPtr("reqId"),
		PollRequest: &workflow.PollForDecisionTaskRequest{
			TaskList: &workflow.TaskList{
				Name: common.StringPtr(tl),
			},
			Identity: common.StringPtr(identity),
		},
	})

	s.NotNil(err)
	s.Nil(response)
	s.Equal(ErrMaxAttemptsExceeded, err)
}

func (s *engine2Suite) TestRecordDecisionTaskSuccess() {
	domainID := validDomainID
	workflowExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(validRunID),
	}

	tl := "testTaskList"
	identity := "testIdentity"

	msBuilder := s.createExecutionStartedState(workflowExecution, tl, identity, false)
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

	response, err := s.historyEngine.RecordDecisionTaskStarted(context.Background(), &h.RecordDecisionTaskStartedRequest{
		DomainUUID:        common.StringPtr(domainID),
		WorkflowExecution: &workflowExecution,
		ScheduleId:        common.Int64Ptr(2),
		TaskId:            common.Int64Ptr(100),
		RequestId:         common.StringPtr("reqId"),
		PollRequest: &workflow.PollForDecisionTaskRequest{
			TaskList: &workflow.TaskList{
				Name: common.StringPtr(tl),
			},
			Identity: common.StringPtr(identity),
		},
	})

	s.Nil(err)
	s.NotNil(response)
	s.Equal("wType", *response.WorkflowType.Name)
	s.True(response.PreviousStartedEventId == nil)
	s.Equal(int64(3), *response.StartedEventId)
}

func (s *engine2Suite) TestRecordActivityTaskStartedIfNoExecution() {
	domainID := validDomainID
	workflowExecution := &workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(validRunID),
	}

	identity := "testIdentity"
	tl := "testTaskList"

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(nil, &workflow.EntityNotExistsError{}).Once()
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

	response, err := s.historyEngine.RecordActivityTaskStarted(context.Background(), &h.RecordActivityTaskStartedRequest{
		DomainUUID:        common.StringPtr(domainID),
		WorkflowExecution: workflowExecution,
		ScheduleId:        common.Int64Ptr(5),
		TaskId:            common.Int64Ptr(100),
		RequestId:         common.StringPtr("reqId"),
		PollRequest: &workflow.PollForActivityTaskRequest{
			TaskList: &workflow.TaskList{
				Name: common.StringPtr(tl),
			},
			Identity: common.StringPtr(identity),
		},
	})
	if err != nil {
		s.logger.Error("Unexpected Error", tag.Error(err))
	}
	s.Nil(response)
	s.NotNil(err)
	s.IsType(&workflow.EntityNotExistsError{}, err)
}

func (s *engine2Suite) TestRecordActivityTaskStartedSuccess() {
	domainID := validDomainID
	workflowExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(validRunID),
	}

	identity := "testIdentity"
	tl := "testTaskList"

	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := []byte("input1")

	msBuilder := s.createExecutionStartedState(workflowExecution, tl, identity, true)
	decisionCompletedEvent := addDecisionTaskCompletedEvent(msBuilder, int64(2), int64(3), nil, identity)
	scheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, *decisionCompletedEvent.EventId, activityID,
		activityType, tl, activityInput, 100, 10, 5)

	ms1 := createMutableState(msBuilder)
	gwmsResponse1 := &p.GetWorkflowExecutionResponse{State: ms1}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse1, nil).Once()
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

	s.mockEventsCache.On("getEvent", domainID, workflowExecution.GetWorkflowId(), workflowExecution.GetRunId(),
		decisionCompletedEvent.GetEventId(), scheduledEvent.GetEventId(), mock.Anything, mock.Anything).Return(scheduledEvent, nil)
	response, err := s.historyEngine.RecordActivityTaskStarted(context.Background(), &h.RecordActivityTaskStartedRequest{
		DomainUUID:        common.StringPtr(domainID),
		WorkflowExecution: &workflowExecution,
		ScheduleId:        common.Int64Ptr(5),
		TaskId:            common.Int64Ptr(100),
		RequestId:         common.StringPtr("reqId"),
		PollRequest: &workflow.PollForActivityTaskRequest{
			TaskList: &workflow.TaskList{
				Name: common.StringPtr(tl),
			},
			Identity: common.StringPtr(identity),
		},
	})
	s.Nil(err)
	s.NotNil(response)
	s.Equal(scheduledEvent, response.ScheduledEvent)
}

func (s *engine2Suite) TestRequestCancelWorkflowExecutionSuccess() {
	domainID := validDomainID
	workflowExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(validRunID),
	}

	identity := "testIdentity"
	tl := "testTaskList"

	msBuilder := s.createExecutionStartedState(workflowExecution, tl, identity, false)
	ms1 := createMutableState(msBuilder)
	gwmsResponse1 := &p.GetWorkflowExecutionResponse{State: ms1}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse1, nil).Once()
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

	err := s.historyEngine.RequestCancelWorkflowExecution(context.Background(), &h.RequestCancelWorkflowExecutionRequest{
		DomainUUID: common.StringPtr(domainID),
		CancelRequest: &workflow.RequestCancelWorkflowExecutionRequest{
			WorkflowExecution: &workflow.WorkflowExecution{
				WorkflowId: workflowExecution.WorkflowId,
				RunId:      workflowExecution.RunId,
			},
			Identity: common.StringPtr("identity"),
		},
	})
	s.Nil(err)

	executionBuilder := s.getBuilder(domainID, workflowExecution)
	s.Equal(int64(4), executionBuilder.GetNextEventID())
}

func (s *engine2Suite) TestRequestCancelWorkflowExecutionFail() {
	domainID := validDomainID
	workflowExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(validRunID),
	}

	identity := "testIdentity"
	tl := "testTaskList"

	msBuilder := s.createExecutionStartedState(workflowExecution, tl, identity, false)
	msBuilder.GetExecutionInfo().State = p.WorkflowStateCompleted
	ms1 := createMutableState(msBuilder)
	gwmsResponse1 := &p.GetWorkflowExecutionResponse{State: ms1}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse1, nil).Once()
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

	err := s.historyEngine.RequestCancelWorkflowExecution(context.Background(), &h.RequestCancelWorkflowExecutionRequest{
		DomainUUID: common.StringPtr(domainID),
		CancelRequest: &workflow.RequestCancelWorkflowExecutionRequest{
			WorkflowExecution: &workflow.WorkflowExecution{
				WorkflowId: workflowExecution.WorkflowId,
				RunId:      workflowExecution.RunId,
			},
			Identity: common.StringPtr("identity"),
		},
	})
	s.NotNil(err)
	s.IsType(&workflow.EntityNotExistsError{}, err)
}

func (s *engine2Suite) createExecutionStartedState(we workflow.WorkflowExecution, tl, identity string,
	startDecision bool) mutableState {
	msBuilder := newMutableStateBuilderWithEventV2(s.mockClusterMetadata.GetCurrentClusterName(), s.historyEngine.shard, s.mockEventsCache,
		s.logger, we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 200, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	if startDecision {
		addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)
	}
	_ = msBuilder.SetHistoryTree(we.GetRunId())

	return msBuilder
}

func (s *engine2Suite) printHistory(builder mutableState) string {
	return builder.GetHistoryBuilder().GetHistory().String()
}

func (s *engine2Suite) TestRespondDecisionTaskCompletedRecordMarkerDecision() {
	domainID := validDomainID
	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(validRunID),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: "wId",
		RunID:      we.GetRunId(),
		ScheduleID: 2,
	})
	identity := "testIdentity"
	markerDetails := []byte("marker details")
	markerName := "marker name"

	msBuilder := newMutableStateBuilderWithEventV2(s.mockClusterMetadata.GetCurrentClusterName(), s.historyEngine.shard, s.mockEventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 200, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)

	decisions := []*workflow.Decision{{
		DecisionType: common.DecisionTypePtr(workflow.DecisionTypeRecordMarker),
		RecordMarkerDecisionAttributes: &workflow.RecordMarkerDecisionAttributes{
			MarkerName: common.StringPtr(markerName),
			Details:    markerDetails,
		},
	}}

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

	_, err := s.historyEngine.RespondDecisionTaskCompleted(context.Background(), &h.RespondDecisionTaskCompletedRequest{
		DomainUUID: common.StringPtr(domainID),
		CompleteRequest: &workflow.RespondDecisionTaskCompletedRequest{
			TaskToken:        taskToken,
			Decisions:        decisions,
			ExecutionContext: nil,
			Identity:         &identity,
		},
	})
	s.Nil(err)
	executionBuilder := s.getBuilder(domainID, we)
	s.Equal(int64(6), executionBuilder.GetExecutionInfo().NextEventID)
	s.Equal(int64(3), executionBuilder.GetExecutionInfo().LastProcessedEvent)
	s.Equal(p.WorkflowStateRunning, executionBuilder.GetExecutionInfo().State)
	s.False(executionBuilder.HasPendingDecisionTask())
}

func (s *engine2Suite) TestStartWorkflowExecution_BrandNew() {
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

func (s *engine2Suite) TestStartWorkflowExecution_StillRunning_Dedup() {
	domainID := validDomainID
	workflowID := "workflowID"
	runID := "runID"
	workflowType := "workflowType"
	taskList := "testTaskList"
	identity := "testIdentity"
	requestID := "requestID"
	lastWriteVersion := common.EmptyVersion

	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("CreateWorkflowExecution", mock.Anything).Return(nil, &p.WorkflowExecutionAlreadyStartedError{
		Msg:              "random message",
		StartRequestID:   requestID,
		RunID:            runID,
		State:            p.WorkflowStateRunning,
		CloseStatus:      p.WorkflowCloseStatusNone,
		LastWriteVersion: lastWriteVersion,
	}).Once()
	s.mockHistoryV2Mgr.On("DeleteHistoryBranch", mock.Anything).Return(nil).Once()
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
	s.Equal(runID, resp.GetRunId())
}

func (s *engine2Suite) TestStartWorkflowExecution_StillRunning_NonDeDup() {
	domainID := validDomainID
	workflowID := "workflowID"
	runID := "runID"
	workflowType := "workflowType"
	taskList := "testTaskList"
	identity := "testIdentity"
	lastWriteVersion := common.EmptyVersion

	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("CreateWorkflowExecution", mock.Anything).Return(nil, &p.WorkflowExecutionAlreadyStartedError{
		Msg:              "random message",
		StartRequestID:   "oldRequestID",
		RunID:            runID,
		State:            p.WorkflowStateRunning,
		CloseStatus:      p.WorkflowCloseStatusNone,
		LastWriteVersion: lastWriteVersion,
	}).Once()
	s.mockHistoryV2Mgr.On("DeleteHistoryBranch", mock.Anything).Return(nil).Once()
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
			RequestId:                           common.StringPtr("newRequestID"),
		},
	})
	if _, ok := err.(*workflow.WorkflowExecutionAlreadyStartedError); !ok {
		s.Fail("return err is not *shared.WorkflowExecutionAlreadyStartedError")
	}
	s.Nil(resp)
}

func (s *engine2Suite) TestStartWorkflowExecution_NotRunning_PrevSuccess() {
	domainID := validDomainID
	workflowID := "workflowID"
	runID := "runID"
	workflowType := "workflowType"
	taskList := "testTaskList"
	identity := "testIdentity"
	lastWriteVersion := common.EmptyVersion

	options := []workflow.WorkflowIdReusePolicy{
		workflow.WorkflowIdReusePolicyAllowDuplicateFailedOnly,
		workflow.WorkflowIdReusePolicyAllowDuplicate,
		workflow.WorkflowIdReusePolicyRejectDuplicate,
	}

	expecedErrs := []bool{true, false, true}

	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Times(len(expecedErrs))
	s.mockExecutionMgr.On(
		"CreateWorkflowExecution",
		mock.MatchedBy(func(request *p.CreateWorkflowExecutionRequest) bool {
			return request.CreateWorkflowMode == p.CreateWorkflowModeBrandNew
		}),
	).Return(nil, &p.WorkflowExecutionAlreadyStartedError{
		Msg:              "random message",
		StartRequestID:   "oldRequestID",
		RunID:            runID,
		State:            p.WorkflowStateCompleted,
		CloseStatus:      p.WorkflowCloseStatusCompleted,
		LastWriteVersion: lastWriteVersion,
	}).Times(len(expecedErrs))
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

	for index, option := range options {
		if !expecedErrs[index] {
			s.mockExecutionMgr.On(
				"CreateWorkflowExecution",
				mock.MatchedBy(func(request *p.CreateWorkflowExecutionRequest) bool {
					return request.CreateWorkflowMode == p.CreateWorkflowModeWorkflowIDReuse &&
						request.PreviousRunID == runID &&
						request.PreviousLastWriteVersion == lastWriteVersion
				}),
			).Return(&p.CreateWorkflowExecutionResponse{}, nil).Once()
		} else {
			s.mockHistoryV2Mgr.On("DeleteHistoryBranch", mock.Anything).Return(nil).Once()
		}

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
				RequestId:                           common.StringPtr("newRequestID"),
				WorkflowIdReusePolicy:               &option,
			},
		})

		if expecedErrs[index] {
			if _, ok := err.(*workflow.WorkflowExecutionAlreadyStartedError); !ok {
				s.Fail("return err is not *shared.WorkflowExecutionAlreadyStartedError")
			}
			s.Nil(resp)
		} else {
			s.Nil(err)
			s.NotNil(resp)
		}
	}
}

func (s *engine2Suite) TestStartWorkflowExecution_NotRunning_PrevFail() {
	domainID := validDomainID
	workflowID := "workflowID"
	workflowType := "workflowType"
	taskList := "testTaskList"
	identity := "testIdentity"
	lastWriteVersion := common.EmptyVersion

	options := []workflow.WorkflowIdReusePolicy{
		workflow.WorkflowIdReusePolicyAllowDuplicateFailedOnly,
		workflow.WorkflowIdReusePolicyAllowDuplicate,
		workflow.WorkflowIdReusePolicyRejectDuplicate,
	}

	expecedErrs := []bool{false, false, true}

	closeStates := []int{
		p.WorkflowCloseStatusFailed,
		p.WorkflowCloseStatusCanceled,
		p.WorkflowCloseStatusTerminated,
		p.WorkflowCloseStatusTimedOut,
	}
	runIDs := []string{"1", "2", "3", "4"}

	for i, closeState := range closeStates {

		s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Times(len(expecedErrs))
		s.mockExecutionMgr.On(
			"CreateWorkflowExecution",
			mock.MatchedBy(func(request *p.CreateWorkflowExecutionRequest) bool {
				return request.CreateWorkflowMode == p.CreateWorkflowModeBrandNew
			}),
		).Return(nil, &p.WorkflowExecutionAlreadyStartedError{
			Msg:              "random message",
			StartRequestID:   "oldRequestID",
			RunID:            runIDs[i],
			State:            p.WorkflowStateCompleted,
			CloseStatus:      closeState,
			LastWriteVersion: lastWriteVersion,
		}).Times(len(expecedErrs))
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

		for j, option := range options {

			if !expecedErrs[j] {
				s.mockExecutionMgr.On(
					"CreateWorkflowExecution",
					mock.MatchedBy(func(request *p.CreateWorkflowExecutionRequest) bool {
						return request.CreateWorkflowMode == p.CreateWorkflowModeWorkflowIDReuse &&
							request.PreviousRunID == runIDs[i] &&
							request.PreviousLastWriteVersion == lastWriteVersion
					}),
				).Return(&p.CreateWorkflowExecutionResponse{}, nil).Once()
			} else {
				s.mockHistoryV2Mgr.On("DeleteHistoryBranch", mock.Anything).Return(nil).Once()
			}

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
					RequestId:                           common.StringPtr("newRequestID"),
					WorkflowIdReusePolicy:               &option,
				},
			})

			if expecedErrs[j] {
				if _, ok := err.(*workflow.WorkflowExecutionAlreadyStartedError); !ok {
					s.Fail("return err is not *shared.WorkflowExecutionAlreadyStartedError")
				}
				s.Nil(resp)
			} else {
				s.Nil(err)
				s.NotNil(resp)
			}
		}
	}
}

func (s *engine2Suite) TestSignalWithStartWorkflowExecution_JustSignal() {
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

func (s *engine2Suite) TestSignalWithStartWorkflowExecution_WorkflowNotExist() {
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

func (s *engine2Suite) TestSignalWithStartWorkflowExecution_WorkflowNotRunning() {
	sRequest := &h.SignalWithStartWorkflowExecutionRequest{}
	_, err := s.historyEngine.SignalWithStartWorkflowExecution(context.Background(), sRequest)
	s.EqualError(err, "BadRequestError{Message: Missing domain UUID.}")

	domainID := validDomainID
	workflowID := "wId"
	runID := validRunID
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

	msBuilder := newMutableStateBuilderWithEventV2(s.mockClusterMetadata.GetCurrentClusterName(), s.historyEngine.shard, s.mockEventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), runID)
	ms := createMutableState(msBuilder)
	ms.ExecutionInfo.State = p.WorkflowStateCompleted
	gwmsResponse := &p.GetWorkflowExecutionResponse{State: ms}
	gceResponse := &p.GetCurrentExecutionResponse{RunID: runID}

	s.mockExecutionMgr.On("GetCurrentExecution", mock.Anything).Return(gceResponse, nil).Once()
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
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
	s.NotEqual(runID, resp.GetRunId())
}

func (s *engine2Suite) TestSignalWithStartWorkflowExecution_Start_DuplicateRequests() {
	domainID := validDomainID
	workflowID := "wId"
	runID := validRunID
	workflowType := "workflowType"
	taskList := "testTaskList"
	identity := "testIdentity"
	signalName := "my signal name"
	input := []byte("test input")
	requestID := "testRequestID"
	sRequest := &h.SignalWithStartWorkflowExecutionRequest{
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

	msBuilder := newMutableStateBuilderWithEventV2(s.mockClusterMetadata.GetCurrentClusterName(), s.historyEngine.shard, s.mockEventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), runID)
	ms := createMutableState(msBuilder)
	ms.ExecutionInfo.State = p.WorkflowStateCompleted
	gwmsResponse := &p.GetWorkflowExecutionResponse{State: ms}
	gceResponse := &p.GetCurrentExecutionResponse{RunID: runID}
	workflowAlreadyStartedErr := &p.WorkflowExecutionAlreadyStartedError{
		Msg:              "random message",
		StartRequestID:   requestID, // use same requestID
		RunID:            runID,
		State:            p.WorkflowStateRunning,
		CloseStatus:      p.WorkflowCloseStatusNone,
		LastWriteVersion: common.EmptyVersion,
	}

	s.mockExecutionMgr.On("GetCurrentExecution", mock.Anything).Return(gceResponse, nil).Once()
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("CreateWorkflowExecution", mock.Anything).Return(nil, workflowAlreadyStartedErr).Once()
	s.mockHistoryV2Mgr.On("DeleteHistoryBranch", mock.Anything).Return(nil).Once()
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
	s.Equal(runID, resp.GetRunId())
}

func (s *engine2Suite) TestSignalWithStartWorkflowExecution_Start_WorkflowAlreadyStarted() {
	domainID := validDomainID
	workflowID := "wId"
	runID := validRunID
	workflowType := "workflowType"
	taskList := "testTaskList"
	identity := "testIdentity"
	signalName := "my signal name"
	input := []byte("test input")
	requestID := "testRequestID"
	sRequest := &h.SignalWithStartWorkflowExecutionRequest{
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

	msBuilder := newMutableStateBuilderWithEventV2(s.mockClusterMetadata.GetCurrentClusterName(), s.historyEngine.shard, s.mockEventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), runID)
	ms := createMutableState(msBuilder)
	ms.ExecutionInfo.State = p.WorkflowStateCompleted
	gwmsResponse := &p.GetWorkflowExecutionResponse{State: ms}
	gceResponse := &p.GetCurrentExecutionResponse{RunID: runID}
	workflowAlreadyStartedErr := &p.WorkflowExecutionAlreadyStartedError{
		Msg:              "random message",
		StartRequestID:   "new request ID",
		RunID:            runID,
		State:            p.WorkflowStateRunning,
		CloseStatus:      p.WorkflowCloseStatusNone,
		LastWriteVersion: common.EmptyVersion,
	}

	s.mockExecutionMgr.On("GetCurrentExecution", mock.Anything).Return(gceResponse, nil).Once()
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("CreateWorkflowExecution", mock.Anything).Return(nil, workflowAlreadyStartedErr).Once()
	s.mockHistoryV2Mgr.On("DeleteHistoryBranch", mock.Anything).Return(nil).Once()
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
	s.Nil(resp)
	s.NotNil(err)
}

func (s *engine2Suite) getBuilder(domainID string, we workflow.WorkflowExecution) mutableState {
	context, release, err := s.historyEngine.historyCache.getOrCreateWorkflowExecution(domainID, we)
	if err != nil {
		return nil
	}
	defer release(nil)

	return context.(*workflowExecutionContextImpl).msBuilder
}
