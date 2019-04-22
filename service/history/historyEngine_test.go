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
	"time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	"github.com/uber/cadence/.gen/go/history"
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
	"github.com/uber/cadence/common/persistence"
	p "github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/service"
	"github.com/uber/cadence/service/worker/archiver"
)

type (
	engineSuite struct {
		suite.Suite
		// override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test,
		// not merely log an error
		*require.Assertions
		mockHistoryEngine   *historyEngineImpl
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
		mockEventsCache     *MockEventsCache
		mockMessagingClient messaging.Client
		mockService         service.Service
		mockMetricClient    metrics.Client
		shardClosedCh       chan int
		eventsCache         eventsCache
		config              *Config
		logger              log.Logger
	}
)

var validDomainID = "deadbeef-0123-4567-890a-bcdef0123456"
var validRunID = "0d00698f-08e1-4d36-a3e2-3bf109f5d2d6"

func TestEngineSuite(t *testing.T) {
	s := new(engineSuite)
	suite.Run(t, s)
}

func (s *engineSuite) SetupSuite() {

	s.logger = loggerimpl.NewDevelopmentForTest(s.Suite)
	s.config = NewDynamicConfigForTest()
}

func (s *engineSuite) TearDownSuite() {

}

func (s *engineSuite) SetupTest() {
	// Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	s.Assertions = require.New(s.T())

	shardID := 10
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
	s.mockMetricClient = metrics.NewClient(tally.NoopScope, metrics.History)
	s.mockMessagingClient = mocks.NewMockMessagingClient(s.mockProducer, nil)
	s.mockClientBean = &client.MockClientBean{}
	s.mockService = service.NewTestService(s.mockClusterMetadata, s.mockMessagingClient, s.mockMetricClient, s.mockClientBean)
	s.mockClusterMetadata.On("GetCurrentClusterName").Return(cluster.TestCurrentClusterName)
	s.mockClusterMetadata.On("GetAllClusterFailoverVersions").Return(cluster.TestSingleDCAllClusterFailoverVersions)
	s.mockEventsCache = &MockEventsCache{}

	historyEventNotifier := newHistoryEventNotifier(
		s.mockMetricClient,
		func(workflowID string) int {
			return len(workflowID)
		},
	)
	domainCache := cache.NewDomainCache(s.mockMetadataMgr, s.mockClusterMetadata, s.mockMetricClient, s.logger)
	mockShard := &shardContextImpl{
		service:                   s.mockService,
		shardInfo:                 &persistence.ShardInfo{ShardID: shardID, RangeID: 1, TransferAckLevel: 0},
		transferSequenceNumber:    1,
		executionManager:          s.mockExecutionMgr,
		historyMgr:                s.mockHistoryMgr,
		historyV2Mgr:              s.mockHistoryV2Mgr,
		domainCache:               domainCache,
		shardManager:              s.mockShardManager,
		maxTransferSequenceNumber: 100000,
		closeCh:                   s.shardClosedCh,
		config:                    s.config,
		logger:                    s.logger,
		metricsClient:             metrics.NewClient(tally.NoopScope, metrics.History),
	}
	s.eventsCache = newEventsCache(mockShard)
	mockShard.eventsCache = s.eventsCache
	currentClusterName := s.mockService.GetClusterMetadata().GetCurrentClusterName()
	shardContextWrapper := &shardContextWrapper{
		currentClusterName:   currentClusterName,
		ShardContext:         mockShard,
		historyEventNotifier: historyEventNotifier,
	}

	historyCache := newHistoryCache(shardContextWrapper)
	// this is used by shard context, not relevant to this test, so we do not care how many times "GetCurrentClusterName" os called
	s.mockClusterMetadata.On("GetCurrentClusterName").Return(cluster.TestCurrentClusterName)
	s.mockClusterMetadata.On("GetAllClusterFailoverVersions").Return(cluster.TestSingleDCAllClusterFailoverVersions)
	s.mockClusterMetadata.On("IsGlobalDomainEnabled").Return(false)
	h := &historyEngineImpl{
		currentClusterName:   currentClusterName,
		shard:                shardContextWrapper,
		executionManager:     s.mockExecutionMgr,
		historyMgr:           s.mockHistoryMgr,
		historyV2Mgr:         s.mockHistoryV2Mgr,
		historyCache:         historyCache,
		logger:               s.logger,
		metricsClient:        metrics.NewClient(tally.NoopScope, metrics.History),
		tokenSerializer:      common.NewJSONTaskTokenSerializer(),
		historyEventNotifier: historyEventNotifier,
		config:               NewDynamicConfigForTest(),
		archivalClient:       s.mockArchivalClient,
	}
	h.txProcessor = newTransferQueueProcessor(shardContextWrapper, h, s.mockVisibilityMgr, s.mockMatchingClient, s.mockHistoryClient, s.logger)
	h.timerProcessor = newTimerQueueProcessor(shardContextWrapper, h, s.mockMatchingClient, s.logger)
	h.historyEventNotifier.Start()
	shardContextWrapper.txProcessor = h.txProcessor
	s.mockHistoryEngine = h
}

func (s *engineSuite) TearDownTest() {
	s.mockHistoryEngine.historyEventNotifier.Stop()
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

func (s *engineSuite) TestGetMutableStateSync() {
	ctx := context.Background()
	domainID := validDomainID
	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("test-get-workflow-execution-event-id"),
		RunId:      common.StringPtr(validRunID),
	}
	tasklist := "testTaskList"
	identity := "testIdentity"

	msBuilder := newMutableStateBuilderWithEventV2(s.mockClusterMetadata.GetCurrentClusterName(), s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), execution.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, execution, "wType", tasklist, []byte("input"), 100, 200, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tasklist, identity)
	ms := createMutableState(msBuilder)
	gweResponse := &persistence.GetWorkflowExecutionResponse{State: ms}
	// right now the next event ID is 4
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gweResponse, nil).Once()

	// test get the next event ID instantly
	response, err := s.mockHistoryEngine.GetMutableState(ctx, &history.GetMutableStateRequest{
		DomainUUID: common.StringPtr(domainID),
		Execution:  &execution,
	})
	s.Nil(err)
	s.Equal(int64(4), response.GetNextEventId())
}

func (s *engineSuite) TestGetMutableState_InvalidRunID() {
	ctx := context.Background()
	domainID := validDomainID
	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("test-get-workflow-execution-event-id"),
		RunId:      common.StringPtr("run-id-not-valid-uuid"),
	}

	_, err := s.mockHistoryEngine.GetMutableState(ctx, &history.GetMutableStateRequest{
		DomainUUID: common.StringPtr(domainID),
		Execution:  &execution,
	})
	s.Equal(errRunIDNotValid, err)
}

func (s *engineSuite) TestGetMutableState_EmptyRunID() {
	ctx := context.Background()
	domainID := validDomainID
	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("test-get-workflow-execution-event-id"),
	}

	s.mockExecutionMgr.On("GetCurrentExecution", mock.Anything).Return(nil, &workflow.EntityNotExistsError{}).Once()

	_, err := s.mockHistoryEngine.GetMutableState(ctx, &history.GetMutableStateRequest{
		DomainUUID: common.StringPtr(domainID),
		Execution:  &execution,
	})
	s.Equal(&workflow.EntityNotExistsError{}, err)
}

func (s *engineSuite) TestGetMutableStateLongPoll() {
	ctx := context.Background()
	domainID := validDomainID
	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("test-get-workflow-execution-event-id"),
		RunId:      common.StringPtr(validRunID),
	}
	tasklist := "testTaskList"
	identity := "testIdentity"

	msBuilder := newMutableStateBuilderWithEventV2(s.mockClusterMetadata.GetCurrentClusterName(), s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), execution.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, execution, "wType", tasklist, []byte("input"), 100, 200, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tasklist, identity)
	ms := createMutableState(msBuilder)
	gweResponse := &persistence.GetWorkflowExecutionResponse{State: ms}
	// right now the next event ID is 4
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gweResponse, nil).Once()
	s.mockMetadataMgr.On("GetDomain", mock.Anything).Return(
		&persistence.GetDomainResponse{
			Info:   &persistence.DomainInfo{ID: domainID, Name: "testDomain"},
			Config: &persistence.DomainConfig{Retention: 1},
			ReplicationConfig: &persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				},
			},
			TableVersion: persistence.DomainTableVersionV1,
		},
		nil,
	).Once()

	// test long poll on next event ID change
	asycWorkflowUpdate := func(delay time.Duration) {
		taskToken, _ := json.Marshal(&common.TaskToken{
			WorkflowID: *execution.WorkflowId,
			RunID:      *execution.RunId,
			ScheduleID: 2,
		})
		s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
		s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()

		timer := time.NewTimer(delay)

		<-timer.C
		s.mockMetadataMgr.On("GetDomain", mock.Anything).Return(
			&persistence.GetDomainResponse{
				Info:   &persistence.DomainInfo{ID: domainID},
				Config: &persistence.DomainConfig{Retention: 1},
				ReplicationConfig: &persistence.DomainReplicationConfig{
					ActiveClusterName: cluster.TestCurrentClusterName,
					Clusters: []*persistence.ClusterReplicationConfig{
						&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
					},
				},
				TableVersion: persistence.DomainTableVersionV1,
			},
			nil,
		)
		s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &history.RespondDecisionTaskCompletedRequest{
			DomainUUID: common.StringPtr(domainID),
			CompleteRequest: &workflow.RespondDecisionTaskCompletedRequest{
				TaskToken: taskToken,
				Identity:  &identity,
			},
		})
		// right now the next event ID is 5
	}

	// return immediately, since the expected next event ID appears
	response, err := s.mockHistoryEngine.GetMutableState(ctx, &history.GetMutableStateRequest{
		DomainUUID:          common.StringPtr(domainID),
		Execution:           &execution,
		ExpectedNextEventId: common.Int64Ptr(3),
	})
	s.Nil(err)
	s.Equal(int64(4), *response.NextEventId)

	// long poll, new event happen before long poll timeout
	go asycWorkflowUpdate(time.Second * 2)
	start := time.Now()
	response, err = s.mockHistoryEngine.GetMutableState(ctx, &history.GetMutableStateRequest{
		DomainUUID:          common.StringPtr(domainID),
		Execution:           &execution,
		ExpectedNextEventId: common.Int64Ptr(4),
	})
	s.True(time.Now().After(start.Add(time.Second * 1)))
	s.Nil(err)
	s.Equal(int64(5), *response.NextEventId)
}

func (s *engineSuite) TestGetMutableStateLongPollTimeout() {
	ctx := context.Background()
	domainID := validDomainID
	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("test-get-workflow-execution-event-id"),
		RunId:      common.StringPtr(validRunID),
	}
	tasklist := "testTaskList"
	identity := "testIdentity"

	msBuilder := newMutableStateBuilderWithEventV2(s.mockClusterMetadata.GetCurrentClusterName(), s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), execution.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, execution, "wType", tasklist, []byte("input"), 100, 200, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tasklist, identity)
	ms := createMutableState(msBuilder)
	gweResponse := &persistence.GetWorkflowExecutionResponse{State: ms}
	// right now the next event ID is 4
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gweResponse, nil).Once()
	s.mockMetadataMgr.On("GetDomain", mock.Anything).Return(
		&persistence.GetDomainResponse{
			Info:   &persistence.DomainInfo{ID: domainID, Name: "testDomain"},
			Config: &persistence.DomainConfig{Retention: 1},
			ReplicationConfig: &persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				},
			},
			TableVersion: persistence.DomainTableVersionV1,
		},
		nil,
	).Once()

	// long poll, no event happen after long poll timeout
	response, err := s.mockHistoryEngine.GetMutableState(ctx, &history.GetMutableStateRequest{
		DomainUUID:          common.StringPtr(domainID),
		Execution:           &execution,
		ExpectedNextEventId: common.Int64Ptr(4),
	})
	s.Nil(err)
	s.Equal(int64(4), *response.NextEventId)
}

func (s *engineSuite) TestRespondDecisionTaskCompletedInvalidToken() {
	domainID := validDomainID
	invalidToken, _ := json.Marshal("bad token")
	identity := "testIdentity"

	s.mockMetadataMgr.On("GetDomain", mock.Anything).Return(
		&persistence.GetDomainResponse{
			Info:   &persistence.DomainInfo{ID: domainID},
			Config: &persistence.DomainConfig{Retention: 1},
			ReplicationConfig: &persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				},
			},
			TableVersion: persistence.DomainTableVersionV1,
		},
		nil,
	)
	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &history.RespondDecisionTaskCompletedRequest{
		DomainUUID: common.StringPtr(domainID),
		CompleteRequest: &workflow.RespondDecisionTaskCompletedRequest{
			TaskToken:        invalidToken,
			Decisions:        nil,
			ExecutionContext: nil,
			Identity:         &identity,
		},
	})

	s.NotNil(err)
	s.IsType(&workflow.BadRequestError{}, err)
}

func (s *engineSuite) TestRespondDecisionTaskCompletedIfNoExecution() {
	domainID := validDomainID
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: "wId",
		RunID:      validRunID,
		ScheduleID: 2,
	})
	identity := "testIdentity"

	s.mockMetadataMgr.On("GetDomain", mock.Anything).Return(
		&persistence.GetDomainResponse{
			Info:   &persistence.DomainInfo{ID: domainID},
			Config: &persistence.DomainConfig{Retention: 1},
			ReplicationConfig: &persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				},
			},
			TableVersion: persistence.DomainTableVersionV1,
		},
		nil,
	)
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(nil, &workflow.EntityNotExistsError{}).Once()

	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &history.RespondDecisionTaskCompletedRequest{
		DomainUUID: common.StringPtr(domainID),
		CompleteRequest: &workflow.RespondDecisionTaskCompletedRequest{
			TaskToken: taskToken,
			Identity:  &identity,
		},
	})
	s.NotNil(err)
	s.IsType(&workflow.EntityNotExistsError{}, err)
}

func (s *engineSuite) TestRespondDecisionTaskCompletedIfGetExecutionFailed() {
	domainID := validDomainID
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: "wId",
		RunID:      validRunID,
		ScheduleID: 2,
	})
	identity := "testIdentity"

	s.mockMetadataMgr.On("GetDomain", mock.Anything).Return(
		&persistence.GetDomainResponse{
			Info:   &persistence.DomainInfo{ID: domainID},
			Config: &persistence.DomainConfig{Retention: 1},
			ReplicationConfig: &persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				},
			},
			TableVersion: persistence.DomainTableVersionV1,
		},
		nil,
	)
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(nil, errors.New("FAILED")).Once()

	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &history.RespondDecisionTaskCompletedRequest{
		DomainUUID: common.StringPtr(domainID),
		CompleteRequest: &workflow.RespondDecisionTaskCompletedRequest{
			TaskToken: taskToken,
			Identity:  &identity,
		},
	})
	s.EqualError(err, "FAILED")
}

func (s *engineSuite) TestRespondDecisionTaskCompletedUpdateExecutionFailed() {
	domainID := validDomainID
	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(validRunID),
	}
	tl := "testTaskList"

	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: *we.WorkflowId,
		RunID:      *we.RunId,
		ScheduleID: 2,
	})
	identity := "testIdentity"

	msBuilder := newMutableStateBuilderWithEventV2(s.mockClusterMetadata.GetCurrentClusterName(), s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 200, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockMetadataMgr.On("GetDomain", mock.Anything).Return(
		&persistence.GetDomainResponse{
			Info:   &persistence.DomainInfo{ID: domainID},
			Config: &persistence.DomainConfig{Retention: 1},
			ReplicationConfig: &persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				},
			},
			TableVersion: persistence.DomainTableVersionV1,
		},
		nil,
	)
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, errors.New("FAILED")).Once()
	s.mockShardManager.On("UpdateShard", mock.Anything).Return(nil).Once()

	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &history.RespondDecisionTaskCompletedRequest{
		DomainUUID: common.StringPtr(domainID),
		CompleteRequest: &workflow.RespondDecisionTaskCompletedRequest{
			TaskToken: taskToken,
			Identity:  &identity,
		},
	})
	s.NotNil(err)
	s.EqualError(err, "FAILED")
}

func (s *engineSuite) TestRespondDecisionTaskCompletedIfTaskCompleted() {
	domainID := validDomainID
	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(validRunID),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: *we.WorkflowId,
		RunID:      *we.RunId,
		ScheduleID: 2,
	})
	identity := "testIdentity"

	msBuilder := newMutableStateBuilderWithEventV2(s.mockClusterMetadata.GetCurrentClusterName(), s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 200, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	startedEvent := addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)
	addDecisionTaskCompletedEvent(msBuilder, di.ScheduleID, *startedEvent.EventId, nil, identity)

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockMetadataMgr.On("GetDomain", mock.Anything).Return(
		&persistence.GetDomainResponse{
			Info:   &persistence.DomainInfo{ID: domainID},
			Config: &persistence.DomainConfig{Retention: 1},
			ReplicationConfig: &persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				},
			},
			TableVersion: persistence.DomainTableVersionV1,
		},
		nil,
	)
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()

	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &history.RespondDecisionTaskCompletedRequest{
		DomainUUID: common.StringPtr(domainID),
		CompleteRequest: &workflow.RespondDecisionTaskCompletedRequest{
			TaskToken: taskToken,
			Identity:  &identity,
		},
	})
	s.NotNil(err)
	s.IsType(&workflow.EntityNotExistsError{}, err)
}

func (s *engineSuite) TestRespondDecisionTaskCompletedIfTaskNotStarted() {
	domainID := validDomainID
	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(validRunID),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: *we.WorkflowId,
		RunID:      *we.RunId,
		ScheduleID: 2,
	})
	identity := "testIdentity"

	msBuilder := newMutableStateBuilderWithEventV2(s.mockClusterMetadata.GetCurrentClusterName(), s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 200, identity)
	addDecisionTaskScheduledEvent(msBuilder)

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockMetadataMgr.On("GetDomain", mock.Anything).Return(
		&persistence.GetDomainResponse{
			Info:   &persistence.DomainInfo{ID: domainID},
			Config: &persistence.DomainConfig{Retention: 1},
			ReplicationConfig: &persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				},
			},
			TableVersion: persistence.DomainTableVersionV1,
		},
		nil,
	)
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()

	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &history.RespondDecisionTaskCompletedRequest{
		DomainUUID: common.StringPtr(domainID),
		CompleteRequest: &workflow.RespondDecisionTaskCompletedRequest{
			TaskToken: taskToken,
		},
	})
	s.NotNil(err)
	s.IsType(&workflow.EntityNotExistsError{}, err)
}

func (s *engineSuite) TestRespondDecisionTaskCompletedConflictOnUpdate() {
	domainID := validDomainID
	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(validRunID),
	}
	tl := "testTaskList"
	identity := "testIdentity"
	executionContext := []byte("context")
	activity1ID := "activity1"
	activity1Type := "activity_type1"
	activity1Input := []byte("input1")
	activity1Result := []byte("activity1_result")
	activity2ID := "activity2"
	activity2Type := "activity_type2"
	activity2Input := []byte("input2")
	activity2Result := []byte("activity2_result")
	activity3ID := "activity3"
	activity3Type := "activity_type3"
	activity3Input := []byte("input3")

	msBuilder := newMutableStateBuilderWithEventV2(s.mockClusterMetadata.GetCurrentClusterName(), s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 100, identity)
	di1 := addDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent1 := addDecisionTaskStartedEvent(msBuilder, di1.ScheduleID, tl, identity)
	decisionCompletedEvent1 := addDecisionTaskCompletedEvent(msBuilder, di1.ScheduleID,
		*decisionStartedEvent1.EventId, nil, identity)
	activity1ScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, *decisionCompletedEvent1.EventId,
		activity1ID, activity1Type, tl, activity1Input, 100, 10, 5)
	activity2ScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, *decisionCompletedEvent1.EventId,
		activity2ID, activity2Type, tl, activity2Input, 100, 10, 5)
	activity1StartedEvent := addActivityTaskStartedEvent(msBuilder, *activity1ScheduledEvent.EventId, tl, identity)
	activity2StartedEvent := addActivityTaskStartedEvent(msBuilder, *activity2ScheduledEvent.EventId, tl, identity)
	addActivityTaskCompletedEvent(msBuilder, *activity1ScheduledEvent.EventId,
		*activity1StartedEvent.EventId, activity1Result, identity)
	di2 := addDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent2 := addDecisionTaskStartedEvent(msBuilder, di2.ScheduleID, tl, identity)

	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: "wId",
		RunID:      we.GetRunId(),
		ScheduleID: di2.ScheduleID,
	})

	decisions := []*workflow.Decision{{
		DecisionType: common.DecisionTypePtr(workflow.DecisionTypeScheduleActivityTask),
		ScheduleActivityTaskDecisionAttributes: &workflow.ScheduleActivityTaskDecisionAttributes{
			ActivityId:                    common.StringPtr(activity3ID),
			ActivityType:                  &workflow.ActivityType{Name: common.StringPtr(activity3Type)},
			TaskList:                      &workflow.TaskList{Name: &tl},
			Input:                         activity3Input,
			ScheduleToCloseTimeoutSeconds: common.Int32Ptr(100),
			ScheduleToStartTimeoutSeconds: common.Int32Ptr(10),
			StartToCloseTimeoutSeconds:    common.Int32Ptr(50),
			HeartbeatTimeoutSeconds:       common.Int32Ptr(5),
		},
	}}

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	addActivityTaskCompletedEvent(msBuilder, *activity2ScheduledEvent.EventId,
		*activity2StartedEvent.EventId, activity2Result, identity)

	ms2 := createMutableState(msBuilder)
	gwmsResponse2 := &persistence.GetWorkflowExecutionResponse{State: ms2}

	s.mockMetadataMgr.On("GetDomain", mock.Anything).Return(
		&persistence.GetDomainResponse{
			Info:   &persistence.DomainInfo{ID: domainID},
			Config: &persistence.DomainConfig{Retention: 1},
			ReplicationConfig: &persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				},
			},
			TableVersion: persistence.DomainTableVersionV1,
		},
		nil,
	)
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}},
		&persistence.ConditionFailedError{}).Once()

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse2, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()

	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &history.RespondDecisionTaskCompletedRequest{
		DomainUUID: common.StringPtr(domainID),
		CompleteRequest: &workflow.RespondDecisionTaskCompletedRequest{
			TaskToken:        taskToken,
			Decisions:        decisions,
			ExecutionContext: executionContext,
			Identity:         &identity,
		},
	})
	s.Nil(err, s.printHistory(msBuilder))
	s.Equal(int64(16), ms2.ExecutionInfo.NextEventID)
	s.Equal(*decisionStartedEvent2.EventId, ms2.ExecutionInfo.LastProcessedEvent)
	s.Equal(executionContext, ms2.ExecutionInfo.ExecutionContext)

	executionBuilder := s.getBuilder(domainID, we)
	activity3Attributes := s.getActivityScheduledEvent(executionBuilder, 13).ActivityTaskScheduledEventAttributes
	s.Equal(activity3ID, *activity3Attributes.ActivityId)
	s.Equal(activity3Type, *activity3Attributes.ActivityType.Name)
	s.Equal(int64(12), *activity3Attributes.DecisionTaskCompletedEventId)
	s.Equal(tl, *activity3Attributes.TaskList.Name)
	s.Equal(activity3Input, activity3Attributes.Input)
	s.Equal(int32(100), *activity3Attributes.ScheduleToCloseTimeoutSeconds)
	s.Equal(int32(10), *activity3Attributes.ScheduleToStartTimeoutSeconds)
	s.Equal(int32(50), *activity3Attributes.StartToCloseTimeoutSeconds)
	s.Equal(int32(5), *activity3Attributes.HeartbeatTimeoutSeconds)

	di, ok := executionBuilder.GetPendingDecision(15)
	s.True(ok)
	s.Equal(int32(100), di.DecisionTimeout)
}

func (s *engineSuite) TestValidateSignalRequest() {
	workflowType := "testType"
	input := []byte("input")
	startRequest := &workflow.StartWorkflowExecutionRequest{
		WorkflowId:                          common.StringPtr("ID"),
		WorkflowType:                        &workflow.WorkflowType{Name: &workflowType},
		TaskList:                            &workflow.TaskList{Name: common.StringPtr("taskptr")},
		Input:                               input,
		ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(10),
		TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(10),
		Identity:                            common.StringPtr("identity"),
	}
	err := validateStartWorkflowExecutionRequest(startRequest, 999)
	s.Error(err, "startRequest doesn't have request id, it should error out")
}

func (s *engineSuite) TestRespondDecisionTaskCompletedMaxAttemptsExceeded() {
	domainID := validDomainID
	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(validRunID),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: *we.WorkflowId,
		RunID:      *we.RunId,
		ScheduleID: 2,
	})
	identity := "testIdentity"
	executionContext := []byte("context")
	input := []byte("input")

	msBuilder := newMutableStateBuilderWithEventV2(s.mockClusterMetadata.GetCurrentClusterName(), s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 200, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)

	decisions := []*workflow.Decision{{
		DecisionType: common.DecisionTypePtr(workflow.DecisionTypeScheduleActivityTask),
		ScheduleActivityTaskDecisionAttributes: &workflow.ScheduleActivityTaskDecisionAttributes{
			ActivityId:                    common.StringPtr("activity1"),
			ActivityType:                  &workflow.ActivityType{Name: common.StringPtr("activity_type1")},
			TaskList:                      &workflow.TaskList{Name: &tl},
			Input:                         input,
			ScheduleToCloseTimeoutSeconds: common.Int32Ptr(100),
			ScheduleToStartTimeoutSeconds: common.Int32Ptr(10),
			StartToCloseTimeoutSeconds:    common.Int32Ptr(50),
			HeartbeatTimeoutSeconds:       common.Int32Ptr(5),
		},
	}}

	for i := 0; i < conditionalRetryCount; i++ {
		ms := createMutableState(msBuilder)
		gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

		s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
		s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
		s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}},
			&persistence.ConditionFailedError{}).Once()
	}

	s.mockMetadataMgr.On("GetDomain", mock.Anything).Return(
		&persistence.GetDomainResponse{
			Info:   &persistence.DomainInfo{ID: domainID},
			Config: &persistence.DomainConfig{Retention: 1},
			ReplicationConfig: &persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				},
			},
			TableVersion: persistence.DomainTableVersionV1,
		},
		nil,
	)
	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &history.RespondDecisionTaskCompletedRequest{
		DomainUUID: common.StringPtr(domainID),
		CompleteRequest: &workflow.RespondDecisionTaskCompletedRequest{
			TaskToken:        taskToken,
			Decisions:        decisions,
			ExecutionContext: executionContext,
			Identity:         &identity,
		},
	})
	s.NotNil(err)
	s.Equal(ErrMaxAttemptsExceeded, err)
}

func (s *engineSuite) TestRespondDecisionTaskCompletedCompleteWorkflowFailed() {
	domainID := validDomainID
	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(validRunID),
	}
	tl := "testTaskList"
	identity := "testIdentity"
	executionContext := []byte("context")
	activity1ID := "activity1"
	activity1Type := "activity_type1"
	activity1Input := []byte("input1")
	activity1Result := []byte("activity1_result")
	activity2ID := "activity2"
	activity2Type := "activity_type2"
	activity2Input := []byte("input2")
	activity2Result := []byte("activity2_result")
	workflowResult := []byte("workflow result")

	msBuilder := newMutableStateBuilderWithEventV2(s.mockClusterMetadata.GetCurrentClusterName(), s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 25, 200, identity)
	di1 := addDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent1 := addDecisionTaskStartedEvent(msBuilder, di1.ScheduleID, tl, identity)
	decisionCompletedEvent1 := addDecisionTaskCompletedEvent(msBuilder, di1.ScheduleID,
		*decisionStartedEvent1.EventId, nil, identity)
	activity1ScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, *decisionCompletedEvent1.EventId,
		activity1ID, activity1Type, tl, activity1Input, 100, 10, 5)
	activity2ScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, *decisionCompletedEvent1.EventId,
		activity2ID, activity2Type, tl, activity2Input, 100, 10, 5)
	activity1StartedEvent := addActivityTaskStartedEvent(msBuilder, *activity1ScheduledEvent.EventId, tl, identity)
	activity2StartedEvent := addActivityTaskStartedEvent(msBuilder, *activity2ScheduledEvent.EventId, tl, identity)
	addActivityTaskCompletedEvent(msBuilder, *activity1ScheduledEvent.EventId,
		*activity1StartedEvent.EventId, activity1Result, identity)
	di2 := addDecisionTaskScheduledEvent(msBuilder)
	addDecisionTaskStartedEvent(msBuilder, di2.ScheduleID, tl, identity)
	addActivityTaskCompletedEvent(msBuilder, *activity2ScheduledEvent.EventId,
		*activity2StartedEvent.EventId, activity2Result, identity)

	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: *we.WorkflowId,
		RunID:      *we.RunId,
		ScheduleID: di2.ScheduleID,
	})

	decisions := []*workflow.Decision{{
		DecisionType: common.DecisionTypePtr(workflow.DecisionTypeCompleteWorkflowExecution),
		CompleteWorkflowExecutionDecisionAttributes: &workflow.CompleteWorkflowExecutionDecisionAttributes{
			Result: workflowResult,
		},
	}}

	for i := 0; i < 2; i++ {
		ms := createMutableState(msBuilder)
		gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}
		s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	}

	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()

	s.mockMetadataMgr.On("GetDomain", mock.Anything).Return(
		&persistence.GetDomainResponse{
			Info:   &persistence.DomainInfo{ID: domainID},
			Config: &persistence.DomainConfig{Retention: 1},
			ReplicationConfig: &persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				},
			},
			TableVersion: persistence.DomainTableVersionV1,
		},
		nil,
	)
	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &history.RespondDecisionTaskCompletedRequest{
		DomainUUID: common.StringPtr(domainID),
		CompleteRequest: &workflow.RespondDecisionTaskCompletedRequest{
			TaskToken:        taskToken,
			Decisions:        decisions,
			ExecutionContext: executionContext,
			Identity:         &identity,
		},
	})
	s.Nil(err, s.printHistory(msBuilder))
	executionBuilder := s.getBuilder(domainID, we)
	s.Equal(int64(15), executionBuilder.GetExecutionInfo().NextEventID)
	s.Equal(*decisionStartedEvent1.EventId, executionBuilder.GetExecutionInfo().LastProcessedEvent)
	s.Equal(executionContext, executionBuilder.GetExecutionInfo().ExecutionContext)
	s.Equal(persistence.WorkflowStateRunning, executionBuilder.GetExecutionInfo().State)
	s.True(executionBuilder.HasPendingDecisionTask())
	di3, ok := executionBuilder.GetPendingDecision(executionBuilder.GetExecutionInfo().NextEventID - 1)
	s.True(ok)
	s.Equal(executionBuilder.GetExecutionInfo().NextEventID-1, di3.ScheduleID)
	s.Equal(int64(0), di3.Attempt)
}

func (s *engineSuite) TestRespondDecisionTaskCompletedFailWorkflowFailed() {
	domainID := validDomainID
	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(validRunID),
	}
	tl := "testTaskList"
	identity := "testIdentity"
	executionContext := []byte("context")
	activity1ID := "activity1"
	activity1Type := "activity_type1"
	activity1Input := []byte("input1")
	activity1Result := []byte("activity1_result")
	activity2ID := "activity2"
	activity2Type := "activity_type2"
	activity2Input := []byte("input2")
	activity2Result := []byte("activity2_result")
	reason := "workflow fail reason"
	details := []byte("workflow fail details")

	msBuilder := newMutableStateBuilderWithEventV2(s.mockClusterMetadata.GetCurrentClusterName(), s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 25, 200, identity)
	di1 := addDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent1 := addDecisionTaskStartedEvent(msBuilder, di1.ScheduleID, tl, identity)
	decisionCompletedEvent1 := addDecisionTaskCompletedEvent(msBuilder, di1.ScheduleID,
		*decisionStartedEvent1.EventId, nil, identity)
	activity1ScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, *decisionCompletedEvent1.EventId, activity1ID,
		activity1Type, tl, activity1Input, 100, 10, 5)
	activity2ScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, *decisionCompletedEvent1.EventId, activity2ID,
		activity2Type, tl, activity2Input, 100, 10, 5)
	activity1StartedEvent := addActivityTaskStartedEvent(msBuilder, *activity1ScheduledEvent.EventId, tl, identity)
	activity2StartedEvent := addActivityTaskStartedEvent(msBuilder, *activity2ScheduledEvent.EventId, tl, identity)
	addActivityTaskCompletedEvent(msBuilder, *activity1ScheduledEvent.EventId,
		*activity1StartedEvent.EventId, activity1Result, identity)
	di2 := addDecisionTaskScheduledEvent(msBuilder)
	addDecisionTaskStartedEvent(msBuilder, di2.ScheduleID, tl, identity)
	addActivityTaskCompletedEvent(msBuilder, *activity2ScheduledEvent.EventId,
		*activity2StartedEvent.EventId, activity2Result, identity)

	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: *we.WorkflowId,
		RunID:      *we.RunId,
		ScheduleID: di2.ScheduleID,
	})

	decisions := []*workflow.Decision{{
		DecisionType: common.DecisionTypePtr(workflow.DecisionTypeFailWorkflowExecution),
		FailWorkflowExecutionDecisionAttributes: &workflow.FailWorkflowExecutionDecisionAttributes{
			Reason:  &reason,
			Details: details,
		},
	}}

	for i := 0; i < 2; i++ {
		ms := createMutableState(msBuilder)
		gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}
		s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	}

	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()

	s.mockMetadataMgr.On("GetDomain", mock.Anything).Return(
		&persistence.GetDomainResponse{
			Info:   &persistence.DomainInfo{ID: domainID},
			Config: &persistence.DomainConfig{Retention: 1},
			ReplicationConfig: &persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				},
			},
			TableVersion: persistence.DomainTableVersionV1,
		},
		nil,
	)
	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &history.RespondDecisionTaskCompletedRequest{
		DomainUUID: common.StringPtr(domainID),
		CompleteRequest: &workflow.RespondDecisionTaskCompletedRequest{
			TaskToken:        taskToken,
			Decisions:        decisions,
			ExecutionContext: executionContext,
			Identity:         &identity,
		},
	})
	s.Nil(err, s.printHistory(msBuilder))
	executionBuilder := s.getBuilder(domainID, we)
	s.Equal(int64(15), executionBuilder.GetExecutionInfo().NextEventID)
	s.Equal(*decisionStartedEvent1.EventId, executionBuilder.GetExecutionInfo().LastProcessedEvent)
	s.Equal(executionContext, executionBuilder.GetExecutionInfo().ExecutionContext)
	s.Equal(persistence.WorkflowStateRunning, executionBuilder.GetExecutionInfo().State)
	s.True(executionBuilder.HasPendingDecisionTask())
	di3, ok := executionBuilder.GetPendingDecision(executionBuilder.GetExecutionInfo().NextEventID - 1)
	s.True(ok)
	s.Equal(executionBuilder.GetExecutionInfo().NextEventID-1, di3.ScheduleID)
	s.Equal(int64(0), di3.Attempt)
}

func (s *engineSuite) TestRespondDecisionTaskCompletedBadDecisionAttributes() {
	domainID := validDomainID
	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(validRunID),
	}
	tl := "testTaskList"
	identity := "testIdentity"
	executionContext := []byte("context")
	activity1ID := "activity1"
	activity1Type := "activity_type1"
	activity1Input := []byte("input1")
	activity1Result := []byte("activity1_result")

	msBuilder := newMutableStateBuilderWithEventV2(s.mockClusterMetadata.GetCurrentClusterName(), s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 25, 200, identity)
	di1 := addDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent1 := addDecisionTaskStartedEvent(msBuilder, di1.ScheduleID, tl, identity)
	decisionCompletedEvent1 := addDecisionTaskCompletedEvent(msBuilder, di1.ScheduleID,
		*decisionStartedEvent1.EventId, nil, identity)
	activity1ScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, *decisionCompletedEvent1.EventId, activity1ID,
		activity1Type, tl, activity1Input, 100, 10, 5)
	activity1StartedEvent := addActivityTaskStartedEvent(msBuilder, *activity1ScheduledEvent.EventId, tl, identity)
	addActivityTaskCompletedEvent(msBuilder, *activity1ScheduledEvent.EventId,
		*activity1StartedEvent.EventId, activity1Result, identity)
	di2 := addDecisionTaskScheduledEvent(msBuilder)
	addDecisionTaskStartedEvent(msBuilder, di2.ScheduleID, tl, identity)

	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: *we.WorkflowId,
		RunID:      *we.RunId,
		ScheduleID: di2.ScheduleID,
	})

	// Decision with nil attributes
	decisions := []*workflow.Decision{{
		DecisionType: common.DecisionTypePtr(workflow.DecisionTypeCompleteWorkflowExecution),
	}}

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()

	s.mockMetadataMgr.On("GetDomain", mock.Anything).Return(
		&persistence.GetDomainResponse{
			Info:   &persistence.DomainInfo{ID: domainID},
			Config: &persistence.DomainConfig{Retention: 1},
			ReplicationConfig: &persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				},
			},
			TableVersion: persistence.DomainTableVersionV1,
		},
		nil,
	)
	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &history.RespondDecisionTaskCompletedRequest{
		DomainUUID: common.StringPtr(domainID),
		CompleteRequest: &workflow.RespondDecisionTaskCompletedRequest{
			TaskToken:        taskToken,
			Decisions:        decisions,
			ExecutionContext: executionContext,
			Identity:         &identity,
		},
	})
	s.NotNil(err)
	s.IsType(&workflow.BadRequestError{}, err)
}

// This test unit tests the activity schedule timeout validation logic of HistoryEngine's RespondDecisionTaskComplete function.
// An scheduled activity decision has 3 timeouts: ScheduleToClose, ScheduleToStart and StartToClose.
// This test verifies that when either ScheduleToClose or ScheduleToStart and StartToClose are specified,
// HistoryEngine's validateActivityScheduleAttribute will deduce the missing timeout and fill it in
// instead of returning a BadRequest error and only when all three are missing should a BadRequest be returned.
func (s *engineSuite) TestRespondDecisionTaskCompletedSingleActivityScheduledAttribute() {
	workflowTimeout := int32(100)
	testIterationVariables := []struct {
		scheduleToClose         *int32
		scheduleToStart         *int32
		startToClose            *int32
		heartbeat               *int32
		expectedScheduleToClose int32
		expectedScheduleToStart int32
		expectedStartToClose    int32
		expectError             bool
	}{
		// No ScheduleToClose timeout, will use ScheduleToStart + StartToClose
		{nil, common.Int32Ptr(3), common.Int32Ptr(7), nil,
			3 + 7, 3, 7, false},
		// Has ScheduleToClose timeout but not ScheduleToStart or StartToClose,
		// will use ScheduleToClose for ScheduleToStart and StartToClose
		{common.Int32Ptr(7), nil, nil, nil,
			7, 7, 7, false},
		// No ScheduleToClose timeout, ScheduleToStart or StartToClose, expect error return
		{nil, nil, nil, nil,
			0, 0, 0, true},
		// Negative ScheduleToClose, expect error return
		{common.Int32Ptr(-1), nil, nil, nil,
			0, 0, 0, true},
		// Negative ScheduleToStart, expect error return
		{nil, common.Int32Ptr(-1), nil, nil,
			0, 0, 0, true},
		// Negative StartToClose, expect error return
		{nil, nil, common.Int32Ptr(-1), nil,
			0, 0, 0, true},
		// Negative HeartBeat, expect error return
		{nil, nil, nil, common.Int32Ptr(-1),
			0, 0, 0, true},
		// Use workflow timeout
		{common.Int32Ptr(workflowTimeout), nil, nil, nil,
			workflowTimeout, workflowTimeout, workflowTimeout, false},
		// Timeout larger than workflow timeout
		{common.Int32Ptr(workflowTimeout + 1), nil, nil, nil,
			workflowTimeout, workflowTimeout, workflowTimeout, false},
		{nil, common.Int32Ptr(workflowTimeout + 1), nil, nil,
			0, 0, 0, true},
		{nil, nil, common.Int32Ptr(workflowTimeout + 1), nil,
			0, 0, 0, true},
		{nil, nil, nil, common.Int32Ptr(workflowTimeout + 1),
			0, 0, 0, true},
		// No ScheduleToClose timeout, will use ScheduleToStart + StartToClose, but exceed limit
		{nil, common.Int32Ptr(workflowTimeout), common.Int32Ptr(10), nil,
			workflowTimeout, workflowTimeout, 10, false},
	}

	for _, iVar := range testIterationVariables {
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
		executionContext := []byte("context")
		input := []byte("input")

		msBuilder := newMutableStateBuilderWithEventV2(s.mockClusterMetadata.GetCurrentClusterName(), s.mockHistoryEngine.shard, s.eventsCache,
			loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
		addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), workflowTimeout, 200, identity)
		di := addDecisionTaskScheduledEvent(msBuilder)
		addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)

		decisions := []*workflow.Decision{{
			DecisionType: common.DecisionTypePtr(workflow.DecisionTypeScheduleActivityTask),
			ScheduleActivityTaskDecisionAttributes: &workflow.ScheduleActivityTaskDecisionAttributes{
				ActivityId:                    common.StringPtr("activity1"),
				ActivityType:                  &workflow.ActivityType{Name: common.StringPtr("activity_type1")},
				TaskList:                      &workflow.TaskList{Name: &tl},
				Input:                         input,
				ScheduleToCloseTimeoutSeconds: iVar.scheduleToClose,
				ScheduleToStartTimeoutSeconds: iVar.scheduleToStart,
				StartToCloseTimeoutSeconds:    iVar.startToClose,
				HeartbeatTimeoutSeconds:       iVar.heartbeat,
			},
		}}

		ms := createMutableState(msBuilder)
		gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

		s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()

		if !iVar.expectError {
			s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
			s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()
		}

		s.mockMetadataMgr.On("GetDomain", mock.Anything).Return(
			&persistence.GetDomainResponse{
				Info:   &persistence.DomainInfo{ID: domainID},
				Config: &persistence.DomainConfig{Retention: 1},
				ReplicationConfig: &persistence.DomainReplicationConfig{
					ActiveClusterName: cluster.TestCurrentClusterName,
					Clusters: []*persistence.ClusterReplicationConfig{
						&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
					},
				},
				TableVersion: persistence.DomainTableVersionV1,
			},
			nil,
		)
		_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &history.RespondDecisionTaskCompletedRequest{
			DomainUUID: common.StringPtr(domainID),
			CompleteRequest: &workflow.RespondDecisionTaskCompletedRequest{
				TaskToken:        taskToken,
				Decisions:        decisions,
				ExecutionContext: executionContext,
				Identity:         &identity,
			},
		})

		if !iVar.expectError {
			s.Nil(err, s.printHistory(msBuilder))
			executionBuilder := s.getBuilder(domainID, we)
			activity1Attributes := s.getActivityScheduledEvent(executionBuilder, int64(5)).ActivityTaskScheduledEventAttributes
			s.Equal(iVar.expectedScheduleToClose, activity1Attributes.GetScheduleToCloseTimeoutSeconds())
			s.Equal(iVar.expectedScheduleToStart, activity1Attributes.GetScheduleToStartTimeoutSeconds())
			s.Equal(iVar.expectedStartToClose, activity1Attributes.GetStartToCloseTimeoutSeconds())
		} else {
			s.NotNil(err)
		}
		s.TearDownTest()
		s.SetupTest()
	}
}

func (s *engineSuite) TestRespondDecisionTaskCompletedSingleActivityScheduledDecision() {
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
	executionContext := []byte("context")
	input := []byte("input")

	msBuilder := newMutableStateBuilderWithEventV2(s.mockClusterMetadata.GetCurrentClusterName(), s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 200, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)

	decisions := []*workflow.Decision{{
		DecisionType: common.DecisionTypePtr(workflow.DecisionTypeScheduleActivityTask),
		ScheduleActivityTaskDecisionAttributes: &workflow.ScheduleActivityTaskDecisionAttributes{
			ActivityId:                    common.StringPtr("activity1"),
			ActivityType:                  &workflow.ActivityType{Name: common.StringPtr("activity_type1")},
			TaskList:                      &workflow.TaskList{Name: &tl},
			Input:                         input,
			ScheduleToCloseTimeoutSeconds: common.Int32Ptr(100),
			ScheduleToStartTimeoutSeconds: common.Int32Ptr(10),
			StartToCloseTimeoutSeconds:    common.Int32Ptr(50),
			HeartbeatTimeoutSeconds:       common.Int32Ptr(5),
		},
	}}

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()

	s.mockMetadataMgr.On("GetDomain", mock.Anything).Return(
		&persistence.GetDomainResponse{
			Info:   &persistence.DomainInfo{ID: domainID},
			Config: &persistence.DomainConfig{Retention: 1},
			ReplicationConfig: &persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				},
			},
			TableVersion: persistence.DomainTableVersionV1,
		},
		nil,
	)
	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &history.RespondDecisionTaskCompletedRequest{
		DomainUUID: common.StringPtr(domainID),
		CompleteRequest: &workflow.RespondDecisionTaskCompletedRequest{
			TaskToken:        taskToken,
			Decisions:        decisions,
			ExecutionContext: executionContext,
			Identity:         &identity,
		},
	})
	s.Nil(err, s.printHistory(msBuilder))
	executionBuilder := s.getBuilder(domainID, we)
	s.Equal(int64(6), executionBuilder.GetExecutionInfo().NextEventID)
	s.Equal(int64(3), executionBuilder.GetExecutionInfo().LastProcessedEvent)
	s.Equal(executionContext, executionBuilder.GetExecutionInfo().ExecutionContext)
	s.Equal(persistence.WorkflowStateRunning, executionBuilder.GetExecutionInfo().State)
	s.False(executionBuilder.HasPendingDecisionTask())

	activity1Attributes := s.getActivityScheduledEvent(executionBuilder, int64(5)).ActivityTaskScheduledEventAttributes
	s.Equal("activity1", *activity1Attributes.ActivityId)
	s.Equal("activity_type1", *activity1Attributes.ActivityType.Name)
	s.Equal(int64(4), *activity1Attributes.DecisionTaskCompletedEventId)
	s.Equal(tl, *activity1Attributes.TaskList.Name)
	s.Equal(input, activity1Attributes.Input)
	s.Equal(int32(100), *activity1Attributes.ScheduleToCloseTimeoutSeconds)
	s.Equal(int32(10), *activity1Attributes.ScheduleToStartTimeoutSeconds)
	s.Equal(int32(50), *activity1Attributes.StartToCloseTimeoutSeconds)
	s.Equal(int32(5), *activity1Attributes.HeartbeatTimeoutSeconds)
}

func (s *engineSuite) TestRespondDecisionTaskCompletedCompleteWorkflowSuccess() {
	domainID := validDomainID
	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(validRunID),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: *we.WorkflowId,
		RunID:      *we.RunId,
		ScheduleID: 2,
	})
	identity := "testIdentity"
	executionContext := []byte("context")
	workflowResult := []byte("success")

	msBuilder := newMutableStateBuilderWithEventV2(s.mockClusterMetadata.GetCurrentClusterName(), s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 200, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)

	decisions := []*workflow.Decision{{
		DecisionType: common.DecisionTypePtr(workflow.DecisionTypeCompleteWorkflowExecution),
		CompleteWorkflowExecutionDecisionAttributes: &workflow.CompleteWorkflowExecutionDecisionAttributes{
			Result: workflowResult,
		},
	}}

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()
	s.mockMetadataMgr.On("GetDomain", mock.Anything).Return(
		&persistence.GetDomainResponse{
			Info: &persistence.DomainInfo{ID: domainID},
			Config: &persistence.DomainConfig{
				Retention:      1,
				ArchivalStatus: workflow.ArchivalStatusEnabled,
			},
			ReplicationConfig: &persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				},
			},
			TableVersion: persistence.DomainTableVersionV1,
		},
		nil,
	)

	s.mockClusterMetadata.On("IsArchivalEnabled").Return(true)
	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &history.RespondDecisionTaskCompletedRequest{
		DomainUUID: common.StringPtr(domainID),
		CompleteRequest: &workflow.RespondDecisionTaskCompletedRequest{
			TaskToken:        taskToken,
			Decisions:        decisions,
			ExecutionContext: executionContext,
			Identity:         &identity,
		},
	})
	s.Nil(err, s.printHistory(msBuilder))
	executionBuilder := s.getBuilder(domainID, we)
	s.Equal(int64(6), executionBuilder.GetExecutionInfo().NextEventID)
	s.Equal(int64(3), executionBuilder.GetExecutionInfo().LastProcessedEvent)
	s.Equal(executionContext, executionBuilder.GetExecutionInfo().ExecutionContext)
	s.Equal(persistence.WorkflowStateCompleted, executionBuilder.GetExecutionInfo().State)
	s.False(executionBuilder.HasPendingDecisionTask())
}

func (s *engineSuite) TestRespondDecisionTaskCompletedFailWorkflowSuccess() {
	domainID := validDomainID
	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(validRunID),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: *we.WorkflowId,
		RunID:      *we.RunId,
		ScheduleID: 2,
	})
	identity := "testIdentity"
	executionContext := []byte("context")
	details := []byte("fail workflow details")
	reason := "fail workflow reason"

	msBuilder := newMutableStateBuilderWithEventV2(s.mockClusterMetadata.GetCurrentClusterName(), s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 200, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)

	decisions := []*workflow.Decision{{
		DecisionType: common.DecisionTypePtr(workflow.DecisionTypeFailWorkflowExecution),
		FailWorkflowExecutionDecisionAttributes: &workflow.FailWorkflowExecutionDecisionAttributes{
			Reason:  &reason,
			Details: details,
		},
	}}

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()
	s.mockMetadataMgr.On("GetDomain", mock.Anything).Return(
		&persistence.GetDomainResponse{
			Info: &persistence.DomainInfo{ID: domainID},
			Config: &persistence.DomainConfig{
				Retention:      1,
				ArchivalStatus: workflow.ArchivalStatusEnabled,
			},
			ReplicationConfig: &persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				},
			},
			TableVersion: persistence.DomainTableVersionV1,
		},
		nil,
	)
	s.mockClusterMetadata.On("IsArchivalEnabled").Return(true)
	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &history.RespondDecisionTaskCompletedRequest{
		DomainUUID: common.StringPtr(domainID),
		CompleteRequest: &workflow.RespondDecisionTaskCompletedRequest{
			TaskToken:        taskToken,
			Decisions:        decisions,
			ExecutionContext: executionContext,
			Identity:         &identity,
		},
	})
	s.Nil(err, s.printHistory(msBuilder))
	executionBuilder := s.getBuilder(domainID, we)
	s.Equal(int64(6), executionBuilder.GetExecutionInfo().NextEventID)
	s.Equal(int64(3), executionBuilder.GetExecutionInfo().LastProcessedEvent)
	s.Equal(executionContext, executionBuilder.GetExecutionInfo().ExecutionContext)
	s.Equal(persistence.WorkflowStateCompleted, executionBuilder.GetExecutionInfo().State)
	s.False(executionBuilder.HasPendingDecisionTask())
}

func (s *engineSuite) TestRespondDecisionTaskCompletedSignalExternalWorkflowSuccess() {
	domainID := validDomainID
	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(validRunID),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: *we.WorkflowId,
		RunID:      *we.RunId,
		ScheduleID: 2,
	})
	identity := "testIdentity"
	executionContext := []byte("context")

	msBuilder := newMutableStateBuilderWithEventV2(s.mockClusterMetadata.GetCurrentClusterName(), s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 200, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)

	decisions := []*workflow.Decision{{
		DecisionType: common.DecisionTypePtr(workflow.DecisionTypeSignalExternalWorkflowExecution),
		SignalExternalWorkflowExecutionDecisionAttributes: &workflow.SignalExternalWorkflowExecutionDecisionAttributes{
			Domain: common.StringPtr(domainID),
			Execution: &workflow.WorkflowExecution{
				WorkflowId: we.WorkflowId,
				RunId:      we.RunId,
			},
			SignalName: common.StringPtr("signal"),
			Input:      []byte("test input"),
		},
	}}

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()
	s.mockMetadataMgr.On("GetDomain", mock.Anything).Return(
		&persistence.GetDomainResponse{
			Info:   &persistence.DomainInfo{ID: domainID},
			Config: &persistence.DomainConfig{Retention: 1},
			ReplicationConfig: &persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				},
			},
			TableVersion: persistence.DomainTableVersionV1,
		},
		nil,
	)
	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &history.RespondDecisionTaskCompletedRequest{
		DomainUUID: common.StringPtr(domainID),
		CompleteRequest: &workflow.RespondDecisionTaskCompletedRequest{
			TaskToken:        taskToken,
			Decisions:        decisions,
			ExecutionContext: executionContext,
			Identity:         &identity,
		},
	})
	s.Nil(err, s.printHistory(msBuilder))
	executionBuilder := s.getBuilder(domainID, we)
	s.Equal(int64(6), executionBuilder.GetExecutionInfo().NextEventID)
	s.Equal(int64(3), executionBuilder.GetExecutionInfo().LastProcessedEvent)
	s.Equal(executionContext, executionBuilder.GetExecutionInfo().ExecutionContext)
}

func (s *engineSuite) TestRespondDecisionTaskCompletedSignalExternalWorkflowFailed() {
	domainID := validDomainID
	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr("invalid run id"),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: we.GetWorkflowId(),
		RunID:      we.GetRunId(),
		ScheduleID: 2,
	})
	identity := "testIdentity"
	executionContext := []byte("context")

	msBuilder := newMutableStateBuilderWithEventV2(s.mockClusterMetadata.GetCurrentClusterName(), s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 200, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)

	decisions := []*workflow.Decision{{
		DecisionType: common.DecisionTypePtr(workflow.DecisionTypeSignalExternalWorkflowExecution),
		SignalExternalWorkflowExecutionDecisionAttributes: &workflow.SignalExternalWorkflowExecutionDecisionAttributes{
			Domain: common.StringPtr(domainID),
			Execution: &workflow.WorkflowExecution{
				WorkflowId: we.WorkflowId,
				RunId:      we.RunId,
			},
			SignalName: common.StringPtr("signal"),
			Input:      []byte("test input"),
		},
	}}

	s.mockMetadataMgr.On("GetDomain", mock.Anything).Return(
		&persistence.GetDomainResponse{
			Info:   &persistence.DomainInfo{ID: domainID},
			Config: &persistence.DomainConfig{Retention: 1},
			ReplicationConfig: &persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				},
			},
			TableVersion: persistence.DomainTableVersionV1,
		},
		nil,
	)
	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &history.RespondDecisionTaskCompletedRequest{
		DomainUUID: common.StringPtr(domainID),
		CompleteRequest: &workflow.RespondDecisionTaskCompletedRequest{
			TaskToken:        taskToken,
			Decisions:        decisions,
			ExecutionContext: executionContext,
			Identity:         &identity,
		},
	})

	s.EqualError(err, "BadRequestError{Message: RunID is not valid UUID.}")
}

func (s *engineSuite) TestRespondDecisionTaskCompletedSignalExternalWorkflowFailed_UnKnownDomain() {
	domainID := validDomainID
	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(validRunID),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: *we.WorkflowId,
		RunID:      *we.RunId,
		ScheduleID: 2,
	})
	identity := "testIdentity"
	executionContext := []byte("context")
	foreignDomain := "unknown domain"

	msBuilder := newMutableStateBuilderWithEventV2(s.mockClusterMetadata.GetCurrentClusterName(), s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 200, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)

	decisions := []*workflow.Decision{{
		DecisionType: common.DecisionTypePtr(workflow.DecisionTypeSignalExternalWorkflowExecution),
		SignalExternalWorkflowExecutionDecisionAttributes: &workflow.SignalExternalWorkflowExecutionDecisionAttributes{
			Domain: common.StringPtr(foreignDomain),
			Execution: &workflow.WorkflowExecution{
				WorkflowId: we.WorkflowId,
				RunId:      we.RunId,
			},
			SignalName: common.StringPtr("signal"),
			Input:      []byte("test input"),
		},
	}}

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockMetadataMgr.On("GetDomain", &persistence.GetDomainRequest{ID: domainID}).Return(
		&persistence.GetDomainResponse{
			Info:   &persistence.DomainInfo{ID: domainID},
			Config: &persistence.DomainConfig{Retention: 1},
			ReplicationConfig: &persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				},
			},
			TableVersion: persistence.DomainTableVersionV1,
		},
		nil,
	).Once()
	s.mockMetadataMgr.On("GetDomain", &persistence.GetDomainRequest{Name: foreignDomain}).Return(
		nil, errors.New("get foreign domain error")).Once()

	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &history.RespondDecisionTaskCompletedRequest{
		DomainUUID: common.StringPtr(domainID),
		CompleteRequest: &workflow.RespondDecisionTaskCompletedRequest{
			TaskToken:        taskToken,
			Decisions:        decisions,
			ExecutionContext: executionContext,
			Identity:         &identity,
		},
	})

	s.NotNil(err)
}

func (s *engineSuite) TestRespondActivityTaskCompletedInvalidToken() {
	domainID := validDomainID
	invalidToken, _ := json.Marshal("bad token")
	identity := "testIdentity"

	s.mockMetadataMgr.On("GetDomain", mock.Anything).Return(
		&persistence.GetDomainResponse{
			Info:   &persistence.DomainInfo{ID: domainID},
			Config: &persistence.DomainConfig{Retention: 1},
			ReplicationConfig: &persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				},
			},
			TableVersion: persistence.DomainTableVersionV1,
		},
		nil,
	)
	err := s.mockHistoryEngine.RespondActivityTaskCompleted(context.Background(), &history.RespondActivityTaskCompletedRequest{
		DomainUUID: common.StringPtr(domainID),
		CompleteRequest: &workflow.RespondActivityTaskCompletedRequest{
			TaskToken: invalidToken,
			Result:    nil,
			Identity:  &identity,
		},
	})

	s.NotNil(err)
	s.IsType(&workflow.BadRequestError{}, err)
}

func (s *engineSuite) TestRespondActivityTaskCompletedIfNoExecution() {
	domainID := validDomainID
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: "wId",
		RunID:      validRunID,
		ScheduleID: 2,
	})
	identity := "testIdentity"

	s.mockMetadataMgr.On("GetDomain", mock.Anything).Return(
		&persistence.GetDomainResponse{
			Info:   &persistence.DomainInfo{ID: domainID},
			Config: &persistence.DomainConfig{Retention: 1},
			ReplicationConfig: &persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				},
			},
			TableVersion: persistence.DomainTableVersionV1,
		},
		nil,
	)
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(nil, &workflow.EntityNotExistsError{}).Once()

	err := s.mockHistoryEngine.RespondActivityTaskCompleted(context.Background(), &history.RespondActivityTaskCompletedRequest{
		DomainUUID: common.StringPtr(domainID),
		CompleteRequest: &workflow.RespondActivityTaskCompletedRequest{
			TaskToken: taskToken,
			Identity:  &identity,
		},
	})
	s.NotNil(err)
	s.IsType(&workflow.EntityNotExistsError{}, err)
}

func (s *engineSuite) TestRespondActivityTaskCompletedIfNoRunID() {
	domainID := validDomainID
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: "wId",
		ScheduleID: 2,
	})
	identity := "testIdentity"

	s.mockMetadataMgr.On("GetDomain", mock.Anything).Return(
		&persistence.GetDomainResponse{
			Info:   &persistence.DomainInfo{ID: domainID},
			Config: &persistence.DomainConfig{Retention: 1},
			ReplicationConfig: &persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				},
			},
			TableVersion: persistence.DomainTableVersionV1,
		},
		nil,
	)
	s.mockExecutionMgr.On("GetCurrentExecution", mock.Anything).Return(nil, &workflow.EntityNotExistsError{}).Once()

	err := s.mockHistoryEngine.RespondActivityTaskCompleted(context.Background(), &history.RespondActivityTaskCompletedRequest{
		DomainUUID: common.StringPtr(domainID),
		CompleteRequest: &workflow.RespondActivityTaskCompletedRequest{
			TaskToken: taskToken,
			Identity:  &identity,
		},
	})
	s.NotNil(err)
	s.IsType(&workflow.EntityNotExistsError{}, err)
}

func (s *engineSuite) TestRespondActivityTaskCompletedIfGetExecutionFailed() {
	domainID := validDomainID
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: "wId",
		RunID:      validRunID,
		ScheduleID: 2,
	})
	identity := "testIdentity"

	s.mockMetadataMgr.On("GetDomain", mock.Anything).Return(
		&persistence.GetDomainResponse{
			Info:   &persistence.DomainInfo{ID: domainID},
			Config: &persistence.DomainConfig{Retention: 1},
			ReplicationConfig: &persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				},
			},
			TableVersion: persistence.DomainTableVersionV1,
		},
		nil,
	)
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(nil, errors.New("FAILED")).Once()

	err := s.mockHistoryEngine.RespondActivityTaskCompleted(context.Background(), &history.RespondActivityTaskCompletedRequest{
		DomainUUID: common.StringPtr(domainID),
		CompleteRequest: &workflow.RespondActivityTaskCompletedRequest{
			TaskToken: taskToken,
			Identity:  &identity,
		},
	})
	s.EqualError(err, "FAILED")
}

func (s *engineSuite) TestRespondActivityTaskCompletedIfNoAIdProvided() {
	domainID := validDomainID
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: "wId",
		ScheduleID: common.EmptyEventID,
	})
	identity := "testIdentity"

	msBuilder := newMutableStateBuilderWithEventV2(s.mockClusterMetadata.GetCurrentClusterName(), s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), validRunID)
	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}
	gceResponse := &persistence.GetCurrentExecutionResponse{RunID: validRunID}

	s.mockMetadataMgr.On("GetDomain", mock.Anything).Return(
		&persistence.GetDomainResponse{
			Info:   &persistence.DomainInfo{ID: domainID},
			Config: &persistence.DomainConfig{Retention: 1},
			ReplicationConfig: &persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				},
			},
			TableVersion: persistence.DomainTableVersionV1,
		},
		nil,
	)
	s.mockExecutionMgr.On("GetCurrentExecution", mock.Anything).Return(gceResponse, nil).Once()
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()

	err := s.mockHistoryEngine.RespondActivityTaskCompleted(context.Background(), &history.RespondActivityTaskCompletedRequest{
		DomainUUID: common.StringPtr(domainID),
		CompleteRequest: &workflow.RespondActivityTaskCompletedRequest{
			TaskToken: taskToken,
			Identity:  &identity,
		},
	})
	s.EqualError(err, "BadRequestError{Message: Neither ActivityID nor ScheduleID is provided}")
}

func (s *engineSuite) TestRespondActivityTaskCompletedIfNoAidFound() {
	domainID := validDomainID
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: "wId",
		ScheduleID: common.EmptyEventID,
		ActivityID: "aid",
	})
	identity := "testIdentity"

	msBuilder := newMutableStateBuilderWithEventV2(s.mockClusterMetadata.GetCurrentClusterName(), s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), validRunID)
	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}
	gceResponse := &persistence.GetCurrentExecutionResponse{RunID: validRunID}

	s.mockMetadataMgr.On("GetDomain", mock.Anything).Return(
		&persistence.GetDomainResponse{
			Info:   &persistence.DomainInfo{ID: domainID},
			Config: &persistence.DomainConfig{Retention: 1},
			ReplicationConfig: &persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				},
			},
			TableVersion: persistence.DomainTableVersionV1,
		},
		nil,
	)
	s.mockExecutionMgr.On("GetCurrentExecution", mock.Anything).Return(gceResponse, nil).Once()
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()

	err := s.mockHistoryEngine.RespondActivityTaskCompleted(context.Background(), &history.RespondActivityTaskCompletedRequest{
		DomainUUID: common.StringPtr(domainID),
		CompleteRequest: &workflow.RespondActivityTaskCompletedRequest{
			TaskToken: taskToken,
			Identity:  &identity,
		},
	})
	s.EqualError(err, "BadRequestError{Message: No such activityID: aid\n}")
}

func (s *engineSuite) TestRespondActivityTaskCompletedUpdateExecutionFailed() {
	domainID := validDomainID
	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(validRunID),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: *we.WorkflowId,
		RunID:      *we.RunId,
		ScheduleID: 5,
	})
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := []byte("input1")
	activityResult := []byte("activity result")

	msBuilder := newMutableStateBuilderWithEventV2(s.mockClusterMetadata.GetCurrentClusterName(), s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 200, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent := addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)
	decisionCompletedEvent := addDecisionTaskCompletedEvent(msBuilder, di.ScheduleID,
		*decisionStartedEvent.EventId, nil, identity)
	activityScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, *decisionCompletedEvent.EventId, activityID,
		activityType, tl, activityInput, 100, 10, 5)
	addActivityTaskStartedEvent(msBuilder, *activityScheduledEvent.EventId, tl, identity)

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockMetadataMgr.On("GetDomain", mock.Anything).Return(
		&persistence.GetDomainResponse{
			Info:   &persistence.DomainInfo{ID: domainID},
			Config: &persistence.DomainConfig{Retention: 1},
			ReplicationConfig: &persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				},
			},
			TableVersion: persistence.DomainTableVersionV1,
		},
		nil,
	)
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, errors.New("FAILED")).Once()
	s.mockShardManager.On("UpdateShard", mock.Anything).Return(nil).Once()

	err := s.mockHistoryEngine.RespondActivityTaskCompleted(context.Background(), &history.RespondActivityTaskCompletedRequest{
		DomainUUID: common.StringPtr(domainID),
		CompleteRequest: &workflow.RespondActivityTaskCompletedRequest{
			TaskToken: taskToken,
			Result:    activityResult,
			Identity:  &identity,
		},
	})
	s.EqualError(err, "FAILED")
}

func (s *engineSuite) TestRespondActivityTaskCompletedIfTaskCompleted() {
	domainID := validDomainID
	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(validRunID),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: *we.WorkflowId,
		RunID:      *we.RunId,
		ScheduleID: 5,
	})
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := []byte("input1")
	activityResult := []byte("activity result")

	msBuilder := newMutableStateBuilderWithEventV2(s.mockClusterMetadata.GetCurrentClusterName(), s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 200, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent := addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)
	decisionCompletedEvent := addDecisionTaskCompletedEvent(msBuilder, di.ScheduleID,
		*decisionStartedEvent.EventId, nil, identity)
	activityScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, *decisionCompletedEvent.EventId, activityID,
		activityType, tl, activityInput, 100, 10, 5)
	activityStartedEvent := addActivityTaskStartedEvent(msBuilder, *activityScheduledEvent.EventId, tl, identity)
	addActivityTaskCompletedEvent(msBuilder, *activityScheduledEvent.EventId, *activityStartedEvent.EventId,
		activityResult, identity)
	addDecisionTaskScheduledEvent(msBuilder)

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockMetadataMgr.On("GetDomain", mock.Anything).Return(
		&persistence.GetDomainResponse{
			Info:   &persistence.DomainInfo{ID: domainID},
			Config: &persistence.DomainConfig{Retention: 1},
			ReplicationConfig: &persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				},
			},
			TableVersion: persistence.DomainTableVersionV1,
		},
		nil,
	)
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()

	err := s.mockHistoryEngine.RespondActivityTaskCompleted(context.Background(), &history.RespondActivityTaskCompletedRequest{
		DomainUUID: common.StringPtr(domainID),
		CompleteRequest: &workflow.RespondActivityTaskCompletedRequest{
			TaskToken: taskToken,
			Result:    activityResult,
			Identity:  &identity,
		},
	})
	s.NotNil(err)
	s.IsType(&workflow.EntityNotExistsError{}, err)
}

func (s *engineSuite) TestRespondActivityTaskCompletedIfTaskNotStarted() {
	domainID := validDomainID
	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(validRunID),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: *we.WorkflowId,
		RunID:      *we.RunId,
		ScheduleID: 5,
	})
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := []byte("input1")
	activityResult := []byte("activity result")

	msBuilder := newMutableStateBuilderWithEventV2(s.mockClusterMetadata.GetCurrentClusterName(), s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 200, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent := addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)
	decisionCompletedEvent := addDecisionTaskCompletedEvent(msBuilder, di.ScheduleID,
		*decisionStartedEvent.EventId, nil, identity)
	addActivityTaskScheduledEvent(msBuilder, *decisionCompletedEvent.EventId, activityID,
		activityType, tl, activityInput, 100, 10, 5)

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockMetadataMgr.On("GetDomain", mock.Anything).Return(
		&persistence.GetDomainResponse{
			Info:   &persistence.DomainInfo{ID: domainID},
			Config: &persistence.DomainConfig{Retention: 1},
			ReplicationConfig: &persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				},
			},
			TableVersion: persistence.DomainTableVersionV1,
		},
		nil,
	)
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()

	err := s.mockHistoryEngine.RespondActivityTaskCompleted(context.Background(), &history.RespondActivityTaskCompletedRequest{
		DomainUUID: common.StringPtr(domainID),
		CompleteRequest: &workflow.RespondActivityTaskCompletedRequest{
			TaskToken: taskToken,
			Result:    activityResult,
			Identity:  &identity,
		},
	})
	s.NotNil(err)
	s.IsType(&workflow.EntityNotExistsError{}, err)
}

func (s *engineSuite) TestRespondActivityTaskCompletedConflictOnUpdate() {
	domainID := validDomainID
	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(validRunID),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: *we.WorkflowId,
		RunID:      *we.RunId,
		ScheduleID: 5,
	})
	identity := "testIdentity"
	activity1ID := "activity1"
	activity1Type := "activity_type1"
	activity1Input := []byte("input1")
	activity1Result := []byte("activity1_result")
	activity2ID := "activity2"
	activity2Type := "activity_type2"
	activity2Input := []byte("input2")

	msBuilder := newMutableStateBuilderWithEventV2(s.mockClusterMetadata.GetCurrentClusterName(), s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 100, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent1 := addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)
	decisionCompletedEvent1 := addDecisionTaskCompletedEvent(msBuilder, di.ScheduleID,
		*decisionStartedEvent1.EventId, nil, identity)
	activity1ScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, *decisionCompletedEvent1.EventId, activity1ID,
		activity1Type, tl, activity1Input, 100, 10, 5)
	activity2ScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, *decisionCompletedEvent1.EventId, activity2ID,
		activity2Type, tl, activity2Input, 100, 10, 5)
	addActivityTaskStartedEvent(msBuilder, *activity1ScheduledEvent.EventId, tl, identity)
	addActivityTaskStartedEvent(msBuilder, *activity2ScheduledEvent.EventId, tl, identity)

	ms1 := createMutableState(msBuilder)
	gwmsResponse1 := &persistence.GetWorkflowExecutionResponse{State: ms1}

	ms2 := createMutableState(msBuilder)
	gwmsResponse2 := &persistence.GetWorkflowExecutionResponse{State: ms2}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse1, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, &persistence.ConditionFailedError{}).Once()

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse2, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()

	s.mockMetadataMgr.On("GetDomain", mock.Anything).Return(
		&persistence.GetDomainResponse{
			Info:   &persistence.DomainInfo{ID: domainID},
			Config: &persistence.DomainConfig{Retention: 1},
			ReplicationConfig: &persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				},
			},
			TableVersion: persistence.DomainTableVersionV1,
		},
		nil,
	)
	err := s.mockHistoryEngine.RespondActivityTaskCompleted(context.Background(), &history.RespondActivityTaskCompletedRequest{
		DomainUUID: common.StringPtr(domainID),
		CompleteRequest: &workflow.RespondActivityTaskCompletedRequest{
			TaskToken: taskToken,
			Result:    activity1Result,
			Identity:  &identity,
		},
	})
	s.Nil(err, s.printHistory(msBuilder))
	executionBuilder := s.getBuilder(domainID, we)
	s.Equal(int64(11), executionBuilder.GetExecutionInfo().NextEventID)
	s.Equal(int64(3), executionBuilder.GetExecutionInfo().LastProcessedEvent)
	s.Equal(persistence.WorkflowStateRunning, executionBuilder.GetExecutionInfo().State)

	s.True(executionBuilder.HasPendingDecisionTask())
	di, ok := executionBuilder.GetPendingDecision(int64(10))
	s.True(ok)
	s.Equal(int32(100), di.DecisionTimeout)
	s.Equal(int64(10), di.ScheduleID)
	s.Equal(common.EmptyEventID, di.StartedID)
}

func (s *engineSuite) TestRespondActivityTaskCompletedMaxAttemptsExceeded() {
	domainID := validDomainID
	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(validRunID),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: *we.WorkflowId,
		RunID:      *we.RunId,
		ScheduleID: 5,
	})
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := []byte("input1")
	activityResult := []byte("activity result")

	msBuilder := newMutableStateBuilderWithEventV2(s.mockClusterMetadata.GetCurrentClusterName(), s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 100, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent := addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)
	decisionCompletedEvent := addDecisionTaskCompletedEvent(msBuilder, di.ScheduleID,
		*decisionStartedEvent.EventId, nil, identity)
	activityScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, *decisionCompletedEvent.EventId, activityID,
		activityType, tl, activityInput, 100, 10, 5)
	addActivityTaskStartedEvent(msBuilder, *activityScheduledEvent.EventId, tl, identity)

	for i := 0; i < conditionalRetryCount; i++ {
		ms := createMutableState(msBuilder)
		gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

		s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
		s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
		s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, &persistence.ConditionFailedError{}).Once()
	}

	s.mockMetadataMgr.On("GetDomain", mock.Anything).Return(
		&persistence.GetDomainResponse{
			Info:   &persistence.DomainInfo{ID: domainID},
			Config: &persistence.DomainConfig{Retention: 1},
			ReplicationConfig: &persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				},
			},
			TableVersion: persistence.DomainTableVersionV1,
		},
		nil,
	)
	err := s.mockHistoryEngine.RespondActivityTaskCompleted(context.Background(), &history.RespondActivityTaskCompletedRequest{
		DomainUUID: common.StringPtr(domainID),
		CompleteRequest: &workflow.RespondActivityTaskCompletedRequest{
			TaskToken: taskToken,
			Result:    activityResult,
			Identity:  &identity,
		},
	})
	s.Equal(ErrMaxAttemptsExceeded, err)
}

func (s *engineSuite) TestRespondActivityTaskCompletedSuccess() {
	domainID := validDomainID
	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(validRunID),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: *we.WorkflowId,
		RunID:      *we.RunId,
		ScheduleID: 5,
	})
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := []byte("input1")
	activityResult := []byte("activity result")

	msBuilder := newMutableStateBuilderWithEventV2(s.mockClusterMetadata.GetCurrentClusterName(), s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 100, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent := addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)
	decisionCompletedEvent := addDecisionTaskCompletedEvent(msBuilder, di.ScheduleID,
		*decisionStartedEvent.EventId, nil, identity)
	activityScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, *decisionCompletedEvent.EventId, activityID,
		activityType, tl, activityInput, 100, 10, 5)
	addActivityTaskStartedEvent(msBuilder, *activityScheduledEvent.EventId, tl, identity)

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()

	s.mockMetadataMgr.On("GetDomain", mock.Anything).Return(
		&persistence.GetDomainResponse{
			Info:   &persistence.DomainInfo{ID: domainID},
			Config: &persistence.DomainConfig{Retention: 1},
			ReplicationConfig: &persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				},
			},
			TableVersion: persistence.DomainTableVersionV1,
		},
		nil,
	)
	err := s.mockHistoryEngine.RespondActivityTaskCompleted(context.Background(), &history.RespondActivityTaskCompletedRequest{
		DomainUUID: common.StringPtr(domainID),
		CompleteRequest: &workflow.RespondActivityTaskCompletedRequest{
			TaskToken: taskToken,
			Result:    activityResult,
			Identity:  &identity,
		},
	})
	s.Nil(err, s.printHistory(msBuilder))
	executionBuilder := s.getBuilder(domainID, we)
	s.Equal(int64(9), executionBuilder.GetExecutionInfo().NextEventID)
	s.Equal(int64(3), executionBuilder.GetExecutionInfo().LastProcessedEvent)
	s.Equal(persistence.WorkflowStateRunning, executionBuilder.GetExecutionInfo().State)

	s.True(executionBuilder.HasPendingDecisionTask())
	di, ok := executionBuilder.GetPendingDecision(int64(8))
	s.True(ok)
	s.Equal(int32(100), di.DecisionTimeout)
	s.Equal(int64(8), di.ScheduleID)
	s.Equal(common.EmptyEventID, di.StartedID)
}

func (s *engineSuite) TestRespondActivityTaskCompletedByIdSuccess() {
	domainID := validDomainID
	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(validRunID),
	}
	tl := "testTaskList"

	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := []byte("input1")
	activityResult := []byte("activity result")
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: *we.WorkflowId,
		ScheduleID: common.EmptyEventID,
		ActivityID: activityID,
	})

	msBuilder := newMutableStateBuilderWithEventV2(s.mockClusterMetadata.GetCurrentClusterName(), s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 100, identity)
	decisionScheduledEvent := addDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent := addDecisionTaskStartedEvent(msBuilder, decisionScheduledEvent.ScheduleID, tl, identity)
	decisionCompletedEvent := addDecisionTaskCompletedEvent(msBuilder, decisionScheduledEvent.ScheduleID,
		*decisionStartedEvent.EventId, nil, identity)
	activityScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, *decisionCompletedEvent.EventId, activityID,
		activityType, tl, activityInput, 100, 10, 5)
	addActivityTaskStartedEvent(msBuilder, *activityScheduledEvent.EventId, tl, identity)

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}
	gceResponse := &persistence.GetCurrentExecutionResponse{RunID: *we.RunId}

	s.mockExecutionMgr.On("GetCurrentExecution", mock.Anything).Return(gceResponse, nil).Once()
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()

	s.mockMetadataMgr.On("GetDomain", mock.Anything).Return(
		&persistence.GetDomainResponse{
			Info:   &persistence.DomainInfo{ID: domainID},
			Config: &persistence.DomainConfig{Retention: 1},
			ReplicationConfig: &persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				},
			},
			TableVersion: persistence.DomainTableVersionV1,
		},
		nil,
	)
	err := s.mockHistoryEngine.RespondActivityTaskCompleted(context.Background(), &history.RespondActivityTaskCompletedRequest{
		DomainUUID: common.StringPtr(domainID),
		CompleteRequest: &workflow.RespondActivityTaskCompletedRequest{
			TaskToken: taskToken,
			Result:    activityResult,
			Identity:  &identity,
		},
	})
	s.Nil(err, s.printHistory(msBuilder))
	executionBuilder := s.getBuilder(domainID, we)
	s.Equal(int64(9), executionBuilder.GetExecutionInfo().NextEventID)
	s.Equal(int64(3), executionBuilder.GetExecutionInfo().LastProcessedEvent)
	s.Equal(persistence.WorkflowStateRunning, executionBuilder.GetExecutionInfo().State)

	s.True(executionBuilder.HasPendingDecisionTask())
	di, ok := executionBuilder.GetPendingDecision(int64(8))
	s.True(ok)
	s.Equal(int32(100), di.DecisionTimeout)
	s.Equal(int64(8), di.ScheduleID)
	s.Equal(common.EmptyEventID, di.StartedID)
}

func (s *engineSuite) TestRespondActivityTaskFailedInvalidToken() {
	domainID := validDomainID
	invalidToken, _ := json.Marshal("bad token")
	identity := "testIdentity"

	s.mockMetadataMgr.On("GetDomain", mock.Anything).Return(
		&persistence.GetDomainResponse{
			Info:   &persistence.DomainInfo{ID: domainID},
			Config: &persistence.DomainConfig{Retention: 1},
			ReplicationConfig: &persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				},
			},
			TableVersion: persistence.DomainTableVersionV1,
		},
		nil,
	)
	err := s.mockHistoryEngine.RespondActivityTaskFailed(context.Background(), &history.RespondActivityTaskFailedRequest{
		DomainUUID: common.StringPtr(domainID),
		FailedRequest: &workflow.RespondActivityTaskFailedRequest{
			TaskToken: invalidToken,
			Identity:  &identity,
		},
	})

	s.NotNil(err)
	s.IsType(&workflow.BadRequestError{}, err)
}

func (s *engineSuite) TestRespondActivityTaskFailedIfNoExecution() {
	domainID := validDomainID
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: "wId",
		RunID:      validRunID,
		ScheduleID: 2,
	})
	identity := "testIdentity"

	s.mockMetadataMgr.On("GetDomain", mock.Anything).Return(
		&persistence.GetDomainResponse{
			Info:   &persistence.DomainInfo{ID: domainID},
			Config: &persistence.DomainConfig{Retention: 1},
			ReplicationConfig: &persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				},
			},
			TableVersion: persistence.DomainTableVersionV1,
		},
		nil,
	)
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(nil,
		&workflow.EntityNotExistsError{}).Once()

	err := s.mockHistoryEngine.RespondActivityTaskFailed(context.Background(), &history.RespondActivityTaskFailedRequest{
		DomainUUID: common.StringPtr(domainID),
		FailedRequest: &workflow.RespondActivityTaskFailedRequest{
			TaskToken: taskToken,
			Identity:  &identity,
		},
	})
	s.NotNil(err)
	s.IsType(&workflow.EntityNotExistsError{}, err)
}

func (s *engineSuite) TestRespondActivityTaskFailedIfNoRunID() {
	domainID := validDomainID
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: "wId",
		ScheduleID: 2,
	})
	identity := "testIdentity"

	s.mockMetadataMgr.On("GetDomain", mock.Anything).Return(
		&persistence.GetDomainResponse{
			Info:   &persistence.DomainInfo{ID: domainID},
			Config: &persistence.DomainConfig{Retention: 1},
			ReplicationConfig: &persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				},
			},
			TableVersion: persistence.DomainTableVersionV1,
		},
		nil,
	)
	s.mockExecutionMgr.On("GetCurrentExecution", mock.Anything).Return(nil,
		&workflow.EntityNotExistsError{}).Once()

	err := s.mockHistoryEngine.RespondActivityTaskFailed(context.Background(), &history.RespondActivityTaskFailedRequest{
		DomainUUID: common.StringPtr(domainID),
		FailedRequest: &workflow.RespondActivityTaskFailedRequest{
			TaskToken: taskToken,
			Identity:  &identity,
		},
	})
	s.NotNil(err)
	s.IsType(&workflow.EntityNotExistsError{}, err)
}

func (s *engineSuite) TestRespondActivityTaskFailedIfGetExecutionFailed() {
	domainID := validDomainID
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: "wId",
		RunID:      validRunID,
		ScheduleID: 2,
	})
	identity := "testIdentity"

	s.mockMetadataMgr.On("GetDomain", mock.Anything).Return(
		&persistence.GetDomainResponse{
			Info:   &persistence.DomainInfo{ID: domainID},
			Config: &persistence.DomainConfig{Retention: 1},
			ReplicationConfig: &persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				},
			},
			TableVersion: persistence.DomainTableVersionV1,
		},
		nil,
	)
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(nil,
		errors.New("FAILED")).Once()

	err := s.mockHistoryEngine.RespondActivityTaskFailed(context.Background(), &history.RespondActivityTaskFailedRequest{
		DomainUUID: common.StringPtr(domainID),
		FailedRequest: &workflow.RespondActivityTaskFailedRequest{
			TaskToken: taskToken,
			Identity:  &identity,
		},
	})
	s.EqualError(err, "FAILED")
}

func (s *engineSuite) TestRespondActivityTaskFailededIfNoAIdProvided() {
	domainID := validDomainID
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: "wId",
		ScheduleID: common.EmptyEventID,
	})
	identity := "testIdentity"

	msBuilder := newMutableStateBuilderWithEventV2(s.mockClusterMetadata.GetCurrentClusterName(), s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), validRunID)
	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}
	gceResponse := &persistence.GetCurrentExecutionResponse{RunID: validRunID}

	s.mockMetadataMgr.On("GetDomain", mock.Anything).Return(
		&persistence.GetDomainResponse{
			Info:   &persistence.DomainInfo{ID: domainID},
			Config: &persistence.DomainConfig{Retention: 1},
			ReplicationConfig: &persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				},
			},
			TableVersion: persistence.DomainTableVersionV1,
		},
		nil,
	)
	s.mockExecutionMgr.On("GetCurrentExecution", mock.Anything).Return(gceResponse, nil).Once()
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()

	err := s.mockHistoryEngine.RespondActivityTaskFailed(context.Background(), &history.RespondActivityTaskFailedRequest{
		DomainUUID: common.StringPtr(domainID),
		FailedRequest: &workflow.RespondActivityTaskFailedRequest{
			TaskToken: taskToken,
			Identity:  &identity,
		},
	})
	s.EqualError(err, "BadRequestError{Message: Neither ActivityID nor ScheduleID is provided}")
}

func (s *engineSuite) TestRespondActivityTaskFailededIfNoAIdFound() {
	domainID := validDomainID
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: "wId",
		ScheduleID: common.EmptyEventID,
		ActivityID: "aid",
	})
	identity := "testIdentity"

	msBuilder := newMutableStateBuilderWithEventV2(s.mockClusterMetadata.GetCurrentClusterName(), s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), validRunID)
	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}
	gceResponse := &persistence.GetCurrentExecutionResponse{RunID: validRunID}

	s.mockMetadataMgr.On("GetDomain", mock.Anything).Return(
		&persistence.GetDomainResponse{
			Info:   &persistence.DomainInfo{ID: domainID},
			Config: &persistence.DomainConfig{Retention: 1},
			ReplicationConfig: &persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				},
			},
			TableVersion: persistence.DomainTableVersionV1,
		},
		nil,
	)
	s.mockExecutionMgr.On("GetCurrentExecution", mock.Anything).Return(gceResponse, nil).Once()
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()

	err := s.mockHistoryEngine.RespondActivityTaskFailed(context.Background(), &history.RespondActivityTaskFailedRequest{
		DomainUUID: common.StringPtr(domainID),
		FailedRequest: &workflow.RespondActivityTaskFailedRequest{
			TaskToken: taskToken,
			Identity:  &identity,
		},
	})
	s.EqualError(err, "BadRequestError{Message: No such activityID: aid\n}")
}

func (s *engineSuite) TestRespondActivityTaskFailedUpdateExecutionFailed() {
	domainID := validDomainID
	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(validRunID),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: *we.WorkflowId,
		RunID:      *we.RunId,
		ScheduleID: 5,
	})
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := []byte("input1")

	msBuilder := newMutableStateBuilderWithEventV2(s.mockClusterMetadata.GetCurrentClusterName(), s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 100, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent := addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)
	decisionCompletedEvent := addDecisionTaskCompletedEvent(msBuilder, di.ScheduleID,
		*decisionStartedEvent.EventId, nil, identity)
	activityScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, *decisionCompletedEvent.EventId, activityID,
		activityType, tl, activityInput, 100, 10, 5)
	addActivityTaskStartedEvent(msBuilder, *activityScheduledEvent.EventId, tl, identity)

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, errors.New("FAILED")).Once()
	s.mockShardManager.On("UpdateShard", mock.Anything).Return(nil).Once()

	s.mockMetadataMgr.On("GetDomain", mock.Anything).Return(
		&persistence.GetDomainResponse{
			Info:   &persistence.DomainInfo{ID: domainID},
			Config: &persistence.DomainConfig{Retention: 1},
			ReplicationConfig: &persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				},
			},
			TableVersion: persistence.DomainTableVersionV1,
		},
		nil,
	)
	err := s.mockHistoryEngine.RespondActivityTaskFailed(context.Background(), &history.RespondActivityTaskFailedRequest{
		DomainUUID: common.StringPtr(domainID),
		FailedRequest: &workflow.RespondActivityTaskFailedRequest{
			TaskToken: taskToken,
			Identity:  &identity,
		},
	})
	s.EqualError(err, "FAILED")
}

func (s *engineSuite) TestRespondActivityTaskFailedIfTaskCompleted() {
	domainID := validDomainID
	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(validRunID),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: *we.WorkflowId,
		RunID:      *we.RunId,
		ScheduleID: 5,
	})
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := []byte("input1")
	failReason := "fail reason"
	details := []byte("fail details")

	msBuilder := newMutableStateBuilderWithEventV2(s.mockClusterMetadata.GetCurrentClusterName(), s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 100, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent := addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)
	decisionCompletedEvent := addDecisionTaskCompletedEvent(msBuilder, di.ScheduleID,
		*decisionStartedEvent.EventId, nil, identity)
	activityScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, *decisionCompletedEvent.EventId, activityID,
		activityType, tl, activityInput, 100, 10, 5)
	activityStartedEvent := addActivityTaskStartedEvent(msBuilder, *activityScheduledEvent.EventId, tl, identity)
	addActivityTaskFailedEvent(msBuilder, *activityScheduledEvent.EventId, *activityStartedEvent.EventId,
		failReason, details, identity)
	addDecisionTaskScheduledEvent(msBuilder)

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockMetadataMgr.On("GetDomain", mock.Anything).Return(
		&persistence.GetDomainResponse{
			Info:   &persistence.DomainInfo{ID: domainID},
			Config: &persistence.DomainConfig{Retention: 1},
			ReplicationConfig: &persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				},
			},
			TableVersion: persistence.DomainTableVersionV1,
		},
		nil,
	)
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()

	err := s.mockHistoryEngine.RespondActivityTaskFailed(context.Background(), &history.RespondActivityTaskFailedRequest{
		DomainUUID: common.StringPtr(domainID),
		FailedRequest: &workflow.RespondActivityTaskFailedRequest{
			TaskToken: taskToken,
			Reason:    &failReason,
			Details:   details,
			Identity:  &identity,
		},
	})
	s.NotNil(err)
	s.IsType(&workflow.EntityNotExistsError{}, err)
}

func (s *engineSuite) TestRespondActivityTaskFailedIfTaskNotStarted() {
	domainID := validDomainID
	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(validRunID),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: *we.WorkflowId,
		RunID:      *we.RunId,
		ScheduleID: 5,
	})
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := []byte("input1")

	msBuilder := newMutableStateBuilderWithEventV2(s.mockClusterMetadata.GetCurrentClusterName(), s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 100, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent := addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)
	decisionCompletedEvent := addDecisionTaskCompletedEvent(msBuilder, di.ScheduleID,
		*decisionStartedEvent.EventId, nil, identity)
	addActivityTaskScheduledEvent(msBuilder, *decisionCompletedEvent.EventId, activityID,
		activityType, tl, activityInput, 100, 10, 5)

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockMetadataMgr.On("GetDomain", mock.Anything).Return(
		&persistence.GetDomainResponse{
			Info:   &persistence.DomainInfo{ID: domainID},
			Config: &persistence.DomainConfig{Retention: 1},
			ReplicationConfig: &persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				},
			},
			TableVersion: persistence.DomainTableVersionV1,
		},
		nil,
	)
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()

	err := s.mockHistoryEngine.RespondActivityTaskFailed(context.Background(), &history.RespondActivityTaskFailedRequest{
		DomainUUID: common.StringPtr(domainID),
		FailedRequest: &workflow.RespondActivityTaskFailedRequest{
			TaskToken: taskToken,
			Identity:  &identity,
		},
	})
	s.NotNil(err)
	s.IsType(&workflow.EntityNotExistsError{}, err)
}

func (s *engineSuite) TestRespondActivityTaskFailedConflictOnUpdate() {
	domainID := validDomainID
	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(validRunID),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: *we.WorkflowId,
		RunID:      *we.RunId,
		ScheduleID: 5,
	})
	identity := "testIdentity"
	activity1ID := "activity1"
	activity1Type := "activity_type1"
	activity1Input := []byte("input1")
	failReason := "fail reason"
	details := []byte("fail details.")
	activity2ID := "activity2"
	activity2Type := "activity_type2"
	activity2Input := []byte("input2")
	activity2Result := []byte("activity2_result")

	msBuilder := newMutableStateBuilderWithEventV2(s.mockClusterMetadata.GetCurrentClusterName(), s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 25, 25, identity)
	di1 := addDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent1 := addDecisionTaskStartedEvent(msBuilder, di1.ScheduleID, tl, identity)
	decisionCompletedEvent1 := addDecisionTaskCompletedEvent(msBuilder, di1.ScheduleID,
		*decisionStartedEvent1.EventId, nil, identity)
	activity1ScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, *decisionCompletedEvent1.EventId, activity1ID,
		activity1Type, tl, activity1Input, 100, 10, 5)
	activity2ScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, *decisionCompletedEvent1.EventId, activity2ID,
		activity2Type, tl, activity2Input, 100, 10, 5)
	addActivityTaskStartedEvent(msBuilder, *activity1ScheduledEvent.EventId, tl, identity)
	activity2StartedEvent := addActivityTaskStartedEvent(msBuilder, *activity2ScheduledEvent.EventId, tl, identity)

	ms1 := createMutableState(msBuilder)
	gwmsResponse1 := &persistence.GetWorkflowExecutionResponse{State: ms1}

	addActivityTaskCompletedEvent(msBuilder, *activity2ScheduledEvent.EventId,
		*activity2StartedEvent.EventId, activity2Result, identity)
	addDecisionTaskScheduledEvent(msBuilder)

	ms2 := createMutableState(msBuilder)
	gwmsResponse2 := &persistence.GetWorkflowExecutionResponse{State: ms2}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse1, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, &persistence.ConditionFailedError{}).Once()

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse2, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()

	s.mockMetadataMgr.On("GetDomain", mock.Anything).Return(
		&persistence.GetDomainResponse{
			Info:   &persistence.DomainInfo{ID: domainID},
			Config: &persistence.DomainConfig{Retention: 1},
			ReplicationConfig: &persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				},
			},
			TableVersion: persistence.DomainTableVersionV1,
		},
		nil,
	)
	err := s.mockHistoryEngine.RespondActivityTaskFailed(context.Background(), &history.RespondActivityTaskFailedRequest{
		DomainUUID: common.StringPtr(domainID),
		FailedRequest: &workflow.RespondActivityTaskFailedRequest{
			TaskToken: taskToken,
			Reason:    &failReason,
			Details:   details,
			Identity:  &identity,
		},
	})
	s.Nil(err, s.printHistory(msBuilder))
	executionBuilder := s.getBuilder(domainID, we)
	s.Equal(int64(12), executionBuilder.GetExecutionInfo().NextEventID)
	s.Equal(int64(3), executionBuilder.GetExecutionInfo().LastProcessedEvent)
	s.Equal(persistence.WorkflowStateRunning, executionBuilder.GetExecutionInfo().State)

	s.True(executionBuilder.HasPendingDecisionTask())
	di, ok := executionBuilder.GetPendingDecision(int64(10))
	s.True(ok)
	s.Equal(int32(25), di.DecisionTimeout)
	s.Equal(int64(10), di.ScheduleID)
	s.Equal(common.EmptyEventID, di.StartedID)
}

func (s *engineSuite) TestRespondActivityTaskFailedMaxAttemptsExceeded() {
	domainID := validDomainID
	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(validRunID),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: *we.WorkflowId,
		RunID:      *we.RunId,
		ScheduleID: 5,
	})
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := []byte("input1")

	msBuilder := newMutableStateBuilderWithEventV2(s.mockClusterMetadata.GetCurrentClusterName(), s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 100, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent := addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)
	decisionCompletedEvent := addDecisionTaskCompletedEvent(msBuilder, di.ScheduleID,
		*decisionStartedEvent.EventId, nil, identity)
	activityScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, *decisionCompletedEvent.EventId, activityID,
		activityType, tl, activityInput, 100, 10, 5)
	addActivityTaskStartedEvent(msBuilder, *activityScheduledEvent.EventId, tl, identity)

	for i := 0; i < conditionalRetryCount; i++ {
		ms := createMutableState(msBuilder)
		gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

		s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
		s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
		s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, &persistence.ConditionFailedError{}).Once()
	}

	s.mockMetadataMgr.On("GetDomain", mock.Anything).Return(
		&persistence.GetDomainResponse{
			Info:   &persistence.DomainInfo{ID: domainID},
			Config: &persistence.DomainConfig{Retention: 1},
			ReplicationConfig: &persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				},
			},
			TableVersion: persistence.DomainTableVersionV1,
		},
		nil,
	)
	err := s.mockHistoryEngine.RespondActivityTaskFailed(context.Background(), &history.RespondActivityTaskFailedRequest{
		DomainUUID: common.StringPtr(domainID),
		FailedRequest: &workflow.RespondActivityTaskFailedRequest{
			TaskToken: taskToken,
			Identity:  &identity,
		},
	})
	s.Equal(ErrMaxAttemptsExceeded, err)
}

func (s *engineSuite) TestRespondActivityTaskFailedSuccess() {
	domainID := validDomainID
	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(validRunID),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: *we.WorkflowId,
		RunID:      *we.RunId,
		ScheduleID: 5,
	})
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := []byte("input1")
	failReason := "failed"
	failDetails := []byte("fail details.")

	msBuilder := newMutableStateBuilderWithEventV2(s.mockClusterMetadata.GetCurrentClusterName(), s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 100, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent := addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)
	decisionCompletedEvent := addDecisionTaskCompletedEvent(msBuilder, di.ScheduleID,
		*decisionStartedEvent.EventId, nil, identity)
	activityScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, *decisionCompletedEvent.EventId, activityID,
		activityType, tl, activityInput, 100, 10, 5)
	addActivityTaskStartedEvent(msBuilder, *activityScheduledEvent.EventId, tl, identity)

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()

	s.mockMetadataMgr.On("GetDomain", mock.Anything).Return(
		&persistence.GetDomainResponse{
			Info:   &persistence.DomainInfo{ID: domainID},
			Config: &persistence.DomainConfig{Retention: 1},
			ReplicationConfig: &persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				},
			},
			TableVersion: persistence.DomainTableVersionV1,
		},
		nil,
	)
	err := s.mockHistoryEngine.RespondActivityTaskFailed(context.Background(), &history.RespondActivityTaskFailedRequest{
		DomainUUID: common.StringPtr(domainID),
		FailedRequest: &workflow.RespondActivityTaskFailedRequest{
			TaskToken: taskToken,
			Reason:    &failReason,
			Details:   failDetails,
			Identity:  &identity,
		},
	})
	s.Nil(err)
	executionBuilder := s.getBuilder(domainID, we)
	s.Equal(int64(9), executionBuilder.GetExecutionInfo().NextEventID)
	s.Equal(int64(3), executionBuilder.GetExecutionInfo().LastProcessedEvent)
	s.Equal(persistence.WorkflowStateRunning, executionBuilder.GetExecutionInfo().State)

	s.True(executionBuilder.HasPendingDecisionTask())
	di, ok := executionBuilder.GetPendingDecision(int64(8))
	s.True(ok)
	s.Equal(int32(100), di.DecisionTimeout)
	s.Equal(int64(8), di.ScheduleID)
	s.Equal(common.EmptyEventID, di.StartedID)
}

func (s *engineSuite) TestRespondActivityTaskFailedByIDSuccess() {
	domainID := validDomainID
	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(validRunID),
	}
	tl := "testTaskList"

	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := []byte("input1")
	failReason := "failed"
	failDetails := []byte("fail details.")
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: *we.WorkflowId,
		ScheduleID: common.EmptyEventID,
		ActivityID: activityID,
	})

	msBuilder := newMutableStateBuilderWithEventV2(s.mockClusterMetadata.GetCurrentClusterName(), s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 100, identity)
	decisionScheduledEvent := addDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent := addDecisionTaskStartedEvent(msBuilder, decisionScheduledEvent.ScheduleID, tl, identity)
	decisionCompletedEvent := addDecisionTaskCompletedEvent(msBuilder, decisionScheduledEvent.ScheduleID,
		*decisionStartedEvent.EventId, nil, identity)
	activityScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, *decisionCompletedEvent.EventId, activityID,
		activityType, tl, activityInput, 100, 10, 5)
	addActivityTaskStartedEvent(msBuilder, *activityScheduledEvent.EventId, tl, identity)

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}
	gceResponse := &persistence.GetCurrentExecutionResponse{RunID: *we.RunId}

	s.mockExecutionMgr.On("GetCurrentExecution", mock.Anything).Return(gceResponse, nil).Once()
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()

	s.mockMetadataMgr.On("GetDomain", mock.Anything).Return(
		&persistence.GetDomainResponse{
			Info:   &persistence.DomainInfo{ID: domainID},
			Config: &persistence.DomainConfig{Retention: 1},
			ReplicationConfig: &persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				},
			},
			TableVersion: persistence.DomainTableVersionV1,
		},
		nil,
	)
	err := s.mockHistoryEngine.RespondActivityTaskFailed(context.Background(), &history.RespondActivityTaskFailedRequest{
		DomainUUID: common.StringPtr(domainID),
		FailedRequest: &workflow.RespondActivityTaskFailedRequest{
			TaskToken: taskToken,
			Reason:    &failReason,
			Details:   failDetails,
			Identity:  &identity,
		},
	})
	s.Nil(err)
	executionBuilder := s.getBuilder(domainID, we)
	s.Equal(int64(9), executionBuilder.GetExecutionInfo().NextEventID)
	s.Equal(int64(3), executionBuilder.GetExecutionInfo().LastProcessedEvent)
	s.Equal(persistence.WorkflowStateRunning, executionBuilder.GetExecutionInfo().State)

	s.True(executionBuilder.HasPendingDecisionTask())
	di, ok := executionBuilder.GetPendingDecision(int64(8))
	s.True(ok)
	s.Equal(int32(100), di.DecisionTimeout)
	s.Equal(int64(8), di.ScheduleID)
	s.Equal(common.EmptyEventID, di.StartedID)
}

func (s *engineSuite) TestRecordActivityTaskHeartBeatSuccess_NoTimer() {
	domainID := validDomainID
	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(validRunID),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: *we.WorkflowId,
		RunID:      *we.RunId,
		ScheduleID: 5,
	})
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := []byte("input1")

	msBuilder := newMutableStateBuilderWithEventV2(s.mockClusterMetadata.GetCurrentClusterName(), s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 100, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent := addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)
	decisionCompletedEvent := addDecisionTaskCompletedEvent(msBuilder, di.ScheduleID,
		*decisionStartedEvent.EventId, nil, identity)
	activityScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, *decisionCompletedEvent.EventId, activityID,
		activityType, tl, activityInput, 100, 10, 0)
	addActivityTaskStartedEvent(msBuilder, *activityScheduledEvent.EventId, tl, identity)

	// No HeartBeat timer running.
	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()

	detais := []byte("details")

	s.mockMetadataMgr.On("GetDomain", mock.Anything).Return(
		&persistence.GetDomainResponse{
			Info:   &persistence.DomainInfo{ID: domainID},
			Config: &persistence.DomainConfig{Retention: 1},
			ReplicationConfig: &persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				},
			},
			TableVersion: persistence.DomainTableVersionV1,
		},
		nil,
	)
	_, err := s.mockHistoryEngine.RecordActivityTaskHeartbeat(context.Background(), &history.RecordActivityTaskHeartbeatRequest{
		DomainUUID: common.StringPtr(domainID),
		HeartbeatRequest: &workflow.RecordActivityTaskHeartbeatRequest{
			TaskToken: taskToken,
			Identity:  &identity,
			Details:   detais,
		},
	})
	s.Nil(err)
}

func (s *engineSuite) TestRecordActivityTaskHeartBeatSuccess_TimerRunning() {
	domainID := validDomainID
	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(validRunID),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: *we.WorkflowId,
		RunID:      *we.RunId,
		ScheduleID: 5,
	})
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := []byte("input1")

	msBuilder := newMutableStateBuilderWithEventV2(s.mockClusterMetadata.GetCurrentClusterName(), s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 100, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent := addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)
	decisionCompletedEvent := addDecisionTaskCompletedEvent(msBuilder, di.ScheduleID,
		*decisionStartedEvent.EventId, nil, identity)
	activityScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, *decisionCompletedEvent.EventId, activityID,
		activityType, tl, activityInput, 100, 10, 1)
	addActivityTaskStartedEvent(msBuilder, *activityScheduledEvent.EventId, tl, identity)

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	// HeartBeat timer running.
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()

	detais := []byte("details")

	s.mockMetadataMgr.On("GetDomain", mock.Anything).Return(
		&persistence.GetDomainResponse{
			Info:   &persistence.DomainInfo{ID: domainID},
			Config: &persistence.DomainConfig{Retention: 1},
			ReplicationConfig: &persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				},
			},
			TableVersion: persistence.DomainTableVersionV1,
		},
		nil,
	)
	_, err := s.mockHistoryEngine.RecordActivityTaskHeartbeat(context.Background(), &history.RecordActivityTaskHeartbeatRequest{
		DomainUUID: common.StringPtr(domainID),
		HeartbeatRequest: &workflow.RecordActivityTaskHeartbeatRequest{
			TaskToken: taskToken,
			Identity:  &identity,
			Details:   detais,
		},
	})
	s.Nil(err)
	executionBuilder := s.getBuilder(domainID, we)
	s.Equal(int64(7), executionBuilder.GetExecutionInfo().NextEventID)
	s.Equal(int64(3), executionBuilder.GetExecutionInfo().LastProcessedEvent)
	s.Equal(persistence.WorkflowStateRunning, executionBuilder.GetExecutionInfo().State)
	s.False(executionBuilder.HasPendingDecisionTask())
}

func (s *engineSuite) TestRecordActivityTaskHeartBeatByIDSuccess() {
	domainID := validDomainID
	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(validRunID),
	}
	tl := "testTaskList"
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := []byte("input1")
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: *we.WorkflowId,
		RunID:      *we.RunId,
		ScheduleID: common.EmptyEventID,
		ActivityID: activityID,
	})

	msBuilder := newMutableStateBuilderWithEventV2(s.mockClusterMetadata.GetCurrentClusterName(), s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 100, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent := addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)
	decisionCompletedEvent := addDecisionTaskCompletedEvent(msBuilder, di.ScheduleID,
		*decisionStartedEvent.EventId, nil, identity)
	activityScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, *decisionCompletedEvent.EventId, activityID,
		activityType, tl, activityInput, 100, 10, 0)
	addActivityTaskStartedEvent(msBuilder, *activityScheduledEvent.EventId, tl, identity)

	// No HeartBeat timer running.
	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()

	detais := []byte("details")

	s.mockMetadataMgr.On("GetDomain", mock.Anything).Return(
		&persistence.GetDomainResponse{
			Info:   &persistence.DomainInfo{ID: domainID},
			Config: &persistence.DomainConfig{Retention: 1},
			ReplicationConfig: &persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				},
			},
			TableVersion: persistence.DomainTableVersionV1,
		},
		nil,
	)
	_, err := s.mockHistoryEngine.RecordActivityTaskHeartbeat(context.Background(), &history.RecordActivityTaskHeartbeatRequest{
		DomainUUID: common.StringPtr(domainID),
		HeartbeatRequest: &workflow.RecordActivityTaskHeartbeatRequest{
			TaskToken: taskToken,
			Identity:  &identity,
			Details:   detais,
		},
	})
	s.Nil(err)
}

func (s *engineSuite) TestRespondActivityTaskCanceled_Scheduled() {
	domainID := validDomainID
	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(validRunID),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: *we.WorkflowId,
		RunID:      *we.RunId,
		ScheduleID: 5,
	})
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := []byte("input1")

	msBuilder := newMutableStateBuilderWithEventV2(s.mockClusterMetadata.GetCurrentClusterName(), s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 100, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent := addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)
	decisionCompletedEvent := addDecisionTaskCompletedEvent(msBuilder, di.ScheduleID,
		*decisionStartedEvent.EventId, nil, identity)
	addActivityTaskScheduledEvent(msBuilder, *decisionCompletedEvent.EventId, activityID,
		activityType, tl, activityInput, 100, 10, 1)

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()

	s.mockMetadataMgr.On("GetDomain", mock.Anything).Return(
		&persistence.GetDomainResponse{
			Info:   &persistence.DomainInfo{ID: domainID},
			Config: &persistence.DomainConfig{Retention: 1},
			ReplicationConfig: &persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				},
			},
			TableVersion: persistence.DomainTableVersionV1,
		},
		nil,
	)
	err := s.mockHistoryEngine.RespondActivityTaskCanceled(context.Background(), &history.RespondActivityTaskCanceledRequest{
		DomainUUID: common.StringPtr(domainID),
		CancelRequest: &workflow.RespondActivityTaskCanceledRequest{
			TaskToken: taskToken,
			Identity:  &identity,
			Details:   []byte("details"),
		},
	})
	s.NotNil(err)
	s.IsType(&workflow.EntityNotExistsError{}, err)
}

func (s *engineSuite) TestRespondActivityTaskCanceled_Started() {
	domainID := validDomainID
	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(validRunID),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: *we.WorkflowId,
		RunID:      *we.RunId,
		ScheduleID: 5,
	})
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := []byte("input1")

	msBuilder := newMutableStateBuilderWithEventV2(s.mockClusterMetadata.GetCurrentClusterName(), s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 100, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent := addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)
	decisionCompletedEvent := addDecisionTaskCompletedEvent(msBuilder, di.ScheduleID,
		*decisionStartedEvent.EventId, nil, identity)
	activityScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, *decisionCompletedEvent.EventId, activityID,
		activityType, tl, activityInput, 100, 10, 1)
	addActivityTaskStartedEvent(msBuilder, *activityScheduledEvent.EventId, tl, identity)
	msBuilder.AddActivityTaskCancelRequestedEvent(*decisionCompletedEvent.EventId, activityID, identity)

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()

	s.mockMetadataMgr.On("GetDomain", mock.Anything).Return(
		&persistence.GetDomainResponse{
			Info:   &persistence.DomainInfo{ID: domainID},
			Config: &persistence.DomainConfig{Retention: 1},
			ReplicationConfig: &persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				},
			},
			TableVersion: persistence.DomainTableVersionV1,
		},
		nil,
	)
	err := s.mockHistoryEngine.RespondActivityTaskCanceled(context.Background(), &history.RespondActivityTaskCanceledRequest{
		DomainUUID: common.StringPtr(domainID),
		CancelRequest: &workflow.RespondActivityTaskCanceledRequest{
			TaskToken: taskToken,
			Identity:  &identity,
			Details:   []byte("details"),
		},
	})
	s.Nil(err)
	executionBuilder := s.getBuilder(domainID, we)
	s.Equal(int64(10), executionBuilder.GetExecutionInfo().NextEventID)
	s.Equal(int64(3), executionBuilder.GetExecutionInfo().LastProcessedEvent)
	s.Equal(persistence.WorkflowStateRunning, executionBuilder.GetExecutionInfo().State)

	s.True(executionBuilder.HasPendingDecisionTask())
	di, ok := executionBuilder.GetPendingDecision(int64(9))
	s.True(ok)
	s.Equal(int32(100), di.DecisionTimeout)
	s.Equal(int64(9), di.ScheduleID)
	s.Equal(common.EmptyEventID, di.StartedID)
}

func (s *engineSuite) TestRespondActivityTaskCanceledByID_Started() {
	domainID := validDomainID
	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(validRunID),
	}
	tl := "testTaskList"
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := []byte("input1")
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: *we.WorkflowId,
		ScheduleID: common.EmptyEventID,
		ActivityID: activityID,
	})

	msBuilder := newMutableStateBuilderWithEventV2(s.mockClusterMetadata.GetCurrentClusterName(), s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 100, identity)
	decisionScheduledEvent := addDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent := addDecisionTaskStartedEvent(msBuilder, decisionScheduledEvent.ScheduleID, tl, identity)
	decisionCompletedEvent := addDecisionTaskCompletedEvent(msBuilder, decisionScheduledEvent.ScheduleID,
		*decisionStartedEvent.EventId, nil, identity)
	activityScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, *decisionCompletedEvent.EventId, activityID,
		activityType, tl, activityInput, 100, 10, 1)
	addActivityTaskStartedEvent(msBuilder, *activityScheduledEvent.EventId, tl, identity)
	msBuilder.AddActivityTaskCancelRequestedEvent(*decisionCompletedEvent.EventId, activityID, identity)

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}
	gceResponse := &persistence.GetCurrentExecutionResponse{RunID: *we.RunId}

	s.mockExecutionMgr.On("GetCurrentExecution", mock.Anything).Return(gceResponse, nil).Once()
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()

	s.mockMetadataMgr.On("GetDomain", mock.Anything).Return(
		&persistence.GetDomainResponse{
			Info:   &persistence.DomainInfo{ID: domainID},
			Config: &persistence.DomainConfig{Retention: 1},
			ReplicationConfig: &persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				},
			},
			TableVersion: persistence.DomainTableVersionV1,
		},
		nil,
	)
	err := s.mockHistoryEngine.RespondActivityTaskCanceled(context.Background(), &history.RespondActivityTaskCanceledRequest{
		DomainUUID: common.StringPtr(domainID),
		CancelRequest: &workflow.RespondActivityTaskCanceledRequest{
			TaskToken: taskToken,
			Identity:  &identity,
			Details:   []byte("details"),
		},
	})
	s.Nil(err)
	executionBuilder := s.getBuilder(domainID, we)
	s.Equal(int64(10), executionBuilder.GetExecutionInfo().NextEventID)
	s.Equal(int64(3), executionBuilder.GetExecutionInfo().LastProcessedEvent)
	s.Equal(persistence.WorkflowStateRunning, executionBuilder.GetExecutionInfo().State)

	s.True(executionBuilder.HasPendingDecisionTask())
	di, ok := executionBuilder.GetPendingDecision(int64(9))
	s.True(ok)
	s.Equal(int32(100), di.DecisionTimeout)
	s.Equal(int64(9), di.ScheduleID)
	s.Equal(common.EmptyEventID, di.StartedID)
}

func (s *engineSuite) TestRespondActivityTaskCanceledIfNoRunID() {
	domainID := validDomainID
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: "wId",
		ScheduleID: 2,
	})
	identity := "testIdentity"

	s.mockExecutionMgr.On("GetCurrentExecution", mock.Anything).Return(nil, &workflow.EntityNotExistsError{}).Once()

	s.mockMetadataMgr.On("GetDomain", mock.Anything).Return(
		&persistence.GetDomainResponse{
			Info:   &persistence.DomainInfo{ID: domainID},
			Config: &persistence.DomainConfig{Retention: 1},
			ReplicationConfig: &persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				},
			},
			TableVersion: persistence.DomainTableVersionV1,
		},
		nil,
	)
	err := s.mockHistoryEngine.RespondActivityTaskCanceled(context.Background(), &history.RespondActivityTaskCanceledRequest{
		DomainUUID: common.StringPtr(domainID),
		CancelRequest: &workflow.RespondActivityTaskCanceledRequest{
			TaskToken: taskToken,
			Identity:  &identity,
		},
	})
	s.NotNil(err)
	s.IsType(&workflow.EntityNotExistsError{}, err)
}

func (s *engineSuite) TestRespondActivityTaskCanceledIfNoAIdProvided() {
	domainID := validDomainID
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: "wId",
		ScheduleID: common.EmptyEventID,
	})
	identity := "testIdentity"

	msBuilder := newMutableStateBuilderWithEventV2(s.mockClusterMetadata.GetCurrentClusterName(), s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), validRunID)
	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}
	gceResponse := &persistence.GetCurrentExecutionResponse{RunID: validRunID}

	s.mockExecutionMgr.On("GetCurrentExecution", mock.Anything).Return(gceResponse, nil).Once()
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()

	s.mockMetadataMgr.On("GetDomain", mock.Anything).Return(
		&persistence.GetDomainResponse{
			Info:   &persistence.DomainInfo{ID: domainID},
			Config: &persistence.DomainConfig{Retention: 1},
			ReplicationConfig: &persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				},
			},
			TableVersion: persistence.DomainTableVersionV1,
		},
		nil,
	)
	err := s.mockHistoryEngine.RespondActivityTaskCanceled(context.Background(), &history.RespondActivityTaskCanceledRequest{
		DomainUUID: common.StringPtr(domainID),
		CancelRequest: &workflow.RespondActivityTaskCanceledRequest{
			TaskToken: taskToken,
			Identity:  &identity,
		},
	})
	s.EqualError(err, "BadRequestError{Message: Neither ActivityID nor ScheduleID is provided}")
}

func (s *engineSuite) TestRespondActivityTaskCanceledIfNoAidFound() {
	domainID := validDomainID
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: "wId",
		ScheduleID: common.EmptyEventID,
		ActivityID: "aid",
	})
	identity := "testIdentity"

	msBuilder := newMutableStateBuilderWithEventV2(s.mockClusterMetadata.GetCurrentClusterName(), s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), validRunID)
	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}
	gceResponse := &persistence.GetCurrentExecutionResponse{RunID: validRunID}

	s.mockExecutionMgr.On("GetCurrentExecution", mock.Anything).Return(gceResponse, nil).Once()
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()

	s.mockMetadataMgr.On("GetDomain", mock.Anything).Return(
		&persistence.GetDomainResponse{
			Info:   &persistence.DomainInfo{ID: domainID},
			Config: &persistence.DomainConfig{Retention: 1},
			ReplicationConfig: &persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				},
			},
			TableVersion: persistence.DomainTableVersionV1,
		},
		nil,
	)
	err := s.mockHistoryEngine.RespondActivityTaskCanceled(context.Background(), &history.RespondActivityTaskCanceledRequest{
		DomainUUID: common.StringPtr(domainID),
		CancelRequest: &workflow.RespondActivityTaskCanceledRequest{
			TaskToken: taskToken,
			Identity:  &identity,
		},
	})
	s.EqualError(err, "BadRequestError{Message: No such activityID: aid\n}")
}

func (s *engineSuite) TestRequestCancel_RespondDecisionTaskCompleted_NotScheduled() {
	domainID := validDomainID
	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(validRunID),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: *we.WorkflowId,
		RunID:      *we.RunId,
		ScheduleID: 2,
	})
	identity := "testIdentity"
	activityID := "activity1_id"

	msBuilder := newMutableStateBuilderWithEventV2(s.mockClusterMetadata.GetCurrentClusterName(), s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 100, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)

	decisions := []*workflow.Decision{{
		DecisionType: common.DecisionTypePtr(workflow.DecisionTypeRequestCancelActivityTask),
		RequestCancelActivityTaskDecisionAttributes: &workflow.RequestCancelActivityTaskDecisionAttributes{
			ActivityId: common.StringPtr(activityID),
		},
	}}

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()

	s.mockMetadataMgr.On("GetDomain", mock.Anything).Return(
		&persistence.GetDomainResponse{
			Info:   &persistence.DomainInfo{ID: domainID},
			Config: &persistence.DomainConfig{Retention: 1},
			ReplicationConfig: &persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				},
			},
			TableVersion: persistence.DomainTableVersionV1,
		},
		nil,
	)
	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &history.RespondDecisionTaskCompletedRequest{
		DomainUUID: common.StringPtr(domainID),
		CompleteRequest: &workflow.RespondDecisionTaskCompletedRequest{
			TaskToken:        taskToken,
			Decisions:        decisions,
			ExecutionContext: []byte("context"),
			Identity:         &identity,
		},
	})
	s.Nil(err)
	executionBuilder := s.getBuilder(domainID, we)
	s.Equal(int64(7), executionBuilder.GetExecutionInfo().NextEventID)
	s.Equal(int64(3), executionBuilder.GetExecutionInfo().LastProcessedEvent)
	s.Equal(persistence.WorkflowStateRunning, executionBuilder.GetExecutionInfo().State)
	s.False(executionBuilder.HasPendingDecisionTask())
}

func (s *engineSuite) TestRequestCancel_RespondDecisionTaskCompleted_Scheduled() {
	domainID := validDomainID
	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(validRunID),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: *we.WorkflowId,
		RunID:      *we.RunId,
		ScheduleID: 6,
	})
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := []byte("input1")

	msBuilder := newMutableStateBuilderWithEventV2(s.mockClusterMetadata.GetCurrentClusterName(), s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 100, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent := addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)
	decisionCompletedEvent := addDecisionTaskCompletedEvent(msBuilder, di.ScheduleID,
		*decisionStartedEvent.EventId, nil, identity)
	addActivityTaskScheduledEvent(msBuilder, *decisionCompletedEvent.EventId, activityID,
		activityType, tl, activityInput, 100, 10, 1)
	di2 := addDecisionTaskScheduledEvent(msBuilder)
	addDecisionTaskStartedEvent(msBuilder, di2.ScheduleID, tl, identity)

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	decisions := []*workflow.Decision{{
		DecisionType: common.DecisionTypePtr(workflow.DecisionTypeRequestCancelActivityTask),
		RequestCancelActivityTaskDecisionAttributes: &workflow.RequestCancelActivityTaskDecisionAttributes{
			ActivityId: common.StringPtr(activityID),
		},
	}}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()

	s.mockMetadataMgr.On("GetDomain", mock.Anything).Return(
		&persistence.GetDomainResponse{
			Info:   &persistence.DomainInfo{ID: domainID},
			Config: &persistence.DomainConfig{Retention: 1},
			ReplicationConfig: &persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				},
			},
			TableVersion: persistence.DomainTableVersionV1,
		},
		nil,
	)
	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &history.RespondDecisionTaskCompletedRequest{
		DomainUUID: common.StringPtr(domainID),
		CompleteRequest: &workflow.RespondDecisionTaskCompletedRequest{
			TaskToken:        taskToken,
			Decisions:        decisions,
			ExecutionContext: []byte("context"),
			Identity:         &identity,
		},
	})
	s.Nil(err)

	executionBuilder := s.getBuilder(domainID, we)
	s.Equal(int64(12), executionBuilder.GetExecutionInfo().NextEventID)
	s.Equal(int64(7), executionBuilder.GetExecutionInfo().LastProcessedEvent)
	s.Equal(persistence.WorkflowStateRunning, executionBuilder.GetExecutionInfo().State)
	s.True(executionBuilder.HasPendingDecisionTask())
	di2, ok := executionBuilder.GetPendingDecision(executionBuilder.GetExecutionInfo().NextEventID - 1)
	s.True(ok)
	s.Equal(executionBuilder.GetExecutionInfo().NextEventID-1, di2.ScheduleID)
	s.Equal(int64(0), di2.Attempt)
}

func (s *engineSuite) TestRequestCancel_RespondDecisionTaskCompleted_Started() {
	domainID := validDomainID
	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(validRunID),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: *we.WorkflowId,
		RunID:      *we.RunId,
		ScheduleID: 7,
	})
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := []byte("input1")

	msBuilder := newMutableStateBuilderWithEventV2(s.mockClusterMetadata.GetCurrentClusterName(), s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 100, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent := addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)
	decisionCompletedEvent := addDecisionTaskCompletedEvent(msBuilder, di.ScheduleID,
		*decisionStartedEvent.EventId, nil, identity)
	activityScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, *decisionCompletedEvent.EventId, activityID,
		activityType, tl, activityInput, 100, 10, 0)
	addActivityTaskStartedEvent(msBuilder, *activityScheduledEvent.EventId, tl, identity)
	di2 := addDecisionTaskScheduledEvent(msBuilder)
	addDecisionTaskStartedEvent(msBuilder, di2.ScheduleID, tl, identity)

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	decisions := []*workflow.Decision{{
		DecisionType: common.DecisionTypePtr(workflow.DecisionTypeRequestCancelActivityTask),
		RequestCancelActivityTaskDecisionAttributes: &workflow.RequestCancelActivityTaskDecisionAttributes{
			ActivityId: common.StringPtr(activityID),
		},
	}}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()

	s.mockMetadataMgr.On("GetDomain", mock.Anything).Return(
		&persistence.GetDomainResponse{
			Info:   &persistence.DomainInfo{ID: domainID},
			Config: &persistence.DomainConfig{Retention: 1},
			ReplicationConfig: &persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				},
			},
			TableVersion: persistence.DomainTableVersionV1,
		},
		nil,
	)
	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &history.RespondDecisionTaskCompletedRequest{
		DomainUUID: common.StringPtr(domainID),
		CompleteRequest: &workflow.RespondDecisionTaskCompletedRequest{
			TaskToken:        taskToken,
			Decisions:        decisions,
			ExecutionContext: []byte("context"),
			Identity:         &identity,
		},
	})
	s.Nil(err)

	executionBuilder := s.getBuilder(domainID, we)
	s.Equal(int64(11), executionBuilder.GetExecutionInfo().NextEventID)
	s.Equal(int64(8), executionBuilder.GetExecutionInfo().LastProcessedEvent)
	s.Equal(persistence.WorkflowStateRunning, executionBuilder.GetExecutionInfo().State)
	s.False(executionBuilder.HasPendingDecisionTask())
}

func (s *engineSuite) TestRequestCancel_RespondDecisionTaskCompleted_Completed() {
	domainID := validDomainID
	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(validRunID),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: *we.WorkflowId,
		RunID:      *we.RunId,
		ScheduleID: 6,
	})
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := []byte("input1")
	workflowResult := []byte("workflow result")

	msBuilder := newMutableStateBuilderWithEventV2(s.mockClusterMetadata.GetCurrentClusterName(), s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 100, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent := addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)
	decisionCompletedEvent := addDecisionTaskCompletedEvent(msBuilder, di.ScheduleID,
		*decisionStartedEvent.EventId, nil, identity)
	addActivityTaskScheduledEvent(msBuilder, *decisionCompletedEvent.EventId, activityID,
		activityType, tl, activityInput, 100, 10, 0)
	di2 := addDecisionTaskScheduledEvent(msBuilder)
	addDecisionTaskStartedEvent(msBuilder, di2.ScheduleID, tl, identity)

	decisions := []*workflow.Decision{
		&workflow.Decision{
			DecisionType: common.DecisionTypePtr(workflow.DecisionTypeRequestCancelActivityTask),
			RequestCancelActivityTaskDecisionAttributes: &workflow.RequestCancelActivityTaskDecisionAttributes{
				ActivityId: common.StringPtr(activityID),
			},
		},
		&workflow.Decision{
			DecisionType: common.DecisionTypePtr(workflow.DecisionTypeCompleteWorkflowExecution),
			CompleteWorkflowExecutionDecisionAttributes: &workflow.CompleteWorkflowExecutionDecisionAttributes{
				Result: workflowResult,
			},
		},
	}

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()

	s.mockMetadataMgr.On("GetDomain", mock.Anything).Return(
		&persistence.GetDomainResponse{
			Info:   &persistence.DomainInfo{ID: domainID},
			Config: &persistence.DomainConfig{Retention: 1},
			ReplicationConfig: &persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				},
			},
			TableVersion: persistence.DomainTableVersionV1,
		},
		nil,
	)
	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &history.RespondDecisionTaskCompletedRequest{
		DomainUUID: common.StringPtr(domainID),
		CompleteRequest: &workflow.RespondDecisionTaskCompletedRequest{
			TaskToken:        taskToken,
			Decisions:        decisions,
			ExecutionContext: []byte("context"),
			Identity:         &identity,
		},
	})
	s.Nil(err)

	executionBuilder := s.getBuilder(domainID, we)
	s.Equal(int64(11), executionBuilder.GetExecutionInfo().NextEventID)
	s.Equal(int64(7), executionBuilder.GetExecutionInfo().LastProcessedEvent)
	s.Equal(persistence.WorkflowStateCompleted, executionBuilder.GetExecutionInfo().State)
	s.False(executionBuilder.HasPendingDecisionTask())
}

func (s *engineSuite) TestRequestCancel_RespondDecisionTaskCompleted_NoHeartBeat() {
	domainID := validDomainID
	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(validRunID),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: *we.WorkflowId,
		RunID:      *we.RunId,
		ScheduleID: 7,
	})
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := []byte("input1")

	msBuilder := newMutableStateBuilderWithEventV2(s.mockClusterMetadata.GetCurrentClusterName(), s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 100, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent := addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)
	decisionCompletedEvent := addDecisionTaskCompletedEvent(msBuilder, di.ScheduleID,
		*decisionStartedEvent.EventId, nil, identity)
	activityScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, *decisionCompletedEvent.EventId, activityID,
		activityType, tl, activityInput, 100, 10, 0)
	addActivityTaskStartedEvent(msBuilder, *activityScheduledEvent.EventId, tl, identity)
	di2 := addDecisionTaskScheduledEvent(msBuilder)
	addDecisionTaskStartedEvent(msBuilder, di2.ScheduleID, tl, identity)

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	decisions := []*workflow.Decision{{
		DecisionType: common.DecisionTypePtr(workflow.DecisionTypeRequestCancelActivityTask),
		RequestCancelActivityTaskDecisionAttributes: &workflow.RequestCancelActivityTaskDecisionAttributes{
			ActivityId: common.StringPtr(activityID),
		},
	}}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()

	s.mockMetadataMgr.On("GetDomain", mock.Anything).Return(
		&persistence.GetDomainResponse{
			Info:   &persistence.DomainInfo{ID: domainID},
			Config: &persistence.DomainConfig{Retention: 1},
			ReplicationConfig: &persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				},
			},
			TableVersion: persistence.DomainTableVersionV1,
		},
		nil,
	)
	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &history.RespondDecisionTaskCompletedRequest{
		DomainUUID: common.StringPtr(domainID),
		CompleteRequest: &workflow.RespondDecisionTaskCompletedRequest{
			TaskToken:        taskToken,
			Decisions:        decisions,
			ExecutionContext: []byte("context"),
			Identity:         &identity,
		},
	})
	s.Nil(err)

	executionBuilder := s.getBuilder(domainID, we)
	s.Equal(int64(11), executionBuilder.GetExecutionInfo().NextEventID)
	s.Equal(int64(8), executionBuilder.GetExecutionInfo().LastProcessedEvent)
	s.Equal(persistence.WorkflowStateRunning, executionBuilder.GetExecutionInfo().State)
	s.False(executionBuilder.HasPendingDecisionTask())

	// Try recording activity heartbeat
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()

	activityTaskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: "wId",
		RunID:      we.GetRunId(),
		ScheduleID: 5,
	})

	hbResponse, err := s.mockHistoryEngine.RecordActivityTaskHeartbeat(context.Background(), &history.RecordActivityTaskHeartbeatRequest{
		DomainUUID: common.StringPtr(domainID),
		HeartbeatRequest: &workflow.RecordActivityTaskHeartbeatRequest{
			TaskToken: activityTaskToken,
			Identity:  &identity,
			Details:   []byte("details"),
		},
	})
	s.Nil(err)
	s.NotNil(hbResponse)
	s.True(*hbResponse.CancelRequested)

	// Try cancelling the request.
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()

	s.mockMetadataMgr.On("GetDomain", mock.Anything).Return(
		&persistence.GetDomainResponse{
			Info:   &persistence.DomainInfo{ID: domainID},
			Config: &persistence.DomainConfig{Retention: 1},
			ReplicationConfig: &persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				},
			},
			TableVersion: persistence.DomainTableVersionV1,
		},
		nil,
	)
	err = s.mockHistoryEngine.RespondActivityTaskCanceled(context.Background(), &history.RespondActivityTaskCanceledRequest{
		DomainUUID: common.StringPtr(domainID),
		CancelRequest: &workflow.RespondActivityTaskCanceledRequest{
			TaskToken: activityTaskToken,
			Identity:  &identity,
			Details:   []byte("details"),
		},
	})
	s.Nil(err)

	executionBuilder = s.getBuilder(domainID, we)
	s.Equal(int64(13), executionBuilder.GetExecutionInfo().NextEventID)
	s.Equal(int64(8), executionBuilder.GetExecutionInfo().LastProcessedEvent)
	s.Equal(persistence.WorkflowStateRunning, executionBuilder.GetExecutionInfo().State)
	s.True(executionBuilder.HasPendingDecisionTask())
}

func (s *engineSuite) TestRequestCancel_RespondDecisionTaskCompleted_Success() {
	domainID := validDomainID
	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(validRunID),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: *we.WorkflowId,
		RunID:      *we.RunId,
		ScheduleID: 7,
	})
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := []byte("input1")

	msBuilder := newMutableStateBuilderWithEventV2(s.mockClusterMetadata.GetCurrentClusterName(), s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 100, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent := addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)
	decisionCompletedEvent := addDecisionTaskCompletedEvent(msBuilder, di.ScheduleID,
		*decisionStartedEvent.EventId, nil, identity)
	activityScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, *decisionCompletedEvent.EventId, activityID,
		activityType, tl, activityInput, 100, 10, 1)
	addActivityTaskStartedEvent(msBuilder, *activityScheduledEvent.EventId, tl, identity)
	di2 := addDecisionTaskScheduledEvent(msBuilder)
	addDecisionTaskStartedEvent(msBuilder, di2.ScheduleID, tl, identity)

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	decisions := []*workflow.Decision{{
		DecisionType: common.DecisionTypePtr(workflow.DecisionTypeRequestCancelActivityTask),
		RequestCancelActivityTaskDecisionAttributes: &workflow.RequestCancelActivityTaskDecisionAttributes{
			ActivityId: common.StringPtr(activityID),
		},
	}}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()

	s.mockMetadataMgr.On("GetDomain", mock.Anything).Return(
		&persistence.GetDomainResponse{
			Info:   &persistence.DomainInfo{ID: domainID},
			Config: &persistence.DomainConfig{Retention: 1},
			ReplicationConfig: &persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				},
			},
			TableVersion: persistence.DomainTableVersionV1,
		},
		nil,
	)
	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &history.RespondDecisionTaskCompletedRequest{
		DomainUUID: common.StringPtr(domainID),
		CompleteRequest: &workflow.RespondDecisionTaskCompletedRequest{
			TaskToken:        taskToken,
			Decisions:        decisions,
			ExecutionContext: []byte("context"),
			Identity:         &identity,
		},
	})
	s.Nil(err)

	executionBuilder := s.getBuilder(domainID, we)
	s.Equal(int64(11), executionBuilder.GetExecutionInfo().NextEventID)
	s.Equal(int64(8), executionBuilder.GetExecutionInfo().LastProcessedEvent)
	s.Equal(persistence.WorkflowStateRunning, executionBuilder.GetExecutionInfo().State)
	s.False(executionBuilder.HasPendingDecisionTask())

	// Try recording activity heartbeat
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()

	activityTaskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: "wId",
		RunID:      we.GetRunId(),
		ScheduleID: 5,
	})

	hbResponse, err := s.mockHistoryEngine.RecordActivityTaskHeartbeat(context.Background(), &history.RecordActivityTaskHeartbeatRequest{
		DomainUUID: common.StringPtr(domainID),
		HeartbeatRequest: &workflow.RecordActivityTaskHeartbeatRequest{
			TaskToken: activityTaskToken,
			Identity:  &identity,
			Details:   []byte("details"),
		},
	})
	s.Nil(err)
	s.NotNil(hbResponse)
	s.True(*hbResponse.CancelRequested)

	// Try cancelling the request.
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()

	s.mockMetadataMgr.On("GetDomain", mock.Anything).Return(
		&persistence.GetDomainResponse{
			Info:   &persistence.DomainInfo{ID: domainID},
			Config: &persistence.DomainConfig{Retention: 1},
			ReplicationConfig: &persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				},
			},
			TableVersion: persistence.DomainTableVersionV1,
		},
		nil,
	)
	err = s.mockHistoryEngine.RespondActivityTaskCanceled(context.Background(), &history.RespondActivityTaskCanceledRequest{
		DomainUUID: common.StringPtr(domainID),
		CancelRequest: &workflow.RespondActivityTaskCanceledRequest{
			TaskToken: activityTaskToken,
			Identity:  &identity,
			Details:   []byte("details"),
		},
	})
	s.Nil(err)

	executionBuilder = s.getBuilder(domainID, we)
	s.Equal(int64(13), executionBuilder.GetExecutionInfo().NextEventID)
	s.Equal(int64(8), executionBuilder.GetExecutionInfo().LastProcessedEvent)
	s.Equal(persistence.WorkflowStateRunning, executionBuilder.GetExecutionInfo().State)
	s.True(executionBuilder.HasPendingDecisionTask())
}

func (s *engineSuite) TestStarTimer_DuplicateTimerID() {
	domainID := validDomainID
	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(validRunID),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: *we.WorkflowId,
		RunID:      *we.RunId,
		ScheduleID: 2,
	})
	identity := "testIdentity"
	timerID := "t1"

	msBuilder := newMutableStateBuilderWithEventV2(s.mockClusterMetadata.GetCurrentClusterName(), s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())

	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 100, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	decisions := []*workflow.Decision{{
		DecisionType: common.DecisionTypePtr(workflow.DecisionTypeStartTimer),
		StartTimerDecisionAttributes: &workflow.StartTimerDecisionAttributes{
			TimerId:                   common.StringPtr(timerID),
			StartToFireTimeoutSeconds: common.Int64Ptr(1),
		},
	}}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()

	s.mockMetadataMgr.On("GetDomain", mock.Anything).Return(
		&persistence.GetDomainResponse{
			Info:   &persistence.DomainInfo{ID: domainID},
			Config: &persistence.DomainConfig{Retention: 1},
			ReplicationConfig: &persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				},
			},
			TableVersion: persistence.DomainTableVersionV1,
		},
		nil,
	)
	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &history.RespondDecisionTaskCompletedRequest{
		DomainUUID: common.StringPtr(domainID),
		CompleteRequest: &workflow.RespondDecisionTaskCompletedRequest{
			TaskToken:        taskToken,
			Decisions:        decisions,
			ExecutionContext: []byte("context"),
			Identity:         &identity,
		},
	})
	s.Nil(err)

	executionBuilder := s.getBuilder(domainID, we)
	s.Equal(persistence.WorkflowStateRunning, executionBuilder.GetExecutionInfo().State)

	// Try to add the same timer ID again.
	di2 := addDecisionTaskScheduledEvent(executionBuilder)
	addDecisionTaskStartedEvent(executionBuilder, di2.ScheduleID, tl, identity)
	taskToken2, _ := json.Marshal(&common.TaskToken{
		WorkflowID: *we.WorkflowId,
		RunID:      *we.RunId,
		ScheduleID: di2.ScheduleID,
	})

	ms2 := createMutableState(executionBuilder)
	gwmsResponse2 := &persistence.GetWorkflowExecutionResponse{State: ms2}

	decisionFailedEvent := false
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse2, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Run(func(arguments mock.Arguments) {
		req := arguments.Get(0).(*persistence.AppendHistoryNodesRequest)
		decTaskIndex := len(req.Events) - 1
		if decTaskIndex >= 0 && *req.Events[decTaskIndex].EventType == workflow.EventTypeDecisionTaskFailed {
			decisionFailedEvent = true
		}
	}).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()

	_, err = s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &history.RespondDecisionTaskCompletedRequest{
		DomainUUID: common.StringPtr(domainID),
		CompleteRequest: &workflow.RespondDecisionTaskCompletedRequest{
			TaskToken:        taskToken2,
			Decisions:        decisions,
			ExecutionContext: []byte("context"),
			Identity:         &identity,
		},
	})
	s.Nil(err)

	s.True(decisionFailedEvent)
	executionBuilder = s.getBuilder(domainID, we)
	s.Equal(persistence.WorkflowStateRunning, executionBuilder.GetExecutionInfo().State)
	s.True(executionBuilder.HasPendingDecisionTask())
	di3, ok := executionBuilder.GetPendingDecision(executionBuilder.GetExecutionInfo().NextEventID)
	s.True(ok, "DI.ScheduleID: %v, ScheduleID: %v, StartedID: %v", di2.ScheduleID,
		executionBuilder.GetExecutionInfo().DecisionScheduleID, executionBuilder.GetExecutionInfo().DecisionStartedID)
	s.Equal(executionBuilder.GetExecutionInfo().NextEventID, di3.ScheduleID)
	s.Equal(int64(1), di3.Attempt)
}

func (s *engineSuite) TestUserTimer_RespondDecisionTaskCompleted() {
	domainID := validDomainID
	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(validRunID),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: *we.WorkflowId,
		RunID:      *we.RunId,
		ScheduleID: 6,
	})
	identity := "testIdentity"
	timerID := "t1"

	msBuilder := newMutableStateBuilderWithEventV2(s.mockClusterMetadata.GetCurrentClusterName(), s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	// Verify cancel timer with a start event.
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 100, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent := addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)
	decisionCompletedEvent := addDecisionTaskCompletedEvent(msBuilder, di.ScheduleID,
		*decisionStartedEvent.EventId, nil, identity)
	addTimerStartedEvent(msBuilder, *decisionCompletedEvent.EventId, timerID, 10)
	di2 := addDecisionTaskScheduledEvent(msBuilder)
	addDecisionTaskStartedEvent(msBuilder, di2.ScheduleID, tl, identity)

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	decisions := []*workflow.Decision{{
		DecisionType: common.DecisionTypePtr(workflow.DecisionTypeCancelTimer),
		CancelTimerDecisionAttributes: &workflow.CancelTimerDecisionAttributes{
			TimerId: common.StringPtr(timerID),
		},
	}}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()

	s.mockMetadataMgr.On("GetDomain", mock.Anything).Return(
		&persistence.GetDomainResponse{
			Info:   &persistence.DomainInfo{ID: domainID},
			Config: &persistence.DomainConfig{Retention: 1},
			ReplicationConfig: &persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				},
			},
			TableVersion: persistence.DomainTableVersionV1,
		},
		nil,
	)
	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &history.RespondDecisionTaskCompletedRequest{
		DomainUUID: common.StringPtr(domainID),
		CompleteRequest: &workflow.RespondDecisionTaskCompletedRequest{
			TaskToken:        taskToken,
			Decisions:        decisions,
			ExecutionContext: []byte("context"),
			Identity:         &identity,
		},
	})
	s.Nil(err)

	executionBuilder := s.getBuilder(domainID, we)
	s.Equal(int64(10), executionBuilder.GetExecutionInfo().NextEventID)
	s.Equal(int64(7), executionBuilder.GetExecutionInfo().LastProcessedEvent)
	s.Equal(persistence.WorkflowStateRunning, executionBuilder.GetExecutionInfo().State)
	s.False(executionBuilder.HasPendingDecisionTask())
}

func (s *engineSuite) TestCancelTimer_RespondDecisionTaskCompleted_NoStartTimer() {
	domainID := validDomainID
	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(validRunID),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: *we.WorkflowId,
		RunID:      *we.RunId,
		ScheduleID: 2,
	})
	identity := "testIdentity"
	timerID := "t1"

	msBuilder := newMutableStateBuilderWithEventV2(s.mockClusterMetadata.GetCurrentClusterName(), s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	// Verify cancel timer with a start event.
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 100, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	decisions := []*workflow.Decision{{
		DecisionType: common.DecisionTypePtr(workflow.DecisionTypeCancelTimer),
		CancelTimerDecisionAttributes: &workflow.CancelTimerDecisionAttributes{
			TimerId: common.StringPtr(timerID),
		},
	}}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()

	s.mockMetadataMgr.On("GetDomain", mock.Anything).Return(
		&persistence.GetDomainResponse{
			Info:   &persistence.DomainInfo{ID: domainID},
			Config: &persistence.DomainConfig{Retention: 1},
			ReplicationConfig: &persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				},
			},
			TableVersion: persistence.DomainTableVersionV1,
		},
		nil,
	)
	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &history.RespondDecisionTaskCompletedRequest{
		DomainUUID: common.StringPtr(domainID),
		CompleteRequest: &workflow.RespondDecisionTaskCompletedRequest{
			TaskToken:        taskToken,
			Decisions:        decisions,
			ExecutionContext: []byte("context"),
			Identity:         &identity,
		},
	})
	s.Nil(err)

	executionBuilder := s.getBuilder(domainID, we)
	s.Equal(int64(6), executionBuilder.GetExecutionInfo().NextEventID)
	s.Equal(int64(3), executionBuilder.GetExecutionInfo().LastProcessedEvent)
	s.Equal(persistence.WorkflowStateRunning, executionBuilder.GetExecutionInfo().State)
	s.False(executionBuilder.HasPendingDecisionTask())
}

func (s *engineSuite) TestCancelTimer_RespondDecisionTaskCompleted_TimerFired() {
	domainID := validDomainID
	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(validRunID),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: *we.WorkflowId,
		RunID:      *we.RunId,
		ScheduleID: 6,
	})
	identity := "testIdentity"
	timerID := "t1"

	msBuilder := newMutableStateBuilderWithEventV2(s.mockClusterMetadata.GetCurrentClusterName(), s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	// Verify cancel timer with a start event.
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 100, identity)
	di := addDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent := addDecisionTaskStartedEvent(msBuilder, di.ScheduleID, tl, identity)
	decisionCompletedEvent := addDecisionTaskCompletedEvent(msBuilder, di.ScheduleID,
		*decisionStartedEvent.EventId, nil, identity)
	addTimerStartedEvent(msBuilder, *decisionCompletedEvent.EventId, timerID, 10)
	di2 := addDecisionTaskScheduledEvent(msBuilder)
	addDecisionTaskStartedEvent(msBuilder, di2.ScheduleID, tl, identity)
	addTimerFiredEvent(msBuilder, di2.ScheduleID, timerID)

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	decisions := []*workflow.Decision{{
		DecisionType: common.DecisionTypePtr(workflow.DecisionTypeCancelTimer),
		CancelTimerDecisionAttributes: &workflow.CancelTimerDecisionAttributes{
			TimerId: common.StringPtr(timerID),
		},
	}}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()

	s.mockMetadataMgr.On("GetDomain", mock.Anything).Return(
		&persistence.GetDomainResponse{
			Info:   &persistence.DomainInfo{ID: domainID},
			Config: &persistence.DomainConfig{Retention: 1},
			ReplicationConfig: &persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				},
			},
			TableVersion: persistence.DomainTableVersionV1,
		},
		nil,
	)
	_, err := s.mockHistoryEngine.RespondDecisionTaskCompleted(context.Background(), &history.RespondDecisionTaskCompletedRequest{
		DomainUUID: common.StringPtr(domainID),
		CompleteRequest: &workflow.RespondDecisionTaskCompletedRequest{
			TaskToken:        taskToken,
			Decisions:        decisions,
			ExecutionContext: []byte("context"),
			Identity:         &identity,
		},
	})
	s.Nil(err)

	executionBuilder := s.getBuilder(domainID, we)
	s.Equal(int64(10), executionBuilder.GetExecutionInfo().NextEventID)
	s.Equal(int64(7), executionBuilder.GetExecutionInfo().LastProcessedEvent)
	s.Equal(persistence.WorkflowStateRunning, executionBuilder.GetExecutionInfo().State)
	s.False(executionBuilder.HasPendingDecisionTask())
	s.False(executionBuilder.HasBufferedEvents())
}

func (s *engineSuite) TestSignalWorkflowExecution() {
	signalRequest := &history.SignalWorkflowExecutionRequest{}
	err := s.mockHistoryEngine.SignalWorkflowExecution(context.Background(), signalRequest)
	s.EqualError(err, "BadRequestError{Message: Missing domain UUID.}")

	domainID := validDomainID
	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(validRunID),
	}
	identity := "testIdentity"
	signalName := "my signal name"
	input := []byte("test input")
	signalRequest = &history.SignalWorkflowExecutionRequest{
		DomainUUID: common.StringPtr(domainID),
		SignalRequest: &workflow.SignalWorkflowExecutionRequest{
			Domain:            common.StringPtr(domainID),
			WorkflowExecution: &we,
			Identity:          common.StringPtr(identity),
			SignalName:        common.StringPtr(signalName),
			Input:             input,
		},
	}

	msBuilder := newMutableStateBuilderWithEventV2(s.mockClusterMetadata.GetCurrentClusterName(), s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	ms := createMutableState(msBuilder)
	ms.ExecutionInfo.DomainID = validDomainID
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()

	s.mockMetadataMgr.On("GetDomain", mock.Anything).Return(
		&persistence.GetDomainResponse{
			Info:   &persistence.DomainInfo{ID: domainID},
			Config: &persistence.DomainConfig{Retention: 1},
			ReplicationConfig: &persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				},
			},
			TableVersion: persistence.DomainTableVersionV1,
		},
		nil,
	)
	err = s.mockHistoryEngine.SignalWorkflowExecution(context.Background(), signalRequest)
	s.Nil(err)
}

// Test signal decision by adding request ID
func (s *engineSuite) TestSignalWorkflowExecution_DuplicateRequest() {
	signalRequest := &history.SignalWorkflowExecutionRequest{}
	err := s.mockHistoryEngine.SignalWorkflowExecution(context.Background(), signalRequest)
	s.EqualError(err, "BadRequestError{Message: Missing domain UUID.}")

	domainID := validDomainID
	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId2"),
		RunId:      common.StringPtr(validRunID),
	}
	identity := "testIdentity2"
	signalName := "my signal name 2"
	input := []byte("test input 2")
	requestID := uuid.New()
	signalRequest = &history.SignalWorkflowExecutionRequest{
		DomainUUID: common.StringPtr(domainID),
		SignalRequest: &workflow.SignalWorkflowExecutionRequest{
			Domain:            common.StringPtr(domainID),
			WorkflowExecution: &we,
			Identity:          common.StringPtr(identity),
			SignalName:        common.StringPtr(signalName),
			Input:             input,
			RequestId:         common.StringPtr(requestID),
		},
	}

	msBuilder := newMutableStateBuilderWithEventV2(s.mockClusterMetadata.GetCurrentClusterName(), s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	ms := createMutableState(msBuilder)
	// assume duplicate request id
	ms.SignalRequestedIDs = make(map[string]struct{})
	ms.SignalRequestedIDs[requestID] = struct{}{}
	ms.ExecutionInfo.DomainID = validDomainID
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()

	s.mockMetadataMgr.On("GetDomain", mock.Anything).Return(
		&persistence.GetDomainResponse{
			Info:   &persistence.DomainInfo{ID: domainID},
			Config: &persistence.DomainConfig{Retention: 1},
			ReplicationConfig: &persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				},
			},
			TableVersion: persistence.DomainTableVersionV1,
		},
		nil,
	)
	err = s.mockHistoryEngine.SignalWorkflowExecution(context.Background(), signalRequest)
	s.Nil(err)
}

func (s *engineSuite) TestSignalWorkflowExecution_Failed() {
	signalRequest := &history.SignalWorkflowExecutionRequest{}
	err := s.mockHistoryEngine.SignalWorkflowExecution(context.Background(), signalRequest)
	s.EqualError(err, "BadRequestError{Message: Missing domain UUID.}")

	domainID := validDomainID
	we := &workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(validRunID),
	}
	identity := "testIdentity"
	signalName := "my signal name"
	input := []byte("test input")
	signalRequest = &history.SignalWorkflowExecutionRequest{
		DomainUUID: common.StringPtr(domainID),
		SignalRequest: &workflow.SignalWorkflowExecutionRequest{
			Domain:            common.StringPtr(domainID),
			WorkflowExecution: we,
			Identity:          common.StringPtr(identity),
			SignalName:        common.StringPtr(signalName),
			Input:             input,
		},
	}

	msBuilder := newMutableStateBuilderWithEventV2(s.mockClusterMetadata.GetCurrentClusterName(), s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), we.GetRunId())
	ms := createMutableState(msBuilder)
	ms.ExecutionInfo.State = persistence.WorkflowStateCompleted
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()

	s.mockMetadataMgr.On("GetDomain", mock.Anything).Return(
		&persistence.GetDomainResponse{
			Info:   &persistence.DomainInfo{ID: domainID},
			Config: &persistence.DomainConfig{Retention: 1},
			ReplicationConfig: &persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				},
			},
			TableVersion: persistence.DomainTableVersionV1,
		},
		nil,
	)
	err = s.mockHistoryEngine.SignalWorkflowExecution(context.Background(), signalRequest)
	s.EqualError(err, "EntityNotExistsError{Message: Workflow execution already completed.}")
}

func (s *engineSuite) TestRemoveSignalMutableState() {
	removeRequest := &history.RemoveSignalMutableStateRequest{}
	err := s.mockHistoryEngine.RemoveSignalMutableState(context.Background(), removeRequest)
	s.EqualError(err, "BadRequestError{Message: Missing domain UUID.}")

	domainID := validDomainID
	requestID := uuid.New()
	removeRequest = &history.RemoveSignalMutableStateRequest{
		DomainUUID: common.StringPtr(domainID),
		WorkflowExecution: &workflow.WorkflowExecution{
			WorkflowId: common.StringPtr("wId"),
			RunId:      common.StringPtr(validRunID),
		},
		RequestId: common.StringPtr(requestID),
	}

	msBuilder := newMutableStateBuilderWithEventV2(s.mockClusterMetadata.GetCurrentClusterName(), s.mockHistoryEngine.shard, s.eventsCache,
		loggerimpl.NewDevelopmentForTest(s.Suite), validRunID)
	ms := createMutableState(msBuilder)
	ms.ExecutionInfo.DomainID = validDomainID
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&p.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &p.MutableStateUpdateSessionStats{}}, nil).Once()

	s.mockMetadataMgr.On("GetDomain", mock.Anything).Return(
		&persistence.GetDomainResponse{
			Info:   &persistence.DomainInfo{ID: domainID},
			Config: &persistence.DomainConfig{Retention: 1},
			ReplicationConfig: &persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				},
			},
			TableVersion: persistence.DomainTableVersionV1,
		},
		nil,
	)
	err = s.mockHistoryEngine.RemoveSignalMutableState(context.Background(), removeRequest)
	s.Nil(err)
}

func (s *engineSuite) TestValidateSignalExternalWorkflowExecutionAttributes() {
	var attributes *workflow.SignalExternalWorkflowExecutionDecisionAttributes
	maxIDLengthLimit := 1000
	err := validateSignalExternalWorkflowExecutionAttributes(attributes, maxIDLengthLimit)
	s.EqualError(err, "BadRequestError{Message: SignalExternalWorkflowExecutionDecisionAttributes is not set on decision.}")

	attributes = &workflow.SignalExternalWorkflowExecutionDecisionAttributes{}
	err = validateSignalExternalWorkflowExecutionAttributes(attributes, maxIDLengthLimit)
	s.EqualError(err, "BadRequestError{Message: Execution is nil on decision.}")

	attributes.Execution = &workflow.WorkflowExecution{}
	attributes.Execution.WorkflowId = common.StringPtr("workflow-id")
	err = validateSignalExternalWorkflowExecutionAttributes(attributes, maxIDLengthLimit)
	s.EqualError(err, "BadRequestError{Message: SignalName is not set on decision.}")

	attributes.Execution.RunId = common.StringPtr("run-id")
	err = validateSignalExternalWorkflowExecutionAttributes(attributes, maxIDLengthLimit)
	s.EqualError(err, "BadRequestError{Message: Invalid RunId set on decision.}")
	attributes.Execution.RunId = common.StringPtr(validRunID)

	attributes.SignalName = common.StringPtr("my signal name")
	err = validateSignalExternalWorkflowExecutionAttributes(attributes, maxIDLengthLimit)
	s.EqualError(err, "BadRequestError{Message: Input is not set on decision.}")

	attributes.Input = []byte("test input")
	err = validateSignalExternalWorkflowExecutionAttributes(attributes, maxIDLengthLimit)
	s.Nil(err)
}

func (s *engineSuite) TestGetWorkflowStartedEvent() {
	req := &persistence.ReadHistoryBranchRequest{
		BranchToken:   []byte{},
		MinEventID:    common.FirstEventID,
		MaxEventID:    common.FirstEventID + 1,
		PageSize:      defaultHistoryPageSize,
		NextPageToken: nil,
		ShardID:       common.IntPtr(0),
	}
	events := []*workflow.HistoryEvent{
		{EventId: common.Int64Ptr(int64(0))},
	}
	s.mockHistoryV2Mgr.On("ReadHistoryBranch", req).Return(&persistence.ReadHistoryBranchResponse{HistoryEvents: events}, nil)
	event, err := getWorkflowStartedEvent(s.mockHistoryMgr, s.mockHistoryV2Mgr, p.EventStoreVersionV2, []byte{}, s.logger, "", "", "", common.IntPtr(0))
	s.NoError(err)
	s.NotNil(event)
}

func (s *engineSuite) getBuilder(domainID string, we workflow.WorkflowExecution) mutableState {
	context, release, err := s.mockHistoryEngine.historyCache.getOrCreateWorkflowExecution(domainID, we)
	if err != nil {
		return nil
	}
	defer release(nil)

	return context.(*workflowExecutionContextImpl).msBuilder
}

func (s *engineSuite) getActivityScheduledEvent(msBuilder mutableState,
	scheduleID int64) *workflow.HistoryEvent {
	event, _ := msBuilder.GetActivityScheduledEvent(scheduleID)
	return event
}

func (s *engineSuite) printHistory(builder mutableState) string {
	return builder.GetHistoryBuilder().GetHistory().String()
}

func addWorkflowExecutionStartedEventWithParent(builder mutableState, workflowExecution workflow.WorkflowExecution,
	workflowType, taskList string, input []byte, executionStartToCloseTimeout, taskStartToCloseTimeout int32,
	parentInfo *history.ParentExecutionInfo, identity string) *workflow.HistoryEvent {
	domainID := validDomainID
	startRequest := &workflow.StartWorkflowExecutionRequest{
		WorkflowId:                          common.StringPtr(*workflowExecution.WorkflowId),
		WorkflowType:                        &workflow.WorkflowType{Name: common.StringPtr(workflowType)},
		TaskList:                            &workflow.TaskList{Name: common.StringPtr(taskList)},
		Input:                               input,
		ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(executionStartToCloseTimeout),
		TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(taskStartToCloseTimeout),
		Identity:                            common.StringPtr(identity),
	}

	e := builder.AddWorkflowExecutionStartedEvent(workflowExecution, &history.StartWorkflowExecutionRequest{
		DomainUUID:          common.StringPtr(domainID),
		StartRequest:        startRequest,
		ParentExecutionInfo: parentInfo,
	})

	return e
}

func addWorkflowExecutionStartedEvent(builder mutableState, workflowExecution workflow.WorkflowExecution,
	workflowType, taskList string, input []byte, executionStartToCloseTimeout, taskStartToCloseTimeout int32,
	identity string) *workflow.HistoryEvent {
	return addWorkflowExecutionStartedEventWithParent(builder, workflowExecution, workflowType, taskList, input,
		executionStartToCloseTimeout, taskStartToCloseTimeout, nil, identity)
}

func addDecisionTaskScheduledEvent(builder mutableState) *decisionInfo {
	return builder.AddDecisionTaskScheduledEvent()
}

func addDecisionTaskStartedEvent(builder mutableState, scheduleID int64, taskList,
	identity string) *workflow.HistoryEvent {
	return addDecisionTaskStartedEventWithRequestID(builder, scheduleID, validRunID, taskList, identity)
}

func addDecisionTaskStartedEventWithRequestID(builder mutableState, scheduleID int64, requestID string,
	taskList, identity string) *workflow.HistoryEvent {
	e, _ := builder.AddDecisionTaskStartedEvent(scheduleID, requestID, &workflow.PollForDecisionTaskRequest{
		TaskList: &workflow.TaskList{Name: common.StringPtr(taskList)},
		Identity: common.StringPtr(identity),
	})

	return e
}

func addDecisionTaskCompletedEvent(builder mutableState, scheduleID, startedID int64, context []byte,
	identity string) *workflow.HistoryEvent {
	e := builder.AddDecisionTaskCompletedEvent(scheduleID, startedID, &workflow.RespondDecisionTaskCompletedRequest{
		ExecutionContext: context,
		Identity:         common.StringPtr(identity),
	})

	builder.FlushBufferedEvents()

	return e
}

func addActivityTaskScheduledEvent(builder mutableState, decisionCompletedID int64, activityID, activityType,
	taskList string, input []byte, timeout, queueTimeout, heartbeatTimeout int32) (*workflow.HistoryEvent,
	*persistence.ActivityInfo) {

	return builder.AddActivityTaskScheduledEvent(decisionCompletedID, &workflow.ScheduleActivityTaskDecisionAttributes{
		ActivityId:                    common.StringPtr(activityID),
		ActivityType:                  &workflow.ActivityType{Name: common.StringPtr(activityType)},
		TaskList:                      &workflow.TaskList{Name: common.StringPtr(taskList)},
		Input:                         input,
		ScheduleToCloseTimeoutSeconds: common.Int32Ptr(timeout),
		ScheduleToStartTimeoutSeconds: common.Int32Ptr(queueTimeout),
		HeartbeatTimeoutSeconds:       common.Int32Ptr(heartbeatTimeout),
		StartToCloseTimeoutSeconds:    common.Int32Ptr(1),
	})
}

func addActivityTaskStartedEvent(builder mutableState, scheduleID int64,
	taskList, identity string) *workflow.HistoryEvent {
	ai, _ := builder.GetActivityInfo(scheduleID)
	return builder.AddActivityTaskStartedEvent(ai, scheduleID, validRunID, identity)
}

func addActivityTaskCompletedEvent(builder mutableState, scheduleID, startedID int64, result []byte,
	identity string) *workflow.HistoryEvent {
	e := builder.AddActivityTaskCompletedEvent(scheduleID, startedID, &workflow.RespondActivityTaskCompletedRequest{
		Result:   result,
		Identity: common.StringPtr(identity),
	})

	return e
}

func addActivityTaskFailedEvent(builder mutableState, scheduleID, startedID int64, reason string, details []byte,
	identity string) *workflow.HistoryEvent {
	e := builder.AddActivityTaskFailedEvent(scheduleID, startedID, &workflow.RespondActivityTaskFailedRequest{
		Reason:   common.StringPtr(reason),
		Details:  details,
		Identity: common.StringPtr(identity),
	})

	return e
}

func addTimerStartedEvent(builder mutableState, decisionCompletedEventID int64, timerID string,
	timeOut int64) (*workflow.HistoryEvent, *persistence.TimerInfo) {
	return builder.AddTimerStartedEvent(decisionCompletedEventID,
		&workflow.StartTimerDecisionAttributes{
			TimerId:                   common.StringPtr(timerID),
			StartToFireTimeoutSeconds: common.Int64Ptr(timeOut),
		})
}

func addTimerFiredEvent(builder mutableState, scheduleID int64, timerID string) *workflow.HistoryEvent {
	return builder.AddTimerFiredEvent(scheduleID, timerID)
}

func addRequestCancelInitiatedEvent(builder mutableState, decisionCompletedEventID int64,
	cancelRequestID, domain, workflowID, runID string) (*workflow.HistoryEvent, *persistence.RequestCancelInfo) {
	event, rci := builder.AddRequestCancelExternalWorkflowExecutionInitiatedEvent(decisionCompletedEventID,
		cancelRequestID, &workflow.RequestCancelExternalWorkflowExecutionDecisionAttributes{
			Domain:     common.StringPtr(domain),
			WorkflowId: common.StringPtr(workflowID),
			RunId:      common.StringPtr(runID),
		})

	return event, rci
}

func addCancelRequestedEvent(builder mutableState, initiatedID int64, domain, workflowID, runID string) *workflow.HistoryEvent {
	event := builder.AddExternalWorkflowExecutionCancelRequested(initiatedID, domain, workflowID, runID)
	return event
}

func addRequestSignalInitiatedEvent(builder mutableState, decisionCompletedEventID int64,
	signalRequestID, domain, workflowID, runID, signalName string, input, control []byte) (*workflow.HistoryEvent, *persistence.SignalInfo) {
	event, si := builder.AddSignalExternalWorkflowExecutionInitiatedEvent(decisionCompletedEventID, signalRequestID,
		&workflow.SignalExternalWorkflowExecutionDecisionAttributes{
			Domain: common.StringPtr(domain),
			Execution: &workflow.WorkflowExecution{
				WorkflowId: common.StringPtr(workflowID),
				RunId:      common.StringPtr(runID),
			},
			SignalName: common.StringPtr(signalName),
			Input:      input,
			Control:    control,
		})

	return event, si
}

func addSignaledEvent(builder mutableState, initiatedID int64, domain, workflowID, runID string, control []byte) *workflow.HistoryEvent {
	event := builder.AddExternalWorkflowExecutionSignaled(initiatedID, domain, workflowID, runID, control)
	return event
}

func addStartChildWorkflowExecutionInitiatedEvent(builder mutableState, decisionCompletedID int64,
	createRequestID, domain, workflowID, workflowType, tasklist string, input []byte,
	executionStartToCloseTimeout, taskStartToCloseTimeout int32) (*workflow.HistoryEvent,
	*persistence.ChildExecutionInfo) {
	return builder.AddStartChildWorkflowExecutionInitiatedEvent(decisionCompletedID, createRequestID,
		&workflow.StartChildWorkflowExecutionDecisionAttributes{
			Domain:                              common.StringPtr(domain),
			WorkflowId:                          common.StringPtr(workflowID),
			WorkflowType:                        &workflow.WorkflowType{Name: common.StringPtr(workflowType)},
			TaskList:                            &workflow.TaskList{Name: common.StringPtr(tasklist)},
			Input:                               input,
			ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(executionStartToCloseTimeout),
			TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(taskStartToCloseTimeout),
			ChildPolicy:                         common.ChildPolicyPtr(workflow.ChildPolicyTerminate),
			Control:                             nil,
		})
}

func addChildWorkflowExecutionStartedEvent(builder mutableState, initiatedID int64, domain, workflowID, runID string,
	workflowType string) *workflow.HistoryEvent {
	event := builder.AddChildWorkflowExecutionStartedEvent(
		common.StringPtr(domain),
		&workflow.WorkflowExecution{
			WorkflowId: common.StringPtr(workflowID),
			RunId:      common.StringPtr(runID),
		},
		&workflow.WorkflowType{Name: common.StringPtr(workflowType)},
		initiatedID,
	)
	return event
}

func addChildWorkflowExecutionCompletedEvent(builder mutableState, initiatedID int64, childExecution *workflow.WorkflowExecution,
	attributes *workflow.WorkflowExecutionCompletedEventAttributes) *workflow.HistoryEvent {
	event := builder.AddChildWorkflowExecutionCompletedEvent(initiatedID, childExecution, attributes)
	return event
}

func addCompleteWorkflowEvent(builder mutableState, decisionCompletedEventID int64,
	result []byte) *workflow.HistoryEvent {
	e := builder.AddCompletedWorkflowEvent(decisionCompletedEventID, &workflow.CompleteWorkflowExecutionDecisionAttributes{
		Result: result,
	})

	return e
}

func newMutableStateBuilderWithEventV2(currentCluster string, shard ShardContext, eventsCache eventsCache,
	logger log.Logger, runID string) *mutableStateBuilder {

	msBuilder := newMutableStateBuilder(currentCluster, shard, eventsCache, logger)
	_ = msBuilder.SetHistoryTree(runID)

	return msBuilder
}

func newMutableStateBuilderWithReplicationStateWithEventV2(currentCluster string, shard ShardContext, eventsCache eventsCache,
	logger log.Logger, version int64, runID string) *mutableStateBuilder {

	msBuilder := newMutableStateBuilderWithReplicationState(currentCluster, shard, eventsCache, logger, version)
	_ = msBuilder.SetHistoryTree(runID)

	return msBuilder
}

func createMutableState(ms mutableState) *persistence.WorkflowMutableState {
	builder := ms.(*mutableStateBuilder)
	builder.FlushBufferedEvents()
	info := copyWorkflowExecutionInfo(builder.executionInfo)
	activityInfos := make(map[int64]*persistence.ActivityInfo)
	for id, info := range builder.pendingActivityInfoIDs {
		activityInfos[id] = copyActivityInfo(info)
	}
	timerInfos := make(map[string]*persistence.TimerInfo)
	for id, info := range builder.pendingTimerInfoIDs {
		timerInfos[id] = copyTimerInfo(info)
	}
	cancellationInfos := make(map[int64]*persistence.RequestCancelInfo)
	for id, info := range builder.pendingRequestCancelInfoIDs {
		cancellationInfos[id] = copyCancellationInfo(info)
	}
	signalInfos := make(map[int64]*persistence.SignalInfo)
	for id, info := range builder.pendingSignalInfoIDs {
		signalInfos[id] = copySignalInfo(info)
	}
	childInfos := make(map[int64]*persistence.ChildExecutionInfo)
	for id, info := range builder.pendingChildExecutionInfoIDs {
		childInfos[id] = copyChildInfo(info)
	}

	builder.FlushBufferedEvents()
	var bufferedEvents []*workflow.HistoryEvent
	if len(builder.bufferedEvents) > 0 {
		bufferedEvents = append(bufferedEvents, builder.bufferedEvents...)
	}
	if len(builder.updateBufferedEvents) > 0 {
		bufferedEvents = append(bufferedEvents, builder.updateBufferedEvents...)
	}
	var replicationState *persistence.ReplicationState
	if builder.replicationState != nil {
		replicationState = copyReplicationState(builder.replicationState)
	}

	return &persistence.WorkflowMutableState{
		ExecutionInfo:       info,
		ActivityInfos:       activityInfos,
		TimerInfos:          timerInfos,
		BufferedEvents:      bufferedEvents,
		SignalInfos:         signalInfos,
		RequestCancelInfos:  cancellationInfos,
		ChildExecutionInfos: childInfos,
		ReplicationState:    replicationState,
	}
}

func copyWorkflowExecutionInfo(sourceInfo *persistence.WorkflowExecutionInfo) *persistence.WorkflowExecutionInfo {
	return &persistence.WorkflowExecutionInfo{
		DomainID:                     sourceInfo.DomainID,
		WorkflowID:                   sourceInfo.WorkflowID,
		RunID:                        sourceInfo.RunID,
		ParentDomainID:               sourceInfo.ParentDomainID,
		ParentWorkflowID:             sourceInfo.ParentWorkflowID,
		ParentRunID:                  sourceInfo.ParentRunID,
		InitiatedID:                  sourceInfo.InitiatedID,
		CompletionEventBatchID:       sourceInfo.CompletionEventBatchID,
		CompletionEvent:              sourceInfo.CompletionEvent,
		TaskList:                     sourceInfo.TaskList,
		StickyTaskList:               sourceInfo.StickyTaskList,
		StickyScheduleToStartTimeout: sourceInfo.StickyScheduleToStartTimeout,
		WorkflowTypeName:             sourceInfo.WorkflowTypeName,
		WorkflowTimeout:              sourceInfo.WorkflowTimeout,
		DecisionTimeoutValue:         sourceInfo.DecisionTimeoutValue,
		ExecutionContext:             sourceInfo.ExecutionContext,
		State:                        sourceInfo.State,
		CloseStatus:                  sourceInfo.CloseStatus,
		LastEventTaskID:              sourceInfo.LastEventTaskID,
		NextEventID:                  sourceInfo.NextEventID,
		LastProcessedEvent:           sourceInfo.LastProcessedEvent,
		LastUpdatedTimestamp:         sourceInfo.LastUpdatedTimestamp,
		CreateRequestID:              sourceInfo.CreateRequestID,
		DecisionVersion:              sourceInfo.DecisionVersion,
		DecisionScheduleID:           sourceInfo.DecisionScheduleID,
		DecisionStartedID:            sourceInfo.DecisionStartedID,
		DecisionRequestID:            sourceInfo.DecisionRequestID,
		DecisionTimeout:              sourceInfo.DecisionTimeout,
		EventStoreVersion:            sourceInfo.EventStoreVersion,
		BranchToken:                  sourceInfo.BranchToken,
		HasRetryPolicy:               sourceInfo.HasRetryPolicy,
		CronSchedule:                 sourceInfo.CronSchedule,
	}
}

func copyActivityInfo(sourceInfo *persistence.ActivityInfo) *persistence.ActivityInfo {
	return &persistence.ActivityInfo{
		Version:                  sourceInfo.Version,
		ScheduleID:               sourceInfo.ScheduleID,
		ScheduledEventBatchID:    sourceInfo.ScheduledEventBatchID,
		ScheduledEvent:           sourceInfo.ScheduledEvent,
		StartedID:                sourceInfo.StartedID,
		StartedEvent:             sourceInfo.StartedEvent,
		ActivityID:               sourceInfo.ActivityID,
		RequestID:                sourceInfo.RequestID,
		Details:                  sourceInfo.Details,
		ScheduledTime:            sourceInfo.ScheduledTime,
		StartedTime:              sourceInfo.StartedTime,
		ScheduleToStartTimeout:   sourceInfo.ScheduleToStartTimeout,
		ScheduleToCloseTimeout:   sourceInfo.ScheduleToCloseTimeout,
		StartToCloseTimeout:      sourceInfo.StartToCloseTimeout,
		HeartbeatTimeout:         sourceInfo.HeartbeatTimeout,
		LastHeartBeatUpdatedTime: sourceInfo.LastHeartBeatUpdatedTime,
		CancelRequested:          sourceInfo.CancelRequested,
		CancelRequestID:          sourceInfo.CancelRequestID,
		TimerTaskStatus:          sourceInfo.TimerTaskStatus,
		Attempt:                  sourceInfo.Attempt,
		StartedIdentity:          sourceInfo.StartedIdentity,
		TaskList:                 sourceInfo.TaskList,
		HasRetryPolicy:           sourceInfo.HasRetryPolicy,
		InitialInterval:          sourceInfo.InitialInterval,
		BackoffCoefficient:       sourceInfo.BackoffCoefficient,
		MaximumInterval:          sourceInfo.MaximumInterval,
		ExpirationTime:           sourceInfo.ExpirationTime,
		MaximumAttempts:          sourceInfo.MaximumAttempts,
	}
}

func copyTimerInfo(sourceInfo *persistence.TimerInfo) *persistence.TimerInfo {
	return &persistence.TimerInfo{
		Version:    sourceInfo.Version,
		TimerID:    sourceInfo.TimerID,
		StartedID:  sourceInfo.StartedID,
		ExpiryTime: sourceInfo.ExpiryTime,
		TaskID:     sourceInfo.TaskID,
	}
}

func copyCancellationInfo(sourceInfo *persistence.RequestCancelInfo) *persistence.RequestCancelInfo {
	return &persistence.RequestCancelInfo{
		Version:         sourceInfo.Version,
		InitiatedID:     sourceInfo.InitiatedID,
		CancelRequestID: sourceInfo.CancelRequestID,
	}
}

func copySignalInfo(sourceInfo *persistence.SignalInfo) *persistence.SignalInfo {
	result := &persistence.SignalInfo{
		Version:         sourceInfo.Version,
		InitiatedID:     sourceInfo.InitiatedID,
		SignalRequestID: sourceInfo.SignalRequestID,
		SignalName:      sourceInfo.SignalName,
	}
	result.Input = make([]byte, len(sourceInfo.Input))
	copy(result.Input, sourceInfo.Input)
	result.Control = make([]byte, len(sourceInfo.Control))
	copy(result.Control, sourceInfo.Control)
	return result
}

func copyChildInfo(sourceInfo *persistence.ChildExecutionInfo) *persistence.ChildExecutionInfo {
	result := &persistence.ChildExecutionInfo{
		Version:               sourceInfo.Version,
		InitiatedID:           sourceInfo.InitiatedID,
		InitiatedEventBatchID: sourceInfo.InitiatedEventBatchID,
		StartedID:             sourceInfo.StartedID,
		StartedWorkflowID:     sourceInfo.StartedWorkflowID,
		StartedRunID:          sourceInfo.StartedRunID,
		CreateRequestID:       sourceInfo.CreateRequestID,
		DomainName:            sourceInfo.DomainName,
		WorkflowTypeName:      sourceInfo.WorkflowTypeName,
	}

	if sourceInfo.InitiatedEvent != nil {
		result.InitiatedEvent = &workflow.HistoryEvent{}
		wv, err := sourceInfo.InitiatedEvent.ToWire()
		if err != nil {
			panic(err)
		}
		err = result.InitiatedEvent.FromWire(wv)
		if err != nil {
			panic(err)
		}
	}
	if sourceInfo.StartedEvent != nil {
		result.StartedEvent = &workflow.HistoryEvent{}
		wv, err := sourceInfo.StartedEvent.ToWire()
		if err != nil {
			panic(err)
		}
		err = result.StartedEvent.FromWire(wv)
		if err != nil {
			panic(err)
		}
	}
	return result
}

func copyReplicationState(source *persistence.ReplicationState) *persistence.ReplicationState {
	return &persistence.ReplicationState{
		CurrentVersion:      source.CurrentVersion,
		StartVersion:        source.StartVersion,
		LastWriteVersion:    source.LastWriteVersion,
		LastWriteEventID:    source.LastWriteEventID,
		LastReplicationInfo: source.LastReplicationInfo,
	}
}
