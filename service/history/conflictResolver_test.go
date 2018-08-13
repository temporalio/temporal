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
	"os"
	"testing"
	"time"

	"github.com/pborman/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"github.com/uber-common/bark"
	"github.com/uber-go/tally"
	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/messaging"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/mocks"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/service"
	"github.com/uber/cadence/common/service/dynamicconfig"
)

type (
	conflictResolverSuite struct {
		suite.Suite
		logger              bark.Logger
		mockExecutionMgr    *mocks.ExecutionManager
		mockHistoryMgr      *mocks.HistoryManager
		mockShardManager    *mocks.ShardManager
		mockClusterMetadata *mocks.ClusterMetadata
		mockProducer        *mocks.KafkaProducer
		mockMetadataMgr     *mocks.MetadataManager
		mockMessagingClient messaging.Client
		mockService         service.Service
		mockShard           *shardContextImpl
		mockContext         *workflowExecutionContext

		conflictResolver *conflictResolverImpl
	}
)

func TestConflictResolverSuite(t *testing.T) {
	s := new(conflictResolverSuite)
	suite.Run(t, s)
}

func (s *conflictResolverSuite) SetupSuite() {
	if testing.Verbose() {
		log.SetOutput(os.Stdout)
	}

}

func (s *conflictResolverSuite) TearDownSuite() {

}

func (s *conflictResolverSuite) SetupTest() {
	log2 := log.New()
	log2.Level = log.DebugLevel
	s.logger = bark.NewLoggerFromLogrus(log2)
	s.mockHistoryMgr = &mocks.HistoryManager{}
	s.mockExecutionMgr = &mocks.ExecutionManager{}
	s.mockClusterMetadata = &mocks.ClusterMetadata{}
	s.mockShardManager = &mocks.ShardManager{}
	s.mockProducer = &mocks.KafkaProducer{}
	s.mockMessagingClient = mocks.NewMockMessagingClient(s.mockProducer, nil)
	s.mockMetadataMgr = &mocks.MetadataManager{}
	metricsClient := metrics.NewClient(tally.NoopScope, metrics.History)
	s.mockService = service.NewTestService(s.mockClusterMetadata, s.mockMessagingClient, metricsClient, s.logger)

	s.mockShard = &shardContextImpl{
		service:                   s.mockService,
		shardInfo:                 &persistence.ShardInfo{ShardID: 0, RangeID: 1, TransferAckLevel: 0},
		transferSequenceNumber:    1,
		executionManager:          s.mockExecutionMgr,
		shardManager:              s.mockShardManager,
		historyMgr:                s.mockHistoryMgr,
		maxTransferSequenceNumber: 100000,
		closeCh:                   make(chan int, 100),
		config:                    NewConfig(dynamicconfig.NewNopCollection(), 1),
		logger:                    s.logger,
		domainCache:               cache.NewDomainCache(s.mockMetadataMgr, s.mockClusterMetadata, metricsClient, s.logger),
		metricsClient:             metrics.NewClient(tally.NoopScope, metrics.History),
	}
	s.mockContext = newWorkflowExecutionContext(validDomainID, shared.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(validRunID),
	}, s.mockShard, s.mockExecutionMgr, s.logger)
	s.mockClusterMetadata.On("GetCurrentClusterName").Return(cluster.TestCurrentClusterName)
	s.conflictResolver = newConflictResolver(s.mockShard, s.mockContext, s.mockHistoryMgr, s.logger)
}

func (s *conflictResolverSuite) TearDownTest() {
}

func (s *conflictResolverSuite) TestGetHistory() {
	domainID := s.mockContext.domainID
	execution := s.mockContext.workflowExecution
	nextEventID := int64(101)

	event1 := &shared.HistoryEvent{
		EventId: common.Int64Ptr(1),
		WorkflowExecutionStartedEventAttributes: &shared.WorkflowExecutionStartedEventAttributes{},
	}
	event2 := &shared.HistoryEvent{
		EventId: common.Int64Ptr(2),
		DecisionTaskScheduledEventAttributes: &shared.DecisionTaskScheduledEventAttributes{},
	}
	event3 := &shared.HistoryEvent{
		EventId: common.Int64Ptr(3),
		DecisionTaskStartedEventAttributes: &shared.DecisionTaskStartedEventAttributes{},
	}
	event4 := &shared.HistoryEvent{
		EventId: common.Int64Ptr(4),
		DecisionTaskCompletedEventAttributes: &shared.DecisionTaskCompletedEventAttributes{},
	}
	event5 := &shared.HistoryEvent{
		EventId: common.Int64Ptr(5),
		ActivityTaskScheduledEventAttributes: &shared.ActivityTaskScheduledEventAttributes{},
	}

	pageToken := []byte("some random token")
	s.mockHistoryMgr.On("GetWorkflowExecutionHistory", &persistence.GetWorkflowExecutionHistoryRequest{
		DomainID:      domainID,
		Execution:     execution,
		FirstEventID:  common.FirstEventID,
		NextEventID:   nextEventID,
		PageSize:      defaultHistoryPageSize,
		NextPageToken: nil,
	}).Return(&persistence.GetWorkflowExecutionHistoryResponse{
		History:          &shared.History{Events: []*shared.HistoryEvent{event1, event2}},
		NextPageToken:    pageToken,
		LastFirstEventID: event1.GetEventId(),
	}, nil)
	history, token, firstEventID, err := s.conflictResolver.getHistory(domainID, execution, common.FirstEventID, nextEventID, nil)
	s.Nil(err)
	s.Equal(history.Events, []*shared.HistoryEvent{event1, event2})
	s.Equal(pageToken, token)
	s.Equal(firstEventID, event1.GetEventId())

	s.mockHistoryMgr.On("GetWorkflowExecutionHistory", &persistence.GetWorkflowExecutionHistoryRequest{
		DomainID:      domainID,
		Execution:     execution,
		FirstEventID:  common.FirstEventID,
		NextEventID:   nextEventID,
		PageSize:      defaultHistoryPageSize,
		NextPageToken: pageToken,
	}).Return(&persistence.GetWorkflowExecutionHistoryResponse{
		History:          &shared.History{Events: []*shared.HistoryEvent{event3, event4, event5}},
		NextPageToken:    nil,
		LastFirstEventID: event4.GetEventId(),
	}, nil)
	history, token, firstEventID, err = s.conflictResolver.getHistory(domainID, execution, common.FirstEventID, nextEventID, token)
	s.Nil(err)
	s.Equal(history.Events, []*shared.HistoryEvent{event3, event4, event5})
	s.Empty(token)
	s.Equal(firstEventID, event4.GetEventId())
}

func (s *conflictResolverSuite) TestReset() {
	sourceCluster := "some random source cluster"
	startTime := time.Now()
	domainID := s.mockContext.domainID
	execution := s.mockContext.workflowExecution
	nextEventID := int64(2)

	event1 := &shared.HistoryEvent{
		EventId: common.Int64Ptr(1),
		Version: common.Int64Ptr(12),
		WorkflowExecutionStartedEventAttributes: &shared.WorkflowExecutionStartedEventAttributes{
			WorkflowType: &shared.WorkflowType{Name: common.StringPtr("some random workflow type")},
			TaskList:     &shared.TaskList{Name: common.StringPtr("some random workflow type")},
			Input:        []byte("some random input"),
			ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(123),
			TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(233),
			Identity:                            common.StringPtr("some random identity"),
		},
	}
	event2 := &shared.HistoryEvent{
		EventId: common.Int64Ptr(2),
		DecisionTaskScheduledEventAttributes: &shared.DecisionTaskScheduledEventAttributes{},
	}

	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", event1.GetVersion()).Return(sourceCluster).Once()
	s.mockHistoryMgr.On("GetWorkflowExecutionHistory", &persistence.GetWorkflowExecutionHistoryRequest{
		DomainID:      domainID,
		Execution:     execution,
		FirstEventID:  common.FirstEventID,
		NextEventID:   nextEventID,
		PageSize:      defaultHistoryPageSize,
		NextPageToken: nil,
	}).Return(&persistence.GetWorkflowExecutionHistoryResponse{
		History:          &shared.History{Events: []*shared.HistoryEvent{event1, event2}},
		NextPageToken:    nil,
		LastFirstEventID: event1.GetEventId(),
	}, nil)

	s.mockContext.updateCondition = int64(59)
	createRequestID := uuid.New()
	// this is only a shallow test, meaning
	// the mutable state only has the minimal information
	// so we can test the conflict resolver
	s.mockExecutionMgr.On("ResetMutableState", &persistence.ResetMutableStateRequest{
		ExecutionInfo: &persistence.WorkflowExecutionInfo{
			DomainID:             domainID,
			WorkflowID:           execution.GetWorkflowId(),
			RunID:                execution.GetRunId(),
			TaskList:             event1.WorkflowExecutionStartedEventAttributes.TaskList.GetName(),
			WorkflowTypeName:     event1.WorkflowExecutionStartedEventAttributes.WorkflowType.GetName(),
			WorkflowTimeout:      *event1.WorkflowExecutionStartedEventAttributes.ExecutionStartToCloseTimeoutSeconds,
			DecisionTimeoutValue: *event1.WorkflowExecutionStartedEventAttributes.TaskStartToCloseTimeoutSeconds,
			State:                persistence.WorkflowStateCreated,
			CloseStatus:          persistence.WorkflowCloseStatusNone,
			LastFirstEventID:     event1.GetEventId(),
			NextEventID:          nextEventID,
			LastProcessedEvent:   common.EmptyEventID,
			StartTimestamp:       startTime,
			LastUpdatedTimestamp: startTime,
			DecisionVersion:      common.EmptyVersion,
			DecisionScheduleID:   common.EmptyEventID,
			DecisionStartedID:    common.EmptyEventID,
			DecisionRequestID:    emptyUUID,
			DecisionTimeout:      0,
			DecisionAttempt:      0,
			DecisionTimestamp:    0,
			CreateRequestID:      createRequestID,
		},
		ReplicationState: &persistence.ReplicationState{
			CurrentVersion:   event1.GetVersion(),
			StartVersion:     event1.GetVersion(),
			LastWriteVersion: event1.GetVersion(),
			LastWriteEventID: event1.GetEventId(),
			LastReplicationInfo: map[string]*persistence.ReplicationInfo{
				sourceCluster: &persistence.ReplicationInfo{
					Version:     event1.GetVersion(),
					LastEventID: event1.GetEventId(),
				},
			},
		},
		Condition:                 s.mockContext.updateCondition,
		RangeID:                   s.mockShard.shardInfo.RangeID,
		InsertActivityInfos:       []*persistence.ActivityInfo{},
		InsertTimerInfos:          []*persistence.TimerInfo{},
		InsertChildExecutionInfos: []*persistence.ChildExecutionInfo{},
		InsertRequestCancelInfos:  []*persistence.RequestCancelInfo{},
		InsertSignalInfos:         []*persistence.SignalInfo{},
		InsertSignalRequestedIDs:  []string{},
	}).Return(nil).Once()
	s.mockExecutionMgr.On("GetWorkflowExecution", &persistence.GetWorkflowExecutionRequest{
		DomainID:  domainID,
		Execution: execution,
	}).Return(&persistence.GetWorkflowExecutionResponse{}, nil).Once() // return empty resoonse since we are not testing the load
	s.mockClusterMetadata.On("IsGlobalDomainEnabled").Return(true)
	s.mockMetadataMgr.On("GetDomain", mock.Anything).Return(
		&persistence.GetDomainResponse{
			Info:   &persistence.DomainInfo{},
			Config: &persistence.DomainConfig{},
			ReplicationConfig: &persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestAlternativeClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
					&persistence.ClusterReplicationConfig{ClusterName: cluster.TestAlternativeClusterName},
				},
			},
			IsGlobalDomain: true,
		},
		nil,
	)
	_, err := s.conflictResolver.reset(createRequestID, nextEventID-1, startTime)
	s.Nil(err)
}
