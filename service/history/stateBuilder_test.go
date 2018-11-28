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
)

type (
	stateBuilderSuite struct {
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
		mockMutableState    *mockMutableState

		stateBuilder *stateBuilderImpl
	}
)

func TestStateBuilderSuite(t *testing.T) {
	s := new(stateBuilderSuite)
	suite.Run(t, s)
}

func (s *stateBuilderSuite) SetupSuite() {
	if testing.Verbose() {
		log.SetOutput(os.Stdout)
	}

}

func (s *stateBuilderSuite) TearDownSuite() {

}

func (s *stateBuilderSuite) SetupTest() {
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
		config:                    NewDynamicConfigForTest(),
		logger:                    s.logger,
		domainCache:               cache.NewDomainCache(s.mockMetadataMgr, s.mockClusterMetadata, metricsClient, s.logger),
		metricsClient:             metrics.NewClient(tally.NoopScope, metrics.History),
	}
	s.mockMutableState = &mockMutableState{}
	s.stateBuilder = newStateBuilder(s.mockShard, s.mockMutableState, s.logger)
	s.mockClusterMetadata.On("GetCurrentClusterName").Return(cluster.TestCurrentClusterName)
	s.mockClusterMetadata.On("IsGlobalDomainEnabled").Return(true)
}

func (s *stateBuilderSuite) TearDownTest() {
	s.stateBuilder = nil
	s.mockHistoryMgr.AssertExpectations(s.T())
	s.mockExecutionMgr.AssertExpectations(s.T())
	s.mockShardManager.AssertExpectations(s.T())
	s.mockProducer.AssertExpectations(s.T())
	s.mockMetadataMgr.AssertExpectations(s.T())
}

func (s *stateBuilderSuite) mockUpdateVersion(events ...*shared.HistoryEvent) {
	for _, event := range events {
		s.mockMutableState.On("UpdateReplicationStateVersion", event.GetVersion(), true).Once()
	}
}

func (s *stateBuilderSuite) toHistory(events ...*shared.HistoryEvent) []*shared.HistoryEvent {
	return events
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeWorkflowExecutionStarted() {
	version := int64(1)
	requestID := uuid.New()
	domainID := validDomainID
	execution := shared.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(validRunID),
	}
	parentName := "some random parent domain names"
	parentDomainID := uuid.New()

	executionInfo := &persistence.WorkflowExecutionInfo{
		WorkflowTimeout: 100,
	}

	now := time.Now()
	evenType := shared.EventTypeWorkflowExecutionStarted
	event := &shared.HistoryEvent{
		Version:   common.Int64Ptr(version),
		EventId:   common.Int64Ptr(1),
		Timestamp: common.Int64Ptr(now.UnixNano()),
		EventType: &evenType,
		WorkflowExecutionStartedEventAttributes: &shared.WorkflowExecutionStartedEventAttributes{
			ParentWorkflowDomain: common.StringPtr(parentName),
		},
	}

	s.mockMetadataMgr.On("GetDomain", &persistence.GetDomainRequest{Name: parentName}).Return(
		&persistence.GetDomainResponse{
			Info:   &persistence.DomainInfo{ID: parentDomainID, Name: parentName},
			Config: &persistence.DomainConfig{Retention: 1},
			ReplicationConfig: &persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				},
			},
			IsGlobalDomain: true,
			TableVersion:   persistence.DomainTableVersionV1,
		}, nil,
	).Once()
	s.mockMutableState.On("ReplicateWorkflowExecutionStartedEvent",
		domainID, &parentDomainID, execution, requestID, event.WorkflowExecutionStartedEventAttributes).Once()
	s.mockUpdateVersion(event)
	s.mockMutableState.On("GetExecutionInfo").Return(executionInfo)

	s.stateBuilder.applyEvents(domainID, requestID, execution, s.toHistory(event), nil, 0, 0)
	s.Equal(1, len(s.stateBuilder.timerTasks))
	timerTask, ok := s.stateBuilder.timerTasks[0].(*persistence.WorkflowTimeoutTask)
	s.True(ok)
	s.True(timerTask.VisibilityTimestamp.Equal(now.Add(time.Duration(executionInfo.WorkflowTimeout) * time.Second)))

	s.Empty(s.stateBuilder.transferTasks)
	s.Empty(s.stateBuilder.newRunTimerTasks)
	s.Empty(s.stateBuilder.newRunTransferTasks)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeWorkflowExecutionTimedOut() {
	version := int64(1)
	requestID := uuid.New()
	domainName := "some random domain name"
	domainID := validDomainID
	execution := shared.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(validRunID),
	}
	retentionDays := int32(1)

	now := time.Now()
	evenType := shared.EventTypeWorkflowExecutionTimedOut
	event := &shared.HistoryEvent{
		Version:                                  common.Int64Ptr(version),
		EventId:                                  common.Int64Ptr(130),
		Timestamp:                                common.Int64Ptr(now.UnixNano()),
		EventType:                                &evenType,
		WorkflowExecutionTimedOutEventAttributes: &shared.WorkflowExecutionTimedOutEventAttributes{},
	}

	s.mockMetadataMgr.On("GetDomain", &persistence.GetDomainRequest{ID: domainID}).Return(
		&persistence.GetDomainResponse{
			Info:   &persistence.DomainInfo{ID: domainID, Name: domainName},
			Config: &persistence.DomainConfig{Retention: retentionDays},
			ReplicationConfig: &persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				},
			},
			IsGlobalDomain: true,
			TableVersion:   persistence.DomainTableVersionV1,
		}, nil,
	).Once()
	s.mockMutableState.On("ReplicateWorkflowExecutionTimedoutEvent", event).Once()
	s.mockUpdateVersion(event)

	s.stateBuilder.applyEvents(domainID, requestID, execution, s.toHistory(event), nil, 0, 0)
	s.Equal([]persistence.Task{&persistence.CloseExecutionTask{}}, s.stateBuilder.transferTasks)
	s.Equal(1, len(s.stateBuilder.timerTasks))
	timerTask, ok := s.stateBuilder.timerTasks[0].(*persistence.DeleteHistoryEventTask)
	s.True(ok)
	s.True(timerTask.VisibilityTimestamp.Equal(now.Add(time.Duration(retentionDays) * time.Hour * 24)))

	s.Empty(s.stateBuilder.newRunTimerTasks)
	s.Empty(s.stateBuilder.newRunTransferTasks)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeWorkflowExecutionTerminated() {
	version := int64(1)
	requestID := uuid.New()
	domainName := "some random domain name"
	domainID := validDomainID
	execution := shared.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(validRunID),
	}
	retentionDays := int32(1)

	now := time.Now()
	evenType := shared.EventTypeWorkflowExecutionTerminated
	event := &shared.HistoryEvent{
		Version:   common.Int64Ptr(version),
		EventId:   common.Int64Ptr(130),
		Timestamp: common.Int64Ptr(now.UnixNano()),
		EventType: &evenType,
		WorkflowExecutionTerminatedEventAttributes: &shared.WorkflowExecutionTerminatedEventAttributes{},
	}

	s.mockMetadataMgr.On("GetDomain", &persistence.GetDomainRequest{ID: domainID}).Return(
		&persistence.GetDomainResponse{
			Info:   &persistence.DomainInfo{ID: domainID, Name: domainName},
			Config: &persistence.DomainConfig{Retention: retentionDays},
			ReplicationConfig: &persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				},
			},
			IsGlobalDomain: true,
			TableVersion:   persistence.DomainTableVersionV1,
		}, nil,
	).Once()
	s.mockMutableState.On("ReplicateWorkflowExecutionTerminatedEvent", event).Once()
	s.mockUpdateVersion(event)

	s.stateBuilder.applyEvents(domainID, requestID, execution, s.toHistory(event), nil, 0, 0)
	s.Equal([]persistence.Task{&persistence.CloseExecutionTask{}}, s.stateBuilder.transferTasks)
	s.Equal(1, len(s.stateBuilder.timerTasks))
	timerTask, ok := s.stateBuilder.timerTasks[0].(*persistence.DeleteHistoryEventTask)
	s.True(ok)
	s.True(timerTask.VisibilityTimestamp.Equal(now.Add(time.Duration(retentionDays) * time.Hour * 24)))

	s.Empty(s.stateBuilder.newRunTimerTasks)
	s.Empty(s.stateBuilder.newRunTransferTasks)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeWorkflowExecutionSignaled() {
	version := int64(1)
	requestID := uuid.New()
	domainID := validDomainID
	execution := shared.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(validRunID),
	}

	now := time.Now()
	evenType := shared.EventTypeWorkflowExecutionSignaled
	event := &shared.HistoryEvent{
		Version:                                  common.Int64Ptr(version),
		EventId:                                  common.Int64Ptr(130),
		Timestamp:                                common.Int64Ptr(now.UnixNano()),
		EventType:                                &evenType,
		WorkflowExecutionSignaledEventAttributes: &shared.WorkflowExecutionSignaledEventAttributes{},
	}
	s.mockUpdateVersion(event)
	s.mockMutableState.On("ReplicateWorkflowExecutionSignaled", event).Once()

	s.stateBuilder.applyEvents(domainID, requestID, execution, s.toHistory(event), nil, 0, 0)

	s.Empty(s.stateBuilder.timerTasks)
	s.Empty(s.stateBuilder.transferTasks)
	s.Empty(s.stateBuilder.newRunTimerTasks)
	s.Empty(s.stateBuilder.newRunTransferTasks)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeWorkflowExecutionFailed() {
	version := int64(1)
	requestID := uuid.New()
	domainName := "some random domain name"
	domainID := validDomainID
	execution := shared.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(validRunID),
	}
	retentionDays := int32(1)

	now := time.Now()
	evenType := shared.EventTypeWorkflowExecutionFailed
	event := &shared.HistoryEvent{
		Version:                                common.Int64Ptr(version),
		EventId:                                common.Int64Ptr(130),
		Timestamp:                              common.Int64Ptr(now.UnixNano()),
		EventType:                              &evenType,
		WorkflowExecutionFailedEventAttributes: &shared.WorkflowExecutionFailedEventAttributes{},
	}

	s.mockMetadataMgr.On("GetDomain", &persistence.GetDomainRequest{ID: domainID}).Return(
		&persistence.GetDomainResponse{
			Info:   &persistence.DomainInfo{ID: domainID, Name: domainName},
			Config: &persistence.DomainConfig{Retention: retentionDays},
			ReplicationConfig: &persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				},
			},
			IsGlobalDomain: true,
			TableVersion:   persistence.DomainTableVersionV1,
		}, nil,
	).Once()
	s.mockMutableState.On("ReplicateWorkflowExecutionFailedEvent", event).Once()
	s.mockUpdateVersion(event)

	s.stateBuilder.applyEvents(domainID, requestID, execution, s.toHistory(event), nil, 0, 0)
	s.Equal([]persistence.Task{&persistence.CloseExecutionTask{}}, s.stateBuilder.transferTasks)
	s.Equal(1, len(s.stateBuilder.timerTasks))
	timerTask, ok := s.stateBuilder.timerTasks[0].(*persistence.DeleteHistoryEventTask)
	s.True(ok)
	s.True(timerTask.VisibilityTimestamp.Equal(now.Add(time.Duration(retentionDays) * time.Hour * 24)))

	s.Empty(s.stateBuilder.newRunTimerTasks)
	s.Empty(s.stateBuilder.newRunTransferTasks)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeWorkflowExecutionContinuedAsNew() {
	sourceCluster := "some random source cluster"
	version := int64(1)
	requestID := uuid.New()
	domainName := "some random domain name"
	domainID := validDomainID
	execution := shared.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(validRunID),
	}
	parentName := "some random parent domain names"
	parentDomainID := uuid.New()
	parentWorkflowID := "some random parent workflow ID"
	parentRunID := uuid.New()
	parentInitiatedEventID := int64(144)
	retentionDays := int32(1)

	now := time.Now()
	tasklist := "some random tasklist"
	workflowType := "some random workflow type"
	workflowTimeoutSecond := int32(110)
	decisionTimeoutSecond := int32(11)
	newRunID := uuid.New()

	continueAsNewEvenType := shared.EventTypeWorkflowExecutionContinuedAsNew
	continueAsNewEvent := &shared.HistoryEvent{
		Version:   common.Int64Ptr(version),
		EventId:   common.Int64Ptr(130),
		Timestamp: common.Int64Ptr(now.UnixNano()),
		EventType: &continueAsNewEvenType,
		WorkflowExecutionContinuedAsNewEventAttributes: &shared.WorkflowExecutionContinuedAsNewEventAttributes{
			NewExecutionRunId: common.StringPtr(newRunID),
		},
	}

	newRunStartedEvenType := shared.EventTypeWorkflowExecutionStarted
	newRunStartedEvent := &shared.HistoryEvent{
		Version:   common.Int64Ptr(version),
		EventId:   common.Int64Ptr(1),
		Timestamp: common.Int64Ptr(now.UnixNano()),
		EventType: &newRunStartedEvenType,
		WorkflowExecutionStartedEventAttributes: &shared.WorkflowExecutionStartedEventAttributes{
			ParentWorkflowDomain: common.StringPtr(parentName),
			ParentWorkflowExecution: &shared.WorkflowExecution{
				WorkflowId: common.StringPtr(parentWorkflowID),
				RunId:      common.StringPtr(parentRunID),
			},
			ParentInitiatedEventId:              common.Int64Ptr(parentInitiatedEventID),
			ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(workflowTimeoutSecond),
			TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(decisionTimeoutSecond),
			TaskList:                            &shared.TaskList{Name: common.StringPtr(tasklist)},
			WorkflowType:                        &shared.WorkflowType{Name: common.StringPtr(workflowType)},
		},
	}

	newRunDecisionEvenType := shared.EventTypeDecisionTaskScheduled
	newRunDecisionAttempt := int64(123)
	newRunDecisionEvent := &shared.HistoryEvent{
		Version:   common.Int64Ptr(version),
		EventId:   common.Int64Ptr(2),
		Timestamp: common.Int64Ptr(now.UnixNano()),
		EventType: &newRunDecisionEvenType,
		DecisionTaskScheduledEventAttributes: &shared.DecisionTaskScheduledEventAttributes{
			TaskList:                   &shared.TaskList{Name: common.StringPtr(tasklist)},
			StartToCloseTimeoutSeconds: common.Int32Ptr(decisionTimeoutSecond),
			Attempt:                    common.Int64Ptr(newRunDecisionAttempt),
		},
	}

	s.mockMetadataMgr.On("GetDomain", &persistence.GetDomainRequest{ID: domainID}).Return(
		&persistence.GetDomainResponse{
			Info:   &persistence.DomainInfo{ID: domainID, Name: domainName},
			Config: &persistence.DomainConfig{Retention: retentionDays},
			ReplicationConfig: &persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				},
			},
			IsGlobalDomain: true,
			TableVersion:   persistence.DomainTableVersionV1,
		}, nil,
	).Once()
	s.mockMetadataMgr.On("GetDomain", &persistence.GetDomainRequest{Name: parentName}).Return(
		&persistence.GetDomainResponse{
			Info:   &persistence.DomainInfo{ID: parentDomainID, Name: parentName},
			Config: &persistence.DomainConfig{Retention: 1},
			ReplicationConfig: &persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				},
			},
			IsGlobalDomain: true,
			TableVersion:   persistence.DomainTableVersionV1,
		}, nil,
	).Once()
	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", continueAsNewEvent.GetVersion()).Return(sourceCluster).Once()
	s.mockMutableState.On("ReplicateWorkflowExecutionContinuedAsNewEvent",
		sourceCluster,
		domainID,
		continueAsNewEvent,
		newRunStartedEvent,
		&decisionInfo{
			Version:         newRunDecisionEvent.GetVersion(),
			ScheduleID:      newRunDecisionEvent.GetEventId(),
			StartedID:       common.EmptyEventID,
			RequestID:       emptyUUID,
			DecisionTimeout: decisionTimeoutSecond,
			TaskList:        tasklist,
			Attempt:         newRunDecisionAttempt,
		},
		mock.Anything,
	).Once()
	s.mockUpdateVersion(continueAsNewEvent)

	newRunHistory := &shared.History{Events: []*shared.HistoryEvent{newRunStartedEvent, newRunDecisionEvent}}
	_, _, newRunStateBuilder, err := s.stateBuilder.applyEvents(domainID, requestID, execution, s.toHistory(continueAsNewEvent), newRunHistory.Events, 0, 0)
	s.Nil(err)
	expectedNewRunStateBuilder := newMutableStateBuilderWithReplicationState(
		s.mockClusterMetadata.GetCurrentClusterName(),
		s.mockShard.GetConfig(),
		s.logger,
		newRunStartedEvent.GetVersion(),
	)
	expectedNewRunStateBuilder.ReplicateWorkflowExecutionStartedEvent(
		domainID,
		common.StringPtr(parentDomainID),
		shared.WorkflowExecution{
			WorkflowId: execution.WorkflowId,
			RunId:      common.StringPtr(newRunID),
		},
		newRunStateBuilder.GetExecutionInfo().CreateRequestID,
		newRunStartedEvent.WorkflowExecutionStartedEventAttributes,
	)
	expectedNewRunStateBuilder.ReplicateDecisionTaskScheduledEvent(
		newRunDecisionEvent.GetVersion(),
		newRunDecisionEvent.GetEventId(),
		tasklist,
		decisionTimeoutSecond,
		newRunDecisionAttempt,
	)
	expectedNewRunStateBuilder.GetExecutionInfo().LastFirstEventID = newRunStartedEvent.GetEventId()
	expectedNewRunStateBuilder.GetExecutionInfo().NextEventID = newRunDecisionEvent.GetEventId() + 1
	expectedNewRunStateBuilder.SetHistoryBuilder(newHistoryBuilderFromEvents(newRunHistory.Events, s.logger))
	expectedNewRunStateBuilder.UpdateReplicationStateLastEventID(sourceCluster, newRunStartedEvent.GetVersion(), newRunDecisionEvent.GetEventId())
	s.Equal(expectedNewRunStateBuilder, newRunStateBuilder)

	s.Equal([]persistence.Task{&persistence.CloseExecutionTask{}}, s.stateBuilder.transferTasks)
	s.Equal(1, len(s.stateBuilder.timerTasks))
	timerTask, ok := s.stateBuilder.timerTasks[0].(*persistence.DeleteHistoryEventTask)
	s.True(ok)
	s.True(timerTask.VisibilityTimestamp.Equal(now.Add(time.Duration(retentionDays) * time.Hour * 24)))

	s.Equal(1, len(s.stateBuilder.newRunTimerTasks))
	newRunTimerTask, ok := s.stateBuilder.newRunTimerTasks[0].(*persistence.WorkflowTimeoutTask)
	s.True(ok)
	s.True(newRunTimerTask.VisibilityTimestamp.Equal(now.Add(time.Duration(workflowTimeoutSecond) * time.Second)))
	s.Equal([]persistence.Task{&persistence.DecisionTask{
		DomainID:   domainID,
		TaskList:   tasklist,
		ScheduleID: newRunDecisionEvent.GetEventId(),
	}}, s.stateBuilder.newRunTransferTasks)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeWorkflowExecutionCompleted() {
	version := int64(1)
	requestID := uuid.New()
	domainName := "some random domain name"
	domainID := validDomainID
	execution := shared.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(validRunID),
	}
	retentionDays := int32(1)

	now := time.Now()
	evenType := shared.EventTypeWorkflowExecutionCompleted
	event := &shared.HistoryEvent{
		Version:   common.Int64Ptr(version),
		EventId:   common.Int64Ptr(130),
		Timestamp: common.Int64Ptr(now.UnixNano()),
		EventType: &evenType,
		WorkflowExecutionCompletedEventAttributes: &shared.WorkflowExecutionCompletedEventAttributes{},
	}

	s.mockMetadataMgr.On("GetDomain", &persistence.GetDomainRequest{ID: domainID}).Return(
		&persistence.GetDomainResponse{
			Info:   &persistence.DomainInfo{ID: domainID, Name: domainName},
			Config: &persistence.DomainConfig{Retention: retentionDays},
			ReplicationConfig: &persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				},
			},
			IsGlobalDomain: true,
			TableVersion:   persistence.DomainTableVersionV1,
		}, nil,
	).Once()
	s.mockMutableState.On("ReplicateWorkflowExecutionCompletedEvent", event).Once()
	s.mockUpdateVersion(event)

	s.stateBuilder.applyEvents(domainID, requestID, execution, s.toHistory(event), nil, 0, 0)
	s.Equal([]persistence.Task{&persistence.CloseExecutionTask{}}, s.stateBuilder.transferTasks)
	s.Equal(1, len(s.stateBuilder.timerTasks))
	timerTask, ok := s.stateBuilder.timerTasks[0].(*persistence.DeleteHistoryEventTask)
	s.True(ok)
	s.True(timerTask.VisibilityTimestamp.Equal(now.Add(time.Duration(retentionDays) * time.Hour * 24)))

	s.Empty(s.stateBuilder.newRunTimerTasks)
	s.Empty(s.stateBuilder.newRunTransferTasks)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeWorkflowExecutionCanceled() {
	version := int64(1)
	requestID := uuid.New()
	domainName := "some random domain name"
	domainID := validDomainID
	execution := shared.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(validRunID),
	}
	retentionDays := int32(1)

	now := time.Now()
	evenType := shared.EventTypeWorkflowExecutionCanceled
	event := &shared.HistoryEvent{
		Version:                                  common.Int64Ptr(version),
		EventId:                                  common.Int64Ptr(130),
		Timestamp:                                common.Int64Ptr(now.UnixNano()),
		EventType:                                &evenType,
		WorkflowExecutionCanceledEventAttributes: &shared.WorkflowExecutionCanceledEventAttributes{},
	}

	s.mockMetadataMgr.On("GetDomain", &persistence.GetDomainRequest{ID: domainID}).Return(
		&persistence.GetDomainResponse{
			Info:   &persistence.DomainInfo{ID: domainID, Name: domainName},
			Config: &persistence.DomainConfig{Retention: retentionDays},
			ReplicationConfig: &persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				},
			},
			IsGlobalDomain: true,
			TableVersion:   persistence.DomainTableVersionV1,
		}, nil,
	).Once()
	s.mockMutableState.On("ReplicateWorkflowExecutionCanceledEvent", event).Once()
	s.mockUpdateVersion(event)

	s.stateBuilder.applyEvents(domainID, requestID, execution, s.toHistory(event), nil, 0, 0)
	s.Equal([]persistence.Task{&persistence.CloseExecutionTask{}}, s.stateBuilder.transferTasks)
	s.Equal(1, len(s.stateBuilder.timerTasks))
	timerTask, ok := s.stateBuilder.timerTasks[0].(*persistence.DeleteHistoryEventTask)
	s.True(ok)
	s.True(timerTask.VisibilityTimestamp.Equal(now.Add(time.Duration(retentionDays) * time.Hour * 24)))

	s.Empty(s.stateBuilder.newRunTimerTasks)
	s.Empty(s.stateBuilder.newRunTransferTasks)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeWorkflowExecutionCancelRequested() {
	version := int64(1)
	requestID := uuid.New()
	domainID := validDomainID
	execution := shared.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(validRunID),
	}
	now := time.Now()
	evenType := shared.EventTypeWorkflowExecutionCancelRequested
	event := &shared.HistoryEvent{
		Version:   common.Int64Ptr(version),
		EventId:   common.Int64Ptr(130),
		Timestamp: common.Int64Ptr(now.UnixNano()),
		EventType: &evenType,
		WorkflowExecutionCancelRequestedEventAttributes: &shared.WorkflowExecutionCancelRequestedEventAttributes{},
	}

	s.mockMutableState.On("ReplicateWorkflowExecutionCancelRequestedEvent", event).Once()
	s.mockUpdateVersion(event)

	s.stateBuilder.applyEvents(domainID, requestID, execution, s.toHistory(event), nil, 0, 0)

	s.Empty(s.stateBuilder.timerTasks)
	s.Empty(s.stateBuilder.transferTasks)
	s.Empty(s.stateBuilder.newRunTimerTasks)
	s.Empty(s.stateBuilder.newRunTransferTasks)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeTimerStarted() {
	version := int64(1)
	requestID := uuid.New()
	domainID := validDomainID
	execution := shared.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(validRunID),
	}

	now := time.Now()
	timerID := "timer ID"
	timeoutSecond := int64(10)
	evenType := shared.EventTypeTimerStarted
	event := &shared.HistoryEvent{
		Version:   common.Int64Ptr(version),
		EventId:   common.Int64Ptr(130),
		Timestamp: common.Int64Ptr(now.UnixNano()),
		EventType: &evenType,
		TimerStartedEventAttributes: &shared.TimerStartedEventAttributes{
			TimerId:                   common.StringPtr(timerID),
			StartToFireTimeoutSeconds: common.Int64Ptr(timeoutSecond),
		},
	}
	ti := &persistence.TimerInfo{
		Version:    event.GetVersion(),
		TimerID:    timerID,
		ExpiryTime: time.Unix(0, event.GetTimestamp()).Add(time.Duration(timeoutSecond) * time.Second),
		StartedID:  event.GetEventId(),
		TaskID:     TimerTaskStatusNone,
	}
	s.mockMutableState.On("GetPendingTimerInfos").Return(map[string]*persistence.TimerInfo{timerID: ti}).Once()
	s.mockMutableState.On("UpdateUserTimer", ti.TimerID, ti).Once()
	s.mockMutableState.On("ReplicateTimerStartedEvent", event).Return(ti).Once()
	s.mockUpdateVersion(event)

	s.stateBuilder.applyEvents(domainID, requestID, execution, s.toHistory(event), nil, 0, 0)
	s.Equal(1, len(s.stateBuilder.timerTasks))
	timerTask, ok := s.stateBuilder.timerTasks[0].(*persistence.UserTimerTask)
	s.True(ok)
	s.True(timerTask.VisibilityTimestamp.Equal(ti.ExpiryTime))
	s.Equal(ti.StartedID, timerTask.EventID)

	s.Empty(s.stateBuilder.transferTasks)
	s.Empty(s.stateBuilder.newRunTimerTasks)
	s.Empty(s.stateBuilder.newRunTransferTasks)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeTimerFired() {
	version := int64(1)
	requestID := uuid.New()
	domainID := validDomainID
	execution := shared.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(validRunID),
	}

	now := time.Now()
	evenType := shared.EventTypeTimerFired
	event := &shared.HistoryEvent{
		Version:                   common.Int64Ptr(version),
		EventId:                   common.Int64Ptr(130),
		Timestamp:                 common.Int64Ptr(now.UnixNano()),
		EventType:                 &evenType,
		TimerFiredEventAttributes: &shared.TimerFiredEventAttributes{},
	}

	// this is the remaining timer
	// should create a user timer for this
	timeoutSecond := int32(20)
	ti := &persistence.TimerInfo{
		Version:    version,
		TimerID:    "some random timer ID",
		ExpiryTime: now.Add(time.Duration(timeoutSecond) * time.Second),
		StartedID:  144,
		TaskID:     TimerTaskStatusNone,
	}
	s.mockMutableState.On("GetPendingTimerInfos").Return(map[string]*persistence.TimerInfo{ti.TimerID: ti}).Once()
	s.mockMutableState.On("UpdateUserTimer", ti.TimerID, ti).Once()
	s.mockMutableState.On("ReplicateTimerFiredEvent", event).Once()
	s.mockUpdateVersion(event)

	s.stateBuilder.applyEvents(domainID, requestID, execution, s.toHistory(event), nil, 0, 0)
	s.Equal(1, len(s.stateBuilder.timerTasks))
	timerTask, ok := s.stateBuilder.timerTasks[0].(*persistence.UserTimerTask)
	s.True(ok)
	s.True(timerTask.VisibilityTimestamp.Equal(ti.ExpiryTime))
	s.Empty(s.stateBuilder.transferTasks)
	s.Empty(s.stateBuilder.newRunTimerTasks)
	s.Empty(s.stateBuilder.newRunTransferTasks)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeTimerCanceled() {
	version := int64(1)
	requestID := uuid.New()
	domainID := validDomainID
	execution := shared.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(validRunID),
	}

	now := time.Now()

	// this is a timer which already got created and will be used to generate a new concrete timer
	timerID := "timer ID"
	timeoutSecond := int64(10)
	ti := &persistence.TimerInfo{
		Version:    version,
		TimerID:    timerID,
		ExpiryTime: time.Unix(0, now.UnixNano()).Add(time.Duration(timeoutSecond) * time.Second),
		StartedID:  111,
		TaskID:     TimerTaskStatusNone,
	}
	s.mockMutableState.On("GetPendingTimerInfos").Return(map[string]*persistence.TimerInfo{timerID: ti}).Once()
	s.mockMutableState.On("UpdateUserTimer", ti.TimerID, ti).Once()

	evenType := shared.EventTypeTimerCanceled
	event := &shared.HistoryEvent{
		Version:                      common.Int64Ptr(version),
		EventId:                      common.Int64Ptr(130),
		Timestamp:                    common.Int64Ptr(now.UnixNano()),
		EventType:                    &evenType,
		TimerCanceledEventAttributes: &shared.TimerCanceledEventAttributes{},
	}
	s.mockMutableState.On("ReplicateTimerCanceledEvent", event).Once()
	s.mockUpdateVersion(event)

	s.stateBuilder.applyEvents(domainID, requestID, execution, s.toHistory(event), nil, 0, 0)

	s.Equal(1, len(s.stateBuilder.timerTasks))
	timerTask, ok := s.stateBuilder.timerTasks[0].(*persistence.UserTimerTask)
	s.True(ok)
	s.True(timerTask.VisibilityTimestamp.Equal(ti.ExpiryTime))
	s.Equal(ti.StartedID, timerTask.EventID)
	s.Empty(s.stateBuilder.transferTasks)
	s.Empty(s.stateBuilder.newRunTimerTasks)
	s.Empty(s.stateBuilder.newRunTransferTasks)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeStartChildWorkflowExecutionInitiated() {
	version := int64(1)
	requestID := uuid.New()
	domainID := validDomainID
	execution := shared.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(validRunID),
	}
	targetDomainID := uuid.New()
	targetWorkflowID := "some random target workflow ID"
	targetDomain := "some random target domain name"

	now := time.Now()
	createRequestID := uuid.New()
	evenType := shared.EventTypeStartChildWorkflowExecutionInitiated
	event := &shared.HistoryEvent{
		Version:   common.Int64Ptr(version),
		EventId:   common.Int64Ptr(130),
		Timestamp: common.Int64Ptr(now.UnixNano()),
		EventType: &evenType,
		StartChildWorkflowExecutionInitiatedEventAttributes: &shared.StartChildWorkflowExecutionInitiatedEventAttributes{
			Domain:     common.StringPtr(targetDomain),
			WorkflowId: common.StringPtr(targetWorkflowID),
		},
	}

	ci := &persistence.ChildExecutionInfo{
		Version:         event.GetVersion(),
		InitiatedID:     event.GetEventId(),
		InitiatedEvent:  event,
		StartedID:       common.EmptyEventID,
		CreateRequestID: createRequestID,
	}

	// the create request ID is generated inside, cannot assert equal
	s.mockMutableState.On("ReplicateStartChildWorkflowExecutionInitiatedEvent", event, mock.Anything).Return(ci).Once()
	s.mockMetadataMgr.On("GetDomain", &persistence.GetDomainRequest{Name: targetDomain}).Return(
		&persistence.GetDomainResponse{
			Info:   &persistence.DomainInfo{ID: targetDomainID, Name: targetDomain},
			Config: &persistence.DomainConfig{Retention: 1},
			ReplicationConfig: &persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				},
			},
			IsGlobalDomain: true,
			TableVersion:   persistence.DomainTableVersionV1,
		}, nil,
	).Once()
	s.mockUpdateVersion(event)

	s.stateBuilder.applyEvents(domainID, requestID, execution, s.toHistory(event), nil, 0, 0)
	s.Equal([]persistence.Task{&persistence.StartChildExecutionTask{
		TargetDomainID:   targetDomainID,
		TargetWorkflowID: targetWorkflowID,
		InitiatedID:      event.GetEventId(),
	}}, s.stateBuilder.transferTasks)

	s.Empty(s.stateBuilder.timerTasks)
	s.Empty(s.stateBuilder.newRunTimerTasks)
	s.Empty(s.stateBuilder.newRunTransferTasks)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeStartChildWorkflowExecutionFailed() {
	version := int64(1)
	requestID := uuid.New()
	domainID := validDomainID
	execution := shared.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(validRunID),
	}

	now := time.Now()
	evenType := shared.EventTypeStartChildWorkflowExecutionFailed
	event := &shared.HistoryEvent{
		Version:   common.Int64Ptr(version),
		EventId:   common.Int64Ptr(130),
		Timestamp: common.Int64Ptr(now.UnixNano()),
		EventType: &evenType,
		StartChildWorkflowExecutionFailedEventAttributes: &shared.StartChildWorkflowExecutionFailedEventAttributes{},
	}
	s.mockMutableState.On("ReplicateStartChildWorkflowExecutionFailedEvent", event).Once()
	s.mockUpdateVersion(event)

	s.stateBuilder.applyEvents(domainID, requestID, execution, s.toHistory(event), nil, 0, 0)

	s.Empty(s.stateBuilder.timerTasks)
	s.Empty(s.stateBuilder.transferTasks)
	s.Empty(s.stateBuilder.newRunTimerTasks)
	s.Empty(s.stateBuilder.newRunTransferTasks)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeSignalExternalWorkflowExecutionInitiated() {
	version := int64(1)
	requestID := uuid.New()
	domainID := validDomainID
	execution := shared.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(validRunID),
	}
	targetDomainID := uuid.New()
	targetWorkflowID := "some random target workflow ID"
	targetRunID := uuid.New()
	childWorkflowOnly := true
	targetDomain := "some random target domain name"

	now := time.Now()
	signalRequestID := uuid.New()
	signalName := "some random signal name"
	signalInput := []byte("some random signal input")
	control := []byte("some random control")
	evenType := shared.EventTypeSignalExternalWorkflowExecutionInitiated
	event := &shared.HistoryEvent{
		Version:   common.Int64Ptr(version),
		EventId:   common.Int64Ptr(130),
		Timestamp: common.Int64Ptr(now.UnixNano()),
		EventType: &evenType,
		SignalExternalWorkflowExecutionInitiatedEventAttributes: &shared.SignalExternalWorkflowExecutionInitiatedEventAttributes{
			Domain: common.StringPtr(targetDomain),
			WorkflowExecution: &shared.WorkflowExecution{
				WorkflowId: common.StringPtr(targetWorkflowID),
				RunId:      common.StringPtr(targetRunID),
			},
			SignalName:        common.StringPtr(signalName),
			Input:             signalInput,
			ChildWorkflowOnly: common.BoolPtr(childWorkflowOnly),
		},
	}
	si := &persistence.SignalInfo{
		Version:         event.GetVersion(),
		InitiatedID:     event.GetEventId(),
		SignalRequestID: signalRequestID,
		SignalName:      signalName,
		Input:           signalInput,
		Control:         control,
	}

	// the cancellation request ID is generated inside, cannot assert equal
	s.mockMutableState.On("ReplicateSignalExternalWorkflowExecutionInitiatedEvent", event, mock.Anything).Return(si).Once()
	s.mockMetadataMgr.On("GetDomain", &persistence.GetDomainRequest{Name: targetDomain}).Return(
		&persistence.GetDomainResponse{
			Info:   &persistence.DomainInfo{ID: targetDomainID, Name: targetDomain},
			Config: &persistence.DomainConfig{Retention: 1},
			ReplicationConfig: &persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				},
			},
			IsGlobalDomain: true,
			TableVersion:   persistence.DomainTableVersionV1,
		}, nil,
	).Once()
	s.mockUpdateVersion(event)

	s.stateBuilder.applyEvents(domainID, requestID, execution, s.toHistory(event), nil, 0, 0)

	s.Equal([]persistence.Task{&persistence.SignalExecutionTask{
		TargetDomainID:          targetDomainID,
		TargetWorkflowID:        targetWorkflowID,
		TargetRunID:             targetRunID,
		TargetChildWorkflowOnly: childWorkflowOnly,
		InitiatedID:             event.GetEventId(),
	}}, s.stateBuilder.transferTasks)

	s.Empty(s.stateBuilder.timerTasks)
	s.Empty(s.stateBuilder.newRunTimerTasks)
	s.Empty(s.stateBuilder.newRunTransferTasks)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeSignalExternalWorkflowExecutionFailed() {
	version := int64(1)
	requestID := uuid.New()
	domainID := validDomainID
	execution := shared.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(validRunID),
	}

	now := time.Now()
	evenType := shared.EventTypeSignalExternalWorkflowExecutionFailed
	event := &shared.HistoryEvent{
		Version:   common.Int64Ptr(version),
		EventId:   common.Int64Ptr(130),
		Timestamp: common.Int64Ptr(now.UnixNano()),
		EventType: &evenType,
		SignalExternalWorkflowExecutionFailedEventAttributes: &shared.SignalExternalWorkflowExecutionFailedEventAttributes{},
	}
	s.mockMutableState.On("ReplicateSignalExternalWorkflowExecutionFailedEvent", event).Once()
	s.mockUpdateVersion(event)

	s.stateBuilder.applyEvents(domainID, requestID, execution, s.toHistory(event), nil, 0, 0)

	s.Empty(s.stateBuilder.timerTasks)
	s.Empty(s.stateBuilder.transferTasks)
	s.Empty(s.stateBuilder.newRunTimerTasks)
	s.Empty(s.stateBuilder.newRunTransferTasks)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeRequestCancelExternalWorkflowExecutionInitiated() {
	version := int64(1)
	requestID := uuid.New()
	domainID := validDomainID
	execution := shared.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(validRunID),
	}
	targetDomainID := uuid.New()
	targetWorkflowID := "some random target workflow ID"
	targetRunID := uuid.New()
	childWorkflowOnly := true
	targetDomain := "some random target domain name"

	now := time.Now()
	cancellationRequestID := uuid.New()
	control := []byte("some random control")
	evenType := shared.EventTypeRequestCancelExternalWorkflowExecutionInitiated
	event := &shared.HistoryEvent{
		Version:   common.Int64Ptr(version),
		EventId:   common.Int64Ptr(130),
		Timestamp: common.Int64Ptr(now.UnixNano()),
		EventType: &evenType,
		RequestCancelExternalWorkflowExecutionInitiatedEventAttributes: &shared.RequestCancelExternalWorkflowExecutionInitiatedEventAttributes{
			Domain: common.StringPtr(targetDomain),
			WorkflowExecution: &shared.WorkflowExecution{
				WorkflowId: common.StringPtr(targetWorkflowID),
				RunId:      common.StringPtr(targetRunID),
			},
			ChildWorkflowOnly: common.BoolPtr(childWorkflowOnly),
			Control:           control,
		},
	}
	rci := &persistence.RequestCancelInfo{
		Version:         event.GetVersion(),
		InitiatedID:     event.GetEventId(),
		CancelRequestID: cancellationRequestID,
	}

	// the cancellation request ID is generated inside, cannot assert equal
	s.mockMutableState.On("ReplicateRequestCancelExternalWorkflowExecutionInitiatedEvent", event, mock.Anything).Return(rci).Once()
	s.mockMetadataMgr.On("GetDomain", &persistence.GetDomainRequest{Name: targetDomain}).Return(
		&persistence.GetDomainResponse{
			Info:   &persistence.DomainInfo{ID: targetDomainID, Name: targetDomain},
			Config: &persistence.DomainConfig{Retention: 1},
			ReplicationConfig: &persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				},
			},
			IsGlobalDomain: true,
			TableVersion:   persistence.DomainTableVersionV1,
		}, nil,
	).Once()
	s.mockUpdateVersion(event)

	s.stateBuilder.applyEvents(domainID, requestID, execution, s.toHistory(event), nil, 0, 0)

	s.Equal([]persistence.Task{&persistence.CancelExecutionTask{
		TargetDomainID:          targetDomainID,
		TargetWorkflowID:        targetWorkflowID,
		TargetRunID:             targetRunID,
		TargetChildWorkflowOnly: childWorkflowOnly,
		InitiatedID:             event.GetEventId(),
	}}, s.stateBuilder.transferTasks)

	s.Empty(s.stateBuilder.timerTasks)
	s.Empty(s.stateBuilder.newRunTimerTasks)
	s.Empty(s.stateBuilder.newRunTransferTasks)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeRequestCancelExternalWorkflowExecutionFailed() {
	version := int64(1)
	requestID := uuid.New()
	domainID := validDomainID
	execution := shared.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(validRunID),
	}

	now := time.Now()
	evenType := shared.EventTypeRequestCancelExternalWorkflowExecutionFailed
	event := &shared.HistoryEvent{
		Version:   common.Int64Ptr(version),
		EventId:   common.Int64Ptr(130),
		Timestamp: common.Int64Ptr(now.UnixNano()),
		EventType: &evenType,
		RequestCancelActivityTaskFailedEventAttributes: &shared.RequestCancelActivityTaskFailedEventAttributes{},
	}
	s.mockMutableState.On("ReplicateRequestCancelExternalWorkflowExecutionFailedEvent", event).Once()
	s.mockUpdateVersion(event)

	s.stateBuilder.applyEvents(domainID, requestID, execution, s.toHistory(event), nil, 0, 0)

	s.Empty(s.stateBuilder.timerTasks)
	s.Empty(s.stateBuilder.transferTasks)
	s.Empty(s.stateBuilder.newRunTimerTasks)
	s.Empty(s.stateBuilder.newRunTransferTasks)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeRequestCancelActivityTaskFailed() {
	version := int64(1)
	requestID := uuid.New()
	domainID := validDomainID
	execution := shared.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(validRunID),
	}

	now := time.Now()
	evenType := shared.EventTypeRequestCancelActivityTaskFailed
	event := &shared.HistoryEvent{
		Version:   common.Int64Ptr(version),
		EventId:   common.Int64Ptr(130),
		Timestamp: common.Int64Ptr(now.UnixNano()),
		EventType: &evenType,
		RequestCancelActivityTaskFailedEventAttributes: &shared.RequestCancelActivityTaskFailedEventAttributes{},
	}
	s.mockUpdateVersion(event)

	s.stateBuilder.applyEvents(domainID, requestID, execution, s.toHistory(event), nil, 0, 0)

	s.Empty(s.stateBuilder.timerTasks)
	s.Empty(s.stateBuilder.transferTasks)
	s.Empty(s.stateBuilder.newRunTimerTasks)
	s.Empty(s.stateBuilder.newRunTransferTasks)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeMarkerRecorded() {
	version := int64(1)
	requestID := uuid.New()
	domainID := validDomainID
	execution := shared.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(validRunID),
	}

	now := time.Now()
	evenType := shared.EventTypeMarkerRecorded
	event := &shared.HistoryEvent{
		Version:                       common.Int64Ptr(version),
		EventId:                       common.Int64Ptr(130),
		Timestamp:                     common.Int64Ptr(now.UnixNano()),
		EventType:                     &evenType,
		MarkerRecordedEventAttributes: &shared.MarkerRecordedEventAttributes{},
	}
	s.mockUpdateVersion(event)

	s.stateBuilder.applyEvents(domainID, requestID, execution, s.toHistory(event), nil, 0, 0)

	s.Empty(s.stateBuilder.timerTasks)
	s.Empty(s.stateBuilder.transferTasks)
	s.Empty(s.stateBuilder.newRunTimerTasks)
	s.Empty(s.stateBuilder.newRunTransferTasks)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeExternalWorkflowExecutionSignaled() {
	version := int64(1)
	requestID := uuid.New()
	domainID := validDomainID
	execution := shared.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(validRunID),
	}

	now := time.Now()
	evenType := shared.EventTypeExternalWorkflowExecutionSignaled
	event := &shared.HistoryEvent{
		Version:   common.Int64Ptr(version),
		EventId:   common.Int64Ptr(130),
		Timestamp: common.Int64Ptr(now.UnixNano()),
		EventType: &evenType,
		ExternalWorkflowExecutionSignaledEventAttributes: &shared.ExternalWorkflowExecutionSignaledEventAttributes{},
	}
	s.mockMutableState.On("ReplicateExternalWorkflowExecutionSignaled", event).Once()
	s.mockUpdateVersion(event)

	s.stateBuilder.applyEvents(domainID, requestID, execution, s.toHistory(event), nil, 0, 0)

	s.Empty(s.stateBuilder.timerTasks)
	s.Empty(s.stateBuilder.transferTasks)
	s.Empty(s.stateBuilder.newRunTimerTasks)
	s.Empty(s.stateBuilder.newRunTransferTasks)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeExternalWorkflowExecutionCancelRequested() {
	version := int64(1)
	requestID := uuid.New()
	domainID := validDomainID
	execution := shared.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(validRunID),
	}

	now := time.Now()
	evenType := shared.EventTypeExternalWorkflowExecutionCancelRequested
	event := &shared.HistoryEvent{
		Version:   common.Int64Ptr(version),
		EventId:   common.Int64Ptr(130),
		Timestamp: common.Int64Ptr(now.UnixNano()),
		EventType: &evenType,
		ExternalWorkflowExecutionCancelRequestedEventAttributes: &shared.ExternalWorkflowExecutionCancelRequestedEventAttributes{},
	}
	s.mockMutableState.On("ReplicateExternalWorkflowExecutionCancelRequested", event).Once()
	s.mockUpdateVersion(event)

	s.stateBuilder.applyEvents(domainID, requestID, execution, s.toHistory(event), nil, 0, 0)

	s.Empty(s.stateBuilder.timerTasks)
	s.Empty(s.stateBuilder.transferTasks)
	s.Empty(s.stateBuilder.newRunTimerTasks)
	s.Empty(s.stateBuilder.newRunTransferTasks)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeDecisionTaskTimedOut() {
	version := int64(1)
	requestID := uuid.New()
	domainID := validDomainID
	execution := shared.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(validRunID),
	}

	now := time.Now()
	scheduleID := int64(12)
	startedID := int64(28)
	evenType := shared.EventTypeDecisionTaskTimedOut
	event := &shared.HistoryEvent{
		Version:   common.Int64Ptr(version),
		EventId:   common.Int64Ptr(130),
		Timestamp: common.Int64Ptr(now.UnixNano()),
		EventType: &evenType,
		DecisionTaskTimedOutEventAttributes: &shared.DecisionTaskTimedOutEventAttributes{
			ScheduledEventId: common.Int64Ptr(scheduleID),
			StartedEventId:   common.Int64Ptr(startedID),
			TimeoutType:      shared.TimeoutTypeStartToClose.Ptr(),
		},
	}
	s.mockMutableState.On("ReplicateDecisionTaskTimedOutEvent", shared.TimeoutTypeStartToClose).Once()
	tasklist := "some random tasklist"
	newScheduleID := int64(233)
	executionInfo := &persistence.WorkflowExecutionInfo{
		TaskList: tasklist,
	}
	s.mockMutableState.On("GetExecutionInfo").Return(executionInfo)
	s.mockMutableState.On("ReplicateTransientDecisionTaskScheduled").Return(&decisionInfo{
		Version:    version,
		ScheduleID: newScheduleID,
		TaskList:   tasklist,
	})
	s.mockUpdateVersion(event)

	s.stateBuilder.applyEvents(domainID, requestID, execution, s.toHistory(event), nil, 0, 0)

	s.Equal([]persistence.Task{&persistence.DecisionTask{
		DomainID:   domainID,
		TaskList:   tasklist,
		ScheduleID: newScheduleID,
	}}, s.stateBuilder.transferTasks)

	s.Empty(s.stateBuilder.timerTasks)
	s.Empty(s.stateBuilder.newRunTimerTasks)
	s.Empty(s.stateBuilder.newRunTransferTasks)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeDecisionTaskStarted() {
	version := int64(1)
	requestID := uuid.New()
	domainID := validDomainID
	execution := shared.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(validRunID),
	}

	now := time.Now()
	tasklist := "some random tasklist"
	timeoutSecond := int32(11)
	scheduleID := int64(111)
	decisionRequestID := uuid.New()
	evenType := shared.EventTypeDecisionTaskStarted
	event := &shared.HistoryEvent{
		Version:   common.Int64Ptr(version),
		EventId:   common.Int64Ptr(130),
		Timestamp: common.Int64Ptr(now.UnixNano()),
		EventType: &evenType,
		DecisionTaskStartedEventAttributes: &shared.DecisionTaskStartedEventAttributes{
			ScheduledEventId: common.Int64Ptr(scheduleID),
			RequestId:        common.StringPtr(decisionRequestID),
		},
	}
	di := &decisionInfo{
		Version:         event.GetVersion(),
		ScheduleID:      scheduleID,
		StartedID:       event.GetEventId(),
		RequestID:       decisionRequestID,
		DecisionTimeout: timeoutSecond,
		TaskList:        tasklist,
		Attempt:         0,
	}
	s.mockMutableState.On("ReplicateDecisionTaskStartedEvent",
		(*decisionInfo)(nil), event.GetVersion(), scheduleID, event.GetEventId(), decisionRequestID, event.GetTimestamp()).Return(di).Once()
	s.mockMutableState.On("UpdateDecision", di).Once()
	s.mockUpdateVersion(event)

	s.stateBuilder.applyEvents(domainID, requestID, execution, s.toHistory(event), nil, 0, 0)

	s.Equal(1, len(s.stateBuilder.timerTasks))
	timerTask, ok := s.stateBuilder.timerTasks[0].(*persistence.DecisionTimeoutTask)
	s.True(ok)
	s.True(timerTask.VisibilityTimestamp.Equal(now.Add(time.Duration(timeoutSecond) * time.Second)))
	s.Equal(di.ScheduleID, timerTask.EventID)

	s.Empty(s.stateBuilder.transferTasks)
	s.Empty(s.stateBuilder.newRunTimerTasks)
	s.Empty(s.stateBuilder.newRunTransferTasks)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeDecisionTaskScheduled() {
	version := int64(1)
	requestID := uuid.New()
	domainID := validDomainID
	execution := shared.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(validRunID),
	}

	now := time.Now()
	tasklist := "some random tasklist"
	timeoutSecond := int32(11)
	evenType := shared.EventTypeDecisionTaskScheduled
	decisionAttempt := int64(111)
	event := &shared.HistoryEvent{
		Version:   common.Int64Ptr(version),
		EventId:   common.Int64Ptr(130),
		Timestamp: common.Int64Ptr(now.UnixNano()),
		EventType: &evenType,
		DecisionTaskScheduledEventAttributes: &shared.DecisionTaskScheduledEventAttributes{
			TaskList:                   &shared.TaskList{Name: common.StringPtr(tasklist)},
			StartToCloseTimeoutSeconds: common.Int32Ptr(timeoutSecond),
			Attempt:                    common.Int64Ptr(decisionAttempt),
		},
	}
	di := &decisionInfo{
		Version:         event.GetVersion(),
		ScheduleID:      event.GetEventId(),
		StartedID:       common.EmptyEventID,
		RequestID:       emptyUUID,
		DecisionTimeout: timeoutSecond,
		TaskList:        tasklist,
		Attempt:         decisionAttempt,
	}
	executionInfo := &persistence.WorkflowExecutionInfo{
		TaskList: tasklist,
	}
	s.mockMutableState.On("GetExecutionInfo").Return(executionInfo)
	s.mockMutableState.On("ReplicateDecisionTaskScheduledEvent",
		event.GetVersion(), event.GetEventId(), tasklist, timeoutSecond, decisionAttempt,
	).Return(di).Once()
	s.mockMutableState.On("UpdateDecision", di).Once()
	s.mockUpdateVersion(event)

	s.stateBuilder.applyEvents(domainID, requestID, execution, s.toHistory(event), nil, 0, 0)

	s.Equal([]persistence.Task{&persistence.DecisionTask{
		DomainID:   domainID,
		TaskList:   tasklist,
		ScheduleID: event.GetEventId(),
	}}, s.stateBuilder.transferTasks)

	s.Empty(s.stateBuilder.timerTasks)
	s.Empty(s.stateBuilder.newRunTimerTasks)
	s.Empty(s.stateBuilder.newRunTransferTasks)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeDecisionTaskFailed() {
	version := int64(1)
	requestID := uuid.New()
	domainID := validDomainID
	execution := shared.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(validRunID),
	}

	now := time.Now()
	scheduleID := int64(12)
	startedID := int64(28)
	evenType := shared.EventTypeDecisionTaskFailed
	event := &shared.HistoryEvent{
		Version:   common.Int64Ptr(version),
		EventId:   common.Int64Ptr(130),
		Timestamp: common.Int64Ptr(now.UnixNano()),
		EventType: &evenType,
		DecisionTaskFailedEventAttributes: &shared.DecisionTaskFailedEventAttributes{
			ScheduledEventId: common.Int64Ptr(scheduleID),
			StartedEventId:   common.Int64Ptr(startedID),
		},
	}
	s.mockMutableState.On("ReplicateDecisionTaskFailedEvent").Once()
	tasklist := "some random tasklist"
	newScheduleID := int64(233)
	executionInfo := &persistence.WorkflowExecutionInfo{
		TaskList: tasklist,
	}
	s.mockMutableState.On("GetExecutionInfo").Return(executionInfo)
	s.mockMutableState.On("ReplicateTransientDecisionTaskScheduled").Return(&decisionInfo{
		Version:    version,
		ScheduleID: newScheduleID,
		TaskList:   tasklist,
	})
	s.mockUpdateVersion(event)

	s.stateBuilder.applyEvents(domainID, requestID, execution, s.toHistory(event), nil, 0, 0)

	s.Equal([]persistence.Task{&persistence.DecisionTask{
		DomainID:   domainID,
		TaskList:   tasklist,
		ScheduleID: newScheduleID,
	}}, s.stateBuilder.transferTasks)

	s.Empty(s.stateBuilder.timerTasks)
	s.Empty(s.stateBuilder.newRunTimerTasks)
	s.Empty(s.stateBuilder.newRunTransferTasks)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeDecisionTaskCompleted() {
	version := int64(1)
	requestID := uuid.New()
	domainID := validDomainID
	execution := shared.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(validRunID),
	}

	now := time.Now()
	scheduleID := int64(12)
	startedID := int64(28)
	evenType := shared.EventTypeDecisionTaskCompleted
	event := &shared.HistoryEvent{
		Version:   common.Int64Ptr(version),
		EventId:   common.Int64Ptr(130),
		Timestamp: common.Int64Ptr(now.UnixNano()),
		EventType: &evenType,
		DecisionTaskCompletedEventAttributes: &shared.DecisionTaskCompletedEventAttributes{
			ScheduledEventId: common.Int64Ptr(scheduleID),
			StartedEventId:   common.Int64Ptr(startedID),
		},
	}
	s.mockMutableState.On("ReplicateDecisionTaskCompletedEvent", scheduleID, startedID).Once()
	s.mockUpdateVersion(event)

	s.stateBuilder.applyEvents(domainID, requestID, execution, s.toHistory(event), nil, 0, 0)

	s.Empty(s.stateBuilder.timerTasks)
	s.Empty(s.stateBuilder.transferTasks)
	s.Empty(s.stateBuilder.newRunTimerTasks)
	s.Empty(s.stateBuilder.newRunTransferTasks)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeChildWorkflowExecutionTimedOut() {
	version := int64(1)
	requestID := uuid.New()
	domainID := validDomainID
	execution := shared.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(validRunID),
	}

	now := time.Now()
	evenType := shared.EventTypeChildWorkflowExecutionTimedOut
	event := &shared.HistoryEvent{
		Version:   common.Int64Ptr(version),
		EventId:   common.Int64Ptr(130),
		Timestamp: common.Int64Ptr(now.UnixNano()),
		EventType: &evenType,
		ChildWorkflowExecutionTimedOutEventAttributes: &shared.ChildWorkflowExecutionTimedOutEventAttributes{},
	}
	s.mockMutableState.On("ReplicateChildWorkflowExecutionTimedOutEvent", event).Once()
	s.mockUpdateVersion(event)

	s.stateBuilder.applyEvents(domainID, requestID, execution, s.toHistory(event), nil, 0, 0)

	s.Empty(s.stateBuilder.timerTasks)
	s.Empty(s.stateBuilder.transferTasks)
	s.Empty(s.stateBuilder.newRunTimerTasks)
	s.Empty(s.stateBuilder.newRunTransferTasks)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeChildWorkflowExecutionTerminated() {
	version := int64(1)
	requestID := uuid.New()
	domainID := validDomainID
	execution := shared.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(validRunID),
	}

	now := time.Now()
	evenType := shared.EventTypeChildWorkflowExecutionTerminated
	event := &shared.HistoryEvent{
		Version:   common.Int64Ptr(version),
		EventId:   common.Int64Ptr(130),
		Timestamp: common.Int64Ptr(now.UnixNano()),
		EventType: &evenType,
		ChildWorkflowExecutionTerminatedEventAttributes: &shared.ChildWorkflowExecutionTerminatedEventAttributes{},
	}
	s.mockMutableState.On("ReplicateChildWorkflowExecutionTerminatedEvent", event).Once()
	s.mockUpdateVersion(event)

	s.stateBuilder.applyEvents(domainID, requestID, execution, s.toHistory(event), nil, 0, 0)

	s.Empty(s.stateBuilder.timerTasks)
	s.Empty(s.stateBuilder.transferTasks)
	s.Empty(s.stateBuilder.newRunTimerTasks)
	s.Empty(s.stateBuilder.newRunTransferTasks)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeChildWorkflowExecutionStarted() {
	version := int64(1)
	requestID := uuid.New()
	domainID := validDomainID
	execution := shared.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(validRunID),
	}

	now := time.Now()
	evenType := shared.EventTypeChildWorkflowExecutionStarted
	event := &shared.HistoryEvent{
		Version:   common.Int64Ptr(version),
		EventId:   common.Int64Ptr(130),
		Timestamp: common.Int64Ptr(now.UnixNano()),
		EventType: &evenType,
		ChildWorkflowExecutionStartedEventAttributes: &shared.ChildWorkflowExecutionStartedEventAttributes{},
	}
	s.mockMutableState.On("ReplicateChildWorkflowExecutionStartedEvent", event).Return(nil).Once()
	s.mockUpdateVersion(event)

	s.stateBuilder.applyEvents(domainID, requestID, execution, s.toHistory(event), nil, 0, 0)

	s.Empty(s.stateBuilder.timerTasks)
	s.Empty(s.stateBuilder.transferTasks)
	s.Empty(s.stateBuilder.newRunTimerTasks)
	s.Empty(s.stateBuilder.newRunTransferTasks)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeChildWorkflowExecutionFailed() {
	version := int64(1)
	requestID := uuid.New()
	domainID := validDomainID
	execution := shared.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(validRunID),
	}

	now := time.Now()
	evenType := shared.EventTypeChildWorkflowExecutionFailed
	event := &shared.HistoryEvent{
		Version:   common.Int64Ptr(version),
		EventId:   common.Int64Ptr(130),
		Timestamp: common.Int64Ptr(now.UnixNano()),
		EventType: &evenType,
		ChildWorkflowExecutionFailedEventAttributes: &shared.ChildWorkflowExecutionFailedEventAttributes{},
	}
	s.mockMutableState.On("ReplicateChildWorkflowExecutionFailedEvent", event).Once()
	s.mockUpdateVersion(event)

	s.stateBuilder.applyEvents(domainID, requestID, execution, s.toHistory(event), nil, 0, 0)

	s.Empty(s.stateBuilder.timerTasks)
	s.Empty(s.stateBuilder.transferTasks)
	s.Empty(s.stateBuilder.newRunTimerTasks)
	s.Empty(s.stateBuilder.newRunTransferTasks)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeChildWorkflowExecutionCompleted() {
	version := int64(1)
	requestID := uuid.New()
	domainID := validDomainID
	execution := shared.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(validRunID),
	}

	now := time.Now()
	evenType := shared.EventTypeChildWorkflowExecutionCompleted
	event := &shared.HistoryEvent{
		Version:   common.Int64Ptr(version),
		EventId:   common.Int64Ptr(130),
		Timestamp: common.Int64Ptr(now.UnixNano()),
		EventType: &evenType,
		ChildWorkflowExecutionCompletedEventAttributes: &shared.ChildWorkflowExecutionCompletedEventAttributes{},
	}
	s.mockMutableState.On("ReplicateChildWorkflowExecutionCompletedEvent", event).Once()
	s.mockUpdateVersion(event)

	s.stateBuilder.applyEvents(domainID, requestID, execution, s.toHistory(event), nil, 0, 0)

	s.Empty(s.stateBuilder.timerTasks)
	s.Empty(s.stateBuilder.transferTasks)
	s.Empty(s.stateBuilder.newRunTimerTasks)
	s.Empty(s.stateBuilder.newRunTransferTasks)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeChildWorkflowExecutionCanceled() {
	version := int64(1)
	requestID := uuid.New()
	domainID := validDomainID
	execution := shared.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(validRunID),
	}

	now := time.Now()
	evenType := shared.EventTypeChildWorkflowExecutionCanceled
	event := &shared.HistoryEvent{
		Version:   common.Int64Ptr(version),
		EventId:   common.Int64Ptr(130),
		Timestamp: common.Int64Ptr(now.UnixNano()),
		EventType: &evenType,
		ChildWorkflowExecutionCanceledEventAttributes: &shared.ChildWorkflowExecutionCanceledEventAttributes{},
	}
	s.mockMutableState.On("ReplicateChildWorkflowExecutionCanceledEvent", event).Once()
	s.mockUpdateVersion(event)

	s.stateBuilder.applyEvents(domainID, requestID, execution, s.toHistory(event), nil, 0, 0)

	s.Empty(s.stateBuilder.timerTasks)
	s.Empty(s.stateBuilder.transferTasks)
	s.Empty(s.stateBuilder.newRunTimerTasks)
	s.Empty(s.stateBuilder.newRunTransferTasks)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeCancelTimerFailed() {
	version := int64(1)
	requestID := uuid.New()
	domainID := validDomainID
	execution := shared.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(validRunID),
	}

	now := time.Now()
	evenType := shared.EventTypeCancelTimerFailed
	event := &shared.HistoryEvent{
		Version:                          common.Int64Ptr(version),
		EventId:                          common.Int64Ptr(130),
		Timestamp:                        common.Int64Ptr(now.UnixNano()),
		EventType:                        &evenType,
		CancelTimerFailedEventAttributes: &shared.CancelTimerFailedEventAttributes{},
	}
	s.mockUpdateVersion(event)

	s.stateBuilder.applyEvents(domainID, requestID, execution, s.toHistory(event), nil, 0, 0)

	s.Empty(s.stateBuilder.timerTasks)
	s.Empty(s.stateBuilder.transferTasks)
	s.Empty(s.stateBuilder.newRunTimerTasks)
	s.Empty(s.stateBuilder.newRunTransferTasks)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeActivityTaskTimedOut() {
	version := int64(1)
	requestID := uuid.New()
	domainID := validDomainID
	execution := shared.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(validRunID),
	}

	now := time.Now()
	evenType := shared.EventTypeActivityTaskTimedOut
	event := &shared.HistoryEvent{
		Version:                             common.Int64Ptr(version),
		EventId:                             common.Int64Ptr(130),
		Timestamp:                           common.Int64Ptr(now.UnixNano()),
		EventType:                           &evenType,
		ActivityTaskTimedOutEventAttributes: &shared.ActivityTaskTimedOutEventAttributes{},
	}

	// this is the remaining activity
	// should create a activity timer for this
	timeoutSecond := int32(10)
	ai := &persistence.ActivityInfo{
		Version:                  version,
		ScheduleID:               123,
		ScheduledEvent:           nil,
		ScheduledTime:            now,
		StartedID:                124,
		StartedEvent:             nil,
		StartedTime:              now,
		ActivityID:               "some random activity ID",
		ScheduleToStartTimeout:   timeoutSecond,
		ScheduleToCloseTimeout:   timeoutSecond,
		StartToCloseTimeout:      timeoutSecond,
		HeartbeatTimeout:         timeoutSecond,
		CancelRequested:          false,
		CancelRequestID:          common.EmptyEventID,
		LastHeartBeatUpdatedTime: time.Time{},
		TimerTaskStatus:          TimerTaskStatusNone,
		TaskList:                 "some random tasklist",
	}
	s.mockMutableState.On("GetPendingActivityInfos").Return(map[int64]*persistence.ActivityInfo{ai.ScheduleID: ai})
	s.mockMutableState.On("UpdateActivity", ai).Return(nil).Once()
	s.mockMutableState.On("ReplicateActivityTaskTimedOutEvent", event).Return(nil).Once()
	s.mockUpdateVersion(event)

	s.stateBuilder.applyEvents(domainID, requestID, execution, s.toHistory(event), nil, 0, 0)
	s.Equal(1, len(s.stateBuilder.timerTasks))
	timerTask, ok := s.stateBuilder.timerTasks[0].(*persistence.ActivityTimeoutTask)
	s.True(ok)
	s.True(timerTask.VisibilityTimestamp.Equal(now.Add(time.Duration(timeoutSecond) * time.Second)))
	s.Equal(ai.ScheduleID, timerTask.EventID)
	s.Empty(s.stateBuilder.transferTasks)
	s.Empty(s.stateBuilder.newRunTimerTasks)
	s.Empty(s.stateBuilder.newRunTransferTasks)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeActivityTaskStarted() {
	version := int64(1)
	requestID := uuid.New()
	domainID := validDomainID
	execution := shared.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(validRunID),
	}

	now := time.Now()
	activityID := "activity ID"
	tasklist := "some random tasklist"
	timeoutSecond := int32(10)
	evenType := shared.EventTypeActivityTaskScheduled
	scheduledEvent := &shared.HistoryEvent{
		Version:                              common.Int64Ptr(version),
		EventId:                              common.Int64Ptr(130),
		Timestamp:                            common.Int64Ptr(now.UnixNano()),
		EventType:                            &evenType,
		ActivityTaskScheduledEventAttributes: &shared.ActivityTaskScheduledEventAttributes{},
	}

	evenType = shared.EventTypeActivityTaskStarted
	startedEvent := &shared.HistoryEvent{
		Version:                            common.Int64Ptr(version),
		EventId:                            common.Int64Ptr(scheduledEvent.GetEventId() + 1),
		Timestamp:                          common.Int64Ptr(scheduledEvent.GetTimestamp() + 1000),
		EventType:                          &evenType,
		ActivityTaskStartedEventAttributes: &shared.ActivityTaskStartedEventAttributes{},
	}

	ai := &persistence.ActivityInfo{
		Version:                  version,
		ScheduleID:               scheduledEvent.GetEventId(),
		ScheduledEvent:           scheduledEvent,
		ScheduledTime:            time.Unix(0, scheduledEvent.GetTimestamp()),
		StartedID:                startedEvent.GetEventId(),
		StartedEvent:             startedEvent,
		StartedTime:              time.Unix(0, startedEvent.GetTimestamp()),
		ActivityID:               activityID,
		ScheduleToStartTimeout:   timeoutSecond,
		ScheduleToCloseTimeout:   timeoutSecond,
		StartToCloseTimeout:      timeoutSecond,
		HeartbeatTimeout:         timeoutSecond,
		CancelRequested:          false,
		CancelRequestID:          common.EmptyEventID,
		LastHeartBeatUpdatedTime: time.Time{},
		TimerTaskStatus:          TimerTaskStatusNone,
		TaskList:                 tasklist,
	}
	executionInfo := &persistence.WorkflowExecutionInfo{
		TaskList: tasklist,
	}
	s.mockMutableState.On("GetExecutionInfo").Return(executionInfo)
	s.mockMutableState.On("GetPendingActivityInfos").Return(map[int64]*persistence.ActivityInfo{scheduledEvent.GetEventId(): ai})
	s.mockMutableState.On("UpdateActivity", ai).Return(nil).Once()
	s.mockMutableState.On("ReplicateActivityTaskStartedEvent", startedEvent).Once()
	s.mockUpdateVersion(startedEvent)

	s.stateBuilder.applyEvents(domainID, requestID, execution, s.toHistory(startedEvent), nil, 0, 0)
	s.Equal(1, len(s.stateBuilder.timerTasks))
	timerTask, ok := s.stateBuilder.timerTasks[0].(*persistence.ActivityTimeoutTask)
	s.True(ok)
	s.True(timerTask.VisibilityTimestamp.Equal(now.Add(time.Duration(timeoutSecond) * time.Second)))
	s.Equal(ai.ScheduleID, timerTask.EventID)
	s.Empty(s.stateBuilder.transferTasks)

	s.Empty(s.stateBuilder.newRunTimerTasks)
	s.Empty(s.stateBuilder.newRunTransferTasks)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeActivityTaskScheduled() {
	version := int64(1)
	requestID := uuid.New()
	domainID := validDomainID
	execution := shared.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(validRunID),
	}

	now := time.Now()
	activityID := "activity ID"
	tasklist := "some random tasklist"
	timeoutSecond := int32(10)
	evenType := shared.EventTypeActivityTaskScheduled
	event := &shared.HistoryEvent{
		Version:                              common.Int64Ptr(version),
		EventId:                              common.Int64Ptr(130),
		Timestamp:                            common.Int64Ptr(now.UnixNano()),
		EventType:                            &evenType,
		ActivityTaskScheduledEventAttributes: &shared.ActivityTaskScheduledEventAttributes{},
	}

	ai := &persistence.ActivityInfo{
		Version:                  event.GetVersion(),
		ScheduleID:               event.GetEventId(),
		ScheduledEvent:           event,
		ScheduledTime:            time.Unix(0, event.GetTimestamp()),
		StartedID:                common.EmptyEventID,
		StartedTime:              time.Time{},
		ActivityID:               activityID,
		ScheduleToStartTimeout:   timeoutSecond,
		ScheduleToCloseTimeout:   timeoutSecond,
		StartToCloseTimeout:      timeoutSecond,
		HeartbeatTimeout:         timeoutSecond,
		CancelRequested:          false,
		CancelRequestID:          common.EmptyEventID,
		LastHeartBeatUpdatedTime: time.Time{},
		TimerTaskStatus:          TimerTaskStatusNone,
		TaskList:                 tasklist,
	}
	executionInfo := &persistence.WorkflowExecutionInfo{
		TaskList: tasklist,
	}
	s.mockMutableState.On("GetExecutionInfo").Return(executionInfo)
	s.mockMutableState.On("GetPendingActivityInfos").Return(map[int64]*persistence.ActivityInfo{event.GetEventId(): ai})
	s.mockMutableState.On("UpdateActivity", ai).Return(nil).Once()
	s.mockMutableState.On("ReplicateActivityTaskScheduledEvent", event).Return(ai).Once()
	s.mockUpdateVersion(event)

	s.stateBuilder.applyEvents(domainID, requestID, execution, s.toHistory(event), nil, 0, 0)
	s.Equal(1, len(s.stateBuilder.timerTasks))
	timerTask, ok := s.stateBuilder.timerTasks[0].(*persistence.ActivityTimeoutTask)
	s.True(ok)
	s.True(timerTask.VisibilityTimestamp.Equal(now.Add(time.Duration(timeoutSecond) * time.Second)))
	s.Equal(ai.ScheduleID, timerTask.EventID)
	s.Equal([]persistence.Task{&persistence.ActivityTask{
		DomainID:   domainID,
		TaskList:   tasklist,
		ScheduleID: event.GetEventId(),
	}}, s.stateBuilder.transferTasks)

	s.Empty(s.stateBuilder.newRunTimerTasks)
	s.Empty(s.stateBuilder.newRunTransferTasks)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeActivityTaskFailed() {
	version := int64(1)
	requestID := uuid.New()
	domainID := validDomainID
	execution := shared.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(validRunID),
	}

	now := time.Now()
	evenType := shared.EventTypeActivityTaskFailed
	event := &shared.HistoryEvent{
		Version:                           common.Int64Ptr(version),
		EventId:                           common.Int64Ptr(130),
		Timestamp:                         common.Int64Ptr(now.UnixNano()),
		EventType:                         &evenType,
		ActivityTaskFailedEventAttributes: &shared.ActivityTaskFailedEventAttributes{},
	}

	// this is the remaining activity
	// should create a activity timer for this
	timeoutSecond := int32(10)
	ai := &persistence.ActivityInfo{
		Version:                  version,
		ScheduleID:               123,
		ScheduledEvent:           nil,
		ScheduledTime:            now,
		StartedID:                124,
		StartedEvent:             nil,
		StartedTime:              now,
		ActivityID:               "some random activity ID",
		ScheduleToStartTimeout:   timeoutSecond,
		ScheduleToCloseTimeout:   timeoutSecond,
		StartToCloseTimeout:      timeoutSecond,
		HeartbeatTimeout:         timeoutSecond,
		CancelRequested:          false,
		CancelRequestID:          common.EmptyEventID,
		LastHeartBeatUpdatedTime: time.Time{},
		TimerTaskStatus:          TimerTaskStatusNone,
		TaskList:                 "some random tasklist",
	}
	s.mockMutableState.On("GetPendingActivityInfos").Return(map[int64]*persistence.ActivityInfo{ai.ScheduleID: ai})
	s.mockMutableState.On("UpdateActivity", ai).Return(nil).Once()
	s.mockMutableState.On("ReplicateActivityTaskFailedEvent", event).Return(nil).Once()
	s.mockUpdateVersion(event)

	s.stateBuilder.applyEvents(domainID, requestID, execution, s.toHistory(event), nil, 0, 0)
	s.Equal(1, len(s.stateBuilder.timerTasks))
	timerTask, ok := s.stateBuilder.timerTasks[0].(*persistence.ActivityTimeoutTask)
	s.True(ok)
	s.True(timerTask.VisibilityTimestamp.Equal(now.Add(time.Duration(timeoutSecond) * time.Second)))
	s.Equal(ai.ScheduleID, timerTask.EventID)
	s.Empty(s.stateBuilder.transferTasks)
	s.Empty(s.stateBuilder.newRunTimerTasks)
	s.Empty(s.stateBuilder.newRunTransferTasks)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeActivityTaskCompleted() {
	version := int64(1)
	requestID := uuid.New()
	domainID := validDomainID
	execution := shared.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(validRunID),
	}

	now := time.Now()
	evenType := shared.EventTypeActivityTaskCompleted
	event := &shared.HistoryEvent{
		Version:                              common.Int64Ptr(version),
		EventId:                              common.Int64Ptr(130),
		Timestamp:                            common.Int64Ptr(now.UnixNano()),
		EventType:                            &evenType,
		ActivityTaskCompletedEventAttributes: &shared.ActivityTaskCompletedEventAttributes{},
	}

	// this is the remaining activity
	// should create a activity timer for this
	timeoutSecond := int32(10)
	ai := &persistence.ActivityInfo{
		Version:                  version,
		ScheduleID:               123,
		ScheduledEvent:           nil,
		ScheduledTime:            now,
		StartedID:                124,
		StartedEvent:             nil,
		StartedTime:              now,
		ActivityID:               "some random activity ID",
		ScheduleToStartTimeout:   timeoutSecond,
		ScheduleToCloseTimeout:   timeoutSecond,
		StartToCloseTimeout:      timeoutSecond,
		HeartbeatTimeout:         timeoutSecond,
		CancelRequested:          false,
		CancelRequestID:          common.EmptyEventID,
		LastHeartBeatUpdatedTime: time.Time{},
		TimerTaskStatus:          TimerTaskStatusNone,
		TaskList:                 "some random tasklist",
	}
	s.mockMutableState.On("GetPendingActivityInfos").Return(map[int64]*persistence.ActivityInfo{ai.ScheduleID: ai})
	s.mockMutableState.On("UpdateActivity", ai).Return(nil).Once()
	s.mockMutableState.On("ReplicateActivityTaskCompletedEvent", event).Return(nil).Once()
	s.mockUpdateVersion(event)

	s.stateBuilder.applyEvents(domainID, requestID, execution, s.toHistory(event), nil, 0, 0)
	s.Equal(1, len(s.stateBuilder.timerTasks))
	timerTask, ok := s.stateBuilder.timerTasks[0].(*persistence.ActivityTimeoutTask)
	s.True(ok)
	s.True(timerTask.VisibilityTimestamp.Equal(now.Add(time.Duration(timeoutSecond) * time.Second)))
	s.Equal(ai.ScheduleID, timerTask.EventID)
	s.Empty(s.stateBuilder.transferTasks)
	s.Empty(s.stateBuilder.newRunTimerTasks)
	s.Empty(s.stateBuilder.newRunTransferTasks)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeActivityTaskCanceled() {
	version := int64(1)
	requestID := uuid.New()
	domainID := validDomainID
	execution := shared.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(validRunID),
	}

	now := time.Now()
	evenType := shared.EventTypeActivityTaskCanceled
	event := &shared.HistoryEvent{
		Version:                             common.Int64Ptr(version),
		EventId:                             common.Int64Ptr(130),
		Timestamp:                           common.Int64Ptr(now.UnixNano()),
		EventType:                           &evenType,
		ActivityTaskCanceledEventAttributes: &shared.ActivityTaskCanceledEventAttributes{},
	}

	// this is the remaining activity
	// should create a activity timer for this
	timeoutSecond := int32(10)
	ai := &persistence.ActivityInfo{
		Version:                  version,
		ScheduleID:               123,
		ScheduledEvent:           nil,
		ScheduledTime:            now,
		StartedID:                124,
		StartedEvent:             nil,
		StartedTime:              now,
		ActivityID:               "some random activity ID",
		ScheduleToStartTimeout:   timeoutSecond,
		ScheduleToCloseTimeout:   timeoutSecond,
		StartToCloseTimeout:      timeoutSecond,
		HeartbeatTimeout:         timeoutSecond,
		CancelRequested:          false,
		CancelRequestID:          common.EmptyEventID,
		LastHeartBeatUpdatedTime: time.Time{},
		TimerTaskStatus:          TimerTaskStatusNone,
		TaskList:                 "some random tasklist",
	}
	s.mockMutableState.On("GetPendingActivityInfos").Return(map[int64]*persistence.ActivityInfo{ai.ScheduleID: ai})
	s.mockMutableState.On("UpdateActivity", ai).Return(nil).Once()
	s.mockMutableState.On("ReplicateActivityTaskCanceledEvent", event).Return(nil).Once()
	s.mockUpdateVersion(event)

	s.stateBuilder.applyEvents(domainID, requestID, execution, s.toHistory(event), nil, 0, 0)
	s.Equal(1, len(s.stateBuilder.timerTasks))
	timerTask, ok := s.stateBuilder.timerTasks[0].(*persistence.ActivityTimeoutTask)
	s.True(ok)
	s.True(timerTask.VisibilityTimestamp.Equal(now.Add(time.Duration(timeoutSecond) * time.Second)))
	s.Equal(ai.ScheduleID, timerTask.EventID)
	s.Empty(s.stateBuilder.transferTasks)
	s.Empty(s.stateBuilder.newRunTimerTasks)
	s.Empty(s.stateBuilder.newRunTransferTasks)
}

func (s *stateBuilderSuite) TestApplyEvents_EventTypeActivityTaskCancelRequested() {
	version := int64(1)
	requestID := uuid.New()
	domainID := validDomainID
	execution := shared.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(validRunID),
	}

	now := time.Now()
	evenType := shared.EventTypeActivityTaskCancelRequested
	event := &shared.HistoryEvent{
		Version:   common.Int64Ptr(version),
		EventId:   common.Int64Ptr(130),
		Timestamp: common.Int64Ptr(now.UnixNano()),
		EventType: &evenType,
		ActivityTaskCancelRequestedEventAttributes: &shared.ActivityTaskCancelRequestedEventAttributes{},
	}
	s.mockMutableState.On("ReplicateActivityTaskCancelRequestedEvent", event).Once()
	s.mockUpdateVersion(event)

	s.stateBuilder.applyEvents(domainID, requestID, execution, s.toHistory(event), nil, 0, 0)

	s.Empty(s.stateBuilder.timerTasks)
	s.Empty(s.stateBuilder.transferTasks)
	s.Empty(s.stateBuilder.newRunTimerTasks)
	s.Empty(s.stateBuilder.newRunTransferTasks)
}
