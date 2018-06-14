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
	"reflect"
	"testing"
	"time"

	"github.com/pborman/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"github.com/uber-common/bark"
	"github.com/uber-go/tally"
	h "github.com/uber/cadence/.gen/go/history"
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
	historyReplicatorSuite struct {
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

		historyReplicator *historyReplicator
	}
)

func TestHistoryReplicatorSuite(t *testing.T) {
	s := new(historyReplicatorSuite)
	suite.Run(t, s)
}

func (s *historyReplicatorSuite) SetupSuite() {
	if testing.Verbose() {
		log.SetOutput(os.Stdout)
	}

}

func (s *historyReplicatorSuite) TearDownSuite() {

}

func (s *historyReplicatorSuite) SetupTest() {
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
		domainCache:               cache.NewDomainCache(s.mockMetadataMgr, s.mockClusterMetadata, s.logger),
		metricsClient:             metrics.NewClient(tally.NoopScope, metrics.History),
	}
	s.mockMutableState = &mockMutableState{}
	historyCache := newHistoryCache(s.mockShard, s.logger)
	s.mockClusterMetadata.On("IsGlobalDomainEnabled").Return(true)
	s.mockClusterMetadata.On("GetCurrentClusterName").Return(cluster.TestCurrentClusterName)
	h := &historyEngineImpl{
		currentClusterName: s.mockShard.GetService().GetClusterMetadata().GetCurrentClusterName(),
		shard:              s.mockShard,
		historyMgr:         s.mockHistoryMgr,
		executionManager:   s.mockExecutionMgr,
		historyCache:       historyCache,
		logger:             s.logger,
		tokenSerializer:    common.NewJSONTaskTokenSerializer(),
		hSerializerFactory: persistence.NewHistorySerializerFactory(),
		metricsClient:      s.mockShard.GetMetricsClient(),
	}
	s.historyReplicator = newHistoryReplicator(s.mockShard, h, historyCache, s.mockShard.domainCache, s.mockHistoryMgr, s.logger)
}

func (s *historyReplicatorSuite) TearDownTest() {
	s.historyReplicator = nil
	s.mockHistoryMgr.AssertExpectations(s.T())
	s.mockExecutionMgr.AssertExpectations(s.T())
	s.mockShardManager.AssertExpectations(s.T())
	s.mockProducer.AssertExpectations(s.T())
	s.mockMetadataMgr.AssertExpectations(s.T())
}

func (s *historyReplicatorSuite) TestApplyStartEvent() {

}

func (s *historyReplicatorSuite) TestApplyOtherEventsMissingMutableState_IncomingNotLessThanCurrent() {
	domainName := "some random domain name"
	domainID := validDomainID
	workflowID := "some random workflow ID"
	version := int64(123)
	currentRunID := uuid.New()
	currentVersion := version - 100

	s.mockMetadataMgr.On("GetDomain", &persistence.GetDomainRequest{ID: domainID}).Return(
		&persistence.GetDomainResponse{
			Info:   &persistence.DomainInfo{ID: domainID, Name: domainName},
			Config: &persistence.DomainConfig{Retention: 1},
			ReplicationConfig: &persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				},
			},
			FailoverVersion: currentVersion,
			IsGlobalDomain:  true,
			TableVersion:    persistence.DomainTableVersionV1,
		}, nil,
	).Once()
	s.mockExecutionMgr.On("GetCurrentExecution", &persistence.GetCurrentExecutionRequest{
		DomainID:   domainID,
		WorkflowID: workflowID,
	}).Return(&persistence.GetCurrentExecutionResponse{
		RunID: currentRunID,
		// other attributes are not used
	}, nil)
	s.mockExecutionMgr.On("GetWorkflowExecution", &persistence.GetWorkflowExecutionRequest{
		DomainID: domainID,
		Execution: shared.WorkflowExecution{
			WorkflowId: common.StringPtr(workflowID),
			RunId:      common.StringPtr(currentRunID),
		},
	}).Return(&persistence.GetWorkflowExecutionResponse{
		State: &persistence.WorkflowMutableState{
			ExecutionInfo:    &persistence.WorkflowExecutionInfo{RunID: currentRunID},
			ReplicationState: &persistence.ReplicationState{LastWriteVersion: currentVersion},
		},
	}, nil)

	err := s.historyReplicator.ApplyOtherEventsMissingMutableState(domainID, workflowID, version, s.logger)
	s.Equal(ErrRetryEntityNotExists, err)
}

func (s *historyReplicatorSuite) TestApplyOtherEventsMissingMutableState_IncomingLessThanCurrent() {
	domainName := "some random domain name"
	domainID := validDomainID
	workflowID := "some random workflow ID"
	version := int64(123)
	currentRunID := uuid.New()
	currentVersion := version + 100

	s.mockMetadataMgr.On("GetDomain", &persistence.GetDomainRequest{ID: domainID}).Return(
		&persistence.GetDomainResponse{
			Info:   &persistence.DomainInfo{ID: domainID, Name: domainName},
			Config: &persistence.DomainConfig{Retention: 1},
			ReplicationConfig: &persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				},
			},
			FailoverVersion: currentVersion,
			IsGlobalDomain:  true,
			TableVersion:    persistence.DomainTableVersionV1,
		}, nil,
	).Once()
	s.mockExecutionMgr.On("GetCurrentExecution", &persistence.GetCurrentExecutionRequest{
		DomainID:   domainID,
		WorkflowID: workflowID,
	}).Return(&persistence.GetCurrentExecutionResponse{
		RunID: currentRunID,
		// other attributes are not used
	}, nil)
	s.mockExecutionMgr.On("GetWorkflowExecution", &persistence.GetWorkflowExecutionRequest{
		DomainID: domainID,
		Execution: shared.WorkflowExecution{
			WorkflowId: common.StringPtr(workflowID),
			RunId:      common.StringPtr(currentRunID),
		},
	}).Return(&persistence.GetWorkflowExecutionResponse{
		State: &persistence.WorkflowMutableState{
			ExecutionInfo:    &persistence.WorkflowExecutionInfo{RunID: currentRunID},
			ReplicationState: &persistence.ReplicationState{LastWriteVersion: currentVersion},
		},
	}, nil)

	err := s.historyReplicator.ApplyOtherEventsMissingMutableState(domainID, workflowID, version, s.logger)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestApplyOtherEventsVersionChecking_IncomingLessThanCurrent() {
	domainID := validDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	incomingVersion := int64(110)
	currentLastWriteVersion := int64(123)

	context := newWorkflowExecutionContext(domainID, shared.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(runID),
	}, s.mockShard, s.mockExecutionMgr, s.logger)
	msBuilderIn := &mockMutableState{}
	context.msBuilder = msBuilderIn
	request := &h.ReplicateEventsRequest{
		Version: common.Int64Ptr(incomingVersion),
	}
	msBuilderIn.On("GetReplicationState").Return(&persistence.ReplicationState{LastWriteVersion: currentLastWriteVersion})

	msBuilderOut, err := s.historyReplicator.ApplyOtherEventsVersionChecking(context, msBuilderIn, request, s.logger)
	s.Nil(msBuilderOut)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestApplyOtherEventsVersionChecking_IncomingEqualToCurrent() {
	domainID := validDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	incomingVersion := int64(110)
	currentLastWriteVersion := incomingVersion

	context := newWorkflowExecutionContext(domainID, shared.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(runID),
	}, s.mockShard, s.mockExecutionMgr, s.logger)
	msBuilderIn := &mockMutableState{}
	context.msBuilder = msBuilderIn
	request := &h.ReplicateEventsRequest{
		Version: common.Int64Ptr(incomingVersion),
	}
	msBuilderIn.On("GetReplicationState").Return(&persistence.ReplicationState{LastWriteVersion: currentLastWriteVersion})

	msBuilderOut, err := s.historyReplicator.ApplyOtherEventsVersionChecking(context, msBuilderIn, request, s.logger)
	s.Equal(msBuilderIn, msBuilderOut)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestApplyOtherEventsVersionChecking_IncomingGreaterThanCurrent_ResolveConflict() {
	domainID := validDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()

	currentLastWriteVersion := int64(10)
	currentLastEventID := int64(98)
	incomingVersion := currentLastWriteVersion + 10
	incomingLastEventID := currentLastEventID - 10

	prevActiveCluster := cluster.TestAlternativeClusterName
	context := newWorkflowExecutionContext(domainID, shared.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(runID),
	}, s.mockShard, s.mockExecutionMgr, s.logger)
	msBuilderIn := &mockMutableState{}
	context.msBuilder = msBuilderIn

	request := &h.ReplicateEventsRequest{
		Version: common.Int64Ptr(incomingVersion),
		ReplicationInfo: map[string]*h.ReplicationInfo{prevActiveCluster: &h.ReplicationInfo{
			LastEventId: common.Int64Ptr(incomingLastEventID),
		}},
	}
	startTimeStamp := time.Now()
	msBuilderIn.On("GetReplicationState").Return(&persistence.ReplicationState{
		LastWriteVersion: currentLastWriteVersion,
		LastWriteEventID: currentLastEventID,
	})
	msBuilderIn.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{StartTimestamp: startTimeStamp})
	msBuilderIn.On("IsWorkflowExecutionRunning").Return(true)
	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", currentLastWriteVersion).Return(prevActiveCluster)

	mockConflictResolver := &mockConflictResolver{}
	s.historyReplicator.getNewConflictResolver = func(context *workflowExecutionContext, logger bark.Logger) conflictResolver {
		return mockConflictResolver
	}
	msBuilderMid := &mockMutableState{}
	msBuilderMid.On("GetNextEventID").Return(int64(12345)) // this is used by log
	mockConflictResolver.On("reset", mock.Anything, incomingLastEventID, startTimeStamp).Return(msBuilderMid, nil)
	msBuilderOut, err := s.historyReplicator.ApplyOtherEventsVersionChecking(context, msBuilderIn, request, s.logger)
	s.Equal(msBuilderMid, msBuilderOut)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestApplyOtherEventsVersionChecking_IncomingGreaterThanCurrent_ResolveConflict_OtherCase() {
	// other cases will be tested in TestConflictResolutionTerminateContinueAsNew
}

func (s *historyReplicatorSuite) TestApplyOtherEventsVersionChecking_IncomingGreaterThanCurrent_NoOp() {
	domainID := validDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()

	currentLastWriteVersion := int64(10)
	currentLastEventID := int64(98)
	incomingVersion := currentLastWriteVersion + 10
	incomingLastEventID := currentLastEventID

	prevActiveCluster := cluster.TestAlternativeClusterName
	context := newWorkflowExecutionContext(domainID, shared.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(runID),
	}, s.mockShard, s.mockExecutionMgr, s.logger)
	msBuilderIn := &mockMutableState{}
	context.msBuilder = msBuilderIn

	request := &h.ReplicateEventsRequest{
		Version: common.Int64Ptr(incomingVersion),
		ReplicationInfo: map[string]*h.ReplicationInfo{prevActiveCluster: &h.ReplicationInfo{
			LastEventId: common.Int64Ptr(incomingLastEventID),
		}},
	}
	startTimeStamp := time.Now()
	msBuilderIn.On("GetReplicationState").Return(&persistence.ReplicationState{
		LastWriteVersion: currentLastWriteVersion,
		LastWriteEventID: currentLastEventID,
	})
	msBuilderIn.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{StartTimestamp: startTimeStamp})
	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", currentLastWriteVersion).Return(prevActiveCluster)

	msBuilderOut, err := s.historyReplicator.ApplyOtherEventsVersionChecking(context, msBuilderIn, request, s.logger)
	s.Equal(msBuilderIn, msBuilderOut)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestApplyOtherEventsVersionChecking_IncomingGreaterThanCurrent_Err() {
	domainID := validDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()

	currentLastWriteVersion := int64(10)
	currentLastEventID := int64(98)
	incomingVersion := currentLastWriteVersion + 10
	incomingLastEventID := currentLastEventID + 10

	prevActiveCluster := cluster.TestAlternativeClusterName
	context := newWorkflowExecutionContext(domainID, shared.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(runID),
	}, s.mockShard, s.mockExecutionMgr, s.logger)
	msBuilderIn := &mockMutableState{}
	context.msBuilder = msBuilderIn

	request := &h.ReplicateEventsRequest{
		Version: common.Int64Ptr(incomingVersion),
		ReplicationInfo: map[string]*h.ReplicationInfo{prevActiveCluster: &h.ReplicationInfo{
			LastEventId: common.Int64Ptr(incomingLastEventID),
		}},
	}
	startTimeStamp := time.Now()
	msBuilderIn.On("GetReplicationState").Return(&persistence.ReplicationState{
		LastWriteVersion: currentLastWriteVersion,
		LastWriteEventID: currentLastEventID,
	})
	msBuilderIn.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{StartTimestamp: startTimeStamp})
	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", currentLastWriteVersion).Return(prevActiveCluster)

	msBuilderOut, err := s.historyReplicator.ApplyOtherEventsVersionChecking(context, msBuilderIn, request, s.logger)
	s.Nil(msBuilderOut)
	s.Equal(ErrCorruptedReplicationInfo, err)
}

func (s *historyReplicatorSuite) TestApplyOtherEvents_IncomingLessThanCurrent() {
	domainID := validDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()

	currentNextEventID := int64(10)
	incomingFirstEventID := currentNextEventID - 4

	context := newWorkflowExecutionContext(domainID, shared.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(runID),
	}, s.mockShard, s.mockExecutionMgr, s.logger)
	msBuilder := &mockMutableState{}
	context.msBuilder = msBuilder

	request := &h.ReplicateEventsRequest{
		FirstEventId: common.Int64Ptr(incomingFirstEventID),
	}
	msBuilder.On("GetNextEventID").Return(currentNextEventID)
	msBuilder.On("GetReplicationState").Return(&persistence.ReplicationState{}) // logger will use this

	err := s.historyReplicator.ApplyOtherEvents(context, msBuilder, request, s.logger)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestApplyOtherEvents_IncomingEqualToCurrent() {
	// TODO
}

func (s *historyReplicatorSuite) TestApplyOtherEvents_IncomingGreaterThanCurrent() {
	domainID := validDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()

	currentNextEventID := int64(10)
	incomingFirstEventID := currentNextEventID + 4

	context := newWorkflowExecutionContext(domainID, shared.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(runID),
	}, s.mockShard, s.mockExecutionMgr, s.logger)
	msBuilder := &mockMutableState{}
	context.msBuilder = msBuilder

	request := &h.ReplicateEventsRequest{
		FirstEventId: common.Int64Ptr(incomingFirstEventID),
	}
	msBuilder.On("GetNextEventID").Return(currentNextEventID)
	msBuilder.On("GetReplicationState").Return(&persistence.ReplicationState{}) // logger will use this
	msBuilder.On("BufferReplicationTask", request).Return(nil).Once()

	err := s.historyReplicator.ApplyOtherEvents(context, msBuilder, request, s.logger)
	s.Nil(err)
}

func (s *historyReplicatorSuite) ApplyReplicationTask() {
	// TODO
}

func (s *historyReplicatorSuite) FlushBuffer() {
	// TODO
}

func (s *historyReplicatorSuite) TestReplicateWorkflowStarted_BrandNew() {
	domainID := validDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	version := int64(144)
	tasklist := "some random tasklist"
	workflowType := "some random workflow type"
	workflowTimeout := int32(3721)
	decisionTimeout := int32(4411)

	initiatedID := int64(4810)
	parentDomainID := validDomainID
	parentWorkflowID := "some random workflow ID"
	parentRunID := uuid.New()
	sourceCluster := "some random source cluster"

	context := newWorkflowExecutionContext(domainID, shared.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(runID),
	}, s.mockShard, s.mockExecutionMgr, s.logger)
	msBuilder := &mockMutableState{}
	context.msBuilder = msBuilder
	di := &decisionInfo{
		Version:         version,
		ScheduleID:      common.FirstEventID + 1,
		StartedID:       common.EmptyEventID,
		DecisionTimeout: decisionTimeout,
		TaskList:        tasklist,
	}
	sBuilder := &mockStateBuilder{}
	requestID := uuid.New()
	history := &shared.History{
		Events: []*shared.HistoryEvent{
			&shared.HistoryEvent{Version: common.Int64Ptr(version), EventId: common.Int64Ptr(1)},
			&shared.HistoryEvent{Version: common.Int64Ptr(version), EventId: common.Int64Ptr(2)},
		},
	}
	nextEventID := di.ScheduleID + 1
	replicationState := &persistence.ReplicationState{
		StartVersion:     version,
		CurrentVersion:   version,
		LastWriteVersion: version,
		LastWriteEventID: nextEventID - 1,
	}
	transferTasks := []persistence.Task{&persistence.CloseExecutionTask{}}
	timerTasks := []persistence.Task{&persistence.DeleteHistoryEventTask{}}

	msBuilder.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{
		CreateRequestID:      requestID,
		DomainID:             domainID,
		WorkflowID:           workflowID,
		RunID:                runID,
		InitiatedID:          initiatedID,
		ParentDomainID:       parentDomainID,
		ParentWorkflowID:     parentWorkflowID,
		ParentRunID:          parentRunID,
		TaskList:             tasklist,
		WorkflowTypeName:     workflowType,
		WorkflowTimeout:      workflowTimeout,
		DecisionTimeoutValue: decisionTimeout,
	})
	msBuilder.On("UpdateReplicationStateLastEventID", sourceCluster, version, nextEventID-1).Once()
	msBuilder.On("GetReplicationState").Return(replicationState)
	msBuilder.On("GetCurrentVersion").Return(version)
	msBuilder.On("GetNextEventID").Return(nextEventID)
	s.mockHistoryMgr.On("AppendHistoryEvents", mock.Anything).Return(nil).Once()
	sBuilder.On("getTransferTasks").Return(transferTasks)
	sBuilder.On("getTimerTasks").Return(timerTasks)
	s.mockExecutionMgr.On("CreateWorkflowExecution", mock.MatchedBy(func(input *persistence.CreateWorkflowExecutionRequest) bool {
		input.RangeID = 0
		s.Equal(&persistence.CreateWorkflowExecutionRequest{
			RequestID: requestID,
			DomainID:  domainID,
			Execution: shared.WorkflowExecution{
				WorkflowId: common.StringPtr(workflowID),
				RunId:      common.StringPtr(runID),
			},
			ParentDomainID: parentDomainID,
			ParentExecution: &shared.WorkflowExecution{
				WorkflowId: common.StringPtr(parentWorkflowID),
				RunId:      common.StringPtr(parentRunID),
			},
			InitiatedID:                 initiatedID,
			TaskList:                    tasklist,
			WorkflowTypeName:            workflowType,
			WorkflowTimeout:             workflowTimeout,
			DecisionTimeoutValue:        decisionTimeout,
			NextEventID:                 msBuilder.GetNextEventID(),
			LastProcessedEvent:          common.EmptyEventID,
			TransferTasks:               transferTasks,
			DecisionVersion:             di.Version,
			DecisionScheduleID:          di.ScheduleID,
			DecisionStartedID:           di.StartedID,
			DecisionStartToCloseTimeout: di.DecisionTimeout,
			TimerTasks:                  timerTasks,
			ContinueAsNew:               false,
			PreviousRunID:               "",
			ReplicationState:            replicationState,
		}, input)
		return true
	})).Return(&persistence.CreateWorkflowExecutionResponse{}, nil).Once()

	err := s.historyReplicator.replicateWorkflowStarted(context, msBuilder, di, sourceCluster, history, sBuilder, s.logger)
	s.Nil(err)
	s.Equal(1, len(transferTasks))
	s.Equal(version, transferTasks[0].GetVersion())
	s.Equal(1, len(timerTasks))
	s.Equal(version, timerTasks[0].GetVersion())
}

func (s *historyReplicatorSuite) TestReplicateWorkflowStarted_ISE() {
	domainID := validDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	version := int64(144)
	tasklist := "some random tasklist"
	workflowType := "some random workflow type"
	workflowTimeout := int32(3721)
	decisionTimeout := int32(4411)

	initiatedID := int64(4810)
	parentDomainID := validDomainID
	parentWorkflowID := "some random workflow ID"
	parentRunID := uuid.New()
	sourceCluster := "some random source cluster"

	context := newWorkflowExecutionContext(domainID, shared.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(runID),
	}, s.mockShard, s.mockExecutionMgr, s.logger)
	msBuilder := &mockMutableState{}
	context.msBuilder = msBuilder
	di := &decisionInfo{
		Version:         version,
		ScheduleID:      common.FirstEventID + 1,
		StartedID:       common.EmptyEventID,
		DecisionTimeout: decisionTimeout,
		TaskList:        tasklist,
	}
	sBuilder := &mockStateBuilder{}
	requestID := uuid.New()
	history := &shared.History{
		Events: []*shared.HistoryEvent{
			&shared.HistoryEvent{Version: common.Int64Ptr(version), EventId: common.Int64Ptr(1)},
			&shared.HistoryEvent{Version: common.Int64Ptr(version), EventId: common.Int64Ptr(2)},
		},
	}
	nextEventID := di.ScheduleID + 1
	replicationState := &persistence.ReplicationState{
		StartVersion:     version,
		CurrentVersion:   version,
		LastWriteVersion: version,
		LastWriteEventID: nextEventID - 1,
	}
	transferTasks := []persistence.Task{&persistence.CloseExecutionTask{}}
	timerTasks := []persistence.Task{&persistence.DeleteHistoryEventTask{}}

	msBuilder.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{
		CreateRequestID:      requestID,
		DomainID:             domainID,
		WorkflowID:           workflowID,
		RunID:                runID,
		InitiatedID:          initiatedID,
		ParentDomainID:       parentDomainID,
		ParentWorkflowID:     parentWorkflowID,
		ParentRunID:          parentRunID,
		TaskList:             tasklist,
		WorkflowTypeName:     workflowType,
		WorkflowTimeout:      workflowTimeout,
		DecisionTimeoutValue: decisionTimeout,
	})
	msBuilder.On("UpdateReplicationStateLastEventID", sourceCluster, version, nextEventID-1).Once()
	msBuilder.On("GetReplicationState").Return(replicationState)
	msBuilder.On("GetCurrentVersion").Return(version)
	msBuilder.On("GetNextEventID").Return(nextEventID)
	s.mockHistoryMgr.On("AppendHistoryEvents", mock.Anything).Return(nil).Once()
	sBuilder.On("getTransferTasks").Return(transferTasks)
	sBuilder.On("getTimerTasks").Return(timerTasks)
	s.mockShardManager.On("UpdateShard", mock.Anything).Return(nil).Once() // this is called when err is returned, and shard will try to update
	s.mockHistoryMgr.On("DeleteWorkflowExecutionHistory", mock.Anything).Return(nil).Once()

	errRet := &shared.InternalServiceError{}
	// the test above already assert the create workflow request, so here jsut use anyting
	s.mockExecutionMgr.On("CreateWorkflowExecution", mock.Anything).Return(nil, errRet).Once()

	err := s.historyReplicator.replicateWorkflowStarted(context, msBuilder, di, sourceCluster, history, sBuilder, s.logger)
	s.Equal(errRet, err)
	s.Equal(1, len(transferTasks))
	s.Equal(version, transferTasks[0].GetVersion())
	s.Equal(1, len(timerTasks))
	s.Equal(version, timerTasks[0].GetVersion())
}

func (s *historyReplicatorSuite) TestReplicateWorkflowStarted_SameRunID() {
	domainID := validDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	version := int64(144)
	tasklist := "some random tasklist"
	workflowType := "some random workflow type"
	workflowTimeout := int32(3721)
	decisionTimeout := int32(4411)

	initiatedID := int64(4810)
	parentDomainID := validDomainID
	parentWorkflowID := "some random workflow ID"
	parentRunID := uuid.New()
	sourceCluster := "some random source cluster"

	context := newWorkflowExecutionContext(domainID, shared.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(runID),
	}, s.mockShard, s.mockExecutionMgr, s.logger)
	msBuilder := &mockMutableState{}
	context.msBuilder = msBuilder
	di := &decisionInfo{
		Version:         version,
		ScheduleID:      common.FirstEventID + 1,
		StartedID:       common.EmptyEventID,
		DecisionTimeout: decisionTimeout,
		TaskList:        tasklist,
	}
	sBuilder := &mockStateBuilder{}
	requestID := uuid.New()
	history := &shared.History{
		Events: []*shared.HistoryEvent{
			&shared.HistoryEvent{Version: common.Int64Ptr(version), EventId: common.Int64Ptr(1)},
			&shared.HistoryEvent{Version: common.Int64Ptr(version), EventId: common.Int64Ptr(2)},
		},
	}
	nextEventID := di.ScheduleID + 1
	replicationState := &persistence.ReplicationState{
		StartVersion:     version,
		CurrentVersion:   version,
		LastWriteVersion: version,
		LastWriteEventID: nextEventID - 1,
	}
	transferTasks := []persistence.Task{&persistence.CloseExecutionTask{}}
	timerTasks := []persistence.Task{&persistence.DeleteHistoryEventTask{}}

	msBuilder.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{
		CreateRequestID:      requestID,
		DomainID:             domainID,
		WorkflowID:           workflowID,
		RunID:                runID,
		InitiatedID:          initiatedID,
		ParentDomainID:       parentDomainID,
		ParentWorkflowID:     parentWorkflowID,
		ParentRunID:          parentRunID,
		TaskList:             tasklist,
		WorkflowTypeName:     workflowType,
		WorkflowTimeout:      workflowTimeout,
		DecisionTimeoutValue: decisionTimeout,
	})
	msBuilder.On("UpdateReplicationStateLastEventID", sourceCluster, version, nextEventID-1).Once()
	msBuilder.On("GetReplicationState").Return(replicationState)
	msBuilder.On("GetCurrentVersion").Return(version)
	msBuilder.On("GetNextEventID").Return(nextEventID)
	s.mockHistoryMgr.On("AppendHistoryEvents", mock.Anything).Return(nil).Once()
	sBuilder.On("getTransferTasks").Return(transferTasks)
	sBuilder.On("getTimerTasks").Return(timerTasks)

	currentVersion := version
	currentRunID := runID
	currentState := persistence.WorkflowStateRunning
	errRet := &persistence.WorkflowExecutionAlreadyStartedError{
		RunID:        currentRunID,
		State:        currentState,
		StartVersion: currentVersion,
	}
	// the test above already assert the create workflow request, so here jsut use anyting
	s.mockExecutionMgr.On("CreateWorkflowExecution", mock.Anything).Return(nil, errRet).Once()

	err := s.historyReplicator.replicateWorkflowStarted(context, msBuilder, di, sourceCluster, history, sBuilder, s.logger)
	s.Nil(err)
	s.Equal(1, len(transferTasks))
	s.Equal(version, transferTasks[0].GetVersion())
	s.Equal(1, len(timerTasks))
	s.Equal(version, timerTasks[0].GetVersion())
}

func (s *historyReplicatorSuite) TestReplicateWorkflowStarted_CurrentComplete_IncomingLessThanCurrent() {
	domainID := validDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	version := int64(144)
	tasklist := "some random tasklist"
	workflowType := "some random workflow type"
	workflowTimeout := int32(3721)
	decisionTimeout := int32(4411)

	initiatedID := int64(4810)
	parentDomainID := validDomainID
	parentWorkflowID := "some random workflow ID"
	parentRunID := uuid.New()
	sourceCluster := "some random source cluster"

	context := newWorkflowExecutionContext(domainID, shared.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(runID),
	}, s.mockShard, s.mockExecutionMgr, s.logger)
	msBuilder := &mockMutableState{}
	context.msBuilder = msBuilder
	di := &decisionInfo{
		Version:         version,
		ScheduleID:      common.FirstEventID + 1,
		StartedID:       common.EmptyEventID,
		DecisionTimeout: decisionTimeout,
		TaskList:        tasklist,
	}
	sBuilder := &mockStateBuilder{}
	requestID := uuid.New()
	history := &shared.History{
		Events: []*shared.HistoryEvent{
			&shared.HistoryEvent{Version: common.Int64Ptr(version), EventId: common.Int64Ptr(1)},
			&shared.HistoryEvent{Version: common.Int64Ptr(version), EventId: common.Int64Ptr(2)},
		},
	}
	nextEventID := di.ScheduleID + 1
	replicationState := &persistence.ReplicationState{
		StartVersion:     version,
		CurrentVersion:   version,
		LastWriteVersion: version,
		LastWriteEventID: nextEventID - 1,
	}
	transferTasks := []persistence.Task{&persistence.CloseExecutionTask{}}
	timerTasks := []persistence.Task{&persistence.DeleteHistoryEventTask{}}

	msBuilder.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{
		CreateRequestID:      requestID,
		DomainID:             domainID,
		WorkflowID:           workflowID,
		RunID:                runID,
		InitiatedID:          initiatedID,
		ParentDomainID:       parentDomainID,
		ParentWorkflowID:     parentWorkflowID,
		ParentRunID:          parentRunID,
		TaskList:             tasklist,
		WorkflowTypeName:     workflowType,
		WorkflowTimeout:      workflowTimeout,
		DecisionTimeoutValue: decisionTimeout,
	})
	msBuilder.On("UpdateReplicationStateLastEventID", sourceCluster, version, nextEventID-1).Once()
	msBuilder.On("GetReplicationState").Return(replicationState)
	msBuilder.On("GetCurrentVersion").Return(version)
	msBuilder.On("GetNextEventID").Return(nextEventID)
	s.mockHistoryMgr.On("AppendHistoryEvents", mock.Anything).Return(nil).Once()
	sBuilder.On("getTransferTasks").Return(transferTasks)
	sBuilder.On("getTimerTasks").Return(timerTasks)

	currentVersion := version + 1
	currentRunID := uuid.New()
	currentState := persistence.WorkflowStateCompleted
	errRet := &persistence.WorkflowExecutionAlreadyStartedError{
		RunID:        currentRunID,
		State:        currentState,
		StartVersion: currentVersion,
	}
	// the test above already assert the create workflow request, so here jsut use anyting
	s.mockExecutionMgr.On("CreateWorkflowExecution", mock.Anything).Return(nil, errRet).Once()
	s.mockHistoryMgr.On("DeleteWorkflowExecutionHistory", mock.Anything).Return(nil).Once()

	err := s.historyReplicator.replicateWorkflowStarted(context, msBuilder, di, sourceCluster, history, sBuilder, s.logger)
	s.Nil(err)
	s.Equal(1, len(transferTasks))
	s.Equal(version, transferTasks[0].GetVersion())
	s.Equal(1, len(timerTasks))
	s.Equal(version, timerTasks[0].GetVersion())
}

func (s *historyReplicatorSuite) TestReplicateWorkflowStarted_CurrentComplete_IncomingEqualToThanCurrent() {
	domainID := validDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	version := int64(144)
	tasklist := "some random tasklist"
	workflowType := "some random workflow type"
	workflowTimeout := int32(3721)
	decisionTimeout := int32(4411)

	initiatedID := int64(4810)
	parentDomainID := validDomainID
	parentWorkflowID := "some random workflow ID"
	parentRunID := uuid.New()
	sourceCluster := "some random source cluster"

	context := newWorkflowExecutionContext(domainID, shared.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(runID),
	}, s.mockShard, s.mockExecutionMgr, s.logger)
	msBuilder := &mockMutableState{}
	context.msBuilder = msBuilder
	di := &decisionInfo{
		Version:         version,
		ScheduleID:      common.FirstEventID + 1,
		StartedID:       common.EmptyEventID,
		DecisionTimeout: decisionTimeout,
		TaskList:        tasklist,
	}
	sBuilder := &mockStateBuilder{}
	requestID := uuid.New()
	history := &shared.History{
		Events: []*shared.HistoryEvent{
			&shared.HistoryEvent{Version: common.Int64Ptr(version), EventId: common.Int64Ptr(1)},
			&shared.HistoryEvent{Version: common.Int64Ptr(version), EventId: common.Int64Ptr(2)},
		},
	}
	nextEventID := di.ScheduleID + 1
	replicationState := &persistence.ReplicationState{
		StartVersion:     version,
		CurrentVersion:   version,
		LastWriteVersion: version,
		LastWriteEventID: nextEventID - 1,
	}
	transferTasks := []persistence.Task{&persistence.CloseExecutionTask{}}
	timerTasks := []persistence.Task{&persistence.DeleteHistoryEventTask{}}

	msBuilder.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{
		CreateRequestID:      requestID,
		DomainID:             domainID,
		WorkflowID:           workflowID,
		RunID:                runID,
		InitiatedID:          initiatedID,
		ParentDomainID:       parentDomainID,
		ParentWorkflowID:     parentWorkflowID,
		ParentRunID:          parentRunID,
		TaskList:             tasklist,
		WorkflowTypeName:     workflowType,
		WorkflowTimeout:      workflowTimeout,
		DecisionTimeoutValue: decisionTimeout,
	})
	msBuilder.On("UpdateReplicationStateLastEventID", sourceCluster, version, nextEventID-1).Once()
	msBuilder.On("GetReplicationState").Return(replicationState)
	msBuilder.On("GetCurrentVersion").Return(version)
	msBuilder.On("GetNextEventID").Return(nextEventID)
	s.mockHistoryMgr.On("AppendHistoryEvents", mock.Anything).Return(nil).Once()
	sBuilder.On("getTransferTasks").Return(transferTasks)
	sBuilder.On("getTimerTasks").Return(timerTasks)

	currentVersion := version
	currentRunID := uuid.New()
	currentState := persistence.WorkflowStateCompleted
	errRet := &persistence.WorkflowExecutionAlreadyStartedError{
		RunID:        currentRunID,
		State:        currentState,
		StartVersion: currentVersion,
	}
	// the test above already assert the create workflow request, so here jsut use anyting
	s.mockExecutionMgr.On("CreateWorkflowExecution", mock.MatchedBy(func(input *persistence.CreateWorkflowExecutionRequest) bool {
		input.RangeID = 0
		return reflect.DeepEqual(&persistence.CreateWorkflowExecutionRequest{
			RequestID: requestID,
			DomainID:  domainID,
			Execution: shared.WorkflowExecution{
				WorkflowId: common.StringPtr(workflowID),
				RunId:      common.StringPtr(runID),
			},
			ParentDomainID: parentDomainID,
			ParentExecution: &shared.WorkflowExecution{
				WorkflowId: common.StringPtr(parentWorkflowID),
				RunId:      common.StringPtr(parentRunID),
			},
			InitiatedID:                 initiatedID,
			TaskList:                    tasklist,
			WorkflowTypeName:            workflowType,
			WorkflowTimeout:             workflowTimeout,
			DecisionTimeoutValue:        decisionTimeout,
			NextEventID:                 msBuilder.GetNextEventID(),
			LastProcessedEvent:          common.EmptyEventID,
			TransferTasks:               transferTasks,
			DecisionVersion:             di.Version,
			DecisionScheduleID:          di.ScheduleID,
			DecisionStartedID:           di.StartedID,
			DecisionStartToCloseTimeout: di.DecisionTimeout,
			TimerTasks:                  timerTasks,
			ContinueAsNew:               false,
			PreviousRunID:               "",
			ReplicationState:            replicationState,
		}, input)
	})).Return(nil, errRet).Once()
	s.mockExecutionMgr.On("CreateWorkflowExecution", mock.MatchedBy(func(input *persistence.CreateWorkflowExecutionRequest) bool {
		input.RangeID = 0
		return reflect.DeepEqual(&persistence.CreateWorkflowExecutionRequest{
			RequestID: requestID,
			DomainID:  domainID,
			Execution: shared.WorkflowExecution{
				WorkflowId: common.StringPtr(workflowID),
				RunId:      common.StringPtr(runID),
			},
			ParentDomainID: parentDomainID,
			ParentExecution: &shared.WorkflowExecution{
				WorkflowId: common.StringPtr(parentWorkflowID),
				RunId:      common.StringPtr(parentRunID),
			},
			InitiatedID:                 initiatedID,
			TaskList:                    tasklist,
			WorkflowTypeName:            workflowType,
			WorkflowTimeout:             workflowTimeout,
			DecisionTimeoutValue:        decisionTimeout,
			NextEventID:                 msBuilder.GetNextEventID(),
			LastProcessedEvent:          common.EmptyEventID,
			TransferTasks:               transferTasks,
			DecisionVersion:             di.Version,
			DecisionScheduleID:          di.ScheduleID,
			DecisionStartedID:           di.StartedID,
			DecisionStartToCloseTimeout: di.DecisionTimeout,
			TimerTasks:                  timerTasks,
			ContinueAsNew:               true,
			PreviousRunID:               currentRunID,
			ReplicationState:            replicationState,
		}, input)
	})).Return(&persistence.CreateWorkflowExecutionResponse{}, nil).Once()

	err := s.historyReplicator.replicateWorkflowStarted(context, msBuilder, di, sourceCluster, history, sBuilder, s.logger)
	s.Nil(err)
	s.Equal(1, len(transferTasks))
	s.Equal(version, transferTasks[0].GetVersion())
	s.Equal(1, len(timerTasks))
	s.Equal(version, timerTasks[0].GetVersion())
}

func (s *historyReplicatorSuite) TestReplicateWorkflowStarted_CurrentComplete_IncomingNotLessThanCurrent() {
	domainID := validDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	version := int64(144)
	tasklist := "some random tasklist"
	workflowType := "some random workflow type"
	workflowTimeout := int32(3721)
	decisionTimeout := int32(4411)

	initiatedID := int64(4810)
	parentDomainID := validDomainID
	parentWorkflowID := "some random workflow ID"
	parentRunID := uuid.New()
	sourceCluster := "some random source cluster"

	context := newWorkflowExecutionContext(domainID, shared.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(runID),
	}, s.mockShard, s.mockExecutionMgr, s.logger)
	msBuilder := &mockMutableState{}
	context.msBuilder = msBuilder
	di := &decisionInfo{
		Version:         version,
		ScheduleID:      common.FirstEventID + 1,
		StartedID:       common.EmptyEventID,
		DecisionTimeout: decisionTimeout,
		TaskList:        tasklist,
	}
	sBuilder := &mockStateBuilder{}
	requestID := uuid.New()
	history := &shared.History{
		Events: []*shared.HistoryEvent{
			&shared.HistoryEvent{Version: common.Int64Ptr(version), EventId: common.Int64Ptr(1)},
			&shared.HistoryEvent{Version: common.Int64Ptr(version), EventId: common.Int64Ptr(2)},
		},
	}
	nextEventID := di.ScheduleID + 1
	replicationState := &persistence.ReplicationState{
		StartVersion:     version,
		CurrentVersion:   version,
		LastWriteVersion: version,
		LastWriteEventID: nextEventID - 1,
	}
	transferTasks := []persistence.Task{&persistence.CloseExecutionTask{}}
	timerTasks := []persistence.Task{&persistence.DeleteHistoryEventTask{}}

	msBuilder.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{
		CreateRequestID:      requestID,
		DomainID:             domainID,
		WorkflowID:           workflowID,
		RunID:                runID,
		InitiatedID:          initiatedID,
		ParentDomainID:       parentDomainID,
		ParentWorkflowID:     parentWorkflowID,
		ParentRunID:          parentRunID,
		TaskList:             tasklist,
		WorkflowTypeName:     workflowType,
		WorkflowTimeout:      workflowTimeout,
		DecisionTimeoutValue: decisionTimeout,
	})
	msBuilder.On("UpdateReplicationStateLastEventID", sourceCluster, version, nextEventID-1).Once()
	msBuilder.On("GetReplicationState").Return(replicationState)
	msBuilder.On("GetCurrentVersion").Return(version)
	msBuilder.On("GetNextEventID").Return(nextEventID)
	s.mockHistoryMgr.On("AppendHistoryEvents", mock.Anything).Return(nil).Once()
	sBuilder.On("getTransferTasks").Return(transferTasks)
	sBuilder.On("getTimerTasks").Return(timerTasks)

	currentVersion := version - 1
	currentRunID := uuid.New()
	currentState := persistence.WorkflowStateCompleted
	errRet := &persistence.WorkflowExecutionAlreadyStartedError{
		RunID:        currentRunID,
		State:        currentState,
		StartVersion: currentVersion,
	}
	// the test above already assert the create workflow request, so here jsut use anyting
	s.mockExecutionMgr.On("CreateWorkflowExecution", mock.MatchedBy(func(input *persistence.CreateWorkflowExecutionRequest) bool {
		input.RangeID = 0
		return reflect.DeepEqual(&persistence.CreateWorkflowExecutionRequest{
			RequestID: requestID,
			DomainID:  domainID,
			Execution: shared.WorkflowExecution{
				WorkflowId: common.StringPtr(workflowID),
				RunId:      common.StringPtr(runID),
			},
			ParentDomainID: parentDomainID,
			ParentExecution: &shared.WorkflowExecution{
				WorkflowId: common.StringPtr(parentWorkflowID),
				RunId:      common.StringPtr(parentRunID),
			},
			InitiatedID:                 initiatedID,
			TaskList:                    tasklist,
			WorkflowTypeName:            workflowType,
			WorkflowTimeout:             workflowTimeout,
			DecisionTimeoutValue:        decisionTimeout,
			NextEventID:                 msBuilder.GetNextEventID(),
			LastProcessedEvent:          common.EmptyEventID,
			TransferTasks:               transferTasks,
			DecisionVersion:             di.Version,
			DecisionScheduleID:          di.ScheduleID,
			DecisionStartedID:           di.StartedID,
			DecisionStartToCloseTimeout: di.DecisionTimeout,
			TimerTasks:                  timerTasks,
			ContinueAsNew:               false,
			PreviousRunID:               "",
			ReplicationState:            replicationState,
		}, input)
	})).Return(nil, errRet).Once()
	s.mockExecutionMgr.On("CreateWorkflowExecution", mock.MatchedBy(func(input *persistence.CreateWorkflowExecutionRequest) bool {
		input.RangeID = 0
		return reflect.DeepEqual(&persistence.CreateWorkflowExecutionRequest{
			RequestID: requestID,
			DomainID:  domainID,
			Execution: shared.WorkflowExecution{
				WorkflowId: common.StringPtr(workflowID),
				RunId:      common.StringPtr(runID),
			},
			ParentDomainID: parentDomainID,
			ParentExecution: &shared.WorkflowExecution{
				WorkflowId: common.StringPtr(parentWorkflowID),
				RunId:      common.StringPtr(parentRunID),
			},
			InitiatedID:                 initiatedID,
			TaskList:                    tasklist,
			WorkflowTypeName:            workflowType,
			WorkflowTimeout:             workflowTimeout,
			DecisionTimeoutValue:        decisionTimeout,
			NextEventID:                 msBuilder.GetNextEventID(),
			LastProcessedEvent:          common.EmptyEventID,
			TransferTasks:               transferTasks,
			DecisionVersion:             di.Version,
			DecisionScheduleID:          di.ScheduleID,
			DecisionStartedID:           di.StartedID,
			DecisionStartToCloseTimeout: di.DecisionTimeout,
			TimerTasks:                  timerTasks,
			ContinueAsNew:               true,
			PreviousRunID:               currentRunID,
			ReplicationState:            replicationState,
		}, input)
	})).Return(&persistence.CreateWorkflowExecutionResponse{}, nil).Once()

	err := s.historyReplicator.replicateWorkflowStarted(context, msBuilder, di, sourceCluster, history, sBuilder, s.logger)
	s.Nil(err)
	s.Equal(1, len(transferTasks))
	s.Equal(version, transferTasks[0].GetVersion())
	s.Equal(1, len(timerTasks))
	s.Equal(version, timerTasks[0].GetVersion())
}

func (s *historyReplicatorSuite) TestReplicateWorkflowStarted_CurrentRunning_IncomingLessThanCurrent() {
	domainID := validDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	version := int64(144)
	tasklist := "some random tasklist"
	workflowType := "some random workflow type"
	workflowTimeout := int32(3721)
	decisionTimeout := int32(4411)

	initiatedID := int64(4810)
	parentDomainID := validDomainID
	parentWorkflowID := "some random workflow ID"
	parentRunID := uuid.New()
	sourceCluster := "some random source cluster"

	context := newWorkflowExecutionContext(domainID, shared.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(runID),
	}, s.mockShard, s.mockExecutionMgr, s.logger)
	msBuilder := &mockMutableState{}
	context.msBuilder = msBuilder
	di := &decisionInfo{
		Version:         version,
		ScheduleID:      common.FirstEventID + 1,
		StartedID:       common.EmptyEventID,
		DecisionTimeout: decisionTimeout,
		TaskList:        tasklist,
	}
	sBuilder := &mockStateBuilder{}
	requestID := uuid.New()
	history := &shared.History{
		Events: []*shared.HistoryEvent{
			&shared.HistoryEvent{Version: common.Int64Ptr(version), EventId: common.Int64Ptr(1)},
			&shared.HistoryEvent{Version: common.Int64Ptr(version), EventId: common.Int64Ptr(2)},
		},
	}
	nextEventID := di.ScheduleID + 1
	replicationState := &persistence.ReplicationState{
		StartVersion:     version,
		CurrentVersion:   version,
		LastWriteVersion: version,
		LastWriteEventID: nextEventID - 1,
	}
	transferTasks := []persistence.Task{&persistence.CloseExecutionTask{}}
	timerTasks := []persistence.Task{&persistence.DeleteHistoryEventTask{}}

	msBuilder.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{
		CreateRequestID:      requestID,
		DomainID:             domainID,
		WorkflowID:           workflowID,
		RunID:                runID,
		InitiatedID:          initiatedID,
		ParentDomainID:       parentDomainID,
		ParentWorkflowID:     parentWorkflowID,
		ParentRunID:          parentRunID,
		TaskList:             tasklist,
		WorkflowTypeName:     workflowType,
		WorkflowTimeout:      workflowTimeout,
		DecisionTimeoutValue: decisionTimeout,
	})
	msBuilder.On("UpdateReplicationStateLastEventID", sourceCluster, version, nextEventID-1).Once()
	msBuilder.On("GetReplicationState").Return(replicationState)
	msBuilder.On("GetCurrentVersion").Return(version)
	msBuilder.On("GetNextEventID").Return(nextEventID)
	s.mockHistoryMgr.On("AppendHistoryEvents", mock.Anything).Return(nil).Once()
	sBuilder.On("getTransferTasks").Return(transferTasks)
	sBuilder.On("getTimerTasks").Return(timerTasks)

	currentVersion := version + 1
	currentRunID := uuid.New()
	currentState := persistence.WorkflowStateRunning
	errRet := &persistence.WorkflowExecutionAlreadyStartedError{
		RunID:        currentRunID,
		State:        currentState,
		StartVersion: currentVersion,
	}
	// the test above already assert the create workflow request, so here jsut use anyting
	s.mockExecutionMgr.On("CreateWorkflowExecution", mock.Anything).Return(nil, errRet).Once()
	s.mockHistoryMgr.On("DeleteWorkflowExecutionHistory", mock.Anything).Return(nil).Once()

	err := s.historyReplicator.replicateWorkflowStarted(context, msBuilder, di, sourceCluster, history, sBuilder, s.logger)
	s.Nil(err)
	s.Equal(1, len(transferTasks))
	s.Equal(version, transferTasks[0].GetVersion())
	s.Equal(1, len(timerTasks))
	s.Equal(version, timerTasks[0].GetVersion())
}

func (s *historyReplicatorSuite) TestReplicateWorkflowStarted_CurrentRunning_IncomingEqualToCurrent() {
	domainID := validDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	version := int64(144)
	tasklist := "some random tasklist"
	workflowType := "some random workflow type"
	workflowTimeout := int32(3721)
	decisionTimeout := int32(4411)

	initiatedID := int64(4810)
	parentDomainID := validDomainID
	parentWorkflowID := "some random workflow ID"
	parentRunID := uuid.New()
	sourceCluster := "some random source cluster"

	context := newWorkflowExecutionContext(domainID, shared.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(runID),
	}, s.mockShard, s.mockExecutionMgr, s.logger)
	msBuilder := &mockMutableState{}
	context.msBuilder = msBuilder
	di := &decisionInfo{
		Version:         version,
		ScheduleID:      common.FirstEventID + 1,
		StartedID:       common.EmptyEventID,
		DecisionTimeout: decisionTimeout,
		TaskList:        tasklist,
	}
	sBuilder := &mockStateBuilder{}
	requestID := uuid.New()
	history := &shared.History{
		Events: []*shared.HistoryEvent{
			&shared.HistoryEvent{Version: common.Int64Ptr(version), EventId: common.Int64Ptr(1)},
			&shared.HistoryEvent{Version: common.Int64Ptr(version), EventId: common.Int64Ptr(2)},
		},
	}
	nextEventID := di.ScheduleID + 1
	replicationState := &persistence.ReplicationState{
		StartVersion:     version,
		CurrentVersion:   version,
		LastWriteVersion: version,
		LastWriteEventID: nextEventID - 1,
	}
	transferTasks := []persistence.Task{&persistence.CloseExecutionTask{}}
	timerTasks := []persistence.Task{&persistence.DeleteHistoryEventTask{}}

	msBuilder.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{
		CreateRequestID:      requestID,
		DomainID:             domainID,
		WorkflowID:           workflowID,
		RunID:                runID,
		InitiatedID:          initiatedID,
		ParentDomainID:       parentDomainID,
		ParentWorkflowID:     parentWorkflowID,
		ParentRunID:          parentRunID,
		TaskList:             tasklist,
		WorkflowTypeName:     workflowType,
		WorkflowTimeout:      workflowTimeout,
		DecisionTimeoutValue: decisionTimeout,
	})
	msBuilder.On("UpdateReplicationStateLastEventID", sourceCluster, version, nextEventID-1).Once()
	msBuilder.On("GetReplicationState").Return(replicationState)
	msBuilder.On("GetCurrentVersion").Return(version)
	msBuilder.On("GetNextEventID").Return(nextEventID)
	s.mockHistoryMgr.On("AppendHistoryEvents", mock.Anything).Return(nil).Once()
	sBuilder.On("getTransferTasks").Return(transferTasks)
	sBuilder.On("getTimerTasks").Return(timerTasks)

	currentVersion := version
	currentRunID := uuid.New()
	currentState := persistence.WorkflowStateRunning
	errRet := &persistence.WorkflowExecutionAlreadyStartedError{
		RunID:        currentRunID,
		State:        currentState,
		StartVersion: currentVersion,
	}
	// the test above already assert the create workflow request, so here jsut use anyting
	s.mockExecutionMgr.On("CreateWorkflowExecution", mock.Anything).Return(nil, errRet).Once()

	err := s.historyReplicator.replicateWorkflowStarted(context, msBuilder, di, sourceCluster, history, sBuilder, s.logger)
	s.Equal(ErrRetryEntityNotExists, err)
	s.Equal(1, len(transferTasks))
	s.Equal(version, transferTasks[0].GetVersion())
	s.Equal(1, len(timerTasks))
	s.Equal(version, timerTasks[0].GetVersion())
}

func (s *historyReplicatorSuite) TestReplicateWorkflowStarted_CurrentRunning_IncomingLargerThanCurrent() {
	domainName := "some random domain name"
	domainID := validDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	version := int64(144)
	tasklist := "some random tasklist"
	workflowType := "some random workflow type"
	workflowTimeout := int32(3721)
	decisionTimeout := int32(4411)

	initiatedID := int64(4810)
	parentDomainID := validDomainID
	parentWorkflowID := "some random workflow ID"
	parentRunID := uuid.New()
	sourceCluster := "some random source cluster"

	context := newWorkflowExecutionContext(domainID, shared.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(runID),
	}, s.mockShard, s.mockExecutionMgr, s.logger)
	msBuilder := &mockMutableState{}
	context.msBuilder = msBuilder
	di := &decisionInfo{
		Version:         version,
		ScheduleID:      common.FirstEventID + 1,
		StartedID:       common.EmptyEventID,
		DecisionTimeout: decisionTimeout,
		TaskList:        tasklist,
	}
	sBuilder := &mockStateBuilder{}
	requestID := uuid.New()
	history := &shared.History{
		Events: []*shared.HistoryEvent{
			&shared.HistoryEvent{Version: common.Int64Ptr(version), EventId: common.Int64Ptr(1)},
			&shared.HistoryEvent{Version: common.Int64Ptr(version), EventId: common.Int64Ptr(2)},
		},
	}
	nextEventID := di.ScheduleID + 1
	replicationState := &persistence.ReplicationState{
		StartVersion:     version,
		CurrentVersion:   version,
		LastWriteVersion: version,
		LastWriteEventID: nextEventID - 1,
	}
	transferTasks := []persistence.Task{&persistence.CloseExecutionTask{}}
	timerTasks := []persistence.Task{&persistence.DeleteHistoryEventTask{}}

	msBuilder.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{
		CreateRequestID:      requestID,
		DomainID:             domainID,
		WorkflowID:           workflowID,
		RunID:                runID,
		InitiatedID:          initiatedID,
		ParentDomainID:       parentDomainID,
		ParentWorkflowID:     parentWorkflowID,
		ParentRunID:          parentRunID,
		TaskList:             tasklist,
		WorkflowTypeName:     workflowType,
		WorkflowTimeout:      workflowTimeout,
		DecisionTimeoutValue: decisionTimeout,
	})
	msBuilder.On("UpdateReplicationStateLastEventID", sourceCluster, version, nextEventID-1).Once()
	msBuilder.On("GetReplicationState").Return(replicationState)
	msBuilder.On("GetCurrentVersion").Return(version)
	msBuilder.On("GetNextEventID").Return(nextEventID)
	s.mockHistoryMgr.On("AppendHistoryEvents", mock.Anything).Return(nil).Once()
	sBuilder.On("getTransferTasks").Return(transferTasks)
	sBuilder.On("getTimerTasks").Return(timerTasks)

	currentVersion := version - 1
	currentRunID := uuid.New()
	currentState := persistence.WorkflowStateRunning
	errRet := &persistence.WorkflowExecutionAlreadyStartedError{
		RunID:        currentRunID,
		State:        currentState,
		StartVersion: currentVersion,
	}
	// the test above already assert the create workflow request, so here jsut use anyting
	s.mockExecutionMgr.On("CreateWorkflowExecution", mock.MatchedBy(func(input *persistence.CreateWorkflowExecutionRequest) bool {
		input.RangeID = 0
		return reflect.DeepEqual(&persistence.CreateWorkflowExecutionRequest{
			RequestID: requestID,
			DomainID:  domainID,
			Execution: shared.WorkflowExecution{
				WorkflowId: common.StringPtr(workflowID),
				RunId:      common.StringPtr(runID),
			},
			ParentDomainID: parentDomainID,
			ParentExecution: &shared.WorkflowExecution{
				WorkflowId: common.StringPtr(parentWorkflowID),
				RunId:      common.StringPtr(parentRunID),
			},
			InitiatedID:                 initiatedID,
			TaskList:                    tasklist,
			WorkflowTypeName:            workflowType,
			WorkflowTimeout:             workflowTimeout,
			DecisionTimeoutValue:        decisionTimeout,
			NextEventID:                 msBuilder.GetNextEventID(),
			LastProcessedEvent:          common.EmptyEventID,
			TransferTasks:               transferTasks,
			DecisionVersion:             di.Version,
			DecisionScheduleID:          di.ScheduleID,
			DecisionStartedID:           di.StartedID,
			DecisionStartToCloseTimeout: di.DecisionTimeout,
			TimerTasks:                  timerTasks,
			ContinueAsNew:               false,
			PreviousRunID:               "",
			ReplicationState:            replicationState,
		}, input)
	})).Return(nil, errRet).Once()
	s.mockExecutionMgr.On("CreateWorkflowExecution", mock.MatchedBy(func(input *persistence.CreateWorkflowExecutionRequest) bool {
		input.RangeID = 0
		return reflect.DeepEqual(&persistence.CreateWorkflowExecutionRequest{
			RequestID: requestID,
			DomainID:  domainID,
			Execution: shared.WorkflowExecution{
				WorkflowId: common.StringPtr(workflowID),
				RunId:      common.StringPtr(runID),
			},
			ParentDomainID: parentDomainID,
			ParentExecution: &shared.WorkflowExecution{
				WorkflowId: common.StringPtr(parentWorkflowID),
				RunId:      common.StringPtr(parentRunID),
			},
			InitiatedID:                 initiatedID,
			TaskList:                    tasklist,
			WorkflowTypeName:            workflowType,
			WorkflowTimeout:             workflowTimeout,
			DecisionTimeoutValue:        decisionTimeout,
			NextEventID:                 msBuilder.GetNextEventID(),
			LastProcessedEvent:          common.EmptyEventID,
			TransferTasks:               transferTasks,
			DecisionVersion:             di.Version,
			DecisionScheduleID:          di.ScheduleID,
			DecisionStartedID:           di.StartedID,
			DecisionStartToCloseTimeout: di.DecisionTimeout,
			TimerTasks:                  timerTasks,
			ContinueAsNew:               true,
			PreviousRunID:               currentRunID,
			ReplicationState:            replicationState,
		}, input)
	})).Return(&persistence.CreateWorkflowExecutionResponse{}, nil).Once()

	// this mocks are for the terminate current workflow operation
	s.mockMetadataMgr.On("GetDomain", &persistence.GetDomainRequest{ID: domainID}).Return(
		&persistence.GetDomainResponse{
			Info:   &persistence.DomainInfo{ID: domainID, Name: domainName},
			Config: &persistence.DomainConfig{Retention: 1},
			ReplicationConfig: &persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				},
			},
			FailoverVersion: currentVersion,
			IsGlobalDomain:  true,
			TableVersion:    persistence.DomainTableVersionV1,
		}, nil,
	).Once()
	currentContext, currentRelease, err := s.historyReplicator.historyCache.getOrCreateWorkflowExecution(domainID, shared.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(currentRunID),
	})
	s.Nil(err)
	currentMsBuilder := &mockMutableState{}
	currentContext.msBuilder = currentMsBuilder
	currentRelease(nil)
	// all method call to currentMsBuilder are randomly mocked
	// we are testing the functionality that workflow termination is called, not its detail
	currentMsBuilder.On("IsWorkflowExecutionRunning").Return(false) // return nil so we can skil a lot of mocking
	currentMsBuilder.On("GetReplicationState").Return(nil)

	err = s.historyReplicator.replicateWorkflowStarted(context, msBuilder, di, sourceCluster, history, sBuilder, s.logger)
	s.Nil(err)
	s.Equal(1, len(transferTasks))
	s.Equal(version, transferTasks[0].GetVersion())
	s.Equal(1, len(timerTasks))
	s.Equal(version, timerTasks[0].GetVersion())
}

func (s *historyReplicatorSuite) TestConflictResolutionTerminateContinueAsNew_TargetRunning() {
	msBuilderTarget := &mockMutableState{}
	msBuilderTarget.On("IsWorkflowExecutionRunning").Return(true)
	err := s.historyReplicator.conflictResolutionTerminateContinueAsNew(msBuilderTarget)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestConflictResolutionTerminateContinueAsNew_TargetClosed_NotContinueAsNew() {
	msBuilderTarget := &mockMutableState{}
	msBuilderTarget.On("IsWorkflowExecutionRunning").Return(false)
	msBuilderTarget.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{CloseStatus: persistence.WorkflowCloseStatusCompleted})

	err := s.historyReplicator.conflictResolutionTerminateContinueAsNew(msBuilderTarget)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestConflictResolutionTerminateContinueAsNew_TargetClosed_ContinueAsNew_CurrentClosed() {
	domainID := validDomainID
	workflowID := "some random target workflow ID"
	targetRunID := uuid.New()

	msBuilderTarget := &mockMutableState{}
	msBuilderTarget.On("IsWorkflowExecutionRunning").Return(false)
	msBuilderTarget.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{
		DomainID:    domainID,
		WorkflowID:  workflowID,
		RunID:       targetRunID,
		CloseStatus: persistence.WorkflowCloseStatusContinuedAsNew,
	})

	currentRunID := uuid.New()
	contextCurrent, release, err := s.historyReplicator.historyCache.getOrCreateWorkflowExecution(domainID, shared.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(currentRunID),
	})
	s.Nil(err)
	msBuilderCurrent := &mockMutableState{}
	msBuilderCurrent.On("GetLastWriteVersion").Return(int64(999))                      // this is not actually used, but will be called
	msBuilderCurrent.On("GetReplicationState").Return(&persistence.ReplicationState{}) // this is used to update the version on mutable state
	msBuilderCurrent.On("IsWorkflowExecutionRunning").Return(false)                    // this is used to update the version on mutable state
	msBuilderCurrent.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{RunID: currentRunID, CloseStatus: persistence.WorkflowCloseStatusTerminated})
	contextCurrent.msBuilder = msBuilderCurrent
	release(nil)

	s.mockExecutionMgr.On("GetCurrentExecution", &persistence.GetCurrentExecutionRequest{
		DomainID:   domainID,
		WorkflowID: workflowID,
	}).Return(&persistence.GetCurrentExecutionResponse{
		RunID: currentRunID,
		// other attributes are not used
	}, nil)

	err = s.historyReplicator.conflictResolutionTerminateContinueAsNew(msBuilderTarget)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestConflictResolutionTerminateContinueAsNew_TargetClosed_ContinueAsNew_CurrentRunning() {
	version := int64(4801) // this does nothing in this test
	domainName := "some random domain name"
	domainID := validDomainID
	workflowID := "some random target workflow ID"
	targetRunID := uuid.New()

	msBuilderTarget := &mockMutableState{}
	msBuilderTarget.On("IsWorkflowExecutionRunning").Return(false)
	msBuilderTarget.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{
		DomainID:    domainID,
		WorkflowID:  workflowID,
		RunID:       targetRunID,
		CloseStatus: persistence.WorkflowCloseStatusContinuedAsNew,
	})

	// this mocks are for the terminate current workflow operation
	s.mockMetadataMgr.On("GetDomain", &persistence.GetDomainRequest{ID: domainID}).Return(
		&persistence.GetDomainResponse{
			Info:   &persistence.DomainInfo{ID: domainID, Name: domainName},
			Config: &persistence.DomainConfig{Retention: 1},
			ReplicationConfig: &persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				},
			},
			FailoverVersion: version,
			IsGlobalDomain:  true,
			TableVersion:    persistence.DomainTableVersionV1,
		}, nil,
	).Once()

	currentRunID := uuid.New()
	contextCurrent, release, err := s.historyReplicator.historyCache.getOrCreateWorkflowExecution(domainID, shared.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(currentRunID),
	})
	s.Nil(err)
	msBuilderCurrent := &mockMutableState{}
	msBuilderCurrent.On("GetLastWriteVersion").Return(int64(999))                      // this is not actually used, but will be called
	msBuilderCurrent.On("GetReplicationState").Return(&persistence.ReplicationState{}) // this is used to update the version on mutable state
	msBuilderCurrent.On("IsWorkflowExecutionRunning").Return(true)                     // this is used to update the version on mutable state
	msBuilderCurrent.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{RunID: currentRunID, CloseStatus: persistence.WorkflowCloseStatusNone})
	msBuilderCurrent.On("UpdateReplicationStateVersion", version)
	contextCurrent.msBuilder = msBuilderCurrent
	release(nil)
	s.mockExecutionMgr.On("GetCurrentExecution", &persistence.GetCurrentExecutionRequest{
		DomainID:   domainID,
		WorkflowID: workflowID,
	}).Return(&persistence.GetCurrentExecutionResponse{
		RunID: currentRunID,
		// other attributes are not used
	}, nil)

	currentStartEvent := &shared.HistoryEvent{
		EventId:   common.Int64Ptr(common.FirstEventID),
		EventType: shared.EventTypeWorkflowExecutionStarted.Ptr(),
		WorkflowExecutionStartedEventAttributes: &shared.WorkflowExecutionStartedEventAttributes{
			ContinuedExecutionRunId: common.StringPtr(targetRunID),
			// other attributes are not used
		},
	}
	currentStartEventBatch := persistence.NewHistoryEventBatch(persistence.GetDefaultHistoryVersion(), []*shared.HistoryEvent{currentStartEvent})
	serializedStartEventBatch, err := persistence.NewJSONHistorySerializer().Serialize(currentStartEventBatch)
	s.Nil(err)
	s.mockHistoryMgr.On("GetWorkflowExecutionHistory", &persistence.GetWorkflowExecutionHistoryRequest{
		DomainID: domainID,
		Execution: shared.WorkflowExecution{
			WorkflowId: common.StringPtr(workflowID),
			RunId:      common.StringPtr(currentRunID),
		},
		FirstEventID:  common.FirstEventID,
		NextEventID:   common.FirstEventID + 1,
		PageSize:      defaultHistoryPageSize,
		NextPageToken: nil,
	}).Return(&persistence.GetWorkflowExecutionHistoryResponse{
		Events:        []persistence.SerializedHistoryEventBatch{*serializedStartEventBatch},
		NextPageToken: nil,
	}, nil)

	// return nil, to trigger the history engine to return err, so we can assert on it
	// this is to save a lot of meaningless mock, since we are not testing functionality of history engine
	msBuilderCurrent.On("AddWorkflowExecutionTerminatedEvent", mock.Anything).Return(nil)

	err = s.historyReplicator.conflictResolutionTerminateContinueAsNew(msBuilderTarget)
	s.NotNil(err)
	_, ok := err.(*shared.InternalServiceError)
	s.True(ok)
}
