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
	ctx "context"
	"reflect"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"

	h "github.com/uber/cadence/.gen/go/history"
	"github.com/uber/cadence/.gen/go/shared"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/clock"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/definition"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/mocks"
	"github.com/uber/cadence/common/persistence"
	p "github.com/uber/cadence/common/persistence"
)

const (
	testShardID = 1
)

type (
	historyReplicatorSuite struct {
		suite.Suite
		*require.Assertions

		controller               *gomock.Controller
		mockShard                *shardContextTest
		mockWorkflowResetor      *MockworkflowResetor
		mockTxProcessor          *MocktransferQueueProcessor
		mockReplicationProcessor *MockReplicatorQueueProcessor
		mockTimerProcessor       *MocktimerQueueProcessor
		mockStateBuilder         *MockstateBuilder
		mockDomainCache          *cache.MockDomainCache
		mockClusterMetadata      *cluster.MockMetadata

		logger           log.Logger
		mockExecutionMgr *mocks.ExecutionManager
		mockHistoryV2Mgr *mocks.HistoryV2Manager
		mockShardManager *mocks.ShardManager

		historyReplicator *historyReplicator
	}
)

func TestHistoryReplicatorSuite(t *testing.T) {
	s := new(historyReplicatorSuite)
	suite.Run(t, s)
}

func (s *historyReplicatorSuite) SetupSuite() {

}

func (s *historyReplicatorSuite) TearDownSuite() {

}

func (s *historyReplicatorSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.mockWorkflowResetor = NewMockworkflowResetor(s.controller)
	s.mockTxProcessor = NewMocktransferQueueProcessor(s.controller)
	s.mockReplicationProcessor = NewMockReplicatorQueueProcessor(s.controller)
	s.mockTimerProcessor = NewMocktimerQueueProcessor(s.controller)
	s.mockStateBuilder = NewMockstateBuilder(s.controller)
	s.mockTxProcessor.EXPECT().NotifyNewTask(gomock.Any(), gomock.Any()).AnyTimes()
	s.mockReplicationProcessor.EXPECT().notifyNewTask().AnyTimes()
	s.mockTimerProcessor.EXPECT().NotifyNewTimers(gomock.Any(), gomock.Any()).AnyTimes()

	s.mockShard = newTestShardContext(
		s.controller,
		&persistence.ShardInfo{
			ShardID:          testShardID,
			RangeID:          1,
			TransferAckLevel: 0,
		},
		NewDynamicConfigForTest(),
	)

	s.mockExecutionMgr = s.mockShard.resource.ExecutionMgr
	s.mockHistoryV2Mgr = s.mockShard.resource.HistoryMgr
	s.mockShardManager = s.mockShard.resource.ShardMgr
	s.mockClusterMetadata = s.mockShard.resource.ClusterMetadata
	s.mockDomainCache = s.mockShard.resource.DomainCache
	s.mockClusterMetadata.EXPECT().IsGlobalDomainEnabled().Return(true).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestAllClusterInfo).AnyTimes()

	s.logger = s.mockShard.GetLogger()

	historyCache := newHistoryCache(s.mockShard)
	engine := &historyEngineImpl{
		currentClusterName:   s.mockShard.GetClusterMetadata().GetCurrentClusterName(),
		shard:                s.mockShard,
		clusterMetadata:      s.mockClusterMetadata,
		executionManager:     s.mockExecutionMgr,
		historyCache:         historyCache,
		logger:               s.logger,
		tokenSerializer:      common.NewJSONTaskTokenSerializer(),
		metricsClient:        s.mockShard.GetMetricsClient(),
		timeSource:           s.mockShard.GetTimeSource(),
		historyEventNotifier: newHistoryEventNotifier(clock.NewRealTimeSource(), metrics.NewClient(tally.NoopScope, metrics.History), func(string) int { return 0 }),
		txProcessor:          s.mockTxProcessor,
		replicatorProcessor:  s.mockReplicationProcessor,
		timerProcessor:       s.mockTimerProcessor,
	}
	s.mockShard.SetEngine(engine)

	s.historyReplicator = newHistoryReplicator(s.mockShard, clock.NewEventTimeSource(), engine, historyCache, s.mockDomainCache, s.mockHistoryV2Mgr, s.logger)
	s.historyReplicator.resetor = s.mockWorkflowResetor
}

func (s *historyReplicatorSuite) TearDownTest() {
	s.historyReplicator = nil
	s.controller.Finish()
	s.mockShard.Finish(s.T())
}

func (s *historyReplicatorSuite) TestApplyStartEvent() {

}

func (s *historyReplicatorSuite) TestApplyOtherEventsMissingMutableState_MissingCurrent() {
	domainID := testDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	version := int64(123)
	now := time.Now().UnixNano()
	req := &h.ReplicateEventsRequest{
		History: &shared.History{
			Events: []*shared.HistoryEvent{
				{
					Version:   common.Int64Ptr(version),
					Timestamp: common.Int64Ptr(now),
				},
			},
		},
	}

	s.mockExecutionMgr.On("GetCurrentExecution", &persistence.GetCurrentExecutionRequest{
		DomainID:   domainID,
		WorkflowID: workflowID,
	}).Return(nil, &shared.EntityNotExistsError{})

	err := s.historyReplicator.ApplyOtherEventsMissingMutableState(ctx.Background(), domainID, workflowID, runID, req, s.logger)
	s.Equal(newRetryTaskErrorWithHint(ErrWorkflowNotFoundMsg, domainID, workflowID, runID, common.FirstEventID), err)
}

func (s *historyReplicatorSuite) TestApplyOtherEventsMissingMutableState_IncomingLessThanCurrent_NoEventsReapplication() {
	domainID := testDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	version := int64(123)
	now := time.Now().UnixNano()
	currentRunID := uuid.New()
	currentVersion := version + 1
	currentNextEventID := int64(2333)
	req := &h.ReplicateEventsRequest{
		History: &shared.History{
			Events: []*shared.HistoryEvent{
				{
					Version:   common.Int64Ptr(version),
					Timestamp: common.Int64Ptr(now),
					EventType: shared.EventTypeWorkflowExecutionCanceled.Ptr(),
				},
			},
		},
	}

	contextCurrent := NewMockworkflowExecutionContext(s.controller)
	contextCurrent.EXPECT().lock(gomock.Any()).Return(nil).Times(1)
	contextCurrent.EXPECT().unlock().Times(1)

	msBuilderCurrent := NewMockmutableState(s.controller)

	contextCurrent.EXPECT().loadWorkflowExecution().Return(msBuilderCurrent, nil).Times(1)
	currentExecution := &shared.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(currentRunID),
	}
	contextCurrentCacheKey := definition.NewWorkflowIdentifier(domainID, currentExecution.GetWorkflowId(), currentExecution.GetRunId())
	_, _ = s.historyReplicator.historyCache.PutIfNotExist(contextCurrentCacheKey, contextCurrent)

	s.mockExecutionMgr.On("GetCurrentExecution", &persistence.GetCurrentExecutionRequest{
		DomainID:   domainID,
		WorkflowID: workflowID,
	}).Return(&persistence.GetCurrentExecutionResponse{
		RunID: currentRunID,
		// other attributes are not used
	}, nil)

	msBuilderCurrent.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{
		RunID:              currentRunID,
		NextEventID:        currentNextEventID,
		DecisionVersion:    common.EmptyVersion,
		DecisionScheduleID: common.EmptyEventID,
		DecisionStartedID:  common.EmptyEventID,
	}).AnyTimes()
	msBuilderCurrent.EXPECT().GetNextEventID().Return(currentNextEventID).AnyTimes()
	msBuilderCurrent.EXPECT().GetLastWriteVersion().Return(currentVersion, nil).AnyTimes()
	msBuilderCurrent.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes()

	err := s.historyReplicator.ApplyOtherEventsMissingMutableState(ctx.Background(), domainID, workflowID, runID, req, s.logger)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestApplyOtherEventsMissingMutableState_IncomingLessThanCurrent_EventsReapplication_PendingDecision() {
	domainID := testDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	version := int64(123)
	now := time.Now().UnixNano()
	currentRunID := uuid.New()
	currentVersion := version + 1
	currentNextEventID := int64(2333)

	signalName := "some random signal name"
	signalInput := []byte("some random signal input")
	signalIdentity := "some random signal identity"

	req := &h.ReplicateEventsRequest{
		History: &shared.History{
			Events: []*shared.HistoryEvent{
				{
					Version:   common.Int64Ptr(version),
					Timestamp: common.Int64Ptr(now),
					EventType: shared.EventTypeWorkflowExecutionSignaled.Ptr(),
					WorkflowExecutionSignaledEventAttributes: &shared.WorkflowExecutionSignaledEventAttributes{
						SignalName: common.StringPtr(signalName),
						Input:      signalInput,
						Identity:   common.StringPtr(signalIdentity),
					},
				},
			},
		},
	}

	contextCurrent := NewMockworkflowExecutionContext(s.controller)
	contextCurrent.EXPECT().lock(gomock.Any()).Return(nil).Times(1)
	contextCurrent.EXPECT().unlock().Times(1)

	msBuilderCurrent := NewMockmutableState(s.controller)

	contextCurrent.EXPECT().loadWorkflowExecution().Return(msBuilderCurrent, nil).Times(1)
	currentExecution := &shared.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(currentRunID),
	}
	contextCurrentCacheKey := definition.NewWorkflowIdentifier(domainID, currentExecution.GetWorkflowId(), currentExecution.GetRunId())
	_, _ = s.historyReplicator.historyCache.PutIfNotExist(contextCurrentCacheKey, contextCurrent)

	s.mockExecutionMgr.On("GetCurrentExecution", &persistence.GetCurrentExecutionRequest{
		DomainID:   domainID,
		WorkflowID: workflowID,
	}).Return(&persistence.GetCurrentExecutionResponse{
		RunID: currentRunID,
		// other attributes are not used
	}, nil)

	msBuilderCurrent.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{
		RunID:              currentRunID,
		NextEventID:        currentNextEventID,
		DecisionVersion:    common.EmptyVersion,
		DecisionScheduleID: common.EmptyEventID,
		DecisionStartedID:  common.EmptyEventID,
	}).AnyTimes()
	msBuilderCurrent.EXPECT().GetNextEventID().Return(currentNextEventID).AnyTimes()
	msBuilderCurrent.EXPECT().GetLastWriteVersion().Return(currentVersion, nil).AnyTimes()
	msBuilderCurrent.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes()
	msBuilderCurrent.EXPECT().UpdateCurrentVersion(currentVersion, true).Return(nil).Times(1)
	msBuilderCurrent.EXPECT().AddWorkflowExecutionSignaled(signalName, signalInput, signalIdentity).Return(&shared.HistoryEvent{
		EventType: shared.EventTypeWorkflowExecutionSignaled.Ptr(),
		Timestamp: common.Int64Ptr(time.Now().UnixNano()),
		WorkflowExecutionSignaledEventAttributes: &shared.WorkflowExecutionSignaledEventAttributes{
			SignalName: common.StringPtr(signalName),
			Input:      signalInput,
			Identity:   common.StringPtr(signalIdentity),
		},
	}, nil).Times(1)
	msBuilderCurrent.EXPECT().HasPendingDecision().Return(true).Times(1)

	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(currentVersion).Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	contextCurrent.EXPECT().updateWorkflowExecutionAsActive(gomock.Any()).Return(nil).Times(1)

	err := s.historyReplicator.ApplyOtherEventsMissingMutableState(ctx.Background(), domainID, workflowID, runID, req, s.logger)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestApplyOtherEventsMissingMutableState_IncomingLessThanCurrent_EventsReapplication_NoPendingDecision() {
	domainID := testDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	version := int64(123)
	now := time.Now().UnixNano()

	signalName := "some random signal name"
	signalInput := []byte("some random signal input")
	signalIdentity := "some random signal identity"

	currentRunID := uuid.New()
	currentVersion := version + 1
	currentNextEventID := int64(2333)
	currentDecisionTimeout := int32(100)
	currentDecisionStickyTimeout := int32(10)
	currentDecisionTasklist := "some random decision tasklist"
	currentDecisionStickyTasklist := "some random decision sticky tasklist"

	req := &h.ReplicateEventsRequest{
		History: &shared.History{
			Events: []*shared.HistoryEvent{
				{
					Version:   common.Int64Ptr(version),
					Timestamp: common.Int64Ptr(now),
					EventType: shared.EventTypeWorkflowExecutionSignaled.Ptr(),
					WorkflowExecutionSignaledEventAttributes: &shared.WorkflowExecutionSignaledEventAttributes{
						SignalName: common.StringPtr(signalName),
						Input:      signalInput,
						Identity:   common.StringPtr(signalIdentity),
					},
				},
			},
		},
	}

	contextCurrent := NewMockworkflowExecutionContext(s.controller)
	contextCurrent.EXPECT().lock(gomock.Any()).Return(nil).Times(1)
	contextCurrent.EXPECT().unlock().Times(1)

	msBuilderCurrent := NewMockmutableState(s.controller)

	contextCurrent.EXPECT().loadWorkflowExecution().Return(msBuilderCurrent, nil).Times(1)
	currentExecution := &shared.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(currentRunID),
	}
	contextCurrentCacheKey := definition.NewWorkflowIdentifier(domainID, currentExecution.GetWorkflowId(), currentExecution.GetRunId())
	_, _ = s.historyReplicator.historyCache.PutIfNotExist(contextCurrentCacheKey, contextCurrent)

	s.mockExecutionMgr.On("GetCurrentExecution", &persistence.GetCurrentExecutionRequest{
		DomainID:   domainID,
		WorkflowID: workflowID,
	}).Return(&persistence.GetCurrentExecutionResponse{
		RunID: currentRunID,
		// other attributes are not used
	}, nil)

	msBuilderCurrent.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{
		DomainID:                     domainID,
		RunID:                        currentRunID,
		NextEventID:                  currentNextEventID,
		TaskList:                     currentDecisionTasklist,
		StickyTaskList:               currentDecisionStickyTasklist,
		DecisionTimeout:              currentDecisionTimeout,
		StickyScheduleToStartTimeout: currentDecisionStickyTimeout,
		DecisionVersion:              common.EmptyVersion,
		DecisionScheduleID:           common.EmptyEventID,
		DecisionStartedID:            common.EmptyEventID,
	}).AnyTimes()
	msBuilderCurrent.EXPECT().GetNextEventID().Return(currentNextEventID).AnyTimes()
	msBuilderCurrent.EXPECT().GetLastWriteVersion().Return(currentVersion, nil).AnyTimes()
	msBuilderCurrent.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes()
	msBuilderCurrent.EXPECT().UpdateCurrentVersion(currentVersion, true).Return(nil).Times(1)
	msBuilderCurrent.EXPECT().AddWorkflowExecutionSignaled(signalName, signalInput, signalIdentity).Return(&shared.HistoryEvent{
		EventType: shared.EventTypeWorkflowExecutionSignaled.Ptr(),
		Timestamp: common.Int64Ptr(time.Now().UnixNano()),
		WorkflowExecutionSignaledEventAttributes: &shared.WorkflowExecutionSignaledEventAttributes{
			SignalName: common.StringPtr(signalName),
			Input:      signalInput,
			Identity:   common.StringPtr(signalIdentity),
		},
	}, nil).Times(1)
	msBuilderCurrent.EXPECT().HasPendingDecision().Return(false).Times(1)
	newDecision := &decisionInfo{
		Version:    currentVersion,
		ScheduleID: 1234,
		StartedID:  common.EmptyEventID,
		TaskList:   currentDecisionStickyTasklist,
		Attempt:    0,
	}
	msBuilderCurrent.EXPECT().AddDecisionTaskScheduledEvent(false).Return(newDecision, nil).Times(1)

	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(currentVersion).Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()

	contextCurrent.EXPECT().updateWorkflowExecutionAsActive(gomock.Any()).Return(nil).Times(1)

	err := s.historyReplicator.ApplyOtherEventsMissingMutableState(ctx.Background(), domainID, workflowID, runID, req, s.logger)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestApplyOtherEventsMissingMutableState_IncomingEqualToCurrent_CurrentRunning() {
	domainName := "some random domain name"
	domainID := testDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	version := int64(123)
	now := time.Now().UnixNano()
	currentRunID := uuid.New()
	currentVersion := version
	currentNextEventID := int64(2333)
	req := &h.ReplicateEventsRequest{
		History: &shared.History{
			Events: []*shared.HistoryEvent{
				{
					Version:   common.Int64Ptr(version),
					Timestamp: common.Int64Ptr(now),
				},
			},
		},
	}

	s.mockDomainCache.EXPECT().GetDomainByID(domainID).Return(
		cache.NewGlobalDomainCacheEntryForTest(
			&persistence.DomainInfo{ID: domainID, Name: domainName},
			&persistence.DomainConfig{Retention: 1},
			&persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					{ClusterName: cluster.TestCurrentClusterName},
					{ClusterName: cluster.TestAlternativeClusterName},
				},
			},
			currentVersion,
			nil,
		), nil,
	).AnyTimes()

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
			ExecutionInfo: &persistence.WorkflowExecutionInfo{
				RunID:              currentRunID,
				NextEventID:        currentNextEventID,
				State:              persistence.WorkflowStateRunning,
				DecisionVersion:    common.EmptyVersion,
				DecisionScheduleID: common.EmptyEventID,
				DecisionStartedID:  common.EmptyEventID,
			},
			ExecutionStats:   &persistence.ExecutionStats{},
			ReplicationState: &persistence.ReplicationState{LastWriteVersion: currentVersion},
		},
	}, nil)

	err := s.historyReplicator.ApplyOtherEventsMissingMutableState(ctx.Background(), domainID, workflowID, runID, req, s.logger)
	s.Equal(newRetryTaskErrorWithHint(ErrWorkflowNotFoundMsg, domainID, workflowID, currentRunID, currentNextEventID), err)
}

func (s *historyReplicatorSuite) TestApplyOtherEventsMissingMutableState_IncomingEqualToCurrent_CurrentRunning_OutOfOrder() {
	domainName := "some random domain name"
	domainID := testDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	version := int64(123)
	lastEventTaskID := int64(5667)
	now := time.Now().UnixNano()
	currentRunID := uuid.New()
	currentVersion := version
	currentNextEventID := int64(2333)
	currentLastEventTaskID := lastEventTaskID + 10
	req := &h.ReplicateEventsRequest{
		History: &shared.History{
			Events: []*shared.HistoryEvent{
				{
					Version:   common.Int64Ptr(version),
					TaskId:    common.Int64Ptr(lastEventTaskID),
					Timestamp: common.Int64Ptr(now),
				},
			},
		},
	}

	s.mockDomainCache.EXPECT().GetDomainByID(domainID).Return(
		cache.NewGlobalDomainCacheEntryForTest(
			&persistence.DomainInfo{ID: domainID, Name: domainName},
			&persistence.DomainConfig{Retention: 1},
			&persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					{ClusterName: cluster.TestCurrentClusterName},
					{ClusterName: cluster.TestAlternativeClusterName},
				},
			},
			currentVersion,
			nil,
		), nil,
	).AnyTimes()

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
			ExecutionInfo: &persistence.WorkflowExecutionInfo{
				RunID:              currentRunID,
				NextEventID:        currentNextEventID,
				LastEventTaskID:    currentLastEventTaskID,
				State:              persistence.WorkflowStateRunning,
				DecisionVersion:    common.EmptyVersion,
				DecisionScheduleID: common.EmptyEventID,
				DecisionStartedID:  common.EmptyEventID,
			},
			ExecutionStats:   &persistence.ExecutionStats{},
			ReplicationState: &persistence.ReplicationState{LastWriteVersion: currentVersion},
		},
	}, nil)

	err := s.historyReplicator.ApplyOtherEventsMissingMutableState(ctx.Background(), domainID, workflowID, runID, req, s.logger)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestApplyOtherEventsMissingMutableState_IncomingLargerThanCurrent_CurrentRunning() {
	domainID := testDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	version := int64(123)
	now := time.Now().UnixNano()
	currentRunID := uuid.New()
	currentVersion := version - 100
	currentNextEventID := int64(2333)
	req := &h.ReplicateEventsRequest{
		History: &shared.History{
			Events: []*shared.HistoryEvent{
				{
					Version:   common.Int64Ptr(version),
					Timestamp: common.Int64Ptr(now),
				},
			},
		},
	}

	sourceClusterName := cluster.TestAlternativeClusterName
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(version).Return(sourceClusterName).AnyTimes()
	currentClusterName := cluster.TestCurrentClusterName
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(currentVersion).Return(currentClusterName).AnyTimes()

	contextCurrent := NewMockworkflowExecutionContext(s.controller)
	contextCurrent.EXPECT().lock(gomock.Any()).Return(nil).Times(2)
	contextCurrent.EXPECT().unlock().Times(2)
	msBuilderCurrent := NewMockmutableState(s.controller)

	contextCurrent.EXPECT().loadWorkflowExecution().Return(msBuilderCurrent, nil).Times(2)
	currentExecution := &shared.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(currentRunID),
	}
	contextCurrent.EXPECT().getExecution().Return(currentExecution).AnyTimes()
	contextCurrentCacheKey := definition.NewWorkflowIdentifier(domainID, currentExecution.GetWorkflowId(), currentExecution.GetRunId())
	_, _ = s.historyReplicator.historyCache.PutIfNotExist(contextCurrentCacheKey, contextCurrent)

	s.mockExecutionMgr.On("GetCurrentExecution", &persistence.GetCurrentExecutionRequest{
		DomainID:   domainID,
		WorkflowID: workflowID,
	}).Return(&persistence.GetCurrentExecutionResponse{
		RunID:       currentRunID,
		CloseStatus: persistence.WorkflowCloseStatusNone,
	}, nil)

	msBuilderCurrent.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{
		RunID:              currentRunID,
		DecisionVersion:    common.EmptyVersion,
		DecisionScheduleID: common.EmptyEventID,
		DecisionStartedID:  common.EmptyEventID,
	}).AnyTimes()
	msBuilderCurrent.EXPECT().GetLastWriteVersion().Return(currentVersion, nil).AnyTimes()
	msBuilderCurrent.EXPECT().GetNextEventID().Return(currentNextEventID).AnyTimes()
	msBuilderCurrent.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes() // this is used to update the version on mutable state
	msBuilderCurrent.EXPECT().UpdateCurrentVersion(currentVersion, true).Return(nil).Times(1)

	s.mockExecutionMgr.On("GetCurrentExecution", &persistence.GetCurrentExecutionRequest{
		DomainID:   domainID,
		WorkflowID: workflowID,
	}).Return(&persistence.GetCurrentExecutionResponse{
		RunID:       currentRunID,
		CloseStatus: persistence.WorkflowCloseStatusNone,
	}, nil)

	msBuilderCurrent.EXPECT().AddWorkflowExecutionTerminatedEvent(
		currentNextEventID, workflowTerminationReason, gomock.Any(), workflowTerminationIdentity,
	).Return(&workflow.HistoryEvent{}, nil).Times(1)
	contextCurrent.EXPECT().updateWorkflowExecutionAsActive(gomock.Any()).Return(nil).Times(1)

	err := s.historyReplicator.ApplyOtherEventsMissingMutableState(ctx.Background(), domainID, workflowID, runID, req, s.logger)
	s.Equal(newRetryTaskErrorWithHint(ErrWorkflowNotFoundMsg, domainID, workflowID, runID, common.FirstEventID), err)
}

func (s *historyReplicatorSuite) TestApplyOtherEventsMissingMutableState_IncomingNotLessThanCurrent_CurrentFinished() {
	domainName := "some random domain name"
	domainID := testDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	version := int64(123)
	now := time.Now().UnixNano()
	currentRunID := uuid.New()
	currentVersion := version - 100
	currentNextEventID := int64(2333)
	req := &h.ReplicateEventsRequest{
		History: &shared.History{
			Events: []*shared.HistoryEvent{
				{
					Version:   common.Int64Ptr(version),
					Timestamp: common.Int64Ptr(now),
				},
			},
		},
	}

	s.mockDomainCache.EXPECT().GetDomainByID(domainID).Return(
		cache.NewGlobalDomainCacheEntryForTest(
			&persistence.DomainInfo{ID: domainID, Name: domainName},
			&persistence.DomainConfig{Retention: 1},
			&persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					{ClusterName: cluster.TestCurrentClusterName},
					{ClusterName: cluster.TestAlternativeClusterName},
				},
			},
			currentVersion,
			nil,
		), nil,
	).AnyTimes()

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
			ExecutionInfo: &persistence.WorkflowExecutionInfo{
				RunID:              currentRunID,
				NextEventID:        currentNextEventID,
				State:              persistence.WorkflowStateCompleted,
				DecisionVersion:    common.EmptyVersion,
				DecisionScheduleID: common.EmptyEventID,
				DecisionStartedID:  common.EmptyEventID,
			},
			ExecutionStats:   &persistence.ExecutionStats{},
			ReplicationState: &persistence.ReplicationState{LastWriteVersion: currentVersion},
		},
	}, nil)

	err := s.historyReplicator.ApplyOtherEventsMissingMutableState(ctx.Background(), domainID, workflowID, runID, req, s.logger)
	s.Equal(newRetryTaskErrorWithHint(ErrWorkflowNotFoundMsg, domainID, workflowID, runID, common.FirstEventID), err)
}

func (s *historyReplicatorSuite) TestWorkflowReset() {
	domainName := "some random domain name"
	domainID := testDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	version := int64(123)
	now := time.Now().UnixNano()
	currentRunID := uuid.New()
	currentVersion := version - 100
	currentNextEventID := int64(2333)
	req := &h.ReplicateEventsRequest{
		History: &shared.History{
			Events: []*shared.HistoryEvent{
				{
					Version:   common.Int64Ptr(version),
					Timestamp: common.Int64Ptr(now),
				},
			},
		},
		ResetWorkflow: common.BoolPtr(true),
	}

	s.mockDomainCache.EXPECT().GetDomainByID(domainID).Return(
		cache.NewGlobalDomainCacheEntryForTest(
			&persistence.DomainInfo{ID: domainID, Name: domainName},
			&persistence.DomainConfig{Retention: 1},
			&persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					{ClusterName: cluster.TestCurrentClusterName},
					{ClusterName: cluster.TestAlternativeClusterName},
				},
			},
			currentVersion,
			nil,
		), nil,
	).AnyTimes()

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
			ExecutionInfo: &persistence.WorkflowExecutionInfo{
				RunID:              currentRunID,
				NextEventID:        currentNextEventID,
				State:              persistence.WorkflowStateCompleted,
				DecisionVersion:    common.EmptyVersion,
				DecisionScheduleID: common.EmptyEventID,
				DecisionStartedID:  common.EmptyEventID,
			},
			ExecutionStats:   &persistence.ExecutionStats{},
			ReplicationState: &persistence.ReplicationState{LastWriteVersion: currentVersion},
		},
	}, nil)

	reqCtx := ctx.Background()

	s.mockWorkflowResetor.EXPECT().ApplyResetEvent(
		reqCtx, req, domainID, workflowID, currentRunID,
	).Return(nil).Times(1)

	err := s.historyReplicator.ApplyOtherEventsMissingMutableState(reqCtx, domainID, workflowID, runID, req, s.logger)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestApplyOtherEventsMissingMutableState_IncomingLessThanCurrent() {
	domainName := "some random domain name"
	domainID := testDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	version := int64(123)
	now := time.Now().UnixNano()
	currentRunID := uuid.New()
	currentVersion := version + 100
	req := &h.ReplicateEventsRequest{
		History: &shared.History{
			Events: []*shared.HistoryEvent{
				{
					Version:   common.Int64Ptr(version),
					Timestamp: common.Int64Ptr(now),
				},
			},
		},
	}

	s.mockDomainCache.EXPECT().GetDomainByID(domainID).Return(
		cache.NewGlobalDomainCacheEntryForTest(
			&persistence.DomainInfo{ID: domainID, Name: domainName},
			&persistence.DomainConfig{Retention: 1},
			&persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					{ClusterName: cluster.TestCurrentClusterName},
					{ClusterName: cluster.TestAlternativeClusterName},
				},
			},
			currentVersion,
			nil,
		), nil,
	).AnyTimes()

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
			ExecutionInfo: &persistence.WorkflowExecutionInfo{
				RunID:              currentRunID,
				DecisionVersion:    common.EmptyVersion,
				DecisionScheduleID: common.EmptyEventID,
				DecisionStartedID:  common.EmptyEventID,
			},
			ExecutionStats:   &persistence.ExecutionStats{},
			ReplicationState: &persistence.ReplicationState{LastWriteVersion: currentVersion},
		},
	}, nil)

	err := s.historyReplicator.ApplyOtherEventsMissingMutableState(ctx.Background(), domainID, workflowID, runID, req, s.logger)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestApplyOtherEventsVersionChecking_IncomingLessThanCurrent_WorkflowClosed_WorkflowIsCurrent_NoEventsReapplication() {
	domainID := testDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	incomingVersion := int64(110)
	currentLastWriteVersion := int64(123)

	context := NewMockworkflowExecutionContext(s.controller)
	context.EXPECT().getDomainID().Return(domainID).AnyTimes()
	context.EXPECT().getExecution().Return(&shared.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(runID),
	}).AnyTimes()

	msBuilderIn := NewMockmutableState(s.controller)
	request := &h.ReplicateEventsRequest{
		Version: common.Int64Ptr(incomingVersion),
		History: &shared.History{Events: []*shared.HistoryEvent{
			{
				EventType: shared.EventTypeWorkflowExecutionCanceled.Ptr(),
				Timestamp: common.Int64Ptr(time.Now().UnixNano()),
			},
		}},
	}
	msBuilderIn.EXPECT().IsWorkflowExecutionRunning().Return(false).AnyTimes()
	msBuilderIn.EXPECT().GetReplicationState().Return(&persistence.ReplicationState{LastWriteVersion: currentLastWriteVersion}).AnyTimes()
	s.mockExecutionMgr.On("GetCurrentExecution", &persistence.GetCurrentExecutionRequest{
		DomainID:   domainID,
		WorkflowID: workflowID,
	}).Return(&persistence.GetCurrentExecutionResponse{
		RunID: runID,
		// other attributes are not used
	}, nil)

	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(currentLastWriteVersion).Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()

	msBuilderOut, err := s.historyReplicator.ApplyOtherEventsVersionChecking(ctx.Background(), context, msBuilderIn,
		request, s.logger)
	s.Nil(msBuilderOut)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestApplyOtherEventsVersionChecking_IncomingLessThanCurrent_WorkflowClosed_WorkflowIsNotCurrent_NoEventsReapplication() {
	domainID := testDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	currentRunID := uuid.New()
	incomingVersion := int64(110)
	lastWriteVersion := int64(123)

	context := NewMockworkflowExecutionContext(s.controller)
	context.EXPECT().getDomainID().Return(domainID).AnyTimes()
	context.EXPECT().getExecution().Return(&shared.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(runID),
	}).AnyTimes()

	msBuilderIn := NewMockmutableState(s.controller)
	request := &h.ReplicateEventsRequest{
		Version: common.Int64Ptr(incomingVersion),
		History: &shared.History{Events: []*shared.HistoryEvent{
			{
				EventType: shared.EventTypeWorkflowExecutionCanceled.Ptr(),
				Timestamp: common.Int64Ptr(time.Now().UnixNano()),
			},
		}},
	}
	msBuilderIn.EXPECT().IsWorkflowExecutionRunning().Return(false).AnyTimes()
	msBuilderIn.EXPECT().GetReplicationState().Return(&persistence.ReplicationState{LastWriteVersion: lastWriteVersion}).AnyTimes()
	s.mockExecutionMgr.On("GetCurrentExecution", &persistence.GetCurrentExecutionRequest{
		DomainID:   domainID,
		WorkflowID: workflowID,
	}).Return(&persistence.GetCurrentExecutionResponse{
		RunID: currentRunID,
		// other attributes are not used
	}, nil)

	contextCurrent := NewMockworkflowExecutionContext(s.controller)
	contextCurrent.EXPECT().lock(gomock.Any()).Return(nil).Times(1)
	contextCurrent.EXPECT().unlock().Times(1)

	msBuilderCurrent := NewMockmutableState(s.controller)

	contextCurrent.EXPECT().loadWorkflowExecution().Return(msBuilderCurrent, nil).Times(1)
	currentExecution := &shared.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(currentRunID),
	}
	contextCurrentCacheKey := definition.NewWorkflowIdentifier(domainID, currentExecution.GetWorkflowId(), currentExecution.GetRunId())
	_, _ = s.historyReplicator.historyCache.PutIfNotExist(contextCurrentCacheKey, contextCurrent)

	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(lastWriteVersion).Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()

	msBuilderOut, err := s.historyReplicator.ApplyOtherEventsVersionChecking(ctx.Background(), context, msBuilderIn,
		request, s.logger)
	s.Nil(msBuilderOut)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestApplyOtherEventsVersionChecking_IncomingLessThanCurrent_WorkflowClosed_WorkflowIsNotCurrent_EventsReapplication_PendingDecision() {
	domainID := testDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	currentRunID := uuid.New()
	incomingVersion := int64(110)
	lastWriteVersion := int64(123)
	currentLastWriteVersion := lastWriteVersion

	signalName := "some random signal name"
	signalInput := []byte("some random signal input")
	signalIdentity := "some random signal identity"

	context := NewMockworkflowExecutionContext(s.controller)
	context.EXPECT().getDomainID().Return(domainID).AnyTimes()
	context.EXPECT().getExecution().Return(&shared.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(runID),
	}).AnyTimes()

	msBuilderIn := NewMockmutableState(s.controller)
	request := &h.ReplicateEventsRequest{
		Version: common.Int64Ptr(incomingVersion),
		History: &shared.History{Events: []*shared.HistoryEvent{
			{
				EventType: shared.EventTypeWorkflowExecutionSignaled.Ptr(),
				Timestamp: common.Int64Ptr(time.Now().UnixNano()),
				WorkflowExecutionSignaledEventAttributes: &shared.WorkflowExecutionSignaledEventAttributes{
					SignalName: common.StringPtr(signalName),
					Input:      signalInput,
					Identity:   common.StringPtr(signalIdentity),
				},
			},
		}},
	}
	msBuilderIn.EXPECT().IsWorkflowExecutionRunning().Return(false).AnyTimes()
	msBuilderIn.EXPECT().GetReplicationState().Return(&persistence.ReplicationState{LastWriteVersion: lastWriteVersion}).AnyTimes()
	s.mockExecutionMgr.On("GetCurrentExecution", &persistence.GetCurrentExecutionRequest{
		DomainID:   domainID,
		WorkflowID: workflowID,
	}).Return(&persistence.GetCurrentExecutionResponse{
		RunID: currentRunID,
		// other attributes are not used
	}, nil)

	contextCurrent := NewMockworkflowExecutionContext(s.controller)
	contextCurrent.EXPECT().lock(gomock.Any()).Return(nil).Times(1)
	contextCurrent.EXPECT().unlock().Times(1)

	msBuilderCurrent := NewMockmutableState(s.controller)

	contextCurrent.EXPECT().loadWorkflowExecution().Return(msBuilderCurrent, nil).Times(1)
	currentExecution := &shared.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(currentRunID),
	}
	contextCurrentCacheKey := definition.NewWorkflowIdentifier(domainID, currentExecution.GetWorkflowId(), currentExecution.GetRunId())
	_, _ = s.historyReplicator.historyCache.PutIfNotExist(contextCurrentCacheKey, contextCurrent)

	msBuilderCurrent.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes()
	msBuilderCurrent.EXPECT().GetLastWriteVersion().Return(currentLastWriteVersion, nil).AnyTimes()
	msBuilderCurrent.EXPECT().UpdateCurrentVersion(currentLastWriteVersion, true).Return(nil).Times(1)
	msBuilderCurrent.EXPECT().AddWorkflowExecutionSignaled(signalName, signalInput, signalIdentity).Return(&shared.HistoryEvent{
		EventType: shared.EventTypeWorkflowExecutionSignaled.Ptr(),
		Timestamp: common.Int64Ptr(time.Now().UnixNano()),
		WorkflowExecutionSignaledEventAttributes: &shared.WorkflowExecutionSignaledEventAttributes{
			SignalName: common.StringPtr(signalName),
			Input:      signalInput,
			Identity:   common.StringPtr(signalIdentity),
		},
	}, nil).Times(1)
	msBuilderCurrent.EXPECT().HasPendingDecision().Return(true).Times(1)

	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(currentLastWriteVersion).Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()

	contextCurrent.EXPECT().updateWorkflowExecutionAsActive(gomock.Any()).Return(nil).Times(1)

	msBuilderOut, err := s.historyReplicator.ApplyOtherEventsVersionChecking(ctx.Background(), context, msBuilderIn,
		request, s.logger)
	s.Nil(msBuilderOut)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestApplyOtherEventsVersionChecking_IncomingLessThanCurrent_WorkflowClosed_WorkflowIsNotCurrent_EventsReapplication_NoPendingDecision() {
	domainID := testDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	currentRunID := uuid.New()
	incomingVersion := int64(110)
	lastWriteVersion := int64(123)
	currentLastWriteVersion := lastWriteVersion

	signalName := "some random signal name"
	signalInput := []byte("some random signal input")
	signalIdentity := "some random signal identity"

	decisionStickyTasklist := "some random decision sticky tasklist"

	context := NewMockworkflowExecutionContext(s.controller)
	context.EXPECT().getDomainID().Return(domainID).AnyTimes()
	context.EXPECT().getExecution().Return(&shared.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(runID),
	}).AnyTimes()

	msBuilderIn := NewMockmutableState(s.controller)
	request := &h.ReplicateEventsRequest{
		Version: common.Int64Ptr(incomingVersion),
		History: &shared.History{Events: []*shared.HistoryEvent{
			{
				EventType: shared.EventTypeWorkflowExecutionSignaled.Ptr(),
				Timestamp: common.Int64Ptr(time.Now().UnixNano()),
				WorkflowExecutionSignaledEventAttributes: &shared.WorkflowExecutionSignaledEventAttributes{
					SignalName: common.StringPtr(signalName),
					Input:      signalInput,
					Identity:   common.StringPtr(signalIdentity),
				},
			},
		}},
	}
	msBuilderIn.EXPECT().IsWorkflowExecutionRunning().Return(false).AnyTimes()
	msBuilderIn.EXPECT().GetReplicationState().Return(&persistence.ReplicationState{LastWriteVersion: lastWriteVersion}).AnyTimes()
	s.mockExecutionMgr.On("GetCurrentExecution", &persistence.GetCurrentExecutionRequest{
		DomainID:   domainID,
		WorkflowID: workflowID,
	}).Return(&persistence.GetCurrentExecutionResponse{
		RunID: currentRunID,
		// other attributes are not used
	}, nil)

	contextCurrent := NewMockworkflowExecutionContext(s.controller)
	contextCurrent.EXPECT().lock(gomock.Any()).Return(nil).Times(1)
	contextCurrent.EXPECT().unlock().Times(1)

	msBuilderCurrent := NewMockmutableState(s.controller)

	contextCurrent.EXPECT().loadWorkflowExecution().Return(msBuilderCurrent, nil).Times(1)
	currentExecution := &shared.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(currentRunID),
	}
	contextCurrentCacheKey := definition.NewWorkflowIdentifier(domainID, currentExecution.GetWorkflowId(), currentExecution.GetRunId())
	_, _ = s.historyReplicator.historyCache.PutIfNotExist(contextCurrentCacheKey, contextCurrent)

	msBuilderCurrent.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes()
	msBuilderCurrent.EXPECT().GetLastWriteVersion().Return(currentLastWriteVersion, nil).AnyTimes()
	msBuilderCurrent.EXPECT().UpdateCurrentVersion(currentLastWriteVersion, true).Return(nil).Times(1)
	msBuilderCurrent.EXPECT().AddWorkflowExecutionSignaled(signalName, signalInput, signalIdentity).Return(&shared.HistoryEvent{
		EventType: shared.EventTypeWorkflowExecutionSignaled.Ptr(),
		Timestamp: common.Int64Ptr(time.Now().UnixNano()),
		WorkflowExecutionSignaledEventAttributes: &shared.WorkflowExecutionSignaledEventAttributes{
			SignalName: common.StringPtr(signalName),
			Input:      signalInput,
			Identity:   common.StringPtr(signalIdentity),
		},
	}, nil).Times(1)
	msBuilderCurrent.EXPECT().HasPendingDecision().Return(false).Times(1)

	newDecision := &decisionInfo{
		Version:    currentLastWriteVersion,
		ScheduleID: 1234,
		StartedID:  common.EmptyEventID,
		TaskList:   decisionStickyTasklist,
		Attempt:    0,
	}
	msBuilderCurrent.EXPECT().AddDecisionTaskScheduledEvent(false).Return(newDecision, nil).Times(1)

	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(currentLastWriteVersion).Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()

	contextCurrent.EXPECT().updateWorkflowExecutionAsActive(gomock.Any()).Return(nil).Times(1)

	msBuilderOut, err := s.historyReplicator.ApplyOtherEventsVersionChecking(ctx.Background(), context, msBuilderIn,
		request, s.logger)
	s.Nil(msBuilderOut)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestApplyOtherEventsVersionChecking_IncomingLessThanCurrent_WorkflowRunning_NoEventsReapplication() {
	incomingVersion := int64(110)
	currentLastWriteVersion := int64(123)

	context := NewMockworkflowExecutionContext(s.controller)
	msBuilderIn := NewMockmutableState(s.controller)
	request := &h.ReplicateEventsRequest{
		Version: common.Int64Ptr(incomingVersion),
		History: &shared.History{Events: []*shared.HistoryEvent{
			{
				EventType: shared.EventTypeWorkflowExecutionCanceled.Ptr(),
				Timestamp: common.Int64Ptr(time.Now().UnixNano()),
			},
		}},
	}
	msBuilderIn.EXPECT().IsWorkflowExecutionRunning().Return(true).Times(1)
	msBuilderIn.EXPECT().GetReplicationState().Return(&persistence.ReplicationState{LastWriteVersion: currentLastWriteVersion}).AnyTimes()

	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(currentLastWriteVersion).Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()

	msBuilderOut, err := s.historyReplicator.ApplyOtherEventsVersionChecking(ctx.Background(), context, msBuilderIn,
		request, s.logger)
	s.Nil(msBuilderOut)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestApplyOtherEventsVersionChecking_IncomingLessThanCurrent_WorkflowRunning_EventsReapplication_PendingDecision() {
	incomingVersion := int64(110)
	currentLastWriteVersion := int64(123)

	signalName := "some random signal name"
	signalInput := []byte("some random signal input")
	signalIdentity := "some random signal identity"

	context := NewMockworkflowExecutionContext(s.controller)
	msBuilderIn := NewMockmutableState(s.controller)
	request := &h.ReplicateEventsRequest{
		Version: common.Int64Ptr(incomingVersion),
		History: &shared.History{Events: []*shared.HistoryEvent{
			{
				EventType: shared.EventTypeWorkflowExecutionSignaled.Ptr(),
				Timestamp: common.Int64Ptr(time.Now().UnixNano()),
				WorkflowExecutionSignaledEventAttributes: &shared.WorkflowExecutionSignaledEventAttributes{
					SignalName: common.StringPtr(signalName),
					Input:      signalInput,
					Identity:   common.StringPtr(signalIdentity),
				},
			},
		}},
	}
	msBuilderIn.EXPECT().GetReplicationState().Return(&persistence.ReplicationState{LastWriteVersion: currentLastWriteVersion}).AnyTimes()
	msBuilderIn.EXPECT().GetLastWriteVersion().Return(currentLastWriteVersion, nil).AnyTimes()
	msBuilderIn.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes()
	msBuilderIn.EXPECT().UpdateCurrentVersion(currentLastWriteVersion, true).Return(nil).Times(1)
	msBuilderIn.EXPECT().AddWorkflowExecutionSignaled(signalName, signalInput, signalIdentity).Return(&shared.HistoryEvent{
		EventType: shared.EventTypeWorkflowExecutionSignaled.Ptr(),
		Timestamp: common.Int64Ptr(time.Now().UnixNano()),
		WorkflowExecutionSignaledEventAttributes: &shared.WorkflowExecutionSignaledEventAttributes{
			SignalName: common.StringPtr(signalName),
			Input:      signalInput,
			Identity:   common.StringPtr(signalIdentity),
		},
	}, nil).Times(1)
	msBuilderIn.EXPECT().HasPendingDecision().Return(true).Times(1)

	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(currentLastWriteVersion).Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()

	context.EXPECT().updateWorkflowExecutionAsActive(gomock.Any()).Return(nil).Times(1)
	msBuilderOut, err := s.historyReplicator.ApplyOtherEventsVersionChecking(ctx.Background(), context, msBuilderIn,
		request, s.logger)
	s.Nil(msBuilderOut)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestApplyOtherEventsVersionChecking_IncomingLessThanCurrent_WorkflowRunning_EventsReapplication_NoPendingDecision() {
	incomingVersion := int64(110)
	currentLastWriteVersion := int64(123)

	signalName := "some random signal name"
	signalInput := []byte("some random signal input")
	signalIdentity := "some random signal identity"

	decisionStickyTasklist := "some random decision sticky tasklist"

	context := NewMockworkflowExecutionContext(s.controller)
	msBuilderIn := NewMockmutableState(s.controller)
	request := &h.ReplicateEventsRequest{
		Version: common.Int64Ptr(incomingVersion),
		History: &shared.History{Events: []*shared.HistoryEvent{
			{
				EventType: shared.EventTypeWorkflowExecutionSignaled.Ptr(),
				Timestamp: common.Int64Ptr(time.Now().UnixNano()),
				WorkflowExecutionSignaledEventAttributes: &shared.WorkflowExecutionSignaledEventAttributes{
					SignalName: common.StringPtr(signalName),
					Input:      signalInput,
					Identity:   common.StringPtr(signalIdentity),
				},
			},
		}},
	}
	msBuilderIn.EXPECT().GetReplicationState().Return(&persistence.ReplicationState{LastWriteVersion: currentLastWriteVersion}).AnyTimes()
	msBuilderIn.EXPECT().GetLastWriteVersion().Return(currentLastWriteVersion, nil).AnyTimes()
	msBuilderIn.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes()
	msBuilderIn.EXPECT().UpdateCurrentVersion(currentLastWriteVersion, true).Return(nil).Times(1)
	msBuilderIn.EXPECT().AddWorkflowExecutionSignaled(signalName, signalInput, signalIdentity).Return(&shared.HistoryEvent{
		EventType: shared.EventTypeWorkflowExecutionSignaled.Ptr(),
		Timestamp: common.Int64Ptr(time.Now().UnixNano()),
		WorkflowExecutionSignaledEventAttributes: &shared.WorkflowExecutionSignaledEventAttributes{
			SignalName: common.StringPtr(signalName),
			Input:      signalInput,
			Identity:   common.StringPtr(signalIdentity),
		},
	}, nil).Times(1)
	msBuilderIn.EXPECT().HasPendingDecision().Return(false).Times(1)

	newDecision := &decisionInfo{
		Version:    currentLastWriteVersion,
		ScheduleID: 1234,
		StartedID:  common.EmptyEventID,
		TaskList:   decisionStickyTasklist,
		Attempt:    0,
	}
	msBuilderIn.EXPECT().AddDecisionTaskScheduledEvent(false).Return(newDecision, nil).Times(1)

	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(currentLastWriteVersion).Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()

	context.EXPECT().updateWorkflowExecutionAsActive(gomock.Any()).Return(nil).Times(1)

	msBuilderOut, err := s.historyReplicator.ApplyOtherEventsVersionChecking(ctx.Background(), context, msBuilderIn,
		request, s.logger)
	s.Nil(msBuilderOut)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestApplyOtherEventsVersionChecking_IncomingEqualToCurrent() {
	incomingVersion := int64(110)
	currentLastWriteVersion := incomingVersion

	context := NewMockworkflowExecutionContext(s.controller)
	msBuilderIn := NewMockmutableState(s.controller)
	request := &h.ReplicateEventsRequest{
		Version: common.Int64Ptr(incomingVersion),
		History: &shared.History{Events: []*shared.HistoryEvent{
			{Timestamp: common.Int64Ptr(time.Now().UnixNano())},
		}},
	}
	msBuilderIn.EXPECT().GetReplicationState().Return(&persistence.ReplicationState{LastWriteVersion: currentLastWriteVersion}).AnyTimes()

	msBuilderOut, err := s.historyReplicator.ApplyOtherEventsVersionChecking(ctx.Background(), context, msBuilderIn, request, s.logger)
	s.Equal(msBuilderIn, msBuilderOut)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestApplyOtherEventsVersionChecking_IncomingGreaterThanCurrent_CurrentWasNotActive_SameCluster() {
	currentLastWriteVersion := int64(10)
	incomingVersion := currentLastWriteVersion + 10

	prevActiveCluster := cluster.TestAlternativeClusterName
	context := NewMockworkflowExecutionContext(s.controller)
	msBuilderIn := NewMockmutableState(s.controller)

	request := &h.ReplicateEventsRequest{
		Version: common.Int64Ptr(incomingVersion),
		History: &shared.History{Events: []*shared.HistoryEvent{
			{Timestamp: common.Int64Ptr(time.Now().UnixNano())},
		}},
	}
	msBuilderIn.EXPECT().GetReplicationState().Return(&persistence.ReplicationState{
		LastWriteVersion: currentLastWriteVersion,
	}).AnyTimes()

	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(currentLastWriteVersion).Return(prevActiveCluster).AnyTimes()
	s.mockClusterMetadata.EXPECT().IsVersionFromSameCluster(incomingVersion, currentLastWriteVersion).Return(true).AnyTimes()

	msBuilderOut, err := s.historyReplicator.ApplyOtherEventsVersionChecking(ctx.Background(), context, msBuilderIn,
		request, s.logger)
	s.Equal(msBuilderIn, msBuilderOut)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestApplyOtherEventsVersionChecking_IncomingGreaterThanCurrent_CurrentWasNotActive_DiffCluster() {
	currentLastWriteVersion := int64(10)
	incomingVersion := currentLastWriteVersion + 10

	prevActiveCluster := cluster.TestAlternativeClusterName
	context := NewMockworkflowExecutionContext(s.controller)
	msBuilderIn := NewMockmutableState(s.controller)

	request := &h.ReplicateEventsRequest{
		Version: common.Int64Ptr(incomingVersion),
		History: &shared.History{Events: []*shared.HistoryEvent{
			{Timestamp: common.Int64Ptr(time.Now().UnixNano())},
		}},
	}
	msBuilderIn.EXPECT().GetReplicationState().Return(&persistence.ReplicationState{
		LastWriteVersion: currentLastWriteVersion,
	}).AnyTimes()

	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(currentLastWriteVersion).Return(prevActiveCluster).AnyTimes()
	s.mockClusterMetadata.EXPECT().IsVersionFromSameCluster(incomingVersion, currentLastWriteVersion).Return(false).AnyTimes()

	msBuilderOut, err := s.historyReplicator.ApplyOtherEventsVersionChecking(ctx.Background(), context, msBuilderIn,
		request, s.logger)
	s.Nil(msBuilderOut)
	s.Equal(ErrMoreThan2DC, err)
}

func (s *historyReplicatorSuite) TestApplyOtherEventsVersionChecking_IncomingGreaterThanCurrent_CurrentWasActive_MissingReplicationInfo() {
	runID := uuid.New()

	currentLastWriteVersion := int64(10)
	currentLastEventID := int64(98)
	currentReplicationInfoLastWriteVersion := currentLastWriteVersion - 10
	currentReplicationInfoLastEventID := currentLastEventID - 11
	incomingVersion := currentLastWriteVersion + 10

	updateCondition := int64(1394)

	incomingActiveCluster := cluster.TestAlternativeClusterName
	prevActiveCluster := cluster.TestCurrentClusterName
	context := NewMockworkflowExecutionContext(s.controller)
	msBuilderIn := NewMockmutableState(s.controller)

	request := &h.ReplicateEventsRequest{
		Version:         common.Int64Ptr(incomingVersion),
		ReplicationInfo: map[string]*shared.ReplicationInfo{},
		History: &shared.History{Events: []*shared.HistoryEvent{
			{Timestamp: common.Int64Ptr(time.Now().UnixNano())},
		}},
	}
	startTimeStamp := time.Now()
	msBuilderIn.EXPECT().GetReplicationState().Return(&persistence.ReplicationState{
		LastWriteVersion: currentLastWriteVersion,
		LastWriteEventID: currentLastEventID,
		LastReplicationInfo: map[string]*persistence.ReplicationInfo{
			incomingActiveCluster: {
				Version:     currentReplicationInfoLastWriteVersion,
				LastEventID: currentReplicationInfoLastEventID,
			},
		},
	}).AnyTimes()
	currentState := persistence.WorkflowStateRunning
	exeInfo := &persistence.WorkflowExecutionInfo{
		StartTimestamp:     startTimeStamp,
		RunID:              runID,
		State:              currentState,
		CloseStatus:        persistence.WorkflowCloseStatusNone,
		DecisionVersion:    common.EmptyVersion,
		DecisionScheduleID: common.EmptyEventID,
		DecisionStartedID:  common.EmptyEventID,
	}
	msBuilderIn.EXPECT().GetExecutionInfo().Return(exeInfo).AnyTimes()
	msBuilderIn.EXPECT().IsWorkflowExecutionRunning().Return(currentState != persistence.WorkflowStateCompleted).AnyTimes()
	msBuilderIn.EXPECT().GetLastWriteVersion().Return(currentLastWriteVersion, nil).AnyTimes()
	msBuilderIn.EXPECT().GetUpdateCondition().Return(updateCondition).AnyTimes()
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(currentLastWriteVersion).Return(prevActiveCluster).AnyTimes()

	mockConflictResolver := NewMockconflictResolver(s.controller)
	s.historyReplicator.getNewConflictResolver = func(context workflowExecutionContext, logger log.Logger) conflictResolver {
		return mockConflictResolver
	}
	msBuilderMid := NewMockmutableState(s.controller)
	msBuilderMid.EXPECT().GetNextEventID().Return(int64(12345)).AnyTimes() // this is used by log
	mockConflictResolver.EXPECT().reset(
		runID, currentLastWriteVersion, currentState, gomock.Any(), currentReplicationInfoLastEventID, exeInfo, updateCondition,
	).Return(msBuilderMid, nil).Times(1)
	msBuilderOut, err := s.historyReplicator.ApplyOtherEventsVersionChecking(ctx.Background(), context, msBuilderIn, request, s.logger)
	s.Equal(msBuilderMid, msBuilderOut)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestApplyOtherEventsVersionChecking_IncomingGreaterThanCurrent_CurrentWasActive_ReplicationInfoVersionLocalLarger() {
	runID := uuid.New()

	currentLastWriteVersion := int64(120)
	currentLastEventID := int64(980)
	currentReplicationInfoLastWriteVersion := currentLastWriteVersion - 20
	currentReplicationInfoLastEventID := currentLastEventID - 15
	incomingVersion := currentLastWriteVersion + 10
	incomingReplicationInfoLastWriteVersion := currentReplicationInfoLastWriteVersion - 10
	incomingReplicationInfoLastEventID := currentReplicationInfoLastEventID - 20

	updateCondition := int64(1394)

	incomingActiveCluster := cluster.TestAlternativeClusterName
	prevActiveCluster := cluster.TestCurrentClusterName
	context := NewMockworkflowExecutionContext(s.controller)
	msBuilderIn := NewMockmutableState(s.controller)

	request := &h.ReplicateEventsRequest{
		Version: common.Int64Ptr(incomingVersion),
		ReplicationInfo: map[string]*shared.ReplicationInfo{
			prevActiveCluster: {
				Version:     common.Int64Ptr(incomingReplicationInfoLastWriteVersion),
				LastEventId: common.Int64Ptr(incomingReplicationInfoLastEventID),
			},
		},
		History: &shared.History{Events: []*shared.HistoryEvent{
			{Timestamp: common.Int64Ptr(time.Now().UnixNano())},
		}},
	}
	startTimeStamp := time.Now()
	msBuilderIn.EXPECT().GetReplicationState().Return(&persistence.ReplicationState{
		LastWriteVersion: currentLastWriteVersion,
		LastWriteEventID: currentLastEventID,
		LastReplicationInfo: map[string]*persistence.ReplicationInfo{
			incomingActiveCluster: {
				Version:     currentReplicationInfoLastWriteVersion,
				LastEventID: currentReplicationInfoLastEventID,
			},
		},
	}).AnyTimes()
	currentState := persistence.WorkflowStateRunning
	exeInfo := &persistence.WorkflowExecutionInfo{
		StartTimestamp:     startTimeStamp,
		RunID:              runID,
		State:              currentState,
		CloseStatus:        persistence.WorkflowCloseStatusNone,
		DecisionVersion:    common.EmptyVersion,
		DecisionScheduleID: common.EmptyEventID,
		DecisionStartedID:  common.EmptyEventID,
	}
	msBuilderIn.EXPECT().GetExecutionInfo().Return(exeInfo).AnyTimes()
	msBuilderIn.EXPECT().IsWorkflowExecutionRunning().Return(currentState != persistence.WorkflowStateCompleted)
	msBuilderIn.EXPECT().GetLastWriteVersion().Return(currentLastWriteVersion, nil).AnyTimes()
	msBuilderIn.EXPECT().GetUpdateCondition().Return(updateCondition).AnyTimes()
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(currentLastWriteVersion).Return(prevActiveCluster).AnyTimes()

	mockConflictResolver := NewMockconflictResolver(s.controller)
	s.historyReplicator.getNewConflictResolver = func(context workflowExecutionContext, logger log.Logger) conflictResolver {
		return mockConflictResolver
	}
	msBuilderMid := NewMockmutableState(s.controller)
	msBuilderMid.EXPECT().GetNextEventID().Return(int64(12345)).AnyTimes() // this is used by log
	mockConflictResolver.EXPECT().reset(
		runID, currentLastWriteVersion, currentState, gomock.Any(), currentReplicationInfoLastEventID, exeInfo, updateCondition,
	).Return(msBuilderMid, nil).Times(1)
	msBuilderOut, err := s.historyReplicator.ApplyOtherEventsVersionChecking(ctx.Background(), context, msBuilderIn, request, s.logger)
	s.Equal(msBuilderMid, msBuilderOut)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestApplyOtherEventsVersionChecking_IncomingGreaterThanCurrent_CurrentWasActive_ReplicationInfoVersionLocalSmaller() {
	currentLastWriteVersion := int64(10)
	currentLastEventID := int64(98)
	incomingVersion := currentLastWriteVersion + 10
	incomingReplicationInfoLastWriteVersion := currentLastWriteVersion + 10
	incomingReplicationInfoLastEventID := currentLastEventID

	prevActiveCluster := cluster.TestCurrentClusterName
	context := NewMockworkflowExecutionContext(s.controller)
	msBuilderIn := NewMockmutableState(s.controller)

	request := &h.ReplicateEventsRequest{
		Version: common.Int64Ptr(incomingVersion),
		ReplicationInfo: map[string]*shared.ReplicationInfo{
			prevActiveCluster: {
				Version:     common.Int64Ptr(incomingReplicationInfoLastWriteVersion),
				LastEventId: common.Int64Ptr(incomingReplicationInfoLastEventID),
			},
		},
		History: &shared.History{Events: []*shared.HistoryEvent{
			{Timestamp: common.Int64Ptr(time.Now().UnixNano())},
		}},
	}
	msBuilderIn.EXPECT().GetReplicationState().Return(&persistence.ReplicationState{
		LastWriteVersion: currentLastWriteVersion,
		LastWriteEventID: currentLastEventID,
	}).AnyTimes()
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(currentLastWriteVersion).Return(prevActiveCluster).AnyTimes()

	msBuilderOut, err := s.historyReplicator.ApplyOtherEventsVersionChecking(ctx.Background(), context, msBuilderIn, request, s.logger)
	s.Nil(msBuilderOut)
	s.Equal(ErrImpossibleRemoteClaimSeenHigherVersion, err)
}

func (s *historyReplicatorSuite) TestApplyOtherEventsVersionChecking_IncomingGreaterThanCurrent_CurrentWasActive_ReplicationInfoVersionEqual_ResolveConflict() {
	runID := uuid.New()

	currentLastWriteVersion := int64(10)
	currentLastEventID := int64(98)
	incomingVersion := currentLastWriteVersion + 10
	incomingReplicationInfoLastWriteVersion := currentLastWriteVersion
	incomingReplicationInfoLastEventID := currentLastEventID - 10

	updateCondition := int64(1394)

	prevActiveCluster := cluster.TestCurrentClusterName
	context := NewMockworkflowExecutionContext(s.controller)
	msBuilderIn := NewMockmutableState(s.controller)

	request := &h.ReplicateEventsRequest{
		Version: common.Int64Ptr(incomingVersion),
		ReplicationInfo: map[string]*shared.ReplicationInfo{
			prevActiveCluster: {
				Version:     common.Int64Ptr(incomingReplicationInfoLastWriteVersion),
				LastEventId: common.Int64Ptr(incomingReplicationInfoLastEventID),
			},
		},
		History: &shared.History{Events: []*shared.HistoryEvent{
			{Timestamp: common.Int64Ptr(time.Now().UnixNano())},
		}},
	}
	startTimeStamp := time.Now()
	msBuilderIn.EXPECT().GetReplicationState().Return(&persistence.ReplicationState{
		LastWriteVersion: currentLastWriteVersion,
		LastWriteEventID: currentLastEventID,
	}).AnyTimes()
	msBuilderIn.EXPECT().HasBufferedEvents().Return(false).Times(1)
	currentState := persistence.WorkflowStateCreated
	exeInfo := &persistence.WorkflowExecutionInfo{
		StartTimestamp:     startTimeStamp,
		RunID:              runID,
		State:              currentState,
		CloseStatus:        persistence.WorkflowCloseStatusNone,
		DecisionVersion:    common.EmptyVersion,
		DecisionScheduleID: common.EmptyEventID,
		DecisionStartedID:  common.EmptyEventID,
	}
	msBuilderIn.EXPECT().GetExecutionInfo().Return(exeInfo).AnyTimes()
	msBuilderIn.EXPECT().IsWorkflowExecutionRunning().Return(currentState != persistence.WorkflowStateCompleted).AnyTimes()
	msBuilderIn.EXPECT().GetLastWriteVersion().Return(currentLastWriteVersion, nil).AnyTimes()
	msBuilderIn.EXPECT().GetUpdateCondition().Return(updateCondition).AnyTimes()
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(currentLastWriteVersion).Return(prevActiveCluster).AnyTimes()

	mockConflictResolver := NewMockconflictResolver(s.controller)
	s.historyReplicator.getNewConflictResolver = func(context workflowExecutionContext, logger log.Logger) conflictResolver {
		return mockConflictResolver
	}
	msBuilderMid := NewMockmutableState(s.controller)
	msBuilderMid.EXPECT().GetNextEventID().Return(int64(12345)).AnyTimes() // this is used by log
	mockConflictResolver.EXPECT().reset(
		runID, currentLastWriteVersion, currentState, gomock.Any(), incomingReplicationInfoLastEventID, exeInfo, updateCondition,
	).Return(msBuilderMid, nil).Times(1)
	msBuilderOut, err := s.historyReplicator.ApplyOtherEventsVersionChecking(ctx.Background(), context, msBuilderIn, request, s.logger)
	s.Equal(msBuilderMid, msBuilderOut)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestApplyOtherEventsVersionChecking_IncomingGreaterThanCurrent_CurrentWasActive_ReplicationInfoVersionEqual_Corrputed() {
	currentLastWriteVersion := int64(10)
	currentLastEventID := int64(98)
	incomingVersion := currentLastWriteVersion + 10
	incomingReplicationInfoLastWriteVersion := currentLastWriteVersion
	incomingReplicationInfoLastEventID := currentLastEventID + 10

	prevActiveCluster := cluster.TestCurrentClusterName
	context := NewMockworkflowExecutionContext(s.controller)
	msBuilderIn := NewMockmutableState(s.controller)

	request := &h.ReplicateEventsRequest{
		Version: common.Int64Ptr(incomingVersion),
		ReplicationInfo: map[string]*shared.ReplicationInfo{
			prevActiveCluster: {
				Version:     common.Int64Ptr(incomingReplicationInfoLastWriteVersion),
				LastEventId: common.Int64Ptr(incomingReplicationInfoLastEventID),
			},
		},
		History: &shared.History{Events: []*shared.HistoryEvent{
			{Timestamp: common.Int64Ptr(time.Now().UnixNano())},
		}},
	}
	msBuilderIn.EXPECT().GetReplicationState().Return(&persistence.ReplicationState{
		LastWriteVersion: currentLastWriteVersion,
		LastWriteEventID: currentLastEventID,
	}).AnyTimes()
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(currentLastWriteVersion).Return(prevActiveCluster).AnyTimes()

	msBuilderOut, err := s.historyReplicator.ApplyOtherEventsVersionChecking(ctx.Background(), context, msBuilderIn, request, s.logger)
	s.Nil(msBuilderOut)
	s.Equal(ErrCorruptedReplicationInfo, err)
}

func (s *historyReplicatorSuite) TestApplyOtherEventsVersionChecking_IncomingGreaterThanCurrent_CurrentWasActive_ReplicationInfoVersionEqual_ResolveConflict_OtherCase() {
	// other cases will be tested in TestConflictResolutionTerminateContinueAsNew
}

func (s *historyReplicatorSuite) TestApplyOtherEventsVersionChecking_IncomingGreaterThanCurrent_CurrentWasActive_ReplicationInfoVersionEqual_NoBufferedEvent_NoOp() {
	currentLastWriteVersion := int64(10)
	currentLastEventID := int64(98)
	incomingVersion := currentLastWriteVersion + 10
	incomingReplicationInfoLastWriteVersion := currentLastWriteVersion
	incomingReplicationInfoLastEventID := currentLastEventID

	prevActiveCluster := cluster.TestCurrentClusterName
	context := NewMockworkflowExecutionContext(s.controller)
	msBuilderIn := NewMockmutableState(s.controller)

	request := &h.ReplicateEventsRequest{
		Version: common.Int64Ptr(incomingVersion),
		ReplicationInfo: map[string]*shared.ReplicationInfo{
			prevActiveCluster: {
				Version:     common.Int64Ptr(incomingReplicationInfoLastWriteVersion),
				LastEventId: common.Int64Ptr(incomingReplicationInfoLastEventID),
			},
		},
		History: &shared.History{Events: []*shared.HistoryEvent{
			{Timestamp: common.Int64Ptr(time.Now().UnixNano())},
		}},
	}

	msBuilderIn.EXPECT().HasBufferedEvents().Return(false).Times(1)
	msBuilderIn.EXPECT().GetReplicationState().Return(&persistence.ReplicationState{
		LastWriteVersion: currentLastWriteVersion,
		LastWriteEventID: currentLastEventID,
	}).AnyTimes()
	msBuilderIn.EXPECT().IsWorkflowExecutionRunning().Return(true).Times(1)
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(currentLastWriteVersion).Return(prevActiveCluster).AnyTimes()

	msBuilderOut, err := s.historyReplicator.ApplyOtherEventsVersionChecking(ctx.Background(), context, msBuilderIn,
		request, s.logger)
	s.Equal(msBuilderIn, msBuilderOut)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestApplyOtherEventsVersionChecking_IncomingGreaterThanCurrent_CurrentWasActive_ReplicationInfoVersionEqual_BufferedEvent_ResolveConflict() {
	domainID := uuid.New()
	runID := uuid.New()

	currentLastWriteVersion := int64(10)
	currentLastEventID := int64(98)
	incomingVersion := currentLastWriteVersion + 10
	incomingReplicationInfoLastWriteVersion := currentLastWriteVersion
	incomingReplicationInfoLastEventID := currentLastEventID
	decisionTimeout := int32(100)
	decisionStickyTimeout := int32(10)
	decisionTasklist := "some random decision tasklist"
	decisionStickyTasklist := "some random decision sticky tasklist"

	updateCondition := int64(1394)

	prevActiveCluster := cluster.TestCurrentClusterName
	context := NewMockworkflowExecutionContext(s.controller)
	msBuilderIn := NewMockmutableState(s.controller)

	request := &h.ReplicateEventsRequest{
		Version: common.Int64Ptr(incomingVersion),
		ReplicationInfo: map[string]*shared.ReplicationInfo{
			prevActiveCluster: {
				Version:     common.Int64Ptr(incomingReplicationInfoLastWriteVersion),
				LastEventId: common.Int64Ptr(incomingReplicationInfoLastEventID),
			},
		},
		History: &shared.History{Events: []*shared.HistoryEvent{
			{Timestamp: common.Int64Ptr(time.Now().UnixNano())},
		}},
	}
	startTimeStamp := time.Now()
	pendingDecisionInfo := &decisionInfo{
		Version:    currentLastWriteVersion,
		ScheduleID: 56,
		StartedID:  57,
	}
	msBuilderIn.EXPECT().GetLastWriteVersion().Return(currentLastWriteVersion, nil).Times(1)
	msBuilderIn.EXPECT().GetReplicationState().Return(&persistence.ReplicationState{
		LastWriteVersion: currentLastWriteVersion,
		LastWriteEventID: currentLastEventID,
	}).Times(1)
	msBuilderIn.EXPECT().HasBufferedEvents().Return(true).Times(1)
	msBuilderIn.EXPECT().GetInFlightDecision().Return(pendingDecisionInfo, true).Times(1)
	msBuilderIn.EXPECT().UpdateCurrentVersion(currentLastWriteVersion, true).Return(nil).Times(1)
	msBuilderIn.EXPECT().AddDecisionTaskFailedEvent(pendingDecisionInfo.ScheduleID, pendingDecisionInfo.StartedID,
		workflow.DecisionTaskFailedCauseFailoverCloseDecision, ([]byte)(nil), identityHistoryService, "", "", "", "", int64(0),
	).Return(&shared.HistoryEvent{}, nil).Times(1)
	msBuilderIn.EXPECT().HasPendingDecision().Return(false).Times(1)
	currentState := persistence.WorkflowStateRunning
	exeInfo := &persistence.WorkflowExecutionInfo{
		StartTimestamp:               startTimeStamp,
		DomainID:                     domainID,
		RunID:                        runID,
		TaskList:                     decisionTasklist,
		StickyTaskList:               decisionStickyTasklist,
		DecisionTimeout:              decisionTimeout,
		StickyScheduleToStartTimeout: decisionStickyTimeout,
		State:                        currentState,
		CloseStatus:                  persistence.WorkflowCloseStatusNone,
		DecisionVersion:              common.EmptyVersion,
		DecisionScheduleID:           common.EmptyEventID,
		DecisionStartedID:            common.EmptyEventID,
	}
	msBuilderIn.EXPECT().GetExecutionInfo().Return(exeInfo).AnyTimes()
	newDecision := &decisionInfo{
		Version:    currentLastWriteVersion,
		ScheduleID: currentLastEventID + 2,
		StartedID:  common.EmptyEventID,
		TaskList:   decisionStickyTasklist,
		Attempt:    0,
	}
	msBuilderIn.EXPECT().AddDecisionTaskScheduledEvent(false).Return(newDecision, nil).Times(1)

	context.EXPECT().updateWorkflowExecutionAsActive(gomock.Any()).Return(nil).Times(1)

	// after the flush, the pending buffered events are gone, however, the last event ID should increase
	msBuilderIn.EXPECT().GetReplicationState().Return(&persistence.ReplicationState{
		LastWriteVersion: currentLastWriteVersion,
		LastWriteEventID: currentLastEventID + 2,
	}).Times(1)
	msBuilderIn.EXPECT().IsWorkflowExecutionRunning().Return(currentState != persistence.WorkflowStateCompleted).AnyTimes()
	msBuilderIn.EXPECT().GetLastWriteVersion().Return(currentLastWriteVersion, nil).Times(1)
	msBuilderIn.EXPECT().GetUpdateCondition().Return(updateCondition).AnyTimes()
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(currentLastWriteVersion).Return(prevActiveCluster).AnyTimes()

	mockConflictResolver := NewMockconflictResolver(s.controller)
	s.historyReplicator.getNewConflictResolver = func(context workflowExecutionContext, logger log.Logger) conflictResolver {
		return mockConflictResolver
	}
	msBuilderMid := NewMockmutableState(s.controller)
	msBuilderMid.EXPECT().GetNextEventID().Return(int64(12345)).AnyTimes() // this is used by log
	mockConflictResolver.EXPECT().reset(
		runID, currentLastWriteVersion, currentState, gomock.Any(), incomingReplicationInfoLastEventID, exeInfo, updateCondition,
	).Return(msBuilderMid, nil).Times(1)
	msBuilderOut, err := s.historyReplicator.ApplyOtherEventsVersionChecking(ctx.Background(), context, msBuilderIn, request, s.logger)
	s.Equal(msBuilderMid, msBuilderOut)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestApplyOtherEvents_IncomingLessThanCurrent() {
	currentNextEventID := int64(10)
	incomingFirstEventID := currentNextEventID - 4

	context := NewMockworkflowExecutionContext(s.controller)
	msBuilder := NewMockmutableState(s.controller)

	request := &h.ReplicateEventsRequest{
		FirstEventId: common.Int64Ptr(incomingFirstEventID),
		History:      &shared.History{},
	}
	msBuilder.EXPECT().GetNextEventID().Return(currentNextEventID).AnyTimes()
	msBuilder.EXPECT().GetReplicationState().Return(&persistence.ReplicationState{}).AnyTimes() // logger will use this

	err := s.historyReplicator.ApplyOtherEvents(ctx.Background(), context, msBuilder, request, s.logger)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestApplyOtherEvents_IncomingEqualToCurrent() {
	// TODO
}

func (s *historyReplicatorSuite) TestApplyOtherEvents_IncomingGreaterThanCurrent() {
	domainID := testDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	currentVersion := int64(4096)
	currentNextEventID := int64(10)

	incomingSourceCluster := "some random incoming source cluster"
	incomingVersion := currentVersion * 2
	incomingFirstEventID := currentNextEventID + 4
	incomingNextEventID := incomingFirstEventID + 4

	context := NewMockworkflowExecutionContext(s.controller)
	context.EXPECT().getDomainID().Return(domainID).AnyTimes()
	context.EXPECT().getExecution().Return(&workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(runID),
	}).AnyTimes()

	msBuilder := NewMockmutableState(s.controller)

	request := &h.ReplicateEventsRequest{
		SourceCluster: common.StringPtr(incomingSourceCluster),
		Version:       common.Int64Ptr(incomingVersion),
		FirstEventId:  common.Int64Ptr(incomingFirstEventID),
		NextEventId:   common.Int64Ptr(incomingNextEventID),
		History:       &shared.History{},
	}

	msBuilder.EXPECT().GetNextEventID().Return(currentNextEventID).AnyTimes()
	msBuilder.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes()

	err := s.historyReplicator.ApplyOtherEvents(ctx.Background(), context, msBuilder, request, s.logger)
	s.Equal(newRetryTaskErrorWithHint(ErrRetryBufferEventsMsg, domainID, workflowID, runID, currentNextEventID), err)
}

func (s *historyReplicatorSuite) TestApplyReplicationTask() {
	// TODO
}

func (s *historyReplicatorSuite) TestApplyReplicationTask_WorkflowClosed() {
	currentVersion := int64(4096)
	currentNextEventID := int64(10)

	incomingSourceCluster := "some random incoming source cluster"
	incomingVersion := currentVersion * 2
	incomingFirstEventID := currentNextEventID + 4
	incomingNextEventID := incomingFirstEventID + 4

	context := NewMockworkflowExecutionContext(s.controller)
	msBuilder := NewMockmutableState(s.controller)

	request := &h.ReplicateEventsRequest{
		SourceCluster:     common.StringPtr(incomingSourceCluster),
		Version:           common.Int64Ptr(incomingVersion),
		FirstEventId:      common.Int64Ptr(incomingFirstEventID),
		NextEventId:       common.Int64Ptr(incomingNextEventID),
		ForceBufferEvents: common.BoolPtr(true),
		History:           &shared.History{Events: []*shared.HistoryEvent{{}}},
	}

	msBuilder.EXPECT().IsWorkflowExecutionRunning().Return(false).AnyTimes()

	err := s.historyReplicator.ApplyReplicationTask(ctx.Background(), context, msBuilder, request, s.logger)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestReplicateWorkflowStarted_BrandNew() {
	domainID := testDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	version := int64(144)
	tasklist := "some random tasklist"
	workflowType := "some random workflow type"
	workflowTimeout := int32(3721)
	decisionTimeout := int32(4411)

	initiatedID := int64(4810)
	parentDomainID := testDomainID
	parentWorkflowID := "some random workflow ID"
	parentRunID := uuid.New()

	context := newWorkflowExecutionContext(domainID, shared.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(runID),
	}, s.mockShard, s.mockExecutionMgr, s.logger)
	msBuilder := NewMockmutableState(s.controller)

	di := &decisionInfo{
		Version:         version,
		ScheduleID:      common.FirstEventID + 1,
		StartedID:       common.EmptyEventID,
		DecisionTimeout: decisionTimeout,
		TaskList:        tasklist,
	}

	requestID := uuid.New()
	now := time.Now()
	history := &shared.History{
		Events: []*shared.HistoryEvent{
			{Version: common.Int64Ptr(version), EventId: common.Int64Ptr(1), Timestamp: common.Int64Ptr(now.UnixNano())},
			{Version: common.Int64Ptr(version), EventId: common.Int64Ptr(2), Timestamp: common.Int64Ptr(now.UnixNano())},
		},
	}
	nextEventID := di.ScheduleID + 1
	replicationState := &persistence.ReplicationState{
		StartVersion:     version,
		CurrentVersion:   version,
		LastWriteVersion: version,
		LastWriteEventID: nextEventID - 1,
	}
	transferTasks := []persistence.Task{&persistence.CloseExecutionTask{Version: version}}
	timerTasks := []persistence.Task{&persistence.DeleteHistoryEventTask{Version: version}}

	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(version).Return(cluster.TestAlternativeClusterName).AnyTimes()
	historySize := 111
	executionInfo := &persistence.WorkflowExecutionInfo{
		CreateRequestID:             requestID,
		DomainID:                    domainID,
		WorkflowID:                  workflowID,
		RunID:                       runID,
		ParentDomainID:              parentDomainID,
		ParentWorkflowID:            parentWorkflowID,
		ParentRunID:                 parentRunID,
		InitiatedID:                 initiatedID,
		TaskList:                    tasklist,
		WorkflowTypeName:            workflowType,
		WorkflowTimeout:             workflowTimeout,
		DecisionStartToCloseTimeout: decisionTimeout,
		NextEventID:                 nextEventID,
		LastProcessedEvent:          common.EmptyEventID,
		BranchToken:                 []byte("some random branch token"),
		DecisionVersion:             di.Version,
		DecisionScheduleID:          di.ScheduleID,
		DecisionStartedID:           di.StartedID,
		DecisionTimeout:             di.DecisionTimeout,
		State:                       persistence.WorkflowStateRunning,
		CloseStatus:                 persistence.WorkflowCloseStatusNone,
	}
	msBuilder.EXPECT().GetExecutionInfo().Return(executionInfo).AnyTimes()
	newWorkflowSnapshot := &persistence.WorkflowSnapshot{
		ExecutionInfo:    executionInfo,
		ExecutionStats:   &persistence.ExecutionStats{HistorySize: int64(historySize)},
		ReplicationState: replicationState,
		TransferTasks:    transferTasks,
		TimerTasks:       timerTasks,
	}
	newWorkflowEventsSeq := []*persistence.WorkflowEvents{{
		DomainID:    domainID,
		WorkflowID:  workflowID,
		RunID:       runID,
		BranchToken: executionInfo.BranchToken,
		Events:      history.Events,
	}}
	msBuilder.EXPECT().CloseTransactionAsSnapshot(now.Local(), transactionPolicyPassive).Return(newWorkflowSnapshot, newWorkflowEventsSeq, nil).Times(1)
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: historySize}, nil).Once()
	s.mockExecutionMgr.On("CreateWorkflowExecution", mock.MatchedBy(func(input *persistence.CreateWorkflowExecutionRequest) bool {
		input.RangeID = 0
		s.Equal(&persistence.CreateWorkflowExecutionRequest{
			Mode:                persistence.CreateWorkflowModeBrandNew,
			PreviousRunID:       "",
			NewWorkflowSnapshot: *newWorkflowSnapshot,
		}, input)
		return true
	})).Return(&persistence.CreateWorkflowExecutionResponse{}, nil).Once()
	s.mockDomainCache.EXPECT().GetDomainByID(domainID).Return(
		cache.NewGlobalDomainCacheEntryForTest(
			&persistence.DomainInfo{ID: domainID},
			&persistence.DomainConfig{Retention: 1},
			&persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					{ClusterName: cluster.TestCurrentClusterName},
					{ClusterName: cluster.TestAlternativeClusterName},
				},
			},
			0, // not used
			nil,
		), nil,
	).AnyTimes()

	err := s.historyReplicator.replicateWorkflowStarted(ctx.Background(), context, msBuilder, history, s.mockStateBuilder, s.logger)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestReplicateWorkflowStarted_ISE() {
	domainID := testDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	version := int64(144)
	tasklist := "some random tasklist"
	workflowType := "some random workflow type"
	workflowTimeout := int32(3721)
	decisionTimeout := int32(4411)

	initiatedID := int64(4810)
	parentDomainID := testDomainID
	parentWorkflowID := "some random workflow ID"
	parentRunID := uuid.New()

	context := newWorkflowExecutionContext(domainID, shared.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(runID),
	}, s.mockShard, s.mockExecutionMgr, s.logger)
	msBuilder := NewMockmutableState(s.controller)

	di := &decisionInfo{
		Version:         version,
		ScheduleID:      common.FirstEventID + 1,
		StartedID:       common.EmptyEventID,
		DecisionTimeout: decisionTimeout,
		TaskList:        tasklist,
	}

	requestID := uuid.New()
	now := time.Now()
	history := &shared.History{
		Events: []*shared.HistoryEvent{
			{Version: common.Int64Ptr(version), EventId: common.Int64Ptr(1), Timestamp: common.Int64Ptr(now.UnixNano())},
			{Version: common.Int64Ptr(version), EventId: common.Int64Ptr(2), Timestamp: common.Int64Ptr(now.UnixNano())},
		},
	}
	nextEventID := di.ScheduleID + 1
	replicationState := &persistence.ReplicationState{
		StartVersion:     version,
		CurrentVersion:   version,
		LastWriteVersion: version,
		LastWriteEventID: nextEventID - 1,
	}
	transferTasks := []persistence.Task{&persistence.CloseExecutionTask{Version: version}}
	timerTasks := []persistence.Task{&persistence.DeleteHistoryEventTask{Version: version}}

	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(version).Return(cluster.TestAlternativeClusterName).AnyTimes()
	historySize := 111
	executionInfo := &persistence.WorkflowExecutionInfo{
		CreateRequestID:             requestID,
		DomainID:                    domainID,
		WorkflowID:                  workflowID,
		RunID:                       runID,
		ParentDomainID:              parentDomainID,
		ParentWorkflowID:            parentWorkflowID,
		ParentRunID:                 parentRunID,
		InitiatedID:                 initiatedID,
		TaskList:                    tasklist,
		WorkflowTypeName:            workflowType,
		WorkflowTimeout:             workflowTimeout,
		DecisionStartToCloseTimeout: decisionTimeout,
		NextEventID:                 nextEventID,
		LastProcessedEvent:          common.EmptyEventID,
		BranchToken:                 []byte("some random branch token"),
		DecisionVersion:             di.Version,
		DecisionScheduleID:          di.ScheduleID,
		DecisionStartedID:           di.StartedID,
		DecisionTimeout:             di.DecisionTimeout,
		State:                       persistence.WorkflowStateRunning,
		CloseStatus:                 persistence.WorkflowCloseStatusNone,
	}
	msBuilder.EXPECT().GetExecutionInfo().Return(executionInfo).AnyTimes()
	newWorkflowSnapshot := &persistence.WorkflowSnapshot{
		ExecutionInfo:    executionInfo,
		ExecutionStats:   &persistence.ExecutionStats{HistorySize: int64(historySize)},
		ReplicationState: replicationState,
		TransferTasks:    transferTasks,
		TimerTasks:       timerTasks,
	}
	newWorkflowEventsSeq := []*persistence.WorkflowEvents{{
		DomainID:    domainID,
		WorkflowID:  workflowID,
		RunID:       runID,
		BranchToken: executionInfo.BranchToken,
		Events:      history.Events,
	}}
	msBuilder.EXPECT().CloseTransactionAsSnapshot(now.Local(), transactionPolicyPassive).Return(newWorkflowSnapshot, newWorkflowEventsSeq, nil).Times(1)
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: historySize}, nil).Once()
	errRet := &shared.InternalServiceError{}
	// the test above already assert the create workflow request, so here just use anyting
	s.mockExecutionMgr.On("CreateWorkflowExecution", mock.Anything).Return(nil, errRet)
	s.mockShardManager.On("UpdateShard", mock.Anything).Return(nil) // this is called when err is returned, and shard will try to update

	s.mockDomainCache.EXPECT().GetDomainByID(domainID).Return(
		cache.NewGlobalDomainCacheEntryForTest(
			&persistence.DomainInfo{ID: domainID},
			&persistence.DomainConfig{Retention: 1},
			&persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					{ClusterName: cluster.TestCurrentClusterName},
					{ClusterName: cluster.TestAlternativeClusterName},
				},
			},
			0, // not used
			nil,
		), nil,
	).AnyTimes()

	err := s.historyReplicator.replicateWorkflowStarted(ctx.Background(), context, msBuilder, history, s.mockStateBuilder, s.logger)
	s.Equal(errRet, err)
}

func (s *historyReplicatorSuite) TestReplicateWorkflowStarted_SameRunID() {
	domainID := testDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	version := int64(144)
	tasklist := "some random tasklist"
	workflowType := "some random workflow type"
	workflowTimeout := int32(3721)
	decisionTimeout := int32(4411)

	initiatedID := int64(4810)
	parentDomainID := testDomainID
	parentWorkflowID := "some random workflow ID"
	parentRunID := uuid.New()

	context := newWorkflowExecutionContext(domainID, shared.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(runID),
	}, s.mockShard, s.mockExecutionMgr, s.logger)
	msBuilder := NewMockmutableState(s.controller)

	di := &decisionInfo{
		Version:         version,
		ScheduleID:      common.FirstEventID + 1,
		StartedID:       common.EmptyEventID,
		DecisionTimeout: decisionTimeout,
		TaskList:        tasklist,
	}

	requestID := uuid.New()
	now := time.Now()
	history := &shared.History{
		Events: []*shared.HistoryEvent{
			{Version: common.Int64Ptr(version), EventId: common.Int64Ptr(1), Timestamp: common.Int64Ptr(now.UnixNano())},
			{Version: common.Int64Ptr(version), EventId: common.Int64Ptr(2), Timestamp: common.Int64Ptr(now.UnixNano())},
		},
	}
	nextEventID := di.ScheduleID + 1
	replicationState := &persistence.ReplicationState{
		StartVersion:     version,
		CurrentVersion:   version,
		LastWriteVersion: version,
		LastWriteEventID: nextEventID - 1,
	}
	transferTasks := []persistence.Task{&persistence.CloseExecutionTask{Version: version}}
	timerTasks := []persistence.Task{&persistence.DeleteHistoryEventTask{Version: version}}

	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(version).Return(cluster.TestAlternativeClusterName).AnyTimes()
	historySize := 111
	executionInfo := &persistence.WorkflowExecutionInfo{
		CreateRequestID:             requestID,
		DomainID:                    domainID,
		WorkflowID:                  workflowID,
		RunID:                       runID,
		ParentDomainID:              parentDomainID,
		ParentWorkflowID:            parentWorkflowID,
		ParentRunID:                 parentRunID,
		InitiatedID:                 initiatedID,
		TaskList:                    tasklist,
		WorkflowTypeName:            workflowType,
		WorkflowTimeout:             workflowTimeout,
		DecisionStartToCloseTimeout: decisionTimeout,
		NextEventID:                 nextEventID,
		LastProcessedEvent:          common.EmptyEventID,
		BranchToken:                 []byte("some random branch token"),
		DecisionVersion:             di.Version,
		DecisionScheduleID:          di.ScheduleID,
		DecisionStartedID:           di.StartedID,
		DecisionTimeout:             di.DecisionTimeout,
		State:                       persistence.WorkflowStateRunning,
		CloseStatus:                 persistence.WorkflowCloseStatusNone,
	}
	msBuilder.EXPECT().GetExecutionInfo().Return(executionInfo).AnyTimes()
	newWorkflowSnapshot := &persistence.WorkflowSnapshot{
		ExecutionInfo:    executionInfo,
		ExecutionStats:   &persistence.ExecutionStats{HistorySize: int64(historySize)},
		ReplicationState: replicationState,
		TransferTasks:    transferTasks,
		TimerTasks:       timerTasks,
	}
	newWorkflowEventsSeq := []*persistence.WorkflowEvents{{
		DomainID:    domainID,
		WorkflowID:  workflowID,
		RunID:       runID,
		BranchToken: executionInfo.BranchToken,
		Events:      history.Events,
	}}
	msBuilder.EXPECT().CloseTransactionAsSnapshot(now.Local(), transactionPolicyPassive).Return(newWorkflowSnapshot, newWorkflowEventsSeq, nil).Times(1)
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: historySize}, nil).Once()

	currentVersion := version
	currentRunID := runID
	currentState := persistence.WorkflowStateRunning
	errRet := &persistence.WorkflowExecutionAlreadyStartedError{
		RunID:            currentRunID,
		State:            currentState,
		LastWriteVersion: currentVersion,
	}
	// the test above already assert the create workflow request, so here just use anything
	s.mockExecutionMgr.On("CreateWorkflowExecution", mock.Anything).Return(nil, errRet).Once()
	s.mockDomainCache.EXPECT().GetDomainByID(domainID).Return(
		cache.NewGlobalDomainCacheEntryForTest(
			&persistence.DomainInfo{ID: domainID},
			&persistence.DomainConfig{Retention: 1},
			&persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					{ClusterName: cluster.TestCurrentClusterName},
					{ClusterName: cluster.TestAlternativeClusterName},
				},
			},
			0, // not used
			nil,
		), nil,
	).AnyTimes()

	err := s.historyReplicator.replicateWorkflowStarted(ctx.Background(), context, msBuilder, history, s.mockStateBuilder, s.logger)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestReplicateWorkflowStarted_CurrentComplete_IncomingLessThanCurrent() {
	domainID := testDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	version := int64(144)
	tasklist := "some random tasklist"
	workflowType := "some random workflow type"
	workflowTimeout := int32(3721)
	decisionTimeout := int32(4411)
	cronSchedule := "some random cron scredule"
	retryPolicy := &workflow.RetryPolicy{
		InitialIntervalInSeconds:    common.Int32Ptr(1),
		MaximumAttempts:             common.Int32Ptr(3),
		MaximumIntervalInSeconds:    common.Int32Ptr(1),
		NonRetriableErrorReasons:    []string{"bad-bug"},
		BackoffCoefficient:          common.Float64Ptr(1),
		ExpirationIntervalInSeconds: common.Int32Ptr(100),
	}

	initiatedID := int64(4810)
	parentDomainID := testDomainID
	parentWorkflowID := "some random workflow ID"
	parentRunID := uuid.New()

	context := newWorkflowExecutionContext(domainID, shared.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(runID),
	}, s.mockShard, s.mockExecutionMgr, s.logger)
	msBuilder := NewMockmutableState(s.controller)

	di := &decisionInfo{
		Version:         version,
		ScheduleID:      common.FirstEventID + 1,
		StartedID:       common.EmptyEventID,
		DecisionTimeout: decisionTimeout,
		TaskList:        tasklist,
	}

	requestID := uuid.New()
	now := time.Now()
	history := &shared.History{
		Events: []*shared.HistoryEvent{
			{Version: common.Int64Ptr(version), EventId: common.Int64Ptr(1), Timestamp: common.Int64Ptr(now.UnixNano())},
			{Version: common.Int64Ptr(version), EventId: common.Int64Ptr(2), Timestamp: common.Int64Ptr(now.UnixNano())},
		},
	}
	nextEventID := di.ScheduleID + 1
	replicationState := &persistence.ReplicationState{
		StartVersion:     version,
		CurrentVersion:   version,
		LastWriteVersion: version,
		LastWriteEventID: nextEventID - 1,
	}
	transferTasks := []persistence.Task{&persistence.CloseExecutionTask{Version: version}}
	timerTasks := []persistence.Task{&persistence.DeleteHistoryEventTask{Version: version}}

	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(version).Return(cluster.TestAlternativeClusterName).AnyTimes()
	historySize := 111
	executionInfo := &persistence.WorkflowExecutionInfo{
		CreateRequestID:             requestID,
		DomainID:                    domainID,
		WorkflowID:                  workflowID,
		RunID:                       runID,
		ParentDomainID:              parentDomainID,
		ParentWorkflowID:            parentWorkflowID,
		ParentRunID:                 parentRunID,
		InitiatedID:                 initiatedID,
		TaskList:                    tasklist,
		WorkflowTypeName:            workflowType,
		WorkflowTimeout:             workflowTimeout,
		DecisionStartToCloseTimeout: decisionTimeout,
		NextEventID:                 nextEventID,
		LastProcessedEvent:          common.EmptyEventID,
		BranchToken:                 []byte("some random branch token"),
		DecisionVersion:             di.Version,
		DecisionScheduleID:          di.ScheduleID,
		DecisionStartedID:           di.StartedID,
		DecisionTimeout:             di.DecisionTimeout,
		State:                       persistence.WorkflowStateRunning,
		CloseStatus:                 persistence.WorkflowCloseStatusNone,
		CronSchedule:                cronSchedule,
		HasRetryPolicy:              true,
		InitialInterval:             retryPolicy.GetInitialIntervalInSeconds(),
		BackoffCoefficient:          retryPolicy.GetBackoffCoefficient(),
		ExpirationSeconds:           retryPolicy.GetExpirationIntervalInSeconds(),
		MaximumAttempts:             retryPolicy.GetMaximumAttempts(),
		MaximumInterval:             retryPolicy.GetMaximumIntervalInSeconds(),
		NonRetriableErrors:          retryPolicy.GetNonRetriableErrorReasons(),
	}
	msBuilder.EXPECT().GetExecutionInfo().Return(executionInfo).AnyTimes()
	newWorkflowSnapshot := &persistence.WorkflowSnapshot{
		ExecutionInfo:    executionInfo,
		ExecutionStats:   &persistence.ExecutionStats{HistorySize: int64(historySize)},
		ReplicationState: replicationState,
		TransferTasks:    transferTasks,
		TimerTasks:       timerTasks,
	}
	newWorkflowEventsSeq := []*persistence.WorkflowEvents{{
		DomainID:    domainID,
		WorkflowID:  workflowID,
		RunID:       runID,
		BranchToken: executionInfo.BranchToken,
		Events:      history.Events,
	}}
	msBuilder.EXPECT().CloseTransactionAsSnapshot(now.Local(), transactionPolicyPassive).Return(newWorkflowSnapshot, newWorkflowEventsSeq, nil).Times(1)
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: historySize}, nil).Once()

	currentVersion := version + 1
	currentRunID := uuid.New()
	currentState := persistence.WorkflowStateCompleted
	errRet := &persistence.WorkflowExecutionAlreadyStartedError{
		RunID:            currentRunID,
		State:            currentState,
		LastWriteVersion: currentVersion,
	}
	// the test above already assert the create workflow request, so here just use anyting
	s.mockExecutionMgr.On("CreateWorkflowExecution", mock.MatchedBy(func(input *persistence.CreateWorkflowExecutionRequest) bool {
		input.RangeID = 0
		return reflect.DeepEqual(&persistence.CreateWorkflowExecutionRequest{
			Mode:                persistence.CreateWorkflowModeBrandNew,
			PreviousRunID:       "",
			NewWorkflowSnapshot: *newWorkflowSnapshot,
		}, input)
	})).Return(nil, errRet).Once()
	s.mockExecutionMgr.On("CreateWorkflowExecution", mock.MatchedBy(func(input *persistence.CreateWorkflowExecutionRequest) bool {
		input.RangeID = 0
		return reflect.DeepEqual(&persistence.CreateWorkflowExecutionRequest{
			Mode:                     persistence.CreateWorkflowModeWorkflowIDReuse,
			PreviousRunID:            currentRunID,
			PreviousLastWriteVersion: currentVersion,
			NewWorkflowSnapshot:      *newWorkflowSnapshot,
		}, input)
	})).Return(&persistence.CreateWorkflowExecutionResponse{}, nil).Once()
	s.mockDomainCache.EXPECT().GetDomainByID(domainID).Return(
		cache.NewGlobalDomainCacheEntryForTest(
			&persistence.DomainInfo{ID: domainID},
			&persistence.DomainConfig{Retention: 1},
			&persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					{ClusterName: cluster.TestCurrentClusterName},
					{ClusterName: cluster.TestAlternativeClusterName},
				},
			},
			0, // not used
			nil,
		), nil,
	).AnyTimes()

	err := s.historyReplicator.replicateWorkflowStarted(ctx.Background(), context, msBuilder, history, s.mockStateBuilder, s.logger)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestReplicateWorkflowStarted_CurrentComplete_IncomingEqualToThanCurrent() {
	domainID := testDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	version := int64(144)
	tasklist := "some random tasklist"
	workflowType := "some random workflow type"
	workflowTimeout := int32(3721)
	decisionTimeout := int32(4411)

	initiatedID := int64(4810)
	parentDomainID := testDomainID
	parentWorkflowID := "some random workflow ID"
	parentRunID := uuid.New()

	context := newWorkflowExecutionContext(domainID, shared.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(runID),
	}, s.mockShard, s.mockExecutionMgr, s.logger)
	msBuilder := NewMockmutableState(s.controller)

	di := &decisionInfo{
		Version:         version,
		ScheduleID:      common.FirstEventID + 1,
		StartedID:       common.EmptyEventID,
		DecisionTimeout: decisionTimeout,
		TaskList:        tasklist,
	}

	requestID := uuid.New()
	now := time.Now()
	history := &shared.History{
		Events: []*shared.HistoryEvent{
			{Version: common.Int64Ptr(version), EventId: common.Int64Ptr(1), Timestamp: common.Int64Ptr(now.UnixNano())},
			{Version: common.Int64Ptr(version), EventId: common.Int64Ptr(2), Timestamp: common.Int64Ptr(now.UnixNano())},
		},
	}
	nextEventID := di.ScheduleID + 1
	replicationState := &persistence.ReplicationState{
		StartVersion:     version,
		CurrentVersion:   version,
		LastWriteVersion: version,
		LastWriteEventID: nextEventID - 1,
	}
	transferTasks := []persistence.Task{&persistence.CloseExecutionTask{Version: version}}
	timerTasks := []persistence.Task{&persistence.DeleteHistoryEventTask{Version: version}}

	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(version).Return(cluster.TestAlternativeClusterName).AnyTimes()
	historySize := 111
	executionInfo := &persistence.WorkflowExecutionInfo{
		CreateRequestID:             requestID,
		DomainID:                    domainID,
		WorkflowID:                  workflowID,
		RunID:                       runID,
		ParentDomainID:              parentDomainID,
		ParentWorkflowID:            parentWorkflowID,
		ParentRunID:                 parentRunID,
		InitiatedID:                 initiatedID,
		TaskList:                    tasklist,
		WorkflowTypeName:            workflowType,
		WorkflowTimeout:             workflowTimeout,
		DecisionStartToCloseTimeout: decisionTimeout,
		NextEventID:                 nextEventID,
		LastProcessedEvent:          common.EmptyEventID,
		BranchToken:                 []byte("some random branch token"),
		DecisionVersion:             di.Version,
		DecisionScheduleID:          di.ScheduleID,
		DecisionStartedID:           di.StartedID,
		DecisionTimeout:             di.DecisionTimeout,
		State:                       persistence.WorkflowStateRunning,
		CloseStatus:                 persistence.WorkflowCloseStatusNone,
	}
	msBuilder.EXPECT().GetExecutionInfo().Return(executionInfo).AnyTimes()
	newWorkflowSnapshot := &persistence.WorkflowSnapshot{
		ExecutionInfo:    executionInfo,
		ExecutionStats:   &persistence.ExecutionStats{HistorySize: int64(historySize)},
		ReplicationState: replicationState,
		TransferTasks:    transferTasks,
		TimerTasks:       timerTasks,
	}
	newWorkflowEventsSeq := []*persistence.WorkflowEvents{{
		DomainID:    domainID,
		WorkflowID:  workflowID,
		RunID:       runID,
		BranchToken: executionInfo.BranchToken,
		Events:      history.Events,
	}}
	msBuilder.EXPECT().CloseTransactionAsSnapshot(now.Local(), transactionPolicyPassive).Return(newWorkflowSnapshot, newWorkflowEventsSeq, nil).Times(1)
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: historySize}, nil).Once()

	currentVersion := version
	currentRunID := uuid.New()
	currentState := persistence.WorkflowStateCompleted
	errRet := &persistence.WorkflowExecutionAlreadyStartedError{
		RunID:            currentRunID,
		State:            currentState,
		LastWriteVersion: currentVersion,
	}
	// the test above already assert the create workflow request, so here just use anyting
	s.mockExecutionMgr.On("CreateWorkflowExecution", mock.MatchedBy(func(input *persistence.CreateWorkflowExecutionRequest) bool {
		input.RangeID = 0
		return reflect.DeepEqual(&persistence.CreateWorkflowExecutionRequest{
			Mode:                persistence.CreateWorkflowModeBrandNew,
			PreviousRunID:       "",
			NewWorkflowSnapshot: *newWorkflowSnapshot,
		}, input)
	})).Return(nil, errRet).Once()
	s.mockExecutionMgr.On("CreateWorkflowExecution", mock.MatchedBy(func(input *persistence.CreateWorkflowExecutionRequest) bool {
		input.RangeID = 0
		return reflect.DeepEqual(&persistence.CreateWorkflowExecutionRequest{
			Mode:                     persistence.CreateWorkflowModeWorkflowIDReuse,
			PreviousRunID:            currentRunID,
			PreviousLastWriteVersion: currentVersion,
			NewWorkflowSnapshot:      *newWorkflowSnapshot,
		}, input)
	})).Return(&persistence.CreateWorkflowExecutionResponse{}, nil).Once()
	s.mockDomainCache.EXPECT().GetDomainByID(domainID).Return(
		cache.NewGlobalDomainCacheEntryForTest(
			&persistence.DomainInfo{ID: domainID},
			&persistence.DomainConfig{Retention: 1},
			&persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					{ClusterName: cluster.TestCurrentClusterName},
					{ClusterName: cluster.TestAlternativeClusterName},
				},
			},
			0, // not used
			nil,
		), nil,
	).AnyTimes()

	err := s.historyReplicator.replicateWorkflowStarted(ctx.Background(), context, msBuilder, history, s.mockStateBuilder, s.logger)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestReplicateWorkflowStarted_CurrentComplete_IncomingNotLessThanCurrent() {
	domainID := testDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	version := int64(144)
	tasklist := "some random tasklist"
	workflowType := "some random workflow type"
	workflowTimeout := int32(3721)
	decisionTimeout := int32(4411)

	initiatedID := int64(4810)
	parentDomainID := testDomainID
	parentWorkflowID := "some random workflow ID"
	parentRunID := uuid.New()

	context := newWorkflowExecutionContext(domainID, shared.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(runID),
	}, s.mockShard, s.mockExecutionMgr, s.logger)
	msBuilder := NewMockmutableState(s.controller)

	di := &decisionInfo{
		Version:         version,
		ScheduleID:      common.FirstEventID + 1,
		StartedID:       common.EmptyEventID,
		DecisionTimeout: decisionTimeout,
		TaskList:        tasklist,
	}

	requestID := uuid.New()
	now := time.Now()
	history := &shared.History{
		Events: []*shared.HistoryEvent{
			{Version: common.Int64Ptr(version), EventId: common.Int64Ptr(1), Timestamp: common.Int64Ptr(now.UnixNano())},
			{Version: common.Int64Ptr(version), EventId: common.Int64Ptr(2), Timestamp: common.Int64Ptr(now.UnixNano())},
		},
	}
	nextEventID := di.ScheduleID + 1
	replicationState := &persistence.ReplicationState{
		StartVersion:     version,
		CurrentVersion:   version,
		LastWriteVersion: version,
		LastWriteEventID: nextEventID - 1,
	}
	transferTasks := []persistence.Task{&persistence.CloseExecutionTask{Version: version}}
	timerTasks := []persistence.Task{&persistence.DeleteHistoryEventTask{Version: version}}

	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(version).Return(cluster.TestAlternativeClusterName).AnyTimes()
	historySize := 111
	executionInfo := &persistence.WorkflowExecutionInfo{
		CreateRequestID:             requestID,
		DomainID:                    domainID,
		WorkflowID:                  workflowID,
		RunID:                       runID,
		ParentDomainID:              parentDomainID,
		ParentWorkflowID:            parentWorkflowID,
		ParentRunID:                 parentRunID,
		InitiatedID:                 initiatedID,
		TaskList:                    tasklist,
		WorkflowTypeName:            workflowType,
		WorkflowTimeout:             workflowTimeout,
		DecisionStartToCloseTimeout: decisionTimeout,
		NextEventID:                 nextEventID,
		LastProcessedEvent:          common.EmptyEventID,
		BranchToken:                 []byte("some random branch token"),
		DecisionVersion:             di.Version,
		DecisionScheduleID:          di.ScheduleID,
		DecisionStartedID:           di.StartedID,
		DecisionTimeout:             di.DecisionTimeout,
		State:                       persistence.WorkflowStateRunning,
		CloseStatus:                 persistence.WorkflowCloseStatusNone,
	}
	msBuilder.EXPECT().GetExecutionInfo().Return(executionInfo).AnyTimes()
	newWorkflowSnapshot := &persistence.WorkflowSnapshot{
		ExecutionInfo:    executionInfo,
		ExecutionStats:   &persistence.ExecutionStats{HistorySize: int64(historySize)},
		ReplicationState: replicationState,
		TransferTasks:    transferTasks,
		TimerTasks:       timerTasks,
	}
	newWorkflowEventsSeq := []*persistence.WorkflowEvents{{
		DomainID:    domainID,
		WorkflowID:  workflowID,
		RunID:       runID,
		BranchToken: executionInfo.BranchToken,
		Events:      history.Events,
	}}
	msBuilder.EXPECT().CloseTransactionAsSnapshot(now.Local(), transactionPolicyPassive).Return(newWorkflowSnapshot, newWorkflowEventsSeq, nil).Times(1)
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: historySize}, nil).Once()

	currentVersion := version - 1
	currentRunID := uuid.New()
	currentState := persistence.WorkflowStateCompleted
	errRet := &persistence.WorkflowExecutionAlreadyStartedError{
		RunID:            currentRunID,
		State:            currentState,
		LastWriteVersion: currentVersion,
	}
	// the test above already assert the create workflow request, so here just use anyting
	s.mockExecutionMgr.On("CreateWorkflowExecution", mock.MatchedBy(func(input *persistence.CreateWorkflowExecutionRequest) bool {
		input.RangeID = 0
		return reflect.DeepEqual(&persistence.CreateWorkflowExecutionRequest{
			Mode:                persistence.CreateWorkflowModeBrandNew,
			PreviousRunID:       "",
			NewWorkflowSnapshot: *newWorkflowSnapshot,
		}, input)
	})).Return(nil, errRet).Once()
	s.mockExecutionMgr.On("CreateWorkflowExecution", mock.MatchedBy(func(input *persistence.CreateWorkflowExecutionRequest) bool {
		input.RangeID = 0
		return reflect.DeepEqual(&persistence.CreateWorkflowExecutionRequest{
			Mode:                     persistence.CreateWorkflowModeWorkflowIDReuse,
			PreviousRunID:            currentRunID,
			PreviousLastWriteVersion: currentVersion,
			NewWorkflowSnapshot:      *newWorkflowSnapshot,
		}, input)
	})).Return(&persistence.CreateWorkflowExecutionResponse{}, nil).Once()
	s.mockDomainCache.EXPECT().GetDomainByID(domainID).Return(
		cache.NewGlobalDomainCacheEntryForTest(
			&persistence.DomainInfo{ID: domainID},
			&persistence.DomainConfig{Retention: 1},
			&persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					{ClusterName: cluster.TestCurrentClusterName},
					{ClusterName: cluster.TestAlternativeClusterName},
				},
			},
			0, // not used
			nil,
		), nil,
	).AnyTimes()

	err := s.historyReplicator.replicateWorkflowStarted(ctx.Background(), context, msBuilder, history, s.mockStateBuilder, s.logger)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestReplicateWorkflowStarted_CurrentRunning_IncomingLessThanCurrent_NoEventsReapplication() {
	domainID := testDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	version := int64(144)
	tasklist := "some random tasklist"
	workflowType := "some random workflow type"
	workflowTimeout := int32(3721)
	decisionTimeout := int32(4411)

	initiatedID := int64(4810)
	parentDomainID := testDomainID
	parentWorkflowID := "some random workflow ID"
	parentRunID := uuid.New()

	context := newWorkflowExecutionContext(domainID, shared.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(runID),
	}, s.mockShard, s.mockExecutionMgr, s.logger)
	msBuilder := NewMockmutableState(s.controller)

	di := &decisionInfo{
		Version:         version,
		ScheduleID:      common.FirstEventID + 1,
		StartedID:       common.EmptyEventID,
		DecisionTimeout: decisionTimeout,
		TaskList:        tasklist,
	}

	requestID := uuid.New()
	now := time.Now()
	history := &shared.History{
		Events: []*shared.HistoryEvent{
			{Version: common.Int64Ptr(version), EventId: common.Int64Ptr(1), Timestamp: common.Int64Ptr(now.UnixNano())},
			{Version: common.Int64Ptr(version), EventId: common.Int64Ptr(2), Timestamp: common.Int64Ptr(now.UnixNano())},
		},
	}
	nextEventID := di.ScheduleID + 1
	replicationState := &persistence.ReplicationState{
		StartVersion:     version,
		CurrentVersion:   version,
		LastWriteVersion: version,
		LastWriteEventID: nextEventID - 1,
	}
	transferTasks := []persistence.Task{&persistence.CloseExecutionTask{Version: version}}
	timerTasks := []persistence.Task{&persistence.DeleteHistoryEventTask{Version: version}}

	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(version).Return(cluster.TestAlternativeClusterName).AnyTimes()
	historySize := 111
	executionInfo := &persistence.WorkflowExecutionInfo{
		CreateRequestID:             requestID,
		DomainID:                    domainID,
		WorkflowID:                  workflowID,
		RunID:                       runID,
		ParentDomainID:              parentDomainID,
		ParentWorkflowID:            parentWorkflowID,
		ParentRunID:                 parentRunID,
		InitiatedID:                 initiatedID,
		TaskList:                    tasklist,
		WorkflowTypeName:            workflowType,
		WorkflowTimeout:             workflowTimeout,
		DecisionStartToCloseTimeout: decisionTimeout,
		NextEventID:                 nextEventID,
		LastProcessedEvent:          common.EmptyEventID,
		BranchToken:                 []byte("some random branch token"),
		DecisionVersion:             di.Version,
		DecisionScheduleID:          di.ScheduleID,
		DecisionStartedID:           di.StartedID,
		DecisionTimeout:             di.DecisionTimeout,
		State:                       persistence.WorkflowStateRunning,
		CloseStatus:                 persistence.WorkflowCloseStatusNone,
	}
	msBuilder.EXPECT().GetCurrentBranchToken().Return(executionInfo.BranchToken, nil).AnyTimes()
	msBuilder.EXPECT().GetExecutionInfo().Return(executionInfo).AnyTimes()
	newWorkflowSnapshot := &persistence.WorkflowSnapshot{
		ExecutionInfo:    executionInfo,
		ExecutionStats:   &persistence.ExecutionStats{HistorySize: int64(historySize)},
		ReplicationState: replicationState,
		TransferTasks:    transferTasks,
		TimerTasks:       timerTasks,
	}
	newWorkflowEventsSeq := []*persistence.WorkflowEvents{{
		DomainID:    domainID,
		WorkflowID:  workflowID,
		RunID:       runID,
		BranchToken: executionInfo.BranchToken,
		Events:      history.Events,
	}}
	msBuilder.EXPECT().CloseTransactionAsSnapshot(now.Local(), transactionPolicyPassive).Return(newWorkflowSnapshot, newWorkflowEventsSeq, nil).Times(1)
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: historySize}, nil).Once()

	currentVersion := version + 1
	currentRunID := uuid.New()
	currentState := persistence.WorkflowStateRunning
	errRet := &persistence.WorkflowExecutionAlreadyStartedError{
		RunID:            currentRunID,
		State:            currentState,
		LastWriteVersion: currentVersion,
	}
	delReq := &persistence.DeleteHistoryBranchRequest{
		BranchToken: executionInfo.BranchToken,
		ShardID:     common.IntPtr(testShardID),
	}
	// the test above already assert the create workflow request, so here just use anything
	s.mockExecutionMgr.On("CreateWorkflowExecution", mock.Anything).Return(nil, errRet).Once()
	s.mockHistoryV2Mgr.On("DeleteHistoryBranch", delReq).Return(nil).Once()
	s.mockDomainCache.EXPECT().GetDomainByID(domainID).Return(
		cache.NewGlobalDomainCacheEntryForTest(
			&persistence.DomainInfo{ID: domainID},
			&persistence.DomainConfig{Retention: 1},
			&persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					{ClusterName: cluster.TestCurrentClusterName},
					{ClusterName: cluster.TestAlternativeClusterName},
				},
			},
			0, // not used
			nil,
		), nil,
	).AnyTimes()

	contextCurrent := NewMockworkflowExecutionContext(s.controller)
	contextCurrent.EXPECT().lock(gomock.Any()).Return(nil).Times(1)
	contextCurrent.EXPECT().unlock().Times(1)

	msBuilderCurrent := NewMockmutableState(s.controller)

	contextCurrent.EXPECT().loadWorkflowExecution().Return(msBuilderCurrent, nil).Times(1)
	currentExecution := &shared.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(currentRunID),
	}
	contextCurrentCacheKey := definition.NewWorkflowIdentifier(domainID, currentExecution.GetWorkflowId(), currentExecution.GetRunId())
	_, _ = s.historyReplicator.historyCache.PutIfNotExist(contextCurrentCacheKey, contextCurrent)

	s.mockExecutionMgr.On("GetCurrentExecution", &persistence.GetCurrentExecutionRequest{
		DomainID:   domainID,
		WorkflowID: workflowID,
	}).Return(&persistence.GetCurrentExecutionResponse{
		RunID: currentRunID,
		// other attributes are not used
	}, nil)

	err := s.historyReplicator.replicateWorkflowStarted(ctx.Background(), context, msBuilder, history, s.mockStateBuilder, s.logger)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestReplicateWorkflowStarted_CurrentRunning_IncomingLessThanCurrent_EventsReapplication_PendingDecision() {
	domainID := testDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	version := int64(144)
	tasklist := "some random tasklist"
	workflowType := "some random workflow type"
	workflowTimeout := int32(3721)
	decisionTimeout := int32(4411)

	initiatedID := int64(4810)
	parentDomainID := testDomainID
	parentWorkflowID := "some random workflow ID"
	parentRunID := uuid.New()

	signalName := "some random signal name"
	signalInput := []byte("some random signal input")
	signalIdentity := "some random signal identity"

	context := newWorkflowExecutionContext(domainID, shared.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(runID),
	}, s.mockShard, s.mockExecutionMgr, s.logger)
	msBuilder := NewMockmutableState(s.controller)

	di := &decisionInfo{
		Version:         version,
		ScheduleID:      common.FirstEventID + 1,
		StartedID:       common.EmptyEventID,
		DecisionTimeout: decisionTimeout,
		TaskList:        tasklist,
	}

	requestID := uuid.New()
	now := time.Now()
	history := &shared.History{
		Events: []*shared.HistoryEvent{
			{Version: common.Int64Ptr(version), EventId: common.Int64Ptr(1), Timestamp: common.Int64Ptr(now.UnixNano())},
			{
				Version:   common.Int64Ptr(version),
				EventId:   common.Int64Ptr(2),
				EventType: shared.EventTypeWorkflowExecutionSignaled.Ptr(),
				Timestamp: common.Int64Ptr(now.UnixNano()),
				WorkflowExecutionSignaledEventAttributes: &shared.WorkflowExecutionSignaledEventAttributes{
					SignalName: common.StringPtr(signalName),
					Input:      signalInput,
					Identity:   common.StringPtr(signalIdentity),
				},
			},
		},
	}
	nextEventID := di.ScheduleID + 1
	replicationState := &persistence.ReplicationState{
		StartVersion:     version,
		CurrentVersion:   version,
		LastWriteVersion: version,
		LastWriteEventID: nextEventID - 1,
	}
	transferTasks := []persistence.Task{&persistence.CloseExecutionTask{Version: version}}
	timerTasks := []persistence.Task{&persistence.DeleteHistoryEventTask{Version: version}}

	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(version).Return(cluster.TestAlternativeClusterName).AnyTimes()
	historySize := 111
	executionInfo := &persistence.WorkflowExecutionInfo{
		CreateRequestID:             requestID,
		DomainID:                    domainID,
		WorkflowID:                  workflowID,
		RunID:                       runID,
		ParentDomainID:              parentDomainID,
		ParentWorkflowID:            parentWorkflowID,
		ParentRunID:                 parentRunID,
		InitiatedID:                 initiatedID,
		TaskList:                    tasklist,
		WorkflowTypeName:            workflowType,
		WorkflowTimeout:             workflowTimeout,
		DecisionStartToCloseTimeout: decisionTimeout,
		NextEventID:                 nextEventID,
		LastProcessedEvent:          common.EmptyEventID,
		BranchToken:                 []byte("some random branch token"),
		DecisionVersion:             di.Version,
		DecisionScheduleID:          di.ScheduleID,
		DecisionStartedID:           di.StartedID,
		DecisionTimeout:             di.DecisionTimeout,
		State:                       persistence.WorkflowStateRunning,
		CloseStatus:                 persistence.WorkflowCloseStatusNone,
	}
	msBuilder.EXPECT().GetCurrentBranchToken().Return(executionInfo.BranchToken, nil).AnyTimes()
	msBuilder.EXPECT().GetExecutionInfo().Return(executionInfo).AnyTimes()
	newWorkflowSnapshot := &persistence.WorkflowSnapshot{
		ExecutionInfo:    executionInfo,
		ExecutionStats:   &persistence.ExecutionStats{HistorySize: int64(historySize)},
		ReplicationState: replicationState,
		TransferTasks:    transferTasks,
		TimerTasks:       timerTasks,
	}
	newWorkflowEventsSeq := []*persistence.WorkflowEvents{{
		DomainID:    domainID,
		WorkflowID:  workflowID,
		RunID:       runID,
		BranchToken: executionInfo.BranchToken,
		Events:      history.Events,
	}}
	msBuilder.EXPECT().CloseTransactionAsSnapshot(now.Local(), transactionPolicyPassive).Return(newWorkflowSnapshot, newWorkflowEventsSeq, nil).Times(1)
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: historySize}, nil).Once()

	currentVersion := version + 1
	currentRunID := uuid.New()
	currentState := persistence.WorkflowStateRunning
	errRet := &persistence.WorkflowExecutionAlreadyStartedError{
		RunID:            currentRunID,
		State:            currentState,
		LastWriteVersion: currentVersion,
	}
	// the test above already assert the create workflow request, so here just use anything
	s.mockExecutionMgr.On("CreateWorkflowExecution", mock.Anything).Return(nil, errRet).Once()
	s.mockHistoryV2Mgr.On("DeleteHistoryBranch", mock.Anything).Return(nil).Once()
	s.mockDomainCache.EXPECT().GetDomainByID(domainID).Return(
		cache.NewGlobalDomainCacheEntryForTest(
			&persistence.DomainInfo{ID: domainID},
			&persistence.DomainConfig{Retention: 1},
			&persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					{ClusterName: cluster.TestCurrentClusterName},
					{ClusterName: cluster.TestAlternativeClusterName},
				},
			},
			0, // not used
			nil,
		), nil,
	).AnyTimes()

	contextCurrent := NewMockworkflowExecutionContext(s.controller)
	contextCurrent.EXPECT().lock(gomock.Any()).Return(nil).Times(1)
	contextCurrent.EXPECT().unlock().Times(1)

	msBuilderCurrent := NewMockmutableState(s.controller)

	contextCurrent.EXPECT().loadWorkflowExecution().Return(msBuilderCurrent, nil).Times(1)
	currentExecution := &shared.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(currentRunID),
	}
	contextCurrentCacheKey := definition.NewWorkflowIdentifier(domainID, currentExecution.GetWorkflowId(), currentExecution.GetRunId())
	_, _ = s.historyReplicator.historyCache.PutIfNotExist(contextCurrentCacheKey, contextCurrent)

	s.mockExecutionMgr.On("GetCurrentExecution", &persistence.GetCurrentExecutionRequest{
		DomainID:   domainID,
		WorkflowID: workflowID,
	}).Return(&persistence.GetCurrentExecutionResponse{
		RunID: currentRunID,
		// other attributes are not used
	}, nil)

	msBuilderCurrent.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes()
	msBuilderCurrent.EXPECT().GetLastWriteVersion().Return(currentVersion, nil).AnyTimes()
	msBuilderCurrent.EXPECT().AddWorkflowExecutionSignaled(signalName, signalInput, signalIdentity).Return(&shared.HistoryEvent{
		EventType: shared.EventTypeWorkflowExecutionSignaled.Ptr(),
		Timestamp: common.Int64Ptr(time.Now().UnixNano()),
		WorkflowExecutionSignaledEventAttributes: &shared.WorkflowExecutionSignaledEventAttributes{
			SignalName: common.StringPtr(signalName),
			Input:      signalInput,
			Identity:   common.StringPtr(signalIdentity),
		},
	}, nil).Times(1)
	msBuilderCurrent.EXPECT().UpdateCurrentVersion(currentVersion, true).Return(nil).Times(1)
	msBuilderCurrent.EXPECT().HasPendingDecision().Return(true).Times(1)
	contextCurrent.EXPECT().updateWorkflowExecutionAsActive(gomock.Any()).Return(nil).Times(1)

	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(currentVersion).Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()

	err := s.historyReplicator.replicateWorkflowStarted(ctx.Background(), context, msBuilder, history, s.mockStateBuilder, s.logger)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestReplicateWorkflowStarted_CurrentRunning_IncomingLessThanCurrent_EventsReapplication_NoPendingDecision() {
	domainID := testDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	version := int64(144)
	tasklist := "some random tasklist"
	workflowType := "some random workflow type"
	workflowTimeout := int32(3721)
	decisionTimeout := int32(4411)

	initiatedID := int64(4810)
	parentDomainID := testDomainID
	parentWorkflowID := "some random workflow ID"
	parentRunID := uuid.New()

	signalName := "some random signal name"
	signalInput := []byte("some random signal input")
	signalIdentity := "some random signal identity"

	context := newWorkflowExecutionContext(domainID, shared.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(runID),
	}, s.mockShard, s.mockExecutionMgr, s.logger)
	msBuilder := NewMockmutableState(s.controller)

	di := &decisionInfo{
		Version:         version,
		ScheduleID:      common.FirstEventID + 1,
		StartedID:       common.EmptyEventID,
		DecisionTimeout: decisionTimeout,
		TaskList:        tasklist,
	}

	requestID := uuid.New()
	now := time.Now()
	history := &shared.History{
		Events: []*shared.HistoryEvent{
			{Version: common.Int64Ptr(version), EventId: common.Int64Ptr(1), Timestamp: common.Int64Ptr(now.UnixNano())},
			{
				Version:   common.Int64Ptr(version),
				EventId:   common.Int64Ptr(2),
				EventType: shared.EventTypeWorkflowExecutionSignaled.Ptr(),
				Timestamp: common.Int64Ptr(now.UnixNano()),
				WorkflowExecutionSignaledEventAttributes: &shared.WorkflowExecutionSignaledEventAttributes{
					SignalName: common.StringPtr(signalName),
					Input:      signalInput,
					Identity:   common.StringPtr(signalIdentity),
				},
			},
		},
	}
	nextEventID := di.ScheduleID + 1
	replicationState := &persistence.ReplicationState{
		StartVersion:     version,
		CurrentVersion:   version,
		LastWriteVersion: version,
		LastWriteEventID: nextEventID - 1,
	}
	transferTasks := []persistence.Task{&persistence.CloseExecutionTask{Version: version}}
	timerTasks := []persistence.Task{&persistence.DeleteHistoryEventTask{Version: version}}

	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(version).Return(cluster.TestAlternativeClusterName).AnyTimes()
	historySize := 111
	executionInfo := &persistence.WorkflowExecutionInfo{
		CreateRequestID:             requestID,
		DomainID:                    domainID,
		WorkflowID:                  workflowID,
		RunID:                       runID,
		ParentDomainID:              parentDomainID,
		ParentWorkflowID:            parentWorkflowID,
		ParentRunID:                 parentRunID,
		InitiatedID:                 initiatedID,
		TaskList:                    tasklist,
		WorkflowTypeName:            workflowType,
		WorkflowTimeout:             workflowTimeout,
		DecisionStartToCloseTimeout: decisionTimeout,
		NextEventID:                 nextEventID,
		LastProcessedEvent:          common.EmptyEventID,
		BranchToken:                 []byte("some random branch token"),
		DecisionVersion:             di.Version,
		DecisionScheduleID:          di.ScheduleID,
		DecisionStartedID:           di.StartedID,
		DecisionTimeout:             di.DecisionTimeout,
		State:                       persistence.WorkflowStateRunning,
		CloseStatus:                 persistence.WorkflowCloseStatusNone,
	}
	msBuilder.EXPECT().GetCurrentBranchToken().Return(executionInfo.BranchToken, nil).AnyTimes()
	msBuilder.EXPECT().GetExecutionInfo().Return(executionInfo).AnyTimes()
	newWorkflowSnapshot := &persistence.WorkflowSnapshot{
		ExecutionInfo:    executionInfo,
		ExecutionStats:   &persistence.ExecutionStats{HistorySize: int64(historySize)},
		ReplicationState: replicationState,
		TransferTasks:    transferTasks,
		TimerTasks:       timerTasks,
	}
	newWorkflowEventsSeq := []*persistence.WorkflowEvents{{
		DomainID:    domainID,
		WorkflowID:  workflowID,
		RunID:       runID,
		BranchToken: executionInfo.BranchToken,
		Events:      history.Events,
	}}
	msBuilder.EXPECT().CloseTransactionAsSnapshot(now.Local(), transactionPolicyPassive).Return(newWorkflowSnapshot, newWorkflowEventsSeq, nil).Times(1)
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: historySize}, nil).Once()

	currentVersion := version + 1
	currentRunID := uuid.New()
	currentState := persistence.WorkflowStateRunning
	currentDecisionStickyTasklist := "some random decision sticky tasklist"

	errRet := &persistence.WorkflowExecutionAlreadyStartedError{
		RunID:            currentRunID,
		State:            currentState,
		LastWriteVersion: currentVersion,
	}
	// the test above already assert the create workflow request, so here just use anything
	s.mockExecutionMgr.On("CreateWorkflowExecution", mock.Anything).Return(nil, errRet).Once()
	s.mockHistoryV2Mgr.On("DeleteHistoryBranch", mock.Anything).Return(nil).Once()
	s.mockDomainCache.EXPECT().GetDomainByID(domainID).Return(
		cache.NewGlobalDomainCacheEntryForTest(
			&persistence.DomainInfo{ID: domainID},
			&persistence.DomainConfig{Retention: 1},
			&persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					{ClusterName: cluster.TestCurrentClusterName},
					{ClusterName: cluster.TestAlternativeClusterName},
				},
			},
			0, // not used
			nil,
		), nil,
	).AnyTimes()

	contextCurrent := NewMockworkflowExecutionContext(s.controller)
	contextCurrent.EXPECT().lock(gomock.Any()).Return(nil).Times(1)
	contextCurrent.EXPECT().unlock().Times(1)

	msBuilderCurrent := NewMockmutableState(s.controller)

	contextCurrent.EXPECT().loadWorkflowExecution().Return(msBuilderCurrent, nil).Times(1)
	currentExecution := &shared.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(currentRunID),
	}
	contextCurrentCacheKey := definition.NewWorkflowIdentifier(domainID, currentExecution.GetWorkflowId(), currentExecution.GetRunId())
	_, _ = s.historyReplicator.historyCache.PutIfNotExist(contextCurrentCacheKey, contextCurrent)

	s.mockExecutionMgr.On("GetCurrentExecution", &persistence.GetCurrentExecutionRequest{
		DomainID:   domainID,
		WorkflowID: workflowID,
	}).Return(&persistence.GetCurrentExecutionResponse{
		RunID: currentRunID,
		// other attributes are not used
	}, nil)

	msBuilderCurrent.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes()
	msBuilderCurrent.EXPECT().GetLastWriteVersion().Return(currentVersion, nil).AnyTimes()
	msBuilderCurrent.EXPECT().AddWorkflowExecutionSignaled(signalName, signalInput, signalIdentity).Return(&shared.HistoryEvent{
		EventType: shared.EventTypeWorkflowExecutionSignaled.Ptr(),
		Timestamp: common.Int64Ptr(time.Now().UnixNano()),
		WorkflowExecutionSignaledEventAttributes: &shared.WorkflowExecutionSignaledEventAttributes{
			SignalName: common.StringPtr(signalName),
			Input:      signalInput,
			Identity:   common.StringPtr(signalIdentity),
		},
	}, nil).Times(1)
	msBuilderCurrent.EXPECT().UpdateCurrentVersion(currentVersion, true).Return(nil).Times(1)
	msBuilderCurrent.EXPECT().HasPendingDecision().Return(false).Times(1)

	newDecision := &decisionInfo{
		Version:    currentVersion,
		ScheduleID: 1234,
		StartedID:  common.EmptyEventID,
		TaskList:   currentDecisionStickyTasklist,
		Attempt:    0,
	}
	msBuilderCurrent.EXPECT().AddDecisionTaskScheduledEvent(false).Return(newDecision, nil).Times(1)

	contextCurrent.EXPECT().updateWorkflowExecutionAsActive(gomock.Any()).Return(nil).Times(1)

	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(currentVersion).Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()

	err := s.historyReplicator.replicateWorkflowStarted(ctx.Background(), context, msBuilder, history, s.mockStateBuilder, s.logger)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestReplicateWorkflowStarted_CurrentRunning_IncomingEqualToCurrent() {
	domainID := testDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	version := int64(144)
	tasklist := "some random tasklist"
	workflowType := "some random workflow type"
	workflowTimeout := int32(3721)
	decisionTimeout := int32(4411)

	initiatedID := int64(4810)
	parentDomainID := testDomainID
	parentWorkflowID := "some random workflow ID"
	parentRunID := uuid.New()

	context := newWorkflowExecutionContext(domainID, shared.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(runID),
	}, s.mockShard, s.mockExecutionMgr, s.logger)
	msBuilder := NewMockmutableState(s.controller)

	di := &decisionInfo{
		Version:         version,
		ScheduleID:      common.FirstEventID + 1,
		StartedID:       common.EmptyEventID,
		DecisionTimeout: decisionTimeout,
		TaskList:        tasklist,
	}

	requestID := uuid.New()
	now := time.Now()
	history := &shared.History{
		Events: []*shared.HistoryEvent{
			{Version: common.Int64Ptr(version), EventId: common.Int64Ptr(1), Timestamp: common.Int64Ptr(now.UnixNano())},
			{Version: common.Int64Ptr(version), EventId: common.Int64Ptr(2), Timestamp: common.Int64Ptr(now.UnixNano())},
		},
	}
	nextEventID := di.ScheduleID + 1
	replicationState := &persistence.ReplicationState{
		StartVersion:     version,
		CurrentVersion:   version,
		LastWriteVersion: version,
		LastWriteEventID: nextEventID - 1,
	}
	transferTasks := []persistence.Task{&persistence.CloseExecutionTask{Version: version}}
	timerTasks := []persistence.Task{&persistence.DeleteHistoryEventTask{Version: version}}

	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(version).Return(cluster.TestAlternativeClusterName).AnyTimes()
	historySize := 111
	executionInfo := &persistence.WorkflowExecutionInfo{
		CreateRequestID:             requestID,
		DomainID:                    domainID,
		WorkflowID:                  workflowID,
		RunID:                       runID,
		ParentDomainID:              parentDomainID,
		ParentWorkflowID:            parentWorkflowID,
		ParentRunID:                 parentRunID,
		InitiatedID:                 initiatedID,
		TaskList:                    tasklist,
		WorkflowTypeName:            workflowType,
		WorkflowTimeout:             workflowTimeout,
		DecisionStartToCloseTimeout: decisionTimeout,
		NextEventID:                 nextEventID,
		LastProcessedEvent:          common.EmptyEventID,
		BranchToken:                 []byte("some random branch token"),
		DecisionVersion:             di.Version,
		DecisionScheduleID:          di.ScheduleID,
		DecisionStartedID:           di.StartedID,
		DecisionTimeout:             di.DecisionTimeout,
		State:                       persistence.WorkflowStateRunning,
		CloseStatus:                 persistence.WorkflowCloseStatusNone,
	}
	msBuilder.EXPECT().GetExecutionInfo().Return(executionInfo).AnyTimes()
	newWorkflowSnapshot := &persistence.WorkflowSnapshot{
		ExecutionInfo:    executionInfo,
		ExecutionStats:   &persistence.ExecutionStats{HistorySize: int64(historySize)},
		ReplicationState: replicationState,
		TransferTasks:    transferTasks,
		TimerTasks:       timerTasks,
	}
	newWorkflowEventsSeq := []*persistence.WorkflowEvents{{
		DomainID:    domainID,
		WorkflowID:  workflowID,
		RunID:       runID,
		BranchToken: executionInfo.BranchToken,
		Events:      history.Events,
	}}
	msBuilder.EXPECT().CloseTransactionAsSnapshot(now.Local(), transactionPolicyPassive).Return(newWorkflowSnapshot, newWorkflowEventsSeq, nil).Times(1)
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: historySize}, nil).Once()

	currentVersion := version
	currentRunID := uuid.New()
	currentNextEventID := int64(3456)
	currentState := persistence.WorkflowStateRunning
	errRet := &persistence.WorkflowExecutionAlreadyStartedError{
		RunID:            currentRunID,
		State:            currentState,
		LastWriteVersion: currentVersion,
	}
	// the test above already assert the create workflow request, so here just use anyting
	s.mockExecutionMgr.On("CreateWorkflowExecution", mock.Anything).Return(nil, errRet).Once()

	contextCurrent := NewMockworkflowExecutionContext(s.controller)
	contextCurrent.EXPECT().lock(gomock.Any()).Return(nil).Times(1)
	contextCurrent.EXPECT().unlock().Times(1)

	msBuilderCurrent := NewMockmutableState(s.controller)

	contextCurrent.EXPECT().loadWorkflowExecution().Return(msBuilderCurrent, nil).Times(1)
	currentExecution := &shared.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(currentRunID),
	}
	contextCurrentCacheKey := definition.NewWorkflowIdentifier(domainID, currentExecution.GetWorkflowId(), currentExecution.GetRunId())
	_, _ = s.historyReplicator.historyCache.PutIfNotExist(contextCurrentCacheKey, contextCurrent)

	s.mockExecutionMgr.On("GetCurrentExecution", &persistence.GetCurrentExecutionRequest{
		DomainID:   domainID,
		WorkflowID: workflowID,
	}).Return(&persistence.GetCurrentExecutionResponse{
		RunID: currentRunID,
		// other attributes are not used
	}, nil)

	msBuilderCurrent.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{
		DomainID:           domainID,
		WorkflowID:         workflowID,
		RunID:              currentRunID,
		DecisionVersion:    common.EmptyVersion,
		DecisionScheduleID: common.EmptyEventID,
		DecisionStartedID:  common.EmptyEventID,
	}).AnyTimes()
	msBuilderCurrent.EXPECT().GetNextEventID().Return(currentNextEventID).AnyTimes()

	s.mockDomainCache.EXPECT().GetDomainByID(domainID).Return(
		cache.NewGlobalDomainCacheEntryForTest(
			&persistence.DomainInfo{ID: domainID},
			&persistence.DomainConfig{Retention: 1},
			&persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					{ClusterName: cluster.TestCurrentClusterName},
					{ClusterName: cluster.TestAlternativeClusterName},
				},
			},
			0, // not used
			nil,
		), nil,
	).AnyTimes()

	err := s.historyReplicator.replicateWorkflowStarted(ctx.Background(), context, msBuilder, history, s.mockStateBuilder, s.logger)
	s.Equal(newRetryTaskErrorWithHint(ErrRetryExistingWorkflowMsg, domainID, workflowID, currentRunID, currentNextEventID), err)
}

func (s *historyReplicatorSuite) TestReplicateWorkflowStarted_CurrentRunning_IncomingEqualToCurrent_OutOfOrder() {
	domainID := testDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	version := int64(144)
	tasklist := "some random tasklist"
	workflowType := "some random workflow type"
	workflowTimeout := int32(3721)
	decisionTimeout := int32(4411)

	initiatedID := int64(4810)
	parentDomainID := testDomainID
	parentWorkflowID := "some random workflow ID"
	parentRunID := uuid.New()
	lastEventTaskID := int64(2333)

	context := newWorkflowExecutionContext(domainID, shared.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(runID),
	}, s.mockShard, s.mockExecutionMgr, s.logger)
	msBuilder := NewMockmutableState(s.controller)

	di := &decisionInfo{
		Version:         version,
		ScheduleID:      common.FirstEventID + 1,
		StartedID:       common.EmptyEventID,
		DecisionTimeout: decisionTimeout,
		TaskList:        tasklist,
	}

	requestID := uuid.New()
	now := time.Now()
	history := &shared.History{
		Events: []*shared.HistoryEvent{
			{Version: common.Int64Ptr(version), EventId: common.Int64Ptr(1), Timestamp: common.Int64Ptr(now.UnixNano())},
			{Version: common.Int64Ptr(version), EventId: common.Int64Ptr(2), Timestamp: common.Int64Ptr(now.UnixNano())},
		},
	}
	nextEventID := di.ScheduleID + 1
	replicationState := &persistence.ReplicationState{
		StartVersion:     version,
		CurrentVersion:   version,
		LastWriteVersion: version,
		LastWriteEventID: nextEventID - 1,
	}
	transferTasks := []persistence.Task{&persistence.CloseExecutionTask{Version: version}}
	timerTasks := []persistence.Task{&persistence.DeleteHistoryEventTask{Version: version}}

	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(version).Return(cluster.TestAlternativeClusterName).AnyTimes()
	historySize := 111
	executionInfo := &persistence.WorkflowExecutionInfo{
		CreateRequestID:             requestID,
		DomainID:                    domainID,
		WorkflowID:                  workflowID,
		RunID:                       runID,
		ParentDomainID:              parentDomainID,
		ParentWorkflowID:            parentWorkflowID,
		ParentRunID:                 parentRunID,
		InitiatedID:                 initiatedID,
		TaskList:                    tasklist,
		WorkflowTypeName:            workflowType,
		WorkflowTimeout:             workflowTimeout,
		DecisionStartToCloseTimeout: decisionTimeout,
		NextEventID:                 nextEventID,
		LastProcessedEvent:          common.EmptyEventID,
		BranchToken:                 []byte("some random branch token"),
		DecisionVersion:             di.Version,
		DecisionScheduleID:          di.ScheduleID,
		DecisionStartedID:           di.StartedID,
		DecisionTimeout:             di.DecisionTimeout,
		State:                       persistence.WorkflowStateRunning,
		CloseStatus:                 persistence.WorkflowCloseStatusNone,
	}
	msBuilder.EXPECT().GetExecutionInfo().Return(executionInfo).AnyTimes()
	newWorkflowSnapshot := &persistence.WorkflowSnapshot{
		ExecutionInfo:    executionInfo,
		ExecutionStats:   &persistence.ExecutionStats{HistorySize: int64(historySize)},
		ReplicationState: replicationState,
		TransferTasks:    transferTasks,
		TimerTasks:       timerTasks,
	}
	newWorkflowEventsSeq := []*persistence.WorkflowEvents{{
		DomainID:    domainID,
		WorkflowID:  workflowID,
		RunID:       runID,
		BranchToken: executionInfo.BranchToken,
		Events:      history.Events,
	}}
	msBuilder.EXPECT().CloseTransactionAsSnapshot(now.Local(), transactionPolicyPassive).Return(newWorkflowSnapshot, newWorkflowEventsSeq, nil).Times(1)
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: historySize}, nil).Once()

	currentVersion := version
	currentRunID := uuid.New()
	currentNextEventID := int64(3456)
	currentState := persistence.WorkflowStateRunning
	errRet := &persistence.WorkflowExecutionAlreadyStartedError{
		RunID:            currentRunID,
		State:            currentState,
		LastWriteVersion: currentVersion,
	}
	// the test above already assert the create workflow request, so here just use anything
	s.mockExecutionMgr.On("CreateWorkflowExecution", mock.Anything).Return(nil, errRet).Once()

	contextCurrent := NewMockworkflowExecutionContext(s.controller)
	contextCurrent.EXPECT().lock(gomock.Any()).Return(nil).Times(1)
	contextCurrent.EXPECT().unlock().Times(1)

	msBuilderCurrent := NewMockmutableState(s.controller)

	contextCurrent.EXPECT().loadWorkflowExecution().Return(msBuilderCurrent, nil).Times(1)
	currentExecution := &shared.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(currentRunID),
	}
	contextCurrentCacheKey := definition.NewWorkflowIdentifier(domainID, currentExecution.GetWorkflowId(), currentExecution.GetRunId())
	_, _ = s.historyReplicator.historyCache.PutIfNotExist(contextCurrentCacheKey, contextCurrent)

	s.mockExecutionMgr.On("GetCurrentExecution", &persistence.GetCurrentExecutionRequest{
		DomainID:   domainID,
		WorkflowID: workflowID,
	}).Return(&persistence.GetCurrentExecutionResponse{
		RunID: currentRunID,
		// other attributes are not used
	}, nil)

	msBuilderCurrent.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{
		DomainID:           domainID,
		WorkflowID:         workflowID,
		RunID:              currentRunID,
		LastEventTaskID:    lastEventTaskID + 10,
		DecisionVersion:    common.EmptyVersion,
		DecisionScheduleID: common.EmptyEventID,
		DecisionStartedID:  common.EmptyEventID,
	}).AnyTimes()
	msBuilderCurrent.EXPECT().GetNextEventID().Return(currentNextEventID).AnyTimes()

	s.mockDomainCache.EXPECT().GetDomainByID(domainID).Return(
		cache.NewGlobalDomainCacheEntryForTest(
			&persistence.DomainInfo{ID: domainID},
			&persistence.DomainConfig{Retention: 1},
			&persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					{ClusterName: cluster.TestCurrentClusterName},
					{ClusterName: cluster.TestAlternativeClusterName},
				},
			},
			0, // not used
			nil,
		), nil,
	).AnyTimes()

	err := s.historyReplicator.replicateWorkflowStarted(ctx.Background(), context, msBuilder, history, s.mockStateBuilder, s.logger)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestReplicateWorkflowStarted_CurrentRunning_IncomingLargerThanCurrent() {
	domainID := testDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	version := int64(144)
	tasklist := "some random tasklist"
	workflowType := "some random workflow type"
	workflowTimeout := int32(3721)
	decisionTimeout := int32(4411)
	cronSchedule := "some random cron scredule"
	retryPolicy := &workflow.RetryPolicy{
		InitialIntervalInSeconds:    common.Int32Ptr(1),
		MaximumAttempts:             common.Int32Ptr(3),
		MaximumIntervalInSeconds:    common.Int32Ptr(1),
		NonRetriableErrorReasons:    []string{"bad-bug"},
		BackoffCoefficient:          common.Float64Ptr(1),
		ExpirationIntervalInSeconds: common.Int32Ptr(100),
	}

	initiatedID := int64(4810)
	parentDomainID := testDomainID
	parentWorkflowID := "some random workflow ID"
	parentRunID := uuid.New()

	context := newWorkflowExecutionContext(domainID, shared.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(runID),
	}, s.mockShard, s.mockExecutionMgr, s.logger)
	msBuilder := NewMockmutableState(s.controller)

	di := &decisionInfo{
		Version:         version,
		ScheduleID:      common.FirstEventID + 1,
		StartedID:       common.EmptyEventID,
		DecisionTimeout: decisionTimeout,
		TaskList:        tasklist,
	}

	requestID := uuid.New()
	now := time.Now()
	history := &shared.History{
		Events: []*shared.HistoryEvent{
			{Version: common.Int64Ptr(version), EventId: common.Int64Ptr(1), Timestamp: common.Int64Ptr(now.UnixNano())},
			{Version: common.Int64Ptr(version), EventId: common.Int64Ptr(2), Timestamp: common.Int64Ptr(now.UnixNano())},
		},
	}
	nextEventID := di.ScheduleID + 1
	replicationState := &persistence.ReplicationState{
		StartVersion:     version,
		CurrentVersion:   version,
		LastWriteVersion: version,
		LastWriteEventID: nextEventID - 1,
	}
	transferTasks := []persistence.Task{&persistence.CloseExecutionTask{Version: version}}
	timerTasks := []persistence.Task{&persistence.DeleteHistoryEventTask{Version: version}}

	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(version).Return(cluster.TestAlternativeClusterName).AnyTimes()
	historySize := 111
	executionInfo := &persistence.WorkflowExecutionInfo{
		CreateRequestID:             requestID,
		DomainID:                    domainID,
		WorkflowID:                  workflowID,
		RunID:                       runID,
		ParentDomainID:              parentDomainID,
		ParentWorkflowID:            parentWorkflowID,
		ParentRunID:                 parentRunID,
		InitiatedID:                 initiatedID,
		TaskList:                    tasklist,
		WorkflowTypeName:            workflowType,
		WorkflowTimeout:             workflowTimeout,
		DecisionStartToCloseTimeout: decisionTimeout,
		NextEventID:                 nextEventID,
		LastProcessedEvent:          common.EmptyEventID,
		BranchToken:                 []byte("some random branch token"),
		DecisionVersion:             di.Version,
		DecisionScheduleID:          di.ScheduleID,
		DecisionStartedID:           di.StartedID,
		DecisionTimeout:             di.DecisionTimeout,
		State:                       persistence.WorkflowStateRunning,
		CloseStatus:                 persistence.WorkflowCloseStatusNone,
		CronSchedule:                cronSchedule,
		HasRetryPolicy:              true,
		InitialInterval:             retryPolicy.GetInitialIntervalInSeconds(),
		BackoffCoefficient:          retryPolicy.GetBackoffCoefficient(),
		MaximumInterval:             retryPolicy.GetMaximumIntervalInSeconds(),
		ExpirationSeconds:           retryPolicy.GetExpirationIntervalInSeconds(),
		MaximumAttempts:             retryPolicy.GetMaximumAttempts(),
		NonRetriableErrors:          retryPolicy.GetNonRetriableErrorReasons(),
	}
	msBuilder.EXPECT().GetExecutionInfo().Return(executionInfo).AnyTimes()
	newWorkflowSnapshot := &persistence.WorkflowSnapshot{
		ExecutionInfo:    executionInfo,
		ExecutionStats:   &persistence.ExecutionStats{HistorySize: int64(historySize)},
		ReplicationState: replicationState,
		TransferTasks:    transferTasks,
		TimerTasks:       timerTasks,
	}
	newWorkflowEventsSeq := []*persistence.WorkflowEvents{{
		DomainID:    domainID,
		WorkflowID:  workflowID,
		RunID:       runID,
		BranchToken: executionInfo.BranchToken,
		Events:      history.Events,
	}}
	msBuilder.EXPECT().CloseTransactionAsSnapshot(now.Local(), transactionPolicyPassive).Return(newWorkflowSnapshot, newWorkflowEventsSeq, nil).Times(1)
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: historySize}, nil).Once()

	currentNextEventID := int64(2333)
	currentVersion := version - 1
	currentRunID := uuid.New()
	currentState := persistence.WorkflowStateRunning
	errRet := &persistence.WorkflowExecutionAlreadyStartedError{
		RunID:            currentRunID,
		State:            currentState,
		LastWriteVersion: currentVersion,
	}
	// the test above already assert the create workflow request, so here just use anyting
	s.mockExecutionMgr.On("CreateWorkflowExecution", mock.MatchedBy(func(input *persistence.CreateWorkflowExecutionRequest) bool {
		input.RangeID = 0
		return reflect.DeepEqual(&persistence.CreateWorkflowExecutionRequest{
			Mode:                persistence.CreateWorkflowModeBrandNew,
			PreviousRunID:       "",
			NewWorkflowSnapshot: *newWorkflowSnapshot,
		}, input)
	})).Return(nil, errRet).Once()
	s.mockExecutionMgr.On("CreateWorkflowExecution", mock.MatchedBy(func(input *persistence.CreateWorkflowExecutionRequest) bool {
		input.RangeID = 0
		return reflect.DeepEqual(&persistence.CreateWorkflowExecutionRequest{
			Mode:                     persistence.CreateWorkflowModeWorkflowIDReuse,
			PreviousRunID:            currentRunID,
			PreviousLastWriteVersion: currentVersion,
			NewWorkflowSnapshot:      *newWorkflowSnapshot,
		}, input)
	})).Return(&persistence.CreateWorkflowExecutionResponse{}, nil).Once()

	// this mocks are for the terminate current workflow operation
	domainVersion := int64(4081)
	domainName := "some random domain name"
	s.mockDomainCache.EXPECT().GetDomainByID(domainID).Return(
		cache.NewGlobalDomainCacheEntryForTest(
			&persistence.DomainInfo{ID: domainID, Name: domainName},
			&persistence.DomainConfig{Retention: 1},
			&persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					{ClusterName: cluster.TestCurrentClusterName},
					{ClusterName: cluster.TestAlternativeClusterName},
				},
			},
			domainVersion,
			nil,
		), nil,
	).AnyTimes()

	contextCurrent := NewMockworkflowExecutionContext(s.controller)
	contextCurrent.EXPECT().lock(gomock.Any()).Return(nil).Times(1)
	contextCurrent.EXPECT().unlock().Times(1)
	msBuilderCurrent := NewMockmutableState(s.controller)

	contextCurrent.EXPECT().loadWorkflowExecution().Return(msBuilderCurrent, nil).Times(1)
	currentExecution := &shared.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(currentRunID),
	}
	contextCurrent.EXPECT().getExecution().Return(currentExecution).AnyTimes()
	contextCurrentCacheKey := definition.NewWorkflowIdentifier(domainID, currentExecution.GetWorkflowId(), currentExecution.GetRunId())
	_, _ = s.historyReplicator.historyCache.PutIfNotExist(contextCurrentCacheKey, contextCurrent)

	msBuilderCurrent.EXPECT().GetNextEventID().Return(currentNextEventID).AnyTimes()
	msBuilderCurrent.EXPECT().GetLastWriteVersion().Return(currentVersion, nil).AnyTimes()
	msBuilderCurrent.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes() // this is used to update the version on mutable state
	msBuilderCurrent.EXPECT().UpdateCurrentVersion(currentVersion, true).Return(nil).Times(1)

	currentClusterName := cluster.TestCurrentClusterName
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(currentVersion).Return(currentClusterName).AnyTimes()

	msBuilderCurrent.EXPECT().AddWorkflowExecutionTerminatedEvent(
		currentNextEventID, workflowTerminationReason, gomock.Any(), workflowTerminationIdentity,
	).Return(&workflow.HistoryEvent{}, nil).Times(1)
	contextCurrent.EXPECT().updateWorkflowExecutionAsActive(gomock.Any()).Return(nil).Times(1)

	err := s.historyReplicator.replicateWorkflowStarted(ctx.Background(), context, msBuilder, history, s.mockStateBuilder, s.logger)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestConflictResolutionTerminateCurrentRunningIfNotSelf_TargetRunning() {
	runID := uuid.New()
	lastWriteVersion := int64(1394)
	state := persistence.WorkflowStateRunning
	incomingVersion := int64(4096)
	incomingTimestamp := int64(11238)

	msBuilderTarget := NewMockmutableState(s.controller)

	msBuilderTarget.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes()
	msBuilderTarget.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{
		RunID:              runID,
		State:              state,
		DecisionVersion:    common.EmptyVersion,
		DecisionScheduleID: common.EmptyEventID,
		DecisionStartedID:  common.EmptyEventID,
	}).AnyTimes()
	msBuilderTarget.EXPECT().GetLastWriteVersion().Return(lastWriteVersion, nil).AnyTimes()
	prevRunID, prevLastWriteVersion, prevState, err := s.historyReplicator.conflictResolutionTerminateCurrentRunningIfNotSelf(
		ctx.Background(), msBuilderTarget, incomingVersion, incomingTimestamp, s.logger,
	)
	s.Nil(err)
	s.Equal(runID, prevRunID)
	s.Equal(lastWriteVersion, prevLastWriteVersion)
	s.Equal(state, prevState)
}

func (s *historyReplicatorSuite) TestConflictResolutionTerminateCurrentRunningIfNotSelf_TargetClosed_CurrentClosed() {
	incomingVersion := int64(4096)
	incomingTimestamp := int64(11238)

	domainID := testDomainID
	workflowID := "some random target workflow ID"
	targetRunID := uuid.New()

	msBuilderTarget := NewMockmutableState(s.controller)

	msBuilderTarget.EXPECT().IsWorkflowExecutionRunning().Return(false).AnyTimes()
	msBuilderTarget.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{
		DomainID:           domainID,
		WorkflowID:         workflowID,
		RunID:              targetRunID,
		State:              persistence.WorkflowStateCompleted,
		CloseStatus:        persistence.WorkflowCloseStatusContinuedAsNew,
		DecisionVersion:    common.EmptyVersion,
		DecisionScheduleID: common.EmptyEventID,
		DecisionStartedID:  common.EmptyEventID,
	}).AnyTimes()

	currentRunID := uuid.New()
	currentLastWriteVersion := int64(1394)
	currentState := persistence.WorkflowStateCompleted
	s.mockExecutionMgr.On("GetCurrentExecution", &persistence.GetCurrentExecutionRequest{
		DomainID:   domainID,
		WorkflowID: workflowID,
	}).Return(&persistence.GetCurrentExecutionResponse{
		RunID:            currentRunID,
		State:            currentState,
		CloseStatus:      persistence.WorkflowCloseStatusCompleted,
		LastWriteVersion: currentLastWriteVersion,
	}, nil)

	prevRunID, prevLastWriteVersion, prevState, err := s.historyReplicator.conflictResolutionTerminateCurrentRunningIfNotSelf(
		ctx.Background(), msBuilderTarget, incomingVersion, incomingTimestamp, s.logger,
	)
	s.Nil(err)
	s.Equal(currentRunID, prevRunID)
	s.Equal(currentLastWriteVersion, prevLastWriteVersion)
	s.Equal(currentState, prevState)
}

func (s *historyReplicatorSuite) TestConflictResolutionTerminateCurrentRunningIfNotSelf_TargetClosed_CurrentRunning_LowerVersion() {
	incomingVersion := int64(4096)
	incomingTimestamp := int64(11238)
	incomingCluster := cluster.TestAlternativeClusterName
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(incomingVersion).Return(incomingCluster).AnyTimes()

	domainID := testDomainID
	workflowID := "some random target workflow ID"
	targetRunID := uuid.New()

	msBuilderTarget := NewMockmutableState(s.controller)
	msBuilderTarget.EXPECT().IsWorkflowExecutionRunning().Return(false).AnyTimes()
	msBuilderTarget.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{
		DomainID:           domainID,
		WorkflowID:         workflowID,
		RunID:              targetRunID,
		State:              persistence.WorkflowStateCompleted,
		CloseStatus:        persistence.WorkflowCloseStatusContinuedAsNew,
		DecisionVersion:    common.EmptyVersion,
		DecisionScheduleID: common.EmptyEventID,
		DecisionStartedID:  common.EmptyEventID,
	}).AnyTimes()

	currentRunID := uuid.New()
	contextCurrent := NewMockworkflowExecutionContext(s.controller)
	contextCurrent.EXPECT().lock(gomock.Any()).Return(nil).Times(1)
	contextCurrent.EXPECT().unlock().Times(1)
	msBuilderCurrent := NewMockmutableState(s.controller)

	contextCurrent.EXPECT().loadWorkflowExecution().Return(msBuilderCurrent, nil).Times(1)
	currentExecution := &shared.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(currentRunID),
	}
	contextCurrent.EXPECT().getExecution().Return(currentExecution).AnyTimes()
	contextCurrentCacheKey := definition.NewWorkflowIdentifier(domainID, currentExecution.GetWorkflowId(), currentExecution.GetRunId())
	_, _ = s.historyReplicator.historyCache.PutIfNotExist(contextCurrentCacheKey, contextCurrent)

	currentNextEventID := int64(2333)
	currentVersion := incomingVersion - 10
	currentCluster := cluster.TestCurrentClusterName
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(currentVersion).Return(currentCluster).AnyTimes()

	msBuilderCurrent.EXPECT().GetNextEventID().Return(currentNextEventID).AnyTimes()
	msBuilderCurrent.EXPECT().GetLastWriteVersion().Return(currentVersion, nil).AnyTimes()
	msBuilderCurrent.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes() // this is used to update the version on mutable state
	msBuilderCurrent.EXPECT().UpdateCurrentVersion(currentVersion, true).Return(nil).Times(1)

	s.mockExecutionMgr.On("GetCurrentExecution", &persistence.GetCurrentExecutionRequest{
		DomainID:   domainID,
		WorkflowID: workflowID,
	}).Return(&persistence.GetCurrentExecutionResponse{
		RunID:            currentRunID,
		State:            persistence.WorkflowStateRunning,
		CloseStatus:      persistence.WorkflowCloseStatusNone,
		LastWriteVersion: currentVersion,
	}, nil)

	msBuilderCurrent.EXPECT().AddWorkflowExecutionTerminatedEvent(
		currentNextEventID, workflowTerminationReason, gomock.Any(), workflowTerminationIdentity,
	).Return(&workflow.HistoryEvent{}, nil).Times(1)
	contextCurrent.EXPECT().updateWorkflowExecutionAsActive(gomock.Any()).Return(nil).Times(1)

	prevRunID, prevLastWriteVersion, prevState, err := s.historyReplicator.conflictResolutionTerminateCurrentRunningIfNotSelf(ctx.Background(), msBuilderTarget, incomingVersion, incomingTimestamp, s.logger)
	s.Nil(err)
	s.Equal(currentRunID, prevRunID)
	s.Equal(currentVersion, prevLastWriteVersion)
	s.Equal(persistence.WorkflowStateCompleted, prevState)
}

func (s *historyReplicatorSuite) TestConflictResolutionTerminateCurrentRunningIfNotSelf_TargetClosed_CurrentRunning_NotLowerVersion() {
	incomingVersion := int64(4096)
	incomingTimestamp := int64(11238)
	incomingCluster := cluster.TestAlternativeClusterName
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(incomingVersion).Return(incomingCluster).AnyTimes()

	domainID := testDomainID
	workflowID := "some random target workflow ID"
	targetRunID := uuid.New()

	msBuilderTarget := NewMockmutableState(s.controller)
	msBuilderTarget.EXPECT().IsWorkflowExecutionRunning().Return(false).AnyTimes()
	msBuilderTarget.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{
		DomainID:           domainID,
		WorkflowID:         workflowID,
		RunID:              targetRunID,
		CloseStatus:        persistence.WorkflowCloseStatusContinuedAsNew,
		DecisionVersion:    common.EmptyVersion,
		DecisionScheduleID: common.EmptyEventID,
		DecisionStartedID:  common.EmptyEventID,
	}).AnyTimes()

	currentRunID := uuid.New()
	s.mockExecutionMgr.On("GetCurrentExecution", &persistence.GetCurrentExecutionRequest{
		DomainID:   domainID,
		WorkflowID: workflowID,
	}).Return(&persistence.GetCurrentExecutionResponse{
		RunID:            currentRunID,
		CloseStatus:      persistence.WorkflowCloseStatusNone,
		LastWriteVersion: incomingVersion,
	}, nil)

	prevRunID, _, _, err := s.historyReplicator.conflictResolutionTerminateCurrentRunningIfNotSelf(ctx.Background(), msBuilderTarget, incomingVersion, incomingTimestamp, s.logger)
	s.Nil(err)
	s.Equal("", prevRunID)
}
