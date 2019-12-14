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
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"

	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/clock"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/mocks"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/service/dynamicconfig"
)

type (
	conflictResolverSuite struct {
		suite.Suite
		*require.Assertions

		controller               *gomock.Controller
		mockShard                *shardContextTest
		mockTxProcessor          *MocktransferQueueProcessor
		mockReplicationProcessor *MockReplicatorQueueProcessor
		mockTimerProcessor       *MocktimerQueueProcessor
		mockEventsCache          *MockeventsCache
		mockDomainCache          *cache.MockDomainCache
		mockClusterMetadata      *cluster.MockMetadata

		logger           log.Logger
		mockExecutionMgr *mocks.ExecutionManager
		mockHistoryV2Mgr *mocks.HistoryV2Manager
		mockContext      *workflowExecutionContextImpl

		conflictResolver *conflictResolverImpl
	}
)

func TestConflictResolverSuite(t *testing.T) {
	s := new(conflictResolverSuite)
	suite.Run(t, s)
}

func (s *conflictResolverSuite) SetupSuite() {
}

func (s *conflictResolverSuite) TearDownSuite() {

}

func (s *conflictResolverSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.mockTxProcessor = NewMocktransferQueueProcessor(s.controller)
	s.mockReplicationProcessor = NewMockReplicatorQueueProcessor(s.controller)
	s.mockTimerProcessor = NewMocktimerQueueProcessor(s.controller)
	s.mockTxProcessor.EXPECT().NotifyNewTask(gomock.Any(), gomock.Any()).AnyTimes()
	s.mockReplicationProcessor.EXPECT().notifyNewTask().AnyTimes()
	s.mockTimerProcessor.EXPECT().NotifyNewTimers(gomock.Any(), gomock.Any()).AnyTimes()

	s.mockShard = newTestShardContext(
		s.controller,
		&persistence.ShardInfo{
			ShardID:          10,
			RangeID:          1,
			TransferAckLevel: 0,
		},
		NewDynamicConfigForTest(),
	)

	s.mockDomainCache = s.mockShard.resource.DomainCache
	s.mockHistoryV2Mgr = s.mockShard.resource.HistoryMgr
	s.mockExecutionMgr = s.mockShard.resource.ExecutionMgr
	s.mockClusterMetadata = s.mockShard.resource.ClusterMetadata
	s.mockEventsCache = s.mockShard.mockEventsCache
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockEventsCache.EXPECT().putEvent(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()

	s.logger = s.mockShard.GetLogger()

	h := &historyEngineImpl{
		shard:                s.mockShard,
		clusterMetadata:      s.mockClusterMetadata,
		historyEventNotifier: newHistoryEventNotifier(clock.NewRealTimeSource(), metrics.NewClient(tally.NoopScope, metrics.History), func(string) int { return 0 }),
		txProcessor:          s.mockTxProcessor,
		replicatorProcessor:  s.mockReplicationProcessor,
		timerProcessor:       s.mockTimerProcessor,
	}
	s.mockShard.SetEngine(h)

	s.mockContext = newWorkflowExecutionContext(testDomainID, shared.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(testRunID),
	}, s.mockShard, s.mockExecutionMgr, s.logger)
	s.conflictResolver = newConflictResolver(s.mockShard, s.mockContext, s.mockHistoryV2Mgr, s.logger)

}

func (s *conflictResolverSuite) TearDownTest() {
	s.controller.Finish()
	s.mockShard.Finish(s.T())
}

func (s *conflictResolverSuite) TestReset() {
	s.mockShard.config.AdvancedVisibilityWritingMode = dynamicconfig.GetStringPropertyFn(common.AdvancedVisibilityWritingModeDual)

	prevRunID := uuid.New()
	prevLastWriteVersion := int64(123)
	prevState := persistence.WorkflowStateRunning

	sourceCluster := cluster.TestAlternativeClusterName
	startTime := time.Now()
	version := int64(12)

	domainID := s.mockContext.domainID
	execution := s.mockContext.workflowExecution
	nextEventID := int64(2)
	branchToken := []byte("some random branch token")

	event1 := &shared.HistoryEvent{
		EventId: common.Int64Ptr(1),
		Version: common.Int64Ptr(version),
		WorkflowExecutionStartedEventAttributes: &shared.WorkflowExecutionStartedEventAttributes{
			WorkflowType:                        &shared.WorkflowType{Name: common.StringPtr("some random workflow type")},
			TaskList:                            &shared.TaskList{Name: common.StringPtr("some random workflow type")},
			Input:                               []byte("some random input"),
			ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(123),
			TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(233),
			Identity:                            common.StringPtr("some random identity"),
		},
	}
	event2 := &shared.HistoryEvent{
		EventId:                              common.Int64Ptr(2),
		DecisionTaskScheduledEventAttributes: &shared.DecisionTaskScheduledEventAttributes{},
	}

	historySize := int64(1234567)
	s.mockHistoryV2Mgr.On("ReadHistoryBranch", &persistence.ReadHistoryBranchRequest{
		BranchToken:   branchToken,
		MinEventID:    common.FirstEventID,
		MaxEventID:    nextEventID,
		PageSize:      defaultHistoryPageSize,
		NextPageToken: nil,
		ShardID:       common.IntPtr(s.mockShard.GetShardID()),
	}).Return(&persistence.ReadHistoryBranchResponse{
		HistoryEvents:    []*shared.HistoryEvent{event1, event2},
		NextPageToken:    nil,
		LastFirstEventID: event1.GetEventId(),
		Size:             int(historySize),
	}, nil)

	s.mockContext.updateCondition = int64(59)
	createRequestID := uuid.New()

	executionInfo := &persistence.WorkflowExecutionInfo{
		DomainID:                    domainID,
		WorkflowID:                  execution.GetWorkflowId(),
		RunID:                       execution.GetRunId(),
		ParentDomainID:              "",
		ParentWorkflowID:            "",
		ParentRunID:                 "",
		InitiatedID:                 common.EmptyEventID,
		TaskList:                    event1.WorkflowExecutionStartedEventAttributes.TaskList.GetName(),
		WorkflowTypeName:            event1.WorkflowExecutionStartedEventAttributes.WorkflowType.GetName(),
		WorkflowTimeout:             *event1.WorkflowExecutionStartedEventAttributes.ExecutionStartToCloseTimeoutSeconds,
		DecisionStartToCloseTimeout: *event1.WorkflowExecutionStartedEventAttributes.TaskStartToCloseTimeoutSeconds,
		State:                       persistence.WorkflowStateCreated,
		CloseStatus:                 persistence.WorkflowCloseStatusNone,
		LastFirstEventID:            event1.GetEventId(),
		NextEventID:                 nextEventID,
		LastProcessedEvent:          common.EmptyEventID,
		StartTimestamp:              startTime,
		LastUpdatedTimestamp:        startTime,
		DecisionVersion:             common.EmptyVersion,
		DecisionScheduleID:          common.EmptyEventID,
		DecisionStartedID:           common.EmptyEventID,
		DecisionRequestID:           emptyUUID,
		DecisionTimeout:             0,
		DecisionAttempt:             0,
		DecisionStartedTimestamp:    0,
		CreateRequestID:             createRequestID,
		BranchToken:                 branchToken,
	}
	// this is only a shallow test, meaning
	// the mutable state only has the minimal information
	// so we can test the conflict resolver
	s.mockExecutionMgr.On("ConflictResolveWorkflowExecution", mock.MatchedBy(func(input *persistence.ConflictResolveWorkflowExecutionRequest) bool {
		transferTasks := input.ResetWorkflowSnapshot.TransferTasks
		if len(transferTasks) != 1 {
			return false
		}
		s.IsType(&persistence.UpsertWorkflowSearchAttributesTask{}, transferTasks[0])
		input.ResetWorkflowSnapshot.TransferTasks = nil

		s.Equal(&persistence.ConflictResolveWorkflowExecutionRequest{
			RangeID: s.mockShard.shardInfo.RangeID,
			CurrentWorkflowCAS: &persistence.CurrentWorkflowCAS{
				PrevRunID:            prevRunID,
				PrevLastWriteVersion: prevLastWriteVersion,
				PrevState:            prevState,
			},
			ResetWorkflowSnapshot: persistence.WorkflowSnapshot{
				ExecutionInfo: executionInfo,
				ExecutionStats: &persistence.ExecutionStats{
					HistorySize: historySize,
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
				ActivityInfos:       []*persistence.ActivityInfo{},
				TimerInfos:          []*persistence.TimerInfo{},
				ChildExecutionInfos: []*persistence.ChildExecutionInfo{},
				RequestCancelInfos:  []*persistence.RequestCancelInfo{},
				SignalInfos:         []*persistence.SignalInfo{},
				SignalRequestedIDs:  []string{},
				TransferTasks:       nil,
				ReplicationTasks:    nil,
				TimerTasks:          nil,
				Condition:           s.mockContext.updateCondition,
			},
			Encoding: common.EncodingType(s.mockShard.GetConfig().EventEncodingType(domainID)),
		}, input)
		return true
	})).Return(nil).Once()
	s.mockExecutionMgr.On("GetWorkflowExecution", &persistence.GetWorkflowExecutionRequest{
		DomainID:  domainID,
		Execution: execution,
	}).Return(&persistence.GetWorkflowExecutionResponse{
		State: &persistence.WorkflowMutableState{
			ExecutionInfo:  &persistence.WorkflowExecutionInfo{},
			ExecutionStats: &persistence.ExecutionStats{},
		},
	}, nil).Once() // return empty resoonse since we are not testing the load
	s.mockClusterMetadata.EXPECT().IsGlobalDomainEnabled().Return(true).AnyTimes()
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(event1.GetVersion()).Return(sourceCluster).AnyTimes()
	s.mockDomainCache.EXPECT().GetDomainByID(gomock.Any()).Return(cache.NewLocalDomainCacheEntryForTest(
		&persistence.DomainInfo{ID: domainID}, &persistence.DomainConfig{}, "", nil,
	), nil).AnyTimes()

	_, err := s.conflictResolver.reset(prevRunID, prevLastWriteVersion, prevState, createRequestID, nextEventID-1, executionInfo, s.mockContext.updateCondition)
	s.Nil(err)
}
