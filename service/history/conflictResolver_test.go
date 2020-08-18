// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
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
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/persistenceblobs/v1"
	"go.temporal.io/server/common/primitives/timestamp"

	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/mocks"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/service/dynamicconfig"
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
		mockNamespaceCache       *cache.MockNamespaceCache
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
		&persistence.ShardInfoWithFailover{
			ShardInfo: &persistenceblobs.ShardInfo{
				ShardId:          10,
				RangeId:          1,
				TransferAckLevel: 0,
			}},
		NewDynamicConfigForTest(),
	)

	s.mockNamespaceCache = s.mockShard.resource.NamespaceCache
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
		historyEventNotifier: newHistoryEventNotifier(clock.NewRealTimeSource(), metrics.NewClient(tally.NoopScope, metrics.History), func(string, string) int { return 1 }),
		txProcessor:          s.mockTxProcessor,
		replicatorProcessor:  s.mockReplicationProcessor,
		timerProcessor:       s.mockTimerProcessor,
	}
	s.mockShard.SetEngine(h)

	s.mockContext = newWorkflowExecutionContext(testNamespaceID, commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      testRunID,
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
	prevState := enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING

	sourceCluster := cluster.TestAlternativeClusterName
	startTime := time.Now().UTC()
	version := int64(12)

	namespaceID := s.mockContext.namespaceID
	execution := s.mockContext.workflowExecution
	nextEventID := int64(2)
	branchToken := []byte("some random branch token")

	event1 := &historypb.HistoryEvent{
		EventId:   1,
		Version:   version,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{
			WorkflowType:             &commonpb.WorkflowType{Name: "some random workflow type"},
			TaskQueue:                &taskqueuepb.TaskQueue{Name: "some random workflow type"},
			Input:                    payloads.EncodeString("some random input"),
			WorkflowExecutionTimeout: timestamp.DurationPtr(123 * time.Second),
			WorkflowRunTimeout:       timestamp.DurationPtr(231 * time.Second),
			WorkflowTaskTimeout:      timestamp.DurationPtr(233 * time.Second),
			Identity:                 "some random identity",
		}},
	}
	event2 := &historypb.HistoryEvent{
		EventId:    2,
		EventType:  enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		Attributes: &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{}}}

	historySize := int64(1234567)
	shardId := s.mockShard.GetShardID()
	s.mockHistoryV2Mgr.On("ReadHistoryBranch", &persistence.ReadHistoryBranchRequest{
		BranchToken:   branchToken,
		MinEventID:    common.FirstEventID,
		MaxEventID:    nextEventID,
		PageSize:      defaultHistoryPageSize,
		NextPageToken: nil,
		ShardID:       &shardId,
	}).Return(&persistence.ReadHistoryBranchResponse{
		HistoryEvents:    []*historypb.HistoryEvent{event1, event2},
		NextPageToken:    nil,
		LastFirstEventID: event1.GetEventId(),
		Size:             int(historySize),
	}, nil)

	s.mockContext.updateCondition = int64(59)
	createRequestID := uuid.New()

	executionInfo := &persistence.WorkflowExecutionInfo{
		NamespaceID:                  namespaceID,
		WorkflowID:                   execution.GetWorkflowId(),
		RunID:                        execution.GetRunId(),
		ParentNamespaceID:            "",
		ParentWorkflowID:             "",
		ParentRunID:                  "",
		InitiatedID:                  common.EmptyEventID,
		TaskQueue:                    event1.GetWorkflowExecutionStartedEventAttributes().TaskQueue.GetName(),
		WorkflowTypeName:             event1.GetWorkflowExecutionStartedEventAttributes().WorkflowType.GetName(),
		WorkflowExecutionTimeout:     int64(timestamp.DurationValue(event1.GetWorkflowExecutionStartedEventAttributes().GetWorkflowExecutionTimeout()).Seconds()),
		WorkflowRunTimeout:           int64(timestamp.DurationValue(event1.GetWorkflowExecutionStartedEventAttributes().GetWorkflowRunTimeout()).Seconds()),
		DefaultWorkflowTaskTimeout:   int64(timestamp.DurationValue(event1.GetWorkflowExecutionStartedEventAttributes().GetWorkflowTaskTimeout()).Seconds()),
		State:                        enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		Status:                       enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		LastFirstEventID:             event1.GetEventId(),
		NextEventID:                  nextEventID,
		LastProcessedEvent:           common.EmptyEventID,
		StartTimestamp:               startTime,
		LastUpdatedTimestamp:         startTime,
		WorkflowTaskVersion:          common.EmptyVersion,
		WorkflowTaskScheduleID:       common.EmptyEventID,
		WorkflowTaskStartedID:        common.EmptyEventID,
		WorkflowTaskRequestID:        emptyUUID,
		WorkflowTaskTimeout:          0,
		WorkflowTaskAttempt:          1,
		WorkflowTaskStartedTimestamp: 0,
		CreateRequestID:              createRequestID,
		BranchToken:                  branchToken,
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

		s.Equal(executionInfo, input.ResetWorkflowSnapshot.ExecutionInfo)

		s.Equal(&persistence.ConflictResolveWorkflowExecutionRequest{
			RangeID: s.mockShard.shardInfo.GetRangeId(),
			CurrentWorkflowCAS: &persistence.CurrentWorkflowCAS{
				PrevRunID:            prevRunID,
				PrevLastWriteVersion: prevLastWriteVersion,
				PrevState:            prevState,
			},
			ResetWorkflowSnapshot: persistence.WorkflowSnapshot{
				ExecutionInfo: executionInfo,
				ExecutionStats: &persistenceblobs.ExecutionStats{
					HistorySize: historySize,
				},
				ReplicationState: &persistenceblobs.ReplicationState{
					CurrentVersion:   event1.GetVersion(),
					StartVersion:     event1.GetVersion(),
					LastWriteVersion: event1.GetVersion(),
					LastWriteEventId: event1.GetEventId(),
					LastReplicationInfo: map[string]*replicationspb.ReplicationInfo{
						sourceCluster: {
							Version:     event1.GetVersion(),
							LastEventId: event1.GetEventId(),
						},
					},
				},
				ActivityInfos:       []*persistenceblobs.ActivityInfo{},
				TimerInfos:          []*persistenceblobs.TimerInfo{},
				ChildExecutionInfos: []*persistenceblobs.ChildExecutionInfo{},
				RequestCancelInfos:  []*persistenceblobs.RequestCancelInfo{},
				SignalInfos:         []*persistenceblobs.SignalInfo{},
				SignalRequestedIDs:  []string{},
				TransferTasks:       nil,
				ReplicationTasks:    nil,
				TimerTasks:          nil,
				Condition:           s.mockContext.updateCondition,
			},
			Encoding: enumspb.EncodingType(enumspb.EncodingType_value[s.mockShard.GetConfig().EventEncodingType(namespaceID)]),
		}, input)
		return true
	})).Return(nil).Once()
	s.mockExecutionMgr.On("GetWorkflowExecution", &persistence.GetWorkflowExecutionRequest{
		NamespaceID: namespaceID,
		Execution:   execution,
	}).Return(&persistence.GetWorkflowExecutionResponse{
		State: &persistence.WorkflowMutableState{
			ExecutionInfo: &persistence.WorkflowExecutionInfo{
				State:  enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
				Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			},
			ExecutionStats: &persistenceblobs.ExecutionStats{},
		},
	}, nil).Once() // return empty resoonse since we are not testing the load
	s.mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(true).AnyTimes()
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(event1.GetVersion()).Return(sourceCluster).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(gomock.Any()).Return(cache.NewLocalNamespaceCacheEntryForTest(
		&persistenceblobs.NamespaceInfo{Id: namespaceID}, &persistenceblobs.NamespaceConfig{}, "", nil,
	), nil).AnyTimes()

	_, err := s.conflictResolver.reset(prevRunID, prevLastWriteVersion, prevState, createRequestID, nextEventID-1, executionInfo, s.mockContext.updateCondition)
	s.Nil(err)
}
