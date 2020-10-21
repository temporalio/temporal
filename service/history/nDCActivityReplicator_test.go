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
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"

	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/persistenceblobs/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/mocks"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives/timestamp"
	serviceerrors "go.temporal.io/server/common/serviceerror"
)

type (
	activityReplicatorSuite struct {
		suite.Suite
		*require.Assertions

		controller               *gomock.Controller
		mockShard                *shardContextTest
		mockTxProcessor          *MocktransferQueueProcessor
		mockReplicationProcessor *MockReplicatorQueueProcessor
		mockTimerProcessor       *MocktimerQueueProcessor
		mockNamespaceCache       *cache.MockNamespaceCache
		mockClusterMetadata      *cluster.MockMetadata
		mockMutableState         *MockmutableState

		mockExecutionMgr *mocks.ExecutionManager

		logger       log.Logger
		historyCache *historyCache

		nDCActivityReplicator nDCActivityReplicator
	}
)

func TestActivityReplicatorSuite(t *testing.T) {
	s := new(activityReplicatorSuite)
	suite.Run(t, s)
}

func (s *activityReplicatorSuite) SetupSuite() {

}

func (s *activityReplicatorSuite) TearDownSuite() {

}

func (s *activityReplicatorSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.mockMutableState = NewMockmutableState(s.controller)
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
				ShardId:          1,
				RangeId:          1,
				TransferAckLevel: 0,
			}},
		NewDynamicConfigForTest(),
	)

	s.mockNamespaceCache = s.mockShard.resource.NamespaceCache
	s.mockExecutionMgr = s.mockShard.resource.ExecutionMgr
	s.mockClusterMetadata = s.mockShard.resource.ClusterMetadata
	s.mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(true).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestAllClusterInfo).AnyTimes()

	s.logger = s.mockShard.GetLogger()

	s.historyCache = newHistoryCache(s.mockShard)
	engine := &historyEngineImpl{
		currentClusterName:   s.mockClusterMetadata.GetCurrentClusterName(),
		shard:                s.mockShard,
		clusterMetadata:      s.mockClusterMetadata,
		executionManager:     s.mockExecutionMgr,
		historyCache:         s.historyCache,
		logger:               s.logger,
		tokenSerializer:      common.NewProtoTaskTokenSerializer(),
		metricsClient:        s.mockShard.GetMetricsClient(),
		timeSource:           s.mockShard.GetTimeSource(),
		historyEventNotifier: newHistoryEventNotifier(clock.NewRealTimeSource(), metrics.NewClient(tally.NoopScope, metrics.History), func(string, string) int32 { return 1 }),
		txProcessor:          s.mockTxProcessor,
		replicatorProcessor:  s.mockReplicationProcessor,
		timerProcessor:       s.mockTimerProcessor,
	}
	s.mockShard.SetEngine(engine)

	s.nDCActivityReplicator = newNDCActivityReplicator(
		s.mockShard,
		s.historyCache,
		s.logger,
	)
}

func (s *activityReplicatorSuite) TearDownTest() {
	s.controller.Finish()
	s.mockShard.Finish(s.T())
}

func (s *activityReplicatorSuite) TestSyncActivity_WorkflowNotFound() {
	namespace := "some random namespace name"
	namespaceID := testNamespaceID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	version := int64(100)

	request := &historyservice.SyncActivityRequest{
		NamespaceId: namespaceID,
		WorkflowId:  workflowID,
		RunId:       runID,
	}
	s.mockExecutionMgr.On("GetWorkflowExecution", &persistence.GetWorkflowExecutionRequest{
		NamespaceID: namespaceID,
		Execution: commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
	}).Return(nil, serviceerror.NewNotFound(""))
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(namespaceID).Return(
		cache.NewGlobalNamespaceCacheEntryForTest(
			&persistenceblobs.NamespaceInfo{Id: namespaceID, Name: namespace},
			&persistenceblobs.NamespaceConfig{Retention: timestamp.DurationFromDays(1)},
			&persistenceblobs.NamespaceReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []string{
					cluster.TestCurrentClusterName,
					cluster.TestAlternativeClusterName,
				},
			},
			version,
			nil,
		), nil,
	).AnyTimes()

	err := s.nDCActivityReplicator.SyncActivity(context.Background(), request)
	s.Nil(err)
}

func (s *activityReplicatorSuite) TestSyncActivity_VersionHistories_WorkflowClosed() {
	namespace := "some random namespace name"
	namespaceID := testNamespaceID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	scheduleID := int64(99)
	version := int64(100)

	lastWriteVersion := version - 100
	incomingVersionHistory := &historyspb.VersionHistory{
		BranchToken: []byte{},
		Items: []*historyspb.VersionHistoryItem{
			{
				EventId: 50,
				Version: 2,
			},
			{
				EventId: scheduleID,
				Version: version,
			},
		},
	}
	key := definition.NewWorkflowIdentifier(namespaceID, workflowID, runID)
	weContext := NewMockworkflowExecutionContext(s.controller)
	weContext.EXPECT().loadWorkflowExecution().Return(s.mockMutableState, nil).Times(1)
	weContext.EXPECT().lock(gomock.Any()).Return(nil)
	weContext.EXPECT().unlock().Times(1)
	weContext.EXPECT().clear().AnyTimes()
	_, err := s.historyCache.PutIfNotExist(key, weContext)
	s.NoError(err)

	request := &historyservice.SyncActivityRequest{
		NamespaceId:    namespaceID,
		WorkflowId:     workflowID,
		RunId:          runID,
		Version:        version,
		ScheduledId:    scheduleID,
		VersionHistory: incomingVersionHistory,
	}
	localVersionHistories := &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{
			{
				BranchToken: []byte{},
				Items: []*historyspb.VersionHistoryItem{
					{
						EventId: scheduleID,
						Version: version,
					},
				},
			},
		},
	}
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistenceblobs.WorkflowExecutionInfo{VersionHistories: localVersionHistories}).AnyTimes()
	s.mockMutableState.EXPECT().GetActivityInfo(scheduleID).Return(nil, false).AnyTimes()
	s.mockMutableState.EXPECT().GetWorkflowStateStatus().Return(enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED, enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(namespaceID).Return(
		cache.NewGlobalNamespaceCacheEntryForTest(
			&persistenceblobs.NamespaceInfo{Id: namespaceID, Name: namespace},
			&persistenceblobs.NamespaceConfig{Retention: timestamp.DurationFromDays(1)},
			&persistenceblobs.NamespaceReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []string{
					cluster.TestCurrentClusterName,
					cluster.TestAlternativeClusterName,
				},
			},
			lastWriteVersion,
			nil,
		), nil,
	).AnyTimes()

	err = s.nDCActivityReplicator.SyncActivity(context.Background(), request)
	s.Nil(err)
}

func (s *activityReplicatorSuite) TestSyncActivity_VersionHistories_IncomingVersionSmaller_DiscardTask() {
	namespace := "some random namespace name"
	namespaceID := testNamespaceID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	scheduleID := int64(144)
	version := int64(99)

	lastWriteVersion := version - 100
	incomingVersionHistory := &historyspb.VersionHistory{
		BranchToken: []byte{},
		Items: []*historyspb.VersionHistoryItem{
			{
				EventId: scheduleID - 1,
				Version: version - 1,
			},
			{
				EventId: scheduleID,
				Version: version,
			},
		},
	}
	key := definition.NewWorkflowIdentifier(namespaceID, workflowID, runID)
	weContext := NewMockworkflowExecutionContext(s.controller)
	weContext.EXPECT().loadWorkflowExecution().Return(s.mockMutableState, nil).Times(1)
	weContext.EXPECT().lock(gomock.Any()).Return(nil)
	weContext.EXPECT().unlock().Times(1)
	weContext.EXPECT().clear().AnyTimes()
	_, err := s.historyCache.PutIfNotExist(key, weContext)
	s.NoError(err)

	request := &historyservice.SyncActivityRequest{
		NamespaceId:    namespaceID,
		WorkflowId:     workflowID,
		RunId:          runID,
		Version:        version,
		ScheduledId:    scheduleID,
		VersionHistory: incomingVersionHistory,
	}
	localVersionHistories := &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{
			{
				BranchToken: []byte{},
				Items: []*historyspb.VersionHistoryItem{
					{
						EventId: scheduleID - 1,
						Version: version - 1,
					},
					{
						EventId: scheduleID + 1,
						Version: version + 1,
					},
				},
			},
		},
	}
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistenceblobs.WorkflowExecutionInfo{VersionHistories: localVersionHistories}).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(namespaceID).Return(
		cache.NewGlobalNamespaceCacheEntryForTest(
			&persistenceblobs.NamespaceInfo{Id: namespaceID, Name: namespace},
			&persistenceblobs.NamespaceConfig{Retention: timestamp.DurationFromDays(1)},
			&persistenceblobs.NamespaceReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []string{
					cluster.TestCurrentClusterName,
					cluster.TestAlternativeClusterName,
				},
			},
			lastWriteVersion,
			nil,
		), nil,
	).AnyTimes()

	err = s.nDCActivityReplicator.SyncActivity(context.Background(), request)
	s.Nil(err)
}

func (s *activityReplicatorSuite) TestSyncActivity_DifferentVersionHistories_IncomingVersionLarger_ReturnRetryError() {
	namespace := "some random namespace name"
	namespaceID := testNamespaceID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	scheduleID := int64(144)
	version := int64(100)
	lastWriteVersion := version - 100

	incomingVersionHistory := &historyspb.VersionHistory{
		BranchToken: []byte{},
		Items: []*historyspb.VersionHistoryItem{
			{
				EventId: 50,
				Version: 2,
			},
			{
				EventId: scheduleID,
				Version: version,
			},
		},
	}
	key := definition.NewWorkflowIdentifier(namespaceID, workflowID, runID)
	weContext := NewMockworkflowExecutionContext(s.controller)
	weContext.EXPECT().loadWorkflowExecution().Return(s.mockMutableState, nil).Times(1)
	weContext.EXPECT().lock(gomock.Any()).Return(nil)
	weContext.EXPECT().unlock().Times(1)
	weContext.EXPECT().clear().AnyTimes()
	_, err := s.historyCache.PutIfNotExist(key, weContext)
	s.NoError(err)

	request := &historyservice.SyncActivityRequest{
		NamespaceId:    namespaceID,
		WorkflowId:     workflowID,
		RunId:          runID,
		Version:        version,
		ScheduledId:    scheduleID,
		VersionHistory: incomingVersionHistory,
	}
	localVersionHistories := &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{
			{
				BranchToken: []byte{},
				Items: []*historyspb.VersionHistoryItem{
					{
						EventId: 100,
						Version: 2,
					},
				},
			},
		},
	}
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistenceblobs.WorkflowExecutionInfo{VersionHistories: localVersionHistories}).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(namespaceID).Return(
		cache.NewGlobalNamespaceCacheEntryForTest(
			&persistenceblobs.NamespaceInfo{Id: namespaceID, Name: namespace},
			&persistenceblobs.NamespaceConfig{Retention: timestamp.DurationFromDays(1)},
			&persistenceblobs.NamespaceReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []string{
					cluster.TestCurrentClusterName,
					cluster.TestAlternativeClusterName,
				},
			},
			lastWriteVersion,
			nil,
		), nil,
	).AnyTimes()

	err = s.nDCActivityReplicator.SyncActivity(context.Background(), request)
	s.Equal(serviceerrors.NewRetryTaskV2(
		resendHigherVersionMessage,
		namespaceID,
		workflowID,
		runID,
		50,
		2,
		common.EmptyEventID,
		common.EmptyVersion,
	),
		err,
	)
}

func (s *activityReplicatorSuite) TestSyncActivity_VersionHistories_IncomingScheduleIDLarger_ReturnRetryError() {
	namespace := "some random namespace name"
	namespaceID := testNamespaceID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	scheduleID := int64(99)
	version := int64(100)

	lastWriteVersion := version - 100
	incomingVersionHistory := &historyspb.VersionHistory{
		BranchToken: []byte{},
		Items: []*historyspb.VersionHistoryItem{
			{
				EventId: 50,
				Version: 2,
			},
			{
				EventId: scheduleID,
				Version: version,
			},
			{
				EventId: scheduleID + 100,
				Version: version + 100,
			},
		},
	}
	key := definition.NewWorkflowIdentifier(namespaceID, workflowID, runID)
	weContext := NewMockworkflowExecutionContext(s.controller)
	weContext.EXPECT().loadWorkflowExecution().Return(s.mockMutableState, nil).Times(1)
	weContext.EXPECT().lock(gomock.Any()).Return(nil)
	weContext.EXPECT().unlock().Times(1)
	weContext.EXPECT().clear().AnyTimes()
	_, err := s.historyCache.PutIfNotExist(key, weContext)
	s.NoError(err)

	request := &historyservice.SyncActivityRequest{
		NamespaceId:    namespaceID,
		WorkflowId:     workflowID,
		RunId:          runID,
		Version:        version,
		ScheduledId:    scheduleID,
		VersionHistory: incomingVersionHistory,
	}
	localVersionHistories := &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{
			{
				BranchToken: []byte{},
				Items: []*historyspb.VersionHistoryItem{
					{
						EventId: scheduleID - 10,
						Version: version,
					},
				},
			},
		},
	}
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistenceblobs.WorkflowExecutionInfo{VersionHistories: localVersionHistories}).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(namespaceID).Return(
		cache.NewGlobalNamespaceCacheEntryForTest(
			&persistenceblobs.NamespaceInfo{Id: namespaceID, Name: namespace},
			&persistenceblobs.NamespaceConfig{Retention: timestamp.DurationFromDays(1)},
			&persistenceblobs.NamespaceReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []string{
					cluster.TestCurrentClusterName,
					cluster.TestAlternativeClusterName,
				},
			},
			lastWriteVersion,
			nil,
		), nil,
	).AnyTimes()

	err = s.nDCActivityReplicator.SyncActivity(context.Background(), request)
	s.Equal(serviceerrors.NewRetryTaskV2(
		resendMissingEventMessage,
		namespaceID,
		workflowID,
		runID,
		scheduleID-10,
		version,
		common.EmptyEventID,
		common.EmptyVersion,
	),
		err,
	)
}

func (s *activityReplicatorSuite) TestSyncActivity_VersionHistories_SameScheduleID() {
	namespace := "some random namespace name"
	namespaceID := testNamespaceID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	scheduleID := int64(99)
	version := int64(100)

	lastWriteVersion := version - 100
	incomingVersionHistory := &historyspb.VersionHistory{
		BranchToken: []byte{},
		Items: []*historyspb.VersionHistoryItem{
			{
				EventId: 50,
				Version: 2,
			},
			{
				EventId: scheduleID,
				Version: version,
			},
		},
	}
	key := definition.NewWorkflowIdentifier(namespaceID, workflowID, runID)
	weContext := NewMockworkflowExecutionContext(s.controller)
	weContext.EXPECT().loadWorkflowExecution().Return(s.mockMutableState, nil).Times(1)
	weContext.EXPECT().lock(gomock.Any()).Return(nil)
	weContext.EXPECT().unlock().Times(1)
	weContext.EXPECT().clear().AnyTimes()
	_, err := s.historyCache.PutIfNotExist(key, weContext)
	s.NoError(err)

	request := &historyservice.SyncActivityRequest{
		NamespaceId:    namespaceID,
		WorkflowId:     workflowID,
		RunId:          runID,
		Version:        version,
		ScheduledId:    scheduleID,
		VersionHistory: incomingVersionHistory,
	}
	localVersionHistories := &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{
			{
				BranchToken: []byte{},
				Items: []*historyspb.VersionHistoryItem{
					{
						EventId: scheduleID,
						Version: version,
					},
				},
			},
		},
	}
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistenceblobs.WorkflowExecutionInfo{VersionHistories: localVersionHistories}).AnyTimes()
	s.mockMutableState.EXPECT().GetActivityInfo(scheduleID).Return(nil, false).AnyTimes()
	s.mockMutableState.EXPECT().GetWorkflowStateStatus().Return(enumsspb.WORKFLOW_EXECUTION_STATE_CREATED, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(namespaceID).Return(
		cache.NewGlobalNamespaceCacheEntryForTest(
			&persistenceblobs.NamespaceInfo{Id: namespaceID, Name: namespace},
			&persistenceblobs.NamespaceConfig{Retention: timestamp.DurationFromDays(1)},
			&persistenceblobs.NamespaceReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []string{
					cluster.TestCurrentClusterName,
					cluster.TestAlternativeClusterName,
				},
			},
			lastWriteVersion,
			nil,
		), nil,
	).AnyTimes()

	err = s.nDCActivityReplicator.SyncActivity(context.Background(), request)
	s.Nil(err)
}

func (s *activityReplicatorSuite) TestSyncActivity_VersionHistories_LocalVersionHistoryWin() {
	namespace := "some random namespace name"
	namespaceID := testNamespaceID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	scheduleID := int64(99)
	version := int64(100)

	lastWriteVersion := version - 100
	incomingVersionHistory := &historyspb.VersionHistory{
		BranchToken: []byte{},
		Items: []*historyspb.VersionHistoryItem{
			{
				EventId: scheduleID,
				Version: version,
			},
		},
	}
	key := definition.NewWorkflowIdentifier(namespaceID, workflowID, runID)
	weContext := NewMockworkflowExecutionContext(s.controller)
	weContext.EXPECT().loadWorkflowExecution().Return(s.mockMutableState, nil).Times(1)
	weContext.EXPECT().lock(gomock.Any()).Return(nil)
	weContext.EXPECT().unlock().Times(1)
	weContext.EXPECT().clear().AnyTimes()
	_, err := s.historyCache.PutIfNotExist(key, weContext)
	s.NoError(err)

	request := &historyservice.SyncActivityRequest{
		NamespaceId:    namespaceID,
		WorkflowId:     workflowID,
		RunId:          runID,
		Version:        version,
		ScheduledId:    scheduleID,
		VersionHistory: incomingVersionHistory,
	}
	localVersionHistories := &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{
			{
				BranchToken: []byte{},
				Items: []*historyspb.VersionHistoryItem{
					{
						EventId: scheduleID,
						Version: version,
					},
					{
						EventId: scheduleID + 1,
						Version: version + 1,
					},
				},
			},
		},
	}
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistenceblobs.WorkflowExecutionInfo{VersionHistories: localVersionHistories}).AnyTimes()
	s.mockMutableState.EXPECT().GetActivityInfo(scheduleID).Return(nil, false).AnyTimes()
	s.mockMutableState.EXPECT().GetWorkflowStateStatus().
		Return(enumsspb.WORKFLOW_EXECUTION_STATE_CREATED, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(namespaceID).Return(
		cache.NewGlobalNamespaceCacheEntryForTest(
			&persistenceblobs.NamespaceInfo{Id: namespaceID, Name: namespace},
			&persistenceblobs.NamespaceConfig{Retention: timestamp.DurationFromDays(1)},
			&persistenceblobs.NamespaceReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []string{
					cluster.TestCurrentClusterName,
					cluster.TestAlternativeClusterName,
				},
			},
			lastWriteVersion,
			nil,
		), nil,
	).AnyTimes()

	err = s.nDCActivityReplicator.SyncActivity(context.Background(), request)
	s.Nil(err)
}

func (s *activityReplicatorSuite) generateIncomingVersionHistory(eventID, version int64) historyspb.VersionHistory {
	incomingVersionHistory := historyspb.VersionHistory{
		BranchToken: []byte{},
		Items: []*historyspb.VersionHistoryItem{
			{
				EventId: eventID,
				Version: version,
			},
		},
	}

	return incomingVersionHistory
}

func (s *activityReplicatorSuite) generateLocalVersionHistory(eventID, version int64) *historyspb.VersionHistories {
	localVersionHistories := &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{
			{
				BranchToken: []byte{},
				Items: []*historyspb.VersionHistoryItem{
					{
						EventId: eventID,
						Version: version,
					},
				},
			},
		},
	}

	return localVersionHistories
}
