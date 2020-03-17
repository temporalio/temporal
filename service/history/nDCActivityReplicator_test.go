// Copyright (c) 2019 Uber Technologies, Inc.
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
	"errors"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	commonproto "go.temporal.io/temporal-proto/common"
	"go.temporal.io/temporal-proto/serviceerror"

	"github.com/temporalio/temporal/.gen/proto/historyservice"
	"github.com/temporalio/temporal/.gen/proto/persistenceblobs"
	"github.com/temporalio/temporal/common"
	"github.com/temporalio/temporal/common/cache"
	"github.com/temporalio/temporal/common/clock"
	"github.com/temporalio/temporal/common/cluster"
	"github.com/temporalio/temporal/common/definition"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/metrics"
	"github.com/temporalio/temporal/common/mocks"
	"github.com/temporalio/temporal/common/persistence"
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
		mockDomainCache          *cache.MockDomainCache
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
				ShardID:          0,
				RangeID:          1,
				TransferAckLevel: 0,
			}},
		NewDynamicConfigForTest(),
	)

	s.mockDomainCache = s.mockShard.resource.DomainCache
	s.mockExecutionMgr = s.mockShard.resource.ExecutionMgr
	s.mockClusterMetadata = s.mockShard.resource.ClusterMetadata
	s.mockClusterMetadata.EXPECT().IsGlobalDomainEnabled().Return(true).AnyTimes()
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
		historyEventNotifier: newHistoryEventNotifier(clock.NewRealTimeSource(), metrics.NewClient(tally.NoopScope, metrics.History), func(string) int { return 0 }),
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
	domainName := "some random domain name"
	domainID := testDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	version := int64(100)

	request := &historyservice.SyncActivityRequest{
		DomainId:   domainID,
		WorkflowId: workflowID,
		RunId:      runID,
	}
	s.mockExecutionMgr.On("GetWorkflowExecution", &persistence.GetWorkflowExecutionRequest{
		DomainID: domainID,
		Execution: commonproto.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
	}).Return(nil, serviceerror.NewNotFound(""))
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
			version,
			nil,
		), nil,
	).AnyTimes()

	err := s.nDCActivityReplicator.SyncActivity(context.Background(), request)
	s.Nil(err)
}

func (s *activityReplicatorSuite) TestSyncActivity_WorkflowClosed() {
	domainName := "some random domain name"
	domainID := testDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	version := int64(100)

	key := definition.NewWorkflowIdentifier(domainID, workflowID, runID)
	weContext := NewMockworkflowExecutionContext(s.controller)
	weContext.EXPECT().loadWorkflowExecution().Return(s.mockMutableState, nil).Times(1)
	weContext.EXPECT().lock(gomock.Any()).Return(nil)
	weContext.EXPECT().unlock().Times(1)
	weContext.EXPECT().clear().AnyTimes()
	_, err := s.historyCache.PutIfNotExist(key, weContext)
	s.NoError(err)
	request := &historyservice.SyncActivityRequest{
		DomainId:   domainID,
		WorkflowId: workflowID,
		RunId:      runID,
	}
	s.mockMutableState.EXPECT().IsWorkflowExecutionRunning().Return(false).AnyTimes()
	var versionHistories *persistence.VersionHistories
	s.mockMutableState.EXPECT().GetVersionHistories().Return(versionHistories).AnyTimes()
	s.mockMutableState.EXPECT().GetReplicationState().Return(&persistence.ReplicationState{}).AnyTimes()
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
			version,
			nil,
		), nil,
	).AnyTimes()

	err = s.nDCActivityReplicator.SyncActivity(context.Background(), request)
	s.Nil(err)
}

func (s *activityReplicatorSuite) TestSyncActivity_IncomingScheduleIDLarger_IncomingVersionSmaller() {
	domainName := "some random domain name"
	domainID := testDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	scheduleID := int64(144)
	version := int64(100)
	lastWriteVersion := version + 100
	nextEventID := scheduleID - 10

	key := definition.NewWorkflowIdentifier(domainID, workflowID, runID)
	weContext := NewMockworkflowExecutionContext(s.controller)
	weContext.EXPECT().loadWorkflowExecution().Return(s.mockMutableState, nil).Times(1)
	weContext.EXPECT().lock(gomock.Any()).Return(nil)
	weContext.EXPECT().unlock().Times(1)
	weContext.EXPECT().clear().AnyTimes()
	_, err := s.historyCache.PutIfNotExist(key, weContext)
	s.NoError(err)

	request := &historyservice.SyncActivityRequest{
		DomainId:    domainID,
		WorkflowId:  workflowID,
		RunId:       runID,
		Version:     version,
		ScheduledId: scheduleID,
	}
	s.mockMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes()
	s.mockMutableState.EXPECT().GetNextEventID().Return(nextEventID).AnyTimes()
	s.mockMutableState.EXPECT().GetLastWriteVersion().Return(lastWriteVersion, nil)
	var versionHistories *persistence.VersionHistories
	s.mockMutableState.EXPECT().GetVersionHistories().Return(versionHistories).AnyTimes()
	s.mockMutableState.EXPECT().GetReplicationState().Return(&persistence.ReplicationState{}).AnyTimes()
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
			lastWriteVersion,
			nil,
		), nil,
	).AnyTimes()

	err = s.nDCActivityReplicator.SyncActivity(context.Background(), request)
	s.Nil(err)
}

func (s *activityReplicatorSuite) TestSyncActivity_IncomingScheduleIDLarger_IncomingVersionLarger() {
	domainName := "some random domain name"
	domainID := testDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	scheduleID := int64(144)
	version := int64(100)
	lastWriteVersion := version - 100
	nextEventID := scheduleID - 10

	key := definition.NewWorkflowIdentifier(domainID, workflowID, runID)
	weContext := NewMockworkflowExecutionContext(s.controller)
	weContext.EXPECT().loadWorkflowExecution().Return(s.mockMutableState, nil).Times(1)
	weContext.EXPECT().lock(gomock.Any()).Return(nil)
	weContext.EXPECT().unlock().Times(1)
	weContext.EXPECT().clear().AnyTimes()
	_, err := s.historyCache.PutIfNotExist(key, weContext)
	s.NoError(err)

	request := &historyservice.SyncActivityRequest{
		DomainId:    domainID,
		WorkflowId:  workflowID,
		RunId:       runID,
		Version:     version,
		ScheduledId: scheduleID,
	}
	s.mockMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes()
	s.mockMutableState.EXPECT().GetNextEventID().Return(nextEventID).AnyTimes()
	s.mockMutableState.EXPECT().GetLastWriteVersion().Return(lastWriteVersion, nil).AnyTimes()
	var versionHistories *persistence.VersionHistories
	s.mockMutableState.EXPECT().GetVersionHistories().Return(versionHistories).AnyTimes()
	s.mockMutableState.EXPECT().GetReplicationState().Return(&persistence.ReplicationState{}).AnyTimes()
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
			lastWriteVersion,
			nil,
		), nil,
	).AnyTimes()

	err = s.nDCActivityReplicator.SyncActivity(context.Background(), request)
	s.Equal(newRetryTaskErrorWithHint(ErrRetrySyncActivityMsg, domainID, workflowID, runID, nextEventID), err)
}

func (s *activityReplicatorSuite) TestSyncActivity_VersionHistories_IncomingVersionSmaller_DiscardTask() {
	domainName := "some random domain name"
	domainID := testDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	scheduleID := int64(144)
	version := int64(99)

	lastWriteVersion := version - 100
	incomingVersionHistory := persistence.VersionHistory{
		BranchToken: []byte{},
		Items: []*persistence.VersionHistoryItem{
			{
				EventID: scheduleID - 1,
				Version: version - 1,
			},
			{
				EventID: scheduleID,
				Version: version,
			},
		},
	}
	key := definition.NewWorkflowIdentifier(domainID, workflowID, runID)
	weContext := NewMockworkflowExecutionContext(s.controller)
	weContext.EXPECT().loadWorkflowExecution().Return(s.mockMutableState, nil).Times(1)
	weContext.EXPECT().lock(gomock.Any()).Return(nil)
	weContext.EXPECT().unlock().Times(1)
	weContext.EXPECT().clear().AnyTimes()
	_, err := s.historyCache.PutIfNotExist(key, weContext)
	s.NoError(err)

	request := &historyservice.SyncActivityRequest{
		DomainId:       domainID,
		WorkflowId:     workflowID,
		RunId:          runID,
		Version:        version,
		ScheduledId:    scheduleID,
		VersionHistory: incomingVersionHistory.ToProto(),
	}
	localVersionHistories := &persistence.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*persistence.VersionHistory{
			{
				BranchToken: []byte{},
				Items: []*persistence.VersionHistoryItem{
					{
						EventID: scheduleID - 1,
						Version: version - 1,
					},
					{
						EventID: scheduleID + 1,
						Version: version + 1,
					},
				},
			},
		},
	}
	s.mockMutableState.EXPECT().GetVersionHistories().Return(localVersionHistories).AnyTimes()
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
			lastWriteVersion,
			nil,
		), nil,
	).AnyTimes()

	err = s.nDCActivityReplicator.SyncActivity(context.Background(), request)
	s.Nil(err)
}

func (s *activityReplicatorSuite) TestSyncActivity_DifferentVersionHistories_IncomingVersionLarger_ReturnRetryError() {
	domainName := "some random domain name"
	domainID := testDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	scheduleID := int64(144)
	version := int64(100)
	lastWriteVersion := version - 100

	incomingVersionHistory := persistence.VersionHistory{
		BranchToken: []byte{},
		Items: []*persistence.VersionHistoryItem{
			{
				EventID: 50,
				Version: 2,
			},
			{
				EventID: scheduleID,
				Version: version,
			},
		},
	}
	key := definition.NewWorkflowIdentifier(domainID, workflowID, runID)
	weContext := NewMockworkflowExecutionContext(s.controller)
	weContext.EXPECT().loadWorkflowExecution().Return(s.mockMutableState, nil).Times(1)
	weContext.EXPECT().lock(gomock.Any()).Return(nil)
	weContext.EXPECT().unlock().Times(1)
	weContext.EXPECT().clear().AnyTimes()
	_, err := s.historyCache.PutIfNotExist(key, weContext)
	s.NoError(err)

	request := &historyservice.SyncActivityRequest{
		DomainId:       domainID,
		WorkflowId:     workflowID,
		RunId:          runID,
		Version:        version,
		ScheduledId:    scheduleID,
		VersionHistory: incomingVersionHistory.ToProto(),
	}
	localVersionHistories := &persistence.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*persistence.VersionHistory{
			{
				BranchToken: []byte{},
				Items: []*persistence.VersionHistoryItem{
					{
						EventID: 100,
						Version: 2,
					},
				},
			},
		},
	}
	s.mockMutableState.EXPECT().GetVersionHistories().Return(localVersionHistories).AnyTimes()
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
			lastWriteVersion,
			nil,
		), nil,
	).AnyTimes()

	err = s.nDCActivityReplicator.SyncActivity(context.Background(), request)
	s.Equal(newNDCRetryTaskErrorWithHint(
		resendHigherVersionMessage,
		domainID,
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
	domainName := "some random domain name"
	domainID := testDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	scheduleID := int64(99)
	version := int64(100)

	lastWriteVersion := version - 100
	incomingVersionHistory := persistence.VersionHistory{
		BranchToken: []byte{},
		Items: []*persistence.VersionHistoryItem{
			{
				EventID: 50,
				Version: 2,
			},
			{
				EventID: scheduleID,
				Version: version,
			},
			{
				EventID: scheduleID + 100,
				Version: version + 100,
			},
		},
	}
	key := definition.NewWorkflowIdentifier(domainID, workflowID, runID)
	weContext := NewMockworkflowExecutionContext(s.controller)
	weContext.EXPECT().loadWorkflowExecution().Return(s.mockMutableState, nil).Times(1)
	weContext.EXPECT().lock(gomock.Any()).Return(nil)
	weContext.EXPECT().unlock().Times(1)
	weContext.EXPECT().clear().AnyTimes()
	_, err := s.historyCache.PutIfNotExist(key, weContext)
	s.NoError(err)

	request := &historyservice.SyncActivityRequest{
		DomainId:       domainID,
		WorkflowId:     workflowID,
		RunId:          runID,
		Version:        version,
		ScheduledId:    scheduleID,
		VersionHistory: incomingVersionHistory.ToProto(),
	}
	localVersionHistories := &persistence.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*persistence.VersionHistory{
			{
				BranchToken: []byte{},
				Items: []*persistence.VersionHistoryItem{
					{
						EventID: scheduleID - 10,
						Version: version,
					},
				},
			},
		},
	}
	s.mockMutableState.EXPECT().GetVersionHistories().Return(localVersionHistories).AnyTimes()
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
			lastWriteVersion,
			nil,
		), nil,
	).AnyTimes()

	err = s.nDCActivityReplicator.SyncActivity(context.Background(), request)
	s.Equal(newNDCRetryTaskErrorWithHint(
		resendMissingEventMessage,
		domainID,
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
	domainName := "some random domain name"
	domainID := testDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	scheduleID := int64(99)
	version := int64(100)

	lastWriteVersion := version - 100
	incomingVersionHistory := persistence.VersionHistory{
		BranchToken: []byte{},
		Items: []*persistence.VersionHistoryItem{
			{
				EventID: 50,
				Version: 2,
			},
			{
				EventID: scheduleID,
				Version: version,
			},
		},
	}
	key := definition.NewWorkflowIdentifier(domainID, workflowID, runID)
	weContext := NewMockworkflowExecutionContext(s.controller)
	weContext.EXPECT().loadWorkflowExecution().Return(s.mockMutableState, nil).Times(1)
	weContext.EXPECT().lock(gomock.Any()).Return(nil)
	weContext.EXPECT().unlock().Times(1)
	weContext.EXPECT().clear().AnyTimes()
	_, err := s.historyCache.PutIfNotExist(key, weContext)
	s.NoError(err)

	request := &historyservice.SyncActivityRequest{
		DomainId:       domainID,
		WorkflowId:     workflowID,
		RunId:          runID,
		Version:        version,
		ScheduledId:    scheduleID,
		VersionHistory: incomingVersionHistory.ToProto(),
	}
	localVersionHistories := &persistence.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*persistence.VersionHistory{
			{
				BranchToken: []byte{},
				Items: []*persistence.VersionHistoryItem{
					{
						EventID: scheduleID,
						Version: version,
					},
				},
			},
		},
	}
	s.mockMutableState.EXPECT().GetVersionHistories().Return(localVersionHistories).AnyTimes()
	s.mockMutableState.EXPECT().GetActivityInfo(scheduleID).Return(nil, false).AnyTimes()
	s.mockMutableState.EXPECT().GetWorkflowStateCloseStatus().
		Return(persistence.WorkflowStateCreated, persistence.WorkflowCloseStatusRunning).AnyTimes()
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
			lastWriteVersion,
			nil,
		), nil,
	).AnyTimes()

	err = s.nDCActivityReplicator.SyncActivity(context.Background(), request)
	s.Nil(err)
}

func (s *activityReplicatorSuite) TestSyncActivity_VersionHistories_LocalVersionHistoryWin() {
	domainName := "some random domain name"
	domainID := testDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	scheduleID := int64(99)
	version := int64(100)

	lastWriteVersion := version - 100
	incomingVersionHistory := persistence.VersionHistory{
		BranchToken: []byte{},
		Items: []*persistence.VersionHistoryItem{
			{
				EventID: scheduleID,
				Version: version,
			},
		},
	}
	key := definition.NewWorkflowIdentifier(domainID, workflowID, runID)
	weContext := NewMockworkflowExecutionContext(s.controller)
	weContext.EXPECT().loadWorkflowExecution().Return(s.mockMutableState, nil).Times(1)
	weContext.EXPECT().lock(gomock.Any()).Return(nil)
	weContext.EXPECT().unlock().Times(1)
	weContext.EXPECT().clear().AnyTimes()
	_, err := s.historyCache.PutIfNotExist(key, weContext)
	s.NoError(err)

	request := &historyservice.SyncActivityRequest{
		DomainId:       domainID,
		WorkflowId:     workflowID,
		RunId:          runID,
		Version:        version,
		ScheduledId:    scheduleID,
		VersionHistory: incomingVersionHistory.ToProto(),
	}
	localVersionHistories := &persistence.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*persistence.VersionHistory{
			{
				BranchToken: []byte{},
				Items: []*persistence.VersionHistoryItem{
					{
						EventID: scheduleID,
						Version: version,
					},
					{
						EventID: scheduleID + 1,
						Version: version + 1,
					},
				},
			},
		},
	}
	s.mockMutableState.EXPECT().GetVersionHistories().Return(localVersionHistories).AnyTimes()
	s.mockMutableState.EXPECT().GetActivityInfo(scheduleID).Return(nil, false).AnyTimes()
	s.mockMutableState.EXPECT().GetWorkflowStateCloseStatus().
		Return(persistence.WorkflowStateCreated, persistence.WorkflowCloseStatusRunning).AnyTimes()
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
			lastWriteVersion,
			nil,
		), nil,
	).AnyTimes()

	err = s.nDCActivityReplicator.SyncActivity(context.Background(), request)
	s.Nil(err)
}

func (s *activityReplicatorSuite) TestSyncActivity_ActivityCompleted() {
	domainName := "some random domain name"
	domainID := testDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	scheduleID := int64(144)
	version := int64(100)
	lastWriteVersion := version
	nextEventID := scheduleID + 10

	key := definition.NewWorkflowIdentifier(domainID, workflowID, runID)
	weContext := NewMockworkflowExecutionContext(s.controller)
	weContext.EXPECT().loadWorkflowExecution().Return(s.mockMutableState, nil).Times(1)
	weContext.EXPECT().lock(gomock.Any()).Return(nil)
	weContext.EXPECT().unlock().Times(1)
	weContext.EXPECT().clear().AnyTimes()
	_, err := s.historyCache.PutIfNotExist(key, weContext)
	s.NoError(err)

	request := &historyservice.SyncActivityRequest{
		DomainId:    domainID,
		WorkflowId:  workflowID,
		RunId:       runID,
		Version:     version,
		ScheduledId: scheduleID,
	}
	s.mockMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes()
	s.mockMutableState.EXPECT().GetNextEventID().Return(nextEventID).AnyTimes()
	var versionHistories *persistence.VersionHistories
	s.mockMutableState.EXPECT().GetVersionHistories().Return(versionHistories).AnyTimes()
	s.mockMutableState.EXPECT().GetReplicationState().Return(&persistence.ReplicationState{}).AnyTimes()
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
			lastWriteVersion,
			nil,
		), nil,
	).AnyTimes()
	s.mockMutableState.EXPECT().GetActivityInfo(scheduleID).Return(nil, false).AnyTimes()

	err = s.nDCActivityReplicator.SyncActivity(context.Background(), request)
	s.Nil(err)
}

func (s *activityReplicatorSuite) TestSyncActivity_ActivityRunning_LocalActivityVersionLarger() {
	domainName := "some random domain name"
	domainID := testDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	scheduleID := int64(144)
	version := int64(100)
	lastWriteVersion := version + 10
	nextEventID := scheduleID + 10

	key := definition.NewWorkflowIdentifier(domainID, workflowID, runID)
	weContext := NewMockworkflowExecutionContext(s.controller)
	weContext.EXPECT().loadWorkflowExecution().Return(s.mockMutableState, nil).Times(1)
	weContext.EXPECT().lock(gomock.Any()).Return(nil)
	weContext.EXPECT().unlock().Times(1)
	weContext.EXPECT().clear().AnyTimes()
	_, err := s.historyCache.PutIfNotExist(key, weContext)
	s.NoError(err)

	request := &historyservice.SyncActivityRequest{
		DomainId:    domainID,
		WorkflowId:  workflowID,
		RunId:       runID,
		Version:     version,
		ScheduledId: scheduleID,
	}
	s.mockMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes()
	s.mockMutableState.EXPECT().GetNextEventID().Return(nextEventID).AnyTimes()
	var versionHistories *persistence.VersionHistories
	s.mockMutableState.EXPECT().GetVersionHistories().Return(versionHistories).AnyTimes()
	s.mockMutableState.EXPECT().GetReplicationState().Return(&persistence.ReplicationState{}).AnyTimes()
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
			lastWriteVersion,
			nil,
		), nil,
	).AnyTimes()
	s.mockMutableState.EXPECT().GetActivityInfo(scheduleID).Return(&persistence.ActivityInfo{
		Version: lastWriteVersion - 1,
	}, true).AnyTimes()

	err = s.nDCActivityReplicator.SyncActivity(context.Background(), request)
	s.Nil(err)
}

func (s *activityReplicatorSuite) TestSyncActivity_ActivityRunning_Update_SameVersionSameAttempt() {
	domainName := "some random domain name"
	domainID := testDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	version := int64(100)
	scheduleID := int64(144)
	scheduledTime := time.Now()
	startedID := scheduleID + 1
	startedTime := scheduledTime.Add(time.Minute)
	heartBeatUpdatedTime := startedTime.Add(time.Minute)
	attempt := int32(0)
	details := []byte("some random activity heartbeat progress")
	nextEventID := scheduleID + 10

	key := definition.NewWorkflowIdentifier(domainID, workflowID, runID)
	weContext := NewMockworkflowExecutionContext(s.controller)
	weContext.EXPECT().loadWorkflowExecution().Return(s.mockMutableState, nil).Times(1)
	weContext.EXPECT().lock(gomock.Any()).Return(nil)
	weContext.EXPECT().unlock().Times(1)
	weContext.EXPECT().clear().Times(1)
	_, err := s.historyCache.PutIfNotExist(key, weContext)
	s.NoError(err)

	request := &historyservice.SyncActivityRequest{
		DomainId:          domainID,
		WorkflowId:        workflowID,
		RunId:             runID,
		Version:           version,
		ScheduledId:       scheduleID,
		ScheduledTime:     scheduledTime.UnixNano(),
		StartedId:         startedID,
		StartedTime:       startedTime.UnixNano(),
		Attempt:           attempt,
		LastHeartbeatTime: heartBeatUpdatedTime.UnixNano(),
		Details:           details,
	}
	s.mockMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes()
	s.mockMutableState.EXPECT().GetNextEventID().Return(nextEventID).AnyTimes()
	var versionHistories *persistence.VersionHistories
	s.mockMutableState.EXPECT().GetVersionHistories().Return(versionHistories).AnyTimes()
	s.mockMutableState.EXPECT().GetReplicationState().Return(&persistence.ReplicationState{}).AnyTimes()
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
			version,
			nil,
		), nil,
	).AnyTimes()
	activityInfo := &persistence.ActivityInfo{
		Version:    version,
		ScheduleID: scheduleID,
		Attempt:    attempt,
	}
	s.mockMutableState.EXPECT().GetActivityInfo(scheduleID).Return(activityInfo, true).AnyTimes()
	s.mockClusterMetadata.EXPECT().IsVersionFromSameCluster(version, activityInfo.Version).Return(true).AnyTimes()

	expectedErr := errors.New("this is error is used to by pass lots of mocking")
	s.mockMutableState.EXPECT().ReplicateActivityInfo(request, false).Return(expectedErr).Times(1)

	err = s.nDCActivityReplicator.SyncActivity(context.Background(), request)
	s.Equal(expectedErr, err)
}

func (s *activityReplicatorSuite) TestSyncActivity_ActivityRunning_Update_SameVersionLargerAttempt() {
	domainName := "some random domain name"
	domainID := testDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	version := int64(100)
	scheduleID := int64(144)
	scheduledTime := time.Now()
	startedID := scheduleID + 1
	startedTime := scheduledTime.Add(time.Minute)
	heartBeatUpdatedTime := startedTime.Add(time.Minute)
	attempt := int32(100)
	details := []byte("some random activity heartbeat progress")
	nextEventID := scheduleID + 10

	key := definition.NewWorkflowIdentifier(domainID, workflowID, runID)
	weContext := NewMockworkflowExecutionContext(s.controller)
	weContext.EXPECT().loadWorkflowExecution().Return(s.mockMutableState, nil).Times(1)
	weContext.EXPECT().lock(gomock.Any()).Return(nil)
	weContext.EXPECT().unlock().Times(1)
	weContext.EXPECT().clear().Times(1)
	_, err := s.historyCache.PutIfNotExist(key, weContext)
	s.NoError(err)

	request := &historyservice.SyncActivityRequest{
		DomainId:          domainID,
		WorkflowId:        workflowID,
		RunId:             runID,
		Version:           version,
		ScheduledId:       scheduleID,
		ScheduledTime:     scheduledTime.UnixNano(),
		StartedId:         startedID,
		StartedTime:       startedTime.UnixNano(),
		Attempt:           attempt,
		LastHeartbeatTime: heartBeatUpdatedTime.UnixNano(),
		Details:           details,
	}
	s.mockMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes()
	s.mockMutableState.EXPECT().GetNextEventID().Return(nextEventID).AnyTimes()
	var versionHistories *persistence.VersionHistories
	s.mockMutableState.EXPECT().GetVersionHistories().Return(versionHistories).AnyTimes()
	s.mockMutableState.EXPECT().GetReplicationState().Return(&persistence.ReplicationState{}).AnyTimes()
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
			version,
			nil,
		), nil,
	).AnyTimes()
	activityInfo := &persistence.ActivityInfo{
		Version:    version,
		ScheduleID: scheduleID,
		Attempt:    attempt - 1,
	}
	s.mockMutableState.EXPECT().GetActivityInfo(scheduleID).Return(activityInfo, true).AnyTimes()
	s.mockClusterMetadata.EXPECT().IsVersionFromSameCluster(version, activityInfo.Version).Return(true).AnyTimes()

	expectedErr := errors.New("this is error is used to by pass lots of mocking")
	s.mockMutableState.EXPECT().ReplicateActivityInfo(request, true).Return(expectedErr).Times(1)

	err = s.nDCActivityReplicator.SyncActivity(context.Background(), request)
	s.Equal(expectedErr, err)
}

func (s *activityReplicatorSuite) TestSyncActivity_ActivityRunning_Update_LargerVersion() {
	domainName := "some random domain name"
	domainID := testDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	version := int64(100)
	scheduleID := int64(144)
	scheduledTime := time.Now()
	startedID := scheduleID + 1
	startedTime := scheduledTime.Add(time.Minute)
	heartBeatUpdatedTime := startedTime.Add(time.Minute)
	attempt := int32(100)
	details := []byte("some random activity heartbeat progress")
	nextEventID := scheduleID + 10

	key := definition.NewWorkflowIdentifier(domainID, workflowID, runID)
	weContext := NewMockworkflowExecutionContext(s.controller)
	weContext.EXPECT().loadWorkflowExecution().Return(s.mockMutableState, nil).Times(1)
	weContext.EXPECT().lock(gomock.Any()).Return(nil)
	weContext.EXPECT().unlock().Times(1)
	weContext.EXPECT().clear().Times(1)
	_, err := s.historyCache.PutIfNotExist(key, weContext)
	s.NoError(err)

	request := &historyservice.SyncActivityRequest{
		DomainId:          domainID,
		WorkflowId:        workflowID,
		RunId:             runID,
		Version:           version,
		ScheduledId:       scheduleID,
		ScheduledTime:     scheduledTime.UnixNano(),
		StartedId:         startedID,
		StartedTime:       startedTime.UnixNano(),
		Attempt:           attempt,
		LastHeartbeatTime: heartBeatUpdatedTime.UnixNano(),
		Details:           details,
	}
	s.mockMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes()
	s.mockMutableState.EXPECT().GetNextEventID().Return(nextEventID).AnyTimes()
	var versionHistories *persistence.VersionHistories
	s.mockMutableState.EXPECT().GetVersionHistories().Return(versionHistories).AnyTimes()
	s.mockMutableState.EXPECT().GetReplicationState().Return(&persistence.ReplicationState{}).AnyTimes()
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
			version,
			nil,
		), nil,
	).AnyTimes()
	activityInfo := &persistence.ActivityInfo{
		Version:    version - 1,
		ScheduleID: scheduleID,
		Attempt:    attempt + 1,
	}
	s.mockMutableState.EXPECT().GetActivityInfo(scheduleID).Return(activityInfo, true).AnyTimes()
	s.mockClusterMetadata.EXPECT().IsVersionFromSameCluster(version, activityInfo.Version).Return(false).AnyTimes()

	expectedErr := errors.New("this is error is used to by pass lots of mocking")
	s.mockMutableState.EXPECT().ReplicateActivityInfo(request, true).Return(expectedErr).Times(1)

	err = s.nDCActivityReplicator.SyncActivity(context.Background(), request)
	s.Equal(expectedErr, err)
}

func (s *activityReplicatorSuite) TestSyncActivity_ActivityRunning() {
	domainName := "some random domain name"
	domainID := testDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	version := int64(100)
	scheduleID := int64(144)
	scheduledTime := time.Now()
	startedID := scheduleID + 1
	startedTime := scheduledTime.Add(time.Minute)
	heartBeatUpdatedTime := startedTime.Add(time.Minute)
	attempt := int32(100)
	details := []byte("some random activity heartbeat progress")
	nextEventID := scheduleID + 10

	key := definition.NewWorkflowIdentifier(domainID, workflowID, runID)
	weContext := NewMockworkflowExecutionContext(s.controller)
	weContext.EXPECT().loadWorkflowExecution().Return(s.mockMutableState, nil).Times(1)
	weContext.EXPECT().lock(gomock.Any()).Return(nil)
	weContext.EXPECT().unlock().Times(1)
	_, err := s.historyCache.PutIfNotExist(key, weContext)
	s.NoError(err)

	request := &historyservice.SyncActivityRequest{
		DomainId:          domainID,
		WorkflowId:        workflowID,
		RunId:             runID,
		Version:           version,
		ScheduledId:       scheduleID,
		ScheduledTime:     scheduledTime.UnixNano(),
		StartedId:         startedID,
		StartedTime:       startedTime.UnixNano(),
		Attempt:           attempt,
		LastHeartbeatTime: heartBeatUpdatedTime.UnixNano(),
		Details:           details,
	}
	s.mockMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes()
	s.mockMutableState.EXPECT().GetNextEventID().Return(nextEventID).AnyTimes()
	var versionHistories *persistence.VersionHistories
	s.mockMutableState.EXPECT().GetVersionHistories().Return(versionHistories).AnyTimes()
	s.mockMutableState.EXPECT().GetReplicationState().Return(&persistence.ReplicationState{}).AnyTimes()
	s.mockMutableState.EXPECT().GetWorkflowStateCloseStatus().Return(1, 0).AnyTimes()
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
			version,
			nil,
		), nil,
	).AnyTimes()
	activityInfo := &persistence.ActivityInfo{
		Version:    version - 1,
		ScheduleID: scheduleID,
		Attempt:    attempt + 1,
	}
	s.mockMutableState.EXPECT().GetActivityInfo(scheduleID).Return(activityInfo, true).AnyTimes()
	activityInfos := map[int64]*persistence.ActivityInfo{activityInfo.ScheduleID: activityInfo}
	s.mockMutableState.EXPECT().GetPendingActivityInfos().Return(activityInfos).AnyTimes()
	s.mockClusterMetadata.EXPECT().IsVersionFromSameCluster(version, activityInfo.Version).Return(false).AnyTimes()

	s.mockMutableState.EXPECT().ReplicateActivityInfo(request, true).Return(nil).Times(1)
	s.mockMutableState.EXPECT().UpdateActivity(activityInfo).Return(nil).Times(1)
	s.mockMutableState.EXPECT().GetCurrentVersion().Return(int64(1)).Times(1)
	s.mockMutableState.EXPECT().AddTimerTasks(gomock.Any()).Times(1)
	now := time.Unix(0, request.GetLastHeartbeatTime())
	weContext.EXPECT().updateWorkflowExecutionWithNew(
		now,
		persistence.UpdateWorkflowModeUpdateCurrent,
		nil,
		nil,
		transactionPolicyPassive,
		nil,
	).Return(nil).Times(1)
	err = s.nDCActivityReplicator.SyncActivity(context.Background(), request)
	s.NoError(err)
}

func (s *activityReplicatorSuite) TestSyncActivity_ActivityRunning_ZombieWorkflow() {
	domainName := "some random domain name"
	domainID := testDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	version := int64(100)
	scheduleID := int64(144)
	scheduledTime := time.Now()
	startedID := scheduleID + 1
	startedTime := scheduledTime.Add(time.Minute)
	heartBeatUpdatedTime := startedTime.Add(time.Minute)
	attempt := int32(100)
	details := []byte("some random activity heartbeat progress")
	nextEventID := scheduleID + 10

	key := definition.NewWorkflowIdentifier(domainID, workflowID, runID)
	weContext := NewMockworkflowExecutionContext(s.controller)
	weContext.EXPECT().loadWorkflowExecution().Return(s.mockMutableState, nil).Times(1)
	weContext.EXPECT().lock(gomock.Any()).Return(nil)
	weContext.EXPECT().unlock().Times(1)
	_, err := s.historyCache.PutIfNotExist(key, weContext)
	s.NoError(err)

	request := &historyservice.SyncActivityRequest{
		DomainId:          domainID,
		WorkflowId:        workflowID,
		RunId:             runID,
		Version:           version,
		ScheduledId:       scheduleID,
		ScheduledTime:     scheduledTime.UnixNano(),
		StartedId:         startedID,
		StartedTime:       startedTime.UnixNano(),
		Attempt:           attempt,
		LastHeartbeatTime: heartBeatUpdatedTime.UnixNano(),
		Details:           details,
	}
	s.mockMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes()
	s.mockMutableState.EXPECT().GetNextEventID().Return(nextEventID).AnyTimes()
	var versionHistories *persistence.VersionHistories
	s.mockMutableState.EXPECT().GetVersionHistories().Return(versionHistories).AnyTimes()
	s.mockMutableState.EXPECT().GetReplicationState().Return(&persistence.ReplicationState{}).AnyTimes()
	s.mockMutableState.EXPECT().GetWorkflowStateCloseStatus().Return(3, 0).AnyTimes()
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
			version,
			nil,
		), nil,
	).AnyTimes()
	activityInfo := &persistence.ActivityInfo{
		Version:    version - 1,
		ScheduleID: scheduleID,
		Attempt:    attempt + 1,
	}
	s.mockMutableState.EXPECT().GetActivityInfo(scheduleID).Return(activityInfo, true).AnyTimes()
	activityInfos := map[int64]*persistence.ActivityInfo{activityInfo.ScheduleID: activityInfo}
	s.mockMutableState.EXPECT().GetPendingActivityInfos().Return(activityInfos).AnyTimes()
	s.mockClusterMetadata.EXPECT().IsVersionFromSameCluster(version, activityInfo.Version).Return(false).AnyTimes()

	s.mockMutableState.EXPECT().ReplicateActivityInfo(request, true).Return(nil).Times(1)
	s.mockMutableState.EXPECT().UpdateActivity(activityInfo).Return(nil).Times(1)
	s.mockMutableState.EXPECT().GetCurrentVersion().Return(int64(1)).Times(1)
	s.mockMutableState.EXPECT().AddTimerTasks(gomock.Any()).Times(1)
	now := time.Unix(0, request.GetLastHeartbeatTime())
	weContext.EXPECT().updateWorkflowExecutionWithNew(
		now,
		persistence.UpdateWorkflowModeBypassCurrent,
		nil,
		nil,
		transactionPolicyPassive,
		nil,
	).Return(nil).Times(1)
	err = s.nDCActivityReplicator.SyncActivity(context.Background(), request)
	s.NoError(err)
}
