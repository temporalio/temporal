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
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"

	h "github.com/uber/cadence/.gen/go/history"
	"github.com/uber/cadence/.gen/go/shared"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/client"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/clock"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/definition"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/loggerimpl"
	"github.com/uber/cadence/common/messaging"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/mocks"
	"github.com/uber/cadence/common/persistence"
	p "github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/service"
)

const (
	testShardID = 1
)

type (
	historyReplicatorSuite struct {
		suite.Suite
		logger                   log.Logger
		mockCtrl                 *gomock.Controller
		mockExecutionMgr         *mocks.ExecutionManager
		mockHistoryV2Mgr         *mocks.HistoryV2Manager
		mockShardManager         *mocks.ShardManager
		mockClusterMetadata      *mocks.ClusterMetadata
		mockProducer             *mocks.KafkaProducer
		mockDomainCache          *cache.DomainCacheMock
		mockMessagingClient      messaging.Client
		mockService              service.Service
		mockShard                *shardContextImpl
		mockClientBean           *client.MockClientBean
		mockWorkflowResetor      *mockWorkflowResetor
		mockTxProcessor          *MockTransferQueueProcessor
		mockReplicationProcessor *MockReplicatorQueueProcessor
		mockTimerProcessor       *MockTimerQueueProcessor

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
	s.logger = loggerimpl.NewDevelopmentForTest(s.Suite)
	s.mockCtrl = gomock.NewController(s.T())
	s.mockHistoryV2Mgr = &mocks.HistoryV2Manager{}
	s.mockExecutionMgr = &mocks.ExecutionManager{}
	s.mockClusterMetadata = &mocks.ClusterMetadata{}
	s.mockShardManager = &mocks.ShardManager{}
	s.mockProducer = &mocks.KafkaProducer{}
	s.mockMessagingClient = mocks.NewMockMessagingClient(s.mockProducer, nil)
	s.mockDomainCache = &cache.DomainCacheMock{}
	metricsClient := metrics.NewClient(tally.NoopScope, metrics.History)
	s.mockClientBean = &client.MockClientBean{}
	s.mockService = service.NewTestService(
		s.mockClusterMetadata,
		s.mockMessagingClient,
		metricsClient,
		s.mockClientBean,
		nil,
		nil,
		nil)

	s.mockShard = &shardContextImpl{
		service:                   s.mockService,
		shardInfo:                 &persistence.ShardInfo{ShardID: testShardID, RangeID: 1, TransferAckLevel: 0},
		shardID:                   testShardID,
		transferSequenceNumber:    1,
		executionManager:          s.mockExecutionMgr,
		shardManager:              s.mockShardManager,
		clusterMetadata:           s.mockClusterMetadata,
		historyV2Mgr:              s.mockHistoryV2Mgr,
		maxTransferSequenceNumber: 100000,
		closeCh:                   make(chan int, 100),
		config:                    NewDynamicConfigForTest(),
		logger:                    s.logger,
		domainCache:               s.mockDomainCache,
		metricsClient:             metrics.NewClient(tally.NoopScope, metrics.History),
		standbyClusterCurrentTime: make(map[string]time.Time),
		timeSource:                clock.NewRealTimeSource(),
	}
	s.mockClusterMetadata.On("GetCurrentClusterName").Return(cluster.TestCurrentClusterName)
	s.mockClusterMetadata.On("GetAllClusterInfo").Return(cluster.TestAllClusterInfo)
	s.mockClusterMetadata.On("IsGlobalDomainEnabled").Return(true)
	s.mockTxProcessor = &MockTransferQueueProcessor{}
	s.mockTxProcessor.On("NotifyNewTask", mock.Anything, mock.Anything).Maybe()
	s.mockReplicationProcessor = NewMockReplicatorQueueProcessor(s.mockCtrl)
	s.mockReplicationProcessor.EXPECT().notifyNewTask().AnyTimes()
	s.mockTimerProcessor = &MockTimerQueueProcessor{}
	s.mockTimerProcessor.On("NotifyNewTimers", mock.Anything, mock.Anything).Maybe()

	historyCache := newHistoryCache(s.mockShard)
	engine := &historyEngineImpl{
		currentClusterName:   s.mockShard.GetService().GetClusterMetadata().GetCurrentClusterName(),
		shard:                s.mockShard,
		clusterMetadata:      s.mockClusterMetadata,
		executionManager:     s.mockExecutionMgr,
		historyCache:         historyCache,
		logger:               s.logger,
		tokenSerializer:      common.NewJSONTaskTokenSerializer(),
		metricsClient:        s.mockShard.GetMetricsClient(),
		timeSource:           s.mockShard.timeSource,
		historyEventNotifier: newHistoryEventNotifier(clock.NewRealTimeSource(), metrics.NewClient(tally.NoopScope, metrics.History), func(string) int { return 0 }),
		txProcessor:          s.mockTxProcessor,
		replicatorProcessor:  s.mockReplicationProcessor,
		timerProcessor:       s.mockTimerProcessor,
	}
	s.mockShard.SetEngine(engine)

	s.historyReplicator = newHistoryReplicator(s.mockShard, clock.NewEventTimeSource(), engine, historyCache, s.mockShard.domainCache, s.mockHistoryV2Mgr, s.logger)
	s.mockWorkflowResetor = &mockWorkflowResetor{}
	s.historyReplicator.resetor = s.mockWorkflowResetor
}

func (s *historyReplicatorSuite) TearDownTest() {
	s.historyReplicator = nil
	s.mockCtrl.Finish()
	s.mockExecutionMgr.AssertExpectations(s.T())
	s.mockShardManager.AssertExpectations(s.T())
	s.mockProducer.AssertExpectations(s.T())
	s.mockDomainCache.AssertExpectations(s.T())
	s.mockTxProcessor.AssertExpectations(s.T())
	s.mockTimerProcessor.AssertExpectations(s.T())
	s.mockClientBean.AssertExpectations(s.T())
	s.mockWorkflowResetor.AssertExpectations(s.T())
	s.mockTxProcessor.AssertExpectations(s.T())
	s.mockTimerProcessor.AssertExpectations(s.T())
}

func (s *historyReplicatorSuite) TestSyncActivity_WorkflowNotFound() {
	domainName := "some random domain name"
	domainID := testDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	version := int64(100)

	request := &h.SyncActivityRequest{
		DomainId:   common.StringPtr(domainID),
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(runID),
	}
	s.mockExecutionMgr.On("GetWorkflowExecution", &persistence.GetWorkflowExecutionRequest{
		DomainID: domainID,
		Execution: shared.WorkflowExecution{
			WorkflowId: common.StringPtr(workflowID),
			RunId:      common.StringPtr(runID),
		},
	}).Return(nil, &shared.EntityNotExistsError{})
	s.mockDomainCache.On("GetDomainByID", domainID).Return(
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
	)

	err := s.historyReplicator.SyncActivity(ctx.Background(), request)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestSyncActivity_WorkflowClosed() {
	domainName := "some random domain name"
	domainID := testDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	version := int64(100)

	context, release, err := s.historyReplicator.historyCache.getOrCreateWorkflowExecutionForBackground(
		domainID,
		shared.WorkflowExecution{
			WorkflowId: common.StringPtr(workflowID),
			RunId:      common.StringPtr(runID),
		},
	)
	s.Nil(err)
	msBuilder := &mockMutableState{}
	defer msBuilder.AssertExpectations(s.T())
	context.(*workflowExecutionContextImpl).msBuilder = msBuilder
	release(nil)
	request := &h.SyncActivityRequest{
		DomainId:   common.StringPtr(domainID),
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(runID),
	}
	msBuilder.On("StartTransaction", mock.Anything).Return(false, nil).Once()
	msBuilder.On("IsWorkflowExecutionRunning").Return(false)
	s.mockDomainCache.On("GetDomainByID", domainID).Return(
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
	)

	err = s.historyReplicator.SyncActivity(ctx.Background(), request)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestSyncActivity_IncomingScheduleIDLarger_IncomingVersionSmaller() {
	domainName := "some random domain name"
	domainID := testDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	scheduleID := int64(144)
	version := int64(100)

	lastWriteVersion := version + 100
	nextEventID := scheduleID - 10

	context, release, err := s.historyReplicator.historyCache.getOrCreateWorkflowExecutionForBackground(
		domainID,
		shared.WorkflowExecution{
			WorkflowId: common.StringPtr(workflowID),
			RunId:      common.StringPtr(runID),
		},
	)
	s.Nil(err)
	msBuilder := &mockMutableState{}
	defer msBuilder.AssertExpectations(s.T())
	context.(*workflowExecutionContextImpl).msBuilder = msBuilder
	release(nil)
	request := &h.SyncActivityRequest{
		DomainId:    common.StringPtr(domainID),
		WorkflowId:  common.StringPtr(workflowID),
		RunId:       common.StringPtr(runID),
		Version:     common.Int64Ptr(version),
		ScheduledId: common.Int64Ptr(scheduleID),
	}
	msBuilder.On("StartTransaction", mock.Anything).Return(false, nil).Once()
	msBuilder.On("IsWorkflowExecutionRunning").Return(true)
	msBuilder.On("GetNextEventID").Return(nextEventID)
	msBuilder.On("GetLastWriteVersion").Return(lastWriteVersion, nil)
	s.mockDomainCache.On("GetDomainByID", domainID).Return(
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
	)

	err = s.historyReplicator.SyncActivity(ctx.Background(), request)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestSyncActivity_IncomingScheduleIDLarger_IncomingVersionLarger() {
	domainName := "some random domain name"
	domainID := testDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	scheduleID := int64(144)
	version := int64(100)

	lastWriteVersion := version - 100
	nextEventID := scheduleID - 10

	context, release, err := s.historyReplicator.historyCache.getOrCreateWorkflowExecutionForBackground(
		domainID,
		shared.WorkflowExecution{
			WorkflowId: common.StringPtr(workflowID),
			RunId:      common.StringPtr(runID),
		},
	)
	s.Nil(err)
	msBuilder := &mockMutableState{}
	defer msBuilder.AssertExpectations(s.T())
	context.(*workflowExecutionContextImpl).msBuilder = msBuilder
	release(nil)
	request := &h.SyncActivityRequest{
		DomainId:    common.StringPtr(domainID),
		WorkflowId:  common.StringPtr(workflowID),
		RunId:       common.StringPtr(runID),
		Version:     common.Int64Ptr(version),
		ScheduledId: common.Int64Ptr(scheduleID),
	}
	msBuilder.On("StartTransaction", mock.Anything).Return(false, nil).Once()
	msBuilder.On("IsWorkflowExecutionRunning").Return(true)
	msBuilder.On("GetNextEventID").Return(nextEventID)
	msBuilder.On("GetLastWriteVersion").Return(lastWriteVersion, nil)
	msBuilder.On("GetReplicationState").Return(&persistence.ReplicationState{})
	s.mockDomainCache.On("GetDomainByID", domainID).Return(
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
	)

	err = s.historyReplicator.SyncActivity(ctx.Background(), request)
	s.Equal(newRetryTaskErrorWithHint(ErrRetrySyncActivityMsg, domainID, workflowID, runID, nextEventID), err)
}

func (s *historyReplicatorSuite) TestSyncActivity_ActivityCompleted() {
	domainName := "some random domain name"
	domainID := testDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	scheduleID := int64(144)
	version := int64(100)

	lastWriteVersion := version
	nextEventID := scheduleID + 10

	context, release, err := s.historyReplicator.historyCache.getOrCreateWorkflowExecutionForBackground(
		domainID,
		shared.WorkflowExecution{
			WorkflowId: common.StringPtr(workflowID),
			RunId:      common.StringPtr(runID),
		},
	)
	s.Nil(err)
	msBuilder := &mockMutableState{}
	defer msBuilder.AssertExpectations(s.T())
	context.(*workflowExecutionContextImpl).msBuilder = msBuilder
	release(nil)
	request := &h.SyncActivityRequest{
		DomainId:    common.StringPtr(domainID),
		WorkflowId:  common.StringPtr(workflowID),
		RunId:       common.StringPtr(runID),
		Version:     common.Int64Ptr(version),
		ScheduledId: common.Int64Ptr(scheduleID),
	}
	msBuilder.On("StartTransaction", mock.Anything).Return(false, nil).Once()
	msBuilder.On("IsWorkflowExecutionRunning").Return(true)
	msBuilder.On("GetNextEventID").Return(nextEventID)
	s.mockDomainCache.On("GetDomainByID", domainID).Return(
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
	)
	msBuilder.On("GetActivityInfo", scheduleID).Return(nil, false)

	err = s.historyReplicator.SyncActivity(ctx.Background(), request)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestSyncActivity_ActivityRunning_LocalActivityVersionLarger() {
	domainName := "some random domain name"
	domainID := testDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	scheduleID := int64(144)
	version := int64(100)

	lastWriteVersion := version + 10
	nextEventID := scheduleID + 10

	context, release, err := s.historyReplicator.historyCache.getOrCreateWorkflowExecutionForBackground(
		domainID,
		shared.WorkflowExecution{
			WorkflowId: common.StringPtr(workflowID),
			RunId:      common.StringPtr(runID),
		},
	)
	s.Nil(err)
	msBuilder := &mockMutableState{}
	defer msBuilder.AssertExpectations(s.T())
	context.(*workflowExecutionContextImpl).msBuilder = msBuilder
	release(nil)
	request := &h.SyncActivityRequest{
		DomainId:    common.StringPtr(domainID),
		WorkflowId:  common.StringPtr(workflowID),
		RunId:       common.StringPtr(runID),
		Version:     common.Int64Ptr(version),
		ScheduledId: common.Int64Ptr(scheduleID),
	}
	msBuilder.On("StartTransaction", mock.Anything).Return(false, nil).Once()
	msBuilder.On("IsWorkflowExecutionRunning").Return(true)
	msBuilder.On("GetNextEventID").Return(nextEventID)
	s.mockDomainCache.On("GetDomainByID", domainID).Return(
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
	)
	msBuilder.On("GetActivityInfo", scheduleID).Return(&persistence.ActivityInfo{
		Version: lastWriteVersion - 1,
	}, true)

	err = s.historyReplicator.SyncActivity(ctx.Background(), request)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestSyncActivity_ActivityRunning_Update_SameVersionSameAttempt() {
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

	context, release, err := s.historyReplicator.historyCache.getOrCreateWorkflowExecutionForBackground(
		domainID,
		shared.WorkflowExecution{
			WorkflowId: common.StringPtr(workflowID),
			RunId:      common.StringPtr(runID),
		},
	)
	s.Nil(err)
	msBuilder := &mockMutableState{}
	defer msBuilder.AssertExpectations(s.T())
	context.(*workflowExecutionContextImpl).msBuilder = msBuilder
	release(nil)
	request := &h.SyncActivityRequest{
		DomainId:          common.StringPtr(domainID),
		WorkflowId:        common.StringPtr(workflowID),
		RunId:             common.StringPtr(runID),
		Version:           common.Int64Ptr(version),
		ScheduledId:       common.Int64Ptr(scheduleID),
		ScheduledTime:     common.Int64Ptr(scheduledTime.UnixNano()),
		StartedId:         common.Int64Ptr(startedID),
		StartedTime:       common.Int64Ptr(startedTime.UnixNano()),
		Attempt:           common.Int32Ptr(attempt),
		LastHeartbeatTime: common.Int64Ptr(heartBeatUpdatedTime.UnixNano()),
		Details:           details,
	}
	msBuilder.On("StartTransaction", mock.Anything).Return(false, nil).Once()
	msBuilder.On("IsWorkflowExecutionRunning").Return(true)
	msBuilder.On("GetNextEventID").Return(nextEventID)
	s.mockDomainCache.On("GetDomainByID", domainID).Return(
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
	)
	activityInfo := &persistence.ActivityInfo{
		Version:    version,
		ScheduleID: scheduleID,
		Attempt:    attempt,
	}
	msBuilder.On("GetActivityInfo", scheduleID).Return(activityInfo, true)
	s.mockClusterMetadata.On("IsVersionFromSameCluster", version, activityInfo.Version).Return(true)

	expectedErr := errors.New("this is error is used to by pass lots of mocking")
	msBuilder.On("ReplicateActivityInfo", request, false).Return(expectedErr)

	err = s.historyReplicator.SyncActivity(ctx.Background(), request)
	s.Equal(expectedErr, err)
}

func (s *historyReplicatorSuite) TestSyncActivity_ActivityRunning_Update_SameVersionLargerAttempt() {
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

	context, release, err := s.historyReplicator.historyCache.getOrCreateWorkflowExecutionForBackground(
		domainID,
		shared.WorkflowExecution{
			WorkflowId: common.StringPtr(workflowID),
			RunId:      common.StringPtr(runID),
		},
	)
	s.Nil(err)
	msBuilder := &mockMutableState{}
	defer msBuilder.AssertExpectations(s.T())
	context.(*workflowExecutionContextImpl).msBuilder = msBuilder
	release(nil)
	request := &h.SyncActivityRequest{
		DomainId:          common.StringPtr(domainID),
		WorkflowId:        common.StringPtr(workflowID),
		RunId:             common.StringPtr(runID),
		Version:           common.Int64Ptr(version),
		ScheduledId:       common.Int64Ptr(scheduleID),
		ScheduledTime:     common.Int64Ptr(scheduledTime.UnixNano()),
		StartedId:         common.Int64Ptr(startedID),
		StartedTime:       common.Int64Ptr(startedTime.UnixNano()),
		Attempt:           common.Int32Ptr(attempt),
		LastHeartbeatTime: common.Int64Ptr(heartBeatUpdatedTime.UnixNano()),
		Details:           details,
	}
	msBuilder.On("StartTransaction", mock.Anything).Return(false, nil).Once()
	msBuilder.On("IsWorkflowExecutionRunning").Return(true)
	msBuilder.On("GetNextEventID").Return(nextEventID)
	s.mockDomainCache.On("GetDomainByID", domainID).Return(
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
	)
	activityInfo := &persistence.ActivityInfo{
		Version:    version,
		ScheduleID: scheduleID,
		Attempt:    attempt - 1,
	}
	msBuilder.On("GetActivityInfo", scheduleID).Return(activityInfo, true)
	s.mockClusterMetadata.On("IsVersionFromSameCluster", version, activityInfo.Version).Return(true)

	expectedErr := errors.New("this is error is used to by pass lots of mocking")
	msBuilder.On("ReplicateActivityInfo", request, true).Return(expectedErr)

	err = s.historyReplicator.SyncActivity(ctx.Background(), request)
	s.Equal(expectedErr, err)
}

func (s *historyReplicatorSuite) TestSyncActivity_ActivityRunning_Update_LargerVersion() {
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

	context, release, err := s.historyReplicator.historyCache.getOrCreateWorkflowExecutionForBackground(
		domainID,
		shared.WorkflowExecution{
			WorkflowId: common.StringPtr(workflowID),
			RunId:      common.StringPtr(runID),
		},
	)
	s.Nil(err)
	msBuilder := &mockMutableState{}
	defer msBuilder.AssertExpectations(s.T())
	context.(*workflowExecutionContextImpl).msBuilder = msBuilder
	release(nil)
	request := &h.SyncActivityRequest{
		DomainId:          common.StringPtr(domainID),
		WorkflowId:        common.StringPtr(workflowID),
		RunId:             common.StringPtr(runID),
		Version:           common.Int64Ptr(version),
		ScheduledId:       common.Int64Ptr(scheduleID),
		ScheduledTime:     common.Int64Ptr(scheduledTime.UnixNano()),
		StartedId:         common.Int64Ptr(startedID),
		StartedTime:       common.Int64Ptr(startedTime.UnixNano()),
		Attempt:           common.Int32Ptr(attempt),
		LastHeartbeatTime: common.Int64Ptr(heartBeatUpdatedTime.UnixNano()),
		Details:           details,
	}
	msBuilder.On("StartTransaction", mock.Anything).Return(false, nil).Once()
	msBuilder.On("IsWorkflowExecutionRunning").Return(true)
	msBuilder.On("GetNextEventID").Return(nextEventID)
	s.mockDomainCache.On("GetDomainByID", domainID).Return(
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
	)
	activityInfo := &persistence.ActivityInfo{
		Version:    version - 1,
		ScheduleID: scheduleID,
		Attempt:    attempt + 1,
	}
	msBuilder.On("GetActivityInfo", scheduleID).Return(activityInfo, true)
	s.mockClusterMetadata.On("IsVersionFromSameCluster", version, activityInfo.Version).Return(false)

	expectedErr := errors.New("this is error is used to by pass lots of mocking")
	msBuilder.On("ReplicateActivityInfo", request, true).Return(expectedErr)

	err = s.historyReplicator.SyncActivity(ctx.Background(), request)
	s.Equal(expectedErr, err)
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

	contextCurrent := &mockWorkflowExecutionContext{}
	defer contextCurrent.AssertExpectations(s.T())
	contextCurrent.On("lock", mock.Anything).Return(nil)
	contextCurrent.On("unlock")

	msBuilderCurrent := &mockMutableState{}
	defer msBuilderCurrent.AssertExpectations(s.T())

	contextCurrent.On("loadWorkflowExecution").Return(msBuilderCurrent, nil).Once()
	currentExecution := &shared.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(currentRunID),
	}
	contextCurrentCacheKey := definition.NewWorkflowIdentifier(domainID, currentExecution.GetWorkflowId(), currentExecution.GetRunId())
	s.historyReplicator.historyCache.PutIfNotExist(contextCurrentCacheKey, contextCurrent)

	s.mockExecutionMgr.On("GetCurrentExecution", &persistence.GetCurrentExecutionRequest{
		DomainID:   domainID,
		WorkflowID: workflowID,
	}).Return(&persistence.GetCurrentExecutionResponse{
		RunID: currentRunID,
		// other attributes are not used
	}, nil)

	msBuilderCurrent.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{
		RunID:              currentRunID,
		NextEventID:        currentNextEventID,
		DecisionVersion:    common.EmptyVersion,
		DecisionScheduleID: common.EmptyEventID,
		DecisionStartedID:  common.EmptyEventID,
	})
	msBuilderCurrent.On("GetNextEventID").Return(currentNextEventID)
	msBuilderCurrent.On("GetLastWriteVersion").Return(currentVersion, nil)
	msBuilderCurrent.On("IsWorkflowExecutionRunning").Return(true)

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

	contextCurrent := &mockWorkflowExecutionContext{}
	defer contextCurrent.AssertExpectations(s.T())
	contextCurrent.On("lock", mock.Anything).Return(nil)
	contextCurrent.On("unlock")

	msBuilderCurrent := &mockMutableState{}
	defer msBuilderCurrent.AssertExpectations(s.T())

	contextCurrent.On("loadWorkflowExecution").Return(msBuilderCurrent, nil).Once()
	currentExecution := &shared.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(currentRunID),
	}
	contextCurrentCacheKey := definition.NewWorkflowIdentifier(domainID, currentExecution.GetWorkflowId(), currentExecution.GetRunId())
	s.historyReplicator.historyCache.PutIfNotExist(contextCurrentCacheKey, contextCurrent)

	s.mockExecutionMgr.On("GetCurrentExecution", &persistence.GetCurrentExecutionRequest{
		DomainID:   domainID,
		WorkflowID: workflowID,
	}).Return(&persistence.GetCurrentExecutionResponse{
		RunID: currentRunID,
		// other attributes are not used
	}, nil)

	msBuilderCurrent.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{
		RunID:              currentRunID,
		NextEventID:        currentNextEventID,
		DecisionVersion:    common.EmptyVersion,
		DecisionScheduleID: common.EmptyEventID,
		DecisionStartedID:  common.EmptyEventID,
	})
	msBuilderCurrent.On("GetNextEventID").Return(currentNextEventID)
	msBuilderCurrent.On("GetLastWriteVersion").Return(currentVersion, nil)
	msBuilderCurrent.On("IsWorkflowExecutionRunning").Return(true)
	msBuilderCurrent.On("UpdateReplicationStateVersion", currentVersion, true).Once()
	msBuilderCurrent.On("AddWorkflowExecutionSignaled", signalName, signalInput, signalIdentity).Return(&shared.HistoryEvent{
		EventType: shared.EventTypeWorkflowExecutionSignaled.Ptr(),
		Timestamp: common.Int64Ptr(time.Now().UnixNano()),
		WorkflowExecutionSignaledEventAttributes: &shared.WorkflowExecutionSignaledEventAttributes{
			SignalName: common.StringPtr(signalName),
			Input:      signalInput,
			Identity:   common.StringPtr(signalIdentity),
		},
	}, nil).Once()
	msBuilderCurrent.On("HasPendingDecision").Return(true)

	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", currentVersion).Return(cluster.TestCurrentClusterName)
	s.mockClusterMetadata.On("GetCurrentClusterName").Return(cluster.TestCurrentClusterName)
	contextCurrent.On("updateWorkflowExecutionAsActive", mock.Anything).Return(nil).Once()

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

	contextCurrent := &mockWorkflowExecutionContext{}
	defer contextCurrent.AssertExpectations(s.T())
	contextCurrent.On("lock", mock.Anything).Return(nil)
	contextCurrent.On("unlock")

	msBuilderCurrent := &mockMutableState{}
	defer msBuilderCurrent.AssertExpectations(s.T())

	contextCurrent.On("loadWorkflowExecution").Return(msBuilderCurrent, nil).Once()
	currentExecution := &shared.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(currentRunID),
	}
	contextCurrentCacheKey := definition.NewWorkflowIdentifier(domainID, currentExecution.GetWorkflowId(), currentExecution.GetRunId())
	s.historyReplicator.historyCache.PutIfNotExist(contextCurrentCacheKey, contextCurrent)

	s.mockExecutionMgr.On("GetCurrentExecution", &persistence.GetCurrentExecutionRequest{
		DomainID:   domainID,
		WorkflowID: workflowID,
	}).Return(&persistence.GetCurrentExecutionResponse{
		RunID: currentRunID,
		// other attributes are not used
	}, nil)

	msBuilderCurrent.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{
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
	})
	msBuilderCurrent.On("GetNextEventID").Return(currentNextEventID)
	msBuilderCurrent.On("GetLastWriteVersion").Return(currentVersion, nil)
	msBuilderCurrent.On("IsWorkflowExecutionRunning").Return(true)
	msBuilderCurrent.On("UpdateReplicationStateVersion", currentVersion, true).Once()
	msBuilderCurrent.On("AddWorkflowExecutionSignaled", signalName, signalInput, signalIdentity).Return(&shared.HistoryEvent{
		EventType: shared.EventTypeWorkflowExecutionSignaled.Ptr(),
		Timestamp: common.Int64Ptr(time.Now().UnixNano()),
		WorkflowExecutionSignaledEventAttributes: &shared.WorkflowExecutionSignaledEventAttributes{
			SignalName: common.StringPtr(signalName),
			Input:      signalInput,
			Identity:   common.StringPtr(signalIdentity),
		},
	}, nil).Once()
	msBuilderCurrent.On("HasPendingDecision").Return(false)
	newDecision := &decisionInfo{
		Version:    currentVersion,
		ScheduleID: 1234,
		StartedID:  common.EmptyEventID,
		TaskList:   currentDecisionStickyTasklist,
		Attempt:    0,
	}
	msBuilderCurrent.On("AddDecisionTaskScheduledEvent", false).Return(newDecision, nil)

	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", currentVersion).Return(cluster.TestCurrentClusterName)
	s.mockClusterMetadata.On("GetCurrentClusterName").Return(cluster.TestCurrentClusterName)

	contextCurrent.On("updateWorkflowExecutionAsActive", mock.Anything).Return(nil).Once()

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

	s.mockDomainCache.On("GetDomainByID", domainID).Return(
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
	)
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

	s.mockDomainCache.On("GetDomainByID", domainID).Return(
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
	)
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
	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", version).Return(sourceClusterName)
	currentClusterName := cluster.TestCurrentClusterName
	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", currentVersion).Return(currentClusterName)

	contextCurrent := &mockWorkflowExecutionContext{}
	defer contextCurrent.AssertExpectations(s.T())
	contextCurrent.On("lock", mock.Anything).Return(nil)
	contextCurrent.On("unlock")
	msBuilderCurrent := &mockMutableState{}
	defer msBuilderCurrent.AssertExpectations(s.T())

	contextCurrent.On("loadWorkflowExecution").Return(msBuilderCurrent, nil)
	currentExecution := &shared.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(currentRunID),
	}
	contextCurrent.On("getExecution").Return(currentExecution)
	contextCurrentCacheKey := definition.NewWorkflowIdentifier(domainID, currentExecution.GetWorkflowId(), currentExecution.GetRunId())
	s.historyReplicator.historyCache.PutIfNotExist(contextCurrentCacheKey, contextCurrent)

	s.mockExecutionMgr.On("GetCurrentExecution", &persistence.GetCurrentExecutionRequest{
		DomainID:   domainID,
		WorkflowID: workflowID,
	}).Return(&persistence.GetCurrentExecutionResponse{
		RunID:       currentRunID,
		CloseStatus: persistence.WorkflowCloseStatusNone,
	}, nil)

	msBuilderCurrent.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{
		RunID:              currentRunID,
		DecisionVersion:    common.EmptyVersion,
		DecisionScheduleID: common.EmptyEventID,
		DecisionStartedID:  common.EmptyEventID,
	})
	msBuilderCurrent.On("GetLastWriteVersion").Return(currentVersion, nil)
	msBuilderCurrent.On("GetNextEventID").Return(currentNextEventID)
	msBuilderCurrent.On("IsWorkflowExecutionRunning").Return(true) // this is used to update the version on mutable state
	msBuilderCurrent.On("UpdateReplicationStateVersion", currentVersion, true).Once()

	s.mockExecutionMgr.On("GetCurrentExecution", &persistence.GetCurrentExecutionRequest{
		DomainID:   domainID,
		WorkflowID: workflowID,
	}).Return(&persistence.GetCurrentExecutionResponse{
		RunID:       currentRunID,
		CloseStatus: persistence.WorkflowCloseStatusNone,
	}, nil)

	msBuilderCurrent.On("AddWorkflowExecutionTerminatedEvent",
		workflowTerminationReason, mock.Anything, workflowTerminationIdentity).Return(&workflow.HistoryEvent{}, nil)
	contextCurrent.On("updateWorkflowExecutionAsActive", mock.Anything).Return(nil).Once()

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

	s.mockDomainCache.On("GetDomainByID", domainID).Return(
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
	)
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

	s.mockDomainCache.On("GetDomainByID", domainID).Return(
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
	)
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

	s.mockWorkflowResetor.On("ApplyResetEvent", reqCtx, req, domainID, workflowID, currentRunID).Return(nil).Once()

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

	s.mockDomainCache.On("GetDomainByID", domainID).Return(
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
	)
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

	context := &mockWorkflowExecutionContext{}
	defer context.AssertExpectations(s.T())
	context.On("getDomainID").Return(domainID)
	context.On("getExecution").Return(&shared.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(runID),
	})

	msBuilderIn := &mockMutableState{}
	defer msBuilderIn.AssertExpectations(s.T())
	request := &h.ReplicateEventsRequest{
		Version: common.Int64Ptr(incomingVersion),
		History: &shared.History{Events: []*shared.HistoryEvent{
			{
				EventType: shared.EventTypeWorkflowExecutionCanceled.Ptr(),
				Timestamp: common.Int64Ptr(time.Now().UnixNano()),
			},
		}},
	}
	msBuilderIn.On("IsWorkflowExecutionRunning").Return(false)
	msBuilderIn.On("GetReplicationState").Return(&persistence.ReplicationState{LastWriteVersion: currentLastWriteVersion})
	s.mockExecutionMgr.On("GetCurrentExecution", &persistence.GetCurrentExecutionRequest{
		DomainID:   domainID,
		WorkflowID: workflowID,
	}).Return(&persistence.GetCurrentExecutionResponse{
		RunID: runID,
		// other attributes are not used
	}, nil)

	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", currentLastWriteVersion).Return(cluster.TestCurrentClusterName)
	s.mockClusterMetadata.On("GetCurrentClusterName").Return(cluster.TestCurrentClusterName)

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

	context := &mockWorkflowExecutionContext{}
	defer context.AssertExpectations(s.T())
	context.On("getDomainID").Return(domainID)
	context.On("getExecution").Return(&shared.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(runID),
	})

	msBuilderIn := &mockMutableState{}
	defer msBuilderIn.AssertExpectations(s.T())
	request := &h.ReplicateEventsRequest{
		Version: common.Int64Ptr(incomingVersion),
		History: &shared.History{Events: []*shared.HistoryEvent{
			{
				EventType: shared.EventTypeWorkflowExecutionCanceled.Ptr(),
				Timestamp: common.Int64Ptr(time.Now().UnixNano()),
			},
		}},
	}
	msBuilderIn.On("IsWorkflowExecutionRunning").Return(false)
	msBuilderIn.On("GetReplicationState").Return(&persistence.ReplicationState{LastWriteVersion: lastWriteVersion})
	s.mockExecutionMgr.On("GetCurrentExecution", &persistence.GetCurrentExecutionRequest{
		DomainID:   domainID,
		WorkflowID: workflowID,
	}).Return(&persistence.GetCurrentExecutionResponse{
		RunID: currentRunID,
		// other attributes are not used
	}, nil)

	contextCurrent := &mockWorkflowExecutionContext{}
	defer contextCurrent.AssertExpectations(s.T())
	contextCurrent.On("lock", mock.Anything).Return(nil)
	contextCurrent.On("unlock")

	msBuilderCurrent := &mockMutableState{}
	defer msBuilderCurrent.AssertExpectations(s.T())

	contextCurrent.On("loadWorkflowExecution").Return(msBuilderCurrent, nil).Once()
	currentExecution := &shared.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(currentRunID),
	}
	contextCurrentCacheKey := definition.NewWorkflowIdentifier(domainID, currentExecution.GetWorkflowId(), currentExecution.GetRunId())
	s.historyReplicator.historyCache.PutIfNotExist(contextCurrentCacheKey, contextCurrent)

	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", lastWriteVersion).Return(cluster.TestCurrentClusterName)
	s.mockClusterMetadata.On("GetCurrentClusterName").Return(cluster.TestCurrentClusterName)

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

	context := &mockWorkflowExecutionContext{}
	defer context.AssertExpectations(s.T())
	context.On("getDomainID").Return(domainID)
	context.On("getExecution").Return(&shared.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(runID),
	})

	msBuilderIn := &mockMutableState{}
	defer msBuilderIn.AssertExpectations(s.T())
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
	msBuilderIn.On("IsWorkflowExecutionRunning").Return(false)
	msBuilderIn.On("GetReplicationState").Return(&persistence.ReplicationState{LastWriteVersion: lastWriteVersion})
	s.mockExecutionMgr.On("GetCurrentExecution", &persistence.GetCurrentExecutionRequest{
		DomainID:   domainID,
		WorkflowID: workflowID,
	}).Return(&persistence.GetCurrentExecutionResponse{
		RunID: currentRunID,
		// other attributes are not used
	}, nil)

	contextCurrent := &mockWorkflowExecutionContext{}
	defer contextCurrent.AssertExpectations(s.T())
	contextCurrent.On("lock", mock.Anything).Return(nil)
	contextCurrent.On("unlock")

	msBuilderCurrent := &mockMutableState{}
	defer msBuilderCurrent.AssertExpectations(s.T())

	contextCurrent.On("loadWorkflowExecution").Return(msBuilderCurrent, nil).Once()
	currentExecution := &shared.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(currentRunID),
	}
	contextCurrentCacheKey := definition.NewWorkflowIdentifier(domainID, currentExecution.GetWorkflowId(), currentExecution.GetRunId())
	s.historyReplicator.historyCache.PutIfNotExist(contextCurrentCacheKey, contextCurrent)

	msBuilderCurrent.On("IsWorkflowExecutionRunning").Return(true)
	msBuilderCurrent.On("GetLastWriteVersion").Return(currentLastWriteVersion, nil)
	msBuilderCurrent.On("UpdateReplicationStateVersion", currentLastWriteVersion, true).Once()
	msBuilderCurrent.On("AddWorkflowExecutionSignaled", signalName, signalInput, signalIdentity).Return(&shared.HistoryEvent{
		EventType: shared.EventTypeWorkflowExecutionSignaled.Ptr(),
		Timestamp: common.Int64Ptr(time.Now().UnixNano()),
		WorkflowExecutionSignaledEventAttributes: &shared.WorkflowExecutionSignaledEventAttributes{
			SignalName: common.StringPtr(signalName),
			Input:      signalInput,
			Identity:   common.StringPtr(signalIdentity),
		},
	}, nil).Once()
	msBuilderCurrent.On("HasPendingDecision").Return(true)

	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", currentLastWriteVersion).Return(cluster.TestCurrentClusterName)
	s.mockClusterMetadata.On("GetCurrentClusterName").Return(cluster.TestCurrentClusterName)

	contextCurrent.On("updateWorkflowExecutionAsActive", mock.Anything).Return(nil).Once()

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

	context := &mockWorkflowExecutionContext{}
	defer context.AssertExpectations(s.T())
	context.On("getDomainID").Return(domainID)
	context.On("getExecution").Return(&shared.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(runID),
	})

	msBuilderIn := &mockMutableState{}
	defer msBuilderIn.AssertExpectations(s.T())
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
	msBuilderIn.On("IsWorkflowExecutionRunning").Return(false)
	msBuilderIn.On("GetReplicationState").Return(&persistence.ReplicationState{LastWriteVersion: lastWriteVersion})
	s.mockExecutionMgr.On("GetCurrentExecution", &persistence.GetCurrentExecutionRequest{
		DomainID:   domainID,
		WorkflowID: workflowID,
	}).Return(&persistence.GetCurrentExecutionResponse{
		RunID: currentRunID,
		// other attributes are not used
	}, nil)

	contextCurrent := &mockWorkflowExecutionContext{}
	defer contextCurrent.AssertExpectations(s.T())
	contextCurrent.On("lock", mock.Anything).Return(nil)
	contextCurrent.On("unlock")

	msBuilderCurrent := &mockMutableState{}
	defer msBuilderCurrent.AssertExpectations(s.T())

	contextCurrent.On("loadWorkflowExecution").Return(msBuilderCurrent, nil).Once()
	currentExecution := &shared.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(currentRunID),
	}
	contextCurrentCacheKey := definition.NewWorkflowIdentifier(domainID, currentExecution.GetWorkflowId(), currentExecution.GetRunId())
	s.historyReplicator.historyCache.PutIfNotExist(contextCurrentCacheKey, contextCurrent)

	msBuilderCurrent.On("IsWorkflowExecutionRunning").Return(true)
	msBuilderCurrent.On("GetLastWriteVersion").Return(currentLastWriteVersion, nil)
	msBuilderCurrent.On("UpdateReplicationStateVersion", currentLastWriteVersion, true).Once()
	msBuilderCurrent.On("AddWorkflowExecutionSignaled", signalName, signalInput, signalIdentity).Return(&shared.HistoryEvent{
		EventType: shared.EventTypeWorkflowExecutionSignaled.Ptr(),
		Timestamp: common.Int64Ptr(time.Now().UnixNano()),
		WorkflowExecutionSignaledEventAttributes: &shared.WorkflowExecutionSignaledEventAttributes{
			SignalName: common.StringPtr(signalName),
			Input:      signalInput,
			Identity:   common.StringPtr(signalIdentity),
		},
	}, nil).Once()
	msBuilderCurrent.On("HasPendingDecision").Return(false)

	newDecision := &decisionInfo{
		Version:    currentLastWriteVersion,
		ScheduleID: 1234,
		StartedID:  common.EmptyEventID,
		TaskList:   decisionStickyTasklist,
		Attempt:    0,
	}
	msBuilderCurrent.On("AddDecisionTaskScheduledEvent", false).Return(newDecision, nil)

	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", currentLastWriteVersion).Return(cluster.TestCurrentClusterName)
	s.mockClusterMetadata.On("GetCurrentClusterName").Return(cluster.TestCurrentClusterName)

	contextCurrent.On("updateWorkflowExecutionAsActive", mock.Anything).Return(nil).Once()

	msBuilderOut, err := s.historyReplicator.ApplyOtherEventsVersionChecking(ctx.Background(), context, msBuilderIn,
		request, s.logger)
	s.Nil(msBuilderOut)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestApplyOtherEventsVersionChecking_IncomingLessThanCurrent_WorkflowRunning_NoEventsReapplication() {
	incomingVersion := int64(110)
	currentLastWriteVersion := int64(123)

	context := &mockWorkflowExecutionContext{}
	defer context.AssertExpectations(s.T())
	msBuilderIn := &mockMutableState{}
	defer msBuilderIn.AssertExpectations(s.T())
	request := &h.ReplicateEventsRequest{
		Version: common.Int64Ptr(incomingVersion),
		History: &shared.History{Events: []*shared.HistoryEvent{
			{
				EventType: shared.EventTypeWorkflowExecutionCanceled.Ptr(),
				Timestamp: common.Int64Ptr(time.Now().UnixNano()),
			},
		}},
	}
	msBuilderIn.On("IsWorkflowExecutionRunning").Return(true)
	msBuilderIn.On("GetReplicationState").Return(&persistence.ReplicationState{LastWriteVersion: currentLastWriteVersion})

	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", currentLastWriteVersion).Return(cluster.TestCurrentClusterName)
	s.mockClusterMetadata.On("GetCurrentClusterName").Return(cluster.TestCurrentClusterName)

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

	context := &mockWorkflowExecutionContext{}
	defer context.AssertExpectations(s.T())
	msBuilderIn := &mockMutableState{}
	defer msBuilderIn.AssertExpectations(s.T())
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
	msBuilderIn.On("GetReplicationState").Return(&persistence.ReplicationState{LastWriteVersion: currentLastWriteVersion})
	msBuilderIn.On("GetLastWriteVersion").Return(currentLastWriteVersion, nil)
	msBuilderIn.On("IsWorkflowExecutionRunning").Return(true)
	msBuilderIn.On("UpdateReplicationStateVersion", currentLastWriteVersion, true).Once()
	msBuilderIn.On("AddWorkflowExecutionSignaled", signalName, signalInput, signalIdentity).Return(&shared.HistoryEvent{
		EventType: shared.EventTypeWorkflowExecutionSignaled.Ptr(),
		Timestamp: common.Int64Ptr(time.Now().UnixNano()),
		WorkflowExecutionSignaledEventAttributes: &shared.WorkflowExecutionSignaledEventAttributes{
			SignalName: common.StringPtr(signalName),
			Input:      signalInput,
			Identity:   common.StringPtr(signalIdentity),
		},
	}, nil).Once()
	msBuilderIn.On("HasPendingDecision").Return(true)

	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", currentLastWriteVersion).Return(cluster.TestCurrentClusterName)
	s.mockClusterMetadata.On("GetCurrentClusterName").Return(cluster.TestCurrentClusterName)

	context.On("updateWorkflowExecutionAsActive", mock.Anything).Return(nil).Once()
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

	context := &mockWorkflowExecutionContext{}
	defer context.AssertExpectations(s.T())
	msBuilderIn := &mockMutableState{}
	defer msBuilderIn.AssertExpectations(s.T())
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
	msBuilderIn.On("GetReplicationState").Return(&persistence.ReplicationState{LastWriteVersion: currentLastWriteVersion})
	msBuilderIn.On("GetLastWriteVersion").Return(currentLastWriteVersion, nil)
	msBuilderIn.On("IsWorkflowExecutionRunning").Return(true)
	msBuilderIn.On("UpdateReplicationStateVersion", currentLastWriteVersion, true).Once()
	msBuilderIn.On("AddWorkflowExecutionSignaled", signalName, signalInput, signalIdentity).Return(&shared.HistoryEvent{
		EventType: shared.EventTypeWorkflowExecutionSignaled.Ptr(),
		Timestamp: common.Int64Ptr(time.Now().UnixNano()),
		WorkflowExecutionSignaledEventAttributes: &shared.WorkflowExecutionSignaledEventAttributes{
			SignalName: common.StringPtr(signalName),
			Input:      signalInput,
			Identity:   common.StringPtr(signalIdentity),
		},
	}, nil).Once()
	msBuilderIn.On("HasPendingDecision").Return(false)

	newDecision := &decisionInfo{
		Version:    currentLastWriteVersion,
		ScheduleID: 1234,
		StartedID:  common.EmptyEventID,
		TaskList:   decisionStickyTasklist,
		Attempt:    0,
	}
	msBuilderIn.On("AddDecisionTaskScheduledEvent", false).Return(newDecision, nil)

	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", currentLastWriteVersion).Return(cluster.TestCurrentClusterName)
	s.mockClusterMetadata.On("GetCurrentClusterName").Return(cluster.TestCurrentClusterName)

	context.On("updateWorkflowExecutionAsActive", mock.Anything).Return(nil).Once()

	msBuilderOut, err := s.historyReplicator.ApplyOtherEventsVersionChecking(ctx.Background(), context, msBuilderIn,
		request, s.logger)
	s.Nil(msBuilderOut)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestApplyOtherEventsVersionChecking_IncomingEqualToCurrent() {
	incomingVersion := int64(110)
	currentLastWriteVersion := incomingVersion

	context := &mockWorkflowExecutionContext{}
	defer context.AssertExpectations(s.T())
	msBuilderIn := &mockMutableState{}
	defer msBuilderIn.AssertExpectations(s.T())
	request := &h.ReplicateEventsRequest{
		Version: common.Int64Ptr(incomingVersion),
		History: &shared.History{Events: []*shared.HistoryEvent{
			{Timestamp: common.Int64Ptr(time.Now().UnixNano())},
		}},
	}
	msBuilderIn.On("GetReplicationState").Return(&persistence.ReplicationState{LastWriteVersion: currentLastWriteVersion})

	msBuilderOut, err := s.historyReplicator.ApplyOtherEventsVersionChecking(ctx.Background(), context, msBuilderIn, request, s.logger)
	s.Equal(msBuilderIn, msBuilderOut)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestApplyOtherEventsVersionChecking_IncomingGreaterThanCurrent_CurrentWasNotActive_SameCluster() {
	currentLastWriteVersion := int64(10)
	incomingVersion := currentLastWriteVersion + 10

	prevActiveCluster := cluster.TestAlternativeClusterName
	context := &mockWorkflowExecutionContext{}
	defer context.AssertExpectations(s.T())
	msBuilderIn := &mockMutableState{}
	defer msBuilderIn.AssertExpectations(s.T())

	request := &h.ReplicateEventsRequest{
		Version: common.Int64Ptr(incomingVersion),
		History: &shared.History{Events: []*shared.HistoryEvent{
			{Timestamp: common.Int64Ptr(time.Now().UnixNano())},
		}},
	}
	msBuilderIn.On("GetReplicationState").Return(&persistence.ReplicationState{
		LastWriteVersion: currentLastWriteVersion,
	})
	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", currentLastWriteVersion).Return(prevActiveCluster)
	s.mockClusterMetadata.On("IsVersionFromSameCluster", incomingVersion, currentLastWriteVersion).Return(true)

	msBuilderOut, err := s.historyReplicator.ApplyOtherEventsVersionChecking(ctx.Background(), context, msBuilderIn,
		request, s.logger)
	s.Equal(msBuilderIn, msBuilderOut)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestApplyOtherEventsVersionChecking_IncomingGreaterThanCurrent_CurrentWasNotActive_DiffCluster() {
	currentLastWriteVersion := int64(10)
	incomingVersion := currentLastWriteVersion + 10

	prevActiveCluster := cluster.TestAlternativeClusterName
	context := &mockWorkflowExecutionContext{}
	defer context.AssertExpectations(s.T())
	msBuilderIn := &mockMutableState{}
	defer msBuilderIn.AssertExpectations(s.T())

	request := &h.ReplicateEventsRequest{
		Version: common.Int64Ptr(incomingVersion),
		History: &shared.History{Events: []*shared.HistoryEvent{
			{Timestamp: common.Int64Ptr(time.Now().UnixNano())},
		}},
	}
	msBuilderIn.On("GetReplicationState").Return(&persistence.ReplicationState{
		LastWriteVersion: currentLastWriteVersion,
	})
	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", currentLastWriteVersion).Return(prevActiveCluster)
	s.mockClusterMetadata.On("IsVersionFromSameCluster", incomingVersion, currentLastWriteVersion).Return(false)

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
	context := &mockWorkflowExecutionContext{}
	defer context.AssertExpectations(s.T())
	msBuilderIn := &mockMutableState{}
	defer msBuilderIn.AssertExpectations(s.T())

	request := &h.ReplicateEventsRequest{
		Version:         common.Int64Ptr(incomingVersion),
		ReplicationInfo: map[string]*shared.ReplicationInfo{},
		History: &shared.History{Events: []*shared.HistoryEvent{
			{Timestamp: common.Int64Ptr(time.Now().UnixNano())},
		}},
	}
	startTimeStamp := time.Now()
	msBuilderIn.On("GetReplicationState").Return(&persistence.ReplicationState{
		LastWriteVersion: currentLastWriteVersion,
		LastWriteEventID: currentLastEventID,
		LastReplicationInfo: map[string]*persistence.ReplicationInfo{
			incomingActiveCluster: {
				Version:     currentReplicationInfoLastWriteVersion,
				LastEventID: currentReplicationInfoLastEventID,
			},
		},
	})
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
	msBuilderIn.On("GetExecutionInfo").Return(exeInfo)
	msBuilderIn.On("IsWorkflowExecutionRunning").Return(currentState != persistence.WorkflowStateCompleted)
	msBuilderIn.On("GetLastWriteVersion").Return(currentLastWriteVersion, nil)
	msBuilderIn.On("GetUpdateCondition").Return(updateCondition)
	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", currentLastWriteVersion).Return(prevActiveCluster)

	mockConflictResolver := &mockConflictResolver{}
	s.historyReplicator.getNewConflictResolver = func(context workflowExecutionContext, logger log.Logger) conflictResolver {
		return mockConflictResolver
	}
	msBuilderMid := &mockMutableState{}
	msBuilderMid.On("GetNextEventID").Return(int64(12345)) // this is used by log
	mockConflictResolver.On("reset",
		runID, currentLastWriteVersion, currentState, mock.Anything, currentReplicationInfoLastEventID, exeInfo, updateCondition,
	).Return(msBuilderMid, nil)
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
	context := &mockWorkflowExecutionContext{}
	defer context.AssertExpectations(s.T())
	msBuilderIn := &mockMutableState{}
	defer msBuilderIn.AssertExpectations(s.T())

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
	msBuilderIn.On("GetReplicationState").Return(&persistence.ReplicationState{
		LastWriteVersion: currentLastWriteVersion,
		LastWriteEventID: currentLastEventID,
		LastReplicationInfo: map[string]*persistence.ReplicationInfo{
			incomingActiveCluster: {
				Version:     currentReplicationInfoLastWriteVersion,
				LastEventID: currentReplicationInfoLastEventID,
			},
		},
	})
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
	msBuilderIn.On("GetExecutionInfo").Return(exeInfo)
	msBuilderIn.On("IsWorkflowExecutionRunning").Return(currentState != persistence.WorkflowStateCompleted)
	msBuilderIn.On("GetLastWriteVersion").Return(currentLastWriteVersion, nil)
	msBuilderIn.On("GetUpdateCondition").Return(updateCondition)
	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", currentLastWriteVersion).Return(prevActiveCluster)

	mockConflictResolver := &mockConflictResolver{}
	s.historyReplicator.getNewConflictResolver = func(context workflowExecutionContext, logger log.Logger) conflictResolver {
		return mockConflictResolver
	}
	msBuilderMid := &mockMutableState{}
	msBuilderMid.On("GetNextEventID").Return(int64(12345)) // this is used by log
	mockConflictResolver.On("reset",
		runID, currentLastWriteVersion, currentState, mock.Anything, currentReplicationInfoLastEventID, exeInfo, updateCondition,
	).Return(msBuilderMid, nil)
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
	context := &mockWorkflowExecutionContext{}
	defer context.AssertExpectations(s.T())
	msBuilderIn := &mockMutableState{}
	defer msBuilderIn.AssertExpectations(s.T())

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
	msBuilderIn.On("GetReplicationState").Return(&persistence.ReplicationState{
		LastWriteVersion: currentLastWriteVersion,
		LastWriteEventID: currentLastEventID,
	})
	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", currentLastWriteVersion).Return(prevActiveCluster)

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
	context := &mockWorkflowExecutionContext{}
	defer context.AssertExpectations(s.T())
	msBuilderIn := &mockMutableState{}
	defer msBuilderIn.AssertExpectations(s.T())

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
	msBuilderIn.On("GetReplicationState").Return(&persistence.ReplicationState{
		LastWriteVersion: currentLastWriteVersion,
		LastWriteEventID: currentLastEventID,
	})
	msBuilderIn.On("HasBufferedEvents").Return(false)
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
	msBuilderIn.On("GetExecutionInfo").Return(exeInfo)
	msBuilderIn.On("IsWorkflowExecutionRunning").Return(currentState != persistence.WorkflowStateCompleted)
	msBuilderIn.On("GetLastWriteVersion").Return(currentLastWriteVersion, nil)
	msBuilderIn.On("GetUpdateCondition").Return(updateCondition)
	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", currentLastWriteVersion).Return(prevActiveCluster)

	mockConflictResolver := &mockConflictResolver{}
	s.historyReplicator.getNewConflictResolver = func(context workflowExecutionContext, logger log.Logger) conflictResolver {
		return mockConflictResolver
	}
	msBuilderMid := &mockMutableState{}
	msBuilderMid.On("GetNextEventID").Return(int64(12345)) // this is used by log
	mockConflictResolver.On("reset",
		runID, currentLastWriteVersion, currentState, mock.Anything, incomingReplicationInfoLastEventID, exeInfo, updateCondition,
	).Return(msBuilderMid, nil)
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
	context := &mockWorkflowExecutionContext{}
	defer context.AssertExpectations(s.T())
	msBuilderIn := &mockMutableState{}
	defer msBuilderIn.AssertExpectations(s.T())

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
	msBuilderIn.On("GetReplicationState").Return(&persistence.ReplicationState{
		LastWriteVersion: currentLastWriteVersion,
		LastWriteEventID: currentLastEventID,
	})
	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", currentLastWriteVersion).Return(prevActiveCluster)

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
	context := &mockWorkflowExecutionContext{}
	defer context.AssertExpectations(s.T())
	msBuilderIn := &mockMutableState{}
	defer msBuilderIn.AssertExpectations(s.T())

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

	msBuilderIn.On("HasBufferedEvents").Return(false)
	msBuilderIn.On("GetReplicationState").Return(&persistence.ReplicationState{
		LastWriteVersion: currentLastWriteVersion,
		LastWriteEventID: currentLastEventID,
	})
	msBuilderIn.On("IsWorkflowExecutionRunning").Return(true)
	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", currentLastWriteVersion).Return(prevActiveCluster)

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
	context := &mockWorkflowExecutionContext{}
	defer context.AssertExpectations(s.T())
	msBuilderIn := &mockMutableState{}
	defer msBuilderIn.AssertExpectations(s.T())

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
	msBuilderIn.On("GetLastWriteVersion").Return(currentLastWriteVersion, nil)
	msBuilderIn.On("GetReplicationState").Return(&persistence.ReplicationState{
		LastWriteVersion: currentLastWriteVersion,
		LastWriteEventID: currentLastEventID,
	}).Once()
	msBuilderIn.On("HasBufferedEvents").Return(true).Once()
	msBuilderIn.On("GetInFlightDecision").Return(pendingDecisionInfo, true)
	msBuilderIn.On("UpdateReplicationStateVersion", currentLastWriteVersion, true).Once()
	msBuilderIn.On("AddDecisionTaskFailedEvent", pendingDecisionInfo.ScheduleID, pendingDecisionInfo.StartedID,
		workflow.DecisionTaskFailedCauseFailoverCloseDecision, ([]byte)(nil), identityHistoryService, "", "", "", int64(0)).Return(&shared.HistoryEvent{}, nil).Once()
	msBuilderIn.On("HasPendingDecision").Return(false)
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
	msBuilderIn.On("GetExecutionInfo").Return(exeInfo)
	newDecision := &decisionInfo{
		Version:    currentLastWriteVersion,
		ScheduleID: currentLastEventID + 2,
		StartedID:  common.EmptyEventID,
		TaskList:   decisionStickyTasklist,
		Attempt:    0,
	}
	msBuilderIn.On("AddDecisionTaskScheduledEvent", false).Return(newDecision, nil)

	context.On("updateWorkflowExecutionAsActive", mock.Anything).Return(nil).Once()

	// after the flush, the pending buffered events are gone, however, the last event ID should increase
	msBuilderIn.On("GetReplicationState").Return(&persistence.ReplicationState{
		LastWriteVersion: currentLastWriteVersion,
		LastWriteEventID: currentLastEventID + 2,
	}).Once()
	msBuilderIn.On("IsWorkflowExecutionRunning").Return(currentState != persistence.WorkflowStateCompleted)
	msBuilderIn.On("GetLastWriteVersion").Return(currentLastWriteVersion, nil)
	msBuilderIn.On("GetUpdateCondition").Return(updateCondition)
	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", currentLastWriteVersion).Return(prevActiveCluster)

	mockConflictResolver := &mockConflictResolver{}
	s.historyReplicator.getNewConflictResolver = func(context workflowExecutionContext, logger log.Logger) conflictResolver {
		return mockConflictResolver
	}
	msBuilderMid := &mockMutableState{}
	msBuilderMid.On("GetNextEventID").Return(int64(12345)) // this is used by log
	mockConflictResolver.On("reset",
		runID, currentLastWriteVersion, currentState, mock.Anything, incomingReplicationInfoLastEventID, exeInfo, updateCondition,
	).Return(msBuilderMid, nil)
	msBuilderOut, err := s.historyReplicator.ApplyOtherEventsVersionChecking(ctx.Background(), context, msBuilderIn, request, s.logger)
	s.Equal(msBuilderMid, msBuilderOut)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestApplyOtherEvents_IncomingLessThanCurrent() {
	currentNextEventID := int64(10)
	incomingFirstEventID := currentNextEventID - 4

	context := &mockWorkflowExecutionContext{}
	defer context.AssertExpectations(s.T())
	msBuilder := &mockMutableState{}
	defer msBuilder.AssertExpectations(s.T())

	request := &h.ReplicateEventsRequest{
		FirstEventId: common.Int64Ptr(incomingFirstEventID),
		History:      &shared.History{},
	}
	msBuilder.On("GetNextEventID").Return(currentNextEventID)
	msBuilder.On("GetReplicationState").Return(&persistence.ReplicationState{}) // logger will use this

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

	context := &mockWorkflowExecutionContext{}
	defer context.AssertExpectations(s.T())
	context.On("getDomainID").Return(domainID)
	context.On("getExecution").Return(&workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(runID),
	})

	msBuilder := &mockMutableState{}
	defer msBuilder.AssertExpectations(s.T())

	request := &h.ReplicateEventsRequest{
		SourceCluster: common.StringPtr(incomingSourceCluster),
		Version:       common.Int64Ptr(incomingVersion),
		FirstEventId:  common.Int64Ptr(incomingFirstEventID),
		NextEventId:   common.Int64Ptr(incomingNextEventID),
		History:       &shared.History{},
	}

	msBuilder.On("GetNextEventID").Return(currentNextEventID)
	msBuilder.On("IsWorkflowExecutionRunning").Return(true)

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

	context := &mockWorkflowExecutionContext{}
	defer context.AssertExpectations(s.T())
	msBuilder := &mockMutableState{}
	defer msBuilder.AssertExpectations(s.T())

	request := &h.ReplicateEventsRequest{
		SourceCluster:     common.StringPtr(incomingSourceCluster),
		Version:           common.Int64Ptr(incomingVersion),
		FirstEventId:      common.Int64Ptr(incomingFirstEventID),
		NextEventId:       common.Int64Ptr(incomingNextEventID),
		ForceBufferEvents: common.BoolPtr(true),
		History:           &shared.History{Events: []*shared.HistoryEvent{{}}},
	}

	msBuilder.On("IsWorkflowExecutionRunning").Return(false)

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
	msBuilder := &mockMutableState{}
	defer msBuilder.AssertExpectations(s.T())

	di := &decisionInfo{
		Version:         version,
		ScheduleID:      common.FirstEventID + 1,
		StartedID:       common.EmptyEventID,
		DecisionTimeout: decisionTimeout,
		TaskList:        tasklist,
	}
	sBuilder := &mockStateBuilder{}
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

	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", version).Return(cluster.TestAlternativeClusterName)
	historySize := 111
	executionInfo := &persistence.WorkflowExecutionInfo{
		CreateRequestID:      requestID,
		DomainID:             domainID,
		WorkflowID:           workflowID,
		RunID:                runID,
		ParentDomainID:       parentDomainID,
		ParentWorkflowID:     parentWorkflowID,
		ParentRunID:          parentRunID,
		InitiatedID:          initiatedID,
		TaskList:             tasklist,
		WorkflowTypeName:     workflowType,
		WorkflowTimeout:      workflowTimeout,
		DecisionTimeoutValue: decisionTimeout,
		NextEventID:          nextEventID,
		LastProcessedEvent:   common.EmptyEventID,
		BranchToken:          []byte("some random branch token"),
		DecisionVersion:      di.Version,
		DecisionScheduleID:   di.ScheduleID,
		DecisionStartedID:    di.StartedID,
		DecisionTimeout:      di.DecisionTimeout,
		State:                persistence.WorkflowStateRunning,
		CloseStatus:          persistence.WorkflowCloseStatusNone,
	}
	msBuilder.On("GetExecutionInfo").Return(executionInfo)
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
	msBuilder.On("CloseTransactionAsSnapshot", now.Local(), transactionPolicyPassive).Return(newWorkflowSnapshot, newWorkflowEventsSeq, nil)
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
	s.mockDomainCache.On("GetDomainByID", domainID).Return(
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
	)

	err := s.historyReplicator.replicateWorkflowStarted(ctx.Background(), context, msBuilder, history, sBuilder, s.logger)
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
	msBuilder := &mockMutableState{}
	defer msBuilder.AssertExpectations(s.T())

	di := &decisionInfo{
		Version:         version,
		ScheduleID:      common.FirstEventID + 1,
		StartedID:       common.EmptyEventID,
		DecisionTimeout: decisionTimeout,
		TaskList:        tasklist,
	}
	sBuilder := &mockStateBuilder{}
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

	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", version).Return(cluster.TestAlternativeClusterName)
	historySize := 111
	executionInfo := &persistence.WorkflowExecutionInfo{
		CreateRequestID:      requestID,
		DomainID:             domainID,
		WorkflowID:           workflowID,
		RunID:                runID,
		ParentDomainID:       parentDomainID,
		ParentWorkflowID:     parentWorkflowID,
		ParentRunID:          parentRunID,
		InitiatedID:          initiatedID,
		TaskList:             tasklist,
		WorkflowTypeName:     workflowType,
		WorkflowTimeout:      workflowTimeout,
		DecisionTimeoutValue: decisionTimeout,
		NextEventID:          nextEventID,
		LastProcessedEvent:   common.EmptyEventID,
		BranchToken:          []byte("some random branch token"),
		DecisionVersion:      di.Version,
		DecisionScheduleID:   di.ScheduleID,
		DecisionStartedID:    di.StartedID,
		DecisionTimeout:      di.DecisionTimeout,
		State:                persistence.WorkflowStateRunning,
		CloseStatus:          persistence.WorkflowCloseStatusNone,
	}
	msBuilder.On("GetExecutionInfo").Return(executionInfo)
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
	msBuilder.On("CloseTransactionAsSnapshot", now.Local(), transactionPolicyPassive).Return(newWorkflowSnapshot, newWorkflowEventsSeq, nil)
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: historySize}, nil).Once()
	errRet := &shared.InternalServiceError{}
	// the test above already assert the create workflow request, so here just use anyting
	s.mockExecutionMgr.On("CreateWorkflowExecution", mock.Anything).Return(nil, errRet)
	s.mockShardManager.On("UpdateShard", mock.Anything).Return(nil) // this is called when err is returned, and shard will try to update

	s.mockDomainCache.On("GetDomainByID", domainID).Return(
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
	)

	err := s.historyReplicator.replicateWorkflowStarted(ctx.Background(), context, msBuilder, history, sBuilder, s.logger)
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
	msBuilder := &mockMutableState{}
	defer msBuilder.AssertExpectations(s.T())

	di := &decisionInfo{
		Version:         version,
		ScheduleID:      common.FirstEventID + 1,
		StartedID:       common.EmptyEventID,
		DecisionTimeout: decisionTimeout,
		TaskList:        tasklist,
	}
	sBuilder := &mockStateBuilder{}
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

	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", version).Return(cluster.TestAlternativeClusterName)
	historySize := 111
	executionInfo := &persistence.WorkflowExecutionInfo{
		CreateRequestID:      requestID,
		DomainID:             domainID,
		WorkflowID:           workflowID,
		RunID:                runID,
		ParentDomainID:       parentDomainID,
		ParentWorkflowID:     parentWorkflowID,
		ParentRunID:          parentRunID,
		InitiatedID:          initiatedID,
		TaskList:             tasklist,
		WorkflowTypeName:     workflowType,
		WorkflowTimeout:      workflowTimeout,
		DecisionTimeoutValue: decisionTimeout,
		NextEventID:          nextEventID,
		LastProcessedEvent:   common.EmptyEventID,
		BranchToken:          []byte("some random branch token"),
		DecisionVersion:      di.Version,
		DecisionScheduleID:   di.ScheduleID,
		DecisionStartedID:    di.StartedID,
		DecisionTimeout:      di.DecisionTimeout,
		State:                persistence.WorkflowStateRunning,
		CloseStatus:          persistence.WorkflowCloseStatusNone,
	}
	msBuilder.On("GetExecutionInfo").Return(executionInfo)
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
	msBuilder.On("CloseTransactionAsSnapshot", now.Local(), transactionPolicyPassive).Return(newWorkflowSnapshot, newWorkflowEventsSeq, nil)
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
	s.mockDomainCache.On("GetDomainByID", domainID).Return(
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
	)

	err := s.historyReplicator.replicateWorkflowStarted(ctx.Background(), context, msBuilder, history, sBuilder, s.logger)
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
	msBuilder := &mockMutableState{}
	defer msBuilder.AssertExpectations(s.T())

	di := &decisionInfo{
		Version:         version,
		ScheduleID:      common.FirstEventID + 1,
		StartedID:       common.EmptyEventID,
		DecisionTimeout: decisionTimeout,
		TaskList:        tasklist,
	}
	sBuilder := &mockStateBuilder{}
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

	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", version).Return(cluster.TestAlternativeClusterName)
	historySize := 111
	executionInfo := &persistence.WorkflowExecutionInfo{
		CreateRequestID:      requestID,
		DomainID:             domainID,
		WorkflowID:           workflowID,
		RunID:                runID,
		ParentDomainID:       parentDomainID,
		ParentWorkflowID:     parentWorkflowID,
		ParentRunID:          parentRunID,
		InitiatedID:          initiatedID,
		TaskList:             tasklist,
		WorkflowTypeName:     workflowType,
		WorkflowTimeout:      workflowTimeout,
		DecisionTimeoutValue: decisionTimeout,
		NextEventID:          nextEventID,
		LastProcessedEvent:   common.EmptyEventID,
		BranchToken:          []byte("some random branch token"),
		DecisionVersion:      di.Version,
		DecisionScheduleID:   di.ScheduleID,
		DecisionStartedID:    di.StartedID,
		DecisionTimeout:      di.DecisionTimeout,
		State:                persistence.WorkflowStateRunning,
		CloseStatus:          persistence.WorkflowCloseStatusNone,
		CronSchedule:         cronSchedule,
		HasRetryPolicy:       true,
		InitialInterval:      retryPolicy.GetInitialIntervalInSeconds(),
		BackoffCoefficient:   retryPolicy.GetBackoffCoefficient(),
		ExpirationSeconds:    retryPolicy.GetExpirationIntervalInSeconds(),
		MaximumAttempts:      retryPolicy.GetMaximumAttempts(),
		MaximumInterval:      retryPolicy.GetMaximumIntervalInSeconds(),
		NonRetriableErrors:   retryPolicy.GetNonRetriableErrorReasons(),
	}
	msBuilder.On("GetExecutionInfo").Return(executionInfo)
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
	msBuilder.On("CloseTransactionAsSnapshot", now.Local(), transactionPolicyPassive).Return(newWorkflowSnapshot, newWorkflowEventsSeq, nil)
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
	s.mockDomainCache.On("GetDomainByID", domainID).Return(
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
	)

	err := s.historyReplicator.replicateWorkflowStarted(ctx.Background(), context, msBuilder, history, sBuilder, s.logger)
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
	msBuilder := &mockMutableState{}
	defer msBuilder.AssertExpectations(s.T())

	di := &decisionInfo{
		Version:         version,
		ScheduleID:      common.FirstEventID + 1,
		StartedID:       common.EmptyEventID,
		DecisionTimeout: decisionTimeout,
		TaskList:        tasklist,
	}
	sBuilder := &mockStateBuilder{}
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

	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", version).Return(cluster.TestAlternativeClusterName)
	historySize := 111
	executionInfo := &persistence.WorkflowExecutionInfo{
		CreateRequestID:      requestID,
		DomainID:             domainID,
		WorkflowID:           workflowID,
		RunID:                runID,
		ParentDomainID:       parentDomainID,
		ParentWorkflowID:     parentWorkflowID,
		ParentRunID:          parentRunID,
		InitiatedID:          initiatedID,
		TaskList:             tasklist,
		WorkflowTypeName:     workflowType,
		WorkflowTimeout:      workflowTimeout,
		DecisionTimeoutValue: decisionTimeout,
		NextEventID:          nextEventID,
		LastProcessedEvent:   common.EmptyEventID,
		BranchToken:          []byte("some random branch token"),
		DecisionVersion:      di.Version,
		DecisionScheduleID:   di.ScheduleID,
		DecisionStartedID:    di.StartedID,
		DecisionTimeout:      di.DecisionTimeout,
		State:                persistence.WorkflowStateRunning,
		CloseStatus:          persistence.WorkflowCloseStatusNone,
	}
	msBuilder.On("GetExecutionInfo").Return(executionInfo)
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
	msBuilder.On("CloseTransactionAsSnapshot", now.Local(), transactionPolicyPassive).Return(newWorkflowSnapshot, newWorkflowEventsSeq, nil)
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
	s.mockDomainCache.On("GetDomainByID", domainID).Return(
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
	)

	err := s.historyReplicator.replicateWorkflowStarted(ctx.Background(), context, msBuilder, history, sBuilder, s.logger)
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
	msBuilder := &mockMutableState{}
	defer msBuilder.AssertExpectations(s.T())

	di := &decisionInfo{
		Version:         version,
		ScheduleID:      common.FirstEventID + 1,
		StartedID:       common.EmptyEventID,
		DecisionTimeout: decisionTimeout,
		TaskList:        tasklist,
	}
	sBuilder := &mockStateBuilder{}
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

	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", version).Return(cluster.TestAlternativeClusterName)
	historySize := 111
	executionInfo := &persistence.WorkflowExecutionInfo{
		CreateRequestID:      requestID,
		DomainID:             domainID,
		WorkflowID:           workflowID,
		RunID:                runID,
		ParentDomainID:       parentDomainID,
		ParentWorkflowID:     parentWorkflowID,
		ParentRunID:          parentRunID,
		InitiatedID:          initiatedID,
		TaskList:             tasklist,
		WorkflowTypeName:     workflowType,
		WorkflowTimeout:      workflowTimeout,
		DecisionTimeoutValue: decisionTimeout,
		NextEventID:          nextEventID,
		LastProcessedEvent:   common.EmptyEventID,
		BranchToken:          []byte("some random branch token"),
		DecisionVersion:      di.Version,
		DecisionScheduleID:   di.ScheduleID,
		DecisionStartedID:    di.StartedID,
		DecisionTimeout:      di.DecisionTimeout,
		State:                persistence.WorkflowStateRunning,
		CloseStatus:          persistence.WorkflowCloseStatusNone,
	}
	msBuilder.On("GetExecutionInfo").Return(executionInfo)
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
	msBuilder.On("CloseTransactionAsSnapshot", now.Local(), transactionPolicyPassive).Return(newWorkflowSnapshot, newWorkflowEventsSeq, nil)
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
	s.mockDomainCache.On("GetDomainByID", domainID).Return(
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
	)

	err := s.historyReplicator.replicateWorkflowStarted(ctx.Background(), context, msBuilder, history, sBuilder, s.logger)
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
	msBuilder := &mockMutableState{}
	defer msBuilder.AssertExpectations(s.T())

	di := &decisionInfo{
		Version:         version,
		ScheduleID:      common.FirstEventID + 1,
		StartedID:       common.EmptyEventID,
		DecisionTimeout: decisionTimeout,
		TaskList:        tasklist,
	}
	sBuilder := &mockStateBuilder{}
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

	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", version).Return(cluster.TestAlternativeClusterName)
	historySize := 111
	executionInfo := &persistence.WorkflowExecutionInfo{
		CreateRequestID:      requestID,
		DomainID:             domainID,
		WorkflowID:           workflowID,
		RunID:                runID,
		ParentDomainID:       parentDomainID,
		ParentWorkflowID:     parentWorkflowID,
		ParentRunID:          parentRunID,
		InitiatedID:          initiatedID,
		TaskList:             tasklist,
		WorkflowTypeName:     workflowType,
		WorkflowTimeout:      workflowTimeout,
		DecisionTimeoutValue: decisionTimeout,
		NextEventID:          nextEventID,
		LastProcessedEvent:   common.EmptyEventID,
		BranchToken:          []byte("some random branch token"),
		DecisionVersion:      di.Version,
		DecisionScheduleID:   di.ScheduleID,
		DecisionStartedID:    di.StartedID,
		DecisionTimeout:      di.DecisionTimeout,
		State:                persistence.WorkflowStateRunning,
		CloseStatus:          persistence.WorkflowCloseStatusNone,
	}
	msBuilder.On("GetCurrentBranchToken").Return(executionInfo.BranchToken, nil)
	msBuilder.On("GetExecutionInfo").Return(executionInfo)
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
	msBuilder.On("CloseTransactionAsSnapshot", now.Local(), transactionPolicyPassive).Return(newWorkflowSnapshot, newWorkflowEventsSeq, nil)
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
	s.mockDomainCache.On("GetDomainByID", domainID).Return(
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
	)

	contextCurrent := &mockWorkflowExecutionContext{}
	defer contextCurrent.AssertExpectations(s.T())
	contextCurrent.On("lock", mock.Anything).Return(nil)
	contextCurrent.On("unlock")

	msBuilderCurrent := &mockMutableState{}
	defer msBuilderCurrent.AssertExpectations(s.T())

	contextCurrent.On("loadWorkflowExecution").Return(msBuilderCurrent, nil).Once()
	currentExecution := &shared.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(currentRunID),
	}
	contextCurrentCacheKey := definition.NewWorkflowIdentifier(domainID, currentExecution.GetWorkflowId(), currentExecution.GetRunId())
	s.historyReplicator.historyCache.PutIfNotExist(contextCurrentCacheKey, contextCurrent)

	s.mockExecutionMgr.On("GetCurrentExecution", &persistence.GetCurrentExecutionRequest{
		DomainID:   domainID,
		WorkflowID: workflowID,
	}).Return(&persistence.GetCurrentExecutionResponse{
		RunID: currentRunID,
		// other attributes are not used
	}, nil)

	err := s.historyReplicator.replicateWorkflowStarted(ctx.Background(), context, msBuilder, history, sBuilder, s.logger)
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
	msBuilder := &mockMutableState{}
	defer msBuilder.AssertExpectations(s.T())

	di := &decisionInfo{
		Version:         version,
		ScheduleID:      common.FirstEventID + 1,
		StartedID:       common.EmptyEventID,
		DecisionTimeout: decisionTimeout,
		TaskList:        tasklist,
	}
	sBuilder := &mockStateBuilder{}
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

	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", version).Return(cluster.TestAlternativeClusterName)
	historySize := 111
	executionInfo := &persistence.WorkflowExecutionInfo{
		CreateRequestID:      requestID,
		DomainID:             domainID,
		WorkflowID:           workflowID,
		RunID:                runID,
		ParentDomainID:       parentDomainID,
		ParentWorkflowID:     parentWorkflowID,
		ParentRunID:          parentRunID,
		InitiatedID:          initiatedID,
		TaskList:             tasklist,
		WorkflowTypeName:     workflowType,
		WorkflowTimeout:      workflowTimeout,
		DecisionTimeoutValue: decisionTimeout,
		NextEventID:          nextEventID,
		LastProcessedEvent:   common.EmptyEventID,
		BranchToken:          []byte("some random branch token"),
		DecisionVersion:      di.Version,
		DecisionScheduleID:   di.ScheduleID,
		DecisionStartedID:    di.StartedID,
		DecisionTimeout:      di.DecisionTimeout,
		State:                persistence.WorkflowStateRunning,
		CloseStatus:          persistence.WorkflowCloseStatusNone,
	}
	msBuilder.On("GetCurrentBranchToken").Return(executionInfo.BranchToken, nil)
	msBuilder.On("GetExecutionInfo").Return(executionInfo)
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
	msBuilder.On("CloseTransactionAsSnapshot", now.Local(), transactionPolicyPassive).Return(newWorkflowSnapshot, newWorkflowEventsSeq, nil)
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
	s.mockDomainCache.On("GetDomainByID", domainID).Return(
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
	)

	contextCurrent := &mockWorkflowExecutionContext{}
	defer contextCurrent.AssertExpectations(s.T())
	contextCurrent.On("lock", mock.Anything).Return(nil)
	contextCurrent.On("unlock")

	msBuilderCurrent := &mockMutableState{}
	defer msBuilderCurrent.AssertExpectations(s.T())

	contextCurrent.On("loadWorkflowExecution").Return(msBuilderCurrent, nil).Once()
	currentExecution := &shared.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(currentRunID),
	}
	contextCurrentCacheKey := definition.NewWorkflowIdentifier(domainID, currentExecution.GetWorkflowId(), currentExecution.GetRunId())
	s.historyReplicator.historyCache.PutIfNotExist(contextCurrentCacheKey, contextCurrent)

	s.mockExecutionMgr.On("GetCurrentExecution", &persistence.GetCurrentExecutionRequest{
		DomainID:   domainID,
		WorkflowID: workflowID,
	}).Return(&persistence.GetCurrentExecutionResponse{
		RunID: currentRunID,
		// other attributes are not used
	}, nil)

	msBuilderCurrent.On("IsWorkflowExecutionRunning").Return(true)
	msBuilderCurrent.On("GetLastWriteVersion").Return(currentVersion, nil)
	msBuilderCurrent.On("AddWorkflowExecutionSignaled", signalName, signalInput, signalIdentity).Return(&shared.HistoryEvent{
		EventType: shared.EventTypeWorkflowExecutionSignaled.Ptr(),
		Timestamp: common.Int64Ptr(time.Now().UnixNano()),
		WorkflowExecutionSignaledEventAttributes: &shared.WorkflowExecutionSignaledEventAttributes{
			SignalName: common.StringPtr(signalName),
			Input:      signalInput,
			Identity:   common.StringPtr(signalIdentity),
		},
	}, nil).Once()
	msBuilderCurrent.On("UpdateReplicationStateVersion", currentVersion, true).Once()
	msBuilderCurrent.On("HasPendingDecision").Return(true)
	contextCurrent.On("updateWorkflowExecutionAsActive", mock.Anything).Return(nil).Once()

	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", currentVersion).Return(cluster.TestCurrentClusterName)
	s.mockClusterMetadata.On("GetCurrentClusterName").Return(cluster.TestCurrentClusterName)

	err := s.historyReplicator.replicateWorkflowStarted(ctx.Background(), context, msBuilder, history, sBuilder, s.logger)
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
	msBuilder := &mockMutableState{}
	defer msBuilder.AssertExpectations(s.T())

	di := &decisionInfo{
		Version:         version,
		ScheduleID:      common.FirstEventID + 1,
		StartedID:       common.EmptyEventID,
		DecisionTimeout: decisionTimeout,
		TaskList:        tasklist,
	}
	sBuilder := &mockStateBuilder{}
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

	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", version).Return(cluster.TestAlternativeClusterName)
	historySize := 111
	executionInfo := &persistence.WorkflowExecutionInfo{
		CreateRequestID:      requestID,
		DomainID:             domainID,
		WorkflowID:           workflowID,
		RunID:                runID,
		ParentDomainID:       parentDomainID,
		ParentWorkflowID:     parentWorkflowID,
		ParentRunID:          parentRunID,
		InitiatedID:          initiatedID,
		TaskList:             tasklist,
		WorkflowTypeName:     workflowType,
		WorkflowTimeout:      workflowTimeout,
		DecisionTimeoutValue: decisionTimeout,
		NextEventID:          nextEventID,
		LastProcessedEvent:   common.EmptyEventID,
		BranchToken:          []byte("some random branch token"),
		DecisionVersion:      di.Version,
		DecisionScheduleID:   di.ScheduleID,
		DecisionStartedID:    di.StartedID,
		DecisionTimeout:      di.DecisionTimeout,
		State:                persistence.WorkflowStateRunning,
		CloseStatus:          persistence.WorkflowCloseStatusNone,
	}
	msBuilder.On("GetCurrentBranchToken").Return(executionInfo.BranchToken, nil)
	msBuilder.On("GetExecutionInfo").Return(executionInfo)
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
	msBuilder.On("CloseTransactionAsSnapshot", now.Local(), transactionPolicyPassive).Return(newWorkflowSnapshot, newWorkflowEventsSeq, nil)
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
	s.mockDomainCache.On("GetDomainByID", domainID).Return(
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
	)

	contextCurrent := &mockWorkflowExecutionContext{}
	defer contextCurrent.AssertExpectations(s.T())
	contextCurrent.On("lock", mock.Anything).Return(nil)
	contextCurrent.On("unlock")

	msBuilderCurrent := &mockMutableState{}
	defer msBuilderCurrent.AssertExpectations(s.T())

	contextCurrent.On("loadWorkflowExecution").Return(msBuilderCurrent, nil).Once()
	currentExecution := &shared.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(currentRunID),
	}
	contextCurrentCacheKey := definition.NewWorkflowIdentifier(domainID, currentExecution.GetWorkflowId(), currentExecution.GetRunId())
	s.historyReplicator.historyCache.PutIfNotExist(contextCurrentCacheKey, contextCurrent)

	s.mockExecutionMgr.On("GetCurrentExecution", &persistence.GetCurrentExecutionRequest{
		DomainID:   domainID,
		WorkflowID: workflowID,
	}).Return(&persistence.GetCurrentExecutionResponse{
		RunID: currentRunID,
		// other attributes are not used
	}, nil)

	msBuilderCurrent.On("IsWorkflowExecutionRunning").Return(true)
	msBuilderCurrent.On("GetLastWriteVersion").Return(currentVersion, nil)
	msBuilderCurrent.On("AddWorkflowExecutionSignaled", signalName, signalInput, signalIdentity).Return(&shared.HistoryEvent{
		EventType: shared.EventTypeWorkflowExecutionSignaled.Ptr(),
		Timestamp: common.Int64Ptr(time.Now().UnixNano()),
		WorkflowExecutionSignaledEventAttributes: &shared.WorkflowExecutionSignaledEventAttributes{
			SignalName: common.StringPtr(signalName),
			Input:      signalInput,
			Identity:   common.StringPtr(signalIdentity),
		},
	}, nil).Once()
	msBuilderCurrent.On("UpdateReplicationStateVersion", currentVersion, true).Once()
	msBuilderCurrent.On("HasPendingDecision").Return(false)

	newDecision := &decisionInfo{
		Version:    currentVersion,
		ScheduleID: 1234,
		StartedID:  common.EmptyEventID,
		TaskList:   currentDecisionStickyTasklist,
		Attempt:    0,
	}
	msBuilderCurrent.On("AddDecisionTaskScheduledEvent", false).Return(newDecision, nil)

	contextCurrent.On("updateWorkflowExecutionAsActive", mock.Anything).Return(nil).Once()

	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", currentVersion).Return(cluster.TestCurrentClusterName)
	s.mockClusterMetadata.On("GetCurrentClusterName").Return(cluster.TestCurrentClusterName)

	err := s.historyReplicator.replicateWorkflowStarted(ctx.Background(), context, msBuilder, history, sBuilder, s.logger)
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
	msBuilder := &mockMutableState{}
	defer msBuilder.AssertExpectations(s.T())

	di := &decisionInfo{
		Version:         version,
		ScheduleID:      common.FirstEventID + 1,
		StartedID:       common.EmptyEventID,
		DecisionTimeout: decisionTimeout,
		TaskList:        tasklist,
	}
	sBuilder := &mockStateBuilder{}
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

	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", version).Return(cluster.TestAlternativeClusterName)
	historySize := 111
	executionInfo := &persistence.WorkflowExecutionInfo{
		CreateRequestID:      requestID,
		DomainID:             domainID,
		WorkflowID:           workflowID,
		RunID:                runID,
		ParentDomainID:       parentDomainID,
		ParentWorkflowID:     parentWorkflowID,
		ParentRunID:          parentRunID,
		InitiatedID:          initiatedID,
		TaskList:             tasklist,
		WorkflowTypeName:     workflowType,
		WorkflowTimeout:      workflowTimeout,
		DecisionTimeoutValue: decisionTimeout,
		NextEventID:          nextEventID,
		LastProcessedEvent:   common.EmptyEventID,
		BranchToken:          []byte("some random branch token"),
		DecisionVersion:      di.Version,
		DecisionScheduleID:   di.ScheduleID,
		DecisionStartedID:    di.StartedID,
		DecisionTimeout:      di.DecisionTimeout,
		State:                persistence.WorkflowStateRunning,
		CloseStatus:          persistence.WorkflowCloseStatusNone,
	}
	msBuilder.On("GetExecutionInfo").Return(executionInfo)
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
	msBuilder.On("CloseTransactionAsSnapshot", now.Local(), transactionPolicyPassive).Return(newWorkflowSnapshot, newWorkflowEventsSeq, nil)
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

	contextCurrent := &mockWorkflowExecutionContext{}
	defer contextCurrent.AssertExpectations(s.T())
	contextCurrent.On("lock", mock.Anything).Return(nil)
	contextCurrent.On("unlock")

	msBuilderCurrent := &mockMutableState{}
	defer msBuilderCurrent.AssertExpectations(s.T())

	contextCurrent.On("loadWorkflowExecution").Return(msBuilderCurrent, nil).Once()
	currentExecution := &shared.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(currentRunID),
	}
	contextCurrentCacheKey := definition.NewWorkflowIdentifier(domainID, currentExecution.GetWorkflowId(), currentExecution.GetRunId())
	s.historyReplicator.historyCache.PutIfNotExist(contextCurrentCacheKey, contextCurrent)

	s.mockExecutionMgr.On("GetCurrentExecution", &persistence.GetCurrentExecutionRequest{
		DomainID:   domainID,
		WorkflowID: workflowID,
	}).Return(&persistence.GetCurrentExecutionResponse{
		RunID: currentRunID,
		// other attributes are not used
	}, nil)

	msBuilderCurrent.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{
		DomainID:           domainID,
		WorkflowID:         workflowID,
		RunID:              currentRunID,
		DecisionVersion:    common.EmptyVersion,
		DecisionScheduleID: common.EmptyEventID,
		DecisionStartedID:  common.EmptyEventID,
	})
	msBuilderCurrent.On("GetNextEventID").Return(currentNextEventID)

	s.mockDomainCache.On("GetDomainByID", domainID).Return(
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
	)

	err := s.historyReplicator.replicateWorkflowStarted(ctx.Background(), context, msBuilder, history, sBuilder, s.logger)
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
	msBuilder := &mockMutableState{}
	defer msBuilder.AssertExpectations(s.T())

	di := &decisionInfo{
		Version:         version,
		ScheduleID:      common.FirstEventID + 1,
		StartedID:       common.EmptyEventID,
		DecisionTimeout: decisionTimeout,
		TaskList:        tasklist,
	}
	sBuilder := &mockStateBuilder{}
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

	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", version).Return(cluster.TestAlternativeClusterName)
	historySize := 111
	executionInfo := &persistence.WorkflowExecutionInfo{
		CreateRequestID:      requestID,
		DomainID:             domainID,
		WorkflowID:           workflowID,
		RunID:                runID,
		ParentDomainID:       parentDomainID,
		ParentWorkflowID:     parentWorkflowID,
		ParentRunID:          parentRunID,
		InitiatedID:          initiatedID,
		TaskList:             tasklist,
		WorkflowTypeName:     workflowType,
		WorkflowTimeout:      workflowTimeout,
		DecisionTimeoutValue: decisionTimeout,
		NextEventID:          nextEventID,
		LastProcessedEvent:   common.EmptyEventID,
		BranchToken:          []byte("some random branch token"),
		DecisionVersion:      di.Version,
		DecisionScheduleID:   di.ScheduleID,
		DecisionStartedID:    di.StartedID,
		DecisionTimeout:      di.DecisionTimeout,
		State:                persistence.WorkflowStateRunning,
		CloseStatus:          persistence.WorkflowCloseStatusNone,
	}
	msBuilder.On("GetExecutionInfo").Return(executionInfo)
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
	msBuilder.On("CloseTransactionAsSnapshot", now.Local(), transactionPolicyPassive).Return(newWorkflowSnapshot, newWorkflowEventsSeq, nil)
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

	contextCurrent := &mockWorkflowExecutionContext{}
	defer contextCurrent.AssertExpectations(s.T())
	contextCurrent.On("lock", mock.Anything).Return(nil)
	contextCurrent.On("unlock")

	msBuilderCurrent := &mockMutableState{}
	defer msBuilderCurrent.AssertExpectations(s.T())

	contextCurrent.On("loadWorkflowExecution").Return(msBuilderCurrent, nil).Once()
	currentExecution := &shared.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(currentRunID),
	}
	contextCurrentCacheKey := definition.NewWorkflowIdentifier(domainID, currentExecution.GetWorkflowId(), currentExecution.GetRunId())
	s.historyReplicator.historyCache.PutIfNotExist(contextCurrentCacheKey, contextCurrent)

	s.mockExecutionMgr.On("GetCurrentExecution", &persistence.GetCurrentExecutionRequest{
		DomainID:   domainID,
		WorkflowID: workflowID,
	}).Return(&persistence.GetCurrentExecutionResponse{
		RunID: currentRunID,
		// other attributes are not used
	}, nil)

	msBuilderCurrent.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{
		DomainID:           domainID,
		WorkflowID:         workflowID,
		RunID:              currentRunID,
		LastEventTaskID:    lastEventTaskID + 10,
		DecisionVersion:    common.EmptyVersion,
		DecisionScheduleID: common.EmptyEventID,
		DecisionStartedID:  common.EmptyEventID,
	})
	msBuilderCurrent.On("GetNextEventID").Return(currentNextEventID)

	s.mockDomainCache.On("GetDomainByID", domainID).Return(
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
	)

	err := s.historyReplicator.replicateWorkflowStarted(ctx.Background(), context, msBuilder, history, sBuilder, s.logger)
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
	msBuilder := &mockMutableState{}
	defer msBuilder.AssertExpectations(s.T())

	di := &decisionInfo{
		Version:         version,
		ScheduleID:      common.FirstEventID + 1,
		StartedID:       common.EmptyEventID,
		DecisionTimeout: decisionTimeout,
		TaskList:        tasklist,
	}
	sBuilder := &mockStateBuilder{}
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

	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", version).Return(cluster.TestAlternativeClusterName)
	historySize := 111
	executionInfo := &persistence.WorkflowExecutionInfo{
		CreateRequestID:      requestID,
		DomainID:             domainID,
		WorkflowID:           workflowID,
		RunID:                runID,
		ParentDomainID:       parentDomainID,
		ParentWorkflowID:     parentWorkflowID,
		ParentRunID:          parentRunID,
		InitiatedID:          initiatedID,
		TaskList:             tasklist,
		WorkflowTypeName:     workflowType,
		WorkflowTimeout:      workflowTimeout,
		DecisionTimeoutValue: decisionTimeout,
		NextEventID:          nextEventID,
		LastProcessedEvent:   common.EmptyEventID,
		BranchToken:          []byte("some random branch token"),
		DecisionVersion:      di.Version,
		DecisionScheduleID:   di.ScheduleID,
		DecisionStartedID:    di.StartedID,
		DecisionTimeout:      di.DecisionTimeout,
		State:                persistence.WorkflowStateRunning,
		CloseStatus:          persistence.WorkflowCloseStatusNone,
		CronSchedule:         cronSchedule,
		HasRetryPolicy:       true,
		InitialInterval:      retryPolicy.GetInitialIntervalInSeconds(),
		BackoffCoefficient:   retryPolicy.GetBackoffCoefficient(),
		MaximumInterval:      retryPolicy.GetMaximumIntervalInSeconds(),
		ExpirationSeconds:    retryPolicy.GetExpirationIntervalInSeconds(),
		MaximumAttempts:      retryPolicy.GetMaximumAttempts(),
		NonRetriableErrors:   retryPolicy.GetNonRetriableErrorReasons(),
	}
	msBuilder.On("GetExecutionInfo").Return(executionInfo)
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
	msBuilder.On("CloseTransactionAsSnapshot", now.Local(), transactionPolicyPassive).Return(newWorkflowSnapshot, newWorkflowEventsSeq, nil)
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&p.AppendHistoryNodesResponse{Size: historySize}, nil).Once()

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
	s.mockDomainCache.On("GetDomainByID", domainID).Return(
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
	)

	contextCurrent := &mockWorkflowExecutionContext{}
	defer contextCurrent.AssertExpectations(s.T())
	contextCurrent.On("lock", mock.Anything).Return(nil)
	contextCurrent.On("unlock")
	msBuilderCurrent := &mockMutableState{}
	defer msBuilderCurrent.AssertExpectations(s.T())

	contextCurrent.On("loadWorkflowExecution").Return(msBuilderCurrent, nil)
	currentExecution := &shared.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(currentRunID),
	}
	contextCurrent.On("getExecution").Return(currentExecution)
	contextCurrentCacheKey := definition.NewWorkflowIdentifier(domainID, currentExecution.GetWorkflowId(), currentExecution.GetRunId())
	s.historyReplicator.historyCache.PutIfNotExist(contextCurrentCacheKey, contextCurrent)

	msBuilderCurrent.On("GetLastWriteVersion").Return(currentVersion, nil)
	msBuilderCurrent.On("IsWorkflowExecutionRunning").Return(true) // this is used to update the version on mutable state
	msBuilderCurrent.On("UpdateReplicationStateVersion", currentVersion, true).Once()

	currentClusterName := cluster.TestCurrentClusterName
	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", currentVersion).Return(currentClusterName)

	msBuilderCurrent.On("AddWorkflowExecutionTerminatedEvent",
		workflowTerminationReason, mock.Anything, workflowTerminationIdentity).Return(&workflow.HistoryEvent{}, nil)
	contextCurrent.On("updateWorkflowExecutionAsActive", mock.Anything).Return(nil).Once()

	err := s.historyReplicator.replicateWorkflowStarted(ctx.Background(), context, msBuilder, history, sBuilder, s.logger)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestConflictResolutionTerminateCurrentRunningIfNotSelf_TargetRunning() {
	runID := uuid.New()
	lastWriteVersion := int64(1394)
	state := persistence.WorkflowStateRunning
	incomingVersion := int64(4096)
	incomingTimestamp := int64(11238)

	msBuilderTarget := &mockMutableState{}
	defer msBuilderTarget.AssertExpectations(s.T())

	msBuilderTarget.On("IsWorkflowExecutionRunning").Return(true)
	msBuilderTarget.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{
		RunID:              runID,
		State:              state,
		DecisionVersion:    common.EmptyVersion,
		DecisionScheduleID: common.EmptyEventID,
		DecisionStartedID:  common.EmptyEventID,
	})
	msBuilderTarget.On("GetLastWriteVersion").Return(lastWriteVersion, nil)
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

	msBuilderTarget := &mockMutableState{}
	defer msBuilderTarget.AssertExpectations(s.T())

	msBuilderTarget.On("IsWorkflowExecutionRunning").Return(false)
	msBuilderTarget.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{
		DomainID:           domainID,
		WorkflowID:         workflowID,
		RunID:              targetRunID,
		State:              persistence.WorkflowStateCompleted,
		CloseStatus:        persistence.WorkflowCloseStatusContinuedAsNew,
		DecisionVersion:    common.EmptyVersion,
		DecisionScheduleID: common.EmptyEventID,
		DecisionStartedID:  common.EmptyEventID,
	})

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
	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", incomingVersion).Return(incomingCluster)

	domainID := testDomainID
	workflowID := "some random target workflow ID"
	targetRunID := uuid.New()

	msBuilderTarget := &mockMutableState{}
	defer msBuilderTarget.AssertExpectations(s.T())
	msBuilderTarget.On("IsWorkflowExecutionRunning").Return(false)
	msBuilderTarget.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{
		DomainID:           domainID,
		WorkflowID:         workflowID,
		RunID:              targetRunID,
		State:              persistence.WorkflowStateCompleted,
		CloseStatus:        persistence.WorkflowCloseStatusContinuedAsNew,
		DecisionVersion:    common.EmptyVersion,
		DecisionScheduleID: common.EmptyEventID,
		DecisionStartedID:  common.EmptyEventID,
	})

	currentRunID := uuid.New()
	contextCurrent := &mockWorkflowExecutionContext{}
	defer contextCurrent.AssertExpectations(s.T())
	contextCurrent.On("lock", mock.Anything).Return(nil)
	contextCurrent.On("unlock")
	msBuilderCurrent := &mockMutableState{}
	defer msBuilderCurrent.AssertExpectations(s.T())

	contextCurrent.On("loadWorkflowExecution").Return(msBuilderCurrent, nil).Once()
	currentExecution := &shared.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(currentRunID),
	}
	contextCurrent.On("getExecution").Return(currentExecution)
	contextCurrentCacheKey := definition.NewWorkflowIdentifier(domainID, currentExecution.GetWorkflowId(), currentExecution.GetRunId())
	s.historyReplicator.historyCache.PutIfNotExist(contextCurrentCacheKey, contextCurrent)

	currentVersion := incomingVersion - 10
	currentCluster := cluster.TestCurrentClusterName
	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", currentVersion).Return(currentCluster)

	msBuilderCurrent.On("GetLastWriteVersion").Return(currentVersion, nil)
	msBuilderCurrent.On("IsWorkflowExecutionRunning").Return(true) // this is used to update the version on mutable state
	msBuilderCurrent.On("UpdateReplicationStateVersion", currentVersion, true).Once()

	s.mockExecutionMgr.On("GetCurrentExecution", &persistence.GetCurrentExecutionRequest{
		DomainID:   domainID,
		WorkflowID: workflowID,
	}).Return(&persistence.GetCurrentExecutionResponse{
		RunID:            currentRunID,
		State:            persistence.WorkflowStateRunning,
		CloseStatus:      persistence.WorkflowCloseStatusNone,
		LastWriteVersion: currentVersion,
	}, nil)

	msBuilderCurrent.On("AddWorkflowExecutionTerminatedEvent",
		workflowTerminationReason, mock.Anything, workflowTerminationIdentity).Return(&workflow.HistoryEvent{}, nil)
	contextCurrent.On("updateWorkflowExecutionAsActive", mock.Anything).Return(nil).Once()

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
	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", incomingVersion).Return(incomingCluster)

	domainID := testDomainID
	workflowID := "some random target workflow ID"
	targetRunID := uuid.New()

	msBuilderTarget := &mockMutableState{}
	defer msBuilderTarget.AssertExpectations(s.T())
	msBuilderTarget.On("IsWorkflowExecutionRunning").Return(false)
	msBuilderTarget.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{
		DomainID:           domainID,
		WorkflowID:         workflowID,
		RunID:              targetRunID,
		CloseStatus:        persistence.WorkflowCloseStatusContinuedAsNew,
		DecisionVersion:    common.EmptyVersion,
		DecisionScheduleID: common.EmptyEventID,
		DecisionStartedID:  common.EmptyEventID,
	})

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
