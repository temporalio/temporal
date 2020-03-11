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
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonproto "go.temporal.io/temporal-proto/common"
	"go.temporal.io/temporal-proto/enums"
	"go.temporal.io/temporal-proto/serviceerror"

	"github.com/temporalio/temporal/.gen/proto/persistenceblobs"
	"github.com/temporalio/temporal/.gen/proto/replication"
	"github.com/temporalio/temporal/common"
	"github.com/temporalio/temporal/common/cache"
	"github.com/temporalio/temporal/common/cluster"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/mocks"
	"github.com/temporalio/temporal/common/persistence"
	"github.com/temporalio/temporal/common/primitives"
)

type (
	replicatorQueueProcessorSuite struct {
		suite.Suite
		*require.Assertions

		controller          *gomock.Controller
		mockShard           *shardContextTest
		mockDomainCache     *cache.MockDomainCache
		mockMutableState    *MockmutableState
		mockClusterMetadata *cluster.MockMetadata

		mockExecutionMgr *mocks.ExecutionManager
		mockHistoryV2Mgr *mocks.HistoryV2Manager
		mockProducer     *mocks.KafkaProducer

		logger log.Logger

		replicatorQueueProcessor *replicatorQueueProcessorImpl
	}
)

func TestReplicatorQueueProcessorSuite(t *testing.T) {
	s := new(replicatorQueueProcessorSuite)
	suite.Run(t, s)
}

func (s *replicatorQueueProcessorSuite) SetupSuite() {

}

func (s *replicatorQueueProcessorSuite) TearDownSuite() {

}

func (s *replicatorQueueProcessorSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.mockMutableState = NewMockmutableState(s.controller)

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

	s.mockProducer = &mocks.KafkaProducer{}
	s.mockDomainCache = s.mockShard.resource.DomainCache
	s.mockExecutionMgr = s.mockShard.resource.ExecutionMgr
	s.mockHistoryV2Mgr = s.mockShard.resource.HistoryMgr
	s.mockClusterMetadata = s.mockShard.resource.ClusterMetadata
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().IsGlobalDomainEnabled().Return(true).AnyTimes()

	s.logger = s.mockShard.GetLogger()
	historyCache := newHistoryCache(s.mockShard)

	s.replicatorQueueProcessor = newReplicatorQueueProcessor(
		s.mockShard, historyCache, s.mockProducer, s.mockExecutionMgr, s.mockHistoryV2Mgr, s.logger,
	).(*replicatorQueueProcessorImpl)
}

func (s *replicatorQueueProcessorSuite) TearDownTest() {
	s.controller.Finish()
	s.mockShard.Finish(s.T())
	s.mockProducer.AssertExpectations(s.T())
}

func (s *replicatorQueueProcessorSuite) TestSyncActivity_WorkflowMissing() {
	domainName := "some random domain name"
	domainID := testDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	scheduleID := int64(144)
	taskID := int64(1444)
	task := &persistenceblobs.ReplicationTaskInfo{
		TaskType:    persistence.ReplicationTaskTypeSyncActivity,
		TaskID:      taskID,
		DomainID:    primitives.MustParseUUID(domainID),
		WorkflowID:  workflowID,
		RunID:       primitives.MustParseUUID(runID),
		ScheduledID: scheduleID,
	}
	s.mockExecutionMgr.On("CompleteReplicationTask", &persistence.CompleteReplicationTaskRequest{TaskID: taskID}).Return(nil).Once()
	s.mockExecutionMgr.On("GetWorkflowExecution", &persistence.GetWorkflowExecutionRequest{
		DomainID: domainID,
		Execution: commonproto.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
	}).Return(nil, serviceerror.NewNotFound(""))
	s.mockDomainCache.EXPECT().GetDomainByID(domainID).Return(cache.NewGlobalDomainCacheEntryForTest(
		&persistence.DomainInfo{ID: domainID, Name: domainName},
		&persistence.DomainConfig{Retention: 1},
		&persistence.DomainReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []*persistence.ClusterReplicationConfig{
				{ClusterName: cluster.TestCurrentClusterName},
				{ClusterName: cluster.TestAlternativeClusterName},
			},
		},
		1234,
		nil,
	), nil).AnyTimes()

	wrapper := &persistence.ReplicationTaskInfoWrapper{ReplicationTaskInfo: task}
	_, err := s.replicatorQueueProcessor.process(newTaskInfo(nil, wrapper, s.logger))
	s.Nil(err)
}

func (s *replicatorQueueProcessorSuite) TestSyncActivity_WorkflowCompleted() {
	domainName := "some random domain name"
	domainID := testDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	scheduleID := int64(144)
	taskID := int64(1444)
	version := int64(2333)
	task := &persistenceblobs.ReplicationTaskInfo{
		TaskType:    persistence.ReplicationTaskTypeSyncActivity,
		TaskID:      taskID,
		DomainID:    primitives.MustParseUUID(domainID),
		WorkflowID:  workflowID,
		RunID:       primitives.MustParseUUID(runID),
		ScheduledID: scheduleID,
	}
	s.mockExecutionMgr.On("CompleteReplicationTask", &persistence.CompleteReplicationTaskRequest{TaskID: taskID}).Return(nil).Once()

	context, release, _ := s.replicatorQueueProcessor.historyCache.getOrCreateWorkflowExecutionForBackground(
		domainID,
		commonproto.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
	)
	context.(*workflowExecutionContextImpl).mutableState = s.mockMutableState
	release(nil)
	s.mockMutableState.EXPECT().StartTransaction(gomock.Any()).Return(false, nil).Times(1)
	s.mockMutableState.EXPECT().IsWorkflowExecutionRunning().Return(false).AnyTimes()
	s.mockDomainCache.EXPECT().GetDomainByID(domainID).Return(cache.NewGlobalDomainCacheEntryForTest(
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
	), nil).AnyTimes()

	wrapper := &persistence.ReplicationTaskInfoWrapper{ReplicationTaskInfo: task}
	_, err := s.replicatorQueueProcessor.process(newTaskInfo(nil, wrapper, s.logger))
	s.Nil(err)
}

func (s *replicatorQueueProcessorSuite) TestSyncActivity_ActivityCompleted() {
	domainName := "some random domain name"
	domainID := testDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	scheduleID := int64(144)
	taskID := int64(1444)
	version := int64(2333)
	task := &persistenceblobs.ReplicationTaskInfo{
		TaskType:    persistence.ReplicationTaskTypeSyncActivity,
		TaskID:      taskID,
		DomainID:    primitives.MustParseUUID(domainID),
		WorkflowID:  workflowID,
		RunID:       primitives.MustParseUUID(runID),
		ScheduledID: scheduleID,
	}
	s.mockExecutionMgr.On("CompleteReplicationTask", &persistence.CompleteReplicationTaskRequest{TaskID: taskID}).Return(nil).Once()

	context, release, _ := s.replicatorQueueProcessor.historyCache.getOrCreateWorkflowExecutionForBackground(
		domainID,
		commonproto.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
	)

	context.(*workflowExecutionContextImpl).mutableState = s.mockMutableState
	release(nil)
	s.mockMutableState.EXPECT().StartTransaction(gomock.Any()).Return(false, nil).Times(1)
	s.mockMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes()
	s.mockMutableState.EXPECT().GetActivityInfo(scheduleID).Return(nil, false).AnyTimes()
	s.mockDomainCache.EXPECT().GetDomainByID(domainID).Return(cache.NewGlobalDomainCacheEntryForTest(
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
	), nil).AnyTimes()

	wrapper := &persistence.ReplicationTaskInfoWrapper{ReplicationTaskInfo: task}
	_, err := s.replicatorQueueProcessor.process(newTaskInfo(nil, wrapper, s.logger))
	s.Nil(err)
}

func (s *replicatorQueueProcessorSuite) TestSyncActivity_ActivityRetry() {
	domainName := "some random domain name"
	domainID := testDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	scheduleID := int64(144)
	taskID := int64(1444)
	version := int64(2333)
	task := &persistenceblobs.ReplicationTaskInfo{
		TaskType:    persistence.ReplicationTaskTypeSyncActivity,
		TaskID:      taskID,
		DomainID:    primitives.MustParseUUID(domainID),
		WorkflowID:  workflowID,
		RunID:       primitives.MustParseUUID(runID),
		ScheduledID: scheduleID,
	}
	s.mockExecutionMgr.On("CompleteReplicationTask", &persistence.CompleteReplicationTaskRequest{TaskID: taskID}).Return(nil).Once()

	context, release, _ := s.replicatorQueueProcessor.historyCache.getOrCreateWorkflowExecutionForBackground(
		domainID,
		commonproto.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
	)

	context.(*workflowExecutionContextImpl).mutableState = s.mockMutableState
	release(nil)

	activityVersion := int64(333)
	activityScheduleID := scheduleID
	activityScheduledTime := time.Now()
	activityStartedID := common.EmptyEventID
	activityStartedTime := time.Time{}
	activityHeartbeatTime := time.Time{}
	activityAttempt := int32(16384)
	activityDetails := []byte("some random activity progress")
	activityLastFailureReason := "some random reason"
	activityLastWorkerIdentity := "some random worker identity"
	activityLastFailureDetails := []byte("some random failure details")
	s.mockMutableState.EXPECT().StartTransaction(gomock.Any()).Return(false, nil).Times(1)
	s.mockMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes()
	s.mockMutableState.EXPECT().GetActivityInfo(scheduleID).Return(&persistence.ActivityInfo{
		Version:                  activityVersion,
		ScheduleID:               activityScheduleID,
		ScheduledTime:            activityScheduledTime,
		StartedID:                activityStartedID,
		StartedTime:              activityStartedTime,
		LastHeartBeatUpdatedTime: activityHeartbeatTime,
		Details:                  activityDetails,
		Attempt:                  activityAttempt,
		LastFailureReason:        activityLastFailureReason,
		LastWorkerIdentity:       activityLastWorkerIdentity,
		LastFailureDetails:       activityLastFailureDetails,
	}, true).AnyTimes()
	versionHistory := &persistence.VersionHistory{
		BranchToken: []byte{},
		Items: []*persistence.VersionHistoryItem{
			{
				EventID: scheduleID,
				Version: 333,
			},
		},
	}
	versionHistories := &persistence.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*persistence.VersionHistory{
			versionHistory,
		},
	}
	s.mockMutableState.EXPECT().GetVersionHistories().Return(versionHistories).AnyTimes()
	s.mockDomainCache.EXPECT().GetDomainByID(domainID).Return(cache.NewGlobalDomainCacheEntryForTest(
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
	), nil).AnyTimes()

	s.mockProducer.On("Publish", &replication.ReplicationTask{
		TaskType: enums.ReplicationTaskTypeSyncActivity,
		Attributes: &replication.ReplicationTask_SyncActivityTaskAttributes{
			SyncActivityTaskAttributes: &replication.SyncActivityTaskAttributes{
				DomainId:           domainID,
				WorkflowId:         workflowID,
				RunId:              runID,
				Version:            activityVersion,
				ScheduledId:        activityScheduleID,
				ScheduledTime:      activityScheduledTime.UnixNano(),
				StartedId:          activityStartedID,
				StartedTime:        0,
				LastHeartbeatTime:  activityHeartbeatTime.UnixNano(),
				Details:            activityDetails,
				Attempt:            activityAttempt,
				LastFailureReason:  activityLastFailureReason,
				LastWorkerIdentity: activityLastWorkerIdentity,
				LastFailureDetails: activityLastFailureDetails,
				VersionHistory:     versionHistory.ToProto(),
			},
		},
	}).Return(nil).Once()

	wrapper := &persistence.ReplicationTaskInfoWrapper{ReplicationTaskInfo: task}
	_, err := s.replicatorQueueProcessor.process(newTaskInfo(nil, wrapper, s.logger))
	s.Nil(err)
}

func (s *replicatorQueueProcessorSuite) TestSyncActivity_ActivityRunning() {
	domainName := "some random domain name"
	domainID := testDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	scheduleID := int64(144)
	taskID := int64(1444)
	version := int64(2333)
	task := &persistenceblobs.ReplicationTaskInfo{
		TaskType:    persistence.ReplicationTaskTypeSyncActivity,
		TaskID:      taskID,
		DomainID:    primitives.MustParseUUID(domainID),
		WorkflowID:  workflowID,
		RunID:       primitives.MustParseUUID(runID),
		ScheduledID: scheduleID,
	}
	s.mockExecutionMgr.On("CompleteReplicationTask", &persistence.CompleteReplicationTaskRequest{TaskID: taskID}).Return(nil).Once()

	context, release, _ := s.replicatorQueueProcessor.historyCache.getOrCreateWorkflowExecutionForBackground(
		domainID,
		commonproto.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
	)

	context.(*workflowExecutionContextImpl).mutableState = s.mockMutableState
	release(nil)

	activityVersion := int64(333)
	activityScheduleID := scheduleID
	activityScheduledTime := time.Now()
	activityStartedID := activityScheduleID + 1
	activityStartedTime := activityScheduledTime.Add(time.Minute)
	activityHeartbeatTime := activityStartedTime.Add(time.Minute)
	activityAttempt := int32(16384)
	activityDetails := []byte("some random activity progress")
	activityLastFailureReason := "some random reason"
	activityLastWorkerIdentity := "some random worker identity"
	activityLastFailureDetails := []byte("some random failure details")
	s.mockMutableState.EXPECT().StartTransaction(gomock.Any()).Return(false, nil).Times(1)
	s.mockMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes()
	s.mockMutableState.EXPECT().GetActivityInfo(scheduleID).Return(&persistence.ActivityInfo{
		Version:                  activityVersion,
		ScheduleID:               activityScheduleID,
		ScheduledTime:            activityScheduledTime,
		StartedID:                activityStartedID,
		StartedTime:              activityStartedTime,
		LastHeartBeatUpdatedTime: activityHeartbeatTime,
		Details:                  activityDetails,
		Attempt:                  activityAttempt,
		LastFailureReason:        activityLastFailureReason,
		LastWorkerIdentity:       activityLastWorkerIdentity,
		LastFailureDetails:       activityLastFailureDetails,
	}, true).AnyTimes()
	versionHistory := &persistence.VersionHistory{
		BranchToken: []byte{},
		Items: []*persistence.VersionHistoryItem{
			{
				EventID: scheduleID,
				Version: 333,
			},
		},
	}
	versionHistories := &persistence.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*persistence.VersionHistory{
			versionHistory,
		},
	}
	s.mockMutableState.EXPECT().GetVersionHistories().Return(versionHistories).AnyTimes()
	s.mockDomainCache.EXPECT().GetDomainByID(domainID).Return(cache.NewGlobalDomainCacheEntryForTest(
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
	), nil).AnyTimes()
	s.mockProducer.On("Publish", &replication.ReplicationTask{
		TaskType: enums.ReplicationTaskTypeSyncActivity,
		Attributes: &replication.ReplicationTask_SyncActivityTaskAttributes{
			SyncActivityTaskAttributes: &replication.SyncActivityTaskAttributes{
				DomainId:           domainID,
				WorkflowId:         workflowID,
				RunId:              runID,
				Version:            activityVersion,
				ScheduledId:        activityScheduleID,
				ScheduledTime:      activityScheduledTime.UnixNano(),
				StartedId:          activityStartedID,
				StartedTime:        activityStartedTime.UnixNano(),
				LastHeartbeatTime:  activityHeartbeatTime.UnixNano(),
				Details:            activityDetails,
				Attempt:            activityAttempt,
				LastFailureReason:  activityLastFailureReason,
				LastWorkerIdentity: activityLastWorkerIdentity,
				LastFailureDetails: activityLastFailureDetails,
				VersionHistory:     versionHistory.ToProto(),
			},
		},
	}).Return(nil).Once()

	wrapper := &persistence.ReplicationTaskInfoWrapper{ReplicationTaskInfo: task}
	_, err := s.replicatorQueueProcessor.process(newTaskInfo(nil, wrapper, s.logger))
	s.Nil(err)
}

func (s *replicatorQueueProcessorSuite) TestPaginateHistoryWithShardID() {
	firstEventID := int64(133)
	nextEventID := int64(134)
	pageSize := 1
	shardID := 1

	req := &persistence.ReadHistoryBranchRequest{
		BranchToken:   []byte("asd"),
		MinEventID:    firstEventID,
		MaxEventID:    nextEventID,
		PageSize:      pageSize,
		NextPageToken: []byte{},
		ShardID:       &shardID,
	}
	s.mockHistoryV2Mgr.On("ReadHistoryBranch", req).Return(&persistence.ReadHistoryBranchResponse{
		HistoryEvents: []*commonproto.HistoryEvent{
			{
				EventId: int64(1),
				// EventType:  enums.EventTypeWorkflowExecutionStarted,
				// Attributes: &commonproto.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: &commonproto.WorkflowExecutionStartedEventAttributes{}},
			},
		},
		NextPageToken:    []byte{},
		Size:             1,
		LastFirstEventID: nextEventID,
	}, nil).Once()
	hEvents, bEvents, token, size, err := PaginateHistory(
		s.mockHistoryV2Mgr,
		false,
		[]byte("asd"),
		firstEventID,
		nextEventID,
		[]byte{},
		pageSize,
		&shardID)

	s.Equal(1, len(hEvents))
	s.Equal(0, len(bEvents))
	s.NotNil(token)
	s.Equal(1, size)
	s.NoError(err)
}
