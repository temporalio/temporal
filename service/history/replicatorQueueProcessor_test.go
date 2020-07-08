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
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/temporal-proto/common/v1"
	historypb "go.temporal.io/temporal-proto/history/v1"
	"go.temporal.io/temporal-proto/serviceerror"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/persistenceblobs/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/failure"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/mocks"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/persistence"
)

type (
	replicatorQueueProcessorSuite struct {
		suite.Suite
		*require.Assertions

		controller          *gomock.Controller
		mockShard           *shardContextTest
		mockNamespaceCache  *cache.MockNamespaceCache
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
				ShardId:          0,
				RangeId:          1,
				TransferAckLevel: 0,
			}},
		NewDynamicConfigForTest(),
	)

	s.mockProducer = &mocks.KafkaProducer{}
	s.mockNamespaceCache = s.mockShard.resource.NamespaceCache
	s.mockExecutionMgr = s.mockShard.resource.ExecutionMgr
	s.mockHistoryV2Mgr = s.mockShard.resource.HistoryMgr
	s.mockClusterMetadata = s.mockShard.resource.ClusterMetadata
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(true).AnyTimes()

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
	namespace := "some random namespace name"
	namespaceID := testNamespaceID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	scheduleID := int64(144)
	taskID := int64(1444)
	task := &persistenceblobs.ReplicationTaskInfo{
		TaskType:    enumsspb.TASK_TYPE_REPLICATION_SYNC_ACTIVITY,
		TaskId:      taskID,
		NamespaceId: namespaceID,
		WorkflowId:  workflowID,
		RunId:       runID,
		ScheduledId: scheduleID,
	}
	s.mockExecutionMgr.On("CompleteReplicationTask", &persistence.CompleteReplicationTaskRequest{TaskID: taskID}).Return(nil).Once()
	s.mockExecutionMgr.On("GetWorkflowExecution", &persistence.GetWorkflowExecutionRequest{
		NamespaceID: namespaceID,
		Execution: commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
	}).Return(nil, serviceerror.NewNotFound(""))
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(namespaceID).Return(cache.NewGlobalNamespaceCacheEntryForTest(
		&persistenceblobs.NamespaceInfo{Id: namespaceID, Name: namespace},
		&persistenceblobs.NamespaceConfig{RetentionDays: 1},
		&persistenceblobs.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []string{
				cluster.TestCurrentClusterName,
				cluster.TestAlternativeClusterName,
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
	namespace := "some random namespace name"
	namespaceID := testNamespaceID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	scheduleID := int64(144)
	taskID := int64(1444)
	version := int64(2333)
	task := &persistenceblobs.ReplicationTaskInfo{
		TaskType:    enumsspb.TASK_TYPE_REPLICATION_SYNC_ACTIVITY,
		TaskId:      taskID,
		NamespaceId: namespaceID,
		WorkflowId:  workflowID,
		RunId:       runID,
		ScheduledId: scheduleID,
	}
	s.mockExecutionMgr.On("CompleteReplicationTask", &persistence.CompleteReplicationTaskRequest{TaskID: taskID}).Return(nil).Once()

	context, release, _ := s.replicatorQueueProcessor.historyCache.getOrCreateWorkflowExecutionForBackground(
		namespaceID,
		commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
	)
	context.(*workflowExecutionContextImpl).mutableState = s.mockMutableState
	release(nil)
	s.mockMutableState.EXPECT().StartTransaction(gomock.Any()).Return(false, nil).Times(1)
	s.mockMutableState.EXPECT().IsWorkflowExecutionRunning().Return(false).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(namespaceID).Return(cache.NewGlobalNamespaceCacheEntryForTest(
		&persistenceblobs.NamespaceInfo{Id: namespaceID, Name: namespace},
		&persistenceblobs.NamespaceConfig{RetentionDays: 1},
		&persistenceblobs.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []string{
				cluster.TestCurrentClusterName,
				cluster.TestAlternativeClusterName,
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
	namespace := "some random namespace name"
	namespaceID := testNamespaceID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	scheduleID := int64(144)
	taskID := int64(1444)
	version := int64(2333)
	task := &persistenceblobs.ReplicationTaskInfo{
		TaskType:    enumsspb.TASK_TYPE_REPLICATION_SYNC_ACTIVITY,
		TaskId:      taskID,
		NamespaceId: namespaceID,
		WorkflowId:  workflowID,
		RunId:       runID,
		ScheduledId: scheduleID,
	}
	s.mockExecutionMgr.On("CompleteReplicationTask", &persistence.CompleteReplicationTaskRequest{TaskID: taskID}).Return(nil).Once()

	context, release, _ := s.replicatorQueueProcessor.historyCache.getOrCreateWorkflowExecutionForBackground(
		namespaceID,
		commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
	)

	context.(*workflowExecutionContextImpl).mutableState = s.mockMutableState
	release(nil)
	s.mockMutableState.EXPECT().StartTransaction(gomock.Any()).Return(false, nil).Times(1)
	s.mockMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes()
	s.mockMutableState.EXPECT().GetActivityInfo(scheduleID).Return(nil, false).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(namespaceID).Return(cache.NewGlobalNamespaceCacheEntryForTest(
		&persistenceblobs.NamespaceInfo{Id: namespaceID, Name: namespace},
		&persistenceblobs.NamespaceConfig{RetentionDays: 1},
		&persistenceblobs.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []string{
				cluster.TestCurrentClusterName,
				cluster.TestAlternativeClusterName,
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
	namespace := "some random namespace name"
	namespaceID := testNamespaceID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	scheduleID := int64(144)
	taskID := int64(1444)
	version := int64(2333)
	task := &persistenceblobs.ReplicationTaskInfo{
		TaskType:    enumsspb.TASK_TYPE_REPLICATION_SYNC_ACTIVITY,
		TaskId:      taskID,
		NamespaceId: namespaceID,
		WorkflowId:  workflowID,
		RunId:       runID,
		ScheduledId: scheduleID,
	}
	s.mockExecutionMgr.On("CompleteReplicationTask", &persistence.CompleteReplicationTaskRequest{TaskID: taskID}).Return(nil).Once()

	context, release, _ := s.replicatorQueueProcessor.historyCache.getOrCreateWorkflowExecutionForBackground(
		namespaceID,
		commonpb.WorkflowExecution{
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
	activityDetails := payloads.EncodeString("some random activity progress")
	activityLastFailure := failure.NewServerFailure("some random reason", false)
	activityLastWorkerIdentity := "some random worker identity"
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
		LastFailure:              activityLastFailure,
		LastWorkerIdentity:       activityLastWorkerIdentity,
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
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(namespaceID).Return(cache.NewGlobalNamespaceCacheEntryForTest(
		&persistenceblobs.NamespaceInfo{Id: namespaceID, Name: namespace},
		&persistenceblobs.NamespaceConfig{RetentionDays: 1},
		&persistenceblobs.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []string{
				cluster.TestCurrentClusterName,
				cluster.TestAlternativeClusterName,
			},
		},
		version,
		nil,
	), nil).AnyTimes()

	s.mockProducer.On("Publish", &replicationspb.ReplicationTask{
		TaskType: enumsspb.REPLICATION_TASK_TYPE_SYNC_ACTIVITY_TASK,
		Attributes: &replicationspb.ReplicationTask_SyncActivityTaskAttributes{
			SyncActivityTaskAttributes: &replicationspb.SyncActivityTaskAttributes{
				NamespaceId:        namespaceID,
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
				LastFailure:        activityLastFailure,
				LastWorkerIdentity: activityLastWorkerIdentity,
				VersionHistory:     versionHistory.ToProto(),
			},
		},
	}).Return(nil).Once()

	wrapper := &persistence.ReplicationTaskInfoWrapper{ReplicationTaskInfo: task}
	_, err := s.replicatorQueueProcessor.process(newTaskInfo(nil, wrapper, s.logger))
	s.Nil(err)
}

func (s *replicatorQueueProcessorSuite) TestSyncActivity_ActivityRunning() {
	namespace := "some random namespace name"
	namespaceID := testNamespaceID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	scheduleID := int64(144)
	taskID := int64(1444)
	version := int64(2333)
	task := &persistenceblobs.ReplicationTaskInfo{
		TaskType:    enumsspb.TASK_TYPE_REPLICATION_SYNC_ACTIVITY,
		TaskId:      taskID,
		NamespaceId: namespaceID,
		WorkflowId:  workflowID,
		RunId:       runID,
		ScheduledId: scheduleID,
	}
	s.mockExecutionMgr.On("CompleteReplicationTask", &persistence.CompleteReplicationTaskRequest{TaskID: taskID}).Return(nil).Once()

	context, release, _ := s.replicatorQueueProcessor.historyCache.getOrCreateWorkflowExecutionForBackground(
		namespaceID,
		commonpb.WorkflowExecution{
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
	activityDetails := payloads.EncodeString("some random activity progress")
	activityLastFailure := failure.NewServerFailure("some random reason", false)
	activityLastWorkerIdentity := "some random worker identity"
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
		LastFailure:              activityLastFailure,
		LastWorkerIdentity:       activityLastWorkerIdentity,
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
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(namespaceID).Return(cache.NewGlobalNamespaceCacheEntryForTest(
		&persistenceblobs.NamespaceInfo{Id: namespaceID, Name: namespace},
		&persistenceblobs.NamespaceConfig{RetentionDays: 1},
		&persistenceblobs.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []string{
				cluster.TestCurrentClusterName,
				cluster.TestAlternativeClusterName,
			},
		},
		version,
		nil,
	), nil).AnyTimes()
	s.mockProducer.On("Publish", &replicationspb.ReplicationTask{
		TaskType: enumsspb.REPLICATION_TASK_TYPE_SYNC_ACTIVITY_TASK,
		Attributes: &replicationspb.ReplicationTask_SyncActivityTaskAttributes{
			SyncActivityTaskAttributes: &replicationspb.SyncActivityTaskAttributes{
				NamespaceId:        namespaceID,
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
				LastFailure:        activityLastFailure,
				LastWorkerIdentity: activityLastWorkerIdentity,
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
		HistoryEvents: []*historypb.HistoryEvent{
			{
				EventId: int64(1),
				// EventType:  enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
				// Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{}},
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
