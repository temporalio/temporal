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

package replication

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/failure"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/workflow"
	wcache "go.temporal.io/server/service/history/workflow/cache"
)

type (
	ackManagerSuite struct {
		suite.Suite
		*require.Assertions

		controller          *gomock.Controller
		mockShard           *shard.ContextTest
		mockNamespaceCache  *namespace.MockRegistry
		mockMutableState    *workflow.MockMutableState
		mockClusterMetadata *cluster.MockMetadata

		mockExecutionMgr *persistence.MockExecutionManager

		logger log.Logger

		replicationAckManager *ackMgrImpl
	}
)

func TestAckManagerSuite(t *testing.T) {
	s := new(ackManagerSuite)
	suite.Run(t, s)
}

func (s *ackManagerSuite) SetupSuite() {

}

func (s *ackManagerSuite) TearDownSuite() {

}

func (s *ackManagerSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.mockMutableState = workflow.NewMockMutableState(s.controller)

	s.mockShard = shard.NewTestContext(
		s.controller,
		&persistence.ShardInfoWithFailover{
			ShardInfo: &persistencespb.ShardInfo{
				ShardId: 0,
				RangeId: 1,
			}},
		tests.NewDynamicConfig(),
	)

	s.mockNamespaceCache = s.mockShard.Resource.NamespaceCache
	s.mockExecutionMgr = s.mockShard.Resource.ExecutionMgr
	s.mockClusterMetadata = s.mockShard.Resource.ClusterMetadata
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(true).AnyTimes()

	s.logger = s.mockShard.GetLogger()
	workflowCache := wcache.NewCache(s.mockShard)

	s.replicationAckManager = NewAckManager(
		s.mockShard, workflowCache, s.mockExecutionMgr, s.logger,
	).(*ackMgrImpl)
}

func (s *ackManagerSuite) TearDownTest() {
	s.controller.Finish()
	s.mockShard.StopForTest()
}

func (s *ackManagerSuite) TestNotifyNewTasks_NotInitialized() {
	s.replicationAckManager.maxTaskID = nil

	s.replicationAckManager.NotifyNewTasks([]tasks.Task{
		&tasks.HistoryReplicationTask{TaskID: 456},
		&tasks.HistoryReplicationTask{TaskID: 123},
	})

	s.Equal(*s.replicationAckManager.maxTaskID, int64(456))
}

func (s *ackManagerSuite) TestNotifyNewTasks_Initialized() {
	s.replicationAckManager.maxTaskID = convert.Int64Ptr(123)

	s.replicationAckManager.NotifyNewTasks([]tasks.Task{
		&tasks.HistoryReplicationTask{TaskID: 100},
	})
	s.Equal(*s.replicationAckManager.maxTaskID, int64(123))

	s.replicationAckManager.NotifyNewTasks([]tasks.Task{
		&tasks.HistoryReplicationTask{TaskID: 234},
	})
	s.Equal(*s.replicationAckManager.maxTaskID, int64(234))
}

func (s *ackManagerSuite) TestTaskIDRange_NotInitialized() {
	s.replicationAckManager.sanityCheckTime = time.Time{}
	expectMaxTaskID := s.mockShard.GetImmediateQueueExclusiveHighReadWatermark().Prev().TaskID
	expectMinTaskID := expectMaxTaskID - 100
	s.replicationAckManager.maxTaskID = convert.Int64Ptr(expectMinTaskID - 100)

	minTaskID, maxTaskID := s.replicationAckManager.taskIDsRange(expectMinTaskID)
	s.Equal(expectMinTaskID, minTaskID)
	s.Equal(expectMaxTaskID, maxTaskID)
	s.NotEqual(time.Time{}, s.replicationAckManager.sanityCheckTime)
	s.Equal(expectMaxTaskID, *s.replicationAckManager.maxTaskID)
}

func (s *ackManagerSuite) TestTaskIDRange_Initialized_UseHighestReplicationTaskID() {
	now := time.Now().UTC()
	sanityCheckTime := now.Add(2 * time.Minute)
	s.replicationAckManager.sanityCheckTime = sanityCheckTime
	expectMinTaskID := s.mockShard.GetImmediateQueueExclusiveHighReadWatermark().TaskID - 100
	expectMaxTaskID := s.mockShard.GetImmediateQueueExclusiveHighReadWatermark().TaskID - 50
	s.replicationAckManager.maxTaskID = convert.Int64Ptr(expectMaxTaskID)

	minTaskID, maxTaskID := s.replicationAckManager.taskIDsRange(expectMinTaskID)
	s.Equal(expectMinTaskID, minTaskID)
	s.Equal(expectMaxTaskID, maxTaskID)
	s.Equal(sanityCheckTime, s.replicationAckManager.sanityCheckTime)
	s.Equal(expectMaxTaskID, *s.replicationAckManager.maxTaskID)
}

func (s *ackManagerSuite) TestTaskIDRange_Initialized_NoHighestReplicationTaskID() {
	now := time.Now().UTC()
	sanityCheckTime := now.Add(2 * time.Minute)
	s.replicationAckManager.sanityCheckTime = sanityCheckTime
	expectMinTaskID := s.mockShard.GetImmediateQueueExclusiveHighReadWatermark().Prev().TaskID - 100
	expectMaxTaskID := s.mockShard.GetImmediateQueueExclusiveHighReadWatermark().Prev().TaskID
	s.replicationAckManager.maxTaskID = nil

	minTaskID, maxTaskID := s.replicationAckManager.taskIDsRange(expectMinTaskID)
	s.Equal(expectMinTaskID, minTaskID)
	s.Equal(expectMaxTaskID, maxTaskID)
	s.Equal(sanityCheckTime, s.replicationAckManager.sanityCheckTime)
	s.Equal(expectMaxTaskID, *s.replicationAckManager.maxTaskID)
}

func (s *ackManagerSuite) TestTaskIDRange_Initialized_UseHighestTransferTaskID() {
	now := time.Now().UTC()
	sanityCheckTime := now.Add(-2 * time.Minute)
	s.replicationAckManager.sanityCheckTime = sanityCheckTime
	expectMinTaskID := s.mockShard.GetImmediateQueueExclusiveHighReadWatermark().Prev().TaskID - 100
	expectMaxTaskID := s.mockShard.GetImmediateQueueExclusiveHighReadWatermark().Prev().TaskID
	s.replicationAckManager.maxTaskID = convert.Int64Ptr(s.mockShard.GetImmediateQueueExclusiveHighReadWatermark().TaskID - 50)

	minTaskID, maxTaskID := s.replicationAckManager.taskIDsRange(expectMinTaskID)
	s.Equal(expectMinTaskID, minTaskID)
	s.Equal(expectMaxTaskID, maxTaskID)
	s.NotEqual(sanityCheckTime, s.replicationAckManager.sanityCheckTime)
	s.Equal(expectMaxTaskID, *s.replicationAckManager.maxTaskID)
}

func (s *ackManagerSuite) TestSyncActivity_WorkflowMissing() {
	ctx := context.Background()
	namespaceID := tests.NamespaceID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	scheduledEventID := int64(144)
	version := int64(288)
	taskID := int64(1444)
	task := &tasks.SyncActivityTask{
		WorkflowKey: definition.NewWorkflowKey(
			namespaceID.String(),
			workflowID,
			runID,
		),
		VisibilityTimestamp: time.Now().UTC(),
		TaskID:              taskID,
		Version:             version,
		ScheduledEventID:    scheduledEventID,
	}
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), &persistence.GetWorkflowExecutionRequest{
		ShardID:     s.mockShard.GetShardID(),
		NamespaceID: namespaceID.String(),
		WorkflowID:  workflowID,
		RunID:       runID,
	}).Return(nil, serviceerror.NewNotFound(""))
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(namespaceID).Return(tests.GlobalNamespaceEntry, nil).AnyTimes()

	result, err := s.replicationAckManager.generateSyncActivityTask(ctx, task)
	s.NoError(err)
	s.Nil(result)
}

func (s *ackManagerSuite) TestSyncActivity_WorkflowCompleted() {
	ctx := context.Background()
	namespaceID := tests.NamespaceID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	scheduledEventID := int64(144)
	taskID := int64(1444)
	version := int64(2333)
	task := &tasks.SyncActivityTask{
		WorkflowKey: definition.NewWorkflowKey(
			namespaceID.String(),
			workflowID,
			runID,
		),
		VisibilityTimestamp: time.Now().UTC(),
		TaskID:              taskID,
		Version:             version,
		ScheduledEventID:    scheduledEventID,
	}

	context, release, _ := s.replicationAckManager.workflowCache.GetOrCreateWorkflowExecution(
		ctx,
		namespaceID,
		commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
		workflow.CallerTypeTask,
	)
	context.(*workflow.ContextImpl).MutableState = s.mockMutableState
	release(nil)
	s.mockMutableState.EXPECT().StartTransaction(gomock.Any()).Return(false, nil)
	s.mockMutableState.EXPECT().IsWorkflowExecutionRunning().Return(false).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(namespaceID).Return(tests.GlobalNamespaceEntry, nil).AnyTimes()

	result, err := s.replicationAckManager.generateSyncActivityTask(ctx, task)
	s.NoError(err)
	s.Nil(result)
}

func (s *ackManagerSuite) TestSyncActivity_ActivityCompleted() {
	ctx := context.Background()
	namespaceID := tests.NamespaceID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	scheduledEventID := int64(144)
	taskID := int64(1444)
	version := int64(2333)
	task := &tasks.SyncActivityTask{
		WorkflowKey: definition.NewWorkflowKey(
			namespaceID.String(),
			workflowID,
			runID,
		),
		VisibilityTimestamp: time.Now().UTC(),
		TaskID:              taskID,
		Version:             version,
		ScheduledEventID:    scheduledEventID,
	}

	context, release, _ := s.replicationAckManager.workflowCache.GetOrCreateWorkflowExecution(
		ctx,
		namespaceID,
		commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
		workflow.CallerTypeTask,
	)

	context.(*workflow.ContextImpl).MutableState = s.mockMutableState
	release(nil)
	s.mockMutableState.EXPECT().StartTransaction(gomock.Any()).Return(false, nil)
	s.mockMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes()
	s.mockMutableState.EXPECT().GetActivityInfo(scheduledEventID).Return(nil, false).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(namespaceID).Return(tests.GlobalNamespaceEntry, nil).AnyTimes()

	result, err := s.replicationAckManager.generateSyncActivityTask(ctx, task)
	s.NoError(err)
	s.Nil(result)
}

func (s *ackManagerSuite) TestSyncActivity_ActivityRetry() {
	ctx := context.Background()
	namespaceID := tests.NamespaceID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	scheduledEventID := int64(144)
	taskID := int64(1444)
	version := int64(2333)
	taskTimestamp := time.Now().UTC()
	task := &tasks.SyncActivityTask{
		WorkflowKey: definition.NewWorkflowKey(
			namespaceID.String(),
			workflowID,
			runID,
		),
		VisibilityTimestamp: taskTimestamp,
		TaskID:              taskID,
		Version:             version,
		ScheduledEventID:    scheduledEventID,
	}

	context, release, _ := s.replicationAckManager.workflowCache.GetOrCreateWorkflowExecution(
		ctx,
		namespaceID,
		commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
		workflow.CallerTypeTask,
	)

	context.(*workflow.ContextImpl).MutableState = s.mockMutableState
	release(nil)

	activityVersion := int64(333)
	activityScheduledEventID := scheduledEventID
	activityScheduledTime := time.Now().UTC()
	activityStartedEventID := common.EmptyEventID
	activityAttempt := int32(16384)
	activityDetails := payloads.EncodeString("some random activity progress")
	activityLastFailure := failure.NewServerFailure("some random reason", false)
	activityLastWorkerIdentity := "some random worker identity"
	s.mockMutableState.EXPECT().StartTransaction(gomock.Any()).Return(false, nil)
	s.mockMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes()
	s.mockMutableState.EXPECT().GetActivityInfo(scheduledEventID).Return(&persistencespb.ActivityInfo{
		Version:                 activityVersion,
		ScheduledEventId:        activityScheduledEventID,
		ScheduledTime:           &activityScheduledTime,
		StartedEventId:          activityStartedEventID,
		StartedTime:             nil,
		LastHeartbeatUpdateTime: nil,
		LastHeartbeatDetails:    activityDetails,
		Attempt:                 activityAttempt,
		RetryLastFailure:        activityLastFailure,
		RetryLastWorkerIdentity: activityLastWorkerIdentity,
	}, true).AnyTimes()
	versionHistory := &historyspb.VersionHistory{
		BranchToken: []byte{},
		Items: []*historyspb.VersionHistoryItem{
			{
				EventId: scheduledEventID,
				Version: 333,
			},
		},
	}
	versionHistories := &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{
			versionHistory,
		},
	}
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{VersionHistories: versionHistories}).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(namespaceID).Return(tests.GlobalNamespaceEntry, nil).AnyTimes()

	result, err := s.replicationAckManager.generateSyncActivityTask(ctx, task)
	s.NoError(err)
	s.Equal(&replicationspb.ReplicationTask{
		SourceTaskId: taskID,
		TaskType:     enumsspb.REPLICATION_TASK_TYPE_SYNC_ACTIVITY_TASK,
		Attributes: &replicationspb.ReplicationTask_SyncActivityTaskAttributes{
			SyncActivityTaskAttributes: &replicationspb.SyncActivityTaskAttributes{
				NamespaceId:        namespaceID.String(),
				WorkflowId:         workflowID,
				RunId:              runID,
				Version:            activityVersion,
				ScheduledEventId:   activityScheduledEventID,
				ScheduledTime:      &activityScheduledTime,
				StartedEventId:     activityStartedEventID,
				StartedTime:        nil,
				LastHeartbeatTime:  nil,
				Details:            activityDetails,
				Attempt:            activityAttempt,
				LastFailure:        activityLastFailure,
				LastWorkerIdentity: activityLastWorkerIdentity,
				VersionHistory:     versionHistory,
			},
		},
		VisibilityTime: timestamp.TimePtr(taskTimestamp),
	}, result)
}

func (s *ackManagerSuite) TestSyncActivity_ActivityRunning() {
	ctx := context.Background()
	namespaceID := tests.NamespaceID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	scheduledEventID := int64(144)
	taskID := int64(1444)
	version := int64(2333)
	taskTimestamp := time.Now().UTC()
	task := &tasks.SyncActivityTask{
		WorkflowKey: definition.NewWorkflowKey(
			namespaceID.String(),
			workflowID,
			runID,
		),
		VisibilityTimestamp: taskTimestamp,
		TaskID:              taskID,
		Version:             version,
		ScheduledEventID:    scheduledEventID,
	}

	context, release, _ := s.replicationAckManager.workflowCache.GetOrCreateWorkflowExecution(
		ctx,
		namespaceID,
		commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
		workflow.CallerTypeTask,
	)

	context.(*workflow.ContextImpl).MutableState = s.mockMutableState
	release(nil)

	activityVersion := int64(333)
	activityScheduledEventID := scheduledEventID
	activityScheduledTime := timestamp.TimePtr(time.Date(1978, 8, 22, 12, 59, 59, 999999, time.UTC))
	activityStartedEventID := activityScheduledEventID + 1
	activityStartedTime := activityScheduledTime.Add(time.Minute)
	activityHeartbeatTime := activityStartedTime.Add(time.Minute)
	activityAttempt := int32(16384)
	activityDetails := payloads.EncodeString("some random activity progress")
	activityLastFailure := failure.NewServerFailure("some random reason", false)
	activityLastWorkerIdentity := "some random worker identity"
	s.mockMutableState.EXPECT().StartTransaction(gomock.Any()).Return(false, nil)
	s.mockMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes()
	s.mockMutableState.EXPECT().GetActivityInfo(scheduledEventID).Return(&persistencespb.ActivityInfo{
		Version:                 activityVersion,
		ScheduledEventId:        activityScheduledEventID,
		ScheduledTime:           activityScheduledTime,
		StartedEventId:          activityStartedEventID,
		StartedTime:             &activityStartedTime,
		LastHeartbeatUpdateTime: &activityHeartbeatTime,
		LastHeartbeatDetails:    activityDetails,
		Attempt:                 activityAttempt,
		RetryLastFailure:        activityLastFailure,
		RetryLastWorkerIdentity: activityLastWorkerIdentity,
	}, true).AnyTimes()
	versionHistory := &historyspb.VersionHistory{
		BranchToken: []byte{},
		Items: []*historyspb.VersionHistoryItem{
			{
				EventId: scheduledEventID,
				Version: 333,
			},
		},
	}
	versionHistories := &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{
			versionHistory,
		},
	}
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{VersionHistories: versionHistories}).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(namespaceID).Return(tests.GlobalNamespaceEntry, nil).AnyTimes()

	result, err := s.replicationAckManager.generateSyncActivityTask(ctx, task)
	s.NoError(err)
	s.Equal(&replicationspb.ReplicationTask{
		SourceTaskId: taskID,
		TaskType:     enumsspb.REPLICATION_TASK_TYPE_SYNC_ACTIVITY_TASK,
		Attributes: &replicationspb.ReplicationTask_SyncActivityTaskAttributes{
			SyncActivityTaskAttributes: &replicationspb.SyncActivityTaskAttributes{
				NamespaceId:        namespaceID.String(),
				WorkflowId:         workflowID,
				RunId:              runID,
				Version:            activityVersion,
				ScheduledEventId:   activityScheduledEventID,
				ScheduledTime:      activityScheduledTime,
				StartedEventId:     activityStartedEventID,
				StartedTime:        &activityStartedTime,
				LastHeartbeatTime:  &activityHeartbeatTime,
				Details:            activityDetails,
				Attempt:            activityAttempt,
				LastFailure:        activityLastFailure,
				LastWorkerIdentity: activityLastWorkerIdentity,
				VersionHistory:     versionHistory,
			},
		},
		VisibilityTime: timestamp.TimePtr(taskTimestamp),
	}, result)
}

func (s *ackManagerSuite) Test_GetMaxTaskInfo() {
	now := time.Now()
	taskSet := []tasks.Task{
		&tasks.HistoryReplicationTask{
			TaskID:              1,
			VisibilityTimestamp: now,
		},
		&tasks.HistoryReplicationTask{
			TaskID:              6,
			VisibilityTimestamp: now.Add(time.Second),
		},
		&tasks.HistoryReplicationTask{
			TaskID:              3,
			VisibilityTimestamp: now.Add(time.Hour),
		},
	}
	s.replicationAckManager.NotifyNewTasks(taskSet)

	maxTaskID, maxVisibilityTimestamp := s.replicationAckManager.GetMaxTaskInfo()
	s.Equal(int64(6), maxTaskID)
	s.Equal(now.Add(time.Hour), maxVisibilityTimestamp)
}

func (s *ackManagerSuite) TestGetTasks_Empty() {
	ctx := context.Background()
	minTaskID := rand.Int63()
	maxTaskID := minTaskID + 100
	batchSize := 100

	s.mockExecutionMgr.EXPECT().GetHistoryTasks(ctx, &persistence.GetHistoryTasksRequest{
		ShardID:             s.mockShard.GetShardID(),
		TaskCategory:        tasks.CategoryReplication,
		InclusiveMinTaskKey: tasks.NewImmediateKey(minTaskID + 1),
		ExclusiveMaxTaskKey: tasks.NewImmediateKey(maxTaskID + 1),
		BatchSize:           batchSize,
		NextPageToken:       nil,
	}).Return(&persistence.GetHistoryTasksResponse{
		Tasks:         nil,
		NextPageToken: nil,
	}, nil)

	replicationTasks, lastTaskID, err := s.replicationAckManager.getTasks(ctx, minTaskID, maxTaskID, batchSize)
	s.NoError(err)
	s.Empty(replicationTasks)
	s.Equal(maxTaskID, lastTaskID)
}

func (s *ackManagerSuite) TestGetTasks_PartialResult_Case1() {
	ctx := context.Background()
	minTaskID := rand.Int63()
	maxTaskID := minTaskID + 100
	batchSize := 100

	namespaceID := tests.NamespaceID
	workflowID := "some random workflow ID"

	historyTask := &tasks.HistoryReplicationTask{
		TaskID:       minTaskID + 10,
		WorkflowKey:  definition.NewWorkflowKey(namespaceID.String(), workflowID, uuid.New()),
		FirstEventID: rand.Int63(),
		NextEventID:  rand.Int63(),
	}
	activityTask := &tasks.SyncActivityTask{
		TaskID:      minTaskID + 20,
		WorkflowKey: definition.NewWorkflowKey(namespaceID.String(), workflowID, uuid.New()),
	}

	s.mockNamespaceCache.EXPECT().GetNamespaceByID(namespaceID).Return(tests.GlobalNamespaceEntry, nil).AnyTimes()
	s.mockExecutionMgr.EXPECT().GetHistoryTasks(ctx, &persistence.GetHistoryTasksRequest{
		ShardID:             s.mockShard.GetShardID(),
		TaskCategory:        tasks.CategoryReplication,
		InclusiveMinTaskKey: tasks.NewImmediateKey(minTaskID + 1),
		ExclusiveMaxTaskKey: tasks.NewImmediateKey(maxTaskID + 1),
		BatchSize:           batchSize,
		NextPageToken:       nil,
	}).Return(&persistence.GetHistoryTasksResponse{
		Tasks:         []tasks.Task{historyTask, activityTask},
		NextPageToken: nil,
	}, nil)
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), &persistence.GetWorkflowExecutionRequest{
		ShardID:     s.mockShard.GetShardID(),
		NamespaceID: historyTask.NamespaceID,
		WorkflowID:  historyTask.WorkflowID,
		RunID:       historyTask.RunID,
	}).Return(nil, serviceerror.NewUnavailable(""))

	_, _, err := s.replicationAckManager.getTasks(ctx, minTaskID, maxTaskID, batchSize)
	s.Error(err)
}

func (s *ackManagerSuite) TestGetTasks_PartialResult_Case2() {
	ctx := context.Background()
	minTaskID := rand.Int63()
	maxTaskID := minTaskID + 100
	batchSize := 100

	namespaceID := tests.NamespaceID
	workflowID := "some random workflow ID"

	historyTask := &tasks.HistoryReplicationTask{
		TaskID:       minTaskID + 10,
		WorkflowKey:  definition.NewWorkflowKey(namespaceID.String(), workflowID, uuid.New()),
		FirstEventID: 1,
		NextEventID:  1 + rand.Int63(),
		Version:      rand.Int63(),
		BranchToken:  []byte("random history branch token"),
	}
	activityTask := &tasks.SyncActivityTask{
		TaskID:           minTaskID + 20,
		WorkflowKey:      definition.NewWorkflowKey(namespaceID.String(), workflowID, uuid.New()),
		ScheduledEventID: rand.Int63(),
		Version:          rand.Int63(),
	}

	s.mockNamespaceCache.EXPECT().GetNamespaceByID(namespaceID).Return(tests.GlobalNamespaceEntry, nil).AnyTimes()
	s.mockExecutionMgr.EXPECT().GetHistoryTasks(ctx, &persistence.GetHistoryTasksRequest{
		ShardID:             s.mockShard.GetShardID(),
		TaskCategory:        tasks.CategoryReplication,
		InclusiveMinTaskKey: tasks.NewImmediateKey(minTaskID + 1),
		ExclusiveMaxTaskKey: tasks.NewImmediateKey(maxTaskID + 1),
		BatchSize:           batchSize,
		NextPageToken:       nil,
	}).Return(&persistence.GetHistoryTasksResponse{
		Tasks:         []tasks.Task{historyTask, activityTask},
		NextPageToken: nil,
	}, nil)

	context, release, _ := s.replicationAckManager.workflowCache.GetOrCreateWorkflowExecution(
		ctx,
		namespace.ID(historyTask.NamespaceID),
		commonpb.WorkflowExecution{
			WorkflowId: historyTask.WorkflowID,
			RunId:      historyTask.RunID,
		},
		workflow.CallerTypeTask,
	)
	context.(*workflow.ContextImpl).MutableState = s.mockMutableState
	s.mockMutableState.EXPECT().StartTransaction(gomock.Any()).Return(false, nil)
	versionHistories := &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{{
			BranchToken: historyTask.BranchToken,
			Items: []*historyspb.VersionHistoryItem{
				{
					EventId: historyTask.NextEventID - 1,
					Version: historyTask.Version,
				},
			},
		}},
	}
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{VersionHistories: versionHistories}).AnyTimes()
	s.mockExecutionMgr.EXPECT().ReadRawHistoryBranch(gomock.Any(), &persistence.ReadHistoryBranchRequest{
		ShardID:       s.mockShard.GetShardID(),
		MinEventID:    historyTask.FirstEventID,
		MaxEventID:    historyTask.NextEventID,
		BranchToken:   historyTask.BranchToken,
		PageSize:      1,
		NextPageToken: nil,
	}).Return(&persistence.ReadRawHistoryBranchResponse{
		HistoryEventBlobs: []*commonpb.DataBlob{{
			EncodingType: enumspb.ENCODING_TYPE_PROTO3,
			Data:         []byte("some random events blob"),
		}},
		NextPageToken: nil,
	}, nil)
	release(nil)

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), &persistence.GetWorkflowExecutionRequest{
		ShardID:     s.mockShard.GetShardID(),
		NamespaceID: activityTask.NamespaceID,
		WorkflowID:  activityTask.WorkflowID,
		RunID:       activityTask.RunID,
	}).Return(nil, serviceerror.NewUnavailable(""))

	replicationTasks, lastTaskID, err := s.replicationAckManager.getTasks(ctx, minTaskID, maxTaskID, batchSize)
	s.NoError(err)
	s.Equal(1, len(replicationTasks))
	s.Equal(historyTask.TaskID, lastTaskID)
}

func (s *ackManagerSuite) TestGetTasks_FullResult() {
	ctx := context.Background()
	minTaskID := rand.Int63()
	maxTaskID := minTaskID + 100
	batchSize := 100

	namespaceID := tests.NamespaceID
	workflowID := "some random workflow ID"

	historyTask := &tasks.HistoryReplicationTask{
		TaskID:       minTaskID + 10,
		WorkflowKey:  definition.NewWorkflowKey(namespaceID.String(), workflowID, uuid.New()),
		FirstEventID: 1,
		NextEventID:  1 + rand.Int63(),
		Version:      rand.Int63(),
		BranchToken:  []byte("random history branch token"),
	}

	s.mockNamespaceCache.EXPECT().GetNamespaceByID(namespaceID).Return(tests.GlobalNamespaceEntry, nil).AnyTimes()
	s.mockExecutionMgr.EXPECT().GetHistoryTasks(ctx, &persistence.GetHistoryTasksRequest{
		ShardID:             s.mockShard.GetShardID(),
		TaskCategory:        tasks.CategoryReplication,
		InclusiveMinTaskKey: tasks.NewImmediateKey(minTaskID + 1),
		ExclusiveMaxTaskKey: tasks.NewImmediateKey(maxTaskID + 1),
		BatchSize:           batchSize,
		NextPageToken:       nil,
	}).Return(&persistence.GetHistoryTasksResponse{
		Tasks:         []tasks.Task{historyTask},
		NextPageToken: nil,
	}, nil)

	context, release, _ := s.replicationAckManager.workflowCache.GetOrCreateWorkflowExecution(
		ctx,
		namespace.ID(historyTask.NamespaceID),
		commonpb.WorkflowExecution{
			WorkflowId: historyTask.WorkflowID,
			RunId:      historyTask.RunID,
		},
		workflow.CallerTypeTask,
	)
	context.(*workflow.ContextImpl).MutableState = s.mockMutableState
	s.mockMutableState.EXPECT().StartTransaction(gomock.Any()).Return(false, nil)
	versionHistories := &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{{
			BranchToken: historyTask.BranchToken,
			Items: []*historyspb.VersionHistoryItem{
				{
					EventId: historyTask.NextEventID - 1,
					Version: historyTask.Version,
				},
			},
		}},
	}
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{VersionHistories: versionHistories}).AnyTimes()
	s.mockExecutionMgr.EXPECT().ReadRawHistoryBranch(gomock.Any(), &persistence.ReadHistoryBranchRequest{
		ShardID:       s.mockShard.GetShardID(),
		MinEventID:    historyTask.FirstEventID,
		MaxEventID:    historyTask.NextEventID,
		BranchToken:   historyTask.BranchToken,
		PageSize:      1,
		NextPageToken: nil,
	}).Return(&persistence.ReadRawHistoryBranchResponse{
		HistoryEventBlobs: []*commonpb.DataBlob{{
			EncodingType: enumspb.ENCODING_TYPE_PROTO3,
			Data:         []byte("some random events blob"),
		}},
		NextPageToken: nil,
	}, nil)
	release(nil)

	replicationTasks, lastTaskID, err := s.replicationAckManager.getTasks(ctx, minTaskID, maxTaskID, batchSize)
	s.NoError(err)
	s.Equal(1, len(replicationTasks))
	s.Equal(historyTask.TaskID, lastTaskID)
}
