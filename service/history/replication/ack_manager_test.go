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
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
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
	historyCache := workflow.NewCache(s.mockShard)

	s.replicationAckManager = NewAckManager(
		s.mockShard, historyCache, s.mockExecutionMgr, s.logger,
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
	expectMaxTaskID := s.mockShard.GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication, s.replicationAckManager.currentClusterName).Prev().TaskID
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
	expectMinTaskID := s.mockShard.GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication, s.replicationAckManager.currentClusterName).TaskID - 100
	expectMaxTaskID := s.mockShard.GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication, s.replicationAckManager.currentClusterName).TaskID - 50
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
	expectMinTaskID := s.mockShard.GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication, s.replicationAckManager.currentClusterName).Prev().TaskID - 100
	expectMaxTaskID := s.mockShard.GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication, s.replicationAckManager.currentClusterName).Prev().TaskID
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
	expectMinTaskID := s.mockShard.GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication, s.replicationAckManager.currentClusterName).Prev().TaskID - 100
	expectMaxTaskID := s.mockShard.GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication, s.replicationAckManager.currentClusterName).Prev().TaskID
	s.replicationAckManager.maxTaskID = convert.Int64Ptr(s.mockShard.GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication, s.replicationAckManager.currentClusterName).TaskID - 50)

	minTaskID, maxTaskID := s.replicationAckManager.taskIDsRange(expectMinTaskID)
	s.Equal(expectMinTaskID, minTaskID)
	s.Equal(expectMaxTaskID, maxTaskID)
	s.NotEqual(sanityCheckTime, s.replicationAckManager.sanityCheckTime)
	s.Equal(expectMaxTaskID, *s.replicationAckManager.maxTaskID)
}

func (s *ackManagerSuite) TestSyncActivity_WorkflowMissing() {
	ctx := context.Background()
	namespaceName := namespace.Name("some random namespace name")
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
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(namespaceID).Return(namespace.NewGlobalNamespaceForTest(
		&persistencespb.NamespaceInfo{Id: namespaceID.String(), Name: namespaceName.String()},
		&persistencespb.NamespaceConfig{Retention: timestamp.DurationFromDays(1)},
		&persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []string{
				cluster.TestCurrentClusterName,
				cluster.TestAlternativeClusterName,
			},
		},
		1234,
	), nil).AnyTimes()

	result, err := s.replicationAckManager.generateSyncActivityTask(ctx, task)
	s.NoError(err)
	s.Nil(result)
}

func (s *ackManagerSuite) TestSyncActivity_WorkflowCompleted() {
	ctx := context.Background()
	namespaceName := namespace.Name("some random namespace name")
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

	context, release, _ := s.replicationAckManager.historyCache.GetOrCreateWorkflowExecution(
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
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(namespaceID).Return(namespace.NewGlobalNamespaceForTest(
		&persistencespb.NamespaceInfo{Id: namespaceID.String(), Name: namespaceName.String()},
		&persistencespb.NamespaceConfig{Retention: timestamp.DurationFromDays(1)},
		&persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []string{
				cluster.TestCurrentClusterName,
				cluster.TestAlternativeClusterName,
			},
		},
		version,
	), nil).AnyTimes()

	result, err := s.replicationAckManager.generateSyncActivityTask(ctx, task)
	s.NoError(err)
	s.Nil(result)
}

func (s *ackManagerSuite) TestSyncActivity_ActivityCompleted() {
	ctx := context.Background()
	namespaceName := namespace.Name("some random namespace name")
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

	context, release, _ := s.replicationAckManager.historyCache.GetOrCreateWorkflowExecution(
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
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(namespaceID).Return(namespace.NewGlobalNamespaceForTest(
		&persistencespb.NamespaceInfo{Id: namespaceID.String(), Name: namespaceName.String()},
		&persistencespb.NamespaceConfig{Retention: timestamp.DurationFromDays(1)},
		&persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []string{
				cluster.TestCurrentClusterName,
				cluster.TestAlternativeClusterName,
			},
		},
		version,
	), nil).AnyTimes()

	result, err := s.replicationAckManager.generateSyncActivityTask(ctx, task)
	s.NoError(err)
	s.Nil(result)
}

func (s *ackManagerSuite) TestSyncActivity_ActivityRetry() {
	ctx := context.Background()
	namespaceName := namespace.Name("some random namespace name")
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

	context, release, _ := s.replicationAckManager.historyCache.GetOrCreateWorkflowExecution(
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
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(namespaceID).Return(namespace.NewGlobalNamespaceForTest(
		&persistencespb.NamespaceInfo{Id: namespaceID.String(), Name: namespaceName.String()},
		&persistencespb.NamespaceConfig{Retention: timestamp.DurationFromDays(1)},
		&persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []string{
				cluster.TestCurrentClusterName,
				cluster.TestAlternativeClusterName,
			},
		},
		version,
	), nil).AnyTimes()

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
	namespaceName := namespace.Name("some random namespace name")
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

	context, release, _ := s.replicationAckManager.historyCache.GetOrCreateWorkflowExecution(
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
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(namespaceID).Return(namespace.NewGlobalNamespaceForTest(
		&persistencespb.NamespaceInfo{Id: namespaceID.String(), Name: namespaceName.String()},
		&persistencespb.NamespaceConfig{Retention: timestamp.DurationFromDays(1)},
		&persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []string{
				cluster.TestCurrentClusterName,
				cluster.TestAlternativeClusterName,
			},
		},
		version,
	), nil).AnyTimes()

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
