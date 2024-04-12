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
	"strconv"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"

	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/util"
	"go.temporal.io/server/service/history/events"
	"go.temporal.io/server/service/history/hsm"
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

		controller            *gomock.Controller
		mockShard             *shard.ContextTest
		mockNamespaceRegistry *namespace.MockRegistry
		mockMutableState      *workflow.MockMutableState
		mockClusterMetadata   *cluster.MockMetadata

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
		&persistencespb.ShardInfo{
			ShardId: 0,
			RangeId: 1,
			Owner:   "test-shard-owner",
		},
		tests.NewDynamicConfig(),
	)

	reg := hsm.NewRegistry()
	err := workflow.RegisterStateMachine(reg)
	s.NoError(err)
	s.mockShard.SetStateMachineRegistry(reg)

	s.mockNamespaceRegistry = s.mockShard.Resource.NamespaceCache
	s.mockNamespaceRegistry.EXPECT().GetNamespaceByID(tests.NamespaceID).Return(tests.GlobalNamespaceEntry, nil).AnyTimes()

	s.mockExecutionMgr = s.mockShard.Resource.ExecutionMgr

	s.mockClusterMetadata = s.mockShard.Resource.ClusterMetadata
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(true).AnyTimes()
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(true, gomock.Any()).Return(cluster.TestCurrentClusterName).AnyTimes()

	s.logger = s.mockShard.GetLogger()
	workflowCache := wcache.NewHostLevelCache(s.mockShard.GetConfig(), metrics.NoopMetricsHandler)

	s.replicationAckManager = NewAckManager(
		s.mockShard, workflowCache, nil, s.mockExecutionMgr, s.logger,
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
	s.replicationAckManager.maxTaskID = util.Ptr(int64(123))

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
	expectMaxTaskID := s.mockShard.GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication).Prev().TaskID
	expectMinTaskID := expectMaxTaskID - 100
	s.replicationAckManager.maxTaskID = util.Ptr(expectMinTaskID - 100)

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
	expectMinTaskID := s.mockShard.GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication).TaskID - 100
	expectMaxTaskID := s.mockShard.GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication).TaskID - 50
	s.replicationAckManager.maxTaskID = util.Ptr(expectMaxTaskID)

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
	expectMinTaskID := s.mockShard.GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication).Prev().TaskID - 100
	expectMaxTaskID := s.mockShard.GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication).Prev().TaskID
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
	expectMinTaskID := s.mockShard.GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication).Prev().TaskID - 100
	expectMaxTaskID := s.mockShard.GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication).Prev().TaskID
	s.replicationAckManager.maxTaskID = util.Ptr(s.mockShard.GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication).TaskID - 50)

	minTaskID, maxTaskID := s.replicationAckManager.taskIDsRange(expectMinTaskID)
	s.Equal(expectMinTaskID, minTaskID)
	s.Equal(expectMaxTaskID, maxTaskID)
	s.NotEqual(sanityCheckTime, s.replicationAckManager.sanityCheckTime)
	s.Equal(expectMaxTaskID, *s.replicationAckManager.maxTaskID)
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

func (s *ackManagerSuite) TestGetTasks_NoTasksInDB() {
	ctx := context.Background()
	minTaskID := int64(220878)
	maxTaskID := minTaskID + 100

	s.mockExecutionMgr.EXPECT().GetHistoryTasks(ctx, &persistence.GetHistoryTasksRequest{
		ShardID:             s.mockShard.GetShardID(),
		TaskCategory:        tasks.CategoryReplication,
		InclusiveMinTaskKey: tasks.NewImmediateKey(minTaskID + 1),
		ExclusiveMaxTaskKey: tasks.NewImmediateKey(maxTaskID + 1),
		BatchSize:           s.replicationAckManager.pageSize(),
		NextPageToken:       nil,
	}).Return(s.getHistoryTasksResponse(0), nil)

	replicationTasks, lastTaskID, err := s.replicationAckManager.getTasks(ctx, cluster.TestCurrentClusterName, minTaskID, maxTaskID)
	s.NoError(err)
	s.Empty(replicationTasks)
	s.Equal(maxTaskID, lastTaskID)
}

func (s *ackManagerSuite) TestGetTasks_FirstPersistenceErrorReturnsErrorAndEmptyResult() {
	ctx := context.Background()
	minTaskID := int64(220878)
	maxTaskID := minTaskID + 100

	tasksResponse := s.getHistoryTasksResponse(2)
	s.mockExecutionMgr.EXPECT().GetHistoryTasks(ctx, &persistence.GetHistoryTasksRequest{
		ShardID:             s.mockShard.GetShardID(),
		TaskCategory:        tasks.CategoryReplication,
		InclusiveMinTaskKey: tasks.NewImmediateKey(minTaskID + 1),
		ExclusiveMaxTaskKey: tasks.NewImmediateKey(maxTaskID + 1),
		BatchSize:           s.replicationAckManager.pageSize(),
		NextPageToken:       nil,
	}).Return(tasksResponse, nil)

	gweErr := serviceerror.NewUnavailable("random error")
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), &persistence.GetWorkflowExecutionRequest{
		ShardID:     s.mockShard.GetShardID(),
		NamespaceID: tasksResponse.Tasks[0].GetNamespaceID(),
		WorkflowID:  tasksResponse.Tasks[0].GetWorkflowID(),
		RunID:       tasksResponse.Tasks[0].GetRunID(),
	}).Return(nil, gweErr)

	replicationTasks, lastTaskID, err := s.replicationAckManager.getTasks(ctx, cluster.TestCurrentClusterName, minTaskID, maxTaskID)
	s.Error(err)
	s.ErrorIs(err, gweErr)
	s.Empty(replicationTasks)
	s.EqualValues(0, lastTaskID)
}

func (s *ackManagerSuite) TestGetTasks_SecondPersistenceErrorReturnsPartialResult() {
	ctx := context.Background()
	minTaskID := int64(220878)
	maxTaskID := minTaskID + 100

	tasksResponse := s.getHistoryTasksResponse(2)
	s.mockExecutionMgr.EXPECT().GetHistoryTasks(ctx, &persistence.GetHistoryTasksRequest{
		ShardID:             s.mockShard.GetShardID(),
		TaskCategory:        tasks.CategoryReplication,
		InclusiveMinTaskKey: tasks.NewImmediateKey(minTaskID + 1),
		ExclusiveMaxTaskKey: tasks.NewImmediateKey(maxTaskID + 1),
		BatchSize:           s.replicationAckManager.pageSize(),
		NextPageToken:       nil,
	}).Return(tasksResponse, nil)

	eventsCache := events.NewHostLevelEventsCache(
		s.mockShard.GetExecutionManager(),
		s.mockShard.GetConfig(),
		s.mockShard.GetMetricsHandler(),
		s.mockShard.GetLogger(),
		false,
	)
	ms := workflow.TestLocalMutableState(s.mockShard, eventsCache, tests.GlobalNamespaceEntry, tests.WorkflowID, tests.RunID, log.NewTestLogger())
	ei := ms.GetExecutionInfo()
	ei.NamespaceId = tests.NamespaceID.String()
	ei.VersionHistories = &historyspb.VersionHistories{
		Histories: []*historyspb.VersionHistory{
			{
				Items: []*historyspb.VersionHistoryItem{
					{
						EventId: 1,
						Version: 1,
					},
				},
			},
		},
	}

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{
		State: workflow.TestCloneToProto(ms)}, nil)
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil, serviceerror.NewUnavailable("some random error"))
	s.mockExecutionMgr.EXPECT().ReadRawHistoryBranch(gomock.Any(), gomock.Any()).Return(&persistence.ReadRawHistoryBranchResponse{
		HistoryEventBlobs: []*commonpb.DataBlob{{}}}, nil)

	replicationTasks, lastTaskID, err := s.replicationAckManager.getTasks(ctx, cluster.TestCurrentClusterName, minTaskID, maxTaskID)
	s.NoError(err)
	s.Equal(1, len(replicationTasks))
	s.Equal(tasksResponse.Tasks[0].GetTaskID(), lastTaskID)
}

func (s *ackManagerSuite) TestGetTasks_FullPage() {
	tasksResponse := s.getHistoryTasksResponse(s.replicationAckManager.pageSize())
	tasksResponse.NextPageToken = []byte{22, 3, 83} // There is more in DB.
	minTaskID, maxTaskID := s.replicationAckManager.taskIDsRange(22)
	s.mockExecutionMgr.EXPECT().GetHistoryTasks(gomock.Any(), &persistence.GetHistoryTasksRequest{
		ShardID:             s.mockShard.GetShardID(),
		TaskCategory:        tasks.CategoryReplication,
		InclusiveMinTaskKey: tasks.NewImmediateKey(minTaskID + 1),
		ExclusiveMaxTaskKey: tasks.NewImmediateKey(maxTaskID + 1),
		BatchSize:           s.replicationAckManager.pageSize(),
		NextPageToken:       nil,
	}).Return(tasksResponse, nil)

	eventsCache := events.NewHostLevelEventsCache(
		s.mockShard.GetExecutionManager(),
		s.mockShard.GetConfig(),
		s.mockShard.GetMetricsHandler(),
		s.mockShard.GetLogger(),
		false,
	)
	ms := workflow.TestLocalMutableState(s.mockShard, eventsCache, tests.GlobalNamespaceEntry, tests.WorkflowID, tests.RunID, log.NewTestLogger())
	ei := ms.GetExecutionInfo()
	ei.NamespaceId = tests.NamespaceID.String()
	ei.VersionHistories = &historyspb.VersionHistories{
		Histories: []*historyspb.VersionHistory{
			{
				Items: []*historyspb.VersionHistoryItem{
					{
						EventId: 1,
						Version: 1,
					},
				},
			},
		},
	}

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{
		State: workflow.TestCloneToProto(ms)}, nil).Times(s.replicationAckManager.pageSize())
	s.mockExecutionMgr.EXPECT().ReadRawHistoryBranch(gomock.Any(), gomock.Any()).Return(&persistence.ReadRawHistoryBranchResponse{
		HistoryEventBlobs: []*commonpb.DataBlob{{}}}, nil).Times(s.replicationAckManager.pageSize())

	replicationMessages, err := s.replicationAckManager.GetTasks(context.Background(), cluster.TestCurrentClusterName, 22)
	s.NoError(err)
	s.NotNil(replicationMessages)
	s.Len(replicationMessages.ReplicationTasks, s.replicationAckManager.pageSize())
	s.Equal(tasksResponse.Tasks[len(tasksResponse.Tasks)-1].GetTaskID(), replicationMessages.LastRetrievedMessageId)

}
func (s *ackManagerSuite) TestGetTasks_PartialPage() {
	numTasks := s.replicationAckManager.pageSize() / 2
	tasksResponse := s.getHistoryTasksResponse(numTasks)
	minTaskID, maxTaskID := s.replicationAckManager.taskIDsRange(22)
	s.mockExecutionMgr.EXPECT().GetHistoryTasks(gomock.Any(), &persistence.GetHistoryTasksRequest{
		ShardID:             s.mockShard.GetShardID(),
		TaskCategory:        tasks.CategoryReplication,
		InclusiveMinTaskKey: tasks.NewImmediateKey(minTaskID + 1),
		ExclusiveMaxTaskKey: tasks.NewImmediateKey(maxTaskID + 1),
		BatchSize:           s.replicationAckManager.pageSize(),
		NextPageToken:       nil,
	}).Return(tasksResponse, nil)

	eventsCache := events.NewHostLevelEventsCache(
		s.mockShard.GetExecutionManager(),
		s.mockShard.GetConfig(),
		s.mockShard.GetMetricsHandler(),
		s.mockShard.GetLogger(),
		false,
	)
	ms := workflow.TestLocalMutableState(s.mockShard, eventsCache, tests.GlobalNamespaceEntry, tests.WorkflowID, tests.RunID, log.NewTestLogger())
	ei := ms.GetExecutionInfo()
	ei.NamespaceId = tests.NamespaceID.String()
	ei.VersionHistories = &historyspb.VersionHistories{
		Histories: []*historyspb.VersionHistory{
			{
				Items: []*historyspb.VersionHistoryItem{
					{
						EventId: 1,
						Version: 1,
					},
				},
			},
		},
	}

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{
		State: workflow.TestCloneToProto(ms)}, nil).Times(numTasks)
	s.mockExecutionMgr.EXPECT().ReadRawHistoryBranch(gomock.Any(), gomock.Any()).Return(&persistence.ReadRawHistoryBranchResponse{
		HistoryEventBlobs: []*commonpb.DataBlob{{}}}, nil).Times(numTasks)

	replicationMessages, err := s.replicationAckManager.GetTasks(context.Background(), cluster.TestCurrentClusterName, 22)
	s.NoError(err)
	s.NotNil(replicationMessages)
	s.Len(replicationMessages.ReplicationTasks, numTasks)
	s.Equal(tasksResponse.Tasks[len(tasksResponse.Tasks)-1].GetTaskID(), replicationMessages.LastRetrievedMessageId)
}

func (s *ackManagerSuite) TestGetTasks_FilterNamespace() {
	notExistOnTestClusterNamespaceID := namespace.ID("not-exist-on-" + cluster.TestCurrentClusterName)
	notExistOnTestClusterNamespaceEntry := namespace.NewLocalNamespaceForTest(
		&persistencespb.NamespaceInfo{},
		&persistencespb.NamespaceConfig{},
		"not-a-"+cluster.TestCurrentClusterName,
	)
	s.mockNamespaceRegistry.EXPECT().GetNamespaceByID(notExistOnTestClusterNamespaceID).Return(notExistOnTestClusterNamespaceEntry, nil).AnyTimes()

	minTaskID, maxTaskID := s.replicationAckManager.taskIDsRange(22)

	tasksResponse1 := s.getHistoryTasksResponse(s.replicationAckManager.pageSize())
	// 2 of 25 tasks are for namespace that doesn't exist on poll cluster.
	tasksResponse1.Tasks[1].(*tasks.HistoryReplicationTask).NamespaceID = notExistOnTestClusterNamespaceID.String()
	tasksResponse1.Tasks[3].(*tasks.HistoryReplicationTask).NamespaceID = notExistOnTestClusterNamespaceID.String()
	tasksResponse1.NextPageToken = []byte{22, 3, 83} // There is more in DB.
	s.mockExecutionMgr.EXPECT().GetHistoryTasks(gomock.Any(), &persistence.GetHistoryTasksRequest{
		ShardID:             s.mockShard.GetShardID(),
		TaskCategory:        tasks.CategoryReplication,
		InclusiveMinTaskKey: tasks.NewImmediateKey(minTaskID + 1),
		ExclusiveMaxTaskKey: tasks.NewImmediateKey(maxTaskID + 1),
		BatchSize:           s.replicationAckManager.pageSize(),
		NextPageToken:       nil,
	}).Return(tasksResponse1, nil)

	tasksResponse2 := s.getHistoryTasksResponse(2)
	// 1 of 2 task is for namespace that doesn't exist on poll cluster.
	tasksResponse2.Tasks[1].(*tasks.HistoryReplicationTask).NamespaceID = notExistOnTestClusterNamespaceID.String()
	tasksResponse2.NextPageToken = []byte{22, 8, 78} // There is more in DB.
	s.mockExecutionMgr.EXPECT().GetHistoryTasks(gomock.Any(), &persistence.GetHistoryTasksRequest{
		ShardID:             s.mockShard.GetShardID(),
		TaskCategory:        tasks.CategoryReplication,
		InclusiveMinTaskKey: tasks.NewImmediateKey(minTaskID + 1),
		ExclusiveMaxTaskKey: tasks.NewImmediateKey(maxTaskID + 1),
		BatchSize:           s.replicationAckManager.pageSize(),
		NextPageToken:       []byte{22, 3, 83}, // previous token
	}).Return(tasksResponse2, nil)

	tasksResponse3 := s.getHistoryTasksResponse(1)
	s.mockExecutionMgr.EXPECT().GetHistoryTasks(gomock.Any(), &persistence.GetHistoryTasksRequest{
		ShardID:             s.mockShard.GetShardID(),
		TaskCategory:        tasks.CategoryReplication,
		InclusiveMinTaskKey: tasks.NewImmediateKey(minTaskID + 1),
		ExclusiveMaxTaskKey: tasks.NewImmediateKey(maxTaskID + 1),
		BatchSize:           s.replicationAckManager.pageSize(),
		NextPageToken:       []byte{22, 8, 78}, // previous token
	}).Return(tasksResponse3, nil)

	eventsCache := events.NewHostLevelEventsCache(
		s.mockShard.GetExecutionManager(),
		s.mockShard.GetConfig(),
		s.mockShard.GetMetricsHandler(),
		s.mockShard.GetLogger(),
		false,
	)
	ms := workflow.TestLocalMutableState(s.mockShard, eventsCache, tests.GlobalNamespaceEntry, tests.WorkflowID, tests.RunID, log.NewTestLogger())
	ei := ms.GetExecutionInfo()
	ei.NamespaceId = tests.NamespaceID.String()
	ei.VersionHistories = &historyspb.VersionHistories{
		Histories: []*historyspb.VersionHistory{
			{
				Items: []*historyspb.VersionHistoryItem{
					{
						EventId: 1,
						Version: 1,
					},
				},
			},
		},
	}

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{
		State: workflow.TestCloneToProto(ms)}, nil).Times(s.replicationAckManager.pageSize())
	s.mockExecutionMgr.EXPECT().ReadRawHistoryBranch(gomock.Any(), gomock.Any()).Return(&persistence.ReadRawHistoryBranchResponse{
		HistoryEventBlobs: []*commonpb.DataBlob{{}}}, nil).Times(s.replicationAckManager.pageSize())

	replicationMessages, err := s.replicationAckManager.GetTasks(context.Background(), cluster.TestCurrentClusterName, 22)
	s.NoError(err)
	s.NotNil(replicationMessages)
	s.Len(replicationMessages.ReplicationTasks, s.replicationAckManager.pageSize())
	s.Equal(tasksResponse3.Tasks[len(tasksResponse3.Tasks)-1].GetTaskID(), replicationMessages.LastRetrievedMessageId)
}

func (s *ackManagerSuite) getHistoryTasksResponse(size int) *persistence.GetHistoryTasksResponse {
	result := &persistence.GetHistoryTasksResponse{}
	for i := 1; i <= size; i++ {
		result.Tasks = append(result.Tasks, &tasks.HistoryReplicationTask{
			WorkflowKey: definition.WorkflowKey{
				NamespaceID: tests.NamespaceID.String(),
				WorkflowID:  tests.WorkflowID + strconv.Itoa(i),
				RunID:       uuid.New(),
			},
			TaskID:       int64(i),
			FirstEventID: 1,
			NextEventID:  1,
			Version:      1,
		},
		)
	}

	return result
}
