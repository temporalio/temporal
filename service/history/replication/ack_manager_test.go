package replication

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/util"
	"go.temporal.io/server/service/history/events"
	"go.temporal.io/server/service/history/hsm"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/workflow"
	wcache "go.temporal.io/server/service/history/workflow/cache"
	"go.uber.org/mock/gomock"
)

type (
	ackManagerSuite struct {
		suite.Suite
		*require.Assertions

		controller            *gomock.Controller
		mockShard             *shard.ContextTest
		mockNamespaceRegistry *namespace.MockRegistry
		mockMutableState      *historyi.MockMutableState
		mockClusterMetadata   *cluster.MockMetadata
		syncStateRetriever    *MockSyncStateRetriever

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
	s.mockMutableState = historyi.NewMockMutableState(s.controller)

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
	s.mockClusterMetadata.EXPECT().GetClusterID().Return(int64(1)).AnyTimes()
	s.mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(true).AnyTimes()
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(true, gomock.Any()).Return(cluster.TestCurrentClusterName).AnyTimes()

	s.logger = s.mockShard.GetLogger()
	workflowCache := wcache.NewHostLevelCache(s.mockShard.GetConfig(), s.mockShard.GetLogger(), metrics.NoopMetricsHandler)
	replicationProgressCache := NewProgressCache(s.mockShard.GetConfig(), s.mockShard.GetLogger(), metrics.NoopMetricsHandler)
	s.syncStateRetriever = NewMockSyncStateRetriever(s.controller)
	s.replicationAckManager = NewAckManager(
		s.mockShard, workflowCache, nil, replicationProgressCache, s.mockExecutionMgr, s.syncStateRetriever, s.logger,
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
		ArchetypeID: chasm.WorkflowArchetypeID,
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
		State: workflow.TestCloneToProto(context.Background(), ms)}, nil)
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
		State: workflow.TestCloneToProto(context.Background(), ms)}, nil).Times(s.replicationAckManager.pageSize())
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
		State: workflow.TestCloneToProto(context.Background(), ms)}, nil).Times(numTasks)
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
		State: workflow.TestCloneToProto(context.Background(), ms)}, nil).Times(s.replicationAckManager.pageSize())
	s.mockExecutionMgr.EXPECT().ReadRawHistoryBranch(gomock.Any(), gomock.Any()).Return(&persistence.ReadRawHistoryBranchResponse{
		HistoryEventBlobs: []*commonpb.DataBlob{{}}}, nil).Times(s.replicationAckManager.pageSize())

	replicationMessages, err := s.replicationAckManager.GetTasks(context.Background(), cluster.TestCurrentClusterName, 22)
	s.NoError(err)
	s.NotNil(replicationMessages)
	s.Len(replicationMessages.ReplicationTasks, s.replicationAckManager.pageSize())
	s.Equal(tasksResponse3.Tasks[len(tasksResponse3.Tasks)-1].GetTaskID(), replicationMessages.LastRetrievedMessageId)
}

func (s *ackManagerSuite) TestSubscribeUnsubscribeNotification() {
	ackMgrCh1, ackMgrSubID1 := s.replicationAckManager.SubscribeNotification(cluster.TestCurrentClusterName)
	s.NotNil(ackMgrCh1)
	s.NotEmpty(ackMgrSubID1)

	ackMgrCh2, ackMgrSubID2 := s.replicationAckManager.SubscribeNotification(cluster.TestAlternativeClusterName)
	s.NotNil(ackMgrCh2)
	s.NotEmpty(ackMgrSubID2)

	s.NotEqual(ackMgrSubID1, ackMgrSubID2)
	s.Len(s.replicationAckManager.subscribers, 2)
	s.Equal(cluster.TestCurrentClusterName, s.replicationAckManager.subscribers[ackMgrSubID1].clusterName)
	s.Equal(cluster.TestAlternativeClusterName, s.replicationAckManager.subscribers[ackMgrSubID2].clusterName)

	s.replicationAckManager.UnsubscribeNotification(ackMgrSubID1)
	s.Len(s.replicationAckManager.subscribers, 1)
	_, ok := s.replicationAckManager.subscribers[ackMgrSubID1]
	s.False(ok)

	// Unsubscribing an unknown ID is a no-op.
	s.replicationAckManager.UnsubscribeNotification("ackMgr-unknown-id")
	s.Len(s.replicationAckManager.subscribers, 1)

	s.replicationAckManager.UnsubscribeNotification(ackMgrSubID2)
	s.Empty(s.replicationAckManager.subscribers)
}

func (s *ackManagerSuite) TestNotifyNewTasks_BroadcastsToSubscribers() {
	ackMgrCh, _ := s.replicationAckManager.SubscribeNotification(cluster.TestCurrentClusterName)

	s.replicationAckManager.NotifyNewTasks([]tasks.Task{
		&tasks.HistoryReplicationTask{TaskID: 1, VisibilityTimestamp: time.Now()},
	})

	// Channel should have received the notification.
	select {
	case <-ackMgrCh:
	default:
		s.Fail("expected notification to be delivered")
	}
}

func (s *ackManagerSuite) TestNotifyNewTasks_Empty() {
	s.replicationAckManager.maxTaskID = util.Ptr(int64(50))
	s.replicationAckManager.NotifyNewTasks(nil)
	// No change since there are no tasks.
	s.Equal(int64(50), *s.replicationAckManager.maxTaskID)
}

func (s *ackManagerSuite) TestBroadcast_FullChannelAndDrain() {
	ackMgrCh, _ := s.replicationAckManager.SubscribeNotification(cluster.TestCurrentClusterName)

	// First broadcast fills the buffered (cap 1) channel.
	s.replicationAckManager.broadcast(3)
	select {
	case <-ackMgrCh:
	default:
		s.Fail("expected notification to be delivered")
	}

	// Re-fill the channel, then broadcast again while it is full -> hits the
	// default (backlog) branch without sending.
	s.replicationAckManager.broadcast(3)
	s.replicationAckManager.broadcast(5)

	// Drain and broadcast again -> sends successfully (backlog-recorded branch).
	<-ackMgrCh
	s.replicationAckManager.broadcast(2)
	select {
	case <-ackMgrCh:
	default:
		s.Fail("expected notification to be delivered after drain")
	}
}

func (s *ackManagerSuite) TestGetMaxTaskInfo_Uninitialized() {
	s.replicationAckManager.maxTaskID = nil
	s.replicationAckManager.maxTaskVisibilityTimestamp = time.Time{}

	maxTaskID, maxVisibilityTimestamp := s.replicationAckManager.GetMaxTaskInfo()
	expectMaxTaskID := s.mockShard.GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication).Prev().TaskID
	s.Equal(expectMaxTaskID, maxTaskID)
	s.False(maxVisibilityTimestamp.IsZero())
}

func (s *ackManagerSuite) TestConvertTask_DeleteExecution() {
	now := time.Now().UTC()
	task := &tasks.DeleteExecutionReplicationTask{
		WorkflowKey: definition.NewWorkflowKey(
			tests.NamespaceID.String(), tests.WorkflowID, tests.RunID,
		),
		VisibilityTimestamp: now,
		TaskID:              99,
	}

	replicationTask, err := s.replicationAckManager.ConvertTask(context.Background(), task)
	s.NoError(err)
	s.NotNil(replicationTask)
	s.EqualValues(99, replicationTask.GetSourceTaskId())
}

func (s *ackManagerSuite) TestConvertTask_Unknown() {
	replicationTask, err := s.replicationAckManager.ConvertTask(context.Background(), &tasks.WorkflowTask{})
	s.ErrorIs(err, errUnknownReplicationTask)
	s.Nil(replicationTask)
}

func (s *ackManagerSuite) TestConvertTask_StateTypes_WorkflowNotFound() {
	wfKey := definition.NewWorkflowKey(tests.NamespaceID.String(), tests.WorkflowID, tests.RunID)
	ackMgrTaskCases := []tasks.Task{
		&tasks.SyncActivityTask{WorkflowKey: wfKey, TaskID: 1, Version: 1, ScheduledEventID: 5},
		&tasks.SyncWorkflowStateTask{WorkflowKey: wfKey, TaskID: 2},
		&tasks.SyncHSMTask{WorkflowKey: wfKey, TaskID: 3},
	}
	for _, task := range ackMgrTaskCases {
		s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).
			Return(nil, serviceerror.NewNotFound("workflow not found"))
		replicationTask, err := s.replicationAckManager.ConvertTask(context.Background(), task)
		s.NoError(err)
		s.Nil(replicationTask)
	}
}

func (s *ackManagerSuite) TestConvertTaskByCluster_SyncVersionedTransition_WorkflowNotFound() {
	task := &tasks.SyncVersionedTransitionTask{
		WorkflowKey: definition.NewWorkflowKey(
			tests.NamespaceID.String(), tests.WorkflowID, tests.RunID,
		),
		TaskID: 77,
	}
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).
		Return(nil, serviceerror.NewNotFound("workflow not found"))

	replicationTask, err := s.replicationAckManager.ConvertTaskByCluster(context.Background(), task, int32(2))
	s.NoError(err)
	s.Nil(replicationTask)
}

func (s *ackManagerSuite) TestConvertTaskByCluster_DelegatesToConvertTask() {
	now := time.Now().UTC()
	task := &tasks.DeleteExecutionReplicationTask{
		WorkflowKey: definition.NewWorkflowKey(
			tests.NamespaceID.String(), tests.WorkflowID, tests.RunID,
		),
		VisibilityTimestamp: now,
		TaskID:              101,
	}

	replicationTask, err := s.replicationAckManager.ConvertTaskByCluster(context.Background(), task, int32(2))
	s.NoError(err)
	s.NotNil(replicationTask)
	s.EqualValues(101, replicationTask.GetSourceTaskId())
}

func (s *ackManagerSuite) TestGetTask_UnknownType() {
	taskInfo := &replicationspb.ReplicationTaskInfo{
		NamespaceId: tests.NamespaceID.String(),
		WorkflowId:  tests.WorkflowID,
		RunId:       tests.RunID,
		TaskType:    enumsspb.TASK_TYPE_UNSPECIFIED,
	}
	replicationTask, err := s.replicationAckManager.GetTask(context.Background(), taskInfo)
	s.Error(err)
	s.Nil(replicationTask)
}

func (s *ackManagerSuite) TestGetTask_SyncActivity_WorkflowNotFound() {
	taskInfo := &replicationspb.ReplicationTaskInfo{
		NamespaceId:      tests.NamespaceID.String(),
		WorkflowId:       tests.WorkflowID,
		RunId:            tests.RunID,
		TaskType:         enumsspb.TASK_TYPE_REPLICATION_SYNC_ACTIVITY,
		TaskId:           12,
		Version:          1,
		ScheduledEventId: 5,
	}
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).
		Return(nil, serviceerror.NewNotFound("workflow not found"))

	// Workflow not found -> mutable state load returns NotFound -> task converts to nil.
	replicationTask, err := s.replicationAckManager.GetTask(context.Background(), taskInfo)
	s.NoError(err)
	s.Nil(replicationTask)
}

func (s *ackManagerSuite) TestGetTask_StateTypes_WorkflowNotFound() {
	ackMgrTaskInfos := []*replicationspb.ReplicationTaskInfo{
		{
			NamespaceId: tests.NamespaceID.String(), WorkflowId: tests.WorkflowID, RunId: tests.RunID,
			TaskType: enumsspb.TASK_TYPE_REPLICATION_HISTORY, TaskId: 21, Version: 1, FirstEventId: 1, NextEventId: 2,
		},
		{
			NamespaceId: tests.NamespaceID.String(), WorkflowId: tests.WorkflowID, RunId: tests.RunID,
			TaskType: enumsspb.TASK_TYPE_REPLICATION_SYNC_WORKFLOW_STATE, TaskId: 22, Version: 1,
		},
		{
			NamespaceId: tests.NamespaceID.String(), WorkflowId: tests.WorkflowID, RunId: tests.RunID,
			TaskType: enumsspb.TASK_TYPE_REPLICATION_SYNC_HSM, TaskId: 23,
		},
	}
	for _, taskInfo := range ackMgrTaskInfos {
		s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).
			Return(nil, serviceerror.NewNotFound("workflow not found"))
		replicationTask, err := s.replicationAckManager.GetTask(context.Background(), taskInfo)
		s.NoError(err)
		s.Nil(replicationTask)
	}
}

func (s *ackManagerSuite) TestGetReplicationTasksIter() {
	ctx := context.Background()
	minTaskID := int64(100)
	maxTaskID := int64(200)

	tasksResponse := s.getHistoryTasksResponse(2)
	s.mockExecutionMgr.EXPECT().GetHistoryTasks(gomock.Any(), &persistence.GetHistoryTasksRequest{
		ShardID:             s.mockShard.GetShardID(),
		TaskCategory:        tasks.CategoryReplication,
		InclusiveMinTaskKey: tasks.NewImmediateKey(minTaskID),
		ExclusiveMaxTaskKey: tasks.NewImmediateKey(maxTaskID),
		BatchSize:           s.replicationAckManager.config.ReplicatorProcessorFetchTasksBatchSize(),
		NextPageToken:       nil,
	}).Return(tasksResponse, nil)

	iter, err := s.replicationAckManager.GetReplicationTasksIter(ctx, cluster.TestCurrentClusterName, minTaskID, maxTaskID)
	s.NoError(err)
	s.NotNil(iter)

	var collected []tasks.Task
	for iter.HasNext() {
		task, err := iter.Next()
		s.NoError(err)
		collected = append(collected, task)
	}
	s.Len(collected, 2)
}

func (s *ackManagerSuite) TestGetReplicationTasksIter_PersistenceError() {
	ctx := context.Background()
	iterErr := serviceerror.NewUnavailable("random error")
	s.mockExecutionMgr.EXPECT().GetHistoryTasks(gomock.Any(), gomock.Any()).Return(nil, iterErr)

	iter, err := s.replicationAckManager.GetReplicationTasksIter(ctx, cluster.TestCurrentClusterName, 100, 200)
	s.NoError(err)
	s.True(iter.HasNext())
	_, err = iter.Next()
	s.ErrorIs(err, iterErr)
}

func (s *ackManagerSuite) TestGetTasks_MinGreaterThanMax() {
	replicationTasks, lastTaskID, err := s.replicationAckManager.getTasks(
		context.Background(), cluster.TestCurrentClusterName, 200, 100,
	)
	s.Error(err)
	s.Nil(replicationTasks)
	s.EqualValues(0, lastTaskID)
}

func (s *ackManagerSuite) TestGetTasks_MinEqualsMax() {
	replicationTasks, lastTaskID, err := s.replicationAckManager.getTasks(
		context.Background(), cluster.TestCurrentClusterName, 100, 100,
	)
	s.NoError(err)
	s.Nil(replicationTasks)
	s.EqualValues(100, lastTaskID)
}

func (s *ackManagerSuite) TestGetTasks_NamespaceNotFound_SkipsTask() {
	ctx := context.Background()
	minTaskID := int64(220878)
	maxTaskID := minTaskID + 100

	missingNamespaceID := namespace.ID("ackMgr-missing-namespace")
	tasksResponse := s.getHistoryTasksResponse(1)
	tasksResponse.Tasks[0].(*tasks.HistoryReplicationTask).NamespaceID = missingNamespaceID.String()
	s.mockExecutionMgr.EXPECT().GetHistoryTasks(ctx, gomock.Any()).Return(tasksResponse, nil)
	s.mockNamespaceRegistry.EXPECT().GetNamespaceByID(missingNamespaceID).
		Return(nil, serviceerror.NewNamespaceNotFound("missing"))

	replicationTasks, lastTaskID, err := s.replicationAckManager.getTasks(ctx, cluster.TestCurrentClusterName, minTaskID, maxTaskID)
	s.NoError(err)
	s.Empty(replicationTasks)
	s.Equal(tasksResponse.Tasks[0].GetTaskID(), lastTaskID)
}

func (s *ackManagerSuite) TestGetTasks_NamespaceLookupError_ReturnsError() {
	ctx := context.Background()
	minTaskID := int64(220878)
	maxTaskID := minTaskID + 100

	badNamespaceID := namespace.ID("ackMgr-bad-namespace")
	lookupErr := serviceerror.NewUnavailable("namespace store unavailable")
	tasksResponse := s.getHistoryTasksResponse(1)
	tasksResponse.Tasks[0].(*tasks.HistoryReplicationTask).NamespaceID = badNamespaceID.String()
	s.mockExecutionMgr.EXPECT().GetHistoryTasks(ctx, gomock.Any()).Return(tasksResponse, nil)
	s.mockNamespaceRegistry.EXPECT().GetNamespaceByID(badNamespaceID).Return(nil, lookupErr)

	replicationTasks, lastTaskID, err := s.replicationAckManager.getTasks(ctx, cluster.TestCurrentClusterName, minTaskID, maxTaskID)
	s.ErrorIs(err, lookupErr)
	s.Nil(replicationTasks)
	s.EqualValues(0, lastTaskID)
}

func (s *ackManagerSuite) getHistoryTasksResponse(size int) *persistence.GetHistoryTasksResponse {
	result := &persistence.GetHistoryTasksResponse{}
	for i := 1; i <= size; i++ {
		result.Tasks = append(result.Tasks, &tasks.HistoryReplicationTask{
			WorkflowKey: definition.WorkflowKey{
				NamespaceID: tests.NamespaceID.String(),
				WorkflowID:  tests.WorkflowID + strconv.Itoa(i),
				RunID:       uuid.NewString(),
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
