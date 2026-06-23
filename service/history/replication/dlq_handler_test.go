package replication

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/adminservicemock/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/resourcetest"
	"go.temporal.io/server/service/history/configs"
	deletemanager "go.temporal.io/server/service/history/deletemanager"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
	wcache "go.temporal.io/server/service/history/workflow/cache"
	"go.uber.org/mock/gomock"
)

type (
	dlqHandlerSuite struct {
		suite.Suite
		*require.Assertions
		controller *gomock.Controller

		mockResource     *resourcetest.Test
		mockShard        *shard.ContextTest
		config           *configs.Config
		mockClientBean   *client.MockBean
		adminClient      *adminservicemock.MockAdminServiceClient
		clusterMetadata  *cluster.MockMetadata
		executionManager *persistence.MockExecutionManager
		shardManager     *persistence.MockShardManager
		taskExecutor     *MockTaskExecutor
		taskExecutors    map[string]TaskExecutor
		sourceCluster    string

		replicationMessageHandler *dlqHandlerImpl
	}
)

func TestDLQHandlerSuite(t *testing.T) {
	s := new(dlqHandlerSuite)
	suite.Run(t, s)
}

func (s *dlqHandlerSuite) SetupSuite() {

}

func (s *dlqHandlerSuite) TearDownSuite() {

}

func (s *dlqHandlerSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.controller = gomock.NewController(s.T())

	s.mockShard = shard.NewTestContext(
		s.controller,
		&persistencespb.ShardInfo{
			ShardId:                0,
			RangeId:                1,
			ReplicationDlqAckLevel: map[string]int64{cluster.TestAlternativeClusterName: persistence.EmptyQueueMessageID},
		},
		tests.NewDynamicConfig(),
	)
	s.mockResource = s.mockShard.Resource
	s.mockClientBean = s.mockResource.ClientBean
	s.adminClient = s.mockResource.RemoteAdminClient
	s.clusterMetadata = s.mockResource.ClusterMetadata
	s.executionManager = s.mockResource.ExecutionMgr
	s.shardManager = s.mockResource.ShardMgr
	s.config = tests.NewDynamicConfig()
	s.clusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.clusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestAllClusterInfo).AnyTimes()
	s.taskExecutors = make(map[string]TaskExecutor)
	s.taskExecutor = NewMockTaskExecutor(s.controller)
	s.sourceCluster = cluster.TestAlternativeClusterName
	s.taskExecutors[s.sourceCluster] = s.taskExecutor

	s.replicationMessageHandler = newDLQHandler(
		s.mockShard,
		deletemanager.NewMockDeleteManager(s.controller),
		wcache.NewMockCache(s.controller),
		s.mockClientBean,
		s.taskExecutors,
		func(params TaskExecutorParams) TaskExecutor {
			return NewTaskExecutor(
				params.RemoteCluster,
				params.Shard,
				params.RemoteHistoryFetcher,
				params.DeleteManager,
				params.WorkflowCache,
			)
		},
	)
}

func (s *dlqHandlerSuite) TearDownTest() {
	s.controller.Finish()
	s.mockShard.StopForTest()
}

func (s *dlqHandlerSuite) TestReadMessages_OK() {
	ctx := context.Background()

	namespaceID := uuid.NewString()
	workflowID := uuid.NewString()
	runID := uuid.NewString()
	taskID := int64(12345)
	version := int64(2333)
	firstEventID := int64(144)
	nextEventID := int64(233)

	lastMessageID := int64(1394)
	pageSize := 1
	pageToken := []byte("some random token")
	dbResp := &persistence.GetHistoryTasksResponse{
		Tasks: []tasks.Task{&tasks.HistoryReplicationTask{
			WorkflowKey: definition.NewWorkflowKey(
				namespaceID,
				workflowID,
				runID,
			),
			Version:      version,
			FirstEventID: firstEventID,
			NextEventID:  nextEventID,
			TaskID:       taskID,
		}},
		NextPageToken: pageToken,
	}

	remoteTask := &replicationspb.ReplicationTask{
		TaskType:     enumsspb.REPLICATION_TASK_TYPE_HISTORY_TASK,
		SourceTaskId: taskID,
		Attributes: &replicationspb.ReplicationTask_HistoryTaskAttributes{
			HistoryTaskAttributes: &replicationspb.HistoryTaskAttributes{
				NamespaceId: namespaceID,
				WorkflowId:  workflowID,
				RunId:       runID,
				VersionHistoryItems: []*historyspb.VersionHistoryItem{{
					Version: version,
					EventId: nextEventID - 1,
				}},
				Events: &commonpb.DataBlob{},
			},
		},
	}

	s.executionManager.EXPECT().GetReplicationTasksFromDLQ(gomock.Any(), &persistence.GetReplicationTasksFromDLQRequest{
		GetHistoryTasksRequest: persistence.GetHistoryTasksRequest{
			ShardID:             s.mockShard.GetShardID(),
			TaskCategory:        tasks.CategoryReplication,
			InclusiveMinTaskKey: tasks.NewImmediateKey(persistence.EmptyQueueMessageID + 1),
			ExclusiveMaxTaskKey: tasks.NewImmediateKey(lastMessageID + 1),
			BatchSize:           pageSize,
			NextPageToken:       pageToken,
		},
		SourceClusterName: s.sourceCluster,
	}).Return(dbResp, nil)

	s.mockClientBean.EXPECT().GetRemoteAdminClient(s.sourceCluster).Return(s.adminClient, nil).AnyTimes()
	s.adminClient.EXPECT().GetDLQReplicationMessages(ctx, gomock.Any()).
		Return(&adminservice.GetDLQReplicationMessagesResponse{
			ReplicationTasks: []*replicationspb.ReplicationTask{remoteTask},
		}, nil)
	taskList, tasksInfo, token, err := s.replicationMessageHandler.GetMessages(ctx, s.sourceCluster, lastMessageID, pageSize, pageToken)
	s.NoError(err)
	s.Equal(pageToken, token)
	s.Equal([]*replicationspb.ReplicationTask{remoteTask}, taskList)
	s.Equal(namespaceID, tasksInfo[0].GetNamespaceId())
	s.Equal(workflowID, tasksInfo[0].GetWorkflowId())
	s.Equal(taskID, tasksInfo[0].GetTaskId())
	s.Equal(version, tasksInfo[0].GetVersion())
	s.Equal(firstEventID, tasksInfo[0].GetFirstEventId())
	s.Equal(nextEventID, tasksInfo[0].GetNextEventId())
}

func (s *dlqHandlerSuite) TestPurgeMessages() {
	lastMessageID := int64(1)

	s.executionManager.EXPECT().RangeDeleteReplicationTaskFromDLQ(
		gomock.Any(),
		&persistence.RangeDeleteReplicationTaskFromDLQRequest{
			RangeCompleteHistoryTasksRequest: persistence.RangeCompleteHistoryTasksRequest{
				ShardID:             s.mockShard.GetShardID(),
				TaskCategory:        tasks.CategoryReplication,
				InclusiveMinTaskKey: tasks.NewImmediateKey(persistence.EmptyQueueMessageID + 1),
				ExclusiveMaxTaskKey: tasks.NewImmediateKey(lastMessageID + 1),
			},
			SourceClusterName: s.sourceCluster,
		}).Return(nil)

	s.shardManager.EXPECT().UpdateShard(gomock.Any(), gomock.Any()).Return(nil)
	err := s.replicationMessageHandler.PurgeMessages(context.Background(), s.sourceCluster, lastMessageID)
	s.NoError(err)
}
func (s *dlqHandlerSuite) TestMergeMessages() {
	ctx := context.Background()

	namespaceID := uuid.NewString()
	workflowID := uuid.NewString()
	runID := uuid.NewString()
	taskID := int64(12345)
	version := int64(2333)
	firstEventID := int64(144)
	nextEventID := int64(233)

	lastMessageID := int64(1394)
	pageSize := 1
	pageToken := []byte("some random token")

	dbResp := &persistence.GetHistoryTasksResponse{
		Tasks: []tasks.Task{&tasks.HistoryReplicationTask{
			WorkflowKey: definition.NewWorkflowKey(
				namespaceID,
				workflowID,
				runID,
			),
			Version:      version,
			FirstEventID: firstEventID,
			NextEventID:  nextEventID,
			TaskID:       taskID,
		}},
		NextPageToken: pageToken,
	}

	remoteTask := &replicationspb.ReplicationTask{
		TaskType:     enumsspb.REPLICATION_TASK_TYPE_HISTORY_TASK,
		SourceTaskId: taskID,
		Attributes: &replicationspb.ReplicationTask_HistoryTaskAttributes{
			HistoryTaskAttributes: &replicationspb.HistoryTaskAttributes{
				NamespaceId: namespaceID,
				WorkflowId:  workflowID,
				RunId:       runID,
				VersionHistoryItems: []*historyspb.VersionHistoryItem{{
					Version: version,
					EventId: nextEventID - 1,
				}},
				Events: &commonpb.DataBlob{},
			},
		},
	}

	s.executionManager.EXPECT().GetReplicationTasksFromDLQ(gomock.Any(), &persistence.GetReplicationTasksFromDLQRequest{
		GetHistoryTasksRequest: persistence.GetHistoryTasksRequest{
			ShardID:             s.mockShard.GetShardID(),
			TaskCategory:        tasks.CategoryReplication,
			InclusiveMinTaskKey: tasks.NewImmediateKey(persistence.EmptyQueueMessageID + 1),
			ExclusiveMaxTaskKey: tasks.NewImmediateKey(lastMessageID + 1),
			BatchSize:           pageSize,
			NextPageToken:       pageToken,
		},
		SourceClusterName: s.sourceCluster,
	}).Return(dbResp, nil)

	s.mockClientBean.EXPECT().GetRemoteAdminClient(s.sourceCluster).Return(s.adminClient, nil).AnyTimes()
	s.adminClient.EXPECT().GetDLQReplicationMessages(ctx, gomock.Any()).
		Return(&adminservice.GetDLQReplicationMessagesResponse{
			ReplicationTasks: []*replicationspb.ReplicationTask{remoteTask},
		}, nil)
	s.taskExecutor.EXPECT().Execute(gomock.Any(), remoteTask, true).Return(nil)
	s.executionManager.EXPECT().RangeDeleteReplicationTaskFromDLQ(gomock.Any(), &persistence.RangeDeleteReplicationTaskFromDLQRequest{
		RangeCompleteHistoryTasksRequest: persistence.RangeCompleteHistoryTasksRequest{
			ShardID:             s.mockShard.GetShardID(),
			TaskCategory:        tasks.CategoryReplication,
			InclusiveMinTaskKey: tasks.NewImmediateKey(persistence.EmptyQueueMessageID + 1),
			ExclusiveMaxTaskKey: tasks.NewImmediateKey(lastMessageID + 1),
		},
		SourceClusterName: s.sourceCluster,
	}).Return(nil)

	s.shardManager.EXPECT().UpdateShard(gomock.Any(), gomock.Any()).Return(nil)

	token, err := s.replicationMessageHandler.MergeMessages(ctx, s.sourceCluster, lastMessageID, pageSize, pageToken)
	s.NoError(err)
	s.Equal(pageToken, token)
}

var dlqHErrTest = fmt.Errorf("dlqH test error")

func (s *dlqHandlerSuite) TestNewLazyDLQHandler() {
	handler := NewLazyDLQHandler(
		s.mockShard,
		deletemanager.NewMockDeleteManager(s.controller),
		wcache.NewMockCache(s.controller),
		s.mockClientBean,
		func(params TaskExecutorParams) TaskExecutor {
			return s.taskExecutor
		},
	)
	s.NotNil(handler)
}

func (s *dlqHandlerSuite) TestNewDLQHandler_NilTaskExecutors_Panics() {
	s.Panics(func() {
		newDLQHandler(
			s.mockShard,
			deletemanager.NewMockDeleteManager(s.controller),
			wcache.NewMockCache(s.controller),
			s.mockClientBean,
			nil,
			func(params TaskExecutorParams) TaskExecutor {
				return s.taskExecutor
			},
		)
	})
}

func (s *dlqHandlerSuite) TestPurgeMessages_UpdateAckLevelError_Ignored() {
	lastMessageID := int64(1)
	s.executionManager.EXPECT().RangeDeleteReplicationTaskFromDLQ(gomock.Any(), gomock.Any()).Return(nil)
	s.shardManager.EXPECT().UpdateShard(gomock.Any(), gomock.Any()).Return(dlqHErrTest).AnyTimes()
	err := s.replicationMessageHandler.PurgeMessages(context.Background(), s.sourceCluster, lastMessageID)
	s.NoError(err)
}

func (s *dlqHandlerSuite) TestPurgeMessages_RangeDeleteError() {
	lastMessageID := int64(1)
	s.executionManager.EXPECT().RangeDeleteReplicationTaskFromDLQ(gomock.Any(), gomock.Any()).Return(dlqHErrTest)
	err := s.replicationMessageHandler.PurgeMessages(context.Background(), s.sourceCluster, lastMessageID)
	s.Equal(dlqHErrTest, err)
}

func (s *dlqHandlerSuite) TestReadMessages_GetFromDLQError() {
	s.executionManager.EXPECT().GetReplicationTasksFromDLQ(gomock.Any(), gomock.Any()).Return(nil, dlqHErrTest)
	taskList, tasksInfo, token, err := s.replicationMessageHandler.GetMessages(context.Background(), s.sourceCluster, int64(100), 1, nil)
	s.Equal(dlqHErrTest, err)
	s.Nil(taskList)
	s.Nil(tasksInfo)
	s.Nil(token)
}

func (s *dlqHandlerSuite) TestReadMessages_EmptyTaskInfo() {
	pageToken := []byte("next page")
	dbResp := &persistence.GetHistoryTasksResponse{
		Tasks:         []tasks.Task{},
		NextPageToken: pageToken,
	}
	s.executionManager.EXPECT().GetReplicationTasksFromDLQ(gomock.Any(), gomock.Any()).Return(dbResp, nil)
	taskList, tasksInfo, token, err := s.replicationMessageHandler.GetMessages(context.Background(), s.sourceCluster, int64(100), 1, nil)
	s.NoError(err)
	s.Nil(taskList)
	s.Nil(tasksInfo)
	s.Equal(pageToken, token)
}

func (s *dlqHandlerSuite) TestReadMessages_AllTaskTypes() {
	wfKey := definition.NewWorkflowKey(uuid.NewString(), uuid.NewString(), uuid.NewString())
	dbResp := &persistence.GetHistoryTasksResponse{
		Tasks: []tasks.Task{
			&tasks.SyncActivityTask{WorkflowKey: wfKey, TaskID: 1, ScheduledEventID: 7},
			&tasks.HistoryReplicationTask{WorkflowKey: wfKey, TaskID: 2, FirstEventID: 1, NextEventID: 2},
			&tasks.SyncWorkflowStateTask{WorkflowKey: wfKey, TaskID: 3},
			&tasks.SyncHSMTask{WorkflowKey: wfKey, TaskID: 4},
		},
	}
	s.executionManager.EXPECT().GetReplicationTasksFromDLQ(gomock.Any(), gomock.Any()).Return(dbResp, nil)
	s.adminClient.EXPECT().GetDLQReplicationMessages(gomock.Any(), gomock.Any()).
		Return(&adminservice.GetDLQReplicationMessagesResponse{ReplicationTasks: nil}, nil)
	_, tasksInfo, _, err := s.replicationMessageHandler.GetMessages(context.Background(), s.sourceCluster, int64(100), 10, nil)
	s.NoError(err)
	s.Len(tasksInfo, 4)
	s.Equal(enumsspb.TASK_TYPE_REPLICATION_SYNC_ACTIVITY, tasksInfo[0].GetTaskType())
	s.Equal(enumsspb.TASK_TYPE_REPLICATION_HISTORY, tasksInfo[1].GetTaskType())
	s.Equal(enumsspb.TASK_TYPE_REPLICATION_SYNC_WORKFLOW_STATE, tasksInfo[2].GetTaskType())
	s.Equal(enumsspb.TASK_TYPE_REPLICATION_SYNC_HSM, tasksInfo[3].GetTaskType())
}

func (s *dlqHandlerSuite) TestReadMessages_GetDLQReplicationMessagesError() {
	dbResp := &persistence.GetHistoryTasksResponse{
		Tasks: []tasks.Task{&tasks.HistoryReplicationTask{
			WorkflowKey: definition.NewWorkflowKey(uuid.NewString(), uuid.NewString(), uuid.NewString()),
			TaskID:      int64(1),
		}},
	}
	s.executionManager.EXPECT().GetReplicationTasksFromDLQ(gomock.Any(), gomock.Any()).Return(dbResp, nil)
	s.adminClient.EXPECT().GetDLQReplicationMessages(gomock.Any(), gomock.Any()).Return(nil, dlqHErrTest)
	_, _, _, err := s.replicationMessageHandler.GetMessages(context.Background(), s.sourceCluster, int64(100), 1, nil)
	s.Equal(dlqHErrTest, err)
}

func (s *dlqHandlerSuite) TestMergeMessages_ReadError() {
	s.executionManager.EXPECT().GetReplicationTasksFromDLQ(gomock.Any(), gomock.Any()).Return(nil, dlqHErrTest)
	token, err := s.replicationMessageHandler.MergeMessages(context.Background(), s.sourceCluster, int64(100), 1, nil)
	s.Equal(dlqHErrTest, err)
	s.Nil(token)
}

func (s *dlqHandlerSuite) TestMergeMessages_ExecuteError() {
	taskID := int64(12345)
	dbResp := &persistence.GetHistoryTasksResponse{
		Tasks: []tasks.Task{&tasks.HistoryReplicationTask{
			WorkflowKey: definition.NewWorkflowKey(uuid.NewString(), uuid.NewString(), uuid.NewString()),
			TaskID:      taskID,
		}},
	}
	remoteTask := &replicationspb.ReplicationTask{
		TaskType:     enumsspb.REPLICATION_TASK_TYPE_HISTORY_TASK,
		SourceTaskId: taskID,
	}
	s.executionManager.EXPECT().GetReplicationTasksFromDLQ(gomock.Any(), gomock.Any()).Return(dbResp, nil)
	s.adminClient.EXPECT().GetDLQReplicationMessages(gomock.Any(), gomock.Any()).
		Return(&adminservice.GetDLQReplicationMessagesResponse{ReplicationTasks: []*replicationspb.ReplicationTask{remoteTask}}, nil)
	s.taskExecutor.EXPECT().Execute(gomock.Any(), remoteTask, true).Return(dlqHErrTest)
	token, err := s.replicationMessageHandler.MergeMessages(context.Background(), s.sourceCluster, int64(100), 1, nil)
	s.Equal(dlqHErrTest, err)
	s.Nil(token)
}

func (s *dlqHandlerSuite) TestMergeMessages_RangeDeleteError() {
	taskID := int64(12345)
	dbResp := &persistence.GetHistoryTasksResponse{
		Tasks: []tasks.Task{&tasks.HistoryReplicationTask{
			WorkflowKey: definition.NewWorkflowKey(uuid.NewString(), uuid.NewString(), uuid.NewString()),
			TaskID:      taskID,
		}},
	}
	remoteTask := &replicationspb.ReplicationTask{
		TaskType:     enumsspb.REPLICATION_TASK_TYPE_HISTORY_TASK,
		SourceTaskId: taskID,
	}
	s.executionManager.EXPECT().GetReplicationTasksFromDLQ(gomock.Any(), gomock.Any()).Return(dbResp, nil)
	s.adminClient.EXPECT().GetDLQReplicationMessages(gomock.Any(), gomock.Any()).
		Return(&adminservice.GetDLQReplicationMessagesResponse{ReplicationTasks: []*replicationspb.ReplicationTask{remoteTask}}, nil)
	s.taskExecutor.EXPECT().Execute(gomock.Any(), remoteTask, true).Return(nil)
	s.executionManager.EXPECT().RangeDeleteReplicationTaskFromDLQ(gomock.Any(), gomock.Any()).Return(dlqHErrTest)
	token, err := s.replicationMessageHandler.MergeMessages(context.Background(), s.sourceCluster, int64(100), 1, nil)
	s.Equal(dlqHErrTest, err)
	s.Nil(token)
}

func (s *dlqHandlerSuite) TestGetOrCreateTaskExecutor_CreatesAndCaches() {
	newCluster := "dlqH-new-cluster"
	callCount := 0
	handler := newDLQHandler(
		s.mockShard,
		deletemanager.NewMockDeleteManager(s.controller),
		wcache.NewMockCache(s.controller),
		s.mockClientBean,
		make(map[string]TaskExecutor),
		func(params TaskExecutorParams) TaskExecutor {
			callCount++
			s.Equal(newCluster, params.RemoteCluster)
			return s.taskExecutor
		},
	)
	exec1, err := handler.getOrCreateTaskExecutor(newCluster)
	s.NoError(err)
	s.Equal(s.taskExecutor, exec1)
	exec2, err := handler.getOrCreateTaskExecutor(newCluster)
	s.NoError(err)
	s.Equal(s.taskExecutor, exec2)
	s.Equal(1, callCount)
}
