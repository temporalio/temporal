package replication

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/server/api/adminservicemock/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/common/resourcetest"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/service/history/configs"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type (
	taskProcessorSuite struct {
		suite.Suite
		*require.Assertions
		protorequire.ProtoAssertions

		controller                  *gomock.Controller
		mockResource                *resourcetest.Test
		mockShard                   *shard.ContextTest
		mockEngine                  *historyi.MockEngine
		mockNamespaceCache          *namespace.MockRegistry
		mockClientBean              *client.MockBean
		mockAdminClient             *adminservicemock.MockAdminServiceClient
		mockClusterMetadata         *cluster.MockMetadata
		mockHistoryClient           *historyservicemock.MockHistoryServiceClient
		mockReplicationTaskExecutor *MockTaskExecutor
		mockReplicationTaskFetcher  *MocktaskFetcher

		mockExecutionManager *persistence.MockExecutionManager

		shardID     int32
		config      *configs.Config
		requestChan chan *replicationTaskRequest

		replicationTaskProcessor *taskProcessorImpl
	}
)

func TestTaskProcessorSuite(t *testing.T) {
	s := new(taskProcessorSuite)
	suite.Run(t, s)
}

func (s *taskProcessorSuite) SetupSuite() {
}

func (s *taskProcessorSuite) TearDownSuite() {
}

func (s *taskProcessorSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.ProtoAssertions = protorequire.New(s.T())
	s.controller = gomock.NewController(s.T())

	s.config = tests.NewDynamicConfig()
	s.requestChan = make(chan *replicationTaskRequest, 10)

	s.shardID = rand.Int31()
	s.mockShard = shard.NewTestContext(
		s.controller,
		persistencespb.ShardInfo_builder{
			ShardId: s.shardID,
			RangeId: 1,
		}.Build(),
		s.config,
	)
	s.mockEngine = historyi.NewMockEngine(s.controller)
	s.mockResource = s.mockShard.Resource
	s.mockNamespaceCache = s.mockResource.NamespaceCache
	s.mockClientBean = s.mockResource.ClientBean
	s.mockAdminClient = s.mockResource.RemoteAdminClient
	s.mockClusterMetadata = s.mockResource.ClusterMetadata
	s.mockExecutionManager = s.mockResource.ExecutionMgr
	s.mockReplicationTaskExecutor = NewMockTaskExecutor(s.controller)
	s.mockHistoryClient = historyservicemock.NewMockHistoryServiceClient(s.controller)
	s.mockReplicationTaskFetcher = NewMocktaskFetcher(s.controller)
	rateLimiter := quotas.NewDefaultOutgoingRateLimiter(
		func() float64 { return 100 },
	)
	s.mockReplicationTaskFetcher.EXPECT().getSourceCluster().Return(cluster.TestAlternativeClusterName).AnyTimes()
	s.mockReplicationTaskFetcher.EXPECT().getRequestChan().Return(s.requestChan).AnyTimes()
	s.mockReplicationTaskFetcher.EXPECT().getRateLimiter().Return(rateLimiter).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestAllClusterInfo).AnyTimes()

	metricsClient := metrics.NoopMetricsHandler

	s.replicationTaskProcessor = NewTaskProcessor(
		s.shardID,
		s.mockShard,
		s.mockEngine,
		s.config,
		metricsClient,
		s.mockReplicationTaskFetcher,
		s.mockReplicationTaskExecutor,
		serialization.NewSerializer(),
		NewExecutionManagerDLQWriter(s.mockExecutionManager),
	).(*taskProcessorImpl)
}

func (s *taskProcessorSuite) TearDownTest() {
	s.controller.Finish()
	s.mockShard.StopForTest()
}

func (s *taskProcessorSuite) TestHandleSyncShardStatus_Stale() {
	now := timestamppb.New(time.Now().Add(-2 * dropSyncShardTaskTimeThreshold))
	err := s.replicationTaskProcessor.handleSyncShardStatus(replicationspb.SyncShardStatus_builder{
		StatusTime: now,
	}.Build())
	s.NoError(err)
}

func (s *taskProcessorSuite) TestHandleSyncShardStatus_Success() {
	now := timestamp.TimeNowPtrUtc()
	s.mockEngine.EXPECT().SyncShardStatus(gomock.Any(), historyservice.SyncShardStatusRequest_builder{
		SourceCluster: cluster.TestAlternativeClusterName,
		ShardId:       s.shardID,
		StatusTime:    now,
	}.Build()).Return(nil)

	err := s.replicationTaskProcessor.handleSyncShardStatus(replicationspb.SyncShardStatus_builder{
		StatusTime: now,
	}.Build())
	s.NoError(err)
}

func (s *taskProcessorSuite) TestHandleReplicationTask_SyncActivity() {
	namespaceID := uuid.NewString()
	workflowID := uuid.NewString()
	runID := uuid.NewString()
	now := time.Now()
	attempt := int32(2)
	task := replicationspb.ReplicationTask_builder{
		TaskType: enumsspb.REPLICATION_TASK_TYPE_SYNC_ACTIVITY_TASK,
		SyncActivityTaskAttributes: replicationspb.SyncActivityTaskAttributes_builder{
			NamespaceId: namespaceID,
			WorkflowId:  workflowID,
			RunId:       runID,
			Attempt:     attempt,
		}.Build(),
		VisibilityTime: timestamppb.New(now),
	}.Build()

	s.mockReplicationTaskExecutor.EXPECT().Execute(gomock.Any(), task, false).Return(nil)
	err := s.replicationTaskProcessor.handleReplicationTask(context.Background(), task)
	s.NoError(err)
}

func (s *taskProcessorSuite) TestHandleReplicationTask_History() {
	namespaceID := uuid.NewString()
	workflowID := uuid.NewString()
	runID := uuid.NewString()
	now := time.Now()
	events := []*historypb.HistoryEvent{historypb.HistoryEvent_builder{
		EventId: 1,
		Version: 1,
	}.Build()}
	versionHistory := []*historyspb.VersionHistoryItem{historyspb.VersionHistoryItem_builder{
		EventId: 1,
		Version: 1,
	}.Build()}
	serializer := s.mockResource.GetPayloadSerializer()
	data, err := serializer.SerializeEvents(events)
	s.NoError(err)

	task := replicationspb.ReplicationTask_builder{
		TaskType: enumsspb.REPLICATION_TASK_TYPE_HISTORY_V2_TASK,
		HistoryTaskAttributes: replicationspb.HistoryTaskAttributes_builder{
			NamespaceId: namespaceID,
			WorkflowId:  workflowID,
			RunId:       runID,
			Events: commonpb.DataBlob_builder{
				EncodingType: enumspb.ENCODING_TYPE_PROTO3,
				Data:         data.GetData(),
			}.Build(),
			VersionHistoryItems: versionHistory,
		}.Build(),
		VisibilityTime: timestamppb.New(now),
	}.Build()

	s.mockReplicationTaskExecutor.EXPECT().Execute(gomock.Any(), task, false).Return(nil)
	err = s.replicationTaskProcessor.handleReplicationTask(context.Background(), task)
	s.NoError(err)
}

func (s *taskProcessorSuite) TestHandleReplicationTask_Panic() {
	task := &replicationspb.ReplicationTask{}

	s.mockReplicationTaskExecutor.EXPECT().Execute(gomock.Any(), task, false).DoAndReturn(
		func(_ context.Context, _ *replicationspb.ReplicationTask, _ bool) error {
			panic("test replication task panic")
		},
	)
	err := s.replicationTaskProcessor.handleReplicationTask(context.Background(), task)
	s.Error(err)
}

func (s *taskProcessorSuite) TestHandleReplicationDLQTask_SyncActivity() {
	namespaceID := uuid.NewString()
	workflowID := uuid.NewString()
	runID := uuid.NewString()
	request := &persistence.PutReplicationTaskToDLQRequest{
		ShardID:           s.shardID,
		SourceClusterName: cluster.TestAlternativeClusterName,
		TaskInfo: persistencespb.ReplicationTaskInfo_builder{
			NamespaceId: namespaceID,
			WorkflowId:  workflowID,
			RunId:       runID,
			TaskType:    enumsspb.TASK_TYPE_REPLICATION_SYNC_ACTIVITY,
		}.Build(),
	}

	s.mockExecutionManager.EXPECT().PutReplicationTaskToDLQ(gomock.Any(), request).Return(nil)
	err := s.replicationTaskProcessor.handleReplicationDLQTask(context.Background(), request)
	s.NoError(err)
}

func (s *taskProcessorSuite) TestHandleReplicationDLQTask_SyncWorkflowState() {
	namespaceID := uuid.NewString()
	workflowID := uuid.NewString()
	runID := uuid.NewString()

	request := &persistence.PutReplicationTaskToDLQRequest{
		ShardID:           s.shardID,
		SourceClusterName: cluster.TestAlternativeClusterName,
		TaskInfo: persistencespb.ReplicationTaskInfo_builder{
			NamespaceId: namespaceID,
			WorkflowId:  workflowID,
			RunId:       runID,
			TaskType:    enumsspb.TASK_TYPE_REPLICATION_SYNC_WORKFLOW_STATE,
			Version:     1,
		}.Build(),
	}

	s.mockExecutionManager.EXPECT().PutReplicationTaskToDLQ(gomock.Any(), request).Return(nil)
	err := s.replicationTaskProcessor.handleReplicationDLQTask(context.Background(), request)
	s.NoError(err)
}

func (s *taskProcessorSuite) TestHandleReplicationDLQTask_History() {
	namespaceID := uuid.NewString()
	workflowID := uuid.NewString()
	runID := uuid.NewString()

	request := &persistence.PutReplicationTaskToDLQRequest{
		ShardID:           s.shardID,
		SourceClusterName: cluster.TestAlternativeClusterName,
		TaskInfo: persistencespb.ReplicationTaskInfo_builder{
			NamespaceId:  namespaceID,
			WorkflowId:   workflowID,
			RunId:        runID,
			TaskType:     enumsspb.TASK_TYPE_REPLICATION_HISTORY,
			FirstEventId: 1,
			NextEventId:  1,
			Version:      1,
		}.Build(),
	}

	s.mockExecutionManager.EXPECT().PutReplicationTaskToDLQ(gomock.Any(), request).Return(nil)
	err := s.replicationTaskProcessor.handleReplicationDLQTask(context.Background(), request)
	s.NoError(err)
}

func (s *taskProcessorSuite) TestConvertTaskToDLQTask_SyncActivity() {
	namespaceID := uuid.NewString()
	workflowID := uuid.NewString()
	runID := uuid.NewString()
	task := replicationspb.ReplicationTask_builder{
		TaskType: enumsspb.REPLICATION_TASK_TYPE_SYNC_ACTIVITY_TASK,
		SyncActivityTaskAttributes: replicationspb.SyncActivityTaskAttributes_builder{
			NamespaceId: namespaceID,
			WorkflowId:  workflowID,
			RunId:       runID,
			Attempt:     1,
		}.Build(),
	}.Build()
	request := &persistence.PutReplicationTaskToDLQRequest{
		ShardID:           s.shardID,
		SourceClusterName: cluster.TestAlternativeClusterName,
		TaskInfo: persistencespb.ReplicationTaskInfo_builder{
			NamespaceId: namespaceID,
			WorkflowId:  workflowID,
			RunId:       runID,
			TaskType:    enumsspb.TASK_TYPE_REPLICATION_SYNC_ACTIVITY,
		}.Build(),
	}

	dlqTask, err := s.replicationTaskProcessor.convertTaskToDLQTask(task)
	s.NoError(err)
	s.Equal(request, dlqTask)
}

func (s *taskProcessorSuite) TestConvertTaskToDLQTask_SyncWorkflowState() {
	namespaceID := uuid.NewString()
	workflowID := uuid.NewString()
	runID := uuid.NewString()
	task := replicationspb.ReplicationTask_builder{
		TaskType: enumsspb.REPLICATION_TASK_TYPE_SYNC_WORKFLOW_STATE_TASK,
		SyncWorkflowStateTaskAttributes: replicationspb.SyncWorkflowStateTaskAttributes_builder{
			WorkflowState: persistencespb.WorkflowMutableState_builder{
				ExecutionInfo: persistencespb.WorkflowExecutionInfo_builder{
					NamespaceId: namespaceID,
					WorkflowId:  workflowID,
					VersionHistories: versionhistory.NewVersionHistories(
						versionhistory.NewVersionHistory(nil, []*historyspb.VersionHistoryItem{versionhistory.NewVersionHistoryItem(1, 1)}),
					),
				}.Build(),
				ExecutionState: persistencespb.WorkflowExecutionState_builder{
					RunId: runID,
				}.Build(),
			}.Build(),
		}.Build(),
	}.Build()
	request := &persistence.PutReplicationTaskToDLQRequest{
		ShardID:           s.shardID,
		SourceClusterName: cluster.TestAlternativeClusterName,
		TaskInfo: persistencespb.ReplicationTaskInfo_builder{
			NamespaceId: namespaceID,
			WorkflowId:  workflowID,
			RunId:       runID,
			TaskType:    enumsspb.TASK_TYPE_REPLICATION_SYNC_WORKFLOW_STATE,
			Version:     1,
		}.Build(),
	}

	dlqTask, err := s.replicationTaskProcessor.convertTaskToDLQTask(task)
	s.NoError(err)
	s.Equal(request, dlqTask)
}

func (s *taskProcessorSuite) TestConvertTaskToDLQTask_SyncHSM() {
	namespaceID := uuid.NewString()
	workflowID := uuid.NewString()
	runID := uuid.NewString()
	task := replicationspb.ReplicationTask_builder{
		SourceTaskId: rand.Int63(),
		TaskType:     enumsspb.REPLICATION_TASK_TYPE_SYNC_HSM_TASK,
		SyncHsmAttributes: replicationspb.SyncHSMAttributes_builder{
			NamespaceId: namespaceID,
			WorkflowId:  workflowID,
			RunId:       runID,
			VersionHistory: historyspb.VersionHistory_builder{
				BranchToken: []byte("branchToken"),
				Items: []*historyspb.VersionHistoryItem{historyspb.VersionHistoryItem_builder{
					EventId: 10,
					Version: 20,
				}.Build()},
			}.Build(),
			StateMachineNode: persistencespb.StateMachineNode_builder{
				Data: []byte("stateMachineData"),
			}.Build(),
		}.Build(),
		VisibilityTime: timestamppb.New(time.Now()),
	}.Build()
	request := &persistence.PutReplicationTaskToDLQRequest{
		ShardID:           s.shardID,
		SourceClusterName: cluster.TestAlternativeClusterName,
		TaskInfo: persistencespb.ReplicationTaskInfo_builder{
			NamespaceId:    namespaceID,
			WorkflowId:     workflowID,
			RunId:          runID,
			TaskId:         task.GetSourceTaskId(),
			TaskType:       enumsspb.TASK_TYPE_REPLICATION_SYNC_HSM,
			VisibilityTime: task.GetVisibilityTime(),
		}.Build(),
	}

	dlqTask, err := s.replicationTaskProcessor.convertTaskToDLQTask(task)
	s.NoError(err)
	s.Equal(request, dlqTask)
}

func (s *taskProcessorSuite) TestConvertTaskToDLQTask_History() {
	namespaceID := uuid.NewString()
	workflowID := uuid.NewString()
	runID := uuid.NewString()
	events := []*historypb.HistoryEvent{historypb.HistoryEvent_builder{
		EventId: 1,
		Version: 1,
	}.Build()}
	versionHistory := []*historyspb.VersionHistoryItem{historyspb.VersionHistoryItem_builder{
		EventId: 1,
		Version: 1,
	}.Build()}
	serializer := s.mockResource.GetPayloadSerializer()
	data, err := serializer.SerializeEvents(events)
	s.NoError(err)

	task := replicationspb.ReplicationTask_builder{
		TaskType: enumsspb.REPLICATION_TASK_TYPE_HISTORY_V2_TASK,
		HistoryTaskAttributes: replicationspb.HistoryTaskAttributes_builder{
			NamespaceId: namespaceID,
			WorkflowId:  workflowID,
			RunId:       runID,
			Events: commonpb.DataBlob_builder{
				EncodingType: enumspb.ENCODING_TYPE_PROTO3,
				Data:         data.GetData(),
			}.Build(),
			VersionHistoryItems: versionHistory,
		}.Build(),
	}.Build()
	request := &persistence.PutReplicationTaskToDLQRequest{
		ShardID:           s.shardID,
		SourceClusterName: cluster.TestAlternativeClusterName,
		TaskInfo: persistencespb.ReplicationTaskInfo_builder{
			NamespaceId:  namespaceID,
			WorkflowId:   workflowID,
			RunId:        runID,
			TaskType:     enumsspb.TASK_TYPE_REPLICATION_HISTORY,
			FirstEventId: 1,
			NextEventId:  2,
			Version:      1,
		}.Build(),
	}

	dlqTask, err := s.replicationTaskProcessor.convertTaskToDLQTask(task)
	s.NoError(err)
	s.Equal(request, dlqTask)
}

func (s *taskProcessorSuite) TestPaginationFn_Success_More() {
	namespaceID := uuid.NewString()
	workflowID := uuid.NewString()
	runID := uuid.NewString()
	events := []*historypb.HistoryEvent{historypb.HistoryEvent_builder{
		EventId: 1,
		Version: 1,
	}.Build()}
	versionHistory := []*historyspb.VersionHistoryItem{historyspb.VersionHistoryItem_builder{
		EventId: 1,
		Version: 1,
	}.Build()}
	serializer := s.mockResource.GetPayloadSerializer()
	data, err := serializer.SerializeEvents(events)
	s.NoError(err)

	syncShardTask := replicationspb.SyncShardStatus_builder{
		StatusTime: timestamp.TimeNowPtrUtc(),
	}.Build()
	taskID := int64(123)
	lastRetrievedMessageID := 2 * taskID
	task := replicationspb.ReplicationTask_builder{
		SourceTaskId: taskID,
		TaskType:     enumsspb.REPLICATION_TASK_TYPE_HISTORY_V2_TASK,
		HistoryTaskAttributes: replicationspb.HistoryTaskAttributes_builder{
			NamespaceId: namespaceID,
			WorkflowId:  workflowID,
			RunId:       runID,
			Events: commonpb.DataBlob_builder{
				EncodingType: enumspb.ENCODING_TYPE_PROTO3,
				Data:         data.GetData(),
			}.Build(),
			VersionHistoryItems: versionHistory,
		}.Build(),
	}.Build()

	maxRxProcessedTaskID := rand.Int63()
	maxRxReceivedTaskID := rand.Int63()
	rxTaskBackoff := time.Duration(rand.Int63())
	s.replicationTaskProcessor.maxRxProcessedTaskID = maxRxProcessedTaskID
	s.replicationTaskProcessor.maxRxReceivedTaskID = maxRxReceivedTaskID
	s.replicationTaskProcessor.rxTaskBackoff = rxTaskBackoff

	requestToken := replicationspb.ReplicationToken_builder{
		ShardId:                     s.shardID,
		LastProcessedMessageId:      maxRxProcessedTaskID,
		LastProcessedVisibilityTime: nil,
		LastRetrievedMessageId:      maxRxReceivedTaskID,
	}.Build()

	go func() {
		request := <-s.requestChan
		defer close(request.respChan)
		s.Equal(requestToken, request.token)
		request.respChan <- replicationspb.ReplicationMessages_builder{
			SyncShardStatus:        syncShardTask,
			ReplicationTasks:       []*replicationspb.ReplicationTask{task},
			LastRetrievedMessageId: lastRetrievedMessageID,
			HasMore:                true,
		}.Build()
	}()

	tasks, _, err := s.replicationTaskProcessor.paginationFn(nil)
	s.NoError(err)
	s.Equal(1, len(tasks))
	s.Equal(task, tasks[0].(*replicationspb.ReplicationTask))
	s.Equal(syncShardTask, <-s.replicationTaskProcessor.syncShardChan)
	s.Equal(lastRetrievedMessageID, s.replicationTaskProcessor.maxRxReceivedTaskID)
	s.Equal(time.Duration(0), s.replicationTaskProcessor.rxTaskBackoff)
}

func (s *taskProcessorSuite) TestPaginationFn_Success_NoMore() {
	namespaceID := uuid.NewString()
	workflowID := uuid.NewString()
	runID := uuid.NewString()
	events := []*historypb.HistoryEvent{historypb.HistoryEvent_builder{
		EventId: 1,
		Version: 1,
	}.Build()}
	versionHistory := []*historyspb.VersionHistoryItem{historyspb.VersionHistoryItem_builder{
		EventId: 1,
		Version: 1,
	}.Build()}
	serializer := s.mockResource.GetPayloadSerializer()
	data, err := serializer.SerializeEvents(events)
	s.NoError(err)

	syncShardTask := replicationspb.SyncShardStatus_builder{
		StatusTime: timestamp.TimeNowPtrUtc(),
	}.Build()
	taskID := int64(123)
	lastRetrievedMessageID := 2 * taskID
	task := replicationspb.ReplicationTask_builder{
		SourceTaskId: taskID,
		TaskType:     enumsspb.REPLICATION_TASK_TYPE_HISTORY_V2_TASK,
		HistoryTaskAttributes: replicationspb.HistoryTaskAttributes_builder{
			NamespaceId: namespaceID,
			WorkflowId:  workflowID,
			RunId:       runID,
			Events: commonpb.DataBlob_builder{
				EncodingType: enumspb.ENCODING_TYPE_PROTO3,
				Data:         data.GetData(),
			}.Build(),
			VersionHistoryItems: versionHistory,
		}.Build(),
	}.Build()

	maxRxProcessedTaskID := rand.Int63()
	maxRxReceivedTaskID := rand.Int63()
	rxTaskBackoff := time.Duration(rand.Int63())
	s.replicationTaskProcessor.maxRxProcessedTaskID = maxRxProcessedTaskID
	s.replicationTaskProcessor.maxRxReceivedTaskID = maxRxReceivedTaskID
	s.replicationTaskProcessor.rxTaskBackoff = rxTaskBackoff

	requestToken := replicationspb.ReplicationToken_builder{
		ShardId:                     s.shardID,
		LastProcessedMessageId:      maxRxProcessedTaskID,
		LastProcessedVisibilityTime: nil,
		LastRetrievedMessageId:      maxRxReceivedTaskID,
	}.Build()

	go func() {
		request := <-s.requestChan
		defer close(request.respChan)
		s.Equal(requestToken, request.token)
		request.respChan <- replicationspb.ReplicationMessages_builder{
			SyncShardStatus:        syncShardTask,
			ReplicationTasks:       []*replicationspb.ReplicationTask{task},
			LastRetrievedMessageId: lastRetrievedMessageID,
			HasMore:                false,
		}.Build()
	}()

	tasks, _, err := s.replicationTaskProcessor.paginationFn(nil)
	s.NoError(err)
	s.Equal(1, len(tasks))
	s.Equal(task, tasks[0].(*replicationspb.ReplicationTask))
	s.Equal(syncShardTask, <-s.replicationTaskProcessor.syncShardChan)
	s.Equal(lastRetrievedMessageID, s.replicationTaskProcessor.maxRxReceivedTaskID)
	s.NotEqual(time.Duration(0), s.replicationTaskProcessor.rxTaskBackoff)
}

func (s *taskProcessorSuite) TestPaginationFn_Error() {
	maxRxProcessedTaskID := rand.Int63()
	maxRxReceivedTaskID := rand.Int63()
	rxTaskBackoff := time.Duration(rand.Int63())
	s.replicationTaskProcessor.maxRxProcessedTaskID = maxRxProcessedTaskID
	s.replicationTaskProcessor.maxRxReceivedTaskID = maxRxReceivedTaskID
	s.replicationTaskProcessor.rxTaskBackoff = rxTaskBackoff

	requestToken := replicationspb.ReplicationToken_builder{
		ShardId:                     s.shardID,
		LastProcessedMessageId:      maxRxProcessedTaskID,
		LastProcessedVisibilityTime: nil,
		LastRetrievedMessageId:      maxRxReceivedTaskID,
	}.Build()

	go func() {
		request := <-s.requestChan
		defer close(request.respChan)
		s.ProtoEqual(requestToken, request.token)
	}()

	tasks, _, err := s.replicationTaskProcessor.paginationFn(nil)
	s.NoError(err)
	s.Empty(tasks)
	select {
	case <-s.replicationTaskProcessor.syncShardChan:
		s.Fail("should not receive any sync shard task")
	default:
		s.Equal(maxRxProcessedTaskID, s.replicationTaskProcessor.maxRxProcessedTaskID)
		s.Equal(maxRxReceivedTaskID, s.replicationTaskProcessor.maxRxReceivedTaskID)
		s.Equal(rxTaskBackoff, s.replicationTaskProcessor.rxTaskBackoff)
	}
}
