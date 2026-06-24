package replication

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/adminservicemock/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/common/resourcetest"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/common/testing/testhooks"
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
		&persistencespb.ShardInfo{
			ShardId: s.shardID,
			RangeId: 1,
		},
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
		testhooks.TestHooks{},
		NewExecutionManagerDLQWriter(s.mockExecutionManager),
	).(*taskProcessorImpl)
}

func (s *taskProcessorSuite) TearDownTest() {
	s.controller.Finish()
	s.mockShard.StopForTest()
}

func (s *taskProcessorSuite) TestHandleSyncShardStatus_Stale() {
	now := timestamppb.New(time.Now().Add(-2 * dropSyncShardTaskTimeThreshold))
	err := s.replicationTaskProcessor.handleSyncShardStatus(&replicationspb.SyncShardStatus{
		StatusTime: now,
	})
	s.NoError(err)
}

func (s *taskProcessorSuite) TestHandleSyncShardStatus_Success() {
	now := timestamp.TimeNowPtrUtc()
	s.mockEngine.EXPECT().SyncShardStatus(gomock.Any(), &historyservice.SyncShardStatusRequest{
		SourceCluster: cluster.TestAlternativeClusterName,
		ShardId:       s.shardID,
		StatusTime:    now,
	}).Return(nil)

	err := s.replicationTaskProcessor.handleSyncShardStatus(&replicationspb.SyncShardStatus{
		StatusTime: now,
	})
	s.NoError(err)
}

func (s *taskProcessorSuite) TestHandleReplicationTask_SyncActivity() {
	namespaceID := uuid.NewString()
	workflowID := uuid.NewString()
	runID := uuid.NewString()
	now := time.Now()
	attempt := int32(2)
	task := &replicationspb.ReplicationTask{
		TaskType: enumsspb.REPLICATION_TASK_TYPE_SYNC_ACTIVITY_TASK,
		Attributes: &replicationspb.ReplicationTask_SyncActivityTaskAttributes{
			SyncActivityTaskAttributes: &replicationspb.SyncActivityTaskAttributes{
				NamespaceId: namespaceID,
				WorkflowId:  workflowID,
				RunId:       runID,
				Attempt:     attempt,
			},
		},
		VisibilityTime: timestamppb.New(now),
	}

	s.mockReplicationTaskExecutor.EXPECT().Execute(gomock.Any(), task, false).Return(nil)
	err := s.replicationTaskProcessor.handleReplicationTask(context.Background(), task)
	s.NoError(err)
}

func (s *taskProcessorSuite) TestHandleReplicationTask_History() {
	namespaceID := uuid.NewString()
	workflowID := uuid.NewString()
	runID := uuid.NewString()
	now := time.Now()
	events := []*historypb.HistoryEvent{{
		EventId: 1,
		Version: 1,
	}}
	versionHistory := []*historyspb.VersionHistoryItem{{
		EventId: 1,
		Version: 1,
	}}
	serializer := s.mockResource.GetPayloadSerializer()
	data, err := serializer.SerializeEvents(events)
	s.NoError(err)

	task := &replicationspb.ReplicationTask{
		TaskType: enumsspb.REPLICATION_TASK_TYPE_HISTORY_V2_TASK,
		Attributes: &replicationspb.ReplicationTask_HistoryTaskAttributes{
			HistoryTaskAttributes: &replicationspb.HistoryTaskAttributes{
				NamespaceId: namespaceID,
				WorkflowId:  workflowID,
				RunId:       runID,
				Events: &commonpb.DataBlob{
					EncodingType: enumspb.ENCODING_TYPE_PROTO3,
					Data:         data.Data,
				},
				VersionHistoryItems: versionHistory,
			},
		},
		VisibilityTime: timestamppb.New(now),
	}

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
		SourceClusterName: cluster.TestAlternativeClusterName,
		TaskInfo: &persistencespb.ReplicationTaskInfo{
			NamespaceId: namespaceID,
			WorkflowId:  workflowID,
			RunId:       runID,
			TaskType:    enumsspb.TASK_TYPE_REPLICATION_SYNC_ACTIVITY,
		},
	}
	persistedRequest := &persistence.PutReplicationTaskToDLQRequest{
		ShardID:           s.config.GetShardID(namespace.ID(namespaceID), workflowID),
		SourceClusterName: request.SourceClusterName,
		TaskInfo:          request.TaskInfo,
	}

	s.mockExecutionManager.EXPECT().PutReplicationTaskToDLQ(gomock.Any(), persistedRequest).Return(nil)
	err := s.replicationTaskProcessor.handleReplicationDLQTask(context.Background(), request)
	s.NoError(err)
}

func (s *taskProcessorSuite) TestHandleReplicationDLQTask_SyncWorkflowState() {
	namespaceID := uuid.NewString()
	workflowID := uuid.NewString()
	runID := uuid.NewString()

	request := &persistence.PutReplicationTaskToDLQRequest{
		SourceClusterName: cluster.TestAlternativeClusterName,
		TaskInfo: &persistencespb.ReplicationTaskInfo{
			NamespaceId: namespaceID,
			WorkflowId:  workflowID,
			RunId:       runID,
			TaskType:    enumsspb.TASK_TYPE_REPLICATION_SYNC_WORKFLOW_STATE,
			Version:     1,
		},
	}
	persistedRequest := &persistence.PutReplicationTaskToDLQRequest{
		ShardID:           s.config.GetShardID(namespace.ID(namespaceID), workflowID),
		SourceClusterName: request.SourceClusterName,
		TaskInfo:          request.TaskInfo,
	}

	s.mockExecutionManager.EXPECT().PutReplicationTaskToDLQ(gomock.Any(), persistedRequest).Return(nil)
	err := s.replicationTaskProcessor.handleReplicationDLQTask(context.Background(), request)
	s.NoError(err)
}

func (s *taskProcessorSuite) TestHandleReplicationDLQTask_History() {
	namespaceID := uuid.NewString()
	workflowID := uuid.NewString()
	runID := uuid.NewString()

	request := &persistence.PutReplicationTaskToDLQRequest{
		SourceClusterName: cluster.TestAlternativeClusterName,
		TaskInfo: &persistencespb.ReplicationTaskInfo{
			NamespaceId:  namespaceID,
			WorkflowId:   workflowID,
			RunId:        runID,
			TaskType:     enumsspb.TASK_TYPE_REPLICATION_HISTORY,
			FirstEventId: 1,
			NextEventId:  1,
			Version:      1,
		},
	}
	persistedRequest := &persistence.PutReplicationTaskToDLQRequest{
		ShardID:           s.config.GetShardID(namespace.ID(namespaceID), workflowID),
		SourceClusterName: request.SourceClusterName,
		TaskInfo:          request.TaskInfo,
	}

	s.mockExecutionManager.EXPECT().PutReplicationTaskToDLQ(gomock.Any(), persistedRequest).Return(nil)
	err := s.replicationTaskProcessor.handleReplicationDLQTask(context.Background(), request)
	s.NoError(err)
}

func (s *taskProcessorSuite) TestConvertTaskToDLQTask_SyncActivity() {
	namespaceID := uuid.NewString()
	workflowID := uuid.NewString()
	runID := uuid.NewString()
	task := &replicationspb.ReplicationTask{
		TaskType: enumsspb.REPLICATION_TASK_TYPE_SYNC_ACTIVITY_TASK,
		Attributes: &replicationspb.ReplicationTask_SyncActivityTaskAttributes{SyncActivityTaskAttributes: &replicationspb.SyncActivityTaskAttributes{
			NamespaceId: namespaceID,
			WorkflowId:  workflowID,
			RunId:       runID,
			Attempt:     1,
		}},
	}
	request := &persistence.PutReplicationTaskToDLQRequest{
		SourceClusterName: cluster.TestAlternativeClusterName,
		TaskInfo: &persistencespb.ReplicationTaskInfo{
			NamespaceId: namespaceID,
			WorkflowId:  workflowID,
			RunId:       runID,
			TaskType:    enumsspb.TASK_TYPE_REPLICATION_SYNC_ACTIVITY,
		},
	}

	dlqTask, err := s.replicationTaskProcessor.convertTaskToDLQTask(task)
	s.NoError(err)
	s.Equal(request, dlqTask)
}

func (s *taskProcessorSuite) TestConvertTaskToDLQTask_SyncWorkflowState() {
	namespaceID := uuid.NewString()
	workflowID := uuid.NewString()
	runID := uuid.NewString()
	task := &replicationspb.ReplicationTask{
		TaskType: enumsspb.REPLICATION_TASK_TYPE_SYNC_WORKFLOW_STATE_TASK,
		Attributes: &replicationspb.ReplicationTask_SyncWorkflowStateTaskAttributes{SyncWorkflowStateTaskAttributes: &replicationspb.SyncWorkflowStateTaskAttributes{
			WorkflowState: &persistencespb.WorkflowMutableState{
				ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
					NamespaceId: namespaceID,
					WorkflowId:  workflowID,
					VersionHistories: versionhistory.NewVersionHistories(
						versionhistory.NewVersionHistory(nil, []*historyspb.VersionHistoryItem{versionhistory.NewVersionHistoryItem(1, 1)}),
					),
				},
				ExecutionState: &persistencespb.WorkflowExecutionState{
					RunId: runID,
				},
			},
		}},
	}
	request := &persistence.PutReplicationTaskToDLQRequest{
		SourceClusterName: cluster.TestAlternativeClusterName,
		TaskInfo: &persistencespb.ReplicationTaskInfo{
			NamespaceId: namespaceID,
			WorkflowId:  workflowID,
			RunId:       runID,
			TaskType:    enumsspb.TASK_TYPE_REPLICATION_SYNC_WORKFLOW_STATE,
			Version:     1,
		},
	}

	dlqTask, err := s.replicationTaskProcessor.convertTaskToDLQTask(task)
	s.NoError(err)
	s.Equal(request, dlqTask)
}

func (s *taskProcessorSuite) TestConvertTaskToDLQTask_SyncHSM() {
	namespaceID := uuid.NewString()
	workflowID := uuid.NewString()
	runID := uuid.NewString()
	task := &replicationspb.ReplicationTask{
		SourceTaskId: rand.Int63(),
		TaskType:     enumsspb.REPLICATION_TASK_TYPE_SYNC_HSM_TASK,
		Attributes: &replicationspb.ReplicationTask_SyncHsmAttributes{SyncHsmAttributes: &replicationspb.SyncHSMAttributes{
			NamespaceId: namespaceID,
			WorkflowId:  workflowID,
			RunId:       runID,
			VersionHistory: &historyspb.VersionHistory{
				BranchToken: []byte("branchToken"),
				Items: []*historyspb.VersionHistoryItem{{
					EventId: 10,
					Version: 20,
				}},
			},
			StateMachineNode: &persistencespb.StateMachineNode{
				Data: []byte("stateMachineData"),
			},
		}},
		VisibilityTime: timestamppb.New(time.Now()),
	}
	request := &persistence.PutReplicationTaskToDLQRequest{
		SourceClusterName: cluster.TestAlternativeClusterName,
		TaskInfo: &persistencespb.ReplicationTaskInfo{
			NamespaceId:    namespaceID,
			WorkflowId:     workflowID,
			RunId:          runID,
			TaskId:         task.GetSourceTaskId(),
			TaskType:       enumsspb.TASK_TYPE_REPLICATION_SYNC_HSM,
			VisibilityTime: task.GetVisibilityTime(),
		},
	}

	dlqTask, err := s.replicationTaskProcessor.convertTaskToDLQTask(task)
	s.NoError(err)
	s.Equal(request, dlqTask)
}

func (s *taskProcessorSuite) TestConvertTaskToDLQTask_History() {
	namespaceID := uuid.NewString()
	workflowID := uuid.NewString()
	runID := uuid.NewString()
	events := []*historypb.HistoryEvent{{
		EventId: 1,
		Version: 1,
	}}
	versionHistory := []*historyspb.VersionHistoryItem{{
		EventId: 1,
		Version: 1,
	}}
	serializer := s.mockResource.GetPayloadSerializer()
	data, err := serializer.SerializeEvents(events)
	s.NoError(err)

	task := &replicationspb.ReplicationTask{
		TaskType: enumsspb.REPLICATION_TASK_TYPE_HISTORY_V2_TASK,
		Attributes: &replicationspb.ReplicationTask_HistoryTaskAttributes{
			HistoryTaskAttributes: &replicationspb.HistoryTaskAttributes{
				NamespaceId: namespaceID,
				WorkflowId:  workflowID,
				RunId:       runID,
				Events: &commonpb.DataBlob{
					EncodingType: enumspb.ENCODING_TYPE_PROTO3,
					Data:         data.Data,
				},
				VersionHistoryItems: versionHistory,
			},
		},
	}
	request := &persistence.PutReplicationTaskToDLQRequest{
		SourceClusterName: cluster.TestAlternativeClusterName,
		TaskInfo: &persistencespb.ReplicationTaskInfo{
			NamespaceId:  namespaceID,
			WorkflowId:   workflowID,
			RunId:        runID,
			TaskType:     enumsspb.TASK_TYPE_REPLICATION_HISTORY,
			FirstEventId: 1,
			NextEventId:  2,
			Version:      1,
		},
	}

	dlqTask, err := s.replicationTaskProcessor.convertTaskToDLQTask(task)
	s.NoError(err)
	s.Equal(request, dlqTask)
}

func (s *taskProcessorSuite) TestPaginationFn_Success_More() {
	namespaceID := uuid.NewString()
	workflowID := uuid.NewString()
	runID := uuid.NewString()
	events := []*historypb.HistoryEvent{{
		EventId: 1,
		Version: 1,
	}}
	versionHistory := []*historyspb.VersionHistoryItem{{
		EventId: 1,
		Version: 1,
	}}
	serializer := s.mockResource.GetPayloadSerializer()
	data, err := serializer.SerializeEvents(events)
	s.NoError(err)

	syncShardTask := &replicationspb.SyncShardStatus{
		StatusTime: timestamp.TimeNowPtrUtc(),
	}
	taskID := int64(123)
	lastRetrievedMessageID := 2 * taskID
	task := &replicationspb.ReplicationTask{
		SourceTaskId: taskID,
		TaskType:     enumsspb.REPLICATION_TASK_TYPE_HISTORY_V2_TASK,
		Attributes: &replicationspb.ReplicationTask_HistoryTaskAttributes{
			HistoryTaskAttributes: &replicationspb.HistoryTaskAttributes{
				NamespaceId: namespaceID,
				WorkflowId:  workflowID,
				RunId:       runID,
				Events: &commonpb.DataBlob{
					EncodingType: enumspb.ENCODING_TYPE_PROTO3,
					Data:         data.Data,
				},
				VersionHistoryItems: versionHistory,
			},
		},
	}

	maxRxProcessedTaskID := rand.Int63()
	maxRxReceivedTaskID := rand.Int63()
	rxTaskBackoff := time.Duration(rand.Int63())
	s.replicationTaskProcessor.maxRxProcessedTaskID = maxRxProcessedTaskID
	s.replicationTaskProcessor.maxRxReceivedTaskID = maxRxReceivedTaskID
	s.replicationTaskProcessor.rxTaskBackoff = rxTaskBackoff

	requestToken := &replicationspb.ReplicationToken{
		ShardId:                     s.shardID,
		LastProcessedMessageId:      maxRxProcessedTaskID,
		LastProcessedVisibilityTime: nil,
		LastRetrievedMessageId:      maxRxReceivedTaskID,
	}

	go func() {
		request := <-s.requestChan
		defer close(request.respChan)
		s.Equal(requestToken, request.token)
		request.respChan <- &replicationspb.ReplicationMessages{
			SyncShardStatus:        syncShardTask,
			ReplicationTasks:       []*replicationspb.ReplicationTask{task},
			LastRetrievedMessageId: lastRetrievedMessageID,
			HasMore:                true,
		}
	}()

	tasks, _, err := s.replicationTaskProcessor.paginationFn(nil)
	s.NoError(err)
	s.Len(tasks, 1)
	s.Equal(task, tasks[0].(*replicationspb.ReplicationTask))
	s.Equal(syncShardTask, <-s.replicationTaskProcessor.syncShardChan)
	s.Equal(lastRetrievedMessageID, s.replicationTaskProcessor.maxRxReceivedTaskID)
	s.Equal(time.Duration(0), s.replicationTaskProcessor.rxTaskBackoff)
}

func (s *taskProcessorSuite) TestPaginationFn_Success_NoMore() {
	namespaceID := uuid.NewString()
	workflowID := uuid.NewString()
	runID := uuid.NewString()
	events := []*historypb.HistoryEvent{{
		EventId: 1,
		Version: 1,
	}}
	versionHistory := []*historyspb.VersionHistoryItem{{
		EventId: 1,
		Version: 1,
	}}
	serializer := s.mockResource.GetPayloadSerializer()
	data, err := serializer.SerializeEvents(events)
	s.NoError(err)

	syncShardTask := &replicationspb.SyncShardStatus{
		StatusTime: timestamp.TimeNowPtrUtc(),
	}
	taskID := int64(123)
	lastRetrievedMessageID := 2 * taskID
	task := &replicationspb.ReplicationTask{
		SourceTaskId: taskID,
		TaskType:     enumsspb.REPLICATION_TASK_TYPE_HISTORY_V2_TASK,
		Attributes: &replicationspb.ReplicationTask_HistoryTaskAttributes{
			HistoryTaskAttributes: &replicationspb.HistoryTaskAttributes{
				NamespaceId: namespaceID,
				WorkflowId:  workflowID,
				RunId:       runID,
				Events: &commonpb.DataBlob{
					EncodingType: enumspb.ENCODING_TYPE_PROTO3,
					Data:         data.Data,
				},
				VersionHistoryItems: versionHistory,
			},
		},
	}

	maxRxProcessedTaskID := rand.Int63()
	maxRxReceivedTaskID := rand.Int63()
	rxTaskBackoff := time.Duration(rand.Int63())
	s.replicationTaskProcessor.maxRxProcessedTaskID = maxRxProcessedTaskID
	s.replicationTaskProcessor.maxRxReceivedTaskID = maxRxReceivedTaskID
	s.replicationTaskProcessor.rxTaskBackoff = rxTaskBackoff

	requestToken := &replicationspb.ReplicationToken{
		ShardId:                     s.shardID,
		LastProcessedMessageId:      maxRxProcessedTaskID,
		LastProcessedVisibilityTime: nil,
		LastRetrievedMessageId:      maxRxReceivedTaskID,
	}

	go func() {
		request := <-s.requestChan
		defer close(request.respChan)
		s.Equal(requestToken, request.token)
		request.respChan <- &replicationspb.ReplicationMessages{
			SyncShardStatus:        syncShardTask,
			ReplicationTasks:       []*replicationspb.ReplicationTask{task},
			LastRetrievedMessageId: lastRetrievedMessageID,
			HasMore:                false,
		}
	}()

	tasks, _, err := s.replicationTaskProcessor.paginationFn(nil)
	s.NoError(err)
	s.Len(tasks, 1)
	s.Equal(task, tasks[0].(*replicationspb.ReplicationTask))
	s.Equal(syncShardTask, <-s.replicationTaskProcessor.syncShardChan)
	s.Equal(lastRetrievedMessageID, s.replicationTaskProcessor.maxRxReceivedTaskID)
	s.NotEqual(time.Duration(0), s.replicationTaskProcessor.rxTaskBackoff)
}

func (s *taskProcessorSuite) TestStartStopIsStopped() {
	s.False(s.replicationTaskProcessor.isStopped())

	// Stop before Start should be a no-op (status still Initialized).
	s.replicationTaskProcessor.Stop()
	s.False(s.replicationTaskProcessor.isStopped())

	s.replicationTaskProcessor.Start()
	// Second Start is a no-op.
	s.replicationTaskProcessor.Start()
	s.False(s.replicationTaskProcessor.isStopped())

	s.replicationTaskProcessor.Stop()
	s.True(s.replicationTaskProcessor.isStopped())

	// Second Stop is a no-op and must not panic (channel already closed).
	s.replicationTaskProcessor.Stop()
	s.True(s.replicationTaskProcessor.isStopped())
}

func (s *taskProcessorSuite) TestIsRetryableError_Stopped() {
	taskProcSetStopped(s.replicationTaskProcessor)
	s.False(s.replicationTaskProcessor.isRetryableError(serviceerror.NewUnavailable("retryable")))
}

func (s *taskProcessorSuite) TestIsRetryableError_InvalidArgument() {
	s.False(s.replicationTaskProcessor.isRetryableError(serviceerror.NewInvalidArgument("bad")))
}

func (s *taskProcessorSuite) TestIsRetryableError_Default() {
	s.True(s.replicationTaskProcessor.isRetryableError(serviceerror.NewUnavailable("retryable")))
	s.True(s.replicationTaskProcessor.isRetryableError(errors.New("some error")))
}

func (s *taskProcessorSuite) TestGetOperationTagValue() {
	cases := map[enumsspb.ReplicationTaskType]string{
		enumsspb.REPLICATION_TASK_TYPE_SYNC_SHARD_STATUS_TASK:   metrics.SyncShardTaskScope,
		enumsspb.REPLICATION_TASK_TYPE_SYNC_ACTIVITY_TASK:       metrics.SyncActivityTaskScope,
		enumsspb.REPLICATION_TASK_TYPE_HISTORY_METADATA_TASK:    metrics.HistoryMetadataReplicationTaskScope,
		enumsspb.REPLICATION_TASK_TYPE_HISTORY_V2_TASK:          metrics.HistoryReplicationTaskScope,
		enumsspb.REPLICATION_TASK_TYPE_SYNC_WORKFLOW_STATE_TASK: metrics.SyncWorkflowStateTaskScope,
		enumsspb.REPLICATION_TASK_TYPE_SYNC_HSM_TASK:            metrics.ReplicatorScope,
	}
	for taskType, expected := range cases {
		s.Equal(expected, s.replicationTaskProcessor.getOperationTagValue(&replicationspb.ReplicationTask{
			TaskType: taskType,
		}))
	}
}

func (s *taskProcessorSuite) TestEmitTaskMetrics_AllBranches() {
	// Exercise each branch of the error type switch. NoopMetricsHandler makes
	// these effectively assertions that no panic occurs and all branches run.
	errs := []error{
		nil,
		context.DeadlineExceeded,
		context.Canceled,
		serviceerrors.NewShardOwnershipLost("", ""),
		serviceerror.NewInvalidArgument("bad"),
		serviceerror.NewNamespaceNotActive("ns", "cur", "active"),
		serviceerror.NewWorkflowExecutionAlreadyStarted("started", "", ""),
		serviceerror.NewNotFound("not found"),
		serviceerror.NewNamespaceNotFound("ns"),
		&serviceerror.ResourceExhausted{},
		serviceerrors.NewRetryReplication("retry", "ns", "wf", "run", 1, 2, 3, 4),
		serviceerror.NewUnavailable("default"),
	}
	for _, err := range errs {
		s.replicationTaskProcessor.emitTaskMetrics("operation", err)
	}
}

func (s *taskProcessorSuite) TestHandleSyncShardStatus_Nil() {
	err := s.replicationTaskProcessor.handleSyncShardStatus(nil)
	s.NoError(err)
}

func (s *taskProcessorSuite) TestConvertTaskToDLQTask_UnknownType() {
	task := &replicationspb.ReplicationTask{
		TaskType: enumsspb.REPLICATION_TASK_TYPE_HISTORY_METADATA_TASK,
	}
	dlqTask, err := s.replicationTaskProcessor.convertTaskToDLQTask(task)
	s.Error(err)
	s.Nil(dlqTask)
}

func (s *taskProcessorSuite) TestConvertTaskToDLQTask_History_EmptyEvents() {
	serializer := s.mockResource.GetPayloadSerializer()
	data, err := serializer.SerializeEvents(nil)
	s.NoError(err)
	task := &replicationspb.ReplicationTask{
		TaskType: enumsspb.REPLICATION_TASK_TYPE_HISTORY_V2_TASK,
		Attributes: &replicationspb.ReplicationTask_HistoryTaskAttributes{
			HistoryTaskAttributes: &replicationspb.HistoryTaskAttributes{
				NamespaceId: uuid.NewString(),
				WorkflowId:  uuid.NewString(),
				RunId:       uuid.NewString(),
				Events: &commonpb.DataBlob{
					EncodingType: enumspb.ENCODING_TYPE_PROTO3,
					Data:         data.Data,
				},
			},
		},
	}
	dlqTask, err := s.replicationTaskProcessor.convertTaskToDLQTask(task)
	s.ErrorIs(err, ErrCorruptedHistoryEventBatch)
	s.Nil(dlqTask)
}

func (s *taskProcessorSuite) TestConvertTaskToDLQTask_History_DeserializeError() {
	task := &replicationspb.ReplicationTask{
		TaskType: enumsspb.REPLICATION_TASK_TYPE_HISTORY_V2_TASK,
		Attributes: &replicationspb.ReplicationTask_HistoryTaskAttributes{
			HistoryTaskAttributes: &replicationspb.HistoryTaskAttributes{
				NamespaceId: uuid.NewString(),
				WorkflowId:  uuid.NewString(),
				RunId:       uuid.NewString(),
				Events: &commonpb.DataBlob{
					EncodingType: enumspb.ENCODING_TYPE_PROTO3,
					Data:         []byte("not-a-valid-proto"),
				},
			},
		},
	}
	dlqTask, err := s.replicationTaskProcessor.convertTaskToDLQTask(task)
	s.Error(err)
	s.Nil(dlqTask)
}

func (s *taskProcessorSuite) TestConvertTaskToDLQTask_SyncWorkflowState_NoVersionHistory() {
	task := &replicationspb.ReplicationTask{
		TaskType: enumsspb.REPLICATION_TASK_TYPE_SYNC_WORKFLOW_STATE_TASK,
		Attributes: &replicationspb.ReplicationTask_SyncWorkflowStateTaskAttributes{
			SyncWorkflowStateTaskAttributes: &replicationspb.SyncWorkflowStateTaskAttributes{
				WorkflowState: &persistencespb.WorkflowMutableState{
					ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
						NamespaceId:      uuid.NewString(),
						WorkflowId:       uuid.NewString(),
						VersionHistories: versionhistory.NewVersionHistories(versionhistory.NewVersionHistory(nil, nil)),
					},
					ExecutionState: &persistencespb.WorkflowExecutionState{
						RunId: uuid.NewString(),
					},
				},
			},
		},
	}
	dlqTask, err := s.replicationTaskProcessor.convertTaskToDLQTask(task)
	s.Error(err)
	s.Nil(dlqTask)
}

func (s *taskProcessorSuite) TestApplyReplicationTask_Success() {
	task := &replicationspb.ReplicationTask{
		TaskType: enumsspb.REPLICATION_TASK_TYPE_SYNC_ACTIVITY_TASK,
		Attributes: &replicationspb.ReplicationTask_SyncActivityTaskAttributes{
			SyncActivityTaskAttributes: &replicationspb.SyncActivityTaskAttributes{},
		},
	}
	s.mockReplicationTaskExecutor.EXPECT().Execute(gomock.Any(), task, false).Return(nil)
	s.NoError(s.replicationTaskProcessor.applyReplicationTask(task))
}

func (s *taskProcessorSuite) TestApplyReplicationTask_Stopped() {
	task := &replicationspb.ReplicationTask{
		TaskType: enumsspb.REPLICATION_TASK_TYPE_SYNC_ACTIVITY_TASK,
		Attributes: &replicationspb.ReplicationTask_SyncActivityTaskAttributes{
			SyncActivityTaskAttributes: &replicationspb.SyncActivityTaskAttributes{},
		},
	}
	execErr := serviceerror.NewUnavailable("unavailable")
	// Once executor fails, isStopped() short-circuits the retry loop and the
	// error is returned without going to DLQ.
	s.mockReplicationTaskExecutor.EXPECT().Execute(gomock.Any(), task, false).DoAndReturn(
		func(_ context.Context, _ *replicationspb.ReplicationTask, _ bool) error {
			taskProcSetStopped(s.replicationTaskProcessor)
			return execErr
		},
	)
	err := s.replicationTaskProcessor.applyReplicationTask(task)
	s.Equal(execErr, err)
}

func (s *taskProcessorSuite) TestApplyReplicationTask_ShardOwnershipLost() {
	task := &replicationspb.ReplicationTask{
		TaskType: enumsspb.REPLICATION_TASK_TYPE_SYNC_ACTIVITY_TASK,
		Attributes: &replicationspb.ReplicationTask_SyncActivityTaskAttributes{
			SyncActivityTaskAttributes: &replicationspb.SyncActivityTaskAttributes{},
		},
	}
	execErr := serviceerrors.NewShardOwnershipLost("owner", "current")
	// ShardOwnershipLost is retryable per isRetryableError, so shrink the retry
	// policy; after retries exhaust, applyReplicationTask short-circuits via
	// shard.IsShardOwnershipLostError and returns the error without DLQ routing.
	s.replicationTaskProcessor.taskRetryPolicy = taskProcOneShotRetryPolicy()
	s.mockReplicationTaskExecutor.EXPECT().Execute(gomock.Any(), task, false).Return(execErr).MinTimes(1)
	err := s.replicationTaskProcessor.applyReplicationTask(task)
	s.Equal(execErr, err)
}

func (s *taskProcessorSuite) TestApplyReplicationTask_ConvertError_ReturnsNil() {
	// Use a task type that cannot be converted to a DLQ task (history metadata).
	task := &replicationspb.ReplicationTask{
		TaskType: enumsspb.REPLICATION_TASK_TYPE_HISTORY_METADATA_TASK,
	}
	// taskRetryPolicy default has 80 attempts; shrink it so the failing
	// executor exhausts retries quickly.
	s.replicationTaskProcessor.taskRetryPolicy = taskProcOneShotRetryPolicy()
	s.mockReplicationTaskExecutor.EXPECT().Execute(gomock.Any(), task, false).Return(
		serviceerror.NewInvalidArgument("bad")).MinTimes(1)
	// convertTaskToDLQTask fails (unknown type) so applyReplicationTask returns nil.
	err := s.replicationTaskProcessor.applyReplicationTask(task)
	s.NoError(err)
}

func (s *taskProcessorSuite) TestApplyReplicationTask_RoutesToDLQ() {
	namespaceID := uuid.NewString()
	workflowID := uuid.NewString()
	runID := uuid.NewString()
	task := &replicationspb.ReplicationTask{
		TaskType: enumsspb.REPLICATION_TASK_TYPE_SYNC_ACTIVITY_TASK,
		Attributes: &replicationspb.ReplicationTask_SyncActivityTaskAttributes{
			SyncActivityTaskAttributes: &replicationspb.SyncActivityTaskAttributes{
				NamespaceId: namespaceID,
				WorkflowId:  workflowID,
				RunId:       runID,
			},
		},
	}
	s.replicationTaskProcessor.taskRetryPolicy = taskProcOneShotRetryPolicy()
	s.mockReplicationTaskExecutor.EXPECT().Execute(gomock.Any(), task, false).Return(
		serviceerror.NewInvalidArgument("bad")).MinTimes(1)

	expectedDLQ := &persistence.PutReplicationTaskToDLQRequest{
		ShardID:           s.config.GetShardID(namespace.ID(namespaceID), workflowID),
		SourceClusterName: cluster.TestAlternativeClusterName,
		TaskInfo: &persistencespb.ReplicationTaskInfo{
			NamespaceId: namespaceID,
			WorkflowId:  workflowID,
			RunId:       runID,
			TaskType:    enumsspb.TASK_TYPE_REPLICATION_SYNC_ACTIVITY,
		},
	}
	s.mockExecutionManager.EXPECT().PutReplicationTaskToDLQ(gomock.Any(), expectedDLQ).Return(nil)
	s.NoError(s.replicationTaskProcessor.applyReplicationTask(task))
}

func (s *taskProcessorSuite) TestHandleReplicationDLQTask_RetryThenSuccess() {
	namespaceID := uuid.NewString()
	workflowID := uuid.NewString()
	request := &persistence.PutReplicationTaskToDLQRequest{
		ShardID:           s.shardID,
		SourceClusterName: cluster.TestAlternativeClusterName,
		TaskInfo: &persistencespb.ReplicationTaskInfo{
			NamespaceId: namespaceID,
			WorkflowId:  workflowID,
			RunId:       uuid.NewString(),
			TaskType:    enumsspb.TASK_TYPE_REPLICATION_SYNC_ACTIVITY,
		},
	}
	gomock.InOrder(
		s.mockExecutionManager.EXPECT().PutReplicationTaskToDLQ(gomock.Any(), gomock.Any()).Return(
			serviceerror.NewUnavailable("transient")),
		s.mockExecutionManager.EXPECT().PutReplicationTaskToDLQ(gomock.Any(), gomock.Any()).Return(nil),
	)
	s.NoError(s.replicationTaskProcessor.handleReplicationDLQTask(context.Background(), request))
}

func (s *taskProcessorSuite) TestPollProcessReplicationTasks_Success() {
	taskID := int64(123)
	lastRetrievedMessageID := int64(456)
	task := &replicationspb.ReplicationTask{
		SourceTaskId: taskID,
		TaskType:     enumsspb.REPLICATION_TASK_TYPE_SYNC_ACTIVITY_TASK,
		Attributes: &replicationspb.ReplicationTask_SyncActivityTaskAttributes{
			SyncActivityTaskAttributes: &replicationspb.SyncActivityTaskAttributes{
				NamespaceId: uuid.NewString(),
				WorkflowId:  uuid.NewString(),
				RunId:       uuid.NewString(),
			},
		},
		VisibilityTime: timestamp.TimeNowPtrUtc(),
	}
	// Respond to however many pagination requests the iterator makes (each
	// reply has HasMore:false). A done signal lets the responder exit cleanly
	// once polling is finished, so no goroutine leaks past the test.
	reqChan := s.requestChan
	responderDone := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() {
		for {
			select {
			case request := <-reqChan:
				request.respChan <- &replicationspb.ReplicationMessages{
					ReplicationTasks:       []*replicationspb.ReplicationTask{task},
					LastRetrievedMessageId: lastRetrievedMessageID,
					HasMore:                false,
				}
				close(request.respChan)
			case <-responderDone:
				return
			}
		}
	})
	s.mockReplicationTaskExecutor.EXPECT().Execute(gomock.Any(), task, false).Return(nil).AnyTimes()
	err := s.replicationTaskProcessor.pollProcessReplicationTasks()
	s.NoError(err)
	s.Equal(lastRetrievedMessageID, s.replicationTaskProcessor.maxRxProcessedTaskID)
	close(responderDone)
	wg.Wait()
}

func (s *taskProcessorSuite) TestPollProcessReplicationTasks_ApplyError() {
	maxRxProcessedTaskID := s.replicationTaskProcessor.maxRxProcessedTaskID
	task := &replicationspb.ReplicationTask{
		SourceTaskId: 123,
		TaskType:     enumsspb.REPLICATION_TASK_TYPE_SYNC_ACTIVITY_TASK,
		Attributes: &replicationspb.ReplicationTask_SyncActivityTaskAttributes{
			SyncActivityTaskAttributes: &replicationspb.SyncActivityTaskAttributes{},
		},
	}
	// applyReplicationTask -> handleReplicationTask retries; mark stopped so it
	// returns quickly with the error and pollProcessReplicationTasks propagates it.
	s.mockReplicationTaskExecutor.EXPECT().Execute(gomock.Any(), task, false).DoAndReturn(
		func(_ context.Context, _ *replicationspb.ReplicationTask, _ bool) error {
			taskProcSetStopped(s.replicationTaskProcessor)
			return serviceerror.NewUnavailable("boom")
		},
	)
	reqChan := s.requestChan
	var wg sync.WaitGroup
	wg.Go(func() {
		request := <-reqChan
		defer close(request.respChan)
		request.respChan <- &replicationspb.ReplicationMessages{
			ReplicationTasks:       []*replicationspb.ReplicationTask{task},
			LastRetrievedMessageId: 456,
			HasMore:                false,
		}
	})
	err := s.replicationTaskProcessor.pollProcessReplicationTasks()
	s.Error(err)
	wg.Wait()
	// On error, maxRxReceivedTaskID is reset to maxRxProcessedTaskID and backoff set.
	s.Equal(maxRxProcessedTaskID, s.replicationTaskProcessor.maxRxReceivedTaskID)
	s.NotEqual(time.Duration(0), s.replicationTaskProcessor.rxTaskBackoff)
}

func (s *taskProcessorSuite) TestEventLoop_ShutdownAndSyncShard() {
	// Drive a sync shard status through the channel, then start/stop the loop.
	now := timestamp.TimeNowPtrUtc()
	s.mockEngine.EXPECT().SyncShardStatus(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	// paginationFn will request tasks; respond with empty and close so the loop
	// keeps running without blocking, until shutdown.
	reqChan := s.requestChan
	responderDone := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() {
		for {
			select {
			case request := <-reqChan:
				request.respChan <- &replicationspb.ReplicationMessages{HasMore: false}
				close(request.respChan)
			case <-responderDone:
				return
			}
		}
	})
	s.replicationTaskProcessor.Start()
	// syncShardChan is buffered (cap 1). The first send is buffered; the second
	// send blocks until the event loop receives the first, which deterministically
	// proves the sync-shard-status receive branch executed (no sleep needed).
	s.replicationTaskProcessor.syncShardChan <- &replicationspb.SyncShardStatus{StatusTime: now}
	s.replicationTaskProcessor.syncShardChan <- &replicationspb.SyncShardStatus{StatusTime: now}
	s.replicationTaskProcessor.Stop()
	close(responderDone)
	wg.Wait()
}

func (s *taskProcessorSuite) TestPaginationFn_Error() {
	maxRxProcessedTaskID := rand.Int63()
	maxRxReceivedTaskID := rand.Int63()
	rxTaskBackoff := time.Duration(rand.Int63())
	s.replicationTaskProcessor.maxRxProcessedTaskID = maxRxProcessedTaskID
	s.replicationTaskProcessor.maxRxReceivedTaskID = maxRxReceivedTaskID
	s.replicationTaskProcessor.rxTaskBackoff = rxTaskBackoff

	requestToken := &replicationspb.ReplicationToken{
		ShardId:                     s.shardID,
		LastProcessedMessageId:      maxRxProcessedTaskID,
		LastProcessedVisibilityTime: nil,
		LastRetrievedMessageId:      maxRxReceivedTaskID,
	}

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

// taskProcSetStopped flips the processor status to stopped without closing the
// shutdown channel, which is what isStopped() observes.
func taskProcSetStopped(p *taskProcessorImpl) {
	atomic.StoreInt32(&p.status, common.DaemonStatusStopped)
}

// taskProcOneShotRetryPolicy builds a retry policy that makes a single attempt
// so that error-path tests do not block on the default 80-attempt policy.
func taskProcOneShotRetryPolicy() backoff.RetryPolicy {
	return backoff.NewExponentialRetryPolicy(time.Millisecond).
		WithMaximumAttempts(1)
}
