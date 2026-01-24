package replication

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/resourcetest"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/service/history/configs"
	deletemanager "go.temporal.io/server/service/history/deletemanager"
	"go.temporal.io/server/service/history/replication/eventhandler"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	wcache "go.temporal.io/server/service/history/workflow/cache"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type (
	taskExecutorSuite struct {
		suite.Suite
		*require.Assertions
		controller *gomock.Controller

		remoteCluster           string
		mockResource            *resourcetest.Test
		mockShard               *shard.ContextTest
		config                  *configs.Config
		historyClient           *historyservicemock.MockHistoryServiceClient
		mockNamespaceCache      *namespace.MockRegistry
		clusterMetadata         *cluster.MockMetadata
		workflowCache           *wcache.MockCache
		remoteHistoryFetcher    *eventhandler.MockHistoryPaginatedFetcher
		replicationTaskExecutor *taskExecutorImpl
	}
)

func TestTaskExecutorSuite(t *testing.T) {
	s := new(taskExecutorSuite)
	suite.Run(t, s)
}

func (s *taskExecutorSuite) SetupSuite() {

}

func (s *taskExecutorSuite) TearDownSuite() {

}

func (s *taskExecutorSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.controller = gomock.NewController(s.T())
	s.remoteCluster = cluster.TestAlternativeClusterName

	s.config = tests.NewDynamicConfig()
	s.mockShard = shard.NewTestContext(
		s.controller,
		persistencespb.ShardInfo_builder{
			ShardId: 0,
			RangeId: 1,
			ReplicationDlqAckLevel: map[string]int64{
				cluster.TestAlternativeClusterName: persistence.EmptyQueueMessageID,
			},
		}.Build(),
		s.config,
	)
	s.mockResource = s.mockShard.Resource
	s.mockNamespaceCache = s.mockResource.NamespaceCache
	s.clusterMetadata = s.mockResource.ClusterMetadata

	s.historyClient = historyservicemock.NewMockHistoryServiceClient(s.controller)
	s.workflowCache = wcache.NewMockCache(s.controller)
	s.remoteHistoryFetcher = eventhandler.NewMockHistoryPaginatedFetcher(s.controller)

	s.clusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespaceName(gomock.Any()).Return(tests.Namespace, nil).AnyTimes()
	s.mockShard.SetHistoryClientForTesting(s.historyClient)
	s.replicationTaskExecutor = NewTaskExecutor(
		s.remoteCluster,
		s.mockShard,
		s.remoteHistoryFetcher,
		deletemanager.NewMockDeleteManager(s.controller),
		s.workflowCache,
	).(*taskExecutorImpl)
}

func (s *taskExecutorSuite) TearDownTest() {
	s.controller.Finish()
	s.mockShard.StopForTest()
}

func (s *taskExecutorSuite) TestFilterTask_Apply() {
	namespaceID := namespace.ID(uuid.NewString())
	s.mockNamespaceCache.EXPECT().
		GetNamespaceByID(namespaceID).
		Return(namespace.NewGlobalNamespaceForTest(
			nil,
			nil,
			persistencespb.NamespaceReplicationConfig_builder{Clusters: []string{
				cluster.TestCurrentClusterName,
				cluster.TestAlternativeClusterName,
			}}.Build(),
			0,
		), nil)
	ok, err := s.replicationTaskExecutor.filterTask(namespaceID, "test-workflow-id", false)
	s.NoError(err)
	s.True(ok)
}

func (s *taskExecutorSuite) TestFilterTask_NotApply() {
	namespaceID := namespace.ID(uuid.NewString())
	s.mockNamespaceCache.EXPECT().
		GetNamespaceByID(namespaceID).
		Return(namespace.NewGlobalNamespaceForTest(
			nil,
			nil,
			persistencespb.NamespaceReplicationConfig_builder{Clusters: []string{cluster.TestAlternativeClusterName}}.Build(),
			0,
		), nil)
	ok, err := s.replicationTaskExecutor.filterTask(namespaceID, "test-workflow-id", false)
	s.NoError(err)
	s.False(ok)
}

func (s *taskExecutorSuite) TestFilterTask_Error() {
	namespaceID := namespace.ID(uuid.NewString())
	s.mockNamespaceCache.EXPECT().
		GetNamespaceByID(namespaceID).
		Return(nil, fmt.Errorf("random error"))
	ok, err := s.replicationTaskExecutor.filterTask(namespaceID, "test-workflow-id", false)
	s.Error(err)
	s.False(ok)
}

func (s *taskExecutorSuite) TestFilterTask_EnforceApply() {
	namespaceID := namespace.ID(uuid.NewString())
	ok, err := s.replicationTaskExecutor.filterTask(namespaceID, "test-workflow-id", true)
	s.NoError(err)
	s.True(ok)
}

func (s *taskExecutorSuite) TestFilterTask_NamespaceNotFound() {
	namespaceID := namespace.ID(uuid.NewString())
	s.mockNamespaceCache.EXPECT().
		GetNamespaceByID(namespaceID).
		Return(nil, &serviceerror.NamespaceNotFound{})
	ok, err := s.replicationTaskExecutor.filterTask(namespaceID, "test-workflow-id", false)
	s.NoError(err)
	s.False(ok)
}

func (s *taskExecutorSuite) TestProcessTaskOnce_SyncActivityReplicationTask() {
	namespaceID := namespace.ID(uuid.NewString())
	workflowID := uuid.NewString()
	runID := uuid.NewString()
	task := replicationspb.ReplicationTask_builder{
		TaskType: enumsspb.REPLICATION_TASK_TYPE_SYNC_ACTIVITY_TASK,
		SyncActivityTaskAttributes: replicationspb.SyncActivityTaskAttributes_builder{
			NamespaceId:                namespaceID.String(),
			WorkflowId:                 workflowID,
			RunId:                      runID,
			Version:                    1234,
			ScheduledEventId:           2345,
			ScheduledTime:              nil,
			StartedEventId:             2346,
			StartedTime:                nil,
			LastHeartbeatTime:          nil,
			Attempt:                    10,
			LastFailure:                nil,
			LastWorkerIdentity:         "",
			LastStartedBuildId:         "ABC",
			LastStartedRedirectCounter: 8,
		}.Build(),
	}.Build()
	request := historyservice.SyncActivityRequest_builder{
		NamespaceId:                namespaceID.String(),
		WorkflowId:                 workflowID,
		RunId:                      runID,
		Version:                    1234,
		ScheduledEventId:           2345,
		ScheduledTime:              nil,
		StartedEventId:             2346,
		StartedTime:                nil,
		LastHeartbeatTime:          nil,
		Attempt:                    10,
		LastFailure:                nil,
		LastWorkerIdentity:         "",
		LastStartedBuildId:         "ABC",
		LastStartedRedirectCounter: 8,
	}.Build()

	s.historyClient.EXPECT().SyncActivity(gomock.Any(), request).Return(&historyservice.SyncActivityResponse{}, nil)
	err := s.replicationTaskExecutor.Execute(context.Background(), task, true)
	s.NoError(err)
}

func (s *taskExecutorSuite) TestProcessTaskOnce_SyncActivityReplicationTask_Resend() {
	namespaceID := namespace.ID(uuid.NewString())
	workflowID := uuid.NewString()
	runID := uuid.NewString()
	task := replicationspb.ReplicationTask_builder{
		TaskType: enumsspb.REPLICATION_TASK_TYPE_SYNC_ACTIVITY_TASK,
		SyncActivityTaskAttributes: replicationspb.SyncActivityTaskAttributes_builder{
			NamespaceId:        namespaceID.String(),
			WorkflowId:         workflowID,
			RunId:              runID,
			Version:            1234,
			ScheduledEventId:   2345,
			ScheduledTime:      nil,
			StartedEventId:     2346,
			StartedTime:        nil,
			LastHeartbeatTime:  nil,
			Attempt:            10,
			LastFailure:        nil,
			LastWorkerIdentity: "",
		}.Build(),
	}.Build()
	request := historyservice.SyncActivityRequest_builder{
		NamespaceId:        namespaceID.String(),
		WorkflowId:         workflowID,
		RunId:              runID,
		Version:            1234,
		ScheduledEventId:   2345,
		ScheduledTime:      nil,
		StartedEventId:     2346,
		StartedTime:        nil,
		LastHeartbeatTime:  nil,
		Attempt:            10,
		LastFailure:        nil,
		LastWorkerIdentity: "",
	}.Build()

	resendErr := serviceerrors.NewRetryReplication(
		"some random error message",
		namespaceID.String(),
		workflowID,
		runID,
		123,
		234,
		345,
		456,
	)
	s.historyClient.EXPECT().SyncActivity(gomock.Any(), request).Return(nil, resendErr)
	emptyIterator := collection.NewPagingIterator(func(paginationToken []byte) ([]*eventhandler.HistoryBatch, []byte, error) {
		return nil, nil, nil
	})
	s.remoteHistoryFetcher.EXPECT().GetSingleWorkflowHistoryPaginatedIteratorExclusive(
		gomock.Any(),
		s.remoteCluster,
		namespaceID,
		workflowID,
		runID,
		int64(123),
		int64(234),
		int64(345),
		int64(456),
	).Return(emptyIterator)

	s.historyClient.EXPECT().SyncActivity(gomock.Any(), request).Return(&historyservice.SyncActivityResponse{}, nil)
	err := s.replicationTaskExecutor.Execute(context.Background(), task, true)
	s.NoError(err)
}

func (s *taskExecutorSuite) TestProcess_HistoryReplicationTask() {
	namespaceID := namespace.ID(uuid.NewString())
	workflowID := uuid.NewString()
	runID := uuid.NewString()
	task := replicationspb.ReplicationTask_builder{
		TaskType: enumsspb.REPLICATION_TASK_TYPE_HISTORY_V2_TASK,
		HistoryTaskAttributes: replicationspb.HistoryTaskAttributes_builder{
			NamespaceId:         namespaceID.String(),
			WorkflowId:          workflowID,
			RunId:               runID,
			VersionHistoryItems: []*historyspb.VersionHistoryItem{historyspb.VersionHistoryItem_builder{EventId: 233, Version: 2333}.Build()},
			Events:              nil,
			NewRunEvents:        nil,
		}.Build(),
	}.Build()
	request := historyservice.ReplicateEventsV2Request_builder{
		NamespaceId: namespaceID.String(),
		WorkflowExecution: commonpb.WorkflowExecution_builder{
			WorkflowId: workflowID,
			RunId:      runID,
		}.Build(),
		VersionHistoryItems: []*historyspb.VersionHistoryItem{historyspb.VersionHistoryItem_builder{EventId: 233, Version: 2333}.Build()},
		Events:              nil,
		NewRunEvents:        nil,
	}.Build()
	s.historyClient.EXPECT().ReplicateEventsV2(gomock.Any(), request).Return(&historyservice.ReplicateEventsV2Response{}, nil)
	err := s.replicationTaskExecutor.Execute(context.Background(), task, true)
	s.NoError(err)
}

func (s *taskExecutorSuite) TestProcess_HistoryReplicationTask_Resend() {
	namespaceID := namespace.ID(uuid.NewString())
	workflowID := uuid.NewString()
	runID := uuid.NewString()
	task := replicationspb.ReplicationTask_builder{
		TaskType: enumsspb.REPLICATION_TASK_TYPE_HISTORY_V2_TASK,
		HistoryTaskAttributes: replicationspb.HistoryTaskAttributes_builder{
			NamespaceId:         namespaceID.String(),
			WorkflowId:          workflowID,
			RunId:               runID,
			VersionHistoryItems: []*historyspb.VersionHistoryItem{historyspb.VersionHistoryItem_builder{EventId: 233, Version: 2333}.Build()},
			Events:              nil,
			NewRunEvents:        nil,
		}.Build(),
	}.Build()
	request := historyservice.ReplicateEventsV2Request_builder{
		NamespaceId: namespaceID.String(),
		WorkflowExecution: commonpb.WorkflowExecution_builder{
			WorkflowId: workflowID,
			RunId:      runID,
		}.Build(),
		VersionHistoryItems: []*historyspb.VersionHistoryItem{historyspb.VersionHistoryItem_builder{EventId: 233, Version: 2333}.Build()},
		Events:              nil,
		NewRunEvents:        nil,
	}.Build()

	resendErr := serviceerrors.NewRetryReplication(
		"some random error message",
		namespaceID.String(),
		workflowID,
		runID,
		123,
		234,
		345,
		456,
	)
	s.historyClient.EXPECT().ReplicateEventsV2(gomock.Any(), request).Return(nil, resendErr)
	emptyIterator := collection.NewPagingIterator(func(paginationToken []byte) ([]*eventhandler.HistoryBatch, []byte, error) {
		return nil, nil, nil
	})
	s.remoteHistoryFetcher.EXPECT().GetSingleWorkflowHistoryPaginatedIteratorExclusive(
		gomock.Any(),
		s.remoteCluster,
		namespaceID,
		workflowID,
		runID,
		int64(123),
		int64(234),
		int64(345),
		int64(456),
	).Return(emptyIterator)

	s.historyClient.EXPECT().ReplicateEventsV2(gomock.Any(), request).Return(&historyservice.ReplicateEventsV2Response{}, nil)
	err := s.replicationTaskExecutor.Execute(context.Background(), task, true)
	s.NoError(err)
}

func (s *taskExecutorSuite) TestProcessTaskOnce_SyncWorkflowStateTask() {
	namespaceID := namespace.ID(uuid.NewString())
	task := replicationspb.ReplicationTask_builder{
		TaskType: enumsspb.REPLICATION_TASK_TYPE_SYNC_WORKFLOW_STATE_TASK,
		SyncWorkflowStateTaskAttributes: replicationspb.SyncWorkflowStateTaskAttributes_builder{
			WorkflowState: persistencespb.WorkflowMutableState_builder{
				ExecutionInfo: persistencespb.WorkflowExecutionInfo_builder{
					NamespaceId: namespaceID.String(),
				}.Build(),
			}.Build(),
		}.Build(),
	}.Build()
	s.historyClient.EXPECT().ReplicateWorkflowState(gomock.Any(), gomock.Any()).Return(&historyservice.ReplicateWorkflowStateResponse{}, nil)

	err := s.replicationTaskExecutor.Execute(context.Background(), task, true)
	s.NoError(err)
}

func (s *taskExecutorSuite) TestProcessTaskOnce_SyncHSMTask() {
	namespaceID := namespace.ID(uuid.NewString())
	workflowID := uuid.NewString()
	runID := uuid.NewString()
	task := replicationspb.ReplicationTask_builder{
		SourceTaskId: rand.Int63(),
		TaskType:     enumsspb.REPLICATION_TASK_TYPE_SYNC_HSM_TASK,
		SyncHsmAttributes: replicationspb.SyncHSMAttributes_builder{
			NamespaceId:      namespaceID.String(),
			WorkflowId:       workflowID,
			RunId:            runID,
			VersionHistory:   &historyspb.VersionHistory{},
			StateMachineNode: &persistencespb.StateMachineNode{},
		}.Build(),
		VisibilityTime: timestamppb.New(time.Now()),
	}.Build()

	// not handling SyncHSMTask in deprecated replication task processing code path
	err := s.replicationTaskExecutor.Execute(context.Background(), task, true)
	s.ErrorIs(err, ErrUnknownReplicationTask)
}
