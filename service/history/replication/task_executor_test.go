package replication

import (
	"context"
	"errors"
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
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/resourcetest"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/consts"
	deletemanager "go.temporal.io/server/service/history/deletemanager"
	historyi "go.temporal.io/server/service/history/interfaces"
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
		deleteManager           *deletemanager.MockDeleteManager
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
		&persistencespb.ShardInfo{
			ShardId: 0,
			RangeId: 1,
			ReplicationDlqAckLevel: map[string]int64{
				cluster.TestAlternativeClusterName: persistence.EmptyQueueMessageID,
			},
		},
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
	s.deleteManager = deletemanager.NewMockDeleteManager(s.controller)
	s.replicationTaskExecutor = NewTaskExecutor(
		s.remoteCluster,
		s.mockShard,
		s.remoteHistoryFetcher,
		s.deleteManager,
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
			&persistencespb.NamespaceReplicationConfig{Clusters: []string{
				cluster.TestCurrentClusterName,
				cluster.TestAlternativeClusterName,
			}},
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
			&persistencespb.NamespaceReplicationConfig{Clusters: []string{cluster.TestAlternativeClusterName}},
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
	task := &replicationspb.ReplicationTask{
		TaskType: enumsspb.REPLICATION_TASK_TYPE_SYNC_ACTIVITY_TASK,
		Attributes: &replicationspb.ReplicationTask_SyncActivityTaskAttributes{
			SyncActivityTaskAttributes: &replicationspb.SyncActivityTaskAttributes{
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
			},
		},
	}
	request := &historyservice.SyncActivityRequest{
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
	}

	s.historyClient.EXPECT().SyncActivity(gomock.Any(), request).Return(&historyservice.SyncActivityResponse{}, nil)
	err := s.replicationTaskExecutor.Execute(context.Background(), task, true)
	s.NoError(err)
}

func (s *taskExecutorSuite) TestProcessTaskOnce_SyncActivityReplicationTask_Resend() {
	namespaceID := namespace.ID(uuid.NewString())
	workflowID := uuid.NewString()
	runID := uuid.NewString()
	task := &replicationspb.ReplicationTask{
		TaskType: enumsspb.REPLICATION_TASK_TYPE_SYNC_ACTIVITY_TASK,
		Attributes: &replicationspb.ReplicationTask_SyncActivityTaskAttributes{
			SyncActivityTaskAttributes: &replicationspb.SyncActivityTaskAttributes{
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
			},
		},
	}
	request := &historyservice.SyncActivityRequest{
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
	}

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
	task := &replicationspb.ReplicationTask{
		TaskType: enumsspb.REPLICATION_TASK_TYPE_HISTORY_V2_TASK,
		Attributes: &replicationspb.ReplicationTask_HistoryTaskAttributes{
			HistoryTaskAttributes: &replicationspb.HistoryTaskAttributes{
				NamespaceId:         namespaceID.String(),
				WorkflowId:          workflowID,
				RunId:               runID,
				VersionHistoryItems: []*historyspb.VersionHistoryItem{{EventId: 233, Version: 2333}},
				Events:              nil,
				NewRunEvents:        nil,
			},
		},
	}
	request := &historyservice.ReplicateEventsV2Request{
		NamespaceId: namespaceID.String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
		VersionHistoryItems: []*historyspb.VersionHistoryItem{{EventId: 233, Version: 2333}},
		Events:              nil,
		NewRunEvents:        nil,
	}
	s.historyClient.EXPECT().ReplicateEventsV2(gomock.Any(), request).Return(&historyservice.ReplicateEventsV2Response{}, nil)
	err := s.replicationTaskExecutor.Execute(context.Background(), task, true)
	s.NoError(err)
}

func (s *taskExecutorSuite) TestProcess_HistoryReplicationTask_Resend() {
	namespaceID := namespace.ID(uuid.NewString())
	workflowID := uuid.NewString()
	runID := uuid.NewString()
	task := &replicationspb.ReplicationTask{
		TaskType: enumsspb.REPLICATION_TASK_TYPE_HISTORY_V2_TASK,
		Attributes: &replicationspb.ReplicationTask_HistoryTaskAttributes{
			HistoryTaskAttributes: &replicationspb.HistoryTaskAttributes{
				NamespaceId:         namespaceID.String(),
				WorkflowId:          workflowID,
				RunId:               runID,
				VersionHistoryItems: []*historyspb.VersionHistoryItem{{EventId: 233, Version: 2333}},
				Events:              nil,
				NewRunEvents:        nil,
			},
		},
	}
	request := &historyservice.ReplicateEventsV2Request{
		NamespaceId: namespaceID.String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
		VersionHistoryItems: []*historyspb.VersionHistoryItem{{EventId: 233, Version: 2333}},
		Events:              nil,
		NewRunEvents:        nil,
	}

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
	task := &replicationspb.ReplicationTask{
		TaskType: enumsspb.REPLICATION_TASK_TYPE_SYNC_WORKFLOW_STATE_TASK,
		Attributes: &replicationspb.ReplicationTask_SyncWorkflowStateTaskAttributes{
			SyncWorkflowStateTaskAttributes: &replicationspb.SyncWorkflowStateTaskAttributes{
				WorkflowState: &persistencespb.WorkflowMutableState{
					ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
						NamespaceId: namespaceID.String(),
					},
				},
			},
		},
	}
	s.historyClient.EXPECT().ReplicateWorkflowState(gomock.Any(), gomock.Any()).Return(&historyservice.ReplicateWorkflowStateResponse{}, nil)

	err := s.replicationTaskExecutor.Execute(context.Background(), task, true)
	s.NoError(err)
}

func (s *taskExecutorSuite) TestProcessTaskOnce_SyncActivityReplicationTask_Duplicate() {
	namespaceID := namespace.ID(uuid.NewString())
	workflowID := uuid.NewString()
	runID := uuid.NewString()
	task := &replicationspb.ReplicationTask{
		TaskType: enumsspb.REPLICATION_TASK_TYPE_SYNC_ACTIVITY_TASK,
		Attributes: &replicationspb.ReplicationTask_SyncActivityTaskAttributes{
			SyncActivityTaskAttributes: &replicationspb.SyncActivityTaskAttributes{
				NamespaceId: namespaceID.String(),
				WorkflowId:  workflowID,
				RunId:       runID,
			},
		},
	}
	s.historyClient.EXPECT().SyncActivity(gomock.Any(), gomock.Any()).Return(nil, consts.ErrDuplicate)
	err := s.replicationTaskExecutor.Execute(context.Background(), task, true)
	s.NoError(err)
}

func (s *taskExecutorSuite) TestProcessTaskOnce_SyncActivityReplicationTask_DefaultError() {
	namespaceID := namespace.ID(uuid.NewString())
	workflowID := uuid.NewString()
	runID := uuid.NewString()
	task := &replicationspb.ReplicationTask{
		TaskType: enumsspb.REPLICATION_TASK_TYPE_SYNC_ACTIVITY_TASK,
		Attributes: &replicationspb.ReplicationTask_SyncActivityTaskAttributes{
			SyncActivityTaskAttributes: &replicationspb.SyncActivityTaskAttributes{
				NamespaceId: namespaceID.String(),
				WorkflowId:  workflowID,
				RunId:       runID,
			},
		},
	}
	syncErr := errors.New("sync activity random error")
	s.historyClient.EXPECT().SyncActivity(gomock.Any(), gomock.Any()).Return(nil, syncErr)
	err := s.replicationTaskExecutor.Execute(context.Background(), task, true)
	s.ErrorIs(err, syncErr)
}

func (s *taskExecutorSuite) TestProcessTaskOnce_SyncActivityReplicationTask_ResendNotFound_Cleanup() {
	namespaceID := namespace.ID(uuid.NewString())
	workflowID := uuid.NewString()
	runID := uuid.NewString()
	task := &replicationspb.ReplicationTask{
		TaskType: enumsspb.REPLICATION_TASK_TYPE_SYNC_ACTIVITY_TASK,
		Attributes: &replicationspb.ReplicationTask_SyncActivityTaskAttributes{
			SyncActivityTaskAttributes: &replicationspb.SyncActivityTaskAttributes{
				NamespaceId: namespaceID.String(),
				WorkflowId:  workflowID,
				RunId:       runID,
			},
		},
	}

	resendErr := serviceerrors.NewRetryReplication(
		"retry", namespaceID.String(), workflowID, runID, 123, 234, 345, 456,
	)
	s.historyClient.EXPECT().SyncActivity(gomock.Any(), gomock.Any()).Return(nil, resendErr)

	// resend iterator surfaces a NotFound error -> triggers cleanup path.
	notFoundIterator := collection.NewPagingIterator(func(paginationToken []byte) ([]*eventhandler.HistoryBatch, []byte, error) {
		return nil, nil, serviceerror.NewNotFound("not found in source")
	})
	s.remoteHistoryFetcher.EXPECT().GetSingleWorkflowHistoryPaginatedIteratorExclusive(
		gomock.Any(), s.remoteCluster, namespaceID, workflowID, runID,
		int64(123), int64(234), int64(345), int64(456),
	).Return(notFoundIterator)

	// cleanupWorkflowExecution wiring.
	execution := &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: runID}
	taskExecWfCtx := historyi.NewMockWorkflowContext(s.controller)
	taskExecMutableState := historyi.NewMockMutableState(s.controller)
	s.workflowCache.EXPECT().GetOrCreateWorkflowExecution(
		gomock.Any(), s.mockShard, namespaceID, execution, locks.PriorityLow,
	).Return(taskExecWfCtx, historyi.ReleaseWorkflowContextFunc(func(error) {}), nil)
	taskExecWfCtx.EXPECT().LoadMutableState(gomock.Any(), s.mockShard).Return(taskExecMutableState, nil)
	s.deleteManager.EXPECT().DeleteWorkflowExecution(
		gomock.Any(), namespaceID, execution, taskExecWfCtx, taskExecMutableState, nil,
	).Return(nil)

	err := s.replicationTaskExecutor.Execute(context.Background(), task, true)
	s.NoError(err)
}

func (s *taskExecutorSuite) TestProcessTaskOnce_SyncActivityReplicationTask_ResendError() {
	namespaceID := namespace.ID(uuid.NewString())
	workflowID := uuid.NewString()
	runID := uuid.NewString()
	task := &replicationspb.ReplicationTask{
		TaskType: enumsspb.REPLICATION_TASK_TYPE_SYNC_ACTIVITY_TASK,
		Attributes: &replicationspb.ReplicationTask_SyncActivityTaskAttributes{
			SyncActivityTaskAttributes: &replicationspb.SyncActivityTaskAttributes{
				NamespaceId: namespaceID.String(),
				WorkflowId:  workflowID,
				RunId:       runID,
			},
		},
	}

	resendErr := serviceerrors.NewRetryReplication(
		"retry", namespaceID.String(), workflowID, runID, 123, 234, 345, 456,
	)
	s.historyClient.EXPECT().SyncActivity(gomock.Any(), gomock.Any()).Return(nil, resendErr)

	// resend iterator returns a non-NotFound error -> default branch, returns original err.
	errIterator := collection.NewPagingIterator(func(paginationToken []byte) ([]*eventhandler.HistoryBatch, []byte, error) {
		return nil, nil, errors.New("resend random error")
	})
	s.remoteHistoryFetcher.EXPECT().GetSingleWorkflowHistoryPaginatedIteratorExclusive(
		gomock.Any(), s.remoteCluster, namespaceID, workflowID, runID,
		int64(123), int64(234), int64(345), int64(456),
	).Return(errIterator)

	err := s.replicationTaskExecutor.Execute(context.Background(), task, true)
	s.ErrorIs(err, resendErr)
}

func (s *taskExecutorSuite) TestProcess_HistoryReplicationTask_Duplicate() {
	namespaceID := namespace.ID(uuid.NewString())
	workflowID := uuid.NewString()
	runID := uuid.NewString()
	task := &replicationspb.ReplicationTask{
		TaskType: enumsspb.REPLICATION_TASK_TYPE_HISTORY_V2_TASK,
		Attributes: &replicationspb.ReplicationTask_HistoryTaskAttributes{
			HistoryTaskAttributes: &replicationspb.HistoryTaskAttributes{
				NamespaceId: namespaceID.String(),
				WorkflowId:  workflowID,
				RunId:       runID,
			},
		},
	}
	s.historyClient.EXPECT().ReplicateEventsV2(gomock.Any(), gomock.Any()).Return(nil, consts.ErrDuplicate)
	err := s.replicationTaskExecutor.Execute(context.Background(), task, true)
	s.NoError(err)
}

func (s *taskExecutorSuite) TestProcess_HistoryReplicationTask_DefaultError() {
	namespaceID := namespace.ID(uuid.NewString())
	workflowID := uuid.NewString()
	runID := uuid.NewString()
	task := &replicationspb.ReplicationTask{
		TaskType: enumsspb.REPLICATION_TASK_TYPE_HISTORY_V2_TASK,
		Attributes: &replicationspb.ReplicationTask_HistoryTaskAttributes{
			HistoryTaskAttributes: &replicationspb.HistoryTaskAttributes{
				NamespaceId: namespaceID.String(),
				WorkflowId:  workflowID,
				RunId:       runID,
			},
		},
	}
	replicateErr := errors.New("replicate random error")
	s.historyClient.EXPECT().ReplicateEventsV2(gomock.Any(), gomock.Any()).Return(nil, replicateErr)
	err := s.replicationTaskExecutor.Execute(context.Background(), task, true)
	s.ErrorIs(err, replicateErr)
}

func (s *taskExecutorSuite) TestProcess_HistoryReplicationTask_ResendNotFound_Cleanup() {
	namespaceID := namespace.ID(uuid.NewString())
	workflowID := uuid.NewString()
	runID := uuid.NewString()
	task := &replicationspb.ReplicationTask{
		TaskType: enumsspb.REPLICATION_TASK_TYPE_HISTORY_V2_TASK,
		Attributes: &replicationspb.ReplicationTask_HistoryTaskAttributes{
			HistoryTaskAttributes: &replicationspb.HistoryTaskAttributes{
				NamespaceId: namespaceID.String(),
				WorkflowId:  workflowID,
				RunId:       runID,
			},
		},
	}

	resendErr := serviceerrors.NewRetryReplication(
		"retry", namespaceID.String(), workflowID, runID, 123, 234, 345, 456,
	)
	s.historyClient.EXPECT().ReplicateEventsV2(gomock.Any(), gomock.Any()).Return(nil, resendErr)

	notFoundIterator := collection.NewPagingIterator(func(paginationToken []byte) ([]*eventhandler.HistoryBatch, []byte, error) {
		return nil, nil, serviceerror.NewNotFound("not found in source")
	})
	s.remoteHistoryFetcher.EXPECT().GetSingleWorkflowHistoryPaginatedIteratorExclusive(
		gomock.Any(), s.remoteCluster, namespaceID, workflowID, runID,
		int64(123), int64(234), int64(345), int64(456),
	).Return(notFoundIterator)

	execution := &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: runID}
	taskExecWfCtx := historyi.NewMockWorkflowContext(s.controller)
	taskExecMutableState := historyi.NewMockMutableState(s.controller)
	s.workflowCache.EXPECT().GetOrCreateWorkflowExecution(
		gomock.Any(), s.mockShard, namespaceID, execution, locks.PriorityLow,
	).Return(taskExecWfCtx, historyi.ReleaseWorkflowContextFunc(func(error) {}), nil)
	taskExecWfCtx.EXPECT().LoadMutableState(gomock.Any(), s.mockShard).Return(taskExecMutableState, nil)
	s.deleteManager.EXPECT().DeleteWorkflowExecution(
		gomock.Any(), namespaceID, execution, taskExecWfCtx, taskExecMutableState, nil,
	).Return(nil)

	err := s.replicationTaskExecutor.Execute(context.Background(), task, true)
	s.NoError(err)
}

func (s *taskExecutorSuite) TestProcess_HistoryReplicationTask_ResendError() {
	namespaceID := namespace.ID(uuid.NewString())
	workflowID := uuid.NewString()
	runID := uuid.NewString()
	task := &replicationspb.ReplicationTask{
		TaskType: enumsspb.REPLICATION_TASK_TYPE_HISTORY_V2_TASK,
		Attributes: &replicationspb.ReplicationTask_HistoryTaskAttributes{
			HistoryTaskAttributes: &replicationspb.HistoryTaskAttributes{
				NamespaceId: namespaceID.String(),
				WorkflowId:  workflowID,
				RunId:       runID,
			},
		},
	}

	resendErr := serviceerrors.NewRetryReplication(
		"retry", namespaceID.String(), workflowID, runID, 123, 234, 345, 456,
	)
	s.historyClient.EXPECT().ReplicateEventsV2(gomock.Any(), gomock.Any()).Return(nil, resendErr)

	errIterator := collection.NewPagingIterator(func(paginationToken []byte) ([]*eventhandler.HistoryBatch, []byte, error) {
		return nil, nil, errors.New("resend random error")
	})
	s.remoteHistoryFetcher.EXPECT().GetSingleWorkflowHistoryPaginatedIteratorExclusive(
		gomock.Any(), s.remoteCluster, namespaceID, workflowID, runID,
		int64(123), int64(234), int64(345), int64(456),
	).Return(errIterator)

	err := s.replicationTaskExecutor.Execute(context.Background(), task, true)
	s.ErrorIs(err, resendErr)
}

func (s *taskExecutorSuite) TestProcessTaskOnce_SyncWorkflowStateTask_Resend() {
	namespaceID := namespace.ID(uuid.NewString())
	workflowID := uuid.NewString()
	runID := uuid.NewString()
	task := &replicationspb.ReplicationTask{
		TaskType: enumsspb.REPLICATION_TASK_TYPE_SYNC_WORKFLOW_STATE_TASK,
		Attributes: &replicationspb.ReplicationTask_SyncWorkflowStateTaskAttributes{
			SyncWorkflowStateTaskAttributes: &replicationspb.SyncWorkflowStateTaskAttributes{
				WorkflowState: &persistencespb.WorkflowMutableState{
					ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
						NamespaceId: namespaceID.String(),
						WorkflowId:  workflowID,
					},
				},
			},
		},
	}

	resendErr := serviceerrors.NewRetryReplication(
		"retry", namespaceID.String(), workflowID, runID, 123, 234, 345, 456,
	)
	s.historyClient.EXPECT().ReplicateWorkflowState(gomock.Any(), gomock.Any()).Return(nil, resendErr)
	emptyIterator := collection.NewPagingIterator(func(paginationToken []byte) ([]*eventhandler.HistoryBatch, []byte, error) {
		return nil, nil, nil
	})
	s.remoteHistoryFetcher.EXPECT().GetSingleWorkflowHistoryPaginatedIteratorExclusive(
		gomock.Any(), s.remoteCluster, namespaceID, workflowID, runID,
		int64(123), int64(234), int64(345), int64(456),
	).Return(emptyIterator)
	// resend succeeds with nil error -> retries ReplicateWorkflowState.
	s.historyClient.EXPECT().ReplicateWorkflowState(gomock.Any(), gomock.Any()).Return(&historyservice.ReplicateWorkflowStateResponse{}, nil)

	err := s.replicationTaskExecutor.Execute(context.Background(), task, true)
	s.NoError(err)
}

func (s *taskExecutorSuite) TestProcessTaskOnce_SyncWorkflowStateTask_ResendNotFound_Cleanup() {
	namespaceID := namespace.ID(uuid.NewString())
	workflowID := uuid.NewString()
	runID := uuid.NewString()
	task := &replicationspb.ReplicationTask{
		TaskType: enumsspb.REPLICATION_TASK_TYPE_SYNC_WORKFLOW_STATE_TASK,
		Attributes: &replicationspb.ReplicationTask_SyncWorkflowStateTaskAttributes{
			SyncWorkflowStateTaskAttributes: &replicationspb.SyncWorkflowStateTaskAttributes{
				WorkflowState: &persistencespb.WorkflowMutableState{
					ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
						NamespaceId: namespaceID.String(),
						WorkflowId:  workflowID,
					},
				},
			},
		},
	}

	resendErr := serviceerrors.NewRetryReplication(
		"retry", namespaceID.String(), workflowID, runID, 123, 234, 345, 456,
	)
	s.historyClient.EXPECT().ReplicateWorkflowState(gomock.Any(), gomock.Any()).Return(nil, resendErr)
	notFoundIterator := collection.NewPagingIterator(func(paginationToken []byte) ([]*eventhandler.HistoryBatch, []byte, error) {
		return nil, nil, serviceerror.NewNotFound("not found in source")
	})
	s.remoteHistoryFetcher.EXPECT().GetSingleWorkflowHistoryPaginatedIteratorExclusive(
		gomock.Any(), s.remoteCluster, namespaceID, workflowID, runID,
		int64(123), int64(234), int64(345), int64(456),
	).Return(notFoundIterator)

	execution := &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: runID}
	taskExecWfCtx := historyi.NewMockWorkflowContext(s.controller)
	taskExecMutableState := historyi.NewMockMutableState(s.controller)
	s.workflowCache.EXPECT().GetOrCreateWorkflowExecution(
		gomock.Any(), s.mockShard, namespaceID, execution, locks.PriorityLow,
	).Return(taskExecWfCtx, historyi.ReleaseWorkflowContextFunc(func(error) {}), nil)
	taskExecWfCtx.EXPECT().LoadMutableState(gomock.Any(), s.mockShard).Return(taskExecMutableState, nil)
	s.deleteManager.EXPECT().DeleteWorkflowExecution(
		gomock.Any(), namespaceID, execution, taskExecWfCtx, taskExecMutableState, nil,
	).Return(nil)

	err := s.replicationTaskExecutor.Execute(context.Background(), task, true)
	s.NoError(err)
}

func (s *taskExecutorSuite) TestProcessTaskOnce_SyncWorkflowStateTask_ResendError() {
	namespaceID := namespace.ID(uuid.NewString())
	workflowID := uuid.NewString()
	runID := uuid.NewString()
	task := &replicationspb.ReplicationTask{
		TaskType: enumsspb.REPLICATION_TASK_TYPE_SYNC_WORKFLOW_STATE_TASK,
		Attributes: &replicationspb.ReplicationTask_SyncWorkflowStateTaskAttributes{
			SyncWorkflowStateTaskAttributes: &replicationspb.SyncWorkflowStateTaskAttributes{
				WorkflowState: &persistencespb.WorkflowMutableState{
					ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
						NamespaceId: namespaceID.String(),
						WorkflowId:  workflowID,
					},
				},
			},
		},
	}

	resendErr := serviceerrors.NewRetryReplication(
		"retry", namespaceID.String(), workflowID, runID, 123, 234, 345, 456,
	)
	s.historyClient.EXPECT().ReplicateWorkflowState(gomock.Any(), gomock.Any()).Return(nil, resendErr)
	errIterator := collection.NewPagingIterator(func(paginationToken []byte) ([]*eventhandler.HistoryBatch, []byte, error) {
		return nil, nil, errors.New("resend random error")
	})
	s.remoteHistoryFetcher.EXPECT().GetSingleWorkflowHistoryPaginatedIteratorExclusive(
		gomock.Any(), s.remoteCluster, namespaceID, workflowID, runID,
		int64(123), int64(234), int64(345), int64(456),
	).Return(errIterator)

	err := s.replicationTaskExecutor.Execute(context.Background(), task, true)
	s.ErrorIs(err, resendErr)
}

func (s *taskExecutorSuite) TestProcessTaskOnce_SyncWorkflowStateTask_Duplicate() {
	namespaceID := namespace.ID(uuid.NewString())
	task := &replicationspb.ReplicationTask{
		TaskType: enumsspb.REPLICATION_TASK_TYPE_SYNC_WORKFLOW_STATE_TASK,
		Attributes: &replicationspb.ReplicationTask_SyncWorkflowStateTaskAttributes{
			SyncWorkflowStateTaskAttributes: &replicationspb.SyncWorkflowStateTaskAttributes{
				WorkflowState: &persistencespb.WorkflowMutableState{
					ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
						NamespaceId: namespaceID.String(),
					},
				},
			},
		},
	}
	s.historyClient.EXPECT().ReplicateWorkflowState(gomock.Any(), gomock.Any()).Return(nil, consts.ErrDuplicate)
	err := s.replicationTaskExecutor.Execute(context.Background(), task, true)
	s.NoError(err)
}

// TestResend_WithBatches drives resend through a non-empty iterator so the
// per-batch ReplicateEventsV2 call path is exercised.
func (s *taskExecutorSuite) TestResend_WithBatches() {
	namespaceID := namespace.ID(uuid.NewString())
	workflowID := uuid.NewString()
	runID := uuid.NewString()

	batch := &eventhandler.HistoryBatch{
		RawEventBatch:  &commonpb.DataBlob{Data: []byte("events")},
		VersionHistory: &historyspb.VersionHistory{Items: []*historyspb.VersionHistoryItem{{EventId: 1, Version: 1}}},
	}
	calls := 0
	iterator := collection.NewPagingIterator(func(paginationToken []byte) ([]*eventhandler.HistoryBatch, []byte, error) {
		calls++
		if calls == 1 {
			return []*eventhandler.HistoryBatch{batch}, nil, nil
		}
		return nil, nil, nil
	})
	s.remoteHistoryFetcher.EXPECT().GetSingleWorkflowHistoryPaginatedIteratorExclusive(
		gomock.Any(), s.remoteCluster, namespaceID, workflowID, runID,
		int64(1), int64(2), int64(3), int64(4),
	).Return(iterator)
	s.historyClient.EXPECT().ReplicateEventsV2(gomock.Any(), gomock.Any()).Return(&historyservice.ReplicateEventsV2Response{}, nil)

	err := s.replicationTaskExecutor.resend(
		context.Background(), s.remoteCluster, namespaceID, workflowID, runID,
		1, 2, 3, 4,
	)
	s.NoError(err)
}

// TestResend_BatchReplicateError drives resend so the ReplicateEventsV2 call
// for a batch fails, returning the error.
func (s *taskExecutorSuite) TestResend_BatchReplicateError() {
	namespaceID := namespace.ID(uuid.NewString())
	workflowID := uuid.NewString()
	runID := uuid.NewString()

	batch := &eventhandler.HistoryBatch{
		RawEventBatch:  &commonpb.DataBlob{Data: []byte("events")},
		VersionHistory: &historyspb.VersionHistory{Items: []*historyspb.VersionHistoryItem{{EventId: 1, Version: 1}}},
	}
	iterator := collection.NewPagingIterator(func(paginationToken []byte) ([]*eventhandler.HistoryBatch, []byte, error) {
		return []*eventhandler.HistoryBatch{batch}, nil, nil
	})
	s.remoteHistoryFetcher.EXPECT().GetSingleWorkflowHistoryPaginatedIteratorExclusive(
		gomock.Any(), s.remoteCluster, namespaceID, workflowID, runID,
		int64(1), int64(2), int64(3), int64(4),
	).Return(iterator)
	replicateErr := errors.New("replicate batch error")
	s.historyClient.EXPECT().ReplicateEventsV2(gomock.Any(), gomock.Any()).Return(nil, replicateErr)

	err := s.replicationTaskExecutor.resend(
		context.Background(), s.remoteCluster, namespaceID, workflowID, runID,
		1, 2, 3, 4,
	)
	s.ErrorIs(err, replicateErr)
}

func (s *taskExecutorSuite) TestExecute_SyncShardStatusTask_NoOp() {
	task := &replicationspb.ReplicationTask{
		TaskType: enumsspb.REPLICATION_TASK_TYPE_SYNC_SHARD_STATUS_TASK,
	}
	err := s.replicationTaskExecutor.Execute(context.Background(), task, true)
	s.NoError(err)
}

func (s *taskExecutorSuite) TestExecute_HistoryMetadataTask_NoOp() {
	task := &replicationspb.ReplicationTask{
		TaskType: enumsspb.REPLICATION_TASK_TYPE_HISTORY_METADATA_TASK,
	}
	err := s.replicationTaskExecutor.Execute(context.Background(), task, true)
	s.NoError(err)
}

// taskExecCleanupExecutor builds an executor wired to a deleteManager and
// workflowCache the test fully controls, so cleanupWorkflowExecution branches
// can be exercised deterministically.
func (s *taskExecutorSuite) taskExecCleanupExecutor(
	deleteManager *deletemanager.MockDeleteManager,
) *taskExecutorImpl {
	return NewTaskExecutor(
		s.remoteCluster,
		s.mockShard,
		s.remoteHistoryFetcher,
		deleteManager,
		s.workflowCache,
	).(*taskExecutorImpl)
}

func (s *taskExecutorSuite) TestCleanupWorkflowExecution_Success() {
	namespaceID := namespace.ID(uuid.NewString())
	workflowID := uuid.NewString()
	runID := uuid.NewString()

	taskExecDeleteMgr := deletemanager.NewMockDeleteManager(s.controller)
	executor := s.taskExecCleanupExecutor(taskExecDeleteMgr)

	taskExecWfCtx := historyi.NewMockWorkflowContext(s.controller)
	taskExecMutableState := historyi.NewMockMutableState(s.controller)
	taskExecReleased := false
	releaseFn := func(error) { taskExecReleased = true }

	execution := &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: runID}
	s.workflowCache.EXPECT().GetOrCreateWorkflowExecution(
		gomock.Any(),
		s.mockShard,
		namespaceID,
		execution,
		locks.PriorityLow,
	).Return(taskExecWfCtx, historyi.ReleaseWorkflowContextFunc(releaseFn), nil)
	taskExecWfCtx.EXPECT().LoadMutableState(gomock.Any(), s.mockShard).Return(taskExecMutableState, nil)
	taskExecDeleteMgr.EXPECT().DeleteWorkflowExecution(
		gomock.Any(),
		namespaceID,
		execution,
		taskExecWfCtx,
		taskExecMutableState,
		nil,
	).Return(nil)

	err := executor.cleanupWorkflowExecution(context.Background(), namespaceID.String(), workflowID, runID)
	s.NoError(err)
	s.True(taskExecReleased)
}

func (s *taskExecutorSuite) TestCleanupWorkflowExecution_GetOrCreateError() {
	namespaceID := namespace.ID(uuid.NewString())
	workflowID := uuid.NewString()
	runID := uuid.NewString()

	taskExecDeleteMgr := deletemanager.NewMockDeleteManager(s.controller)
	executor := s.taskExecCleanupExecutor(taskExecDeleteMgr)

	getErr := errors.New("get or create error")
	s.workflowCache.EXPECT().GetOrCreateWorkflowExecution(
		gomock.Any(),
		s.mockShard,
		namespaceID,
		&commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: runID},
		locks.PriorityLow,
	).Return(nil, historyi.ReleaseWorkflowContextFunc(nil), getErr)

	err := executor.cleanupWorkflowExecution(context.Background(), namespaceID.String(), workflowID, runID)
	s.ErrorIs(err, getErr)
}

func (s *taskExecutorSuite) TestCleanupWorkflowExecution_LoadMutableStateError() {
	namespaceID := namespace.ID(uuid.NewString())
	workflowID := uuid.NewString()
	runID := uuid.NewString()

	taskExecDeleteMgr := deletemanager.NewMockDeleteManager(s.controller)
	executor := s.taskExecCleanupExecutor(taskExecDeleteMgr)

	taskExecWfCtx := historyi.NewMockWorkflowContext(s.controller)
	taskExecReleased := false
	releaseFn := func(error) { taskExecReleased = true }

	loadErr := errors.New("load mutable state error")
	s.workflowCache.EXPECT().GetOrCreateWorkflowExecution(
		gomock.Any(),
		s.mockShard,
		namespaceID,
		&commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: runID},
		locks.PriorityLow,
	).Return(taskExecWfCtx, historyi.ReleaseWorkflowContextFunc(releaseFn), nil)
	taskExecWfCtx.EXPECT().LoadMutableState(gomock.Any(), s.mockShard).Return(nil, loadErr)

	err := executor.cleanupWorkflowExecution(context.Background(), namespaceID.String(), workflowID, runID)
	s.ErrorIs(err, loadErr)
	s.True(taskExecReleased)
}

func (s *taskExecutorSuite) TestProcessTaskOnce_SyncHSMTask() {
	namespaceID := namespace.ID(uuid.NewString())
	workflowID := uuid.NewString()
	runID := uuid.NewString()
	task := &replicationspb.ReplicationTask{
		SourceTaskId: rand.Int63(),
		TaskType:     enumsspb.REPLICATION_TASK_TYPE_SYNC_HSM_TASK,
		Attributes: &replicationspb.ReplicationTask_SyncHsmAttributes{SyncHsmAttributes: &replicationspb.SyncHSMAttributes{
			NamespaceId:      namespaceID.String(),
			WorkflowId:       workflowID,
			RunId:            runID,
			VersionHistory:   &historyspb.VersionHistory{},
			StateMachineNode: &persistencespb.StateMachineNode{},
		}},
		VisibilityTime: timestamppb.New(time.Now()),
	}

	// not handling SyncHSMTask in deprecated replication task processing code path
	err := s.replicationTaskExecutor.Execute(context.Background(), task, true)
	s.ErrorIs(err, ErrUnknownReplicationTask)
}
