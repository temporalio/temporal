package replication

import (
	"math/rand"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/configs"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
)

type (
	executableSyncVersionedTransitionTaskSuite struct {
		suite.Suite
		*require.Assertions

		controller      *gomock.Controller
		clusterMetadata *cluster.MockMetadata
		shardController *shard.MockController
		namespaceCache  *namespace.MockRegistry
		metricsHandler  metrics.Handler
		logger          log.Logger
		config          *configs.Config
		executableTask  *MockExecutableTask

		namespaceID       namespace.ID
		workflowID        string
		runID             string
		taskID            int64
		taskCreationTime  time.Time
		sourceClusterName string
		sourceShardKey    ClusterShardKey
		processToolBox    ProcessToolBox
		replicationTask   *replicationspb.ReplicationTask
		task              *ExecutableSyncVersionedTransitionTask
	}
)

func TestExecutableSyncVersionedTransitionTaskSuite(t *testing.T) {
	s := new(executableSyncVersionedTransitionTaskSuite)
	suite.Run(t, s)
}

func (s *executableSyncVersionedTransitionTaskSuite) SetupSuite() {
	s.Assertions = require.New(s.T())
}

func (s *executableSyncVersionedTransitionTaskSuite) SetupTest() {
	s.controller = gomock.NewController(s.T())
	s.clusterMetadata = cluster.NewMockMetadata(s.controller)
	s.shardController = shard.NewMockController(s.controller)
	s.namespaceCache = namespace.NewMockRegistry(s.controller)
	s.metricsHandler = metrics.NoopMetricsHandler
	s.logger = log.NewNoopLogger()
	s.config = tests.NewDynamicConfig()
	s.executableTask = NewMockExecutableTask(s.controller)

	s.namespaceID = namespace.NewID()
	s.workflowID = uuid.NewString()
	s.runID = uuid.NewString()
	s.taskID = rand.Int63()
	s.taskCreationTime = time.Unix(0, rand.Int63())
	s.sourceClusterName = cluster.TestAlternativeClusterName
	s.sourceShardKey = ClusterShardKey{
		ClusterID: int32(cluster.TestAlternativeClusterInitialFailoverVersion),
		ShardID:   rand.Int31(),
	}
	s.processToolBox = ProcessToolBox{
		Config:          s.config,
		ClusterMetadata: s.clusterMetadata,
		ShardController: s.shardController,
		NamespaceCache:  s.namespaceCache,
		MetricsHandler:  s.metricsHandler,
		Logger:          s.logger,
		ThrottledLogger: s.logger,
	}

	// The cleanup path builds an ExecutableDeleteExecutionTask from the raw task info, so both the
	// sync attributes and the raw task info have to be present, as they are on the wire.
	s.replicationTask = &replicationspb.ReplicationTask{
		TaskType:     enumsspb.REPLICATION_TASK_TYPE_SYNC_VERSIONED_TRANSITION_TASK,
		SourceTaskId: s.taskID,
		Attributes: &replicationspb.ReplicationTask_SyncVersionedTransitionTaskAttributes{
			SyncVersionedTransitionTaskAttributes: &replicationspb.SyncVersionedTransitionTaskAttributes{
				NamespaceId: s.namespaceID.String(),
				WorkflowId:  s.workflowID,
				RunId:       s.runID,
				ArchetypeId: chasm.WorkflowArchetypeID,
			},
		},
		RawTaskInfo: &persistencespb.ReplicationTaskInfo{
			NamespaceId: s.namespaceID.String(),
			WorkflowId:  s.workflowID,
			RunId:       s.runID,
			TaskId:      s.taskID,
			ArchetypeId: chasm.WorkflowArchetypeID,
		},
	}

	s.task = NewExecutableSyncVersionedTransitionTask(
		s.processToolBox,
		s.taskID,
		s.taskCreationTime,
		s.sourceClusterName,
		s.sourceShardKey,
		s.replicationTask,
	)
	s.task.ExecutableTask = s.executableTask

	s.executableTask.EXPECT().NamespaceName().Return("namespace-name").AnyTimes()
	s.executableTask.EXPECT().TaskID().Return(s.taskID).AnyTimes()
	s.executableTask.EXPECT().TaskCreationTime().Return(s.taskCreationTime).AnyTimes()
	s.executableTask.EXPECT().SourceClusterName().Return(s.sourceClusterName).AnyTimes()
	s.executableTask.EXPECT().SourceShardKey().Return(s.sourceShardKey).AnyTimes()
	s.executableTask.EXPECT().ReplicationTask().Return(s.replicationTask).AnyTimes()
	s.executableTask.EXPECT().GetPriority().Return(enumsspb.TASK_PRIORITY_HIGH).AnyTimes()

	s.clusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
}

func (s *executableSyncVersionedTransitionTaskSuite) TearDownTest() {
	s.controller.Finish()
}

// The workflow was deleted on the source cluster while this task was in flight, and the target
// cluster no longer has it either. The cleanup has nothing left to delete, which is success, not
// failure: returning the NotFound would retry until the retry policy expires and DLQ the task.
func (s *executableSyncVersionedTransitionTaskSuite) TestHandleErr_NotFound_CleanupFindsNothingToDelete() {
	s.expectPassiveNamespace()
	s.expectDeleteWorkflowExecution(serviceerror.NewNotFoundf(
		"workflow execution not found for workflow ID %q and run ID %q", s.workflowID, s.runID))

	s.NoError(s.task.HandleErr(serviceerror.NewNotFound("workflow not found")))
}

func (s *executableSyncVersionedTransitionTaskSuite) TestHandleErr_NotFound_CleanupDeletesWorkflow() {
	s.expectPassiveNamespace()
	s.expectDeleteWorkflowExecution(nil)

	s.NoError(s.task.HandleErr(serviceerror.NewNotFound("workflow not found")))
}

func (s *executableSyncVersionedTransitionTaskSuite) TestHandleErr_NotFound_CleanupFails() {
	s.expectPassiveNamespace()
	s.expectDeleteWorkflowExecution(serviceerror.NewUnavailable("shard unavailable"))

	s.Error(s.task.HandleErr(serviceerror.NewNotFound("workflow not found")))
}

func (s *executableSyncVersionedTransitionTaskSuite) TestHandleErr_OtherError_PassedThrough() {
	err := serviceerror.NewUnavailable("shard unavailable")
	s.Equal(err, s.task.HandleErr(err))
}

// The namespace is active on the source cluster, so the deletion task proceeds past its
// active-cluster guard and reaches the actual delete.
func (s *executableSyncVersionedTransitionTaskSuite) expectPassiveNamespace() {
	detail := &persistencespb.NamespaceDetail{
		Info: &persistencespb.NamespaceInfo{
			Id:   s.namespaceID.String(),
			Name: "namespace-name",
		},
		Config: &persistencespb.NamespaceConfig{},
		ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestAlternativeClusterName,
			Clusters: []string{
				cluster.TestCurrentClusterName,
				cluster.TestAlternativeClusterName,
			},
		},
		FailoverVersion: cluster.TestCurrentClusterInitialFailoverVersion,
	}
	namespaceEntry, err := namespace.FromPersistentState(
		detail,
		namespace.NewDefaultReplicationResolverFactory()(detail),
	)
	s.NoError(err)
	s.namespaceCache.EXPECT().GetNamespaceByID(s.namespaceID).Return(namespaceEntry, nil).AnyTimes()
}

func (s *executableSyncVersionedTransitionTaskSuite) expectDeleteWorkflowExecution(deleteErr error) {
	shardContext := historyi.NewMockShardContext(s.controller)
	engine := historyi.NewMockEngine(s.controller)
	s.shardController.EXPECT().GetShardByNamespaceWorkflow(s.namespaceID, s.workflowID).Return(shardContext, nil)
	shardContext.EXPECT().GetEngine(gomock.Any()).Return(engine, nil)
	engine.EXPECT().DeleteWorkflowExecution(gomock.Any(), &historyservice.DeleteWorkflowExecutionRequest{
		NamespaceId: s.namespaceID.String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: s.workflowID,
			RunId:      s.runID,
		},
	}).Return(&historyservice.DeleteWorkflowExecutionResponse{}, deleteErr)
}
