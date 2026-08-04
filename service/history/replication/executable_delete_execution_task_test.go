package replication

import (
	"math/rand"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common"
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
	executableDeleteExecutionTaskSuite struct {
		suite.Suite
		*require.Assertions

		controller      *gomock.Controller
		clusterMetadata *cluster.MockMetadata
		shardController *shard.MockController
		namespaceCache  *namespace.MockRegistry
		metricsHandler  metrics.Handler
		logger          log.Logger
		config          *configs.Config

		namespaceID       namespace.ID
		workflowID        string
		runID             string
		taskID            int64
		taskCreationTime  time.Time
		sourceClusterName string
		sourceShardKey    ClusterShardKey
		processToolBox    ProcessToolBox
	}
)

func TestExecutableDeleteExecutionTaskSuite(t *testing.T) {
	s := new(executableDeleteExecutionTaskSuite)
	suite.Run(t, s)
}

func (s *executableDeleteExecutionTaskSuite) SetupSuite() {
	s.Assertions = require.New(s.T())
}

func (s *executableDeleteExecutionTaskSuite) SetupTest() {
	s.controller = gomock.NewController(s.T())
	s.clusterMetadata = cluster.NewMockMetadata(s.controller)
	s.shardController = shard.NewMockController(s.controller)
	s.namespaceCache = namespace.NewMockRegistry(s.controller)
	s.metricsHandler = metrics.NoopMetricsHandler
	s.logger = log.NewNoopLogger()
	s.config = tests.NewDynamicConfig()

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

	s.clusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
}

func (s *executableDeleteExecutionTaskSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *executableDeleteExecutionTaskSuite) TestExecute_CurrentClusterActive_SkipsTask() {
	namespaceEntry := s.namespaceEntry(cluster.TestCurrentClusterName)
	s.namespaceCache.EXPECT().GetNamespaceByID(s.namespaceID).Return(namespaceEntry, nil).Times(3)

	s.NoError(s.newTask().Execute())
}

func (s *executableDeleteExecutionTaskSuite) TestExecute_CurrentClusterPassive_DeletesWorkflowExecution() {
	namespaceEntry := s.namespaceEntry(cluster.TestAlternativeClusterName)
	s.namespaceCache.EXPECT().GetNamespaceByID(s.namespaceID).Return(namespaceEntry, nil).Times(3)

	s.expectDeleteWorkflowExecution()

	s.NoError(s.newTask().Execute())
}

// A task generated before the namespace failed over is stale: the cluster that decided the deletion
// no longer owns the execution, so the deletion must not be applied here.
func (s *executableDeleteExecutionTaskSuite) TestExecute_StaleVersionAfterFailover_SkipsTask() {
	taskVersion := cluster.TestAlternativeClusterInitialFailoverVersion
	namespaceEntry := s.namespaceEntryWithFailoverVersion(
		cluster.TestAlternativeClusterName,
		taskVersion+cluster.TestFailoverVersionIncrement,
	)
	s.namespaceCache.EXPECT().GetNamespaceByID(s.namespaceID).Return(namespaceEntry, nil).Times(3)

	// No delete is expected on the shard controller / engine.
	s.NoError(s.newTaskWithVersion(taskVersion).Execute())
}

func (s *executableDeleteExecutionTaskSuite) TestExecute_CurrentVersion_DeletesWorkflowExecution() {
	taskVersion := cluster.TestAlternativeClusterInitialFailoverVersion
	namespaceEntry := s.namespaceEntryWithFailoverVersion(cluster.TestAlternativeClusterName, taskVersion)
	s.namespaceCache.EXPECT().GetNamespaceByID(s.namespaceID).Return(namespaceEntry, nil).Times(3)

	s.expectDeleteWorkflowExecution()

	s.NoError(s.newTaskWithVersion(taskVersion).Execute())
}

// The target's namespace entry may lag behind the source's. A task newer than the locally known
// failover version is not stale and must still be applied.
func (s *executableDeleteExecutionTaskSuite) TestExecute_VersionNewerThanNamespace_DeletesWorkflowExecution() {
	taskVersion := cluster.TestAlternativeClusterInitialFailoverVersion + cluster.TestFailoverVersionIncrement
	namespaceEntry := s.namespaceEntryWithFailoverVersion(
		cluster.TestAlternativeClusterName,
		cluster.TestAlternativeClusterInitialFailoverVersion,
	)
	s.namespaceCache.EXPECT().GetNamespaceByID(s.namespaceID).Return(namespaceEntry, nil).Times(3)

	s.expectDeleteWorkflowExecution()

	s.NoError(s.newTaskWithVersion(taskVersion).Execute())
}

// Tasks generated before the version was introduced carry no version and must keep being applied.
func (s *executableDeleteExecutionTaskSuite) TestExecute_UnversionedTask_DeletesWorkflowExecution() {
	namespaceEntry := s.namespaceEntryWithFailoverVersion(
		cluster.TestAlternativeClusterName,
		cluster.TestAlternativeClusterInitialFailoverVersion+cluster.TestFailoverVersionIncrement,
	)
	s.namespaceCache.EXPECT().GetNamespaceByID(s.namespaceID).Return(namespaceEntry, nil).Times(3)

	s.expectDeleteWorkflowExecution()

	s.NoError(s.newTaskWithVersion(common.EmptyVersion).Execute())
}

// Sync/verify versioned transition tasks synthesize a deletion out of their own replication task.
// Their version describes a different operation and must not be treated as a deletion version.
func (s *executableDeleteExecutionTaskSuite) TestExecute_SynthesizedDeletion_IgnoresSourceTaskVersion() {
	namespaceEntry := s.namespaceEntryWithFailoverVersion(
		cluster.TestAlternativeClusterName,
		cluster.TestAlternativeClusterInitialFailoverVersion+cluster.TestFailoverVersionIncrement,
	)
	s.namespaceCache.EXPECT().GetNamespaceByID(s.namespaceID).Return(namespaceEntry, nil).Times(3)

	s.expectDeleteWorkflowExecution()

	task := NewExecutableDeleteExecutionTask(
		s.processToolBox,
		s.taskID,
		s.taskCreationTime,
		s.sourceClusterName,
		s.sourceShardKey,
		&replicationspb.ReplicationTask{
			TaskType: enumsspb.REPLICATION_TASK_TYPE_SYNC_VERSIONED_TRANSITION_TASK,
			RawTaskInfo: &persistencespb.ReplicationTaskInfo{
				NamespaceId: s.namespaceID.String(),
				WorkflowId:  s.workflowID,
				RunId:       s.runID,
				TaskId:      s.taskID,
				TaskType:    enumsspb.TASK_TYPE_REPLICATION_SYNC_VERSIONED_TRANSITION,
				ArchetypeId: chasm.WorkflowArchetypeID,
				Version:     cluster.TestAlternativeClusterInitialFailoverVersion,
			},
		},
	)
	s.NoError(task.Execute())
}

func (s *executableDeleteExecutionTaskSuite) expectDeleteWorkflowExecution() {
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
	}).Return(&historyservice.DeleteWorkflowExecutionResponse{}, nil)
}

func (s *executableDeleteExecutionTaskSuite) newTask() *ExecutableDeleteExecutionTask {
	return s.newTaskWithVersion(common.EmptyVersion)
}

func (s *executableDeleteExecutionTaskSuite) newTaskWithVersion(version int64) *ExecutableDeleteExecutionTask {
	return NewExecutableDeleteExecutionTask(
		s.processToolBox,
		s.taskID,
		s.taskCreationTime,
		s.sourceClusterName,
		s.sourceShardKey,
		&replicationspb.ReplicationTask{
			TaskType: enumsspb.REPLICATION_TASK_TYPE_DELETE_EXECUTION_TASK,
			RawTaskInfo: &persistencespb.ReplicationTaskInfo{
				NamespaceId: s.namespaceID.String(),
				WorkflowId:  s.workflowID,
				RunId:       s.runID,
				TaskId:      s.taskID,
				TaskType:    enumsspb.TASK_TYPE_REPLICATION_DELETE_EXECUTION,
				ArchetypeId: chasm.WorkflowArchetypeID,
				Version:     version,
			},
		},
	)
}

func (s *executableDeleteExecutionTaskSuite) namespaceEntry(activeCluster string) *namespace.Namespace {
	return s.namespaceEntryWithFailoverVersion(activeCluster, cluster.TestCurrentClusterInitialFailoverVersion)
}

func (s *executableDeleteExecutionTaskSuite) namespaceEntryWithFailoverVersion(
	activeCluster string,
	failoverVersion int64,
) *namespace.Namespace {
	detail := &persistencespb.NamespaceDetail{
		Info: &persistencespb.NamespaceInfo{
			Id:   s.namespaceID.String(),
			Name: "namespace-name",
		},
		Config: &persistencespb.NamespaceConfig{},
		ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: activeCluster,
			Clusters: []string{
				cluster.TestCurrentClusterName,
				cluster.TestAlternativeClusterName,
			},
		},
		FailoverVersion: failoverVersion,
	}
	namespaceEntry, err := namespace.FromPersistentState(
		detail,
		namespace.NewDefaultReplicationResolverFactory()(detail),
	)
	s.NoError(err)
	return namespaceEntry
}
