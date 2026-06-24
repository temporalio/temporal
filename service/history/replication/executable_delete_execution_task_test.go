package replication

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
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
		chasmEngine     *chasm.MockEngine
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
	s.chasmEngine = chasm.NewMockEngine(s.controller)
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
		ChasmEngine:     s.chasmEngine,
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

	s.NoError(s.newTask().Execute())
}

func (s *executableDeleteExecutionTaskSuite) TestQueueID() {
	task := s.newTask()
	s.Equal(
		definition.NewWorkflowKey(s.namespaceID.String(), s.workflowID, s.runID),
		task.QueueID(),
	)
}

func (s *executableDeleteExecutionTaskSuite) TestNewTask_UnspecifiedArchetype_DefaultsToWorkflow() {
	task := NewExecutableDeleteExecutionTask(
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
				ArchetypeId: chasm.UnspecifiedArchetypeID,
			},
		},
	)
	archetypeID, err := task.ArchetypeID(nil)
	s.NoError(err)
	s.Equal(chasm.WorkflowArchetypeID, archetypeID)
}

func (s *executableDeleteExecutionTaskSuite) TestNewTask_NilRawTaskInfo_DefaultsToWorkflow() {
	task := NewExecutableDeleteExecutionTask(
		s.processToolBox,
		s.taskID,
		s.taskCreationTime,
		s.sourceClusterName,
		s.sourceShardKey,
		&replicationspb.ReplicationTask{
			TaskType:    enumsspb.REPLICATION_TASK_TYPE_DELETE_EXECUTION_TASK,
			RawTaskInfo: nil,
		},
	)
	archetypeID, err := task.ArchetypeID(nil)
	s.NoError(err)
	s.Equal(chasm.WorkflowArchetypeID, archetypeID)
}

func (s *executableDeleteExecutionTaskSuite) TestExecute_TerminalState_Skips() {
	task := s.newTask()
	delExecMockTask := NewMockExecutableTask(s.controller)
	delExecMockTask.EXPECT().TerminalState().Return(true)
	task.ExecutableTask = delExecMockTask

	s.NoError(task.Execute())
}

func (s *executableDeleteExecutionTaskSuite) TestExecute_GetNamespaceInfoError_ReturnsError() {
	delExecErr := serviceerror.NewUnavailable("namespace cache unavailable")
	s.namespaceCache.EXPECT().GetNamespaceByID(s.namespaceID).Return(nil, delExecErr)

	err := s.newTask().Execute()
	s.Equal(delExecErr, err)
}

func (s *executableDeleteExecutionTaskSuite) TestExecute_NamespaceNotApply_Skips() {
	namespaceEntry := s.namespaceEntryWithState(cluster.TestAlternativeClusterName, enumspb.NAMESPACE_STATE_DELETED)
	// GetNamespaceInfo calls GetNamespaceByID twice; deleted namespace short-circuits before the cluster check.
	s.namespaceCache.EXPECT().GetNamespaceByID(s.namespaceID).Return(namespaceEntry, nil).Times(2)

	s.NoError(s.newTask().Execute())
}

func (s *executableDeleteExecutionTaskSuite) TestExecute_GetNamespaceByIDErrorInExecute_ReturnsError() {
	namespaceEntry := s.namespaceEntry(cluster.TestAlternativeClusterName)
	delExecErr := serviceerror.NewUnavailable("namespace lookup failed")
	gomock.InOrder(
		s.namespaceCache.EXPECT().GetNamespaceByID(s.namespaceID).Return(namespaceEntry, nil).Times(2),
		s.namespaceCache.EXPECT().GetNamespaceByID(s.namespaceID).Return(nil, delExecErr),
	)

	err := s.newTask().Execute()
	s.Equal(delExecErr, err)
}

func (s *executableDeleteExecutionTaskSuite) TestExecute_DeleteWorkflowExecution_GetShardError() {
	namespaceEntry := s.namespaceEntry(cluster.TestAlternativeClusterName)
	s.namespaceCache.EXPECT().GetNamespaceByID(s.namespaceID).Return(namespaceEntry, nil).Times(3)

	delExecErr := serviceerror.NewUnavailable("shard error")
	s.shardController.EXPECT().GetShardByNamespaceWorkflow(s.namespaceID, s.workflowID).Return(nil, delExecErr)

	err := s.newTask().Execute()
	s.Equal(delExecErr, err)
}

func (s *executableDeleteExecutionTaskSuite) TestExecute_DeleteWorkflowExecution_GetEngineError() {
	namespaceEntry := s.namespaceEntry(cluster.TestAlternativeClusterName)
	s.namespaceCache.EXPECT().GetNamespaceByID(s.namespaceID).Return(namespaceEntry, nil).Times(3)

	shardContext := historyi.NewMockShardContext(s.controller)
	s.shardController.EXPECT().GetShardByNamespaceWorkflow(s.namespaceID, s.workflowID).Return(shardContext, nil)
	delExecErr := serviceerror.NewUnavailable("engine error")
	shardContext.EXPECT().GetEngine(gomock.Any()).Return(nil, delExecErr)

	err := s.newTask().Execute()
	s.Equal(delExecErr, err)
}

func (s *executableDeleteExecutionTaskSuite) TestExecute_DeleteWorkflowExecution_DeleteError() {
	namespaceEntry := s.namespaceEntry(cluster.TestAlternativeClusterName)
	s.namespaceCache.EXPECT().GetNamespaceByID(s.namespaceID).Return(namespaceEntry, nil).Times(3)

	shardContext := historyi.NewMockShardContext(s.controller)
	engine := historyi.NewMockEngine(s.controller)
	s.shardController.EXPECT().GetShardByNamespaceWorkflow(s.namespaceID, s.workflowID).Return(shardContext, nil)
	shardContext.EXPECT().GetEngine(gomock.Any()).Return(engine, nil)
	delExecErr := serviceerror.NewUnavailable("delete failed")
	engine.EXPECT().DeleteWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil, delExecErr)

	err := s.newTask().Execute()
	s.Equal(delExecErr, err)
}

func (s *executableDeleteExecutionTaskSuite) TestExecute_DeletesChasmExecution() {
	namespaceEntry := s.namespaceEntry(cluster.TestAlternativeClusterName)
	s.namespaceCache.EXPECT().GetNamespaceByID(s.namespaceID).Return(namespaceEntry, nil).Times(3)

	task := s.newTaskWithArchetype(chasm.SchedulerArchetypeID)
	s.chasmEngine.EXPECT().DeleteExecution(
		gomock.Any(),
		task.ComponentRef,
		chasm.DeleteExecutionRequest{},
	).Return(nil)

	s.NoError(task.Execute())
}

func (s *executableDeleteExecutionTaskSuite) TestExecute_DeleteChasmExecution_Error() {
	namespaceEntry := s.namespaceEntry(cluster.TestAlternativeClusterName)
	s.namespaceCache.EXPECT().GetNamespaceByID(s.namespaceID).Return(namespaceEntry, nil).Times(3)

	task := s.newTaskWithArchetype(chasm.SchedulerArchetypeID)
	delExecErr := serviceerror.NewUnavailable("chasm delete failed")
	s.chasmEngine.EXPECT().DeleteExecution(gomock.Any(), gomock.Any(), gomock.Any()).Return(delExecErr)

	s.Equal(delExecErr, task.Execute())
}

func (s *executableDeleteExecutionTaskSuite) TestHandleErr_Nil() {
	task := s.newTask()
	delExecMockTask := NewMockExecutableTask(s.controller)
	delExecMockTask.EXPECT().NamespaceName().Return("namespace-name").AnyTimes()
	task.ExecutableTask = delExecMockTask

	s.NoError(task.HandleErr(nil))
}

func (s *executableDeleteExecutionTaskSuite) TestHandleErr_NotFound() {
	task := s.newTask()
	delExecMockTask := NewMockExecutableTask(s.controller)
	delExecMockTask.EXPECT().NamespaceName().Return("namespace-name").AnyTimes()
	task.ExecutableTask = delExecMockTask

	s.NoError(task.HandleErr(serviceerror.NewNotFound("not found")))
}

func (s *executableDeleteExecutionTaskSuite) TestHandleErr_OtherError() {
	task := s.newTask()
	delExecMockTask := NewMockExecutableTask(s.controller)
	delExecMockTask.EXPECT().NamespaceName().Return("namespace-name").AnyTimes()
	delExecMockTask.EXPECT().TaskID().Return(s.taskID).AnyTimes()
	task.ExecutableTask = delExecMockTask

	delExecErr := serviceerror.NewUnavailable("boom")
	err := task.HandleErr(delExecErr)
	s.Error(err)
	s.ErrorIs(err, delExecErr)
	s.Equal(fmt.Errorf("delete execution replication task error: %w", delExecErr).Error(), err.Error())
}

func (s *executableDeleteExecutionTaskSuite) TestMarkPoisonPill_PopulatesRawTaskInfo() {
	// Construct with RawTaskInfo so the task's NamespaceID/BusinessID/RunID are populated
	// from the ComponentRef, then clear RawTaskInfo to exercise the repopulation branch.
	replicationTask := &replicationspb.ReplicationTask{
		TaskType: enumsspb.REPLICATION_TASK_TYPE_DELETE_EXECUTION_TASK,
		RawTaskInfo: &persistencespb.ReplicationTaskInfo{
			NamespaceId: s.namespaceID.String(),
			WorkflowId:  s.workflowID,
			RunId:       s.runID,
			TaskId:      s.taskID,
			ArchetypeId: chasm.WorkflowArchetypeID,
		},
	}
	task := NewExecutableDeleteExecutionTask(
		s.processToolBox,
		s.taskID,
		s.taskCreationTime,
		s.sourceClusterName,
		s.sourceShardKey,
		replicationTask,
	)
	replicationTask.RawTaskInfo = nil
	delExecMockTask := NewMockExecutableTask(s.controller)
	delExecMockTask.EXPECT().ReplicationTask().Return(replicationTask).AnyTimes()
	delExecMockTask.EXPECT().TaskID().Return(s.taskID).AnyTimes()
	delExecMockTask.EXPECT().MarkPoisonPill().Return(nil).Times(1)
	task.ExecutableTask = delExecMockTask

	s.NoError(task.MarkPoisonPill())
	s.NotNil(replicationTask.GetRawTaskInfo())
	s.Equal(s.namespaceID.String(), replicationTask.GetRawTaskInfo().NamespaceId)
	s.Equal(s.workflowID, replicationTask.GetRawTaskInfo().WorkflowId)
	s.Equal(s.runID, replicationTask.GetRawTaskInfo().RunId)
	s.Equal(enumsspb.TASK_TYPE_REPLICATION_DELETE_EXECUTION, replicationTask.GetRawTaskInfo().TaskType)
}

func (s *executableDeleteExecutionTaskSuite) TestMarkPoisonPill_RawTaskInfoAlreadyPresent() {
	rawInfo := &persistencespb.ReplicationTaskInfo{
		NamespaceId: s.namespaceID.String(),
		WorkflowId:  s.workflowID,
		RunId:       s.runID,
		TaskId:      s.taskID,
		ArchetypeId: chasm.WorkflowArchetypeID,
	}
	replicationTask := &replicationspb.ReplicationTask{
		TaskType:    enumsspb.REPLICATION_TASK_TYPE_DELETE_EXECUTION_TASK,
		RawTaskInfo: rawInfo,
	}
	task := NewExecutableDeleteExecutionTask(
		s.processToolBox,
		s.taskID,
		s.taskCreationTime,
		s.sourceClusterName,
		s.sourceShardKey,
		replicationTask,
	)
	delExecMockTask := NewMockExecutableTask(s.controller)
	delExecMockTask.EXPECT().ReplicationTask().Return(replicationTask).AnyTimes()
	delExecMockTask.EXPECT().MarkPoisonPill().Return(nil).Times(1)
	task.ExecutableTask = delExecMockTask

	s.NoError(task.MarkPoisonPill())
	// Existing raw task info should be preserved (not overwritten).
	s.Same(rawInfo, replicationTask.GetRawTaskInfo())
}

func (s *executableDeleteExecutionTaskSuite) newTaskWithArchetype(archetypeID chasm.ArchetypeID) *ExecutableDeleteExecutionTask {
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
				ArchetypeId: archetypeID,
			},
		},
	)
}

func (s *executableDeleteExecutionTaskSuite) namespaceEntryWithState(activeCluster string, state enumspb.NamespaceState) *namespace.Namespace {
	detail := &persistencespb.NamespaceDetail{
		Info: &persistencespb.NamespaceInfo{
			Id:    s.namespaceID.String(),
			Name:  "namespace-name",
			State: state,
		},
		Config: &persistencespb.NamespaceConfig{},
		ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: activeCluster,
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
	return namespaceEntry
}

func (s *executableDeleteExecutionTaskSuite) newTask() *ExecutableDeleteExecutionTask {
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
				ArchetypeId: chasm.WorkflowArchetypeID,
			},
		},
	)
}

func (s *executableDeleteExecutionTaskSuite) namespaceEntry(activeCluster string) *namespace.Namespace {
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
		FailoverVersion: cluster.TestCurrentClusterInitialFailoverVersion,
	}
	namespaceEntry, err := namespace.FromPersistentState(
		detail,
		namespace.NewDefaultReplicationResolverFactory()(detail),
	)
	s.NoError(err)
	return namespaceEntry
}
