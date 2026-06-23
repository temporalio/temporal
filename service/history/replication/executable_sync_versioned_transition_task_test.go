package replication

import (
	"errors"
	"math/rand"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/consts"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
)

type (
	executableSyncVersionedTransitionTaskSuite struct {
		suite.Suite
		*require.Assertions

		controller              *gomock.Controller
		clusterMetadata         *cluster.MockMetadata
		clientBean              *client.MockBean
		shardController         *shard.MockController
		namespaceCache          *namespace.MockRegistry
		metricsHandler          metrics.Handler
		logger                  log.Logger
		executableTask          *MockExecutableTask
		eagerNamespaceRefresher *MockEagerNamespaceRefresher
		mockExecutionManager    *persistence.MockExecutionManager
		config                  *configs.Config

		sourceClusterName string
		sourceShardKey    ClusterShardKey

		taskID           int64
		namespaceID      string
		workflowID       string
		runID            string
		taskCreationTime time.Time
		toolBox          ProcessToolBox
		task             *ExecutableSyncVersionedTransitionTask
	}
)

func TestExecutableSyncVersionedTransitionTaskSuite(t *testing.T) {
	s := new(executableSyncVersionedTransitionTaskSuite)
	suite.Run(t, s)
}

func (s *executableSyncVersionedTransitionTaskSuite) SetupSuite() {
	s.Assertions = require.New(s.T())
}

func (s *executableSyncVersionedTransitionTaskSuite) TearDownSuite() {
}

func (s *executableSyncVersionedTransitionTaskSuite) SetupTest() {
	s.controller = gomock.NewController(s.T())
	s.clusterMetadata = cluster.NewMockMetadata(s.controller)
	s.clientBean = client.NewMockBean(s.controller)
	s.shardController = shard.NewMockController(s.controller)
	s.namespaceCache = namespace.NewMockRegistry(s.controller)
	s.metricsHandler = metrics.NoopMetricsHandler
	s.logger = log.NewNoopLogger()
	s.executableTask = NewMockExecutableTask(s.controller)
	s.eagerNamespaceRefresher = NewMockEagerNamespaceRefresher(s.controller)
	s.mockExecutionManager = persistence.NewMockExecutionManager(s.controller)
	s.config = tests.NewDynamicConfig()

	s.namespaceID = uuid.NewString()
	s.workflowID = uuid.NewString()
	s.runID = uuid.NewString()
	s.taskID = rand.Int63()
	s.sourceClusterName = cluster.TestCurrentClusterName
	s.sourceShardKey = ClusterShardKey{
		ClusterID: int32(cluster.TestCurrentClusterInitialFailoverVersion),
		ShardID:   rand.Int31(),
	}

	s.taskCreationTime = time.Unix(0, rand.Int63())
	s.toolBox = ProcessToolBox{
		ClusterMetadata:         s.clusterMetadata,
		ClientBean:              s.clientBean,
		ShardController:         s.shardController,
		NamespaceCache:          s.namespaceCache,
		MetricsHandler:          s.metricsHandler,
		Logger:                  s.logger,
		EagerNamespaceRefresher: s.eagerNamespaceRefresher,
		DLQWriter:               NewExecutionManagerDLQWriter(s.mockExecutionManager),
		Config:                  s.config,
	}
	s.task = s.newTask(s.newArtifactSnapshot())
	s.task.ExecutableTask = s.executableTask
	s.executableTask.EXPECT().TaskID().Return(s.taskID).AnyTimes()
	s.executableTask.EXPECT().SourceClusterName().Return(s.sourceClusterName).AnyTimes()
	s.executableTask.EXPECT().SourceShardKey().Return(s.sourceShardKey).AnyTimes()
	s.executableTask.EXPECT().TaskCreationTime().Return(s.taskCreationTime).AnyTimes()
	s.executableTask.EXPECT().GetPriority().Return(enumsspb.TASK_PRIORITY_HIGH).AnyTimes()
	s.executableTask.EXPECT().NamespaceName().Return("test-namespace").AnyTimes()
}

func (s *executableSyncVersionedTransitionTaskSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *executableSyncVersionedTransitionTaskSuite) newArtifactSnapshot() *replicationspb.VersionedTransitionArtifact {
	return &replicationspb.VersionedTransitionArtifact{
		StateAttributes: &replicationspb.VersionedTransitionArtifact_SyncWorkflowStateSnapshotAttributes{
			SyncWorkflowStateSnapshotAttributes: &replicationspb.SyncWorkflowStateSnapshotAttributes{
				State: &persistencespb.WorkflowMutableState{
					ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
						VersionHistories: &historyspb.VersionHistories{
							CurrentVersionHistoryIndex: 0,
							Histories: []*historyspb.VersionHistory{
								{
									BranchToken: []byte{1, 2, 3},
									Items: []*historyspb.VersionHistoryItem{
										{EventId: 10, Version: 1},
									},
								},
							},
						},
					},
				},
			},
		},
	}
}

func (s *executableSyncVersionedTransitionTaskSuite) newReplicationTask(
	artifact *replicationspb.VersionedTransitionArtifact,
) *replicationspb.ReplicationTask {
	return &replicationspb.ReplicationTask{
		SourceTaskId: s.taskID,
		Attributes: &replicationspb.ReplicationTask_SyncVersionedTransitionTaskAttributes{
			SyncVersionedTransitionTaskAttributes: &replicationspb.SyncVersionedTransitionTaskAttributes{
				NamespaceId:                 s.namespaceID,
				WorkflowId:                  s.workflowID,
				RunId:                       s.runID,
				ArchetypeId:                 chasm.WorkflowArchetypeID,
				VersionedTransitionArtifact: artifact,
			},
		},
	}
}

func (s *executableSyncVersionedTransitionTaskSuite) newTask(
	artifact *replicationspb.VersionedTransitionArtifact,
) *ExecutableSyncVersionedTransitionTask {
	return NewExecutableSyncVersionedTransitionTask(
		s.toolBox,
		s.taskID,
		s.taskCreationTime,
		s.sourceClusterName,
		s.sourceShardKey,
		s.newReplicationTask(artifact),
	)
}

func (s *executableSyncVersionedTransitionTaskSuite) TestNewTask_DefaultArchetype() {
	replicationTask := &replicationspb.ReplicationTask{
		SourceTaskId: s.taskID,
		Attributes: &replicationspb.ReplicationTask_SyncVersionedTransitionTaskAttributes{
			SyncVersionedTransitionTaskAttributes: &replicationspb.SyncVersionedTransitionTaskAttributes{
				NamespaceId: s.namespaceID,
				WorkflowId:  s.workflowID,
				RunId:       s.runID,
				ArchetypeId: chasm.UnspecifiedArchetypeID,
			},
		},
	}
	task := NewExecutableSyncVersionedTransitionTask(
		s.toolBox,
		s.taskID,
		s.taskCreationTime,
		s.sourceClusterName,
		s.sourceShardKey,
		replicationTask,
	)
	s.Equal(uint32(chasm.WorkflowArchetypeID), task.taskAttr.GetArchetypeId())
	s.Equal(s.task.WorkflowKey, task.QueueID())
}

func (s *executableSyncVersionedTransitionTaskSuite) TestExecute_Skip_TerminalState() {
	s.executableTask.EXPECT().TerminalState().Return(true)
	s.NoError(s.task.Execute())
}

func (s *executableSyncVersionedTransitionTaskSuite) TestExecute_Skip_Namespace() {
	s.executableTask.EXPECT().TerminalState().Return(false)
	s.executableTask.EXPECT().MarkExecutionStart()
	s.executableTask.EXPECT().GetNamespaceInfo(gomock.Any(), s.task.NamespaceID, gomock.Any()).Return(
		uuid.NewString(), false, nil,
	)
	s.NoError(s.task.Execute())
}

func (s *executableSyncVersionedTransitionTaskSuite) TestExecute_NamespaceErr() {
	s.executableTask.EXPECT().TerminalState().Return(false)
	s.executableTask.EXPECT().MarkExecutionStart()
	err := errors.New("namespace error")
	s.executableTask.EXPECT().GetNamespaceInfo(gomock.Any(), s.task.NamespaceID, gomock.Any()).Return(
		"", false, err,
	)
	s.Equal(err, s.task.Execute())
}

func (s *executableSyncVersionedTransitionTaskSuite) TestExecute_Success() {
	s.executableTask.EXPECT().TerminalState().Return(false)
	s.executableTask.EXPECT().MarkExecutionStart()
	s.executableTask.EXPECT().GetNamespaceInfo(gomock.Any(), s.task.NamespaceID, gomock.Any()).Return(
		uuid.NewString(), true, nil,
	)
	shardContext := historyi.NewMockShardContext(s.controller)
	engine := historyi.NewMockEngine(s.controller)
	s.shardController.EXPECT().GetShardByNamespaceWorkflow(
		namespace.ID(s.task.NamespaceID),
		s.task.WorkflowID,
	).Return(shardContext, nil)
	shardContext.EXPECT().GetEngine(gomock.Any()).Return(engine, nil)
	engine.EXPECT().ReplicateVersionedTransition(
		gomock.Any(),
		chasm.WorkflowArchetypeID,
		s.task.taskAttr.VersionedTransitionArtifact,
		s.sourceClusterName,
	).Return(nil)

	s.NoError(s.task.Execute())
}

func (s *executableSyncVersionedTransitionTaskSuite) TestExecute_ShardErr() {
	s.executableTask.EXPECT().TerminalState().Return(false)
	s.executableTask.EXPECT().MarkExecutionStart()
	s.executableTask.EXPECT().GetNamespaceInfo(gomock.Any(), s.task.NamespaceID, gomock.Any()).Return(
		uuid.NewString(), true, nil,
	)
	err := errors.New("shard error")
	s.shardController.EXPECT().GetShardByNamespaceWorkflow(
		namespace.ID(s.task.NamespaceID),
		s.task.WorkflowID,
	).Return(nil, err)
	s.Equal(err, s.task.Execute())
}

func (s *executableSyncVersionedTransitionTaskSuite) TestExecute_EngineErr() {
	s.executableTask.EXPECT().TerminalState().Return(false)
	s.executableTask.EXPECT().MarkExecutionStart()
	s.executableTask.EXPECT().GetNamespaceInfo(gomock.Any(), s.task.NamespaceID, gomock.Any()).Return(
		uuid.NewString(), true, nil,
	)
	shardContext := historyi.NewMockShardContext(s.controller)
	s.shardController.EXPECT().GetShardByNamespaceWorkflow(
		namespace.ID(s.task.NamespaceID),
		s.task.WorkflowID,
	).Return(shardContext, nil)
	err := errors.New("engine error")
	shardContext.EXPECT().GetEngine(gomock.Any()).Return(nil, err)
	s.Equal(err, s.task.Execute())
}

func (s *executableSyncVersionedTransitionTaskSuite) TestHandleErr_Duplicate() {
	s.executableTask.EXPECT().MarkTaskDuplicated().Times(1)
	s.NoError(s.task.HandleErr(consts.ErrDuplicate))
}

func (s *executableSyncVersionedTransitionTaskSuite) TestHandleErr_Default() {
	err := errors.New("random error")
	s.Equal(err, s.task.HandleErr(err))
}

func (s *executableSyncVersionedTransitionTaskSuite) TestHandleErr_SyncState_SyncStateErr() {
	syncStateErr := serviceerrors.NewSyncState("", s.namespaceID, s.workflowID, s.runID, 0, nil, nil)
	s.executableTask.EXPECT().GetNamespaceInfo(gomock.Any(), s.task.NamespaceID, gomock.Any()).Return(
		uuid.NewString(), true, nil,
	)
	s.executableTask.EXPECT().SyncState(gomock.Any(), syncStateErr, ResendAttempt).Return(false, errors.New("sync error"))
	s.Equal(syncStateErr, s.task.HandleErr(syncStateErr))
}

func (s *executableSyncVersionedTransitionTaskSuite) TestHandleErr_SyncState_NamespaceErr() {
	syncStateErr := serviceerrors.NewSyncState("", s.namespaceID, s.workflowID, s.runID, 0, nil, nil)
	s.executableTask.EXPECT().GetNamespaceInfo(gomock.Any(), s.task.NamespaceID, gomock.Any()).Return(
		"", false, errors.New("ns error"),
	)
	s.Equal(syncStateErr, s.task.HandleErr(syncStateErr))
}

func (s *executableSyncVersionedTransitionTaskSuite) TestHandleErr_SyncState_NoContinue() {
	syncStateErr := serviceerrors.NewSyncState("", s.namespaceID, s.workflowID, s.runID, 0, nil, nil)
	s.executableTask.EXPECT().GetNamespaceInfo(gomock.Any(), s.task.NamespaceID, gomock.Any()).Return(
		uuid.NewString(), true, nil,
	)
	s.executableTask.EXPECT().SyncState(gomock.Any(), syncStateErr, ResendAttempt).Return(false, nil)
	s.NoError(s.task.HandleErr(syncStateErr))
}

func (s *executableSyncVersionedTransitionTaskSuite) TestHandleErr_SyncState_Continue() {
	syncStateErr := serviceerrors.NewSyncState("", s.namespaceID, s.workflowID, s.runID, 0, nil, nil)
	s.executableTask.EXPECT().GetNamespaceInfo(gomock.Any(), s.task.NamespaceID, gomock.Any()).Return(
		uuid.NewString(), true, nil,
	).AnyTimes()
	s.executableTask.EXPECT().SyncState(gomock.Any(), syncStateErr, ResendAttempt).Return(true, nil)
	// doContinue == true triggers re-Execute.
	s.executableTask.EXPECT().TerminalState().Return(false)
	s.executableTask.EXPECT().MarkExecutionStart()
	shardContext := historyi.NewMockShardContext(s.controller)
	engine := historyi.NewMockEngine(s.controller)
	s.shardController.EXPECT().GetShardByNamespaceWorkflow(
		namespace.ID(s.task.NamespaceID),
		s.task.WorkflowID,
	).Return(shardContext, nil)
	shardContext.EXPECT().GetEngine(gomock.Any()).Return(engine, nil)
	engine.EXPECT().ReplicateVersionedTransition(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
	s.NoError(s.task.HandleErr(syncStateErr))
}

func (s *executableSyncVersionedTransitionTaskSuite) TestHandleErr_RetryReplication_Snapshot() {
	retryErr := serviceerrors.NewRetryReplication(
		"",
		s.namespaceID,
		s.workflowID,
		s.runID,
		3,
		1,
		5,
		1,
	)
	s.executableTask.EXPECT().GetNamespaceInfo(gomock.Any(), s.task.NamespaceID, gomock.Any()).Return(
		uuid.NewString(), true, nil,
	).AnyTimes()
	s.executableTask.EXPECT().BackFillEvents(
		gomock.Any(),
		s.sourceClusterName,
		s.task.WorkflowKey,
		int64(4),
		int64(1),
		int64(4),
		int64(1),
		"",
	).Return(nil)
	// after backfill, Execute is invoked again.
	s.executableTask.EXPECT().TerminalState().Return(false)
	s.executableTask.EXPECT().MarkExecutionStart()
	shardContext := historyi.NewMockShardContext(s.controller)
	engine := historyi.NewMockEngine(s.controller)
	s.shardController.EXPECT().GetShardByNamespaceWorkflow(
		namespace.ID(s.task.NamespaceID),
		s.task.WorkflowID,
	).Return(shardContext, nil)
	shardContext.EXPECT().GetEngine(gomock.Any()).Return(engine, nil)
	engine.EXPECT().ReplicateVersionedTransition(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)

	s.NoError(s.task.HandleErr(retryErr))
}

func (s *executableSyncVersionedTransitionTaskSuite) TestHandleErr_RetryReplication_Mutation() {
	artifact := &replicationspb.VersionedTransitionArtifact{
		StateAttributes: &replicationspb.VersionedTransitionArtifact_SyncWorkflowStateMutationAttributes{
			SyncWorkflowStateMutationAttributes: &replicationspb.SyncWorkflowStateMutationAttributes{
				StateMutation: &persistencespb.WorkflowMutableStateMutation{
					ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
						VersionHistories: &historyspb.VersionHistories{
							CurrentVersionHistoryIndex: 0,
							Histories: []*historyspb.VersionHistory{
								{
									BranchToken: []byte{1, 2, 3},
									Items:       []*historyspb.VersionHistoryItem{{EventId: 10, Version: 1}},
								},
							},
						},
					},
				},
			},
		},
	}
	task := s.newTask(artifact)
	task.ExecutableTask = s.executableTask
	retryErr := serviceerrors.NewRetryReplication("", s.namespaceID, s.workflowID, s.runID, 3, 1, 5, 1)
	s.executableTask.EXPECT().GetNamespaceInfo(gomock.Any(), s.task.NamespaceID, gomock.Any()).Return(
		uuid.NewString(), true, nil,
	).AnyTimes()
	s.executableTask.EXPECT().BackFillEvents(
		gomock.Any(), s.sourceClusterName, task.WorkflowKey,
		int64(4), int64(1), int64(4), int64(1), "",
	).Return(nil)
	s.executableTask.EXPECT().TerminalState().Return(false)
	s.executableTask.EXPECT().MarkExecutionStart()
	shardContext := historyi.NewMockShardContext(s.controller)
	engine := historyi.NewMockEngine(s.controller)
	s.shardController.EXPECT().GetShardByNamespaceWorkflow(gomock.Any(), gomock.Any()).Return(shardContext, nil)
	shardContext.EXPECT().GetEngine(gomock.Any()).Return(engine, nil)
	engine.EXPECT().ReplicateVersionedTransition(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)

	s.NoError(task.HandleErr(retryErr))
}

func (s *executableSyncVersionedTransitionTaskSuite) TestHandleErr_RetryReplication_VersionLookupErr() {
	// startEvent (StartEventId+1) is beyond the version history, so the event
	// version lookup fails and the original error is returned.
	retryErr := serviceerrors.NewRetryReplication("", s.namespaceID, s.workflowID, s.runID, 100, 1, 200, 1)
	s.executableTask.EXPECT().GetNamespaceInfo(gomock.Any(), s.task.NamespaceID, gomock.Any()).Return(
		uuid.NewString(), true, nil,
	).AnyTimes()
	s.Error(s.task.HandleErr(retryErr))
}

func (s *executableSyncVersionedTransitionTaskSuite) TestHandleErr_RetryReplication_UnknownArtifact() {
	artifact := &replicationspb.VersionedTransitionArtifact{}
	task := s.newTask(artifact)
	task.ExecutableTask = s.executableTask
	retryErr := serviceerrors.NewRetryReplication("", s.namespaceID, s.workflowID, s.runID, 3, 1, 5, 1)
	s.executableTask.EXPECT().GetNamespaceInfo(gomock.Any(), s.task.NamespaceID, gomock.Any()).Return(
		uuid.NewString(), true, nil,
	)
	err := task.HandleErr(retryErr)
	s.IsType(&serviceerror.InvalidArgument{}, err)
}

func (s *executableSyncVersionedTransitionTaskSuite) TestHandleErr_NotFound() {
	notFoundErr := serviceerror.NewNotFound("workflow not found")
	// The cleanup deletion task is created from the original replication task and
	// executed. We drive its (real) Execute() into the namespace-info lookup and
	// fail it there, asserting the error propagates back.
	s.executableTask.EXPECT().ReplicationTask().Return(s.newReplicationTask(s.newArtifactSnapshot())).AnyTimes()
	nsErr := errors.New("namespace lookup error")
	s.namespaceCache.EXPECT().GetNamespaceByID(gomock.Any()).Return(nil, nsErr).AnyTimes()
	s.Equal(nsErr, s.task.HandleErr(notFoundErr))
}
