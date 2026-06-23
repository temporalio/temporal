package replication

import (
	"errors"
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
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/client"
	common2 "go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/consts"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/workflow/cache"
	"go.uber.org/mock/gomock"
)

type (
	executableVerifyVersionedTransitionTaskSuite struct {
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
		wfcache                 *cache.MockCache
		eventSerializer         serialization.Serializer
		mockExecutionManager    *persistence.MockExecutionManager
		config                  *configs.Config
		sourceClusterName       string
		sourceShardKey          ClusterShardKey

		taskID      int64
		namespaceID string
		workflowID  string
		runID       string
		task        *ExecutableVerifyVersionedTransitionTask
		newRunID    string
		toolBox     ProcessToolBox
	}
)

func TestExecutableVerifyVersionedTransitionTaskSuite(t *testing.T) {
	s := new(executableVerifyVersionedTransitionTaskSuite)
	suite.Run(t, s)
}

func (s *executableVerifyVersionedTransitionTaskSuite) SetupSuite() {
	s.Assertions = require.New(s.T())
}

func (s *executableVerifyVersionedTransitionTaskSuite) TearDownSuite() {

}

func (s *executableVerifyVersionedTransitionTaskSuite) SetupTest() {
	s.controller = gomock.NewController(s.T())
	s.clusterMetadata = cluster.NewMockMetadata(s.controller)
	s.clientBean = client.NewMockBean(s.controller)
	s.shardController = shard.NewMockController(s.controller)
	s.namespaceCache = namespace.NewMockRegistry(s.controller)
	s.metricsHandler = metrics.NoopMetricsHandler
	s.logger = log.NewNoopLogger()
	s.executableTask = NewMockExecutableTask(s.controller)
	s.eventSerializer = serialization.NewSerializer()
	s.eagerNamespaceRefresher = NewMockEagerNamespaceRefresher(s.controller)
	s.wfcache = cache.NewMockCache(s.controller)
	s.namespaceID = uuid.NewString()
	s.workflowID = uuid.NewString()
	s.runID = "old_run"
	s.newRunID = "new_run"

	s.taskID = rand.Int63()

	s.sourceClusterName = cluster.TestCurrentClusterName
	s.sourceShardKey = ClusterShardKey{
		ClusterID: int32(cluster.TestCurrentClusterInitialFailoverVersion),
		ShardID:   rand.Int31(),
	}
	s.mockExecutionManager = persistence.NewMockExecutionManager(s.controller)
	s.config = tests.NewDynamicConfig()

	taskCreationTime := time.Unix(0, rand.Int63())
	s.toolBox = ProcessToolBox{
		ClusterMetadata:         s.clusterMetadata,
		ClientBean:              s.clientBean,
		ShardController:         s.shardController,
		NamespaceCache:          s.namespaceCache,
		MetricsHandler:          s.metricsHandler,
		Logger:                  s.logger,
		Serializer:              s.eventSerializer,
		EagerNamespaceRefresher: s.eagerNamespaceRefresher,
		DLQWriter:               NewExecutionManagerDLQWriter(s.mockExecutionManager),
		Config:                  s.config,
		WorkflowCache:           s.wfcache,
	}
	replicationTask := &replicationspb.ReplicationTask{
		TaskType:     enumsspb.REPLICATION_TASK_TYPE_VERIFY_VERSIONED_TRANSITION_TASK,
		SourceTaskId: s.taskID,
		Attributes: &replicationspb.ReplicationTask_VerifyVersionedTransitionTaskAttributes{
			VerifyVersionedTransitionTaskAttributes: &replicationspb.VerifyVersionedTransitionTaskAttributes{
				NamespaceId: s.namespaceID,
				WorkflowId:  s.workflowID,
				RunId:       s.runID,
				ArchetypeId: chasm.WorkflowArchetypeID,
			},
		},
		VersionedTransition: &persistencespb.VersionedTransition{
			NamespaceFailoverVersion: 3,
			TransitionCount:          5,
		},
	}
	s.task = NewExecutableVerifyVersionedTransitionTask(
		s.toolBox,
		s.taskID,
		taskCreationTime,
		s.sourceClusterName,
		s.sourceShardKey,
		replicationTask,
	)
	s.task.ExecutableTask = s.executableTask
	s.executableTask.EXPECT().TaskID().Return(s.taskID).AnyTimes()
	s.executableTask.EXPECT().SourceClusterName().Return(s.sourceClusterName).AnyTimes()
	s.executableTask.EXPECT().TaskCreationTime().Return(taskCreationTime).AnyTimes()
	s.executableTask.EXPECT().GetPriority().Return(enumsspb.TASK_PRIORITY_HIGH).AnyTimes()
}

func (s *executableVerifyVersionedTransitionTaskSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *executableVerifyVersionedTransitionTaskSuite) TestExecute_CurrentBranch_VerifySuccess() {
	taskNextEvent := int64(10)
	replicationTask := &replicationspb.ReplicationTask{
		TaskType:     enumsspb.REPLICATION_TASK_TYPE_VERIFY_VERSIONED_TRANSITION_TASK,
		SourceTaskId: s.taskID,
		Attributes: &replicationspb.ReplicationTask_VerifyVersionedTransitionTaskAttributes{
			VerifyVersionedTransitionTaskAttributes: &replicationspb.VerifyVersionedTransitionTaskAttributes{
				NamespaceId: s.namespaceID,
				WorkflowId:  s.workflowID,
				RunId:       s.runID,
				NextEventId: taskNextEvent,
				NewRunId:    s.newRunID,
				ArchetypeId: chasm.WorkflowArchetypeID,
			},
		},
		VersionedTransition: &persistencespb.VersionedTransition{
			NamespaceFailoverVersion: 3,
			TransitionCount:          5,
		},
	}
	s.executableTask.EXPECT().TerminalState().Return(false)
	s.executableTask.EXPECT().MarkExecutionStart()
	s.executableTask.EXPECT().ReplicationTask().Times(1).Return(replicationTask)
	s.executableTask.EXPECT().GetNamespaceInfo(gomock.Any(), s.task.NamespaceID, gomock.Any()).Return(
		uuid.NewString(), true, nil,
	).AnyTimes()
	mu := historyi.NewMockMutableState(s.controller)
	mu.EXPECT().CloneToProto().Return(
		&persistencespb.WorkflowMutableState{
			ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
				TransitionHistory: []*persistencespb.VersionedTransition{
					{NamespaceFailoverVersion: 1, TransitionCount: 3},
					{NamespaceFailoverVersion: 3, TransitionCount: 6},
				},
			},
			NextEventId: taskNextEvent,
		},
	).AnyTimes()

	s.mockGetMutableState(s.namespaceID, s.workflowID, s.runID, mu, nil)
	newRunMs := historyi.NewMockMutableState(s.controller)
	newRunMs.EXPECT().CloneToProto().Return(&persistencespb.WorkflowMutableState{}).AnyTimes()
	s.mockGetMutableState(s.namespaceID, s.workflowID, s.newRunID, newRunMs, nil)

	task := NewExecutableVerifyVersionedTransitionTask(
		s.toolBox,
		s.taskID,
		time.Now(),
		s.sourceClusterName,
		s.sourceShardKey,
		replicationTask,
	)
	task.ExecutableTask = s.executableTask

	err := task.Execute()
	s.NoError(err)
}

func (s *executableVerifyVersionedTransitionTaskSuite) TestExecute_CurrentBranch_NewRunNotFound() {
	taskNextEvent := int64(10)
	replicationTask := &replicationspb.ReplicationTask{
		TaskType:     enumsspb.REPLICATION_TASK_TYPE_VERIFY_VERSIONED_TRANSITION_TASK,
		SourceTaskId: s.taskID,
		Attributes: &replicationspb.ReplicationTask_VerifyVersionedTransitionTaskAttributes{
			VerifyVersionedTransitionTaskAttributes: &replicationspb.VerifyVersionedTransitionTaskAttributes{
				NamespaceId: s.namespaceID,
				WorkflowId:  s.workflowID,
				RunId:       s.runID,
				NextEventId: taskNextEvent,
				NewRunId:    s.newRunID,
				ArchetypeId: chasm.WorkflowArchetypeID,
			},
		},
		VersionedTransition: &persistencespb.VersionedTransition{
			NamespaceFailoverVersion: 3,
			TransitionCount:          5,
		},
	}
	s.executableTask.EXPECT().TerminalState().Return(false)
	s.executableTask.EXPECT().MarkExecutionStart()
	s.executableTask.EXPECT().ReplicationTask().Times(1).Return(replicationTask).AnyTimes()
	s.executableTask.EXPECT().GetNamespaceInfo(gomock.Any(), s.task.NamespaceID, gomock.Any()).Return(
		uuid.NewString(), true, nil,
	).AnyTimes()

	mu := historyi.NewMockMutableState(s.controller)
	mu.EXPECT().CloneToProto().Return(
		&persistencespb.WorkflowMutableState{
			ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
				TransitionHistory: []*persistencespb.VersionedTransition{
					{NamespaceFailoverVersion: 1, TransitionCount: 3},
					{NamespaceFailoverVersion: 3, TransitionCount: 6},
				},
			},
			NextEventId: taskNextEvent,
		},
	).AnyTimes()

	s.mockGetMutableState(s.namespaceID, s.workflowID, s.runID, mu, nil)
	s.mockGetMutableState(s.namespaceID, s.workflowID, s.newRunID, nil, serviceerror.NewNotFound("workflow not found"))
	task := NewExecutableVerifyVersionedTransitionTask(
		s.toolBox,
		s.taskID,
		time.Now(),
		s.sourceClusterName,
		s.sourceShardKey,
		replicationTask,
	)
	task.ExecutableTask = s.executableTask

	err := task.Execute()
	s.ErrorAs(err, new(*serviceerror.DataLoss))
}

func (s *executableVerifyVersionedTransitionTaskSuite) mockGetMutableState(
	namespaceId string,
	workflowId string,
	runId string,
	mutableState historyi.MutableState,
	err error,
) {
	shardContext := historyi.NewMockShardContext(s.controller)
	s.shardController.EXPECT().GetShardByNamespaceWorkflow(
		namespace.ID(s.task.NamespaceID),
		s.task.WorkflowID,
	).Return(shardContext, nil)
	wfCtx := historyi.NewMockWorkflowContext(s.controller)
	if err == nil {
		wfCtx.EXPECT().LoadMutableState(gomock.Any(), shardContext).Return(mutableState, err)
	}
	s.wfcache.EXPECT().GetOrCreateChasmExecution(
		gomock.Any(),
		shardContext,
		namespace.ID(namespaceId),
		&commonpb.WorkflowExecution{
			WorkflowId: workflowId,
			RunId:      runId,
		},
		chasm.WorkflowArchetypeID,
		locks.PriorityHigh,
	).Return(wfCtx, func(err error) {}, err)
}

func (s *executableVerifyVersionedTransitionTaskSuite) TestExecute_CurrentBranch_NotUpToDate() {
	taskNextEvent := int64(10)
	replicationTask := &replicationspb.ReplicationTask{
		TaskType:     enumsspb.REPLICATION_TASK_TYPE_VERIFY_VERSIONED_TRANSITION_TASK,
		SourceTaskId: s.taskID,
		Attributes: &replicationspb.ReplicationTask_VerifyVersionedTransitionTaskAttributes{
			VerifyVersionedTransitionTaskAttributes: &replicationspb.VerifyVersionedTransitionTaskAttributes{
				NamespaceId: s.namespaceID,
				WorkflowId:  s.workflowID,
				RunId:       s.runID,
				NextEventId: taskNextEvent,
				NewRunId:    s.newRunID,
				ArchetypeId: chasm.WorkflowArchetypeID,
			},
		},
		VersionedTransition: &persistencespb.VersionedTransition{
			NamespaceFailoverVersion: 3,
			TransitionCount:          7,
		},
	}
	s.executableTask.EXPECT().TerminalState().Return(false)
	s.executableTask.EXPECT().MarkExecutionStart()
	s.executableTask.EXPECT().ReplicationTask().Return(replicationTask).AnyTimes()
	s.executableTask.EXPECT().GetNamespaceInfo(gomock.Any(), s.task.NamespaceID, gomock.Any()).Return(
		uuid.NewString(), true, nil,
	).AnyTimes()

	mu := historyi.NewMockMutableState(s.controller)
	transitionHistory := []*persistencespb.VersionedTransition{
		{NamespaceFailoverVersion: 1, TransitionCount: 3},
		{NamespaceFailoverVersion: 3, TransitionCount: 6},
	}
	mu.EXPECT().CloneToProto().Return(
		&persistencespb.WorkflowMutableState{
			ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
				TransitionHistory: transitionHistory,
			},
			NextEventId: taskNextEvent,
		},
	).AnyTimes()

	s.mockGetMutableState(s.namespaceID, s.workflowID, s.runID, mu, nil)

	task := NewExecutableVerifyVersionedTransitionTask(
		s.toolBox,
		s.taskID,
		time.Now(),
		s.sourceClusterName,
		s.sourceShardKey,
		replicationTask,
	)
	task.ExecutableTask = s.executableTask

	err := task.Execute()
	s.ErrorAs(err, new(*serviceerrors.SyncState))
	s.Equal(transitionHistory[1], err.(*serviceerrors.SyncState).VersionedTransition)
}

func (s *executableVerifyVersionedTransitionTaskSuite) TestExecute_MissingVersionedTransition() {
	taskNextEvent := int64(10)
	replicationTask := &replicationspb.ReplicationTask{
		TaskType:     enumsspb.REPLICATION_TASK_TYPE_VERIFY_VERSIONED_TRANSITION_TASK,
		SourceTaskId: s.taskID,
		Attributes: &replicationspb.ReplicationTask_VerifyVersionedTransitionTaskAttributes{
			VerifyVersionedTransitionTaskAttributes: &replicationspb.VerifyVersionedTransitionTaskAttributes{
				NamespaceId: s.namespaceID,
				WorkflowId:  s.workflowID,
				RunId:       s.runID,
				NextEventId: taskNextEvent,
				NewRunId:    s.newRunID,
				ArchetypeId: chasm.WorkflowArchetypeID,
			},
		},
		VersionedTransition: &persistencespb.VersionedTransition{
			NamespaceFailoverVersion: 3,
			TransitionCount:          7,
		},
	}
	s.executableTask.EXPECT().TerminalState().Return(false)
	s.executableTask.EXPECT().MarkExecutionStart()
	s.executableTask.EXPECT().ReplicationTask().Return(replicationTask).AnyTimes()
	s.executableTask.EXPECT().GetNamespaceInfo(gomock.Any(), s.task.NamespaceID, gomock.Any()).Return(
		uuid.NewString(), true, nil,
	).AnyTimes()

	mu := historyi.NewMockMutableState(s.controller)
	mu.EXPECT().CloneToProto().Return(
		&persistencespb.WorkflowMutableState{
			ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
				TransitionHistory: nil,
			},
			NextEventId: taskNextEvent,
		},
	).AnyTimes()

	s.mockGetMutableState(s.namespaceID, s.workflowID, s.runID, mu, nil)

	task := NewExecutableVerifyVersionedTransitionTask(
		s.toolBox,
		s.taskID,
		time.Now(),
		s.sourceClusterName,
		s.sourceShardKey,
		replicationTask,
	)
	task.ExecutableTask = s.executableTask

	err := task.Execute()
	s.ErrorAs(err, new(*serviceerrors.SyncState))
	var expected *persistencespb.VersionedTransition
	s.Equal(expected, err.(*serviceerrors.SyncState).VersionedTransition)
}

func (s *executableVerifyVersionedTransitionTaskSuite) TestExecute_NonCurrentBranch_VerifySuccess() {
	taskNextEvent := int64(10)
	replicationTask := &replicationspb.ReplicationTask{
		TaskType:     enumsspb.REPLICATION_TASK_TYPE_VERIFY_VERSIONED_TRANSITION_TASK,
		SourceTaskId: s.taskID,
		Attributes: &replicationspb.ReplicationTask_VerifyVersionedTransitionTaskAttributes{
			VerifyVersionedTransitionTaskAttributes: &replicationspb.VerifyVersionedTransitionTaskAttributes{
				NamespaceId: s.namespaceID,
				WorkflowId:  s.workflowID,
				RunId:       s.runID,
				NextEventId: taskNextEvent,
				NewRunId:    s.newRunID,
				EventVersionHistory: []*historyspb.VersionHistoryItem{
					{
						EventId: 9,
						Version: 1,
					},
				},
				ArchetypeId: chasm.WorkflowArchetypeID,
			},
		},
		VersionedTransition: &persistencespb.VersionedTransition{
			NamespaceFailoverVersion: 1,
			TransitionCount:          4,
		},
	}
	s.executableTask.EXPECT().TerminalState().Return(false)
	s.executableTask.EXPECT().MarkExecutionStart()
	s.executableTask.EXPECT().ReplicationTask().Return(replicationTask).AnyTimes()
	s.executableTask.EXPECT().GetNamespaceInfo(gomock.Any(), s.task.NamespaceID, gomock.Any()).Return(
		uuid.NewString(), true, nil,
	).AnyTimes()

	mu := historyi.NewMockMutableState(s.controller)
	mu.EXPECT().CloneToProto().Return(
		&persistencespb.WorkflowMutableState{
			ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
				TransitionHistory: []*persistencespb.VersionedTransition{
					{NamespaceFailoverVersion: 1, TransitionCount: 3},
					{NamespaceFailoverVersion: 3, TransitionCount: 6},
				},
				VersionHistories: &historyspb.VersionHistories{
					Histories: []*historyspb.VersionHistory{
						{
							BranchToken: []byte{1, 2, 3},
							Items: []*historyspb.VersionHistoryItem{
								{
									EventId: 5,
									Version: 1,
								},
								{
									EventId: 10,
									Version: 3,
								},
							},
						},
						{
							BranchToken: []byte{1, 2, 3, 4},
							Items: []*historyspb.VersionHistoryItem{
								{
									EventId: 10,
									Version: 1,
								},
							},
						},
					},
				},
			},
			NextEventId: taskNextEvent,
		},
	).AnyTimes()

	s.mockGetMutableState(s.namespaceID, s.workflowID, s.runID, mu, nil)
	newRunMs := historyi.NewMockMutableState(s.controller)
	newRunMs.EXPECT().CloneToProto().Return(&persistencespb.WorkflowMutableState{}).AnyTimes()
	s.mockGetMutableState(s.namespaceID, s.workflowID, s.newRunID, newRunMs, nil)

	task := NewExecutableVerifyVersionedTransitionTask(
		s.toolBox,
		s.taskID,
		time.Now(),
		s.sourceClusterName,
		s.sourceShardKey,
		replicationTask,
	)
	task.ExecutableTask = s.executableTask

	err := task.Execute()
	s.NoError(err)
}

func (s *executableVerifyVersionedTransitionTaskSuite) TestExecute_NonCurrentBranch_NotUpToDate() {
	taskNextEvent := int64(10)
	replicationTask := &replicationspb.ReplicationTask{
		TaskType:     enumsspb.REPLICATION_TASK_TYPE_VERIFY_VERSIONED_TRANSITION_TASK,
		SourceTaskId: s.taskID,
		Attributes: &replicationspb.ReplicationTask_VerifyVersionedTransitionTaskAttributes{
			VerifyVersionedTransitionTaskAttributes: &replicationspb.VerifyVersionedTransitionTaskAttributes{
				NamespaceId: s.namespaceID,
				WorkflowId:  s.workflowID,
				RunId:       s.runID,
				NextEventId: taskNextEvent,
				NewRunId:    s.newRunID,
				EventVersionHistory: []*historyspb.VersionHistoryItem{
					{
						EventId: 9,
						Version: 1,
					},
				},
				ArchetypeId: chasm.WorkflowArchetypeID,
			},
		},
		VersionedTransition: &persistencespb.VersionedTransition{
			NamespaceFailoverVersion: 1,
			TransitionCount:          4,
		},
	}
	s.executableTask.EXPECT().TerminalState().Return(false)
	s.executableTask.EXPECT().MarkExecutionStart()
	s.executableTask.EXPECT().ReplicationTask().Return(replicationTask).AnyTimes()
	s.executableTask.EXPECT().GetNamespaceInfo(gomock.Any(), s.task.NamespaceID, gomock.Any()).Return(
		uuid.NewString(), true, nil,
	).AnyTimes()

	mu := historyi.NewMockMutableState(s.controller)
	mu.EXPECT().CloneToProto().Return(
		&persistencespb.WorkflowMutableState{
			ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
				TransitionHistory: []*persistencespb.VersionedTransition{
					{NamespaceFailoverVersion: 1, TransitionCount: 3},
					{NamespaceFailoverVersion: 3, TransitionCount: 6},
				},
				VersionHistories: &historyspb.VersionHistories{
					Histories: []*historyspb.VersionHistory{
						{
							BranchToken: []byte{1, 2, 3},
							Items: []*historyspb.VersionHistoryItem{
								{
									EventId: 8,
									Version: 1,
								},
							},
						},
					},
				},
			},
			NextEventId: taskNextEvent,
		},
	).AnyTimes()

	s.mockGetMutableState(s.namespaceID, s.workflowID, s.runID, mu, nil)

	task := NewExecutableVerifyVersionedTransitionTask(
		s.toolBox,
		s.taskID,
		time.Now(),
		s.sourceClusterName,
		s.sourceShardKey,
		replicationTask,
	)
	task.ExecutableTask = s.executableTask
	s.executableTask.EXPECT().BackFillEvents(
		gomock.Any(),
		s.sourceClusterName,
		s.task.WorkflowKey,
		int64(9),
		int64(1),
		int64(9),
		int64(1),
		s.newRunID,
	).Return(nil)

	err := task.Execute()
	s.NoError(err)
}

func (s *executableVerifyVersionedTransitionTaskSuite) TestExecute_Skip_TerminalState() {
	s.executableTask.EXPECT().TerminalState().Return(true)

	err := s.task.Execute()
	s.NoError(err)
}

func (s *executableVerifyVersionedTransitionTaskSuite) TestExecute_Skip_Namespace() {
	s.executableTask.EXPECT().TerminalState().Return(false)
	s.executableTask.EXPECT().MarkExecutionStart()
	s.executableTask.EXPECT().GetNamespaceInfo(gomock.Any(), s.task.NamespaceID, gomock.Any()).Return(
		uuid.NewString(), false, nil,
	).AnyTimes()

	err := s.task.Execute()
	s.NoError(err)
}

func (s *executableVerifyVersionedTransitionTaskSuite) TestExecute_Err() {
	s.executableTask.EXPECT().TerminalState().Return(false)
	s.executableTask.EXPECT().MarkExecutionStart()
	err := errors.New("OwO")
	s.executableTask.EXPECT().GetNamespaceInfo(gomock.Any(), s.task.NamespaceID, gomock.Any()).Return(
		"", false, err,
	).AnyTimes()

	s.Equal(err, s.task.Execute())
}

func (s *executableVerifyVersionedTransitionTaskSuite) TestQueueID() {
	s.Equal(s.task.WorkflowKey, s.task.QueueID())
}

func (s *executableVerifyVersionedTransitionTaskSuite) TestNew_DefaultsArchetypeID() {
	replicationTask := &replicationspb.ReplicationTask{
		TaskType:     enumsspb.REPLICATION_TASK_TYPE_VERIFY_VERSIONED_TRANSITION_TASK,
		SourceTaskId: s.taskID,
		Attributes: &replicationspb.ReplicationTask_VerifyVersionedTransitionTaskAttributes{
			VerifyVersionedTransitionTaskAttributes: &replicationspb.VerifyVersionedTransitionTaskAttributes{
				NamespaceId: s.namespaceID,
				WorkflowId:  s.workflowID,
				RunId:       s.runID,
				ArchetypeId: chasm.UnspecifiedArchetypeID,
			},
		},
	}
	task := NewExecutableVerifyVersionedTransitionTask(
		s.toolBox,
		s.taskID,
		time.Now(),
		s.sourceClusterName,
		s.sourceShardKey,
		replicationTask,
	)
	s.Equal(chasm.WorkflowArchetypeID, task.taskAttr.GetArchetypeId())
	s.Equal(definition.NewWorkflowKey(s.namespaceID, s.workflowID, s.runID), task.WorkflowKey)
}

// Execute case 3: state transition on non-current branch, but no event to verify (NextEventId == EmptyEventID).
func (s *executableVerifyVersionedTransitionTaskSuite) TestExecute_NonCurrentBranch_NoEventToVerify() {
	replicationTask := &replicationspb.ReplicationTask{
		TaskType:     enumsspb.REPLICATION_TASK_TYPE_VERIFY_VERSIONED_TRANSITION_TASK,
		SourceTaskId: s.taskID,
		Attributes: &replicationspb.ReplicationTask_VerifyVersionedTransitionTaskAttributes{
			VerifyVersionedTransitionTaskAttributes: &replicationspb.VerifyVersionedTransitionTaskAttributes{
				NamespaceId: s.namespaceID,
				WorkflowId:  s.workflowID,
				RunId:       s.runID,
				NextEventId: common2.EmptyEventID,
				ArchetypeId: chasm.WorkflowArchetypeID,
			},
		},
		VersionedTransition: &persistencespb.VersionedTransition{
			NamespaceFailoverVersion: 1,
			TransitionCount:          4,
		},
	}
	s.executableTask.EXPECT().TerminalState().Return(false)
	s.executableTask.EXPECT().MarkExecutionStart()
	s.executableTask.EXPECT().ReplicationTask().Return(replicationTask).AnyTimes()
	s.executableTask.EXPECT().GetNamespaceInfo(gomock.Any(), s.task.NamespaceID, gomock.Any()).Return(
		uuid.NewString(), true, nil,
	).AnyTimes()

	mu := historyi.NewMockMutableState(s.controller)
	mu.EXPECT().CloneToProto().Return(
		&persistencespb.WorkflowMutableState{
			ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
				TransitionHistory: []*persistencespb.VersionedTransition{
					{NamespaceFailoverVersion: 1, TransitionCount: 3},
					{NamespaceFailoverVersion: 3, TransitionCount: 6},
				},
			},
		},
	).AnyTimes()

	s.mockGetMutableState(s.namespaceID, s.workflowID, s.runID, mu, nil)

	task := NewExecutableVerifyVersionedTransitionTask(
		s.toolBox,
		s.taskID,
		time.Now(),
		s.sourceClusterName,
		s.sourceShardKey,
		replicationTask,
	)
	task.ExecutableTask = s.executableTask

	// no NewRunId, so verifyNewRunExist short-circuits to nil
	err := task.Execute()
	s.NoError(err)
}

// Execute case 4 with empty EventVersionHistory: no events to verify, returns nil.
func (s *executableVerifyVersionedTransitionTaskSuite) TestExecute_NonCurrentBranch_EmptyEventVersionHistory() {
	taskNextEvent := int64(10)
	replicationTask := &replicationspb.ReplicationTask{
		TaskType:     enumsspb.REPLICATION_TASK_TYPE_VERIFY_VERSIONED_TRANSITION_TASK,
		SourceTaskId: s.taskID,
		Attributes: &replicationspb.ReplicationTask_VerifyVersionedTransitionTaskAttributes{
			VerifyVersionedTransitionTaskAttributes: &replicationspb.VerifyVersionedTransitionTaskAttributes{
				NamespaceId: s.namespaceID,
				WorkflowId:  s.workflowID,
				RunId:       s.runID,
				NextEventId: taskNextEvent,
				ArchetypeId: chasm.WorkflowArchetypeID,
			},
		},
		VersionedTransition: &persistencespb.VersionedTransition{
			NamespaceFailoverVersion: 1,
			TransitionCount:          4,
		},
	}
	s.executableTask.EXPECT().TerminalState().Return(false)
	s.executableTask.EXPECT().MarkExecutionStart()
	s.executableTask.EXPECT().ReplicationTask().Return(replicationTask).AnyTimes()
	s.executableTask.EXPECT().GetNamespaceInfo(gomock.Any(), s.task.NamespaceID, gomock.Any()).Return(
		uuid.NewString(), true, nil,
	).AnyTimes()

	mu := historyi.NewMockMutableState(s.controller)
	mu.EXPECT().CloneToProto().Return(
		&persistencespb.WorkflowMutableState{
			ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
				TransitionHistory: []*persistencespb.VersionedTransition{
					{NamespaceFailoverVersion: 1, TransitionCount: 3},
					{NamespaceFailoverVersion: 3, TransitionCount: 6},
				},
			},
			NextEventId: taskNextEvent,
		},
	).AnyTimes()

	s.mockGetMutableState(s.namespaceID, s.workflowID, s.runID, mu, nil)

	task := NewExecutableVerifyVersionedTransitionTask(
		s.toolBox,
		s.taskID,
		time.Now(),
		s.sourceClusterName,
		s.sourceShardKey,
		replicationTask,
	)
	task.ExecutableTask = s.executableTask

	err := task.Execute()
	s.NoError(err)
}

// Execute: getMutableState returns NotFound -> SyncState resend.
func (s *executableVerifyVersionedTransitionTaskSuite) TestExecute_MutableStateNotFound() {
	replicationTask := &replicationspb.ReplicationTask{
		TaskType:     enumsspb.REPLICATION_TASK_TYPE_VERIFY_VERSIONED_TRANSITION_TASK,
		SourceTaskId: s.taskID,
		Attributes: &replicationspb.ReplicationTask_VerifyVersionedTransitionTaskAttributes{
			VerifyVersionedTransitionTaskAttributes: &replicationspb.VerifyVersionedTransitionTaskAttributes{
				NamespaceId: s.namespaceID,
				WorkflowId:  s.workflowID,
				RunId:       s.runID,
				ArchetypeId: chasm.WorkflowArchetypeID,
			},
		},
		VersionedTransition: &persistencespb.VersionedTransition{
			NamespaceFailoverVersion: 3,
			TransitionCount:          5,
		},
	}
	s.executableTask.EXPECT().TerminalState().Return(false)
	s.executableTask.EXPECT().MarkExecutionStart()
	s.executableTask.EXPECT().ReplicationTask().Return(replicationTask).AnyTimes()
	s.executableTask.EXPECT().GetNamespaceInfo(gomock.Any(), s.task.NamespaceID, gomock.Any()).Return(
		uuid.NewString(), true, nil,
	).AnyTimes()

	s.mockGetMutableState(s.namespaceID, s.workflowID, s.runID, nil, serviceerror.NewNotFound("missing"))

	task := NewExecutableVerifyVersionedTransitionTask(
		s.toolBox,
		s.taskID,
		time.Now(),
		s.sourceClusterName,
		s.sourceShardKey,
		replicationTask,
	)
	task.ExecutableTask = s.executableTask

	err := task.Execute()
	s.ErrorAs(err, new(*serviceerrors.SyncState))
}

// Execute: getMutableState returns a non-NotFound error -> returned as-is.
func (s *executableVerifyVersionedTransitionTaskSuite) TestExecute_MutableStateError() {
	replicationTask := &replicationspb.ReplicationTask{
		TaskType:     enumsspb.REPLICATION_TASK_TYPE_VERIFY_VERSIONED_TRANSITION_TASK,
		SourceTaskId: s.taskID,
		Attributes: &replicationspb.ReplicationTask_VerifyVersionedTransitionTaskAttributes{
			VerifyVersionedTransitionTaskAttributes: &replicationspb.VerifyVersionedTransitionTaskAttributes{
				NamespaceId: s.namespaceID,
				WorkflowId:  s.workflowID,
				RunId:       s.runID,
				ArchetypeId: chasm.WorkflowArchetypeID,
			},
		},
		VersionedTransition: &persistencespb.VersionedTransition{
			NamespaceFailoverVersion: 3,
			TransitionCount:          5,
		},
	}
	s.executableTask.EXPECT().TerminalState().Return(false)
	s.executableTask.EXPECT().MarkExecutionStart()
	s.executableTask.EXPECT().ReplicationTask().Return(replicationTask).AnyTimes()
	s.executableTask.EXPECT().GetNamespaceInfo(gomock.Any(), s.task.NamespaceID, gomock.Any()).Return(
		uuid.NewString(), true, nil,
	).AnyTimes()

	verifyVTErr := serviceerror.NewUnavailable("boom")
	s.mockGetMutableState(s.namespaceID, s.workflowID, s.runID, nil, verifyVTErr)

	task := NewExecutableVerifyVersionedTransitionTask(
		s.toolBox,
		s.taskID,
		time.Now(),
		s.sourceClusterName,
		s.sourceShardKey,
		replicationTask,
	)
	task.ExecutableTask = s.executableTask

	err := task.Execute()
	s.Equal(verifyVTErr, err)
}

// Execute case 1: current branch up-to-date but NextEventId greater than ms -> data loss.
func (s *executableVerifyVersionedTransitionTaskSuite) TestExecute_CurrentBranch_EventMissed() {
	taskNextEvent := int64(10)
	replicationTask := &replicationspb.ReplicationTask{
		TaskType:     enumsspb.REPLICATION_TASK_TYPE_VERIFY_VERSIONED_TRANSITION_TASK,
		SourceTaskId: s.taskID,
		Attributes: &replicationspb.ReplicationTask_VerifyVersionedTransitionTaskAttributes{
			VerifyVersionedTransitionTaskAttributes: &replicationspb.VerifyVersionedTransitionTaskAttributes{
				NamespaceId: s.namespaceID,
				WorkflowId:  s.workflowID,
				RunId:       s.runID,
				NextEventId: taskNextEvent,
				ArchetypeId: chasm.WorkflowArchetypeID,
			},
		},
		VersionedTransition: &persistencespb.VersionedTransition{
			NamespaceFailoverVersion: 3,
			TransitionCount:          5,
		},
	}
	s.executableTask.EXPECT().TerminalState().Return(false)
	s.executableTask.EXPECT().MarkExecutionStart()
	s.executableTask.EXPECT().ReplicationTask().Return(replicationTask).AnyTimes()
	s.executableTask.EXPECT().GetNamespaceInfo(gomock.Any(), s.task.NamespaceID, gomock.Any()).Return(
		uuid.NewString(), true, nil,
	).AnyTimes()

	mu := historyi.NewMockMutableState(s.controller)
	mu.EXPECT().CloneToProto().Return(
		&persistencespb.WorkflowMutableState{
			ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
				TransitionHistory: []*persistencespb.VersionedTransition{
					{NamespaceFailoverVersion: 1, TransitionCount: 3},
					{NamespaceFailoverVersion: 3, TransitionCount: 6},
				},
			},
			NextEventId: taskNextEvent - 1,
		},
	).AnyTimes()

	s.mockGetMutableState(s.namespaceID, s.workflowID, s.runID, mu, nil)

	task := NewExecutableVerifyVersionedTransitionTask(
		s.toolBox,
		s.taskID,
		time.Now(),
		s.sourceClusterName,
		s.sourceShardKey,
		replicationTask,
	)
	task.ExecutableTask = s.executableTask

	err := task.Execute()
	s.ErrorAs(err, new(*serviceerror.DataLoss))
}

// verifyNewRunExist: getMutableState for new run returns a non-NotFound error -> propagated.
func (s *executableVerifyVersionedTransitionTaskSuite) TestExecute_VerifyNewRun_Error() {
	taskNextEvent := int64(10)
	replicationTask := &replicationspb.ReplicationTask{
		TaskType:     enumsspb.REPLICATION_TASK_TYPE_VERIFY_VERSIONED_TRANSITION_TASK,
		SourceTaskId: s.taskID,
		Attributes: &replicationspb.ReplicationTask_VerifyVersionedTransitionTaskAttributes{
			VerifyVersionedTransitionTaskAttributes: &replicationspb.VerifyVersionedTransitionTaskAttributes{
				NamespaceId: s.namespaceID,
				WorkflowId:  s.workflowID,
				RunId:       s.runID,
				NextEventId: taskNextEvent,
				NewRunId:    s.newRunID,
				ArchetypeId: chasm.WorkflowArchetypeID,
			},
		},
		VersionedTransition: &persistencespb.VersionedTransition{
			NamespaceFailoverVersion: 3,
			TransitionCount:          5,
		},
	}
	s.executableTask.EXPECT().TerminalState().Return(false)
	s.executableTask.EXPECT().MarkExecutionStart()
	s.executableTask.EXPECT().ReplicationTask().Return(replicationTask).AnyTimes()
	s.executableTask.EXPECT().GetNamespaceInfo(gomock.Any(), s.task.NamespaceID, gomock.Any()).Return(
		uuid.NewString(), true, nil,
	).AnyTimes()

	mu := historyi.NewMockMutableState(s.controller)
	mu.EXPECT().CloneToProto().Return(
		&persistencespb.WorkflowMutableState{
			ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
				TransitionHistory: []*persistencespb.VersionedTransition{
					{NamespaceFailoverVersion: 1, TransitionCount: 3},
					{NamespaceFailoverVersion: 3, TransitionCount: 6},
				},
			},
			NextEventId: taskNextEvent,
		},
	).AnyTimes()

	s.mockGetMutableState(s.namespaceID, s.workflowID, s.runID, mu, nil)
	verifyVTErr := serviceerror.NewUnavailable("new run boom")
	s.mockGetMutableState(s.namespaceID, s.workflowID, s.newRunID, nil, verifyVTErr)

	task := NewExecutableVerifyVersionedTransitionTask(
		s.toolBox,
		s.taskID,
		time.Now(),
		s.sourceClusterName,
		s.sourceShardKey,
		replicationTask,
	)
	task.ExecutableTask = s.executableTask

	err := task.Execute()
	s.Equal(verifyVTErr, err)
}

// getMutableState: GetShardByNamespaceWorkflow returns an error -> propagated through Execute.
func (s *executableVerifyVersionedTransitionTaskSuite) TestExecute_GetShardError() {
	replicationTask := &replicationspb.ReplicationTask{
		TaskType:     enumsspb.REPLICATION_TASK_TYPE_VERIFY_VERSIONED_TRANSITION_TASK,
		SourceTaskId: s.taskID,
		Attributes: &replicationspb.ReplicationTask_VerifyVersionedTransitionTaskAttributes{
			VerifyVersionedTransitionTaskAttributes: &replicationspb.VerifyVersionedTransitionTaskAttributes{
				NamespaceId: s.namespaceID,
				WorkflowId:  s.workflowID,
				RunId:       s.runID,
				ArchetypeId: chasm.WorkflowArchetypeID,
			},
		},
		VersionedTransition: &persistencespb.VersionedTransition{
			NamespaceFailoverVersion: 3,
			TransitionCount:          5,
		},
	}
	s.executableTask.EXPECT().TerminalState().Return(false)
	s.executableTask.EXPECT().MarkExecutionStart()
	s.executableTask.EXPECT().ReplicationTask().Return(replicationTask).AnyTimes()
	s.executableTask.EXPECT().GetNamespaceInfo(gomock.Any(), s.task.NamespaceID, gomock.Any()).Return(
		uuid.NewString(), true, nil,
	).AnyTimes()

	verifyVTErr := serviceerror.NewUnavailable("shard err")
	s.shardController.EXPECT().GetShardByNamespaceWorkflow(
		namespace.ID(s.task.NamespaceID),
		s.task.WorkflowID,
	).Return(nil, verifyVTErr)

	task := NewExecutableVerifyVersionedTransitionTask(
		s.toolBox,
		s.taskID,
		time.Now(),
		s.sourceClusterName,
		s.sourceShardKey,
		replicationTask,
	)
	task.ExecutableTask = s.executableTask

	err := task.Execute()
	s.Equal(verifyVTErr, err)
}

// getMutableState: LoadMutableState returns an error -> propagated through Execute.
func (s *executableVerifyVersionedTransitionTaskSuite) TestExecute_LoadMutableStateError() {
	replicationTask := &replicationspb.ReplicationTask{
		TaskType:     enumsspb.REPLICATION_TASK_TYPE_VERIFY_VERSIONED_TRANSITION_TASK,
		SourceTaskId: s.taskID,
		Attributes: &replicationspb.ReplicationTask_VerifyVersionedTransitionTaskAttributes{
			VerifyVersionedTransitionTaskAttributes: &replicationspb.VerifyVersionedTransitionTaskAttributes{
				NamespaceId: s.namespaceID,
				WorkflowId:  s.workflowID,
				RunId:       s.runID,
				ArchetypeId: chasm.WorkflowArchetypeID,
			},
		},
		VersionedTransition: &persistencespb.VersionedTransition{
			NamespaceFailoverVersion: 3,
			TransitionCount:          5,
		},
	}
	s.executableTask.EXPECT().TerminalState().Return(false)
	s.executableTask.EXPECT().MarkExecutionStart()
	s.executableTask.EXPECT().ReplicationTask().Return(replicationTask).AnyTimes()
	s.executableTask.EXPECT().GetNamespaceInfo(gomock.Any(), s.task.NamespaceID, gomock.Any()).Return(
		uuid.NewString(), true, nil,
	).AnyTimes()

	shardContext := historyi.NewMockShardContext(s.controller)
	s.shardController.EXPECT().GetShardByNamespaceWorkflow(
		namespace.ID(s.task.NamespaceID),
		s.task.WorkflowID,
	).Return(shardContext, nil)
	wfCtx := historyi.NewMockWorkflowContext(s.controller)
	verifyVTErr := serviceerror.NewUnavailable("load ms err")
	wfCtx.EXPECT().LoadMutableState(gomock.Any(), shardContext).Return(nil, verifyVTErr)
	s.wfcache.EXPECT().GetOrCreateChasmExecution(
		gomock.Any(),
		shardContext,
		namespace.ID(s.namespaceID),
		&commonpb.WorkflowExecution{
			WorkflowId: s.workflowID,
			RunId:      s.runID,
		},
		chasm.WorkflowArchetypeID,
		locks.PriorityHigh,
	).Return(wfCtx, func(err error) {}, nil)

	task := NewExecutableVerifyVersionedTransitionTask(
		s.toolBox,
		s.taskID,
		time.Now(),
		s.sourceClusterName,
		s.sourceShardKey,
		replicationTask,
	)
	task.ExecutableTask = s.executableTask

	err := task.Execute()
	s.Equal(verifyVTErr, err)
}

// Execute: LCA lookup fails because version histories share no joint point.
func (s *executableVerifyVersionedTransitionTaskSuite) TestExecute_NonCurrentBranch_LCAError() {
	taskNextEvent := int64(10)
	replicationTask := &replicationspb.ReplicationTask{
		TaskType:     enumsspb.REPLICATION_TASK_TYPE_VERIFY_VERSIONED_TRANSITION_TASK,
		SourceTaskId: s.taskID,
		Attributes: &replicationspb.ReplicationTask_VerifyVersionedTransitionTaskAttributes{
			VerifyVersionedTransitionTaskAttributes: &replicationspb.VerifyVersionedTransitionTaskAttributes{
				NamespaceId: s.namespaceID,
				WorkflowId:  s.workflowID,
				RunId:       s.runID,
				NextEventId: taskNextEvent,
				NewRunId:    s.newRunID,
				EventVersionHistory: []*historyspb.VersionHistoryItem{
					{EventId: 9, Version: 7},
				},
				ArchetypeId: chasm.WorkflowArchetypeID,
			},
		},
		VersionedTransition: &persistencespb.VersionedTransition{
			NamespaceFailoverVersion: 1,
			TransitionCount:          4,
		},
	}
	s.executableTask.EXPECT().TerminalState().Return(false)
	s.executableTask.EXPECT().MarkExecutionStart()
	s.executableTask.EXPECT().ReplicationTask().Return(replicationTask).AnyTimes()
	s.executableTask.EXPECT().GetNamespaceInfo(gomock.Any(), s.task.NamespaceID, gomock.Any()).Return(
		uuid.NewString(), true, nil,
	).AnyTimes()

	mu := historyi.NewMockMutableState(s.controller)
	mu.EXPECT().CloneToProto().Return(
		&persistencespb.WorkflowMutableState{
			ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
				TransitionHistory: []*persistencespb.VersionedTransition{
					{NamespaceFailoverVersion: 1, TransitionCount: 3},
					{NamespaceFailoverVersion: 3, TransitionCount: 6},
				},
				VersionHistories: &historyspb.VersionHistories{
					Histories: []*historyspb.VersionHistory{
						{
							BranchToken: []byte{1, 2, 3},
							Items: []*historyspb.VersionHistoryItem{
								{EventId: 8, Version: 5},
							},
						},
					},
				},
			},
			NextEventId: taskNextEvent,
		},
	).AnyTimes()

	s.mockGetMutableState(s.namespaceID, s.workflowID, s.runID, mu, nil)

	task := NewExecutableVerifyVersionedTransitionTask(
		s.toolBox,
		s.taskID,
		time.Now(),
		s.sourceClusterName,
		s.sourceShardKey,
		replicationTask,
	)
	task.ExecutableTask = s.executableTask

	err := task.Execute()
	s.Error(err)
}

func (s *executableVerifyVersionedTransitionTaskSuite) TestHandleErr_Duplicate() {
	s.executableTask.EXPECT().NamespaceName().Return("test-namespace").AnyTimes()
	s.executableTask.EXPECT().MarkTaskDuplicated().Times(1)
	s.NoError(s.task.HandleErr(consts.ErrDuplicate))
}

func (s *executableVerifyVersionedTransitionTaskSuite) TestHandleErr_Default() {
	s.executableTask.EXPECT().NamespaceName().Return("test-namespace").AnyTimes()
	err := errors.New("OwO")
	s.Equal(err, s.task.HandleErr(err))
}

func (s *executableVerifyVersionedTransitionTaskSuite) TestHandleErr_SyncState_NamespaceErr() {
	s.executableTask.EXPECT().NamespaceName().Return("test-namespace").AnyTimes()
	syncStateErr := serviceerrors.NewSyncState(
		"resend", s.namespaceID, s.workflowID, s.runID, chasm.WorkflowArchetypeID, nil, nil,
	)
	nsErr := errors.New("ns error")
	s.executableTask.EXPECT().GetNamespaceInfo(gomock.Any(), s.task.NamespaceID, gomock.Any()).Return(
		"", false, nsErr,
	)
	s.Equal(syncStateErr, s.task.HandleErr(syncStateErr))
}

func (s *executableVerifyVersionedTransitionTaskSuite) TestHandleErr_SyncState_SyncErr() {
	s.executableTask.EXPECT().NamespaceName().Return("test-namespace").AnyTimes()
	syncStateErr := serviceerrors.NewSyncState(
		"resend", s.namespaceID, s.workflowID, s.runID, chasm.WorkflowArchetypeID, nil, nil,
	)
	s.executableTask.EXPECT().GetNamespaceInfo(gomock.Any(), s.task.NamespaceID, gomock.Any()).Return(
		uuid.NewString(), true, nil,
	)
	s.executableTask.EXPECT().SyncState(gomock.Any(), syncStateErr, ResendAttempt).Return(false, errors.New("sync boom"))
	s.Equal(syncStateErr, s.task.HandleErr(syncStateErr))
}

func (s *executableVerifyVersionedTransitionTaskSuite) TestHandleErr_SyncState_NoContinue() {
	s.executableTask.EXPECT().NamespaceName().Return("test-namespace").AnyTimes()
	syncStateErr := serviceerrors.NewSyncState(
		"resend", s.namespaceID, s.workflowID, s.runID, chasm.WorkflowArchetypeID, nil, nil,
	)
	s.executableTask.EXPECT().GetNamespaceInfo(gomock.Any(), s.task.NamespaceID, gomock.Any()).Return(
		uuid.NewString(), true, nil,
	)
	s.executableTask.EXPECT().SyncState(gomock.Any(), syncStateErr, ResendAttempt).Return(false, nil)
	s.NoError(s.task.HandleErr(syncStateErr))
}

func (s *executableVerifyVersionedTransitionTaskSuite) TestHandleErr_SyncState_ContinueReExecute() {
	s.executableTask.EXPECT().NamespaceName().Return("test-namespace").AnyTimes()
	syncStateErr := serviceerrors.NewSyncState(
		"resend", s.namespaceID, s.workflowID, s.runID, chasm.WorkflowArchetypeID, nil, nil,
	)
	// HandleErr GetNamespaceInfo (1) then Execute's GetNamespaceInfo (AnyTimes)
	s.executableTask.EXPECT().GetNamespaceInfo(gomock.Any(), s.task.NamespaceID, gomock.Any()).Return(
		uuid.NewString(), true, nil,
	).AnyTimes()
	s.executableTask.EXPECT().SyncState(gomock.Any(), syncStateErr, ResendAttempt).Return(true, nil)

	// After SyncState returns continue=true, e.Execute() runs. Drive it to a simple success.
	s.executableTask.EXPECT().TerminalState().Return(false)
	s.executableTask.EXPECT().MarkExecutionStart()
	s.executableTask.EXPECT().ReplicationTask().Return(&replicationspb.ReplicationTask{
		VersionedTransition: &persistencespb.VersionedTransition{
			NamespaceFailoverVersion: 3,
			TransitionCount:          5,
		},
	}).AnyTimes()

	mu := historyi.NewMockMutableState(s.controller)
	mu.EXPECT().CloneToProto().Return(
		&persistencespb.WorkflowMutableState{
			ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
				TransitionHistory: []*persistencespb.VersionedTransition{
					{NamespaceFailoverVersion: 3, TransitionCount: 6},
				},
			},
		},
	).AnyTimes()
	s.mockGetMutableState(s.namespaceID, s.workflowID, s.runID, mu, nil)

	// taskAttr has no NewRunId and NextEventId==0 -> case 1 verifyNewRunExist returns nil.
	s.NoError(s.task.HandleErr(syncStateErr))
}

func (s *executableVerifyVersionedTransitionTaskSuite) TestHandleErr_NotFound_Cleanup() {
	s.executableTask.EXPECT().NamespaceName().Return("test-namespace").AnyTimes()
	s.executableTask.EXPECT().SourceShardKey().Return(s.sourceShardKey).AnyTimes()
	// Delete task is built from the verify task's ReplicationTask.
	s.executableTask.EXPECT().ReplicationTask().Return(&replicationspb.ReplicationTask{
		TaskType: enumsspb.REPLICATION_TASK_TYPE_DELETE_EXECUTION_TASK,
		RawTaskInfo: &persistencespb.ReplicationTaskInfo{
			NamespaceId: s.namespaceID,
			WorkflowId:  s.workflowID,
			RunId:       s.runID,
			TaskId:      s.taskID,
			ArchetypeId: chasm.WorkflowArchetypeID,
		},
	}).AnyTimes()

	// The delete task uses the real ExecutableTaskImpl; drive its Execute to the
	// "active cluster" skip so it returns nil without engine interaction.
	s.clusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.namespaceCache.EXPECT().GetNamespaceByID(namespace.ID(s.namespaceID)).Return(
		verifyVTNamespaceEntry(s.namespaceID, cluster.TestCurrentClusterName), nil,
	).AnyTimes()

	err := s.task.HandleErr(serviceerror.NewNotFound("workflow not found in source"))
	s.NoError(err)
}

func verifyVTNamespaceEntry(namespaceID string, activeCluster string) *namespace.Namespace {
	detail := &persistencespb.NamespaceDetail{
		Info: &persistencespb.NamespaceInfo{
			Id:   namespaceID,
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
	namespaceEntry, _ := namespace.FromPersistentState(
		detail,
		namespace.NewDefaultReplicationResolverFactory()(detail),
	)
	return namespaceEntry
}
