package replication

import (
	"errors"
	"math/rand"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/service/history/configs"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
)

type (
	executableWorkflowStateTaskSuite struct {
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

		replicationTask   *replicationspb.SyncWorkflowStateTaskAttributes
		sourceClusterName string
		sourceShardKey    ClusterShardKey

		taskID int64
		task   *ExecutableWorkflowStateTask
	}
)

func TestExecutableWorkflowStateTaskSuite(t *testing.T) {
	s := new(executableWorkflowStateTaskSuite)
	suite.Run(t, s)
}

func (s *executableWorkflowStateTaskSuite) SetupSuite() {
	s.Assertions = require.New(s.T())
}

func (s *executableWorkflowStateTaskSuite) TearDownSuite() {

}

func (s *executableWorkflowStateTaskSuite) SetupTest() {
	s.controller = gomock.NewController(s.T())
	s.clusterMetadata = cluster.NewMockMetadata(s.controller)
	s.clientBean = client.NewMockBean(s.controller)
	s.shardController = shard.NewMockController(s.controller)
	s.namespaceCache = namespace.NewMockRegistry(s.controller)
	s.metricsHandler = metrics.NoopMetricsHandler
	s.logger = log.NewNoopLogger()
	s.executableTask = NewMockExecutableTask(s.controller)
	s.eagerNamespaceRefresher = NewMockEagerNamespaceRefresher(s.controller)
	s.replicationTask = &replicationspb.SyncWorkflowStateTaskAttributes{
		WorkflowState: &persistencespb.WorkflowMutableState{
			ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
				NamespaceId: uuid.NewString(),
				WorkflowId:  uuid.NewString(),
			},
			ExecutionState: &persistencespb.WorkflowExecutionState{
				RunId: uuid.NewString(),
			},
		},
	}
	s.sourceClusterName = cluster.TestCurrentClusterName
	s.sourceShardKey = ClusterShardKey{
		ClusterID: int32(cluster.TestCurrentClusterInitialFailoverVersion),
		ShardID:   rand.Int31(),
	}
	s.mockExecutionManager = persistence.NewMockExecutionManager(s.controller)
	s.config = tests.NewDynamicConfig()

	s.taskID = rand.Int63()
	s.task = NewExecutableWorkflowStateTask(
		ProcessToolBox{
			ClusterMetadata:         s.clusterMetadata,
			ClientBean:              s.clientBean,
			ShardController:         s.shardController,
			NamespaceCache:          s.namespaceCache,
			MetricsHandler:          s.metricsHandler,
			Logger:                  s.logger,
			EagerNamespaceRefresher: s.eagerNamespaceRefresher,
			DLQWriter:               NewExecutionManagerDLQWriter(s.mockExecutionManager),
			Config:                  s.config,
		},
		s.taskID,
		time.Unix(0, rand.Int63()),
		s.replicationTask,
		s.sourceClusterName,
		s.sourceShardKey,
		&replicationspb.ReplicationTask{
			Priority: enumsspb.TASK_PRIORITY_HIGH,
		},
	)
	s.task.ExecutableTask = s.executableTask
	s.executableTask.EXPECT().TaskID().Return(s.taskID).AnyTimes()
	s.executableTask.EXPECT().SourceClusterName().Return(s.sourceClusterName).AnyTimes()
	s.executableTask.EXPECT().GetPriority().Return(enumsspb.TASK_PRIORITY_HIGH).AnyTimes()
}

func (s *executableWorkflowStateTaskSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *executableWorkflowStateTaskSuite) TestExecute_Process() {
	s.executableTask.EXPECT().TerminalState().Return(false)
	s.executableTask.EXPECT().GetNamespaceInfo(gomock.Any(), s.task.NamespaceID, gomock.Any()).Return(
		uuid.NewString(), true, nil,
	).AnyTimes()

	shardContext := historyi.NewMockShardContext(s.controller)
	engine := historyi.NewMockEngine(s.controller)
	s.shardController.EXPECT().GetShardByNamespaceWorkflow(
		namespace.ID(s.task.NamespaceID),
		s.task.WorkflowID,
	).Return(shardContext, nil).AnyTimes()
	shardContext.EXPECT().GetEngine(gomock.Any()).Return(engine, nil).AnyTimes()
	engine.EXPECT().ReplicateWorkflowState(gomock.Any(), &historyservice.ReplicateWorkflowStateRequest{
		NamespaceId:   s.task.NamespaceID,
		WorkflowState: s.replicationTask.GetWorkflowState(),
		RemoteCluster: s.sourceClusterName,
	}).Return(nil)

	err := s.task.Execute()
	s.NoError(err)
}

func (s *executableWorkflowStateTaskSuite) TestExecute_Skip_TerminalState() {
	s.executableTask.EXPECT().TerminalState().Return(true)

	err := s.task.Execute()
	s.NoError(err)
}

func (s *executableWorkflowStateTaskSuite) TestExecute_Skip_Namespace() {
	s.executableTask.EXPECT().TerminalState().Return(false)
	s.executableTask.EXPECT().GetNamespaceInfo(gomock.Any(), s.task.NamespaceID, gomock.Any()).Return(
		uuid.NewString(), false, nil,
	).AnyTimes()

	err := s.task.Execute()
	s.NoError(err)
}

func (s *executableWorkflowStateTaskSuite) TestExecute_Err() {
	s.executableTask.EXPECT().TerminalState().Return(false)
	err := errors.New("OwO")
	s.executableTask.EXPECT().GetNamespaceInfo(gomock.Any(), s.task.NamespaceID, gomock.Any()).Return(
		"", false, err,
	).AnyTimes()

	s.Equal(err, s.task.Execute())
}

func (s *executableWorkflowStateTaskSuite) TestHandleErr_Resend_Success() {
	s.executableTask.EXPECT().TerminalState().Return(false)
	s.executableTask.EXPECT().GetNamespaceInfo(gomock.Any(), s.task.NamespaceID, gomock.Any()).Return(
		uuid.NewString(), true, nil,
	).AnyTimes()
	shardContext := historyi.NewMockShardContext(s.controller)
	engine := historyi.NewMockEngine(s.controller)
	s.shardController.EXPECT().GetShardByNamespaceWorkflow(
		namespace.ID(s.task.NamespaceID),
		s.task.WorkflowID,
	).Return(shardContext, nil).AnyTimes()
	shardContext.EXPECT().GetEngine(gomock.Any()).Return(engine, nil).AnyTimes()
	err := serviceerrors.NewRetryReplication(
		"",
		s.task.NamespaceID,
		s.task.WorkflowID,
		s.task.RunID,
		rand.Int63(),
		rand.Int63(),
		rand.Int63(),
		rand.Int63(),
	)
	s.executableTask.EXPECT().Resend(gomock.Any(), s.sourceClusterName, err, ResendAttempt).Return(true, nil)
	engine.EXPECT().ReplicateWorkflowState(gomock.Any(), gomock.Any()).Return(nil)
	s.NoError(s.task.HandleErr(err))
}

func (s *executableWorkflowStateTaskSuite) TestHandleErr_Resend_Error() {
	s.executableTask.EXPECT().GetNamespaceInfo(gomock.Any(), s.task.NamespaceID, gomock.Any()).Return(
		uuid.NewString(), true, nil,
	).AnyTimes()
	err := serviceerrors.NewRetryReplication(
		"",
		s.task.NamespaceID,
		s.task.WorkflowID,
		s.task.RunID,
		rand.Int63(),
		rand.Int63(),
		rand.Int63(),
		rand.Int63(),
	)
	s.executableTask.EXPECT().Resend(gomock.Any(), s.sourceClusterName, err, ResendAttempt).Return(false, errors.New("OwO"))

	s.Equal(err, s.task.HandleErr(err))
}

func (s *executableWorkflowStateTaskSuite) TestMarkPoisonPill() {
	replicationTask := &replicationspb.ReplicationTask{
		TaskType:     enumsspb.REPLICATION_TASK_TYPE_SYNC_WORKFLOW_STATE_TASK,
		SourceTaskId: s.taskID,
		Attributes: &replicationspb.ReplicationTask_SyncWorkflowStateTaskAttributes{
			SyncWorkflowStateTaskAttributes: s.replicationTask,
		},
		RawTaskInfo: nil,
	}
	s.executableTask.EXPECT().ReplicationTask().Return(replicationTask).AnyTimes()
	s.executableTask.EXPECT().MarkPoisonPill().Times(1)

	err := s.task.MarkPoisonPill()
	s.NoError(err)

	s.Equal(&persistencespb.ReplicationTaskInfo{
		NamespaceId: s.task.NamespaceID,
		WorkflowId:  s.task.WorkflowID,
		RunId:       s.task.RunID,
		TaskId:      s.task.ExecutableTask.TaskID(),
		TaskType:    enumsspb.TASK_TYPE_REPLICATION_SYNC_WORKFLOW_STATE,
	}, replicationTask.RawTaskInfo)
}
