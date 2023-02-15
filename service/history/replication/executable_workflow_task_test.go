package replication

import (
	"errors"
	"math/rand"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/api/historyservice/v1"
	persistencepb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/xdc"
	"go.temporal.io/server/service/history/shard"
)

type (
	executableWorkflowTaskSuite struct {
		suite.Suite
		*require.Assertions

		controller         *gomock.Controller
		clusterMetadata    *cluster.MockMetadata
		clientBean         *client.MockBean
		shardController    *shard.MockController
		namespaceCache     *namespace.MockRegistry
		ndcHistoryResender *xdc.MockNDCHistoryResender
		metricsHandler     metrics.Handler
		logger             log.Logger
		executableTask     *MockExecutableTask

		replicationTask   *replicationspb.SyncWorkflowStateTaskAttributes
		sourceClusterName string

		task *ExecutableWorkflowTask
	}
)

func TestExecutableWorkflowTaskSuite(t *testing.T) {
	s := new(executableWorkflowTaskSuite)
	suite.Run(t, s)
}

func (s *executableWorkflowTaskSuite) SetupSuite() {
	s.Assertions = require.New(s.T())
}

func (s *executableWorkflowTaskSuite) TearDownSuite() {

}

func (s *executableWorkflowTaskSuite) SetupTest() {
	s.controller = gomock.NewController(s.T())
	s.clusterMetadata = cluster.NewMockMetadata(s.controller)
	s.clientBean = client.NewMockBean(s.controller)
	s.shardController = shard.NewMockController(s.controller)
	s.namespaceCache = namespace.NewMockRegistry(s.controller)
	s.ndcHistoryResender = xdc.NewMockNDCHistoryResender(s.controller)
	s.metricsHandler = metrics.NoopMetricsHandler
	s.logger = log.NewNoopLogger()
	s.executableTask = NewMockExecutableTask(s.controller)
	s.replicationTask = &replicationspb.SyncWorkflowStateTaskAttributes{
		WorkflowState: &persistencepb.WorkflowMutableState{
			ExecutionInfo: &persistencepb.WorkflowExecutionInfo{
				NamespaceId: uuid.NewString(),
				WorkflowId:  uuid.NewString(),
			},
			ExecutionState: &persistencepb.WorkflowExecutionState{
				RunId: uuid.NewString(),
			},
		},
	}
	s.sourceClusterName = cluster.TestCurrentClusterName

	s.task = NewExecutableWorkflowTask(
		ProcessToolBox{
			ClusterMetadata:    s.clusterMetadata,
			ClientBean:         s.clientBean,
			ShardController:    s.shardController,
			NamespaceCache:     s.namespaceCache,
			NDCHistoryResender: s.ndcHistoryResender,
			MetricsHandler:     s.metricsHandler,
			Logger:             s.logger,
		},
		rand.Int63(),
		time.Unix(0, rand.Int63()),
		s.replicationTask,
		s.sourceClusterName,
	)
	s.task.ExecutableTask = s.executableTask
}

func (s *executableWorkflowTaskSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *executableWorkflowTaskSuite) TestExecute_Process() {
	s.executableTask.EXPECT().GetNamespaceInfo(s.task.NamespaceID).Return(
		uuid.NewString(), true, nil,
	).AnyTimes()

	shardContext := shard.NewMockContext(s.controller)
	engine := shard.NewMockEngine(s.controller)
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

func (s *executableWorkflowTaskSuite) TestExecute_Skip() {
	s.executableTask.EXPECT().GetNamespaceInfo(s.task.NamespaceID).Return(
		uuid.NewString(), false, nil,
	).AnyTimes()

	err := s.task.Execute()
	s.NoError(err)
}

func (s *executableWorkflowTaskSuite) TestExecute_Err() {
	err := errors.New("（╯‵□′）╯︵┴─┴")
	s.executableTask.EXPECT().GetNamespaceInfo(s.task.NamespaceID).Return(
		"", false, err,
	).AnyTimes()

	s.Equal(err, s.task.Execute())
}

func (s *executableWorkflowTaskSuite) TestHandleErr() {
	err := errors.New("（╯‵□′）╯︵┴─┴")
	s.Equal(err, s.task.HandleErr(err))

	err = serviceerror.NewUnavailable("")
	s.Equal(err, s.task.HandleErr(err))
}
