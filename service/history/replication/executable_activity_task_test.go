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
	commonpb "go.temporal.io/api/common/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/primitives/timestamp"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/common/xdc"
	"go.temporal.io/server/service/history/shard"
)

type (
	executableActivityTaskSuite struct {
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

		replicationTask   *replicationspb.SyncActivityTaskAttributes
		sourceClusterName string

		task *ExecutableActivityTask
	}
)

func TestExecutableActivityTaskSuite(t *testing.T) {
	s := new(executableActivityTaskSuite)
	suite.Run(t, s)
}

func (s *executableActivityTaskSuite) SetupSuite() {
	s.Assertions = require.New(s.T())
}

func (s *executableActivityTaskSuite) TearDownSuite() {

}

func (s *executableActivityTaskSuite) SetupTest() {
	s.controller = gomock.NewController(s.T())
	s.clusterMetadata = cluster.NewMockMetadata(s.controller)
	s.clientBean = client.NewMockBean(s.controller)
	s.shardController = shard.NewMockController(s.controller)
	s.namespaceCache = namespace.NewMockRegistry(s.controller)
	s.ndcHistoryResender = xdc.NewMockNDCHistoryResender(s.controller)
	s.metricsHandler = metrics.NoopMetricsHandler
	s.logger = log.NewNoopLogger()
	s.executableTask = NewMockExecutableTask(s.controller)
	s.replicationTask = &replicationspb.SyncActivityTaskAttributes{
		NamespaceId:        uuid.NewString(),
		WorkflowId:         uuid.NewString(),
		RunId:              uuid.NewString(),
		Version:            rand.Int63(),
		ScheduledEventId:   rand.Int63(),
		ScheduledTime:      timestamp.TimePtr(time.Unix(0, rand.Int63())),
		StartedEventId:     rand.Int63(),
		StartedTime:        timestamp.TimePtr(time.Unix(0, rand.Int63())),
		LastHeartbeatTime:  timestamp.TimePtr(time.Unix(0, rand.Int63())),
		Details:            &commonpb.Payloads{},
		Attempt:            rand.Int31(),
		LastFailure:        &failurepb.Failure{},
		LastWorkerIdentity: uuid.NewString(),
		VersionHistory:     &history.VersionHistory{},
	}
	s.sourceClusterName = cluster.TestCurrentClusterName

	s.task = NewExecutableActivityTask(
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

func (s *executableActivityTaskSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *executableActivityTaskSuite) TestExecute_Process() {
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
	engine.EXPECT().SyncActivity(gomock.Any(), &historyservice.SyncActivityRequest{
		NamespaceId:        s.replicationTask.NamespaceId,
		WorkflowId:         s.replicationTask.WorkflowId,
		RunId:              s.replicationTask.RunId,
		Version:            s.replicationTask.Version,
		ScheduledEventId:   s.replicationTask.ScheduledEventId,
		ScheduledTime:      s.replicationTask.ScheduledTime,
		StartedEventId:     s.replicationTask.StartedEventId,
		StartedTime:        s.replicationTask.StartedTime,
		LastHeartbeatTime:  s.replicationTask.LastHeartbeatTime,
		Details:            s.replicationTask.Details,
		Attempt:            s.replicationTask.Attempt,
		LastFailure:        s.replicationTask.LastFailure,
		LastWorkerIdentity: s.replicationTask.LastWorkerIdentity,
		VersionHistory:     s.replicationTask.GetVersionHistory(),
	}).Return(nil)

	err := s.task.Execute()
	s.NoError(err)
}

func (s *executableActivityTaskSuite) TestExecute_Skip() {
	s.executableTask.EXPECT().GetNamespaceInfo(s.task.NamespaceID).Return(
		uuid.NewString(), false, nil,
	).AnyTimes()

	err := s.task.Execute()
	s.NoError(err)
}

func (s *executableActivityTaskSuite) TestExecute_Err() {
	err := errors.New("（╯‵□′）╯︵┴─┴")
	s.executableTask.EXPECT().GetNamespaceInfo(s.task.NamespaceID).Return(
		"", false, err,
	).AnyTimes()

	s.Equal(err, s.task.Execute())
}

func (s *executableActivityTaskSuite) TestHandleErr_Resend_Success() {
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
	engine.EXPECT().SyncActivity(gomock.Any(), &historyservice.SyncActivityRequest{
		NamespaceId:        s.replicationTask.NamespaceId,
		WorkflowId:         s.replicationTask.WorkflowId,
		RunId:              s.replicationTask.RunId,
		Version:            s.replicationTask.Version,
		ScheduledEventId:   s.replicationTask.ScheduledEventId,
		ScheduledTime:      s.replicationTask.ScheduledTime,
		StartedEventId:     s.replicationTask.StartedEventId,
		StartedTime:        s.replicationTask.StartedTime,
		LastHeartbeatTime:  s.replicationTask.LastHeartbeatTime,
		Details:            s.replicationTask.Details,
		Attempt:            s.replicationTask.Attempt,
		LastFailure:        s.replicationTask.LastFailure,
		LastWorkerIdentity: s.replicationTask.LastWorkerIdentity,
		VersionHistory:     s.replicationTask.GetVersionHistory(),
	}).Return(nil)

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
	s.executableTask.EXPECT().Resend(gomock.Any(), s.sourceClusterName, gomock.Any(), err).Return(nil)

	s.NoError(s.task.HandleErr(err))
}

func (s *executableActivityTaskSuite) TestHandleErr_Resend_Error() {
	s.executableTask.EXPECT().GetNamespaceInfo(s.task.NamespaceID).Return(
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
	s.executableTask.EXPECT().Resend(gomock.Any(), s.sourceClusterName, gomock.Any(), err).Return(errors.New("（╯‵□′）╯︵┴─┴"))

	s.Equal(err, s.task.HandleErr(err))
}

func (s *executableActivityTaskSuite) TestHandleErr_Other() {
	err := errors.New("（╯‵□′）╯︵┴─┴")
	s.Equal(err, s.task.HandleErr(err))

	err = serviceerror.NewUnavailable("")
	s.Equal(err, s.task.HandleErr(err))
}
