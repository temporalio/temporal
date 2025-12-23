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
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	workflowspb "go.temporal.io/server/api/workflow/v1"
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
	"google.golang.org/protobuf/types/known/timestamppb"
)

type (
	executableActivityStateTaskSuite struct {
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
		EagerNamespaceRefresher *MockEagerNamespaceRefresher
		mockExecutionManager    *persistence.MockExecutionManager
		config                  *configs.Config

		replicationTask   *replicationspb.SyncActivityTaskAttributes
		sourceClusterName string
		sourceShardKey    ClusterShardKey

		taskID int64
		task   *ExecutableActivityStateTask
	}
)

func TestExecutableActivityStateTaskSuite(t *testing.T) {
	s := new(executableActivityStateTaskSuite)
	suite.Run(t, s)
}

func (s *executableActivityStateTaskSuite) SetupSuite() {
	s.Assertions = require.New(s.T())
}

func (s *executableActivityStateTaskSuite) TearDownSuite() {

}

func (s *executableActivityStateTaskSuite) SetupTest() {
	s.controller = gomock.NewController(s.T())
	s.clusterMetadata = cluster.NewMockMetadata(s.controller)
	s.clientBean = client.NewMockBean(s.controller)
	s.shardController = shard.NewMockController(s.controller)
	s.namespaceCache = namespace.NewMockRegistry(s.controller)
	s.metricsHandler = metrics.NoopMetricsHandler
	s.logger = log.NewNoopLogger()
	s.executableTask = NewMockExecutableTask(s.controller)
	s.EagerNamespaceRefresher = NewMockEagerNamespaceRefresher(s.controller)
	s.config = tests.NewDynamicConfig()
	s.replicationTask = &replicationspb.SyncActivityTaskAttributes{
		NamespaceId:        uuid.NewString(),
		WorkflowId:         uuid.NewString(),
		RunId:              uuid.NewString(),
		Version:            rand.Int63(),
		ScheduledEventId:   rand.Int63(),
		ScheduledTime:      timestamppb.New(time.Unix(0, rand.Int63())),
		StartedEventId:     rand.Int63(),
		StartedTime:        timestamppb.New(time.Unix(0, rand.Int63())),
		LastHeartbeatTime:  timestamppb.New(time.Unix(0, rand.Int63())),
		Details:            &commonpb.Payloads{},
		Attempt:            rand.Int31(),
		LastFailure:        &failurepb.Failure{},
		LastWorkerIdentity: uuid.NewString(),
		BaseExecutionInfo:  &workflowspb.BaseExecutionInfo{},
		VersionHistory:     &historyspb.VersionHistory{},
	}
	s.sourceClusterName = cluster.TestCurrentClusterName
	s.sourceShardKey = ClusterShardKey{
		ClusterID: int32(cluster.TestCurrentClusterInitialFailoverVersion),
		ShardID:   rand.Int31(),
	}
	s.taskID = rand.Int63()
	s.mockExecutionManager = persistence.NewMockExecutionManager(s.controller)
	s.task = NewExecutableActivityStateTask(
		ProcessToolBox{
			ClusterMetadata: s.clusterMetadata,
			ClientBean:      s.clientBean,
			ShardController: s.shardController,
			NamespaceCache:  s.namespaceCache,
			MetricsHandler:  s.metricsHandler,
			Logger:          s.logger,
			DLQWriter:       NewExecutionManagerDLQWriter(s.mockExecutionManager),
			Config:          s.config,
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

func (s *executableActivityStateTaskSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *executableActivityStateTaskSuite) TestExecute_Process() {
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
	engine.EXPECT().SyncActivity(gomock.Any(), &historyservice.SyncActivityRequest{
		NamespaceId:        s.replicationTask.NamespaceId,
		WorkflowId:         s.replicationTask.WorkflowId,
		RunId:              s.replicationTask.RunId,
		Version:            s.replicationTask.Version,
		ScheduledEventId:   s.replicationTask.ScheduledEventId,
		ScheduledTime:      s.replicationTask.ScheduledTime,
		StartedEventId:     s.replicationTask.StartedEventId,
		StartVersion:       s.replicationTask.StartVersion,
		StartedTime:        s.replicationTask.StartedTime,
		LastHeartbeatTime:  s.replicationTask.LastHeartbeatTime,
		Details:            s.replicationTask.Details,
		Attempt:            s.replicationTask.Attempt,
		LastFailure:        s.replicationTask.LastFailure,
		LastWorkerIdentity: s.replicationTask.LastWorkerIdentity,
		BaseExecutionInfo:  s.replicationTask.BaseExecutionInfo,
		VersionHistory:     s.replicationTask.GetVersionHistory(),
	}).Return(nil)

	err := s.task.Execute()
	s.NoError(err)
}

func (s *executableActivityStateTaskSuite) TestExecute_Skip_TerminalState() {
	s.executableTask.EXPECT().TerminalState().Return(true)

	err := s.task.Execute()
	s.NoError(err)
}

func (s *executableActivityStateTaskSuite) TestExecute_Skip_Namespace() {
	s.executableTask.EXPECT().TerminalState().Return(false)
	s.executableTask.EXPECT().GetNamespaceInfo(gomock.Any(), s.task.NamespaceID, gomock.Any()).Return(
		uuid.NewString(), false, nil,
	).AnyTimes()

	err := s.task.Execute()
	s.NoError(err)
}

func (s *executableActivityStateTaskSuite) TestExecute_Err() {
	err := errors.New("OwO")
	s.executableTask.EXPECT().TerminalState().Return(false)
	s.executableTask.EXPECT().GetNamespaceInfo(gomock.Any(), s.task.NamespaceID, gomock.Any()).Return(
		"", false, err,
	).AnyTimes()

	s.Equal(err, s.task.Execute())
}

func (s *executableActivityStateTaskSuite) TestHandleErr_Resend_Success() {
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
	engine.EXPECT().SyncActivity(gomock.Any(), &historyservice.SyncActivityRequest{
		NamespaceId:        s.replicationTask.NamespaceId,
		WorkflowId:         s.replicationTask.WorkflowId,
		RunId:              s.replicationTask.RunId,
		Version:            s.replicationTask.Version,
		ScheduledEventId:   s.replicationTask.ScheduledEventId,
		ScheduledTime:      s.replicationTask.ScheduledTime,
		StartedEventId:     s.replicationTask.StartedEventId,
		StartVersion:       s.replicationTask.StartVersion,
		StartedTime:        s.replicationTask.StartedTime,
		LastHeartbeatTime:  s.replicationTask.LastHeartbeatTime,
		Details:            s.replicationTask.Details,
		Attempt:            s.replicationTask.Attempt,
		LastFailure:        s.replicationTask.LastFailure,
		LastWorkerIdentity: s.replicationTask.LastWorkerIdentity,
		BaseExecutionInfo:  s.replicationTask.BaseExecutionInfo,
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
	s.executableTask.EXPECT().Resend(gomock.Any(), s.sourceClusterName, err, ResendAttempt).Return(true, nil)

	s.NoError(s.task.HandleErr(err))
}

func (s *executableActivityStateTaskSuite) TestHandleErr_Resend_Error() {
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

func (s *executableActivityStateTaskSuite) TestHandleErr_Other() {
	err := errors.New("OwO")
	s.Equal(err, s.task.HandleErr(err))

	err = serviceerror.NewNotFound("")
	s.Equal(nil, s.task.HandleErr(err))

	err = serviceerror.NewUnavailable("")
	s.Equal(err, s.task.HandleErr(err))
}

func (s *executableActivityStateTaskSuite) TestMarkPoisonPill() {
	replicationTask := &replicationspb.ReplicationTask{
		TaskType:     enumsspb.REPLICATION_TASK_TYPE_SYNC_ACTIVITY_TASK,
		SourceTaskId: s.taskID,
		Attributes: &replicationspb.ReplicationTask_SyncActivityTaskAttributes{
			SyncActivityTaskAttributes: s.replicationTask,
		},
		RawTaskInfo: nil,
	}
	s.executableTask.EXPECT().ReplicationTask().Return(replicationTask).AnyTimes()
	s.executableTask.EXPECT().MarkPoisonPill().Times(1)

	err := s.task.MarkPoisonPill()
	s.NoError(err)

	s.Equal(&persistencespb.ReplicationTaskInfo{
		NamespaceId:      s.task.NamespaceID,
		WorkflowId:       s.task.WorkflowID,
		RunId:            s.task.RunID,
		TaskId:           s.task.ExecutableTask.TaskID(),
		TaskType:         enumsspb.TASK_TYPE_REPLICATION_SYNC_ACTIVITY,
		ScheduledEventId: s.task.req.ScheduledEventId,
		Version:          s.task.req.Version,
	}, replicationTask.RawTaskInfo)
}

func (s *executableActivityStateTaskSuite) TestBatchedTask_ShouldBatchTogether_AndExecute() {
	namespaceId := uuid.NewString()
	workflowId := uuid.NewString()
	runId := uuid.NewString()
	replicationAttribute1 := s.generateReplicationAttribute(namespaceId, workflowId, runId)
	config := tests.NewDynamicConfig()
	config.EnableReplicationTaskBatching = func() bool {
		return true
	}
	task1 := NewExecutableActivityStateTask(
		ProcessToolBox{
			ClusterMetadata: s.clusterMetadata,
			ClientBean:      s.clientBean,
			ShardController: s.shardController,
			NamespaceCache:  s.namespaceCache,
			MetricsHandler:  s.metricsHandler,
			Logger:          s.logger,
			DLQWriter:       NewExecutionManagerDLQWriter(s.mockExecutionManager),
			Config:          config,
		},
		1,
		time.Unix(0, rand.Int63()),
		replicationAttribute1,
		s.sourceClusterName,
		s.sourceShardKey,
		&replicationspb.ReplicationTask{
			Priority: enumsspb.TASK_PRIORITY_HIGH,
		},
	)
	task1.ExecutableTask = s.executableTask

	replicationAttribute2 := s.generateReplicationAttribute(namespaceId, workflowId, runId)
	task2 := NewExecutableActivityStateTask(
		ProcessToolBox{
			ClusterMetadata: s.clusterMetadata,
			ClientBean:      s.clientBean,
			ShardController: s.shardController,
			NamespaceCache:  s.namespaceCache,
			MetricsHandler:  s.metricsHandler,
			Logger:          s.logger,
			DLQWriter:       NewExecutionManagerDLQWriter(s.mockExecutionManager),
			Config:          s.config,
		},
		2,
		time.Unix(0, rand.Int63()),
		replicationAttribute2,
		s.sourceClusterName,
		s.sourceShardKey,
		&replicationspb.ReplicationTask{
			Priority: enumsspb.TASK_PRIORITY_HIGH,
		},
	)
	task2.ExecutableTask = s.executableTask

	batchResult, batched := task1.BatchWith(task2)
	s.True(batched)
	activityTask, _ := batchResult.(*ExecutableActivityStateTask)
	s.Equal(2, len(activityTask.activityInfos))
	s.assertAttributeEqual(replicationAttribute1, activityTask.activityInfos[0])
	s.assertAttributeEqual(replicationAttribute2, activityTask.activityInfos[1])

	s.executableTask.EXPECT().TerminalState().Return(false)
	s.executableTask.EXPECT().GetNamespaceInfo(gomock.Any(), namespaceId, gomock.Any()).Return(
		uuid.NewString(), true, nil,
	).AnyTimes()
	shardContext := historyi.NewMockShardContext(s.controller)
	engine := historyi.NewMockEngine(s.controller)
	s.shardController.EXPECT().GetShardByNamespaceWorkflow(
		namespace.ID(namespaceId),
		workflowId,
	).Return(shardContext, nil).AnyTimes()
	shardContext.EXPECT().GetEngine(gomock.Any()).Return(engine, nil).AnyTimes()

	engine.EXPECT().SyncActivities(gomock.Any(), &historyservice.SyncActivitiesRequest{
		NamespaceId:    namespaceId,
		WorkflowId:     workflowId,
		RunId:          runId,
		ActivitiesInfo: activityTask.activityInfos,
	})
	err := batchResult.Execute()
	s.Nil(err)
}

func (s *executableActivityStateTaskSuite) TestBatchWith_InvalidBatchTask_ShouldNotBatch() {
	namespaceId := uuid.NewString()
	runId := uuid.NewString()
	replicationAttribute1 := s.generateReplicationAttribute(namespaceId, "wf_1", runId)
	task1 := NewExecutableActivityStateTask(
		ProcessToolBox{
			ClusterMetadata: s.clusterMetadata,
			ClientBean:      s.clientBean,
			ShardController: s.shardController,
			NamespaceCache:  s.namespaceCache,
			MetricsHandler:  s.metricsHandler,
			Logger:          s.logger,
			DLQWriter:       NewExecutionManagerDLQWriter(s.mockExecutionManager),
			Config:          s.config,
		},
		1,
		time.Unix(0, rand.Int63()),
		replicationAttribute1,
		s.sourceClusterName,
		s.sourceShardKey,
		&replicationspb.ReplicationTask{
			Priority: enumsspb.TASK_PRIORITY_HIGH,
		},
	)

	replicationAttribute2 := s.generateReplicationAttribute(namespaceId, "wf_2", runId) //
	task2 := NewExecutableActivityStateTask(
		ProcessToolBox{
			ClusterMetadata: s.clusterMetadata,
			ClientBean:      s.clientBean,
			ShardController: s.shardController,
			NamespaceCache:  s.namespaceCache,
			MetricsHandler:  s.metricsHandler,
			Logger:          s.logger,
			DLQWriter:       NewExecutionManagerDLQWriter(s.mockExecutionManager),
			Config:          s.config,
		},
		2,
		time.Unix(0, rand.Int63()),
		replicationAttribute2,
		s.sourceClusterName,
		s.sourceShardKey,
		&replicationspb.ReplicationTask{
			Priority: enumsspb.TASK_PRIORITY_HIGH,
		},
	)
	batchResult, batched := task1.BatchWith(task2)
	s.False(batched)
	s.Nil(batchResult)
}

func (s *executableActivityStateTaskSuite) generateReplicationAttribute(
	namespaceId string,
	workflowId string,
	runId string,
) *replicationspb.SyncActivityTaskAttributes {
	return &replicationspb.SyncActivityTaskAttributes{
		NamespaceId:        namespaceId,
		WorkflowId:         workflowId,
		RunId:              runId,
		Version:            rand.Int63(),
		ScheduledEventId:   rand.Int63(),
		ScheduledTime:      timestamppb.New(time.Unix(0, rand.Int63())),
		StartedEventId:     rand.Int63(),
		StartedTime:        timestamppb.New(time.Unix(0, rand.Int63())),
		LastHeartbeatTime:  timestamppb.New(time.Unix(0, rand.Int63())),
		Details:            &commonpb.Payloads{},
		Attempt:            rand.Int31(),
		LastFailure:        &failurepb.Failure{},
		LastWorkerIdentity: uuid.NewString(),
		BaseExecutionInfo:  &workflowspb.BaseExecutionInfo{},
		VersionHistory:     &historyspb.VersionHistory{},
	}
}

func (s *executableActivityStateTaskSuite) assertAttributeEqual(
	expected *replicationspb.SyncActivityTaskAttributes,
	actual *historyservice.ActivitySyncInfo,
) {
	s.Equal(expected.Version, actual.Version)
	s.Equal(expected.ScheduledEventId, actual.ScheduledEventId)
	s.Equal(expected.ScheduledTime, actual.ScheduledTime)
	s.Equal(expected.StartedEventId, actual.StartedEventId)
	s.Equal(expected.StartedTime, actual.StartedTime)
	s.Equal(expected.LastHeartbeatTime, actual.LastHeartbeatTime)
	s.Equal(expected.Details, actual.Details)
	s.Equal(expected.Attempt, actual.Attempt)
	s.Equal(expected.LastFailure, actual.LastFailure)
	s.Equal(expected.LastWorkerIdentity, actual.LastWorkerIdentity)
	s.Equal(expected.VersionHistory, actual.VersionHistory)
}
