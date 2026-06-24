package replication

import (
	"context"
	"errors"
	"math/rand"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/adminservicemock/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	ctasks "go.temporal.io/server/common/tasks"
	"go.temporal.io/server/common/testing/protomock"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/consts"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/replication/eventhandler"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
)

type (
	executableTaskSuite struct {
		suite.Suite
		*require.Assertions

		controller              *gomock.Controller
		clusterMetadata         *cluster.MockMetadata
		clientBean              *client.MockBean
		shardController         *shard.MockController
		namespaceCache          *namespace.MockRegistry
		remoteHistoryFetcher    *eventhandler.MockHistoryPaginatedFetcher
		metricsHandler          metrics.Handler
		logger                  log.Logger
		sourceCluster           string
		sourceShardKey          ClusterShardKey
		eagerNamespaceRefresher *MockEagerNamespaceRefresher
		config                  *configs.Config
		namespaceId             string
		workflowId              string
		runId                   string
		taskId                  int64
		mockExecutionManager    *persistence.MockExecutionManager
		serializer              serialization.Serializer
		resendHandler           *eventhandler.MockResendHandler
		toolBox                 ProcessToolBox

		task *ExecutableTaskImpl
	}
)

func TestExecutableTaskSuite(t *testing.T) {
	s := new(executableTaskSuite)
	suite.Run(t, s)
}

func (s *executableTaskSuite) SetupSuite() {
	s.Assertions = require.New(s.T())
}

func (s *executableTaskSuite) TearDownSuite() {

}

func (s *executableTaskSuite) SetupTest() {
	s.controller = gomock.NewController(s.T())
	s.clusterMetadata = cluster.NewMockMetadata(s.controller)
	s.clientBean = client.NewMockBean(s.controller)
	s.shardController = shard.NewMockController(s.controller)
	s.namespaceCache = namespace.NewMockRegistry(s.controller)
	s.mockExecutionManager = persistence.NewMockExecutionManager(s.controller)
	s.eagerNamespaceRefresher = NewMockEagerNamespaceRefresher(s.controller)
	s.remoteHistoryFetcher = eventhandler.NewMockHistoryPaginatedFetcher(s.controller)
	s.metricsHandler = metrics.NoopMetricsHandler
	s.logger = log.NewNoopLogger()
	s.config = tests.NewDynamicConfig()
	s.serializer = serialization.NewSerializer()
	s.resendHandler = eventhandler.NewMockResendHandler(s.controller)

	s.clusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.clusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestAllClusterInfo).AnyTimes()

	creationTime := time.Unix(0, rand.Int63())
	receivedTime := creationTime.Add(time.Duration(rand.Int63()))
	s.sourceCluster = cluster.TestAlternativeClusterName
	s.sourceShardKey = ClusterShardKey{
		ClusterID: int32(cluster.TestAlternativeClusterInitialFailoverVersion),
		ShardID:   rand.Int31(),
	}
	s.namespaceId = uuid.NewString()
	s.workflowId = uuid.NewString()
	s.runId = uuid.NewString()
	s.taskId = rand.Int63()
	s.toolBox = ProcessToolBox{
		Config:                  s.config,
		ClusterMetadata:         s.clusterMetadata,
		ClientBean:              s.clientBean,
		ResendHandler:           s.resendHandler,
		ShardController:         s.shardController,
		NamespaceCache:          s.namespaceCache,
		MetricsHandler:          s.metricsHandler,
		Logger:                  s.logger,
		ThrottledLogger:         s.logger,
		EagerNamespaceRefresher: s.eagerNamespaceRefresher,
		DLQWriter:               NewExecutionManagerDLQWriter(s.mockExecutionManager),
		Serializer:              s.serializer,
		RemoteHistoryFetcher:    s.remoteHistoryFetcher,
	}

	s.task = NewExecutableTask(
		s.toolBox,
		s.taskId,
		"metrics-tag",
		creationTime,
		receivedTime,
		s.sourceCluster,
		s.sourceShardKey,
		&replicationspb.ReplicationTask{
			RawTaskInfo: &persistencespb.ReplicationTaskInfo{
				NamespaceId: s.namespaceId,
				WorkflowId:  s.workflowId,
				RunId:       s.runId,
				TaskId:      s.taskId,
				TaskEquivalents: []*persistencespb.ReplicationTaskInfo{
					{NamespaceId: s.namespaceId, WorkflowId: s.workflowId, RunId: s.runId},
					{NamespaceId: s.namespaceId, WorkflowId: s.workflowId, RunId: s.runId},
				},
			},
		},
	)
}

func (s *executableTaskSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *executableTaskSuite) TestTaskID() {
	s.Equal(s.task.taskID, s.task.TaskID())
}

func (s *executableTaskSuite) TestCreationTime() {
	s.Equal(s.task.taskCreationTime, s.task.TaskCreationTime())
}

func (s *executableTaskSuite) TestAckStateAttempt() {
	s.Equal(ctasks.TaskStatePending, s.task.State())
	s.False(s.task.TerminalState())

	s.task.Ack()
	s.Equal(ctasks.TaskStateAcked, s.task.State())
	s.Equal(1, s.task.Attempt())

	s.task.Nack(nil)
	s.Equal(ctasks.TaskStateAcked, s.task.State())
	s.Equal(1, s.task.Attempt())
	s.task.Abort()
	s.Equal(ctasks.TaskStateAcked, s.task.State())
	s.Equal(1, s.task.Attempt())
	s.task.Cancel()
	s.Equal(ctasks.TaskStateAcked, s.task.State())
	s.Equal(1, s.task.Attempt())
	s.task.Reschedule()
	s.Equal(ctasks.TaskStateAcked, s.task.State())
	s.Equal(1, s.task.Attempt())

	s.True(s.task.TerminalState())
}

func (s *executableTaskSuite) TestNackStateAttempt() {
	s.Equal(ctasks.TaskStatePending, s.task.State())
	s.False(s.task.TerminalState())

	s.task.Nack(nil)
	s.Equal(ctasks.TaskStateNacked, s.task.State())
	s.Equal(1, s.task.Attempt())

	s.task.Ack()
	s.Equal(ctasks.TaskStateNacked, s.task.State())
	s.Equal(1, s.task.Attempt())
	s.task.Abort()
	s.Equal(ctasks.TaskStateNacked, s.task.State())
	s.Equal(1, s.task.Attempt())
	s.task.Cancel()
	s.Equal(ctasks.TaskStateNacked, s.task.State())
	s.Equal(1, s.task.Attempt())
	s.task.Reschedule()
	s.Equal(ctasks.TaskStateNacked, s.task.State())
	s.Equal(1, s.task.Attempt())

	s.True(s.task.TerminalState())
}

func (s *executableTaskSuite) TestAbortStateAttempt() {
	s.Equal(ctasks.TaskStatePending, s.task.State())
	s.False(s.task.TerminalState())

	s.task.Abort()
	s.Equal(ctasks.TaskStateAborted, s.task.State())
	s.Equal(1, s.task.Attempt())

	s.task.Ack()
	s.Equal(ctasks.TaskStateAborted, s.task.State())
	s.Equal(1, s.task.Attempt())
	s.task.Nack(nil)
	s.Equal(ctasks.TaskStateAborted, s.task.State())
	s.Equal(1, s.task.Attempt())
	s.task.Cancel()
	s.Equal(ctasks.TaskStateAborted, s.task.State())
	s.Equal(1, s.task.Attempt())
	s.task.Reschedule()
	s.Equal(ctasks.TaskStateAborted, s.task.State())
	s.Equal(1, s.task.Attempt())

	s.True(s.task.TerminalState())
}

func (s *executableTaskSuite) TestCancelStateAttempt() {
	s.Equal(ctasks.TaskStatePending, s.task.State())
	s.False(s.task.TerminalState())

	s.task.Cancel()
	s.Equal(ctasks.TaskStateCancelled, s.task.State())
	s.Equal(1, s.task.Attempt())

	s.task.Ack()
	s.Equal(ctasks.TaskStateCancelled, s.task.State())
	s.Equal(1, s.task.Attempt())
	s.task.Nack(nil)
	s.Equal(ctasks.TaskStateCancelled, s.task.State())
	s.Equal(1, s.task.Attempt())
	s.task.Abort()
	s.Equal(ctasks.TaskStateCancelled, s.task.State())
	s.Equal(1, s.task.Attempt())
	s.task.Reschedule()
	s.Equal(ctasks.TaskStateCancelled, s.task.State())
	s.Equal(1, s.task.Attempt())

	s.True(s.task.TerminalState())
}

func (s *executableTaskSuite) TestRescheduleStateAttempt() {
	s.Equal(ctasks.TaskStatePending, s.task.State())
	s.False(s.task.TerminalState())

	s.task.Reschedule()
	s.Equal(ctasks.TaskStatePending, s.task.State())
	s.Equal(2, s.task.Attempt())

	s.False(s.task.TerminalState())
}

func (s *executableTaskSuite) TestIsRetryableError() {
	err := errors.New("OwO")
	s.True(s.task.IsRetryableError(err))

	err = serviceerror.NewInternal("OwO")
	s.True(s.task.IsRetryableError(err))

	err = serviceerror.NewUnavailable("OwO")
	s.True(s.task.IsRetryableError(err))

	err = serviceerror.NewInvalidArgument("OwO")
	s.False(s.task.IsRetryableError(err))
}

func (s *executableTaskSuite) TestResend_Success() {
	remoteCluster := cluster.TestAlternativeClusterName
	resendErr := &serviceerrors.RetryReplication{
		NamespaceId:       uuid.NewString(),
		WorkflowId:        uuid.NewString(),
		RunId:             uuid.NewString(),
		StartEventId:      rand.Int63(),
		StartEventVersion: rand.Int63(),
		EndEventId:        rand.Int63(),
		EndEventVersion:   rand.Int63(),
	}

	s.resendHandler.EXPECT().ResendHistoryEvents(
		gomock.Any(),
		remoteCluster,
		namespace.ID(resendErr.NamespaceId),
		resendErr.WorkflowId,
		resendErr.RunId,
		resendErr.StartEventId,
		resendErr.StartEventVersion,
		resendErr.EndEventId,
		resendErr.EndEventVersion,
	).Return(nil)

	doContinue, err := s.task.Resend(context.Background(), remoteCluster, resendErr, ResendAttempt)
	s.NoError(err)
	s.True(doContinue)
}

func (s *executableTaskSuite) TestResend_NotFound() {
	remoteCluster := cluster.TestAlternativeClusterName
	resendErr := &serviceerrors.RetryReplication{
		NamespaceId:       uuid.NewString(),
		WorkflowId:        uuid.NewString(),
		RunId:             uuid.NewString(),
		StartEventId:      rand.Int63(),
		StartEventVersion: rand.Int63(),
		EndEventId:        rand.Int63(),
		EndEventVersion:   rand.Int63(),
	}

	s.resendHandler.EXPECT().ResendHistoryEvents(
		gomock.Any(),
		remoteCluster,
		namespace.ID(resendErr.NamespaceId),
		resendErr.WorkflowId,
		resendErr.RunId,
		resendErr.StartEventId,
		resendErr.StartEventVersion,
		resendErr.EndEventId,
		resendErr.EndEventVersion,
	).Return(serviceerror.NewNotFound(""))
	shardContext := historyi.NewMockShardContext(s.controller)
	engine := historyi.NewMockEngine(s.controller)
	s.shardController.EXPECT().GetShardByNamespaceWorkflow(
		namespace.ID(resendErr.NamespaceId),
		resendErr.WorkflowId,
	).Return(shardContext, nil).AnyTimes()
	shardContext.EXPECT().GetEngine(gomock.Any()).Return(engine, nil).AnyTimes()
	engine.EXPECT().DeleteWorkflowExecution(gomock.Any(), &historyservice.DeleteWorkflowExecutionRequest{
		NamespaceId: resendErr.NamespaceId,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: resendErr.WorkflowId,
			RunId:      resendErr.RunId,
		},
		ClosedWorkflowOnly: false,
	}).Return(&historyservice.DeleteWorkflowExecutionResponse{}, nil)

	doContinue, err := s.task.Resend(context.Background(), remoteCluster, resendErr, ResendAttempt)
	s.NoError(err)
	s.False(doContinue)
}

func (s *executableTaskSuite) TestResend_ResendError_Success() {
	remoteCluster := cluster.TestAlternativeClusterName
	resendErr := &serviceerrors.RetryReplication{
		NamespaceId:       uuid.NewString(),
		WorkflowId:        uuid.NewString(),
		RunId:             uuid.NewString(),
		StartEventId:      rand.Int63(),
		StartEventVersion: rand.Int63(),
		EndEventId:        rand.Int63(),
		EndEventVersion:   rand.Int63(),
	}

	anotherResendErr := &serviceerrors.RetryReplication{
		NamespaceId:       resendErr.NamespaceId,
		WorkflowId:        resendErr.WorkflowId,
		RunId:             uuid.NewString(),
		StartEventId:      rand.Int63(),
		StartEventVersion: rand.Int63(),
		EndEventId:        rand.Int63(),
		EndEventVersion:   rand.Int63(),
	}

	gomock.InOrder(
		s.resendHandler.EXPECT().ResendHistoryEvents(
			gomock.Any(),
			remoteCluster,
			namespace.ID(resendErr.NamespaceId),
			resendErr.WorkflowId,
			resendErr.RunId,
			resendErr.StartEventId,
			resendErr.StartEventVersion,
			resendErr.EndEventId,
			resendErr.EndEventVersion,
		).Return(anotherResendErr),
		s.resendHandler.EXPECT().ResendHistoryEvents(
			gomock.Any(),
			remoteCluster,
			namespace.ID(anotherResendErr.NamespaceId),
			anotherResendErr.WorkflowId,
			anotherResendErr.RunId,
			anotherResendErr.StartEventId,
			anotherResendErr.StartEventVersion,
			anotherResendErr.EndEventId,
			anotherResendErr.EndEventVersion,
		).Return(nil),
		s.resendHandler.EXPECT().ResendHistoryEvents(
			gomock.Any(),
			remoteCluster,
			namespace.ID(resendErr.NamespaceId),
			resendErr.WorkflowId,
			resendErr.RunId,
			resendErr.StartEventId,
			resendErr.StartEventVersion,
			resendErr.EndEventId,
			resendErr.EndEventVersion,
		).Return(nil),
	)

	doContinue, err := s.task.Resend(context.Background(), remoteCluster, resendErr, ResendAttempt)
	s.NoError(err)
	s.True(doContinue)
}

func (s *executableTaskSuite) TestResend_ResendError_Error() {
	remoteCluster := cluster.TestAlternativeClusterName
	resendErr := &serviceerrors.RetryReplication{
		NamespaceId:       uuid.NewString(),
		WorkflowId:        uuid.NewString(),
		RunId:             uuid.NewString(),
		StartEventId:      rand.Int63(),
		StartEventVersion: rand.Int63(),
		EndEventId:        rand.Int63(),
		EndEventVersion:   rand.Int63(),
	}

	anotherResendErr := &serviceerrors.RetryReplication{
		NamespaceId:       resendErr.NamespaceId,
		WorkflowId:        resendErr.WorkflowId,
		RunId:             uuid.NewString(),
		StartEventId:      rand.Int63(),
		StartEventVersion: rand.Int63(),
		EndEventId:        rand.Int63(),
		EndEventVersion:   rand.Int63(),
	}

	gomock.InOrder(
		s.resendHandler.EXPECT().ResendHistoryEvents(
			gomock.Any(),
			remoteCluster,
			namespace.ID(resendErr.NamespaceId),
			resendErr.WorkflowId,
			resendErr.RunId,
			resendErr.StartEventId,
			resendErr.StartEventVersion,
			resendErr.EndEventId,
			resendErr.EndEventVersion,
		).Return(anotherResendErr),
		s.resendHandler.EXPECT().ResendHistoryEvents(
			gomock.Any(),
			remoteCluster,
			namespace.ID(anotherResendErr.NamespaceId),
			anotherResendErr.WorkflowId,
			anotherResendErr.RunId,
			anotherResendErr.StartEventId,
			anotherResendErr.StartEventVersion,
			anotherResendErr.EndEventId,
			anotherResendErr.EndEventVersion,
		).Return(&serviceerrors.RetryReplication{}),
	)

	doContinue, err := s.task.Resend(context.Background(), remoteCluster, resendErr, ResendAttempt)
	s.Error(err)
	s.False(doContinue)
}

func (s *executableTaskSuite) TestResend_SecondResendError_SameWorkflowRun() {
	remoteCluster := cluster.TestAlternativeClusterName
	resendErr := &serviceerrors.RetryReplication{
		NamespaceId:       uuid.NewString(),
		WorkflowId:        uuid.NewString(),
		RunId:             uuid.NewString(),
		StartEventId:      rand.Int63(),
		StartEventVersion: rand.Int63(),
		EndEventId:        rand.Int63(),
		EndEventVersion:   rand.Int63(),
	}

	anotherResendErr := &serviceerrors.RetryReplication{
		NamespaceId:       resendErr.NamespaceId,
		WorkflowId:        resendErr.WorkflowId,
		RunId:             resendErr.RunId,
		StartEventId:      resendErr.StartEventId,
		StartEventVersion: resendErr.StartEventVersion,
		EndEventId:        resendErr.EndEventId,
		EndEventVersion:   resendErr.EndEventVersion,
	}

	s.resendHandler.EXPECT().ResendHistoryEvents(
		gomock.Any(),
		remoteCluster,
		namespace.ID(resendErr.NamespaceId),
		resendErr.WorkflowId,
		resendErr.RunId,
		resendErr.StartEventId,
		resendErr.StartEventVersion,
		resendErr.EndEventId,
		resendErr.EndEventVersion,
	).Return(anotherResendErr)

	doContinue, err := s.task.Resend(context.Background(), remoteCluster, resendErr, ResendAttempt)
	var dataLossErr *serviceerror.DataLoss
	s.ErrorAs(err, &dataLossErr)
	s.False(doContinue)
}

func (s *executableTaskSuite) TestResend_Error() {
	remoteCluster := cluster.TestAlternativeClusterName
	resendErr := &serviceerrors.RetryReplication{
		NamespaceId:       uuid.NewString(),
		WorkflowId:        uuid.NewString(),
		RunId:             uuid.NewString(),
		StartEventId:      rand.Int63(),
		StartEventVersion: rand.Int63(),
		EndEventId:        rand.Int63(),
		EndEventVersion:   rand.Int63(),
	}

	s.resendHandler.EXPECT().ResendHistoryEvents(
		gomock.Any(),
		remoteCluster,
		namespace.ID(resendErr.NamespaceId),
		resendErr.WorkflowId,
		resendErr.RunId,
		resendErr.StartEventId,
		resendErr.StartEventVersion,
		resendErr.EndEventId,
		resendErr.EndEventVersion,
	).Return(serviceerror.NewUnavailable(""))

	doContinue, err := s.task.Resend(context.Background(), remoteCluster, resendErr, ResendAttempt)
	s.Error(err)
	s.False(doContinue)
}

func (s *executableTaskSuite) TestResend_TransitionHistoryDisabled() {
	syncStateErr := &serviceerrors.SyncState{
		NamespaceId: uuid.NewString(),
		WorkflowId:  uuid.NewString(),
		RunId:       uuid.NewString(),
		ArchetypeId: chasm.WorkflowArchetypeID,
		VersionedTransition: &persistencespb.VersionedTransition{
			NamespaceFailoverVersion: rand.Int63(),
			TransitionCount:          rand.Int63(),
		},
		VersionHistories: &historyspb.VersionHistories{
			Histories: []*historyspb.VersionHistory{
				{
					BranchToken: []byte("token#1"),
					Items: []*historyspb.VersionHistoryItem{
						{EventId: 102, Version: 1234},
					},
				},
			},
		},
	}

	mockRemoteAdminClient := adminservicemock.NewMockAdminServiceClient(s.controller)
	s.clientBean.EXPECT().GetRemoteAdminClient(s.sourceCluster).Return(mockRemoteAdminClient, nil).AnyTimes()

	mockRemoteAdminClient.EXPECT().SyncWorkflowState(
		gomock.Any(),
		&adminservice.SyncWorkflowStateRequest{
			NamespaceId: syncStateErr.NamespaceId,
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: syncStateErr.WorkflowId,
				RunId:      syncStateErr.RunId,
			},
			ArchetypeId:         syncStateErr.ArchetypeId,
			VersionedTransition: syncStateErr.VersionedTransition,
			VersionHistories: &historyspb.VersionHistories{
				Histories: []*historyspb.VersionHistory{
					{
						// BranchToken is removed in the actual implementation
						Items: []*historyspb.VersionHistoryItem{
							{EventId: 102, Version: 1234},
						},
					},
				},
			},
			TargetClusterId: int32(s.clusterMetadata.GetAllClusterInfo()[s.clusterMetadata.GetCurrentClusterName()].InitialFailoverVersion),
		},
	).Return(nil, consts.ErrTransitionHistoryDisabled).Times(1)

	mockRemoteAdminClient.EXPECT().AddTasks(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, request *adminservice.AddTasksRequest, opts ...grpc.CallOption) (*adminservice.AddTasksResponse, error) {
			s.Equal(s.sourceShardKey.ShardID, request.ShardId)
			s.Len(request.Tasks, len(s.task.replicationTask.GetRawTaskInfo().TaskEquivalents))
			for _, task := range request.Tasks {
				s.Equal(tasks.CategoryIDReplication, int(task.CategoryId))
			}
			return nil, nil
		},
	)

	doContinue, err := s.task.SyncState(context.Background(), syncStateErr, ResendAttempt)
	s.NoError(err)
	s.False(doContinue)
}

func (s *executableTaskSuite) TestSyncState_SourceMutableStateHasUnFlushedBufferEvents() {
	syncStateErr := &serviceerrors.SyncState{
		NamespaceId: uuid.NewString(),
		WorkflowId:  uuid.NewString(),
		RunId:       uuid.NewString(),
		ArchetypeId: chasm.WorkflowArchetypeID,
		VersionedTransition: &persistencespb.VersionedTransition{
			NamespaceFailoverVersion: rand.Int63(),
			TransitionCount:          rand.Int63(),
		},
		VersionHistories: &historyspb.VersionHistories{
			Histories: []*historyspb.VersionHistory{
				{
					BranchToken: []byte("token#1"),
					Items: []*historyspb.VersionHistoryItem{
						{EventId: 102, Version: 1234},
					},
				},
			},
		},
	}

	mockRemoteAdminClient := adminservicemock.NewMockAdminServiceClient(s.controller)
	s.clientBean.EXPECT().GetRemoteAdminClient(s.sourceCluster).Return(mockRemoteAdminClient, nil).AnyTimes()

	mockRemoteAdminClient.EXPECT().SyncWorkflowState(
		gomock.Any(),
		&adminservice.SyncWorkflowStateRequest{
			NamespaceId: syncStateErr.NamespaceId,
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: syncStateErr.WorkflowId,
				RunId:      syncStateErr.RunId,
			},
			ArchetypeId:         chasm.WorkflowArchetypeID,
			VersionedTransition: syncStateErr.VersionedTransition,
			VersionHistories: &historyspb.VersionHistories{
				Histories: []*historyspb.VersionHistory{
					{
						// BranchToken is removed in the actual implementation
						Items: []*historyspb.VersionHistoryItem{
							{EventId: 102, Version: 1234},
						},
					},
				},
			},
			TargetClusterId: int32(s.clusterMetadata.GetAllClusterInfo()[s.clusterMetadata.GetCurrentClusterName()].InitialFailoverVersion),
		},
	).Return(nil, serviceerror.NewWorkflowNotReady("workflow not ready")).Times(1)

	doContinue, err := s.task.SyncState(context.Background(), syncStateErr, ResendAttempt)
	s.NoError(err)
	s.False(doContinue)
}

func (s *executableTaskSuite) TestBackFillEvents_Success() {
	workflowKey := definition.NewWorkflowKey(
		s.namespaceId,
		s.workflowId,
		s.runId,
	)

	startEventId := int64(20)
	startEventVersion := int64(10)
	endEventId := int64(21)
	endEventVersion := int64(12)
	newRunId := uuid.NewString()
	remoteCluster := "remote cluster"
	eventBatchOriginal1 := []*historypb.HistoryEvent{
		{EventId: 20, Version: 10},
	}
	eventBatchOriginal2 := []*historypb.HistoryEvent{
		{EventId: 21, Version: 12},
	}
	blogOriginal1, err := s.serializer.SerializeEvents(eventBatchOriginal1)
	s.NoError(err)

	blogOriginal2, err := s.serializer.SerializeEvents(eventBatchOriginal2)
	s.NoError(err)
	versionHistory := &historyspb.VersionHistory{
		Items: []*historyspb.VersionHistoryItem{
			{EventId: 20, Version: 10},
			{EventId: 28, Version: 12},
		},
	}
	fetcher := collection.NewPagingIterator(func(paginationToken []byte) ([]*eventhandler.HistoryBatch, []byte, error) {
		return []*eventhandler.HistoryBatch{
			{RawEventBatch: blogOriginal1, VersionHistory: versionHistory},
			{RawEventBatch: blogOriginal2, VersionHistory: versionHistory},
		}, nil, nil
	})
	eventBatchNewRun := []*historypb.HistoryEvent{
		{EventId: 1, Version: 12},
		{EventId: 2, Version: 12},
	}
	blobNewRun, err := s.serializer.SerializeEvents(eventBatchNewRun)
	s.NoError(err)
	fetcherNewRun := collection.NewPagingIterator(func(paginationToken []byte) ([]*eventhandler.HistoryBatch, []byte, error) {
		return []*eventhandler.HistoryBatch{
			{RawEventBatch: blobNewRun, VersionHistory: &historyspb.VersionHistory{
				Items: []*historyspb.VersionHistoryItem{
					{EventId: 4, Version: 12},
				},
			}},
		}, nil, nil
	})
	s.remoteHistoryFetcher.EXPECT().GetSingleWorkflowHistoryPaginatedIteratorInclusive(
		gomock.Any(),
		remoteCluster,
		namespace.ID(workflowKey.NamespaceID),
		workflowKey.WorkflowID,
		newRunId,
		int64(1),
		endEventVersion,
		int64(1),
		endEventVersion,
	).Return(fetcherNewRun)
	s.remoteHistoryFetcher.EXPECT().GetSingleWorkflowHistoryPaginatedIteratorInclusive(
		gomock.Any(),
		remoteCluster,
		namespace.ID(workflowKey.NamespaceID),
		workflowKey.WorkflowID,
		workflowKey.RunID,
		startEventId,
		startEventVersion,
		endEventId,
		endEventVersion,
	).Return(fetcher)
	shardContext := historyi.NewMockShardContext(s.controller)
	engine := historyi.NewMockEngine(s.controller)
	s.shardController.EXPECT().GetShardByNamespaceWorkflow(
		namespace.ID(workflowKey.NamespaceID),
		workflowKey.WorkflowID,
	).Return(shardContext, nil).AnyTimes()
	shardContext.EXPECT().GetEngine(gomock.Any()).Return(engine, nil).AnyTimes()
	engine.EXPECT().BackfillHistoryEvents(
		gomock.Any(), protomock.Eq(&historyi.BackfillHistoryEventsRequest{
			WorkflowKey:         workflowKey,
			SourceClusterName:   s.sourceCluster,
			VersionedHistory:    s.task.replicationTask.VersionedTransition,
			VersionHistoryItems: versionHistory.Items,
			Events:              [][]*historypb.HistoryEvent{eventBatchOriginal1},
		})).Return(nil)
	engine.EXPECT().BackfillHistoryEvents(
		gomock.Any(), protomock.Eq(&historyi.BackfillHistoryEventsRequest{
			WorkflowKey:         workflowKey,
			SourceClusterName:   s.sourceCluster,
			VersionedHistory:    s.task.replicationTask.VersionedTransition,
			VersionHistoryItems: versionHistory.Items,
			Events:              [][]*historypb.HistoryEvent{eventBatchOriginal2},
			NewRunID:            newRunId,
			NewEvents:           eventBatchNewRun,
		})).Return(nil)
	task := NewExecutableTask(
		s.toolBox,
		s.taskId,
		"metrics-tag",
		time.Now(),
		time.Now(),
		s.sourceCluster,
		s.sourceShardKey,
		&replicationspb.ReplicationTask{
			TaskType: enumsspb.REPLICATION_TASK_TYPE_VERIFY_VERSIONED_TRANSITION_TASK,
			RawTaskInfo: &persistencespb.ReplicationTaskInfo{
				NamespaceId: s.namespaceId,
				WorkflowId:  s.workflowId,
				RunId:       s.runId,
				TaskId:      s.taskId,
				TaskEquivalents: []*persistencespb.ReplicationTaskInfo{
					{NamespaceId: s.namespaceId, WorkflowId: s.workflowId, RunId: s.runId},
					{NamespaceId: s.namespaceId, WorkflowId: s.workflowId, RunId: s.runId},
				},
			},
		},
	)
	err = task.BackFillEvents(
		context.Background(),
		remoteCluster,
		workflowKey,
		startEventId,
		startEventVersion,
		endEventId,
		endEventVersion,
		newRunId,
	)
	s.NoError(err)
}

func (s *executableTaskSuite) TestGetNamespaceInfo_Process() {
	namespaceID := uuid.NewString()
	namespaceName := uuid.NewString()
	factory := namespace.NewDefaultReplicationResolverFactory()
	detail := &persistencespb.NamespaceDetail{
		Info: &persistencespb.NamespaceInfo{
			Id:   namespaceID,
			Name: namespaceName,
		},
		Config: &persistencespb.NamespaceConfig{},
		ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestAlternativeClusterName,
			Clusters: []string{
				cluster.TestCurrentClusterName,
				cluster.TestAlternativeClusterName,
			},
		},
	}
	namespaceEntry, err := namespace.FromPersistentState(detail, factory(detail))
	s.NoError(err)
	s.namespaceCache.EXPECT().GetNamespaceByID(namespace.ID(namespaceID)).Return(namespaceEntry, nil).AnyTimes()

	name, toProcess, err := s.task.GetNamespaceInfo(context.Background(), namespaceID, "test-workflow-id")
	s.NoError(err)
	s.Equal(namespaceName, name)
	s.True(toProcess)
}

func (s *executableTaskSuite) TestGetNamespaceInfo_Skip() {
	namespaceID := uuid.NewString()
	namespaceName := uuid.NewString()
	factory := namespace.NewDefaultReplicationResolverFactory()
	detail := &persistencespb.NamespaceDetail{
		Info: &persistencespb.NamespaceInfo{
			Id:   namespaceID,
			Name: namespaceName,
		},
		Config: &persistencespb.NamespaceConfig{},
		ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestAlternativeClusterName,
			Clusters: []string{
				cluster.TestAlternativeClusterName,
			},
		},
	}
	namespaceEntry, err := namespace.FromPersistentState(detail, factory(detail))
	s.NoError(err)
	s.namespaceCache.EXPECT().GetNamespaceByID(namespace.ID(namespaceID)).Return(namespaceEntry, nil).AnyTimes()

	name, toProcess, err := s.task.GetNamespaceInfo(context.Background(), namespaceID, "test-workflow-id")
	s.NoError(err)
	s.Equal(namespaceName, name)
	s.False(toProcess)
}

func (s *executableTaskSuite) TestGetNamespaceInfo_Deleted() {
	namespaceID := uuid.NewString()
	namespaceName := uuid.NewString()
	factory := namespace.NewDefaultReplicationResolverFactory()
	detail := &persistencespb.NamespaceDetail{
		Info: &persistencespb.NamespaceInfo{
			Id:    namespaceID,
			Name:  namespaceName,
			State: enumspb.NAMESPACE_STATE_DELETED,
		},
		Config: &persistencespb.NamespaceConfig{},
		ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestAlternativeClusterName,
			Clusters: []string{
				cluster.TestCurrentClusterName,
				cluster.TestAlternativeClusterName,
			},
		},
	}
	namespaceEntry, err := namespace.FromPersistentState(detail, factory(detail))
	s.NoError(err)
	s.namespaceCache.EXPECT().GetNamespaceByID(namespace.ID(namespaceID)).Return(namespaceEntry, nil).AnyTimes()

	name, toProcess, err := s.task.GetNamespaceInfo(context.Background(), namespaceID, "test-workflow-id")
	s.NoError(err)
	s.Equal(namespaceName, name)
	s.False(toProcess)
}

func (s *executableTaskSuite) TestGetNamespaceInfo_Error() {
	namespaceID := uuid.NewString()
	s.namespaceCache.EXPECT().GetNamespaceByID(namespace.ID(namespaceID)).Return(nil, errors.New("OwO")).AnyTimes()

	_, _, err := s.task.GetNamespaceInfo(context.Background(), namespaceID, "test-workflow-id")
	s.Error(err)
}

func (s *executableTaskSuite) TestGetNamespaceInfo_NotFoundOnCurrentCluster_SyncFromRemoteSuccess() {
	namespaceID := uuid.NewString()
	namespaceName := uuid.NewString()
	factory := namespace.NewDefaultReplicationResolverFactory()
	detail := &persistencespb.NamespaceDetail{
		Info: &persistencespb.NamespaceInfo{
			Id:   namespaceID,
			Name: namespaceName,
		},
		Config: &persistencespb.NamespaceConfig{},
		ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestAlternativeClusterName,
			Clusters: []string{
				cluster.TestCurrentClusterName,
				cluster.TestAlternativeClusterName,
			},
		},
	}
	namespaceEntry, err := namespace.FromPersistentState(detail, factory(detail))
	s.NoError(err)
	// enable feature flag

	s.namespaceCache.EXPECT().GetNamespaceByID(namespace.ID(namespaceID)).Return(nil, serviceerror.NewNamespaceNotFound("namespace not found")).Times(1)
	s.namespaceCache.EXPECT().GetNamespaceByID(namespace.ID(namespaceID)).Return(namespaceEntry, nil).Times(1)
	s.eagerNamespaceRefresher.EXPECT().SyncNamespaceFromSourceCluster(gomock.Any(), namespace.ID(namespaceID), gomock.Any()).Return(
		namespaceEntry, nil)

	name, toProcess, err := s.task.GetNamespaceInfo(context.Background(), namespaceID, "test-workflow-id")
	s.NoError(err)
	s.Equal(namespaceName, name)
	s.True(toProcess)
}

func (s *executableTaskSuite) TestGetNamespaceInfo_NamespaceFailoverNotSync_SyncFromRemoteSuccess() {
	namespaceID := uuid.NewString()
	namespaceName := uuid.NewString()
	now := time.Now()
	s.task = NewExecutableTask(
		ProcessToolBox{
			Config:                  s.config,
			ClusterMetadata:         s.clusterMetadata,
			ClientBean:              s.clientBean,
			ShardController:         s.shardController,
			NamespaceCache:          s.namespaceCache,
			ResendHandler:           s.resendHandler,
			MetricsHandler:          s.metricsHandler,
			Logger:                  s.logger,
			EagerNamespaceRefresher: s.eagerNamespaceRefresher,
			DLQWriter:               NoopDLQWriter{},
		},
		rand.Int63(),
		"metrics-tag",
		now,
		now,
		s.sourceCluster,
		s.sourceShardKey,
		&replicationspb.ReplicationTask{
			TaskType:            enumsspb.REPLICATION_TASK_TYPE_NAMESPACE_TASK,
			VersionedTransition: &persistencespb.VersionedTransition{NamespaceFailoverVersion: 80},
		},
	)
	factory := namespace.NewDefaultReplicationResolverFactory()
	detailOld := &persistencespb.NamespaceDetail{
		Info: &persistencespb.NamespaceInfo{
			Id:   namespaceID,
			Name: namespaceName,
		},
		Config: &persistencespb.NamespaceConfig{},
		ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestAlternativeClusterName,
			Clusters: []string{
				cluster.TestCurrentClusterName,
				cluster.TestAlternativeClusterName,
			},
		},
		FailoverVersion: 10,
	}
	namespaceEntryOld, err := namespace.FromPersistentState(detailOld, factory(detailOld))
	s.NoError(err)
	detailNew := &persistencespb.NamespaceDetail{
		Info: &persistencespb.NamespaceInfo{
			Id:   namespaceID,
			Name: namespaceName,
		},
		Config: &persistencespb.NamespaceConfig{},
		ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestAlternativeClusterName,
			Clusters: []string{
				cluster.TestCurrentClusterName,
				cluster.TestAlternativeClusterName,
			},
		},
		FailoverVersion: 100,
	}
	namespaceEntryNew, err := namespace.FromPersistentState(detailNew, factory(detailNew))
	s.NoError(err)

	s.namespaceCache.EXPECT().GetNamespaceByID(namespace.ID(namespaceID)).Return(namespaceEntryOld, nil).Times(1)
	s.namespaceCache.EXPECT().GetNamespaceByID(namespace.ID(namespaceID)).Return(namespaceEntryNew, nil).Times(1)
	s.eagerNamespaceRefresher.EXPECT().SyncNamespaceFromSourceCluster(gomock.Any(), namespace.ID(namespaceID), gomock.Any()).Return(
		namespaceEntryNew, nil)

	name, toProcess, err := s.task.GetNamespaceInfo(context.Background(), namespaceID, "test-workflow-id")
	s.NoError(err)
	s.Equal(namespaceName, name)
	s.True(toProcess)
}

func (s *executableTaskSuite) TestGetNamespaceInfo_NamespaceFailoverBehind_StillBehandAfterSyncFromRemote() {
	namespaceID := uuid.NewString()
	namespaceName := uuid.NewString()
	now := time.Now()
	s.task = NewExecutableTask(
		ProcessToolBox{
			Config:                  s.config,
			ClusterMetadata:         s.clusterMetadata,
			ClientBean:              s.clientBean,
			ShardController:         s.shardController,
			NamespaceCache:          s.namespaceCache,
			ResendHandler:           s.resendHandler,
			MetricsHandler:          s.metricsHandler,
			Logger:                  s.logger,
			EagerNamespaceRefresher: s.eagerNamespaceRefresher,
			DLQWriter:               NoopDLQWriter{},
		},
		rand.Int63(),
		"metrics-tag",
		now,
		now,
		s.sourceCluster,
		s.sourceShardKey,
		&replicationspb.ReplicationTask{
			TaskType:            enumsspb.REPLICATION_TASK_TYPE_NAMESPACE_TASK,
			VersionedTransition: &persistencespb.VersionedTransition{NamespaceFailoverVersion: 80},
		},
	)
	factory := namespace.NewDefaultReplicationResolverFactory()
	detailOld := &persistencespb.NamespaceDetail{
		Info: &persistencespb.NamespaceInfo{
			Id:   namespaceID,
			Name: namespaceName,
		},
		Config: &persistencespb.NamespaceConfig{},
		ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestAlternativeClusterName,
			Clusters: []string{
				cluster.TestCurrentClusterName,
				cluster.TestAlternativeClusterName,
			},
		},
		FailoverVersion: 10,
	}
	namespaceEntryOld, err := namespace.FromPersistentState(detailOld, factory(detailOld))
	s.NoError(err)

	s.namespaceCache.EXPECT().GetNamespaceByID(namespace.ID(namespaceID)).Return(namespaceEntryOld, nil).Times(1)
	s.namespaceCache.EXPECT().GetNamespaceByID(namespace.ID(namespaceID)).Return(namespaceEntryOld, nil).Times(1)
	s.eagerNamespaceRefresher.EXPECT().SyncNamespaceFromSourceCluster(gomock.Any(), namespace.ID(namespaceID), gomock.Any()).Return(
		namespaceEntryOld, nil)

	name, toProcess, err := s.task.GetNamespaceInfo(context.Background(), namespaceID, "test-workflow-id")
	s.Empty(name)
	s.Error(err)
	s.False(toProcess)
}

func (s *executableTaskSuite) TestGetNamespaceInfo_NotFoundOnCurrentCluster_SyncFromRemoteFailed() {
	namespaceID := uuid.NewString()

	s.namespaceCache.EXPECT().GetNamespaceByID(namespace.ID(namespaceID)).Return(nil, serviceerror.NewNamespaceNotFound("namespace not found")).AnyTimes()
	s.eagerNamespaceRefresher.EXPECT().SyncNamespaceFromSourceCluster(gomock.Any(), namespace.ID(namespaceID), gomock.Any()).Return(
		nil, errors.New("some error"))

	_, toProcess, err := s.task.GetNamespaceInfo(context.Background(), namespaceID, "test-workflow-id")
	s.NoError(err)
	s.False(toProcess)
}

func (s *executableTaskSuite) TestMarkPoisonPill() {
	shardID := rand.Int31()
	shardContext := historyi.NewMockShardContext(s.controller)
	s.shardController.EXPECT().GetShardByNamespaceWorkflow(
		namespace.ID(s.namespaceId),
		s.workflowId,
	).Return(shardContext, nil).AnyTimes()
	shardContext.EXPECT().GetShardID().Return(shardID).AnyTimes()
	s.mockExecutionManager.EXPECT().PutReplicationTaskToDLQ(gomock.Any(), &persistence.PutReplicationTaskToDLQRequest{
		ShardID:           shardID,
		SourceClusterName: s.task.sourceClusterName,
		TaskInfo:          s.task.replicationTask.RawTaskInfo,
	}).Return(nil)

	err := s.task.MarkPoisonPill()
	s.NoError(err)
}

func (s *executableTaskSuite) TestMarkPoisonPill_MaxAttemptsReached() {
	s.task.markPoisonPillAttempts = MarkPoisonPillMaxAttempts - 1
	shardID := rand.Int31()
	shardContext := historyi.NewMockShardContext(s.controller)
	s.shardController.EXPECT().GetShardByNamespaceWorkflow(
		namespace.ID(s.namespaceId),
		s.workflowId,
	).Return(shardContext, nil).AnyTimes()
	shardContext.EXPECT().GetShardID().Return(shardID).AnyTimes()
	s.mockExecutionManager.EXPECT().PutReplicationTaskToDLQ(gomock.Any(), &persistence.PutReplicationTaskToDLQRequest{
		ShardID:           shardID,
		SourceClusterName: s.task.sourceClusterName,
		TaskInfo:          s.task.replicationTask.RawTaskInfo,
	}).Return(serviceerror.NewInternal("failed"))

	err := s.task.MarkPoisonPill()
	s.Error(err)
	err = s.task.MarkPoisonPill()
	s.NoError(err)
}

func (s *executableTaskSuite) TestSyncState() {
	syncStateErr := &serviceerrors.SyncState{
		NamespaceId: uuid.NewString(),
		WorkflowId:  uuid.NewString(),
		RunId:       uuid.NewString(),
		ArchetypeId: chasm.WorkflowArchetypeID,
		VersionedTransition: &persistencespb.VersionedTransition{
			NamespaceFailoverVersion: rand.Int63(),
			TransitionCount:          rand.Int63(),
		},
		VersionHistories: &historyspb.VersionHistories{
			Histories: []*historyspb.VersionHistory{
				{
					BranchToken: []byte("token#1"),
					Items: []*historyspb.VersionHistoryItem{
						{EventId: 102, Version: 1234},
					},
				},
			},
		},
	}

	versionedTransitionArtifact := &replicationspb.VersionedTransitionArtifact{
		StateAttributes: &replicationspb.VersionedTransitionArtifact_SyncWorkflowStateSnapshotAttributes{
			SyncWorkflowStateSnapshotAttributes: &replicationspb.SyncWorkflowStateSnapshotAttributes{
				State: &persistencespb.WorkflowMutableState{
					Checksum: &persistencespb.Checksum{
						Value: []byte("test-checksum"),
					},
				},
			},
		},
	}

	mockRemoteAdminClient := adminservicemock.NewMockAdminServiceClient(s.controller)
	s.clientBean.EXPECT().GetRemoteAdminClient(s.sourceCluster).Return(mockRemoteAdminClient, nil).AnyTimes()
	mockRemoteAdminClient.EXPECT().SyncWorkflowState(
		gomock.Any(),
		&adminservice.SyncWorkflowStateRequest{
			NamespaceId: syncStateErr.NamespaceId,
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: syncStateErr.WorkflowId,
				RunId:      syncStateErr.RunId,
			},
			ArchetypeId:         chasm.WorkflowArchetypeID,
			VersionedTransition: syncStateErr.VersionedTransition,
			VersionHistories: &historyspb.VersionHistories{
				Histories: []*historyspb.VersionHistory{
					{
						// BranchToken is removed in the actual implementation
						Items: []*historyspb.VersionHistoryItem{
							{EventId: 102, Version: 1234},
						},
					},
				},
			},
			TargetClusterId: int32(s.clusterMetadata.GetAllClusterInfo()[s.clusterMetadata.GetCurrentClusterName()].InitialFailoverVersion),
		},
	).Return(&adminservice.SyncWorkflowStateResponse{
		VersionedTransitionArtifact: versionedTransitionArtifact,
	}, nil).Times(1)

	shardContext := historyi.NewMockShardContext(s.controller)
	engine := historyi.NewMockEngine(s.controller)
	s.shardController.EXPECT().GetShardByNamespaceWorkflow(
		namespace.ID(syncStateErr.NamespaceId),
		syncStateErr.WorkflowId,
	).Return(shardContext, nil).AnyTimes()
	shardContext.EXPECT().GetEngine(gomock.Any()).Return(engine, nil).AnyTimes()
	engine.EXPECT().ReplicateVersionedTransition(gomock.Any(), chasm.WorkflowArchetypeID, versionedTransitionArtifact, s.sourceCluster).Return(nil)

	doContinue, err := s.task.SyncState(context.Background(), syncStateErr, ResendAttempt)
	s.NoError(err)
	s.True(doContinue)
}

func (s *executableTaskSuite) TestSourceShardKey() {
	s.Equal(s.sourceShardKey, s.task.SourceShardKey())
}

func (s *executableTaskSuite) TestRetryPolicy() {
	policy := s.task.RetryPolicy()
	s.NotNil(policy)
	// computing next interval should not panic and yields a non-negative backoff
	next := policy.ComputeNextDelay(0, 1, nil)
	s.GreaterOrEqual(next, time.Duration(0))
}

func (s *executableTaskSuite) TestGetPriority() {
	s.Equal(s.task.taskPriority, s.task.GetPriority())
}

func (s *executableTaskSuite) TestMarkTaskDuplicated() {
	s.False(s.task.isDuplicated)
	s.task.MarkTaskDuplicated()
	s.True(s.task.isDuplicated)
}

func (s *executableTaskSuite) TestMarkExecutionStart() {
	s.True(s.task.taskExecuteStartTime.IsZero())
	s.task.MarkExecutionStart()
	first := s.task.taskExecuteStartTime
	s.False(first.IsZero())
	// second call should be a no-op (does not overwrite)
	s.task.MarkExecutionStart()
	s.Equal(first, s.task.taskExecuteStartTime)
}

func (s *executableTaskSuite) TestNamespaceName() {
	// before namespace stored
	s.Empty(s.task.NamespaceName())
	// after namespace stored
	execTaskNsName := namespace.Name("execTask-ns-name")
	s.task.namespace.Store(execTaskNsName)
	s.Equal(execTaskNsName.String(), s.task.NamespaceName())
}

func (s *executableTaskSuite) TestWorkflowKeyFromTask_NilTask() {
	execTask := NewExecutableTask(
		s.toolBox,
		s.taskId,
		"metrics-tag",
		time.Now(),
		time.Now(),
		s.sourceCluster,
		s.sourceShardKey,
		&replicationspb.ReplicationTask{},
	)
	execTask.replicationTask = nil
	nsID, wfID, runID := execTask.workflowKeyFromTask()
	s.Empty(nsID)
	s.Empty(wfID)
	s.Empty(runID)
}

func (s *executableTaskSuite) TestWorkflowKeyFromTask_RawTaskInfo() {
	nsID, wfID, runID := s.task.workflowKeyFromTask()
	s.Equal(s.namespaceId, nsID)
	s.Equal(s.workflowId, wfID)
	s.Equal(s.runId, runID)
}

func (s *executableTaskSuite) execTaskWithTask(task *replicationspb.ReplicationTask) *ExecutableTaskImpl {
	return NewExecutableTask(
		s.toolBox,
		s.taskId,
		"metrics-tag",
		time.Now(),
		time.Now(),
		s.sourceCluster,
		s.sourceShardKey,
		task,
	)
}

func (s *executableTaskSuite) TestWorkflowKeyFromTask_Attributes() {
	execNs := uuid.NewString()
	execWf := uuid.NewString()
	execRun := uuid.NewString()

	cases := []*replicationspb.ReplicationTask{
		{Attributes: &replicationspb.ReplicationTask_SyncWorkflowStateTaskAttributes{
			SyncWorkflowStateTaskAttributes: &replicationspb.SyncWorkflowStateTaskAttributes{
				WorkflowState: &persistencespb.WorkflowMutableState{
					ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
						NamespaceId: execNs,
						WorkflowId:  execWf,
					},
					ExecutionState: &persistencespb.WorkflowExecutionState{
						RunId: execRun,
					},
				},
			},
		}},
		{Attributes: &replicationspb.ReplicationTask_SyncActivityTaskAttributes{
			SyncActivityTaskAttributes: &replicationspb.SyncActivityTaskAttributes{
				NamespaceId: execNs, WorkflowId: execWf, RunId: execRun,
			},
		}},
		{Attributes: &replicationspb.ReplicationTask_HistoryTaskAttributes{
			HistoryTaskAttributes: &replicationspb.HistoryTaskAttributes{
				NamespaceId: execNs, WorkflowId: execWf, RunId: execRun,
			},
		}},
		{Attributes: &replicationspb.ReplicationTask_SyncHsmAttributes{
			SyncHsmAttributes: &replicationspb.SyncHSMAttributes{
				NamespaceId: execNs, WorkflowId: execWf, RunId: execRun,
			},
		}},
		{Attributes: &replicationspb.ReplicationTask_BackfillHistoryTaskAttributes{
			BackfillHistoryTaskAttributes: &replicationspb.BackfillHistoryTaskAttributes{
				NamespaceId: execNs, WorkflowId: execWf, RunId: execRun,
			},
		}},
		{Attributes: &replicationspb.ReplicationTask_VerifyVersionedTransitionTaskAttributes{
			VerifyVersionedTransitionTaskAttributes: &replicationspb.VerifyVersionedTransitionTaskAttributes{
				NamespaceId: execNs, WorkflowId: execWf, RunId: execRun,
			},
		}},
		{Attributes: &replicationspb.ReplicationTask_SyncVersionedTransitionTaskAttributes{
			SyncVersionedTransitionTaskAttributes: &replicationspb.SyncVersionedTransitionTaskAttributes{
				NamespaceId: execNs, WorkflowId: execWf, RunId: execRun,
			},
		}},
	}

	for _, task := range cases {
		execTask := s.execTaskWithTask(task)
		nsID, wfID, runID := execTask.workflowKeyFromTask()
		s.Equal(execNs, nsID)
		s.Equal(execWf, wfID)
		s.Equal(execRun, runID)
	}
}

func (s *executableTaskSuite) TestWorkflowKeyFromTask_UnknownAttributes() {
	execTask := s.execTaskWithTask(&replicationspb.ReplicationTask{
		Attributes: &replicationspb.ReplicationTask_SyncShardStatusTaskAttributes{
			SyncShardStatusTaskAttributes: &replicationspb.SyncShardStatusTaskAttributes{},
		},
	})
	nsID, wfID, runID := execTask.workflowKeyFromTask()
	s.Empty(nsID)
	s.Empty(wfID)
	s.Empty(runID)
}

func (s *executableTaskSuite) TestEmitFinishMetrics_Duplicated() {
	// duplicated path: should short-circuit, exercising workflowKeyFromTask + duplicated metric
	s.task.MarkTaskDuplicated()
	s.task.emitFinishMetrics(time.Now().UTC())
}

func (s *executableTaskSuite) TestEmitFinishMetrics_ExecutionStarted_HighLatency() {
	// store a namespace so the nsTag branch is exercised
	s.task.namespace.Store(namespace.Name("execTask-ns"))
	// set received/creation/execution-start times so that processing latency > 10s,
	// triggering the slow-task warning branch (which also calls workflowKeyFromTask).
	now := time.Now().UTC()
	s.task.taskCreationTime = now.Add(-time.Hour)
	s.task.taskReceivedTime = now.Add(-30 * time.Minute)
	s.task.taskExecuteStartTime = now.Add(-20 * time.Minute)
	s.task.emitFinishMetrics(now)
}

func (s *executableTaskSuite) TestAck_EmitsMetricsWithExecutionStart() {
	s.task.namespace.Store(namespace.Name("execTask-ns"))
	s.task.MarkExecutionStart()
	s.task.Ack()
	s.Equal(ctasks.TaskStateAcked, s.task.State())
}

func (s *executableTaskSuite) TestNack_EmitsFailedMetricWithNamespace() {
	s.task.namespace.Store(namespace.Name("execTask-ns"))
	s.task.Nack(serviceerror.NewInternal("boom"))
	s.Equal(ctasks.TaskStateNacked, s.task.State())
}

func (s *executableTaskSuite) TestResend_AttemptsExceeded() {
	remoteCluster := cluster.TestAlternativeClusterName
	resendErr := &serviceerrors.RetryReplication{
		NamespaceId: uuid.NewString(),
		WorkflowId:  uuid.NewString(),
		RunId:       uuid.NewString(),
	}
	doContinue, err := s.task.Resend(context.Background(), remoteCluster, resendErr, 0)
	s.ErrorIs(err, ErrResendAttemptExceeded)
	s.False(doContinue)
}

func (s *executableTaskSuite) TestDeleteWorkflow_GetShardError() {
	workflowKey := definition.NewWorkflowKey(s.namespaceId, s.workflowId, s.runId)
	s.shardController.EXPECT().GetShardByNamespaceWorkflow(
		namespace.ID(workflowKey.NamespaceID),
		workflowKey.WorkflowID,
	).Return(nil, serviceerror.NewUnavailable("shard unavailable"))

	err := s.task.DeleteWorkflow(context.Background(), workflowKey)
	s.Error(err)
}

func (s *executableTaskSuite) TestDeleteWorkflow_GetEngineError() {
	workflowKey := definition.NewWorkflowKey(s.namespaceId, s.workflowId, s.runId)
	shardContext := historyi.NewMockShardContext(s.controller)
	s.shardController.EXPECT().GetShardByNamespaceWorkflow(
		namespace.ID(workflowKey.NamespaceID),
		workflowKey.WorkflowID,
	).Return(shardContext, nil)
	shardContext.EXPECT().GetEngine(gomock.Any()).Return(nil, serviceerror.NewUnavailable("engine unavailable"))

	err := s.task.DeleteWorkflow(context.Background(), workflowKey)
	s.Error(err)
}

func (s *executableTaskSuite) TestDeleteWorkflow_NotFound() {
	workflowKey := definition.NewWorkflowKey(s.namespaceId, s.workflowId, s.runId)
	shardContext := historyi.NewMockShardContext(s.controller)
	engine := historyi.NewMockEngine(s.controller)
	s.shardController.EXPECT().GetShardByNamespaceWorkflow(
		namespace.ID(workflowKey.NamespaceID),
		workflowKey.WorkflowID,
	).Return(shardContext, nil)
	shardContext.EXPECT().GetEngine(gomock.Any()).Return(engine, nil)
	engine.EXPECT().DeleteWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil, serviceerror.NewNotFound("not found"))

	// NotFound is swallowed and treated as success.
	err := s.task.DeleteWorkflow(context.Background(), workflowKey)
	s.NoError(err)
}

func (s *executableTaskSuite) TestSyncState_GetRemoteAdminClientError() {
	syncStateErr := &serviceerrors.SyncState{
		NamespaceId: uuid.NewString(),
		WorkflowId:  uuid.NewString(),
		RunId:       uuid.NewString(),
		ArchetypeId: chasm.WorkflowArchetypeID,
	}
	s.clientBean.EXPECT().GetRemoteAdminClient(s.sourceCluster).Return(nil, serviceerror.NewUnavailable("no client"))

	doContinue, err := s.task.SyncState(context.Background(), syncStateErr, ResendAttempt)
	s.Error(err)
	s.False(doContinue)
}

func (s *executableTaskSuite) TestSyncState_ResourceExhausted() {
	syncStateErr := &serviceerrors.SyncState{
		NamespaceId: uuid.NewString(),
		WorkflowId:  uuid.NewString(),
		RunId:       uuid.NewString(),
		ArchetypeId: chasm.WorkflowArchetypeID,
	}
	mockRemoteAdminClient := adminservicemock.NewMockAdminServiceClient(s.controller)
	s.clientBean.EXPECT().GetRemoteAdminClient(s.sourceCluster).Return(mockRemoteAdminClient, nil).AnyTimes()
	mockRemoteAdminClient.EXPECT().SyncWorkflowState(gomock.Any(), gomock.Any()).Return(
		nil, &serviceerror.ResourceExhausted{})

	doContinue, err := s.task.SyncState(context.Background(), syncStateErr, ResendAttempt)
	var invalidArg *serviceerror.InvalidArgument
	s.ErrorAs(err, &invalidArg)
	s.False(doContinue)
}

func (s *executableTaskSuite) TestSyncState_FailedPrecondition_NoTaskEquivalents() {
	syncStateErr := &serviceerrors.SyncState{
		NamespaceId: uuid.NewString(),
		WorkflowId:  uuid.NewString(),
		RunId:       uuid.NewString(),
		ArchetypeId: chasm.WorkflowArchetypeID,
	}
	// task with no task equivalents -> task is dropped (no AddTasks call)
	execTask := NewExecutableTask(
		s.toolBox,
		s.taskId,
		"metrics-tag",
		time.Now(),
		time.Now(),
		s.sourceCluster,
		s.sourceShardKey,
		&replicationspb.ReplicationTask{
			RawTaskInfo: &persistencespb.ReplicationTaskInfo{
				NamespaceId: s.namespaceId,
				WorkflowId:  s.workflowId,
				RunId:       s.runId,
			},
		},
	)
	mockRemoteAdminClient := adminservicemock.NewMockAdminServiceClient(s.controller)
	s.clientBean.EXPECT().GetRemoteAdminClient(s.sourceCluster).Return(mockRemoteAdminClient, nil).AnyTimes()
	mockRemoteAdminClient.EXPECT().SyncWorkflowState(gomock.Any(), gomock.Any()).Return(
		nil, serviceerror.NewFailedPrecondition("transition history disabled"))

	doContinue, err := execTask.SyncState(context.Background(), syncStateErr, ResendAttempt)
	s.NoError(err)
	s.False(doContinue)
}

func (s *executableTaskSuite) TestSyncState_GenericError() {
	syncStateErr := &serviceerrors.SyncState{
		NamespaceId: uuid.NewString(),
		WorkflowId:  uuid.NewString(),
		RunId:       uuid.NewString(),
		ArchetypeId: chasm.WorkflowArchetypeID,
	}
	mockRemoteAdminClient := adminservicemock.NewMockAdminServiceClient(s.controller)
	s.clientBean.EXPECT().GetRemoteAdminClient(s.sourceCluster).Return(mockRemoteAdminClient, nil).AnyTimes()
	mockRemoteAdminClient.EXPECT().SyncWorkflowState(gomock.Any(), gomock.Any()).Return(
		nil, serviceerror.NewUnavailable("transient"))

	doContinue, err := s.task.SyncState(context.Background(), syncStateErr, ResendAttempt)
	s.Error(err)
	s.False(doContinue)
}

func (s *executableTaskSuite) TestSyncState_GetShardError() {
	syncStateErr := &serviceerrors.SyncState{
		NamespaceId: uuid.NewString(),
		WorkflowId:  uuid.NewString(),
		RunId:       uuid.NewString(),
		ArchetypeId: chasm.WorkflowArchetypeID,
	}
	mockRemoteAdminClient := adminservicemock.NewMockAdminServiceClient(s.controller)
	s.clientBean.EXPECT().GetRemoteAdminClient(s.sourceCluster).Return(mockRemoteAdminClient, nil).AnyTimes()
	mockRemoteAdminClient.EXPECT().SyncWorkflowState(gomock.Any(), gomock.Any()).Return(
		&adminservice.SyncWorkflowStateResponse{}, nil)
	s.shardController.EXPECT().GetShardByNamespaceWorkflow(
		namespace.ID(syncStateErr.NamespaceId),
		syncStateErr.WorkflowId,
	).Return(nil, serviceerror.NewUnavailable("shard unavailable"))

	doContinue, err := s.task.SyncState(context.Background(), syncStateErr, ResendAttempt)
	s.Error(err)
	s.False(doContinue)
}

func (s *executableTaskSuite) TestSyncState_ReplicateVersionedTransition_Duplicate() {
	syncStateErr := &serviceerrors.SyncState{
		NamespaceId: uuid.NewString(),
		WorkflowId:  uuid.NewString(),
		RunId:       uuid.NewString(),
		ArchetypeId: chasm.WorkflowArchetypeID,
	}
	mockRemoteAdminClient := adminservicemock.NewMockAdminServiceClient(s.controller)
	s.clientBean.EXPECT().GetRemoteAdminClient(s.sourceCluster).Return(mockRemoteAdminClient, nil).AnyTimes()
	mockRemoteAdminClient.EXPECT().SyncWorkflowState(gomock.Any(), gomock.Any()).Return(
		&adminservice.SyncWorkflowStateResponse{}, nil)
	shardContext := historyi.NewMockShardContext(s.controller)
	engine := historyi.NewMockEngine(s.controller)
	s.shardController.EXPECT().GetShardByNamespaceWorkflow(
		namespace.ID(syncStateErr.NamespaceId),
		syncStateErr.WorkflowId,
	).Return(shardContext, nil).AnyTimes()
	shardContext.EXPECT().GetEngine(gomock.Any()).Return(engine, nil).AnyTimes()
	engine.EXPECT().ReplicateVersionedTransition(gomock.Any(), chasm.WorkflowArchetypeID, gomock.Any(), s.sourceCluster).Return(consts.ErrDuplicate)

	doContinue, err := s.task.SyncState(context.Background(), syncStateErr, ResendAttempt)
	s.NoError(err)
	s.True(doContinue)
}

func (s *executableTaskSuite) TestSyncState_ReplicateVersionedTransition_Error() {
	syncStateErr := &serviceerrors.SyncState{
		NamespaceId: uuid.NewString(),
		WorkflowId:  uuid.NewString(),
		RunId:       uuid.NewString(),
		ArchetypeId: chasm.WorkflowArchetypeID,
	}
	mockRemoteAdminClient := adminservicemock.NewMockAdminServiceClient(s.controller)
	s.clientBean.EXPECT().GetRemoteAdminClient(s.sourceCluster).Return(mockRemoteAdminClient, nil).AnyTimes()
	mockRemoteAdminClient.EXPECT().SyncWorkflowState(gomock.Any(), gomock.Any()).Return(
		&adminservice.SyncWorkflowStateResponse{}, nil)
	shardContext := historyi.NewMockShardContext(s.controller)
	engine := historyi.NewMockEngine(s.controller)
	s.shardController.EXPECT().GetShardByNamespaceWorkflow(
		namespace.ID(syncStateErr.NamespaceId),
		syncStateErr.WorkflowId,
	).Return(shardContext, nil).AnyTimes()
	shardContext.EXPECT().GetEngine(gomock.Any()).Return(engine, nil).AnyTimes()
	engine.EXPECT().ReplicateVersionedTransition(gomock.Any(), chasm.WorkflowArchetypeID, gomock.Any(), s.sourceCluster).Return(serviceerror.NewUnavailable("boom"))

	doContinue, err := s.task.SyncState(context.Background(), syncStateErr, ResendAttempt)
	s.Error(err)
	s.False(doContinue)
}

func (s *executableTaskSuite) TestBackFillEvents_NewRunIdNonVerifyTask() {
	// task is not a verify task in default setup, so passing a newRunId should error
	err := s.task.BackFillEvents(
		context.Background(),
		"remote cluster",
		definition.NewWorkflowKey(s.namespaceId, s.workflowId, s.runId),
		int64(1), int64(1), int64(2), int64(1),
		uuid.NewString(),
	)
	s.Error(err)
}

func (s *executableTaskSuite) TestBackFillEvents_GetShardError() {
	workflowKey := definition.NewWorkflowKey(s.namespaceId, s.workflowId, s.runId)
	s.shardController.EXPECT().GetShardByNamespaceWorkflow(
		namespace.ID(workflowKey.NamespaceID),
		workflowKey.WorkflowID,
	).Return(nil, serviceerror.NewUnavailable("shard unavailable"))

	err := s.task.BackFillEvents(
		context.Background(),
		"remote cluster",
		workflowKey,
		int64(1), int64(1), int64(2), int64(1),
		"",
	)
	s.Error(err)
}

func (s *executableTaskSuite) TestBackFillEvents_GetEngineError() {
	workflowKey := definition.NewWorkflowKey(s.namespaceId, s.workflowId, s.runId)
	shardContext := historyi.NewMockShardContext(s.controller)
	s.shardController.EXPECT().GetShardByNamespaceWorkflow(
		namespace.ID(workflowKey.NamespaceID),
		workflowKey.WorkflowID,
	).Return(shardContext, nil)
	shardContext.EXPECT().GetEngine(gomock.Any()).Return(nil, serviceerror.NewUnavailable("engine unavailable"))

	err := s.task.BackFillEvents(
		context.Background(),
		"remote cluster",
		workflowKey,
		int64(1), int64(1), int64(2), int64(1),
		"",
	)
	s.Error(err)
}

func (s *executableTaskSuite) TestBackFillEvents_EmptyBatchError() {
	workflowKey := definition.NewWorkflowKey(s.namespaceId, s.workflowId, s.runId)
	remoteCluster := "remote cluster"
	emptyBlob, err := s.serializer.SerializeEvents(nil)
	s.NoError(err)
	fetcher := collection.NewPagingIterator(func(paginationToken []byte) ([]*eventhandler.HistoryBatch, []byte, error) {
		return []*eventhandler.HistoryBatch{
			{RawEventBatch: emptyBlob, VersionHistory: &historyspb.VersionHistory{Items: []*historyspb.VersionHistoryItem{}}},
		}, nil, nil
	})
	s.remoteHistoryFetcher.EXPECT().GetSingleWorkflowHistoryPaginatedIteratorInclusive(
		gomock.Any(), remoteCluster,
		namespace.ID(workflowKey.NamespaceID), workflowKey.WorkflowID, workflowKey.RunID,
		int64(1), int64(1), int64(2), int64(1),
	).Return(fetcher)
	shardContext := historyi.NewMockShardContext(s.controller)
	engine := historyi.NewMockEngine(s.controller)
	s.shardController.EXPECT().GetShardByNamespaceWorkflow(
		namespace.ID(workflowKey.NamespaceID),
		workflowKey.WorkflowID,
	).Return(shardContext, nil).AnyTimes()
	shardContext.EXPECT().GetEngine(gomock.Any()).Return(engine, nil).AnyTimes()

	err = s.task.BackFillEvents(
		context.Background(),
		remoteCluster,
		workflowKey,
		int64(1), int64(1), int64(2), int64(1),
		"",
	)
	var invalidArg *serviceerror.InvalidArgument
	s.ErrorAs(err, &invalidArg)
}

func (s *executableTaskSuite) TestSyncState_NotFound() {
	syncStateErr := &serviceerrors.SyncState{
		NamespaceId: uuid.NewString(),
		WorkflowId:  uuid.NewString(),
		RunId:       uuid.NewString(),
		ArchetypeId: chasm.WorkflowArchetypeID,
		VersionedTransition: &persistencespb.VersionedTransition{
			NamespaceFailoverVersion: rand.Int63(),
			TransitionCount:          rand.Int63(),
		},
		VersionHistories: &historyspb.VersionHistories{
			Histories: []*historyspb.VersionHistory{
				{
					BranchToken: []byte("token#1"),
					Items: []*historyspb.VersionHistoryItem{
						{EventId: 102, Version: 1234},
					},
				},
			},
		},
	}

	mockRemoteAdminClient := adminservicemock.NewMockAdminServiceClient(s.controller)
	s.clientBean.EXPECT().GetRemoteAdminClient(s.sourceCluster).Return(mockRemoteAdminClient, nil).AnyTimes()
	mockRemoteAdminClient.EXPECT().SyncWorkflowState(
		gomock.Any(),
		&adminservice.SyncWorkflowStateRequest{
			NamespaceId: syncStateErr.NamespaceId,
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: syncStateErr.WorkflowId,
				RunId:      syncStateErr.RunId,
			},
			ArchetypeId:         chasm.WorkflowArchetypeID,
			VersionedTransition: syncStateErr.VersionedTransition,
			VersionHistories: &historyspb.VersionHistories{
				Histories: []*historyspb.VersionHistory{
					{
						// BranchToken is removed in the actual implementation
						Items: []*historyspb.VersionHistoryItem{
							{EventId: 102, Version: 1234},
						},
					},
				},
			},
			TargetClusterId: int32(s.clusterMetadata.GetAllClusterInfo()[s.clusterMetadata.GetCurrentClusterName()].InitialFailoverVersion),
		},
	).Return(nil, serviceerror.NewNotFound("workflow not found")).Times(1)

	shardContext := historyi.NewMockShardContext(s.controller)
	engine := historyi.NewMockEngine(s.controller)
	s.shardController.EXPECT().GetShardByNamespaceWorkflow(
		namespace.ID(syncStateErr.NamespaceId),
		syncStateErr.WorkflowId,
	).Return(shardContext, nil).AnyTimes()
	shardContext.EXPECT().GetEngine(gomock.Any()).Return(engine, nil).AnyTimes()
	engine.EXPECT().DeleteWorkflowExecution(gomock.Any(), &historyservice.DeleteWorkflowExecutionRequest{
		NamespaceId: syncStateErr.NamespaceId,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: syncStateErr.WorkflowId,
			RunId:      syncStateErr.RunId,
		},
		ClosedWorkflowOnly: false,
	}).Return(&historyservice.DeleteWorkflowExecutionResponse{}, nil)

	doContinue, err := s.task.SyncState(context.Background(), syncStateErr, ResendAttempt)
	s.NoError(err)
	s.False(doContinue)
}
