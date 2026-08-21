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
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/dynamicconfig"
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
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
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
		timeSource              *clock.EventTimeSource
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
	s.timeSource = clock.NewEventTimeSource()

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
		TimeSource:              s.timeSource,
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
	s.task.replicationTask.RawTaskInfo.IsForceReplication = true
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
				taskInfo, err := s.serializer.ReplicationTaskInfoFromBlob(task.Blob)
				s.NoError(err)
				s.True(taskInfo.GetIsForceReplication())
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

func (s *executableTaskSuite) newGradualConnectNamespace(ramp *persistencespb.NamespaceReplicationRamp) (*namespace.Namespace, string) {
	namespaceID := uuid.NewString()
	factory := namespace.NewDefaultReplicationResolverFactory()
	replicationConfig := &persistencespb.NamespaceReplicationConfig{
		ActiveClusterName: cluster.TestAlternativeClusterName,
		Clusters: []string{
			cluster.TestCurrentClusterName,
			cluster.TestAlternativeClusterName,
		},
	}
	if ramp != nil {
		replicationConfig.ClusterReplicationRamps = map[string]*persistencespb.NamespaceReplicationRamp{
			cluster.TestCurrentClusterName: ramp,
		}
	}
	detail := &persistencespb.NamespaceDetail{
		Info:              &persistencespb.NamespaceInfo{Id: namespaceID, Name: uuid.NewString()},
		Config:            &persistencespb.NamespaceConfig{},
		ReplicationConfig: replicationConfig,
	}
	namespaceEntry, err := namespace.FromPersistentState(detail, factory(detail))
	s.NoError(err)
	return namespaceEntry, namespaceID
}

func (s *executableTaskSuite) TestGetNamespaceInfo_GradualConnect_NoRampAdmits() {
	namespaceEntry, namespaceID := s.newGradualConnectNamespace(nil)
	s.namespaceCache.EXPECT().GetNamespaceByID(namespace.ID(namespaceID)).Return(namespaceEntry, nil).AnyTimes()

	_, toProcess, err := s.task.GetNamespaceInfo(context.Background(), namespaceID, "test-workflow-id")
	s.NoError(err)
	s.True(toProcess)
}

func (s *executableTaskSuite) TestGetNamespaceInfo_GradualConnect_ConnectTimeInFutureUsesInitialPercent() {
	future := s.timeSource.Now().Add(time.Hour)
	namespaceEntry, namespaceID := s.newGradualConnectNamespace(&persistencespb.NamespaceReplicationRamp{
		StartTime:         timestamppb.New(future),
		Duration:          durationpb.New(time.Hour),
		InitialPercentage: 0,
	})
	s.namespaceCache.EXPECT().GetNamespaceByID(namespace.ID(namespaceID)).Return(namespaceEntry, nil).AnyTimes()

	_, toProcess, err := s.task.GetNamespaceInfo(context.Background(), namespaceID, "test-workflow-id")
	s.NoError(err)
	s.False(toProcess, "clock skew must not temporarily admit a workflow that could be shed once clocks catch up")

	s.timeSource.Update(future)
	_, toProcess, err = s.task.GetNamespaceInfo(context.Background(), namespaceID, "test-workflow-id")
	s.NoError(err)
	s.False(toProcess, "catching up to the ramp start must not reduce the admission percentage")
}

// TestGetNamespaceInfo_GradualConnect_ForceReplication_Admits: a shed task is dropped for good, so
// shedding force-replication traffic would stall migration verification until it hard-fails.
func (s *executableTaskSuite) TestGetNamespaceInfo_GradualConnect_ForceReplication_Admits() {
	connectTime := s.timeSource.Now() // freshly connected -- percent would otherwise be 0, i.e. shed
	namespaceEntry, namespaceID := s.newGradualConnectNamespace(&persistencespb.NamespaceReplicationRamp{
		StartTime:         timestamppb.New(connectTime),
		Duration:          durationpb.New(time.Hour),
		InitialPercentage: 0,
	})
	s.namespaceCache.EXPECT().GetNamespaceByID(namespace.ID(namespaceID)).Return(namespaceEntry, nil).AnyTimes()
	s.task.replicationTask.RawTaskInfo.IsForceReplication = true

	_, toProcess, err := s.task.GetNamespaceInfo(context.Background(), namespaceID, "test-workflow-id")
	s.NoError(err)
	s.True(toProcess, "a force-replication task must bypass the ramp regardless of percent")
}

func (s *executableTaskSuite) TestGetNamespaceInfo_GradualConnect_RampComplete_Admits() {
	connectTime := s.timeSource.Now()
	namespaceEntry, namespaceID := s.newGradualConnectNamespace(&persistencespb.NamespaceReplicationRamp{
		StartTime:         timestamppb.New(connectTime),
		Duration:          durationpb.New(10 * time.Minute),
		InitialPercentage: 0,
	})
	s.namespaceCache.EXPECT().GetNamespaceByID(namespace.ID(namespaceID)).Return(namespaceEntry, nil).AnyTimes()

	s.timeSource.Update(connectTime.Add(time.Hour)) // well past the ramp duration
	for range 20 {
		_, toProcess, err := s.task.GetNamespaceInfo(context.Background(), namespaceID, uuid.NewString())
		s.NoError(err)
		s.True(toProcess, "every workflow ID must be admitted once the ramp has run past 100%%")
	}
}

func (s *executableTaskSuite) TestGetNamespaceInfo_GradualConnect_AdmissionGrowsAsClockMovesForward() {
	sampleInterval := time.Minute
	connectTime := s.timeSource.Now()
	namespaceEntry, namespaceID := s.newGradualConnectNamespace(&persistencespb.NamespaceReplicationRamp{
		StartTime:         timestamppb.New(connectTime),
		Duration:          durationpb.New(10 * sampleInterval),
		InitialPercentage: 10,
	})
	s.namespaceCache.EXPECT().GetNamespaceByID(namespace.ID(namespaceID)).Return(namespaceEntry, nil).AnyTimes()

	const numWorkflows = 2000
	workflowIDs := make([]string, numWorkflows)
	for i := range workflowIDs {
		workflowIDs[i] = uuid.NewString()
	}

	admitted := make(map[string]bool, numWorkflows)
	for tick := 0; tick <= 10; tick++ { // Sample the linear ramp once per minute.
		s.timeSource.Update(connectTime.Add(time.Duration(tick) * sampleInterval))
		for _, wfID := range workflowIDs {
			_, toProcess, err := s.task.GetNamespaceInfo(context.Background(), namespaceID, wfID)
			s.NoError(err)
			if admitted[wfID] {
				s.True(toProcess, "workflow %s was admitted at an earlier tick; must not be shed at tick %d", wfID, tick)
			}
			if toProcess {
				admitted[wfID] = true
			}
		}
	}
	for _, wfID := range workflowIDs {
		s.True(admitted[wfID], "workflow %s must be admitted by the time the ramp reaches 100%%", wfID)
	}
}

func (s *executableTaskSuite) TestGetNamespaceInfo_GradualConnect_ClockRegressionCanShedOrdinaryTask() {
	connectTime := s.timeSource.Now()
	namespaceEntry, namespaceID := s.newGradualConnectNamespace(&persistencespb.NamespaceReplicationRamp{
		StartTime:         timestamppb.New(connectTime),
		Duration:          durationpb.New(10 * time.Minute),
		InitialPercentage: 0,
	})
	s.namespaceCache.EXPECT().GetNamespaceByID(namespace.ID(namespaceID)).Return(namespaceEntry, nil).AnyTimes()

	var businessID string
	for {
		businessID = uuid.NewString()
		if dynamicconfig.RolloutAccepts([]byte(businessID), 50) && !dynamicconfig.RolloutAccepts([]byte(businessID), 10) {
			break
		}
	}

	s.timeSource.Update(connectTime.Add(5 * time.Minute))
	_, toProcess, err := s.task.GetNamespaceInfo(context.Background(), namespaceID, businessID)
	s.NoError(err)
	s.True(toProcess)

	s.timeSource.Update(connectTime.Add(time.Minute))
	_, toProcess, err = s.task.GetNamespaceInfo(context.Background(), namespaceID, businessID)
	s.NoError(err)
	s.False(toProcess, "ordinary tasks may be shed after clock regression and are repaired by force replication")
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

func TestGradualConnectPercent(t *testing.T) {
	t.Parallel()
	connectTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	testCases := []struct {
		name           string
		now            time.Time
		initialPercent int
		duration       time.Duration
		want           int
	}{
		{
			name:           "at_connect_time_returns_initial_percent",
			now:            connectTime,
			initialPercent: 10,
			duration:       50 * time.Minute,
			want:           10,
		},
		{
			name:           "elapsed_time_increases_percent",
			now:            connectTime.Add(20 * time.Minute),
			initialPercent: 10,
			duration:       50 * time.Minute,
			want:           46,
		},
		{
			name:           "capped_at_100_regardless_of_how_far_past",
			now:            connectTime.Add(365 * 24 * time.Hour),
			initialPercent: 10,
			duration:       50 * time.Minute,
			want:           100,
		},
		{
			name:           "negative_elapsed_uses_initial_percent",
			now:            connectTime.Add(-time.Minute),
			initialPercent: 10,
			duration:       50 * time.Minute,
			want:           10,
		},
		{
			name:           "non_positive_duration_admits_everything",
			now:            connectTime.Add(time.Minute),
			initialPercent: 0,
			duration:       0,
			want:           100,
		},
		{
			name:           "invalid_initial_percent_fails_open",
			now:            connectTime.Add(15 * time.Minute),
			initialPercent: -1,
			duration:       50 * time.Minute,
			want:           100,
		},
	}
	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := gradualConnectPercent(connectTime, tt.now, tt.duration, tt.initialPercent)
			require.Equal(t, tt.want, got)
		})
	}
}
