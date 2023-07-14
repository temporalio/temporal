package migration

import (
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservicemock/v1"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/adminservicemock/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	persistencepb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
)

type activitiesSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite

	controller                    *gomock.Controller
	mockTaskManager               *persistence.MockTaskManager
	mockNamespaceReplicationQueue *persistence.MockNamespaceReplicationQueue
	mockNamespaceRegistry         *namespace.MockRegistry
	mockClientFactory             *client.MockFactory

	mockFrontendClient    *workflowservicemock.MockWorkflowServiceClient
	mockHistoryClient     *historyservicemock.MockHistoryServiceClient
	mockRemoteAdminClient *adminservicemock.MockAdminServiceClient

	logger             log.Logger
	mockMetricsHandler *metrics.MockHandler

	a *activities
}

const (
	mockedNamespace   = "test_namespace"
	mockedNamespaceID = "test_namespace_id"
	remoteRpcAddress  = "remote"
)

var (
	execution1 = commonpb.WorkflowExecution{
		WorkflowId: "workflow1",
		RunId:      "run1",
	}

	execution2 = commonpb.WorkflowExecution{
		WorkflowId: "workflow2",
		RunId:      "run2",
	}

	zombieExecution = commonpb.WorkflowExecution{
		WorkflowId: "zombie",
		RunId:      "z1",
	}
)

func TestActivitiesSuite(t *testing.T) {
	suite.Run(t, new(activitiesSuite))
}

func (s *activitiesSuite) SetupTest() {
	s.controller = gomock.NewController(s.T())
	s.mockTaskManager = persistence.NewMockTaskManager(s.controller)
	s.mockNamespaceReplicationQueue = persistence.NewMockNamespaceReplicationQueue(s.controller)
	s.mockNamespaceRegistry = namespace.NewMockRegistry(s.controller)
	s.mockClientFactory = client.NewMockFactory(s.controller)

	s.mockFrontendClient = workflowservicemock.NewMockWorkflowServiceClient(s.controller)
	s.mockHistoryClient = historyservicemock.NewMockHistoryServiceClient(s.controller)
	s.mockRemoteAdminClient = adminservicemock.NewMockAdminServiceClient(s.controller)

	s.logger = log.NewNoopLogger()
	s.mockMetricsHandler = metrics.NewMockHandler(s.controller)
	s.mockMetricsHandler.EXPECT().Timer(gomock.Any()).Return(metrics.NoopTimerMetricFunc).AnyTimes()
	s.mockMetricsHandler.EXPECT().Counter(gomock.Any()).Return(metrics.NoopCounterMetricFunc).AnyTimes()
	s.mockClientFactory.EXPECT().NewRemoteAdminClientWithTimeout(remoteRpcAddress, gomock.Any(), gomock.Any()).
		Return(s.mockRemoteAdminClient).AnyTimes()

	s.a = &activities{
		namespaceRegistry:              s.mockNamespaceRegistry,
		namespaceReplicationQueue:      s.mockNamespaceReplicationQueue,
		clientFactory:                  s.mockClientFactory,
		taskManager:                    s.mockTaskManager,
		frontendClient:                 s.mockFrontendClient,
		historyClient:                  s.mockHistoryClient,
		logger:                         log.NewCLILogger(),
		metricsHandler:                 s.mockMetricsHandler,
		forceReplicationMetricsHandler: s.mockMetricsHandler,
	}
}

func (s *activitiesSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *activitiesSuite) initEnv() (*testsuite.TestActivityEnvironment, *heartbeatRecordingInterceptor) {
	env := s.NewTestActivityEnvironment()
	env.RegisterActivity(s.a)
	iceptor := heartbeatRecordingInterceptor{T: s.T()}
	env.SetWorkerOptions(worker.Options{Interceptors: []interceptor.WorkerInterceptor{&iceptor}})

	return env, &iceptor
}

func (s *activitiesSuite) TestGenerateAndVerifyReplicationTasks_Success() {
	env, iceptor := s.initEnv()

	request := genearteAndVerifyReplicationTasksRequest{
		Namespace:             mockedNamespace,
		NamespaceID:           mockedNamespaceID,
		RPS:                   10,
		TargetClusterEndpoint: remoteRpcAddress,
		Executions:            []commonpb.WorkflowExecution{execution1, execution2},
	}

	// Setup create replication tasks
	for i := 0; i < len(request.Executions); i++ {
		we := request.Executions[i]
		s.mockHistoryClient.EXPECT().DescribeMutableState(gomock.Any(), &historyservice.DescribeMutableStateRequest{
			NamespaceId: mockedNamespaceID,
			Execution:   &we,
		}).Return(&historyservice.DescribeMutableStateResponse{
			DatabaseMutableState: &persistencepb.WorkflowMutableState{
				ExecutionState: &persistencepb.WorkflowExecutionState{
					State: enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
				},
			},
		}, nil).Times(1)

		s.mockHistoryClient.EXPECT().GenerateLastHistoryReplicationTasks(gomock.Any(), &historyservice.GenerateLastHistoryReplicationTasksRequest{
			NamespaceId: mockedNamespaceID,
			Execution:   &we,
		}).Return(&historyservice.GenerateLastHistoryReplicationTasksResponse{}, nil).Times(1)
	}

	// Immediately replicated
	s.mockRemoteAdminClient.EXPECT().DescribeMutableState(gomock.Any(), &adminservice.DescribeMutableStateRequest{
		Namespace: mockedNamespace,
		Execution: &execution1,
	}).Return(&adminservice.DescribeMutableStateResponse{}, nil).Times(1)

	// Slowly replicated
	replicationSlowReponses := []struct {
		resp *adminservice.DescribeMutableStateResponse
		err  error
	}{
		{nil, serviceerror.NewNotFound("")},
		{nil, serviceerror.NewNotFound("")},
		{&adminservice.DescribeMutableStateResponse{}, nil},
	}

	for _, r := range replicationSlowReponses {
		s.mockRemoteAdminClient.EXPECT().DescribeMutableState(gomock.Any(), &adminservice.DescribeMutableStateRequest{
			Namespace: mockedNamespace,
			Execution: &execution2,
		}).Return(r.resp, r.err).Times(1)
	}

	_, err := env.ExecuteActivity(s.a.GenerateAndVerifyReplicationTasks, &request)
	s.NoError(err)

	s.Greater(len(iceptor.replicationRecordedHeartbeats), 0)
	lastHeartBeat := iceptor.replicationRecordedHeartbeats[len(iceptor.replicationRecordedHeartbeats)-1]
	s.Equal(len(request.Executions), len(lastHeartBeat.Results))
	for _, r := range lastHeartBeat.Results {
		s.True(r.isVerified())
	}
}

func (s *activitiesSuite) TestGenerateAndVerifyReplicationTasks_Skipped() {
	mockErr := serviceerror.NewInternal("mock error")
	var testcases = []struct {
		resp           *historyservice.DescribeMutableStateResponse
		err            error
		expectedStatus VerifyStatus
		expectedReason string
		expectedErr    error
	}{
		{
			&historyservice.DescribeMutableStateResponse{
				DatabaseMutableState: &persistencepb.WorkflowMutableState{
					ExecutionState: &persistencepb.WorkflowExecutionState{
						State: enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
					},
				},
			},
			nil,
			VERIFY_SKIPPED,
			reasonZombieWorkflow,
			nil,
		},
		{
			nil, serviceerror.NewNotFound(""),
			VERIFY_SKIPPED,
			reasonWorkflowNotFound,
			nil,
		},
		{
			nil, mockErr,
			NOT_CREATED,
			"",
			mockErr,
		},
	}

	request := genearteAndVerifyReplicationTasksRequest{
		Namespace:             mockedNamespace,
		NamespaceID:           mockedNamespaceID,
		RPS:                   10,
		TargetClusterEndpoint: remoteRpcAddress,
		Executions:            []commonpb.WorkflowExecution{execution1},
	}

	start := time.Now()
	for _, t := range testcases {
		env, iceptor := s.initEnv()

		s.mockHistoryClient.EXPECT().DescribeMutableState(gomock.Any(), &historyservice.DescribeMutableStateRequest{
			NamespaceId: mockedNamespaceID,
			Execution:   &execution1,
		}).Return(t.resp, t.err)

		_, err := env.ExecuteActivity(s.a.GenerateAndVerifyReplicationTasks, &request)
		if t.expectedErr == nil {
			s.NoError(err)
		} else {
			s.ErrorContains(err, "mock error")
		}

		s.Greater(len(iceptor.replicationRecordedHeartbeats), 0)
		lastHeartBeat := iceptor.replicationRecordedHeartbeats[len(iceptor.replicationRecordedHeartbeats)-1]
		s.Equal(len(request.Executions), len(lastHeartBeat.Results))
		for _, r := range lastHeartBeat.Results {
			s.Equal(t.expectedStatus, r.Status)
			s.Equal(t.expectedReason, r.Reason)
		}

		s.True(lastHeartBeat.CheckPoint.After(start))
	}
}
