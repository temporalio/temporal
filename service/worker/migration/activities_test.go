// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package migration

import (
	"context"
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
	emptyExecutions = commonpb.WorkflowExecution{}

	execution1 = commonpb.WorkflowExecution{
		WorkflowId: "workflow1",
		RunId:      "run1",
	}

	execution2 = commonpb.WorkflowExecution{
		WorkflowId: "workflow2",
		RunId:      "run2",
	}

	execution3 = commonpb.WorkflowExecution{
		WorkflowId: "workflow3",
		RunId:      "run3",
	}

	completeState = historyservice.DescribeMutableStateResponse{
		DatabaseMutableState: &persistencepb.WorkflowMutableState{
			ExecutionState: &persistencepb.WorkflowExecutionState{
				State: enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
			},
		},
	}

	zombieState = historyservice.DescribeMutableStateResponse{
		DatabaseMutableState: &persistencepb.WorkflowMutableState{
			ExecutionState: &persistencepb.WorkflowExecutionState{
				State: enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
			},
		},
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
	s.mockMetricsHandler.EXPECT().WithTags(gomock.Any()).Return(s.mockMetricsHandler).AnyTimes()
	s.mockMetricsHandler.EXPECT().Timer(gomock.Any()).Return(metrics.NoopTimerMetricFunc).AnyTimes()
	s.mockMetricsHandler.EXPECT().Counter(gomock.Any()).Return(metrics.NoopCounterMetricFunc).AnyTimes()
	s.mockClientFactory.EXPECT().NewRemoteAdminClientWithTimeout(remoteRpcAddress, gomock.Any(), gomock.Any()).
		Return(s.mockRemoteAdminClient).AnyTimes()
	s.mockNamespaceRegistry.EXPECT().GetNamespaceName(gomock.Any()).
		Return(namespace.Name(mockedNamespace), nil).AnyTimes()

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

func (s *activitiesSuite) TestVerifyReplicationTasks_Success() {
	env, iceptor := s.initEnv()

	request := verifyReplicationTasksRequest{
		Namespace:             mockedNamespace,
		NamespaceID:           mockedNamespaceID,
		TargetClusterEndpoint: remoteRpcAddress,
		Executions:            []commonpb.WorkflowExecution{execution1, execution2},
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

	s.mockHistoryClient.EXPECT().DescribeMutableState(gomock.Any(), &historyservice.DescribeMutableStateRequest{
		NamespaceId: mockedNamespaceID,
		Execution:   &execution2,
	}).Return(&completeState, nil).Times(2)

	r, err := env.ExecuteActivity(s.a.VerifyReplicationTasks, &request)
	s.NoError(err)
	var response verifyReplicationTasksResponse
	s.NoError(r.Get(&response))
	s.Empty(response.SkippedWorkflowExecutions)

	s.Greater(len(iceptor.replicationRecordedHeartbeats), 0)
	lastHeartBeat := iceptor.replicationRecordedHeartbeats[len(iceptor.replicationRecordedHeartbeats)-1]
	s.Equal(len(request.Executions), lastHeartBeat.NextIndex)
	s.Equal(execution2, lastHeartBeat.LastNotFoundWorkflowExecution)
}

func (s *activitiesSuite) TestVerifyReplicationTasks_SkipWorkflowExecution() {
	mockErr := serviceerror.NewInternal("mock error")
	var testcases = []struct {
		resp           *historyservice.DescribeMutableStateResponse
		err            error
		expectedReason string
		expectedErr    error
	}{
		{
			&zombieState,
			nil,
			reasonZombieWorkflow,
			nil,
		},
		{
			nil, serviceerror.NewNotFound(""),
			reasonWorkflowNotFound,
			nil,
		},
		{
			nil, mockErr,
			"",
			mockErr,
		},
	}

	request := verifyReplicationTasksRequest{
		Namespace:             mockedNamespace,
		NamespaceID:           mockedNamespaceID,
		TargetClusterEndpoint: remoteRpcAddress,
		Executions:            []commonpb.WorkflowExecution{execution1},
	}

	start := time.Now()
	for _, t := range testcases {
		env, iceptor := s.initEnv()

		// Call DescribeMutableState for enough times to trigger trySkipWorkflowExecution
		s.mockRemoteAdminClient.EXPECT().DescribeMutableState(gomock.Any(), &adminservice.DescribeMutableStateRequest{
			Namespace: mockedNamespace,
			Execution: &execution1,
		}).Return(nil, serviceerror.NewNotFound("")).Times(1)

		s.mockHistoryClient.EXPECT().DescribeMutableState(gomock.Any(), &historyservice.DescribeMutableStateRequest{
			NamespaceId: mockedNamespaceID,
			Execution:   &execution1,
		}).Return(t.resp, t.err).Times(1)

		r, err := env.ExecuteActivity(s.a.VerifyReplicationTasks, &request)
		s.Greater(len(iceptor.replicationRecordedHeartbeats), 0)
		lastHeartBeat := iceptor.replicationRecordedHeartbeats[len(iceptor.replicationRecordedHeartbeats)-1]
		if t.expectedErr == nil {
			s.NoError(err)
			s.Equal(len(request.Executions), lastHeartBeat.NextIndex)

			var response verifyReplicationTasksResponse
			s.NoError(r.Get(&response))
			s.Equal(request.Executions[0], response.SkippedWorkflowExecutions[0].WorkflowExecution)
			s.Equal(t.expectedReason, response.SkippedWorkflowExecutions[0].Reason)
		} else {
			s.ErrorContains(err, "mock error")
			s.Equal(0, lastHeartBeat.NextIndex)
		}

		s.Equal(emptyExecutions, lastHeartBeat.LastNotFoundWorkflowExecution)
		s.True(lastHeartBeat.CheckPoint.After(start))
	}
}

func (s *activitiesSuite) TestVerifyReplicationTasks_FailedNotFound() {
	env, iceptor := s.initEnv()
	request := verifyReplicationTasksRequest{
		Namespace:             mockedNamespace,
		NamespaceID:           mockedNamespaceID,
		TargetClusterEndpoint: remoteRpcAddress,
		Executions:            []commonpb.WorkflowExecution{execution1},
	}

	s.mockHistoryClient.EXPECT().DescribeMutableState(gomock.Any(), &historyservice.DescribeMutableStateRequest{
		NamespaceId: mockedNamespaceID,
		Execution:   &execution1,
	}).Return(&completeState, nil)

	// Workflow not found at target cluster.
	s.mockRemoteAdminClient.EXPECT().DescribeMutableState(gomock.Any(), &adminservice.DescribeMutableStateRequest{
		Namespace: mockedNamespace,
		Execution: &execution1,
	}).Return(nil, serviceerror.NewNotFound("")).AnyTimes()

	// Set CheckPoint to an early to trigger failure.
	env.SetHeartbeatDetails(&replicationTasksHeartbeatDetails{
		NextIndex:  0,
		CheckPoint: time.Now().Add(-defaultNoProgressNotRetryableTimeout),
	})

	_, err := env.ExecuteActivity(s.a.VerifyReplicationTasks, &request)
	s.Error(err)
	s.ErrorContains(err, "verifyReplicationTasks was not able to make progress")

	s.Greater(len(iceptor.replicationRecordedHeartbeats), 0)
	lastHeartBeat := iceptor.replicationRecordedHeartbeats[len(iceptor.replicationRecordedHeartbeats)-1]
	s.Equal(0, lastHeartBeat.NextIndex)
	s.Equal(execution1, lastHeartBeat.LastNotFoundWorkflowExecution)
}

func (s *activitiesSuite) TestVerifyReplicationTasks_AlreadyVerified() {
	env, iceptor := s.initEnv()
	request := verifyReplicationTasksRequest{
		Namespace:             mockedNamespace,
		NamespaceID:           mockedNamespaceID,
		TargetClusterEndpoint: remoteRpcAddress,
		Executions:            []commonpb.WorkflowExecution{execution1, execution2},
	}

	// Set NextIndex to indicate all executions have been verified. No additional mock is needed.
	env.SetHeartbeatDetails(&replicationTasksHeartbeatDetails{
		NextIndex:  len(request.Executions),
		CheckPoint: time.Now(),
	})

	_, err := env.ExecuteActivity(s.a.VerifyReplicationTasks, &request)
	s.NoError(err)

	s.Greater(len(iceptor.replicationRecordedHeartbeats), 0)
	lastHeartBeat := iceptor.replicationRecordedHeartbeats[len(iceptor.replicationRecordedHeartbeats)-1]
	s.Equal(len(request.Executions), lastHeartBeat.NextIndex)
}

type executionState int

const (
	executionFound    executionState = 0
	executionNotfound executionState = 1
	executionErr      executionState = 2
)

func createExecutions(mockClient *adminservicemock.MockAdminServiceClient, states []executionState, nextIndex int) []commonpb.WorkflowExecution {
	var executions []commonpb.WorkflowExecution

	for i := 0; i < len(states); i++ {
		executions = append(executions, execution1)
	}

	for i := nextIndex; i < len(states); i++ {
		switch states[i] {
		case executionFound:
			mockClient.EXPECT().DescribeMutableState(gomock.Any(), &adminservice.DescribeMutableStateRequest{
				Namespace: mockedNamespace,
				Execution: &execution1,
			}).Return(&adminservice.DescribeMutableStateResponse{}, nil).Times(1)
		case executionNotfound:
			mockClient.EXPECT().DescribeMutableState(gomock.Any(), &adminservice.DescribeMutableStateRequest{
				Namespace: mockedNamespace,
				Execution: &execution1,
			}).Return(nil, serviceerror.NewNotFound("")).Times(1)
		case executionErr:
			mockClient.EXPECT().DescribeMutableState(gomock.Any(), &adminservice.DescribeMutableStateRequest{
				Namespace: mockedNamespace,
				Execution: &execution1,
			}).Return(nil, serviceerror.NewInternal("")).Times(1)
		}
	}

	return executions
}

func (s *activitiesSuite) Test_verifyReplicationTasks() {
	request := verifyReplicationTasksRequest{
		Namespace:             mockedNamespace,
		NamespaceID:           mockedNamespaceID,
		TargetClusterEndpoint: remoteRpcAddress,
	}

	ctx := context.TODO()

	var tests = []struct {
		executionStates  []executionState
		nextIndex        int
		expectedVerified bool
		expectedErr      error
		expectedIndex    int
	}{
		{
			expectedVerified: true,
			expectedErr:      nil,
		},
		{
			executionStates:  []executionState{executionFound, executionFound, executionFound, executionFound},
			nextIndex:        0,
			expectedVerified: true,
			expectedErr:      nil,
			expectedIndex:    4,
		},
		{
			executionStates:  []executionState{executionFound, executionFound, executionFound, executionFound},
			nextIndex:        2,
			expectedVerified: true,
			expectedErr:      nil,
			expectedIndex:    4,
		},
		{
			executionStates:  []executionState{executionFound, executionFound, executionNotfound},
			nextIndex:        0,
			expectedVerified: false,
			expectedErr:      nil,
			expectedIndex:    2,
		},
	}

	s.mockHistoryClient.EXPECT().DescribeMutableState(gomock.Any(), &historyservice.DescribeMutableStateRequest{
		NamespaceId: mockedNamespaceID,
		Execution:   &execution1,
	}).Return(&completeState, nil).AnyTimes()

	for _, tc := range tests {
		mockRemoteAdminClient := adminservicemock.NewMockAdminServiceClient(s.controller)
		request.Executions = createExecutions(mockRemoteAdminClient, tc.executionStates, tc.nextIndex)
		details := replicationTasksHeartbeatDetails{
			NextIndex: tc.nextIndex,
		}

		verified, _, err := s.a.verifyReplicationTasks(ctx, &request, &details, mockRemoteAdminClient)
		if tc.expectedErr == nil {
			s.NoError(err)
		}
		s.Equal(tc.expectedVerified, verified)
		s.Equal(tc.expectedIndex, details.NextIndex)
		s.GreaterOrEqual(len(tc.executionStates), details.NextIndex)
		if details.NextIndex < len(tc.executionStates) && tc.executionStates[details.NextIndex] == executionNotfound {
			s.Equal(execution1, details.LastNotFoundWorkflowExecution)
		}
	}
}

func (s *activitiesSuite) Test_isNotFoundServiceError() {
	s.True(isNotFoundServiceError(serviceerror.NewNotFound("")))
	var err error
	s.False(isNotFoundServiceError(err))
	s.False(isNotFoundServiceError(serviceerror.NewInternal("")))
}

func (s *activitiesSuite) TestGenerateReplicationTasks_Success() {
	env, iceptor := s.initEnv()

	request := generateReplicationTasksRequest{
		NamespaceID: mockedNamespaceID,
		RPS:         10,
		Executions:  []commonpb.WorkflowExecution{execution1, execution2},
	}

	for i := 0; i < len(request.Executions); i++ {
		we := request.Executions[i]
		s.mockHistoryClient.EXPECT().GenerateLastHistoryReplicationTasks(gomock.Any(), &historyservice.GenerateLastHistoryReplicationTasksRequest{
			NamespaceId: mockedNamespaceID,
			Execution:   &we,
		}).Return(&historyservice.GenerateLastHistoryReplicationTasksResponse{}, nil).Times(1)
	}

	_, err := env.ExecuteActivity(s.a.GenerateReplicationTasks, &request)
	s.NoError(err)

	s.Greater(len(iceptor.generateReplicationRecordedHeartbeats), 0)
	lastIdx := len(iceptor.generateReplicationRecordedHeartbeats) - 1
	lastHeartBeat := iceptor.generateReplicationRecordedHeartbeats[lastIdx]
	s.Equal(lastIdx, lastHeartBeat)
}

func (s *activitiesSuite) TestGenerateReplicationTasks_NotFound() {
	env, iceptor := s.initEnv()

	request := generateReplicationTasksRequest{
		NamespaceID: mockedNamespaceID,
		RPS:         10,
		Executions:  []commonpb.WorkflowExecution{execution1},
	}

	s.mockHistoryClient.EXPECT().GenerateLastHistoryReplicationTasks(gomock.Any(), &historyservice.GenerateLastHistoryReplicationTasksRequest{
		NamespaceId: mockedNamespaceID,
		Execution:   &execution1,
	}).Return(nil, serviceerror.NewNotFound("")).Times(1)

	_, err := env.ExecuteActivity(s.a.GenerateReplicationTasks, &request)
	s.NoError(err)

	s.Greater(len(iceptor.generateReplicationRecordedHeartbeats), 0)
	lastIdx := len(iceptor.generateReplicationRecordedHeartbeats) - 1
	lastHeartBeat := iceptor.generateReplicationRecordedHeartbeats[lastIdx]
	s.Equal(0, lastHeartBeat)
}

func (s *activitiesSuite) TestGenerateReplicationTasks_Failed() {
	env, iceptor := s.initEnv()

	request := generateReplicationTasksRequest{
		NamespaceID: mockedNamespaceID,
		RPS:         10,
		Executions:  []commonpb.WorkflowExecution{execution1, execution2},
	}

	s.mockHistoryClient.EXPECT().GenerateLastHistoryReplicationTasks(gomock.Any(), &historyservice.GenerateLastHistoryReplicationTasksRequest{
		NamespaceId: mockedNamespaceID,
		Execution:   &execution1,
	}).Return(&historyservice.GenerateLastHistoryReplicationTasksResponse{}, nil).Times(1)

	s.mockHistoryClient.EXPECT().GenerateLastHistoryReplicationTasks(gomock.Any(), &historyservice.GenerateLastHistoryReplicationTasksRequest{
		NamespaceId: mockedNamespaceID,
		Execution:   &execution2,
	}).Return(nil, serviceerror.NewInternal(""))

	_, err := env.ExecuteActivity(s.a.GenerateReplicationTasks, &request)
	s.Error(err)

	s.Greater(len(iceptor.generateReplicationRecordedHeartbeats), 0)
	lastIdx := len(iceptor.generateReplicationRecordedHeartbeats) - 1
	lastHeartBeat := iceptor.generateReplicationRecordedHeartbeats[lastIdx]
	// Only the generation of 1st execution suceeded.
	s.Equal(0, lastHeartBeat)
}
