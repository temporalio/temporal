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
	"go.temporal.io/server/common/testing/protoassert"
	"go.temporal.io/server/common/testing/protomock"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
)

type activitiesSuite struct {
	suite.Suite
	protoassert.ProtoAssertions
	testsuite.WorkflowTestSuite

	controller                    *gomock.Controller
	mockTaskManager               *persistence.MockTaskManager
	mockNamespaceReplicationQueue *persistence.MockNamespaceReplicationQueue
	mockNamespaceRegistry         *namespace.MockRegistry
	mockClientFactory             *client.MockFactory
	mockClientBean                *client.MockBean

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
	remoteCluster     = "remote_cluster"
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

	testNamespace = namespace.Namespace{}
)

func TestActivitiesSuite(t *testing.T) {
	suite.Run(t, new(activitiesSuite))
}

func (s *activitiesSuite) SetupTest() {
	s.ProtoAssertions = protoassert.New(s.T())
	s.controller = gomock.NewController(s.T())
	s.mockTaskManager = persistence.NewMockTaskManager(s.controller)
	s.mockNamespaceReplicationQueue = persistence.NewMockNamespaceReplicationQueue(s.controller)
	s.mockNamespaceRegistry = namespace.NewMockRegistry(s.controller)
	s.mockClientBean = client.NewMockBean(s.controller)

	s.mockFrontendClient = workflowservicemock.NewMockWorkflowServiceClient(s.controller)
	s.mockHistoryClient = historyservicemock.NewMockHistoryServiceClient(s.controller)
	s.mockRemoteAdminClient = adminservicemock.NewMockAdminServiceClient(s.controller)

	s.logger = log.NewNoopLogger()
	s.mockMetricsHandler = metrics.NewMockHandler(s.controller)
	s.mockMetricsHandler.EXPECT().WithTags(gomock.Any()).Return(s.mockMetricsHandler).AnyTimes()
	s.mockMetricsHandler.EXPECT().Timer(gomock.Any()).Return(metrics.NoopTimerMetricFunc).AnyTimes()
	s.mockMetricsHandler.EXPECT().Counter(gomock.Any()).Return(metrics.NoopCounterMetricFunc).AnyTimes()
	s.mockClientBean.EXPECT().GetRemoteAdminClient(remoteCluster).Return(s.mockRemoteAdminClient, nil).AnyTimes()
	s.mockNamespaceRegistry.EXPECT().GetNamespaceName(gomock.Any()).
		Return(namespace.Name(mockedNamespace), nil).AnyTimes()
	s.mockNamespaceRegistry.EXPECT().GetNamespace(gomock.Any()).
		Return(&testNamespace, nil).AnyTimes()

	s.a = &activities{
		namespaceRegistry:              s.mockNamespaceRegistry,
		namespaceReplicationQueue:      s.mockNamespaceReplicationQueue,
		clientFactory:                  s.mockClientFactory,
		clientBean:                     s.mockClientBean,
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
		Namespace:         mockedNamespace,
		NamespaceID:       mockedNamespaceID,
		TargetClusterName: remoteCluster,
		Executions:        []*commonpb.WorkflowExecution{&execution1, &execution2},
	}

	// Immediately replicated
	s.mockRemoteAdminClient.EXPECT().DescribeMutableState(gomock.Any(), protomock.Eq(&adminservice.DescribeMutableStateRequest{
		Namespace: mockedNamespace,
		Execution: &execution1,
	})).Return(&adminservice.DescribeMutableStateResponse{}, nil).Times(1)

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
		s.mockRemoteAdminClient.EXPECT().DescribeMutableState(gomock.Any(), protomock.Eq(&adminservice.DescribeMutableStateRequest{
			Namespace: mockedNamespace,
			Execution: &execution2,
		})).Return(r.resp, r.err).Times(1)
	}

	s.mockHistoryClient.EXPECT().DescribeMutableState(gomock.Any(), protomock.Eq(&historyservice.DescribeMutableStateRequest{
		NamespaceId: mockedNamespaceID,
		Execution:   &execution2,
	})).Return(&completeState, nil).Times(2)

	_, err := env.ExecuteActivity(s.a.VerifyReplicationTasks, &request)
	s.NoError(err)

	s.Greater(len(iceptor.replicationRecordedHeartbeats), 0)
	lastHeartBeat := iceptor.replicationRecordedHeartbeats[len(iceptor.replicationRecordedHeartbeats)-1]
	s.Equal(len(request.Executions), lastHeartBeat.NextIndex)
	s.ProtoEqual(&execution2, lastHeartBeat.LastNotVerifiedWorkflowExecution)
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
		Namespace:         mockedNamespace,
		NamespaceID:       mockedNamespaceID,
		TargetClusterName: remoteCluster,
		Executions:        []*commonpb.WorkflowExecution{&execution1},
	}

	start := time.Now()
	for _, t := range testcases {
		env, iceptor := s.initEnv()

		s.mockRemoteAdminClient.EXPECT().DescribeMutableState(gomock.Any(), protomock.Eq(&adminservice.DescribeMutableStateRequest{
			Namespace: mockedNamespace,
			Execution: &execution1,
		})).Return(nil, serviceerror.NewNotFound("")).Times(1)

		s.mockHistoryClient.EXPECT().DescribeMutableState(gomock.Any(), protomock.Eq(&historyservice.DescribeMutableStateRequest{
			NamespaceId: mockedNamespaceID,
			Execution:   &execution1,
		})).Return(t.resp, t.err).Times(1)

		_, err := env.ExecuteActivity(s.a.VerifyReplicationTasks, &request)
		s.Greater(len(iceptor.replicationRecordedHeartbeats), 0)
		lastHeartBeat := iceptor.replicationRecordedHeartbeats[len(iceptor.replicationRecordedHeartbeats)-1]
		if t.expectedErr == nil {
			s.NoError(err)
			s.Equal(len(request.Executions), lastHeartBeat.NextIndex)
		} else {
			s.ErrorContains(err, "mock error")
			s.Equal(0, lastHeartBeat.NextIndex)
		}

		s.Nil(lastHeartBeat.LastNotVerifiedWorkflowExecution)
		s.True(lastHeartBeat.CheckPoint.After(start))
	}
}

func (s *activitiesSuite) TestVerifyReplicationTasks_FailedNotFound() {
	env, iceptor := s.initEnv()
	request := verifyReplicationTasksRequest{
		Namespace:         mockedNamespace,
		NamespaceID:       mockedNamespaceID,
		TargetClusterName: remoteCluster,
		Executions:        []*commonpb.WorkflowExecution{&execution1},
	}

	s.mockHistoryClient.EXPECT().DescribeMutableState(gomock.Any(), protomock.Eq(&historyservice.DescribeMutableStateRequest{
		NamespaceId: mockedNamespaceID,
		Execution:   &execution1,
	})).Return(&completeState, nil)

	// Workflow not found at target cluster.
	s.mockRemoteAdminClient.EXPECT().DescribeMutableState(gomock.Any(), protomock.Eq(&adminservice.DescribeMutableStateRequest{
		Namespace: mockedNamespace,
		Execution: &execution1,
	})).Return(nil, serviceerror.NewNotFound("")).AnyTimes()

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
	s.ProtoEqual(&execution1, lastHeartBeat.LastNotVerifiedWorkflowExecution)
}

func (s *activitiesSuite) TestVerifyReplicationTasks_AlreadyVerified() {
	env, iceptor := s.initEnv()
	request := verifyReplicationTasksRequest{
		Namespace:         mockedNamespace,
		NamespaceID:       mockedNamespaceID,
		TargetClusterName: remoteCluster,
		Executions:        []*commonpb.WorkflowExecution{&execution1, &execution2},
	}

	// Set NextIndex to indicate all executions have been verified. No additional mock is needed.
	env.SetHeartbeatDetails(&replicationTasksHeartbeatDetails{
		NextIndex:  len(request.Executions),
		CheckPoint: time.Now(),
	})

	_, err := env.ExecuteActivity(s.a.VerifyReplicationTasks, &request)
	s.NoError(err)

	s.Equal(len(iceptor.replicationRecordedHeartbeats), 1)
}

func (s *activitiesSuite) Test_verifySingleReplicationTask() {
	request := verifyReplicationTasksRequest{
		Namespace:         mockedNamespace,
		NamespaceID:       mockedNamespaceID,
		TargetClusterName: remoteCluster,
		Executions:        []*commonpb.WorkflowExecution{&execution1, &execution2},
	}
	ctx := context.TODO()

	mockRemoteAdminClient := adminservicemock.NewMockAdminServiceClient(s.controller)
	mockRemoteAdminClient.EXPECT().DescribeMutableState(gomock.Any(), protomock.Eq(&adminservice.DescribeMutableStateRequest{
		Namespace: mockedNamespace,
		Execution: &execution1,
	})).Return(&adminservice.DescribeMutableStateResponse{}, nil).Times(1)
	result, err := s.a.verifySingleReplicationTask(ctx, &request, mockRemoteAdminClient, &testNamespace, request.Executions[0])
	s.NoError(err)
	s.True(result.isVerified())

	// Test not verified workflow
	mockRemoteAdminClient.EXPECT().DescribeMutableState(gomock.Any(), protomock.Eq(&adminservice.DescribeMutableStateRequest{
		Namespace: mockedNamespace,
		Execution: &execution2,
	})).Return(&adminservice.DescribeMutableStateResponse{}, serviceerror.NewNotFound("")).Times(1)

	s.mockHistoryClient.EXPECT().DescribeMutableState(gomock.Any(), protomock.Eq(&historyservice.DescribeMutableStateRequest{
		NamespaceId: mockedNamespaceID,
		Execution:   &execution2,
	})).Return(&completeState, nil).AnyTimes()

	result, err = s.a.verifySingleReplicationTask(ctx, &request, mockRemoteAdminClient, &testNamespace, request.Executions[1])
	s.NoError(err)
	s.False(result.isVerified())
}

type executionState int

const (
	executionFound    executionState = 0
	executionNotfound executionState = 1
	executionErr      executionState = 2
)

func createExecutions(mockClient *adminservicemock.MockAdminServiceClient, states []executionState, nextIndex int) []*commonpb.WorkflowExecution {
	var executions []*commonpb.WorkflowExecution

	for i := 0; i < len(states); i++ {
		executions = append(executions, &execution1)
	}

Loop:
	for i := nextIndex; i < len(states); i++ {
		switch states[i] {
		case executionFound:
			mockClient.EXPECT().DescribeMutableState(gomock.Any(), protomock.Eq(&adminservice.DescribeMutableStateRequest{
				Namespace: mockedNamespace,
				Execution: &execution1,
			})).Return(&adminservice.DescribeMutableStateResponse{}, nil).Times(1)
		case executionNotfound:
			mockClient.EXPECT().DescribeMutableState(gomock.Any(), protomock.Eq(&adminservice.DescribeMutableStateRequest{
				Namespace: mockedNamespace,
				Execution: &execution1,
			})).Return(nil, serviceerror.NewNotFound("")).Times(1)
			break Loop
		case executionErr:
			mockClient.EXPECT().DescribeMutableState(gomock.Any(), protomock.Eq(&adminservice.DescribeMutableStateRequest{
				Namespace: mockedNamespace,
				Execution: &execution1,
			})).Return(nil, serviceerror.NewInternal("")).Times(1)
		}
	}

	return executions
}

type mockHeartBeatRecorder struct {
	lastHeartBeat replicationTasksHeartbeatDetails
}

func (m *mockHeartBeatRecorder) hearbeat(details replicationTasksHeartbeatDetails) {
	m.lastHeartBeat = details
}

func (s *activitiesSuite) Test_verifyReplicationTasks() {
	request := verifyReplicationTasksRequest{
		Namespace:         mockedNamespace,
		NamespaceID:       mockedNamespaceID,
		TargetClusterName: remoteCluster,
	}

	ctx := context.TODO()

	var tests = []struct {
		remoteExecutionStates []executionState
		nextIndex             int
		expectedVerified      bool
		expectedErr           error
		expectedNextIndex     int
	}{
		{
			expectedVerified: true,
			expectedErr:      nil,
		},
		{
			remoteExecutionStates: []executionState{executionFound, executionFound, executionFound, executionFound},
			nextIndex:             0,
			expectedVerified:      true,
			expectedErr:           nil,
			expectedNextIndex:     4,
		},
		{
			remoteExecutionStates: []executionState{executionFound, executionFound, executionFound, executionFound},
			nextIndex:             2,
			expectedVerified:      true,
			expectedErr:           nil,
			expectedNextIndex:     4,
		},
		{
			remoteExecutionStates: []executionState{executionFound, executionFound, executionNotfound},
			nextIndex:             0,
			expectedVerified:      false,
			expectedErr:           nil,
			expectedNextIndex:     2,
		},
		{
			remoteExecutionStates: []executionState{executionFound, executionFound, executionNotfound, executionFound, executionNotfound},
			nextIndex:             0,
			expectedVerified:      false,
			expectedErr:           nil,
			expectedNextIndex:     2,
		},
	}

	s.mockHistoryClient.EXPECT().DescribeMutableState(gomock.Any(), protomock.Eq(&historyservice.DescribeMutableStateRequest{
		NamespaceId: mockedNamespaceID,
		Execution:   &execution1,
	})).Return(&completeState, nil).AnyTimes()

	checkPointTime := time.Now()
	for _, tc := range tests {
		var recorder mockHeartBeatRecorder
		mockRemoteAdminClient := adminservicemock.NewMockAdminServiceClient(s.controller)
		request.Executions = createExecutions(mockRemoteAdminClient, tc.remoteExecutionStates, tc.nextIndex)
		details := replicationTasksHeartbeatDetails{
			NextIndex:  tc.nextIndex,
			CheckPoint: checkPointTime,
		}

		verified, err := s.a.verifyReplicationTasks(ctx, &request, &details, mockRemoteAdminClient, &testNamespace, recorder.hearbeat)
		if tc.expectedErr == nil {
			s.NoError(err)
		}
		s.Equal(tc.expectedVerified, verified)
		s.Equal(tc.expectedNextIndex, details.NextIndex)
		s.GreaterOrEqual(len(tc.remoteExecutionStates), details.NextIndex)
		s.Equal(recorder.lastHeartBeat, details)
		if details.NextIndex < len(tc.remoteExecutionStates) && tc.remoteExecutionStates[details.NextIndex] == executionNotfound {
			s.ProtoEqual(&execution1, details.LastNotVerifiedWorkflowExecution)
		}

		if len(request.Executions) > 0 {
			// Except for empty Executions, all should set new CheckPoint to indicate making progress.
			s.True(checkPointTime.Before(details.CheckPoint))
		}
	}
}

func (s *activitiesSuite) Test_verifyReplicationTasksNoProgress() {
	var recorder mockHeartBeatRecorder
	mockRemoteAdminClient := adminservicemock.NewMockAdminServiceClient(s.controller)

	request := verifyReplicationTasksRequest{
		Namespace:         mockedNamespace,
		NamespaceID:       mockedNamespaceID,
		TargetClusterName: remoteCluster,
		Executions:        createExecutions(mockRemoteAdminClient, []executionState{executionFound, executionFound, executionNotfound, executionFound}, 0),
	}

	s.mockHistoryClient.EXPECT().DescribeMutableState(gomock.Any(), protomock.Eq(&historyservice.DescribeMutableStateRequest{
		NamespaceId: mockedNamespaceID,
		Execution:   &execution1,
	})).Return(&completeState, nil).AnyTimes()

	checkPointTime := time.Now()
	details := replicationTasksHeartbeatDetails{
		NextIndex:  0,
		CheckPoint: checkPointTime,
	}

	ctx := context.TODO()
	verified, err := s.a.verifyReplicationTasks(ctx, &request, &details, mockRemoteAdminClient, &testNamespace, recorder.hearbeat)
	s.NoError(err)
	s.False(verified)
	// Verify has made progress.
	s.True(checkPointTime.Before(details.CheckPoint))
	s.Equal(2, details.NextIndex)

	prevDetails := details

	// Mock for one more NotFound call
	mockRemoteAdminClient.EXPECT().DescribeMutableState(gomock.Any(), &adminservice.DescribeMutableStateRequest{
		Namespace: mockedNamespace,
		Execution: &execution1,
	}).Return(nil, serviceerror.NewNotFound("")).Times(1)

	// All results should be either NotFound or cached and no progress should be made.
	verified, err = s.a.verifyReplicationTasks(ctx, &request, &details, mockRemoteAdminClient, &testNamespace, recorder.hearbeat)
	s.NoError(err)
	s.False(verified)
	s.Equal(prevDetails, details)
}

func (s *activitiesSuite) Test_verifyReplicationTasksSkipRetention() {
	request := verifyReplicationTasksRequest{
		Namespace:         mockedNamespace,
		NamespaceID:       mockedNamespaceID,
		TargetClusterName: remoteCluster,
		Executions:        []*commonpb.WorkflowExecution{&execution1},
	}

	var tests = []struct {
		deleteDiff time.Duration // diff between deleteTime and now
		verified   bool
	}{
		{
			-30 * time.Second,
			true,
		},
		{
			30 * time.Second,
			false,
		},
	}

	for _, tc := range tests {
		var recorder mockHeartBeatRecorder
		deleteTime := time.Now().Add(tc.deleteDiff)
		retention := time.Hour
		closeTime := deleteTime.Add(-retention)

		mockRemoteAdminClient := adminservicemock.NewMockAdminServiceClient(s.controller)
		mockRemoteAdminClient.EXPECT().DescribeMutableState(gomock.Any(), protomock.Eq(&adminservice.DescribeMutableStateRequest{
			Namespace: mockedNamespace,
			Execution: &execution1,
		})).Return(nil, serviceerror.NewNotFound("")).Times(1)

		s.mockHistoryClient.EXPECT().DescribeMutableState(gomock.Any(), protomock.Eq(&historyservice.DescribeMutableStateRequest{
			NamespaceId: mockedNamespaceID,
			Execution:   &execution1,
		})).Return(&historyservice.DescribeMutableStateResponse{
			DatabaseMutableState: &persistencepb.WorkflowMutableState{
				ExecutionState: &persistencepb.WorkflowExecutionState{
					State: enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
				},
				ExecutionInfo: &persistencepb.WorkflowExecutionInfo{
					CloseTime: timestamppb.New(closeTime),
				},
			},
		}, nil).Times(1)

		ns := namespace.FromPersistentState(&persistence.GetNamespaceResponse{
			Namespace: &persistencepb.NamespaceDetail{
				Info: &persistencepb.NamespaceInfo{},
				Config: &persistencepb.NamespaceConfig{
					Retention: durationpb.New(retention),
				},
				ReplicationConfig: &persistencepb.NamespaceReplicationConfig{},
			},
		})

		details := replicationTasksHeartbeatDetails{}
		ctx := context.TODO()
		verified, err := s.a.verifyReplicationTasks(ctx, &request, &details, mockRemoteAdminClient, ns, recorder.hearbeat)
		s.NoError(err)
		s.Equal(tc.verified, verified)
		s.Equal(recorder.lastHeartBeat, details)
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
		Executions:  []*commonpb.WorkflowExecution{&execution1, &execution2},
	}

	for i := 0; i < len(request.Executions); i++ {
		we := request.Executions[i]
		s.mockHistoryClient.EXPECT().GenerateLastHistoryReplicationTasks(gomock.Any(), protomock.Eq(&historyservice.GenerateLastHistoryReplicationTasksRequest{
			NamespaceId: mockedNamespaceID,
			Execution:   we,
		})).Return(&historyservice.GenerateLastHistoryReplicationTasksResponse{}, nil).Times(1)
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
		Executions:  []*commonpb.WorkflowExecution{&execution1},
	}

	s.mockHistoryClient.EXPECT().GenerateLastHistoryReplicationTasks(gomock.Any(), protomock.Eq(&historyservice.GenerateLastHistoryReplicationTasksRequest{
		NamespaceId: mockedNamespaceID,
		Execution:   &execution1,
	})).Return(nil, serviceerror.NewNotFound("")).Times(1)

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
		Executions:  []*commonpb.WorkflowExecution{&execution1, &execution2},
	}

	s.mockHistoryClient.EXPECT().GenerateLastHistoryReplicationTasks(gomock.Any(), protomock.Eq(&historyservice.GenerateLastHistoryReplicationTasksRequest{
		NamespaceId: mockedNamespaceID,
		Execution:   &execution1,
	})).Return(&historyservice.GenerateLastHistoryReplicationTasksResponse{}, nil).Times(1)

	s.mockHistoryClient.EXPECT().GenerateLastHistoryReplicationTasks(gomock.Any(), protomock.Eq(&historyservice.GenerateLastHistoryReplicationTasksRequest{
		NamespaceId: mockedNamespaceID,
		Execution:   &execution2,
	})).Return(nil, serviceerror.NewInternal(""))

	_, err := env.ExecuteActivity(s.a.GenerateReplicationTasks, &request)
	s.Error(err)

	s.Greater(len(iceptor.generateReplicationRecordedHeartbeats), 0)
	lastIdx := len(iceptor.generateReplicationRecordedHeartbeats) - 1
	lastHeartBeat := iceptor.generateReplicationRecordedHeartbeats[lastIdx]
	// Only the generation of 1st execution suceeded.
	s.Equal(0, lastHeartBeat)
}
