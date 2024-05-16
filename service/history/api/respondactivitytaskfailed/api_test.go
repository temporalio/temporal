// The MIT License
//
// Copyright (c) 2023 Temporal Technologies Inc.  All rights reserved.
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

package respondactivitytaskfailed

import (
	"context"
	"fmt"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/cluster/clustertest"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencepb "go.temporal.io/server/api/persistence/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	persistencespb "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/workflow"
	wcache "go.temporal.io/server/service/history/workflow/cache"
)

type (
	workflowSuite struct {
		suite.Suite
		*require.Assertions

		controller        *gomock.Controller
		shardContext      *shard.MockContext
		namespaceRegistry *namespace.MockRegistry

		workflowCache              *wcache.MockCache
		workflowConsistencyChecker api.WorkflowConsistencyChecker

		workflowContext     *workflow.MockContext
		currentMutableState *workflow.MockMutableState

		activityInfo *persistencepb.ActivityInfo
	}
)

func TestWorkflowSuite(t *testing.T) {
	s := new(workflowSuite)
	suite.Run(t, s)
}

func (s *workflowSuite) SetupSuite() {
}

func (s *workflowSuite) TearDownSuite() {
}

func (s *workflowSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.controller = gomock.NewController(s.T())
}

func (s *workflowSuite) TearDownTest() {
	s.controller.Finish()
}

type UsecaseConfig struct {
	request             *historyservice.RespondActivityTaskFailedRequest
	attempt             int32
	activityId          string
	activityType        string
	startedEventId      int64
	scheduledEventId    int64
	taskQueueId         string
	isActivityActive    bool
	isExecutionRunning  bool
	expectRetryActivity bool
	retryActivityError  error
	retryActivityState  enumspb.RetryState
	namespaceId         namespace.ID
	namespaceName       namespace.Name
	wfType              *commonpb.WorkflowType
	tokenVersion        int64
	tokenAttempt        int32
	isCacheStale        bool
	includeHeartbeat    bool
}

func (s *workflowSuite) Test_NormalFlowShouldRescheduleActivity_UpdatesWorkflowExecutionAsActive() {
	ctx := context.Background()
	uc := newUseCase(UsecaseConfig{
		attempt:             int32(1),
		startedEventId:      int64(40),
		scheduledEventId:    int64(42),
		taskQueueId:         "some-task-queue",
		expectRetryActivity: true,
		isCacheStale:        false,
		retryActivityState:  enumspb.RETRY_STATE_IN_PROGRESS,
	})
	request := s.newRespondActivityTaskFailedRequest(uc)
	s.setupStubs(uc)

	s.expectTimerMetricsRecorded(uc, s.shardContext)
	s.workflowContext.EXPECT().UpdateWorkflowExecutionAsActive(ctx, s.shardContext).Return(nil)

	_, err := Invoke(ctx, request, s.shardContext, s.workflowConsistencyChecker)
	s.NoError(err)
}

func (s *workflowSuite) Test_WorkflowExecutionIsNotRunning_ReturnWorkflowNotRunningError() {
	uc := newUseCase(UsecaseConfig{
		attempt:            int32(1),
		startedEventId:     int64(40),
		scheduledEventId:   int64(42),
		isExecutionRunning: false,
	})
	request := s.newRespondActivityTaskFailedRequest(uc)
	s.setupStubs(uc)
	_, err := Invoke(
		context.Background(),
		request,
		s.shardContext,
		s.workflowConsistencyChecker,
	)
	s.Error(err)
	s.EqualValues(consts.ErrWorkflowCompleted, err)
}

func (s *workflowSuite) Test_CacheRefreshRequired_ReturnCacheStaleError() {
	uc := newUseCase(UsecaseConfig{
		attempt:            int32(1),
		startedEventId:     int64(40),
		scheduledEventId:   int64(42),
		isActivityActive:   false,
		isExecutionRunning: true,
		isCacheStale:       true,
	})
	s.setupStubs(uc)
	request := s.newRespondActivityTaskFailedRequest(uc)
	s.expectCounterRecorded(s.shardContext)

	_, err := Invoke(
		context.Background(),
		request,
		s.shardContext,
		s.workflowConsistencyChecker,
	)
	s.Error(err)
	s.EqualValues(consts.ErrStaleState, err)
}

func (s *workflowSuite) Test_ActivityTaskDoesNotExist_ActivityNotRunning() {
	uc := newUseCase(UsecaseConfig{
		attempt:            int32(1),
		startedEventId:     int64(40),
		scheduledEventId:   int64(42),
		isActivityActive:   false,
		isExecutionRunning: true,
	})
	s.setupStubs(uc)
	request := s.newRespondActivityTaskFailedRequest(uc)

	_, err := Invoke(
		context.Background(),
		request,
		s.shardContext,
		s.workflowConsistencyChecker,
	)
	s.Error(err)
	s.EqualValues(consts.ErrActivityTaskNotFound, err)
}

func (s *workflowSuite) Test_ActivityTaskDoesNotExist_TokenVersionDoesNotMatchActivityVersion() {
	uc := newUseCase(UsecaseConfig{
		attempt:            int32(1),
		startedEventId:     int64(40),
		scheduledEventId:   int64(42),
		isActivityActive:   true,
		isExecutionRunning: true,
		tokenVersion:       int64(72),
	})
	s.setupStubs(uc)
	request := s.newRespondActivityTaskFailedRequest(uc)

	_, err := Invoke(
		context.Background(),
		request,
		s.shardContext,
		s.workflowConsistencyChecker,
	)
	s.Error(err)
	s.EqualValues(consts.ErrActivityTaskNotFound, err)
}

func (s *workflowSuite) Test_ActivityTaskDoesNotExist_TokenVersionNonZeroAndAttemptDoesNotMatchActivityAttempt() {
	uc := newUseCase(UsecaseConfig{
		attempt:            int32(1),
		startedEventId:     int64(40),
		scheduledEventId:   int64(42),
		isActivityActive:   true,
		isExecutionRunning: true,
		tokenVersion:       int64(2),
		tokenAttempt:       int32(5),
	})
	s.setupStubs(uc)
	request := s.newRespondActivityTaskFailedRequest(uc)

	_, err := Invoke(
		context.Background(),
		request,
		s.shardContext,
		s.workflowConsistencyChecker,
	)
	s.Error(err)
	s.EqualValues(consts.ErrActivityTaskNotFound, err)
}

func (s *workflowSuite) Test_LastHeartBeatDetailsExist_UpdatesMutableState() {
	uc := newUseCase(UsecaseConfig{
		attempt:             int32(1),
		startedEventId:      int64(40),
		scheduledEventId:    int64(42),
		taskQueueId:         "some-task-queue",
		expectRetryActivity: true,
		retryActivityState:  enumspb.RETRY_STATE_IN_PROGRESS,
		isCacheStale:        false,
		includeHeartbeat:    true,
	})
	s.setupStubs(uc)
	request := s.newRespondActivityTaskFailedRequest(uc)

	ctx := context.Background()
	s.workflowContext.EXPECT().UpdateWorkflowExecutionAsActive(ctx, s.shardContext).Return(nil)
	s.currentMutableState.EXPECT().UpdateActivityProgress(s.activityInfo, &workflowservice.RecordActivityTaskHeartbeatRequest{
		TaskToken: request.FailedRequest.GetTaskToken(),
		Details:   request.FailedRequest.GetLastHeartbeatDetails(),
		Identity:  request.FailedRequest.GetIdentity(),
		Namespace: request.FailedRequest.GetNamespace(),
	})

	s.expectTimerMetricsRecorded(uc, s.shardContext)

	_, err := Invoke(
		ctx,
		request,
		s.shardContext,
		s.workflowConsistencyChecker,
	)

	s.NoError(err)
}

func (s *workflowSuite) Test_RetryActivityFailsWithAnError_WillReturnTheError() {
	retryError := fmt.Errorf("bizzare error")
	uc := newUseCase(UsecaseConfig{
		attempt:             int32(1),
		startedEventId:      int64(40),
		scheduledEventId:    int64(42),
		taskQueueId:         "some-task-queue",
		expectRetryActivity: true,
		retryActivityError:  retryError,
	})
	s.setupStubs(uc)
	request := s.newRespondActivityTaskFailedRequest(uc)

	_, err := Invoke(
		context.Background(),
		request,
		s.shardContext,
		s.workflowConsistencyChecker,
	)

	s.Error(err)
	s.Equal(retryError, err, "error from RetryActivity was not propagated expected %v got %v", retryError, err)
}

func (s *workflowSuite) Test_NoMoreRetriesAndMutableStateHasNoPendingTasks_WillRecordFailedEventAndAddWorkflowTaskScheduledEvent() {
	ctx := context.Background()
	uc := newUseCase(UsecaseConfig{
		attempt:             int32(1),
		startedEventId:      int64(40),
		scheduledEventId:    int64(42),
		taskQueueId:         "some-task-queue",
		expectRetryActivity: true,
		retryActivityState:  enumspb.RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED,
	})
	s.setupStubs(uc)
	request := s.newRespondActivityTaskFailedRequest(uc)
	s.expectTimerMetricsRecorded(uc, s.shardContext)
	s.currentMutableState.EXPECT().AddActivityTaskFailedEvent(
		uc.scheduledEventId,
		uc.startedEventId,
		request.FailedRequest.GetFailure(),
		uc.retryActivityState,
		request.FailedRequest.GetIdentity(),
		request.FailedRequest.WorkerVersion,
	).Return(nil, nil)
	s.currentMutableState.EXPECT().AddWorkflowTaskScheduledEvent(false, enumsspb.WORKFLOW_TASK_TYPE_NORMAL)
	s.workflowContext.EXPECT().UpdateWorkflowExecutionAsActive(ctx, s.shardContext).Return(nil)

	_, err := Invoke(ctx, request, s.shardContext, s.workflowConsistencyChecker)

	s.NoError(err)
}

func (s *workflowSuite) Test_AttemptToAddActivityTaskFailedEventFails_ReturnError() {
	addTaskError := fmt.Errorf("can't add task")
	uc := newUseCase(UsecaseConfig{
		attempt:             int32(1),
		startedEventId:      int64(40),
		scheduledEventId:    int64(42),
		taskQueueId:         "some-task-queue",
		expectRetryActivity: true,
		retryActivityState:  enumspb.RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED,
	})
	s.setupStubs(uc)
	request := s.newRespondActivityTaskFailedRequest(uc)
	s.currentMutableState.EXPECT().AddActivityTaskFailedEvent(
		uc.scheduledEventId,
		uc.startedEventId,
		request.FailedRequest.GetFailure(),
		uc.retryActivityState,
		request.FailedRequest.GetIdentity(),
		request.FailedRequest.WorkerVersion,
	).Return(nil, addTaskError)

	_, err := Invoke(
		context.Background(),
		request,
		s.shardContext,
		s.workflowConsistencyChecker,
	)

	s.Error(err)
	s.Equal(addTaskError, err)
}

func newUseCase(uconfig UsecaseConfig) UsecaseConfig {
	if uconfig.activityId == "" {
		uconfig.activityId = "activity-1"
	}
	if uconfig.wfType == nil {
		uconfig.wfType = &commonpb.WorkflowType{Name: "workflow-type"}
	}
	if uconfig.taskQueueId == "" {
		uconfig.taskQueueId = "some-task-queue"
	}
	if uconfig.namespaceId == "" {
		uconfig.namespaceId = namespace.ID("066935ba-910d-4656-bb56-85488e90b151")
	}
	if uconfig.expectRetryActivity {
		uconfig.isActivityActive = true
		uconfig.isExecutionRunning = true
	}
	return uconfig
}

func (s *workflowSuite) setupStubs(uc UsecaseConfig) {
	s.T().Helper()
	s.False(uc.isActivityActive && uc.isCacheStale, "either activity can be active or cache is stale not both")
	s.activityInfo = s.setupActivityInfo(uc)
	s.currentMutableState = s.setupMutableState(uc, s.activityInfo)
	s.namespaceRegistry = s.setupNamespaceRegistry(uc)
	s.shardContext = s.setupShardContext(s.namespaceRegistry)
	s.workflowContext = s.setupWorkflowContext(s.currentMutableState)
	s.workflowCache = s.setupCache()

	s.workflowConsistencyChecker = api.NewWorkflowConsistencyChecker(s.shardContext, s.workflowCache)
}

func (s *workflowSuite) newRespondActivityTaskFailedRequest(uc UsecaseConfig) *historyservice.RespondActivityTaskFailedRequest {
	s.T().Helper()
	tt := &tokenspb.Task{
		Attempt:          uc.tokenAttempt,
		NamespaceId:      uc.namespaceId.String(),
		WorkflowId:       tests.WorkflowID,
		ScheduledEventId: common.EmptyEventID,
		ActivityId:       uc.activityId,
		ActivityType:     uc.activityType,
		Version:          uc.tokenVersion,
	}
	taskToken, err := tt.Marshal()
	s.NoError(err)
	var hbDetails *commonpb.Payloads
	if uc.includeHeartbeat {
		hbDetails = &commonpb.Payloads{}
	}
	request := &historyservice.RespondActivityTaskFailedRequest{
		NamespaceId: uc.namespaceId.String(),
		FailedRequest: &workflowservice.RespondActivityTaskFailedRequest{
			Identity:             "ID1",
			Namespace:            uc.namespaceId.String(),
			TaskToken:            taskToken,
			LastHeartbeatDetails: hbDetails,
		},
	}
	return request
}

func (s *workflowSuite) setupWorkflowContext(mutableState *workflow.MockMutableState) *workflow.MockContext {
	workflowContext := workflow.NewMockContext(s.controller)
	workflowContext.EXPECT().LoadMutableState(gomock.Any(), s.shardContext).Return(mutableState, nil).AnyTimes()
	return workflowContext
}

func (s *workflowSuite) setupCache() *wcache.MockCache {
	workflowCache := wcache.NewMockCache(s.controller)
	workflowCache.EXPECT().GetOrCreateWorkflowExecution(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), workflow.LockPriorityHigh).
		Return(s.workflowContext, wcache.NoopReleaseFn, nil).AnyTimes()
	workflowCache.EXPECT().GetOrCreateCurrentWorkflowExecution(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), workflow.LockPriorityHigh).Return(wcache.NoopReleaseFn, nil).AnyTimes()
	return workflowCache
}

func (s *workflowSuite) setupShardContext(registry namespace.Registry) *shard.MockContext {
	shardContext := shard.NewMockContext(s.controller)
	shardContext.EXPECT().GetNamespaceRegistry().Return(registry).AnyTimes()
	shardContext.EXPECT().GetConfig().Return(tests.NewDynamicConfig()).AnyTimes()
	shardContext.EXPECT().GetLogger().Return(log.NewTestLogger()).AnyTimes()
	shardContext.EXPECT().GetThrottledLogger().Return(log.NewTestLogger()).AnyTimes()

	shardContext.EXPECT().GetTimeSource().Return(clock.NewRealTimeSource()).AnyTimes()
	shardContext.EXPECT().GetClusterMetadata().Return(clustertest.NewMetadataForTest(cluster.NewTestClusterMetadataConfig(true, true))).AnyTimes()
	shardContext.EXPECT().GetShardID().Return(int32(1)).AnyTimes()
	response := &persistencespb.GetCurrentExecutionResponse{
		RunID: tests.RunID,
	}
	shardContext.EXPECT().GetCurrentExecution(gomock.Any(), gomock.Any()).Return(response, nil).AnyTimes()
	return shardContext
}

func (s *workflowSuite) expectTimerMetricsRecorded(uc UsecaseConfig, shardContext *shard.MockContext) {
	timer := metrics.NewMockTimerIface(s.controller)
	timer.EXPECT().Record(
		gomock.Any(),
		metrics.OperationTag(metrics.HistoryRespondActivityTaskFailedScope),
		metrics.NamespaceTag(uc.namespaceName.String()),
		metrics.WorkflowTypeTag(uc.wfType.Name),
		metrics.ActivityTypeTag(uc.activityType),
		metrics.TaskQueueTag(uc.taskQueueId),
	)
	metricsHandler := metrics.NewMockHandler(s.controller)
	metricsHandler.EXPECT().Timer(metrics.ActivityE2ELatency.Name()).Return(timer)

	shardContext.EXPECT().GetMetricsHandler().Return(metricsHandler).AnyTimes()
}

func (s *workflowSuite) expectCounterRecorded(shardContext *shard.MockContext) *shard.MockContext {
	counter := metrics.NewMockCounterIface(s.controller)
	counter.EXPECT().Record(int64(1), metrics.OperationTag(metrics.HistoryRespondActivityTaskFailedScope))

	counterHandler := metrics.NewMockHandler(s.controller)
	counterHandler.EXPECT().Counter(gomock.Any()).Return(counter)
	shardContext.EXPECT().GetMetricsHandler().Return(counterHandler).AnyTimes()
	return shardContext
}

func (s *workflowSuite) setupNamespaceRegistry(uc UsecaseConfig) *namespace.MockRegistry {
	namespaceEntry := namespace.NewGlobalNamespaceForTest(
		&persistencepb.NamespaceInfo{
			Id:   uc.namespaceId.String(),
			Name: uc.namespaceName.String(),
		},
		&persistencepb.NamespaceConfig{
			Retention:               timestamp.DurationFromDays(1),
			VisibilityArchivalState: enumspb.ARCHIVAL_STATE_ENABLED,
			VisibilityArchivalUri:   "test:///visibility/archival",
		},
		&persistencepb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []string{
				cluster.TestCurrentClusterName,
				cluster.TestAlternativeClusterName,
			},
		},
		1234,
	)
	namespaceRegistry := namespace.NewMockRegistry(s.controller)
	namespaceRegistry.EXPECT().GetNamespaceByID(uc.namespaceId).Return(namespaceEntry, nil).AnyTimes()
	return namespaceRegistry
}

func (s *workflowSuite) setupMutableState(uc UsecaseConfig, ai *persistencepb.ActivityInfo) *workflow.MockMutableState {
	currentMutableState := workflow.NewMockMutableState(s.controller)
	currentMutableState.EXPECT().GetNamespaceEntry().Return(tests.GlobalNamespaceEntry).AnyTimes()
	currentMutableState.EXPECT().GetExecutionInfo().Return(&persistencepb.WorkflowExecutionInfo{
		WorkflowId: tests.WorkflowID,
	}).AnyTimes()
	currentMutableState.EXPECT().GetExecutionState().Return(&persistencepb.WorkflowExecutionState{
		RunId: tests.RunID,
	}).AnyTimes()
	currentMutableState.EXPECT().IsWorkflowExecutionRunning().Return(uc.isExecutionRunning).AnyTimes()

	currentMutableState.EXPECT().GetActivityByActivityID(uc.activityId).Return(ai, true).AnyTimes()
	currentMutableState.EXPECT().GetActivityInfo(uc.scheduledEventId).Return(ai, uc.isActivityActive).AnyTimes()
	if uc.isExecutionRunning == true && uc.isActivityActive == false {
		if uc.isCacheStale {
			currentMutableState.EXPECT().GetNextEventID().Return(uc.scheduledEventId - 4).AnyTimes()
		} else {
			currentMutableState.EXPECT().GetNextEventID().Return(uc.scheduledEventId + 4).AnyTimes()
		}
	}

	currentMutableState.EXPECT().GetWorkflowType().Return(uc.wfType).AnyTimes()
	if uc.expectRetryActivity {
		currentMutableState.EXPECT().RetryActivity(ai, gomock.Any()).Return(uc.retryActivityState, uc.retryActivityError)
		currentMutableState.EXPECT().HasPendingWorkflowTask().Return(false).AnyTimes()
	}
	return currentMutableState
}

func (s *workflowSuite) setupActivityInfo(uc UsecaseConfig) *persistencepb.ActivityInfo {
	return &persistencepb.ActivityInfo{
		ScheduledEventId: uc.scheduledEventId,
		Attempt:          uc.attempt,
		StartedEventId:   uc.startedEventId,
		TaskQueue:        uc.taskQueueId,
	}
}
