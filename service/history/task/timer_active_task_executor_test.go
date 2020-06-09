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

package task

import (
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/uber/cadence/.gen/go/history"
	"github.com/uber/cadence/.gen/go/matching"
	"github.com/uber/cadence/.gen/go/matching/matchingservicetest"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/clock"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/definition"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/mocks"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/service/history/config"
	"github.com/uber/cadence/service/history/constants"
	"github.com/uber/cadence/service/history/engine"
	"github.com/uber/cadence/service/history/events"
	"github.com/uber/cadence/service/history/execution"
	"github.com/uber/cadence/service/history/shard"
	test "github.com/uber/cadence/service/history/testing"
)

type (
	timerActiveTaskExecutorSuite struct {
		suite.Suite
		*require.Assertions

		controller          *gomock.Controller
		mockShard           *shard.TestContext
		mockEngine          *engine.MockEngine
		mockDomainCache     *cache.MockDomainCache
		mockMatchingClient  *matchingservicetest.MockClient
		mockClusterMetadata *cluster.MockMetadata

		mockExecutionMgr *mocks.ExecutionManager
		mockHistoryV2Mgr *mocks.HistoryV2Manager

		executionCache          *execution.Cache
		logger                  log.Logger
		domainID                string
		domainEntry             *cache.DomainCacheEntry
		version                 int64
		now                     time.Time
		timeSource              *clock.EventTimeSource
		timerActiveTaskExecutor *timerActiveTaskExecutor
	}
)

func TestTimerActiveTaskExecutorSuite(t *testing.T) {
	s := new(timerActiveTaskExecutorSuite)
	suite.Run(t, s)
}

func (s *timerActiveTaskExecutorSuite) SetupSuite() {

}

func (s *timerActiveTaskExecutorSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.domainID = constants.TestDomainID
	s.domainEntry = constants.TestGlobalDomainEntry
	s.version = s.domainEntry.GetFailoverVersion()
	s.now = time.Now()
	s.timeSource = clock.NewEventTimeSource().Update(s.now)

	s.controller = gomock.NewController(s.T())

	config := config.NewForTest()
	s.mockShard = shard.NewTestContext(
		s.controller,
		&persistence.ShardInfo{
			RangeID:          1,
			TransferAckLevel: 0,
		},
		config,
	)
	s.mockShard.SetEventsCache(events.NewCache(
		s.mockShard.GetShardID(),
		s.mockShard.GetHistoryManager(),
		s.mockShard.GetConfig(),
		s.mockShard.GetLogger(),
		s.mockShard.GetMetricsClient(),
	))
	s.mockShard.Resource.TimeSource = s.timeSource

	s.mockEngine = engine.NewMockEngine(s.controller)
	s.mockEngine.EXPECT().NotifyNewHistoryEvent(gomock.Any()).AnyTimes()
	s.mockEngine.EXPECT().NotifyNewTransferTasks(gomock.Any()).AnyTimes()
	s.mockEngine.EXPECT().NotifyNewReplicationTasks(gomock.Any()).AnyTimes()
	s.mockEngine.EXPECT().NotifyNewTimerTasks(gomock.Any()).AnyTimes()
	s.mockShard.SetEngine(s.mockEngine)

	s.mockDomainCache = s.mockShard.Resource.DomainCache
	s.mockMatchingClient = s.mockShard.Resource.MatchingClient
	s.mockExecutionMgr = s.mockShard.Resource.ExecutionMgr
	s.mockHistoryV2Mgr = s.mockShard.Resource.HistoryMgr
	s.mockClusterMetadata = s.mockShard.Resource.ClusterMetadata
	// ack manager will use the domain information
	s.mockDomainCache.EXPECT().GetDomainByID(gomock.Any()).Return(constants.TestGlobalDomainEntry, nil).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestAllClusterInfo).AnyTimes()
	s.mockClusterMetadata.EXPECT().IsGlobalDomainEnabled().Return(true).AnyTimes()
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(s.version).Return(s.mockClusterMetadata.GetCurrentClusterName()).AnyTimes()

	s.logger = s.mockShard.GetLogger()
	s.executionCache = execution.NewCache(s.mockShard)
	s.timerActiveTaskExecutor = NewTimerActiveTaskExecutor(
		s.mockShard,
		nil,
		s.executionCache,
		s.logger,
		s.mockShard.GetMetricsClient(),
		config,
	).(*timerActiveTaskExecutor)
}

func (s *timerActiveTaskExecutorSuite) TearDownTest() {
	s.controller.Finish()
	s.mockShard.Finish(s.T())
}

func (s *timerActiveTaskExecutorSuite) TestProcessUserTimerTimeout_Fire() {

	workflowExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(uuid.New()),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	mutableState := execution.NewMutableStateBuilderWithReplicationStateWithEventV2(
		s.mockShard,
		s.logger,
		s.version,
		workflowExecution.GetRunId(),
		constants.TestGlobalDomainEntry,
	)
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		workflowExecution,
		&history.StartWorkflowExecutionRequest{
			DomainUUID: common.StringPtr(s.domainID),
			StartRequest: &workflow.StartWorkflowExecutionRequest{
				WorkflowType:                        &workflow.WorkflowType{Name: common.StringPtr(workflowType)},
				TaskList:                            &workflow.TaskList{Name: common.StringPtr(taskListName)},
				ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(2),
				TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
			},
		},
	)
	s.Nil(err)

	di := test.AddDecisionTaskScheduledEvent(mutableState)
	event := test.AddDecisionTaskStartedEvent(mutableState, di.ScheduleID, taskListName, uuid.New())
	di.StartedID = event.GetEventId()
	event = test.AddDecisionTaskCompletedEvent(mutableState, di.ScheduleID, di.StartedID, nil, "some random identity")

	timerID := "timer"
	timerTimeout := 2 * time.Second
	event, _ = test.AddTimerStartedEvent(mutableState, event.GetEventId(), timerID, int64(timerTimeout.Seconds()))

	timerSequence := execution.NewTimerSequence(s.timeSource, mutableState)
	mutableState.DeleteTimerTasks()
	modified, err := timerSequence.CreateNextUserTimer()
	s.NoError(err)
	s.True(modified)
	task := mutableState.GetTimerTasks()[0]
	timerTask := &persistence.TimerTaskInfo{
		Version:             s.version,
		DomainID:            s.domainID,
		WorkflowID:          workflowExecution.GetWorkflowId(),
		RunID:               workflowExecution.GetRunId(),
		TaskID:              int64(100),
		TaskType:            persistence.TaskTypeUserTimer,
		TimeoutType:         int(workflow.TimeoutTypeStartToClose),
		VisibilityTimestamp: task.(*persistence.UserTimerTask).GetVisibilityTimestamp(),
		EventID:             event.GetEventId(),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&persistence.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&persistence.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &persistence.MutableStateUpdateSessionStats{}}, nil).Once()

	s.timeSource.Update(s.now.Add(2 * timerTimeout))
	err = s.timerActiveTaskExecutor.Execute(timerTask, true)
	s.NoError(err)

	_, ok := s.getMutableStateFromCache(s.domainID, workflowExecution.GetWorkflowId(), workflowExecution.GetRunId()).GetUserTimerInfo(timerID)
	s.False(ok)
}

func (s *timerActiveTaskExecutorSuite) TestProcessUserTimerTimeout_Noop() {

	workflowExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(uuid.New()),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	mutableState := execution.NewMutableStateBuilderWithReplicationStateWithEventV2(
		s.mockShard,
		s.logger,
		s.version,
		workflowExecution.GetRunId(),
		constants.TestGlobalDomainEntry,
	)
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		workflowExecution,
		&history.StartWorkflowExecutionRequest{
			DomainUUID: common.StringPtr(s.domainID),
			StartRequest: &workflow.StartWorkflowExecutionRequest{
				WorkflowType:                        &workflow.WorkflowType{Name: common.StringPtr(workflowType)},
				TaskList:                            &workflow.TaskList{Name: common.StringPtr(taskListName)},
				ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(2),
				TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
			},
		},
	)
	s.Nil(err)

	di := test.AddDecisionTaskScheduledEvent(mutableState)
	event := test.AddDecisionTaskStartedEvent(mutableState, di.ScheduleID, taskListName, uuid.New())
	di.StartedID = event.GetEventId()
	event = test.AddDecisionTaskCompletedEvent(mutableState, di.ScheduleID, di.StartedID, nil, "some random identity")

	timerID := "timer"
	timerTimeout := 2 * time.Second
	event, _ = test.AddTimerStartedEvent(mutableState, event.GetEventId(), timerID, int64(timerTimeout.Seconds()))

	timerSequence := execution.NewTimerSequence(s.timeSource, mutableState)
	mutableState.DeleteTimerTasks()
	modified, err := timerSequence.CreateNextUserTimer()
	s.NoError(err)
	s.True(modified)
	task := mutableState.GetTimerTasks()[0]
	timerTask := &persistence.TimerTaskInfo{
		Version:             s.version,
		DomainID:            s.domainID,
		WorkflowID:          workflowExecution.GetWorkflowId(),
		RunID:               workflowExecution.GetRunId(),
		TaskID:              int64(100),
		TaskType:            persistence.TaskTypeUserTimer,
		TimeoutType:         int(workflow.TimeoutTypeStartToClose),
		VisibilityTimestamp: task.(*persistence.UserTimerTask).GetVisibilityTimestamp(),
		EventID:             event.GetEventId(),
	}

	event = test.AddTimerFiredEvent(mutableState, timerID)

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	s.timeSource.Update(s.now.Add(2 * timerTimeout))
	err = s.timerActiveTaskExecutor.Execute(timerTask, true)
	s.NoError(err)
}

func (s *timerActiveTaskExecutorSuite) TestProcessActivityTimeout_NoRetryPolicy_Fire() {

	workflowExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(uuid.New()),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	mutableState := execution.NewMutableStateBuilderWithReplicationStateWithEventV2(
		s.mockShard,
		s.logger,
		s.version,
		workflowExecution.GetRunId(),
		constants.TestGlobalDomainEntry,
	)
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		workflowExecution,
		&history.StartWorkflowExecutionRequest{
			DomainUUID: common.StringPtr(s.domainID),
			StartRequest: &workflow.StartWorkflowExecutionRequest{
				WorkflowType:                        &workflow.WorkflowType{Name: common.StringPtr(workflowType)},
				TaskList:                            &workflow.TaskList{Name: common.StringPtr(taskListName)},
				ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(2),
				TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
			},
		},
	)
	s.Nil(err)

	di := test.AddDecisionTaskScheduledEvent(mutableState)
	event := test.AddDecisionTaskStartedEvent(mutableState, di.ScheduleID, taskListName, uuid.New())
	di.StartedID = event.GetEventId()
	event = test.AddDecisionTaskCompletedEvent(mutableState, di.ScheduleID, di.StartedID, nil, "some random identity")

	tasklist := "tasklist"
	activityID := "activity"
	activityType := "activity type"
	timerTimeout := 2 * time.Second
	scheduledEvent, _ := test.AddActivityTaskScheduledEvent(
		mutableState,
		event.GetEventId(),
		activityID,
		activityType,
		tasklist,
		[]byte(nil),
		int32(timerTimeout.Seconds()),
		int32(timerTimeout.Seconds()),
		int32(timerTimeout.Seconds()),
		int32(timerTimeout.Seconds()),
	)

	timerSequence := execution.NewTimerSequence(s.timeSource, mutableState)
	mutableState.DeleteTimerTasks()
	modified, err := timerSequence.CreateNextActivityTimer()
	s.NoError(err)
	s.True(modified)
	task := mutableState.GetTimerTasks()[0]
	timerTask := &persistence.TimerTaskInfo{
		Version:             s.version,
		DomainID:            s.domainID,
		WorkflowID:          workflowExecution.GetWorkflowId(),
		RunID:               workflowExecution.GetRunId(),
		TaskID:              int64(100),
		TaskType:            persistence.TaskTypeActivityTimeout,
		TimeoutType:         int(workflow.TimeoutTypeScheduleToClose),
		VisibilityTimestamp: task.(*persistence.ActivityTimeoutTask).GetVisibilityTimestamp(),
		EventID:             scheduledEvent.GetEventId(),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, scheduledEvent.GetEventId(), scheduledEvent.GetVersion())
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&persistence.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&persistence.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &persistence.MutableStateUpdateSessionStats{}}, nil).Once()

	s.timeSource.Update(s.now.Add(2 * timerTimeout))
	err = s.timerActiveTaskExecutor.Execute(timerTask, true)
	s.NoError(err)

	_, ok := s.getMutableStateFromCache(s.domainID, workflowExecution.GetWorkflowId(), workflowExecution.GetRunId()).GetActivityInfo(scheduledEvent.GetEventId())
	s.False(ok)
}

func (s *timerActiveTaskExecutorSuite) TestProcessActivityTimeout_NoRetryPolicy_Noop() {

	workflowExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(uuid.New()),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	mutableState := execution.NewMutableStateBuilderWithReplicationStateWithEventV2(
		s.mockShard,
		s.logger,
		s.version,
		workflowExecution.GetRunId(),
		constants.TestGlobalDomainEntry,
	)
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		workflowExecution,
		&history.StartWorkflowExecutionRequest{
			DomainUUID: common.StringPtr(s.domainID),
			StartRequest: &workflow.StartWorkflowExecutionRequest{
				WorkflowType:                        &workflow.WorkflowType{Name: common.StringPtr(workflowType)},
				TaskList:                            &workflow.TaskList{Name: common.StringPtr(taskListName)},
				ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(2),
				TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
			},
		},
	)
	s.Nil(err)

	di := test.AddDecisionTaskScheduledEvent(mutableState)
	event := test.AddDecisionTaskStartedEvent(mutableState, di.ScheduleID, taskListName, uuid.New())
	di.StartedID = event.GetEventId()
	event = test.AddDecisionTaskCompletedEvent(mutableState, di.ScheduleID, di.StartedID, nil, "some random identity")

	identity := "identity"
	tasklist := "tasklist"
	activityID := "activity"
	activityType := "activity type"
	timerTimeout := 2 * time.Second
	scheduledEvent, _ := test.AddActivityTaskScheduledEvent(
		mutableState,
		event.GetEventId(),
		activityID,
		activityType,
		tasklist,
		[]byte(nil),
		int32(timerTimeout.Seconds()),
		int32(timerTimeout.Seconds()),
		int32(timerTimeout.Seconds()),
		int32(timerTimeout.Seconds()),
	)
	startedEvent := test.AddActivityTaskStartedEvent(mutableState, scheduledEvent.GetEventId(), identity)

	timerSequence := execution.NewTimerSequence(s.timeSource, mutableState)
	mutableState.DeleteTimerTasks()
	modified, err := timerSequence.CreateNextActivityTimer()
	s.NoError(err)
	s.True(modified)
	task := mutableState.GetTimerTasks()[0]
	timerTask := &persistence.TimerTaskInfo{
		Version:             s.version,
		DomainID:            s.domainID,
		WorkflowID:          workflowExecution.GetWorkflowId(),
		RunID:               workflowExecution.GetRunId(),
		TaskID:              int64(100),
		TaskType:            persistence.TaskTypeActivityTimeout,
		TimeoutType:         int(workflow.TimeoutTypeScheduleToClose),
		VisibilityTimestamp: task.(*persistence.ActivityTimeoutTask).GetVisibilityTimestamp(),
		EventID:             scheduledEvent.GetEventId(),
	}

	completeEvent := test.AddActivityTaskCompletedEvent(mutableState, scheduledEvent.GetEventId(), startedEvent.GetEventId(), []byte(nil), identity)

	persistenceMutableState := s.createPersistenceMutableState(mutableState, completeEvent.GetEventId(), completeEvent.GetVersion())
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	s.timeSource.Update(s.now.Add(2 * timerTimeout))
	err = s.timerActiveTaskExecutor.Execute(timerTask, true)
	s.NoError(err)
}

func (s *timerActiveTaskExecutorSuite) TestProcessActivityTimeout_RetryPolicy_Retry() {

	workflowExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(uuid.New()),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	mutableState := execution.NewMutableStateBuilderWithReplicationStateWithEventV2(
		s.mockShard,
		s.logger,
		s.version,
		workflowExecution.GetRunId(),
		constants.TestGlobalDomainEntry,
	)
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		workflowExecution,
		&history.StartWorkflowExecutionRequest{
			DomainUUID: common.StringPtr(s.domainID),
			StartRequest: &workflow.StartWorkflowExecutionRequest{
				WorkflowType:                        &workflow.WorkflowType{Name: common.StringPtr(workflowType)},
				TaskList:                            &workflow.TaskList{Name: common.StringPtr(taskListName)},
				ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(2),
				TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
			},
		},
	)
	s.Nil(err)

	di := test.AddDecisionTaskScheduledEvent(mutableState)
	event := test.AddDecisionTaskStartedEvent(mutableState, di.ScheduleID, taskListName, uuid.New())
	di.StartedID = event.GetEventId()
	event = test.AddDecisionTaskCompletedEvent(mutableState, di.ScheduleID, di.StartedID, nil, "some random identity")

	identity := "identity"
	tasklist := "tasklist"
	activityID := "activity"
	activityType := "activity type"
	timerTimeout := 2 * time.Second
	scheduledEvent, _ := test.AddActivityTaskScheduledEventWithRetry(
		mutableState,
		event.GetEventId(),
		activityID,
		activityType,
		tasklist,
		[]byte(nil),
		int32(timerTimeout.Seconds()),
		int32(timerTimeout.Seconds()),
		int32(timerTimeout.Seconds()),
		int32(timerTimeout.Seconds()),
		&workflow.RetryPolicy{
			InitialIntervalInSeconds:    common.Int32Ptr(1),
			BackoffCoefficient:          common.Float64Ptr(1.2),
			MaximumIntervalInSeconds:    common.Int32Ptr(5),
			MaximumAttempts:             common.Int32Ptr(5),
			NonRetriableErrorReasons:    []string{"（╯' - ')╯ ┻━┻ "},
			ExpirationIntervalInSeconds: common.Int32Ptr(999),
		},
	)
	startedEvent := test.AddActivityTaskStartedEvent(mutableState, scheduledEvent.GetEventId(), identity)
	s.Nil(startedEvent)

	timerSequence := execution.NewTimerSequence(s.timeSource, mutableState)
	mutableState.DeleteTimerTasks()
	modified, err := timerSequence.CreateNextActivityTimer()
	s.NoError(err)
	s.True(modified)
	task := mutableState.GetTimerTasks()[0]
	timerTask := &persistence.TimerTaskInfo{
		Version:             s.version,
		DomainID:            s.domainID,
		WorkflowID:          workflowExecution.GetWorkflowId(),
		RunID:               workflowExecution.GetRunId(),
		TaskID:              int64(100),
		TaskType:            persistence.TaskTypeActivityTimeout,
		TimeoutType:         int(workflow.TimeoutTypeScheduleToClose),
		VisibilityTimestamp: task.(*persistence.ActivityTimeoutTask).GetVisibilityTimestamp(),
		EventID:             scheduledEvent.GetEventId(),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, scheduledEvent.GetEventId(), scheduledEvent.GetVersion())
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&persistence.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &persistence.MutableStateUpdateSessionStats{}}, nil).Once()

	s.timeSource.Update(s.now.Add(2 * timerTimeout))
	err = s.timerActiveTaskExecutor.Execute(timerTask, true)
	s.NoError(err)

	activityInfo, ok := s.getMutableStateFromCache(s.domainID, workflowExecution.GetWorkflowId(), workflowExecution.GetRunId()).GetActivityInfo(scheduledEvent.GetEventId())
	s.True(ok)
	s.Equal(scheduledEvent.GetEventId(), activityInfo.ScheduleID)
	s.Equal(common.EmptyEventID, activityInfo.StartedID)
	// only a schedule to start timer will be created, apart from the retry timer
	s.Equal(int32(execution.TimerTaskStatusCreatedScheduleToStart), activityInfo.TimerTaskStatus)
}

func (s *timerActiveTaskExecutorSuite) TestProcessActivityTimeout_RetryPolicy_Fire() {

	workflowExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(uuid.New()),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	mutableState := execution.NewMutableStateBuilderWithReplicationStateWithEventV2(
		s.mockShard,
		s.logger,
		s.version,
		workflowExecution.GetRunId(),
		constants.TestGlobalDomainEntry,
	)
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		workflowExecution,
		&history.StartWorkflowExecutionRequest{
			DomainUUID: common.StringPtr(s.domainID),
			StartRequest: &workflow.StartWorkflowExecutionRequest{
				WorkflowType:                        &workflow.WorkflowType{Name: common.StringPtr(workflowType)},
				TaskList:                            &workflow.TaskList{Name: common.StringPtr(taskListName)},
				ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(2),
				TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
			},
		},
	)
	s.Nil(err)

	di := test.AddDecisionTaskScheduledEvent(mutableState)
	event := test.AddDecisionTaskStartedEvent(mutableState, di.ScheduleID, taskListName, uuid.New())
	di.StartedID = event.GetEventId()
	event = test.AddDecisionTaskCompletedEvent(mutableState, di.ScheduleID, di.StartedID, nil, "some random identity")

	tasklist := "tasklist"
	activityID := "activity"
	activityType := "activity type"
	timerTimeout := 2 * time.Second
	scheduledEvent, _ := test.AddActivityTaskScheduledEventWithRetry(
		mutableState,
		event.GetEventId(),
		activityID,
		activityType,
		tasklist,
		[]byte(nil),
		int32(timerTimeout.Seconds()),
		int32(timerTimeout.Seconds()),
		int32(timerTimeout.Seconds()),
		int32(timerTimeout.Seconds()),
		&workflow.RetryPolicy{
			InitialIntervalInSeconds:    common.Int32Ptr(1),
			BackoffCoefficient:          common.Float64Ptr(1.2),
			MaximumIntervalInSeconds:    common.Int32Ptr(5),
			MaximumAttempts:             common.Int32Ptr(5),
			NonRetriableErrorReasons:    []string{"（╯' - ')╯ ┻━┻ "},
			ExpirationIntervalInSeconds: common.Int32Ptr(999),
		},
	)

	timerSequence := execution.NewTimerSequence(s.timeSource, mutableState)
	mutableState.DeleteTimerTasks()
	modified, err := timerSequence.CreateNextActivityTimer()
	s.NoError(err)
	s.True(modified)
	task := mutableState.GetTimerTasks()[0]
	timerTask := &persistence.TimerTaskInfo{
		Version:             s.version,
		DomainID:            s.domainID,
		WorkflowID:          workflowExecution.GetWorkflowId(),
		RunID:               workflowExecution.GetRunId(),
		TaskID:              int64(100),
		TaskType:            persistence.TaskTypeActivityTimeout,
		TimeoutType:         int(workflow.TimeoutTypeScheduleToClose),
		VisibilityTimestamp: task.(*persistence.ActivityTimeoutTask).GetVisibilityTimestamp(),
		EventID:             scheduledEvent.GetEventId(),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, scheduledEvent.GetEventId(), scheduledEvent.GetVersion())
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&persistence.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&persistence.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &persistence.MutableStateUpdateSessionStats{}}, nil).Once()

	s.timeSource.Update(s.now.Add(2 * timerTimeout))
	err = s.timerActiveTaskExecutor.Execute(timerTask, true)
	s.NoError(err)

	_, ok := s.getMutableStateFromCache(s.domainID, workflowExecution.GetWorkflowId(), workflowExecution.GetRunId()).GetActivityInfo(scheduledEvent.GetEventId())
	s.False(ok)
}

func (s *timerActiveTaskExecutorSuite) TestProcessActivityTimeout_RetryPolicy_Noop() {
	workflowExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(uuid.New()),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	mutableState := execution.NewMutableStateBuilderWithReplicationStateWithEventV2(
		s.mockShard,
		s.logger,
		s.version,
		workflowExecution.GetRunId(),
		constants.TestGlobalDomainEntry,
	)
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		workflowExecution,
		&history.StartWorkflowExecutionRequest{
			DomainUUID: common.StringPtr(s.domainID),
			StartRequest: &workflow.StartWorkflowExecutionRequest{
				WorkflowType:                        &workflow.WorkflowType{Name: common.StringPtr(workflowType)},
				TaskList:                            &workflow.TaskList{Name: common.StringPtr(taskListName)},
				ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(2),
				TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
			},
		},
	)
	s.Nil(err)

	di := test.AddDecisionTaskScheduledEvent(mutableState)
	event := test.AddDecisionTaskStartedEvent(mutableState, di.ScheduleID, taskListName, uuid.New())
	di.StartedID = event.GetEventId()
	event = test.AddDecisionTaskCompletedEvent(mutableState, di.ScheduleID, di.StartedID, nil, "some random identity")

	identity := "identity"
	tasklist := "tasklist"
	activityID := "activity"
	activityType := "activity type"
	timerTimeout := 2 * time.Second
	scheduledEvent, _ := test.AddActivityTaskScheduledEventWithRetry(
		mutableState,
		event.GetEventId(),
		activityID,
		activityType,
		tasklist,
		[]byte(nil),
		int32(timerTimeout.Seconds()),
		int32(timerTimeout.Seconds()),
		int32(timerTimeout.Seconds()),
		int32(timerTimeout.Seconds()),
		&workflow.RetryPolicy{
			InitialIntervalInSeconds:    common.Int32Ptr(1),
			BackoffCoefficient:          common.Float64Ptr(1.2),
			MaximumIntervalInSeconds:    common.Int32Ptr(5),
			MaximumAttempts:             common.Int32Ptr(5),
			NonRetriableErrorReasons:    []string{"（╯' - ')╯ ┻━┻ "},
			ExpirationIntervalInSeconds: common.Int32Ptr(999),
		},
	)
	startedEvent := test.AddActivityTaskStartedEvent(mutableState, scheduledEvent.GetEventId(), identity)
	s.Nil(startedEvent)

	timerSequence := execution.NewTimerSequence(s.timeSource, mutableState)
	mutableState.DeleteTimerTasks()
	modified, err := timerSequence.CreateNextActivityTimer()
	s.NoError(err)
	s.True(modified)
	task := mutableState.GetTimerTasks()[0]
	timerTask := &persistence.TimerTaskInfo{
		Version:             s.version,
		DomainID:            s.domainID,
		WorkflowID:          workflowExecution.GetWorkflowId(),
		RunID:               workflowExecution.GetRunId(),
		TaskID:              int64(100),
		TaskType:            persistence.TaskTypeActivityTimeout,
		TimeoutType:         int(workflow.TimeoutTypeScheduleToClose),
		VisibilityTimestamp: task.(*persistence.ActivityTimeoutTask).GetVisibilityTimestamp(),
		EventID:             scheduledEvent.GetEventId(),
	}

	completeEvent := test.AddActivityTaskCompletedEvent(mutableState, scheduledEvent.GetEventId(), common.TransientEventID, []byte(nil), identity)

	persistenceMutableState := s.createPersistenceMutableState(mutableState, completeEvent.GetEventId(), completeEvent.GetVersion())
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	s.timeSource.Update(s.now.Add(2 * timerTimeout))
	err = s.timerActiveTaskExecutor.Execute(timerTask, true)
	s.NoError(err)
}

func (s *timerActiveTaskExecutorSuite) TestProcessActivityTimeout_Heartbeat_Noop() {
	workflowExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(uuid.New()),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	mutableState := execution.NewMutableStateBuilderWithReplicationStateWithEventV2(
		s.mockShard,
		s.logger,
		s.version,
		workflowExecution.GetRunId(),
		constants.TestGlobalDomainEntry,
	)
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		workflowExecution,
		&history.StartWorkflowExecutionRequest{
			DomainUUID: common.StringPtr(s.domainID),
			StartRequest: &workflow.StartWorkflowExecutionRequest{
				WorkflowType:                        &workflow.WorkflowType{Name: common.StringPtr(workflowType)},
				TaskList:                            &workflow.TaskList{Name: common.StringPtr(taskListName)},
				ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(2),
				TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
			},
		},
	)
	s.Nil(err)

	di := test.AddDecisionTaskScheduledEvent(mutableState)
	event := test.AddDecisionTaskStartedEvent(mutableState, di.ScheduleID, taskListName, uuid.New())
	di.StartedID = event.GetEventId()
	event = test.AddDecisionTaskCompletedEvent(mutableState, di.ScheduleID, di.StartedID, nil, "some random identity")

	identity := "identity"
	tasklist := "tasklist"
	activityID := "activity"
	activityType := "activity type"
	timerTimeout := 2 * time.Second
	heartbeatTimerTimeout := time.Second
	scheduledEvent, _ := test.AddActivityTaskScheduledEventWithRetry(
		mutableState,
		event.GetEventId(),
		activityID,
		activityType,
		tasklist,
		[]byte(nil),
		int32(timerTimeout.Seconds()),
		int32(timerTimeout.Seconds()),
		int32(timerTimeout.Seconds()),
		int32(heartbeatTimerTimeout.Seconds()),
		&workflow.RetryPolicy{
			InitialIntervalInSeconds:    common.Int32Ptr(1),
			BackoffCoefficient:          common.Float64Ptr(1.2),
			MaximumIntervalInSeconds:    common.Int32Ptr(5),
			MaximumAttempts:             common.Int32Ptr(5),
			NonRetriableErrorReasons:    []string{"（╯' - ')╯ ┻━┻ "},
			ExpirationIntervalInSeconds: common.Int32Ptr(999),
		},
	)
	startedEvent := test.AddActivityTaskStartedEvent(mutableState, scheduledEvent.GetEventId(), identity)
	s.Nil(startedEvent)

	timerSequence := execution.NewTimerSequence(s.timeSource, mutableState)
	mutableState.DeleteTimerTasks()
	modified, err := timerSequence.CreateNextActivityTimer()
	s.NoError(err)
	s.True(modified)
	task := mutableState.GetTimerTasks()[0]
	s.Equal(int(execution.TimerTypeHeartbeat), task.(*persistence.ActivityTimeoutTask).TimeoutType)
	timerTask := &persistence.TimerTaskInfo{
		Version:             s.version,
		DomainID:            s.domainID,
		WorkflowID:          workflowExecution.GetWorkflowId(),
		RunID:               workflowExecution.GetRunId(),
		TaskID:              int64(100),
		TaskType:            persistence.TaskTypeActivityTimeout,
		TimeoutType:         int(workflow.TimeoutTypeHeartbeat),
		VisibilityTimestamp: task.(*persistence.ActivityTimeoutTask).GetVisibilityTimestamp().Add(-time.Second),
		EventID:             scheduledEvent.GetEventId(),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, scheduledEvent.GetEventId(), scheduledEvent.GetVersion())
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	err = s.timerActiveTaskExecutor.Execute(timerTask, true)
	s.NoError(err)
}

func (s *timerActiveTaskExecutorSuite) TestDecisionTimeout_Fire() {

	workflowExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(uuid.New()),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	mutableState := execution.NewMutableStateBuilderWithReplicationStateWithEventV2(
		s.mockShard,
		s.logger,
		s.version,
		workflowExecution.GetRunId(),
		constants.TestGlobalDomainEntry,
	)
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		workflowExecution,
		&history.StartWorkflowExecutionRequest{
			DomainUUID: common.StringPtr(s.domainID),
			StartRequest: &workflow.StartWorkflowExecutionRequest{
				WorkflowType:                        &workflow.WorkflowType{Name: common.StringPtr(workflowType)},
				TaskList:                            &workflow.TaskList{Name: common.StringPtr(taskListName)},
				ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(2),
				TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
			},
		},
	)
	s.Nil(err)

	di := test.AddDecisionTaskScheduledEvent(mutableState)
	startedEvent := test.AddDecisionTaskStartedEvent(mutableState, di.ScheduleID, taskListName, uuid.New())

	timerTask := &persistence.TimerTaskInfo{
		Version:             s.version,
		DomainID:            s.domainID,
		WorkflowID:          workflowExecution.GetWorkflowId(),
		RunID:               workflowExecution.GetRunId(),
		TaskID:              int64(100),
		TaskType:            persistence.TaskTypeDecisionTimeout,
		TimeoutType:         int(workflow.TimeoutTypeStartToClose),
		VisibilityTimestamp: s.now,
		EventID:             di.ScheduleID,
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, startedEvent.GetEventId(), startedEvent.GetVersion())
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&persistence.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&persistence.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &persistence.MutableStateUpdateSessionStats{}}, nil).Once()

	err = s.timerActiveTaskExecutor.Execute(timerTask, true)
	s.NoError(err)

	decisionInfo, ok := s.getMutableStateFromCache(s.domainID, workflowExecution.GetWorkflowId(), workflowExecution.GetRunId()).GetPendingDecision()
	s.True(ok)
	s.True(decisionInfo.ScheduleID != common.EmptyEventID)
	s.Equal(common.EmptyEventID, decisionInfo.StartedID)
	s.Equal(int64(1), decisionInfo.Attempt)
}

func (s *timerActiveTaskExecutorSuite) TestDecisionTimeout_Noop() {

	workflowExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(uuid.New()),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	mutableState := execution.NewMutableStateBuilderWithReplicationStateWithEventV2(
		s.mockShard,
		s.logger,
		s.version,
		workflowExecution.GetRunId(),
		constants.TestGlobalDomainEntry,
	)
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		workflowExecution,
		&history.StartWorkflowExecutionRequest{
			DomainUUID: common.StringPtr(s.domainID),
			StartRequest: &workflow.StartWorkflowExecutionRequest{
				WorkflowType:                        &workflow.WorkflowType{Name: common.StringPtr(workflowType)},
				TaskList:                            &workflow.TaskList{Name: common.StringPtr(taskListName)},
				ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(2),
				TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
			},
		},
	)
	s.Nil(err)

	di := test.AddDecisionTaskScheduledEvent(mutableState)
	startedEvent := test.AddDecisionTaskStartedEvent(mutableState, di.ScheduleID, taskListName, uuid.New())

	timerTask := &persistence.TimerTaskInfo{
		Version:             s.version,
		DomainID:            s.domainID,
		WorkflowID:          workflowExecution.GetWorkflowId(),
		RunID:               workflowExecution.GetRunId(),
		TaskID:              int64(100),
		TaskType:            persistence.TaskTypeDecisionTimeout,
		TimeoutType:         int(workflow.TimeoutTypeStartToClose),
		VisibilityTimestamp: s.now,
		EventID:             di.ScheduleID - 1,
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, startedEvent.GetEventId(), startedEvent.GetVersion())
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil).Once()

	err = s.timerActiveTaskExecutor.Execute(timerTask, true)
	s.NoError(err)
}

func (s *timerActiveTaskExecutorSuite) TestWorkflowBackoffTimer_Fire() {

	workflowExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(uuid.New()),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	mutableState := execution.NewMutableStateBuilderWithReplicationStateWithEventV2(
		s.mockShard,
		s.logger,
		s.version,
		workflowExecution.GetRunId(),
		constants.TestGlobalDomainEntry,
	)
	event, err := mutableState.AddWorkflowExecutionStartedEvent(
		workflowExecution,
		&history.StartWorkflowExecutionRequest{
			DomainUUID: common.StringPtr(s.domainID),
			StartRequest: &workflow.StartWorkflowExecutionRequest{
				WorkflowType:                        &workflow.WorkflowType{Name: common.StringPtr(workflowType)},
				TaskList:                            &workflow.TaskList{Name: common.StringPtr(taskListName)},
				ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(2),
				TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
			},
		},
	)
	s.Nil(err)

	timerTask := &persistence.TimerTaskInfo{
		Version:             s.version,
		DomainID:            s.domainID,
		WorkflowID:          workflowExecution.GetWorkflowId(),
		RunID:               workflowExecution.GetRunId(),
		TaskID:              int64(100),
		TaskType:            persistence.TaskTypeWorkflowBackoffTimer,
		TimeoutType:         persistence.WorkflowBackoffTimeoutTypeRetry,
		VisibilityTimestamp: s.now,
		EventID:             0,
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil).Once()
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&persistence.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&persistence.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &persistence.MutableStateUpdateSessionStats{}}, nil).Once()

	err = s.timerActiveTaskExecutor.Execute(timerTask, true)
	s.NoError(err)

	decisionInfo, ok := s.getMutableStateFromCache(s.domainID, workflowExecution.GetWorkflowId(), workflowExecution.GetRunId()).GetPendingDecision()
	s.True(ok)
	s.True(decisionInfo.ScheduleID != common.EmptyEventID)
	s.Equal(common.EmptyEventID, decisionInfo.StartedID)
	s.Equal(int64(0), decisionInfo.Attempt)
}

func (s *timerActiveTaskExecutorSuite) TestWorkflowBackoffTimer_Noop() {

	workflowExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(uuid.New()),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	mutableState := execution.NewMutableStateBuilderWithReplicationStateWithEventV2(
		s.mockShard,
		s.logger,
		s.version,
		workflowExecution.GetRunId(),
		constants.TestGlobalDomainEntry,
	)
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		workflowExecution,
		&history.StartWorkflowExecutionRequest{
			DomainUUID: common.StringPtr(s.domainID),
			StartRequest: &workflow.StartWorkflowExecutionRequest{
				WorkflowType:                        &workflow.WorkflowType{Name: common.StringPtr(workflowType)},
				TaskList:                            &workflow.TaskList{Name: common.StringPtr(taskListName)},
				ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(2),
				TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
			},
		},
	)
	s.Nil(err)

	di := test.AddDecisionTaskScheduledEvent(mutableState)
	event := test.AddDecisionTaskStartedEvent(mutableState, di.ScheduleID, taskListName, uuid.New())
	di.StartedID = event.GetEventId()
	event = test.AddDecisionTaskCompletedEvent(mutableState, di.ScheduleID, di.StartedID, nil, "some random identity")

	timerTask := &persistence.TimerTaskInfo{
		Version:             s.version,
		DomainID:            s.domainID,
		WorkflowID:          workflowExecution.GetWorkflowId(),
		RunID:               workflowExecution.GetRunId(),
		TaskID:              int64(100),
		TaskType:            persistence.TaskTypeWorkflowBackoffTimer,
		TimeoutType:         persistence.WorkflowBackoffTimeoutTypeRetry,
		VisibilityTimestamp: s.now,
		EventID:             0,
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, event.GetEventId(), event.GetVersion())
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil).Once()

	err = s.timerActiveTaskExecutor.Execute(timerTask, true)
	s.NoError(err)
}

func (s *timerActiveTaskExecutorSuite) TestActivityRetryTimer_Fire() {

	workflowExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(uuid.New()),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	mutableState := execution.NewMutableStateBuilderWithReplicationStateWithEventV2(
		s.mockShard,
		s.logger,
		s.version,
		workflowExecution.GetRunId(),
		constants.TestGlobalDomainEntry,
	)
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		workflowExecution,
		&history.StartWorkflowExecutionRequest{
			DomainUUID: common.StringPtr(s.domainID),
			StartRequest: &workflow.StartWorkflowExecutionRequest{
				WorkflowType:                        &workflow.WorkflowType{Name: common.StringPtr(workflowType)},
				TaskList:                            &workflow.TaskList{Name: common.StringPtr(taskListName)},
				ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(2),
				TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
			},
		},
	)
	s.Nil(err)

	di := test.AddDecisionTaskScheduledEvent(mutableState)
	event := test.AddDecisionTaskStartedEvent(mutableState, di.ScheduleID, taskListName, uuid.New())
	di.StartedID = event.GetEventId()
	event = test.AddDecisionTaskCompletedEvent(mutableState, di.ScheduleID, di.StartedID, nil, "some random identity")

	tasklist := "tasklist"
	activityID := "activity"
	activityType := "activity type"
	timerTimeout := 2 * time.Second
	scheduledEvent, activityInfo := test.AddActivityTaskScheduledEventWithRetry(
		mutableState,
		event.GetEventId(),
		activityID,
		activityType,
		tasklist,
		[]byte(nil),
		int32(timerTimeout.Seconds()),
		int32(timerTimeout.Seconds()),
		int32(timerTimeout.Seconds()),
		int32(timerTimeout.Seconds()),
		&workflow.RetryPolicy{
			InitialIntervalInSeconds:    common.Int32Ptr(1),
			BackoffCoefficient:          common.Float64Ptr(1.2),
			MaximumIntervalInSeconds:    common.Int32Ptr(5),
			MaximumAttempts:             common.Int32Ptr(5),
			NonRetriableErrorReasons:    []string{"（╯' - ')╯ ┻━┻ "},
			ExpirationIntervalInSeconds: common.Int32Ptr(999),
		},
	)
	activityInfo.Attempt = 1

	timerTask := &persistence.TimerTaskInfo{
		Version:             s.version,
		DomainID:            s.domainID,
		WorkflowID:          workflowExecution.GetWorkflowId(),
		RunID:               workflowExecution.GetRunId(),
		TaskID:              int64(100),
		TaskType:            persistence.TaskTypeActivityRetryTimer,
		TimeoutType:         0,
		VisibilityTimestamp: s.now,
		EventID:             activityInfo.ScheduleID,
		ScheduleAttempt:     int64(activityInfo.Attempt),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, scheduledEvent.GetEventId(), scheduledEvent.GetVersion())
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockMatchingClient.EXPECT().AddActivityTask(
		gomock.Any(),
		&matching.AddActivityTaskRequest{
			DomainUUID:       common.StringPtr(activityInfo.DomainID),
			SourceDomainUUID: common.StringPtr(activityInfo.DomainID),
			Execution:        &workflowExecution,
			TaskList: &workflow.TaskList{
				Name: common.StringPtr(activityInfo.TaskList),
			},
			ScheduleId:                    common.Int64Ptr(activityInfo.ScheduleID),
			ScheduleToStartTimeoutSeconds: common.Int32Ptr(activityInfo.ScheduleToStartTimeout),
		},
	).Return(nil).Times(1)

	err = s.timerActiveTaskExecutor.Execute(timerTask, true)
	s.NoError(err)
}

func (s *timerActiveTaskExecutorSuite) TestActivityRetryTimer_Noop() {

	workflowExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(uuid.New()),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	mutableState := execution.NewMutableStateBuilderWithReplicationStateWithEventV2(
		s.mockShard,
		s.logger,
		s.version,
		workflowExecution.GetRunId(),
		constants.TestGlobalDomainEntry,
	)
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		workflowExecution,
		&history.StartWorkflowExecutionRequest{
			DomainUUID: common.StringPtr(s.domainID),
			StartRequest: &workflow.StartWorkflowExecutionRequest{
				WorkflowType:                        &workflow.WorkflowType{Name: common.StringPtr(workflowType)},
				TaskList:                            &workflow.TaskList{Name: common.StringPtr(taskListName)},
				ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(2),
				TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
			},
		},
	)
	s.Nil(err)

	di := test.AddDecisionTaskScheduledEvent(mutableState)
	event := test.AddDecisionTaskStartedEvent(mutableState, di.ScheduleID, taskListName, uuid.New())
	di.StartedID = event.GetEventId()
	event = test.AddDecisionTaskCompletedEvent(mutableState, di.ScheduleID, di.StartedID, nil, "some random identity")

	identity := "identity"
	tasklist := "tasklist"
	activityID := "activity"
	activityType := "activity type"
	timerTimeout := 2 * time.Second
	scheduledEvent, activityInfo := test.AddActivityTaskScheduledEventWithRetry(
		mutableState,
		event.GetEventId(),
		activityID,
		activityType,
		tasklist,
		[]byte(nil),
		int32(timerTimeout.Seconds()),
		int32(timerTimeout.Seconds()),
		int32(timerTimeout.Seconds()),
		int32(timerTimeout.Seconds()),
		&workflow.RetryPolicy{
			InitialIntervalInSeconds:    common.Int32Ptr(1),
			BackoffCoefficient:          common.Float64Ptr(1.2),
			MaximumIntervalInSeconds:    common.Int32Ptr(5),
			MaximumAttempts:             common.Int32Ptr(5),
			NonRetriableErrorReasons:    []string{"（╯' - ')╯ ┻━┻ "},
			ExpirationIntervalInSeconds: common.Int32Ptr(999),
		},
	)
	startedEvent := test.AddActivityTaskStartedEvent(mutableState, scheduledEvent.GetEventId(), identity)
	s.Nil(startedEvent)

	timerTask := &persistence.TimerTaskInfo{
		Version:             s.version,
		DomainID:            s.domainID,
		WorkflowID:          workflowExecution.GetWorkflowId(),
		RunID:               workflowExecution.GetRunId(),
		TaskID:              int64(100),
		TaskType:            persistence.TaskTypeActivityRetryTimer,
		TimeoutType:         0,
		VisibilityTimestamp: s.now,
		EventID:             activityInfo.ScheduleID,
		ScheduleAttempt:     int64(activityInfo.Attempt),
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, scheduledEvent.GetEventId(), scheduledEvent.GetVersion())
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)

	err = s.timerActiveTaskExecutor.Execute(timerTask, true)
	s.NoError(err)
}

func (s *timerActiveTaskExecutorSuite) TestWorkflowTimeout_Fire() {

	workflowExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(uuid.New()),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	mutableState := execution.NewMutableStateBuilderWithReplicationStateWithEventV2(
		s.mockShard,
		s.logger,
		s.version,
		workflowExecution.GetRunId(),
		constants.TestGlobalDomainEntry,
	)
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		workflowExecution,
		&history.StartWorkflowExecutionRequest{
			DomainUUID: common.StringPtr(s.domainID),
			StartRequest: &workflow.StartWorkflowExecutionRequest{
				WorkflowType:                        &workflow.WorkflowType{Name: common.StringPtr(workflowType)},
				TaskList:                            &workflow.TaskList{Name: common.StringPtr(taskListName)},
				ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(2),
				TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
			},
		},
	)
	s.Nil(err)

	di := test.AddDecisionTaskScheduledEvent(mutableState)
	startEvent := test.AddDecisionTaskStartedEvent(mutableState, di.ScheduleID, taskListName, uuid.New())
	di.StartedID = startEvent.GetEventId()
	completionEvent := test.AddDecisionTaskCompletedEvent(mutableState, di.ScheduleID, di.StartedID, nil, "some random identity")

	timerTask := &persistence.TimerTaskInfo{
		Version:             s.version,
		DomainID:            s.domainID,
		WorkflowID:          workflowExecution.GetWorkflowId(),
		RunID:               workflowExecution.GetRunId(),
		TaskID:              int64(100),
		TaskType:            persistence.TaskTypeWorkflowTimeout,
		TimeoutType:         int(workflow.TimeoutTypeStartToClose),
		VisibilityTimestamp: s.now,
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, completionEvent.GetEventId(), completionEvent.GetVersion())
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&persistence.AppendHistoryNodesResponse{Size: 0}, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&persistence.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &persistence.MutableStateUpdateSessionStats{}}, nil).Once()

	err = s.timerActiveTaskExecutor.Execute(timerTask, true)
	s.NoError(err)

	running := s.getMutableStateFromCache(s.domainID, workflowExecution.GetWorkflowId(), workflowExecution.GetRunId()).IsWorkflowExecutionRunning()
	s.False(running)
}

func (s *timerActiveTaskExecutorSuite) TestWorkflowTimeout_ContinueAsNew_Retry() {

	workflowExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(uuid.New()),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	mutableState := execution.NewMutableStateBuilderWithReplicationStateWithEventV2(
		s.mockShard,
		s.logger,
		s.version,
		workflowExecution.GetRunId(),
		constants.TestGlobalDomainEntry,
	)
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		workflowExecution,
		&history.StartWorkflowExecutionRequest{
			DomainUUID: common.StringPtr(s.domainID),
			StartRequest: &workflow.StartWorkflowExecutionRequest{
				WorkflowType:                        &workflow.WorkflowType{Name: common.StringPtr(workflowType)},
				TaskList:                            &workflow.TaskList{Name: common.StringPtr(taskListName)},
				ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(2),
				TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
			},
		},
	)
	s.Nil(err)
	// need to override the workflow retry policy
	executionInfo := mutableState.GetExecutionInfo()
	executionInfo.HasRetryPolicy = true
	executionInfo.ExpirationTime = s.now.Add(1000 * time.Second)
	executionInfo.MaximumAttempts = 10
	executionInfo.InitialInterval = 1
	executionInfo.MaximumInterval = 1
	executionInfo.BackoffCoefficient = 1

	di := test.AddDecisionTaskScheduledEvent(mutableState)
	startEvent := test.AddDecisionTaskStartedEvent(mutableState, di.ScheduleID, taskListName, uuid.New())
	di.StartedID = startEvent.GetEventId()
	completionEvent := test.AddDecisionTaskCompletedEvent(mutableState, di.ScheduleID, di.StartedID, nil, "some random identity")

	timerTask := &persistence.TimerTaskInfo{
		Version:             s.version,
		DomainID:            s.domainID,
		WorkflowID:          workflowExecution.GetWorkflowId(),
		RunID:               workflowExecution.GetRunId(),
		TaskID:              int64(100),
		TaskType:            persistence.TaskTypeWorkflowTimeout,
		TimeoutType:         int(workflow.TimeoutTypeStartToClose),
		VisibilityTimestamp: s.now,
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, completionEvent.GetEventId(), completionEvent.GetVersion())
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	// one for current workflow, one for new
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&persistence.AppendHistoryNodesResponse{Size: 0}, nil).Times(2)
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&persistence.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &persistence.MutableStateUpdateSessionStats{}}, nil).Once()

	err = s.timerActiveTaskExecutor.Execute(timerTask, true)
	s.NoError(err)

	state, closeStatus := s.getMutableStateFromCache(s.domainID, workflowExecution.GetWorkflowId(), workflowExecution.GetRunId()).GetWorkflowStateCloseStatus()
	s.Equal(persistence.WorkflowStateCompleted, state)
	s.Equal(persistence.WorkflowCloseStatusContinuedAsNew, closeStatus)
}

func (s *timerActiveTaskExecutorSuite) TestWorkflowTimeout_ContinueAsNew_Cron() {

	workflowExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(uuid.New()),
	}
	workflowType := "some random workflow type"
	taskListName := "some random task list"

	mutableState := execution.NewMutableStateBuilderWithReplicationStateWithEventV2(
		s.mockShard,
		s.logger,
		s.version,
		workflowExecution.GetRunId(),
		constants.TestGlobalDomainEntry,
	)
	_, err := mutableState.AddWorkflowExecutionStartedEvent(
		workflowExecution,
		&history.StartWorkflowExecutionRequest{
			DomainUUID: common.StringPtr(s.domainID),
			StartRequest: &workflow.StartWorkflowExecutionRequest{
				WorkflowType:                        &workflow.WorkflowType{Name: common.StringPtr(workflowType)},
				TaskList:                            &workflow.TaskList{Name: common.StringPtr(taskListName)},
				ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(2),
				TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
			},
		},
	)
	s.Nil(err)
	executionInfo := mutableState.GetExecutionInfo()
	executionInfo.StartTimestamp = s.now
	executionInfo.CronSchedule = "* * * * *"

	di := test.AddDecisionTaskScheduledEvent(mutableState)
	startEvent := test.AddDecisionTaskStartedEvent(mutableState, di.ScheduleID, taskListName, uuid.New())
	di.StartedID = startEvent.GetEventId()
	completionEvent := test.AddDecisionTaskCompletedEvent(mutableState, di.ScheduleID, di.StartedID, nil, "some random identity")

	timerTask := &persistence.TimerTaskInfo{
		Version:             s.version,
		DomainID:            s.domainID,
		WorkflowID:          workflowExecution.GetWorkflowId(),
		RunID:               workflowExecution.GetRunId(),
		TaskID:              int64(100),
		TaskType:            persistence.TaskTypeWorkflowTimeout,
		TimeoutType:         int(workflow.TimeoutTypeStartToClose),
		VisibilityTimestamp: s.now,
	}

	persistenceMutableState := s.createPersistenceMutableState(mutableState, completionEvent.GetEventId(), completionEvent.GetVersion())
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{State: persistenceMutableState}, nil)
	// one for current workflow, one for new
	s.mockHistoryV2Mgr.On("AppendHistoryNodes", mock.Anything).Return(&persistence.AppendHistoryNodesResponse{Size: 0}, nil).Times(2)
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&persistence.UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: &persistence.MutableStateUpdateSessionStats{}}, nil).Once()

	err = s.timerActiveTaskExecutor.Execute(timerTask, true)
	s.NoError(err)

	state, closeStatus := s.getMutableStateFromCache(s.domainID, workflowExecution.GetWorkflowId(), workflowExecution.GetRunId()).GetWorkflowStateCloseStatus()
	s.Equal(persistence.WorkflowStateCompleted, state)
	s.Equal(persistence.WorkflowCloseStatusContinuedAsNew, closeStatus)
}

func (s *timerActiveTaskExecutorSuite) createPersistenceMutableState(
	ms execution.MutableState,
	lastEventID int64,
	lastEventVersion int64,
) *persistence.WorkflowMutableState {

	if ms.GetReplicationState() != nil {
		ms.UpdateReplicationStateLastEventID(lastEventVersion, lastEventID)
	} else if ms.GetVersionHistories() != nil {
		currentVersionHistory, err := ms.GetVersionHistories().GetCurrentVersionHistory()
		s.NoError(err)
		err = currentVersionHistory.AddOrUpdateItem(persistence.NewVersionHistoryItem(
			lastEventID, lastEventVersion,
		))
		s.NoError(err)
	}

	return execution.CreatePersistenceMutableState(ms)
}

func (s *timerActiveTaskExecutorSuite) getMutableStateFromCache(
	domainID string,
	workflowID string,
	runID string,
) execution.MutableState {

	return s.executionCache.Get(
		definition.NewWorkflowIdentifier(domainID, workflowID, runID),
	).(execution.Context).GetWorkflowExecution()
}
