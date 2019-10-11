// Copyright (c) 2017 Uber Technologies, Inc.
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

package history

import (
	"fmt"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"

	"github.com/uber/cadence/.gen/go/matching/matchingservicetest"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/client/matching"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/clock"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/loggerimpl"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/mocks"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/service/dynamicconfig"
)

type (
	timerQueueProcessorSuite struct {
		suite.Suite
		TestBase
		engineImpl     *historyEngineImpl
		matchingClient matching.Client
		shardClosedCh  chan int
		logger         log.Logger

		controller               *gomock.Controller
		mockVisibilityMgr        *mocks.VisibilityManager
		mockMatchingClient       *matchingservicetest.MockClient
		mockClusterMetadata      *mocks.ClusterMetadata
		mockEventsCache          *MockEventsCache
		mockTxProcessor          *MockTransferQueueProcessor
		mockReplicationProcessor *MockReplicatorQueueProcessor
		mockTimerProcessor       *MockTimerQueueProcessor
	}
)

func TestTimerQueueProcessorSuite(t *testing.T) {
	s := new(timerQueueProcessorSuite)
	suite.Run(t, s)
}

func (s *timerQueueProcessorSuite) SetupTest() {

	s.logger = loggerimpl.NewDevelopmentForTest(s.Suite)

	s.SetupWorkflowStore()
	s.SetupDomains()

	// override config for testing
	s.ShardContext.config.ShardUpdateMinInterval = dynamicconfig.GetDurationPropertyFn(0 * time.Second)
	s.ShardContext.config.TransferProcessorCompleteTransferInterval = dynamicconfig.GetDurationPropertyFn(100 * time.Millisecond)
	s.ShardContext.config.TimerProcessorCompleteTimerInterval = dynamicconfig.GetDurationPropertyFn(100 * time.Millisecond)
	s.ShardContext.config.TransferProcessorUpdateAckInterval = dynamicconfig.GetDurationPropertyFn(100 * time.Millisecond)
	s.ShardContext.config.TimerProcessorUpdateAckInterval = dynamicconfig.GetDurationPropertyFn(100 * time.Millisecond)

	s.controller = gomock.NewController(s.T())

	s.mockMatchingClient = matchingservicetest.NewMockClient(s.controller)
	s.mockClusterMetadata = &mocks.ClusterMetadata{}

	s.mockClusterMetadata.On("IsGlobalDomainEnabled").Return(false)
	s.mockClusterMetadata.On("GetCurrentClusterName").Return(cluster.TestCurrentClusterName)
	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", common.EmptyVersion).Return(cluster.TestCurrentClusterName)
	s.mockTxProcessor = &MockTransferQueueProcessor{}
	s.mockTxProcessor.On("NotifyNewTask", mock.Anything, mock.Anything).Maybe()
	s.mockReplicationProcessor = NewMockReplicatorQueueProcessor(s.controller)
	s.mockReplicationProcessor.EXPECT().notifyNewTask().AnyTimes()
	s.mockTimerProcessor = &MockTimerQueueProcessor{}
	s.mockTimerProcessor.On("NotifyNewTimers", mock.Anything, mock.Anything).Maybe()

	historyCache := newHistoryCache(s.ShardContext)
	historyCache.disabled = true
	// set the standby cluster's timer ack level to max since we are not testing it
	// but we are testing the complete timer functionality
	err := s.ShardContext.UpdateTimerClusterAckLevel(cluster.TestAlternativeClusterName, maximumTime)
	s.Nil(err)
	s.engineImpl = &historyEngineImpl{
		currentClusterName:   s.ShardContext.GetService().GetClusterMetadata().GetCurrentClusterName(),
		shard:                s.ShardContext,
		clusterMetadata:      s.ShardContext.GetClusterMetadata(),
		historyCache:         historyCache,
		logger:               s.logger,
		tokenSerializer:      common.NewJSONTaskTokenSerializer(),
		metricsClient:        metrics.NewClient(tally.NoopScope, metrics.History),
		historyEventNotifier: newHistoryEventNotifier(clock.NewRealTimeSource(), metrics.NewClient(tally.NoopScope, metrics.History), func(string) int { return 0 }),
		txProcessor:          s.mockTxProcessor,
		replicatorProcessor:  s.mockReplicationProcessor,
		timerProcessor:       s.mockTimerProcessor,
	}
	s.ShardContext.SetEngine(s.engineImpl)
	s.engineImpl.historyEventNotifier = newHistoryEventNotifier(clock.NewRealTimeSource(), metrics.NewClient(tally.NoopScope, metrics.History), func(string) int { return 0 })
	s.engineImpl.txProcessor = newTransferQueueProcessor(
		s.ShardContext, s.engineImpl, s.mockVisibilityMgr, nil, nil, s.logger,
	)
	s.engineImpl.replicatorProcessor = newReplicatorQueueProcessor(s.ShardContext, historyCache, nil, s.ExecutionManager, s.HistoryV2Mgr, s.logger)
	s.engineImpl.timerProcessor = newTimerQueueProcessor(s.ShardContext, s.engineImpl, s.mockMatchingClient, s.logger)
	s.ShardContext.SetEngine(s.engineImpl)
}

func (s *timerQueueProcessorSuite) TearDownTest() {
	s.TeardownDomains()
	s.TearDownWorkflowStore()
}

func (s *timerQueueProcessorSuite) updateTimerSeqNumbers(timerTasks []persistence.Task) {
	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", mock.Anything).Return(cluster.TestCurrentClusterName)

	clusterMetadata := s.mockClusterMetadata
	cluster := clusterMetadata.GetCurrentClusterName()
	for _, task := range timerTasks {
		ts := task.GetVisibilityTimestamp()
		if task.GetVersion() != common.EmptyVersion {
			cluster = clusterMetadata.ClusterNameForFailoverVersion(task.GetVersion())
		}
		if ts.Before(s.engineImpl.shard.GetTimerMaxReadLevel(cluster)) {
			// This can happen if shard move and new host have a time SKU, or there is db write delay.
			// We generate a new timer ID using timerMaxReadLevel.
			s.logger.Debug(fmt.Sprintf("%v: New timer generated is less than read level. timestamp: %v, timerMaxReadLevel: %v",
				time.Now(), ts, s.engineImpl.shard.GetTimerMaxReadLevel(cluster)))
			task.SetVisibilityTimestamp(s.engineImpl.shard.GetTimerMaxReadLevel(cluster).Add(time.Millisecond))
		}
		taskID, err := s.ShardContext.GenerateTransferTaskID()
		if err != nil {
			panic(err)
		}
		task.SetTaskID(taskID)
		ts = task.GetVisibilityTimestamp()
		s.logger.Info(fmt.Sprintf("%v: TestTimerQueueProcessorSuite: Assigning timer: %s",
			time.Now().UTC(), TimerSequenceID{VisibilityTimestamp: ts, TaskID: task.GetTaskID()}))
	}
}

func (s *timerQueueProcessorSuite) createExecutionWithTimers(domainID string, we workflow.WorkflowExecution, tl,
	identity string, timeOuts []int32) (*persistence.WorkflowMutableState, []persistence.Task) {

	// Generate first decision task event.
	builder := newMutableStateBuilderWithEventV2(s.ShardContext,
		s.ShardContext.GetEventsCache(), s.logger, we.GetRunId())
	addWorkflowExecutionStartedEvent(builder, we, "wType", tl, []byte("input"), 100, 200, identity)
	di := addDecisionTaskScheduledEvent(builder)

	createState := createMutableState(builder)
	info := createState.ExecutionInfo
	task0, err0 := s.CreateWorkflowExecutionWithBranchToken(domainID, we, tl, info.WorkflowTypeName, info.WorkflowTimeout, info.DecisionTimeoutValue,
		info.ExecutionContext, info.NextEventID, info.LastProcessedEvent, info.DecisionScheduleID, info.BranchToken, nil)
	s.NoError(err0, "No error expected.")
	s.NotNil(task0, "Expected non empty task identifier.")

	state0, err2 := s.GetWorkflowExecutionInfo(domainID, we)
	s.NoError(err2, "No error expected.")

	builder = newMutableStateBuilderWithEventV2(s.ShardContext,
		s.ShardContext.GetEventsCache(), s.logger, we.GetRunId())
	builder.Load(state0)
	startedEvent := addDecisionTaskStartedEvent(builder, di.ScheduleID, tl, identity)
	addDecisionTaskCompletedEvent(builder, di.ScheduleID, *startedEvent.EventId, nil, identity)
	timerTasks := []persistence.Task{}
	timerInfos := []*persistence.TimerInfo{}
	decisionCompletedID := int64(4)
	tBuilder := newTimerBuilder(clock.NewRealTimeSource())

	for _, timeOut := range timeOuts {
		_, ti, err := builder.AddTimerStartedEvent(decisionCompletedID,
			&workflow.StartTimerDecisionAttributes{
				TimerId:                   common.StringPtr(uuid.New()),
				StartToFireTimeoutSeconds: common.Int64Ptr(int64(timeOut)),
			})
		s.Nil(err)
		timerInfos = append(timerInfos, ti)
	}

	if t := tBuilder.GetUserTimerTaskIfNeeded(builder); t != nil {
		timerTasks = append(timerTasks, t)
	}

	s.ShardContext.Lock()
	s.updateTimerSeqNumbers(timerTasks)
	updatedState := createMutableState(builder)
	err3 := s.UpdateWorkflowExecution(updatedState.ExecutionInfo, updatedState.ExecutionStats, nil, nil, nil, int64(3), timerTasks, nil, nil, timerInfos, nil)
	s.ShardContext.Unlock()
	s.NoError(err3)

	return createMutableState(builder), timerTasks
}

func (s *timerQueueProcessorSuite) addDecisionTimer(domainID string, we workflow.WorkflowExecution, tb *timerBuilder) []persistence.Task {
	state, err := s.GetWorkflowExecutionInfo(domainID, we)
	s.NoError(err)

	condition := state.ExecutionInfo.NextEventID
	builder := newMutableStateBuilderWithEventV2(s.ShardContext,
		s.ShardContext.GetEventsCache(), s.logger, we.GetRunId())
	builder.Load(state)

	di := addDecisionTaskScheduledEvent(builder)
	startedEvent := addDecisionTaskStartedEvent(builder, di.ScheduleID, state.ExecutionInfo.TaskList, "identity")

	timeOutTask := tb.AddStartToCloseDecisionTimoutTask(di.ScheduleID, di.Attempt, 1)
	timerTasks := []persistence.Task{timeOutTask}

	s.ShardContext.Lock()
	s.updateTimerSeqNumbers(timerTasks)
	addDecisionTaskCompletedEvent(builder, di.ScheduleID, startedEvent.GetEventId(), nil, "identity")
	err2 := s.UpdateWorkflowExecution(state.ExecutionInfo, state.ExecutionStats, nil, nil, nil, condition, timerTasks, nil, nil, nil, nil)
	s.ShardContext.Unlock()
	s.NoError(err2, "No error expected.")
	return timerTasks
}

func (s *timerQueueProcessorSuite) addUserTimer(domainID string, we workflow.WorkflowExecution, timerID string, tb *timerBuilder) []persistence.Task {
	state, err := s.GetWorkflowExecutionInfo(domainID, we)
	s.NoError(err)
	builder := newMutableStateBuilderWithEventV2(s.ShardContext,
		s.ShardContext.GetEventsCache(), s.logger, we.GetRunId())
	builder.Load(state)
	condition := state.ExecutionInfo.NextEventID

	// create a user timer
	_, _, err = builder.AddTimerStartedEvent(common.EmptyEventID,
		&workflow.StartTimerDecisionAttributes{TimerId: common.StringPtr(timerID), StartToFireTimeoutSeconds: common.Int64Ptr(1)})
	s.Nil(err)
	t := tb.GetUserTimerTaskIfNeeded(builder)
	s.NotNil(t)
	timerTasks := []persistence.Task{t}

	s.updateHistoryAndTimers(builder, timerTasks, condition)
	return timerTasks
}

func (s *timerQueueProcessorSuite) addHeartBeatTimer(domainID string,
	we workflow.WorkflowExecution, tb *timerBuilder) (*workflow.HistoryEvent, []persistence.Task) {
	state, err := s.GetWorkflowExecutionInfo(domainID, we)
	s.NoError(err)
	builder := newMutableStateBuilderWithEventV2(s.ShardContext,
		s.ShardContext.GetEventsCache(), s.logger, we.GetRunId())
	builder.Load(state)
	condition := state.ExecutionInfo.NextEventID

	ase, ai, err := builder.AddActivityTaskScheduledEvent(common.EmptyEventID,
		&workflow.ScheduleActivityTaskDecisionAttributes{
			ActivityId:              common.StringPtr("testID"),
			HeartbeatTimeoutSeconds: common.Int32Ptr(1),
			TaskList:                &workflow.TaskList{Name: common.StringPtr("task-list")},
		})
	s.Nil(err)
	s.NotNil(ase)
	_, err = builder.AddActivityTaskStartedEvent(ai, *ase.EventId, uuid.New(), "")
	s.Nil(err)

	// create a heart beat timeout
	tt := tb.GetActivityTimerTaskIfNeeded(builder)
	s.NotNil(tt)
	timerTasks := []persistence.Task{tt}

	s.updateHistoryAndTimers(builder, timerTasks, condition)
	return ase, timerTasks
}

func (s *timerQueueProcessorSuite) closeWorkflow(domainID string, we workflow.WorkflowExecution) {
	state, err := s.GetWorkflowExecutionInfo(domainID, we)
	s.NoError(err)

	state.ExecutionInfo.State = persistence.WorkflowStateCompleted
	state.ExecutionInfo.CloseStatus = persistence.WorkflowCloseStatusCompleted

	err2 := s.UpdateWorkflowExecution(state.ExecutionInfo, state.ExecutionStats, nil, nil, nil, state.ExecutionInfo.NextEventID, nil, nil, nil, nil, nil)
	s.NoError(err2, "No error expected.")
}

func (s *timerQueueProcessorSuite) TestSingleTimerTask() {
	domainID := testDomainActiveID
	workflowExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("single-timer-test"),
		RunId:      common.StringPtr(testRunID),
	}
	taskList := "single-timer-queue"
	identity := "testIdentity"
	_, tt := s.createExecutionWithTimers(domainID, workflowExecution, taskList, identity, []int32{1})

	timerInfo, err := s.GetTimerIndexTasks(100, true)
	s.NoError(err, "No error expected.")
	s.NotEmpty(timerInfo, "Expected non empty timers list")
	s.Equal(1, len(timerInfo))

	processor := s.engineImpl.timerProcessor.(*timerQueueProcessorImpl)
	processor.Start()
	processor.NotifyNewTimers(cluster.TestCurrentClusterName, tt)

	expectedFireCount := uint64(1)
	s.waitForTimerTasksToProcess(processor, expectedFireCount)
	s.Equal(expectedFireCount, processor.getTimerFiredCount(cluster.TestCurrentClusterName))
}

func (s *timerQueueProcessorSuite) TestManyTimerTasks() {
	domainID := testDomainActiveID
	workflowExecution := workflow.WorkflowExecution{WorkflowId: common.StringPtr("multiple-timer-test"),
		RunId: common.StringPtr(testRunID)}

	taskList := "multiple-timer-queue"
	identity := "testIdentity"
	_, tt := s.createExecutionWithTimers(domainID, workflowExecution, taskList, identity, []int32{1, 2, 3})

	timerInfo, err := s.GetTimerIndexTasks(100, true)
	s.NoError(err, "No error expected.")
	s.NotEmpty(timerInfo, "Expected non empty timers list")
	s.Equal(1, len(timerInfo))

	processor := s.engineImpl.timerProcessor.(*timerQueueProcessorImpl)
	processor.Start()
	processor.NotifyNewTimers(cluster.TestCurrentClusterName, tt)

	expectedFireCount := uint64(3)
	s.waitForTimerTasksToProcess(processor, expectedFireCount)
	s.Equal(expectedFireCount, processor.getTimerFiredCount(cluster.TestCurrentClusterName))
}

func (s *timerQueueProcessorSuite) TestTimerTaskAfterProcessorStart() {
	domainID := testDomainActiveID
	workflowExecution := workflow.WorkflowExecution{WorkflowId: common.StringPtr("After-timer-test"),
		RunId: common.StringPtr(testRunID)}

	taskList := "After-timer-queue"
	identity := "testIdentity"

	s.createExecutionWithTimers(domainID, workflowExecution, taskList, identity, []int32{})

	timerInfo, err := s.GetTimerIndexTasks(100, true)
	s.NoError(err, "No error expected.")
	s.Empty(timerInfo, "Expected empty timers list")

	processor := s.engineImpl.timerProcessor.(*timerQueueProcessorImpl)
	processor.Start()

	tBuilder := newTimerBuilder(clock.NewRealTimeSource())
	tt := s.addDecisionTimer(domainID, workflowExecution, tBuilder)
	processor.NotifyNewTimers(cluster.TestCurrentClusterName, tt)

	expectedFireCount := uint64(1)
	s.waitForTimerTasksToProcess(processor, expectedFireCount)
	s.Equal(expectedFireCount, processor.getTimerFiredCount(cluster.TestCurrentClusterName))
}

func (s *timerQueueProcessorSuite) waitForTimerTasksToProcess(p *timerQueueProcessorImpl, expectedFireCount uint64) {
	// retry for 20 seconds
	for i := 0; i < 20; i++ {
		if expectedFireCount == p.getTimerFiredCount(cluster.TestCurrentClusterName) {
			break
		}
		time.Sleep(1000 * time.Millisecond)
	}
	p.Stop()
}

func (s *timerQueueProcessorSuite) checkTimedOutEventFor(domainID string, we workflow.WorkflowExecution,
	scheduleID int64) bool {
	info, err1 := s.GetWorkflowExecutionInfo(domainID, we)
	s.NoError(err1)
	builder := newMutableStateBuilderWithEventV2(s.ShardContext,
		s.ShardContext.GetEventsCache(), s.logger, we.GetRunId())
	builder.Load(info)
	_, ok := builder.GetActivityInfo(scheduleID)

	return ok
}

func (s *timerQueueProcessorSuite) checkTimedOutEventForUserTimer(domainID string, we workflow.WorkflowExecution,
	timerID string) bool {
	info, err1 := s.GetWorkflowExecutionInfo(domainID, we)
	s.NoError(err1)
	builder := newMutableStateBuilderWithEventV2(s.ShardContext,
		s.ShardContext.GetEventsCache(), s.logger, we.GetRunId())
	builder.Load(info)

	_, ok := builder.GetUserTimer(timerID)
	return ok
}

func (s *timerQueueProcessorSuite) updateHistoryAndTimers(ms mutableState, timerTasks []persistence.Task, condition int64) {
	updatedState := createMutableState(ms)

	actInfos := []*persistence.ActivityInfo{}
	for _, x := range updatedState.ActivityInfos {
		actInfos = append(actInfos, x)
	}
	timerInfos := []*persistence.TimerInfo{}
	for _, x := range updatedState.TimerInfos {
		timerInfos = append(timerInfos, x)
	}

	s.ShardContext.Lock()
	s.updateTimerSeqNumbers(timerTasks)
	err3 := s.UpdateWorkflowExecution(
		updatedState.ExecutionInfo, updatedState.ExecutionStats, nil, nil, nil, condition, timerTasks, actInfos, nil, timerInfos, nil)
	s.ShardContext.Unlock()
	s.NoError(err3)
}

func (s *timerQueueProcessorSuite) TestTimerActivityTaskScheduleToStart_WithOutStart() {
	domainID := testDomainActiveID
	workflowExecution := workflow.WorkflowExecution{WorkflowId: common.StringPtr("activity-timer-SCHEDULE_TO_START-test"),
		RunId: common.StringPtr(testRunID)}

	taskList := "activity-timer-queue"

	s.createExecutionWithTimers(domainID, workflowExecution, taskList, "identity", []int32{})

	// TimeoutTypeScheduleToStart - Without Start
	processor := s.engineImpl.timerProcessor.(*timerQueueProcessorImpl)
	processor.Start()

	state, err := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.NoError(err)
	builder := newMutableStateBuilderWithEventV2(s.ShardContext,
		s.ShardContext.GetEventsCache(), s.logger, workflowExecution.GetRunId())
	builder.Load(state)
	condition := state.ExecutionInfo.NextEventID

	activityScheduledEvent, _, err := builder.AddActivityTaskScheduledEvent(common.EmptyEventID,
		&workflow.ScheduleActivityTaskDecisionAttributes{
			ActivityId:                    common.StringPtr("testID"),
			ScheduleToStartTimeoutSeconds: common.Int32Ptr(1),
			ScheduleToCloseTimeoutSeconds: common.Int32Ptr(2),
			TaskList:                      &workflow.TaskList{Name: common.StringPtr("task-list")},
		})
	s.Nil(err)
	s.NotNil(activityScheduledEvent)

	tBuilder := newTimerBuilder(&mockTimeSource{currTime: time.Now()})
	tt := tBuilder.GetActivityTimerTaskIfNeeded(builder)
	s.NotNil(tt)
	timerTasks := []persistence.Task{tt}

	s.updateHistoryAndTimers(builder, timerTasks, condition)
	processor.NotifyNewTimers(cluster.TestCurrentClusterName, timerTasks)

	expectedFireCount := uint64(1)
	s.waitForTimerTasksToProcess(processor, expectedFireCount)
	s.Equal(expectedFireCount, processor.getTimerFiredCount(cluster.TestCurrentClusterName))
	running := s.checkTimedOutEventFor(domainID, workflowExecution, *activityScheduledEvent.EventId)
	s.False(running)
}

func (s *timerQueueProcessorSuite) TestTimerActivityTaskScheduleToStart_WithStart() {
	domainID := testDomainActiveID
	workflowExecution := workflow.WorkflowExecution{WorkflowId: common.StringPtr("activity-timer-SCHEDULE_TO_START-Started-test"),
		RunId: common.StringPtr(testRunID)}

	taskList := "activity-timer-queue"

	s.createExecutionWithTimers(domainID, workflowExecution, taskList, "identity", []int32{})

	// TimeoutTypeScheduleToStart - With Start
	p := s.engineImpl.timerProcessor.(*timerQueueProcessorImpl)
	p.Start()

	state, err := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.NoError(err)
	builder := newMutableStateBuilderWithEventV2(s.ShardContext,
		s.ShardContext.GetEventsCache(), s.logger, workflowExecution.GetRunId())
	builder.Load(state)
	condition := state.ExecutionInfo.NextEventID

	ase, ai, err := builder.AddActivityTaskScheduledEvent(common.EmptyEventID,
		&workflow.ScheduleActivityTaskDecisionAttributes{
			ActivityId:                    common.StringPtr("testID"),
			ScheduleToStartTimeoutSeconds: common.Int32Ptr(1),
			StartToCloseTimeoutSeconds:    common.Int32Ptr(1),
			TaskList:                      &workflow.TaskList{Name: common.StringPtr("task-list")},
		})
	s.Nil(err)
	s.NotNil(ase)
	_, err = builder.AddActivityTaskStartedEvent(ai, *ase.EventId, uuid.New(), "")
	s.Nil(err)

	// create a schedule to start timeout
	tBuilder := newTimerBuilder(&mockTimeSource{currTime: time.Now()})
	tt := tBuilder.GetActivityTimerTaskIfNeeded(builder)
	s.NotNil(tt)
	timerTasks := []persistence.Task{tt}

	s.updateHistoryAndTimers(builder, timerTasks, condition)
	p.NotifyNewTimers(cluster.TestCurrentClusterName, timerTasks)

	expectedFireCount := uint64(1)
	s.waitForTimerTasksToProcess(p, expectedFireCount)
	s.Equal(expectedFireCount, p.getTimerFiredCount(cluster.TestCurrentClusterName))
	running := s.checkTimedOutEventFor(domainID, workflowExecution, *ase.EventId)
	s.False(running)
}

func (s *timerQueueProcessorSuite) TestTimerActivityTaskScheduleToStart_MoreThanStartToClose() {
	domainID := testDomainActiveID
	workflowExecution := workflow.WorkflowExecution{WorkflowId: common.StringPtr("activity-timer-SCHEDULE_TO_START-more-than-start2close"),
		RunId: common.StringPtr(testRunID)}

	taskList := "activity-timer-queue"

	s.createExecutionWithTimers(domainID, workflowExecution, taskList, "identity", []int32{})

	// TimeoutTypeScheduleToStart - Without Start
	processor := s.engineImpl.timerProcessor.(*timerQueueProcessorImpl)
	processor.Start()

	state, err := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.NoError(err)
	builder := newMutableStateBuilderWithEventV2(s.ShardContext,
		s.ShardContext.GetEventsCache(), s.logger, workflowExecution.GetRunId())
	builder.Load(state)
	condition := state.ExecutionInfo.NextEventID

	activityScheduledEvent, _, err := builder.AddActivityTaskScheduledEvent(common.EmptyEventID,
		&workflow.ScheduleActivityTaskDecisionAttributes{
			ActivityId:                    common.StringPtr("testID"),
			ScheduleToStartTimeoutSeconds: common.Int32Ptr(2),
			StartToCloseTimeoutSeconds:    common.Int32Ptr(1),
			ScheduleToCloseTimeoutSeconds: common.Int32Ptr(3),
			TaskList:                      &workflow.TaskList{Name: common.StringPtr("task-list")},
		})
	s.Nil(err)
	s.NotNil(activityScheduledEvent)

	tBuilder := newTimerBuilder(&mockTimeSource{currTime: time.Now()})
	tt := tBuilder.GetActivityTimerTaskIfNeeded(builder)
	s.NotNil(tt)
	s.Equal(workflow.TimeoutTypeScheduleToStart, workflow.TimeoutType(tt.(*persistence.ActivityTimeoutTask).TimeoutType))
	timerTasks := []persistence.Task{tt}

	s.updateHistoryAndTimers(builder, timerTasks, condition)
	processor.NotifyNewTimers(cluster.TestCurrentClusterName, timerTasks)

	expectedFireCount := uint64(1)
	s.waitForTimerTasksToProcess(processor, expectedFireCount)
	s.Equal(expectedFireCount, processor.getTimerFiredCount(cluster.TestCurrentClusterName))
	running := s.checkTimedOutEventFor(domainID, workflowExecution, *activityScheduledEvent.EventId)
	s.False(running)
}

func (s *timerQueueProcessorSuite) TestTimerActivityTaskStartToClose_WithStart() {
	domainID := testDomainActiveID
	workflowExecution := workflow.WorkflowExecution{WorkflowId: common.StringPtr("activity-timer-START_TO_CLOSE-Started-test"),
		RunId: common.StringPtr(testRunID)}

	taskList := "activity-timer-queue"

	s.createExecutionWithTimers(domainID, workflowExecution, taskList, "identity", []int32{})

	// TimeoutTypeStartToClose - Just start.
	p := s.engineImpl.timerProcessor.(*timerQueueProcessorImpl)
	p.Start()

	state, err := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.NoError(err)
	builder := newMutableStateBuilderWithEventV2(s.ShardContext,
		s.ShardContext.GetEventsCache(), s.logger, workflowExecution.GetRunId())
	builder.Load(state)
	condition := state.ExecutionInfo.NextEventID

	ase, ai, err := builder.AddActivityTaskScheduledEvent(common.EmptyEventID,
		&workflow.ScheduleActivityTaskDecisionAttributes{
			ActivityId:                    common.StringPtr("testID"),
			ScheduleToStartTimeoutSeconds: common.Int32Ptr(1),
			StartToCloseTimeoutSeconds:    common.Int32Ptr(1),
			TaskList:                      &workflow.TaskList{Name: common.StringPtr("task-list")},
		})
	s.Nil(err)
	_, err = builder.AddActivityTaskStartedEvent(ai, *ase.EventId, uuid.New(), "")
	s.Nil(err)

	// create a start to close timeout
	tBuilder := newTimerBuilder(&mockTimeSource{currTime: time.Now()})
	tt := tBuilder.GetActivityTimerTaskIfNeeded(builder)
	s.NotNil(tt)
	timerTasks := []persistence.Task{tt}

	s.updateHistoryAndTimers(builder, timerTasks, condition)
	p.NotifyNewTimers(cluster.TestCurrentClusterName, timerTasks)

	expectedFireCount := uint64(1)
	s.waitForTimerTasksToProcess(p, expectedFireCount)
	s.Equal(expectedFireCount, p.getTimerFiredCount(cluster.TestCurrentClusterName))
	running := s.checkTimedOutEventFor(domainID, workflowExecution, *ase.EventId)
	s.False(running)
}

func (s *timerQueueProcessorSuite) TestTimerActivityTaskStartToClose_CompletedActivity() {
	domainID := testDomainActiveID
	workflowExecution := workflow.WorkflowExecution{WorkflowId: common.StringPtr("activity-timer-START_TO_CLOSE-Completed-test"),
		RunId: common.StringPtr(testRunID)}

	taskList := "activity-timer-queue"
	s.createExecutionWithTimers(domainID, workflowExecution, taskList, "identity", []int32{})

	// TimeoutTypeStartToClose - Start and Completed activity.
	p := s.engineImpl.timerProcessor.(*timerQueueProcessorImpl)
	p.Start()

	state, err := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.NoError(err)
	builder := newMutableStateBuilderWithEventV2(s.ShardContext,
		s.ShardContext.GetEventsCache(), s.logger, workflowExecution.GetRunId())
	builder.Load(state)
	condition := state.ExecutionInfo.NextEventID

	ase, ai, err := builder.AddActivityTaskScheduledEvent(common.EmptyEventID,
		&workflow.ScheduleActivityTaskDecisionAttributes{
			ActivityId:                    common.StringPtr("testID"),
			ScheduleToStartTimeoutSeconds: common.Int32Ptr(1),
			StartToCloseTimeoutSeconds:    common.Int32Ptr(1),
			TaskList:                      &workflow.TaskList{Name: common.StringPtr("task-list")},
		})
	s.Nil(err)
	aste, err := builder.AddActivityTaskStartedEvent(ai, *ase.EventId, uuid.New(), "")
	s.Nil(err)
	_, err = builder.AddActivityTaskCompletedEvent(*ase.EventId, *aste.EventId, &workflow.RespondActivityTaskCompletedRequest{
		Identity: common.StringPtr("test-id"),
		Result:   []byte("result"),
	})
	s.Nil(err)

	// create a start to close timeout
	tBuilder := newTimerBuilder(&mockTimeSource{currTime: time.Now()})
	t, err := tBuilder.AddStartToCloseActivityTimeout(ai)
	s.NoError(err)
	s.NotNil(t)
	timerTasks := []persistence.Task{t}

	s.updateHistoryAndTimers(builder, timerTasks, condition)
	p.NotifyNewTimers(cluster.TestCurrentClusterName, timerTasks)

	expectedFireCount := uint64(1)
	s.waitForTimerTasksToProcess(p, expectedFireCount)
	s.Equal(expectedFireCount, p.getTimerFiredCount(cluster.TestCurrentClusterName))
	running := s.checkTimedOutEventFor(domainID, workflowExecution, *ase.EventId)
	s.False(running)
}

func (s *timerQueueProcessorSuite) TestTimerActivityTaskScheduleToClose_JustScheduled() {
	domainID := testDomainActiveID
	workflowExecution := workflow.WorkflowExecution{WorkflowId: common.StringPtr("activity-timer-SCHEDULE_TO_CLOSE-Scheduled-test"),
		RunId: common.StringPtr(testRunID)}

	taskList := "activity-timer-queue"
	s.createExecutionWithTimers(domainID, workflowExecution, taskList, "identity", []int32{})

	// TimeoutTypeScheduleToClose - Just Scheduled.
	p := s.engineImpl.timerProcessor.(*timerQueueProcessorImpl)
	p.Start()

	state, err := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.NoError(err)
	builder := newMutableStateBuilderWithEventV2(s.ShardContext,
		s.ShardContext.GetEventsCache(), s.logger, workflowExecution.GetRunId())
	builder.Load(state)
	condition := state.ExecutionInfo.NextEventID

	ase, _, err := builder.AddActivityTaskScheduledEvent(common.EmptyEventID,
		&workflow.ScheduleActivityTaskDecisionAttributes{
			ActivityId:                    common.StringPtr("testID"),
			ScheduleToStartTimeoutSeconds: common.Int32Ptr(1),
			ScheduleToCloseTimeoutSeconds: common.Int32Ptr(1),
			TaskList:                      &workflow.TaskList{Name: common.StringPtr("task-list")},
		})
	s.Nil(err)
	s.NotNil(ase)

	// create a schedule to close timeout
	tBuilder := newTimerBuilder(&mockTimeSource{currTime: time.Now()})
	tt := tBuilder.GetActivityTimerTaskIfNeeded(builder)
	s.NotNil(tt)
	timerTasks := []persistence.Task{tt}

	s.updateHistoryAndTimers(builder, timerTasks, condition)
	p.NotifyNewTimers(cluster.TestCurrentClusterName, timerTasks)

	expectedFireCount := uint64(1)
	s.waitForTimerTasksToProcess(p, expectedFireCount)
	s.Equal(expectedFireCount, p.getTimerFiredCount(cluster.TestCurrentClusterName))
	running := s.checkTimedOutEventFor(domainID, workflowExecution, *ase.EventId)
	s.False(running)
}

func (s *timerQueueProcessorSuite) TestTimerActivityTaskScheduleToClose_Started() {
	domainID := testDomainActiveID
	workflowExecution := workflow.WorkflowExecution{WorkflowId: common.StringPtr("activity-timer-SCHEDULE_TO_CLOSE-Started-test"),
		RunId: common.StringPtr(testRunID)}

	taskList := "activity-timer-queue"
	s.createExecutionWithTimers(domainID, workflowExecution, taskList, "identity", []int32{})

	// TimeoutTypeScheduleToClose - Scheduled and started.
	p := s.engineImpl.timerProcessor.(*timerQueueProcessorImpl)
	p.Start()

	state, err := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.NoError(err)
	builder := newMutableStateBuilderWithEventV2(s.ShardContext,
		s.ShardContext.GetEventsCache(), s.logger, workflowExecution.GetRunId())
	builder.Load(state)
	condition := state.ExecutionInfo.NextEventID

	ase, ai, err := builder.AddActivityTaskScheduledEvent(common.EmptyEventID,
		&workflow.ScheduleActivityTaskDecisionAttributes{
			ActivityId:                    common.StringPtr("testID"),
			ScheduleToStartTimeoutSeconds: common.Int32Ptr(1),
			ScheduleToCloseTimeoutSeconds: common.Int32Ptr(1),
			StartToCloseTimeoutSeconds:    common.Int32Ptr(1),
			TaskList:                      &workflow.TaskList{Name: common.StringPtr("task-list")},
		})
	s.Nil(err)
	s.NotNil(ase)
	_, err = builder.AddActivityTaskStartedEvent(ai, *ase.EventId, uuid.New(), "")
	s.Nil(err)

	// create a schedule to close timeout
	tBuilder := newTimerBuilder(&mockTimeSource{currTime: time.Now()})
	tt := tBuilder.GetActivityTimerTaskIfNeeded(builder)
	s.NotNil(tt)
	timerTasks := []persistence.Task{tt}

	s.updateHistoryAndTimers(builder, timerTasks, condition)
	p.NotifyNewTimers(cluster.TestCurrentClusterName, timerTasks)

	expectedFireCount := uint64(1)
	s.waitForTimerTasksToProcess(p, expectedFireCount)
	s.Equal(expectedFireCount, p.getTimerFiredCount(cluster.TestCurrentClusterName))
	running := s.checkTimedOutEventFor(domainID, workflowExecution, *ase.EventId)
	s.False(running)
}

func (s *timerQueueProcessorSuite) TestTimerActivityTaskScheduleToClose_Completed() {
	domainID := testDomainActiveID
	workflowExecution := workflow.WorkflowExecution{WorkflowId: common.StringPtr("activity-timer-SCHEDULE_TO_CLOSE-Completed-test"),
		RunId: common.StringPtr(testRunID)}

	taskList := "activity-timer-queue"
	s.createExecutionWithTimers(domainID, workflowExecution, taskList, "identity", []int32{})

	// TimeoutTypeScheduleToClose - Scheduled, started, completed.
	p := s.engineImpl.timerProcessor.(*timerQueueProcessorImpl)
	p.Start()

	state, err := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.NoError(err)
	builder := newMutableStateBuilderWithEventV2(s.ShardContext,
		s.ShardContext.GetEventsCache(), s.logger, workflowExecution.GetRunId())
	builder.Load(state)
	condition := state.ExecutionInfo.NextEventID

	ase, ai, err := builder.AddActivityTaskScheduledEvent(common.EmptyEventID,
		&workflow.ScheduleActivityTaskDecisionAttributes{
			ActivityId:                    common.StringPtr("testID"),
			ScheduleToStartTimeoutSeconds: common.Int32Ptr(1),
			ScheduleToCloseTimeoutSeconds: common.Int32Ptr(1),
			TaskList:                      &workflow.TaskList{Name: common.StringPtr("task-list")},
		})
	s.Nil(err)
	s.NotNil(ase)
	aste, err := builder.AddActivityTaskStartedEvent(ai, *ase.EventId, uuid.New(), "")
	s.Nil(err)
	_, err = builder.AddActivityTaskCompletedEvent(*ase.EventId, *aste.EventId, &workflow.RespondActivityTaskCompletedRequest{
		Identity: common.StringPtr("test-id"),
		Result:   []byte("result"),
	})
	s.Nil(err)

	// create a schedule to close timeout
	tBuilder := newTimerBuilder(&mockTimeSource{currTime: time.Now()})
	t, err := tBuilder.AddScheduleToCloseActivityTimeout(ai)
	s.NoError(err)
	s.NotNil(t)
	timerTasks := []persistence.Task{t}

	s.updateHistoryAndTimers(builder, timerTasks, condition)
	p.NotifyNewTimers(cluster.TestCurrentClusterName, timerTasks)

	expectedFireCount := uint64(1)
	s.waitForTimerTasksToProcess(p, expectedFireCount)
	s.Equal(expectedFireCount, p.getTimerFiredCount(cluster.TestCurrentClusterName))
	running := s.checkTimedOutEventFor(domainID, workflowExecution, *ase.EventId)
	s.False(running)
}

func (s *timerQueueProcessorSuite) TestTimerActivityTaskHeartBeat_JustStarted() {
	domainID := testDomainActiveID
	workflowExecution := workflow.WorkflowExecution{WorkflowId: common.StringPtr("activity-timer-hb-started-test"),
		RunId: common.StringPtr(testRunID)}

	taskList := "activity-timer-queue"

	s.createExecutionWithTimers(domainID, workflowExecution, taskList, "identity", []int32{})

	// TimeoutTypeHeartbeat - Scheduled, started.
	p := s.engineImpl.timerProcessor.(*timerQueueProcessorImpl)
	p.Start()

	tBuilder := newTimerBuilder(&mockTimeSource{currTime: time.Now()})
	ase, timerTasks := s.addHeartBeatTimer(domainID, workflowExecution, tBuilder)

	p.NotifyNewTimers(cluster.TestCurrentClusterName, timerTasks)
	expectedFireCount := uint64(1)
	s.waitForTimerTasksToProcess(p, expectedFireCount)
	s.Equal(expectedFireCount, p.getTimerFiredCount(cluster.TestCurrentClusterName))
	running := s.checkTimedOutEventFor(domainID, workflowExecution, *ase.EventId)
	s.False(running)
}

func (s *timerQueueProcessorSuite) TestTimerActivityTask_SameExpiry() {
	domainID := testDomainActiveID
	workflowExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("activity-timer-same-expiry-test"),
		RunId:      common.StringPtr(testRunID),
	}

	taskList := "activity-timer-queue"
	s.createExecutionWithTimers(domainID, workflowExecution, taskList, "identity", []int32{})

	// TimeoutTypeScheduleToClose - Scheduled, started, completed.
	p := s.engineImpl.timerProcessor.(*timerQueueProcessorImpl)
	p.Start()

	state, err := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.NoError(err)
	builder := newMutableStateBuilderWithEventV2(s.ShardContext,
		s.ShardContext.GetEventsCache(), s.logger, workflowExecution.GetRunId())
	builder.Load(state)
	condition := state.ExecutionInfo.NextEventID

	ase1, ai1, err := builder.AddActivityTaskScheduledEvent(common.EmptyEventID,
		&workflow.ScheduleActivityTaskDecisionAttributes{
			ActivityId:                    common.StringPtr("testID-1"),
			ScheduleToStartTimeoutSeconds: common.Int32Ptr(1),
			ScheduleToCloseTimeoutSeconds: common.Int32Ptr(1),
			TaskList:                      &workflow.TaskList{Name: common.StringPtr("task-list")},
		})
	s.Nil(err)
	s.NotNil(ase1)
	ase2, ai2, err := builder.AddActivityTaskScheduledEvent(common.EmptyEventID,
		&workflow.ScheduleActivityTaskDecisionAttributes{
			ActivityId:                    common.StringPtr("testID-2"),
			ScheduleToStartTimeoutSeconds: common.Int32Ptr(1),
			ScheduleToCloseTimeoutSeconds: common.Int32Ptr(1),
			TaskList:                      &workflow.TaskList{Name: common.StringPtr("task-list")},
		})
	s.Nil(err)
	s.NotNil(ase2)

	// create a schedule to close timeout
	tBuilder := newTimerBuilder(&mockTimeSource{currTime: time.Now()})
	t, err := tBuilder.AddScheduleToCloseActivityTimeout(ai1)
	s.NoError(err)
	s.NotNil(t)
	t, err = tBuilder.AddScheduleToCloseActivityTimeout(ai2)
	s.NoError(err)
	s.NotNil(t)
	timerTasks := []persistence.Task{t}

	s.updateHistoryAndTimers(builder, timerTasks, condition)
	p.NotifyNewTimers(cluster.TestCurrentClusterName, timerTasks)

	expectedFireCount := uint64(1)
	s.waitForTimerTasksToProcess(p, expectedFireCount)
	s.Equal(expectedFireCount, p.getTimerFiredCount(cluster.TestCurrentClusterName))
	running := s.checkTimedOutEventFor(domainID, workflowExecution, *ase1.EventId)
	s.False(running)
	running = s.checkTimedOutEventFor(domainID, workflowExecution, *ase2.EventId)
	s.False(running)

	// assert activity infos are deleted
	state, err = s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.NoError(err)
	builder = newMutableStateBuilderWithEventV2(s.ShardContext,
		s.ShardContext.GetEventsCache(), s.logger, workflowExecution.GetRunId())
	builder.Load(state)
	s.Equal(0, len(builder.pendingActivityInfoIDs))
}

func (s *timerQueueProcessorSuite) TestTimerUserTimers() {
	domainID := testDomainActiveID
	workflowExecution := workflow.WorkflowExecution{WorkflowId: common.StringPtr("user-timer-test"),
		RunId: common.StringPtr(testRunID)}

	taskList := "user-timer-queue"
	s.createExecutionWithTimers(domainID, workflowExecution, taskList, "identity", []int32{})

	// Single timer.
	p := s.engineImpl.timerProcessor.(*timerQueueProcessorImpl)
	p.Start()

	tBuilder := newTimerBuilder(&mockTimeSource{currTime: time.Now()})
	timerID := "tid1"
	timerTasks := s.addUserTimer(domainID, workflowExecution, timerID, tBuilder)
	p.NotifyNewTimers(cluster.TestCurrentClusterName, timerTasks)
	expectedFireCount := uint64(1)
	s.waitForTimerTasksToProcess(p, expectedFireCount)
	s.Equal(expectedFireCount, p.getTimerFiredCount(cluster.TestCurrentClusterName))
	running := s.checkTimedOutEventForUserTimer(domainID, workflowExecution, timerID)
	s.False(running)
}

func (s *timerQueueProcessorSuite) TestTimerUserTimers_SameExpiry() {
	domainID := testDomainActiveID
	workflowExecution := workflow.WorkflowExecution{WorkflowId: common.StringPtr("user-timer-same-expiry-test"),
		RunId: common.StringPtr(testRunID)}

	taskList := "user-timer-same-expiry-queue"
	s.createExecutionWithTimers(domainID, workflowExecution, taskList, "identity", []int32{})

	// Two timers.
	p := s.engineImpl.timerProcessor.(*timerQueueProcessorImpl)
	p.Start()

	state, err := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.NoError(err)
	builder := newMutableStateBuilderWithEventV2(s.ShardContext,
		s.ShardContext.GetEventsCache(), s.logger, workflowExecution.GetRunId())
	builder.Load(state)
	condition := state.ExecutionInfo.NextEventID

	// load any timers.
	tBuilder := newTimerBuilder(&mockTimeSource{currTime: time.Now().Add(-1 * time.Second)})
	timerTasks := []persistence.Task{}

	// create two user timers.
	_, ti, err := builder.AddTimerStartedEvent(common.EmptyEventID,
		&workflow.StartTimerDecisionAttributes{TimerId: common.StringPtr("tid1"), StartToFireTimeoutSeconds: common.Int64Ptr(1)})
	s.Nil(err)
	_, ti2, err := builder.AddTimerStartedEvent(common.EmptyEventID,
		&workflow.StartTimerDecisionAttributes{TimerId: common.StringPtr("tid2"), StartToFireTimeoutSeconds: common.Int64Ptr(1)})
	s.Nil(err)

	t := tBuilder.GetUserTimerTaskIfNeeded(builder)
	timerTasks = append(timerTasks, t)

	s.updateHistoryAndTimers(builder, timerTasks, condition)
	p.NotifyNewTimers(cluster.TestCurrentClusterName, timerTasks)

	expectedFireCount := uint64(len(timerTasks))
	s.waitForTimerTasksToProcess(p, expectedFireCount)
	s.Equal(expectedFireCount, p.getTimerFiredCount(cluster.TestCurrentClusterName))
	running := s.checkTimedOutEventForUserTimer(domainID, workflowExecution, ti.TimerID)
	s.False(running)
	running = s.checkTimedOutEventForUserTimer(domainID, workflowExecution, ti2.TimerID)
	s.False(running)

	// assert user timer infos are deleted
	state, err = s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.NoError(err)
	builder = newMutableStateBuilderWithEventV2(s.ShardContext,
		s.ShardContext.GetEventsCache(), s.logger, workflowExecution.GetRunId())
	builder.Load(state)
	s.Equal(0, len(builder.pendingTimerInfoIDs))
}

func (s *timerQueueProcessorSuite) TestTimersOnClosedWorkflow() {
	domainID := testDomainActiveID
	workflowExecution := workflow.WorkflowExecution{WorkflowId: common.StringPtr("closed-workflow-test-decision-timer"),
		RunId: common.StringPtr(testRunID)}

	taskList := "closed-workflow-queue"
	s.createExecutionWithTimers(domainID, workflowExecution, taskList, "identity", []int32{})

	p := s.engineImpl.timerProcessor.(*timerQueueProcessorImpl)
	p.Start()

	tBuilder := newTimerBuilder(&mockTimeSource{currTime: time.Now()})

	// Start of one of each timers each
	s.addDecisionTimer(domainID, workflowExecution, tBuilder)
	tt := s.addUserTimer(domainID, workflowExecution, "tid1", tBuilder)
	s.addHeartBeatTimer(domainID, workflowExecution, tBuilder)

	// GEt current state of workflow.
	state0, err := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.NoError(err)

	// close workflow
	s.closeWorkflow(domainID, workflowExecution)

	p.NotifyNewTimers(cluster.TestCurrentClusterName, tt)
	expectedFireCount := uint64(3)
	s.waitForTimerTasksToProcess(p, expectedFireCount)
	s.Equal(expectedFireCount, p.getTimerFiredCount(cluster.TestCurrentClusterName))

	// Verify that no new events are added to workflow.
	state1, err := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.NoError(err)
	s.Equal(state0.ExecutionInfo.NextEventID, state1.ExecutionInfo.NextEventID)
}

func (s *timerQueueProcessorSuite) printHistory(builder mutableState) string {
	return builder.GetHistoryBuilder().GetHistory().String()
}
