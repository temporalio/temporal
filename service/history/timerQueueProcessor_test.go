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
	"os"
	"testing"
	"time"

	"github.com/uber-go/tally"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/mocks"
	"github.com/uber/cadence/common/persistence"

	"github.com/pborman/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/suite"
	"github.com/uber-common/bark"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common/cache"
)

type (
	timerQueueProcessorSuite struct {
		suite.Suite
		TestBase
		engineImpl       *historyEngineImpl
		mockShardManager *mocks.ShardManager
		shardClosedCh    chan int
		logger           bark.Logger

		mockMetadataMgr   *mocks.MetadataManager
		mockVisibilityMgr *mocks.VisibilityManager
	}
)

func TestTimerQueueProcessorSuite(t *testing.T) {
	s := new(timerQueueProcessorSuite)
	suite.Run(t, s)
}

func (s *timerQueueProcessorSuite) SetupSuite() {
	if testing.Verbose() {
		log.SetOutput(os.Stdout)
	}

	s.SetupWorkflowStore()

	log2 := log.New()
	log2.Level = log.DebugLevel
	s.logger = bark.NewLoggerFromLogrus(log2)

	s.mockShardManager = &mocks.ShardManager{}
	s.mockMetadataMgr = &mocks.MetadataManager{}
	historyCache := newHistoryCache(s.ShardContext, s.logger)
	historyCache.disabled = true
	domainCache := cache.NewDomainCache(s.mockMetadataMgr, s.logger)
	txProcessor := newTransferQueueProcessor(s.ShardContext, s.mockVisibilityMgr, &mocks.MatchingClient{}, &mocks.HistoryClient{}, historyCache, domainCache)
	s.engineImpl = &historyEngineImpl{
		shard:              s.ShardContext,
		historyMgr:         s.HistoryMgr,
		txProcessor:        txProcessor,
		historyCache:       historyCache,
		domainCache:        domainCache,
		logger:             s.logger,
		tokenSerializer:    common.NewJSONTaskTokenSerializer(),
		hSerializerFactory: persistence.NewHistorySerializerFactory(),
		metricsClient:      metrics.NewClient(tally.NoopScope, metrics.History),
	}
}

func (s *timerQueueProcessorSuite) TearDownSuite() {
	s.TearDownWorkflowStore()
}

func (s *timerQueueProcessorSuite) TearDownTest() {
	s.mockShardManager.AssertExpectations(s.T())
}

func (s *timerQueueProcessorSuite) updateTimerSeqNumbers(timerTasks []persistence.Task) {
	for _, task := range timerTasks {
		taskID, err := s.ShardContext.GetNextTransferTaskID()
		if err != nil {
			panic(err)
		}
		task.SetTaskID(taskID)
		s.logger.Infof("%v: TestTimerQueueProcessorSuite: Assigning timer: %s",
			time.Now().UTC(), SequenceID{VisibilityTimestamp: persistence.GetVisibilityTSFrom(task), TaskID: task.GetTaskID()})
	}
}

func (s *timerQueueProcessorSuite) createExecutionWithTimers(domainID string, we workflow.WorkflowExecution, tl,
	identity string, timeOuts []int32) (*persistence.WorkflowMutableState, []persistence.Task) {

	// Generate first decision task event.
	builder := newMutableStateBuilder(s.ShardContext.GetConfig(), s.logger)
	addWorkflowExecutionStartedEvent(builder, we, "wType", tl, []byte("input"), 100, 200, identity)
	scheduleEvent, _ := addDecisionTaskScheduledEvent(builder)

	createState := createMutableState(builder)
	info := createState.ExecutionInfo
	task0, err0 := s.CreateWorkflowExecution(domainID, we, tl, info.WorkflowTypeName, info.DecisionTimeoutValue,
		info.ExecutionContext, info.NextEventID, info.LastProcessedEvent, info.DecisionScheduleID, nil)
	s.Nil(err0, "No error expected.")
	s.NotEmpty(task0, "Expected non empty task identifier.")

	state0, err2 := s.GetWorkflowExecutionInfo(domainID, we)
	s.Nil(err2, "No error expected.")

	builder = newMutableStateBuilder(s.ShardContext.GetConfig(), s.logger)
	builder.Load(state0)
	startedEvent := addDecisionTaskStartedEvent(builder, scheduleEvent.GetEventId(), tl, identity)
	addDecisionTaskCompletedEvent(builder, scheduleEvent.GetEventId(), startedEvent.GetEventId(), nil, identity)
	timerTasks := []persistence.Task{}
	timerInfos := []*persistence.TimerInfo{}
	decisionCompletedID := int64(4)
	tBuilder := newTimerBuilder(s.ShardContext.GetConfig(), s.logger, &mockTimeSource{currTime: time.Now()})

	for _, timeOut := range timeOuts {
		_, ti := builder.AddTimerStartedEvent(decisionCompletedID,
			&workflow.StartTimerDecisionAttributes{
				TimerId:                   common.StringPtr(uuid.New()),
				StartToFireTimeoutSeconds: common.Int64Ptr(int64(timeOut)),
			})
		timerInfos = append(timerInfos, ti)
		tBuilder.AddUserTimer(ti, builder)
	}

	if t := tBuilder.GetUserTimerTaskIfNeeded(builder); t != nil {
		timerTasks = append(timerTasks, t)
	}

	s.updateTimerSeqNumbers(timerTasks)

	updatedState := createMutableState(builder)
	err3 := s.UpdateWorkflowExecution(updatedState.ExecutionInfo, nil, nil, int64(3), timerTasks, nil, nil, nil, timerInfos, nil)
	s.Nil(err3)

	return createMutableState(builder), timerTasks
}

func (s *timerQueueProcessorSuite) addDecisionTimer(domainID string, we workflow.WorkflowExecution, tb *timerBuilder) []persistence.Task {
	state, err := s.GetWorkflowExecutionInfo(domainID, we)
	s.Nil(err)

	condition := state.ExecutionInfo.NextEventID
	builder := newMutableStateBuilder(s.ShardContext.GetConfig(), s.logger)
	builder.Load(state)

	scheduledEvent, _ := addDecisionTaskScheduledEvent(builder)
	addDecisionTaskStartedEvent(builder, scheduledEvent.GetEventId(), state.ExecutionInfo.TaskList, "identity")

	timeOutTask := tb.AddDecisionTimoutTask(scheduledEvent.GetEventId(), 1)
	timerTasks := []persistence.Task{timeOutTask}

	s.updateTimerSeqNumbers(timerTasks)
	err2 := s.UpdateWorkflowExecution(state.ExecutionInfo, nil, nil, condition, timerTasks, nil, nil, nil, nil, nil)
	s.Nil(err2, "No error expected.")
	return timerTasks
}

func (s *timerQueueProcessorSuite) addUserTimer(domainID string, we workflow.WorkflowExecution, timerID string, tb *timerBuilder) []persistence.Task {
	state, err := s.GetWorkflowExecutionInfo(domainID, we)
	s.Nil(err)
	builder := newMutableStateBuilder(s.ShardContext.GetConfig(), s.logger)
	builder.Load(state)
	condition := state.ExecutionInfo.NextEventID

	// create a user timer
	_, ti := builder.AddTimerStartedEvent(emptyEventID,
		&workflow.StartTimerDecisionAttributes{TimerId: common.StringPtr(timerID), StartToFireTimeoutSeconds: common.Int64Ptr(1)})
	tb.AddUserTimer(ti, builder)
	t := tb.GetUserTimerTaskIfNeeded(builder)
	s.NotNil(t)
	timerTasks := []persistence.Task{t}

	s.updateHistoryAndTimers(builder, timerTasks, condition)
	return timerTasks
}

func (s *timerQueueProcessorSuite) addHeartBeatTimer(domainID string,
	we workflow.WorkflowExecution, tb *timerBuilder) (*workflow.HistoryEvent, []persistence.Task) {
	state, err := s.GetWorkflowExecutionInfo(domainID, we)
	s.Nil(err)
	builder := newMutableStateBuilder(s.ShardContext.GetConfig(), s.logger)
	builder.Load(state)
	condition := state.ExecutionInfo.NextEventID

	ase, ai := builder.AddActivityTaskScheduledEvent(emptyEventID,
		&workflow.ScheduleActivityTaskDecisionAttributes{
			HeartbeatTimeoutSeconds: common.Int32Ptr(1),
		})
	builder.AddActivityTaskStartedEvent(ai, ase.GetEventId(), uuid.New(), &workflow.PollForActivityTaskRequest{})

	// create a heart beat timeout
	tt := tb.GetActivityTimerTaskIfNeeded(builder)
	s.NotNil(tt)
	timerTasks := []persistence.Task{tt}

	s.updateHistoryAndTimers(builder, timerTasks, condition)
	return ase, timerTasks
}

func (s *timerQueueProcessorSuite) closeWorkflow(domainID string, we workflow.WorkflowExecution) {
	state, err := s.GetWorkflowExecutionInfo(domainID, we)
	s.Nil(err)

	state.ExecutionInfo.State = persistence.WorkflowStateCompleted

	err2 := s.UpdateWorkflowExecution(state.ExecutionInfo, nil, nil, state.ExecutionInfo.NextEventID, nil, nil, nil, nil, nil, nil)
	s.Nil(err2, "No error expected.")
}

func (s *timerQueueProcessorSuite) TestSingleTimerTask() {
	domainID := "7b3fe0f6-e98f-4960-bdb7-220d0fb3f521"
	workflowExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("single-timer-test"),
		RunId:      common.StringPtr("6cc028d3-b4be-4038-80c9-bbcf99f7f109"),
	}
	taskList := "single-timer-queue"
	identity := "testIdentity"
	_, tt := s.createExecutionWithTimers(domainID, workflowExecution, taskList, identity, []int32{1})

	timerInfo, err := s.GetTimerIndexTasks()
	s.Nil(err, "No error expected.")
	s.NotEmpty(timerInfo, "Expected non empty timers list")
	s.Equal(1, len(timerInfo))

	processor := newTimerQueueProcessor(s.ShardContext, s.engineImpl, s.WorkflowMgr, s.logger).(*timerQueueProcessorImpl)
	processor.Start()
	processor.NotifyNewTimer(tt)

	for {
		timerInfo, err := s.GetTimerIndexTasks()
		s.Nil(err, "No error expected.")
		if len(timerInfo) == 0 {
			processor.Stop()
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	timerInfo, err = s.GetTimerIndexTasks()
	s.Nil(err, "No error expected.")
	s.Equal(0, len(timerInfo))
}

func (s *timerQueueProcessorSuite) TestManyTimerTasks() {
	domainID := "5bb49df8-71bc-4c63-b57f-05f2a508e7b5"
	workflowExecution := workflow.WorkflowExecution{WorkflowId: common.StringPtr("multiple-timer-test"),
		RunId: common.StringPtr("0d00698f-08e1-4d36-a3e2-3bf109f5d2d6")}

	taskList := "multiple-timer-queue"
	identity := "testIdentity"
	_, tt := s.createExecutionWithTimers(domainID, workflowExecution, taskList, identity, []int32{1, 2, 3})

	timerInfo, err := s.GetTimerIndexTasks()
	s.Nil(err, "No error expected.")
	s.NotEmpty(timerInfo, "Expected non empty timers list")
	s.Equal(1, len(timerInfo))

	processor := newTimerQueueProcessor(s.ShardContext, s.engineImpl, s.WorkflowMgr, s.logger).(*timerQueueProcessorImpl)
	processor.Start()

	processor.NotifyNewTimer(tt)

	for {
		timerInfo, err := s.GetTimerIndexTasks()
		s.logger.Infof("TestManyTimerTasks: GetTimerIndexTasks: Response Count: %d \n", len(timerInfo))
		s.Nil(err, "No error expected.")
		if len(timerInfo) == 0 {
			processor.Stop()
			break
		}
		time.Sleep(1000 * time.Millisecond)
	}

	timerInfo, err = s.GetTimerIndexTasks()
	s.Nil(err, "No error expected.")
	s.Equal(0, len(timerInfo))

	s.Equal(uint64(3), processor.timerFiredCount)
}

func (s *timerQueueProcessorSuite) TestTimerTaskAfterProcessorStart() {
	domainID := "5bb49df8-71bc-4c63-b57f-05f2a508e7b5"
	workflowExecution := workflow.WorkflowExecution{WorkflowId: common.StringPtr("After-timer-test"),
		RunId: common.StringPtr("0d00698f-08e1-4d36-a3e2-3bf109f5d2d6")}

	taskList := "After-timer-queue"
	identity := "testIdentity"

	s.createExecutionWithTimers(domainID, workflowExecution, taskList, identity, []int32{})

	timerInfo, err := s.GetTimerIndexTasks()
	s.Nil(err, "No error expected.")
	s.Empty(timerInfo, "Expected empty timers list")

	processor := newTimerQueueProcessor(s.ShardContext, s.engineImpl, s.WorkflowMgr, s.logger).(*timerQueueProcessorImpl)
	processor.Start()

	tBuilder := newTimerBuilder(s.ShardContext.GetConfig(), s.logger, &mockTimeSource{currTime: time.Now()})
	tt := s.addDecisionTimer(domainID, workflowExecution, tBuilder)
	processor.NotifyNewTimer(tt)

	s.waitForTimerTasksToProcess(processor)

	timerInfo, err = s.GetTimerIndexTasks()
	s.Nil(err, "No error expected.")
	s.Equal(0, len(timerInfo))

	s.Equal(uint64(1), processor.timerFiredCount)
}

func (s *timerQueueProcessorSuite) waitForTimerTasksToProcess(p timerQueueProcessor) {
	for i := 0; i < 10; i++ {
		timerInfo, err := s.GetTimerIndexTasks()
		//fmt.Printf("TestAfterTimerTasks: GetTimerIndexTasks: Response Count: %d \n", len(timerInfo))
		s.Nil(err, "No error expected.")
		if len(timerInfo) == 0 {
			p.Stop()
			break
		}
		time.Sleep(1000 * time.Millisecond)
	}
}

func (s *timerQueueProcessorSuite) checkTimedOutEventFor(domainID string, we workflow.WorkflowExecution,
	scheduleID int64) bool {
	info, err1 := s.GetWorkflowExecutionInfo(domainID, we)
	s.Nil(err1)
	builder := newMutableStateBuilder(s.ShardContext.GetConfig(), s.logger)
	builder.Load(info)
	_, isRunning := builder.GetActivityInfo(scheduleID)

	return isRunning
}

func (s *timerQueueProcessorSuite) checkTimedOutEventForUserTimer(domainID string, we workflow.WorkflowExecution,
	timerID string) bool {
	info, err1 := s.GetWorkflowExecutionInfo(domainID, we)
	s.Nil(err1)
	builder := newMutableStateBuilder(s.ShardContext.GetConfig(), s.logger)
	builder.Load(info)

	isRunning, _ := builder.GetUserTimer(timerID)
	return isRunning
}

func (s *timerQueueProcessorSuite) updateHistoryAndTimers(ms *mutableStateBuilder, timerTasks []persistence.Task, condition int64) {
	updatedState := createMutableState(ms)

	actInfos := []*persistence.ActivityInfo{}
	for _, x := range updatedState.ActivitInfos {
		actInfos = append(actInfos, x)
	}
	timerInfos := []*persistence.TimerInfo{}
	for _, x := range updatedState.TimerInfos {
		timerInfos = append(timerInfos, x)
	}

	s.updateTimerSeqNumbers(timerTasks)
	err3 := s.UpdateWorkflowExecution(
		updatedState.ExecutionInfo, nil, nil, condition, timerTasks, nil, actInfos, nil, timerInfos, nil)
	s.Nil(err3)
}

func (s *timerQueueProcessorSuite) TestTimerActivityTaskScheduleToStart_WithOutStart() {
	domainID := "5bb49df8-71bc-4c63-b57f-05f2a508e7b5"
	workflowExecution := workflow.WorkflowExecution{WorkflowId: common.StringPtr("activity-timer-SCHEDULE_TO_START-test"),
		RunId: common.StringPtr("0d00698f-08e1-4d36-a3e2-3bf109f5d2d6")}

	taskList := "activity-timer-queue"

	s.createExecutionWithTimers(domainID, workflowExecution, taskList, "identity", []int32{})

	// TimeoutType_SCHEDULE_TO_START - Without Start
	processor := newTimerQueueProcessor(s.ShardContext, s.engineImpl, s.WorkflowMgr, s.logger).(*timerQueueProcessorImpl)
	processor.Start()

	state, err := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.Nil(err)
	builder := newMutableStateBuilder(s.ShardContext.GetConfig(), s.logger)
	builder.Load(state)
	condition := state.ExecutionInfo.NextEventID

	activityScheduledEvent, _ := builder.AddActivityTaskScheduledEvent(emptyEventID,
		&workflow.ScheduleActivityTaskDecisionAttributes{
			ScheduleToStartTimeoutSeconds: common.Int32Ptr(1),
			ScheduleToCloseTimeoutSeconds: common.Int32Ptr(2),
		})

	tBuilder := newTimerBuilder(s.ShardContext.GetConfig(), s.logger, &mockTimeSource{currTime: time.Now()})
	tt := tBuilder.GetActivityTimerTaskIfNeeded(builder)
	s.NotNil(tt)
	timerTasks := []persistence.Task{tt}

	s.updateHistoryAndTimers(builder, timerTasks, condition)
	processor.NotifyNewTimer(timerTasks)

	s.waitForTimerTasksToProcess(processor)
	s.Equal(uint64(1), processor.timerFiredCount)
	running := s.checkTimedOutEventFor(domainID, workflowExecution, activityScheduledEvent.GetEventId())
	s.False(running)
}

func (s *timerQueueProcessorSuite) TestTimerActivityTaskScheduleToStart_WithStart() {
	domainID := "5bb49df8-71bc-4c63-b57f-05f2a508e7b5"
	workflowExecution := workflow.WorkflowExecution{WorkflowId: common.StringPtr("activity-timer-SCHEDULE_TO_START-Started-test"),
		RunId: common.StringPtr("0d00698f-08e1-4d36-a3e2-3bf109f5d2d6")}

	taskList := "activity-timer-queue"

	s.createExecutionWithTimers(domainID, workflowExecution, taskList, "identity", []int32{})

	// TimeoutType_SCHEDULE_TO_START - With Start
	p := newTimerQueueProcessor(s.ShardContext, s.engineImpl, s.WorkflowMgr, s.logger).(*timerQueueProcessorImpl)
	p.Start()

	state, err := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.Nil(err)
	builder := newMutableStateBuilder(s.ShardContext.GetConfig(), s.logger)
	builder.Load(state)
	condition := state.ExecutionInfo.NextEventID

	ase, ai := builder.AddActivityTaskScheduledEvent(emptyEventID,
		&workflow.ScheduleActivityTaskDecisionAttributes{
			ScheduleToStartTimeoutSeconds: common.Int32Ptr(1),
			StartToCloseTimeoutSeconds:    common.Int32Ptr(1),
		})
	builder.AddActivityTaskStartedEvent(ai, ase.GetEventId(), uuid.New(), &workflow.PollForActivityTaskRequest{})

	// create a schedule to start timeout
	tBuilder := newTimerBuilder(s.ShardContext.GetConfig(), s.logger, &mockTimeSource{currTime: time.Now()})
	tt := tBuilder.GetActivityTimerTaskIfNeeded(builder)
	s.NotNil(tt)
	timerTasks := []persistence.Task{tt}

	s.updateHistoryAndTimers(builder, timerTasks, condition)
	p.NotifyNewTimer(timerTasks)

	s.waitForTimerTasksToProcess(p)
	s.Equal(uint64(1), p.timerFiredCount)
	running := s.checkTimedOutEventFor(domainID, workflowExecution, ase.GetEventId())
	s.False(running)
}

func (s *timerQueueProcessorSuite) TestTimerActivityTaskScheduleToStart_MoreThanStartToClose() {
	domainID := "5bb49df8-71bc-4c63-b57f-05f2a508e7b5"
	workflowExecution := workflow.WorkflowExecution{WorkflowId: common.StringPtr("activity-timer-SCHEDULE_TO_START-more-than-start2close"),
		RunId: common.StringPtr("0d00698f-08e1-4d36-a3e2-3bf109f5d2d6")}

	taskList := "activity-timer-queue"

	s.createExecutionWithTimers(domainID, workflowExecution, taskList, "identity", []int32{})

	// TimeoutType_SCHEDULE_TO_START - Without Start
	processor := newTimerQueueProcessor(s.ShardContext, s.engineImpl, s.WorkflowMgr, s.logger).(*timerQueueProcessorImpl)
	processor.Start()

	state, err := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.Nil(err)
	builder := newMutableStateBuilder(s.ShardContext.GetConfig(), s.logger)
	builder.Load(state)
	condition := state.ExecutionInfo.NextEventID

	activityScheduledEvent, _ := builder.AddActivityTaskScheduledEvent(emptyEventID,
		&workflow.ScheduleActivityTaskDecisionAttributes{
			ScheduleToStartTimeoutSeconds: common.Int32Ptr(2),
			StartToCloseTimeoutSeconds:    common.Int32Ptr(1),
		})

	tBuilder := newTimerBuilder(s.ShardContext.GetConfig(), s.logger, &mockTimeSource{currTime: time.Now()})
	tt := tBuilder.GetActivityTimerTaskIfNeeded(builder)
	s.NotNil(tt)
	s.Equal(workflow.TimeoutType_SCHEDULE_TO_START, workflow.TimeoutType(tt.(*persistence.ActivityTimeoutTask).TimeoutType))
	timerTasks := []persistence.Task{tt}

	s.updateHistoryAndTimers(builder, timerTasks, condition)
	processor.NotifyNewTimer(timerTasks)

	s.waitForTimerTasksToProcess(processor)
	s.Equal(uint64(1), processor.timerFiredCount)
	running := s.checkTimedOutEventFor(domainID, workflowExecution, activityScheduledEvent.GetEventId())
	s.False(running)
}

func (s *timerQueueProcessorSuite) TestTimerActivityTaskStartToClose_WithStart() {
	domainID := "5bb49df8-71bc-4c63-b57f-05f2a508e123"
	workflowExecution := workflow.WorkflowExecution{WorkflowId: common.StringPtr("activity-timer-START_TO_CLOSE-Started-test"),
		RunId: common.StringPtr("0d00698f-08e1-4d36-a3e2-3bf109f5d2d6")}

	taskList := "activity-timer-queue"

	s.createExecutionWithTimers(domainID, workflowExecution, taskList, "identity", []int32{})

	// TimeoutType_START_TO_CLOSE - Just start.
	p := newTimerQueueProcessor(s.ShardContext, s.engineImpl, s.WorkflowMgr, s.logger).(*timerQueueProcessorImpl)
	p.Start()

	state, err := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.Nil(err)
	builder := newMutableStateBuilder(s.ShardContext.GetConfig(), s.logger)
	builder.Load(state)
	condition := state.ExecutionInfo.NextEventID

	ase, ai := builder.AddActivityTaskScheduledEvent(emptyEventID,
		&workflow.ScheduleActivityTaskDecisionAttributes{
			ScheduleToStartTimeoutSeconds: common.Int32Ptr(1),
			StartToCloseTimeoutSeconds:    common.Int32Ptr(1),
		})
	builder.AddActivityTaskStartedEvent(ai, ase.GetEventId(), uuid.New(), &workflow.PollForActivityTaskRequest{})

	// create a start to close timeout
	tBuilder := newTimerBuilder(s.ShardContext.GetConfig(), s.logger, &mockTimeSource{currTime: time.Now()})
	tt := tBuilder.GetActivityTimerTaskIfNeeded(builder)
	s.NotNil(tt)
	timerTasks := []persistence.Task{tt}

	s.updateHistoryAndTimers(builder, timerTasks, condition)
	p.NotifyNewTimer(timerTasks)

	s.waitForTimerTasksToProcess(p)
	s.Equal(uint64(1), p.timerFiredCount)
	running := s.checkTimedOutEventFor(domainID, workflowExecution, ase.GetEventId())
	s.False(running)
}

func (s *timerQueueProcessorSuite) TestTimerActivityTaskStartToClose_CompletedActivity() {
	domainID := "5bb49df8-71bc-4c63-b57f-05f2a508e7b5"
	workflowExecution := workflow.WorkflowExecution{WorkflowId: common.StringPtr("activity-timer-START_TO_CLOSE-Completed-test"),
		RunId: common.StringPtr("0d00698f-08e1-4d36-a3e2-3bf109f5d2d6")}

	taskList := "activity-timer-queue"
	s.createExecutionWithTimers(domainID, workflowExecution, taskList, "identity", []int32{})

	// TimeoutType_START_TO_CLOSE - Start and Completed activity.
	p := newTimerQueueProcessor(s.ShardContext, s.engineImpl, s.WorkflowMgr, s.logger).(*timerQueueProcessorImpl)
	p.Start()

	state, err := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.Nil(err)
	builder := newMutableStateBuilder(s.ShardContext.GetConfig(), s.logger)
	builder.Load(state)
	condition := state.ExecutionInfo.NextEventID

	ase, ai := builder.AddActivityTaskScheduledEvent(emptyEventID,
		&workflow.ScheduleActivityTaskDecisionAttributes{
			ScheduleToStartTimeoutSeconds: common.Int32Ptr(1),
			StartToCloseTimeoutSeconds:    common.Int32Ptr(1),
		})
	aste := builder.AddActivityTaskStartedEvent(ai, ase.GetEventId(), uuid.New(), &workflow.PollForActivityTaskRequest{})
	builder.AddActivityTaskCompletedEvent(ase.GetEventId(), aste.GetEventId(), &workflow.RespondActivityTaskCompletedRequest{
		Identity: common.StringPtr("test-id"),
		Result_:  []byte("result"),
	})

	// create a start to close timeout
	tBuilder := newTimerBuilder(s.ShardContext.GetConfig(), s.logger, &mockTimeSource{currTime: time.Now()})
	t, err := tBuilder.AddStartToCloseActivityTimeout(ai)
	s.NoError(err)
	s.NotNil(t)
	timerTasks := []persistence.Task{t}

	s.updateHistoryAndTimers(builder, timerTasks, condition)
	p.NotifyNewTimer(timerTasks)

	s.waitForTimerTasksToProcess(p)
	s.Equal(uint64(1), p.timerFiredCount)
	running := s.checkTimedOutEventFor(domainID, workflowExecution, ase.GetEventId())
	s.False(running)
}

func (s *timerQueueProcessorSuite) TestTimerActivityTaskScheduleToClose_JustScheduled() {
	domainID := "5bb49df8-71bc-4c63-b57f-05f2a508e7b5"
	workflowExecution := workflow.WorkflowExecution{WorkflowId: common.StringPtr("activity-timer-SCHEDULE_TO_CLOSE-Scheduled-test"),
		RunId: common.StringPtr("0d00698f-08e1-4d36-a3e2-3bf109f5d2d6")}

	taskList := "activity-timer-queue"
	s.createExecutionWithTimers(domainID, workflowExecution, taskList, "identity", []int32{})

	// TimeoutType_SCHEDULE_TO_CLOSE - Just Scheduled.
	p := newTimerQueueProcessor(s.ShardContext, s.engineImpl, s.WorkflowMgr, s.logger).(*timerQueueProcessorImpl)
	p.Start()

	state, err := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.Nil(err)
	builder := newMutableStateBuilder(s.ShardContext.GetConfig(), s.logger)
	builder.Load(state)
	condition := state.ExecutionInfo.NextEventID

	ase, _ := builder.AddActivityTaskScheduledEvent(emptyEventID,
		&workflow.ScheduleActivityTaskDecisionAttributes{
			ScheduleToStartTimeoutSeconds: common.Int32Ptr(1),
			ScheduleToCloseTimeoutSeconds: common.Int32Ptr(1),
		})

	// create a schedule to close timeout
	tBuilder := newTimerBuilder(s.ShardContext.GetConfig(), s.logger, &mockTimeSource{currTime: time.Now()})
	tt := tBuilder.GetActivityTimerTaskIfNeeded(builder)
	s.NotNil(tt)
	timerTasks := []persistence.Task{tt}

	s.updateHistoryAndTimers(builder, timerTasks, condition)
	p.NotifyNewTimer(timerTasks)

	s.waitForTimerTasksToProcess(p)
	s.Equal(uint64(1), p.timerFiredCount)
	running := s.checkTimedOutEventFor(domainID, workflowExecution, ase.GetEventId())
	s.False(running)
}

func (s *timerQueueProcessorSuite) TestTimerActivityTaskScheduleToClose_Started() {
	domainID := "5bb49df8-71bc-4c63-b57f-05f2a508e7b5"
	workflowExecution := workflow.WorkflowExecution{WorkflowId: common.StringPtr("activity-timer-SCHEDULE_TO_CLOSE-Started-test"),
		RunId: common.StringPtr("0d00698f-08e1-4d36-a3e2-3bf109f5d2d6")}

	taskList := "activity-timer-queue"
	s.createExecutionWithTimers(domainID, workflowExecution, taskList, "identity", []int32{})

	// TimeoutType_SCHEDULE_TO_CLOSE - Scheduled and started.
	p := newTimerQueueProcessor(s.ShardContext, s.engineImpl, s.WorkflowMgr, s.logger).(*timerQueueProcessorImpl)
	p.Start()

	state, err := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.Nil(err)
	builder := newMutableStateBuilder(s.ShardContext.GetConfig(), s.logger)
	builder.Load(state)
	condition := state.ExecutionInfo.NextEventID

	ase, ai := builder.AddActivityTaskScheduledEvent(emptyEventID,
		&workflow.ScheduleActivityTaskDecisionAttributes{
			ScheduleToStartTimeoutSeconds: common.Int32Ptr(1),
			ScheduleToCloseTimeoutSeconds: common.Int32Ptr(1),
			StartToCloseTimeoutSeconds:    common.Int32Ptr(1),
		})
	builder.AddActivityTaskStartedEvent(ai, ase.GetEventId(), uuid.New(), &workflow.PollForActivityTaskRequest{})

	// create a schedule to close timeout
	tBuilder := newTimerBuilder(s.ShardContext.GetConfig(), s.logger, &mockTimeSource{currTime: time.Now()})
	tt := tBuilder.GetActivityTimerTaskIfNeeded(builder)
	s.NotNil(tt)
	timerTasks := []persistence.Task{tt}

	s.updateHistoryAndTimers(builder, timerTasks, condition)
	p.NotifyNewTimer(timerTasks)

	s.waitForTimerTasksToProcess(p)
	s.Equal(uint64(1), p.timerFiredCount)
	running := s.checkTimedOutEventFor(domainID, workflowExecution, ase.GetEventId())
	s.False(running)
}

func (s *timerQueueProcessorSuite) TestTimerActivityTaskScheduleToClose_Completed() {
	domainID := "5bb49df8-71bc-4c63-b57f-05f2a508e7b5"
	workflowExecution := workflow.WorkflowExecution{WorkflowId: common.StringPtr("activity-timer-SCHEDULE_TO_CLOSE-Completed-test"),
		RunId: common.StringPtr("0d00698f-08e1-4d36-a3e2-3bf109f5d2d6")}

	taskList := "activity-timer-queue"
	s.createExecutionWithTimers(domainID, workflowExecution, taskList, "identity", []int32{})

	// TimeoutType_SCHEDULE_TO_CLOSE - Scheduled, started, completed.
	p := newTimerQueueProcessor(s.ShardContext, s.engineImpl, s.WorkflowMgr, s.logger).(*timerQueueProcessorImpl)
	p.Start()

	state, err := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.Nil(err)
	builder := newMutableStateBuilder(s.ShardContext.GetConfig(), s.logger)
	builder.Load(state)
	condition := state.ExecutionInfo.NextEventID

	ase, ai := builder.AddActivityTaskScheduledEvent(emptyEventID,
		&workflow.ScheduleActivityTaskDecisionAttributes{
			ScheduleToStartTimeoutSeconds: common.Int32Ptr(1),
			ScheduleToCloseTimeoutSeconds: common.Int32Ptr(1),
		})
	aste := builder.AddActivityTaskStartedEvent(ai, ase.GetEventId(), uuid.New(), &workflow.PollForActivityTaskRequest{})
	builder.AddActivityTaskCompletedEvent(ase.GetEventId(), aste.GetEventId(), &workflow.RespondActivityTaskCompletedRequest{
		Identity: common.StringPtr("test-id"),
		Result_:  []byte("result"),
	})

	// create a schedule to close timeout
	tBuilder := newTimerBuilder(s.ShardContext.GetConfig(), s.logger, &mockTimeSource{currTime: time.Now()})
	t, err := tBuilder.AddScheduleToCloseActivityTimeout(ai)
	s.NoError(err)
	s.NotNil(t)
	timerTasks := []persistence.Task{t}

	s.updateHistoryAndTimers(builder, timerTasks, condition)
	p.NotifyNewTimer(timerTasks)

	s.waitForTimerTasksToProcess(p)
	s.Equal(uint64(1), p.timerFiredCount)
	running := s.checkTimedOutEventFor(domainID, workflowExecution, ase.GetEventId())
	s.False(running)
}

func (s *timerQueueProcessorSuite) TestTimerActivityTaskHeartBeat_JustStarted() {
	domainID := "5bb49df8-71bc-4c63-b57f-05f2a508e7b5"
	workflowExecution := workflow.WorkflowExecution{WorkflowId: common.StringPtr("activity-timer-hb-started-test"),
		RunId: common.StringPtr("0d00698f-08e1-4d36-a3e2-3bf109f5d2d6")}

	taskList := "activity-timer-queue"

	s.createExecutionWithTimers(domainID, workflowExecution, taskList, "identity", []int32{})

	// TimeoutType_HEARTBEAT - Scheduled, started.
	p := newTimerQueueProcessor(s.ShardContext, s.engineImpl, s.WorkflowMgr, s.logger).(*timerQueueProcessorImpl)
	p.Start()

	tBuilder := newTimerBuilder(s.ShardContext.GetConfig(), s.logger, &mockTimeSource{currTime: time.Now()})
	ase, timerTasks := s.addHeartBeatTimer(domainID, workflowExecution, tBuilder)

	p.NotifyNewTimer(timerTasks)
	s.waitForTimerTasksToProcess(p)
	s.Equal(uint64(1), p.timerFiredCount)
	running := s.checkTimedOutEventFor(domainID, workflowExecution, ase.GetEventId())
	s.False(running)
}

func (s *timerQueueProcessorSuite) TestTimerUserTimers() {
	domainID := "5bb49df8-71bc-4c63-b57f-05f2a508e7b5"
	workflowExecution := workflow.WorkflowExecution{WorkflowId: common.StringPtr("user-timer-test"),
		RunId: common.StringPtr("0d00698f-08e1-4d36-a3e2-3bf109f5d2d6")}

	taskList := "user-timer-queue"
	s.createExecutionWithTimers(domainID, workflowExecution, taskList, "identity", []int32{})

	// Single timer.
	p := newTimerQueueProcessor(s.ShardContext, s.engineImpl, s.WorkflowMgr, s.logger).(*timerQueueProcessorImpl)
	p.Start()

	tBuilder := newTimerBuilder(s.ShardContext.GetConfig(), s.logger, &mockTimeSource{currTime: time.Now()})
	timerID := "tid1"
	timerTasks := s.addUserTimer(domainID, workflowExecution, timerID, tBuilder)
	p.NotifyNewTimer(timerTasks)
	s.waitForTimerTasksToProcess(p)
	s.Equal(uint64(1), p.timerFiredCount)
	running := s.checkTimedOutEventForUserTimer(domainID, workflowExecution, timerID)
	s.False(running)
}

func (s *timerQueueProcessorSuite) TestTimerUserTimersSameExpiry() {
	domainID := "5bb49df8-71bc-4c63-b57f-05f2a508e7b5"
	workflowExecution := workflow.WorkflowExecution{WorkflowId: common.StringPtr("user-timer-same-expiry-test"),
		RunId: common.StringPtr("0d00698f-08e1-4d36-a3e2-3bf109f5d2d6")}

	taskList := "user-timer-same-expiry-queue"
	s.createExecutionWithTimers(domainID, workflowExecution, taskList, "identity", []int32{})

	// Two timers.
	p := newTimerQueueProcessor(s.ShardContext, s.engineImpl, s.WorkflowMgr, s.logger).(*timerQueueProcessorImpl)
	p.Start()

	state, err := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.Nil(err)
	builder := newMutableStateBuilder(s.ShardContext.GetConfig(), s.logger)
	builder.Load(state)
	condition := state.ExecutionInfo.NextEventID

	// load any timers.
	tBuilder := newTimerBuilder(s.ShardContext.GetConfig(), s.logger, &mockTimeSource{currTime: time.Now().Add(-1 * time.Second)})
	timerTasks := []persistence.Task{}

	// create two user timers.
	_, ti := builder.AddTimerStartedEvent(emptyEventID,
		&workflow.StartTimerDecisionAttributes{TimerId: common.StringPtr("tid1"), StartToFireTimeoutSeconds: common.Int64Ptr(1)})
	_, ti2 := builder.AddTimerStartedEvent(emptyEventID,
		&workflow.StartTimerDecisionAttributes{TimerId: common.StringPtr("tid2"), StartToFireTimeoutSeconds: common.Int64Ptr(1)})

	tBuilder.AddUserTimer(ti, builder)
	tBuilder.AddUserTimer(ti2, builder)
	t := tBuilder.GetUserTimerTaskIfNeeded(builder)
	timerTasks = append(timerTasks, t)

	s.updateHistoryAndTimers(builder, timerTasks, condition)
	p.NotifyNewTimer(timerTasks)

	s.waitForTimerTasksToProcess(p)
	s.Equal(uint64(len(timerTasks)), p.timerFiredCount)
	running := s.checkTimedOutEventForUserTimer(domainID, workflowExecution, ti.TimerID)
	s.False(running)
	running = s.checkTimedOutEventForUserTimer(domainID, workflowExecution, ti2.TimerID)
	s.False(running)
}

func (s *timerQueueProcessorSuite) TestTimersOnClosedWorkflow() {
	domainID := "5bb49df8-71bc-4c63-b57f-05f2a508e7b5"
	workflowExecution := workflow.WorkflowExecution{WorkflowId: common.StringPtr("closed-workflow-test"),
		RunId: common.StringPtr("0d00698f-08e1-4d36-a3e2-3bf109f5d2d6")}

	taskList := "closed-workflow-queue"
	s.createExecutionWithTimers(domainID, workflowExecution, taskList, "identity", []int32{})

	p := newTimerQueueProcessor(s.ShardContext, s.engineImpl, s.WorkflowMgr, s.logger).(*timerQueueProcessorImpl)
	p.Start()

	tBuilder := newTimerBuilder(s.ShardContext.GetConfig(), s.logger, &mockTimeSource{currTime: time.Now()})

	// Start of one of each timers each
	s.addDecisionTimer(domainID, workflowExecution, tBuilder)
	tt := s.addUserTimer(domainID, workflowExecution, "tid1", tBuilder)
	s.addHeartBeatTimer(domainID, workflowExecution, tBuilder)

	// GEt current state of workflow.
	state0, err := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.Nil(err)

	// close workflow
	s.closeWorkflow(domainID, workflowExecution)

	p.NotifyNewTimer(tt)
	s.waitForTimerTasksToProcess(p)
	s.Equal(uint64(3), p.timerFiredCount)

	// Verify that no new events are added to workflow.
	state1, err := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.Nil(err)
	s.Equal(state0.ExecutionInfo.NextEventID, state1.ExecutionInfo.NextEventID)
}

func (s *timerQueueProcessorSuite) printHistory(builder *mutableStateBuilder) string {
	history, err := builder.hBuilder.Serialize()
	if err != nil {
		s.logger.Errorf("Error serializing history: %v", err)
		return ""
	}

	//s.logger.Info(string(history))
	return history.String()
}
