package history

import (
	"os"
	"testing"
	"time"

	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/mocks"
	"github.com/uber/cadence/common/persistence"

	log "github.com/Sirupsen/logrus"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
	"github.com/uber-common/bark"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common/cache"
)

type (
	timerQueueProcessorSuite struct {
		suite.Suite
		persistence.TestBase
		engineImpl       *historyEngineImpl
		mockShardManager *mocks.ShardManager
		shardClosedCh    chan int
		logger           bark.Logger

		mockMetadataMgr    *mocks.MetadataManager
		mockVisibilityMgr  *mocks.VisibilityManager
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

	shardID := 0
	s.mockShardManager = &mocks.ShardManager{}
	s.mockMetadataMgr = &mocks.MetadataManager{}
	resp, err := s.ShardMgr.GetShard(&persistence.GetShardRequest{ShardID: shardID})
	if err != nil {
		log.Fatal(err)
	}

	shard := &shardContextImpl{
		shardInfo:                 resp.ShardInfo,
		transferSequenceNumber:    1,
		executionManager:          s.WorkflowMgr,
		shardManager:              s.mockShardManager,
		historyMgr:                s.HistoryMgr,
		rangeSize:                 defaultRangeSize,
		maxTransferSequenceNumber: 100000,
		closeCh:                   s.shardClosedCh,
		logger:                    s.logger,
	}
	historyCache := newHistoryCache(historyCacheMaxSize, shard, s.logger)
	historyCache.disabled = true
	txProcessor := newTransferQueueProcessor(shard, s.mockVisibilityMgr, &mocks.MatchingClient{}, &mocks.HistoryClient{}, historyCache)
	s.engineImpl = &historyEngineImpl{
		shard:            shard,
		historyMgr:       s.HistoryMgr,
		txProcessor:      txProcessor,
		historyCache:     historyCache,
		domainCache:      cache.NewDomainCache(s.mockMetadataMgr, s.logger),
		logger:           s.logger,
		tokenSerializer:  common.NewJSONTaskTokenSerializer(),
		hSerializer:      common.NewJSONHistorySerializer(),
	}
}

func (s *timerQueueProcessorSuite) TearDownSuite() {
	s.TearDownWorkflowStore()
}

func (s *timerQueueProcessorSuite) TearDownTest() {
	s.mockShardManager.AssertExpectations(s.T())
}

func (s *timerQueueProcessorSuite) createExecutionWithTimers(domainID string, we workflow.WorkflowExecution, tl,
	identity string, timeOuts []int32) (*persistence.WorkflowMutableState, []persistence.Task) {

	// Generate first decision task event.
	builder := newMutableStateBuilder(s.logger)
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

	builder = newMutableStateBuilder(s.logger)
	builder.Load(state0)
	startedEvent := addDecisionTaskStartedEvent(builder, scheduleEvent.GetEventId(), tl, identity)
	addDecisionTaskCompletedEvent(builder, scheduleEvent.GetEventId(), startedEvent.GetEventId(), nil, identity)
	timerTasks := []persistence.Task{}
	timerInfos := []*persistence.TimerInfo{}
	decisionCompletedID := int64(4)
	tBuilder := newTimerBuilder(&localSeqNumGenerator{counter: 1}, s.logger)
	for _, timeOut := range timeOuts {
		_, ti := builder.AddTimerStartedEvent(decisionCompletedID,
			&workflow.StartTimerDecisionAttributes{
				TimerId:                   common.StringPtr(uuid.New()),
				StartToFireTimeoutSeconds: common.Int64Ptr(int64(timeOut)),
			})

		timerInfos = append(timerInfos, ti)
		if t := tBuilder.AddUserTimer(ti, builder); t != nil {
			timerTasks = append(timerTasks, t)
		}
	}

	updatedState := createMutableState(builder)
	err3 := s.UpdateWorkflowExecution(updatedState.ExecutionInfo, nil, nil, int64(3), timerTasks, nil, nil, nil, timerInfos, nil)
	s.Nil(err3)

	return createMutableState(builder), timerTasks
}

func (s *timerQueueProcessorSuite) addDecisionTimer(domainID string, we workflow.WorkflowExecution, tb *timerBuilder) *persistence.DecisionTimeoutTask {
	state, err := s.GetWorkflowExecutionInfo(domainID, we)
	s.Nil(err)

	condition := state.ExecutionInfo.NextEventID
	builder := newMutableStateBuilder(s.logger)
	builder.Load(state)

	scheduledEvent, _ := addDecisionTaskScheduledEvent(builder)
	addDecisionTaskStartedEvent(builder, scheduledEvent.GetEventId(), state.ExecutionInfo.TaskList, "identity")

	timeOutTask := tb.AddDecisionTimoutTask(scheduledEvent.GetEventId(), 1)
	timerTasks := []persistence.Task{timeOutTask}

	err2 := s.UpdateWorkflowExecution(state.ExecutionInfo, nil, nil, condition, timerTasks, nil, nil, nil, nil, nil)
	s.Nil(err2, "No error expected.")
	return timeOutTask
}

func (s *timerQueueProcessorSuite) addUserTimer(domainID string, we workflow.WorkflowExecution, timerID string, tb *timerBuilder) persistence.Task {
	state, err := s.GetWorkflowExecutionInfo(domainID, we)
	s.Nil(err)
	builder := newMutableStateBuilder(s.logger)
	builder.Load(state)
	condition := state.ExecutionInfo.NextEventID

	// create a user timer
	_, ti := builder.AddTimerStartedEvent(emptyEventID,
		&workflow.StartTimerDecisionAttributes{TimerId: common.StringPtr(timerID), StartToFireTimeoutSeconds: common.Int64Ptr(1)})
	t := tb.AddUserTimer(ti, builder)
	s.NotNil(t)
	timerTasks := []persistence.Task{t}

	s.updateHistoryAndTimers(builder, timerTasks, condition)
	return t
}

func (s *timerQueueProcessorSuite) addHeartBeatTimer(domainID string,
	we workflow.WorkflowExecution, tb *timerBuilder) (*workflow.HistoryEvent, *persistence.ActivityTimeoutTask) {
	state, err := s.GetWorkflowExecutionInfo(domainID, we)
	s.Nil(err)
	builder := newMutableStateBuilder(s.logger)
	builder.Load(state)
	condition := state.ExecutionInfo.NextEventID

	ase, ai := builder.AddActivityTaskScheduledEvent(emptyEventID,
		&workflow.ScheduleActivityTaskDecisionAttributes{
			HeartbeatTimeoutSeconds: common.Int32Ptr(1),
		})
	builder.AddActivityTaskStartedEvent(ai, ase.GetEventId(), uuid.New(), &workflow.PollForActivityTaskRequest{})

	// create a heart beat timeout
	t, err := tb.AddHeartBeatActivityTimeout(ai)
	s.NoError(err)
	s.NotNil(t)
	timerTasks := []persistence.Task{t}

	s.updateHistoryAndTimers(builder, timerTasks, condition)
	return ase, t
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
	s.createExecutionWithTimers(domainID, workflowExecution, taskList, identity, []int32{1})

	timerInfo, err := s.GetTimerIndexTasks(int64(MinTimerKey), int64(MaxTimerKey))
	s.Nil(err, "No error expected.")
	s.NotEmpty(timerInfo, "Expected non empty timers list")
	s.Equal(1, len(timerInfo))

	processor := newTimerQueueProcessor(s.engineImpl, s.WorkflowMgr, s.logger).(*timerQueueProcessorImpl)
	processor.Start()

	for {
		timerInfo, err := s.GetTimerIndexTasks(int64(MinTimerKey), int64(MaxTimerKey))
		s.Nil(err, "No error expected.")
		if len(timerInfo) == 0 {
			processor.Stop()
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	timerInfo, err = s.GetTimerIndexTasks(int64(MinTimerKey), int64(MaxTimerKey))
	s.Nil(err, "No error expected.")
	s.Equal(0, len(timerInfo))
}

func (s *timerQueueProcessorSuite) TestManyTimerTasks() {
	domainID := "5bb49df8-71bc-4c63-b57f-05f2a508e7b5"
	workflowExecution := workflow.WorkflowExecution{WorkflowId: common.StringPtr("multiple-timer-test"),
		RunId: common.StringPtr("0d00698f-08e1-4d36-a3e2-3bf109f5d2d6")}

	taskList := "multiple-timer-queue"
	identity := "testIdentity"
	s.createExecutionWithTimers(domainID, workflowExecution, taskList, identity, []int32{1, 2, 3})

	timerInfo, err := s.GetTimerIndexTasks(int64(MinTimerKey), int64(MaxTimerKey))
	s.Nil(err, "No error expected.")
	s.NotEmpty(timerInfo, "Expected non empty timers list")
	s.Equal(1, len(timerInfo))

	processor := newTimerQueueProcessor(s.engineImpl, s.WorkflowMgr, s.logger).(*timerQueueProcessorImpl)
	processor.Start()

	for {
		timerInfo, err := s.GetTimerIndexTasks(int64(MinTimerKey), int64(MaxTimerKey))
		s.logger.Infof("TestManyTimerTasks: GetTimerIndexTasks: Response Count: %d \n", len(timerInfo))
		s.Nil(err, "No error expected.")
		if len(timerInfo) == 0 {
			processor.Stop()
			break
		}
		time.Sleep(1000 * time.Millisecond)
	}

	timerInfo, err = s.GetTimerIndexTasks(int64(MinTimerKey), int64(MaxTimerKey))
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

	timerInfo, err := s.GetTimerIndexTasks(int64(MinTimerKey), int64(MaxTimerKey))
	s.Nil(err, "No error expected.")
	s.Empty(timerInfo, "Expected empty timers list")

	processor := newTimerQueueProcessor(s.engineImpl, s.WorkflowMgr, s.logger).(*timerQueueProcessorImpl)
	processor.Start()

	tBuilder := newTimerBuilder(&localSeqNumGenerator{counter: 1}, s.logger)
	timeOutTask := s.addDecisionTimer(domainID, workflowExecution, tBuilder)
	processor.NotifyNewTimer(timeOutTask.GetTaskID())

	s.waitForTimerTasksToProcess(processor)

	timerInfo, err = s.GetTimerIndexTasks(int64(MinTimerKey), int64(MaxTimerKey))
	s.Nil(err, "No error expected.")
	s.Equal(0, len(timerInfo))

	s.Equal(uint64(1), processor.timerFiredCount)
}

func (s *timerQueueProcessorSuite) waitForTimerTasksToProcess(p timerQueueProcessor) {
	for {
		timerInfo, err := s.GetTimerIndexTasks(int64(MinTimerKey), int64(MaxTimerKey))
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
	builder := newMutableStateBuilder(s.logger)
	builder.Load(info)
	_, isRunning := builder.GetActivityInfo(scheduleID)

	return isRunning
}

func (s *timerQueueProcessorSuite) checkTimedOutEventForUserTimer(domainID string, we workflow.WorkflowExecution,
	timerID string) bool {
	info, err1 := s.GetWorkflowExecutionInfo(domainID, we)
	s.Nil(err1)
	builder := newMutableStateBuilder(s.logger)
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
	processor := newTimerQueueProcessor(s.engineImpl, s.WorkflowMgr, s.logger).(*timerQueueProcessorImpl)
	processor.Start()

	state, err := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.Nil(err)
	builder := newMutableStateBuilder(s.logger)
	builder.Load(state)
	condition := state.ExecutionInfo.NextEventID

	activityScheduledEvent, ai := builder.AddActivityTaskScheduledEvent(emptyEventID,
		&workflow.ScheduleActivityTaskDecisionAttributes{
			ScheduleToStartTimeoutSeconds: common.Int32Ptr(1),
		})

	tBuilder := newTimerBuilder(&localSeqNumGenerator{counter: 1}, s.logger)
	t := tBuilder.AddScheduleToStartActivityTimeout(ai)
	s.NotNil(t)
	timerTasks := []persistence.Task{t}

	s.updateHistoryAndTimers(builder, timerTasks, condition)
	processor.NotifyNewTimer(t.GetTaskID())

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
	p := newTimerQueueProcessor(s.engineImpl, s.WorkflowMgr, s.logger).(*timerQueueProcessorImpl)
	p.Start()

	state, err := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.Nil(err)
	builder := newMutableStateBuilder(s.logger)
	builder.Load(state)
	condition := state.ExecutionInfo.NextEventID

	ase, ai := builder.AddActivityTaskScheduledEvent(emptyEventID,
		&workflow.ScheduleActivityTaskDecisionAttributes{
			ScheduleToStartTimeoutSeconds: common.Int32Ptr(1),
		})
	builder.AddActivityTaskStartedEvent(ai, ase.GetEventId(), uuid.New(), &workflow.PollForActivityTaskRequest{})

	// create a schedule to start timeout
	tBuilder := newTimerBuilder(&localSeqNumGenerator{counter: 1}, s.logger)
	t := tBuilder.AddScheduleToStartActivityTimeout(ai)
	s.NotNil(t)
	timerTasks := []persistence.Task{t}

	s.updateHistoryAndTimers(builder, timerTasks, condition)
	p.NotifyNewTimer(t.GetTaskID())

	s.waitForTimerTasksToProcess(p)
	s.Equal(uint64(1), p.timerFiredCount)
	running := s.checkTimedOutEventFor(domainID, workflowExecution, ase.GetEventId())
	s.True(running)
}

func (s *timerQueueProcessorSuite) TestTimerActivityTaskStartToClose_WithStart() {
	domainID := "5bb49df8-71bc-4c63-b57f-05f2a508e123"
	workflowExecution := workflow.WorkflowExecution{WorkflowId: common.StringPtr("activity-timer-START_TO_CLOSE-Started-test"),
		RunId: common.StringPtr("0d00698f-08e1-4d36-a3e2-3bf109f5d2d6")}

	taskList := "activity-timer-queue"

	s.createExecutionWithTimers(domainID, workflowExecution, taskList, "identity", []int32{})

	// TimeoutType_START_TO_CLOSE - Just start.
	p := newTimerQueueProcessor(s.engineImpl, s.WorkflowMgr, s.logger).(*timerQueueProcessorImpl)
	p.Start()

	state, err := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.Nil(err)
	builder := newMutableStateBuilder(s.logger)
	builder.Load(state)
	condition := state.ExecutionInfo.NextEventID

	ase, ai := builder.AddActivityTaskScheduledEvent(emptyEventID,
		&workflow.ScheduleActivityTaskDecisionAttributes{
			ScheduleToStartTimeoutSeconds: common.Int32Ptr(1),
		})
	builder.AddActivityTaskStartedEvent(ai, ase.GetEventId(), uuid.New(), &workflow.PollForActivityTaskRequest{})

	// create a start to close timeout
	tBuilder := newTimerBuilder(&localSeqNumGenerator{counter: 1}, s.logger)
	t, err := tBuilder.AddStartToCloseActivityTimeout(ai)
	s.NoError(err)
	s.NotNil(t)
	timerTasks := []persistence.Task{t}

	s.updateHistoryAndTimers(builder, timerTasks, condition)
	p.NotifyNewTimer(t.GetTaskID())

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
	p := newTimerQueueProcessor(s.engineImpl, s.WorkflowMgr, s.logger).(*timerQueueProcessorImpl)
	p.Start()

	state, err := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.Nil(err)
	builder := newMutableStateBuilder(s.logger)
	builder.Load(state)
	condition := state.ExecutionInfo.NextEventID

	ase, ai := builder.AddActivityTaskScheduledEvent(emptyEventID,
		&workflow.ScheduleActivityTaskDecisionAttributes{
			ScheduleToStartTimeoutSeconds: common.Int32Ptr(1),
		})
	aste := builder.AddActivityTaskStartedEvent(ai, ase.GetEventId(), uuid.New(), &workflow.PollForActivityTaskRequest{})
	builder.AddActivityTaskCompletedEvent(ase.GetEventId(), aste.GetEventId(), &workflow.RespondActivityTaskCompletedRequest{
		Identity: common.StringPtr("test-id"),
		Result_:  []byte("result"),
	})

	// create a start to close timeout
	tBuilder := newTimerBuilder(&localSeqNumGenerator{counter: 1}, s.logger)
	t, err := tBuilder.AddStartToCloseActivityTimeout(ai)
	s.NoError(err)
	s.NotNil(t)
	timerTasks := []persistence.Task{t}

	s.updateHistoryAndTimers(builder, timerTasks, condition)
	p.NotifyNewTimer(t.GetTaskID())

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
	p := newTimerQueueProcessor(s.engineImpl, s.WorkflowMgr, s.logger).(*timerQueueProcessorImpl)
	p.Start()

	state, err := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.Nil(err)
	builder := newMutableStateBuilder(s.logger)
	builder.Load(state)
	condition := state.ExecutionInfo.NextEventID

	ase, ai := builder.AddActivityTaskScheduledEvent(emptyEventID,
		&workflow.ScheduleActivityTaskDecisionAttributes{
			ScheduleToStartTimeoutSeconds: common.Int32Ptr(1),
		})

	// create a schedule to close timeout
	tBuilder := newTimerBuilder(&localSeqNumGenerator{counter: 1}, s.logger)
	t, err := tBuilder.AddScheduleToCloseActivityTimeout(ai)
	s.NoError(err)
	s.NotNil(t)
	timerTasks := []persistence.Task{t}

	s.updateHistoryAndTimers(builder, timerTasks, condition)
	p.NotifyNewTimer(t.GetTaskID())

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
	p := newTimerQueueProcessor(s.engineImpl, s.WorkflowMgr, s.logger).(*timerQueueProcessorImpl)
	p.Start()

	state, err := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.Nil(err)
	builder := newMutableStateBuilder(s.logger)
	builder.Load(state)
	condition := state.ExecutionInfo.NextEventID

	ase, ai := builder.AddActivityTaskScheduledEvent(emptyEventID,
		&workflow.ScheduleActivityTaskDecisionAttributes{
			ScheduleToStartTimeoutSeconds: common.Int32Ptr(1),
		})
	builder.AddActivityTaskStartedEvent(ai, ase.GetEventId(), uuid.New(), &workflow.PollForActivityTaskRequest{})

	// create a schedule to close timeout
	tBuilder := newTimerBuilder(&localSeqNumGenerator{counter: 1}, s.logger)
	t, err := tBuilder.AddScheduleToCloseActivityTimeout(ai)
	s.NoError(err)
	s.NotNil(t)
	timerTasks := []persistence.Task{t}

	s.updateHistoryAndTimers(builder, timerTasks, condition)
	p.NotifyNewTimer(t.GetTaskID())

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
	p := newTimerQueueProcessor(s.engineImpl, s.WorkflowMgr, s.logger).(*timerQueueProcessorImpl)
	p.Start()

	state, err := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.Nil(err)
	builder := newMutableStateBuilder(s.logger)
	builder.Load(state)
	condition := state.ExecutionInfo.NextEventID

	ase, ai := builder.AddActivityTaskScheduledEvent(emptyEventID,
		&workflow.ScheduleActivityTaskDecisionAttributes{
			ScheduleToStartTimeoutSeconds: common.Int32Ptr(1),
		})
	aste := builder.AddActivityTaskStartedEvent(ai, ase.GetEventId(), uuid.New(), &workflow.PollForActivityTaskRequest{})
	builder.AddActivityTaskCompletedEvent(ase.GetEventId(), aste.GetEventId(), &workflow.RespondActivityTaskCompletedRequest{
		Identity: common.StringPtr("test-id"),
		Result_:  []byte("result"),
	})

	// create a schedule to close timeout
	tBuilder := newTimerBuilder(&localSeqNumGenerator{counter: 1}, s.logger)
	t, err := tBuilder.AddScheduleToCloseActivityTimeout(ai)
	s.NoError(err)
	s.NotNil(t)
	timerTasks := []persistence.Task{t}

	s.updateHistoryAndTimers(builder, timerTasks, condition)
	p.NotifyNewTimer(t.GetTaskID())

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
	p := newTimerQueueProcessor(s.engineImpl, s.WorkflowMgr, s.logger).(*timerQueueProcessorImpl)
	p.Start()

	tBuilder := newTimerBuilder(&localSeqNumGenerator{counter: 1}, s.logger)
	ase, t := s.addHeartBeatTimer(domainID, workflowExecution, tBuilder)

	p.NotifyNewTimer(t.GetTaskID())
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
	p := newTimerQueueProcessor(s.engineImpl, s.WorkflowMgr, s.logger).(*timerQueueProcessorImpl)
	p.Start()

	tBuilder := newTimerBuilder(&localSeqNumGenerator{counter: 1}, s.logger)
	timerID := "tid1"
	t := s.addUserTimer(domainID, workflowExecution, timerID, tBuilder)
	p.NotifyNewTimer(t.GetTaskID())
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
	p := newTimerQueueProcessor(s.engineImpl, s.WorkflowMgr, s.logger).(*timerQueueProcessorImpl)
	p.Start()

	state, err := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.Nil(err)
	builder := newMutableStateBuilder(s.logger)
	builder.Load(state)
	condition := state.ExecutionInfo.NextEventID

	// create two user timers.
	_, ti := builder.AddTimerStartedEvent(emptyEventID,
		&workflow.StartTimerDecisionAttributes{TimerId: common.StringPtr("tid1"), StartToFireTimeoutSeconds: common.Int64Ptr(1)})
	_, ti2 := builder.AddTimerStartedEvent(emptyEventID,
		&workflow.StartTimerDecisionAttributes{TimerId: common.StringPtr("tid2"), StartToFireTimeoutSeconds: common.Int64Ptr(1)})

	tBuilder := newTimerBuilder(&localSeqNumGenerator{counter: 1}, s.logger)
	timerTasks := []persistence.Task{}
	t1 := tBuilder.AddUserTimer(ti, builder)
	if t1 != nil {
		timerTasks = append(timerTasks, t1)
	}
	t2 := tBuilder.AddUserTimer(ti2, builder)
	if t2 != nil {
		timerTasks = append(timerTasks, t2)
	}

	s.updateHistoryAndTimers(builder, timerTasks, condition)
	p.NotifyNewTimer(timerTasks[0].GetTaskID())

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

	p := newTimerQueueProcessor(s.engineImpl, s.WorkflowMgr, s.logger).(*timerQueueProcessorImpl)
	p.Start()

	tBuilder := newTimerBuilder(&localSeqNumGenerator{counter: 1}, s.logger)

	// Start of one of each timers each
	dt := s.addDecisionTimer(domainID, workflowExecution, tBuilder)
	s.addUserTimer(domainID, workflowExecution, "tid1", tBuilder)
	s.addHeartBeatTimer(domainID, workflowExecution, tBuilder)

	// GEt current state of workflow.
	state0, err := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.Nil(err)

	// close workflow
	s.closeWorkflow(domainID, workflowExecution)

	p.NotifyNewTimer(dt.GetTaskID())
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
	return string(history)
}
