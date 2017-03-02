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
)

type (
	timerQueueProcessorSuite struct {
		suite.Suite
		persistence.TestBase
		engineImpl       *historyEngineImpl
		mockShardManager *mocks.ShardManager
		shardClosedCh    chan int
		logger           bark.Logger
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
	resp, err := s.ShardMgr.GetShard(&persistence.GetShardRequest{ShardID: shardID})
	if err != nil {
		log.Fatal(err)
	}

	shard := &shardContextImpl{
		shardInfo:                 resp.ShardInfo,
		transferSequenceNumber:    1,
		executionManager:          s.WorkflowMgr,
		shardManager:              s.mockShardManager,
		rangeSize:                 defaultRangeSize,
		maxTransferSequenceNumber: 100000,
		closeCh:                   s.shardClosedCh,
		logger:                    s.logger,
	}
	txProcessor := newTransferQueueProcessor(shard, &mocks.MatchingClient{})
	tracker := newPendingTaskTracker(shard, txProcessor, s.logger)
	cache := newHistoryCache(shard, s.logger)
	cache.disabled = true
	s.engineImpl = &historyEngineImpl{
		shard:            shard,
		executionManager: s.WorkflowMgr,
		txProcessor:      txProcessor,
		cache:            cache,
		logger:           s.logger,
		tracker:          tracker,
		tokenSerializer:  common.NewJSONTaskTokenSerializer(),
	}
}

func (s *timerQueueProcessorSuite) TearDownSuite() {
	s.TearDownWorkflowStore()
}

func (s *timerQueueProcessorSuite) TearDownTest() {
	s.mockShardManager.AssertExpectations(s.T())
}

func (s *timerQueueProcessorSuite) getHistoryAndTimers(timeOuts []int32) ([]byte, []persistence.Task) {
	// Generate first decision task event.
	logger := bark.NewLoggerFromLogrus(log.New())
	tBuilder := newTimerBuilder(&localSeqNumGenerator{counter: 1}, logger)
	builder := newHistoryBuilder(logger)
	builder.AddWorkflowExecutionStartedEvent(&workflow.StartWorkflowExecutionRequest{})

	timerTasks := []persistence.Task{}
	builder.AddDecisionTaskScheduledEvent("taskList", 1)

	counter := int64(3)
	for _, timeOut := range timeOuts {
		timerTasks = append(timerTasks, tBuilder.createUserTimerTask(int64(timeOut), counter))
		builder.AddTimerStartedEvent(counter,
			&workflow.StartTimerDecisionAttributes{
				TimerId:                   common.StringPtr(uuid.New()),
				StartToFireTimeoutSeconds: common.Int64Ptr(int64(timeOut))})
		counter++
	}

	// Serialize the history
	h, serializedError := builder.Serialize()
	s.Nil(serializedError)
	return h, timerTasks
}

func (s *timerQueueProcessorSuite) TestSingleTimerTask() {
	workflowExecution := workflow.WorkflowExecution{WorkflowId: common.StringPtr("single-timer-test"),
		RunId:                                                    common.StringPtr("0d00698f-08e1-4d36-a3e2-3bf109f5d2d6")}

	taskList := "single-timer-queue"
	h, tt := s.getHistoryAndTimers([]int32{1})
	task0, err0 := s.CreateWorkflowExecution(workflowExecution, taskList, string(h), nil, 3, 0, 2, tt)
	s.Nil(err0, "No error expected.")
	s.NotEmpty(task0, "Expected non empty task identifier.")

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
	workflowExecution := workflow.WorkflowExecution{WorkflowId: common.StringPtr("multiple-timer-test"),
		RunId:                                                    common.StringPtr("0d00698f-08e1-4d36-a3e2-3bf109f5d2d6")}

	taskList := "multiple-timer-queue"
	h, tt := s.getHistoryAndTimers([]int32{1, 2, 3})
	task0, err0 := s.CreateWorkflowExecution(workflowExecution, taskList, string(h), nil, 3, 0, 2, tt)
	s.Nil(err0, "No error expected.")
	s.NotEmpty(task0, "Expected non empty task identifier.")

	timerInfo, err := s.GetTimerIndexTasks(int64(MinTimerKey), int64(MaxTimerKey))
	s.Nil(err, "No error expected.")
	s.NotEmpty(timerInfo, "Expected non empty timers list")
	s.Equal(3, len(timerInfo))

	processor := newTimerQueueProcessor(s.engineImpl, s.WorkflowMgr, s.logger).(*timerQueueProcessorImpl)
	processor.Start()

	for {
		timerInfo, err := s.GetTimerIndexTasks(int64(MinTimerKey), int64(MaxTimerKey))
		// fmt.Printf("TestManyTimerTasks: GetTimerIndexTasks: Response Count: %d \n", len(timerInfo))
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
	workflowExecution := workflow.WorkflowExecution{WorkflowId: common.StringPtr("After-timer-test"),
		RunId:                                                    common.StringPtr("0d00698f-08e1-4d36-a3e2-3bf109f5d2d6")}

	taskList := "After-timer-queue"

	tBuilder := newTimerBuilder(&localSeqNumGenerator{counter: 1}, s.logger)
	builder := newHistoryBuilder(s.logger)
	builder.AddWorkflowExecutionStartedEvent(&workflow.StartWorkflowExecutionRequest{
		TaskList:                       common.TaskListPtr(workflow.TaskList{Name: common.StringPtr(taskList)}),
		TaskStartToCloseTimeoutSeconds: common.Int32Ptr(1),
	})
	scheduledEvent := builder.AddDecisionTaskScheduledEvent(taskList, 1)
	builder.AddDecisionTaskStartedEvent(scheduledEvent.GetEventId(), uuid.New(),
		&workflow.PollForDecisionTaskRequest{Identity: common.StringPtr("test-ID")})
	h, serializedError := builder.Serialize()
	s.Nil(serializedError)

	task0, err0 := s.CreateWorkflowExecution(workflowExecution, taskList, string(h), nil, 3, 0, 2, nil)
	s.Nil(err0, "No error expected.")
	s.NotEmpty(task0, "Expected non empty task identifier.")

	timerInfo, err := s.GetTimerIndexTasks(int64(MinTimerKey), int64(MaxTimerKey))
	s.Nil(err, "No error expected.")
	s.Empty(timerInfo, "Expected empty timers list")

	processor := newTimerQueueProcessor(s.engineImpl, s.WorkflowMgr, s.logger).(*timerQueueProcessorImpl)
	processor.Start()

	timeOutTask := tBuilder.createDecisionTimeoutTask(1, scheduledEvent.GetEventId())
	timerTasks := []persistence.Task{timeOutTask}

	info, err1 := s.GetWorkflowExecutionInfo(workflowExecution)
	s.Nil(err1)
	err2 := s.UpdateWorkflowExecution(info, nil, nil, int64(3), timerTasks, nil, nil, nil, nil, nil)
	s.Nil(err2, "No error expected.")

	processor.NotifyNewTimer(timeOutTask.GetTaskID())

	for {
		timerInfo, err := s.GetTimerIndexTasks(int64(MinTimerKey), int64(MaxTimerKey))
		//fmt.Printf("TestAfterTimerTasks: GetTimerIndexTasks: Response Count: %d \n", len(timerInfo))
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

func (s *timerQueueProcessorSuite) checkTimedOutEventFor(workflowExecution workflow.WorkflowExecution,
	scheduleID int64) (bool, bool, *historyBuilder) {
	info, err1 := s.GetWorkflowExecutionInfo(workflowExecution)
	s.Nil(err1)
	builder := newHistoryBuilder(s.logger)
	builder.loadExecutionInfo(info)
	isRunning, _ := builder.isActivityTaskRunning(scheduleID)

	minfo, err1 := s.GetWorkflowMutableState(workflowExecution)
	s.Nil(err1)
	msBuilder := newMutableStateBuilder(s.logger)
	msBuilder.Load(minfo.ActivitInfos, minfo.TimerInfos)
	isRunningFromMutableState, _ := msBuilder.isActivityRunning(scheduleID)

	return isRunning, isRunningFromMutableState, builder
}

func (s *timerQueueProcessorSuite) checkTimedOutEventForUserTimer(workflowExecution workflow.WorkflowExecution,
	startedID int64) (bool, *historyBuilder) {
	info, err1 := s.GetWorkflowExecutionInfo(workflowExecution)
	s.Nil(err1)
	builder := newHistoryBuilder(s.logger)
	builder.loadExecutionInfo(info)
	startedEvent := builder.GetEvent(startedID)

	minfo, err1 := s.GetWorkflowMutableState(workflowExecution)
	s.Nil(err1)
	msBuilder := newMutableStateBuilder(s.logger)
	msBuilder.Load(minfo.ActivitInfos, minfo.TimerInfos)
	isRunning, _ := msBuilder.isTimerRunning(startedEvent.GetTimerStartedEventAttributes().GetTimerId())
	return isRunning, builder
}

func (s *timerQueueProcessorSuite) updateHistoryAndTimers(workflowExecution workflow.WorkflowExecution, history []byte,
	timerTasks []persistence.Task, activityInfos []*persistence.ActivityInfo, timerInfos []*persistence.TimerInfo) {
	info, err1 := s.GetWorkflowExecutionInfo(workflowExecution)
	s.Nil(err1)
	info.History = history
	err2 := s.UpdateWorkflowExecution(info, nil, nil, info.NextEventID, timerTasks, nil, activityInfos, nil, timerInfos, nil)
	s.Nil(err2, "No error expected.")
}

func (s *timerQueueProcessorSuite) TestTimerActivityTask() {
	workflowExecution := workflow.WorkflowExecution{WorkflowId: common.StringPtr("activity-timer-test"),
		RunId:                                                    common.StringPtr("0d00698f-08e1-4d36-a3e2-3bf109f5d2d6")}

	taskList := "activity-timer-queue"
	tBuilder := newTimerBuilder(&localSeqNumGenerator{counter: 1}, s.logger)
	builder := newHistoryBuilder(s.logger)
	builder.AddWorkflowExecutionStartedEvent(&workflow.StartWorkflowExecutionRequest{
		TaskList:                       common.TaskListPtr(workflow.TaskList{Name: common.StringPtr(taskList)}),
		TaskStartToCloseTimeoutSeconds: common.Int32Ptr(1),
	})
	scheduledEvent := builder.AddDecisionTaskScheduledEvent(taskList, 1)
	decisionTaskStartEvent := builder.AddDecisionTaskStartedEvent(scheduledEvent.GetEventId(), uuid.New(),
		&workflow.PollForDecisionTaskRequest{Identity: common.StringPtr("test-ID")})
	h, serializedError := builder.Serialize()
	s.Nil(serializedError)

	task0, err0 := s.CreateWorkflowExecution(workflowExecution, taskList, string(h), nil, 3, 0, 2, nil)
	s.Nil(err0, "No error expected.")
	s.NotEmpty(task0, "Expected non empty task identifier.")

	// TimeoutType_SCHEDULE_TO_START - Without Start
	processor := newTimerQueueProcessor(s.engineImpl, s.WorkflowMgr, s.logger).(*timerQueueProcessorImpl)
	processor.Start()

	activityScheduled := builder.AddActivityTaskScheduledEvent(decisionTaskStartEvent.GetEventId(),
		&workflow.ScheduleActivityTaskDecisionAttributes{
			ScheduleToStartTimeoutSeconds: common.Int32Ptr(1),
		})
	history, err := builder.Serialize()
	s.Nil(err)

	msBuilder := newMutableStateBuilder(s.logger)
	t := tBuilder.AddScheduleToStartActivityTimeout(activityScheduled.GetEventId(), activityScheduled, msBuilder)
	s.NotNil(t)
	timerTasks := []persistence.Task{t}

	s.updateHistoryAndTimers(workflowExecution, history, timerTasks, msBuilder.updateActivityInfos, nil)
	processor.NotifyNewTimer(t.GetTaskID())

	s.waitForTimerTasksToProcess(processor)
	s.Equal(uint64(1), processor.timerFiredCount)
	running, isRunningFromMS, b := s.checkTimedOutEventFor(workflowExecution, activityScheduled.GetEventId())
	if running {
		common.PrettyPrintHistory(b.getHistory(), s.logger)
	}
	s.False(running)
	s.False(isRunningFromMS)

	// TimeoutType_SCHEDULE_TO_START - With Start
	p := newTimerQueueProcessor(s.engineImpl, s.WorkflowMgr, s.logger).(*timerQueueProcessorImpl)
	p.Start()

	ase := b.AddActivityTaskScheduledEvent(decisionTaskStartEvent.GetEventId(),
		&workflow.ScheduleActivityTaskDecisionAttributes{
			ScheduleToStartTimeoutSeconds: common.Int32Ptr(1),
		})
	aste := b.AddActivityTaskStartedEvent(ase.GetEventId(), uuid.New(), &workflow.PollForActivityTaskRequest{})
	history, err = b.Serialize()
	s.Nil(err)
	s.logger.Infof("Added Schedule Activity ID: %v, Start Activity ID: %v", ase.GetEventId(), aste.GetEventId())
	common.PrettyPrintHistory(b.getHistory(), s.logger)

	msBuilder = newMutableStateBuilder(s.logger)
	t = tBuilder.AddScheduleToStartActivityTimeout(ase.GetEventId(), ase, msBuilder)
	s.NotNil(t)
	timerTasks = []persistence.Task{t}
	msBuilder.updateActivityInfos[0].StartedID = aste.GetEventId()

	s.updateHistoryAndTimers(workflowExecution, history, timerTasks, msBuilder.updateActivityInfos, nil)
	p.NotifyNewTimer(t.GetTaskID())

	s.waitForTimerTasksToProcess(p)
	s.Equal(uint64(1), p.timerFiredCount)
	running, isRunningFromMS, b = s.checkTimedOutEventFor(workflowExecution, ase.GetEventId())
	s.logger.Infof("HERE!!!! Running: %v, TimerID: %v", running, t.GetTaskID())
	if !running {
		s.logger.Info("Printing History: ")
		common.PrettyPrintHistory(b.getHistory(), s.logger)
	}
	s.True(running)
	s.True(isRunningFromMS)

	// TimeoutType_START_TO_CLOSE - Just start.
	p = newTimerQueueProcessor(s.engineImpl, s.WorkflowMgr, s.logger).(*timerQueueProcessorImpl)
	p.Start()

	ase = b.AddActivityTaskScheduledEvent(decisionTaskStartEvent.GetEventId(),
		&workflow.ScheduleActivityTaskDecisionAttributes{
			StartToCloseTimeoutSeconds: common.Int32Ptr(1),
		})
	aste = b.AddActivityTaskStartedEvent(ase.GetEventId(), uuid.New(), &workflow.PollForActivityTaskRequest{})

	msBuilder = newMutableStateBuilder(s.logger)
	msBuilder.UpdatePendingActivity(ase.GetEventId(), &persistence.ActivityInfo{
		ScheduleID: ase.GetEventId(), StartedID: aste.GetEventId(), StartToCloseTimeout: 1})
	t, err = tBuilder.AddStartToCloseActivityTimeout(ase.GetEventId(), msBuilder)
	s.Nil(err)
	s.NotNil(t)
	timerTasks = []persistence.Task{t}

	history, err = b.Serialize()
	s.Nil(err)

	s.updateHistoryAndTimers(workflowExecution, history, timerTasks, msBuilder.updateActivityInfos, nil)
	p.NotifyNewTimer(t.GetTaskID())

	s.waitForTimerTasksToProcess(p)
	s.Equal(uint64(1), p.timerFiredCount)
	running, isRunningFromMS, b = s.checkTimedOutEventFor(workflowExecution, ase.GetEventId())
	s.False(running)
	s.False(isRunningFromMS)

	// TimeoutType_START_TO_CLOSE - Start and Completed activity.
	p = newTimerQueueProcessor(s.engineImpl, s.WorkflowMgr, s.logger).(*timerQueueProcessorImpl)
	p.Start()

	ase = b.AddActivityTaskScheduledEvent(decisionTaskStartEvent.GetEventId(),
		&workflow.ScheduleActivityTaskDecisionAttributes{
			StartToCloseTimeoutSeconds: common.Int32Ptr(1),
		})
	aste = b.AddActivityTaskStartedEvent(ase.GetEventId(), uuid.New(), &workflow.PollForActivityTaskRequest{})

	msBuilder = newMutableStateBuilder(s.logger)
	msBuilder.UpdatePendingActivity(ase.GetEventId(), &persistence.ActivityInfo{StartToCloseTimeout: 1})
	t, err = tBuilder.AddStartToCloseActivityTimeout(ase.GetEventId(), msBuilder)
	s.Nil(err)
	s.NotNil(t)
	timerTasks = []persistence.Task{t}

	b.AddActivityTaskCompletedEvent(ase.GetEventId(), aste.GetEventId(), &workflow.RespondActivityTaskCompletedRequest{
		Identity: common.StringPtr("test-id"),
		Result_:  []byte("result"),
	})

	history, err = b.Serialize()
	s.Nil(err)

	s.updateHistoryAndTimers(workflowExecution, history, timerTasks, nil /* since activity is completed */ , nil)
	p.NotifyNewTimer(t.GetTaskID())

	s.waitForTimerTasksToProcess(p)
	s.Equal(uint64(1), p.timerFiredCount)
	running, isRunningFromMS, b = s.checkTimedOutEventFor(workflowExecution, ase.GetEventId())
	s.False(running)
	s.False(isRunningFromMS)

	// TimeoutType_SCHEDULE_TO_CLOSE - Just Scheduled.
	p = newTimerQueueProcessor(s.engineImpl, s.WorkflowMgr, s.logger).(*timerQueueProcessorImpl)
	p.Start()

	ase = b.AddActivityTaskScheduledEvent(decisionTaskStartEvent.GetEventId(),
		&workflow.ScheduleActivityTaskDecisionAttributes{
			ScheduleToCloseTimeoutSeconds: common.Int32Ptr(1),
		})

	msBuilder = newMutableStateBuilder(s.logger)
	msBuilder.UpdatePendingActivity(ase.GetEventId(), &persistence.ActivityInfo{
		ScheduleID: ase.GetEventId(), StartedID: emptyEventID, ScheduleToCloseTimeout: 1})
	t, err = tBuilder.AddScheduleToCloseActivityTimeout(ase.GetEventId(), msBuilder)
	s.Nil(err)
	s.NotNil(t)
	timerTasks = []persistence.Task{t}

	history, err = b.Serialize()
	s.Nil(err)

	s.updateHistoryAndTimers(workflowExecution, history, timerTasks, msBuilder.updateActivityInfos, nil)
	p.NotifyNewTimer(t.GetTaskID())

	s.waitForTimerTasksToProcess(p)
	s.Equal(uint64(1), p.timerFiredCount)
	running, isRunningFromMS, b = s.checkTimedOutEventFor(workflowExecution, ase.GetEventId())
	s.False(running)
	s.False(isRunningFromMS)

	// TimeoutType_SCHEDULE_TO_CLOSE - Scheduled and started.
	p = newTimerQueueProcessor(s.engineImpl, s.WorkflowMgr, s.logger).(*timerQueueProcessorImpl)
	p.Start()

	ase = b.AddActivityTaskScheduledEvent(decisionTaskStartEvent.GetEventId(),
		&workflow.ScheduleActivityTaskDecisionAttributes{
			ScheduleToCloseTimeoutSeconds: common.Int32Ptr(1),
		})
	aste = b.AddActivityTaskStartedEvent(ase.GetEventId(), uuid.New(), &workflow.PollForActivityTaskRequest{})

	msBuilder = newMutableStateBuilder(s.logger)
	msBuilder.UpdatePendingActivity(ase.GetEventId(), &persistence.ActivityInfo{
		ScheduleID: ase.GetEventId(), StartedID: aste.GetEventId(), ScheduleToCloseTimeout: 1})
	t, err = tBuilder.AddScheduleToCloseActivityTimeout(ase.GetEventId(), msBuilder)
	s.Nil(err)
	s.NotNil(t)
	timerTasks = []persistence.Task{t}

	history, err = b.Serialize()
	s.Nil(err)

	s.updateHistoryAndTimers(workflowExecution, history, timerTasks, msBuilder.updateActivityInfos, nil)
	p.NotifyNewTimer(t.GetTaskID())

	s.waitForTimerTasksToProcess(p)
	s.Equal(uint64(1), p.timerFiredCount)
	running, isRunningFromMS, b = s.checkTimedOutEventFor(workflowExecution, ase.GetEventId())
	s.False(running)
	s.False(isRunningFromMS)

	// TimeoutType_SCHEDULE_TO_CLOSE - Scheduled, started, completed.
	p = newTimerQueueProcessor(s.engineImpl, s.WorkflowMgr, s.logger).(*timerQueueProcessorImpl)
	p.Start()

	ase = b.AddActivityTaskScheduledEvent(decisionTaskStartEvent.GetEventId(),
		&workflow.ScheduleActivityTaskDecisionAttributes{
			ScheduleToCloseTimeoutSeconds: common.Int32Ptr(1),
		})
	aste = b.AddActivityTaskStartedEvent(ase.GetEventId(), uuid.New(), &workflow.PollForActivityTaskRequest{})

	msBuilder = newMutableStateBuilder(s.logger)
	msBuilder.UpdatePendingActivity(ase.GetEventId(), &persistence.ActivityInfo{ScheduleToCloseTimeout: 1})
	t, err = tBuilder.AddScheduleToCloseActivityTimeout(ase.GetEventId(), msBuilder)
	s.Nil(err)
	s.NotNil(t)
	timerTasks = []persistence.Task{t}

	b.AddActivityTaskCompletedEvent(ase.GetEventId(), aste.GetEventId(), &workflow.RespondActivityTaskCompletedRequest{
		Identity: common.StringPtr("test-id"),
		Result_:  []byte("result"),
	})

	history, err = b.Serialize()
	s.Nil(err)

	s.updateHistoryAndTimers(workflowExecution, history, timerTasks, nil /* since it is completed */ , nil)
	p.NotifyNewTimer(t.GetTaskID())

	s.waitForTimerTasksToProcess(p)
	s.Equal(uint64(1), p.timerFiredCount)
	running, isRunningFromMS, b = s.checkTimedOutEventFor(workflowExecution, ase.GetEventId())
	s.False(running)
	s.False(isRunningFromMS)

	// TimeoutType_HEARTBEAT - Scheduled, started.
	p = newTimerQueueProcessor(s.engineImpl, s.WorkflowMgr, s.logger).(*timerQueueProcessorImpl)
	p.Start()

	ase = b.AddActivityTaskScheduledEvent(decisionTaskStartEvent.GetEventId(),
		&workflow.ScheduleActivityTaskDecisionAttributes{
			HeartbeatTimeoutSeconds: common.Int32Ptr(1),
		})
	aste = b.AddActivityTaskStartedEvent(ase.GetEventId(), uuid.New(), &workflow.PollForActivityTaskRequest{})

	msBuilder = newMutableStateBuilder(s.logger)
	msBuilder.UpdatePendingActivity(ase.GetEventId(), &persistence.ActivityInfo{
		ScheduleID: ase.GetEventId(), StartedID: aste.GetEventId(), HeartbeatTimeout: 1})

	t, err = tBuilder.AddHeartBeatActivityTimeout(ase.GetEventId(), msBuilder)
	s.Nil(err)
	s.NotNil(t)
	timerTasks = []persistence.Task{t}

	history, err = b.Serialize()
	s.Nil(err)

	s.updateHistoryAndTimers(workflowExecution, history, timerTasks, msBuilder.updateActivityInfos, nil)
	p.NotifyNewTimer(t.GetTaskID())

	s.waitForTimerTasksToProcess(p)
	s.Equal(uint64(1), p.timerFiredCount)
	running, isRunningFromMS, b = s.checkTimedOutEventFor(workflowExecution, ase.GetEventId())
	s.False(running)
	s.False(isRunningFromMS)
}

func (s *timerQueueProcessorSuite) TestTimerUserTimers() {
	workflowExecution := workflow.WorkflowExecution{WorkflowId: common.StringPtr("user-timer-test"),
		RunId:                                                    common.StringPtr("0d00698f-08e1-4d36-a3e2-3bf109f5d2d6")}

	taskList := "user-timer-queue"
	tBuilder := newTimerBuilder(&localSeqNumGenerator{counter: 1}, s.logger)
	builder := newHistoryBuilder(s.logger)
	builder.AddWorkflowExecutionStartedEvent(&workflow.StartWorkflowExecutionRequest{
		TaskList:                       common.TaskListPtr(workflow.TaskList{Name: common.StringPtr(taskList)}),
		TaskStartToCloseTimeoutSeconds: common.Int32Ptr(1),
	})
	scheduledEvent := builder.AddDecisionTaskScheduledEvent(taskList, 1)
	decisionTaskStartEvent := builder.AddDecisionTaskStartedEvent(scheduledEvent.GetEventId(), uuid.New(),
		&workflow.PollForDecisionTaskRequest{Identity: common.StringPtr("test-ID")})
	h, serializedError := builder.Serialize()
	s.Nil(serializedError)

	task0, err0 := s.CreateWorkflowExecution(workflowExecution, taskList, string(h), nil, 3, 0, 2, nil)
	s.Nil(err0, "No error expected.")
	s.NotEmpty(task0, "Expected non empty task identifier.")

	// Single timer.
	processor := newTimerQueueProcessor(s.engineImpl, s.WorkflowMgr, s.logger).(*timerQueueProcessorImpl)
	processor.Start()

	msBuilder := newMutableStateBuilder(s.logger)
	startTimerEvent := builder.AddTimerStartedEvent(decisionTaskStartEvent.GetEventId(),
		&workflow.StartTimerDecisionAttributes{TimerId: common.StringPtr("tid1"), StartToFireTimeoutSeconds: common.Int64Ptr(1)})
	t1, err := tBuilder.AddUserTimer("tid1", 1, startTimerEvent.GetEventId(), msBuilder)
	s.Nil(err)

	history, err := builder.Serialize()
	s.Nil(err)

	timerTasks := []persistence.Task{t1}

	s.updateHistoryAndTimers(workflowExecution, history, timerTasks, nil, msBuilder.updateTimerInfos)
	processor.NotifyNewTimer(t1.GetTaskID())

	s.waitForTimerTasksToProcess(processor)
	s.Equal(uint64(1), processor.timerFiredCount)
	running, _ := s.checkTimedOutEventForUserTimer(workflowExecution, startTimerEvent.GetEventId())
	s.False(running)
}

func (s *timerQueueProcessorSuite) TestTimerUserTimersSameExpiry() {
	workflowExecution := workflow.WorkflowExecution{WorkflowId: common.StringPtr("user-timer-same-expiry-test"),
		RunId:                                                    common.StringPtr("0d00698f-08e1-4d36-a3e2-3bf109f5d2d6")}

	taskList := "user-timer-same-expiry-queue"
	tBuilder := newTimerBuilder(&localSeqNumGenerator{counter: 1}, s.logger)
	builder := newHistoryBuilder(s.logger)
	builder.AddWorkflowExecutionStartedEvent(&workflow.StartWorkflowExecutionRequest{
		TaskList:                       common.TaskListPtr(workflow.TaskList{Name: common.StringPtr(taskList)}),
		TaskStartToCloseTimeoutSeconds: common.Int32Ptr(1),
	})
	scheduledEvent := builder.AddDecisionTaskScheduledEvent(taskList, 1)
	decisionTaskStartEvent := builder.AddDecisionTaskStartedEvent(scheduledEvent.GetEventId(), uuid.New(),
		&workflow.PollForDecisionTaskRequest{Identity: common.StringPtr("test-ID")})
	h, serializedError := builder.Serialize()
	s.Nil(serializedError)

	task0, err0 := s.CreateWorkflowExecution(workflowExecution, taskList, string(h), nil, 3, 0, 2, nil)
	s.Nil(err0, "No error expected.")
	s.NotEmpty(task0, "Expected non empty task identifier.")

	// Two timers.
	processor := newTimerQueueProcessor(s.engineImpl, s.WorkflowMgr, s.logger).(*timerQueueProcessorImpl)
	processor.Start()

	msBuilder := newMutableStateBuilder(s.logger)
	startTimerEvent1 := builder.AddTimerStartedEvent(decisionTaskStartEvent.GetEventId(),
		&workflow.StartTimerDecisionAttributes{TimerId: common.StringPtr("tid1"), StartToFireTimeoutSeconds: common.Int64Ptr(1)})
	startTimerEvent2 := builder.AddTimerStartedEvent(decisionTaskStartEvent.GetEventId(),
		&workflow.StartTimerDecisionAttributes{TimerId: common.StringPtr("tid2"), StartToFireTimeoutSeconds: common.Int64Ptr(1)})

	t1, err := tBuilder.AddUserTimer("tid1", 1, startTimerEvent1.GetEventId(), msBuilder)
	s.Nil(err)

	msBuilder = newMutableStateBuilder(s.logger)
	t2, err := tBuilder.AddUserTimer("tid2", 1, startTimerEvent2.GetEventId(), msBuilder)
	s.Nil(err)

	history, err := builder.Serialize()
	s.Nil(err)

	timerTasks := []persistence.Task{t2}

	s.updateHistoryAndTimers(workflowExecution, history, timerTasks, nil, msBuilder.updateTimerInfos)
	processor.NotifyNewTimer(t1.GetTaskID())

	s.waitForTimerTasksToProcess(processor)
	s.Equal(uint64(1), processor.timerFiredCount)
	running, _ := s.checkTimedOutEventForUserTimer(workflowExecution, startTimerEvent1.GetEventId())
	s.False(running)
	running, _ = s.checkTimedOutEventForUserTimer(workflowExecution, startTimerEvent2.GetEventId())
	s.False(running)
}
