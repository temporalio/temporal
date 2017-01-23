package history

import (
	"os"
	"testing"
	"time"

	"code.uber.internal/devexp/minions/client/matching/mocks"
	"code.uber.internal/devexp/minions/common"
	"code.uber.internal/devexp/minions/common/persistence"

	workflow "code.uber.internal/devexp/minions/.gen/go/shared"
	log "github.com/Sirupsen/logrus"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
	"github.com/uber-common/bark"
)

type (
	timerQueueProcessorSuite struct {
		suite.Suite
		persistence.TestBase
		engineImpl *historyEngineImpl
		logger     bark.Logger
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
	//log2.Level = log.DebugLevel
	s.logger = bark.NewLoggerFromLogrus(log2)

	resp, err := s.ShardMgr.GetShard(&persistence.GetShardRequest{ShardID: 1})
	if err != nil {
		log.Fatal(err)
	}

	shard := &shardContextImpl{shardInfo: resp.ShardInfo}
	txProcessor := newTransferQueueProcessor(shard, s.WorkflowMgr, &mocks.Client{}, s.logger)
	tracker := newPendingTaskTracker(shard, txProcessor, s.logger)
	s.engineImpl = &historyEngineImpl{
		shard:            shard,
		executionManager: s.WorkflowMgr,
		txProcessor:      txProcessor,
		logger:           s.logger,
		tracker:          tracker,
		tokenSerializer:  common.NewJSONTaskTokenSerializer(),
	}
}

func (s *timerQueueProcessorSuite) TearDownSuite() {
	s.TearDownWorkflowStore()
}

func (s *timerQueueProcessorSuite) getHistoryAndTimers(timeOuts []int32) ([]byte, []persistence.Task) {
	// Generate first decision task event.
	logger := bark.NewLoggerFromLogrus(log.New())
	tBuilder := newTimerBuilder(&localSeqNumGenerator{counter: 1}, logger)
	builder := newHistoryBuilder(tBuilder, logger)
	builder.AddWorkflowExecutionStartedEvent(&workflow.StartWorkflowExecutionRequest{})

	timerTasks := []persistence.Task{}
	builder.AddDecisionTaskScheduledEvent("taskList", 1)

	counter := int64(3)
	for _, timeOut := range timeOuts {
		timerTasks = append(timerTasks, tBuilder.CreateUserTimerTask(timeOut, "tid", counter))
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
		RunId: common.StringPtr("0d00698f-08e1-4d36-a3e2-3bf109f5d2d6")}

	taskList := "single-timer-queue"
	h, tt := s.getHistoryAndTimers([]int32{1})
	task0, err0 := s.CreateWorkflowExecution(workflowExecution, taskList, string(h), nil, 3, 0, 2, tt)
	s.Nil(err0, "No error expected.")
	s.NotEmpty(task0, "Expected non empty task identifier.")

	timerInfo, err := s.GetTimerIndexTasks(MinTimerKey, MaxTimerKey)
	s.Nil(err, "No error expected.")
	s.NotEmpty(timerInfo, "Expected non empty timers list")
	s.Equal(1, len(timerInfo))

	processor := newTimerQueueProcessor(s.engineImpl, s.WorkflowMgr, s.logger).(*timerQueueProcessorImpl)
	processor.Start()

	for {
		timerInfo, err := s.GetTimerIndexTasks(MinTimerKey, MaxTimerKey)
		s.Nil(err, "No error expected.")
		if len(timerInfo) == 0 {
			processor.Stop()
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	timerInfo, err = s.GetTimerIndexTasks(MinTimerKey, MaxTimerKey)
	s.Nil(err, "No error expected.")
	s.Equal(0, len(timerInfo))
}

func (s *timerQueueProcessorSuite) TestManyTimerTasks() {
	workflowExecution := workflow.WorkflowExecution{WorkflowId: common.StringPtr("multiple-timer-test"),
		RunId: common.StringPtr("0d00698f-08e1-4d36-a3e2-3bf109f5d2d6")}

	taskList := "multiple-timer-queue"
	h, tt := s.getHistoryAndTimers([]int32{1, 2, 3})
	task0, err0 := s.CreateWorkflowExecution(workflowExecution, taskList, string(h), nil, 3, 0, 2, tt[0:1])
	s.Nil(err0, "No error expected.")
	s.NotEmpty(task0, "Expected non empty task identifier.")

	timerInfo, err := s.GetTimerIndexTasks(MinTimerKey, MaxTimerKey)
	s.Nil(err, "No error expected.")
	s.NotEmpty(timerInfo, "Expected non empty timers list")
	s.Equal(1, len(timerInfo))

	processor := newTimerQueueProcessor(s.engineImpl, s.WorkflowMgr, s.logger).(*timerQueueProcessorImpl)
	processor.Start()

	for {
		timerInfo, err := s.GetTimerIndexTasks(MinTimerKey, MaxTimerKey)
		// fmt.Printf("TestManyTimerTasks: GetTimerIndexTasks: Response Count: %d \n", len(timerInfo))
		s.Nil(err, "No error expected.")
		if len(timerInfo) == 0 {
			processor.Stop()
			break
		}
		time.Sleep(1000 * time.Millisecond)
	}

	timerInfo, err = s.GetTimerIndexTasks(MinTimerKey, MaxTimerKey)
	s.Nil(err, "No error expected.")
	s.Equal(0, len(timerInfo))

	s.Equal(uint64(3), processor.timerFiredCount)
}

func (s *timerQueueProcessorSuite) TestTimerTaskAfterProcessorStart() {
	workflowExecution := workflow.WorkflowExecution{WorkflowId: common.StringPtr("After-timer-test"),
		RunId: common.StringPtr("0d00698f-08e1-4d36-a3e2-3bf109f5d2d6")}

	taskList := "After-timer-queue"

	tBuilder := newTimerBuilder(&localSeqNumGenerator{counter: 1}, s.logger)
	builder := newHistoryBuilder(tBuilder, s.logger)
	builder.AddWorkflowExecutionStartedEvent(&workflow.StartWorkflowExecutionRequest{
		TaskList:                       common.TaskListPtr(workflow.TaskList{Name: common.StringPtr(taskList)}),
		TaskStartToCloseTimeoutSeconds: common.Int32Ptr(1),
	})
	scheduledEvent := builder.AddDecisionTaskScheduledEvent(taskList, 1)
	builder.AddDecisionTaskStartedEvent(
		scheduledEvent.GetEventId(), &workflow.PollForDecisionTaskRequest{Identity: common.StringPtr("test-ID")})
	h, serializedError := builder.Serialize()
	s.Nil(serializedError)

	task0, err0 := s.CreateWorkflowExecution(workflowExecution, taskList, string(h), nil, 3, 0, 2, nil)
	s.Nil(err0, "No error expected.")
	s.NotEmpty(task0, "Expected non empty task identifier.")

	timerInfo, err := s.GetTimerIndexTasks(MinTimerKey, MaxTimerKey)
	s.Nil(err, "No error expected.")
	s.Empty(timerInfo, "Expected empty timers list")

	processor := newTimerQueueProcessor(s.engineImpl, s.WorkflowMgr, s.logger).(*timerQueueProcessorImpl)
	processor.Start()

	timeOutTask := tBuilder.createDecisionTimeoutTask(1, scheduledEvent.GetEventId())
	timerTasks := []persistence.Task{timeOutTask}

	info, err1 := s.GetWorkflowExecutionInfo(workflowExecution)
	s.Nil(err1)
	err2 := s.UpdateWorkflowExecution(info, nil, nil, int64(3), timerTasks, nil, nil, nil)
	s.Nil(err2, "No error expected.")

	processor.NotifyNewTimer()

	for {
		timerInfo, err := s.GetTimerIndexTasks(MinTimerKey, MaxTimerKey)
		//fmt.Printf("TestAfterTimerTasks: GetTimerIndexTasks: Response Count: %d \n", len(timerInfo))
		s.Nil(err, "No error expected.")
		if len(timerInfo) == 0 {
			processor.Stop()
			break
		}
		time.Sleep(1000 * time.Millisecond)
	}

	timerInfo, err = s.GetTimerIndexTasks(MinTimerKey, MaxTimerKey)
	s.Nil(err, "No error expected.")
	s.Equal(0, len(timerInfo))

	s.Equal(uint64(1), processor.timerFiredCount)
}

func (s *timerQueueProcessorSuite) waitForTimerTasksToProcess(p timerQueueProcessor) {
	for {
		timerInfo, err := s.GetTimerIndexTasks(MinTimerKey, MaxTimerKey)
		//fmt.Printf("TestAfterTimerTasks: GetTimerIndexTasks: Response Count: %d \n", len(timerInfo))
		s.Nil(err, "No error expected.")
		if len(timerInfo) == 0 {
			p.Stop()
			break
		}
		time.Sleep(1000 * time.Millisecond)
	}
}

func (s *timerQueueProcessorSuite) checkTimedOutEventFor(workflowExecution workflow.WorkflowExecution, scheduleID int64) (bool, *historyBuilder) {
	info, err1 := s.GetWorkflowExecutionInfo(workflowExecution)
	s.Nil(err1)
	tBuilder := newTimerBuilder(&localSeqNumGenerator{counter: 1}, s.logger)
	builder := newHistoryBuilder(tBuilder, s.logger)
	builder.loadExecutionInfo(info)
	isRunning, _ := builder.isActivityTaskRunning(scheduleID)
	return isRunning, builder
}

func (s *timerQueueProcessorSuite) updateHistoryAndTimers(workflowExecution workflow.WorkflowExecution,
	history []byte, timerTasks []persistence.Task, activityInfos []*persistence.ActivityInfo) {
	info, err1 := s.GetWorkflowExecutionInfo(workflowExecution)
	s.Nil(err1)
	info.History = history
	err2 := s.UpdateWorkflowExecution(info, nil, nil, info.NextEventID, timerTasks, nil, activityInfos, nil)
	s.Nil(err2, "No error expected.")
}

func (s *timerQueueProcessorSuite) TestTimerActivityTask() {
	workflowExecution := workflow.WorkflowExecution{WorkflowId: common.StringPtr("activity-timer-test"),
		RunId: common.StringPtr("0d00698f-08e1-4d36-a3e2-3bf109f5d2d6")}

	taskList := "activity-timer-queue"
	tBuilder := newTimerBuilder(&localSeqNumGenerator{counter: 1}, s.logger)
	builder := newHistoryBuilder(tBuilder, s.logger)
	builder.AddWorkflowExecutionStartedEvent(&workflow.StartWorkflowExecutionRequest{
		TaskList:                       common.TaskListPtr(workflow.TaskList{Name: common.StringPtr(taskList)}),
		TaskStartToCloseTimeoutSeconds: common.Int32Ptr(1),
	})
	scheduledEvent := builder.AddDecisionTaskScheduledEvent(taskList, 1)
	decisionTaskStartEvent := builder.AddDecisionTaskStartedEvent(
		scheduledEvent.GetEventId(), &workflow.PollForDecisionTaskRequest{Identity: common.StringPtr("test-ID")})
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

	s.updateHistoryAndTimers(workflowExecution, history, timerTasks, nil)
	processor.NotifyNewTimer()

	s.waitForTimerTasksToProcess(processor)
	s.Equal(uint64(1), processor.timerFiredCount)
	running, b := s.checkTimedOutEventFor(workflowExecution, activityScheduled.GetEventId())
	s.False(running)

	// TimeoutType_SCHEDULE_TO_START - With Start
	p := newTimerQueueProcessor(s.engineImpl, s.WorkflowMgr, s.logger).(*timerQueueProcessorImpl)
	p.Start()

	ase := b.AddActivityTaskScheduledEvent(decisionTaskStartEvent.GetEventId(),
		&workflow.ScheduleActivityTaskDecisionAttributes{
			ScheduleToStartTimeoutSeconds: common.Int32Ptr(1),
		})
	b.AddActivityTaskStartedEvent(ase.GetEventId(), &workflow.PollForActivityTaskRequest{})
	history, err = b.Serialize()
	s.Nil(err)

	msBuilder = newMutableStateBuilder(s.logger)
	t = tBuilder.AddScheduleToStartActivityTimeout(ase.GetEventId(), ase, msBuilder)
	s.NotNil(t)
	timerTasks = []persistence.Task{t}

	s.updateHistoryAndTimers(workflowExecution, history, timerTasks, nil)
	p.NotifyNewTimer()

	s.waitForTimerTasksToProcess(p)
	s.Equal(uint64(1), p.timerFiredCount)
	running, b = s.checkTimedOutEventFor(workflowExecution, ase.GetEventId())
	s.True(running)

	// TimeoutType_START_TO_CLOSE - Just start.
	p = newTimerQueueProcessor(s.engineImpl, s.WorkflowMgr, s.logger).(*timerQueueProcessorImpl)
	p.Start()

	ase = b.AddActivityTaskScheduledEvent(decisionTaskStartEvent.GetEventId(),
		&workflow.ScheduleActivityTaskDecisionAttributes{
			StartToCloseTimeoutSeconds: common.Int32Ptr(1),
		})
	b.AddActivityTaskStartedEvent(ase.GetEventId(), &workflow.PollForActivityTaskRequest{})

	msBuilder = newMutableStateBuilder(s.logger)
	msBuilder.UpdatePendingActivity(ase.GetEventId(), &persistence.ActivityInfo{StartToCloseTimeout: 1})
	t, err = tBuilder.AddStartToCloseActivityTimeout(ase.GetEventId(), msBuilder)
	s.Nil(err)
	s.NotNil(t)
	timerTasks = []persistence.Task{t}

	history, err = b.Serialize()
	s.Nil(err)

	s.updateHistoryAndTimers(workflowExecution, history, timerTasks, nil)
	p.NotifyNewTimer()

	s.waitForTimerTasksToProcess(p)
	s.Equal(uint64(1), p.timerFiredCount)
	running, b = s.checkTimedOutEventFor(workflowExecution, ase.GetEventId())
	s.False(running)

	// TimeoutType_START_TO_CLOSE - Start and Completed activity.
	p = newTimerQueueProcessor(s.engineImpl, s.WorkflowMgr, s.logger).(*timerQueueProcessorImpl)
	p.Start()

	ase = b.AddActivityTaskScheduledEvent(decisionTaskStartEvent.GetEventId(),
		&workflow.ScheduleActivityTaskDecisionAttributes{
			StartToCloseTimeoutSeconds: common.Int32Ptr(1),
		})
	aste := b.AddActivityTaskStartedEvent(ase.GetEventId(), &workflow.PollForActivityTaskRequest{})

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

	s.updateHistoryAndTimers(workflowExecution, history, timerTasks, nil)
	p.NotifyNewTimer()

	s.waitForTimerTasksToProcess(p)
	s.Equal(uint64(1), p.timerFiredCount)
	running, b = s.checkTimedOutEventFor(workflowExecution, ase.GetEventId())
	s.False(running)

	// TimeoutType_SCHEDULE_TO_CLOSE - Just Scheduled.
	p = newTimerQueueProcessor(s.engineImpl, s.WorkflowMgr, s.logger).(*timerQueueProcessorImpl)
	p.Start()

	ase = b.AddActivityTaskScheduledEvent(decisionTaskStartEvent.GetEventId(),
		&workflow.ScheduleActivityTaskDecisionAttributes{
			ScheduleToCloseTimeoutSeconds: common.Int32Ptr(1),
		})

	msBuilder = newMutableStateBuilder(s.logger)
	msBuilder.UpdatePendingActivity(ase.GetEventId(), &persistence.ActivityInfo{ScheduleToCloseTimeout: 1})
	t, err = tBuilder.AddScheduleToCloseActivityTimeout(ase.GetEventId(), msBuilder)
	s.Nil(err)
	s.NotNil(t)
	timerTasks = []persistence.Task{t}

	history, err = b.Serialize()
	s.Nil(err)

	s.updateHistoryAndTimers(workflowExecution, history, timerTasks, nil)
	p.NotifyNewTimer()

	s.waitForTimerTasksToProcess(p)
	s.Equal(uint64(1), p.timerFiredCount)
	running, b = s.checkTimedOutEventFor(workflowExecution, ase.GetEventId())
	s.False(running)

	// TimeoutType_SCHEDULE_TO_CLOSE - Scheduled and started.
	p = newTimerQueueProcessor(s.engineImpl, s.WorkflowMgr, s.logger).(*timerQueueProcessorImpl)
	p.Start()

	ase = b.AddActivityTaskScheduledEvent(decisionTaskStartEvent.GetEventId(),
		&workflow.ScheduleActivityTaskDecisionAttributes{
			ScheduleToCloseTimeoutSeconds: common.Int32Ptr(1),
		})
	aste = b.AddActivityTaskStartedEvent(ase.GetEventId(), &workflow.PollForActivityTaskRequest{})

	msBuilder = newMutableStateBuilder(s.logger)
	msBuilder.UpdatePendingActivity(ase.GetEventId(), &persistence.ActivityInfo{ScheduleToCloseTimeout: 1})
	t, err = tBuilder.AddScheduleToCloseActivityTimeout(ase.GetEventId(), msBuilder)
	s.Nil(err)
	s.NotNil(t)
	timerTasks = []persistence.Task{t}

	history, err = b.Serialize()
	s.Nil(err)

	s.updateHistoryAndTimers(workflowExecution, history, timerTasks, nil)
	p.NotifyNewTimer()

	s.waitForTimerTasksToProcess(p)
	s.Equal(uint64(1), p.timerFiredCount)
	running, b = s.checkTimedOutEventFor(workflowExecution, ase.GetEventId())
	s.False(running)

	// TimeoutType_SCHEDULE_TO_CLOSE - Scheduled, started, completed.
	p = newTimerQueueProcessor(s.engineImpl, s.WorkflowMgr, s.logger).(*timerQueueProcessorImpl)
	p.Start()

	ase = b.AddActivityTaskScheduledEvent(decisionTaskStartEvent.GetEventId(),
		&workflow.ScheduleActivityTaskDecisionAttributes{
			ScheduleToCloseTimeoutSeconds: common.Int32Ptr(1),
		})
	aste = b.AddActivityTaskStartedEvent(ase.GetEventId(), &workflow.PollForActivityTaskRequest{})

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

	s.updateHistoryAndTimers(workflowExecution, history, timerTasks, nil)
	p.NotifyNewTimer()

	s.waitForTimerTasksToProcess(p)
	s.Equal(uint64(1), p.timerFiredCount)
	running, b = s.checkTimedOutEventFor(workflowExecution, ase.GetEventId())
	s.False(running)

	// TimeoutType_HEARTBEAT - Scheduled, started.
	p = newTimerQueueProcessor(s.engineImpl, s.WorkflowMgr, s.logger).(*timerQueueProcessorImpl)
	p.Start()

	ase = b.AddActivityTaskScheduledEvent(decisionTaskStartEvent.GetEventId(),
		&workflow.ScheduleActivityTaskDecisionAttributes{
			HeartbeatTimeoutSeconds: common.Int32Ptr(1),
		})
	aste = b.AddActivityTaskStartedEvent(ase.GetEventId(), &workflow.PollForActivityTaskRequest{})

	msBuilder = newMutableStateBuilder(s.logger)
	msBuilder.UpdatePendingActivity(ase.GetEventId(), &persistence.ActivityInfo{HeartbeatTimeout: 1})

	t, err = tBuilder.AddHeartBeatActivityTimeout(ase.GetEventId(), msBuilder)
	s.Nil(err)
	s.NotNil(t)
	timerTasks = []persistence.Task{t}

	history, err = b.Serialize()
	s.Nil(err)

	// Update heart beat timer ID.
	msBuilder = newMutableStateBuilder(s.logger)
	msBuilder.UpdatePendingActivity(ase.GetEventId(), &persistence.ActivityInfo{
		ScheduleID: ase.GetEventId(), HeartbeatTimeout: 1})

	s.updateHistoryAndTimers(workflowExecution, history, timerTasks, msBuilder.updateActivityInfos)
	p.NotifyNewTimer()

	s.waitForTimerTasksToProcess(p)
	s.Equal(uint64(1), p.timerFiredCount)
	running, b = s.checkTimedOutEventFor(workflowExecution, ase.GetEventId())
	s.False(running)
}
