package matching

// This is disabled because of the dependency on history builder which is internal to history service
// TODO: rewrite tests to remove dependency

import (
	"math"
	"os"
	"testing"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/stretchr/testify/suite"
	"github.com/uber-common/bark"

	gohistory "code.uber.internal/devexp/minions/.gen/go/history"
	"code.uber.internal/devexp/minions/.gen/go/matching"
	workflow "code.uber.internal/devexp/minions/.gen/go/shared"
	"code.uber.internal/devexp/minions/common"
	"code.uber.internal/devexp/minions/common/mocks"
	"code.uber.internal/devexp/minions/common/persistence"
)

type (
	matchingEngineSuite struct {
		suite.Suite
		historyClient    *mocks.HistoryClient
		matchingEngine   *matchingEngineImpl
		mockTaskMgr      *mocks.TaskManager
		mockExecutionMgr *mocks.ExecutionManager
		logger           bark.Logger
	}
)

func TestMatchingEngineSuite(t *testing.T) {
	s := new(matchingEngineSuite)
	suite.Run(t, s)
}

func (s *matchingEngineSuite) SetupSuite() {
	if testing.Verbose() {
		log.SetOutput(os.Stdout)
	}
	s.logger = bark.NewLoggerFromLogrus(log.New())
}

func (s *matchingEngineSuite) TearDownSuite() {
}

func (s *matchingEngineSuite) SetupTest() {
	s.mockTaskMgr = &mocks.TaskManager{}
	s.mockExecutionMgr = &mocks.ExecutionManager{}
	s.historyClient = &mocks.HistoryClient{}
	s.matchingEngine = &matchingEngineImpl{
		taskManager:     s.mockTaskMgr,
		historyService:  s.historyClient,
		taskLists:       make(map[taskListID]*taskListContext),
		logger:          s.logger,
		tokenSerializer: common.NewJSONTaskTokenSerializer(),
	}
}

func (s *matchingEngineSuite) TearDownTest() {
	s.mockTaskMgr.AssertExpectations(s.T())
	s.mockExecutionMgr.AssertExpectations(s.T())
}

func (s *matchingEngineSuite) TestPollForActivityTasksEmptyResult() {
	s.matchingEngine.rangeSize = 2 // to test that range is not updated without tasks
	tl := "makeToast"
	identity := "selfDrivingToaster"

	taskList := workflow.NewTaskList()
	taskList.Name = &tl

	const rangeID = 123
	const pollCount = 15
	taskRequest := &persistence.GetTasksRequest{
		TaskList:     tl,
		TaskType:     persistence.TaskTypeActivity,
		RangeID:      rangeID,
		ReadLevel:    0,
		MaxReadLevel: math.MaxInt64,
		BatchSize:    1}

	// There are no tasks
	s.mockTaskMgr.On("GetTasks", taskRequest).Return(&persistence.GetTasksResponse{}, nil).Times(pollCount)
	s.mockTaskMgr.On("LeaseTaskList",
		&persistence.LeaseTaskListRequest{TaskList: "makeToast", TaskType: 1}).
		Return(&persistence.LeaseTaskListResponse{RangeID: rangeID}, nil).Once()
	for i := 0; i < pollCount; i++ {
		_, err := s.matchingEngine.pollForActivityTaskOperation(&workflow.PollForActivityTaskRequest{
			TaskList: taskList,
			Identity: &identity})
		s.Equal(ErrNoTasks, err)
	}
	s.mockTaskMgr.AssertExpectations(s.T())
}

func (s *matchingEngineSuite) TestPollForActivityTasks() {
	tl := "makeToast"
	identity := "selfDrivingToaster"

	taskList := workflow.NewTaskList()
	taskList.Name = &tl

	const rangeID = 123
	const pollCount = 115
	runID := "run1"
	workflowID := "workflow1"
	activityID := "activityId1"
	workflowExecution := &workflow.WorkflowExecution{RunId: &runID, WorkflowId: &workflowID}
	activityTypeName := "activity1"
	activityType := &workflow.ActivityType{Name: &activityTypeName}
	activityInput := []byte("Activity1 Input")
	taskRequest := &persistence.GetTasksRequest{
		TaskList:     tl,
		TaskType:     persistence.TaskTypeActivity,
		RangeID:      rangeID,
		ReadLevel:    0,
		MaxReadLevel: math.MaxInt64,
		BatchSize:    1}

	s.mockTaskMgr.On("LeaseTaskList",
		&persistence.LeaseTaskListRequest{TaskList: "makeToast", TaskType: persistence.TaskTypeActivity}).
		Return(&persistence.LeaseTaskListResponse{RangeID: rangeID}, nil).Once()
	for i := int64(0); i < pollCount; i++ {
		scheduleID := i * 10
		startedID := scheduleID + 10

		taskID := i
		tasks := []*persistence.TaskInfo{{ScheduleID: scheduleID, TaskID: taskID, RunID: runID, WorkflowID: workflowID}}
		s.mockTaskMgr.On("GetTasks", taskRequest).Return(&persistence.GetTasksResponse{Tasks: tasks}, nil).Once()
		s.historyClient.On("RecordActivityTaskStarted",
			&gohistory.RecordActivityTaskStartedRequest{
				WorkflowExecution: workflowExecution,
				ScheduleId:        &scheduleID,
				TaskId:            &taskID,
				PollRequest: &workflow.PollForActivityTaskRequest{
					TaskList: &workflow.TaskList{Name: taskList.Name}, Identity: &identity},
			}).Return(&gohistory.RecordActivityTaskStartedResponse{
			ScheduledEvent: newActivityTaskScheduledEvent(scheduleID, 0,
				&workflow.ScheduleActivityTaskDecisionAttributes{
					ActivityId:   &activityID,
					TaskList:     &workflow.TaskList{Name: taskList.Name},
					ActivityType: activityType,
					Input:        activityInput,
				}),
			StartedEvent: newActivityTaskStartedEvent(startedID, 0, &workflow.PollForActivityTaskRequest{
				TaskList: &workflow.TaskList{Name: taskList.Name},
				Identity: &identity,
			})}, nil)
		s.mockTaskMgr.On("CompleteTask",
			&persistence.CompleteTaskRequest{
				TaskID: i, TaskList: tl, TaskType: persistence.TaskTypeActivity}).Return(nil).Once()
	}
	for i := int64(0); i < pollCount; i++ {
		scheduleID := i * 10
		startedID := scheduleID + 10

		result, err := s.matchingEngine.pollForActivityTaskOperation(&workflow.PollForActivityTaskRequest{
			TaskList: taskList,
			Identity: &identity})
		s.NoError(err)
		s.EqualValues(activityID, *result.ActivityId)
		s.EqualValues(activityType, result.ActivityType)
		s.EqualValues(activityInput, result.Input)
		s.EqualValues(startedID, *result.StartedEventId)
		s.EqualValues(workflowExecution, result.WorkflowExecution)
		token := &common.TaskToken{
			WorkflowID: workflowID,
			RunID:      runID,
			ScheduleID: scheduleID,
		}

		taskToken, _ := s.matchingEngine.tokenSerializer.Serialize(token)
		s.EqualValues(taskToken, result.TaskToken)
	}
	s.mockTaskMgr.AssertExpectations(s.T())
}

func newActivityTaskScheduledEvent(eventID int64, decisionTaskCompletedEventID int64,
	scheduleAttributes *workflow.ScheduleActivityTaskDecisionAttributes) *workflow.HistoryEvent {
	historyEvent := newHistoryEvent(eventID, workflow.EventType_ActivityTaskScheduled)
	attributes := workflow.NewActivityTaskScheduledEventAttributes()
	attributes.ActivityId = common.StringPtr(scheduleAttributes.GetActivityId())
	attributes.ActivityType = scheduleAttributes.GetActivityType()
	attributes.TaskList = scheduleAttributes.GetTaskList()
	attributes.Input = scheduleAttributes.GetInput()
	attributes.ScheduleToCloseTimeoutSeconds = common.Int32Ptr(scheduleAttributes.GetScheduleToCloseTimeoutSeconds())
	attributes.ScheduleToStartTimeoutSeconds = common.Int32Ptr(scheduleAttributes.GetScheduleToStartTimeoutSeconds())
	attributes.StartToCloseTimeoutSeconds = common.Int32Ptr(scheduleAttributes.GetStartToCloseTimeoutSeconds())
	attributes.HeartbeatTimeoutSeconds = common.Int32Ptr(scheduleAttributes.GetHeartbeatTimeoutSeconds())
	attributes.DecisionTaskCompletedEventId = common.Int64Ptr(decisionTaskCompletedEventID)
	historyEvent.ActivityTaskScheduledEventAttributes = attributes

	return historyEvent
}

func newActivityTaskStartedEvent(eventID, scheduledEventID int64,
	request *workflow.PollForActivityTaskRequest) *workflow.HistoryEvent {
	historyEvent := newHistoryEvent(eventID, workflow.EventType_ActivityTaskStarted)
	attributes := workflow.NewActivityTaskStartedEventAttributes()
	attributes.ScheduledEventId = common.Int64Ptr(scheduledEventID)
	attributes.Identity = common.StringPtr(request.GetIdentity())
	historyEvent.ActivityTaskStartedEventAttributes = attributes

	return historyEvent
}

func newHistoryEvent(eventID int64, eventType workflow.EventType) *workflow.HistoryEvent {
	ts := common.Int64Ptr(time.Now().UnixNano())
	historyEvent := workflow.NewHistoryEvent()
	historyEvent.EventId = common.Int64Ptr(eventID)
	historyEvent.Timestamp = ts
	historyEvent.EventType = &eventType

	return historyEvent
}

func (s *matchingEngineSuite) TestAddActivityTasks() {
	s.matchingEngine.rangeSize = 3 // override to low number for the test
	tl := "makeToast"

	taskList := workflow.NewTaskList()
	taskList.Name = &tl

	const initialRangeID = 120
	const taskCount = 111

	runID := "run1"
	workflowID := "workflow1"
	execution := workflow.WorkflowExecution{RunId: &runID, WorkflowId: &workflowID}

	// Ensure that range is updated once the previous one is exhausted
	for i := int64(0); i <= (taskCount-1)/s.matchingEngine.rangeSize; i++ {
		s.mockTaskMgr.On("LeaseTaskList",
			&persistence.LeaseTaskListRequest{TaskList: "makeToast", TaskType: 1}).
			Return(&persistence.LeaseTaskListResponse{RangeID: initialRangeID + i}, nil).Once()
	}

	var rangeID int64 = initialRangeID
	for i := int64(0); i < taskCount; i++ {
		if i > 0 && i%s.matchingEngine.rangeSize == 0 {
			rangeID++
		}
		taskID := rangeID*s.matchingEngine.rangeSize + int64(i%s.matchingEngine.rangeSize)
		expectedTask := &persistence.CreateTaskRequest{
			Execution: execution,
			Data:      &persistence.ActivityTask{TaskID: taskID, TaskList: *taskList.Name, ScheduleID: i * 3},
			RangeID:   rangeID,
			TaskID:    taskID}
		s.mockTaskMgr.On("CreateTask", expectedTask).Return(&persistence.CreateTaskResponse{}, nil)
	}

	for i := int64(0); i < taskCount; i++ {
		scheduleID := i * 3
		addRequest := matching.AddActivityTaskRequest{
			Execution:  &execution,
			ScheduleId: &scheduleID,
			TaskList:   taskList}

		err := s.matchingEngine.AddActivityTask(&addRequest)
		s.Assert().NoError(err)
	}
	s.mockTaskMgr.AssertExpectations(s.T())
}
