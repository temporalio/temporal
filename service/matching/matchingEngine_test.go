package matching

import (
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/emirpasic/gods/maps/treemap"
	"github.com/stretchr/testify/mock"
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
	l := log.New()
	//l.Level = log.DebugLevel
	s.logger = bark.NewLoggerFromLogrus(l)
	// Get pprof HTTP UI at http://localhost:6060/debug/pprof/
	//go func() {
	//	log.Println(http.ListenAndServe("localhost:6060", nil))
	//}()
}

func (s *matchingEngineSuite) TearDownSuite() {
}

func (s *matchingEngineSuite) SetupTest() {
	s.mockTaskMgr = &mocks.TaskManager{}
	s.mockExecutionMgr = &mocks.ExecutionManager{}
	s.historyClient = &mocks.HistoryClient{}
	s.matchingEngine = &matchingEngineImpl{
		taskManager:                s.mockTaskMgr,
		historyService:             s.historyClient,
		taskLists:                  make(map[taskListID]*taskListContext),
		logger:                     s.logger,
		tokenSerializer:            common.NewJSONTaskTokenSerializer(),
		longPollExpirationInterval: 100 * time.Second, //time.Millisecond,
	}
}

func (s *matchingEngineSuite) TearDownTest() {
	s.mockTaskMgr.AssertExpectations(s.T())
	s.mockExecutionMgr.AssertExpectations(s.T())
}

func (s *matchingEngineSuite) TestAckManager() {
	m := newAckManager(s.logger)
	m.setAckLevel(100)
	s.EqualValues(100, m.getAckLevel())
	s.EqualValues(100, m.getReadLevel())
	const t1 = 200
	const t2 = 220
	const t3 = 320
	const t4 = 340

	m.addTask(t1)
	s.EqualValues(100, m.getAckLevel())
	s.EqualValues(t1, m.getReadLevel())

	m.addTask(t2)
	s.EqualValues(100, m.getAckLevel())
	s.EqualValues(t2, m.getReadLevel())

	m.completeTask(t2)
	s.EqualValues(100, m.getAckLevel())
	s.EqualValues(t2, m.getReadLevel())

	m.completeTask(t1)
	s.EqualValues(t2, m.getAckLevel())
	s.EqualValues(t2, m.getReadLevel())

	m.setAckLevel(300)
	s.EqualValues(300, m.getAckLevel())
	s.EqualValues(300, m.getReadLevel())

	m.addTask(t3)
	s.EqualValues(300, m.getAckLevel())
	s.EqualValues(t3, m.getReadLevel())

	m.addTask(t4)
	s.EqualValues(300, m.getAckLevel())
	s.EqualValues(t4, m.getReadLevel())

	m.completeTask(t3)
	s.EqualValues(t3, m.getAckLevel())
	s.EqualValues(t4, m.getReadLevel())

	m.completeTask(t4)
	s.EqualValues(t4, m.getAckLevel())
	s.EqualValues(t4, m.getReadLevel())
}

func (s *matchingEngineSuite) TestPollForActivityTasksEmptyResult() {
	s.PollForTasksEmptyResultTest(persistence.TaskListTypeActivity)
}

func (s *matchingEngineSuite) TestPollForDecisionTasksEmptyResult() {
	s.PollForTasksEmptyResultTest(persistence.TaskListTypeDecision)
}

func (s *matchingEngineSuite) PollForTasksEmptyResultTest(taskType int) {
	s.matchingEngine.rangeSize = 2 // to test that range is not updated without tasks
	s.matchingEngine.longPollExpirationInterval = 10 * time.Millisecond
	ttm := newTestTaskManager(s.logger)
	s.matchingEngine.taskManager = ttm
	tl := "makeToast"
	identity := "selfDrivingToaster"

	taskList := workflow.NewTaskList()
	taskList.Name = &tl
	tlID := newTaskListID(tl, taskType)
	//const rangeID = 123
	const pollCount = 10
	for i := 0; i < pollCount; i++ {
		if taskType == persistence.TaskListTypeActivity {
			resp, err := s.matchingEngine.PollForActivityTask(&workflow.PollForActivityTaskRequest{
				TaskList: taskList,
				Identity: &identity})
			s.NoError(err)
			s.Equal(emptyPollForActivityTaskResponse, resp)
		} else {
			resp, err := s.matchingEngine.PollForDecisionTask(&workflow.PollForDecisionTaskRequest{
				TaskList: taskList,
				Identity: &identity})
			s.NoError(err)
			s.Equal(emptyPollForDecisionTaskResponse, resp)
		}
	}
	s.EqualValues(1, ttm.taskLists[*tlID].rangeID)
}

func (s *matchingEngineSuite) TestAddActivityTasks() {
	s.AddTasksTest(persistence.TaskListTypeActivity)
}

func (s *matchingEngineSuite) TestAddDecisionTasks() {
	s.AddTasksTest(persistence.TaskListTypeDecision)
}

func (s *matchingEngineSuite) AddTasksTest(taskType int) {
	ttm := newTestTaskManager(s.logger)
	s.matchingEngine.taskManager = ttm
	s.matchingEngine.rangeSize = 300 // override to low number for the test

	tl := "makeToast"

	taskList := workflow.NewTaskList()
	taskList.Name = &tl

	const initialRangeID = 120
	const taskCount = 111

	runID := "run1"
	workflowID := "workflow1"
	execution := workflow.WorkflowExecution{RunId: &runID, WorkflowId: &workflowID}

	for i := int64(0); i < taskCount; i++ {
		scheduleID := i * 3
		var err error
		if taskType == persistence.TaskListTypeActivity {
			addRequest := matching.AddActivityTaskRequest{
				Execution:  &execution,
				ScheduleId: &scheduleID,
				TaskList:   taskList}

			err = s.matchingEngine.AddActivityTask(&addRequest)
		} else {
			addRequest := matching.AddDecisionTaskRequest{
				Execution:  &execution,
				ScheduleId: &scheduleID,
				TaskList:   taskList}

			err = s.matchingEngine.AddDecisionTask(&addRequest)
		}
		s.Assert().NoError(err)
	}
	s.Assert().EqualValues(taskCount, ttm.getTaskCount(&taskListID{taskListName: tl, taskType: taskType}))

}

func (s *matchingEngineSuite) TestAddThenConsumeActivities() {
	s.matchingEngine.longPollExpirationInterval = 10 * time.Millisecond

	runID := "run1"
	workflowID := "workflow1"
	workflowExecution := workflow.WorkflowExecution{RunId: &runID, WorkflowId: &workflowID}

	const taskCount = 1000
	const initialRangeID = 102
	// TODO: Understand why publish is low when rangeSize is 3
	const rangeSize = 30

	tl := "makeToast"
	tlID := &taskListID{taskListName: tl, taskType: persistence.TaskListTypeActivity}
	ttm := newTestTaskManager(s.logger)
	ttm.getTaskListManager(tlID).rangeID = initialRangeID
	s.matchingEngine.taskManager = ttm
	s.matchingEngine.rangeSize = rangeSize // override to low number for the test

	taskList := workflow.NewTaskList()
	taskList.Name = &tl

	for i := int64(0); i < taskCount; i++ {
		scheduleID := i * 3
		addRequest := matching.AddActivityTaskRequest{
			Execution:  &workflowExecution,
			ScheduleId: &scheduleID,
			TaskList:   taskList}

		err := s.matchingEngine.AddActivityTask(&addRequest)
		s.Assert().NoError(err)
	}
	s.Assert().EqualValues(taskCount, ttm.getTaskCount(tlID))

	activityTypeName := "activity1"
	activityID := "activityId1"
	activityType := &workflow.ActivityType{Name: &activityTypeName}
	activityInput := []byte("Activity1 Input")

	var startedID int64 = 123456
	identity := "nobody"

	// History service is using mock
	s.historyClient.On("RecordActivityTaskStarted",
		mock.AnythingOfType("*history.RecordActivityTaskStartedRequest")).Return(
		func(taskRequest *gohistory.RecordActivityTaskStartedRequest) *gohistory.RecordActivityTaskStartedResponse {
			s.logger.Debug("Mock Received RecordActivityTaskStartedRequest")
			return &gohistory.RecordActivityTaskStartedResponse{
				ScheduledEvent: newActivityTaskScheduledEvent(*taskRequest.ScheduleId, 0,
					&workflow.ScheduleActivityTaskDecisionAttributes{
						ActivityId:   &activityID,
						TaskList:     &workflow.TaskList{Name: taskList.Name},
						ActivityType: activityType,
						Input:        activityInput,
					}),
				StartedEvent: newActivityTaskStartedEvent(startedID, 0, &workflow.PollForActivityTaskRequest{
					TaskList: &workflow.TaskList{Name: taskList.Name},
					Identity: &identity,
				})}
		}, nil)

	for i := int64(0); i < taskCount; {
		scheduleID := i * 3

		result, err := s.matchingEngine.PollForActivityTask(&workflow.PollForActivityTaskRequest{
			TaskList: taskList,
			Identity: &identity})

		s.NoError(err)
		s.NotNil(result)
		if len(result.TaskToken) == 0 {
			s.logger.Debugf("empty poll returned")
			continue
		}
		s.EqualValues(activityID, *result.ActivityId)
		s.EqualValues(activityType, result.ActivityType)
		s.EqualValues(activityInput, result.Input)
		s.EqualValues(startedID, *result.StartedEventId)
		s.EqualValues(workflowExecution, *result.WorkflowExecution)
		token := &common.TaskToken{
			WorkflowID: workflowID,
			RunID:      runID,
			ScheduleID: scheduleID,
		}

		taskToken, _ := s.matchingEngine.tokenSerializer.Serialize(token)
		s.EqualValues(taskToken, result.TaskToken)
		i++
	}
	s.Assert().EqualValues(0, ttm.getTaskCount(tlID))
	expectedRange := int64(initialRangeID + taskCount/rangeSize)
	if taskCount%rangeSize > 0 {
		expectedRange++
	}
	// Due to conflicts some ids are skipped and more real ranges are used.
	s.Assert().True(expectedRange <= ttm.getTaskListManager(tlID).rangeID)
}

func (s *matchingEngineSuite) TestConcurrentPublishConsumeActivities() {
	runID := "run1"
	workflowID := "workflow1"
	workflowExecution := workflow.WorkflowExecution{RunId: &runID, WorkflowId: &workflowID}

	const workerCount = 20
	const taskCount = 100
	const initialRangeID = 0
	const rangeSize = 3
	var scheduleID int64 = 123

	tl := "makeToast"
	tlID := &taskListID{taskListName: tl, taskType: persistence.TaskListTypeActivity}
	ttm := newTestTaskManager(s.logger)
	ttm.getTaskListManager(tlID).rangeID = initialRangeID
	s.matchingEngine.taskManager = ttm
	s.matchingEngine.rangeSize = rangeSize // override to low number for the test

	taskList := workflow.NewTaskList()
	taskList.Name = &tl

	var wg sync.WaitGroup
	wg.Add(2 * workerCount)

	for p := 0; p < workerCount; p++ {
		go func() {
			for i := int64(0); i < taskCount; i++ {
				addRequest := matching.AddActivityTaskRequest{
					Execution:  &workflowExecution,
					ScheduleId: &scheduleID,
					TaskList:   taskList}

				err := s.matchingEngine.AddActivityTask(&addRequest)
				if err != nil {
					panic(err)
				}
			}
			wg.Done()
		}()
	}

	activityTypeName := "activity1"
	activityID := "activityId1"
	activityType := &workflow.ActivityType{Name: &activityTypeName}
	activityInput := []byte("Activity1 Input")

	var startedID int64 = 123456
	identity := "nobody"

	// History service is using mock
	s.historyClient.On("RecordActivityTaskStarted",
		mock.AnythingOfType("*history.RecordActivityTaskStartedRequest")).Return(
		func(taskRequest *gohistory.RecordActivityTaskStartedRequest) *gohistory.RecordActivityTaskStartedResponse {
			s.logger.Debug("Mock Received RecordActivityTaskStartedRequest")
			return &gohistory.RecordActivityTaskStartedResponse{
				ScheduledEvent: newActivityTaskScheduledEvent(*taskRequest.ScheduleId, 0,
					&workflow.ScheduleActivityTaskDecisionAttributes{
						ActivityId:   &activityID,
						TaskList:     &workflow.TaskList{Name: taskList.Name},
						ActivityType: activityType,
						Input:        activityInput,
					}),
				StartedEvent: newActivityTaskStartedEvent(startedID, 0, &workflow.PollForActivityTaskRequest{
					TaskList: &workflow.TaskList{Name: taskList.Name},
					Identity: &identity,
				})}
		}, nil)
	for p := 0; p < workerCount; p++ {
		go func() {
			for i := int64(0); i < taskCount; {
				result, err := s.matchingEngine.PollForActivityTask(&workflow.PollForActivityTaskRequest{
					TaskList: taskList,
					Identity: &identity})
				if err != nil {
					panic(err)
				}
				s.NotNil(result)
				if len(result.TaskToken) == 0 {
					s.logger.Debugf("empty poll returned")
					continue
				}
				s.EqualValues(activityID, *result.ActivityId)
				s.EqualValues(activityType, result.ActivityType)
				s.EqualValues(activityInput, result.Input)
				s.EqualValues(startedID, *result.StartedEventId)
				s.EqualValues(workflowExecution, *result.WorkflowExecution)
				token := &common.TaskToken{
					WorkflowID: workflowID,
					RunID:      runID,
					ScheduleID: scheduleID,
				}
				resultToken, err := s.matchingEngine.tokenSerializer.Deserialize(result.TaskToken)
				if err != nil {
					panic(err)
				}

				//taskToken, _ := s.matchingEngine.tokenSerializer.Serialize(token)
				//s.EqualValues(taskToken, result.TaskToken, fmt.Sprintf("%v!=%v", string(taskToken)))
				s.EqualValues(token, resultToken, fmt.Sprintf("%v!=%v", token, resultToken))
				i++
			}
			wg.Done()
		}()
	}
	wg.Wait()
	s.Assert().EqualValues(0, ttm.getTaskCount(tlID))
	expectedRange := int64(initialRangeID + taskCount*workerCount/rangeSize)
	if (taskCount*taskCount)%rangeSize > 0 {
		expectedRange++
	}
	// Due to conflicts some ids are skipped and more real ranges are used.
	s.Assert().True(expectedRange <= ttm.getTaskListManager(tlID).rangeID)

}

func (s *matchingEngineSuite) TestConcurrentPublishConsumeDecisions() {
	runID := "run1"
	workflowID := "workflow1"
	workflowExecution := workflow.WorkflowExecution{RunId: &runID, WorkflowId: &workflowID}

	const workerCount = 20
	const taskCount = 100
	const initialRangeID = 0
	const rangeSize = 5
	var scheduleID int64 = 123
	var startedEventID int64 = 1412

	tl := "makeToast"
	tlID := &taskListID{taskListName: tl, taskType: persistence.TaskListTypeDecision}
	ttm := newTestTaskManager(s.logger)
	ttm.getTaskListManager(tlID).rangeID = initialRangeID
	s.matchingEngine.taskManager = ttm
	s.matchingEngine.rangeSize = rangeSize // override to low number for the test

	taskList := workflow.NewTaskList()
	taskList.Name = &tl

	var wg sync.WaitGroup
	wg.Add(2 * workerCount)

	for p := 0; p < workerCount; p++ {
		go func() {
			for i := int64(0); i < taskCount; i++ {
				addRequest := matching.AddDecisionTaskRequest{
					Execution:  &workflowExecution,
					ScheduleId: &scheduleID,
					TaskList:   taskList}

				err := s.matchingEngine.AddDecisionTask(&addRequest)
				if err != nil {
					panic(err)
				}
			}
			wg.Done()
		}()
	}
	workflowTypeName := "workflowType1"
	activityID := "workflow1"
	activityTypeName := "activity1"
	workflowType := &workflow.WorkflowType{Name: &workflowTypeName}
	activityType := &workflow.ActivityType{Name: &activityTypeName}

	decisionInput := []byte("Decision1 Input")

	identity := "nobody"

	// History service is using mock
	s.historyClient.On("RecordDecisionTaskStarted",
		mock.AnythingOfType("*history.RecordDecisionTaskStartedRequest")).Return(
		func(taskRequest *gohistory.RecordDecisionTaskStartedRequest) *gohistory.RecordDecisionTaskStartedResponse {
			s.logger.Debug("Mock Received RecordDecisionTaskStartedRequest")
			return &gohistory.RecordDecisionTaskStartedResponse{
				PreviousStartedEventId: &startedEventID,
				StartedEventId:         &startedEventID,
				WorkflowType:           workflowType,
				History: &workflow.History{Events: []*workflow.HistoryEvent{
					newActivityTaskScheduledEvent(*taskRequest.ScheduleId, 0,
						&workflow.ScheduleActivityTaskDecisionAttributes{
							ActivityId:   &activityID,
							TaskList:     &workflow.TaskList{Name: taskList.Name},
							ActivityType: activityType,
							Input:        decisionInput,
						}),
					newActivityTaskStartedEvent(startedEventID, 0, &workflow.PollForActivityTaskRequest{
						TaskList: &workflow.TaskList{Name: taskList.Name},
						Identity: &identity,
					})}}}
		}, nil)
	for p := 0; p < workerCount; p++ {
		go func() {
			for i := int64(0); i < taskCount; {
				result, err := s.matchingEngine.PollForDecisionTask(&workflow.PollForDecisionTaskRequest{
					TaskList: taskList,
					Identity: &identity})
				if err != nil {
					panic(err)
				}
				s.NotNil(result)
				if len(result.TaskToken) == 0 {
					s.logger.Debugf("empty poll returned")
					continue
				}
				history := result.History.Events
				s.EqualValues(2, len(history))
				s.EqualValues(workflowExecution, *result.WorkflowExecution)
				s.EqualValues(workflowType, result.WorkflowType)
				s.EqualValues(startedEventID, *result.StartedEventId)
				s.EqualValues(workflowExecution, *result.WorkflowExecution)
				token := &common.TaskToken{
					WorkflowID: workflowID,
					RunID:      runID,
					ScheduleID: scheduleID,
				}
				resultToken, err := s.matchingEngine.tokenSerializer.Deserialize(result.TaskToken)
				if err != nil {
					panic(err)
				}

				//taskToken, _ := s.matchingEngine.tokenSerializer.Serialize(token)
				//s.EqualValues(taskToken, result.TaskToken, fmt.Sprintf("%v!=%v", string(taskToken)))
				s.EqualValues(token, resultToken, fmt.Sprintf("%v!=%v", token, resultToken))
				i++
			}
			wg.Done()
		}()
	}
	wg.Wait()
	s.Assert().EqualValues(0, ttm.getTaskCount(tlID))
	expectedRange := int64(initialRangeID + taskCount*workerCount/rangeSize)
	if (taskCount*taskCount)%rangeSize > 0 {
		expectedRange++
	}
	// Due to conflicts some ids are skipped and more real ranges are used.
	s.Assert().True(expectedRange <= ttm.getTaskListManager(tlID).rangeID)

}

func newActivityTaskScheduledEvent(eventID int64, decisionTaskCompletedEventID int64,
	scheduleAttributes *workflow.ScheduleActivityTaskDecisionAttributes) *workflow.HistoryEvent {
	historyEvent := newHistoryEvent(eventID, workflow.EventType_ActivityTaskScheduled)
	attributes := workflow.NewActivityTaskScheduledEventAttributes()
	attributes.ActivityId = scheduleAttributes.ActivityId
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

var _ persistence.TaskManager = (*testTaskManager)(nil) // Asserts that interface is indeed implemented

type testTaskManager struct {
	sync.Mutex
	taskLists map[taskListID]*testTaskListManager
	logger    bark.Logger
}

func newTestTaskManager(logger bark.Logger) *testTaskManager {
	return &testTaskManager{taskLists: make(map[taskListID]*testTaskListManager), logger: logger}
}

func (m *testTaskManager) getTaskListManager(id *taskListID) *testTaskListManager {
	m.Lock()
	defer m.Unlock()
	result, ok := m.taskLists[*id]
	if ok {
		return result
	}
	result = newTestTaskListManager()
	m.taskLists[*id] = result
	return result
}

type testTaskListManager struct {
	sync.Mutex
	rangeID  int64
	ackLevel int64
	tasks    *treemap.Map
}

func Int64Comparator(a, b interface{}) int {
	aAsserted := a.(int64)
	bAsserted := b.(int64)
	switch {
	case aAsserted > bAsserted:
		return 1
	case aAsserted < bAsserted:
		return -1
	default:
		return 0
	}
}

func newTestTaskListManager() *testTaskListManager {
	return &testTaskListManager{tasks: treemap.NewWith(Int64Comparator)}
}

// LeaseTaskList provides a mock function with given fields: request
func (m *testTaskManager) LeaseTaskList(request *persistence.LeaseTaskListRequest) (*persistence.LeaseTaskListResponse, error) {
	tlm := m.getTaskListManager(newTaskListID(request.TaskList, request.TaskType))
	tlm.Lock()
	defer tlm.Unlock()
	tlm.rangeID++
	return &persistence.LeaseTaskListResponse{
		TaskListInfo: &persistence.TaskListInfo{AckLevel: tlm.ackLevel, Name: request.TaskList, TaskType: request.TaskType, RangeID: tlm.rangeID},
	}, nil
}

// UpdateTaskList provides a mock function with given fields: request
func (m *testTaskManager) UpdateTaskList(request *persistence.UpdateTaskListRequest) (*persistence.UpdateTaskListResponse, error) {
	tli := request.TaskListInfo
	tlm := m.getTaskListManager(newTaskListID(tli.Name, tli.TaskType))

	tlm.Lock()
	defer tlm.Unlock()
	if tlm.rangeID != tli.RangeID {
		return nil, &persistence.ConditionFailedError{
			Msg: fmt.Sprintf("Failed to update task list: name=%v, type=%v", tli.Name, tli.TaskType),
		}
	}
	tlm.ackLevel = tli.AckLevel
	return &persistence.UpdateTaskListResponse{}, nil
}

// CompleteTask provides a mock function with given fields: request
func (m *testTaskManager) CompleteTask(request *persistence.CompleteTaskRequest) error {
	tli := request.TaskList
	tlm := m.getTaskListManager(newTaskListID(tli.Name, tli.TaskType))

	tlm.Lock()
	defer tlm.Unlock()
	if tlm.rangeID != tli.RangeID {
		return &persistence.ConditionFailedError{
			Msg: fmt.Sprintf("Failed to complete task. TaskList: %v, taskType: %v, rangeID: %v, db rangeID: %v",
				request.TaskList.Name, request.TaskList.TaskType, tli.RangeID, tlm.rangeID),
		}
	}
	tlm.tasks.Remove(request.TaskID)
	return nil
}

// CreateTask provides a mock function with given fields: request
func (m *testTaskManager) CreateTask(request *persistence.CreateTaskRequest) (*persistence.CreateTaskResponse, error) {
	var taskList string
	var scheduleID int64
	taskType := request.Data.GetType()
	switch taskType {
	case persistence.TaskListTypeActivity:
		taskList = request.Data.(*persistence.ActivityTask).TaskList
		scheduleID = request.Data.(*persistence.ActivityTask).ScheduleID

	case persistence.TaskListTypeDecision:
		taskList = request.Data.(*persistence.DecisionTask).TaskList
		scheduleID = request.Data.(*persistence.DecisionTask).ScheduleID
	}
	tlm := m.getTaskListManager(newTaskListID(taskList, taskType))
	tlm.Lock()
	defer tlm.Unlock()

	if tlm.rangeID != request.RangeID {
		return nil, &persistence.ConditionFailedError{
			Msg: fmt.Sprintf("Failed to create task. TaskList: %v, taskType: %v, rangeID: %v, db rangeID: %v",
				taskList, taskType, request.RangeID, tlm.rangeID),
		}
	}
	_, ok := tlm.tasks.Get(request.TaskID)
	if ok {
		return nil, fmt.Errorf("Duplicated TaskID %v", request.TaskID)
	}
	tlm.tasks.Put(request.TaskID, &persistence.TaskInfo{
		RunID:      *request.Execution.RunId,
		ScheduleID: scheduleID,
		TaskID:     request.TaskID,
		WorkflowID: *request.Execution.WorkflowId,
	})
	return &persistence.CreateTaskResponse{}, nil
}

// GetTasks provides a mock function with given fields: request
func (m *testTaskManager) GetTasks(request *persistence.GetTasksRequest) (*persistence.GetTasksResponse, error) {
	tlm := m.getTaskListManager(newTaskListID(request.TaskList, request.TaskType))
	tlm.Lock()
	defer tlm.Unlock()
	if tlm.rangeID != request.RangeID {
		return nil, &persistence.ConditionFailedError{
			Msg: fmt.Sprintf("Failed to create task. TaskList: %v, taskType: %v, rangeID: %v, db rangeID: %v",
				request.TaskList, request.TaskType, request.RangeID, tlm.rangeID),
		}
	}
	var tasks []*persistence.TaskInfo

	it := tlm.tasks.Iterator()
	for it.Next() {
		taskID := it.Key().(int64)
		if taskID <= request.ReadLevel {
			continue
		}
		if taskID > request.MaxReadLevel {
			break
		}
		tasks = append(tasks, it.Value().(*persistence.TaskInfo))
	}
	return &persistence.GetTasksResponse{
		Tasks: tasks,
	}, nil
}

// GetTasks provides a mock function with given fields: request
func (m *testTaskManager) getTaskCount(taskList *taskListID) int {
	tlm := m.getTaskListManager(taskList)
	tlm.Lock()
	defer tlm.Unlock()
	return tlm.tasks.Size()
}
