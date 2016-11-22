package workflow

import (
	"math/rand"
	"os"
	"testing"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/gocql/gocql"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"

	workflow "code.uber.internal/devexp/minions/.gen/go/minions"
	"code.uber.internal/devexp/minions/common"
)

const (
	testWorkflowClusterHosts = "127.0.0.1"
)

type (
	cassandraPersistenceSuite struct {
		suite.Suite
		workflowTestBase
	}

	workflowTestBase struct {
		workflowMgr workflowExecutionPersistence
		taskMgr     taskPersistence
		cassandraTestCluster
	}

	cassandraTestCluster struct {
		keyspace string
		cluster  *gocql.ClusterConfig
		session  *gocql.Session
	}
)

func TestCassandraPersistenceSuite(t *testing.T) {
	s := new(cassandraPersistenceSuite)
	suite.Run(t, s)
}

func (s *cassandraPersistenceSuite) SetupSuite() {
	if testing.Verbose() {
		log.SetOutput(os.Stdout)
	}

	s.setupWorkflowStore()
}

func (s *cassandraPersistenceSuite) TearDownSuite() {
	s.tearDownWorkflowStore()
}

func (s *cassandraPersistenceSuite) TestStartWorkflow() {
	workflowExecution := workflow.WorkflowExecution{WorkflowId: common.StringPtr("start-workflow-test"),
		RunId: common.StringPtr("7f9fe8a0-9237-11e6-ae22-56b6b6499611")}
	task0, err0 := s.createWorkflowExecution(workflowExecution, "queue1", "event1", nil, 3, 0, 2)
	s.Nil(err0, "No error expected.")
	s.NotEmpty(task0, "Expected non empty task identifier.")

	task1, err1 := s.createWorkflowExecution(workflowExecution, "queue1", "event1", nil, 3, 0, 2)
	s.NotNil(err1, "Expected workflow creation to fail.")
	s.Empty(task1, "Expected empty task identifier.")
	log.Infof("Workflow execution failed with error: %v", err1)
}

func (s *cassandraPersistenceSuite) TestGetWorkflow() {
	workflowExecution := workflow.WorkflowExecution{WorkflowId: common.StringPtr("get-workflow-test"),
		RunId: common.StringPtr("918e7b1d-bfa4-4fe0-86cb-604858f90ce4")}
	task0, err0 := s.createWorkflowExecution(workflowExecution, "queue1", "event1", nil, 3, 0, 2)
	s.Nil(err0, "No error expected.")
	s.NotEmpty(task0, "Expected non empty task identifier.")

	info, err1 := s.getWorkflowExecutionInfo(workflowExecution)
	s.Nil(err1, "No error expected.")
	s.NotNil(info, "Valid Workflow response expected.")
	s.NotNil(info, "Valid Workflow info expected.")
	s.Equal("get-workflow-test", info.workflowID)
	s.Equal("918e7b1d-bfa4-4fe0-86cb-604858f90ce4", info.runID)
	s.Equal("queue1", info.taskList)
	s.Equal("event1", string(info.history))
	s.Equal([]byte(nil), info.executionContext)
	s.Equal(workflowStateCreated, info.state)
	s.Equal(int64(3), info.nextEventID)
	s.Equal(int64(0), info.lastProcessedEvent)
	s.Equal(true, info.decisionPending)
	s.Equal(true, validateTimeRange(info.lastUpdatedTimestamp, time.Hour))
	log.Infof("Workflow execution last updated: %v", info.lastUpdatedTimestamp)
}

func (s *cassandraPersistenceSuite) TestUpdateWorkflow() {
	workflowExecution := workflow.WorkflowExecution{WorkflowId: common.StringPtr("update-workflow-test"),
		RunId: common.StringPtr("5ba5e531-e46b-48d9-b4b3-859919839553")}
	task0, err0 := s.createWorkflowExecution(workflowExecution, "queue1", "event1", nil, 3, 0, 2)
	s.Nil(err0, "No error expected.")
	s.NotEmpty(task0, "Expected non empty task identifier.")

	info0, err1 := s.getWorkflowExecutionInfo(workflowExecution)
	s.Nil(err1, "No error expected.")
	s.NotNil(info0, "Valid Workflow info expected.")
	s.Equal("update-workflow-test", info0.workflowID)
	s.Equal("5ba5e531-e46b-48d9-b4b3-859919839553", info0.runID)
	s.Equal("queue1", info0.taskList)
	s.Equal("event1", string(info0.history))
	s.Equal([]byte(nil), info0.executionContext)
	s.Equal(workflowStateCreated, info0.state)
	s.Equal(int64(3), info0.nextEventID)
	s.Equal(int64(0), info0.lastProcessedEvent)
	s.Equal(true, info0.decisionPending)
	s.Equal(true, validateTimeRange(info0.lastUpdatedTimestamp, time.Hour))
	log.Infof("Workflow execution last updated: %v", info0.lastUpdatedTimestamp)

	updatedInfo := copyWorkflowExecutionInfo(info0)
	updatedInfo.history = []byte(`event2`)
	updatedInfo.nextEventID = int64(5)
	updatedInfo.lastProcessedEvent = int64(2)
	err2 := s.updateWorkflowExecution(updatedInfo, []int64{int64(4)}, nil, int64(3))
	s.Nil(err2, "No error expected.")

	info1, err3 := s.getWorkflowExecutionInfo(workflowExecution)
	s.Nil(err3, "No error expected.")
	s.NotNil(info1, "Valid Workflow info expected.")
	s.Equal("update-workflow-test", info1.workflowID)
	s.Equal("5ba5e531-e46b-48d9-b4b3-859919839553", info1.runID)
	s.Equal("queue1", info1.taskList)
	s.Equal("event2", string(info1.history))
	s.Equal([]byte(nil), info1.executionContext)
	s.Equal(workflowStateCreated, info1.state)
	s.Equal(int64(5), info1.nextEventID)
	s.Equal(int64(2), info1.lastProcessedEvent)
	s.Equal(true, info1.decisionPending)
	s.Equal(true, validateTimeRange(info1.lastUpdatedTimestamp, time.Hour))
	log.Infof("Workflow execution last updated: %v", info1.lastUpdatedTimestamp)

	failedUpdatedInfo := copyWorkflowExecutionInfo(info0)
	failedUpdatedInfo.history = []byte(`event3`)
	failedUpdatedInfo.nextEventID = int64(6)
	failedUpdatedInfo.lastProcessedEvent = int64(3)
	err4 := s.updateWorkflowExecution(updatedInfo, []int64{int64(5)}, nil, int64(3))
	s.NotNil(err4, "No error expected.")
	s.IsType(&conditionFailedError{}, err4)
	log.Infof("Conditional update failed with error: %v", err4)

	info2, err4 := s.getWorkflowExecutionInfo(workflowExecution)
	s.Nil(err4, "No error expected.")
	s.NotNil(info2, "Valid Workflow info expected.")
	s.Equal("update-workflow-test", info2.workflowID)
	s.Equal("5ba5e531-e46b-48d9-b4b3-859919839553", info2.runID)
	s.Equal("queue1", info2.taskList)
	s.Equal("event2", string(info2.history))
	s.Equal([]byte(nil), info2.executionContext)
	s.Equal(workflowStateCreated, info2.state)
	s.Equal(int64(5), info2.nextEventID)
	s.Equal(int64(2), info2.lastProcessedEvent)
	s.Equal(true, info2.decisionPending)
	s.Equal(true, validateTimeRange(info2.lastUpdatedTimestamp, time.Hour))
	log.Infof("Workflow execution last updated: %v", info2.lastUpdatedTimestamp)
}

func (s *cassandraPersistenceSuite) TestDeleteWorkflow() {
	workflowExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("delete-workflow-test"),
		RunId:      common.StringPtr("4e0917f2-9361-4a14-b16f-1fafe09b287a")}
	task0, err0 := s.createWorkflowExecution(workflowExecution, "queue1", "event1", nil, 3, 0, 2)
	s.Nil(err0, "No error expected.")
	s.NotNil(task0, "Expected non empty task identifier.")

	info0, err1 := s.getWorkflowExecutionInfo(workflowExecution)
	s.Nil(err1, "No error expected.")
	s.NotNil(info0, "Valid Workflow info expected.")
	s.Equal("delete-workflow-test", info0.workflowID)
	s.Equal("4e0917f2-9361-4a14-b16f-1fafe09b287a", info0.runID)
	s.Equal("queue1", info0.taskList)
	s.Equal("event1", string(info0.history))
	s.Equal([]byte(nil), info0.executionContext)
	s.Equal(workflowStateCreated, info0.state)
	s.Equal(int64(3), info0.nextEventID)
	s.Equal(int64(0), info0.lastProcessedEvent)
	s.Equal(true, info0.decisionPending)
	s.Equal(true, validateTimeRange(info0.lastUpdatedTimestamp, time.Hour))
	log.Infof("Workflow execution last updated: %v", info0.lastUpdatedTimestamp)

	updatedInfo := copyWorkflowExecutionInfo(info0)
	updatedInfo.history = []byte(`event2`)
	updatedInfo.nextEventID = int64(5)
	updatedInfo.lastProcessedEvent = int64(2)
	err2 := s.updateWorkflowExecution(updatedInfo, []int64{int64(5)}, nil, int64(3))
	s.Nil(err2, "No error expected.")

	err4 := s.deleteWorkflowExecution(workflowExecution, info0.nextEventID)
	s.NotNil(err4, "conflict expected.")
	s.IsType(&conditionFailedError{}, err4)
	log.Infof("Conditional update failed with error: %v", err4)

	info1, err3 := s.getWorkflowExecutionInfo(workflowExecution)
	s.Nil(err3, "No error expected.")
	s.NotNil(info1, "Valid Workflow info expected.")
	s.Equal("delete-workflow-test", info1.workflowID)
	s.Equal("4e0917f2-9361-4a14-b16f-1fafe09b287a", info1.runID)
	s.Equal("queue1", info1.taskList)
	s.Equal("event2", string(info1.history))
	s.Equal([]byte(nil), info1.executionContext)
	s.Equal(workflowStateCreated, info1.state)
	s.Equal(int64(5), info1.nextEventID)
	s.Equal(int64(2), info1.lastProcessedEvent)
	s.Equal(true, info1.decisionPending)
	s.Equal(true, validateTimeRange(info1.lastUpdatedTimestamp, time.Hour))
	log.Infof("Workflow execution last updated: %v", info1.lastUpdatedTimestamp)

	err5 := s.deleteWorkflowExecution(workflowExecution, info1.nextEventID)
	s.Nil(err5, "No error expected.")

	info2, err6 := s.getWorkflowExecutionInfo(workflowExecution)
	s.Nil(info2, "No result expected.")
	s.NotNil(err6, "Workflow execution should not exist.")
	s.IsType(&workflow.EntityNotExistsError{}, err6)
	log.Infof("Workflow execution not found: %v", err6)
}

func (s *cassandraPersistenceSuite) TestTransferTasks() {
	// First cleanup transfer tasks from other tests
	s.clearTransferQueue()

	startTime := time.Now()
	workflowExecution := workflow.WorkflowExecution{WorkflowId: common.StringPtr("get-transfer-tasks-test"),
		RunId: common.StringPtr("93c87aff-ed89-4ecb-b0fd-d5d1e25dc46d")}

	task0, err0 := s.createWorkflowExecution(workflowExecution, "queue1", "event1", nil, 3, 0, 2)
	s.Nil(err0, "No error expected.")
	s.NotEmpty(task0, "Expected non empty task identifier.")

	tasks1, err1 := s.getTransferTasks(time.Minute, 1)
	s.Nil(err1, "No error expected.")
	s.NotNil(tasks1, "expected valid list of tasks.")
	s.Equal(1, len(tasks1), "Expected 1 decision task.")
	task1 := tasks1[0]
	s.Equal(workflowExecution.GetWorkflowId(), task1.workflowID)
	s.Equal(workflowExecution.GetRunId(), task1.runID)
	s.Equal("queue1", task1.taskList)
	s.Equal(taskTypeDecision, task1.taskType)
	s.Equal(int64(2), task1.scheduleID)
	s.NotNil(task1.lockToken)
	s.True(task1.visibilityTime.Before(startTime.Add(10 * time.Minute)))
	s.True(time.Now().Before(task1.visibilityTime))
	s.Equal(1, task1.deliveryCount)

	badToken := uuid.New()
	err2 := s.completeTransferTask(workflowExecution, task1.taskID, badToken)
	s.NotNil(err2)
	log.Infof("Failed to complete task '%v' using lock token '%v'.  Error: %v", task1.taskID, badToken, err2)
	s.IsType(&conditionFailedError{}, err2, err2.Error())

	err3 := s.completeTransferTask(workflowExecution, task1.taskID, task1.lockToken)
	s.Nil(err3)

	err4 := s.completeTransferTask(workflowExecution, task1.taskID, task1.lockToken)
	s.NotNil(err4)
	log.Infof("Failed to complete task '%v' using lock token '%v'.  Error: %v", task1.taskID, task1.lockToken, err4)
	s.IsType(&workflow.EntityNotExistsError{}, err4)
}

func (s *cassandraPersistenceSuite) TestCreateTask() {
	workflowExecution := workflow.WorkflowExecution{WorkflowId: common.StringPtr("create-task-test"),
		RunId: common.StringPtr("c949447a-691a-4132-8b2a-a5b38106793c")}
	task0, err0 := s.createDecisionTask(workflowExecution, "a5b38106793c", 5)
	s.Nil(err0, "No error expected.")
	s.NotEmpty(task0, "Expected non empty task identifier.")

	tasks1, err1 := s.createActivityTasks(workflowExecution, map[int64]string{
		10: "a5b38106793c"})
	s.Nil(err1, "No error expected.")
	s.NotNil(tasks1, "Expected valid task identifiers.")
	s.Equal(1, len(tasks1), "expected single valid task identifier.")
	for _, t := range tasks1 {
		s.NotEmpty(t, "Expected non empty task identifier.")
	}

	tasks2, err2 := s.createActivityTasks(workflowExecution, map[int64]string{
		20: "a5b38106793a",
		30: "a5b38106793b",
		40: "a5b38106793c",
		50: "a5b38106793d",
		60: "a5b38106793e",
	})
	s.Nil(err2, "No error expected.")
	s.NotNil(tasks2, "Expected valid task identifiers.")
	s.Equal(5, len(tasks2), "expected single valid task identifier.")
	for _, t := range tasks2 {
		s.NotEmpty(t, "Expected non empty task identifier.")
	}
}

func (s *cassandraPersistenceSuite) TestGetDecisionTasks() {
	workflowExecution := workflow.WorkflowExecution{WorkflowId: common.StringPtr("get-decision-task-test"),
		RunId: common.StringPtr("db20f7e2-1a1e-40d9-9278-d8b886738e05")}
	taskList := "d8b886738e05"
	task0, err0 := s.createDecisionTask(workflowExecution, taskList, 5)
	s.Nil(err0, "No error expected.")
	s.NotEmpty(task0, "Expected non empty task identifier.")

	tasks1, err1 := s.getTasks(taskList, taskTypeDecision, time.Minute, 1)
	s.Nil(err1, "No error expected.")
	s.NotNil(tasks1, "expected valid list of tasks.")
	s.Equal(1, len(tasks1), "Expected 1 decision task.")
}

func (s *cassandraPersistenceSuite) TestCompleteDecisionTask() {
	workflowExecution := workflow.WorkflowExecution{WorkflowId: common.StringPtr("complete-decision-task-test"),
		RunId: common.StringPtr("2aa0a74e-16ee-4f27-983d-48b07ec1915d")}
	taskList := "48b07ec1915d"
	startTime := time.Now()
	tasks0, err0 := s.createActivityTasks(workflowExecution, map[int64]string{
		10: taskList,
		20: taskList,
		30: taskList,
		40: taskList,
		50: taskList,
	})
	s.Nil(err0, "No error expected.")
	s.NotNil(tasks0, "Expected non empty task identifier.")
	s.Equal(5, len(tasks0), "expected 5 valid task identifier.")
	for _, t := range tasks0 {
		s.NotEmpty(t, "Expected non empty task identifier.")
	}

	tasks1, err1 := s.getTasks(taskList, taskTypeActivity, time.Minute, 5)
	s.Nil(err1, "No error expected.")
	s.NotNil(tasks1, "expected valid list of tasks.")

	s.Equal(5, len(tasks1), "Expected 5 activity tasks.")
	for _, t := range tasks1 {
		s.Equal(workflowExecution.GetWorkflowId(), t.workflowID)
		s.Equal(workflowExecution.GetRunId(), t.runID)
		s.NotEmpty(t.taskID)
		s.Equal(taskList, t.taskList)
		s.Equal(taskTypeActivity, t.taskType)
		s.True(t.visibilityTime.Before(startTime.Add(10 * time.Minute)))
		s.True(time.Now().Before(t.visibilityTime))
		s.NotEmpty(t.lockToken)
		s.Equal(1, t.deliveryCount)

		err2 := s.completeTask(workflowExecution, t.taskList, t.taskType, t.taskID, t.lockToken)
		s.Nil(err2)
	}

	for _, t := range tasks1 {
		err3 := s.completeTask(workflowExecution, t.taskList, t.taskType, t.taskID, t.lockToken)
		s.NotNil(err3)
		log.Infof("Failed to complete task '%v' using lock token '%v'.  Error: %v", t.taskID, t.lockToken, err3)
		s.IsType(&workflow.EntityNotExistsError{}, err3)
	}
}

func (s *cassandraPersistenceSuite) TestCompleteDecisionTaskConflict() {
	workflowExecution := workflow.WorkflowExecution{WorkflowId: common.StringPtr("conflict-decision-task-test"),
		RunId: common.StringPtr("c4f353fe-302e-483d-9b57-9d2278cfcadb")}
	taskList := "9d2278cfcadb"
	badToken := "99fd2d2e-e9eb-47d0-9b1d-e08dab6ffb65"
	tasks0, err0 := s.createActivityTasks(workflowExecution, map[int64]string{
		10: taskList,
	})
	s.Nil(err0, "No error expected.")
	s.NotNil(tasks0, "Expected non empty task identifier.")
	s.Equal(1, len(tasks0), "expected 1 valid task identifier.")
	for _, t := range tasks0 {
		s.NotEmpty(t, "Expected non empty task identifier.")
	}

	tasks1, err1 := s.getTasks(taskList, taskTypeActivity, time.Minute, 1)
	s.Nil(err1, "No error expected.")
	s.NotNil(tasks1, "expected valid list of tasks.")
	s.Equal(1, len(tasks1), "Expected 1 activity task.")

	t := tasks1[0]
	err3 := s.completeTask(workflowExecution, t.taskList, t.taskType, t.taskID, badToken)
	s.NotNil(err3)
	log.Infof("Failed to complete task '%v' using lock token '%v'.  Error: %v", t.taskID, badToken, err3)
	s.IsType(&conditionFailedError{}, err3)
}

func (s *workflowTestBase) createWorkflowExecution(workflowExecution workflow.WorkflowExecution, taskList string,
	history string, executionContext []byte, nextEventID int64, lastProcessedEventID int64, decisionScheduleID int64) (
	string, error) {
	response, err := s.workflowMgr.CreateWorkflowExecution(&createWorkflowExecutionRequest{
		execution:          workflowExecution,
		taskList:           taskList,
		history:            []byte(history),
		executionContext:   executionContext,
		nextEventID:        nextEventID,
		lastProcessedEvent: lastProcessedEventID,
		transferTasks:      []task{&decisionTask{taskList: taskList, scheduleID: decisionScheduleID}}})

	if err != nil {
		return "", err
	}

	return response.taskID, nil
}

func (s *workflowTestBase) createWorkflowExecutionManyTasks(workflowExecution workflow.WorkflowExecution,
	taskList string, history string, executionContext []byte, nextEventID int64, lastProcessedEventID int64,
	decisionScheduleIDs []int64, activityScheduleIDs []int64) (string, error) {

	transferTasks := []task{}
	for _, decisionScheduleID := range decisionScheduleIDs {
		transferTasks = append(transferTasks, &decisionTask{taskList: taskList, scheduleID: int64(decisionScheduleID)})
	}

	for _, activityScheduleID := range activityScheduleIDs {
		transferTasks = append(transferTasks, &activityTask{taskList: taskList, scheduleID: int64(activityScheduleID)})
	}

	response, err := s.workflowMgr.CreateWorkflowExecution(&createWorkflowExecutionRequest{
		execution:          workflowExecution,
		taskList:           taskList,
		history:            []byte(history),
		executionContext:   executionContext,
		nextEventID:        nextEventID,
		lastProcessedEvent: lastProcessedEventID,
		transferTasks:      transferTasks})

	if err != nil {
		return "", err
	}

	return response.taskID, nil
}

func (s *workflowTestBase) getWorkflowExecutionInfo(workflowExecution workflow.WorkflowExecution) (*workflowExecutionInfo,
	error) {
	response, err := s.workflowMgr.GetWorkflowExecution(&getWorkflowExecutionRequest{
		execution: workflowExecution,
	})
	if err != nil {
		return nil, err
	}

	return response.executionInfo, nil
}

func (s *workflowTestBase) updateWorkflowExecution(updatedInfo *workflowExecutionInfo, decisionScheduleIDs []int64,
	activityScheduleIDs []int64, condition int64) error {
	transferTasks := []task{}
	for _, decisionScheduleID := range decisionScheduleIDs {
		transferTasks = append(transferTasks, &decisionTask{taskList: updatedInfo.taskList,
			scheduleID: int64(decisionScheduleID)})
	}

	for _, activityScheduleID := range activityScheduleIDs {
		transferTasks = append(transferTasks, &activityTask{taskList: updatedInfo.taskList,
			scheduleID: int64(activityScheduleID)})
	}

	return s.workflowMgr.UpdateWorkflowExecution(&updateWorkflowExecutionRequest{
		executionInfo: updatedInfo,
		transferTasks: transferTasks,
		condition:     int64(3),
	})
}

func (s *workflowTestBase) deleteWorkflowExecution(workflowExecution workflow.WorkflowExecution, condition int64) error {
	return s.workflowMgr.DeleteWorkflowExecution(&deleteWorkflowExecutionRequest{
		execution: workflowExecution,
		condition: condition,
	})
}

func (s *workflowTestBase) getTransferTasks(timeout time.Duration, batchSize int) ([]*taskInfo, error) {
	response, err := s.workflowMgr.GetTransferTasks(&getTransferTasksRequest{
		lockTimeout: timeout,
		batchSize:   batchSize,
	})

	if err != nil {
		return nil, err
	}

	return response.tasks, nil
}

func (s *workflowTestBase) completeTransferTask(workflowExecution workflow.WorkflowExecution, taskID string,
	lockToken string) error {

	return s.workflowMgr.CompleteTransferTask(&completeTransferTaskRequest{
		execution: workflowExecution,
		taskID:    taskID,
		lockToken: lockToken,
	})
}

func (s *workflowTestBase) createDecisionTask(workflowExecution workflow.WorkflowExecution, taskList string,
	decisionScheduleID int64) (string, error) {
	response, err := s.taskMgr.CreateTask(&createTaskRequest{
		execution: workflowExecution,
		taskList:  taskList,
		data: &decisionTask{
			taskList:   taskList,
			scheduleID: decisionScheduleID,
		},
	})

	if err != nil {
		return "", err
	}

	return response.taskID, nil
}

func (s *workflowTestBase) createActivityTasks(workflowExecution workflow.WorkflowExecution, activities map[int64]string) (
	[]string, error) {
	var taskIDs []string
	for activityScheduleID, taskList := range activities {
		response, err := s.taskMgr.CreateTask(&createTaskRequest{
			execution: workflowExecution,
			taskList:  taskList,
			data: &activityTask{
				taskList:   taskList,
				scheduleID: activityScheduleID,
			},
		})

		if err != nil {
			return nil, err
		}

		taskIDs = append(taskIDs, response.taskID)
	}

	return taskIDs, nil
}

func (s *workflowTestBase) getTasks(taskList string, taskType int, timeout time.Duration, batchSize int) ([]*taskInfo,
	error) {
	response, err := s.taskMgr.GetTasks(&getTasksRequest{
		taskList:    taskList,
		taskType:    taskType,
		lockTimeout: timeout,
		batchSize:   batchSize,
	})

	if err != nil {
		return nil, err
	}

	return response.tasks, nil
}

func (s *workflowTestBase) completeTask(workflowExecution workflow.WorkflowExecution, taskList string,
	taskType int, taskID string, lockToken string) error {

	return s.taskMgr.CompleteTask(&completeTaskRequest{
		execution: workflowExecution,
		taskList:  taskList,
		taskType:  taskType,
		taskID:    taskID,
		lockToken: lockToken,
	})
}

func (s *workflowTestBase) clearTransferQueue() {
	tasks, err := s.getTransferTasks(time.Minute, 100)
	if err != nil {
		for _, t := range tasks {
			e := workflow.WorkflowExecution{WorkflowId: common.StringPtr(t.workflowID), RunId: common.StringPtr(t.runID)}
			s.completeTransferTask(e, t.taskID, t.lockToken)
		}
	}
}

func validateTimeRange(t time.Time, expectedDuration time.Duration) bool {
	currentTime := time.Now()
	diff := time.Duration(currentTime.UnixNano() - t.UnixNano())
	if diff > expectedDuration {
		log.Infof("Current time: %v, Application time: %v, Differenrce: %v", currentTime, t, diff)
		return false
	}
	return true
}

func generateRandomKeyspace(n int) string {
	rand.Seed(time.Now().UnixNano())
	letterRunes := []rune("workflow")
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

func (s *workflowTestBase) setupWorkflowStore() {
	// Setup Workflow keyspace and deploy schema for tests
	s.cassandraTestCluster.setupTestCluster()
	var err error
	s.workflowMgr, err = newCassandraWorkflowExecutionPersistence(testWorkflowClusterHosts,
		s.cassandraTestCluster.keyspace)
	s.taskMgr, err = newCassandraTaskPersistence(testWorkflowClusterHosts, s.cassandraTestCluster.keyspace)
	if err != nil {
		log.Fatal(err)
	}
}

func (s *workflowTestBase) tearDownWorkflowStore() {
	s.cassandraTestCluster.tearDownTestCluster()
}

func (s *cassandraTestCluster) setupTestCluster() {
	s.createCluster(testWorkflowClusterHosts, gocql.Consistency(1), generateRandomKeyspace(10))
	s.createKeyspace(1)
	s.loadSchema("workflow_test.cql")
}

func (s *cassandraTestCluster) tearDownTestCluster() {
	s.dropKeyspace()
	s.session.Close()
}

func (s *cassandraTestCluster) createCluster(clusterHosts string, cons gocql.Consistency, keyspace string) {
	s.cluster = common.NewCassandraCluster(clusterHosts)
	s.cluster.Consistency = cons
	s.cluster.Keyspace = "system"
	s.cluster.Timeout = 40 * time.Second
	var err error
	s.session, err = s.cluster.CreateSession()
	if err != nil {
		log.WithField(common.TagErr, err).Fatal(`createSession`)
	}
	s.keyspace = keyspace
}

func (s *cassandraTestCluster) createKeyspace(replicas int) {
	err := common.CreateCassandraKeyspace(s.session, s.keyspace, replicas, true)
	if err != nil {
		log.Fatal(err)
	}

	s.cluster.Keyspace = s.keyspace
}

func (s *cassandraTestCluster) dropKeyspace() {
	err := common.DropCassandraKeyspace(s.session, s.keyspace)
	if err != nil {
		log.Fatal(err)
	}
}

func (s *cassandraTestCluster) loadSchema(fileName string) {
	err := common.LoadCassandraSchema("../../../cassandra/bin/cqlsh", fileName, s.keyspace)
	if err != nil {
		err = common.LoadCassandraSchema("../../../cassandra/bin/cqlsh", "../../schema/workflow/"+fileName, s.keyspace)
	}

	if err != nil {
		log.Fatal(err)
	}
}

func copyWorkflowExecutionInfo(sourceInfo *workflowExecutionInfo) *workflowExecutionInfo {
	return &workflowExecutionInfo{
		workflowID:           sourceInfo.workflowID,
		runID:                sourceInfo.runID,
		taskList:             sourceInfo.taskList,
		history:              sourceInfo.history,
		executionContext:     sourceInfo.executionContext,
		state:                sourceInfo.state,
		nextEventID:          sourceInfo.nextEventID,
		lastProcessedEvent:   sourceInfo.lastProcessedEvent,
		lastUpdatedTimestamp: sourceInfo.lastUpdatedTimestamp,
		decisionPending:      sourceInfo.decisionPending,
	}
}
