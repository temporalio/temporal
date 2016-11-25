package workflow

import (
	"math/rand"
	"time"

	workflow "code.uber.internal/devexp/minions/.gen/go/minions"
	"code.uber.internal/devexp/minions/common"
	"code.uber.internal/go-common.git/x/log"
	"github.com/gocql/gocql"
)

const (
	testWorkflowClusterHosts = "127.0.0.1"
)

type (
	// WorkflowTestBaseOptions options to configure workflow test base.
	WorkflowTestBaseOptions struct {
		ClusterHost string
	}

	// WorkflowTestBase wraps the base setup needed to create workflows over engine layer.
	WorkflowTestBase struct {
		WorkflowMgr workflowExecutionPersistence
		TaskMgr     taskPersistence
		cassandraTestCluster
	}

	cassandraTestCluster struct {
		keyspace string
		cluster  *gocql.ClusterConfig
		session  *gocql.Session
	}
)

// SetupWorkflowStoreWithOptions to setup workflow test base
func (s *WorkflowTestBase) SetupWorkflowStoreWithOptions(options WorkflowTestBaseOptions) {
	// Setup Workflow keyspace and deploy schema for tests
	s.cassandraTestCluster.setupTestCluster()
	var err error
	s.WorkflowMgr, err = newCassandraWorkflowExecutionPersistence(options.ClusterHost,
		s.cassandraTestCluster.keyspace)
	s.TaskMgr, err = newCassandraTaskPersistence(options.ClusterHost, s.cassandraTestCluster.keyspace)
	if err != nil {
		log.Fatal(err)
	}
}

func (s *WorkflowTestBase) createWorkflowExecution(workflowExecution workflow.WorkflowExecution, taskList string,
	history string, executionContext []byte, nextEventID int64, lastProcessedEventID int64, decisionScheduleID int64) (
	string, error) {
	response, err := s.WorkflowMgr.CreateWorkflowExecution(&createWorkflowExecutionRequest{
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

func (s *WorkflowTestBase) createWorkflowExecutionManyTasks(workflowExecution workflow.WorkflowExecution,
	taskList string, history string, executionContext []byte, nextEventID int64, lastProcessedEventID int64,
	decisionScheduleIDs []int64, activityScheduleIDs []int64) (string, error) {

	transferTasks := []task{}
	for _, decisionScheduleID := range decisionScheduleIDs {
		transferTasks = append(transferTasks, &decisionTask{taskList: taskList, scheduleID: int64(decisionScheduleID)})
	}

	for _, activityScheduleID := range activityScheduleIDs {
		transferTasks = append(transferTasks, &activityTask{taskList: taskList, scheduleID: int64(activityScheduleID)})
	}

	response, err := s.WorkflowMgr.CreateWorkflowExecution(&createWorkflowExecutionRequest{
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

func (s *WorkflowTestBase) getWorkflowExecutionInfo(workflowExecution workflow.WorkflowExecution) (*workflowExecutionInfo,
	error) {
	response, err := s.WorkflowMgr.GetWorkflowExecution(&getWorkflowExecutionRequest{
		execution: workflowExecution,
	})
	if err != nil {
		return nil, err
	}

	return response.executionInfo, nil
}

func (s *WorkflowTestBase) updateWorkflowExecution(updatedInfo *workflowExecutionInfo, decisionScheduleIDs []int64,
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

	return s.WorkflowMgr.UpdateWorkflowExecution(&updateWorkflowExecutionRequest{
		executionInfo: updatedInfo,
		transferTasks: transferTasks,
		condition:     int64(3),
	})
}

func (s *WorkflowTestBase) deleteWorkflowExecution(workflowExecution workflow.WorkflowExecution, condition int64) error {
	return s.WorkflowMgr.DeleteWorkflowExecution(&deleteWorkflowExecutionRequest{
		execution: workflowExecution,
		condition: condition,
	})
}

func (s *WorkflowTestBase) getTransferTasks(timeout time.Duration, batchSize int) ([]*taskInfo, error) {
	response, err := s.WorkflowMgr.GetTransferTasks(&getTransferTasksRequest{
		lockTimeout: timeout,
		batchSize:   batchSize,
	})

	if err != nil {
		return nil, err
	}

	return response.tasks, nil
}

func (s *WorkflowTestBase) completeTransferTask(workflowExecution workflow.WorkflowExecution, taskID string,
	lockToken string) error {

	return s.WorkflowMgr.CompleteTransferTask(&completeTransferTaskRequest{
		execution: workflowExecution,
		taskID:    taskID,
		lockToken: lockToken,
	})
}

func (s *WorkflowTestBase) createDecisionTask(workflowExecution workflow.WorkflowExecution, taskList string,
	decisionScheduleID int64) (string, error) {
	response, err := s.TaskMgr.CreateTask(&createTaskRequest{
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

func (s *WorkflowTestBase) createActivityTasks(workflowExecution workflow.WorkflowExecution, activities map[int64]string) (
	[]string, error) {
	var taskIDs []string
	for activityScheduleID, taskList := range activities {
		response, err := s.TaskMgr.CreateTask(&createTaskRequest{
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

func (s *WorkflowTestBase) getTasks(taskList string, taskType int, timeout time.Duration, batchSize int) ([]*taskInfo,
	error) {
	response, err := s.TaskMgr.GetTasks(&getTasksRequest{
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

func (s *WorkflowTestBase) completeTask(workflowExecution workflow.WorkflowExecution, taskList string,
	taskType int, taskID string, lockToken string) error {

	return s.TaskMgr.CompleteTask(&completeTaskRequest{
		execution: workflowExecution,
		taskList:  taskList,
		taskType:  taskType,
		taskID:    taskID,
		lockToken: lockToken,
	})
}

func (s *WorkflowTestBase) clearTransferQueue() {
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

func (s *WorkflowTestBase) setupWorkflowStore() {
	s.SetupWorkflowStoreWithOptions(WorkflowTestBaseOptions{ClusterHost: testWorkflowClusterHosts})
}

func (s *WorkflowTestBase) tearDownWorkflowStore() {
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
	err := common.LoadCassandraSchema("./cassandra/bin/cqlsh", "./schema/"+fileName, s.keyspace)
	if err != nil {
		err = common.LoadCassandraSchema("../cassandra/bin/cqlsh", "../schema/"+fileName, s.keyspace)
	}

	if err != nil {
		log.Fatal(err)
	}
}
