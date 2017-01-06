package persistence

import (
	"fmt"
	"math/rand"
	"strings"
	"sync/atomic"
	"time"

	workflow "code.uber.internal/devexp/minions/.gen/go/shared"
	"code.uber.internal/devexp/minions/common"
	log "github.com/Sirupsen/logrus"
	"github.com/gocql/gocql"
)

const (
	testWorkflowClusterHosts = "127.0.0.1"
)

type (
	// TestBaseOptions options to configure workflow test base.
	TestBaseOptions struct {
		ClusterHost  string
		KeySpace     string
		DropKeySpace bool
		SchemaDir    string
	}

	// TestBase wraps the base setup needed to create workflows over engine layer.
	TestBase struct {
		WorkflowMgr  ExecutionManager
		TaskMgr      TaskManager
		ShardInfo    *ShardInfo
		ShardContext *testShardContext
		readLevel    int64
		CassandraTestCluster
	}

	// CassandraTestCluster allows executing cassandra operations in testing.
	CassandraTestCluster struct {
		keyspace string
		cluster  *gocql.ClusterConfig
		session  *gocql.Session
	}

	testShardContext struct {
		shardInfo              *ShardInfo
		transferSequenceNumber int64
		timerSequeceNumber     int64
	}
)

func newTestShardContext(shardInfo *ShardInfo, transferSequenceNumber int64) *testShardContext {
	return &testShardContext{
		shardInfo:              shardInfo,
		transferSequenceNumber: transferSequenceNumber,
	}
}

func (s *testShardContext) GetTransferTaskID() int64 {
	return atomic.AddInt64(&s.transferSequenceNumber, 1)
}

func (s *testShardContext) GetRangeID() int64 {
	return atomic.LoadInt64(&s.shardInfo.RangeID)
}

func (s *testShardContext) GetTransferAckLevel() int64 {
	return atomic.LoadInt64(&s.shardInfo.TransferAckLevel)
}

func (s *testShardContext) GetTimerSequenceNumber() int64 {
	return atomic.AddInt64(&s.timerSequeceNumber, 1)
}

func (s *testShardContext) UpdateAckLevel(ackLevel int64) error {
	atomic.StoreInt64(&s.shardInfo.TransferAckLevel, ackLevel)
	return nil
}

func (s *testShardContext) Reset() {
	atomic.StoreInt64(&s.shardInfo.RangeID, 100)
	atomic.StoreInt64(&s.shardInfo.TransferAckLevel, 0)
}

// SetupWorkflowStoreWithOptions to setup workflow test base
func (s *TestBase) SetupWorkflowStoreWithOptions(options TestBaseOptions) {
	// Setup Workflow keyspace and deploy schema for tests
	s.CassandraTestCluster.setupTestCluster(options.KeySpace, options.DropKeySpace, options.SchemaDir)
	var err error
	s.WorkflowMgr, err = NewCassandraWorkflowExecutionPersistence(options.ClusterHost,
		s.CassandraTestCluster.keyspace, 1)
	if err != nil {
		log.Fatal(err)
	}
	s.TaskMgr, err = NewCassandraTaskPersistence(options.ClusterHost, s.CassandraTestCluster.keyspace)
	if err != nil {
		log.Fatal(err)
	}
	// Create a shard for test
	s.readLevel = 0
	s.ShardInfo = &ShardInfo{
		ShardID:          1,
		RangeID:          100,
		TransferAckLevel: 0,
	}
	s.ShardContext = newTestShardContext(s.ShardInfo, 0)
	err1 := s.WorkflowMgr.CreateShard(&CreateShardRequest{
		ShardInfo: s.ShardInfo,
	})
	if err1 != nil {
		log.Fatal(err1)
	}
}

// CreateWorkflowExecution is a utility method to create workflow executions
func (s *TestBase) CreateWorkflowExecution(workflowExecution workflow.WorkflowExecution, taskList string,
	history string, executionContext []byte, nextEventID int64, lastProcessedEventID int64, decisionScheduleID int64,
	timerTasks []Task) (
	string, error) {
	response, err := s.WorkflowMgr.CreateWorkflowExecution(&CreateWorkflowExecutionRequest{
		Execution:          workflowExecution,
		TaskList:           taskList,
		History:            []byte(history),
		ExecutionContext:   executionContext,
		NextEventID:        nextEventID,
		LastProcessedEvent: lastProcessedEventID,
		RangeID:            s.ShardContext.GetRangeID(),
		TransferTasks: []Task{
			&DecisionTask{TaskID: s.GetNextSequenceNumber(), TaskList: taskList, ScheduleID: decisionScheduleID},
		},
		TimerTasks: timerTasks})

	if err != nil {
		return "", err
	}

	return response.TaskID, nil
}

// CreateWorkflowExecutionManyTasks is a utility method to create workflow executions
func (s *TestBase) CreateWorkflowExecutionManyTasks(workflowExecution workflow.WorkflowExecution,
	taskList string, history string, executionContext []byte, nextEventID int64, lastProcessedEventID int64,
	decisionScheduleIDs []int64, activityScheduleIDs []int64) (string, error) {

	transferTasks := []Task{}
	for _, decisionScheduleID := range decisionScheduleIDs {
		transferTasks = append(transferTasks,
			&DecisionTask{TaskID: s.GetNextSequenceNumber(), TaskList: taskList, ScheduleID: int64(decisionScheduleID)})
	}

	for _, activityScheduleID := range activityScheduleIDs {
		transferTasks = append(transferTasks,
			&ActivityTask{TaskID: s.GetNextSequenceNumber(), TaskList: taskList, ScheduleID: int64(activityScheduleID)})
	}

	response, err := s.WorkflowMgr.CreateWorkflowExecution(&CreateWorkflowExecutionRequest{
		Execution:          workflowExecution,
		TaskList:           taskList,
		History:            []byte(history),
		ExecutionContext:   executionContext,
		NextEventID:        nextEventID,
		LastProcessedEvent: lastProcessedEventID,
		TransferTasks:      transferTasks,
		RangeID:            s.ShardContext.GetRangeID()})

	if err != nil {
		return "", err
	}

	return response.TaskID, nil
}

// GetWorkflowExecutionInfo is a utility method to retrieve execution info
func (s *TestBase) GetWorkflowExecutionInfo(workflowExecution workflow.WorkflowExecution) (*WorkflowExecutionInfo,
	error) {
	response, err := s.WorkflowMgr.GetWorkflowExecution(&GetWorkflowExecutionRequest{
		Execution: workflowExecution,
	})
	if err != nil {
		return nil, err
	}

	return response.ExecutionInfo, nil
}

// UpdateWorkflowExecution is a utility method to update workflow execution
func (s *TestBase) UpdateWorkflowExecution(updatedInfo *WorkflowExecutionInfo, decisionScheduleIDs []int64,
	activityScheduleIDs []int64, condition int64, timerTasks []Task, deleteTimerTask Task) error {
	transferTasks := []Task{}
	for _, decisionScheduleID := range decisionScheduleIDs {
		transferTasks = append(transferTasks, &DecisionTask{TaskList: updatedInfo.TaskList,
			ScheduleID: int64(decisionScheduleID)})
	}

	for _, activityScheduleID := range activityScheduleIDs {
		transferTasks = append(transferTasks, &ActivityTask{TaskList: updatedInfo.TaskList,
			ScheduleID: int64(activityScheduleID)})
	}

	return s.WorkflowMgr.UpdateWorkflowExecution(&UpdateWorkflowExecutionRequest{
		ExecutionInfo:   updatedInfo,
		TransferTasks:   transferTasks,
		TimerTasks:      timerTasks,
		Condition:       condition,
		DeleteTimerTask: deleteTimerTask,
		RangeID:         s.ShardContext.GetRangeID(),
	})
}

// DeleteWorkflowExecution is a utility method to delete a workflow execution
func (s *TestBase) DeleteWorkflowExecution(info *WorkflowExecutionInfo) error {
	return s.WorkflowMgr.DeleteWorkflowExecution(&DeleteWorkflowExecutionRequest{
		ExecutionInfo: info,
	})
}

// GetTransferTasks is a utility method to get tasks from transfer task queue
func (s *TestBase) GetTransferTasks(batchSize int) ([]*TaskInfo, error) {
	response, err := s.WorkflowMgr.GetTransferTasks(&GetTransferTasksRequest{
		ReadLevel: s.GetReadLevel(),
		BatchSize: batchSize,
		RangeID:   s.ShardContext.GetRangeID(),
	})

	if err != nil {
		return nil, err
	}

	for _, task := range response.Tasks {
		atomic.StoreInt64(&s.readLevel, task.TaskID)
	}

	return response.Tasks, nil
}

// CompleteTransferTask is a utility method to complete a transfer task
func (s *TestBase) CompleteTransferTask(workflowExecution workflow.WorkflowExecution, taskID int64) error {

	return s.WorkflowMgr.CompleteTransferTask(&CompleteTransferTaskRequest{
		Execution: workflowExecution,
		TaskID:    taskID,
	})
}

// GetTimerIndexTasks is a utility method to get tasks from transfer task queue
func (s *TestBase) GetTimerIndexTasks(minKey int64, maxKey int64) ([]*TimerInfo, error) {
	response, err := s.WorkflowMgr.GetTimerIndexTasks(&GetTimerIndexTasksRequest{
		MinKey: minKey, MaxKey: maxKey, BatchSize: 10})

	if err != nil {
		return nil, err
	}

	return response.Timers, nil
}

// CreateDecisionTask is a utility method to create a task
func (s *TestBase) CreateDecisionTask(workflowExecution workflow.WorkflowExecution, taskList string,
	decisionScheduleID int64) (string, error) {
	response, err := s.TaskMgr.CreateTask(&CreateTaskRequest{
		Execution: workflowExecution,
		TaskList:  taskList,
		Data: &DecisionTask{
			TaskID:     s.GetNextSequenceNumber(),
			TaskList:   taskList,
			ScheduleID: decisionScheduleID,
		},
	})

	if err != nil {
		return "", err
	}

	return response.TaskID, nil
}

// CreateActivityTasks is a utility method to create tasks
func (s *TestBase) CreateActivityTasks(workflowExecution workflow.WorkflowExecution, activities map[int64]string) (
	[]string, error) {
	var taskIDs []string
	sequenceNum := 1
	for activityScheduleID, taskList := range activities {
		response, err := s.TaskMgr.CreateTask(&CreateTaskRequest{
			Execution: workflowExecution,
			TaskList:  taskList,
			Data: &ActivityTask{
				TaskID:     s.GetNextSequenceNumber(),
				TaskList:   taskList,
				ScheduleID: activityScheduleID,
			},
		})

		if err != nil {
			return nil, err
		}

		taskIDs = append(taskIDs, response.TaskID)
		sequenceNum++
	}

	return taskIDs, nil
}

// GetTasks is a utility method to get tasks from persistence
func (s *TestBase) GetTasks(taskList string, taskType int, timeout time.Duration, batchSize int) ([]*TaskInfoWithID,
	error) {
	response, err := s.TaskMgr.GetTasks(&GetTasksRequest{
		TaskList:    taskList,
		TaskType:    taskType,
		LockTimeout: timeout,
		BatchSize:   batchSize,
	})

	if err != nil {
		return nil, err
	}

	return response.Tasks, nil
}

// CompleteTask is a utility method to complete a task
func (s *TestBase) CompleteTask(workflowExecution workflow.WorkflowExecution, taskList string,
	taskType int, taskID string, lockToken string) error {

	return s.TaskMgr.CompleteTask(&CompleteTaskRequest{
		Execution: workflowExecution,
		TaskList:  taskList,
		TaskType:  taskType,
		TaskID:    taskID,
		LockToken: lockToken,
	})
}

// ClearTransferQueue completes all tasks in transfer queue
func (s *TestBase) ClearTransferQueue() {
	log.Infof("Clearing transfer tasks (RangeID: %v, ReadLevel: %v, AckLevel: %v)", s.ShardContext.GetRangeID(),
		s.GetReadLevel(), s.ShardContext.GetTransferAckLevel())
	tasks, err := s.GetTransferTasks(100)
	if err != nil {
		log.Fatalf("Error during cleanup: %v", err)
	}

	counter := 0
	for _, t := range tasks {
		log.Infof("Deleting transfer task with ID: %v", t.TaskID)
		e := workflow.WorkflowExecution{WorkflowId: common.StringPtr(t.WorkflowID), RunId: common.StringPtr(t.RunID)}
		s.CompleteTransferTask(e, t.TaskID)
		counter++
	}

	log.Infof("Deleted '%v' transfer tasks.", counter)
	s.ShardContext.Reset()
	atomic.StoreInt64(&s.readLevel, 0)
}

// SetupWorkflowStore to setup workflow test base
func (s *TestBase) SetupWorkflowStore() {
	s.SetupWorkflowStoreWithOptions(TestBaseOptions{ClusterHost: testWorkflowClusterHosts, DropKeySpace: true})
}

// TearDownWorkflowStore to cleanup
func (s *TestBase) TearDownWorkflowStore() {
	s.CassandraTestCluster.tearDownTestCluster()
}

// GetNextSequenceNumber generates a unique sequence number for can be used for transfer queue taskId
func (s *TestBase) GetNextSequenceNumber() int64 {
	return s.ShardContext.GetTransferTaskID()
}

// GetReadLevel returns the current read level for shard
func (s *TestBase) GetReadLevel() int64 {
	return atomic.LoadInt64(&s.readLevel)
}

func (s *CassandraTestCluster) setupTestCluster(keySpace string, dropKeySpace bool, schemaDir string) {
	if keySpace == "" {
		keySpace = generateRandomKeyspace(10)
	}
	s.createCluster(testWorkflowClusterHosts, gocql.Consistency(1), keySpace)
	s.createKeyspace(1, dropKeySpace)
	s.loadSchema("workflow_test.cql", schemaDir)
}

func (s *CassandraTestCluster) tearDownTestCluster() {
	s.dropKeyspace()
	s.session.Close()
}

func (s *CassandraTestCluster) createCluster(clusterHosts string, cons gocql.Consistency, keyspace string) {
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

func (s *CassandraTestCluster) createKeyspace(replicas int, dropKeySpace bool) {
	err := common.CreateCassandraKeyspace(s.session, s.keyspace, replicas, dropKeySpace)
	if err != nil {
		log.Fatal(err)
	}

	s.cluster.Keyspace = s.keyspace
}

func (s *CassandraTestCluster) dropKeyspace() {
	err := common.DropCassandraKeyspace(s.session, s.keyspace)
	if err != nil && !strings.Contains(err.Error(), "AlreadyExists") {
		log.Fatal(err)
	}
}

func (s *CassandraTestCluster) loadSchema(fileName string, schemaDir string) {

	cqlshDir := "./cassandra/bin/cqlsh"
	workflowSchemaDir := "./schema/"

	if schemaDir != "" {
		cqlshDir = schemaDir + "/cassandra/bin/cqlsh"
		workflowSchemaDir = schemaDir + "/schema/"
	}

	fmt.Printf("schemaDir: %s, cqlshDir: %s, workflowSchemaDir: %s \n", schemaDir, cqlshDir, workflowSchemaDir)

	err := common.LoadCassandraSchema(cqlshDir, workflowSchemaDir+fileName, s.keyspace)

	if err != nil && !strings.Contains(err.Error(), "AlreadyExists") {
		err = common.LoadCassandraSchema("../cassandra/bin/cqlsh", "../schema/"+fileName, s.keyspace)
	}

	if err != nil && !strings.Contains(err.Error(), "AlreadyExists") {
		log.Fatal(err)
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
