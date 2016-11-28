package workflow

import (
	"fmt"
	"strings"
	"time"

	"github.com/gocql/gocql"
	"github.com/pborman/uuid"

	workflow "code.uber.internal/devexp/minions/.gen/go/minions"
	"code.uber.internal/devexp/minions/common"
)

const (
	cassandraProtoVersion    = 4
	defaultSessionTimeout    = 10 * time.Second
	rowTypeExecutionTaskUUID = "1e26b948-4fc9-f203-f5a7-ac2109dbfbe4"
)

// Row types for table executions
const (
	rowTypeExecution = iota
	rowTypeTransferTask
	rowTypeTimerTask
)

const (
	templateWorkflowExecutionType = `{` +
		`workflow_id: ?, ` +
		`run_id: ?, ` +
		`task_list: ?, ` +
		`history: ?, ` +
		`execution_context: ?, ` +
		`state: ?, ` +
		`next_event_id: ?, ` +
		`last_processed_event: ?, ` +
		`last_updated_time: ?, ` +
		`decision_pending: ?` +
		`}`

	templateTaskType = `{` +
		`workflow_id: ?, ` +
		`run_id: ?, ` +
		`task_id: ?, ` +
		`task_list: ?, ` +
		`type: ?, ` +
		`schedule_id: ?, ` +
		`visibility_time: ?, ` +
		`lock_token: ?, ` +
		`delivery_count: ?` +
		`}`

	templateCreateWorkflowExecutionQuery = `INSERT INTO executions (` +
		`shard_id, workflow_id, type, execution, next_event_id, task_id) ` +
		`VALUES(?, ?, ?, ` + templateWorkflowExecutionType + `, ?, ?) IF NOT EXISTS`

	templateCreateTransferTaskQuery = `INSERT INTO executions (` +
		`shard_id, workflow_id, type, transfer, task_id, lock_token) ` +
		`VALUES(?, ?, ?, ` + templateTaskType + `, ?, ?)`

	templateGetWorkflowExecutionQuery = `SELECT execution ` +
		`FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and workflow_id = ? ` +
		`and task_id = ?`

	templateUpdateWorkflowExecutionQuery = `UPDATE executions ` +
		`SET execution = ` + templateWorkflowExecutionType + `, next_event_id = ? ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and workflow_id = ? ` +
		`and task_id = ? ` +
		`IF next_event_id = ?`

	templateDeleteWorkflowExecutionQuery = `DELETE FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and workflow_id = ? ` +
		`and task_id = ? ` +
		`IF next_event_id = ?`

	templateGetTransferTasksQuery = `SELECT transfer ` +
		`FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ?`

	templateLockTransferTaskQuery = `UPDATE executions ` +
		`SET transfer = ` + templateTaskType + `, lock_token = ? ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and workflow_id = ? ` +
		`and task_id = ? ` +
		`IF lock_token = ?`

	templateCompleteTransferTaskQuery = `DELETE FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and workflow_id = ? ` +
		`and task_id = ?` +
		`IF lock_token = ?`

	templateCreateTaskQuery = `INSERT INTO tasks (` +
		`task_list, type, workflow_id, run_id, task_id, task, lock_token) ` +
		`VALUES(?, ?, ?, ?, ?, ` + templateTaskType + `, ?)`

	templateGetTasksQuery = `SELECT task ` +
		`FROM tasks ` +
		`WHERE task_list = ? ` +
		`and type = ? `

	templateLockTaskQuery = `UPDATE tasks ` +
		`SET task = ` + templateTaskType + `, lock_token = ? ` +
		`WHERE task_list = ? ` +
		`and type = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and task_id = ? ` +
		`IF lock_token = ?`

	templateCompleteTaskQuery = `DELETE FROM tasks ` +
		`WHERE task_list = ? ` +
		`and type = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and task_id = ?` +
		`IF lock_token = ?`
)

type (
	cassandraPersistence struct {
		session      *gocql.Session
		lowConslevel gocql.Consistency
		shardID      int
	}
)

// NewCassandraWorkflowExecutionPersistence is used to create an instance of workflowExecutionPersistence implementation
func NewCassandraWorkflowExecutionPersistence(hosts string, keyspace string) (ExecutionPersistence, error) {
	cluster := common.NewCassandraCluster(hosts)
	cluster.Keyspace = keyspace
	cluster.ProtoVersion = cassandraProtoVersion
	cluster.Consistency = gocql.Quorum
	cluster.Timeout = defaultSessionTimeout

	session, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}
	return &cassandraPersistence{shardID: 1, session: session, lowConslevel: gocql.One}, nil
}

// NewCassandraTaskPersistence is used to create an instance of taskPersistence implementation
func NewCassandraTaskPersistence(hosts string, keyspace string) (TaskPersistence, error) {
	cluster := common.NewCassandraCluster(hosts)
	cluster.Keyspace = keyspace
	cluster.ProtoVersion = cassandraProtoVersion
	cluster.Consistency = gocql.Quorum
	cluster.Timeout = defaultSessionTimeout

	session, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}
	return &cassandraPersistence{shardID: -1, session: session, lowConslevel: gocql.One}, nil
}

func (d *cassandraPersistence) CreateWorkflowExecution(request *createWorkflowExecutionRequest) (
	*createWorkflowExecutionResponse, error) {
	cqlNowTimestamp := common.UnixNanoToCQLTimestamp(time.Now().UnixNano())
	transferTaskID := uuid.New()

	batch := d.session.NewBatch(gocql.LoggedBatch)
	batch.Query(templateCreateWorkflowExecutionQuery,
		d.shardID,
		request.execution.GetWorkflowId(),
		rowTypeExecution,
		request.execution.GetWorkflowId(),
		request.execution.GetRunId(),
		request.taskList,
		request.history,
		request.executionContext,
		workflowStateCreated,
		request.nextEventID,
		request.lastProcessedEvent,
		cqlNowTimestamp,
		true,
		request.nextEventID,
		rowTypeExecutionTaskUUID)

	d.createTransferTasks(batch, request.transferTasks, request.execution.GetWorkflowId(), request.execution.GetRunId(),
		cqlNowTimestamp)

	previous := make(map[string]interface{})
	applied, _, err := d.session.MapExecuteBatchCAS(batch, previous)
	if err != nil {
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("CreateWorkflowExecution operation failed. Error: %v", err),
		}
	}

	if !applied {
		execution := previous["execution"].(map[string]interface{})
		return nil, &workflow.WorkflowExecutionAlreadyStartedError{
			Message: fmt.Sprintf("Workflow execution already running. WorkflowId: %v, RunId: %v",
				execution["workflow_id"], execution["run_id"]),
		}
	}

	return &createWorkflowExecutionResponse{taskID: transferTaskID}, nil
}

func (d *cassandraPersistence) GetWorkflowExecution(request *getWorkflowExecutionRequest) (
	*getWorkflowExecutionResponse, error) {
	execution := request.execution
	query := d.session.Query(templateGetWorkflowExecutionQuery,
		d.shardID,
		rowTypeExecution,
		execution.GetWorkflowId(),
		rowTypeExecutionTaskUUID).Consistency(d.lowConslevel)

	result := make(map[string]interface{})
	if err := query.MapScan(result); err != nil {
		if err == gocql.ErrNotFound {
			return nil, &workflow.EntityNotExistsError{
				Message: fmt.Sprintf("Workflow execution not found.  WorkflowId: %v, RunId: %v", execution.GetWorkflowId(),
					execution.GetRunId()),
			}
		}

		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("GetWorkflowExecution operation failed. Error: %v", err),
		}
	}

	info := createWorkflowExecutionInfo(result["execution"].(map[string]interface{}))

	return &getWorkflowExecutionResponse{executionInfo: info}, nil
}

func (d *cassandraPersistence) UpdateWorkflowExecution(request *updateWorkflowExecutionRequest) error {
	executionInfo := request.executionInfo
	cqlNowTimestamp := common.UnixNanoToCQLTimestamp(time.Now().UnixNano())

	batch := d.session.NewBatch(gocql.LoggedBatch)
	batch.Query(templateUpdateWorkflowExecutionQuery,
		executionInfo.workflowID,
		executionInfo.runID,
		executionInfo.taskList,
		executionInfo.history,
		executionInfo.executionContext,
		executionInfo.state,
		executionInfo.nextEventID,
		executionInfo.lastProcessedEvent,
		cqlNowTimestamp,
		executionInfo.decisionPending,
		executionInfo.nextEventID,
		d.shardID,
		rowTypeExecution,
		executionInfo.workflowID,
		rowTypeExecutionTaskUUID,
		request.condition)

	d.createTransferTasks(batch, request.transferTasks, executionInfo.workflowID, executionInfo.runID, cqlNowTimestamp)

	previous := make(map[string]interface{})
	applied, _, err := d.session.MapExecuteBatchCAS(batch, previous)
	if err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("UpdateWorkflowExecution operation failed. Error: %v", err),
		}
	}

	if !applied {
		var columns []string
		for k, v := range previous {
			columns = append(columns, fmt.Sprintf("%s=%v", k, v))
		}

		return &conditionFailedError{
			msg: fmt.Sprintf("Failed to update workflow execution.  condition: %v, columns: (%v)",
				request.condition, strings.Join(columns, ",")),
		}
	}

	return nil
}

func (d *cassandraPersistence) DeleteWorkflowExecution(request *deleteWorkflowExecutionRequest) error {
	execution := request.execution
	query := d.session.Query(templateDeleteWorkflowExecutionQuery,
		d.shardID,
		rowTypeExecution,
		execution.GetWorkflowId(),
		rowTypeExecutionTaskUUID,
		request.condition)

	previous := make(map[string]interface{})
	applied, err := query.MapScanCAS(previous)
	if err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("DeleteWorkflowExecution operation failed. Error: %v", err),
		}
	}

	if !applied {
		var columns []string
		for k, v := range previous {
			columns = append(columns, fmt.Sprintf("%s=%v", k, v))
		}
		return &conditionFailedError{
			msg: fmt.Sprintf("Failed to delete workflow execution.  condition: %v, columns: (%v)", request.condition,
				strings.Join(columns, ",")),
		}
	}

	return nil
}

func (d *cassandraPersistence) GetTransferTasks(request *getTransferTasksRequest) (*getTransferTasksResponse, error) {
	currentTimestamp := time.Now()

	query := d.session.Query(templateGetTransferTasksQuery,
		d.shardID,
		rowTypeTransferTask).Consistency(d.lowConslevel)

	iter := query.Iter()
	if iter == nil {
		return nil, &workflow.InternalServiceError{
			Message: "GetTransferTasks operation failed.  Not able to create query iterator.",
		}
	}

	response := &getTransferTasksResponse{}
	task := make(map[string]interface{})
PopulateTasks:
	for iter.MapScan(task) {
		t := createTaskInfo(task["transfer"].(map[string]interface{}))
		// Reset task map to get it ready for next scan
		task = make(map[string]interface{})
		// Skip if the task should not be delivered right now
		if t.visibilityTime.After(currentTimestamp) {
			continue
		}

		newVisibilityTime := currentTimestamp.Add(request.lockTimeout)
		newCQLVisibilityTime := common.UnixNanoToCQLTimestamp(newVisibilityTime.UnixNano())
		newLockToken := uuid.New()
		newDeliveryCount := t.deliveryCount + 1

		lockQuery := d.session.Query(templateLockTransferTaskQuery,
			t.workflowID,
			t.runID,
			t.taskID,
			t.taskList,
			t.taskType,
			t.scheduleID,
			newCQLVisibilityTime,
			newLockToken,
			newDeliveryCount,
			newLockToken,
			d.shardID,
			rowTypeTransferTask,
			t.workflowID,
			t.taskID,
			t.lockToken)

		previous := make(map[string]interface{})
		applied, err1 := lockQuery.MapScanCAS(previous)
		if err1 != nil || !applied {
			// TODO: log on failure to acquire lock
			continue
		}

		t.visibilityTime = newVisibilityTime
		t.lockToken = newLockToken
		t.deliveryCount = newDeliveryCount
		response.tasks = append(response.tasks, t)
		if len(response.tasks) == request.batchSize {
			break PopulateTasks
		}
	}

	if err := iter.Close(); err != nil {
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("GetTransferTasks operation failed. Error: %v", err),
		}
	}

	return response, nil
}

func (d *cassandraPersistence) CompleteTransferTask(request *completeTransferTaskRequest) error {
	execution := request.execution
	query := d.session.Query(templateCompleteTransferTaskQuery,
		d.shardID,
		rowTypeTransferTask,
		execution.GetWorkflowId(),
		request.taskID,
		request.lockToken)

	previous := make(map[string]interface{})
	applied, err := query.MapScanCAS(previous)
	if err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("CompleteTransferTask operation failed. Error: %v", err),
		}
	}

	if !applied {
		if v, ok := previous["lock_token"]; ok {
			actualLockToken := v.(gocql.UUID).String()
			return &conditionFailedError{
				msg: fmt.Sprintf("Failed to complete task.  Provided lock_token: %v, actual lock_token: %v", request.lockToken,
					actualLockToken),
			}
		}

		return &workflow.EntityNotExistsError{
			Message: fmt.Sprintf("Task not found.  WorkflowId: %v, RunId: %v, TaskId: %v", execution.GetWorkflowId(),
				execution.GetRunId(), request.taskID),
		}
	}

	return nil
}

func (d *cassandraPersistence) CreateTask(request *createTaskRequest) (*createTaskResponse, error) {
	cqlNowTimestamp := common.UnixNanoToCQLTimestamp(time.Now().UnixNano())
	taskID := uuid.New()
	lockToken := uuid.New()

	var taskList string
	var scheduleID int64
	switch request.data.GetType() {
	case taskTypeActivity:
		taskList = request.data.(*activityTask).taskList
		scheduleID = request.data.(*activityTask).scheduleID
	case taskTypeDecision:
		taskList = request.data.(*decisionTask).taskList
		scheduleID = request.data.(*decisionTask).scheduleID
	}

	query := d.session.Query(templateCreateTaskQuery,
		request.taskList,
		request.data.GetType(),
		request.execution.GetWorkflowId(),
		request.execution.GetRunId(),
		taskID,
		request.execution.GetWorkflowId(),
		request.execution.GetRunId(),
		taskID,
		taskList,
		request.data.GetType(),
		scheduleID,
		cqlNowTimestamp,
		lockToken,
		0,
		lockToken).Consistency(d.lowConslevel)

	if err := query.Exec(); err != nil {
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("CreateTask operation failed. Error: %v", err),
		}
	}

	return &createTaskResponse{taskID: taskID}, nil
}

func (d *cassandraPersistence) GetTasks(request *getTasksRequest) (*getTasksResponse, error) {
	currentTimestamp := time.Now()

	query := d.session.Query(templateGetTasksQuery,
		request.taskList,
		request.taskType).Consistency(d.lowConslevel)

	iter := query.Iter()
	if iter == nil {
		return nil, &workflow.InternalServiceError{
			Message: "GetTasks operation failed.  Not able to create query iterator.",
		}
	}

	response := &getTasksResponse{}
	task := make(map[string]interface{})
PopulateTasks:
	for iter.MapScan(task) {
		t := createTaskInfo(task["task"].(map[string]interface{}))
		// Reset task map to get it ready for next scan
		task = make(map[string]interface{})
		// Skip if the task should not be delivered right now
		if t.visibilityTime.After(currentTimestamp) {
			continue
		}

		newVisibilityTime := currentTimestamp.Add(request.lockTimeout)
		newCQLVisibilityTime := common.UnixNanoToCQLTimestamp(newVisibilityTime.UnixNano())
		newLockToken := uuid.New()
		newDeliveryCount := t.deliveryCount + 1

		lockQuery := d.session.Query(templateLockTaskQuery,
			t.workflowID,
			t.runID,
			t.taskID,
			t.taskList,
			t.taskType,
			t.scheduleID,
			newCQLVisibilityTime,
			newLockToken,
			newDeliveryCount,
			newLockToken,
			t.taskList,
			t.taskType,
			t.workflowID,
			t.runID,
			t.taskID,
			t.lockToken)

		previous := make(map[string]interface{})
		applied, err1 := lockQuery.MapScanCAS(previous)
		if err1 != nil || !applied {
			// TODO: log on failure to acquire lock
			continue
		}

		t.visibilityTime = newVisibilityTime
		t.lockToken = newLockToken
		t.deliveryCount = newDeliveryCount
		response.tasks = append(response.tasks, t)
		if len(response.tasks) == request.batchSize {
			break PopulateTasks
		}
	}

	if err := iter.Close(); err != nil {
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("GetTasks operation failed. Error: %v", err),
		}
	}

	return response, nil
}

func (d *cassandraPersistence) CompleteTask(request *completeTaskRequest) error {
	execution := request.execution
	query := d.session.Query(templateCompleteTaskQuery,
		request.taskList,
		request.taskType,
		execution.GetWorkflowId(),
		execution.GetRunId(),
		request.taskID,
		request.lockToken)

	previous := make(map[string]interface{})
	applied, err := query.MapScanCAS(previous)
	if err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("CompleteTask operation failed. Error: %v", err),
		}
	}

	if !applied {
		if v, ok := previous["lock_token"]; ok {
			actualLockToken := v.(gocql.UUID).String()
			return &conditionFailedError{
				msg: fmt.Sprintf("Failed to complete task.  Provided lock_token: %v, actual lock_token: %v", request.lockToken,
					actualLockToken),
			}
		}

		return &workflow.EntityNotExistsError{
			Message: fmt.Sprintf("Task not found.  WorkflowId: %v, RunId: %v, TaskId: %v", execution.GetWorkflowId(),
				execution.GetRunId(), request.taskID),
		}
	}

	return nil
}

func (d *cassandraPersistence) createTransferTasks(batch *gocql.Batch, transferTasks []task, workflowID string,
	runID string, cqlNowTimestamp int64) {
	for _, task := range transferTasks {
		var taskList string
		var scheduleID int64
		switch task.GetType() {
		case taskTypeActivity:
			taskList = task.(*activityTask).taskList
			scheduleID = task.(*activityTask).scheduleID
		case taskTypeDecision:
			taskList = task.(*decisionTask).taskList
			scheduleID = task.(*decisionTask).scheduleID
		}

		transferTaskID := uuid.New()
		lockToken := uuid.New()
		batch.Query(templateCreateTransferTaskQuery,
			d.shardID,
			workflowID,
			rowTypeTransferTask,
			workflowID,
			runID,
			transferTaskID,
			taskList,
			task.GetType(),
			scheduleID,
			cqlNowTimestamp,
			lockToken,
			0,
			transferTaskID,
			lockToken)
	}
}

func createWorkflowExecutionInfo(result map[string]interface{}) *workflowExecutionInfo {
	info := &workflowExecutionInfo{}
	for k, v := range result {
		switch k {
		case "workflow_id":
			info.workflowID = v.(string)
		case "run_id":
			info.runID = v.(gocql.UUID).String()
		case "task_list":
			info.taskList = v.(string)
		case "history":
			info.history = v.([]byte)
		case "execution_context":
			info.executionContext = v.([]byte)
		case "state":
			info.state = v.(int)
		case "next_event_id":
			info.nextEventID = v.(int64)
		case "last_processed_event":
			info.lastProcessedEvent = v.(int64)
		case "last_updated_time":
			info.lastUpdatedTimestamp = v.(time.Time)
		case "decision_pending":
			info.decisionPending = v.(bool)
		}
	}

	return info
}

func createTaskInfo(result map[string]interface{}) *taskInfo {
	info := &taskInfo{}
	for k, v := range result {
		switch k {
		case "workflow_id":
			info.workflowID = v.(string)
		case "run_id":
			info.runID = v.(gocql.UUID).String()
		case "task_id":
			info.taskID = v.(gocql.UUID).String()
		case "task_list":
			info.taskList = v.(string)
		case "type":
			info.taskType = v.(int)
		case "schedule_id":
			info.scheduleID = v.(int64)
		case "visibility_time":
			info.visibilityTime = v.(time.Time)
		case "lock_token":
			info.lockToken = v.(gocql.UUID).String()
		case "delivery_count":
			info.deliveryCount = v.(int)
		}
	}

	return info
}
