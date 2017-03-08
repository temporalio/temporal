package persistence

import (
	"fmt"
	"strings"
	"time"

	"github.com/gocql/gocql"
	"github.com/pborman/uuid"
	"github.com/uber-common/bark"

	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
)

const (
	cassandraProtoVersion     = 4
	defaultSessionTimeout     = 10 * time.Second
	rowTypeExecutionTaskID    = int64(77)
	permanentRunID            = "dcb940ac-0c63-ffa2-ffea-a6c305881d71"
	rowTypeShardWorkflowID    = "3fe89dad-8326-fac5-fd40-fe08cfa25dec"
	rowTypeShardRunID         = "228ce20b-af54-fe2f-ff17-be728a00f785"
	rowTypeTransferWorkflowID = "5739f107-1a97-f929-fd00-b6fef701457d"
	rowTypeTransferRunID      = "49756028-f1fa-fa16-f67b-4553d9859b8c"
	rowTypeTimerWorkflowID    = "cd1f9688-d7ac-fc6b-f69e-8b44a3460a3d"
	rowTypeTimerRunID         = "c82b7881-892f-fd9e-feb3-a6d9f7b32f7f"
	rowTypeShardTaskID        = int64(23)
	defaultDeleteTTLSeconds   = int64(time.Hour*24*7) / int64(time.Second) // keep deleted records for 7 days
)

const (
	// Row types for table executions
	rowTypeShard = iota
	rowTypeExecution
	rowTypeTransferTask
	rowTypeTimerTask
)

const (
	// Row types for table tasks
	rowTypeTask = iota
	rowTypeTaskList
)

const (
	taskListTaskID = -12345 // for debugging
	initialRangeID = 1      // Id of the first range of a new task list
)

const (
	templateShardType = `{` +
		`shard_id: ?, ` +
		`owner: ?, ` +
		`range_id: ?, ` +
		`stolen_since_renew: ?, ` +
		`updated_at: ?, ` +
		`transfer_ack_level: ?` +
		`}`

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
		`decision_pending: ?,` +
		`create_request_id: ?` +
		`}`

	templateTransferTaskType = `{` +
		`workflow_id: ?, ` +
		`run_id: ?, ` +
		`task_id: ?, ` +
		`task_list: ?, ` +
		`type: ?, ` +
		`schedule_id: ?` +
		`}`

	templateTimerTaskType = `{` +
		`workflow_id: ?, ` +
		`run_id: ?, ` +
		`task_id: ?, ` +
		`type: ?, ` +
		`timeout_type: ?, ` +
		`event_id: ?` +
		`}`

	templateActivityInfoType = `{` +
		`schedule_id: ?, ` +
		`started_id: ?, ` +
		`activity_id: ?, ` +
		`request_id: ?, ` +
		`details: ?, ` +
		`schedule_to_start_timeout: ?, ` +
		`schedule_to_close_timeout: ?, ` +
		`start_to_close_timeout: ?, ` +
		`heart_beat_timeout: ?, ` +
		`cancel_requested: ?, ` +
		`cancel_request_id: ?` +
		`}`

	templateTimerInfoType = `{` +
		`timer_id: ?, ` +
		`started_id: ?, ` +
		`expiry_time: ?, ` +
		`task_id: ?` +
		`}`

	templateTaskListType = `{` +
		`name: ?, ` +
		`type: ?, ` +
		`ack_level: ? ` +
		`}`

	templateTaskType = `{` +
		`workflow_id: ?, ` +
		`run_id: ?, ` +
		`schedule_id: ?` +
		`}`

	templateCreateShardQuery = `INSERT INTO executions (` +
		`shard_id, type, workflow_id, run_id, task_id, shard, range_id)` +
		`VALUES(?, ?, ?, ?, ?, ` + templateShardType + `, ?) IF NOT EXISTS`

	templateGetShardQuery = `SELECT shard ` +
		`FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and task_id = ?`

	templateUpdateShardQuery = `UPDATE executions ` +
		`SET shard = ` + templateShardType + `, range_id = ? ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and task_id = ? ` +
		`IF range_id = ?`

	templateCreateWorkflowExecutionQuery = `INSERT INTO executions (` +
		`shard_id, type, workflow_id, run_id, task_id, current_run_id) ` +
		`VALUES(?, ?, ?, ?, ?, ?) IF NOT EXISTS`

	templateCreateWorkflowExecutionQuery2 = `INSERT INTO executions (` +
		`shard_id, workflow_id, run_id, type, execution, next_event_id, task_id) ` +
		`VALUES(?, ?, ?, ?, ` + templateWorkflowExecutionType + `, ?, ?) IF NOT EXISTS`

	templateCreateTransferTaskQuery = `INSERT INTO executions (` +
		`shard_id, type, workflow_id, run_id, transfer, task_id) ` +
		`VALUES(?, ?, ?, ?, ` + templateTransferTaskType + `, ?)`

	templateCreateTimerTaskQuery = `INSERT INTO executions (` +
		`shard_id, type, workflow_id, run_id, timer, task_id) ` +
		`VALUES(?, ?, ?, ?, ` + templateTimerTaskType + `, ?)`

	templateUpdateLeaseQuery = `UPDATE executions ` +
		`SET range_id = ? ` +
		`WHERE shard_id = ? ` +
		`IF range_id = ?`

	templateGetWorkflowExecutionQuery = `SELECT execution ` +
		`FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and task_id = ?`

	templateGetWorkflowMutabeStateQuery = `SELECT activity_map, timer_map ` +
		`FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and task_id = ?`

	templateUpdateWorkflowExecutionQuery = `UPDATE executions ` +
		`SET execution = ` + templateWorkflowExecutionType + `, next_event_id = ? ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and task_id = ? ` +
		`IF next_event_id = ? and range_id = ?`

	templateUpdateActivityInfoQuery = `UPDATE executions ` +
		`SET activity_map[ ? ] =` + templateActivityInfoType + ` ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and task_id = ? ` +
		`IF next_event_id = ? and range_id = ?`

	templateUpdateTimerInfoQuery = `UPDATE executions ` +
		`SET timer_map[ ? ] =` + templateTimerInfoType + ` ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and task_id = ? ` +
		`IF next_event_id = ? and range_id = ?`

	templateDeleteActivityInfoQuery = `DELETE activity_map[ ? ] ` +
		`FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and task_id = ? ` +
		`IF next_event_id = ? and range_id = ?`

	templateDeleteTimerInfoQuery = `DELETE timer_map[ ? ] ` +
		`FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and task_id = ? ` +
		`IF next_event_id = ? and range_id = ?`

	templateDeleteWorkflowExecutionQuery = `DELETE FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and task_id = ? `

	templateDeleteWorkflowExecutionTTLQuery = `INSERT INTO executions (` +
		`shard_id, workflow_id, run_id, type, execution, next_event_id, task_id) ` +
		`VALUES(?, ?, ?, ?, ` + templateWorkflowExecutionType + `, ?, ?) USING TTL ?`

	templateGetTransferTasksQuery = `SELECT transfer ` +
		`FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and task_id > ? ` +
		`and task_id <= ? LIMIT ?`

	templateCompleteTransferTaskQuery = `DELETE FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and task_id = ?` +
		`IF EXISTS`

	templateGetTimerTasksQuery = `SELECT timer ` +
		`FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ?` +
		`and workflow_id = ?` +
		`and run_id = ?` +
		`and task_id >= ?` +
		`and task_id < ?`

	templateCompleteTimerTaskQuery = `DELETE FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and workflow_id = ?` +
		`and run_id = ?` +
		`and task_id = ?`

	templateCreateTaskQuery = `INSERT INTO tasks (` +
		`task_list_name, task_list_type, type, task_id, task) ` +
		`VALUES(?, ?, ?, ?, ` + templateTaskType + `)`

	templateGetTasksQuery = `SELECT task_id, task ` +
		`FROM tasks ` +
		`WHERE task_list_name = ? ` +
		`and task_list_type = ? ` +
		`and type = ? ` +
		`and task_id > ? ` +
		`and task_id <= ? LIMIT ?`

	templateCompleteTaskQuery = `DELETE FROM tasks ` +
		`WHERE task_list_name = ? ` +
		`and task_list_type = ? ` +
		`and type = ? ` +
		`and task_id = ?`

	templateGetTaskList = `SELECT ` +
		`range_id, ` +
		`task_list ` +
		`FROM tasks ` +
		`WHERE ` +
		`task_list_name = ? ` +
		`and task_list_type = ? ` +
		`and type = ? ` +
		`and task_id = ?`

	templateInsertTaskListQuery = `INSERT INTO tasks (` +
		`task_list_name, ` +
		`task_list_type, ` +
		`type, ` +
		`task_id, ` +
		`range_id, ` +
		`task_list ` +
		`) VALUES (?, ?, ?, ?, ?, ` + templateTaskListType + `) IF NOT EXISTS`

	templateUpdateTaskListQuery = `UPDATE tasks SET ` +
		`range_id = ?, ` +
		`task_list = ` + templateTaskListType + " " +
		`WHERE  task_list_name = ? ` +
		`and task_list_type = ? ` +
		`and type = ? ` +
		`and task_id = ? ` +
		`IF range_id = ?`

	templateUpdateTaskListRangeOnlyQuery = `UPDATE tasks SET ` +
		`range_id = ? ` +
		`WHERE  task_list_name = ? ` +
		`and task_list_type = ? ` +
		`IF range_id = ?`
)

type (
	cassandraPersistence struct {
		session      *gocql.Session
		lowConslevel gocql.Consistency
		shardID      int
		logger       bark.Logger
	}
)

// NewCassandraShardPersistence is used to create an instance of ShardManager implementation
func NewCassandraShardPersistence(hosts string, dc string, keyspace string, logger bark.Logger) (ShardManager, error) {
	cluster := common.NewCassandraCluster(hosts, dc)
	cluster.Keyspace = keyspace
	cluster.ProtoVersion = cassandraProtoVersion
	cluster.Consistency = gocql.LocalQuorum
	cluster.SerialConsistency = gocql.LocalSerial
	cluster.Timeout = defaultSessionTimeout

	session, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}

	return &cassandraPersistence{shardID: -1, session: session, lowConslevel: gocql.One, logger: logger}, nil
}

// NewCassandraWorkflowExecutionPersistence is used to create an instance of workflowExecutionManager implementation
func NewCassandraWorkflowExecutionPersistence(hosts string, dc string, keyspace string, shardID int, logger bark.Logger) (ExecutionManager, error) {
	cluster := common.NewCassandraCluster(hosts, dc)
	cluster.Keyspace = keyspace
	cluster.ProtoVersion = cassandraProtoVersion
	cluster.Consistency = gocql.LocalQuorum
	cluster.SerialConsistency = gocql.LocalSerial
	cluster.Timeout = defaultSessionTimeout

	session, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}

	return &cassandraPersistence{shardID: shardID, session: session, lowConslevel: gocql.One, logger: logger}, nil
}

// NewCassandraTaskPersistence is used to create an instance of TaskManager implementation
func NewCassandraTaskPersistence(hosts string, dc string, keyspace string, logger bark.Logger) (TaskManager, error) {
	cluster := common.NewCassandraCluster(hosts, dc)
	cluster.Keyspace = keyspace
	cluster.ProtoVersion = cassandraProtoVersion
	cluster.Consistency = gocql.LocalQuorum
	cluster.SerialConsistency = gocql.LocalSerial
	cluster.Timeout = defaultSessionTimeout

	session, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}
	return &cassandraPersistence{shardID: -1, session: session, lowConslevel: gocql.One, logger: logger}, nil
}

func (d *cassandraPersistence) CreateShard(request *CreateShardRequest) error {
	cqlNowTimestamp := common.UnixNanoToCQLTimestamp(time.Now().UnixNano())
	shardInfo := request.ShardInfo
	query := d.session.Query(templateCreateShardQuery,
		shardInfo.ShardID,
		rowTypeShard,
		rowTypeShardWorkflowID,
		rowTypeShardRunID,
		rowTypeShardTaskID,
		shardInfo.ShardID,
		shardInfo.Owner,
		shardInfo.RangeID,
		shardInfo.StolenSinceRenew,
		cqlNowTimestamp,
		shardInfo.TransferAckLevel,
		shardInfo.RangeID)

	previous := make(map[string]interface{})
	applied, err := query.MapScanCAS(previous)
	if err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("CreateShard operation failed. Error : %v", err),
		}
	}

	if !applied {
		shard := previous["shard"].(map[string]interface{})
		return &ShardAlreadyExistError{
			Msg: fmt.Sprintf("Shard already exists in executions table.  ShardId: %v, RangeId: %v",
				shard["shard_id"], shard["range_id"]),
		}
	}

	return nil
}

func (d *cassandraPersistence) GetShard(request *GetShardRequest) (*GetShardResponse, error) {
	shardID := request.ShardID
	query := d.session.Query(templateGetShardQuery,
		shardID,
		rowTypeShard,
		rowTypeShardWorkflowID,
		rowTypeShardRunID,
		rowTypeShardTaskID).Consistency(d.lowConslevel)

	result := make(map[string]interface{})
	if err := query.MapScan(result); err != nil {
		if err == gocql.ErrNotFound {
			return nil, &workflow.EntityNotExistsError{
				Message: fmt.Sprintf("Shard not found.  ShardId: %v", shardID),
			}
		}

		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("GetShard operation failed. Error: %v", err),
		}
	}

	info := createShardInfo(result["shard"].(map[string]interface{}))

	return &GetShardResponse{ShardInfo: info}, nil
}

func (d *cassandraPersistence) UpdateShard(request *UpdateShardRequest) error {
	cqlNowTimestamp := common.UnixNanoToCQLTimestamp(time.Now().UnixNano())
	shardInfo := request.ShardInfo

	query := d.session.Query(templateUpdateShardQuery,
		shardInfo.ShardID,
		shardInfo.Owner,
		shardInfo.RangeID,
		shardInfo.StolenSinceRenew,
		cqlNowTimestamp,
		shardInfo.TransferAckLevel,
		shardInfo.RangeID,
		shardInfo.ShardID,
		rowTypeShard,
		rowTypeShardWorkflowID,
		rowTypeShardRunID,
		rowTypeShardTaskID,
		request.PreviousRangeID)

	previous := make(map[string]interface{})
	applied, err := query.MapScanCAS(previous)
	if err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("UpdateShard operation failed. Error: %v", err),
		}
	}

	if !applied {
		var columns []string
		for k, v := range previous {
			columns = append(columns, fmt.Sprintf("%s=%v", k, v))
		}

		return &ShardOwnershipLostError{
			ShardID: d.shardID,
			Msg: fmt.Sprintf("Failed to update shard.  previous_range_id: %v, columns: (%v)",
				request.PreviousRangeID, strings.Join(columns, ",")),
		}
	}

	return nil
}

func (d *cassandraPersistence) CreateWorkflowExecution(request *CreateWorkflowExecutionRequest) (
	*CreateWorkflowExecutionResponse, error) {
	cqlNowTimestamp := common.UnixNanoToCQLTimestamp(time.Now().UnixNano())
	transferTaskID := uuid.New()

	batch := d.session.NewBatch(gocql.LoggedBatch)
	batch.Query(templateCreateWorkflowExecutionQuery,
		d.shardID,
		rowTypeExecution,
		request.Execution.GetWorkflowId(),
		permanentRunID,
		rowTypeExecutionTaskID,
		request.Execution.GetRunId())

	batch.Query(templateCreateWorkflowExecutionQuery2,
		d.shardID,
		request.Execution.GetWorkflowId(),
		request.Execution.GetRunId(),
		rowTypeExecution,
		request.Execution.GetWorkflowId(),
		request.Execution.GetRunId(),
		request.TaskList,
		request.History,
		request.ExecutionContext,
		WorkflowStateCreated,
		request.NextEventID,
		request.LastProcessedEvent,
		cqlNowTimestamp,
		true,
		request.RequestID,
		request.NextEventID,
		rowTypeExecutionTaskID)

	d.createTransferTasks(batch, request.TransferTasks, request.Execution.GetWorkflowId(), request.Execution.GetRunId(),
		cqlNowTimestamp)
	d.createTimerTasks(batch, request.TimerTasks, nil, request.Execution.GetWorkflowId(), request.Execution.GetRunId(), cqlNowTimestamp)

	batch.Query(templateUpdateLeaseQuery,
		request.RangeID,
		d.shardID,
		request.RangeID,
	)

	previous := make(map[string]interface{})
	applied, _, err := d.session.MapExecuteBatchCAS(batch, previous)
	if err != nil {
		if _, ok := err.(*gocql.RequestErrWriteTimeout); ok {
			// Write may have succeeded, but we don't know
			// return this info to the caller so they have the option of trying to find out by executing a read
			return nil, &TimeoutError{Msg: fmt.Sprintf("CreateWorkflowExecution timed out. Error: %v", err)}
		}
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("CreateWorkflowExecution operation failed. Error: %v", err),
		}
	}

	if !applied {
		if rangeID, ok := previous["range_id"].(int64); ok && rangeID != request.RangeID {
			// CreateWorkflowExecution failed because rangeID was modified
			return nil, &ShardOwnershipLostError{
				ShardID: d.shardID,
				Msg: fmt.Sprintf("Failed to create workflow execution.  Request RangeID: %v, Actual RangeID: %v",
					request.RangeID, rangeID),
			}
		}

		var columns []string
		for k, v := range previous {
			columns = append(columns, fmt.Sprintf("%s=%v", k, v))
		}

		if execution, ok := previous["execution"].(map[string]interface{}); ok {
			// CreateWorkflowExecution failed because it already exists
			msg := fmt.Sprintf("Workflow execution already running. WorkflowId: %v, RunId: %v, rangeID: %v, columns: (%v)",
				execution["workflow_id"], execution["run_id"], request.RangeID, strings.Join(columns, ","))
			return nil, &workflow.WorkflowExecutionAlreadyStartedError{
				Message:        common.StringPtr(msg),
				StartRequestId: common.StringPtr(fmt.Sprintf("%v", execution["create_request_id"])),
			}
		}

		return nil, &ConditionFailedError{
			Msg: fmt.Sprintf("Failed to create workflow execution.  Request RangeID: %v, columns: (%v)",
				request.RangeID, strings.Join(columns, ",")),
		}
	}

	return &CreateWorkflowExecutionResponse{TaskID: transferTaskID}, nil
}

func (d *cassandraPersistence) GetWorkflowExecution(request *GetWorkflowExecutionRequest) (
	*GetWorkflowExecutionResponse, error) {
	execution := request.Execution
	query := d.session.Query(templateGetWorkflowExecutionQuery,
		d.shardID,
		rowTypeExecution,
		execution.GetWorkflowId(),
		execution.GetRunId(),
		rowTypeExecutionTaskID)

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

	return &GetWorkflowExecutionResponse{ExecutionInfo: info}, nil
}

func (d *cassandraPersistence) UpdateWorkflowExecution(request *UpdateWorkflowExecutionRequest) error {
	executionInfo := request.ExecutionInfo
	cqlNowTimestamp := common.UnixNanoToCQLTimestamp(time.Now().UnixNano())

	batch := d.session.NewBatch(gocql.LoggedBatch)
	batch.Query(templateUpdateWorkflowExecutionQuery,
		executionInfo.WorkflowID,
		executionInfo.RunID,
		executionInfo.TaskList,
		executionInfo.History,
		executionInfo.ExecutionContext,
		executionInfo.State,
		executionInfo.NextEventID,
		executionInfo.LastProcessedEvent,
		cqlNowTimestamp,
		executionInfo.DecisionPending,
		executionInfo.CreateRequestID,
		executionInfo.NextEventID,
		d.shardID,
		rowTypeExecution,
		executionInfo.WorkflowID,
		executionInfo.RunID,
		rowTypeExecutionTaskID,
		request.Condition,
		request.RangeID)

	d.createTransferTasks(batch, request.TransferTasks, executionInfo.WorkflowID, executionInfo.RunID, cqlNowTimestamp)

	d.createTimerTasks(batch, request.TimerTasks, request.DeleteTimerTask,
		executionInfo.WorkflowID, executionInfo.RunID, cqlNowTimestamp)

	d.updateActivityInfos(batch, request.UpsertActivityInfos, request.DeleteActivityInfo,
		executionInfo.WorkflowID, executionInfo.RunID, request.Condition, request.RangeID)

	d.updateTimerInfos(batch, request.UpserTimerInfos, request.DeleteTimerInfos,
		executionInfo.WorkflowID, executionInfo.RunID, request.Condition, request.RangeID)

	previous := make(map[string]interface{})
	applied, _, err := d.session.MapExecuteBatchCAS(batch, previous)
	if err != nil {
		if _, ok := err.(*gocql.RequestErrWriteTimeout); ok {
			// Write may have succeeded, but we don't know
			// return this info to the caller so they have the option of trying to find out by executing a read
			return &TimeoutError{Msg: fmt.Sprintf("UpdateWorkflowExecution timed out. Error: %v", err)}
		}
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("UpdateWorkflowExecution operation failed. Error: %v", err),
		}
	}

	if !applied {
		if rangeID, ok := previous["range_id"].(int64); ok && rangeID != request.RangeID {
			// UpdateWorkflowExecution failed because rangeID was modified
			return &ShardOwnershipLostError{
				ShardID: d.shardID,
				Msg: fmt.Sprintf("Failed to update workflow execution.  Request RangeID: %v, Actual RangeID: %v",
					request.RangeID, rangeID),
			}
		}

		if nextEventID, ok := previous["next_event_id"].(int64); ok && nextEventID != request.Condition {
			// CreateWorkflowExecution failed because next event ID is unexpected
			return &ConditionFailedError{
				Msg: fmt.Sprintf("Failed to update workflow execution.  Request Condition: %v, Actual Value: %v",
					request.Condition, nextEventID),
			}
		}

		var columns []string
		for k, v := range previous {
			columns = append(columns, fmt.Sprintf("%s=%v", k, v))
		}

		return &ConditionFailedError{
			Msg: fmt.Sprintf("Failed to update workflow execution.  RangeID: %v, Condition: %v, columns: (%v)",
				request.RangeID, request.Condition, strings.Join(columns, ",")),
		}
	}

	return nil
}

func (d *cassandraPersistence) DeleteWorkflowExecution(request *DeleteWorkflowExecutionRequest) error {
	info := request.ExecutionInfo
	cqlNowTimestamp := common.UnixNanoToCQLTimestamp(time.Now().UnixNano())
	batch := d.session.NewBatch(gocql.LoggedBatch)
	batch.Query(templateDeleteWorkflowExecutionQuery,
		d.shardID,
		rowTypeExecution,
		info.WorkflowID,
		permanentRunID,
		rowTypeExecutionTaskID)

	batch.Query(templateDeleteWorkflowExecutionTTLQuery,
		d.shardID,
		info.WorkflowID,
		info.RunID,
		rowTypeExecution,
		info.WorkflowID,
		info.RunID,
		info.TaskList,
		info.History,
		info.ExecutionContext,
		info.State,
		info.NextEventID,
		info.LastProcessedEvent,
		cqlNowTimestamp,
		info.DecisionPending,
		info.CreateRequestID,
		info.NextEventID,
		rowTypeExecutionTaskID,
		defaultDeleteTTLSeconds)

	err := d.session.ExecuteBatch(batch)
	if err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("DeleteWorkflowExecution operation failed. Error: %v", err),
		}
	}

	return nil
}

func (d *cassandraPersistence) GetTransferTasks(request *GetTransferTasksRequest) (*GetTransferTasksResponse, error) {

	// Reading transfer tasks need to be quorum level consistent, otherwise we could loose task
	query := d.session.Query(templateGetTransferTasksQuery,
		d.shardID,
		rowTypeTransferTask,
		rowTypeTransferWorkflowID,
		rowTypeTransferRunID,
		request.ReadLevel,
		request.MaxReadLevel,
		request.BatchSize)

	iter := query.Iter()
	if iter == nil {
		return nil, &workflow.InternalServiceError{
			Message: "GetTransferTasks operation failed.  Not able to create query iterator.",
		}
	}

	response := &GetTransferTasksResponse{}
	task := make(map[string]interface{})
	for iter.MapScan(task) {
		t := createTransferTaskInfo(task["transfer"].(map[string]interface{}))
		// Reset task map to get it ready for next scan
		task = make(map[string]interface{})

		response.Tasks = append(response.Tasks, t)
	}

	if err := iter.Close(); err != nil {
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("GetTransferTasks operation failed. Error: %v", err),
		}
	}

	return response, nil
}

func (d *cassandraPersistence) CompleteTransferTask(request *CompleteTransferTaskRequest) error {
	execution := request.Execution
	query := d.session.Query(templateCompleteTransferTaskQuery,
		d.shardID,
		rowTypeTransferTask,
		rowTypeTransferWorkflowID,
		rowTypeTransferRunID,
		request.TaskID)

	previous := make(map[string]interface{})
	applied, err := query.MapScanCAS(previous)
	if err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("CompleteTransferTask operation failed. Error: %v", err),
		}
	}

	if !applied {
		return &workflow.EntityNotExistsError{
			Message: fmt.Sprintf("Task not found.  WorkflowId: %v, RunId: %v, TaskId: %v", execution.GetWorkflowId(),
				execution.GetRunId(), request.TaskID),
		}
	}

	return nil
}

// From TaskManager interface
func (d *cassandraPersistence) LeaseTaskList(request *LeaseTaskListRequest) (*LeaseTaskListResponse, error) {
	if len(request.TaskList) == 0 {
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("LeaseTaskList requires non empty task list"),
		}
	}
	query := d.session.Query(templateGetTaskList,
		request.TaskList,
		request.TaskType,
		rowTypeTaskList,
		taskListTaskID,
	)
	var rangeID, ackLevel int64
	var tlDB map[string]interface{}
	err := query.Scan(&rangeID, &tlDB)
	if err != nil {
		if err == gocql.ErrNotFound { // First time task list is used
			query = d.session.Query(templateInsertTaskListQuery,
				request.TaskList,
				request.TaskType,
				rowTypeTaskList,
				taskListTaskID,
				initialRangeID,
				request.TaskList,
				request.TaskType,
				0)
		} else {
			return nil, &workflow.InternalServiceError{
				Message: fmt.Sprintf("LeaseTaskList operation failed. TaskList: %v, TaskType: %v, Error : %v",
					request.TaskList, request.TaskType, err),
			}
		}
	} else {
		ackLevel = tlDB["ack_level"].(int64)
		query = d.session.Query(templateUpdateTaskListQuery,
			rangeID+1,
			&request.TaskList,
			request.TaskType,
			ackLevel,
			&request.TaskList,
			request.TaskType,
			rowTypeTaskList,
			taskListTaskID,
			rangeID,
		)
	}
	previous := make(map[string]interface{})
	applied, err := query.MapScanCAS(previous)
	if err != nil {
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("LeaseTaskList operation failed. Error : %v", err),
		}
	}
	if !applied {
		previousRangeID := previous["range_id"]
		return nil, &ConditionFailedError{
			Msg: fmt.Sprintf("LeaseTaskList failed to apply. db rangeID %v", previousRangeID),
		}
	}
	tli := &TaskListInfo{Name: request.TaskList, TaskType: request.TaskType, RangeID: rangeID + 1, AckLevel: ackLevel}
	return &LeaseTaskListResponse{TaskListInfo: tli}, nil
}

// From TaskManager interface
func (d *cassandraPersistence) UpdateTaskList(request *UpdateTaskListRequest) (*UpdateTaskListResponse, error) {
	tli := request.TaskListInfo

	query := d.session.Query(templateUpdateTaskListQuery,
		tli.RangeID,
		&tli.Name,
		tli.TaskType,
		tli.AckLevel,
		&tli.Name,
		tli.TaskType,
		rowTypeTaskList,
		taskListTaskID,
		tli.RangeID,
	)

	previous := make(map[string]interface{})
	applied, err := query.MapScanCAS(previous)
	if err != nil {
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("UpdateTaskList operation failed. Error: %v", err),
		}
	}

	if !applied {
		var columns []string
		for k, v := range previous {
			columns = append(columns, fmt.Sprintf("%s=%v", k, v))
		}

		return nil, &ConditionFailedError{
			Msg: fmt.Sprintf("Failed to update task list. name: %v, type: %v, rangeID: %v, columns: (%v)",
				tli.Name, tli.TaskType, tli.RangeID, strings.Join(columns, ",")),
		}
	}

	return &UpdateTaskListResponse{}, nil
}

// From TaskManager interface
func (d *cassandraPersistence) CreateTask(request *CreateTaskRequest) (*CreateTaskResponse, error) {
	var taskList string
	var scheduleID int64
	taskType := request.Data.GetType()
	switch taskType {
	case TaskListTypeActivity:
		taskList = request.Data.(*ActivityTask).TaskList
		scheduleID = request.Data.(*ActivityTask).ScheduleID

	case TaskListTypeDecision:
		taskList = request.Data.(*DecisionTask).TaskList
		scheduleID = request.Data.(*DecisionTask).ScheduleID
	}

	// Batch is used to include conditional update on range_id
	batch := d.session.NewBatch(gocql.LoggedBatch)

	batch.Query(templateCreateTaskQuery,
		taskList,
		request.Data.GetType(),
		rowTypeTask,
		request.TaskID,
		request.Execution.GetWorkflowId(),
		request.Execution.GetRunId(),
		scheduleID)

	// The following query is used to ensure that range_id didn't change
	batch.Query(templateUpdateTaskListRangeOnlyQuery,
		request.RangeID,
		taskList,
		taskType,
		request.RangeID,
	)
	previous := make(map[string]interface{})
	applied, _, err := d.session.MapExecuteBatchCAS(batch, previous)
	if err != nil {
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("CreateTask operation failed. Error : %v", err),
		}
	}
	if !applied {
		rangeID := previous["range_id"]
		return nil, &ConditionFailedError{
			Msg: fmt.Sprintf("Failed to create task. TaskList: %v, taskType: %v, rangeID: %v, db rangeID: %v",
				taskList, taskType, request.RangeID, rangeID),
		}
	}
	return &CreateTaskResponse{}, nil
}

// From TaskManager interface
func (d *cassandraPersistence) GetTasks(request *GetTasksRequest) (*GetTasksResponse, error) {
	if request.ReadLevel > request.MaxReadLevel {
		return &GetTasksResponse{}, nil
	}

	// Reading tasklist tasks need to be quorum level consistent, otherwise we could loose task
	query := d.session.Query(templateGetTasksQuery,
		request.TaskList,
		request.TaskType,
		rowTypeTask,
		request.ReadLevel,
		request.MaxReadLevel,
		request.BatchSize)

	iter := query.Iter()
	if iter == nil {
		return nil, &workflow.InternalServiceError{
			Message: "GetTasks operation failed.  Not able to create query iterator.",
		}
	}

	response := &GetTasksResponse{}
	task := make(map[string]interface{})
PopulateTasks:
	for iter.MapScan(task) {
		taskID, ok := task["task_id"]
		if !ok { // no tasks, but static column record returned
			continue
		}
		t := createTaskInfo(task["task"].(map[string]interface{}))
		t.TaskID = taskID.(int64)
		response.Tasks = append(response.Tasks, t)
		if len(response.Tasks) == request.BatchSize {
			break PopulateTasks
		}
		task = make(map[string]interface{}) // Reinitialize map as initialized fails on unmarshalling
	}

	if err := iter.Close(); err != nil {
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("GetTasks operation failed. Error: %v", err),
		}
	}

	return response, nil
}

// From TaskManager interface
func (d *cassandraPersistence) CompleteTask(request *CompleteTaskRequest) error {
	batch := d.session.NewBatch(gocql.LoggedBatch)
	tli := request.TaskList
	batch.Query(templateCompleteTaskQuery,
		tli.Name,
		tli.TaskType,
		rowTypeTask,
		request.TaskID)
	batch.Query(templateUpdateTaskListQuery,
		tli.RangeID,
		tli.Name,
		tli.TaskType,
		tli.AckLevel,
		tli.Name,
		tli.TaskType,
		rowTypeTaskList,
		taskListTaskID,
		tli.RangeID,
	)

	previous := make(map[string]interface{})
	applied, _, err := d.session.MapExecuteBatchCAS(batch, previous)
	if err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("CompleteTask operation failed. Error: %v", err),
		}
	}

	if !applied {
		var columns []string
		for k, v := range previous {
			columns = append(columns, fmt.Sprintf("%s=%v", k, v))
		}

		return &ConditionFailedError{
			Msg: fmt.Sprintf("Failed to complete task. columns: (%v)",
				strings.Join(columns, ",")),
		}
	}
	return nil
}

func (d *cassandraPersistence) GetTimerIndexTasks(request *GetTimerIndexTasksRequest) (*GetTimerIndexTasksResponse, error) {
	// Reading timer tasks need to be quorum level consistent, otherwise we could loose task
	query := d.session.Query(templateGetTimerTasksQuery,
		d.shardID,
		rowTypeTimerTask,
		rowTypeTimerWorkflowID,
		rowTypeTimerRunID,
		request.MinKey,
		request.MaxKey)

	iter := query.Iter()
	if iter == nil {
		return nil, &workflow.InternalServiceError{
			Message: "GetTimerTasks operation failed.  Not able to create query iterator.",
		}
	}

	response := &GetTimerIndexTasksResponse{}
	task := make(map[string]interface{})
PopulateTasks:
	for iter.MapScan(task) {
		t := createTimerTaskInfo(task["timer"].(map[string]interface{}))
		// Reset task map to get it ready for next scan
		task = make(map[string]interface{})
		// Skip the task if it is not in the bounds.
		if t.TaskID < request.MinKey {
			continue
		}
		if t.TaskID >= request.MaxKey {
			break PopulateTasks
		}

		response.Timers = append(response.Timers, t)
		if len(response.Timers) == request.BatchSize {
			break PopulateTasks
		}
	}

	if err := iter.Close(); err != nil {
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("GetTimerTasks operation failed. Error: %v", err),
		}
	}

	return response, nil
}

func (d *cassandraPersistence) GetWorkflowMutableState(request *GetWorkflowMutableStateRequest) (
	*GetWorkflowMutableStateResponse, error) {
	query := d.session.Query(templateGetWorkflowMutabeStateQuery,
		d.shardID,
		rowTypeExecution,
		request.WorkflowID,
		request.RunID,
		rowTypeExecutionTaskID).Consistency(d.lowConslevel)

	result := make(map[string]interface{})
	if err := query.MapScan(result); err != nil {
		if err == gocql.ErrNotFound {
			return nil, &workflow.EntityNotExistsError{
				Message: fmt.Sprintf("Workflow execution not found.  WorkflowId: %v, RunId: %v",
					request.WorkflowID, request.RunID),
			}
		}

		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("GetWorkflowExecution operation failed. Error: %v", err),
		}
	}

	state := &WorkflowMutableState{}

	activityInfos := make(map[int64]*ActivityInfo)
	aMap := result["activity_map"].(map[int64]map[string]interface{})
	for key, value := range aMap {
		info := createActivityInfo(value)
		activityInfos[key] = info
	}
	state.ActivitInfos = activityInfos

	timerInfos := make(map[string]*TimerInfo)
	tMap := result["timer_map"].(map[string]map[string]interface{})
	for key, value := range tMap {
		info := createTimerInfo(value)
		timerInfos[key] = info
	}
	state.TimerInfos = timerInfos

	return &GetWorkflowMutableStateResponse{State: state}, nil
}

func (d *cassandraPersistence) createTransferTasks(batch *gocql.Batch, transferTasks []Task, workflowID string,
	runID string, cqlNowTimestamp int64) {
	for _, task := range transferTasks {
		var taskList string
		var scheduleID int64

		switch task.GetType() {
		case TaskListTypeActivity:
			taskList = task.(*ActivityTask).TaskList
			scheduleID = task.(*ActivityTask).ScheduleID

		case TaskListTypeDecision:
			taskList = task.(*DecisionTask).TaskList
			scheduleID = task.(*DecisionTask).ScheduleID
		}

		batch.Query(templateCreateTransferTaskQuery,
			d.shardID,
			rowTypeTransferTask,
			rowTypeTransferWorkflowID,
			rowTypeTransferRunID,
			workflowID,
			runID,
			task.GetTaskID(),
			taskList,
			task.GetType(),
			scheduleID,
			task.GetTaskID())
	}
}

func (d *cassandraPersistence) createTimerTasks(batch *gocql.Batch, timerTasks []Task, deleteTimerTask Task, workflowID string,
	runID string, cqlNowTimestamp int64) {

	for _, task := range timerTasks {
		var eventID int64

		timeoutType := 0

		switch task.GetType() {
		case TaskTypeDecisionTimeout:
			eventID = task.(*DecisionTimeoutTask).EventID

		case TaskTypeActivityTimeout:
			eventID = task.(*ActivityTimeoutTask).EventID
			timeoutType = task.(*ActivityTimeoutTask).TimeoutType

		case TaskTypeUserTimer:
			eventID = task.(*UserTimerTask).EventID
		}

		batch.Query(templateCreateTimerTaskQuery,
			d.shardID,
			rowTypeTimerTask,
			rowTypeTimerWorkflowID,
			rowTypeTimerRunID,
			workflowID,
			runID,
			task.GetTaskID(),
			task.GetType(),
			timeoutType,
			eventID,
			task.GetTaskID())
	}

	if deleteTimerTask != nil {
		batch.Query(templateCompleteTimerTaskQuery,
			d.shardID,
			rowTypeTimerTask,
			rowTypeTimerWorkflowID,
			rowTypeTimerRunID,
			deleteTimerTask.GetTaskID())
	}
}

func (d *cassandraPersistence) updateActivityInfos(batch *gocql.Batch, activityInfos []*ActivityInfo, deleteInfo *int64,
	workflowID string, runID string, condition int64, rangeID int64) {

	for _, a := range activityInfos {
		batch.Query(templateUpdateActivityInfoQuery,
			a.ScheduleID,
			a.ScheduleID,
			a.StartedID,
			a.ActivityID,
			a.RequestID,
			a.Details,
			a.ScheduleToStartTimeout,
			a.ScheduleToCloseTimeout,
			a.StartToCloseTimeout,
			a.HeartbeatTimeout,
			a.CancelRequested,
			a.CancelRequestID,
			d.shardID,
			rowTypeExecution,
			workflowID,
			runID,
			rowTypeExecutionTaskID,
			condition,
			rangeID)
	}

	if deleteInfo != nil {
		batch.Query(templateDeleteActivityInfoQuery,
			*deleteInfo,
			d.shardID,
			rowTypeExecution,
			workflowID,
			runID,
			rowTypeExecutionTaskID,
			condition,
			rangeID)
	}
}

func (d *cassandraPersistence) updateTimerInfos(batch *gocql.Batch, timerInfos []*TimerInfo, deleteInfos []string,
	workflowID string, runID string, condition int64, rangeID int64) {

	for _, a := range timerInfos {
		batch.Query(templateUpdateTimerInfoQuery,
			a.TimerID,
			a.TimerID,
			a.StartedID,
			a.ExpiryTime,
			a.TaskID,
			d.shardID,
			rowTypeExecution,
			workflowID,
			runID,
			rowTypeExecutionTaskID,
			condition,
			rangeID)
	}

	for _, t := range deleteInfos {
		batch.Query(templateDeleteTimerInfoQuery,
			t,
			d.shardID,
			rowTypeExecution,
			workflowID,
			runID,
			rowTypeExecutionTaskID,
			condition,
			rangeID)
	}
}

func createShardInfo(result map[string]interface{}) *ShardInfo {
	info := &ShardInfo{}
	for k, v := range result {
		switch k {
		case "shard_id":
			info.ShardID = v.(int)
		case "owner":
			info.Owner = v.(string)
		case "range_id":
			info.RangeID = v.(int64)
		case "stolen_since_renew":
			info.StolenSinceRenew = v.(int)
		case "updated_at":
			info.UpdatedAt = v.(time.Time)
		case "transfer_ack_level":
			info.TransferAckLevel = v.(int64)
		}
	}

	return info
}

func createWorkflowExecutionInfo(result map[string]interface{}) *WorkflowExecutionInfo {
	info := &WorkflowExecutionInfo{}
	for k, v := range result {
		switch k {
		case "workflow_id":
			info.WorkflowID = v.(string)
		case "run_id":
			info.RunID = v.(gocql.UUID).String()
		case "task_list":
			info.TaskList = v.(string)
		case "history":
			info.History = v.([]byte)
		case "execution_context":
			info.ExecutionContext = v.([]byte)
		case "state":
			info.State = v.(int)
		case "next_event_id":
			info.NextEventID = v.(int64)
		case "last_processed_event":
			info.LastProcessedEvent = v.(int64)
		case "last_updated_time":
			info.LastUpdatedTimestamp = v.(time.Time)
		case "decision_pending":
			info.DecisionPending = v.(bool)
		case "create_request_id":
			info.CreateRequestID = v.(gocql.UUID).String()
		}
	}

	return info
}

func createTransferTaskInfo(result map[string]interface{}) *TransferTaskInfo {
	info := &TransferTaskInfo{}
	for k, v := range result {
		switch k {
		case "workflow_id":
			info.WorkflowID = v.(string)
		case "run_id":
			info.RunID = v.(gocql.UUID).String()
		case "task_id":
			info.TaskID = v.(int64)
		case "task_list":
			info.TaskList = v.(string)
		case "type":
			info.TaskType = v.(int)
		case "schedule_id":
			info.ScheduleID = v.(int64)
		}
	}

	return info
}

func createActivityInfo(result map[string]interface{}) *ActivityInfo {
	info := &ActivityInfo{}
	for k, v := range result {
		switch k {
		case "schedule_id":
			info.ScheduleID = v.(int64)
		case "started_id":
			info.StartedID = v.(int64)
		case "activity_id":
			info.ActivityID = v.(string)
		case "request_id":
			info.RequestID = v.(string)
		case "details":
			info.Details = v.([]byte)
		case "schedule_to_start_timeout":
			info.ScheduleToStartTimeout = int32(v.(int))
		case "schedule_to_close_timeout":
			info.ScheduleToCloseTimeout = int32(v.(int))
		case "start_to_close_timeout":
			info.StartToCloseTimeout = int32(v.(int))
		case "heart_beat_timeout":
			info.HeartbeatTimeout = int32(v.(int))
		case "cancel_requested":
			info.CancelRequested = v.(bool)
		case "cancel_request_id":
			info.CancelRequestID = v.(int64)
		}
	}

	return info
}

func createTimerInfo(result map[string]interface{}) *TimerInfo {
	info := &TimerInfo{}
	for k, v := range result {
		switch k {
		case "timer_id":
			info.TimerID = v.(string)
		case "started_id":
			info.StartedID = v.(int64)
		case "expiry_time":
			info.ExpiryTime = v.(time.Time)
		case "task_id":
			info.TaskID = v.(int64)
		}
	}
	return info
}

func createTaskInfo(result map[string]interface{}) *TaskInfo {
	info := &TaskInfo{}
	for k, v := range result {
		switch k {
		case "workflow_id":
			info.WorkflowID = v.(string)
		case "run_id":
			info.RunID = v.(gocql.UUID).String()
		case "task_id":
			info.TaskID = v.(int64)
		case "schedule_id":
			info.ScheduleID = v.(int64)
		}
	}

	return info
}

func createTimerTaskInfo(result map[string]interface{}) *TimerTaskInfo {
	info := &TimerTaskInfo{}
	for k, v := range result {
		switch k {
		case "workflow_id":
			info.WorkflowID = v.(string)
		case "run_id":
			info.RunID = v.(gocql.UUID).String()
		case "task_id":
			info.TaskID = v.(int64)
		case "type":
			info.TaskType = v.(int)
		case "timeout_type":
			info.TimeoutType = v.(int)
		case "event_id":
			info.EventID = v.(int64)
		}
	}

	return info
}
