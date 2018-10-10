// Copyright (c) 2018 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package sql

import (
	"database/sql"
	"fmt"
	"time"

	workflow "github.com/uber/cadence/.gen/go/shared"
	p "github.com/uber/cadence/common/persistence"

	"github.com/jmoiron/sqlx"
	"github.com/uber-common/bark"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/service/config"
)

type (
	// Implements ExecutionManager
	sqlExecutionManager struct {
		db      *sqlx.DB
		shardID int
		logger  bark.Logger
	}

	flatCreateWorkflowExecutionRequest struct {
		DomainID               string
		WorkflowID             string
		RunID                  string
		ParentDomainID         *string
		ParentWorkflowID       *string
		ParentRunID            *string
		InitiatedID            *int64
		TaskList               string
		WorkflowTypeName       string
		WorkflowTimeoutSeconds int64
		DecisionTimeoutValue   int64
		ExecutionContext       []byte
		NextEventID            int64
		LastProcessedEvent     int64
		// maybe i don't need this.
	}

	executionRow struct {
		DomainID                     string
		WorkflowID                   string
		RunID                        string
		ParentDomainID               *string
		ParentWorkflowID             *string
		ParentRunID                  *string
		InitiatedID                  *int64
		CompletionEvent              *[]byte
		TaskList                     string
		WorkflowTypeName             string
		WorkflowTimeoutSeconds       int64
		DecisionTaskTimeoutMinutes   int64
		ExecutionContext             *[]byte
		State                        int64
		CloseStatus                  int64
		StartVersion                 *int64
		CurrentVersion               *int64
		LastWriteVersion             *int64
		LastWriteEventID             *int64
		LastReplicationInfo          *[]byte
		LastFirstEventID             int64
		NextEventID                  int64
		LastProcessedEvent           int64
		StartTime                    time.Time
		LastUpdatedTime              time.Time
		CreateRequestID              string
		DecisionVersion              int64
		DecisionScheduleID           int64
		DecisionStartedID            int64
		DecisionRequestID            string
		DecisionTimeout              int64
		DecisionAttempt              int64
		DecisionTimestamp            int64
		CancelRequested              *int64
		CancelRequestID              *string
		StickyTaskList               string
		StickyScheduleToStartTimeout int64
		ClientLibraryVersion         string
		ClientFeatureVersion         string
		ClientImpl                   string
		ShardID                      int64
	}

	currentExecutionRow struct {
		ShardID    int64
		DomainID   string
		WorkflowID string

		RunID            string
		CreateRequestID  string
		State            int64
		CloseStatus      int64
		StartVersion     int64
		LastWriteVersion int64
	}

	transferTasksRow struct {
		p.TransferTaskInfo
		ShardID int
	}

	replicationTasksRow struct {
		DomainID            string
		WorkflowID          string
		RunID               string
		TaskID              int64
		TaskType            int
		FirstEventID        int64
		NextEventID         int64
		Version             int64
		LastReplicationInfo []byte
		ShardID             int
	}

	timerTasksRow struct {
		p.TimerTaskInfo
		ShardID int
	}

	updateExecutionRow struct {
		executionRow
		Condition int64
	}
)

const (
	executionsNonNullableColumns = `shard_id,
domain_id,
workflow_id,
run_id,
task_list,
workflow_type_name,
workflow_timeout_seconds,
decision_task_timeout_minutes,
state,
close_status,
last_first_event_id,
next_event_id,
last_processed_event,
start_time,
last_updated_time,
create_request_id,
decision_version,
decision_schedule_id,
decision_started_id,
decision_timeout,
decision_attempt,
decision_timestamp,
sticky_task_list,
sticky_schedule_to_start_timeout,
client_library_version,
client_feature_version,
client_impl`

	executionsNonNullableColumnsTags = `:shard_id,
:domain_id,
:workflow_id,
:run_id,
:task_list,
:workflow_type_name,
:workflow_timeout_seconds,
:decision_task_timeout_minutes,
:state,
:close_status,
:last_first_event_id,
:next_event_id,
:last_processed_event,
:start_time,
:last_updated_time,
:create_request_id,
:decision_version,
:decision_schedule_id,
:decision_started_id,
:decision_timeout,
:decision_attempt,
:decision_timestamp,
:sticky_task_list,
:sticky_schedule_to_start_timeout,
:client_library_version,
:client_feature_version,
:client_impl`

	executionsBlobColumns = `completion_event,
execution_context`

	executionsBlobColumnsTags = `:completion_event,
:execution_context`

	// Excluding completion_event
	executionsNonblobParentColumns = `parent_domain_id,
parent_workflow_id,
parent_run_id,
initiated_id`

	executionsNonblobParentColumnsTags = `:parent_domain_id,
:parent_workflow_id,
:parent_run_id,
:initiated_id`

	executionsCancelColumns = `cancel_requested,
cancel_request_id`

	executionsCancelColumnsTags = `:cancel_requested,
:cancel_request_id`

	executionsReplicationStateColumns     = `start_version, current_version, last_write_version, last_write_event_id, last_replication_info`
	executionsReplicationStateColumnsTags = `:start_version, :current_version, :last_write_version, :last_write_event_id, :last_replication_info`

	createExecutionSQLQuery = `INSERT INTO executions
(` + executionsNonNullableColumns + `,` +
		executionsNonblobParentColumns +
		`,
execution_context,
cancel_requested,
cancel_request_id,` +
		executionsReplicationStateColumns +
		`)
VALUES
(` + executionsNonNullableColumnsTags + `,` +
		executionsNonblobParentColumnsTags + `,
:execution_context,
:cancel_requested,
:cancel_request_id,` +
		executionsReplicationStateColumnsTags +
		`)
`

	updateExecutionSQLQuery = `UPDATE executions SET
domain_id = :domain_id,
workflow_id = :workflow_id,
run_id = :run_id,
parent_domain_id = :parent_domain_id,
parent_workflow_id = :parent_workflow_id,
parent_run_id = :parent_run_id,
initiated_id = :initiated_id,
completion_event = :completion_event,
task_list = :task_list,
workflow_type_name = :workflow_type_name,
workflow_timeout_seconds = :workflow_timeout_seconds,
decision_task_timeout_minutes = :decision_task_timeout_minutes,
execution_context = :execution_context,
state = :state,
close_status = :close_status,
last_first_event_id = :last_first_event_id,
next_event_id = :next_event_id,
last_processed_event = :last_processed_event,
start_time = :start_time,
last_updated_time = :last_updated_time,
create_request_id = :create_request_id,
decision_version = :decision_version,
decision_schedule_id = :decision_schedule_id,
decision_started_id = :decision_started_id,
decision_request_id = :decision_request_id,
decision_timeout = :decision_timeout,
decision_attempt = :decision_attempt,
decision_timestamp = :decision_timestamp,
cancel_requested = :cancel_requested,
cancel_request_id = :cancel_request_id,
sticky_task_list = :sticky_task_list,
sticky_schedule_to_start_timeout = :sticky_schedule_to_start_timeout,
client_library_version = :client_library_version,
client_feature_version = :client_feature_version,
client_impl = :client_impl,
start_version = :start_version,
current_version = :current_version,
last_write_version = :last_write_version,
last_write_event_id = :last_write_event_id,
last_replication_info = :last_replication_info
WHERE
shard_id = :shard_id AND
domain_id = :domain_id AND
workflow_id = :workflow_id AND
run_id = :run_id
`

	getExecutionSQLQuery = `SELECT ` +
		executionsNonNullableColumns + "," +
		executionsBlobColumns + "," +
		executionsNonblobParentColumns + "," +
		executionsCancelColumns + "," +
		executionsReplicationStateColumns +
		` FROM executions WHERE
shard_id = ? AND
domain_id = ? AND
workflow_id = ? AND
run_id = ?`

	deleteExecutionSQLQuery = `DELETE FROM executions WHERE
shard_id = ? AND
domain_id = ? AND
workflow_id = ? AND
run_id = ?`

	transferTaskInfoColumns = `task_id, 
domain_id,
workflow_id,
run_id,
task_type,
target_domain_id,
target_workflow_id,
target_run_id,
target_child_workflow_only,
task_list,
schedule_id,
version`

	transferTaskInfoColumnsTags = `:task_id,
:domain_id,
:workflow_id,
:run_id,
:task_type,
:target_domain_id,
:target_workflow_id,
:target_run_id,
:target_child_workflow_only,
:task_list,
:schedule_id,
:version`

	transferTasksColumns = `shard_id,` + transferTaskInfoColumns

	transferTasksColumnsTags = `:shard_id,` + transferTaskInfoColumnsTags

	getTransferTasksSQLQuery = `SELECT
` + transferTaskInfoColumns +
		`
FROM transfer_tasks WHERE
shard_id = ? AND
task_id > ? AND
task_id <= ?
`

	createCurrentExecutionSQLQuery = `INSERT INTO current_executions
(shard_id, domain_id, workflow_id, run_id, create_request_id, state, close_status, start_version) VALUES
(:shard_id, :domain_id, :workflow_id, :run_id, :create_request_id, :state, :close_status, :start_version)`

	getCurrentExecutionSQLQuery = `SELECT
shard_id, domain_id, workflow_id, run_id, state, close_status, last_write_version
FROM current_executions
WHERE
shard_id = ? AND domain_id = ? AND workflow_id = ?
`

	// The continueAsNewLockRunIDSQLQuery and continueAsNewUpdateCurrentExecutionsSQLQuery together comprise ContinueAsNew.
	// The updates must be executed only after locking current_run_id of
	// the current_executions row that we are going to update,
	// and asserting that it is PreviousRunId.
	continueAsNewLockRunIDSQLQuery = `SELECT run_id FROM current_executions WHERE
shard_id = ? AND
domain_id = ? AND
workflow_id = ?
FOR UPDATE`

	// The workflowIDReuseSQLQuery and continueAsNewUpdateCurrentExecutionsSQLQuery together comprise workflowIDReuse.
	// The updates must be executed only after locking current_run_id, current_state and current_last_write_version of
	// the current_executions row that we are going to update,
	// and asserting that it is PreviousRunId.
	workflowIDReuseSQLQuery = `SELECT run_id, state, last_write_version FROM current_executions WHERE
shard_id = ? AND
domain_id = ? AND
workflow_id = ?
FOR UPDATE`

	continueAsNewUpdateCurrentExecutionsSQLQuery = `UPDATE current_executions SET
run_id = :run_id,
create_request_id = :create_request_id,
state = :state,
close_status = :close_status,
start_version = :start_version
WHERE
shard_id = :shard_id AND
domain_id = :domain_id AND
workflow_id = :workflow_id
`

	createTransferTasksSQLQuery = `INSERT INTO transfer_tasks
(` + transferTasksColumns + `)
VALUES
(` + transferTasksColumnsTags + `
)
`

	completeTransferTaskSQLQuery      = `DELETE FROM transfer_tasks WHERE shard_id = :shard_id AND task_id = :task_id`
	rangeCompleteTransferTaskSQLQuery = `DELETE FROM transfer_tasks WHERE shard_id = ? AND task_id > ? AND task_id <= ?`

	replicationTaskInfoColumns = `task_id,
domain_id,
workflow_id,
run_id,
task_type,
first_event_id,
next_event_id,
version,
last_replication_info`

	replicationTaskInfoColumnsTags = `:task_id,
:domain_id,
:workflow_id,
:run_id,
:task_type,
:first_event_id,
:next_event_id,
:version,
:last_replication_info`

	replicationTasksColumns     = `shard_id, ` + replicationTaskInfoColumns
	replicationTasksColumnsTags = `:shard_id, ` + replicationTaskInfoColumnsTags

	createReplicationTasksSQLQuery = `INSERT INTO replication_tasks (` +
		replicationTasksColumns + `) VALUES(` +
		replicationTasksColumnsTags + `)`

	getReplicationTasksSQLQuery = `SELECT ` + replicationTaskInfoColumns +
		`
FROM replication_tasks WHERE
shard_id = ? AND
task_id > ? AND
task_id <= ?`

	timerTaskInfoColumns     = `visibility_timestamp, task_id, domain_id, workflow_id, run_id, task_type, timeout_type, event_id, schedule_attempt, version`
	timerTaskInfoColumnsTags = `:visibility_timestamp, :task_id, :domain_id, :workflow_id, :run_id, :task_type, :timeout_type, :event_id, :schedule_attempt, :version`
	timerTasksColumns        = `shard_id,` + timerTaskInfoColumns
	timerTasksColumnsTags    = `:shard_id,` + timerTaskInfoColumnsTags
	createTimerTasksSQLQuery = `INSERT INTO timer_tasks (` +
		timerTasksColumns + `) VALUES (` +
		timerTasksColumnsTags + `)`
	getTimerTasksSQLQuery = `SELECT ` + timerTaskInfoColumns +
		`
FROM timer_tasks WHERE
shard_id = ? AND
visibility_timestamp >= ? AND
visibility_timestamp < ?`
	completeTimerTaskSQLQuery       = `DELETE FROM timer_tasks WHERE shard_id = ? AND visibility_timestamp = ? AND task_id = ?`
	rangeCompleteTimerTaskSQLQuery  = `DELETE FROM timer_tasks WHERE shard_id = ? AND visibility_timestamp >= ? AND visibility_timestamp < ?`
	lockAndCheckNextEventIDSQLQuery = `SELECT next_event_id FROM executions WHERE
shard_id = ? AND
domain_id = ? AND
workflow_id = ? AND
run_id = ?
FOR UPDATE`
)

func (m *sqlExecutionManager) Close() {
	if m.db != nil {
		m.db.Close()
	}
}

func (m *sqlExecutionManager) CreateWorkflowExecution(request *p.CreateWorkflowExecutionRequest) (*p.CreateWorkflowExecutionResponse, error) {
	/*
		(x) make a transaction
		( ) check for a parent
		(x) check for ContinueAsNew, update the current exec or create one for this new workflow
		(x) create a workflow with/without cross dc
	*/

	tx, err := m.db.Beginx()
	if err != nil {
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("CreateWorkflowExecution failed. Failed to start transaction. Error: %v", err),
		}
	}
	defer tx.Rollback()

	if row, err := getCurrentExecutionIfExists(tx, int64(m.shardID), request.DomainID, *request.Execution.WorkflowId); err == nil {
		return nil, &p.WorkflowExecutionAlreadyStartedError{
			Msg:              fmt.Sprintf("Workflow execution already running. WorkflowId: %v", row.WorkflowID),
			StartRequestID:   row.CreateRequestID,
			RunID:            row.RunID,
			State:            int(row.State),
			CloseStatus:      int(row.CloseStatus),
			LastWriteVersion: row.LastWriteVersion,
		}
	}

	if err := createCurrentExecution(tx, request, m.shardID); err != nil {
		return nil, err
	}

	if err := createExecution(tx, request, m.shardID, time.Now()); err != nil {
		return nil, err
	}

	if err := createTransferTasks(tx, request.TransferTasks, m.shardID, request.DomainID, *request.Execution.WorkflowId, *request.Execution.RunId); err != nil {
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("CreateWorkflowExecution operation failed. Failed to create transfer tasks. Error: %v", err),
		}
	}

	if err := createReplicationTasks(tx,
		request.ReplicationTasks,
		m.shardID,
		request.DomainID,
		*request.Execution.WorkflowId,
		*request.Execution.RunId); err != nil {
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("CreateWorkflowExecution operation failed. Failed to create replication tasks. Error: %v", err),
		}
	}

	if err := createTimerTasks(tx,
		request.TimerTasks,
		nil,
		m.shardID,
		request.DomainID,
		*request.Execution.WorkflowId,
		*request.Execution.RunId); err != nil {
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("CreateWorkflowExecution operation failed. Failed to create timer tasks. Error: %v", err),
		}
	}

	if err := lockShard(tx, m.shardID, request.RangeID); err != nil {
		switch err.(type) {
		case *p.ShardOwnershipLostError:
			return nil, err
		default:
			return nil, &workflow.InternalServiceError{
				Message: fmt.Sprintf("CreateWorkflowExecution operation failed. Error: %v", err),
			}
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("CreateWorkflowExecution operation failed. Failed to commit transaction. Error: %v", err),
		}
	}

	return &p.CreateWorkflowExecutionResponse{}, nil
}

func (m *sqlExecutionManager) GetWorkflowExecution(request *p.GetWorkflowExecutionRequest) (*p.GetWorkflowExecutionResponse, error) {
	tx, err := m.db.Beginx()
	if err != nil {
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("GetWorkflowExecution operation failed. Failed to start transaction. Error: %v", err),
		}
	}
	defer tx.Rollback()

	// Have to lock next_event_id so that things aren't modified while we are getting
	// all the other parts of mutable state
	// TODO Replace with repeatable read transaction level

	if _, err := lockNextEventID(tx, m.shardID, request.DomainID, *request.Execution.WorkflowId, *request.Execution.RunId); err != nil {
		switch err.(type) {
		case *workflow.EntityNotExistsError:
			return nil, &workflow.EntityNotExistsError{
				Message: fmt.Sprintf("GetWorkflowExecution operation failed. Error: %v", err),
			}
		default:
			return nil, &workflow.InternalServiceError{
				Message: fmt.Sprintf("GetWorkflowExecution operation failed. Failed to write-lock executions row. Error: %v", err),
			}
		}
	}

	var execution executionRow
	if err := sqlx.Get(tx, &execution, getExecutionSQLQuery,
		m.shardID,
		request.DomainID,
		*request.Execution.WorkflowId,
		*request.Execution.RunId); err != nil {
		if err == sql.ErrNoRows {
			return nil, &workflow.EntityNotExistsError{
				Message: fmt.Sprintf("Workflow execution not found.  WorkflowId: %v, RunId: %v",
					*request.Execution.WorkflowId,
					*request.Execution.RunId),
			}
		}
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("GetWorkflowExecution failed. Error: %v", err),
		}
	}

	var state p.WorkflowMutableState
	state.ExecutionInfo = &p.WorkflowExecutionInfo{
		DomainID:                     execution.DomainID,
		WorkflowID:                   execution.WorkflowID,
		RunID:                        execution.RunID,
		TaskList:                     execution.TaskList,
		WorkflowTypeName:             execution.WorkflowTypeName,
		WorkflowTimeout:              int32(execution.WorkflowTimeoutSeconds),
		DecisionTimeoutValue:         int32(execution.DecisionTaskTimeoutMinutes),
		State:                        int(execution.State),
		CloseStatus:                  int(execution.CloseStatus),
		LastFirstEventID:             execution.LastFirstEventID,
		NextEventID:                  execution.NextEventID,
		LastProcessedEvent:           execution.LastProcessedEvent,
		StartTimestamp:               execution.StartTime,
		LastUpdatedTimestamp:         execution.LastUpdatedTime,
		CreateRequestID:              execution.CreateRequestID,
		DecisionVersion:              execution.DecisionVersion,
		DecisionScheduleID:           execution.DecisionScheduleID,
		DecisionStartedID:            execution.DecisionStartedID,
		DecisionRequestID:            execution.DecisionRequestID,
		DecisionTimeout:              int32(execution.DecisionTimeout),
		DecisionAttempt:              execution.DecisionAttempt,
		DecisionTimestamp:            execution.DecisionTimestamp,
		StickyTaskList:               execution.StickyTaskList,
		StickyScheduleToStartTimeout: int32(execution.StickyScheduleToStartTimeout),
		ClientLibraryVersion:         execution.ClientLibraryVersion,
		ClientFeatureVersion:         execution.ClientFeatureVersion,
		ClientImpl:                   execution.ClientImpl,
	}

	if execution.ExecutionContext != nil {
		state.ExecutionInfo.ExecutionContext = *execution.ExecutionContext
	}

	state.ReplicationState = &p.ReplicationState{}
	if execution.StartVersion != nil {
		state.ReplicationState.StartVersion = *execution.StartVersion
	}
	if execution.CurrentVersion != nil {
		state.ReplicationState.CurrentVersion = *execution.CurrentVersion
	}
	if execution.LastWriteVersion != nil {
		state.ReplicationState.LastWriteVersion = *execution.LastWriteVersion
	}
	if execution.LastWriteEventID != nil {
		state.ReplicationState.LastWriteEventID = *execution.LastWriteEventID
	}
	if execution.LastReplicationInfo != nil {
		state.ReplicationState.LastReplicationInfo = make(map[string]*p.ReplicationInfo)
		if err := gobDeserialize(*execution.LastReplicationInfo, &state.ReplicationState.LastReplicationInfo); err != nil {
			return nil, &workflow.InternalServiceError{
				Message: fmt.Sprintf("GetWorkflowExecution failed. Failed to deserialize LastReplicationInfo. Error: %v", err),
			}
		}
	}

	if execution.ParentDomainID != nil {
		state.ExecutionInfo.ParentDomainID = *execution.ParentDomainID
		state.ExecutionInfo.ParentWorkflowID = *execution.ParentWorkflowID
		state.ExecutionInfo.ParentRunID = *execution.ParentRunID
		state.ExecutionInfo.InitiatedID = *execution.InitiatedID
		if state.ExecutionInfo.CompletionEvent != nil {
			state.ExecutionInfo.CompletionEvent = nil
		}
	}

	if execution.CancelRequested != nil && (*execution.CancelRequested != 0) {
		state.ExecutionInfo.CancelRequested = true
		state.ExecutionInfo.CancelRequestID = *execution.CancelRequestID
	}

	{
		var err error
		state.ActivitInfos, err = getActivityInfoMap(tx,
			m.shardID,
			request.DomainID,
			*request.Execution.WorkflowId,
			*request.Execution.RunId)
		if err != nil {
			return nil, &workflow.InternalServiceError{
				Message: fmt.Sprintf("GetWorkflowExecution failed. Failed to get activity info. Error: %v", err),
			}
		}
	}

	{
		var err error
		state.TimerInfos, err = getTimerInfoMap(tx,
			m.shardID,
			request.DomainID,
			*request.Execution.WorkflowId,
			*request.Execution.RunId)
		if err != nil {
			return nil, &workflow.InternalServiceError{
				Message: fmt.Sprintf("GetWorkflowExecution failed. Failed to get timer info. Error: %v", err),
			}
		}
	}

	{
		var err error
		state.ChildExecutionInfos, err = getChildExecutionInfoMap(tx,
			m.shardID,
			request.DomainID,
			*request.Execution.WorkflowId,
			*request.Execution.RunId)
		if err != nil {
			return nil, &workflow.InternalServiceError{
				Message: fmt.Sprintf("GetWorkflowExecution failed. Failed to get child execution info. Error: %v", err),
			}
		}
	}

	{
		var err error
		state.RequestCancelInfos, err = getRequestCancelInfoMap(tx,
			m.shardID,
			request.DomainID,
			*request.Execution.WorkflowId,
			*request.Execution.RunId)
		if err != nil {
			return nil, &workflow.InternalServiceError{
				Message: fmt.Sprintf("GetWorkflowExecution failed. Failed to get request cancel info. Error: %v", err),
			}
		}
	}

	{
		var err error
		state.SignalInfos, err = getSignalInfoMap(tx,
			m.shardID,
			request.DomainID,
			*request.Execution.WorkflowId,
			*request.Execution.RunId)
		if err != nil {
			return nil, &workflow.InternalServiceError{
				Message: fmt.Sprintf("GetWorkflowExecution failed. Failed to get signal info. Error: %v", err),
			}
		}
	}

	{
		var err error
		state.BufferedReplicationTasks, err = getBufferedReplicationTasks(tx,
			m.shardID,
			request.DomainID,
			*request.Execution.WorkflowId,
			*request.Execution.RunId)
		if err != nil {
			return nil, &workflow.InternalServiceError{
				Message: fmt.Sprintf("GetWorkflowExecution failed. Failed to get buffered replication tasks. Error: %v", err),
			}
		}
	}

	{
		var err error
		state.SignalRequestedIDs, err = getSignalsRequested(tx,
			m.shardID,
			request.DomainID,
			*request.Execution.WorkflowId,
			*request.Execution.RunId)
		if err != nil {
			return nil, &workflow.InternalServiceError{
				Message: fmt.Sprintf("GetWorkflowExecution failed. Failed to get signals requested. Error: %v", err),
			}
		}
	}

	return &p.GetWorkflowExecutionResponse{State: &state}, nil
}

func (m *sqlExecutionManager) UpdateWorkflowExecution(request *p.UpdateWorkflowExecutionRequest) error {
	tx, err := m.db.Beginx()
	if err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("UpdateWorkflowExecution operation failed. Failed to begin transaction. Erorr: %v", err),
		}
	}
	defer tx.Rollback()

	if err := createTransferTasks(tx,
		request.TransferTasks,
		m.shardID,
		request.ExecutionInfo.DomainID,
		request.ExecutionInfo.WorkflowID,
		request.ExecutionInfo.RunID); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("UpdateWorkflowExecution operation failed. Failed to create transfer tasks. Error: %v", err),
		}
	}

	if err := createReplicationTasks(tx,
		request.ReplicationTasks,
		m.shardID,
		request.ExecutionInfo.DomainID,
		request.ExecutionInfo.WorkflowID,
		request.ExecutionInfo.RunID); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("UpdateWorkflowExecution operation failed. Failed to create replication tasks. Error: %v", err),
		}
	}

	if err := createTimerTasks(tx,
		request.TimerTasks,
		request.DeleteTimerTask,
		m.shardID,
		request.ExecutionInfo.DomainID,
		request.ExecutionInfo.WorkflowID,
		request.ExecutionInfo.RunID); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("UpdateWorkflowExecution operation failed. Failed to create timer tasks. Error: %v", err),
		}
	}

	// TODO Note to self: The only reason that this is up here because a certain test was required
	// TODO to fail due to ShardOwnershipLostError and not due to ConditionFailedError
	// TODO Otherwise, it'd probably be OK to put this right before the .Commit()
	if err := lockShard(tx, m.shardID, request.RangeID); err != nil {
		switch err.(type) {
		case *p.ShardOwnershipLostError:
			return err
		default:
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("UpdateWorkflowExecution operation failed. Error: %v", err),
			}
		}
	}

	// TODO Remove me if UPDATE holds the lock to the end of a transaction
	if err := lockAndCheckNextEventID(tx,
		m.shardID,
		request.ExecutionInfo.DomainID,
		request.ExecutionInfo.WorkflowID,
		request.ExecutionInfo.RunID,
		request.Condition); err != nil {
		switch err.(type) {
		case *p.ConditionFailedError:
			return err
		default:
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("UpdateWorkflowExecution operation failed. Failed to lock executions row. Error: %v", err),
			}
		}
	}

	if err := updateExecution(tx, request.ExecutionInfo, request.ReplicationState, request.Condition); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("UpdateWorkflowExecution operation failed. Failed to update executions row. Erorr: %v", err),
		}
	}

	if err := updateActivityInfos(tx,
		request.UpsertActivityInfos,
		request.DeleteActivityInfos,
		m.shardID,
		request.ExecutionInfo.DomainID,
		request.ExecutionInfo.WorkflowID,
		request.ExecutionInfo.RunID); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("UpdateWorkflowExecution operation failed. Error: %v", err),
		}
	}

	if err := updateTimerInfos(tx,
		request.UpserTimerInfos,
		request.DeleteTimerInfos,
		m.shardID,
		request.ExecutionInfo.DomainID,
		request.ExecutionInfo.WorkflowID,
		request.ExecutionInfo.RunID); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("UpdateWorkflowExecution operation failed. Error: %v", err),
		}
	}

	if err := updateChildExecutionInfos(tx,
		request.UpsertChildExecutionInfos,
		request.DeleteChildExecutionInfo,
		m.shardID,
		request.ExecutionInfo.DomainID,
		request.ExecutionInfo.WorkflowID,
		request.ExecutionInfo.RunID); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("UpdateWorkflowExecution operation failed. Error: %v", err),
		}
	}

	if err := updateRequestCancelInfos(tx,
		request.UpsertRequestCancelInfos,
		request.DeleteRequestCancelInfo,
		m.shardID,
		request.ExecutionInfo.DomainID,
		request.ExecutionInfo.WorkflowID,
		request.ExecutionInfo.RunID); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("UpdateWorkflowExecution operation failed. Error: %v", err),
		}
	}

	if err := updateSignalInfos(tx,
		request.UpsertSignalInfos,
		request.DeleteSignalInfo,
		m.shardID,
		request.ExecutionInfo.DomainID,
		request.ExecutionInfo.WorkflowID,
		request.ExecutionInfo.RunID); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("UpdateWorkflowExecution operation failed. Error: %v", err),
		}
	}

	if err := updateBufferedReplicationTasks(tx,
		request.NewBufferedReplicationTask,
		request.DeleteBufferedReplicationTask,
		m.shardID,
		request.ExecutionInfo.DomainID,
		request.ExecutionInfo.WorkflowID,
		request.ExecutionInfo.RunID); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("UpdateWorkflowExecution operation failed. Error: %v", err),
		}
	}

	if err := updateSignalsRequested(tx,
		request.UpsertSignalRequestedIDs,
		request.DeleteSignalRequestedID,
		m.shardID,
		request.ExecutionInfo.DomainID,
		request.ExecutionInfo.WorkflowID,
		request.ExecutionInfo.RunID); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("UpdateWorkflowExecution operation failed. Error: %v", err),
		}
	}

	if request.ContinueAsNew != nil {
		if err := createCurrentExecution(tx, request.ContinueAsNew, m.shardID); err != nil {
			return err
		}

		if err := createExecution(tx, request.ContinueAsNew, m.shardID, time.Now()); err != nil {
			return err
		}

		if err := createTransferTasks(tx,
			request.ContinueAsNew.TransferTasks,
			m.shardID,
			request.ContinueAsNew.DomainID,
			request.ContinueAsNew.Execution.GetWorkflowId(),
			request.ContinueAsNew.Execution.GetRunId()); err != nil {
			return err
		}

		if err := createTimerTasks(tx,
			request.ContinueAsNew.TimerTasks,
			nil,
			m.shardID,
			request.ContinueAsNew.DomainID,
			request.ContinueAsNew.Execution.GetWorkflowId(),
			request.ContinueAsNew.Execution.GetRunId()); err != nil {
			return err
		}
	} else {
		executionInfo := request.ExecutionInfo
		startVersion := common.EmptyVersion
		lastWriteVersion := common.EmptyVersion
		if request.ReplicationState != nil {
			startVersion = request.ReplicationState.StartVersion
			lastWriteVersion = request.ReplicationState.LastWriteVersion
		}
		if request.FinishExecution {
			// TODO when finish execution, the current record should be marked with a TTL
		} else {
			// this is only to update the current record
			if err := continueAsNew(tx,
				m.shardID,
				executionInfo.DomainID,
				executionInfo.WorkflowID,
				executionInfo.RunID,
				executionInfo.RunID,
				executionInfo.CreateRequestID,
				int64(executionInfo.State),
				int64(executionInfo.CloseStatus),
				startVersion,
				lastWriteVersion); err != nil {
				return &workflow.InternalServiceError{
					Message: fmt.Sprintf("UpdateWorkflowExecution operation failed. Failed to update current execution. Error: %v", err),
				}
			}
		}
	}

	if err := tx.Commit(); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("UpdateWorkflowExecution operation failed. Failed to commit transaction. Error: %v", err),
		}
	}

	return nil
}

func (m *sqlExecutionManager) ResetMutableState(request *p.ResetMutableStateRequest) error {
	tx, err := m.db.Beginx()
	if err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("ResetMutableState operation failed. Failed to start transaction. Error: %v", err),
		}
	}
	defer tx.Rollback()

	// TODO Is there a way to modify the various map tables without fear of other people adding rows after we delete, without locking the executions row?
	if err := lockAndCheckNextEventID(tx,
		m.shardID,
		request.ExecutionInfo.DomainID,
		request.ExecutionInfo.WorkflowID,
		request.ExecutionInfo.RunID,
		request.Condition); err != nil {
		switch err.(type) {
		case *p.ConditionFailedError:
			return err
		default:
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("ResetMutableState operation failed. Failed to lock executions row. Error: %v", err),
			}
		}
	}

	if err := updateExecution(tx, request.ExecutionInfo, request.ReplicationState, request.Condition); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("UpdateWorkflowExecution operation failed. Failed to update executions row. Erorr: %v", err),
		}
	}

	if err := deleteActivityInfoMap(tx,
		m.shardID,
		request.ExecutionInfo.DomainID,
		request.ExecutionInfo.WorkflowID,
		request.ExecutionInfo.RunID); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("ResetMutableState operation failed. Failed to clear activity info map. Error: %v", err),
		}
	}

	if err := updateActivityInfos(tx,
		request.InsertActivityInfos,
		nil,
		m.shardID,
		request.ExecutionInfo.DomainID,
		request.ExecutionInfo.WorkflowID,
		request.ExecutionInfo.RunID); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("ResetMutableState operation failed. Failed to insert into activity info map after clearing. Error: %v", err),
		}
	}

	if err := deleteTimerInfoMap(tx,
		m.shardID,
		request.ExecutionInfo.DomainID,
		request.ExecutionInfo.WorkflowID,
		request.ExecutionInfo.RunID); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("ResetMutableState operation failed. Failed to clear timer info map. Error: %v", err),
		}
	}

	if err := updateTimerInfos(tx,
		request.InsertTimerInfos,
		nil,
		m.shardID,
		request.ExecutionInfo.DomainID,
		request.ExecutionInfo.WorkflowID,
		request.ExecutionInfo.RunID); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("ResetMutableState operation failed. Failed to insert into timer info map after clearing. Error: %v", err),
		}
	}

	if err := deleteChildExecutionInfoMap(tx,
		m.shardID,
		request.ExecutionInfo.DomainID,
		request.ExecutionInfo.WorkflowID,
		request.ExecutionInfo.RunID); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("ResetMutableState operation failed. Failed to clear child execution info map. Error: %v", err),
		}
	}

	if err := updateChildExecutionInfos(tx,
		request.InsertChildExecutionInfos,
		nil,
		m.shardID,
		request.ExecutionInfo.DomainID,
		request.ExecutionInfo.WorkflowID,
		request.ExecutionInfo.RunID); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("ResetMutableState operation failed. Failed to insert into activity info map after clearing. Error: %v", err),
		}
	}

	if err := deleteRequestCancelInfoMap(tx,
		m.shardID,
		request.ExecutionInfo.DomainID,
		request.ExecutionInfo.WorkflowID,
		request.ExecutionInfo.RunID); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("ResetMutableState operation failed. Failed to clear request cancel info map. Error: %v", err),
		}
	}

	if err := updateRequestCancelInfos(tx,
		request.InsertRequestCancelInfos,
		nil,
		m.shardID,
		request.ExecutionInfo.DomainID,
		request.ExecutionInfo.WorkflowID,
		request.ExecutionInfo.RunID); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("ResetMutableState operation failed. Failed to insert into request cancel info map after clearing. Error: %v", err),
		}
	}

	if err := deleteSignalInfoMap(tx,
		m.shardID,
		request.ExecutionInfo.DomainID,
		request.ExecutionInfo.WorkflowID,
		request.ExecutionInfo.RunID); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("ResetMutableState operation failed. Failed to clear signal info map. Error: %v", err),
		}
	}

	if err := updateSignalInfos(tx,
		request.InsertSignalInfos,
		nil,
		m.shardID,
		request.ExecutionInfo.DomainID,
		request.ExecutionInfo.WorkflowID,
		request.ExecutionInfo.RunID); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("ResetMutableState operation failed. Failed to insert into signal info map after clearing. Error: %v", err),
		}
	}

	if err := deleteSignalsRequestedSet(tx,
		m.shardID,
		request.ExecutionInfo.DomainID,
		request.ExecutionInfo.WorkflowID,
		request.ExecutionInfo.RunID); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("ResetMutableState operation failed. Failed to clear signals requested set. Error: %v", err),
		}
	}

	if err := updateSignalsRequested(tx,
		request.InsertSignalRequestedIDs,
		"",
		m.shardID,
		request.ExecutionInfo.DomainID,
		request.ExecutionInfo.WorkflowID,
		request.ExecutionInfo.RunID); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("ResetMutableState operation failed. Failed to insert into signals requested set after clearing. Error: %v", err),
		}
	}

	if err := lockShard(tx, m.shardID, request.RangeID); err != nil {
		switch err.(type) {
		case *p.ShardOwnershipLostError:
			return err
		default:
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("ResetMutableState operation failed. Error: %v", err),
			}
		}
	}

	if err := tx.Commit(); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("ResetMutableState operation failed. Failed to commit transaction. Error: %v", err),
		}
	}

	return nil
}

func (m *sqlExecutionManager) DeleteWorkflowExecution(request *p.DeleteWorkflowExecutionRequest) error {
	if _, err := m.db.Exec(deleteExecutionSQLQuery, m.shardID, request.DomainID, request.WorkflowID, request.RunID); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("DeleteWorkflowExecution operation failed. Error: %v", err),
		}
	}
	return nil
}

func (m *sqlExecutionManager) GetCurrentExecution(request *p.GetCurrentExecutionRequest) (*p.GetCurrentExecutionResponse, error) {
	var row currentExecutionRow
	if err := m.db.Get(&row, getCurrentExecutionSQLQuery, m.shardID, request.DomainID, request.WorkflowID); err != nil {
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("GetCurrentExecution operation failed. Error: %v", err),
		}
	}
	return &p.GetCurrentExecutionResponse{
		StartRequestID: row.CreateRequestID,
		RunID:          row.RunID,
		State:          int(row.State),
		CloseStatus:    int(row.CloseStatus),
	}, nil
}

func (m *sqlExecutionManager) GetTransferTasks(request *p.GetTransferTasksRequest) (*p.GetTransferTasksResponse, error) {
	var resp p.GetTransferTasksResponse
	if err := m.db.Select(&resp.Tasks,
		getTransferTasksSQLQuery,
		m.shardID,
		request.ReadLevel,
		request.MaxReadLevel); err != nil {
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("GetTransferTasks operation failed. Select failed. Error: %v", err),
		}
	}

	return &resp, nil
}

func (m *sqlExecutionManager) CompleteTransferTask(request *p.CompleteTransferTaskRequest) error {
	if _, err := m.db.NamedExec(completeTransferTaskSQLQuery, &struct {
		ShardID int64
		TaskID  int64
	}{int64(m.shardID), request.TaskID}); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("CompleteTransferTask operation failed. Error: %v", err),
		}
	}
	return nil
}

func (m *sqlExecutionManager) RangeCompleteTransferTask(request *p.RangeCompleteTransferTaskRequest) error {
	if _, err := m.db.Exec(rangeCompleteTransferTaskSQLQuery, m.shardID, request.ExclusiveBeginTaskID, request.InclusiveEndTaskID); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("RangeCompleteTransferTask operation failed. Error: %v", err),
		}
	}
	return nil
}

func (m *sqlExecutionManager) GetReplicationTasks(request *p.GetReplicationTasksRequest) (*p.GetReplicationTasksResponse, error) {
	var rows []replicationTasksRow

	if err := m.db.Select(&rows,
		getReplicationTasksSQLQuery,
		m.shardID,
		request.ReadLevel,
		request.MaxReadLevel); err != nil {
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("GetReplicationTasks operation failed. Select failed: %v", err),
		}
	}

	var tasks = make([]*p.ReplicationTaskInfo, len(rows))

	for i, row := range rows {
		var lastReplicationInfo map[string]*p.ReplicationInfo
		if err := gobDeserialize(row.LastReplicationInfo, &lastReplicationInfo); err != nil {
			return nil, &workflow.InternalServiceError{
				Message: fmt.Sprintf("GetReplicationTasks operation failed. Failed to deserialize LastReplicationInfo. Error: %v", err),
			}
		}

		tasks[i] = &p.ReplicationTaskInfo{
			DomainID:            row.DomainID,
			WorkflowID:          row.WorkflowID,
			RunID:               row.RunID,
			TaskID:              row.TaskID,
			TaskType:            row.TaskType,
			FirstEventID:        row.FirstEventID,
			NextEventID:         row.NextEventID,
			Version:             row.Version,
			LastReplicationInfo: lastReplicationInfo,
		}

	}

	return &p.GetReplicationTasksResponse{
		Tasks: tasks,
	}, nil
}

func (m *sqlExecutionManager) CompleteReplicationTask(request *p.CompleteReplicationTaskRequest) error {
	return nil
}

func (m *sqlExecutionManager) GetTimerIndexTasks(request *p.GetTimerIndexTasksRequest) (*p.GetTimerIndexTasksResponse, error) {
	var resp p.GetTimerIndexTasksResponse

	if err := m.db.Select(&resp.Timers, getTimerTasksSQLQuery,
		m.shardID,
		request.MinTimestamp,
		request.MaxTimestamp); err != nil {
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("GetTimerTasks operation failed. Select failed. Error: %v", err),
		}
	}

	return &resp, nil
}

func (m *sqlExecutionManager) CompleteTimerTask(request *p.CompleteTimerTaskRequest) error {
	if _, err := m.db.Exec(completeTimerTaskSQLQuery, m.shardID, request.VisibilityTimestamp, request.TaskID); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("CompleteTimerTask operation failed. Error: %v", err),
		}
	}
	return nil
}

func (m *sqlExecutionManager) RangeCompleteTimerTask(request *p.RangeCompleteTimerTaskRequest) error {
	start := p.UnixNanoToDBTimestamp(request.InclusiveBeginTimestamp.UnixNano())
	end := p.UnixNanoToDBTimestamp(request.ExclusiveEndTimestamp.UnixNano())
	if _, err := m.db.Exec(rangeCompleteTimerTaskSQLQuery, m.shardID, start, end); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("CompleteTimerTask operation failed. Error: %v", err),
		}
	}
	return nil
}

// NewSQLMatchingPersistence creates an instance of ExecutionManager
func NewSQLMatchingPersistence(cfg config.SQL, logger bark.Logger) (p.ExecutionStore, error) {
	var _, err = newConnection(cfg)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func getCurrentExecutionIfExists(tx *sqlx.Tx, shardID int64, domainID string, workflowID string) (*currentExecutionRow, error) {
	var row currentExecutionRow
	if err := tx.Get(&row, getCurrentExecutionSQLQuery, shardID, domainID, workflowID); err != nil {
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("Failed to get current_executions row for (shard,domain,workflow) = (%v, %v, %v). Error: %v", shardID, domainID, workflowID, err),
		}
	}
	return &row, nil
}

func createExecution(tx *sqlx.Tx, request *p.CreateWorkflowExecutionRequest, shardID int, nowTimestamp time.Time) error {
	args := &executionRow{
		ShardID:                      int64(shardID),
		DomainID:                     request.DomainID,
		WorkflowID:                   *request.Execution.WorkflowId,
		RunID:                        *request.Execution.RunId,
		TaskList:                     request.TaskList,
		WorkflowTypeName:             request.WorkflowTypeName,
		WorkflowTimeoutSeconds:       int64(request.WorkflowTimeout),
		DecisionTaskTimeoutMinutes:   int64(request.DecisionTimeoutValue),
		State:                        p.WorkflowStateCreated,
		CloseStatus:                  p.WorkflowCloseStatusNone,
		LastFirstEventID:             common.FirstEventID,
		NextEventID:                  request.NextEventID,
		LastProcessedEvent:           request.LastProcessedEvent,
		StartTime:                    nowTimestamp,
		LastUpdatedTime:              nowTimestamp,
		CreateRequestID:              request.RequestID,
		DecisionVersion:              int64(request.DecisionVersion),
		DecisionScheduleID:           int64(request.DecisionScheduleID),
		DecisionStartedID:            int64(request.DecisionStartedID),
		DecisionTimeout:              int64(request.DecisionStartToCloseTimeout),
		DecisionAttempt:              0,
		DecisionTimestamp:            0,
		StickyTaskList:               "",
		StickyScheduleToStartTimeout: 0,
		ClientLibraryVersion:         "",
		ClientFeatureVersion:         "",
		ClientImpl:                   "",
	}

	if request.ReplicationState != nil {
		args.StartVersion = &request.ReplicationState.StartVersion
		args.CurrentVersion = &request.ReplicationState.CurrentVersion
		args.LastWriteVersion = &request.ReplicationState.LastWriteVersion
		args.LastWriteEventID = &request.ReplicationState.LastWriteEventID

		lastReplicationInfo, err := gobSerialize(&request.ReplicationState.LastReplicationInfo)
		if err != nil {
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("CreateWorkflowExecution operation failed. Failed to serialize LastReplicationInfo. Error: %v", err),
			}
		}
		args.LastReplicationInfo = &lastReplicationInfo
	}

	if request.ParentExecution != nil {
		args.InitiatedID = &request.InitiatedID
		args.ParentDomainID = &request.ParentDomainID
		args.ParentWorkflowID = request.ParentExecution.WorkflowId
		args.ParentRunID = request.ParentExecution.RunId
	}

	_, err := tx.NamedExec(createExecutionSQLQuery, args)

	if err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("CreateWorkflowExecution operation failed. Failed to insert into executions table. Error: %v", err),
		}
	}
	return nil
}

func createCurrentExecution(tx *sqlx.Tx, request *p.CreateWorkflowExecutionRequest, shardID int) error {
	arg := currentExecutionRow{
		ShardID:         int64(shardID),
		DomainID:        request.DomainID,
		WorkflowID:      *request.Execution.WorkflowId,
		RunID:           *request.Execution.RunId,
		CreateRequestID: request.RequestID,
		State:           p.WorkflowStateRunning,
		CloseStatus:     p.WorkflowCloseStatusNone,
	}
	if request.ReplicationState != nil {
		arg.StartVersion = request.ReplicationState.StartVersion
		arg.LastWriteVersion = request.ReplicationState.LastWriteVersion
	} else {
		arg.StartVersion = common.EmptyVersion
		arg.LastWriteVersion = common.EmptyVersion
	}
	if request.ParentExecution != nil {
		arg.State = p.WorkflowStateCreated
	}

	switch request.CreateWorkflowMode {
	case p.CreateWorkflowModeContinueAsNew:
		if err := continueAsNew(tx,
			shardID,
			request.DomainID,
			*request.Execution.WorkflowId,
			*request.Execution.RunId,
			request.PreviousRunID,
			request.RequestID,
			p.WorkflowStateRunning,
			p.WorkflowCloseStatusNone,
			arg.StartVersion,
			arg.LastWriteVersion); err != nil {
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("CreateWorkflowExecution operation failed. Failed to continue as new. Error: %v", err),
			}
		}
	case p.CreateWorkflowModeWorkflowIDReuse:
		if err := workflowIDReuse(tx,
			shardID,
			request.DomainID,
			*request.Execution.WorkflowId,
			*request.Execution.RunId,
			request.PreviousRunID,
			request.PreviousLastWriteVersion,
			p.WorkflowCloseStatusCompleted,
			request.RequestID,
			p.WorkflowStateRunning,
			p.WorkflowCloseStatusNone,
			arg.StartVersion,
			arg.LastWriteVersion); err != nil {
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("CreateWorkflowExecution operation failed. Failed to reuse workflow ID. Error: %v", err),
			}
		}
	case p.CreateWorkflowModeBrandNew:
		if _, err := tx.NamedExec(createCurrentExecutionSQLQuery, &arg); err != nil {
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("CreateWorkflowExecution operation failed. Failed to insert into current_executions table. Error: %v", err),
			}
		}
	default:
		return fmt.Errorf("Unknown workflow creation mode: %v", request.CreateWorkflowMode)
	}

	return nil
}

func lockAndCheckNextEventID(tx *sqlx.Tx, shardID int, domainID, workflowID, runID string, condition int64) error {
	nextEventID, err := lockNextEventID(tx, shardID, domainID, workflowID, runID)
	if err != nil {
		return err
	}
	if *nextEventID != condition {
		return &p.ConditionFailedError{
			Msg: fmt.Sprintf("next_event_id was %v when it should have been %v.", nextEventID, condition),
		}
	}
	return nil
}

func lockNextEventID(tx *sqlx.Tx, shardID int, domainID, workflowID, runID string) (*int64, error) {
	var nextEventID int64
	if err := tx.Get(&nextEventID, lockAndCheckNextEventIDSQLQuery, shardID, domainID, workflowID, runID); err != nil {
		if err == sql.ErrNoRows {
			return nil, &workflow.EntityNotExistsError{
				Message: fmt.Sprintf("Failed to lock executions row with (shard, domain, workflow, run) = (%v,%v,%v,%v) which does not exist.", shardID, domainID, workflowID, runID),
			}
		}
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("Failed to lock executions row. Error: %v", err),
		}
	}
	return &nextEventID, nil
}

func createTransferTasks(tx *sqlx.Tx, transferTasks []p.Task, shardID int, domainID, workflowID, runID string) error {
	if len(transferTasks) == 0 {
		return nil
	}
	transferTasksRows := make([]transferTasksRow, len(transferTasks))

	for i, task := range transferTasks {
		transferTasksRows[i].ShardID = shardID
		transferTasksRows[i].DomainID = domainID
		transferTasksRows[i].WorkflowID = workflowID
		transferTasksRows[i].RunID = runID
		transferTasksRows[i].TargetDomainID = domainID
		transferTasksRows[i].TargetWorkflowID = p.TransferTaskTransferTargetWorkflowID
		transferTasksRows[i].TargetChildWorkflowOnly = false
		transferTasksRows[i].TaskList = ""
		transferTasksRows[i].ScheduleID = 0

		switch task.GetType() {
		case p.TransferTaskTypeActivityTask:
			transferTasksRows[i].TargetDomainID = task.(*p.ActivityTask).DomainID
			transferTasksRows[i].TaskList = task.(*p.ActivityTask).TaskList
			transferTasksRows[i].ScheduleID = task.(*p.ActivityTask).ScheduleID

		case p.TransferTaskTypeDecisionTask:
			transferTasksRows[i].TargetDomainID = task.(*p.DecisionTask).DomainID
			transferTasksRows[i].TaskList = task.(*p.DecisionTask).TaskList
			transferTasksRows[i].ScheduleID = task.(*p.DecisionTask).ScheduleID

		case p.TransferTaskTypeCancelExecution:
			transferTasksRows[i].TargetDomainID = task.(*p.CancelExecutionTask).TargetDomainID
			transferTasksRows[i].TargetWorkflowID = task.(*p.CancelExecutionTask).TargetWorkflowID
			if task.(*p.CancelExecutionTask).TargetRunID != "" {
				transferTasksRows[i].TargetRunID = task.(*p.CancelExecutionTask).TargetRunID
			}
			transferTasksRows[i].TargetChildWorkflowOnly = task.(*p.CancelExecutionTask).TargetChildWorkflowOnly
			transferTasksRows[i].ScheduleID = task.(*p.CancelExecutionTask).InitiatedID

		case p.TransferTaskTypeSignalExecution:
			transferTasksRows[i].TargetDomainID = task.(*p.SignalExecutionTask).TargetDomainID
			transferTasksRows[i].TargetWorkflowID = task.(*p.SignalExecutionTask).TargetWorkflowID
			if task.(*p.SignalExecutionTask).TargetRunID != "" {
				transferTasksRows[i].TargetRunID = task.(*p.SignalExecutionTask).TargetRunID
			}
			transferTasksRows[i].TargetChildWorkflowOnly = task.(*p.SignalExecutionTask).TargetChildWorkflowOnly
			transferTasksRows[i].ScheduleID = task.(*p.SignalExecutionTask).InitiatedID

		case p.TransferTaskTypeStartChildExecution:
			transferTasksRows[i].TargetDomainID = task.(*p.StartChildExecutionTask).TargetDomainID
			transferTasksRows[i].TargetWorkflowID = task.(*p.StartChildExecutionTask).TargetWorkflowID
			transferTasksRows[i].ScheduleID = task.(*p.StartChildExecutionTask).InitiatedID

		case p.TransferTaskTypeCloseExecution:
			// No explicit property needs to be set

		default:
			// hmm what should i do here?
			//d.logger.Fatal("Unknown Transfer Task.")
		}

		transferTasksRows[i].TaskID = task.GetTaskID()
		transferTasksRows[i].TaskType = task.GetType()
		transferTasksRows[i].Version = task.GetVersion()
	}

	query, args, err := tx.BindNamed(createTransferTasksSQLQuery, transferTasksRows)
	if err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("Failed to create transfer tasks. Failed to bind query. Error: %v", err),
		}
	}

	result, err := tx.Exec(query, args...)
	if err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("Failed to create transfer tasks. Error: %v", err),
		}
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("Failed to create transfer tasks. Could not verify number of rows inserted. Error: %v", err),
		}
	}

	if int(rowsAffected) != len(transferTasks) {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("Failed to create transfer tasks. Inserted %v instead of %v rows into transfer_tasks. Error: %v", rowsAffected, len(transferTasks), err),
		}
	}

	return nil
}

func createReplicationTasks(tx *sqlx.Tx, replicationTasks []p.Task, shardID int, domainID, workflowID, runID string) error {
	if len(replicationTasks) == 0 {
		return nil
	}
	replicationTasksRows := make([]replicationTasksRow, len(replicationTasks))

	for i, task := range replicationTasks {
		replicationTasksRows[i].DomainID = domainID
		replicationTasksRows[i].WorkflowID = workflowID
		replicationTasksRows[i].RunID = runID
		replicationTasksRows[i].ShardID = shardID
		replicationTasksRows[i].TaskType = task.GetType()
		replicationTasksRows[i].TaskID = task.GetTaskID()

		firstEventID := common.EmptyEventID
		nextEventID := common.EmptyEventID
		version := int64(0)
		var lastReplicationInfo []byte
		var err error

		switch task.GetType() {
		case p.ReplicationTaskTypeHistory:
			historyReplicationTask, ok := task.(*p.HistoryReplicationTask)
			if !ok {
				return &workflow.InternalServiceError{
					Message: fmt.Sprintf("Failed to cast %v to HistoryReplicationTask", task),
				}
			}

			firstEventID = historyReplicationTask.FirstEventID
			nextEventID = historyReplicationTask.NextEventID
			version = task.GetVersion()
			lastReplicationInfo, err = gobSerialize(historyReplicationTask.LastReplicationInfo)
			if err != nil {
				return &workflow.InternalServiceError{
					Message: fmt.Sprintf("Failed to serialize LastReplicationInfo. Task: %v", task),
				}
			}

		default:
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("Unknown replication task: %v", task),
			}
		}

		replicationTasksRows[i].FirstEventID = firstEventID
		replicationTasksRows[i].NextEventID = nextEventID
		replicationTasksRows[i].Version = version
		replicationTasksRows[i].LastReplicationInfo = lastReplicationInfo
	}

	query, args, err := tx.BindNamed(createReplicationTasksSQLQuery, replicationTasksRows)
	if err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("Failed to create replication tasks. Failed to bind query. Error: %v", err),
		}
	}

	result, err := tx.Exec(query, args...)
	if err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("Failed to create replication tasks. Error: %v", err),
		}
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("Failed to create replication tasks. Could not verify number of rows inserted. Error: %v", err),
		}
	}

	if int(rowsAffected) != len(replicationTasks) {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("Failed to create replication tasks. Inserted %v instead of %v rows into transfer_tasks. Error: %v", rowsAffected, len(replicationTasks), err),
		}
	}

	return nil
}

func createTimerTasks(tx *sqlx.Tx, timerTasks []p.Task, deleteTimerTask p.Task, shardID int, domainID, workflowID, runID string) error {
	if len(timerTasks) > 0 {
		timerTasksRows := make([]timerTasksRow, len(timerTasks))

		for i, task := range timerTasks {
			switch t := task.(type) {
			case *p.DecisionTimeoutTask:
				timerTasksRows[i].EventID = t.EventID
				timerTasksRows[i].TimeoutType = t.TimeoutType
				timerTasksRows[i].ScheduleAttempt = t.ScheduleAttempt
			case *p.ActivityTimeoutTask:
				timerTasksRows[i].EventID = t.EventID
				timerTasksRows[i].TimeoutType = t.TimeoutType
				timerTasksRows[i].ScheduleAttempt = t.Attempt
			case *p.UserTimerTask:
				timerTasksRows[i].EventID = t.EventID
			case *p.ActivityRetryTimerTask:
				timerTasksRows[i].EventID = t.EventID
				timerTasksRows[i].ScheduleAttempt = int64(t.Attempt)
			case *p.WorkflowRetryTimerTask:
				timerTasksRows[i].EventID = t.EventID
			}

			timerTasksRows[i].ShardID = shardID
			timerTasksRows[i].DomainID = domainID
			timerTasksRows[i].WorkflowID = workflowID
			timerTasksRows[i].RunID = runID
			t, err := p.GetVisibilityTSFrom(task)
			if err != nil {
				return &workflow.InternalServiceError{
					Message: fmt.Sprintf("Failed to create timer tasks. Error: %v", err),
				}
			}
			timerTasksRows[i].VisibilityTimestamp = t
			timerTasksRows[i].TaskID = task.GetTaskID()
			timerTasksRows[i].Version = task.GetVersion()
			timerTasksRows[i].TaskType = task.GetType()
		}

		query, args, err := tx.BindNamed(createTimerTasksSQLQuery, timerTasksRows)
		if err != nil {
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("Failed to create timer tasks. Failed to bind query. Error: %v", err),
			}
		}
		result, err := tx.Exec(query, args...)
		if err != nil {
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("Failed to create timer tasks. Error: %v", err),
			}
		}
		rowsAffected, err := result.RowsAffected()
		if err != nil {
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("Failed to create timer tasks. Could not verify number of rows inserted. Error: %v", err),
			}
		}

		if int(rowsAffected) != len(timerTasks) {
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("Failed to create timer tasks. Inserted %v instead of %v rows into timer_tasks. Error: %v", rowsAffected, len(timerTasks), err),
			}
		}
	}

	if deleteTimerTask != nil {
		ts, err := p.GetVisibilityTSFrom(deleteTimerTask)
		if err != nil {
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("Failed to delete timer task. Task: %v. Error: %v", deleteTimerTask, err),
			}
		}

		if _, err := tx.Exec(completeTimerTaskSQLQuery, shardID, ts, deleteTimerTask.GetTaskID()); err != nil {
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("Failed to delete timer task. Task: %v. Error: %v", deleteTimerTask, err),
			}
		}
	}

	return nil
}

func continueAsNew(tx *sqlx.Tx, shardID int, domainID, workflowID, runID, previousRunID string,
	createRequestID string, state int64, closeStatus int64, startVersion int64, lastWriteVersion int64) error {

	var currentRunID string
	if err := tx.Get(&currentRunID, continueAsNewLockRunIDSQLQuery, int64(shardID), domainID, workflowID); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("ContinueAsNew failed. Failed to check current run ID. Error: %v", err),
		}
	}
	if currentRunID != previousRunID {
		return &p.ConditionFailedError{
			Msg: fmt.Sprintf("ContinueAsNew failed. Current run ID was %v, expected %v", currentRunID, previousRunID),
		}
	}
	return conditionalUpdateCurrentExecution(tx, shardID, domainID, workflowID, runID, createRequestID, state, closeStatus, startVersion, lastWriteVersion)
}

func workflowIDReuse(tx *sqlx.Tx, shardID int, domainID, workflowID, runID, previousRunID string,
	previousLastWriteVersion int64, previousState int64,
	createRequestID string, state int64, closeStatus int64, startVersion int64, lastWriteVersion int64) error {

	var currentExecutionRow currentExecutionRow
	if err := tx.Get(&currentExecutionRow, workflowIDReuseSQLQuery, int64(shardID), domainID, workflowID); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("WorkfowIDReuse failed. Failed to check current run ID or current state or current last write version. Error: %v", err),
		}
	}
	if currentExecutionRow.RunID != previousRunID ||
		currentExecutionRow.State != previousState ||
		currentExecutionRow.LastWriteVersion != previousLastWriteVersion {
		return &p.ConditionFailedError{
			Msg: fmt.Sprintf("ContinueAsNew failed. Current run ID %v, expected %v, current state: %v, expected: %v, current last write version: %v, expected: %v",
				currentExecutionRow.RunID, previousRunID,
				currentExecutionRow.State, previousState,
				currentExecutionRow.LastWriteVersion, previousLastWriteVersion,
			),
		}
	}
	return conditionalUpdateCurrentExecution(tx, shardID, domainID, workflowID, runID, createRequestID, state, closeStatus, startVersion, lastWriteVersion)
}

func conditionalUpdateCurrentExecution(tx *sqlx.Tx, shardID int, domainID, workflowID, runID,
	createRequestID string, state int64, closeStatus int64, startVersion int64, lastWriteVersion int64) error {

	result, err := tx.NamedExec(continueAsNewUpdateCurrentExecutionsSQLQuery, &currentExecutionRow{
		ShardID:          int64(shardID),
		DomainID:         domainID,
		WorkflowID:       workflowID,
		RunID:            runID,
		CreateRequestID:  createRequestID,
		State:            state,
		CloseStatus:      closeStatus,
		StartVersion:     startVersion,
		LastWriteVersion: lastWriteVersion,
	})
	// The current_executions row is locked, and the run ID has been verified. We can do the updates.
	if err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("ContinueAsNew failed. Failed to update current_executions table. Error: %v", err),
		}
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("ContinueAsNew failed. Failed to check number of rows updated in current_executions table. Error: %v", err),
		}
	}
	if rowsAffected != 1 {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("ContinueAsNew failed. %v rows of current_executions updated instead of 1.", rowsAffected),
		}
	}
	return nil
}

func updateExecution(tx *sqlx.Tx,
	executionInfo *p.WorkflowExecutionInfo,
	replicationState *p.ReplicationState,
	condition int64) error {
	args := updateExecutionRow{
		executionRow{
			DomainID:                     executionInfo.DomainID,
			WorkflowID:                   executionInfo.WorkflowID,
			RunID:                        executionInfo.RunID,
			ParentDomainID:               &executionInfo.ParentDomainID,
			ParentWorkflowID:             &executionInfo.ParentWorkflowID,
			ParentRunID:                  &executionInfo.ParentRunID,
			InitiatedID:                  &executionInfo.InitiatedID,
			CompletionEvent:              nil,
			TaskList:                     executionInfo.TaskList,
			WorkflowTypeName:             executionInfo.WorkflowTypeName,
			WorkflowTimeoutSeconds:       int64(executionInfo.WorkflowTimeout),
			DecisionTaskTimeoutMinutes:   int64(executionInfo.DecisionTimeoutValue),
			State:                        int64(executionInfo.State),
			CloseStatus:                  int64(executionInfo.CloseStatus),
			LastFirstEventID:             int64(executionInfo.LastFirstEventID),
			NextEventID:                  int64(executionInfo.NextEventID),
			LastProcessedEvent:           int64(executionInfo.LastProcessedEvent),
			StartTime:                    executionInfo.StartTimestamp,
			LastUpdatedTime:              executionInfo.LastUpdatedTimestamp,
			CreateRequestID:              executionInfo.CreateRequestID,
			DecisionVersion:              executionInfo.DecisionVersion,
			DecisionScheduleID:           executionInfo.DecisionScheduleID,
			DecisionStartedID:            executionInfo.DecisionStartedID,
			DecisionRequestID:            executionInfo.DecisionRequestID,
			DecisionTimeout:              int64(executionInfo.DecisionTimeout),
			DecisionAttempt:              executionInfo.DecisionAttempt,
			DecisionTimestamp:            executionInfo.DecisionTimestamp,
			StickyTaskList:               executionInfo.StickyTaskList,
			StickyScheduleToStartTimeout: int64(executionInfo.StickyScheduleToStartTimeout),
			ClientLibraryVersion:         executionInfo.ClientLibraryVersion,
			ClientFeatureVersion:         executionInfo.ClientFeatureVersion,
			ClientImpl:                   executionInfo.ClientImpl,
		},
		condition,
	}

	if executionInfo.ExecutionContext != nil {
		args.executionRow.ExecutionContext = &executionInfo.ExecutionContext
	}

	if replicationState != nil {
		args.StartVersion = &replicationState.StartVersion
		args.CurrentVersion = &replicationState.CurrentVersion
		args.LastWriteVersion = &replicationState.LastWriteVersion
		args.LastWriteEventID = &replicationState.LastWriteEventID
		lastReplicationInfo, err := gobSerialize(&replicationState.LastReplicationInfo)
		if err != nil {
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("CreateWorkflowExecution operation failed. Failed to serialize LastReplicationInfo. Error: %v", err),
			}
		}
		args.LastReplicationInfo = &lastReplicationInfo
	}

	if executionInfo.ParentDomainID != "" {
		args.ParentDomainID = &executionInfo.ParentDomainID
		args.ParentWorkflowID = &executionInfo.ParentWorkflowID
		args.ParentRunID = &executionInfo.ParentRunID
		args.InitiatedID = &executionInfo.InitiatedID
		args.CompletionEvent = nil
	}

	if executionInfo.CancelRequested {
		var i int64 = 1
		args.CancelRequested = &i
		args.CancelRequestID = &executionInfo.CancelRequestID
	}

	result, err := tx.NamedExec(updateExecutionSQLQuery, &args)
	if err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("Failed to update executions row. Erorr: %v", err),
		}
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("Failed to update executions row. Failed to verify number of rows affected. Erorr: %v", err),
		}
	}
	if rowsAffected != 1 {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("Failed to update executions row. Affected %v rows updated instead of 1.", rowsAffected),
		}
	}

	return nil
}
