// Copyright (c) 2017 Uber Technologies, Inc.
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

package sqlplugin

import (
	"database/sql"
	"time"

	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/service/config"
)

type (
	// Plugin defines the interface for any SQL database that needs to implement
	Plugin interface {
		CreateDB(cfg *config.SQL) (DB, error)
		CreateAdminDB(cfg *config.SQL) (AdminDB, error)
	}

	// DomainRow represents a row in domain table
	DomainRow struct {
		ID           UUID
		Name         string
		Data         []byte
		DataEncoding string
		IsGlobal     bool
	}

	// DomainFilter contains the column names within domain table that
	// can be used to filter results through a WHERE clause. When ID is not
	// nil, it will be used for WHERE condition. If ID is nil and Name is non-nil,
	// Name will be used for WHERE condition. When both ID and Name are nil,
	// no WHERE clause will be used
	DomainFilter struct {
		ID            *UUID
		Name          *string
		GreaterThanID *UUID
		PageSize      *int
	}

	// DomainMetadataRow represents a row in domain_metadata table
	DomainMetadataRow struct {
		NotificationVersion int64
	}

	// ShardsRow represents a row in shards table
	ShardsRow struct {
		ShardID      int64
		RangeID      int64
		Data         []byte
		DataEncoding string
	}

	// ShardsFilter contains the column names within shards table that
	// can be used to filter results through a WHERE clause
	ShardsFilter struct {
		ShardID int64
	}

	// TransferTasksRow represents a row in transfer_tasks table
	TransferTasksRow struct {
		ShardID      int
		TaskID       int64
		Data         []byte
		DataEncoding string
	}

	// TransferTasksFilter contains the column names within transfer_tasks table that
	// can be used to filter results through a WHERE clause
	TransferTasksFilter struct {
		ShardID   int
		TaskID    *int64
		MinTaskID *int64
		MaxTaskID *int64
	}

	// ExecutionsRow represents a row in executions table
	ExecutionsRow struct {
		ShardID                  int
		DomainID                 UUID
		WorkflowID               string
		RunID                    UUID
		NextEventID              int64
		LastWriteVersion         int64
		Data                     []byte
		DataEncoding             string
		VersionHistories         []byte
		VersionHistoriesEncoding string
	}

	// ExecutionsFilter contains the column names within executions table that
	// can be used to filter results through a WHERE clause
	ExecutionsFilter struct {
		ShardID    int
		DomainID   UUID
		WorkflowID string
		RunID      UUID
	}

	// CurrentExecutionsRow represents a row in current_executions table
	CurrentExecutionsRow struct {
		ShardID          int64
		DomainID         UUID
		WorkflowID       string
		RunID            UUID
		CreateRequestID  string
		State            int
		CloseStatus      int
		LastWriteVersion int64
		StartVersion     int64
	}

	// CurrentExecutionsFilter contains the column names within current_executions table that
	// can be used to filter results through a WHERE clause
	CurrentExecutionsFilter struct {
		ShardID    int64
		DomainID   UUID
		WorkflowID string
		RunID      UUID
	}

	// BufferedEventsRow represents a row in buffered_events table
	BufferedEventsRow struct {
		ShardID      int
		DomainID     UUID
		WorkflowID   string
		RunID        UUID
		Data         []byte
		DataEncoding string
	}

	// BufferedEventsFilter contains the column names within buffered_events table that
	// can be used to filter results through a WHERE clause
	BufferedEventsFilter struct {
		ShardID    int
		DomainID   UUID
		WorkflowID string
		RunID      UUID
	}

	// TasksRow represents a row in tasks table
	TasksRow struct {
		DomainID     UUID
		TaskType     int64
		TaskID       int64
		TaskListName string
		Data         []byte
		DataEncoding string
	}

	// TasksFilter contains the column names within tasks table that
	// can be used to filter results through a WHERE clause
	TasksFilter struct {
		DomainID             UUID
		TaskListName         string
		TaskType             int64
		TaskID               *int64
		MinTaskID            *int64
		MaxTaskID            *int64
		TaskIDLessThanEquals *int64
		Limit                *int
		PageSize             *int
	}

	// TaskListsRow represents a row in task_lists table
	TaskListsRow struct {
		ShardID      int
		DomainID     UUID
		Name         string
		TaskType     int64
		RangeID      int64
		Data         []byte
		DataEncoding string
	}

	// TaskListsFilter contains the column names within task_lists table that
	// can be used to filter results through a WHERE clause
	TaskListsFilter struct {
		ShardID             int
		DomainID            *UUID
		Name                *string
		TaskType            *int64
		DomainIDGreaterThan *UUID
		NameGreaterThan     *string
		TaskTypeGreaterThan *int64
		RangeID             *int64
		PageSize            *int
	}

	// ReplicationTasksRow represents a row in replication_tasks table
	ReplicationTasksRow struct {
		ShardID      int
		TaskID       int64
		Data         []byte
		DataEncoding string
	}

	// ReplicationTaskDLQRow represents a row in replication_tasks_dlq table
	ReplicationTaskDLQRow struct {
		SourceClusterName string
		ShardID           int
		TaskID            int64
		Data              []byte
		DataEncoding      string
	}

	// ReplicationTasksFilter contains the column names within replication_tasks table that
	// can be used to filter results through a WHERE clause
	ReplicationTasksFilter struct {
		ShardID            int
		TaskID             int64
		InclusiveEndTaskID int64
		MinTaskID          int64
		MaxTaskID          int64
		PageSize           int
	}

	// ReplicationTasksDLQFilter contains the column names within replication_tasks_dlq table that
	// can be used to filter results through a WHERE clause
	ReplicationTasksDLQFilter struct {
		ReplicationTasksFilter
		SourceClusterName string
	}

	// TimerTasksRow represents a row in timer_tasks table
	TimerTasksRow struct {
		ShardID             int
		VisibilityTimestamp time.Time
		TaskID              int64
		Data                []byte
		DataEncoding        string
	}

	// TimerTasksFilter contains the column names within timer_tasks table that
	// can be used to filter results through a WHERE clause
	TimerTasksFilter struct {
		ShardID                int
		TaskID                 int64
		VisibilityTimestamp    *time.Time
		MinVisibilityTimestamp *time.Time
		MaxVisibilityTimestamp *time.Time
		PageSize               *int
	}

	// EventsRow represents a row in events table
	EventsRow struct {
		DomainID     UUID
		WorkflowID   string
		RunID        UUID
		FirstEventID int64
		BatchVersion int64
		RangeID      int64
		TxID         int64
		Data         []byte
		DataEncoding string
	}

	// EventsFilter contains the column names within events table that
	// can be used to filter results through a WHERE clause
	EventsFilter struct {
		DomainID     UUID
		WorkflowID   string
		RunID        UUID
		FirstEventID *int64
		NextEventID  *int64
		PageSize     *int
	}

	// HistoryNodeRow represents a row in history_node table
	HistoryNodeRow struct {
		ShardID  int
		TreeID   UUID
		BranchID UUID
		NodeID   int64
		// use pointer so that it's easier to multiple by -1
		TxnID        *int64
		Data         []byte
		DataEncoding string
	}

	// HistoryNodeFilter contains the column names within history_node table that
	// can be used to filter results through a WHERE clause
	HistoryNodeFilter struct {
		ShardID  int
		TreeID   UUID
		BranchID UUID
		// Inclusive
		MinNodeID *int64
		// Exclusive
		MaxNodeID *int64
		PageSize  *int
	}

	// HistoryTreeRow represents a row in history_tree table
	HistoryTreeRow struct {
		ShardID      int
		TreeID       UUID
		BranchID     UUID
		Data         []byte
		DataEncoding string
	}

	// HistoryTreeFilter contains the column names within history_tree table that
	// can be used to filter results through a WHERE clause
	HistoryTreeFilter struct {
		ShardID  int
		TreeID   UUID
		BranchID *UUID
	}

	// ActivityInfoMapsRow represents a row in activity_info_maps table
	ActivityInfoMapsRow struct {
		ShardID                  int64
		DomainID                 UUID
		WorkflowID               string
		RunID                    UUID
		ScheduleID               int64
		Data                     []byte
		DataEncoding             string
		LastHeartbeatDetails     []byte
		LastHeartbeatUpdatedTime time.Time
	}

	// ActivityInfoMapsFilter contains the column names within activity_info_maps table that
	// can be used to filter results through a WHERE clause
	ActivityInfoMapsFilter struct {
		ShardID    int64
		DomainID   UUID
		WorkflowID string
		RunID      UUID
		ScheduleID *int64
	}

	// TimerInfoMapsRow represents a row in timer_info_maps table
	TimerInfoMapsRow struct {
		ShardID      int64
		DomainID     UUID
		WorkflowID   string
		RunID        UUID
		TimerID      string
		Data         []byte
		DataEncoding string
	}

	// TimerInfoMapsFilter contains the column names within timer_info_maps table that
	// can be used to filter results through a WHERE clause
	TimerInfoMapsFilter struct {
		ShardID    int64
		DomainID   UUID
		WorkflowID string
		RunID      UUID
		TimerID    *string
	}

	// ChildExecutionInfoMapsRow represents a row in child_execution_info_maps table
	ChildExecutionInfoMapsRow struct {
		ShardID      int64
		DomainID     UUID
		WorkflowID   string
		RunID        UUID
		InitiatedID  int64
		Data         []byte
		DataEncoding string
	}

	// ChildExecutionInfoMapsFilter contains the column names within child_execution_info_maps table that
	// can be used to filter results through a WHERE clause
	ChildExecutionInfoMapsFilter struct {
		ShardID     int64
		DomainID    UUID
		WorkflowID  string
		RunID       UUID
		InitiatedID *int64
	}

	// RequestCancelInfoMapsRow represents a row in request_cancel_info_maps table
	RequestCancelInfoMapsRow struct {
		ShardID      int64
		DomainID     UUID
		WorkflowID   string
		RunID        UUID
		InitiatedID  int64
		Data         []byte
		DataEncoding string
	}

	// RequestCancelInfoMapsFilter contains the column names within request_cancel_info_maps table that
	// can be used to filter results through a WHERE clause
	RequestCancelInfoMapsFilter struct {
		ShardID     int64
		DomainID    UUID
		WorkflowID  string
		RunID       UUID
		InitiatedID *int64
	}

	// SignalInfoMapsRow represents a row in signal_info_maps table
	SignalInfoMapsRow struct {
		ShardID      int64
		DomainID     UUID
		WorkflowID   string
		RunID        UUID
		InitiatedID  int64
		Data         []byte
		DataEncoding string
	}

	// SignalInfoMapsFilter contains the column names within signal_info_maps table that
	// can be used to filter results through a WHERE clause
	SignalInfoMapsFilter struct {
		ShardID     int64
		DomainID    UUID
		WorkflowID  string
		RunID       UUID
		InitiatedID *int64
	}

	// SignalsRequestedSetsRow represents a row in signals_requested_sets table
	SignalsRequestedSetsRow struct {
		ShardID    int64
		DomainID   UUID
		WorkflowID string
		RunID      UUID
		SignalID   string
	}

	// SignalsRequestedSetsFilter contains the column names within signals_requested_sets table that
	// can be used to filter results through a WHERE clause
	SignalsRequestedSetsFilter struct {
		ShardID    int64
		DomainID   UUID
		WorkflowID string
		RunID      UUID
		SignalID   *string
	}

	// VisibilityRow represents a row in executions_visibility table
	VisibilityRow struct {
		DomainID         string
		RunID            string
		WorkflowTypeName string
		WorkflowID       string
		StartTime        time.Time
		ExecutionTime    time.Time
		CloseStatus      *int32
		CloseTime        *time.Time
		HistoryLength    *int64
		Memo             []byte
		Encoding         string
	}

	// VisibilityFilter contains the column names within executions_visibility table that
	// can be used to filter results through a WHERE clause
	VisibilityFilter struct {
		DomainID         string
		Closed           bool
		RunID            *string
		WorkflowID       *string
		WorkflowTypeName *string
		CloseStatus      *int32
		MinStartTime     *time.Time
		MaxStartTime     *time.Time
		PageSize         *int
	}

	// QueueRow represents a row in queue table
	QueueRow struct {
		QueueType      persistence.QueueType
		MessageID      int
		MessagePayload []byte
	}

	// QueueMetadataRow represents a row in queue_metadata table
	QueueMetadataRow struct {
		QueueType persistence.QueueType
		Data      []byte
	}

	// tableCRUD defines the API for interacting with the database tables
	tableCRUD interface {
		InsertIntoDomain(rows *DomainRow) (sql.Result, error)
		UpdateDomain(row *DomainRow) (sql.Result, error)
		// SelectFromDomain returns domains that match filter criteria. Either ID or
		// Name can be specified to filter results. If both are not specified, all rows
		// will be returned
		SelectFromDomain(filter *DomainFilter) ([]DomainRow, error)
		// DeleteDomain deletes a single row. One of ID or Name MUST be specified
		DeleteFromDomain(filter *DomainFilter) (sql.Result, error)

		LockDomainMetadata() error
		UpdateDomainMetadata(row *DomainMetadataRow) (sql.Result, error)
		SelectFromDomainMetadata() (*DomainMetadataRow, error)

		InsertIntoShards(rows *ShardsRow) (sql.Result, error)
		UpdateShards(row *ShardsRow) (sql.Result, error)
		SelectFromShards(filter *ShardsFilter) (*ShardsRow, error)
		ReadLockShards(filter *ShardsFilter) (int, error)
		WriteLockShards(filter *ShardsFilter) (int, error)

		InsertIntoTasks(rows []TasksRow) (sql.Result, error)
		// SelectFromTasks retrieves one or more rows from the tasks table
		// Required filter params - {domainID, tasklistName, taskType, minTaskID, maxTaskID, pageSize}
		SelectFromTasks(filter *TasksFilter) ([]TasksRow, error)
		// DeleteFromTasks deletes a row from tasks table
		// Required filter params:
		//  to delete single row
		//     - {domainID, tasklistName, taskType, taskID}
		//  to delete multiple rows
		//    - {domainID, tasklistName, taskType, taskIDLessThanEquals, limit }
		//    - this will delete upto limit number of tasks less than or equal to the given task id
		DeleteFromTasks(filter *TasksFilter) (sql.Result, error)

		InsertIntoTaskLists(row *TaskListsRow) (sql.Result, error)
		ReplaceIntoTaskLists(row *TaskListsRow) (sql.Result, error)
		UpdateTaskLists(row *TaskListsRow) (sql.Result, error)
		// SelectFromTaskLists returns one or more rows from task_lists table
		// Required Filter params:
		//  to read a single row: {shardID, domainID, name, taskType}
		//  to range read multiple rows: {shardID, domainIDGreaterThan, nameGreaterThan, taskTypeGreaterThan, pageSize}
		SelectFromTaskLists(filter *TaskListsFilter) ([]TaskListsRow, error)
		DeleteFromTaskLists(filter *TaskListsFilter) (sql.Result, error)
		LockTaskLists(filter *TaskListsFilter) (int64, error)

		// eventsV2
		InsertIntoHistoryNode(row *HistoryNodeRow) (sql.Result, error)
		SelectFromHistoryNode(filter *HistoryNodeFilter) ([]HistoryNodeRow, error)
		DeleteFromHistoryNode(filter *HistoryNodeFilter) (sql.Result, error)
		InsertIntoHistoryTree(row *HistoryTreeRow) (sql.Result, error)
		SelectFromHistoryTree(filter *HistoryTreeFilter) ([]HistoryTreeRow, error)
		DeleteFromHistoryTree(filter *HistoryTreeFilter) (sql.Result, error)

		InsertIntoExecutions(row *ExecutionsRow) (sql.Result, error)
		UpdateExecutions(row *ExecutionsRow) (sql.Result, error)
		SelectFromExecutions(filter *ExecutionsFilter) (*ExecutionsRow, error)
		DeleteFromExecutions(filter *ExecutionsFilter) (sql.Result, error)
		ReadLockExecutions(filter *ExecutionsFilter) (int, error)
		WriteLockExecutions(filter *ExecutionsFilter) (int, error)

		LockCurrentExecutionsJoinExecutions(filter *CurrentExecutionsFilter) ([]CurrentExecutionsRow, error)

		InsertIntoCurrentExecutions(row *CurrentExecutionsRow) (sql.Result, error)
		UpdateCurrentExecutions(row *CurrentExecutionsRow) (sql.Result, error)
		// SelectFromCurrentExecutions returns one or more rows from current_executions table
		// Required params - {shardID, domainID, workflowID}
		SelectFromCurrentExecutions(filter *CurrentExecutionsFilter) (*CurrentExecutionsRow, error)
		// DeleteFromCurrentExecutions deletes a single row that matches the filter criteria
		// If a row exist, that row will be deleted and this method will return success
		// If there is no row matching the filter criteria, this method will still return success
		// Callers can check the output of Result.RowsAffected() to see if a row was deleted or not
		// Required params - {shardID, domainID, workflowID, runID}
		DeleteFromCurrentExecutions(filter *CurrentExecutionsFilter) (sql.Result, error)
		LockCurrentExecutions(filter *CurrentExecutionsFilter) (*CurrentExecutionsRow, error)

		InsertIntoTransferTasks(rows []TransferTasksRow) (sql.Result, error)
		// SelectFromTransferTasks returns rows that match filter criteria from transfer_tasks table.
		// Required filter params - {shardID, minTaskID, maxTaskID}
		SelectFromTransferTasks(filter *TransferTasksFilter) ([]TransferTasksRow, error)
		// DeleteFromTransferTasks deletes one or more rows from transfer_tasks table.
		// Filter params - shardID is required. If TaskID is not nil, a single row is deleted.
		// When MinTaskID and MaxTaskID are not-nil, a range of rows are deleted.
		DeleteFromTransferTasks(filter *TransferTasksFilter) (sql.Result, error)

		InsertIntoTimerTasks(rows []TimerTasksRow) (sql.Result, error)
		// SelectFromTimerTasks returns one or more rows from timer_tasks table
		// Required filter Params - {shardID, taskID, minVisibilityTimestamp, maxVisibilityTimestamp, pageSize}
		SelectFromTimerTasks(filter *TimerTasksFilter) ([]TimerTasksRow, error)
		// DeleteFromTimerTasks deletes one or more rows from timer_tasks table
		// Required filter Params:
		//  - to delete one row - {shardID, visibilityTimestamp, taskID}
		//  - to delete multiple rows - {shardID, minVisibilityTimestamp, maxVisibilityTimestamp}
		DeleteFromTimerTasks(filter *TimerTasksFilter) (sql.Result, error)

		InsertIntoBufferedEvents(rows []BufferedEventsRow) (sql.Result, error)
		SelectFromBufferedEvents(filter *BufferedEventsFilter) ([]BufferedEventsRow, error)
		DeleteFromBufferedEvents(filter *BufferedEventsFilter) (sql.Result, error)

		InsertIntoReplicationTasks(rows []ReplicationTasksRow) (sql.Result, error)
		// SelectFromReplicationTasks returns one or more rows from replication_tasks table
		// Required filter params - {shardID, minTaskID, maxTaskID, pageSize}
		SelectFromReplicationTasks(filter *ReplicationTasksFilter) ([]ReplicationTasksRow, error)
		// DeleteFromReplicationTasks deletes a row from replication_tasks table
		// Required filter params - {shardID, inclusiveEndTaskID}
		DeleteFromReplicationTasks(filter *ReplicationTasksFilter) (sql.Result, error)
		// DeleteFromReplicationTasks deletes multi rows from replication_tasks table
		// Required filter params - {shardID, inclusiveEndTaskID}
		RangeDeleteFromReplicationTasks(filter *ReplicationTasksFilter) (sql.Result, error)
		// InsertIntoReplicationTasksDLQ puts the replication task into DLQ
		InsertIntoReplicationTasksDLQ(row *ReplicationTaskDLQRow) (sql.Result, error)
		// SelectFromReplicationTasksDLQ returns one or more rows from replication_tasks_dlq table
		// Required filter params - {sourceClusterName, shardID, minTaskID, pageSize}
		SelectFromReplicationTasksDLQ(filter *ReplicationTasksDLQFilter) ([]ReplicationTasksRow, error)
		// DeleteMessageFromReplicationTasksDLQ deletes one row from replication_tasks_dlq table
		// Required filter params - {sourceClusterName, shardID, taskID}
		DeleteMessageFromReplicationTasksDLQ(filter *ReplicationTasksDLQFilter) (sql.Result, error)
		// RangeDeleteMessageFromReplicationTasksDLQ deletes one or more rows from replication_tasks_dlq table
		// Required filter params - {sourceClusterName, shardID, taskID, inclusiveTaskID}
		RangeDeleteMessageFromReplicationTasksDLQ(filter *ReplicationTasksDLQFilter) (sql.Result, error)

		ReplaceIntoActivityInfoMaps(rows []ActivityInfoMapsRow) (sql.Result, error)
		// SelectFromActivityInfoMaps returns one or more rows from activity_info_maps
		// Required filter params - {shardID, domainID, workflowID, runID}
		SelectFromActivityInfoMaps(filter *ActivityInfoMapsFilter) ([]ActivityInfoMapsRow, error)
		// DeleteFromActivityInfoMaps deletes a row from activity_info_maps table
		// Required filter params
		// - single row delete - {shardID, domainID, workflowID, runID, scheduleID}
		// - range delete - {shardID, domainID, workflowID, runID}
		DeleteFromActivityInfoMaps(filter *ActivityInfoMapsFilter) (sql.Result, error)

		ReplaceIntoTimerInfoMaps(rows []TimerInfoMapsRow) (sql.Result, error)
		// SelectFromTimerInfoMaps returns one or more rows form timer_info_maps table
		// Required filter params - {shardID, domainID, workflowID, runID}
		SelectFromTimerInfoMaps(filter *TimerInfoMapsFilter) ([]TimerInfoMapsRow, error)
		// DeleteFromTimerInfoMaps deletes one or more rows from timer_info_maps
		// Required filter params
		// - single row - {shardID, domainID, workflowID, runID, timerID}
		// - multiple rows - {shardID, domainID, workflowID, runID}
		DeleteFromTimerInfoMaps(filter *TimerInfoMapsFilter) (sql.Result, error)

		ReplaceIntoChildExecutionInfoMaps(rows []ChildExecutionInfoMapsRow) (sql.Result, error)
		// SelectFromChildExecutionInfoMaps returns one or more rows form child_execution_info_maps table
		// Required filter params - {shardID, domainID, workflowID, runID}
		SelectFromChildExecutionInfoMaps(filter *ChildExecutionInfoMapsFilter) ([]ChildExecutionInfoMapsRow, error)
		// DeleteFromChildExecutionInfoMaps deletes one or more rows from child_execution_info_maps
		// Required filter params
		// - single row - {shardID, domainID, workflowID, runID, initiatedID}
		// - multiple rows - {shardID, domainID, workflowID, runID}
		DeleteFromChildExecutionInfoMaps(filter *ChildExecutionInfoMapsFilter) (sql.Result, error)

		ReplaceIntoRequestCancelInfoMaps(rows []RequestCancelInfoMapsRow) (sql.Result, error)
		// SelectFromRequestCancelInfoMaps returns one or more rows form request_cancel_info_maps table
		// Required filter params - {shardID, domainID, workflowID, runID}
		SelectFromRequestCancelInfoMaps(filter *RequestCancelInfoMapsFilter) ([]RequestCancelInfoMapsRow, error)
		// DeleteFromRequestCancelInfoMaps deletes one or more rows from request_cancel_info_maps
		// Required filter params
		// - single row - {shardID, domainID, workflowID, runID, initiatedID}
		// - multiple rows - {shardID, domainID, workflowID, runID}
		DeleteFromRequestCancelInfoMaps(filter *RequestCancelInfoMapsFilter) (sql.Result, error)

		ReplaceIntoSignalInfoMaps(rows []SignalInfoMapsRow) (sql.Result, error)
		// SelectFromSignalInfoMaps returns one or more rows form signal_info_maps table
		// Required filter params - {shardID, domainID, workflowID, runID}
		SelectFromSignalInfoMaps(filter *SignalInfoMapsFilter) ([]SignalInfoMapsRow, error)
		// DeleteFromSignalInfoMaps deletes one or more rows from signal_info_maps table
		// Required filter params
		// - single row - {shardID, domainID, workflowID, runID, initiatedID}
		// - multiple rows - {shardID, domainID, workflowID, runID}
		DeleteFromSignalInfoMaps(filter *SignalInfoMapsFilter) (sql.Result, error)

		InsertIntoSignalsRequestedSets(rows []SignalsRequestedSetsRow) (sql.Result, error)
		// SelectFromSignalInfoMaps returns one or more rows form singals_requested_sets table
		// Required filter params - {shardID, domainID, workflowID, runID}
		SelectFromSignalsRequestedSets(filter *SignalsRequestedSetsFilter) ([]SignalsRequestedSetsRow, error)
		// DeleteFromSignalsRequestedSets deletes one or more rows from signals_requested_sets
		// Required filter params
		// - single row - {shardID, domainID, workflowID, runID, signalID}
		// - multiple rows - {shardID, domainID, workflowID, runID}
		DeleteFromSignalsRequestedSets(filter *SignalsRequestedSetsFilter) (sql.Result, error)

		// InsertIntoVisibility inserts a row into visibility table. If a row already exist,
		// no changes will be made by this API
		InsertIntoVisibility(row *VisibilityRow) (sql.Result, error)
		// ReplaceIntoVisibility deletes old row (if it exist) and inserts new row into visibility table
		ReplaceIntoVisibility(row *VisibilityRow) (sql.Result, error)
		// SelectFromVisibility returns one or more rows from visibility table
		// Required filter params:
		// - getClosedWorkflowExecution - retrieves single row - {domainID, runID, closed=true}
		// - All other queries retrieve multiple rows (range):
		//   - MUST specify following required params:
		//     - domainID, minStartTime, maxStartTime, runID and pageSize where some or all of these may come from previous page token
		//   - OPTIONALLY specify one of following params
		//     - workflowID, workflowTypeName, closeStatus (along with closed=true)
		SelectFromVisibility(filter *VisibilityFilter) ([]VisibilityRow, error)
		DeleteFromVisibility(filter *VisibilityFilter) (sql.Result, error)

		InsertIntoQueue(row *QueueRow) (sql.Result, error)
		GetLastEnqueuedMessageIDForUpdate(queueType persistence.QueueType) (int, error)
		GetMessagesFromQueue(queueType persistence.QueueType, lastMessageID, maxRows int) ([]QueueRow, error)
		GetMessagesBetween(queueType persistence.QueueType, firstMessageID int, lastMessageID int, maxRows int) ([]QueueRow, error)
		DeleteMessagesBefore(queueType persistence.QueueType, messageID int) (sql.Result, error)
		RangeDeleteMessages(queueType persistence.QueueType, exclusiveBeginMessageID int, inclusiveEndMessageID int) (sql.Result, error)
		DeleteMessage(queueType persistence.QueueType, messageID int) (sql.Result, error)
		InsertAckLevel(queueType persistence.QueueType, messageID int, clusterName string) error
		UpdateAckLevels(queueType persistence.QueueType, clusterAckLevels map[string]int) error
		GetAckLevels(queueType persistence.QueueType, forUpdate bool) (map[string]int, error)
	}

	// adminCRUD defines admin operations for CLI and test suites
	adminCRUD interface {
		CreateSchemaVersionTables() error
		ReadSchemaVersion(database string) (string, error)
		UpdateSchemaVersion(database string, newVersion string, minCompatibleVersion string) error
		WriteSchemaUpdateLog(oldVersion string, newVersion string, manifestMD5 string, desc string) error
		ListTables(database string) ([]string, error)
		DropTable(table string) error
		DropAllTables(database string) error
		CreateDatabase(database string) error
		DropDatabase(database string) error
		Exec(stmt string, args ...interface{}) error
	}

	// Tx defines the API for a SQL transaction
	Tx interface {
		tableCRUD
		Commit() error
		Rollback() error
	}

	// DB defines the API for regular SQL operations of a Cadence server
	DB interface {
		tableCRUD

		BeginTx() (Tx, error)
		PluginName() string
		IsDupEntryError(err error) bool
		Close() error
	}

	// AdminDB defines the API for admin SQL operations for CLI and testing suites
	AdminDB interface {
		adminCRUD
		PluginName() string
		Close() error
	}
	// Conn defines the API for a single database connection
	Conn interface {
		Exec(query string, args ...interface{}) (sql.Result, error)
		NamedExec(query string, arg interface{}) (sql.Result, error)
		Get(dest interface{}, query string, args ...interface{}) error
		Select(dest interface{}, query string, args ...interface{}) error
	}
)
