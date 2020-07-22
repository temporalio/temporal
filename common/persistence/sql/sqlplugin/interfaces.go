// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
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

	enumspb "go.temporal.io/api/enums/v1"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/service/config"
)

type (
	// Plugin defines the interface for any SQL database that needs to implement
	Plugin interface {
		CreateDB(cfg *config.SQL) (DB, error)
		CreateAdminDB(cfg *config.SQL) (AdminDB, error)
	}

	// ClusterMetadataRow represents a row in the cluster_metadata table
	ClusterMetadataRow struct {
		ImmutableData         []byte
		ImmutableDataEncoding string
	}

	// ClusterMembershipRow represents a row in the cluster_membership table
	ClusterMembershipRow struct {
		Role           persistence.ServiceType
		HostID         []byte
		RPCAddress     string
		RPCPort        uint16
		SessionStart   time.Time
		LastHeartbeat  time.Time
		RecordExpiry   time.Time
		InsertionOrder uint64
	}

	// ClusterMembershipFilter is used for GetClusterMembership queries
	ClusterMembershipFilter struct {
		RPCAddressEquals    string
		HostIDEquals        []byte
		HostIDGreaterThan   []byte
		RoleEquals          persistence.ServiceType
		LastHeartbeatAfter  time.Time
		RecordExpiryAfter   time.Time
		SessionStartedAfter time.Time
		MaxRecordCount      int
	}

	// PruneClusterMembershipFilter is used for PruneClusterMembership queries
	PruneClusterMembershipFilter struct {
		PruneRecordsBefore time.Time
		MaxRecordsAffected int
	}

	// NamespaceRow represents a row in namespace table
	NamespaceRow struct {
		ID                  primitives.UUID
		Name                string
		Data                []byte
		DataEncoding        string
		IsGlobal            bool
		NotificationVersion int64
	}

	// NamespaceFilter contains the column names within namespace table that
	// can be used to filter results through a WHERE clause. When ID is not
	// nil, it will be used for WHERE condition. If ID is nil and Name is non-nil,
	// Name will be used for WHERE condition. When both ID and Name are nil,
	// no WHERE clause will be used
	NamespaceFilter struct {
		ID            *primitives.UUID
		Name          *string
		GreaterThanID *primitives.UUID
		PageSize      *int
	}

	// NamespaceMetadataRow represents a row in namespace_metadata table
	NamespaceMetadataRow struct {
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
		NamespaceID              primitives.UUID
		WorkflowID               string
		RunID                    primitives.UUID
		NextEventID              int64
		LastWriteVersion         int64
		Data                     []byte
		DataEncoding             string
		State                    []byte
		StateEncoding            string
		VersionHistories         []byte
		VersionHistoriesEncoding string
	}

	// ExecutionsFilter contains the column names within executions table that
	// can be used to filter results through a WHERE clause
	ExecutionsFilter struct {
		ShardID     int
		NamespaceID primitives.UUID
		WorkflowID  string
		RunID       primitives.UUID
	}

	// CurrentExecutionsRow represents a row in current_executions table
	CurrentExecutionsRow struct {
		ShardID          int64
		NamespaceID      primitives.UUID
		WorkflowID       string
		RunID            primitives.UUID
		CreateRequestID  string
		State            enumsspb.WorkflowExecutionState
		Status           enumspb.WorkflowExecutionStatus
		LastWriteVersion int64
		StartVersion     int64
	}

	// CurrentExecutionsFilter contains the column names within current_executions table that
	// can be used to filter results through a WHERE clause
	CurrentExecutionsFilter struct {
		ShardID     int64
		NamespaceID primitives.UUID
		WorkflowID  string
		RunID       primitives.UUID
	}

	// BufferedEventsRow represents a row in buffered_events table
	BufferedEventsRow struct {
		ShardID      int
		NamespaceID  primitives.UUID
		WorkflowID   string
		RunID        primitives.UUID
		Data         []byte
		DataEncoding string
	}

	// BufferedEventsFilter contains the column names within buffered_events table that
	// can be used to filter results through a WHERE clause
	BufferedEventsFilter struct {
		ShardID     int
		NamespaceID primitives.UUID
		WorkflowID  string
		RunID       primitives.UUID
	}

	// TasksRow represents a row in tasks table
	TasksRow struct {
		RangeHash    uint32
		TaskQueueID  []byte
		TaskID       int64
		Data         []byte
		DataEncoding string
	}

	// TasksFilter contains the column names within tasks table that
	// can be used to filter results through a WHERE clause
	TasksFilter struct {
		RangeHash            uint32
		TaskQueueID          []byte
		TaskID               *int64
		MinTaskID            *int64
		MaxTaskID            *int64
		TaskIDLessThanEquals *int64
		Limit                *int
		PageSize             *int
	}

	// TaskQueuesRow represents a row in task_queues table
	TaskQueuesRow struct {
		RangeHash    uint32
		TaskQueueID  []byte
		RangeID      int64
		Data         []byte
		DataEncoding string
	}

	// TaskQueuesFilter contains the column names within task_queues table that
	// can be used to filter results through a WHERE clause
	TaskQueuesFilter struct {
		RangeHash                   uint32
		RangeHashGreaterThanEqualTo uint32
		RangeHashLessThanEqualTo    uint32
		TaskQueueID                 []byte
		TaskQueueIDGreaterThan      []byte
		RangeID                     *int64
		PageSize                    *int
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
		NamespaceID  primitives.UUID
		WorkflowID   string
		RunID        primitives.UUID
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
		NamespaceID  primitives.UUID
		WorkflowID   string
		RunID        primitives.UUID
		FirstEventID *int64
		NextEventID  *int64
		PageSize     *int
	}

	// HistoryNodeRow represents a row in history_node table
	HistoryNodeRow struct {
		ShardID  int
		TreeID   primitives.UUID
		BranchID primitives.UUID
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
		TreeID   primitives.UUID
		BranchID primitives.UUID
		// Inclusive
		MinNodeID *int64
		// Exclusive
		MaxNodeID *int64
		PageSize  *int
	}

	// HistoryTreeRow represents a row in history_tree table
	HistoryTreeRow struct {
		ShardID      int
		TreeID       primitives.UUID
		BranchID     primitives.UUID
		Data         []byte
		DataEncoding string
	}

	// HistoryTreeFilter contains the column names within history_tree table that
	// can be used to filter results through a WHERE clause
	HistoryTreeFilter struct {
		ShardID  int
		TreeID   primitives.UUID
		BranchID primitives.UUID
	}

	// ActivityInfoMapsRow represents a row in activity_info_maps table
	ActivityInfoMapsRow struct {
		ShardID      int64
		NamespaceID  primitives.UUID
		WorkflowID   string
		RunID        primitives.UUID
		ScheduleID   int64
		Data         []byte
		DataEncoding string
	}

	// ActivityInfoMapsFilter contains the column names within activity_info_maps table that
	// can be used to filter results through a WHERE clause
	ActivityInfoMapsFilter struct {
		ShardID     int64
		NamespaceID primitives.UUID
		WorkflowID  string
		RunID       primitives.UUID
		ScheduleID  *int64
	}

	// TimerInfoMapsRow represents a row in timer_info_maps table
	TimerInfoMapsRow struct {
		ShardID      int64
		NamespaceID  primitives.UUID
		WorkflowID   string
		RunID        primitives.UUID
		TimerID      string
		Data         []byte
		DataEncoding string
	}

	// TimerInfoMapsFilter contains the column names within timer_info_maps table that
	// can be used to filter results through a WHERE clause
	TimerInfoMapsFilter struct {
		ShardID     int64
		NamespaceID primitives.UUID
		WorkflowID  string
		RunID       primitives.UUID
		TimerID     *string
	}

	// ChildExecutionInfoMapsRow represents a row in child_execution_info_maps table
	ChildExecutionInfoMapsRow struct {
		ShardID      int64
		NamespaceID  primitives.UUID
		WorkflowID   string
		RunID        primitives.UUID
		InitiatedID  int64
		Data         []byte
		DataEncoding string
	}

	// ChildExecutionInfoMapsFilter contains the column names within child_execution_info_maps table that
	// can be used to filter results through a WHERE clause
	ChildExecutionInfoMapsFilter struct {
		ShardID     int64
		NamespaceID primitives.UUID
		WorkflowID  string
		RunID       primitives.UUID
		InitiatedID *int64
	}

	// RequestCancelInfoMapsRow represents a row in request_cancel_info_maps table
	RequestCancelInfoMapsRow struct {
		ShardID      int64
		NamespaceID  primitives.UUID
		WorkflowID   string
		RunID        primitives.UUID
		InitiatedID  int64
		Data         []byte
		DataEncoding string
	}

	// RequestCancelInfoMapsFilter contains the column names within request_cancel_info_maps table that
	// can be used to filter results through a WHERE clause
	RequestCancelInfoMapsFilter struct {
		ShardID     int64
		NamespaceID primitives.UUID
		WorkflowID  string
		RunID       primitives.UUID
		InitiatedID *int64
	}

	// SignalInfoMapsRow represents a row in signal_info_maps table
	SignalInfoMapsRow struct {
		ShardID      int64
		NamespaceID  primitives.UUID
		WorkflowID   string
		RunID        primitives.UUID
		InitiatedID  int64
		Data         []byte
		DataEncoding string
	}

	// SignalInfoMapsFilter contains the column names within signal_info_maps table that
	// can be used to filter results through a WHERE clause
	SignalInfoMapsFilter struct {
		ShardID     int64
		NamespaceID primitives.UUID
		WorkflowID  string
		RunID       primitives.UUID
		InitiatedID *int64
	}

	// SignalsRequestedSetsRow represents a row in signals_requested_sets table
	SignalsRequestedSetsRow struct {
		ShardID     int64
		NamespaceID primitives.UUID
		WorkflowID  string
		RunID       primitives.UUID
		SignalID    string
	}

	// SignalsRequestedSetsFilter contains the column names within signals_requested_sets table that
	// can be used to filter results through a WHERE clause
	SignalsRequestedSetsFilter struct {
		ShardID     int64
		NamespaceID primitives.UUID
		WorkflowID  string
		RunID       primitives.UUID
		SignalID    *string
	}

	// VisibilityRow represents a row in executions_visibility table
	VisibilityRow struct {
		NamespaceID      string
		RunID            string
		WorkflowTypeName string
		WorkflowID       string
		StartTime        time.Time
		ExecutionTime    time.Time
		Status           int32
		CloseTime        *time.Time
		HistoryLength    *int64
		Memo             []byte
		Encoding         string
	}

	// VisibilityFilter contains the column names within executions_visibility table that
	// can be used to filter results through a WHERE clause
	VisibilityFilter struct {
		NamespaceID      string
		RunID            *string
		WorkflowID       *string
		WorkflowTypeName *string
		Status           int32
		MinStartTime     *time.Time
		MaxStartTime     *time.Time
		PageSize         *int
	}

	// QueueRow represents a row in queue table
	QueueRow struct {
		QueueType      persistence.QueueType
		MessageID      int64
		MessagePayload []byte
	}

	// QueueMetadataRow represents a row in queue_metadata table
	QueueMetadataRow struct {
		QueueType persistence.QueueType
		Data      []byte
	}

	// tableCRUD defines the API for interacting with the database tables
	tableCRUD interface {
		InsertIfNotExistsIntoClusterMetadata(row *ClusterMetadataRow) (sql.Result, error)
		GetClusterMetadata() (*ClusterMetadataRow, error)
		GetClusterMembers(filter *ClusterMembershipFilter) ([]ClusterMembershipRow, error)
		UpsertClusterMembership(row *ClusterMembershipRow) (sql.Result, error)
		PruneClusterMembership(filter *PruneClusterMembershipFilter) (sql.Result, error)

		InsertIntoNamespace(rows *NamespaceRow) (sql.Result, error)
		UpdateNamespace(row *NamespaceRow) (sql.Result, error)
		// SelectFromNamespace returns namespaces that match filter criteria. Either ID or
		// Name can be specified to filter results. If both are not specified, all rows
		// will be returned
		SelectFromNamespace(filter *NamespaceFilter) ([]NamespaceRow, error)
		// DeleteNamespace deletes a single row. One of ID or Name MUST be specified
		DeleteFromNamespace(filter *NamespaceFilter) (sql.Result, error)

		LockNamespaceMetadata() error
		UpdateNamespaceMetadata(row *NamespaceMetadataRow) (sql.Result, error)
		SelectFromNamespaceMetadata() (*NamespaceMetadataRow, error)

		InsertIntoShards(rows *ShardsRow) (sql.Result, error)
		UpdateShards(row *ShardsRow) (sql.Result, error)
		SelectFromShards(filter *ShardsFilter) (*ShardsRow, error)
		ReadLockShards(filter *ShardsFilter) (int, error)
		WriteLockShards(filter *ShardsFilter) (int, error)

		InsertIntoTasks(rows []TasksRow) (sql.Result, error)
		// SelectFromTasks retrieves one or more rows from the tasks table
		// Required filter params - {namespaceID, taskqueueName, taskType, minTaskID, maxTaskID, pageSize}
		SelectFromTasks(filter *TasksFilter) ([]TasksRow, error)
		// DeleteFromTasks deletes a row from tasks table
		// Required filter params:
		//  to delete single row
		//     - {namespaceID, taskqueueName, taskType, taskID}
		//  to delete multiple rows
		//    - {namespaceID, taskqueueName, taskType, taskIDLessThanEquals, limit }
		//    - this will delete upto limit number of tasks less than or equal to the given task id
		DeleteFromTasks(filter *TasksFilter) (sql.Result, error)

		InsertIntoTaskQueues(row *TaskQueuesRow) (sql.Result, error)
		ReplaceIntoTaskQueues(row *TaskQueuesRow) (sql.Result, error)
		UpdateTaskQueues(row *TaskQueuesRow) (sql.Result, error)
		// SelectFromTaskQueues returns one or more rows from task_queues table
		// Required Filter params:
		//  to read a single row: {shardID, namespaceID, name, taskType}
		//  to range read multiple rows: {shardID, namespaceIDGreaterThan, nameGreaterThan, taskTypeGreaterThan, pageSize}
		SelectFromTaskQueues(filter *TaskQueuesFilter) ([]TaskQueuesRow, error)
		DeleteFromTaskQueues(filter *TaskQueuesFilter) (sql.Result, error)
		LockTaskQueues(filter *TaskQueuesFilter) (int64, error)

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
		// Required params - {shardID, namespaceID, workflowID}
		SelectFromCurrentExecutions(filter *CurrentExecutionsFilter) (*CurrentExecutionsRow, error)
		// DeleteFromCurrentExecutions deletes a single row that matches the filter criteria
		// If a row exist, that row will be deleted and this method will return success
		// If there is no row matching the filter criteria, this method will still return success
		// Callers can check the output of Result.RowsAffected() to see if a row was deleted or not
		// Required params - {shardID, namespaceID, workflowID, runID}
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
		// Required filter params - {shardID, namespaceID, workflowID, runID}
		SelectFromActivityInfoMaps(filter *ActivityInfoMapsFilter) ([]ActivityInfoMapsRow, error)
		// DeleteFromActivityInfoMaps deletes a row from activity_info_maps table
		// Required filter params
		// - single row delete - {shardID, namespaceID, workflowID, runID, scheduleID}
		// - range delete - {shardID, namespaceID, workflowID, runID}
		DeleteFromActivityInfoMaps(filter *ActivityInfoMapsFilter) (sql.Result, error)

		ReplaceIntoTimerInfoMaps(rows []TimerInfoMapsRow) (sql.Result, error)
		// SelectFromTimerInfoMaps returns one or more rows form timer_info_maps table
		// Required filter params - {shardID, namespaceID, workflowID, runID}
		SelectFromTimerInfoMaps(filter *TimerInfoMapsFilter) ([]TimerInfoMapsRow, error)
		// DeleteFromTimerInfoMaps deletes one or more rows from timer_info_maps
		// Required filter params
		// - single row - {shardID, namespaceID, workflowID, runID, timerID}
		// - multiple rows - {shardID, namespaceID, workflowID, runID}
		DeleteFromTimerInfoMaps(filter *TimerInfoMapsFilter) (sql.Result, error)

		ReplaceIntoChildExecutionInfoMaps(rows []ChildExecutionInfoMapsRow) (sql.Result, error)
		// SelectFromChildExecutionInfoMaps returns one or more rows form child_execution_info_maps table
		// Required filter params - {shardID, namespaceID, workflowID, runID}
		SelectFromChildExecutionInfoMaps(filter *ChildExecutionInfoMapsFilter) ([]ChildExecutionInfoMapsRow, error)
		// DeleteFromChildExecutionInfoMaps deletes one or more rows from child_execution_info_maps
		// Required filter params
		// - single row - {shardID, namespaceID, workflowID, runID, initiatedID}
		// - multiple rows - {shardID, namespaceID, workflowID, runID}
		DeleteFromChildExecutionInfoMaps(filter *ChildExecutionInfoMapsFilter) (sql.Result, error)

		ReplaceIntoRequestCancelInfoMaps(rows []RequestCancelInfoMapsRow) (sql.Result, error)
		// SelectFromRequestCancelInfoMaps returns one or more rows form request_cancel_info_maps table
		// Required filter params - {shardID, namespaceID, workflowID, runID}
		SelectFromRequestCancelInfoMaps(filter *RequestCancelInfoMapsFilter) ([]RequestCancelInfoMapsRow, error)
		// DeleteFromRequestCancelInfoMaps deletes one or more rows from request_cancel_info_maps
		// Required filter params
		// - single row - {shardID, namespaceID, workflowID, runID, initiatedID}
		// - multiple rows - {shardID, namespaceID, workflowID, runID}
		DeleteFromRequestCancelInfoMaps(filter *RequestCancelInfoMapsFilter) (sql.Result, error)

		ReplaceIntoSignalInfoMaps(rows []SignalInfoMapsRow) (sql.Result, error)
		// SelectFromSignalInfoMaps returns one or more rows form signal_info_maps table
		// Required filter params - {shardID, namespaceID, workflowID, runID}
		SelectFromSignalInfoMaps(filter *SignalInfoMapsFilter) ([]SignalInfoMapsRow, error)
		// DeleteFromSignalInfoMaps deletes one or more rows from signal_info_maps table
		// Required filter params
		// - single row - {shardID, namespaceID, workflowID, runID, initiatedID}
		// - multiple rows - {shardID, namespaceID, workflowID, runID}
		DeleteFromSignalInfoMaps(filter *SignalInfoMapsFilter) (sql.Result, error)

		InsertIntoSignalsRequestedSets(rows []SignalsRequestedSetsRow) (sql.Result, error)
		// SelectFromSignalInfoMaps returns one or more rows form singals_requested_sets table
		// Required filter params - {shardID, namespaceID, workflowID, runID}
		SelectFromSignalsRequestedSets(filter *SignalsRequestedSetsFilter) ([]SignalsRequestedSetsRow, error)
		// DeleteFromSignalsRequestedSets deletes one or more rows from signals_requested_sets
		// Required filter params
		// - single row - {shardID, namespaceID, workflowID, runID, signalID}
		// - multiple rows - {shardID, namespaceID, workflowID, runID}
		DeleteFromSignalsRequestedSets(filter *SignalsRequestedSetsFilter) (sql.Result, error)

		// InsertIntoVisibility inserts a row into visibility table. If a row already exist,
		// no changes will be made by this API
		InsertIntoVisibility(row *VisibilityRow) (sql.Result, error)
		// ReplaceIntoVisibility deletes old row (if it exist) and inserts new row into visibility table
		ReplaceIntoVisibility(row *VisibilityRow) (sql.Result, error)
		// SelectFromVisibility returns one or more rows from visibility table
		// Required filter params:
		// - getClosedWorkflowExecution - retrieves single row - {namespaceID, runID, closed=true}
		// - All other queries retrieve multiple rows (range):
		//   - MUST specify following required params:
		//     - namespaceID, minStartTime, maxStartTime, runID and pageSize where some or all of these may come from previous page token
		//   - OPTIONALLY specify one of following params
		//     - workflowID, workflowTypeName, status (along with closed=true)
		SelectFromVisibility(filter *VisibilityFilter) ([]VisibilityRow, error)
		DeleteFromVisibility(filter *VisibilityFilter) (sql.Result, error)

		InsertIntoQueue(row *QueueRow) (sql.Result, error)
		GetLastEnqueuedMessageIDForUpdate(queueType persistence.QueueType) (int64, error)
		GetMessagesFromQueue(queueType persistence.QueueType, lastMessageID int64, maxRows int) ([]QueueRow, error)
		GetMessagesBetween(queueType persistence.QueueType, firstMessageID int64, lastMessageID int64, maxRows int) ([]QueueRow, error)
		DeleteMessagesBefore(queueType persistence.QueueType, messageID int64) (sql.Result, error)
		RangeDeleteMessages(queueType persistence.QueueType, exclusiveBeginMessageID int64, inclusiveEndMessageID int64) (sql.Result, error)
		DeleteMessage(queueType persistence.QueueType, messageID int64) (sql.Result, error)
		InsertAckLevel(queueType persistence.QueueType, messageID int64, clusterName string) error
		UpdateAckLevels(queueType persistence.QueueType, clusterAckLevels map[string]int64) error
		GetAckLevels(queueType persistence.QueueType, forUpdate bool) (map[string]int64, error)
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

	// DB defines the API for regular SQL operations of a Temporal server
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
