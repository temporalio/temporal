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

package sqldb

import (
	"database/sql"
	"time"
)

type (
	// DomainRow represents a row in domain table
	DomainRow struct {
		ID                          string
		Name                        string
		Status                      int
		Description                 string
		OwnerEmail                  string
		Data                        []byte
		Retention                   int
		EmitMetric                  bool
		ArchivalBucket              string
		ArchivalStatus              int
		ConfigVersion               int64
		NotificationVersion         int64
		FailoverNotificationVersion int64
		FailoverVersion             int64
		IsGlobalDomain              bool
		ActiveClusterName           string
		Clusters                    []byte
	}

	// DomainFilter contains the column names within domain table that
	// can be used to filter results through a WHERE clause. When ID is not
	// nil, it will be used for WHERE condition. If ID is nil and Name is non-nil,
	// Name will be used for WHERE condition. When both ID and Name are nil,
	// no WHERE clause will be used
	DomainFilter struct {
		ID   *string
		Name *string
	}

	// DomainMetadataRow represents a row in domain_metadata table
	DomainMetadataRow struct {
		NotificationVersion int64
	}

	// ShardsRow represents a row in shards table
	ShardsRow struct {
		ShardID                   int64
		Owner                     string
		RangeID                   int64
		StolenSinceRenew          int64
		UpdatedAt                 time.Time
		ReplicationAckLevel       int64
		TransferAckLevel          int64
		TimerAckLevel             time.Time
		ClusterTransferAckLevel   []byte
		ClusterTimerAckLevel      []byte
		DomainNotificationVersion int64
	}

	// ShardsFilter contains the column names within shards table that
	// can be used to filter results through a WHERE clause
	ShardsFilter struct {
		ShardID int64
	}

	// TransferTasksRow represents a row in transfer_tasks table
	TransferTasksRow struct {
		ShardID                 int
		TaskID                  int64
		DomainID                string
		WorkflowID              string
		RunID                   string
		TaskType                int
		TargetDomainID          string
		TargetWorkflowID        string
		TargetRunID             string
		TargetChildWorkflowOnly bool
		TaskList                string
		ScheduleID              int64
		Version                 int64
		VisibilityTimestamp     time.Time
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
		ShardID                      int
		DomainID                     string
		WorkflowID                   string
		RunID                        string
		ParentDomainID               *string
		ParentWorkflowID             *string
		ParentRunID                  *string
		InitiatedID                  *int64
		CompletionEventBatchID       *int64
		CompletionEvent              *[]byte
		CompletionEventEncoding      *string
		TaskList                     string
		WorkflowTypeName             string
		WorkflowTimeoutSeconds       int64
		DecisionTaskTimeoutMinutes   int64
		ExecutionContext             *[]byte
		State                        int64
		CloseStatus                  int64
		StartVersion                 int64
		CurrentVersion               int64
		LastWriteVersion             int64
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
		SignalCount                  int
		CronSchedule                 string
	}

	// ExecutionsFilter contains the column names within domain table that
	// can be used to filter results through a WHERE clause
	ExecutionsFilter struct {
		ShardID    int
		DomainID   string
		WorkflowID string
		RunID      string
	}

	// CurrentExecutionsRow represents a row in current_executions table
	CurrentExecutionsRow struct {
		ShardID          int64
		DomainID         string
		WorkflowID       string
		RunID            string
		CreateRequestID  string
		State            int
		CloseStatus      int
		LastWriteVersion int64
		StartVersion     int64
	}

	// CurrentExecutionsFilter contains the column names within domain table that
	// can be used to filter results through a WHERE clause
	CurrentExecutionsFilter struct {
		ShardID    int64
		DomainID   string
		WorkflowID string
	}

	// BufferedEventsRow represents a row in buffered_events table
	BufferedEventsRow struct {
		ShardID      int
		DomainID     string
		WorkflowID   string
		RunID        string
		Data         []byte
		DataEncoding string
	}

	// BufferedEventsFilter contains the column names within domain table that
	// can be used to filter results through a WHERE clause
	BufferedEventsFilter struct {
		ShardID    int
		DomainID   string
		WorkflowID string
		RunID      string
	}

	// TasksRow represents a row in tasks table
	TasksRow struct {
		DomainID     string
		TaskType     int64
		TaskID       int64
		TaskListName string
		WorkflowID   string
		RunID        string
		ScheduleID   int64
		ExpiryTs     time.Time
	}

	// TasksFilter contains the column names within domain table that
	// can be used to filter results through a WHERE clause
	TasksFilter struct {
		DomainID     string
		TaskListName string
		TaskType     int64
		TaskID       *int64
		MinTaskID    *int64
		MaxTaskID    *int64
	}

	// TaskListsRow represents a row in task_lists table
	TaskListsRow struct {
		DomainID string
		Name     string
		TaskType int64
		RangeID  int64
		AckLevel int64
		Kind     int64
		ExpiryTs time.Time
	}

	// TaskListsFilter contains the column names within domain table that
	// can be used to filter results through a WHERE clause
	TaskListsFilter struct {
		DomainID string
		Name     string
		TaskType int64
	}

	// ReplicationTasksRow represents a row in replication_tasks table
	ReplicationTasksRow struct {
		ShardID             int
		TaskID              int64
		DomainID            string
		WorkflowID          string
		RunID               string
		TaskType            int
		FirstEventID        int64
		NextEventID         int64
		Version             int64
		LastReplicationInfo []byte
		ScheduledID         int64
	}

	// ReplicationTasksFilter contains the column names within domain table that
	// can be used to filter results through a WHERE clause
	ReplicationTasksFilter struct {
		ShardID   int
		TaskID    *int64
		MinTaskID *int64
		MaxTaskID *int64
		PageSize  *int
	}

	// TimerTasksRow represents a row in timer_tasks table
	TimerTasksRow struct {
		ShardID             int
		VisibilityTimestamp time.Time
		TaskID              int64
		DomainID            string
		WorkflowID          string
		RunID               string
		TaskType            int
		TimeoutType         int
		EventID             int64
		ScheduleAttempt     int64
		Version             int64
	}

	// TimerTasksFilter contains the column names within domain table that
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
		DomainID     string
		WorkflowID   string
		RunID        string
		FirstEventID int64
		BatchVersion int64
		RangeID      int64
		TxID         int64
		Data         []byte
		DataEncoding string
	}

	// EventsFilter contains the column names within domain table that
	// can be used to filter results through a WHERE clause
	EventsFilter struct {
		DomainID     string
		WorkflowID   string
		RunID        string
		FirstEventID *int64
		NextEventID  *int64
		PageSize     *int
	}

	// ActivityInfoMapsRow represents a row in activity_info_maps table
	ActivityInfoMapsRow struct {
		ShardID                  int64
		DomainID                 string
		WorkflowID               string
		RunID                    string
		ScheduleID               int64
		Version                  int64
		ScheduledEventBatchID    int64
		ScheduledEvent           []byte
		ScheduledEventEncoding   string
		ScheduledTime            time.Time
		StartedID                int64
		StartedEvent             *[]byte
		StartedEventEncoding     string
		StartedTime              time.Time
		ActivityID               string
		RequestID                string
		Details                  *[]byte
		ScheduleToStartTimeout   int64
		ScheduleToCloseTimeout   int64
		StartToCloseTimeout      int64
		HeartbeatTimeout         int64
		CancelRequested          int64
		CancelRequestID          int64
		LastHeartbeatUpdatedTime time.Time
		TimerTaskStatus          int64
		Attempt                  int64
		TaskList                 string
		StartedIdentity          string
		HasRetryPolicy           int64
		InitInterval             int64
		BackoffCoefficient       float64
		MaxInterval              int64
		ExpirationTime           time.Time
		MaxAttempts              int64
		NonRetriableErrors       *[]byte
	}

	// ActivityInfoMapsFilter contains the column names within domain table that
	// can be used to filter results through a WHERE clause
	ActivityInfoMapsFilter struct {
		ShardID    int64
		DomainID   string
		WorkflowID string
		RunID      string
		ScheduleID *int64
	}

	// TimerInfoMapsRow represents a row in timer_info_maps table
	TimerInfoMapsRow struct {
		ShardID    int64
		DomainID   string
		WorkflowID string
		RunID      string
		TimerID    string
		Version    int64
		StartedID  int64
		ExpiryTime time.Time
		TaskID     int64
	}

	// TimerInfoMapsFilter contains the column names within domain table that
	// can be used to filter results through a WHERE clause
	TimerInfoMapsFilter struct {
		ShardID    int64
		DomainID   string
		WorkflowID string
		RunID      string
		TimerID    *string
	}

	// ChildExecutionInfoMapsRow represents a row in child_execution_info_maps table
	ChildExecutionInfoMapsRow struct {
		ShardID                int64
		DomainID               string
		WorkflowID             string
		RunID                  string
		InitiatedID            int64
		Version                int64
		InitiatedEventBatchID  int64
		InitiatedEvent         *[]byte
		InitiatedEventEncoding string
		StartedID              int64
		StartedWorkflowID      string
		StartedRunID           string
		StartedEvent           *[]byte
		StartedEventEncoding   string
		CreateRequestID        string
		DomainName             string
		WorkflowTypeName       string
	}

	// ChildExecutionInfoMapsFilter contains the column names within domain table that
	// can be used to filter results through a WHERE clause
	ChildExecutionInfoMapsFilter struct {
		ShardID     int64
		DomainID    string
		WorkflowID  string
		RunID       string
		InitiatedID *int64
	}

	// RequestCancelInfoMapsRow represents a row in request_cancel_info_maps table
	RequestCancelInfoMapsRow struct {
		ShardID         int64
		DomainID        string
		WorkflowID      string
		RunID           string
		InitiatedID     int64
		Version         int64
		CancelRequestID string
	}

	// RequestCancelInfoMapsFilter contains the column names within domain table that
	// can be used to filter results through a WHERE clause
	RequestCancelInfoMapsFilter struct {
		ShardID     int64
		DomainID    string
		WorkflowID  string
		RunID       string
		InitiatedID *int64
	}

	// SignalInfoMapsRow represents a row in signal_info_maps table
	SignalInfoMapsRow struct {
		ShardID         int64
		DomainID        string
		WorkflowID      string
		RunID           string
		InitiatedID     int64
		Version         int64
		SignalRequestID string
		SignalName      string
		Input           *[]byte
		Control         *[]byte
	}

	// SignalInfoMapsFilter contains the column names within domain table that
	// can be used to filter results through a WHERE clause
	SignalInfoMapsFilter struct {
		ShardID     int64
		DomainID    string
		WorkflowID  string
		RunID       string
		InitiatedID *int64
	}

	// BufferedReplicationTaskMapsRow represents a row in buffered_replication_task_maps table
	BufferedReplicationTaskMapsRow struct {
		ShardID               int64
		DomainID              string
		WorkflowID            string
		RunID                 string
		FirstEventID          int64
		NextEventID           int64
		Version               int64
		History               *[]byte
		HistoryEncoding       string
		NewRunHistory         *[]byte
		NewRunHistoryEncoding string
	}

	// BufferedReplicationTaskMapsFilter contains the column names within domain table that
	// can be used to filter results through a WHERE clause
	BufferedReplicationTaskMapsFilter struct {
		ShardID      int64
		DomainID     string
		WorkflowID   string
		RunID        string
		FirstEventID *int64
	}

	// SignalsRequestedSetsRow represents a row in signals_requested_sets table
	SignalsRequestedSetsRow struct {
		ShardID    int64
		DomainID   string
		WorkflowID string
		RunID      string
		SignalID   string
	}

	// SignalsRequestedSetsFilter contains the column names within domain table that
	// can be used to filter results through a WHERE clause
	SignalsRequestedSetsFilter struct {
		ShardID    int64
		DomainID   string
		WorkflowID string
		RunID      string
		SignalID   *string
	}

	// VisibilityRow represents a row in executions_visibility table
	VisibilityRow struct {
		DomainID         string
		RunID            string
		WorkflowTypeName string
		WorkflowID       string
		StartTime        time.Time
		CloseStatus      *int32
		CloseTime        *time.Time
		HistoryLength    *int64
	}

	// VisibilityFilter contains the column names within domain table that
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
		SelectFromTasks(filter *TasksFilter) ([]TasksRow, error)
		DeleteFromTasks(filter *TasksFilter) (sql.Result, error)

		InsertIntoTaskLists(row *TaskListsRow) (sql.Result, error)
		ReplaceIntoTaskLists(row *TaskListsRow) (sql.Result, error)
		UpdateTaskLists(row *TaskListsRow) (sql.Result, error)
		SelectFromTaskLists(filter *TaskListsFilter) (*TaskListsRow, error)
		DeleteFromTaskLists(filter *TaskListsFilter) (sql.Result, error)
		LockTaskLists(filter *TaskListsFilter) (int64, error)

		InsertIntoEvents(row *EventsRow) (sql.Result, error)
		UpdateEvents(rows *EventsRow) (sql.Result, error)
		SelectFromEvents(filter *EventsFilter) ([]EventsRow, error)
		DeleteFromEvents(filter *EventsFilter) (sql.Result, error)
		LockEvents(filter *EventsFilter) (*EventsRow, error)

		InsertIntoExecutions(row *ExecutionsRow) (sql.Result, error)
		UpdateExecutions(row *ExecutionsRow) (sql.Result, error)
		SelectFromExecutions(filter *ExecutionsFilter) (*ExecutionsRow, error)
		DeleteFromExecutions(filter *ExecutionsFilter) (sql.Result, error)
		LockExecutions(filter *ExecutionsFilter) (int, error)

		LockCurrentExecutionsJoinExecutions(filter *CurrentExecutionsFilter) ([]CurrentExecutionsRow, error)

		InsertIntoCurrentExecutions(row *CurrentExecutionsRow) (sql.Result, error)
		UpdateCurrentExecutions(row *CurrentExecutionsRow) (sql.Result, error)
		SelectFromCurrentExecutions(filter *CurrentExecutionsFilter) (*CurrentExecutionsRow, error)
		DeleteFromCurrentExecutions(filter *CurrentExecutionsFilter) (sql.Result, error)
		LockCurrentExecutions(filter *CurrentExecutionsFilter) (string, error)

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
		// Required filter params - {shardID, taskID}
		DeleteFromReplicationTasks(filter *ReplicationTasksFilter) (sql.Result, error)

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

		ReplaceIntoBufferedReplicationTasks(rows *BufferedReplicationTaskMapsRow) (sql.Result, error)
		// SelectFromSignalInfoMaps returns one or more rows form buffered_replication_tasks table
		// Required filter params - {shardID, domainID, workflowID, runID}
		SelectFromBufferedReplicationTasks(filter *BufferedReplicationTaskMapsFilter) ([]BufferedReplicationTaskMapsRow, error)
		// DeleteFromBufferedReplicationTasks deletes one or more rows from buffered_replication_tasks
		// Required filter params
		// - single row - {shardID, domainID, workflowID, runID, firstEventID}
		// - multiple rows - {shardID, domainID, workflowID, runID}
		DeleteFromBufferedReplicationTasks(filter *BufferedReplicationTaskMapsFilter) (sql.Result, error)

		InsertIntoSignalsRequestedSets(rows []SignalsRequestedSetsRow) (sql.Result, error)
		// SelectFromSignalInfoMaps returns one or more rows form singals_requested_sets table
		// Required filter params - {shardID, domainID, workflowID, runID}
		SelectFromSignalsRequestedSets(filter *SignalsRequestedSetsFilter) ([]SignalsRequestedSetsRow, error)
		// DeleteFromSignalsRequestedSets deletes one or more rows from signals_requested_sets
		// Required filter params
		// - single row - {shardID, domainID, workflowID, runID, signalID}
		// - multiple rows - {shardID, domainID, workflowID, runID}
		DeleteFromSignalsRequestedSets(filter *SignalsRequestedSetsFilter) (sql.Result, error)

		InsertIntoVisibility(row *VisibilityRow) (sql.Result, error)
		UpdateVisibility(row *VisibilityRow) (sql.Result, error)
		// SelectFromVisibility returns one or more rows from visibility table
		// Required filter params:
		// - getClosedWorkflowExecution - retrieves single row - {domainID, runID, closed=true}
		// - All other queries retrieve multiple rows (range):
		//   - MUST specify following required params:
		//     - domainID, minStartTime, maxStartTime, runID and pageSize where some or all of these may come from previous page token
		//   - OPTIONALLY specify one of following params
		//     - workflowID, workflowTypeName, closeStatus (along with closed=true)
		SelectFromVisibility(filter *VisibilityFilter) ([]VisibilityRow, error)
	}

	// Tx defines the API for a SQL transaction
	Tx interface {
		tableCRUD
		Commit() error
		Rollback() error
	}

	// Interface defines the API for a SQL database
	Interface interface {
		tableCRUD
		BeginTx() (Tx, error)
		DriverName() string
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
