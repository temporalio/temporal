// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
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

import "database/sql"

type (
	// HistoryShard is the SQL persistence interface for history shards
	HistoryShard interface {
		InsertIntoShards(rows *ShardsRow) (sql.Result, error)
		UpdateShards(row *ShardsRow) (sql.Result, error)
		SelectFromShards(filter *ShardsFilter) (*ShardsRow, error)
		ReadLockShards(filter *ShardsFilter) (int64, error)
		WriteLockShards(filter *ShardsFilter) (int64, error)
	}

	// historyEvent is the SQL persistence interface for history nodes and history trees
	historyEvent interface {
		InsertIntoHistoryNode(row *HistoryNodeRow) (sql.Result, error)
		SelectFromHistoryNode(filter *HistoryNodeFilter) ([]HistoryNodeRow, error)
		DeleteFromHistoryNode(filter *HistoryNodeFilter) (sql.Result, error)
		InsertIntoHistoryTree(row *HistoryTreeRow) (sql.Result, error)
		SelectFromHistoryTree(filter *HistoryTreeFilter) ([]HistoryTreeRow, error)
		DeleteFromHistoryTree(filter *HistoryTreeFilter) (sql.Result, error)
	}

	// historyExecution is the SQL persistence interface for history nodes and history executions
	historyExecution interface {
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
	}

	// HistoryExecutionBuffer is the SQL persistence interface for history nodes and history execution buffer events
	HistoryExecutionBuffer interface {
		InsertIntoBufferedEvents(rows []BufferedEventsRow) (sql.Result, error)
		SelectFromBufferedEvents(filter *BufferedEventsFilter) ([]BufferedEventsRow, error)
		DeleteFromBufferedEvents(filter *BufferedEventsFilter) (sql.Result, error)
	}

	// HistoryExecutionActivity is the SQL persistence interface for history nodes and history execution activities
	HistoryExecutionActivity interface {
		ReplaceIntoActivityInfoMaps(rows []ActivityInfoMapsRow) (sql.Result, error)
		// SelectFromActivityInfoMaps returns one or more rows from activity_info_maps
		// Required filter params - {shardID, namespaceID, workflowID, runID}
		SelectFromActivityInfoMaps(filter *ActivityInfoMapsFilter) ([]ActivityInfoMapsRow, error)
		// DeleteFromActivityInfoMaps deletes a row from activity_info_maps table
		// Required filter params
		// - single row delete - {shardID, namespaceID, workflowID, runID, scheduleID}
		// - range delete - {shardID, namespaceID, workflowID, runID}
		DeleteFromActivityInfoMaps(filter *ActivityInfoMapsFilter) (sql.Result, error)
	}

	// HistoryExecutionChildWorkflow is the SQL persistence interface for history nodes and history execution child workflows
	HistoryExecutionChildWorkflow interface {
		ReplaceIntoChildExecutionInfoMaps(rows []ChildExecutionInfoMapsRow) (sql.Result, error)
		// SelectFromChildExecutionInfoMaps returns one or more rows form child_execution_info_maps table
		// Required filter params - {shardID, namespaceID, workflowID, runID}
		SelectFromChildExecutionInfoMaps(filter *ChildExecutionInfoMapsFilter) ([]ChildExecutionInfoMapsRow, error)
		// DeleteFromChildExecutionInfoMaps deletes one or more rows from child_execution_info_maps
		// Required filter params
		// - single row - {shardID, namespaceID, workflowID, runID, initiatedID}
		// - multiple rows - {shardID, namespaceID, workflowID, runID}
		DeleteFromChildExecutionInfoMaps(filter *ChildExecutionInfoMapsFilter) (sql.Result, error)
	}

	// HistoryExecutionTimer is the SQL persistence interface for history nodes and history execution timers
	HistoryExecutionTimer interface {
		ReplaceIntoTimerInfoMaps(rows []TimerInfoMapsRow) (sql.Result, error)
		// SelectFromTimerInfoMaps returns one or more rows form timer_info_maps table
		// Required filter params - {shardID, namespaceID, workflowID, runID}
		SelectFromTimerInfoMaps(filter *TimerInfoMapsFilter) ([]TimerInfoMapsRow, error)
		// DeleteFromTimerInfoMaps deletes one or more rows from timer_info_maps
		// Required filter params
		// - single row - {shardID, namespaceID, workflowID, runID, timerID}
		// - multiple rows - {shardID, namespaceID, workflowID, runID}
		DeleteFromTimerInfoMaps(filter *TimerInfoMapsFilter) (sql.Result, error)
	}

	// HistoryExecutionRequestCancel is the SQL persistence interface for history nodes and history execution request cancels
	HistoryExecutionRequestCancel interface {
		ReplaceIntoRequestCancelInfoMaps(rows []RequestCancelInfoMapsRow) (sql.Result, error)
		// SelectFromRequestCancelInfoMaps returns one or more rows form request_cancel_info_maps table
		// Required filter params - {shardID, namespaceID, workflowID, runID}
		SelectFromRequestCancelInfoMaps(filter *RequestCancelInfoMapsFilter) ([]RequestCancelInfoMapsRow, error)
		// DeleteFromRequestCancelInfoMaps deletes one or more rows from request_cancel_info_maps
		// Required filter params
		// - single row - {shardID, namespaceID, workflowID, runID, initiatedID}
		// - multiple rows - {shardID, namespaceID, workflowID, runID}
		DeleteFromRequestCancelInfoMaps(filter *RequestCancelInfoMapsFilter) (sql.Result, error)
	}

	// HistoryExecutionSignal is the SQL persistence interface for history nodes and history execution signals
	HistoryExecutionSignal interface {
		ReplaceIntoSignalInfoMaps(rows []SignalInfoMapsRow) (sql.Result, error)
		// SelectFromSignalInfoMaps returns one or more rows form signal_info_maps table
		// Required filter params - {shardID, namespaceID, workflowID, runID}
		SelectFromSignalInfoMaps(filter *SignalInfoMapsFilter) ([]SignalInfoMapsRow, error)
		// DeleteFromSignalInfoMaps deletes one or more rows from signal_info_maps table
		// Required filter params
		// - single row - {shardID, namespaceID, workflowID, runID, initiatedID}
		// - multiple rows - {shardID, namespaceID, workflowID, runID}
		DeleteFromSignalInfoMaps(filter *SignalInfoMapsFilter) (sql.Result, error)
	}

	// HistoryExecutionSignalRequest is the SQL persistence interface for history nodes and history execution signal request
	HistoryExecutionSignalRequest interface {
		ReplaceIntoSignalsRequestedSets(rows []SignalsRequestedSetsRow) (sql.Result, error)
		// SelectFromSignalInfoMaps returns one or more rows form signals_requested_sets table
		// Required filter params - {shardID, namespaceID, workflowID, runID}
		SelectFromSignalsRequestedSets(filter *SignalsRequestedSetsFilter) ([]SignalsRequestedSetsRow, error)
		// DeleteFromSignalsRequestedSets deletes one or more rows from signals_requested_sets
		// Required filter params
		// - single row - {shardID, namespaceID, workflowID, runID, signalID}
		// - multiple rows - {shardID, namespaceID, workflowID, runID}
		DeleteFromSignalsRequestedSets(filter *SignalsRequestedSetsFilter) (sql.Result, error)
	}

	// historyTransferTask is the SQL persistence interface for history nodes and history transfer tasks
	historyTransferTask interface {
		InsertIntoTransferTasks(rows []TransferTasksRow) (sql.Result, error)
		// SelectFromTransferTasks returns rows that match filter criteria from transfer_tasks table.
		// Required filter params - {shardID, minTaskID, maxTaskID}
		SelectFromTransferTasks(filter *TransferTasksFilter) ([]TransferTasksRow, error)
		// DeleteFromTransferTasks deletes one or more rows from transfer_tasks table.
		// Filter params - shardID is required. If TaskID is not nil, a single row is deleted.
		// When MinTaskID and MaxTaskID are not-nil, a range of rows are deleted.
		DeleteFromTransferTasks(filter *TransferTasksFilter) (sql.Result, error)
	}

	// historyTimerTask is the SQL persistence interface for history nodes and history timer tasks
	historyTimerTask interface {
		InsertIntoTimerTasks(rows []TimerTasksRow) (sql.Result, error)
		// SelectFromTimerTasks returns one or more rows from timer_tasks table
		// Required filter Params - {shardID, taskID, minVisibilityTimestamp, maxVisibilityTimestamp, pageSize}
		SelectFromTimerTasks(filter *TimerTasksFilter) ([]TimerTasksRow, error)
		// DeleteFromTimerTasks deletes one or more rows from timer_tasks table
		// Required filter Params:
		//  - to delete one row - {shardID, visibilityTimestamp, taskID}
		//  - to delete multiple rows - {shardID, minVisibilityTimestamp, maxVisibilityTimestamp}
		DeleteFromTimerTasks(filter *TimerTasksFilter) (sql.Result, error)
	}

	// historyReplicationTask is the SQL persistence interface for history nodes and history replication tasks
	historyReplicationTask interface {
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
	}
)
