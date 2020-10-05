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

import (
	"time"

	enumspb "go.temporal.io/api/enums/v1"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives"
)

type (

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
		ShardID      int32
		NamespaceID  primitives.UUID
		WorkflowID   string
		RunID        primitives.UUID
		Data         []byte
		DataEncoding string
	}

	// BufferedEventsFilter contains the column names within buffered_events table that
	// can be used to filter results through a WHERE clause
	BufferedEventsFilter struct {
		ShardID     int32
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
		ShardID      int32
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
		ShardID     int32
		NamespaceID primitives.UUID
		WorkflowID  string
		RunID       primitives.UUID
		ScheduleID  *int64
	}

	// TimerInfoMapsRow represents a row in timer_info_maps table
	TimerInfoMapsRow struct {
		ShardID      int32
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
		ShardID     int32
		NamespaceID primitives.UUID
		WorkflowID  string
		RunID       primitives.UUID
		TimerID     *string
	}

	// ChildExecutionInfoMapsRow represents a row in child_execution_info_maps table
	ChildExecutionInfoMapsRow struct {
		ShardID      int32
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
		ShardID     int32
		NamespaceID primitives.UUID
		WorkflowID  string
		RunID       primitives.UUID
		InitiatedID *int64
	}

	// RequestCancelInfoMapsRow represents a row in request_cancel_info_maps table
	RequestCancelInfoMapsRow struct {
		ShardID      int32
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
		ShardID     int32
		NamespaceID primitives.UUID
		WorkflowID  string
		RunID       primitives.UUID
		InitiatedID *int64
	}

	// SignalInfoMapsRow represents a row in signal_info_maps table
	SignalInfoMapsRow struct {
		ShardID      int32
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
		ShardID     int32
		NamespaceID primitives.UUID
		WorkflowID  string
		RunID       primitives.UUID
		InitiatedID *int64
	}

	// SignalsRequestedSetsRow represents a row in signals_requested_sets table
	SignalsRequestedSetsRow struct {
		ShardID     int32
		NamespaceID primitives.UUID
		WorkflowID  string
		RunID       primitives.UUID
		SignalID    string
	}

	// SignalsRequestedSetsFilter contains the column names within signals_requested_sets table that
	// can be used to filter results through a WHERE clause
	SignalsRequestedSetsFilter struct {
		ShardID     int32
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
)
