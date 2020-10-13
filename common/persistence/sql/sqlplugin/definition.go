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

	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives"
)

type (

	// ClusterMetadataRow represents a row in the cluster_metadata table
	ClusterMetadataRow struct {
		Data         []byte
		DataEncoding string
		Version      int64
		// TODO(vitarb): immutable metadata is needed for backward compatibility only, remove after 1.1 release.
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
		ShardID      int32
		RangeID      int64
		Data         []byte
		DataEncoding string
	}

	// ShardsFilter contains the column names within shards table that
	// can be used to filter results through a WHERE clause
	ShardsFilter struct {
		ShardID int32
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
		ShardID      int32
		TaskID       int64
		Data         []byte
		DataEncoding string
	}

	// ReplicationTaskDLQRow represents a row in replication_tasks_dlq table
	ReplicationTaskDLQRow struct {
		SourceClusterName string
		ShardID           int32
		TaskID            int64
		Data              []byte
		DataEncoding      string
	}

	// ReplicationTasksFilter contains the column names within replication_tasks table that
	// can be used to filter results through a WHERE clause
	ReplicationTasksFilter struct {
		ShardID            int32
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
