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

package cassandra

import (
	"context"
	"time"

	"go.temporal.io/server/common/log"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
)

// Guidelines for creating new special UUID constants
// Each UUID should be of the form: E0000000-R000-f000-f000-00000000000x
// Where x is any hexadecimal value, E represents the entity type valid values are:
// E = {NamespaceID = 1, WorkflowID = 2, RunID = 3}
// R represents row type in executions table, valid values are:
// R = {Shard = 1, Execution = 2, Transfer = 3, Timer = 4, Replication = 5}
const (
	// Special Run IDs
	permanentRunID = "30000000-0000-f000-f000-000000000001"
	// Row Constants for Shard Row
	rowTypeShardNamespaceID = "10000000-1000-f000-f000-000000000000"
	rowTypeShardWorkflowID  = "20000000-1000-f000-f000-000000000000"
	rowTypeShardRunID       = "30000000-1000-f000-f000-000000000000"
	// Row Constants for Transfer Task Row
	rowTypeTransferNamespaceID = "10000000-3000-f000-f000-000000000000"
	rowTypeTransferWorkflowID  = "20000000-3000-f000-f000-000000000000"
	rowTypeTransferRunID       = "30000000-3000-f000-f000-000000000000"
	// Row Constants for Timer Task Row
	rowTypeTimerNamespaceID = "10000000-4000-f000-f000-000000000000"
	rowTypeTimerWorkflowID  = "20000000-4000-f000-f000-000000000000"
	rowTypeTimerRunID       = "30000000-4000-f000-f000-000000000000"
	// Row Constants for Replication Task Row
	rowTypeReplicationNamespaceID = "10000000-5000-f000-f000-000000000000"
	rowTypeReplicationWorkflowID  = "20000000-5000-f000-f000-000000000000"
	rowTypeReplicationRunID       = "30000000-5000-f000-f000-000000000000"
	// Row constants for visibility task row.
	rowTypeVisibilityTaskNamespaceID = "10000000-6000-f000-f000-000000000000"
	rowTypeVisibilityTaskWorkflowID  = "20000000-6000-f000-f000-000000000000"
	rowTypeVisibilityTaskRunID       = "30000000-6000-f000-f000-000000000000"
	// Row Constants for Replication Task DLQ Row. Source cluster name will be used as WorkflowID.
	rowTypeDLQNamespaceID = "10000000-6000-f000-f000-000000000000"
	rowTypeDLQRunID       = "30000000-6000-f000-f000-000000000000"
	// Row constants for History task row.
	rowTypeHistoryTaskNamespaceID = "10000000-8000-f000-f000-000000000000"
	rowTypeHistoryTaskWorkflowID  = "20000000-8000-f000-f000-000000000000"
	rowTypeHistoryTaskRunID       = "30000000-8000-f000-f000-000000000000"
	// Special TaskId constants
	rowTypeExecutionTaskID = int64(-10)
	rowTypeShardTaskID     = int64(-11)
)

const (
	// Row types for table executions
	rowTypeShard = iota
	rowTypeExecution
	rowTypeTransferTask
	rowTypeTimerTask
	rowTypeReplicationTask
	rowTypeDLQ
	rowTypeVisibilityTask
	// NOTE: the row type for history task is the task category ID
	// rowTypeHistoryTask
)

const (
	// Row types for table tasks
	rowTypeTask = iota
	rowTypeTaskQueue
)

const (
	taskQueueTaskID = -12345

	// ref: https://docs.datastax.com/en/dse-trblshoot/doc/troubleshooting/recoveringTtlYear2038Problem.html
	maxCassandraTTL = int64(315360000) // Cassandra max support time is 2038-01-19T03:14:06+00:00. Updated this to 10 years to support until year 2028
)

var (
	defaultDateTime            = time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)
	defaultVisibilityTimestamp = p.UnixMilliseconds(defaultDateTime)
)

type (
	ExecutionStore struct {
		*HistoryStore
		*MutableStateStore
		*MutableStateTaskStore
	}
)

var _ p.ExecutionStore = (*ExecutionStore)(nil)

func NewExecutionStore(
	session gocql.Session,
	logger log.Logger,
) *ExecutionStore {
	return &ExecutionStore{
		HistoryStore:          NewHistoryStore(session, logger),
		MutableStateStore:     NewMutableStateStore(session, logger),
		MutableStateTaskStore: NewMutableStateTaskStore(session, logger),
	}
}

func (d *ExecutionStore) CreateWorkflowExecution(
	ctx context.Context,
	request *p.InternalCreateWorkflowExecutionRequest,
) (*p.InternalCreateWorkflowExecutionResponse, error) {
	for _, req := range request.NewWorkflowNewEvents {
		if err := d.AppendHistoryNodes(ctx, req); err != nil {
			return nil, err
		}
	}

	return d.MutableStateStore.CreateWorkflowExecution(ctx, request)
}

func (d *ExecutionStore) UpdateWorkflowExecution(
	ctx context.Context,
	request *p.InternalUpdateWorkflowExecutionRequest,
) error {
	for _, req := range request.UpdateWorkflowNewEvents {
		if err := d.AppendHistoryNodes(ctx, req); err != nil {
			return err
		}
	}
	for _, req := range request.NewWorkflowNewEvents {
		if err := d.AppendHistoryNodes(ctx, req); err != nil {
			return err
		}
	}

	return d.MutableStateStore.UpdateWorkflowExecution(ctx, request)
}

func (d *ExecutionStore) ConflictResolveWorkflowExecution(
	ctx context.Context,
	request *p.InternalConflictResolveWorkflowExecutionRequest,
) error {
	for _, req := range request.CurrentWorkflowEventsNewEvents {
		if err := d.AppendHistoryNodes(ctx, req); err != nil {
			return err
		}
	}
	for _, req := range request.ResetWorkflowEventsNewEvents {
		if err := d.AppendHistoryNodes(ctx, req); err != nil {
			return err
		}
	}
	for _, req := range request.NewWorkflowEventsNewEvents {
		if err := d.AppendHistoryNodes(ctx, req); err != nil {
			return err
		}
	}

	return d.MutableStateStore.ConflictResolveWorkflowExecution(ctx, request)
}

func (d *ExecutionStore) GetName() string {
	return cassandraPersistenceName
}

func (d *ExecutionStore) Close() {
	if d.HistoryStore.Session != nil {
		d.HistoryStore.Session.Close()
	}
	if d.MutableStateStore.Session != nil {
		d.MutableStateStore.Session.Close()
	}
	if d.MutableStateTaskStore.Session != nil {
		d.MutableStateTaskStore.Session.Close()
	}
}
