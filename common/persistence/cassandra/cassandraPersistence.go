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
	"fmt"
	"strings"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/log"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/common/primitives/timestamp"
)

//	"go.temporal.io/api/serviceerror"
// Guidelines for creating new special UUID constants
// Each UUID should be of the form: E0000000-R000-f000-f000-00000000000x
// Where x is any hexadecimal value, E represents the entity type valid values are:
// E = {NamespaceID = 1, WorkflowID = 2, RunID = 3}
// R represents row type in executions table, valid values are:
// R = {Shard = 1, Execution = 2, Transfer = 3, Timer = 4, Replication = 5}
const (
	// Special Namespaces related constants
	emptyNamespaceID = "10000000-0000-f000-f000-000000000000"
	// Special Run IDs
	emptyRunID     = "30000000-0000-f000-f000-000000000000"
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
	// Special TaskId constants
	rowTypeExecutionTaskID = int64(-10)
	rowTypeShardTaskID     = int64(-11)
	emptyInitiatedID       = int64(-7)

	stickyTaskQueueTTL = int32(24 * time.Hour / time.Second) // if sticky task_queue stopped being updated, remove it in one day
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
)

const (
	// Row types for table tasks
	rowTypeTask = iota
	rowTypeTaskQueue
)

const (
	taskQueueTaskID = -12345
	initialRangeID  = 1 // Id of the first range of a new task queue
)

const (
	templateCreateShardQuery = `INSERT INTO executions (` +
		`shard_id, type, namespace_id, workflow_id, run_id, visibility_ts, task_id, shard, shard_encoding, range_id)` +
		`VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?) IF NOT EXISTS`

	templateGetShardQuery = `SELECT shard, shard_encoding ` +
		`FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ?`

	templateUpdateShardQuery = `UPDATE executions ` +
		`SET shard = ?, shard_encoding = ?, range_id = ? ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? ` +
		`IF range_id = ?`

	templateUpdateCurrentWorkflowExecutionQuery = `UPDATE executions USING TTL 0 ` +
		`SET current_run_id = ?, execution_state = ?, execution_state_encoding = ?, workflow_last_write_version = ?, workflow_state = ? ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? ` +
		`IF current_run_id = ? `

	templateUpdateCurrentWorkflowExecutionForNewQuery = templateUpdateCurrentWorkflowExecutionQuery +
		`and workflow_last_write_version = ? ` +
		`and workflow_state = ? `

	templateCreateCurrentWorkflowExecutionQuery = `INSERT INTO executions (` +
		`shard_id, type, namespace_id, workflow_id, run_id, ` +
		`visibility_ts, task_id, current_run_id, execution_state, execution_state_encoding, ` +
		`workflow_last_write_version, workflow_state) ` +
		`VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) IF NOT EXISTS USING TTL 0 `

	templateCreateWorkflowExecutionQuery = `INSERT INTO executions (` +
		`shard_id, namespace_id, workflow_id, run_id, type, ` +
		`execution, execution_encoding, execution_state, execution_state_encoding, next_event_id, db_record_version, ` +
		`visibility_ts, task_id, checksum, checksum_encoding) ` +
		`VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) IF NOT EXISTS `

	templateCreateTransferTaskQuery = `INSERT INTO executions (` +
		`shard_id, type, namespace_id, workflow_id, run_id, transfer, transfer_encoding, visibility_ts, task_id) ` +
		`VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)`

	templateCreateReplicationTaskQuery = `INSERT INTO executions (` +
		`shard_id, type, namespace_id, workflow_id, run_id, replication, replication_encoding, visibility_ts, task_id) ` +
		`VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)`

	templateCreateVisibilityTaskQuery = `INSERT INTO executions (` +
		`shard_id, type, namespace_id, workflow_id, run_id, visibility_task_data, visibility_task_encoding, visibility_ts, task_id) ` +
		`VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)`

	templateCreateTimerTaskQuery = `INSERT INTO executions (` +
		`shard_id, type, namespace_id, workflow_id, run_id, timer, timer_encoding, visibility_ts, task_id) ` +
		`VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)`

	templateUpdateLeaseQuery = `UPDATE executions ` +
		`SET range_id = ? ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? ` +
		`IF range_id = ?`

	templateGetWorkflowExecutionQuery = `SELECT execution, execution_encoding, execution_state, execution_state_encoding, next_event_id, activity_map, activity_map_encoding, timer_map, timer_map_encoding, ` +
		`child_executions_map, child_executions_map_encoding, request_cancel_map, request_cancel_map_encoding, signal_map, signal_map_encoding, signal_requested, buffered_events_list, ` +
		`checksum, checksum_encoding, db_record_version ` +
		`FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ?`

	templateGetCurrentExecutionQuery = `SELECT current_run_id, execution, execution_encoding, execution_state, execution_state_encoding, workflow_last_write_version ` +
		`FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ?`

	templateListWorkflowExecutionQuery = `SELECT run_id, execution, execution_encoding, execution_state, execution_state_encoding, next_event_id ` +
		`FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ?`

	// TODO deprecate templateUpdateWorkflowExecutionQueryDeprecated in favor of templateUpdateWorkflowExecutionQuery
	templateUpdateWorkflowExecutionQueryDeprecated = `UPDATE executions ` +
		`SET execution = ? ` +
		`, execution_encoding = ? ` +
		`, execution_state = ? ` +
		`, execution_state_encoding = ? ` +
		`, next_event_id = ? ` +
		`, checksum = ? ` +
		`, checksum_encoding = ? ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? ` +
		`IF next_event_id = ? `
	templateUpdateWorkflowExecutionQuery = `UPDATE executions ` +
		`SET execution = ? ` +
		`, execution_encoding = ? ` +
		`, execution_state = ? ` +
		`, execution_state_encoding = ? ` +
		`, next_event_id = ? ` +
		`, db_record_version = ? ` +
		`, checksum = ? ` +
		`, checksum_encoding = ? ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? ` +
		`IF db_record_version = ? `

	templateUpdateActivityInfoQuery = `UPDATE executions ` +
		`SET activity_map[ ? ] = ?, activity_map_encoding = ? ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? `

	templateResetActivityInfoQuery = `UPDATE executions ` +
		`SET activity_map = ?, activity_map_encoding = ? ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? `

	templateUpdateTimerInfoQuery = `UPDATE executions ` +
		`SET timer_map[ ? ] = ?, timer_map_encoding = ? ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? `

	templateResetTimerInfoQuery = `UPDATE executions ` +
		`SET timer_map = ?, timer_map_encoding = ? ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? `

	templateUpdateChildExecutionInfoQuery = `UPDATE executions ` +
		`SET child_executions_map[ ? ] = ?, child_executions_map_encoding = ? ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? `

	templateResetChildExecutionInfoQuery = `UPDATE executions ` +
		`SET child_executions_map = ?, child_executions_map_encoding = ? ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? `

	templateUpdateRequestCancelInfoQuery = `UPDATE executions ` +
		`SET request_cancel_map[ ? ] = ?, request_cancel_map_encoding = ? ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? `

	templateResetRequestCancelInfoQuery = `UPDATE executions ` +
		`SET request_cancel_map = ?, request_cancel_map_encoding = ? ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? `

	templateUpdateSignalInfoQuery = `UPDATE executions ` +
		`SET signal_map[ ? ] = ?, signal_map_encoding = ? ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? `

	templateResetSignalInfoQuery = `UPDATE executions ` +
		`SET signal_map = ?, signal_map_encoding = ? ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? `

	templateUpdateSignalRequestedQuery = `UPDATE executions ` +
		`SET signal_requested = signal_requested + ? ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? `

	templateResetSignalRequestedQuery = `UPDATE executions ` +
		`SET signal_requested = ?` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? `

	templateAppendBufferedEventsQuery = `UPDATE executions ` +
		`SET buffered_events_list = buffered_events_list + ? ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? `

	templateDeleteBufferedEventsQuery = `UPDATE executions ` +
		`SET buffered_events_list = [] ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? `

	templateDeleteActivityInfoQuery = `DELETE activity_map[ ? ] ` +
		`FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? `

	templateDeleteTimerInfoQuery = `DELETE timer_map[ ? ] ` +
		`FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? `

	templateDeleteChildExecutionInfoQuery = `DELETE child_executions_map[ ? ] ` +
		`FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? `

	templateDeleteRequestCancelInfoQuery = `DELETE request_cancel_map[ ? ] ` +
		`FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? `

	templateDeleteSignalInfoQuery = `DELETE signal_map[ ? ] ` +
		`FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? `

	templateDeleteWorkflowExecutionMutableStateQuery = `DELETE FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? `

	templateDeleteWorkflowExecutionCurrentRowQuery = templateDeleteWorkflowExecutionMutableStateQuery + " if current_run_id = ? "

	templateDeleteWorkflowExecutionSignalRequestedQuery = `UPDATE executions ` +
		`SET signal_requested = signal_requested - ? ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? `

	templateGetTransferTaskQuery = `SELECT transfer, transfer_encoding ` +
		`FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? `

	templateGetTransferTasksQuery = `SELECT transfer, transfer_encoding ` +
		`FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id > ? ` +
		`and task_id <= ?`

	templateGetVisibilityTaskQuery = `SELECT visibility_task_data, visibility_task_encoding ` +
		`FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? `

	templateGetVisibilityTasksQuery = `SELECT visibility_task_data, visibility_task_encoding ` +
		`FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id > ? ` +
		`and task_id <= ?`

	templateGetReplicationTaskQuery = `SELECT replication, replication_encoding ` +
		`FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? `

	templateGetReplicationTasksQuery = `SELECT replication, replication_encoding ` +
		`FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id > ? ` +
		`and task_id <= ?`

	templateCompleteTransferTaskQuery = `DELETE FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ?`

	templateRangeCompleteTransferTaskQuery = `DELETE FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id > ? ` +
		`and task_id <= ?`

	templateCompleteVisibilityTaskQuery = `DELETE FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ?`

	templateRangeCompleteVisibilityTaskQuery = `DELETE FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id > ? ` +
		`and task_id <= ?`

	templateCompleteReplicationTaskBeforeQuery = `DELETE FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id <= ?`

	templateCompleteReplicationTaskQuery = templateCompleteTransferTaskQuery

	templateRangeCompleteReplicationTaskQuery = templateRangeCompleteTransferTaskQuery

	templateGetTimerTaskQuery = `SELECT timer, timer_encoding ` +
		`FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? `

	templateGetTimerTasksQuery = `SELECT timer, timer_encoding ` +
		`FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ?` +
		`and namespace_id = ? ` +
		`and workflow_id = ?` +
		`and run_id = ?` +
		`and visibility_ts >= ? ` +
		`and visibility_ts < ?`

	templateCompleteTimerTaskQuery = `DELETE FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ?` +
		`and run_id = ?` +
		`and visibility_ts = ? ` +
		`and task_id = ?`

	templateRangeCompleteTimerTaskQuery = `DELETE FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ?` +
		`and run_id = ?` +
		`and visibility_ts >= ? ` +
		`and visibility_ts < ?`

	templateCreateTaskQuery = `INSERT INTO tasks (` +
		`namespace_id, task_queue_name, task_queue_type, type, task_id, task, task_encoding) ` +
		`VALUES(?, ?, ?, ?, ?, ?, ?)`

	templateCreateTaskWithTTLQuery = `INSERT INTO tasks (` +
		`namespace_id, task_queue_name, task_queue_type, type, task_id, task, task_encoding) ` +
		`VALUES(?, ?, ?, ?, ?, ?, ?) USING TTL ?`

	templateGetTasksQuery = `SELECT task_id, task, task_encoding ` +
		`FROM tasks ` +
		`WHERE namespace_id = ? ` +
		`and task_queue_name = ? ` +
		`and task_queue_type = ? ` +
		`and type = ? ` +
		`and task_id > ? ` +
		`and task_id <= ?`

	templateCompleteTaskQuery = `DELETE FROM tasks ` +
		`WHERE namespace_id = ? ` +
		`and task_queue_name = ? ` +
		`and task_queue_type = ? ` +
		`and type = ? ` +
		`and task_id = ?`

	templateCompleteTasksLessThanQuery = `DELETE FROM tasks ` +
		`WHERE namespace_id = ? ` +
		`AND task_queue_name = ? ` +
		`AND task_queue_type = ? ` +
		`AND type = ? ` +
		`AND task_id <= ? `

	templateGetTaskQueue = `SELECT ` +
		`range_id, ` +
		`task_queue, ` +
		`task_queue_encoding ` +
		`FROM tasks ` +
		`WHERE namespace_id = ? ` +
		`and task_queue_name = ? ` +
		`and task_queue_type = ? ` +
		`and type = ? ` +
		`and task_id = ?`

	templateInsertTaskQueueQuery = `INSERT INTO tasks (` +
		`namespace_id, ` +
		`task_queue_name, ` +
		`task_queue_type, ` +
		`type, ` +
		`task_id, ` +
		`range_id, ` +
		`task_queue, ` +
		`task_queue_encoding ` +
		`) VALUES (?, ?, ?, ?, ?, ?, ?, ?) IF NOT EXISTS`

	templateUpdateTaskQueueQuery = `UPDATE tasks SET ` +
		`range_id = ?, ` +
		`task_queue = ?, ` +
		`task_queue_encoding = ? ` +
		`WHERE namespace_id = ? ` +
		`and task_queue_name = ? ` +
		`and task_queue_type = ? ` +
		`and type = ? ` +
		`and task_id = ? ` +
		`IF range_id = ?`

	templateUpdateTaskQueueQueryWithTTLPart1 = `INSERT INTO tasks (` +
		`namespace_id, ` +
		`task_queue_name, ` +
		`task_queue_type, ` +
		`type, ` +
		`task_id ` +
		`) VALUES (?, ?, ?, ?, ?) USING TTL ?`

	templateUpdateTaskQueueQueryWithTTLPart2 = `UPDATE tasks USING TTL ? SET ` +
		`range_id = ?, ` +
		`task_queue = ?, ` +
		`task_queue_encoding = ? ` +
		`WHERE namespace_id = ? ` +
		`and task_queue_name = ? ` +
		`and task_queue_type = ? ` +
		`and type = ? ` +
		`and task_id = ? ` +
		`IF range_id = ?`

	templateDeleteTaskQueueQuery = `DELETE FROM tasks ` +
		`WHERE namespace_id = ? ` +
		`AND task_queue_name = ? ` +
		`AND task_queue_type = ? ` +
		`AND type = ? ` +
		`AND task_id = ? ` +
		`IF range_id = ?`
)

var (
	defaultDateTime            = time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)
	defaultVisibilityTimestamp = p.UnixNanoToDBTimestamp(defaultDateTime.UnixNano())
)

type (
	cassandraStore struct {
		session gocql.Session
		logger  log.Logger
	}

	// Implements ExecutionManager, ShardManager and TaskManager
	cassandraPersistence struct {
		cassandraStore
		shardID            int32
		currentClusterName string
	}
)

var _ p.ExecutionStore = (*cassandraPersistence)(nil)

// newShardPersistence is used to create an instance of ShardManager implementation
func newShardPersistence(
	session gocql.Session,
	clusterName string,
	logger log.Logger,
) (p.ShardStore, error) {
	return &cassandraPersistence{
		cassandraStore:     cassandraStore{session: session, logger: logger},
		shardID:            -1,
		currentClusterName: clusterName,
	}, nil
}

// NewWorkflowExecutionPersistence is used to create an instance of workflowExecutionManager implementation
func NewWorkflowExecutionPersistence(
	shardID int32,
	session gocql.Session,
	logger log.Logger,
) (p.ExecutionStore, error) {
	return &cassandraPersistence{
		cassandraStore: cassandraStore{session: session, logger: logger},
		shardID:        shardID,
	}, nil
}

// newTaskPersistence is used to create an instance of TaskManager implementation
func newTaskPersistence(
	session gocql.Session,
	logger log.Logger,
) (p.TaskStore, error) {
	return &cassandraPersistence{
		cassandraStore: cassandraStore{session: session, logger: logger},
		shardID:        -1,
	}, nil
}

func (d *cassandraStore) GetName() string {
	return cassandraPersistenceName
}

// Close releases the underlying resources held by this object
func (d *cassandraStore) Close() {
	if d.session != nil {
		d.session.Close()
	}
}

func (d *cassandraPersistence) GetShardID() int32 {
	return d.shardID
}

func (d *cassandraPersistence) CreateShard(request *p.CreateShardRequest) error {
	shardInfo := request.ShardInfo
	shardInfo.UpdateTime = timestamp.TimeNowPtrUtc()
	data, err := serialization.ShardInfoToBlob(shardInfo)

	if err != nil {
		return gocql.ConvertError("CreateShard", err)
	}

	query := d.session.Query(templateCreateShardQuery,
		shardInfo.GetShardId(),
		rowTypeShard,
		rowTypeShardNamespaceID,
		rowTypeShardWorkflowID,
		rowTypeShardRunID,
		defaultVisibilityTimestamp,
		rowTypeShardTaskID,
		data.Data,
		data.EncodingType.String(),
		shardInfo.GetRangeId())

	previous := make(map[string]interface{})
	applied, err := query.MapScanCAS(previous)
	if err != nil {
		return gocql.ConvertError("CreateShard", err)
	}

	if !applied {
		data := previous["shard"].([]byte)
		encoding := previous["shard_encoding"].(string)
		shard, _ := serialization.ShardInfoFromBlob(data, encoding, d.currentClusterName)

		return &p.ShardAlreadyExistError{
			Msg: fmt.Sprintf("Shard already exists in executions table.  ShardId: %v, RangeId: %v",
				shard.GetShardId(), shard.GetRangeId()),
		}
	}

	return nil
}

func (d *cassandraPersistence) GetShard(request *p.GetShardRequest) (*p.GetShardResponse, error) {
	shardID := request.ShardID
	query := d.session.Query(templateGetShardQuery,
		shardID,
		rowTypeShard,
		rowTypeShardNamespaceID,
		rowTypeShardWorkflowID,
		rowTypeShardRunID,
		defaultVisibilityTimestamp,
		rowTypeShardTaskID)

	var data []byte
	var encoding string
	if err := query.Scan(&data, &encoding); err != nil {
		return nil, gocql.ConvertError("GetShard", err)
	}

	info, err := serialization.ShardInfoFromBlob(data, encoding, d.currentClusterName)

	if err != nil {
		return nil, gocql.ConvertError("GetShard", err)
	}

	return &p.GetShardResponse{ShardInfo: info}, nil
}

func (d *cassandraPersistence) UpdateShard(request *p.UpdateShardRequest) error {
	shardInfo := request.ShardInfo
	shardInfo.UpdateTime = timestamp.TimeNowPtrUtc()
	data, err := serialization.ShardInfoToBlob(shardInfo)

	if err != nil {
		return gocql.ConvertError("UpdateShard", err)
	}

	query := d.session.Query(templateUpdateShardQuery,
		data.Data,
		data.EncodingType.String(),
		shardInfo.GetRangeId(),
		shardInfo.GetShardId(), // Where
		rowTypeShard,
		rowTypeShardNamespaceID,
		rowTypeShardWorkflowID,
		rowTypeShardRunID,
		defaultVisibilityTimestamp,
		rowTypeShardTaskID,
		request.PreviousRangeID) // If

	previous := make(map[string]interface{})
	applied, err := query.MapScanCAS(previous)
	if err != nil {
		return gocql.ConvertError("UpdateShard", err)
	}

	if !applied {
		var columns []string
		for k, v := range previous {
			columns = append(columns, fmt.Sprintf("%s=%v", k, v))
		}

		return &p.ShardOwnershipLostError{
			ShardID: d.shardID,
			Msg: fmt.Sprintf("Failed to update shard.  previous_range_id: %v, columns: (%v)",
				request.PreviousRangeID, strings.Join(columns, ",")),
		}
	}

	return nil
}

func (d *cassandraPersistence) CreateWorkflowExecution(
	request *p.InternalCreateWorkflowExecutionRequest,
) (*p.CreateWorkflowExecutionResponse, error) {

	batch := d.session.NewBatch(gocql.LoggedBatch)

	newWorkflow := request.NewWorkflowSnapshot
	lastWriteVersion := newWorkflow.LastWriteVersion
	namespaceID := newWorkflow.ExecutionInfo.NamespaceId
	workflowID := newWorkflow.ExecutionInfo.WorkflowId
	runID := newWorkflow.ExecutionState.RunId

	if err := p.ValidateCreateWorkflowModeState(
		request.Mode,
		newWorkflow,
	); err != nil {
		return nil, err
	}

	switch request.Mode {
	case p.CreateWorkflowModeZombie:
		// noop

	default:
		if err := createOrUpdateCurrentExecution(batch,
			request.Mode,
			d.shardID,
			namespaceID,
			workflowID,
			runID,
			newWorkflow.ExecutionState.State,
			newWorkflow.ExecutionState.Status,
			newWorkflow.ExecutionState.CreateRequestId,
			lastWriteVersion,
			request.PreviousRunID,
			request.PreviousLastWriteVersion,
		); err != nil {
			return nil, err
		}
	}

	if err := applyWorkflowSnapshotBatchAsNew(batch,
		d.shardID,
		&newWorkflow,
	); err != nil {
		return nil, err
	}

	batch.Query(templateUpdateLeaseQuery,
		request.RangeID,
		d.shardID,
		rowTypeShard,
		rowTypeShardNamespaceID,
		rowTypeShardWorkflowID,
		rowTypeShardRunID,
		defaultVisibilityTimestamp,
		rowTypeShardTaskID,
		request.RangeID,
	)

	previous := make(map[string]interface{})
	applied, iter, err := d.session.MapExecuteBatchCAS(batch, previous)
	if err != nil {
		return nil, gocql.ConvertError("CreateWorkflowExecution", err)
	}
	defer func() {
		_ = iter.Close()
	}()

	if !applied {
		// There can be two reasons why the query does not get applied. Either the RangeID has changed, or
		// the workflow is already started. Check the row info returned by Cassandra to figure out which one it is.
	GetFailureReasonLoop:
		for {
			rowType, ok := previous["type"].(int)
			if !ok {
				// This should never happen, as all our rows have the type field.
				break GetFailureReasonLoop
			}
			runID := gocql.UUIDToString(previous["run_id"])

			if rowType == rowTypeShard {
				if rangeID, ok := previous["range_id"].(int64); ok && rangeID != request.RangeID {
					// CreateWorkflowExecution failed because rangeID was modified
					return nil, &p.ShardOwnershipLostError{
						ShardID: d.shardID,
						Msg: fmt.Sprintf("Failed to create workflow execution.  Request RangeID: %v, Actual RangeID: %v",
							request.RangeID, rangeID),
					}
				}

			} else if rowType == rowTypeExecution && runID == permanentRunID {
				var columns []string
				for k, v := range previous {
					columns = append(columns, fmt.Sprintf("%s=%v", k, v))
				}

				if state, ok := previous["execution_state"].([]byte); ok {
					stateEncoding, ok := previous["execution_state_encoding"].(string)
					if !ok {
						return nil, newPersistedTypeMismatchError("execution_state_encoding", "", stateEncoding, previous)
					}

					// todo: Move serialization to manager
					protoState, err := serialization.WorkflowExecutionStateFromBlob(state, stateEncoding)
					if err != nil {
						return nil, err
					}

					lastWriteVersion := previous["workflow_last_write_version"].(int64)

					msg := fmt.Sprintf("Workflow execution already running. WorkflowId: %v, RunId: %v, rangeID: %v, columns: (%v)",
						newWorkflow.ExecutionInfo.WorkflowId, protoState.RunId, request.RangeID, strings.Join(columns, ","))
					if request.Mode == p.CreateWorkflowModeBrandNew {
						// todo: Look at moving these errors upstream to manager
						return nil, &p.WorkflowExecutionAlreadyStartedError{
							Msg:              msg,
							StartRequestID:   protoState.CreateRequestId,
							RunID:            protoState.RunId,
							State:            protoState.State,
							Status:           protoState.Status,
							LastWriteVersion: lastWriteVersion,
						}
					}
					return nil, &p.CurrentWorkflowConditionFailedError{Msg: msg}
				}

				if prevRunID := gocql.UUIDToString(previous["current_run_id"]); prevRunID != request.PreviousRunID {
					// currentRunID on previous run has been changed, return to caller to handle
					msg := fmt.Sprintf("Workflow execution creation condition failed by mismatch runID. WorkflowId: %v, Expected Current RunId: %v, Actual Current RunId: %v",
						newWorkflow.ExecutionInfo.WorkflowId, request.PreviousRunID, prevRunID)
					return nil, &p.CurrentWorkflowConditionFailedError{Msg: msg}
				}

				msg := fmt.Sprintf("Workflow execution creation condition failed. WorkflowId: %v, CurrentRunId: %v, columns: (%v)",
					newWorkflow.ExecutionInfo.WorkflowId, newWorkflow.ExecutionState.RunId, strings.Join(columns, ","))
				return nil, &p.CurrentWorkflowConditionFailedError{Msg: msg}
			} else if rowType == rowTypeExecution && runID == newWorkflow.ExecutionState.RunId {
				msg := fmt.Sprintf("Workflow execution already running. WorkflowId: %v, RunId: %v, rangeId: %v",
					newWorkflow.ExecutionInfo.WorkflowId, newWorkflow.ExecutionState.RunId, request.RangeID)

				mutableState, err := mutableStateFromRow(previous)
				if err != nil {
					return nil, serviceerror.NewInternal(fmt.Sprintf("CreateWorkflowExecution operation error check failed. Error: %v", err))
				}
				lastWriteVersion := common.EmptyVersion
				if mutableState.ExecutionInfo.VersionHistories != nil {
					currentVersionHistory, err := versionhistory.GetCurrentVersionHistory(mutableState.ExecutionInfo.VersionHistories)
					if err != nil {
						return nil, serviceerror.NewInternal(fmt.Sprintf("CreateWorkflowExecution operation error check failed. Error: %v", err))
					}
					lastItem, err := versionhistory.GetLastVersionHistoryItem(currentVersionHistory)
					if err != nil {
						return nil, serviceerror.NewInternal(fmt.Sprintf("CreateWorkflowExecution operation error check failed. Error: %v", err))
					}
					lastWriteVersion = lastItem.GetVersion()
				}
				return nil, &p.WorkflowExecutionAlreadyStartedError{
					Msg:              msg,
					StartRequestID:   newWorkflow.ExecutionState.CreateRequestId,
					RunID:            newWorkflow.ExecutionState.RunId,
					State:            newWorkflow.ExecutionState.State,
					Status:           newWorkflow.ExecutionState.Status,
					LastWriteVersion: lastWriteVersion,
				}
			}

			previous = make(map[string]interface{})
			if !iter.MapScan(previous) {
				// Cassandra returns the actual row that caused a condition failure, so we should always return
				// from the checks above, but just in case.
				break GetFailureReasonLoop
			}
		}

		// At this point we only know that the write was not applied.
		// It's much safer to return ShardOwnershipLostError as the default to force the application to reload
		// shard to recover from such errors
		var columns []string
		for k, v := range previous {
			columns = append(columns, fmt.Sprintf("%s=%v", k, v))
		}
		return nil, &p.ShardOwnershipLostError{
			ShardID: d.shardID,
			Msg: fmt.Sprintf("Failed to create workflow execution.  Request RangeID: %v, columns: (%v)",
				request.RangeID, strings.Join(columns, ",")),
		}
	}

	return &p.CreateWorkflowExecutionResponse{}, nil
}

func (d *cassandraPersistence) GetWorkflowExecution(
	request *p.GetWorkflowExecutionRequest,
) (*p.InternalGetWorkflowExecutionResponse, error) {
	execution := request.Execution
	query := d.session.Query(templateGetWorkflowExecutionQuery,
		d.shardID,
		rowTypeExecution,
		request.NamespaceID,
		execution.WorkflowId,
		execution.GetRunId(),
		defaultVisibilityTimestamp,
		rowTypeExecutionTaskID)

	result := make(map[string]interface{})
	if err := query.MapScan(result); err != nil {
		return nil, gocql.ConvertError("GetWorkflowExecution", err)
	}

	state, err := mutableStateFromRow(result)
	if err != nil {
		return nil, serviceerror.NewInternal(fmt.Sprintf("GetWorkflowExecution operation failed. Error: %v", err))
	}

	activityInfos := make(map[int64]*persistencespb.ActivityInfo)
	aMap := result["activity_map"].(map[int64][]byte)
	aMapEncoding := result["activity_map_encoding"].(string)
	for key, value := range aMap {
		aInfo, err := serialization.ActivityInfoFromBlob(value, aMapEncoding)
		if err != nil {
			return nil, err
		}
		activityInfos[key] = aInfo
	}
	state.ActivityInfos = activityInfos

	timerInfos := make(map[string]*persistencespb.TimerInfo)
	tMapEncoding := result["timer_map_encoding"].(string)
	tMap := result["timer_map"].(map[string][]byte)
	for key, value := range tMap {
		info, err := serialization.TimerInfoFromBlob(value, tMapEncoding)
		if err != nil {
			return nil, err
		}
		timerInfos[key] = info
	}
	state.TimerInfos = timerInfos

	childExecutionInfos := make(map[int64]*persistencespb.ChildExecutionInfo)
	cMap := result["child_executions_map"].(map[int64][]byte)
	cMapEncoding := result["child_executions_map_encoding"].(string)
	for key, value := range cMap {
		cInfo, err := serialization.ChildExecutionInfoFromBlob(value, cMapEncoding)
		if err != nil {
			return nil, err
		}
		childExecutionInfos[key] = cInfo
	}
	state.ChildExecutionInfos = childExecutionInfos

	requestCancelInfos := make(map[int64]*persistencespb.RequestCancelInfo)
	rMapEncoding := result["request_cancel_map_encoding"].(string)
	rMap := result["request_cancel_map"].(map[int64][]byte)
	for key, value := range rMap {
		info, err := serialization.RequestCancelInfoFromBlob(value, rMapEncoding)
		if err != nil {
			return nil, err
		}
		requestCancelInfos[key] = info
	}
	state.RequestCancelInfos = requestCancelInfos

	signalInfos := make(map[int64]*persistencespb.SignalInfo)
	sMapEncoding := result["signal_map_encoding"].(string)
	sMap := result["signal_map"].(map[int64][]byte)
	for key, value := range sMap {
		info, err := serialization.SignalInfoFromBlob(value, sMapEncoding)
		if err != nil {
			return nil, err
		}
		signalInfos[key] = info
	}
	state.SignalInfos = signalInfos
	state.SignalRequestedIDs = gocql.UUIDsToStrings(result["signal_requested"])

	eList := result["buffered_events_list"].([]map[string]interface{})
	bufferedEventsBlobs := make([]*commonpb.DataBlob, 0, len(eList))
	for _, v := range eList {
		blob := createHistoryEventBatchBlob(v)
		bufferedEventsBlobs = append(bufferedEventsBlobs, blob)
	}
	state.BufferedEvents = bufferedEventsBlobs

	cs, err := serialization.ChecksumFromBlob(result["checksum"].([]byte), result["checksum_encoding"].(string))
	if err != nil {
		return nil, serviceerror.NewInternal(fmt.Sprintf("GetWorkflowExecution operation failed. Error: %v", err))
	}
	state.Checksum = cs

	dbVersion := int64(0)
	if dbRecordVersion, ok := result["db_record_version"]; ok {
		dbVersion = dbRecordVersion.(int64)
	} else {
		dbVersion = 0
	}

	return &p.InternalGetWorkflowExecutionResponse{
		State:           state,
		DBRecordVersion: dbVersion,
	}, nil
}

func protoExecutionStateFromRow(result map[string]interface{}) (*persistencespb.WorkflowExecutionState, error) {
	state, ok := result["execution_state"].([]byte)
	if !ok {
		return nil, newPersistedTypeMismatchError("execution_state", "", state, result)
	}

	stateEncoding, ok := result["execution_state_encoding"].(string)
	if !ok {
		return nil, newPersistedTypeMismatchError("execution_state_encoding", "", stateEncoding, result)
	}

	protoState, err := serialization.WorkflowExecutionStateFromBlob(state, stateEncoding)
	if err != nil {
		return nil, err
	}
	return protoState, nil
}

func (d *cassandraPersistence) UpdateWorkflowExecution(request *p.InternalUpdateWorkflowExecutionRequest) error {

	batch := d.session.NewBatch(gocql.LoggedBatch)

	updateWorkflow := request.UpdateWorkflowMutation
	newWorkflow := request.NewWorkflowSnapshot

	namespaceID := updateWorkflow.ExecutionInfo.NamespaceId
	workflowID := updateWorkflow.ExecutionInfo.WorkflowId
	runID := updateWorkflow.ExecutionState.RunId
	shardID := d.shardID

	if err := p.ValidateUpdateWorkflowModeState(
		request.Mode,
		updateWorkflow,
		newWorkflow,
	); err != nil {
		return err
	}

	switch request.Mode {
	case p.UpdateWorkflowModeBypassCurrent:
		if err := d.assertNotCurrentExecution(
			namespaceID,
			workflowID,
			runID); err != nil {
			return err
		}

	case p.UpdateWorkflowModeUpdateCurrent:
		if newWorkflow != nil {
			newLastWriteVersion := newWorkflow.LastWriteVersion
			newNamespaceID := newWorkflow.ExecutionInfo.NamespaceId
			newWorkflowID := newWorkflow.ExecutionInfo.WorkflowId
			newRunID := newWorkflow.ExecutionState.RunId

			if namespaceID != newNamespaceID {
				return serviceerror.NewInternal(fmt.Sprintf("UpdateWorkflowExecution: cannot continue as new to another namespace"))
			}

			if err := createOrUpdateCurrentExecution(batch,
				p.CreateWorkflowModeContinueAsNew,
				d.shardID,
				newNamespaceID,
				newWorkflowID,
				newRunID,
				newWorkflow.ExecutionState.State,
				newWorkflow.ExecutionState.Status,
				newWorkflow.ExecutionState.CreateRequestId,
				newLastWriteVersion,
				runID,
				0, // for continue as new, this is not used
			); err != nil {
				return err
			}

		} else {
			lastWriteVersion := updateWorkflow.LastWriteVersion

			executionStateDatablob, err := serialization.WorkflowExecutionStateToBlob(updateWorkflow.ExecutionState)
			if err != nil {
				return err
			}

			batch.Query(templateUpdateCurrentWorkflowExecutionQuery,
				runID,
				executionStateDatablob.Data,
				executionStateDatablob.EncodingType.String(),
				lastWriteVersion,
				updateWorkflow.ExecutionState.State,
				d.shardID,
				rowTypeExecution,
				namespaceID,
				workflowID,
				permanentRunID,
				defaultVisibilityTimestamp,
				rowTypeExecutionTaskID,
				runID,
			)
		}

	default:
		return serviceerror.NewInternal(fmt.Sprintf("UpdateWorkflowExecution: unknown mode: %v", request.Mode))
	}

	if err := applyWorkflowMutationBatch(batch, shardID, &updateWorkflow); err != nil {
		return err
	}
	if newWorkflow != nil {
		if err := applyWorkflowSnapshotBatchAsNew(batch,
			d.shardID,
			newWorkflow,
		); err != nil {
			return err
		}
	}

	// Verifies that the RangeID has not changed
	batch.Query(templateUpdateLeaseQuery,
		request.RangeID,
		d.shardID,
		rowTypeShard,
		rowTypeShardNamespaceID,
		rowTypeShardWorkflowID,
		rowTypeShardRunID,
		defaultVisibilityTimestamp,
		rowTypeShardTaskID,
		request.RangeID,
	)

	previous := make(map[string]interface{})
	applied, iter, err := d.session.MapExecuteBatchCAS(batch, previous)
	if err != nil {
		return gocql.ConvertError("UpdateWorkflowExecution", err)
	}
	defer func() {
		_ = iter.Close()
	}()

	if !applied {
		return d.getExecutionConditionalUpdateFailure(previous, iter, updateWorkflow.ExecutionState.RunId, updateWorkflow.Condition, request.RangeID, updateWorkflow.ExecutionState.RunId)
	}
	return nil
}

func (d *cassandraPersistence) ConflictResolveWorkflowExecution(request *p.InternalConflictResolveWorkflowExecutionRequest) error {
	batch := d.session.NewBatch(gocql.LoggedBatch)

	currentWorkflow := request.CurrentWorkflowMutation
	resetWorkflow := request.ResetWorkflowSnapshot
	newWorkflow := request.NewWorkflowSnapshot

	shardID := d.shardID

	namespaceID := resetWorkflow.ExecutionInfo.NamespaceId
	workflowID := resetWorkflow.ExecutionInfo.WorkflowId

	if err := p.ValidateConflictResolveWorkflowModeState(
		request.Mode,
		resetWorkflow,
		newWorkflow,
		currentWorkflow,
	); err != nil {
		return err
	}

	var prevRunID string

	switch request.Mode {
	case p.ConflictResolveWorkflowModeBypassCurrent:
		if err := d.assertNotCurrentExecution(
			namespaceID,
			workflowID,
			resetWorkflow.ExecutionState.RunId); err != nil {
			return err
		}

	case p.ConflictResolveWorkflowModeUpdateCurrent:
		executionState := resetWorkflow.ExecutionState
		lastWriteVersion := resetWorkflow.LastWriteVersion
		if newWorkflow != nil {
			lastWriteVersion = newWorkflow.LastWriteVersion
			executionState = newWorkflow.ExecutionState
		}
		runID := executionState.RunId
		createRequestID := executionState.CreateRequestId
		state := executionState.State
		status := executionState.Status

		executionStateDatablob, err := serialization.WorkflowExecutionStateToBlob(&persistencespb.WorkflowExecutionState{
			RunId:           runID,
			CreateRequestId: createRequestID,
			State:           state,
			Status:          status,
		})
		if err != nil {
			return serviceerror.NewInternal(fmt.Sprintf("ConflictResolveWorkflowExecution operation failed. Error: %v", err))
		}

		if currentWorkflow != nil {
			prevRunID = currentWorkflow.ExecutionState.RunId

			batch.Query(templateUpdateCurrentWorkflowExecutionQuery,
				runID,
				executionStateDatablob.Data,
				executionStateDatablob.EncodingType.String(),
				lastWriteVersion,
				state,
				shardID,
				rowTypeExecution,
				namespaceID,
				workflowID,
				permanentRunID,
				defaultVisibilityTimestamp,
				rowTypeExecutionTaskID,
				prevRunID,
			)
		} else {
			// reset workflow is current
			prevRunID = resetWorkflow.ExecutionState.RunId

			batch.Query(templateUpdateCurrentWorkflowExecutionQuery,
				runID,
				executionStateDatablob.Data,
				executionStateDatablob.EncodingType.String(),
				lastWriteVersion,
				state,
				shardID,
				rowTypeExecution,
				namespaceID,
				workflowID,
				permanentRunID,
				defaultVisibilityTimestamp,
				rowTypeExecutionTaskID,
				prevRunID,
			)
		}

	default:
		return serviceerror.NewInternal(fmt.Sprintf("ConflictResolveWorkflowExecution: unknown mode: %v", request.Mode))
	}

	if err := applyWorkflowSnapshotBatchAsReset(batch, shardID, &resetWorkflow); err != nil {
		return err
	}

	if currentWorkflow != nil {
		if err := applyWorkflowMutationBatch(batch, shardID, currentWorkflow); err != nil {
			return err
		}
	}
	if newWorkflow != nil {
		if err := applyWorkflowSnapshotBatchAsNew(batch, shardID, newWorkflow); err != nil {
			return err
		}
	}

	// Verifies that the RangeID has not changed
	batch.Query(templateUpdateLeaseQuery,
		request.RangeID,
		d.shardID,
		rowTypeShard,
		rowTypeShardNamespaceID,
		rowTypeShardWorkflowID,
		rowTypeShardRunID,
		defaultVisibilityTimestamp,
		rowTypeShardTaskID,
		request.RangeID,
	)

	previous := make(map[string]interface{})
	applied, iter, err := d.session.MapExecuteBatchCAS(batch, previous)
	if err != nil {
		return gocql.ConvertError("ConflictResolveWorkflowExecution", err)
	}
	defer func() {
		_ = iter.Close()
	}()

	if !applied {
		return d.getExecutionConditionalUpdateFailure(previous, iter, resetWorkflow.ExecutionState.RunId, request.ResetWorkflowSnapshot.Condition, request.RangeID, prevRunID)
	}
	return nil
}

func (d *cassandraPersistence) getExecutionConditionalUpdateFailure(previous map[string]interface{}, iter gocql.Iter, requestRunID string, requestCondition int64, requestRangeID int64, requestConditionalRunID string) error {
	// There can be three reasons why the query does not get applied: the RangeID has changed, or the next_event_id or current_run_id check failed.
	// Check the row info returned by Cassandra to figure out which one it is.
	rangeIDUnmatch := false
	actualRangeID := int64(0)
	nextEventIDUnmatch := false
	actualNextEventID := int64(0)
	runIDUnmatch := false
	actualCurrRunID := ""
	allPrevious := []map[string]interface{}{}

GetFailureReasonLoop:
	for {
		rowType, ok := previous["type"].(int)
		if !ok {
			// This should never happen, as all our rows have the type field.
			break GetFailureReasonLoop
		}

		runID := gocql.UUIDToString(previous["run_id"])

		if rowType == rowTypeShard {
			if actualRangeID, ok = previous["range_id"].(int64); ok && actualRangeID != requestRangeID {
				// UpdateWorkflowExecution failed because rangeID was modified
				rangeIDUnmatch = true
			}
		} else if rowType == rowTypeExecution && runID == requestRunID {
			if actualNextEventID, ok = previous["next_event_id"].(int64); ok && actualNextEventID != requestCondition {
				// UpdateWorkflowExecution failed because next event ID is unexpected
				nextEventIDUnmatch = true
			}
		} else if rowType == rowTypeExecution && runID == permanentRunID {
			// UpdateWorkflowExecution failed because current_run_id is unexpected
			if actualCurrRunID = gocql.UUIDToString(previous["current_run_id"]); actualCurrRunID != requestConditionalRunID {
				// UpdateWorkflowExecution failed because next event ID is unexpected
				runIDUnmatch = true
			}
		}

		allPrevious = append(allPrevious, previous)
		previous = make(map[string]interface{})
		if !iter.MapScan(previous) {
			// Cassandra returns the actual row that caused a condition failure, so we should always return
			// from the checks above, but just in case.
			break GetFailureReasonLoop
		}
	}

	if rangeIDUnmatch {
		return &p.ShardOwnershipLostError{
			ShardID: d.shardID,
			Msg: fmt.Sprintf("Failed to update mutable state.  Request RangeID: %v, Actual RangeID: %v",
				requestRangeID, actualRangeID),
		}
	}

	if runIDUnmatch {
		return &p.CurrentWorkflowConditionFailedError{
			Msg: fmt.Sprintf("Failed to update mutable state.  Request Condition: %v, Actual Value: %v, Request Current RunId: %v, Actual Value: %v",
				requestCondition, actualNextEventID, requestConditionalRunID, actualCurrRunID),
		}
	}

	if nextEventIDUnmatch {
		return &p.ConditionFailedError{
			Msg: fmt.Sprintf("Failed to update mutable state.  Request Condition: %v, Actual Value: %v, Request Current RunId: %v, Actual Value: %v",
				requestCondition, actualNextEventID, requestConditionalRunID, actualCurrRunID),
		}
	}

	// At this point we only know that the write was not applied.
	var columns []string
	columnID := 0
	for _, previous := range allPrevious {
		for k, v := range previous {
			columns = append(columns, fmt.Sprintf("%v: %s=%v", columnID, k, v))
		}
		columnID++
	}
	return &p.ConditionFailedError{
		Msg: fmt.Sprintf("Failed to reset mutable state. ShardId: %v, RangeId: %v, Condition: %v, Request Current RunId: %v, columns: (%v)",
			d.shardID, requestRangeID, requestCondition, requestConditionalRunID, strings.Join(columns, ",")),
	}
}

func (d *cassandraPersistence) assertNotCurrentExecution(
	namespaceID string,
	workflowID string,
	runID string,
) error {

	if resp, err := d.GetCurrentExecution(&p.GetCurrentExecutionRequest{
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
	}); err != nil {
		if _, ok := err.(*serviceerror.NotFound); ok {
			// allow bypassing no current record
			return nil
		}
		return err
	} else if resp.RunID == runID {
		return &p.ConditionFailedError{
			Msg: fmt.Sprintf("Assertion on current record failed. Current run ID is not expected: %v", resp.RunID),
		}
	}

	return nil
}

func (d *cassandraPersistence) DeleteWorkflowExecution(request *p.DeleteWorkflowExecutionRequest) error {
	query := d.session.Query(templateDeleteWorkflowExecutionMutableStateQuery,
		d.shardID,
		rowTypeExecution,
		request.NamespaceID,
		request.WorkflowID,
		request.RunID,
		defaultVisibilityTimestamp,
		rowTypeExecutionTaskID)

	err := query.Exec()
	return gocql.ConvertError("DeleteWorkflowExecution", err)
}

func (d *cassandraPersistence) DeleteCurrentWorkflowExecution(request *p.DeleteCurrentWorkflowExecutionRequest) error {
	query := d.session.Query(templateDeleteWorkflowExecutionCurrentRowQuery,
		d.shardID,
		rowTypeExecution,
		request.NamespaceID,
		request.WorkflowID,
		permanentRunID,
		defaultVisibilityTimestamp,
		rowTypeExecutionTaskID,
		request.RunID)

	err := query.Exec()
	return gocql.ConvertError("DeleteWorkflowCurrentRow", err)
}

func (d *cassandraPersistence) GetCurrentExecution(request *p.GetCurrentExecutionRequest) (*p.GetCurrentExecutionResponse,
	error) {
	query := d.session.Query(templateGetCurrentExecutionQuery,
		d.shardID,
		rowTypeExecution,
		request.NamespaceID,
		request.WorkflowID,
		permanentRunID,
		defaultVisibilityTimestamp,
		rowTypeExecutionTaskID)

	result := make(map[string]interface{})
	if err := query.MapScan(result); err != nil {
		return nil, gocql.ConvertError("GetCurrentExecution", err)
	}

	currentRunID := gocql.UUIDToString(result["current_run_id"])
	lastWriteVersion := result["workflow_last_write_version"].(int64)
	executionState, err := protoExecutionStateFromRow(result)
	if err != nil {
		return nil, serviceerror.NewInternal(fmt.Sprintf("GetCurrentExecution operation failed. Error: %v", err))
	}

	return &p.GetCurrentExecutionResponse{
		RunID:            currentRunID,
		StartRequestID:   executionState.CreateRequestId,
		State:            executionState.State,
		Status:           executionState.Status,
		LastWriteVersion: lastWriteVersion,
	}, nil
}

func (d *cassandraPersistence) ListConcreteExecutions(
	request *p.ListConcreteExecutionsRequest,
) (*p.InternalListConcreteExecutionsResponse, error) {
	query := d.session.Query(
		templateListWorkflowExecutionQuery,
		d.shardID,
		rowTypeExecution,
	).PageSize(request.PageSize).PageState(request.PageToken)

	iter := query.Iter()
	if iter == nil {
		return nil, serviceerror.NewInternal("ListConcreteExecutions operation failed.  Not able to create query iterator.")
	}

	response := &p.InternalListConcreteExecutionsResponse{}
	result := make(map[string]interface{})
	for iter.MapScan(result) {
		runID := gocql.UUIDToString(result["run_id"])
		if runID == permanentRunID {
			result = make(map[string]interface{})
			continue
		}
		if _, ok := result["execution"]; ok {
			state, err := mutableStateFromRow(result)
			if err != nil {
				return nil, err
			}
			response.States = append(response.States, state)
		}
		result = make(map[string]interface{})
	}
	nextPageToken := iter.PageState()
	response.NextPageToken = make([]byte, len(nextPageToken))
	copy(response.NextPageToken, nextPageToken)
	return response, nil
}

func (d *cassandraPersistence) AddTasks(request *p.AddTasksRequest) error {
	batch := d.session.NewBatch(gocql.LoggedBatch)

	if err := applyTasks(
		batch,
		d.shardID,
		request.NamespaceID,
		request.WorkflowID,
		request.RunID,
		request.TransferTasks,
		request.TimerTasks,
		request.ReplicationTasks,
		request.VisibilityTasks,
	); err != nil {
		return err
	}

	batch.Query(templateUpdateLeaseQuery,
		request.RangeID,
		d.shardID,
		rowTypeShard,
		rowTypeShardNamespaceID,
		rowTypeShardWorkflowID,
		rowTypeShardRunID,
		defaultVisibilityTimestamp,
		rowTypeShardTaskID,
		request.RangeID,
	)

	previous := make(map[string]interface{})
	applied, iter, err := d.session.MapExecuteBatchCAS(batch, previous)
	if err != nil {
		return gocql.ConvertError("AddTasks", err)
	}
	defer func() {
		_ = iter.Close()
	}()

	if !applied {
		if previousRangeID, ok := previous["range_id"].(int64); ok && previousRangeID != request.RangeID {
			// CreateWorkflowExecution failed because rangeID was modified
			return &p.ShardOwnershipLostError{
				ShardID: d.shardID,
				Msg:     fmt.Sprintf("Failed to add tasks.  Request RangeID: %v, Actual RangeID: %v", request.RangeID, previousRangeID),
			}
		} else {
			return serviceerror.NewInternal("AddTasks operation failed: %v")
		}
	}
	return nil
}

func (d *cassandraPersistence) GetTransferTask(request *p.GetTransferTaskRequest) (*p.GetTransferTaskResponse, error) {
	shardID := d.shardID
	taskID := request.TaskID
	query := d.session.Query(templateGetTransferTaskQuery,
		shardID,
		rowTypeTransferTask,
		rowTypeTransferNamespaceID,
		rowTypeTransferWorkflowID,
		rowTypeTransferRunID,
		defaultVisibilityTimestamp,
		taskID)

	var data []byte
	var encoding string
	if err := query.Scan(&data, &encoding); err != nil {
		return nil, gocql.ConvertError("GetTransferTask", err)
	}

	info, err := serialization.TransferTaskInfoFromBlob(data, encoding)

	if err != nil {
		return nil, gocql.ConvertError("GetTransferTask", err)
	}

	return &p.GetTransferTaskResponse{TransferTaskInfo: info}, nil
}

func (d *cassandraPersistence) GetTransferTasks(request *p.GetTransferTasksRequest) (*p.GetTransferTasksResponse, error) {

	// Reading transfer tasks need to be quorum level consistent, otherwise we could lose task
	query := d.session.Query(templateGetTransferTasksQuery,
		d.shardID,
		rowTypeTransferTask,
		rowTypeTransferNamespaceID,
		rowTypeTransferWorkflowID,
		rowTypeTransferRunID,
		defaultVisibilityTimestamp,
		request.ReadLevel,
		request.MaxReadLevel,
	).PageSize(request.BatchSize).PageState(request.NextPageToken)

	iter := query.Iter()
	if iter == nil {
		return nil, serviceerror.NewInternal("GetTransferTasks operation failed.  Not able to create query iterator.")
	}

	response := &p.GetTransferTasksResponse{}
	var data []byte
	var encoding string

	for iter.Scan(&data, &encoding) {
		t, err := serialization.TransferTaskInfoFromBlob(data, encoding)
		if err != nil {
			return nil, gocql.ConvertError("GetTransferTasks", err)
		}

		response.Tasks = append(response.Tasks, t)
	}
	nextPageToken := iter.PageState()
	response.NextPageToken = make([]byte, len(nextPageToken))
	copy(response.NextPageToken, nextPageToken)

	if err := iter.Close(); err != nil {
		return nil, gocql.ConvertError("GetTransferTasks", err)
	}

	return response, nil
}

func (d *cassandraPersistence) GetVisibilityTask(request *p.GetVisibilityTaskRequest) (*p.GetVisibilityTaskResponse, error) {
	shardID := d.shardID
	taskID := request.TaskID
	query := d.session.Query(templateGetVisibilityTaskQuery,
		shardID,
		rowTypeVisibilityTask,
		rowTypeVisibilityTaskNamespaceID,
		rowTypeVisibilityTaskWorkflowID,
		rowTypeVisibilityTaskRunID,
		defaultVisibilityTimestamp,
		taskID)

	var data []byte
	var encoding string
	if err := query.Scan(&data, &encoding); err != nil {
		return nil, gocql.ConvertError("GetVisibilityTask", err)
	}

	info, err := serialization.VisibilityTaskInfoFromBlob(data, encoding)

	if err != nil {
		return nil, gocql.ConvertError("GetVisibilityTask", err)
	}

	return &p.GetVisibilityTaskResponse{VisibilityTaskInfo: info}, nil
}

func (d *cassandraPersistence) GetVisibilityTasks(request *p.GetVisibilityTasksRequest) (*p.GetVisibilityTasksResponse, error) {

	// Reading Visibility tasks need to be quorum level consistent, otherwise we could lose task
	query := d.session.Query(templateGetVisibilityTasksQuery,
		d.shardID,
		rowTypeVisibilityTask,
		rowTypeVisibilityTaskNamespaceID,
		rowTypeVisibilityTaskWorkflowID,
		rowTypeVisibilityTaskRunID,
		defaultVisibilityTimestamp,
		request.ReadLevel,
		request.MaxReadLevel,
	).PageSize(request.BatchSize).PageState(request.NextPageToken)

	iter := query.Iter()
	if iter == nil {
		return nil, serviceerror.NewInternal("GetVisibilityTasks operation failed.  Not able to create query iterator.")
	}

	response := &p.GetVisibilityTasksResponse{}
	var data []byte
	var encoding string

	for iter.Scan(&data, &encoding) {
		t, err := serialization.VisibilityTaskInfoFromBlob(data, encoding)
		if err != nil {
			return nil, gocql.ConvertError("GetVisibilityTasks", err)
		}

		response.Tasks = append(response.Tasks, t)
	}
	nextPageToken := iter.PageState()
	response.NextPageToken = make([]byte, len(nextPageToken))
	copy(response.NextPageToken, nextPageToken)

	if err := iter.Close(); err != nil {
		return nil, gocql.ConvertError("GetVisibilityTasks", err)
	}

	return response, nil
}

func (d *cassandraPersistence) GetReplicationTask(request *p.GetReplicationTaskRequest) (*p.GetReplicationTaskResponse, error) {
	shardID := d.shardID
	taskID := request.TaskID
	query := d.session.Query(templateGetReplicationTaskQuery,
		shardID,
		rowTypeReplicationTask,
		rowTypeReplicationNamespaceID,
		rowTypeReplicationWorkflowID,
		rowTypeReplicationRunID,
		defaultVisibilityTimestamp,
		taskID)

	var data []byte
	var encoding string
	if err := query.Scan(&data, &encoding); err != nil {
		return nil, gocql.ConvertError("GetReplicationTask", err)
	}

	info, err := serialization.ReplicationTaskInfoFromBlob(data, encoding)

	if err != nil {
		return nil, gocql.ConvertError("GetReplicationTask", err)
	}

	return &p.GetReplicationTaskResponse{ReplicationTaskInfo: info}, nil
}

func (d *cassandraPersistence) GetReplicationTasks(
	request *p.GetReplicationTasksRequest,
) (*p.GetReplicationTasksResponse, error) {

	// Reading replication tasks need to be quorum level consistent, otherwise we could lose task
	query := d.session.Query(templateGetReplicationTasksQuery,
		d.shardID,
		rowTypeReplicationTask,
		rowTypeReplicationNamespaceID,
		rowTypeReplicationWorkflowID,
		rowTypeReplicationRunID,
		defaultVisibilityTimestamp,
		request.MinTaskID,
		request.MaxTaskID,
	).PageSize(request.BatchSize).PageState(request.NextPageToken)

	return d.populateGetReplicationTasksResponse(query, "GetReplicationTasks")
}

func (d *cassandraPersistence) populateGetReplicationTasksResponse(
	query gocql.Query, operation string,
) (*p.GetReplicationTasksResponse, error) {
	iter := query.Iter()
	if iter == nil {
		return nil, serviceerror.NewInternal("GetReplicationTasks operation failed.  Not able to create query iterator.")
	}

	response := &p.GetReplicationTasksResponse{}
	var data []byte
	var encoding string

	for iter.Scan(&data, &encoding) {
		t, err := serialization.ReplicationTaskInfoFromBlob(data, encoding)

		if err != nil {
			return nil, gocql.ConvertError(operation, err)
		}

		response.Tasks = append(response.Tasks, t)
	}
	nextPageToken := iter.PageState()
	response.NextPageToken = make([]byte, len(nextPageToken))
	copy(response.NextPageToken, nextPageToken)

	if err := iter.Close(); err != nil {
		return nil, gocql.ConvertError(operation, err)
	}

	return response, nil
}

func (d *cassandraPersistence) CompleteTransferTask(request *p.CompleteTransferTaskRequest) error {
	query := d.session.Query(templateCompleteTransferTaskQuery,
		d.shardID,
		rowTypeTransferTask,
		rowTypeTransferNamespaceID,
		rowTypeTransferWorkflowID,
		rowTypeTransferRunID,
		defaultVisibilityTimestamp,
		request.TaskID)

	err := query.Exec()
	return gocql.ConvertError("CompleteTransferTask", err)
}

func (d *cassandraPersistence) RangeCompleteTransferTask(request *p.RangeCompleteTransferTaskRequest) error {
	query := d.session.Query(templateRangeCompleteTransferTaskQuery,
		d.shardID,
		rowTypeTransferTask,
		rowTypeTransferNamespaceID,
		rowTypeTransferWorkflowID,
		rowTypeTransferRunID,
		defaultVisibilityTimestamp,
		request.ExclusiveBeginTaskID,
		request.InclusiveEndTaskID,
	)

	err := query.Exec()
	return gocql.ConvertError("RangeCompleteTransferTask", err)
}

func (d *cassandraPersistence) CompleteVisibilityTask(request *p.CompleteVisibilityTaskRequest) error {
	query := d.session.Query(templateCompleteVisibilityTaskQuery,
		d.shardID,
		rowTypeVisibilityTask,
		rowTypeVisibilityTaskNamespaceID,
		rowTypeVisibilityTaskWorkflowID,
		rowTypeVisibilityTaskRunID,
		defaultVisibilityTimestamp,
		request.TaskID)

	err := query.Exec()
	return gocql.ConvertError("CompleteVisibilityTask", err)
}

func (d *cassandraPersistence) RangeCompleteVisibilityTask(request *p.RangeCompleteVisibilityTaskRequest) error {
	query := d.session.Query(templateRangeCompleteVisibilityTaskQuery,
		d.shardID,
		rowTypeVisibilityTask,
		rowTypeVisibilityTaskNamespaceID,
		rowTypeVisibilityTaskWorkflowID,
		rowTypeVisibilityTaskRunID,
		defaultVisibilityTimestamp,
		request.ExclusiveBeginTaskID,
		request.InclusiveEndTaskID,
	)

	err := query.Exec()
	return gocql.ConvertError("RangeCompleteVisibilityTask", err)
}

func (d *cassandraPersistence) CompleteReplicationTask(request *p.CompleteReplicationTaskRequest) error {
	query := d.session.Query(templateCompleteReplicationTaskQuery,
		d.shardID,
		rowTypeReplicationTask,
		rowTypeReplicationNamespaceID,
		rowTypeReplicationWorkflowID,
		rowTypeReplicationRunID,
		defaultVisibilityTimestamp,
		request.TaskID)

	err := query.Exec()
	return gocql.ConvertError("CompleteReplicationTask", err)
}

func (d *cassandraPersistence) RangeCompleteReplicationTask(
	request *p.RangeCompleteReplicationTaskRequest,
) error {

	query := d.session.Query(templateCompleteReplicationTaskBeforeQuery,
		d.shardID,
		rowTypeReplicationTask,
		rowTypeReplicationNamespaceID,
		rowTypeReplicationWorkflowID,
		rowTypeReplicationRunID,
		defaultVisibilityTimestamp,
		request.InclusiveEndTaskID,
	)

	err := query.Exec()
	return gocql.ConvertError("RangeCompleteReplicationTask", err)
}

func (d *cassandraPersistence) CompleteTimerTask(request *p.CompleteTimerTaskRequest) error {
	ts := p.UnixNanoToDBTimestamp(request.VisibilityTimestamp.UnixNano())
	query := d.session.Query(templateCompleteTimerTaskQuery,
		d.shardID,
		rowTypeTimerTask,
		rowTypeTimerNamespaceID,
		rowTypeTimerWorkflowID,
		rowTypeTimerRunID,
		ts,
		request.TaskID)

	err := query.Exec()
	return gocql.ConvertError("CompleteTimerTask", err)
}

func (d *cassandraPersistence) RangeCompleteTimerTask(request *p.RangeCompleteTimerTaskRequest) error {
	start := p.UnixNanoToDBTimestamp(request.InclusiveBeginTimestamp.UnixNano())
	end := p.UnixNanoToDBTimestamp(request.ExclusiveEndTimestamp.UnixNano())
	query := d.session.Query(templateRangeCompleteTimerTaskQuery,
		d.shardID,
		rowTypeTimerTask,
		rowTypeTimerNamespaceID,
		rowTypeTimerWorkflowID,
		rowTypeTimerRunID,
		start,
		end,
	)

	err := query.Exec()
	return gocql.ConvertError("RangeCompleteTimerTask", err)
}

// From TaskManager interface
func (d *cassandraPersistence) LeaseTaskQueue(request *p.LeaseTaskQueueRequest) (*p.LeaseTaskQueueResponse, error) {
	if len(request.TaskQueue) == 0 {
		return nil, serviceerror.NewInternal(fmt.Sprintf("LeaseTaskQueue requires non empty task queue"))
	}
	now := timestamp.TimeNowPtrUtc()
	query := d.session.Query(templateGetTaskQueue,
		request.NamespaceID,
		request.TaskQueue,
		request.TaskType,
		rowTypeTaskQueue,
		taskQueueTaskID,
	)
	var rangeID int64
	var tlBytes []byte
	var tlEncoding string
	err := query.Scan(&rangeID, &tlBytes, &tlEncoding)
	var tl *p.PersistedTaskQueueInfo
	if err != nil {
		if gocql.IsNotFoundError(err) { // First time task queue is used
			tl = &p.PersistedTaskQueueInfo{
				Data: &persistencespb.TaskQueueInfo{
					NamespaceId:    request.NamespaceID,
					Name:           request.TaskQueue,
					TaskType:       request.TaskType,
					Kind:           request.TaskQueueKind,
					AckLevel:       0,
					ExpiryTime:     nil,
					LastUpdateTime: now,
				},
				RangeID: initialRangeID,
			}
			datablob, err := serialization.TaskQueueInfoToBlob(tl.Data)

			if err != nil {
				return nil, serviceerror.NewInternal(fmt.Sprintf("LeaseTaskQueue operation failed during serialization. TaskQueue: %v, TaskType: %v, Error: %v", request.TaskQueue, request.TaskType, err))
			}

			query = d.session.Query(templateInsertTaskQueueQuery,
				request.NamespaceID,
				request.TaskQueue,
				request.TaskType,
				rowTypeTaskQueue,
				taskQueueTaskID,
				initialRangeID,
				datablob.Data,
				datablob.EncodingType.String(),
			)
		} else if gocql.IsThrottlingError(err) {
			return nil, serviceerror.NewResourceExhausted(fmt.Sprintf("LeaseTaskQueue operation failed. TaskQueue: %v, TaskType: %v, Error: %v", request.TaskQueue, request.TaskType, err))
		} else {
			return nil, serviceerror.NewInternal(fmt.Sprintf("LeaseTaskQueue operation failed. TaskQueue: %v, TaskType: %v, Error: %v", request.TaskQueue, request.TaskType, err))
		}
	} else {
		// if request.RangeID is > 0, we are trying to renew an already existing
		// lease on the task queue. If request.RangeID=0, we are trying to steal
		// the taskqueue from its current owner
		if request.RangeID > 0 && request.RangeID != rangeID {
			return nil, &p.ConditionFailedError{
				Msg: fmt.Sprintf("leaseTaskQueue:renew failed: taskQueue:%v, taskQueueType:%v, haveRangeID:%v, gotRangeID:%v",
					request.TaskQueue, request.TaskType, request.RangeID, rangeID),
			}
		}

		tli, err := serialization.TaskQueueInfoFromBlob(tlBytes, tlEncoding)
		if err != nil {
			return nil, serviceerror.NewInternal(fmt.Sprintf("LeaseTaskQueue operation failed during serialization. TaskQueue: %v, TaskType: %v, Error: %v", request.TaskQueue, request.TaskType, err))
		}

		tli.LastUpdateTime = now
		tl = &p.PersistedTaskQueueInfo{
			Data:    tli,
			RangeID: rangeID + 1,
		}

		datablob, err := serialization.TaskQueueInfoToBlob(tl.Data)
		if err != nil {
			return nil, serviceerror.NewInternal(fmt.Sprintf("LeaseTaskQueue operation failed during serialization. TaskQueue: %v, TaskType: %v, Error: %v", request.TaskQueue, request.TaskType, err))
		}

		query = d.session.Query(templateUpdateTaskQueueQuery,
			rangeID+1,
			datablob.Data,
			datablob.EncodingType.String(),
			request.NamespaceID,
			&request.TaskQueue,
			request.TaskType,
			rowTypeTaskQueue,
			taskQueueTaskID,
			rangeID,
		)
	}
	previous := make(map[string]interface{})
	applied, err := query.MapScanCAS(previous)
	if err != nil {
		return nil, gocql.ConvertError("LeaseTaskQueue", err)
	}
	if !applied {
		previousRangeID := previous["range_id"]
		return nil, &p.ConditionFailedError{
			Msg: fmt.Sprintf("leaseTaskQueue: taskQueue:%v, taskQueueType:%v, haveRangeID:%v, gotRangeID:%v",
				request.TaskQueue, request.TaskType, rangeID, previousRangeID),
		}
	}

	return &p.LeaseTaskQueueResponse{TaskQueueInfo: tl}, nil
}

// From TaskManager interface
func (d *cassandraPersistence) UpdateTaskQueue(request *p.UpdateTaskQueueRequest) (*p.UpdateTaskQueueResponse, error) {
	tli := *request.TaskQueueInfo
	tli.LastUpdateTime = timestamp.TimeNowPtrUtc()
	datablob, err := serialization.TaskQueueInfoToBlob(&tli)
	if err != nil {
		return nil, gocql.ConvertError("UpdateTaskQueue", err)
	}

	var applied bool
	previous := make(map[string]interface{})
	if tli.Kind == enumspb.TASK_QUEUE_KIND_STICKY { // if task_queue is sticky, then update with TTL
		batch := d.session.NewBatch(gocql.LoggedBatch)
		batch.Query(templateUpdateTaskQueueQueryWithTTLPart1,
			tli.GetNamespaceId(),
			&tli.Name,
			tli.TaskType,
			rowTypeTaskQueue,
			taskQueueTaskID,
			stickyTaskQueueTTL,
		)
		batch.Query(templateUpdateTaskQueueQueryWithTTLPart2,
			stickyTaskQueueTTL,
			request.RangeID,
			datablob.Data,
			datablob.EncodingType.String(),
			tli.GetNamespaceId(),
			&tli.Name,
			tli.TaskType,
			rowTypeTaskQueue,
			taskQueueTaskID,
			request.RangeID,
		)
		applied, _, err = d.session.MapExecuteBatchCAS(batch, previous)
	} else {
		query := d.session.Query(templateUpdateTaskQueueQuery,
			request.RangeID,
			datablob.Data,
			datablob.EncodingType.String(),
			tli.GetNamespaceId(),
			&tli.Name,
			tli.TaskType,
			rowTypeTaskQueue,
			taskQueueTaskID,
			request.RangeID,
		)
		applied, err = query.MapScanCAS(previous)
	}

	if err != nil {
		return nil, gocql.ConvertError("UpdateTaskQueue", err)
	}

	if !applied {
		var columns []string
		for k, v := range previous {
			columns = append(columns, fmt.Sprintf("%s=%v", k, v))
		}

		return nil, &p.ConditionFailedError{
			Msg: fmt.Sprintf("Failed to update task queue. name: %v, type: %v, rangeID: %v, columns: (%v)",
				tli.Name, tli.TaskType, request.RangeID, strings.Join(columns, ",")),
		}
	}

	return &p.UpdateTaskQueueResponse{}, nil
}

func (d *cassandraPersistence) ListTaskQueue(_ *p.ListTaskQueueRequest) (*p.ListTaskQueueResponse, error) {
	return nil, serviceerror.NewInternal(fmt.Sprintf("unsupported operation"))
}

func (d *cassandraPersistence) DeleteTaskQueue(request *p.DeleteTaskQueueRequest) error {
	query := d.session.Query(templateDeleteTaskQueueQuery,
		request.TaskQueue.NamespaceID, request.TaskQueue.Name, request.TaskQueue.TaskType, rowTypeTaskQueue, taskQueueTaskID, request.RangeID)
	previous := make(map[string]interface{})
	applied, err := query.MapScanCAS(previous)
	if err != nil {
		return gocql.ConvertError("DeleteTaskQueue", err)
	}
	if !applied {
		return &p.ConditionFailedError{
			Msg: fmt.Sprintf("DeleteTaskQueue operation failed: expected_range_id=%v but found %+v", request.RangeID, previous),
		}
	}
	return nil
}

// From TaskManager interface
func (d *cassandraPersistence) CreateTasks(request *p.CreateTasksRequest) (*p.CreateTasksResponse, error) {
	batch := d.session.NewBatch(gocql.LoggedBatch)
	namespaceID := request.TaskQueueInfo.Data.GetNamespaceId()
	taskQueue := request.TaskQueueInfo.Data.Name
	taskQueueType := request.TaskQueueInfo.Data.TaskType

	for _, task := range request.Tasks {
		ttl := GetTaskTTL(task.Data)
		datablob, err := serialization.TaskInfoToBlob(task)
		if err != nil {
			return nil, serviceerror.NewInternal(fmt.Sprintf("CreateTasks operation failed during serialization. Error : %v", err))
		}

		if ttl <= 0 || ttl > maxCassandraTTL {
			batch.Query(templateCreateTaskQuery,
				namespaceID,
				taskQueue,
				taskQueueType,
				rowTypeTask,
				task.GetTaskId(),
				datablob.Data,
				datablob.EncodingType.String())
		} else {
			batch.Query(templateCreateTaskWithTTLQuery,
				namespaceID,
				taskQueue,
				taskQueueType,
				rowTypeTask,
				task.GetTaskId(),
				datablob.Data,
				datablob.EncodingType.String(),
				ttl)
		}
	}

	tl := *request.TaskQueueInfo.Data
	tl.LastUpdateTime = timestamp.TimeNowPtrUtc()
	datablob, err := serialization.TaskQueueInfoToBlob(&tl)

	if err != nil {
		return nil, serviceerror.NewInternal(fmt.Sprintf("CreateTasks operation failed during serialization. Error : %v", err))
	}

	// The following query is used to ensure that range_id didn't change
	batch.Query(templateUpdateTaskQueueQuery,
		request.TaskQueueInfo.RangeID,
		datablob.Data,
		datablob.EncodingType.String(),
		namespaceID,
		taskQueue,
		taskQueueType,
		rowTypeTaskQueue,
		taskQueueTaskID,
		request.TaskQueueInfo.RangeID,
	)

	previous := make(map[string]interface{})
	applied, _, err := d.session.MapExecuteBatchCAS(batch, previous)
	if err != nil {
		return nil, gocql.ConvertError("CreateTasks", err)
	}
	if !applied {
		rangeID := previous["range_id"]
		return nil, &p.ConditionFailedError{
			Msg: fmt.Sprintf("Failed to create task. TaskQueue: %v, taskQueueType: %v, rangeID: %v, db rangeID: %v",
				taskQueue, taskQueueType, request.TaskQueueInfo.RangeID, rangeID),
		}
	}

	return &p.CreateTasksResponse{}, nil
}

func GetTaskTTL(task *persistencespb.TaskInfo) int64 {
	var ttl int64 = 0
	if task.ExpiryTime != nil {
		expiryTtl := convert.Int64Ceil(time.Until(timestamp.TimeValue(task.ExpiryTime)).Seconds())

		// 0 means no ttl, we dont want that.
		// Todo: Come back and correctly ignore expired in-memory tasks before persisting
		if expiryTtl < 1 {
			expiryTtl = 1
		}

		ttl = expiryTtl
	}
	return ttl
}

// From TaskManager interface
func (d *cassandraPersistence) GetTasks(request *p.GetTasksRequest) (*p.GetTasksResponse, error) {
	if request.MaxReadLevel == nil {
		return nil, serviceerror.NewInternal("getTasks: both readLevel and maxReadLevel MUST be specified for cassandra persistence")
	}
	if request.ReadLevel > *request.MaxReadLevel {
		return &p.GetTasksResponse{}, nil
	}

	// Reading taskqueue tasks need to be quorum level consistent, otherwise we could lose tasks
	query := d.session.Query(templateGetTasksQuery,
		request.NamespaceID,
		request.TaskQueue,
		request.TaskType,
		rowTypeTask,
		request.ReadLevel,
		*request.MaxReadLevel,
	).PageSize(request.BatchSize)

	iter := query.Iter()
	if iter == nil {
		return nil, serviceerror.NewInternal("GetTasks operation failed.  Not able to create query iterator.")
	}

	response := &p.GetTasksResponse{}
	task := make(map[string]interface{})
PopulateTasks:
	for iter.MapScan(task) {
		_, ok := task["task_id"]
		if !ok { // no tasks, but static column record returned
			continue
		}

		rawTask, ok := task["task"]
		if !ok {
			return nil, newFieldNotFoundError("task", task)
		}
		taskVal, ok := rawTask.([]byte)
		if !ok {
			var byteSliceType []byte
			return nil, newPersistedTypeMismatchError("task", byteSliceType, rawTask, task)

		}

		rawEncoding, ok := task["task_encoding"]
		if !ok {
			return nil, newFieldNotFoundError("task_encoding", task)
		}
		encodingVal, ok := rawEncoding.(string)
		if !ok {
			var byteSliceType []byte
			return nil, newPersistedTypeMismatchError("task_encoding", byteSliceType, rawEncoding, task)
		}

		t, err := serialization.TaskInfoFromBlob(taskVal, encodingVal)
		if err != nil {
			return nil, gocql.ConvertError("GetTasks", err)
		}

		response.Tasks = append(response.Tasks, t)
		if len(response.Tasks) == request.BatchSize {
			break PopulateTasks
		}
		task = make(map[string]interface{}) // Reinitialize map as initialized fails on unmarshalling
	}

	if err := iter.Close(); err != nil {
		return nil, serviceerror.NewInternal(fmt.Sprintf("GetTasks operation failed. Error: %v", err))
	}

	return response, nil
}

// From TaskManager interface
func (d *cassandraPersistence) CompleteTask(request *p.CompleteTaskRequest) error {
	tli := request.TaskQueue
	query := d.session.Query(templateCompleteTaskQuery,
		tli.NamespaceID,
		tli.Name,
		tli.TaskType,
		rowTypeTask,
		request.TaskID)

	err := query.Exec()
	if err != nil {
		return gocql.ConvertError("CompleteTask", err)
	}

	return nil
}

// CompleteTasksLessThan deletes all tasks less than or equal to the given task id. This API ignores the
// Limit request parameter i.e. either all tasks leq the task_id will be deleted or an error will
// be returned to the caller
func (d *cassandraPersistence) CompleteTasksLessThan(request *p.CompleteTasksLessThanRequest) (int, error) {
	query := d.session.Query(templateCompleteTasksLessThanQuery,
		request.NamespaceID, request.TaskQueueName, request.TaskType, rowTypeTask, request.TaskID)
	err := query.Exec()
	if err != nil {
		return 0, gocql.ConvertError("CompleteTasksLessThan", err)
	}
	return p.UnknownNumRowsAffected, nil
}

func (d *cassandraPersistence) GetTimerTask(request *p.GetTimerTaskRequest) (*p.GetTimerTaskResponse, error) {
	shardID := d.shardID
	taskID := request.TaskID
	visibilityTs := request.VisibilityTimestamp
	query := d.session.Query(templateGetTimerTaskQuery,
		shardID,
		rowTypeTimerTask,
		rowTypeTimerNamespaceID,
		rowTypeTimerWorkflowID,
		rowTypeTimerRunID,
		visibilityTs,
		taskID)

	var data []byte
	var encoding string
	if err := query.Scan(&data, &encoding); err != nil {
		return nil, gocql.ConvertError("GetTimerTask", err)
	}

	info, err := serialization.TimerTaskInfoFromBlob(data, encoding)

	if err != nil {
		return nil, gocql.ConvertError("GetTimerTask", err)
	}

	return &p.GetTimerTaskResponse{TimerTaskInfo: info}, nil
}

func (d *cassandraPersistence) GetTimerIndexTasks(request *p.GetTimerIndexTasksRequest) (*p.GetTimerIndexTasksResponse,
	error) {
	// Reading timer tasks need to be quorum level consistent, otherwise we could lose tasks
	minTimestamp := p.UnixNanoToDBTimestamp(request.MinTimestamp.UnixNano())
	maxTimestamp := p.UnixNanoToDBTimestamp(request.MaxTimestamp.UnixNano())
	query := d.session.Query(templateGetTimerTasksQuery,
		d.shardID,
		rowTypeTimerTask,
		rowTypeTimerNamespaceID,
		rowTypeTimerWorkflowID,
		rowTypeTimerRunID,
		minTimestamp,
		maxTimestamp,
	).PageSize(request.BatchSize).PageState(request.NextPageToken)

	iter := query.Iter()
	if iter == nil {
		return nil, serviceerror.NewInternal("GetTimerTasks operation failed.  Not able to create query iterator.")
	}

	response := &p.GetTimerIndexTasksResponse{}
	var data []byte
	var encoding string

	for iter.Scan(&data, &encoding) {
		t, err := serialization.TimerTaskInfoFromBlob(data, encoding)

		if err != nil {
			return nil, gocql.ConvertError("GetTimerIndexTasks", err)
		}

		response.Timers = append(response.Timers, t)
	}
	nextPageToken := iter.PageState()
	response.NextPageToken = make([]byte, len(nextPageToken))
	copy(response.NextPageToken, nextPageToken)

	if err := iter.Close(); err != nil {
		return nil, gocql.ConvertError("GetTimerIndexTasks", err)
	}

	return response, nil
}

func (d *cassandraPersistence) PutReplicationTaskToDLQ(request *p.PutReplicationTaskToDLQRequest) error {
	task := request.TaskInfo
	datablob, err := serialization.ReplicationTaskInfoToBlob(task)
	if err != nil {
		return gocql.ConvertError("PutReplicationTaskToDLQ", err)
	}

	// Use source cluster name as the workflow id for replication dlq
	query := d.session.Query(templateCreateReplicationTaskQuery,
		d.shardID,
		rowTypeDLQ,
		rowTypeDLQNamespaceID,
		request.SourceClusterName,
		rowTypeDLQRunID,
		datablob.Data,
		datablob.EncodingType.String(),
		defaultVisibilityTimestamp,
		task.GetTaskId())

	err = query.Exec()
	if err != nil {
		return gocql.ConvertError("PutReplicationTaskToDLQ", err)
	}

	return nil
}

func (d *cassandraPersistence) GetReplicationTasksFromDLQ(
	request *p.GetReplicationTasksFromDLQRequest,
) (*p.GetReplicationTasksFromDLQResponse, error) {
	// Reading replication tasks need to be quorum level consistent, otherwise we could lose tasks
	query := d.session.Query(templateGetReplicationTasksQuery,
		d.shardID,
		rowTypeDLQ,
		rowTypeDLQNamespaceID,
		request.SourceClusterName,
		rowTypeDLQRunID,
		defaultVisibilityTimestamp,
		request.MinTaskID,
		request.MinTaskID+int64(request.BatchSize),
	).PageSize(request.BatchSize).PageState(request.NextPageToken)

	return d.populateGetReplicationTasksResponse(query, "GetReplicationTasksFromDLQ")
}

func (d *cassandraPersistence) DeleteReplicationTaskFromDLQ(
	request *p.DeleteReplicationTaskFromDLQRequest,
) error {

	query := d.session.Query(templateCompleteReplicationTaskQuery,
		d.shardID,
		rowTypeDLQ,
		rowTypeDLQNamespaceID,
		request.SourceClusterName,
		rowTypeDLQRunID,
		defaultVisibilityTimestamp,
		request.TaskID,
	)

	err := query.Exec()
	return gocql.ConvertError("DeleteReplicationTaskFromDLQ", err)
}

func (d *cassandraPersistence) RangeDeleteReplicationTaskFromDLQ(
	request *p.RangeDeleteReplicationTaskFromDLQRequest,
) error {

	query := d.session.Query(templateRangeCompleteReplicationTaskQuery,
		d.shardID,
		rowTypeDLQ,
		rowTypeDLQNamespaceID,
		request.SourceClusterName,
		rowTypeDLQRunID,
		defaultVisibilityTimestamp,
		request.ExclusiveBeginTaskID,
		request.InclusiveEndTaskID,
	)

	err := query.Exec()
	return gocql.ConvertError("RangeDeleteReplicationTaskFromDLQ", err)
}

func mutableStateFromRow(result map[string]interface{}) (*p.InternalWorkflowMutableState, error) {
	eiBytes, ok := result["execution"].([]byte)
	if !ok {
		return nil, newPersistedTypeMismatchError("execution", "", eiBytes, result)
	}

	eiEncoding, ok := result["execution_encoding"].(string)
	if !ok {
		return nil, newPersistedTypeMismatchError("execution_encoding", "", eiEncoding, result)
	}

	protoInfo, err := serialization.WorkflowExecutionInfoFromBlob(eiBytes, eiEncoding)
	if err != nil {
		return nil, err
	}

	nextEventID, ok := result["next_event_id"].(int64)
	if !ok {
		return nil, newPersistedTypeMismatchError("next_event_id", "", nextEventID, result)
	}

	protoState, err := protoExecutionStateFromRow(result)
	if err != nil {
		return nil, err
	}

	mutableState := &p.InternalWorkflowMutableState{
		ExecutionInfo:  protoInfo,
		ExecutionState: protoState,
		NextEventID:    nextEventID,
	}
	return mutableState, nil
}
