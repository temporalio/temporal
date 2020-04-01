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

package cassandra

import (
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/gocql/gocql"
	"github.com/gogo/protobuf/types"
	"go.temporal.io/temporal-proto/enums"
	"go.temporal.io/temporal-proto/serviceerror"

	"github.com/temporalio/temporal/.gen/proto/persistenceblobs"
	"github.com/temporalio/temporal/common"
	"github.com/temporalio/temporal/common/cassandra"
	checksum "github.com/temporalio/temporal/common/checksum"
	"github.com/temporalio/temporal/common/log"
	p "github.com/temporalio/temporal/common/persistence"
	"github.com/temporalio/temporal/common/persistence/serialization"
	"github.com/temporalio/temporal/common/primitives"
	"github.com/temporalio/temporal/common/service/config"
)

//	"go.temporal.io/temporal-proto/serviceerror"
// Guidelines for creating new special UUID constants
// Each UUID should be of the form: E0000000-R000-f000-f000-00000000000x
// Where x is any hexadecimal value, E represents the entity type valid values are:
// E = {NamespaceID = 1, WorkflowID = 2, RunID = 3}
// R represents row type in executions table, valid values are:
// R = {Shard = 1, Execution = 2, Transfer = 3, Timer = 4, Replication = 5}
const (
	cassandraProtoVersion = 4
	defaultSessionTimeout = 10 * time.Second
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
	// Row Constants for Replication Task DLQ Row. Source cluster name will be used as WorkflowID.
	rowTypeDLQNamespaceID = "10000000-6000-f000-f000-000000000000"
	rowTypeDLQRunID       = "30000000-6000-f000-f000-000000000000"
	// Special TaskId constants
	rowTypeExecutionTaskID = int64(-10)
	rowTypeShardTaskID     = int64(-11)
	emptyInitiatedID       = int64(-7)

	stickyTaskListTTL = int32(24 * time.Hour / time.Second) // if sticky task_list stopped being updated, remove it in one day
)

const (
	// Row types for table executions
	rowTypeShard = iota
	rowTypeExecution
	rowTypeTransferTask
	rowTypeTimerTask
	rowTypeReplicationTask
	rowTypeDLQ
)

const (
	// Row types for table tasks
	rowTypeTask = iota
	rowTypeTaskList
)

const (
	taskListTaskID = -12345
	initialRangeID = 1 // Id of the first range of a new task list
)

const (
	templateWorkflowExecutionType = `{` +
		`namespace_id: ?, ` +
		`workflow_id: ?, ` +
		`run_id: ?, ` +
		`parent_namespace_id: ?, ` +
		`parent_workflow_id: ?, ` +
		`parent_run_id: ?, ` +
		`initiated_id: ?, ` +
		`completion_event_batch_id: ?, ` +
		`completion_event: ?, ` +
		`completion_event_data_encoding: ?, ` +
		`task_list: ?, ` +
		`workflow_type_name: ?, ` +
		`workflow_timeout: ?, ` +
		`decision_task_timeout: ?, ` +
		`execution_context: ?, ` +
		`state: ?, ` +
		`close_status: ?, ` +
		`last_first_event_id: ?, ` +
		`last_event_task_id: ?, ` +
		`next_event_id: ?, ` +
		`last_processed_event: ?, ` +
		`start_time: ?, ` +
		`last_updated_time: ?, ` +
		`create_request_id: ?, ` +
		`signal_count: ?, ` +
		`history_size: ?, ` +
		`decision_version: ?, ` +
		`decision_schedule_id: ?, ` +
		`decision_started_id: ?, ` +
		`decision_request_id: ?, ` +
		`decision_timeout: ?, ` +
		`decision_attempt: ?, ` +
		`decision_timestamp: ?, ` +
		`decision_scheduled_timestamp: ?, ` +
		`decision_original_scheduled_timestamp: ?, ` +
		`cancel_requested: ?, ` +
		`cancel_request_id: ?, ` +
		`sticky_task_list: ?, ` +
		`sticky_schedule_to_start_timeout: ?,` +
		`client_library_version: ?, ` +
		`client_feature_version: ?, ` +
		`client_impl: ?, ` +
		`auto_reset_points: ?, ` +
		`auto_reset_points_encoding: ?, ` +
		`attempt: ?, ` +
		`has_retry_policy: ?, ` +
		`init_interval: ?, ` +
		`backoff_coefficient: ?, ` +
		`max_interval: ?, ` +
		`expiration_time: ?, ` +
		`max_attempts: ?, ` +
		`non_retriable_errors: ?, ` +
		`event_store_version: ?, ` +
		`branch_token: ?, ` +
		`cron_schedule: ?, ` +
		`expiration_seconds: ?, ` +
		`search_attributes: ?, ` +
		`memo: ? ` +
		`}`

	templateReplicationStateType = `{` +
		`current_version: ?, ` +
		`start_version: ?, ` +
		`last_write_version: ?, ` +
		`last_write_event_id: ?, ` +
		`last_replication_info: ?` +
		`}`

	templateActivityInfoType = `{` +
		`version: ?,` +
		`schedule_id: ?, ` +
		`scheduled_event_batch_id: ?, ` +
		`scheduled_event: ?, ` +
		`scheduled_time: ?, ` +
		`started_id: ?, ` +
		`started_event: ?, ` +
		`started_time: ?, ` +
		`activity_id: ?, ` +
		`request_id: ?, ` +
		`details: ?, ` +
		`schedule_to_start_timeout: ?, ` +
		`schedule_to_close_timeout: ?, ` +
		`start_to_close_timeout: ?, ` +
		`heart_beat_timeout: ?, ` +
		`cancel_requested: ?, ` +
		`cancel_request_id: ?, ` +
		`last_hb_updated_time: ?, ` +
		`timer_task_status: ?, ` +
		`attempt: ?, ` +
		`task_list: ?, ` +
		`started_identity: ?, ` +
		`has_retry_policy: ?, ` +
		`init_interval: ?, ` +
		`backoff_coefficient: ?, ` +
		`max_interval: ?, ` +
		`expiration_time: ?, ` +
		`max_attempts: ?, ` +
		`non_retriable_errors: ?, ` +
		`last_failure_reason: ?, ` +
		`last_worker_identity: ?, ` +
		`last_failure_details: ?, ` +
		`event_data_encoding: ?` +
		`}`

	templateChildExecutionInfoType = `{` +
		`version: ?,` +
		`initiated_id: ?, ` +
		`initiated_event_batch_id: ?, ` +
		`initiated_event: ?, ` +
		`started_id: ?, ` +
		`started_workflow_id: ?, ` +
		`started_run_id: ?, ` +
		`started_event: ?, ` +
		`create_request_id: ?, ` +
		`event_data_encoding: ?, ` +
		`namespace: ?, ` +
		`workflow_type_name: ?, ` +
		`parent_close_policy: ?` +
		`}`

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
		`SET current_run_id = ?,
execution_state = ?, execution_state_encoding = ?,
replication_state = {start_version: ?, last_write_version: ?},
workflow_last_write_version = ?,
workflow_state = ? ` +
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
		`shard_id, type, namespace_id, workflow_id, run_id, visibility_ts, task_id, current_run_id, execution_state, execution_state_encoding, replication_state, workflow_last_write_version, workflow_state) ` +
		`VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, {start_version: ?, last_write_version: ?}, ?, ?) IF NOT EXISTS USING TTL 0 `

	templateCreateWorkflowExecutionQuery = `INSERT INTO executions (` +
		`shard_id, namespace_id, workflow_id, run_id, type, ` +
		`execution, execution_encoding, execution_state, execution_state_encoding, next_event_id, ` +
		`visibility_ts, task_id, checksum, checksum_encoding) ` +
		`VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) IF NOT EXISTS `

	templateCreateWorkflowExecutionWithReplicationQuery = `INSERT INTO executions (` +
		`shard_id, namespace_id, workflow_id, run_id, type, ` +
		`execution, execution_encoding, execution_state, execution_state_encoding, replication_state, ` +
		`next_event_id, visibility_ts, task_id, checksum, checksum_encoding) ` +
		`VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ` + templateReplicationStateType +
		`, ?, ?, ?, ?, ?) IF NOT EXISTS `

	templateCreateWorkflowExecutionWithVersionHistoriesQuery = `INSERT INTO executions (` +
		`shard_id, namespace_id, workflow_id, run_id, type, ` +
		`execution, execution_encoding, execution_state, execution_state_encoding, next_event_id, ` +
		`visibility_ts, task_id, version_histories, version_histories_encoding, checksum, checksum_encoding) ` +
		`VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) IF NOT EXISTS `

	templateCreateTransferTaskQuery = `INSERT INTO executions (` +
		`shard_id, type, namespace_id, workflow_id, run_id, transfer, transfer_encoding, visibility_ts, task_id) ` +
		`VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)`

	templateCreateReplicationTaskQuery = `INSERT INTO executions (` +
		`shard_id, type, namespace_id, workflow_id, run_id, replication, replication_encoding, visibility_ts, task_id) ` +
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

	templateGetWorkflowExecutionQuery = `SELECT execution, execution_encoding, execution_state, execution_state_encoding, next_event_id, replication_state, activity_map, activity_map_encoding, timer_map, timer_map_encoding, ` +
		`child_executions_map, child_executions_map_encoding, request_cancel_map, request_cancel_map_encoding, signal_map, signal_map_encoding, signal_requested, buffered_events_list, ` +
		`version_histories, version_histories_encoding, checksum, checksum_encoding ` +
		`FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ?`

	templateGetCurrentExecutionQuery = `SELECT current_run_id, execution, execution_encoding, execution_state, execution_state_encoding, replication_state ` +
		`FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ?`

	templateCheckWorkflowExecutionQuery = `UPDATE executions ` +
		`SET next_event_id = ? ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? ` +
		`IF next_event_id = ?`

	templateUpdateWorkflowExecutionQuery = `UPDATE executions ` +
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

	templateUpdateWorkflowExecutionWithReplicationQuery = `UPDATE executions ` +
		`SET execution = ? ` +
		`, execution_encoding = ? ` +
		`, execution_state = ? ` +
		`, execution_state_encoding = ? ` +
		`, replication_state = ` + templateReplicationStateType +
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

	templateUpdateWorkflowExecutionWithVersionHistoriesQuery = `UPDATE executions ` +
		`SET execution = ?` +
		`, execution_encoding = ?` +
		`, execution_state = ? ` +
		`, execution_state_encoding = ? ` +
		`, next_event_id = ? ` +
		`, version_histories = ? ` +
		`, version_histories_encoding = ? ` +
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
		`namespace_id, task_list_name, task_list_type, type, task_id, task, task_encoding) ` +
		`VALUES(?, ?, ?, ?, ?, ?, ?)`

	templateCreateTaskWithTTLQuery = `INSERT INTO tasks (` +
		`namespace_id, task_list_name, task_list_type, type, task_id, task, task_encoding) ` +
		`VALUES(?, ?, ?, ?, ?, ?, ?) USING TTL ?`

	templateGetTasksQuery = `SELECT task_id, task, task_encoding ` +
		`FROM tasks ` +
		`WHERE namespace_id = ? ` +
		`and task_list_name = ? ` +
		`and task_list_type = ? ` +
		`and type = ? ` +
		`and task_id > ? ` +
		`and task_id <= ?`

	templateCompleteTaskQuery = `DELETE FROM tasks ` +
		`WHERE namespace_id = ? ` +
		`and task_list_name = ? ` +
		`and task_list_type = ? ` +
		`and type = ? ` +
		`and task_id = ?`

	templateCompleteTasksLessThanQuery = `DELETE FROM tasks ` +
		`WHERE namespace_id = ? ` +
		`AND task_list_name = ? ` +
		`AND task_list_type = ? ` +
		`AND type = ? ` +
		`AND task_id <= ? `

	templateGetTaskList = `SELECT ` +
		`range_id, ` +
		`task_list, ` +
		`task_list_encoding ` +
		`FROM tasks ` +
		`WHERE namespace_id = ? ` +
		`and task_list_name = ? ` +
		`and task_list_type = ? ` +
		`and type = ? ` +
		`and task_id = ?`

	templateInsertTaskListQuery = `INSERT INTO tasks (` +
		`namespace_id, ` +
		`task_list_name, ` +
		`task_list_type, ` +
		`type, ` +
		`task_id, ` +
		`range_id, ` +
		`task_list, ` +
		`task_list_encoding ` +
		`) VALUES (?, ?, ?, ?, ?, ?, ?, ?) IF NOT EXISTS`

	templateUpdateTaskListQuery = `UPDATE tasks SET ` +
		`range_id = ?, ` +
		`task_list = ?, ` +
		`task_list_encoding = ? ` +
		`WHERE namespace_id = ? ` +
		`and task_list_name = ? ` +
		`and task_list_type = ? ` +
		`and type = ? ` +
		`and task_id = ? ` +
		`IF range_id = ?`

	templateUpdateTaskListQueryWithTTL = `INSERT INTO tasks (` +
		`namespace_id, ` +
		`task_list_name, ` +
		`task_list_type, ` +
		`type, ` +
		`task_id, ` +
		`range_id, ` +
		`task_list, ` +
		`task_list_encoding ` +
		`) VALUES (?, ?, ?, ?, ?, ?, ?, ?) USING TTL ?`

	templateDeleteTaskListQuery = `DELETE FROM tasks ` +
		`WHERE namespace_id = ? ` +
		`AND task_list_name = ? ` +
		`AND task_list_type = ? ` +
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
		session *gocql.Session
		logger  log.Logger
	}

	// Implements ExecutionManager, ShardManager and TaskManager
	cassandraPersistence struct {
		cassandraStore
		shardID            int
		currentClusterName string
	}
)

var _ p.ExecutionStore = (*cassandraPersistence)(nil)

// newShardPersistence is used to create an instance of ShardManager implementation
func newShardPersistence(cfg config.Cassandra, clusterName string, logger log.Logger) (p.ShardStore, error) {
	cluster := cassandra.NewCassandraCluster(cfg)
	cluster.ProtoVersion = cassandraProtoVersion
	cluster.Consistency = gocql.LocalQuorum
	cluster.SerialConsistency = gocql.LocalSerial
	cluster.Timeout = defaultSessionTimeout

	session, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}

	return &cassandraPersistence{
		cassandraStore:     cassandraStore{session: session, logger: logger},
		shardID:            -1,
		currentClusterName: clusterName,
	}, nil
}

// NewWorkflowExecutionPersistence is used to create an instance of workflowExecutionManager implementation
func NewWorkflowExecutionPersistence(
	shardID int,
	session *gocql.Session,
	logger log.Logger,
) (p.ExecutionStore, error) {
	return &cassandraPersistence{cassandraStore: cassandraStore{session: session, logger: logger}, shardID: shardID}, nil
}

// newTaskPersistence is used to create an instance of TaskManager implementation
func newTaskPersistence(cfg config.Cassandra, logger log.Logger) (p.TaskStore, error) {
	cluster := cassandra.NewCassandraCluster(cfg)
	cluster.ProtoVersion = cassandraProtoVersion
	cluster.Consistency = gocql.LocalQuorum
	cluster.SerialConsistency = gocql.LocalSerial
	cluster.Timeout = defaultSessionTimeout
	session, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}
	return &cassandraPersistence{cassandraStore: cassandraStore{session: session, logger: logger}, shardID: -1}, nil
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

func (d *cassandraPersistence) GetShardID() int {
	return d.shardID
}

func (d *cassandraPersistence) CreateShard(request *p.CreateShardRequest) error {
	shardInfo := request.ShardInfo
	shardInfo.UpdatedAt = types.TimestampNow()
	data, err := serialization.ShardInfoToBlob(shardInfo)

	if err != nil {
		return convertCommonErrors("CreateShard", err)
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
		data.Encoding,
		shardInfo.GetRangeId())

	previous := make(map[string]interface{})
	applied, err := query.MapScanCAS(previous)
	if err != nil {
		return convertCommonErrors("CreateShard", err)
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
		return nil, convertCommonErrors("GetShard", err)
	}

	info, err := serialization.ShardInfoFromBlob(data, encoding, d.currentClusterName)

	if err != nil {
		return nil, convertCommonErrors("GetShard", err)
	}

	return &p.GetShardResponse{ShardInfo: info}, nil
}

func (d *cassandraPersistence) UpdateShard(request *p.UpdateShardRequest) error {
	shardInfo := request.ShardInfo
	shardInfo.UpdatedAt = types.TimestampNow()
	data, err := serialization.ShardInfoToBlob(shardInfo)

	if err != nil {
		return convertCommonErrors("UpdateShard", err)
	}

	query := d.session.Query(templateUpdateShardQuery,
		data.Data,
		data.Encoding,
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
		return convertCommonErrors("UpdateShard", err)
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
	executionInfo := newWorkflow.ExecutionInfo
	startVersion := newWorkflow.StartVersion
	lastWriteVersion := newWorkflow.LastWriteVersion
	namespaceID := executionInfo.NamespaceID
	workflowID := executionInfo.WorkflowID
	runID := executionInfo.RunID

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
			executionInfo.State,
			executionInfo.CloseStatus,
			executionInfo.CreateRequestID,
			startVersion,
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
	defer func() {
		if iter != nil {
			iter.Close()
		}
	}()

	if err != nil {
		if isTimeoutError(err) {
			// Write may have succeeded, but we don't know
			// return this info to the caller so they have the option of trying to find out by executing a read
			return nil, &p.TimeoutError{Msg: fmt.Sprintf("CreateWorkflowExecution timed out. Error: %v", err)}
		} else if isThrottlingError(err) {
			return nil, serviceerror.NewResourceExhausted(fmt.Sprintf("CreateWorkflowExecution operation failed. Error: %v", err))
		}

		return nil, serviceerror.NewInternal(fmt.Sprintf("CreateWorkflowExecution operation failed. Error: %v", err))
	}

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
			runID := previous["run_id"].(gocql.UUID).String()

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

					replicationState := createReplicationState(previous["replication_state"].(map[string]interface{}))
					lastWriteVersion := replicationState.LastWriteVersion

					msg := fmt.Sprintf("Workflow execution already running. WorkflowId: %v, RunId: %v, rangeID: %v, columns: (%v)",
						executionInfo.WorkflowID, primitives.UUIDString(protoState.RunId), request.RangeID, strings.Join(columns, ","))
					if request.Mode == p.CreateWorkflowModeBrandNew {
						// todo: Look at moving these errors upstream to manager
						return nil, &p.WorkflowExecutionAlreadyStartedError{
							Msg:              msg,
							StartRequestID:   protoState.CreateRequestId,
							RunID:            primitives.UUIDString(protoState.RunId),
							State:            int(protoState.State),
							CloseStatus:      enums.WorkflowExecutionCloseStatus(protoState.CloseStatus),
							LastWriteVersion: lastWriteVersion,
						}
					}
					return nil, &p.CurrentWorkflowConditionFailedError{Msg: msg}
				}

				if prevRunID := previous["current_run_id"].(gocql.UUID).String(); prevRunID != request.PreviousRunID {
					// currentRunID on previous run has been changed, return to caller to handle
					msg := fmt.Sprintf("Workflow execution creation condition failed by mismatch runID. WorkflowId: %v, Expected Current RunID: %v, Actual Current RunID: %v",
						executionInfo.WorkflowID, request.PreviousRunID, prevRunID)
					return nil, &p.CurrentWorkflowConditionFailedError{Msg: msg}
				}

				msg := fmt.Sprintf("Workflow execution creation condition failed. WorkflowId: %v, CurrentRunID: %v, columns: (%v)",
					executionInfo.WorkflowID, executionInfo.RunID, strings.Join(columns, ","))
				return nil, &p.CurrentWorkflowConditionFailedError{Msg: msg}
			} else if rowType == rowTypeExecution && runID == executionInfo.RunID {
				msg := fmt.Sprintf("Workflow execution already running. WorkflowId: %v, RunId: %v, rangeID: %v",
					executionInfo.WorkflowID, executionInfo.RunID, request.RangeID)
				replicationState := createReplicationState(previous["replication_state"].(map[string]interface{}))
				lastWriteVersion = common.EmptyVersion
				if replicationState != nil {
					lastWriteVersion = replicationState.LastWriteVersion
				}
				return nil, &p.WorkflowExecutionAlreadyStartedError{
					Msg:              msg,
					StartRequestID:   executionInfo.CreateRequestID,
					RunID:            executionInfo.RunID,
					State:            executionInfo.State,
					CloseStatus:      executionInfo.CloseStatus,
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

func (d *cassandraPersistence) GetWorkflowExecution(request *p.GetWorkflowExecutionRequest) (
	*p.InternalGetWorkflowExecutionResponse, error) {
	execution := request.Execution
	query := d.session.Query(templateGetWorkflowExecutionQuery,
		d.shardID,
		rowTypeExecution,
		request.NamespaceID,
		execution.WorkflowId,
		execution.RunId,
		defaultVisibilityTimestamp,
		rowTypeExecutionTaskID)

	result := make(map[string]interface{})
	if err := query.MapScan(result); err != nil {
		return nil, convertCommonErrors("GetWorkflowExecution", err)
	}

	info, err := workflowExecutionFromRow(result)
	if err != nil {
		return nil, serviceerror.NewInternal(fmt.Sprintf("GetWorkflowExecution operation failed. Error: %v", err))
	}

	state := &p.InternalWorkflowMutableState{
		ExecutionInfo:    info,
		ReplicationState: createReplicationState(result["replication_state"].(map[string]interface{})),
		VersionHistories: p.NewDataBlob(result["version_histories"].([]byte), common.EncodingType(result["version_histories_encoding"].(string))),
	}

	if state.VersionHistories != nil && state.ReplicationState != nil {
		return nil, serviceerror.NewInternal(fmt.Sprintf("GetWorkflowExecution operation failed. VersionHistories and ReplicationState both are set."))
	}

	activityInfos := make(map[int64]*p.InternalActivityInfo)
	aMap := result["activity_map"].(map[int64][]byte)
	aMapEncoding := result["activity_map_encoding"].(string)
	for key, value := range aMap {
		aInfo, err := serialization.ActivityInfoFromBlob(value, aMapEncoding)
		if err != nil {
			return nil, err
		}
		info := p.ProtoActivityInfoToInternalActivityInfo(aInfo)
		activityInfos[key] = info
	}
	state.ActivityInfos = activityInfos

	timerInfos := make(map[string]*persistenceblobs.TimerInfo)
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

	childExecutionInfos := make(map[int64]*p.InternalChildExecutionInfo)
	cMap := result["child_executions_map"].(map[int64][]byte)
	cMapEncoding := result["child_executions_map_encoding"].(string)
	for key, value := range cMap {
		cInfo, err := serialization.ChildExecutionInfoFromBlob(value, cMapEncoding)
		if err != nil {
			return nil, err
		}
		childExecutionInfos[key] = p.ProtoChildExecutionInfoToInternal(cInfo)
	}
	state.ChildExecutionInfos = childExecutionInfos

	requestCancelInfos := make(map[int64]*persistenceblobs.RequestCancelInfo)
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

	signalInfos := make(map[int64]*persistenceblobs.SignalInfo)
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

	signalRequestedIDs := make(map[string]struct{})
	sList := result["signal_requested"].([]gocql.UUID)
	for _, v := range sList {
		signalRequestedIDs[v.String()] = struct{}{}
	}
	state.SignalRequestedIDs = signalRequestedIDs

	eList := result["buffered_events_list"].([]map[string]interface{})
	bufferedEventsBlobs := make([]*serialization.DataBlob, 0, len(eList))
	for _, v := range eList {
		blob := createHistoryEventBatchBlob(v)
		bufferedEventsBlobs = append(bufferedEventsBlobs, blob)
	}
	state.BufferedEvents = bufferedEventsBlobs

	cs, err := serialization.ChecksumFromBlob(result["checksum"].([]byte), result["checksum_encoding"].(string))
	if err != nil {
		return nil, serviceerror.NewInternal(fmt.Sprintf("GetWorkflowExecution operation failed. Error: %v", err))
	}
	state.Checksum = *checksum.FromProto(cs)

	return &p.InternalGetWorkflowExecutionResponse{State: state}, nil
}

func protoExecutionStateFromRow(result map[string]interface{}) (*persistenceblobs.WorkflowExecutionState, error) {
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

func protoActivityInfoFromRow(result map[string]interface{}) (*persistenceblobs.WorkflowExecutionState, error) {
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

func workflowExecutionFromRow(result map[string]interface{}) (*p.InternalWorkflowExecutionInfo, error) {
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

	info := p.ProtoWorkflowExecutionToPartialInternalExecution(protoInfo, protoState, nextEventID)
	return info, nil
}

func (d *cassandraPersistence) UpdateWorkflowExecution(request *p.InternalUpdateWorkflowExecutionRequest) error {

	batch := d.session.NewBatch(gocql.LoggedBatch)

	updateWorkflow := request.UpdateWorkflowMutation
	newWorkflow := request.NewWorkflowSnapshot

	executionInfo := updateWorkflow.ExecutionInfo
	namespaceID := executionInfo.NamespaceID
	workflowID := executionInfo.WorkflowID
	runID := executionInfo.RunID
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
			newExecutionInfo := newWorkflow.ExecutionInfo
			newStartVersion := newWorkflow.StartVersion
			newLastWriteVersion := newWorkflow.LastWriteVersion
			newNamespaceID := newExecutionInfo.NamespaceID
			newWorkflowID := newExecutionInfo.WorkflowID
			newRunID := newExecutionInfo.RunID

			if namespaceID != newNamespaceID {
				return serviceerror.NewInternal(fmt.Sprintf("UpdateWorkflowExecution: cannot continue as new to another namespace"))
			}

			if err := createOrUpdateCurrentExecution(batch,
				p.CreateWorkflowModeContinueAsNew,
				d.shardID,
				newNamespaceID,
				newWorkflowID,
				newRunID,
				newExecutionInfo.State,
				newExecutionInfo.CloseStatus,
				newExecutionInfo.CreateRequestID,
				newStartVersion,
				newLastWriteVersion,
				runID,
				0, // for continue as new, this is not used
			); err != nil {
				return err
			}

		} else {
			startVersion := updateWorkflow.StartVersion
			lastWriteVersion := updateWorkflow.LastWriteVersion

			executionStateDatablob, err := serialization.WorkflowExecutionStateToBlob(&persistenceblobs.WorkflowExecutionState{
				RunId:           primitives.MustParseUUID(runID),
				CreateRequestId: executionInfo.CreateRequestID,
				State:           int32(executionInfo.State),
				CloseStatus:     int32(executionInfo.CloseStatus),
			})

			if err != nil {
				return err
			}

			batch.Query(templateUpdateCurrentWorkflowExecutionQuery,
				runID,
				executionStateDatablob.Data,
				executionStateDatablob.Encoding.String(),
				startVersion,
				lastWriteVersion,
				lastWriteVersion,
				executionInfo.State,
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
	defer func() {
		if iter != nil {
			iter.Close()
		}
	}()

	if err != nil {
		if isTimeoutError(err) {
			// Write may have succeeded, but we don't know
			// return this info to the caller so they have the option of trying to find out by executing a read
			return &p.TimeoutError{Msg: fmt.Sprintf("UpdateWorkflowExecution timed out. Error: %v", err)}
		} else if isThrottlingError(err) {
			return serviceerror.NewResourceExhausted(fmt.Sprintf("UpdateWorkflowExecution operation failed. Error: %v", err))
		}
		return serviceerror.NewInternal(fmt.Sprintf("UpdateWorkflowExecution operation failed. Error: %v", err))
	}

	if !applied {
		return d.getExecutionConditionalUpdateFailure(previous, iter, executionInfo.RunID, updateWorkflow.Condition, request.RangeID, executionInfo.RunID)
	}
	return nil
}

//TODO: update query with version histories
func (d *cassandraPersistence) ResetWorkflowExecution(request *p.InternalResetWorkflowExecutionRequest) error {

	batch := d.session.NewBatch(gocql.LoggedBatch)

	shardID := d.shardID

	namespaceID := request.NewWorkflowSnapshot.ExecutionInfo.NamespaceID
	workflowID := request.NewWorkflowSnapshot.ExecutionInfo.WorkflowID

	baseRunID := request.BaseRunID
	baseRunNextEventID := request.BaseRunNextEventID

	currentRunID := request.CurrentRunID
	currentRunNextEventID := request.CurrentRunNextEventID

	newRunID := request.NewWorkflowSnapshot.ExecutionInfo.RunID
	newExecutionInfo := request.NewWorkflowSnapshot.ExecutionInfo

	startVersion := request.NewWorkflowSnapshot.StartVersion
	lastWriteVersion := request.NewWorkflowSnapshot.LastWriteVersion

	stateDatablob, err := serialization.WorkflowExecutionStateToBlob(&persistenceblobs.WorkflowExecutionState{
		CreateRequestId: newExecutionInfo.CreateRequestID,
		State:           int32(newExecutionInfo.State),
		CloseStatus:     int32(newExecutionInfo.CloseStatus),
		RunId:           primitives.MustParseUUID(newRunID),
	})
	if err != nil {
		return err
	}

	batch.Query(templateUpdateCurrentWorkflowExecutionQuery,
		newRunID,
		stateDatablob.Data,
		stateDatablob.Encoding,
		startVersion,
		lastWriteVersion,
		lastWriteVersion,
		newExecutionInfo.State,
		d.shardID,
		rowTypeExecution,
		newExecutionInfo.NamespaceID,
		newExecutionInfo.WorkflowID,
		permanentRunID,
		defaultVisibilityTimestamp,
		rowTypeExecutionTaskID,
		currentRunID,
	)

	// for forkRun, check condition without updating anything to make sure the forkRun hasn't been deleted.
	// Without this check, it will run into race condition with deleteHistoryEvent timer task
	// we only do it when forkRun != currentRun
	if baseRunID != currentRunID {
		batch.Query(templateCheckWorkflowExecutionQuery,
			baseRunNextEventID,
			d.shardID,
			rowTypeExecution,
			namespaceID,
			workflowID,
			request.BaseRunID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID,
			baseRunNextEventID,
		)
	}

	if request.CurrentWorkflowMutation != nil {
		if err := applyWorkflowMutationBatch(batch, shardID, request.CurrentWorkflowMutation); err != nil {
			return err
		}
	} else {
		// check condition without updating anything
		batch.Query(templateCheckWorkflowExecutionQuery,
			currentRunNextEventID,
			d.shardID,
			rowTypeExecution,
			namespaceID,
			workflowID,
			currentRunID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID,
			currentRunNextEventID,
		)
	}

	if err := applyWorkflowSnapshotBatchAsNew(batch, shardID, &request.NewWorkflowSnapshot); err != nil {
		return err
	}

	//Verifies that the RangeID has not changed
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
	defer func() {
		if iter != nil {
			iter.Close()
		}
	}()

	if err != nil {
		if isTimeoutError(err) {
			// Write may have succeeded, but we don't know
			// return this info to the caller so they have the option of trying to find out by executing a read
			return &p.TimeoutError{Msg: fmt.Sprintf("ResetWorkflowExecution timed out. Error: %v", err)}
		} else if isThrottlingError(err) {
			return serviceerror.NewResourceExhausted(fmt.Sprintf("ResetWorkflowExecution operation failed. Error: %v", err))
		}
		return serviceerror.NewInternal(fmt.Sprintf("ResetWorkflowExecution operation failed. Error: %v", err))
	}

	if !applied {
		return d.getExecutionConditionalUpdateFailure(previous, iter, currentRunID, currentRunNextEventID, request.RangeID, currentRunID)
	}

	return nil
}

func (d *cassandraPersistence) ConflictResolveWorkflowExecution(request *p.InternalConflictResolveWorkflowExecutionRequest) error {
	batch := d.session.NewBatch(gocql.LoggedBatch)

	currentWorkflow := request.CurrentWorkflowMutation
	resetWorkflow := request.ResetWorkflowSnapshot
	newWorkflow := request.NewWorkflowSnapshot

	shardID := d.shardID

	namespaceID := resetWorkflow.ExecutionInfo.NamespaceID
	workflowID := resetWorkflow.ExecutionInfo.WorkflowID

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
			resetWorkflow.ExecutionInfo.RunID); err != nil {
			return err
		}

	case p.ConflictResolveWorkflowModeUpdateCurrent:
		executionInfo := resetWorkflow.ExecutionInfo
		startVersion := resetWorkflow.StartVersion
		lastWriteVersion := resetWorkflow.LastWriteVersion
		if newWorkflow != nil {
			executionInfo = newWorkflow.ExecutionInfo
			startVersion = newWorkflow.StartVersion
			lastWriteVersion = newWorkflow.LastWriteVersion
		}
		runID := executionInfo.RunID
		createRequestID := executionInfo.CreateRequestID
		state := executionInfo.State
		closeStatus := executionInfo.CloseStatus

		executionStateDatablob, err := serialization.WorkflowExecutionStateToBlob(&persistenceblobs.WorkflowExecutionState{
			RunId:           primitives.MustParseUUID(runID),
			CreateRequestId: createRequestID,
			State:           int32(state),
			CloseStatus:     int32(closeStatus),
		})

		if err != nil {
			return serviceerror.NewInternal(fmt.Sprintf("ConflictResolveWorkflowExecution operation failed. Error: %v", err))
		}

		if request.CurrentWorkflowCAS != nil {
			prevRunID = request.CurrentWorkflowCAS.PrevRunID
			prevLastWriteVersion := request.CurrentWorkflowCAS.PrevLastWriteVersion
			prevState := request.CurrentWorkflowCAS.PrevState

			batch.Query(templateUpdateCurrentWorkflowExecutionForNewQuery,
				runID,
				executionStateDatablob.Data,
				executionStateDatablob.Encoding,
				startVersion,
				lastWriteVersion,
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
				prevLastWriteVersion,
				prevState,
			)
		} else if currentWorkflow != nil {
			prevRunID = currentWorkflow.ExecutionInfo.RunID

			batch.Query(templateUpdateCurrentWorkflowExecutionQuery,
				runID,
				executionStateDatablob.Data,
				executionStateDatablob.Encoding,
				startVersion,
				lastWriteVersion,
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
			prevRunID = resetWorkflow.ExecutionInfo.RunID

			batch.Query(templateUpdateCurrentWorkflowExecutionQuery,
				runID,
				executionStateDatablob.Data,
				executionStateDatablob.Encoding,
				startVersion,
				lastWriteVersion,
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

	if err := applyWorkflowSnapshotBatchAsReset(batch,
		shardID,
		&resetWorkflow); err != nil {
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
	defer func() {
		if iter != nil {
			iter.Close()
		}
	}()

	if err != nil {
		if isTimeoutError(err) {
			// Write may have succeeded, but we don't know
			// return this info to the caller so they have the option of trying to find out by executing a read
			return &p.TimeoutError{Msg: fmt.Sprintf("ConflictResolveWorkflowExecution timed out. Error: %v", err)}
		} else if isThrottlingError(err) {
			return serviceerror.NewResourceExhausted(fmt.Sprintf("ConflictResolveWorkflowExecution operation failed. Error: %v", err))
		}
		return serviceerror.NewInternal(fmt.Sprintf("ConflictResolveWorkflowExecution operation failed. Error: %v", err))
	}

	if !applied {
		return d.getExecutionConditionalUpdateFailure(previous, iter, resetWorkflow.ExecutionInfo.RunID, request.ResetWorkflowSnapshot.Condition, request.RangeID, prevRunID)
	}
	return nil
}

func (d *cassandraPersistence) getExecutionConditionalUpdateFailure(previous map[string]interface{}, iter *gocql.Iter, requestRunID string, requestCondition int64, requestRangeID int64, requestConditionalRunID string) error {
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

		runID := previous["run_id"].(gocql.UUID).String()

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
			if actualCurrRunID = previous["current_run_id"].(gocql.UUID).String(); actualCurrRunID != requestConditionalRunID {
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
			Msg: fmt.Sprintf("Failed to update mutable state.  Request Condition: %v, Actual Value: %v, Request Current RunID: %v, Actual Value: %v",
				requestCondition, actualNextEventID, requestConditionalRunID, actualCurrRunID),
		}
	}

	if nextEventIDUnmatch {
		return &p.ConditionFailedError{
			Msg: fmt.Sprintf("Failed to update mutable state.  Request Condition: %v, Actual Value: %v, Request Current RunID: %v, Actual Value: %v",
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
		Msg: fmt.Sprintf("Failed to reset mutable state. ShardID: %v, RangeID: %v, Condition: %v, Request Current RunID: %v, columns: (%v)",
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

func (d *cassandraPersistence) DeleteTask(request *p.DeleteTaskRequest) error {
	var namespaceID, workflowID, runID string
	switch request.Type {
	case rowTypeTransferTask:
		namespaceID = rowTypeTransferNamespaceID
		workflowID = rowTypeTransferWorkflowID
		runID = rowTypeTransferRunID

	case rowTypeTimerTask:
		namespaceID = rowTypeTimerNamespaceID
		workflowID = rowTypeTimerWorkflowID
		runID = rowTypeTimerRunID

	case rowTypeReplicationTask:
		namespaceID = rowTypeReplicationNamespaceID
		workflowID = rowTypeReplicationWorkflowID
		runID = rowTypeReplicationRunID

	default:
		return fmt.Errorf("DeleteTask type id is not one of 2 (transfer task), 3 (timer task), 4 (replication task) ")

	}

	query := d.session.Query(templateDeleteWorkflowExecutionMutableStateQuery,
		request.ShardID,
		request.Type,
		namespaceID,
		workflowID,
		runID,
		defaultVisibilityTimestamp,
		request.TaskID)

	err := query.Exec()
	if err != nil {
		if isThrottlingError(err) {
			return serviceerror.NewResourceExhausted(fmt.Sprintf("DeleteTask operation failed. Error: %v", err))
		}
		return serviceerror.NewInternal(fmt.Sprintf("DeleteTask operation failed. Error: %v", err))
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
	if err != nil {
		if isThrottlingError(err) {
			return serviceerror.NewResourceExhausted(fmt.Sprintf("DeleteWorkflowExecution operation failed. Error: %v", err))
		}
		return serviceerror.NewInternal(fmt.Sprintf("DeleteWorkflowExecution operation failed. Error: %v", err))
	}

	return nil
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
	if err != nil {
		if isThrottlingError(err) {
			return serviceerror.NewResourceExhausted(fmt.Sprintf("DeleteWorkflowCurrentRow operation failed. Error: %v", err))
		}
		return serviceerror.NewInternal(fmt.Sprintf("DeleteWorkflowCurrentRow operation failed. Error: %v", err))
	}

	return nil
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
		if err == gocql.ErrNotFound {
			return nil, serviceerror.NewNotFound(fmt.Sprintf("Workflow execution not found.  WorkflowId: %v", request.WorkflowID))
		} else if isThrottlingError(err) {
			return nil, serviceerror.NewResourceExhausted(fmt.Sprintf("GetCurrentExecution operation failed. Error: %v", err))
		}

		return nil, serviceerror.NewInternal(fmt.Sprintf("GetCurrentExecution operation failed. Error: %v", err))
	}

	currentRunID := result["current_run_id"].(gocql.UUID).String()
	executionInfo, err := protoExecutionStateFromRow(result)
	if err != nil {
		return nil, serviceerror.NewInternal(fmt.Sprintf("GetCurrentExecution operation failed. Error: %v", err))
	}
	replicationState := createReplicationState(result["replication_state"].(map[string]interface{}))
	return &p.GetCurrentExecutionResponse{
		RunID:            currentRunID,
		StartRequestID:   executionInfo.CreateRequestId,
		State:            int(executionInfo.State),
		CloseStatus:      int(executionInfo.CloseStatus),
		LastWriteVersion: replicationState.LastWriteVersion,
	}, nil
}

func (d *cassandraPersistence) GetTransferTasks(request *p.GetTransferTasksRequest) (*p.GetTransferTasksResponse, error) {

	// Reading transfer tasks need to be quorum level consistent, otherwise we could loose task
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
			return nil, convertCommonErrors("GetTransferTasks", err)
		}

		response.Tasks = append(response.Tasks, t)
	}
	nextPageToken := iter.PageState()
	response.NextPageToken = make([]byte, len(nextPageToken))
	copy(response.NextPageToken, nextPageToken)

	if err := iter.Close(); err != nil {
		return nil, convertCommonErrors("GetTransferTasks", err)
	}

	return response, nil
}

func (d *cassandraPersistence) GetReplicationTasks(
	request *p.GetReplicationTasksRequest,
) (*p.GetReplicationTasksResponse, error) {

	// Reading replication tasks need to be quorum level consistent, otherwise we could loose task
	query := d.session.Query(templateGetReplicationTasksQuery,
		d.shardID,
		rowTypeReplicationTask,
		rowTypeReplicationNamespaceID,
		rowTypeReplicationWorkflowID,
		rowTypeReplicationRunID,
		defaultVisibilityTimestamp,
		request.ReadLevel,
		request.MaxReadLevel,
	).PageSize(request.BatchSize).PageState(request.NextPageToken)

	return d.populateGetReplicationTasksResponse(query, "GetReplicationTasks")
}

func (d *cassandraPersistence) populateGetReplicationTasksResponse(
	query *gocql.Query, operation string,
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
			return nil, convertCommonErrors(operation, err)
		}

		response.Tasks = append(response.Tasks, t)
	}
	nextPageToken := iter.PageState()
	response.NextPageToken = make([]byte, len(nextPageToken))
	copy(response.NextPageToken, nextPageToken)

	if err := iter.Close(); err != nil {
		return nil, convertCommonErrors(operation, err)
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
	if err != nil {
		if isThrottlingError(err) {
			return serviceerror.NewResourceExhausted(fmt.Sprintf("CompleteTransferTask operation failed. Error: %v", err))
		}
		return serviceerror.NewInternal(fmt.Sprintf("CompleteTransferTask operation failed. Error: %v", err))
	}

	return nil
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
	if err != nil {
		if isThrottlingError(err) {
			return serviceerror.NewResourceExhausted(fmt.Sprintf("RangeCompleteTransferTask operation failed. Error: %v", err))
		}
		return serviceerror.NewInternal(fmt.Sprintf("RangeCompleteTransferTask operation failed. Error: %v", err))
	}

	return nil
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
	if err != nil {
		if isThrottlingError(err) {
			return serviceerror.NewResourceExhausted(fmt.Sprintf("CompleteReplicationTask operation failed. Error: %v", err))
		}
		return serviceerror.NewInternal(fmt.Sprintf("CompleteReplicationTask operation failed. Error: %v", err))
	}

	return nil
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
	if err != nil {
		if isThrottlingError(err) {
			return serviceerror.NewResourceExhausted(fmt.Sprintf("RangeCompleteReplicationTask operation failed. Error: %v", err))
		}
		return serviceerror.NewInternal(fmt.Sprintf("RangeCompleteReplicationTask operation failed. Error: %v", err))
	}

	return nil
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
	if err != nil {
		if isThrottlingError(err) {
			return serviceerror.NewResourceExhausted(fmt.Sprintf("CompleteTimerTask operation failed. Error: %v", err))
		}
		return serviceerror.NewInternal(fmt.Sprintf("CompleteTimerTask operation failed. Error: %v", err))
	}

	return nil
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
	if err != nil {
		if isThrottlingError(err) {
			return serviceerror.NewResourceExhausted(fmt.Sprintf("RangeCompleteTimerTask operation failed. Error: %v", err))
		}
		return serviceerror.NewInternal(fmt.Sprintf("RangeCompleteTimerTask operation failed. Error: %v", err))
	}

	return nil
}

// From TaskManager interface
func (d *cassandraPersistence) LeaseTaskList(request *p.LeaseTaskListRequest) (*p.LeaseTaskListResponse, error) {
	if len(request.TaskList) == 0 {
		return nil, serviceerror.NewInternal(fmt.Sprintf("LeaseTaskList requires non empty task list"))
	}
	now := types.TimestampNow()
	query := d.session.Query(templateGetTaskList,
		request.NamespaceID.Downcast(),
		request.TaskList,
		request.TaskType,
		rowTypeTaskList,
		taskListTaskID,
	)
	var rangeID int64
	var tlBytes []byte
	var tlEncoding string
	err := query.Scan(&rangeID, &tlBytes, &tlEncoding)
	var tl *p.PersistedTaskListInfo
	if err != nil {
		if err == gocql.ErrNotFound { // First time task list is used
			tl = &p.PersistedTaskListInfo{
				Data: &persistenceblobs.TaskListInfo{
					NamespaceId: request.NamespaceID,
					Name:        request.TaskList,
					TaskType:    request.TaskType,
					Kind:        request.TaskListKind,
					AckLevel:    0,
					Expiry:      nil,
					LastUpdated: now,
				},
				RangeID: initialRangeID,
			}
			datablob, err := serialization.TaskListInfoToBlob(tl.Data)

			if err != nil {
				return nil, serviceerror.NewInternal(fmt.Sprintf("LeaseTaskList operation failed during serialization. TaskList: %v, TaskType: %v, Error: %v", request.TaskList, request.TaskType, err))
			}

			query = d.session.Query(templateInsertTaskListQuery,
				request.NamespaceID.Downcast(),
				request.TaskList,
				request.TaskType,
				rowTypeTaskList,
				taskListTaskID,
				initialRangeID,
				datablob.Data,
				datablob.Encoding,
			)
		} else if isThrottlingError(err) {
			return nil, serviceerror.NewResourceExhausted(fmt.Sprintf("LeaseTaskList operation failed. TaskList: %v, TaskType: %v, Error: %v", request.TaskList, request.TaskType, err))
		} else {
			return nil, serviceerror.NewInternal(fmt.Sprintf("LeaseTaskList operation failed. TaskList: %v, TaskType: %v, Error: %v", request.TaskList, request.TaskType, err))
		}
	} else {
		// if request.RangeID is > 0, we are trying to renew an already existing
		// lease on the task list. If request.RangeID=0, we are trying to steal
		// the tasklist from its current owner
		if request.RangeID > 0 && request.RangeID != rangeID {
			return nil, &p.ConditionFailedError{
				Msg: fmt.Sprintf("leaseTaskList:renew failed: taskList:%v, taskListType:%v, haveRangeID:%v, gotRangeID:%v",
					request.TaskList, request.TaskType, request.RangeID, rangeID),
			}
		}

		tli, err := serialization.TaskListInfoFromBlob(tlBytes, tlEncoding)
		if err != nil {
			return nil, serviceerror.NewInternal(fmt.Sprintf("LeaseTaskList operation failed during serialization. TaskList: %v, TaskType: %v, Error: %v", request.TaskList, request.TaskType, err))
		}

		tli.LastUpdated = now
		tl = &p.PersistedTaskListInfo{
			Data:    tli,
			RangeID: rangeID + 1,
		}

		datablob, err := serialization.TaskListInfoToBlob(tl.Data)
		if err != nil {
			return nil, serviceerror.NewInternal(fmt.Sprintf("LeaseTaskList operation failed during serialization. TaskList: %v, TaskType: %v, Error: %v", request.TaskList, request.TaskType, err))
		}

		query = d.session.Query(templateUpdateTaskListQuery,
			rangeID+1,
			datablob.Data,
			datablob.Encoding,
			request.NamespaceID.Downcast(),
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
		if isThrottlingError(err) {
			return nil, serviceerror.NewResourceExhausted(fmt.Sprintf("LeaseTaskList operation failed. Error: %v", err))
		}
		return nil, serviceerror.NewInternal(fmt.Sprintf("LeaseTaskList operation failed. Error : %v", err))
	}
	if !applied {
		previousRangeID := previous["range_id"]
		return nil, &p.ConditionFailedError{
			Msg: fmt.Sprintf("leaseTaskList: taskList:%v, taskListType:%v, haveRangeID:%v, gotRangeID:%v",
				request.TaskList, request.TaskType, rangeID, previousRangeID),
		}
	}

	return &p.LeaseTaskListResponse{TaskListInfo: tl}, nil
}

// From TaskManager interface
func (d *cassandraPersistence) UpdateTaskList(request *p.UpdateTaskListRequest) (*p.UpdateTaskListResponse, error) {
	tli := *request.TaskListInfo
	tli.LastUpdated = types.TimestampNow()
	if tli.Kind == p.TaskListKindSticky { // if task_list is sticky, then update with TTL
		expiry := types.TimestampNow()
		expiry.Seconds += int64(stickyTaskListTTL)

		datablob, err := serialization.TaskListInfoToBlob(&tli)
		if err != nil {
			return nil, convertCommonErrors("UpdateTaskList", err)
		}

		query := d.session.Query(templateUpdateTaskListQueryWithTTL,
			tli.GetNamespaceId(),
			&tli.Name,
			tli.TaskType,
			rowTypeTaskList,
			taskListTaskID,
			request.RangeID,
			datablob.Data,
			datablob.Encoding,
			stickyTaskListTTL,
		)
		err = query.Exec()
		if err != nil {
			return nil, convertCommonErrors("UpdateTaskList", err)
		}

		return &p.UpdateTaskListResponse{}, nil
	}

	tli.LastUpdated = types.TimestampNow()
	datablob, err := serialization.TaskListInfoToBlob(&tli)
	if err != nil {
		return nil, convertCommonErrors("UpdateTaskList", err)
	}

	query := d.session.Query(templateUpdateTaskListQuery,
		request.RangeID,
		datablob.Data,
		datablob.Encoding,
		tli.GetNamespaceId(),
		&tli.Name,
		tli.TaskType,
		rowTypeTaskList,
		taskListTaskID,
		request.RangeID,
	)

	previous := make(map[string]interface{})
	applied, err := query.MapScanCAS(previous)
	if err != nil {
		if isThrottlingError(err) {
			return nil, serviceerror.NewResourceExhausted(fmt.Sprintf("UpdateTaskList operation failed. Error: %v", err))
		}
		return nil, serviceerror.NewInternal(fmt.Sprintf("UpdateTaskList operation failed. Error: %v", err))
	}

	if !applied {
		var columns []string
		for k, v := range previous {
			columns = append(columns, fmt.Sprintf("%s=%v", k, v))
		}

		return nil, &p.ConditionFailedError{
			Msg: fmt.Sprintf("Failed to update task list. name: %v, type: %v, rangeID: %v, columns: (%v)",
				tli.Name, tli.TaskType, request.RangeID, strings.Join(columns, ",")),
		}
	}

	return &p.UpdateTaskListResponse{}, nil
}

func (d *cassandraPersistence) ListTaskList(request *p.ListTaskListRequest) (*p.ListTaskListResponse, error) {
	return nil, serviceerror.NewInternal(fmt.Sprintf("unsupported operation"))
}

func (d *cassandraPersistence) DeleteTaskList(request *p.DeleteTaskListRequest) error {
	query := d.session.Query(templateDeleteTaskListQuery,
		request.TaskList.NamespaceID.Downcast(), request.TaskList.Name, request.TaskList.TaskType, rowTypeTaskList, taskListTaskID, request.RangeID)
	previous := make(map[string]interface{})
	applied, err := query.MapScanCAS(previous)
	if err != nil {
		if isThrottlingError(err) {
			return serviceerror.NewResourceExhausted(fmt.Sprintf("DeleteTaskList operation failed. Error: %v", err))
		}
		return serviceerror.NewInternal(fmt.Sprintf("DeleteTaskList operation failed. Error: %v", err))
	}
	if !applied {
		return &p.ConditionFailedError{
			Msg: fmt.Sprintf("DeleteTaskList operation failed: expected_range_id=%v but found %+v", request.RangeID, previous),
		}
	}
	return nil
}

func MintAllocatedTaskInfo(taskID *int64, info *persistenceblobs.TaskInfo) *persistenceblobs.AllocatedTaskInfo {
	return &persistenceblobs.AllocatedTaskInfo{
		Data:   info,
		TaskId: *taskID,
	}
}

// From TaskManager interface
func (d *cassandraPersistence) CreateTasks(request *p.CreateTasksRequest) (*p.CreateTasksResponse, error) {
	batch := d.session.NewBatch(gocql.LoggedBatch)
	namespaceID := request.TaskListInfo.Data.GetNamespaceId()
	taskList := request.TaskListInfo.Data.Name
	taskListType := request.TaskListInfo.Data.TaskType

	for _, task := range request.Tasks {
		ttl := GetTaskTTL(task.Data)
		datablob, err := serialization.TaskInfoToBlob(task)
		if err != nil {
			return nil, serviceerror.NewInternal(fmt.Sprintf("CreateTasks operation failed during serialization. Error : %v", err))
		}

		if ttl <= 0 {
			batch.Query(templateCreateTaskQuery,
				namespaceID,
				taskList,
				taskListType,
				rowTypeTask,
				task.GetTaskId(),
				datablob.Data,
				datablob.Encoding)
		} else {
			if ttl > maxCassandraTTL {
				ttl = maxCassandraTTL
			}

			batch.Query(templateCreateTaskWithTTLQuery,
				namespaceID,
				taskList,
				taskListType,
				rowTypeTask,
				task.GetTaskId(),
				datablob.Data,
				datablob.Encoding,
				ttl)
		}
	}

	tl := *request.TaskListInfo.Data
	tl.LastUpdated = types.TimestampNow()
	datablob, err := serialization.TaskListInfoToBlob(&tl)

	if err != nil {
		return nil, serviceerror.NewInternal(fmt.Sprintf("CreateTasks operation failed during serialization. Error : %v", err))
	}

	// The following query is used to ensure that range_id didn't change
	batch.Query(templateUpdateTaskListQuery,
		request.TaskListInfo.RangeID,
		datablob.Data,
		datablob.Encoding,
		namespaceID,
		taskList,
		taskListType,
		rowTypeTaskList,
		taskListTaskID,
		request.TaskListInfo.RangeID,
	)

	previous := make(map[string]interface{})
	applied, _, err := d.session.MapExecuteBatchCAS(batch, previous)
	if err != nil {
		if isThrottlingError(err) {
			return nil, serviceerror.NewResourceExhausted(fmt.Sprintf("CreateTasks operation failed. Error: %v", err))
		}
		return nil, serviceerror.NewInternal(fmt.Sprintf("CreateTasks operation failed. Error : %v", err))
	}
	if !applied {
		rangeID := previous["range_id"]
		return nil, &p.ConditionFailedError{
			Msg: fmt.Sprintf("Failed to create task. TaskList: %v, taskListType: %v, rangeID: %v, db rangeID: %v",
				taskList, taskListType, request.TaskListInfo.RangeID, rangeID),
		}
	}

	return &p.CreateTasksResponse{}, nil
}

func GetTaskTTL(task *persistenceblobs.TaskInfo) int64 {
	var ttl int64 = 0
	if task.Expiry != nil {
		// Ignoring error since err is just validating 0 < yyyy < 1000 and nanos < 1e9
		// and we'd have checked this upstream
		expiryGo, _ := types.TimestampFromProto(task.Expiry)
		expiryTtl := int64(math.Ceil(expiryGo.Sub(time.Now()).Seconds()))
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

	// Reading tasklist tasks need to be quorum level consistent, otherwise we could loose task
	query := d.session.Query(templateGetTasksQuery,
		request.NamespaceID.Downcast(),
		request.TaskList,
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
			return nil, convertCommonErrors("GetTasks", err)
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
	tli := request.TaskList
	query := d.session.Query(templateCompleteTaskQuery,
		tli.NamespaceID.Downcast(),
		tli.Name,
		tli.TaskType,
		rowTypeTask,
		request.TaskID)

	err := query.Exec()
	if err != nil {
		if isThrottlingError(err) {
			return serviceerror.NewResourceExhausted(fmt.Sprintf("CompleteTask operation failed. Error: %v", err))
		}
		return serviceerror.NewInternal(fmt.Sprintf("CompleteTask operation failed. Error: %v", err))
	}

	return nil
}

// CompleteTasksLessThan deletes all tasks less than or equal to the given task id. This API ignores the
// Limit request parameter i.e. either all tasks leq the task_id will be deleted or an error will
// be returned to the caller
func (d *cassandraPersistence) CompleteTasksLessThan(request *p.CompleteTasksLessThanRequest) (int, error) {
	query := d.session.Query(templateCompleteTasksLessThanQuery,
		request.NamespaceID.Downcast(), request.TaskListName, request.TaskType, rowTypeTask, request.TaskID)
	err := query.Exec()
	if err != nil {
		if isThrottlingError(err) {
			return 0, serviceerror.NewResourceExhausted(fmt.Sprintf("CompleteTasksLessThan operation failed. Error: %v", err))
		}
		return 0, serviceerror.NewInternal(fmt.Sprintf("CompleteTasksLessThan operation failed. Error: %v", err))
	}
	return p.UnknownNumRowsAffected, nil
}

func (d *cassandraPersistence) GetTimerIndexTasks(request *p.GetTimerIndexTasksRequest) (*p.GetTimerIndexTasksResponse,
	error) {
	// Reading timer tasks need to be quorum level consistent, otherwise we could loose task
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
			return nil, convertCommonErrors("GetTimerIndexTasks", err)
		}

		response.Timers = append(response.Timers, t)
	}
	nextPageToken := iter.PageState()
	response.NextPageToken = make([]byte, len(nextPageToken))
	copy(response.NextPageToken, nextPageToken)

	if err := iter.Close(); err != nil {
		return nil, convertCommonErrors("GetTimerIndexTasks", err)
	}

	return response, nil
}

func (d *cassandraPersistence) PutReplicationTaskToDLQ(request *p.PutReplicationTaskToDLQRequest) error {
	task := request.TaskInfo
	datablob, err := serialization.ReplicationTaskInfoToBlob(task)
	if err != nil {
		return convertCommonErrors("PutReplicationTaskToDLQ", err)
	}

	// Use source cluster name as the workflow id for replication dlq
	query := d.session.Query(templateCreateReplicationTaskQuery,
		d.shardID,
		rowTypeDLQ,
		rowTypeDLQNamespaceID,
		request.SourceClusterName,
		rowTypeDLQRunID,
		datablob.Data,
		datablob.Encoding,
		defaultVisibilityTimestamp,
		task.GetTaskId())

	err = query.Exec()
	if err != nil {
		return convertCommonErrors("PutReplicationTaskToDLQ", err)
	}

	return nil
}

func (d *cassandraPersistence) GetReplicationTasksFromDLQ(
	request *p.GetReplicationTasksFromDLQRequest,
) (*p.GetReplicationTasksFromDLQResponse, error) {
	// Reading replication tasks need to be quorum level consistent, otherwise we could loose task
	query := d.session.Query(templateGetReplicationTasksQuery,
		d.shardID,
		rowTypeDLQ,
		rowTypeDLQNamespaceID,
		request.SourceClusterName,
		rowTypeDLQRunID,
		defaultVisibilityTimestamp,
		request.ReadLevel,
		request.ReadLevel+int64(request.BatchSize),
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
	if err != nil {
		if isThrottlingError(err) {
			return serviceerror.NewResourceExhausted(fmt.Sprintf("DeleteReplicationTaskFromDLQ operation failed. Error: %v", err))
		}
		return serviceerror.NewInternal(fmt.Sprintf("DeleteReplicationTaskFromDLQ operation failed. Error: %v", err))
	}
	return nil
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
	if err != nil {
		if isThrottlingError(err) {
			return serviceerror.NewResourceExhausted(fmt.Sprintf("RangeDeleteReplicationTaskFromDLQ operation failed. Error: %v", err))
		}
		return serviceerror.NewInternal(fmt.Sprintf("RangeDeleteReplicationTaskFromDLQ operation failed. Error: %v", err))
	}
	return nil
}
