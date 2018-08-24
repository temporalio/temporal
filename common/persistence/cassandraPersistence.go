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

package persistence

import (
	"fmt"
	"strings"
	"time"

	"github.com/gocql/gocql"
	"github.com/uber-common/bark"

	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
)

// Guidelines for creating new special UUID constants
// Each UUID should be of the form: E0000000-R000-f000-f000-00000000000x
// Where x is any hexadecimal value, E represents the entity type valid values are:
// E = {DomainID = 1, WorkflowID = 2, RunID = 3}
// R represents row type in executions table, valid values are:
// R = {Shard = 1, Execution = 2, Transfer = 3, Timer = 4, Replication = 5}
const (
	cassandraProtoVersion = 4
	defaultSessionTimeout = 10 * time.Second
	// Special Domains related constants
	emptyDomainID = "10000000-0000-f000-f000-000000000000"
	// Special Run IDs
	emptyRunID                          = "30000000-0000-f000-f000-000000000000"
	permanentRunID                      = "30000000-0000-f000-f000-000000000001"
	transferTaskTypeTransferTargetRunID = "30000000-0000-f000-f000-000000000002"
	// Special Workflow IDs
	transferTaskTransferTargetWorkflowID = "20000000-0000-f000-f000-000000000001"
	// Row Constants for Shard Row
	rowTypeShardDomainID   = "10000000-1000-f000-f000-000000000000"
	rowTypeShardWorkflowID = "20000000-1000-f000-f000-000000000000"
	rowTypeShardRunID      = "30000000-1000-f000-f000-000000000000"
	// Row Constants for Transfer Task Row
	rowTypeTransferDomainID   = "10000000-3000-f000-f000-000000000000"
	rowTypeTransferWorkflowID = "20000000-3000-f000-f000-000000000000"
	rowTypeTransferRunID      = "30000000-3000-f000-f000-000000000000"
	// Row Constants for Timer Task Row
	rowTypeTimerDomainID   = "10000000-4000-f000-f000-000000000000"
	rowTypeTimerWorkflowID = "20000000-4000-f000-f000-000000000000"
	rowTypeTimerRunID      = "30000000-4000-f000-f000-000000000000"
	// Row Constants for Replication Task Row
	rowTypeReplicationDomainID   = "10000000-5000-f000-f000-000000000000"
	rowTypeReplicationWorkflowID = "20000000-5000-f000-f000-000000000000"
	rowTypeReplicationRunID      = "30000000-5000-f000-f000-000000000000"
	// Special TaskId constants
	rowTypeExecutionTaskID  = int64(-10)
	rowTypeShardTaskID      = int64(-11)
	emptyInitiatedID        = int64(-7)
	defaultDeleteTTLSeconds = int64(time.Hour*24*7) / int64(time.Second) // keep deleted records for 7 days

	// minimum current execution retention TTL when current execution is deleted, in seconds
	minCurrentExecutionRetentionTTL = int32(24 * time.Hour / time.Second)

	stickyTaskListTTL = int32(24 * time.Hour / time.Second) // if sticky task_list stopped being updated, remove it in one day
)

const (
	// Row types for table executions
	rowTypeShard = iota
	rowTypeExecution
	rowTypeTransferTask
	rowTypeTimerTask
	rowTypeReplicationTask
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
	templateShardType = `{` +
		`shard_id: ?, ` +
		`owner: ?, ` +
		`range_id: ?, ` +
		`stolen_since_renew: ?, ` +
		`updated_at: ?, ` +
		`replication_ack_level: ?, ` +
		`transfer_ack_level: ?, ` +
		`timer_ack_level: ?, ` +
		`cluster_transfer_ack_level: ?, ` +
		`cluster_timer_ack_level: ?, ` +
		`domain_notification_version: ? ` +
		`}`

	templateWorkflowExecutionType = `{` +
		`domain_id: ?, ` +
		`workflow_id: ?, ` +
		`run_id: ?, ` +
		`parent_domain_id: ?, ` +
		`parent_workflow_id: ?, ` +
		`parent_run_id: ?, ` +
		`initiated_id: ?, ` +
		`completion_event: ?, ` +
		`task_list: ?, ` +
		`workflow_type_name: ?, ` +
		`workflow_timeout: ?, ` +
		`decision_task_timeout: ?, ` +
		`execution_context: ?, ` +
		`state: ?, ` +
		`close_status: ?, ` +
		`last_first_event_id: ?, ` +
		`next_event_id: ?, ` +
		`last_processed_event: ?, ` +
		`start_time: ?, ` +
		`last_updated_time: ?, ` +
		`create_request_id: ?, ` +
		`decision_version: ?, ` +
		`decision_schedule_id: ?, ` +
		`decision_started_id: ?, ` +
		`decision_request_id: ?, ` +
		`decision_timeout: ?, ` +
		`decision_attempt: ?, ` +
		`decision_timestamp: ?, ` +
		`cancel_requested: ?, ` +
		`cancel_request_id: ?, ` +
		`sticky_task_list: ?, ` +
		`sticky_schedule_to_start_timeout: ?,` +
		`client_library_version: ?, ` +
		`client_feature_version: ?, ` +
		`client_impl: ?, ` +
		`attempt: ?, ` +
		`has_retry_policy: ?, ` +
		`init_interval: ?, ` +
		`backoff_coefficient: ?, ` +
		`max_interval: ?, ` +
		`expiration_time: ?, ` +
		`max_attempts: ?, ` +
		`non_retriable_errors: ?` +
		`}`

	templateReplicationStateType = `{` +
		`current_version: ?, ` +
		`start_version: ?, ` +
		`last_write_version: ?, ` +
		`last_write_event_id: ?, ` +
		`last_replication_info: ?` +
		`}`

	templateTransferTaskType = `{` +
		`domain_id: ?, ` +
		`workflow_id: ?, ` +
		`run_id: ?, ` +
		`visibility_ts: ?, ` +
		`task_id: ?, ` +
		`target_domain_id: ?, ` +
		`target_workflow_id: ?, ` +
		`target_run_id: ?, ` +
		`target_child_workflow_only: ?, ` +
		`task_list: ?, ` +
		`type: ?, ` +
		`schedule_id: ?, ` +
		`version: ?` +
		`}`

	templateReplicationTaskType = `{` +
		`domain_id: ?, ` +
		`workflow_id: ?, ` +
		`run_id: ?, ` +
		`task_id: ?, ` +
		`type: ?, ` +
		`first_event_id: ?,` +
		`next_event_id: ?,` +
		`version: ?,` +
		`last_replication_info: ?` +
		`}`

	templateTimerTaskType = `{` +
		`domain_id: ?, ` +
		`workflow_id: ?, ` +
		`run_id: ?, ` +
		`visibility_ts: ?, ` +
		`task_id: ?, ` +
		`type: ?, ` +
		`timeout_type: ?, ` +
		`event_id: ?, ` +
		`schedule_attempt: ?, ` +
		`version: ?` +
		`}`

	templateActivityInfoType = `{` +
		`version: ?,` +
		`schedule_id: ?, ` +
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
		`non_retriable_errors: ?` +
		`}`

	templateTimerInfoType = `{` +
		`version: ?,` +
		`timer_id: ?, ` +
		`started_id: ?, ` +
		`expiry_time: ?, ` +
		`task_id: ?` +
		`}`

	templateChildExecutionInfoType = `{` +
		`version: ?,` +
		`initiated_id: ?, ` +
		`initiated_event: ?, ` +
		`started_id: ?, ` +
		`started_event: ?, ` +
		`create_request_id: ?` +
		`}`

	templateRequestCancelInfoType = `{` +
		`version: ?,` +
		`initiated_id: ?, ` +
		`cancel_request_id: ? ` +
		`}`

	templateSignalInfoType = `{` +
		`version: ?,` +
		`initiated_id: ?, ` +
		`signal_request_id: ?, ` +
		`signal_name: ?, ` +
		`input: ?, ` +
		`control: ?` +
		`}`

	templateBufferedReplicationTaskInfoType = `{` +
		`first_event_id: ?, ` +
		`next_event_id: ?, ` +
		`version: ?, ` +
		`history: ` + templateSerializedEventBatch + `, ` +
		`new_run_history: ` + templateSerializedEventBatch + ` ` +
		`}`

	templateBufferedReplicationTaskInfoNoNewRunHistoryType = `{` +
		`first_event_id: ?, ` +
		`next_event_id: ?, ` +
		`version: ?, ` +
		`history: ` + templateSerializedEventBatch + ` ` +
		`}`

	templateSerializedEventBatch = `{` +
		`encoding_type: ?, ` +
		`version: ?, ` +
		`data: ?` +
		`}`

	templateTaskListType = `{` +
		`domain_id: ?, ` +
		`name: ?, ` +
		`type: ?, ` +
		`ack_level: ?, ` +
		`kind: ? ` +
		`}`

	templateTaskType = `{` +
		`domain_id: ?, ` +
		`workflow_id: ?, ` +
		`run_id: ?, ` +
		`schedule_id: ?` +
		`}`

	templateCreateShardQuery = `INSERT INTO executions (` +
		`shard_id, type, domain_id, workflow_id, run_id, visibility_ts, task_id, shard, range_id)` +
		`VALUES(?, ?, ?, ?, ?, ?, ?, ` + templateShardType + `, ?) IF NOT EXISTS`

	templateGetShardQuery = `SELECT shard ` +
		`FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and domain_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ?`

	templateUpdateShardQuery = `UPDATE executions ` +
		`SET shard = ` + templateShardType + `, range_id = ? ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and domain_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? ` +
		`IF range_id = ?`

	templateUpdateCurrentWorkflowExecutionQuery = `UPDATE executions USING TTL 0 ` +
		`SET current_run_id = ?, execution = {run_id: ?, create_request_id: ?, state: ?, close_status: ?}, replication_state = {start_version: ?}` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and domain_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? ` +
		`IF current_run_id = ? `

	templateCreateCurrentWorkflowExecutionQuery = `INSERT INTO executions (` +
		`shard_id, type, domain_id, workflow_id, run_id, visibility_ts, task_id, current_run_id, execution, replication_state) ` +
		`VALUES(?, ?, ?, ?, ?, ?, ?, ?, {run_id: ?, create_request_id: ?, state: ?, close_status: ?}, {start_version: ?}) IF NOT EXISTS USING TTL 0 `

	templateCreateWorkflowExecutionQuery = `INSERT INTO executions (` +
		`shard_id, domain_id, workflow_id, run_id, type, execution, next_event_id, visibility_ts, task_id) ` +
		`VALUES(?, ?, ?, ?, ?, ` + templateWorkflowExecutionType + `, ?, ?, ?) `

	templateCreateWorkflowExecutionWithReplicationQuery = `INSERT INTO executions (` +
		`shard_id, domain_id, workflow_id, run_id, type, execution, replication_state, next_event_id, visibility_ts, task_id) ` +
		`VALUES(?, ?, ?, ?, ?, ` + templateWorkflowExecutionType + `, ` + templateReplicationStateType + `, ?, ?, ?) `

	templateCreateTransferTaskQuery = `INSERT INTO executions (` +
		`shard_id, type, domain_id, workflow_id, run_id, transfer, visibility_ts, task_id) ` +
		`VALUES(?, ?, ?, ?, ?, ` + templateTransferTaskType + `, ?, ?)`

	templateCreateReplicationTaskQuery = `INSERT INTO executions (` +
		`shard_id, type, domain_id, workflow_id, run_id, replication, visibility_ts, task_id) ` +
		`VALUES(?, ?, ?, ?, ?, ` + templateReplicationTaskType + `, ?, ?)`

	templateCreateTimerTaskQuery = `INSERT INTO executions (` +
		`shard_id, type, domain_id, workflow_id, run_id, timer, visibility_ts, task_id) ` +
		`VALUES(?, ?, ?, ?, ?, ` + templateTimerTaskType + `, ?, ?)`

	templateUpdateLeaseQuery = `UPDATE executions ` +
		`SET range_id = ? ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and domain_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? ` +
		`IF range_id = ?`

	templateGetWorkflowExecutionQuery = `SELECT execution, replication_state, activity_map, timer_map, child_executions_map, request_cancel_map, signal_map, signal_requested, buffered_events_list, buffered_replication_tasks_map ` +
		`FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and domain_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ?`

	templateGetCurrentExecutionQuery = `SELECT current_run_id, execution ` +
		`FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and domain_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ?`

	templateUpdateWorkflowExecutionQuery = `UPDATE executions ` +
		`SET execution = ` + templateWorkflowExecutionType + `, next_event_id = ? ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and domain_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? ` +
		`IF next_event_id = ?`

	templateUpdateWorkflowExecutionWithReplicationQuery = `UPDATE executions ` +
		`SET execution = ` + templateWorkflowExecutionType + `, replication_state = ` + templateReplicationStateType + `, next_event_id = ? ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and domain_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? ` +
		`IF next_event_id = ?`

	templateUpdateActivityInfoQuery = `UPDATE executions ` +
		`SET activity_map[ ? ] =` + templateActivityInfoType + ` ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and domain_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? ` +
		`IF next_event_id = ?`

	templateResetActivityInfoQuery = `UPDATE executions ` +
		`SET activity_map = ?` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and domain_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? ` +
		`IF next_event_id = ?`

	templateUpdateTimerInfoQuery = `UPDATE executions ` +
		`SET timer_map[ ? ] =` + templateTimerInfoType + ` ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and domain_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? ` +
		`IF next_event_id = ?`

	templateResetTimerInfoQuery = `UPDATE executions ` +
		`SET timer_map = ?` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and domain_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? ` +
		`IF next_event_id = ?`

	templateUpdateChildExecutionInfoQuery = `UPDATE executions ` +
		`SET child_executions_map[ ? ] =` + templateChildExecutionInfoType + ` ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and domain_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? ` +
		`IF next_event_id = ?`

	templateResetChildExecutionInfoQuery = `UPDATE executions ` +
		`SET child_executions_map = ?` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and domain_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? ` +
		`IF next_event_id = ?`

	templateUpdateRequestCancelInfoQuery = `UPDATE executions ` +
		`SET request_cancel_map[ ? ] =` + templateRequestCancelInfoType + ` ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and domain_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? ` +
		`IF next_event_id = ?`

	templateResetRequestCancelInfoQuery = `UPDATE executions ` +
		`SET request_cancel_map = ?` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and domain_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? ` +
		`IF next_event_id = ?`

	templateUpdateSignalInfoQuery = `UPDATE executions ` +
		`SET signal_map[ ? ] =` + templateSignalInfoType + ` ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and domain_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? ` +
		`IF next_event_id = ?`

	templateResetSignalInfoQuery = `UPDATE executions ` +
		`SET signal_map = ?` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and domain_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? ` +
		`IF next_event_id = ?`

	templateUpdateSignalRequestedQuery = `UPDATE executions ` +
		`SET signal_requested = signal_requested + ? ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and domain_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? ` +
		`IF next_event_id = ?`

	templateResetSignalRequestedQuery = `UPDATE executions ` +
		`SET signal_requested = ?` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and domain_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? ` +
		`IF next_event_id = ?`

	templateAppendBufferedEventsQuery = `UPDATE executions ` +
		`SET buffered_events_list = buffered_events_list + ? ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and domain_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? ` +
		`IF next_event_id = ?`

	templateDeleteBufferedEventsQuery = `UPDATE executions ` +
		`SET buffered_events_list = [] ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and domain_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? ` +
		`IF next_event_id = ?`

	templateDeleteActivityInfoQuery = `DELETE activity_map[ ? ] ` +
		`FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and domain_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? ` +
		`IF next_event_id = ?`

	templateDeleteTimerInfoQuery = `DELETE timer_map[ ? ] ` +
		`FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and domain_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? ` +
		`IF next_event_id = ?`

	templateDeleteChildExecutionInfoQuery = `DELETE child_executions_map[ ? ] ` +
		`FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and domain_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? ` +
		`IF next_event_id = ?`

	templateDeleteRequestCancelInfoQuery = `DELETE request_cancel_map[ ? ] ` +
		`FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and domain_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? ` +
		`IF next_event_id = ?`

	templateDeleteWorkflowExecutionQueryWithTTL = `INSERT INTO executions ` +
		`(shard_id, type, domain_id, workflow_id, run_id, visibility_ts, task_id, current_run_id, execution, replication_state) ` +
		`VALUES(?, ?, ?, ?, ?, ?, ?, ?, {run_id: ?, create_request_id: ?, state: ?, close_status: ?}, {start_version: ?}) USING TTL ? `

	templateDeleteSignalInfoQuery = `DELETE signal_map[ ? ] ` +
		`FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and domain_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? ` +
		`IF next_event_id = ?`

	templateUpdateBufferedReplicationTasksQuery = `UPDATE executions ` +
		`SET buffered_replication_tasks_map[ ? ] =` + templateBufferedReplicationTaskInfoType + ` ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and domain_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? ` +
		`IF next_event_id = ?`

	templateUpdateBufferedReplicationTasksNoNewRunHistoryQuery = `UPDATE executions ` +
		`SET buffered_replication_tasks_map[ ? ] =` + templateBufferedReplicationTaskInfoNoNewRunHistoryType + ` ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and domain_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? ` +
		`IF next_event_id = ?`

	templateDeleteBufferedReplicationTaskQuery = `DELETE buffered_replication_tasks_map[ ? ] ` +
		`FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and domain_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? ` +
		`IF next_event_id = ?`

	templateClearBufferedReplicationTaskQuery = `UPDATE executions ` +
		`SET buffered_replication_tasks_map = {} ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and domain_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? ` +
		`IF next_event_id = ?`

	templateDeleteWorkflowExecutionMutableStateQuery = `DELETE FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and domain_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? `

	templateDeleteWorkflowExecutionSignalRequestedQuery = `UPDATE executions ` +
		`SET signal_requested = signal_requested - ? ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and domain_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? ` +
		`IF next_event_id = ?`

	templateGetTransferTasksQuery = `SELECT transfer ` +
		`FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and domain_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id > ? ` +
		`and task_id <= ?`

	templateGetReplicationTasksQuery = `SELECT replication ` +
		`FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and domain_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id > ? ` +
		`and task_id <= ?`

	templateCompleteTransferTaskQuery = `DELETE FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and domain_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ?`

	templateRangeCompleteTransferTaskQuery = `DELETE FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and domain_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id > ? ` +
		`and task_id <= ?`

	templateGetTimerTasksQuery = `SELECT timer ` +
		`FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ?` +
		`and domain_id = ? ` +
		`and workflow_id = ?` +
		`and run_id = ?` +
		`and visibility_ts >= ? ` +
		`and visibility_ts < ?`

	templateCompleteTimerTaskQuery = `DELETE FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and domain_id = ? ` +
		`and workflow_id = ?` +
		`and run_id = ?` +
		`and visibility_ts = ? ` +
		`and task_id = ?`

	templateRangeCompleteTimerTaskQuery = `DELETE FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and domain_id = ? ` +
		`and workflow_id = ?` +
		`and run_id = ?` +
		`and visibility_ts >= ? ` +
		`and visibility_ts < ?`

	templateCreateTaskQuery = `INSERT INTO tasks (` +
		`domain_id, task_list_name, task_list_type, type, task_id, task) ` +
		`VALUES(?, ?, ?, ?, ?, ` + templateTaskType + `)`

	templateCreateTaskWithTTLQuery = `INSERT INTO tasks (` +
		`domain_id, task_list_name, task_list_type, type, task_id, task) ` +
		`VALUES(?, ?, ?, ?, ?, ` + templateTaskType + `) USING TTL ?`

	templateGetTasksQuery = `SELECT task_id, task ` +
		`FROM tasks ` +
		`WHERE domain_id = ? ` +
		`and task_list_name = ? ` +
		`and task_list_type = ? ` +
		`and type = ? ` +
		`and task_id > ? ` +
		`and task_id <= ?`

	templateCompleteTaskQuery = `DELETE FROM tasks ` +
		`WHERE domain_id = ? ` +
		`and task_list_name = ? ` +
		`and task_list_type = ? ` +
		`and type = ? ` +
		`and task_id = ?`

	templateGetTaskList = `SELECT ` +
		`range_id, ` +
		`task_list ` +
		`FROM tasks ` +
		`WHERE domain_id = ? ` +
		`and task_list_name = ? ` +
		`and task_list_type = ? ` +
		`and type = ? ` +
		`and task_id = ?`

	templateInsertTaskListQuery = `INSERT INTO tasks (` +
		`domain_id, ` +
		`task_list_name, ` +
		`task_list_type, ` +
		`type, ` +
		`task_id, ` +
		`range_id, ` +
		`task_list ` +
		`) VALUES (?, ?, ?, ?, ?, ?, ` + templateTaskListType + `) IF NOT EXISTS`

	templateUpdateTaskListQuery = `UPDATE tasks SET ` +
		`range_id = ?, ` +
		`task_list = ` + templateTaskListType + " " +
		`WHERE domain_id = ? ` +
		`and task_list_name = ? ` +
		`and task_list_type = ? ` +
		`and type = ? ` +
		`and task_id = ? ` +
		`IF range_id = ?`

	templateUpdateTaskListQueryWithTTL = `INSERT INTO tasks (` +
		`domain_id, ` +
		`task_list_name, ` +
		`task_list_type, ` +
		`type, ` +
		`task_id, ` +
		`range_id, ` +
		`task_list ` +
		`) VALUES (?, ?, ?, ?, ?, ?, ` + templateTaskListType + `) USING TTL ?`
)

var (
	defaultDateTime            = time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)
	defaultVisibilityTimestamp = common.UnixNanoToCQLTimestamp(defaultDateTime.UnixNano())
)

type (
	cassandraPersistence struct {
		session            *gocql.Session
		shardID            int
		currentClusterName string
		logger             bark.Logger
	}
)

// NewCassandraShardPersistence is used to create an instance of ShardManager implementation
func NewCassandraShardPersistence(hosts string, port int, user, password, dc string, keyspace string,
	currentClusterName string, logger bark.Logger) (ShardManager, error) {
	cluster := common.NewCassandraCluster(hosts, port, user, password, dc)
	cluster.Keyspace = keyspace
	cluster.ProtoVersion = cassandraProtoVersion
	cluster.Consistency = gocql.LocalQuorum
	cluster.SerialConsistency = gocql.LocalSerial
	cluster.Timeout = defaultSessionTimeout

	session, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}

	return &cassandraPersistence{shardID: -1, session: session, currentClusterName: currentClusterName, logger: logger}, nil
}

// NewCassandraWorkflowExecutionPersistence is used to create an instance of workflowExecutionManager implementation
func NewCassandraWorkflowExecutionPersistence(shardID int, session *gocql.Session,
	logger bark.Logger) (ExecutionManager, error) {
	return &cassandraPersistence{shardID: shardID, session: session, logger: logger}, nil
}

// NewCassandraTaskPersistence is used to create an instance of TaskManager implementation
func NewCassandraTaskPersistence(hosts string, port int, user, password, dc string, keyspace string,
	logger bark.Logger) (TaskManager, error) {
	cluster := common.NewCassandraCluster(hosts, port, user, password, dc)
	cluster.Keyspace = keyspace
	cluster.ProtoVersion = cassandraProtoVersion
	cluster.Consistency = gocql.LocalQuorum
	cluster.SerialConsistency = gocql.LocalSerial
	cluster.Timeout = defaultSessionTimeout

	session, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}
	return &cassandraPersistence{shardID: -1, session: session, logger: logger}, nil
}

// Close releases the underlying resources held by this object
func (d *cassandraPersistence) Close() {
	if d.session != nil {
		d.session.Close()
	}
}

func (d *cassandraPersistence) CreateShard(request *CreateShardRequest) error {
	cqlNowTimestamp := common.UnixNanoToCQLTimestamp(time.Now().UnixNano())
	shardInfo := request.ShardInfo
	query := d.session.Query(templateCreateShardQuery,
		shardInfo.ShardID,
		rowTypeShard,
		rowTypeShardDomainID,
		rowTypeShardWorkflowID,
		rowTypeShardRunID,
		defaultVisibilityTimestamp,
		rowTypeShardTaskID,
		shardInfo.ShardID,
		shardInfo.Owner,
		shardInfo.RangeID,
		shardInfo.StolenSinceRenew,
		cqlNowTimestamp,
		shardInfo.ReplicationAckLevel,
		shardInfo.TransferAckLevel,
		shardInfo.TimerAckLevel,
		shardInfo.ClusterTransferAckLevel,
		shardInfo.ClusterTimerAckLevel,
		shardInfo.DomainNotificationVersion,
		shardInfo.RangeID)

	previous := make(map[string]interface{})
	applied, err := query.MapScanCAS(previous)
	if err != nil {
		if isThrottlingError(err) {
			return &workflow.ServiceBusyError{
				Message: fmt.Sprintf("CreateShard operation failed. Error: %v", err),
			}
		}
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("CreateShard operation failed. Error : %v", err),
		}
	}

	if !applied {
		shard := previous["shard"].(map[string]interface{})
		return &ShardAlreadyExistError{
			Msg: fmt.Sprintf("Shard already exists in executions table.  ShardId: %v, RangeId: %v",
				shard["shard_id"], shard["range_id"]),
		}
	}

	return nil
}

func (d *cassandraPersistence) GetShard(request *GetShardRequest) (*GetShardResponse, error) {
	shardID := request.ShardID
	query := d.session.Query(templateGetShardQuery,
		shardID,
		rowTypeShard,
		rowTypeShardDomainID,
		rowTypeShardWorkflowID,
		rowTypeShardRunID,
		defaultVisibilityTimestamp,
		rowTypeShardTaskID)

	result := make(map[string]interface{})
	if err := query.MapScan(result); err != nil {
		if err == gocql.ErrNotFound {
			return nil, &workflow.EntityNotExistsError{
				Message: fmt.Sprintf("Shard not found.  ShardId: %v", shardID),
			}
		} else if isThrottlingError(err) {
			return nil, &workflow.ServiceBusyError{
				Message: fmt.Sprintf("GetShard operation failed. Error: %v", err),
			}
		}

		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("GetShard operation failed. Error: %v", err),
		}
	}

	info := createShardInfo(d.currentClusterName, result["shard"].(map[string]interface{}))

	return &GetShardResponse{ShardInfo: info}, nil
}

func (d *cassandraPersistence) UpdateShard(request *UpdateShardRequest) error {
	cqlNowTimestamp := common.UnixNanoToCQLTimestamp(time.Now().UnixNano())
	shardInfo := request.ShardInfo

	query := d.session.Query(templateUpdateShardQuery,
		shardInfo.ShardID,
		shardInfo.Owner,
		shardInfo.RangeID,
		shardInfo.StolenSinceRenew,
		cqlNowTimestamp,
		shardInfo.ReplicationAckLevel,
		shardInfo.TransferAckLevel,
		shardInfo.TimerAckLevel,
		shardInfo.ClusterTransferAckLevel,
		shardInfo.ClusterTimerAckLevel,
		shardInfo.DomainNotificationVersion,
		shardInfo.RangeID,
		shardInfo.ShardID,
		rowTypeShard,
		rowTypeShardDomainID,
		rowTypeShardWorkflowID,
		rowTypeShardRunID,
		defaultVisibilityTimestamp,
		rowTypeShardTaskID,
		request.PreviousRangeID)

	previous := make(map[string]interface{})
	applied, err := query.MapScanCAS(previous)
	if err != nil {
		if isThrottlingError(err) {
			return &workflow.ServiceBusyError{
				Message: fmt.Sprintf("UpdateShard operation failed. Error: %v", err),
			}
		}
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("UpdateShard operation failed. Error: %v", err),
		}
	}

	if !applied {
		var columns []string
		for k, v := range previous {
			columns = append(columns, fmt.Sprintf("%s=%v", k, v))
		}

		return &ShardOwnershipLostError{
			ShardID: d.shardID,
			Msg: fmt.Sprintf("Failed to update shard.  previous_range_id: %v, columns: (%v)",
				request.PreviousRangeID, strings.Join(columns, ",")),
		}
	}

	return nil
}

func (d *cassandraPersistence) CreateWorkflowExecution(request *CreateWorkflowExecutionRequest) (
	*CreateWorkflowExecutionResponse, error) {
	cqlNowTimestamp := common.UnixNanoToCQLTimestamp(time.Now().UnixNano())
	batch := d.session.NewBatch(gocql.LoggedBatch)

	d.CreateWorkflowExecutionWithinBatch(request, batch, cqlNowTimestamp)

	d.createTransferTasks(batch, request.TransferTasks, request.DomainID, *request.Execution.WorkflowId,
		*request.Execution.RunId)
	d.createReplicationTasks(batch, request.ReplicationTasks, request.DomainID, *request.Execution.WorkflowId,
		*request.Execution.RunId)
	d.createTimerTasks(batch, request.TimerTasks, nil, request.DomainID, *request.Execution.WorkflowId,
		*request.Execution.RunId, cqlNowTimestamp)

	batch.Query(templateUpdateLeaseQuery,
		request.RangeID,
		d.shardID,
		rowTypeShard,
		rowTypeShardDomainID,
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
			return nil, &TimeoutError{Msg: fmt.Sprintf("CreateWorkflowExecution timed out. Error: %v", err)}
		} else if isThrottlingError(err) {
			return nil, &workflow.ServiceBusyError{
				Message: fmt.Sprintf("CreateWorkflowExecution operation failed. Error: %v", err),
			}
		}
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("CreateWorkflowExecution operation failed. Error: %v", err),
		}

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

			if rowType == rowTypeShard {
				if rangeID, ok := previous["range_id"].(int64); ok && rangeID != request.RangeID {
					// CreateWorkflowExecution failed because rangeID was modified
					return nil, &ShardOwnershipLostError{
						ShardID: d.shardID,
						Msg: fmt.Sprintf("Failed to create workflow execution.  Request RangeID: %v, Actual RangeID: %v",
							request.RangeID, rangeID),
					}
				}

			} else if rowType == rowTypeExecution {
				var columns []string
				for k, v := range previous {
					columns = append(columns, fmt.Sprintf("%s=%v", k, v))
				}

				if execution, ok := previous["execution"].(map[string]interface{}); ok {
					// CreateWorkflowExecution failed because it already exists
					executionInfo := createWorkflowExecutionInfo(execution)

					startVersion := common.EmptyVersion
					replicationState := createReplicationState(previous["replication_state"].(map[string]interface{}))
					if replicationState != nil {
						startVersion = replicationState.StartVersion
					}

					msg := fmt.Sprintf("Workflow execution already running. WorkflowId: %v, RunId: %v, rangeID: %v, columns: (%v)",
						request.Execution.GetWorkflowId(), executionInfo.RunID, request.RangeID, strings.Join(columns, ","))
					return nil, &WorkflowExecutionAlreadyStartedError{
						Msg:            msg,
						StartRequestID: executionInfo.CreateRequestID,
						RunID:          executionInfo.RunID,
						State:          executionInfo.State,
						CloseStatus:    executionInfo.CloseStatus,
						StartVersion:   startVersion,
					}
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
		return nil, &ShardOwnershipLostError{
			ShardID: d.shardID,
			Msg: fmt.Sprintf("Failed to create workflow execution.  Request RangeID: %v, columns: (%v)",
				request.RangeID, strings.Join(columns, ",")),
		}
	}

	return &CreateWorkflowExecutionResponse{}, nil
}

func (d *cassandraPersistence) CreateWorkflowExecutionWithinBatch(request *CreateWorkflowExecutionRequest,
	batch *gocql.Batch, cqlNowTimestamp int64) {

	parentDomainID := emptyDomainID
	parentWorkflowID := ""
	parentRunID := emptyRunID
	initiatedID := emptyInitiatedID
	state := WorkflowStateRunning
	closeStatus := WorkflowCloseStatusNone
	if request.ParentExecution != nil {
		parentDomainID = request.ParentDomainID
		parentWorkflowID = *request.ParentExecution.WorkflowId
		parentRunID = *request.ParentExecution.RunId
		initiatedID = request.InitiatedID
		state = WorkflowStateCreated
	}

	startVersion := common.EmptyVersion
	if request.ReplicationState != nil {
		startVersion = request.ReplicationState.StartVersion
	}
	if request.ContinueAsNew {
		batch.Query(templateUpdateCurrentWorkflowExecutionQuery,
			*request.Execution.RunId,
			*request.Execution.RunId,
			request.RequestID,
			state,
			closeStatus,
			startVersion,
			d.shardID,
			rowTypeExecution,
			request.DomainID,
			*request.Execution.WorkflowId,
			permanentRunID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID,
			request.PreviousRunID,
		)
	} else {
		batch.Query(templateCreateCurrentWorkflowExecutionQuery,
			d.shardID,
			rowTypeExecution,
			request.DomainID,
			*request.Execution.WorkflowId,
			permanentRunID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID,
			*request.Execution.RunId,
			*request.Execution.RunId,
			request.RequestID,
			state,
			closeStatus,
			startVersion,
		)
	}

	if request.ReplicationState == nil {
		// Cross DC feature is currently disabled so we will be creating workflow executions without replication state
		batch.Query(templateCreateWorkflowExecutionQuery,
			d.shardID,
			request.DomainID,
			*request.Execution.WorkflowId,
			*request.Execution.RunId,
			rowTypeExecution,
			request.DomainID,
			*request.Execution.WorkflowId,
			*request.Execution.RunId,
			parentDomainID,
			parentWorkflowID,
			parentRunID,
			initiatedID,
			nil,
			request.TaskList,
			request.WorkflowTypeName,
			request.WorkflowTimeout,
			request.DecisionTimeoutValue,
			request.ExecutionContext,
			WorkflowStateCreated,
			WorkflowCloseStatusNone,
			common.FirstEventID,
			request.NextEventID,
			request.LastProcessedEvent,
			cqlNowTimestamp,
			cqlNowTimestamp,
			request.RequestID,
			request.DecisionVersion,
			request.DecisionScheduleID,
			request.DecisionStartedID,
			"", // Decision Start Request ID
			request.DecisionStartToCloseTimeout,
			0,
			0,
			false,
			"",
			"", // sticky_task_list (no sticky tasklist for new workflow execution)
			0,  // sticky_schedule_to_start_timeout
			"", // client_library_version
			"", // client_feature_version
			"", // client_impl
			request.Attempt,
			request.HasRetryPolicy,
			request.InitialInterval,
			request.BackoffCoefficient,
			request.MaximumInterval,
			request.ExpirationTime,
			request.MaximumAttempts,
			request.NonRetriableErrors,
			request.NextEventID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID)
	} else {
		lastReplicationInfo := make(map[string]map[string]interface{})
		for k, v := range request.ReplicationState.LastReplicationInfo {
			lastReplicationInfo[k] = createReplicationInfoMap(v)
		}

		batch.Query(templateCreateWorkflowExecutionWithReplicationQuery,
			d.shardID,
			request.DomainID,
			*request.Execution.WorkflowId,
			*request.Execution.RunId,
			rowTypeExecution,
			request.DomainID,
			*request.Execution.WorkflowId,
			*request.Execution.RunId,
			parentDomainID,
			parentWorkflowID,
			parentRunID,
			initiatedID,
			nil,
			request.TaskList,
			request.WorkflowTypeName,
			request.WorkflowTimeout,
			request.DecisionTimeoutValue,
			request.ExecutionContext,
			WorkflowStateCreated,
			WorkflowCloseStatusNone,
			common.FirstEventID,
			request.NextEventID,
			request.LastProcessedEvent,
			cqlNowTimestamp,
			cqlNowTimestamp,
			request.RequestID,
			request.DecisionVersion,
			request.DecisionScheduleID,
			request.DecisionStartedID,
			"", // Decision Start Request ID
			request.DecisionStartToCloseTimeout,
			0,
			0,
			false,
			"",
			"", // sticky_task_list (no sticky tasklist for new workflow execution)
			0,  // sticky_schedule_to_start_timeout
			"", // client_library_version
			"", // client_feature_version
			"", // client_impl
			request.Attempt,
			request.HasRetryPolicy,
			request.InitialInterval,
			request.BackoffCoefficient,
			request.MaximumInterval,
			request.ExpirationTime,
			request.MaximumAttempts,
			request.NonRetriableErrors,
			request.ReplicationState.CurrentVersion,
			request.ReplicationState.StartVersion,
			request.ReplicationState.LastWriteVersion,
			request.ReplicationState.LastWriteEventID,
			lastReplicationInfo,
			request.NextEventID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID)
	}
}

func (d *cassandraPersistence) GetWorkflowExecution(request *GetWorkflowExecutionRequest) (
	*GetWorkflowExecutionResponse, error) {
	execution := request.Execution
	query := d.session.Query(templateGetWorkflowExecutionQuery,
		d.shardID,
		rowTypeExecution,
		request.DomainID,
		*execution.WorkflowId,
		*execution.RunId,
		defaultVisibilityTimestamp,
		rowTypeExecutionTaskID)

	result := make(map[string]interface{})
	if err := query.MapScan(result); err != nil {
		if err == gocql.ErrNotFound {
			return nil, &workflow.EntityNotExistsError{
				Message: fmt.Sprintf("Workflow execution not found.  WorkflowId: %v, RunId: %v",
					*execution.WorkflowId, *execution.RunId),
			}
		} else if isThrottlingError(err) {
			return nil, &workflow.ServiceBusyError{
				Message: fmt.Sprintf("GetWorkflowExecution operation failed. Error: %v", err),
			}
		}

		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("GetWorkflowExecution operation failed. Error: %v", err),
		}
	}

	state := &WorkflowMutableState{}
	info := createWorkflowExecutionInfo(result["execution"].(map[string]interface{}))
	state.ExecutionInfo = info

	replicationState := createReplicationState(result["replication_state"].(map[string]interface{}))
	state.ReplicationState = replicationState

	activityInfos := make(map[int64]*ActivityInfo)
	aMap := result["activity_map"].(map[int64]map[string]interface{})
	for key, value := range aMap {
		info := createActivityInfo(value)
		activityInfos[key] = info
	}
	state.ActivitInfos = activityInfos

	timerInfos := make(map[string]*TimerInfo)
	tMap := result["timer_map"].(map[string]map[string]interface{})
	for key, value := range tMap {
		info := createTimerInfo(value)
		timerInfos[key] = info
	}
	state.TimerInfos = timerInfos

	childExecutionInfos := make(map[int64]*ChildExecutionInfo)
	cMap := result["child_executions_map"].(map[int64]map[string]interface{})
	for key, value := range cMap {
		info := createChildExecutionInfo(value)
		childExecutionInfos[key] = info
	}
	state.ChildExecutionInfos = childExecutionInfos

	requestCancelInfos := make(map[int64]*RequestCancelInfo)
	rMap := result["request_cancel_map"].(map[int64]map[string]interface{})
	for key, value := range rMap {
		info := createRequestCancelInfo(value)
		requestCancelInfos[key] = info
	}
	state.RequestCancelInfos = requestCancelInfos

	signalInfos := make(map[int64]*SignalInfo)
	sMap := result["signal_map"].(map[int64]map[string]interface{})
	for key, value := range sMap {
		info := createSignalInfo(value)
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
	bufferedEvents := make([]*SerializedHistoryEventBatch, 0, len(eList))
	for _, v := range eList {
		eventBatch := createSerializedHistoryEventBatch(v)
		bufferedEvents = append(bufferedEvents, eventBatch)
	}
	state.BufferedEvents = bufferedEvents

	bufferedReplicationTasks := make(map[int64]*BufferedReplicationTask)
	bufferedRTMap := result["buffered_replication_tasks_map"].(map[int64]map[string]interface{})
	for k, v := range bufferedRTMap {
		info := createBufferedReplicationTaskInfo(v)
		bufferedReplicationTasks[k] = info
	}
	state.BufferedReplicationTasks = bufferedReplicationTasks

	return &GetWorkflowExecutionResponse{State: state}, nil
}

func (d *cassandraPersistence) UpdateWorkflowExecution(request *UpdateWorkflowExecutionRequest) error {
	batch := d.session.NewBatch(gocql.LoggedBatch)
	cqlNowTimestamp := common.UnixNanoToCQLTimestamp(time.Now().UnixNano())
	executionInfo := request.ExecutionInfo
	replicationState := request.ReplicationState

	if replicationState == nil {
		// Updates will be called with null ReplicationState while the feature is disabled
		batch.Query(templateUpdateWorkflowExecutionQuery,
			executionInfo.DomainID,
			executionInfo.WorkflowID,
			executionInfo.RunID,
			executionInfo.ParentDomainID,
			executionInfo.ParentWorkflowID,
			executionInfo.ParentRunID,
			executionInfo.InitiatedID,
			executionInfo.CompletionEvent,
			executionInfo.TaskList,
			executionInfo.WorkflowTypeName,
			executionInfo.WorkflowTimeout,
			executionInfo.DecisionTimeoutValue,
			executionInfo.ExecutionContext,
			executionInfo.State,
			executionInfo.CloseStatus,
			executionInfo.LastFirstEventID,
			executionInfo.NextEventID,
			executionInfo.LastProcessedEvent,
			executionInfo.StartTimestamp,
			cqlNowTimestamp,
			executionInfo.CreateRequestID,
			executionInfo.DecisionVersion,
			executionInfo.DecisionScheduleID,
			executionInfo.DecisionStartedID,
			executionInfo.DecisionRequestID,
			executionInfo.DecisionTimeout,
			executionInfo.DecisionAttempt,
			executionInfo.DecisionTimestamp,
			executionInfo.CancelRequested,
			executionInfo.CancelRequestID,
			executionInfo.StickyTaskList,
			executionInfo.StickyScheduleToStartTimeout,
			executionInfo.ClientLibraryVersion,
			executionInfo.ClientFeatureVersion,
			executionInfo.ClientImpl,
			executionInfo.Attempt,
			executionInfo.HasRetryPolicy,
			executionInfo.InitialInterval,
			executionInfo.BackoffCoefficient,
			executionInfo.MaximumInterval,
			executionInfo.ExpirationTime,
			executionInfo.MaximumAttempts,
			executionInfo.NonRetriableErrors,
			executionInfo.NextEventID,
			d.shardID,
			rowTypeExecution,
			executionInfo.DomainID,
			executionInfo.WorkflowID,
			executionInfo.RunID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID,
			request.Condition)
	} else {
		lastReplicationInfo := make(map[string]map[string]interface{})
		for k, v := range replicationState.LastReplicationInfo {
			lastReplicationInfo[k] = createReplicationInfoMap(v)
		}

		batch.Query(templateUpdateWorkflowExecutionWithReplicationQuery,
			executionInfo.DomainID,
			executionInfo.WorkflowID,
			executionInfo.RunID,
			executionInfo.ParentDomainID,
			executionInfo.ParentWorkflowID,
			executionInfo.ParentRunID,
			executionInfo.InitiatedID,
			executionInfo.CompletionEvent,
			executionInfo.TaskList,
			executionInfo.WorkflowTypeName,
			executionInfo.WorkflowTimeout,
			executionInfo.DecisionTimeoutValue,
			executionInfo.ExecutionContext,
			executionInfo.State,
			executionInfo.CloseStatus,
			executionInfo.LastFirstEventID,
			executionInfo.NextEventID,
			executionInfo.LastProcessedEvent,
			executionInfo.StartTimestamp,
			cqlNowTimestamp,
			executionInfo.CreateRequestID,
			executionInfo.DecisionVersion,
			executionInfo.DecisionScheduleID,
			executionInfo.DecisionStartedID,
			executionInfo.DecisionRequestID,
			executionInfo.DecisionTimeout,
			executionInfo.DecisionAttempt,
			executionInfo.DecisionTimestamp,
			executionInfo.CancelRequested,
			executionInfo.CancelRequestID,
			executionInfo.StickyTaskList,
			executionInfo.StickyScheduleToStartTimeout,
			executionInfo.ClientLibraryVersion,
			executionInfo.ClientFeatureVersion,
			executionInfo.ClientImpl,
			executionInfo.Attempt,
			executionInfo.HasRetryPolicy,
			executionInfo.InitialInterval,
			executionInfo.BackoffCoefficient,
			executionInfo.MaximumInterval,
			executionInfo.ExpirationTime,
			executionInfo.MaximumAttempts,
			executionInfo.NonRetriableErrors,
			replicationState.CurrentVersion,
			replicationState.StartVersion,
			replicationState.LastWriteVersion,
			replicationState.LastWriteEventID,
			lastReplicationInfo,
			executionInfo.NextEventID,
			d.shardID,
			rowTypeExecution,
			executionInfo.DomainID,
			executionInfo.WorkflowID,
			executionInfo.RunID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID,
			request.Condition)
	}

	d.createTransferTasks(batch, request.TransferTasks, executionInfo.DomainID, executionInfo.WorkflowID,
		executionInfo.RunID)

	d.createReplicationTasks(batch, request.ReplicationTasks, executionInfo.DomainID, executionInfo.WorkflowID,
		executionInfo.RunID)

	d.createTimerTasks(batch, request.TimerTasks, request.DeleteTimerTask, request.ExecutionInfo.DomainID,
		executionInfo.WorkflowID, executionInfo.RunID, cqlNowTimestamp)

	d.updateActivityInfos(batch, request.UpsertActivityInfos, request.DeleteActivityInfos, executionInfo.DomainID,
		executionInfo.WorkflowID, executionInfo.RunID, request.Condition, request.RangeID)

	d.updateTimerInfos(batch, request.UpserTimerInfos, request.DeleteTimerInfos, executionInfo.DomainID,
		executionInfo.WorkflowID, executionInfo.RunID, request.Condition, request.RangeID)

	d.updateChildExecutionInfos(batch, request.UpsertChildExecutionInfos, request.DeleteChildExecutionInfo,
		executionInfo.DomainID, executionInfo.WorkflowID, executionInfo.RunID, request.Condition, request.RangeID)

	d.updateRequestCancelInfos(batch, request.UpsertRequestCancelInfos, request.DeleteRequestCancelInfo,
		executionInfo.DomainID, executionInfo.WorkflowID, executionInfo.RunID, request.Condition, request.RangeID)

	d.updateSignalInfos(batch, request.UpsertSignalInfos, request.DeleteSignalInfo,
		executionInfo.DomainID, executionInfo.WorkflowID, executionInfo.RunID, request.Condition, request.RangeID)

	d.updateSignalsRequested(batch, request.UpsertSignalRequestedIDs, request.DeleteSignalRequestedID,
		executionInfo.DomainID, executionInfo.WorkflowID, executionInfo.RunID, request.Condition, request.RangeID)

	d.updateBufferedEvents(batch, request.NewBufferedEvents, request.ClearBufferedEvents,
		executionInfo.DomainID, executionInfo.WorkflowID, executionInfo.RunID, request.Condition, request.RangeID)

	d.updateBufferedReplicationTasks(batch, request.NewBufferedReplicationTask, request.DeleteBufferedReplicationTask,
		executionInfo.DomainID, executionInfo.WorkflowID, executionInfo.RunID, request.Condition, request.RangeID)

	if request.ContinueAsNew != nil {
		startReq := request.ContinueAsNew
		d.CreateWorkflowExecutionWithinBatch(startReq, batch, cqlNowTimestamp)
		d.createTransferTasks(batch, startReq.TransferTasks, startReq.DomainID, startReq.Execution.GetWorkflowId(),
			startReq.Execution.GetRunId())
		d.createTimerTasks(batch, startReq.TimerTasks, nil, startReq.DomainID, startReq.Execution.GetWorkflowId(),
			startReq.Execution.GetRunId(), cqlNowTimestamp)
	} else if request.FinishExecution {
		retentionInSeconds := request.FinishedExecutionTTL
		if retentionInSeconds <= 0 {
			retentionInSeconds = minCurrentExecutionRetentionTTL
		}

		startVersion := common.EmptyVersion
		if request.ReplicationState != nil {
			startVersion = request.ReplicationState.StartVersion
		}

		// Delete WorkflowExecution row representing current execution, by using a TTL
		batch.Query(templateDeleteWorkflowExecutionQueryWithTTL,
			d.shardID,
			rowTypeExecution,
			executionInfo.DomainID,
			executionInfo.WorkflowID,
			permanentRunID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID,
			executionInfo.RunID,
			executionInfo.RunID,
			executionInfo.CreateRequestID,
			executionInfo.State,
			executionInfo.CloseStatus,
			startVersion,
			retentionInSeconds,
		)
	}

	// Verifies that the RangeID has not changed
	batch.Query(templateUpdateLeaseQuery,
		request.RangeID,
		d.shardID,
		rowTypeShard,
		rowTypeShardDomainID,
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
			return &TimeoutError{Msg: fmt.Sprintf("UpdateWorkflowExecution timed out. Error: %v", err)}
		} else if isThrottlingError(err) {
			return &workflow.ServiceBusyError{
				Message: fmt.Sprintf("UpdateWorkflowExecution operation failed. Error: %v", err),
			}
		}
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("UpdateWorkflowExecution operation failed. Error: %v", err),
		}
	}

	if !applied {
		// There can be two reasons why the query does not get applied. Either the RangeID has changed, or
		// the next_event_id check failed. Check the row info returned by Cassandra to figure out which one it is.
	GetFailureReasonLoop:
		for {
			rowType, ok := previous["type"].(int)
			if !ok {
				// This should never happen, as all our rows have the type field.
				break GetFailureReasonLoop
			}

			if rowType == rowTypeShard {
				if rangeID, ok := previous["range_id"].(int64); ok && rangeID != request.RangeID {
					// UpdateWorkflowExecution failed because rangeID was modified
					return &ShardOwnershipLostError{
						ShardID: d.shardID,
						Msg: fmt.Sprintf("Failed to update workflow execution.  Request RangeID: %v, Actual RangeID: %v",
							request.RangeID, rangeID),
					}
				}
			} else {
				if nextEventID, ok := previous["next_event_id"].(int64); ok && nextEventID != request.Condition {
					// CreateWorkflowExecution failed because next event ID is unexpected
					return &ConditionFailedError{
						Msg: fmt.Sprintf("Failed to update workflow execution.  Request Condition: %v, Actual Value: %v",
							request.Condition, nextEventID),
					}
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

		return &ShardOwnershipLostError{
			ShardID: d.shardID,
			Msg: fmt.Sprintf("Failed to update workflow execution.  RangeID: %v, Condition: %v, columns: (%v)",
				request.RangeID, request.Condition, strings.Join(columns, ",")),
		}
	}

	return nil
}

func (d *cassandraPersistence) ResetMutableState(request *ResetMutableStateRequest) error {
	batch := d.session.NewBatch(gocql.LoggedBatch)
	cqlNowTimestamp := common.UnixNanoToCQLTimestamp(time.Now().UnixNano())
	executionInfo := request.ExecutionInfo
	replicationState := request.ReplicationState

	lastReplicationInfo := make(map[string]map[string]interface{})
	for k, v := range replicationState.LastReplicationInfo {
		lastReplicationInfo[k] = createReplicationInfoMap(v)
	}

	parentDomainID := emptyDomainID
	if executionInfo.ParentDomainID != "" {
		parentDomainID = executionInfo.ParentDomainID
	}
	parentRunID := emptyRunID
	if executionInfo.ParentRunID != "" {
		parentRunID = executionInfo.ParentRunID
	}

	batch.Query(templateUpdateCurrentWorkflowExecutionQuery,
		executionInfo.RunID,
		executionInfo.RunID,
		executionInfo.CreateRequestID,
		executionInfo.State,
		executionInfo.CloseStatus,
		replicationState.StartVersion,
		d.shardID,
		rowTypeExecution,
		executionInfo.DomainID,
		executionInfo.WorkflowID,
		permanentRunID,
		defaultVisibilityTimestamp,
		rowTypeExecutionTaskID,
		request.PrevRunID,
	)

	batch.Query(templateUpdateWorkflowExecutionWithReplicationQuery,
		executionInfo.DomainID,
		executionInfo.WorkflowID,
		executionInfo.RunID,
		parentDomainID,
		executionInfo.ParentWorkflowID,
		parentRunID,
		executionInfo.InitiatedID,
		executionInfo.CompletionEvent,
		executionInfo.TaskList,
		executionInfo.WorkflowTypeName,
		executionInfo.WorkflowTimeout,
		executionInfo.DecisionTimeoutValue,
		executionInfo.ExecutionContext,
		executionInfo.State,
		executionInfo.CloseStatus,
		executionInfo.LastFirstEventID,
		executionInfo.NextEventID,
		executionInfo.LastProcessedEvent,
		executionInfo.StartTimestamp,
		cqlNowTimestamp,
		executionInfo.CreateRequestID,
		executionInfo.DecisionVersion,
		executionInfo.DecisionScheduleID,
		executionInfo.DecisionStartedID,
		executionInfo.DecisionRequestID,
		executionInfo.DecisionTimeout,
		executionInfo.DecisionAttempt,
		executionInfo.DecisionTimestamp,
		executionInfo.CancelRequested,
		executionInfo.CancelRequestID,
		executionInfo.StickyTaskList,
		executionInfo.StickyScheduleToStartTimeout,
		executionInfo.ClientLibraryVersion,
		executionInfo.ClientFeatureVersion,
		executionInfo.ClientImpl,
		executionInfo.Attempt,
		executionInfo.HasRetryPolicy,
		executionInfo.InitialInterval,
		executionInfo.BackoffCoefficient,
		executionInfo.MaximumInterval,
		executionInfo.ExpirationTime,
		executionInfo.MaximumAttempts,
		executionInfo.NonRetriableErrors,
		replicationState.CurrentVersion,
		replicationState.StartVersion,
		replicationState.LastWriteVersion,
		replicationState.LastWriteEventID,
		lastReplicationInfo,
		executionInfo.NextEventID,
		d.shardID,
		rowTypeExecution,
		executionInfo.DomainID,
		executionInfo.WorkflowID,
		executionInfo.RunID,
		defaultVisibilityTimestamp,
		rowTypeExecutionTaskID,
		request.Condition)

	d.resetActivityInfos(batch, request.InsertActivityInfos, executionInfo.DomainID, executionInfo.WorkflowID,
		executionInfo.RunID, request.Condition)

	d.resetTimerInfos(batch, request.InsertTimerInfos, executionInfo.DomainID, executionInfo.WorkflowID,
		executionInfo.RunID, request.Condition)

	d.resetChildExecutionInfos(batch, request.InsertChildExecutionInfos, executionInfo.DomainID, executionInfo.WorkflowID,
		executionInfo.RunID, request.Condition)

	d.resetRequestCancelInfos(batch, request.InsertRequestCancelInfos, executionInfo.DomainID, executionInfo.WorkflowID,
		executionInfo.RunID, request.Condition)

	d.resetSignalInfos(batch, request.InsertSignalInfos, executionInfo.DomainID, executionInfo.WorkflowID,
		executionInfo.RunID, request.Condition)

	d.resetSignalRequested(batch, request.InsertSignalRequestedIDs, executionInfo.DomainID, executionInfo.WorkflowID,
		executionInfo.RunID, request.Condition)

	d.resetBufferedEvents(batch, executionInfo.DomainID, executionInfo.WorkflowID, executionInfo.RunID, request.Condition)

	// Verifies that the RangeID has not changed
	batch.Query(templateUpdateLeaseQuery,
		request.RangeID,
		d.shardID,
		rowTypeShard,
		rowTypeShardDomainID,
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
			return &TimeoutError{Msg: fmt.Sprintf("ResetMutableState timed out. Error: %v", err)}
		} else if isThrottlingError(err) {
			return &workflow.ServiceBusyError{
				Message: fmt.Sprintf("ResetMutableState operation failed. Error: %v", err),
			}
		}
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("ResetMutableState operation failed. Error: %v", err),
		}
	}

	if !applied {
		// There can be two reasons why the query does not get applied. Either the RangeID has changed, or
		// the next_event_id check failed. Check the row info returned by Cassandra to figure out which one it is.
	GetFailureReasonLoop:
		for {
			rowType, ok := previous["type"].(int)
			if !ok {
				// This should never happen, as all our rows have the type field.
				break GetFailureReasonLoop
			}

			if rowType == rowTypeShard {
				if rangeID, ok := previous["range_id"].(int64); ok && rangeID != request.RangeID {
					// UpdateWorkflowExecution failed because rangeID was modified
					return &ShardOwnershipLostError{
						ShardID: d.shardID,
						Msg: fmt.Sprintf("Failed to reset mutable state.  Request RangeID: %v, Actual RangeID: %v",
							request.RangeID, rangeID),
					}
				}
			} else {
				if nextEventID, ok := previous["next_event_id"].(int64); ok && nextEventID != request.Condition {
					// CreateWorkflowExecution failed because next event ID is unexpected
					return &ConditionFailedError{
						Msg: fmt.Sprintf("Failed to reset mutable state.  Request Condition: %v, Actual Value: %v",
							request.Condition, nextEventID),
					}
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

		return &ShardOwnershipLostError{
			ShardID: d.shardID,
			Msg: fmt.Sprintf("Failed to reset mutable state.  RangeID: %v, Condition: %v, columns: (%v)",
				request.RangeID, request.Condition, strings.Join(columns, ",")),
		}
	}

	return nil
}

func (d *cassandraPersistence) DeleteWorkflowExecution(request *DeleteWorkflowExecutionRequest) error {
	query := d.session.Query(templateDeleteWorkflowExecutionMutableStateQuery,
		d.shardID,
		rowTypeExecution,
		request.DomainID,
		request.WorkflowID,
		request.RunID,
		defaultVisibilityTimestamp,
		rowTypeExecutionTaskID)

	err := query.Exec()
	if err != nil {
		if isThrottlingError(err) {
			return &workflow.ServiceBusyError{
				Message: fmt.Sprintf("DeleteWorkflowExecution operation failed. Error: %v", err),
			}
		}
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("DeleteWorkflowExecution operation failed. Error: %v", err),
		}
	}

	return nil
}

func (d *cassandraPersistence) GetCurrentExecution(request *GetCurrentExecutionRequest) (*GetCurrentExecutionResponse,
	error) {
	query := d.session.Query(templateGetCurrentExecutionQuery,
		d.shardID,
		rowTypeExecution,
		request.DomainID,
		request.WorkflowID,
		permanentRunID,
		defaultVisibilityTimestamp,
		rowTypeExecutionTaskID)

	result := make(map[string]interface{})
	if err := query.MapScan(result); err != nil {
		if err == gocql.ErrNotFound {
			return nil, &workflow.EntityNotExistsError{
				Message: fmt.Sprintf("Workflow execution not found.  WorkflowId: %v",
					request.WorkflowID),
			}
		} else if isThrottlingError(err) {
			return nil, &workflow.ServiceBusyError{
				Message: fmt.Sprintf("GetCurrentExecution operation failed. Error: %v", err),
			}
		}

		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("GetCurrentExecution operation failed. Error: %v", err),
		}
	}

	currentRunID := result["current_run_id"].(gocql.UUID).String()
	executionInfo := createWorkflowExecutionInfo(result["execution"].(map[string]interface{}))
	return &GetCurrentExecutionResponse{
		RunID:          currentRunID,
		StartRequestID: executionInfo.CreateRequestID,
		State:          executionInfo.State,
		CloseStatus:    executionInfo.CloseStatus,
	}, nil
}

func (d *cassandraPersistence) GetTransferTasks(request *GetTransferTasksRequest) (*GetTransferTasksResponse, error) {

	// Reading transfer tasks need to be quorum level consistent, otherwise we could loose task
	query := d.session.Query(templateGetTransferTasksQuery,
		d.shardID,
		rowTypeTransferTask,
		rowTypeTransferDomainID,
		rowTypeTransferWorkflowID,
		rowTypeTransferRunID,
		defaultVisibilityTimestamp,
		request.ReadLevel,
		request.MaxReadLevel,
	).PageSize(request.BatchSize).PageState(request.NextPageToken)

	iter := query.Iter()
	if iter == nil {
		return nil, &workflow.InternalServiceError{
			Message: "GetTransferTasks operation failed.  Not able to create query iterator.",
		}
	}

	response := &GetTransferTasksResponse{}
	task := make(map[string]interface{})
	for iter.MapScan(task) {
		t := createTransferTaskInfo(task["transfer"].(map[string]interface{}))
		// Reset task map to get it ready for next scan
		task = make(map[string]interface{})

		response.Tasks = append(response.Tasks, t)
	}
	nextPageToken := iter.PageState()
	response.NextPageToken = make([]byte, len(nextPageToken))
	copy(response.NextPageToken, nextPageToken)

	if err := iter.Close(); err != nil {
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("GetTransferTasks operation failed. Error: %v", err),
		}
	}

	return response, nil
}

func (d *cassandraPersistence) GetReplicationTasks(request *GetReplicationTasksRequest) (*GetReplicationTasksResponse,
	error) {

	// Reading replication tasks need to be quorum level consistent, otherwise we could loose task
	query := d.session.Query(templateGetReplicationTasksQuery,
		d.shardID,
		rowTypeReplicationTask,
		rowTypeReplicationDomainID,
		rowTypeReplicationWorkflowID,
		rowTypeReplicationRunID,
		defaultVisibilityTimestamp,
		request.ReadLevel,
		request.MaxReadLevel,
	).PageSize(request.BatchSize).PageState(request.NextPageToken)

	iter := query.Iter()
	if iter == nil {
		return nil, &workflow.InternalServiceError{
			Message: "GetReplicationTasks operation failed.  Not able to create query iterator.",
		}
	}

	response := &GetReplicationTasksResponse{}
	task := make(map[string]interface{})
	for iter.MapScan(task) {
		t := createReplicationTaskInfo(task["replication"].(map[string]interface{}))
		// Reset task map to get it ready for next scan
		task = make(map[string]interface{})

		response.Tasks = append(response.Tasks, t)
	}
	nextPageToken := iter.PageState()
	response.NextPageToken = make([]byte, len(nextPageToken))
	copy(response.NextPageToken, nextPageToken)

	if err := iter.Close(); err != nil {
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("GetReplicationTasks operation failed. Error: %v", err),
		}
	}

	return response, nil
}

func (d *cassandraPersistence) CompleteTransferTask(request *CompleteTransferTaskRequest) error {
	query := d.session.Query(templateCompleteTransferTaskQuery,
		d.shardID,
		rowTypeTransferTask,
		rowTypeTransferDomainID,
		rowTypeTransferWorkflowID,
		rowTypeTransferRunID,
		defaultVisibilityTimestamp,
		request.TaskID)

	err := query.Exec()
	if err != nil {
		if isThrottlingError(err) {
			return &workflow.ServiceBusyError{
				Message: fmt.Sprintf("CompleteTransferTask operation failed. Error: %v", err),
			}
		}
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("CompleteTransferTask operation failed. Error: %v", err),
		}
	}

	return nil
}

func (d *cassandraPersistence) RangeCompleteTransferTask(request *RangeCompleteTransferTaskRequest) error {
	query := d.session.Query(templateRangeCompleteTransferTaskQuery,
		d.shardID,
		rowTypeTransferTask,
		rowTypeTransferDomainID,
		rowTypeTransferWorkflowID,
		rowTypeTransferRunID,
		defaultVisibilityTimestamp,
		request.ExclusiveBeginTaskID,
		request.InclusiveEndTaskID,
	)

	err := query.Exec()
	if err != nil {
		if isThrottlingError(err) {
			return &workflow.ServiceBusyError{
				Message: fmt.Sprintf("RangeCompleteTransferTask operation failed. Error: %v", err),
			}
		}
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("RangeCompleteTransferTask operation failed. Error: %v", err),
		}
	}

	return nil
}

func (d *cassandraPersistence) CompleteReplicationTask(request *CompleteReplicationTaskRequest) error {
	query := d.session.Query(templateCompleteTransferTaskQuery,
		d.shardID,
		rowTypeReplicationTask,
		rowTypeReplicationDomainID,
		rowTypeReplicationWorkflowID,
		rowTypeReplicationRunID,
		defaultVisibilityTimestamp,
		request.TaskID)

	err := query.Exec()
	if err != nil {
		if isThrottlingError(err) {
			return &workflow.ServiceBusyError{
				Message: fmt.Sprintf("CompleteReplicationTask operation failed. Error: %v", err),
			}
		}
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("CompleteReplicationTask operation failed. Error: %v", err),
		}
	}

	return nil
}

func (d *cassandraPersistence) CompleteTimerTask(request *CompleteTimerTaskRequest) error {
	ts := common.UnixNanoToCQLTimestamp(request.VisibilityTimestamp.UnixNano())
	query := d.session.Query(templateCompleteTimerTaskQuery,
		d.shardID,
		rowTypeTimerTask,
		rowTypeTimerDomainID,
		rowTypeTimerWorkflowID,
		rowTypeTimerRunID,
		ts,
		request.TaskID)

	err := query.Exec()
	if err != nil {
		if isThrottlingError(err) {
			return &workflow.ServiceBusyError{
				Message: fmt.Sprintf("CompleteTimerTask operation failed. Error: %v", err),
			}
		}
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("CompleteTimerTask operation failed. Error: %v", err),
		}
	}

	return nil
}

func (d *cassandraPersistence) RangeCompleteTimerTask(request *RangeCompleteTimerTaskRequest) error {
	start := common.UnixNanoToCQLTimestamp(request.InclusiveBeginTimestamp.UnixNano())
	end := common.UnixNanoToCQLTimestamp(request.ExclusiveEndTimestamp.UnixNano())
	query := d.session.Query(templateRangeCompleteTimerTaskQuery,
		d.shardID,
		rowTypeTimerTask,
		rowTypeTimerDomainID,
		rowTypeTimerWorkflowID,
		rowTypeTimerRunID,
		start,
		end,
	)

	err := query.Exec()
	if err != nil {
		if isThrottlingError(err) {
			return &workflow.ServiceBusyError{
				Message: fmt.Sprintf("RangeCompleteTimerTask operation failed. Error: %v", err),
			}
		}
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("RangeCompleteTimerTask operation failed. Error: %v", err),
		}
	}

	return nil
}

// From TaskManager interface
func (d *cassandraPersistence) LeaseTaskList(request *LeaseTaskListRequest) (*LeaseTaskListResponse, error) {
	if len(request.TaskList) == 0 {
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("LeaseTaskList requires non empty task list"),
		}
	}
	query := d.session.Query(templateGetTaskList,
		request.DomainID,
		request.TaskList,
		request.TaskType,
		rowTypeTaskList,
		taskListTaskID,
	)
	var rangeID, ackLevel int64
	var tlDB map[string]interface{}
	err := query.Scan(&rangeID, &tlDB)
	if err != nil {
		if err == gocql.ErrNotFound { // First time task list is used
			query = d.session.Query(templateInsertTaskListQuery,
				request.DomainID,
				request.TaskList,
				request.TaskType,
				rowTypeTaskList,
				taskListTaskID,
				initialRangeID,
				request.DomainID,
				request.TaskList,
				request.TaskType,
				0,
				request.TaskListKind,
			)
		} else if isThrottlingError(err) {
			return nil, &workflow.ServiceBusyError{
				Message: fmt.Sprintf("LeaseTaskList operation failed. TaskList: %v, TaskType: %v, Error: %v",
					request.TaskList, request.TaskType, err),
			}
		} else {
			return nil, &workflow.InternalServiceError{
				Message: fmt.Sprintf("LeaseTaskList operation failed. TaskList: %v, TaskType: %v, Error: %v",
					request.TaskList, request.TaskType, err),
			}
		}
	} else {
		ackLevel = tlDB["ack_level"].(int64)
		taskListKind := tlDB["kind"].(int)
		query = d.session.Query(templateUpdateTaskListQuery,
			rangeID+1,
			request.DomainID,
			&request.TaskList,
			request.TaskType,
			ackLevel,
			taskListKind,
			request.DomainID,
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
			return nil, &workflow.ServiceBusyError{
				Message: fmt.Sprintf("LeaseTaskList operation failed. Error: %v", err),
			}
		}
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("LeaseTaskList operation failed. Error : %v", err),
		}
	}
	if !applied {
		previousRangeID := previous["range_id"]
		return nil, &ConditionFailedError{
			Msg: fmt.Sprintf("LeaseTaskList failed to apply. db rangeID %v", previousRangeID),
		}
	}
	tli := &TaskListInfo{DomainID: request.DomainID, Name: request.TaskList, TaskType: request.TaskType, RangeID: rangeID + 1, AckLevel: ackLevel, Kind: request.TaskListKind}
	return &LeaseTaskListResponse{TaskListInfo: tli}, nil
}

// From TaskManager interface
func (d *cassandraPersistence) UpdateTaskList(request *UpdateTaskListRequest) (*UpdateTaskListResponse, error) {
	tli := request.TaskListInfo

	if tli.Kind == TaskListKindSticky { // if task_list is sticky, then update with TTL
		query := d.session.Query(templateUpdateTaskListQueryWithTTL,
			tli.DomainID,
			&tli.Name,
			tli.TaskType,
			rowTypeTaskList,
			taskListTaskID,
			tli.RangeID,
			tli.DomainID,
			&tli.Name,
			tli.TaskType,
			tli.AckLevel,
			tli.Kind,
			stickyTaskListTTL,
		)
		err := query.Exec()
		if err != nil {
			if isThrottlingError(err) {
				return nil, &workflow.ServiceBusyError{
					Message: fmt.Sprintf("UpdateTaskList operation failed. Error: %v", err),
				}
			}
			return nil, &workflow.InternalServiceError{
				Message: fmt.Sprintf("UpdateTaskList operation failed. Error: %v", err),
			}
		}
		return &UpdateTaskListResponse{}, nil
	}

	query := d.session.Query(templateUpdateTaskListQuery,
		tli.RangeID,
		tli.DomainID,
		&tli.Name,
		tli.TaskType,
		tli.AckLevel,
		tli.Kind,
		tli.DomainID,
		&tli.Name,
		tli.TaskType,
		rowTypeTaskList,
		taskListTaskID,
		tli.RangeID,
	)

	previous := make(map[string]interface{})
	applied, err := query.MapScanCAS(previous)
	if err != nil {
		if isThrottlingError(err) {
			return nil, &workflow.ServiceBusyError{
				Message: fmt.Sprintf("UpdateTaskList operation failed. Error: %v", err),
			}
		}
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("UpdateTaskList operation failed. Error: %v", err),
		}
	}

	if !applied {
		var columns []string
		for k, v := range previous {
			columns = append(columns, fmt.Sprintf("%s=%v", k, v))
		}

		return nil, &ConditionFailedError{
			Msg: fmt.Sprintf("Failed to update task list. name: %v, type: %v, rangeID: %v, columns: (%v)",
				tli.Name, tli.TaskType, tli.RangeID, strings.Join(columns, ",")),
		}
	}

	return &UpdateTaskListResponse{}, nil
}

// From TaskManager interface
func (d *cassandraPersistence) CreateTasks(request *CreateTasksRequest) (*CreateTasksResponse, error) {
	batch := d.session.NewBatch(gocql.LoggedBatch)
	domainID := request.TaskListInfo.DomainID
	taskList := request.TaskListInfo.Name
	taskListType := request.TaskListInfo.TaskType
	taskListKind := request.TaskListInfo.Kind
	ackLevel := request.TaskListInfo.AckLevel

	for _, task := range request.Tasks {
		scheduleID := task.Data.ScheduleID
		if task.Data.ScheduleToStartTimeout == 0 {
			batch.Query(templateCreateTaskQuery,
				domainID,
				taskList,
				taskListType,
				rowTypeTask,
				task.TaskID,
				domainID,
				task.Execution.GetWorkflowId(),
				task.Execution.GetRunId(),
				scheduleID)
		} else {
			batch.Query(templateCreateTaskWithTTLQuery,
				domainID,
				taskList,
				taskListType,
				rowTypeTask,
				task.TaskID,
				domainID,
				task.Execution.GetWorkflowId(),
				task.Execution.GetRunId(),
				scheduleID,
				task.Data.ScheduleToStartTimeout)
		}
	}

	// The following query is used to ensure that range_id didn't change
	batch.Query(templateUpdateTaskListQuery,
		request.TaskListInfo.RangeID,
		domainID,
		taskList,
		taskListType,
		ackLevel,
		taskListKind,
		domainID,
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
			return nil, &workflow.ServiceBusyError{
				Message: fmt.Sprintf("CreateTasks operation failed. Error: %v", err),
			}
		}
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("CreateTasks operation failed. Error : %v", err),
		}
	}
	if !applied {
		rangeID := previous["range_id"]
		return nil, &ConditionFailedError{
			Msg: fmt.Sprintf("Failed to create task. TaskList: %v, taskListType: %v, rangeID: %v, db rangeID: %v",
				taskList, taskListType, request.TaskListInfo.RangeID, rangeID),
		}
	}

	return &CreateTasksResponse{}, nil
}

// From TaskManager interface
func (d *cassandraPersistence) GetTasks(request *GetTasksRequest) (*GetTasksResponse, error) {
	if request.ReadLevel > request.MaxReadLevel {
		return &GetTasksResponse{}, nil
	}

	// Reading tasklist tasks need to be quorum level consistent, otherwise we could loose task
	query := d.session.Query(templateGetTasksQuery,
		request.DomainID,
		request.TaskList,
		request.TaskType,
		rowTypeTask,
		request.ReadLevel,
		request.MaxReadLevel,
	).PageSize(request.BatchSize)

	iter := query.Iter()
	if iter == nil {
		return nil, &workflow.InternalServiceError{
			Message: "GetTasks operation failed.  Not able to create query iterator.",
		}
	}

	response := &GetTasksResponse{}
	task := make(map[string]interface{})
PopulateTasks:
	for iter.MapScan(task) {
		taskID, ok := task["task_id"]
		if !ok { // no tasks, but static column record returned
			continue
		}
		t := createTaskInfo(task["task"].(map[string]interface{}))
		t.TaskID = taskID.(int64)
		response.Tasks = append(response.Tasks, t)
		if len(response.Tasks) == request.BatchSize {
			break PopulateTasks
		}
		task = make(map[string]interface{}) // Reinitialize map as initialized fails on unmarshalling
	}

	if err := iter.Close(); err != nil {
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("GetTasks operation failed. Error: %v", err),
		}
	}

	return response, nil
}

// From TaskManager interface
func (d *cassandraPersistence) CompleteTask(request *CompleteTaskRequest) error {
	tli := request.TaskList
	query := d.session.Query(templateCompleteTaskQuery,
		tli.DomainID,
		tli.Name,
		tli.TaskType,
		rowTypeTask,
		request.TaskID)

	err := query.Exec()
	if err != nil {
		if isThrottlingError(err) {
			return &workflow.ServiceBusyError{
				Message: fmt.Sprintf("CompleteTask operation failed. Error: %v", err),
			}
		}
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("CompleteTask operation failed. Error: %v", err),
		}
	}

	return nil
}

func (d *cassandraPersistence) GetTimerIndexTasks(request *GetTimerIndexTasksRequest) (*GetTimerIndexTasksResponse,
	error) {
	// Reading timer tasks need to be quorum level consistent, otherwise we could loose task
	minTimestamp := common.UnixNanoToCQLTimestamp(request.MinTimestamp.UnixNano())
	maxTimestamp := common.UnixNanoToCQLTimestamp(request.MaxTimestamp.UnixNano())
	query := d.session.Query(templateGetTimerTasksQuery,
		d.shardID,
		rowTypeTimerTask,
		rowTypeTimerDomainID,
		rowTypeTimerWorkflowID,
		rowTypeTimerRunID,
		minTimestamp,
		maxTimestamp,
	).PageSize(request.BatchSize).PageState(request.NextPageToken)

	iter := query.Iter()
	if iter == nil {
		return nil, &workflow.InternalServiceError{
			Message: "GetTimerTasks operation failed.  Not able to create query iterator.",
		}
	}

	response := &GetTimerIndexTasksResponse{}
	task := make(map[string]interface{})
	for iter.MapScan(task) {
		t := createTimerTaskInfo(task["timer"].(map[string]interface{}))
		// Reset task map to get it ready for next scan
		task = make(map[string]interface{})

		response.Timers = append(response.Timers, t)
	}
	nextPageToken := iter.PageState()
	response.NextPageToken = make([]byte, len(nextPageToken))
	copy(response.NextPageToken, nextPageToken)

	if err := iter.Close(); err != nil {
		if isThrottlingError(err) {
			return nil, &workflow.ServiceBusyError{
				Message: fmt.Sprintf("GetTimerTasks operation failed. Error: %v", err),
			}
		}
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("GetTimerTasks operation failed. Error: %v", err),
		}
	}

	return response, nil
}

func (d *cassandraPersistence) createTransferTasks(batch *gocql.Batch, transferTasks []Task, domainID, workflowID,
	runID string) {
	targetDomainID := domainID
	for _, task := range transferTasks {
		var taskList string
		var scheduleID int64
		targetWorkflowID := transferTaskTransferTargetWorkflowID
		targetRunID := transferTaskTypeTransferTargetRunID
		targetChildWorkflowOnly := false

		switch task.GetType() {
		case TransferTaskTypeActivityTask:
			targetDomainID = task.(*ActivityTask).DomainID
			taskList = task.(*ActivityTask).TaskList
			scheduleID = task.(*ActivityTask).ScheduleID

		case TransferTaskTypeDecisionTask:
			targetDomainID = task.(*DecisionTask).DomainID
			taskList = task.(*DecisionTask).TaskList
			scheduleID = task.(*DecisionTask).ScheduleID

		case TransferTaskTypeCancelExecution:
			targetDomainID = task.(*CancelExecutionTask).TargetDomainID
			targetWorkflowID = task.(*CancelExecutionTask).TargetWorkflowID
			targetRunID = task.(*CancelExecutionTask).TargetRunID
			if targetRunID == "" {
				targetRunID = transferTaskTypeTransferTargetRunID
			}
			targetChildWorkflowOnly = task.(*CancelExecutionTask).TargetChildWorkflowOnly
			scheduleID = task.(*CancelExecutionTask).InitiatedID

		case TransferTaskTypeSignalExecution:
			targetDomainID = task.(*SignalExecutionTask).TargetDomainID
			targetWorkflowID = task.(*SignalExecutionTask).TargetWorkflowID
			targetRunID = task.(*SignalExecutionTask).TargetRunID
			if targetRunID == "" {
				targetRunID = transferTaskTypeTransferTargetRunID
			}
			targetChildWorkflowOnly = task.(*SignalExecutionTask).TargetChildWorkflowOnly
			scheduleID = task.(*SignalExecutionTask).InitiatedID

		case TransferTaskTypeStartChildExecution:
			targetDomainID = task.(*StartChildExecutionTask).TargetDomainID
			targetWorkflowID = task.(*StartChildExecutionTask).TargetWorkflowID
			scheduleID = task.(*StartChildExecutionTask).InitiatedID

		case TransferTaskTypeCloseExecution:
			// No explicit property needs to be set

		default:
			d.logger.Fatal("Unknown Transfer Task.")
		}

		batch.Query(templateCreateTransferTaskQuery,
			d.shardID,
			rowTypeTransferTask,
			rowTypeTransferDomainID,
			rowTypeTransferWorkflowID,
			rowTypeTransferRunID,
			domainID,
			workflowID,
			runID,
			task.GetVisibilityTimestamp(),
			task.GetTaskID(),
			targetDomainID,
			targetWorkflowID,
			targetRunID,
			targetChildWorkflowOnly,
			taskList,
			task.GetType(),
			scheduleID,
			task.GetVersion(),
			defaultVisibilityTimestamp,
			task.GetTaskID())
	}
}

func (d *cassandraPersistence) createReplicationTasks(batch *gocql.Batch, replicationTasks []Task, domainID, workflowID,
	runID string) {

	for _, task := range replicationTasks {
		// Replication task specific information
		firstEventID := common.EmptyEventID
		nextEventID := common.EmptyEventID
		version := int64(0)
		var lastReplicationInfo map[string]map[string]interface{}

		switch task.GetType() {
		case ReplicationTaskTypeHistory:
			firstEventID = task.(*HistoryReplicationTask).FirstEventID
			nextEventID = task.(*HistoryReplicationTask).NextEventID
			version = task.GetVersion()
			lastReplicationInfo = make(map[string]map[string]interface{})
			for k, v := range task.(*HistoryReplicationTask).LastReplicationInfo {
				lastReplicationInfo[k] = createReplicationInfoMap(v)
			}

		default:
			d.logger.Fatal("Unknown Replication Task.")
		}

		batch.Query(templateCreateReplicationTaskQuery,
			d.shardID,
			rowTypeReplicationTask,
			rowTypeReplicationDomainID,
			rowTypeReplicationWorkflowID,
			rowTypeReplicationRunID,
			domainID,
			workflowID,
			runID,
			task.GetTaskID(),
			task.GetType(),
			firstEventID,
			nextEventID,
			version,
			lastReplicationInfo,
			defaultVisibilityTimestamp,
			task.GetTaskID())
	}
}

func (d *cassandraPersistence) createTimerTasks(batch *gocql.Batch, timerTasks []Task, deleteTimerTask Task,
	domainID, workflowID, runID string, cqlNowTimestamp int64) {

	for _, task := range timerTasks {
		var eventID int64
		var attempt int64

		timeoutType := 0

		switch t := task.(type) {
		case *DecisionTimeoutTask:
			eventID = t.EventID
			timeoutType = t.TimeoutType
			attempt = t.ScheduleAttempt
		case *ActivityTimeoutTask:
			eventID = t.EventID
			timeoutType = t.TimeoutType
			attempt = t.Attempt
		case *UserTimerTask:
			eventID = t.EventID
		case *ActivityRetryTimerTask:
			eventID = t.EventID
			attempt = int64(t.Attempt)
		case *WorkflowRetryTimerTask:
			eventID = t.EventID
		}

		ts := common.UnixNanoToCQLTimestamp(GetVisibilityTSFrom(task).UnixNano())

		batch.Query(templateCreateTimerTaskQuery,
			d.shardID,
			rowTypeTimerTask,
			rowTypeTimerDomainID,
			rowTypeTimerWorkflowID,
			rowTypeTimerRunID,
			domainID,
			workflowID,
			runID,
			ts,
			task.GetTaskID(),
			task.GetType(),
			timeoutType,
			eventID,
			attempt,
			task.GetVersion(),
			ts,
			task.GetTaskID())
	}

	if deleteTimerTask != nil {
		ts := common.UnixNanoToCQLTimestamp(GetVisibilityTSFrom(deleteTimerTask).UnixNano())
		batch.Query(templateCompleteTimerTaskQuery,
			d.shardID,
			rowTypeTimerTask,
			rowTypeTimerDomainID,
			rowTypeTimerWorkflowID,
			rowTypeTimerRunID,
			ts,
			deleteTimerTask.GetTaskID())
	}
}

func (d *cassandraPersistence) updateActivityInfos(batch *gocql.Batch, activityInfos []*ActivityInfo, deleteInfos []int64,
	domainID, workflowID, runID string, condition int64, rangeID int64) {

	for _, a := range activityInfos {
		batch.Query(templateUpdateActivityInfoQuery,
			a.ScheduleID,
			a.Version,
			a.ScheduleID,
			a.ScheduledEvent,
			a.ScheduledTime,
			a.StartedID,
			a.StartedEvent,
			a.StartedTime,
			a.ActivityID,
			a.RequestID,
			a.Details,
			a.ScheduleToStartTimeout,
			a.ScheduleToCloseTimeout,
			a.StartToCloseTimeout,
			a.HeartbeatTimeout,
			a.CancelRequested,
			a.CancelRequestID,
			a.LastHeartBeatUpdatedTime,
			a.TimerTaskStatus,
			a.Attempt,
			a.TaskList,
			a.StartedIdentity,
			a.HasRetryPolicy,
			a.InitialInterval,
			a.BackoffCoefficient,
			a.MaximumInterval,
			a.ExpirationTime,
			a.MaximumAttempts,
			a.NonRetriableErrors,
			d.shardID,
			rowTypeExecution,
			domainID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID,
			condition)
	}

	for _, deleteInfo := range deleteInfos {
		batch.Query(templateDeleteActivityInfoQuery,
			deleteInfo,
			d.shardID,
			rowTypeExecution,
			domainID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID,
			condition)
	}
}

func (d *cassandraPersistence) resetBufferedEvents(batch *gocql.Batch, domainID, workflowID, runID string,
	condition int64) {
	batch.Query(templateDeleteBufferedEventsQuery,
		d.shardID,
		rowTypeExecution,
		domainID,
		workflowID,
		runID,
		defaultVisibilityTimestamp,
		rowTypeExecutionTaskID,
		condition)

	batch.Query(templateClearBufferedReplicationTaskQuery,
		d.shardID,
		rowTypeExecution,
		domainID,
		workflowID,
		runID,
		defaultVisibilityTimestamp,
		rowTypeExecutionTaskID,
		condition)
}

func (d *cassandraPersistence) resetActivityInfos(batch *gocql.Batch, activityInfos []*ActivityInfo, domainID,
	workflowID, runID string, condition int64) {
	batch.Query(templateResetActivityInfoQuery,
		resetActivityInfoMap(activityInfos),
		d.shardID,
		rowTypeExecution,
		domainID,
		workflowID,
		runID,
		defaultVisibilityTimestamp,
		rowTypeExecutionTaskID,
		condition)
}

func (d *cassandraPersistence) updateTimerInfos(batch *gocql.Batch, timerInfos []*TimerInfo, deleteInfos []string,
	domainID, workflowID, runID string, condition int64, rangeID int64) {

	for _, a := range timerInfos {
		batch.Query(templateUpdateTimerInfoQuery,
			a.TimerID,
			a.Version,
			a.TimerID,
			a.StartedID,
			a.ExpiryTime,
			a.TaskID,
			d.shardID,
			rowTypeExecution,
			domainID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID,
			condition)
	}

	for _, t := range deleteInfos {
		batch.Query(templateDeleteTimerInfoQuery,
			t,
			d.shardID,
			rowTypeExecution,
			domainID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID,
			condition)
	}
}

func (d *cassandraPersistence) resetTimerInfos(batch *gocql.Batch, timerInfos []*TimerInfo, domainID, workflowID,
	runID string, condition int64) {
	batch.Query(templateResetTimerInfoQuery,
		resetTimerInfoMap(timerInfos),
		d.shardID,
		rowTypeExecution,
		domainID,
		workflowID,
		runID,
		defaultVisibilityTimestamp,
		rowTypeExecutionTaskID,
		condition)
}

func (d *cassandraPersistence) updateChildExecutionInfos(batch *gocql.Batch, childExecutionInfos []*ChildExecutionInfo,
	deleteInfo *int64, domainID, workflowID, runID string, condition int64, rangeID int64) {

	for _, c := range childExecutionInfos {
		batch.Query(templateUpdateChildExecutionInfoQuery,
			c.InitiatedID,
			c.Version,
			c.InitiatedID,
			c.InitiatedEvent,
			c.StartedID,
			c.StartedEvent,
			c.CreateRequestID,
			d.shardID,
			rowTypeExecution,
			domainID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID,
			condition)
	}

	// deleteInfo is the initiatedID for ChildInfo being deleted
	if deleteInfo != nil {
		batch.Query(templateDeleteChildExecutionInfoQuery,
			*deleteInfo,
			d.shardID,
			rowTypeExecution,
			domainID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID,
			condition)
	}
}

func (d *cassandraPersistence) resetChildExecutionInfos(batch *gocql.Batch, childExecutionInfos []*ChildExecutionInfo,
	domainID, workflowID, runID string, condition int64) {
	batch.Query(templateResetChildExecutionInfoQuery,
		resetChildExecutionInfoMap(childExecutionInfos),
		d.shardID,
		rowTypeExecution,
		domainID,
		workflowID,
		runID,
		defaultVisibilityTimestamp,
		rowTypeExecutionTaskID,
		condition)
}

func (d *cassandraPersistence) updateRequestCancelInfos(batch *gocql.Batch, requestCancelInfos []*RequestCancelInfo,
	deleteInfo *int64, domainID, workflowID, runID string, condition int64, rangeID int64) {

	for _, c := range requestCancelInfos {
		batch.Query(templateUpdateRequestCancelInfoQuery,
			c.InitiatedID,
			c.Version,
			c.InitiatedID,
			c.CancelRequestID,
			d.shardID,
			rowTypeExecution,
			domainID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID,
			condition)
	}

	// deleteInfo is the initiatedID for RequestCancelInfo being deleted
	if deleteInfo != nil {
		batch.Query(templateDeleteRequestCancelInfoQuery,
			*deleteInfo,
			d.shardID,
			rowTypeExecution,
			domainID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID,
			condition)
	}
}

func (d *cassandraPersistence) resetRequestCancelInfos(batch *gocql.Batch, requestCancelInfos []*RequestCancelInfo,
	domainID, workflowID, runID string, condition int64) {
	batch.Query(templateResetRequestCancelInfoQuery,
		resetRequestCancelInfoMap(requestCancelInfos),
		d.shardID,
		rowTypeExecution,
		domainID,
		workflowID,
		runID,
		defaultVisibilityTimestamp,
		rowTypeExecutionTaskID,
		condition)
}

func (d *cassandraPersistence) updateSignalInfos(batch *gocql.Batch, signalInfos []*SignalInfo,
	deleteInfo *int64, domainID, workflowID, runID string, condition int64, rangeID int64) {

	for _, c := range signalInfos {
		batch.Query(templateUpdateSignalInfoQuery,
			c.InitiatedID,
			c.Version,
			c.InitiatedID,
			c.SignalRequestID,
			c.SignalName,
			c.Input,
			c.Control,
			d.shardID,
			rowTypeExecution,
			domainID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID,
			condition)
	}

	// deleteInfo is the initiatedID for SignalInfo being deleted
	if deleteInfo != nil {
		batch.Query(templateDeleteSignalInfoQuery,
			*deleteInfo,
			d.shardID,
			rowTypeExecution,
			domainID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID,
			condition)
	}
}

func (d *cassandraPersistence) resetSignalInfos(batch *gocql.Batch, signalInfos []*SignalInfo,
	domainID, workflowID, runID string, condition int64) {
	batch.Query(templateResetSignalInfoQuery,
		resetSignalInfoMap(signalInfos),
		d.shardID,
		rowTypeExecution,
		domainID,
		workflowID,
		runID,
		defaultVisibilityTimestamp,
		rowTypeExecutionTaskID,
		condition)
}

func (d *cassandraPersistence) updateSignalsRequested(batch *gocql.Batch, signalReqIDs []string, deleteSignalReqID string,
	domainID, workflowID, runID string, condition int64, rangeID int64) {

	if len(signalReqIDs) > 0 {
		batch.Query(templateUpdateSignalRequestedQuery,
			signalReqIDs,
			d.shardID,
			rowTypeExecution,
			domainID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID,
			condition)
	}

	if deleteSignalReqID != "" {
		req := []string{deleteSignalReqID} // for cassandra set binding
		batch.Query(templateDeleteWorkflowExecutionSignalRequestedQuery,
			req,
			d.shardID,
			rowTypeExecution,
			domainID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID,
			condition)
	}
}

func (d *cassandraPersistence) resetSignalRequested(batch *gocql.Batch, signalRequested []string,
	domainID, workflowID, runID string, condition int64) {
	batch.Query(templateResetSignalRequestedQuery,
		signalRequested,
		d.shardID,
		rowTypeExecution,
		domainID,
		workflowID,
		runID,
		defaultVisibilityTimestamp,
		rowTypeExecutionTaskID,
		condition)
}

func (d *cassandraPersistence) updateBufferedEvents(batch *gocql.Batch, newBufferedEvents *SerializedHistoryEventBatch,
	clearBufferedEvents bool, domainID, workflowID, runID string, condition int64, rangeID int64) {

	if clearBufferedEvents {
		batch.Query(templateDeleteBufferedEventsQuery,
			d.shardID,
			rowTypeExecution,
			domainID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID,
			condition)
	} else if newBufferedEvents != nil {
		values := make(map[string]interface{})
		values["encoding_type"] = newBufferedEvents.EncodingType
		values["version"] = newBufferedEvents.Version
		values["data"] = newBufferedEvents.Data
		newEventValues := []map[string]interface{}{values}
		batch.Query(templateAppendBufferedEventsQuery,
			newEventValues,
			d.shardID,
			rowTypeExecution,
			domainID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID,
			condition)
	}
}

func (d *cassandraPersistence) updateBufferedReplicationTasks(batch *gocql.Batch, newBufferedReplicationTask *BufferedReplicationTask,
	deleteInfo *int64, domainID, workflowID, runID string, condition int64, rangeID int64) {

	if newBufferedReplicationTask != nil {
		if newBufferedReplicationTask.NewRunHistory != nil {
			batch.Query(templateUpdateBufferedReplicationTasksQuery,
				newBufferedReplicationTask.FirstEventID,
				newBufferedReplicationTask.FirstEventID,
				newBufferedReplicationTask.NextEventID,
				newBufferedReplicationTask.Version,
				newBufferedReplicationTask.History.EncodingType,
				newBufferedReplicationTask.History.Version,
				newBufferedReplicationTask.History.Data,
				newBufferedReplicationTask.NewRunHistory.EncodingType,
				newBufferedReplicationTask.NewRunHistory.Version,
				newBufferedReplicationTask.NewRunHistory.Data,
				d.shardID,
				rowTypeExecution,
				domainID,
				workflowID,
				runID,
				defaultVisibilityTimestamp,
				rowTypeExecutionTaskID,
				condition)
		} else {
			batch.Query(templateUpdateBufferedReplicationTasksNoNewRunHistoryQuery,
				newBufferedReplicationTask.FirstEventID,
				newBufferedReplicationTask.FirstEventID,
				newBufferedReplicationTask.NextEventID,
				newBufferedReplicationTask.Version,
				newBufferedReplicationTask.History.EncodingType,
				newBufferedReplicationTask.History.Version,
				newBufferedReplicationTask.History.Data,
				d.shardID,
				rowTypeExecution,
				domainID,
				workflowID,
				runID,
				defaultVisibilityTimestamp,
				rowTypeExecutionTaskID,
				condition)
		}
	}

	// deleteInfo is the FirstEventID for the history batch being deleted
	if deleteInfo != nil {
		batch.Query(templateDeleteBufferedReplicationTaskQuery,
			*deleteInfo,
			d.shardID,
			rowTypeExecution,
			domainID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID,
			condition)
	}
}

func createShardInfo(currentCluster string, result map[string]interface{}) *ShardInfo {
	info := &ShardInfo{}
	for k, v := range result {
		switch k {
		case "shard_id":
			info.ShardID = v.(int)
		case "owner":
			info.Owner = v.(string)
		case "range_id":
			info.RangeID = v.(int64)
		case "stolen_since_renew":
			info.StolenSinceRenew = v.(int)
		case "updated_at":
			info.UpdatedAt = v.(time.Time)
		case "replication_ack_level":
			info.ReplicationAckLevel = v.(int64)
		case "transfer_ack_level":
			info.TransferAckLevel = v.(int64)
		case "timer_ack_level":
			info.TimerAckLevel = v.(time.Time)
		case "cluster_transfer_ack_level":
			info.ClusterTransferAckLevel = v.(map[string]int64)
		case "cluster_timer_ack_level":
			info.ClusterTimerAckLevel = v.(map[string]time.Time)
		case "domain_notification_version":
			info.DomainNotificationVersion = v.(int64)
		}
	}

	if info.ClusterTransferAckLevel == nil {
		info.ClusterTransferAckLevel = map[string]int64{
			currentCluster: info.TransferAckLevel,
		}
	}
	if info.ClusterTimerAckLevel == nil {
		info.ClusterTimerAckLevel = map[string]time.Time{
			currentCluster: info.TimerAckLevel,
		}
	}

	return info
}

func createWorkflowExecutionInfo(result map[string]interface{}) *WorkflowExecutionInfo {
	info := &WorkflowExecutionInfo{}
	for k, v := range result {
		switch k {
		case "domain_id":
			info.DomainID = v.(gocql.UUID).String()
		case "workflow_id":
			info.WorkflowID = v.(string)
		case "run_id":
			info.RunID = v.(gocql.UUID).String()
		case "parent_domain_id":
			info.ParentDomainID = v.(gocql.UUID).String()
		case "parent_workflow_id":
			info.ParentWorkflowID = v.(string)
		case "parent_run_id":
			info.ParentRunID = v.(gocql.UUID).String()
		case "initiated_id":
			info.InitiatedID = v.(int64)
		case "completion_event":
			info.CompletionEvent = v.([]byte)
		case "task_list":
			info.TaskList = v.(string)
		case "workflow_type_name":
			info.WorkflowTypeName = v.(string)
		case "workflow_timeout":
			info.WorkflowTimeout = int32(v.(int))
		case "decision_task_timeout":
			info.DecisionTimeoutValue = int32(v.(int))
		case "execution_context":
			info.ExecutionContext = v.([]byte)
		case "state":
			info.State = v.(int)
		case "close_status":
			info.CloseStatus = v.(int)
		case "last_first_event_id":
			info.LastFirstEventID = v.(int64)
		case "next_event_id":
			info.NextEventID = v.(int64)
		case "last_processed_event":
			info.LastProcessedEvent = v.(int64)
		case "start_time":
			info.StartTimestamp = v.(time.Time)
		case "last_updated_time":
			info.LastUpdatedTimestamp = v.(time.Time)
		case "create_request_id":
			info.CreateRequestID = v.(gocql.UUID).String()
		case "decision_version":
			info.DecisionVersion = v.(int64)
		case "decision_schedule_id":
			info.DecisionScheduleID = v.(int64)
		case "decision_started_id":
			info.DecisionStartedID = v.(int64)
		case "decision_request_id":
			info.DecisionRequestID = v.(string)
		case "decision_timeout":
			info.DecisionTimeout = int32(v.(int))
		case "decision_attempt":
			info.DecisionAttempt = v.(int64)
		case "decision_timestamp":
			info.DecisionTimestamp = v.(int64)
		case "cancel_requested":
			info.CancelRequested = v.(bool)
		case "cancel_request_id":
			info.CancelRequestID = v.(string)
		case "sticky_task_list":
			info.StickyTaskList = v.(string)
		case "sticky_schedule_to_start_timeout":
			info.StickyScheduleToStartTimeout = int32(v.(int))
		case "client_library_version":
			info.ClientLibraryVersion = v.(string)
		case "client_feature_version":
			info.ClientFeatureVersion = v.(string)
		case "client_impl":
			info.ClientImpl = v.(string)
		case "attempt":
			info.Attempt = int32(v.(int))
		case "has_retry_policy":
			info.HasRetryPolicy = v.(bool)
		case "init_interval":
			info.InitialInterval = int32(v.(int))
		case "backoff_coefficient":
			info.BackoffCoefficient = v.(float64)
		case "max_interval":
			info.MaximumInterval = int32(v.(int))
		case "max_attempts":
			info.MaximumAttempts = int32(v.(int))
		case "expiration_time":
			info.ExpirationTime = v.(time.Time)
		case "non_retriable_errors":
			info.NonRetriableErrors = v.([]string)
		}
	}

	return info
}

func createReplicationState(result map[string]interface{}) *ReplicationState {
	if len(result) == 0 {
		return nil
	}

	info := &ReplicationState{}
	for k, v := range result {
		switch k {
		case "current_version":
			info.CurrentVersion = v.(int64)
		case "start_version":
			info.StartVersion = v.(int64)
		case "last_write_version":
			info.LastWriteVersion = v.(int64)
		case "last_write_event_id":
			info.LastWriteEventID = v.(int64)
		case "last_replication_info":
			info.LastReplicationInfo = make(map[string]*ReplicationInfo)
			replicationInfoMap := v.(map[string]map[string]interface{})
			for key, value := range replicationInfoMap {
				info.LastReplicationInfo[key] = createReplicationInfo(value)
			}
		}
	}

	return info
}

func createTransferTaskInfo(result map[string]interface{}) *TransferTaskInfo {
	info := &TransferTaskInfo{}
	for k, v := range result {
		switch k {
		case "domain_id":
			info.DomainID = v.(gocql.UUID).String()
		case "workflow_id":
			info.WorkflowID = v.(string)
		case "run_id":
			info.RunID = v.(gocql.UUID).String()
		case "visibility_ts":
			info.VisibilityTimestamp = v.(time.Time)
		case "task_id":
			info.TaskID = v.(int64)
		case "target_domain_id":
			info.TargetDomainID = v.(gocql.UUID).String()
		case "target_workflow_id":
			info.TargetWorkflowID = v.(string)
		case "target_run_id":
			info.TargetRunID = v.(gocql.UUID).String()
			if info.TargetRunID == transferTaskTypeTransferTargetRunID {
				info.TargetRunID = ""
			}
		case "target_child_workflow_only":
			info.TargetChildWorkflowOnly = v.(bool)
		case "task_list":
			info.TaskList = v.(string)
		case "type":
			info.TaskType = v.(int)
		case "schedule_id":
			info.ScheduleID = v.(int64)
		case "version":
			info.Version = v.(int64)
		}
	}

	return info
}

func createReplicationTaskInfo(result map[string]interface{}) *ReplicationTaskInfo {
	info := &ReplicationTaskInfo{}
	for k, v := range result {
		switch k {
		case "domain_id":
			info.DomainID = v.(gocql.UUID).String()
		case "workflow_id":
			info.WorkflowID = v.(string)
		case "run_id":
			info.RunID = v.(gocql.UUID).String()
		case "task_id":
			info.TaskID = v.(int64)
		case "type":
			info.TaskType = v.(int)
		case "first_event_id":
			info.FirstEventID = v.(int64)
		case "next_event_id":
			info.NextEventID = v.(int64)
		case "version":
			info.Version = v.(int64)
		case "last_replication_info":
			info.LastReplicationInfo = make(map[string]*ReplicationInfo)
			replicationInfoMap := v.(map[string]map[string]interface{})
			for key, value := range replicationInfoMap {
				info.LastReplicationInfo[key] = createReplicationInfo(value)
			}
		}
	}

	return info
}

func createActivityInfo(result map[string]interface{}) *ActivityInfo {
	info := &ActivityInfo{}
	for k, v := range result {
		switch k {
		case "version":
			info.Version = v.(int64)
		case "schedule_id":
			info.ScheduleID = v.(int64)
		case "scheduled_event":
			info.ScheduledEvent = v.([]byte)
		case "scheduled_time":
			info.ScheduledTime = v.(time.Time)
		case "started_id":
			info.StartedID = v.(int64)
		case "started_event":
			info.StartedEvent = v.([]byte)
		case "started_time":
			info.StartedTime = v.(time.Time)
		case "activity_id":
			info.ActivityID = v.(string)
		case "request_id":
			info.RequestID = v.(string)
		case "details":
			info.Details = v.([]byte)
		case "schedule_to_start_timeout":
			info.ScheduleToStartTimeout = int32(v.(int))
		case "schedule_to_close_timeout":
			info.ScheduleToCloseTimeout = int32(v.(int))
		case "start_to_close_timeout":
			info.StartToCloseTimeout = int32(v.(int))
		case "heart_beat_timeout":
			info.HeartbeatTimeout = int32(v.(int))
		case "cancel_requested":
			info.CancelRequested = v.(bool)
		case "cancel_request_id":
			info.CancelRequestID = v.(int64)
		case "last_hb_updated_time":
			info.LastHeartBeatUpdatedTime = v.(time.Time)
		case "timer_task_status":
			info.TimerTaskStatus = int32(v.(int))
		case "attempt":
			info.Attempt = int32(v.(int))
		case "task_list":
			info.TaskList = v.(string)
		case "started_identity":
			info.StartedIdentity = v.(string)
		case "has_retry_policy":
			info.HasRetryPolicy = v.(bool)
		case "init_interval":
			info.InitialInterval = (int32)(v.(int))
		case "backoff_coefficient":
			info.BackoffCoefficient = v.(float64)
		case "max_interval":
			info.MaximumInterval = (int32)(v.(int))
		case "max_attempts":
			info.MaximumAttempts = (int32)(v.(int))
		case "expiration_time":
			info.ExpirationTime = v.(time.Time)
		case "non_retriable_errors":
			info.NonRetriableErrors = v.([]string)
		}
	}

	return info
}

func createTimerInfo(result map[string]interface{}) *TimerInfo {
	info := &TimerInfo{}
	for k, v := range result {
		switch k {
		case "version":
			info.Version = v.(int64)
		case "timer_id":
			info.TimerID = v.(string)
		case "started_id":
			info.StartedID = v.(int64)
		case "expiry_time":
			info.ExpiryTime = v.(time.Time)
		case "task_id":
			info.TaskID = v.(int64)
		}
	}
	return info
}

func createChildExecutionInfo(result map[string]interface{}) *ChildExecutionInfo {
	info := &ChildExecutionInfo{}
	for k, v := range result {
		switch k {
		case "version":
			info.Version = v.(int64)
		case "initiated_id":
			info.InitiatedID = v.(int64)
		case "initiated_event":
			info.InitiatedEvent = v.([]byte)
		case "started_id":
			info.StartedID = v.(int64)
		case "started_event":
			info.StartedEvent = v.([]byte)
		case "create_request_id":
			info.CreateRequestID = v.(gocql.UUID).String()
		}
	}

	return info
}

func createRequestCancelInfo(result map[string]interface{}) *RequestCancelInfo {
	info := &RequestCancelInfo{}
	for k, v := range result {
		switch k {
		case "version":
			info.Version = v.(int64)
		case "initiated_id":
			info.InitiatedID = v.(int64)
		case "cancel_request_id":
			info.CancelRequestID = v.(string)
		}
	}

	return info
}

func createSignalInfo(result map[string]interface{}) *SignalInfo {
	info := &SignalInfo{}
	for k, v := range result {
		switch k {
		case "version":
			info.Version = v.(int64)
		case "initiated_id":
			info.InitiatedID = v.(int64)
		case "signal_request_id":
			info.SignalRequestID = v.(gocql.UUID).String()
		case "signal_name":
			info.SignalName = v.(string)
		case "input":
			info.Input = v.([]byte)
		case "control":
			info.Control = v.([]byte)
		}
	}

	return info
}

func createBufferedReplicationTaskInfo(result map[string]interface{}) *BufferedReplicationTask {
	info := &BufferedReplicationTask{}
	for k, v := range result {
		switch k {
		case "first_event_id":
			info.FirstEventID = v.(int64)
		case "next_event_id":
			info.NextEventID = v.(int64)
		case "version":
			info.Version = v.(int64)
		case "history":
			h := v.(map[string]interface{})
			info.History = createSerializedHistoryEventBatch(h)
		case "new_run_history":
			h := v.(map[string]interface{})
			info.NewRunHistory = createSerializedHistoryEventBatch(h)
		}
	}

	return info
}

func resetActivityInfoMap(activityInfos []*ActivityInfo) map[int64]map[string]interface{} {
	aMap := make(map[int64]map[string]interface{})
	for _, a := range activityInfos {
		aInfo := make(map[string]interface{})
		aInfo["version"] = a.Version
		aInfo["schedule_id"] = a.ScheduleID
		aInfo["scheduled_event"] = a.ScheduledEvent
		aInfo["scheduled_time"] = a.ScheduledTime
		aInfo["started_id"] = a.StartedID
		aInfo["started_event"] = a.StartedEvent
		aInfo["started_time"] = a.StartedTime
		aInfo["activity_id"] = a.ActivityID
		aInfo["request_id"] = a.RequestID
		aInfo["details"] = a.Details
		aInfo["schedule_to_start_timeout"] = a.ScheduleToStartTimeout
		aInfo["schedule_to_close_timeout"] = a.ScheduleToCloseTimeout
		aInfo["start_to_close_timeout"] = a.StartToCloseTimeout
		aInfo["heart_beat_timeout"] = a.HeartbeatTimeout
		aInfo["cancel_requested"] = a.CancelRequested
		aInfo["cancel_request_id"] = a.CancelRequestID
		aInfo["last_hb_updated_time"] = a.LastHeartBeatUpdatedTime
		aInfo["timer_task_status"] = a.TimerTaskStatus
		aInfo["attempt"] = a.Attempt
		aInfo["task_list"] = a.TaskList
		aInfo["started_identity"] = a.StartedIdentity
		aInfo["has_retry_policy"] = a.HasRetryPolicy
		aInfo["init_interval"] = a.InitialInterval
		aInfo["backoff_coefficient"] = a.BackoffCoefficient
		aInfo["max_interval"] = a.MaximumInterval
		aInfo["expiration_time"] = a.ExpirationTime
		aInfo["max_attempts"] = a.MaximumAttempts
		aInfo["non_retriable_errors"] = a.NonRetriableErrors

		aMap[a.ScheduleID] = aInfo
	}

	return aMap
}

func resetTimerInfoMap(timerInfos []*TimerInfo) map[string]map[string]interface{} {
	tMap := make(map[string]map[string]interface{})
	for _, t := range timerInfos {
		tInfo := make(map[string]interface{})
		tInfo["version"] = t.Version
		tInfo["timer_id"] = t.TimerID
		tInfo["started_id"] = t.StartedID
		tInfo["expiry_time"] = t.ExpiryTime
		tInfo["task_id"] = t.TaskID

		tMap[t.TimerID] = tInfo
	}

	return tMap
}

func resetChildExecutionInfoMap(childExecutionInfos []*ChildExecutionInfo) map[int64]map[string]interface{} {
	cMap := make(map[int64]map[string]interface{})
	for _, c := range childExecutionInfos {
		cInfo := make(map[string]interface{})
		cInfo["version"] = c.Version
		cInfo["initiated_id"] = c.InitiatedID
		cInfo["initiated_event"] = c.InitiatedEvent
		cInfo["started_id"] = c.StartedID
		cInfo["started_event"] = c.StartedEvent
		cInfo["create_request_id"] = c.CreateRequestID

		cMap[c.InitiatedID] = cInfo
	}

	return cMap
}

func resetRequestCancelInfoMap(requestCancelInfos []*RequestCancelInfo) map[int64]map[string]interface{} {
	rcMap := make(map[int64]map[string]interface{})
	for _, rc := range requestCancelInfos {
		rcInfo := make(map[string]interface{})
		rcInfo["version"] = rc.Version
		rcInfo["initiated_id"] = rc.InitiatedID
		rcInfo["cancel_request_id"] = rc.CancelRequestID

		rcMap[rc.InitiatedID] = rcInfo
	}

	return rcMap
}

func resetSignalInfoMap(signalInfos []*SignalInfo) map[int64]map[string]interface{} {
	sMap := make(map[int64]map[string]interface{})
	for _, s := range signalInfos {
		sInfo := make(map[string]interface{})
		sInfo["version"] = s.Version
		sInfo["initiated_id"] = s.InitiatedID
		sInfo["signal_request_id"] = s.SignalRequestID
		sInfo["signal_name"] = s.SignalName
		sInfo["input"] = s.Input
		sInfo["control"] = s.Control

		sMap[s.InitiatedID] = sInfo
	}

	return sMap
}

func createSerializedHistoryEventBatch(result map[string]interface{}) *SerializedHistoryEventBatch {
	// TODO: default to JSON, update this when we support different encoding types.
	eventBatch := &SerializedHistoryEventBatch{EncodingType: common.EncodingTypeJSON}
	for k, v := range result {
		switch k {
		case "version":
			eventBatch.Version = v.(int)
		case "data":
			eventBatch.Data = v.([]byte)
		}
	}

	return eventBatch
}

func createTaskInfo(result map[string]interface{}) *TaskInfo {
	info := &TaskInfo{}
	for k, v := range result {
		switch k {
		case "domain_id":
			info.DomainID = v.(gocql.UUID).String()
		case "workflow_id":
			info.WorkflowID = v.(string)
		case "run_id":
			info.RunID = v.(gocql.UUID).String()
		case "schedule_id":
			info.ScheduleID = v.(int64)
		}
	}

	return info
}

func createTimerTaskInfo(result map[string]interface{}) *TimerTaskInfo {
	info := &TimerTaskInfo{}
	for k, v := range result {
		switch k {
		case "domain_id":
			info.DomainID = v.(gocql.UUID).String()
		case "workflow_id":
			info.WorkflowID = v.(string)
		case "run_id":
			info.RunID = v.(gocql.UUID).String()
		case "visibility_ts":
			info.VisibilityTimestamp = v.(time.Time)
		case "task_id":
			info.TaskID = v.(int64)
		case "type":
			info.TaskType = v.(int)
		case "timeout_type":
			info.TimeoutType = v.(int)
		case "event_id":
			info.EventID = v.(int64)
		case "schedule_attempt":
			info.ScheduleAttempt = v.(int64)
		case "version":
			info.Version = v.(int64)
		}
	}

	return info
}

func createReplicationInfo(result map[string]interface{}) *ReplicationInfo {
	info := &ReplicationInfo{}
	for k, v := range result {
		switch k {
		case "version":
			info.Version = v.(int64)
		case "last_event_id":
			info.LastEventID = v.(int64)
		}
	}

	return info
}

func createReplicationInfoMap(info *ReplicationInfo) map[string]interface{} {
	rInfoMap := make(map[string]interface{})
	rInfoMap["version"] = info.Version
	rInfoMap["last_event_id"] = info.LastEventID

	return rInfoMap
}

func isTimeoutError(err error) bool {
	if err == gocql.ErrTimeoutNoResponse {
		return true
	}
	if err == gocql.ErrConnectionClosed {
		return true
	}
	_, ok := err.(*gocql.RequestErrWriteTimeout)
	return ok
}

func isThrottlingError(err error) bool {
	if req, ok := err.(gocql.RequestError); ok {
		// gocql does not expose the constant errOverloaded = 0x1001
		return req.Code() == 0x1001
	}
	return false
}

// GetVisibilityTSFrom - helper method to get visibility timestamp
func GetVisibilityTSFrom(task Task) time.Time {
	switch task.GetType() {
	case TaskTypeDecisionTimeout:
		return task.(*DecisionTimeoutTask).VisibilityTimestamp

	case TaskTypeActivityTimeout:
		return task.(*ActivityTimeoutTask).VisibilityTimestamp

	case TaskTypeUserTimer:
		return task.(*UserTimerTask).VisibilityTimestamp

	case TaskTypeWorkflowTimeout:
		return task.(*WorkflowTimeoutTask).VisibilityTimestamp

	case TaskTypeDeleteHistoryEvent:
		return task.(*DeleteHistoryEventTask).VisibilityTimestamp

	case TaskTypeActivityRetryTimer:
		return task.(*ActivityRetryTimerTask).VisibilityTimestamp

	case TaskTypeWorkflowRetryTimer:
		return task.(*WorkflowRetryTimerTask).VisibilityTimestamp
	}
	return time.Time{}
}

// SetVisibilityTSFrom - helper method to set visibility timestamp
func SetVisibilityTSFrom(task Task, t time.Time) {
	switch task.GetType() {
	case TaskTypeDecisionTimeout:
		task.(*DecisionTimeoutTask).VisibilityTimestamp = t

	case TaskTypeActivityTimeout:
		task.(*ActivityTimeoutTask).VisibilityTimestamp = t

	case TaskTypeUserTimer:
		task.(*UserTimerTask).VisibilityTimestamp = t

	case TaskTypeWorkflowTimeout:
		task.(*WorkflowTimeoutTask).VisibilityTimestamp = t

	case TaskTypeDeleteHistoryEvent:
		task.(*DeleteHistoryEventTask).VisibilityTimestamp = t

	case TaskTypeActivityRetryTimer:
		task.(*ActivityRetryTimerTask).VisibilityTimestamp = t

	case TaskTypeWorkflowRetryTimer:
		task.(*WorkflowRetryTimerTask).VisibilityTimestamp = t
	}
}
