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
	"strings"
	"time"

	"github.com/gocql/gocql"
	"github.com/uber-common/bark"

	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	p "github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/service/config"
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
	emptyRunID     = "30000000-0000-f000-f000-000000000000"
	permanentRunID = "30000000-0000-f000-f000-000000000001"
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
		`completion_event_data_encoding: ?, ` +
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
		`signal_count: ?, ` +
		`history_size: ?, ` +
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
		`non_retriable_errors: ?, ` +
		`event_store_version: ?, ` +
		`branch_token: ?, ` +
		`cron_schedule: ?, ` +
		`expiration_seconds: ? ` +
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
		`record_visibility: ?, ` +
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
		`last_replication_info: ?, ` +
		`scheduled_id: ?, ` +
		`event_store_version: ?, ` +
		`branch_token: ?, ` +
		`reset_workflow: ?, ` +
		`new_run_first_event_id: ?,` +
		`new_run_next_event_id: ?,` +
		`new_run_event_store_version: ?, ` +
		`new_run_branch_token: ? ` +
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
		`non_retriable_errors: ?, ` +
		`event_data_encoding: ?` +
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
		`create_request_id: ?, ` +
		`event_data_encoding: ?` +
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
		`event_store_version: ?, ` +
		`new_run_event_store_version: ?, ` +
		`history: ` + templateSerializedEventBatch + `, ` +
		`new_run_history: ` + templateSerializedEventBatch + ` ` +
		`}`

	templateBufferedReplicationTaskInfoNoNewRunHistoryType = `{` +
		`first_event_id: ?, ` +
		`next_event_id: ?, ` +
		`version: ?, ` +
		`event_store_version: ?, ` +
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
		`SET current_run_id = ?, 
execution = {run_id: ?, create_request_id: ?, state: ?, close_status: ?}, 
replication_state = {start_version: ?, last_write_version: ?}, 
workflow_last_write_version = ?, 
workflow_state = ? ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and domain_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? ` +
		`IF current_run_id = ? `

	templateUpdateCurrentWorkflowExecutionForNewQuery = templateUpdateCurrentWorkflowExecutionQuery +
		`and workflow_last_write_version = ? ` +
		`and workflow_state = ? `

	templateCreateCurrentWorkflowExecutionQuery = `INSERT INTO executions (` +
		`shard_id, type, domain_id, workflow_id, run_id, visibility_ts, task_id, current_run_id, execution, replication_state, workflow_last_write_version, workflow_state) ` +
		`VALUES(?, ?, ?, ?, ?, ?, ?, ?, {run_id: ?, create_request_id: ?, state: ?, close_status: ?}, {start_version: ?, last_write_version: ?}, ?, ?) IF NOT EXISTS USING TTL 0 `

	templateDeleteCurrentWorkflowExecutionQueryWithTTL = `INSERT INTO executions ` +
		`(shard_id, type, domain_id, workflow_id, run_id, visibility_ts, task_id, current_run_id, execution, replication_state, workflow_last_write_version, workflow_state) ` +
		`VALUES(?, ?, ?, ?, ?, ?, ?, ?, {run_id: ?, create_request_id: ?, state: ?, close_status: ?}, {start_version: ?, last_write_version: ?}, ?, ?) USING TTL ? `

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

	templateCheckWorkflowExecutionQuery = `UPDATE executions ` +
		`SET next_event_id = ? ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and domain_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? ` +
		`IF next_event_id = ?`

	templateUpdateWorkflowExecutionConditionSuffix = ` IF next_event_id = ?`

	templateUpdateWorkflowExecutionQuery = `UPDATE executions ` +
		`SET execution = ` + templateWorkflowExecutionType + `, next_event_id = ? ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and domain_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? `

	templateUpdateWorkflowExecutionWithReplicationQuery = `UPDATE executions ` +
		`SET execution = ` + templateWorkflowExecutionType + `, replication_state = ` + templateReplicationStateType + `, next_event_id = ? ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and domain_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? `

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
		`and task_id = ? `

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
		`and task_id = ? `

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
		`and task_id = ? `

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
		`and task_id = ? `

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
		`and task_id = ? `

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
		`and task_id = ? `

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
	defaultVisibilityTimestamp = p.UnixNanoToDBTimestamp(defaultDateTime.UnixNano())
)

type (
	cassandraStore struct {
		session *gocql.Session
		logger  bark.Logger
	}

	// Implements ExecutionManager, ShardManager and TaskManager
	cassandraPersistence struct {
		cassandraStore
		shardID            int
		currentClusterName string
	}
)

//NewWorkflowExecutionPersistenceFromSession returns new ExecutionStore
func NewWorkflowExecutionPersistenceFromSession(session *gocql.Session, shardID int, logger bark.Logger) p.ExecutionStore {
	return &cassandraPersistence{cassandraStore: cassandraStore{session: session, logger: logger}, shardID: shardID}
}

// newShardPersistence is used to create an instance of ShardManager implementation
func newShardPersistence(cfg config.Cassandra, clusterName string, logger bark.Logger) (p.ShardStore, error) {
	cluster := NewCassandraCluster(cfg.Hosts, cfg.Port, cfg.User, cfg.Password, cfg.Datacenter)
	cluster.Keyspace = cfg.Keyspace
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
func NewWorkflowExecutionPersistence(shardID int, session *gocql.Session,
	logger bark.Logger) (p.ExecutionStore, error) {
	return &cassandraPersistence{cassandraStore: cassandraStore{session: session, logger: logger}, shardID: shardID}, nil
}

// newTaskPersistence is used to create an instance of TaskManager implementation
func newTaskPersistence(cfg config.Cassandra, logger bark.Logger) (p.TaskStore, error) {
	cluster := NewCassandraCluster(cfg.Hosts, cfg.Port, cfg.User, cfg.Password, cfg.Datacenter)
	cluster.Keyspace = cfg.Keyspace
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
	cqlNowTimestamp := p.UnixNanoToDBTimestamp(time.Now().UnixNano())
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
			Message: fmt.Sprintf("CreateShard operation failed. Error: %v", err),
		}
	}

	if !applied {
		shard := previous["shard"].(map[string]interface{})
		return &p.ShardAlreadyExistError{
			Msg: fmt.Sprintf("Shard already exists in executions table.  ShardId: %v, RangeId: %v",
				shard["shard_id"], shard["range_id"]),
		}
	}

	return nil
}

func (d *cassandraPersistence) GetShard(request *p.GetShardRequest) (*p.GetShardResponse, error) {
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

	return &p.GetShardResponse{ShardInfo: info}, nil
}

func (d *cassandraPersistence) UpdateShard(request *p.UpdateShardRequest) error {
	cqlNowTimestamp := p.UnixNanoToDBTimestamp(time.Now().UnixNano())
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

		return &p.ShardOwnershipLostError{
			ShardID: d.shardID,
			Msg: fmt.Sprintf("Failed to update shard.  previous_range_id: %v, columns: (%v)",
				request.PreviousRangeID, strings.Join(columns, ",")),
		}
	}

	return nil
}

func (d *cassandraPersistence) CreateWorkflowExecution(request *p.CreateWorkflowExecutionRequest) (
	*p.CreateWorkflowExecutionResponse, error) {
	cqlNowTimestamp := p.UnixNanoToDBTimestamp(time.Now().UnixNano())
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
			return nil, &p.TimeoutError{Msg: fmt.Sprintf("CreateWorkflowExecution timed out. Error: %v", err)}
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

				if execution, ok := previous["execution"].(map[string]interface{}); ok {
					// CreateWorkflowExecution failed because it already exists
					executionInfo := createWorkflowExecutionInfo(execution)

					lastWriteVersion := common.EmptyVersion
					if request.ReplicationState != nil {
						replicationState := createReplicationState(previous["replication_state"].(map[string]interface{}))
						lastWriteVersion = replicationState.LastWriteVersion
					}

					msg := fmt.Sprintf("Workflow execution already running. WorkflowId: %v, RunId: %v, rangeID: %v, columns: (%v)",
						request.Execution.GetWorkflowId(), executionInfo.RunID, request.RangeID, strings.Join(columns, ","))
					return nil, &p.WorkflowExecutionAlreadyStartedError{
						Msg:              msg,
						StartRequestID:   executionInfo.CreateRequestID,
						RunID:            executionInfo.RunID,
						State:            executionInfo.State,
						CloseStatus:      executionInfo.CloseStatus,
						LastWriteVersion: lastWriteVersion,
					}
				}

				if prevRunID := previous["current_run_id"].(gocql.UUID).String(); prevRunID != request.Execution.GetRunId() {
					// currentRunID on previous run has been changed, return to caller to handle
					msg := fmt.Sprintf("Workflow execution creation condition failed by mismatch runID. WorkflowId: %v, CurrentRunID: %v, columns: (%v)",
						request.Execution.GetWorkflowId(), request.Execution.GetRunId(), strings.Join(columns, ","))
					return nil, &p.CurrentWorkflowConditionFailedError{Msg: msg}
				}

				msg := fmt.Sprintf("Workflow execution creation condition failed. WorkflowId: %v, CurrentRunID: %v, columns: (%v)",
					request.Execution.GetWorkflowId(), request.Execution.GetRunId(), strings.Join(columns, ","))
				return nil, &p.ConditionFailedError{Msg: msg}
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

func (d *cassandraPersistence) CreateWorkflowExecutionWithinBatch(request *p.CreateWorkflowExecutionRequest,
	batch *gocql.Batch, cqlNowTimestamp int64) {

	parentDomainID := emptyDomainID
	parentWorkflowID := ""
	parentRunID := emptyRunID
	initiatedID := emptyInitiatedID
	state := p.WorkflowStateRunning
	closeStatus := p.WorkflowCloseStatusNone
	if request.ParentExecution != nil {
		parentDomainID = request.ParentDomainID
		parentWorkflowID = *request.ParentExecution.WorkflowId
		parentRunID = *request.ParentExecution.RunId
		initiatedID = request.InitiatedID
		state = p.WorkflowStateCreated
	}

	startVersion := common.EmptyVersion
	lastWriteVersion := common.EmptyVersion
	if request.ReplicationState != nil {
		startVersion = request.ReplicationState.StartVersion
		lastWriteVersion = request.ReplicationState.LastWriteVersion
	} else {
		// this is to deal with issue that gocql cannot return null value for value inside user defined type
		// so we cannot know whether the last write version of current workflow record is null or not
		// since non global domain (request.ReplicationState == null) will not have workflow reset problem
		// so the CAS on last write version is not necessary
		if request.CreateWorkflowMode == p.CreateWorkflowModeWorkflowIDReuse {
			request.CreateWorkflowMode = p.CreateWorkflowModeContinueAsNew
		}
	}
	switch request.CreateWorkflowMode {
	case p.CreateWorkflowModeContinueAsNew:
		batch.Query(templateUpdateCurrentWorkflowExecutionQuery,
			*request.Execution.RunId,
			*request.Execution.RunId,
			request.RequestID,
			state,
			closeStatus,
			startVersion,
			lastWriteVersion,
			lastWriteVersion,
			state,
			d.shardID,
			rowTypeExecution,
			request.DomainID,
			*request.Execution.WorkflowId,
			permanentRunID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID,
			request.PreviousRunID,
		)
	case p.CreateWorkflowModeWorkflowIDReuse:
		batch.Query(templateUpdateCurrentWorkflowExecutionForNewQuery,
			*request.Execution.RunId,
			*request.Execution.RunId,
			request.RequestID,
			state,
			closeStatus,
			startVersion,
			lastWriteVersion,
			lastWriteVersion,
			state,
			d.shardID,
			rowTypeExecution,
			request.DomainID,
			*request.Execution.WorkflowId,
			permanentRunID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID,
			request.PreviousRunID,
			request.PreviousLastWriteVersion,
			p.WorkflowStateCompleted,
		)
	case p.CreateWorkflowModeBrandNew:
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
			lastWriteVersion,
			lastWriteVersion,
			state,
		)
	default:
		d.logger.Panic(fmt.Sprintf("Unknown CreateWorkflowMode: %v", request.CreateWorkflowMode))
	}

	// TODO use updateMutableState() with useCondition=false to make code much cleaner
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
			"",
			request.TaskList,
			request.WorkflowTypeName,
			request.WorkflowTimeout,
			request.DecisionTimeoutValue,
			request.ExecutionContext,
			p.WorkflowStateCreated,
			p.WorkflowCloseStatusNone,
			common.FirstEventID,
			request.NextEventID,
			request.LastProcessedEvent,
			cqlNowTimestamp,
			cqlNowTimestamp,
			request.RequestID,
			request.SignalCount,
			request.HistorySize,
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
			request.EventStoreVersion,
			request.BranchToken,
			request.CronSchedule,
			request.ExpirationSeconds,
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
			"",
			request.TaskList,
			request.WorkflowTypeName,
			request.WorkflowTimeout,
			request.DecisionTimeoutValue,
			request.ExecutionContext,
			p.WorkflowStateCreated,
			p.WorkflowCloseStatusNone,
			common.FirstEventID,
			request.NextEventID,
			request.LastProcessedEvent,
			cqlNowTimestamp,
			cqlNowTimestamp,
			request.RequestID,
			request.SignalCount,
			request.HistorySize,
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
			request.EventStoreVersion,
			request.BranchToken,
			request.CronSchedule,
			request.ExpirationSeconds,
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

func (d *cassandraPersistence) GetWorkflowExecution(request *p.GetWorkflowExecutionRequest) (
	*p.InternalGetWorkflowExecutionResponse, error) {
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

	state := &p.InternalWorkflowMutableState{}
	info := createWorkflowExecutionInfo(result["execution"].(map[string]interface{}))
	state.ExecutionInfo = info

	replicationState := createReplicationState(result["replication_state"].(map[string]interface{}))
	state.ReplicationState = replicationState

	activityInfos := make(map[int64]*p.InternalActivityInfo)
	aMap := result["activity_map"].(map[int64]map[string]interface{})
	for key, value := range aMap {
		info := createActivityInfo(value)
		activityInfos[key] = info
	}
	state.ActivitInfos = activityInfos

	timerInfos := make(map[string]*p.TimerInfo)
	tMap := result["timer_map"].(map[string]map[string]interface{})
	for key, value := range tMap {
		info := createTimerInfo(value)
		timerInfos[key] = info
	}
	state.TimerInfos = timerInfos

	childExecutionInfos := make(map[int64]*p.InternalChildExecutionInfo)
	cMap := result["child_executions_map"].(map[int64]map[string]interface{})
	for key, value := range cMap {
		info := createChildExecutionInfo(value, d.logger)
		childExecutionInfos[key] = info
	}
	state.ChildExecutionInfos = childExecutionInfos

	requestCancelInfos := make(map[int64]*p.RequestCancelInfo)
	rMap := result["request_cancel_map"].(map[int64]map[string]interface{})
	for key, value := range rMap {
		info := createRequestCancelInfo(value)
		requestCancelInfos[key] = info
	}
	state.RequestCancelInfos = requestCancelInfos

	signalInfos := make(map[int64]*p.SignalInfo)
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
	bufferedEventsBlobs := make([]*p.DataBlob, 0, len(eList))
	for _, v := range eList {
		blob := createHistoryEventBatchBlob(v)
		bufferedEventsBlobs = append(bufferedEventsBlobs, blob)
	}
	state.BufferedEvents = bufferedEventsBlobs

	bufferedReplicationTasks := make(map[int64]*p.InternalBufferedReplicationTask)
	bufferedRTMap := result["buffered_replication_tasks_map"].(map[int64]map[string]interface{})
	for k, v := range bufferedRTMap {
		info := createBufferedReplicationTaskInfo(v)
		bufferedReplicationTasks[k] = info
	}
	state.BufferedReplicationTasks = bufferedReplicationTasks

	return &p.InternalGetWorkflowExecutionResponse{State: state}, nil
}

// this helper is passing dynamic number of arguments based on whether needing condition or not
func batchQueryHelper(batch *gocql.Batch, stmt string, useCondition bool, condition int64, args ...interface{}) {
	if useCondition {
		stmt += templateUpdateWorkflowExecutionConditionSuffix
		args = append(args, condition)
	}
	batch.Query(stmt, args...)
}

func (d *cassandraPersistence) updateMutableState(batch *gocql.Batch, executionInfo *p.InternalWorkflowExecutionInfo, replicationState *p.ReplicationState, cqlNowTimestamp int64, useCondition bool, condition int64) {
	if executionInfo.ParentDomainID == "" {
		executionInfo.ParentDomainID = emptyDomainID
	}
	if executionInfo.ParentRunID == "" {
		executionInfo.ParentRunID = emptyRunID
	}

	completionData, completionEncoding := p.FromDataBlob(executionInfo.CompletionEvent)
	if replicationState == nil {
		// Updates will be called with null ReplicationState while the feature is disabled
		batchQueryHelper(batch, templateUpdateWorkflowExecutionQuery, useCondition, condition,
			executionInfo.DomainID,
			executionInfo.WorkflowID,
			executionInfo.RunID,
			executionInfo.ParentDomainID,
			executionInfo.ParentWorkflowID,
			executionInfo.ParentRunID,
			executionInfo.InitiatedID,
			completionData,
			completionEncoding,
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
			executionInfo.SignalCount,
			executionInfo.HistorySize,
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
			executionInfo.EventStoreVersion,
			executionInfo.BranchToken,
			executionInfo.CronSchedule,
			executionInfo.ExpirationSeconds,
			executionInfo.NextEventID,
			d.shardID,
			rowTypeExecution,
			executionInfo.DomainID,
			executionInfo.WorkflowID,
			executionInfo.RunID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID)
	} else {
		lastReplicationInfo := make(map[string]map[string]interface{})
		for k, v := range replicationState.LastReplicationInfo {
			lastReplicationInfo[k] = createReplicationInfoMap(v)
		}

		batchQueryHelper(batch, templateUpdateWorkflowExecutionWithReplicationQuery, useCondition, condition,
			executionInfo.DomainID,
			executionInfo.WorkflowID,
			executionInfo.RunID,
			executionInfo.ParentDomainID,
			executionInfo.ParentWorkflowID,
			executionInfo.ParentRunID,
			executionInfo.InitiatedID,
			completionData,
			completionEncoding,
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
			executionInfo.SignalCount,
			executionInfo.HistorySize,
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
			executionInfo.EventStoreVersion,
			executionInfo.BranchToken,
			executionInfo.CronSchedule,
			executionInfo.ExpirationSeconds,
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
			rowTypeExecutionTaskID)
	}
}

func (d *cassandraPersistence) UpdateWorkflowExecution(request *p.InternalUpdateWorkflowExecutionRequest) error {
	batch := d.session.NewBatch(gocql.LoggedBatch)
	cqlNowTimestamp := p.UnixNanoToDBTimestamp(time.Now().UnixNano())
	executionInfo := request.ExecutionInfo
	replicationState := request.ReplicationState

	d.updateMutableState(batch, executionInfo, replicationState, cqlNowTimestamp, true, request.Condition)

	d.createTransferTasks(batch, request.TransferTasks, executionInfo.DomainID, executionInfo.WorkflowID,
		executionInfo.RunID)

	d.createReplicationTasks(batch, request.ReplicationTasks, executionInfo.DomainID, executionInfo.WorkflowID,
		executionInfo.RunID)

	d.createTimerTasks(batch, request.TimerTasks, request.DeleteTimerTask, request.ExecutionInfo.DomainID,
		executionInfo.WorkflowID, executionInfo.RunID, cqlNowTimestamp)

	err := d.updateActivityInfos(batch, request.UpsertActivityInfos, request.DeleteActivityInfos, executionInfo.DomainID,
		executionInfo.WorkflowID, executionInfo.RunID, request.Condition, request.RangeID)
	if err != nil {
		return err
	}

	d.updateTimerInfos(batch, request.UpserTimerInfos, request.DeleteTimerInfos, executionInfo.DomainID,
		executionInfo.WorkflowID, executionInfo.RunID, request.Condition, request.RangeID)

	err = d.updateChildExecutionInfos(batch, request.UpsertChildExecutionInfos, request.DeleteChildExecutionInfo,
		executionInfo.DomainID, executionInfo.WorkflowID, executionInfo.RunID, request.Condition, request.RangeID)
	if err != nil {
		return err
	}

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
	} else {
		startVersion := common.EmptyVersion
		lastWriteVersion := common.EmptyVersion
		if request.ReplicationState != nil {
			startVersion = request.ReplicationState.StartVersion
			lastWriteVersion = request.ReplicationState.LastWriteVersion
		}
		if request.FinishExecution {
			retentionInSeconds := request.FinishedExecutionTTL
			if retentionInSeconds <= 0 {
				retentionInSeconds = minCurrentExecutionRetentionTTL
			}

			// Delete WorkflowExecution row representing current execution, by using a TTL
			batch.Query(templateDeleteCurrentWorkflowExecutionQueryWithTTL,
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
				lastWriteVersion,
				lastWriteVersion,
				executionInfo.State,
				retentionInSeconds,
			)
		} else {
			batch.Query(templateUpdateCurrentWorkflowExecutionQuery,
				executionInfo.RunID,
				executionInfo.RunID,
				executionInfo.CreateRequestID,
				executionInfo.State,
				executionInfo.CloseStatus,
				startVersion,
				lastWriteVersion,
				lastWriteVersion,
				executionInfo.State,
				d.shardID,
				rowTypeExecution,
				executionInfo.DomainID,
				executionInfo.WorkflowID,
				permanentRunID,
				defaultVisibilityTimestamp,
				rowTypeExecutionTaskID,
				executionInfo.RunID,
			)
		}
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
			return &p.TimeoutError{Msg: fmt.Sprintf("UpdateWorkflowExecution timed out. Error: %v", err)}
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
		return d.getExecutionConditionalUpdateFailure(previous, iter, executionInfo.RunID, request.Condition, request.RangeID, executionInfo.RunID)
	}
	return nil
}

func (d *cassandraPersistence) ResetWorkflowExecution(request *p.InternalResetWorkflowExecutionRequest) error {
	batch := d.session.NewBatch(gocql.LoggedBatch)
	cqlNowTimestamp := p.UnixNanoToDBTimestamp(time.Now().UnixNano())

	currExecutionInfo := request.CurrExecutionInfo
	currReplicationState := request.CurrReplicationState

	insertExecutionInfo := request.InsertExecutionInfo
	insertReplicationState := request.InsertReplicationState

	startVersion := common.EmptyVersion
	lastWriteVersion := common.EmptyVersion
	if insertReplicationState != nil {
		startVersion = insertReplicationState.StartVersion
		lastWriteVersion = insertReplicationState.LastWriteVersion
	}
	batch.Query(templateUpdateCurrentWorkflowExecutionQuery,
		insertExecutionInfo.RunID,
		insertExecutionInfo.RunID,
		insertExecutionInfo.CreateRequestID,
		insertExecutionInfo.State,
		insertExecutionInfo.CloseStatus,
		startVersion,
		lastWriteVersion,
		lastWriteVersion,
		insertExecutionInfo.State,
		d.shardID,
		rowTypeExecution,
		insertExecutionInfo.DomainID,
		insertExecutionInfo.WorkflowID,
		permanentRunID,
		defaultVisibilityTimestamp,
		rowTypeExecutionTaskID,
		request.PrevRunID,
	)

	if request.UpdateCurr {
		d.updateMutableState(batch, currExecutionInfo, currReplicationState, cqlNowTimestamp, true, request.Condition)
		d.createTimerTasks(batch, request.CurrTimerTasks, nil, currExecutionInfo.DomainID, currExecutionInfo.WorkflowID, currExecutionInfo.RunID, cqlNowTimestamp)
		d.createTransferTasks(batch, request.CurrTransferTasks, currExecutionInfo.DomainID, currExecutionInfo.WorkflowID, currExecutionInfo.RunID)
	} else {
		// check condition without updating anything
		batch.Query(templateCheckWorkflowExecutionQuery,
			request.Condition,
			d.shardID,
			rowTypeExecution,
			currExecutionInfo.DomainID,
			currExecutionInfo.WorkflowID,
			currExecutionInfo.RunID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID,
			request.Condition,
		)
	}

	if len(request.InsertReplicationTasks) > 0 {
		d.createReplicationTasks(batch, request.InsertReplicationTasks, currExecutionInfo.DomainID, currExecutionInfo.WorkflowID, currExecutionInfo.RunID)
	}

	// we need to insert new mutableState, there is no condition to check. We use update without condition as insert
	d.updateMutableState(batch, insertExecutionInfo, insertReplicationState, cqlNowTimestamp, false, 0)

	if len(request.InsertActivityInfos) > 0 {
		d.resetActivityInfos(batch, request.InsertActivityInfos, insertExecutionInfo.DomainID, insertExecutionInfo.WorkflowID,
			insertExecutionInfo.RunID, false, 0)
	}

	if len(request.InsertTimerInfos) > 0 {
		d.resetTimerInfos(batch, request.InsertTimerInfos, insertExecutionInfo.DomainID, insertExecutionInfo.WorkflowID,
			insertExecutionInfo.RunID, false, 0)
	}

	if len(request.InsertRequestCancelInfos) > 0 {
		d.resetRequestCancelInfos(batch, request.InsertRequestCancelInfos, insertExecutionInfo.DomainID, insertExecutionInfo.WorkflowID,
			insertExecutionInfo.RunID, false, 0)
	}

	if len(request.InsertChildExecutionInfos) > 0 {
		d.resetChildExecutionInfos(batch, request.InsertChildExecutionInfos, insertExecutionInfo.DomainID, insertExecutionInfo.WorkflowID,
			insertExecutionInfo.RunID, false, 0)
	}

	if len(request.InsertSignalInfos) > 0 {
		d.resetSignalInfos(batch, request.InsertSignalInfos, insertExecutionInfo.DomainID, insertExecutionInfo.WorkflowID,
			insertExecutionInfo.RunID, false, 0)
	}

	if len(request.InsertSignalRequestedIDs) > 0 {
		d.resetSignalRequested(batch, request.InsertSignalRequestedIDs, insertExecutionInfo.DomainID, insertExecutionInfo.WorkflowID,
			insertExecutionInfo.RunID, false, 0)
	}

	if len(request.InsertTimerTasks) > 0 {
		d.createTimerTasks(batch, request.InsertTimerTasks, nil, insertExecutionInfo.DomainID, insertExecutionInfo.WorkflowID, insertExecutionInfo.RunID, cqlNowTimestamp)
	}

	if len(request.InsertTransferTasks) > 0 {
		d.createTransferTasks(batch, request.InsertTransferTasks, insertExecutionInfo.DomainID, insertExecutionInfo.WorkflowID, insertExecutionInfo.RunID)
	}

	//Verifies that the RangeID has not changed
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
			return &p.TimeoutError{Msg: fmt.Sprintf("ResetWorkflowExecution timed out. Error: %v", err)}
		} else if isThrottlingError(err) {
			return &workflow.ServiceBusyError{
				Message: fmt.Sprintf("ResetWorkflowExecution operation failed. Error: %v", err),
			}
		}
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("ResetWorkflowExecution operation failed. Error: %v", err),
		}
	}

	if !applied {
		return d.getExecutionConditionalUpdateFailure(previous, iter, currExecutionInfo.RunID, request.Condition, request.RangeID, request.PrevRunID)
	}

	return nil
}

func (d *cassandraPersistence) ResetMutableState(request *p.InternalResetMutableStateRequest) error {
	batch := d.session.NewBatch(gocql.LoggedBatch)
	cqlNowTimestamp := p.UnixNanoToDBTimestamp(time.Now().UnixNano())
	executionInfo := request.ExecutionInfo
	replicationState := request.ReplicationState

	batch.Query(templateUpdateCurrentWorkflowExecutionQuery,
		executionInfo.RunID,
		executionInfo.RunID,
		executionInfo.CreateRequestID,
		executionInfo.State,
		executionInfo.CloseStatus,
		replicationState.StartVersion,
		replicationState.LastWriteVersion,
		replicationState.LastWriteVersion,
		executionInfo.State,
		d.shardID,
		rowTypeExecution,
		executionInfo.DomainID,
		executionInfo.WorkflowID,
		permanentRunID,
		defaultVisibilityTimestamp,
		rowTypeExecutionTaskID,
		request.PrevRunID,
	)

	d.updateMutableState(batch, executionInfo, replicationState, cqlNowTimestamp, true, request.Condition)

	d.resetActivityInfos(batch, request.InsertActivityInfos, executionInfo.DomainID, executionInfo.WorkflowID,
		executionInfo.RunID, true, request.Condition)

	d.resetTimerInfos(batch, request.InsertTimerInfos, executionInfo.DomainID, executionInfo.WorkflowID,
		executionInfo.RunID, true, request.Condition)

	d.resetChildExecutionInfos(batch, request.InsertChildExecutionInfos, executionInfo.DomainID, executionInfo.WorkflowID,
		executionInfo.RunID, true, request.Condition)

	d.resetRequestCancelInfos(batch, request.InsertRequestCancelInfos, executionInfo.DomainID, executionInfo.WorkflowID,
		executionInfo.RunID, true, request.Condition)

	d.resetSignalInfos(batch, request.InsertSignalInfos, executionInfo.DomainID, executionInfo.WorkflowID,
		executionInfo.RunID, true, request.Condition)

	d.resetSignalRequested(batch, request.InsertSignalRequestedIDs, executionInfo.DomainID, executionInfo.WorkflowID,
		executionInfo.RunID, true, request.Condition)

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
			return &p.TimeoutError{Msg: fmt.Sprintf("ResetMutableState timed out. Error: %v", err)}
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
		return d.getExecutionConditionalUpdateFailure(previous, iter, executionInfo.RunID, request.Condition, request.RangeID, request.PrevRunID)
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

func (d *cassandraPersistence) DeleteWorkflowExecution(request *p.DeleteWorkflowExecutionRequest) error {
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

func (d *cassandraPersistence) GetCurrentExecution(request *p.GetCurrentExecutionRequest) (*p.GetCurrentExecutionResponse,
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
	return &p.GetCurrentExecutionResponse{
		RunID:          currentRunID,
		StartRequestID: executionInfo.CreateRequestID,
		State:          executionInfo.State,
		CloseStatus:    executionInfo.CloseStatus,
	}, nil
}

func (d *cassandraPersistence) GetTransferTasks(request *p.GetTransferTasksRequest) (*p.GetTransferTasksResponse, error) {

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

	response := &p.GetTransferTasksResponse{}
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

func (d *cassandraPersistence) GetReplicationTasks(request *p.GetReplicationTasksRequest) (*p.GetReplicationTasksResponse,
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

	response := &p.GetReplicationTasksResponse{}
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

func (d *cassandraPersistence) CompleteTransferTask(request *p.CompleteTransferTaskRequest) error {
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

func (d *cassandraPersistence) RangeCompleteTransferTask(request *p.RangeCompleteTransferTaskRequest) error {
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

func (d *cassandraPersistence) CompleteReplicationTask(request *p.CompleteReplicationTaskRequest) error {
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

func (d *cassandraPersistence) CompleteTimerTask(request *p.CompleteTimerTaskRequest) error {
	ts := p.UnixNanoToDBTimestamp(request.VisibilityTimestamp.UnixNano())
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

func (d *cassandraPersistence) RangeCompleteTimerTask(request *p.RangeCompleteTimerTaskRequest) error {
	start := p.UnixNanoToDBTimestamp(request.InclusiveBeginTimestamp.UnixNano())
	end := p.UnixNanoToDBTimestamp(request.ExclusiveEndTimestamp.UnixNano())
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
func (d *cassandraPersistence) LeaseTaskList(request *p.LeaseTaskListRequest) (*p.LeaseTaskListResponse, error) {
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
		return nil, &p.ConditionFailedError{
			Msg: fmt.Sprintf("LeaseTaskList failed to apply. db rangeID %v", previousRangeID),
		}
	}
	tli := &p.TaskListInfo{DomainID: request.DomainID, Name: request.TaskList, TaskType: request.TaskType, RangeID: rangeID + 1, AckLevel: ackLevel, Kind: request.TaskListKind}
	return &p.LeaseTaskListResponse{TaskListInfo: tli}, nil
}

// From TaskManager interface
func (d *cassandraPersistence) UpdateTaskList(request *p.UpdateTaskListRequest) (*p.UpdateTaskListResponse, error) {
	tli := request.TaskListInfo

	if tli.Kind == p.TaskListKindSticky { // if task_list is sticky, then update with TTL
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
		return &p.UpdateTaskListResponse{}, nil
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

		return nil, &p.ConditionFailedError{
			Msg: fmt.Sprintf("Failed to update task list. name: %v, type: %v, rangeID: %v, columns: (%v)",
				tli.Name, tli.TaskType, tli.RangeID, strings.Join(columns, ",")),
		}
	}

	return &p.UpdateTaskListResponse{}, nil
}

// From TaskManager interface
func (d *cassandraPersistence) CreateTasks(request *p.CreateTasksRequest) (*p.CreateTasksResponse, error) {
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
		return nil, &p.ConditionFailedError{
			Msg: fmt.Sprintf("Failed to create task. TaskList: %v, taskListType: %v, rangeID: %v, db rangeID: %v",
				taskList, taskListType, request.TaskListInfo.RangeID, rangeID),
		}
	}

	return &p.CreateTasksResponse{}, nil
}

// From TaskManager interface
func (d *cassandraPersistence) GetTasks(request *p.GetTasksRequest) (*p.GetTasksResponse, error) {
	if request.ReadLevel > request.MaxReadLevel {
		return &p.GetTasksResponse{}, nil
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

	response := &p.GetTasksResponse{}
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
func (d *cassandraPersistence) CompleteTask(request *p.CompleteTaskRequest) error {
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

func (d *cassandraPersistence) GetTimerIndexTasks(request *p.GetTimerIndexTasksRequest) (*p.GetTimerIndexTasksResponse,
	error) {
	// Reading timer tasks need to be quorum level consistent, otherwise we could loose task
	minTimestamp := p.UnixNanoToDBTimestamp(request.MinTimestamp.UnixNano())
	maxTimestamp := p.UnixNanoToDBTimestamp(request.MaxTimestamp.UnixNano())
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

	response := &p.GetTimerIndexTasksResponse{}
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

func (d *cassandraPersistence) createTransferTasks(batch *gocql.Batch, transferTasks []p.Task, domainID, workflowID,
	runID string) {
	targetDomainID := domainID
	for _, task := range transferTasks {
		var taskList string
		var scheduleID int64
		targetWorkflowID := p.TransferTaskTransferTargetWorkflowID
		targetRunID := p.TransferTaskTransferTargetRunID
		targetChildWorkflowOnly := false
		recordVisibility := false

		switch task.GetType() {
		case p.TransferTaskTypeActivityTask:
			targetDomainID = task.(*p.ActivityTask).DomainID
			taskList = task.(*p.ActivityTask).TaskList
			scheduleID = task.(*p.ActivityTask).ScheduleID

		case p.TransferTaskTypeDecisionTask:
			targetDomainID = task.(*p.DecisionTask).DomainID
			taskList = task.(*p.DecisionTask).TaskList
			scheduleID = task.(*p.DecisionTask).ScheduleID
			recordVisibility = task.(*p.DecisionTask).RecordVisibility

		case p.TransferTaskTypeCancelExecution:
			targetDomainID = task.(*p.CancelExecutionTask).TargetDomainID
			targetWorkflowID = task.(*p.CancelExecutionTask).TargetWorkflowID
			targetRunID = task.(*p.CancelExecutionTask).TargetRunID
			if targetRunID == "" {
				targetRunID = p.TransferTaskTransferTargetRunID
			}
			targetChildWorkflowOnly = task.(*p.CancelExecutionTask).TargetChildWorkflowOnly
			scheduleID = task.(*p.CancelExecutionTask).InitiatedID

		case p.TransferTaskTypeSignalExecution:
			targetDomainID = task.(*p.SignalExecutionTask).TargetDomainID
			targetWorkflowID = task.(*p.SignalExecutionTask).TargetWorkflowID
			targetRunID = task.(*p.SignalExecutionTask).TargetRunID
			if targetRunID == "" {
				targetRunID = p.TransferTaskTransferTargetRunID
			}
			targetChildWorkflowOnly = task.(*p.SignalExecutionTask).TargetChildWorkflowOnly
			scheduleID = task.(*p.SignalExecutionTask).InitiatedID

		case p.TransferTaskTypeStartChildExecution:
			targetDomainID = task.(*p.StartChildExecutionTask).TargetDomainID
			targetWorkflowID = task.(*p.StartChildExecutionTask).TargetWorkflowID
			scheduleID = task.(*p.StartChildExecutionTask).InitiatedID

		case p.TransferTaskTypeCloseExecution:
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
			recordVisibility,
			task.GetVersion(),
			defaultVisibilityTimestamp,
			task.GetTaskID())
	}
}

func (d *cassandraPersistence) createReplicationTasks(batch *gocql.Batch, replicationTasks []p.Task, domainID, workflowID,
	runID string) {

	for _, task := range replicationTasks {
		// Replication task specific information
		firstEventID := common.EmptyEventID
		nextEventID := common.EmptyEventID
		version := common.EmptyVersion
		var lastReplicationInfo map[string]map[string]interface{}
		activityScheduleID := common.EmptyEventID
		var eventStoreVersion, newRunEventStoreVersion int32
		var branchToken, newRunBranchToken []byte
		resetWorkflow := false
		newRunFirstEventID := common.EmptyEventID
		newRunNextEventID := common.EmptyEventID

		switch task.GetType() {
		case p.ReplicationTaskTypeHistory:
			histTask := task.(*p.HistoryReplicationTask)
			eventStoreVersion = histTask.EventStoreVersion
			newRunEventStoreVersion = histTask.NewRunEventStoreVersion
			branchToken = histTask.BranchToken
			newRunBranchToken = histTask.NewRunBranchToken
			firstEventID = histTask.FirstEventID
			nextEventID = histTask.NextEventID
			version = task.GetVersion()
			lastReplicationInfo = make(map[string]map[string]interface{})
			for k, v := range histTask.LastReplicationInfo {
				lastReplicationInfo[k] = createReplicationInfoMap(v)
			}
			newRunFirstEventID = histTask.NewRunFirstEventID
			newRunNextEventID = histTask.NewRunNextEventID
			resetWorkflow = histTask.ResetWorkflow
		case p.ReplicationTaskTypeSyncActivity:
			version = task.GetVersion()
			activityScheduleID = task.(*p.SyncActivityTask).ScheduledID
			// cassandra does not like null
			lastReplicationInfo = make(map[string]map[string]interface{})
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
			activityScheduleID,
			eventStoreVersion,
			branchToken,
			resetWorkflow,
			newRunFirstEventID,
			newRunNextEventID,
			newRunEventStoreVersion,
			newRunBranchToken,
			defaultVisibilityTimestamp,
			task.GetTaskID())
	}
}

func (d *cassandraPersistence) createTimerTasks(batch *gocql.Batch, timerTasks []p.Task, deleteTimerTask p.Task,
	domainID, workflowID, runID string, cqlNowTimestamp int64) {

	for _, task := range timerTasks {
		var eventID int64
		var attempt int64

		timeoutType := 0

		switch t := task.(type) {
		case *p.DecisionTimeoutTask:
			eventID = t.EventID
			timeoutType = t.TimeoutType
			attempt = t.ScheduleAttempt
		case *p.ActivityTimeoutTask:
			eventID = t.EventID
			timeoutType = t.TimeoutType
			attempt = t.Attempt
		case *p.UserTimerTask:
			eventID = t.EventID
		case *p.ActivityRetryTimerTask:
			eventID = t.EventID
			attempt = int64(t.Attempt)
		case *p.WorkflowBackoffTimerTask:
			eventID = t.EventID
			timeoutType = t.TimeoutType
		}

		// Ignoring possible type cast errors.
		ts := p.UnixNanoToDBTimestamp(task.GetVisibilityTimestamp().UnixNano())

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
		// Ignoring possible type cast errors.
		ts := p.UnixNanoToDBTimestamp(deleteTimerTask.GetVisibilityTimestamp().UnixNano())

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

func (d *cassandraPersistence) updateActivityInfos(batch *gocql.Batch, activityInfos []*p.InternalActivityInfo, deleteInfos []int64,
	domainID, workflowID, runID string, condition int64, rangeID int64) error {

	for _, a := range activityInfos {
		startedEventData, _ := p.FromDataBlob(a.StartedEvent)
		if a.StartedEvent != nil && a.StartedEvent.Encoding != a.ScheduledEvent.Encoding {
			return p.NewHistorySerializationError(fmt.Sprintf("expect to have the same encoding, but %v != %v", a.ScheduledEvent.Encoding, a.StartedEvent.Encoding))
		}

		batch.Query(templateUpdateActivityInfoQuery,
			a.ScheduleID,
			a.Version,
			a.ScheduleID,
			a.ScheduledEvent.Data,
			a.ScheduledTime,
			a.StartedID,
			startedEventData,
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
			a.ScheduledEvent.Encoding,
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
	return nil
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

func (d *cassandraPersistence) resetActivityInfos(batch *gocql.Batch, activityInfos []*p.InternalActivityInfo, domainID,
	workflowID, runID string, useCondition bool, condition int64) error {

	infoMap, err := resetActivityInfoMap(activityInfos)
	if err != nil {
		return err
	}

	batchQueryHelper(batch, templateResetActivityInfoQuery, useCondition, condition,
		infoMap,
		d.shardID,
		rowTypeExecution,
		domainID,
		workflowID,
		runID,
		defaultVisibilityTimestamp,
		rowTypeExecutionTaskID)
	return nil
}

func (d *cassandraPersistence) updateTimerInfos(batch *gocql.Batch, timerInfos []*p.TimerInfo, deleteInfos []string,
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

func (d *cassandraPersistence) resetTimerInfos(batch *gocql.Batch, timerInfos []*p.TimerInfo, domainID, workflowID,
	runID string, useCondition bool, condition int64) {
	batchQueryHelper(batch, templateResetTimerInfoQuery, useCondition, condition,
		resetTimerInfoMap(timerInfos),
		d.shardID,
		rowTypeExecution,
		domainID,
		workflowID,
		runID,
		defaultVisibilityTimestamp,
		rowTypeExecutionTaskID)
}

func (d *cassandraPersistence) updateChildExecutionInfos(batch *gocql.Batch, childExecutionInfos []*p.InternalChildExecutionInfo,
	deleteInfo *int64, domainID, workflowID, runID string, condition int64, rangeID int64) error {

	for _, c := range childExecutionInfos {
		encoding := c.InitiatedEvent.GetEncoding()
		var startedEventData []byte
		if c.StartedEvent != nil {
			startedEventData = c.StartedEvent.Data
			if c.StartedEvent.GetEncoding() != encoding {
				return p.NewHistorySerializationError(fmt.Sprintf("expect to have the same encoding, but %v != %v", encoding, c.StartedEvent.GetEncoding()))
			}
		}
		batch.Query(templateUpdateChildExecutionInfoQuery,
			c.InitiatedID,
			c.Version,
			c.InitiatedID,
			c.InitiatedEvent.Data,
			c.StartedID,
			startedEventData,
			c.CreateRequestID,
			encoding,
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
	return nil
}

func (d *cassandraPersistence) resetChildExecutionInfos(batch *gocql.Batch, childExecutionInfos []*p.InternalChildExecutionInfo,
	domainID, workflowID, runID string, useCondition bool, condition int64) error {
	infoMap, err := resetChildExecutionInfoMap(childExecutionInfos, d.logger)
	if err != nil {
		return err
	}
	batchQueryHelper(batch, templateResetChildExecutionInfoQuery, useCondition, condition,
		infoMap,
		d.shardID,
		rowTypeExecution,
		domainID,
		workflowID,
		runID,
		defaultVisibilityTimestamp,
		rowTypeExecutionTaskID)
	return nil
}

func (d *cassandraPersistence) updateRequestCancelInfos(batch *gocql.Batch, requestCancelInfos []*p.RequestCancelInfo,
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

func (d *cassandraPersistence) resetRequestCancelInfos(batch *gocql.Batch, requestCancelInfos []*p.RequestCancelInfo,
	domainID, workflowID, runID string, useCondition bool, condition int64) {
	batchQueryHelper(batch, templateResetRequestCancelInfoQuery, useCondition, condition,
		resetRequestCancelInfoMap(requestCancelInfos),
		d.shardID,
		rowTypeExecution,
		domainID,
		workflowID,
		runID,
		defaultVisibilityTimestamp,
		rowTypeExecutionTaskID)
}

func (d *cassandraPersistence) updateSignalInfos(batch *gocql.Batch, signalInfos []*p.SignalInfo,
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

func (d *cassandraPersistence) resetSignalInfos(batch *gocql.Batch, signalInfos []*p.SignalInfo,
	domainID, workflowID, runID string, useCondition bool, condition int64) {
	batchQueryHelper(batch, templateResetSignalInfoQuery, useCondition, condition,
		resetSignalInfoMap(signalInfos),
		d.shardID,
		rowTypeExecution,
		domainID,
		workflowID,
		runID,
		defaultVisibilityTimestamp,
		rowTypeExecutionTaskID)
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
	domainID, workflowID, runID string, useCondition bool, condition int64) {
	batchQueryHelper(batch, templateResetSignalRequestedQuery, useCondition, condition,
		signalRequested,
		d.shardID,
		rowTypeExecution,
		domainID,
		workflowID,
		runID,
		defaultVisibilityTimestamp,
		rowTypeExecutionTaskID)
}

func (d *cassandraPersistence) updateBufferedEvents(batch *gocql.Batch, newBufferedEvents *p.DataBlob,
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
		values["encoding_type"] = newBufferedEvents.Encoding
		values["version"] = int64(0)
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

func (d *cassandraPersistence) updateBufferedReplicationTasks(batch *gocql.Batch, newBufferedReplicationTask *p.InternalBufferedReplicationTask,
	deleteInfo *int64, domainID, workflowID, runID string, condition int64, rangeID int64) {

	if newBufferedReplicationTask != nil {
		if newBufferedReplicationTask.NewRunHistory != nil {
			batch.Query(templateUpdateBufferedReplicationTasksQuery,
				newBufferedReplicationTask.FirstEventID,
				newBufferedReplicationTask.FirstEventID,
				newBufferedReplicationTask.NextEventID,
				newBufferedReplicationTask.Version,
				newBufferedReplicationTask.EventStoreVersion,
				newBufferedReplicationTask.NewRunEventStoreVersion,
				newBufferedReplicationTask.History.Encoding,
				int64(0),
				newBufferedReplicationTask.History.Data,
				newBufferedReplicationTask.NewRunHistory.Encoding,
				int64(0),
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
				newBufferedReplicationTask.EventStoreVersion,
				newBufferedReplicationTask.History.Encoding,
				int64(0),
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

func createShardInfo(currentCluster string, result map[string]interface{}) *p.ShardInfo {
	info := &p.ShardInfo{}
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

func createWorkflowExecutionInfo(result map[string]interface{}) *p.InternalWorkflowExecutionInfo {
	info := &p.InternalWorkflowExecutionInfo{}
	var completionEventData []byte
	var completionEventEncoding common.EncodingType
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
			completionEventData = v.([]byte)
		case "completion_event_data_encoding":
			completionEventEncoding = common.EncodingType(v.(string))
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
		case "signal_count":
			info.SignalCount = int32(v.(int))
		case "history_size":
			info.HistorySize = v.(int64)
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
		case "event_store_version":
			info.EventStoreVersion = int32(v.(int))
		case "branch_token":
			info.BranchToken = v.([]byte)
		case "cron_schedule":
			info.CronSchedule = v.(string)
		case "expiration_seconds":
			info.ExpirationSeconds = int32(v.(int))
		}
	}
	info.CompletionEvent = p.NewDataBlob(completionEventData, completionEventEncoding)
	return info
}

func createReplicationState(result map[string]interface{}) *p.ReplicationState {
	if len(result) == 0 {
		return nil
	}

	info := &p.ReplicationState{}
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
			info.LastReplicationInfo = make(map[string]*p.ReplicationInfo)
			replicationInfoMap := v.(map[string]map[string]interface{})
			for key, value := range replicationInfoMap {
				info.LastReplicationInfo[key] = createReplicationInfo(value)
			}
		}
	}

	return info
}

func createTransferTaskInfo(result map[string]interface{}) *p.TransferTaskInfo {
	info := &p.TransferTaskInfo{}
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
			if info.TargetRunID == p.TransferTaskTransferTargetRunID {
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
		case "record_visibility":
			info.RecordVisibility = v.(bool)
		case "version":
			info.Version = v.(int64)
		}
	}

	return info
}

func createReplicationTaskInfo(result map[string]interface{}) *p.ReplicationTaskInfo {
	info := &p.ReplicationTaskInfo{}
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
			info.LastReplicationInfo = make(map[string]*p.ReplicationInfo)
			replicationInfoMap := v.(map[string]map[string]interface{})
			for key, value := range replicationInfoMap {
				info.LastReplicationInfo[key] = createReplicationInfo(value)
			}
		case "scheduled_id":
			info.ScheduledID = v.(int64)
		case "event_store_version":
			info.EventStoreVersion = int32(v.(int))
		case "branch_token":
			info.BranchToken = v.([]byte)
		case "reset_workflow":
			info.ResetWorkflow = v.(bool)
		case "new_run_first_event_id":
			info.NewRunFirstEventID = v.(int64)
		case "new_run_next_event_id":
			info.NewRunNextEventID = v.(int64)
		case "new_run_event_store_version":
			info.NewRunEventStoreVersion = int32(v.(int))
		case "new_run_branch_token":
			info.NewRunBranchToken = v.([]byte)
		}
	}

	return info
}

func createActivityInfo(result map[string]interface{}) *p.InternalActivityInfo {
	info := &p.InternalActivityInfo{}
	var sharedEncoding common.EncodingType
	var scheduledEventData, startedEventData []byte
	for k, v := range result {
		switch k {
		case "version":
			info.Version = v.(int64)
		case "schedule_id":
			info.ScheduleID = v.(int64)
		case "scheduled_event":
			scheduledEventData = v.([]byte)
		case "scheduled_time":
			info.ScheduledTime = v.(time.Time)
		case "started_id":
			info.StartedID = v.(int64)
		case "started_event":
			startedEventData = v.([]byte)
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
		case "event_data_encoding":
			sharedEncoding = common.EncodingType(v.(string))
		}
	}
	info.ScheduledEvent = p.NewDataBlob(scheduledEventData, sharedEncoding)
	info.StartedEvent = p.NewDataBlob(startedEventData, sharedEncoding)

	return info
}

func createTimerInfo(result map[string]interface{}) *p.TimerInfo {
	info := &p.TimerInfo{}
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

func createChildExecutionInfo(result map[string]interface{}, logger bark.Logger) *p.InternalChildExecutionInfo {
	info := &p.InternalChildExecutionInfo{}
	var startedData []byte
	for k, v := range result {
		switch k {
		case "version":
			info.Version = v.(int64)
		case "initiated_id":
			info.InitiatedID = v.(int64)
		case "initiated_event":
			info.InitiatedEvent.Data = v.([]byte)
		case "started_id":
			info.StartedID = v.(int64)
		case "started_event":
			startedData = v.([]byte)
		case "create_request_id":
			info.CreateRequestID = v.(gocql.UUID).String()
		case "event_data_encoding":
			info.InitiatedEvent.Encoding = common.EncodingType(v.(string))
		}
	}
	info.StartedEvent = p.NewDataBlob(startedData, info.InitiatedEvent.Encoding)
	return info
}

func createRequestCancelInfo(result map[string]interface{}) *p.RequestCancelInfo {
	info := &p.RequestCancelInfo{}
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

func createSignalInfo(result map[string]interface{}) *p.SignalInfo {
	info := &p.SignalInfo{}
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

func createBufferedReplicationTaskInfo(result map[string]interface{}) *p.InternalBufferedReplicationTask {
	info := &p.InternalBufferedReplicationTask{
		History:       &p.DataBlob{},
		NewRunHistory: &p.DataBlob{},
	}
	for k, v := range result {
		switch k {
		case "first_event_id":
			info.FirstEventID = v.(int64)
		case "next_event_id":
			info.NextEventID = v.(int64)
		case "version":
			info.Version = v.(int64)
		case "event_store_version":
			info.EventStoreVersion = int32(v.(int))
		case "new_run_event_store_version":
			info.NewRunEventStoreVersion = int32(v.(int))
		case "history":
			h := v.(map[string]interface{})
			info.History = createHistoryEventBatchBlob(h)
		case "new_run_history":
			h := v.(map[string]interface{})
			info.NewRunHistory = createHistoryEventBatchBlob(h)
		}
	}

	return info
}

func resetActivityInfoMap(activityInfos []*p.InternalActivityInfo) (map[int64]map[string]interface{}, error) {

	aMap := make(map[int64]map[string]interface{})
	for _, a := range activityInfos {
		if a.StartedEvent != nil && a.ScheduledEvent.Encoding != a.StartedEvent.Encoding {
			return nil, p.NewHistorySerializationError(fmt.Sprintf("expect to have the same encoding, but %v != %v", a.ScheduledEvent.Encoding, a.StartedEvent.Encoding))
		}
		startedEventData, _ := p.FromDataBlob(a.StartedEvent)
		aInfo := make(map[string]interface{})
		aInfo["version"] = a.Version
		aInfo["schedule_id"] = a.ScheduleID
		aInfo["scheduled_event"] = a.ScheduledEvent.Data
		aInfo["scheduled_time"] = a.ScheduledTime
		aInfo["started_id"] = a.StartedID
		aInfo["started_event"] = startedEventData
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
		aInfo["event_data_encoding"] = a.ScheduledEvent.Encoding

		aMap[a.ScheduleID] = aInfo
	}

	return aMap, nil
}

func resetTimerInfoMap(timerInfos []*p.TimerInfo) map[string]map[string]interface{} {
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

func resetChildExecutionInfoMap(childExecutionInfos []*p.InternalChildExecutionInfo, logger bark.Logger) (map[int64]map[string]interface{}, error) {
	cMap := make(map[int64]map[string]interface{})
	for _, c := range childExecutionInfos {
		cInfo := make(map[string]interface{})
		startedEvent := c.StartedEvent
		if startedEvent != nil {
			if startedEvent.Encoding != c.InitiatedEvent.Encoding {
				return nil, p.NewHistorySerializationError(fmt.Sprintf("expect to have the same encoding, but %v != %v", c.InitiatedEvent.Encoding, startedEvent.Encoding))
			}
			cInfo["started_event"] = startedEvent.Data
		} else {
			cInfo["started_event"] = []byte{}
		}
		cInfo["event_data_encoding"] = c.InitiatedEvent.Encoding
		cInfo["version"] = c.Version
		cInfo["initiated_id"] = c.InitiatedID
		cInfo["initiated_event"] = c.InitiatedEvent.Data
		cInfo["create_request_id"] = c.CreateRequestID
		cInfo["started_id"] = c.StartedID

		cMap[c.InitiatedID] = cInfo
	}

	return cMap, nil
}

func resetRequestCancelInfoMap(requestCancelInfos []*p.RequestCancelInfo) map[int64]map[string]interface{} {
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

func resetSignalInfoMap(signalInfos []*p.SignalInfo) map[int64]map[string]interface{} {
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

func createHistoryEventBatchBlob(result map[string]interface{}) *p.DataBlob {
	eventBatch := &p.DataBlob{Encoding: common.EncodingTypeJSON}
	for k, v := range result {
		switch k {
		case "encoding_type":
			eventBatch.Encoding = common.EncodingType(v.(string))
		case "data":
			eventBatch.Data = v.([]byte)
		}
	}

	return eventBatch
}

func createTaskInfo(result map[string]interface{}) *p.TaskInfo {
	info := &p.TaskInfo{}
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

func createTimerTaskInfo(result map[string]interface{}) *p.TimerTaskInfo {
	info := &p.TimerTaskInfo{}
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

func createReplicationInfo(result map[string]interface{}) *p.ReplicationInfo {
	info := &p.ReplicationInfo{}
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

func createReplicationInfoMap(info *p.ReplicationInfo) map[string]interface{} {
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
