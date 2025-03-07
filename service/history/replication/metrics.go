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

package replication

import (
	enumsspb "go.temporal.io/server/api/enums/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/service/history/tasks"
)

func TaskOperationTag(
	replicationTask *replicationspb.ReplicationTask,
) string {
	if replicationTask == nil {
		return "__unknown__"
	}
	switch replicationTask.GetTaskType() {
	case enumsspb.REPLICATION_TASK_TYPE_SYNC_SHARD_STATUS_TASK: // TODO to be deprecated
		return "__unknown__"
	case enumsspb.REPLICATION_TASK_TYPE_HISTORY_METADATA_TASK: // TODO to be deprecated
		return "__unknown__"
	case enumsspb.REPLICATION_TASK_TYPE_SYNC_ACTIVITY_TASK:
		return metrics.SyncActivityTaskScope
	case enumsspb.REPLICATION_TASK_TYPE_SYNC_WORKFLOW_STATE_TASK:
		return metrics.SyncWorkflowStateTaskScope
	case enumsspb.REPLICATION_TASK_TYPE_HISTORY_V2_TASK:
		return metrics.HistoryReplicationTaskScope
	case enumsspb.REPLICATION_TASK_TYPE_SYNC_HSM_TASK:
		return metrics.SyncHSMTaskScope
	case enumsspb.REPLICATION_TASK_TYPE_SYNC_VERSIONED_TRANSITION_TASK:
		return metrics.SyncVersionedTransitionTaskScope
	case enumsspb.REPLICATION_TASK_TYPE_BACKFILL_HISTORY_TASK:
		return metrics.BackfillHistoryEventsTaskScope
	case enumsspb.REPLICATION_TASK_TYPE_VERIFY_VERSIONED_TRANSITION_TASK:
		return metrics.VerifyVersionedTransitionTaskScope
	default:
		return metrics.NoopTaskScope
	}
}

func TaskOperationTagFromTask(
	item tasks.Task,
) string {
	switch item.GetType() {
	case enumsspb.TASK_TYPE_REPLICATION_SYNC_HSM:
		return metrics.SyncHSMTaskScope
	case enumsspb.TASK_TYPE_REPLICATION_SYNC_VERSIONED_TRANSITION:
		return metrics.SyncVersionedTransitionTaskScope
	case enumsspb.TASK_TYPE_REPLICATION_SYNC_ACTIVITY:
		return metrics.SyncActivityTaskScope
	case enumsspb.TASK_TYPE_REPLICATION_SYNC_WORKFLOW_STATE:
		return metrics.SyncWorkflowStateTaskScope
	case enumsspb.TASK_TYPE_REPLICATION_HISTORY:
		return metrics.HistoryReplicationTaskScope
	default:
		return "__unknown__"
	}
}
