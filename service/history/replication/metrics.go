package replication

import (
	enumsspb "go.temporal.io/server/api/enums/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/metrics"
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
	taskType enumsspb.TaskType,
) string {
	switch taskType {
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
		return metrics.UnknownTaskScope
	}
}
