package replication

import (
	"fmt"
	"time"

	enumsspb "go.temporal.io/server/api/enums/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/metrics"
)

type (
	ExecutableTaskConverter interface {
		Convert(
			taskClusterName string,
			clientShardKey ClusterShardKey,
			serverShardKey ClusterShardKey,
			replicationTasks ...*replicationspb.ReplicationTask,
		) []TrackableExecutableTask
	}

	executableTaskConverterImpl struct {
		processToolBox ProcessToolBox
	}
)

func NewExecutableTaskConverter(
	processToolBox ProcessToolBox,
) *executableTaskConverterImpl {
	return &executableTaskConverterImpl{
		processToolBox: processToolBox,
	}
}

func (e *executableTaskConverterImpl) Convert(
	sourceClusterName string,
	clientShardKey ClusterShardKey,
	serverShardKey ClusterShardKey,
	replicationTasks ...*replicationspb.ReplicationTask,
) []TrackableExecutableTask {
	tasks := make([]TrackableExecutableTask, len(replicationTasks))
	for index, replicationTask := range replicationTasks {
		metrics.ReplicationTasksRecv.With(e.processToolBox.MetricsHandler).Record(
			int64(1),
			metrics.FromClusterIDTag(serverShardKey.ClusterID),
			metrics.ToClusterIDTag(clientShardKey.ClusterID),
			metrics.OperationTag(TaskOperationTag(replicationTask)),
		)
		tasks[index] = e.convertOne(sourceClusterName, serverShardKey, replicationTask)
	}
	return tasks
}

func (e *executableTaskConverterImpl) convertOne(
	sourceClusterName string,
	sourceShardKey ClusterShardKey,
	replicationTask *replicationspb.ReplicationTask,
) TrackableExecutableTask {
	var taskCreationTime time.Time
	if replicationTask.VisibilityTime != nil {
		taskCreationTime = replicationTask.VisibilityTime.AsTime()
	} else {
		taskCreationTime = time.Now().UTC()
	}

	switch replicationTask.GetTaskType() {
	case enumsspb.REPLICATION_TASK_TYPE_SYNC_SHARD_STATUS_TASK: // TODO to be deprecated
		return NewExecutableNoopTask(
			e.processToolBox,
			replicationTask.SourceTaskId,
			taskCreationTime,
			sourceClusterName,
			sourceShardKey,
		)
	case enumsspb.REPLICATION_TASK_TYPE_HISTORY_METADATA_TASK: // TODO to be deprecated
		return NewExecutableNoopTask(
			e.processToolBox,
			replicationTask.SourceTaskId,
			taskCreationTime,
			sourceClusterName,
			sourceShardKey,
		)
	case enumsspb.REPLICATION_TASK_TYPE_SYNC_ACTIVITY_TASK:
		return NewExecutableActivityStateTask(
			e.processToolBox,
			replicationTask.SourceTaskId,
			taskCreationTime,
			replicationTask.GetSyncActivityTaskAttributes(),
			sourceClusterName,
			sourceShardKey,
			replicationTask,
		)
	case enumsspb.REPLICATION_TASK_TYPE_SYNC_WORKFLOW_STATE_TASK:
		return NewExecutableWorkflowStateTask(
			e.processToolBox,
			replicationTask.SourceTaskId,
			taskCreationTime,
			replicationTask.GetSyncWorkflowStateTaskAttributes(),
			sourceClusterName,
			sourceShardKey,
			replicationTask,
		)
	case enumsspb.REPLICATION_TASK_TYPE_HISTORY_V2_TASK:
		return NewExecutableHistoryTask(
			e.processToolBox,
			replicationTask.SourceTaskId,
			taskCreationTime,
			replicationTask.GetHistoryTaskAttributes(),
			sourceClusterName,
			sourceShardKey,
			replicationTask,
		)
	case enumsspb.REPLICATION_TASK_TYPE_SYNC_HSM_TASK:
		return NewExecutableSyncHSMTask(
			e.processToolBox,
			replicationTask.SourceTaskId,
			taskCreationTime,
			replicationTask.GetSyncHsmAttributes(),
			sourceClusterName,
			sourceShardKey,
			replicationTask,
		)
	case enumsspb.REPLICATION_TASK_TYPE_BACKFILL_HISTORY_TASK:
		return NewExecutableBackfillHistoryEventsTask(
			e.processToolBox,
			replicationTask.SourceTaskId,
			taskCreationTime,
			sourceClusterName,
			sourceShardKey,
			replicationTask,
		)
	case enumsspb.REPLICATION_TASK_TYPE_VERIFY_VERSIONED_TRANSITION_TASK:
		return NewExecutableVerifyVersionedTransitionTask(
			e.processToolBox,
			replicationTask.SourceTaskId,
			taskCreationTime,
			sourceClusterName,
			sourceShardKey,
			replicationTask,
		)
	case enumsspb.REPLICATION_TASK_TYPE_SYNC_VERSIONED_TRANSITION_TASK:
		return NewExecutableSyncVersionedTransitionTask(
			e.processToolBox,
			replicationTask.SourceTaskId,
			taskCreationTime,
			sourceClusterName,
			sourceShardKey,
			replicationTask,
		)
	default:
		e.processToolBox.Logger.Error(fmt.Sprintf("unknown replication task: %v", replicationTask))
		return NewExecutableUnknownTask(
			e.processToolBox,
			replicationTask.SourceTaskId,
			taskCreationTime,
			replicationTask,
		)
	}
}
