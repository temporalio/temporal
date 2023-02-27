package replication

import (
	"fmt"
	"time"

	"go.uber.org/fx"

	enumsspb "go.temporal.io/server/api/enums/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/xdc"
	"go.temporal.io/server/service/history/shard"
)

type (
	ProcessToolBox struct {
		fx.In

		ClusterMetadata    cluster.Metadata
		ClientBean         client.Bean
		ShardController    shard.Controller
		NamespaceCache     namespace.Registry
		NDCHistoryResender xdc.NDCHistoryResender
		MetricsHandler     metrics.Handler
		Logger             log.Logger
	}
)

func (i *ProcessToolBox) ConvertTasks(
	sourceClusterName string,
	replicationTasks ...*replicationspb.ReplicationTask,
) []TrackableExecutableTask {
	tasks := make([]TrackableExecutableTask, len(replicationTasks))
	for _, replicationTask := range replicationTasks {
		tasks = append(tasks, i.convertOne(sourceClusterName, replicationTask))
	}
	return tasks
}

func (i *ProcessToolBox) convertOne(
	sourceClusterName string,
	replicationTask *replicationspb.ReplicationTask,
) TrackableExecutableTask {
	var taskCreationTime time.Time
	if replicationTask.VisibilityTime != nil {
		taskCreationTime = *replicationTask.VisibilityTime
	} else {
		taskCreationTime = time.Now().UTC()
	}

	switch replicationTask.GetTaskType() {
	case enumsspb.REPLICATION_TASK_TYPE_SYNC_SHARD_STATUS_TASK: // TODO to be deprecated
		return NewExecutableNoopTask(
			*i,
			replicationTask.SourceTaskId,
			taskCreationTime,
		)
	case enumsspb.REPLICATION_TASK_TYPE_HISTORY_METADATA_TASK: // TODO to be deprecated
		return NewExecutableNoopTask(
			*i,
			replicationTask.SourceTaskId,
			taskCreationTime,
		)
	case enumsspb.REPLICATION_TASK_TYPE_SYNC_ACTIVITY_TASK:
		return NewExecutableActivityTask(
			*i,
			replicationTask.SourceTaskId,
			taskCreationTime,
			replicationTask.GetSyncActivityTaskAttributes(),
			sourceClusterName,
		)
	case enumsspb.REPLICATION_TASK_TYPE_SYNC_WORKFLOW_STATE_TASK:
		return NewExecutableWorkflowTask(
			*i,
			replicationTask.SourceTaskId,
			taskCreationTime,
			replicationTask.GetSyncWorkflowStateTaskAttributes(),
			sourceClusterName,
		)
	case enumsspb.REPLICATION_TASK_TYPE_HISTORY_V2_TASK:
		return NewExecutableHistoryTask(
			*i,
			replicationTask.SourceTaskId,
			taskCreationTime,
			replicationTask.GetHistoryTaskAttributes(),
			sourceClusterName,
		)
	default:
		i.Logger.Error(fmt.Sprintf("unknown replication task: %v", replicationTask))
		return NewExecutableUnknownTask(
			*i,
			replicationTask.SourceTaskId,
			taskCreationTime,
			replicationTask,
		)
	}
}
