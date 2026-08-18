package history

import (
	"errors"
	"time"

	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/wideevents"
	"go.temporal.io/server/service/history/consts"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/tasks"
)

func emitStandbyTaskError(
	shardContext historyi.ShardContext,
	executable queues.Executable,
	taskType string,
	err error,
) {
	if err == nil || !shardContext.GetConfig().EmitReplicationLifecycleEvents() {
		return
	}
	eventLogger := shardContext.GetEventLogger()
	if eventLogger == nil {
		return
	}

	task := executable.GetTask()
	namespaceName := ""
	activeCluster := ""
	if entry, nsErr := shardContext.GetNamespaceRegistry().GetNamespaceByID(namespace.ID(task.GetNamespaceID())); nsErr == nil {
		namespaceName = entry.Name().String()
		activeCluster = entry.ActiveClusterName(namespace.RoutingKey{ID: task.GetWorkflowID()})
	}

	details := map[string]any{
		"active_cluster":  activeCluster,
		"attempt":         executable.Attempt(),
		"category":        task.GetCategory().Name(),
		"local_task_id":   task.GetTaskID(),
		"local_task_type": task.GetType().String(),
		"target_cluster":  shardContext.GetClusterMetadata().GetCurrentClusterName(),
		"visibility_time": task.GetVisibilityTime().Format(time.RFC3339Nano),
	}
	if errors.Is(err, consts.ErrTaskRetry) {
		details["disposition"] = wideevents.ReplDispositionRetry
	} else if errors.Is(err, consts.ErrTaskDiscarded) {
		details["disposition"] = wideevents.ReplDispositionDiscarded
	}
	if versionedTask, ok := task.(tasks.HasVersion); ok {
		details["version"] = versionedTask.GetVersion()
	}
	if destinationTask, ok := task.(tasks.HasDestination); ok {
		details["destination"] = destinationTask.GetDestination()
	}

	wideevents.EmitReplicationError(eventLogger, wideevents.ReplicationLifecyclePayload{
		TaskType:    taskType,
		Shard:       shardContext.GetShardID(),
		Namespace:   namespaceName,
		NamespaceID: task.GetNamespaceID(),
		WorkflowID:  task.GetWorkflowID(),
		RunID:       task.GetRunID(),
	}, wideevents.ReplOperationStandbyTaskExecution, "Standby queue task execution failed", err, details)
}
