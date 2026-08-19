package matching

import (
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/tasktoken"
	"go.temporal.io/server/common/telemetry"
)

func annotateWorkerTask(span trace.Span, workerTaskID string) {
	span.SetAttributes(attribute.String(telemetry.WorkerTaskIDKey, workerTaskID))
}

func workflowWorkerTaskID(task *persistencespb.TaskInfo) string {
	return tasktoken.WorkflowWorkerTaskID(
		task.GetNamespaceId(),
		task.GetRunId(),
		task.GetScheduledEventId(),
	)
}

func activityWorkerTaskID(task *persistencespb.TaskInfo) string {
	return tasktoken.ActivityWorkerTaskID(
		task.GetNamespaceId(),
		task.GetRunId(),
		task.GetScheduledEventId(),
	)
}

func queryWorkerTaskID(namespaceID string, taskID string) string {
	return tasktoken.QueryWorkerTaskID(namespaceID, taskID)
}

func nexusWorkerTaskID(namespaceID string, taskID string) string {
	return tasktoken.NexusWorkerTaskID(namespaceID, taskID)
}
