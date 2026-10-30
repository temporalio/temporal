package tasktoken

import (
	"strconv"
	"strings"
)

// WorkflowWorkerTaskID returns the correlation ID shared by a workflow task's producer, poll, and response traces.
func WorkflowWorkerTaskID(namespaceID string, runID string, scheduledEventID int64) string {
	return workerTaskID("workflow", namespaceID, runID, strconv.FormatInt(scheduledEventID, 10))
}

// ActivityWorkerTaskID returns the correlation ID shared by an activity task's producer, poll, and response traces.
func ActivityWorkerTaskID(namespaceID string, runID string, scheduledEventID int64) string {
	return workerTaskID("activity", namespaceID, runID, strconv.FormatInt(scheduledEventID, 10))
}

// QueryWorkerTaskID returns the correlation ID shared by a query task's producer, poll, and response traces.
func QueryWorkerTaskID(namespaceID string, taskID string) string {
	return workerTaskID("query", namespaceID, taskID)
}

// NexusWorkerTaskID returns the correlation ID shared by a Nexus task's producer, poll, and response traces.
func NexusWorkerTaskID(namespaceID string, taskID string) string {
	return workerTaskID("nexus", namespaceID, taskID)
}

func workerTaskID(taskType string, parts ...string) string {
	return taskType + "/" + strings.Join(parts, "/")
}
