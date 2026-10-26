package persistence

import (
	enumspb "go.temporal.io/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
)

// IsTaskQueueMetadataRetained reports whether deleting metadata would discard a single-session assignment.
func IsTaskQueueMetadataRetained(info *persistencespb.TaskQueueInfo) bool {
	return info.GetKind() == enumspb.TASK_QUEUE_KIND_STICKY &&
		info.GetTaskType() == enumspb.TASK_QUEUE_TYPE_WORKFLOW &&
		info.GetSingleSessionAssigned()
}
