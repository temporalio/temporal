package metrics

import (
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/tqid"
)

// GetPerActivityScope returns the standard tags for metrics associated with an activity.
func GetPerActivityScope(
	handler Handler,
	namespaceName string,
	taskQueueFamily *tqid.TaskQueueFamily,
	taskQueueBreakdown bool,
	operation string,
	activityType string,
	workflowType string,
	versioningBehavior enumspb.VersioningBehavior,
) Handler {
	return GetPerTaskQueueFamilyScope(
		handler,
		namespaceName,
		taskQueueFamily,
		taskQueueBreakdown,
		OperationTag(operation),
		ActivityTypeTag(activityType),
		VersioningBehaviorTag(versioningBehavior),
		WorkflowTypeTag(workflowType),
	)
}
