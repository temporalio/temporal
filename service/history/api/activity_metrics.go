package api

import (
	enumspb "go.temporal.io/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/tqid"
	historyi "go.temporal.io/server/service/history/interfaces"
)

// ActivityMetricsInfo captures the values needed to emit metrics for an activity after its
// mutable-state transaction commits.
type ActivityMetricsInfo struct {
	namespaceName      string
	taskQueue          string
	activityType       string
	workflowType       string
	versioningBehavior enumspb.VersioningBehavior
}

// NewActivityMetricsInfo captures the standard activity metric tags from mutable state.
func NewActivityMetricsInfo(
	mutableState historyi.MutableState,
	activityInfo *persistencespb.ActivityInfo,
) ActivityMetricsInfo {
	return ActivityMetricsInfo{
		namespaceName:      mutableState.GetNamespaceEntry().Name().String(),
		taskQueue:          activityInfo.GetTaskQueue(),
		activityType:       activityInfo.GetActivityType().GetName(),
		workflowType:       mutableState.GetWorkflowType().GetName(),
		versioningBehavior: mutableState.GetEffectiveVersioningBehavior(),
	}
}

// Handler returns a metrics handler with the standard activity tags.
func (i ActivityMetricsInfo) Handler(
	shardContext historyi.ShardContext,
	operation string,
) metrics.Handler {
	return metrics.GetPerActivityScope(
		shardContext.GetMetricsHandler(),
		i.namespaceName,
		tqid.UnsafeTaskQueueFamily(i.namespaceName, i.taskQueue),
		shardContext.GetConfig().BreakdownMetricsByTaskQueue(
			i.namespaceName,
			i.taskQueue,
			enumspb.TASK_QUEUE_TYPE_ACTIVITY,
		),
		operation,
		i.activityType,
		i.workflowType,
		i.versioningBehavior,
	)
}
