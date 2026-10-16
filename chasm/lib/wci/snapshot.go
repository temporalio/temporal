package wci

import (
	"slices"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/auto-scaled-workers/wci/workflow/iface"
	scalingalgorithm "go.temporal.io/auto-scaled-workers/wci/workflow/scaling_algorithm"
)

// snapshotFromDescribe aggregates per-task-queue stats by type, as the workflow's PullStats does.
func snapshotFromDescribe(resp *workflowservice.DescribeWorkerDeploymentVersionResponse) *scalingalgorithm.ScalingMetricsSnapshot {
	snapshot := &scalingalgorithm.ScalingMetricsSnapshot{
		Workflow: &iface.QueueTypeScalingMetrics{},
		Activity: &iface.QueueTypeScalingMetrics{},
		Nexus:    &iface.QueueTypeScalingMetrics{},
	}
	for _, tq := range resp.GetVersionTaskQueues() {
		stats := tq.GetStats()
		if stats == nil {
			continue
		}
		var m *iface.QueueTypeScalingMetrics
		switch tq.GetType() {
		case enumspb.TASK_QUEUE_TYPE_WORKFLOW:
			m = snapshot.Workflow
		case enumspb.TASK_QUEUE_TYPE_ACTIVITY:
			m = snapshot.Activity
		case enumspb.TASK_QUEUE_TYPE_NEXUS:
			m = snapshot.Nexus
		default:
			continue
		}
		m.LastBacklogCount += stats.GetApproximateBacklogCount()
		m.LastArrivalRate += stats.GetTasksAddRate()
		m.LastProcessingRate += stats.GetTasksDispatchRate()
		m.LastBacklogAge = max(m.LastBacklogAge, stats.GetApproximateBacklogAge().AsDuration())
	}
	return snapshot
}

// filterSnapshot keeps only the group's task types; others are nil, as the algorithms expect.
func filterSnapshot(snapshot *scalingalgorithm.ScalingMetricsSnapshot, taskTypes []enumspb.TaskQueueType) *scalingalgorithm.ScalingMetricsSnapshot {
	filtered := &scalingalgorithm.ScalingMetricsSnapshot{}
	if snapshot == nil {
		return filtered
	}
	pick := func(t enumspb.TaskQueueType, m *iface.QueueTypeScalingMetrics) *iface.QueueTypeScalingMetrics {
		if m == nil || !slices.Contains(taskTypes, t) {
			return nil
		}
		clone := *m
		return &clone
	}
	filtered.Workflow = pick(enumspb.TASK_QUEUE_TYPE_WORKFLOW, snapshot.Workflow)
	filtered.Activity = pick(enumspb.TASK_QUEUE_TYPE_ACTIVITY, snapshot.Activity)
	filtered.Nexus = pick(enumspb.TASK_QUEUE_TYPE_NEXUS, snapshot.Nexus)
	return filtered
}
