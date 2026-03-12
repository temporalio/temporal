package taskqueue

import (
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"google.golang.org/protobuf/types/known/durationpb"
)

// MergeStats merges from into into. Mutates into.
func MergeStats(into, from *taskqueuepb.TaskQueueStats) {
	if from == nil {
		return
	}
	into.ApproximateBacklogCount += from.ApproximateBacklogCount
	into.ApproximateBacklogAge = oldestBacklogAge(into.ApproximateBacklogAge, from.ApproximateBacklogAge)
	into.TasksAddRate += from.TasksAddRate
	into.TasksDispatchRate += from.TasksDispatchRate
}

// AggregateStats merges all stats from the map into a single TaskQueueStats.
func AggregateStats(stats map[int32]*taskqueuepb.TaskQueueStats) *taskqueuepb.TaskQueueStats {
	result := &taskqueuepb.TaskQueueStats{ApproximateBacklogAge: durationpb.New(0)}
	for _, s := range stats {
		MergeStats(result, s)
	}
	return result
}

// DedupPollers removes duplicate pollers by identity.
func DedupPollers(pollerInfos []*taskqueuepb.PollerInfo) []*taskqueuepb.PollerInfo {
	allKeys := make(map[string]bool)
	var list []*taskqueuepb.PollerInfo
	for _, item := range pollerInfos {
		if _, value := allKeys[item.GetIdentity()]; !value {
			allKeys[item.GetIdentity()] = true
			list = append(list, item)
		}
	}
	return list
}

func oldestBacklogAge(left, right *durationpb.Duration) *durationpb.Duration {
	if left == nil {
		left = durationpb.New(0)
	}
	if right == nil {
		right = durationpb.New(0)
	}
	if left.AsDuration() > right.AsDuration() {
		return left
	}
	return right
}
