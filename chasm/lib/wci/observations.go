package wci

import (
	"cmp"
	"slices"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	wcipb "go.temporal.io/server/chasm/lib/wci/gen/wcipb/v1"
	"go.temporal.io/server/common"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// maxObservedTaskQueues bounds per-group state; a version can have many task queues.
const maxObservedTaskQueues = 50

// observedTaskQueuesFromDescribe lists the version's task queues, largest backlog first.
func observedTaskQueuesFromDescribe(resp *workflowservice.DescribeWorkerDeploymentVersionResponse) []*wcipb.ObservedTaskQueue {
	out := make([]*wcipb.ObservedTaskQueue, 0, len(resp.GetVersionTaskQueues()))
	for _, tq := range resp.GetVersionTaskQueues() {
		stats := tq.GetStats()
		out = append(out, &wcipb.ObservedTaskQueue{
			Name:         tq.GetName(),
			Type:         tq.GetType(),
			BacklogCount: stats.GetApproximateBacklogCount(),
			BacklogAge:   stats.GetApproximateBacklogAge(),
			AddRate:      stats.GetTasksAddRate(),
			DispatchRate: stats.GetTasksDispatchRate(),
		})
	}
	slices.SortStableFunc(out, func(a, b *wcipb.ObservedTaskQueue) int {
		return cmp.Or(cmp.Compare(b.BacklogCount, a.BacklogCount), cmp.Compare(a.Name, b.Name))
	})
	return out
}

func (g *ScalingGroup) observations() *wcipb.TaskQueueObservations {
	if g.Observations == nil {
		g.Observations = &wcipb.TaskQueueObservations{}
	}
	return g.Observations
}

// observePoll records the group's task queues from a successful poll.
func (g *ScalingGroup) observePoll(now time.Time, observed []*wcipb.ObservedTaskQueue, taskTypes []enumspb.TaskQueueType) {
	queues := make([]*wcipb.ObservedTaskQueue, 0, min(len(observed), maxObservedTaskQueues))
	for _, tq := range observed {
		if len(queues) == maxObservedTaskQueues {
			break
		}
		if slices.Contains(taskTypes, tq.GetType()) {
			queues = append(queues, common.CloneProto(tq))
		}
	}
	o := g.observations()
	o.PolledAt = timestamppb.New(now)
	o.TaskQueues = queues
}

func (g *ScalingGroup) observeTaskAdd(now time.Time, event *wcipb.TaskAddEvent) {
	o := g.observations()
	o.LastTaskAdd = common.CloneProto(event)
	o.LastTaskAddTime = timestamppb.New(now)
}
