package worker

import (
	"time"

	"go.temporal.io/server/chasm"
	workerstatepb "go.temporal.io/server/chasm/lib/worker/gen/workerpb/v1"
)

type eventHeartbeatReceived struct {
	leaseDuration time.Duration
}

// A heartbeat keeps all workers in the group alive under the same lease.
var transitionActiveHeartbeat = chasm.NewTransition(
	[]workerstatepb.WorkerGroupStatus{workerstatepb.WORKER_GROUP_STATUS_ACTIVE},
	workerstatepb.WORKER_GROUP_STATUS_ACTIVE,
	func(g *WorkerGroup, ctx chasm.MutableContext, event eventHeartbeatReceived) error {
		deadline := ctx.Now(g).Add(event.leaseDuration)
		// Replace the previous expiry task so the group closes at the renewed deadline.
		ctx.AddTask(g, chasm.TaskAttributes{ScheduledTime: deadline}, &workerstatepb.LeaseExpiryTask{})
		return nil
	},
)

type eventLeaseExpired struct{}

// Lease expiry ends the current run for the group. A later heartbeat must start a new run.
var transitionLeaseExpired = chasm.NewTransition(
	[]workerstatepb.WorkerGroupStatus{workerstatepb.WORKER_GROUP_STATUS_ACTIVE},
	workerstatepb.WORKER_GROUP_STATUS_INACTIVE,
	func(_ *WorkerGroup, _ chasm.MutableContext, _ eventLeaseExpired) error {
		// TODO: When activity tracking is added, schedule tasks on the group to
		// reschedule activities. These tasks can continue after the group closes, within retention.
		return nil
	},
)
