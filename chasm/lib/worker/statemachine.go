package worker

import (
	"time"

	workerpb "go.temporal.io/api/worker/v1"
	"go.temporal.io/server/chasm"
	workerstatepb "go.temporal.io/server/chasm/lib/worker/gen/workerpb/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type eventHeartbeatReceived struct {
	heartbeats    []*workerpb.WorkerHeartbeat
	leaseDuration time.Duration
}

// A heartbeat keeps all workers in the group alive under the same lease.
var transitionActiveHeartbeat = chasm.NewTransition(
	[]workerstatepb.WorkerGroupStatus{workerstatepb.WORKER_GROUP_STATUS_ACTIVE},
	workerstatepb.WORKER_GROUP_STATUS_ACTIVE,
	func(g *WorkerGroup, ctx chasm.MutableContext, event eventHeartbeatReceived) error {
		g.updateWorkers(ctx, event.heartbeats)
		deadline := ctx.Now(g).Add(event.leaseDuration)
		g.LeaseExpirationTime = timestamppb.New(deadline)
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
		// TODO: When the lease expires, schedule a task for each worker to
		// reschedule its activities. These tasks can continue after the group closes.
		return nil
	},
)
