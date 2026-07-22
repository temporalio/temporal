// State transition logic for Worker.
package worker

import (
	"time"

	"go.temporal.io/server/chasm"
	workerstatepb "go.temporal.io/server/chasm/lib/worker/gen/workerpb/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// updateLease updates the worker's lease and schedules expiry.
func (w *Worker) updateLease(ctx chasm.MutableContext, leaseDeadline time.Time) {
	w.LeaseExpirationTime = timestamppb.New(leaseDeadline)
	w.scheduleLeaseExpiry(ctx, leaseDeadline)
}

// scheduleLeaseExpiry schedules a timer task that will fire when the lease expires.
func (w *Worker) scheduleLeaseExpiry(ctx chasm.MutableContext, leaseDeadline time.Time) {
	expiryTask := &workerstatepb.LeaseExpiryTask{}

	taskAttrs := chasm.TaskAttributes{
		ScheduledTime: leaseDeadline,
	}

	ctx.AddTask(w, taskAttrs, expiryTask)
}

// EventHeartbeatReceived is triggered when a heartbeat is received from the worker.
type EventHeartbeatReceived struct {
	LeaseDeadline time.Time
}

// TransitionActiveHeartbeat handles heartbeat reception for active workers, extending the lease.
var TransitionActiveHeartbeat = chasm.NewTransition(
	[]workerstatepb.WorkerStatus{workerstatepb.WORKER_STATUS_ACTIVE},
	workerstatepb.WORKER_STATUS_ACTIVE,
	func(w *Worker, ctx chasm.MutableContext, event EventHeartbeatReceived) error {
		w.updateLease(ctx, event.LeaseDeadline)
		return nil
	},
)

// EventLeaseExpired is triggered when the worker lease expires.
type EventLeaseExpired struct{}

// TransitionLeaseExpired handles lease expiry, marking worker as inactive (terminal).
// INACTIVE is the terminal state - the CHASM framework will delete the entity.
var TransitionLeaseExpired = chasm.NewTransition(
	[]workerstatepb.WorkerStatus{workerstatepb.WORKER_STATUS_ACTIVE},
	workerstatepb.WORKER_STATUS_INACTIVE,
	func(w *Worker, ctx chasm.MutableContext, event EventLeaseExpired) error {
		// TODO: Reschedule activities bound to this worker.
		return nil
	},
)
