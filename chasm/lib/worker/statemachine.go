// State transition logic for Worker.
package worker

import (
	"fmt"
	"time"

	"go.temporal.io/server/chasm"
	workerstatepb "go.temporal.io/server/chasm/lib/worker/gen/workerpb/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// updateWorkerLease is a shared helper that updates the worker's lease and schedules expiry.
func updateWorkerLease(ctx chasm.MutableContext, w *Worker, leaseDeadline time.Time) {
	w.LeaseExpirationTime = timestamppb.New(leaseDeadline)
	scheduleLeaseExpiry(ctx, w, leaseDeadline)
}

// RecordHeartbeat processes a heartbeat by applying the appropriate transition based on worker state.
func RecordHeartbeat(ctx chasm.MutableContext, w *Worker, leaseDeadline time.Time) error {
	switch w.Status {
	case workerstatepb.WORKER_STATUS_ACTIVE:
		return TransitionActiveHeartbeat.Apply(ctx, w, EventHeartbeatReceived{
			Time:          time.Now(),
			LeaseDeadline: leaseDeadline,
		})
	case workerstatepb.WORKER_STATUS_INACTIVE:
		// Handle worker resurrection after network partition
		return TransitionWorkerResurrection.Apply(ctx, w, EventHeartbeatReceived{
			Time:          time.Now(),
			LeaseDeadline: leaseDeadline,
		})
	default:
		// CLEANED_UP or other states - not allowed
		return fmt.Errorf("cannot record heartbeat for worker in state %v", w.Status)
	}
}

// scheduleLeaseExpiry schedules a timer task that will fire when the lease expires.
func scheduleLeaseExpiry(ctx chasm.MutableContext, w *Worker, leaseDeadline time.Time) {
	expiryTask := &workerstatepb.LeaseExpiryTask{}

	taskAttrs := chasm.TaskAttributes{
		ScheduledTime: leaseDeadline,
		Destination:   "", // Empty means local execution.
	}

	ctx.AddTask(w, taskAttrs, expiryTask)
}

// EventHeartbeatReceived is triggered when a heartbeat is received from the worker.
type EventHeartbeatReceived struct {
	Time          time.Time
	LeaseDeadline time.Time
}

// TransitionActiveHeartbeat handles heartbeat reception for active workers, extending the lease.
var TransitionActiveHeartbeat = chasm.NewTransition(
	[]workerstatepb.WorkerStatus{workerstatepb.WORKER_STATUS_ACTIVE},
	workerstatepb.WORKER_STATUS_ACTIVE,
	func(w *Worker, ctx chasm.MutableContext, event EventHeartbeatReceived) error {
		updateWorkerLease(ctx, w, event.LeaseDeadline)
		return nil
	},
)

// EventLeaseExpired is triggered when the worker lease expires.
type EventLeaseExpired struct {
	Time         time.Time
	CleanupDelay time.Duration
}

// TransitionLeaseExpired handles lease expiry, marking worker as inactive and scheduling cleanup.
var TransitionLeaseExpired = chasm.NewTransition(
	[]workerstatepb.WorkerStatus{workerstatepb.WORKER_STATUS_ACTIVE},
	workerstatepb.WORKER_STATUS_INACTIVE,
	func(w *Worker, ctx chasm.MutableContext, event EventLeaseExpired) error {
		// Schedule cleanup task with provided delay.
		cleanupTask := &workerstatepb.WorkerCleanupTask{}
		taskAttrs := chasm.TaskAttributes{
			ScheduledTime: event.Time.Add(event.CleanupDelay),
		}
		ctx.AddTask(w, taskAttrs, cleanupTask)
		return nil
	},
)

// EventCleanupCompleted is triggered when cleanup is finished.
type EventCleanupCompleted struct {
	Time time.Time
}

// TransitionCleanupCompleted handles cleanup completion, marking worker as cleaned up.
var TransitionCleanupCompleted = chasm.NewTransition(
	[]workerstatepb.WorkerStatus{workerstatepb.WORKER_STATUS_INACTIVE},
	workerstatepb.WORKER_STATUS_CLEANED_UP,
	func(w *Worker, ctx chasm.MutableContext, event EventCleanupCompleted) error {
		return nil
	},
)

// TransitionWorkerResurrection handles worker reconnection when in INACTIVE state.
// This is a special case for when the same worker process reconnects after network partition.
// Note: Any activities associated with this worker may have already been rescheduled.
var TransitionWorkerResurrection = chasm.NewTransition(
	[]workerstatepb.WorkerStatus{workerstatepb.WORKER_STATUS_INACTIVE},
	workerstatepb.WORKER_STATUS_ACTIVE,
	func(w *Worker, ctx chasm.MutableContext, event EventHeartbeatReceived) error {
		updateWorkerLease(ctx, w, event.LeaseDeadline)
		return nil
	},
)
