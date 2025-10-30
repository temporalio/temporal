package worker

import (
	"fmt"
	"time"

	"go.temporal.io/server/chasm"
	workerpb "go.temporal.io/server/chasm/lib/worker/gen/workerpb/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	// Time to wait before cleaning up an inactive worker.
	inactiveWorkerCleanupDelay = 60 * time.Minute
)

// updateWorkerLease is a shared helper that updates the worker's lease and schedules expiry.
func updateWorkerLease(ctx chasm.MutableContext, w *Worker, leaseDeadline time.Time) {
	w.LeaseExpirationTime = timestamppb.New(leaseDeadline)
	scheduleLeaseExpiry(ctx, w, leaseDeadline)
}

// RecordHeartbeat processes a heartbeat by applying the appropriate transition based on worker state.
func RecordHeartbeat(ctx chasm.MutableContext, w *Worker, leaseDeadline time.Time) error {
	switch w.Status {
	case workerpb.WORKER_STATUS_ACTIVE:
		return TransitionActiveHeartbeat.Apply(ctx, w, EventHeartbeatReceived{
			Time:          time.Now(),
			LeaseDeadline: leaseDeadline,
		})
	case workerpb.WORKER_STATUS_INACTIVE:
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
	expiryTask := &workerpb.LeaseExpiryTask{}

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
	[]workerpb.WorkerStatus{workerpb.WORKER_STATUS_ACTIVE},
	workerpb.WORKER_STATUS_ACTIVE,
	func(ctx chasm.MutableContext, w *Worker, event EventHeartbeatReceived) error {
		updateWorkerLease(ctx, w, event.LeaseDeadline)
		return nil
	},
)

// EventLeaseExpired is triggered when the worker lease expires.
type EventLeaseExpired struct {
	Time time.Time
}

// TransitionLeaseExpired handles lease expiry, marking worker as inactive and scheduling cleanup.
var TransitionLeaseExpired = chasm.NewTransition(
	[]workerpb.WorkerStatus{workerpb.WORKER_STATUS_ACTIVE},
	workerpb.WORKER_STATUS_INACTIVE,
	func(ctx chasm.MutableContext, w *Worker, event EventLeaseExpired) error {
		// Schedule cleanup task.
		cleanupTask := &workerpb.WorkerCleanupTask{}
		taskAttrs := chasm.TaskAttributes{
			ScheduledTime: event.Time.Add(inactiveWorkerCleanupDelay),
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
	[]workerpb.WorkerStatus{workerpb.WORKER_STATUS_INACTIVE},
	workerpb.WORKER_STATUS_CLEANED_UP,
	func(ctx chasm.MutableContext, w *Worker, event EventCleanupCompleted) error {
		return nil
	},
)

// TransitionWorkerResurrection handles worker reconnection when in INACTIVE state.
// This is a special case for when the same worker process reconnects after network partition.
// Note: Any activities associated with this worker have already been cancelled and rescheduled.
var TransitionWorkerResurrection = chasm.NewTransition(
	[]workerpb.WorkerStatus{workerpb.WORKER_STATUS_INACTIVE},
	workerpb.WORKER_STATUS_ACTIVE,
	func(ctx chasm.MutableContext, w *Worker, event EventHeartbeatReceived) error {
		updateWorkerLease(ctx, w, event.LeaseDeadline)
		return nil
	},
)
