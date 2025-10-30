package worker

import (
	"time"

	"go.temporal.io/server/chasm"
	workerpb "go.temporal.io/server/chasm/lib/worker/gen/workerpb/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	// Time to wait before cleaning up an inactive worker.
	inactiveWorkerCleanupDelay = 60 * time.Minute
)

// RecordHeartbeat processes a heartbeat by applying the heartbeat received transition.
func RecordHeartbeat(ctx chasm.MutableContext, w *Worker, leaseDeadline time.Time) error {
	return TransitionHeartbeatReceived.Apply(ctx, w, EventHeartbeatReceived{
		Time:          time.Now(),
		LeaseDeadline: leaseDeadline,
	})
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

// TransitionHeartbeatReceived handles heartbeat reception, extending the lease and scheduling a new timeout.
var TransitionHeartbeatReceived = chasm.NewTransition(
	[]workerpb.WorkerStatus{workerpb.WORKER_STATUS_ACTIVE},
	workerpb.WORKER_STATUS_ACTIVE,
	func(ctx chasm.MutableContext, w *Worker, event EventHeartbeatReceived) error {
		// Store client-provided lease deadline.
		w.LeaseExpirationTime = timestamppb.New(event.LeaseDeadline)

		// Schedule new lease expiry timer.
		scheduleLeaseExpiry(ctx, w, event.LeaseDeadline)
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
