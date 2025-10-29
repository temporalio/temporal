package workersession

import (
	"time"

	"go.temporal.io/server/chasm"
	workersessionpb "go.temporal.io/server/chasm/lib/workersession/gen/workersessionpb/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	// Time to wait before cleaning up an expired session.
	expiredSessionCleanupDelay = 10 * time.Minute
)

// RecordHeartbeat processes a heartbeat by applying the heartbeat received transition.
func RecordHeartbeat(ctx chasm.MutableContext, ws *WorkerSession, leaseDeadline time.Time) error {
	return TransitionHeartbeatReceived.Apply(ctx, ws, EventHeartbeatReceived{
		Time:          time.Now(),
		LeaseDeadline: leaseDeadline,
	})
}

// scheduleLeaseExpiry schedules a timer task that will fire when the lease expires.
func scheduleLeaseExpiry(ctx chasm.MutableContext, ws *WorkerSession, leaseDeadline time.Time) {
	expiryTask := &workersessionpb.LeaseExpiryTask{
		LeaseDeadline: timestamppb.New(leaseDeadline),
	}

	taskAttrs := chasm.TaskAttributes{
		ScheduledTime: leaseDeadline,
		Destination:   "", // Empty means local execution.
	}

	ctx.AddTask(ws, taskAttrs, expiryTask)
}

// EventHeartbeatReceived is triggered when a heartbeat is received from the worker.
type EventHeartbeatReceived struct {
	Time          time.Time
	LeaseDeadline time.Time
}

// TransitionHeartbeatReceived handles heartbeat reception, extending the lease and scheduling a new timeout.
var TransitionHeartbeatReceived = chasm.NewTransition(
	[]workersessionpb.WorkerSessionStatus{workersessionpb.WORKER_SESSION_STATUS_ACTIVE},
	workersessionpb.WORKER_SESSION_STATUS_ACTIVE,
	func(ctx chasm.MutableContext, ws *WorkerSession, event EventHeartbeatReceived) error {
		// Store client-provided lease deadline.
		ws.LeaseDeadline = timestamppb.New(event.LeaseDeadline)

		// Schedule new lease expiry timer.
		scheduleLeaseExpiry(ctx, ws, event.LeaseDeadline)
		return nil
	},
)

// EventLeaseExpired is triggered when the worker session lease expires.
type EventLeaseExpired struct {
	Time time.Time
}

// TransitionLeaseExpired handles lease expiry, marking session as expired and scheduling cleanup.
var TransitionLeaseExpired = chasm.NewTransition(
	[]workersessionpb.WorkerSessionStatus{workersessionpb.WORKER_SESSION_STATUS_ACTIVE},
	workersessionpb.WORKER_SESSION_STATUS_EXPIRED,
	func(ctx chasm.MutableContext, ws *WorkerSession, event EventLeaseExpired) error {
		// Schedule cleanup task.
		cleanupTask := &workersessionpb.SessionCleanupTask{}
		taskAttrs := chasm.TaskAttributes{
			ScheduledTime: event.Time.Add(expiredSessionCleanupDelay),
		}
		ctx.AddTask(ws, taskAttrs, cleanupTask)
		return nil
	},
)

// EventCleanupCompleted is triggered when cleanup is finished.
type EventCleanupCompleted struct {
	Time time.Time
}

// TransitionCleanupCompleted handles cleanup completion, marking session as terminated.
var TransitionCleanupCompleted = chasm.NewTransition(
	[]workersessionpb.WorkerSessionStatus{workersessionpb.WORKER_SESSION_STATUS_EXPIRED},
	workersessionpb.WORKER_SESSION_STATUS_TERMINATED,
	func(ctx chasm.MutableContext, ws *WorkerSession, event EventCleanupCompleted) error {
		return nil
	},
)
