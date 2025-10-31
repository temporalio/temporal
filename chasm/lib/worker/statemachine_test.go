package worker

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/chasm"
	workerpb "go.temporal.io/server/chasm/lib/worker/gen/workerpb/v1"
)

func TestRecordHeartbeat(t *testing.T) {
	worker := NewWorker()
	ctx := &chasm.MockMutableContext{}
	leaseDeadline := time.Now().Add(30 * time.Second)

	// Test successful heartbeat recording
	err := RecordHeartbeat(ctx, worker, leaseDeadline)
	require.NoError(t, err)

	// Verify lease deadline was set
	require.NotNil(t, worker.LeaseExpirationTime)
	require.Equal(t, leaseDeadline.Unix(), worker.LeaseExpirationTime.AsTime().Unix())

	// Verify worker is still active
	require.Equal(t, workerpb.WORKER_STATUS_ACTIVE, worker.Status)

	// Verify a task was scheduled with correct deadline
	require.Len(t, ctx.Tasks, 1)
	require.Equal(t, leaseDeadline, ctx.Tasks[0].Attributes.ScheduledTime)
}

func TestUpdateWorkerLease(t *testing.T) {
	worker := NewWorker()
	ctx := &chasm.MockMutableContext{}
	leaseDeadline := time.Now().Add(30 * time.Second)

	updateWorkerLease(ctx, worker, leaseDeadline)

	// Verify lease deadline was set
	require.NotNil(t, worker.LeaseExpirationTime)
	require.Equal(t, leaseDeadline.Unix(), worker.LeaseExpirationTime.AsTime().Unix())

	// Verify lease expiry task was scheduled
	require.Len(t, ctx.Tasks, 1)
	require.Equal(t, leaseDeadline, ctx.Tasks[0].Attributes.ScheduledTime)
}

func TestTransitionActiveHeartbeat(t *testing.T) {
	worker := NewWorker()
	ctx := &chasm.MockMutableContext{}

	heartbeatTime := time.Now()
	leaseDeadline := heartbeatTime.Add(30 * time.Second)

	event := EventHeartbeatReceived{
		Time:          heartbeatTime,
		LeaseDeadline: leaseDeadline,
	}

	// Apply the transition
	err := TransitionActiveHeartbeat.Apply(ctx, worker, event)
	require.NoError(t, err)

	// Verify state was updated
	require.NotNil(t, worker.LeaseExpirationTime)
	require.Equal(t, leaseDeadline.Unix(), worker.LeaseExpirationTime.AsTime().Unix())
	require.Equal(t, workerpb.WORKER_STATUS_ACTIVE, worker.Status)

	// Verify task was scheduled
	require.Len(t, ctx.Tasks, 1)
	require.Equal(t, leaseDeadline, ctx.Tasks[0].Attributes.ScheduledTime)
}

func TestTransitionLeaseExpired(t *testing.T) {
	worker := NewWorker()
	worker.Status = workerpb.WORKER_STATUS_ACTIVE
	ctx := &chasm.MockMutableContext{}

	expiryTime := time.Now()
	event := EventLeaseExpired{
		Time: expiryTime,
	}

	// Apply the transition
	err := TransitionLeaseExpired.Apply(ctx, worker, event)
	require.NoError(t, err)

	// Verify status changed to inactive
	require.Equal(t, workerpb.WORKER_STATUS_INACTIVE, worker.Status)

	// Verify cleanup task was scheduled
	require.Len(t, ctx.Tasks, 1)

	// Verify cleanup task is scheduled for the right time
	expectedCleanupTime := expiryTime.Add(inactiveWorkerCleanupDelay)
	require.Equal(t, expectedCleanupTime, ctx.Tasks[0].Attributes.ScheduledTime)

	// Verify it's a WorkerCleanupTask
	_, ok := ctx.Tasks[0].Payload.(*workerpb.WorkerCleanupTask)
	require.True(t, ok)
}

func TestTransitionCleanupCompleted(t *testing.T) {
	worker := NewWorker()
	worker.Status = workerpb.WORKER_STATUS_INACTIVE
	ctx := &chasm.MockMutableContext{}

	cleanupTime := time.Now()
	event := EventCleanupCompleted{
		Time: cleanupTime,
	}

	// Apply the transition
	err := TransitionCleanupCompleted.Apply(ctx, worker, event)
	require.NoError(t, err)

	// Verify status changed to cleaned up
	require.Equal(t, workerpb.WORKER_STATUS_CLEANED_UP, worker.Status)

	// Verify no additional tasks were scheduled
	require.Empty(t, ctx.Tasks)
}

func TestScheduleLeaseExpiry(t *testing.T) {
	worker := NewWorker()
	ctx := &chasm.MockMutableContext{}
	leaseDeadline := time.Now().Add(1 * time.Minute)

	// Schedule lease expiry
	scheduleLeaseExpiry(ctx, worker, leaseDeadline)

	// Verify task was scheduled
	require.Len(t, ctx.Tasks, 1)

	// Verify task details
	_, ok := ctx.Tasks[0].Payload.(*workerpb.LeaseExpiryTask)
	require.True(t, ok)
	require.Equal(t, leaseDeadline, ctx.Tasks[0].Attributes.ScheduledTime)
	require.Empty(t, ctx.Tasks[0].Attributes.Destination) // Local execution
}

func TestMultipleHeartbeats(t *testing.T) {
	worker := NewWorker()
	ctx := &chasm.MockMutableContext{}

	// First heartbeat
	firstDeadline := time.Now().Add(30 * time.Second)
	err := RecordHeartbeat(ctx, worker, firstDeadline)
	require.NoError(t, err)
	require.Equal(t, firstDeadline.Unix(), worker.LeaseExpirationTime.AsTime().Unix())

	// Second heartbeat extends the lease
	secondDeadline := time.Now().Add(60 * time.Second)
	err = RecordHeartbeat(ctx, worker, secondDeadline)
	require.NoError(t, err)
	require.Equal(t, secondDeadline.Unix(), worker.LeaseExpirationTime.AsTime().Unix())

	// Verify two tasks were scheduled (one for each heartbeat)
	require.Len(t, ctx.Tasks, 2)
}

func TestWorkerResurrection(t *testing.T) {
	ctx := &chasm.MockMutableContext{}

	t.Run("ResurrectionFromInactive", func(t *testing.T) {
		worker := NewWorker()
		worker.Status = workerpb.WORKER_STATUS_INACTIVE

		leaseDeadline := time.Now().Add(30 * time.Second)
		err := RecordHeartbeat(ctx, worker, leaseDeadline)

		// Should succeed - worker resurrection handles same identity reconnection
		require.NoError(t, err)
		require.Equal(t, workerpb.WORKER_STATUS_ACTIVE, worker.Status)
		require.NotNil(t, worker.LeaseExpirationTime)
		require.Equal(t, leaseDeadline.Unix(), worker.LeaseExpirationTime.AsTime().Unix())

		// Verify new lease expiry task was scheduled
		require.Len(t, ctx.Tasks, 1)
		require.Equal(t, leaseDeadline, ctx.Tasks[0].Attributes.ScheduledTime)
	})
}

func TestTransitionWorkerResurrection(t *testing.T) {
	worker := NewWorker()
	worker.Status = workerpb.WORKER_STATUS_INACTIVE
	ctx := &chasm.MockMutableContext{}

	heartbeatTime := time.Now()
	leaseDeadline := heartbeatTime.Add(30 * time.Second)

	event := EventHeartbeatReceived{
		Time:          heartbeatTime,
		LeaseDeadline: leaseDeadline,
	}

	// Apply the resurrection transition directly
	err := TransitionWorkerResurrection.Apply(ctx, worker, event)
	require.NoError(t, err)

	// Verify state changed to active
	require.Equal(t, workerpb.WORKER_STATUS_ACTIVE, worker.Status)

	// Verify lease was updated
	require.NotNil(t, worker.LeaseExpirationTime)
	require.Equal(t, leaseDeadline.Unix(), worker.LeaseExpirationTime.AsTime().Unix())

	// Verify task was scheduled
	require.Len(t, ctx.Tasks, 1)
	require.Equal(t, leaseDeadline, ctx.Tasks[0].Attributes.ScheduledTime)
}

func TestInvalidTransitions(t *testing.T) {
	ctx := &chasm.MockMutableContext{}

	t.Run("HeartbeatOnCleanedUpWorker", func(t *testing.T) {
		worker := NewWorker()
		worker.Status = workerpb.WORKER_STATUS_CLEANED_UP

		leaseDeadline := time.Now().Add(30 * time.Second)
		err := RecordHeartbeat(ctx, worker, leaseDeadline)

		// Should fail because worker is cleaned up (terminal state)
		require.Error(t, err)
		require.Contains(t, err.Error(), "cannot record heartbeat for worker in state CleanedUp")
	})

	t.Run("LeaseExpiryOnCleanedUpWorker", func(t *testing.T) {
		worker := NewWorker()
		worker.Status = workerpb.WORKER_STATUS_CLEANED_UP

		event := EventLeaseExpired{Time: time.Now()}
		err := TransitionLeaseExpired.Apply(ctx, worker, event)

		// Should fail because worker is not active
		require.Error(t, err)
	})

	t.Run("CleanupOnActiveWorker", func(t *testing.T) {
		worker := NewWorker()
		worker.Status = workerpb.WORKER_STATUS_ACTIVE

		event := EventCleanupCompleted{Time: time.Now()}
		err := TransitionCleanupCompleted.Apply(ctx, worker, event)

		// Should fail because worker is not inactive
		require.Error(t, err)
	})
}
