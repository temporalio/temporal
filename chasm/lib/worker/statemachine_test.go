package worker

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	workerpb "go.temporal.io/api/worker/v1"
	"go.temporal.io/server/chasm"
	workerstatepb "go.temporal.io/server/chasm/lib/worker/gen/workerpb/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// newTestWorker creates a worker for testing with a default heartbeat
func newTestWorker() *Worker {
	worker := NewWorker()
	// Initialize with heartbeat data for testing
	worker.WorkerHeartbeat = &workerpb.WorkerHeartbeat{
		WorkerInstanceKey: "test-worker",
	}
	return worker
}

func TestRecordHeartbeat(t *testing.T) {
	worker := newTestWorker()
	ctx := &chasm.MockMutableContext{}
	leaseDuration := 30 * time.Second

	// Test successful heartbeat recording
	err := worker.recordHeartbeat(ctx, worker.WorkerHeartbeat, leaseDuration)
	require.NoError(t, err)

	// Verify lease deadline was set (approximately)
	require.NotNil(t, worker.LeaseExpirationTime)
	expectedDeadline := time.Now().Add(leaseDuration)
	actualDeadline := worker.LeaseExpirationTime.AsTime()
	require.WithinDuration(t, expectedDeadline, actualDeadline, time.Second)

	// Verify worker is still active
	require.Equal(t, workerstatepb.WORKER_STATUS_ACTIVE, worker.Status)

	// Verify a task was scheduled
	require.Len(t, ctx.Tasks, 1)
}

func TestUpdateWorkerLease(t *testing.T) {
	worker := newTestWorker()
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
	worker := newTestWorker()
	ctx := &chasm.MockMutableContext{}

	leaseDeadline := time.Now().Add(30 * time.Second)

	event := EventHeartbeatReceived{
		LeaseDeadline: leaseDeadline,
	}

	// Apply the transition
	err := TransitionActiveHeartbeat.Apply(ctx, worker, event)
	require.NoError(t, err)

	// Verify state was updated
	require.NotNil(t, worker.LeaseExpirationTime)
	require.Equal(t, leaseDeadline.Unix(), worker.LeaseExpirationTime.AsTime().Unix())
	require.Equal(t, workerstatepb.WORKER_STATUS_ACTIVE, worker.Status)

	// Verify task was scheduled
	require.Len(t, ctx.Tasks, 1)
	require.Equal(t, leaseDeadline, ctx.Tasks[0].Attributes.ScheduledTime)
}

func TestTransitionLeaseExpired(t *testing.T) {
	worker := newTestWorker()
	worker.Status = workerstatepb.WORKER_STATUS_ACTIVE
	ctx := &chasm.MockMutableContext{}

	event := EventLeaseExpired{
		CleanupDelay: 60 * time.Second,
	}

	// Apply the transition
	err := TransitionLeaseExpired.Apply(ctx, worker, event)
	require.NoError(t, err)

	// Verify status changed to inactive
	require.Equal(t, workerstatepb.WORKER_STATUS_INACTIVE, worker.Status)

	// Verify cleanup task was scheduled
	require.Len(t, ctx.Tasks, 1)

	// Verify cleanup task is scheduled for the right time (approximately)
	require.WithinDuration(t, time.Now().Add(event.CleanupDelay), ctx.Tasks[0].Attributes.ScheduledTime, time.Second)

	// Verify it's a WorkerCleanupTask
	_, ok := ctx.Tasks[0].Payload.(*workerstatepb.WorkerCleanupTask)
	require.True(t, ok)
}

func TestTransitionCleanupCompleted(t *testing.T) {
	worker := newTestWorker()
	worker.Status = workerstatepb.WORKER_STATUS_INACTIVE
	ctx := &chasm.MockMutableContext{}

	event := EventCleanupCompleted{}

	// Apply the transition
	err := TransitionCleanupCompleted.Apply(ctx, worker, event)
	require.NoError(t, err)

	// Verify status changed to cleaned up
	require.Equal(t, workerstatepb.WORKER_STATUS_CLEANED_UP, worker.Status)

	// Verify no additional tasks were scheduled
	require.Empty(t, ctx.Tasks)
}

func TestScheduleLeaseExpiry(t *testing.T) {
	worker := newTestWorker()
	ctx := &chasm.MockMutableContext{}
	leaseDeadline := time.Now().Add(1 * time.Minute)

	// Schedule lease expiry
	scheduleLeaseExpiry(ctx, worker, leaseDeadline)

	// Verify task was scheduled
	require.Len(t, ctx.Tasks, 1)

	// Verify task details
	_, ok := ctx.Tasks[0].Payload.(*workerstatepb.LeaseExpiryTask)
	require.True(t, ok)
	require.Equal(t, leaseDeadline, ctx.Tasks[0].Attributes.ScheduledTime)
	require.Empty(t, ctx.Tasks[0].Attributes.Destination) // Local execution
}

func TestMultipleHeartbeats(t *testing.T) {
	worker := newTestWorker()
	ctx := &chasm.MockMutableContext{}

	// First heartbeat
	firstDuration := 30 * time.Second
	err := worker.recordHeartbeat(ctx, worker.WorkerHeartbeat, firstDuration)
	require.NoError(t, err)

	// Second heartbeat extends the lease
	secondDuration := 60 * time.Second
	err = worker.recordHeartbeat(ctx, worker.WorkerHeartbeat, secondDuration)
	require.NoError(t, err)

	// Verify two tasks were scheduled (one for each heartbeat)
	require.Len(t, ctx.Tasks, 2)
}

func TestWorkerResurrection(t *testing.T) {
	ctx := &chasm.MockMutableContext{}

	t.Run("ResurrectionFromInactive", func(t *testing.T) {
		worker := newTestWorker()
		worker.Status = workerstatepb.WORKER_STATUS_INACTIVE

		// Set cleanup time to simulate previous inactive period
		oldCleanupTime := time.Now().Add(60 * time.Minute)
		worker.CleanupTime = timestamppb.New(oldCleanupTime)

		leaseDuration := 30 * time.Second
		err := worker.recordHeartbeat(ctx, worker.WorkerHeartbeat, leaseDuration)

		// Should succeed - worker resurrection handles same identity reconnection
		require.NoError(t, err)
		require.Equal(t, workerstatepb.WORKER_STATUS_ACTIVE, worker.Status)
		require.NotNil(t, worker.LeaseExpirationTime)

		// Verify cleanup time was cleared during resurrection
		require.Nil(t, worker.CleanupTime, "CleanupTime should be cleared during resurrection")

		// Verify new lease expiry task was scheduled
		require.Len(t, ctx.Tasks, 1)
	})
}

func TestTransitionResurrected(t *testing.T) {
	worker := newTestWorker()
	worker.Status = workerstatepb.WORKER_STATUS_INACTIVE
	ctx := &chasm.MockMutableContext{}

	// Set cleanup time to simulate previous inactive period
	oldCleanupTime := time.Now().Add(60 * time.Minute)
	worker.CleanupTime = timestamppb.New(oldCleanupTime)

	leaseDeadline := time.Now().Add(30 * time.Second)

	event := EventHeartbeatReceived{
		LeaseDeadline: leaseDeadline,
	}

	// Apply the resurrection transition directly
	err := TransitionResurrected.Apply(ctx, worker, event)
	require.NoError(t, err)

	// Verify state changed to active
	require.Equal(t, workerstatepb.WORKER_STATUS_ACTIVE, worker.Status)

	// Verify lease was updated
	require.NotNil(t, worker.LeaseExpirationTime)
	require.Equal(t, leaseDeadline.Unix(), worker.LeaseExpirationTime.AsTime().Unix())

	// Verify cleanup time was cleared during resurrection
	require.Nil(t, worker.CleanupTime, "CleanupTime should be cleared during resurrection")

	// Verify task was scheduled
	require.Len(t, ctx.Tasks, 1)
	require.Equal(t, leaseDeadline, ctx.Tasks[0].Attributes.ScheduledTime)
}

func TestInvalidTransitions(t *testing.T) {
	ctx := &chasm.MockMutableContext{}

	t.Run("HeartbeatOnCleanedUpWorker", func(t *testing.T) {
		worker := newTestWorker()
		worker.Status = workerstatepb.WORKER_STATUS_CLEANED_UP

		leaseDuration := 30 * time.Second
		err := worker.recordHeartbeat(ctx, worker.WorkerHeartbeat, leaseDuration)

		// Should fail because worker is cleaned up (terminal state)
		require.Error(t, err)
		require.Contains(t, err.Error(), "cannot record heartbeat for worker in state CleanedUp")
	})

	t.Run("LeaseExpiryOnCleanedUpWorker", func(t *testing.T) {
		worker := newTestWorker()
		worker.Status = workerstatepb.WORKER_STATUS_CLEANED_UP

		event := EventLeaseExpired{}
		err := TransitionLeaseExpired.Apply(ctx, worker, event)

		// Should fail because worker is not active
		require.Error(t, err)
	})

	t.Run("CleanupOnActiveWorker", func(t *testing.T) {
		worker := newTestWorker()
		worker.Status = workerstatepb.WORKER_STATUS_ACTIVE

		event := EventCleanupCompleted{}
		err := TransitionCleanupCompleted.Apply(ctx, worker, event)

		// Should fail because worker is not inactive
		require.Error(t, err)
	})
}

func TestCleanupTaskValidation(t *testing.T) {
	t.Run("CleanupTaskInvalidatedAfterResurrection", func(t *testing.T) {
		worker := newTestWorker()
		executor := NewWorkerCleanupTaskExecutor(nil)

		// 1. Worker becomes inactive and cleanup task is scheduled for future
		cleanupTaskTime := time.Now().Add(60 * time.Minute)

		worker.Status = workerstatepb.WORKER_STATUS_INACTIVE
		worker.CleanupTime = timestamppb.New(cleanupTaskTime)

		// 2. Cleanup task with matching scheduled time
		attrs := chasm.TaskAttributes{ScheduledTime: cleanupTaskTime}

		// Task should be valid initially
		ctx := &chasm.MockMutableContext{}
		valid := executor.isCleanupTaskValid(ctx, worker, attrs)
		require.True(t, valid, "Cleanup task should be valid for inactive worker")

		// 3. Worker resurrects - cleanup_time gets cleared
		worker.Status = workerstatepb.WORKER_STATUS_ACTIVE
		worker.CleanupTime = nil

		// 4. Old cleanup task should now be invalid
		valid = executor.isCleanupTaskValid(ctx, worker, attrs)
		require.False(t, valid, "Cleanup task should be invalid after worker resurrection")
	})

	t.Run("CleanupTaskInvalidatedAfterNewCleanupTaskScheduled", func(t *testing.T) {
		worker := newTestWorker()
		executor := NewWorkerCleanupTaskExecutor(nil)

		// 1. Worker is inactive with one cleanup time
		cleanupTaskTime := time.Now().Add(60 * time.Minute)

		worker.Status = workerstatepb.WORKER_STATUS_INACTIVE
		worker.CleanupTime = timestamppb.New(cleanupTaskTime)

		// 2. Simulate a cleanup task with earlier scheduled time. This task should be invalid.
		earlierCleanupTaskTime := cleanupTaskTime.Add(-10 * time.Minute)
		attrs := chasm.TaskAttributes{ScheduledTime: earlierCleanupTaskTime}

		ctx := &chasm.MockMutableContext{}
		valid := executor.isCleanupTaskValid(ctx, worker, attrs)
		require.False(t, valid, "Cleanup task should be invalid with later scheduled time")
	})
}
