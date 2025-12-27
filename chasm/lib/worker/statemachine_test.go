package worker

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	workerpb "go.temporal.io/api/worker/v1"
	"go.temporal.io/server/chasm"
	workerstatepb "go.temporal.io/server/chasm/lib/worker/gen/workerpb/v1"
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

	// Test successful heartbeat recording (first heartbeat, no token)
	newToken, err := worker.recordHeartbeat(ctx, worker.WorkerHeartbeat, nil, leaseDuration)
	require.NoError(t, err)
	require.NotEmpty(t, newToken)

	// Verify lease deadline was set (approximately)
	require.NotNil(t, worker.LeaseExpirationTime)
	expectedDeadline := time.Now().Add(leaseDuration)
	actualDeadline := worker.LeaseExpirationTime.AsTime()
	require.WithinDuration(t, expectedDeadline, actualDeadline, time.Second)

	// Verify worker is still active
	require.Equal(t, workerstatepb.WORKER_STATUS_ACTIVE, worker.Status)

	// Verify a task was scheduled
	require.Len(t, ctx.Tasks, 1)

	// Verify conflict token was incremented
	require.Equal(t, int64(1), worker.ConflictToken)
}

func TestRecordHeartbeat_TokenValidation(t *testing.T) {
	worker := newTestWorker()
	ctx := &chasm.MockMutableContext{}
	leaseDuration := 30 * time.Second

	// First heartbeat - no token required
	token1, err := worker.recordHeartbeat(ctx, worker.WorkerHeartbeat, nil, leaseDuration)
	require.NoError(t, err)
	require.NotEmpty(t, token1)

	// Second heartbeat with correct token
	token2, err := worker.recordHeartbeat(ctx, worker.WorkerHeartbeat, token1, leaseDuration)
	require.NoError(t, err)
	require.NotEmpty(t, token2)

	// Third heartbeat with wrong token
	wrongToken := []byte("wrong-token")
	_, err = worker.recordHeartbeat(ctx, worker.WorkerHeartbeat, wrongToken, leaseDuration)
	require.Error(t, err)

	// Verify it's a TokenMismatchError with correct token
	tokenErr, ok := err.(*TokenMismatchError)
	require.True(t, ok, "expected TokenMismatchError")
	require.Equal(t, token2, tokenErr.CurrentToken)
}

func TestRecordHeartbeat_InactiveWorker(t *testing.T) {
	worker := newTestWorker()
	worker.Status = workerstatepb.WORKER_STATUS_INACTIVE
	ctx := &chasm.MockMutableContext{}
	leaseDuration := 30 * time.Second

	// Heartbeat on inactive worker should fail
	_, err := worker.recordHeartbeat(ctx, worker.WorkerHeartbeat, nil, leaseDuration)
	require.Error(t, err)
	_, ok := err.(*WorkerInactiveError)
	require.True(t, ok, "expected WorkerInactiveError")
}

func TestUpdateLease(t *testing.T) {
	worker := newTestWorker()
	ctx := &chasm.MockMutableContext{}
	leaseDeadline := time.Now().Add(30 * time.Second)

	worker.updateLease(ctx, leaseDeadline)

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

	event := EventLeaseExpired{}

	// Apply the transition
	err := TransitionLeaseExpired.Apply(ctx, worker, event)
	require.NoError(t, err)

	// Verify status changed to inactive (terminal)
	require.Equal(t, workerstatepb.WORKER_STATUS_INACTIVE, worker.Status)

	// Verify no additional tasks were scheduled (no cleanup task needed)
	require.Empty(t, ctx.Tasks)
}

func TestScheduleLeaseExpiry(t *testing.T) {
	worker := newTestWorker()
	ctx := &chasm.MockMutableContext{}
	leaseDeadline := time.Now().Add(1 * time.Minute)

	// Schedule lease expiry
	worker.scheduleLeaseExpiry(ctx, leaseDeadline)

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
	token1, err := worker.recordHeartbeat(ctx, worker.WorkerHeartbeat, nil, firstDuration)
	require.NoError(t, err)

	// Second heartbeat extends the lease (with correct token)
	secondDuration := 60 * time.Second
	_, err = worker.recordHeartbeat(ctx, worker.WorkerHeartbeat, token1, secondDuration)
	require.NoError(t, err)

	// Verify two tasks were scheduled (one for each heartbeat)
	require.Len(t, ctx.Tasks, 2)
}

func TestInvalidTransitions(t *testing.T) {
	ctx := &chasm.MockMutableContext{}

	t.Run("HeartbeatOnInactiveWorker", func(t *testing.T) {
		worker := newTestWorker()
		worker.Status = workerstatepb.WORKER_STATUS_INACTIVE

		leaseDuration := 30 * time.Second
		_, err := worker.recordHeartbeat(ctx, worker.WorkerHeartbeat, nil, leaseDuration)

		// Should fail because worker is inactive (terminal state)
		require.Error(t, err)
		_, ok := err.(*WorkerInactiveError)
		require.True(t, ok, "expected WorkerInactiveError")
	})

	t.Run("LeaseExpiryOnInactiveWorker", func(t *testing.T) {
		worker := newTestWorker()
		worker.Status = workerstatepb.WORKER_STATUS_INACTIVE

		event := EventLeaseExpired{}
		err := TransitionLeaseExpired.Apply(ctx, worker, event)

		// Should fail because worker is not active
		require.Error(t, err)
	})
}
