package worker

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	workerpb "go.temporal.io/api/worker/v1"
	workflowservice "go.temporal.io/api/workflowservice/v1"
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

// newTestRequest creates a test RecordHeartbeatRequest
func newTestRequest(workerInstanceKey string) *workerstatepb.RecordHeartbeatRequest {
	return &workerstatepb.RecordHeartbeatRequest{
		NamespaceId: "test-namespace-id",
		FrontendRequest: &workflowservice.RecordWorkerHeartbeatRequest{
			Namespace: "test-namespace",
			Identity:  "test-identity",
			WorkerHeartbeat: []*workerpb.WorkerHeartbeat{
				{
					WorkerInstanceKey: workerInstanceKey,
				},
			},
		},
	}
}

func TestRecordHeartbeat(t *testing.T) {
	worker := newTestWorker()
	ctx := &chasm.MockMutableContext{}

	req := newTestRequest("test-worker")

	// Test successful heartbeat recording
	resp, err := worker.recordHeartbeat(ctx, req)
	require.NoError(t, err)
	require.NotNil(t, resp)

	// Verify lease deadline was set (approximately, using default 1 minute)
	require.NotNil(t, worker.LeaseExpirationTime)
	expectedDeadline := time.Now().Add(1 * time.Minute) // Default lease duration
	actualDeadline := worker.LeaseExpirationTime.AsTime()
	require.WithinDuration(t, expectedDeadline, actualDeadline, time.Second)

	// Verify worker is still active
	require.Equal(t, workerstatepb.WORKER_STATUS_ACTIVE, worker.Status)

	// Verify a task was scheduled
	require.Len(t, ctx.Tasks, 1)

	// Verify conflict token was incremented
	require.Equal(t, int64(1), worker.ConflictToken)
}

func TestRecordHeartbeat_InactiveWorker(t *testing.T) {
	worker := newTestWorker()
	worker.Status = workerstatepb.WORKER_STATUS_INACTIVE
	ctx := &chasm.MockMutableContext{}

	req := newTestRequest("test-worker")

	// Heartbeat on inactive worker should fail
	_, err := worker.recordHeartbeat(ctx, req)
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
	err := TransitionActiveHeartbeat.Apply(worker, ctx, event)
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
	err := TransitionLeaseExpired.Apply(worker, ctx, event)
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
	resp, err := worker.recordHeartbeat(ctx, newTestRequest("test-worker"))
	require.NoError(t, err)
	require.NotNil(t, resp)

	// Second heartbeat extends the lease
	resp, err = worker.recordHeartbeat(ctx, newTestRequest("test-worker"))
	require.NoError(t, err)
	require.NotNil(t, resp)

	// Verify two tasks were scheduled (one for each heartbeat)
	require.Len(t, ctx.Tasks, 2)
}

func TestInvalidTransitions(t *testing.T) {
	ctx := &chasm.MockMutableContext{}

	t.Run("HeartbeatOnInactiveWorker", func(t *testing.T) {
		worker := newTestWorker()
		worker.Status = workerstatepb.WORKER_STATUS_INACTIVE

		resp, err := worker.recordHeartbeat(ctx, newTestRequest("test-worker"))

		// Should fail because worker is inactive (terminal state)
		require.Error(t, err)
		require.Nil(t, resp)
		_, ok := err.(*WorkerInactiveError)
		require.True(t, ok, "expected WorkerInactiveError")
	})

	t.Run("LeaseExpiryOnInactiveWorker", func(t *testing.T) {
		worker := newTestWorker()
		worker.Status = workerstatepb.WORKER_STATUS_INACTIVE

		event := EventLeaseExpired{}
		err := TransitionLeaseExpired.Apply(worker, ctx, event)

		// Should fail because worker is not active
		require.Error(t, err)
	})
}
