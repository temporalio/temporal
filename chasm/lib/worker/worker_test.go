package worker

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	workerpb "go.temporal.io/api/worker/v1"
	"go.temporal.io/server/chasm"
	workerstatepb "go.temporal.io/server/chasm/lib/worker/gen/workerpb/v1"
)

func TestNewWorker(t *testing.T) {
	worker := NewWorker()

	// Verify basic initialization
	require.NotNil(t, worker)
	require.Equal(t, workerstatepb.WORKER_STATUS_ACTIVE, worker.Status)
	require.Nil(t, worker.LeaseExpirationTime)
	require.Nil(t, worker.WorkerHeartbeat)  // No heartbeat data initially
	require.Equal(t, "", worker.WorkerID()) // Empty until first heartbeat
}

func TestWorkerLifecycleState(t *testing.T) {
	worker := NewWorker()
	var ctx chasm.Context

	// Test ACTIVE -> Running
	state := worker.LifecycleState(ctx)
	require.Equal(t, chasm.LifecycleStateRunning, state)

	// Test INACTIVE -> Running (still running until cleanup)
	worker.Status = workerstatepb.WORKER_STATUS_INACTIVE
	state = worker.LifecycleState(ctx)
	require.Equal(t, chasm.LifecycleStateRunning, state)

	// Test CLEANED_UP -> Completed
	worker.Status = workerstatepb.WORKER_STATUS_CLEANED_UP
	state = worker.LifecycleState(ctx)
	require.Equal(t, chasm.LifecycleStateCompleted, state)
}

func TestWorkerRecordHeartbeat(t *testing.T) {
	worker := NewWorker()
	ctx := &chasm.MockMutableContext{}

	heartbeat := &workerpb.WorkerHeartbeat{
		WorkerInstanceKey: "test-worker-3",
	}
	leaseDuration := 30 * time.Second

	// Test recording heartbeat on new worker
	err := worker.RecordHeartbeat(ctx, heartbeat, leaseDuration)
	require.NoError(t, err)

	// Verify heartbeat data was set
	require.Equal(t, heartbeat, worker.WorkerHeartbeat)
	require.Equal(t, "test-worker-3", worker.WorkerID())

	// Verify lease expiration time was set
	require.NotNil(t, worker.LeaseExpirationTime)

	// Verify worker is still active
	require.Equal(t, workerstatepb.WORKER_STATUS_ACTIVE, worker.Status)

	// Verify a task was scheduled (lease expiry)
	require.Len(t, ctx.Tasks, 1)
}
