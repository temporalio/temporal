package worker

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/chasm"
	workerpb "go.temporal.io/server/chasm/lib/worker/gen/workerpb/v1"
)

func TestNewWorker(t *testing.T) {
	worker := NewWorker()

	// Verify basic initialization
	require.NotNil(t, worker)
	require.Equal(t, workerpb.WORKER_STATUS_ACTIVE, worker.Status)
	require.Nil(t, worker.LeaseExpirationTime)
}

func TestWorkerLifecycleState(t *testing.T) {
	worker := NewWorker()
	var ctx chasm.Context

	// Test ACTIVE -> Running
	state := worker.LifecycleState(ctx)
	require.Equal(t, chasm.LifecycleStateRunning, state)

	// Test INACTIVE -> Running (still running until cleanup)
	worker.Status = workerpb.WORKER_STATUS_INACTIVE
	state = worker.LifecycleState(ctx)
	require.Equal(t, chasm.LifecycleStateRunning, state)

	// Test CLEANED_UP -> Completed
	worker.Status = workerpb.WORKER_STATUS_CLEANED_UP
	state = worker.LifecycleState(ctx)
	require.Equal(t, chasm.LifecycleStateCompleted, state)
}
