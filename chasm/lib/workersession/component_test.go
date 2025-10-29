package workersession

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/chasm"
	workersessionpb "go.temporal.io/server/chasm/lib/workersession/gen/workersessionpb/v1"
)

func TestNewWorkerSession(t *testing.T) {
	session := NewWorkerSession()

	// Verify basic initialization
	require.NotNil(t, session)
	require.Equal(t, workersessionpb.WORKER_SESSION_STATUS_ACTIVE, session.Status)
	require.Nil(t, session.LeaseDeadline)
}

func TestWorkerSessionLifecycleState(t *testing.T) {
	session := NewWorkerSession()
	var ctx chasm.Context

	// Test ACTIVE -> Running
	state := session.LifecycleState(ctx)
	require.Equal(t, chasm.LifecycleStateRunning, state)

	// Test EXPIRED -> Running (still running until cleanup)
	session.Status = workersessionpb.WORKER_SESSION_STATUS_EXPIRED
	state = session.LifecycleState(ctx)
	require.Equal(t, chasm.LifecycleStateRunning, state)

	// Test TERMINATED -> Completed
	session.Status = workersessionpb.WORKER_SESSION_STATUS_TERMINATED
	state = session.LifecycleState(ctx)
	require.Equal(t, chasm.LifecycleStateCompleted, state)
}
