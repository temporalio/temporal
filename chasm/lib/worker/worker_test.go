package worker

import (
	"testing"

	"github.com/stretchr/testify/require"
	workerpb "go.temporal.io/api/worker/v1"
	workflowservice "go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	workerstatepb "go.temporal.io/server/chasm/lib/worker/gen/workerpb/v1"
)

func TestNewWorker(t *testing.T) {
	worker := NewWorker()

	// Verify basic initialization
	require.NotNil(t, worker)
	require.Equal(t, workerstatepb.WORKER_STATUS_ACTIVE, worker.Status)
	require.Nil(t, worker.LeaseExpirationTime)
	require.Nil(t, worker.WorkerHeartbeat) // No heartbeat data initially
	require.Empty(t, worker.workerID())    // Empty until first heartbeat
	require.Zero(t, worker.ConflictToken)  // Zero until first heartbeat
}

func TestWorkerLifecycleState(t *testing.T) {
	worker := NewWorker()
	var ctx chasm.Context

	// Test ACTIVE -> Running
	state := worker.LifecycleState(ctx)
	require.Equal(t, chasm.LifecycleStateRunning, state)

	// Test INACTIVE -> Completed (terminal state)
	worker.Status = workerstatepb.WORKER_STATUS_INACTIVE
	state = worker.LifecycleState(ctx)
	require.Equal(t, chasm.LifecycleStateCompleted, state)
}

func TestWorkerRecordHeartbeat(t *testing.T) {
	worker := NewWorker()
	ctx := &chasm.MockMutableContext{}

	req := &workerstatepb.RecordHeartbeatRequest{
		NamespaceId: "test-namespace-id",
		FrontendRequest: &workflowservice.RecordWorkerHeartbeatRequest{
			Namespace: "test-namespace",
			Identity:  "test-identity",
			WorkerHeartbeat: []*workerpb.WorkerHeartbeat{
				{
					WorkerInstanceKey: "test-worker-3",
				},
			},
		},
	}

	// Test recording heartbeat on new worker
	resp, err := worker.recordHeartbeat(ctx, req)
	require.NoError(t, err)
	require.NotNil(t, resp)

	// Verify heartbeat data was set
	require.Equal(t, "test-worker-3", worker.workerID())

	// Verify lease expiration time was set
	require.NotNil(t, worker.LeaseExpirationTime)

	// Verify worker is still active
	require.Equal(t, workerstatepb.WORKER_STATUS_ACTIVE, worker.Status)

	// Verify a task was scheduled (lease expiry)
	require.Len(t, ctx.Tasks, 1)

	// Verify conflict token was incremented
	require.Equal(t, int64(1), worker.ConflictToken)
}

func TestWorkerRecordHeartbeat_InactiveWorker(t *testing.T) {
	worker := NewWorker()
	worker.Status = workerstatepb.WORKER_STATUS_INACTIVE
	ctx := &chasm.MockMutableContext{}

	req := &workerstatepb.RecordHeartbeatRequest{
		NamespaceId: "test-namespace-id",
		FrontendRequest: &workflowservice.RecordWorkerHeartbeatRequest{
			Namespace: "test-namespace",
			Identity:  "test-identity",
			WorkerHeartbeat: []*workerpb.WorkerHeartbeat{
				{
					WorkerInstanceKey: "test-worker",
				},
			},
		},
	}

	// Heartbeat on inactive worker should fail
	_, err := worker.recordHeartbeat(ctx, req)
	require.Error(t, err)
	_, ok := err.(*WorkerInactiveError)
	require.True(t, ok, "expected WorkerInactiveError")
}

func TestEncodeAndValidateConflictToken(t *testing.T) {
	worker := NewWorker()

	// Set conflict token
	worker.ConflictToken = 42

	// Encode token
	token := worker.encodeConflictToken()
	require.Len(t, token, 8)

	// Validate correct token
	require.True(t, worker.validateConflictToken(token))

	// Validate wrong token
	wrongToken := []byte{0, 0, 0, 0, 0, 0, 0, 1}
	require.False(t, worker.validateConflictToken(wrongToken))

	// Validate wrong length
	require.False(t, worker.validateConflictToken([]byte{1, 2, 3}))
}
