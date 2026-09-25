package worker

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	workerpb "go.temporal.io/api/worker/v1"
	"go.temporal.io/server/chasm"
	workerstatepb "go.temporal.io/server/chasm/lib/worker/gen/workerpb/v1"
	"go.temporal.io/server/common/log"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// TestLeaseExpiryTaskExecutor_Validate tests the executor's Validate method.
// Note: The underlying isLeaseExpiryTaskValid logic is a Worker method;
// these tests verify the executor wiring and logging behavior.
func TestLeaseExpiryTaskExecutor_Validate(t *testing.T) {
	executor := NewLeaseExpiryTaskExecutor(log.NewNoopLogger())

	t.Run("ValidTask", func(t *testing.T) {
		leaseExpiry := time.Now().Add(1 * time.Minute)
		worker := NewWorker()
		worker.WorkerHeartbeat = &workerpb.WorkerHeartbeat{
			WorkerInstanceKey: "test-worker",
		}
		worker.Status = workerstatepb.WORKER_STATUS_ACTIVE
		worker.LeaseExpirationTime = timestamppb.New(leaseExpiry)

		attrs := chasm.TaskAttributes{
			ScheduledTime: leaseExpiry,
		}

		ctx := &chasm.MockMutableContext{}
		valid, err := executor.Validate(ctx, worker, attrs, &workerstatepb.LeaseExpiryTask{})

		require.NoError(t, err)
		require.True(t, valid)
	})

	t.Run("InvalidTask_WorkerNotActive", func(t *testing.T) {
		leaseExpiry := time.Now().Add(1 * time.Minute)
		worker := NewWorker()
		worker.WorkerHeartbeat = &workerpb.WorkerHeartbeat{
			WorkerInstanceKey: "test-worker",
		}
		worker.Status = workerstatepb.WORKER_STATUS_INACTIVE
		worker.LeaseExpirationTime = timestamppb.New(leaseExpiry)

		attrs := chasm.TaskAttributes{
			ScheduledTime: leaseExpiry,
		}

		ctx := &chasm.MockMutableContext{}
		valid, err := executor.Validate(ctx, worker, attrs, &workerstatepb.LeaseExpiryTask{})

		require.NoError(t, err)
		require.False(t, valid)
	})

	t.Run("InvalidTask_LeaseRenewed", func(t *testing.T) {
		oldLeaseExpiry := time.Now().Add(1 * time.Minute)
		newLeaseExpiry := time.Now().Add(2 * time.Minute)

		worker := NewWorker()
		worker.WorkerHeartbeat = &workerpb.WorkerHeartbeat{
			WorkerInstanceKey: "test-worker",
		}
		worker.Status = workerstatepb.WORKER_STATUS_ACTIVE
		worker.LeaseExpirationTime = timestamppb.New(newLeaseExpiry)

		// Task scheduled for old lease expiry
		attrs := chasm.TaskAttributes{
			ScheduledTime: oldLeaseExpiry,
		}

		ctx := &chasm.MockMutableContext{}
		valid, err := executor.Validate(ctx, worker, attrs, &workerstatepb.LeaseExpiryTask{})

		require.NoError(t, err)
		require.False(t, valid)
	})

	t.Run("InvalidTask_NilLeaseExpiration", func(t *testing.T) {
		worker := NewWorker()
		worker.WorkerHeartbeat = &workerpb.WorkerHeartbeat{
			WorkerInstanceKey: "test-worker",
		}
		worker.Status = workerstatepb.WORKER_STATUS_ACTIVE
		worker.LeaseExpirationTime = nil

		attrs := chasm.TaskAttributes{
			ScheduledTime: time.Now(),
		}

		ctx := &chasm.MockMutableContext{}
		valid, err := executor.Validate(ctx, worker, attrs, &workerstatepb.LeaseExpiryTask{})

		require.NoError(t, err)
		require.False(t, valid)
	})
}
