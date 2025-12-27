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

func TestLeaseExpiryTaskExecutor_Execute(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		currentTime := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)

		worker := NewWorker()
		worker.WorkerHeartbeat = &workerpb.WorkerHeartbeat{
			WorkerInstanceKey: "test-worker",
		}
		worker.Status = workerstatepb.WORKER_STATUS_ACTIVE
		worker.LeaseExpirationTime = timestamppb.New(currentTime)

		ctx := &chasm.MockMutableContext{
			MockContext: chasm.MockContext{
				HandleNow: func(component chasm.Component) time.Time {
					return currentTime
				},
			},
		}
		executor := NewLeaseExpiryTaskExecutor(log.NewNoopLogger())

		// Execute the task
		err := executor.Execute(ctx, worker, chasm.TaskAttributes{}, &workerstatepb.LeaseExpiryTask{})

		// Verify transition was applied - worker is now inactive (terminal)
		require.NoError(t, err)
		require.Equal(t, workerstatepb.WORKER_STATUS_INACTIVE, worker.Status)

		// Verify no additional tasks were scheduled (no cleanup task needed)
		require.Empty(t, ctx.Tasks)
	})
}

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
