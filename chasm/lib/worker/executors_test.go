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
		cleanupDelay := 10 * time.Minute
		expectedCleanupTime := currentTime.Add(cleanupDelay)

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
		config := &Config{
			InactiveWorkerCleanupDelay: func(string) time.Duration {
				return cleanupDelay
			},
		}
		executor := NewLeaseExpiryTaskExecutor(log.NewNoopLogger(), config)

		// Execute the task
		err := executor.Execute(ctx, worker, chasm.TaskAttributes{}, &workerstatepb.LeaseExpiryTask{})

		// Verify transition was applied
		require.NoError(t, err)
		require.Equal(t, workerstatepb.WORKER_STATUS_INACTIVE, worker.Status)

		// Verify cleanup task was scheduled
		require.Len(t, ctx.Tasks, 1)
		cleanupTask, ok := ctx.Tasks[0].Payload.(*workerstatepb.CleanupTask)
		require.True(t, ok)
		require.NotNil(t, cleanupTask)

		// Verify cleanup time was set to exact expected time
		require.NotNil(t, worker.CleanupTime)
		require.Equal(t, expectedCleanupTime, worker.CleanupTime.AsTime())

		// Verify cleanup task is scheduled for the right time
		require.Equal(t, expectedCleanupTime, ctx.Tasks[0].Attributes.ScheduledTime)
	})
}

func TestLeaseExpiryTaskExecutor_Validate(t *testing.T) {
	config := &Config{
		InactiveWorkerCleanupDelay: func(string) time.Duration {
			return 10 * time.Minute
		},
	}
	executor := NewLeaseExpiryTaskExecutor(log.NewNoopLogger(), config)

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

func TestWorkerCleanupTaskExecutor_Execute(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		worker := NewWorker()
		worker.WorkerHeartbeat = &workerpb.WorkerHeartbeat{
			WorkerInstanceKey: "test-worker",
		}
		worker.Status = workerstatepb.WORKER_STATUS_INACTIVE
		worker.CleanupTime = timestamppb.New(time.Now())

		ctx := &chasm.MockMutableContext{}
		executor := NewWorkerCleanupTaskExecutor(log.NewNoopLogger())

		// Execute the task
		err := executor.Execute(ctx, worker, chasm.TaskAttributes{}, &workerstatepb.CleanupTask{})

		// Verify transition was applied
		require.NoError(t, err)
		require.Equal(t, workerstatepb.WORKER_STATUS_CLEANED_UP, worker.Status)

		require.Empty(t, ctx.Tasks)
	})
}

func TestWorkerCleanupTaskExecutor_Validate(t *testing.T) {
	executor := NewWorkerCleanupTaskExecutor(log.NewNoopLogger())

	t.Run("ValidTask", func(t *testing.T) {
		cleanupTime := time.Now().Add(10 * time.Minute)
		worker := NewWorker()
		worker.WorkerHeartbeat = &workerpb.WorkerHeartbeat{
			WorkerInstanceKey: "test-worker",
		}
		worker.Status = workerstatepb.WORKER_STATUS_INACTIVE
		worker.CleanupTime = timestamppb.New(cleanupTime)

		attrs := chasm.TaskAttributes{
			ScheduledTime: cleanupTime,
		}

		ctx := &chasm.MockMutableContext{}
		valid, err := executor.Validate(ctx, worker, attrs, &workerstatepb.CleanupTask{})

		require.NoError(t, err)
		require.True(t, valid)
	})

	t.Run("InvalidTask_WorkerNotInactive", func(t *testing.T) {
		cleanupTime := time.Now().Add(10 * time.Minute)
		worker := NewWorker()
		worker.WorkerHeartbeat = &workerpb.WorkerHeartbeat{
			WorkerInstanceKey: "test-worker",
		}
		worker.Status = workerstatepb.WORKER_STATUS_ACTIVE
		worker.CleanupTime = timestamppb.New(cleanupTime)

		attrs := chasm.TaskAttributes{
			ScheduledTime: cleanupTime,
		}

		ctx := &chasm.MockMutableContext{}
		valid, err := executor.Validate(ctx, worker, attrs, &workerstatepb.CleanupTask{})

		require.NoError(t, err)
		require.False(t, valid)
	})

	t.Run("InvalidTask_WorkerResurrected", func(t *testing.T) {
		oldCleanupTime := time.Now().Add(10 * time.Minute)

		worker := NewWorker()
		worker.WorkerHeartbeat = &workerpb.WorkerHeartbeat{
			WorkerInstanceKey: "test-worker",
		}
		worker.Status = workerstatepb.WORKER_STATUS_ACTIVE
		worker.CleanupTime = nil // Cleared during resurrection

		// Task scheduled for old cleanup time
		attrs := chasm.TaskAttributes{
			ScheduledTime: oldCleanupTime,
		}

		ctx := &chasm.MockMutableContext{}
		valid, err := executor.Validate(ctx, worker, attrs, &workerstatepb.CleanupTask{})

		require.NoError(t, err)
		require.False(t, valid)
	})

	t.Run("InvalidTask_CleanupRescheduled", func(t *testing.T) {
		oldCleanupTime := time.Now().Add(10 * time.Minute)
		newCleanupTime := time.Now().Add(20 * time.Minute)

		worker := NewWorker()
		worker.WorkerHeartbeat = &workerpb.WorkerHeartbeat{
			WorkerInstanceKey: "test-worker",
		}
		worker.Status = workerstatepb.WORKER_STATUS_INACTIVE
		worker.CleanupTime = timestamppb.New(newCleanupTime)

		// Task scheduled for old cleanup time
		attrs := chasm.TaskAttributes{
			ScheduledTime: oldCleanupTime,
		}

		ctx := &chasm.MockMutableContext{}
		valid, err := executor.Validate(ctx, worker, attrs, &workerstatepb.CleanupTask{})

		require.NoError(t, err)
		require.False(t, valid)
	})

	t.Run("InvalidTask_NilCleanupTime", func(t *testing.T) {
		worker := NewWorker()
		worker.WorkerHeartbeat = &workerpb.WorkerHeartbeat{
			WorkerInstanceKey: "test-worker",
		}
		worker.Status = workerstatepb.WORKER_STATUS_INACTIVE
		worker.CleanupTime = nil

		attrs := chasm.TaskAttributes{
			ScheduledTime: time.Now(),
		}

		ctx := &chasm.MockMutableContext{}
		valid, err := executor.Validate(ctx, worker, attrs, &workerstatepb.CleanupTask{})

		require.NoError(t, err)
		require.False(t, valid)
	})
}
