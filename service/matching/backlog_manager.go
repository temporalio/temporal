package matching

import (
	"context"
	"time"

	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	taskqueuespb "go.temporal.io/server/api/taskqueue/v1"
	"go.temporal.io/server/common/backoff"
)

var (
	// This retry policy is currently only used for matching persistence operations
	// that, if failed, the entire task queue needs to be reloaded.
	persistenceOperationRetryPolicy = backoff.NewExponentialRetryPolicy(50 * time.Millisecond).
					WithMaximumInterval(1 * time.Second).
					WithExpirationInterval(30 * time.Second)

	// This retry policy is used for the initial metadata load and range id takeover.
	foreverRetryPolicy = backoff.NewExponentialRetryPolicy(1 * time.Second).
				WithMaximumInterval(10 * time.Second).
				WithExpirationInterval(backoff.NoInterval)
)

type (
	// backlogManagerImpl manages the backlog and persistence of a physical task queue
	backlogManager interface {
		Start()
		Stop()
		WaitUntilInitialized(context.Context) error
		SpoolTask(taskInfo *persistencespb.TaskInfo) error
		// BacklogCountHint returns the number of backlog tasks loaded in memory now.
		// It's returned as a hint to the SDK to influence polling behavior (sticky vs normal).
		BacklogCountHint() int64
		BacklogStatus() *taskqueuepb.TaskQueueStatus
		BacklogStatsByPriority() map[int32]*taskqueuepb.TaskQueueStats
		InternalStatus() []*taskqueuespb.InternalTaskQueueStatus
		// FinalGC does a final gc pass before unloading.
		// Used when unloading a draining queue that won't be reloaded.
		FinalGC()

		// TODO(pri): remove
		getDB() *taskQueueDB
	}
)

func rangeIDToTaskIDBlock(rangeID int64, rangeSize int64) taskIDBlock {
	return taskIDBlock{
		start: (rangeID-1)*rangeSize + 1,
		end:   rangeID * rangeSize,
	}
}
