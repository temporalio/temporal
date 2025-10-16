package matching

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	taskqueuespb "go.temporal.io/server/api/taskqueue/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/future"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"google.golang.org/protobuf/types/known/durationpb"
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

		// TODO(pri): remove
		getDB() *taskQueueDB
	}

	backlogManagerImpl struct {
		pqMgr            physicalTaskQueueManager
		tqCtx            context.Context
		db               *taskQueueDB
		taskWriter       *taskWriter
		taskReader       *taskReader // reads tasks from db and async matches it with poller
		taskGC           *taskGC
		taskAckManager   *ackManager // tracks ackLevel for delivered messages
		config           *taskQueueConfig
		logger           log.Logger
		throttledLogger  log.ThrottledLogger
		matchingClient   matchingservice.MatchingServiceClient
		metricsHandler   metrics.Handler
		initializedError *future.FutureImpl[struct{}]
		// skipFinalUpdate controls behavior on Stop: if it's false, we try to write one final
		// update before unloading
		skipFinalUpdate atomic.Bool
	}
)

var _ backlogManager = (*backlogManagerImpl)(nil)

func newBacklogManager(
	tqCtx context.Context,
	pqMgr physicalTaskQueueManager,
	config *taskQueueConfig,
	taskManager persistence.TaskManager,
	logger log.Logger,
	throttledLogger log.ThrottledLogger,
	matchingClient matchingservice.MatchingServiceClient,
	metricsHandler metrics.Handler,
) *backlogManagerImpl {
	bmg := &backlogManagerImpl{
		pqMgr:            pqMgr,
		tqCtx:            tqCtx,
		matchingClient:   matchingClient,
		metricsHandler:   metricsHandler,
		logger:           logger,
		throttledLogger:  throttledLogger,
		config:           config,
		initializedError: future.NewFuture[struct{}](),
	}
	bmg.db = newTaskQueueDB(config, taskManager, pqMgr.QueueKey(), logger, metricsHandler)
	bmg.taskWriter = newTaskWriter(bmg)
	bmg.taskReader = newTaskReader(bmg)
	bmg.taskAckManager = newAckManager(bmg.db, logger)
	bmg.taskGC = newTaskGC(tqCtx, bmg.db, config)

	return bmg
}

// signalIfFatal calls UnloadFromPartitionManager of the physicalTaskQueueManager
// if and only if the supplied error represents a fatal condition, e.g. the existence
// of a newer lease by another backlogManager. Returns true if the unload signal
// is emitted, false otherwise.
func (c *backlogManagerImpl) signalIfFatal(err error) bool {
	if err == nil {
		return false
	}
	var condfail *persistence.ConditionFailedError
	if errors.As(err, &condfail) {
		c.metricsHandler.Counter(metrics.ConditionFailedErrorPerTaskQueueCounter.Name()).Record(1)
		c.skipFinalUpdate.Store(true)
		c.pqMgr.UnloadFromPartitionManager(unloadCauseConflict)
		return true
	}
	return false
}

func (c *backlogManagerImpl) Start() {
	c.taskWriter.Start()
	c.taskReader.Start()
}

func (c *backlogManagerImpl) Stop() {
	// Maybe try to write one final update of ack level and GC some tasks.
	// Skip the update if we never initialized (ackLevel will be -1 in that case).
	// Also skip if we're stopping due to lost ownership (the update will fail in that case).
	// Ignore any errors.
	// Note that it's fine to GC even if the update ack level fails because we did match the
	// tasks, the next owner will just read over an empty range.
	ackLevel := c.taskAckManager.getAckLevel()
	if ackLevel >= 0 && !c.skipFinalUpdate.Load() {
		ctx, cancel := context.WithTimeout(c.tqCtx, ioTimeout)
		defer cancel()

		_ = c.db.OldUpdateState(ctx, ackLevel)
		c.taskGC.RunNow(ackLevel)
	}
}

func (c *backlogManagerImpl) SetInitializedError(err error) {
	c.initializedError.Set(struct{}{}, err)
	if err != nil {
		// We can't recover from here without starting over, so unload the whole task queue.
		// Skip final update since we never initialized.
		c.skipFinalUpdate.Store(true)
		c.pqMgr.UnloadFromPartitionManager(unloadCauseInitError)
	}
}

func (c *backlogManagerImpl) WaitUntilInitialized(ctx context.Context) error {
	_, err := c.initializedError.Get(ctx)
	return err
}

func (c *backlogManagerImpl) SpoolTask(taskInfo *persistencespb.TaskInfo) error {
	err := c.taskWriter.appendTask(taskInfo)
	c.signalIfFatal(err)
	if err == nil {
		c.taskReader.Signal()
	}
	return err
}

// TODO(pri): old matcher cleanup
func (c *backlogManagerImpl) processSpooledTask(
	ctx context.Context,
	task *internalTask,
) error {
	return c.pqMgr.ProcessSpooledTask(ctx, task)
}

func (c *backlogManagerImpl) BacklogCountHint() int64 {
	return c.taskAckManager.getBacklogCountHint()
}

func (c *backlogManagerImpl) BacklogStatus() *taskqueuepb.TaskQueueStatus {
	taskIDBlock := rangeIDToTaskIDBlock(c.db.RangeID(), c.config.RangeSize)
	return &taskqueuepb.TaskQueueStatus{
		ReadLevel: c.taskAckManager.getReadLevel(),
		AckLevel:  c.taskAckManager.getAckLevel(),
		// use getTotalApproximateBacklogCount instead of BacklogCountHint since it's more accurate
		BacklogCountHint: c.db.getTotalApproximateBacklogCount(),
		TaskIdBlock: &taskqueuepb.TaskIdBlock{
			StartId: taskIDBlock.start,
			EndId:   taskIDBlock.end,
		},
	}
}

func (c *backlogManagerImpl) BacklogStatsByPriority() map[int32]*taskqueuepb.TaskQueueStats {
	defaultPriority := int32(c.config.DefaultPriorityKey)
	return map[int32]*taskqueuepb.TaskQueueStats{
		defaultPriority: &taskqueuepb.TaskQueueStats{
			ApproximateBacklogCount: c.db.getTotalApproximateBacklogCount(),
			ApproximateBacklogAge:   durationpb.New(c.taskReader.getBacklogHeadAge()),
		},
	}
}

func (c *backlogManagerImpl) InternalStatus() []*taskqueuespb.InternalTaskQueueStatus {
	currentTaskIDBlock := c.taskWriter.getCurrentTaskIDBlock()
	return []*taskqueuespb.InternalTaskQueueStatus{
		&taskqueuespb.InternalTaskQueueStatus{
			ReadLevel: c.taskAckManager.getReadLevel(),
			AckLevel:  c.taskAckManager.getAckLevel(),
			TaskIdBlock: &taskqueuepb.TaskIdBlock{
				StartId: currentTaskIDBlock.start,
				EndId:   currentTaskIDBlock.end,
			},
			LoadedTasks:             c.taskAckManager.getBacklogCountHint(),
			MaxReadLevel:            c.db.GetMaxReadLevel(subqueueZero),
			ApproximateBacklogCount: c.db.getTotalApproximateBacklogCount(),
		},
	}
}

// completeTask marks a task as processed. Only tasks created by taskReader (i.e. backlog from db) reach
// here. As part of completion:
//   - task is deleted from the database when err is nil
//   - new task is created and current task is deleted when err is not nil
func (c *backlogManagerImpl) completeTask(itask *internalTask, err error) {
	task := itask.event.AllocatedTaskInfo
	if err != nil {
		// failed to start the task.
		// We cannot just remove it from persistence because then it will be lost.
		// We handle this by writing the task back to persistence with a higher taskID.
		// This will allow subsequent tasks to make progress, and hopefully by the time this task is picked-up
		// again the underlying reason for failing to start will be resolved.
		metrics.TaskRewrites.With(c.metricsHandler).Record(1)
		err = executeWithRetry(context.Background(), func(_ context.Context) error {
			return c.taskWriter.appendTask(task.Data)
		})

		if err != nil {
			// OK, we also failed to write to persistence.
			// This should only happen in very extreme cases where persistence is completely down.
			// We still can't lose the old task, so we just unload the entire task queue.
			// We haven't advanced the ack level past this task, so when the task queue reloads,
			// it will see this task again.
			c.logger.Error("Persistent store operation failure",
				tag.StoreOperationStopTaskQueue,
				tag.Error(err),
				tag.WorkflowTaskQueueName(c.queueKey().PersistenceName()),
				tag.WorkflowTaskQueueType(c.queueKey().TaskType()))
			// Skip final update since persistence is having problems.
			c.skipFinalUpdate.Store(true)
			c.pqMgr.UnloadFromPartitionManager(unloadCauseOtherError)
			return
		}
		c.taskReader.Signal()
	}

	ackLevel, numAcked := c.taskAckManager.completeTask(task.GetTaskId())
	if numAcked > 0 {
		var backlogHead time.Time
		if nanos := c.taskReader.backlogHeadCreateTime.Load(); nanos > 0 {
			backlogHead = time.Unix(0, nanos)
		}
		c.db.updateBacklogStats(-numAcked, backlogHead)
	}
	c.taskGC.Run(ackLevel)
}

func rangeIDToTaskIDBlock(rangeID int64, rangeSize int64) taskIDBlock {
	return taskIDBlock{
		start: (rangeID-1)*rangeSize + 1,
		end:   rangeID * rangeSize,
	}
}

// Retry operation on transient error.
func executeWithRetry(
	ctx context.Context,
	operation func(context.Context) error,
) error {
	return backoff.ThrottleRetryContext(ctx, operation, persistenceOperationRetryPolicy, func(err error) bool {
		if common.IsContextDeadlineExceededErr(err) || common.IsContextCanceledErr(err) {
			return false
		}
		if _, ok := err.(*persistence.ConditionFailedError); ok {
			return false
		}
		return common.IsPersistenceTransientError(err)
	})
}

func (c *backlogManagerImpl) queueKey() *PhysicalTaskQueueKey {
	return c.pqMgr.QueueKey()
}

func (c *backlogManagerImpl) getDB() *taskQueueDB {
	return c.db
}
