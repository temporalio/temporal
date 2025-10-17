package matching

import (
	"context"
	"errors"
	"slices"
	"sync"
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

// this retry policy is currently only used for matching persistence operations
// that, if failed, the entire task queue needs to be reloaded
// TODO(pri): old matcher cleanup: move here
// var persistenceOperationRetryPolicy = backoff.NewExponentialRetryPolicy(50 * time.Millisecond).
// 	WithMaximumInterval(1 * time.Second).
// 	WithExpirationInterval(30 * time.Second)

type (
	// backlogManager manages the backlog and persistence of a physical task queue
	// TODO(pri): old matcher cleanup: move here
	// backlogManager interface {
	// 	Start()
	// 	Stop()
	// 	WaitUntilInitialized(context.Context) error
	// 	SpoolTask(taskInfo *persistencespb.TaskInfo) error
	// 	BacklogCountHint() int64
	// 	BacklogStatus() *taskqueuepb.TaskQueueStatus
	// 	TotalApproximateBacklogCount() int64
	// 	BacklogHeadAge() time.Duration
	// }

	priBacklogManagerImpl struct {
		pqMgr      physicalTaskQueueManager
		config     *taskQueueConfig
		tqCtx      context.Context
		isDraining bool
		db         *taskQueueDB
		taskWriter *priTaskWriter

		subqueueLock        sync.Mutex
		subqueues           []*priTaskReader // subqueue index -> fairTaskReader
		subqueuesByPriority map[priorityKey]subqueueIndex
		priorityBySubqueue  map[subqueueIndex]priorityKey

		logger           log.Logger
		throttledLogger  log.ThrottledLogger
		matchingClient   matchingservice.MatchingServiceClient
		metricsHandler   metrics.Handler
		initializedError *future.FutureImpl[struct{}]
		// skipFinalUpdate controls behavior on Stop: if it's false, we try to write one final
		// update before unloading
		skipFinalUpdate atomic.Bool
	}

	priorityKey int32
)

var _ backlogManager = (*priBacklogManagerImpl)(nil)

func newPriBacklogManager(
	tqCtx context.Context,
	pqMgr physicalTaskQueueManager,
	config *taskQueueConfig,
	taskManager persistence.TaskManager,
	logger log.Logger,
	throttledLogger log.ThrottledLogger,
	matchingClient matchingservice.MatchingServiceClient,
	metricsHandler metrics.Handler,
	isDraining bool,
) *priBacklogManagerImpl {
	bmg := &priBacklogManagerImpl{
		pqMgr:               pqMgr,
		config:              config,
		tqCtx:               tqCtx,
		isDraining:          isDraining,
		db:                  newTaskQueueDB(config, taskManager, pqMgr.QueueKey(), logger, metricsHandler, isDraining),
		subqueuesByPriority: make(map[priorityKey]subqueueIndex),
		priorityBySubqueue:  make(map[subqueueIndex]priorityKey),
		matchingClient:      matchingClient,
		metricsHandler:      metricsHandler,
		logger:              logger,
		throttledLogger:     throttledLogger,
		initializedError:    future.NewFuture[struct{}](),
	}
	bmg.taskWriter = newPriTaskWriter(bmg)
	return bmg
}

// signalIfFatal calls UnloadFromPartitionManager of the physicalTaskQueueManager
// if and only if the supplied error represents a fatal condition, e.g. the existence
// of a newer lease by another backlogManager. Returns true if the unload signal
// is emitted, false otherwise.
func (c *priBacklogManagerImpl) signalIfFatal(err error) bool {
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

func (c *priBacklogManagerImpl) Start() {
	c.taskWriter.Start()
}

func (c *priBacklogManagerImpl) Stop() {
	// Maybe try to write one final update of ack level. Skip the update if we never
	// initialized. Also skip if we're stopping due to lost ownership (the update will
	// fail in that case). Ignore any errors. Don't bother with GC, the next reload will
	// handle that.
	if !c.initializedError.Ready() || c.skipFinalUpdate.Load() {
		return
	}

	c.subqueueLock.Lock()
	for i, r := range c.subqueues {
		_, ackLevel := r.getLevels()
		// oldestTime can be time.Time{} here since countDelta is 0
		c.db.updateAckLevelAndBacklogStats(subqueueIndex(i), ackLevel, 0, time.Time{})
	}
	c.subqueueLock.Unlock()

	ctx, cancel := context.WithTimeout(c.tqCtx, ioTimeout)
	_ = c.db.SyncState(ctx)
	cancel()
}

func (c *priBacklogManagerImpl) initState(state taskQueueState, err error) {
	defer c.initializedError.Set(struct{}{}, err)

	if err != nil {
		// We can't recover from here without starting over, so unload the whole task queue.
		// Skip final update since we never initialized.
		c.skipFinalUpdate.Store(true)
		c.pqMgr.UnloadFromPartitionManager(unloadCauseInitError)
		return
	}

	if state.otherHasTasks {
		c.pqMgr.SetupDraining()
	}

	c.subqueueLock.Lock()
	defer c.subqueueLock.Unlock()

	c.loadSubqueuesLocked(state.subqueues)
	go c.periodicSync()
}

func (c *priBacklogManagerImpl) WaitUntilInitialized(ctx context.Context) error {
	_, err := c.initializedError.Get(ctx)
	return err
}

func (c *priBacklogManagerImpl) loadSubqueuesLocked(subqueues []persistencespb.SubqueueInfo) {
	// TODO(pri): This assumes that subqueues never shrinks, and priority/fairness index of
	// existing subqueues never changes. If we change that, this logic will need to change.
	for i := range subqueues {
		if i >= len(c.subqueues) {
			r := newPriTaskReader(c, subqueueIndex(i), subqueues[i].AckLevel)
			r.Start()
			c.subqueues = append(c.subqueues, r)
		}
		c.subqueuesByPriority[priorityKey(subqueues[i].Key.Priority)] = subqueueIndex(i)
		c.priorityBySubqueue[subqueueIndex(i)] = priorityKey(subqueues[i].Key.Priority)
	}
}

func (c *priBacklogManagerImpl) getSubqueueForPriority(priority priorityKey) subqueueIndex {
	priority = c.config.clipPriority(priority)

	c.subqueueLock.Lock()
	defer c.subqueueLock.Unlock()

	if i, ok := c.subqueuesByPriority[priority]; ok {
		return i
	}

	// We need to allocate a new subqueue. Note this is doing io under subqueueLock,
	// but we want to serialize these updates.
	// TODO(pri): maybe we can improve that
	subqueues, err := c.db.AllocateSubqueue(c.tqCtx, &persistencespb.SubqueueKey{
		Priority: int32(priority),
	})
	if err != nil {
		c.signalIfFatal(err)
		// If we failed to write the metadata update, just use subqueueZero.
		// If err was a fatal error (most likely case), the subsequent call to SpoolTask will fail.
		return subqueueZero
	}

	c.loadSubqueuesLocked(subqueues)

	// After AllocateSubqueue added a subqueue for this priority, and we merged the result into
	// our state with loadSubqueuesLocked, this lookup should now find a subqueue.
	if i, ok := c.subqueuesByPriority[priority]; ok {
		return i
	}

	// But if something went wrong, return zero.
	return subqueueZero
}

func (c *priBacklogManagerImpl) periodicSync() {
	for {
		select {
		case <-c.tqCtx.Done():
			return
		case <-time.After(c.config.UpdateAckInterval()):
			ctx, cancel := context.WithTimeout(c.tqCtx, ioTimeout)
			err := c.db.SyncState(ctx)
			cancel()
			c.signalIfFatal(err)
		}
	}
}

func (c *priBacklogManagerImpl) SpoolTask(taskInfo *persistencespb.TaskInfo) error {
	subqueue := c.getSubqueueForPriority(priorityKey(taskInfo.Priority.GetPriorityKey()))
	err := c.taskWriter.appendTask(subqueue, taskInfo)
	c.signalIfFatal(err)
	return err
}

func (c *priBacklogManagerImpl) signalReaders(resp createTasksResponse) {
	c.subqueueLock.Lock()
	subqueues := slices.Clone(c.subqueues)
	c.subqueueLock.Unlock()

	for subqueue, subqueueResp := range resp.bySubqueue {
		subqueues[subqueue].signalNewTasks(subqueueResp)
	}
}

func (c *priBacklogManagerImpl) addSpooledTask(task *internalTask) error {
	return c.pqMgr.AddSpooledTask(task)
}

func (c *priBacklogManagerImpl) BacklogCountHint() (total int64) {
	c.subqueueLock.Lock()
	defer c.subqueueLock.Unlock()
	for _, r := range c.subqueues {
		total += int64(r.getLoadedTasks())
	}
	return
}

func (c *priBacklogManagerImpl) BacklogStatsByPriority() map[int32]*taskqueuepb.TaskQueueStats {
	c.subqueueLock.Lock()
	defer c.subqueueLock.Unlock()

	result := make(map[int32]*taskqueuepb.TaskQueueStats)
	backlogCounts := c.db.getApproximateBacklogCountsBySubqueue()
	for subqueueIdx, priorityKey := range c.priorityBySubqueue {
		pk := int32(priorityKey)

		// Note that there could be more than one subqueue for the same priority.
		if _, ok := result[pk]; !ok {
			result[pk] = &taskqueuepb.TaskQueueStats{
				// TODO(pri): returning 0 to match existing behavior, but maybe emptyBacklogAge would
				// be more appropriate in the future.
				ApproximateBacklogAge: durationpb.New(0),
			}
		}

		// Add backlog counts together across all subqueues for the same priority.
		result[pk].ApproximateBacklogCount += backlogCounts[subqueueIdx]

		// Find greatest backlog age for across all subqueues for the same priority.
		oldestBacklogTime := c.subqueues[subqueueIdx].getOldestBacklogTime()
		if !oldestBacklogTime.IsZero() {
			oldestBacklogAge := time.Since(oldestBacklogTime)
			if oldestBacklogAge > result[pk].ApproximateBacklogAge.AsDuration() {
				result[pk].ApproximateBacklogAge = durationpb.New(oldestBacklogAge)
			}
		}
	}
	return result
}

func (c *priBacklogManagerImpl) BacklogStatus() *taskqueuepb.TaskQueueStatus {
	c.subqueueLock.Lock()
	defer c.subqueueLock.Unlock()

	// TODO(pri): needs more work for subqueues, for now just return read/ack level for subqueue 0
	var readLevel, ackLevel int64
	if len(c.subqueues) > 0 {
		readLevel, ackLevel = c.subqueues[subqueueZero].getLevels()
	}

	taskIDBlock := rangeIDToTaskIDBlock(c.db.RangeID(), c.config.RangeSize)
	return &taskqueuepb.TaskQueueStatus{
		ReadLevel: readLevel,
		AckLevel:  ackLevel,
		// use getTotalApproximateBacklogCount instead of BacklogCountHint since it's more accurate
		BacklogCountHint: c.db.getTotalApproximateBacklogCount(),
		TaskIdBlock: &taskqueuepb.TaskIdBlock{
			StartId: taskIDBlock.start,
			EndId:   taskIDBlock.end,
		},
	}
}

func (c *priBacklogManagerImpl) InternalStatus() []*taskqueuespb.InternalTaskQueueStatus {
	currentTaskIDBlock := c.taskWriter.getCurrentTaskIDBlock()

	c.subqueueLock.Lock()
	defer c.subqueueLock.Unlock()

	status := make([]*taskqueuespb.InternalTaskQueueStatus, len(c.subqueues))
	backlogCountsBySubqueue := c.db.getApproximateBacklogCountsBySubqueue()
	for i, r := range c.subqueues {
		readLevel, ackLevel := r.getLevels()
		status[i] = &taskqueuespb.InternalTaskQueueStatus{
			ReadLevel: readLevel,
			AckLevel:  ackLevel,
			TaskIdBlock: &taskqueuepb.TaskIdBlock{
				StartId: currentTaskIDBlock.start,
				EndId:   currentTaskIDBlock.end,
			},
			LoadedTasks:             int64(r.getLoadedTasks()),
			MaxReadLevel:            c.db.GetMaxReadLevel(subqueueIndex(i)),
			ApproximateBacklogCount: backlogCountsBySubqueue[i],
		}
	}
	return status
}

func (c *priBacklogManagerImpl) respoolTaskAfterError(task *persistencespb.TaskInfo) error {
	// We cannot just remove it from persistence because then it will be lost.
	// We handle this by writing the task back to persistence with a higher taskID.
	// This will allow subsequent tasks to make progress, and hopefully by the time this task is picked-up
	// again the underlying reason for failing to start will be resolved.
	// Note the task may get written to a different subqueue than it came from.
	metrics.TaskRewrites.With(c.metricsHandler).Record(1)
	err := backoff.ThrottleRetryContext(c.tqCtx, func(context.Context) error {
		return c.SpoolTask(task)
	}, persistenceOperationRetryPolicy, common.IsPersistenceTransientError)
	if err == nil {
		return nil
	}

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
	return err
}

// TODO(pri): old matcher cleanup: move here
// func rangeIDToTaskIDBlock(rangeID int64, rangeSize int64) taskIDBlock {
// 	return taskIDBlock{
// 		start: (rangeID-1)*rangeSize + 1,
// 		end:   rangeID * rangeSize,
// 	}
// }

func (c *priBacklogManagerImpl) queueKey() *PhysicalTaskQueueKey {
	return c.pqMgr.QueueKey()
}

func (c *priBacklogManagerImpl) getDB() *taskQueueDB {
	return c.db
}

func (c *priBacklogManagerImpl) setPriority(task *internalTask) {
	c.config.setDefaultPriority(task)
	if c.isDraining {
		// draining goes before active backlog so we're guaranteed to finish migration
		task.effectivePriority -= maxPriorityLevels
	}
}
