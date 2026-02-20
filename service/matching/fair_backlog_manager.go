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
	"go.temporal.io/server/common/softassert"
	"go.temporal.io/server/service/matching/counter"
	"google.golang.org/protobuf/types/known/durationpb"
)

type (
	fairBacklogManagerImpl struct {
		pqMgr      physicalTaskQueueManager
		config     *taskQueueConfig
		tqCtx      context.Context
		isDraining bool
		db         *taskQueueDB
		taskWriter *fairTaskWriter

		subqueueLock        sync.Mutex
		subqueues           []*fairTaskReader // subqueue index -> fairTaskReader
		subqueuesByPriority map[priorityKey]subqueueIndex
		priorityBySubqueue  map[subqueueIndex]priorityKey

		logger          log.Logger
		throttledLogger log.ThrottledLogger
		matchingClient  matchingservice.MatchingServiceClient
		metricsHandler  metrics.Handler
		counterFactory  func() counter.Counter

		initializedError *future.FutureImpl[struct{}]
		// skipFinalUpdate controls behavior on Stop: if it's false, we try to write one final
		// update before unloading
		skipFinalUpdate atomic.Bool
	}
)

var _ backlogManager = (*fairBacklogManagerImpl)(nil)

func newFairBacklogManager(
	tqCtx context.Context,
	pqMgr physicalTaskQueueManager,
	config *taskQueueConfig,
	fairTaskManager persistence.FairTaskManager,
	logger log.Logger,
	throttledLogger log.ThrottledLogger,
	matchingClient matchingservice.MatchingServiceClient,
	metricsHandler metrics.Handler,
	counterFactory func() counter.Counter,
	isDraining bool,
) *fairBacklogManagerImpl {
	// For the purposes of taskQueueDB, call this just a TaskManager. It'll return errors if we
	// use it incorectly. TODO(fairness): consider a cleaner way of doing this.
	taskManager := persistence.TaskManager(fairTaskManager)

	bmg := &fairBacklogManagerImpl{
		pqMgr:               pqMgr,
		config:              config,
		tqCtx:               tqCtx,
		isDraining:          isDraining,
		db:                  newTaskQueueDB(config, taskManager, pqMgr.QueueKey(), logger, metricsHandler, isDraining),
		subqueuesByPriority: make(map[priorityKey]subqueueIndex),
		priorityBySubqueue:  make(map[subqueueIndex]priorityKey),
		matchingClient:      matchingClient,
		metricsHandler:      metricsHandler,
		counterFactory:      counterFactory,
		logger:              logger,
		throttledLogger:     throttledLogger,
		initializedError:    future.NewFuture[struct{}](),
	}
	bmg.taskWriter = newFairTaskWriter(bmg, bmg.newCounterForSubqueue)
	return bmg
}

// signalIfFatal calls UnloadFromPartitionManager of the physicalTaskQueueManager
// if and only if the supplied error represents a fatal condition, e.g. the existence
// of a newer lease by another backlogManager. Returns true if the unload signal
// is emitted, false otherwise.
func (c *fairBacklogManagerImpl) signalIfFatal(err error) bool {
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

func (c *fairBacklogManagerImpl) Start() {
	c.taskWriter.Start()
}

func (c *fairBacklogManagerImpl) Stop() {
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
		c.db.updateFairAckLevel(subqueueIndex(i), ackLevel, 0, -1, time.Time{})
	}
	c.subqueueLock.Unlock()

	ctx, cancel := context.WithTimeout(c.tqCtx, ioTimeout)
	_ = c.db.SyncState(ctx)
	cancel()
}

func (c *fairBacklogManagerImpl) initState(state taskQueueState, err error) {
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

func (c *fairBacklogManagerImpl) WaitUntilInitialized(ctx context.Context) error {
	_, err := c.initializedError.Get(ctx)
	return err
}

func (c *fairBacklogManagerImpl) loadSubqueuesLocked(subqueues []persistencespb.SubqueueInfo) {
	// TODO(pri): This assumes that subqueues never shrinks, and priority/fairness index of
	// existing subqueues never changes. If we change that, this logic will need to change.
	for i := range subqueues {
		subqueueIdx := subqueueIndex(i)
		if i >= len(c.subqueues) {
			r := newFairTaskReader(c, subqueueIdx, fairLevelFromProto(subqueues[i].FairAckLevel))
			r.Start()
			c.subqueues = append(c.subqueues, r)
		}
		c.subqueuesByPriority[priorityKey(subqueues[i].Key.Priority)] = subqueueIdx
		c.priorityBySubqueue[subqueueIdx] = priorityKey(subqueues[i].Key.Priority)
	}
}

func (c *fairBacklogManagerImpl) getSubqueueForPriority(priority priorityKey) subqueueIndex {
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

func (c *fairBacklogManagerImpl) periodicSync() {
	for {
		select {
		case <-c.tqCtx.Done():
			return
		case <-time.After(c.config.UpdateAckInterval()):
			ctx, cancel := context.WithTimeout(c.tqCtx, ioTimeout)
			err := c.db.SyncState(ctx)
			cancel()
			c.signalIfFatal(err)

			if c.hasFinishedDraining() {
				c.pqMgr.FinishedDraining()
				return
			}
		}
	}
}

func (c *fairBacklogManagerImpl) SpoolTask(taskInfo *persistencespb.TaskInfo) error {
	subqueue := c.getSubqueueForPriority(priorityKey(taskInfo.Priority.GetPriorityKey()))
	err := c.taskWriter.appendTask(subqueue, taskInfo)
	c.signalIfFatal(err)
	return err
}

func (c *fairBacklogManagerImpl) getAndPinAckLevels() ([]fairLevel, func(error)) {
	c.subqueueLock.Lock()
	subqueues := slices.Clone(c.subqueues)
	c.subqueueLock.Unlock()

	levels := make([]fairLevel, len(subqueues))
	for i, s := range subqueues {
		levels[i] = s.getAndPinAckLevel()
	}
	unpin := func(writeErr error) {
		for _, s := range subqueues {
			s.unpinAckLevel(writeErr)
		}
	}
	return levels, unpin
}

func (c *fairBacklogManagerImpl) wroteNewTasks(resp createFairTasksResponse) {
	c.subqueueLock.Lock()
	subqueues := slices.Clone(c.subqueues)
	c.subqueueLock.Unlock()

	for subqueue, subqueueResp := range resp {
		subqueues[subqueue].wroteNewTasks(subqueueResp)
	}
}

func (c *fairBacklogManagerImpl) addSpooledTask(task *internalTask) error {
	return c.pqMgr.AddSpooledTask(task)
}

func (c *fairBacklogManagerImpl) BacklogCountHint() (total int64) {
	c.subqueueLock.Lock()
	defer c.subqueueLock.Unlock()
	for _, r := range c.subqueues {
		total += int64(r.getLoadedTasks())
	}
	return
}

func (c *fairBacklogManagerImpl) BacklogStatsByPriority() map[int32]*taskqueuepb.TaskQueueStats {
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

func (c *fairBacklogManagerImpl) BacklogStatus() *taskqueuepb.TaskQueueStatus {
	c.subqueueLock.Lock()
	defer c.subqueueLock.Unlock()

	// TODO(pri): needs more work for subqueues, for now just return read/ack level for subqueue 0
	// TODO(fair): needs even more work for fairness
	var readLevel, ackLevel fairLevel
	if len(c.subqueues) > 0 {
		readLevel, ackLevel = c.subqueues[subqueueZero].getLevels()
	}

	taskIDBlock := rangeIDToTaskIDBlock(c.db.RangeID(), c.config.RangeSize)
	return &taskqueuepb.TaskQueueStatus{
		ReadLevel: readLevel.id,
		AckLevel:  ackLevel.id,
		// use getTotalApproximateBacklogCount instead of BacklogCountHint since it's more accurate
		BacklogCountHint: c.db.getTotalApproximateBacklogCount(),
		TaskIdBlock: &taskqueuepb.TaskIdBlock{
			StartId: taskIDBlock.start,
			EndId:   taskIDBlock.end,
		},
	}
}

func (c *fairBacklogManagerImpl) InternalStatus() []*taskqueuespb.InternalTaskQueueStatus {
	currentTaskIDBlock := c.taskWriter.getCurrentTaskIDBlock()

	c.subqueueLock.Lock()
	defer c.subqueueLock.Unlock()

	status := make([]*taskqueuespb.InternalTaskQueueStatus, len(c.subqueues))
	for i, r := range c.subqueues {
		readLevel, ackLevel := r.getLevels()
		count, maxReadLevel := c.db.getApproximateBacklogCountAndMaxReadLevel(subqueueIndex(i))
		status[i] = &taskqueuespb.InternalTaskQueueStatus{
			FairReadLevel: readLevel.toProto(),
			FairAckLevel:  ackLevel.toProto(),
			TaskIdBlock: &taskqueuepb.TaskIdBlock{
				StartId: currentTaskIDBlock.start,
				EndId:   currentTaskIDBlock.end,
			},
			LoadedTasks:             int64(r.getLoadedTasks()),
			FairMaxReadLevel:        maxReadLevel.toProto(),
			ApproximateBacklogCount: count,
		}
	}
	return status
}

func (c *fairBacklogManagerImpl) respoolTaskAfterError(task *persistencespb.TaskInfo) error {
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

func (c *fairBacklogManagerImpl) queueKey() *PhysicalTaskQueueKey {
	return c.pqMgr.QueueKey()
}

func (c *fairBacklogManagerImpl) getDB() *taskQueueDB {
	return c.db
}

func (c *fairBacklogManagerImpl) newCounterForSubqueue(subqueue subqueueIndex) counter.Counter {
	cntr := c.counterFactory()
	// restore persisted keys
	for _, entry := range c.db.getTopKFairnessKeys(subqueue) {
		_ = cntr.GetPass(entry.Key, entry.Count, 0)
	}
	return cntr
}

// hasFinishedDraining returns true if this is a draining backlog manager and all tasks have
// been fully drained (read and acked).
func (c *fairBacklogManagerImpl) hasFinishedDraining() bool {
	if !c.isDraining {
		return false
	}

	c.subqueueLock.Lock()
	defer c.subqueueLock.Unlock()

	for _, r := range c.subqueues {
		if !r.isDrained() {
			return false
		}
	}
	return true
}

// FinalGC does a final gc pass on all subqueues.
func (c *fairBacklogManagerImpl) FinalGC() {
	if !softassert.That(c.logger, c.isDraining, "FinalGC called on non-draining backlog manager") {
		return
	}
	c.subqueueLock.Lock()
	subqueues := slices.Clone(c.subqueues)
	c.subqueueLock.Unlock()

	for _, r := range subqueues {
		r.finalGC()
	}
}

func (c *fairBacklogManagerImpl) setPriority(task *internalTask) {
	c.config.setDefaultPriority(task)
	if c.isDraining {
		// draining goes before active backlog so we're guaranteed to finish migration
		task.effectivePriority -= effectivePriorityFactor * maxPriorityLevels
	}
}
