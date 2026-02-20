package matching

import (
	"context"
	"errors"
	"slices"
	"sync"
	"time"

	"github.com/emirpasic/gods/maps/treemap"
	"github.com/tidwall/btree"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/common/softassert"
	"go.temporal.io/server/common/util"
	"golang.org/x/sync/semaphore"
)

type (
	fairTaskReader struct {
		backlogMgr *fairBacklogManagerImpl
		subqueue   subqueueIndex
		logger     log.Logger

		lock sync.Mutex

		readPending  bool
		backoffTimer *time.Timer
		retrier      backoff.Retrier
		addRetries   *semaphore.Weighted

		backlogAge       backlogAgeTracker
		outstandingTasks treemap.Map // fairLevel -> *internalTask if unacked, or nil if acked
		loadedTasks      int         // == number of unacked (non-nil) entries in outstandingTasks
		readLevel        fairLevel   // == highest level in outstandingTasks, or if empty, the level we should read next
		ackLevel         fairLevel   // inclusive: task exactly at ackLevel _has_ been acked
		atEnd            bool        // whether we believe outstandingTasks represents the entire queue right now

		// Small cache of acked task levels that were evicted from outstandingTasks. When tasks
		// are evicted from memory, we lose track of which ones were already acked. This cache
		// helps avoid reprocessing tasks that we know were already acked but whose ack was
		// evicted before it could be used to advance ackLevel.
		evictedAcks *btree.BTreeG[fairLevel]

		// Hold tasks written while a read is pending so we make sure to account for them in
		// our read level.
		newlyWrittenTasks []*persistencespb.AllocatedTaskInfo

		// Pin ack level while writing tasks so that we don't delete just-written tasks.
		// Also pin it while reading if we have newlyWrittenTasks, to handle the case of concurrent
		// reads and writes: if it's pinned by a write while a read is pending, we need to hold
		// it pinned until newlyWrittenTasks are processed.
		ackLevelPinnedByWriter bool

		// gc state
		inGC       bool
		numToGC    int       // counts approximately how many tasks we can delete with a GC
		lastGCTime time.Time // last time GCed
	}

	mergeMode int
)

const (
	mergeReadMiddle mergeMode = iota
	mergeReadToEnd
	mergeWrite
)

// Max number of evicted ack levels to cache. This is a small cache to avoid
// reprocessing tasks that were acked but whose acks were evicted before they
// could be used to advance ackLevel.
const evictedAcksCacheSize = 256

func newFairTaskReader(
	backlogMgr *fairBacklogManagerImpl,
	subqueue subqueueIndex,
	initialAckLevel fairLevel,
) *fairTaskReader {
	return &fairTaskReader{
		backlogMgr: backlogMgr,
		subqueue:   subqueue,
		logger:     backlogMgr.logger,
		retrier: backoff.NewRetrier(
			common.CreateReadTaskRetryPolicy(),
			clock.NewRealTimeSource(),
		),
		backlogAge: newBacklogAgeTracker(),
		addRetries: semaphore.NewWeighted(concurrentAddRetries),

		// ack manager
		outstandingTasks: *newFairLevelTreeMap(),
		readLevel:        initialAckLevel,
		ackLevel:         initialAckLevel,
		evictedAcks:      btree.NewBTreeG(fairLevel.less),

		// gc state
		lastGCTime: time.Now(),
	}
}

func (tr *fairTaskReader) Start() {
	tr.lock.Lock()
	defer tr.lock.Unlock()
	tr.maybeReadTasksLocked()
}

func (tr *fairTaskReader) getOldestBacklogTime() time.Time {
	tr.lock.Lock()
	defer tr.lock.Unlock()
	return tr.backlogAge.oldestTime()
}

func (tr *fairTaskReader) completeTask(task *internalTask, res taskResponse) {
	tr.lock.Lock()

	// We might have a race where mergeTasks tries to read a task from matcher (because new tasks
	// came in under it), but it had already been matched and removed. In that case the
	// removeFromMatcher will be a no-op, and we'll eventually end up here. We can tell because
	// the task won't be present in outstandingTasks.
	//
	// We can't ack the task, so we'll eventually read it again and then discover that it's a
	// duplicate when we try to RecordTaskStarted.
	if task, found := tr.outstandingTasks.Get(fairLevelFromAllocatedTask(task.event.AllocatedTaskInfo)); !found {
		metrics.TaskCompletedMissing.With(tr.backlogMgr.metricsHandler).Record(1)
		tr.lock.Unlock()
		return
	} else if _, ok := task.(*internalTask); !softassert.That(tr.logger, ok, "completed task was already acked") {
		tr.lock.Unlock()
		return
	}

	// Handle happy path first:
	err := res.err()
	if err == nil {
		tr.completeTaskLocked(task)
		tr.lock.Unlock()
		return
	}

	tr.lock.Unlock()

	// We can handle some transient errors by just putting the task back in the matcher to
	// match again. Note that for forwarded tasks, it's expected to get DeadlineExceeded when
	// the task doesn't match on the root after backlogTaskForwardTimeout, and also expected to
	// get errRemoteSyncMatchFailed, which is a serviceerror.Canceled error.
	if common.IsServiceClientTransientError(err) ||
		common.IsContextDeadlineExceededErr(err) ||
		common.IsContextCanceledErr(err) {
		// TODO(pri): if this was a start error (not a forwarding error): consider adding a
		// per-task backoff here, in case the error was workflow busy, we don't want to end up
		// trying the same task immediately. maybe also: after a few attempts on the same task,
		// let it get cycled to the end of the queue, in case there's some task/wf-specific
		// thing.
		tr.addTaskToMatcher(task)
		metrics.TaskRetryTransient.With(tr.backlogMgr.metricsHandler).Record(1)
		return
	}

	// On other errors: ask backlog manager to re-spool to persistence
	if tr.backlogMgr.respoolTaskAfterError(task.event.Data) != nil {
		return // task queue will unload now
	}

	// If we re-spooled successfully, remove the old version of the task.
	tr.lock.Lock()
	defer tr.lock.Unlock()
	tr.completeTaskLocked(task)
}

func (tr *fairTaskReader) completeTaskLocked(task *internalTask) {
	tr.backlogAge.record(task.event.Data.CreateTime, -1)
	tr.outstandingTasks.Put(fairLevelFromAllocatedTask(task.event.AllocatedTaskInfo), nil)
	tr.loadedTasks--
	softassert.That(tr.logger, tr.loadedTasks >= 0, "loadedTasks went negative")

	tr.advanceAckLevelLocked()
	tr.maybeReadTasksLocked()
}

func (tr *fairTaskReader) maybeReadTasksLocked() {
	// If readPending is true, readTasksImpl is running and will check shouldReadMoreLocked
	// before it exits, so we'll definitely do another read if shouldReadMoreLocked is true.
	// We also abort here if we're in the middle of a backoff or shutting down.
	if tr.readPending || !tr.shouldReadMoreLocked() ||
		tr.backoffTimer != nil || tr.backlogMgr.tqCtx.Err() != nil {
		return
	}
	tr.readPending = true
	go tr.readTasksImpl()
}

func (tr *fairTaskReader) shouldReadMoreLocked() bool {
	if tr.atEnd {
		// If we have the whole backlog in memory, we don't need to read anything.
		return false
	} else if tr.loadedTasks > tr.backlogMgr.config.GetTasksReloadAt() {
		// Too many loaded already. We'll get called again when loadedTasks drops.
		return false
	}
	return true
}

func (tr *fairTaskReader) readTasksImpl() {
	var lastErr error
	for {
		tr.lock.Lock()
		if lastErr != nil || !tr.shouldReadMoreLocked() {
			break // with lock still held
		}
		readLevel, loadedTasks := tr.readLevel, tr.loadedTasks
		tr.lock.Unlock()

		lastErr = tr.readTaskBatch(readLevel, loadedTasks)
	}

	// note tr.lock is still held here!
	tr.readPending = false

	// process any tasks that were written while readPending was true
	var newTasks []*internalTask
	if len(tr.newlyWrittenTasks) != 0 {
		newTasks = tr.mergeTasksLocked(tr.newlyWrittenTasks, mergeWrite)
		clear(tr.newlyWrittenTasks)
		tr.newlyWrittenTasks = tr.newlyWrittenTasks[:0]

		// ack level would have been pinned here, we may be able to advance it now (if it's not
		// explicitly pinned by another write)
		tr.advanceAckLevelLocked()
	}

	// unlock before calling addTaskToMatcher
	tr.lock.Unlock()

	for _, task := range newTasks {
		tr.addTaskToMatcher(task)
	}
}

func (tr *fairTaskReader) readTaskBatch(readLevel fairLevel, loadedTasks int) error {
	batchSize := tr.backlogMgr.config.GetTasksBatchSize() - loadedTasks
	readFrom := readLevel.max(fairLevel{pass: 1, id: 0}).inc()
	res, err := tr.backlogMgr.db.GetFairTasks(tr.backlogMgr.tqCtx, tr.subqueue, readFrom, batchSize)
	if err != nil {
		// TODO: Should we ever stop retrying on db errors?
		if tr.backlogMgr.signalIfFatal(err) || common.IsContextCanceledErr(err) {
			// don't retry
		} else if common.IsResourceExhausted(err) {
			tr.retryReadAfter(taskReaderThrottleRetryDelay)
		} else {
			tr.retryReadAfter(tr.retrier.NextBackOff(err))
		}
		return err
	}
	tr.retrier.Reset()

	// If we got less than we asked for, we know we hit the end.
	// If there was a concurrent write such that we incorrectly think we hit the end here,
	// it will be held and processed after we're done reading, and maybe reset atEnd then.
	mode := mergeReadMiddle
	if len(res.Tasks) < batchSize {
		mode = mergeReadToEnd
	}

	// filter out expired
	// TODO(fairness): if we have _only_ expired tasks, and we filter them out here, we won't move
	// the ack level and delete them. maybe we should put them in outstandingTasks as pre-acked.
	tasks := slices.DeleteFunc(res.Tasks, func(t *persistencespb.AllocatedTaskInfo) bool {
		if IsTaskExpired(t) {
			metrics.ExpiredTasksPerTaskQueueCounter.With(tr.backlogMgr.metricsHandler).Record(1, metrics.TaskExpireStageReadTag)
			return true
		}
		return false
	})

	// Note: even if (especially if) len(tasks) == 0, we should go through the mergeTasks logic
	// to update atEnd and the backlog size estimate.
	tr.mergeTasks(tasks, mode)

	return nil
}

// call with_out_ lock held
func (tr *fairTaskReader) addTaskToMatcher(task *internalTask) {
	task.resetMatcherState()
	err := tr.backlogMgr.addSpooledTask(task)
	if err == nil {
		return
	}

	if drop, retry := tr.addErrorBehavior(err); drop {
		task.finish(nil, false)
	} else if retry {
		// This should only be due to persistence problems. Retry in a new goroutine
		// to not block other tasks, up to some concurrency limit.
		if tr.addRetries.Acquire(tr.backlogMgr.tqCtx, 1) != nil {
			return
		}
		go tr.retryAddAfterError(task)
	}
}

func (tr *fairTaskReader) addErrorBehavior(err error) (drop, retry bool) {
	// addSpooledTask can only fail due to:
	// - the task queue is closed (errTaskQueueClosed or context.Canceled)
	// - ValidateDeployment failed (InvalidArgument)
	// - versioning wants to get a versioned queue and it can't be initialized
	// - versioning wants to re-spool the task on a different queue and that failed
	// - versioning says StickyWorkerUnavailable
	if errors.Is(err, errTaskQueueClosed) || common.IsContextCanceledErr(err) {
		// maybe we tried to add a task to a versioned queue as it was unloading, and have to
		// retry here. if tqCtx is closing, addTaskToMatcher will give up.
		return false, true
	}
	var stickyUnavailable *serviceerrors.StickyWorkerUnavailable
	if errors.As(err, &stickyUnavailable) {
		return true, false // drop the task
	}
	var invalid *serviceerror.InvalidArgument
	var internal *serviceerror.Internal
	if errors.As(err, &invalid) || errors.As(err, &internal) {
		tr.backlogMgr.throttledLogger.Error("nonretryable error processing spooled task", tag.Error(err))
		return true, false // drop the task
	}
	// For any other error (this should be very rare), we can retry.
	tr.backlogMgr.throttledLogger.Error("retryable error processing spooled task", tag.Error(err))
	return false, true
}

func (tr *fairTaskReader) retryAddAfterError(task *internalTask) {
	defer tr.addRetries.Release(1)
	metrics.BufferThrottlePerTaskQueueCounter.With(tr.backlogMgr.metricsHandler).Record(1)

	// initial sleep since we just tried once
	_ = util.InterruptibleSleep(tr.backlogMgr.tqCtx, time.Second)

	_ = backoff.ThrottleRetryContext(
		tr.backlogMgr.tqCtx,
		func(context.Context) error {
			if IsTaskExpired(task.event.AllocatedTaskInfo) {
				task.finish(nil, false)
				return nil
			}
			err := tr.backlogMgr.addSpooledTask(task)
			if drop, retry := tr.addErrorBehavior(err); drop {
				task.finish(nil, false)
			} else if retry {
				metrics.BufferThrottlePerTaskQueueCounter.With(tr.backlogMgr.metricsHandler).Record(1)
				return err
			}
			return nil
		},
		addErrorRetryPolicy,
		nil,
	)
}

func (tr *fairTaskReader) wroteNewTasks(tasks []*persistencespb.AllocatedTaskInfo) {
	tr.mergeTasks(tasks, mergeWrite)
}

func (tr *fairTaskReader) mergeTasks(tasks []*persistencespb.AllocatedTaskInfo, mode mergeMode) {
	tr.lock.Lock()

	if mode == mergeWrite && tr.readPending {
		// concurrent write + read: hold the just-written tasks and merge them after we process
		// the read.
		tr.newlyWrittenTasks = append(tr.newlyWrittenTasks, tasks...)
		tr.lock.Unlock()
		return
	}

	newTasks := tr.mergeTasksLocked(tasks, mode)

	// unlock before calling addTaskToMatcher
	tr.lock.Unlock()

	for _, task := range newTasks {
		tr.addTaskToMatcher(task)
	}
}

// nolint:revive,cognitive-complexity // will be simplified in the future
func (tr *fairTaskReader) mergeTasksLocked(tasks []*persistencespb.AllocatedTaskInfo, mode mergeMode) []*internalTask {
	// Collect (1) currently loaded tasks in the matcher plus (2) the tasks we just read/wrote; sorted by level.

	// (1) Note these values are *internalTask.
	merged := tr.outstandingTasks.Select(func(k, v any) bool {
		_, ok := v.(*internalTask)
		return ok
	})
	// (2) Note these values are *AllocatedTaskInfo.
	for _, t := range tasks {
		level := fairLevelFromAllocatedTask(t)
		if !tr.ackLevel.less(level) {
			// Reads may race with completes/acks such that we read some tasks that are already
			// acked. We should ignore these.
			continue
		} else if mode == mergeWrite && !tr.atEnd && tr.readLevel.less(level) {
			// If we're writing and we're not at the end, then we have to ignore tasks
			// above readLevel since we don't know what's in between readLevel and there.
			continue
		} else if _, have := tr.outstandingTasks.Get(level); have {
			// If write/read race or we have to re-read a range, we may read something we had
			// already added to the matcher or acked. Ignore tasks we already have.
			continue
		} else if _, have := tr.evictedAcks.Delete(level); have {
			// This task was already acked but the ack was evicted. Skip it.
			continue
		}
		merged.Put(level, t)
	}

	// Take as many of those as we want to keep in memory. The ones that are not already in the
	// matcher, we have to add to the matcher.
	batchSize := tr.backlogMgr.config.GetTasksBatchSize()
	it := merged.Iterator()
	var highestLevel fairLevel
	tasks = tasks[:0] // reuse incoming slice to avoid an allocation
	for b := 0; b < batchSize && it.Next(); b++ {
		if t, ok := it.Value().(*persistencespb.AllocatedTaskInfo); ok {
			// new task we need to add to the matcher
			tasks = append(tasks, t)
		}
		highestLevel = it.Key().(fairLevel) // nolint:revive
	}

	if highestLevel.id != 0 {
		// If we have any tasks at all in memory, set readLevel to the maximum of that set.
		tr.readLevel = highestLevel
	} else {
		// Otherwise start reading at ack level next.
		tr.readLevel = tr.ackLevel
	}

	// If there are remaining tasks in the merged set, they can't fit in memory. If they came
	// from the tasks we just wrote, ignore them. If they came from matcher, remove them.
	evictedAnyTasks := false
	for it.Next() {
		evictedAnyTasks = true
		if task, ok := it.Value().(*internalTask); ok {
			// task that was in the matcher that we have to remove
			tr.backlogAge.record(task.event.Data.CreateTime, -1)
			tr.loadedTasks--
			softassert.That(tr.logger, tr.loadedTasks >= 0, "loadedTasks went negative")
			tr.outstandingTasks.Remove(it.Key().(fairLevel))

			// Note that the task may have already been matched and removed from the matcher,
			// but not completed yet. In that case this will be a noop. See comment at the top
			// of completeTask. Lock order: task reader lock < matcher lock so this is okay.
			task.setEvicted()
		}
	}

	// We also have to remove any acked levels (nils) in outstandingTasks that are above our
	// new read level (and accept reprocessing those tasks when we see them again), otherwise
	// we may use these acks to increment our ack level across dropped ranges of tasks.
	// Cache these evicted acks so we can skip them if we re-read them later.
	tr.outstandingTasks.Select(func(k, v any) bool {
		return v == nil && tr.readLevel.less(k.(fairLevel))
	}).Each(func(k, v any) {
		evictedAnyTasks = true
		level := k.(fairLevel) //nolint:revive
		tr.outstandingTasks.Remove(level)
		tr.evictedAcks.Set(level)
	})
	// Trim the cache to max size by removing highest levels.
	for tr.evictedAcks.Len() > evictedAcksCacheSize {
		tr.evictedAcks.PopMax()
	}

	internalTasks := make([]*internalTask, len(tasks))
	for i, t := range tasks {
		level := fairLevelFromAllocatedTask(t)
		internalTasks[i] = newInternalTaskFromBacklog(t, tr.completeTask)
		tr.backlogMgr.setPriority(internalTasks[i])
		// After we get to this point, we must eventually call task.finish or
		// task.finishForwarded, which will call tr.completeTask.
		tr.outstandingTasks.Put(level, internalTasks[i])
		tr.loadedTasks++
		tr.backlogAge.record(t.Data.CreateTime, 1)
	}

	// Update atEnd:
	// If we did a read and didn't get to the end, we can't possibly be at the end.
	// Also if we evicted anything from memory, we can't either.
	// If we read to the end and didn't evict anything, then we know we're at the end.
	// Otherwise (i.e. on write) leave atEnd unchanged.
	if mode == mergeReadMiddle || evictedAnyTasks {
		tr.atEnd = false
	} else if mode == mergeReadToEnd {
		tr.atEnd = true
	}

	// If we're at the end, then outstandingTasks is the whole queue so we can set count.
	if count := tr.knownCountLocked(); count >= 0 {
		tr.backlogMgr.db.setKnownFairBacklogCount(tr.subqueue, count)
	}

	return internalTasks

	// TODO: fine-grained metrics for mergeTasks behavior:
	// we have two sources: currently loaded, and newly read/written.
	// we have two destinations: loaded and evicted. we could count these four values:
	// loaded->loaded, loaded->evicted, new->loaded, new->evicted
	// let's say that's one metric with two labels of two values each.
	// add another label for whether we're doing this on read or write.
	// maybe do this as a wide event? we can also throw in loadedTasks then.
}

func (tr *fairTaskReader) retryReadAfter(duration time.Duration) {
	tr.lock.Lock()
	defer tr.lock.Unlock()

	if tr.backoffTimer == nil {
		tr.backoffTimer = time.AfterFunc(duration, func() {
			tr.lock.Lock()
			defer tr.lock.Unlock()
			tr.backoffTimer = nil
			tr.maybeReadTasksLocked()
		})
	}
}

// ack manager

func (tr *fairTaskReader) getLoadedTasks() int {
	tr.lock.Lock()
	defer tr.lock.Unlock()
	return tr.loadedTasks
}

// isDrained returns true if this subqueue has been fully drained:
// - We've read to the end of the queue (atEnd is true)
// - No tasks are loaded in memory
func (tr *fairTaskReader) isDrained() bool {
	tr.lock.Lock()
	defer tr.lock.Unlock()
	return tr.atEnd && tr.loadedTasks == 0
}

func (tr *fairTaskReader) ackLevelPinnedLocked() bool {
	return tr.ackLevelPinnedByWriter || len(tr.newlyWrittenTasks) > 0
}

// call this whenever new tasks are acked or when ackLevelPinnedLocked() may turn from true to
// false (i.e. when ackLevelPinnedByWriter is set to false or newlyWrittenTasks is cleared).
func (tr *fairTaskReader) advanceAckLevelLocked() {
	if tr.ackLevelPinnedLocked() {
		return
	}

	// Adjust the ack level as far as we can
	var numAcked int64
	for {
		minLevel, v := tr.outstandingTasks.Min()
		if minLevel == nil {
			break
		} else if _, ok := v.(*internalTask); ok {
			break
		}
		tr.ackLevel = minLevel.(fairLevel) // nolint:revive
		tr.outstandingTasks.Remove(minLevel)
		numAcked += 1
	}

	if numAcked > 0 {
		tr.numToGC += int(numAcked)
		tr.maybeGCLocked()

		tr.backlogMgr.db.updateFairAckLevel(
			tr.subqueue, tr.ackLevel, -numAcked, tr.knownCountLocked(), tr.backlogAge.oldestTime())
	}
}

func (tr *fairTaskReader) getAndPinAckLevel() fairLevel {
	tr.lock.Lock()
	defer tr.lock.Unlock()

	softassert.That(tr.logger, !tr.ackLevelPinnedByWriter, "ack level already pinned")
	tr.ackLevelPinnedByWriter = true
	return tr.ackLevel
}

func (tr *fairTaskReader) unpinAckLevel(writeErr error) {
	tr.lock.Lock()
	defer tr.lock.Unlock()

	if writeErr != nil {
		// We got an error writing but the write may have succeeded anyway.
		// We can't assume we know where the end is anymore.
		tr.atEnd = false
		// Initiate a read to try to find the end again.
		tr.maybeReadTasksLocked()
	}

	softassert.That(tr.logger, tr.ackLevelPinnedByWriter, "ack level wasn't pinned")
	tr.ackLevelPinnedByWriter = false
	tr.advanceAckLevelLocked()
}

func (tr *fairTaskReader) getLevels() (readLevel, ackLevel fairLevel) {
	tr.lock.Lock()
	defer tr.lock.Unlock()
	return tr.readLevel, tr.ackLevel
}

func (tr *fairTaskReader) knownCountLocked() int64 {
	if tr.atEnd {
		return int64(tr.loadedTasks)
	}
	return -1
}

// gc

func (tr *fairTaskReader) maybeGCLocked() {
	if !tr.shouldGCLocked() {
		return
	}
	tr.inGC = true
	tr.lastGCTime = time.Now()
	// gc in new goroutine so poller doesn't have to wait
	go tr.doGC(tr.ackLevel)
}

func (tr *fairTaskReader) shouldGCLocked() bool {
	if tr.inGC || tr.numToGC == 0 {
		return false
	}
	return tr.numToGC >= tr.backlogMgr.config.MaxTaskDeleteBatchSize() ||
		time.Since(tr.lastGCTime) > tr.backlogMgr.config.TaskDeleteInterval()
}

// called in new goroutine
func (tr *fairTaskReader) doGC(ackLevel fairLevel) {
	rowsDeleted, err := tr.doGCAt(ackLevel)

	tr.lock.Lock()
	defer tr.lock.Unlock()

	tr.inGC = false
	if err != nil {
		return
	}
	// implementation behavior for CompleteTasksLessThan:
	// - unit test, cassandra: always return UnknownNumRowsAffected (in this case means "all")
	// - sql: return number of rows affected (should be <= batchSize)
	if rowsDeleted == persistence.UnknownNumRowsAffected {
		tr.numToGC = 0
	} else {
		tr.numToGC = max(0, tr.numToGC-rowsDeleted)
	}
}

func (tr *fairTaskReader) doGCAt(ackLevel fairLevel) (int, error) {
	batchSize := tr.backlogMgr.config.MaxTaskDeleteBatchSize()

	ctx, cancel := context.WithTimeout(tr.backlogMgr.tqCtx, ioTimeout)
	defer cancel()

	n, err := tr.backlogMgr.db.CompleteFairTasksLessThan(ctx, ackLevel.inc(), batchSize, tr.subqueue)
	if err != nil {
		tr.logger.Warn("failed to gc tasks", tag.Error(err))
	}
	return n, err
}

// finalGC does a single synchronous gc.
// Used when unloading a draining queue that won't be reloaded.
func (tr *fairTaskReader) finalGC() {
	tr.lock.Lock()
	ackLevel := tr.ackLevel
	tr.lock.Unlock()
	if ackLevel.pass == 0 {
		return
	}
	_, _ = tr.doGCAt(ackLevel)
}
