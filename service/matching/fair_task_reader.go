package matching

import (
	"context"
	"errors"
	"slices"
	"sync"
	"time"

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
		backlogMgr      *fairBacklogManagerImpl
		subqueue        subqueueIndex
		logger          log.Logger
		throttledLogger log.ThrottledLogger

		lock sync.Mutex

		readPending     bool
		backoffTimer    *time.Timer
		retrier         backoff.Retrier
		throttleRetrier backoff.Retrier
		addRetries      *semaphore.Weighted

		backlogAge       backlogAgeTracker
		outstandingTasks *btree.BTreeG[outstandingTask] // level-ordered outstanding tasks and acks
		loadedTasks      int                            // == number of loaded (unacked) entries in outstandingTasks
		readLevel        fairLevel                      // == highest level in outstandingTasks, or if empty, the level we should read next
		ackLevel         fairLevel                      // inclusive: task exactly at ackLevel _has_ been acked
		atEnd            bool                           // whether we believe outstandingTasks represents the entire queue right now

		// Small cache of acked task levels that were evicted from outstandingTasks. When tasks
		// are evicted from memory, we lose track of which ones were already acked. This cache
		// helps avoid reprocessing tasks that we know were already acked but whose ack was
		// evicted before it could be used to advance ackLevel.
		evictedAcks btree.BTreeG[fairLevel]

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

	// outstandingTask is one level-keyed entry in fairTaskReader.outstandingTasks. Each entry is
	// either loaded (task != nil, the task is in the matcher awaiting dispatch) or acked
	// (task == nil, the level is completed/expired but not yet advanced past).
	outstandingTask struct {
		level fairLevel
		task  *internalTask
	}
)

// outstandingTaskLess orders entries by level. Levels are unique per task, so no tiebreaker is
// needed and Set at an existing level replaces (rather than duplicates) the entry there.
func outstandingTaskLess(a, b outstandingTask) bool {
	return a.level.less(b.level)
}

// acked reports whether this entry is an ack (completed or pre-acked) rather than a task that
// occupies a memory slot. Acked entries don't count toward the loaded-task limit.
func (o outstandingTask) acked() bool {
	return o.task == nil
}

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
	subqueueTag := tag.Int("subqueue-id", int(subqueue))
	return &fairTaskReader{
		backlogMgr:      backlogMgr,
		subqueue:        subqueue,
		logger:          log.With(backlogMgr.logger, subqueueTag),
		throttledLogger: log.With(backlogMgr.throttledLogger, subqueueTag),
		retrier: backoff.NewRetrier(
			backoff.NewExponentialRetryPolicy(50*time.Millisecond).
				WithMaximumInterval(10*time.Second).
				WithExpirationInterval(backoff.NoInterval),
			clock.NewRealTimeSource(),
		),
		throttleRetrier: backoff.NewRetrier(
			backoff.NewExponentialRetryPolicy(2*time.Second).
				WithMaximumInterval(30*time.Second).
				WithExpirationInterval(backoff.NoInterval),
			clock.NewRealTimeSource(),
		),
		backlogAge: newBacklogAgeTracker(),
		addRetries: semaphore.NewWeighted(concurrentAddRetries),

		// ack manager
		outstandingTasks: btree.NewBTreeGOptions(outstandingTaskLess, btree.Options{NoLocks: true}),
		readLevel:        initialAckLevel,
		ackLevel:         initialAckLevel,
		evictedAcks:      *btree.NewBTreeGOptions(fairLevel.less, btree.Options{NoLocks: true}),

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
	recordDroppedTask(tr.backlogMgr.metricsHandler, res.dropReason)

	tr.lock.Lock()

	// We might have a race where mergeTasks tries to read a task from matcher (because new tasks
	// came in under it), but it had already been matched and removed. In that case the
	// removeFromMatcher will be a no-op, and we'll eventually end up here. We can tell because
	// the task won't be present in outstandingTasks.
	//
	// We can't ack the task, so we'll eventually read it again and then discover that it's a
	// duplicate when we try to RecordTaskStarted.
	if entry, found := tr.outstandingTasks.Get(outstandingTask{level: task.fairLevel()}); !found {
		metrics.TaskCompletedMissing.With(tr.backlogMgr.metricsHandler).Record(1)
		tr.lock.Unlock()
		return
	} else if !softassert.That(tr.logger, entry.task != nil, "completed task was already acked") {
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
	tr.outstandingTasks.Set(outstandingTask{level: task.fairLevel()}) // replace loaded task with an ack
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

	// If a backoff timer fired while readPending was still true, its maybeReadTasksLocked call
	// was a no-op. Re-check now that readPending is false to avoid getting stuck.
	tr.maybeReadTasksLocked()

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
			tr.retryReadAfter(tr.throttleRetrier.NextBackOff(err))
		} else {
			tr.retryReadAfter(tr.retrier.NextBackOff(err))
		}
		return err
	}
	tr.retrier.Reset()
	tr.throttleRetrier.Reset()

	// If we got less than we asked for, we know we hit the end.
	// If there was a concurrent write such that we incorrectly think we hit the end here,
	// it will be held and processed after we're done reading, and maybe reset atEnd then.
	mode := mergeReadMiddle
	if len(res.Tasks) < batchSize {
		mode = mergeReadToEnd
	}

	// Note: even if (especially if) len(tasks) == 0, we should go through the mergeTasks logic
	// to update atEnd and the backlog size estimate. Expired tasks are passed through to
	// mergeTasksLocked where they'll be added as pre-acked (nil) entries so they advance the
	// ack level and get GC'd.
	tr.mergeTasks(res.Tasks, mode)

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
		task.finish(taskFinishResult{})
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
		tr.throttledLogger.Error("nonretryable error processing spooled task", tag.Error(err))
		return true, false // drop the task
	}
	// For any other error (this should be very rare), we can retry.
	tr.throttledLogger.Error("retryable error processing spooled task", tag.Error(err))
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
				task.finish(taskFinishResult{})
				return nil
			}
			err := tr.backlogMgr.addSpooledTask(task)
			if drop, retry := tr.addErrorBehavior(err); drop {
				task.finish(taskFinishResult{})
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

	// This specific state shouldn't ever happen and indicates a bug: if we're not at the end,
	// we should either have loaded tasks, or have a read pending to re-establish the end. If
	// we did get into this state without triggering a read, then newly-written tasks would all
	// be dropped due to being after read level, and there'd be no tasks to complete and
	// trigger a read. So we should do it here.
	//
	// The bug that led to this is fixed, but we'll leave this check defensively in case other
	// bugs produce the same state.
	if mode == mergeWrite && !tr.atEnd && tr.loadedTasks == 0 && !tr.readPending && tr.backoffTimer == nil {
		softassert.Fail(tr.logger, "fair reader stuck")
		metrics.FairReaderStuckDetected.With(tr.backlogMgr.metricsHandler).Record(1)
		tr.maybeReadTasksLocked()
	}

	// unlock before calling addTaskToMatcher
	tr.lock.Unlock()

	for _, task := range newTasks {
		tr.addTaskToMatcher(task)
	}
}

// nolint:revive,cognitive-complexity // merge is an inherently multi-phase operation
func (tr *fairTaskReader) mergeTasksLocked(tasks []*persistencespb.AllocatedTaskInfo, mode mergeMode) []*internalTask {
	batchSize := tr.backlogMgr.config.GetTasksBatchSize()

	// Work on a copy-on-write snapshot of the outstanding tasks: merge the newly read/written
	// tasks into it, trim it back to batchSize loaded tasks, and install the result as the new set
	// of outstanding tasks. The btree does the trimming for us.
	merged := tr.outstandingTasks.Copy()

	// The merged operations below all touch nearby levels, so share one path hint across the Get,
	// Set, and Delete point operations on merged.
	var hint btree.PathHint

	// (1) Merge the newly read/written tasks into the snapshot. Already-acked tasks (expired, or
	// re-read after their ack was evicted) go in as acks; the rest become loaded tasks. We create
	// the internalTask now so the tree can order it by level, and remember the ones we created so
	// we can drop any that don't survive the trim below.
	var created []*internalTask
	for _, t := range tasks {
		level := fairLevelFromAllocatedTask(t)
		if !tr.ackLevel.less(level) {
			// Reads may race with completes/acks such that we read some tasks that are already
			// acked. Ignore these.
			continue
		} else if mode == mergeWrite && !tr.atEnd && tr.readLevel.less(level) {
			// If we're writing and we're not at the end, ignore tasks above readLevel since we
			// don't know what's in between readLevel and there.
			continue
		} else if _, have := merged.GetHint(outstandingTask{level: level}, &hint); have {
			// On a write/read race or a re-read of a range we may see a task we already have
			// (loaded or acked). Ignore tasks we already track.
			continue
		} else if _, wasAcked := tr.evictedAcks.Delete(level); wasAcked {
			// This task was already acked, but its ack was evicted from memory before it could
			// advance the ack level, and now we've re-read it. Insert it as a pre-acked (nil) entry
			// so it advances the ack level instead of being re-delivered to the matcher.
			merged.SetHint(outstandingTask{level: level}, &hint)
		} else if IsTaskExpired(t) {
			// Expired tasks are inserted pre-acked so they advance ackLevel and get GC'd.
			merged.SetHint(outstandingTask{level: level}, &hint)
			recordDroppedTask(tr.backlogMgr.metricsHandler, dropReasonExpiredRead)
		} else {
			task := newInternalTaskFromBacklog(t, tr.completeTask)
			tr.backlogMgr.setPriority(task)
			merged.SetHint(outstandingTask{level: level, task: task}, &hint)
			created = append(created, task)
		}
	}

	// (2) Find the trim point: the level of the first loaded task past the lowest batchSize. Acked
	// levels don't occupy a memory slot, so they don't count toward the limit. If we never exceed
	// batchSize, cut stays at maxFairLevel, meaning nothing is trimmed.
	cut := outstandingTask{level: maxFairLevel}
	loaded := 0
	merged.Scan(func(o outstandingTask) bool {
		if o.acked() {
			return true
		} else if loaded++; loaded > batchSize {
			cut = o
			return false
		}
		return true
	})

	// (3) Split the tasks we just created into survivors and overflow (at or above the cut). We
	// never added the overflow to the matcher, so we just drop them from the tree; only survivors
	// are counted and returned to be matched. After we get to this point, each survivor must
	// eventually call task.finish/finishForwarded, which calls tr.completeTask.
	created = slices.DeleteFunc(created, func(task *internalTask) bool {
		if level := task.fairLevel(); !level.less(cut.level) {
			merged.DeleteHint(outstandingTask{level: level}, &hint)
			return true
		}
		tr.loadedTasks++
		tr.backlogAge.record(task.event.Data.CreateTime, 1)
		return false
	})

	// (4) Chop the pre-existing entries at or above the cut and evict them. The overflow we created
	// is already gone, so everything chopped here was in the matcher (a loaded task) or is an ack.
	// When there's no cut, cut is maxFairLevel and this range is empty.
	chopped := merged.DeleteRange(cut, outstandingTask{level: maxFairLevel}, nil)
	chopped.Scan(func(o outstandingTask) bool {
		if o.task != nil {
			// A loaded task we're dropping from memory. It may already have been matched and
			// removed (then setEvicted is a no-op); otherwise it's removed from the matcher.
			// Lock order: task reader lock < matcher lock, so this is okay.
			tr.backlogAge.record(o.task.event.Data.CreateTime, -1)
			tr.loadedTasks--
			o.task.setEvicted()
		} else {
			// An ack above the cut. Cache it so if we re-read the task we can skip it, rather
			// than using the ack to advance the ack level across the tasks dropped below it.
			tr.evictedAcks.Set(o.level)
		}
		return true
	})
	softassert.That(tr.logger, tr.loadedTasks >= 0, "loadedTasks went negative")
	// Trim the evicted-ack cache to max size by dropping the highest levels.
	for tr.evictedAcks.Len() > evictedAcksCacheSize {
		tr.evictedAcks.PopMax()
	}

	// Install the trimmed snapshot as the new outstanding tasks.
	tr.outstandingTasks = merged

	// readLevel is the highest level we track, loaded or acked. If we track nothing, leave it where
	// it is so we resume reading from there.
	if maxItem, ok := merged.Max(); ok {
		tr.readLevel = maxItem.level
	}

	// Advance the ack level past any pre-acked (nil) entries we just added: expired tasks and acks
	// we re-inserted from the evicted-ack cache. Harmless if we added none.
	tr.advanceAckLevelLocked()

	// Update atEnd:
	// If we did a read and didn't get to the end, we can't possibly be at the end.
	// Also if we trimmed anything from memory (cut moved below maxFairLevel), we can't either.
	// If we read to the end and didn't trim anything, then we know we're at the end.
	// Otherwise (i.e. on write) leave atEnd unchanged.
	if mode == mergeReadMiddle || cut.level != maxFairLevel {
		tr.atEnd = false
	} else if mode == mergeReadToEnd {
		tr.atEnd = true
	}

	// If we're at the end, then outstandingTasks is the whole queue so we can set count.
	if count := tr.knownCountLocked(); count >= 0 {
		tr.backlogMgr.db.setKnownFairBacklogCount(tr.subqueue, count)
	}

	return created

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

	// Adjust the ack level as far as we can, past any leading acks.
	var numAcked int64
	for {
		minItem, ok := tr.outstandingTasks.Min()
		if !ok || !minItem.acked() {
			break
		}
		tr.ackLevel = minItem.level
		tr.outstandingTasks.PopMin()
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
