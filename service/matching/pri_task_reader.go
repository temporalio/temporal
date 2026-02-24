package matching

import (
	"context"
	"errors"
	"slices"
	"sync"
	"time"

	"github.com/emirpasic/gods/maps/treemap"
	godsutils "github.com/emirpasic/gods/utils"
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

const (
	// TODO(pri): old matcher cleanup, move to here
	// taskReaderThrottleRetryDelay = 3 * time.Second

	concurrentAddRetries = 10
)

type (
	priTaskReader struct {
		backlogMgr *priBacklogManagerImpl
		subqueue   subqueueIndex
		notifyC    chan struct{} // Used as signal to notify pump of new tasks
		logger     log.Logger

		lock sync.Mutex

		backoffTimer *time.Timer
		retrier      backoff.Retrier

		backlogAge backlogAgeTracker

		addRetries *semaphore.Weighted

		// ack manager state
		outstandingTasks *treemap.Map // TaskID->acked
		loadedTasks      int
		readLevel        int64 // Maximum TaskID inserted into outstandingTasks
		ackLevel         int64 // Maximum TaskID below which all tasks are acked

		// gc state
		inGC       bool
		gcAckLevel int64     // last ack level GCed
		lastGCTime time.Time // last time GCed
	}
)

var addErrorRetryPolicy = backoff.NewExponentialRetryPolicy(2 * time.Second).
	WithExpirationInterval(backoff.NoInterval)

func newPriTaskReader(
	backlogMgr *priBacklogManagerImpl,
	subqueue subqueueIndex,
	initialAckLevel int64,
) *priTaskReader {
	return &priTaskReader{
		backlogMgr: backlogMgr,
		subqueue:   subqueue,
		notifyC:    make(chan struct{}, 1),
		logger:     backlogMgr.logger,
		retrier: backoff.NewRetrier(
			common.CreateReadTaskRetryPolicy(),
			clock.NewRealTimeSource(),
		),
		backlogAge: newBacklogAgeTracker(),
		addRetries: semaphore.NewWeighted(concurrentAddRetries),

		// ack manager
		outstandingTasks: treemap.NewWith(godsutils.Int64Comparator),
		readLevel:        initialAckLevel,
		ackLevel:         initialAckLevel,

		// gc state
		lastGCTime: time.Now(),
	}
}

// Start priTaskReader background goroutines.
func (tr *priTaskReader) Start() {
	go tr.getTasksPump()
}

func (tr *priTaskReader) SignalTaskLoading() {
	select {
	case tr.notifyC <- struct{}{}:
	default: // channel already has an event, don't block
	}
}

func (tr *priTaskReader) getOldestBacklogTime() time.Time {
	tr.lock.Lock()
	defer tr.lock.Unlock()
	return tr.backlogAge.oldestTime()
}

func (tr *priTaskReader) completeTask(task *internalTask, res taskResponse) {
	err := res.err()

	// We can handle some transient errors by just putting the task back in the matcher to
	// match again. Note that for forwarded tasks, it's expected to get DeadlineExceeded when
	// the task doesn't match on the root after backlogTaskForwardTimeout, and also expected to
	// get errRemoteSyncMatchFailed, which is a serviceerror.Canceled error.
	if err != nil && (common.IsServiceClientTransientError(err) ||
		common.IsContextDeadlineExceededErr(err) ||
		common.IsContextCanceledErr(err)) {
		// TODO(pri): if this was a start error (not a forwarding error): consider adding a
		// per-task backoff here, in case the error was workflow busy, we don't want to end up
		// trying the same task immediately. maybe also: after a few attempts on the same task,
		// let it get cycled to the end of the queue, in case there's some task/wf-specific
		// thing.
		tr.addTaskToMatcher(task)
		return
	}

	// On other errors: ask backlog manager to re-spool to persistence
	if err != nil {
		if tr.backlogMgr.respoolTaskAfterError(task.event.Data) != nil {
			return // task queue will unload now
		}
	}

	tr.lock.Lock()
	defer tr.lock.Unlock()

	tr.backlogAge.record(task.event.AllocatedTaskInfo.Data.CreateTime, -1)

	numAcked := tr.ackTaskLocked(task.event.TaskId)

	tr.maybeGCLocked()

	// use == so we just signal once when we cross this threshold
	// TODO(pri): is this safe? maybe we need to improve this
	if tr.loadedTasks == tr.backlogMgr.config.GetTasksReloadAt() {
		tr.SignalTaskLoading()
	}

	tr.backlogMgr.db.updateAckLevelAndBacklogStats(tr.subqueue, tr.ackLevel, -numAcked, tr.backlogAge.oldestTime())
}

// nolint:revive // can simplify later
func (tr *priTaskReader) getTasksPump() {
	ctx := tr.backlogMgr.tqCtx

	tr.SignalTaskLoading() // prime pump
	for {
		select {
		case <-ctx.Done():
			return
		case <-tr.notifyC:
		}

		if tr.getLoadedTasks() > tr.backlogMgr.config.GetTasksReloadAt() {
			// Too many loaded already, ignore this signal. We'll get another signal when
			// loadedTasks drops low enough.
			continue
		}

		batch, err := tr.getTaskBatch(ctx)
		tr.backlogMgr.signalIfFatal(err)
		if err != nil {
			// TODO: Should we ever stop retrying on db errors?
			if common.IsResourceExhausted(err) {
				tr.backoffSignal(taskReaderThrottleRetryDelay)
			} else {
				tr.backoffSignal(tr.retrier.NextBackOff(err))
			}
			continue
		}
		tr.retrier.Reset()

		if len(batch.tasks) == 0 {
			tr.setReadLevelAfterGap(batch.readLevel)
			if !batch.isReadBatchDone {
				tr.SignalTaskLoading()
			}
			continue
		}

		tr.processTaskBatch(batch.tasks)
		// There may be more tasks.
		tr.SignalTaskLoading()
	}
}

// TODO(pri): old matcher cleanup: move here
// type getTasksBatchResponse struct {
// 	tasks           []*persistencespb.AllocatedTaskInfo
// 	readLevel       int64
// 	isReadBatchDone bool
// }

// Returns a batch of tasks from persistence starting form current read level.
// Also return a number that can be used to update readLevel
// Also return a bool to indicate whether read is finished
func (tr *priTaskReader) getTaskBatch(ctx context.Context) (getTasksBatchResponse, error) {
	tr.lock.Lock()
	readLevel := tr.readLevel
	tr.lock.Unlock()

	maxReadLevel := tr.backlogMgr.db.GetMaxReadLevel(tr.subqueue)

	// counter i is used to break and let caller check whether taskqueue is still alive and needs to resume read.
	for i := 0; i < 10 && readLevel < maxReadLevel; i++ {
		upper := min(readLevel+tr.backlogMgr.config.RangeSize, maxReadLevel)
		response, err := tr.backlogMgr.db.GetTasks(
			ctx,
			tr.subqueue,
			readLevel+1,
			upper+1,
			tr.backlogMgr.config.GetTasksBatchSize(),
		)
		if err != nil {
			return getTasksBatchResponse{}, err
		}
		// return as long as it grabs any tasks
		if len(response.Tasks) > 0 {
			return getTasksBatchResponse{tasks: response.Tasks}, nil
		}
		readLevel = upper
	}
	return getTasksBatchResponse{
		tasks:           nil,
		readLevel:       readLevel,
		isReadBatchDone: readLevel == maxReadLevel,
	}, nil // caller will update readLevel when no task grabbed
}

func (tr *priTaskReader) processTaskBatch(tasks []*persistencespb.AllocatedTaskInfo) {
	tr.lock.Lock()

	tasks = slices.DeleteFunc(tasks, func(t *persistencespb.AllocatedTaskInfo) bool {
		tr.readLevel = max(tr.readLevel, t.TaskId)

		if IsTaskExpired(t) {
			// task expired when we read it
			metrics.ExpiredTasksPerTaskQueueCounter.With(tr.backlogMgr.metricsHandler).Record(1, metrics.TaskExpireStageReadTag)
			return true
		}

		// We may race to read tasks with signalNewTasks. If it wins, we may end up seeing
		// tasks twice. In that case, we should just ignore them. If we win (based on
		// readLevel), signalNewTasks will give up and signal us.
		_, found := tr.outstandingTasks.Get(t.TaskId)
		return found
	})

	tr.recordNewTasksLocked(tasks)

	tr.lock.Unlock()

	tr.addNewTasks(tasks)
}

// To add tasks to the matcher: call recordNewTasksLocked with tr.lock held, then release the
// lock and call addNewTasks. We call addTaskToMatcher outside tr.lock since it may take other
// locks to redirect the task.
func (tr *priTaskReader) recordNewTasksLocked(tasks []*persistencespb.AllocatedTaskInfo) {
	// After we get to this point, we must eventually call task.finish or
	// task.finishForwarded, which will call tr.completeTask.
	for _, t := range tasks {
		tr.outstandingTasks.Put(t.TaskId, false)
		tr.loadedTasks++
		tr.backlogAge.record(t.Data.CreateTime, 1)
	}
}

// To add tasks to the matcher: call recordNewTasksLocked with tr.lock held, then release the
// lock and call addNewTasks. We call addTaskToMatcher outside tr.lock since it may take other
// locks to redirect the task.
func (tr *priTaskReader) addNewTasks(tasks []*persistencespb.AllocatedTaskInfo) {
	for _, t := range tasks {
		task := newInternalTaskFromBacklog(t, tr.completeTask)
		tr.backlogMgr.setPriority(task)
		tr.addTaskToMatcher(task)
	}
}

func (tr *priTaskReader) addTaskToMatcher(task *internalTask) {
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

func (tr *priTaskReader) addErrorBehavior(err error) (drop, retry bool) {
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

func (tr *priTaskReader) retryAddAfterError(task *internalTask) {
	defer tr.addRetries.Release(1)
	metrics.BufferThrottlePerTaskQueueCounter.With(tr.backlogMgr.metricsHandler).Record(1)

	// initial sleep since we just tried once
	util.InterruptibleSleep(tr.backlogMgr.tqCtx, time.Second)

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

func (tr *priTaskReader) signalNewTasks(resp subqueueCreateTasksResponse) {
	tr.lock.Lock()

	// We have to be very careful not to increment the read level past an ID that will somehow
	// end up in the database, otherwise we might lose a task. We do this by verifying that our
	// read level was equal to the previous max read level (i.e. we were at the end of the
	// queue), and then we set it to the max read level as of CreateTasks.
	// We also check that there's room in memory.
	canAddDirect := tr.readLevel == resp.maxReadLevelBefore &&
		(tr.loadedTasks+len(resp.tasks)) <= tr.backlogMgr.config.GetTasksBatchSize() &&
		!slices.ContainsFunc(resp.tasks, func(t *persistencespb.AllocatedTaskInfo) bool {
			// Because we checked readLevel, we know that getTasksPump can't have beat us to
			// adding these tasks to outstandingTasks. So they should definitely not be there.
			_, found := tr.outstandingTasks.Get(t.TaskId)
			return softassert.That(tr.logger, !found, "newly-written task already present in outstanding tasks")
		})

	if !canAddDirect {
		tr.lock.Unlock()
		tr.SignalTaskLoading()
		return
	}

	tr.readLevel = resp.maxReadLevelAfter

	tr.recordNewTasksLocked(resp.tasks)

	tr.lock.Unlock()

	tr.addNewTasks(resp.tasks)
}

func (tr *priTaskReader) backoffSignal(duration time.Duration) {
	tr.lock.Lock()
	defer tr.lock.Unlock()

	if tr.backoffTimer == nil {
		tr.backoffTimer = time.AfterFunc(duration, func() {
			tr.lock.Lock()
			defer tr.lock.Unlock()

			tr.SignalTaskLoading() // re-enqueue the event
			tr.backoffTimer = nil
		})
	}
}

// ack manager

func (tr *priTaskReader) getLoadedTasks() int {
	tr.lock.Lock()
	defer tr.lock.Unlock()
	return tr.loadedTasks
}

// isDrained returns true if this subqueue has been fully drained:
// - We've read to the end of the queue (readLevel >= maxReadLevel)
// - No tasks are outstanding (in flight)
// Note: outstandingTasks.Empty() implies loadedTasks == 0
func (tr *priTaskReader) isDrained() bool {
	tr.lock.Lock()
	defer tr.lock.Unlock()
	maxReadLevel := tr.backlogMgr.db.GetMaxReadLevel(tr.subqueue)
	return tr.readLevel >= maxReadLevel && tr.outstandingTasks.Empty()
}

func (tr *priTaskReader) ackTaskLocked(taskId int64) int64 {
	wasAlreadyAcked, found := tr.outstandingTasks.Get(taskId)
	if !softassert.That(tr.logger, found, "completed task not found in oustandingTasks") {
		return 0
	}
	if !softassert.That(tr.logger, !wasAlreadyAcked.(bool), "completed task was already acked") {
		return 0
	}

	tr.outstandingTasks.Put(taskId, true)
	tr.loadedTasks--

	// Adjust the ack level as far as we can
	var numAcked int64
	for {
		minId, acked := tr.outstandingTasks.Min()
		if minId == nil || !acked.(bool) {
			break
		}
		tr.ackLevel = minId.(int64) // nolint:revive
		tr.outstandingTasks.Remove(minId)
		numAcked += 1
	}
	return numAcked
}

func (tr *priTaskReader) setReadLevelAfterGap(newReadLevel int64) {
	tr.lock.Lock()
	defer tr.lock.Unlock()
	if tr.ackLevel == tr.readLevel {
		// This is called after we read a range and find no tasks. The range we read was tr.readLevel to newReadLevel.
		// (We know this because nothing should change tr.readLevel except the getTasksPump loop itself, after initialization.
		// And getTasksPump doesn't start until it gets a signal from taskWriter that it's initialized the levels.)
		// If we've acked all tasks up to tr.readLevel, and there are no tasks between that and newReadLevel, then we've
		// acked all tasks up to newReadLevel too. This lets us advance the ack level on a task queue with no activity
		// but where the rangeid has moved higher, to prevent excessive reads on the next load.
		tr.ackLevel = newReadLevel
	}
	tr.readLevel = newReadLevel
}

func (tr *priTaskReader) getLevels() (readLevel, ackLevel int64) {
	tr.lock.Lock()
	defer tr.lock.Unlock()
	return tr.readLevel, tr.ackLevel
}

// gc

func (tr *priTaskReader) maybeGCLocked() {
	if !tr.shouldGCLocked() {
		return
	}
	tr.inGC = true
	tr.lastGCTime = time.Now()
	// gc in new goroutine so poller doesn't have to wait
	go tr.doGC(tr.ackLevel)
}

func (tr *priTaskReader) shouldGCLocked() bool {
	if tr.inGC {
		return false
	} else if gcGap := int(tr.ackLevel - tr.gcAckLevel); gcGap == 0 {
		return false
	} else if gcGap >= tr.backlogMgr.config.MaxTaskDeleteBatchSize() {
		return true
	}
	return time.Since(tr.lastGCTime) > tr.backlogMgr.config.TaskDeleteInterval()
}

// called in new goroutine
func (tr *priTaskReader) doGC(ackLevel int64) {
	wasComplete, err := tr.doGCAt(ackLevel)

	tr.lock.Lock()
	defer tr.lock.Unlock()

	tr.inGC = false
	if err != nil {
		return
	}
	if wasComplete {
		tr.gcAckLevel = ackLevel
	}
}

func (tr *priTaskReader) doGCAt(ackLevel int64) (bool, error) {
	batchSize := tr.backlogMgr.config.MaxTaskDeleteBatchSize()

	ctx, cancel := context.WithTimeout(tr.backlogMgr.tqCtx, ioTimeout)
	defer cancel()

	n, err := tr.backlogMgr.db.CompleteTasksLessThan(ctx, ackLevel+1, batchSize, tr.subqueue)
	if err != nil {
		tr.logger.Warn("failed to gc tasks", tag.Error(err))
		return false, err
	}

	// implementation behavior for CompleteTasksLessThan:
	// - unit test, cassandra: always return UnknownNumRowsAffected (in this case means "all")
	// - sql: return number of rows affected (should be <= batchSize)
	// if we get UnknownNumRowsAffected or a smaller number than our limit, we know we got
	// everything <= ackLevel, so we can reset ours. if not, we may have to try again.
	wasComplete := n == persistence.UnknownNumRowsAffected || n < batchSize
	return wasComplete, err
}

// finalGC does a single synchronous gc.
// Used when unloading a draining queue that won't be reloaded.
func (tr *priTaskReader) finalGC() {
	tr.lock.Lock()
	ackLevel := tr.ackLevel
	tr.lock.Unlock()
	if ackLevel == 0 {
		return
	}
	_, _ = tr.doGCAt(ackLevel)
}
