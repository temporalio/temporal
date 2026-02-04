package matching

import (
	"context"
	"sync/atomic"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/softassert"
	"go.temporal.io/server/service/matching/counter"
)

type (
	// fairTaskWriter writes tasks with stride scheduling
	fairTaskWriter struct {
		backlogMgr     *fairBacklogManagerImpl
		config         *taskQueueConfig
		db             *taskQueueDB
		logger         log.Logger
		counterFactory func(subqueueIndex) counter.Counter
		appendCh       chan *writeTaskRequest

		// state:
		taskIDBlock        taskIDBlock
		currentTaskIDBlock taskIDBlock                       // copy of taskIDBlock for safe concurrent access via getCurrentTaskIDBlock()
		counters           map[subqueueIndex]counter.Counter // only used in taskWriterLoop.
	}
)

func newFairTaskWriter(
	backlogMgr *fairBacklogManagerImpl,
	counterFactory func(subqueueIndex) counter.Counter,
) *fairTaskWriter {
	return &fairTaskWriter{
		backlogMgr:     backlogMgr,
		config:         backlogMgr.config,
		db:             backlogMgr.db,
		logger:         backlogMgr.logger,
		counterFactory: counterFactory,
		appendCh:       make(chan *writeTaskRequest, backlogMgr.config.OutstandingTaskAppendsThreshold()),

		taskIDBlock: noTaskIDs,
		counters:    make(map[subqueueIndex]counter.Counter),
	}
}

// Start fairTaskWriter background goroutine.
func (w *fairTaskWriter) Start() {
	go w.taskWriterLoop()
}

func (w *fairTaskWriter) appendTask(
	subqueue subqueueIndex,
	taskInfo *persistencespb.TaskInfo,
) error {
	select {
	case <-w.backlogMgr.tqCtx.Done():
		return errShutdown
	default:
		// noop
	}

	startTime := time.Now().UTC()
	ch := make(chan error)
	req := &writeTaskRequest{
		taskInfo:   taskInfo,
		responseCh: ch,
		subqueue:   subqueue,
	}

	select {
	case w.appendCh <- req:
		select {
		case err := <-ch:
			metrics.TaskWriteLatencyPerTaskQueue.With(w.backlogMgr.metricsHandler).Record(time.Since(startTime))
			return err
		case <-w.backlogMgr.tqCtx.Done():
			// if we are shutting down, this request will never make
			// it to persistence, just bail out and fail this request
			return errShutdown
		}
	default: // channel is full, throttle
		metrics.TaskWriteThrottlePerTaskQueueCounter.With(w.backlogMgr.metricsHandler).Record(1)
		return &serviceerror.ResourceExhausted{
			Cause:   enumspb.RESOURCE_EXHAUSTED_CAUSE_SYSTEM_OVERLOADED,
			Scope:   enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE,
			Message: "Too many outstanding appends to the task queue",
		}
	}
}

func (w *fairTaskWriter) allocTaskIDs(reqs []*writeTaskRequest) error {
	for i := range reqs {
		if w.taskIDBlock.start > w.taskIDBlock.end {
			// we ran out of current allocation block
			newBlock, err := w.allocTaskIDBlock(w.taskIDBlock.end)
			if err != nil {
				return err
			}
			w.taskIDBlock = newBlock
		}
		reqs[i].id = w.taskIDBlock.start
		w.taskIDBlock.start++
	}
	return nil
}

func (w *fairTaskWriter) pickPasses(tasks []*writeTaskRequest, bases []fairLevel) {
	// Fetch latest fairness weight overrides from the partition's rate limit manager via pqMgr
	overrides := w.backlogMgr.pqMgr.GetFairnessWeightOverrides()

	for i, task := range tasks {
		pri := task.taskInfo.Priority
		key := pri.GetFairnessKey()
		weight := getEffectiveWeight(overrides, pri)
		inc := max(1, int64(strideFactor/weight))
		base := bases[task.subqueue].pass
		cntr := w.counters[task.subqueue]
		if cntr == nil {
			cntr = w.counterFactory(task.subqueue)
			w.counters[task.subqueue] = cntr
		}
		pass := cntr.GetPass(key, base, inc)
		softassert.That(w.logger, pass >= base, "counter returned pass below base")
		tasks[i].pass = pass
	}
}

func (w *fairTaskWriter) initState() error {
	state, err := w.renewLeaseWithRetry(foreverRetryPolicy, common.IsPersistenceTransientError)
	if err != nil {
		w.backlogMgr.initState(taskQueueState{}, err)
		return err
	}
	w.taskIDBlock = rangeIDToTaskIDBlock(state.rangeID, w.config.RangeSize)
	w.currentTaskIDBlock = w.taskIDBlock
	w.backlogMgr.initState(state, nil)
	return nil
}

func (w *fairTaskWriter) taskWriterLoop() {
	if w.initState() != nil {
		return
	}

	// TODO: this will be out of phase with the timer in fairBacklogManagerImpl.periodicSync.
	// can we align them better?
	persistFairnessKeys := time.NewTicker(w.config.UpdateAckInterval()).C

	var reqs []*writeTaskRequest
	for {
		atomic.StoreInt64(&w.currentTaskIDBlock.start, w.taskIDBlock.start)
		atomic.StoreInt64(&w.currentTaskIDBlock.end, w.taskIDBlock.end)

		// prepare slice for reuse
		clear(reqs)
		reqs = reqs[:0]

		select {
		case <-w.backlogMgr.tqCtx.Done():
			return
		case req := <-w.appendCh:
			// read a batch of requests from the channel
			reqs = append(reqs, req)
			reqs = w.getWriteBatch(reqs)
		}

		err := w.allocTaskIDs(reqs)
		if err == nil {
			err = w.writeBatch(reqs)
		}

		for _, req := range reqs {
			req.responseCh <- err
		}

		// maybe persist fairness key counts if it's time
		select {
		case <-persistFairnessKeys:
			for subqueue, cntr := range w.counters {
				w.db.persistTopKFairnessKeys(subqueue, cntr.TopK())
			}
		default:
		}
	}
}

func (w *fairTaskWriter) getWriteBatch(reqs []*writeTaskRequest) []*writeTaskRequest {
	for range w.config.MaxTaskBatchSize() - 1 {
		select {
		case req := <-w.appendCh:
			reqs = append(reqs, req)
		default: // channel is empty, don't block
			return reqs
		}
	}
	return reqs
}

func (w *fairTaskWriter) writeBatch(reqs []*writeTaskRequest) (retErr error) {
	bases, unpin := w.backlogMgr.getAndPinAckLevels()
	defer func() { unpin(retErr) }()

	w.pickPasses(reqs, bases)
	resp, err := w.db.CreateFairTasks(w.backlogMgr.tqCtx, reqs)
	if err == nil {
		w.backlogMgr.wroteNewTasks(resp) // must be called before unpin()
	} else {
		w.logger.Error("Persistent store operation failure", tag.StoreOperationCreateTask, tag.Error(err))
		w.backlogMgr.signalIfFatal(err)
	}
	return err
}

func (w *fairTaskWriter) renewLeaseWithRetry(
	retryPolicy backoff.RetryPolicy,
	retryErrors backoff.IsRetryable,
) (taskQueueState, error) {
	var newState taskQueueState
	op := func(ctx context.Context) (err error) {
		newState, err = w.db.RenewLease(ctx)
		return
	}
	metrics.LeaseRequestPerTaskQueueCounter.With(w.backlogMgr.metricsHandler).Record(1)
	err := backoff.ThrottleRetryContext(w.backlogMgr.tqCtx, op, retryPolicy, retryErrors)
	if err != nil {
		metrics.LeaseFailurePerTaskQueueCounter.With(w.backlogMgr.metricsHandler).Record(1)
		return newState, err
	}
	return newState, nil
}

func (w *fairTaskWriter) allocTaskIDBlock(prevBlockEnd int64) (taskIDBlock, error) {
	currBlock := rangeIDToTaskIDBlock(w.db.RangeID(), w.config.RangeSize)
	if currBlock.end != prevBlockEnd {
		return taskIDBlock{}, errNonContiguousBlocks
	}
	state, err := w.renewLeaseWithRetry(persistenceOperationRetryPolicy, common.IsPersistenceTransientError)
	if err != nil {
		if w.backlogMgr.signalIfFatal(err) {
			return taskIDBlock{}, errShutdown
		}
		return taskIDBlock{}, err
	}
	return rangeIDToTaskIDBlock(state.rangeID, w.config.RangeSize), nil
}

// getCurrentTaskIDBlock returns the current taskIDBlock. Safe to be called concurrently.
func (w *fairTaskWriter) getCurrentTaskIDBlock() taskIDBlock {
	return taskIDBlock{
		start: atomic.LoadInt64(&w.currentTaskIDBlock.start),
		end:   atomic.LoadInt64(&w.currentTaskIDBlock.end),
	}
}
