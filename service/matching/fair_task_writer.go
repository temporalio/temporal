package matching

import (
	"context"
	"math/rand"
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
	"go.temporal.io/server/service/matching/counter"
)

const (
	// minWeight * strideFactor must be >= 1
	strideFactor = 1000
	minWeight    = 0.001
)

type (
	// fairTaskWriter writes tasks with stride scheduling
	fairTaskWriter struct {
		backlogMgr         *fairBacklogManagerImpl
		config             *taskQueueConfig
		db                 *taskQueueDB
		counter            counter.Counter
		logger             log.Logger
		appendCh           chan *writeTaskRequest
		taskIDBlock        taskIDBlock
		currentTaskIDBlock taskIDBlock // copy of taskIDBlock for safe concurrent access via getCurrentTaskIDBlock()
	}
)

func newFairTaskWriter(
	backlogMgr *fairBacklogManagerImpl,
	cntr counter.Counter,
) *fairTaskWriter {
	return &fairTaskWriter{
		backlogMgr:  backlogMgr,
		config:      backlogMgr.config,
		db:          backlogMgr.db,
		counter:     cntr,
		logger:      backlogMgr.logger,
		appendCh:    make(chan *writeTaskRequest, backlogMgr.config.OutstandingTaskAppendsThreshold()),
		taskIDBlock: noTaskIDs,
	}
}

// Start fairTaskWriter background goroutine.
func (w *fairTaskWriter) Start() {
	go w.taskWriterLoop()
}

func (w *fairTaskWriter) appendTask(
	subqueue int,
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
			// it to cassandra, just bail out and fail this request
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
	// TODO(fairness): get this from config
	var overrideWeights map[string]float32

	for i, task := range tasks {
		key := task.taskInfo.Priority.GetFairnessKey()

		weight, ok := overrideWeights[key]
		if !ok {
			weight = task.taskInfo.Priority.GetFairnessWeight()
		}
		if weight <= 0.0 {
			weight = 1.0
		} else if weight < minWeight {
			weight = minWeight
		}

		inc := max(1, int64(strideFactor/weight))

		base := bases[task.subqueue].pass
		base += int64(rand.Float64() * float64(inc)) // randomize keys within "initial" pass

		tasks[i].pass = w.counter.GetPass(key, base, inc)
	}
}

func (w *fairTaskWriter) initState() error {
	retryForever := backoff.NewExponentialRetryPolicy(1 * time.Second).
		WithMaximumInterval(10 * time.Second).
		WithExpirationInterval(backoff.NoInterval)
	state, err := w.renewLeaseWithRetry(retryForever, common.IsPersistenceTransientError)
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

	var reqs []*writeTaskRequest
	for {
		atomic.StoreInt64(&w.currentTaskIDBlock.start, w.taskIDBlock.start)
		atomic.StoreInt64(&w.currentTaskIDBlock.end, w.taskIDBlock.end)

		var req *writeTaskRequest
		select {
		case <-w.backlogMgr.tqCtx.Done():
			return
		case req = <-w.appendCh:
		}

		// read a batch of requests from the channel
		reqs = append(reqs[:0], req)
		reqs = w.getWriteBatch(reqs)

		err := w.allocTaskIDs(reqs)

		if err == nil {
			bases, unpin := w.backlogMgr.getAndPinAckLevels()
			w.pickPasses(reqs, bases)
			var resp createFairTasksResponse
			resp, err = w.db.CreateFairTasks(w.backlogMgr.tqCtx, reqs)
			if err == nil {
				w.backlogMgr.wroteNewTasks(resp)
			} else {
				w.logger.Error("Persistent store operation failure", tag.StoreOperationCreateTask, tag.Error(err))
				w.backlogMgr.signalIfFatal(err)
			}
			unpin(err) // note this must be called after wroteNewTasks!
		}
		for _, req := range reqs {
			req.responseCh <- err
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
