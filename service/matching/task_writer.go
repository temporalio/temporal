package matching

import (
	"context"
	"errors"
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
	"go.temporal.io/server/common/persistence"
)

type (
	writeTaskRequest struct {
		taskInfo   *persistencespb.TaskInfo
		responseCh chan<- error
		subqueue   subqueueIndex // for priTaskWriter only
		fairLevel                // filled in by taskWriterLoop
	}

	taskIDBlock struct {
		start int64
		end   int64
	}

	// taskWriter writes tasks sequentially to persistence
	taskWriter struct {
		backlogMgr         *backlogManagerImpl
		config             *taskQueueConfig
		db                 *taskQueueDB
		logger             log.Logger
		appendCh           chan *writeTaskRequest
		taskIDBlock        taskIDBlock
		currentTaskIDBlock taskIDBlock // copy of taskIDBlock for safe concurrent access via getCurrentTaskIDBlock()
	}
)

var (
	// errShutdown indicates that the task queue is shutting down
	errShutdown            = &persistence.ConditionFailedError{Msg: "task queue shutting down"}
	errNonContiguousBlocks = errors.New("previous block end is not equal to current block")

	noTaskIDs = taskIDBlock{start: 1, end: 0}
)

func newTaskWriter(
	backlogMgr *backlogManagerImpl,
) *taskWriter {
	return &taskWriter{
		backlogMgr:  backlogMgr,
		config:      backlogMgr.config,
		db:          backlogMgr.db,
		logger:      backlogMgr.logger,
		appendCh:    make(chan *writeTaskRequest, backlogMgr.config.OutstandingTaskAppendsThreshold()),
		taskIDBlock: noTaskIDs,
	}
}

// Start taskWriter background goroutine.
func (w *taskWriter) Start() {
	go w.taskWriterLoop()
}

func (w *taskWriter) initReadWriteState() error {
	state, err := w.renewLeaseWithRetry(foreverRetryPolicy, common.IsPersistenceTransientError)
	if err != nil {
		return err
	}
	w.taskIDBlock = rangeIDToTaskIDBlock(state.rangeID, w.config.RangeSize)
	w.currentTaskIDBlock = w.taskIDBlock
	w.backlogMgr.taskAckManager.setAckLevel(state.ackLevel)

	return nil
}

func (w *taskWriter) appendTask(
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

func (w *taskWriter) assignTaskIDs(reqs []*writeTaskRequest) error {
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

func (w *taskWriter) appendTasks(reqs []*writeTaskRequest) error {
	_, err := w.db.CreateTasks(w.backlogMgr.tqCtx, reqs)
	if err != nil {
		w.backlogMgr.signalIfFatal(err)
		w.logger.Error("Persistent store operation failure",
			tag.StoreOperationCreateTask,
			tag.Error(err),
			tag.WorkflowTaskQueueName(w.backlogMgr.queueKey().PersistenceName()),
			tag.WorkflowTaskQueueType(w.backlogMgr.queueKey().TaskType()))
		return err
	}
	return nil
}

func (w *taskWriter) taskWriterLoop() {
	err := w.initReadWriteState()
	w.backlogMgr.SetInitializedError(err)

	for {
		atomic.StoreInt64(&w.currentTaskIDBlock.start, w.taskIDBlock.start)
		atomic.StoreInt64(&w.currentTaskIDBlock.end, w.taskIDBlock.end)

		select {
		case request := <-w.appendCh:
			// read a batch of requests from the channel
			reqs := []*writeTaskRequest{request}
			reqs = w.getWriteBatch(reqs)

			err := w.assignTaskIDs(reqs)
			if err == nil {
				err = w.appendTasks(reqs)
			}
			for _, req := range reqs {
				req.responseCh <- err
			}

		case <-w.backlogMgr.tqCtx.Done():
			return
		}
	}
}

func (w *taskWriter) getWriteBatch(reqs []*writeTaskRequest) []*writeTaskRequest {
readLoop:
	for i := 0; i < w.config.MaxTaskBatchSize(); i++ {
		select {
		case req := <-w.appendCh:
			reqs = append(reqs, req)
		default: // channel is empty, don't block
			break readLoop
		}
	}
	return reqs
}

func (w *taskWriter) renewLeaseWithRetry(
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

func (w *taskWriter) allocTaskIDBlock(prevBlockEnd int64) (taskIDBlock, error) {
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
func (w *taskWriter) getCurrentTaskIDBlock() taskIDBlock {
	return taskIDBlock{
		start: atomic.LoadInt64(&w.currentTaskIDBlock.start),
		end:   atomic.LoadInt64(&w.currentTaskIDBlock.end),
	}
}
