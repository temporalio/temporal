// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package matching

import (
	"context"
	"errors"
	"fmt"
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
	"go.temporal.io/server/internal/goro"
)

type (
	writeTaskResponse struct {
		err                 error
		persistenceResponse *persistence.CreateTasksResponse
	}

	writeTaskRequest struct {
		taskInfo   *persistencespb.TaskInfo
		responseCh chan<- *writeTaskResponse
	}

	taskIDBlock struct {
		start int64
		end   int64
	}

	// taskWriter writes tasks sequentially to persistence
	taskWriter struct {
		status       int32
		backlogMgr   *backlogManagerImpl
		config       *taskQueueConfig
		appendCh     chan *writeTaskRequest
		taskIDBlock  taskIDBlock
		maxReadLevel int64
		logger       log.Logger
		writeLoop    *goro.Handle
		idAlloc      idBlockAllocator
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
		status:       common.DaemonStatusInitialized,
		backlogMgr:   backlogMgr,
		config:       backlogMgr.config,
		appendCh:     make(chan *writeTaskRequest, backlogMgr.config.OutstandingTaskAppendsThreshold()),
		taskIDBlock:  noTaskIDs,
		maxReadLevel: noTaskIDs.start - 1,
		logger:       backlogMgr.logger,
		idAlloc:      backlogMgr.db,
		writeLoop:    goro.NewHandle(backlogMgr.contextInfoProvider(context.Background())),
	}
}

func (w *taskWriter) Start() {
	if !atomic.CompareAndSwapInt32(
		&w.status,
		common.DaemonStatusInitialized,
		common.DaemonStatusStarted,
	) {
		return
	}

	w.writeLoop.Go(w.taskWriterLoop)
}

// Stop stops the taskWriter
func (w *taskWriter) Stop() {
	if !atomic.CompareAndSwapInt32(
		&w.status,
		common.DaemonStatusStarted,
		common.DaemonStatusStopped,
	) {
		return
	}
	w.writeLoop.Cancel()
}

func (w *taskWriter) initReadWriteState(ctx context.Context) error {
	retryForever := backoff.NewExponentialRetryPolicy(1 * time.Second).
		WithMaximumInterval(10 * time.Second).
		WithExpirationInterval(backoff.NoInterval)

	state, err := w.renewLeaseWithRetry(
		ctx, retryForever, common.IsPersistenceTransientError)
	if err != nil {
		return err
	}
	w.taskIDBlock = rangeIDToTaskIDBlock(state.rangeID, w.config.RangeSize)
	atomic.StoreInt64(&w.maxReadLevel, w.taskIDBlock.start-1)
	w.backlogMgr.taskAckManager.setAckLevel(state.ackLevel)
	return nil
}

func (w *taskWriter) appendTask(
	taskInfo *persistencespb.TaskInfo,
) (*persistence.CreateTasksResponse, error) {

	select {
	case <-w.writeLoop.Done():
		return nil, errShutdown
	default:
		// noop
	}

	startTime := time.Now().UTC()
	ch := make(chan *writeTaskResponse)
	req := &writeTaskRequest{
		taskInfo:   taskInfo,
		responseCh: ch,
	}

	select {
	case w.appendCh <- req:
		select {
		case r := <-ch:
			metrics.TaskWriteLatencyPerTaskQueue.With(w.backlogMgr.metricsHandler).Record(time.Since(startTime))
			return r.persistenceResponse, r.err
		case <-w.writeLoop.Done():
			// if we are shutting down, this request will never make
			// it to cassandra, just bail out and fail this request
			return nil, errShutdown
		}
	default: // channel is full, throttle
		metrics.TaskWriteThrottlePerTaskQueueCounter.With(w.backlogMgr.metricsHandler).Record(1)
		return nil, &serviceerror.ResourceExhausted{
			Cause:   enumspb.RESOURCE_EXHAUSTED_CAUSE_SYSTEM_OVERLOADED,
			Scope:   enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE,
			Message: "Too many outstanding appends to the task queue",
		}
	}
}

func (w *taskWriter) GetMaxReadLevel() int64 {
	return atomic.LoadInt64(&w.maxReadLevel)
}

func (w *taskWriter) allocTaskIDs(ctx context.Context, count int) ([]int64, error) {
	result := make([]int64, count)
	for i := 0; i < count; i++ {
		if w.taskIDBlock.start > w.taskIDBlock.end {
			// we ran out of current allocation block
			newBlock, err := w.allocTaskIDBlock(ctx, w.taskIDBlock.end)
			if err != nil {
				return nil, err
			}
			w.taskIDBlock = newBlock
		}
		result[i] = w.taskIDBlock.start
		w.taskIDBlock.start++
	}
	return result, nil
}

func (w *taskWriter) appendTasks(
	ctx context.Context,
	tasks []*persistencespb.AllocatedTaskInfo,
) (*persistence.CreateTasksResponse, error) {

	resp, err := w.backlogMgr.db.CreateTasks(ctx, tasks)
	if err != nil {
		w.backlogMgr.signalIfFatal(err)
		w.logger.Error("Persistent store operation failure",
			tag.StoreOperationCreateTask,
			tag.Error(err),
			tag.WorkflowTaskQueueName(w.backlogMgr.queueKey().PersistenceName()),
			tag.WorkflowTaskQueueType(w.backlogMgr.queueKey().TaskType()))
		return nil, err
	}
	return resp, nil
}

func (w *taskWriter) taskWriterLoop(ctx context.Context) error {
	err := w.initReadWriteState(ctx)
	w.backlogMgr.SetInitializedError(err)

writerLoop:
	for {
		select {
		case request := <-w.appendCh:
			// read a batch of requests from the channel
			reqs := []*writeTaskRequest{request}
			reqs = w.getWriteBatch(reqs)
			batchSize := len(reqs)

			maxReadLevel := int64(0)

			taskIDs, err := w.allocTaskIDs(ctx, batchSize)
			if err != nil {
				w.sendWriteResponse(reqs, nil, err)
				continue writerLoop
			}

			var tasks []*persistencespb.AllocatedTaskInfo
			for i, req := range reqs {
				tasks = append(tasks, &persistencespb.AllocatedTaskInfo{
					TaskId: taskIDs[i],
					Data:   req.taskInfo,
				})
				maxReadLevel = taskIDs[i]
			}

			resp, err := w.appendTasks(ctx, tasks)
			// Update the maxReadLevel after the writes are completed, but before we send the response,
			// so that taskReader is guaranteed to see the new read level when SpoolTask wakes it up.
			if maxReadLevel > 0 {
				atomic.StoreInt64(&w.maxReadLevel, maxReadLevel)
			}
			w.sendWriteResponse(reqs, resp, err)

		case <-ctx.Done():
			return ctx.Err()
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

func (w *taskWriter) sendWriteResponse(
	reqs []*writeTaskRequest,
	persistenceResponse *persistence.CreateTasksResponse,
	err error,
) {
	for _, req := range reqs {
		resp := &writeTaskResponse{
			err:                 err,
			persistenceResponse: persistenceResponse,
		}

		req.responseCh <- resp
	}
}

func (w *taskWriter) renewLeaseWithRetry(
	ctx context.Context,
	retryPolicy backoff.RetryPolicy,
	retryErrors backoff.IsRetryable,
) (taskQueueState, error) {
	var newState taskQueueState
	op := func(context.Context) (err error) {
		newState, err = w.idAlloc.RenewLease(ctx)
		return
	}
	metrics.LeaseRequestPerTaskQueueCounter.With(w.backlogMgr.metricsHandler).Record(1)
	err := backoff.ThrottleRetryContext(ctx, op, retryPolicy, retryErrors)
	if err != nil {
		metrics.LeaseFailurePerTaskQueueCounter.With(w.backlogMgr.metricsHandler).Record(1)
		return newState, err
	}
	return newState, nil
}

func (w *taskWriter) allocTaskIDBlock(ctx context.Context, prevBlockEnd int64) (taskIDBlock, error) {
	currBlock := rangeIDToTaskIDBlock(w.idAlloc.RangeID(), w.config.RangeSize)
	if currBlock.end != prevBlockEnd {
		return taskIDBlock{},
			fmt.Errorf(
				"%w: allocTaskIDBlock: invalid state: prevBlockEnd:%v != currTaskIDBlock:%+v",
				errNonContiguousBlocks,
				prevBlockEnd,
				currBlock,
			)
	}
	state, err := w.renewLeaseWithRetry(ctx, persistenceOperationRetryPolicy, common.IsPersistenceTransientError)
	if err != nil {
		if w.backlogMgr.signalIfFatal(err) {
			return taskIDBlock{}, errShutdown
		}
		return taskIDBlock{}, err
	}
	return rangeIDToTaskIDBlock(state.rangeID, w.config.RangeSize), nil
}
