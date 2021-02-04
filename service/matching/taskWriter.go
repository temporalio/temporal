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
	"sync/atomic"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/persistence"
)

type (
	writeTaskResponse struct {
		err                 error
		persistenceResponse *persistence.CreateTasksResponse
	}

	writeTaskRequest struct {
		execution  *commonpb.WorkflowExecution
		taskInfo   *persistencespb.TaskInfo
		responseCh chan<- *writeTaskResponse
	}

	taskIDBlock struct {
		start int64
		end   int64
	}

	// taskWriter writes tasks sequentially to persistence
	taskWriter struct {
		tlMgr        *taskQueueManagerImpl
		config       *taskQueueConfig
		taskQueueID  *taskQueueID
		appendCh     chan *writeTaskRequest
		taskIDBlock  taskIDBlock
		maxReadLevel int64
		stopped      int64 // set to 1 if the writer is stopped or is shutting down
		logger       log.Logger
		stopCh       chan struct{} // shutdown signal for all routines in this class
	}
)

// errShutdown indicates that the task queue is shutting down
var errShutdown = &persistence.ConditionFailedError{Msg: "task queue shutting down"}

func newTaskWriter(tlMgr *taskQueueManagerImpl) *taskWriter {
	return &taskWriter{
		tlMgr:       tlMgr,
		config:      tlMgr.config,
		taskQueueID: tlMgr.taskQueueID,
		stopCh:      make(chan struct{}),
		appendCh:    make(chan *writeTaskRequest, tlMgr.config.OutstandingTaskAppendsThreshold()),
		logger:      tlMgr.logger,
	}
}

func (w *taskWriter) Start(block taskIDBlock) {
	w.taskIDBlock = block
	w.maxReadLevel = block.start - 1
	go w.taskWriterLoop()
}

// Stop stops the taskWriter
func (w *taskWriter) Stop() {
	if atomic.CompareAndSwapInt64(&w.stopped, 0, 1) {
		close(w.stopCh)
	}
}

func (w *taskWriter) isStopped() bool {
	return atomic.LoadInt64(&w.stopped) == 1
}

func (w *taskWriter) appendTask(
	execution *commonpb.WorkflowExecution,
	taskInfo *persistencespb.TaskInfo,
) (*persistence.CreateTasksResponse, error) {

	if w.isStopped() {
		return nil, errShutdown
	}

	ch := make(chan *writeTaskResponse)
	req := &writeTaskRequest{
		execution:  execution,
		taskInfo:   taskInfo,
		responseCh: ch,
	}

	select {
	case w.appendCh <- req:
		select {
		case r := <-ch:
			return r.persistenceResponse, r.err
		case <-w.stopCh:
			// if we are shutting down, this request will never make
			// it to cassandra, just bail out and fail this request
			return nil, errShutdown
		}
	default: // channel is full, throttle
		return nil, serviceerror.NewResourceExhausted("Too many outstanding appends to the TaskQueue")
	}
}

func (w *taskWriter) GetMaxReadLevel() int64 {
	return atomic.LoadInt64(&w.maxReadLevel)
}

func (w *taskWriter) allocTaskIDs(count int) ([]int64, error) {
	result := make([]int64, count)
	for i := 0; i < count; i++ {
		if w.taskIDBlock.start > w.taskIDBlock.end {
			// we ran out of current allocation block
			newBlock, err := w.tlMgr.allocTaskIDBlock(w.taskIDBlock.end)
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
	tasks []*persistencespb.AllocatedTaskInfo,
) (*persistence.CreateTasksResponse, error) {

	resp, err := w.tlMgr.db.CreateTasks(tasks)
	switch err.(type) {
	case nil:
		return resp, nil

	case *persistence.ConditionFailedError:
		w.tlMgr.Stop()
		return nil, err

	default:
		w.logger.Error("Persistent store operation failure",
			tag.StoreOperationCreateTask,
			tag.Error(err),
			tag.WorkflowTaskQueueName(w.taskQueueID.name),
			tag.WorkflowTaskQueueType(w.taskQueueID.taskType),
		)
		return nil, err
	}
}

func (w *taskWriter) taskWriterLoop() {
writerLoop:
	for {
		select {
		case request := <-w.appendCh:
			{
				// read a batch of requests from the channel
				reqs := []*writeTaskRequest{request}
				reqs = w.getWriteBatch(reqs)
				batchSize := len(reqs)

				maxReadLevel := int64(0)

				taskIDs, err := w.allocTaskIDs(batchSize)
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

				resp, err := w.appendTasks(tasks)
				w.sendWriteResponse(reqs, resp, err)
				// Update the maxReadLevel after the writes are completed.
				if maxReadLevel > 0 {
					atomic.StoreInt64(&w.maxReadLevel, maxReadLevel)
				}
			}
		case <-w.stopCh:
			// we don't close the appendCh here
			// because that can cause on a send on closed
			// channel panic on the appendTask()
			break writerLoop
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
