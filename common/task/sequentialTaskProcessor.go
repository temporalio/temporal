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

package task

import (
	"sync"
	"sync/atomic"
	"time"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
)

type (
	sequentialTaskProcessorImpl struct {
		status       int32
		shutdownChan chan struct{}
		waitGroup    sync.WaitGroup

		coroutineSize    int
		taskqueues       collection.ConcurrentTxMap
		taskQueueFactory SequentialTaskQueueFactory
		taskqueueChan    chan SequentialTaskQueue

		metricsScope metrics.Scope
		logger       log.Logger
	}
)

// NewSequentialTaskProcessor create a new sequential tasks processor
func NewSequentialTaskProcessor(
	coroutineSize int,
	taskQueueHashFn collection.HashFunc,
	taskQueueFactory SequentialTaskQueueFactory,
	metricsClient metrics.Client,
	logger log.Logger,
) Processor {

	return &sequentialTaskProcessorImpl{
		status:           common.DaemonStatusInitialized,
		shutdownChan:     make(chan struct{}),
		coroutineSize:    coroutineSize,
		taskqueues:       collection.NewShardedConcurrentTxMap(1024, taskQueueHashFn),
		taskQueueFactory: taskQueueFactory,
		taskqueueChan:    make(chan SequentialTaskQueue, coroutineSize),
		metricsScope:     metricsClient.Scope(metrics.SequentialTaskProcessingScope),
		logger:           logger,
	}
}

func (t *sequentialTaskProcessorImpl) Start() {
	if !atomic.CompareAndSwapInt32(&t.status, common.DaemonStatusInitialized, common.DaemonStatusStarted) {
		return
	}

	t.waitGroup.Add(t.coroutineSize)
	for i := 0; i < t.coroutineSize; i++ {
		go t.pollAndProcessTaskQueue()
	}
	t.logger.Info("Task processor started.")
}

func (t *sequentialTaskProcessorImpl) Stop() {
	if !atomic.CompareAndSwapInt32(&t.status, common.DaemonStatusStarted, common.DaemonStatusStopped) {
		return
	}

	close(t.shutdownChan)
	if success := common.AwaitWaitGroup(&t.waitGroup, time.Minute); !success {
		t.logger.Warn("Task processor timeout trying to stop.")
	}
	t.logger.Info("Task processor stopped.")
}

func (t *sequentialTaskProcessorImpl) Submit(task Task) error {

	t.metricsScope.IncCounter(metrics.SequentialTaskSubmitRequest)
	metricsTimer := t.metricsScope.StartTimer(metrics.SequentialTaskSubmitLatency)
	defer metricsTimer.Stop()

	taskqueue := t.taskQueueFactory(task)
	taskqueue.Add(task)

	_, fnEvaluated, err := t.taskqueues.PutOrDo(
		taskqueue.QueueID(),
		taskqueue,
		func(key interface{}, value interface{}) error {
			value.(SequentialTaskQueue).Add(task)
			return nil
		},
	)
	if err != nil {
		return err
	}

	// if function evaluated, meaning that the task set is
	// already dispatched
	if fnEvaluated {
		t.metricsScope.IncCounter(metrics.SequentialTaskSubmitRequestTaskQueueExist)
		return nil
	}

	// need to dispatch this task set
	t.metricsScope.IncCounter(metrics.SequentialTaskSubmitRequestTaskQueueMissing)
	select {
	case <-t.shutdownChan:
	case t.taskqueueChan <- taskqueue:
	}
	return nil

}

func (t *sequentialTaskProcessorImpl) pollAndProcessTaskQueue() {
	defer t.waitGroup.Done()

	for {
		select {
		case <-t.shutdownChan:
			return
		case taskqueue := <-t.taskqueueChan:
			metricsTimer := t.metricsScope.StartTimer(metrics.SequentialTaskQueueProcessingLatency)
			t.processTaskQueue(taskqueue)
			metricsTimer.Stop()
		}
	}
}

func (t *sequentialTaskProcessorImpl) processTaskQueue(taskqueue SequentialTaskQueue) {
	for {
		select {
		case <-t.shutdownChan:
			return
		default:
			queueSize := taskqueue.Len()
			t.metricsScope.RecordTimer(metrics.SequentialTaskQueueSize, time.Duration(queueSize))
			if queueSize > 0 {
				t.processTaskOnce(taskqueue)
			} else {
				deleted := t.taskqueues.RemoveIf(taskqueue.QueueID(), func(key interface{}, value interface{}) bool {
					return value.(SequentialTaskQueue).IsEmpty()
				})
				if deleted {
					return
				}

				// if deletion failed, meaning that task queue is offered with new task
				// continue execution
			}
		}
	}
}

func (t *sequentialTaskProcessorImpl) processTaskOnce(taskqueue SequentialTaskQueue) {
	metricsTimer := t.metricsScope.StartTimer(metrics.SequentialTaskTaskProcessingLatency)
	defer metricsTimer.Stop()

	task := taskqueue.Remove()
	err := task.Execute()
	err = task.HandleErr(err)

	if err != nil {
		if task.RetryErr(err) {
			taskqueue.Add(task)
		} else {
			t.logger.Error("Unable to process task", tag.Error(err))
			task.Nack()
		}
	} else {
		task.Ack()
	}
}
