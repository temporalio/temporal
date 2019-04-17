// Copyright (c) 2017 Uber Technologies, Inc.
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
	"github.com/uber/cadence/common/log"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/uber/cadence/common"
)

type (
	sequentialTaskProcessorImpl struct {
		status       int32
		shutdownChan chan struct{}
		waitGroup    sync.WaitGroup

		coroutineSize       int
		taskBatchSize       int
		coroutineTaskQueues []chan SequentialTask
		logger              log.Logger
	}

	// SequentialTasks slice of SequentialTask
	SequentialTasks []SequentialTask
)

// NewSequentialTaskProcessor create a new sequential tasks processor
func NewSequentialTaskProcessor(coroutineSize int, taskBatchSize int, logger log.Logger) SequentialTaskProcessor {

	coroutineTaskQueues := make([]chan SequentialTask, coroutineSize)
	for i := 0; i < coroutineSize; i++ {
		coroutineTaskQueues[i] = make(chan SequentialTask, taskBatchSize)
	}

	return &sequentialTaskProcessorImpl{
		status:              common.DaemonStatusInitialized,
		shutdownChan:        make(chan struct{}),
		coroutineSize:       coroutineSize,
		taskBatchSize:       taskBatchSize,
		coroutineTaskQueues: coroutineTaskQueues,
		logger:              logger,
	}
}

func (t *sequentialTaskProcessorImpl) Start() {
	if !atomic.CompareAndSwapInt32(&t.status, common.DaemonStatusInitialized, common.DaemonStatusStarted) {
		return
	}

	t.waitGroup.Add(t.coroutineSize)
	for i := 0; i < t.coroutineSize; i++ {
		coroutineTaskQueue := t.coroutineTaskQueues[i]
		go t.pollAndProcessTaskQueue(coroutineTaskQueue)
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

func (t *sequentialTaskProcessorImpl) Submit(task SequentialTask) error {
	hashCode := int(task.HashCode()) % t.coroutineSize
	taskQueue := t.coroutineTaskQueues[hashCode]
	// need to dispatch this task set
	select {
	case <-t.shutdownChan:
	case taskQueue <- task:
	}
	return nil
}

func (t *sequentialTaskProcessorImpl) pollAndProcessTaskQueue(coroutineTaskQueue chan SequentialTask) {
	defer t.waitGroup.Done()

	for {
		select {
		case <-t.shutdownChan:
			return
		default:
			t.batchPollTaskQueue(coroutineTaskQueue)
		}
	}
}

func (t *sequentialTaskProcessorImpl) batchPollTaskQueue(coroutineTaskQueue chan SequentialTask) {
	bufferedSequentialTasks := make(map[interface{}][]SequentialTask)
	indexTasks := func(task SequentialTask) {
		sequentialTasks, ok := bufferedSequentialTasks[task.PartitionID()]
		if ok {
			sequentialTasks = append(sequentialTasks, task)
			bufferedSequentialTasks[task.PartitionID()] = sequentialTasks
		} else {
			bufferedSequentialTasks[task.PartitionID()] = []SequentialTask{task}
		}
	}

	select {
	case <-t.shutdownChan:
		return
	case task := <-coroutineTaskQueue:
		indexTasks(task)
	BufferLoop:
		for i := 0; i < t.taskBatchSize-1; i++ {
			select {
			case <-t.shutdownChan:
				return
			case task := <-coroutineTaskQueue:
				indexTasks(task)
			default:
				// currently no more task
				break BufferLoop
			}
		}
	}

	for _, sequentialTasks := range bufferedSequentialTasks {
		t.batchProcessingSequentialTasks(sequentialTasks)
	}
}

func (t *sequentialTaskProcessorImpl) batchProcessingSequentialTasks(sequentialTasks []SequentialTask) {
	sort.Sort(SequentialTasks(sequentialTasks))

	for _, task := range sequentialTasks {
		t.processTaskOnce(task)
	}
}

func (t *sequentialTaskProcessorImpl) processTaskOnce(task SequentialTask) {
	var err error

TaskProcessingLoop:
	for {
		select {
		case <-t.shutdownChan:
			return
		default:
			err = task.Execute()
			err = task.HandleErr(err)
			if err == nil || !task.RetryErr(err) {
				break TaskProcessingLoop
			}
		}
	}

	if err != nil {
		task.Nack()
		return
	}
	task.Ack()
}

func (tasks SequentialTasks) Len() int {
	return len(tasks)
}

func (tasks SequentialTasks) Swap(i, j int) {
	tasks[i], tasks[j] = tasks[j], tasks[i]
}

func (tasks SequentialTasks) Less(i, j int) bool {
	return tasks[i].TaskID() < tasks[j].TaskID()
}
