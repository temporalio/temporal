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
	"errors"
	"math/rand"
	"sync"
	"testing"

	"github.com/uber-go/tally"

	"github.com/stretchr/testify/suite"

	"github.com/uber/cadence/common/collection"
	"github.com/uber/cadence/common/log/loggerimpl"
	"github.com/uber/cadence/common/metrics"
)

type (
	SequentialTaskProcessorSuite struct {
		suite.Suite
		processor Processor
	}

	testSequentialTaskQueueImpl struct {
		id        uint32
		taskQueue collection.Queue
	}

	testSequentialTaskImpl struct {
		waitgroup *sync.WaitGroup
		queueID   uint32
		taskID    uint32

		lock   sync.Mutex
		acked  int
		nacked int
	}
)

func TestSequentialTaskProcessorSuite(t *testing.T) {
	suite.Run(t, new(SequentialTaskProcessorSuite))
}

func (s *SequentialTaskProcessorSuite) SetupTest() {
	logger, err := loggerimpl.NewDevelopment()
	s.Nil(err)
	s.processor = NewSequentialTaskProcessor(
		20,
		func(key interface{}) uint32 {
			return key.(uint32)
		},
		func(task Task) SequentialTaskQueue {
			taskQueue := collection.NewConcurrentPriorityQueue(func(this interface{}, other interface{}) bool {
				return this.(*testSequentialTaskImpl).taskID < other.(*testSequentialTaskImpl).taskID
			})

			return &testSequentialTaskQueueImpl{
				id:        task.(*testSequentialTaskImpl).queueID,
				taskQueue: taskQueue,
			}
		},
		metrics.NewClient(tally.NoopScope, metrics.Common),
		logger,
	)
}

func (s *SequentialTaskProcessorSuite) TestSubmit_NoPriorTask() {
	waitgroup := &sync.WaitGroup{}
	waitgroup.Add(1)
	task := newTestSequentialTaskImpl(waitgroup, 4, uint32(1))

	// do not start the processor
	s.Nil(s.processor.Submit(task))
	sequentialTaskQueue := <-s.processor.(*sequentialTaskProcessorImpl).taskqueueChan
	sequentialTask := sequentialTaskQueue.Remove()
	s.True(sequentialTaskQueue.IsEmpty())
	s.Equal(task, sequentialTask)
}

func (s *SequentialTaskProcessorSuite) TestSubmit_HasPriorTask() {
	waitgroup := &sync.WaitGroup{}
	task1 := newTestSequentialTaskImpl(waitgroup, 4, uint32(1))
	task2 := newTestSequentialTaskImpl(waitgroup, 4, uint32(2))

	// do not start the processor
	s.Nil(s.processor.Submit(task1))
	s.Nil(s.processor.Submit(task2))
	sequentialTaskQueue := <-s.processor.(*sequentialTaskProcessorImpl).taskqueueChan
	sequentialTask1 := sequentialTaskQueue.Remove()
	sequentialTask2 := sequentialTaskQueue.Remove()
	s.True(sequentialTaskQueue.IsEmpty())
	s.Equal(task1, sequentialTask1)
	s.Equal(task2, sequentialTask2)
}

func (s *SequentialTaskProcessorSuite) TestProcessTaskQueue_ShutDown() {
	waitgroup := &sync.WaitGroup{}
	waitgroup.Add(2)
	task1 := newTestSequentialTaskImpl(waitgroup, 4, uint32(1))
	task2 := newTestSequentialTaskImpl(waitgroup, 4, uint32(2))

	// do not start the processor
	s.Nil(s.processor.Submit(task1))
	s.Nil(s.processor.Submit(task2))
	sequentialTaskQueue := <-s.processor.(*sequentialTaskProcessorImpl).taskqueueChan

	s.processor.Start()
	s.processor.Stop()
	s.processor.(*sequentialTaskProcessorImpl).processTaskQueue(sequentialTaskQueue)

	s.Equal(0, task1.NumAcked())
	s.Equal(0, task1.NumNcked())
	s.Equal(0, task2.NumAcked())
	s.Equal(0, task2.NumNcked())
	s.Equal(1, s.processor.(*sequentialTaskProcessorImpl).taskqueues.Len())
	s.Equal(2, sequentialTaskQueue.Len())
}

func (s *SequentialTaskProcessorSuite) TestProcessTaskQueue() {
	waitgroup := &sync.WaitGroup{}
	waitgroup.Add(2)
	task1 := newTestSequentialTaskImpl(waitgroup, 4, uint32(1))
	task2 := newTestSequentialTaskImpl(waitgroup, 4, uint32(2))

	// do not start the processor
	s.Nil(s.processor.Submit(task1))
	s.Nil(s.processor.Submit(task2))
	sequentialTaskQueue := <-s.processor.(*sequentialTaskProcessorImpl).taskqueueChan

	s.processor.(*sequentialTaskProcessorImpl).processTaskQueue(sequentialTaskQueue)
	waitgroup.Wait()

	s.Equal(1, task1.NumAcked())
	s.Equal(0, task1.NumNcked())
	s.Equal(1, task2.NumAcked())
	s.Equal(0, task2.NumNcked())
	s.Equal(0, s.processor.(*sequentialTaskProcessorImpl).taskqueues.Len())
	s.Equal(0, sequentialTaskQueue.Len())
}

func (s *SequentialTaskProcessorSuite) TestSequentialTaskProcessing() {
	numTasks := 100
	waitgroup := &sync.WaitGroup{}
	waitgroup.Add(numTasks)

	tasks := []*testSequentialTaskImpl{}
	for i := 0; i < numTasks; i++ {
		tasks = append(tasks, newTestSequentialTaskImpl(waitgroup, 4, uint32(i)))
	}

	s.processor.Start()
	for _, task := range tasks {
		s.Nil(s.processor.Submit(task))
	}
	waitgroup.Wait()
	s.processor.Stop()

	for _, task := range tasks {
		s.Equal(1, task.NumAcked())
		s.Equal(0, task.NumNcked())
	}
	s.Equal(0, s.processor.(*sequentialTaskProcessorImpl).taskqueues.Len())
}

func (s *SequentialTaskProcessorSuite) TestRandomizedTaskProcessing() {
	numQueues := 100
	numTasks := 1000
	waitgroup := &sync.WaitGroup{}
	waitgroup.Add(numQueues * numTasks)

	tasks := make([][]*testSequentialTaskImpl, numQueues)
	for i := 0; i < numQueues; i++ {
		tasks[i] = make([]*testSequentialTaskImpl, numTasks)

		for j := 0; j < numTasks; j++ {
			tasks[i][j] = newTestSequentialTaskImpl(waitgroup, uint32(i), uint32(j))
		}

		randomize(tasks[i])
	}

	s.processor.Start()
	startChan := make(chan struct{})
	for i := 0; i < numQueues; i++ {
		go func(i int) {
			<-startChan

			for j := 0; j < numTasks; j++ {
				s.Nil(s.processor.Submit(tasks[i][j]))
			}
		}(i)
	}
	close(startChan)
	waitgroup.Wait()
	s.processor.Stop()

	for i := 0; i < numQueues; i++ {
		for j := 0; j < numTasks; j++ {
			task := tasks[i][j]
			s.Equal(1, task.NumAcked())
			s.Equal(0, task.NumNcked())
		}
	}
	s.Equal(0, s.processor.(*sequentialTaskProcessorImpl).taskqueues.Len())
}

func randomize(array []*testSequentialTaskImpl) {
	for i := 0; i < len(array); i++ {
		index := rand.Int31n(int32(i) + 1)
		array[i], array[index] = array[index], array[i]
	}
}

func newTestSequentialTaskImpl(waitgroup *sync.WaitGroup, queueID uint32, taskID uint32) *testSequentialTaskImpl {
	return &testSequentialTaskImpl{
		waitgroup: waitgroup,
		queueID:   queueID,
		taskID:    taskID,
	}
}

func (t *testSequentialTaskImpl) Execute() error {
	if rand.Float64() < 0.5 {
		return nil
	}

	return errors.New("some random error")
}

func (t *testSequentialTaskImpl) HandleErr(err error) error {
	return err
}

func (t *testSequentialTaskImpl) RetryErr(err error) bool {
	return true
}

func (t *testSequentialTaskImpl) State() State {
	if t.acked > 0 {
		return TaskStateAcked
	}

	if t.nacked > 0 {
		return TaskStateNacked
	}

	return TaskStatePending
}

func (t *testSequentialTaskImpl) Ack() {
	t.lock.Lock()
	defer t.lock.Unlock()

	t.acked++
	t.waitgroup.Done()
}

func (t *testSequentialTaskImpl) NumAcked() int {
	t.lock.Lock()
	defer t.lock.Unlock()

	return t.acked
}

func (t *testSequentialTaskImpl) Nack() {
	t.lock.Lock()
	defer t.lock.Unlock()

	t.nacked++
	t.waitgroup.Done()
}

func (t *testSequentialTaskImpl) NumNcked() int {
	t.lock.Lock()
	defer t.lock.Unlock()

	return t.nacked
}

func (t *testSequentialTaskQueueImpl) QueueID() interface{} {
	return t.id
}

func (t *testSequentialTaskQueueImpl) Add(task Task) {
	t.taskQueue.Add(task)
}

func (t *testSequentialTaskQueueImpl) Remove() Task {
	return t.taskQueue.Remove().(Task)
}

func (t *testSequentialTaskQueueImpl) IsEmpty() bool {
	return t.taskQueue.IsEmpty()
}

func (t *testSequentialTaskQueueImpl) Len() int {
	return t.taskQueue.Len()
}
