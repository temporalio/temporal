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
	"github.com/uber/cadence/common/log"
	"go.uber.org/zap"
	"math/rand"
	"sync"
	"testing"

	"github.com/stretchr/testify/suite"
)

type (
	SequentialTaskProcessorSuite struct {
		suite.Suite
		coroutineSize int
		processor     SequentialTaskProcessor
	}

	testSequentialTaskImpl struct {
		waitgroup   *sync.WaitGroup
		partitionID uint32
		taskID      int64

		lock   sync.Mutex
		acked  int
		nacked int
	}
)

func TestSequentialTaskProcessorSuite(t *testing.T) {
	suite.Run(t, new(SequentialTaskProcessorSuite))
}

func (s *SequentialTaskProcessorSuite) SetupTest() {
	s.coroutineSize = 20
	zapLogger, err := zap.NewDevelopment()
	s.Require().NoError(err)
	logger := log.NewLogger(zapLogger)
	s.processor = NewSequentialTaskProcessor(
		s.coroutineSize,
		1000,
		logger,
	)
}

func (s *SequentialTaskProcessorSuite) TestSubmit() {
	waitgroup := &sync.WaitGroup{}
	waitgroup.Add(1)
	task := newTestSequentialTaskImpl(waitgroup, 4, int64(1))

	// do not start the processor
	s.Nil(s.processor.Submit(task))
	taskQueue := s.processor.(*sequentialTaskProcessorImpl).coroutineTaskQueues[int(task.HashCode())%s.coroutineSize]
	tasks := []SequentialTask{}
Loop:
	for {
		select {
		case task := <-taskQueue:
			tasks = append(tasks, task)
		default:
			break Loop
		}
	}
	s.Equal(1, len(tasks))
	s.Equal(task, tasks[0])
}

func (s *SequentialTaskProcessorSuite) TestPollAndProcessTaskQueue_ShutDown() {
	waitgroup := &sync.WaitGroup{}
	waitgroup.Add(1)
	task := newTestSequentialTaskImpl(waitgroup, 4, int64(1))
	taskQueue := make(chan SequentialTask, 1)
	taskQueue <- task

	s.processor.Start()
	s.processor.Stop()

	s.processor.(*sequentialTaskProcessorImpl).waitGroup.Add(1)
	s.processor.(*sequentialTaskProcessorImpl).pollAndProcessTaskQueue(taskQueue)
	select {
	case taskInQueue := <-taskQueue:
		s.Equal(task, taskInQueue)
	default:
		s.Fail("there should be one task in task queue when task processing logic is shutdown")
	}
	s.Equal(0, task.NumAcked())
	s.Equal(0, task.NumNcked())
}

func (s *SequentialTaskProcessorSuite) TestProcessTaskQueue() {
	waitgroup := &sync.WaitGroup{}
	waitgroup.Add(2)
	task1 := newTestSequentialTaskImpl(waitgroup, 4, int64(1))
	task2 := newTestSequentialTaskImpl(waitgroup, 4, int64(2))

	s.processor.Start()
	s.Nil(s.processor.Submit(task1))
	s.Nil(s.processor.Submit(task2))
	waitgroup.Wait()
	s.processor.Stop()

	s.Equal(1, task1.NumAcked())
	s.Equal(0, task1.NumNcked())
	s.Equal(1, task2.NumAcked())
	s.Equal(0, task2.NumNcked())
}

func (s *SequentialTaskProcessorSuite) TestTaskProcessing_UniqueQueueID() {
	numTasks := 100
	waitgroup := &sync.WaitGroup{}
	waitgroup.Add(numTasks)

	tasks := []*testSequentialTaskImpl{}
	for i := 0; i < numTasks; i++ {
		tasks = append(tasks, newTestSequentialTaskImpl(waitgroup, 4, int64(i)))
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
}

func (s *SequentialTaskProcessorSuite) TestTaskProcessing_RandomizedQueueID() {
	numQueues := 100
	numTasks := 1000
	waitgroup := &sync.WaitGroup{}
	waitgroup.Add(numQueues * numTasks)

	tasks := make([][]*testSequentialTaskImpl, numQueues)
	for i := 0; i < numQueues; i++ {
		tasks[i] = make([]*testSequentialTaskImpl, numTasks)

		for j := 0; j < numTasks; j++ {
			tasks[i][j] = newTestSequentialTaskImpl(waitgroup, uint32(i), int64(j))
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
}

func randomize(array []*testSequentialTaskImpl) {
	for i := 0; i < len(array); i++ {
		index := rand.Int31n(int32(i) + 1)
		array[i], array[index] = array[index], array[i]
	}
}

func newTestSequentialTaskImpl(waitgroup *sync.WaitGroup, partitionID uint32, taskID int64) *testSequentialTaskImpl {
	return &testSequentialTaskImpl{
		waitgroup:   waitgroup,
		partitionID: partitionID,
		taskID:      taskID,
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

func (t *testSequentialTaskImpl) PartitionID() interface{} {
	return t.partitionID
}

func (t *testSequentialTaskImpl) TaskID() int64 {
	return t.taskID
}

func (t *testSequentialTaskImpl) HashCode() uint32 {
	return t.partitionID
}
