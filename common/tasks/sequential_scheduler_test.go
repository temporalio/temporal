package tasks

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/log"
	"go.uber.org/mock/gomock"
)

type (
	sequentialSchedulerSuite struct {
		*require.Assertions
		suite.Suite

		controller *gomock.Controller

		scheduler   *SequentialScheduler[*MockTask]
		retryPolicy backoff.RetryPolicy
	}
)

func TestSequentialSchedulerSuite(t *testing.T) {
	s := new(sequentialSchedulerSuite)
	suite.Run(t, s)
}

func (s *sequentialSchedulerSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())

	s.retryPolicy = backoff.NewExponentialRetryPolicy(time.Millisecond)
	s.scheduler = s.newTestProcessor()
	s.scheduler.Start()
}

func (s *sequentialSchedulerSuite) TearDownTest() {
	s.scheduler.Stop()
	s.controller.Finish()
}

func (s *sequentialSchedulerSuite) TestSubmitProcess_Running_Success() {
	testWaitGroup := sync.WaitGroup{}
	testWaitGroup.Add(1)

	mockTask := NewMockTask(s.controller)
	mockTask.EXPECT().RetryPolicy().Return(s.retryPolicy).AnyTimes()
	mockTask.EXPECT().Execute().Return(nil).Times(1)
	mockTask.EXPECT().Ack().Do(func() { testWaitGroup.Done() }).Times(1)

	s.scheduler.Submit(mockTask)

	testWaitGroup.Wait()
}

func (s *sequentialSchedulerSuite) TestSubmitProcess_Running_Panic_ShouldCapturePanicAndNackTask() {
	testWaitGroup := sync.WaitGroup{}
	testWaitGroup.Add(1)

	mockTask := NewMockTask(s.controller)
	mockTask.EXPECT().RetryPolicy().Return(s.retryPolicy).AnyTimes()
	mockTask.EXPECT().Execute().DoAndReturn(func() {
		panic("random panic")
	}).Times(1)
	mockTask.EXPECT().Nack(gomock.Any()).Do(func(arg interface{}) { testWaitGroup.Done() }).Times(1)

	s.scheduler.Submit(mockTask)

	testWaitGroup.Wait()
}

func (s *sequentialSchedulerSuite) TestSubmitProcess_Running_FailExecution() {
	testWaitGroup := sync.WaitGroup{}
	testWaitGroup.Add(1)

	mockTask := NewMockTask(s.controller)
	mockTask.EXPECT().RetryPolicy().Return(s.retryPolicy).AnyTimes()
	executionErr := errors.New("random error")
	mockTask.EXPECT().Execute().Return(executionErr).Times(1)
	mockTask.EXPECT().HandleErr(executionErr).Return(executionErr).Times(1)
	mockTask.EXPECT().IsRetryableError(executionErr).Return(false).MaxTimes(1)
	mockTask.EXPECT().Nack(executionErr).Do(func(_ error) { testWaitGroup.Done() }).Times(1)

	s.scheduler.Submit(mockTask)

	testWaitGroup.Wait()
}

func (s *sequentialSchedulerSuite) TestSubmitProcess_Stopped_Submission() {
	testWaitGroup := sync.WaitGroup{}
	testWaitGroup.Add(1)

	s.scheduler.Stop()

	mockTask := NewMockTask(s.controller)

	// if task get picked up before worker goroutine receives the shutdown notification
	mockTask.EXPECT().RetryPolicy().Return(s.retryPolicy).MaxTimes(1)
	mockTask.EXPECT().Execute().Return(nil).MaxTimes(1)
	mockTask.EXPECT().Ack().Do(func() { testWaitGroup.Done() }).MaxTimes(1)

	// if task get drained
	mockTask.EXPECT().Abort().Do(func() { testWaitGroup.Done() }).MaxTimes(1)

	s.scheduler.Submit(mockTask)

	testWaitGroup.Wait()
}

func (s *sequentialSchedulerSuite) TestSubmitProcess_Stopped_FailExecution() {
	testWaitGroup := sync.WaitGroup{}
	testWaitGroup.Add(1)

	mockTask := NewMockTask(s.controller)
	mockTask.EXPECT().RetryPolicy().Return(s.retryPolicy).AnyTimes()
	executionErr := errors.New("random transient error")
	mockTask.EXPECT().Execute().Return(executionErr).Times(1)
	mockTask.EXPECT().HandleErr(executionErr).DoAndReturn(func(err error) error {
		s.scheduler.Stop()
		return err
	}).Times(1)
	mockTask.EXPECT().IsRetryableError(executionErr).Return(true).MaxTimes(1)
	mockTask.EXPECT().Abort().Do(func() { testWaitGroup.Done() }).Times(1)

	s.scheduler.Submit(mockTask)

	testWaitGroup.Wait()
}

func (s *sequentialSchedulerSuite) TestParallelSubmitProcess() {
	numSubmitter := 200
	numTasks := 100

	testWaitGroup := sync.WaitGroup{}
	testWaitGroup.Add(numSubmitter * numTasks)

	startWaitGroup := sync.WaitGroup{}
	endWaitGroup := sync.WaitGroup{}

	startWaitGroup.Add(numSubmitter)

	for i := 0; i < numSubmitter; i++ {
		channel := make(chan *MockTask, numTasks)
		for j := 0; j < numTasks; j++ {
			mockTask := NewMockTask(s.controller)
			mockTask.EXPECT().RetryPolicy().Return(s.retryPolicy).AnyTimes()
			switch j % 2 {
			case 0:
				// success
				mockTask.EXPECT().Execute().Return(nil).Times(1)
				mockTask.EXPECT().Ack().Do(func() { testWaitGroup.Done() }).Times(1)

			case 1:
				// fail
				executionErr := errors.New("random error")
				mockTask.EXPECT().Execute().Return(executionErr).Times(1)
				mockTask.EXPECT().HandleErr(executionErr).Return(executionErr).Times(1)
				mockTask.EXPECT().IsRetryableError(executionErr).Return(false).Times(1)
				mockTask.EXPECT().Nack(executionErr).Do(func(_ error) { testWaitGroup.Done() }).Times(1)

			default:
				s.Fail("case not expected")
			}
			channel <- mockTask
		}
		close(channel)

		endWaitGroup.Add(1)
		go func() {
			startWaitGroup.Wait()

			for mockTask := range channel {
				s.scheduler.Submit(mockTask)
			}

			endWaitGroup.Done()
		}()
		startWaitGroup.Done()
	}
	endWaitGroup.Wait()

	testWaitGroup.Wait()
}

func (s *sequentialSchedulerSuite) TestStartStopWorkers() {
	processor := s.newTestProcessor()
	// don't start the processor,
	// manually add/remove workers here to test the start/stop logic

	numWorkers := 10
	processor.startWorkers(numWorkers)
	s.Len(processor.workerShutdownCh, numWorkers)

	processor.stopWorkers(numWorkers / 2)
	s.Len(processor.workerShutdownCh, numWorkers/2)

	processor.stopWorkers(len(processor.workerShutdownCh))
	s.Empty(processor.workerShutdownCh)

	processor.shutdownWG.Wait()
}

func (s *sequentialSchedulerSuite) TestPendingTaskCount() {
	// Create a scheduler with multiple queue capacity to allow multiple queues to be buffered
	hashFn := func(key interface{}) uint32 {
		return uint32(key.(int))
	}
	// Track submission order to assign queue IDs
	submissionIndex := 0
	factory := func(task *MockTask) SequentialTaskQueue[*MockTask] {
		idx := submissionIndex
		submissionIndex++

		// Assign queue IDs: first 3 tasks to queue 1, next 2 to queue 2, last 4 to queue 3
		var queueID int
		if idx < 3 {
			queueID = 1
		} else if idx < 5 {
			queueID = 2
		} else {
			queueID = 3
		}
		return newTestSequentialTaskQueue[*MockTask](queueID, 100)
	}
	scheduler := NewSequentialScheduler[*MockTask](
		&SequentialSchedulerOptions{
			QueueSize: 10, // Large enough to buffer multiple queues
			WorkerCount: func(_ func(int)) (v int, cancel func()) {
				return 0, func() {} // No workers to prevent task processing
			},
		},
		hashFn,
		factory,
		log.NewNoopLogger(),
	)
	scheduler.Start()
	defer scheduler.Stop()

	// Submit tasks to different queues
	// Queue 1: 3 tasks
	// Queue 2: 2 tasks
	// Queue 3: 4 tasks
	// Total: 9 tasks across 3 queues
	numTasks := 9
	tasks := make([]*MockTask, numTasks)
	for i := 0; i < numTasks; i++ {
		tasks[i] = NewMockTask(s.controller)
		tasks[i].EXPECT().RetryPolicy().Return(s.retryPolicy).AnyTimes()
		// Tasks will be aborted when we manually drain or when scheduler stops
		tasks[i].EXPECT().Abort().Times(1)
	}

	// Submit all tasks
	for i := 0; i < numTasks; i++ {
		scheduler.Submit(tasks[i])
	}

	// Verify PendingTaskCount returns the number of tasks
	pendingCount := scheduler.PendingTaskCount()
	s.Equal(numTasks, pendingCount)

	// Manually drain all queues before stopping (drainTasks only drains from queueChan)
	// This ensures all tasks are aborted as expected
	iter := scheduler.queues.Iter()
	defer iter.Close()
	for entry := range iter.Entries() {
		queue := entry.Value.(SequentialTaskQueue[*MockTask])
		for !queue.IsEmpty() {
			queue.Remove().Abort()
		}
	}
}

func (s *sequentialSchedulerSuite) newTestProcessor() *SequentialScheduler[*MockTask] {
	hashFn := func(key interface{}) uint32 {
		return 1
	}
	factory := func(task *MockTask) SequentialTaskQueue[*MockTask] {
		return newTestSequentialTaskQueue[*MockTask](1, 3000)
	}
	return NewSequentialScheduler[*MockTask](
		&SequentialSchedulerOptions{
			QueueSize: 1,
			WorkerCount: func(_ func(int)) (v int, cancel func()) {
				return 1, func() {}
			},
		},
		hashFn,
		factory,
		log.NewNoopLogger(),
	)
}
