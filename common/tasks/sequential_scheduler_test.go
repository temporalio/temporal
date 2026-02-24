package tasks

import (
	"errors"
	"sync"
	"sync/atomic"
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
	mockTask.EXPECT().Nack(gomock.Any()).Do(func(arg any) { testWaitGroup.Done() }).Times(1)

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

	for range numSubmitter {
		channel := make(chan *MockTask, numTasks)
		for j := range numTasks {
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

func (s *sequentialSchedulerSuite) TestTrySubmitLockLeak() {
	scheduler := s.newTestProcessor()
	scheduler.Start()
	defer scheduler.Stop()

	// Manually acquire the lock to force timeout on first TrySubmit
	scheduler.trySubmitLock.Lock()

	// Create a task
	mockTask1 := NewMockTask(s.controller)
	mockTask1.EXPECT().RetryPolicy().Return(s.retryPolicy).AnyTimes()

	// Track whether first TrySubmit completed
	var firstTrySubmitDone atomic.Bool
	var firstTrySubmitResult atomic.Bool

	// Try to submit - this will timeout after 200ms
	go func() {
		result := scheduler.TrySubmit(mockTask1)
		firstTrySubmitResult.Store(result)
		firstTrySubmitDone.Store(true)
	}()

	// Wait for the first TrySubmit to timeout and the spawned goroutine to acquire the lock
	s.Require().Eventually(func() bool {
		return firstTrySubmitDone.Load()
	}, 500*time.Millisecond, 10*time.Millisecond, "First TrySubmit should timeout")

	s.Require().False(firstTrySubmitResult.Load(), "First TrySubmit should return false on timeout")

	// Release the lock - the spawned goroutine from TrySubmit should have already been signaled to unlock
	scheduler.trySubmitLock.Unlock()

	// Now try another TrySubmit - if there's a lock leak, this will also timeout
	mockTask2 := NewMockTask(s.controller)
	mockTask2.EXPECT().RetryPolicy().Return(s.retryPolicy).AnyTimes()
	mockTask2.EXPECT().Execute().Return(nil).MaxTimes(1)
	mockTask2.EXPECT().Ack().MaxTimes(1)

	// Track second TrySubmit result
	var secondTrySubmitDone atomic.Bool
	var secondTrySubmitResult atomic.Bool
	var secondTrySubmitElapsed atomic.Int64

	go func() {
		start := time.Now()
		result := scheduler.TrySubmit(mockTask2)
		elapsed := time.Since(start)
		secondTrySubmitResult.Store(result)
		secondTrySubmitElapsed.Store(int64(elapsed))
		secondTrySubmitDone.Store(true)
	}()

	// Wait for second TrySubmit to complete
	s.Require().Eventually(func() bool {
		return secondTrySubmitDone.Load()
	}, 500*time.Millisecond, 10*time.Millisecond, "Second TrySubmit should complete")

	elapsed := time.Duration(secondTrySubmitElapsed.Load())
	result := secondTrySubmitResult.Load()

	s.T().Logf("Second TrySubmit took %v, result=%v", elapsed, result)

	// If there's a lock leak, this will timeout (200ms+)
	// If the lock is properly released, this should succeed quickly (<50ms)
	s.Less(elapsed, 150*time.Millisecond,
		"Second TrySubmit should complete quickly, not timeout (would indicate lock leak)")
	s.True(result, "Second TrySubmit should succeed if lock is properly managed")
}

func (s *sequentialSchedulerSuite) TestTrySubmitConcurrent() {
	// Use a custom scheduler with QueueSize=1 to force contention
	scheduler := s.newTestProcessorWithQueueSize(1)
	scheduler.Start()
	defer scheduler.Stop()

	numGoroutines := 100
	tasksPerGoroutine := 20
	totalTasks := numGoroutines * tasksPerGoroutine

	// Atomic counters for tracking
	var trySubmitSuccess atomic.Int64
	var trySubmitFailure atomic.Int64
	var tasksProcessed atomic.Int64

	// Synchronization primitives
	goroutineStartWG := sync.WaitGroup{}
	goroutineEndWG := sync.WaitGroup{}

	// Launch goroutines
	goroutineStartWG.Add(numGoroutines)
	goroutineEndWG.Add(numGoroutines)

	for goroutineID := range numGoroutines {
		go func(gID int) {
			defer goroutineEndWG.Done()

			// Create tasks with mock expectations
			tasks := make([]*MockTask, tasksPerGoroutine)

			for taskIdx := range tasksPerGoroutine {
				mockTask := NewMockTask(s.controller)
				mockTask.EXPECT().RetryPolicy().Return(s.retryPolicy).AnyTimes()
				mockTask.EXPECT().Execute().DoAndReturn(func() error {
					tasksProcessed.Add(1)
					return nil
				}).MaxTimes(1)
				mockTask.EXPECT().Ack().Times(1)
				tasks[taskIdx] = mockTask
			}

			// Wait for all goroutines to be ready
			goroutineStartWG.Done()
			goroutineStartWG.Wait()

			// Submit all tasks concurrently
			for _, task := range tasks {
				if scheduler.TrySubmit(task) {
					trySubmitSuccess.Add(1)
				} else {
					trySubmitFailure.Add(1)
				}
			}
		}(goroutineID)
	}

	// Wait for all goroutines to submit
	goroutineEndWG.Wait()

	// Wait for all successfully submitted tasks to process
	// We use a polling approach to check if processing is complete
	timeout := time.After(10 * time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	expectedSuccess := trySubmitSuccess.Load()
	for {
		select {
		case <-timeout:
			s.T().Logf("Timeout: expected %d successful tasks, got %d processed",
				expectedSuccess, tasksProcessed.Load())
			s.Fail("Timeout waiting for tasks to process")
			return
		case <-ticker.C:
			processedCount := tasksProcessed.Load()
			if processedCount >= expectedSuccess {
				// All successfully submitted tasks have been processed
				goto assertionsLabel
			}
		}
	}

assertionsLabel:

	// Assertions
	successCount := trySubmitSuccess.Load()
	failureCount := trySubmitFailure.Load()
	processedCount := tasksProcessed.Load()

	// All TrySubmit calls accounted for
	s.Equal(int64(totalTasks), successCount+failureCount,
		"All TrySubmit calls must return either true or false")

	// All successful submissions were processed
	s.Equal(successCount, processedCount,
		"All tasks that returned true from TrySubmit must be processed")

	// At least some succeeded (system functional)
	s.Positive(successCount, "At least some TrySubmit calls should succeed")

	// Log the results - failure count may be 0 if all tasks are added to the same queue
	s.T().Logf("TrySubmit Results: %d succeeded, %d failed, %d processed (success rate: %.1f%%)",
		successCount, failureCount, processedCount, float64(successCount)/float64(totalTasks)*100)
}

func (s *sequentialSchedulerSuite) newTestProcessor() *SequentialScheduler[*MockTask] {
	hashFn := func(key any) uint32 {
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

func (s *sequentialSchedulerSuite) newTestProcessorWithQueueSize(queueSize int) *SequentialScheduler[*MockTask] {
	hashFn := func(key any) uint32 {
		return 1
	}
	factory := func(task *MockTask) SequentialTaskQueue[*MockTask] {
		return newTestSequentialTaskQueue[*MockTask](1, 3000)
	}
	return NewSequentialScheduler[*MockTask](
		&SequentialSchedulerOptions{
			QueueSize: queueSize,
			WorkerCount: func(_ func(int)) (v int, cancel func()) {
				return 1, func() {}
			},
		},
		hashFn,
		factory,
		log.NewNoopLogger(),
	)
}
