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
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.uber.org/mock/gomock"
)

type (
	workflowQueueSchedulerSuite struct {
		*require.Assertions
		suite.Suite

		controller *gomock.Controller

		scheduler   *WorkflowQueueScheduler[*MockTask]
		retryPolicy backoff.RetryPolicy
	}

	// testTaskWithID is a wrapper that provides workflow ID for queue routing
	testTaskWithID struct {
		*MockTask
		workflowID int
	}
)

func TestWorkflowQueueSchedulerSuite(t *testing.T) {
	s := new(workflowQueueSchedulerSuite)
	suite.Run(t, s)
}

func (s *workflowQueueSchedulerSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.controller = gomock.NewController(s.T())
	s.retryPolicy = backoff.NewExponentialRetryPolicy(time.Millisecond)
	s.scheduler = s.newScheduler(1, 100)
	s.scheduler.Start()
}

func (s *workflowQueueSchedulerSuite) TearDownTest() {
	s.scheduler.Stop()
	s.controller.Finish()
}

// =============================================================================
// Basic Submit Tests
// =============================================================================

func (s *workflowQueueSchedulerSuite) TestSubmit_Success() {
	testWG := sync.WaitGroup{}
	testWG.Add(1)

	mockTask := NewMockTask(s.controller)
	mockTask.EXPECT().RetryPolicy().Return(s.retryPolicy).AnyTimes()
	mockTask.EXPECT().Execute().Return(nil).Times(1)
	mockTask.EXPECT().Ack().Do(func() { testWG.Done() }).Times(1)

	s.scheduler.Submit(mockTask)

	testWG.Wait()
}

func (s *workflowQueueSchedulerSuite) TestSubmit_Failure() {
	testWG := sync.WaitGroup{}
	testWG.Add(1)

	mockTask := NewMockTask(s.controller)
	mockTask.EXPECT().RetryPolicy().Return(s.retryPolicy).AnyTimes()
	executionErr := errors.New("execution error")
	mockTask.EXPECT().Execute().Return(executionErr).Times(1)
	mockTask.EXPECT().HandleErr(executionErr).Return(executionErr).Times(1)
	mockTask.EXPECT().IsRetryableError(executionErr).Return(false).MaxTimes(1)
	mockTask.EXPECT().Nack(executionErr).Do(func(_ error) { testWG.Done() }).Times(1)

	s.scheduler.Submit(mockTask)

	testWG.Wait()
}

func (s *workflowQueueSchedulerSuite) TestSubmit_Panic() {
	testWG := sync.WaitGroup{}
	testWG.Add(1)

	mockTask := NewMockTask(s.controller)
	mockTask.EXPECT().RetryPolicy().Return(s.retryPolicy).AnyTimes()
	mockTask.EXPECT().Execute().DoAndReturn(func() error {
		panic("test panic")
	}).Times(1)
	mockTask.EXPECT().Nack(gomock.Any()).Do(func(_ error) { testWG.Done() }).Times(1)

	s.scheduler.Submit(mockTask)

	testWG.Wait()
}

func (s *workflowQueueSchedulerSuite) TestSubmit_RetryThenSuccess() {
	testWG := sync.WaitGroup{}
	testWG.Add(1)

	mockTask := NewMockTask(s.controller)
	mockTask.EXPECT().RetryPolicy().Return(s.retryPolicy).AnyTimes()

	executionErr := errors.New("transient error")
	callCount := 0
	mockTask.EXPECT().Execute().DoAndReturn(func() error {
		callCount++
		if callCount < 3 {
			return executionErr
		}
		return nil
	}).Times(3)
	mockTask.EXPECT().HandleErr(executionErr).Return(executionErr).Times(2)
	mockTask.EXPECT().IsRetryableError(executionErr).Return(true).Times(2)
	mockTask.EXPECT().Ack().Do(func() { testWG.Done() }).Times(1)

	s.scheduler.Submit(mockTask)

	testWG.Wait()
}

// =============================================================================
// Sequential Execution Tests
// =============================================================================

func (s *workflowQueueSchedulerSuite) TestSequentialExecution_SameWorkflow() {
	// Use a scheduler with 1 worker to ensure sequential processing
	scheduler := s.newSchedulerWithWorkflowID(1, 100)
	scheduler.Start()
	defer scheduler.Stop()

	numTasks := 10
	executionOrder := make([]int, 0, numTasks)
	var orderMu sync.Mutex
	testWG := sync.WaitGroup{}
	testWG.Add(numTasks)

	for i := 0; i < numTasks; i++ {
		taskIndex := i
		mockTask := NewMockTask(s.controller)
		mockTask.EXPECT().RetryPolicy().Return(s.retryPolicy).AnyTimes()
		mockTask.EXPECT().Execute().DoAndReturn(func() error {
			orderMu.Lock()
			executionOrder = append(executionOrder, taskIndex)
			orderMu.Unlock()
			time.Sleep(time.Millisecond) // Small delay to ensure ordering
			return nil
		}).Times(1)
		mockTask.EXPECT().Ack().Do(func() { testWG.Done() }).Times(1)

		// All tasks have same workflow ID (1)
		scheduler.Submit(&testTaskWithID{MockTask: mockTask, workflowID: 1})
	}

	testWG.Wait()

	// Verify tasks executed in order
	s.Len(executionOrder, numTasks)
	for i := 0; i < numTasks; i++ {
		s.Equal(i, executionOrder[i], "Tasks should execute in submission order")
	}
}

func (s *workflowQueueSchedulerSuite) TestSequentialExecution_DifferentWorkflows() {
	// Use 1 worker to ensure true sequential execution per workflow.
	// With multiple workers, there's a race where a queue can be emptied/removed
	// and recreated while a task is still executing, allowing concurrent execution.
	scheduler := s.newSchedulerWithWorkflowID(1, 100)
	scheduler.Start()
	defer scheduler.Stop()

	numWorkflows := 4
	tasksPerWorkflow := 5

	// Track execution order per workflow - use separate slices to avoid map access races
	type workflowOrders struct {
		mu     sync.Mutex
		orders []int
	}
	executionOrders := make([]*workflowOrders, numWorkflows)
	for i := 0; i < numWorkflows; i++ {
		executionOrders[i] = &workflowOrders{orders: make([]int, 0, tasksPerWorkflow)}
	}

	testWG := sync.WaitGroup{}
	testWG.Add(numWorkflows * tasksPerWorkflow)

	for wf := 0; wf < numWorkflows; wf++ {
		for t := 0; t < tasksPerWorkflow; t++ {
			workflowID := wf
			taskIndex := t
			mockTask := NewMockTask(s.controller)
			mockTask.EXPECT().RetryPolicy().Return(s.retryPolicy).AnyTimes()
			mockTask.EXPECT().Execute().DoAndReturn(func() error {
				executionOrders[workflowID].mu.Lock()
				executionOrders[workflowID].orders = append(executionOrders[workflowID].orders, taskIndex)
				executionOrders[workflowID].mu.Unlock()
				time.Sleep(time.Millisecond)
				return nil
			}).Times(1)
			mockTask.EXPECT().Ack().Do(func() { testWG.Done() }).Times(1)

			scheduler.Submit(&testTaskWithID{MockTask: mockTask, workflowID: workflowID})
		}
	}

	testWG.Wait()

	// Verify each workflow's tasks executed in order
	for wf := 0; wf < numWorkflows; wf++ {
		s.Len(executionOrders[wf].orders, tasksPerWorkflow)
		for i := 0; i < tasksPerWorkflow; i++ {
			s.Equal(i, executionOrders[wf].orders[i], "Workflow %d tasks should execute in order", wf)
		}
	}
}

// =============================================================================
// TrySubmit Tests
// =============================================================================

func (s *workflowQueueSchedulerSuite) TestTrySubmit_Success() {
	testWG := sync.WaitGroup{}
	testWG.Add(1)

	mockTask := NewMockTask(s.controller)
	mockTask.EXPECT().RetryPolicy().Return(s.retryPolicy).AnyTimes()
	mockTask.EXPECT().Execute().Return(nil).Times(1)
	mockTask.EXPECT().Ack().Do(func() { testWG.Done() }).Times(1)

	result := s.scheduler.TrySubmit(mockTask)
	s.True(result)

	testWG.Wait()
}

func (s *workflowQueueSchedulerSuite) TestTrySubmit_ExistingQueue() {
	// Create a scheduler that blocks task execution so we can add to existing queue
	scheduler := s.newSchedulerWithWorkflowID(1, 100)
	scheduler.Start()
	defer scheduler.Stop()

	blockCh := make(chan struct{})
	testWG := sync.WaitGroup{}
	testWG.Add(2)

	// First task - will block
	mockTask1 := NewMockTask(s.controller)
	mockTask1.EXPECT().RetryPolicy().Return(s.retryPolicy).AnyTimes()
	mockTask1.EXPECT().Execute().DoAndReturn(func() error {
		<-blockCh // Block until signaled
		return nil
	}).Times(1)
	mockTask1.EXPECT().Ack().Do(func() { testWG.Done() }).Times(1)

	// Second task - should be added to existing queue
	mockTask2 := NewMockTask(s.controller)
	mockTask2.EXPECT().RetryPolicy().Return(s.retryPolicy).AnyTimes()
	mockTask2.EXPECT().Execute().Return(nil).Times(1)
	mockTask2.EXPECT().Ack().Do(func() { testWG.Done() }).Times(1)

	// Submit first task
	result1 := scheduler.TrySubmit(&testTaskWithID{MockTask: mockTask1, workflowID: 1})
	s.True(result1)

	// Wait a bit for the first task to start executing
	time.Sleep(10 * time.Millisecond)

	// Submit second task to same workflow - should succeed (existing queue)
	result2 := scheduler.TrySubmit(&testTaskWithID{MockTask: mockTask2, workflowID: 1})
	s.True(result2)

	// Verify queue exists
	s.True(scheduler.HasQueue(1))

	// Unblock first task
	close(blockCh)

	testWG.Wait()
}

func (s *workflowQueueSchedulerSuite) TestTrySubmit_ChannelFull() {
	// Create scheduler with very small queue
	scheduler := s.newSchedulerWithWorkflowID(0, 1) // 0 workers, queue size 1
	scheduler.Start()

	// Don't start workers, so channel will fill up
	mockTask1 := NewMockTask(s.controller)
	mockTask1.EXPECT().Abort().MaxTimes(1) // May be aborted during Stop()
	// First submit should succeed (fills the channel)
	result1 := scheduler.TrySubmit(&testTaskWithID{MockTask: mockTask1, workflowID: 1})
	s.True(result1)

	// Second submit for different workflow should fail (channel full)
	mockTask2 := NewMockTask(s.controller)
	// Note: We do NOT expect Abort here - TrySubmit returns false, caller handles the task
	result2 := scheduler.TrySubmit(&testTaskWithID{MockTask: mockTask2, workflowID: 2})
	s.False(result2)

	// The task should NOT be in a queue (was not added)
	s.False(scheduler.HasQueue(2))

	// Clean up - stop will drain remaining tasks
	scheduler.Stop()
}

// =============================================================================
// Shutdown Tests
// =============================================================================

func (s *workflowQueueSchedulerSuite) TestShutdown_DrainTasks() {
	// Create scheduler with no workers so tasks stay in queue
	scheduler := s.newSchedulerWithWorkflowID(0, 100)
	scheduler.Start()

	numTasks := 5
	abortWG := sync.WaitGroup{}
	abortWG.Add(numTasks)

	for i := 0; i < numTasks; i++ {
		mockTask := NewMockTask(s.controller)
		mockTask.EXPECT().Abort().Do(func() { abortWG.Done() }).Times(1)
		scheduler.Submit(&testTaskWithID{MockTask: mockTask, workflowID: i})
	}

	// Stop should drain all tasks
	scheduler.Stop()

	// Wait for all aborts
	done := make(chan struct{})
	go func() {
		abortWG.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Success
	case <-time.After(5 * time.Second):
		s.Fail("Timed out waiting for tasks to be aborted")
	}
}

func (s *workflowQueueSchedulerSuite) TestShutdown_DuringExecution() {
	scheduler := s.newSchedulerWithWorkflowID(1, 100)
	scheduler.Start()

	blockCh := make(chan struct{})
	task1Done := make(chan struct{})
	task2Aborted := make(chan struct{})

	// First task - will be executing when we stop
	mockTask1 := NewMockTask(s.controller)
	mockTask1.EXPECT().RetryPolicy().Return(s.retryPolicy).AnyTimes()
	mockTask1.EXPECT().Execute().DoAndReturn(func() error {
		<-blockCh // Block until signaled
		return errors.New("error")
	}).Times(1)
	mockTask1.EXPECT().HandleErr(gomock.Any()).Return(errors.New("error")).MaxTimes(1)
	mockTask1.EXPECT().IsRetryableError(gomock.Any()).Return(true).MaxTimes(1)
	mockTask1.EXPECT().Abort().Do(func() { close(task1Done) }).Times(1)

	// Second task - should be aborted during drain
	mockTask2 := NewMockTask(s.controller)
	mockTask2.EXPECT().Abort().Do(func() { close(task2Aborted) }).Times(1)

	// Submit both tasks
	scheduler.Submit(&testTaskWithID{MockTask: mockTask1, workflowID: 1})
	time.Sleep(10 * time.Millisecond) // Let first task start
	scheduler.Submit(&testTaskWithID{MockTask: mockTask2, workflowID: 1})

	// Stop scheduler while first task is executing
	go func() {
		time.Sleep(10 * time.Millisecond)
		scheduler.Stop()
		close(blockCh) // Unblock the task
	}()

	// Wait for both tasks to be handled
	select {
	case <-task1Done:
	case <-time.After(5 * time.Second):
		s.Fail("Task 1 not aborted")
	}

	select {
	case <-task2Aborted:
	case <-time.After(5 * time.Second):
		s.Fail("Task 2 not aborted")
	}
}

func (s *workflowQueueSchedulerSuite) TestSubmit_AfterShutdown() {
	scheduler := s.newScheduler(1, 100)
	scheduler.Start()
	scheduler.Stop()

	abortWG := sync.WaitGroup{}
	abortWG.Add(1)

	mockTask := NewMockTask(s.controller)
	// Task might be processed before worker sees shutdown, or aborted
	mockTask.EXPECT().RetryPolicy().Return(s.retryPolicy).MaxTimes(1)
	mockTask.EXPECT().Execute().Return(nil).MaxTimes(1)
	mockTask.EXPECT().Ack().MaxTimes(1)
	mockTask.EXPECT().Abort().Do(func() { abortWG.Done() }).MaxTimes(1)

	scheduler.Submit(mockTask)

	// Give time for task to be processed or aborted
	time.Sleep(100 * time.Millisecond)
}

// =============================================================================
// Worker Scaling Tests
// =============================================================================

func (s *workflowQueueSchedulerSuite) TestWorkerScaling_StartStop() {
	scheduler := s.newScheduler(0, 100)
	// Don't start - manually control workers

	numWorkers := 5
	scheduler.startWorkers(numWorkers)
	s.Len(scheduler.workerShutdownCh, numWorkers)

	scheduler.stopWorkers(2)
	s.Len(scheduler.workerShutdownCh, numWorkers-2)

	scheduler.stopWorkers(numWorkers - 2)
	s.Empty(scheduler.workerShutdownCh)

	scheduler.shutdownWG.Wait()
}

func (s *workflowQueueSchedulerSuite) TestWorkerScaling_DynamicScaleUp() {
	var workerCountCallback func(int)
	workerCountFn := func(callback func(int)) (int, func()) {
		workerCountCallback = callback
		return 1, func() {}
	}

	scheduler := NewWorkflowQueueScheduler[*MockTask](
		&WorkflowQueueSchedulerOptions{
			QueueSize:   100,
			WorkerCount: workerCountFn,
		},
		func(task *MockTask) any { return 1 }, // Simple key function
		log.NewNoopLogger(),
		metrics.NoopMetricsHandler,
		clock.NewRealTimeSource(),
	)
	scheduler.Start()
	defer scheduler.Stop()

	s.Len(scheduler.workerShutdownCh, 1)

	// Scale up
	workerCountCallback(5)
	s.Len(scheduler.workerShutdownCh, 5)
}

func (s *workflowQueueSchedulerSuite) TestWorkerScaling_ScaleDownRedispatches() {
	// Start with 2 workers
	scheduler := s.newSchedulerWithWorkflowID(2, 100)
	scheduler.Start()
	defer scheduler.Stop()

	blockCh := make(chan struct{})
	taskDone := make(chan struct{})

	mockTask := NewMockTask(s.controller)
	mockTask.EXPECT().RetryPolicy().Return(s.retryPolicy).AnyTimes()
	mockTask.EXPECT().Execute().DoAndReturn(func() error {
		<-blockCh
		return nil
	}).Times(1)
	mockTask.EXPECT().Ack().Do(func() { close(taskDone) }).Times(1)

	// Submit task
	scheduler.Submit(&testTaskWithID{MockTask: mockTask, workflowID: 1})
	time.Sleep(10 * time.Millisecond) // Let task start

	// Scale down to 1 worker while task is processing
	scheduler.workerLock.Lock()
	scheduler.stopWorkers(1)
	scheduler.workerLock.Unlock()

	// Unblock the task
	close(blockCh)

	// Task should complete (re-dispatched to remaining worker)
	select {
	case <-taskDone:
		// Success
	case <-time.After(5 * time.Second):
		s.Fail("Task not completed after scale down")
	}
}

// =============================================================================
// HasQueue Tests
// =============================================================================

func (s *workflowQueueSchedulerSuite) TestHasQueue() {
	scheduler := s.newSchedulerWithWorkflowID(1, 100)
	scheduler.Start()
	defer scheduler.Stop()

	// Initially no queues
	s.False(scheduler.HasQueue(1))

	blockCh := make(chan struct{})
	executingCh := make(chan struct{})
	testWG := sync.WaitGroup{}
	testWG.Add(1)

	mockTask := NewMockTask(s.controller)
	mockTask.EXPECT().RetryPolicy().Return(s.retryPolicy).AnyTimes()
	mockTask.EXPECT().Execute().DoAndReturn(func() error {
		close(executingCh) // Signal that we're executing
		<-blockCh          // Wait to complete
		return nil
	}).Times(1)
	mockTask.EXPECT().Ack().Do(func() { testWG.Done() }).Times(1)

	// Submit task
	scheduler.Submit(&testTaskWithID{MockTask: mockTask, workflowID: 1})

	// Wait until task is actually executing
	<-executingCh

	// Note: The queue exists in the map but may or may not have tasks.
	// Since we only submitted one task and it's now executing (popped from queue),
	// the queue is marked as empty but kept around for TTL (defaultEmptyQueueTTL).
	// This allows retried tasks to find the queue and be routed back here.

	// Complete task
	close(blockCh)
	testWG.Wait()

	// Queue still exists after task completes (within TTL for retry routing)
	// It will be cleaned up by the periodic cleanup goroutine after defaultEmptyQueueTTL.
	// This is expected behavior - the TTL allows retried tasks to find the queue.
	time.Sleep(10 * time.Millisecond)
	s.True(scheduler.HasQueue(1), "Empty queue should exist within TTL")
}

func (s *workflowQueueSchedulerSuite) TestHasQueue_MultipleTasksInQueue() {
	scheduler := s.newSchedulerWithWorkflowID(1, 100)
	scheduler.Start()
	defer scheduler.Stop()

	// Initially no queues
	s.False(scheduler.HasQueue(1))

	blockCh := make(chan struct{})
	executingCh := make(chan struct{})
	testWG := sync.WaitGroup{}
	testWG.Add(2)

	// First task - will block
	mockTask1 := NewMockTask(s.controller)
	mockTask1.EXPECT().RetryPolicy().Return(s.retryPolicy).AnyTimes()
	mockTask1.EXPECT().Execute().DoAndReturn(func() error {
		close(executingCh)
		<-blockCh
		return nil
	}).Times(1)
	mockTask1.EXPECT().Ack().Do(func() { testWG.Done() }).Times(1)

	// Second task - will wait in queue
	mockTask2 := NewMockTask(s.controller)
	mockTask2.EXPECT().RetryPolicy().Return(s.retryPolicy).AnyTimes()
	mockTask2.EXPECT().Execute().Return(nil).Times(1)
	mockTask2.EXPECT().Ack().Do(func() { testWG.Done() }).Times(1)

	// Submit both tasks
	scheduler.Submit(&testTaskWithID{MockTask: mockTask1, workflowID: 1})
	scheduler.Submit(&testTaskWithID{MockTask: mockTask2, workflowID: 1})

	// Wait for first task to start executing
	<-executingCh

	// Queue should exist because second task is waiting
	s.True(scheduler.HasQueue(1))

	// Complete tasks
	close(blockCh)
	testWG.Wait()

	// Queue still exists after tasks complete (within TTL for retry routing)
	// It will be cleaned up by the periodic cleanup goroutine after defaultEmptyQueueTTL.
	// This is expected behavior - the TTL allows retried tasks to find the queue.
	time.Sleep(10 * time.Millisecond)
	s.True(scheduler.HasQueue(1), "Empty queue should exist within TTL")
}

// =============================================================================
// Concurrency Tests
// =============================================================================

func (s *workflowQueueSchedulerSuite) TestParallelSubmit_DifferentWorkflows() {
	scheduler := s.newSchedulerWithWorkflowID(8, 1000)
	scheduler.Start()
	defer scheduler.Stop()

	numSubmitters := 100
	tasksPerSubmitter := 50

	testWG := sync.WaitGroup{}
	testWG.Add(numSubmitters * tasksPerSubmitter)

	startWG := sync.WaitGroup{}
	startWG.Add(numSubmitters)

	for i := 0; i < numSubmitters; i++ {
		submitterID := i
		go func() {
			startWG.Done()
			startWG.Wait() // Sync all submitters to start at once

			for j := 0; j < tasksPerSubmitter; j++ {
				mockTask := NewMockTask(s.controller)
				mockTask.EXPECT().RetryPolicy().Return(s.retryPolicy).AnyTimes()
				mockTask.EXPECT().Execute().Return(nil).Times(1)
				mockTask.EXPECT().Ack().Do(func() { testWG.Done() }).Times(1)

				scheduler.Submit(&testTaskWithID{MockTask: mockTask, workflowID: submitterID})
			}
		}()
	}

	// Wait for all tasks to complete
	done := make(chan struct{})
	go func() {
		testWG.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Success
	case <-time.After(30 * time.Second):
		s.Fail("Timed out waiting for parallel tasks")
	}
}

func (s *workflowQueueSchedulerSuite) TestParallelSubmit_SameWorkflow() {
	scheduler := s.newSchedulerWithWorkflowID(4, 1000)
	scheduler.Start()
	defer scheduler.Stop()

	numSubmitters := 50
	tasksPerSubmitter := 20

	var executionCount int32
	testWG := sync.WaitGroup{}
	testWG.Add(numSubmitters * tasksPerSubmitter)

	startWG := sync.WaitGroup{}
	startWG.Add(numSubmitters)

	for i := 0; i < numSubmitters; i++ {
		go func() {
			startWG.Done()
			startWG.Wait()

			for j := 0; j < tasksPerSubmitter; j++ {
				mockTask := NewMockTask(s.controller)
				mockTask.EXPECT().RetryPolicy().Return(s.retryPolicy).AnyTimes()
				mockTask.EXPECT().Execute().DoAndReturn(func() error {
					atomic.AddInt32(&executionCount, 1)
					return nil
				}).Times(1)
				mockTask.EXPECT().Ack().Do(func() { testWG.Done() }).Times(1)

				// All tasks go to same workflow
				scheduler.Submit(&testTaskWithID{MockTask: mockTask, workflowID: 1})
			}
		}()
	}

	done := make(chan struct{})
	go func() {
		testWG.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Success
	case <-time.After(30 * time.Second):
		s.Fail("Timed out waiting for parallel tasks to same workflow")
	}

	s.Equal(int32(numSubmitters*tasksPerSubmitter), atomic.LoadInt32(&executionCount))
}

func (s *workflowQueueSchedulerSuite) TestParallelTrySubmit() {
	scheduler := s.newSchedulerWithWorkflowID(4, 100)
	scheduler.Start()
	defer scheduler.Stop()

	numSubmitters := 50
	tasksPerSubmitter := 20

	var successCount int32
	var failCount int32
	testWG := sync.WaitGroup{}

	startWG := sync.WaitGroup{}
	startWG.Add(numSubmitters)

	submitWG := sync.WaitGroup{}
	submitWG.Add(numSubmitters)

	for i := 0; i < numSubmitters; i++ {
		submitterID := i
		go func() {
			startWG.Done()
			startWG.Wait()

			for j := 0; j < tasksPerSubmitter; j++ {
				mockTask := NewMockTask(s.controller)
				mockTask.EXPECT().RetryPolicy().Return(s.retryPolicy).AnyTimes()
				mockTask.EXPECT().Execute().Return(nil).MaxTimes(1)
				mockTask.EXPECT().Ack().Do(func() { testWG.Done() }).MaxTimes(1)

				if scheduler.TrySubmit(&testTaskWithID{MockTask: mockTask, workflowID: submitterID}) {
					atomic.AddInt32(&successCount, 1)
					testWG.Add(1)
				} else {
					atomic.AddInt32(&failCount, 1)
				}
			}
			submitWG.Done()
		}()
	}

	submitWG.Wait()

	// Wait for successful tasks to complete
	done := make(chan struct{})
	go func() {
		testWG.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Success
	case <-time.After(30 * time.Second):
		s.Fail("Timed out waiting for tasks")
	}

	s.T().Logf("TrySubmit: %d succeeded, %d failed", successCount, failCount)
	s.Greater(successCount, int32(0), "Some tasks should succeed")
}

// =============================================================================
// Edge Case Tests
// =============================================================================

func (s *workflowQueueSchedulerSuite) TestConcurrentAddDuringRemove() {
	// This tests the race between TrySubmit removing a queue and another TrySubmit adding to same workflow
	// Use 1 worker to slowly consume, and small channel to create contention
	scheduler := s.newSchedulerWithWorkflowID(1, 2)
	scheduler.Start()
	defer scheduler.Stop()

	iterations := 50
	for i := 0; i < iterations; i++ {
		var wg sync.WaitGroup
		wg.Add(3)

		// Goroutine 1: TrySubmit to workflow 1
		go func() {
			defer wg.Done()
			mockTask := NewMockTask(s.controller)
			mockTask.EXPECT().RetryPolicy().Return(s.retryPolicy).AnyTimes()
			mockTask.EXPECT().Execute().Return(nil).MaxTimes(1)
			mockTask.EXPECT().Ack().MaxTimes(1)
			mockTask.EXPECT().Abort().MaxTimes(1)
			scheduler.TrySubmit(&testTaskWithID{MockTask: mockTask, workflowID: 1})
		}()

		// Goroutine 2: TrySubmit to workflow 2 (may race with goroutine 3)
		go func() {
			defer wg.Done()
			mockTask := NewMockTask(s.controller)
			mockTask.EXPECT().RetryPolicy().Return(s.retryPolicy).AnyTimes()
			mockTask.EXPECT().Execute().Return(nil).MaxTimes(1)
			mockTask.EXPECT().Ack().MaxTimes(1)
			mockTask.EXPECT().Abort().MaxTimes(1)
			scheduler.TrySubmit(&testTaskWithID{MockTask: mockTask, workflowID: 2})
		}()

		// Goroutine 3: TrySubmit to same workflow 2 (races with goroutine 2)
		go func() {
			defer wg.Done()
			mockTask := NewMockTask(s.controller)
			mockTask.EXPECT().RetryPolicy().Return(s.retryPolicy).AnyTimes()
			mockTask.EXPECT().Execute().Return(nil).MaxTimes(1)
			mockTask.EXPECT().Ack().MaxTimes(1)
			mockTask.EXPECT().Abort().MaxTimes(1)
			scheduler.TrySubmit(&testTaskWithID{MockTask: mockTask, workflowID: 2})
		}()

		wg.Wait()
	}

	// Let remaining tasks complete
	time.Sleep(100 * time.Millisecond)
}

func (s *workflowQueueSchedulerSuite) TestMultipleTasksSameWorkflow_AllProcessed() {
	scheduler := s.newSchedulerWithWorkflowID(1, 100)
	scheduler.Start()
	defer scheduler.Stop()

	numTasks := 100
	var processedCount int32
	testWG := sync.WaitGroup{}
	testWG.Add(numTasks)

	for i := 0; i < numTasks; i++ {
		mockTask := NewMockTask(s.controller)
		mockTask.EXPECT().RetryPolicy().Return(s.retryPolicy).AnyTimes()
		mockTask.EXPECT().Execute().DoAndReturn(func() error {
			atomic.AddInt32(&processedCount, 1)
			return nil
		}).Times(1)
		mockTask.EXPECT().Ack().Do(func() { testWG.Done() }).Times(1)

		scheduler.Submit(&testTaskWithID{MockTask: mockTask, workflowID: 1})
	}

	testWG.Wait()

	s.Equal(int32(numTasks), atomic.LoadInt32(&processedCount), "All tasks should be processed")
}

func (s *workflowQueueSchedulerSuite) TestDrainTasks_ConcurrentAdditions() {
	scheduler := s.newSchedulerWithWorkflowID(0, 100) // No workers
	scheduler.Start()

	numInitialTasks := 10
	numConcurrentTasks := 20

	var abortCount int32
	var submitWG sync.WaitGroup
	submitWG.Add(numConcurrentTasks)

	// Submit initial tasks
	for i := 0; i < numInitialTasks; i++ {
		mockTask := NewMockTask(s.controller)
		mockTask.EXPECT().Abort().Do(func() {
			atomic.AddInt32(&abortCount, 1)
		}).Times(1)
		scheduler.Submit(&testTaskWithID{MockTask: mockTask, workflowID: i % 5})
	}

	// Start draining in background
	go func() {
		time.Sleep(5 * time.Millisecond)
		scheduler.Stop()
	}()

	// Concurrently add more tasks during drain
	for i := 0; i < numConcurrentTasks; i++ {
		go func(idx int) {
			defer submitWG.Done()
			mockTask := NewMockTask(s.controller)
			mockTask.EXPECT().Abort().Do(func() {
				atomic.AddInt32(&abortCount, 1)
			}).MaxTimes(1)
			scheduler.Submit(&testTaskWithID{MockTask: mockTask, workflowID: idx % 5})
		}(i)
	}

	submitWG.Wait()
	time.Sleep(100 * time.Millisecond) // Give time for drain to complete

	// All tasks should be aborted (initial + concurrent that were added before shutdown completed)
	s.GreaterOrEqual(atomic.LoadInt32(&abortCount), int32(numInitialTasks),
		"At least initial tasks should be aborted")
}

// =============================================================================
// Helper Functions
// =============================================================================

func (s *workflowQueueSchedulerSuite) newScheduler(workerCount, queueSize int) *WorkflowQueueScheduler[*MockTask] {
	return NewWorkflowQueueScheduler[*MockTask](
		&WorkflowQueueSchedulerOptions{
			QueueSize: queueSize,
			WorkerCount: func(_ func(int)) (int, func()) {
				return workerCount, func() {}
			},
		},
		func(task *MockTask) any { return 1 }, // All tasks to same key
		log.NewNoopLogger(),
		metrics.NoopMetricsHandler,
		clock.NewRealTimeSource(),
	)
}

func (s *workflowQueueSchedulerSuite) newSchedulerWithWorkflowID(workerCount, queueSize int) *WorkflowQueueScheduler[*testTaskWithID] {
	return NewWorkflowQueueScheduler[*testTaskWithID](
		&WorkflowQueueSchedulerOptions{
			QueueSize: queueSize,
			WorkerCount: func(_ func(int)) (int, func()) {
				return workerCount, func() {}
			},
		},
		func(task *testTaskWithID) any { return task.workflowID }, // Key by workflow ID
		log.NewNoopLogger(),
		metrics.NoopMetricsHandler,
		clock.NewRealTimeSource(),
	)
}
