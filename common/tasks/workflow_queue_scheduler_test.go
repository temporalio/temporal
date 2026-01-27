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
	s.scheduler = s.newScheduler(100)
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
	scheduler := s.newSchedulerWithWorkflowID(100)
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
	scheduler := s.newSchedulerWithWorkflowID(100)
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
	scheduler := s.newSchedulerWithWorkflowID(100)
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
	scheduler := s.newSchedulerWithWorkflowIDAndOptions(1, 1, time.Hour) // queue size 1, long TTL
	scheduler.Start()

	// First submit should succeed and create queue
	blockCh := make(chan struct{})
	mockTask1 := NewMockTask(s.controller)
	mockTask1.EXPECT().RetryPolicy().Return(s.retryPolicy).AnyTimes()
	mockTask1.EXPECT().Execute().DoAndReturn(func() error {
		<-blockCh
		return nil
	}).MaxTimes(1)
	mockTask1.EXPECT().Ack().MaxTimes(1)
	mockTask1.EXPECT().Abort().MaxTimes(1)
	result1 := scheduler.TrySubmit(&testTaskWithID{MockTask: mockTask1, workflowID: 1})
	s.True(result1)

	// Wait for goroutine to start processing
	time.Sleep(10 * time.Millisecond)

	// Fill the queue (buffer size is 1)
	mockTask2 := NewMockTask(s.controller)
	mockTask2.EXPECT().RetryPolicy().Return(s.retryPolicy).AnyTimes()
	mockTask2.EXPECT().Execute().Return(nil).MaxTimes(1)
	mockTask2.EXPECT().Ack().MaxTimes(1)
	mockTask2.EXPECT().Abort().MaxTimes(1)
	result2 := scheduler.TrySubmit(&testTaskWithID{MockTask: mockTask2, workflowID: 1})
	s.True(result2)

	// Third submit should fail (channel full)
	mockTask3 := NewMockTask(s.controller)
	result3 := scheduler.TrySubmit(&testTaskWithID{MockTask: mockTask3, workflowID: 1})
	s.False(result3)

	// Clean up
	close(blockCh)
	scheduler.Stop()
}

// =============================================================================
// Shutdown Tests
// =============================================================================

func (s *workflowQueueSchedulerSuite) TestShutdown_DrainTasks() {
	// Test that tasks submitted after Stop() are aborted
	scheduler := s.newSchedulerWithWorkflowID(100)
	scheduler.Start()

	testWG := sync.WaitGroup{}
	testWG.Add(1)

	// Submit a task that will complete
	mockTask1 := NewMockTask(s.controller)
	mockTask1.EXPECT().RetryPolicy().Return(s.retryPolicy).AnyTimes()
	mockTask1.EXPECT().Execute().Return(nil).Times(1)
	mockTask1.EXPECT().Ack().Do(func() { testWG.Done() }).Times(1)
	scheduler.Submit(&testTaskWithID{MockTask: mockTask1, workflowID: 1})
	testWG.Wait()

	// Stop the scheduler
	scheduler.Stop()

	// Tasks submitted after stop should be aborted
	mockTask2 := NewMockTask(s.controller)
	mockTask2.EXPECT().Abort().Times(1)
	scheduler.Submit(&testTaskWithID{MockTask: mockTask2, workflowID: 1})
}

func (s *workflowQueueSchedulerSuite) TestSubmit_AfterShutdown() {
	scheduler := s.newScheduler(100)
	scheduler.Start()
	scheduler.Stop()

	mockTask := NewMockTask(s.controller)
	mockTask.EXPECT().Abort().Times(1)

	scheduler.Submit(mockTask)
}

// =============================================================================
// HasQueue Tests
// =============================================================================

func (s *workflowQueueSchedulerSuite) TestHasQueue() {
	scheduler := s.newSchedulerWithWorkflowID(100)
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

	// Queue should exist while task is executing
	s.True(scheduler.HasQueue(1))

	// Complete task
	close(blockCh)
	testWG.Wait()

	// Queue still exists within TTL
	s.True(scheduler.HasQueue(1))
}

func (s *workflowQueueSchedulerSuite) TestHasQueue_MultipleTasksInQueue() {
	scheduler := s.newSchedulerWithWorkflowID(100)
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

	// Queue still exists within TTL
	s.True(scheduler.HasQueue(1))
}

// =============================================================================
// Queue TTL Tests
// =============================================================================

func (s *workflowQueueSchedulerSuite) TestQueueTTL_ExpiresAfterIdle() {
	// Use very short TTL for testing
	scheduler := s.newSchedulerWithWorkflowIDAndOptions(100, 500, 50*time.Millisecond)
	scheduler.Start()
	defer scheduler.Stop()

	testWG := sync.WaitGroup{}
	testWG.Add(1)

	mockTask := NewMockTask(s.controller)
	mockTask.EXPECT().RetryPolicy().Return(s.retryPolicy).AnyTimes()
	mockTask.EXPECT().Execute().Return(nil).Times(1)
	mockTask.EXPECT().Ack().Do(func() { testWG.Done() }).Times(1)

	scheduler.Submit(&testTaskWithID{MockTask: mockTask, workflowID: 1})
	testWG.Wait()

	// Queue exists immediately after task completes
	s.True(scheduler.HasQueue(1))

	// Wait for TTL to expire
	time.Sleep(100 * time.Millisecond)

	// Queue should be gone after TTL
	s.False(scheduler.HasQueue(1))
}

// =============================================================================
// Max Queues Tests
// =============================================================================

func (s *workflowQueueSchedulerSuite) TestMaxQueues_RejectsNewQueues() {
	// Create scheduler with max 2 queues
	scheduler := NewWorkflowQueueScheduler(
		&WorkflowQueueSchedulerOptions{
			QueueSize: 100,
			MaxQueues: 2,
			QueueTTL:  time.Hour,
		},
		func(task *testTaskWithID) any { return task.workflowID },
		log.NewNoopLogger(),
		metrics.NoopMetricsHandler,
		clock.NewRealTimeSource(),
	)
	scheduler.Start()
	defer scheduler.Stop()

	// Block tasks so queues stay active
	blockCh := make(chan struct{})
	defer close(blockCh)

	for i := 0; i < 2; i++ {
		mockTask := NewMockTask(s.controller)
		mockTask.EXPECT().RetryPolicy().Return(s.retryPolicy).AnyTimes()
		mockTask.EXPECT().Execute().DoAndReturn(func() error {
			<-blockCh
			return nil
		}).MaxTimes(1)
		mockTask.EXPECT().Ack().MaxTimes(1)
		mockTask.EXPECT().Abort().MaxTimes(1)

		result := scheduler.TrySubmit(&testTaskWithID{MockTask: mockTask, workflowID: i})
		s.True(result, "First 2 queues should be created")
	}

	// Third queue should be rejected
	mockTask3 := NewMockTask(s.controller)
	result := scheduler.TrySubmit(&testTaskWithID{MockTask: mockTask3, workflowID: 99})
	s.False(result, "Third queue should be rejected")
}

// =============================================================================
// Concurrency Tests
// =============================================================================

func (s *workflowQueueSchedulerSuite) TestParallelSubmit_DifferentWorkflows() {
	scheduler := s.newSchedulerWithWorkflowID(1000)
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
	scheduler := s.newSchedulerWithWorkflowID(1000)
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
	scheduler := s.newSchedulerWithWorkflowID(100)
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

func (s *workflowQueueSchedulerSuite) TestMultipleTasksSameWorkflow_AllProcessed() {
	scheduler := s.newSchedulerWithWorkflowID(100)
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

// =============================================================================
// Helper Functions
// =============================================================================

func (s *workflowQueueSchedulerSuite) newScheduler(queueSize int) *WorkflowQueueScheduler[*MockTask] {
	return NewWorkflowQueueScheduler[*MockTask](
		&WorkflowQueueSchedulerOptions{
			QueueSize: queueSize,
			MaxQueues: 500,
			QueueTTL:  5 * time.Second,
		},
		func(task *MockTask) any { return 1 }, // All tasks to same key
		log.NewNoopLogger(),
		metrics.NoopMetricsHandler,
		clock.NewRealTimeSource(),
	)
}

func (s *workflowQueueSchedulerSuite) newSchedulerWithWorkflowID(queueSize int) *WorkflowQueueScheduler[*testTaskWithID] {
	return NewWorkflowQueueScheduler[*testTaskWithID](
		&WorkflowQueueSchedulerOptions{
			QueueSize: queueSize,
			MaxQueues: 500,
			QueueTTL:  5 * time.Second,
		},
		func(task *testTaskWithID) any { return task.workflowID }, // Key by workflow ID
		log.NewNoopLogger(),
		metrics.NoopMetricsHandler,
		clock.NewRealTimeSource(),
	)
}

func (s *workflowQueueSchedulerSuite) newSchedulerWithWorkflowIDAndOptions(queueSize, maxQueues int, queueTTL time.Duration) *WorkflowQueueScheduler[*testTaskWithID] {
	return NewWorkflowQueueScheduler[*testTaskWithID](
		&WorkflowQueueSchedulerOptions{
			QueueSize: queueSize,
			MaxQueues: maxQueues,
			QueueTTL:  queueTTL,
		},
		func(task *testTaskWithID) any { return task.workflowID }, // Key by workflow ID
		log.NewNoopLogger(),
		metrics.NoopMetricsHandler,
		clock.NewRealTimeSource(),
	)
}
