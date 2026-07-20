package tasks

import (
	"errors"
	"fmt"
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
	executionQueueSchedulerSuite struct {
		*require.Assertions
		suite.Suite

		controller *gomock.Controller

		scheduler   *executionQueueScheduler[*MockTask]
		retryPolicy backoff.RetryPolicy
	}

	// testExecutionTask is a wrapper that provides an execution key for queue routing
	testExecutionTask struct {
		*MockTask
		workflowID string
		runID      string
	}

	testExecutionKey struct {
		workflowID string
		runID      string
	}
)

func executionKeyFn(task *testExecutionTask) any {
	return testExecutionKey{workflowID: task.workflowID, runID: task.runID}
}

func TestExecutionQueueSchedulerSuite(t *testing.T) {
	s := new(executionQueueSchedulerSuite)
	suite.Run(t, s)
}

func (s *executionQueueSchedulerSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.controller = gomock.NewController(s.T())
	s.retryPolicy = backoff.NewExponentialRetryPolicy(time.Millisecond)
	s.scheduler = s.newScheduler()
	s.scheduler.Start()
}

func (s *executionQueueSchedulerSuite) TearDownTest() {
	s.scheduler.Stop()
	s.controller.Finish()
}

// =============================================================================
// Basic Tests
// =============================================================================

func (s *executionQueueSchedulerSuite) TestTrySubmit_Failure() {
	testWG := sync.WaitGroup{}
	testWG.Add(1)

	mockTask := NewMockTask(s.controller)
	mockTask.EXPECT().RetryPolicy().Return(s.retryPolicy).AnyTimes()
	executionErr := errors.New("execution error")
	mockTask.EXPECT().Execute().Return(executionErr).Times(1)
	mockTask.EXPECT().HandleErr(executionErr).Return(executionErr).Times(1)
	mockTask.EXPECT().IsRetryableError(executionErr).Return(false).MaxTimes(1)
	mockTask.EXPECT().Nack(executionErr).Do(func(_ error) { testWG.Done() }).Times(1)

	s.scheduler.TrySubmit(mockTask)

	testWG.Wait()
}

func (s *executionQueueSchedulerSuite) TestTrySubmit_RetryThenSuccess() {
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

	s.scheduler.TrySubmit(mockTask)

	testWG.Wait()
}

// =============================================================================
// Sequential Execution Tests
// =============================================================================

func (s *executionQueueSchedulerSuite) TestSequentialExecution_SameWorkflow() {
	scheduler := s.newSchedulerWithExecution(500, 5*time.Second)
	scheduler.Start()
	defer scheduler.Stop()

	numTasks := 10
	executionOrder := make([]int, 0, numTasks)
	var orderMu sync.Mutex
	testWG := sync.WaitGroup{}
	testWG.Add(numTasks)

	for i := range numTasks {
		taskIndex := i
		mockTask := NewMockTask(s.controller)
		mockTask.EXPECT().RetryPolicy().Return(s.retryPolicy).AnyTimes()
		mockTask.EXPECT().Execute().DoAndReturn(func() error {
			orderMu.Lock()
			executionOrder = append(executionOrder, taskIndex)
			orderMu.Unlock()
			return nil
		}).Times(1)
		mockTask.EXPECT().Ack().Do(func() { testWG.Done() }).Times(1)

		// All tasks have same workflow ID (1)
		scheduler.TrySubmit(&testExecutionTask{MockTask: mockTask, workflowID: "wf1", runID: "run1"})
	}

	testWG.Wait()

	// Verify tasks executed in order
	s.Len(executionOrder, numTasks)
	for i := range numTasks {
		s.Equal(i, executionOrder[i], "Tasks should execute in submission order")
	}
}

func (s *executionQueueSchedulerSuite) TestSequentialExecution_DifferentWorkflows() {
	scheduler := s.newSchedulerWithExecution(500, 5*time.Second)
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
	for i := range numWorkflows {
		executionOrders[i] = &workflowOrders{orders: make([]int, 0, tasksPerWorkflow)}
	}

	testWG := sync.WaitGroup{}
	testWG.Add(numWorkflows * tasksPerWorkflow)

	for wf := range numWorkflows {
		for t := range tasksPerWorkflow {
			workflowID := wf
			taskIndex := t
			mockTask := NewMockTask(s.controller)
			mockTask.EXPECT().RetryPolicy().Return(s.retryPolicy).AnyTimes()
			mockTask.EXPECT().Execute().DoAndReturn(func() error {
				executionOrders[workflowID].mu.Lock()
				executionOrders[workflowID].orders = append(executionOrders[workflowID].orders, taskIndex)
				executionOrders[workflowID].mu.Unlock()
				return nil
			}).Times(1)
			mockTask.EXPECT().Ack().Do(func() { testWG.Done() }).Times(1)

			scheduler.TrySubmit(&testExecutionTask{MockTask: mockTask, workflowID: fmt.Sprintf("%d", workflowID), runID: "1"})
		}
	}

	testWG.Wait()

	// Verify each workflow's tasks executed in order
	for wf := range numWorkflows {
		s.Len(executionOrders[wf].orders, tasksPerWorkflow)
		for i := range tasksPerWorkflow {
			s.Equal(i, executionOrders[wf].orders[i], "Workflow %d tasks should execute in order", wf)
		}
	}
}

// =============================================================================
// TrySubmit Tests
// =============================================================================

func (s *executionQueueSchedulerSuite) TestTrySubmit_Success() {
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

func (s *executionQueueSchedulerSuite) TestTrySubmit_ExistingQueue() {
	// Create a scheduler that blocks task execution so we can add to existing queue
	scheduler := s.newSchedulerWithExecution(500, 5*time.Second)
	scheduler.Start()
	defer scheduler.Stop()

	blockCh := make(chan struct{})
	blockStarted := make(chan struct{})
	testWG := sync.WaitGroup{}
	testWG.Add(2)

	// First task - will block
	mockTask1 := NewMockTask(s.controller)
	mockTask1.EXPECT().RetryPolicy().Return(s.retryPolicy).AnyTimes()
	mockTask1.EXPECT().Execute().DoAndReturn(func() error {
		close(blockStarted)
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
	result1 := scheduler.TrySubmit(&testExecutionTask{MockTask: mockTask1, workflowID: "wf1", runID: "run1"})
	s.True(result1)

	// Wait for first task to start executing
	<-blockStarted

	// Submit second task to same workflow - should succeed (existing queue)
	result2 := scheduler.TrySubmit(&testExecutionTask{MockTask: mockTask2, workflowID: "wf1", runID: "run1"})
	s.True(result2)

	// Verify queue exists
	s.True(scheduler.HasQueue(testExecutionKey{workflowID: "wf1", runID: "run1"}))

	// Unblock first task
	close(blockCh)

	testWG.Wait()
}

// =============================================================================
// Shutdown Tests
// =============================================================================

func (s *executionQueueSchedulerSuite) TestShutdown_DrainTasks() {
	// Test that tasks submitted after Stop() are aborted
	scheduler := s.newSchedulerWithExecution(500, 5*time.Second)
	scheduler.Start()

	testWG := sync.WaitGroup{}
	testWG.Add(1)

	// Submit a task that will complete
	mockTask1 := NewMockTask(s.controller)
	mockTask1.EXPECT().RetryPolicy().Return(s.retryPolicy).AnyTimes()
	mockTask1.EXPECT().Execute().Return(nil).Times(1)
	mockTask1.EXPECT().Ack().Do(func() { testWG.Done() }).Times(1)
	scheduler.TrySubmit(&testExecutionTask{MockTask: mockTask1, workflowID: "wf1", runID: "run1"})
	testWG.Wait()

	// Stop the scheduler
	scheduler.Stop()

	// Tasks submitted after stop should be aborted
	mockTask2 := NewMockTask(s.controller)
	mockTask2.EXPECT().Abort().Times(1)
	scheduler.TrySubmit(&testExecutionTask{MockTask: mockTask2, workflowID: "wf1", runID: "run1"})
}

// =============================================================================
// HasQueue Tests
// =============================================================================

func (s *executionQueueSchedulerSuite) TestHasQueue() {
	scheduler := s.newSchedulerWithExecution(500, 5*time.Second)
	scheduler.Start()
	defer scheduler.Stop()

	// Initially no queues
	s.False(scheduler.HasQueue(testExecutionKey{workflowID: "wf1", runID: "run1"}))

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
	scheduler.TrySubmit(&testExecutionTask{MockTask: mockTask, workflowID: "wf1", runID: "run1"})

	// Wait until task is actually executing
	<-executingCh

	// Queue should exist while task is executing
	s.True(scheduler.HasQueue(testExecutionKey{workflowID: "wf1", runID: "run1"}))

	// Complete task
	close(blockCh)
	testWG.Wait()

	// Queue still exists within TTL
	s.True(scheduler.HasQueue(testExecutionKey{workflowID: "wf1", runID: "run1"}))
}

// =============================================================================
// Queue TTL Tests
// =============================================================================

func (s *executionQueueSchedulerSuite) TestQueueTTL_ExpiresAfterIdle() {
	// Use very short TTL for testing.
	// The background sweeper runs every QueueTTL, so a queue may linger
	// up to 2x QueueTTL after becoming idle.
	ttl := 50 * time.Millisecond
	scheduler := s.newSchedulerWithExecution(500, ttl)
	scheduler.Start()
	defer scheduler.Stop()

	testWG := sync.WaitGroup{}
	testWG.Add(1)

	mockTask := NewMockTask(s.controller)
	mockTask.EXPECT().RetryPolicy().Return(s.retryPolicy).AnyTimes()
	mockTask.EXPECT().Execute().Return(nil).Times(1)
	mockTask.EXPECT().Ack().Do(func() { testWG.Done() }).Times(1)

	scheduler.TrySubmit(&testExecutionTask{MockTask: mockTask, workflowID: "wf1", runID: "run1"})
	testWG.Wait()

	// Queue exists immediately after task completes
	s.True(scheduler.HasQueue(testExecutionKey{workflowID: "wf1", runID: "run1"}))

	// Wait for sweeper to remove the idle queue
	s.Eventually(func() bool {
		return !scheduler.HasQueue(testExecutionKey{workflowID: "wf1", runID: "run1"})
	}, 5*time.Second, 10*time.Millisecond, "Queue should be removed after TTL")
}

func (s *executionQueueSchedulerSuite) TestTTLExpiryRace_NoTaskOrphaning() {
	// Regression test for Issue #3: tasks submitted near TTL expiry must not be orphaned.
	// Uses a very short TTL and repeatedly submits tasks right as queues expire,
	// verifying that every submitted task is eventually executed (Ack'd).
	ttl := 20 * time.Millisecond
	scheduler := s.newSchedulerWithExecution(500, ttl)
	scheduler.Start()
	defer scheduler.Stop()

	const iterations = 50
	var processedCount int32
	testWG := sync.WaitGroup{}

	for range iterations {
		testWG.Add(1)

		mockTask := NewMockTask(s.controller)
		mockTask.EXPECT().RetryPolicy().Return(s.retryPolicy).AnyTimes()
		mockTask.EXPECT().Execute().Return(nil).Times(1)
		mockTask.EXPECT().Ack().Do(func() {
			atomic.AddInt32(&processedCount, 1)
			testWG.Done()
		}).Times(1)

		// Submit, then wait for the queue to be removed by the sweeper before next submit.
		// This maximizes the chance of hitting the TTL expiry race.
		scheduler.TrySubmit(&testExecutionTask{MockTask: mockTask, workflowID: "wf1", runID: "run1"})

		// Wait for sweeper to remove the queue before next iteration
		s.Eventually(func() bool {
			return !scheduler.HasQueue(testExecutionKey{workflowID: "wf1", runID: "run1"})
		}, 5*time.Second, time.Millisecond)
	}

	// All tasks must be processed — no orphaning allowed
	done := make(chan struct{})
	go func() {
		testWG.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Success
	case <-time.After(30 * time.Second):
		s.Fail("Timed out waiting for tasks — possible task orphaning",
			"processed %d/%d tasks", atomic.LoadInt32(&processedCount), iterations)
	}

	s.Equal(int32(iterations), atomic.LoadInt32(&processedCount),
		"All tasks must be processed, none orphaned")
}

// =============================================================================
// Max Queues Tests
// =============================================================================

func (s *executionQueueSchedulerSuite) TestMaxQueues_RejectsNewQueues() {
	// Create scheduler with max 2 queues
	scheduler := newExecutionQueueScheduler(
		func() int { return 2 },
		func() time.Duration { return time.Hour },
		func() int { return 1 },
		executionKeyFn,
		log.NewNoopLogger(),
		metrics.NoopMetricsHandler,
		clock.NewRealTimeSource(),
	)
	scheduler.Start()
	defer scheduler.Stop()

	// Block tasks so queues stay active
	blockCh := make(chan struct{})
	defer close(blockCh)

	for i := range 2 {
		mockTask := NewMockTask(s.controller)
		mockTask.EXPECT().RetryPolicy().Return(s.retryPolicy).AnyTimes()
		mockTask.EXPECT().Execute().DoAndReturn(func() error {
			<-blockCh
			return nil
		}).MaxTimes(1)
		mockTask.EXPECT().Ack().MaxTimes(1)
		mockTask.EXPECT().Abort().MaxTimes(1)

		result := scheduler.TrySubmit(&testExecutionTask{MockTask: mockTask, workflowID: fmt.Sprintf("%d", i), runID: "1"})
		s.True(result, "First 2 queues should be created")
	}

	// Third queue should be rejected
	mockTask3 := NewMockTask(s.controller)
	result := scheduler.TrySubmit(&testExecutionTask{MockTask: mockTask3, workflowID: "wf99", runID: "run1"})
	s.False(result, "Third queue should be rejected")
}

// =============================================================================
// Concurrency Tests
// =============================================================================

func (s *executionQueueSchedulerSuite) TestParallelTrySubmit_DifferentWorkflows() {
	scheduler := s.newSchedulerWithExecution(500, 5*time.Second)
	scheduler.Start()
	defer scheduler.Stop()

	numSubmitters := 100
	tasksPerSubmitter := 50

	testWG := sync.WaitGroup{}
	testWG.Add(numSubmitters * tasksPerSubmitter)

	startWG := sync.WaitGroup{}
	startWG.Add(numSubmitters)

	for i := range numSubmitters {
		submitterID := i
		go func() {
			startWG.Done()
			startWG.Wait() // Sync all submitters to start at once

			for range tasksPerSubmitter {
				mockTask := NewMockTask(s.controller)
				mockTask.EXPECT().RetryPolicy().Return(s.retryPolicy).AnyTimes()
				mockTask.EXPECT().Execute().Return(nil).Times(1)
				mockTask.EXPECT().Ack().Do(func() { testWG.Done() }).Times(1)

				scheduler.TrySubmit(&testExecutionTask{MockTask: mockTask, workflowID: fmt.Sprintf("%d", submitterID), runID: "1"})
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

func (s *executionQueueSchedulerSuite) TestParallelTrySubmit_SameWorkflow() {
	scheduler := s.newSchedulerWithExecution(500, 5*time.Second)
	scheduler.Start()
	defer scheduler.Stop()

	numSubmitters := 50
	tasksPerSubmitter := 20

	var executionCount int32
	testWG := sync.WaitGroup{}
	testWG.Add(numSubmitters * tasksPerSubmitter)

	startWG := sync.WaitGroup{}
	startWG.Add(numSubmitters)

	for range numSubmitters {
		go func() {
			startWG.Done()
			startWG.Wait()

			for range tasksPerSubmitter {
				mockTask := NewMockTask(s.controller)
				mockTask.EXPECT().RetryPolicy().Return(s.retryPolicy).AnyTimes()
				mockTask.EXPECT().Execute().DoAndReturn(func() error {
					atomic.AddInt32(&executionCount, 1)
					return nil
				}).Times(1)
				mockTask.EXPECT().Ack().Do(func() { testWG.Done() }).Times(1)

				// All tasks go to same workflow
				scheduler.TrySubmit(&testExecutionTask{MockTask: mockTask, workflowID: "wf1", runID: "run1"})
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

// =============================================================================
// Helper Functions
// =============================================================================

func (s *executionQueueSchedulerSuite) newScheduler() *executionQueueScheduler[*MockTask] {
	return newExecutionQueueScheduler(
		func() int { return 500 },
		func() time.Duration { return 5 * time.Second },
		func() int { return 1 },
		func(task *MockTask) any { return 1 }, // All tasks to same key
		log.NewNoopLogger(),
		metrics.NoopMetricsHandler,
		clock.NewRealTimeSource(),
	)
}

func (s *executionQueueSchedulerSuite) newSchedulerWithExecution(maxQueues int, queueTTL time.Duration) *executionQueueScheduler[*testExecutionTask] {
	return newExecutionQueueScheduler(
		func() int { return maxQueues },
		func() time.Duration { return queueTTL },
		func() int { return 1 },
		executionKeyFn,
		log.NewNoopLogger(),
		metrics.NoopMetricsHandler,
		clock.NewRealTimeSource(),
	)
}
