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

		scheduler   *ExecutionQueueScheduler[*MockTask]
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
// Basic Submit Tests
// =============================================================================

func (s *executionQueueSchedulerSuite) TestSubmit_Success() {
	testWG := sync.WaitGroup{}
	testWG.Add(1)

	mockTask := NewMockTask(s.controller)
	mockTask.EXPECT().RetryPolicy().Return(s.retryPolicy).AnyTimes()
	mockTask.EXPECT().Execute().Return(nil).Times(1)
	mockTask.EXPECT().Ack().Do(func() { testWG.Done() }).Times(1)

	s.scheduler.Submit(mockTask)

	testWG.Wait()
}

func (s *executionQueueSchedulerSuite) TestSubmit_Failure() {
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

func (s *executionQueueSchedulerSuite) TestSubmit_Panic() {
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

func (s *executionQueueSchedulerSuite) TestSubmit_RetryThenSuccess() {
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

func (s *executionQueueSchedulerSuite) TestSequentialExecution_SameWorkflow() {
	scheduler := s.newSchedulerWithExecution(500, 5*time.Second)
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
		scheduler.Submit(&testExecutionTask{MockTask: mockTask, workflowID: "wf1", runID: "run1"})
	}

	testWG.Wait()

	// Verify tasks executed in order
	s.Len(executionOrder, numTasks)
	for i := 0; i < numTasks; i++ {
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

			scheduler.Submit(&testExecutionTask{MockTask: mockTask, workflowID: fmt.Sprintf("%d", workflowID), runID: "1"})
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
	scheduler.Submit(&testExecutionTask{MockTask: mockTask1, workflowID: "wf1", runID: "run1"})
	testWG.Wait()

	// Stop the scheduler
	scheduler.Stop()

	// Tasks submitted after stop should be aborted
	mockTask2 := NewMockTask(s.controller)
	mockTask2.EXPECT().Abort().Times(1)
	scheduler.Submit(&testExecutionTask{MockTask: mockTask2, workflowID: "wf1", runID: "run1"})
}

func (s *executionQueueSchedulerSuite) TestSubmit_AfterShutdown() {
	scheduler := s.newScheduler()
	scheduler.Start()
	scheduler.Stop()

	mockTask := NewMockTask(s.controller)
	mockTask.EXPECT().Abort().Times(1)

	scheduler.Submit(mockTask)
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
	scheduler.Submit(&testExecutionTask{MockTask: mockTask, workflowID: "wf1", runID: "run1"})

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

func (s *executionQueueSchedulerSuite) TestHasQueue_MultipleTasksInQueue() {
	scheduler := s.newSchedulerWithExecution(500, 5*time.Second)
	scheduler.Start()
	defer scheduler.Stop()

	// Initially no queues
	s.False(scheduler.HasQueue(testExecutionKey{workflowID: "wf1", runID: "run1"}))

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
	scheduler.Submit(&testExecutionTask{MockTask: mockTask1, workflowID: "wf1", runID: "run1"})
	scheduler.Submit(&testExecutionTask{MockTask: mockTask2, workflowID: "wf1", runID: "run1"})

	// Wait for first task to start executing
	<-executingCh

	// Queue should exist because second task is waiting
	s.True(scheduler.HasQueue(testExecutionKey{workflowID: "wf1", runID: "run1"}))

	// Complete tasks
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

	scheduler.Submit(&testExecutionTask{MockTask: mockTask, workflowID: "wf1", runID: "run1"})
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

	for i := 0; i < iterations; i++ {
		testWG.Add(1)

		mockTask := NewMockTask(s.controller)
		mockTask.EXPECT().RetryPolicy().Return(s.retryPolicy).AnyTimes()
		mockTask.EXPECT().Execute().Return(nil).Times(1)
		mockTask.EXPECT().Ack().Do(func() {
			atomic.AddInt32(&processedCount, 1)
			testWG.Done()
		}).Times(1)

		// Submit, then wait ~TTL to let the queue expire before next submit.
		// This maximizes the chance of hitting the TTL expiry race.
		scheduler.Submit(&testExecutionTask{MockTask: mockTask, workflowID: "wf1", runID: "run1"})

		// Wait for TTL to expire (queue removed) before next iteration
		time.Sleep(ttl + 10*time.Millisecond)
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

func (s *executionQueueSchedulerSuite) TestSubmit_BlocksUntilQueueAvailable() {
	// Create scheduler with max 1 queue and short TTL
	ttl := 50 * time.Millisecond
	scheduler := NewExecutionQueueScheduler(
		&ExecutionQueueSchedulerOptions{
			MaxQueues:        func() int { return 1 },
			QueueTTL:         func() time.Duration { return ttl },
			QueueConcurrency: func() int { return 1 },
		},
		executionKeyFn,
		log.NewNoopLogger(),
		metrics.NoopMetricsHandler,
		clock.NewRealTimeSource(),
	)
	scheduler.Start()
	defer scheduler.Stop()

	testWG := sync.WaitGroup{}
	testWG.Add(1)

	// Submit task for key A — creates queue, succeeds immediately
	mockTaskA := NewMockTask(s.controller)
	mockTaskA.EXPECT().RetryPolicy().Return(s.retryPolicy).AnyTimes()
	mockTaskA.EXPECT().Execute().Return(nil).Times(1)
	mockTaskA.EXPECT().Ack().Do(func() { testWG.Done() }).Times(1)
	scheduler.Submit(&testExecutionTask{MockTask: mockTaskA, workflowID: "wfA", runID: "run1"})
	testWG.Wait()

	// Key A's queue is now idle. Submit for key B should block until sweeper frees the slot.
	submitDone := make(chan struct{})
	testWG.Add(1)

	mockTaskB := NewMockTask(s.controller)
	mockTaskB.EXPECT().RetryPolicy().Return(s.retryPolicy).AnyTimes()
	mockTaskB.EXPECT().Execute().Return(nil).Times(1)
	mockTaskB.EXPECT().Ack().Do(func() { testWG.Done() }).Times(1)

	go func() {
		scheduler.Submit(&testExecutionTask{MockTask: mockTaskB, workflowID: "wfB", runID: "run1"})
		close(submitDone)
	}()

	// Verify Submit is blocked (hasn't returned yet)
	select {
	case <-submitDone:
		s.Fail("Submit should block when MaxQueues is reached")
	case <-time.After(20 * time.Millisecond):
		// Good — still blocked
	}

	// Wait for sweeper to free key A's queue, which should unblock key B's Submit
	select {
	case <-submitDone:
		// Good — unblocked after sweeper ran
	case <-time.After(5 * time.Second):
		s.Fail("Submit should unblock after sweeper frees a queue slot")
	}

	testWG.Wait()
}

func (s *executionQueueSchedulerSuite) TestSubmit_UnblocksOnStop() {
	// Create scheduler with max 1 queue and long TTL (sweeper won't help)
	scheduler := NewExecutionQueueScheduler(
		&ExecutionQueueSchedulerOptions{
			MaxQueues:        func() int { return 1 },
			QueueTTL:         func() time.Duration { return time.Hour },
			QueueConcurrency: func() int { return 1 },
		},
		executionKeyFn,
		log.NewNoopLogger(),
		metrics.NoopMetricsHandler,
		clock.NewRealTimeSource(),
	)
	scheduler.Start()

	blockCh := make(chan struct{})

	// Submit task for key A that blocks execution
	mockTaskA := NewMockTask(s.controller)
	mockTaskA.EXPECT().RetryPolicy().Return(s.retryPolicy).AnyTimes()
	mockTaskA.EXPECT().Execute().DoAndReturn(func() error {
		<-blockCh
		return nil
	}).MaxTimes(1)
	mockTaskA.EXPECT().Ack().MaxTimes(1)
	mockTaskA.EXPECT().Abort().MaxTimes(1)
	scheduler.Submit(&testExecutionTask{MockTask: mockTaskA, workflowID: "wfA", runID: "run1"})

	// Submit for key B should block (MaxQueues=1, key A's queue exists)
	submitDone := make(chan struct{})
	mockTaskB := NewMockTask(s.controller)
	mockTaskB.EXPECT().Abort().Times(1)

	go func() {
		scheduler.Submit(&testExecutionTask{MockTask: mockTaskB, workflowID: "wfB", runID: "run1"})
		close(submitDone)
	}()

	// Verify Submit is blocked
	select {
	case <-submitDone:
		s.Fail("Submit should block when MaxQueues is reached")
	case <-time.After(50 * time.Millisecond):
		// Good — still blocked
	}

	// Stop the scheduler — should unblock Submit and abort the task
	close(blockCh)
	scheduler.Stop()

	select {
	case <-submitDone:
		// Good — unblocked after Stop
	case <-time.After(5 * time.Second):
		s.Fail("Submit should unblock after Stop is called")
	}
}

func (s *executionQueueSchedulerSuite) TestSubmit_MultipleWaitersForSameKey() {
	// When two Submit callers block for the same key and one creates the queue,
	// the other should be woken and append to the existing queue.
	ttl := 50 * time.Millisecond
	scheduler := NewExecutionQueueScheduler(
		&ExecutionQueueSchedulerOptions{
			MaxQueues:        func() int { return 1 },
			QueueTTL:         func() time.Duration { return ttl },
			QueueConcurrency: func() int { return 1 },
		},
		executionKeyFn,
		log.NewNoopLogger(),
		metrics.NoopMetricsHandler,
		clock.NewRealTimeSource(),
	)
	scheduler.Start()
	defer scheduler.Stop()

	testWG := sync.WaitGroup{}
	testWG.Add(1)

	// Fill the one queue slot with key A
	mockTaskA := NewMockTask(s.controller)
	mockTaskA.EXPECT().RetryPolicy().Return(s.retryPolicy).AnyTimes()
	mockTaskA.EXPECT().Execute().Return(nil).Times(1)
	mockTaskA.EXPECT().Ack().Do(func() { testWG.Done() }).Times(1)
	scheduler.Submit(&testExecutionTask{MockTask: mockTaskA, workflowID: "wfA", runID: "run1"})
	testWG.Wait()

	// Now submit two tasks for key B — both should block, then both should complete
	testWG.Add(2)
	submitsDone := make(chan struct{})

	mockTaskB1 := NewMockTask(s.controller)
	mockTaskB1.EXPECT().RetryPolicy().Return(s.retryPolicy).AnyTimes()
	mockTaskB1.EXPECT().Execute().Return(nil).Times(1)
	mockTaskB1.EXPECT().Ack().Do(func() { testWG.Done() }).Times(1)

	mockTaskB2 := NewMockTask(s.controller)
	mockTaskB2.EXPECT().RetryPolicy().Return(s.retryPolicy).AnyTimes()
	mockTaskB2.EXPECT().Execute().Return(nil).Times(1)
	mockTaskB2.EXPECT().Ack().Do(func() { testWG.Done() }).Times(1)

	var submitsReturned atomic.Int32
	go func() {
		scheduler.Submit(&testExecutionTask{MockTask: mockTaskB1, workflowID: "wfB", runID: "run1"})
		if submitsReturned.Add(1) == 2 {
			close(submitsDone)
		}
	}()
	go func() {
		scheduler.Submit(&testExecutionTask{MockTask: mockTaskB2, workflowID: "wfB", runID: "run1"})
		if submitsReturned.Add(1) == 2 {
			close(submitsDone)
		}
	}()

	// Both submits should complete after sweeper frees key A's slot
	select {
	case <-submitsDone:
		// Good
	case <-time.After(5 * time.Second):
		s.Fail("Both Submit calls for the same key should unblock")
	}

	testWG.Wait()
}

func (s *executionQueueSchedulerSuite) TestMaxQueues_RejectsNewQueues() {
	// Create scheduler with max 2 queues
	scheduler := NewExecutionQueueScheduler(
		&ExecutionQueueSchedulerOptions{
			MaxQueues:        func() int { return 2 },
			QueueTTL:         func() time.Duration { return time.Hour },
			QueueConcurrency: func() int { return 1 },
		},
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

	for i := 0; i < 2; i++ {
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

func (s *executionQueueSchedulerSuite) TestParallelSubmit_DifferentWorkflows() {
	scheduler := s.newSchedulerWithExecution(500, 5*time.Second)
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

				scheduler.Submit(&testExecutionTask{MockTask: mockTask, workflowID: fmt.Sprintf("%d", submitterID), runID: "1"})
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

func (s *executionQueueSchedulerSuite) TestParallelSubmit_SameWorkflow() {
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
				scheduler.Submit(&testExecutionTask{MockTask: mockTask, workflowID: "wf1", runID: "run1"})
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

func (s *executionQueueSchedulerSuite) TestParallelTrySubmit() {
	scheduler := s.newSchedulerWithExecution(500, 5*time.Second)
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

				if scheduler.TrySubmit(&testExecutionTask{MockTask: mockTask, workflowID: fmt.Sprintf("%d", submitterID), runID: "1"}) {
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

func (s *executionQueueSchedulerSuite) TestMultipleTasksSameWorkflow_AllProcessed() {
	scheduler := s.newSchedulerWithExecution(500, 5*time.Second)
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

		scheduler.Submit(&testExecutionTask{MockTask: mockTask, workflowID: "wf1", runID: "run1"})
	}

	testWG.Wait()

	s.Equal(int32(numTasks), atomic.LoadInt32(&processedCount), "All tasks should be processed")
}

// =============================================================================
// Helper Functions
// =============================================================================

func (s *executionQueueSchedulerSuite) newScheduler() *ExecutionQueueScheduler[*MockTask] {
	return NewExecutionQueueScheduler(
		&ExecutionQueueSchedulerOptions{
			MaxQueues:        func() int { return 500 },
			QueueTTL:         func() time.Duration { return 5 * time.Second },
			QueueConcurrency: func() int { return 1 },
		},
		func(task *MockTask) any { return 1 }, // All tasks to same key
		log.NewNoopLogger(),
		metrics.NoopMetricsHandler,
		clock.NewRealTimeSource(),
	)
}

func (s *executionQueueSchedulerSuite) newSchedulerWithExecution(maxQueues int, queueTTL time.Duration) *ExecutionQueueScheduler[*testExecutionTask] {
	return NewExecutionQueueScheduler(
		&ExecutionQueueSchedulerOptions{
			MaxQueues:        func() int { return maxQueues },
			QueueTTL:         func() time.Duration { return queueTTL },
			QueueConcurrency: func() int { return 1 },
		},
		executionKeyFn,
		log.NewNoopLogger(),
		metrics.NoopMetricsHandler,
		clock.NewRealTimeSource(),
	)
}
