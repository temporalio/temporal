// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
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

package queues

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/tasks"
	"go.uber.org/mock/gomock"
)

type (
	executionAwareSchedulerSuite struct {
		*require.Assertions
		suite.Suite

		controller *gomock.Controller
		logger     log.Logger
	}
)

func TestExecutionAwareSchedulerSuite(t *testing.T) {
	s := new(executionAwareSchedulerSuite)
	suite.Run(t, s)
}

func (s *executionAwareSchedulerSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.controller = gomock.NewController(s.T())
	s.logger = log.NewNoopLogger()
}

func (s *executionAwareSchedulerSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *executionAwareSchedulerSuite) createMockBaseScheduler() *tasks.MockScheduler[Executable] {
	return tasks.NewMockScheduler[Executable](s.controller)
}

// =============================================================================
// Constructor Tests
// =============================================================================

func (s *executionAwareSchedulerSuite) TestNewExecutionAwareScheduler_DefaultQueueSize() {
	mockBaseScheduler := s.createMockBaseScheduler()
	scheduler := NewExecutionAwareScheduler(
		mockBaseScheduler,
		ExecutionAwareSchedulerOptions{
			EnableExecutionQueueScheduler:      func() bool { return false },
			ExecutionQueueSchedulerMaxQueues: func() int { return 500 },
			ExecutionQueueSchedulerQueueTTL:  func() time.Duration { return 5 * time.Second },
		},
		s.logger,
		metrics.NoopMetricsHandler,
		clock.NewRealTimeSource(),
	)
	s.NotNil(scheduler)
	s.NotNil(scheduler.executionQueueScheduler)
	s.NotNil(scheduler.baseScheduler)
}

func (s *executionAwareSchedulerSuite) TestNewExecutionAwareScheduler_CustomQueueSize() {
	mockBaseScheduler := s.createMockBaseScheduler()
	scheduler := NewExecutionAwareScheduler(
		mockBaseScheduler,
		ExecutionAwareSchedulerOptions{
			EnableExecutionQueueScheduler:      func() bool { return true },
			ExecutionQueueSchedulerMaxQueues: func() int { return 500 },
			ExecutionQueueSchedulerQueueTTL:  func() time.Duration { return 5 * time.Second },
		},
		s.logger,
		metrics.NoopMetricsHandler,
		clock.NewRealTimeSource(),
	)
	s.NotNil(scheduler)
}

// =============================================================================
// Start/Stop Tests
// =============================================================================

func (s *executionAwareSchedulerSuite) TestStart_WithExecutionQueueSchedulerEnabled() {
	scheduler, _ := s.createSchedulerWithMock(true)
	scheduler.Start()
	scheduler.Stop()
}

func (s *executionAwareSchedulerSuite) TestStart_AlwaysStartsExecutionQueueScheduler() {
	// Even when EnableExecutionQueueScheduler returns false, the ExecutionQueueScheduler
	// is always started to handle dynamic config changes from disabled to enabled.
	scheduler, _ := s.createSchedulerWithMock(false)
	scheduler.Start()
	scheduler.Stop()
}

func (s *executionAwareSchedulerSuite) TestStop_StopsBothSchedulers() {
	scheduler, _ := s.createSchedulerWithMock(true)
	scheduler.Start()
	scheduler.Stop()
}

// =============================================================================
// Submit Tests
// =============================================================================

func (s *executionAwareSchedulerSuite) TestSubmit_DelegatesToBaseWhenExecutionQueueSchedulerDisabled() {
	scheduler, mockBaseScheduler := s.createSchedulerWithMock(false)
	mockExec := s.createMockExecutable("ns1", "wf1", "run1")
	mockBaseScheduler.EXPECT().Submit(mockExec).Times(1)

	scheduler.Start()
	defer scheduler.Stop()

	scheduler.Submit(mockExec)
}

func (s *executionAwareSchedulerSuite) TestSubmit_RoutesToBaseWhenNoActiveQueue() {
	scheduler, mockBaseScheduler := s.createSchedulerWithMock(true)
	mockExec := s.createMockExecutable("ns1", "wf1", "run1")
	mockBaseScheduler.EXPECT().Submit(mockExec).Times(1)

	scheduler.Start()
	defer scheduler.Stop()

	scheduler.Submit(mockExec)
}

func (s *executionAwareSchedulerSuite) TestSubmit_RoutesToExecutionQueueSchedulerWhenActiveQueue() {
	mockBaseScheduler := s.createMockBaseScheduler()
	mockBaseScheduler.EXPECT().Start().Times(1)
	mockBaseScheduler.EXPECT().Stop().Times(1)

	scheduler := NewExecutionAwareScheduler(
		mockBaseScheduler,
		ExecutionAwareSchedulerOptions{
			EnableExecutionQueueScheduler:      func() bool { return true },
			ExecutionQueueSchedulerMaxQueues: func() int { return 500 },
			ExecutionQueueSchedulerQueueTTL:  func() time.Duration { return 5 * time.Second },
		},
		s.logger,
		metrics.NoopMetricsHandler,
		clock.NewRealTimeSource(),
	)
	scheduler.Start()
	defer scheduler.Stop()

	// Create an active queue by submitting via HandleBusyWorkflow
	mockExec1 := s.createMockExecutable("ns1", "wf1", "run1")
	mockExec1.EXPECT().RetryPolicy().Return(backoff.NewExponentialRetryPolicy(time.Millisecond)).AnyTimes()

	// Use channels to control execution timing
	execStarted := make(chan struct{})
	execContinue := make(chan struct{})
	var completionWG sync.WaitGroup
	completionWG.Add(3) // 3 tasks total
	mockExec1.EXPECT().Execute().DoAndReturn(func() error {
		close(execStarted)
		<-execContinue
		return nil
	}).Times(1)
	mockExec1.EXPECT().Ack().Do(func() { completionWG.Done() }).Times(1)

	// First, route to ExecutionQueueScheduler via HandleBusyWorkflow
	s.True(scheduler.HandleBusyWorkflow(mockExec1))

	// Wait for execution to start
	<-execStarted

	// Now there's an active queue, subsequent submit should route to ExecutionQueueScheduler
	// Add task 2 which will be queued behind task 1
	mockExec2 := s.createMockExecutable("ns1", "wf1", "run1")
	mockExec2.EXPECT().RetryPolicy().Return(backoff.NewExponentialRetryPolicy(time.Millisecond)).AnyTimes()
	mockExec2.EXPECT().Execute().Return(nil).Times(1)
	mockExec2.EXPECT().Ack().Do(func() { completionWG.Done() }).Times(1)

	// HandleBusyWorkflow instead of Submit since we want to add to workflow queue
	// (Submit checks HasQueue which may not return true if timing is wrong)
	s.True(scheduler.HandleBusyWorkflow(mockExec2))

	// Verify queue exists for this workflow before task1 completes
	mockExec3 := s.createMockExecutable("ns1", "wf1", "run1")
	s.True(scheduler.HasExecutionQueue(mockExec3))

	// Add a third task via Submit - should route to ExecutionQueueScheduler since queue exists
	mockExec4 := s.createMockExecutable("ns1", "wf1", "run1")
	mockExec4.EXPECT().RetryPolicy().Return(backoff.NewExponentialRetryPolicy(time.Millisecond)).AnyTimes()
	mockExec4.EXPECT().Execute().Return(nil).Times(1)
	mockExec4.EXPECT().Ack().Do(func() { completionWG.Done() }).Times(1)
	scheduler.Submit(mockExec4) // Should route to ExecutionQueueScheduler because queue exists

	// Let tasks complete
	close(execContinue)
	completionWG.Wait()
}

// =============================================================================
// TrySubmit Tests
// =============================================================================

func (s *executionAwareSchedulerSuite) TestTrySubmit_DelegatesToBaseWhenExecutionQueueSchedulerDisabled() {
	mockBaseScheduler := s.createMockBaseScheduler()
	mockExec := s.createMockExecutable("ns1", "wf1", "run1")

	mockBaseScheduler.EXPECT().Start().Times(1)
	mockBaseScheduler.EXPECT().Stop().Times(1)
	mockBaseScheduler.EXPECT().TrySubmit(mockExec).Return(true).Times(1)

	scheduler := NewExecutionAwareScheduler(
		mockBaseScheduler,
		ExecutionAwareSchedulerOptions{
			EnableExecutionQueueScheduler:      func() bool { return false },
			ExecutionQueueSchedulerMaxQueues: func() int { return 500 },
			ExecutionQueueSchedulerQueueTTL:  func() time.Duration { return 5 * time.Second },
		},
		s.logger,
		metrics.NoopMetricsHandler,
		clock.NewRealTimeSource(),
	)
	scheduler.Start()
	defer scheduler.Stop()

	s.True(scheduler.TrySubmit(mockExec))
}

func (s *executionAwareSchedulerSuite) TestTrySubmit_RoutesToBaseWhenNoActiveQueue() {
	mockBaseScheduler := s.createMockBaseScheduler()
	mockExec := s.createMockExecutable("ns1", "wf1", "run1")

	mockBaseScheduler.EXPECT().Start().Times(1)
	mockBaseScheduler.EXPECT().Stop().Times(1)
	mockBaseScheduler.EXPECT().TrySubmit(mockExec).Return(true).Times(1)

	scheduler := NewExecutionAwareScheduler(
		mockBaseScheduler,
		ExecutionAwareSchedulerOptions{
			EnableExecutionQueueScheduler:      func() bool { return true },
			ExecutionQueueSchedulerMaxQueues: func() int { return 500 },
			ExecutionQueueSchedulerQueueTTL:  func() time.Duration { return 5 * time.Second },
		},
		s.logger,
		metrics.NoopMetricsHandler,
		clock.NewRealTimeSource(),
	)
	scheduler.Start()
	defer scheduler.Stop()

	s.True(scheduler.TrySubmit(mockExec))
}

func (s *executionAwareSchedulerSuite) TestTrySubmit_AddsToExistingQueueSuccessfully() {
	// When TrySubmit is called for a workflow with an active queue,
	// it should route to ExecutionQueueScheduler and add to the existing queue (returning true)
	mockBaseScheduler := s.createMockBaseScheduler()
	mockBaseScheduler.EXPECT().Start().Times(1)
	mockBaseScheduler.EXPECT().Stop().Times(1)

	scheduler := NewExecutionAwareScheduler(
		mockBaseScheduler,
		ExecutionAwareSchedulerOptions{
			EnableExecutionQueueScheduler:      func() bool { return true },
			ExecutionQueueSchedulerMaxQueues: func() int { return 500 },
			ExecutionQueueSchedulerQueueTTL:  func() time.Duration { return 5 * time.Second },
		},
		s.logger,
		metrics.NoopMetricsHandler,
		clock.NewRealTimeSource(),
	)
	scheduler.Start()
	defer scheduler.Stop()

	// First task blocks execution - this creates a queue for wf1
	mockExec1 := s.createMockExecutable("ns1", "wf1", "run1")
	mockExec1.EXPECT().RetryPolicy().Return(backoff.NewExponentialRetryPolicy(time.Millisecond)).AnyTimes()
	execStarted := make(chan struct{})
	execContinue := make(chan struct{})
	var completionWG sync.WaitGroup
	completionWG.Add(3)
	mockExec1.EXPECT().Execute().DoAndReturn(func() error {
		close(execStarted)
		<-execContinue
		return nil
	}).Times(1)
	mockExec1.EXPECT().Ack().Do(func() { completionWG.Done() }).Times(1)
	s.True(scheduler.HandleBusyWorkflow(mockExec1))

	// Wait for first task to start executing
	<-execStarted

	// Add second task to keep queue active
	mockExec2 := s.createMockExecutable("ns1", "wf1", "run1")
	mockExec2.EXPECT().RetryPolicy().Return(backoff.NewExponentialRetryPolicy(time.Millisecond)).AnyTimes()
	mockExec2.EXPECT().Execute().Return(nil).Times(1)
	mockExec2.EXPECT().Ack().Do(func() { completionWG.Done() }).Times(1)
	scheduler.HandleBusyWorkflow(mockExec2)

	// TrySubmit for wf1 which has an active queue - should add to existing queue and succeed
	mockExec3 := s.createMockExecutable("ns1", "wf1", "run1")
	mockExec3.EXPECT().RetryPolicy().Return(backoff.NewExponentialRetryPolicy(time.Millisecond)).AnyTimes()
	mockExec3.EXPECT().Execute().Return(nil).Times(1)
	mockExec3.EXPECT().Ack().Do(func() { completionWG.Done() }).Times(1)

	// TrySubmit should return true because task was added to existing queue
	s.True(scheduler.TrySubmit(mockExec3))

	// Cleanup
	close(execContinue)
	completionWG.Wait()
}

func (s *executionAwareSchedulerSuite) TestTrySubmit_RoutesToBaseWhenMaxQueuesReached() {
	// When WQS is at max capacity and TrySubmit is called for a workflow
	// without an active queue, it should route to the base (FIFO) scheduler.
	mockBaseScheduler := s.createMockBaseScheduler()
	mockBaseScheduler.EXPECT().Start().Times(1)
	mockBaseScheduler.EXPECT().Stop().Times(1)

	// MaxQueues=1 so only one workflow can have a WQS queue
	scheduler := NewExecutionAwareScheduler(
		mockBaseScheduler,
		ExecutionAwareSchedulerOptions{
			EnableExecutionQueueScheduler:    func() bool { return true },
			ExecutionQueueSchedulerMaxQueues: func() int { return 1 },
			ExecutionQueueSchedulerQueueTTL:  func() time.Duration { return 5 * time.Second },
		},
		s.logger,
		metrics.NoopMetricsHandler,
		clock.NewRealTimeSource(),
	)
	scheduler.Start()
	defer scheduler.Stop()

	// First task blocks execution — fills the single WQS slot
	blockCh := make(chan struct{})
	execStarted := make(chan struct{})
	var completionWG sync.WaitGroup
	completionWG.Add(1)
	mockExec1 := s.createMockExecutable("ns1", "wf1", "run1")
	mockExec1.EXPECT().RetryPolicy().Return(backoff.NewExponentialRetryPolicy(time.Millisecond)).AnyTimes()
	mockExec1.EXPECT().Execute().DoAndReturn(func() error {
		close(execStarted)
		<-blockCh
		return nil
	}).MaxTimes(1)
	mockExec1.EXPECT().Ack().Do(func() { completionWG.Done() }).MaxTimes(1)
	mockExec1.EXPECT().Abort().MaxTimes(1)
	s.True(scheduler.HandleBusyWorkflow(mockExec1))

	// Wait for worker to pick up task
	<-execStarted

	// TrySubmit for a different workflow — WQS is full, should go to base scheduler
	mockExec2 := s.createMockExecutable("ns1", "wf2", "run2")
	mockBaseScheduler.EXPECT().TrySubmit(mockExec2).Return(true).Times(1)
	s.True(scheduler.TrySubmit(mockExec2))

	// Cleanup
	close(blockCh)
	completionWG.Wait()
}

// =============================================================================
// HandleBusyWorkflow Tests
// =============================================================================

func (s *executionAwareSchedulerSuite) TestHandleBusyWorkflow_ReturnsFalseWhenDisabled() {
	mockBaseScheduler := s.createMockBaseScheduler()
	mockExec := NewMockExecutable(s.controller)

	scheduler := NewExecutionAwareScheduler(
		mockBaseScheduler,
		ExecutionAwareSchedulerOptions{
			EnableExecutionQueueScheduler:      func() bool { return false },
			ExecutionQueueSchedulerMaxQueues: func() int { return 500 },
			ExecutionQueueSchedulerQueueTTL:  func() time.Duration { return 5 * time.Second },
		},
		s.logger,
		metrics.NoopMetricsHandler,
		clock.NewRealTimeSource(),
	)
	s.False(scheduler.HandleBusyWorkflow(mockExec))
}

func (s *executionAwareSchedulerSuite) TestHandleBusyWorkflow_SubmitsToExecutionQueueScheduler() {
	mockBaseScheduler := s.createMockBaseScheduler()
	mockBaseScheduler.EXPECT().Start().Times(1)
	mockBaseScheduler.EXPECT().Stop().Times(1)

	scheduler := NewExecutionAwareScheduler(
		mockBaseScheduler,
		ExecutionAwareSchedulerOptions{
			EnableExecutionQueueScheduler:      func() bool { return true },
			ExecutionQueueSchedulerMaxQueues: func() int { return 500 },
			ExecutionQueueSchedulerQueueTTL:  func() time.Duration { return 5 * time.Second },
		},
		s.logger,
		metrics.NoopMetricsHandler,
		clock.NewRealTimeSource(),
	)
	scheduler.Start()
	defer scheduler.Stop()

	var completionWG sync.WaitGroup
	completionWG.Add(1)
	mockExec := s.createMockExecutable("ns1", "wf1", "run1")
	mockExec.EXPECT().RetryPolicy().Return(backoff.NewExponentialRetryPolicy(time.Millisecond)).AnyTimes()
	mockExec.EXPECT().Execute().Return(nil).Times(1)
	mockExec.EXPECT().Ack().Do(func() { completionWG.Done() }).Times(1)

	s.True(scheduler.HandleBusyWorkflow(mockExec))
	completionWG.Wait()
}

func (s *executionAwareSchedulerSuite) TestHandleBusyWorkflow_FallsBackToBaseSchedulerWhenMaxQueues() {
	mockBaseScheduler := s.createMockBaseScheduler()
	mockBaseScheduler.EXPECT().Start().Times(1)
	mockBaseScheduler.EXPECT().Stop().Times(1)

	// Use MaxQueues=1 so the second workflow can't get a queue
	scheduler := NewExecutionAwareScheduler(
		mockBaseScheduler,
		ExecutionAwareSchedulerOptions{
			EnableExecutionQueueScheduler:    func() bool { return true },
			ExecutionQueueSchedulerMaxQueues: func() int { return 1 },
			ExecutionQueueSchedulerQueueTTL:  func() time.Duration { return 5 * time.Second },
		},
		s.logger,
		metrics.NoopMetricsHandler,
		clock.NewRealTimeSource(),
	)
	scheduler.Start()
	defer scheduler.Stop()

	// First task blocks execution so queue stays occupied
	blockCh := make(chan struct{})
	execStarted := make(chan struct{})
	var completionWG sync.WaitGroup
	completionWG.Add(1)
	mockExec1 := s.createMockExecutable("ns1", "wf1", "run1")
	mockExec1.EXPECT().RetryPolicy().Return(backoff.NewExponentialRetryPolicy(time.Millisecond)).AnyTimes()
	mockExec1.EXPECT().Execute().DoAndReturn(func() error {
		close(execStarted)
		<-blockCh
		return nil
	}).MaxTimes(1)
	mockExec1.EXPECT().Ack().Do(func() { completionWG.Done() }).MaxTimes(1)
	mockExec1.EXPECT().Abort().MaxTimes(1)
	s.True(scheduler.HandleBusyWorkflow(mockExec1))

	// Wait for goroutine to pick up first task
	<-execStarted

	// Second task for a DIFFERENT workflow should fall back to base scheduler (MaxQueues reached)
	mockExec2 := s.createMockExecutable("ns1", "wf2", "run2")
	mockBaseScheduler.EXPECT().TrySubmit(mockExec2).Return(true).Times(1)
	s.True(scheduler.HandleBusyWorkflow(mockExec2))

	// Cleanup
	close(blockCh)
	completionWG.Wait()
}

// =============================================================================
// HasExecutionQueue Tests
// =============================================================================

func (s *executionAwareSchedulerSuite) TestHasExecutionQueue_ReturnsFalseWhenDisabled() {
	mockBaseScheduler := s.createMockBaseScheduler()
	mockExec := NewMockExecutable(s.controller)

	scheduler := NewExecutionAwareScheduler(
		mockBaseScheduler,
		ExecutionAwareSchedulerOptions{
			EnableExecutionQueueScheduler:      func() bool { return false },
			ExecutionQueueSchedulerMaxQueues: func() int { return 500 },
			ExecutionQueueSchedulerQueueTTL:  func() time.Duration { return 5 * time.Second },
		},
		s.logger,
		metrics.NoopMetricsHandler,
		clock.NewRealTimeSource(),
	)
	s.False(scheduler.HasExecutionQueue(mockExec))
}

func (s *executionAwareSchedulerSuite) TestHasExecutionQueue_ReturnsFalseWhenNoQueue() {
	mockBaseScheduler := s.createMockBaseScheduler()
	mockBaseScheduler.EXPECT().Start().Times(1)
	mockBaseScheduler.EXPECT().Stop().Times(1)

	scheduler := NewExecutionAwareScheduler(
		mockBaseScheduler,
		ExecutionAwareSchedulerOptions{
			EnableExecutionQueueScheduler:      func() bool { return true },
			ExecutionQueueSchedulerMaxQueues: func() int { return 500 },
			ExecutionQueueSchedulerQueueTTL:  func() time.Duration { return 5 * time.Second },
		},
		s.logger,
		metrics.NoopMetricsHandler,
		clock.NewRealTimeSource(),
	)
	scheduler.Start()
	defer scheduler.Stop()

	mockExec := s.createMockExecutable("ns1", "wf1", "run1")
	s.False(scheduler.HasExecutionQueue(mockExec))
}

func (s *executionAwareSchedulerSuite) TestHasExecutionQueue_ReturnsTrueWhenQueueExists() {
	mockBaseScheduler := s.createMockBaseScheduler()
	mockBaseScheduler.EXPECT().Start().Times(1)
	mockBaseScheduler.EXPECT().Stop().Times(1)

	scheduler := NewExecutionAwareScheduler(
		mockBaseScheduler,
		ExecutionAwareSchedulerOptions{
			EnableExecutionQueueScheduler:      func() bool { return true },
			ExecutionQueueSchedulerMaxQueues: func() int { return 500 },
			ExecutionQueueSchedulerQueueTTL:  func() time.Duration { return 5 * time.Second },
		},
		s.logger,
		metrics.NoopMetricsHandler,
		clock.NewRealTimeSource(),
	)
	scheduler.Start()
	defer scheduler.Stop()

	// Create a queue by submitting a task
	mockExec1 := s.createMockExecutable("ns1", "wf1", "run1")
	mockExec1.EXPECT().RetryPolicy().Return(backoff.NewExponentialRetryPolicy(time.Millisecond)).AnyTimes()

	// Block execution so queue stays active
	execStarted := make(chan struct{})
	execContinue := make(chan struct{})
	var completionWG sync.WaitGroup
	completionWG.Add(2)
	mockExec1.EXPECT().Execute().DoAndReturn(func() error {
		close(execStarted)
		<-execContinue
		return nil
	}).Times(1)
	mockExec1.EXPECT().Ack().Do(func() { completionWG.Done() }).Times(1)

	scheduler.HandleBusyWorkflow(mockExec1)
	<-execStarted

	// Now add another task to ensure queue stays alive
	mockExec2 := s.createMockExecutable("ns1", "wf1", "run1")
	mockExec2.EXPECT().RetryPolicy().Return(backoff.NewExponentialRetryPolicy(time.Millisecond)).AnyTimes()
	mockExec2.EXPECT().Execute().Return(nil).Times(1)
	mockExec2.EXPECT().Ack().Do(func() { completionWG.Done() }).Times(1)
	scheduler.HandleBusyWorkflow(mockExec2)

	// Check that queue exists for this workflow
	mockExec3 := s.createMockExecutable("ns1", "wf1", "run1")
	s.True(scheduler.HasExecutionQueue(mockExec3))

	// Check that queue doesn't exist for different workflow
	mockExec4 := s.createMockExecutable("ns1", "wf2", "run2")
	s.False(scheduler.HasExecutionQueue(mockExec4))

	close(execContinue)
	completionWG.Wait()
}

// =============================================================================
// Helper Functions Tests
// =============================================================================

func (s *executionAwareSchedulerSuite) TestGetWorkflowKey() {
	mockExec := NewMockExecutable(s.controller)
	mockExec.EXPECT().GetNamespaceID().Return("ns1").Times(1)
	mockExec.EXPECT().GetWorkflowID().Return("wf1").Times(1)
	mockExec.EXPECT().GetRunID().Return("run1").Times(1)

	key := getWorkflowKey(mockExec)
	s.Equal("ns1", key.NamespaceID)
	s.Equal("wf1", key.WorkflowID)
	s.Equal("run1", key.RunID)
}

func (s *executionAwareSchedulerSuite) TestExecutableQueueKeyFn() {
	mockExec := s.createMockExecutable("ns1", "wf1", "run1")

	key := executableQueueKeyFn(mockExec)
	s.Equal(definition.NewWorkflowKey("ns1", "wf1", "run1"), key)
}

// =============================================================================
// Concurrent Tests
// =============================================================================

func (s *executionAwareSchedulerSuite) TestConcurrentSubmit() {
	mockBaseScheduler := s.createMockBaseScheduler()
	mockBaseScheduler.EXPECT().Start().Times(1)
	mockBaseScheduler.EXPECT().Stop().Times(1)
	// All tasks should be routed to base scheduler (no active queue)
	mockBaseScheduler.EXPECT().Submit(gomock.Any()).AnyTimes()

	scheduler := NewExecutionAwareScheduler(
		mockBaseScheduler,
		ExecutionAwareSchedulerOptions{
			EnableExecutionQueueScheduler:      func() bool { return true },
			ExecutionQueueSchedulerMaxQueues: func() int { return 500 },
			ExecutionQueueSchedulerQueueTTL:  func() time.Duration { return 5 * time.Second },
		},
		s.logger,
		metrics.NoopMetricsHandler,
		clock.NewRealTimeSource(),
	)
	scheduler.Start()
	defer scheduler.Stop()

	var wg sync.WaitGroup
	numTasks := 100
	wg.Add(numTasks)

	for range numTasks {
		go func() {
			defer wg.Done()
			mockExec := NewMockExecutable(s.controller)
			mockExec.EXPECT().GetNamespaceID().Return("ns1").AnyTimes()
			mockExec.EXPECT().GetWorkflowID().Return("wf1").AnyTimes()
			mockExec.EXPECT().GetRunID().Return("run1").AnyTimes()
			scheduler.Submit(mockExec)
		}()
	}

	wg.Wait()
}

func (s *executionAwareSchedulerSuite) TestConcurrentHandleBusyWorkflow() {
	mockBaseScheduler := s.createMockBaseScheduler()
	mockBaseScheduler.EXPECT().Start().Times(1)
	mockBaseScheduler.EXPECT().Stop().Times(1)

	scheduler := NewExecutionAwareScheduler(
		mockBaseScheduler,
		ExecutionAwareSchedulerOptions{
			EnableExecutionQueueScheduler:      func() bool { return true },
			ExecutionQueueSchedulerMaxQueues: func() int { return 500 },
			ExecutionQueueSchedulerQueueTTL:  func() time.Duration { return 5 * time.Second },
		},
		s.logger,
		metrics.NoopMetricsHandler,
		clock.NewRealTimeSource(),
	)
	scheduler.Start()
	defer scheduler.Stop()

	var submitWG sync.WaitGroup
	var completionWG sync.WaitGroup
	numTasks := 50
	submitWG.Add(numTasks)
	completionWG.Add(numTasks)

	for range numTasks {
		go func() {
			defer submitWG.Done()
			mockExec := s.createMockExecutable("ns1", "wf1", "run1")
			mockExec.EXPECT().RetryPolicy().Return(backoff.NewExponentialRetryPolicy(time.Millisecond)).AnyTimes()
			mockExec.EXPECT().Execute().Return(nil).MaxTimes(1)
			mockExec.EXPECT().Ack().Do(func() { completionWG.Done() }).MaxTimes(1)
			mockExec.EXPECT().Abort().Do(func() { completionWG.Done() }).MaxTimes(1)
			scheduler.HandleBusyWorkflow(mockExec)
		}()
	}

	submitWG.Wait()
	completionWG.Wait()
}

// =============================================================================
// Helper Functions
// =============================================================================

func (s *executionAwareSchedulerSuite) createMockExecutable(namespaceID, workflowID, runID string) *MockExecutable {
	mockExec := NewMockExecutable(s.controller)
	mockExec.EXPECT().GetNamespaceID().Return(namespaceID).AnyTimes()
	mockExec.EXPECT().GetWorkflowID().Return(workflowID).AnyTimes()
	mockExec.EXPECT().GetRunID().Return(runID).AnyTimes()
	return mockExec
}

// defaultSchedulerOptions returns ExecutionAwareSchedulerOptions with common test defaults.
func (s *executionAwareSchedulerSuite) defaultSchedulerOptions(enabled bool) ExecutionAwareSchedulerOptions {
	return ExecutionAwareSchedulerOptions{
		EnableExecutionQueueScheduler:    func() bool { return enabled },
		ExecutionQueueSchedulerMaxQueues: func() int { return 500 },
		ExecutionQueueSchedulerQueueTTL:  func() time.Duration { return 5 * time.Second },
	}
}

// createSchedulerWithMock creates a ExecutionAwareScheduler with a mock base scheduler
// and sets up Start/Stop expectations. Returns both the scheduler and the mock for
// additional expectation setup.
func (s *executionAwareSchedulerSuite) createSchedulerWithMock(enabled bool) (*ExecutionAwareScheduler, *tasks.MockScheduler[Executable]) {
	mockBaseScheduler := s.createMockBaseScheduler()
	mockBaseScheduler.EXPECT().Start().Times(1)
	mockBaseScheduler.EXPECT().Stop().Times(1)

	scheduler := NewExecutionAwareScheduler(
		mockBaseScheduler,
		s.defaultSchedulerOptions(enabled),
		s.logger,
		metrics.NoopMetricsHandler,
		clock.NewRealTimeSource(),
	)
	return scheduler, mockBaseScheduler
}
