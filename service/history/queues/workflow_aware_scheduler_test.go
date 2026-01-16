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
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/tasks"
	"go.uber.org/mock/gomock"
)

type (
	workflowAwareSchedulerSuite struct {
		*require.Assertions
		suite.Suite

		controller *gomock.Controller
		logger     log.Logger
	}
)

func TestWorkflowAwareSchedulerSuite(t *testing.T) {
	s := new(workflowAwareSchedulerSuite)
	suite.Run(t, s)
}

func (s *workflowAwareSchedulerSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.controller = gomock.NewController(s.T())
	s.logger = log.NewNoopLogger()
}

func (s *workflowAwareSchedulerSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *workflowAwareSchedulerSuite) createMockBaseScheduler() *tasks.MockScheduler[Executable] {
	return tasks.NewMockScheduler[Executable](s.controller)
}

// =============================================================================
// Constructor Tests
// =============================================================================

func (s *workflowAwareSchedulerSuite) TestNewWorkflowAwareScheduler_DefaultQueueSize() {
	mockBaseScheduler := s.createMockBaseScheduler()
	scheduler := NewWorkflowAwareScheduler(
		mockBaseScheduler,
		WorkflowAwareSchedulerOptions{
			EnableWorkflowQueueScheduler:      func() bool { return false },
			WorkflowQueueSchedulerQueueSize:   func() int { return 1000 }, // default queue size
			WorkflowQueueSchedulerWorkerCount: func(_ func(int)) (int, func()) { return 1, func() {} },
		},
		s.logger,
	)
	s.NotNil(scheduler)
	s.NotNil(scheduler.workflowQueueScheduler)
	s.NotNil(scheduler.baseScheduler)
}

func (s *workflowAwareSchedulerSuite) TestNewWorkflowAwareScheduler_CustomQueueSize() {
	mockBaseScheduler := s.createMockBaseScheduler()
	scheduler := NewWorkflowAwareScheduler(
		mockBaseScheduler,
		WorkflowAwareSchedulerOptions{
			EnableWorkflowQueueScheduler:      func() bool { return true },
			WorkflowQueueSchedulerQueueSize:   func() int { return 500 },
			WorkflowQueueSchedulerWorkerCount: func(_ func(int)) (int, func()) { return 2, func() {} },
		},
		s.logger,
	)
	s.NotNil(scheduler)
}

// =============================================================================
// Start/Stop Tests
// =============================================================================

func (s *workflowAwareSchedulerSuite) TestStart_WithWorkflowQueueSchedulerEnabled() {
	scheduler, _ := s.createSchedulerWithMock(true)
	scheduler.Start()
	scheduler.Stop()
}

func (s *workflowAwareSchedulerSuite) TestStart_AlwaysStartsWorkflowQueueScheduler() {
	// Even when EnableWorkflowQueueScheduler returns false, the WorkflowQueueScheduler
	// is always started to handle dynamic config changes from disabled to enabled.
	scheduler, _ := s.createSchedulerWithMock(false)
	scheduler.Start()
	scheduler.Stop()
}

func (s *workflowAwareSchedulerSuite) TestStop_StopsBothSchedulers() {
	scheduler, _ := s.createSchedulerWithMock(true)
	scheduler.Start()
	scheduler.Stop()
}

// =============================================================================
// Submit Tests
// =============================================================================

func (s *workflowAwareSchedulerSuite) TestSubmit_DelegatesToBaseWhenWorkflowQueueSchedulerDisabled() {
	scheduler, mockBaseScheduler := s.createSchedulerWithMock(false)
	mockExec := s.createMockExecutable("ns1", "wf1", "run1")
	mockBaseScheduler.EXPECT().Submit(mockExec).Times(1)

	scheduler.Start()
	defer scheduler.Stop()

	scheduler.Submit(mockExec)
}

func (s *workflowAwareSchedulerSuite) TestSubmit_RoutesToBaseWhenNoActiveQueue() {
	scheduler, mockBaseScheduler := s.createSchedulerWithMock(true)
	mockExec := s.createMockExecutable("ns1", "wf1", "run1")
	mockBaseScheduler.EXPECT().Submit(mockExec).Times(1)

	scheduler.Start()
	defer scheduler.Stop()

	scheduler.Submit(mockExec)
}

func (s *workflowAwareSchedulerSuite) TestSubmit_RoutesToWorkflowQueueSchedulerWhenActiveQueue() {
	mockBaseScheduler := s.createMockBaseScheduler()
	mockBaseScheduler.EXPECT().Start().Times(1)
	mockBaseScheduler.EXPECT().Stop().Times(1)

	scheduler := NewWorkflowAwareScheduler(
		mockBaseScheduler,
		WorkflowAwareSchedulerOptions{
			EnableWorkflowQueueScheduler:      func() bool { return true },
			WorkflowQueueSchedulerQueueSize:   func() int { return 100 },
			WorkflowQueueSchedulerWorkerCount: func(_ func(int)) (int, func()) { return 1, func() {} },
		},
		s.logger,
	)
	scheduler.Start()
	defer scheduler.Stop()

	// Create an active queue by submitting via HandleBusyWorkflow
	mockExec1 := s.createMockExecutable("ns1", "wf1", "run1")
	mockExec1.EXPECT().RetryPolicy().Return(backoff.NewExponentialRetryPolicy(time.Millisecond)).AnyTimes()

	// Use channels to control execution timing
	execStarted := make(chan struct{})
	execContinue := make(chan struct{})
	mockExec1.EXPECT().Execute().DoAndReturn(func() error {
		close(execStarted)
		<-execContinue
		return nil
	}).Times(1)
	mockExec1.EXPECT().Ack().Times(1)

	// First, route to WorkflowQueueScheduler via HandleBusyWorkflow
	s.True(scheduler.HandleBusyWorkflow(mockExec1))

	// Wait for execution to start
	<-execStarted

	// Now there's an active queue, subsequent submit should route to WorkflowQueueScheduler
	// Add task 2 which will be queued behind task 1
	mockExec2 := s.createMockExecutable("ns1", "wf1", "run1")
	mockExec2.EXPECT().RetryPolicy().Return(backoff.NewExponentialRetryPolicy(time.Millisecond)).AnyTimes()
	mockExec2.EXPECT().Execute().Return(nil).Times(1)
	mockExec2.EXPECT().Ack().Times(1)

	// HandleBusyWorkflow instead of Submit since we want to add to workflow queue
	// (Submit checks HasQueue which may not return true if timing is wrong)
	s.True(scheduler.HandleBusyWorkflow(mockExec2))

	// Verify queue exists for this workflow before task1 completes
	mockExec3 := s.createMockExecutable("ns1", "wf1", "run1")
	s.True(scheduler.HasWorkflowQueue(mockExec3))

	// Add a third task via Submit - should route to WorkflowQueueScheduler since queue exists
	mockExec4 := s.createMockExecutable("ns1", "wf1", "run1")
	mockExec4.EXPECT().RetryPolicy().Return(backoff.NewExponentialRetryPolicy(time.Millisecond)).AnyTimes()
	mockExec4.EXPECT().Execute().Return(nil).Times(1)
	mockExec4.EXPECT().Ack().Times(1)
	scheduler.Submit(mockExec4) // Should route to WorkflowQueueScheduler because queue exists

	// Let tasks complete
	close(execContinue)
	time.Sleep(100 * time.Millisecond) // Give time for tasks to complete
}

// =============================================================================
// TrySubmit Tests
// =============================================================================

func (s *workflowAwareSchedulerSuite) TestTrySubmit_DelegatesToBaseWhenWorkflowQueueSchedulerDisabled() {
	mockBaseScheduler := s.createMockBaseScheduler()
	mockExec := s.createMockExecutable("ns1", "wf1", "run1")

	mockBaseScheduler.EXPECT().Start().Times(1)
	mockBaseScheduler.EXPECT().Stop().Times(1)
	mockBaseScheduler.EXPECT().TrySubmit(mockExec).Return(true).Times(1)

	scheduler := NewWorkflowAwareScheduler(
		mockBaseScheduler,
		WorkflowAwareSchedulerOptions{
			EnableWorkflowQueueScheduler:      func() bool { return false },
			WorkflowQueueSchedulerQueueSize:   func() int { return 100 },
			WorkflowQueueSchedulerWorkerCount: func(_ func(int)) (int, func()) { return 1, func() {} },
		},
		s.logger,
	)
	scheduler.Start()
	defer scheduler.Stop()

	s.True(scheduler.TrySubmit(mockExec))
}

func (s *workflowAwareSchedulerSuite) TestTrySubmit_RoutesToBaseWhenNoActiveQueue() {
	mockBaseScheduler := s.createMockBaseScheduler()
	mockExec := s.createMockExecutable("ns1", "wf1", "run1")

	mockBaseScheduler.EXPECT().Start().Times(1)
	mockBaseScheduler.EXPECT().Stop().Times(1)
	mockBaseScheduler.EXPECT().TrySubmit(mockExec).Return(true).Times(1)

	scheduler := NewWorkflowAwareScheduler(
		mockBaseScheduler,
		WorkflowAwareSchedulerOptions{
			EnableWorkflowQueueScheduler:      func() bool { return true },
			WorkflowQueueSchedulerQueueSize:   func() int { return 100 },
			WorkflowQueueSchedulerWorkerCount: func(_ func(int)) (int, func()) { return 1, func() {} },
		},
		s.logger,
	)
	scheduler.Start()
	defer scheduler.Stop()

	s.True(scheduler.TrySubmit(mockExec))
}

func (s *workflowAwareSchedulerSuite) TestTrySubmit_AddsToExistingQueueSuccessfully() {
	// When TrySubmit is called for a workflow with an active queue,
	// it should route to WorkflowQueueScheduler and add to the existing queue (returning true)
	mockBaseScheduler := s.createMockBaseScheduler()
	mockBaseScheduler.EXPECT().Start().Times(1)
	mockBaseScheduler.EXPECT().Stop().Times(1)

	scheduler := NewWorkflowAwareScheduler(
		mockBaseScheduler,
		WorkflowAwareSchedulerOptions{
			EnableWorkflowQueueScheduler:      func() bool { return true },
			WorkflowQueueSchedulerQueueSize:   func() int { return 100 },
			WorkflowQueueSchedulerWorkerCount: func(_ func(int)) (int, func()) { return 1, func() {} },
		},
		s.logger,
	)
	scheduler.Start()
	defer scheduler.Stop()

	// First task blocks execution - this creates a queue for wf1
	mockExec1 := s.createMockExecutable("ns1", "wf1", "run1")
	mockExec1.EXPECT().RetryPolicy().Return(backoff.NewExponentialRetryPolicy(time.Millisecond)).AnyTimes()
	execStarted := make(chan struct{})
	execContinue := make(chan struct{})
	mockExec1.EXPECT().Execute().DoAndReturn(func() error {
		close(execStarted)
		<-execContinue
		return nil
	}).Times(1)
	mockExec1.EXPECT().Ack().Times(1)
	s.True(scheduler.HandleBusyWorkflow(mockExec1))

	// Wait for first task to start executing
	<-execStarted

	// Add second task to keep queue active
	mockExec2 := s.createMockExecutable("ns1", "wf1", "run1")
	mockExec2.EXPECT().RetryPolicy().Return(backoff.NewExponentialRetryPolicy(time.Millisecond)).AnyTimes()
	mockExec2.EXPECT().Execute().Return(nil).Times(1)
	mockExec2.EXPECT().Ack().Times(1)
	scheduler.HandleBusyWorkflow(mockExec2)

	// TrySubmit for wf1 which has an active queue - should add to existing queue and succeed
	mockExec3 := s.createMockExecutable("ns1", "wf1", "run1")
	mockExec3.EXPECT().RetryPolicy().Return(backoff.NewExponentialRetryPolicy(time.Millisecond)).AnyTimes()
	mockExec3.EXPECT().Execute().Return(nil).Times(1)
	mockExec3.EXPECT().Ack().Times(1)

	// TrySubmit should return true because task was added to existing queue
	s.True(scheduler.TrySubmit(mockExec3))

	// Cleanup
	close(execContinue)
	time.Sleep(100 * time.Millisecond)
}

// =============================================================================
// HandleBusyWorkflow Tests
// =============================================================================

func (s *workflowAwareSchedulerSuite) TestHandleBusyWorkflow_ReturnsFalseWhenDisabled() {
	mockBaseScheduler := s.createMockBaseScheduler()
	mockExec := NewMockExecutable(s.controller)

	scheduler := NewWorkflowAwareScheduler(
		mockBaseScheduler,
		WorkflowAwareSchedulerOptions{
			EnableWorkflowQueueScheduler:      func() bool { return false },
			WorkflowQueueSchedulerQueueSize:   func() int { return 100 },
			WorkflowQueueSchedulerWorkerCount: func(_ func(int)) (int, func()) { return 1, func() {} },
		},
		s.logger,
	)
	s.False(scheduler.HandleBusyWorkflow(mockExec))
}

func (s *workflowAwareSchedulerSuite) TestHandleBusyWorkflow_SubmitsToWorkflowQueueScheduler() {
	mockBaseScheduler := s.createMockBaseScheduler()
	mockBaseScheduler.EXPECT().Start().Times(1)
	mockBaseScheduler.EXPECT().Stop().Times(1)

	scheduler := NewWorkflowAwareScheduler(
		mockBaseScheduler,
		WorkflowAwareSchedulerOptions{
			EnableWorkflowQueueScheduler:      func() bool { return true },
			WorkflowQueueSchedulerQueueSize:   func() int { return 100 },
			WorkflowQueueSchedulerWorkerCount: func(_ func(int)) (int, func()) { return 1, func() {} },
		},
		s.logger,
	)
	scheduler.Start()
	defer scheduler.Stop()

	mockExec := s.createMockExecutable("ns1", "wf1", "run1")
	mockExec.EXPECT().RetryPolicy().Return(backoff.NewExponentialRetryPolicy(time.Millisecond)).AnyTimes()
	mockExec.EXPECT().Execute().Return(nil).Times(1)
	mockExec.EXPECT().Ack().Times(1)

	s.True(scheduler.HandleBusyWorkflow(mockExec))
	time.Sleep(50 * time.Millisecond) // Give time for task to complete
}

func (s *workflowAwareSchedulerSuite) TestHandleBusyWorkflow_ReschedulesWhenFull() {
	mockBaseScheduler := s.createMockBaseScheduler()
	mockBaseScheduler.EXPECT().Start().Times(1)
	mockBaseScheduler.EXPECT().Stop().Times(1)

	// Use very small queue size
	scheduler := NewWorkflowAwareScheduler(
		mockBaseScheduler,
		WorkflowAwareSchedulerOptions{
			EnableWorkflowQueueScheduler:      func() bool { return true },
			WorkflowQueueSchedulerQueueSize:   func() int { return 1 },
			WorkflowQueueSchedulerWorkerCount: func(_ func(int)) (int, func()) { return 0, func() {} }, // 0 workers
		},
		s.logger,
	)
	scheduler.Start()
	defer scheduler.Stop()

	// Fill the queue
	mockExec1 := s.createMockExecutable("ns1", "wf1", "run1")
	mockExec1.EXPECT().RetryPolicy().Return(backoff.NewExponentialRetryPolicy(time.Millisecond)).AnyTimes()
	mockExec1.EXPECT().Abort().MaxTimes(1)
	s.True(scheduler.HandleBusyWorkflow(mockExec1))

	// Second task should be rescheduled
	mockExec2 := s.createMockExecutable("ns1", "wf2", "run2")
	mockExec2.EXPECT().Reschedule().Times(1)
	s.True(scheduler.HandleBusyWorkflow(mockExec2))
}

// =============================================================================
// HasWorkflowQueue Tests
// =============================================================================

func (s *workflowAwareSchedulerSuite) TestHasWorkflowQueue_ReturnsFalseWhenDisabled() {
	mockBaseScheduler := s.createMockBaseScheduler()
	mockExec := NewMockExecutable(s.controller)

	scheduler := NewWorkflowAwareScheduler(
		mockBaseScheduler,
		WorkflowAwareSchedulerOptions{
			EnableWorkflowQueueScheduler:      func() bool { return false },
			WorkflowQueueSchedulerQueueSize:   func() int { return 100 },
			WorkflowQueueSchedulerWorkerCount: func(_ func(int)) (int, func()) { return 1, func() {} },
		},
		s.logger,
	)
	s.False(scheduler.HasWorkflowQueue(mockExec))
}

func (s *workflowAwareSchedulerSuite) TestHasWorkflowQueue_ReturnsFalseWhenNoQueue() {
	mockBaseScheduler := s.createMockBaseScheduler()
	mockBaseScheduler.EXPECT().Start().Times(1)
	mockBaseScheduler.EXPECT().Stop().Times(1)

	scheduler := NewWorkflowAwareScheduler(
		mockBaseScheduler,
		WorkflowAwareSchedulerOptions{
			EnableWorkflowQueueScheduler:      func() bool { return true },
			WorkflowQueueSchedulerQueueSize:   func() int { return 100 },
			WorkflowQueueSchedulerWorkerCount: func(_ func(int)) (int, func()) { return 1, func() {} },
		},
		s.logger,
	)
	scheduler.Start()
	defer scheduler.Stop()

	mockExec := s.createMockExecutable("ns1", "wf1", "run1")
	s.False(scheduler.HasWorkflowQueue(mockExec))
}

func (s *workflowAwareSchedulerSuite) TestHasWorkflowQueue_ReturnsTrueWhenQueueExists() {
	mockBaseScheduler := s.createMockBaseScheduler()
	mockBaseScheduler.EXPECT().Start().Times(1)
	mockBaseScheduler.EXPECT().Stop().Times(1)

	scheduler := NewWorkflowAwareScheduler(
		mockBaseScheduler,
		WorkflowAwareSchedulerOptions{
			EnableWorkflowQueueScheduler:      func() bool { return true },
			WorkflowQueueSchedulerQueueSize:   func() int { return 100 },
			WorkflowQueueSchedulerWorkerCount: func(_ func(int)) (int, func()) { return 1, func() {} },
		},
		s.logger,
	)
	scheduler.Start()
	defer scheduler.Stop()

	// Create a queue by submitting a task
	mockExec1 := s.createMockExecutable("ns1", "wf1", "run1")
	mockExec1.EXPECT().RetryPolicy().Return(backoff.NewExponentialRetryPolicy(time.Millisecond)).AnyTimes()

	// Block execution so queue stays active
	execStarted := make(chan struct{})
	execContinue := make(chan struct{})
	mockExec1.EXPECT().Execute().DoAndReturn(func() error {
		close(execStarted)
		<-execContinue
		return nil
	}).Times(1)
	mockExec1.EXPECT().Ack().Times(1)

	scheduler.HandleBusyWorkflow(mockExec1)
	<-execStarted

	// Now add another task to ensure queue stays alive
	mockExec2 := s.createMockExecutable("ns1", "wf1", "run1")
	mockExec2.EXPECT().RetryPolicy().Return(backoff.NewExponentialRetryPolicy(time.Millisecond)).AnyTimes()
	mockExec2.EXPECT().Execute().Return(nil).Times(1)
	mockExec2.EXPECT().Ack().Times(1)
	scheduler.HandleBusyWorkflow(mockExec2)

	// Check that queue exists for this workflow
	mockExec3 := s.createMockExecutable("ns1", "wf1", "run1")
	s.True(scheduler.HasWorkflowQueue(mockExec3))

	// Check that queue doesn't exist for different workflow
	mockExec4 := s.createMockExecutable("ns1", "wf2", "run2")
	s.False(scheduler.HasWorkflowQueue(mockExec4))

	close(execContinue)
	time.Sleep(50 * time.Millisecond)
}

// =============================================================================
// Helper Functions Tests
// =============================================================================

func (s *workflowAwareSchedulerSuite) TestGetWorkflowKey() {
	mockExec := NewMockExecutable(s.controller)
	mockExec.EXPECT().GetNamespaceID().Return("ns1").Times(1)
	mockExec.EXPECT().GetWorkflowID().Return("wf1").Times(1)
	mockExec.EXPECT().GetRunID().Return("run1").Times(1)

	key := getWorkflowKey(mockExec)
	s.Equal("ns1", key.NamespaceID)
	s.Equal("wf1", key.WorkflowID)
	s.Equal("run1", key.RunID)
}

func (s *workflowAwareSchedulerSuite) TestExecutableQueueKeyFn() {
	mockExec := s.createMockExecutable("ns1", "wf1", "run1")

	key := executableQueueKeyFn(mockExec)
	s.Equal(definition.NewWorkflowKey("ns1", "wf1", "run1"), key)
}

// =============================================================================
// Concurrent Tests
// =============================================================================

func (s *workflowAwareSchedulerSuite) TestConcurrentSubmit() {
	mockBaseScheduler := s.createMockBaseScheduler()
	mockBaseScheduler.EXPECT().Start().Times(1)
	mockBaseScheduler.EXPECT().Stop().Times(1)
	// All tasks should be routed to base scheduler (no active queue)
	mockBaseScheduler.EXPECT().Submit(gomock.Any()).AnyTimes()

	scheduler := NewWorkflowAwareScheduler(
		mockBaseScheduler,
		WorkflowAwareSchedulerOptions{
			EnableWorkflowQueueScheduler:      func() bool { return true },
			WorkflowQueueSchedulerQueueSize:   func() int { return 1000 },
			WorkflowQueueSchedulerWorkerCount: func(_ func(int)) (int, func()) { return 4, func() {} },
		},
		s.logger,
	)
	scheduler.Start()
	defer scheduler.Stop()

	var wg sync.WaitGroup
	numTasks := 100
	wg.Add(numTasks)

	for i := 0; i < numTasks; i++ {
		go func(idx int) {
			defer wg.Done()
			mockExec := NewMockExecutable(s.controller)
			mockExec.EXPECT().GetNamespaceID().Return("ns1").AnyTimes()
			mockExec.EXPECT().GetWorkflowID().Return("wf1").AnyTimes()
			mockExec.EXPECT().GetRunID().Return("run1").AnyTimes()
			scheduler.Submit(mockExec)
		}(i)
	}

	wg.Wait()
	time.Sleep(200 * time.Millisecond) // Give time for tasks to complete
}

func (s *workflowAwareSchedulerSuite) TestConcurrentHandleBusyWorkflow() {
	mockBaseScheduler := s.createMockBaseScheduler()
	mockBaseScheduler.EXPECT().Start().Times(1)
	mockBaseScheduler.EXPECT().Stop().Times(1)

	scheduler := NewWorkflowAwareScheduler(
		mockBaseScheduler,
		WorkflowAwareSchedulerOptions{
			EnableWorkflowQueueScheduler:      func() bool { return true },
			WorkflowQueueSchedulerQueueSize:   func() int { return 1000 },
			WorkflowQueueSchedulerWorkerCount: func(_ func(int)) (int, func()) { return 4, func() {} },
		},
		s.logger,
	)
	scheduler.Start()
	defer scheduler.Stop()

	var wg sync.WaitGroup
	numTasks := 50
	wg.Add(numTasks)

	for i := 0; i < numTasks; i++ {
		go func(idx int) {
			defer wg.Done()
			mockExec := s.createMockExecutable("ns1", "wf1", "run1")
			mockExec.EXPECT().RetryPolicy().Return(backoff.NewExponentialRetryPolicy(time.Millisecond)).AnyTimes()
			mockExec.EXPECT().Execute().Return(nil).MaxTimes(1)
			mockExec.EXPECT().Ack().MaxTimes(1)
			mockExec.EXPECT().Abort().MaxTimes(1)
			scheduler.HandleBusyWorkflow(mockExec)
		}(i)
	}

	wg.Wait()
	time.Sleep(100 * time.Millisecond) // Give time for tasks to complete
}

// =============================================================================
// Helper Functions
// =============================================================================

func (s *workflowAwareSchedulerSuite) createMockExecutable(namespaceID, workflowID, runID string) *MockExecutable {
	mockExec := NewMockExecutable(s.controller)
	mockExec.EXPECT().GetNamespaceID().Return(namespaceID).AnyTimes()
	mockExec.EXPECT().GetWorkflowID().Return(workflowID).AnyTimes()
	mockExec.EXPECT().GetRunID().Return(runID).AnyTimes()
	return mockExec
}

// defaultSchedulerOptions returns WorkflowAwareSchedulerOptions with common test defaults.
func (s *workflowAwareSchedulerSuite) defaultSchedulerOptions(enabled bool) WorkflowAwareSchedulerOptions {
	return WorkflowAwareSchedulerOptions{
		EnableWorkflowQueueScheduler:      func() bool { return enabled },
		WorkflowQueueSchedulerQueueSize:   func() int { return 100 },
		WorkflowQueueSchedulerWorkerCount: func(_ func(int)) (int, func()) { return 1, func() {} },
	}
}

// createSchedulerWithMock creates a WorkflowAwareScheduler with a mock base scheduler
// and sets up Start/Stop expectations. Returns both the scheduler and the mock for
// additional expectation setup.
func (s *workflowAwareSchedulerSuite) createSchedulerWithMock(enabled bool) (*WorkflowAwareScheduler, *tasks.MockScheduler[Executable]) {
	mockBaseScheduler := s.createMockBaseScheduler()
	mockBaseScheduler.EXPECT().Start().Times(1)
	mockBaseScheduler.EXPECT().Stop().Times(1)

	scheduler := NewWorkflowAwareScheduler(
		mockBaseScheduler,
		s.defaultSchedulerOptions(enabled),
		s.logger,
	)
	return scheduler, mockBaseScheduler
}
