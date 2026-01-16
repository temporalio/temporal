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
	"go.uber.org/mock/gomock"
)

type (
	workflowAwareSchedulerSuite struct {
		*require.Assertions
		suite.Suite

		controller    *gomock.Controller
		baseScheduler *MockScheduler
		logger        log.Logger
	}
)

func TestWorkflowAwareSchedulerSuite(t *testing.T) {
	s := new(workflowAwareSchedulerSuite)
	suite.Run(t, s)
}

func (s *workflowAwareSchedulerSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.controller = gomock.NewController(s.T())
	s.baseScheduler = NewMockScheduler(s.controller)
	s.logger = log.NewNoopLogger()
}

func (s *workflowAwareSchedulerSuite) TearDownTest() {
	s.controller.Finish()
}

// =============================================================================
// Constructor Tests
// =============================================================================

func (s *workflowAwareSchedulerSuite) TestNewWorkflowAwareScheduler_DefaultQueueSize() {
	scheduler := NewWorkflowAwareScheduler(
		s.baseScheduler,
		WorkflowAwareSchedulerOptions{
			EnableSequentialScheduler:      nil,
			SequentialSchedulerQueueSize:   nil, // nil should use default
			SequentialSchedulerWorkerCount: func(_ func(int)) (int, func()) { return 1, func() {} },
		},
		s.logger,
	)
	s.NotNil(scheduler)
	s.NotNil(scheduler.sequentialScheduler)
}

func (s *workflowAwareSchedulerSuite) TestNewWorkflowAwareScheduler_CustomQueueSize() {
	scheduler := NewWorkflowAwareScheduler(
		s.baseScheduler,
		WorkflowAwareSchedulerOptions{
			EnableSequentialScheduler:      func() bool { return true },
			SequentialSchedulerQueueSize:   func() int { return 500 },
			SequentialSchedulerWorkerCount: func(_ func(int)) (int, func()) { return 2, func() {} },
		},
		s.logger,
	)
	s.NotNil(scheduler)
}

// =============================================================================
// Start/Stop Tests
// =============================================================================

func (s *workflowAwareSchedulerSuite) TestStart_WithSequentialSchedulerEnabled() {
	s.baseScheduler.EXPECT().Start().Times(1)
	s.baseScheduler.EXPECT().Stop().Times(1)

	scheduler := NewWorkflowAwareScheduler(
		s.baseScheduler,
		WorkflowAwareSchedulerOptions{
			EnableSequentialScheduler:      func() bool { return true },
			SequentialSchedulerQueueSize:   func() int { return 100 },
			SequentialSchedulerWorkerCount: func(_ func(int)) (int, func()) { return 1, func() {} },
		},
		s.logger,
	)
	scheduler.Start()
	scheduler.Stop()
}

func (s *workflowAwareSchedulerSuite) TestStart_AlwaysStartsSequentialScheduler() {
	// Even when EnableSequentialScheduler returns false, the sequential scheduler
	// is always started to handle dynamic config changes from disabled to enabled.
	s.baseScheduler.EXPECT().Start().Times(1)
	s.baseScheduler.EXPECT().Stop().Times(1)

	scheduler := NewWorkflowAwareScheduler(
		s.baseScheduler,
		WorkflowAwareSchedulerOptions{
			EnableSequentialScheduler:      func() bool { return false },
			SequentialSchedulerQueueSize:   func() int { return 100 },
			SequentialSchedulerWorkerCount: func(_ func(int)) (int, func()) { return 1, func() {} },
		},
		s.logger,
	)
	scheduler.Start()
	scheduler.Stop()
}

func (s *workflowAwareSchedulerSuite) TestStop_StopsBothSchedulers() {
	s.baseScheduler.EXPECT().Start().Times(1)
	s.baseScheduler.EXPECT().Stop().Times(1)

	scheduler := NewWorkflowAwareScheduler(
		s.baseScheduler,
		WorkflowAwareSchedulerOptions{
			EnableSequentialScheduler:      func() bool { return true },
			SequentialSchedulerQueueSize:   func() int { return 100 },
			SequentialSchedulerWorkerCount: func(_ func(int)) (int, func()) { return 1, func() {} },
		},
		s.logger,
	)
	scheduler.Start()
	scheduler.Stop()
}

// =============================================================================
// Submit Tests
// =============================================================================

func (s *workflowAwareSchedulerSuite) TestSubmit_RoutesToBaseWhenSequentialDisabled() {
	mockExec := NewMockExecutable(s.controller)
	s.baseScheduler.EXPECT().Submit(mockExec).Times(1)

	scheduler := NewWorkflowAwareScheduler(
		s.baseScheduler,
		WorkflowAwareSchedulerOptions{
			EnableSequentialScheduler:      func() bool { return false },
			SequentialSchedulerQueueSize:   func() int { return 100 },
			SequentialSchedulerWorkerCount: func(_ func(int)) (int, func()) { return 1, func() {} },
		},
		s.logger,
	)
	scheduler.Submit(mockExec)
}

func (s *workflowAwareSchedulerSuite) TestSubmit_RoutesToBaseWhenNoActiveQueue() {
	mockExec := NewMockExecutable(s.controller)
	mockExec.EXPECT().GetNamespaceID().Return("ns1").AnyTimes()
	mockExec.EXPECT().GetWorkflowID().Return("wf1").AnyTimes()
	mockExec.EXPECT().GetRunID().Return("run1").AnyTimes()
	s.baseScheduler.EXPECT().Submit(mockExec).Times(1)
	s.baseScheduler.EXPECT().Start().Times(1)
	s.baseScheduler.EXPECT().Stop().Times(1)

	scheduler := NewWorkflowAwareScheduler(
		s.baseScheduler,
		WorkflowAwareSchedulerOptions{
			EnableSequentialScheduler:      func() bool { return true },
			SequentialSchedulerQueueSize:   func() int { return 100 },
			SequentialSchedulerWorkerCount: func(_ func(int)) (int, func()) { return 1, func() {} },
		},
		s.logger,
	)
	scheduler.Start()
	defer scheduler.Stop()

	// No active queue, should route to base
	scheduler.Submit(mockExec)
}

func (s *workflowAwareSchedulerSuite) TestSubmit_RoutesToSequentialWhenActiveQueue() {
	s.baseScheduler.EXPECT().Start().Times(1)
	s.baseScheduler.EXPECT().Stop().Times(1)

	scheduler := NewWorkflowAwareScheduler(
		s.baseScheduler,
		WorkflowAwareSchedulerOptions{
			EnableSequentialScheduler:      func() bool { return true },
			SequentialSchedulerQueueSize:   func() int { return 100 },
			SequentialSchedulerWorkerCount: func(_ func(int)) (int, func()) { return 1, func() {} },
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

	// First, route to sequential scheduler via HandleBusyWorkflow
	s.True(scheduler.HandleBusyWorkflow(mockExec1))

	// Wait for execution to start
	<-execStarted

	// Now there's an active queue, subsequent submit should route to sequential
	// Add task 2 which will be queued behind task 1
	mockExec2 := s.createMockExecutable("ns1", "wf1", "run1")
	mockExec2.EXPECT().RetryPolicy().Return(backoff.NewExponentialRetryPolicy(time.Millisecond)).AnyTimes()
	mockExec2.EXPECT().Execute().Return(nil).Times(1)
	mockExec2.EXPECT().Ack().Times(1)

	// HandleBusyWorkflow instead of Submit since we want to add to sequential queue
	// (Submit checks HasQueue which may not return true if timing is wrong)
	s.True(scheduler.HandleBusyWorkflow(mockExec2))

	// Verify queue exists for this workflow before task1 completes
	mockExec3 := s.createMockExecutable("ns1", "wf1", "run1")
	s.True(scheduler.HasWorkflowQueue(mockExec3))

	// Add a third task via Submit - should route to sequential since queue exists
	mockExec4 := s.createMockExecutable("ns1", "wf1", "run1")
	mockExec4.EXPECT().RetryPolicy().Return(backoff.NewExponentialRetryPolicy(time.Millisecond)).AnyTimes()
	mockExec4.EXPECT().Execute().Return(nil).Times(1)
	mockExec4.EXPECT().Ack().Times(1)
	scheduler.Submit(mockExec4) // Should route to sequential because queue exists

	// Let tasks complete
	close(execContinue)
	time.Sleep(100 * time.Millisecond) // Give time for tasks to complete
}

// =============================================================================
// TrySubmit Tests
// =============================================================================

func (s *workflowAwareSchedulerSuite) TestTrySubmit_RoutesToBaseWhenSequentialDisabled() {
	mockExec := NewMockExecutable(s.controller)
	s.baseScheduler.EXPECT().TrySubmit(mockExec).Return(true).Times(1)

	scheduler := NewWorkflowAwareScheduler(
		s.baseScheduler,
		WorkflowAwareSchedulerOptions{
			EnableSequentialScheduler:      func() bool { return false },
			SequentialSchedulerQueueSize:   func() int { return 100 },
			SequentialSchedulerWorkerCount: func(_ func(int)) (int, func()) { return 1, func() {} },
		},
		s.logger,
	)
	s.True(scheduler.TrySubmit(mockExec))
}

func (s *workflowAwareSchedulerSuite) TestTrySubmit_RoutesToBaseWhenNoActiveQueue() {
	mockExec := NewMockExecutable(s.controller)
	mockExec.EXPECT().GetNamespaceID().Return("ns1").AnyTimes()
	mockExec.EXPECT().GetWorkflowID().Return("wf1").AnyTimes()
	mockExec.EXPECT().GetRunID().Return("run1").AnyTimes()
	s.baseScheduler.EXPECT().TrySubmit(mockExec).Return(true).Times(1)
	s.baseScheduler.EXPECT().Start().Times(1)
	s.baseScheduler.EXPECT().Stop().Times(1)

	scheduler := NewWorkflowAwareScheduler(
		s.baseScheduler,
		WorkflowAwareSchedulerOptions{
			EnableSequentialScheduler:      func() bool { return true },
			SequentialSchedulerQueueSize:   func() int { return 100 },
			SequentialSchedulerWorkerCount: func(_ func(int)) (int, func()) { return 1, func() {} },
		},
		s.logger,
	)
	scheduler.Start()
	defer scheduler.Stop()

	s.True(scheduler.TrySubmit(mockExec))
}

func (s *workflowAwareSchedulerSuite) TestTrySubmit_AddsToExistingQueueSuccessfully() {
	// When TrySubmit is called for a workflow with an active queue,
	// it should route to sequential and add to the existing queue (returning true)
	s.baseScheduler.EXPECT().Start().Times(1)
	s.baseScheduler.EXPECT().Stop().Times(1)

	scheduler := NewWorkflowAwareScheduler(
		s.baseScheduler,
		WorkflowAwareSchedulerOptions{
			EnableSequentialScheduler:      func() bool { return true },
			SequentialSchedulerQueueSize:   func() int { return 100 },
			SequentialSchedulerWorkerCount: func(_ func(int)) (int, func()) { return 1, func() {} },
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
// TaskChannelKeyFn Tests
// =============================================================================

func (s *workflowAwareSchedulerSuite) TestTaskChannelKeyFn_DelegatesToBase() {
	expectedFn := func(e Executable) TaskChannelKey {
		return TaskChannelKey{NamespaceID: "test"}
	}
	s.baseScheduler.EXPECT().TaskChannelKeyFn().Return(expectedFn).Times(1)

	scheduler := NewWorkflowAwareScheduler(
		s.baseScheduler,
		WorkflowAwareSchedulerOptions{},
		s.logger,
	)
	fn := scheduler.TaskChannelKeyFn()
	s.NotNil(fn)
}

// =============================================================================
// HandleBusyWorkflow Tests
// =============================================================================

func (s *workflowAwareSchedulerSuite) TestHandleBusyWorkflow_ReturnsFalseWhenDisabled() {
	mockExec := NewMockExecutable(s.controller)

	scheduler := NewWorkflowAwareScheduler(
		s.baseScheduler,
		WorkflowAwareSchedulerOptions{
			EnableSequentialScheduler:      func() bool { return false },
			SequentialSchedulerQueueSize:   func() int { return 100 },
			SequentialSchedulerWorkerCount: func(_ func(int)) (int, func()) { return 1, func() {} },
		},
		s.logger,
	)
	s.False(scheduler.HandleBusyWorkflow(mockExec))
}

func (s *workflowAwareSchedulerSuite) TestHandleBusyWorkflow_SubmitsToSequential() {
	s.baseScheduler.EXPECT().Start().Times(1)
	s.baseScheduler.EXPECT().Stop().Times(1)

	scheduler := NewWorkflowAwareScheduler(
		s.baseScheduler,
		WorkflowAwareSchedulerOptions{
			EnableSequentialScheduler:      func() bool { return true },
			SequentialSchedulerQueueSize:   func() int { return 100 },
			SequentialSchedulerWorkerCount: func(_ func(int)) (int, func()) { return 1, func() {} },
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
	s.baseScheduler.EXPECT().Start().Times(1)
	s.baseScheduler.EXPECT().Stop().Times(1)

	// Use very small queue size
	scheduler := NewWorkflowAwareScheduler(
		s.baseScheduler,
		WorkflowAwareSchedulerOptions{
			EnableSequentialScheduler:      func() bool { return true },
			SequentialSchedulerQueueSize:   func() int { return 1 },
			SequentialSchedulerWorkerCount: func(_ func(int)) (int, func()) { return 0, func() {} }, // 0 workers
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
	mockExec := NewMockExecutable(s.controller)

	scheduler := NewWorkflowAwareScheduler(
		s.baseScheduler,
		WorkflowAwareSchedulerOptions{
			EnableSequentialScheduler:      func() bool { return false },
			SequentialSchedulerQueueSize:   func() int { return 100 },
			SequentialSchedulerWorkerCount: func(_ func(int)) (int, func()) { return 1, func() {} },
		},
		s.logger,
	)
	s.False(scheduler.HasWorkflowQueue(mockExec))
}

func (s *workflowAwareSchedulerSuite) TestHasWorkflowQueue_ReturnsFalseWhenNoQueue() {
	s.baseScheduler.EXPECT().Start().Times(1)
	s.baseScheduler.EXPECT().Stop().Times(1)

	scheduler := NewWorkflowAwareScheduler(
		s.baseScheduler,
		WorkflowAwareSchedulerOptions{
			EnableSequentialScheduler:      func() bool { return true },
			SequentialSchedulerQueueSize:   func() int { return 100 },
			SequentialSchedulerWorkerCount: func(_ func(int)) (int, func()) { return 1, func() {} },
		},
		s.logger,
	)
	scheduler.Start()
	defer scheduler.Stop()

	mockExec := s.createMockExecutable("ns1", "wf1", "run1")
	s.False(scheduler.HasWorkflowQueue(mockExec))
}

func (s *workflowAwareSchedulerSuite) TestHasWorkflowQueue_ReturnsTrueWhenQueueExists() {
	s.baseScheduler.EXPECT().Start().Times(1)
	s.baseScheduler.EXPECT().Stop().Times(1)

	scheduler := NewWorkflowAwareScheduler(
		s.baseScheduler,
		WorkflowAwareSchedulerOptions{
			EnableSequentialScheduler:      func() bool { return true },
			SequentialSchedulerQueueSize:   func() int { return 100 },
			SequentialSchedulerWorkerCount: func(_ func(int)) (int, func()) { return 1, func() {} },
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

func (s *workflowAwareSchedulerSuite) TestWorkflowKeyHashFn() {
	key1 := definition.NewWorkflowKey("ns1", "wf1", "run1")
	key2 := definition.NewWorkflowKey("ns1", "wf1", "run1")
	key3 := definition.NewWorkflowKey("ns2", "wf2", "run2")

	// Same keys should produce same hash
	s.Equal(workflowKeyHashFn(key1), workflowKeyHashFn(key2))

	// Different keys should produce different hash (with high probability)
	s.NotEqual(workflowKeyHashFn(key1), workflowKeyHashFn(key3))

	// Invalid key type should return 0
	s.Equal(uint32(0), workflowKeyHashFn("invalid"))
}

func (s *workflowAwareSchedulerSuite) TestWorkflowTaskQueueFactory() {
	mockExec := s.createMockExecutable("ns1", "wf1", "run1")

	queue := workflowTaskQueueFactory(mockExec)
	s.NotNil(queue)
	s.Equal(definition.NewWorkflowKey("ns1", "wf1", "run1"), queue.ID())
}

// =============================================================================
// Concurrent Tests
// =============================================================================

func (s *workflowAwareSchedulerSuite) TestConcurrentSubmit() {
	s.baseScheduler.EXPECT().Start().Times(1)
	s.baseScheduler.EXPECT().Stop().Times(1)
	s.baseScheduler.EXPECT().Submit(gomock.Any()).AnyTimes()

	scheduler := NewWorkflowAwareScheduler(
		s.baseScheduler,
		WorkflowAwareSchedulerOptions{
			EnableSequentialScheduler:      func() bool { return true },
			SequentialSchedulerQueueSize:   func() int { return 1000 },
			SequentialSchedulerWorkerCount: func(_ func(int)) (int, func()) { return 4, func() {} },
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
}

func (s *workflowAwareSchedulerSuite) TestConcurrentHandleBusyWorkflow() {
	s.baseScheduler.EXPECT().Start().Times(1)
	s.baseScheduler.EXPECT().Stop().Times(1)

	scheduler := NewWorkflowAwareScheduler(
		s.baseScheduler,
		WorkflowAwareSchedulerOptions{
			EnableSequentialScheduler:      func() bool { return true },
			SequentialSchedulerQueueSize:   func() int { return 1000 },
			SequentialSchedulerWorkerCount: func(_ func(int)) (int, func()) { return 4, func() {} },
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
// Nil Options Tests
// =============================================================================

func (s *workflowAwareSchedulerSuite) TestNilEnableOption() {
	s.baseScheduler.EXPECT().Submit(gomock.Any()).Times(1)

	scheduler := NewWorkflowAwareScheduler(
		s.baseScheduler,
		WorkflowAwareSchedulerOptions{
			EnableSequentialScheduler:      nil, // nil option
			SequentialSchedulerQueueSize:   func() int { return 100 },
			SequentialSchedulerWorkerCount: func(_ func(int)) (int, func()) { return 1, func() {} },
		},
		s.logger,
	)

	mockExec := NewMockExecutable(s.controller)
	scheduler.Submit(mockExec)
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
