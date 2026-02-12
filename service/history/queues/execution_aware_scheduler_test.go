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

func (s *executionAwareSchedulerSuite) TestStartStop_Enabled() {
	scheduler, _ := s.createSchedulerWithMock(true)
	scheduler.Start()
	scheduler.Stop()
}

func (s *executionAwareSchedulerSuite) TestStartStop_Disabled() {
	// Even when EnableExecutionQueueScheduler returns false, the ExecutionQueueScheduler
	// is always started to handle dynamic config changes from disabled to enabled.
	scheduler, _ := s.createSchedulerWithMock(false)
	scheduler.Start()
	scheduler.Stop()
}

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
	scheduler, _ := s.createSchedulerWithMock(true)
	scheduler.Start()
	defer scheduler.Stop()

	// Create an active queue by submitting via HandleBusyWorkflow
	mockExec1 := s.createMockExecutable("ns1", "wf1", "run1")
	mockExec1.EXPECT().RetryPolicy().Return(backoff.NewExponentialRetryPolicy(time.Millisecond)).AnyTimes()

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

	s.True(scheduler.HandleBusyWorkflow(mockExec1))
	<-execStarted

	// Add task 2 via HandleBusyWorkflow
	mockExec2 := s.createMockExecutable("ns1", "wf1", "run1")
	mockExec2.EXPECT().RetryPolicy().Return(backoff.NewExponentialRetryPolicy(time.Millisecond)).AnyTimes()
	mockExec2.EXPECT().Execute().Return(nil).Times(1)
	mockExec2.EXPECT().Ack().Do(func() { completionWG.Done() }).Times(1)
	s.True(scheduler.HandleBusyWorkflow(mockExec2))

	// Verify queue exists
	mockExec3 := s.createMockExecutable("ns1", "wf1", "run1")
	s.True(scheduler.HasExecutionQueue(mockExec3))

	// Add a third task via Submit - should route to ExecutionQueueScheduler since queue exists
	mockExec4 := s.createMockExecutable("ns1", "wf1", "run1")
	mockExec4.EXPECT().RetryPolicy().Return(backoff.NewExponentialRetryPolicy(time.Millisecond)).AnyTimes()
	mockExec4.EXPECT().Execute().Return(nil).Times(1)
	mockExec4.EXPECT().Ack().Do(func() { completionWG.Done() }).Times(1)
	scheduler.Submit(mockExec4)

	close(execContinue)
	completionWG.Wait()
}

func (s *executionAwareSchedulerSuite) TestTrySubmit_DelegatesToBaseWhenExecutionQueueSchedulerDisabled() {
	scheduler, mockBaseScheduler := s.createSchedulerWithMock(false)
	mockExec := s.createMockExecutable("ns1", "wf1", "run1")
	mockBaseScheduler.EXPECT().TrySubmit(mockExec).Return(true).Times(1)

	scheduler.Start()
	defer scheduler.Stop()

	s.True(scheduler.TrySubmit(mockExec))
}

func (s *executionAwareSchedulerSuite) TestTrySubmit_RoutesToBaseWhenNoActiveQueue() {
	scheduler, mockBaseScheduler := s.createSchedulerWithMock(true)
	mockExec := s.createMockExecutable("ns1", "wf1", "run1")
	mockBaseScheduler.EXPECT().TrySubmit(mockExec).Return(true).Times(1)

	scheduler.Start()
	defer scheduler.Stop()

	s.True(scheduler.TrySubmit(mockExec))
}

func (s *executionAwareSchedulerSuite) TestTrySubmit_AddsToExistingQueueSuccessfully() {
	scheduler, _ := s.createSchedulerWithMock(true)
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

	<-execStarted

	// Add second task to keep queue active
	mockExec2 := s.createMockExecutable("ns1", "wf1", "run1")
	mockExec2.EXPECT().RetryPolicy().Return(backoff.NewExponentialRetryPolicy(time.Millisecond)).AnyTimes()
	mockExec2.EXPECT().Execute().Return(nil).Times(1)
	mockExec2.EXPECT().Ack().Do(func() { completionWG.Done() }).Times(1)
	scheduler.HandleBusyWorkflow(mockExec2)

	// TrySubmit for wf1 which has an active queue - should succeed
	mockExec3 := s.createMockExecutable("ns1", "wf1", "run1")
	mockExec3.EXPECT().RetryPolicy().Return(backoff.NewExponentialRetryPolicy(time.Millisecond)).AnyTimes()
	mockExec3.EXPECT().Execute().Return(nil).Times(1)
	mockExec3.EXPECT().Ack().Do(func() { completionWG.Done() }).Times(1)
	s.True(scheduler.TrySubmit(mockExec3))

	close(execContinue)
	completionWG.Wait()
}

func (s *executionAwareSchedulerSuite) TestTrySubmit_RoutesToBaseWhenMaxQueuesReached() {
	scheduler, mockBaseScheduler := s.createSchedulerWithMaxQueues(1)
	scheduler.Start()
	defer scheduler.Stop()

	// First task blocks execution - fills the single queue slot
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

	<-execStarted

	// TrySubmit for a different workflow - queue is full, should go to base scheduler
	mockExec2 := s.createMockExecutable("ns1", "wf2", "run2")
	mockBaseScheduler.EXPECT().TrySubmit(mockExec2).Return(true).Times(1)
	s.True(scheduler.TrySubmit(mockExec2))

	close(blockCh)
	completionWG.Wait()
}

func (s *executionAwareSchedulerSuite) TestHandleBusyWorkflow_ReturnsFalseWhenDisabled() {
	scheduler, _ := s.createSchedulerWithoutLifecycle(false)
	mockExec := NewMockExecutable(s.controller)
	s.False(scheduler.HandleBusyWorkflow(mockExec))
}

func (s *executionAwareSchedulerSuite) TestHandleBusyWorkflow_SubmitsToExecutionQueueScheduler() {
	scheduler, _ := s.createSchedulerWithMock(true)
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

func (s *executionAwareSchedulerSuite) TestHandleBusyWorkflow_ReturnsFalseWhenMaxQueuesReached() {
	scheduler, _ := s.createSchedulerWithMaxQueues(1)
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

	<-execStarted

	// Second task for a DIFFERENT workflow should return false (MaxQueues reached)
	mockExec2 := s.createMockExecutable("ns1", "wf2", "run2")
	s.False(scheduler.HandleBusyWorkflow(mockExec2))

	close(blockCh)
	completionWG.Wait()
}

func (s *executionAwareSchedulerSuite) TestHasExecutionQueue_ReturnsFalseWhenDisabled() {
	scheduler, _ := s.createSchedulerWithoutLifecycle(false)
	mockExec := NewMockExecutable(s.controller)
	s.False(scheduler.HasExecutionQueue(mockExec))
}

func (s *executionAwareSchedulerSuite) TestHasExecutionQueue_ReturnsFalseWhenNoQueue() {
	scheduler, _ := s.createSchedulerWithMock(true)
	scheduler.Start()
	defer scheduler.Stop()

	mockExec := s.createMockExecutable("ns1", "wf1", "run1")
	s.False(scheduler.HasExecutionQueue(mockExec))
}

func (s *executionAwareSchedulerSuite) TestHasExecutionQueue_ReturnsTrueWhenQueueExists() {
	scheduler, _ := s.createSchedulerWithMock(true)
	scheduler.Start()
	defer scheduler.Stop()

	// Create a queue by submitting a task
	mockExec1 := s.createMockExecutable("ns1", "wf1", "run1")
	mockExec1.EXPECT().RetryPolicy().Return(backoff.NewExponentialRetryPolicy(time.Millisecond)).AnyTimes()

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

	// Add another task to ensure queue stays alive
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

func (s *executionAwareSchedulerSuite) TestExecutableQueueKeyFn() {
	mockExec := s.createMockExecutable("ns1", "wf1", "run1")

	key := executableQueueKeyFn(mockExec)
	s.Equal(definition.NewWorkflowKey("ns1", "wf1", "run1"), key)
}

func (s *executionAwareSchedulerSuite) TestConcurrentSubmit() {
	scheduler, mockBaseScheduler := s.createSchedulerWithMock(true)
	mockBaseScheduler.EXPECT().Submit(gomock.Any()).AnyTimes()

	scheduler.Start()
	defer scheduler.Stop()

	var wg sync.WaitGroup
	numTasks := 100
	wg.Add(numTasks)

	for range numTasks {
		go func() {
			defer wg.Done()
			mockExec := s.createMockExecutable("ns1", "wf1", "run1")
			scheduler.Submit(mockExec)
		}()
	}

	wg.Wait()
}

func (s *executionAwareSchedulerSuite) TestConcurrentHandleBusyWorkflow() {
	scheduler, _ := s.createSchedulerWithMock(true)
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

// Helper functions

func (s *executionAwareSchedulerSuite) createMockBaseScheduler() *tasks.MockScheduler[Executable] {
	return tasks.NewMockScheduler[Executable](s.controller)
}

func (s *executionAwareSchedulerSuite) createMockExecutable(namespaceID, workflowID, runID string) *MockExecutable {
	mockExec := NewMockExecutable(s.controller)
	mockExec.EXPECT().GetNamespaceID().Return(namespaceID).AnyTimes()
	mockExec.EXPECT().GetWorkflowID().Return(workflowID).AnyTimes()
	mockExec.EXPECT().GetRunID().Return(runID).AnyTimes()
	return mockExec
}

func (s *executionAwareSchedulerSuite) defaultSchedulerOptions(enabled bool) ExecutionAwareSchedulerOptions {
	return ExecutionAwareSchedulerOptions{
		EnableExecutionQueueScheduler: func() bool { return enabled },
		ExecutionQueueSchedulerOptions: tasks.ExecutionQueueSchedulerOptions{
			MaxQueues:        func() int { return 500 },
			QueueTTL:         func() time.Duration { return 5 * time.Second },
			QueueConcurrency: func() int { return 1 },
		},
	}
}

// createSchedulerWithMock creates a scheduler with Start/Stop expectations on the mock base.
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

// createSchedulerWithoutLifecycle creates a scheduler without Start/Stop expectations.
func (s *executionAwareSchedulerSuite) createSchedulerWithoutLifecycle(enabled bool) (*ExecutionAwareScheduler, *tasks.MockScheduler[Executable]) {
	mockBaseScheduler := s.createMockBaseScheduler()
	scheduler := NewExecutionAwareScheduler(
		mockBaseScheduler,
		s.defaultSchedulerOptions(enabled),
		s.logger,
		metrics.NoopMetricsHandler,
		clock.NewRealTimeSource(),
	)
	return scheduler, mockBaseScheduler
}

// createSchedulerWithMaxQueues creates a scheduler with MaxQueues=maxQueues and Start/Stop expectations.
func (s *executionAwareSchedulerSuite) createSchedulerWithMaxQueues(maxQueues int) (*ExecutionAwareScheduler, *tasks.MockScheduler[Executable]) {
	mockBaseScheduler := s.createMockBaseScheduler()
	mockBaseScheduler.EXPECT().Start().Times(1)
	mockBaseScheduler.EXPECT().Stop().Times(1)

	opts := s.defaultSchedulerOptions(true)
	opts.ExecutionQueueSchedulerOptions.MaxQueues = func() int { return maxQueues }

	scheduler := NewExecutionAwareScheduler(
		mockBaseScheduler,
		opts,
		s.logger,
		metrics.NoopMetricsHandler,
		clock.NewRealTimeSource(),
	)
	return scheduler, mockBaseScheduler
}
