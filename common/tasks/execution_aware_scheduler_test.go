package tasks

import (
	"sync"
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
	scheduler, _ := s.createSchedulerWithMock(false)
	scheduler.Start()
	scheduler.Stop()
}

func (s *executionAwareSchedulerSuite) TestSubmit_DelegatesToBaseWhenDisabled() {
	scheduler, mockBaseScheduler := s.createSchedulerWithMock(false)
	mockTask := s.createTestTask("wf1", "run1")
	mockBaseScheduler.EXPECT().Submit(mockTask).Times(1)

	scheduler.Start()
	defer scheduler.Stop()

	scheduler.Submit(mockTask)
}

func (s *executionAwareSchedulerSuite) TestSubmit_RoutesToBaseWhenNoActiveQueue() {
	scheduler, mockBaseScheduler := s.createSchedulerWithMock(true)
	mockTask := s.createTestTask("wf1", "run1")
	mockBaseScheduler.EXPECT().Submit(mockTask).Times(1)

	scheduler.Start()
	defer scheduler.Stop()

	scheduler.Submit(mockTask)
}

func (s *executionAwareSchedulerSuite) TestSubmit_RoutesToExecutionQueueSchedulerWhenActiveQueue() {
	scheduler, _ := s.createSchedulerWithMock(true)
	scheduler.Start()
	defer scheduler.Stop()

	// Create an active queue by submitting via HandleBusyWorkflow
	task1 := s.createTestTask("wf1", "run1")
	task1.EXPECT().RetryPolicy().Return(backoff.NewExponentialRetryPolicy(time.Millisecond)).AnyTimes()

	execStarted := make(chan struct{})
	execContinue := make(chan struct{})
	var completionWG sync.WaitGroup
	completionWG.Add(3) // 3 tasks total
	task1.EXPECT().Execute().DoAndReturn(func() error {
		close(execStarted)
		<-execContinue
		return nil
	}).Times(1)
	task1.EXPECT().Ack().Do(func() { completionWG.Done() }).Times(1)

	s.True(scheduler.HandleBusyWorkflow(task1))
	<-execStarted

	// Add task 2 via HandleBusyWorkflow
	task2 := s.createTestTask("wf1", "run1")
	task2.EXPECT().RetryPolicy().Return(backoff.NewExponentialRetryPolicy(time.Millisecond)).AnyTimes()
	task2.EXPECT().Execute().Return(nil).Times(1)
	task2.EXPECT().Ack().Do(func() { completionWG.Done() }).Times(1)
	s.True(scheduler.HandleBusyWorkflow(task2))

	// Verify queue exists
	task3 := s.createTestTask("wf1", "run1")
	s.True(scheduler.HasExecutionQueue(task3))

	// Add a third task via Submit - should route to executionQueueScheduler since queue exists
	task4 := s.createTestTask("wf1", "run1")
	task4.EXPECT().RetryPolicy().Return(backoff.NewExponentialRetryPolicy(time.Millisecond)).AnyTimes()
	task4.EXPECT().Execute().Return(nil).Times(1)
	task4.EXPECT().Ack().Do(func() { completionWG.Done() }).Times(1)
	scheduler.Submit(task4)

	close(execContinue)
	completionWG.Wait()
}

func (s *executionAwareSchedulerSuite) TestTrySubmit_DelegatesToBaseWhenDisabled() {
	scheduler, mockBaseScheduler := s.createSchedulerWithMock(false)
	mockTask := s.createTestTask("wf1", "run1")
	mockBaseScheduler.EXPECT().TrySubmit(mockTask).Return(true).Times(1)

	scheduler.Start()
	defer scheduler.Stop()

	s.True(scheduler.TrySubmit(mockTask))
}

func (s *executionAwareSchedulerSuite) TestTrySubmit_RoutesToBaseWhenNoActiveQueue() {
	scheduler, mockBaseScheduler := s.createSchedulerWithMock(true)
	mockTask := s.createTestTask("wf1", "run1")
	mockBaseScheduler.EXPECT().TrySubmit(mockTask).Return(true).Times(1)

	scheduler.Start()
	defer scheduler.Stop()

	s.True(scheduler.TrySubmit(mockTask))
}

func (s *executionAwareSchedulerSuite) TestTrySubmit_AddsToExistingQueueSuccessfully() {
	scheduler, _ := s.createSchedulerWithMock(true)
	scheduler.Start()
	defer scheduler.Stop()

	// First task blocks execution - this creates a queue
	task1 := s.createTestTask("wf1", "run1")
	task1.EXPECT().RetryPolicy().Return(backoff.NewExponentialRetryPolicy(time.Millisecond)).AnyTimes()
	execStarted := make(chan struct{})
	execContinue := make(chan struct{})
	var completionWG sync.WaitGroup
	completionWG.Add(3)
	task1.EXPECT().Execute().DoAndReturn(func() error {
		close(execStarted)
		<-execContinue
		return nil
	}).Times(1)
	task1.EXPECT().Ack().Do(func() { completionWG.Done() }).Times(1)
	s.True(scheduler.HandleBusyWorkflow(task1))

	<-execStarted

	// Add second task to keep queue active
	task2 := s.createTestTask("wf1", "run1")
	task2.EXPECT().RetryPolicy().Return(backoff.NewExponentialRetryPolicy(time.Millisecond)).AnyTimes()
	task2.EXPECT().Execute().Return(nil).Times(1)
	task2.EXPECT().Ack().Do(func() { completionWG.Done() }).Times(1)
	scheduler.HandleBusyWorkflow(task2)

	// TrySubmit for same key which has an active queue - should succeed
	task3 := s.createTestTask("wf1", "run1")
	task3.EXPECT().RetryPolicy().Return(backoff.NewExponentialRetryPolicy(time.Millisecond)).AnyTimes()
	task3.EXPECT().Execute().Return(nil).Times(1)
	task3.EXPECT().Ack().Do(func() { completionWG.Done() }).Times(1)
	s.True(scheduler.TrySubmit(task3))

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
	task1 := s.createTestTask("wf1", "run1")
	task1.EXPECT().RetryPolicy().Return(backoff.NewExponentialRetryPolicy(time.Millisecond)).AnyTimes()
	task1.EXPECT().Execute().DoAndReturn(func() error {
		close(execStarted)
		<-blockCh
		return nil
	}).MaxTimes(1)
	task1.EXPECT().Ack().Do(func() { completionWG.Done() }).MaxTimes(1)
	task1.EXPECT().Abort().MaxTimes(1)
	s.True(scheduler.HandleBusyWorkflow(task1))

	<-execStarted

	// TrySubmit for a different key - queue is full, should go to base scheduler
	task2 := s.createTestTask("wf2", "run2")
	mockBaseScheduler.EXPECT().TrySubmit(task2).Return(true).Times(1)
	s.True(scheduler.TrySubmit(task2))

	close(blockCh)
	completionWG.Wait()
}

func (s *executionAwareSchedulerSuite) TestHandleBusyWorkflow_ReturnsFalseWhenDisabled() {
	scheduler, _ := s.createSchedulerWithoutLifecycle(false)
	mockTask := s.createTestTask("wf1", "run1")
	s.False(scheduler.HandleBusyWorkflow(mockTask))
}

func (s *executionAwareSchedulerSuite) TestHandleBusyWorkflow_SubmitsToExecutionQueueScheduler() {
	scheduler, _ := s.createSchedulerWithMock(true)
	scheduler.Start()
	defer scheduler.Stop()

	var completionWG sync.WaitGroup
	completionWG.Add(1)
	task := s.createTestTask("wf1", "run1")
	task.EXPECT().RetryPolicy().Return(backoff.NewExponentialRetryPolicy(time.Millisecond)).AnyTimes()
	task.EXPECT().Execute().Return(nil).Times(1)
	task.EXPECT().Ack().Do(func() { completionWG.Done() }).Times(1)

	s.True(scheduler.HandleBusyWorkflow(task))
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
	task1 := s.createTestTask("wf1", "run1")
	task1.EXPECT().RetryPolicy().Return(backoff.NewExponentialRetryPolicy(time.Millisecond)).AnyTimes()
	task1.EXPECT().Execute().DoAndReturn(func() error {
		close(execStarted)
		<-blockCh
		return nil
	}).MaxTimes(1)
	task1.EXPECT().Ack().Do(func() { completionWG.Done() }).MaxTimes(1)
	task1.EXPECT().Abort().MaxTimes(1)
	s.True(scheduler.HandleBusyWorkflow(task1))

	<-execStarted

	// Second task for a DIFFERENT key should return false (MaxQueues reached)
	task2 := s.createTestTask("wf2", "run2")
	s.False(scheduler.HandleBusyWorkflow(task2))

	close(blockCh)
	completionWG.Wait()
}

func (s *executionAwareSchedulerSuite) TestHasExecutionQueue_ReturnsFalseWhenDisabled() {
	scheduler, _ := s.createSchedulerWithoutLifecycle(false)
	mockTask := s.createTestTask("wf1", "run1")
	s.False(scheduler.HasExecutionQueue(mockTask))
}

func (s *executionAwareSchedulerSuite) TestHasExecutionQueue_ReturnsFalseWhenNoQueue() {
	scheduler, _ := s.createSchedulerWithMock(true)
	scheduler.Start()
	defer scheduler.Stop()

	mockTask := s.createTestTask("wf1", "run1")
	s.False(scheduler.HasExecutionQueue(mockTask))
}

func (s *executionAwareSchedulerSuite) TestHasExecutionQueue_ReturnsTrueWhenQueueExists() {
	scheduler, _ := s.createSchedulerWithMock(true)
	scheduler.Start()
	defer scheduler.Stop()

	// Create a queue by submitting a task
	task1 := s.createTestTask("wf1", "run1")
	task1.EXPECT().RetryPolicy().Return(backoff.NewExponentialRetryPolicy(time.Millisecond)).AnyTimes()

	execStarted := make(chan struct{})
	execContinue := make(chan struct{})
	var completionWG sync.WaitGroup
	completionWG.Add(2)
	task1.EXPECT().Execute().DoAndReturn(func() error {
		close(execStarted)
		<-execContinue
		return nil
	}).Times(1)
	task1.EXPECT().Ack().Do(func() { completionWG.Done() }).Times(1)

	scheduler.HandleBusyWorkflow(task1)
	<-execStarted

	// Add another task to ensure queue stays alive
	task2 := s.createTestTask("wf1", "run1")
	task2.EXPECT().RetryPolicy().Return(backoff.NewExponentialRetryPolicy(time.Millisecond)).AnyTimes()
	task2.EXPECT().Execute().Return(nil).Times(1)
	task2.EXPECT().Ack().Do(func() { completionWG.Done() }).Times(1)
	scheduler.HandleBusyWorkflow(task2)

	// Check that queue exists for this key
	task3 := s.createTestTask("wf1", "run1")
	s.True(scheduler.HasExecutionQueue(task3))

	// Check that queue doesn't exist for different key
	task4 := s.createTestTask("wf2", "run2")
	s.False(scheduler.HasExecutionQueue(task4))

	close(execContinue)
	completionWG.Wait()
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
			mockTask := s.createTestTask("wf1", "run1")
			scheduler.Submit(mockTask)
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
			mockTask := s.createTestTask("wf1", "run1")
			mockTask.EXPECT().RetryPolicy().Return(backoff.NewExponentialRetryPolicy(time.Millisecond)).AnyTimes()
			mockTask.EXPECT().Execute().Return(nil).MaxTimes(1)
			mockTask.EXPECT().Ack().Do(func() { completionWG.Done() }).MaxTimes(1)
			mockTask.EXPECT().Abort().Do(func() { completionWG.Done() }).MaxTimes(1)
			scheduler.HandleBusyWorkflow(mockTask)
		}()
	}

	submitWG.Wait()
	completionWG.Wait()
}

// Helper functions

func (s *executionAwareSchedulerSuite) createTestTask(workflowID, runID string) *testExecutionTask {
	mockTask := NewMockTask(s.controller)
	return &testExecutionTask{
		MockTask:   mockTask,
		workflowID: workflowID,
		runID:      runID,
	}
}

func (s *executionAwareSchedulerSuite) defaultSchedulerOptions(enabled bool) ExecutionAwareSchedulerOptions {
	return ExecutionAwareSchedulerOptions{
		Enabled:          func() bool { return enabled },
		MaxQueues:        func() int { return 500 },
		QueueTTL:         func() time.Duration { return 5 * time.Second },
		QueueConcurrency: func() int { return 1 },
	}
}

func (s *executionAwareSchedulerSuite) createSchedulerWithMock(enabled bool) (*ExecutionAwareScheduler[*testExecutionTask], *MockScheduler[*testExecutionTask]) {
	mockBaseScheduler := NewMockScheduler[*testExecutionTask](s.controller)
	mockBaseScheduler.EXPECT().Start().Times(1)
	mockBaseScheduler.EXPECT().Stop().Times(1)

	scheduler := NewExecutionAwareScheduler[*testExecutionTask](
		mockBaseScheduler,
		s.defaultSchedulerOptions(enabled),
		executionKeyFn,
		s.logger,
		metrics.NoopMetricsHandler,
		clock.NewRealTimeSource(),
	)
	return scheduler, mockBaseScheduler
}

func (s *executionAwareSchedulerSuite) createSchedulerWithoutLifecycle(enabled bool) (*ExecutionAwareScheduler[*testExecutionTask], *MockScheduler[*testExecutionTask]) {
	mockBaseScheduler := NewMockScheduler[*testExecutionTask](s.controller)
	scheduler := NewExecutionAwareScheduler[*testExecutionTask](
		mockBaseScheduler,
		s.defaultSchedulerOptions(enabled),
		executionKeyFn,
		s.logger,
		metrics.NoopMetricsHandler,
		clock.NewRealTimeSource(),
	)
	return scheduler, mockBaseScheduler
}

func (s *executionAwareSchedulerSuite) createSchedulerWithMaxQueues(maxQueues int) (*ExecutionAwareScheduler[*testExecutionTask], *MockScheduler[*testExecutionTask]) {
	mockBaseScheduler := NewMockScheduler[*testExecutionTask](s.controller)
	mockBaseScheduler.EXPECT().Start().Times(1)
	mockBaseScheduler.EXPECT().Stop().Times(1)

	opts := s.defaultSchedulerOptions(true)
	opts.MaxQueues = func() int { return maxQueues }

	scheduler := NewExecutionAwareScheduler[*testExecutionTask](
		mockBaseScheduler,
		opts,
		executionKeyFn,
		s.logger,
		metrics.NoopMetricsHandler,
		clock.NewRealTimeSource(),
	)
	return scheduler, mockBaseScheduler
}
