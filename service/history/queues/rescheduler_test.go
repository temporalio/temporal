package queues

import (
	"math/rand"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/tasks"
	ctasks "go.temporal.io/server/common/tasks"
	htasks "go.temporal.io/server/service/history/tasks"
	"go.uber.org/mock/gomock"
)

type (
	rescheudulerSuite struct {
		suite.Suite
		*require.Assertions

		controller    *gomock.Controller
		mockScheduler *MockScheduler

		timeSource *clock.EventTimeSource

		rescheduler *reschedulerImpl
	}
)

func TestReschdulerSuite(t *testing.T) {
	s := new(rescheudulerSuite)
	suite.Run(t, s)
}

func (s *rescheudulerSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.mockScheduler = NewMockScheduler(s.controller)
	s.mockScheduler.EXPECT().TaskChannelKeyFn().Return(
		func(_ Executable) TaskChannelKey { return TaskChannelKey{} },
	).AnyTimes()

	s.timeSource = clock.NewEventTimeSource()

	s.rescheduler = NewRescheduler(
		s.mockScheduler,
		s.timeSource,
		log.NewTestLogger(),
		metrics.NoopMetricsHandler,
	)
}

func (s *rescheudulerSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *rescheudulerSuite) TestStartStop() {
	timeSource := clock.NewRealTimeSource()
	rescheduler := NewRescheduler(
		s.mockScheduler,
		timeSource,
		log.NewTestLogger(),
		metrics.NoopMetricsHandler,
	)

	rescheduler.Start()

	numTasks := 20
	taskCh := make(chan struct{}, numTasks)
	s.mockScheduler.EXPECT().TrySubmit(gomock.Any()).DoAndReturn(func(_ Executable) bool {
		taskCh <- struct{}{}
		return true
	}).Times(numTasks)

	for i := 0; i != numTasks; i++ {
		mockExecutable := NewMockExecutable(s.controller)
		mockExecutable.EXPECT().SetScheduledTime(gomock.Any()).Times(1)
		mockExecutable.EXPECT().State().Return(ctasks.TaskStatePending).Times(1)
		rescheduler.Add(
			mockExecutable,
			timeSource.Now().Add(time.Duration(rand.Int63n(300))*time.Millisecond),
		)
	}

	for i := 0; i != numTasks; i++ {
		<-taskCh
	}
	rescheduler.Stop()

	s.Equal(0, rescheduler.Len())
}

func (s *rescheudulerSuite) TestDrain() {
	timeSource := clock.NewRealTimeSource()
	rescheduler := NewRescheduler(
		s.mockScheduler,
		timeSource,
		log.NewTestLogger(),
		metrics.NoopMetricsHandler,
	)

	rescheduler.Start()
	rescheduler.Stop()

	for i := 0; i != 10; i++ {
		rescheduler.Add(
			NewMockExecutable(s.controller),
			timeSource.Now().Add(time.Duration(rand.Int63n(300))*time.Second),
		)
	}

	s.Equal(0, rescheduler.Len())
}

func (s *rescheudulerSuite) TestReschedule_NoRescheduleLimit() {
	now := time.Now()
	s.timeSource.Update(now)
	rescheduleInterval := time.Minute

	numExecutable := 10
	for i := 0; i != numExecutable/2; i++ {
		mockTask := NewMockExecutable(s.controller)
		mockTask.EXPECT().SetScheduledTime(gomock.Any()).AnyTimes()
		mockTask.EXPECT().State().Return(ctasks.TaskStatePending).AnyTimes()
		s.rescheduler.Add(
			mockTask,
			now.Add(time.Duration(rand.Int63n(rescheduleInterval.Nanoseconds()))),
		)

		s.rescheduler.Add(
			NewMockExecutable(s.controller),
			now.Add(rescheduleInterval+time.Duration(rand.Int63n(time.Minute.Nanoseconds()))),
		)
	}
	s.Equal(numExecutable, s.rescheduler.Len())

	s.mockScheduler.EXPECT().TrySubmit(gomock.Any()).Return(true).Times(numExecutable / 2)

	s.timeSource.Update(now.Add(rescheduleInterval))
	s.rescheduler.reschedule()
	s.Equal(numExecutable/2, s.rescheduler.Len())
}

func (s *rescheudulerSuite) TestReschedule_TaskChanFull() {
	now := time.Now()
	s.timeSource.Update(now)
	rescheduleInterval := time.Minute

	numExecutable := 10
	for i := 0; i != numExecutable; i++ {
		mockTask := NewMockExecutable(s.controller)
		mockTask.EXPECT().SetScheduledTime(gomock.Any()).AnyTimes()
		mockTask.EXPECT().State().Return(ctasks.TaskStatePending).MaxTimes(1)
		s.rescheduler.Add(
			mockTask,
			now.Add(time.Duration(rand.Int63n(rescheduleInterval.Nanoseconds()))),
		)
	}
	s.Equal(numExecutable, s.rescheduler.Len())

	numSubmitted := 0
	numAllowed := numExecutable / 2
	s.mockScheduler.EXPECT().TrySubmit(gomock.Any()).DoAndReturn(func(_ Executable) bool {
		if numSubmitted < numAllowed {
			numSubmitted++
			return true
		}

		return false
	}).Times(numAllowed + 1)

	s.timeSource.Update(now.Add(rescheduleInterval))
	s.rescheduler.reschedule()
	s.Equal(numExecutable-numSubmitted, s.rescheduler.Len())
}

func (s *rescheudulerSuite) TestReschedule_DropCancelled() {
	now := time.Now()
	s.timeSource.Update(now)
	rescheduleInterval := time.Minute

	for i := 0; i != 10; i++ {
		mockTask := NewMockExecutable(s.controller)
		mockTask.EXPECT().State().Return(ctasks.TaskStateCancelled).Times(1)
		s.rescheduler.Add(
			mockTask,
			now.Add(time.Duration(rand.Int63n(rescheduleInterval.Nanoseconds()))),
		)
	}

	s.timeSource.Update(now.Add(rescheduleInterval))
	s.rescheduler.reschedule()
	s.Equal(0, s.rescheduler.Len())
}

func (s *rescheudulerSuite) TestForceReschedule_ImmediateTask() {
	now := time.Now()
	s.timeSource.Update(now)
	namespaceID := s.mockScheduler.TaskChannelKeyFn()(nil).NamespaceID

	s.rescheduler.Start()
	defer s.rescheduler.Stop()

	numTask := 10
	taskWG := &sync.WaitGroup{}
	taskWG.Add(numTask)
	for i := 0; i != numTask; i++ {
		mockTask := NewMockExecutable(s.controller)
		mockTask.EXPECT().State().Return(ctasks.TaskStatePending).AnyTimes()
		mockTask.EXPECT().SetScheduledTime(gomock.Any()).AnyTimes()
		mockTask.EXPECT().GetKey().Return(htasks.NewImmediateKey(int64(i))).AnyTimes()
		s.rescheduler.Add(
			mockTask,
			now.Add(time.Minute+time.Duration(rand.Int63n(time.Minute.Nanoseconds()))),
		)
	}

	s.mockScheduler.EXPECT().TrySubmit(gomock.Any()).DoAndReturn(func(_ Executable) bool {
		taskWG.Done()
		return true
	}).Times(numTask)

	s.rescheduler.Reschedule(namespaceID)
	taskWG.Wait()
	s.Equal(0, s.rescheduler.Len())
}

func (s *rescheudulerSuite) TestForceReschedule_ScheduledTask() {
	now := time.Now()
	s.timeSource.Update(now)
	namespaceID := s.mockScheduler.TaskChannelKeyFn()(nil).NamespaceID

	s.rescheduler.Start()
	defer s.rescheduler.Stop()

	taskWG := &sync.WaitGroup{}
	taskWG.Add(1)

	retryingTask := NewMockExecutable(s.controller)
	retryingTask.EXPECT().State().Return(ctasks.TaskStatePending).AnyTimes()
	retryingTask.EXPECT().SetScheduledTime(gomock.Any()).AnyTimes()
	retryingTask.EXPECT().GetKey().Return(htasks.NewKey(now.Add(-time.Minute), int64(1))).AnyTimes()
	s.rescheduler.Add(
		retryingTask,
		now.Add(time.Minute),
	)

	// schedule queue pre-fetches tasks
	futureTaskTimestamp := now.Add(time.Second)
	futureTask := NewMockExecutable(s.controller)
	futureTask.EXPECT().State().Return(ctasks.TaskStatePending).AnyTimes()
	futureTask.EXPECT().SetScheduledTime(gomock.Any()).AnyTimes()
	futureTask.EXPECT().GetKey().Return(htasks.NewKey(futureTaskTimestamp, int64(2))).AnyTimes()
	s.rescheduler.Add(
		futureTask,
		futureTaskTimestamp,
	)

	s.mockScheduler.EXPECT().TrySubmit(gomock.Any()).DoAndReturn(func(_ Executable) bool {
		taskWG.Done()
		return true
	}).Times(1)

	s.rescheduler.Reschedule(namespaceID)
	taskWG.Wait()
	s.Equal(1, s.rescheduler.Len())
}

func (s *rescheudulerSuite) TestPriorityOrdering() {
	// Test that tasks are processed in priority order
	now := time.Now()
	s.timeSource.Update(now)

	// Track the order in which tasks are submitted
	var submittedTasks []string
	var mu sync.Mutex

	// Map executables to their task IDs for identification
	taskMap := make(map[Executable]string)

	// Create a custom scheduler that records submission order
	mockScheduler := NewMockScheduler(s.controller)
	mockScheduler.EXPECT().TaskChannelKeyFn().Return(
		func(executable Executable) TaskChannelKey {
			taskID := taskMap[executable]
			switch taskID {
			case "high_ns_a":
				return TaskChannelKey{NamespaceID: "namespace_a", Priority: tasks.PriorityHigh}
			case "high_ns_c":
				return TaskChannelKey{NamespaceID: "namespace_c", Priority: tasks.PriorityHigh}
			case "low_ns_b":
				return TaskChannelKey{NamespaceID: "namespace_b", Priority: tasks.PriorityLow}
			case "low_ns_d":
				return TaskChannelKey{NamespaceID: "namespace_d", Priority: tasks.PriorityLow}
			case "preempt_ns_a":
				return TaskChannelKey{NamespaceID: "namespace_a", Priority: tasks.PriorityPreemptable}
			case "preempt_ns_b":
				return TaskChannelKey{NamespaceID: "namespace_b", Priority: tasks.PriorityPreemptable}
			}
			return TaskChannelKey{NamespaceID: "default", Priority: tasks.PriorityLow}
		},
	).AnyTimes()

	mockScheduler.EXPECT().TrySubmit(gomock.Any()).DoAndReturn(func(executable Executable) bool {
		taskID := taskMap[executable]
		mu.Lock()
		submittedTasks = append(submittedTasks, taskID)
		mu.Unlock()
		return true
	}).AnyTimes()

	rescheduler := NewRescheduler(
		mockScheduler,
		s.timeSource,
		log.NewTestLogger(),
		metrics.NoopMetricsHandler,
	)

	// Create tasks with mixed priorities and namespaces in random insertion order
	taskIDs := []string{
		"preempt_ns_b", "high_ns_c", "low_ns_d",
		"preempt_ns_a", "high_ns_a", "low_ns_b",
	}

	// Add tasks in random order to ensure we're testing sorting, not insertion order
	for _, taskID := range taskIDs {
		mockTask := NewMockExecutable(s.controller)
		taskMap[mockTask] = taskID
		mockTask.EXPECT().SetScheduledTime(gomock.Any()).AnyTimes()
		mockTask.EXPECT().State().Return(ctasks.TaskStatePending).AnyTimes()

		rescheduler.Add(mockTask, now.Add(-time.Second)) // All tasks ready to be processed
	}

	// Process all tasks
	rescheduler.reschedule()

	// All tasks should be processed in priority order
	s.Len(submittedTasks, 6, "Should process all 6 tasks")
	s.Equal(0, rescheduler.Len(), "Should have no remaining tasks")

	rank := func(taskID string) int {
		switch {
		case strings.HasPrefix(taskID, "high_"):
			return 1
		case strings.HasPrefix(taskID, "low_"):
			return 2
		case strings.HasPrefix(taskID, "preempt_"):
			return 3
		default:
			return 999
		}
	}

	s.True(slices.IsSortedFunc(submittedTasks, func(a, b string) int {
		return rank(a) - rank(b)
	}), "Tasks should be sorted by priority order")
}
