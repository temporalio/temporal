package queues

import (
	"math/rand"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	ctasks "go.temporal.io/server/common/tasks"
	"go.temporal.io/server/common/telemetry"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
)

type (
	memoryScheduledQueueSuite struct {
		suite.Suite
		*require.Assertions

		controller            *gomock.Controller
		mockTimeSource        *clock.EventTimeSource
		mockScheduler         *ctasks.MockScheduler[ctasks.Task]
		mockClusterMetadata   *cluster.MockMetadata
		mockNamespaceRegistry *namespace.MockRegistry

		scheduledQueue *memoryScheduledQueue
	}
)

func TestMemoryScheduledQueueSuite(t *testing.T) {
	s := new(memoryScheduledQueueSuite)
	suite.Run(t, s)
}

func (s *memoryScheduledQueueSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.mockTimeSource = clock.NewEventTimeSource()
	s.mockTimeSource.Update(time.Now().UTC())
	s.mockClusterMetadata = cluster.NewMockMetadata(s.controller)
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockNamespaceRegistry = namespace.NewMockRegistry(s.controller)
	s.mockNamespaceRegistry.EXPECT().GetNamespaceByID(gomock.Any()).Return(tests.LocalNamespaceEntry, nil).AnyTimes()

	s.mockScheduler = ctasks.NewMockScheduler[ctasks.Task](s.controller)
	s.mockScheduler.EXPECT().Start().AnyTimes()
	s.mockScheduler.EXPECT().Stop().AnyTimes()

	s.scheduledQueue = newMemoryScheduledQueue(
		s.mockScheduler,
		s.mockTimeSource,
		log.NewTestLogger(),
		metrics.NoopMetricsHandler,
	)

	s.scheduledQueue.Start()
}

func (s *memoryScheduledQueueSuite) TearDownTest() {
	s.scheduledQueue.Stop()
}

func (s *memoryScheduledQueueSuite) Test_ThreeInOrderTasks() {

	now := s.mockTimeSource.Now()
	t1 := s.newSpeculativeWorkflowTaskTimeoutTestExecutable(now.Add(20 * time.Millisecond))
	t2 := s.newSpeculativeWorkflowTaskTimeoutTestExecutable(now.Add(10 * time.Millisecond))
	t3 := s.newSpeculativeWorkflowTaskTimeoutTestExecutable(now.Add(30 * time.Millisecond))

	calls := atomic.Int32{}
	calls.Store(3)
	gomock.InOrder(
		s.mockScheduler.EXPECT().TrySubmit(t2).Return(true).Do(func(_ ctasks.Task) { calls.Add(-1) }),
		s.mockScheduler.EXPECT().TrySubmit(t1).Return(true).Do(func(_ ctasks.Task) { calls.Add(-1) }),
		s.mockScheduler.EXPECT().TrySubmit(t3).Return(true).Do(func(_ ctasks.Task) { calls.Add(-1) }),
	)

	s.scheduledQueue.Add(t1)
	s.scheduledQueue.Add(t2)
	s.scheduledQueue.Add(t3)

	// To ensure all timers have fired.
	s.Eventually(func() bool { return calls.Load() == 0 }, time.Second, 100*time.Millisecond)
}

func (s *memoryScheduledQueueSuite) Test_ThreeCancelledTasks() {

	s.scheduledQueue.scheduler = s.mockScheduler

	now := s.mockTimeSource.Now()
	t1 := s.newSpeculativeWorkflowTaskTimeoutTestExecutable(now.Add(20 * time.Millisecond))
	t2 := s.newSpeculativeWorkflowTaskTimeoutTestExecutable(now.Add(10 * time.Millisecond))
	t3 := s.newSpeculativeWorkflowTaskTimeoutTestExecutable(now.Add(30 * time.Millisecond))

	t1.Cancel()
	t2.Cancel()

	calls := atomic.Int32{}
	calls.Store(1)
	s.mockScheduler.EXPECT().TrySubmit(t3).Return(true).Do(func(_ ctasks.Task) { calls.Add(-1) })

	s.scheduledQueue.Add(t1)
	s.scheduledQueue.Add(t2)
	s.scheduledQueue.Add(t3)

	s.Eventually(func() bool { return calls.Load() == 0 }, time.Second, 100*time.Millisecond)
}

func (s *memoryScheduledQueueSuite) Test_1KRandomTasks() {

	s.scheduledQueue.scheduler = s.mockScheduler

	now := s.mockTimeSource.Now()
	t := make([]*speculativeWorkflowTaskTimeoutExecutable, 1000)
	calls := atomic.Int32{}

	for i := 0; i < 1000; i++ {
		t[i] = s.newSpeculativeWorkflowTaskTimeoutTestExecutable(now.Add(time.Duration(rand.Intn(100)) * time.Microsecond))

		// Randomly cancel some tasks.
		if rand.Intn(2) == 0 {
			t[i].Cancel()
		} else {
			calls.Add(1)
		}
	}

	for i := 0; i < 1000; i++ {
		if t[i].State() != ctasks.TaskStateCancelled {
			s.mockScheduler.EXPECT().TrySubmit(t[i]).Return(true).Do(func(_ ctasks.Task) { calls.Add(-1) })
		}
	}

	for i := 0; i < 1000; i++ {
		s.scheduledQueue.Add(t[i])
	}

	// To ensure all timers have fired.
	s.Eventually(func() bool { return calls.Load() == 0 }, 10*time.Second, 100*time.Millisecond)
}

func (s *memoryScheduledQueueSuite) newSpeculativeWorkflowTaskTimeoutTestExecutable(
	visibilityTimestamp time.Time,
) *speculativeWorkflowTaskTimeoutExecutable {

	wttt := &tasks.WorkflowTaskTimeoutTask{
		VisibilityTimestamp: visibilityTimestamp,
	}

	return newSpeculativeWorkflowTaskTimeoutExecutable(
		NewExecutable(
			0,
			wttt,
			nil,
			nil,
			nil,
			NewNoopPriorityAssigner(),
			s.mockTimeSource,
			s.mockNamespaceRegistry,
			s.mockClusterMetadata,
			chasm.NewRegistry(log.NewTestLogger()),
			GetTaskTypeTagValue,
			nil,
			metrics.NoopMetricsHandler,
			telemetry.NoopTracer,
		),
		wttt,
	)
}
