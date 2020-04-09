package task

import (
	"sync"
	"testing"
	"time"

	gomock "github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"

	"github.com/temporalio/temporal/common/backoff"
	"github.com/temporalio/temporal/common/log/loggerimpl"
	"github.com/temporalio/temporal/common/metrics"
)

type (
	fifoTaskSchedulerSuite struct {
		*require.Assertions
		suite.Suite

		controller    *gomock.Controller
		mockProcessor *MockProcessor

		queueSize int

		scheduler *fifoTaskSchedulerImpl
	}
)

func TestFIFOTaskSchedulerSuite(t *testing.T) {
	s := new(fifoTaskSchedulerSuite)
	suite.Run(t, s)
}

func (s *fifoTaskSchedulerSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.mockProcessor = NewMockProcessor(s.controller)

	s.queueSize = 2
	s.scheduler = NewFIFOTaskScheduler(
		loggerimpl.NewDevelopmentForTest(s.Suite),
		metrics.NewClient(tally.NoopScope, metrics.Common).Scope(metrics.TaskSchedulerScope),
		&FIFOTaskSchedulerOptions{
			QueueSize:   s.queueSize,
			WorkerCount: 1,
			RetryPolicy: backoff.NewExponentialRetryPolicy(time.Millisecond),
		},
	).(*fifoTaskSchedulerImpl)
}

func (s *fifoTaskSchedulerSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *fifoTaskSchedulerSuite) TestFIFO() {
	numTasks := 5
	tasks := []PriorityTask{}
	var taskWG sync.WaitGroup

	calls := []*gomock.Call{
		s.mockProcessor.EXPECT().Start(),
	}
	mockFn := func(_ Task) error {
		taskWG.Done()
		return nil
	}
	for i := 0; i != numTasks; i++ {
		mockTask := NewMockPriorityTask(s.controller)
		tasks = append(tasks, mockTask)
		taskWG.Add(1)
		calls = append(calls, s.mockProcessor.EXPECT().Submit(newMockPriorityTaskMatcher(mockTask)).DoAndReturn(mockFn))
	}
	calls = append(calls, s.mockProcessor.EXPECT().Stop())
	gomock.InOrder(calls...)

	s.scheduler.processor = s.mockProcessor
	s.scheduler.Start()
	for _, task := range tasks {
		s.NoError(s.scheduler.Submit(task))
	}
	taskWG.Wait()
	s.scheduler.Stop()
}

func (s *fifoTaskSchedulerSuite) TestTrySubmit() {
	for i := 0; i != s.queueSize; i++ {
		mockTask := NewMockPriorityTask(s.controller)
		submitted, err := s.scheduler.TrySubmit(mockTask)
		s.NoError(err)
		s.True(submitted)
	}

	// now the queue is full, submit one more task, should be non-blocking
	mockTask := NewMockPriorityTask(s.controller)
	submitted, err := s.scheduler.TrySubmit(mockTask)
	s.NoError(err)
	s.False(submitted)
}
