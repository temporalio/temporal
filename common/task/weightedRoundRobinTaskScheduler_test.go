// Copyright (c) 2020 Uber Technologies, Inc.
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

package task

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	gomock "github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"

	"github.com/uber/cadence/common/backoff"
	"github.com/uber/cadence/common/log/loggerimpl"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/service/dynamicconfig"
)

type (
	weightedRoundRobinTaskSchedulerSuite struct {
		*require.Assertions
		suite.Suite

		controller    *gomock.Controller
		mockProcessor *MockProcessor

		queueSize int

		scheduler *weightedRoundRobinTaskSchedulerImpl
	}

	mockPriorityTaskMatcher struct {
		task *MockPriorityTask
	}
)

var (
	testSchedulerWeights = map[int]dynamicconfig.IntPropertyFn{
		0: dynamicconfig.GetIntPropertyFn(3),
		1: dynamicconfig.GetIntPropertyFn(2),
		2: dynamicconfig.GetIntPropertyFn(1),
	}
)

func TestWeightedRoundRobinTaskSchedulerSuite(t *testing.T) {
	s := new(weightedRoundRobinTaskSchedulerSuite)
	suite.Run(t, s)
}

func (s *weightedRoundRobinTaskSchedulerSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.mockProcessor = NewMockProcessor(s.controller)

	s.queueSize = 10
	s.scheduler = s.newTestWeightedRoundRobinTaskScheduler(
		&WeightedRoundRobinTaskSchedulerOptions{
			Weights:     testSchedulerWeights,
			QueueSize:   s.queueSize,
			WorkerCount: 1,
			RetryPolicy: backoff.NewExponentialRetryPolicy(time.Millisecond),
		},
	)
}

func (s *weightedRoundRobinTaskSchedulerSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *weightedRoundRobinTaskSchedulerSuite) TestSubmit_Success() {
	taskPriority := 1
	mockTask := NewMockPriorityTask(s.controller)
	mockTask.EXPECT().Priority().Return(taskPriority)

	err := s.scheduler.Submit(mockTask)
	s.NoError(err)

	task := <-s.scheduler.taskChs[taskPriority]
	s.Equal(mockTask, task)
	for _, taskCh := range s.scheduler.taskChs {
		s.Empty(taskCh)
	}
}

func (s *weightedRoundRobinTaskSchedulerSuite) TestSubmit_Fail_SchedulerShutDown() {
	// create a new scheduler here with queue size 0, otherwise test is non-deterministic
	scheduler := s.newTestWeightedRoundRobinTaskScheduler(
		&WeightedRoundRobinTaskSchedulerOptions{
			Weights:     testSchedulerWeights,
			QueueSize:   0,
			WorkerCount: 1,
			RetryPolicy: backoff.NewExponentialRetryPolicy(time.Millisecond),
		},
	)

	taskPriority := 1
	mockTask := NewMockPriorityTask(s.controller)
	mockTask.EXPECT().Priority().Return(taskPriority)
	scheduler.Start()
	scheduler.Stop()
	err := scheduler.Submit(mockTask)
	s.Equal(ErrTaskSchedulerClosed, err)
}

func (s *weightedRoundRobinTaskSchedulerSuite) TestSubmit_Fail_UnknownPriority() {
	taskPriority := 5 // make sure the number is not in testSchedulerWeights
	mockTask := NewMockPriorityTask(s.controller)
	mockTask.EXPECT().Priority().Return(taskPriority)
	err := s.scheduler.Submit(mockTask)
	s.Error(err)
	s.NotEqual(ErrTaskSchedulerClosed, err)
}

func (s *weightedRoundRobinTaskSchedulerSuite) TestTrySubmit() {
	taskPriority := 1
	for i := 0; i != s.queueSize; i++ {
		mockTask := NewMockPriorityTask(s.controller)
		mockTask.EXPECT().Priority().Return(taskPriority)
		submitted, err := s.scheduler.TrySubmit(mockTask)
		s.NoError(err)
		s.True(submitted)
	}

	// now the queue is full, submit one more task, should be non-blocking
	mockTask := NewMockPriorityTask(s.controller)
	mockTask.EXPECT().Priority().Return(taskPriority)
	submitted, err := s.scheduler.TrySubmit(mockTask)
	s.NoError(err)
	s.False(submitted)
}

func (s *weightedRoundRobinTaskSchedulerSuite) TestDispatcher_SubmitWithNoError() {
	numPriorities := len(testSchedulerWeights)
	tasks := [][]*MockPriorityTask{}
	var taskWG sync.WaitGroup
	for i := 0; i != numPriorities; i++ {
		tasks = append(tasks, []*MockPriorityTask{})
	}

	taskPerPriority := 5
	numSubmittedTask := 0
	tasksPerRound := []int{6, 5, 2, 1, 1}
	round := 0
	mockFn := func(_ Task) error {
		numSubmittedTask++
		if numSubmittedTask == tasksPerRound[round] {
			round++
			numSubmittedTask = 0
			for priority, weightFn := range testSchedulerWeights {
				expectedRemainingTasksNum := taskPerPriority - round*weightFn()
				if expectedRemainingTasksNum < 0 {
					expectedRemainingTasksNum = 0
				}
				s.Equal(expectedRemainingTasksNum, len(s.scheduler.taskChs[priority]))
			}
		}

		taskWG.Done()
		return nil
	}

	for priority := range testSchedulerWeights {
		for i := 0; i != taskPerPriority; i++ {
			mockTask := NewMockPriorityTask(s.controller)
			mockTask.EXPECT().Priority().Return(priority).AnyTimes()
			s.scheduler.Submit(mockTask)
			tasks[priority] = append(tasks[priority], mockTask)
			taskWG.Add(1)
			s.mockProcessor.EXPECT().Submit(newMockPriorityTaskMatcher(mockTask)).DoAndReturn(mockFn)
		}
	}

	s.scheduler.processor = s.mockProcessor

	doneCh := make(chan struct{})
	go func() {
		s.scheduler.dispatcherWG.Add(1)
		s.scheduler.dispatcher()
		close(doneCh)
	}()

	taskWG.Wait()
	close(s.scheduler.shutdownCh)

	<-doneCh
}

func (s *weightedRoundRobinTaskSchedulerSuite) TestDispatcher_FailToSubmit() {
	mockTask := NewMockPriorityTask(s.controller)
	mockTask.EXPECT().Priority().Return(0)
	mockTask.EXPECT().Nack()

	var taskWG sync.WaitGroup
	s.scheduler.Submit(mockTask)
	taskWG.Add(1)

	mockFn := func(_ Task) error {
		taskWG.Done()
		return errors.New("some random error")
	}
	s.mockProcessor.EXPECT().Submit(newMockPriorityTaskMatcher(mockTask)).DoAndReturn(mockFn)
	s.scheduler.processor = s.mockProcessor

	doneCh := make(chan struct{})
	go func() {
		s.scheduler.dispatcherWG.Add(1)
		s.scheduler.dispatcher()
		close(doneCh)
	}()

	taskWG.Wait()
	close(s.scheduler.shutdownCh)

	<-doneCh
}

func (s *weightedRoundRobinTaskSchedulerSuite) newTestWeightedRoundRobinTaskScheduler(
	options *WeightedRoundRobinTaskSchedulerOptions,
) *weightedRoundRobinTaskSchedulerImpl {
	scheduler, err := NewWeightedRoundRobinTaskScheduler(
		loggerimpl.NewDevelopmentForTest(s.Suite),
		metrics.NewClient(tally.NoopScope, metrics.Common).Scope(metrics.TaskSchedulerScope),
		options,
	)
	s.NoError(err)
	return scheduler.(*weightedRoundRobinTaskSchedulerImpl)
}

func newMockPriorityTaskMatcher(mockTask *MockPriorityTask) gomock.Matcher {
	return &mockPriorityTaskMatcher{
		task: mockTask,
	}
}

func (m *mockPriorityTaskMatcher) Matches(x interface{}) bool {
	taskPtr, ok := x.(*MockPriorityTask)
	if !ok {
		return false
	}
	return taskPtr == m.task
}

func (m *mockPriorityTaskMatcher) String() string {
	return fmt.Sprintf("is equal to %v", m.task)
}
