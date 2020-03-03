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

package history

import (
	"errors"
	"fmt"
	"math/rand"
	"testing"
	"time"

	gomock "github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	"github.com/uber/cadence/common/backoff"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/loggerimpl"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/task"
)

type (
	queueTaskProcessorSuite struct {
		suite.Suite
		*require.Assertions

		controller           *gomock.Controller
		mockPriorityAssigner *MocktaskPriorityAssigner

		metricsClient metrics.Client
		logger        log.Logger

		processor *queueTaskProcessorImpl
	}

	mockQueueTaskMatcher struct {
		task *MockqueueTask
	}
)

func TestQueueTaskProcessorSuite(t *testing.T) {
	s := new(queueTaskProcessorSuite)
	suite.Run(t, s)
}

func (s *queueTaskProcessorSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.mockPriorityAssigner = NewMocktaskPriorityAssigner(s.controller)

	s.metricsClient = metrics.NewClient(tally.NoopScope, metrics.History)
	s.logger = loggerimpl.NewDevelopmentForTest(s.Suite)

	s.processor = s.newTestQueueTaskProcessor()
}

func (s *queueTaskProcessorSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *queueTaskProcessorSuite) TestIsRunning() {
	s.False(s.processor.isRunning())

	s.processor.Start()
	s.True(s.processor.isRunning())

	s.processor.Stop()
	s.False(s.processor.isRunning())
}

func (s *queueTaskProcessorSuite) TestPrepareSubmit_AssignPriorityFailed() {
	mockTask := NewMockqueueTask(s.controller)
	errAssign := errors.New("some random error")
	s.mockPriorityAssigner.EXPECT().Assign(newMockQueueTaskMatcher(mockTask)).Return(errAssign).Times(1)

	s.processor.Start()
	scheduler, err := s.processor.prepareSubmit(mockTask)
	s.Equal(errAssign, err)
	s.Nil(scheduler)
}

func (s *queueTaskProcessorSuite) TestPrepareSubmit_ProcessorNotRunning() {
	shardID := 0

	mockTask := NewMockqueueTask(s.controller)
	mockTask.EXPECT().GetShardID().Return(shardID).Times(1)
	s.mockPriorityAssigner.EXPECT().Assign(newMockQueueTaskMatcher(mockTask)).Return(nil).Times(1)

	scheduler, err := s.processor.prepareSubmit(mockTask)
	s.Equal(errTaskProcessorNotRunning, err)
	s.Nil(scheduler)
}

func (s *queueTaskProcessorSuite) TestPrepareSubmit_ShardProcessorAlreadyExists() {
	shardID := 0

	mockTask := NewMockqueueTask(s.controller)
	mockTask.EXPECT().GetShardID().Return(shardID).Times(1)
	s.mockPriorityAssigner.EXPECT().Assign(newMockQueueTaskMatcher(mockTask)).Return(nil).Times(1)

	mockScheduler := task.NewMockScheduler(s.controller)
	s.processor.schedulers[shardID] = mockScheduler

	s.processor.Start()
	scheduler, err := s.processor.prepareSubmit(mockTask)
	s.NoError(err)
	s.Equal(mockScheduler, scheduler)
}

func (s *queueTaskProcessorSuite) TestPrepareSubmit_ShardProcessorNotExist() {
	shardID := 0

	mockTask := NewMockqueueTask(s.controller)
	mockTask.EXPECT().GetShardID().Return(shardID).Times(1)
	s.mockPriorityAssigner.EXPECT().Assign(newMockQueueTaskMatcher(mockTask)).Return(nil).Times(1)

	s.Empty(s.processor.schedulers)

	s.processor.Start()
	scheduler, err := s.processor.prepareSubmit(mockTask)
	s.NoError(err)

	s.Len(s.processor.schedulers, 1)
	scheduler.Stop()
}

func (s *queueTaskProcessorSuite) TestStopShardProcessor() {
	shardID := 0

	s.Empty(s.processor.schedulers)
	s.processor.StopShardProcessor(shardID)

	mockScheduler := task.NewMockScheduler(s.controller)
	mockScheduler.EXPECT().Stop().Times(1)
	s.processor.schedulers[shardID] = mockScheduler

	s.processor.StopShardProcessor(shardID)
	s.Empty(s.processor.schedulers)
}

func (s *queueTaskProcessorSuite) TestStop() {
	for i := 0; i != 10; i++ {
		shardID := rand.Int()
		mockScheduler := task.NewMockScheduler(s.controller)
		mockScheduler.EXPECT().Stop().Times(1)
		s.processor.schedulers[shardID] = mockScheduler
	}

	s.processor.Start()
	s.processor.Stop()

	s.Empty(s.processor.schedulers)
}

func (s *queueTaskProcessorSuite) TestSubmit() {
	shardID := 0

	mockTask := NewMockqueueTask(s.controller)
	mockTask.EXPECT().GetShardID().Return(shardID).Times(1)
	s.mockPriorityAssigner.EXPECT().Assign(newMockQueueTaskMatcher(mockTask)).Return(nil).Times(1)

	mockScheduler := task.NewMockScheduler(s.controller)
	mockScheduler.EXPECT().Submit(newMockQueueTaskMatcher(mockTask)).Return(nil).Times(1)
	s.processor.schedulers[shardID] = mockScheduler

	s.processor.Start()
	err := s.processor.Submit(mockTask)
	s.NoError(err)
}

func (s *queueTaskProcessorSuite) TestTrySubmit_Fail() {
	shardID := 0

	mockTask := NewMockqueueTask(s.controller)
	mockTask.EXPECT().GetShardID().Return(shardID).Times(1)
	s.mockPriorityAssigner.EXPECT().Assign(newMockQueueTaskMatcher(mockTask)).Return(nil).Times(1)

	errTrySubmit := errors.New("some randome error")
	mockScheduler := task.NewMockScheduler(s.controller)
	mockScheduler.EXPECT().TrySubmit(newMockQueueTaskMatcher(mockTask)).Return(false, errTrySubmit).Times(1)
	s.processor.schedulers[shardID] = mockScheduler

	s.processor.Start()
	submitted, err := s.processor.TrySubmit(mockTask)
	s.Equal(errTrySubmit, err)
	s.False(submitted)
}

func (s *queueTaskProcessorSuite) TestNewQueueTaskProcessor_UnknownSchedulerType() {
	processor, err := newQueueTaskProcessor(
		s.mockPriorityAssigner,
		&queueTaskProcessorOptions{
			schedulerType: 0,
			fifoSchedulerOptions: &task.FIFOTaskSchedulerOptions{
				QueueSize:   100,
				WorkerCount: 10,
				RetryPolicy: backoff.NewExponentialRetryPolicy(time.Millisecond),
			},
		},
		s.logger,
		s.metricsClient,
	)
	s.Equal(errUnknownTaskSchedulerType, err)
	s.Nil(processor)
}

func (s *queueTaskProcessorSuite) TestNewQueueTaskProcessor_SchedulerOptionNotSpecified() {
	processor, err := newQueueTaskProcessor(
		s.mockPriorityAssigner,
		&queueTaskProcessorOptions{
			schedulerType: task.SchedulerTypeFIFO,
		},
		s.logger,
		s.metricsClient,
	)
	s.Equal(errTaskSchedulerOptionsNotSpecified, err)
	s.Nil(processor)
}

func (s *queueTaskProcessorSuite) newTestQueueTaskProcessor() *queueTaskProcessorImpl {
	processor, err := newQueueTaskProcessor(
		s.mockPriorityAssigner,
		&queueTaskProcessorOptions{
			schedulerType: task.SchedulerTypeFIFO,
			fifoSchedulerOptions: &task.FIFOTaskSchedulerOptions{
				QueueSize:   100,
				WorkerCount: 10,
				RetryPolicy: backoff.NewExponentialRetryPolicy(time.Millisecond),
			},
		},
		s.logger,
		s.metricsClient,
	)
	s.NoError(err)
	return processor.(*queueTaskProcessorImpl)
}

func newMockQueueTaskMatcher(mockTask *MockqueueTask) gomock.Matcher {
	return &mockQueueTaskMatcher{
		task: mockTask,
	}
}

func (m *mockQueueTaskMatcher) Matches(x interface{}) bool {
	taskPtr, ok := x.(*MockqueueTask)
	if !ok {
		return false
	}
	return taskPtr == m.task
}

func (m *mockQueueTaskMatcher) String() string {
	return fmt.Sprintf("is equal to %v", m.task)
}
